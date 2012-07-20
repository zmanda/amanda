# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Changer::compat;

use strict;
use warnings;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

use Carp;
use File::Glob qw( :glob );
use File::Path;
use Amanda::Paths;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Config qw( :getconf );
use Amanda::Debug qw( debug );
use Amanda::Device qw( :constants );
use Amanda::Changer;
use Amanda::MainLoop;

=head1 NAME

Amanda::Changer::compat -- run "old" changer scripts

=head1 DESCRIPTION

This package calls through to old Changer API shell scripts using the new API.
If necessary, it writes temporary configurations under C<$AMANDA_TMPDIR> and
invokes the changer there, allowing multiple distinct changers to run within
the same Amanda process.

See the amanda-changers(7) manpage for usage information.

=head2 NOTE

In-process reservations are handled correctly - only one device may be used at
a time.  However, the underlying scripts do not support reservations, so
another application can easily run the script and change the current device.
Caveat emptor.

=cut

# TODO
# Clean out old changer temporary directories on object destruction.

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;
    my ($script) = ($tpchanger =~ /chg-compat:(.*)/);

    unless (-e $script) {
	$script = "$amlibexecdir/$script";
    }

    if (! -x $script) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "'$script' is not executable");
    }

    my $self = {
        script => $script,
	config => $config,
	reserved => 0,
	nslots => undef,
	backwards => undef,
	searchable => undef,
	lock => [],
	got_info => 0,
	info_lock => [],
    };
    bless ($self, $class);

    $self->_make_cfg_dir($config);

    debug("$class initialized with script $script, temporary directory $self->{cfg_dir}");

    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;

    $self->validate_params('load', \%params);
    return if $self->check_error($params{'res_cb'});

    if ($self->{'reserved'}) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "driveinuse",
	    message => "Changer is already reserved: '" . $self->{'reserved'}->device_name . "'");
    }

    my $steps = define_steps
	cb_ref => \$params{'res_cb'};

    # make sure the info is loaded, and re-call load() if we have to wait
    step get_info => sub {
	$self->_get_info($steps->{'got_info'});
    };

    step got_info => sub {
	my ($exitval, $message) = @_;
	if (defined $exitval) { # error
	    # this is always fatal - we can't load without info
	    return $self->make_error("fatal", $params{'res_cb'},
		message => $message);
	}

	$steps->{'start_load'}->();
    };

    step start_load => sub {
	if (exists $params{'label'}) {
	    if ($self->{'searchable'}) {
		$self->_run_tpchanger($steps->{'load_run_done'}, "-search", $params{'label'});
	    } else {
		# not searchable -- run a manual scan
		$self->_manual_scan(%params);
	    }
	} elsif (exists $params{'relative_slot'}) {
	    # if there is an explicit $slot, then just hope it's the same as the current
	    # slot, or we're in trouble.  We don't know what the current slot is, so we
	    # can't verify, but the current slot is set on *every* load, so this works.

	    # if we've already seen nslots slots, then the next slot is
	    # certainly one of them, so the iteration should terminate.
	    # However, not all changers will return nslots distinct slots
	    # (chg-zd-mtx skips empty slots, for example), so we will need to
	    # protect against except_slots in other ways, too.
	    if (exists $params{'except_slots'} and (keys %{$params{'except_slots'}}) == $self->{'nslots'}) {
		return $self->make_error("failed", $params{'res_cb'},
		    reason => 'notfound',
		    message => "all slots have been loaded");
	    }

	    $self->_run_tpchanger($steps->{'load_run_done'}, "-slot", $params{'relative_slot'});
	} elsif (exists $params{'slot'}) {
	    $self->_run_tpchanger($steps->{'load_run_done'}, "-slot", $params{'slot'});
	}
    };

    step load_run_done => sub {
	my ($exitval, $slot, $rest) = @_;
	if ($exitval == 0) {
	    if (!$rest) {
		return $self->make_error("fatal", $params{'res_cb'},
		    message => "changer script did not provide a device name");
	    }
	} elsif ($exitval >= 2) {
		return $self->make_error("fatal", $params{'res_cb'},
		    message => $rest);
	} else {
	    return $self->make_error("failed", $params{'res_cb'},
		reason => "notfound",
		message => $rest);
	}

	# re-check except_slots, and return 'notfound' if we've loaded a
	# forbidden slot.  This will generally happen when scanning, and when
	# the underlying changer script has "skipped" some slots and looped
	# around earlier than we expected.
	if (exists $params{'except_slots'} and exists $params{'except_slots'}{$slot}) {
	    return $self->make_error("failed", $params{'res_cb'},
		reason => 'notfound',
		message => "all slots have been loaded");
	}

	return $self->_make_res($params{'res_cb'}, $slot, $rest, undef);
    };
}

sub _manual_scan {
    my $self = shift;
    my %params = @_;
    my $nchecked = 0;
    my ($get_info, $got_info, $run_cb, $load_next);
    my $first_scanned_slot = -1;

    my $user_msg_fn = $params{'user_msg_fn'};
    $user_msg_fn ||= sub { Amanda::Debug::info("chg-compat: " . $_[0]); };

    # search manually, starting with "current" and proceeding through nslots-1
    # loads of "next".  This doesn't use the except_slots iteration mechanism as
    # that would just add extra layers of complexity with no benefit

    $get_info = sub {
	$self->_get_info($got_info);
    };

    $got_info = sub {
	$user_msg_fn->("beginning manual scan of $self->{nslots} slots");
	$self->_run_tpchanger($run_cb, "-slot", "current");
    };
    $run_cb = sub {
        my ($exitval, $slot, $rest) = @_;

	if ($slot == $first_scanned_slot) {
	    $nchecked = $self->{'nslots'};
	    return $load_next->();
	}

	$first_scanned_slot = $slot if $first_scanned_slot == -1;

	$user_msg_fn->("updated slot $slot");
	if ($exitval == 0) {
	    # if we're looking for a label, check what we got
	    if (defined $params{'label'}) {
		my $device = Amanda::Device->new($rest);
		if ($device and $device->configure(1)
			    and $device->read_label() == $DEVICE_STATUS_SUCCESS
			    and $device->volume_label() eq $params{'label'}) {
		    # we found the correct slot
		    $self->_make_res($params{'res_cb'}, $slot, $rest, $device);
		    return;
		}
	    }

	    return $load_next->();
	} else {
	    # don't continue scanning after a fatal error
	    if ($exitval >= 2) {
		return $self->make_error("fatal", $params{'res_cb'},
		    message => $rest);
	    }

	    return $load_next->();
	}
    };

    $load_next = sub {
	# if we've scanned all nslots, we haven't found the label.
        if (++$nchecked >= $self->{'nslots'}) {
	    if (defined $params{'label'}) {
		return $self->make_error("failed", $params{'res_cb'},
		    reason => "notfound",
		    message => "Volume '$params{label}' not found");
	    } else {
		return $params{'res_cb'}->(undef, undef);
	    }
	}

	$self->_run_tpchanger($run_cb, "-slot", "next");
    };

    $get_info->();
}

# takes $res_cb, $slot and $rest; creates and configures the device, and calls
# $res_cb with the results.
sub _make_res {
    my $self = shift;
    my ($res_cb, $slot, $rest, $device) = @_;
    my $res;

    if (!defined $device) {
	$device = Amanda::Device->new($rest);
	if ($device->status != $DEVICE_STATUS_SUCCESS) {
	    return $self->make_error("failed", $res_cb,
		    reason => "device",
		    message => "opening '$rest': " . $device->error_or_status());
	}
    }

    if (my $err = $self->{'config'}->configure_device($device)) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => $err);
    }

    $res = Amanda::Changer::compat::Reservation->new($self, $slot, $device);
    $device->read_label();

    $res_cb->(undef, $res);
}

sub info_setup {
    my $self = shift;
    my %params = @_;

    $self->_get_info(sub {
	my ($exitval, $message) = @_;
	if (defined $exitval) { # error
	    if ($exitval >= 2) {
		return $self->make_error("fatal", $params{'finished_cb'},
		    message => $message);
	    } else {
		return $self->make_error("failed", $params{'finished_cb'},
		    reason => "notfound",
		    message => $message);
	    }
	}

	# no error, so we're done with setup
	$params{'finished_cb'}->();
    });
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;

    if ($key eq 'num_slots') {
	$results{$key} = $self->{'nslots'};
    } elsif ($key eq 'fast_search') {
	$results{$key} = $self->{'searchable'};
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
}

# run a simple op -- no arguments, no slot returned
sub _simple_op {
    my $self = shift;
    my $op = shift;
    my %params = @_;

    my $run_cb = sub {
	my ($exitval, $slot, $rest) = @_;
	if ($exitval == 0) {
	    if (exists $params{'finished_cb'}) {
		$params{'finished_cb'}->(undef);
	    }
	} else {
	    if ($exitval >= 2) {
		return $self->make_error("fatal", $params{'finished_cb'},
		    message => $rest);
	    } else {
		return $self->make_error("failed", $params{'finished_cb'},
		    reason => "unknown",
		    message => $rest);
	    }
	}
    };
    $self->_run_tpchanger($run_cb, "-$op");
}

sub reset {
    my $self = shift;
    my %params = @_;

    $self->_simple_op("reset", %params);
}

sub clean {
    my $self = shift;
    my %params = @_;

    # note: parameter 'drive' is ignored
    $self->_simple_op("clean", %params);
}

sub eject {
    my $self = shift;
    my %params = @_;

    # note: parameter 'drive' is ignored
    $self->_simple_op("eject", %params);
}

sub update {
    my $self = shift;
    my %params = @_;

    if ($params{'changed'}) {
	return $self->make_error("failed", $params{'finished_cb'},
	    reason => 'invalid',
	    message => 'chg-compat does not support specifying what has changed');
    }

    my $scan_done_cb = make_cb(scan_done_cb => sub {
	my ($err, $res) = @_;
	if ($err) {
	    return $params{'finished_cb'}->($err);
	}

	# we didn't search for a label, so we don't get a reservation
	$params{'finished_cb'}->(undef);
    });

    # for compat changers, "update" just entails scanning the whole changer
    $self->_manual_scan(
	res_cb => $scan_done_cb,
	label => undef, # search forever
	user_msg_fn => $params{'user_msg_fn'},
    );
}

# Internal function to call the script's -info and store the results in $self.
# If this returns true, then the info is loaded; otherwise, got_info_cb will be
# called either with no arguments (success) or ($exitval, $message) on error.
sub _get_info {
    my ($self, $got_info_cb) = @_;

    Amanda::MainLoop::synchronized($self->{'info_lock'}, $got_info_cb, sub {
	my ($got_info_cb) = @_;

	# if we've already got info, just call back right away
	if ($self->{'got_info'}) {
	    return $got_info_cb->();
	}

	my $run_cb = sub {
	    my ($exitval, $slot, $rest) = @_;
	    if ($exitval == 0) {
		# old, unsearchable changers don't return the third result, so it's
		# optional in the regex
		unless ($rest =~ /(\d+) (\d+) ?(\d+)?/) {
		    return $got_info_cb->(2,
			    "Malformed response from changer -info: $rest");
		}

		$self->{'nslots'} = $1;
		$self->{'backward'} = $2;
		$self->{'searchable'} = $3? 1:0;

		$self->{'got_info'} = 1;
		return $got_info_cb->(undef, undef);
	    } else {
		return $got_info_cb->($exitval, $rest);
	    }
	};

	$self->_run_tpchanger($run_cb, "-info");
    });
}

# Internal function to create a temporary configuration directory, which persists
# for the duration of this changer's lifetime (and beyond, TODO)
sub _make_cfg_dir {
    my ($self, $config) = @_;

    if ($config->{'is_global'}) {
	# for the default changer, we don't need to invent a config..
	$self->{'cfg_dir'} = Amanda::Config::get_config_dir();
    } else {
	my $cfg_name = Amanda::Config::get_config_name();
	my $changer_name = $config->{'name'};
	my $tapedev = $config->{'tapedev'};
	my $tpchanger = $config->{'tpchanger'};
	my $changerdev = $config->{'changerdev'};
	my $changerfile = $config->{'changerfile'};

	my $cfg_dir = "$AMANDA_TMPDIR/Amanda::Changer::compat/$cfg_name-$changer_name";

	if (-d $cfg_dir) {
	    rmtree($cfg_dir)
		or die("Could not delete '$cfg_dir'");
	}

	mkpath($cfg_dir)
	    or die("Could not create '$cfg_dir'");

	# Write an amanda.conf
	open(my $amconf, ">", "$cfg_dir/amanda.conf")
	    or die ("Could not write '$cfg_dir/amanda.conf'");

	print $amconf "# automatically generated by Amanda::Changer::compat\n";
	print $amconf 'org "', getconf($CNF_ORG), "\"\n"
	    if getconf_seen($CNF_ORG);
	print $amconf 'mailto "', getconf($CNF_MAILTO), "\"\n"
	    if getconf_seen($CNF_MAILTO);
	print $amconf 'mailer "', getconf($CNF_MAILER), "\"\n"
	    if getconf_seen($CNF_MAILER);
	print $amconf "tapedev \"$tapedev\"\n"
	    if defined($tapedev);
	print $amconf "tpchanger \"$tpchanger\"\n"
	    if defined($tpchanger);
	print $amconf "changerdev \"$changerdev\"\n"
	    if defined($changerdev);
	print $amconf "changerfile \"",
		Amanda::Config::config_dir_relative($changerfile),
		"\"\n"
	    if defined($changerfile);

	# TODO: device_property, tapetype, and the tapetype def

	close $amconf;

	$self->{'cfg_dir'} = $cfg_dir;
    }

}

# Internal-use function to actually invoke a changer script and parse
# its output.
#
# @param $run_cb: called with ($exitval, $slot, $rest)
# @params @args: command-line arguments to follow the name of the changer
sub _run_tpchanger {
    my ($self, $run_cb, @args) = @_;

    Amanda::MainLoop::synchronized($self->{'lock'}, $run_cb, sub {
	my ($run_cb) = @_;
	debug("Amanda::Changer::compat: invoking $self->{script} with " . join(" ", @args));

	my ($readfd, $writefd) = POSIX::pipe();
	if (!defined($writefd)) {
	    croak("Error creating pipe to run changer script: $!");
	}

	my $pid = fork();
	if (!defined($pid) or $pid < 0) {
	    croak("Can't fork to run changer script: $!");
	}

	if (!$pid) {
	    ## child

	    # get our file-handle house in order
	    POSIX::close($readfd);
	    POSIX::dup2($writefd, 1);
	    POSIX::close($writefd);

	    # cd into the config dir
	    if (!chdir($self->{'cfg_dir'})) {
		print "<error> Could not chdir to '" . $self->{cfg_dir} . "'\n";
		exit(2);
	    }

	    %ENV = Amanda::Util::safe_env();

	    my $script = $self->{'script'};
	    { exec { $script } $script, @args; } # braces protect against warning

	    my $err = "<error> Could not exec $script: $!\n";
	    POSIX::write($writefd, $err, length($err));
	    exit 2;
	}

	## parent

	# clean up file descriptors from the fork
	POSIX::close($writefd);

	# the callbacks that follow share these lexical variables
	my $child_eof = 0;
	my $child_output = '';
	my $child_dead = 0;
	my $child_exit_status = 0;
	my ($fdsrc, $cwsrc);
	my ($maybe_finished, $fd_source_cb, $child_watch_source_cb);

	# Perl note: we have to use anonymous subs here, as they are instantiated
	# at runtime, rather than at compile time.

	$maybe_finished = sub {
	    return unless $child_eof;
	    return unless $child_dead;

	    # everything is finished -- process the results and invoke the callback
	    chomp $child_output;

	    # handle unexpected exit status as a fatal error
	    if (!POSIX::WIFEXITED($child_exit_status) || POSIX::WEXITSTATUS($child_exit_status) > 2) {
		$run_cb->(POSIX::WEXITSTATUS($child_exit_status), undef,
		    "Fatal error from changer script: ".$child_output);
		return;
	    }

	    # parse the child's output
	    my @child_output = split '\n', $child_output;
	    my $exitval = POSIX::WEXITSTATUS($child_exit_status);

	    debug("Amanda::Changer::compat: Got response '$child_output' with exit status $exitval");
	    if (@child_output < 1) {
		$run_cb->(2, undef, "Malformed output from changer script -- no output");
		return;
	    }
	    my $slotline = shift @child_output;
	    if ($slotline !~ /\s*([^\s]+)(?:\s+(.+))?/) {
		$run_cb->(2, undef, "Malformed output from changer script: '$slotline'");
		return;
	    }
	    my ($slot, $rest) = ($1, $2);

	    # append any additional lines to $rest
	    if (@child_output) {
		$rest .= "\n" . join("\n", @child_output);
	    }

	    # let the callback take care of any further interpretation
	    $run_cb->($exitval, $slot, $rest);
	};

	$fd_source_cb = sub {
	    my ($fdsrc) = @_;
	    my ($len, $bytes);
	    $len = POSIX::read($readfd, $bytes, 1024);

	    # if we got an EOF, shut things down.
	    if ($len == 0) {
		$child_eof = 1;
		POSIX::close($readfd);
		$fdsrc->remove();
		$fdsrc = undef; # break a reference loop
		$maybe_finished->();
	    } else {
		# otherwise, just keep the bytes
		$child_output .= $bytes;
	    }
	};
	$fdsrc = Amanda::MainLoop::fd_source($readfd, $G_IO_IN | $G_IO_ERR | $G_IO_HUP);
	$fdsrc->set_callback($fd_source_cb);

	$child_watch_source_cb = sub {
	    my ($cwsrc, $got_pid, $got_status) = @_;
	    $cwsrc->remove();
	    $cwsrc = undef; # break a reference loop
	    $child_dead = 1;
	    $child_exit_status = $got_status;

	    $maybe_finished->();
	};
	$cwsrc = Amanda::MainLoop::child_watch_source($pid);
	$cwsrc->set_callback($child_watch_source_cb);
    });
}

package Amanda::Changer::compat::Reservation;
use vars qw( @ISA );
use Amanda::Debug qw( debug );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $slot, $device) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;

    $self->{'device'} = $device;
    $self->{'this_slot'} = $slot;

    # mark the changer as reserved
    $self->{'chg'}->{'reserved'} = $device;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;

    my $finished = sub {
	my ($message) = @_;

	$self->{'chg'}->{'reserved'} = 0;

	# unref the device, for good measure
	$self->{'device'} = undef;

	$params{'finished_cb'}->($message) if $params{'finished_cb'};
    };

    if (exists $params{'eject'} && $params{'eject'}) {
	$self->{'chg'}->eject(finished_cb => $finished);
    } else {
	$finished->(undef);
    }
}

sub set_label {
    my $self = shift;
    my %params = @_;

    # non-searchable changers don't get -label, except that chg-zd-mtx needs
    # it to maintain its slotinfofile (this is a hack)
    if (!$self->{'chg'}->{'searchable'}
	&& $self->{'chg'}->{'script'} !~ /chg-zd-mtx$/) {
	debug("Amanda::Changer::compat - changer script is not searchable, so not invoking -label for set_label");
        $params{'finished_cb'}->(undef) if $params{'finished_cb'};
        return;
    }

    if (!defined $params{'label'}) {
        $params{'finished_cb'}->(undef) if $params{'finished_cb'};
        return;
    }

    my $run_cb = sub {
	my ($exitval, $slot, $rest) = @_;
	if ($exitval == 0) {
	    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
	} else {
	    if ($exitval >= 2) {
		return $self->{'chg'}->make_error("fatal", $params{'finished_cb'},
		    message => $rest);
	    } else {
		return $self->{'chg'}->make_error("failed", $params{'finished_cb'},
		    reason => "unknown",
		    message => $rest);
	    }
	}
    };
    $self->{'chg'}->_run_tpchanger(
        $run_cb, "-label", $params{'label'});
}

1;
