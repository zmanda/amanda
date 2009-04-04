# Copyright (c) 2005-2008 Zmanda, Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as
# published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
#
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

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

=head1 NAME

Amanda::Changer::compat -- run "old" changer scripts

=head1 DESCRIPTION

This package, calls through to old Changer API shell scripts using the new API.
If necessary, this writes temporary configurations under C<$AMANDA_TMPDIR> and
invokes the changer there, allowing multiple distinct changers to run within
the same Amanda process.

=head1 TODO

In-process reservations are handled correctly - only one device may be used at
a time.  However, the underlying scripts do not support reservations, so
another application can easily run the script and change the current device.
Caveat emptor.

Concurrent _run_tpchanger invocations are currently forbidden with a die() --
that should change to a simple FIFO queue of tpchanger invocations to make.

Clean out old changer temporary directories on object destruction.

Support 'update'

=cut

sub new {
    my $class = shift;
    my ($cc, $tpchanger) = @_;
    my ($script) = ($tpchanger =~ /chg-compat:(.*)/);

    my $self = {
        script => $script,
	reserved => 0,
	nslots => undef,
	backwards => undef,
	searchable => undef,
    };
    bless ($self, $class);

    $self->_make_cfg_dir($cc);

    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;

    die "no callback supplied" unless (exists $params{'res_cb'});
    my $cb = $params{'res_cb'};

    if ($self->{'reserved'}) {
	$cb->("Changer is already reserved: '" . $self->{'reserved'} . "'", undef);
	return;
    }

    # make sure the info is loaded, and re-call load() if we have to wait
    if (!defined($self->{'nslots'})) {
	$self->_get_info(
	    sub {
                my ($err) = @_;
		$self->load(%params);
	    },
	    sub {
		my ($msg) = @_;
		$cb->($msg, undef);
	    });
	return;
    }

    my $run_success_cb = sub {
        my ($slot, $rest) = @_;
        my $res = Amanda::Changer::compat::Reservation->new($self, $slot, $rest);
        $cb->(undef, $res);
    };
    my $run_fail_cb = sub {
        my ($exitval, $message) = @_;
        $cb->($message, undef);
    };

    if (exists $params{'label'}) {
        if ($self->{'searchable'}) {
            $self->_run_tpchanger($run_success_cb, $run_fail_cb, "-search", $params{'label'});
        } else {
            # not searchable -- run a manual scan
            $self->_manual_scan(%params);
        }
    } elsif (exists $params{'slot'}) {
        $self->_run_tpchanger($run_success_cb, $run_fail_cb, "-slot", $params{'slot'});
    }
}

sub _manual_scan {
    my $self = shift;
    my %params = @_;
    my $nchecked = 0;
    my ($run_success_cb, $run_fail_cb, $load_next);

    # search manually, starting with "current" and proceeding through nslots-1
    # loads of "next"

    # TODO: support the case where nslots == -1

    $run_success_cb = sub {
        my ($slot, $rest) = @_;

	my $device = Amanda::Device->new($rest);
	if ($device and $device->configure(1)
		    and $device->read_label() == $DEVICE_STATUS_SUCCESS
		    and $device->volume_label() eq $params{'label'}) {
            # we found the correct slot
	    my $res = Amanda::Changer::compat::Reservation->new($self, $slot, $rest);
            Amanda::MainLoop::call_later($params{'res_cb'}, undef, $res);
            return;
        }

        $load_next->();
    };

    $run_fail_cb = sub {
	my ($exitval, $message) = @_;

	# don't continue scanning after a fatal error
        if ($exitval > 1) {
	    Amanda::MainLoop::call_later($params{'res_cb'}, $message, undef);
	    return;
	}

	$load_next->();
    };

    $load_next = sub {
	# if we've scanned all nslots, we haven't found the label.
        if (++$nchecked >= $self->{'nslots'}) {
            Amanda::MainLoop::call_later($params{'res_cb'},
                    "Volume '$params{label}' not found", undef);
            return;
	}

	$self->_run_tpchanger($run_success_cb, $run_fail_cb, "-slot", "next");
    };

    $self->_run_tpchanger($run_success_cb, $run_fail_cb, "-slot", "current");
}

sub info {
    my $self = shift;
    my %params = @_;
    my %results;

    die "no info_cb supplied" unless (exists $params{'info_cb'});
    die "no info supplied" unless (exists $params{'info'});

    # make sure the info is loaded, and re-call info() if we have to wait
    if (!defined($self->{'nslots'}) && grep(/^num_slots$/, @{$params{'info'}})) {
	$self->_get_info(
	    sub {
                my ($err) = @_;
		$self->info(%params);
	    },
	    sub {
		my ($msg) = @_;
		$params{'info_cb'}->($msg);
	    });
	return;
    }

    # ok, info is loaded, so call back with the results
    for my $inf (@{$params{'info'}}) {
        if ($inf eq 'num_slots') {
            $results{$inf} = $self->{'nslots'};
        } else {
            warn "Ignoring request for info key '$inf'";
        }
    }

    Amanda::MainLoop::call_later($params{'info_cb'}, undef, %results);
}

# run a simple op -- no arguments, no slot returned
sub _simple_op {
    my $self = shift;
    my $op = shift;
    my %params = @_;

    Amanda::Debug::debug("running simple op '$op'");
    my $run_success_cb = sub {
	Amanda::Debug::debug("simple op '$op' ok");
        if (exists $params{'finished_cb'}) {
            $params{'finished_cb'}->(undef);
        }
    };
    my $run_fail_cb = sub {
	my ($exitval, $message) = @_;
	Amanda::Debug::debug("simple op '$op' failed: $message");
        if (exists $params{'finished_cb'}) {
            $params{'finished_cb'}->($message);
        }
    };
    $self->_run_tpchanger($run_success_cb, $run_fail_cb, "-$op");
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

    # TODO: not implemented
    # -- need to shuffle the driver over every slot
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->(undef);
    }
}

# Internal function to call the script's -info, store the results in $self, and
# call either $success_cb (with no arguments) or $error_cb (with an error
# message).
sub _get_info {
    my ($self, $success_cb, $error_cb) = @_;

    my $run_success_cb = sub {
	my ($slot, $rest) = @_;
	# old, unsearchable changers don't return the third result, so it's
	# optional in the regex
	$rest =~ /(\d+) (\d+) ?(\d+)?/ or
	    croak("Malformed response from changer -info: $rest");

	$self->{'nslots'} = $1;
	$self->{'backward'} = $2;
	$self->{'searchable'} = $3? 1:0;

	$success_cb->();
    };
    my $run_fail_cb = sub {
	my ($exitval, $message) = @_;
	$error_cb->($message);
    };
    $self->_run_tpchanger($run_success_cb, $run_fail_cb, "-info");
}

# Internal function to create a temporary configuration directory, which persists
# for the duration of this changer's lifetime (and beyond, TODO)
sub _make_cfg_dir {
    my ($self, $cc) = @_;

    if (defined $cc) {
	my $cfg_name = Amanda::Config::get_config_name();
	my $changer_name = changer_config_name($cc);
	my $tapedev = changer_config_getconf($cc, $CHANGER_CONFIG_TAPEDEV);
	my $tpchanger = changer_config_getconf($cc, $CHANGER_CONFIG_TPCHANGER);
	my $changerdev = changer_config_getconf($cc, $CHANGER_CONFIG_CHANGERDEV);
	my $changerfile = changer_config_getconf($cc, $CHANGER_CONFIG_CHANGERFILE);

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
    } else {
	# for the default changer, we don't need to invent a config..
	$self->{'cfg_dir'} = Amanda::Config::get_config_dir();
    }

}

# Internal-use function to actually invoke a changer script and parse
# its output.
#
# @param $success_cb: called with ($slot, $rest) on success
# @param $failure_cb: called with ($exitval, $message) on any failure
# @params @args: command-line arguments to follow the name of the changer
# @returns: array ($error, $slot, $rest), where $error is an error message if
#       a benign error occurred, or 0 if no error occurred
sub _run_tpchanger {
    my ($self, $success_cb, $failure_cb, @args) = @_;

    if ($self->{'busy'}) {
	croak("Changer is already in use");
    }

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
        unless (-x $script) {
            $script = "$amlibexecdir/$script";
        }
        { exec { $script } $script, @args; } # braces protect against warning

	my $err = "<error> Could not exec $script: $!\n";
	POSIX::write($writefd, $err, length($err));
        exit 2;
    }

    ## parent

    # clean up file descriptors from the fork
    POSIX::close($writefd);

    # mark this object as "busy", so we can't begin another operation
    # until this one is finished.
    $self->{'busy'} = 1;

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

	# mark this object as no longer busy.  This frees the
	# object up to begin the next operation, which may happen
	# during the invocation of the callback
	$self->{'busy'} = 0;

	# handle unexpected exit status as a fatal error
	if (!POSIX::WIFEXITED($child_exit_status) || POSIX::WEXITSTATUS($child_exit_status) > 2) {
	    $failure_cb->(POSIX::WEXITSTATUS($child_exit_status),
		"Fatal error from changer script: ".$child_output);
	    return;
	}

	# parse the child's output
	my @child_output = split '\n', $child_output;
	if (@child_output < 1) {
	    $failure_cb->(2, "Malformed output from changer script -- no output");
	    return;
	}
	if (@child_output > 1) {
	    $failure_cb->(2, "Malformed output from changer script -- too many lines");
	    return;
	}
	if ($child_output[0] !~ /\s*([^\s]+)(?:\s+(.+))?/) {
	    $failure_cb->(2, "Malformed output from changer script: '$child_output[0]'");
	    return;
	}
	my ($slot, $rest) = ($1, $2);

	# let the callback take care of any further interpretation
	my $exitval = POSIX::WEXITSTATUS($child_exit_status);
	if ($exitval == 0) {
	    $success_cb->($slot, $rest);
	} else {
	    $failure_cb->($exitval, $rest);
	}
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
}

package Amanda::Changer::compat::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

use Amanda::Debug qw( :logging );

sub new {
    my $class = shift;
    my ($chg, $slot, $device_name) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;

    $self->{'device_name'} = $device_name;
    $self->{'this_slot'} = $slot;
    $self->{'next_slot'} = "next"; # clever, no?

    # mark the changer as reserved
    $self->{'chg'}->{'reserved'} = $device_name;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;

    my $finished = sub {
	my ($msg) = @_;

	$self->{'chg'}->{'reserved'} = 0;

	if (exists $params{'finished_cb'}) {
	    Amanda::MainLoop::call_later($params{'finished_cb'}, $msg);
	}
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
        if (exists $params{'finished_cb'}) {
            Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
        }
        return;
    }

    my $run_success_cb = sub {
        if (exists $params{'finished_cb'}) {
            Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
        }
    };
    my $run_fail_cb = sub {
	my ($exitval, $message) = @_;
        if (exists $params{'finished_cb'}) {
            Amanda::MainLoop::call_later($params{'finished_cb'}, $message);
        }
    };
    $self->{'chg'}->_run_tpchanger(
        $run_success_cb, $run_fail_cb, "-label", $params{'label'});
}

1;
