#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use File::Basename;
use Getopt::Long;
use Text::Wrap;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Taper::Scan;
use Amanda::Recovery::Scan;
use Amanda::Interactivity;
use Amanda::Tapelist;

my $exit_status = 0;
my $tl;

##
# Subcommand handling

my %subcommands;

sub usage {
    my ($finished_cb) = @_;

    $finished_cb = sub { exit(1); } if (!$finished_cb or !(ref($finished_cb) eq "CODE"));

    print STDERR <<EOF;
Usage: amtape [-o configoption]* <conf> <command> {<args>}
  Valid commands are:
EOF
    local $Text::Wrap::columns = 80 - 20;
    for my $subcmd (sort keys %subcommands) {
	my ($syntax, $descr, $code) = @{$subcommands{$subcmd}};
	$descr = wrap('', ' ' x 20, $descr);
	printf("    %-15s %s\n", $syntax, $descr);
    }
    $exit_status = 1;
    $finished_cb->();
}

sub subcommand($$$&) {
    my ($subcmd, $syntax, $descr, $code) = @_;

    $subcommands{$subcmd} = [ $syntax, $descr, make_cb($subcmd => $code) ];
}

sub invoke_subcommand {
    my ($subcmd, $finished_cb, @args) = @_;
    die "invalid subcommand $subcmd" unless exists $subcommands{$subcmd};

    $subcommands{$subcmd}->[2]->($finished_cb, @args);
}

##
# subcommands

subcommand("usage", "usage", "this message",
sub {
    my ($finished_cb, @args) = @_;

    return usage($finished_cb);
});

subcommand("reset", "reset", "reset changer to known state",
sub {
    my ($finished_cb, @args) = @_;

    my $chg = load_changer($finished_cb) or return;

    $chg->reset(finished_cb => sub {
	    my ($err) = @_;
	    $chg->quit();
	    return failure($err, $finished_cb) if $err;

	    print STDERR "changer is reset\n";
	    $finished_cb->();
	});
});

subcommand("eject", "eject [<drive>]", "eject the volume in the specified drive",
sub {
    my ($finished_cb, @args) = @_;
    my @drive_args;

    my $chg = load_changer($finished_cb) or return;

    if (@args) {
	@drive_args = (drive => shift @args);
    }
    $chg->eject(@drive_args,
	finished_cb => sub {
	    my ($err) = @_;
	    $chg->quit();
	    return failure($err, $finished_cb) if $err;

	    print STDERR "drive ejected\n";
	    $finished_cb->();
	});
});

subcommand("clean", "clean [<drive>]", "clean a drive in the changer",
sub {
    my ($finished_cb, @args) = @_;
    my @drive_args;

    my $chg = load_changer($finished_cb) or return;

    if (@args == 1) {
	@drive_args = (drive => shift @args);
    } elsif (@args != 0) {
	return usage($finished_cb);
    }

    $chg->clean(@drive_args,
	finished_cb => sub {
	    my ($err) = @_;
	    $chg->quit();
	    return failure($err, $finished_cb) if $err;

	    print STDERR "drive cleaned\n";
	    $finished_cb->();
	});
});

subcommand("show", "show [<slots>]", "scan all slots (or listed slots) in the changer, starting with the current slot",
sub {
    my ($finished_cb, @args) = @_;
    my $last_slot;
    my %seen_slots;
    my $chg;

    if (@args > 1) {
	return usage($finished_cb);
    }

    my $what = $args[0];
    my @slots;

    if (defined $what) {
	my @what1 = split /,/, $what;
	foreach my $what1 (@what1) {
	    if ($what1 =~ /^(\d*)-(\d*)$/) {
		my $begin = $1;
		my $end = $2;
		$end = $begin if $begin > $end;
		while ($begin <= $end) {
		    push @slots, $begin;
		    $begin++;
		}
	    } else {
		push @slots, $what1;
	    }
	}
    }

    my $use_slots = @slots > 0;

    $chg = load_changer($finished_cb) or return;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg };

    step start => sub {
	$chg->info(info => [ 'num_slots' ], info_cb => $steps->{'info_cb'});
    };

    step info_cb => sub {
	my ($err, %info) = @_;
	return failure($err, $finished_cb) if $err;

	if ($use_slots) {
	   my $slot = shift @slots;
	   $chg->load(slot => $slot,
		      mode => "read",
		      res_cb => $steps->{'loaded'});

	} else {
	    print STDERR "amtape: scanning all $info{num_slots} slots in changer:\n";

	    $chg->load(relative_slot => 'current',
		       mode => "read",
		       res_cb => $steps->{'loaded'});
	}
    };

    step loaded => sub {
	my ($err, $res) = @_;
	if ($err) {
	    if ($err->notfound) {
		# no more interesting slots
		$finished_cb->();
		return;
	    } elsif ($err->volinuse and defined $err->{'slot'}) {
		$last_slot = $err->{'slot'};
	    } else {
		return failure($err, $finished_cb) if $err;
	    }
	} else {
	    $last_slot = $res->{'this_slot'};
	}

	$seen_slots{$last_slot} = 1;

	if ($res) {
	    my $dev = $res->{'device'};
	    my $st = $dev->read_label();
	    if ($st == $DEVICE_STATUS_SUCCESS) {
		print STDERR sprintf("slot %3s: date %-14s label %s\n",
			$last_slot, $dev->volume_time(),
			$dev->volume_label());
	    } elsif ($st == $DEVICE_STATUS_VOLUME_UNLABELED) {
		print STDERR sprintf("slot %3s: unlabeled volume\n", $last_slot);
	    } else {
		print STDERR sprintf("slot %3s: %s\n", $last_slot, $dev->error_or_status());
	    }
	} else {
	    print STDERR sprintf("slot %3s: in use\n", $last_slot);
	}

	if ($res) {
	    $res->release(finished_cb => $steps->{'released'});
	} else {
	    $steps->{'released'}->();
	}
    };

    step released => sub {
	if ($use_slots) {
	   return $finished_cb->() if @slots == 0;
	   my $slot = shift @slots;
	   $chg->load(slot => $slot,
		      mode => "read",
		      res_cb => $steps->{'loaded'});

	} else {
	    $chg->load(relative_slot => 'next',
		       slot => $last_slot,
		       except_slots => { %seen_slots },
		       res_cb => $steps->{'loaded'});
	}
    };
});

subcommand("inventory", "inventory", "show inventory of changer slots",
sub {
    my ($finished_cb, @args) = @_;

    my $chg = load_changer($finished_cb) or return;

    if (@args != 0) {
	return usage($finished_cb);
    }

    # TODO -- support an --xml option

    my $inventory_cb = make_cb(inventory_cb => sub {
	my ($err, $inv) = @_;
	if ($err) {
	    if ($err->notimpl) {
		if ($err->{'message'}) {
		    print STDERR "inventory not supported by this changer: $err->{'message'}\n";
		} else {
		    print STDERR "inventory not supported by this changer\n";
		}
	    } else {
		print STDERR "$err\n";
	    }

	    $chg->quit();
	    return $finished_cb->();
	}

	for my $sl (@$inv) {
	    my $line = "slot $sl->{slot}:";
	    if (!defined($sl->{device_status}) && !defined($sl->{label})) {
		$line .= " unknown state";
	    } elsif ($sl->{'state'} == Amanda::Changer::SLOT_EMPTY) {
		$line .= " empty";
	    } else {
		if (defined $sl->{label}) {
		    $line .= " label $sl->{label}";
		    my $tle = $tl->lookup_tapelabel($sl->{label});
		    if ($tle->{'meta'}) {
			$line .= " ($tle->{'meta'})";
		    }
		} elsif ($sl->{'device_status'} == $DEVICE_STATUS_VOLUME_UNLABELED) {
		    $line .= " blank";
		} elsif ($sl->{'device_status'} != $DEVICE_STATUS_SUCCESS) {
		    if (defined $sl->{'device_error'}) {
			$line .= " " . $sl->{'device_error'};
		    } else {
			$line .= "device error";
		    }
		} elsif ($sl->{'f_type'} != $Amanda::Header::F_TAPESTART) {
		    $line .= " blank";
		} else {
		    $line .= " unknown";
		}
	    }
	    if ($sl->{'barcode'}) {
		$line .= " barcode $sl->{barcode}";
	    }
	    if ($sl->{'reserved'}) {
		$line .= " reserved";
	    }
	    if (defined $sl->{'loaded_in'}) {
		$line .= " (in drive $sl->{'loaded_in'})";
	    }
	    if ($sl->{'import_export'}) {
		$line .= " (import/export slot)";
	    }
	    if ($sl->{'current'}) {
		$line .= " (current)";
	    }

	    # note that inventory goes to stdout
	    print "$line\n";
	}

	$chg->quit();
	$finished_cb->();
    });
    $chg->inventory(inventory_cb => $inventory_cb);
});

subcommand("current", "current", "load and show the contents of the current slot",
sub {
    my ($finished_cb, @args) = @_;

    return usage($finished_cb) if @args;

    # alias for 'slot current'
    return invoke_subcommand("slot", $finished_cb, "current");
});

subcommand("slot", "slot <slot>",
	   "load the volume in slot <slot>; <slot> can also be 'current', 'next', 'first', or 'last'",
sub {
    my ($finished_cb, @args) = @_;
    my @slotarg;
    my $chg;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg };

    # NOTE: the syntax of this subcommand precludes actual slots named
    # 'current' or 'next' ..  when we have a changer using such slot names,
    # this subcommand will need to support a --literal flag

    return usage($finished_cb) unless (@args == 1);
    my $slot = shift @args;

    $chg = load_changer($finished_cb) or return;

    step get_slot => sub {
	if ($slot eq 'current' or $slot eq 'next') {
	    @slotarg = (relative_slot => $slot);
	} elsif ($slot eq 'first' or $slot eq 'last') {
	    return $chg->inventory(inventory_cb => $steps->{'inventory_cb'});
	} else {
	    @slotarg = (slot => $slot);
	}

	$steps->{'do_load'}->();
    };

    step inventory_cb => sub {
	my ($err, $inv) = @_;
	if ($err) {
	    if ($err->failed and $err->notimpl) {
		return failed("This changer does not support special slot '$slot'");
	    } else {
		return failed($err);
	    }
	}

	if ($slot eq 'first') {
	    @slotarg = (slot => $inv->[0]->{'slot'});
	} else {
	    @slotarg = (slot => $inv->[-1]->{'slot'});
	}

	$steps->{'do_load'}->();
    };

    step do_load => sub {
	$chg->load(@slotarg, set_current => 1,
	    res_cb => $steps->{'done_load'});
    };

    step done_load => sub {
	my ($err, $res) = @_;
	return failure($err, $finished_cb) if ($err);

	show_slot($res);
	my $gotslot = $res->{'this_slot'};
	print STDERR "changed to slot $gotslot\n";

	$res->release(finished_cb => $steps->{'released'});
    };

    step released => sub {
	my ($err) = @_;
	return failure($err, $finished_cb) if ($err);

	$finished_cb->();
    };
});

subcommand("label", "label <label>", "load the volume with label <label>",
sub {
    my ($finished_cb, @args) = @_;
    my $interactivity;
    my $scan;
    my $chg;

    return usage($finished_cb) unless (@args == 1);
    my $label = shift @args;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $scan->quit() if defined $scan;
			  $chg->quit() if defined $chg };

    step start => sub {
	my $_user_msg_fn = sub {
	    my %params = @_;

	    if (exists($params{'scan_slot'})) {
		print "slot $params{'slot'}:";
	    } elsif (exists($params{'slot_result'})) {
		if (defined($params{'err'})) {
		    print " $params{'err'}\n";
		} else { # res must be defined
		    my $res = $params{'res'};
		    my $dev = $res->{'device'};
		    if ($dev->status == $DEVICE_STATUS_SUCCESS) {
			my $volume_label = $res->{device}->volume_label;
			print " $volume_label\n";
		    } else {
			my $errmsg = $res->{device}->error_or_status();
			print " $errmsg\n";
		    }
		}
	    }
	};

	$interactivity = Amanda::Interactivity->new(name => 'stdin');
	$chg = load_changer($finished_cb) or return;
	$scan = Amanda::Recovery::Scan->new(chg => $chg,
					    interactivity => $interactivity);
	return failure("$scan", $finished_cb)
	    if ($scan->isa("Amanda::Changer::Error"));

	$scan->find_volume(label  => $label,
			   res_cb => $steps->{'done_load'},
			   user_msg_fn => $_user_msg_fn,
			   set_current => 1);
    };

    step done_load => sub {
	my ($err, $res) = @_;
	return failure($err, $finished_cb) if ($err);

	my $gotslot = $res->{'this_slot'};
	my $devname = $res->{'device'}->device_name;
	show_slot($res);
	print STDERR "label $label is now loaded from slot $gotslot\n";

	$res->release(finished_cb => $steps->{'released'});
    };

    step released => sub {
	my ($err) = @_;
	return failure($err, $finished_cb) if ($err);

	$finished_cb->();
    };
});

subcommand("taper", "taper", "perform the taperscan algorithm and display the result",
sub {
    my ($finished_cb, @args) = @_;

    my $taper_user_msg_fn = sub {
	my %params = @_;
	if (exists($params{'text'})) {
	    print STDERR "$params{'text'}\n";
	} elsif (exists($params{'scan_slot'})) {
	    print STDERR "slot $params{'slot'}:";
	} elsif (exists($params{'search_label'})) {
	    print STDERR "Searching for label '$params{'label'}':";
	} elsif (exists($params{'slot_result'}) ||
		 exists($params{'search_result'})) {
	    if (defined($params{'err'})) {
		if (exists($params{'search_result'}) &&
		    defined($params{'err'}->{'slot'})) {
		    print STDERR "slot $params{'err'}->{'slot'}:";
		}
		print STDERR " $params{'err'}\n";
	    } else { # res must be defined
		my $res = $params{'res'};
		my $dev = $res->{'device'};
		if (exists($params{'search_result'})) {
		    print STDERR " found in slot $res->{'this_slot'}:";
		}
		if ($dev->status == $DEVICE_STATUS_SUCCESS) {
		    my $volume_label = $res->{device}->volume_label;
		    if ($params{'active'}) {
			print STDERR " volume '$volume_label' is still active and cannot be overwritten\n";
		    } elsif ($params{'does_not_match_labelstr'}) {
			print STDERR " volume '$volume_label' does not match labelstr '$params{'labelstr'}'\n";
		    } elsif ($params{'not_in_tapelist'}) {
			print STDERR " volume '$volume_label' is not in the tapelist\n"
		    } else {
			print STDERR " volume '$volume_label'\n";
		    }
		} elsif ($dev->status & $DEVICE_STATUS_VOLUME_UNLABELED and
			 $dev->volume_header and
			 $dev->volume_header->{'type'} == $Amanda::Header::F_EMPTY) {
		    print STDERR " contains an empty volume\n";
		} elsif ($dev->status & $DEVICE_STATUS_VOLUME_UNLABELED and
			 $dev->volume_header and
			 $dev->volume_header->{'type'} == $Amanda::Header::F_WEIRD) {
		    my $autolabel = getconf($CNF_AUTOLABEL);
		    if ($autolabel->{'non_amanda'}) {
			print STDERR " contains a non-Amanda volume\n";
		    } else {
			print STDERR " contains a non-Amanda volume; check and relabel it with 'amlabel -f'\n";
		    }
		} elsif ($dev->status & $DEVICE_STATUS_VOLUME_ERROR) {
		    my $message = $dev->error_or_status();
		    print STDERR " can't read label: $message\n";
		} else {
		    my $errmsg = $res->{device}->error_or_status();
		    print STDERR " $errmsg\n";
		}
	    }
	} else {
	    print STDERR "UNKNOWN\n";
	}
    };

    return usage($finished_cb) unless (@args == 0);
    my $label = shift @args;

    my $chg = load_changer($finished_cb) or return;
    my $interactivity = Amanda::Interactivity->new(name => 'tty');
    my $scan_name = getconf($CNF_TAPERSCAN);
    my $taperscan = Amanda::Taper::Scan->new(algorithm => $scan_name,
					     changer => $chg,
					     tapelist => $tl);

    my $result_cb = make_cb(result_cb => sub {
	my ($err, $res, $label, $mode) = @_;
	if ($err) {
	    if ($res) {
		$res->release(finished_cb => sub {
		    $taperscan->quit() if defined $taperscan;
		    return failure($err, $finished_cb);
		});
		return;
	    } else {
		$taperscan->quit() if defined $taperscan;
		return failure($err, $finished_cb);
	    }
	}

	my $modestr = ($mode == $ACCESS_APPEND)? "append" : "write";
	my $slot = $res->{'this_slot'};
	if (defined $res->{'device'} and defined $res->{'device'}->volume_label()) {
	    print STDERR "Will $modestr to volume '$label' in slot $slot.\n";
	} else {
	    my $header = $res->{'device'}->volume_header();
	    if ($header->{'type'} == $Amanda::Header::F_WEIRD) {
		print STDERR "Will $modestr label '$label' to non-Amanda volume in slot $slot.\n";
	    } else {
		print STDERR "Will $modestr label '$label' to new volume in slot $slot.\n";
	    }
	}
	$res->release(finished_cb => sub {
	    my ($err) = @_;
	    die "$err" if $err;

	    $taperscan->quit() if defined $taperscan;
	    $finished_cb->();
	});
    });

    $taperscan->scan(
	result_cb => $result_cb,
	user_msg_fn => $taper_user_msg_fn,
    );
});

subcommand("update", "update [WHAT]", "update the changer's state; see changer docs for syntax of WHAT",
sub {
    my ($finished_cb, @args) = @_;
    my @changed_args;

    my $chg = load_changer($finished_cb) or return;

    if (@args) {
	@changed_args = (changed => shift @args);
    }
    $chg->update(@changed_args,
	user_msg_fn => sub {
	    print STDERR "$_[0]\n";
	},
	finished_cb => sub {
	    my ($err) = @_;
	    $chg->quit();
	    return failure($err, $finished_cb) if $err;

	    print STDERR "update complete\n";
	    $finished_cb->();
	});
});

##
# Utilities

sub load_changer {
    my ($finished_cb) = @_;

    my $chg = Amanda::Changer->new(undef, tapelist => $tl);
    return failure($chg, $finished_cb) if ($chg->isa("Amanda::Changer::Error"));
    return $chg;
}

sub failure {
    my ($msg, $finished_cb) = @_;
    print STDERR "ERROR: $msg\n";
    $exit_status = 1;
    $finished_cb->();
}

# show the slot contents in the old-fashioned format
sub show_slot {
    my ($res) = @_;

    printf STDERR "slot %3s: ", $res->{'this_slot'};
    my $dev = $res->{'device'};
    if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	print STDERR "Could not open device: "
		. $dev->error_or_status() . "\n";
	return;
    }

    printf STDERR "time %-14s label %s\n", $dev->volume_time, $dev->volume_label;
}

##
# main

Amanda::Util::setup_application("amtape", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

usage() if (@ARGV < 1);

my $config_name = shift @ARGV;
set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
$tl = Amanda::Tapelist->new($tlf);

#make STDOUT not line buffered
my $previous_fh = select(STDOUT);
$| = 1;
select($previous_fh);

sub main {
    my ($finished_cb) = @_;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	my $subcmd = shift @ARGV;
	return usage($finished_cb) unless defined($subcmd) and exists ($subcommands{$subcmd});
	invoke_subcommand($subcmd, $finished_cb, @ARGV);
    }
}

main(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit($exit_status);
