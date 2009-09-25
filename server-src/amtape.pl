#! @PERL@
# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;

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

my $exit_status = 0;

##
# Subcommand handling

my %subcommands;

sub usage {
    print STDERR <<EOF;
Usage: amtape <conf> <command> {<args>} [-o configoption]*
  Valid commands are:
EOF
    local $Text::Wrap::columns = 80 - 20;
    for my $subcmd (sort keys %subcommands) {
	my ($syntax, $descr, $code) = @{$subcommands{$subcmd}};
	$descr = wrap('', ' ' x 20, $descr);
	printf("    %-15s %s\n", $syntax, $descr);
    }
    exit(1);
}

sub subcommand($$$&) {
    my ($subcmd, $syntax, $descr, $code) = @_;

    $subcommands{$subcmd} = [ $syntax, $descr, make_cb($subcmd => $code) ];
}

sub invoke_subcommand {
    my ($subcmd, @args) = @_;
    die "invalid subcommand $subcmd" unless exists $subcommands{$subcmd};

    $subcommands{$subcmd}->[2]->(@args);
}

##
# subcommands

subcommand("usage", "usage", "this message",
sub {
    my @args = @_;

    usage();
});

subcommand("reset", "reset", "reset changer to known state",
sub {
    my @args = @_;
    my %subs;

    my $chg = load_changer() or return;

    $chg->reset(finished_cb => sub {
	    my ($err) = @_;
	    return failure($err) if $err;

	    print STDERR "changer is reset\n";
	    Amanda::MainLoop::quit();
	});
});

subcommand("eject", "eject [<drive>]", "eject the volume in the specified drive",
sub {
    my @args = @_;
    my %subs;
    my @drive_args;

    my $chg = load_changer() or return;

    if (@args) {
	@drive_args = (drive => shift @args);
    }
    $chg->eject(@drive_args,
	finished_cb => sub {
	    my ($err) = @_;
	    return failure($err) if $err;

	    print STDERR "drive ejected\n";
	    Amanda::MainLoop::quit();
	});
});

subcommand("clean", "clean [<drive>]", "clean a drive in the changer",
sub {
    my @args = @_;
    my %subs;
    my @drive_args;

    my $chg = load_changer() or return;

    if (@args == 1) {
	@drive_args = (drive => shift @args);
    } elsif (@args != 0) {
	return usage();
    }

    $chg->clean(@drive_args,
	finished_cb => sub {
	    my ($err) = @_;
	    return failure($err) if $err;

	    print STDERR "drive cleaned\n";
	    Amanda::MainLoop::quit();
	});
});

subcommand("show", "show", "scan all slots in the changer, starting with the current slot",
sub {
    my @args = @_;
    my %subs;
    my $last_slot;
    my %seen_slots;

    if (@args != 0) {
	return usage();
    }

    my $chg = load_changer() or return;

    $subs{'start'} = sub {
	$chg->info(info => [ 'num_slots' ], info_cb => $subs{'info_cb'});
    };

    $subs{'info_cb'} = sub {
	my ($err, %info) = @_;
	return failure($err) if $err;

	print STDERR "amtape: scanning all $info{num_slots} slots in changer:\n";

	$subs{'load_current'}->();
    };

    $subs{'load_current'} = sub {
	$chg->load(relative_slot => 'current', mode => "read", res_cb => $subs{'loaded'});
    };

    $subs{'loaded'} = sub {
	my ($err, $res) = @_;
	if ($err) {
	    if ($err->notfound) {
		# no more interesting slots
		Amanda::MainLoop::quit();
		return;
	    } elsif ($err->inuse and defined $err->{'slot'}) {
		$last_slot = $err->{'slot'};
	    } else {
		return failure($err) if $err;
	    }
	} else {
	    $last_slot = $res->{'this_slot'};
	}

	$seen_slots{$last_slot} = 1;

	if ($res) {
	    my $dev = $res->{'device'};
	    if (!defined $dev->volume_label) {
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
	    }
	} else {
	    print STDERR sprintf("slot %3s: in use\n", $last_slot);
	}

	if ($res) {
	    $res->release(finished_cb => $subs{'released'});
	} else {
	    $subs{'released'}->();
	}
    };

    $subs{'released'} = sub {
	$chg->load(relative_slot => 'next', slot => $last_slot,
		   except_slots => { %seen_slots }, res_cb => $subs{'loaded'});
    };

    $subs{'start'}->();
});

subcommand("inventory", "inventory", "show inventory of changer slots",
sub {
    my @args = @_;
    my %subs;

    my $chg = load_changer() or return;

    if (@args != 0) {
	return usage();
    }

    # TODO -- support an --xml option

    my $inventory_cb = make_cb(inventory_cb => sub {
	my ($err, $inv) = @_;
	if ($err) {
	    if ($err->notimpl) {
		print STDERR "inventory not supported by this changer\n";
	    } else {
		print STDERR "$err\n";
	    }

	    Amanda::MainLoop::quit();
	    return;
	}

	for my $sl (@$inv) {
	    my $line = "slot $sl->{slot}:";
	    if ($sl->{'empty'}) {
		$line .= " empty";
	    } else {
		if ($sl->{'label'}) {
		    $line .= " label $sl->{label}";
		} elsif (defined $sl->{'label'}) {
		    $line .= " blank";
		} else {
		    $line .= " unknown";
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
	    }

	    # note that inventory goes to stdout
	    print "$line\n";
	}

	Amanda::MainLoop::quit();
    });
    $chg->inventory(inventory_cb => $inventory_cb);
});

subcommand("current", "current", "load and show the contents of the current slot",
sub {
    # alias for 'slot current'
    return invoke_subcommand("slot", "current");
});

subcommand("slot", "slot <slot>",
	   "load the volume in slot <slot>; <slot> can also be 'current', 'next', 'first', or 'last'",
sub {
    my @args = @_;
    my @slotarg;
    my %subs;

    # NOTE: the syntax of this subcommand precludes actual slots named
    # 'current' or 'next' ..  when we have a changer using such slot names,
    # this subcommand will need to support a --literal flag

    usage unless (@args == 1);
    my $slot = shift @args;

    my $chg = load_changer() or return;

    $subs{'get_slot'} = make_cb(get_slot => sub {
	if ($slot eq 'current' or $slot eq 'next') {
	    @slotarg = (relative_slot => $slot);
	} elsif ($slot eq 'first' or $slot eq 'last') {
	    return $chg->inventory(inventory_cb => $subs{'inventory_cb'});
	} else {
	    @slotarg = (slot => $slot);
	}

	$subs{'do_load'}->();
    });

    $subs{'inventory_cb'} = make_cb(inventory_cb => sub {
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

	$subs{'do_load'}->();
    });

    $subs{'do_load'} = make_cb(do_load => sub {
	$chg->load(@slotarg, set_current => 1,
	    res_cb => $subs{'done_load'});
    });

    $subs{'done_load'} = make_cb(done_load => sub {
	my ($err, $res) = @_;
	return failure($err) if ($err);

	show_slot($res);
	my $gotslot = $res->{'this_slot'};
	print STDERR "changed to slot $gotslot\n";

	$res->release(finished_cb => $subs{'released'});
    });

    $subs{'released'} = make_cb(released => sub {
	my ($err) = @_;
	return failure($err) if ($err);

	Amanda::MainLoop::quit();
    });

    $subs{'get_slot'}->();
});

subcommand("label", "label <label>", "load the volume with label <label>",
sub {
    my @args = @_;
    my %subs;

    usage unless (@args == 1);
    my $label = shift @args;

    my $chg = load_changer() or return;

    $subs{'do_load'} = make_cb(do_load => sub {
	$chg->load(label => $label, set_current => 1,
	    res_cb => $subs{'done_load'});
    });

    $subs{'done_load'} = make_cb(done_load => sub {
	my ($err, $res) = @_;
	return failure($err) if ($err);

	my $gotslot = $res->{'this_slot'};
	my $devname = $res->{'device'}->device_name;
	show_slot($res);
	print STDERR "label $label is now loaded from slot $gotslot\n";

	$res->release(finished_cb => $subs{'released'});
    });

    $subs{'released'} = make_cb(released => sub {
	my ($err) = @_;
	return failure($err) if ($err);

	Amanda::MainLoop::quit();
    });

    $subs{'do_load'}->();
});

subcommand("taper", "taper", "perform the taperscan algorithm and display the result",
sub {
    my @args = @_;

    usage unless (@args == 0);
    my $label = shift @args;

    my $chg = load_changer() or return;

    my $result_cb = make_cb(result_cb => sub {
	my ($err, $res, $label, $mode) = @_;
	return failure($err) if $err;

	my $modestr = ($mode == $ACCESS_APPEND)? "append" : "write";
	my $slot = $res->{'this_slot'};
	print STDERR "Will $modestr to volume $label in slot $slot.\n";
	$res->release(finished_cb => sub {
	    Amanda::MainLoop::quit();
	});
    });

    my $taperscan = Amanda::Taper::Scan->new(changer => $chg);
    $taperscan->scan(
	result_cb => $result_cb,
	user_msg_fn => sub {
	    print STDERR "$_[0]\n";
	}
    );
});

subcommand("update", "update [WHAT]", "update the changer's state; see changer docs for syntax of WHAT",
sub {
    my @args = @_;
    my %subs;
    my @changed_args;

    my $chg = load_changer() or return;

    if (@args) {
	@changed_args = (changed => shift @args);
    }
    $chg->update(@changed_args,
	user_msg_fn => sub {
	    print STDERR "$_[0]\n";
	},
	finished_cb => sub {
	    my ($err) = @_;
	    return failure($err) if $err;

	    print STDERR "update complete\n";
	    Amanda::MainLoop::quit();
	});
});

##
# Utilities

sub load_changer {
    my $chg = Amanda::Changer->new();
    return failure($chg) if ($chg->isa("Amanda::Changer::Error"));
    return $chg;
}

sub failure {
    my ($msg) = @_;
    print STDERR "ERROR: $msg\n";
    $exit_status = 1;
    Amanda::MainLoop::quit();
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

    if (!$dev->volume_label and $dev->read_label() != $DEVICE_STATUS_SUCCESS) {
	print STDERR $dev->error_or_status() . "\n";
	return;
    }

    printf STDERR "time %-14s label %s\n", $dev->volume_time, $dev->volume_label;
}

##
# main

sub main {
    Amanda::Util::setup_application("amtape", "server", $CONTEXT_CMDLINE);

    my $config_overrides = new_config_overrides($#ARGV+1);

    Getopt::Long::Configure(qw(bundling));
    GetOptions(
	'help|usage|?' => \&usage,
	'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    ) or usage();

    usage() if (@ARGV < 1);

    my $config_name = shift @ARGV;
    config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
    apply_config_overrides($config_overrides);
    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_WARNINGS) {
	config_print_errors();
	if ($cfgerr_level >= $CFGERR_ERRORS) {
	    die("errors processing config file");
	}
    }

    Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

    my $subcmd = shift @ARGV;
    usage unless defined($subcmd) and exists ($subcommands{$subcmd});
    invoke_subcommand($subcmd, @ARGV);
    Amanda::MainLoop::run();
    exit($exit_status);
}

main();
