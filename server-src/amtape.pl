#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
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
use Amanda::Util qw( :constants match_labelstr );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Taper::Scan;
use Amanda::Recovery::Scan;
use Amanda::Interactivity;
use Amanda::DB::Catalog2;
use Amanda::Message qw( :severity );

my $exit_status = 0;
my $catalog;

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

    my ($storage, $chg) = load_changer($finished_cb) or return;

    $chg->reset(finished_cb => sub {
	    my ($err) = @_;
	    $storage->quit();
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

    my ($storage, $chg) = load_changer($finished_cb) or return;

    if (@args) {
	@drive_args = (drive => shift @args);
    }
    $chg->eject(@drive_args,
	finished_cb => sub {
	    my ($err) = @_;
	    $storage->quit();
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

    my ($storage, $chg) = load_changer($finished_cb) or return;

    if (@args == 1) {
	@drive_args = (drive => shift @args);
    } elsif (@args != 0) {
	return usage($finished_cb);
    }

    $chg->clean(@drive_args,
	finished_cb => sub {
	    my ($err) = @_;
	    $storage->quit();
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

    if (@args > 1) {
	return usage($finished_cb);
    }

    my $user_msg = sub {
	my $msg = shift;
	print STDERR $msg->message() . "\n";
    };

    my ($storage, $chg) = load_changer($finished_cb) or return;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $storage->quit() if defined $storage;
			  $chg->quit() if defined $chg };

    step start => sub {
	$chg->show(slots => $args[0],
		   user_msg => $user_msg,
		   finished_cb => $finished_cb);
    };
});

subcommand("inventory", "inventory", "show inventory of changer slots",
sub {
    my ($finished_cb, @args) = @_;

    my ($storage, $chg) = load_changer($finished_cb) or return;

    if (@args != 0) {
	return usage($finished_cb);
    }

    # TODO -- support an --xml option

    my $inventory_cb = make_cb(inventory_cb => sub {
	my ($err, $inv) = @_;
	if ($err) {
	    print STDERR "Storage '$storage->{'storage_name'}': ";
	    if ($err->notimpl) {
		if ($err->{'message'}) {
		    print STDERR "inventory not supported by this changer: $err->{'message'}\n";
		} else {
		    print STDERR "inventory not supported by this changer\n";
		}
	    } else {
		print STDERR "$err\n";
	    }

	    $storage->quit();
	    $chg->quit();
	    return $finished_cb->();
	}

	for my $sl (@$inv) {
	    my $line = "Storage '$storage->{'storage_name'}': slot $sl->{slot}:";
	    my $volume;
	    my $meta;
	    if ($sl->{'state'} == Amanda::Changer::SLOT_EMPTY) {
		$line .= " empty";
	    } elsif (!defined($sl->{device_status}) && !defined($sl->{label})) {
		$line .= " unknown state";
	    } else {
		if (defined $sl->{label}) {
		    $line .= " label $sl->{label}";
		    $volume = $catalog->find_volume($storage->{'tapepool'}, $sl->{label});
		    if (defined $volume) {
			if ($volume->{'meta'}) {
				$line .= " ($volume->{'meta'})";
				$meta = $volume->{'meta'};
			}
		    }
		} elsif ($sl->{'device_status'} == $DEVICE_STATUS_VOLUME_UNLABELED) {
		    $line .= " blank";
		} elsif ($sl->{'f_type'} != $Amanda::Header::F_TAPESTART) {
		    $line .= " blank";
		} elsif (!defined $sl->{label}) {
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
	    if ($sl->{'device_status'} != $DEVICE_STATUS_SUCCESS &&
		$sl->{'device_status'} != $DEVICE_STATUS_VOLUME_UNLABELED) {
		if (defined $sl->{'device_error'}) {
		    $line .= " [" . $sl->{'device_error'} . "]";
		} else {
		    $line .= " [device error]";
		}
	    }
	    if ($sl->{'label'}) {
		if (!match_labelstr($storage->{'labelstr'},
				    $storage->{'autolabel'},
				    $sl->{label},
				    $sl->{'barcode'}, $meta,
				    $storage->{'storage_name'})) {
		    $line .= " (label do not match labelstr)";
		}
	    }
	    if (defined $volume) {
		my $retention_type = $volume->retention_type();
		$line .= " [" . Amanda::Config::get_retention_name($retention_type) . "]";
		if (defined $sl->{'barcode'} and
		    defined $volume->{'barcode'} and
		    $sl->{'barcode'} ne $volume->{'barcode'}) {
		    $line .= " MISTMATCH barcode in catalog $volume->{'barcode'}";
		}
	    }

	    # note that inventory goes to stdout
	    print "$line\n";
	}

	$storage->quit();
	$chg->quit();
	$finished_cb->();
    });
    $chg->inventory(inventory_cb => $inventory_cb);
});

subcommand("create", "create", "create the changer root directory",
sub {
    my ($finished_cb, @args) = @_;

    my ($storage, $chg) = load_changer($finished_cb) or return;

    if (@args != 0) {
	return usage($finished_cb);
    }

    # TODO -- support an --xml option

    my $create_cb = make_cb(create => sub {
	my ($err, @results) = @_;
	if ($err) {
	    if ($err->notimpl) {
		if ($err->{'message'}) {
		    print STDERR "create not supported by this changer: $err->{'message'}\n";
		} else {
		    print STDERR "create not supported by this changer\n";
		}
	    } else {
		print STDERR "$err\n";
	    }
	} else {
	    print STDERR "Created\n";
	}
	$storage->quit();
	$chg->quit();
	return $finished_cb->();
    });
    $chg->create(finished_cb => $create_cb);
});

subcommand("verify", "verify", "verify the changer is correctly configured",
sub {
    my ($finished_cb, @args) = @_;

    my ($storage, $chg) = load_changer($finished_cb) or return;

    if (@args != 0) {
	return usage($finished_cb);
    }

    # TODO -- support an --xml option

    my $verify_cb = make_cb(verify => sub {
	my ($err, @results) = @_;
	if ($err) {
	    if ($err->notimpl) {
		if ($err->{'message'}) {
		    print STDERR "verify not supported by this changer: $err->{'message'}\n";
		} else {
		    print STDERR "verify not supported by this changer\n";
		}
	    } else {
		print STDERR "$err\n";
	    }
	} else {
	    foreach my $result (@results) {
		if ($result->isa("Amanda::Message")) {
		    print "GOOD : " if $result->{'code'} == 1100006;
		    print "HINT : " if $result->{'code'} == 1100007;
		    print "ERROR: " if $result->{'code'} == 1100009;
		    print "ERROR: " if $result->{'code'} == 1100024;
		    print "ERROR: " if $result->{'code'} == 1100025;
		}
		print STDERR $result . "\n";
	    }
	}
	$storage->quit();
	$chg->quit();
	return $finished_cb->();
    });
    $chg->verify(finished_cb => $verify_cb);
});

subcommand("current", "current", "load and show the contents of the current slot",
sub {
    my ($finished_cb, @args) = @_;

    return usage($finished_cb) if @args;

    # alias for 'slot current'
    return invoke_subcommand("slot", $finished_cb, "current");
});

subcommand("slot", "slot <slot> [drive <drive>]",
	   "load the volume in slot <slot> in drive <drive>; <slot> can also be 'current', 'next', 'first', or 'last'",
sub {
    my ($finished_cb, @args) = @_;
    my @slotarg;
    my $storage;
    my $chg;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $storage->quit() if defined $storage;
			  $chg->quit() if defined $chg };

    # NOTE: the syntax of this subcommand precludes actual slots named
    # 'current' or 'next' ..  when we have a changer using such slot names,
    # this subcommand will need to support a --literal flag

    return usage($finished_cb) unless (@args == 1 || @args == 3);
    my $slot = shift @args;
    my $drive = shift @args;
    if (defined $drive) {	# check drive keyword
	$drive = shift @args;
    }

    ($storage, $chg) = load_changer($finished_cb) or return;

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
	$chg->load(@slotarg, drive => $drive, set_current => 1,
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
    my $storage;
    my $chg;

    return usage($finished_cb) unless (@args == 1);
    my $label = shift @args;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $scan->quit() if defined $scan;
			  $storage->quit() if defined $storage;
			  $chg->quit() if defined $chg };

    step start => sub {
	my $_user_msg_fn = sub {
	    my $msg = shift;

	    if ($msg->{'code'} == 1200000) {
		printf "slot %3s:", $msg->{'slot'};
	    } elsif ($msg->{'code'} == 1200001) {
		print " " . $msg->message() . "\n";
	    } elsif ($msg->{'code'} == 1200002) {
		print " " . $msg->message() . "\n";
	    } elsif ($msg->{'code'} == 1200003) {
		print " " . $msg->message() . "\n";
	    }
	};

	$interactivity = Amanda::Interactivity->new(name => 'stdin',
						    storage_name => $storage->{'storage_name'},
						    changer_name => $chg->{'chg_name'});
	($storage, $chg) = load_changer($finished_cb) or return;
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
    return usage($finished_cb) unless (@args == 0);
    my $label = shift @args;

    my ($storage, $chg) = load_changer($finished_cb) or return;

    my $taper_user_message_fn = sub {
	my $message = shift;

	print STDERR "$message\n";
    };

    my $interactivity = Amanda::Interactivity->new(name => 'tty',
						   storage_name => $storage->{'storage_name'},
						   changer_name => $chg->{'chg_name'});
    my $scan_name = $chg->{'storage'}->{'taperscan_name'};
    my $taperscan = Amanda::Taper::Scan->new(algorithm => $scan_name,
					     storage => $chg->{'storage'},
					     changer => $chg,
					     catalog  => $catalog);

    my $result_cb = make_cb(result_cb => sub {
	my ($err, $res, $label, $mode) = @_;
	if ($err) {
	    if ($res) {
		$res->release(finished_cb => sub {
		    $storage->quit() if defined $storage;
		    $taperscan->quit() if defined $taperscan;
		    return failure($err, $finished_cb);
		});
		return;
	    } else {
		$storage->quit() if defined $storage;
		$taperscan->quit() if defined $taperscan;
		return failure($err, $finished_cb);
	    }
	}

	my $modestr = ($mode == $ACCESS_APPEND)? "append" : "write";
	my $slot = $res->{'this_slot'};
	if (defined $res->{'device'} and defined $res->{'device'}->volume_label() and $res->{'device'}->volume_label() eq $label) {
	    print STDERR "Will $modestr to volume '$label' in slot $slot.\n";
	} elsif (defined $res->{'device'} and defined $res->{'device'}->volume_label()) {
	    print STDERR "Will $modestr label '$label' to '" . $res->{'device'}->volume_label() . "' labelled volume in slot $slot.\n";
	} else {
	    my $header = $res->{'device'}->volume_header();
	    if (!defined $header || $header->{'type'} == $Amanda::Header::F_WEIRD) {
		print STDERR "Will $modestr label '$label' to non-Amanda volume in slot $slot.\n";
	    } else {
		print STDERR "Will $modestr label '$label' to new volume in slot $slot.\n";
	    }
	}
	$res->release(finished_cb => sub {
	    my ($err) = @_;
	    die "$err" if $err;

	    $storage->quit() if defined $storage;
	    $taperscan->quit() if defined $taperscan;
	    $finished_cb->();
	});
    });

    $taperscan->scan(
	result_cb => $result_cb,
	user_message_fn => $taper_user_message_fn,
    );
});

subcommand("update", "update [WHAT]", "update the changer's state; see changer docs for syntax of WHAT",
sub {
    my ($finished_cb, @args) = @_;
    my @changed_args;
    my $got_success;
    my $got_error;

    my ($storage, $chg) = load_changer($finished_cb) or return;

    if (@args) {
	@changed_args = (changed => shift @args);
    }
    $chg->update(@changed_args,
	user_msg_fn => sub {
	    if ($_[0]->{'code'} == 1100019) {
		print STDERR "$_[0]";
	    } else {
		print STDERR "$_[0]\n";
	    }
	    $got_success++ if $_[0]->{'severity'} eq $Amanda::Message::SUCCESS;
	    $got_error++ if $_[0]->{'severity'} eq $Amanda::Message::ERROR;
	},
	finished_cb => sub {
	    my ($err) = @_;
	    $storage->quit();
	    $chg->quit();
	    return failure($err, $finished_cb) if $err;

	    print STDERR "update complete\n" if $got_success;
	    print STDERR "update failed\n" if !$got_success;
	    $finished_cb->();
	});
});

subcommand("sync-catalog", "sync-catalog [request] [wait]", "sync the catalog whith the devices.",
sub {
    my ($finished_cb, $request, $wait) = @_;

    my ($storage, $chg) = load_changer($finished_cb) or return;

    $chg->sync_catalog(
	request => $request,
	wait => $wait,
	user_msg_fn => sub {
	    print STDERR "$_[0]\n";
	},
	sync_catalog_cb => sub {
	    my ($err) = @_;
	    $storage->quit();
	    $chg->quit();
	    return failure($err, $finished_cb) if $err;

	    print STDERR "sync-catalog complete\n";
	    $finished_cb->();
	});
});

##
# Utilities

sub load_changer {
    my ($finished_cb) = @_;

    my $storage  = Amanda::Storage->new(catalog => $catalog);
    return failure("$storage", $finished_cb) if $storage->isa("Amanda::Changer::Error");
    my $chg = $storage->{'chg'};
    if ($chg->isa("Amanda::Changer::Error")) {
	$storage->quit();
	return failure($chg, $finished_cb);
    }
    return ($storage, $chg );
}

sub failure {
    my ($msg, $finished_cb) = @_;
    if ($msg->isa("Amanda::Changer::Error") and defined $msg->{'slot'}) {
	print STDERR "ERROR: Slot: $msg->{'slot'}: $msg\n";
    } else {
	print STDERR "ERROR: $msg\n";
    }
    $exit_status = 1;
    $finished_cb->();
}

# show the slot contents in the old-fashioned format
sub show_slot {
    my ($res) = @_;

    if (defined $res->{'chg'}->{'storage'}) {
	print STDERR "Storage '$res->{'chg'}->{'storage'}->{'storage_name'}': ";
    }
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

Amanda::Util::setup_application("amtape", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

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
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

$catalog = Amanda::DB::Catalog2->new();

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
$catalog->quit() if defined $catalog;
Amanda::Util::finish_application();
exit($exit_status);
