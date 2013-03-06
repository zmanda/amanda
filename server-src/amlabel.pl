#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
use Amanda::Util qw( :constants match_labelstr );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header qw( :constants );
use Amanda::MainLoop;
use Amanda::Tapelist;

my $exit_status = 0;

##
# Subcommand handling

my %subcommands;

sub usage {
    print STDERR "Usage: amlabel [--barcode <barcode>] [--meta <meta>] [--pool <pool>]\n"
	       . "               [--storage] [--assign] [--version] [-f] [-o configoption]*\n"
	       . " <conf> [<label>] [slot <slot-number>]\n";
    exit(1);
}

Amanda::Util::setup_application("amlabel", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);
my ($opt_force, $opt_config, $opt_slot, $opt_label);
my ($opt_barcode, $opt_meta, $opt_assign, $opt_pool, $opt_storage);

$opt_force = 0;
$opt_barcode = undef;
$opt_meta = undef;
$opt_assign = undef;
$opt_pool = undef;
$opt_storage = undef;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'o=s'        => sub { add_config_override_opt($config_overrides, $_[1]); },
    'f'          => \$opt_force,
    'barcode=s'  => \$opt_barcode,
    'meta=s'     => \$opt_meta,
    'pool=s'     => \$opt_pool,
    'storage=s'  => \$opt_storage,
    'assign'     => \$opt_assign,
    'version'    => \&Amanda::Util::version_opt,
) or usage();

usage() if @ARGV == 0;
$opt_config = $ARGV[0];
if (@ARGV == 1) {
    $opt_slot = undef;
    $opt_label = undef;
} elsif (@ARGV == 2) {
    $opt_slot = undef;
    $opt_label = $ARGV[1];
} elsif (@ARGV == 3 and $ARGV[1] eq 'slot') {
    $opt_slot = $ARGV[2];
    $opt_label = undef;
} elsif (@ARGV == 4 and $ARGV[2] eq 'slot') {
    $opt_slot = $ARGV[3];
    $opt_label = $ARGV[1];
} else {
    usage();
}

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	print STDERR "errors processing config file";
	exit 1;
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my ($tlf, $tl, $res);

sub failure {
    my ($msg, $finished_cb) = @_;
    print STDERR "$msg\n";
    $exit_status = 1;
    if ($res) {
	$res->release(finished_cb => sub {
	    # ignore error
	    $finished_cb->()
	});
    } else {
	$finished_cb->();
    }
}

sub main {
    my ($finished_cb) = @_;
    my $gerr;
    my $chg;
    my $dev;
    my $dev_ok;
    my $labelstr;
    my $autolabel;
    my $tapepool;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg };

    step start => sub {
	$tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	$tl = Amanda::Tapelist->new($tlf);
	if (!defined $tl) {
	    return failure("Can't load tapelist file ($tlf)", $finished_cb);
	}

	my $storage  = Amanda::Storage->new(tapelist => $tl);
	return failure("$storage", $finished_cb) if $storage->isa("Amanda::Changer::Error");
	$chg = $storage->{'chg'};
	return failure($chg, $finished_cb) if $chg->isa("Amanda::Changer::Error");

	$labelstr = $storage->{'labelstr'};
	$autolabel = $storage->{'autolabel'};
	$tapepool = $storage->{'tapepool'};

	if ($opt_assign) {
	    return $steps->{'assign'}->();
	}

	if (defined($opt_label) && !$opt_force) {
	    if ($tl->lookup_tapelabel($opt_label)) {
		return failure("Label '$opt_label' already on a volume", $finished_cb);
	    }
	}

	$steps->{'load'}->();
    };

    step load => sub {
	print "Reading label...\n";
	if ($opt_slot) {
	    $chg->load(slot => $opt_slot, mode => "write",
		       res_cb => $steps->{'loaded'});
	} elsif ($opt_barcode) {
	    $chg->inventory(inventory_cb => $steps->{'inventory'});
	} else {
	    $chg->load(relative_slot => "current", mode => "write",
		       res_cb => $steps->{'loaded'});
	}
    };

    step inventory => sub {
	my ($err, $inv) = @_;

	return failure($err, $finished_cb) if $err;

	for my $sl (@$inv) {
	    if ($sl->{'barcode'} eq $opt_barcode) {
		return $chg->load(slot => $sl->{'slot'}, mode => "write",
				  res_cb => $steps->{'loaded'});
	    }
	}

	return failure("No volume with barcode '$opt_barcode' available", $finished_cb);
    };

    step loaded => sub {
	(my $err, $res) = @_;

	return failure($err, $finished_cb) if $err;

	if (defined $opt_slot && defined $opt_barcode &&
	    $opt_barcode ne $res->{'barcode'}) {
	    if (defined $res->{'barcode'}) {
		return failure("Volume in slot $opt_slot have barcode '$res->{'barcode'}, it is not '$opt_barcode'", $finished_cb);
	    } else {
		return failure("Volume in slot $opt_slot have no barcode", $finished_cb);
	    }
	}
	$dev = $res->{'device'};
	$dev_ok = 1;
	if ($dev->status & $DEVICE_STATUS_VOLUME_UNLABELED) {
	    if (!$dev->volume_header or $dev->volume_header->{'type'} == $F_EMPTY) {
		print "Found an empty tape.\n";
	    } else {
		# force is required for non-Amanda tapes
		print "Found a non-Amanda tape.\n";
		$dev_ok = 0 unless ($opt_force);
	    }
	} elsif ($dev->status & $DEVICE_STATUS_VOLUME_ERROR) {
	    # it's OK to force through VOLUME_ERROR
	    print "Error reading volume label: " . $dev->error_or_status(), "\n";
	    $dev_ok = 0 unless ($opt_force);
	} elsif ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    # but anything else is fatal
	    print "Error reading volume label: " . $dev->error_or_status(), "\n";
	    $dev_ok = 0;
	} else {
	    # this is a labeled Amanda tape
	    my $label = $dev->volume_label;
	    my $barcode = $res->{'barcode'};
	    my $meta = $res->{'meta'};
	    my $tle = $tl->lookup_tapelabel($label);

	    if ($opt_label &&
		$labelstr->{'template'} &&
		!match_labelstr($labelstr, $autolabel, $opt_label, $barcode, $meta)) {
		print STDERR "Label '$label' doesn't match the labelstr '$labelstr->{'template'}'\n";
		$dev_ok = 0;
	    } elsif ($tle) {
		if ($tle->{'config'} && $tle->{'config'} ne get_config_name()) {
		    print STDERR "Found label '$label' but it is from config '$tle->{'config'}'\n";
		    $dev_ok = 0;
		} elsif ($tle->{'pool'} &&
			 $tle->{'pool'} ne $tapepool) {
		    print STDERR "Found label '$label' but it is from tape pool '$tle->{'pool'}'\n";
		    $dev_ok = 0;
		} elsif (!$tle->{'pool'} &&
			 !match_labelstr($labelstr, $autolabel, $label, $barcode, $meta)) {
		    print STDERR "Found label '$label' but it doesn't match the labelstr '$labelstr->{'template'}'\n";
		    $dev_ok = 0;
		} else {
		    print "Volume with label '$label' is active and contains data from this configuration.\n";
		    if ($opt_force) {
			# if -f, then the user should clean things up..
			print "Consider using 'amrmtape' to remove volume '$label' from the catalog.\n";
			# note that we don't run amrmtape automatically, as it could result in data loss when
			# multiple volumes have (perhaps accidentally) the same label
		    } else {
			$dev_ok = 0
		    }
		}
	    } else {
		print "Found label '$label' but it is not in the tapelist file.\n";
		$dev_ok = 0 if !$opt_force;
	    }
	}

	$res->get_meta_label(finished_cb => $steps->{'got_meta'});
    };

    step got_meta => sub {
	my ($err, $meta) = @_;

	if (defined $err) {
	    return failure($err, $finished_cb);
	}
	if (defined $meta && defined $opt_meta && $meta ne $opt_meta) {
	    return failure("Device meta '$meta' is not the same as the --meta argument '$opt_meta'", $finished_cb);
	}
	$meta = $opt_meta if !defined $meta;
	($meta, my $merr) = $res->make_new_meta_label() if !defined $meta;
	if (defined $merr) {
	    return failure($merr, $finished_cb);
	}
	$opt_meta = $meta;

	my $label = $opt_label;
	if (!defined($label)) {
	    ($label, my $lerr) = $res->make_new_tape_label(meta => $meta);
	    if (defined $lerr) {
		return failure($lerr, $finished_cb);
	    }
	}

	if ($dev_ok) {
	    print "Writing label '$label'...\n";

	    if (!$dev->start($ACCESS_WRITE, $label, "X")) {
		return failure("Error writing label: " . $dev->error_or_status(), $finished_cb);
	    } elsif (!$dev->finish()) {
		return failure("Error finishing device: " . $dev->error_or_status(), $finished_cb);
	    }

	    print "Checking label...\n";
	    my $status = $dev->read_label();
	    if ($status != $DEVICE_STATUS_SUCCESS) {
		return failure("Checking the tape label failed: " . $dev->error_or_status(),
			$finished_cb);
	    } elsif (!$dev->volume_label) {
		return failure("No label found.", $finished_cb);
	    } elsif ($dev->volume_label ne $label) {
		my $got = $dev->volume_label;
		return failure("Read back a different label: got '$got', but expected '$label'",
			$finished_cb);
	    } elsif ($dev->volume_time ne "X") {
		my $got = $dev->volume_time;
		return failure("Read back a different timetstamp: got '$got', but expected 'X'",
			$finished_cb);
	    }

	    # update the tapelist
	    $tl->reload(1);
	    $tl->remove_tapelabel($label);
	    # the label is not yet assigned a config
	    $tl->add_tapelabel("0", $label, undef, 1, $meta, $res->{'barcode'},
			       $dev->block_size/1024,
			       $tapepool, undef, undef);
	    $tl->write();

	    print "Success!\n";

	    # notify the changer
	    $res->set_label(label => $label, finished_cb => $steps->{'set_meta_label'});
	} else {
	    return failure("Not writing label.", $finished_cb);
	}
    };

    step set_meta_label => sub {
	my ($gerr) = @_;

	if ($opt_meta) {
	    return $res->set_meta_label(meta => $opt_meta,
					finished_cb => $steps->{'labeled'});
	} else {
	    return $steps->{'labeled'}->();
	}
    };

    step labeled => sub {
	my ($err) = @_;
	$gerr = $err if !$gerr;

	$res->release(finished_cb => $steps->{'released'});
    };

    step released => sub {
	my ($err) = @_;
	return failure($gerr, $finished_cb) if $gerr;
	return failure($err, $finished_cb) if $err;

	$finished_cb->();
    };

    step assign => sub {
	my $matched = 0;
	my $changed = 0;
	$tl->reload(1);
	for my $tle (@{$tl->{'tles'}}) {
	  if ($tle->{'label'} =~ /$opt_label/ or
	      $tle->{'label'} eq $opt_label) {
		my $changed1 = 0;
		my $error = 0;
		$matched = 1;
		if ($tle->{'config'} &&
		    $tle->{'config'} ne Amanda::Config::get_config_name()) {
		    print STDERR "$tle->{'label'}: Can't assign because it is is the '$tle->{'config'}' config\n";
		    $error = 1;
		} else {
		    if (!$tle->{'config'} && $tle->{'datestamp'} ne "0") {
			$tle->{'config'} = Amanda::Config::get_config_name();
			$changed1 = 1;
		    }
		    if (defined $opt_meta) {
			if (defined($tle->{'meta'}) &&
			    $opt_meta ne $tle->{'meta'} &&
			    !$opt_force) {
			    print STDERR "$tle->{'label'}: Can't assign meta-label without -f, old meta-label is '$tle->{'meta'}'\n";
			    $error = 1;
			} elsif ($tle->{'meta'} ne $opt_meta) {
			    $tle->{'meta'} = $opt_meta;
			    $changed1 = 1;
			}
		    }
		    if (defined $opt_barcode) {
			if (defined($tle->{'barcode'}) &&
			    $opt_barcode ne $tle->{'barcode'} &&
			    !$opt_force) {
			    print STDERR "$tle->{'label'}: Can't assign barcode without -f, old barcode is '$tle->{'barcode'}'\n";
			    $error = 1;
			} elsif ($tle->{'barcode'} ne $opt_barcode) {
			    $tle->{'barcode'} = $opt_barcode;
			    $changed1 = 1;
			}
		    }
		    if (defined $opt_pool) {
			if (defined($tle->{'pool'}) &&
			    $opt_pool ne $tle->{'pool'} &&
			    !$opt_force) {
			    print STDERR "$tle->{'label'}: Can't assign pool without -f, old pool is '$tle->{'pool'}'\n";
			    $error = 1;
			} elsif ($tle->{'pool'} ne $opt_pool) {
			    $tle->{'pool'} = $opt_pool;
			    $changed1 = 1;
			}
		    }
		    if (defined $opt_storage) {
			if (defined($tle->{'storage'}) &&
			    $opt_storage ne $tle->{'storage'} &&
			    !$opt_force) {
			    print STDERR "$tle->{'label'}: Can't assign storage without -f, old storage is '$tle->{'storage'}'\n";
			    $error = 1;
			} elsif ($tle->{'datestamp'} eq "0") {
			    print STDOUT "$tle->{'label'}: Can't assign storage because it is a new labelled tape.\n";
			} elsif ($tle->{'storage'} ne $opt_storage) {
			    $tle->{'storage'} = $opt_storage;
			    $changed1 = 1;
			}
		    }
		}

		if ($changed1 && !$error) {
		    print "Setting $tle->{'label'}\n";
		    $changed++;
		}
		$exit_status |= $error;
	    }
	}
	if ($exit_status == 1) {
	   return $finished_cb->();
	} elsif ($changed) {
	    $tl->write();
	} elsif ($matched) {
	    $tl->unlock();
	    print "All labels already correctly set.\n";
	} else {
	    $tl->unlock();
	    return failure("No label matching '$opt_label' in the tapelist file", $finished_cb);
	}

	$chg->inventory(inventory_cb => $steps->{'assign_inventory'});
    };

    step assign_inventory => sub {
	my ($err, $inv) = @_;

	if ($err) {
	    return $finished_cb->() if $err->notimpl;
	    return failure($err, $finished_cb);
	}

	for my $sl (@$inv) {
	    if (defined $sl->{'label'} && ($sl->{'label'} =~ /$opt_label/ ||
					   $sl->{'label'} eq $opt_label)) {
		return $chg->set_meta_label(meta => $opt_meta,
					    slot => $sl->{'slot'},
					    finished_cb => $steps->{'done'});
	    }
	}
	$finished_cb->();
    };

    step done => sub {
	$finished_cb->();
    }
}

main(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit($exit_status);
