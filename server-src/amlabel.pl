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
use Amanda::Header qw( :constants );
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Ndmp;

my $exit_status = 0;

##
# Subcommand handling

my %subcommands;

sub usage {
    print STDERR "Usage: amlabel <conf> <label> [slot <slot-number>] "
	       . "[-f] [-o configoption]*\n";
    exit(1);
}

Amanda::Util::setup_application("amlabel", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);
my ($opt_force, $opt_config, $opt_slot, $opt_label);

$opt_force = 0;
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'help|usage|?' => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    'f' => \$opt_force,
    'version' => \&Amanda::Util::version_opt,
) or usage();

if (@ARGV == 2) {
    $opt_slot = undef;
} elsif (@ARGV == 4 and $ARGV[2] eq 'slot') {
    $opt_slot = $ARGV[3];
} else {
    usage();
}

$opt_config = $ARGV[0];
$opt_label = $ARGV[1];

config_init($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
apply_config_overrides($config_overrides);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	print STDERR "errors processing config file";
	exit 1;
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my %subs;
my ($tlf, $tl, $res);

sub failure {
    my ($msg) = @_;
    print STDERR "$msg\n";
    $exit_status = 1;
    if ($res) {
	$res->release(finished_cb => sub { Amanda::MainLoop::quit(); });
    } else {
	Amanda::MainLoop::quit();
    }
}

$subs{'start'} = make_cb(start => sub {
    my $labelstr = getconf($CNF_LABELSTR);
    if ($opt_label !~ /$labelstr/) {
	return failure("Label '$opt_label' doesn't match labelstr '$labelstr'.");
    }

    $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    $tl = Amanda::Tapelist::read_tapelist($tlf);
    if (!defined $tl) {
	return failure("Can't load tapelist file ($tlf)");
    }
    if (!$opt_force) {
	if ($tl->lookup_tapelabel($opt_label)) {
	    return failure("Label '$opt_label' already on a volume");
	}
    }

    $subs{'load'}->();
});

$subs{'load'} = make_cb(load => sub {
    my $chg = Amanda::Changer->new();

    return failure($chg)
	if $chg->isa("Amanda::Changer::Error");

    if ($opt_slot) {
	$chg->load(slot => $opt_slot, mode => "write",
		res_cb => $subs{'loaded'});
    } else {
	$chg->load(relative_slot => "current", mode => "write",
		res_cb => $subs{'loaded'});
    }
});

$subs{'loaded'} = make_cb(loaded => sub {
    (my $err, $res) = @_;

    return failure($err) if $err;

    my $dev = $res->{'device'};
    my $dev_ok = 1;
    if (!defined $dev->volume_label) {
	$dev->read_label();
    }
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
	my $labelstr = getconf($CNF_LABELSTR);

	if ($label !~ /$labelstr/) {
	    print "Found label '$label', but it is not from configuration " .
		"'" . Amanda::Config::get_config_name() . "'.\n";
	    $dev_ok = 0 unless ($opt_force);
	} elsif ($tl->lookup_tapelabel($label)) {
	    print "Volume with label '$label' contains data from this configuration.\n";
	    $dev_ok = 0 unless ($opt_force);
	} else {
	    print "Found Amanda volume '$label'.\n";
	}
    }

    if ($dev_ok) {
	print "Writing label '$opt_label'..\n";

	if (!$dev->start($ACCESS_WRITE, $opt_label, "X")) {
	    return failure("Error writing label: " . $dev->error_or_status());
	} elsif (!$dev->finish()) {
	    return failure("Error finishing device: " . $dev->error_or_status());
	}

	print "Checking label...\n";
	my $status = $dev->read_label();
	if ($status != $DEVICE_STATUS_SUCCESS) {
	    return failure("Checking the tape label failed: " . $dev->error_or_status());
	} elsif (!$dev->volume_label) {
	    return failure("No label found.");
	} elsif ($dev->volume_label ne $opt_label) {
	    my $got = $dev->volume_label;
	    return failure("Read back a different label: got '$got', but expected '$opt_label'");
	} elsif ($dev->volume_time ne "X") {
	    my $got = $dev->volume_time;
	    return failure("Read back a different timetstamp: got '$got', but expected 'X'");
	}

	# update the tapelist
	$tl->remove_tapelabel($opt_label);
	$tl->add_tapelabel("0", $opt_label, undef);
	$tl->write($tlf);

	print "Success!\n";

	# notify the changer
	$res->set_label(label => $opt_label, finished_cb => $subs{'labeled'});
    } else {
	return failure("Volume not labeled.");
    }
});

$subs{'labeled'} = make_cb(labeled => sub {
    my ($err) = @_;
    return failure($err) if $err;

    $res->release(finished_cb => $subs{'released'});
});

$subs{'released'} = make_cb(released => sub {
    my ($err) = @_;
    return failure($err) if $err;

    Amanda::MainLoop::quit();
});

$subs{'start'}->();
Amanda::MainLoop::run();
Amanda::Ndmp::stop_ndmp_proxy();
Amanda::Util::finish_application();
exit($exit_status);
