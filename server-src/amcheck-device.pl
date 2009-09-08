#! @PERL@
# Copyright (c) 2009 Zmanda Inc.  All Rights Reserved.
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
use warnings;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );
use Amanda::Logfile qw( :logtype_t log_add $amanda_log_trace_log );
use Amanda::Debug;
use Amanda::Device qw( :constants );
use Amanda::MainLoop;
use Amanda::Changer;
use Amanda::Taper::Scan;
use Amanda::Ndmp;
use Getopt::Long;

Amanda::Util::setup_application("amcheck-device", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);
my $overwrite = 0;
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    'w' => \$overwrite,
) or usage();

if (@ARGV != 1) {
    print STDERR "USAGE: amcheck-device <config> [-w] <config-overwrites>";
    exit 1;
}

config_init($CONFIG_INIT_EXPLICIT_NAME, $ARGV[0]);
apply_config_overrides($config_overrides);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        print STDERR "Errors processing config file";
	exit 1;
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);
my $exit_status = 0;

sub failure {
    my ($msg) = @_;
    print STDERR "ERROR: $msg\n";
    $exit_status = 1;
    Amanda::MainLoop::quit();
}

my %subs;
my ($res, $label, $mode);

$subs{'start'} = make_cb(start => sub {
    my $chg = Amanda::Changer->new();

    return failure($chg) if ($chg->isa("Amanda::Changer::Error"));

    my $taperscan = Amanda::Taper::Scan->new(changer => $chg);
    $taperscan->scan(
	result_cb => $subs{'result_cb'},
	user_msg_fn => sub {
	    print STDERR "$_[0]\n";
	}
    );
});

$subs{'result_cb'} = make_cb(result_cb => sub {
    (my $err, $res, $label, $mode) = @_;
    return failure($err) if $err;

    # try reading the label if it's not already present
    if (!defined $res->{'device'}->volume_label()) {
	$res->{'device'}->read_label();
    }

    my $modestr = ($mode == $ACCESS_APPEND)? "append" : "write";
    my $slot = $res->{'this_slot'};
    if (defined $res->{'device'}->volume_label()) {
	print "Will $modestr to volume '$label' in slot $slot.\n";
    } else {
	print "Will $modestr label '$label' to new volume in slot $slot.\n";
    }

    $subs{'check_access_type'}->();
});

$subs{'check_access_type'} = make_cb(check_access_type => sub {
    my $mat = $res->{'device'}->property_get('medium_access_type');
    if (defined $mat and $mat == $MEDIA_ACCESS_MODE_WRITE_ONLY) {
	print "WARNING: Media access mode is WRITE_ONLY; dumps may not be recoverable\n";
    }

    $subs{'check_overwrite'}->();
});

$subs{'check_overwrite'} = make_cb(check_overwrite => sub {
    # if we're not overwriting, just release the reservation
    if (!$overwrite) {
        print "NOTE: skipping tape-writable test\n";
	return $subs{'release'}->();
    }

    if ($mode != $ACCESS_WRITE) {
	my $modestr = Amanda::Device::DeviceAccessMode_to_string($mode);
	print "NOTE: taperscan specified access mode $modestr; skipping volume-writeable test\n";
	return $subs{'release'}->();
    }

    print "Writing label '$label' to check writablility\n";
    if (!$res->{'device'}->start($ACCESS_WRITE, $label, "X")) {
	print "ERROR: writing to volume: " . $res->{'device'}->error_or_status(), "\n";
	$exit_status = 1;
    } else {
	print "Volume '$label' is writeable.\n";
    }

    $subs{'release'}->();
});

$subs{'release'} = make_cb(release => sub {
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
