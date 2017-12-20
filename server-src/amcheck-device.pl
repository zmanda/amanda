#! @PERL@
# Copyright (c) 2009-2012 Zmanda Inc.  All Rights Reserved.
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

package Amanda::Amcheck_Device::Message;
use Amanda::Changer;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 5200000) {
	return "slot $self->{'slot'}: Will $self->{'modestr'} to volume '$self->{'label'}' in slot $self->{'slot'}";
    } elsif ($self->{'code'} == 5200001) {
	return "slot $self->{'slot'}: Will $self->{'modestr'} label '$self->{'label'}' to non-Amanda volume in slot $self->{'slot'}";
    } elsif ($self->{'code'} == 5200002) {
	return "slot $self->{'slot'}: Will $self->{'modestr'} label '$self->{'label'}' to new volume in slot $self->{'slot'}";
    } elsif ($self->{'code'} == 5200003) {
	return "slot $self->{'slot'}: Media access mode is WRITE_ONLY; dumps may not be recoverable";
    } elsif ($self->{'code'} == 5200004) {
	return "DEVICE-OUTPUT-BUFFER-SIZE is not at least twice the block size of the device, it should be increased for better throughput";
    } elsif ($self->{'code'} == 5200005) {
	return "slot $self->{'slot'}: $self->{'dev_error'}";
    } elsif ($self->{'code'} == 5200006) {
	return "slot $self->{'slot'}: skipping tape-writable test";
    } elsif ($self->{'code'} == 5200007) {
	return "slot $self->{'slot'}: taperscan specified access mode $self->{'modestr'}; skipping volume-writeable test";
    } elsif ($self->{'code'} == 5200008) {
	return "slot $self->{'slot'}: Writing label '$self->{'label'}' to check writability";
    } elsif ($self->{'code'} == 5200009) {
	return "slot $self->{'slot'}: writing to volume: $self->{'dev_error'}";
    } elsif ($self->{'code'} == 5200010) {
	return "slot $self->{'slot'}: Volume '$self->{'label'}' is writeable";
    } else {
        return "No mesage for code '$self->{'code'}'";
    }
}

package main;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Logfile qw( :logtype_t log_add $amanda_log_trace_log );
use Amanda::Debug;
use Amanda::Device qw( :constants );
use Amanda::MainLoop;
use Amanda::Policy;
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Taper::Scan;
use Amanda::Interactivity;
use Amanda::Message;
use Getopt::Long;
use JSON -convert_blessed_universally;

Amanda::Util::setup_application("amcheck-device", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);
my $overwrite = 0;
my $opt_message = 0;
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    'message' => \$opt_message,
    'w' => \$overwrite,
) or usage();

sub usage {
    print STDERR "USAGE: amcheck-device <config> <storage> [--message] [-w] <config-overwrites>";
    exit 1;
}

if (@ARGV != 2) {
    usage();
}

set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $ARGV[0]);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        print STDERR "Errors processing config file";
	exit 1;
    }
}

my $storage_name = $ARGV[1];
my $storage;
Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);
my $exit_status = 0;

sub _user_msg_fn {
    my $message = shift;

    my $msg = $message->message();
    delete $message->{'res'};
    my $json = JSON->new->allow_nonref;
    print STDOUT $json->pretty->allow_blessed->convert_blessed->encode($message);
}

sub failure {
    my ($msg, $finished_cb) = @_;
    _user_msg_fn($msg);
    $exit_status = 1;
    $finished_cb->();
}

sub do_check {
    my ($finished_cb) = @_;
    my ($res, $label, $mode);
    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my ($tl, $message) = Amanda::Tapelist->new($tlf);
    if (defined $message) {
	if ($message->{'severity'} >= $Amanda::Message::CRITICAL) {
	    return failure($message, $finished_cb);
	} else {
	    print STDERR "ERROR: $message\n";
	}
    }

    $storage  = Amanda::Storage->new(storage_name => $storage_name,
				     tapelist => $tl);
    return failure($storage, $finished_cb) if $storage->isa("Amanda::Message");
    my $chg = $storage->{'chg'};
    if ($chg->isa("Amanda::Message")) {
	$storage->quit();
	return failure($chg, $finished_cb);
    }
    my $interactivity = Amanda::Interactivity->new(
					name => $storage->{'interactivity'},
					storage_name => $storage->{'storage_name'},
					changer_name => $chg->{'chg_name'});
    my $scan_name = $storage->{'taperscan_name'};
    my $taperscan = Amanda::Taper::Scan->new(algorithm => $scan_name,
					     storage => $storage,
					     changer => $chg,
					     interactivity => $interactivity,
					     tapelist => $tl);

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $storage->quit();
			  $taperscan->quit(); };

    step start => sub {
	$taperscan->scan(
	    result_cb => $steps->{'result_cb'},
	    user_msg_fn => \&_user_msg_fn,
	);
    };

    step result_cb => sub {
	(my $err, $res, $label, $mode) = @_;
	if ($err) {
	    if ($res) {
		$res->release(finished_cb => sub {
		    return failure("A $err", $finished_cb);
		});
		return;
	    } else {
		return failure($err, $finished_cb);
	    }
	}

	my $modestr = ($mode == $ACCESS_APPEND)? "append" : "write";
	my $slot = $res->{'this_slot'};
	if (defined $res->{'device'} and defined $res->{'device'}->volume_label()) {
	    _user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200000,
				severity	=> $Amanda::Message::INFO,
				modestr		=> $modestr,
				label		=> $label,
				slot		=> $slot,
				storage_name	=> $storage->{'storage_name'}));
	    #print "Will $modestr to volume '$label' in slot $slot.\n";
	} else {
	    my $header = $res->{'device'}->volume_header();
	    if (defined $header and defined $header->{'type'} and
		$header->{'type'} == $Amanda::Header::F_WEIRD) {
		_user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200001,
				severity	=> $Amanda::Message::INFO,
				modestr		=> $modestr,
				label		=> $label,
				slot		=> $slot,
				storage_name	=> $storage->{'storage_name'}));
		#print "Will $modestr label '$label' to non-Amanda volume in slot $slot.\n";
	    } else {
		_user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200002,
				severity	=> $Amanda::Message::INFO,
				modestr		=> $modestr,
				label		=> $label,
				slot		=> $slot,
				storage_name	=> $storage->{'storage_name'}));
		#print "Will $modestr label '$label' to new volume in slot $slot.\n";
	    }
	}

	$steps->{'check_access_type'}->();
    };

    step check_access_type => sub {
	my $mat = $res->{'device'}->property_get('medium_access_type');
	my $slot = $res->{'this_slot'};
	if (defined $mat and $mat == $MEDIA_ACCESS_MODE_WRITE_ONLY) {
	    _user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200003,
				severity	=> $Amanda::Message::WARNING,
				slot		=> $slot,
				storage_name	=> $storage->{'storage_name'}));
	    #print "WARNING: Media access mode is WRITE_ONLY; dumps may not be recoverable\n";
	}

	if ($storage->{'seen_device_output_buffer_size'}) {
	    my $dobs = $storage->{'device_output_buffer_size'};
	    my $block_size = $res->{'device'}->property_get("BLOCK_SIZE");
	    if ($block_size * 2 > $dobs) {
		_user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200004,
				severity	=> $Amanda::Message::WARNING,
				slot		=> $slot,
				storage_name	=> $storage->{'storage_name'}));
		#print "WARNING: DEVICE-OUTPUT-BUFFER-SIZE is not at least twice the block size of the device, it should be increased for better throughput\n";
	    }
	}
	$steps->{'check_writable'}->();
    };

    step check_writable => sub {

	if($res->{'device'} and !$res->{'device'}->check_writable()) {
	    _user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200005,
				severity	=> $Amanda::Message::ERROR,
				slot		=> $res->{'this_slot'},
				storage_name	=> $storage->{'storage_name'},
				dev_error	=> $res->{'device'}->error_or_status()));
	    #print "ERROR: " . $res->{'device'}->error_or_status() . "\n";
	    return $steps->{'release'}->();
	}
	$steps->{'check_overwrite'}->();
    };

    step check_overwrite => sub {
	# if we're not overwriting, just release the reservation
	if (!$overwrite) {
	    #_user_msg_fn(Amanda::Amcheck_Device::Message->new(
				#source_filename	=> __FILE__,
				#source_line	=> __LINE__,
				#code		=> 5200006,
				#severity	=> $Amanda::Message::INFO,
				#slot		=> $res->{'this_slot'},
				#storage_name	=> $storage->{'storage_name'}));
	    #print "NOTE: skipping tape-writable test\n";
	    return $steps->{'release'}->();
	}

	if ($mode != $ACCESS_WRITE) {
	    my $modestr = Amanda::Device::DeviceAccessMode_to_string($mode);
	    _user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200007,
				severity	=> $Amanda::Message::INFO,
				slot		=> $res->{'this_slot'},
				modestr		=> $modestr,
				storage_name	=> $storage->{'storage_name'}));
	    #print "NOTE: taperscan specified access mode $modestr; skipping volume-writeable test\n";
	    return $steps->{'release'}->();
	}

	_user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200008,
				severity	=> $Amanda::Message::INFO,
				slot		=> $res->{'this_slot'},
				label		=> $label,
				storage_name	=> $storage->{'storage_name'}));
	#print "Writing label '$label' to check writability\n";
	if (!$res->{'device'}->start($ACCESS_WRITE, $label, "X")) {
	    _user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200009,
				severity	=> $Amanda::Message::INFO,
				slot		=> $res->{'this_slot'},
				dev_error	=> $res->{'device'}->error_or_status(),
				storage_name	=> $storage->{'storage_name'}));
	    #print "ERROR: writing to volume: " . $res->{'device'}->error_or_status(), "\n";
	    $exit_status = 1;
	} else {
	    _user_msg_fn(Amanda::Amcheck_Device::Message->new(
				source_filename	=> __FILE__,
				source_line	=> __LINE__,
				code		=> 5200010,
				severity	=> $Amanda::Message::INFO,
				slot		=> $res->{'this_slot'},
				label		=> $label,
				storage_name	=> $storage->{'storage_name'}));
	    #print "Volume '$label' is writeable.\n";
	}

	$steps->{'release'}->();
    };

    step release => sub {
	$res->release(finished_cb => $steps->{'released'});
    };

    step released => sub {
	my ($err) = @_;
	return failure("C $err", $finished_cb) if $err;

	$finished_cb->();
    };
}

select(STDERR);
$| = 1;
select(STDOUT); # default
$| = 1;

Amanda::MainLoop::call_later(\&do_check, \&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit($exit_status);
