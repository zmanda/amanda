# Copyright (c) 2009-2014 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use strict;
use warnings;

package Amanda::FetchDump::Message;
use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 3300000) {
        return int($self->{'size'}/1024) . " kb";
    } elsif ($self->{'code'} == 3300001) {
        return "WARNING: Fetch first dump only because sending data stream to a pipe";
    } elsif ($self->{'code'} == 3300002) {
	my $msg = "";
	if (@{$self->{'needed_labels'}}) {
	    $msg .= (scalar @{$self->{'needed_labels'}}) . " volume(s) needed for restoration\n";
	    $msg .= "The following volumes are needed: " .
		join(" ", map { $_->{'label'} } @{$self->{'needed_labels'}} ) . "\n";
	}
	if (@{$self->{'needed_holding'}}) {
	    $msg .= (scalar @{$self->{'needed_holding'}}) . " holding file(s) needed for restoration\n";
	    for my $hf (@{$self->{'needed_holding'}}) {
		$msg .= "  $hf\n";
	    }
	}
        return $msg;
    } elsif ($self->{'code'} == 3300003) {
        return "Reading label '$self->{'label'}' filenum $self->{'filenum'}\n$self->{'header_summary'}";
    } elsif ($self->{'code'} == 3300004) {
        return "Reading '$self->{'holding_file'}'\n$self->{'header_summary'}";
    } elsif ($self->{'code'} == 3300005) {
        return "recovery failed: server-crc in header ($self->{'header_server_crc'}) and server-crc in log ($self->{'log_server_crc'}) differ";
    } elsif ($self->{'code'} == 3300006) {
        return "recovery failed: server-crc ($self->{'log_server_crc'}) and source_crc ($self->{'source_crc'}) differ",
    } elsif ($self->{'code'} == 3300007) {
        return "recovery failed: native-crc ($self->{'log_native_crc'}) and restore-native-crc ($self->{'restore_native_crc'}) differ";
    } elsif ($self->{'code'} == 3300008) {
        return "recovery failed: client-crc ($self->{'log_client_crc'}) and restore-client-crc ($self->{'restore_client_crc'}) differ";
    } elsif ($self->{'code'} == 3300009) {
        return "recovery failed: dest-crc ($self->{'dest_crc'}) and restore-native-crc ($self->{'restore_native_crc'}) differ";
    } elsif ($self->{'code'} == 3300010) {
        return "recovery failed: dest-crc ($self->{'dest_crc'}) and restore-client-crc ($self->{'restore_client_crc'}) differ";
    } elsif ($self->{'code'} == 3300011) {
        return "recovery failed: dest-crc ($self->{'dest_crc'}) and source-crc ($self->{'source_crc'}) differ";
    } elsif ($self->{'code'} == 3300012) {
        return "READ SIZE: " . int($self->{'size'}/1024) . " kb";
    } elsif ($self->{'code'} == 3300013) {
	return "The application stdout";
    } elsif ($self->{'code'} == 3300014) {
        return "recovery failed: native-crc in header ($self->{'header_native_crc'}) and native-crc in log ($self->{'log_native_crc'}) differ";
    } elsif ($self->{'code'} == 3300015) {
        return "recovery failed: client-crc in header ($self->{'header_client_crc'}) and client-crc in log ($self->{'log_client_crc'}) differ";
    } elsif ($self->{'code'} == 3300016) {
        return "ERROR: XML error: $self->{'xml_error'}\n$self->{'dle_str'}";
    } elsif ($self->{'code'} == 3300017) {
        return "Not decompressing because the backup image is not decrypted";
    } elsif ($self->{'code'} == 3300018) {
        return "filter stderr: $self->{'line'}\n";
    } elsif ($self->{'code'} == 3300019) {
        return "amndmp stdout: $self->{'line'}\n";
    } elsif ($self->{'code'} == 3300020) {
        return "amndmp stderr: $self->{'line'}\n";
    } elsif ($self->{'code'} == 3300021) {
	return "'compress' is not compatible with 'compress-best'";
    } elsif ($self->{'code'} == 3300022) {
	return "'leave' is not compatible with 'compress'";
    } elsif ($self->{'code'} == 3300023) {
	return "'leave' is not compatible with 'compress-best'";
    } elsif ($self->{'code'} == 3300024) {
	return "'pipe' is not compatible with 'no-reassembly'";
    } elsif ($self->{'code'} == 3300025) {
	return "'header', 'header-file' and 'header-fd' are mutually incompatible";
    } elsif ($self->{'code'} == 3300026) {
	return "'data-path' must be 'amanda' or 'directtcp'";
    } elsif ($self->{'code'} == 3300027) {
	return "'leave' is incompatible with 'decrypt'";
    } elsif ($self->{'code'} == 3300028) {
	return "'leave' is incompatible with 'server-decrypt'"
    } elsif ($self->{'code'} == 3300029) {
	return "'leave' is incompatible with 'client-decrypt'"
    } elsif ($self->{'code'} == 3300030) {
	return "'leave' is incompatible with 'decompress'";
    } elsif ($self->{'code'} == 3300031) {
	return "'leave' is incompatible with 'server-decompress'"
    } elsif ($self->{'code'} == 3300032) {
	return "'leave' is incompatible with 'client-decompress'"
    } elsif ($self->{'code'} == 3300033) {
	return "Both 'directory' and 'extract' must be set"
    } elsif ($self->{'code'} == 3300034) {
	return "'server-decrypt' or 'client-decrypt' is incompatible with 'extract'";
    } elsif ($self->{'code'} == 3300035) {
	return "'server-decompress' or 'client-decompress' is incompatible with 'extract'";
    } elsif ($self->{'code'} == 3300036) {
	return "Can't use 'leave' or 'compress' with 'extract'";
    } elsif ($self->{'code'} == 3300037) {
	return "'pipe' is incompatible with 'extract'";
    } elsif ($self->{'code'} == 3300038) {
	return "'header' is incompatible wth 'extract'";
    } elsif ($self->{'code'} == 3300039) {
	return "Can't use only on of 'decrypt', 'no-decrypt', 'server-decrypt' or 'client-decrypt'";
    } elsif ($self->{'code'} == 3300040) {
	return "Can't use only on of 'decompress', 'no-decompress', 'server-decompress' or 'client-decompress'";
    } elsif ($self->{'code'} == 3300041) {
	return "Can't specify 'compress' with one of 'decompress', 'no-decompress', 'server-decompress' or 'client-decompress'";
    } elsif ($self->{'code'} == 3300042) {
	return "Can't specify 'compress-best' with one of 'decompress', 'no-decompress', 'server-decompress' or 'client-decompress'";
    } elsif ($self->{'code'} == 3300043) {
	return "Cannot chdir to $self->{'dir'}: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 3300044) {
	return "No matching dumps found";
    } elsif ($self->{'code'} == 3300045) {
	return join("; ", @{$self->{'errs'}});
    } elsif ($self->{'code'} == 3300046) {
	return "Could not open '$self->{'filename'}' for writing: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 3300047) {
	return "could not open '$self->{'header_file'}': $self->('errnostr'}";
    } elsif ($self->{'code'} == 3300048) {
	return "could not open fd '$self->{'header_fd'}': $self->('errnostr'}";
    } elsif ($self->{'code'} == 3300049) {
	return "The device can't do directtcp";
    } elsif ($self->{'code'} == 3300050) {
	return "Unknown program '$self->{'program'}' in header; no validation to perform";
    } elsif ($self->{'code'} == 3300051) {
	return "Application not set";
    } elsif ($self->{'code'} == 3300052) {
	return "Application '$self->{application} ($self->{'program_path'})' not available on the server";
    } elsif ($self->{'code'} == 3300053) {
	return "The application can't do directtcp";
    } elsif ($self->{'code'} == 3300054) {
	return "could not decrypt encrypted dump: no program specified";
    } elsif ($self->{'code'} == 3300055) {
	return join("; ", @{$self->{'xfer_errs'}});
    } elsif ($self->{'code'} == 3300056) {
	return "recovery failed";
    } elsif ($self->{'code'} == 3300057) {
	return "Running a Fetchdump";
    } elsif ($self->{'code'} == 3300058) {
	return "Failed to fork the FetchDump process";
    } elsif ($self->{'code'} == 3300059) {
	return "The message filename is '$self->{'message_filename'}'";
    } elsif ($self->{'code'} == 3300060) {
	return "Exit status: $self->{'exit_status'}";
    } else {
	return "No mesage for code '$self->{'code'}'";
    }
}

sub local_full_message {
    my $self = shift;

    if ($self->{'code'} == 3300013) {
	my $msg;
	foreach (@{$self->{'application_stdout'}}) {
	    next if $_ =~ /^$/;
	    $msg .= "Application stdout: $_\n";
	}
	return $msg;
    } else {
	return $self->{'message'};
    }
}

package Amanda::FetchDump;

use POSIX qw(strftime);
use File::Basename;
use XML::Simple;
use IPC::Open3;
use IO::Handle;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants :quoting );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Header;
use Amanda::Holding;
use Amanda::Cmdline;
use Amanda::Xfer qw( :constants );
use Amanda::Recovery::Planner;
use Amanda::Recovery::Clerk;
use Amanda::Recovery::Scan;
use Amanda::Extract;
use Amanda::Feature;
use Amanda::Logfile qw( :logtype_t log_add log_add_full );
use MIME::Base64 ();

my $NEVER = 0;
my $ALWAYS = 1;
my $ONLY_SERVER = 2;
my $ONLY_CLIENT = 3;

sub new {
    my $class = shift;
    my %params = @_;

    my @result_messages;

    my $self = bless {
	delay => $params{'delay'},
	src => undef,
	dst => undef,
	message_count => 0,

    }, $class;

    $self->{'delay'} = 15000 if !defined $self->{'delay'};
    $self->{'exit_status'} = 0;
    $self->{'amlibexecdir'} = 0;

    my $logdir = $self->{'logdir'} = config_dir_relative(getconf($CNF_LOGDIR));
    my @now = localtime;
    my $timestamp = strftime "%Y%m%d%H%M%S", @now;
    $self->{'pid'} = $$;
    $self->{'timestamp'} = Amanda::Logfile::make_logname("fetchdump", $timestamp);
    $self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
    debug("beginning trace log: $self->{'trace_log_filename'}");
    $self->{'fetchdump_log_filename'} = "fetchdump.$timestamp";
    $self->{'fetchdump_log_pathname'} = "$logdir/fetchdump.$timestamp";

    # Must be opened in append so that all subprocess can write to it.
    if (!open($self->{'message_file'}, ">>", $self->{'fetchdump_log_pathname'})) {
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700021,
		message_filename => $self->{'fetchdump_log_pathname'},
		errno            => $!,
		severity         => $Amanda::Message::ERROR);
    } else {
	$self->{'message_file'}->autoflush;
	log_add($L_INFO, "message file $self->{'fetchdump_log_filename'}");
    }

    return $self, \@result_messages;
}


sub start_reading
{
    my $self = shift;
    my $fd   = shift;
    my $cb   = shift;
    my $text = shift;

    $fd.="";
    $fd = int($fd);
    my $src = Amanda::MainLoop::fd_source($fd,
				  $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    my $buffer = "";
    $self->{'all_filter'}->{$src} = 1;
    $src->set_callback( sub {
	my $b;
	my $n_read = POSIX::read($fd, $b, 1);
	if (!defined $n_read) {
	    return;
	} elsif ($n_read == 0) {
	    delete $self->{'all_filter'}->{$src};
	    $src->remove();
	    POSIX::close($fd);
	    if (!%{$self->{'all_filter'}} and $self->{'recovery_done'}) {
		$cb->();
	    }
	} else {
	    $buffer .= $b;
	    if ($b eq "\n") {
		my $line = $buffer;
		chomp $line;
		if (length($line) > 1) {
#		    if (!$app_success || $app_error) {
			$self->user_message(
			    Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300018,
				severity	=> $Amanda::Message::ERROR,
				line		=> $line));
#		    }
		}
		$buffer = "";
	    }
	}
    });
}

sub user_message {
    my $self = shift;
    my $msg = shift;

    if (defined $self->{'message_file'}) {
	my $d;
	if (ref $msg eq "SCALAR") {
	    $d = Data::Dumper->new([$msg], ["MESSAGES[$self->{'message_count'}]"]);
	} else {
	    my %msg_hash = %$msg;
	    $d = Data::Dumper->new([\%msg_hash], ["MESSAGES[$self->{'message_count'}]"]);
	}
	print {$self->{'message_file'}} $d->Dump();
	$self->{'message_count'}++;
    }
    if ($self->{'feedback'}->can('user_message')) {
	$self->{'feedback'}->user_message($msg);
    }
}

sub restore {
    my $self = shift;
    my %params = @_;

    $self->{'feedback'} = $params{'feedback'};

    if (defined $params{'compress'} and defined $params{'compress-best'}) {
	$self->user_message(
	    Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300021,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined $params{'leave'} and defined $params{'compress'}) {
	$self->user_message(
	    Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300022,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'leave'} and defined $params{'compress-best'}) {
	$self->user_message(
	    Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300023,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'leave'} and defined $params{'no-reassembly'}) {
	$self->user_message(
	    Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300024,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'header'} and (defined $params{'header-file'} or
				       defined $params{'header-fd'})) {
	$self->user_message(
	    Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300025,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined $params{'data-path'}) {
	$params{'data-path'} = lc($params{'data-path'});
	if ($params{'data-path'} ne 'directtcp' and
	    $params{'data-path'} ne 'amanda') {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300026,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
    }

    if ($params{'leave'}) {
	if ($params{'decrypt'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300027,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'server-decrypt'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300028,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'client-decrypt'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300029,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'decompress'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300030,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'server-decompress'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300031,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'client-decompress'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300032,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
    }

    if ((defined $params{'directory'} and !defined $params{'extract'}) or
	(!defined $params{'directory'} and  defined $params{'extract'})) {
	$self->user_message(
	    Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300033,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined $params{'directory'} and defined $params{'extract'}) {
	$params{'decrypt'} = 1;
	if (defined $params{'server-decrypt'} or defined $params{'client-decrypt'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300034,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}

	$params{'decompress'} = 1;
	if (defined $params{'server-decompress'} || defined $params{'client-decompress'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300035,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if (defined($params{'leave'}) +
	    defined($params{'compress'}) +
	    defined($params{'compress-best'})) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300036,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if (defined $params{'pipe'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300037,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if (defined $params{'header'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300038,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
    }

    if (defined($params{'decrypt'}) +
	defined($params{'server-decrypt'}) +
	defined($params{'client-decrypt'}) > 1) {
	$self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300039,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined($params{'decompress'}) +
	defined($params{'server-decompress'}) +
	defined($params{'client-decompress'}) > 1) {
	$self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300040,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined($params{'compress'}) and
	defined($params{'decompress'}) +
	defined($params{'server-decompress'}) +
	defined($params{'client-decompress'}) > 0) {
	$self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300041,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined($params{'compress-best'}) and
	defined($params{'decompress'}) +
	defined($params{'server-decompress'}) +
	defined($params{'client-decompress'}) > 0) {
	$self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300042,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    my $decompress = $ALWAYS;
    my $decrypt = $ALWAYS;

    $decrypt = $NEVER  if defined $params{'leave'};
    $decrypt = $NEVER  if defined $params{'decrypt'} and !$params{'decrypt'};
    $decrypt = $ALWAYS if defined $params{'decrypt'} and $params{'decrypt'};
    $decrypt = $ONLY_SERVER if defined $params{'server-decrypt'};
    $decrypt = $ONLY_CLIENT if defined $params{'client-decrypt'};

    $params{'compress'} = 1 if $params{'compress-best'};

    $decompress = $NEVER  if defined $params{'compress'};
    $decompress = $NEVER  if defined $params{'leave'};
    $decompress = $NEVER  if defined $params{'decompress'} and !$params{'decompress'};
    $decompress = $ALWAYS if defined $params{'decompress'} and $params{'decompress'};
    $decompress = $ONLY_SERVER if defined $params{'server-decompress'};
    $decompress = $ONLY_CLIENT if defined $params{'client-decompress'};

    use Data::Dumper;

    my $current_dump;
    my $plan;
    my @xfer_errs;
    my %recovery_params;
    my $timer;
    my $is_tty;
    my $last_is_size;
    my $directtcp = 0;
    my @directtcp_command;
    my @init_needed_labels;
    my $init_label;
    my %scan;
    my $clerk;
    my %clerk;
    my %storage;
    my $interactivity;
    my $hdr;
    my $source_crc;
    my $dest_crc;
    my $xfer_src;
    my $xfer_dest;
    my $client_filter;
    my $native_filter;
    my $restore_native_crc;
    my $restore_client_crc;
    my $dest_is_server;
    my $dest_is_client;
    my $dest_is_native;
    my $xfer_app;
    my $app_success;
    my $app_error;
    my $check_crc;
    my $tl;
    my $offset;
    my $size;
    my $xfer;
    my $use_dar = $params{'use_dar'} || 0;
    my $xfer_waiting_dar;

    my $steps = define_steps
	cb_ref => \$params{'finished_cb'},
	finalize => sub { foreach my $name (keys %storage) {
			    $storage{$name}->quit();
			  }
			  log_add($L_INFO, "pid-done $$");
			};

    step start => sub {
	my $chg;

	my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	($tl, my $message) = Amanda::Tapelist->new($tlf);
	if (defined $message) {
	    die "Could not read the tapelist: $message";
	}

	# first, go to params{'directory'} or the original working directory we
	# were started in
	my $destdir = $params{'chdir'} || Amanda::Util::get_original_cwd();
	if (!chdir($destdir)) {
	    return $steps->{'failure'}->(
		Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300043,
			severity	=> $Amanda::Message::ERROR,
			dir		=> $destdir,
			errno		=> $!));
	}

	$interactivity = $params{'interactivity'};

	my $storage;
	if (defined $params{'storage'}) {
	    $storage = $params{'storage'};
	} elsif (defined $params{'device'}) {
	    # if we have an explicit device, then the clerk doesn't get a changer --
	    # we operate the changer via Amanda::Recovery::Scan
	    $storage = Amanda::Storage->new(changer_name => $params{'device'},
					    tapelist => $tl);
	    return $steps->{'failure'}->($storage) if $storage->isa("Amanda::Changer::Error");
	    $storage{$storage->{"storage_name"}} = $storage;
	    $chg = $storage->{'chg'};
	    return $steps->{'failure'}->($chg) if $chg->isa("Amanda::Changer::Error");
	    $params{'feedback'}->set_feedback(chg => $chg);
	} else {
	    $storage = Amanda::Storage->new(tapelist => $tl);
	    return $steps->{'failure'}->($storage) if $storage->isa("Amanda::Changer::Error");
	    $storage{$storage->{"storage_name"}} = $storage;
	    $chg = $storage->{'chg'};
	    return $steps->{'failure'}->($chg) if $chg->isa("Amanda::Changer::Error");
	}

	my $scan;
	if (defined $params{'scan'}) {
	    $scan = $params{'scan'}
	} else {
	    $scan = Amanda::Recovery::Scan->new(
			storage       => $storage,
			chg           => $chg,
			interactivity => $params{'interactivity'});
	    return $steps->{'failure'}->($scan) if $scan->isa("Amanda::Changer::Error");
	}
	$scan{$storage->{"storage_name"}} = $scan;

	my $clerk;
	if (defined $params{'clerk'}) {
	    $clerk = $params{'clerk'};
	} else {
	    $clerk = Amanda::Recovery::Clerk->new(
		changer => $chg,
		feedback => $params{'feedback'},
		scan     => $scan);
	}
	$clerk{$storage->{"storage_name"}} = $clerk;

	# planner gets to plan against the same changer the user specified
	my $storage_list = getconf($CNF_STORAGE);
	my $only_in_storage = 0;
	if (getconf_linenum($CNF_STORAGE) == -2) {
	    $only_in_storage = 1;
	}

	if (defined $params{'plan'}) {
	    $plan = $params{'plan'};
	    $steps->{'start_dump'}->();
	} else {
	    Amanda::Recovery::Planner::make_plan(
		dumpspecs => [ @{$params{'dumpspecs'}} ],
		labelstr => $storage->{'labelstr'},
		storage_list => $storage_list,
		only_in_storage => $only_in_storage,
		plan_cb => $steps->{'plan_cb'},
		$params{'no-reassembly'}? (one_dump_per_part => 1) : ());
	}
    };

    step plan_cb => sub {
	(my $err, $plan) = @_;
	return $steps->{'failure'}->($err) if $err;

	if (!@{$plan->{'dumps'}}) {
	    return $steps->{'failure'}->(
		Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300044,
			severity	=> $Amanda::Message::ERROR));
	}

	# if we are doing a -p operation, only keep the first dump
	if ($params{'pipe-fd'}) {
	    $self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300001,
			severity	=> $Amanda::Message::INFO)) if @{$plan->{'dumps'}} > 1;
	    @{$plan->{'dumps'}} = ($plan->{'dumps'}[0]);
	}
	if ($params{'init'}) {
	    return $steps->{'init_seek_file'}->();
	}
	$steps->{'list_volume'}->();
    };

    step init_seek_file => sub {

	@init_needed_labels = $plan->get_volume_list();
	$steps->{'loop_init_seek_file'}->();
    };

    step loop_init_seek_file => sub {
	my $Xinit_label = shift @init_needed_labels;
	return $steps->{'end_init_seek_file'}->() if !$Xinit_label;
	#return $steps->{'end_init_seek_file'}->() if !$Xinit_label->{'available'};
	$init_label = $Xinit_label->{'label'};
	return $steps->{'end_init_seek_file'}->() if !$init_label;

	#find the storage
	my $storage_name=$Xinit_label->{'storage'};
	if (!$storage{$storage_name}) {
	    my ($storage) = Amanda::Storage->new(storage_name => $storage_name,
						 tapelist => $tl);
	    #return  $steps->{'quit'}->($storage) if $storage->isa("Amanda::Changer::Error");
	    $storage{$storage_name} = $storage;
	};

	my $chg = $storage{$storage_name}->{'chg'};
	if (!$scan{$storage_name}) {
	    my $scan = Amanda::Recovery::Scan->new(chg => $chg,
						   interactivity => $interactivity);
	    #return $steps->{'quit'}->($scan)
	    #    if $scan{$storage->{"storage_name"}}->isa("Amanda::Changer::Error");
	    $scan{$storage_name} = $scan;
	};

	$scan{$storage_name}->find_volume(label  => $init_label,
					  res_cb => $steps->{'init_seek_file_done_load'},
					  set_current => 0);
    };

    step init_seek_file_done_load => sub {
	my ($err, $res) = @_;
        return $steps->{'failure'}->($err) if ($err);

	my $dev = $res->{'device'};
	if (!$dev->start($Amanda::Device::ACCESS_READ, undef, undef)) {
	    $err = $dev->error_or_status();
	}
	for my $dump (@{$plan->{'dumps'}}) {
	    for my $part (@{$dump->{'parts'}}) {
		next unless defined $part; # skip parts[0]
		next unless defined $part->{'label'}; # skip holding parts
		next if $part->{'label'} ne $init_label;
		$dev->init_seek_file($part->{'filenum'});
	    }
	}
	$res->release(finished_cb => $steps->{'loop_init_seek_file'});
    };

    step end_init_seek_file => sub {
	if (defined $params{'restore'} && $params{'restore'} == 0) {
	    return $steps->{'finished'}->();
	}
	$steps->{'list_volume'}->();
    };

    step list_volume => sub {
	my @needed_labels = $plan->get_volume_list();
	my @needed_holding = $plan->get_holding_file_list();
	$self->user_message(
		Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300002,
			severity	=> $Amanda::Message::SUCCESS,
			needed_labels   => \@needed_labels,
			needed_holding	=> \@needed_holding));

	$steps->{'start_dump'}->();
    };

    step start_dump => sub {
	$current_dump = shift @{$plan->{'dumps'}};

	if (!$current_dump) {
	    return $steps->{'finished'}->();
	}

	$self->{'recovery_done'} = 0;
	$use_dar = 0;
	%recovery_params = ();
	$app_success = 0;
	$app_error = 0;
	$check_crc = !$params{'no-reassembly'};
	$xfer_app = undef;

	my $storage_name = $current_dump->{'storage'};
	if (!$storage{$storage_name}) {
	    my ($storage) = Amanda::Storage->new(storage_name => $storage_name,
						 tapelist => $tl);
	    #return  $steps->{'quit'}->($storage) if $storage->isa("Amanda::Changer::Error");
	    $storage{$storage_name} = $storage;
	};

	my $chg = $storage{$storage_name}->{'chg'};
	if (!$scan{$storage_name}) {
	    my $scan = Amanda::Recovery::Scan->new(chg => $chg,
						   interactivity => $interactivity);
	    #return $steps->{'quit'}->($scan)
	    #    if $scan{$storage->{"storage_name"}}->isa("Amanda::Changer::Error");
	    $scan{$storage_name} = $scan;
	};

	if (!$clerk{$storage_name}) {
	    if ($params{'feedback'}) {
		$params{'feedback'}->set_feedback(chg => $chg, device_name => undef);
	    }
	    my $clerk = Amanda::Recovery::Clerk->new(feedback => $params{'feedback'},
						     scan     => $scan{$storage_name});
	    $clerk{$storage_name} = $clerk;
	};
	$clerk = $clerk{$storage_name};

	$clerk->get_xfer_src(
	    dump => $current_dump,
	    xfer_src_cb => $steps->{'xfer_src_cb'});
    };

    step xfer_src_cb => sub {
	(my $errs, $hdr, $xfer_src, my $directtcp_supported) = @_;
	return $steps->{'failure'}->(
		Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300045,
			severity	=> $Amanda::Message::ERROR,
			errs		=> $errs)) if $errs;

	my $dle_str = $hdr->{'dle_str'};
	my $p1 = XML::Simple->new();
	my $dle;
	if (defined $dle_str) {
	    eval { $dle = $p1->XMLin($dle_str); };
	    if ($@) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300016,
				severity	=> $Amanda::Message::ERROR,
				dle_str		=> $dle_str,
				xml_error       => $@));
	    }
	    if (defined $dle->{'diskdevice'} and UNIVERSAL::isa( $dle->{'diskdevice'}, "HASH" )) {
		$dle->{'diskdevice'} = MIME::Base64::decode($dle->{'diskdevice'}->{'raw'});
            }
	}

	if ($self->{'feedback'}->can('send_state_file')) {
	    $self->{'feedback'}->send_state_file($hdr);
	}
	# and set up the destination..
	my $dest_fh;
	my @filters;

	# Take the CRC from the log if they are not in the header
	if (!defined $hdr->{'native_crc'} ||
            $hdr->{'native_crc'} =~ /^00000000:/) {
            $hdr->{'native_crc'} = $current_dump->{'native_crc'};
	}
	if (!defined $hdr->{'client_crc'} ||
            $hdr->{'client_crc'} =~ /^00000000:/) {
            $hdr->{'client_crc'} = $current_dump->{'client_crc'};
	}
	if (!defined $hdr->{'server_crc'} ||
            $hdr->{'server_crc'} =~ /^00000000:/) {
            $hdr->{'server_crc'} = $current_dump->{'server_crc'};
	}
	if (!$params{'extract'} and !$params{'pipe-fd'}) {
	    my $filename = sprintf("%s.%s.%s.%d",
		    $hdr->{'name'},
		    Amanda::Util::sanitise_filename("".$hdr->{'disk'}), # workaround SWIG bug
		    $hdr->{'datestamp'},
		    $hdr->{'dumplevel'});
	    if ($params{'no-reassembly'}) {
		$filename .= sprintf(".%07d", $hdr->{'partnum'});
	    }

	    # add an appropriate suffix
	    if ($params{'compress'}) {
		$filename .= ($hdr->{'compressed'} && $hdr->{'comp_suffix'})?
		    $hdr->{'comp_suffix'} : $Amanda::Constants::COMPRESS_SUFFIX;
	    }

	    if (!open($dest_fh, ">", $filename)) {
		return $steps->{'failure'}->(
		    Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300046,
			severity	=> $Amanda::Message::ERROR,
			errs		=> $filename,
			errno		=> $!));
	    }
	    $directtcp_supported = 0;
	}

	# write the header to the destination if requested
	$hdr->{'blocksize'} = Amanda::Holding::DISK_BLOCK_BYTES;
	if (defined $params{'header'} or defined $params{'header-file'} or defined $params{'header-fd'}) {
	    my $hdr_fh;
	    if (defined $params{'pipe-fd'}) {
		my $hdr_fh = $params{'pipe-fd'};
		if ($params{'feedback'}->isa('amidxtaped')) {
		    # amrecover doesn't like F_SPLIT_DUMPFILE.
		    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
		    # filter out some things the remote might not be able to process
		    if (!$params{'their_features'}->has($Amanda::Feature::fe_amrecover_dle_in_header)) {
			$hdr->{'dle_str'} = undef;
		    } else {
			$hdr->{'dle_str'} =
			    Amanda::Disklist::clean_dle_str_for_client($hdr->{'dle_str'},
				Amanda::Feature::am_features($params{'their_features'}));
		    }
		    if (!$params{'their_features'}->has($Amanda::Feature::fe_amrecover_origsize_in_header)) {
			$hdr->{'orig_size'} = 0;
		    }
		    if (!$params{'their_features'}->has($Amanda::Feature::fe_amrecover_crc_in_header)) {
			$hdr->{'native_crc'} = undef;
			$hdr->{'client_crc'} = undef;
			$hdr->{'server_crc'} = undef;
		    }
		}
		#syswrite $hdr_fh, $hdr->to_string(32768, 32768), 32768;
		Amanda::Util::full_write($hdr_fh, $hdr->to_string(32768, 32768), 32768);
	    } elsif (defined $params{'header'}) {
		syswrite $dest_fh, $hdr->to_string(32768, 32768), 32768;
		#Amanda::Util::full_write($dest_fh, $hdr->to_string(32768, 32768), 32768);
	    } elsif (defined $params{'header-file'}) {
		open($hdr_fh, ">", $params{'header-file'})
		    or return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300047,
				severity	=> $Amanda::Message::ERROR,
				header_file	=> $params{'header-file'},
				errno		=> $!));
		syswrite $hdr_fh, $hdr->to_string(32768, 32768), 32768;
		#Amanda::Util::full_write($hdr_fh, $hdr->to_string(32768, 32768), 32768);
		close($hdr_fh);
	    } elsif (defined $params{'header-fd'}) {
		open($hdr_fh, "<&".($params{'header-fd'}+0))
		    or return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300048,
				severity	=> $Amanda::Message::ERROR,
				header_fd	=> $params{'header-fd'},
				errno		=> $!));
		syswrite $hdr_fh, $hdr->to_string(32768, 32768), 32768;
		#Amanda::Util::full_write($hdr_fh, $hdr->to_string(32768, 32768), 32768);
		close($hdr_fh);
	    }
	}

	if ($params{'feedback'}->can("expect_dar")) {
	    $use_dar = $params{'feedback'}->expect_dar();
	    $self->{'ignore_dar'} =  $use_dar &&
				    ($hdr->{'compressed'} ||
				     $hdr->{'encrypted'});
	}

	$self->{'feedback'}->{'xfer_src_supports_directtcp'} = $directtcp_supported;
	if ($params{'feedback'}->can("expect_datapath")) {
	    $params{'feedback'}->expect_datapath();
	    $params{'data-path'} = $params{'feedback'}->{'datapath'};
	}

	if (defined $params{'data-path'} and $params{'data-path'} eq 'directtcp' and !$directtcp_supported) {
	    return $steps->{'failure'}->(
		    Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300049,
			severity	=> $Amanda::Message::ERROR));
	}

	$directtcp_supported = 0 if defined $params{'data-path'} and $params{'data-path'} eq 'amanda';
	$self->{'feedback'}->{'xfer_src_supports_directtcp'} = $directtcp_supported;

	if ($params{'extract'}) {
	    my $program = uc(basename($hdr->{program}));
	    my @argv;
	    if ($program ne "APPLICATION") {
		$directtcp_supported = 0;
		my %validation_programs = (
			"STAR" => [ $Amanda::Constants::STAR, qw(-x -f -) ],
			"DUMP" => [ $Amanda::Constants::RESTORE, qw(xbf 2 -) ],
			"VDUMP" => [ $Amanda::Constants::VRESTORE, qw(xf -) ],
			"VXDUMP" => [ $Amanda::Constants::VXRESTORE, qw(xbf 2 -) ],
			"XFSDUMP" => [ $Amanda::Constants::XFSRESTORE, qw(-v silent) ],
			"TAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -xf -) ],
			"GTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -xf -) ],
			"GNUTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -xf -) ],
			"SMBCLIENT" => [ $Amanda::Constants::GNUTAR, qw(-xf -) ],
			"PKZIP" => undef,
		);
		if (!exists $validation_programs{$program}) {
		    return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300050,
				severity	=> $Amanda::Message::ERROR,
				program		=> $program));
		}
		@argv = @{$validation_programs{$program}};
	    } else {
		if (!defined $hdr->{application}) {
		    return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300051,
				severity	=> $Amanda::Message::ERROR));
		}
		my $program_path = $Amanda::Paths::APPLICATION_DIR . "/" .
				   $hdr->{application};
		if (!-x $program_path) {
		    return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300052,
				severity	=> $Amanda::Message::ERROR,
				application	=> $hdr->{'application'},
				program_path	=> $program_path));
		}
		my %bsu_argv;
		$bsu_argv{'application'} = $hdr->{application};
		$bsu_argv{'config'} = Amanda::Config::get_config_name();
		$bsu_argv{'host'} = $hdr->{'name'};
		$bsu_argv{'disk'} = $hdr->{'disk'};
		$bsu_argv{'device'} = $dle->{'diskdevice'} if defined $dle->{'diskdevice'};
		my ($bsu, $err) = Amanda::Extract::BSU(%bsu_argv);
		if (defined $params{'data-path'} and $params{'data-path'} eq 'directtcp' and
		    !$bsu->{'data-path-directtcp'}) {
		    return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300053,
				severity	=> $Amanda::Message::ERROR));
		}
		if ($directtcp_supported and !$bsu->{'data-path-directtcp'}) {
		    # application do not support directtcp
		    $directtcp_supported = 0;
		}

		push @argv, $program_path, "restore";
		push @argv, "--config", Amanda::Config::get_config_name();
		push @argv, "--host", $hdr->{'name'};
		push @argv, "--disk", $hdr->{'disk'};
		push @argv, "--device", $dle->{'diskdevice'} if defined ($dle->{'diskdevice'});
		push @argv, "--level", $hdr->{'dumplevel'};
		push @argv, "--directory", $params{'directory'};
		$use_dar = ($bsu->{'dar'} and
			    !$hdr->{'compressed'} and
			    !$hdr->{'encrypted'});
		if ($use_dar) {
		    push @argv, "--dar", "YES";
		}

		if ($bsu->{'recover-dump-state-file'}) {
		    my $host = sanitise_filename("".$hdr->{'name'});
		    my $disk = sanitise_filename("".$hdr->{'disk'});
		    my $state_filename = getconf($CNF_INDEXDIR) . '/' . $host .
				 '/' . $disk . '/' . $hdr->{'datestamp'} . '_' .
				 $hdr->{'dumplevel'} . '.state';

		    if (-e $state_filename) {
			push @argv, "--recover-dump-state-file",
				    $state_filename;
		    }
		}

		# add application_property
		while (my($name, $values) = each(%{$params{'application_property'}})) {
		    if (UNIVERSAL::isa( $values, "ARRAY" )) {
			foreach my $value (@{$values}) {
			    push @argv, "--".$name, $value if defined $value;
			}
		    } else {
			push @argv, "--".$name, $values;
		    }
		}

		#merge property from header;
		if (exists $dle->{'backup-program'}->{'property'}->{'name'} and
		    !UNIVERSAL::isa($dle->{'backup-program'}->{'property'}->{'name'}, "HASH")) {
		    # header have one property
		    my $name = $dle->{'backup-program'}->{'property'}->{'name'};
		    if (!exists $params{'application_property'}{$name}) {
			my $values = $dle->{'backup-program'}->{'property'}->{'value'};
			if (UNIVERSAL::isa( $values, "ARRAY" )) {
			    # multiple values
			    foreach my $value (@{$values}) {
				push @argv, "--".$name, $value if defined $value;
			    }
			} else {
			    # one value
			    push @argv, "--".$name, $values;
			}
		    }
		} elsif (exists $dle->{'backup-program'}->{'property'}) {
		    # header have multiple properties
		    while (my($name, $values) =
			 each (%{$dle->{'backup-program'}->{'property'}})) {
			if (!exists $params{'application_property'}{$name}) {
			    if (UNIVERSAL::isa( $values->{'value'}, "ARRAY" )) {
				# multiple values
				foreach my $value (@{$values->{'value'}}) {
				    push @argv, "--".$name, $value if defined $value;
				}
			    } else {
				# one value
				push @argv, "--".$name, $values->{'value'};
			    }
			}
		    }
		}
	    }

	    $directtcp = $directtcp_supported;
	    if ($directtcp_supported) {
		$xfer_dest = Amanda::Xfer::Dest::DirectTCPListen->new();
		@directtcp_command = @argv;
	    } else {
		# set up the extraction command as a filter element, since
		# we need its stderr.
		debug("Running: ". join(' ',@argv));
		$xfer_dest = Amanda::Xfer::Dest::Application->new(\@argv, 0, 0, 0, 1);

		$dest_fh = \*STDOUT;
		#$xfer_dest = Amanda::Xfer::Dest::Buffer->new(1048576);
	    }
	} elsif ($params{'pipe-fd'}) {
	    $params{'feedback'}->{'xfer_src_supports_directtcp'} = $directtcp_supported;
	    if ($params{'feedback'}->can('check_datapath')) {
		$self->{'xfer_src_supports_directtcp'} = $directtcp_supported;
		my $err = $params{'feedback'}->check_datapath();
		return $steps->{'failure'}->($err) if $err;
		if ($params{'feedback'}->{'datapath'} ne 'directtcp') {
		    $directtcp_supported = 0;
		}
	    } else {
		    $directtcp_supported = 0;
	    }

	    if ($directtcp_supported) {
		$xfer_dest = Amanda::Xfer::Dest::DirectTCPListen->new();
	    } else {
		$xfer_dest = Amanda::Xfer::Dest::Fd->new($params{'pipe-fd'});
	    }
	    $dest_fh = $params{'pipe-fd'};
	} else {
	    $xfer_dest = Amanda::Xfer::Dest::Fd->new($dest_fh);
	}
	$self->{'xfer_src_supports_directtcp'} = $directtcp_supported;

	$timer = Amanda::MainLoop::timeout_source($self->{'delay'});
	$timer->set_callback(sub {
	    if (defined $xfer_src) {
		my $size = $xfer_src->get_bytes_read();
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300000,
				severity	=> $Amanda::Message::INFO,
				size            => $size));
	    }
	});

	$dest_is_server = 1;
	$dest_is_client = 0;
	$dest_is_native = 0;
	# set up any filters that need to be applied; decryption first
	if ($hdr->{'encrypted'} and
	    (($hdr->{'srv_encrypt'} and ($decrypt == $ALWAYS || $decrypt == $ONLY_SERVER)) ||
	     ($hdr->{'clnt_encrypt'} and ($decrypt == $ALWAYS || $decrypt == $ONLY_CLIENT)))) {
	    if ($hdr->{'srv_encrypt'}) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srv_encrypt'}, $hdr->{'srv_decrypt_opt'} ], 0, 0, 0, 1);
	    } elsif ($hdr->{'clnt_encrypt'}) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clnt_encrypt'}, $hdr->{'clnt_decrypt_opt'} ], 0, 0, 0, 1);
	    } else {
		return $steps->{'failure'}->(
			Amanda::FetchDump::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300054,
				severity	=> $Amanda::Message::INFO));
	    }

	    $hdr->{'encrypted'} = 0;
	    $hdr->{'srv_encrypt'} = '';
	    $hdr->{'srv_decrypt_opt'} = '';
	    $hdr->{'clnt_encrypt'} = '';
	    $hdr->{'clnt_decrypt_opt'} = '';
	    $hdr->{'encrypt_suffix'} = 'N';

	    if (!$hdr->{'compressed'}) {
		$native_filter = Amanda::Xfer::Filter::Crc->new();
		push @filters, $native_filter;
		$dest_is_native = 1;
	    } elsif ($hdr->{'srv_encrypt'} and
	        (!$hdr->{'srvcompprog'} and
		 $dle->{'compress'} eq "SERVER-FAST" and
		 $dle->{'compress'} eq "SERVER-BEST")) {
		$client_filter = Amanda::Xfer::Filter::Crc->new();
		push @filters, $client_filter;
		$dest_is_client = 1;
	    }
	    $dest_is_server = 0;
	}

	if ($hdr->{'compressed'} and not $params{'compress'} and
	    (($hdr->{'srvcompprog'} and ($decompress == $ALWAYS || $decompress == $ONLY_SERVER)) ||
	     ($hdr->{'clntcompprog'} and ($decompress == $ALWAYS || $decompress == $ONLY_CLIENT)) ||
	     ($dle->{'compress'} and $dle->{'compress'} eq "SERVER-FAST" and ($decompress == $ALWAYS || $decompress == $ONLY_SERVER)) ||
	     ($dle->{'compress'} and $dle->{'compress'} eq "SERVER-BEST" and ($decompress == $ALWAYS || $decompress == $ONLY_SERVER)) ||
	     ($dle->{'compress'} and $dle->{'compress'} eq "FAST" and ($decompress == $ALWAYS || $decompress == $ONLY_CLIENT)) ||
	     ($dle->{'compress'} and $dle->{'compress'} eq "BEST" and ($decompress == $ALWAYS || $decompress == $ONLY_CLIENT)))) {
	    # need to uncompress this file
	    if ($hdr->{'encrypted'}) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300017,
				severity	=> $Amanda::Message::ERROR));
	    } elsif ($hdr->{'srvcompprog'}) {
		# TODO: this assumes that srvcompprog takes "-d" to decompress
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srvcompprog'}, "-d" ], 0, 0, 0, 1);
	    } elsif ($hdr->{'clntcompprog'}) {
		# TODO: this assumes that clntcompprog takes "-d" to decompress
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clntcompprog'}, "-d" ], 0, 0, 0, 1);
	    } else {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $Amanda::Constants::UNCOMPRESS_PATH,
			  $Amanda::Constants::UNCOMPRESS_OPT ], 0, 0, 0, 1);
	    }

	    # adjust the header
	    $hdr->{'compressed'} = 0;
	    $hdr->{'uncompress_cmd'} = '';

	    $native_filter = Amanda::Xfer::Filter::Crc->new();
	    push @filters, $native_filter;
	    $dest_is_native = 1;
	    $dest_is_client = 0;
	    $dest_is_server = 0;
	} elsif (!$hdr->{'compressed'} and $params{'compress'} and not $params{'leave'}) {
	    # need to compress this file

	    my $compress_opt = $params{'compress-best'} ?
		$Amanda::Constants::COMPRESS_BEST_OPT :
		$Amanda::Constants::COMPRESS_FAST_OPT;
	    push @filters,
		Amanda::Xfer::Filter::Process->new(
		    [ $Amanda::Constants::COMPRESS_PATH,
		      $compress_opt ], 0, 0, 0, 1);

	    # adjust the header
	    $hdr->{'compressed'} = 1;
	    $hdr->{'uncompress_cmd'} = " $Amanda::Constants::UNCOMPRESS_PATH " .
		"$Amanda::Constants::UNCOMPRESS_OPT |";
	    $hdr->{'comp_suffix'} = $Amanda::Constants::COMPRESS_SUFFIX;
	    $dest_is_server = 0;
	}

	if ($xfer_app) {
	    push @filters, $xfer_app;
	}

	# start reading all filter stderr
	foreach my $filter (@filters) {
	    next if !$filter->can('get_stderr_fd');
	    my $fd = $filter->get_stderr_fd();
	    $self->start_reading($fd, $steps->{'filter_done'}, 'filter stderr');
	}

	# start reading application stdout, stderr and dar
	if ($xfer_dest->can('get_stdout_fd')) {
	    my $fd = $xfer_dest->get_stdout_fd();
	    $self->start_reading($fd, $steps->{'filter_done'}, 'application stdout');
	}
	if ($xfer_dest->can('get_stderr_fd')) {
	    my $fd = $xfer_dest->get_stderr_fd();
	    $self->start_reading($fd, $steps->{'filter_done'}, 'application stderr');
	}

	if ($use_dar) {
	    $self->{'feedback'}->start_read_dar($xfer_dest, $steps->{'dar_data'}, $steps->{'filter_done'}, 'application dar');
	    $xfer_waiting_dar = 1;
	} else {
	    push @{$current_dump->{'range'}}, "0:-1";
	}

	my $xfer;
	if (@filters) {
	    $xfer = Amanda::Xfer->new([ $xfer_src, @filters, $xfer_dest ]);
	} else {
	    $xfer = Amanda::Xfer->new([ $xfer_src, $xfer_dest ]);
	}
	$self->{'xfer_dest'} = $xfer_dest;
	$xfer->start($steps->{'handle_xmsg'}, 0, 0);
	# JLM to verify
	if ($params{'feedback'}->can('send_directtcp_datapath')) {
	    my $err = $params{'feedback'}->send_directtcp_datapath();
	    return $steps->{'failure'}->($err) if $err;
	}
	$clerk->init_recovery(
		xfer => $xfer,
		recovery_cb => $steps->{'recovery_cb'});

		$steps->{'xfer_range'}->();
	if ($self->{'feedback'}->can('start_msg')) {
	    $self->{'feedback'}->start_msg($steps->{'dar_data'});
	}
    };

    step dar_data => sub {
	my $line = shift;

	# check EOF
	if (!defined $line) {
	    return;
	}
	my ($DAR, $range) = split ' ', $line;
	my ($offset, $size) = split ':', $range;
	push @{$current_dump->{'range'}}, "$offset:$size";
	$steps->{'xfer_range'}->() if $xfer_waiting_dar;
    };

    step xfer_range => sub {
	#return if !$xfer_waiting_dar;
	my $range = shift @{$current_dump->{'range'}};
	if (defined $range) {
	    ($offset, my $range1) = split ':', $range;
	    if ($offset == -1) {
		$xfer_src->cancel(0);
		return;
	    }
	    $size = -1;
	    $size = $range1 - $offset + 1 if $range1 >= 0;;
	} elsif ($use_dar) {
	    $xfer_waiting_dar = 1;
	    return;
	} else {
	    $offset = 0;
	    $size = $current_dump->{'bytes'};
	    $size = -1 if $size == 0;
	}
	$xfer_waiting_dar = 0;

	$clerk->do_recovery(
	    xfer => $xfer,
	    offset => $offset,
	    size   => $size);

	if ($directtcp) {
	    my $addr = $xfer_dest->get_addrs();
	    push @directtcp_command, "--data-path", "DIRECTTCP";
	    push @directtcp_command, "--direct-tcp", "$addr->[0]->[0]:$addr->[0]->[1]";
	    debug("Running: ". join(' ', @directtcp_command));

	    my ($wtr, $rdr);
	    my $err = Symbol::gensym;
	    my $amndmp_pid = open3($wtr, $rdr, $err, @directtcp_command);
	    $amndmp_pid = $amndmp_pid;
	    my $file_to_close = 2;
	    my $amndmp_stdout_src = Amanda::MainLoop::fd_source($rdr,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
	    my $amndmp_stderr_src = Amanda::MainLoop::fd_source($err,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);

	    $amndmp_stdout_src->set_callback( sub {
		my $line = <$rdr>;
		if (!defined $line) {
		    $file_to_close--;
		    $amndmp_stdout_src->remove();
		    if ($file_to_close == 0) {
			#abort the xfer
			$xfer->cancel() if $xfer->get_status != $XFER_DONE;
		    }
		    return;
		}
		chomp $line;
		debug("amndmp stdout: $line");
		print "$line\n";
	    });

	    $amndmp_stderr_src->set_callback( sub {
		my $line = <$err>;
		if (!defined $line) {
		    $file_to_close--;
		    $amndmp_stderr_src->remove();
		    if ($file_to_close == 0) {
			#abort the xfer
			$xfer->cancel() if $xfer->get_status != $XFER_DONE;
		    }
		    return;
		}
		chomp $line;
		debug("amndmp stderr: $line");
		print STDERR "$line\n";
		$last_is_size = 0;
	    });
	}
    };

    step handle_xmsg => sub {
	my ($src, $msg, $xfer) = @_;

	if ($msg->{'type'} == $XMSG_CRC) {
	    if ($msg->{'elt'} == $xfer_src) {
		$source_crc = $msg->{'crc'}.":".$msg->{'size'};
		debug("source_crc: $source_crc");
	    } elsif ($msg->{'elt'} == $xfer_dest) {
		$dest_crc = $msg->{'crc'}.":".$msg->{'size'};
		debug("dest_crc: $dest_crc");
	    } elsif (defined $native_filter and $msg->{'elt'} == $native_filter) {
		$restore_native_crc =  $msg->{'crc'}.":".$msg->{'size'};
		debug("restore_native_crc: $restore_native_crc");
	    } elsif (defined $client_filter and $msg->{'elt'} == $client_filter) {
		$restore_client_crc =  $msg->{'crc'}.":".$msg->{'size'};
		debug("restore_client_crc: $restore_client_crc");
	    } else {
		debug("unhandled XMSG_CRC $msg->{'elt'}");
	    }
	} else {
	    if ($msg->{'elt'} == $xfer_app) {
		if ($msg->{'type'} == $XMSG_DONE) {
		    $app_success = 1;
		} elsif ($msg->{'type'} == $XMSG_ERROR) {
		} elsif ($msg->{'type'} == $XMSG_INFO and
			 $msg->{'message'} eq "SUCCESS") {
		    $app_success = 1;
		} elsif ($msg->{'type'} == $XMSG_INFO and
			 $msg->{'message'} eq "ERROR") {
		    $app_error = 1;
		}
	    }

	    $clerk->handle_xmsg($src, $msg, $xfer);

	    if ($msg->{'type'} == $XMSG_INFO) {
		Amanda::Debug::info($msg->{'message'});
	    } elsif ($msg->{'type'} == $XMSG_CANCEL) {
		$check_crc = 0;
	    } elsif ($msg->{'type'} == $XMSG_ERROR) {
		push @xfer_errs, $msg->{'message'};
		$check_crc = 0;
	    }

	    if ($msg->{'elt'} == $xfer_src) {
		if ($msg->{'type'} == $XMSG_SEGMENT_DONE) {
		    $xfer_waiting_dar = 1;
		    $steps->{'xfer_range'}->();
		}
	    }
	}
    };

    step recovery_cb => sub {
	%recovery_params = @_;
	$self->{'recovery_done'} = 1;

	if (!defined $self->{'all_filter'} or
	    !%{$self->{'all_filter'}}) {
	    $steps->{'filter_done'}->();
	}
    };

    step filter_done => sub {
	$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300012,
				severity	=> $Amanda::Message::INFO,
				size            => $recovery_params{'bytes_read'}));
	if ($xfer_dest && $xfer_dest->isa("Amanda::Xfer::Dest::Buffer")) {
	    my $buf = $xfer_dest->get();
	    if ($buf) {
		my @lines = split "\n", $buf;
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300013,
				severity	=> $Amanda::Message::INFO,
				application_stdout => \@lines));
	    }
	}
	@xfer_errs = (@xfer_errs, @{$recovery_params{'errors'}})
	    if $recovery_params{'errors'};

	return $steps->{'failure'}->(
		Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300055,
			severity	=> $Amanda::Message::ERROR,
			xfer_errs	=> \@xfer_errs)) if @xfer_errs;
	return $steps->{'failure'}->(
		Amanda::FetchDump::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300056,
			severity	=> $Amanda::Message::ERROR)) if $recovery_params{'result'} ne 'DONE';

	if ($check_crc) {
	    my $msg;
	    if (defined $hdr->{'native_crc'} and $hdr->{'native_crc'} !~ /^00000000:/ and
		defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
		$hdr->{'native_crc'} ne $current_dump->{'native_crc'}) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300014,
				severity	=> $Amanda::Message::ERROR,
				header_native_crc => $hdr->{'native_crc'},
				log_native_crc	=> $current_dump->{'native_crc'}));
	    }
	    if (defined $hdr->{'client_crc'} and $hdr->{'client_crc'} !~ /^00000000:/ and
		defined $current_dump->{'client_crc'} and $current_dump->{'client_crc'} !~ /^00000000:/ and
		$hdr->{'client_crc'} ne $current_dump->{'client_crc'}) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300015,
				severity	=> $Amanda::Message::ERROR,
				header_client_crc => $hdr->{'client_crc'},
				log_client_crc	=> $current_dump->{'client_crc'}));
	    }

	    my $hdr_server_crc_size;
	    my $current_dump_server_crc_size;
	    my $source_crc_size;

	    if (defined $hdr->{'server_crc'}) {
		$hdr->{'server_crc'} =~ /[^:]*:(.*)/;
		$hdr_server_crc_size = $1;
	    }
	    if (defined $current_dump->{'server_crc'}) {
		$current_dump->{'server_crc'} =~ /[^:]*:(.*)/;
		$current_dump_server_crc_size = $1;
	    }
	    if (defined $source_crc) {
		$source_crc =~ /[^:]*:(.*)/;
		$source_crc_size = $1;
	    }

	    if (defined $hdr->{'server_crc'} and $hdr->{'server_crc'} !~ /^00000000:/ and
		defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
		$hdr_server_crc_size == $current_dump_server_crc_size and
		$hdr->{'server_crc'} ne $current_dump->{'server_crc'}) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300005,
				severity	=> $Amanda::Message::ERROR,
				header_server_crc => $hdr->{'server_crc'},
				log_server_crc	=> $current_dump->{'server_crc'}));
	    }

	    if (defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
		$current_dump_server_crc_size == $source_crc_size and
		$current_dump->{'server_crc'} ne $source_crc) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300006,
				severity	=> $Amanda::Message::ERROR,
				log_server_crc	=> $current_dump->{'server_crc'},
				source_crc	=> $source_crc));
	    }

	    if (defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
		defined $restore_native_crc and $current_dump->{'native_crc'} ne $restore_native_crc) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300007,
				severity	=> $Amanda::Message::ERROR,
				log_native_crc	=> $current_dump->{'native_crc'},
				restore_native_crc => $restore_native_crc));
	    }
	    if (defined $current_dump->{'client_crc'} and $current_dump->{'client_crc'} !~ /^00000000:/ and
		defined $restore_client_crc and $current_dump->{'client_crc'} ne $restore_client_crc) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300008,
				severity	=> $Amanda::Message::ERROR,
				log_client_crc	=> $current_dump->{'client_crc'},
				restore_client_crc => $restore_client_crc));
	    }

	    debug("dest_is_native $dest_crc $restore_native_crc") if $dest_is_native;
	    debug("dest_is_client $dest_crc $restore_client_crc") if $dest_is_client;
	    debug("dest_is_server $dest_crc $source_crc") if $dest_is_server;
	    if ($dest_is_native and $restore_native_crc ne $dest_crc) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300009,
				severity	=> $Amanda::Message::ERROR,
				dest_crc	=> $dest_crc,
				restore_native_crc => $restore_native_crc));
	    }
	    if ($dest_is_client and $restore_client_crc ne $dest_crc) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300010,
				severity	=> $Amanda::Message::ERROR,
				dest_crc	=> $dest_crc,
				restore_client_crc => $restore_client_crc));
	    }
	    if ($dest_is_server and $source_crc ne $dest_crc) {
		$self->user_message(
			Amanda::FetchDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 3300011,
				severity	=> $Amanda::Message::ERROR,
				dest_crc	=> $dest_crc,
				source_crc	=> $source_crc));
	    }
	}
	$hdr = undef;
	$xfer_src = undef;
	$xfer_dest = undef;
	$native_filter = undef;
	$client_filter = undef;
	$source_crc = undef;
	$dest_crc = undef;
	$restore_native_crc = undef;
	$restore_client_crc = undef;

	$steps->{'start_dump'}->();
    };

    step finished => sub {
	if ($clerk) {
	    $clerk->quit(finished_cb => $steps->{'quit'});
	} else {
	    $steps->{'quit'}->();
	}
    };

    step quit => sub {
	my ($err) = @_;

	if (defined $timer) {
	    $timer->remove();
	    $timer = undef;
	}
	$steps->{'quit2'}->();
    };

    step quit2 => sub {
	my ($storage_name) = keys %clerk;
	if ($storage_name) {
	    my $clerk = $clerk{$storage_name};
	    delete $clerk{$storage_name};
	    return $clerk->quit(finished_cb => $steps->{'quit2'});
	}

	return $params{'finished_cb'}->($self->{'exit_status'});
    };

    step failure => sub {
	my ($msg) = @_;
	$self->user_message($msg);
	$self->{'exit_status'} = 1;
	$steps->{'quit2'}->();
    };
}
