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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use strict;
use warnings;

=head1 NAME

Amanda::Restore -- interface to restore backup

=head1 SYNOPSIS

    use Amanda::Restore;

    ($self->{'restore'}, my $result_message) = Amanda::Restore->new();
    if (@$result_message) {
        foreach my $message (@$result_message) {
            $self->user_message($message);
        }
        return $params{'finished_cb'}->(1);
    }

    $self->{'restore'}->restore(
                'application_property'  => $params{'application_property'},
                'assume'                => $params{'assume'},
                'client-decompress'     => $params{'client-decompress'},
                'client-decrypt'        => $params{'client-decrypt'},
                'compress'              => $params{'compress'},
                'compress-best'         => $params{'compress-best'},
                'data-path'             => $params{'data-path'},
                'decompress'            => $params{'decompress'},
                'decrypt'               => $params{'decrypt'},
                'device'                => $params{'device'},
                'directory'             => $params{'directory'},
                'dumpspecs'             => $params{'dumpspecs'},
                'exact-match'           => $params{'exact-match'},
                'extract'               => $params{'extract'},
                'extract-client'        => $params{'extract-client'},
                'header'                => $params{'header'},
                'header-fd'             => $params{'header-fd'},
                'header-file'           => $params{'header-file'},
                'init'                  => $params{'init'},
                'leave'                 => $params{'leave'},
                'no-reassembly'         => $params{'no-reassembly'},
                'pipe-fd'               => $params{'pipe-fd'} ? 1 : undef,
                'restore'               => $params{'restore'},
                'server-decompress'     => $params{'server-decompress'},
                'server-decrypt'        => $params{'server-decrypt'},
                'finished_cb'           => $params{'finished_cb'},
                'interactivity'         => $params{'interactivity'},
                'reserve-tapes'         => $params{'reserve-tapes'},
                'release-tapes'         => $params{'release-tapes'},
                'feedback'              => $self);

=head1 ARGUMENTS

=head2 application_property

Application properties to add when extracting.

=head2 assume

Assume all tapes are already available, do not ask for them.

=head2 chdir

Directory where to restore.

=head2 client-decompress

Decompress only if it is client compressed.

=head2 client-decrypt

Decrypt only if it is client encrypted.

=head2 compress

Compress fast the backup image.

=head2 compress-best

Compress best the backup image.

=head2 data-path

The datapath to use when extracting.

=head2 decompress

Always decompress if compressed.

=head2 decrypt

Always decrypt if encrypted.

=head2 delay

Delay in milliseconds between progress update, default to 15000 (15 seconds)

=head2 device

=head2 directory

Directory where to extract.

=head2 dumpspecs

=head2 exact-match

=head2 extract

Run the extraction on the server.

=head2 extract-client

Run the extraction on the client.

=head2 finished_cb

Callback to call when all restore are done.

=head2 header

Send the header to the same stream as the backup.

=head2 header-fd

Fd where to send the header.

=head2 header-file

Filename where to put the header.

=head2 init

Prepare for a restore, some device require it for faster restore.

=head2 interactivity

An interactivity module

=head2 leave

Leave the backup as it is (compressed/encrypted)

=head2 no-reassembly

Do not re-assemble split dump

=head2 pipe-fd

A fd where to send the dump

=head2 release-tapes

Release the tapes already reserved for the restore

=head2 reserve-tapes

Reserve the tapes needed for the restore

=head2 restore

Set to 0 to not do the restore, undef will do the restore.
use with 'init'.

=head2 server-decompress

Decompress only if it is server compressed

=head2 server-decrypt

Decrypt only if it is server encrypted

=head2 source-fd

The fd where to read the backup (do not read from device).

=head1 FEEDBACK

The feedback must Implement the Amanda::recovery::F

=head2 $feedback->user_message($msg)

A function that get an Amanda::Message as argument, it must be sent to the user.

=head2 $feedback->set_feedback($chg)

A function called withthe initial changer as first argument.

=head2 $feedback->notif_start($dump)

A function called to notify when a new dump is started.
First argument is the dump.

=head2 $hdr = $feedback->get_header()

A function to retrieve the header, the header is returned.

=head2 $feedback->clean_hdr($hdr)

A function to clean the header for compatibility.
The first argument is a reference to the header.

=head2 $feedback->set($hdr, $dle, $application_property)

This function is called after clean_hdr and before send_header.
First argument is header.
Second argument is dle.
Third argument is application_property.

=head2 $feedback->send_header

A function to send the header (first argument).

=head2 $feedback->transmit_state_file

A function to send/receive the state file (the first argument is the header).

=head2 $use_dar = $feedback->transmit_dar($use_dar)

A function to exchange dar setting. The first argument is out DAR setting.
Return 1 if DAR must be used.
Return 0 if DAR must not be used.

=head2 $firecttcp_supported = $feedback->get_datapath($directtcp_supported)

First argument if 1 if we support direccttcp.
Return 1 if other side also support directtcp.

=head2 $dest_fh = $feedback->new_dest_fh()

Allow to change the $dest_fh

=head2 $xfer_dest = $feedback->get_xfer_dest

A function that create and return the xfer_dest

=head2 $feedback->get_mesg_fd()

return the fd of the MESG stream

=head2 $feedback->get_mesg_json()

return 1 if the MESG stream is in JSON

=head2 $feedback->get_stdout_fd()

return the stdout of the application

=head2 $feedback->get_stderr_fd()

return the stderr of the application

=head2 $feedback->start_read_dar()

A function to start reading DAR request

=head2 $feedback->send_amanda_datapath()

A function to send/receive aggrement on the amanda datapath

=head2 $feedback->send_directtcp_datapath()

A function to send/receive aggrement on the directtcp datapath

=head2 $feedback->notify_start_backup()

A function to notify the start of a restore, it is called after send_*_datapath

=head2 $feedback->start_msg($dar_data_cb)

Start read fromthe application CTL stream
The first argument is a callback that must be called with lien like: 'DAR offset:length'

=head2 $feedback->send_dar_data($line)

A function to send a 'DAR offset:length' line

=head2 $feedback->run_directtcp_application($xfer)

A function to run an applicationwith directtcp datapath

=head2 $feedback->run_pre_scripts

A function to run the PRE-*-RECOVER scripts

=head2 $feedback->run_post_scripts

A function to run the POST-*-RECOVER scripts

=cut

package Amanda::Restore::Message;
use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 4900000) {
        return int($self->{'size'}/1024) . " kb";
    } elsif ($self->{'code'} == 4900001) {
        return "WARNING: Fetch first dump only because sending data stream to a pipe";
    } elsif ($self->{'code'} == 4900002) {
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
    } elsif ($self->{'code'} == 4900005) {
        return "recovery failed: server-crc in header ($self->{'header_server_crc'}) and server-crc in log ($self->{'log_server_crc'}) differ";
    } elsif ($self->{'code'} == 4900006) {
        return "recovery failed: server-crc ($self->{'log_server_crc'}) and source_crc ($self->{'source_crc'}) differ",
    } elsif ($self->{'code'} == 4900007) {
        return "recovery failed: native-crc ($self->{'log_native_crc'}) and restore-native-crc ($self->{'restore_native_crc'}) differ";
    } elsif ($self->{'code'} == 4900008) {
        return "recovery failed: client-crc ($self->{'log_client_crc'}) and restore-client-crc ($self->{'restore_client_crc'}) differ";
    } elsif ($self->{'code'} == 4900009) {
        return "recovery failed: dest-crc ($self->{'dest_crc'}) and restore-native-crc ($self->{'restore_native_crc'}) differ";
    } elsif ($self->{'code'} == 4900010) {
        return "recovery failed: dest-crc ($self->{'dest_crc'}) and restore-client-crc ($self->{'restore_client_crc'}) differ";
    } elsif ($self->{'code'} == 4900011) {
        return "recovery failed: dest-crc ($self->{'dest_crc'}) and source-crc ($self->{'source_crc'}) differ";
    } elsif ($self->{'code'} == 4900012) {
        return int($self->{'size'}/1024) . " kb";
    } elsif ($self->{'code'} == 4900013) {
	return "The application stdout";
    } elsif ($self->{'code'} == 4900014) {
        return "recovery failed: native-crc in header ($self->{'header_native_crc'}) and native-crc in log ($self->{'log_native_crc'}) differ";
    } elsif ($self->{'code'} == 4900015) {
        return "recovery failed: client-crc in header ($self->{'header_client_crc'}) and client-crc in log ($self->{'log_client_crc'}) differ";
    } elsif ($self->{'code'} == 4900016) {
        return "ERROR: XML error: $self->{'xml_error'}\n$self->{'dle_str'}";
    } elsif ($self->{'code'} == 4900017) {
        return "Not decompressing because the backup image is not decrypted";
    } elsif ($self->{'code'} == 4900018) {
        return "$self->{'text'}: $self->{'line'}";
    } elsif ($self->{'code'} == 4900021) {
	return "'compress' is not compatible with 'compress-best'";
    } elsif ($self->{'code'} == 4900022) {
	return "'leave' is not compatible with 'compress'";
    } elsif ($self->{'code'} == 4900023) {
	return "'leave' is not compatible with 'compress-best'";
    } elsif ($self->{'code'} == 4900024) {
	return "'pipe-fd' is not compatible with 'no-reassembly'";
    } elsif ($self->{'code'} == 4900025) {
	return "'header', 'header-file' and 'header-fd' are mutually incompatible";
    } elsif ($self->{'code'} == 4900026) {
	return "'data-path' must be 'amanda' or 'directtcp'";
    } elsif ($self->{'code'} == 4900027) {
	return "'leave' is incompatible with 'decrypt'";
    } elsif ($self->{'code'} == 4900028) {
	return "'leave' is incompatible with 'server-decrypt'"
    } elsif ($self->{'code'} == 4900029) {
	return "'leave' is incompatible with 'client-decrypt'"
    } elsif ($self->{'code'} == 4900030) {
	return "'leave' is incompatible with 'decompress'";
    } elsif ($self->{'code'} == 4900031) {
	return "'leave' is incompatible with 'server-decompress'"
    } elsif ($self->{'code'} == 4900032) {
	return "'leave' is incompatible with 'client-decompress'"
    } elsif ($self->{'code'} == 4900034) {
	return "'server-decrypt' or 'client-decrypt' is incompatible with 'extract'";
    } elsif ($self->{'code'} == 4900035) {
	return "'server-decompress' or 'client-decompress' is incompatible with 'extract'";
    } elsif ($self->{'code'} == 4900036) {
	return "Can't use 'leave' or 'compress' with 'extract'";
    } elsif ($self->{'code'} == 4900037) {
	return "'pipe-fd' is incompatible with 'extract'";
    } elsif ($self->{'code'} == 4900038) {
	return "'header' is incompatible wth 'extract'";
    } elsif ($self->{'code'} == 4900039) {
	return "Can't use only on of 'decrypt', 'no-decrypt', 'server-decrypt' or 'client-decrypt'";
    } elsif ($self->{'code'} == 4900040) {
	return "Can't use only on of 'decompress', 'no-decompress', 'server-decompress' or 'client-decompress'";
    } elsif ($self->{'code'} == 4900041) {
	return "Can't specify 'compress' with one of 'decompress', 'no-decompress', 'server-decompress' or 'client-decompress'";
    } elsif ($self->{'code'} == 4900042) {
	return "Can't specify 'compress-best' with one of 'decompress', 'no-decompress', 'server-decompress' or 'client-decompress'";
    } elsif ($self->{'code'} == 4900043) {
	return "Cannot chdir to $self->{'dir'}: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 4900044) {
	return "No matching dumps found";
    } elsif ($self->{'code'} == 4900045) {
	return join("; ", @{$self->{'errs'}});
    } elsif ($self->{'code'} == 4900046) {
	return "Could not open '$self->{'filename'}' for writing: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 4900047) {
	return "could not open '$self->{'header_file'}': $self->('errnostr'}";
    } elsif ($self->{'code'} == 4900048) {
	return "could not open fd '$self->{'header_fd'}': $self->('errnostr'}";
    } elsif ($self->{'code'} == 4900049) {
	return "The device can't do directtcp";
    } elsif ($self->{'code'} == 4900054) {
	return "could not decrypt encrypted dump: no program specified";
    } elsif ($self->{'code'} == 4900055) {
	return join("; ", @{$self->{'xfer_errs'}});
    } elsif ($self->{'code'} == 4900056) {
	return "recovery failed";
    } elsif ($self->{'code'} == 4900060) {
	return "'directory' must be set if 'extract' is set"
    } elsif ($self->{'code'} == 4900061) {
	return "'directory' must be set if 'extract-client' is set"
    } elsif ($self->{'code'} == 4900062) {
	return "Exit status: $self->{'exit_status'}";
    } elsif ($self->{'code'} == 4900066) {
	return "Can't open message file '$self->{'message_filename'}' for writing: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 4900067) {
	return "Storage $self->{'storage'} have no changer";
    } elsif ($self->{'code'} == 4900068) {
	return "$self->{'msg'}";
    } else {
	return "No mesage for code '$self->{'code'}'";
    }
}

sub local_full_message {
    my $self = shift;

    if ($self->{'code'} == 4900013) {
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

package Amanda::Restore;

use POSIX qw(strftime);
use File::Basename;
use XML::Simple;
use IPC::Open3;
use IPC::Open2;
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
use Amanda::Cmdfile;
use Amanda::Xfer qw( :constants );
use Amanda::Recovery::Planner;
use Amanda::Recovery::Clerk;
use Amanda::Recovery::Scan;
use Amanda::Extract;
use Amanda::Feature;
#use Amanda::CheckDump; # for Amanda::CheckDump::Message
use Amanda::Logfile qw( :logtype_t log_add log_add_full );
use MIME::Base64 ();

my $NEVER = 0;
my $ALWAYS = 1;		# always decompress/decrypt
my $ONLY_SERVER = 2;	# decompress/decrypt if server compressed/encrypted
my $ONLY_CLIENT = 3;	# decompress/decrypt if client compressed/encrypted

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
    $self->{'image_status'} = 0;
    $self->{'exit_status'} = 0;
    $self->{'amlibexecdir'} = 0;

    $self->{'message_pathname'} = $params{'message_pathname'};

    # Must be opened in append so that all subprocess can write to it.
    if (defined $self->{'message_file'}) {
	if (!open($self->{'message_file'}, ">>", $self->{'message_pathname'})) {
	    push @result_messages, Amanda::Restore::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 4900066,
		message_filename => $self->{'message_pathname'},
		errno            => $!,
		severity         => $Amanda::Message::ERROR);
	    $self->{'exit_status'} = 1;
	} else {
	    $self->{'message_file'}->autoflush;
	    log_add($L_INFO, "message file $self->{'message_pathname'}");
	}
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
#		if (length($line) > 1) {
		    if ($line !~ /trailing garbage ignored/) {
			my $msg_severity;
			if ($text eq 'application stdout') {
			    $msg_severity = $Amanda::Message::INFO;
			} else {
			    $msg_severity = $Amanda::Message::ERROR;
			    $self->{'image_status'} = 1;
			    $self->{'exit_status'} = 1;
			}
			$self->user_message(
			    Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900018,
				text		=> $text,
				severity	=> $msg_severity,
				line		=> $line));
		    }
#		}
		$buffer = "";
	    }
	}
    });
}

sub start_reading_json
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
    my $line = "";
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
	    $line .= $b;
	    if ($b eq "\n") {
		$buffer .= $line;
		if ($line eq "}\n") {
		    my $msg = Amanda::Message->new_from_json_text($buffer);
		    $buffer = "";
		    $self->user_message($msg);
		}
		$line = "";
	    }
	}
    });
}

sub user_message {
    my $self = shift;
    my $msg = shift;

    if (defined $self->{'message_file'}) {
	my $d;
	if (!ref $msg) {
	    $d = Data::Dumper->new([$msg], ["MESSAGES[$self->{'message_count'}]"]);
	} elsif (ref $msg eq "SCALAR") {
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

    $self->{'nb_image'} = 0;
    $self->{'image_restored'} = 0;
    $self->{'image_failed'} = 0;
    $self->{'feedback'} = $params{'feedback'};

    if (defined $params{'compress'} and defined $params{'compress-best'}) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900021,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined $params{'leave'} and defined $params{'compress'}) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900022,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'leave'} and defined $params{'compress-best'}) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900023,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'pipe-fd'} and defined $params{'no-reassembly'}) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900024,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'header'} and (defined $params{'header-file'} or
				       defined $params{'header-fd'})) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900025,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined $params{'data-path'}) {
	$params{'data-path'} = lc($params{'data-path'});
	if ($params{'data-path'} ne 'directtcp' and
	    $params{'data-path'} ne 'amanda') {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900026,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
    }

    if ($params{'leave'}) {
	if ($params{'decrypt'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900027,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'server-decrypt'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900028,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'client-decrypt'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900029,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'decompress'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900030,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'server-decompress'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900031,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if ($params{'client-decompress'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900032,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
    }

    if (defined $params{'extract'} && !exists $params{'directory'}) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900060,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if (defined $params{'extract-client'} && !defined $params{'directory'}) {
	$self->user_message(
	    Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900061,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if (defined $params{'directory'} and defined $params{'extract'}) {
	$params{'decrypt'} = 1;
#	if (defined $params{'server-decrypt'} or defined $params{'client-decrypt'}) {
#	    $self->user_message(
#		Amanda::Restore::Message->new(
#			source_filename => __FILE__,
#			source_line     => __LINE__,
#			code            => 4900034,
#			severity	=> $Amanda::Message::ERROR));
#	    return $params{'finished_cb'}->(1);
#	}

	$params{'decompress'} = 1;
#	if (defined $params{'server-decompress'} || defined $params{'client-decompress'}) {
#	    $self->user_message(
#		Amanda::Restore::Message->new(
#			source_filename => __FILE__,
#			source_line     => __LINE__,
#			code            => 4900035,
#			severity	=> $Amanda::Message::ERROR));
#	    return $params{'finished_cb'}->(1);
#	}
	if (defined($params{'leave'}) +
	    defined($params{'compress'}) +
	    defined($params{'compress-best'})) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900036,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if (defined $params{'pipe-fd'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900037,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
	if (defined $params{'header'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900038,
			severity	=> $Amanda::Message::ERROR));
	    return $params{'finished_cb'}->(1);
	}
    }

    if ((defined($params{'decrypt'})?$params{'decrypt'}:0) +
	(defined($params{'server-decrypt'}) ? $params{'server-decrypt'}:0) +
	(defined($params{'client-decrypt'}) ? $params{'client-decrypt'}:0) > 1) {
	$self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900039,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }
    if ((defined($params{'decompress'})?$params{'decompress'}:0) +
	(defined($params{'server-decompress'}) ? $params{'server-decompress'}:0) +
	(defined($params{'client-decompress'}) ? $params{'client-decompress'}:0) > 1) {
	$self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900040,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if ($params{'compress'} and
        ((defined($params{'decompress'})?$params{'decompress'}:0) +
	 (defined($params{'server-decompress'}) ? $params{'server-decompress'}:0) +
	 (defined($params{'client-decompress'}) ? $params{'client-decompress'}:0) > 0)) {
	$self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900041,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    if ($params{'compress-best'} and
        ((defined($params{'decompress'})?$params{'decompress'}:0) +
	 (defined($params{'server-decompress'}) ? $params{'server-decompress'}:0) +
	 (defined($params{'client-decompress'}) ? $params{'client-decompress'}:0) > 0)) {
	$self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900042,
			severity	=> $Amanda::Message::ERROR));
	return $params{'finished_cb'}->(1);
    }

    my $decompress = $ALWAYS;
    my $decrypt = $ALWAYS;

    $decrypt = $NEVER  if defined $params{'leave'} && $params{'leave'};
    $decrypt = $NEVER  if defined $params{'decrypt'} and !$params{'decrypt'};
    $decrypt = $ALWAYS if defined $params{'decrypt'} and $params{'decrypt'};
    $decrypt = $ONLY_SERVER if defined $params{'server-decrypt'} && $params{'server-decrypt'};
    $decrypt = $ONLY_CLIENT if defined $params{'client-decrypt'} && $params{'client-decrypt'};

    $params{'compress'} = 1 if $params{'compress-best'};

    $decompress = $NEVER  if defined $params{'compress'} && $params{'compress'};
    $decompress = $NEVER  if defined $params{'leave'} && $params{'leave'};
    $decompress = $NEVER  if defined $params{'decompress'} and !$params{'decompress'};
    $decompress = $ALWAYS if defined $params{'decompress'} and $params{'decompress'};
    $decompress = $ONLY_SERVER if defined $params{'server-decompress'} && $params{'server-decompress'};
    $decompress = $ONLY_CLIENT if defined $params{'client-decompress'} && $params{'client-decompress'};


    my $source_fd = $params{'source-fd'};
    my $hdr = $params{'hdr'};

    use Data::Dumper;

    my $current_dump;
    my $plan;
    my @xfer_errs;
    my %recovery_params;
    my $timer;
    my $is_tty;
    my $directtcp = 0;
    my @directtcp_command;
    my @init_needed_labels;
    my $init_label;
    my %scan;
    my $clerk;
    my %clerk;
    my %storage;
    my $interactivity;
    my $source_crc;
    my $dest_crc;
    my $xfer_src;
    my $xfer_dest;
    my $xfer_dest_crc;
    my $client_filter;
    my $native_filter;
    my $restore_native_crc;
    my $restore_client_crc;
    my $dest_is_server;
    my $dest_is_client;
    my $dest_is_native;
    my $check_crc;
    my $tl;
    my $offset;
    my $size;
    my $xfer;
    my $use_dar = 0;
    my $xfer_waiting_dar = 0;

    my $steps = define_steps
	cb_ref => \$params{'finished_cb'},
	finalize => sub { foreach my $name (keys %storage) {
			    $storage{$name}->quit();
			  }
debug("XYZ");
			  log_add($L_INFO, "pid-done $$");
			};

    step start => sub {

	$self->{'delay'} = $params{'delay'} if defined $params{'delay'};
	if (defined $source_fd) {
	    my $xfer_src = Amanda::Xfer::Source::Fd->new($source_fd);
	    return $steps->{'client_recovery'}->($xfer_src);
	}

	my $chg;

	my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	($tl, my $message) = Amanda::Tapelist->new($tlf);
	if (defined $message) {
	    die "Could not read the tapelist: $message";
	}

	# first, go to params{'chdir'} or the original working directory we
	# were started in
	my $destdir;
	if ($params{'chdir'} && $params{'chdir'} =~ /^\// && $params{'chdir'} !~ /^\/\//) {
	    $destdir = $params{'chdir'};
	} else {
	    $destdir = Amanda::Util::get_original_cwd();
	}
	if (!chdir($destdir)) {
	    return $steps->{'failure'}->(
		Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900043,
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
	    $scan = $params{'scan'};
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
		all_copy => $params{'all_copy'},
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
		Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900044,
			severity	=> $Amanda::Message::ERROR));
	}

	# if we are doing a -p operation, only keep the first dump
	if ($params{'pipe-fd'}) {
	    $self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900001,
			severity	=> $Amanda::Message::INFO)) if @{$plan->{'dumps'}} > 1;
	    @{$plan->{'dumps'}} = ($plan->{'dumps'}[0]);
	}
	$self->{'nb_image'} = @{$plan->{'dumps'}};
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

	my $conf_cmdfile = config_dir_relative(getconf($CNF_CMDFILE));
	my $cmdfile = Amanda::Cmdfile->new($conf_cmdfile);
	my $cmd_added;
	my $last_label;

	if ($params{'reserve-tapes'}) {
debug("plan: " . Data::Dumper::Dumper($plan->{'dumps'}));
	    for my $dump (@{$plan->{'dumps'}}) {
		for my $part (@{$dump->{'parts'}}) {
		    next unless defined $part; # skip parts[0]
		    if (defined $part->{'label'}) {
			if (!defined $last_label ||
			    $part->{'label'} ne $last_label) {
			    $last_label = $part->{'label'};
			    my $cmddata = Amanda::Cmdfile->new_Cmddata(
				operation      => $Amanda::Cmdfile::CMD_RESTORE,
				config         => get_config_name(),
				hostname       => $dump->{'hostname'},
				diskname       => $dump->{'diskname'},
				dump_timestamp => $dump->{'dump_timestamp'},
				src_storage    => $dump->{'storage'},
				src_pool       => $dump->{'pool'},
				src_label      => $part->{'label'},
				#start_time     => time,
				status         => $Amanda::Cmdfile::CMD_TODO);
			    my $id = $cmdfile->add_to_memory($cmddata);
			    $cmd_added++;
			}
		    } else {
			my $cmddata = Amanda::Cmdfile->new_Cmddata(
			    operation      => $Amanda::Cmdfile::CMD_RESTORE,
			    config         => get_config_name(),
			    hostname       => $dump->{'hostname'},
			    diskname       => $dump->{'diskname'},
			    dump_timestamp => $dump->{'dump_timestamp'},
			    src_storage    => 'HOLDING',
			    src_pool       => 'HOLDING',
			    holding_file   => $part->{'holding_file'},
			    #start_time     => time,
			    status         => $Amanda::Cmdfile::CMD_TODO);
			my $id = $cmdfile->add_to_memory($cmddata);
			$cmd_added++;
		    }
		}
	    }
	}

	if ($params{'release-tapes'}) {
	    for my $dump (@{$plan->{'dumps'}}) {
		for my $part (@{$dump->{'parts'}}) {
		    next unless defined $part; # skip parts[0]
		    if (defined $part->{'label'}) {
			if (!defined $last_label ||
			    $part->{'label'} ne $last_label) {
			    $last_label = $part->{'label'};
			    $cmdfile->remove_for_restore_label(
				$dump->{'hostname'},
				$dump->{'diskname'},
				$dump->{'dump_timestamp'},
				$dump->{'storage'},
				$dump->{'pool'},
				$part->{'label'});
			    $cmd_added++;
			}
		    } else {
			$cmdfile->remove_for_restore_holding(
				$dump->{'hostname'},
				$dump->{'diskname'},
				$dump->{'dump_timestamp'},
				$part->{'holding_file'});
			$cmd_added++;
		    }
		}
	    }
	}

	if ($cmd_added) {
	    $cmdfile->write();
	    return $steps->{'finished'}->();
	}

	#reserve the tapes
	for my $dump (@{$plan->{'dumps'}}) {
	    for my $part (@{$dump->{'parts'}}) {
		next unless defined $part; # skip parts[0]
		if (defined $part->{'label'}) {
		    if (!defined $last_label ||
			$part->{'label'} ne $last_label) {
			$last_label = $part->{'label'};
			my $cmddata = Amanda::Cmdfile->new_Cmddata(
			    operation      => $Amanda::Cmdfile::CMD_RESTORE,
			    config         => get_config_name(),
			    hostname       => $dump->{'hostname'},
			    diskname       => $dump->{'diskname'},
			    dump_timestamp => $dump->{'dump_timestamp'},
			    src_storage    => $part->{'storage'},
			    src_pool       => $part->{'pool'},
			    src_label      => $part->{'label'},
			    working_pid    => $$,
			    status         => $Amanda::Cmdfile::CMD_WORKING);
			my $id = $cmdfile->add_to_memory($cmddata);
			$cmd_added++;
		    }
		} else {
		    my $cmddata = Amanda::Cmdfile->new_Cmddata(
			operation      => $Amanda::Cmdfile::CMD_RESTORE,
			config         => get_config_name(),
			hostname       => $dump->{'hostname'},
			diskname       => $dump->{'diskname'},
			dump_timestamp => $dump->{'dump_timestamp'},
			holding_file   => $part->{'holding_file'},
			working_pid    => $$,
			status         => $Amanda::Cmdfile::CMD_WORKING);
		    my $id = $cmdfile->add_to_memory($cmddata);
		    $cmd_added++;
		}
	    }
	}

	if ($cmd_added) {
	    $cmdfile->write();
	} else {
	    $cmdfile->unlock();
	}

	$self->user_message(
		Amanda::Restore::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900002,
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

	$self->{'image_status'} = 0;
	$self->{'recovery_done'} = 0;
	$use_dar = 0;
	%recovery_params = ();
	$check_crc = !$params{'no-reassembly'};
	$self->{'feedback'}->notif_start($current_dump) if $self->{'feedback'}->can('notif_start');

	my $storage_name = $current_dump->{'storage'};
	my $chg;

	if ($storage_name ne 'HOLDING') {
	    if (!$storage{$storage_name}) {
		my ($storage) = Amanda::Storage->new(
						storage_name => $storage_name,
						tapelist => $tl);
		return  $steps->{'failure'}->($storage)
		    if $storage->isa("Amanda::Changer::Error");
		$storage{$storage_name} = $storage;
	    };

	    $chg = $storage{$storage_name}->{'chg'};
	    return  $steps->{'failure'}->(
		Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900067,
			severity	=> $Amanda::Message::ERROR,
			storage		=> $storage_name))
		if !defined $chg;
	    return  $steps->{'failure'}->($chg)
		if $chg->isa("Amanda::Changer::Error");

	    if (!$scan{$storage_name}) {
		my $scan = Amanda::Recovery::Scan->new(
					chg => $chg,
					interactivity => $interactivity);
		return $steps->{'failure'}->($scan)
	            if $scan->isa("Amanda::Changer::Error");
		$scan{$storage_name} = $scan;
	    };
	}

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

    step client_recovery => sub {
	$xfer_src = shift;

	$self->{'nb_image'} = 1;
	$self->{'recovery_done'} = 0;
	$use_dar = 0;
	%recovery_params = ();
	$check_crc = !$params{'no-reassembly'};

	if ($params{'feedback'}->can('get_header')) {
	    $hdr = $params{'feedback'}->get_header();
	    return $steps->{'failure'}->($hdr) if ref $hdr eq "HASH" && $hdr->isa('Amanda::Message');
	}

	return $steps->{'xfer_src_cb'}->(undef, $hdr, $xfer_src, 0);
    };

    step xfer_src_cb => sub {
	(my $errs, $hdr, $xfer_src, my $directtcp_supported) = @_;
	return $steps->{'failure'}->(
		Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900045,
			severity	=> $Amanda::Message::ERROR,
			errs		=> $errs)) if $errs;

	delete $self->{'all_filter'};
	my $dle_str = $hdr->{'dle_str'};
	my $p1 = XML::Simple->new();
	my $dle;
	if (defined $dle_str) {
	    eval { $dle = $p1->XMLin($dle_str, ForceArray => ['property']); };
	    if ($@) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900016,
				severity	=> $Amanda::Message::ERROR,
				dle_str		=> $dle_str,
				xml_error       => $@));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }
	    if (defined $dle->{'diskdevice'} and UNIVERSAL::isa( $dle->{'diskdevice'}, "HASH" )) {
		$dle->{'diskdevice'} = MIME::Base64::decode($dle->{'diskdevice'}->{'raw'});
            }
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
	if (!$params{'extract'} and !$params{'extract-client'} and !$params{'pipe-fd'}) {
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
		    Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900046,
			severity	=> $Amanda::Message::ERROR,
			errs		=> $filename,
			errno		=> $!));
	    }
	    $directtcp_supported = 0;
	}

	my $filtered = 0;
	# set up any filters that need to be applied; decryption first
	if ($hdr->{'encrypted'} and
	    (($hdr->{'srv_encrypt'} and ($decrypt == $ALWAYS || $decrypt == $ONLY_SERVER)) ||
	     ($hdr->{'clnt_encrypt'} and ($decrypt == $ALWAYS || $decrypt == $ONLY_CLIENT)))) {
	    $filtered = 1;
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
			Amanda::Restore::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900054,
				severity	=> $Amanda::Message::INFO));
	    }

	    $hdr->{'encrypted'} = 0;
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
	    $filtered = 1;
	    # need to uncompress this file
	    if ($hdr->{'encrypted'}) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900017,
				severity	=> $Amanda::Message::ERROR));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    } elsif ($hdr->{'srvcompprog'}) {
		# TODO: this assumes that srvcompprog takes "-d" to decompress
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srvcompprog'}, "-d" ], 0, 0, 0, 1);
		$hdr->{'srvcompprog'} = '';
	    } elsif ($hdr->{'clntcompprog'}) {
		# TODO: this assumes that clntcompprog takes "-d" to decompress
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clntcompprog'}, "-d" ], 0, 0, 0, 1);
		$hdr->{'clntcompprog'} = '';
	    } else {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $Amanda::Constants::UNCOMPRESS_PATH,
			  $Amanda::Constants::UNCOMPRESS_OPT ], 0, 0, 0, 1);
	    }
	    $dle->{'compress'} = "NONE";

	    # adjust the header
	    $hdr->{'compressed'} = 0;
	    $hdr->{'uncompress_cmd'} = '';
	    $hdr->{'comp_suffix'} = '';

	    $native_filter = Amanda::Xfer::Filter::Crc->new();
	    push @filters, $native_filter;
	    $dest_is_native = 1;
	    $dest_is_client = 0;
	    $dest_is_server = 0;
	} elsif (!$hdr->{'compressed'} and $params{'compress'} and not $params{'leave'}) {
	    # need to compress this file

	    $filtered = 1;
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
        $use_dar |= !$filtered && !$hdr->{'compressed'} && !$hdr->{'encrypted'};

	# write the header to the destination if requested
	$hdr->{'blocksize'} = Amanda::Holding::DISK_BLOCK_BYTES;

	if ($params{'feedback'}->can('clean_hdr')) {
	    $params{'feedback'}->clean_hdr($hdr);
	}

	if ($params{'feedback'}->can('set')) {
	    my $err = $params{'feedback'}->set($hdr, $dle, $params{'application_property'});
	    return $steps->{'failure'}->($err) if defined $err;
	}

	if ($params{'feedback'}->can('send_header')) {
	    my $err = $params{'feedback'}->send_header($hdr);
	    return $steps->{'failure'}->($err) if defined $err;
	} elsif (defined $params{'header'} or defined $params{'header-file'} or defined $params{'header-fd'}) {
	    my $hdr_fh;
	    if (defined $params{'pipe-fd'}) {
		my $hdr_fh = $params{'pipe-fd'};
		#syswrite $hdr_fh, $hdr->to_string(32768, 32768), 32768;
		Amanda::Util::full_write($hdr_fh, $hdr->to_string(32768, 32768), 32768);
	    } elsif (defined $params{'header'}) {
		syswrite $dest_fh, $hdr->to_string(32768, 32768), 32768;
		#Amanda::Util::full_write($dest_fh, $hdr->to_string(32768, 32768), 32768);
	    } elsif (defined $params{'header-file'}) {
		open($hdr_fh, ">", $params{'header-file'})
		    or return $steps->{'failure'}->(
			Amanda::Restore::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900047,
				severity	=> $Amanda::Message::ERROR,
				header_file	=> $params{'header-file'},
				errno		=> $!));
		syswrite $hdr_fh, $hdr->to_string(32768, 32768), 32768;
		#Amanda::Util::full_write($hdr_fh, $hdr->to_string(32768, 32768), 32768);
		close($hdr_fh);
	    } elsif (defined $params{'header-fd'}) {
		open($hdr_fh, "<&".($params{'header-fd'}+0))
		    or return $steps->{'failure'}->(
			Amanda::Restore::Message-> new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900048,
				severity	=> $Amanda::Message::ERROR,
				header_fd	=> $params{'header-fd'},
				errno		=> $!));
		syswrite $hdr_fh, $hdr->to_string(32768, 32768), 32768;
		#Amanda::Util::full_write($hdr_fh, $hdr->to_string(32768, 32768), 32768);
		close($hdr_fh);
	    }
	}

	if ($self->{'feedback'}->can('transmit_state_file')) {
	    my $err = $self->{'feedback'}->transmit_state_file($hdr);
	    return $steps->{'failure'}->($err) if defined $err;
	}

	if ($params{'feedback'}->can('transmit_dar')) {
	    $use_dar = $params{'feedback'}->transmit_dar($use_dar);
	    return $steps->{'failure'}->($use_dar) if ref $use_dar eq "HASH" && $use_dar->isa('Amanda::Message');
	}

	if ($params{'feedback'}->can('get_datapath')) {
	    $directtcp_supported = $params{'feedback'}->get_datapath($directtcp_supported);
	    return $steps->{'failure'}->($directtcp_supported) if ref $directtcp_supported eq "HASH" && $directtcp_supported->isa('Amanda::Message');
	}

	if (defined $params{'data-path'} and $params{'data-path'} eq 'directtcp' and !$directtcp_supported) {
	    return $steps->{'failure'}->(
		    Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900049,
			severity	=> $Amanda::Message::ERROR));
	}
	if ($params{'feedback'}->can('send_amanda_datapath')) {
	} else {
	    $directtcp_supported = 0;
	}

	if ($params{'extract'}) {
	    if ($self->{'feedback'}->can('run_pre_scripts')) {
		$self->{'feedback'}->run_pre_scripts();
	    }
	    $xfer_dest = $self->{'feedback'}->get_xfer_dest();
	    return $steps->{'failure'}->($xfer_dest) if $xfer_dest->isa('Amanda::Message');
	    if ($self->{'feedback'}->can('new_dest_fh')) {
		if (my $new_dest_fh = $self->{'feedback'}->new_dest_fh()) {
		    return $steps->{'failure'}->($new_dest_fh) if ref $new_dest_fh eq "HASH" && $new_dest_fh->isa('Amanda::Message');
		    $dest_fh = $new_dest_fh;
		}
	    }
	} elsif ($params{'extract-client'}) {
	    if ($self->{'feedback'}->can('run_pre_scripts')) {
		$self->{'feedback'}->run_pre_scripts();
	    }
	    $xfer_dest = $self->{'feedback'}->get_xfer_dest();
	    return $steps->{'failure'}->($xfer_dest) if $xfer_dest->isa('Amanda::Message');
	} elsif ($params{'pipe-fd'}) {
	    if ($directtcp_supported) {
		$xfer_dest = Amanda::Xfer::Dest::DirectTCPListen->new();
	    } else {
		$xfer_dest = Amanda::Xfer::Dest::Fd->new($params{'pipe-fd'});
	    }
	    $dest_fh = $params{'pipe-fd'};
	} else {
	    $xfer_dest = Amanda::Xfer::Dest::Fd->new($dest_fh);
	    close($dest_fh);
	    $dest_fh = undef;
	}
	$self->{'xfer_dest'} = $xfer_dest;

	if (defined $clerk) {
	    $timer = Amanda::MainLoop::timeout_source($self->{'delay'});
	    $timer->set_callback(sub {
		if (defined $xfer_src) {
		    my $size = $xfer_src->get_bytes_read();
		    $self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900000,
				severity	=> $Amanda::Message::INFO,
				size            => $size));
		}
	    });
	}

	if ($self->{'feedback'}->can('get_mesg_fd')) {
	    my $fd = $self->{'feedback'}->get_mesg_fd();
	    if ($self->{'feedback'}->get_mesg_json()) {
		$self->start_reading_json($fd, $steps->{'filter_done'}, 'client message');
	    } else {
		$self->start_reading($fd, $steps->{'filter_done'}, 'client message');
	    }
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
	if (@filters) {
	    $xfer = Amanda::Xfer->new([ $xfer_src, @filters, $xfer_dest ]);
	} else {
	    $xfer = Amanda::Xfer->new([ $xfer_src, $xfer_dest ]);
	}
	$xfer->start($steps->{'handle_xmsg'}, 0, 0);
	if ($params{'feedback'}->can('send_amanda_datapath')) {
	    my $err = $params{'feedback'}->send_amanda_datapath();
	    return $steps->{'failure'}->($err) if $err;
	    if ($params{'feedback'}->{'datapath'} ne 'directtcp') {
		$directtcp_supported = 0;
	    }
	} else {
	    $directtcp_supported = 0;
	}

	# JLM to verify
	if ($params{'feedback'}->can('send_directtcp_datapath')) {
	    my $err = $params{'feedback'}->send_directtcp_datapath();
	    return $steps->{'failure'}->($err) if $err;
	}
	if ($self->{'feedback'}->can('notify_start_backup')) {
	    my $result = $self->{'feedback'}->notify_start_backup();
	    return $steps->{'failure'}->($result) if defined $result;
	}
	if ($use_dar) {
	    if ($self->{'feedback'}->can('start_read_dar')) {
		$self->{'feedback'}->start_read_dar($xfer_dest, $steps->{'dar_data'}, $steps->{'filter_done'}, 'application dar');
	    }
	    $xfer_waiting_dar = 1;
	} else {
	    push @{$current_dump->{'range'}}, "0:-1";
	}

	$xfer_waiting_dar = 1;
	if ($self->{'feedback'}->can('start_msg')) {
	    $self->{'feedback'}->start_msg($steps->{'dar_data'});
	}
	if (defined $clerk) {
	    $clerk->init_recovery(
		xfer => $xfer,
		recovery_cb => $steps->{'recovery_cb'});
	}
	$steps->{'xfer_range'}->();
    };

    step dar_data => sub {
	my $line = shift;

	# check EOF
	if (!defined $line) {
	    return;
	}
	if ($params{'feedback'}->can('send_dar_data')) {
	    $params{'feedback'}->send_dar_data($line);
	} else {
	    my ($DAR, $range) = split ' ', $line;
	    my ($offset, $size) = split ':', $range;
	    push @{$current_dump->{'range'}}, "$offset:$size";
	    $steps->{'xfer_range'}->() if $xfer_waiting_dar;
	}
    };

    step xfer_range => sub {
	return if !$xfer_waiting_dar;
	my $range = shift @{$current_dump->{'range'}};
	if (defined $range) {
	    ($offset, my $range1) = split ':', $range;
	    if ($offset == -1 &&
		defined $xfer_src && $xfer_src->can('cancel')) {
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

	if (defined $clerk) {
	    $clerk->do_recovery(
		xfer => $xfer,
		offset => $offset,
		size   => $size);
	}

	if ($self->{'feedback'}->can('run_directtcp_application')) {
	    $self->{'feedback'}->run_directtcp_application($xfer);
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
	    if (defined $clerk) {
		$clerk->handle_xmsg($src, $msg, $xfer);
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		$steps->{'recovery_cb'}->(
			result => "DONE",
			errors => undef,
			bytes_read => undef);
	    }

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
	if (!$use_dar && $xfer_src->can('cancel')) {
	    #$xfer_src->cancel(0);
	    return;
	}
    };

    step filter_done => sub {
	$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900012,
				severity	=> $Amanda::Message::INFO,
				size            => $recovery_params{'bytes_read'}));

	if ($self->{'feedback'}->can('run_post_scripts')) {
	    $self->{'feedback'}->run_post_scripts();
	}

	if ($xfer_dest && $xfer_dest->isa("Amanda::Xfer::Dest::Buffer")) {
	    my $buf = $xfer_dest->get();
	    if ($buf) {
		my @lines = split "\n", $buf;
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900013,
				severity	=> $Amanda::Message::INFO,
				application_stdout => \@lines));
	    }
	}
	@xfer_errs = (@xfer_errs, @{$recovery_params{'errors'}})
	    if $recovery_params{'errors'};

	return $steps->{'failure'}->(
		Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900055,
			severity	=> $Amanda::Message::ERROR,
			xfer_errs	=> \@xfer_errs)) if @xfer_errs;
	return $steps->{'failure'}->(
		Amanda::Restore::Message-> new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 4900056,
			severity	=> $Amanda::Message::ERROR)) if $recovery_params{'result'} ne 'DONE';

	my $status = 0; # good
	if ($check_crc) {
	    my $msg;
	    if (defined $hdr->{'native_crc'} and $hdr->{'native_crc'} !~ /^00000000:/ and
		defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
		$hdr->{'native_crc'} ne $current_dump->{'native_crc'}) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900014,
				severity	=> $Amanda::Message::ERROR,
				header_native_crc => $hdr->{'native_crc'},
				log_native_crc	=> $current_dump->{'native_crc'}));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }
	    if (defined $hdr->{'client_crc'} and $hdr->{'client_crc'} !~ /^00000000:/ and
		defined $current_dump->{'client_crc'} and $current_dump->{'client_crc'} !~ /^00000000:/ and
		$hdr->{'client_crc'} ne $current_dump->{'client_crc'}) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900015,
				severity	=> $Amanda::Message::ERROR,
				header_client_crc => $hdr->{'client_crc'},
				log_client_crc	=> $current_dump->{'client_crc'}));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }

	    my $hdr_server_crc_size;
	    my $current_dump_server_crc_size;
	    my $current_dump_native_crc_size;
	    my $restore_native_crc_size;
	    my $source_crc_size;
	    my $dest_crc_size;

	    if (defined $hdr->{'server_crc'}) {
		$hdr->{'server_crc'} =~ /[^:]*:(.*)/;
		$hdr_server_crc_size = $1;
	    }
	    if (defined $current_dump->{'native_crc'}) {
		$current_dump->{'native_crc'} =~ /[^:]*:(.*)/;
		$current_dump_native_crc_size = $1;
	    }
	    if (defined $current_dump->{'server_crc'}) {
		$current_dump->{'server_crc'} =~ /[^:]*:(.*)/;
		$current_dump_server_crc_size = $1;
	    }
	    if (defined $restore_native_crc) {
		$restore_native_crc =~ /[^:]*:(.*)/;
		$restore_native_crc_size = $1;
	    }
	    if (defined $source_crc) {
		$source_crc =~ /[^:]*:(.*)/;
		$source_crc_size = $1;
	    }
	    if (defined $dest_crc) {
		$dest_crc =~ /[^:]*:(.*)/;
		$dest_crc_size = $1;
	    }

	    if (defined $hdr->{'server_crc'} and $hdr->{'server_crc'} !~ /^00000000:/ and
		defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
		$hdr_server_crc_size == $current_dump_server_crc_size and
		$hdr->{'server_crc'} ne $current_dump->{'server_crc'}) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900005,
				severity	=> $Amanda::Message::ERROR,
				header_server_crc => $hdr->{'server_crc'},
				log_server_crc	=> $current_dump->{'server_crc'}));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }

	    if (defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
		defined $source_crc_size and
		$current_dump_server_crc_size == $source_crc_size and
		$current_dump->{'server_crc'} ne $source_crc) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900006,
				severity	=> $Amanda::Message::ERROR,
				log_server_crc	=> $current_dump->{'server_crc'},
				source_crc	=> $source_crc));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }

	    if (defined $current_dump->{'native_crc'} and
		$current_dump->{'native_crc'} !~ /^00000000:/ and
		defined $restore_native_crc and
		$current_dump_native_crc_size == $restore_native_crc_size and
		$current_dump->{'native_crc'} ne $restore_native_crc) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900007,
				severity	=> $Amanda::Message::ERROR,
				log_native_crc	=> $current_dump->{'native_crc'},
				restore_native_crc => $restore_native_crc));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }
	    if (defined $current_dump->{'client_crc'} and $current_dump->{'client_crc'} !~ /^00000000:/ and
		defined $restore_client_crc and $current_dump->{'client_crc'} ne $restore_client_crc) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900008,
				severity	=> $Amanda::Message::ERROR,
				log_client_crc	=> $current_dump->{'client_crc'},
				restore_client_crc => $restore_client_crc));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }

	    debug("dest_is_native $dest_crc $restore_native_crc") if $dest_is_native;
	    debug("dest_is_client $dest_crc $restore_client_crc") if $dest_is_client;
	    debug("dest_is_server $dest_crc $source_crc") if $dest_is_server;
	    if ($dest_is_native && $restore_native_crc && $restore_native_crc ne $dest_crc) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900009,
				severity	=> $Amanda::Message::ERROR,
				dest_crc	=> $dest_crc,
				restore_native_crc => $restore_native_crc));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }
	    if ($dest_is_client && $restore_client_crc && $restore_client_crc ne $dest_crc) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900010,
				severity	=> $Amanda::Message::ERROR,
				dest_crc	=> $dest_crc,
				restore_client_crc => $restore_client_crc));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }
	    if ($dest_is_server and
		defined $source_crc and defined $dest_crc and
		$source_crc ne $dest_crc) {
		$self->user_message(
			Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900011,
				severity	=> $Amanda::Message::ERROR,
				dest_crc	=> $dest_crc,
				source_crc	=> $source_crc));
		$self->{'image_status'} = 1;
		$self->{'exit_status'} = 1;
	    }
	}

	if ($self->{'image_status'}) {
	    $self->{'image_failed'}++;
	    $self->{'exit_status'} = 1;
	} else {
	    $self->{'image_restored'}++;
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
	if (ref $msg ne "HASH" || !$msg->isa('Amanda::Message')) {
	    $msg = Amanda::Restore::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4900068,
				severity	=> $Amanda::Message::ERROR,
				msg		=> $msg);
	}
	$self->user_message($msg);
	$self->{'exit_status'} = 1;
	$steps->{'quit2'}->();
    };
}
