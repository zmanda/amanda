# Copyright (c) 2009-2015 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::FetchDump::ClientApplication;

use base 'Amanda::FetchDump';

use IPC::Open2;

use Amanda::Config qw( :getconf );
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Util qw( :quoting );
use Amanda::Amservice;
use Amanda::Debug qw( debug );
use Amanda::Disklist;

sub set
{
    my $self = shift;
    my $hdr = shift;
    my $hdr_dle = shift;
    my $application_property = shift;
    my @args;

    $self->{'hdr'} = $hdr;
    $self->{'hdr_dle'} = $hdr_dle;
    $self->{'application_property'} = $application_property;
    my $extract_host = $self->{'extract-client'};

    my $auth = "NULL"; # default amservice auth (amanda-client.conf)
    # Do not use the auth from the header, it might have changed,
    # and we don't have all the parameters
    #   my $auth = $dle->{'auth'} || "bsdtcp";

    # use the auth and parameter from the disklist if the host is listed there
    my $host = Amanda::Disklist::get_host($extract_host);
    if ($host) {
	$auth = $host->{'auth'} if defined $host->{'auth'};
	    push @args, "-oamandad-path=$host->{'amandad_path'}" if defined $host->{'amandad_path'} && $host->{'amandad_path'} ne '';
	    push @args, "-oclient-username=$host->{'client_username'}" if defined $host->{'client_username'} && $host->{'client_username'} ne '';
	    push @args, "-oclient-port=$host->{'client_port'}" if defined $host->{'client_port'} && $host->{'client_port'} ne '';
	if ($auth eq 'ssh') {
	    push @args, "-ossh-keys=$host->{'ssh_keys'}" if defined $host->{'ssh_keys'} && $host->{'ssh_keys'} ne '';
	}
	if ($auth eq 'ssl') {
	    push @args, "-ossl-fingerprint-file=$host->{'ssl_fingerprint_file'}" if defined $host->{'ssl_fingerprint_file'} && $host->{'ssl_fingerprint_file'} ne '';
	    push @args, "-ossl-cert-file=$host->{'ssl_cert_file'}" if defined $host->{'ssl_cert_file'} && $host->{'ssl_cert_file'} ne '';
	    push @args, "-ossl-key-file=$host->{'ssl_key_file'}" if defined $host->{'ssl_key_file'} && $host->{'ssl_key_file'} ne '';
	    push @args, "-ossl-ca-cert-file=$host->{'ssl_ca_cert_file'}" if defined $host->{'ssl_ca_cert_file'} && $host->{'ssl_ca_cert_file'} ne '';
	    push @args, "-ossl-cipher-list=$host->{'ssl_cipher_list'}" if defined $host->{'ssl_cipher_list'} && $host->{'ssl_cipher_list'} ne '';
	    push @args, "-ossl-check-certificate-host=$host->{'ssl_check_certificate_host'}" if defined $host->{'ssl_check_certificate_host'};
	    push @args, "-ossl-check-host=$host->{'ssl_check_host'}" if defined $host->{'ssl_check_host'};
	    push @args, "-ossl-check-fingerprint=$host->{'ssl_check_fingerprint'}" if defined $host->{'ssl_check_fingerprint'};
	}
    }

    my $req = $self->{'hdr'}->{'dle_str'};
    $self->{'service'} = Amanda::Amservice->new();
    my $rep = $self->{'service'}->run($req, $extract_host, $auth, "restore", \@args, "CTL", "MESG", "DATA", "STATE");

    if ($rep !~ /^CONNECT/) {
	return Amanda::FetchDump::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 3300063,
		severity        => $Amanda::Message::ERROR,
		amservice_error => $rep);
    }

    $self->{'service'}->close('MESG','w');
    $self->{'service'}->close('DATA','r');
    $self->{'service'}->close('STATE','r');

    my $line = $self->getline('CTL');
    if ($line !~ /^FEATURES (.*)\r\n/) {
	chomp $line;
	chop $line;
	return Amanda::FetchDump::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 3300064,
		severity        => $Amanda::Message::ERROR,
		expect		=> "FEATURES",
		line		=> $line);
    }
    $self->{'their_features'} = Amanda::Feature::Set->from_string($1);
    my $qdirectory = quote_string_always($self->{'directory'});
    $self->sendctlline("DIRECTORY $qdirectory\r\n");
    return undef;
}

sub get_xfer_dest {
    my $self = shift;

    $self->{'xfer_dest'} = Amanda::Xfer::Dest::Fd->new($self->{'service'}->wfd_fileno('DATA'));
    $self->{'service'}->close('DATA','w');
    return $self->{'xfer_dest'};
}

sub send_header {
    my $self = shift;
    my $hdr = shift;

    my $data;
    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_header_send_size)) {
	$data = $hdr->to_string(128, 32768);
	$self->sendctlline("HEADER-SEND-SIZE " . length($data) . "\r\n");
    } else {
	$data = $hdr->to_string(32768, 32768);
    }
    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_header_ready)) {
	my $line = $self->getline('CTL');
	if ($line ne "HEADER-READY\r\n") {
	    chomp $line;
	    chop $line;
	    return Amanda::FetchDump::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 3300064,
		severity        => $Amanda::Message::ERROR,
		expect		=> "HEADER-READY",
		line		=> $line);
	}
    }

    $self->senddata('DATA', $data);
    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_header_done)) {
	my $line = $self->getline('CTL');
	if ($line !~ /^HEADER-DONE/) {
	    chomp $line;
	    chop $line;
	    return Amanda::FetchDump::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 3300064,
		severity        => $Amanda::Message::ERROR,
		expect		=> "HEADER-DONE",
		line		=> $line);
	}
    }
    return undef;
}

sub transmit_state_file {
    my $self = shift;
    my $header = shift;

    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_state_stream)) {
	my $host = Amanda::Util::sanitise_filename("" . $header->{'name'});
	my $disk = Amanda::Util::sanitise_filename("" . $header->{'disk'});
	my $state_filename = getconf($CNF_INDEXDIR) . '/' . $host .
		'/' . $disk . '/' . $header->{'datestamp'} . '_' .
		$header->{'dumplevel'} . '.state';
	my $state_filename_gz = $state_filename . $Amanda::Constants::COMPRESS_SUFFIX;
	if (-e $state_filename || -e $state_filename_gz) {
	    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_state_send)) {
		$self->sendctlline("STATE-SEND\r\n");
	    }
	    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_state_ready)) {
		my $line = $self->getline('CTL');
		if ($line !~ /^STATE-READY/) {
		    chomp $line;
		    chop $line;
		    return Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300064,
			severity        => $Amanda::Message::ERROR,
			expect		=> "STATE-READY",
			line		=> $line);
		}
	    }
	    my $pid;
	    if (-e $state_filename_gz) {
		$pid = open2(\*STATEFILE, undef,
			     $Amanda::Constants::UNCOMPRESS_PATH,
			     $Amanda::Constants::UNCOMPRESS_OPT,
			     $state_filename_gz);
	    } elsif (-e $state_filename) {
		open STATEFILE, '<', $state_filename;
	    }
	    my $block;
	    my $length;
	    while ($length = sysread(STATEFILE, $block, 32768)) {
		Amanda::Util::full_write($self->{'service'}->wfd_fileno('STATE'),
					 $block, $length)
		    or die "writing to " . $self->{'service'}->wfd_fileno('STATE') . " : $!";
	    }
	    $self->{'service'}->close('STATE', 'w');
	    if ($pid) {
		waitpid($pid, 0);
	    }
	    close(STATEFILE);
	    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_state_done)) {
		my $line = $self->getline('CTL');
		if ($line !~ /^STATE-DONE/) {
		    chomp $line;
		    chop $line;
		    return Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300064,
			severity        => $Amanda::Message::ERROR,
			expect		=> "STATE-DONE",
			line		=> $line);
		}
	    }
	} else {
	    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_state_send)) {
		$self->sendctlline("NO-STATE-SEND\r\n");
	    }
	    $self->{'service'}->close('STATE', 'w');
	}
    }
    return undef;
}

sub get_mesg_fd {
    my $self = shift;

    return $self->{'service'}->rfd_fileno('MESG');
}

sub get_mesg_json {
    my $self = shift;

    return $self->{'their_features'}->has($Amanda::Feature::fe_restore_mesg_json);
}

sub notify_start_backup {
    my $self = shift;

    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_data_send)) {
        $self->sendctlline("DATA-SEND\r\n");
    }

    if ($self->{'their_features'}->has($Amanda::Feature::fe_restore_data_ready)) {
        my $line = $self->getline('CTL');
        if ($line ne "DATA-READY\r\n") {
	    chomp $line;
	    chop $line;
	    return Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300064,
			severity        => $Amanda::Message::ERROR,
			expect		=> "DATA-READY",
			line		=> $line);
        }
    }
    return undef;
}

sub start_read_dar
{
    my $self = shift;
    my $xfer_dest = shift;
    my $cb_data = shift;
    my $cb_done = shift;
    my $text = shift;

    my $fd = $xfer_dest->get_dar_fd();
    $fd.="";
    $fd = int($fd);
    my $src = Amanda::MainLoop::fd_source($fd,
                                          $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    my $buffer = "";
    $self->{'fetchdump'}->{'all_filter'}->{$src} = 1;
    $src->set_callback( sub {
	my $b;
	my $n_read = POSIX::read($fd, $b, 1);
	if (!defined $n_read) {
	    return;
	} elsif ($n_read == 0) {
	    delete $self->{'fetchdump'}->{'all_filter'}->{$src};
	    $cb_data->("DAR -1:0");
	    $src->remove();
	    POSIX::close($fd);
	    if (!%{$self->{'fetchdump'}->{'all_filter'}} and $self->{'recovery_done'}) {
		$cb_done->();
	    }
	} else {
	    $buffer .= $b;
	    if ($b eq "\n") {
		my $line = $buffer;
		chomp $line;
		if (length($line) > 1) {
		    $cb_data->($line);
		}
	    $buffer = "";
	    }
	}
    });
    return undef;
}

sub transmit_dar {
    my $self = shift;
    my $use_dar = shift;

    return 0 if !$self->{'their_features'}->has($Amanda::Feature::fe_restore_dar);

    if ($use_dar) {
        $self->sendctlline("USE-DAR YES\r\n");
    } else {
        $self->sendctlline("USE-DAR NO\r\n");
    }

    my $line = $self->getline('CTL');
    my $darspec = ($line =~ /^USE-DAR (.*)\r\n$/);
    if ($1 ne "YES" && $1 ne "NO") {
	chomp $line;
	chop $line;
	return Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300064,
			severity        => $Amanda::Message::ERROR,
			expect		=> "USE-DAR [YES|NO]",
			line		=> $line);
    }
    $use_dar = ($1 eq 'YES');

    return $use_dar;
}

sub get_datapath
{
    my $self = shift;

    $self->{'datapath'} = 'amanda';
    return if !$self->{'their_features'}->has($Amanda::Feature::fe_restore_datapath);
    $self->sendctlline("AVAIL-DATAPATH AMANDA\r\n");
    return undef;
}

sub send_amanda_datapath
{
    my $self = shift;

    return if !$self->{'their_features'}->has($Amanda::Feature::fe_restore_datapath);

    my $line = $self->getline('CTL');
    if ($line =~ /^USE-DATAPATH AMANDA/) {
	$self->sendctlline("DATAPATH-OK\r\n");
    } else {
	chomp $line;
	chop $line;
	return Amanda::FetchDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3300064,
			severity        => $Amanda::Message::ERROR,
			expect		=> "USE-DATAPATH",
			line		=> $line);
    }
    return undef;
}

sub send_directtcp_datapath
{
    my $self = shift;

    return if !$self->{'their_features'}->has($Amanda::Feature::fe_restore_datapath);

    if ($self->{'datapath'} eq 'directtcp') {
	die("unsuported datapath directtcp");
    }
    return undef;
}

# helper function to get a line, including the trailing '\n', from a stream.  This
# reads a character at a time to ensure that no extra characters are consumed.  This
# could certainly be more efficient! (TODO)
sub getline {
    my $self = shift;
    my ($stream) = @_;
    my $fd = $self->{'service'}->rfd_fileno($stream);
    my $line = '';

    while (1) {
	my $c;
	my $s = POSIX::read($fd, $c, 1);
	last if $s == 0;	# EOF
	last if !defined $s;	# Error
	$line .= $c;
	last if $c eq "\n";
    }

    $line =~ /^(.*)$/;
    my $chopped = $1;
    $chopped =~ s/[\r\n]*$//g;
    debug("CTL << $chopped");

    return $line;
}

# helper function to write a data to a stream.  This does not add newline characters.
# If the callback is given, this is async (TODO: all calls should be async)
sub senddata {
    my $self = shift;
    my ($stream, $data, $async_write_cb) = @_;
    my $fd = $self->{'service'}->wfd_fileno($stream);

    if (defined $async_write_cb) {
	return Amanda::MainLoop::async_write(
		fd => $fd,
		data => $data,
		async_write_cb => $async_write_cb);
    } else {
	Amanda::Util::full_write($fd, $data, length($data))
	    or die "writing to $stream ($fd): $!";
    }
}

# send a line on the control stream, or just log it if the ctl stream is gone;
# async callback is just like for senddata
sub sendctlline {
    my $self = shift;
    my ($msg, $async_write_cb) = @_;

    my $chopped = $msg;
    $chopped =~ s/[\r\n]*$//g;

    if (defined $self->{'service'}->wfd('CTL')) {
	debug("CTL >> $chopped");
	return $self->senddata('CTL', $msg, $async_write_cb);
    } else {
	debug("not sending CTL message as CTL is closed >> $chopped");
	if (defined $async_write_cb) {
	    $async_write_cb->(undef, length($msg));
	}
    }
}


1;
