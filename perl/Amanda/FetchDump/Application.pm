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

package Amanda::FetchDump::Application;

use base 'Amanda::FetchDump';

use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Xfer qw( :constants );
use IPC::Open2;

sub DESTROY {
    my $self = shift;

    if ($self->{'uncompressed_state_filename'}) {
#	unlink $self->{'state_filename'};
    }
}

sub set {
    my $self = shift;
    my $hdr = shift;
    my $dle = shift;
    my $application_property = shift;

    $self->{'hdr'} = $hdr;
    $self->{'dle'} = $dle;
    $self->{'application_property'} = $application_property;

    $self->{'extract'} = Amanda::Extract->new(
		hdr			=> $hdr,
		dle			=> $dle,
		'include-file'		=> $self->{'include-file'},
		'include-list'		=> $self->{'include-list'},
		'include-list-glob'	=> $self->{'include-list-glob'},
		'exclude-file'		=> $self->{'exclude-file'},
		'exclude-list'		=> $self->{'exclude-list'},
		'exclude-list-glob'	=> $self->{'exclude-list-glob'});
    die("$self->{'extract'}") if $self->{'extract'}->isa('Amanda::Message');
    ($self->{'bsu'}, my $err) = $self->{'extract'}->BSU();
    if (@$err) {
	die("BSU err " . join("\n", @$err));
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
    my $dar_end = 0;

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
	    $cb_data->("DAR -1:0") if $dar_end == 0;;
	    $dar_end = 1;
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
		    $dar_end = 1 if $line eq "DAR -1:0";
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

    $self->{'use_dar'} = $use_dar && $self->{'bsu'}->{'dar'};
    return $self->{'use_dar'};
}

sub get_datapath {
    my $self = shift;
    my $directtcp_supported = shift;

    $self->{'use_directtcp'} = $directtcp_supported && !$self->{'bsu'}->{'data-path-directtcp'};
    return $self->{'use_directtcp'};
}

sub get_xfer_dest {
    my $self = shift;

    $self->{'extract'}->set_restore_argv(
		directory => $self->{'directory'},
		use_dar   => $self->{'use_dar'},
		state_filename => $self->{'state_filename'},
		application_property => $self->{'application_property'});

    if ($self->{'use_directtcp'}) {
	$self->{'xfer_dest'} = Amanda::Xfer::Dest::DirectTCPListen->new();
    } else {
	$self->{'xfer_dest'} = Amanda::Xfer::Dest::Application->new($self->{'extract'}->{'restore_argv'}, 0, 0, 0, 1);
    }

    return $self->{'xfer_dest'};
}

sub transmit_state_file {
    my $self = shift;
    my $header = shift;

    my $state_filename = Amanda::Logfile::getstatefname(
		"".$header->{'name'}, "".$header->{'disk'},
		$header->{'datestamp'}, $header->{'dumplevel'});
    my $state_filename_gz = $state_filename . $Amanda::Constants::COMPRESS_SUFFIX;
    if (-e $state_filename) {
	$self->{'state_filename'} = $state_filename;
    } elsif (-e $state_filename_gz) {
	my $pid;
	open STATEFILE, '>', $state_filename;
	$pid = open2(">&STATEFILE", undef,
			$Amanda::Constants::UNCOMPRESS_PATH,
			$Amanda::Constants::UNCOMPRESS_OPT,
			$state_filename_gz);
	close STATEFILE;
	waitpid($pid, 0);
	$self->{'state_filename'} = $state_filename;
	$self->{'uncompressed_state_filename'} = 1;
    }
    return undef;
}

sub new_dest_fh {
    my $self = shift;

    if (!$self->{'use_directtcp'}) {
	my $new_dest_fh = \*STDOUT;
	return $new_dest_fh;
    }
    return;
}

sub run_directtcp_application {
    my $self = shift;
    my $xfer = shift;

    return if !$self->{'use_directtcp'};

    my $addr = $self->{'xfer_dest'}->get_addrs();
    my @directtcp_command = $self->{'extract'}->{'restore_argv'};
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
	$self->{'last_is_size'} = 0;
    });

}

1;
