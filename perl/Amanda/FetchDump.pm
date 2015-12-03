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

package Amanda::FetchDump::Message;
use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 3300003) {
	return "Reading label '$self->{'label'}' filenum $self->{'filenum'}\n$self->{'header_summary'}";
    } elsif ($self->{'code'} == 3300004) {
	return "Reading '$self->{'holding_file'}'\n$self->{'header_summary'}";
    } elsif ($self->{'code'} == 3300057) {
	return "Running a Fetchdump";
    } elsif ($self->{'code'} == 3300058) {
	return "Failed to fork the FetchDump process";
    } elsif ($self->{'code'} == 3300059) {
	return "The message filename is '$self->{'message_filename'}'";
    } elsif ($self->{'code'} == 3300062) {
	return "Exit status: $self->{'exit_status'}";
    } elsif ($self->{'code'} == 3300063) {
	return "amservice error message: $self->{'amservice_error'}";
    } elsif ($self->{'code'} == 3300064) {
	return "expecting $self->{'expect'} line, got: $self->{'line'}";
    } else {
	return "No mesage for code '$self->{'code'}'";
    }
}

package Amanda::FetchDump;

use Amanda::Recovery::Clerk;
use base 'Amanda::Recovery::Clerk::Feedback';

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants :quoting );
use Amanda::Constants;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Cmdline;
use Amanda::Restore;
use Amanda::Extract;

sub new {
    my $class = shift;

    my $self = $class->SUPER::new(@_);
    $self->{'is_tty'} = -t STDERR;

    return $self;
    # must return undef on error
    # must call user_message to print error
}

sub run {
    my $self = shift;
    my %params = @_;

    $self->{'directory'} = $params{'directory'};
    $self->{'extract-client'} = $params{'extract-client'};
    $self->{'include-file'} = $params{'include-file'};
    $self->{'include-list'} = $params{'include-list'};
    $self->{'include-list-glob'} = $params{'include-list-glob'};
    $self->{'exclude-file'} = $params{'exclude-file'};
    $self->{'exclude-list'} = $params{'exclude-list'};
    $self->{'exclude-list-glob'} = $params{'exclude-list-glob'};
    $self->{'prev-level'} = $params{'prev-level'};
    $self->{'next-level'} = $params{'next-level'};

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
		'chdir'                 => $params{'chdir'},
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
		'feedback'              => $self);
}

sub set_feedback {
    my $self = shift;
    my %params = @_;

    $self->{'chg'} = $params{'chg'} if exists $params{'chg'};
    $self->{'dev_name'} = $params{'dev_name'} if exists $params{'dev_name'};

    return $self;
}

sub clerk_notif_part {
    my $self = shift;
    my ($label, $filenum, $header) = @_;

    $self->user_message(Amanda::FetchDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 3300003,
		severity	=> $Amanda::Message::INFO,
		label		=> $label,
		filenum		=> $filenum,
		header_summary	=> $header->summary()));
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    $self->user_message(Amanda::FetchDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 3300004,
		severity	=> $Amanda::Message::INFO,
		holding_file	=> $filename,
		header_summary	=> $header->summary()));
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
}

sub user_message {
    my $self = shift;
    my $message = shift;

    if ($message->{'code'} == 4900000) { #SIZE
	if ($self->{'is_tty'}) {
	    print STDERR "\r$message    ";
	    $self->{'last_is_size'} = 1;
	} else {
	    print STDERR "READ SIZE: $message\n";
	}
    } elsif ($message->{'code'} == 4900012) { #READ SIZE
	print STDERR "\r$message    \n";
    } else {
	if ($message->{'code'} == 3300003 || $message->{'code'} == 3300004) {
	    print "\n";
	}
	print STDERR "\n" if $self->{'is_tty'} and $self->{'last_is_size'};
	print STDERR "$message\n";
	$self->{'last_is_size'} = 0;

	if ($message->{'code'} == 4900002 && !$self->{'assume'}) {
	    print STDERR "Press enter when ready\n";
	    my $resp = <STDIN>;
	}
    }
}

1;
