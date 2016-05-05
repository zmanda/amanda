# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::CheckDump::Message;
use strict;
use warnings;

use Amanda::Message;
use Amanda::Debug;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2700003) {
	return "Reading volume $self->{'label'} file $self->{'filenum'}";
    } elsif ($self->{'code'} == 2700004) {
	return "Reading '$self->{'filename'}'";
    } elsif ($self->{'code'} == 2700005) {
	return "Validating image " . $self->{hostname} . ":" .
	    $self->{diskname} . " dumped " . $self->{dump_timestamp} . " level ".
	    $self->{level} . ($self->{'nparts'} > 1 ? " ($self->{nparts} parts)" : "");
    } elsif ($self->{'code'} == 2700006) {
	return "All images successfully validated";
    } elsif ($self->{'code'} == 2700007) {
	return "Some images failed to be correctly validated.";

    } else {
	return "No message for code '$self->{'code'}'";
    }
}

package Amanda::CheckDump;


use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants :quoting );
use Amanda::Cmdline;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Recovery::Clerk;
use Amanda::Restore;
use Amanda::Extract;

use base 'Amanda::Recovery::Clerk::Feedback';

sub new {
    my $class = shift;

    my $self = $class->SUPER::new(@_);
    $self->{'is_tty'} = -t STDOUT;

    return $self;
    # must return undef on error
    # must call user_message to print error
}

sub run {
    my $self = shift;
    my %params = @_;

    $self->{'directory'} = $params{'directory'};
    $self->{'extract-client'} = $params{'extract-client'};
    $self->{'assume'} = $params{'assume'};

    ($self->{'restore'}, my $result_message) = Amanda::Restore->new();
    if (@$result_message) {
	foreach my $message (@$result_message) {
	    $self->user_message($message);
	}
	return $params{'finished_cb'}->(1);
    }

    select(STDERR);
    $| = 1;
    select(STDOUT); # default
    $| = 1;

    my $timestamp = $params{'timestamp'};
    $timestamp = Amanda::DB::Catalog::get_latest_write_timestamp()
			unless defined $timestamp;
    my @spec = Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, undef, $timestamp);
    $self->{'restore'}->restore(
		#'application_property'  => $params{'application_property'},
		'assume'                => $params{'assume'},
		'decompress'            => 1,
		'decrypt'               => 1,
		'device'                => $params{'device'},
		'directory'             => undef,
		'dumpspecs'             => \@spec,
		'exact-match'           => $params{'exact-match'},
		'extract'               => 1,
		#'init'                  => $params{'init'},
		#'restore'               => $params{'restore'},
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

sub set {
    my $self = shift;
    my $hdr = shift;;
    my $dle = shift;;
    my $application_property = shift;

    $self->{'hdr'} = $hdr;
    $self->{'dle'} = $dle;
    $self->{'application_property'} = $application_property;

    $self->{'extract'} = Amanda::Extract->new(hdr => $hdr, dle => $dle);
    die("$self->{'extract'}") if $self->{'extract'}->isa('Amanda::Message');
    ($self->{'bsu'}, my $err) = $self->{'extract'}->BSU();
    if ($err && @$err) {
        die("BSU err " . join("\n", @$err));
    }

    return undef;
}

sub transmit_dar {
    my $self = shift;
    my $use_dar = shift;

    return 0;
}

sub get_datapath {
    my $self = shift;
    my $directtcp_supported = shift;

    $self->{'use_directtcp'} = $directtcp_supported && !$self->{'bsu'}->{'data-path-directtcp'};
    return $self->{'use_directtcp'};
}

sub get_xfer_dest {
    my $self = shift;

    $self->{'extract'}->set_validate_argv();

    if ($self->{'extract'}->{'validate_argv'}) {
	$self->{'xfer_dest'} = Amanda::Xfer::Dest::Application->new($self->{'extract'}->{'validate_argv'}, 0, 0, 0, 1);
    } else {
	$self->{'xfer_dest'} = Amanda::Xfer::Dest::Null->new(0);
    }
    return $self->{'xfer_dest'};
}

sub new_dest_fh {
    my $self = shift;

    my $new_dest_fh;
    open ($new_dest_fh, '>/dev/null');
    return $new_dest_fh;
}

sub clerk_notif_part {
    my $self = shift;
    my ($label, $filenum, $header) = @_;

    $self->user_message(Amanda::CheckDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 2700003,
		severity	=> $Amanda::Message::INFO,
		label		=> $label,
		filenum		=> "$filenum"+0));
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    $self->user_message(Amanda::CheckDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 2700004,
		severity	=> $Amanda::Message::INFO,
		holding_file	=> $filename));
}

sub notif_start {
    my $self = shift;
    my $dump = shift;
    $self->user_message(Amanda::CheckDump::Message->new(
	source_filename => __FILE__,
	source_line     => __LINE__,
	code            => 2700005,
	severity        => $Amanda::Message::INFO,
	hostname        => $dump->{hostname},
	diskname        => $dump->{diskname},
	dump_timestamp  => $dump->{dump_timestamp},
	level           => "$dump->{level}"+0,
	nparts          => $dump->{nparts}));

}

sub user_message {
    my $self = shift;
    my $message = shift;

    if ($message->{'code'} == 4900000) { #SIZE
	if ($self->{'is_tty'}) {
	    print STDOUT "\r$message    ";
	    $self->{'last_is_size'} = 1;
	} else {
	    print STDOUT "SIZE: $message\n";
	}
    } elsif ($message->{'code'} == 4900012) { #READ SIZE
	if ($self->{'is_tty'}) {
	    print STDOUT "\r$message    ";
	    $self->{'last_is_size'} = 1;
	} else {
	    print STDOUT "READ SIZE: $message\n";
	}
    } elsif ($message->{'code'} == 4900018 && $message->{'text'} eq 'application stdout') {
	# do nothing with application stdout
    } else {
	if ($message->{'code'} == 3300003 || $message->{'code'} == 3300004) {
	    print "\n";
	}
	print STDOUT "\n" if $self->{'is_tty'} and $self->{'last_is_size'};
	print STDOUT "$message\n";
	$self->{'last_is_size'} = 0;

	if ($message->{'code'} == 3300002 && $self->{'assume'}) {
	    print STDOUT "Press enter when ready\n";
	    my $resp = <STDIN>;
	}
    }
}

1;
