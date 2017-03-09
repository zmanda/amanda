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
	if ($self->{'image_validated'} == 1) {
	    return "All image ($self->{'image_validated'}) successfully validated";
	} else {
	    return "All images ($self->{'image_validated'}) successfully validated";
	}
    } elsif ($self->{'code'} == 2700007) {
	if ($self->{'image_failed'} == 1) {
	    return "$self->{'image_failed'} image failed to be correctly validated.";
	} else {
	    return "$self->{'image_failed'} images failed to be correctly validated.";
	}
    } elsif ($self->{'code'} == 2700008) {
	if ($self->{'image_validated'} == 1) {
	    return "$self->{'image_validated'} image successfully validated.";
	} else {
	    return "$self->{'image_validated'} images successfully validated.";
	}
    } elsif ($self->{'code'} == 2700009) {
	my $count = $self->{'nb_image'} - $self->{'image_validated'} - $self->{'image_failed'};
	if ($count == 1) {
	    return "$count image not validated.";
	} else {
	    return "$count images not validated.";
	}
    } elsif ($self->{'code'} == 2700010) {
	return "No images validated.";
    } elsif ($self->{'code'} == 2700018) {
	return "Running a CheckDump";
    } elsif ($self->{'code'} == 2700019) {
	return "Failed to fork the CheckDump process";
    } elsif ($self->{'code'} == 2700020) {
	return "The message filename is '$self->{'message_filename'}'";
    } else {
	return "No message for code '$self->{'code'}'";
    }
}

package Amanda::CheckDump;

use POSIX qw(strftime);
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
    $self->{'image_validated'} = 0;
    $self->{'image_failed'} = 0;
    $self->{'nb_image'} = 0;

    my $logdir = $self->{'logdir'} = config_dir_relative(getconf($CNF_LOGDIR));
    my @now = localtime;
    my $timestamp = strftime "%Y%m%d%H%M%S", @now;
    $self->{'pid'} = $$; 
    $self->{'timestamp'} = Amanda::Logfile::make_logname("checkdump", $timestamp);
    $self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
    debug("beginning trace log: $self->{'trace_log_filename'}");
    $self->{'message_filename'} = "checkdump.$timestamp";
    $self->{'message_pathname'} = "$logdir/checkdump.$timestamp";

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

    ($self->{'restore'}, my $result_message) = Amanda::Restore->new(
			message_pathname => $self->{'message_pathname'});
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
    my $validate_finish_cb = sub {

	$self->{'nb_image'} = $self->{'restore'}->{'nb_image'};
	$self->{'image_validated'} = $self->{'restore'}->{'image_restored'};
	$self->{'image_failed'} = $self->{'restore'}->{'image_failed'};

	if (!defined $self->{'nb_image'} || $self->{'nb_image'} == 0) {
	    $self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 2700010,
			nb_image        => $self->{'nb_image'},
			image_validated => $self->{'image_validate'},
			image_failed    => $self->{'image_failed'},
			severity        => $Amanda::Message::ERROR));
	} elsif ($self->{'image_failed'} == 0 &&
		   $self->{'nb_image'} == $self->{'image_validated'}) {
	    $self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 2700006,
			image_validated => $self->{'image_validated'},
			severity        => $Amanda::Message::SUCCESS));
	} else {
	    if ($self->{'image_validated'}) {
	        $self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 2700008,
			image_validated => $self->{'image_validated'},
			severity        => $Amanda::Message::SUCCESS));
	    }
	    if ($self->{'image_failed'}) {
		$self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 2700007,
			image_failed    => $self->{'image_failed'},
			severity        => $Amanda::Message::ERROR));
	    }
	    if ($self->{'image_validated'} + $self->{'image_failed'} != $self->{'nb_image'}) {
		$self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 2700009,
			nb_image        => $self->{'nb_image'},
			image_validated => $self->{'image_validate'},
			image_failed    => $self->{'image_failed'},
			severity        => $Amanda::Message::ERROR));
	    }
	}

	$params{'finished_cb'}->(@_);
    };

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
		'all_copy'              => 1,
		#'init'                  => $params{'init'},
		#'restore'               => $params{'restore'},
		'finished_cb'           => $validate_finish_cb,
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

    $self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
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
    $self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 2700004,
		severity	=> $Amanda::Message::INFO,
		holding_file	=> $filename));
}

sub notif_start {
    my $self = shift;
    my $dump = shift;
    $self->{'restore'}->user_message(Amanda::CheckDump::Message->new(
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
