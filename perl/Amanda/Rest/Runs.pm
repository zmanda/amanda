# Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Rest::Runs;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Amdump;
use Amanda::Amflush;
use Amanda::Vault;
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Amdump -- Rest interface to Amanda::Amdump

=head1 INTERFACE

=over

=item Run amdump

request:
  POST localhost:5000/amanda/v1.0/configs/:CONFIG/runs/amdump
    query arguments:
        host=HOST
        disk=DISK               #repeatable
        hostdisk=HOST|DISK      #repeatable
        no_taper=0|1
        from_client=0|1

reply:
  HTTP status: 202 Accepted
  [
     {
        "amdump_log" : "/var/log/amanda/test/amdump.20140205112550",
        "code" : "2000001",
        "message" : "The amdump log file is '/var/log/amanda/test/amdump.20140205112550'",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Amdump.pm",
        "source_line" : "91"
     },
     {
        "code" : "2000000",
        "message" : "The trace log file is '/amanda/h1/var/amanda/test/log.20140205112550.0'",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Amdump.pm",
        "source_line" : "97",
        "trace_log" : "/var/log/amanda/test/log.20140205112550.0"
     },
     {
        "code" : "2000002",
        "message" : "Running a dump",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Runs.pm",
        "source_line" : "105"
     }
  ]

=item Run amflush

request:
  POST localhost:5000/amanda/v1.0/configs/:CONFIG/runs/amflush
    query arguments:
        host=HOST
        disk=DISK               #repeatable
        hostdisk=HOST|DISK      #repeatable
	datestamps=DATESTAMP    #repeatable

reply:
  HTTP status: 202 Accepted
  [
     {
        "amdump_log" : "/var/log/amanda/test/amdump.20140205120327",
        "code" : "2200001",
        "message" : "The amdump log file is '/var/log/amanda/test/amdump.20140205120327'",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Amflush.pm",
        "source_line" : "98"
     },
     {
        "code" : "2200000",
        "message" : "The trace log file is '/var/log/amanda/test/log.20140205120327.0'",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Amflush.pm",
        "source_line" : "104",
        "trace_log" : "/var/log/amanda/test/log.20140205120327.0"
     },
     {
        "code" : "2200005",
        "message" : "Running a flush",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Runs.pm",
        "source_line" : "315"
     }
  ]

=item Run amflush

request:
  POST localhost:5000/amanda/v1.0/configs/:CONFIG/runs/amvault
    query arguments:
        host=HOST
        disk=DISK               #repeatable
        hostdisk=HOST|DISK      #repeatable
quiet=0|1
fulls_only=0|1
latest_fulls=0|1
incrs_only=0|1
opt_export=0|1
opt_dry_run=0|1
src_write_timestamp=TIMESTAMP
dst_write_timestamp=TIMESTAMP


reply:
  HTTP status: 202 Accepted


=back

=cut

sub amdump {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    Amanda::Util::set_pname("amdump");
    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    if (defined($params{'host'})) {
	my @hostdisk;
	if (defined($params{'disk'})) {
	    if (ref($params{'disk'}) eq 'ARRAY') {
		foreach my $disk (@{$params{'disk'}}) {
		    push @hostdisk, $params{'host'}, $disk;
		}
	    } else {
		push @hostdisk, $params{'host'}, ${'disk'};
	    }
	} else {
	    push @hostdisk, $params{'host'};
	}
	$params{'hostdisk'} = \@hostdisk;
    }
    $params{'config'} = $params{'CONF'};
    $params{'exact_match'} = 1;
    my ($amdump, $messages) = Amanda::Amdump->new(%params, user_msg => $user_msg);
    push @result_messages, @{$messages};

    # fork the amdump process and detach
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $exit_code = $amdump->run(0);
	Amanda::Debug::debug("exiting with code $exit_code");
	exit($exit_code);
    }

    push @result_messages, Amanda::Amdump::Message->new(
	source_filename => __FILE__,
	source_line     => __LINE__,
	code         => 2000002,
	severity     => $Amanda::Message::INFO);
    Dancer::status(202);

    return \@result_messages;
}

sub amvault {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    Amanda::Util::set_pname("amvault");
    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    if (defined($params{'host'})) {
	my @hostdisk;
	if (defined($params{'disk'})) {
	    if (ref($params{'disk'}) eq 'ARRAY') {
		foreach my $disk (@{$params{'disk'}}) {
		    push @hostdisk, $params{'host'}, $disk;
		}
	    } else {
		push @hostdisk, $params{'host'}, ${'disk'};
	    }
	} else {
	    push @hostdisk, $params{'host'};
	}
	$params{'hostdisk'} = \@hostdisk;
    }
    $params{'config'} = $params{'CONF'};
    $params{'exact_match'} = $params{'CONF'};
    my ($vault, $messages) = Amanda::Vault->new(%params, user_msg => $user_msg);
    push @result_messages, @{$messages};

    # fork the vault process and detach
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $exit_code = $vault->run(0);
	Amanda::Debug::debug("exiting with code $exit_code");
	exit($exit_code);
    }

    push @result_messages, Amanda::Vault::Message->new(
	source_filename => __FILE__,
	source_line     => __LINE__,
	code         => 2400003,
	severity     => $Amanda::Message::INFO);
    Dancer::status(202);

    return \@result_messages;
}

sub amflush {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
    Amanda::Disklist::unload_disklist();
    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	return Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400006,
			diskfile     => $diskfile,
			cfgerr_level => $cfgerr_level);
    }

    Amanda::Util::set_pname("amflush");
    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    if (defined($params{'host'})) {
	my @hostdisk;
	if (defined($params{'disk'})) {
	    if (ref($params{'disk'}) eq 'ARRAY') {
		foreach my $disk (@{$params{'disk'}}) {
		    push @hostdisk, $params{'host'}, $disk;
		}
	    } else {
		push @hostdisk, $params{'host'}, ${'disk'};
	    }
	} else {
	    push @hostdisk, $params{'host'};
	}
	$params{'hostdisk'} = \@hostdisk;
    }
    $params{'config'} = $params{'CONF'};
    $params{'exact_match'} = 1;

    Amanda::Disklist::match_disklist(
        user_msg => $user_msg,
        exact_match => $params{'exact_match'},
        args        => $params{'hostdisk'});

    my ($amflush, $messages) = Amanda::Amflush->new(%params, user_msg => $user_msg);
    push @result_messages, @{$messages};
    open STDERR, ">>&", $amflush->{'amdump_log'} || die("stdout: $!");

    my $code;
    my @ts = Amanda::Holding::get_all_datestamps();
    my @datestamps;
    if (defined $params{'datestamps'} and @{$params{'datestamps'}}) {
	foreach my $datestamp (@{$params{'datestamps'}}) {
	    my $matched = 0;
	    foreach my $ts (@ts) {
		if (Amanda::Util::match_datestamp($datestamp, $ts)) {
		    push @datestamps, $ts;
		    $matched = 1;
		    last;
		}
	    }
	    if (!$matched) {
		push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 2200002,
			severity     => $Amanda::Message::INFO,
			datestamp    => $datestamp);
	    }
	}
	$code = 2200003;
    } else {
	@datestamps = @ts;
	$code = 2200004;
    }
    if (!@datestamps) {
	push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => $code,
			severity     => $Amanda::Message::WARNING);
	return \@result_messages;
    }

    # fork the amdump process and detach
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $to_flushs = $amflush->get_flush(datestamps => \@datestamps);
	my $exit_code = $amflush->run(0, $to_flushs);
	Amanda::Debug::debug("exiting with code $exit_code");
	exit($exit_code);
    }
    push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 2200005,
			severity     => $Amanda::Message::INFO);
    Dancer::status(202);

    return \@result_messages;
}


sub list {

   return "LIST or runs in not available yet\n";
}

1;
