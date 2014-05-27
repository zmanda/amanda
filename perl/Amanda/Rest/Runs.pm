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
use Amanda::CheckDump;
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
        "message" : "The trace log file is '/var/log/amanda/test/log.20140205112550.0'",
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

=item Run amvault

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


=item Run amcheckdump

request:
  POST localhost:5000/amanda/v1.0/configs/:CONFIG/runs/checkdump
    query argument:
        timestamp=TIMESTAMP

reply:
  HTTP status 202 Accepted
  [
     {
        "code" : "2700018",
        "message" : "Running a CheckDump",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Runs.pm",
        "source_line" : "415"
     },
     {
        "code" : "2700020",
        "message" : "The message filename is 'checkdump.3545'",
        "message_filename" : "checkdump.3545",
        "severity" : "2",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Runs.pm",
        "source_line" : "420"
     }
  ]

=item Get messages for amcheckdump

request:
  POST http://localhost:5000/amanda/v1.0/configs/:CONFIG/runs/messages
    query argument:
        message_filename=MESAGE_FILENAME

reply:
  HTTP status 200 Ok
  [
     {
        "code" : 2700001,
        "labels" : [
           {
              "available" : 0,
              "label" : "test-ORG-AG-vtapes-005",
              "storage" : "my_vtapes"
           }
        ],
        "message" : "You will need the following volume: test-ORG-AG-vtapes-005",
        "severity" : 16,
        "source_filename" : "/usr/lib/amanda/perl/Amanda/CheckDump.pm",
        "source_line" : "367"
     },
     {
        "code" : 2700005,
        "diskname" : "/bootAMGTAR",
        "dump_timestamp" : "20140516130638",
        "hostname" : "localhost.localdomain",
        "level" : 1,
        "message" : "Validating image localhost.localdomain:/bootAMGTAR dumped 20140516130638 level 1",
        "nparts" : 1,
        "severity" : 16,
        "source_filename" : "/usr/lib/amanda/perl/Amanda/CheckDump.pm",
        "source_line" : "402"
     },
     {
        "code" : 2700003,
        "filenum" : 1,
        "label" : "test-ORG-AG-vtapes-005",
        "message" : "Reading volume test-ORG-AG-vtapes-005 file 1",
        "severity" : 16,
        "source_filename" : "/usr/lib/amanda/perl/Amanda/CheckDump.pm",
        "source_line" : "175"
     },
     {
        "code" : 2700006,
        "message" : "All images successfully validated",
        "severity" : 16,
        "source_filename" : "/usr/lib/amanda/perl/Amanda/CheckDump.pm",
        "source_line" : "695"
     }
  ]

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

sub checkdump {
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

    Amanda::Util::set_pname("amcheckdump");

    my $message_filename = "checkdump.$$";
    my $message_path =  getconf($CNF_LOGDIR) . "/" . $message_filename;
    my $message_fh;
    if (open ($message_fh, ">$message_path") == 0) {
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700021,
		message_filename => $message_filename,
		errno            => $!,
		severity         => $Amanda::Message::ERROR);
	return \@result_messages;
    }

    my $count = 0;
    my $user_msg = sub {
	my $msg = shift;
	my $d;
	if (ref $msg eq "SCALAR") {
	    $d = Data::Dumper->new([$msg], ["MESSAGES[$count]"]);
	} else {
	    my %msg_hash = %$msg;
	    $d = Data::Dumper->new([\%msg_hash], ["MESSAGES[$count]"]);
	}
	print {$message_fh} $d->Dump();
	#print Data::Dumper::Dumper($msg) , "\n";
	#print {$message_fh} Data::Dumper::Dumper($msg) , "\n";
	#push @result_messages, $msg;
	$count++;
    };

    my ($checkdump, $messages) = Amanda::CheckDump->new(%params, user_msg => $user_msg);
    push @result_messages, @{$messages};

    if (!$checkdump) {
	return \@result_messages;
    }

    my $exit_status = 0;
    my $exit_cb = sub {
	($exit_status) = @_;
	Amanda::MainLoop::quit();
    };

    my $pid = POSIX::fork();
    if ($pid == 0) {
	Amanda::MainLoop::call_later(sub { $checkdump->run($exit_cb) });
	Amanda::MainLoop::run();
	exit;
    } elsif ($pid > 0) {
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700018,
		severity         => $Amanda::Message::INFO);
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700020,
		message_filename => $message_filename,
		severity         => $Amanda::Message::INFO);
    } else {
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700019,
		severity         => $Amanda::Message::ERROR);
    }
    Dancer::status(202);

    return \@result_messages;
}

sub messages {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    if (!$params{'message_filename'}) {
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700022,
		severity         => $Amanda::Message::ERROR);
	return \@result_messages;
    }
    my $message_path =  getconf($CNF_LOGDIR) . "/" . $params{'message_filename'};
    my $message_fh;
    if (open ($message_fh, "<$message_path") == 0) {
	push @result_messages, Amanda::CheckDump::Message->new(
		source_filename  => __FILE__,
		source_line      => __LINE__,
		code             => 2700023,
		message_filename => $params{'message_filename'},
		errno            => $!,
		severity         => $Amanda::Message::ERROR);
	return \@result_messages;
    }

    my $data;
    {
	local $/;
	$data = <$message_fh>;
    }

    my @MESSAGES;
    eval $data;
    return \@MESSAGES;
}

sub list {

   return "LIST or runs in not available yet\n";
}

1;
