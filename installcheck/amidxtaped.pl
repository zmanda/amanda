# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 85;

use strict;
use warnings;
use Data::Dumper;
use Carp;
use POSIX;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Run qw( $diskname $holdingdir );
use Installcheck::Dumpcache;
use Installcheck::ClientService qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Header;
use Amanda::Feature;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();
my $debug = !exists $ENV{'HARNESS_ACTIVE'};

# parameters:
#   emulate - inetd or amandad (default)
#   datapath -
#	none: do not send fe_amidxtaped_datapath
#	amanda: send fe_amidxtaped_dtapth and do datapath negotiation, but send AMANDA
#	amanda: send fe_amidxtaped_dtapth and do datapath negotiation and send DIRECT-TCP
#		(expects an answer of DIRECT-TCP, too)
#   header - send HEADER and expect a header
#   splits - send fe_recover_splits (value is 'basic' (one part) or 'parts' (multiple))
#   digit_end - end command with digits instead of 'END'
#   dumpspec - include DISK=, HOST=, (but not DATESTAMP=) that match the dump
#   feedme - send a bad device initially, and expect FEEDME response
#   holding - filename of holding file to recover from
#   bad_auth - send incorrect auth in OPTIONS (amandad only)
#   holding_err - 'could not open' error from bogus holding file
#   ndmp - using NDMP device (so expect directtcp connection)
sub test {
    my %params = @_;

    # sort out the parameters
    $params{'emulate'} ||= 'amandad';
    $params{'datapath'} ||= 'none';

    # ignore some incompatible combinations
    return if ($params{'datapath'} ne 'none' and not $params{'splits'});
    return if ($params{'bad_auth'} and $params{'emulate'} ne 'amandad');
    return if ($params{'feedme'} and not $params{'splits'});
    return if ($params{'feedme'} and $params{'holding'});
    return if ($params{'holding_err'} and not $params{'holding'});
    return if ($params{'emulate'} eq 'amandad' and not $params{'splits'});

    my $service;
    my $datasize = 0;
    my $hdr;
    my $expect_error = ($params{'bad_auth'} or $params{'holding_err'});
    my $chg_name;
    if ($params{'ndmp'}) {
	$chg_name = "ndmp_server"; # changer name from ndmp dumpcache
    } else {
	$chg_name = "chg-disk:" . Installcheck::Run::vtape_dir();
    }

    # this test run shoud finish reasonably quickly
    alarm(120);

    # useful sub to report an event
    my @events;
    my $event = sub {
	my ($evt) = @_;
	diag($evt) if $debug;
	push @events, $evt;
    };

    my $testmsg = $params{'emulate'} . " ";
    $testmsg .= $params{'header'}? "header " : "no-header ";
    $testmsg .= "datapath($params{'datapath'}) ";
    $testmsg .= $params{'splits'}? "fe-splits($params{splits}) " : "!fe-splits ";
    $testmsg .= $params{'feedme'}? "feedme " : "!feedme ";
    $testmsg .= $params{'holding'}? "holding " : "media ";
    $testmsg .= $params{'dumpspec'}? "dumpspec " : "";
    $testmsg .= $params{'digit_end'}? "digits " : "";
    $testmsg .= $params{'bad_auth'}? "bad_auth " : "";
    $testmsg .= $params{'holding_err'}? "holding_err " : "";
    $testmsg .= $params{'ndmp'}? "ndmp " : "";

    diag("starting $testmsg") if $debug;

    # walk the service through its paces, using the Expect functionality from
    # ClientService. This has lots of $params conditionals, so it can be a bit
    # difficult to read!

    my %subs;
    my ($data_stream, $cmd_stream);

    $subs{'start'} = make_cb(send_cmd => sub {
	$cmd_stream = 'main';
	if ($params{'emulate'} eq 'inetd') {
	    # send security line
	    $service->send('main', "SECURITY USER installcheck\r\n");
	    $event->("MAIN-SECURITY");
	    $subs{'send_cmd1'}->();
	} else {
	    # send REQ packet
	    my $featstr = Amanda::Feature::Set->mine()->as_string();
	    my $auth = $params{'bad_auth'}? 'bogus' : 'bsdtcp';
	    $service->send('main', "OPTIONS features=$featstr;auth=$auth;");
	    $service->close('main', 'w');
	    $event->('SENT-REQ');
	    $subs{'expect_rep'}->();
	}
    });

    $subs{'expect_rep'} = make_cb(expect_rep => sub {
	my $ctl_hdl = DATA_FD_OFFSET;
	my $data_hdl = DATA_FD_OFFSET+1;
	$service->expect('main',
	    [ re => qr/^CONNECT CTL $ctl_hdl DATA $data_hdl\n\n/, $subs{'got_rep'} ],
	    [ re => qr/^ERROR .*\n/, $subs{'got_rep_err'} ]);
    });

    $subs{'got_rep'} = make_cb(got_rep => sub {
	$event->('GOT-REP');
	$cmd_stream = 'stream1';
	$service->expect('main',
	    [ eof => $subs{'send_cmd1'} ]);
    });

    $subs{'got_rep_err'} = make_cb(got_rep_err => sub {
	die "$_[0]" unless $expect_error;
	$event->('GOT-REP-ERR');
    });

    $subs{'send_cmd1'} = make_cb(send_cmd1 => sub {
	my $sendfeat = Amanda::Feature::Set->mine();
	if ($params{'datapath'} eq 'none') {
	    $sendfeat->remove($Amanda::Feature::fe_amidxtaped_datapath);
	}
	unless ($params{'splits'}) {
	    $sendfeat->remove($Amanda::Feature::fe_recover_splits);
	}
	$service->send($cmd_stream, "LABEL=TESTCONF01:1\r\n") unless $params{'holding'};
	$service->send($cmd_stream, "FSF=0\r\n");
	$service->send($cmd_stream, "HEADER\r\n") if $params{'header'};
	$service->send($cmd_stream, "FEATURES=" . $sendfeat->as_string() . "\r\n");
	$event->("SEND-FEAT");

	# the feature line looks different depending on what we're emulating
	if ($params{'emulate'} eq 'inetd') {
	    # note that this has no trailing newline.  Rather than rely on the
	    # TCP connection to feed us all the bytes and no more, we just look
	    # for the exact feature sequence we expect.
	    my $mine = Amanda::Feature::Set->mine()->as_string();
	    $service->expect($cmd_stream,
		[ re => qr/^$mine/, $subs{'got_feat'} ]);
	} else {
	    $service->expect($cmd_stream,
		[ re => qr/^FEATURES=[0-9a-f]+\r\n/, $subs{'got_feat'} ]);
	}
    });

    $subs{'got_feat'} = make_cb(got_feat => sub {
	$event->("GOT-FEAT");

	# continue sending the command
	if ($params{'holding'}) {
	    my $safe = $params{'holding'};
	    $safe =~ s/([\\:;,])/\\$1/g;
	    $safe .= ':0';
	    $service->send($cmd_stream, "DEVICE=$safe\r\n");
	} elsif ($params{'feedme'}) {
	    # bogus device name
	    $service->send($cmd_stream, "DEVICE=file:/does/not/exist\r\n");
	} else {
	    $service->send($cmd_stream, "DEVICE=$chg_name\r\n");
	}
	if ($params{'dumpspec'}) {
	    $service->send($cmd_stream, "HOST=^localhost\$\r\n");
	    $service->send($cmd_stream, "DISK=^$Installcheck::Run::diskname\$\r\n");
	    # we don't know the datestamp from the dumpcache, so omit this for now
	    #$service->send($cmd_stream, "DATESTAMP=^$datestamp\$\r\n");
	}
	$service->send($cmd_stream, "CONFIG=TESTCONF\r\n");
	if ($params{'digit_end'}) {
	    $service->send($cmd_stream, "999\r\n"); # dunno why this works..
	} else {
	    $service->send($cmd_stream, "END\r\n");
	}
	$event->("SENT-CMD");

	$subs{'expect_connect'}->();
    });

    $subs{'expect_connect'} = make_cb(expect_connect => sub {
	if ($params{'splits'}) {
	    if ($params{'emulate'} eq 'inetd') {
		$service->expect($cmd_stream,
		    [ re => qr/^CONNECT \d+\n/, $subs{'got_connect'} ]);
	    } else {
		$data_stream = 'stream2';
		$subs{'expect_feedme'}->();
	    }
	} else {
	    # with no split parts, data comes on the command stream
	    $data_stream = $cmd_stream;
	    $subs{'expect_feedme'}->();
	}
    });

    $subs{'got_connect'} = make_cb(got_connect => sub {
	my ($port) = ($_[0] =~ /CONNECT (\d+)/);
	$event->("GOT-CONNECT");

	$service->connect('data', $port);
	$data_stream = 'data';
	$service->send($data_stream, "SECURITY USER installcheck\r\n");
	$event->("DATA-SECURITY");

	$subs{'expect_feedme'}->();
    });

    $subs{'expect_feedme'} = make_cb(expect_feedme => sub  {
	if ($params{'feedme'}) {
	    $service->expect($cmd_stream,
		[ re => qr/^FEEDME TESTCONF01\r\n/, $subs{'got_feedme'} ]);
	} elsif ($params{'holding_err'}) {
	    $service->expect($cmd_stream,
		[ re => qr/^MESSAGE could not open.*\r\n/, $subs{'got_holding_err'} ]);
	} else {
	    $subs{'expect_header'}->();
	}
    });

    $subs{'got_holding_err'} = make_cb(got_holding_err => sub {
	$event->('GOT-HOLDING-ERR');
	# process should exit..
    });

    $subs{'got_feedme'} = make_cb(got_feedme => sub {
	$event->('GOT-FEEDME');
	my $dev_name = "file:" . Installcheck::Run::vtape_dir();
	$service->send($cmd_stream, "TAPE $dev_name\r\n");
	$subs{'expect_header'}->();
    });

    $subs{'expect_header'} = make_cb(expect_header => sub {
	if ($params{'header'}) {
	    $service->expect($data_stream,
		[ bytes => 32768, $subs{'got_header'} ]);
	} else {
	    $subs{'expect_datapath'}->();
	}
    });

    $subs{'got_header'} = make_cb(got_header => sub {
	my ($buf) = @_;
	$event->("GOT-HEADER");

	$hdr = Amanda::Header->from_string($buf);
	$subs{'expect_datapath'}->();
    });

    $subs{'expect_datapath'} = make_cb(expect_datapath => sub {
	if ($params{'datapath'} ne 'none') {
	    my $dp = ($params{'datapath'} eq 'amanda')? 'AMANDA' : 'DIRECT-TCP';
	    $service->send($cmd_stream, "DATA-PATH $dp\r\n");
	    $event->("SENT-DP-$dp");

	    $service->expect($cmd_stream,
		[ re => qr/^DATA-PATH .*\r\n/, $subs{'got_dp'} ]);
	} else {
	    $subs{'expect_data'}->();
	}
    });

    $subs{'got_dp'} = make_cb(got_dp => sub {
	my ($dp, $addrs) = ($_[0] =~ /DATA-PATH (\S+)(.*)\r\n/);
	$event->("GOT-DP-$dp");

	# if this is a direct-tcp connection, then we need to connect to
	# it and expect the data across it
	if ($dp eq 'DIRECT-TCP') {
	    my ($port) = ($addrs =~ / 127.0.0.1:(\d+).*/);
	    die "invalid DIRECT-TCP reply $addrs" unless ($port);
	    $service->connect('directtcp', $port);
	    $data_stream = 'directtcp';
	}

	$subs{'expect_data'}->();
    });

    $subs{'expect_data'} = make_cb(expect_data => sub {
	$service->expect($data_stream,
	    [ bytes_to_eof => $subs{'got_data'} ]);
	# note that we ignore EOF on the control connection,
	# as its timing is not very predictable
    });

    $subs{'got_data'} = make_cb(got_data => sub {
	my ($bytes) = @_;

	$datasize = $bytes;
	$event->("DATA-TO-EOF");
    });

    $subs{'process_done'} = make_cb(process_done => sub {
	my ($w) = @_;
	my $exitstatus = POSIX::WIFEXITED($w)? POSIX::WEXITSTATUS($w) : -1;
	$event->("EXIT-$exitstatus");
	Amanda::MainLoop::quit();
    });

    $service = Installcheck::ClientService->new(
	    emulate => $params{'emulate'},
	    service => 'amidxtaped',
	    process_done => $subs{'process_done'});
    Amanda::MainLoop::call_later($subs{'start'});
    Amanda::MainLoop::run();

    # reset the alarm - the risk of deadlock has passed
    alarm(0);

    # do a little bit of gymnastics to only treat this as one test

    my $ok = 1;

    if ($ok and !$expect_error and $params{'header'}) {
	if ($hdr->{'name'} ne 'localhost' or $hdr->{'disk'} ne $diskname) {
	    $ok = 0;
	    is_deeply([ $hdr->{'name'}, $hdr->{'disk'} ],
		      [ 'localhost',    $diskname ],
		$testmsg);
	}
    }

    if ($ok and !$expect_error) {
	if ($params{'holding'}) {
	    $ok = 0 if ($datasize != 131072);
	} else {
	    $ok = 0 if ($datasize < 65536);
	}

	if (!$ok) {
	    fail($testmsg);
	    diag("got $datasize bytes of data");
	}
    }

    if ($ok) {
	my $inetd = $params{'emulate'} eq 'inetd';
	my @datapath_evts;
	if ($params{'datapath'} eq 'amanda') {
	    @datapath_evts = ('SENT-DP-AMANDA', 'GOT-DP-AMANDA');
	} elsif ($params{'datapath'} eq 'directtcp' and not $params{'ndmp'}) {
	    @datapath_evts = ('SENT-DP-DIRECT-TCP', 'GOT-DP-AMANDA');
	} elsif ($params{'datapath'} eq 'directtcp' and $params{'ndmp'}) {
	    @datapath_evts = ('SENT-DP-DIRECT-TCP', 'GOT-DP-DIRECT-TCP');
	}
	my @exp_events = (
		    $inetd? ('MAIN-SECURITY') : ('SENT-REQ', 'GOT-REP'),
		    'SEND-FEAT', 'GOT-FEAT', 'SENT-CMD',
		    ($inetd and $params{'splits'})? ('GOT-CONNECT', 'DATA-SECURITY') : (),
		    $params{'feedme'}? ('GOT-FEEDME') : (),
		    $params{'header'}? ('GOT-HEADER') : (),
		    @datapath_evts,
		    'DATA-TO-EOF', 'EXIT-0', );
	# handle a few error conditions differently
	if ($params{'bad_auth'}) {
	    @exp_events = ( 'SENT-REQ', 'GOT-REP-ERR', 'EXIT-1' );
	}
	if ($params{'holding_err'}) {
	    @exp_events = (
		    $inetd? ('MAIN-SECURITY') : ('SENT-REQ', 'GOT-REP'),
		    'SEND-FEAT', 'GOT-FEAT', 'SENT-CMD',
		    ($inetd and $params{'splits'})? ('GOT-CONNECT', 'DATA-SECURITY') : (),
		    'GOT-HOLDING-ERR', 'EXIT-0' );
	}
	$ok = is_deeply([@events], [@exp_events],
	    $testmsg);
    }

    diag(Dumper([@events])) if not $ok;
}

sub make_holding_file {
    my $hdr = Amanda::Header->new();
    my $filename = "$Installcheck::TMP/holdingfile";
    open(my $fh, ">", $filename) or die "opening '$filename': $!";

    # header plus 128k

    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = '20111111090909';
    $hdr->{'dumplevel'} = 3;
    $hdr->{'compressed'} = 0;
    $hdr->{'comp_suffix'} = ".foo";
    $hdr->{'name'} = 'localhost';
    $hdr->{'disk'} = "$diskname";
    $hdr->{'program'} = "INSTALLCHECK";
    print $fh $hdr->to_string(32768,32768);

    my $bytes_to_write = 131072;
    my $bufbase = substr((('='x127)."\n".('-'x127)."\n") x 4, 8, -3) . "1K\n";
    die length($bufbase) unless length($bufbase) == 1024-8;
    my $k = 0;
    while ($bytes_to_write > 0) {
	my $buf = sprintf("%08x", $k++).$bufbase;
	my $written = $fh->syswrite($buf, $bytes_to_write);
	if (!defined($written)) {
	    die "writing holding file: $!";
	}
	$bytes_to_write -= $written;
    }
    close($fh);

    return $filename;
}

## normal operation

my $holdingfile = make_holding_file();
Installcheck::Dumpcache::load('basic');
my $loaded_dumpcache = 'basic';

for my $splits (0, 'basic', 'parts') { # two flavors of 'true'
    if ($splits and $splits ne $loaded_dumpcache) {
	Installcheck::Dumpcache::load($splits);
	$loaded_dumpcache = $splits;
    }
    for my $emulate ('inetd', 'amandad') {
	# note that 'directtcp' here expects amidxd to reply with AMANDA
	for my $datapath ('none', 'amanda', 'directtcp') {
	    for my $header (0, 1) {
		for my $feedme (0, 1) {
		    for my $holding (undef, $holdingfile) {
			test(
			    dumpspec => 1,
			    emulate => $emulate,
			    datapath => $datapath,
			    header => $header,
			    splits => $splits,
			    feedme => $feedme,
			    holding => $holding,
			);
		    }
		}
	    }
	}
    }
}

Installcheck::Dumpcache::load("basic");

## miscellaneous edge cases

# bad authentication triggers an error REP with amandad
test(emulate => 'amandad', bad_auth => 1);

for my $emulate ('inetd', 'amandad') {
    # can send something beginning with a digit instead of "END\r\n"
    test(emulate => $emulate, digit_end => 1);

    # missing dumpspec doesn't cause an error
    test(emulate => $emulate, dumpspec => 0);

    # missing holding generates error message
    test(emulate => $emulate, splits => 1,
	 holding => "$Installcheck::TMP/no-such-file", holding_err => 1);
}

## directtcp device (NDMP)

my $ndmp = Installcheck::Mock::NdmpServer->new();
Installcheck::Dumpcache::load('ndmp');
$ndmp->edit_config();

# test a real directtcp transfer both with and without a header
test(emulate => 'amandad', splits => 1,
    datapath => 'directtcp', header => 1, ndmp => $ndmp);
test(emulate => 'amandad', splits => 1,
    datapath => 'directtcp', header => 0, ndmp => $ndmp);

# and likewise an amanda transfer with a directtcp device
test(emulate => 'amandad', splits => 1,
    datapath => 'amanda', header => 1, ndmp => $ndmp);
test(emulate => 'amandad', splits => 1,
    datapath => 'amanda', header => 0, ndmp => $ndmp);

# and finally a datapath-free transfer with such a device
test(emulate => 'amandad', splits => 1,
    datapath => 'none', header => 1, ndmp => $ndmp);

## cleanup

unlink($holdingfile);
Installcheck::Run::cleanup();
