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

use Test::More tests => 102;

use strict;
use warnings;
use File::Path;
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
use Amanda::Util;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();
my $debug = !exists $ENV{'HARNESS_ACTIVE'};

# parameters:
#   emulate - inetd or amandad (default)
#   datapath -
#	none: do not send fe_amidxtaped_datapath
#	amanda: send fe_amidxtaped_datapath and do datapath negotiation, but send AMANDA
#	directtcp: send fe_amidxtaped_datapath and do datapath negotiation and send both
#		(expects an answer of AMANDA, too)
#   header - send HEADER and expect a header
#   splits - send fe_recover_splits (value is 0, 'basic' (one part; default), or 'parts' (multiple))
#   digit_end - end command with digits instead of 'END'
#   dumpspec - include DISK=, HOST=, (but not DATESTAMP=) that match the dump (default 1)
#   feedme - send a bad device initially, and expect FEEDME response
#   holding - filename of holding file to recover from
#   bad_auth - send incorrect auth in OPTIONS (amandad only)
#   holding_err - 'could not open' error from bogus holding file
#   holding_no_colon_zero - do not append a :0 to the holding filename in DEVICE=
#   no_config - do not send CONFIG=
#   no_tapespec - do not send a tapespec in LABEL=, and send the first partnum in FSF=
#	no_fsf - or don't send the first partnum in FSF= and leave amidxtaped to guess
#   ndmp - using NDMP device (so expect directtcp connection)
#   bad_cmd - send a bogus command line and expect an error
sub test {
    my %params = @_;

    # sort out the parameters
    $params{'emulate'} ||= 'amandad';
    $params{'datapath'} ||= 'none';
    $params{'splits'} = 'basic' unless exists $params{'splits'};
    $params{'dumpspec'} = 1 unless exists $params{'dumpspec'};

    # ignore some incompatible combinations
    return if ($params{'datapath'} ne 'none' and not $params{'splits'});
    return if ($params{'bad_auth'} and $params{'emulate'} ne 'amandad');
    return if ($params{'feedme'} and not $params{'splits'});
    return if ($params{'feedme'} and $params{'holding'});
    return if ($params{'holding_err'} and not $params{'holding'});
    return if ($params{'emulate'} eq 'amandad' and not $params{'splits'});
    return if ($params{'holding_no_colon_zero'} and not $params{'holding'});

    my $service;
    my $datasize = -1; # -1 means EOF never arrived
    my $hdr;

    my $expect_error = ($params{'bad_auth'}
		     or $params{'holding_err'}
		     or $params{'bad_cmd'});

    my $chg_name;
    if ($params{'ndmp'}) {
	$chg_name = "ndmp_server"; # changer name from ndmp dumpcache
    } else {
	$chg_name = "chg-disk:" . Installcheck::Run::vtape_dir();
    }

    alarm(120);
    local $SIG{'ALRM'} = sub {
	diag "TIMEOUT";
	$service->kill();
    };

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
    $testmsg .= $params{'holding_no_colon_zero'}? "holding-no-:0 " : "";
    $testmsg .= $params{'no_config'}? "no-config " : "";
    $testmsg .= $params{'no_tapespec'}? "no-tapespec " : "";
    $testmsg .= $params{'no_fsf'}? "no-fsf " : "";
    $testmsg .= $params{'bad_cmd'}? "bad_cmd " : "";

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
	# note that the earlier features are ignored..
	my $sendfeat = Amanda::Feature::Set->mine();
	if ($params{'datapath'} eq 'none') {
	    $sendfeat->remove($Amanda::Feature::fe_amidxtaped_datapath);
	}
	unless ($params{'splits'}) {
	    $sendfeat->remove($Amanda::Feature::fe_recover_splits);
	}
	if (!$params{'holding'}) {
	    if ($params{'splits'} eq 'parts') {
		# nine-part dump
		if ($params{'no_tapespec'}) {
		    $service->send($cmd_stream, "LABEL=TESTCONF01\r\n");
		} else {
		    $service->send($cmd_stream, "LABEL=TESTCONF01:1,2,3,4,5,6,7,8,9\r\n");
		}
	    } else {
		# single-part dump
		$service->send($cmd_stream, "LABEL=TESTCONF01:1\r\n");
	    }
	}
	if (!$params{'no_fsf'}) {
	    if ($params{'no_tapespec'}) {
		$service->send($cmd_stream, "FSF=1\r\n");
	    } else {
		$service->send($cmd_stream, "FSF=0\r\n");
	    }
	}
	if ($params{'bad_cmd'}) {
	    $service->send($cmd_stream, "AWESOMENESS=11\r\n");
	    return $subs{'expect_err_message'}->();
	}
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
	    $safe .= ':0' unless $params{'holding_no_colon_zero'};
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
	    if ($params{'holding'}) {
		$service->send($cmd_stream, "DATESTAMP=^20111111090909\$\r\n");
	    } else {
		my $timestamp = $Installcheck::Dumpcache::timestamps[0];
		$service->send($cmd_stream, "DATESTAMP=^$timestamp\$\r\n");
	    }
	}
	$service->send($cmd_stream, "CONFIG=TESTCONF\r\n")
	    unless $params{'no_config'};
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
	    $subs{'expect_err_message'}->();
	} else {
	    $subs{'expect_header'}->();
	}
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

	$service->expect($data_stream,
	    [ bytes => 1, $subs{'got_early_bytes'} ]);

	$hdr = Amanda::Header->from_string($buf);
	$subs{'expect_datapath'}->();
    });

    $subs{'got_early_bytes'} = make_cb(got_early_bytes => sub {
	$event->("GOT-EARLY-BYTES");
    });

    $subs{'expect_datapath'} = make_cb(expect_datapath => sub {
	if ($params{'datapath'} ne 'none') {
	    my $dp = ($params{'datapath'} eq 'amanda')? 'AMANDA' : 'AMANDA DIRECT-TCP';
	    $service->send($cmd_stream, "AVAIL-DATAPATH $dp\r\n");
	    $event->("SENT-DATAPATH");

	    $service->expect($cmd_stream,
		[ re => qr/^USE-DATAPATH .*\r\n/, $subs{'got_dp'} ]);
	} else {
	    $subs{'expect_data'}->();
	}
    });

    $subs{'got_dp'} = make_cb(got_dp => sub {
	my ($dp, $addrs) = ($_[0] =~ /USE-DATAPATH (\S+)(.*)\r\n/);
	$event->("GOT-DP-$dp");

	# if this is a direct-tcp connection, then we need to connect to
	# it and expect the data across it
	if ($dp eq 'DIRECT-TCP') {
	    my ($port) = ($addrs =~ / 127.0.0.1:(\d+).*/);
	    die "invalid DIRECT-TCP reply $addrs" unless ($port);
	    #remove got_early_bytes on $data_stream
	    $service->expect($data_stream,
	        [ eof => $subs{'do_nothing'} ]);

	    $service->connect('directtcp', $port);
	    $data_stream = 'directtcp';
	}

	$subs{'expect_data'}->();
    });

    $subs{'do_nothing'} = make_cb(do_nothing => sub {
    });

    $subs{'expect_data'} = make_cb(expect_data => sub {
	$service->expect($data_stream,
	    [ bytes_to_eof => $subs{'got_data'} ]);
	# note that we ignore EOF on the control connection,
	# as its timing is not very predictable

	if ($params{'datapath'} ne 'none') {
	    $service->send($cmd_stream, "DATAPATH-OK\r\n");
	    $event->("SENT-DATAPATH-OK");
	}

    });

    $subs{'got_data'} = make_cb(got_data => sub {
	my ($bytes) = @_;

	$datasize = $bytes;
	$event->("DATA-TO-EOF");
    });

    # expected errors jump right to this
    $subs{'expect_err_message'} = make_cb(expect_err_message => sub {
	$expect_error = 1;
	$service->expect($cmd_stream,
	    [ re => qr/^MESSAGE.*\r\n/, $subs{'got_err_message'} ])
    });

    $subs{'got_err_message'} = make_cb(got_err_message => sub {
	my ($line) = @_;
	if ($line =~ /^MESSAGE invalid command.*/) {
	    $event->("ERR-INVAL-CMD");
	} elsif ($line =~ /^MESSAGE could not open.*/) {
	    $event->('GOT-HOLDING-ERR');
	} else {
	    $event->('UNKNOWN-MSG');
	}

	# process should exit now
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
		"$testmsg (header mismatch; header logged to debug log)")
		or $hdr->debug_dump();
	}
    }

    if ($ok and !$expect_error) {
	if ($params{'holding'}) {
	    $ok = 0 if ($datasize != 131072);
	    diag("got $datasize bytes of data but expected exactly 128k from holding file")
		unless $ok;
	} else {
	    # get the original size from the header and calculate the size we
	    # read, rounded up to the next kilobyte
	    my $orig_size = $hdr? $hdr->{'orig_size'} : 0;
	    my $got_kb = int($datasize / 1024);

	    if ($orig_size) {
		my $diff = abs($got_kb - $orig_size);

		# allow 32k of "slop" here, for rounding, etc.
		$ok = 0 if $diff > 32;
		diag("got $got_kb kb; expected about $orig_size kb based on header")
		    unless $ok;
	    } else {
		$ok = 0 if $got_kb < 64;
		diag("got $got_kb; expected at least 64k")
		    unless $ok;
	    }
	}

	if (!$ok) {
	    fail($testmsg);
	}
    }

    if ($ok) {
	my $inetd = $params{'emulate'} eq 'inetd';

	my @sec_evts = $inetd? ('MAIN-SECURITY') : ('SENT-REQ', 'GOT-REP'),
	my @datapath_evts;
	if ($params{'datapath'} eq 'amanda') {
	    @datapath_evts = ('SENT-DATAPATH', 'GOT-DP-AMANDA', 'SENT-DATAPATH-OK');
	} elsif ($params{'datapath'} eq 'directtcp' and not $params{'ndmp'}) {
	    @datapath_evts = ('SENT-DATAPATH', 'GOT-DP-AMANDA', 'SENT-DATAPATH-OK');
	} elsif ($params{'datapath'} eq 'directtcp' and $params{'ndmp'}) {
	    @datapath_evts = ('SENT-DATAPATH', 'GOT-DP-DIRECT-TCP', 'SENT-DATAPATH-OK');
	}

	my @exp_events = (
		    @sec_evts,
		    'SEND-FEAT', 'GOT-FEAT', 'SENT-CMD',
		    ($inetd and $params{'splits'})? ('GOT-CONNECT', 'DATA-SECURITY') : (),
		    $params{'feedme'}? ('GOT-FEEDME') : (),
		    $params{'header'}? ('GOT-HEADER') : (),
		    @datapath_evts,
		    'DATA-TO-EOF', 'EXIT-0', );
	# handle a few error conditions differently
	if ($params{'bad_cmd'}) {
	    @exp_events = ( @sec_evts, 'ERR-INVAL-CMD', 'EXIT-0' );
	}
	if ($params{'bad_auth'}) {
	    @exp_events = ( 'SENT-REQ', 'GOT-REP-ERR', 'EXIT-1' );
	}
	if ($params{'holding_err'}) {
	    @exp_events = (
		    @sec_evts,
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

    my $hdir = "$holdingdir/20111111090909";
    my $safe_diskname = Amanda::Util::sanitise_filename($diskname);
    my $filename = "$hdir/localhost.$safe_diskname.3";

    mkpath($hdir) or die("Could not create $hdir");
    open(my $fh, ">", $filename) or die "opening '$filename': $!";

    # header plus 128k

    my $hdr = Amanda::Header->new();
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

Installcheck::Dumpcache::load('basic');
my $loaded_dumpcache = 'basic';
my $holdingfile;
my $emulate;

for my $splits (0, 'basic', 'parts') { # two flavors of 'true'
    if ($splits and $splits ne $loaded_dumpcache) {
	Installcheck::Dumpcache::load($splits);
	$loaded_dumpcache = $splits;
    }
    for $emulate ('inetd', 'amandad') {
	# note that 'directtcp' here expects amidxtaped to reply with AMANDA
	for my $datapath ('none', 'amanda', 'directtcp') {
	    for my $header (0, 1) {
		for my $feedme (0, 1) {
		    for my $holding (0, 1) {
			if ($holding and (!$holdingfile or ! -e $holdingfile)) {
			    $holdingfile = make_holding_file();
			}
			test(
			    emulate => $emulate,
			    datapath => $datapath,
			    header => $header,
			    splits => $splits,
			    feedme => $feedme,
			    $holding? (holding => $holdingfile):(),
			);
		    }
		}
	    }
	}

	# dumps from media can omit the tapespec in the label (amrecover-2.4.5 does
	# this).  We try it with multiple
	test(emulate => $emulate, splits => $splits, no_tapespec => 1);

	# and may even omit the FSF! (not sure what does this, but it's testable)
	test(emulate => $emulate, splits => $splits, no_tapespec => 1, no_fsf => 1);
    }
}

Installcheck::Dumpcache::load("basic");
$holdingfile = make_holding_file();
$loaded_dumpcache = 'basic';

## miscellaneous edge cases

for $emulate ('inetd', 'amandad') {
    # can send something beginning with a digit instead of "END\r\n"
    test(emulate => $emulate, digit_end => 1);

    # missing dumpspec doesn't cause an error
    test(emulate => $emulate, dumpspec => 0);

    # missing holding generates error message
    test(emulate => $emulate,
	 holding => "$Installcheck::TMP/no-such-file", holding_err => 1);

    # holding can omit the :0 suffix (amrecover-2.4.5 does this)
    test(emulate => $emulate, holding => $holdingfile,
	 holding_no_colon_zero => 1);
}

# bad authentication triggers an error message
test(emulate => 'amandad', bad_auth => 1);

# and a bad command triggers an error
test(emulate => 'amandad', bad_cmd => 1);

## check decompression

Installcheck::Dumpcache::load('compress');

test(dumpspec => 0, emulate => 'amandad',
     datapath => 'none', header => 1,
     splits => 'basic', feedme => 0, holding => 0);

## directtcp device (NDMP)

SKIP: {
    skip "not built with ndmp and server", 5 unless
	Amanda::Util::built_with_component("ndmp") and
	Amanda::Util::built_with_component("server");

    my $ndmp = Installcheck::Mock::NdmpServer->new();
    Installcheck::Dumpcache::load('ndmp');
    $ndmp->edit_config();

    # test a real directtcp transfer both with and without a header
    test(emulate => 'amandad', splits => 'basic',
	datapath => 'directtcp', header => 1, ndmp => $ndmp);
    test(emulate => 'amandad', splits => 'basic',
	datapath => 'directtcp', header => 0, ndmp => $ndmp);

    # and likewise an amanda transfer with a directtcp device
    test(emulate => 'amandad', splits => 'basic',
	datapath => 'amanda', header => 1, ndmp => $ndmp);
    test(emulate => 'amandad', splits => 'basic',
	datapath => 'amanda', header => 0, ndmp => $ndmp);

    # and finally a datapath-free transfer with such a device
    test(emulate => 'amandad', splits => 'basic',
	datapath => 'none', header => 1, ndmp => $ndmp);
}

## cleanup

unlink($holdingfile);
Installcheck::Run::cleanup();
