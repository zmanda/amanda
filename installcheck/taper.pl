# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 269;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Run;
use Installcheck::Mock;
use IO::Handle;
use IPC::Open3;
use Data::Dumper;
use IO::Socket::INET;
use POSIX ":sys_wait_h";
use Cwd qw(abs_path);

use Amanda::Paths;
use Amanda::Header qw( :constants );
use Amanda::Debug;
use Amanda::Paths;
use Amanda::Device qw( :constants );

# ABOUT THESE TESTS:
#
# We run a sequence of fixed interactions with the taper, putting it
# through its paces.  Each message to or from the taper is represented
# as a test, for readability.  If the taper produces unexpected results,
# the script dies, on the assumption that subsequent tests will be
# meaningless.
#
# This uses IPC::Open3 instead of Expect mainly because the interactions
# are so carefully scripted that Expect is not required.

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $test_filename = "$Installcheck::TMP/installcheck-taper-holding-file";
my $taper_stderr_file = "$Installcheck::TMP/taper-stderr";
my $debug = !exists $ENV{'HARNESS_ACTIVE'};

# information on the current run
my ($datestamp, $handle);
my ($taper_pid, $taper_in, $taper_out, $last_taper_reply, $taper_reply_timeout);

sub run_taper {
    my ($length, $description, %params) = @_;

    cleanup_taper();

    unless ($params{'keep_config'}) {
	my $testconf;
	if ($params{'new_vtapes'}) {
	    $testconf = Installcheck::Run::setup(1);
	} else {
	    $testconf = Installcheck::Run::setup();
	}
	$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
	if ($params{'notapedev'}) {
	    $testconf->remove_param('tapedev');
	    $testconf->remove_param('tpchanger');
	} elsif ($params{'ndmp_server'}) {
	    my $ndmp = $params{'ndmp_server'};
	    $ndmp->reset();
	    $ndmp->config($testconf);
	}
	unless ($params{'leom'} or $params{'ndmp_server'}) {
	    $testconf->add_param('device_property', '"LEOM" "OFF"');
	}
	$testconf->add_param('debug_taper', '9'); ## TEMPORARY
	$testconf->add_tapetype('TEST-TAPE', [
	    'length' =>  "$length",
	    ]);
	if ($params{'taperscan'}) {
	    $testconf->add_param('taperscan', "\"$params{'taperscan'}\"");
	    $testconf->add_taperscan($params{'taperscan'}, [
			'comment' => '"my taperscan is mine, not yours"',
			'plugin'  => "\"$params{'taperscan'}\"",
			]);

	}
	$testconf->write();
    }

    open(TAPER_ERR, ">", $taper_stderr_file);
    $taper_in = $taper_out = '';
    $taper_pid = open3($taper_in, $taper_out, ">&TAPER_ERR",
	"$amlibexecdir/taper", "TESTCONF");
    close TAPER_ERR;
    $taper_in->blocking(1);
    $taper_out->autoflush();

    if ($params{'keep_config'}) {
	pass("spawned new taper for $description (same config)");
    } else {
	pass("spawned taper for $description (tape length $length kb)");
    }

    # define this to get the installcheck to wait and allow you to attach
    # a gdb instance to the taper
    if ($params{'use_gdb'}) {
	$taper_reply_timeout = 0; # no timeouts while debugging
	diag("attach debugger to pid $taper_pid and press ENTER");
	<>;
    } else {
	$taper_reply_timeout = 120;
    }

    taper_cmd("START-TAPER worker0 $datestamp");
}

sub wait_for_exit {
    if ($taper_pid) {
	waitpid($taper_pid, 0);
	$taper_pid = undef;
    }
}

sub cleanup_taper {
    -f $test_filename and unlink($test_filename);
    -f $taper_stderr_file and unlink($taper_stderr_file);

    # make a small effort to collect zombies
    if ($taper_pid) {
	if (waitpid($taper_pid, WNOHANG) == $taper_pid) {
	    $taper_pid = undef;
	}
    }
}

sub taper_cmd {
    my ($cmd) = @_;

    diag(">>> $cmd") if $debug;
    print $taper_in "$cmd\n";
}

sub taper_reply {
    local $SIG{ALRM} = sub { die "Timeout while waiting for reply\n" };
    alarm($taper_reply_timeout);
    $last_taper_reply = $taper_out->getline();
    alarm(0);

    if (!$last_taper_reply) {
	die("wrong pid") unless ($taper_pid == waitpid($taper_pid, 0));
	my $exit_status = $?;

	open(my $fh, "<", $taper_stderr_file) or die("open $taper_stderr_file: $!");
	my $stderr = do { local $/; <$fh> };
	close($fh);

	diag("taper stderr:\n$stderr") if $stderr;
	die("taper (pid $taper_pid) died unexpectedly with status $exit_status");
    }

    # trim trailing whitespace -- C taper outputs an extra ' ' after
    # single-word replies
    $last_taper_reply =~ s/\s*$//;
    diag("<<< $last_taper_reply") if $debug;

    return $last_taper_reply;
}

sub check_logs {
    my ($expected, $msg) = @_;
    my $re;
    my $line;

    # must contain a pid line at the beginning and end
    unshift @$expected, qr/^INFO taper taper pid \d+$/;
    push @$expected, qr/^INFO taper pid-done \d+$/;

    open(my $logfile, "<", "$CONFIG_DIR/TESTCONF/log/log")
	or die("opening log: $!");
    my @logfile = grep(/^\S+ taper /, <$logfile>);
    close($logfile);

    while (@logfile and @$expected) {
	my $logline = shift @logfile;
	my $expline = shift @$expected;
	chomp $logline;
	if ($logline !~ $expline) {
	    like($logline, $expline, $msg);
	    return;
	}
    }
    if (@logfile) {
	fail("$msg (extra trailing log lines)");
	return;
    }
    if (@$expected) {
	fail("$msg (logfile ends early)");
	diag("first missing line should match ");
	diag("".$expected->[0]);
	return;
    }

    pass($msg);
}

sub cleanup_log {
    my $logfile = "$CONFIG_DIR/TESTCONF/log/log";
    -f $logfile and unlink($logfile);
}

# functions to create dumpfiles

sub write_dumpfile_header_to {
    my ($fh, $size, $hostname, $disk, $expect_failure) = @_;

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = $datestamp;
    $hdr->{'dumplevel'} = 0;
    $hdr->{'compressed'} = 0;
    $hdr->{'comp_suffix'} = ".foo";
    $hdr->{'name'} = $hostname;
    $hdr->{'disk'} = $disk;
    $hdr->{'program'} = "INSTALLCHECK";
    $hdr = $hdr->to_string(32768,32768);

    $fh->syswrite($hdr, 32768);
}

sub write_dumpfile_data_to {
    my ($fh, $size, $hostname, $disk, $expect_failure) = @_;

    my $bytes_to_write = $size;
    my $bufbase = substr((('='x127)."\n".('-'x127)."\n") x 4, 8, -3) . "1K\n";
    die length($bufbase) unless length($bufbase) == 1024-8;
    my $k = 0;
    while ($bytes_to_write > 0) {
	my $buf = sprintf("%08x", $k++).$bufbase;
	my $written = $fh->syswrite($buf, $bytes_to_write);
	if (!defined($written)) {
	    die "writing: $!" unless $expect_failure;
	    exit;
	}
	$bytes_to_write -= $written;
    }
}

# make a new holding-like file in test_filename
sub make_holding_file {
    my ($size, $hostname, $disk) = @_;
    open(my $fh, ">", $test_filename);
    write_dumpfile_header_to($fh, $size, $hostname, $disk);
    write_dumpfile_data_to($fh, $size, $hostname, $disk);
}

# connect to the given port and write a dumpfile; this *will* create
# zombies, but it's OK -- installchecks aren't daemons.
sub write_to_port {
    my ($port_cmd, $size, $hostname, $disk, $expect_error) = @_;

    my ($header_port, $data_addr) =
	($last_taper_reply =~ /^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+)/);

    # just run this in the child
    return unless fork() == 0;

    my $sock = IO::Socket::INET->new(
	PeerAddr => "127.0.0.1:$header_port",
	Proto => "tcp",
	ReuseAddr => 1,
    );

    write_dumpfile_header_to($sock, $size, $hostname, $disk, $expect_error);
    close $sock;

    $sock = IO::Socket::INET->new(
	PeerAddr => $data_addr,
	Proto => "tcp",
	ReuseAddr => 1,
    );

    write_dumpfile_data_to($sock, $size, $hostname, $disk, $expect_error);
    exit;
}

########

##
# A simple, one-part FILE-WRITE
$handle = "11-11111";
$datestamp = "20070102030405";
run_taper(4096, "single-part and multipart FILE-WRITE");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(1024*1024, "localhost", "/home");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /home 0 $datestamp \"\" \"\" \"\" \"\" \"\" \"\" \"\" \"\" 12");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 1024 "\[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]"$/,
	"got PARTDONE") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]" "" ""$/,
	"got DONE") or die;

##
# A multipart FILE-WRITE, using the same taper instance
#  (note that the third part is of length 0, and is not logged)

$handle = '11-22222';
make_holding_file(1024*1024, "localhost", "/usr");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /usr 0 $datestamp 524288 \"\" \"\" 1 \"\" \"\" \"\" \"\" 512");
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 512 "\[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 512 "\[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 512\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /home $datestamp 1/-1 0 \[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]$),
    qr(^DONE taper localhost /home $datestamp 1 0 \[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]$),
    qr(^PART taper TESTCONF01 2 localhost /usr $datestamp 1/-1 0 \[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]$),
    qr(^PART taper TESTCONF01 3 localhost /usr $datestamp 2/-1 0 \[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]$),
    qr(^DONE taper localhost /usr $datestamp 2 0 \[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 512\]$),
    qr(^INFO taper tape TESTCONF01 kb 2048 fm 4 \[OK\]$),
], "single-part and multi-part dump logged correctly");

# check out the headers on those files, just to be sure
{
    my $dev = Amanda::Device->new("file:" . Installcheck::Run::vtape_dir());
    die("bad device: " . $dev->error_or_status()) unless $dev->status == $DEVICE_STATUS_SUCCESS;

    $dev->start($ACCESS_READ, undef, undef)
	or die("can't start device: " . $dev->error_or_status());

    sub is_hdr {
	my ($hdr, $expected, $msg) = @_;
	my $got = {};
	for (keys %$expected) { $got->{$_} = "".$hdr->{$_}; }
	if (!is_deeply($got, $expected, $msg)) {
	    diag("got: " . Dumper($got));
	}
    }

    is_hdr($dev->seek_file(1), {
	type => $F_SPLIT_DUMPFILE,
	datestamp => $datestamp,
	name => 'localhost',
	disk => '/home',
    }, "header on file 1 is correct");

    is_hdr($dev->seek_file(2), {
	type => $F_SPLIT_DUMPFILE,
	datestamp => $datestamp,
	name => 'localhost',
	disk => '/usr',
	partnum => 1,
	totalparts => -1,
    }, "header on file 2 is correct");

    is_hdr($dev->seek_file(3), {
	type => $F_SPLIT_DUMPFILE,
	datestamp => $datestamp,
	name => 'localhost',
	disk => '/usr',
	partnum => 2,
	totalparts => -1,
    }, "header on file 3 is correct");

    is_hdr($dev->seek_file(4), {
	type => $F_SPLIT_DUMPFILE,
	datestamp => $datestamp,
	name => 'localhost',
	disk => '/usr',
	partnum => 3,
	totalparts => -1,
    }, "header on file 4 is correct");
}

##
# A PORT-WRITE with no disk buffer

$handle = "11-33333";
$datestamp = "19780615010203";
run_taper(4096, "multipart PORT-WRITE");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE worker0 $handle localhost /var 0 $datestamp 524288 \"\" 393216 1 0 \"\" \"\" 0 AMANDA");
like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_taper_reply, 63*32768, "localhost", "/var", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 4 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 5 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 5") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 6 96 "\[sec [\d.]+ bytes 98304 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 6") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE worker0 $handle 712");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 2064384 kps [\d.]+ orig-kb 712\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /var $datestamp 1/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 2 localhost /var $datestamp 2/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 3 localhost /var $datestamp 3/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 4 localhost /var $datestamp 4/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 5 localhost /var $datestamp 5/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 6 localhost /var $datestamp 6/-1 0 \[sec [\d.]+ bytes 98304 kps [\d.]+\]$),
    qr(^DONE taper localhost /var $datestamp 6 0 \[sec [\d.]+ bytes 2064384 kps [\d.]+ orig-kb 712\]$),
    qr(^INFO taper tape TESTCONF01 kb 2016 fm 6 \[OK\]$),
], "multipart PORT-WRITE logged correctly");

##
# Test NO-NEW-TAPE

$handle = "11-44444";
$datestamp = "19411207000000";
run_taper(4096, "testing NO-NEW-TAPE from the driver on 1st request");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(1024*1024, "localhost", "/home");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /home 0 $datestamp 0 \"\" 0 1 0 \"\" \"\" 0 912");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("NO-NEW-TAPE worker0 $handle sorry");
like(taper_reply, qr/^FAILED $handle INPUT-GOOD TAPE-ERROR "" "?sorry"?.*$/,
	"got FAILED") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^ERROR taper no-tape config \[sorry\]$),
    qr(^FAIL taper localhost /home $datestamp 0 config sorry$),
], "NO-NEW-TAPE logged correctly");

##
# Test retrying on EOT (via PORT-WRITE with a mem cache)

$handle = "11-55555";
$datestamp = "19750711095836";
run_taper(1024, "PORT-WRITE retry on EOT (mem cache)");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE worker0 $handle localhost /usr/local 0 $datestamp 786432 \"\" 786432 1 0 \"\" \"\" 0 AMANDA");
like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_taper_reply, 1575936, "localhost", "/usr/local", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 768 "\[sec [\d.]+ bytes 786432 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 768 "\[sec [\d.]+ bytes 786432 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 3 "\[sec [\d.]+ bytes 3072 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE worker0 $handle 1012");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1575936 kps [\d.]+ orig-kb 1012\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr/local $datestamp 1/-1 0 \[sec [\d.]+ bytes 786432 kps [\d.]+\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ bytes 163840 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 2 \[OK\]$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ bytes 786432 kps [\d.]+\]$),
    qr(^PART taper TESTCONF02 2 localhost /usr/local $datestamp 3/-1 0 \[sec [\d.]+ bytes 3072 kps [\d.]+\]$),
    qr(^DONE taper localhost /usr/local $datestamp 3 0 \[sec [\d.]+ bytes 1575936 kps [\d.]+ orig-kb 1012\]$),
    qr(^INFO taper tape TESTCONF02 kb 771 fm 2 \[OK\]$),
], "multivolume PORT-WRITE logged correctly");

##
# Test retrying on EOT (via FILE-WRITE)

$handle = "11-66666";
$datestamp = "19470815000000";
run_taper(1024, "FILE-WRITE retry on EOT");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(1575936, "localhost", "/usr");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /usr 0 $datestamp \"\" \"\" \"\" 1 786432 \"\" \"\" \"\" 1112");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 768 "\[sec [\d.]+ bytes 786432 kps [\d.]+ orig-kb 1112\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 768 "\[sec [\d.]+ bytes 786432 kps [\d.]+ orig-kb 1112\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 3 "\[sec [\d.]+ bytes 3072 kps [\d.]+ orig-kb 1112\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1575936 kps [\d.]+ orig-kb 1112\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr $datestamp 1/-1 0 \[sec [\d.]+ bytes 786432 kps [\d.]+ orig-kb 1112\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /usr $datestamp 2/-1 0 \[sec [\d.]+ bytes 163840 kps [\d.]+ orig-kb 1112\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 2 \[OK\]$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /usr $datestamp 2/-1 0 \[sec [\d.]+ bytes 786432 kps [\d.]+ orig-kb 1112\]$),
    qr(^PART taper TESTCONF02 2 localhost /usr $datestamp 3/-1 0 \[sec [\d.]+ bytes 3072 kps [\d.]+ orig-kb 1112\]$),
    qr(^DONE taper localhost /usr $datestamp 3 0 \[sec [\d.]+ bytes 1575936 kps [\d.]+ orig-kb 1112\]$),
    qr(^INFO taper tape TESTCONF02 kb 771 fm 2 \[OK\]$),
], "multivolume FILE-WRITE logged correctly");

##
# Test retrying on EOT (via PORT-WRITE with a disk cache)

$handle = "11-77777";
$datestamp = "20090427212500";
run_taper(1024, "PORT-WRITE retry on EOT (disk cache)");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE worker0 $handle localhost /usr/local 0 $datestamp 786432 \"$Installcheck::TMP\" 786432 1 0 \"\" \"\" 0 AMANDA");
like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_taper_reply, 1575936, "localhost", "/usr/local", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 768 "\[sec [\d.]+ bytes 786432 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 768 "\[sec [\d.]+ bytes 786432 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 3 "\[sec [\d.]+ bytes 3072 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE worker0 $handle 1212");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1575936 kps [\d.]+ orig-kb 1212\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr/local $datestamp 1/-1 0 \[sec [\d.]+ bytes 786432 kps [\d.]+\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ bytes 163840 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 2 \[OK\]$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ bytes 786432 kps [\d.]+\]$),
    qr(^PART taper TESTCONF02 2 localhost /usr/local $datestamp 3/-1 0 \[sec [\d.]+ bytes 3072 kps [\d.]+\]$),
    qr(^DONE taper localhost /usr/local $datestamp 3 0 \[sec [\d.]+ bytes 1575936 kps [\d.]+ orig-kb 1212\]$),
    qr(^INFO taper tape TESTCONF02 kb 771 fm 2 \[OK\]$),
], "multivolume PORT-WRITE (disk cache) logged correctly");

##
# Test failure on EOT (via PORT-WRITE with no cache), and a new try on the
# next tape.

$handle = "11-88888";
$datestamp = "20090424173000";
run_taper(1024, "PORT-WRITE failure on EOT (no cache)");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE worker0 $handle localhost /var/log 0 $datestamp 0 \"\" 0 0 0 \"\" \"\" 0 AMANDA");
like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_taper_reply, 1575936, "localhost", "/var/log", 1);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTIAL $handle INPUT-GOOD TAPE-ERROR "\[sec [\d.]+ bytes 0 kps [\d.]+\]" "" "No space left on device: more than MAX_VOLUME_USAGE bytes written, splitting not enabled"$/,
	"got PARTIAL") or die;
# retry on the next tape
$handle = "11-88899";
taper_cmd("PORT-WRITE worker0 $handle localhost /boot 0 $datestamp 0 \"\" 0 0 0 \"\" \"\" 0 AMANDA");
like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_taper_reply, 65536, "localhost", "/boot", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 64 "\[sec [\d.]+ bytes 65536 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE worker0 $handle 64");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 65536 kps [\d.]+ orig-kb 64\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PARTPARTIAL taper TESTCONF01 1 localhost /var/log $datestamp 1/-1 0 \[sec [\d.]+ bytes 983040 kps [\d.]+\] "No space left on device"$),
    qr(^PARTIAL taper localhost /var/log $datestamp 1 0 \[sec [\d.]+ bytes 0 kps [\d.]+\] "No space left on device: more than MAX_VOLUME_USAGE bytes written, splitting not enabled"$),
    qr(^INFO taper tape TESTCONF01 kb 0 fm 1 \[OK\]$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /boot $datestamp 1/-1 0 \[sec [\d.]+ bytes 65536 kps [\d.]+\]$),
    qr(^DONE taper localhost /boot $datestamp 1 0 \[sec [\d.]+ bytes 65536 kps [\d.]+ orig-kb 64\]$),
    qr(^INFO taper tape TESTCONF02 kb 64 fm 1 \[OK\]$),
], "failure on EOT (no cache) with subsequent dump logged correctly");

##
# Test running out of tapes (second REQUEST-NEW-TAPE fails)

$handle = "11-99999";
$datestamp = "20100101000000";
run_taper(512, "FILE-WRITE runs out of tapes");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(512*1024, "localhost", "/music");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /music 0 $datestamp \"\" \"\" \"\" 1 262144 \"none\" \"\" 10240 1312");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1312\]"$/,
	"got PARTDONE for filenum 1 on first tape") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("NO-NEW-TAPE worker0 $handle \"that's enough\"");
like(taper_reply, qr/^PARTIAL $handle INPUT-GOOD TAPE-ERROR "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1312\]" "" "that's enough"$/,
	"got PARTIAL") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /music $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1312\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /music $datestamp 2/-1 0 \[sec [\d.]+ bytes 163840 kps [\d.]+ orig-kb 1312\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF01 kb 256 fm 2 \[OK\]$),
    qr(^ERROR taper no-tape config \[that's enough\]$),
    qr(^PARTIAL taper localhost /music $datestamp 2 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1312\] "that's enough"$),
], "running out of tapes (simulating runtapes=1) logged correctly");

##
# A PORT-WRITE with no disk buffer

$handle = "22-00000";
$datestamp = "20200202222222";
run_taper(4096, "multipart PORT-WRITE");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE worker0 $handle localhost /sbin 0 $datestamp 999999 \"\" 655360 1 \"\" \"\" \"\" \"\" AMANDA");
like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_taper_reply, 63*32768, "localhost", "/sbin", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 640 "\[sec [\d.]+ bytes 655360 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 640 "\[sec [\d.]+ bytes 655360 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 640 "\[sec [\d.]+ bytes 655360 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 4 96 "\[sec [\d.]+ bytes 98304 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("FAILED worker0 $handle");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 2064384 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /sbin $datestamp 1/-1 0 \[sec [\d.]+ bytes 655360 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 2 localhost /sbin $datestamp 2/-1 0 \[sec [\d.]+ bytes 655360 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 3 localhost /sbin $datestamp 3/-1 0 \[sec [\d.]+ bytes 655360 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 4 localhost /sbin $datestamp 4/-1 0 \[sec [\d.]+ bytes 98304 kps [\d.]+\]$),
    qr(^PARTIAL taper localhost /sbin $datestamp 4 0 \[sec [\d.]+ bytes 2064384 kps [\d.]+\]$), # note no error message
    qr(^INFO taper tape TESTCONF01 kb 2016 fm 4 \[OK\]$),
], "DUMPER_STATUS => FAILED logged correctly");

##
# Test a sequence of writes to the same set of tapes

$handle = "33-11111";
$datestamp = "20090101010000";
run_taper(1024, "first in a sequence");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(500000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1412");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1412\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 232 "\[sec [\d.]+ bytes 237856 kps [\d.]+ orig-kb 1412\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 500000 kps [\d.]+ orig-kb 1412\]" "" ""$/,
	"got DONE") or die;
$handle = "33-22222";
make_holding_file(614400, "localhost", "/u02");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u02 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1512");
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 88 "\[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1512\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1512\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1412\]$),
    qr(^PART taper TESTCONF01 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 237856 kps [\d.]+ orig-kb 1412\]$),
    qr(^DONE taper localhost /u01 $datestamp 2 0 \[sec [\d.]+ bytes 500000 kps [\d.]+ orig-kb 1412\]$),
    qr(^PART taper TESTCONF01 3 localhost /u02 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]$),
    qr(^PARTPARTIAL taper TESTCONF01 4 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 98304 kps [\d.]+ orig-kb 1512\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF01 kb 744 fm 4 \[OK\]$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]$),
    qr(^PART taper TESTCONF02 2 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1512\]$),
    qr(^DONE taper localhost /u02 $datestamp 3 0 \[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1512\]$),
    qr(^INFO taper tape TESTCONF02 kb 344 fm 2 \[OK\]$),
], "first taper invocation in sequence logged correctly");
cleanup_log();

$handle = "33-33333";
$datestamp = "20090202020000";
run_taper(1024, "second in a sequence", keep_config => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(300000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF03$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 2 36 "\[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]" "" ""$/,
	"got DONE") or die;
$handle = "33-44444";
make_holding_file(614400, "localhost", "/u02");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u02 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1712");
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 3 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 4 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 88 "\[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 2 with label TESTCONF02 is not reusable$),
    qr(^INFO taper Slot 3 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF03 tape 1$),
    qr(^PART taper TESTCONF03 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF03 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]$),
    qr(^DONE taper localhost /u01 $datestamp 2 0 \[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF03 3 localhost /u02 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PART taper TESTCONF03 4 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PARTPARTIAL taper TESTCONF03 5 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 0 kps [\d.]+ orig-kb 1712\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF03 kb 804 fm 5 \[OK\]$),
    qr(^INFO taper Slot 1 with label TESTCONF01 is usable$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 2$),
    qr(^PART taper TESTCONF01 1 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]$),
    qr(^DONE taper localhost /u02 $datestamp 3 0 \[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]$),
    qr(^INFO taper tape TESTCONF01 kb 88 fm 1 \[OK\]$),
], "second taper invocation in sequence logged correctly");
cleanup_log();

##
# test failure to overwrite a tape label

$handle = "33-55555";
$datestamp = "20090303030000";
run_taper(1024, "failure to overwrite a volume", keep_config => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(32768, "localhost", "/u03");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u03 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1812");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
# we've secretly replaced the tape in slot 1 with a read-only tape.. let's see
# if anyone can tell the difference!
chmod(0555, Installcheck::Run::vtape_dir(2));
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
# NO-NEW-TAPE indicates it did *not* overwrite the tape
like(taper_reply, qr/^NO-NEW-TAPE $handle$/,
	"got proper NO-NEW-TAPE worker0 $handle"); # no "die" here, so we can restore perms
chmod(0755, Installcheck::Run::vtape_dir(2));
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("NO-NEW-TAPE worker0 $handle \"sorry\"");
like(taper_reply, qr/^FAILED $handle INPUT-GOOD TAPE-ERROR "" "?sorry"?.*$/,
	"got FAILED") or die;
taper_cmd("QUIT");
wait_for_exit();

# (logs aren't that interesting here - filled with VFS-specific error messages)

# TODO: simulate an "erased" tape, to which taper should reply with "NEW-TAPE worker0 $handle" and
# immediately REQUEST-NEW-TAPE.  I can't see a way to make the VFS device erase a
# volume without start_device succeeding.

##
# A run with a bogus tapedev/tpchanger
$handle = "44-11111";
$datestamp = "20070102030405";
run_taper(4096, "no tapedev", notapedev => 1);
like(taper_reply, qr/^TAPE-ERROR SETUP "You must specify one of 'tapedev' or 'tpchanger'"$/,
	"got TAPE-ERROR") or die;
wait_for_exit();

##
# A run with 2 workers
my $handle0 = "66-00000";
my $handle1 = "66-11111";
$datestamp = "20090202020000";
run_taper(1024, "with 2 workers", new_vtapes => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("START-TAPER worker1 $datestamp");
like(taper_reply, qr/^TAPER-OK worker1$/,
       "got TAPER-OK") or die;
make_holding_file(300000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle0 \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle0$/,
	"got REQUEST-NEW-TAPE worker0 $handle0") or die;
taper_cmd("START-SCAN worker0 $handle0");
taper_cmd("NEW-TAPE worker0 $handle0");
like(taper_reply, qr/^NEW-TAPE $handle0 TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle0") or die;
like(taper_reply, qr/^PARTDONE $handle0 TESTCONF01 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle0 TESTCONF01 2 36 "\[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^DONE $handle0 INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]" "" ""$/,
	"got DONE") or die;
make_holding_file(614400, "localhost", "/u02");
taper_cmd("FILE-WRITE worker1 $handle1 \"$test_filename\" localhost /u02 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1712");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle1$/,
	"got REQUEST-NEW-TAPE worker1 $handle1") or die;
taper_cmd("START-SCAN worker1 $handle1");
taper_cmd("NEW-TAPE worker1 $handle1");
like(taper_reply, qr/^NEW-TAPE $handle1 TESTCONF02$/,
	"got proper NEW-TAPE worker1 $handle1") or die;
like(taper_reply, qr/^PARTDONE $handle1 TESTCONF02 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle1 TESTCONF02 2 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle1 TESTCONF02 3 88 "\[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 3 on second tape") or die;
like(taper_reply, qr/^DONE $handle1 INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF01 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]$),
    qr(^DONE taper localhost /u01 $datestamp 2 0 \[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /u02 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PART taper TESTCONF02 2 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PART taper TESTCONF02 3 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]$),
    qr(^DONE taper localhost /u02 $datestamp 3 0 \[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]$),
    qr(^INFO taper tape TESTCONF01 kb 292 fm 2 \[OK\]$),
    qr(^INFO taper tape TESTCONF02 kb 600 fm 3 \[OK\]$),
], "two workers logged correctly");
cleanup_log();

##
# A run with 2 workers and a take_scribe
$handle = "66-22222";
$datestamp = "20090202020000";
run_taper(1024, "with 2 workers and a take_scribe", new_vtapes => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("START-TAPER worker1 $datestamp");
like(taper_reply, qr/^TAPER-OK worker1$/,
	"got TAPER-OK") or die;
make_holding_file(1000000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("TAKE-SCRIBE-FROM worker0 $handle worker1");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 208 "\[sec [\d.]+ bytes 213568 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1000000 kps [\d.]+ orig-kb 1612\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF01 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF01 3 localhost /u01 $datestamp 3/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PARTPARTIAL taper TESTCONF01 4 localhost /u01 $datestamp 4/-1 0 \[sec [\d.]+ bytes 98304 kps [\d.]+ orig-kb 1612\] \"No space left on device\"$),
    qr(^INFO taper Will request retry of failed split part.$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 4 \[OK\]$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /u01 $datestamp 4/-1 0 \[sec [\d.]+ bytes 213568 kps [\d.]+ orig-kb 1612\]$),
    qr(^DONE taper localhost /u01 $datestamp 4 0 \[sec [\d.]+ bytes 1000000 kps [\d.]+ orig-kb 1612\]$),
    qr(^INFO taper tape TESTCONF02 kb 208 fm 1 \[OK\]$),
], "TAKE-SCRIBE logged correctly");
cleanup_log();

##
# Test with NDMP device (DirectTCP)

SKIP : {
    skip "not built with NDMP", 33 unless Amanda::Util::built_with_component("ndmp");

    my $ndmp = Installcheck::Mock::NdmpServer->new(tape_limit => 1024*1024);
    my $ndmp_port = $ndmp->{'port'};
    my $drive = $ndmp->{'drive'};

    $handle = "55-11111";
    $datestamp = "19780615010305";
    run_taper(4096, "multipart directtcp PORT-WRITE",
	ndmp_server => $ndmp);
    like(taper_reply, qr/^TAPER-OK worker0$/,
	    "got TAPER-OK") or die;
    # note that, with the addition of the new splitting params, this does the "sensible"
    # thing and uses the tape_splitsize, not the fallback_splitsize (this is a change from
    # Amanda-3.1)
    taper_cmd("PORT-WRITE worker0 $handle localhost /var 0 $datestamp 393216 \"\" 327680 \"\" \"\" \"\" \"\" \"\" DIRECTTCP");
    like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	    "got PORT with data address");
    write_to_port($last_taper_reply, 1230*1024, "localhost", "/var", 0);
    like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	    "got REQUEST-NEW-TAPE worker0 $handle") or die;
    taper_cmd("START-SCAN worker0 $handle");
    taper_cmd("NEW-TAPE worker0 $handle");
    like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	    "got proper NEW-TAPE worker0 $handle") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	    "got PARTDONE for part 1") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	    "got PARTDONE for part 2") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 64 "\[sec [\d.]+ bytes 65536 kps [\d.]+\]"$/,
	    "got PARTDONE for part 3 (short part)") or die;
    like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	    "got REQUEST-NEW-TAPE worker0 $handle") or die;
    taper_cmd("START-SCAN worker0 $handle");
    taper_cmd("NEW-TAPE worker0 $handle");
    like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	    "got proper NEW-TAPE worker0 $handle") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	    "got PARTDONE for part 4") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 32 "\[sec [\d.]+ bytes 32768 kps [\d.]+\]"$/,
	    "got PARTDONE for part 5") or die;
    like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	    "got DUMPER-STATUS request") or die;
    taper_cmd("DONE worker0 $handle 1912");
    like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1277952 kps [\d.]+ orig-kb 1912\]" "" ""$/,
	    "got DONE") or die;
    $handle = "55-22222";
    taper_cmd("PORT-WRITE worker0 $handle localhost /etc 0 $datestamp 524288 \"\" 393216 \"\" \"\" \"\" \"\" \"\" DIRECTTCP");
    like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	    "got PORT with data address");
    write_to_port($last_taper_reply, 300*1024, "localhost", "/etc", 0);
    like(taper_reply, qr/^PARTDONE $handle TESTCONF02 3 320 "\[sec [\d.]+ bytes 327680 kps [\d.]+\]"$/,
	    "got PARTDONE for part 1") or die;
    like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	    "got DUMPER-STATUS request") or die;
    taper_cmd("DONE worker0 $handle 2012");
    like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 327680 kps [\d.]+ orig-kb 2012\]" "" ""$/,
	    "got DONE") or die;
    taper_cmd("QUIT");
    wait_for_exit();

    check_logs([
        qr(^INFO taper Slot 3 without label can be labeled$),
	qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
	qr(^PART taper TESTCONF01 1 localhost /var $datestamp 1/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
	qr(^PART taper TESTCONF01 2 localhost /var $datestamp 2/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
	qr(^PART taper TESTCONF01 3 localhost /var $datestamp 3/-1 0 \[sec [\d.]+ bytes 65536 kps [\d.]+\]$),
	# note no "Will retry.."
	qr(^INFO taper tape TESTCONF01 kb 832 fm 3 \[OK\]$),
        qr(^INFO taper Slot 4 without label can be labeled$),
	qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
	qr(^PART taper TESTCONF02 1 localhost /var $datestamp 4/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
	qr(^PART taper TESTCONF02 2 localhost /var $datestamp 5/-1 0 \[sec [\d.]+ bytes 32768 kps [\d.]+\]$),
	qr(^DONE taper localhost /var $datestamp 5 0 \[sec [\d.]+ bytes 1277952 kps [\d.]+ orig-kb 1912\]$),
	qr(^PART taper TESTCONF02 3 localhost /etc $datestamp 1/-1 0 \[sec [\d.]+ bytes 327680 kps [\d.]+\]$),
	qr(^DONE taper localhost /etc $datestamp 1 0 \[sec [\d.]+ bytes 327680 kps [\d.]+ orig-kb 2012\]$),
	qr(^INFO taper tape TESTCONF02 kb 736 fm 3 \[OK\]$),
    ], "multipart directtcp PORT-WRITE logged correctly");

    $handle = "55-33333";
    $datestamp = "19780615010305";
    run_taper(4096, "multipart directtcp PORT-WRITE, with a zero-byte part",
	ndmp_server => $ndmp);
    like(taper_reply, qr/^TAPER-OK worker0$/,
	    "got TAPER-OK") or die;
    # use a different part size this time, to hit EOM "on the head"
    taper_cmd("PORT-WRITE worker0 $handle localhost /var 0 $datestamp 425984 \"\" 327680 \"\" \"\" \"\" \"\" \"\" DIRECTTCP");
    like(taper_reply, qr/^PORT worker0 $handle (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	    "got PORT with data address");
    write_to_port($last_taper_reply, 1632*1024, "localhost", "/var", 0);
    like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	    "got REQUEST-NEW-TAPE worker0 $handle") or die;
    taper_cmd("START-SCAN worker0 $handle");
    taper_cmd("NEW-TAPE worker0 $handle");
    like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	    "got proper NEW-TAPE worker0 $handle") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 416 "\[sec [\d.]+ bytes 425984 kps [\d.]+\]"$/,
	    "got PARTDONE for part 1") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 416 "\[sec [\d.]+ bytes 425984 kps [\d.]+\]"$/,
	    "got PARTDONE for part 2") or die;
    # note: zero-byte part is not reported as PARTDONE
    like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	    "got REQUEST-NEW-TAPE worker0 $handle") or die;
    taper_cmd("START-SCAN worker0 $handle");
    taper_cmd("NEW-TAPE worker0 $handle");
    like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	    "got proper NEW-TAPE worker0 $handle") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 416 "\[sec [\d.]+ bytes 425984 kps [\d.]+\]"$/,
	    "got PARTDONE for part 3") or die;
    like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 384 "\[sec [\d.]+ bytes 393216 kps [\d.]+\]"$/,
	    "got PARTDONE for part 4") or die;
    like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	    "got DUMPER-STATUS request") or die;
    taper_cmd("DONE worker0 $handle 2112");
    like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1671168 kps [\d.]+ orig-kb 2112\]" "" ""$/,
	    "got DONE") or die;
    taper_cmd("QUIT");
    wait_for_exit();

    check_logs([
        qr(^INFO taper Slot 3 without label can be labeled$),
	qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
	qr(^PART taper TESTCONF01 1 localhost /var $datestamp 1/-1 0 \[sec [\d.]+ bytes 425984 kps [\d.]+\]$),
	qr(^PART taper TESTCONF01 2 localhost /var $datestamp 2/-1 0 \[sec [\d.]+ bytes 425984 kps [\d.]+\]$),
	# Note: zero-byte part is not logged, but is counted in this INFO line's 'fm' field
	qr(^INFO taper tape TESTCONF01 kb 832 fm 3 \[OK\]$),
        qr(^INFO taper Slot 4 without label can be labeled$),
	qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
	qr(^PART taper TESTCONF02 1 localhost /var $datestamp 3/-1 0 \[sec [\d.]+ bytes 425984 kps [\d.]+\]$),
	qr(^PART taper TESTCONF02 2 localhost /var $datestamp 4/-1 0 \[sec [\d.]+ bytes 393216 kps [\d.]+\]$),
	qr(^DONE taper localhost /var $datestamp 4 0 \[sec [\d.]+ bytes 1671168 kps [\d.]+ orig-kb 2112\]$),
	qr(^INFO taper tape TESTCONF02 kb 800 fm 2 \[OK\]$),
    ], "multipart directtcp PORT-WRITE with a zero-byte part logged correctly");
    cleanup_log();

    $ndmp->cleanup();
} # end of ndmp SKIP

##
# A run without LEOM and without allow-split
$handle = "77-11111";
$datestamp = "20090302020000";
run_taper(1024, "without LEOM and without allow-split", new_vtapes => 1);
make_holding_file(1024*1024, "localhost", "/usr");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /usr 0 $datestamp 262144 \"\" \"\" \"0\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTIAL $handle INPUT-GOOD TAPE-ERROR "\[sec [\d.]+ bytes \d* kps [\d.]+ orig-kb 1612\]" "" "No space left on device: more than MAX_VOLUME_USAGE bytes written, splitting not enabled"$/,
	"got PARTIAL for filenum 1") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PARTPARTIAL taper TESTCONF01 1 localhost /usr $datestamp 1/-1 0 \[sec [\d.]+ bytes 983040 kps [\d.]+ orig-kb 1612\] \"No space left on device\"$),
    qr(^PARTIAL taper localhost /usr $datestamp 1 0 \[sec [\d.]+ bytes 0 kps [\d.]+ orig-kb 1612\] "No space left on device: more than MAX_VOLUME_USAGE bytes written, splitting not enabled"$),
    qr(^INFO taper tape TESTCONF01 kb 0 fm 1 \[OK\]$),
], "without LEOM and without allow-split logged correctly");
cleanup_log();

##
# A run with LEOM and without allow-split
$handle = "77-11112";
$datestamp = "20090303020000";
run_taper(1024, "with LEOM and without allow-split", new_vtapes => 1, leom => 1);
make_holding_file(1024*1024, "localhost", "/usr");
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /usr 0 $datestamp 262144 \"\" \"\" \"0\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 864 "\[sec [\d.]+ bytes 884736 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTIAL $handle INPUT-GOOD TAPE-ERROR "\[sec [\d.]+ bytes 884736 kps [\d.]+ orig-kb 1612\]" "" "No space left on device, splitting not enabled"$/,
	"got PARTIAL") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr $datestamp 1/-1 0 \[sec [\d.]+ bytes 884736 kps [\d.]+ orig-kb 1612\]$),
    qr(^PARTIAL taper localhost /usr $datestamp 1 0 \[sec [\d.]+ bytes 884736 kps [\d.]+ orig-kb 1612\] "No space left on device, splitting not enabled"$),
    qr(^INFO taper tape TESTCONF01 kb 864 fm 1 \[OK\]$),
], "with LEOM and without allow-split logged correctly");
cleanup_log();

## test lexical with new changer

##
# A simple, one-part FILE-WRITE
$handle = "11-11111";
$datestamp = "20070102030405";
run_taper(4096, "single-part and multipart FILE-WRITE", taperscan => "lexical", new_vtapes => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(1024*1024, "localhost", "/home");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /home 0 $datestamp \"\" \"\" \"\" \"\" \"\" \"\" \"\" \"\" 12");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 1024 "\[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]"$/,
	"got PARTDONE") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]" "" ""$/,
	"got DONE") or die;

##
# A multipart FILE-WRITE, using the same taper instance
#  (note that the third part is of length 0, and is not logged)

$handle = '11-22222';
make_holding_file(1024*1024, "localhost", "/usr");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /usr 0 $datestamp 524288 \"\" \"\" 1 \"\" \"\" \"\" \"\" 512");
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 512 "\[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 512 "\[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 512\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /home $datestamp 1/-1 0 \[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]$),
    qr(^DONE taper localhost /home $datestamp 1 0 \[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 12\]$),
    qr(^PART taper TESTCONF01 2 localhost /usr $datestamp 1/-1 0 \[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]$),
    qr(^PART taper TESTCONF01 3 localhost /usr $datestamp 2/-1 0 \[sec [\d.]+ bytes 524288 kps [\d.]+ orig-kb 512\]$),
    qr(^DONE taper localhost /usr $datestamp 2 0 \[sec [\d.]+ bytes 1048576 kps [\d.]+ orig-kb 512\]$),
    qr(^INFO taper tape TESTCONF01 kb 2048 fm 4 \[OK\]$),
], "single-part and multi-part dump logged correctly");

##
# Test a sequence of writes to the same set of tapes

$handle = "33-11111";
$datestamp = "20090101010000";
run_taper(1024, "first in a sequence", taperscan => "lexical", new_vtapes => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(500000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1412");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1412\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 232 "\[sec [\d.]+ bytes 237856 kps [\d.]+ orig-kb 1412\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 500000 kps [\d.]+ orig-kb 1412\]" "" ""$/,
	"got DONE") or die;
$handle = "33-22222";
make_holding_file(614400, "localhost", "/u02");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u02 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1512");
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 88 "\[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1512\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1512\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1412\]$),
    qr(^PART taper TESTCONF01 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 237856 kps [\d.]+ orig-kb 1412\]$),
    qr(^DONE taper localhost /u01 $datestamp 2 0 \[sec [\d.]+ bytes 500000 kps [\d.]+ orig-kb 1412\]$),
    qr(^PART taper TESTCONF01 3 localhost /u02 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]$),
    qr(^PARTPARTIAL taper TESTCONF01 4 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 98304 kps [\d.]+ orig-kb 1512\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF01 kb 744 fm 4 \[OK\]$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1512\]$),
    qr(^PART taper TESTCONF02 2 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1512\]$),
    qr(^DONE taper localhost /u02 $datestamp 3 0 \[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1512\]$),
    qr(^INFO taper tape TESTCONF02 kb 344 fm 2 \[OK\]$),
], "first taper invocation in sequence logged correctly");
cleanup_log();

$handle = "33-33333";
$datestamp = "20090202020000";
run_taper(1024, "second in a sequence", keep_config => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
make_holding_file(300000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF03$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 2 36 "\[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]" "" ""$/,
	"got DONE") or die;
$handle = "33-44444";
make_holding_file(614400, "localhost", "/u02");
taper_cmd("FILE-WRITE worker0 $handle \"$test_filename\" localhost /u02 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1712");
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 3 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF03 4 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE worker0 $handle") or die;
taper_cmd("START-SCAN worker0 $handle");
taper_cmd("NEW-TAPE worker0 $handle");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 88 "\[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 3 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF03 tape 1$),
    qr(^PART taper TESTCONF03 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF03 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]$),
    qr(^DONE taper localhost /u01 $datestamp 2 0 \[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF03 3 localhost /u02 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PART taper TESTCONF03 4 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PARTPARTIAL taper TESTCONF03 5 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 0 kps [\d.]+ orig-kb 1712\] "No space left on device"$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper tape TESTCONF03 kb 804 fm 5 \[OK\]$),
    qr(^INFO taper Slot 1 with label TESTCONF01 is usable$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 2$),
    qr(^PART taper TESTCONF01 1 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]$),
    qr(^DONE taper localhost /u02 $datestamp 3 0 \[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]$),
    qr(^INFO taper tape TESTCONF01 kb 88 fm 1 \[OK\]$),
], "second taper invocation in sequence logged correctly");
cleanup_log();

##
# A run with a bogus tapedev/tpchanger
$handle = "44-11111";
$datestamp = "20070102030405";
run_taper(4096, "no tapedev", notapedev => 1, taperscan => "lexical", new_vtapes => 1);
like(taper_reply, qr/^TAPE-ERROR SETUP "You must specify one of 'tapedev' or 'tpchanger'"$/,
	"got TAPE-ERROR") or die;
wait_for_exit();

##
# A run with 2 workers
$handle0 = "66-00000";
$handle1 = "66-11111";
$datestamp = "20090202020000";
run_taper(1024, "with 2 workers", new_vtapes => 1, taperscan => "lexical", new_vtapes => 1);
like(taper_reply, qr/^TAPER-OK worker0$/,
	"got TAPER-OK") or die;
taper_cmd("START-TAPER worker1 $datestamp");
like(taper_reply, qr/^TAPER-OK worker1$/,
       "got TAPER-OK") or die;
make_holding_file(300000, "localhost", "/u01");
taper_cmd("FILE-WRITE worker0 $handle0 \"$test_filename\" localhost /u01 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1612");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle0$/,
	"got REQUEST-NEW-TAPE worker0 $handle0") or die;
taper_cmd("START-SCAN worker0 $handle0");
taper_cmd("NEW-TAPE worker0 $handle0");
like(taper_reply, qr/^NEW-TAPE $handle0 TESTCONF01$/,
	"got proper NEW-TAPE worker0 $handle0") or die;
like(taper_reply, qr/^PARTDONE $handle0 TESTCONF01 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle0 TESTCONF01 2 36 "\[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^DONE $handle0 INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]" "" ""$/,
	"got DONE") or die;
make_holding_file(614400, "localhost", "/u02");
taper_cmd("FILE-WRITE worker1 $handle1 \"$test_filename\" localhost /u02 0 $datestamp 262144 \"\" \"\" \"\" \"\" \"\" \"\" \"\" 1712");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle1$/,
	"got REQUEST-NEW-TAPE worker1 $handle1") or die;
taper_cmd("START-SCAN worker1 $handle1");
taper_cmd("NEW-TAPE worker1 $handle1");
like(taper_reply, qr/^NEW-TAPE $handle1 TESTCONF02$/,
	"got proper NEW-TAPE worker1 $handle1") or die;
like(taper_reply, qr/^PARTDONE $handle1 TESTCONF02 1 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle1 TESTCONF02 2 256 "\[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle1 TESTCONF02 3 88 "\[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]"$/,
	"got PARTDONE for filenum 3 on second tape") or die;
like(taper_reply, qr/^DONE $handle1 INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
wait_for_exit();

check_logs([
    qr(^INFO taper Slot 1 without label can be labeled$),
    qr(^INFO taper Slot 1 is already in use by drive.*$),
    qr(^INFO taper Slot 2 without label can be labeled$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /u01 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1612\]$),
    qr(^PART taper TESTCONF01 2 localhost /u01 $datestamp 2/-1 0 \[sec [\d.]+ bytes 37856 kps [\d.]+ orig-kb 1612\]$),
    qr(^DONE taper localhost /u01 $datestamp 2 0 \[sec [\d.]+ bytes 300000 kps [\d.]+ orig-kb 1612\]$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /u02 $datestamp 1/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PART taper TESTCONF02 2 localhost /u02 $datestamp 2/-1 0 \[sec [\d.]+ bytes 262144 kps [\d.]+ orig-kb 1712\]$),
    qr(^PART taper TESTCONF02 3 localhost /u02 $datestamp 3/-1 0 \[sec [\d.]+ bytes 90112 kps [\d.]+ orig-kb 1712\]$),
    qr(^DONE taper localhost /u02 $datestamp 3 0 \[sec [\d.]+ bytes 614400 kps [\d.]+ orig-kb 1712\]$),
    qr(^INFO taper tape TESTCONF01 kb 292 fm 2 \[OK\]$),
    qr(^INFO taper tape TESTCONF02 kb 600 fm 3 \[OK\]$),
], "two workers logged correctly");
cleanup_log();

cleanup_taper();
