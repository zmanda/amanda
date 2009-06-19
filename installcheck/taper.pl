# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 103;

use lib '@amperldir@';
use Installcheck::Run;
use IO::Handle;
use IPC::Open3;
use IO::Socket::INET;
use POSIX ":sys_wait_h";

use Amanda::Paths;
use Amanda::Header;
use Amanda::Debug;
use Amanda::Paths;

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

my $test_filename = "$Installcheck::TMP/installcheck-taper-holding-file";
my $taper_stderr_file = "$Installcheck::TMP/taper-stderr";
my $debug = !exists $ENV{'HARNESS_ACTIVE'};

# information on the current run
my @results;
my $port;
my ($datestamp, $handle);
my ($taper_pid, $taper_in, $taper_out, $last_taper_reply);

sub run_taper {
    (my $length, my $description) = @_;

    # clear any previous run
    @results = ();
    cleanup_taper();
    if ($taper_pid) {
	# make a small effort to collect zombies
	waitpid($taper_pid, WNOHANG);
	$taper_pid = undef;
    }

    my $testconf = Installcheck::Run::setup();
    $testconf->add_param('label_new_tapes', '"TESTCONF%%"');
    $testconf->add_tapetype('TEST-TAPE', [
	'length' =>  "$length",
	]);
    $testconf->write();

    open(TAPER_ERR, ">", $taper_stderr_file);
    $taper_in = $taper_out = '';
    #$taper_pid = open3($taper_in, $taper_out, $taper_err,
    $taper_pid = open3($taper_in, $taper_out, ">&TAPER_ERR",
	"$amlibexecdir/taper", "TESTCONF");
    $taper_in->blocking(1);
    $taper_out->autoflush();
    close TAPER_ERR;

    pass("spawned taper for $description (tape length $length kb)");

    taper_cmd("START-TAPER $datestamp");
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
}

sub taper_cmd {
    my ($cmd) = @_;

    diag(">>> $cmd") if $debug;
    print $taper_in "$cmd\n";
}

sub taper_reply {
    local $SIG{ALRM} = sub { die "Timeout while waiting for reply\n" };
    alarm(120);
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
	$logline = shift @logfile;
	$expline = shift @$expected;
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

# functions to create dumpfiles

sub write_dumpfile_to {
    my ($fh, $size, $hostname, $disk, $expect_failure) = @_;

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = $datestamp;
    $hdr->{'dumplevel'} = 0;
    $hdr->{'compressed'} = 1;
    $hdr->{'name'} = $hostname;
    $hdr->{'disk'} = $disk;
    $hdr->{'program'} = "INSTALLCHECK";
    $hdr = $hdr->to_string(32768,32768);

    $fh->write($hdr);

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
    write_dumpfile_to($fh, $size, $hostname, $disk);
}

# connect to the given port and write a dumpfile; this *will* create
# zombies, but it's OK -- installchecks aren't daemons.
sub write_to_port {
    my ($port, $size, $hostname, $disk, $expect_error) = @_;

    # just run this in the child
    return unless fork() == 0;

    my $sock = IO::Socket::INET->new(
	PeerAddr => "127.0.0.1:$port",
	Proto => "tcp",
	ReuseAddr => 1,
    );

    write_dumpfile_to($sock, $size, $hostname, $disk, $expect_error);
    exit;
}

########

##
# A simple, one-part FILE-WRITE

$handle = "11-11111";
$datestamp = "20070102030405";
run_taper(4096, "single-part and multipart FILE-WRITE");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
make_holding_file(1024*1024, "localhost", "/home");
taper_cmd("FILE-WRITE $handle \"$test_filename\" localhost /home 0 $datestamp 0");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 1024 "\[sec [\d.]+ kb 1024 kps [\d.]+\]"$/,
	"got PARTDONE") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 1024 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;

##
# A multipart FILE-WRITE, using the same taper instance
#  (note that the third part is of length 0)

$handle = '11-22222';
make_holding_file(1024*1024, "localhost", "/usr");
taper_cmd("FILE-WRITE $handle \"$test_filename\" localhost /usr 0 $datestamp 524288");
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 512 "\[sec [\d.]+ kb 512 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 512 "\[sec [\d.]+ kb 512 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 4 0 "\[sec [\d.]+ kb 0 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 1024 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /home $datestamp 1/1 0 \[sec [\d.]+ kb 1024 kps [\d.]+\]$),
    qr(^DONE taper localhost /home $datestamp 1 0 \[sec [\d.]+ kb 1024 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 2 localhost /usr $datestamp 1/3 0 \[sec [\d.]+ kb 512 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 3 localhost /usr $datestamp 2/3 0 \[sec [\d.]+ kb 512 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 4 localhost /usr $datestamp 3/3 0 \[sec [\d.]+ kb 0 kps [\d.]+\]$),
    qr(^DONE taper localhost /usr $datestamp 3 0 \[sec [\d.]+ kb 1024 kps [\d.]+\]$),
    qr(^INFO taper tape TESTCONF01 kb 2048 fm 4 \[OK\]$),
], "single-part and multi-part dump logged correctly");

##
# A PORT-WRITE with no disk buffer

$handle = "11-33333";
$datestamp = "19780615010203";
run_taper(4096, "multipart PORT-WRITE");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE $handle localhost /var 0 $datestamp 524288 NULL 393216");
like(taper_reply, qr/^PORT (\d+)$/,
	"got PORT");
($port) = ($last_taper_reply =~ /^PORT (\d+)/);
write_to_port($port, 63*32768, "localhost", "/var", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 384 "\[sec [\d.]+ kb 384 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 384 "\[sec [\d.]+ kb 384 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 384 "\[sec [\d.]+ kb 384 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 4 384 "\[sec [\d.]+ kb 384 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 5 384 "\[sec [\d.]+ kb 384 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 5") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 6 96 "\[sec [\d.]+ kb 96 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 6") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE $handle");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 2016 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /var $datestamp 1/-1 0 \[sec [\d.]+ kb 384 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 2 localhost /var $datestamp 2/-1 0 \[sec [\d.]+ kb 384 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 3 localhost /var $datestamp 3/-1 0 \[sec [\d.]+ kb 384 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 4 localhost /var $datestamp 4/-1 0 \[sec [\d.]+ kb 384 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 5 localhost /var $datestamp 5/-1 0 \[sec [\d.]+ kb 384 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 6 localhost /var $datestamp 6/-1 0 \[sec [\d.]+ kb 96 kps [\d.]+\]$),
    qr(^DONE taper localhost /var $datestamp 6 0 \[sec [\d.]+ kb 2016 kps [\d.]+\]$),
    qr(^INFO taper tape TESTCONF01 kb 2016 fm 6 \[OK\]$),
], "multipart PORT-WRITE logged correctly");

##
# Test NO-NEW-TAPE

$handle = "11-44444";
$datestamp = "19411207000000";
run_taper(4096, "testing NO-NEW-TAPE from the driver on 1st request");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
make_holding_file(1024*1024, "localhost", "/home");
taper_cmd("FILE-WRITE $handle \"$test_filename\" localhost /home 0 $datestamp 0");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NO-NEW-TAPE sorry");
like(taper_reply, qr/^FAILED $handle INPUT-GOOD TAPE-ERROR "" "?sorry"?.*$/,
	"got FAILED") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^ERROR taper no-tape \[sorry\]$),
    qr(^FAIL taper localhost /home $datestamp 0 sorry$),
], "NO-NEW-TAPE logged correctly");

##
# Test retrying on EOT (via PORT-WRITE with a mem cache)

$handle = "11-55555";
$datestamp = "19750711095836";
run_taper(1024, "PORT-WRITE retry on EOT (mem cache)");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE $handle localhost /usr/local 0 $datestamp 786432 NULL 786432");
like(taper_reply, qr/^PORT (\d+)$/,
	"got PORT");
($port) = ($last_taper_reply =~ /^PORT (\d+)/);
write_to_port($port, 1575936, "localhost", "/usr/local", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 3 "\[sec [\d.]+ kb 3 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE $handle");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 1539 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr/local $datestamp 1/-1 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ kb 160 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 2 \[OK\]$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper Will write new label `TESTCONF02' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
    qr(^PART taper TESTCONF02 2 localhost /usr/local $datestamp 3/-1 0 \[sec [\d.]+ kb 3 kps [\d.]+\]$),
    qr(^DONE taper localhost /usr/local $datestamp 3 0 \[sec [\d.]+ kb 1539 kps [\d.]+\]$),
    qr(^INFO taper tape TESTCONF02 kb 771 fm 2 \[OK\]$),
], "multivolume PORT-WRITE logged correctly");

##
# Test retrying on EOT (via FILE-WRITE)

$handle = "11-66666";
$datestamp = "19470815000000";
run_taper(1024, "FILE-WRITE retry on EOT");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
make_holding_file(1575936, "localhost", "/usr");
taper_cmd("FILE-WRITE $handle \"$test_filename\" localhost /usr 0 $datestamp 786432");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 3 "\[sec [\d.]+ kb 3 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 1539 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr $datestamp 1/3 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /usr $datestamp 2/3 0 \[sec [\d.]+ kb 160 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 2 \[OK\]$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper Will write new label `TESTCONF02' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /usr $datestamp 2/3 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
    qr(^PART taper TESTCONF02 2 localhost /usr $datestamp 3/3 0 \[sec [\d.]+ kb 3 kps [\d.]+\]$),
    qr(^DONE taper localhost /usr $datestamp 3 0 \[sec [\d.]+ kb 1539 kps [\d.]+\]$),
    qr(^INFO taper tape TESTCONF02 kb 771 fm 2 \[OK\]$),
], "multivolume FILE-WRITE logged correctly");

##
# Test retrying on EOT (via PORT-WRITE with a disk cache)

$handle = "11-77777";
$datestamp = "20090427212500";
run_taper(1024, "PORT-WRITE retry on EOT (disk cache)");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE $handle localhost /usr/local 0 $datestamp 786432 \"$Installcheck::TMP\" 786432");
like(taper_reply, qr/^PORT (\d+)$/,
	"got PORT");
($port) = ($last_taper_reply =~ /^PORT (\d+)/);
write_to_port($port, 1575936, "localhost", "/usr/local", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF02$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 1 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on second tape") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF02 2 3 "\[sec [\d.]+ kb 3 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2 on second tape") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("DONE $handle");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 1539 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /usr/local $datestamp 1/-1 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ kb 160 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper tape TESTCONF01 kb 768 fm 2 \[OK\]$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^INFO taper Will write new label `TESTCONF02' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF02 tape 2$),
    qr(^PART taper TESTCONF02 1 localhost /usr/local $datestamp 2/-1 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
    qr(^PART taper TESTCONF02 2 localhost /usr/local $datestamp 3/-1 0 \[sec [\d.]+ kb 3 kps [\d.]+\]$),
    qr(^DONE taper localhost /usr/local $datestamp 3 0 \[sec [\d.]+ kb 1539 kps [\d.]+\]$),
    qr(^INFO taper tape TESTCONF02 kb 771 fm 2 \[OK\]$),
], "multivolume PORT-WRITE (disk cache) logged correctly");

##
# Test failure on EOT (via PORT-WRITE with no cache)

$handle = "11-88888";
$datestamp = "20090424173000";
run_taper(1024, "PORT-WRITE failure on EOT (no cache)");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE $handle localhost /var/log 0 $datestamp 0 NULL 0");
like(taper_reply, qr/^PORT (\d+)$/,
	"got PORT");
($port) = ($last_taper_reply =~ /^PORT (\d+)/);
write_to_port($port, 1575936, "localhost", "/var/log", 1);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTIAL $handle INPUT-GOOD TAPE-ERROR "\[sec [\d.]+ kb 0 kps [\d.]+\]" "" "No space left on device"$/,
	"got PARTIAL") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PARTPARTIAL taper TESTCONF01 1 localhost /var/log $datestamp 1/1 0 \[sec [\d.]+ kb 960 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper tape TESTCONF01 kb 0 fm 1 \[OK\]$),
    qr(^PARTIAL taper localhost /var/log $datestamp 1 0 \[sec [\d.]+ kb 0 kps [\d.]+\] "No space left on device"$),
], "failure on EOT (no cache) logged correctly");

##
# Test running out of tapes (second REQUEST-NEW-TAPE fails)

$handle = "11-99999";
$datestamp = "20100101000000";
run_taper(512, "FILE-WRITE runs out of tapes");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
make_holding_file(512*1024, "localhost", "/music");
taper_cmd("FILE-WRITE $handle \"$test_filename\" localhost /music 0 $datestamp 262144");
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 256 "\[sec [\d.]+ kb 256 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1 on first tape") or die;
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NO-NEW-TAPE \"that's enough\"");
like(taper_reply, qr/^PARTIAL $handle INPUT-GOOD TAPE-ERROR "\[sec [\d.]+ kb 256 kps [\d.]+\]" "" "No space left on device"$/,
	"got PARTIAL") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /music $datestamp 1/3 0 \[sec [\d.]+ kb 256 kps [\d.]+\]$),
    qr(^PARTPARTIAL taper TESTCONF01 2 localhost /music $datestamp 2/3 0 \[sec [\d.]+ kb 160 kps [\d.]+\] "No space left on device"$),
    qr(^INFO taper tape TESTCONF01 kb 256 fm 2 \[OK\]$),
    qr(^INFO taper Will request retry of failed split part\.$),
    qr(^ERROR taper no-tape \[that's enough\]$),
    qr(^PARTIAL taper localhost /music $datestamp 2 0 \[sec [\d.]+ kb 256 kps [\d.]+\] "No space left on device"$),
], "running out of tapes (simulating runtapes=1) logged correctly");

##
# A PORT-WRITE with no disk buffer

$handle = "22-00000";
$datestamp = "20200202222222";
run_taper(4096, "multipart PORT-WRITE");
like(taper_reply, qr/^TAPER-OK$/,
	"got TAPER-OK") or die;
taper_cmd("PORT-WRITE $handle localhost /sbin 0 $datestamp 10 NULL 655360");
like(taper_reply, qr/^PORT (\d+)$/,
	"got PORT");
($port) = ($last_taper_reply =~ /^PORT (\d+)/);
write_to_port($port, 63*32768, "localhost", "/sbin", 0);
like(taper_reply, qr/^REQUEST-NEW-TAPE $handle$/,
	"got REQUEST-NEW-TAPE") or die;
taper_cmd("NEW-TAPE");
like(taper_reply, qr/^NEW-TAPE $handle TESTCONF01$/,
	"got proper NEW-TAPE") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 1 640 "\[sec [\d.]+ kb 640 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 1") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 2 640 "\[sec [\d.]+ kb 640 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 2") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 3 640 "\[sec [\d.]+ kb 640 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 3") or die;
like(taper_reply, qr/^PARTDONE $handle TESTCONF01 4 96 "\[sec [\d.]+ kb 96 kps [\d.]+\]"$/,
	"got PARTDONE for filenum 4") or die;
like(taper_reply, qr/^DUMPER-STATUS $handle$/,
	"got DUMPER-STATUS request") or die;
taper_cmd("FAILED $handle");
like(taper_reply, qr/^DONE $handle INPUT-GOOD TAPE-GOOD "\[sec [\d.]+ kb 2016 kps [\d.]+\]" "" ""$/,
	"got DONE") or die;
taper_cmd("QUIT");
like(taper_reply, qr/^QUITTING$/,
	"got QUITTING") or die;
wait_for_exit();

check_logs([
    qr(^INFO taper Will write new label `TESTCONF01' to new tape$),
    qr(^START taper datestamp $datestamp label TESTCONF01 tape 1$),
    qr(^PART taper TESTCONF01 1 localhost /sbin $datestamp 1/-1 0 \[sec [\d.]+ kb 640 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 2 localhost /sbin $datestamp 2/-1 0 \[sec [\d.]+ kb 640 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 3 localhost /sbin $datestamp 3/-1 0 \[sec [\d.]+ kb 640 kps [\d.]+\]$),
    qr(^PART taper TESTCONF01 4 localhost /sbin $datestamp 4/-1 0 \[sec [\d.]+ kb 96 kps [\d.]+\]$),
    qr(^PARTIAL taper localhost /sbin $datestamp 4 0 \[sec [\d.]+ kb 2016 kps [\d.]+\]$), # note no error message
    qr(^INFO taper tape TESTCONF01 kb 2016 fm 4 \[OK\]$),
], "DUMPER_STATUS => FAILED logged correctly");

cleanup_taper();
