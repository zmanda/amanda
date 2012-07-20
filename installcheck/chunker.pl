# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 60;

use warnings;
use strict;

use lib '@amperldir@';
use Installcheck::Run;
use Installcheck::Mock;
use IO::Handle;
use IPC::Open3;
use Data::Dumper;
use IO::Socket::INET;
use POSIX ":sys_wait_h";
use Cwd qw(abs_path);
use File::Path;

use Amanda::Paths;
use Amanda::Header qw( :constants );
use Amanda::Debug;
use Amanda::Holding;
use Amanda::Util;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $test_hdir = "$Installcheck::TMP/chunker-holding";
my $test_hfile = "$test_hdir/holder";
my $chunker_stderr_file = "$Installcheck::TMP/chunker-stderr";
my $debug = !exists $ENV{'HARNESS_ACTIVE'};

# information on the current run
my ($datestamp, $handle);
my ($chunker_pid, $chunker_in, $chunker_out, $last_chunker_reply, $chunker_reply_timeout);
my $writer_pid;

sub run_chunker {
    my ($description, %params) = @_;

    cleanup_chunker();

    diag("******** $description") if $debug;

    my $testconf = Installcheck::Run::setup();
    $testconf->add_param('debug_chunker', 9);
    $testconf->write();

    if (exists $params{'ENOSPC_at'}) {
	diag("setting CHUNKER_FAKE_ENOSPC_AT=$params{ENOSPC_at}") if $debug;
	$ENV{'CHUNKER_FAKE_ENOSPC_AT'} = $params{'ENOSPC_at'};
    } else {
	delete $ENV{'CHUNKER_FAKE_ENOSPC'};
    }

    open(CHUNKER_ERR, ">", $chunker_stderr_file);
    $chunker_in = $chunker_out = '';
    $chunker_pid = open3($chunker_in, $chunker_out, ">&CHUNKER_ERR",
	"$amlibexecdir/chunker", "TESTCONF");
    close CHUNKER_ERR;
    $chunker_in->blocking(1);
    $chunker_out->autoflush();

    pass("spawned new chunker for 'test $description'");

    # define this to get the installcheck to wait and allow you to attach
    # a gdb instance to the chunker
    if ($params{'use_gdb'}) {
	$chunker_reply_timeout = 0; # no timeouts while debugging
	diag("attach debugger to pid $chunker_pid and press ENTER");
	<>;
    } else {
	$chunker_reply_timeout = 120;
    }

    chunker_cmd("START $datestamp");
}

sub wait_for_exit {
    if ($chunker_pid) {
	waitpid($chunker_pid, 0);
	$chunker_pid = undef;
    }
}

sub cleanup_chunker {
    -d $test_hdir and rmtree($test_hdir);
    mkpath($test_hdir);

    # make a small effort to collect zombies
    if ($chunker_pid) {
	if (waitpid($chunker_pid, WNOHANG) == $chunker_pid) {
	    $chunker_pid = undef;
	}
    }
    if ($writer_pid) {
	if (waitpid($writer_pid, WNOHANG) == $writer_pid) {
	    $writer_pid = undef;
	}
    }
}

sub wait_for_writer {
    if ($writer_pid) {
	if (waitpid($writer_pid, 0) == $writer_pid) {
	    $writer_pid = undef;
	}
    }
}

sub chunker_cmd {
    my ($cmd) = @_;

    diag(">>> $cmd") if $debug;
    print $chunker_in "$cmd\n";
}

sub chunker_reply {
    local $SIG{ALRM} = sub { die "Timeout while waiting for reply\n" };
    alarm($chunker_reply_timeout);
    $last_chunker_reply = $chunker_out->getline();
    alarm(0);

    if (!$last_chunker_reply) {
	die("wrong pid") unless ($chunker_pid == waitpid($chunker_pid, 0));
	my $exit_status = $?;

	open(my $fh, "<", $chunker_stderr_file) or die("open $chunker_stderr_file: $!");
	my $stderr = do { local $/; <$fh> };
	close($fh);

	diag("chunker stderr:\n$stderr") if $stderr;
	die("chunker (pid $chunker_pid) died unexpectedly with status $exit_status");
    }

    # trim trailing whitespace -- C chunker outputs an extra ' ' after
    # single-word replies
    $last_chunker_reply =~ s/\s*$//;
    diag("<<< $last_chunker_reply") if $debug;

    return $last_chunker_reply;
}

sub check_logs {
    my ($expected, $msg) = @_;
    my $re;
    my $line;

    # must contain a pid line at the beginning and end
    unshift @$expected, qr/^INFO chunker chunker pid \d+$/;
    push @$expected, qr/^INFO chunker pid-done \d+$/;

    open(my $logfile, "<", "$CONFIG_DIR/TESTCONF/log/log")
	or die("opening log: $!");
    my @logfile = grep(/^\S+ chunker /, <$logfile>);
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

sub check_holding_chunks {
    my ($filename, $chunks, $host, $disk, $datestamp, $level) = @_;

    my $msg = ".tmp holding chunk files";
    my $exp_nchunks = @$chunks;
    my $nchunks = 0;
    while ($filename) {
	$nchunks++;

	my $filename_tmp = "$filename.tmp";
	if (!-f $filename_tmp) {
	    fail($msg);
	    diag("file $filename_tmp doesn't exist");
	    diag(`ls -1l $test_hdir`);
	    return 0;
	}

	my $fh;
	open($fh, "<", $filename_tmp) or die("opening $filename_tmp: $!");
	my $hdr_str = Amanda::Util::full_read(fileno($fh), Amanda::Holding::DISK_BLOCK_BYTES);
	close($fh);

	my $hdr = Amanda::Header->from_string($hdr_str);
	my $exp_type = ($nchunks == 1)? $F_DUMPFILE : $F_CONT_DUMPFILE;
	if ($hdr->{'type'} != $exp_type) {
	    my ($exp, $got) = (Amanda::Header::filetype_t_to_string($exp_type),
			       Amanda::Header::filetype_t_to_string($hdr->{'type'}));
	    fail($msg);
	    diag("file $filename_tmp has header type $got; expected $exp");
	    return 0;
	}

	my $ok = 1;
	$ok &&= $hdr->{'name'} eq $host;
	$ok &&= $hdr->{'disk'} eq $disk;
	$ok &&= $hdr->{'datestamp'} eq $datestamp;
	$ok &&= $hdr->{'dumplevel'} eq $level;
	if (!$ok) {
	    fail($msg);
	    diag("file $filename_tmp header has unexpected values:\n" . $hdr->summary());
	    return 0;
	}

	my $data_size = (stat($filename_tmp))[7] - Amanda::Holding::DISK_BLOCK_BYTES;
	my $exp_size = (shift @$chunks) * 1024;
	if (defined $exp_size and $exp_size != $data_size) {
	    fail($msg);
	    diag("file $filename_tmp: expected $exp_size bytes, got $data_size");
	    return 0;
	} # note: if @$exp_chunks is empty, the final is() will catch it

	my $last_filename = $filename;
	$filename = $hdr->{'cont_filename'};
	die("header loop!") if $last_filename eq $filename;
    }

    return is($nchunks, $exp_nchunks, $msg);
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
    $hdr = $hdr->to_string(Amanda::Holding::DISK_BLOCK_BYTES,
			   Amanda::Holding::DISK_BLOCK_BYTES);

    $fh->write($hdr);
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

# connect to the given port and write a dumpfile; this *will* create
# zombies, but it's OK -- installchecks aren't daemons.
sub write_to_port {
    my ($port_cmd, $size, $hostname, $disk, $expect_error) = @_;

    my ($header_port, $data_addr) =
	($last_chunker_reply =~ /^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+)/);

    # just run this in the child
    $writer_pid = fork();
    return unless $writer_pid == 0;

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
# A simple, two-chunk PORT-WRITE

$handle = "11-11111";
$datestamp = "20070102030405";
run_chunker("simple");
# note that features (ffff here) and options (ops) are ignored by the chunker
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /boot 0 $datestamp 512 INSTALLCHECK 10240 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 700*1024, "ghost", "/boot", 0);
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 700 "\[sec [\d.]+ kb 700 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker ghost /boot $datestamp 0 \[sec [\d.]+ kb 700 kps [\d.]+\]$),
], "logs correct");

check_holding_chunks($test_hfile, [ 480, 220 ], "ghost", "/boot", $datestamp, 0);

##
# A two-chunk PORT-WRITE that the dumper flags as a failure, but chunker as PARTIAL

$handle = "22-11111";
$datestamp = "20080808080808";
run_chunker("partial");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /root 0 $datestamp 512 INSTALLCHECK 10240 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 768*1024, "ghost", "/root", 0);
wait_for_writer();
chunker_cmd("FAILED $handle");
like(chunker_reply, qr/^PARTIAL $handle 768 "\[sec [\d.]+ kb 768 kps [\d.]+\]"$/,
	"got PARTIAL") or die;
wait_for_exit();

check_logs([
    qr(^PARTIAL chunker ghost /root $datestamp 0 \[sec [\d.]+ kb 768 kps [\d.]+\]$),
], "logs correct");

check_holding_chunks($test_hfile, [ 480, 288 ], "ghost", "/root", $datestamp, 0);

##
# A two-chunk PORT-WRITE that the dumper flags as a failure and chunker
# does too, since no appreciatble bytes were transferred

$handle = "33-11111";
$datestamp = "20070202020202";
run_chunker("failed");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /usr 0 $datestamp 512 INSTALLCHECK 10240 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 0, "ghost", "/usr", 0);
wait_for_writer();
chunker_cmd("FAILED $handle");
like(chunker_reply, qr/^FAILED $handle "\[dumper returned FAILED\]"$/,
	"got FAILED") or die;
wait_for_exit();

check_logs([
    qr(^FAIL chunker ghost /usr $datestamp 0 \[dumper returned FAILED\]$),
], "logs correct");

check_holding_chunks($test_hfile, [ 0 ], "ghost", "/usr", $datestamp, 0);

cleanup_chunker();

##
# A PORT-WRITE with a USE value smaller than the dump size, but an overly large
# chunksize

$handle = "44-11111";
$datestamp = "20040404040404";
run_chunker("more-than-use");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /var 0 $datestamp 10240 INSTALLCHECK 512 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 700*1024, "ghost", "/var", 1);
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile-u2 10240 512");
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 700 "\[sec [\d.]+ kb 700 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker ghost /var $datestamp 0 \[sec [\d.]+ kb 700 kps [\d.]+\]$),
], "logs correct");

check_holding_chunks($test_hfile, [ 480, 220 ], "ghost", "/var", $datestamp, 0);

##
# A PORT-WRITE with a USE value smaller than the dump size, and an even smaller
# chunksize, with a different chunksize on the second holding disk

$handle = "55-11111";
$datestamp = "20050505050505";
run_chunker("more-than-use-and-chunks");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /var 0 $datestamp 96 INSTALLCHECK 160 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 400*1024, "ghost", "/var", 1);
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile-u2 128 10240");
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 400 "\[sec [\d.]+ kb 400 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker ghost /var $datestamp 0 \[sec [\d.]+ kb 400 kps [\d.]+\]$),
], "logs correct");

check_holding_chunks($test_hfile, [ 64, 32, 96, 96, 96, 16 ],
    "ghost", "/var", $datestamp, 0);

cleanup_chunker();

##
# A PORT-WRITE with a USE value smaller than the dump size, but with the CONTINUE
# giving the same filename, so that the dump continues in the same file

$handle = "55-22222";
$datestamp = "20050505050505";
run_chunker("use, continue on same file");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /var/lib 0 $datestamp 10240 INSTALLCHECK 64 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 70*1024, "ghost", "/var/lib", 1);
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile 10240 10240");
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 70 "\[sec [\d.]+ kb 70 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker ghost /var/lib $datestamp 0 \[sec [\d.]+ kb 70 kps [\d.]+\]$),
], "logs correct");

check_holding_chunks($test_hfile, [ 70 ],
    "ghost", "/var/lib", $datestamp, 0);

cleanup_chunker();

##
# A PORT-WRITE with a USE value that will trigger in the midst of a header
# on the second chunk

$handle = "66-11111";
$datestamp = "20060606060606";
run_chunker("out-of-use-during-header");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" ghost ffff /u01 0 $datestamp 96 INSTALLCHECK 120 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 400*1024, "ghost", "/u01", 1);
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile-u2 128 10240");
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 400 "\[sec [\d.]+ kb 400 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker ghost /u01 $datestamp 0 \[sec [\d.]+ kb 400 kps [\d.]+\]$),
], "logs for more-than-use-and-chunks PORT-WRITE");

check_holding_chunks($test_hfile, [ 64, 96, 96, 96, 48 ],
    "ghost", "/u01", $datestamp, 0);

##
# A two-disk PORT-WRITE, but with the DONE sent before the first byte of data
# arrives, to test the ability of the chunker to defer the DONE until it gets
# an EOF

$handle = "77-11111";
$datestamp = "20070707070707";
run_chunker("early-DONE");
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" roast ffff /boot 0 $datestamp 10240 INSTALLCHECK 128 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
chunker_cmd("DONE $handle");
write_to_port($last_chunker_reply, 180*1024, "roast", "/boot", 0);
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile-u2 10240 10240");
wait_for_writer();
like(chunker_reply, qr/^DONE $handle 180 "\[sec [\d.]+ kb 180 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker roast /boot $datestamp 0 \[sec [\d.]+ kb 180 kps [\d.]+\]$),
], "logs for simple PORT-WRITE");

check_holding_chunks($test_hfile, [ 96, 84 ], "roast", "/boot", $datestamp, 0);

##
# A two-disk PORT-WRITE, where the first disk runs out of space before it hits
# the USE limit.

$handle = "88-11111";
$datestamp = "20080808080808";
run_chunker("ENOSPC-1", ENOSPC_at => 90*1024);
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" roast ffff /boot 0 $datestamp 10240 INSTALLCHECK 10240 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 100*1024, "roast", "/boot", 0);
like(chunker_reply, qr/^NO-ROOM $handle 10150$/, # == 10240-90
	"got NO-ROOM") or die;
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile-u2 10240 10240");
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 100 "\[sec [\d.]+ kb 100 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker roast /boot $datestamp 0 \[sec [\d.]+ kb 100 kps [\d.]+\]$),
], "logs for simple PORT-WRITE");

check_holding_chunks($test_hfile, [ 58, 42 ], "roast", "/boot", $datestamp, 0);

##
# A two-chunk PORT-WRITE, where the second chunk gets ENOSPC in the header.  This
# also checks the behavior of rounding down the use value to the nearest multiple
# of 32k (with am_floor)

$handle = "88-22222";
$datestamp = "20080808080808";
run_chunker("ENOSPC-2", ENOSPC_at => 130*1024);
chunker_cmd("PORT-WRITE $handle \"$test_hfile\" roast ffff /boot 0 $datestamp 128 INSTALLCHECK 1000 ops");
like(chunker_reply, qr/^PORT (\d+) "?(\d+\.\d+\.\d+\.\d+:\d+;?)+"?$/,
	"got PORT with data address");
write_to_port($last_chunker_reply, 128*1024, "roast", "/boot", 0);
like(chunker_reply, qr/^NO-ROOM $handle 864$/, # == am_floor(1000)-128
	"got NO-ROOM") or die;
like(chunker_reply, qr/^RQ-MORE-DISK $handle$/,
	"got RQ-MORE-DISK") or die;
chunker_cmd("CONTINUE $handle $test_hfile-u2 300 128");
wait_for_writer();
chunker_cmd("DONE $handle");
like(chunker_reply, qr/^DONE $handle 128 "\[sec [\d.]+ kb 128 kps [\d.]+\]"$/,
	"got DONE") or die;
wait_for_exit();

check_logs([
    qr(^SUCCESS chunker roast /boot $datestamp 0 \[sec [\d.]+ kb 128 kps [\d.]+\]$),
], "logs for simple PORT-WRITE");

check_holding_chunks($test_hfile, [ 96, 32 ], "roast", "/boot", $datestamp, 0);
ok(!-f "$test_hfile.1.tmp",
    "half-written header is deleted");

cleanup_chunker();
