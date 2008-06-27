# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 224;
use File::Path qw( mkpath rmtree );
use Sys::Hostname;
use strict;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Debug;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :getconf :init );
use Amanda::Types;
use Amanda::Paths;
use Amanda::Tests;

my $dev;
my $dev_name;
my ($vtape1, $vtape2);
my ($input_filename, $output_filename) =
    ( "$AMANDA_TMPDIR/input.tmp", "$AMANDA_TMPDIR/output.tmp" );
my $taperoot = "$AMANDA_TMPDIR/Amanda_Device_test_tapes";
my $testconf;
my $queue_fd;

# we'll need some vtapes..
sub mkvtape {
    my ($num) = @_;

    my $mytape = "$taperoot/$num";
    if (-d $mytape) { rmtree($mytape); }
    mkpath("$mytape/data");
    return $mytape;
}


# make up a fake dumpfile_t to write with
my $dumpfile = Amanda::Types::dumpfile_t->new();
$dumpfile->{type} = $Amanda::Types::F_DUMPFILE;
$dumpfile->{datestamp} = "20070102030405";
$dumpfile->{dumplevel} = 0;
$dumpfile->{compressed} = 1;
$dumpfile->{name} = "localhost";
$dumpfile->{disk} = "/home";
$dumpfile->{program} = "INSTALLCHECK";

# function to set up a queue_fd for a filename
sub make_queue_fd {
    my ($filename, $mode) = @_;

    open(my $fd, $mode, $filename) or die("Could not open $filename: $!");
    return $fd, Amanda::Device::queue_fd_t->new(fileno($fd));
}

sub write_file {
    my ($seed, $length, $filenum) = @_;

    Amanda::Tests::write_random_file($seed, $length, $input_filename);

    ok($dev->start_file($dumpfile),
	"start file $filenum")
	or diag($dev->error_or_status());

    is($dev->file(), $filenum,
	"Device has correct filenum");

    my ($input, $queue_fd) = make_queue_fd($input_filename, "<");
    ok($dev->write_from_fd($queue_fd),
	"write some data")
	or diag($dev->error_or_status());
    close($input) or die("Error closing $input_filename");

    if(ok($dev->in_file(),
	"still in_file")) {
	ok($dev->finish_file(),
	    "finish_file")
	    or diag($dev->error_or_status());
    } else {
	pass("not in file, so not calling finish_file");
    }
}

sub verify_file {
    my ($seed, $length, $filenum) = @_;

    ok(my $read_dumpfile = $dev->seek_file($filenum),
	"seek to file $filenum")
	or diag($dev->error_or_status());
    is($dev->file(), $filenum,
	"device is really at file $filenum");
    is($read_dumpfile->{name}, "localhost",
	"header looks vaguely familiar")
	or diag($dev->error_or_status());

    my ($output, $queue_fd) = make_queue_fd($output_filename, ">");
    ok($dev->read_to_fd($queue_fd),
	"read data from file $filenum")
	or diag($dev->error_or_status());
    close($output) or die("Error closing $output_filename");

    ok(Amanda::Tests::verify_random_file($seed, $length, $output_filename, 0),
	"verified file contents");
}

####
## get stuff set up

$testconf = Installcheck::Config->new();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load configuration");

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");

####
## Test errors a little bit

$dev = Amanda::Device->new("foobar:");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of a bogus 'foobar:' device fails");

$dev = Amanda::Device->new("rait:{{");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of a bogus 'rait:{{' device fails");

####
## first, test out the 'null' device.

$dev_name = "null:";

$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "create null device")
    or diag $dev->error_or_status();
ok($dev->start($ACCESS_WRITE, "NULL1", "19780615010203"),
    "start null device in write mode")
    or diag $dev->error_or_status();

# check some info methods
isnt($dev->write_min_size(), 0,
    "write_min_size > 0 on null device");
isnt($dev->write_max_size(), 0,
    "write_max_size > 0 on null device");
isnt($dev->read_max_size(), 0,
    "read_max_size > 0 on null device");

# try properties
my $plist = $dev->property_list();
ok($plist,
    "got some properties on null device");
is($dev->property_get("canonical_name"), $dev_name,
    "property_get on null device");

# and write a file to it
write_file(0xabcde, 1024*256, 1);

# (don't finish the device, testing the finalize method's cleanup)

####
## Now some full device tests

## VFS device

$vtape1 = mkvtape(1);
$dev_name = "file:$vtape1";

$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: create successful")
    or diag($dev->error_or_status());

$dev->read_label();
ok($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED,
    "initially unlabeled")
    or diag($dev->error_or_status());

ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
    "start in write mode")
    or diag($dev->error_or_status());

ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
    "not unlabeled anymore")
    or diag($dev->error_or_status());

for (my $i = 1; $i <= 3; $i++) {
    write_file(0x2FACE, $dev->write_min_size()*10+17, $i);
}

ok($dev->finish(),
    "finish device after write")
    or diag($dev->error_or_status());

$dev->read_label();
ok(!($dev->status()),
    "no error, at all, from read_label")
    or diag($dev->error_or_status());

# append one more copy, to test ACCESS_APPEND

ok($dev->start($ACCESS_APPEND, undef, undef),
    "start in append mode")
    or diag($dev->error_or_status());

write_file(0xD0ED0E, $dev->write_min_size()*4, 4);

ok($dev->finish(),
    "finish device after append")
    or diag($dev->error_or_status());

# try reading the third file back

ok($dev->start($ACCESS_READ, undef, undef),
    "start in read mode")
    or diag($dev->error_or_status());

verify_file(0x2FACE, $dev->write_min_size()*10+17, 3);

ok($dev->finish(),
    "finish device after read")
    or diag($dev->error_or_status());

####
## Test a RAIT device of two vfs devices.

($vtape1, $vtape2) = (mkvtape(1), mkvtape(2));
$dev_name = "rait:{file:$vtape1,file:$vtape2}";

$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
   "$dev_name: create successful")
    or diag($dev->error_or_status());

$dev->read_label();
ok($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED,
   "initially unlabeled")
    or diag($dev->error_or_status());

ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
   "start in write mode")
    or diag($dev->error_or_status());

ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
   "not unlabeled anymore")
    or diag($dev->error_or_status());

for (my $i = 1; $i <= 3; $i++) {
    write_file(0x2FACE, $dev->write_max_size()*10+17, $i);
}

ok($dev->finish(),
   "finish device after write")
    or diag($dev->error_or_status());

$dev->read_label();
ok(!($dev->status()),
   "no error, at all, from read_label")
    or diag($dev->error_or_status());

# append one more copy, to test ACCESS_APPEND

ok($dev->start($ACCESS_APPEND, undef, undef),
   "start in append mode")
    or diag($dev->error_or_status());

write_file(0xD0ED0E, $dev->write_max_size()*4, 4);

ok($dev->finish(),
   "finish device after append")
    or diag($dev->error_or_status());

# try reading the third file back

ok($dev->start($ACCESS_READ, undef, undef),
   "start in read mode")
    or diag($dev->error_or_status());

verify_file(0x2FACE, $dev->write_max_size()*10+17, 3);

ok($dev->finish(),
   "finish device after read")
    or diag($dev->error_or_status());

TODO: {
    todo_skip "RAIT device doesn't actually support this yet", 23;

    # corrupt the device somehow and hope it keeps working
    mkvtape(2);

    ok($dev->start($ACCESS_READ, undef, undef),
	"start in read mode after device corruption")
	or diag($dev->error_or_status());

    verify_file(0x2FACE, $dev->write_max_size()*10+17, 3);
    verify_file(0xD0ED0E, $dev->write_max_size()*4, 4);
    verify_file(0x2FACE, $dev->write_max_size()*10+17, 2);

    ok($dev->finish(),
	"finish device read after device corruption")
	or diag($dev->error_or_status());

    ok($dev->start($ACCESS_WRITE, "TESTCONF29", undef),
	"start in write mode after device corruption")
	or diag($dev->error_or_status());

    write_file(0x2FACE, $dev->write_max_size()*20+17, 1);

    ok($dev->finish(),
	"finish device write after device corruption")
	or diag($dev->error_or_status());
}

undef $dev;

# Make two devices with different labels, should get a
# message accordingly.
($vtape1, $vtape2) = (mkvtape(1), mkvtape(2));
my $rait_name = "rait:{file:$vtape1,file:$vtape2}";

my $dev1 = Amanda::Device->new($vtape1);
is($dev1->status(), $DEVICE_STATUS_SUCCESS,
   "$vtape1: Open successful")
    or diag($dev->error_or_status());
$dev1->start($ACCESS_WRITE, "TESTCONF13", undef);
$dev1->finish();
my $dev2 = Amanda::Device->new($vtape2);
is($dev2->status(), $DEVICE_STATUS_SUCCESS,
   "$vtape2: Open successful")
    or diag($dev->error_or_status());
$dev2->start($ACCESS_WRITE, "TESTCONF14", undef);
$dev2->finish();

$dev1 = $dev2 = undef;

$dev = Amanda::Device->new($rait_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
   "$rait_name: Open successful")
    or diag($dev->error_or_status());

$dev->read_label();
ok($dev->status() & $DEVICE_STATUS_DEVICE_ERROR,
   "Label mismatch error")
    or diag($dev->error_or_status());

# Test an S3 device if the proper environment variables are set
my $S3_SECRET_KEY = $ENV{'INSTALLCHECK_S3_SECRET_KEY'};
my $S3_ACCESS_KEY = $ENV{'INSTALLCHECK_S3_ACCESS_KEY'};
my $run_s3_tests = defined $S3_SECRET_KEY && defined $S3_ACCESS_KEY;
SKIP: {
    skip "define \$INSTALLCHECK_S3_{SECRET,ACCESS}_KEY to run S3 tests", 77
	unless $run_s3_tests;

    # XXX for best results, the bucket should already exist (Amazon doesn't create
    # buckets quickly enough to pass subsequent tests), but should be empty (so that
    # the device appears unlabeled)
    my $hostname = hostname();
    $dev_name = "s3:$S3_ACCESS_KEY-installcheck-$hostname";

    # (note: we don't use write_max_size here, as the maximum for S3 is very large)
    # run once with an unconstrained bucket, then again with a constrained one
    for my $s3_loc (undef, "EU") {
        $dev_name = lc($dev_name) if $s3_loc;
        $dev = Amanda::Device->new($dev_name);
        is($dev->status(), $DEVICE_STATUS_SUCCESS,
           "$dev_name: create successful")
            or diag($dev->error_or_status());

        ok($dev->property_set('S3_ACCESS_KEY', $S3_ACCESS_KEY),
           "set S3 access key")
            or diag($dev->error_or_status());

        ok($dev->property_set('S3_SECRET_KEY', $S3_SECRET_KEY),
           "set S3 secret key")
            or diag($dev->error_or_status());

        ok($dev->property_set('S3_BUCKET_LOCATION', $s3_loc),
           "set S3 bucket location")
            or diag($dev->error_or_status()) if $s3_loc;


        $dev->read_label();
        is($dev->status() & ~$DEVICE_STATUS_VOLUME_UNLABELED, 0,
           "read_label OK, possibly already labeled")
            or diag($dev->error_or_status());

        ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
           "start in write mode")
            or diag($dev->error_or_status());

        ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
           "it's labeled now")
            or diag($dev->error_or_status());

        for (my $i = 1; $i <= 3; $i++) {
            write_file(0x2FACE, 32768*10, $i);
        }

        ok($dev->finish(),
           "finish device after write")
            or diag($dev->error_or_status());

        $dev->read_label();
        ok(!($dev->status()),
           "no error, at all, from read_label")
	or diag($dev->error_or_status());

        # append one more copy, to test ACCESS_APPEND

        ok($dev->start($ACCESS_APPEND, undef, undef),
           "start in append mode")
            or diag($dev->error_or_status());

        write_file(0xD0ED0E, 32768*10, 4);

        ok($dev->finish(),
           "finish device after append")
            or diag($dev->error_or_status());

        # try reading the third file back

        ok($dev->start($ACCESS_READ, undef, undef),
           "start in read mode")
            or diag($dev->error_or_status());

        verify_file(0x2FACE, 32768*10, 3);

        ok($dev->finish(),
           "finish device after read")
            or diag($dev->error_or_status());
    }

    $dev_name = "-$dev_name";
    $dev = Amanda::Device->new($dev_name);

    is($dev->status(), $DEVICE_STATUS_SUCCESS,
       "$dev_name: create successful")
        or diag($dev->error_or_status());

    ok(!$dev->property_set('S3_BUCKET_LOCATION', $dev_name),
       "should not be able to set S3 bucket location with an incompatible name")
        or diag($dev->error_or_status());

}

# Test a tape device if the proper environment variables are set
my $TAPE_DEVICE = $ENV{'INSTALLCHECK_TAPE_DEVICE'};
my $run_tape_tests = defined $TAPE_DEVICE;
SKIP: {
    skip "define \$INSTALLCHECK_TAPE_DEVICE to run tape tests", 36
	unless $run_tape_tests;

    $dev_name = "tape:$TAPE_DEVICE";
    $dev = Amanda::Device->new($dev_name);
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"$dev_name: create successful")
	or diag($dev->error_or_status());

    $dev->read_label();
    ok(!($dev->status() & ~$DEVICE_STATUS_VOLUME_UNLABELED),
	"no error, except possibly unlabeled, from read_label")
	or diag($dev->error_or_status());

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
	"start in write mode")
	or diag($dev->error_or_status());

    ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
	"not unlabeled anymore")
	or diag($dev->error_or_status());

    for (my $i = 1; $i <= 3; $i++) {
	write_file(0x2FACE, $dev->write_min_size()*10+17, $i);
    }

    ok($dev->finish(),
	"finish device after write")
	or diag($dev->error_or_status());

    $dev->read_label();
    ok(!($dev->status()),
	"no error, at all, from read_label")
	or diag($dev->error_or_status());

    is($dev->volume_label(), "TESTCONF13",
	"read_label reads the correct label")
	or diag($dev->error_or_status());

    # append one more copy, to test ACCESS_APPEND

    ok($dev->start($ACCESS_APPEND, undef, undef),
	"start in append mode")
	or diag($dev->error_or_status());

    write_file(0xD0ED0E, $dev->write_min_size()*4, 4);

    ok($dev->finish(),
	"finish device after append")
	or diag($dev->error_or_status());

    # try reading the third file back

    ok($dev->start($ACCESS_READ, undef, undef),
	"start in read mode")
	or diag($dev->error_or_status());

    verify_file(0x2FACE, $dev->write_min_size()*10+17, 3);

    ok($dev->finish(),
	"finish device after read")
	or diag($dev->error_or_status());

}

unlink($input_filename);
unlink($output_filename);
rmtree($taperoot);
