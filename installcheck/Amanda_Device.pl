# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 609;
use File::Path qw( mkpath rmtree );
use Sys::Hostname;
use Carp;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Mock;
use Installcheck::Config;
use Amanda::Debug;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :getconf :init );
use Amanda::Xfer qw( :constants );
use Amanda::Header qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Util;
use Amanda::MainLoop;
use IO::Socket;

my $dev;
my $dev_name;
my ($vtape1, $vtape2);
my ($input_filename, $output_filename) =
    ( "$Installcheck::TMP/input.tmp", "$Installcheck::TMP/output.tmp" );
my $taperoot = "$Installcheck::TMP/Amanda_Device_test_tapes";
my $testconf;

# we'll need some vtapes..
sub mkvtape {
    my ($num) = @_;

    my $mytape = "$taperoot/$num";
    if (-d $mytape) { rmtree($mytape); }
    mkpath("$mytape/data");
    return $mytape;
}


# make up a fake dumpfile_t to write with
my $dumpfile = Amanda::Header->new();
$dumpfile->{type} = $Amanda::Header::F_DUMPFILE;
$dumpfile->{datestamp} = "20070102030405";
$dumpfile->{dumplevel} = 0;
$dumpfile->{compressed} = 1;
$dumpfile->{name} = "localhost";
$dumpfile->{disk} = "/home";
$dumpfile->{program} = "INSTALLCHECK";

my $write_file_count = 5;
sub write_file {
    my ($seed, $length, $filenum) = @_;

    $dumpfile->{'datestamp'} = "2000010101010$filenum";

    ok($dev->start_file($dumpfile),
	"start file $filenum")
	or diag($dev->error_or_status());

    is($dev->file(), $filenum,
	"Device has correct filenum");

    croak ("selected file size $length is *way* too big")
	unless ($length < 1024*1024*10);
    ok(Amanda::Device::write_random_to_device($seed, $length, $dev),
	"write random data");

    if(ok($dev->in_file(),
	"still in_file")) {
	ok($dev->finish_file(),
	    "finish_file")
	    or diag($dev->error_or_status());
    } else {
	pass("not in file, so not calling finish_file");
    }
}

my $verify_file_count = 4;
sub verify_file {
    my ($seed, $length, $filenum) = @_;

    ok(my $read_dumpfile = $dev->seek_file($filenum),
	"seek to file $filenum")
	or diag($dev->error_or_status());
    is($dev->file(), $filenum,
	"device is really at file $filenum");
    ok(header_for($read_dumpfile, $filenum),
	"header is correct")
	or diag($dev->error_or_status());
    ok(Amanda::Device::verify_random_from_device($seed, $length, $dev),
	"verified file contents");
}

sub header_for {
    my ($hdr, $filenum) = @_;
    return ($hdr and $hdr->{'datestamp'} eq "2000010101010$filenum");
}

# properties test

my @common_properties = (
    'appendable',
    'block_size',
    'canonical_name',
    'concurrency',
    'max_block_size',
    'medium_access_type',
    'min_block_size',
    'partial_deletion',
    'full_deletion',
    'streaming',
);

sub properties_include {
    my ($got, $should_include, $msg) = @_;
    my %got = map { $_->{'name'}, 1 } @$got;
    my @missing = grep { !defined($got{$_}) } @$should_include;
    if (@missing) {
	fail($msg);
	diag(" Expected properties: " . join(", ", @$should_include));
	diag("      Got properties: " . join(", ", @$got));
	diag("  Missing properties: " . join(", ", @missing));
    } else {
	pass($msg);
    }
}

####
## get stuff set up

$testconf = Installcheck::Config->new();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load configuration");

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

####
## Test errors a little bit

$dev = Amanda::Device->new("foobar:");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of a bogus 'foobar:' device fails");

$dev = Amanda::Device->new("rait:{{");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of a bogus 'rait:{{' device fails");

$dev = Amanda::Device->new("rait:{a,b");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of a bogus 'rait:{a,b' device fails");

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

# try properties
properties_include([ $dev->property_list() ], [ @common_properties ],
    "necessary properties listed on null device");
is($dev->property_get("canonical_name"), "null:",
    "property_get(canonical_name) on null device");
is($dev->property_get("caNONical-name"), "null:",
    "property_get(caNONical-name) on null device (case, dash-insensitivity)");
is_deeply([ $dev->property_get("canonical_name") ],
    [ "null:", $PROPERTY_SURETY_GOOD, $PROPERTY_SOURCE_DEFAULT ],
    "extended property_get returns correct surety/source");
for my $prop ($dev->property_list()) {
    next unless $prop->{'name'} eq 'canonical_name';
    is($prop->{'description'},
	"The most reliable device name to use to refer to this device.",
	"property info for canonical name is correct");
}
ok(!$dev->property_get("full_deletion"),
    "property_get(full_deletion) on null device");
is($dev->property_get("comment"), undef,
    "no comment by default");
ok($dev->property_set("comment", "well, that was silly"),
    "set comment property");
is($dev->property_get("comment"), "well, that was silly",
    "comment correctly stored");

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

properties_include([ $dev->property_list() ],
    [ @common_properties, 'max_volume_usage' ],
    "necessary properties listed on vfs device");

# play with properties a little bit
ok($dev->property_set("comment", 16),
    "set an string property to an integer");

ok($dev->property_set("comment", 16.0),
    "set an string property to a float");

ok($dev->property_set("comment", "hi mom"),
    "set an string property to a string");

ok($dev->property_set("comment", "32768"),
    "set an integer property to a simple string");

ok($dev->property_set("comment", "32k"),
    "set an integer property to a string with a unit");

ok($dev->property_set("block_size", 32768),
    "set an integer property to an integer");

ok(!($dev->property_set("invalid-property-name", 32768)),
    "set an invalid-property-name");

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
    write_file(0x2FACE, $dev->block_size()*10+17, $i);
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

write_file(0xD0ED0E, $dev->block_size()*4, 4);

ok($dev->finish(),
    "finish device after append")
    or diag($dev->error_or_status());

# try reading the third file back, creating a new device
# object first, and skipping the read-label step.

$dev = undef;
$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: re-create successful")
    or diag($dev->error_or_status());

ok($dev->start($ACCESS_READ, undef, undef),
    "start in read mode")
    or diag($dev->error_or_status());

verify_file(0x2FACE, $dev->block_size()*10+17, 3);

{
    # try two seek_file's in a row
    my $hdr = $dev->seek_file(3);
    is($hdr? $hdr->{'type'} : -1, $Amanda::Header::F_DUMPFILE, "seek_file the first time");
    $hdr = $dev->seek_file(3);
    is($hdr? $hdr->{'type'} : -1, $Amanda::Header::F_DUMPFILE, "seek_file the second time");
}

ok($dev->finish(),
    "finish device after read")
    or diag($dev->error_or_status());

# test erase
ok($dev->erase(),
   "erase device")
    or diag($dev->error_or_status());

ok($dev->erase(),
   "erase device (again)")
    or diag($dev->error_or_status());

ok($dev->finish(),
   "finish device after erase")
    or diag($dev->error_or_status());

# test monitor_free_space property (testing the monitoring would require a
# dedicated partition for the tests - it's not worth it)

ok($dev->property_get("monitor_free_space"),
    "monitor_free_space property is set by default");

ok($dev->property_set("monitor_free_space", 0),
    "monitor_free_space property can be set to false");

ok(!$dev->property_get("monitor_free_space"),
    "monitor_free_space property value 'sticks'");

# test the LEOM functionality

$dev = undef;
$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: re-create successful")
    or diag($dev->error_or_status());
ok($dev->property_set("MAX_VOLUME_USAGE", "512k"),
    "set MAX_VOLUME_USAGE to test LEOM");
ok($dev->property_set("LEOM", 1),
    "set LEOM");
ok($dev->property_set("ENFORCE_MAX_VOLUME_USAGE", 0),
    "set ENFORCE_MAX_VOLUME_USAGE");

ok($dev->start($ACCESS_WRITE, 'TESTCONF23', undef),
    "start in write mode")
    or diag($dev->error_or_status());

ok($dev->start_file($dumpfile),
    "start file 1")
    or diag($dev->error_or_status());

ok(Amanda::Device::write_random_to_device(0xCAFE, 440*1024, $dev),
    "write random data into the early-warning zone");

ok(!$dev->is_eom,
    "device does not indicates LEOM after writing when ENFORCE_MAX_VOLUME_USAGE is FALSE");

ok($dev->finish_file(),
    "..but a finish_file is allowed to complete")
    or diag($dev->error_or_status());

ok($dev->finish(),
   "finish device after LEOM test")
    or diag($dev->error_or_status());

$dev = undef;
$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: re-create successful")
    or diag($dev->error_or_status());
ok($dev->property_set("MAX_VOLUME_USAGE", "512k"),
    "set MAX_VOLUME_USAGE to test LEOM");
ok($dev->property_set("LEOM", 1),
    "set LEOM");
ok($dev->property_set("ENFORCE_MAX_VOLUME_USAGE", 1),
    "set ENFORCE_MAX_VOLUME_USAGE");

ok($dev->start($ACCESS_WRITE, 'TESTCONF23', undef),
    "start in write mode")
    or diag($dev->error_or_status());

ok($dev->start_file($dumpfile),
    "start file 1")
    or diag($dev->error_or_status());

ok(!$dev->is_eom,
    "device does not indicate LEOM before writing");

ok(Amanda::Device::write_random_to_device(0xCAFE, 440*1024, $dev),
    "write random data into the early-warning zone");

ok($dev->is_eom,
    "device indicates LEOM after writing");

ok($dev->finish_file(),
    "..but a finish_file is allowed to complete")
    or diag($dev->error_or_status());

ok($dev->finish(),
   "finish device after LEOM test")
    or diag($dev->error_or_status());

$dev = undef;
$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: re-create successful")
    or diag($dev->error_or_status());
ok($dev->property_set("MAX_VOLUME_USAGE", "512k"),
    "set MAX_VOLUME_USAGE to test LEOM");
ok($dev->property_set("LEOM", 1),
    "set LEOM");

ok($dev->start($ACCESS_WRITE, 'TESTCONF23', undef),
    "start in write mode")
    or diag($dev->error_or_status());

ok($dev->start_file($dumpfile),
    "start file 1")
    or diag($dev->error_or_status());

ok(!$dev->is_eom,
    "device does not indicate LEOM before writing");

ok(Amanda::Device::write_random_to_device(0xCAFE, 440*1024, $dev),
    "write random data into the early-warning zone");

ok($dev->is_eom,
    "device indicates LEOM after writing as default value of ENFORCE_MAX_VOLUME_USAGE is true for vfs device");

ok($dev->finish_file(),
    "..but a finish_file is allowed to complete")
    or diag($dev->error_or_status());

ok($dev->finish(),
   "finish device after LEOM test")
    or diag($dev->error_or_status());

$dev = undef;
$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: re-create successful")
    or diag($dev->error_or_status());
ok($dev->property_set("MAX_VOLUME_USAGE", "160k"),
    "set MAX_VOLUME_USAGE to test LEOM while writing the first header");
ok($dev->property_set("LEOM", 1),
    "set LEOM");

ok($dev->start($ACCESS_WRITE, 'TESTCONF23', undef),
    "start in write mode")
    or diag($dev->error_or_status());

ok($dev->start_file($dumpfile),
    "start file 1")
    or diag($dev->error_or_status());

ok($dev->is_eom,
    "device indicates LEOM after writing first header");

ok($dev->finish_file(),
    "..but a finish_file is allowed to complete")
    or diag($dev->error_or_status());

ok($dev->finish(),
   "finish device after LEOM test")
    or diag($dev->error_or_status());

####
## Test a RAIT device of two vfs devices.

($vtape1, $vtape2) = (mkvtape(1), mkvtape(2));
$dev_name = "rait:file:{$vtape1,$vtape2}";

$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
   "$dev_name: create successful")
    or diag($dev->error_or_status());

ok($dev->configure(1), "configure device");

properties_include([ $dev->property_list() ], [ @common_properties ],
    "necessary properties listed on rait device");

is($dev->property_get("block_size"), 32768, # (RAIT default)
    "rait device calculates a default block size correctly");

ok($dev->property_set("block_size", 32768*16),
    "rait device accepts an explicit block size");

is($dev->property_get("block_size"), 32768*16,
    "..and remembers it");

ok($dev->property_set("max_volume_usage", 32768*1000),
    "rait device accepts property MAX_VOLUME_USAGE");

is($dev->property_get("max_volume_usage"), 32768*1000,
    "..and remembers it");

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
    write_file(0x2FACE, $dev->block_size()*10+17, $i);
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

write_file(0xD0ED0E, $dev->block_size()*4, 4);

ok($dev->finish(),
   "finish device after append")
    or diag($dev->error_or_status());

# try reading the third file back, creating a new device
# object first, and skipping the read-label step.

$dev = undef;
$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "$dev_name: re-create successful")
    or diag($dev->error_or_status());

ok($dev->start($ACCESS_READ, undef, undef),
   "start in read mode")
    or diag($dev->error_or_status());

verify_file(0x2FACE, $dev->block_size()*10+17, 3);

ok($dev->finish(),
   "finish device after read")
    or diag($dev->error_or_status());

ok($dev->start($ACCESS_READ, undef, undef),
   "start in read mode after missing volume")
    or diag($dev->error_or_status());

# corrupt the device somehow and hope it keeps working
rmtree("$taperoot/1");

verify_file(0x2FACE, $dev->block_size()*10+17, 3);
verify_file(0xD0ED0E, $dev->block_size()*4, 4);
verify_file(0x2FACE, $dev->block_size()*10+17, 2);

ok($dev->finish(),
   "finish device read after missing volume")
    or diag($dev->error_or_status());

ok(!($dev->start($ACCESS_WRITE, "TESTCONF29", undef)),
   "start in write mode fails with missing volume")
    or diag($dev->error_or_status());

undef $dev;

$dev_name = "rait:{file:$vtape2,MISSING}";
$dev = Amanda::Device->new($dev_name);

ok($dev->start($ACCESS_READ, undef, undef),
   "start in read mode with MISSING")
    or diag($dev->error_or_status());

verify_file(0x2FACE, $dev->block_size()*10+17, 3);
verify_file(0xD0ED0E, $dev->block_size()*4, 4);
verify_file(0x2FACE, $dev->block_size()*10+17, 2);

ok($dev->finish(),
   "finish device read with MISSING")
    or diag($dev->error_or_status());

ok(!($dev->start($ACCESS_WRITE, "TESTCONF29", undef)),
   "start in write mode fails with MISSING")
    or diag($dev->error_or_status());

undef $dev;

$dev = Amanda::Device->new_rait_from_children(
    Amanda::Device->new("file:$vtape2"), undef);

ok(!($dev->start($ACCESS_WRITE, "TESTCONF29", undef)),
   "start a RAIT device in write mode fails, when created with 'undef'")
    or diag($dev->error_or_status());

# Make two devices with different labels, should get a
# message accordingly.
($vtape1, $vtape2) = (mkvtape(1), mkvtape(2));

my $n = 13;
for $dev_name ("file:$vtape1", "file:$vtape2") {
    my $dev = Amanda::Device->new($dev_name);
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
       "$dev_name: Open successful")
	or diag($dev->error_or_status());
    ok($dev->start($ACCESS_WRITE, "TESTCONF$n", undef),
	"wrote label 'TESTCONF$n'");
    ok($dev->finish(), "finished device");
    $n++;
}

$dev = Amanda::Device->new_rait_from_children(
    Amanda::Device->new("file:$vtape1"),
    Amanda::Device->new("file:$vtape2"));
is($dev->status(), $DEVICE_STATUS_SUCCESS,
   "new_rait_from_children: Open successful")
    or diag($dev->error_or_status());

$dev->read_label();
ok($dev->status() & $DEVICE_STATUS_VOLUME_ERROR,
   "Label mismatch error handled correctly")
    or diag($dev->error_or_status());

# Use some config to set a block size on a child device
($vtape1, $vtape2) = (mkvtape(1), mkvtape(2));
$dev_name = "rait:{file:$vtape1,mytape2}";

$testconf = Installcheck::Config->new();
$testconf->add_device("mytape2", [
    "tapedev" => "\"file:$vtape2\"",
    "device_property" => "\"BLOCK_SIZE\" \"64k\""
]);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load configuration");

$dev = Amanda::Device->new($dev_name);
is($dev->status(), $DEVICE_STATUS_SUCCESS,
   "$dev_name: create successful")
    or diag($dev->error_or_status());

ok($dev->configure(1), "configure device");

is($dev->property_get("block_size"), 65536,
    "rait device calculates a block size from its children correctly");

# Test an S3 device if the proper environment variables are set
my $S3_SECRET_KEY = $ENV{'INSTALLCHECK_S3_SECRET_KEY'};
my $S3_ACCESS_KEY = $ENV{'INSTALLCHECK_S3_ACCESS_KEY'};
my $DEVPAY_SECRET_KEY = $ENV{'INSTALLCHECK_DEVPAY_SECRET_KEY'};
my $DEVPAY_ACCESS_KEY = $ENV{'INSTALLCHECK_DEVPAY_ACCESS_KEY'};
my $DEVPAY_USER_TOKEN = $ENV{'INSTALLCHECK_DEVPAY_USER_TOKEN'};

my $run_s3_tests = defined $S3_SECRET_KEY && defined $S3_ACCESS_KEY;
my $run_devpay_tests = defined $DEVPAY_SECRET_KEY &&
    defined $DEVPAY_ACCESS_KEY && $DEVPAY_USER_TOKEN;

my $s3_make_device_count = 7;
sub s3_make_device($$) {
    my ($dev_name, $kind) = @_;
    $dev = Amanda::Device->new($dev_name);
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
       "$dev_name: create successful")
        or diag($dev->error_or_status());

    my @s3_props = ( 's3_access_key', 's3_secret_key' );
    push @s3_props, 's3_user_token' if ($kind eq "devpay");
    properties_include([ $dev->property_list() ], [ @common_properties, @s3_props ],
	"necessary properties listed on s3 device");

    ok($dev->property_set('BLOCK_SIZE', 32768*2),
	"set block size")
	or diag($dev->error_or_status());

    # might as well save a few cents while testing this property..
    ok($dev->property_set('S3_STORAGE_CLASS', 'REDUCED_REDUNDANCY'),
	"set storage class")
	or diag($dev->error_or_status());

    if ($kind eq "s3") {
        # use regular S3 credentials
        ok($dev->property_set('S3_ACCESS_KEY', $S3_ACCESS_KEY),
           "set S3 access key")
        or diag($dev->error_or_status());

        ok($dev->property_set('S3_SECRET_KEY', $S3_SECRET_KEY),
           "set S3 secret key")
            or diag($dev->error_or_status());

	pass("(placeholder)");
    } elsif ($kind eq "devpay") {
        # use devpay credentials
        ok($dev->property_set('S3_ACCESS_KEY', $DEVPAY_ACCESS_KEY),
           "set devpay access key")
        or diag($dev->error_or_status());

        ok($dev->property_set('S3_SECRET_KEY', $DEVPAY_SECRET_KEY),
           "set devpay secret key")
            or diag($dev->error_or_status());

        ok($dev->property_set('S3_USER_TOKEN', $DEVPAY_USER_TOKEN),
           "set devpay user token")
            or diag($dev->error_or_status());
    } else {
        croak("didn't recognize the device kind, so no credentials were set");
    }
    return $dev;
}

my $base_name;

SKIP: {
    skip "define \$INSTALLCHECK_S3_{SECRET,ACCESS}_KEY to run S3 tests",
            101 +
            1 * $verify_file_count +
            7 * $write_file_count +
            13 * $s3_make_device_count
	unless $run_s3_tests;

    $dev_name = "s3:";
    $dev = Amanda::Device->new($dev_name);
    isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
         "creating $dev_name fails miserably");

    $dev_name = "s3:foo";
    $dev = Amanda::Device->new($dev_name);

    ok($dev->property_get("full_deletion"),
       "property_get(full_deletion) on s3 device");

    ok($dev->property_get("leom"),
       "property_get(leom) on s3 device");

    # test parsing of boolean values
    # (s3 is the only device driver that has a writable boolean property at the
    # moment)

    my @verbose_vals = (
	{'val' => '1', 'true' => 1},
	{'val' => '0', 'true' => 0},
	{'val' => 't', 'true' => 1},
	{'val' => 'true', 'true' => 1},
	{'val' => 'f', 'true' => 0},
	{'val' => 'false', 'true' => 0},
	{'val' => 'y', 'true' => 1},
	{'val' => 'yes', 'true' => 1},
	{'val' => 'n', 'true' => 0},
	{'val' => 'no', 'true' => 0},
	{'val' => 'on', 'true' => 1},
	{'val' => 'off', 'true' => 0},
	{'val' => 'oFf', 'true' => 0},
	);

    foreach my $v (@verbose_vals) {
	$dev_name = "s3:foo";
	$dev = Amanda::Device->new($dev_name);

	$testconf = Installcheck::Config->new();
	$testconf->add_param("device_property", "\"verbose\" \"$v->{'val'}\"");
	$testconf->write();
	config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
	    or die("Could not load configuration");

	ok($dev->configure(1),
	   "configured device with verbose set to $v->{'val'}")
	    or diag($dev->error_or_status());

	my $get_val = $dev->property_get('verbose');
	# see if truth-iness matches
	my $expec = $v->{'true'}? "true" : "false";
	is(!!$dev->property_get('verbose'), !!$v->{'true'},
	   "device_property 'VERBOSE' '$v->{'val'}' => property_get(verbose) returning $expec");
    }

    # test unparsable property
    $dev_name = "s3:foo";
    $dev = Amanda::Device->new($dev_name);

    $testconf = Installcheck::Config->new();
    $testconf->add_param("device_property", "\"verbose\" \"foo\"");
    $testconf->write();
    config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
	or die("Could not load configuration");

    ok(!$dev->configure(1),
       "failed to configure device with verbose set to foo");

    like($dev->error_or_status(), qr/'verbose'/,
         "error message mentions property name");

    like($dev->error_or_status(), qr/'foo'/,
         "error message mentions property value");

    like($dev->error_or_status(), qr/gboolean/,
         "error message mentions property type");

    my $hostname  = hostname();
    $hostname =~ s/\./-/g;
    $base_name = "$S3_ACCESS_KEY-installcheck-$hostname";
    $dev_name = "s3:$base_name-s3";
    $dev = s3_make_device($dev_name, "s3");
    $dev->read_label();
    my $status = $dev->status();
    # this test appears very liberal, but catches the case where setup_handle fails without
    # giving false positives
    ok(($status == $DEVICE_STATUS_SUCCESS) || (($status & $DEVICE_STATUS_VOLUME_UNLABELED) != 0),
       "status is either OK or possibly unlabeled")
        or diag($dev->error_or_status());

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
       "start in write mode")
        or diag($dev->error_or_status());

    ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
       "it's labeled now")
        or diag($dev->error_or_status());

    for (my $i = 1; $i <= 3; $i++) {
        write_file(0x2FACE, $dev->block_size()*10, $i);
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

    write_file(0xD0ED0E, $dev->block_size()*10, 4);

    ok($dev->finish(),
       "finish device after append")
        or diag($dev->error_or_status());

    # try reading the third file back

    ok($dev->start($ACCESS_READ, undef, undef),
       "start in read mode")
        or diag($dev->error_or_status());

    verify_file(0x2FACE, $dev->block_size()*10, 3);

    # test EOT indications on reading
    my $hdr = $dev->seek_file(4);
    is($hdr->{'type'}, $Amanda::Header::F_DUMPFILE,
	"file 4 has correct type F_DUMPFILE");

    $hdr = $dev->seek_file(5);
    is($hdr->{'type'}, $Amanda::Header::F_TAPEEND,
	"file 5 has correct type F_TAPEEND");

    $hdr = $dev->seek_file(6);
    is($hdr, undef, "seek_file returns undef for file 6");

    ok($dev->finish(),
       "finish device after read")
        or diag($dev->error_or_status());    # (note: we don't use write_max_size here,
					     # as the maximum for S3 is very large)

    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());

    ok($dev->erase(),
       "erase device (again)")
       or diag($dev->error_or_status());

    ok($dev->finish(),
       "finish device after erase")
        or diag($dev->error_or_status());

    $dev->read_label();
    $status = $dev->status();
    ok($status & $DEVICE_STATUS_VOLUME_UNLABELED,
       "status is unlabeled after an erase")
        or diag($dev->error_or_status());

    $dev = s3_make_device($dev_name, "s3");

    ok($dev->erase(),
       "erase device right after creation")
       or diag($dev->error_or_status());

    $dev = s3_make_device($dev_name, "s3");

    # set MAX_VOLUME_USAGE, LEOM=true, ENFORCE_MAX_VOLUME_USAGE=false
    ok($dev->property_set('MAX_VOLUME_USAGE', "512k"),
       "set MAX_VOLUME_USAGE to test LEOM");

    ok($dev->property_set("LEOM", 1),
        "set LEOM");

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef), 
       "start in write mode")
        or diag($dev->error_or_status());

    write_file(0x2FACE, 440*1024, 1);

    ok(!$dev->is_eom,
        "device does not indicate LEOM after writing as property ENFORCE_MAX_VOLUME_USAGE not set and its default value is false");

    ok($dev->finish(),
       "finish device after LEOM test")
       or diag($dev->error_or_status());
    
    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());
    
    $dev = s3_make_device($dev_name, "s3");

    # set MAX_VOLUME_USAGE, LEOM=true, ENFORCE_MAX_VOLUME_USAGE=true
    ok($dev->property_set('MAX_VOLUME_USAGE', "512k"),
       "set MAX_VOLUME_USAGE to test LEOM");

    ok($dev->property_set('ENFORCE_MAX_VOLUME_USAGE', 1 ),
       "set ENFORCE_MAX_VOLUME_USAGE");

    ok($dev->property_set("LEOM", 1),
        "set LEOM");

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef), 
       "start in write mode")
        or diag($dev->error_or_status());

    write_file(0x2FACE, 440*1024, 1);

    ok($dev->is_eom,
        "device indicates LEOM after writing, when property ENFORCE_MAX_VOLUME_USAGE set to true");

    ok($dev->finish(),
       "finish device after LEOM test")
       or diag($dev->error_or_status());

    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());
    
    $dev = s3_make_device($dev_name, "s3");

    # set MAX_VOLUME_USAGE, LEOM=true, ENFORCE_MAX_VOLUME_USAGE=false
    ok($dev->property_set('MAX_VOLUME_USAGE', "512k"),
       "set MAX_VOLUME_USAGE to test LEOM");

    ok($dev->property_set('ENFORCE_MAX_VOLUME_USAGE', 0 ),
       "set ENFORCE_MAX_VOLUME_USAGE");

    ok($dev->property_set("LEOM", 1),
        "set LEOM");

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef), 
       "start in write mode")
        or diag($dev->error_or_status());

    write_file(0x2FACE, 440*1024, 1);

    ok(!$dev->is_eom,
        "device does not indicate LEOM after writing, when property ENFORCE_MAX_VOLUME_USAGE set to false");

    ok($dev->finish(),
       "finish device after LEOM test")
       or diag($dev->error_or_status());
    
    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());
    
    # try with empty user token
    $dev_name = lc("s3:$base_name-s3");
    $dev = s3_make_device($dev_name, "s3");
    ok($dev->property_set('S3_USER_TOKEN', ''),
       "set devpay user token")
        or diag($dev->error_or_status());

    $dev->read_label();
    $status = $dev->status();
    ok(($status == $DEVICE_STATUS_SUCCESS) || (($status & $DEVICE_STATUS_VOLUME_UNLABELED) != 0),
       "status is either OK or possibly unlabeled")
        or diag($dev->error_or_status());

    $dev->finish();

    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());

    # try a eu-constrained bucket
    $dev_name = lc("s3:$base_name-s3-eu");
    $dev = s3_make_device($dev_name, "s3");
    ok($dev->property_set('S3_BUCKET_LOCATION', 'EU'),
       "set S3 bucket location to 'EU'")
        or diag($dev->error_or_status());

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
       "start in write mode")
        or diag($dev->error_or_status());

    is($dev->status(), $DEVICE_STATUS_SUCCESS,
       "status is OK")
        or diag($dev->error_or_status());

    $dev->finish();

    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());

    # try a wildcard-constrained bucket
    $dev_name = lc("s3:$base_name-s3-wild");
    $dev = s3_make_device($dev_name, "s3");
    ok($dev->property_set('S3_BUCKET_LOCATION', '*'),
       "set S3 bucket location to ''")
        or diag($dev->error_or_status());

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
       "start in write mode")
        or diag($dev->error_or_status());

    is($dev->status(), $DEVICE_STATUS_SUCCESS,
       "status is OK")
        or diag($dev->error_or_status());

    $dev->finish();

    # test again with invalid ca_info
    $dev = s3_make_device($dev_name, "s3");
    SKIP: {
	skip "SSL not supported; can't check SSL_CA_INFO", 2
	    unless $dev->property_get('S3_SSL');

	ok($dev->property_set('SSL_CA_INFO', '/dev/null'),
	   "set invalid SSL/TLS CA certificate")
	    or diag($dev->error_or_status());

        ok(!$dev->start($ACCESS_WRITE, "TESTCONF13", undef),
           "start in write mode")
            or diag($dev->error_or_status());

        isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
           "status is OK")
            or diag($dev->error_or_status());

        $dev->finish();
    }

    # test again with our own CA bundle
    $dev = s3_make_device($dev_name, "s3");
    SKIP: {
	skip "SSL not supported; can't check SSL_CA_INFO", 4
	    unless $dev->property_get('S3_SSL');
	ok($dev->property_set('SSL_CA_INFO', "$srcdir/data/aws-bundle.crt"),
	   "set our own SSL/TLS CA certificate bundle")
	    or diag($dev->error_or_status());

        ok($dev->erase(),
           "erase device")
            or diag($dev->error_or_status());

        ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
           "start in write mode")
            or diag($dev->error_or_status());

        is($dev->status(), $DEVICE_STATUS_SUCCESS,
           "status is OK")
            or diag($dev->error_or_status());

	$dev->finish();
    }

    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());

    # bucket names incompatible with location constraint
    $dev_name = "s3:-$base_name-s3-eu";
    $dev = s3_make_device($dev_name, "s3");

    ok($dev->property_set('S3_BUCKET_LOCATION', ''),
       "should be able to set an empty S3 bucket location with an incompatible name")
        or diag($dev->error_or_status());

    $dev_name = "s3:$base_name-s3.eu";
    $dev = s3_make_device($dev_name, "s3");

    ok($dev->property_set('S3_BUCKET_LOCATION', ''),
       "should be able to set an empty S3 bucket location with an incompatible name")
        or diag($dev->error_or_status());

    $dev_name = "s3:-$base_name-s3-eu";
    $dev = s3_make_device($dev_name, "s3");

    ok(!$dev->property_set('S3_BUCKET_LOCATION', 'EU'),
       "should not be able to set S3 bucket location with an incompatible name")
        or diag($dev->error_or_status());

    $dev_name = lc("s3:$base_name-s3-eu");
    $dev = s3_make_device($dev_name, "s3");
    ok($dev->property_set('S3_BUCKET_LOCATION', 'XYZ'),
       "should be able to set S3 bucket location with a compatible name")
        or diag($dev->error_or_status());
    $dev->read_label();
    $status = $dev->status();
    ok(($status == $DEVICE_STATUS_DEVICE_ERROR),
       "status is DEVICE_STATUS_DEVICE_ERROR")
        or diag($dev->error_or_status());
    my $error_msg = $dev->error_or_status();
    ok(($dev->error_or_status() == "While creating new S3 bucket: The specified location-constraint is not valid (Unknown) (HTTP 400)"),
       "invalid location-constraint")
       or diag("bad error: " . $dev->error_or_status());

}

SKIP: {
    # in this case, most of our code has already been exercised
    # just make sure that authentication works as a basic sanity check
    skip "skipping abbreviated devpay tests", $s3_make_device_count + 1
	unless $run_devpay_tests;
    $dev_name = "s3:$base_name-devpay";
    $dev = s3_make_device($dev_name, "devpay");
    $dev->read_label();
    my $status = $dev->status();
    # this test appears very liberal, but catches the case where setup_handle fails without
    # giving false positives
    ok(($status == 0) || (($status & $DEVICE_STATUS_VOLUME_UNLABELED) != 0),
       "status is either OK or possibly unlabeled")
	or diag($dev->error_or_status());
}

# Test a tape device if the proper environment variables are set
my $TAPE_DEVICE = $ENV{'INSTALLCHECK_TAPE_DEVICE'};
my $run_tape_tests = defined $TAPE_DEVICE;
SKIP: {
    skip "define \$INSTALLCHECK_TAPE_DEVICE to run tape tests",
	    30 +
	    7 * $verify_file_count +
	    5 * $write_file_count
	unless $run_tape_tests;

    $dev_name = "tape:$TAPE_DEVICE";
    $dev = Amanda::Device->new($dev_name);
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"$dev_name: create successful")
	or diag($dev->error_or_status());

    my $status = $dev->read_label();
    ok(($status == $DEVICE_STATUS_SUCCESS) || (($status & $DEVICE_STATUS_VOLUME_UNLABELED) != 0),
       "status is either OK or possibly unlabeled")
        or diag($dev->error_or_status());

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
	"start in write mode")
	or diag($dev->error_or_status());

    ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
	"not unlabeled anymore")
	or diag($dev->error_or_status());

    for (my $i = 1; $i <= 4; $i++) {
	write_file(0x2FACE+$i, $dev->block_size()*10+17, $i);
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

    # if final_filemarks is 1, then the tape device will use F_NOOP,
    # inserting an extra file, and we'll be appending at file number 6.
    my $append_fileno = ($dev->property_get("FINAL_FILEMARKS") == 2)? 5:6;

    SKIP: {
        skip "APPEND not supported", $write_file_count + 2
            unless $dev->property_get("APPENDABLE");

        ok($dev->start($ACCESS_APPEND, undef, undef),
            "start in append mode")
            or diag($dev->error_or_status());

        write_file(0xD0ED0E, $dev->block_size()*4, $append_fileno);

        ok($dev->finish(),
            "finish device after append")
            or diag($dev->error_or_status());
    }

    # try reading the second and third files back, creating a new
    # device object first, and skipping the read-label step.

    $dev = undef;
    $dev = Amanda::Device->new($dev_name);
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"$dev_name: re-create successful")
	or diag($dev->error_or_status());

    # use a big read_block_size, checking that it's also settable
    # via read_buffer_size
    ok($dev->property_set("read_buffer_size", 256*1024),
	"can set read_buffer_size");
    is($dev->property_get("read_block_size"), 256*1024,
	"and its value is reflected in read_block_size");
    ok($dev->property_set("read_block_size", 32*1024),
	"can set read_block_size");

    ok($dev->start($ACCESS_READ, undef, undef),
	"start in read mode")
	or diag($dev->error_or_status());

    # now verify those files in a particular order to trigger all of the
    # seeking edge cases

    verify_file(0x2FACE+1, $dev->block_size()*10+17, 1);
    verify_file(0x2FACE+2, $dev->block_size()*10+17, 2);
    verify_file(0x2FACE+4, $dev->block_size()*10+17, 4);
    verify_file(0x2FACE+3, $dev->block_size()*10+17, 3);
    verify_file(0x2FACE+1, $dev->block_size()*10+17, 1);

    # try re-seeking to the same file
    ok(header_for($dev->seek_file(2), 2), "seek to file 2 the first time");
    verify_file(0x2FACE+2, $dev->block_size()*10+17, 2);
    ok(header_for($dev->seek_file(2), 2), "seek to file 2 the third time");

    # and seek through the same pattern *without* reading to EOF
    ok(header_for($dev->seek_file(1), 1), "seek to file 1");
    ok(header_for($dev->seek_file(2), 2), "seek to file 2");
    ok(header_for($dev->seek_file(4), 4), "seek to file 4");
    ok(header_for($dev->seek_file(3), 3), "seek to file 3");
    ok(header_for($dev->seek_file(1), 1), "seek to file 1");

    SKIP: {
        skip "APPEND not supported", $verify_file_count
            unless $dev->property_get("APPENDABLE");
	verify_file(0xD0ED0E, $dev->block_size()*4, $append_fileno);
    }

    ok($dev->finish(),
	"finish device after read")
	or diag($dev->error_or_status());

    # tickle a regression in improperly closing fd's
    ok($dev->finish(),
	"finish device again after read")
	or diag($dev->error_or_status());

    ok($dev->read_label() == $DEVICE_STATUS_SUCCESS,
	"read_label after second finish (used to fail)")
	or diag($dev->error_or_status());

    # finally, run the device with FSF and BSF set to "no", to test the
    # fallback schemes for this condition

    $dev = undef;
    $dev = Amanda::Device->new($dev_name);
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"$dev_name: re-create successful")
	or diag($dev->error_or_status());
    $dev->property_set("fsf", "no");
    $dev->property_set("bsf", "no");

    ok($dev->start($ACCESS_READ, undef, undef),
	"start in read mode")
	or diag($dev->error_or_status());

    ok(header_for($dev->seek_file(1), 1), "seek to file 1");
    ok(header_for($dev->seek_file(4), 4), "seek to file 4");
    ok(header_for($dev->seek_file(2), 2), "seek to file 2");

    ok($dev->finish(),
	"finish device after read")
	or diag($dev->error_or_status());
}

SKIP: {
    skip "not built with ndmp and server", 94 unless
	Amanda::Util::built_with_component("ndmp") and
	Amanda::Util::built_with_component("server");

    my $dev;
    my $testconf = Installcheck::Config->new();
    $testconf->write();

    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }

    my $ndmp = Installcheck::Mock::NdmpServer->new();
    my $ndmp_port = $ndmp->{'port'};
    my $drive = $ndmp->{'drive'};
    pass("started ndmjob in daemon mode");

    # set up a header for use below
    my $hdr = Amanda::Header->new();
    $hdr->{type} = $Amanda::Header::F_DUMPFILE;
    $hdr->{datestamp} = "20070102030405";
    $hdr->{dumplevel} = 0;
    $hdr->{compressed} = 1;
    $hdr->{name} = "localhost";
    $hdr->{disk} = "/home";
    $hdr->{program} = "INSTALLCHECK";

    $dev = Amanda::Device->new("ndmp:127.0.0.1:9i1\@foo");
    isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
	"creation of an ndmp device fails with invalid port");

    $dev = Amanda::Device->new("ndmp:127.0.0.1:90000\@foo");
    isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
	"creation of an ndmp device fails with too-large port");

    $dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port");
    isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
	"creation of an ndmp device fails without ..\@device_name");

    $dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$drive");
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"creation of an ndmp device succeeds with correct syntax");

    ok($dev->property_set("ndmp_username", "foo"),
	"set ndmp_username property");
    is($dev->property_get("ndmp_username"), "foo",
	"..and get the value back");
    ok($dev->property_set("ndmp_password", "bar"),
	"set ndmp_password property");
    is($dev->property_get("ndmp_password"), "bar",
	"..and get the value back");

    ok($dev->property_set("verbose", 1),
	"set VERBOSE");

    # set 'em back to the defaults
    $dev->property_set("ndmp_username", "ndmp");
    $dev->property_set("ndmp_password", "ndmp");

    # use a big read_block_size, checking that it's also settable
    # via read_buffer_size
    ok($dev->property_set("read_block_size", 256*1024),
    "can set read_block_size");
    is($dev->property_get("read_block_size"), 256*1024,
    "and its value is reflected");
    ok($dev->property_set("read_block_size", 64*1024),
    "set read_block_size back to something smaller");

    # ok, let's fire the thing up
    ok($dev->start($ACCESS_WRITE, "TEST1", "20090915000000"),
	"start device in write mode")
	or diag $dev->error_or_status();

    ok($dev->start_file($hdr),
	"start_file");

    {   # write to the file
	my $xfer = Amanda::Xfer->new([
		Amanda::Xfer::Source::Random->new(32768*21, 0xBEEFEE00),
		Amanda::Xfer::Dest::Device->new($dev, 0) ]);
	$xfer->start(make_cb(xmsg_cb => sub {
	    my ($src, $msg, $xfer) = @_;
	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		Amanda::MainLoop::quit();
	    }
	}));

	Amanda::MainLoop::run();
	pass("wrote 21 blocks");
    }

    ok($dev->finish(),
	"finish device")
	or diag $dev->error_or_status();

    is($dev->read_label(), $DEVICE_STATUS_SUCCESS,
	"read label from (same) device")
	or diag $dev->error_or_status();

    is($dev->volume_label, "TEST1",
	"volume label read back correctly");

    ## label a device and check the label, but open a new device in between

    # Write a label
    $dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$drive");
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"creation of an ndmp device succeeds with correct syntax");
    $dev->property_set("ndmp_username", "ndmp");
    $dev->property_set("ndmp_password", "ndmp");
    $dev->property_set("verbose", 1);

    # Write the label
    ok($dev->start($ACCESS_WRITE, "TEST2", "20090915000000"),
	"start device in write mode")
	or diag $dev->error_or_status();
    ok($dev->finish(),
	"finish device")
	or diag $dev->error_or_status();

    # Read the label with a new device.
    $dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$drive");
    is($dev->status(), $DEVICE_STATUS_SUCCESS,
	"creation of an ndmp device succeeds with correct syntax");
    $dev->property_set("ndmp_username", "ndmp");
    $dev->property_set("ndmp_password", "ndmp");
    $dev->property_set("verbose", 1);

    # read the label
    is($dev->read_label(), $DEVICE_STATUS_SUCCESS,
	"read label from device")
	or diag $dev->error_or_status();
    is($dev->volume_label, "TEST2",
	"volume label read back correctly");
    ok($dev->finish(),
	"finish device")
	or diag $dev->error_or_status();

    #
    # test the directtcp-target implementation
    #

    ok($dev->directtcp_supported(), "is a directtcp target");
    for my $dev_use ('initiator', 'listener') {
	my ($xfer, $addrs, $dest_elt);
	if ($dev_use eq 'listener') {
	    $addrs = $dev->listen(1);
	    ok($addrs, "listen returns successfully") or die($dev->error_or_status());

	    # set up an xfer to write to the device
	    $dest_elt = Amanda::Xfer::Dest::DirectTCPConnect->new($addrs);
	} else {
	    # set up an xfer to write to the device
	    $dest_elt = Amanda::Xfer::Dest::DirectTCPListen->new();
	}
	$xfer = Amanda::Xfer->new([
		Amanda::Xfer::Source::Random->new(32768*34, 0xB00),
		$dest_elt,
	    ]);

	my @messages;
	$xfer->start(make_cb(xmsg_cb => sub {
	    my ($src, $msg, $xfer) = @_;
	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		Amanda::MainLoop::quit();
	    }
	}));

	# write files from the connection until EOF
	my $num_files;
	my $conn;
	my ($finish_connection, $start_device, $write_file_cb);


	$finish_connection = make_cb(finish_connection => sub {
	    if ($dev_use eq 'listener') {
		$conn = $dev->accept();
	    } else {
		$addrs = $dest_elt->get_addrs();
		$conn = $dev->connect(1, $addrs);
	    }
	    Amanda::MainLoop::call_later($start_device);
	});


	$start_device = make_cb(start_device => sub {
	    ok($dev->start($ACCESS_WRITE, "TEST2", "20090915000000"),
		"start device in write mode")
		or diag $dev->error_or_status();

	    Amanda::MainLoop::call_later($write_file_cb);
	});

	$write_file_cb = make_cb(write_file_cb => sub {
	    ++$num_files < 20 or die "I seem to be in a loop!";

	    ok($dev->start_file($hdr), "start file $num_files for writing");
	    is($dev->file, $num_files, "..file number is correct");

	    my ($ok, $size) = $dev->write_from_connection(32768*15);
	    push @messages, sprintf("WRITE-%s-%d-%s-%s",
		$ok?"OK":"ERR", $size,
		$dev->is_eof()? "EOF":"!eof",
		$dev->is_eom()? "EOM":"!eom");
	    ok($ok, "..write from connection succeeds");
	    my $eof = $dev->is_eof();

	    ok($dev->finish_file(), "..finish file after writing");

	    if (!$eof) {
		Amanda::MainLoop::call_later($write_file_cb);
	    }
	});

	Amanda::MainLoop::call_later($finish_connection);
	Amanda::MainLoop::run();
	is_deeply([@messages], [
		'WRITE-OK-491520-!eof-!eom',
		'WRITE-OK-491520-!eof-!eom',
		'WRITE-OK-131072-EOF-!eom',
	    ],
	    "a sequence of write_from_connection calls works correctly");

	$dev->finish();

	if (my $err = $conn->close()) {
	    die $err;
	}
    }

    #
    # Test indirecttcp
    # 

    {
	ok($dev->directtcp_supported(), "is a directtcp target");

	$dev->property_set("indirect", 1);

	my $addrs = $dev->listen(1);
	ok($addrs, "listen returns successfully") or die($dev->error_or_status());

	# fork off to evaluate the indirecttcp addresses and then set up an
	# xfer to write to the device
	if (POSIX::fork() == 0) {
	    # allow other process to start listening.
	    sleep 1;
	    my $nc = $Amanda::Constants::NC;
	    $nc = $Amanda::Constants::NC6 if !$nc;
	    $nc = $Amanda::Constants::NETCAT if !$nc;
	    my $sockresult = `$nc localhost $addrs->[0][1] < /dev/null`;

	    my @sockresult = map { [ split(/:/, $_) ] } split(/ /, $sockresult);
	    $addrs = [ map { $_->[1] = 0 + $_->[1]; $_ } @sockresult ];

	    my $xfer = Amanda::Xfer->new([
		    Amanda::Xfer::Source::Random->new(32768*34, 0xB00),
		    Amanda::Xfer::Dest::DirectTCPConnect->new($addrs) ]);

	    $xfer->start(make_cb(xmsg_cb => sub {
		my ($src, $msg, $xfer) = @_;
		if ($msg->{'type'} == $XMSG_ERROR) {
		    die $msg->{'elt'} . " failed: " . $msg->{'message'};
		} elsif ($msg->{'type'} == $XMSG_DONE) {
		    Amanda::MainLoop::quit();
		}
	    }));

	    Amanda::MainLoop::run();
	    exit(0);
	}

	# write files from the connection until EOF
	my @messages;
	my $num_files;
	my $conn;
	my ($call_accept, $start_device, $write_file_cb);

	$call_accept = make_cb(call_accept => sub {
	    $conn = $dev->accept();
	    Amanda::MainLoop::call_later($start_device);
	});

	$start_device = make_cb(start_device => sub {
	    ok($dev->start($ACCESS_WRITE, "TEST2", "20090915000000"),
		"start device in write mode")
		or diag $dev->error_or_status();

	    Amanda::MainLoop::call_later($write_file_cb);
	});

	$write_file_cb = make_cb(write_file_cb => sub {
	    ++$num_files < 20 or die "I seem to be in a loop!";

	    ok($dev->start_file($hdr), "start file $num_files for writing");
	    is($dev->file, $num_files, "..file number is correct");

	    my ($ok, $size) = $dev->write_from_connection(32768*15);
	    push @messages, sprintf("WRITE-%s-%d-%s-%s",
		$ok?"OK":"ERR", $size,
		$dev->is_eof()? "EOF":"!eof",
		$dev->is_eom()? "EOM":"!eom");
	    ok($ok, "..write from connection succeeds");
	    my $eof = $dev->is_eof();

	    ok($dev->finish_file(), "..finish file after writing");

	    if (!$eof) {
		Amanda::MainLoop::call_later($write_file_cb);
	    } else {
		Amanda::MainLoop::quit();
	    }
	});

	Amanda::MainLoop::call_later($call_accept);
	Amanda::MainLoop::run();
	is_deeply([@messages], [
		'WRITE-OK-491520-!eof-!eom',
		'WRITE-OK-491520-!eof-!eom',
		'WRITE-OK-131072-EOF-!eom',
	    ],
	    "a sequence of write_from_connection calls works correctly");

	$dev->finish();

	if (my $err = $conn->close()) {
	    die $err;
	}
    }

    # now try reading that back piece by piece

    {
	my $filename = "$Installcheck::TMP/Amanda_Device_ndmp.tmp";
	open(my $dest_fh, ">", $filename);

	ok($dev->start($ACCESS_READ, undef, undef),
	    "start device in read mode")
	    or diag $dev->error_or_status();

	my $file;
	for ($file = 1; $file <= 3; $file++) {
	    ok($dev->seek_file($file),
		"seek_file $file");
	    is($dev->file, $file, "..file num is correct");
	    is($dev->block, 0, "..block num is correct");

	    # read the file, writing to our temp file.  We'll check that the byte
	    # sequence is correct later
	    my $xfer = Amanda::Xfer->new([
		    Amanda::Xfer::Source::Device->new($dev),
		    Amanda::Xfer::Dest::Fd->new($dest_fh) ]);

	    $xfer->start(make_cb(xmsg_cb => sub {
		my ($src, $msg, $xfer) = @_;
		if ($msg->{'type'} == $XMSG_ERROR) {
		    die $msg->{'elt'} . " failed: " . $msg->{'message'};
		} elsif ($msg->{'type'} == $XMSG_DONE) {
		    Amanda::MainLoop::quit();
		}
	    }));
	    Amanda::MainLoop::run();

	    pass("read back file " . $file);
	}

	$dev->finish();
	close $dest_fh;

	# now read back and verify that file
	open(my $src_fh, "<", $filename);
	my $xfer = Amanda::Xfer->new([
		Amanda::Xfer::Source::Fd->new($src_fh),
		Amanda::Xfer::Dest::Null->new(0xB00) ]);

	$xfer->start(make_cb(xmsg_cb => sub {
	    my ($src, $msg, $xfer) = @_;
	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		Amanda::MainLoop::quit();
	    }
	}));
	Amanda::MainLoop::run();

	pass("data in the three parts is correct");
	unlink $filename;
    }

    ####
    # Test read_to_connection
    #
    # This requires something that can connect to a device and read from
    # it; the XFA does not have an XFER_MECH_DIRECTTCP_CONNECT, so we fake
    # it by manually connecting and then setting up an xfer with a regular
    # XferSourceFd.  This works because the NDMP server will accept an
    # incoming connection before the Device API accept() method is called;
    # this trick may not work with other DirectTCP-capable devices.  Also,
    # this doesn't work so well if there's an error in the xfer (e.g., a
    # random value mismatch).  But tests are supposed to succeed!

    sub test_read2conn {
	my ($finished_cb) = @_;
	my @events;
	my $file = 1;
	my ($conn, $sock);

	my $steps = define_steps
	    cb_ref => \$finished_cb;

	step setup => sub {
	    my $addrs = $dev->listen(0);

	    # now connect to that
	    $sock = IO::Socket::INET->new(
		Proto => "tcp",
		PeerHost => $addrs->[0][0],
		PeerPort => $addrs->[0][1],
		Blocking => 1,
	    );

	    # and set up a transfer to read from that socket
	    my $xfer = Amanda::Xfer->new([
		    Amanda::Xfer::Source::Fd->new($sock),
		    Amanda::Xfer::Dest::Null->new(0xB00) ]);

	    $xfer->start(make_cb(xmsg_cb => sub {
		my ($src, $msg, $xfer) = @_;
		if ($msg->{'type'} == $XMSG_ERROR) {
		    die $msg->{'elt'} . " failed: " . $msg->{'message'};
		}
		if ($msg->{'type'} == $XMSG_DONE) {
		    push @events, "DONE";
		    $steps->{'quit'}->();
		}
	    }));

	    $steps->{'accept'}->();
	};

	step accept => sub {
	    $conn = $dev->accept();
	    die $dev->error_or_status() unless ($conn);

	    Amanda::MainLoop::call_later($steps->{'start_dev'});
	};

	step start_dev => sub {
	    ok($dev->start($ACCESS_READ, undef, undef),
		"start device in read mode")
		or diag $dev->error_or_status();

	    Amanda::MainLoop::call_later($steps->{'read_part_cb'});
	};

	step read_part_cb => sub {
	    my $hdr = $dev->seek_file($file);
	    die $dev->error_or_status() unless ($hdr);
	    my $size = $dev->read_to_connection(0);
	    push @events, "READ-$size";

	    if (++$file <= 3) {
		Amanda::MainLoop::call_later($steps->{'read_part_cb'});
	    } else {
		# close the connection, which will end the xfer, which will
		# result in a call to finished_cb.  So there.
		push @events, "CLOSE";
		$conn->close();
	    }
	};

	step quit => sub {
	    close $sock or die "close: $!";

	    is_deeply([@events],
		[ "READ-491520", "READ-491520", "READ-131072", "CLOSE", "DONE" ],
		"sequential read_to_connection operations read the right amounts " .
		"and bytestream matches");

	    $finished_cb->();
	};
    }
    test_read2conn(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();

    # try two seek_file's in a row
    $hdr = $dev->seek_file(2);
    is($hdr? $hdr->{'type'} : -1, $Amanda::Header::F_DUMPFILE, "seek_file the first time");
    $hdr = $dev->seek_file(2);
    is($hdr? $hdr->{'type'} : -1, $Amanda::Header::F_DUMPFILE, "seek_file the second time");

    ## test seek_file's handling of EOM

    $hdr = $dev->seek_file(3);
    is($hdr->{type}, $Amanda::Header::F_DUMPFILE, "file 3 is a dumpfile");
    $hdr = $dev->seek_file(4);
    is($hdr->{type}, $Amanda::Header::F_TAPEEND, "file 4 is tapeend");
    $hdr = $dev->seek_file(5);
    is($hdr, undef, "file 5 is an error");
    $hdr = $dev->seek_file(6);
    is($hdr, undef, "file 6 is an error");

    $ndmp->cleanup();
}
unlink($input_filename);
unlink($output_filename);
rmtree($taperoot);
