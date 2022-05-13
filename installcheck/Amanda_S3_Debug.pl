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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

# use Test::More tests => 634;
use Test::More;
use File::Path qw( mkpath rmtree );
use Sys::Hostname;
use Carp;
use strict;
use warnings;

use lib '@amperldir@';
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
use Amanda::Util qw( :constants );
use Amanda::MainLoop;
use IO::Socket;

my $dev;
my $dev_name;
my ($vtape1, $vtape2);
my ($input_filename, $output_filename) =
    ( "$Installcheck::TMP/input.tmp", "$Installcheck::TMP/output.tmp" );
my $taperoot = "$Installcheck::TMP/Amanda_Device_test_tapes";
my $diskflatroot = "$Installcheck::TMP/Amanda_Device_test_diskflat";
my $testconf;

our $BASE_API = "s3";
our $BASE_DEVICE = "s3";

# our $blksize = 0x200 * 0x100000;
our $blksize = 0xc * 0x100000;
our $nblks = 10;
our $padding = 1234;

$ENV{'INSTALLCHECK_S3_CLASS'} = 'REDUCED_REDUNDANCY';
#########################################
# AWS S3 #1
$ENV{'INSTALLCHECK_S3_HOST'} = 's3.amazonaws.com';
$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'AKIA4KLALORIT7EPEEUM';
$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = '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';

#########################################
# AWS S3 #2
#$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'AKIA4KLALORIYJOZXVXS';
#$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = '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';

#########################################
# AWS S3 #3
# $ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'AKIA4KLALORIQ6UZ6QPU';
# $ENV{'INSTALLCHECK_S3_SECRET_KEY'} = '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';

#########################################
# WASABI 
#$ENV{'INSTALLCHECK_S3_HOST'} = 's3.us-east-1.wasabisys.com';
#$ENV{'INSTALLCHECK_S3_HOST'} = 's3.wasabisys.com';
#$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'AEOST9FF7XUCW8W093LP';
#$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = '6RZ03j5PsgPrgPtTC1wJ7tI7pEgUGT4rRq6QDJBp';

#########################################
# GOOGLE 
#$ENV{'INSTALLCHECK_S3_HOST'} = 'storage.googleapis.com';
#$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'GOOG1EF2GHLRRRRPOBUVF3JEYFQ6GZJUVILMGG3XT57BJTYJ5JRKCP6PKLXJQ';
#$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = '7AihyFYrZWEftjouzrS/LYgdOjXBuCWuxRQ8ZKTC';
#$ENV{'INSTALLCHECK_S3_CLASS'} = 'STANDARD';
#$ENV{'INSTALLCHECK_S3_BUCKET_LOCATION'} = 'US';
#
# MINIO
#$ENV{'INSTALLCHECK_S3_HOST'} = 'os-builder.betsol.com:9000';
#$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'test';
#$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = 'amanda-store';
#$ENV{'INSTALLCHECK_S3_CLASS'} = 'STANDARD';

# OPENSTACK
#$BASE_API = "swift3";
#$ENV{'INSTALLCHECK_S3_HOST'} = '192.168.54.13:5000';
#$ENV{'INSTALLCHECK_S3_BUCKET_LOCATION'} = 'RegionOne';
#$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'qauser';
#$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = 'Bet$0l@123';
#########################################
# AZURE #1
#$BASE_API = "azure";
#$BASE_DEVICE = "azure";
#$ENV{'INSTALLCHECK_S3_CLASS'} = 'Cool';
#$ENV{'INSTALLCHECK_S3_HOST'} = 'zmandadevstorage5.blob.core.windows.net';
#$ENV{'INSTALLCHECK_S3_ACCESS_KEY'} = 'zmandadevstorage5';
#$ENV{'INSTALLCHECK_S3_SECRET_KEY'} = 'NM/cSCmKi9azCY39E2Nn6PHXmeRgtgfXm6sIl+109b9fh+1ufx8tZF857vg05MnHLFLmWMyi/SOnMGzf4sVfqg==';

Amanda::Util::setup_application("${BASE_DEVICE}-debug", "server", $CONTEXT_CMDLINE, "amanda","amanda");

# we'll need some vtapes..
sub mkvtape {
    my ($num) = @_;

    my $mytape = "$taperoot/$num";
    if (-d $mytape) { rmtree($mytape); }
    mkpath("$mytape/data");
    return $mytape;
}

# we'll need some diskflat..
sub mkdiskflat {
    my ($num) = @_;

    my $mytape = "$diskflatroot/TESTCONF$num";
    if (-e $mytape) { unlink($mytape); }
    mkpath("$diskflatroot");
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

    my $dev_name = $dev->property_get("CANONICAL_NAME");

    $dumpfile->{'datestamp'} = "2000010101010$filenum";

    ok($dev->start_file($dumpfile),
	"$dev_name: start file $filenum")
	or diag($dev->error_or_status());

    is($dev->file(), $filenum,
	"$dev_name: Device has correct filenum");

    croak ("selected file size $length is *way* too big:" . sprintf("%#lx",$dev->block_size()) )
	unless ($length < $dev->block_size()*10001);
    croak ("write_random_to_device failed(n=$length) blk=" . sprintf("%#lx",$dev->block_size()) )
        unless ( ok(Amanda::Device::write_random_to_device($seed, $length, $dev), "$dev_name: write random data") );

    if(ok($dev->in_file(), "$dev_name: still in_file")) {
      
	unless ( ok($dev->finish_file(), "$dev_name: finish_file") ) {
	    croak ( sprintf("finish_file failed blk=%#lx status=\"%s\"",$dev->block_size(),$dev->error_or_status()) );
	    diag($dev->error_or_status());
	}
    } else {
	pass("$dev_name: not in file, so not calling finish_file");
    }
}

my $verify_file_count = 4;
sub verify_file {
    my ($seed, $length, $filenum) = @_;
    my $r;

    my $dev_name = $dev->property_get("CANONICAL_NAME");
    ok(my $read_dumpfile = $dev->seek_file($filenum),
	"$dev_name: seek to file $filenum")
	or diag($dev->error_or_status());
    is($dev->file(), $filenum,
	"$dev_name: device is really at file $filenum");
    ok(header_for($read_dumpfile, $filenum),
	"$dev_name: header is correct")
	or diag($dev->error_or_status());

    $r = Amanda::Device::verify_random_from_device($seed, $length, $dev);
    ok($r, "$dev_name: verified file contents");
    return $r;
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
#########################################
# Test an S3 device if the proper environment variables are set
my $S3_SECRET_KEY = $ENV{'INSTALLCHECK_S3_SECRET_KEY'};
my $S3_ACCESS_KEY = $ENV{'INSTALLCHECK_S3_ACCESS_KEY'};
my $S3_HOST = $ENV{'INSTALLCHECK_S3_HOST'};
my $S3_CLASS = $ENV{'INSTALLCHECK_S3_CLASS'};
my $S3_BUCKET_LOCATION = $ENV{'INSTALLCHECK_S3_BUCKET_LOCATION'}; # may be undef
my $DEVPAY_SECRET_KEY = $ENV{'INSTALLCHECK_DEVPAY_SECRET_KEY'};
my $DEVPAY_ACCESS_KEY = $ENV{'INSTALLCHECK_DEVPAY_ACCESS_KEY'};
my $DEVPAY_USER_TOKEN = $ENV{'INSTALLCHECK_DEVPAY_USER_TOKEN'};

my $run_s3_tests = defined $S3_SECRET_KEY && defined $S3_ACCESS_KEY;
my $run_devpay_tests = defined $DEVPAY_SECRET_KEY &&
    defined $DEVPAY_ACCESS_KEY && $DEVPAY_USER_TOKEN;

sub make_device($$@) {
    my ($dev_name, $kind, @opts) = @_;
    my $api;

    $dev = Amanda::Device->new($dev_name);

    $dev_name = $dev->property_get("CANONICAL_NAME");

    is($dev->status(), $DEVICE_STATUS_SUCCESS,
       "$dev_name: create successful")
        or diag($dev->error_or_status());

    $dev->property_set("NB_THREADS_BACKUP", 5);
    $dev->property_set("NB_THREADS_RECOVERY", 5);

    # get block size from opts
    my ($blk) = ( grep { m{^[\da-fx]*$} && eval($_); } @opts);

    is($dev->property_set('BLOCK_SIZE', ($blk || 2*32768)), undef,
	"$dev_name: set block size")
	or diag($dev->error_or_status());

    $dev->property_set('VERBOSE', '1') or diag($dev->error_or_status())
	if (grep { lc($_) eq "verbose"; } @opts);

    is($dev->property_set('CHUNKED', '1'), undef, "$dev_name: set CHUNKED to test chunked mode")
	if (grep { lc($_) eq "chunked"; } @opts);
   
    azure_set_properties($dev,@opts)
	if ($kind eq "azure");
    s3_set_properties($dev,$kind,@opts)
	if ($kind ne "azure");

    $dev->create();

    return $dev;
}

my $make_device_count = 7;
sub s3_set_properties($$@) {
    my ($dev, $kind, @opts) = @_;
    my $api;

    die "wrong time!" if ($main::BASE_DEVICE eq "azure");

    is($dev->property_set("CREATE_BUCKET", 1), undef,
	"$dev_name: assert create_bucket")
	or diag($dev->error_or_status());

    my @s3_props = ( 's3_access_key', 's3_secret_key' );
    push @s3_props, 's3_user_token' if ($kind eq "devpay");
    properties_include([ $dev->property_list() ], [ @common_properties, @s3_props ],
	"$dev_name: necessary properties listed on s3 device");

    # might as well save a few cents while testing this property..
    if ( $S3_CLASS // 0 ) {
	is($dev->property_set('S3_STORAGE_CLASS', $S3_CLASS), undef,
	    "$dev_name: set storage class")
	    or diag($dev->error_or_status());
    }

    if ($S3_HOST) {
        is($dev->property_set('S3_HOST', $S3_HOST), undef,
           "$dev_name: set S3 host")
            or diag($dev->error_or_status());
        if ($S3_HOST =~ m{^localhost|^192\.168\.|^127\.0\.0\.1|^::1|^\[::1\]|betsol.com}) {
            is($dev->property_set('S3_SSL', "no"), undef,
               "$dev_name: set S3 no-SSL")
                or diag($dev->error_or_status());
        }
    }

    if ($kind eq "s3" || $kind eq "devpay" || $kind eq "aws4"  ) {
        # use regular S3 credentials
        is($dev->property_set('S3_ACCESS_KEY', $S3_ACCESS_KEY), undef,
           "$dev_name: set S3 access key")
        or diag($dev->error_or_status());

        is($dev->property_set('S3_SECRET_KEY', $S3_SECRET_KEY), undef,
           "$dev_name: set S3 secret key")
            or diag($dev->error_or_status());

	pass("$dev_name: (placeholder)");
    } 

    if ($kind eq "s3") {
	$api = "s3";
    }
    if ($kind eq "aws4") {
	$api = "aws4";
    }
    if ($kind eq "devpay") {
        # use devpay credentials
        is($dev->property_set('S3_USER_TOKEN', $DEVPAY_USER_TOKEN), undef,
           "$dev_name: set devpay user token")
            or diag($dev->error_or_status());
	$api = "s3";
    }
    if ($kind eq "swift1") {
        #hdl->swift_account_id = g_strdup(swift_account_id);
        #hdl->swift_access_key = g_strdup(swift_access_key);                                                                         
	#$api = "swift-1.0";
    }
    if ($kind eq "swift2-username") {
        #hdl->username = g_strdup(username);
        #hdl->password = g_strdup(password);
	#$api = "swift-2.0";
    }
    if ($kind eq "swift3-username") {
        #hdl->username = g_strdup(username);
        #hdl->password = g_strdup(password);
	#$api = "swift-3";
    }
    if ($kind eq "oauth2") {
        #hdl->client_id = g_strdup(client_id);
        #hdl->client_secret = g_strdup(client_secret);
        #hdl->refresh_token = g_strdup(refresh_token);
	#$api = "oauth2";
    }
    if ($kind eq "castor") {
        #hdl->username = g_strdup(username);
        #hdl->password = g_strdup(password);
        #hdl->tenant_name = g_strdup(tenant_name);
        #hdl->reps = g_strdup(reps);
        #hdl->reps_bucket = g_strdup(reps_bucket);
	#$api = "castor";
    }
    if ( ! index($kind,"swift2") ) {
        #g_assert(tenant_id || tenant_name);
        #hdl->tenant_id = g_strdup(tenant_id);
        #hdl->tenant_name = g_strdup(tenant_name);
        #$api = "swift-2.0"
    }
    if ( ! index($kind,"swift3") ) {
        # CREATE-BUCKET: 'off' # test sets to on
        is($dev->property_set('REUSE_CONNECTION', 'on'), undef, 
                "$dev_name: set CHUNKED to test chunked mode");
        is($dev->property_set('S3_SERVICE_PATH', '/v3/auth/tokens'), undef, 
                "$dev_name: set S3_SERVICE_PATH to test /v3/auth/tokens");

        is($dev->property_set('PROJECT_NAME', 'Zmanda_QA'), undef, 
                "$dev_name: set PROJECT_NAME to test Zmanda_QA");
        is($dev->property_set('TENANT_NAME', 'Zmanda_QA'), undef, 
                "$dev_name: set TENANT_NAME to Zmanda_QA");
        is($dev->property_set('TENANT_ID', 'ac9d341b6f504151ac0a10e3ea99b67f'), undef, 
                "$dev_name: set TENANT_ID to ac9d341b6f504151ac0a10e3ea99b67f");
        is($dev->property_set('USERNAME', $S3_ACCESS_KEY), undef, 
                "$dev_name: set USERNAME to $S3_ACCESS_KEY");
        is($dev->property_set('PASSWORD', $S3_SECRET_KEY), undef, 
                "$dev_name: set PASSWORD");

        $api = "swift-3"
    }

    # need a default apparently..?
    if ( $S3_BUCKET_LOCATION ) {
	is($dev->property_set('S3_BUCKET_LOCATION', $S3_BUCKET_LOCATION), undef,
	   "should be able to set an empty S3 bucket location to US")
	    or diag($dev->error_or_status());
    }

    is($dev->property_set('S3_MULTI_PART_UPLOAD', '1'), undef, "$dev_name: set S3_MULTI_PART_UPLOAD to test multi-part mode")
	if (grep { lc($_) eq "multi-part"; } @opts);

    is($dev->property_set('CHUNKED', '1'), undef, "$dev_name: set CHUNKED to test chunked mode")
	if (grep { lc($_) eq "chunked"; } @opts);

    croak("$dev_name: didn't recognize the device kind, so no credentials were set")
	if ( ! $api);

    is($dev->property_set('STORAGE_API', uc($api)), undef, "$dev_name: set storage api to $api")
		or diag($dev->error_or_status());

    return $dev;
}

sub azure_set_properties($$@) 
{
    my ($dev,@opts) = @_;

    is($dev->property_set("AZURE_CREATE_CONTAINER", 1), undef,
	"$dev_name: assert create_container")
	or diag($dev->error_or_status());

    my @azure_props = ( 'azure_account_name', 'azure_account_key' );

    properties_include([ $dev->property_list() ], [ @common_properties, @azure_props ],
	"$dev_name: necessary properties listed on azure device");

    # might as well save a few cents while testing this property..
    is($dev->property_set('AZURE_STORAGE_CLASS', $S3_CLASS), undef,
	"$dev_name: set storage class")
	or diag($dev->error_or_status());

    if ($S3_HOST) {
        is($dev->property_set('AZURE_HOST', $S3_HOST), undef,
           "$dev_name: set Azure host")
            or diag($dev->error_or_status())
    }

    is($dev->property_set('AZURE_ACCOUNT_NAME', $S3_ACCESS_KEY), undef,
       "$dev_name: set Azure access key")
    or diag($dev->error_or_status());

    is($dev->property_set('AZURE_ACCOUNT_KEY', $S3_SECRET_KEY), undef,
       "$dev_name: set Azure secret key")
	or diag($dev->error_or_status());

    is($dev->property_set('AZURE_BLOCK_LIST_UPLOAD', '1'), undef,
       "$dev_name: set AZURE_MULTI_PART_UPLOAD to test multi-part mode")
	if (grep { lc($_) eq "multi-part"; } @opts);
}

sub random_16bit() {
    my $seed = qx{od -A none -N2 -x /dev/urandom};
    $seed =~ tr{0-9a-f}{}cd; 
    return hex($seed);
}

sub erase_and_remake($)
{
    my ($dev) = @_;
    my $dev_name = $dev->property_get("CANONICAL_NAME");

    ok($dev->erase(),
       "$dev_name: erase device")
       or diag($dev->error_or_status());

    ok($dev->erase(),
       "$dev_name: erase device (again)")
       or diag($dev->error_or_status());

    ok($dev->finish(),
       "$dev_name: finish device after erase")
        or diag($dev->error_or_status());

    $dev->create();

    $dev->read_label();
    my $status = $dev->status();
    ok($status & $DEVICE_STATUS_VOLUME_UNLABELED,
       "$dev_name: status is unlabeled after an erase")
        or diag($dev->error_or_status());
}

sub four_file_test($)
{
    my ($dev) = @_;
    my $dev_name = $dev->property_get("CANONICAL_NAME");
    my ($seed1) = random_16bit();
    my ($seed2) = random_16bit();
    my ($fileno) = 0;
    my ($status);
    my $r;
    my $nbytes = $dev->block_size()*( $main::nblks - 1 ) + $main::padding;

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
       "$dev_name: start in write mode")
        or diag($dev->error_or_status());

    ok(!($dev->status() & $DEVICE_STATUS_VOLUME_UNLABELED),
       "$dev_name: it's labeled now")
        or diag($dev->error_or_status());

    #
    # direct write and first read for debug
    #
    write_file($seed1, $nbytes, ++$fileno);
    ok($dev->finish(),
       "$dev_name: finish device after write")
        or diag($dev->error_or_status());

    {
	ok($dev->start($ACCESS_READ, undef, undef),
	   "$dev_name: start in read mode")
	    or diag($dev->error_or_status());
	$r = verify_file($seed1, $nbytes, $fileno);
	ok($dev->finish(),
		"$dev_name: finish device after write")
		or diag($dev->error_or_status());
        return $r if (!$r);
    }

    ok($dev->start($ACCESS_APPEND, "TESTCONF13", undef),
       "$dev_name: start in write mode")
        or diag($dev->error_or_status());

    write_file($seed1, $nbytes, ++$fileno);
    write_file($seed1, $nbytes, ++$fileno);

    ok($dev->finish(),
       "$dev_name: finish device after write")
        or diag($dev->error_or_status());

    $dev->read_label();
    ok(!($dev->status()),
       "$dev_name: no error, at all, from read_label")
	or diag($dev->error_or_status());

    # append one more copy, to test ACCESS_APPEND

    ok($dev->start($ACCESS_APPEND, undef, undef),
       "$dev_name: start in append mode")
        or diag($dev->error_or_status());

    write_file($seed2, $nbytes, ++$fileno);

    ok($dev->finish(),
       "$dev_name: finish device after append")
        or diag($dev->error_or_status());

    # try reading the third file back

    ok($dev->start($ACCESS_READ, undef, undef),
       "$dev_name: start in read mode")
        or diag($dev->error_or_status());

    verify_file($seed1, $nbytes, --$fileno);

    # test EOT indications on reading
    my $hdr = $dev->seek_file(4);
    is($hdr->{'type'}, $Amanda::Header::F_DUMPFILE,
	"$dev_name: file 4 has correct type F_DUMPFILE");

    $hdr = $dev->seek_file(5);
    is($hdr->{'type'}, $Amanda::Header::F_TAPEEND,
	"$dev_name: file 5 has correct type F_TAPEEND");

    $hdr = $dev->seek_file(6);
    is($hdr, undef, "seek_file returns undef for file 6");

    ok($dev->finish(),
       "$dev_name: finish device after read")
        or diag($dev->error_or_status());    # (note: we don't use write_max_size here,
					     # as the maximum for S3 is very large)

    ok($dev->erase(),
       "$dev_name: erase device")
       or diag($dev->error_or_status());

    ok($dev->erase(),
       "$dev_name: erase device (again)")
       or diag($dev->error_or_status());

    ok($dev->finish(),
       "$dev_name: finish device after erase")
        or diag($dev->error_or_status());

    $dev->read_label();
    $status = $dev->status();
    ok($status & $DEVICE_STATUS_VOLUME_UNLABELED,
       "$dev_name: status is unlabeled after an erase")
        or diag($dev->error_or_status());

    ok($dev->erase(),
       "$dev_name: erase device")
      or diag($dev->error_or_status());
}


my $base_name;
my $hostname  = hostname();
$hostname =~ s{\..*}{};
$base_name = lc(substr($S3_ACCESS_KEY,-15)) . "-test-" . lc(substr($hostname,0,10))
    if ( $main::BASE_DEVICE eq "s3" );
$base_name = lc("${S3_ACCESS_KEY}-test-$hostname")
    if ( $main::BASE_DEVICE eq "azure" );

$hostname =~ s/\./-/g;
# strip $base_name too long
if (length($base_name)> 52) {
    $base_name =~ s/buildbot/bb/g;
}
if (length($base_name)> 52) {
    $base_name =~ s/dhcp-//g;
}
if (length($base_name)> 52) {
    $base_name =~ s/zmanda-com/zc/g;
}

my @dev_names = (
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-1",
    "${main::BASE_DEVICE}:${base_name}-aws4-1",
    "${main::BASE_DEVICE}:${base_name}-chunked-${main::BASE_API}-1",
    "${main::BASE_DEVICE}:${base_name}-chunked-aws4-1",
    "${main::BASE_DEVICE}:${base_name}-multip-${main::BASE_API}-1",
    "${main::BASE_DEVICE}:${base_name}-multip-aws4-1",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-2",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-3",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-4",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-5",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-6",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-eu",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-wild",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-ca",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-oca",
    "${main::BASE_DEVICE}:${base_name}-s3.eu-3",
    "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-eu-4",
    "${main::BASE_DEVICE}:TESTCONF-s3_eu_2"
    );

# for my $i (@dev_names) {
#     $dev_name = $i;
#     $dev = Amanda::Device->new($dev_name);
#     $dev->property_set('S3_ACCESS_KEY', $S3_ACCESS_KEY) && diag($dev->error_or_status());
#     $dev->property_set('S3_SECRET_KEY', $S3_SECRET_KEY) && diag($dev->error_or_status());
#     diag("$dev_name: erase device twice");
#     $dev->erase() && diag($dev->error_or_status());
#     $dev->erase() && diag($dev->error_or_status());
# }

SKIP: {
    skip "define \$INSTALLCHECK_S3_{SECRET,ACCESS}_KEY to run tests",
            104 +
            1 * $verify_file_count +
            7 * $write_file_count +
            14 * $make_device_count
	unless $run_s3_tests;

    $dev_name = "${main::BASE_DEVICE}:";
    $dev = Amanda::Device->new($dev_name);
    isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
         "creating $dev_name fails miserably");

    $dev_name = "${main::BASE_DEVICE}:foo";
    $dev = Amanda::Device->new($dev_name);

    ok($dev->property_get("full_deletion"),
       "property_get(full_deletion) on $main::BASE_DEVICE device");

    ok($dev->property_get("leom"),
       "property_get(leom) on $main::BASE_DEVICE device");

    # test parsing of boolean values
    # (s3 is the only device driver that has a writable boolean property at the
    # moment)

    # test unparsable property
    $dev_name = "$BASE_DEVICE:foo";
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

    my $hdr;
    my $status;

    $dev_name = "$BASE_DEVICE:${base_name}-multip-${main::BASE_API}-1";
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose multi-part }, $main::blksize );

    $dev->read_label();
    $status = $dev->status();
    # this test appears very liberal, but catches the case where setup_handle fails without
    # giving false positives

    ok(($status == $DEVICE_STATUS_SUCCESS) || (($status & $DEVICE_STATUS_VOLUME_UNLABELED) != 0),
       "status is either OK or possibly unlabeled (#1)")
        or diag($dev->error_or_status());

    ###################################################
    #### write four times with multip

    SKIP: {
	#skip "skip over", 0;
	skip "Azure cannot support AWS4", 0
           if ($main::BASE_DEVICE eq "azure");
	skip "Swift3 cannot support AWS4 or multipart", 0
           if ($main::BASE_API eq "swift3");
	skip "Google cannot support AWS4 with multipart", 0
           if ($S3_HOST =~ m{googleapis.com$});

        # $main::blksize = 50 * 0x100000;
        # $main::nblks = 10000;

	$dev_name = "${main::BASE_DEVICE}:${base_name}-multip-aws4-1";
	$dev = make_device($dev_name, "aws4", qw{ verbose multi-part }, $main::blksize );

	four_file_test($dev) || die("failed $dev_name test");
    }

    SKIP: {
	#skip "skip over", 0;
	skip "Swift3 cannot support multipart", 0
           if ($main::BASE_API eq "swift3");

	$dev_name = "${main::BASE_DEVICE}:${base_name}-multip-${main::BASE_API}-1";
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose multi-part }, $main::blksize );

	four_file_test($dev) || die("failed $dev_name test");
    }

    ###################################################
    #### write three times with chunked-stream

    SKIP: {
	skip "Chunked is not supported by Amazon", 0;

	$dev_name = "${main::BASE_DEVICE}:${base_name}-chunked-${main::BASE_API}-1";
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose chunked });

	four_file_test($dev) || die("failed $dev_name test");
    }

    SKIP: {
	skip "Chunked is not supported by Amazon", 0;

	$dev_name = "${main::BASE_DEVICE}:${base_name}-chunked-aws4-1";
	$dev = make_device($dev_name, "aws4", qw{ verbose chunked });

	four_file_test($dev) || die("failed $dev_name test");
    }

    ###################################################
    #### write three times with block mode

    SKIP: {
        # more interesting results as this is a default for Amazon
	skip "Swift3 cannot support AWS4", 0
           if ($main::BASE_API eq "swift3");
	skip "Azure cannot support AWS4", 0
           if ($main::BASE_DEVICE eq "azure");

	$dev_name = "${main::BASE_DEVICE}:${base_name}-aws4-1";
	$dev = make_device($dev_name, "aws4", qw{ verbose }, $main::blksize );

	four_file_test($dev) || die("failed $dev_name test");
    }

    SKIP: {
    	# all should accept this mode

	$dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-1";
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose }, $main::blksize );

	four_file_test($dev) || die("failed $dev_name test");
    }

    ########################################################################

    SKIP: {
        # more interesting results as this is a default for Amazon
	skip "Swift3 cannot support AWS4", 0
           if ($main::BASE_API eq "swift3");
	skip "Azure cannot support AWS4", 0
           if ($main::BASE_DEVICE eq "azure");

	$dev_name = "${main::BASE_DEVICE}:${base_name}-aws4-1";
	$dev = make_device($dev_name, "aws4", qw{ verbose }); # small blksize

	four_file_test($dev) || die("failed $dev_name test");
    }

    SKIP: {
    	# all should accept this mode

	$dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-1";
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose }); # small blksize

	four_file_test($dev) || die("failed $dev_name test");
    }

    ########################################################################

    $dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-2";
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

    ok($dev->erase(),
       "$dev_name: erase device right after creation")
       or diag($dev->error_or_status());

    $dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-3";
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

    # set MAX_VOLUME_USAGE, LEOM=true, ENFORCE_MAX_VOLUME_USAGE=false
    is($dev->property_set('MAX_VOLUME_USAGE', "512k"), undef,
       "$dev_name: set MAX_VOLUME_USAGE to test LEOM");

    is($dev->property_set("LEOM", 1), undef,
        "set LEOM");

    ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef), 
       "$dev_name: start in write mode")
        or diag($dev->error_or_status());

    write_file(0x2FACE, 440*1024, 1);

    ok(!$dev->is_eom,
        "device does not indicate LEOM after writing as property ENFORCE_MAX_VOLUME_USAGE not set and its default value is false");

    ok($dev->finish(),
       "$dev_name: finish device after LEOM test")
       or diag($dev->error_or_status());
    
    ok($dev->erase(),
       "$dev_name: erase device")
       or diag($dev->error_or_status());
    
    $dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-4";
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

    # set MAX_VOLUME_USAGE, LEOM=true, ENFORCE_MAX_VOLUME_USAGE=true
    is($dev->property_set('MAX_VOLUME_USAGE', "512k"), undef,
       "set MAX_VOLUME_USAGE to test LEOM");

    is($dev->property_set('ENFORCE_MAX_VOLUME_USAGE', 1 ), undef,
       "set ENFORCE_MAX_VOLUME_USAGE");

    is($dev->property_set("LEOM", 1), undef,
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
    
    $dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-5";
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

    # set MAX_VOLUME_USAGE, LEOM=true, ENFORCE_MAX_VOLUME_USAGE=false
    is($dev->property_set('MAX_VOLUME_USAGE', "512k"), undef,
       "set MAX_VOLUME_USAGE to test LEOM");

    is($dev->property_set('ENFORCE_MAX_VOLUME_USAGE', 0 ), undef,
       "set ENFORCE_MAX_VOLUME_USAGE");

    is($dev->property_set("LEOM", 1), undef,
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
    
    SKIP: {
	skip "Swift3 cannot support AWS4", 0
	   if ($main::BASE_API eq "swift3");
	skip "Swift3 cannot support AWS4", 0
	   if ($main::BASE_DEVICE eq "azure");

	# try with empty user token
	$dev_name = lc("${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-6");
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose });
	is($dev->property_set('S3_USER_TOKEN', ''), undef,
	   "set devpay user token")
	    or diag($dev->error_or_status());

	$dev->read_label();
	$status = $dev->status();
	ok(($status == $DEVICE_STATUS_SUCCESS) || (($status & $DEVICE_STATUS_VOLUME_UNLABELED) != 0),
	   "status is either OK or possibly unlabeled (#2)")
	    or diag($dev->error_or_status());

	$dev->finish();

	ok($dev->erase(),
	   "erase device")
	   or diag($dev->error_or_status());

	# try a eu-constrained bucket
	$dev_name = lc("${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-eu");
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose });
	is($dev->property_set('S3_BUCKET_LOCATION', 'EU'), undef,
	   "$dev_name: set S3 bucket location to 'EU'")
	    or diag($dev->error_or_status());

	ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
	   "$dev_name: start in write mode")
	    or diag($dev->error_or_status());

	is($dev->status(), $DEVICE_STATUS_SUCCESS,
	   "$dev_name: status is OK")
	    or diag($dev->error_or_status());

	$dev->finish();

	ok($dev->erase(),
	   "$dev_name: erase device")
	   or diag($dev->error_or_status());

	# try a wildcard-constrained bucket
	$dev_name = lc("${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-wild");
	$dev = make_device($dev_name, $main::BASE_API, qw{ verbose });
	is($dev->property_set('S3_BUCKET_LOCATION', '*'), undef,
	   "$dev_name: set S3 bucket location to '*'")
	    or diag($dev->error_or_status());

	ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
	   "$dev_name: start in write mode")
	    or diag($dev->error_or_status());

	is($dev->status(), $DEVICE_STATUS_SUCCESS,
	   "$dev_name: status is OK")
	    or diag($dev->error_or_status());

	$dev->finish();
	ok($dev->erase(),
	   "erase device")
	  or diag($dev->error_or_status());
    }

    # test again with invalid ca_info
    $dev_name = lc("${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-ca");
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });
    SKIP: {
	skip "SSL not supported; can't check SSL_CA_INFO", 2
	    unless ( defined($dev->property_get('SSL_CA_INFO')) );

	is($dev->property_set('SSL_CA_INFO', '/dev/null'), undef,
	   "$dev_name: set invalid SSL/TLS CA certificate")
	    or diag($dev->error_or_status());

        ok(!$dev->start($ACCESS_WRITE, "TESTCONF13", undef),
           "$dev_name: start in write mode")
            or diag($dev->error_or_status());

        isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
           "$dev_name: status is OK")
            or diag($dev->error_or_status());

        $dev->finish();
    }

    # test again with our own CA bundle
    $dev_name = lc("${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-oca");
    $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });
    SKIP: {
	skip "SSL not supported; can't check SSL_CA_INFO", 4
	    unless ( defined($dev->property_get('SSL_CA_INFO')) );

	is($dev->property_set('SSL_CA_INFO', "$abs_srcdir/data/aws-bundle.crt"), undef,
	   "set our own SSL/TLS CA certificate bundle")
	    or diag($dev->error_or_status());

        ok($dev->erase(),
           "$dev_name: erase device")
            or diag($dev->error_or_status());

        ok($dev->start($ACCESS_WRITE, "TESTCONF13", undef),
           "$dev_name: start in write mode")
            or diag($dev->error_or_status());

        is($dev->status(), $DEVICE_STATUS_SUCCESS,
           "$dev_name: status is OK")
            or diag($dev->error_or_status());

	$dev->finish();
    }

    ok($dev->erase(),
       "erase device")
       or diag($dev->error_or_status());

    SKIP: {
	skip "Swift3 cannot support AWS4", 0
	   if ($main::BASE_API eq "swift3");
	skip "Swift3 cannot support AWS4", 0
	   if ($main::BASE_DEVICE eq "azure");

        # bucket names incompatible with location constraint
        $dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-eu-2";
        $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

        is($dev->property_set('S3_BUCKET_LOCATION', ''), undef,
           "should be able to set an empty S3 bucket location with an incompatible name")
            or diag($dev->error_or_status());

        $dev_name = "${main::BASE_DEVICE}:${base_name}-s3.eu-3";
        $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

        is($dev->property_set('S3_BUCKET_LOCATION', ''), undef,
           "should be able to set an empty S3 bucket location with an incompatible name")
            or diag($dev->error_or_status());

        $dev_name = "${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-eu";
        $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

        is($dev->property_set('S3_BUCKET_LOCATION', 'EU'), 
           "Location constraint given for Amazon S3 bucket, but the bucket name (-${base_name}-${main::BASE_API}-eu) is not usable as a subdomain.",
           "should not be able to set S3 bucket location with an incompatible name")
            or diag($dev->error_or_status());

        $dev_name = lc("${main::BASE_DEVICE}:${base_name}-${main::BASE_API}-eu-4");
        $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

        is($dev->property_set('S3_BUCKET_LOCATION', 'XYZ'), undef,
           "should be able to set S3 bucket location with a compatible name")
            or diag($dev->error_or_status());
        $dev->read_label();
        $status = $dev->status();
        ok(($status == $DEVICE_STATUS_DEVICE_ERROR),
           "status is DEVICE_STATUS_DEVICE_ERROR")
            or diag($dev->error_or_status());
        my $error_msg = $dev->error_or_status();
        ok(($error_msg =~ m{^While creating new $BASE_DEVICE bucket: The specified location-constraint is not valid \(InvalidLocationConstraint\) \(HTTP 400\)}),
           "invalid location-constraint")
           or diag("bad error: $error_msg");

        $dev_name = "${main::BASE_DEVICE}:TESTCONF-${BASE_DEVICE}_eu_2";
        $dev = make_device($dev_name, $main::BASE_API, qw{ verbose });

        is($dev->property_set('S3_SUBDOMAIN', 'ON'), 
            'S3-SUBDOMAIN is set, but the bucket name (TESTCONF-s3_eu_2) is not usable as a subdomain.',
            "should be able to set an empty S3_SUBDOMAIN with an incompatible name")
            or diag($dev->error_or_status());
    }

}

unlink($input_filename);
unlink($output_filename);
rmtree($taperoot);
