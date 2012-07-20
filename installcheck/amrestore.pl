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

use Test::More tests => 39;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Mock;
use Installcheck::Run qw(run run_get run_get_err run_err
		$stderr $stdout $diskname vtape_dir);
use Installcheck::Dumpcache;
use File::Path qw(rmtree mkpath);
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Holding;
use Amanda::Config qw( :init :getconf );
use Amanda::Device qw( :constants );
use Amanda::Xfer qw( :constants );
use Amanda::MainLoop;
use Cwd;
use Data::Dumper;

my $testdir = "$Installcheck::TMP/amrestore-installcheck";
rmtree($testdir);
mkpath($testdir);

my $origdir = getcwd;

sub cleandir {
    for my $filename (<$testdir/*>) {
	unlink($filename);
    }
}

like(run_err('amrestore'),
    qr{Usage:},
    "'amrestore' gives usage message on stderr");

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

Installcheck::Dumpcache::load("multi");
Installcheck::Run::load_vtape(1);
chdir($testdir);
my @filenames;
my ($orig_size, $comp_size, $comp_best_size, $raw_size, $hdr_size);

cleandir();
like(run_get_err('amrestore', "file:".vtape_dir()),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "simple amrestore from VFS device");

@filenames = sort <localhost.*>;
is(scalar @filenames, 2, "..and the restored files are present in testdir")
    or diag(join("\n", @filenames));
like($filenames[0], qr/localhost\..*\.[0-9]{14}\.0/,
    "..first filename looks correct");
like($filenames[1], qr/localhost\..*_dir\.[0-9]{14}\.0/,
    "..second filename looks correct");

# get the size of the first file for later
$orig_size = (stat($filenames[0]))[7];

cleandir();
Installcheck::Run::load_vtape(2);
like(run_get_err('amrestore', "file:".vtape_dir()),
    qr{Restoring from tape TESTCONF02 starting with file 1.
amrestore: 1: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 1 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "simple amrestore from VFS device (tape 2)");

@filenames = sort <localhost.*>;
is(scalar @filenames, 2, "..and the restored files are present in testdir")
    or diag(join("\n", @filenames));
like($filenames[0], qr/localhost\..*\.[0-9]{14}\.0/,
    "..first filename looks correct");
like($filenames[1], qr/localhost\..*_dir\.[0-9]{14}\.1/,
    "..second filename looks correct");

cleandir();
Installcheck::Run::load_vtape(1);
like(run_get_err('amrestore', "file:".vtape_dir(), "otherhost"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "amrestore with a nonexistent hostname restores nothing");

cleandir();
like(run_get_err('amrestore', "file:".vtape_dir(), "localhost", "/NOSuCH/DIR"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "amrestore with a good hostname but non-matching dir restores nothing");

@filenames = <localhost.*>;
is(scalar @filenames, 0, "..and no restored files are present in testdir")
    or diag(join("\n", @filenames));

cleandir();
like(run_get_err('amrestore', "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "amrestore with a good hostname and matching dir restores one file");

@filenames = <localhost.*>;
is(scalar @filenames, 1, "..and the file is present in testdir")
    or diag(join("\n", @filenames));

cleandir();
like(run_get_err('amrestore', "-l", "TESTCONF01", "file:".vtape_dir(), "localhost", "$diskname/dir"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "label checking OK");

cleandir();
ok(run('amrestore', "-b", "16384", "file:".vtape_dir()),
    "blocksize option accepted (although with no particular effect on a VFS device)");

cleandir();
like(run_get_err('amrestore', "-c", "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "compression works");

@filenames = <localhost.*>;
$comp_size = (stat($filenames[0]))[7];
ok($comp_size < $orig_size, "..compressed size is smaller than original");

cleandir();
like(run_get_err('amrestore', "-C", "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "best compression works");

@filenames = <localhost.*>;
$comp_best_size = (stat($filenames[0]))[7];
ok($comp_best_size < $comp_size, "..compressed best size is smaller than compressed fast");

cleandir();
like(run_get_err('amrestore', "-f", "2", "file:".vtape_dir()),
    qr{Restoring from tape TESTCONF01 starting with file 2.},
    "starting at filenum 2 gets no dumps");

ok(run_err('amrestore', "-c", "-r", "file:".vtape_dir()),
    "-c and -r conflict");

cleandir();
like(run_get_err('amrestore', "-r", "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "raw restore");

@filenames = <localhost.*>;
is(scalar @filenames, 1, "..and restored file is present in testdir")
    or diag(join("\n", @filenames));

# get the size of the file for later
$raw_size = (stat($filenames[0]))[7];

is($raw_size, $orig_size + Amanda::Holding::DISK_BLOCK_BYTES,
    "raw dump is orig_size + header size");

cleandir();
like(run_get_err('amrestore', "-h", "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 0 comp N program .*
amrestore: 2: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E part 1/UNKNOWN lev 0 comp N program .*},
    "header (-h) restore");

@filenames = <localhost.*>;
is(scalar @filenames, 1, "..and restored file is present in testdir")
    or diag(join("\n", @filenames));

# get the size of the file for later
$hdr_size = (stat($filenames[0]))[7];

is($hdr_size, $orig_size + Amanda::Holding::DISK_BLOCK_BYTES,
    "header (-h) dump is orig_size + header size");

# find the holding files
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
@filenames = sort(+Amanda::Holding::files());
is(scalar @filenames, 2, "two holding files found") or die("holding is not what I thought");
my $holding_filename = $filenames[0];

cleandir();
like(run_get_err('amrestore', $holding_filename),
    qr{Reading from '\Q$holding_filename\E'
FILE: date [0-9]* host localhost disk \Q$diskname\E lev 1 comp N program .*},
    "simple amrestore from holding disk");

@filenames = <localhost.*>;
is(scalar @filenames, 1, "..and restored file is present in testdir")
    or diag(join("\n", @filenames));

cleandir();
Installcheck::Run::load_vtape(2);
like(run_get_err('amrestore', "-h", "-p", "file:".vtape_dir(), "localhost", "$diskname/dir"),
    qr{Restoring from tape TESTCONF02 starting with file 1.
amrestore: 1: restoring split dumpfile: date [0-9]* host localhost disk \Q$diskname\E/dir part 1/UNKNOWN lev 1 comp N program .*},
    "piped amrestore");
# (note that amrestore does not go on to the next part; it used to, but would either skip or
# give an error for every subsequent file)

@filenames = <localhost.*>;
is(scalar @filenames, 0, "..leaves no files in current dir")
    or diag(join("\n", @filenames));

like($stdout, qr/AMANDA: /, "..and stdout contains something with a header");

####
# For DirectTCP, we write a dumpfile to disk manually with the NDMP device, and
# then use amrestore to get it.

SKIP: {
    skip "not built with ndmp and server", 5 unless
	Amanda::Util::built_with_component("ndmp") and Amanda::Util::built_with_component("server");

    my $ndmp = Installcheck::Mock::NdmpServer->new();
    my $port = $ndmp->{'port'};
    my $drive = $ndmp->{'drive'};

    # set up a header for use below
    my $hdr = Amanda::Header->new();
    $hdr->{type} = $Amanda::Header::F_SPLIT_DUMPFILE;
    $hdr->{datestamp} = "20091220000000";
    $hdr->{dumplevel} = 0;
    $hdr->{compressed} = 0;
    $hdr->{comp_suffix} = 'N';
    $hdr->{name} = "localhost";
    $hdr->{disk} = "/home";
    $hdr->{program} = "INSTALLCHECK";

    my $device_name = "ndmp:127.0.0.1:$port\@$drive";
    my $dev = Amanda::Device->new($device_name);
    ($dev->status() == $DEVICE_STATUS_SUCCESS)
	or die "creation of an ndmp device failed: " . $dev->error_or_status();

    $dev->start($ACCESS_WRITE, "TEST-17", "20091220000000")
	or die "could not start NDMP device in write mode: " . $dev->error_or_status();

    $dev->start_file($hdr),
	or die "could not start_file: " . $dev->error_or_status();

    {   # write to the file
	my $xfer = Amanda::Xfer->new([
		Amanda::Xfer::Source::Random->new(32768*40+280, 0xEEEEE),
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
    }

    $dev->finish()
	or die "could not finish device: " . $dev->error_or_status();
    undef $dev;

    pass("wrote a file to an NDMP device");

    cleandir();
    like(run_get_err('amrestore', $device_name),
	qr{Restoring from tape TEST-17 starting with file 1.
amrestore: 1: restoring split dumpfile: date [0-9]* host localhost disk /home part 1/UNKNOWN lev 0 comp N program .*},
	"simple amrestore from NDMP device");

    @filenames = sort <localhost.*>;
    is(scalar @filenames, 1, "..and the restored file is present in testdir")
	or diag(join("\n", @filenames));
    like($filenames[0], qr/localhost\..*\.[0-9]{14}\.0/,
	".. filename looks correct");

    # get the size of the first file for later
    is((stat($filenames[0]))[7], 32768*41, # note: last block is padded
	".. file length is correct");

    # let's just assume the contents of the file are correct, ok?
}

chdir("$testdir/..");
rmtree($testdir);
Installcheck::Run::cleanup();
