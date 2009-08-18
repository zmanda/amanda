# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 34;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_get_err run_err
		$stderr $stdout $diskname vtape_dir);
use Installcheck::Dumpcache;
use File::Path qw(rmtree mkpath);
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Holding;
use Amanda::Config qw( :init :getconf );
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

Installcheck::Dumpcache::load("multi");
Installcheck::Run::load_vtape(1);
chdir($testdir);
my @filenames;
my ($orig_size, $comp_size, $comp_best_size, $raw_size, $hdr_size);

cleandir();
like(run_get_err('amrestore', "file:".vtape_dir()),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
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
amrestore: 1: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 1 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
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
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
    "amrestore with a nonexistent hostname restores nothing");

cleandir();
like(run_get_err('amrestore', "file:".vtape_dir(), "localhost", "/NOSuCH/DIR"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
    "amrestore with a good hostname but non-matching dir restores nothing");

@filenames = <localhost.*>;
is(scalar @filenames, 0, "..and no restored files are present in testdir")
    or diag(join("\n", @filenames));

cleandir();
like(run_get_err('amrestore', "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
    "amrestore with a good hostname and matching dir restores one file");

@filenames = <localhost.*>;
is(scalar @filenames, 1, "..and the file is present in testdir")
    or diag(join("\n", @filenames));

cleandir();
like(run_get_err('amrestore', "-l", "TESTCONF01", "file:".vtape_dir(), "localhost", "$diskname/dir"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
    "label checking OK");

cleandir();
ok(run('amrestore', "-b", "16384", "file:".vtape_dir()),
    "blocksize option accepted (although with no particular effect on a VFS device)");

cleandir();
like(run_get_err('amrestore', "-c", "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
    "compression works");

@filenames = <localhost.*>;
$comp_size = (stat($filenames[0]))[7];
ok($comp_size < $orig_size, "..compressed size is smaller than original");

cleandir();
like(run_get_err('amrestore', "-C", "file:".vtape_dir(), "localhost", "$diskname\$"),
    qr{Restoring from tape TESTCONF01 starting with file 1.
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
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
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
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
amrestore: 1: skipping FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 0 comp N program .*
amrestore: 2: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E lev 0 comp N program .*},
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
@filenames = sort(Amanda::Holding::files());
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
amrestore: 1: restoring FILE: date [0-9]* host localhost disk \Q$diskname\E/dir lev 1 comp N program .*},
    "piped amrestore");
# (note that amrestore does not go on to the next part; it used to, but would either skip or
# give an error for every subsequent file)

@filenames = <localhost.*>;
is(scalar @filenames, 0, "..leaves no files in current dir")
    or diag(join("\n", @filenames));

like($stdout, qr/AMANDA: /, "..and stdout contains something with a header");

chdir("$testdir/..");
rmtree($testdir);
Installcheck::Run::cleanup();
