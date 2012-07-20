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

use Test::More tests => 20;
use strict;
use warnings;

# This test only puts the perl wrappers through their paces -- the underlying
# library is well-covered by amar-test.

use lib "@amperldir@";
use Installcheck;
use Amanda::Archive;
use Amanda::Paths;
use Data::Dumper;

my $arch_filename = "$Installcheck::TMP/amanda_archive.bin";
my $data_filename = "$Installcheck::TMP/some_data.bin";
my ($fh, $dfh, $ar, $f1, $f2, $a1, $a2, @res, $posn);

# some versions of Test::More will fail tests if the identity
# relationships of the two objects passed to is_deeply do not
# match, so we use the same object for $user_data throughout.
my $user_data = [ "x", "y", "z" ];

# set up a large file full of data

open($dfh, ">", $data_filename);
my $onek = "abcd" x 256;
my $onemeg = $onek x 1024;
for (my $i = 0; $i < 5; $i++) {
    print $dfh $onemeg;
}
$onek = $onemeg = undef;
close($dfh);

# utility functions for creating a "fake" archive file

sub make_header {
    my ($fh, $version) = @_;
    my $hdr = "AMANDA ARCHIVE FORMAT $version";
    $hdr .= "\0" x (28 - length $hdr);
    print $fh $hdr;
}

sub make_record {
    my ($fh, $filenum, $attrid, $data, $eoa) = @_;
    my $size = length($data);
    if ($eoa) {
	$size |= 0x80000000;
    }
    print $fh pack("nnN", $filenum, $attrid, $size);
    print $fh $data;
}

####
## TEST WRITING

open($fh, ">", $arch_filename) or die("opening $arch_filename: $!");
$ar = Amanda::Archive->new(fileno($fh), ">");
pass("Create a new archive");

$f1 = $ar->new_file("filename1");
pass("Start an archive file");

$a1 = $f1->new_attr(18);
$a1->add_data("foo!", 0);
$a2 = $f1->new_attr(19);
$a2->add_data("BAR!", 0);
$a1->add_data("FOO.", 1);
$a2->add_data("bar.", 0);
pass("Write some interleaved data");

$a1->close();
pass("Close an attribute with the close() method");

$a1 = Amanda::Archive::Attr->new($f1, 99);
pass("Create an attribute with its constructor");

open($dfh, "<", $data_filename);
$a1->add_data_fd(fileno($dfh), 1);
close($dfh);
pass("Add data from a file descriptor");

$a1 = undef;
pass("Close attribute when its refcount hits zero");

$f2 = Amanda::Archive::File->new($ar, "filename2");
pass("Add a new file (filename2)");

$a1 = $f2->new_attr(82);
$a1->add_data("word", 1);
pass("Add data to it");

$a2->add_data("barrrrr?", 0);	# note no EOA
pass("Add more data to first attribute");

($f1, $posn) = $ar->new_file("posititioned file", 1);
ok($posn > 0, "new_file returns a positive position");

$ar = undef;
pass("unref archive early");

($ar, $f1, $f2, $a1, $a2) = ();
pass("Close remaining objects");

close($fh);

####
## TEST READING

open($fh, ">", $arch_filename);
make_header($fh, 1);
make_record($fh, 16, 0, "/etc/passwd", 1);
make_record($fh, 16, 20, "root:foo", 1);
make_record($fh, 16, 21, "boot:foot", 0);
make_record($fh, 16, 22, "dustin:snazzy", 1);
make_record($fh, 16, 21, "..more-boot:foot", 1);
make_record($fh, 16, 1, "", 1);
close($fh);

open($fh, "<", $arch_filename);
$ar = Amanda::Archive->new(fileno($fh), "<");
pass("Create a new archive for reading");

@res = ();
$ar->read(
    file_start => sub {
	push @res, [ "file_start", @_ ];
	return "cows";
    },
    file_finish => sub {
	push @res, [ "file_finish", @_ ];
    },
    0 => sub {
	push @res, [ "frag", @_ ];
	return "ants";
    },
    user_data => $user_data,
);
is_deeply([@res], [
	[ 'file_start', $user_data, 16, '/etc/passwd' ],
        [ 'frag', $user_data, 16, "cows", 20, undef, 'root:foo', 1, 0 ],
        [ 'frag', $user_data, 16, "cows", 21, undef, 'boot:foot', 0, 0 ],
        [ 'frag', $user_data, 16, "cows", 22, undef, 'dustin:snazzy', 1, 0 ],
        [ 'frag', $user_data, 16, "cows", 21, "ants", '..more-boot:foot', 1, 0 ],
        [ 'file_finish', $user_data, "cows", 16, 0 ]
], "simple read callbacks called in the right order")
    or diag(Dumper(\@res));
$ar->close();
close($fh);


open($fh, "<", $arch_filename);
$ar = Amanda::Archive->new(fileno($fh), "<");
pass("Create a new archive for reading");

@res = ();
$ar->read(
    file_start => sub {
	push @res, [ "file_start", @_ ];
	return "IGNORE";
    },
    file_finish => sub {
	push @res, [ "file_finish", @_ ];
    },
    0 => sub {
	push @res, [ "frag", @_ ];
	return "ants";
    },
    user_data => $user_data,
);
is_deeply([@res], [
	[ 'file_start', $user_data, 16, '/etc/passwd' ],
], "'IGNORE' handled correctly")
    or diag(Dumper(\@res));
# TODO: check that file data gets dumped appropriately?


open($fh, "<", $arch_filename);
$ar = Amanda::Archive->new(fileno($fh), "<");

@res = ();
$ar->read(
    file_start => sub {
	push @res, [ "file_start", @_ ];
	return "dogs";
    },
    file_finish => sub {
	push @res, [ "file_finish", @_ ];
    },
    21 => [ 100, sub {
	push @res, [ "fragbuf", @_ ];
	return "pants";
    } ],
    0 => sub {
	push @res, [ "frag", @_ ];
	return "ants";
    },
    user_data => $user_data,
);
is_deeply([@res], [
	[ 'file_start', $user_data, 16, '/etc/passwd' ],
        [ 'frag', $user_data, 16, "dogs", 20, undef, 'root:foo', 1, 0 ],
        [ 'frag', $user_data, 16, "dogs", 22, undef, 'dustin:snazzy', 1, 0 ],
        [ 'fragbuf', $user_data, 16, "dogs", 21, undef, 'boot:foot..more-boot:foot', 1, 0 ],
        [ 'file_finish', $user_data, "dogs", 16, 0 ]
], "buffering parameters parsed correctly")
    or diag(Dumper(\@res));


open($fh, "<", $arch_filename);
$ar = Amanda::Archive->new(fileno($fh), "<");

@res = ();
eval {
    $ar->read(
	file_start => sub {
	    push @res, [ "file_start", @_ ];
	    die "uh oh";
	},
	user_data => $user_data,
    );
};
like($@, qr/uh oh at .*/, "exception propagated correctly");
is_deeply([@res], [
	[ 'file_start', $user_data, 16, '/etc/passwd' ],
], "file_start called before exception was rasied")
    or diag(Dumper(\@res));
$ar->close();

unlink($data_filename);
unlink($arch_filename);
