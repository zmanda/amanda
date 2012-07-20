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

use Test::More tests => 21;

use lib "@amperldir@";
use strict;
use warnings;
use Installcheck;
use Amanda::Constants;
use Amanda::Paths;
use File::Path;
use Installcheck::Application;
use IO::File;

unless ($Amanda::Constants::GNUTAR and -x $Amanda::Constants::GNUTAR) {
    SKIP: {
        skip("GNU tar is not available", Test::More->builder->expected_tests);
    }
    exit 0;
}

my $app = Installcheck::Application->new('amgtar');

my $support = $app->support();
is($support->{'INDEX-LINE'}, 'YES', "supports indexing");
is($support->{'MESSAGE-LINE'}, 'YES', "supports messages");
is($support->{'CALCSIZE'}, 'YES', "supports calcsize");

my $root_dir = "$Installcheck::TMP/installcheck-amgtar";
my $back_dir = "$root_dir/to_backup";
my $rest_dir = "$root_dir/restore";
my $list_dir = "$root_dir/list";

sub ok_foreach {
    my $code = shift @_;
    my $stringify = shift @_;
    my $name = shift @_;
    my @list = @_;

    my @errors;
    foreach my $elm (@list) {
        my $elm_str = $stringify? $stringify->($elm) : "$elm";
        push @errors, "on element $elm_str: $@" unless eval {$code->($elm); 1;};
    }
    unless (ok(!@errors, $name)) {
        foreach my $err (@errors) {
            diag($err);
        }
    }
}

ok_foreach(
    sub {
        my $dir = shift @_;
        rmtree($dir);
    },
    undef,
    "emptied directories",
    $back_dir, $rest_dir, $list_dir);

ok_foreach(
    sub {
        my $dir = shift @_;
        mkpath($dir);
    },
    undef,
    "create directories",
    $back_dir, $rest_dir, $list_dir);


my @dir_struct = (
    {'type' => 'f', 'name' => 'foo'},
    {'type' => 'd', 'name' => 'bar/baz/bat/'},
    {'type' => 'h', 'name' => 'hard', 'to' => 'foo'},
    {'type' => 's', 'name' => 'sym', 'to' => 'bar'},
    {'type' => 's', 'name' => 'a', 'to' => 'b'},
    {'type' => 's', 'name' => 'b', 'to' => 'a'},
);

ok_foreach(
    sub {
        my $obj = shift @_;

        if ($obj->{'type'} eq 'f') {
            my $fh = new IO::File("$back_dir/$obj->{'name'}", '>');
            ok($fh, "created file $obj->{'name'}");
            undef $fh;
        } elsif ($obj->{'type'} eq 'd') {
            mkpath("$back_dir/$obj->{'name'}");
        } elsif ($obj->{'type'} eq 'h') {
            link("$back_dir/$obj->{'to'}", "$back_dir/$obj->{'name'}") or die "$!";
        } elsif ($obj->{'type'} eq 's') {
            symlink("$obj->{'to'}", "$back_dir/$obj->{'name'}") or die "$!";
        } else {
            die "unknown object type $obj->{'type'} for $obj->{'name'}";
        }
    },
    sub {shift(@_)->{'name'}},
    "create directory structure",
    @dir_struct);

$app->add_property('gnutar-listdir', $list_dir);
# GNU tar on Solaris doesn't support this, so avoid it
$app->add_property('atime-preserve', 'no');

my $selfcheck = $app->selfcheck('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok(!@{$selfcheck->{'errors'}}, "no errors during selfcheck");

my $backup = $app->backup('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($backup->{'exit_status'}, 0, "error status ok");
ok(!@{$backup->{'errors'}}, "no errors during backup")
    or diag(@{$backup->{'errors'}});

is(length($backup->{'data'}), $backup->{'size'}, "reported and actual size match");

ok(@{$backup->{'index'}}, "index is not empty");
ok_foreach(
    sub {
        my $obj = shift @_;
        my $name = $obj->{'name'};
        die "missing $name" unless
            grep {"/$name" eq $_} @{$backup->{'index'}};
    },
    sub {shift(@_)->{'name'}},
    "index contains all names/paths",
    @dir_struct);

my $orig_cur_dir = POSIX::getcwd();
ok($orig_cur_dir, "got current directory");

ok(chdir($rest_dir), "changed working directory (for restore)");

my $restore = $app->restore('objects' => ['./foo', './bar'], 'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok");

ok(chdir($orig_cur_dir), "changed working directory (back to original)");

ok(-f "$rest_dir/foo", "foo restored");
ok(-d "$rest_dir/bar", "bar/ restored");
ok(-d "$rest_dir/bar", "bar/baz/bat/ restored");

# cleanup
rmtree($root_dir);
