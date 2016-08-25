# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 47;

use lib '@amperldir@';
use strict;
use warnings;
use Installcheck;
use Amanda::Constants;
use Amanda::Paths;
use File::Path;
use Installcheck::Application;
use IO::File;
use Data::Dumper;

unless ($Amanda::Constants::STAR and -x $Amanda::Constants::STAR) {
    SKIP: {
        skip("Star is not available", Test::More->builder->expected_tests);
    }
    exit 0;
}

$SIG{'PIPE'} = 'IGNORE';
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $app = Installcheck::Application->new('amstar');

my $support = $app->support();
is($support->{'INDEX-LINE'}, 'YES', "supports indexing");
is($support->{'MESSAGE-LINE'}, 'YES', "supports messages");
is($support->{'CALCSIZE'}, 'YES', "supports calcsize");

my $root_dir = "$Installcheck::TMP/installcheck-amstar";
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

my $star_tardump = "$Installcheck::TMP/star_tardump";
open XX, ">$star_tardump";
close XX;
chmod(0700, $star_tardump);
$app->add_property('STAR-TARDUMP' => $star_tardump);

my $selfcheck = $app->selfcheck_message('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok(!@{$selfcheck->{'errors'}}, "no errors during selfcheck") || diag(Data::Dumper::Dumper($selfcheck->{'errors'}));

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

my $restore = $app->restore('objects' => ['./sym', './bar'], 'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok");

ok(chdir($orig_cur_dir), "changed working directory (back to original)");

ok(-d "$rest_dir/sym", "sym/ restored");
ok(-d "$rest_dir/bar", "bar/ restored");
ok(-d "$rest_dir/bar", "bar/baz/bat/ restored");

$app->add_property('STAR-PATH' => '/do/not/exists');

$restore = $app->restore('objects' => ['./foo', './bar'], 'data' => $backup->{'data'}, data_sigpipe => 1);
is($restore->{'exit_status'}, 256, "error status of 1 if STAR-PATH does not exists");
chomp $restore->{'errs'};
if ($Amanda::Constants::SINGLE_USERID) {
    ok($restore->{'errs'} eq "amstar: error [exec /do/not/exists: No such file or directory]", "correct error for No such file or directory")
	or diag($restore->{'errs'});
} else {
    ok($restore->{'errs'} eq "Can't find real path for '/do/not/exists': No such file or directory", "correct error for No such file or directory")
	or diag($restore->{'errs'});
}

$selfcheck = $app->selfcheck_message('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
if ($Amanda::Constants::SINGLE_USERID) {
    ok($selfcheck->{'errors'}[0]->{code} eq '3600060' &&
       $selfcheck->{'errors'}[0]->{errnocode} eq 'ENOENT', "good error selfcheck ")
	or diag(Data::Dumper::Dumper(\@{$selfcheck->{'errors'}}));
} else {
    ok($selfcheck->{'errors'}[0]->{code} eq '3600091' &&
       $selfcheck->{'errors'}[0]->{errnocode} eq 'ENOENT', "good error selfcheck ")
	or diag(Data::Dumper::Dumper(\@{$selfcheck->{'errors'}}));
}

my $estimate = $app->estimate('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($estimate->{'exit_status'}, 0, "error status ok");
if ($Amanda::Constants::SINGLE_USERID) {
    ok($estimate->{'errors'}[0] eq 'no size line match in /do/not/exists output', "good error estimate")
	or diag(Data::Dumper::Dumper(\@{$estimate->{'errors'}}));
} else {
    ok($estimate->{'errors'}[0] eq 'Can\'t find real path for \'/do/not/exists\': No such file or directory', "good error estimate")
	or diag(Data::Dumper::Dumper(\@{$estimate->{'errors'}}));
}

$backup = $app->backup('device' => $back_dir, 'level' => 0, 'index' => 'line');
if ($Amanda::Constants::SINGLE_USERID) {
    is($backup->{'exit_status'}, 0, "error status ok");
    is($backup->{'errors'}[0], "amstar: error [exec /do/not/exists: No such file or directory]", "good error backup")
	or diag(Data::Dumper::Dumper(\@{$backup->{'errors'}}));
} else {
    is($backup->{'exit_status'}, 256, "error status ok");
    is($backup->{'errors'}[0], "Can't find real path for '/do/not/exists': No such file or directory", "good error backup")
	or diag(Data::Dumper::Dumper(\@{$backup->{'errors'}}));
}
$app->delete_property('STAR-PATH');

my $bad_star = "$Installcheck::TMP/bad-star";
open TOTO, ">$bad_star";
close TOTO;
$app->add_property('STAR-PATH', "$bad_star");

$selfcheck = $app->selfcheck_message('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
if ($Amanda::Constants::SINGLE_USERID) {
    ok($selfcheck->{'errors'}[0]->{code} eq '3600063' &&
       $selfcheck->{'errors'}[0]->{filename} eq "$bad_star", "good error selfcheck ")
	or diag("XX:". Data::Dumper::Dumper(\@{$selfcheck->{'errors'}}));
} else {
    ok($selfcheck->{'errors'}[0]->{code} eq '3600096' &&
       $selfcheck->{'errors'}[0]->{path} eq "$bad_star", "good error selfcheck ")
	or diag("XX:". Data::Dumper::Dumper(\@{$selfcheck->{'errors'}}));
}

$estimate = $app->estimate('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($estimate->{'exit_status'}, 0, "error status ok");
if ($Amanda::Constants::SINGLE_USERID) {
    is($estimate->{'errors'}[0], "no size line match in $bad_star output", "good error estimate")
	or diag(Data::Dumper::Dumper(\@{$estimate->{'errors'}}));
} else {
    is($estimate->{'errors'}[0], "security file do not allow to run '$bad_star' as root for 'amstar:star_path'", "good error estimate")
	or diag(Data::Dumper::Dumper(\@{$estimate->{'errors'}}));
}

$backup = $app->backup('device' => $back_dir, 'level' => 0, 'index' => 'line');
if ($Amanda::Constants::SINGLE_USERID) {
    is($backup->{'exit_status'}, 0, "error status ok");
    is($backup->{'errors'}[0], "amstar: error [exec $bad_star: Permission denied]", "good error backup")
	or diag(Data::Dumper::Dumper(\@{$backup->{'errors'}}));
} else {
    is($backup->{'exit_status'}, 256, "error status ok");
    is($backup->{'errors'}[0], "security file do not allow to run '$bad_star' as root for 'amstar:star_path'", "good error backup")
	or diag(Data::Dumper::Dumper(\@{$backup->{'errors'}}));
}
$app->delete_property('STAR-PATH');
unlink "$bad_star";

open XX, ">$star_tardump";
close XX;
chmod(0000, $star_tardump);

$selfcheck = $app->selfcheck_message('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok($selfcheck->{'errors'}[0]->{code} eq '3600063' &&
   $selfcheck->{'errors'}[0]->{errnocode} eq 'EACCES', "good error selfcheck ")
    or diag(Data::Dumper::Dumper(\@{$selfcheck->{'errors'}}));

$estimate = $app->estimate('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($estimate->{'exit_status'}, 0, "error status ok");
ok($estimate->{'errors'}[0] =~ /^can not access .*: Permission denied \(ruid:\d* euid:\d*\)$/, "good error estimate")
    or diag(Data::Dumper::Dumper(\@{$estimate->{'errors'}}));

$backup = $app->backup('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($backup->{'exit_status'}, 256, "error status ok");
ok($backup->{'errors'}[0] =~ /^can not access .*: Permission denied \(ruid:\d* euid:\d*\)$/, "good error backup")
    or diag(Data::Dumper::Dumper(\@{$backup->{'errors'}}));
chmod(0700, $star_tardump);

$app->add_property('sparse', 'bad-sparse');
$app->add_property('acl', 'bad-acl');
$selfcheck = $app->selfcheck_message('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok($selfcheck->{'errors'}[0]->{code} eq '3701008' &&
   $selfcheck->{'errors'}[1]->{code} eq '3701009', "good error selfcheck ")
    or diag(Data::Dumper::Dumper(\@{$selfcheck->{'errors'}}));

$estimate = $app->estimate('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($estimate->{'exit_status'}, 256, "error status ok");
is($estimate->{'errors'}[0], 'bad SPARSE property value \'bad-sparse\'', "good error estimate")
    or diag(Data::Dumper::Dumper(\@{$estimate->{'errors'}}));

$backup = $app->backup('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($backup->{'exit_status'}, 256, "error status ok");
is($backup->{'errors'}[0], 'bad SPARSE property value \'bad-sparse\'', "good error backup")
    or diag(Data::Dumper::Dumper(\@{$backup->{'errors'}}));

# cleanup
rmtree($root_dir);
