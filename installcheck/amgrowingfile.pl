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

use Test::More tests => 28;

use lib '@amperldir@';
use strict;
use warnings;
use Installcheck;
use Amanda::Constants;
use Amanda::Debug;
use Amanda::Paths;
use Amanda::Tests;
use File::Path;
use Installcheck::Application;
use IO::File;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $app = Installcheck::Application->new('amgrowingfile');

my $support = $app->support();
is($support->{'INDEX-LINE'}, 'YES', "supports indexing");
is($support->{'MESSAGE-LINE'}, 'YES', "supports messages");
is($support->{'CLIENT-ESTIMATE'}, 'YES', "supports estimates");
is($support->{'RECORD'}, 'YES', "supports record");
is($support->{'MULTI-ESTIMATE'}, 'YES', "supports multi-estimates");

my $root_dir = "$Installcheck::TMP/installcheck-amgrowingfile";
my $back_file = "$root_dir/to_backup";
my $rest_dir = "$root_dir/restore";

File::Path::mkpath($root_dir);
File::Path::mkpath($rest_dir);
Amanda::Tests::write_random_file(0xabcde, 1024*256, $back_file);

my $selfcheck = $app->selfcheck('device' => $back_file, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok(!@{$selfcheck->{'errors'}}, "no errors during selfcheck");

my $backup = $app->backup('device' => $back_file, 'level' => 0,
                          'index' => 'line', 'record' => undef);
is($backup->{'exit_status'}, 0, "error status ok");
ok(!@{$backup->{'errors'}}, "no errors during backup")
    or diag(@{$backup->{'errors'}});

is(length($backup->{'data'}), $backup->{'size'}, "reported and actual size match");

ok(@{$backup->{'index'}}, "index is not empty");
is_deeply($backup->{'index'}, ["/"], "index is '/'");

open my $bfh, '>>', $back_file;
print $bfh $backup->{'data'};
close $bfh;

my $backup1 = $app->backup('device' => $back_file, 'level' => 1,
                           'index' => 'line');
is($backup1->{'exit_status'}, 0, "error status ok");
ok(!@{$backup1->{'errors'}}, "no errors during backup")
    or diag(@{$backup1->{'errors'}});

is(length($backup1->{'data'}), $backup1->{'size'}, "reported and actual size match");

ok(@{$backup1->{'index'}}, "index is not empty");
is_deeply($backup1->{'index'}, ["/"], "index is '/'");

my $orig_cur_dir = POSIX::getcwd();
ok($orig_cur_dir, "got current directory");

ok(chdir($rest_dir), "changed working directory (for restore)");

my $restore = $app->restore('objects' => ['.'],'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok (default filename)");
$restore = $app->restore('objects' => ['.'],'data' => $backup1->{'data'},
                         'level'=> 1);
is($restore->{'exit_status'}, 0, "error status ok (default filename; increment)");

$app->add_property('filename', 'custom-filename');
$restore = $app->restore('objects' => ['.'],'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok (custom filename)");
$restore = $app->restore('objects' => ['.'],'data' => $backup1->{'data'},
                            'level'=> 1);
is($restore->{'exit_status'}, 0, "error status ok (custom filename; increment)");

ok(chdir($orig_cur_dir), "changed working directory (back to original)");

my $restore_file = "$rest_dir/amgrowingfile-restored";
ok(-f "$restore_file", "amgrowingfile-restored restored");
is(`cmp $back_file $restore_file`, "", "restore match");

$restore_file = "$rest_dir/custom-filename";
ok(-f "$restore_file", "custom-filename restored");
is(`cmp $back_file $restore_file`, "", "restore match");

# cleanup
#exit(1);
rmtree($root_dir);
