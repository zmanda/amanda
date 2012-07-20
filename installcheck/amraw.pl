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

use Test::More tests => 15;

use lib "@amperldir@";
use strict;
use warnings;
use Installcheck;
use Amanda::Constants;
use Amanda::Paths;
use Amanda::Tests;
use File::Path;
use Installcheck::Application;
use IO::File;

unless ($Amanda::Constants::GNUTAR and -x $Amanda::Constants::GNUTAR) {
    SKIP: {
        skip("GNU tar is not available", Test::More->builder->expected_tests);
    }
    exit 0;
}

my $app = Installcheck::Application->new('amraw');

my $support = $app->support();
is($support->{'INDEX-LINE'}, 'YES', "supports indexing");
is($support->{'MESSAGE-LINE'}, 'YES', "supports messages");

my $root_dir = "$Installcheck::TMP/installcheck-amraw";
my $back_file = "$root_dir/to_backup";
my $rest_dir = "$root_dir/restore";

File::Path::mkpath($root_dir);
File::Path::mkpath($rest_dir);
Amanda::Tests::write_random_file(0xabcde, 1024*256, $back_file);

my $selfcheck = $app->selfcheck('device' => $back_file, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok(!@{$selfcheck->{'errors'}}, "no errors during selfcheck");

my $backup = $app->backup('device' => $back_file, 'level' => 0, 'index' => 'line');
is($backup->{'exit_status'}, 0, "error status ok");
ok(!@{$backup->{'errors'}}, "no errors during backup")
    or diag(@{$backup->{'errors'}});

is(length($backup->{'data'}), $backup->{'size'}, "reported and actual size match");

ok(@{$backup->{'index'}}, "index is not empty");
is_deeply($backup->{'index'}, ["/"], "index is '/'");

my $orig_cur_dir = POSIX::getcwd();
ok($orig_cur_dir, "got current directory");

ok(chdir($rest_dir), "changed working directory (for restore)");

my $restore = $app->restore('objects' => ['.'],'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok");

ok(chdir($orig_cur_dir), "changed working directory (back to original)");

my $restore_file = "$rest_dir/amraw-restored";
ok(-f "$restore_file", "amraw-restored restored");
is(`cmp $back_file $restore_file`, "", "restore match");

# cleanup
#exit(1);
rmtree($root_dir);
