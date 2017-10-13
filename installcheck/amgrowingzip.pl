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

use Test::More;

use lib '@amperldir@';
use strict;
use warnings;
use Installcheck;
use Amanda::Constants;
use Amanda::Debug;
use Amanda::Paths;
use Amanda::Tests;
use Fcntl qw(SEEK_SET);
use File::Path;
use Installcheck::Application;
use IO::File;

eval {
    require Archive::Zip;
    plan tests => 31;
    1;
} or do {
    plan skip_all => 'tested only if Archive::Zip is installed';
};

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $app = Installcheck::Application->new('amgrowingzip');

my $support = $app->support();
is($support->{'INDEX-LINE'}, 'YES', "supports indexing");
is($support->{'MESSAGE-LINE'}, 'YES', "supports messages");
is($support->{'CLIENT-ESTIMATE'}, 'YES', "supports estimates");
is($support->{'RECORD'}, 'YES', "supports record");
is($support->{'MULTI-ESTIMATE'}, 'YES', "supports multi-estimates");

my $root_dir = "$Installcheck::TMP/installcheck-amgrowingzip";
my $back_file = "$root_dir/to_backup";
my $rest_dir = "$root_dir/restore";

File::Path::mkpath($root_dir);
File::Path::mkpath($rest_dir);

# Create a test ZIP archive whose members will consist of random stuff;
# Amanda::Tests::write_random_file only writes an actual file. So (this
# is a test, it doesn't have to be pretty) just do that, then slurp in
# the file contents as $random_stuff; the file will get truncated and
# overwritten later as the actual ZIP archive.

Amanda::Tests::write_random_file(0xabcde, 1024*256, $back_file);

my $random_stuff;
{
    local $/;
    open my $fh, '<', $back_file;
    $random_stuff = <$fh>;
    close $fh;
}

# Ironically, while Perl's Archive::Zip provides all the functionality needed
# to do incremental backup of incrementally-growing ZIP files (a function to
# return the central directory offset is enough), it isn't adequate to actually
# CREATE incrementally-growing ZIP files (easy as that is in Python with the
# zipfile module). That makes creating the test case in Perl a bit tricky.
# Perl does, however, provide enough functionality to do the job backwards:
# write a two-entry ZIP file, then delete the second entry from the central
# directory, and rewrite the central directory at the original offset of the
# second entry in the ZIP file, leaving a one-entry ZIP. By first saving the
# raw bytes from the second entry offset to the end of the file before
# truncating, they can be written back later to emulate 'growing' the ZIP by
# adding the second entry. The things done in the name of testing....

my $zf = Archive::Zip->new();
my $thing1 = $zf->addString($random_stuff, 'Thing1');
my $thing2 = $zf->addString($random_stuff, 'Thing2');

open my $bfh, '+>', $back_file;
$zf->writeToFileHandle($bfh, 1);
ok($thing2->wasWritten(), "both entries in test zip were written");

# Backspace to the offset where thing2 begins, and save everything from there
# to the end of the file (including the directory records at the end). The
# writeLocalHeaderRelativeOffset method is just a getter, doesn't write stuff.

my $thing2offset = $thing2->writeLocalHeaderRelativeOffset();
seek $bfh, $thing2offset, SEEK_SET;
my $tail_data;
{
    local $/;
    $tail_data = <$bfh>;
}

# Remove thing2 from the in-memory directory of entries, then rewrite that
# directory at the offset where thing2 originally started in the file.

$zf->removeMember($thing2);
$zf->writeCentralDirectory($bfh, $thing2offset);
truncate $bfh, (tell $bfh);

# Wasn't that easy? Now it's a one-entry archive suitable for testing
# backup level 0.

close $bfh;

my $selfcheck = $app->selfcheck('device' => $back_file, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok(!@{$selfcheck->{'errors'}}, "no errors during selfcheck");

my $backup = $app->backup('device' => $back_file, 'level' => 0,
                          'index' => 'line', 'record' => undef);
is($backup->{'exit_status'}, 0, "error status ok");
ok(!@{$backup->{'errors'}}, "no errors during backup")
    or diag(@{$backup->{'errors'}});

my $size_rounded_up = 1024 * int(( length($backup->{'data'}) + 1023 ) / 1024);
is($size_rounded_up, $backup->{'size'}, "reported and actual size match");

ok(@{$backup->{'index'}}, "index is not empty");
is_deeply($backup->{'index'}, ["/"], "index is '/'");

# Ok, here it goes, back to a two-entry archive....

open $bfh, '+<', $back_file;
seek $bfh, $thing2offset, SEEK_SET;
print $bfh $tail_data;
close $bfh;

my $backup1 = $app->backup('device' => $back_file, 'level' => 1,
                           'index' => 'line');
is($backup1->{'exit_status'}, 0, "error status ok");
ok(!@{$backup1->{'errors'}}, "no errors during backup")
    or diag(@{$backup1->{'errors'}});

$size_rounded_up = 1024 * int(( length($backup1->{'data'}) + 1023 ) / 1024);
is($size_rounded_up, $backup1->{'size'}, "reported and actual size match");

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

$app->add_property('target', 'custom-filename');
$restore = $app->restore('objects' => ['.'],'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok (custom filename)");
$restore = $app->restore('objects' => ['.'],'data' => $backup1->{'data'},
                            'level'=> 1);
is($restore->{'exit_status'}, 0, "error status ok (custom filename; increment)");

ok(chdir($orig_cur_dir), "changed working directory (back to original)");

my $restore_file = "$rest_dir/amgrowingzip-restored";
ok(-f "$restore_file", "amgrowingzip-restored restored");
is(`cmp $back_file $restore_file`, "", "restore match");

$restore_file = "$rest_dir/custom-filename";
ok(-f "$restore_file", "custom-filename restored");
is(`cmp $back_file $restore_file`, "", "restore match");

$zf = Archive::Zip->new($restore_file);
isnt($zf, undef, "restored archive is present and well-formed");
is_deeply(\[$zf->memberNames()], \['Thing1', 'Thing2'],
          "contains the right entries");

# cleanup
#exit(1);
rmtree($root_dir);
