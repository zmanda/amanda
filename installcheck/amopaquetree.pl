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
use Amanda::Util;
use Fcntl qw(SEEK_SET);
use File::Path;
use Installcheck::Application;
use IO::File;
use IPC::Open3;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

sub rsync_is_unusable {
    my ( $self ) = @_;
    my ( $wtr, $rdr );
    my $pid = eval { open3($wtr, $rdr, undef, 'rsync', '--version') };
    return $@ if $@;
    close $wtr;
    my $output = do { local $/; <$rdr> };
    close $rdr;
    waitpid $pid, 0;
    return $output if $?;
    unless ( $output =~ qr/(?:^\s|,\s)hardlinks(?:,\s|$)/m ) {
        return 'rsync lacks hardlink support.';
    }
    unless ( $output =~ qr/(?:^\s|,\s)batchfiles(?:,\s|$)/m ) {
        return 'rsync lacks batchfile support.';
    }
    return 0; # hooray, it isn't unusable.
}

my $why = rsync_is_unusable();
unless ( $why ) {
    plan tests => 28;
} else {
    plan skip_all => $why;
}

my $app = Installcheck::Application->new('amopaquetree');

my $support = $app->support();
is($support->{'INDEX-LINE'}, 'YES', "supports indexing");
is($support->{'MESSAGE-LINE'}, 'YES', "supports messages");
is($support->{'CLIENT-ESTIMATE'}, 'YES', "supports estimates");
is($support->{'RECORD'}, 'YES', "supports record");
is($support->{'MULTI-ESTIMATE'}, 'YES', "supports multi-estimates");
is($support->{'CMD-STREAM'}, 'YES',
    "supports command stream to/from sendbackup");
is($support->{'WANT-SERVER-BACKUP-RESULT'}, 'YES',
    "supports server backup results");

my $root_dir = "$Installcheck::TMP/installcheck-amopaquetree";
my $back_dir = "$root_dir/to_backup";
my $rest_dir = "$root_dir/restore";
my $file1 = "file1";
my $file2 = "file2";

File::Path::mkpath($back_dir);
File::Path::mkpath($rest_dir);

Amanda::Tests::write_random_file(0xabcde, 1024*256, "$back_dir/$file1");
Amanda::Tests::write_random_file(0xfedcb, 1024*256, "$back_dir/$file2");

my $selfcheck = $app->selfcheck('device' => $back_dir, 'level' => 0, 'index' => 'line');
is($selfcheck->{'exit_status'}, 0, "error status ok");
ok(!@{$selfcheck->{'errors'}}, "no errors during selfcheck")
    or diag(@{$selfcheck->{'errors'}});

my $backup = $app->backup('device' => $back_dir, 'level' => 0,
                          'index' => 'line', 'record' => undef);
is($backup->{'exit_status'}, 0, "error status ok");
ok(!@{$backup->{'errors'}}, "no errors during backup")
    or diag(@{$backup->{'errors'}});

my $size_rounded_up = 1024 * int(( length($backup->{'data'}) + 1023 ) / 1024);
is($size_rounded_up, $backup->{'size'}, "reported and actual size match");

ok(@{$backup->{'index'}}, "index is not empty");
is_deeply($backup->{'index'}, ["/"], "index is '/'");

# make a small modification somewhere inside file2 (just grab some of the
# random data from some spot in file1, to some other spot in file2)....
open my $fh1,  '<', $back_dir.'/'.$file1;
open my $fh2, '+<', $back_dir.'/'.$file2;
seek $fh1, 1024*23, SEEK_SET;
seek $fh2, 1024*57, SEEK_SET;
my $buf = Amanda::Util::full_read(fileno($fh1), 1024*32);
close $fh1;
Amanda::Util::full_write(fileno($fh2), $buf, 1024*32);
close $fh2;

my $backup1 = $app->backup('device' => $back_dir, 'level' => 1,
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
is($restore->{'exit_status'}, 0, "error status ok");
$restore = $app->restore('objects' => ['.'],'data' => $backup1->{'data'},
                         'level'=> 1);
is($restore->{'exit_status'}, 0, "error status ok (increment)");
ok(chdir($orig_cur_dir), "changed working directory (back to original)");

my $restore_file = "$rest_dir/$file1";
ok(-f "$restore_file", "file1 restored");
is(`cmp "$back_dir/$file1" $restore_file`, "", "file1 match");

$restore_file = "$rest_dir/$file2";
ok(-f "$restore_file", "file2 restored");
is(`cmp "$back_dir/$file2" $restore_file`, "", "file2 match");

# cleanup
#exit(1);
rmtree($root_dir);
