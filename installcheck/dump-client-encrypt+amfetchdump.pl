# Copyright (c) 2014-2016 Carbonite, Inc.  All Rights Reserved.
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
use File::Path;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag check_amreport check_amstatus);
use Installcheck::Catalogs;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

eval 'use Installcheck::Rest;';
if ($@) {
    plan skip_all => "Can't load Installcheck::Rest: $@";
    exit 1;
}

eval "require Time::HiRes;";

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $rest = Installcheck::Rest->new();
if ($rest->{'error'}) {
   plan skip_all => "Can't start JSON Rest server: $rest->{'error'}: see " . Amanda::Debug::dbfn();
   exit 1;
}
plan tests => 36;

my $reply;

my $config_dir = $Amanda::Paths::CONFIG_DIR;
my $amperldir = $Amanda::Paths::amperldir;
my $testconf;
my $diskfile;
my $infodir;
my $timestamp;
my $tracefile;
my $logfile;
my $hostname = `hostname`;
chomp $hostname;

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
$testconf->add_param('columnspec', '"Dumprate=1:-8:1,TapeRate=1:-8:1"');
$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
#    compress client custom
#    client-custom-compress  "$Amanda::Constants::COMPRESS_PATH"
    encrypt client
    client-encrypt  "$Amanda::Constants::COMPRESS_PATH"
    client-decrypt-option "-d"
    application {
        plugin "amrandom"
        property "SIZE" "262144"
    }
    holdingdisk never
}
EODLE
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
$diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
$infodir = getconf($CNF_INFOFILE);

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs/amdump","");
foreach my $message (@{$reply->{'body'}}) {
    if (defined $message and defined $message->{'code'}) {
        if ($message->{'code'} == 2000003) {
            $timestamp = $message->{'timestamp'};
        }
        if ($message->{'code'} == 2000001) {
            $tracefile = $message->{'tracefile'};
        }
        if ($message->{'code'} == 2000000) {
            $logfile = $message->{'logfile'};
        }
    }
}
#wait until it is done
do {
    Time::HiRes::sleep(0.5);
    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
    } while ($reply->{'body'}[0]->{'code'} == 2000004 and
             $reply->{'body'}[0]->{'status'} ne 'done');

# get REST report
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$logfile");
is($reply->{'body'}->[0]->{'severity'}, 'success', 'severity is success');
is($reply->{'body'}->[0]->{'code'}, '1900001', 'code is 1900001');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'hostname'}, $hostname , 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'exit_status'}, '0' , 'exit_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'status'}, 'done' , 'status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'org'}, 'DailySet1' , 'org is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'config_name'}, 'TESTCONF' , 'config_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'timestamp'}, $timestamp , 'timestamp is correct');
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[1], '  planner: Adding new disk localhost:diskname2.' , 'notes[1] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[2], '  taper: Slot 1 without label can be labeled' , 'notes[2] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
#is($reply->{'body'}->[0]->{'report'}->{'notes'}->[3], '  taper: tape TESTCONF01 kb 94 fm 1 [OK]' , 'notes[3] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
ok(!exists $reply->{'body'}->[0]->{'report'}->{'notes'}->[4], 'no notes[4]') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'nb'}, '1' , 'one dle on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'nc'}, '1' , 'one part on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'tape_label'}, 'TESTCONF01' , 'label tape_label on tape 0');
#is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'size'}, '94' , 'size 94  on tape 0');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1], 'only one tape');
is_deeply($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'TESTCONF'}->{'use'}, [ 'TESTCONF01'], 'use TESTCONF');
#is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tape_size'}, { 'full' => '94',
#									     'total' => '94',
#									     'incr' => '0' }, 'tape_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'parts_taped'}, { 'full' => '1',
									       'total' => '1',
									       'incr' => '0' }, 'parts_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_taped'}, { 'full' => '1',
									      'total' => '1',
									      'incr' => '0' }, 'dles_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_dumped'}, { 'full' => '1',
									       'total' => '1',
									       'incr' => '0' }, 'dles_dumped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'original_size'}, { 'full' => '256',
									         'total' => '256',
									         'incr' => '0' }, 'original_size is correct');
#is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'output_size'}, { 'full' => '94',
#									       'total' => '94',
#									       'incr' => '0' }, 'output_size is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dumpdisks'}, '', 'dumpdisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapedisks'}, '', 'tapedisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapeparts'}, '', 'tapeparts is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'backup_level'}, '0', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'disk_name'}, 'diskname2', 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'hostname'}, 'localhost', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_orig_kb'}, '256', 'dump_orig_kb is correct');
#is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_out_kb'}, '94', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dle_status'}, 'full', 'dle_status is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'summary'}->[1], 'Only one summary');

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?tracefile=$tracefile");
is($reply->{'body'}->[0]->{'severity'}, 'info', 'severity is info');
is($reply->{'body'}->[0]->{'code'}, '1800000', 'code is 1800000');
is($reply->{'body'}->[0]->{'status'}->{'dead_run'}, '1', 'dead_run is correct');
is($reply->{'body'}->[0]->{'status'}->{'exit_status'}, '0', 'exit_status is correct');
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'chunk_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'dump_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'storage'}->{'TESTCONF'}->{'taper_time'} = undef;
is($reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'status'}, '21', 'status 21 is good');
is($reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'storage'}->{'TESTCONF'}->{'status'}, '21', 'storage status 21 is good');

$rest->stop();

ok(run('amfetchdump', 'TESTCONF', 'localhost'),
    "amfetchdump restore a client compressed dump");

#Installcheck::Run::cleanup();
