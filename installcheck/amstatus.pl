# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 7;

use lib "@amperldir@";
use Installcheck::Run qw( run run_get );
use Amanda::Paths;
use Amanda::Constants;

my $filename="$AMANDA_TMPDIR/installcheck-amdump.1";
my $testconf = Installcheck::Run::setup();
$testconf->write();

# read __DATA__ to a hash, keyed by the names following '%%%%'
my %logfiles;
my $key = undef;
while (<DATA>) {
    if (/^%%%% (.*)/) {
	$key = $1;
    } else {
	$logfiles{$key} .= $_;
    }
}

sub write_logfile {
    my ($data) = @_;
    open(my $fh, ">", $filename) or die("Could not open '$filename' for writing");
    print $fh $data;
    close($fh);
};

## try a few various options with a pretty normal logfile

write_logfile($logfiles{'normal'});

ok(run('amstatus', 'TESTCONF', '--file', $filename),
    "plain amstatus runs without error");
like($Installcheck::Run::stdout,
    qr{clienthost:/some/dir\s*0\s*100k\s*finished\s*\(13:01:53\)},
    "output is reasonable");

ok(run('amstatus', 'TESTCONF', '--file', $filename, '--summary'),
    "amstatus --summary runs without error");
unlike($Installcheck::Run::stdout,
    qr{clienthost:/some/dir\s*0\s*100k\s*finished\s*\(13:01:53\)},
    "output does not contain the finished dump");
like($Installcheck::Run::stdout,
    qr{taped\s+:\s+1\s+},
    "output contains summary info");

## now test a file with spaces and other funny characters in filenames

write_logfile($logfiles{'quoted'});

ok(run('amstatus', 'TESTCONF', '--file', $filename),
    "amstatus runs without error with quoted disknames");
like($Installcheck::Run::stdout,
    # note that amstatus' output is quoted, so backslashes are doubled
    qr{clienthost:"C:\\\\Some Dir\\\\"\s*0\s*100k\s*finished\s*\(13:01:53\)},
    "output is correct");

unlink($filename);

__DATA__
%%%% normal
amdump: start at Wed Jun 18 13:01:47 EDT 2008
amdump: datestamp 20080618
amdump: starttime 20080618130147
amdump: starttime-locale-independent 2008-06-18 13:01:47 EDT
planner: pid 4079 executable /path/to/planner version 9.8.7
READING CONF INFO...
planner: timestamp 20080618130147
planner: time 0.000: startup took 0.000 secs

SENDING FLUSHES...
ENDFLUSH

SETTING UP FOR ESTIMATES...
planner: time 0.000: setting up estimates for clienthost:/some/dir
clienthost:/some/dir overdue 14049 days for level 0
setup_estimate: clienthost:/some/dir: command 0, options: none    last_level -1 next_level0 -14049 level_days 0    getting estimates 0 (-2) -1 (-2) -1 (-2)
planner: time 0.000: setting up estimates took 0.000 secs

GETTING ESTIMATES...
driver: pid 4080 executable /path/to/driver version 9.8.7
driver: adding holding disk 0 dir /holding size 868352 chunksize 1048576
reserving 0 out of 868352 for degraded-mode dumps
driver: send-cmd time 0.015 to taper: START-TAPER 20080618130147
taper: pid 4084 executable taper version 9.8.7
driver: started dumper0 pid 4086
driver: send-cmd time 0.031 to dumper0: START 20080618130147
planner: time 0.050: got partial result for host clienthost disk /some/dir: 0 -> -2K, -1 -> -2K, -1 -> -2K
dumper: pid 4090 executable dumper1 version 9.8.7
driver: started dumper1 pid 4090
driver: send-cmd time 0.046 to dumper1: START 20080618130147
driver: started dumper2 pid 4094
driver: send-cmd time 0.048 to dumper2: START 20080618130147
driver: started dumper3 pid 4095
driver: send-cmd time 0.059 to dumper3: START 20080618130147
driver: start time 0.059 inparallel 4 bandwidth 600 diskspace 868352  dir OBSOLETE datestamp 20080618130147 driver: drain-ends tapeq FIRST big-dumpers sssS
dumper: pid 4094 executable dumper2 version 9.8.7
planner: time 0.088: got partial result for host clienthost disk /some/dir: 0 -> 100K, -1 -> -2K, -1 -> -2K
planner: time 0.091: got result for host clienthost disk /some/dir: 0 -> 100K, -1 -> -2K, -1 -> -2K
planner: time 0.091: getting estimates took 0.090 secs
FAILED QUEUE: empty
DONE QUEUE:
  0: clienthost     /some/dir

ANALYZING ESTIMATES...
pondering clienthost:/some/dir... next_level0 -14049 last_level -1 (due for level 0) (new disk, can't switch to degraded mode)
  curr level 0 nsize 100 csize 100 total size 208 total_lev0 100 balanced-lev0size 50
INITIAL SCHEDULE (size 208):
  clienthost /some/dir pri 14050 lev 0 nsize 100 csize 100

DELAYING DUMPS IF NEEDED, total_size 208, tape length 102400 mark 4
  delay: Total size now 208.

PROMOTING DUMPS IF NEEDED, total_lev0 100, balanced_size 50...
planner: time 0.091: analysis took 0.000 secs

GENERATING SCHEDULE:
--------
DUMP clienthost ffffffff9ffeffffffff1f /some/dir 20080618130147 14050 0 1970:1:1:0:0:0 100 100 0 1024
--------
dumper: pid 4086 executable dumper0 version 9.8.7
dumper: pid 4095 executable dumper3 version 9.8.7
taper: using label `Conf-001' date `20080618130147'
driver: result time 1.312 from taper: TAPER-OK
driver: state time 1.312 free kps: 600 space: 868352 taper: idle idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: not-idle
driver: interface-state time 1.312 if default: free 600
driver: hdisk-state time 1.312 hdisk 0: free 868352 dumpers 0
driver: flush size 0
driver: started chunker0 pid 4129
driver: send-cmd time 1.314 to chunker0: START 20080618130147
driver: send-cmd time 1.314 to chunker0: PORT-WRITE 00-00001 /holding/20080618130147/clienthost._some_dir.0 clienthost ffffffff9ffeffffffff1f /some/dir 0 1970:1:1:0:0:0 1048576 GNUTAR 192 |;auth=local;index;
chunker: pid 4129 executable chunker0 version 9.8.7
driver: result time 1.330 from chunker0: PORT 1487
driver: send-cmd time 1.330 to dumper0: PORT-DUMP 00-00001 1487 clienthost ffffffff9ffeffffffff1f /some/dir NODEVICE 0 1970:1:1:0:0:0 GNUTAR X amanda X local |"  <auth>local</auth>\n  <record>YES</record>\n  <index>YES</index>\n"
driver: state time 6.408 free kps: 0 space: 868160 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.408 if default: free 0
driver: hdisk-state time 6.408 hdisk 0: free 868160 dumpers 1
driver: result time 6.408 from dumper0: DONE 00-00001 100 100 0 "[sec 0.012 kb 100 kps 7915.1 orig-kb 100]"
driver: finished-cmd time 6.408 dumper0 dumped clienthost:/some/dir
driver: send-cmd time 6.408 to chunker0: DONE 00-00001
driver: state time 6.408 free kps: 0 space: 868160 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.408 if default: free 0
driver: hdisk-state time 6.408 hdisk 0: free 868160 dumpers 1
driver: result time 6.408 from chunker0: DONE 00-00001 100 "[sec 5.075 kb 100 kps 26.0]"
driver: finished-cmd time 6.408 chunker0 chunked clienthost:/some/dir
driver: send-cmd time 6.410 to taper: FILE-WRITE 00-00002 /holding/20080618130147/clienthost._some_dir.0 clienthost /some/dir 0 20080618130147 0
driver: startaflush: FIRST clienthost /some/dir 132 102400
driver: state time 6.410 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.410 if default: free 600
driver: hdisk-state time 6.410 hdisk 0: free 868220 dumpers 0
driver: result time 6.411 from taper: REQUEST-NEW-TAPE 00-00002
driver: send-cmd time 6.411 to taper: NEW-TAPE
driver: state time 6.412 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.412 if default: free 600
driver: hdisk-state time 6.412 hdisk 0: free 868220 dumpers 0
driver: result time 6.412 from taper: NEW-TAPE 00-00002 Conf-001
driver: state time 6.414 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.414 if default: free 600
driver: hdisk-state time 6.414 hdisk 0: free 868220 dumpers 0
driver: result time 6.415 from taper: PARTDONE 00-00002 Conf-001 1 100 "[sec 0.001177 kb 100 kps 84961.767205]"
driver: state time 6.415 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.415 if default: free 600
driver: hdisk-state time 6.415 hdisk 0: free 868220 dumpers 0
driver: result time 6.415 from taper: DONE 00-00002 INPUT-GOOD TAPE-GOOD "[sec 0.001177 kb 100 kps 84961.767205]" "" ""
driver: finished-cmd time 6.415 taper wrote clienthost:/some/dir
driver: state time 6.415 free kps: 600 space: 868352 taper: idle idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.415 if default: free 600
driver: hdisk-state time 6.415 hdisk 0: free 868352 dumpers 0
driver: QUITTING time 6.415 telling children to quit
driver: send-cmd time 6.415 to dumper0: QUIT
driver: send-cmd time 6.415 to dumper1: QUIT
driver: send-cmd time 6.417 to dumper2: QUIT
driver: send-cmd time 6.417 to dumper3: QUIT
driver: send-cmd time 6.418 to taper: QUIT
taper: DONE
driver: FINISHED time 7.426
amdump: end at Wed Jun 18 13:01:55 EDT 2008
%%%% quoted
amdump: start at Wed Jun 18 13:01:47 EDT 2008
amdump: datestamp 20080618
amdump: starttime 20080618130147
amdump: starttime-locale-independent 2008-06-18 13:01:47 EDT
planner: pid 4079 executable /path/to/planner version 9.8.7
READING CONF INFO...
planner: timestamp 20080618130147
planner: time 0.000: startup took 0.000 secs

SENDING FLUSHES...
ENDFLUSH

SETTING UP FOR ESTIMATES...
planner: time 0.000: setting up estimates for clienthost:"C:\\Some Dir\\"
clienthost:"C:\\Some Dir\\" overdue 14049 days for level 0
setup_estimate: clienthost:"C:\\Some Dir\\": command 0, options: none    last_level -1 next_level0 -14049 level_days 0    getting estimates 0 (-2) -1 (-2) -1 (-2)
planner: time 0.000: setting up estimates took 0.000 secs

GETTING ESTIMATES...
driver: pid 4080 executable /path/to/driver version 9.8.7
driver: adding holding disk 0 dir /holding size 868352 chunksize 1048576
reserving 0 out of 868352 for degraded-mode dumps
driver: send-cmd time 0.015 to taper: START-TAPER 20080618130147
taper: pid 4084 executable taper version 9.8.7
driver: started dumper0 pid 4086
driver: send-cmd time 0.031 to dumper0: START 20080618130147
planner: time 0.050: got partial result for host clienthost disk "C:\\Some Dir\\": 0 -> -2K, -1 -> -2K, -1 -> -2K
dumper: pid 4090 executable dumper1 version 9.8.7
driver: started dumper1 pid 4090
driver: send-cmd time 0.046 to dumper1: START 20080618130147
driver: started dumper2 pid 4094
driver: send-cmd time 0.048 to dumper2: START 20080618130147
driver: started dumper3 pid 4095
driver: send-cmd time 0.059 to dumper3: START 20080618130147
driver: start time 0.059 inparallel 4 bandwidth 600 diskspace 868352  dir OBSOLETE datestamp 20080618130147 driver: drain-ends tapeq FIRST big-dumpers sssS
dumper: pid 4094 executable dumper2 version 9.8.7
planner: time 0.088: got partial result for host clienthost disk "C:\\Some Dir\\": 0 -> 100K, -1 -> -2K, -1 -> -2K
planner: time 0.091: got result for host clienthost disk "C:\\Some Dir\\": 0 -> 100K, -1 -> -2K, -1 -> -2K
planner: time 0.091: getting estimates took 0.090 secs
FAILED QUEUE: empty
DONE QUEUE:
  0: clienthost     "C:\\Some Dir\\"

ANALYZING ESTIMATES...
pondering clienthost:"C:\\Some Dir\\"... next_level0 -14049 last_level -1 (due for level 0) (new disk, can't switch to degraded mode)
  curr level 0 nsize 100 csize 100 total size 208 total_lev0 100 balanced-lev0size 50
INITIAL SCHEDULE (size 208):
  clienthost "C:\\Some Dir\\" pri 14050 lev 0 nsize 100 csize 100

DELAYING DUMPS IF NEEDED, total_size 208, tape length 102400 mark 4
  delay: Total size now 208.

PROMOTING DUMPS IF NEEDED, total_lev0 100, balanced_size 50...
planner: time 0.091: analysis took 0.000 secs

GENERATING SCHEDULE:
--------
DUMP clienthost ffffffff9ffeffffffff1f "C:\\Some Dir\\" 20080618130147 14050 0 1970:1:1:0:0:0 100 100 0 1024
--------
dumper: pid 4086 executable dumper0 version 9.8.7
dumper: pid 4095 executable dumper3 version 9.8.7
taper: using label `Conf-001' date `20080618130147'
driver: result time 1.312 from taper: TAPER-OK
driver: state time 1.312 free kps: 600 space: 868352 taper: idle idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: not-idle
driver: interface-state time 1.312 if default: free 600
driver: hdisk-state time 1.312 hdisk 0: free 868352 dumpers 0
driver: flush size 0
driver: started chunker0 pid 4129
driver: send-cmd time 1.314 to chunker0: START 20080618130147
driver: send-cmd time 1.314 to chunker0: PORT-WRITE 00-00001 /holding/20080618130147/clienthost._some_dir.0 clienthost ffffffff9ffeffffffff1f "C:\\Some Dir\\" 0 1970:1:1:0:0:0 1048576 GNUTAR 192 |;auth=local;index;
chunker: pid 4129 executable chunker0 version 9.8.7
driver: result time 1.330 from chunker0: PORT 1487
driver: send-cmd time 1.330 to dumper0: PORT-DUMP 00-00001 1487 clienthost ffffffff9ffeffffffff1f "C:\\Some Dir\\" NODEVICE 0 1970:1:1:0:0:0 GNUTAR X amanda X local |"  <auth>local</auth>\n  <record>YES</record>\n  <index>YES</index>\n"
driver: state time 6.408 free kps: 0 space: 868160 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.408 if default: free 0
driver: hdisk-state time 6.408 hdisk 0: free 868160 dumpers 1
driver: result time 6.408 from dumper0: DONE 00-00001 100 100 0 "[sec 0.012 kb 100 kps 7915.1 orig-kb 100]"
driver: finished-cmd time 6.408 dumper0 dumped clienthost:"C:\\Some Dir\\"
driver: send-cmd time 6.408 to chunker0: DONE 00-00001
driver: state time 6.408 free kps: 0 space: 868160 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.408 if default: free 0
driver: hdisk-state time 6.408 hdisk 0: free 868160 dumpers 1
driver: result time 6.408 from chunker0: DONE 00-00001 100 "[sec 5.075 kb 100 kps 26.0]"
driver: finished-cmd time 6.408 chunker0 chunked clienthost:"C:\\Some Dir\\"
driver: send-cmd time 6.410 to taper: FILE-WRITE 00-00002 /holding/20080618130147/clienthost._some_dir.0 clienthost "C:\\Some Dir\\" 0 20080618130147 0
driver: startaflush: FIRST clienthost "C:\\Some Dir\\" 132 102400
driver: state time 6.410 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.410 if default: free 600
driver: hdisk-state time 6.410 hdisk 0: free 868220 dumpers 0
driver: result time 6.411 from taper: REQUEST-NEW-TAPE 00-00002
driver: send-cmd time 6.411 to taper: NEW-TAPE
driver: state time 6.412 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.412 if default: free 600
driver: hdisk-state time 6.412 hdisk 0: free 868220 dumpers 0
driver: result time 6.412 from taper: NEW-TAPE 00-00002 Conf-001
driver: state time 6.414 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.414 if default: free 600
driver: hdisk-state time 6.414 hdisk 0: free 868220 dumpers 0
driver: result time 6.415 from taper: PARTDONE 00-00002 Conf-001 1 100 "[sec 0.001177 kb 100 kps 84961.767205]"
driver: state time 6.415 free kps: 600 space: 868220 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.415 if default: free 600
driver: hdisk-state time 6.415 hdisk 0: free 868220 dumpers 0
driver: result time 6.415 from taper: DONE 00-00002 INPUT-GOOD TAPE-GOOD "[sec 0.001177 kb 100 kps 84961.767205]" "" ""
driver: finished-cmd time 6.415 taper wrote clienthost:"C:\\Some Dir\\"
driver: state time 6.415 free kps: 600 space: 868352 taper: idle idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 6.415 if default: free 600
driver: hdisk-state time 6.415 hdisk 0: free 868352 dumpers 0
driver: QUITTING time 6.415 telling children to quit
driver: send-cmd time 6.415 to dumper0: QUIT
driver: send-cmd time 6.415 to dumper1: QUIT
driver: send-cmd time 6.417 to dumper2: QUIT
driver: send-cmd time 6.417 to dumper3: QUIT
driver: send-cmd time 6.418 to taper: QUIT
taper: DONE
driver: FINISHED time 7.426
amdump: end at Wed Jun 18 13:01:55 EDT 2008
