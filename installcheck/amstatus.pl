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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 10;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Run qw( run run_get );
use Amanda::Paths;
use Amanda::Constants;

my $filename="$Installcheck::TMP/installcheck-amdump.1";
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

## now test a chunker partial result

write_logfile($logfiles{'chunker_partial'});

ok(!run('amstatus', 'TESTCONF', '--file', $filename),
    "amstatus return error with chunker partial");
ok($Installcheck::Run::exit_code == 4,
    "correct exit code for chunker partial");
like($Installcheck::Run::stdout,
    qr{localhost:/etc 0 backup failed: dumper: \[/usr/sbin/tar returned error\] \(7:49:23\)},
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
%%%% chunker_partial
amdump: start at Fri Apr 10 07:47:59 PDT 2009
amdump: datestamp 20090410
amdump: starttime 20090410074759
amdump: starttime-locale-independent 2009-04-10 07:47:59 PDT
planner: pid 4108 executable /usr/lib/amanda/planner version 3.0.0
planner: build: VERSION="Amanda-3.0.0"
planner:        BUILT_DATE="Wed Apr 8 17:49:57 PDT 2009"
planner:        BUILT_MACH="i386-pc-solaris2.10" BUILT_REV="16514"
planner:        BUILT_BRANCH="Amanda_Enterprise_3_0" CC="gcc"
planner: paths: bindir="/usr/bin" sbindir="/usr/sbin"
planner:        libexecdir="/usr/lib" amlibexecdir="/usr/lib/amanda"
planner:        mandir="/usr/share/man" AMANDA_TMPDIR="/tmp/amanda"
planner:        AMANDA_DBGDIR="/var/log/amanda" CONFIG_DIR="/etc/amanda"
planner:        DEV_PREFIX="/dev/dsk/" RDEV_PREFIX="/dev/rdsk/"
planner:        DUMP="/usr/sbin/ufsdump" RESTORE="/usr/sbin/ufsrestore"
planner:        VDUMP=UNDEF VRESTORE=UNDEF XFSDUMP=UNDEF XFSRESTORE=UNDEF
planner:        VXDUMP=UNDEF VXRESTORE=UNDEF
planner:        SAMBA_CLIENT="/usr/sfw/bin/smbclient"
planner:        STAR="/opt/csw/bin/star" GNUTAR="/opt/csw/bin/gtar"
planner:        COMPRESS_PATH="/usr/bin/gzip"
planner:        UNCOMPRESS_PATH="/usr/bin/gzip" LPRCMD="/usr/bin/lp"
planner:         MAILER=UNDEF listed_incr_dir="/var/lib/amanda/gnutar-lists"
planner: defs:  DEFAULT_SERVER="localhost" DEFAULT_CONFIG="DailySet1"
planner:        DEFAULT_TAPE_SERVER="localhost" DEFAULT_TAPE_DEVICE=""
planner:        HAVE_MMAP NEED_STRSTR HAVE_SYSVSHM AMFLOCK_POSIX AMFLOCK_LOCKF
planner:        AMFLOCK_LNLOCK SETPGRP_VOID ASSERTIONS AMANDA_DEBUG_DAYS=4
planner:        BSD_SECURITY RSH_SECURITY USE_AMANDAHOSTS
planner:        CLIENT_LOGIN="amandabackup" CHECK_USERID HAVE_GZIP
planner:        COMPRESS_SUFFIX=".gz" COMPRESS_FAST_OPT="--fast"
planner:        COMPRESS_BEST_OPT="--best" UNCOMPRESS_OPT="-dc"
READING CONF INFO...
planner: timestamp 20090410074759
planner: time 0.001: startup took 0.001 secs

SENDING FLUSHES...
ENDFLUSH

SETTING UP FOR ESTIMATES...
planner: time 0.001: setting up estimates for localhost:/etc
localhost:/etc overdue 14338 days for level 0
setup_estimate: localhost:/etc: command 0, options: none    last_level 0 next_level0 -14338 level_days 0    getting estimates 0 (-2) 1 (-2) -1 (-2)
planner: time 0.002: setting up estimates took 0.000 secs

GETTING ESTIMATES...
planner time 0.113: got result for host localhost disk /etc: 0 -> 80822K, 1 -> 61440K, -1 -> -2K
driver: pid 4109 executable /usr/lib/amanda/driver version 3.0.0
driver: tape size 122880
driver: adding holding disk 0 dir /var/lib/amanda/staging size 1215488 chunksize 1048576
reserving 1215488 out of 1215488 for degraded-mode dumps
driver: send-cmd time 0.010 to taper: START-TAPER 20090410074759
driver: started dumper0 pid 4116
driver: send-cmd time 0.012 to dumper0: START 20090410074759
driver: started dumper1 pid 4117
driver: send-cmd time 0.014 to dumper1: START 20090410074759
driver: started dumper2 pid 4118
driver: send-cmd time 0.016 to dumper2: START 20090410074759
driver: started dumper3 pid 4119
driver: send-cmd time 0.018 to dumper3: START 20090410074759
driver: start time 0.018 inparallel 4 bandwidth 8000 diskspace 1215488  dir OBSOLETE datestamp 20090410074759 driver: drain-ends tapeq FIRST big-dumpers sssS
taper: pid 4115 executable taper version 3.0.0
dumper: pid 4116 executable dumper0 version 3.0.0
planner: time 0.744: got partial result for host localhost disk /etc: 0 -> 80822K, 1 -> -1K, -1 -> -2K
planner: time 0.744: got result for host localhost disk /etc: 0 -> 80822K, 1 -> -1K, -1 -> -2K
planner: time 0.745: getting estimates took 0.742 secs
FAILED QUEUE: empty
DONE QUEUE:
  0: localhost  /etc

ANALYZING ESTIMATES...
pondering localhost:/etc... next_level0 -14338 last_level 0 (due for level 0) (picking inclevel for degraded mode)   picklev: last night 0, so tonight level 1
(no inc estimate)
  curr level 0 nsize 80822 csize 80822 total size 80921 total_lev0 80822 balanced-lev0size 11546
INITIAL SCHEDULE (size 80921):
  localhost /etc pri 14339 lev 0 nsize 80822 csize 80822

DELAYING DUMPS IF NEEDED, total_size 80921, tape length 122880 mark 1
  delay: Total size now 80921.

PROMOTING DUMPS IF NEEDED, total_lev0 80822, balanced_size 11546...
planner: time 0.745: analysis took 0.000 secs

GENERATING SCHEDULE:
--------
DUMP localhost ffffffff9efeffffffffff01 /etc 20090410074759 14339 0 1970:1:1:0:0:0 80822 80822 86 929 "Can't switch to degraded mode because an incremental estimate could not be performed"
--------
dumper: pid 4119 executable dumper3 version 3.0.0
dumper: pid 4118 executable dumper2 version 3.0.0
dumper: pid 4117 executable dumper1 version 3.0.0
taper: using label `maitreyee-010' date `20090410074759'
driver: result time 2.928 from taper: TAPER-OK 
driver: state time 2.937 free kps: 8000 space: 1215488 taper: idle idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: not-idle
driver: interface-state time 2.937 if default: free 8000
driver: hdisk-state time 2.937 hdisk 0: free 1215488 dumpers 0
driver: flush size 0
driver: started chunker0 pid 4160
driver: send-cmd time 2.954 to chunker0: START 20090410074759
driver: send-cmd time 2.954 to chunker0: PORT-WRITE 00-00001 /var/lib/amanda/staging/20090410074759/localhost._etc.0 localhost ffffffff9efeffffffffff01 /etc 0 1970:1:1:0:0:0 1048576 APPLICATION 80896 |;auth=bsdtcp;index;
chunker: pid 4160 executable chunker0 version 3.0.0
driver: result time 3.000 from chunker0: PORT 11005
driver: send-cmd time 3.001 to dumper0: PORT-DUMP 00-00001 11005 localhost ffffffff9efeffffffffff01 /etc /etc 0 1970:1:1:0:0:0 amsuntar X X X bsdtcp |"  <auth>bsdtcp</auth>\n  <record>YES</record>\n  <index>YES</index>\n  <backup-program>\n    <plugin>amsuntar</plugin>\n    <property>\n      <name>EXTENDED-HEADERS</name>\n      <value>NO</value>\n    </property>\n    <property>\n      <name>EXTENDED-ATTRIBUTES</name>\n      <value>NO</value>\n    </property>\n    <property>\n      <name>BLOCK-SIZE</name>\n      <value>64</value>\n    </property>\n  </backup-program>\n"
driver: state time 3.034 free kps: 7071 space: 1134592 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 3.034 if default: free 7071
driver: hdisk-state time 3.034 hdisk 0: free 1134592 dumpers 1
driver: state time 49.732 free kps: 7071 space: 1134592 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 49.732 if default: free 7071
driver: hdisk-state time 49.732 hdisk 0: free 1134592 dumpers 1
driver: result time 49.732 from chunker0: RQ-MORE-DISK 00-00001
driver: send-cmd time 49.733 to chunker0: CONTINUE 00-00001 /var/lib/amanda/staging/20090410074759/localhost._etc.0 1048576 4096
dumper: kill index command
driver: state time 49.901 free kps: 7071 space: 1130496 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 49.901 if default: free 7071
driver: hdisk-state time 49.901 hdisk 0: free 1130496 dumpers 1
driver: result time 49.901 from dumper0: FAILED 00-00001 "[/usr/sbin/tar returned error]"
driver: send-cmd time 49.901 to chunker0: FAILED 00-00001
driver: state time 49.906 free kps: 7071 space: 1130496 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 49.906 if default: free 7071
driver: hdisk-state time 49.906 hdisk 0: free 1130496 dumpers 1
driver: result time 49.906 from chunker0: PARTIAL 00-00001 80909 "[sec 46.789 kb 80909 kps 1729.9]"
driver: finished-cmd time 49.907 chunker0 chunked localhost:/etc
driver: started chunker0 pid 4184
driver: send-cmd time 50.094 to chunker0: START 20090410074759
driver: send-cmd time 50.094 to chunker0: PORT-WRITE 00-00002 /var/lib/amanda/staging/20090410074759/localhost._etc.0 localhost ffffffff9efeffffffffff01 /etc 0 1970:1:1:0:0:0 1048576 APPLICATION 84960 |;auth=bsdtcp;index;
chunker: pid 4184 executable chunker0 version 3.0.0
driver: result time 50.139 from chunker0: PORT 11035
driver: send-cmd time 50.140 to dumper0: PORT-DUMP 00-00002 11035 localhost ffffffff9efeffffffffff01 /etc /etc 0 1970:1:1:0:0:0 amsuntar X X X bsdtcp |"  <auth>bsdtcp</auth>\n  <record>YES</record>\n  <index>YES</index>\n  <backup-program>\n    <plugin>amsuntar</plugin>\n    <property>\n      <name>EXTENDED-HEADERS</name>\n      <value>NO</value>\n    </property>\n    <property>\n      <name>EXTENDED-ATTRIBUTES</name>\n      <value>NO</value>\n    </property>\n    <property>\n      <name>BLOCK-SIZE</name>\n      <value>64</value>\n    </property>\n  </backup-program>\n"
driver: state time 50.175 free kps: 7071 space: 1130528 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 50.175 if default: free 7071
driver: hdisk-state time 50.175 hdisk 0: free 1130528 dumpers 1
driver: state time 84.740 free kps: 7071 space: 1130528 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 84.740 if default: free 7071
driver: hdisk-state time 84.740 hdisk 0: free 1130528 dumpers 1
driver: result time 84.740 from dumper0: FAILED 00-00002 "[/usr/sbin/tar returned error]"
driver: send-cmd time 84.740 to chunker0: FAILED 00-00002
driver: state time 84.745 free kps: 7071 space: 1130528 taper: idle idle-dumpers: 3 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 84.745 if default: free 7071
driver: hdisk-state time 84.745 hdisk 0: free 1130528 dumpers 1
driver: result time 84.745 from chunker0: PARTIAL 00-00002 80917 "[sec 34.565 kb 80917 kps 2341.9]"
driver: finished-cmd time 84.745 chunker0 chunked localhost:/etc
dumper: kill index command
driver: send-cmd time 84.890 to taper: FILE-WRITE 00-00003 /var/lib/amanda/staging/20090410074759/localhost._etc.0 localhost /etc 0 20090410074759 0
driver: startaflush: FIRST localhost /etc 80949 122880
driver: state time 84.890 free kps: 8000 space: 1134539 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 84.890 if default: free 8000
driver: hdisk-state time 84.890 hdisk 0: free 1134539 dumpers 0
driver: state time 84.891 free kps: 8000 space: 1134539 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 84.891 if default: free 8000
driver: hdisk-state time 84.891 hdisk 0: free 1134539 dumpers 0
driver: result time 84.891 from taper: REQUEST-NEW-TAPE 00-00003
driver: send-cmd time 84.891 to taper: NEW-TAPE
driver: state time 84.906 free kps: 8000 space: 1134539 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 84.906 if default: free 8000
driver: hdisk-state time 84.906 hdisk 0: free 1134539 dumpers 0
driver: result time 84.906 from taper: NEW-TAPE 00-00003 maitreyee-010
driver: state time 114.910 free kps: 8000 space: 1134539 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 114.910 if default: free 8000
driver: hdisk-state time 114.910 hdisk 0: free 1134539 dumpers 0
driver: result time 114.911 from taper: PARTDONE 00-00003 maitreyee-010 1 80917 "[sec 30.003926 kb 80917 kps 2696.880402]"
driver: state time 114.911 free kps: 8000 space: 1134539 taper: writing idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 114.911 if default: free 8000
driver: hdisk-state time 114.911 hdisk 0: free 1134539 dumpers 0
driver: result time 114.911 from taper: DONE 00-00003 INPUT-GOOD TAPE-GOOD "[sec 30.003926 kb 80917 kps 2696.880402]" "" ""
driver: finished-cmd time 114.911 taper wrote localhost:/etc
driver: state time 114.943 free kps: 8000 space: 1215488 taper: idle idle-dumpers: 4 qlen tapeq: 0 runq: 0 roomq: 0 wakeup: 0 driver-idle: no-dumpers
driver: interface-state time 114.943 if default: free 8000
driver: hdisk-state time 114.943 hdisk 0: free 1215488 dumpers 0
driver: QUITTING time 114.943 telling children to quit
driver: send-cmd time 114.943 to dumper0: QUIT ""
driver: send-cmd time 114.944 to dumper1: QUIT ""
driver: send-cmd time 114.944 to dumper2: QUIT ""
driver: send-cmd time 114.944 to dumper3: QUIT ""
driver: send-cmd time 114.944 to taper: QUIT
taper: DONE
driver: FINISHED time 115.961
amdump: end at Fri Apr 10 07:49:55 PDT 2009
