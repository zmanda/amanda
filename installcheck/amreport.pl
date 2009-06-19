# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 10;

use strict;
use warnings;
use lib "@amperldir@";
use Installcheck;
use Installcheck::Run qw( run run_get );
use Amanda::Paths;
use Amanda::Constants;

# we use Text::Diff for diagnostics if it's installed
my $have_text_diff;
BEGIN {
    eval "use Text::Diff;";
    if ($@) {
	$have_text_diff = 0;
    } else {
	$have_text_diff = 1;
    }
}

my $filename="$Installcheck::TMP/installcheck-log.datastamp.0";
my $out_filename="$Installcheck::TMP/installcheck-amreport-output";
my $testconf = Installcheck::Run::setup();
$testconf->write();

# read __DATA__ to a hash, keyed by the names following '%%%%'
my %datas;
my $key = undef;
while (<DATA>) {
    if (/^%%%% (.*)/) {
	$key = $1;
    } else {
	$datas{$key} .= $_;
    }
}

sub write_file {
    my $filename = shift;
    my ($data) = @_;
    open(my $fh, ">", $filename) or die("Could not open '$filename' for writing");
    print $fh $data;
    close($fh);
}

sub slurp {
    my ($filename) = @_;

    open(my $fh, "<", $filename) or die("open $filename: $!");
    my $result = do { local $/; <$fh> };
    close($fh);

    return $result;
}

## compare two multiline strings, giving a diff if they do not match

sub results_match {
    my ($a, $b, $msg) = @_;

    sub cleanup {
	my $str = shift;
	chomp $str;
	# chomp whitespace before newlines
	$str =~ s/\s+$//mg;
	# chomp the "brought to you by.." line
	$str =~ s/brought to you by Amanda version .*\)/<versioninfo>/g;
	$str;
    }
    $a = cleanup($a);
    $b = cleanup($b);

    if ($a eq $b) {
	pass($msg);
    } else {
	my $diff;
	if ($have_text_diff) {
	    $diff = diff(\$a, \$b, { 'STYLE' => "Unified" });
	} else {
	    $diff = "---- GOT: ----\n$a\n---- EXPECTED: ----\n$b\n---- ----";
	}
	fail($msg);
	diag($diff);
    }
}

## try a few various options with a pretty normal logfile

write_file($filename, $datas{'normal'});

ok(run('amreport', 'TESTCONF', '-l', $filename, '-f', $out_filename),
    "plain amreport");
results_match(slurp($out_filename), $datas{'report1'},
    "result for plain amreport matches");

ok(run('amreport', 'TESTCONF', '-l', $filename, '-f', $out_filename, '-o', 'columnspec=OrigKB=::2'),
    "amreport with OrigKB=::2");
results_match(slurp($out_filename), $datas{'report2'},
    "result for amreport with OrigKB=::2 matches");

ok(run('amreport', 'TESTCONF', '-l', $filename, '-f', $out_filename, '-o', 'columnspec=OrigKB=:5'),
    "amreport with OrigKB=:5");
results_match(slurp($out_filename), $datas{'report3'},
    "result for amreport with OrigKB=:5 matches");

ok(run('amreport', 'TESTCONF', '-l', $filename, '-f', $out_filename, '-o', 'columnspec=OrigKB=:5'),
    "amreport with OrigKB=5:-1:3");
results_match(slurp($out_filename), $datas{'report4'},
    "result for amreport with OrigKB=5:-1:3 matches");

ok(run('amreport', 'TESTCONF', '-l', $filename, '-f', $out_filename, '-o', 'displayunit=m'),
    "amreport with displayunit=m");
results_match(slurp($out_filename), $datas{'report5'},
    "result for amreport with displayunit=m matches");

__DATA__
%%%% normal
INFO amdump amdump pid 23649
INFO planner planner pid 23682
DISK planner localhost.localdomain /boot1
DISK planner localhost.localdomain /boot2
DISK planner localhost.localdomain /boot3
DISK planner localhost.localdomain /boot4
DISK planner localhost.localdomain /boot5
DISK planner localhost.localdomain /boot6
DISK planner localhost.localdomain /boot7
DISK planner localhost.localdomain /boot8
DISK planner localhost.localdomain /boot9
START planner date 20090225080737
INFO driver driver pid 23684
START driver date 20090225080737
STATS driver hostname localhost.localdomain
STATS driver startup time 0.004
INFO dumper dumper pid 23686
INFO taper taper pid 23685
FINISH planner date 20090225080737 time 0.084
INFO planner pid-done 23682
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot1 20090225080737 0 [sec 1.585 kb 12 kps 24748.4 orig-kb 16]
STATS driver estimate localhost.localdomain /boot1 20090225080737 0 [sec 1 nkb 12 ckb 12 kps 25715]
SUCCESS chunker localhost.localdomain /boot1 20090225080737 0 [sec 1.607 kb 12 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
START taper datestamp 20090225080737 label DIRO-TEST-003 tape 1
PART taper DIRO-TEST-003 1 localhost.localdomain /boot1 20090225080737 1/1 0 [sec 0.250557 kb 12 kps 156611.070535]
DONE taper localhost.localdomain /boot1 20090225080737 1 0 [sec 0.250557 kb 12 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot2 20090225080737 0 [sec 1.585 kb 123 kps 24748.4 orig-kb 167]
STATS driver estimate localhost.localdomain /boot2 20090225080737 0 [sec 1 nkb 123 ckb 123 kps 25715]
SUCCESS chunker localhost.localdomain /boot2 20090225080737 0 [sec 1.607 kb 123 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 2 localhost.localdomain /boot2 20090225080737 1/1 0 [sec 0.250557 kb 123 kps 156611.070535]
DONE taper localhost.localdomain /boot2 20090225080737 1 0 [sec 0.250557 kb 123 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot3 20090225080737 0 [sec 1.585 kb 1234 kps 24748.4 orig-kb 1678]
STATS driver estimate localhost.localdomain /boot3 20090225080737 0 [sec 1 nkb 1234 ckb 1234 kps 25715]
SUCCESS chunker localhost.localdomain /boot3 20090225080737 0 [sec 1.607 kb 1234 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 3 localhost.localdomain /boot3 20090225080737 1/1 0 [sec 0.250557 kb 1234 kps 156611.070535]
DONE taper localhost.localdomain /boot3 20090225080737 1 0 [sec 0.250557 kb 1234 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot4 20090225080737 0 [sec 1.585 kb 12345 kps 24748.4 orig-kb 16789]
STATS driver estimate localhost.localdomain /boot4 20090225080737 0 [sec 1 nkb 12345 ckb 12345 kps 25715]
SUCCESS chunker localhost.localdomain /boot4 20090225080737 0 [sec 1.607 kb 12345 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 4 localhost.localdomain /boot4 20090225080737 1/1 0 [sec 0.250557 kb 12345 kps 156611.070535]
DONE taper localhost.localdomain /boot4 20090225080737 1 0 [sec 0.250557 kb 12345 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot5 20090225080737 0 [sec 1.585 kb 123456 kps 24748.4 orig-kb 167890]
STATS driver estimate localhost.localdomain /boot5 20090225080737 0 [sec 1 nkb 123456 ckb 123456 kps 25715]
SUCCESS chunker localhost.localdomain /boot5 20090225080737 0 [sec 1.607 kb 123456 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 5 localhost.localdomain /boot5 20090225080737 1/1 0 [sec 0.250557 kb 123456 kps 156611.070535]
DONE taper localhost.localdomain /boot5 20090225080737 1 0 [sec 0.250557 kb 123456 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot6 20090225080737 0 [sec 1.585 kb 1234567 kps 24748.4 orig-kb 1678901]
STATS driver estimate localhost.localdomain /boot6 20090225080737 0 [sec 1 nkb 1234567 ckb 1234567 kps 25715]
SUCCESS chunker localhost.localdomain /boot6 20090225080737 0 [sec 1.607 kb 1234567 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 6 localhost.localdomain /boot6 20090225080737 1/1 0 [sec 0.250557 kb 1234567 kps 156611.070535]
DONE taper localhost.localdomain /boot6 20090225080737 1 0 [sec 0.250557 kb 1234567 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot7 20090225080737 0 [sec 1.585 kb 12345678 kps 24748.4 orig-kb 16789012]
STATS driver estimate localhost.localdomain /boot7 20090225080737 0 [sec 1 nkb 12345678 ckb 12345678 kps 25715]
SUCCESS chunker localhost.localdomain /boot7 20090225080737 0 [sec 1.607 kb 12345678 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 7 localhost.localdomain /boot7 20090225080737 1/1 0 [sec 0.250557 kb 12345678 kps 156611.070535]
DONE taper localhost.localdomain /boot7 20090225080737 1 0 [sec 0.250557 kb 12345678 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot8 20090225080737 0 [sec 1.585 kb 123456789 kps 24748.4 orig-kb 167890123]
STATS driver estimate localhost.localdomain /boot8 20090225080737 0 [sec 1 nkb 123456789 ckb 123456789 kps 25715]
SUCCESS chunker localhost.localdomain /boot8 20090225080737 0 [sec 1.607 kb 123456789 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 8 localhost.localdomain /boot8 20090225080737 1/1 0 [sec 0.250557 kb 123456789 kps 156611.070535]
DONE taper localhost.localdomain /boot8 20090225080737 1 0 [sec 0.250557 kb 123456789 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot9 20090225080737 0 [sec 1.585 kb 1234567890 kps 24748.4 orig-kb 1678901234]
STATS driver estimate localhost.localdomain /boot9 20090225080737 0 [sec 1 nkb 1234567890 ckb 1234567890 kps 25715]
SUCCESS chunker localhost.localdomain /boot9 20090225080737 0 [sec 1.607 kb 1234567890 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 9 localhost.localdomain /boot9 20090225080737 1/1 0 [sec 0.250557 kb 1234567890 kps 156611.070535]
DONE taper localhost.localdomain /boot9 20090225080737 1 0 [sec 0.250557 kb 1234567890 kps 156611.070535]
INFO dumper pid-done 23686
INFO taper tape DIRO-TEST-003 kb 39240 fm 10 [OK]
INFO taper pid-done 23685
FINISH driver date 20090225080737 time 5.306
INFO driver pid-done 23684
%%%% report1
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        -- 
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        -- 

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Chunks Taped                  9          9          0
Avg Tp Write Rate (k/s) #######    #######        -- 

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                          DUMPER STATS                    TAPER STATS  
HOSTNAME     DISK        L    ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- -------------------------------------------- ---------------
localhost.lo /boot1      0         12         12    --     0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0        167        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0       1678       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0      16789      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0     167890     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0    1678901    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0   16789012   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0  167890123  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% report2
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        -- 
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        -- 

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Chunks Taped                  9          9          0
Avg Tp Write Rate (k/s) #######    #######        -- 

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                            DUMPER STATS                     TAPER STATS  
HOSTNAME     DISK        L       ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- ----------------------------------------------- ---------------
localhost.lo /boot1      0         12.00         12    --     0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0        167.00        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0       1678.00       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0      16789.00      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0     167890.00     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0    1678901.00    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0   16789012.00   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0  167890123.00  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234.00 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% report3
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        -- 
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        -- 

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Chunks Taped                  9          9          0
Avg Tp Write Rate (k/s) #######    #######        -- 

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                        DUMPER STATS                 TAPER STATS  
HOSTNAME     DISK        L ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- --------------------------------------- ---------------
localhost.lo /boot1      0    12         12    --     0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0   167        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0  1678       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0 16789      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0 167890     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0 1678901    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0 16789012   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0 167890123  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% report4
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        -- 
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        -- 

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Chunks Taped                  9          9          0
Avg Tp Write Rate (k/s) #######    #######        -- 

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                        DUMPER STATS                 TAPER STATS  
HOSTNAME     DISK        L ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- --------------------------------------- ---------------
localhost.lo /boot1      0    12         12    --     0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0   167        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0  1678       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0 16789      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0 167890     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0 1678901    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0 16789012   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0 167890123  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% report5
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        -- 
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        -- 

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Chunks Taped                  9          9          0
Avg Tp Write Rate (k/s) #######    #######        -- 

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00  1339592M  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                       DUMPER STATS                 TAPER STATS  
HOSTNAME     DISK        L ORIG-MB  OUT-MB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- -------------------------------------- ---------------
localhost.lo /boot1      0       0       0    --     0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0       0       0   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0       2       1   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0      16      12   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0     164     121   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0    1640    1206   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0   16396   12056   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0  163955  120563   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1639552 1205633   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
