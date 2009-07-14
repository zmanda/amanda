# Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 39;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::DB::Catalog;

# set up and load a simple config
my $testconf = Installcheck::Config->new();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load test config");

# test functions against an empty set of logfiles

is_deeply([ Amanda::DB::Catalog::get_write_timestamps() ], [],
    "No write_timestamps in an empty catalog");

is_deeply(Amanda::DB::Catalog::get_latest_write_timestamp(), undef,
    "No latest write_timestamp in an empty catalog");

is_deeply([ Amanda::DB::Catalog::get_dumps() ], [],
    "No dumpfiles in an empty catalog");

# and add some logfiles to query, and a corresponding tapelist, while also gathering
# a list of dumpfiles for comparison with the results from Amanda::DB::Catalog
my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
my $tapelist_fn = config_dir_relative(getconf($CNF_TAPELIST));
my $output;
my $write_timestamp;
my %dumpfiles;
open (my $tapelist, ">", $tapelist_fn);
while (<DATA>) {
    # skip comments
    next if (/^#/ or /^\S*$/);

    # add to tapelist
    if (/^:tapelist (\d+) (\S+)\s*$/) {
	print $tapelist "$1 $2 reuse\n";
	next;
    }
    
    # new logfile
    if (/^::: (.*)/) {
	open $output, ">", "$logdir/$1" or die("Could not open $1 for writing: $!");
	next;
    }

    # write_timestamp
    if (/^:timestamp (\d+)/) {
	$write_timestamp = $1;
	next;
    }

    # new dumpfile
    if (/^:dumpfile (\S+) (\S+) (\S+) (\S+) (\d+) (\S+) (\d+) (\d+) (\d+) (\S+) (\S+) (\d+)/) {
	$dumpfiles{$1} = {
	    'dump_timestamp' => $2,	'hostname' => $3,	    'diskname' => $4,
	    'level' => $5,		'label' => $6,		    'filenum' => $7,
	    'partnum' => $8,		'nparts' => $9,		    'status' => $10,
	    'sec' => $11+0.0,		'kb' => $12,
	    'write_timestamp' => $write_timestamp,
	};
	next;
    }

    die("syntax error") if (/^:/);

    print $output $_;
}
close($output);
close($tapelist);
Amanda::DB::Catalog::_clear_cache();

##
# Test the timestamps

is_deeply([ Amanda::DB::Catalog::get_write_timestamps(), ],
    [ '20080111000000', '20080222222222', '20080313133333', '20080414144444' ],
    "get_write_timestamps returns all logfile datestamps in proper order, with zero-padding");

is(Amanda::DB::Catalog::get_latest_write_timestamp(), '20080414144444',
    "get_latest_write_timestamp correctly returns the latest write timestamp");

##
# test get_dumps and sort_dumps

# get dumps filtered by a regexp on the key
sub dump_names($) {
    my ($expr) = @_; 
    my @selected_keys = grep { $_ =~ $expr } keys %dumpfiles;
    return map { $dumpfiles{$_} } @selected_keys;
}

# get dumps filtered by an expression on the dumpfile itself
sub dumps(&) {
    my ($block) = @_; 
    return grep { &$block } values %dumpfiles;
}

# put @_ in a canonical order
sub sortdumps {
    map {
	# convert bigints to strings and on to integers so is_deeply doesn't get confused
	$_->{'level'} = "$_->{level}" + 0;
	$_->{'filenum'} = "$_->{filenum}" + 0;
	$_->{'kb'} = "$_->{kb}" + 0;
	$_;
    } sort {
	$a->{'label'} cmp $b->{'label'}
	    or $a->{'filenum'} <=> $b->{'filenum'}
    }
    @_;
}

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps() ],
    [ sortdumps dump_names qr/.*/ ],
    "get_dumps returns all dumps when given no parameters");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamp => '20080111000000') ],
    [ sortdumps dump_names qr/somebox_lib_20080111/ ],
    "get_dumps parameter write_timestamp");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamp => '20080111') ],
    [ sortdumps dump_names qr/somebox_lib_20080111/ ],
    "get_dumps accepts a short write_timestamp and zero-pads it");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamps => ['20080111000000','20080222222222']) ],
    [ sortdumps dump_names qr/(20080111|20080222222222_p\d*)$/ ],
    "get_dumps parameter write_timestamps");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(dump_timestamp => '20080111000000') ],
    [ sortdumps dump_names qr/somebox_lib_20080111/ ],
    "get_dumps parameter dump_timestamp");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(dump_timestamp => '20080111') ],
    [ sortdumps dump_names qr/somebox_lib_20080111/ ],
    "get_dumps accepts a short dump_timestamp and zero-pads it");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(dump_timestamps => ['20080111000000','20080222222222']) ],
    [ sortdumps dump_names qr/(20080111|20080222222222_p\d*)$/ ],
    "get_dumps parameter dump_timestamps");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(dump_timestamp_match => '200801-2') ],
    [ sortdumps dump_names qr/20080[12]/ ],
    "get_dumps parameter dump_timestamp_match");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(hostname => 'otherbox') ],
    [ sortdumps dump_names qr/^otherbox_/ ],
    "get_dumps parameter hostname");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(hostnames => ['otherbox','somebox']) ],
    [ sortdumps dump_names qr/^(otherbox_|somebox_)/ ],
    "get_dumps parameter hostnames");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(hostname_match => '*box') ],
    [ sortdumps dump_names qr/box/ ],
    "get_dumps parameter hostname_match");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(diskname => '/lib') ],
    [ sortdumps dump_names qr/_lib_/ ],
    "get_dumps parameter diskname");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(disknames => ['/lib','/usr/bin']) ],
    [ sortdumps dump_names qr/(_lib_|_usr_bin_)/ ],
    "get_dumps parameter disknames");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(diskname_match => '/usr') ],
    [ sortdumps dump_names qr/_usr_/ ],
    "get_dumps parameter diskname_match");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(label => 'Conf-001') ],
    [ sortdumps dumps { $_->{'label'} eq 'Conf-001' } ],
    "get_dumps parameter label");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(labels => ['Conf-002','Conf-003']) ],
    [ sortdumps dumps { $_->{'label'} eq 'Conf-002' or $_->{'label'} eq 'Conf-003' } ],
    "get_dumps parameter labels");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(level => 0) ],
    [ sortdumps dumps { $_->{'level'} == 0 } ],
    "get_dumps parameter level");
is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(levels => [ 1 ]) ],
    [ sortdumps dumps { $_->{'level'} == 1 } ],
    "get_dumps parameter levels");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(status => "OK") ],
    [ sortdumps dumps { $_->{'status'} eq "OK" } ],
    "get_dumps parameter status = OK");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(status => "PARTIAL") ],
    [ sortdumps dumps { $_->{'status'} eq "PARTIAL" } ],
    "get_dumps parameter status = PARTIAL");

## more complex, multi-parameter queries

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(hostname => 'somebox',
						     diskname_match => '/lib') ],
    [ sortdumps dump_names qr/^somebox_lib_/ ],
    "get_dumps parameters hostname and diskname_match");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamp => '20080313133333',
						     dump_timestamp => '20080311131133') ],
    [ sortdumps dumps { $_->{'dump_timestamp'} eq '20080311131133' 
                    and $_->{'write_timestamp'} eq '20080313133333' } ],
    "get_dumps parameters write_timestamp and dump_timestamp");

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamp => '20080414144444',
						     status => 'OK') ],
    [ ], # there were no OK dumps on that date
    "get_dumps parameters write_timestamp status");

## test sorting

is_deeply([ Amanda::DB::Catalog::sort_dumps(['write_timestamp'],
		@dumpfiles{'somebox_lib_20080222222222_p1','somebox_lib_20080111'}) ],
	      [ @dumpfiles{'somebox_lib_20080111','somebox_lib_20080222222222_p1'} ],
    "sort by write_timestamps");
is_deeply([ Amanda::DB::Catalog::sort_dumps(['-write_timestamp'],
		@dumpfiles{'somebox_lib_20080111','somebox_lib_20080222222222_p1'}) ],
	      [ @dumpfiles{'somebox_lib_20080222222222_p1','somebox_lib_20080111'} ],
    "sort by write_timestamps, reverse");

is_deeply([ Amanda::DB::Catalog::sort_dumps(['hostname', '-diskname', 'write_timestamp'],
		@dumpfiles{
		    'somebox_lib_20080222222222_p1',
		    'somebox_usr_bin_20080313133333',
		    'somebox_lib_20080313133333_p4',
		    'otherbox_lib_20080313133333',
		    'somebox_lib_20080111',
		    }) ],
	      [ @dumpfiles{
		    'otherbox_lib_20080313133333',
		    'somebox_usr_bin_20080313133333',
		    'somebox_lib_20080111',
		    'somebox_lib_20080222222222_p1',
		    'somebox_lib_20080313133333_p4',
	            } ],
    "multi-key sort");

is_deeply([ Amanda::DB::Catalog::sort_dumps(['filenum'],
		@dumpfiles{
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p1',
		    }) ],
	      [ @dumpfiles{
		    'somebox_lib_20080313133333_p1',
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p10',
		    } ],
		"filenum is sorted numerically, not lexically");

is_deeply([ Amanda::DB::Catalog::sort_dumps(['-partnum'],
		@dumpfiles{
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p1',
		    }) ],
	      [ @dumpfiles{
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p1',
		    } ],
		"partnum is sorted numerically (and in reverse!), not lexically");

is_deeply([ Amanda::DB::Catalog::sort_dumps(['nparts'],
		@dumpfiles{
		    'somebox_lib_20080313133333_p9', # nparts=10
		    'somebox_lib_20080222222222_p2', # nparts=2
		    }) ],
	      [ @dumpfiles{
		    'somebox_lib_20080222222222_p2', # nparts=2
		    'somebox_lib_20080313133333_p9', # nparts=10
		    } ],
		"nparts is sorted numerically, not lexically");

is_deeply([ Amanda::DB::Catalog::sort_dumps(['kb'],
		@dumpfiles{
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p1',
		    }) ],
	      [ @dumpfiles{
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p1',
		    } ],
		"kb is sorted numerically, not lexically");

## add log entries

# one to an existing logfile, same tape
Amanda::DB::Catalog::add_dump({
    'write_timestamp' => '20080111',
    'dump_timestamp' => '20080707070707',
    'hostname' => 'newbox',
    'diskname' => '/newdisk',
    'level' => 3,
    'label' => 'Conf-001',
    'filenum' => 2,
    'partnum' => 1,
    'nparts' => 1,
    'status' => 'OK',
    'sec' => 13.0,
    'kb' => 12380,
});

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(hostname => 'newbox') ],
    [ sortdumps {
	'write_timestamp' => '20080111000000',
	'dump_timestamp' => '20080707070707',
	'hostname' => 'newbox',
	'diskname' => '/newdisk',
	'level' => 3,
	'label' => 'Conf-001',
	'filenum' => 2,
	'partnum' => 1,
	'nparts' => 1,
	'status' => 'OK',
	'sec' => 13.0,
	'kb' => 12380,
    } ],
    "successfully re-read an added dump in an existing logfile");

# and again, to test the last-logfile cache in Amanda::DB::Catalog
Amanda::DB::Catalog::add_dump({
    'write_timestamp' => '20080111',
    'dump_timestamp' => '20080707070707',
    'hostname' => 'newbox',
    'diskname' => '/newdisk2',
    'level' => 0,
    'label' => 'Conf-001',
    'filenum' => 3,
    'partnum' => 1,
    'nparts' => 1,
    'status' => 'OK',
    'sec' => 27.0,
    'kb' => 32380,
});

is(scalar Amanda::DB::Catalog::get_dumps(hostname => 'newbox'), 2,
    "adding another dump to that logfile and re-reading gives 2 dumps");

# and another in a new file, as well as a tapelist entry
Amanda::DB::Catalog::add_dump({
    'write_timestamp' => '20080707070707',
    'dump_timestamp' => '20080707070707',
    'hostname' => 'newlog',
    'diskname' => '/newdisk',
    'level' => 3,
    'label' => 'Conf-009',
    'filenum' => 1,
    'partnum' => 1,
    'nparts' => 1,
    'status' => 'OK',
    'sec' => 13.0,
    'kb' => 12380,
});

is_deeply([ sortdumps Amanda::DB::Catalog::get_dumps(hostname => 'newlog') ],
    [ sortdumps {
	'write_timestamp' => '20080707070707',
	'dump_timestamp' => '20080707070707',
	'hostname' => 'newlog',
	'diskname' => '/newdisk',
	'level' => 3,
	'label' => 'Conf-009',
	'filenum' => 1,
	'partnum' => 1,
	'nparts' => 1,
	'status' => 'OK',
	'sec' => 13.0,
	'kb' => 12380,
    } ],
    "successfully re-read an added dump in a new logfile");

# and add a multipart dump to that same logfile
for (my $i = 1; $i <= 5; $i++) {
    Amanda::DB::Catalog::add_dump({
	'write_timestamp' => '20080707070707',
	'dump_timestamp' => '20080707070707',
	'hostname' => 'newlog',
	'diskname' => '/bigdisk',
	'level' => 1,
	'label' => 'Conf-009',
	'filenum' => $i+1,
	'partnum' => $i,
	'nparts' => 5,
	'status' => 'OK',
	'sec' => 13.0,
	'kb' => 12380,
    });
}

is(scalar Amanda::DB::Catalog::get_dumps(diskname => '/bigdisk'), 5,
    "multi-part dump added and re-read successfully");

__DATA__
# a short-datestamp logfile with only a single, single-part file in it
::: log.20080111.0
:tapelist 20080111 Conf-001
:timestamp 20080111000000
DISK planner somebox /lib
START planner date 20080111
START driver date 20080111
STATS driver hostname somebox
STATS driver startup time 0.051
FINISH planner date 20080111 time 82.721
START taper datestamp 20080111 label Conf-001 tape 1
SUCCESS dumper somebox /lib 20080111 0 [sec 0.209 kb 1970 kps 9382.2 orig-kb 1970]
SUCCESS chunker somebox /lib 20080111 0 [sec 0.305 kb 420 kps 1478.7]
STATS driver estimate somebox /lib 20080111 0 [sec 1 nkb 2002 ckb 480 kps 385]
:dumpfile somebox_lib_20080111 20080111000000 somebox /lib 0 Conf-001 1 1 1 OK 4.813543 419
PART taper Conf-001 1 somebox /lib 20080111 1/1 0 [sec 4.813543 kb 419 kps 87.133307]
DONE taper somebox /lib 20080111 1 0 [sec 4.813543 kb 419 kps 87.133307]
FINISH driver date 20080111 time 2167.581

# a long-datestamp logfile, also fairly simple
::: log.20080222222222.0
:tapelist 20080222222222 Conf-002
:timestamp 20080222222222
DISK planner somebox /lib
START planner date 20080222222222
START driver date 20080222222222
STATS driver hostname somebox
STATS driver startup time 0.051
FINISH planner date 20080222222222 time 0.102
SUCCESS dumper somebox /lib 20080222222222 0 [sec 0.012 kb 100 kps 8115.6 orig-kb 100]
SUCCESS chunker somebox /lib 20080222222222 0 [sec 5.075 kb 100 kps 26.0]
STATS driver estimate somebox /lib 20080222222222 0 [sec 0 nkb 132 ckb 160 kps 1024]
START taper datestamp 20080222222222 label Conf-002 tape 1
:dumpfile somebox_lib_20080222222222_p1 20080222222222 somebox /lib 0 Conf-002 1 1 2 OK 0.000733 100
PART taper Conf-002 1 somebox /lib 20080222222222 1/2 0 [sec 0.000733 kb 100 kps 136425.648022]
:dumpfile somebox_lib_20080222222222_p2 20080222222222 somebox /lib 0 Conf-002 2 2 2 OK 0.000428 72
PART taper Conf-002 2 somebox /lib 20080222222222 2/2 0 [sec 0.000428 kb 72 kps 136425.648022]
DONE taper somebox /lib 20080222222222 2 0 [sec 0.001161 kb 172 kps 136425.648022]
FINISH driver date 20080222222222 time 6.206

# a logfile with several dumps in it, one of which comes in many parts, and one of which is
# from a previous run
::: log.20080313133333.0
:tapelist 20080313133333 Conf-003
:timestamp 20080313133333
DISK planner somebox /usr/bin
DISK planner somebox /lib
DISK planner otherbox /lib
DISK planner otherbox /usr/bin
START planner date 20080313133333
START driver date 20080313133333
STATS driver hostname somebox
STATS driver startup time 0.059
INFO planner Full dump of somebox:/lib promoted from 2 days ahead.
FINISH planner date 20080313133333 time 0.286
SUCCESS dumper somebox /usr/bin 20080313133333 1 [sec 0.001 kb 20 kps 10352.0 orig-kb 20]
SUCCESS chunker somebox /usr/bin 20080313133333 1 [sec 1.023 kb 20 kps 50.8]
STATS driver estimate somebox /usr/bin 20080313133333 1 [sec 0 nkb 52 ckb 64 kps 1024]
START taper datestamp 20080313133333 label Conf-003 tape 1
:dumpfile somebox_usr_bin_20080313133333 20080313133333 somebox /usr/bin 1 Conf-003 1 1 1 OK 0.000370 20
PART taper Conf-003 1 somebox /usr/bin 20080313133333 1/1 1 [sec 0.000370 kb 20 kps 54054.054054]
DONE taper somebox /usr/bin 20080313133333 1 1 [sec 0.000370 kb 20 kps 54054.054054]
# a multi-part dump
SUCCESS dumper somebox /lib 20080313133333 0 [sec 0.189 kb 3156 kps 50253.1 orig-kb 3156]
SUCCESS chunker somebox /lib 20080313133333 0 [sec 5.250 kb 3156 kps 1815.5]
STATS driver estimate somebox /lib 20080313133333 0 [sec 1 nkb 3156 ckb 3156 kps 9500]
:dumpfile somebox_lib_20080313133333_p1 20080313133333 somebox /lib 0 Conf-003 2 1 10 OK 0.005621 1024
PART taper Conf-003 2 somebox /lib 20080313133333 1/10 0 [sec 0.005621 kb 1024 kps 182173.990393]
:dumpfile somebox_lib_20080313133333_p2 20080313133333 somebox /lib 0 Conf-003 3 2 10 OK 0.006527 1024
PART taper Conf-003 3 somebox /lib 20080313133333 2/10 0 [sec 0.006527 kb 1024 kps 156886.777999]
:dumpfile somebox_lib_20080313133333_p3 20080313133333 somebox /lib 0 Conf-003 4 3 10 OK 0.005854 1024
PART taper Conf-003 4 somebox /lib 20080313133333 3/10 0 [sec 0.005854 kb 1024 kps 174923.129484]
:dumpfile somebox_lib_20080313133333_p4 20080313133333 somebox /lib 0 Conf-003 5 4 10 OK 0.007344 1024
PART taper Conf-003 5 somebox /lib 20080313133333 4/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:dumpfile somebox_lib_20080313133333_p5 20080313133333 somebox /lib 0 Conf-003 6 5 10 OK 0.007344 1024
PART taper Conf-003 6 somebox /lib 20080313133333 5/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:dumpfile somebox_lib_20080313133333_p6 20080313133333 somebox /lib 0 Conf-003 7 6 10 OK 0.007344 1024
PART taper Conf-003 7 somebox /lib 20080313133333 6/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:dumpfile somebox_lib_20080313133333_p7 20080313133333 somebox /lib 0 Conf-003 8 7 10 OK 0.007344 1024
PART taper Conf-003 8 somebox /lib 20080313133333 7/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:dumpfile somebox_lib_20080313133333_p8 20080313133333 somebox /lib 0 Conf-003 9 8 10 OK 0.007344 1024
PART taper Conf-003 9 somebox /lib 20080313133333 8/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:dumpfile somebox_lib_20080313133333_p9 20080313133333 somebox /lib 0 Conf-003 10 9 10 OK 0.007344 1024
PART taper Conf-003 10 somebox /lib 20080313133333 9/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:dumpfile somebox_lib_20080313133333_p10 20080313133333 somebox /lib 0 Conf-003 11 10 10 OK 0.001919 284
PART taper Conf-003 11 somebox /lib 20080313133333 10/10 0 [sec 0.001919 kb 284 kps 147993.746743]
DONE taper somebox /lib 20080313133333 10 0 [sec 0.051436 kb 3156 kps 184695.543977]
SUCCESS dumper otherbox /lib 20080313133333 0 [sec 0.001 kb 190 kps 10352.0 orig-kb 20]
SUCCESS chunker otherbox /lib 20080313133333 0 [sec 1.023 kb 190 kps 50.8]
STATS driver estimate otherbox /lib 20080313133333 0 [sec 0 nkb 190 ckb 190 kps 1024]
# this dump is from a previous run, with an older dump_timestamp
:dumpfile otherbox_usr_bin_20080313133333 20080311131133 otherbox /usr/bin 0 Conf-003 12 1 1 OK 0.002733 240
PART taper Conf-003 12 otherbox /usr/bin 20080311131133 1/1 0 [sec 0.002733 kb 240 kps 136425.648022]
:dumpfile otherbox_lib_20080313133333 20080313133333 otherbox /lib 0 Conf-003 13 1 1 OK 0.001733 190
PART taper Conf-003 13 otherbox /lib 20080313133333 1/1 0 [sec 0.001733 kb 190 kps 136425.648022]
DONE taper otherbox /lib 20080313133333 1 0 [sec 0.001733 kb 190 kps 136425.648022]
FINISH driver date 20080313133333 time 24.777

# A logfile with some partial parts (PARTPARTIAL) in it
::: log.20080414144444.0
:tapelist 20080414144444 Conf-004
:tapelist 20080414144444 Conf-005
:timestamp 20080414144444
DISK planner otherbox /lib
START planner date 20080414144444
START driver date 20080414144444
STATS driver hostname otherbox
STATS driver startup time 0.075
INFO taper Will write new label `Conf-004' to new (previously non-amanda) tape
FINISH planner date 20080414144444 time 2.139
SUCCESS dumper otherbox /lib 20080414144444 1 [sec 0.003 kb 60 kps 16304.3 orig-kb 60]
SUCCESS chunker otherbox /lib 20080414144444 1 [sec 1.038 kb 60 kps 88.5]
STATS driver estimate otherbox /lib 20080414144444 1 [sec 0 nkb 92 ckb 96 kps 1024]
START taper datestamp 20080414144444 label Conf-004 tape 1
:dumpfile otherbox_lib_20080414144444_try1 20080414144444 otherbox /lib 1 Conf-004 1 1 1 PARTIAL 0.000707 32
PARTPARTIAL taper Conf-004 1 otherbox /lib 20080414144444 1/1 1 [sec 0.000707 kb 32 kps 45261.669024] ""
INFO taper Will request retry of failed split part.
INFO taper Will write new label `Conf-005' to new (previously non-amanda) tape
START taper datestamp 20080414144444 label Conf-005 tape 2
:dumpfile otherbox_lib_20080414144444_try2 20080414144444 otherbox /lib 1 Conf-005 1 1 1 PARTIAL 0.000540 32
PARTPARTIAL taper Conf-005 1 otherbox /lib 20080414144444 1/1 1 [sec 0.000540 kb 32 kps 59259.259259] ""
INFO taper Will request retry of failed split part.
WARNING driver Out of tapes; going into degraded mode.
PARTIAL taper otherbox /lib 20080414144444 1 1 [sec 0.000540 kb 32 kps 59259.259259] ""
FINISH driver date 20080414144444 time 6.959
