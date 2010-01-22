# Copyright (c) 2008, 2010 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 62;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::DB::Catalog;
use Amanda::Cmdline;

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

is_deeply([ Amanda::DB::Catalog::get_parts() ], [],
    "No parts in an empty catalog");

# and add some logfiles to query, and a corresponding tapelist, while also gathering
# a list of parts and dumps for comparison with the results from Amanda::DB::Catalog
my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
my $tapelist_fn = config_dir_relative(getconf($CNF_TAPELIST));
my $output;
my $write_timestamp;
my (%parts, %dumps, $last_dump);
my @dumpspecs;
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

    # new dump
    if (/^:dump (\S+) (\S+) (\S+) (\S+) (\d+) (\S+) (\S+) (\d+) (\S+) (\d+)/) {
	$last_dump = $dumps{$1} = {
	    'dump_timestamp' => $2,	'hostname' => $3,	    'diskname' => $4,
	    'level' => $5+0,		'status' => $6,		    'message' => $7,
	    'nparts' => $8,		'sec' => $9+0.0,	    'kb' => $10,
	    'write_timestamp' => $write_timestamp,
	};
	$last_dump->{'message'} = ''
	    if $last_dump->{'message'} eq '""';
	next;
    }

    # new part
    if (/^:part (\S+) (\S+) (\S+) (\d+) (\d+) (\S+) (\S+) (\d+)/) {
	$parts{$1} = {
	    'dump' => $dumps{$2},	'label' => $3,		    'filenum' => $4,
	    'partnum' => $5,		'status' => $6,		    'sec' => $7+0.0,
	    'kb' => $8,
	};
	$last_dump->{'parts'}->[$parts{$1}->{'partnum'}] = $parts{$1};
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
# test get_parts and sort_parts

sub partstr {
    my ($part) = @_;
    return "$part->{label}:$part->{filenum}: " .
	   "$part->{dump}->{hostname} $part->{dump}->{diskname}";
}

sub got_parts {
    my ($got, $exp, $msg, %params) = @_;

    # give a warning if we accidentally select zero parts
    if (@$exp == 0) {
	diag("warning: zero parts expected: $msg")
	    unless $params{'zero_parts_expected'};
    }
    if (!is_deeply($got, $exp, $msg)) {
	diag("got parts:");
	for (@$got) {
	    diag("  " . partstr($_));
	}
	diag("expected parts:");
	for (@$exp) {
	    diag("  " . partstr($_));
	}
	return '';
    }
    return 1;
}

# get parts filtered by a regexp on the key
sub parts_named($) {
    my ($expr) = @_; 
    my @selected_keys = grep { $_ =~ $expr } keys %parts;
    return map { $parts{$_} } @selected_keys;
}

# get parts filtered by an expression on the dumpfile itself
sub parts_matching(&) {
    my ($block) = @_; 
    return grep { &$block } values %parts;
}

# put @_ in a canonical order
sub sortparts {
    map {
	# convert bigints to strings and on to integers so is_deeply doesn't get confused
	$_->{'dump'}->{'level'} = "$_->{dump}->{level}" + 0;
	$_->{'dump'}->{'kb'} = "$_->{dump}->{kb}" + 0;
	$_->{'filenum'} = "$_->{filenum}" + 0;
	$_->{'kb'} = "$_->{kb}" + 0;
	$_->{'partnum'} = "$_->{partnum}" + 0;
	$_;
    } sort {
	$a->{'label'} cmp $b->{'label'}
	    or $a->{'filenum'} <=> $b->{'filenum'}
    }
    @_;
}

sub dumpstr {
    my ($dump) = @_;
    return "$dump->{hostname} $dump->{diskname} " .
	   "$dump->{dump_timestamp} $dump->{level}";
}

sub got_dumps {
    my ($got, $exp, $msg, %params) = @_;

    # give a warning if we accidentally select zero dumps
    if (@$exp == 0) {
	diag("warning: zero dumps expected: $msg")
	    unless $params{'zero_dumps_expected'};
    }
    if (!is_deeply($got, $exp, $msg)) {
	diag("got dumps:");
	for (@$got) {
	    diag("  " . dumpstr($_));
	}
	diag("expected dumps:");
	for (@$exp) {
	    diag("  " . dumpstr($_));
	}
	return '';
    }
    return 1;
}

# get dumps filtered by a regexp on the key
sub dumps_named($) {
    my ($expr) = @_; 
    my @selected_keys = grep { $_ =~ $expr } keys %dumps;
    return map { $dumps{$_} } @selected_keys;
}

# get dumps filtered by an expression on the dumpfile itself
sub dumps_matching(&) {
    my ($block) = @_; 
    return grep { &$block } values %dumps;
}

# put @_ in a canonical order
sub sortdumps {
    map {
	# convert bigints to strings and on to integers so is_deeply doesn't get confused
	$_->{'level'} = "$_->{level}" + 0;
	$_->{'kb'} = "$_->{kb}" + 0;
	$_->{'nparts'} = "$_->{nparts}" + 0;
	$_;
    } sort {
	$a->{'write_timestamp'} cmp $b->{'write_timestamp'}
	    or $a->{'hostname'} cmp $b->{'hostname'}
	    or $a->{'diskname'} cmp $b->{'diskname'}
	    or $a->{'level'} <=> $b->{'level'}
    }
    @_;
}

### test part selecting

got_parts([ sortparts Amanda::DB::Catalog::get_parts() ],
    [ sortparts parts_named qr/.*/ ],
    "get_parts returns all dumps when given no parameters");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080111000000') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts parameter write_timestamp");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080111') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts accepts a short write_timestamp and zero-pads it");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamps => ['20080111000000','20080222222222']) ],
    [ sortparts parts_named qr/(20080111|20080222222222_p\d*)$/ ],
    "get_parts parameter write_timestamps");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(dump_timestamp => '20080111000000') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts parameter dump_timestamp");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dump_timestamp => '20080111') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts accepts a short dump_timestamp and zero-pads it");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dump_timestamps => ['20080111000000','20080222222222']) ],
    [ sortparts parts_named qr/(20080111|20080222222222_p\d*)$/ ],
    "get_parts parameter dump_timestamps");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dump_timestamp_match => '200801-2') ],
    [ sortparts parts_named qr/20080[12]/ ],
    "get_parts parameter dump_timestamp_match");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostname => 'otherbox') ],
    [ sortparts parts_named qr/^otherbox_/ ],
    "get_parts parameter hostname");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostnames => ['otherbox','somebox']) ],
    [ sortparts parts_named qr/^(otherbox_|somebox_)/ ],
    "get_parts parameter hostnames");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostname_match => '*box') ],
    [ sortparts parts_named qr/box/ ],
    "get_parts parameter hostname_match");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(diskname => '/lib') ],
    [ sortparts parts_named qr/_lib_/ ],
    "get_parts parameter diskname");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(disknames => ['/lib','/usr/bin']) ],
    [ sortparts parts_named qr/(_lib_|_usr_bin_)/ ],
    "get_parts parameter disknames");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(diskname_match => '/usr') ],
    [ sortparts parts_named qr/_usr_/ ],
    "get_parts parameter diskname_match");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(label => 'Conf-001') ],
    [ sortparts parts_matching { $_->{'label'} eq 'Conf-001' } ],
    "get_parts parameter label");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(labels => ['Conf-002','Conf-003']) ],
    [ sortparts parts_matching { $_->{'label'} eq 'Conf-002' or $_->{'label'} eq 'Conf-003' } ],
    "get_parts parameter labels");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(level => 0) ],
    [ sortparts parts_matching { $_->{'dump'}->{'level'} == 0 } ],
    "get_parts parameter level");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(levels => [ 1 ]) ],
    [ sortparts parts_matching { $_->{'dump'}->{'level'} == 1 } ],
    "get_parts parameter levels");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(status => "OK") ],
    [ sortparts parts_matching { $_->{'status'} eq "OK" } ],
    "get_parts parameter status = OK");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(status => "PARTIAL") ],
    [ sortparts parts_matching { $_->{'status'} eq "PARTIAL" } ],
    "get_parts parameter status = PARTIAL");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib"], 0);
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dumpspecs => [ @dumpspecs ]) ],
    [ sortparts parts_named qr/_lib_/ ],
    "get_parts parameter dumpspecs with one dumpspec");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib", "somebox"], 0);
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dumpspecs => [ @dumpspecs ]) ],
    [ sortparts parts_matching { $_->{'dump'}->{'diskname'} eq '/lib'
			      or $_->{'dump'}->{'hostname'} eq 'somebox' } ],
    "get_parts parameter dumpspecs with two dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["otherbox", "*", "somebox"], 0);
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dumpspecs => [ @dumpspecs ]) ],
    [ sortparts parts_matching { $_->{'dump'}->{'hostname'} eq 'otherbox'
			      or $_->{'dump'}->{'hostname'} eq 'somebox' } ],
    "get_parts parameter dumpspecs with two non-overlapping dumpspecs");

## more complex, multi-parameter queries

got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostname => 'somebox',
						     diskname_match => '/lib') ],
    [ sortparts parts_named qr/^somebox_lib_/ ],
    "get_parts parameters hostname and diskname_match");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080313133333',
						     dump_timestamp => '20080311131133') ],
    [ sortparts parts_matching { $_->{'dump'}->{'dump_timestamp'} eq '20080311131133' 
                    and $_->{'dump'}->{'write_timestamp'} eq '20080313133333' } ],
    "get_parts parameters write_timestamp and dump_timestamp");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080414144444',
						     status => 'OK') ],
    [ ], # there were no OK dumps on that date
    "get_parts parameters write_timestamp status",
    zero_parts_expected => 1);

## test part sorting

got_parts([ Amanda::DB::Catalog::sort_parts(['write_timestamp'],
		@parts{'somebox_lib_20080222222222_p1','somebox_lib_20080111'}) ],
	      [ @parts{'somebox_lib_20080111','somebox_lib_20080222222222_p1'} ],
    "sort by write_timestamps");
got_parts([ Amanda::DB::Catalog::sort_parts(['-write_timestamp'],
		@parts{'somebox_lib_20080111','somebox_lib_20080222222222_p1'}) ],
	      [ @parts{'somebox_lib_20080222222222_p1','somebox_lib_20080111'} ],
    "sort by write_timestamps, reverse");

got_parts([ Amanda::DB::Catalog::sort_parts(['hostname', '-diskname', 'write_timestamp'],
		@parts{
		    'somebox_lib_20080222222222_p1',
		    'somebox_usr_bin_20080313133333',
		    'somebox_lib_20080313133333_p4',
		    'otherbox_lib_20080313133333',
		    'somebox_lib_20080111',
		    }) ],
	      [ @parts{
		    'otherbox_lib_20080313133333',
		    'somebox_usr_bin_20080313133333',
		    'somebox_lib_20080111',
		    'somebox_lib_20080222222222_p1',
		    'somebox_lib_20080313133333_p4',
	            } ],
    "multi-key sort");

got_parts([ Amanda::DB::Catalog::sort_parts(['filenum'],
		@parts{
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p1',
		    }) ],
	      [ @parts{
		    'somebox_lib_20080313133333_p1',
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p10',
		    } ],
		"filenum is sorted numerically, not lexically");

got_parts([ Amanda::DB::Catalog::sort_parts(['-partnum'],
		@parts{
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p1',
		    }) ],
	      [ @parts{
		    'somebox_lib_20080313133333_p10',
		    'somebox_lib_20080313133333_p9',
		    'somebox_lib_20080313133333_p1',
		    } ],
		"partnum is sorted numerically (and in reverse!), not lexically");

got_parts([ Amanda::DB::Catalog::sort_parts(['nparts'],
		@parts{
		    'somebox_lib_20080313133333_p9', # nparts=10
		    'somebox_lib_20080222222222_p2', # nparts=2
		    }) ],
	      [ @parts{
		    'somebox_lib_20080222222222_p2', # nparts=2
		    'somebox_lib_20080313133333_p9', # nparts=10
		    } ],
		"nparts is sorted numerically, not lexically");

got_parts([ Amanda::DB::Catalog::sort_parts(['label'],
		@parts{
		    'somebox_lib_20080313133333_p9', # Conf-003
		    'somebox_lib_20080222222222_p2', # Conf-002
		    }) ],
	      [ @parts{
		    'somebox_lib_20080222222222_p2', # Conf-002
		    'somebox_lib_20080313133333_p9', # Conf-003
		    } ],
		"labels sort correctly");

### test dump selecting

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps() ],
    [ sortdumps dumps_named qr/.*/ ],
    "get_dumps returns all dumps when given no parameters");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamp => '20080111000000') ],
    [ sortdumps dumps_named qr/somebox_lib_20080111/ ],
    "get_dumps parameter write_timestamp");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamp => '20080111') ],
    [ sortdumps dumps_named qr/somebox_lib_20080111/ ],
    "get_dumps accepts a short write_timestamp and zero-pads it");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(write_timestamps => ['20080111000000','20080222222222']) ],
    [ sortdumps dumps_named qr/(20080111|20080222222222)$/ ],
    "get_dumps parameter write_timestamps");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(hostname => 'otherbox') ],
    [ sortdumps dumps_named qr/^otherbox/ ],
    "get_dumps parameter hostname");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(hostnames => ['notthere', 'otherbox']) ],
    [ sortdumps dumps_named qr/^otherbox/ ],
    "get_dumps parameter hostnames");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(hostname_match => 'other*') ],
    [ sortdumps dumps_named qr/^otherbox/ ],
    "get_dumps parameter hostname_match");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(diskname => '/lib') ],
    [ sortdumps dumps_named qr/^[^_]*_lib_/ ],
    "get_dumps parameter diskname");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(disknames => ['/lib', '/usr/bin']) ],
    [ sortdumps dumps_named qr/^[^_]*_(usr_bin|lib)_/ ],
    "get_dumps parameter disknames");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(diskname_match => 'bin') ],
    [ sortdumps dumps_named qr/.*_bin_/ ],
    "get_dumps parameter diskname_match");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dump_timestamp => '20080414144444') ],
    [ sortdumps dumps_matching { $_->{'dump_timestamp'} eq '20080414144444' } ],
    "get_dumps parameter dump_timestamp");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(
			dump_timestamps => ['20080414144444', '20080311131133']) ],
    [ sortdumps dumps_matching { $_->{'dump_timestamp'} eq '20080414144444' 
			      or $_->{'dump_timestamp'} eq '20080311131133' } ],
    "get_dumps parameter dump_timestamps");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dump_timestamp_match => '200804-7') ],
    [ sortdumps dumps_matching { $_->{'dump_timestamp'} =~ /^20080[4567]/ } ],
    "get_dumps parameter dump_timestamp_match");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(level => 0) ],
    [ sortdumps dumps_matching { $_->{'level'} == 0 } ],
    "get_dumps parameter level");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(levels => [ 1 ]) ],
    [ sortdumps dumps_matching { $_->{'level'} == 1 } ],
    "get_dumps parameter levels");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(status => "OK") ],
    [ sortdumps dumps_matching { $_->{'status'} eq "OK" } ],
    "get_dumps parameter status = OK");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(status => "PARTIAL") ],
    [ sortdumps dumps_matching { $_->{'status'} eq "PARTIAL" } ],
    "get_dumps parameter status = PARTIAL");

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(status => "FAIL") ],
    [ sortdumps dumps_matching { $_->{'status'} eq "FAIL" } ],
    "get_dumps parameter status = FAIL");

## test dump sorting

got_dumps([ Amanda::DB::Catalog::sort_dumps(['write_timestamp'],
		@dumps{'somebox_lib_20080222222222','somebox_lib_20080111'}) ],
	      [ @dumps{'somebox_lib_20080111','somebox_lib_20080222222222'} ],
    "sort dumps by write_timestamps");
got_dumps([ Amanda::DB::Catalog::sort_dumps(['-write_timestamp'],
		@dumps{'somebox_lib_20080111','somebox_lib_20080222222222'}) ],
	      [ @dumps{'somebox_lib_20080222222222','somebox_lib_20080111'} ],
    "sort dumps by write_timestamps, reverse");

got_dumps([ Amanda::DB::Catalog::sort_dumps(['hostname', '-diskname', 'write_timestamp'],
		@dumps{
		    'somebox_lib_20080222222222',
		    'somebox_usr_bin_20080313133333',
		    'somebox_lib_20080313133333',
		    'otherbox_lib_20080313133333',
		    'somebox_lib_20080111',
		    }) ],
	      [ @dumps{
		    'otherbox_lib_20080313133333',
		    'somebox_usr_bin_20080313133333',
		    'somebox_lib_20080111',
		    'somebox_lib_20080222222222',
		    'somebox_lib_20080313133333',
	            } ],
		"multi-key dump sort");

got_dumps([ Amanda::DB::Catalog::sort_dumps(['nparts'],
		@dumps{
		    'somebox_lib_20080313133333', # nparts=10
		    'somebox_lib_20080222222222', # nparts=2
		    }) ],
	      [ @dumps{
		    'somebox_lib_20080222222222', # nparts=2
		    'somebox_lib_20080313133333', # nparts=10
		    } ],
		"dumps' nparts is sorted numerically, not lexically");

got_dumps([ Amanda::DB::Catalog::sort_dumps(['-level'],
		@dumps{
		    'somebox_lib_20080313133333', # level=0
		    'somebox_usr_bin_20080313133333', # level=1
		    }) ],
	      [ @dumps{
		    'somebox_usr_bin_20080313133333', # level=1
		    'somebox_lib_20080313133333', # level=0
		    } ],
		"sort dumps by level, reversed");

got_dumps([ Amanda::DB::Catalog::sort_dumps(['dump_timestamp'],
		@dumps{
		    'somebox_lib_20080313133333', # dts=20080313133333
		    'otherbox_usr_bin_20080313133333', # dts=20080311131133
		    }) ],
	      [ @dumps{
		    'otherbox_usr_bin_20080313133333', # dts=20080311131133
		    'somebox_lib_20080313133333', # dts=20080313133333
		    } ],
		"sort dumps by write_timestamp");

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
:dump somebox_lib_20080111 20080111000000 somebox /lib 0 OK "" 1 4.813543 419
:part somebox_lib_20080111 somebox_lib_20080111 Conf-001 1 1 OK 4.813543 419
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
:dump somebox_lib_20080222222222 20080222222222 somebox /lib 0 OK "" 2 0.001161 172
:part somebox_lib_20080222222222_p1 somebox_lib_20080222222222 Conf-002 1 1 OK 0.000733 100
PART taper Conf-002 1 somebox /lib 20080222222222 1/2 0 [sec 0.000733 kb 100 kps 136425.648022]
:part somebox_lib_20080222222222_p2 somebox_lib_20080222222222 Conf-002 2 2 OK 0.000428 72
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
:dump somebox_usr_bin_20080313133333 20080313133333 somebox /usr/bin 1 OK "" 1 0.000370 20
:part somebox_usr_bin_20080313133333 somebox_usr_bin_20080313133333 Conf-003 1 1 OK 0.000370 20
PART taper Conf-003 1 somebox /usr/bin 20080313133333 1/1 1 [sec 0.000370 kb 20 kps 54054.054054]
DONE taper somebox /usr/bin 20080313133333 1 1 [sec 0.000370 kb 20 kps 54054.054054]
# a multi-part dump
SUCCESS dumper somebox /lib 20080313133333 0 [sec 0.189 kb 3156 kps 50253.1 orig-kb 3156]
SUCCESS chunker somebox /lib 20080313133333 0 [sec 5.250 kb 3156 kps 1815.5]
STATS driver estimate somebox /lib 20080313133333 0 [sec 1 nkb 3156 ckb 3156 kps 9500]
:dump somebox_lib_20080313133333 20080313133333 somebox /lib 0 OK "" 10 0.051436 3156
:part somebox_lib_20080313133333_p1 somebox_lib_20080313133333 Conf-003 2 1 OK 0.005621 1024
PART taper Conf-003 2 somebox /lib 20080313133333 1/10 0 [sec 0.005621 kb 1024 kps 182173.990393]
:part somebox_lib_20080313133333_p2 somebox_lib_20080313133333 Conf-003 3 2 OK 0.006527 1024
PART taper Conf-003 3 somebox /lib 20080313133333 2/10 0 [sec 0.006527 kb 1024 kps 156886.777999]
:part somebox_lib_20080313133333_p3 somebox_lib_20080313133333 Conf-003 4 3 OK 0.005854 1024
PART taper Conf-003 4 somebox /lib 20080313133333 3/10 0 [sec 0.005854 kb 1024 kps 174923.129484]
:part somebox_lib_20080313133333_p4 somebox_lib_20080313133333 Conf-003 5 4 OK 0.007344 1024
PART taper Conf-003 5 somebox /lib 20080313133333 4/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:part somebox_lib_20080313133333_p5 somebox_lib_20080313133333 Conf-003 6 5 OK 0.007344 1024
PART taper Conf-003 6 somebox /lib 20080313133333 5/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:part somebox_lib_20080313133333_p6 somebox_lib_20080313133333 Conf-003 7 6 OK 0.007344 1024
PART taper Conf-003 7 somebox /lib 20080313133333 6/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:part somebox_lib_20080313133333_p7 somebox_lib_20080313133333 Conf-003 8 7 OK 0.007344 1024
PART taper Conf-003 8 somebox /lib 20080313133333 7/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:part somebox_lib_20080313133333_p8 somebox_lib_20080313133333 Conf-003 9 8 OK 0.007344 1024
PART taper Conf-003 9 somebox /lib 20080313133333 8/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:part somebox_lib_20080313133333_p9 somebox_lib_20080313133333 Conf-003 10 9 OK 0.007344 1024
PART taper Conf-003 10 somebox /lib 20080313133333 9/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
:part somebox_lib_20080313133333_p10 somebox_lib_20080313133333 Conf-003 11 10 OK 0.001919 284
PART taper Conf-003 11 somebox /lib 20080313133333 10/10 0 [sec 0.001919 kb 284 kps 147993.746743]
DONE taper somebox /lib 20080313133333 10 0 [sec 0.051436 kb 3156 kps 184695.543977]
SUCCESS dumper otherbox /lib 20080313133333 0 [sec 0.001 kb 190 kps 10352.0 orig-kb 20]
SUCCESS chunker otherbox /lib 20080313133333 0 [sec 1.023 kb 190 kps 50.8]
STATS driver estimate otherbox /lib 20080313133333 0 [sec 0 nkb 190 ckb 190 kps 1024]
# this dump is from a previous run, with an older dump_timestamp
:dump otherbox_usr_bin_20080313133333 20080311131133 otherbox /usr/bin 0 OK "" 1 0.002733 240
:part otherbox_usr_bin_20080313133333 otherbox_usr_bin_20080313133333 Conf-003 12 1 OK 0.002733 240
PART taper Conf-003 12 otherbox /usr/bin 20080311131133 1/1 0 [sec 0.002733 kb 240 kps 136425.648022]
DONE taper otherbox /usr/bin 20080311131133 1 0 [sec 0.002733 kb 240 kps 136425.648022]
:dump otherbox_lib_20080313133333 20080313133333 otherbox /lib 0 OK "" 1 0.001733 190
:part otherbox_lib_20080313133333 otherbox_lib_20080313133333 Conf-003 13 1 OK 0.001733 190
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
:dump otherbox_lib_20080414144444 20080414144444 otherbox /lib 1 PARTIAL full-up 0 0.000540 32
:part otherbox_lib_20080414144444_try1 otherbox_lib_20080414144444 Conf-004 1 1 PARTIAL 0.000707 32
PARTPARTIAL taper Conf-004 1 otherbox /lib 20080414144444 1/1 1 [sec 0.000707 kb 32 kps 45261.669024] ""
INFO taper Will request retry of failed split part.
INFO taper Will write new label `Conf-005' to new (previously non-amanda) tape
START taper datestamp 20080414144444 label Conf-005 tape 2
:part otherbox_lib_20080414144444_try2 otherbox_lib_20080414144444 Conf-005 1 1 PARTIAL 0.000540 32
PARTPARTIAL taper Conf-005 1 otherbox /lib 20080414144444 1/1 1 [sec 0.000540 kb 32 kps 59259.259259] ""
INFO taper Will request retry of failed split part.
WARNING driver Out of tapes; going into degraded mode.
PARTIAL taper otherbox /lib 20080414144444 1 1 [sec 0.000540 kb 32 kps 59259.259259] "full-up"
# a completely failed dump
:dump otherbox_boot_20080414144444 20080414144444 otherbox /boot 0 FAIL no-space 0 0.0 0
FAIL taper otherbox /boot 20080414144444 0 "no-space"
FINISH driver date 20080414144444 time 6.959
