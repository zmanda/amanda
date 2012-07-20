# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 85;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Run;
use Installcheck::Catalogs;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::DB::Catalog;
use Amanda::Cmdline;
use Amanda::Xfer qw( :constants );

# send xfer logging somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# set up and load a simple config
my $testconf = Installcheck::Run->setup();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load test config");

# test functions against an empty set of logfiles

is_deeply([ Amanda::DB::Catalog::get_write_timestamps() ], [],
    "No write_timestamps in an empty catalog");

is_deeply(Amanda::DB::Catalog::get_latest_write_timestamp(), undef,
    "No latest write_timestamp in an empty catalog");

is_deeply(Amanda::DB::Catalog::get_latest_write_timestamp(type => 'amvault'), undef,
    "No latest write_timestamp in an empty catalog, even of a specific type");

is_deeply([ Amanda::DB::Catalog::get_parts() ], [],
    "No parts in an empty catalog");

# and add some logfiles to query, and a corresponding tapelist, while also gathering
# a list of parts and dumps for comparison with the results from Amanda::DB::Catalog.
# also add some files to holding disk
my @dumpspecs;

# install the bigdb catalog
my $cat = Installcheck::Catalogs::load("bigdb");
$cat->install();
my %dumps = $cat->get_dumps();
my %parts = $cat->get_parts();

Amanda::DB::Catalog::_clear_cache();

sub partstr {
    my ($part) = @_;
    if (exists $part->{'holding_file'}) {
	return "$part->{holding_file}: " .
	       "$part->{dump}->{hostname} $part->{dump}->{diskname} " .
	       "w$part->{dump}->{write_timestamp} d$part->{dump}->{dump_timestamp}";
   } else {
	return "$part->{label}:$part->{filenum}: " .
	       "$part->{dump}->{hostname} $part->{dump}->{diskname} $part->{dump}->{orig_kb} " .
	       "w$part->{dump}->{write_timestamp} d$part->{dump}->{dump_timestamp}";
   }
}

# filter out recursive links from part->dump->parts, without changing
# the original objects.  Note that this removes the object-identity of
# dumps.
sub filter_parts {
    my ($parts) = @_;

    my @rv;
    for my $p (@$parts) {
	$p = do { my %t = %$p; \%t; }; # copy hash
	$p->{'dump'} = do { my %t = %{$p->{'dump'}}; \%t; };
	$p->{'dump'}->{'parts'} = undef;
	push @rv, $p;
    }

    return \@rv;
}

sub got_parts {
    my ($got, $exp, $msg, %params) = @_;

    # give a warning if we accidentally select zero parts
    if (@$exp == 0) {
	diag("warning: zero parts expected: $msg")
	    unless $params{'zero_parts_expected'};
    }

    # filter recursive references to avoid confusing old is_deeply instances
    if (!is_deeply(filter_parts($got), filter_parts($exp), $msg)) {
	diag("got parts:");
	my $i = 0;
	for (@$got) {
	    diag(" [$i]  " . partstr($_));
	    $i++;
	}

	diag("expected parts:");
	$i = 0;
	for (@$exp) {
	    diag(" [$i]  " . partstr($_));
	    $i++;
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
	$_->{'dump'}->{'bytes'} = "$_->{dump}->{bytes}" + 0;
	$_->{'dump'}->{'kb'} = "$_->{dump}->{kb}" + 0;
	$_->{'dump'}->{'orig_kb'} = "$_->{dump}->{orig_kb}" + 0;
	if (!defined $_->{filenum}) {
	    $_->{'filenum'} = 0;
	} else {
	    $_->{'filenum'} = "$_->{filenum}" + 0;
	}
	$_->{'kb'} = "$_->{kb}" + 0;
	$_->{'orig_kb'} = "$_->{orig_kb}" + 0;
	$_->{'partnum'} = "$_->{partnum}" + 0;
	$_;
    } sort {
	if (exists $a->{'holding_file'} and exists $b->{'holding_file'}) {
	    return $a->{'holding_file'} cmp $b->{'holding_file'};
	} elsif (not exists $a->{'holding_file'} and not exists $b->{'holding_file'}) {
	    return ($a->{'label'} cmp $b->{'label'})
		|| ($a->{'filenum'} <=> $b->{'filenum'});
	} else {
	    return (exists $a->{'holding_file'})? 1 : -1;
	}
    }
    @_;
}

sub dumpstr {
    my ($dump) = @_;
    return "$dump->{hostname} $dump->{diskname} " .
	   "$dump->{dump_timestamp} $dump->{level}";
}

# filter out recursive links from dump->parts->dump, without changing
# the original objects.
sub filter_dumps {
    my ($dumps) = @_;

    my @rv;
    for my $d (@$dumps) {
	$d = do { my %t = %$d; \%t; }; # copy hash
	my @dparts = map {
		return undef unless defined $_;
		my $p = do { my %t = %$_; \%t }; # copy part
		$p->{'dump'} = undef;
		$p;
	    } @{$d->{'parts'}};
	$d->{'parts'} = [ @dparts ];
	push @rv, $d;
    }

    return \@rv;
}

sub got_dumps {
    my ($got, $exp, $msg, %params) = @_;

    # give a warning if we accidentally select zero dumps
    if (@$exp == 0) {
	diag("warning: zero dumps expected: $msg")
	    unless $params{'zero_dumps_expected'};
    }

    # filter recursive references to avoid confusing old is_deeply instances
    if (!is_deeply(filter_dumps($got), filter_dumps($exp), $msg)) {
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
	$_->{'bytes'} = "$_->{bytes}" + 0;
	$_->{'kb'} = "$_->{kb}" + 0;
	$_->{'orig_kb'} = "$_->{orig_kb}" + 0;
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

##
# Test the timestamps

is_deeply([ Amanda::DB::Catalog::get_write_timestamps(), ],
    [ '20080111000000', '20080222222222', '20080313133333',
      '20080414144444', '20080515155555', '20080616166666',
      '20100722000000' ],
    "get_write_timestamps returns all logfile datestamps in proper order, with zero-padding");

is(Amanda::DB::Catalog::get_latest_write_timestamp(), '20100722000000',
    "get_latest_write_timestamp correctly returns the latest write timestamp");

is(Amanda::DB::Catalog::get_latest_write_timestamp(type => 'amdump'), '20100722000000',
    "get_latest_write_timestamp correctly returns the latest write timestamp of type amdump");

is(Amanda::DB::Catalog::get_latest_write_timestamp(type => 'amflush'), '20080111000000',
    "get_latest_write_timestamp correctly returns the latest write timestamp of type amflush");

is(Amanda::DB::Catalog::get_latest_write_timestamp(types => [qw(amvault amflush)]),
    '20080222222222',
    "get_latest_write_timestamp correctly returns the latest write timestamp of a set of ts's");

is(Amanda::DB::Catalog::get_run_type('20080222222222'), "amvault",
    "get_run_type detects amvault");

is(Amanda::DB::Catalog::get_run_type('20080111'), "amflush",
    "get_run_type detects amflush (short ts)");

is(Amanda::DB::Catalog::get_run_type('20080111000000'), "amflush",
    "get_run_type detects amflush (long ts)");

##
# test get_parts and sort_parts

got_parts([ sortparts Amanda::DB::Catalog::get_parts() ],
    [ sortparts parts_named qr/.*/ ],
    "get_parts returns all parts when given no parameters");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080111000000') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts parameter write_timestamp");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080111') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts accepts a short write_timestamp and zero-pads it");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamps => ['20080111000000','20080222222222']) ],
    [ sortparts parts_named qr/(20080111|20080222222222_p\d*)$/ ],
    "get_parts parameter write_timestamps");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(write_timestamp => '20080111', holding => 1) ],
    [ ],
    "get_parts parameter write_timestamp + holding => 1 returns nothing",
    zero_parts_expected => 1);

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
    [ sortparts parts_matching { defined $_->{'label'} and $_->{'label'} eq 'Conf-001' } ],
    "get_parts parameter label");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(labels => ['Conf-002','Conf-003']) ],
    [ sortparts parts_matching { defined $_->{'label'} and ($_->{'label'} eq 'Conf-002' or $_->{'label'} eq 'Conf-003') } ],
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

got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostname => "oldbox") ],
    [ sortparts parts_named qr/^oldbox_/ ],
    "get_parts finds a holding-disk dump");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostname => "oldbox", holding => 0) ],
    [ ],
    "get_parts ignores a holding-disk dump if holding is false",
    zero_parts_expected => 1);
got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostname => "oldbox", holding => 1) ],
    [ sortparts parts_named qr/^oldbox_/ ],
    "get_parts supplies a holding-disk dump if holding is true");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostnames => [ "oldbox", "somebox" ]) ],
    [ sortparts (parts_named qr/^oldbox_.*_holding/, parts_named qr/^somebox_/) ],
    "get_parts returns both holding and on-media dumps");
got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostnames => [ "oldbox", "somebox" ],
						     holding => 1) ],
    [ sortparts parts_named qr/^oldbox_.*_holding/ ],
    "get_parts ignores an on-media dump if holding is true");

got_parts([ sortparts Amanda::DB::Catalog::get_parts(hostnames => [ "oldbox", "somebox" ],
						     holding => 0) ],
    [ sortparts parts_named qr/^somebox_/ ],
    "get_parts ignores an holding dump if holding is false");

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

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["otherbox", "*", "somebox"], 0);
got_parts([ sortparts Amanda::DB::Catalog::get_parts(dumpspecs => [ @dumpspecs ], holding => 1), ],
    [ sortparts parts_matching { $_->{'dump'}->{'hostname'} eq 'otherbox'
			     and exists $_->{'holding_file'} } ],
    "get_parts parameter dumpspecs with two non-overlapping dumpspecs, but holding files only");

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

got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(hostname => 'oldbox') ],
    [ sortdumps dumps_named qr/^oldbox_.*_holding/ ],
    "get_dumps parameter hostname, holding");

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

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib"], 0);
got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dumpspecs => [ @dumpspecs ]) ],
    [ sortdumps dumps_named qr/_lib_/ ],
    "get_dumps parameter dumpspecs with one dumpspec");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib"], 0);
got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dumpspecs => [ @dumpspecs ], holding => 1) ],
    [ sortdumps dumps_named qr/_lib_.*_holding/ ],
    "get_dumps parameter dumpspecs with one dumpspec");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib", "somebox"], 0);
got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dumpspecs => [ @dumpspecs ]) ],
    [ sortdumps dumps_matching { $_->{'diskname'} eq '/lib'
			      or $_->{'hostname'} eq 'somebox' } ],
    "get_dumps parameter dumpspecs with two dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["otherbox", "*", "somebox"], 0);
got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dumpspecs => [ @dumpspecs ]) ],
    [ sortdumps dumps_matching { $_->{'hostname'} eq 'otherbox'
			      or $_->{'hostname'} eq 'somebox' } ],
    "get_dumps parameter dumpspecs with two non-overlapping dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["does-not-exist"], 0);
got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dumpspecs => [ @dumpspecs ]) ],
    [ ],
    "get_dumps parameter dumpspecs with a dumpspec that matches nothing",
    zero_dumps_expected => 1);

@dumpspecs = Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, undef, '20080222222222');
got_dumps([ sortdumps Amanda::DB::Catalog::get_dumps(dumpspecs => [ @dumpspecs ]) ],
    [ sortdumps dumps_matching { $_->{'write_timestamp'} eq '20080222222222' }],
    "get_dumps parameter dumpspecs with write_timestamp");

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
		    'otherbox_usr_bin_20080313133333_1', # dts=20080311131133
		    }) ],
	      [ @dumps{
		    'otherbox_usr_bin_20080313133333_1', # dts=20080311131133
		    'somebox_lib_20080313133333', # dts=20080313133333
		    } ],
		"sort dumps by write_timestamp");


# install the multi-taper catalog
$cat = Installcheck::Catalogs::load("multi-taper");
$cat->install();
%dumps = $cat->get_dumps();
%parts = $cat->get_parts();

Amanda::DB::Catalog::_clear_cache();

got_parts([ sortparts Amanda::DB::Catalog::get_parts() ],
	[ sortparts parts_named qr/.*/ ],
	"get_parts returns all parts when given no parameters");

