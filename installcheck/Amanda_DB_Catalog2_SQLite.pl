# Copyright (c) 2011 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 104;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Run;
use Installcheck::Catalogs;
use Installcheck::DBCatalog2;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Paths;
use Amanda::DB::Catalog2;
use Amanda::Storage;
use Amanda::Cmdline;
use Amanda::Xfer qw( :constants );
use POSIX qw( strftime );

# send xfer logging somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# set up and load a simple config
my $testconf = Installcheck::Run::setup();
$testconf->add_catalog('my_catalog', [
  'comment' => '"SQLite catalog"',
  'plugin'  => '"SQLite"',
  'property' => "\"dbname\" \"$CONFIG_DIR/TESTCONF/SQLite.db\""
]);
$testconf->add_param('catalog','"my_catalog"');
# Use add_text because it must be after the catalog definition in the config file
#$testconf->add_text(<<EOF);
#catalog "my_catalog"
#EOF

$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load test config");

unlink("$CONFIG_DIR/TESTCONF/SQLite.db");
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1);

# test functions against an empty set of logfiles

is_deeply([ $catalog->get_write_timestamps() ], [],
    "No write_timestamps in an empty catalog");

is_deeply($catalog->get_latest_write_timestamp(), undef,
    "No latest write_timestamp in an empty catalog");

is_deeply($catalog->get_latest_write_timestamp(type => 'amvault'), undef,
    "No latest write_timestamp in an empty catalog, even of a specific type");

my @dumps = $catalog->get_dumps();
is_deeply([ @dumps ], [],
    "No dumps in an empty catalog");
SKIP : {
    skip "not testing get_parts", 1;
is_deeply([ $catalog->get_parts(@dumps) ], [],
    "No parts in an empty catalog");
}

# and add some logfiles to query, and a corresponding tapelist, while also gathering
# a list of parts and dumps for comparison with the results from Amanda::DB::Catalog.
# also add some files to holding disk
my @dumpspecs;

# install the bigdb catalog
my $cat = Installcheck::Catalogs::load("bigdb");
recreate_db_catalog2('TESTCONF');
$cat->install();
my %dumps = $cat->get_dumps();
my %parts = $cat->get_parts();

unlink("$CONFIG_DIR/TESTCONF/SQLite.db");
Amanda::DB::Catalog::_clear_cache();
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, load => 1);

sub partstr {
    my ($part) = @_;
    if (exists $part->{'holding_file'}) {
	return "$part->{pool}:$part->{holding_file}: " .
	       "$part->{dump}->{hostname} $part->{dump}->{diskname} " .
	       "w$part->{dump}->{write_timestamp} d$part->{dump}->{dump_timestamp}";
   } else {
	return "$part->{pool}:$part->{label}:$part->{filenum}: " .
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
	   "$dump->{dump_timestamp} $dump->{write_timestamp} $dump->{level} $dump->{status} $dump->{dump_status} $dump->{copy_status} $dump->{nparts} $dump->{kb}"
	 . " " . Data::Dumper::Dumper($dump);
}

# filter out recursive links from dump->parts->dump, without changing
# the original objects.
sub filter_dumps {
    my ($dumps) = @_;

    my @rv;
    for my $d (@$dumps) {
	next if $d->{'status'} eq "FAIL";
	$d = do { my %t = %$d; \%t; }; # copy hash
	my @dparts = map {
		if (defined $_) {
		    my $p = do { my %t = %$_; \%t }; # copy part
		    $p->{'dump'} = undef;
		    $p->{'sec'} = undef;	# not in database
		    $p->{'kb'} = undef;		# not in dabatase
		    delete $p->{'filenum'} if !defined $p->{'filenum'} || $p->{'filenum'} == 0;
		    $p;
		} else {
		    undef;
		}
	    } @{$d->{'parts'}};
	$d->{'parts'} = [ @dparts ];
	$d->{'allparts'} = undef;
	$d->{'write_timestamp'} = $d->{'dump_timestamp'} if $d->{'write_timestamp'} == 0;
	$d->{'dump_status'} = undef;	# only in database
	$d->{'copy_status'} = undef;	# only in database
	$d->{'sec'} = undef;		# not in database
	delete $d->{'copy_id'};		# only in database
	push @rv, $d;
    }

    return \@rv;
}

use Data::Dumper;
sub got_dumps {
    my ($got, $exp, $msg, %params) = @_;

    # give a warning if we accidentally select zero dumps
    if (@$exp == 0) {
	diag("warning: zero dumps expected: $msg")
	    unless $params{'zero_dumps_expected'};
    }

    # filter recursive references to avoid confusing old is_deeply instances
    my $got_filter = filter_dumps($got);
    my @got_filter = sortdumps(@$got_filter);
    my $exp_filter = filter_dumps($exp);
    my @exp_filter = sortdumps(@$exp_filter);
    if (!is_deeply(\@got_filter, \@exp_filter, $msg)) {
	diag("got dumps:");
	for (@got_filter) {
	    diag("  " . dumpstr($_));
	}
	diag("expected dumps:");
	for (@exp_filter) {
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
	    or $a->{'dump_timestamp'} cmp $b->{'dump_timestamp'}
	    or $a->{'hostname'} cmp $b->{'hostname'}
	    or $a->{'diskname'} cmp $b->{'diskname'}
	    or $a->{'level'} <=> $b->{'level'}
	    or $a->{'storage'} cmp $b->{'storage'}
	    or $a->{'dump_status'} cmp $b->{'dump_status'}
    }
    @_;
}

##
# Test the timestamps
is_deeply([ $catalog->get_write_timestamps(), ],
    [ '20080111000000', '20080222222222', '20080313133333',
      '20080414144444', '20080515155555', '20080616166666',
      '20100722000000' ],
    "get_write_timestamps returns all logfile datestamps in proper order, with zero-padding") || diag("get_write_timestamps: " . Data::Dumper::Dumper($catalog->get_write_timestamps()));

is($catalog->get_latest_write_timestamp(), '20100722000000',
    "get_latest_write_timestamp correctly returns the latest write timestamp");

is($catalog->get_latest_write_timestamp(type => 'amdump'), '20100722000000',
    "get_latest_write_timestamp correctly returns the latest write timestamp of type amdump");

is($catalog->get_latest_write_timestamp(type => 'amflush'), '20080111000000',
    "get_latest_write_timestamp correctly returns the latest write timestamp of type amflush");

is($catalog->get_latest_write_timestamp(types => [qw(amvault amflush)]),
    '20080222222222',
    "get_latest_write_timestamp correctly returns the latest write timestamp of a set of ts's");

is($catalog->get_run_type('20080222222222'), "amvault",
    "get_run_type detects amvault");

is($catalog->get_run_type('20080111'), "amflush",
    "get_run_type detects amflush (short ts)");

is($catalog->get_run_type('20080111000000'), "amflush",
    "get_run_type detects amflush (long ts)");

##
# test get_parts and sort_parts

SKIP : {
    skip "not testing get_parts", 40;
got_parts([ sortparts $catalog->get_parts() ],
    [ sortparts parts_named qr/.*/ ],
    "get_parts returns all parts when given no parameters");
got_parts([ sortparts $catalog->get_parts(write_timestamp => '20080111000000') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts parameter write_timestamp");
got_parts([ sortparts $catalog->get_parts(write_timestamp => '20080111') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts accepts a short write_timestamp and zero-pads it");
got_parts([ sortparts $catalog->get_parts(write_timestamps => ['20080111000000','20080222222222']) ],
    [ sortparts parts_named qr/(20080111|20080222222222_p\d*)$/ ],
    "get_parts parameter write_timestamps");
got_parts([ sortparts $catalog->get_parts(write_timestamp => '20080111', holding => 1) ],
    [ ],
    "get_parts parameter write_timestamp + holding => 1 returns nothing",
    zero_parts_expected => 1);

got_parts([ sortparts $catalog->get_parts(dump_timestamp => '20080111000000') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts parameter dump_timestamp");
got_parts([ sortparts $catalog->get_parts(dump_timestamp => '20080111') ],
    [ sortparts parts_named qr/somebox_lib_20080111/ ],
    "get_parts accepts a short dump_timestamp and zero-pads it");
got_parts([ sortparts $catalog->get_parts(dump_timestamps => ['20080111000000','20080222222222']) ],
    [ sortparts parts_named qr/(20080111|20080222222222_p\d*)$/ ],
    "get_parts parameter dump_timestamps");
got_parts([ sortparts $catalog->get_parts(dump_timestamp_match => '200801-2') ],
    [ sortparts parts_named qr/20080[12]/ ],
    "get_parts parameter dump_timestamp_match");

got_parts([ sortparts $catalog->get_parts(hostname => 'otherbox') ],
    [ sortparts parts_named qr/^otherbox_/ ],
    "get_parts parameter hostname");
got_parts([ sortparts $catalog->get_parts(hostnames => ['otherbox','somebox']) ],
    [ sortparts parts_named qr/^(otherbox_|somebox_)/ ],
    "get_parts parameter hostnames");
got_parts([ sortparts $catalog->get_parts(hostname_match => '*box') ],
    [ sortparts parts_named qr/box/ ],
    "get_parts parameter hostname_match");

got_parts([ sortparts $catalog->get_parts(diskname => '/lib') ],
    [ sortparts parts_named qr/_lib_/ ],
    "get_parts parameter diskname");
got_parts([ sortparts $catalog->get_parts(disknames => ['/lib','/usr/bin']) ],
    [ sortparts parts_named qr/(_lib_|_usr_bin_)/ ],
    "get_parts parameter disknames");
got_parts([ sortparts $catalog->get_parts(diskname_match => '/usr') ],
    [ sortparts parts_named qr/_usr_/ ],
    "get_parts parameter diskname_match");

got_parts([ sortparts $catalog->get_parts(label => 'Conf-001') ],
    [ sortparts parts_matching { defined $_->{'label'} and $_->{'label'} eq 'Conf-001' } ],
    "get_parts parameter label");
got_parts([ sortparts $catalog->get_parts(labels => ['Conf-002','Conf-003']) ],
    [ sortparts parts_matching { defined $_->{'label'} and ($_->{'label'} eq 'Conf-002' or $_->{'label'} eq 'Conf-003') } ],
    "get_parts parameter labels");

got_parts([ sortparts $catalog->get_parts(level => 0) ],
    [ sortparts parts_matching { $_->{'dump'}->{'level'} == 0 } ],
    "get_parts parameter level");
got_parts([ sortparts $catalog->get_parts(levels => [ 1 ]) ],
    [ sortparts parts_matching { $_->{'dump'}->{'level'} == 1 } ],
    "get_parts parameter levels");

got_parts([ sortparts $catalog->get_parts(status => "OK") ],
    [ sortparts parts_matching { $_->{'status'} eq "OK" } ],
    "get_parts parameter status = OK");

got_parts([ sortparts $catalog->get_parts(status => "PARTIAL") ],
    [ sortparts parts_matching { $_->{'status'} eq "PARTIAL" } ],
    "get_parts parameter status = PARTIAL");

got_parts([ sortparts $catalog->get_parts(hostname => "oldbox") ],
    [ sortparts parts_named qr/^oldbox_/ ],
    "get_parts finds a holding-disk dump");

got_parts([ sortparts $catalog->get_parts(hostname => "oldbox", holding => 0) ],
    [ ],
    "get_parts ignores a holding-disk dump if holding is false",
    zero_parts_expected => 1);
got_parts([ sortparts $catalog->get_parts(hostname => "oldbox", holding => 1) ],
    [ sortparts parts_named qr/^oldbox_/ ],
    "get_parts supplies a holding-disk dump if holding is true");
got_parts([ sortparts $catalog->get_parts(hostnames => [ "oldbox", "somebox" ]) ],
    [ sortparts (parts_named qr/^oldbox_.*_holding/, parts_named qr/^somebox_/) ],
    "get_parts returns both holding and on-media dumps");
got_parts([ sortparts $catalog->get_parts(hostnames => [ "oldbox", "somebox" ],
						     holding => 1) ],
    [ sortparts parts_named qr/^oldbox_.*_holding/ ],
    "get_parts ignores an on-media dump if holding is true");

got_parts([ sortparts $catalog->get_parts(hostnames => [ "oldbox", "somebox" ],
						     holding => 0) ],
    [ sortparts parts_named qr/^somebox_/ ],
    "get_parts ignores an holding dump if holding is false");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib"], 0);
got_parts([ sortparts $catalog->get_parts(dumpspecs => [ @dumpspecs ]) ],
    [ sortparts parts_named qr/_lib_/ ],
    "get_parts parameter dumpspecs with one dumpspec");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib", "somebox"], 0);
got_parts([ sortparts $catalog->get_parts(dumpspecs => [ @dumpspecs ]) ],
    [ sortparts parts_matching { $_->{'dump'}->{'diskname'} eq '/lib'
			      or $_->{'dump'}->{'hostname'} eq 'somebox' } ],
    "get_parts parameter dumpspecs with two dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["otherbox", "*", "somebox"], 0);
got_parts([ sortparts $catalog->get_parts(dumpspecs => [ @dumpspecs ]) ],
    [ sortparts parts_matching { $_->{'dump'}->{'hostname'} eq 'otherbox'
			      or $_->{'dump'}->{'hostname'} eq 'somebox' } ],
    "get_parts parameter dumpspecs with two non-overlapping dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["otherbox", "*", "somebox"], 0);
got_parts([ sortparts $catalog->get_parts(dumpspecs => [ @dumpspecs ], holding => 1), ],
    [ sortparts parts_matching { $_->{'dump'}->{'hostname'} eq 'otherbox'
			     and exists $_->{'holding_file'} } ],
    "get_parts parameter dumpspecs with two non-overlapping dumpspecs, but holding files only");

## more complex, multi-parameter queries

got_parts([ sortparts $catalog->get_parts(hostname => 'somebox',
						     diskname_match => '/lib') ],
    [ sortparts parts_named qr/^somebox_lib_/ ],
    "get_parts parameters hostname and diskname_match");

got_parts([ sortparts $catalog->get_parts(write_timestamp => '20080313133333',
						     dump_timestamp => '20080311131133') ],
    [ sortparts parts_matching { $_->{'dump'}->{'dump_timestamp'} eq '20080311131133'
                    and $_->{'dump'}->{'write_timestamp'} eq '20080313133333' } ],
    "get_parts parameters write_timestamp and dump_timestamp");

got_parts([ sortparts $catalog->get_parts(write_timestamp => '20080414144444',
						     status => 'OK') ],
    [ ], # there were no OK dumps on that date
    "get_parts parameters write_timestamp status",
    zero_parts_expected => 1);

## test part sorting

got_parts([ $catalog->sort_parts(['write_timestamp'],
		@parts{'somebox_lib_20080222222222_p1','somebox_lib_20080111'}) ],
	      [ @parts{'somebox_lib_20080111','somebox_lib_20080222222222_p1'} ],
    "sort by write_timestamps");
got_parts([ $catalog->sort_parts(['-write_timestamp'],
		@parts{'somebox_lib_20080111','somebox_lib_20080222222222_p1'}) ],
	      [ @parts{'somebox_lib_20080222222222_p1','somebox_lib_20080111'} ],
    "sort by write_timestamps, reverse");

got_parts([ $catalog->sort_parts(['hostname', '-diskname', 'write_timestamp'],
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

got_parts([ $catalog->sort_parts(['filenum'],
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

got_parts([ $catalog->sort_parts(['-partnum'],
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

got_parts([ $catalog->sort_parts(['nparts'],
		@parts{
		    'somebox_lib_20080313133333_p9', # nparts=10
		    'somebox_lib_20080222222222_p2', # nparts=2
		    }) ],
	      [ @parts{
		    'somebox_lib_20080222222222_p2', # nparts=2
		    'somebox_lib_20080313133333_p9', # nparts=10
		    } ],
		"nparts is sorted numerically, not lexically");

got_parts([ $catalog->sort_parts(['label'],
		@parts{
		    'somebox_lib_20080313133333_p9', # Conf-003
		    'somebox_lib_20080222222222_p2', # Conf-002
		    }) ],
	      [ @parts{
		    'somebox_lib_20080222222222_p2', # Conf-002
		    'somebox_lib_20080313133333_p9', # Conf-003
		    } ],
		"labels sort correctly");
};

### test dump selecting

#diag("get_dumps : ". Data::Dumper::Dumper($catalog->get_dumps(parts => 1)) . "\n");
got_dumps([ sortdumps $catalog->get_dumps(parts => 1) ],
    [ sortdumps dumps_named qr/.*/ ],
    "get_dumps returns all dumps when given no parameters");

got_dumps([ sortdumps $catalog->get_dumps(write_timestamp => '20080111000000', parts => 1) ],
    [ sortdumps dumps_named qr/somebox_lib_20080111/ ],
    "get_dumps parameter write_timestamp");

got_dumps([ sortdumps $catalog->get_dumps(write_timestamp => '20080111', parts => 1) ],
    [ sortdumps dumps_named qr/somebox_lib_20080111/ ],
    "get_dumps accepts a short write_timestamp and zero-pads it");

got_dumps([ sortdumps $catalog->get_dumps(write_timestamps => ['20080111000000','20080222222222'], parts => 1) ],
    [ sortdumps dumps_named qr/(20080111|20080222222222)$/ ],
    "get_dumps parameter write_timestamps");

got_dumps([ sortdumps $catalog->get_dumps(hostname => 'otherbox', parts => 1) ],
    [ sortdumps dumps_named qr/^otherbox/ ],
    "get_dumps parameter hostname otherbox");

got_dumps([ sortdumps $catalog->get_dumps(hostname => 'oldbox', parts => 1) ],
    [ sortdumps dumps_named qr/^oldbox_.*_holding/ ],
    "get_dumps parameter hostname oldbox, holding");

got_dumps([ sortdumps $catalog->get_dumps(hostnames => ['notthere', 'otherbox'], parts => 1) ],
    [ sortdumps dumps_named qr/^otherbox/ ],
    "get_dumps parameter hostnames notthere|notthere");

got_dumps([ sortdumps $catalog->get_dumps(hostname_match => 'other*', parts => 1) ],
    [ sortdumps dumps_named qr/^otherbox/ ],
    "get_dumps parameter hostname_match other*");

got_dumps([ sortdumps $catalog->get_dumps(diskname => '/lib', parts => 1) ],
    [ sortdumps dumps_named qr/^[^_]*_lib_/ ],
    "get_dumps parameter diskname");

got_dumps([ sortdumps $catalog->get_dumps(disknames => ['/lib', '/usr/bin'], parts => 1) ],
    [ sortdumps dumps_named qr/^[^_]*_(usr_bin|lib)_/ ],
    "get_dumps parameter disknames");

got_dumps([ sortdumps $catalog->get_dumps(diskname_match => 'bin', parts => 1) ],
    [ sortdumps dumps_named qr/.*_bin_/ ],
    "get_dumps parameter diskname_match");

got_dumps([ sortdumps $catalog->get_dumps(dump_timestamp => '20080414144444', parts => 1) ],
    [ sortdumps dumps_matching { $_->{'dump_timestamp'} eq '20080414144444' } ],
    "get_dumps parameter dump_timestamp");

got_dumps([ sortdumps $catalog->get_dumps(
			dump_timestamps => ['20080414144444', '20080311131133'], parts => 1) ],
    [ sortdumps dumps_matching { $_->{'dump_timestamp'} eq '20080414144444'
			      or $_->{'dump_timestamp'} eq '20080311131133' } ],
    "get_dumps parameter dump_timestamps");

got_dumps([ sortdumps $catalog->get_dumps(dump_timestamp_match => '200804-7', parts => 1) ],
    [ sortdumps dumps_matching { $_->{'dump_timestamp'} =~ /^20080[4567]/ } ],
    "get_dumps parameter dump_timestamp_match");

got_dumps([ sortdumps $catalog->get_dumps(level => 0, parts => 1) ],
    [ sortdumps dumps_matching { $_->{'level'} == 0 } ],
    "get_dumps parameter level");

got_dumps([ sortdumps $catalog->get_dumps(levels => [ 1 ], parts => 1) ],
    [ sortdumps dumps_matching { $_->{'level'} == 1 } ],
    "get_dumps parameter levels");

got_dumps([ sortdumps $catalog->get_dumps(status => "OK", parts => 1) ],
    [ sortdumps dumps_matching { $_->{'status'} eq "OK" } ],
    "get_dumps parameter status = OK");

got_dumps([ sortdumps $catalog->get_dumps(status => "PARTIAL", parts => 1) ],
    [ sortdumps dumps_matching { $_->{'status'} eq "PARTIAL" } ],
    "get_dumps parameter status = PARTIAL");

got_dumps([ sortdumps $catalog->get_dumps(status => "FAIL", parts => 1) ],
    [ sortdumps dumps_matching { $_->{'status'} eq "FAIL" } ],
    "get_dumps parameter status = FAIL");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib"], 0);
got_dumps([ sortdumps $catalog->get_dumps(dumpspecs => [ @dumpspecs ], parts => 1) ],
    [ sortdumps dumps_named qr/_lib_/ ],
    "get_dumps parameter dumpspecs with one dumpspec");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/lib", "somebox"], 0);
got_dumps([ sortdumps $catalog->get_dumps(dumpspecs => [ @dumpspecs ], parts => 1) ],
    [ sortdumps dumps_matching { $_->{'diskname'} eq '/lib'
			      or $_->{'hostname'} eq 'somebox' } ],
    "get_dumps parameter dumpspecs with two dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["otherbox", "*", "somebox"], 0);
got_dumps([ sortdumps $catalog->get_dumps(dumpspecs => [ @dumpspecs ], parts => 1) ],
    [ sortdumps dumps_matching { $_->{'hostname'} eq 'otherbox'
			      or $_->{'hostname'} eq 'somebox' } ],
    "get_dumps parameter dumpspecs with two non-overlapping dumpspecs");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["does-not-exist"], 0);
got_dumps([ sortdumps $catalog->get_dumps(dumpspecs => [ @dumpspecs ], parts => 1) ],
    [ ],
    "get_dumps parameter dumpspecs with a dumpspec that matches nothing",
    zero_dumps_expected => 1);

@dumpspecs = Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, undef, '20080222222222');
got_dumps([ sortdumps $catalog->get_dumps(dumpspecs => [ @dumpspecs ], parts => 1) ],
    [ sortdumps dumps_matching { $_->{'write_timestamp'} eq '20080222222222' }],
    "get_dumps parameter dumpspecs with write_timestamp");

## test dump sorting

got_dumps([ $catalog->sort_dumps(['write_timestamp'],
		@dumps{'somebox_lib_20080222222222','somebox_lib_20080111'}) ],
	      [ @dumps{'somebox_lib_20080111','somebox_lib_20080222222222'} ],
    "sort dumps by write_timestamps");
got_dumps([ $catalog->sort_dumps(['-write_timestamp'],
		@dumps{'somebox_lib_20080111','somebox_lib_20080222222222'}) ],
	      [ @dumps{'somebox_lib_20080222222222','somebox_lib_20080111'} ],
    "sort dumps by write_timestamps, reverse");

got_dumps([ $catalog->sort_dumps(['hostname', '-diskname', 'write_timestamp'],
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

got_dumps([ $catalog->sort_dumps(['nparts'],
		@dumps{
		    'somebox_lib_20080313133333', # nparts=10
		    'somebox_lib_20080222222222', # nparts=2
		    }) ],
	      [ @dumps{
		    'somebox_lib_20080222222222', # nparts=2
		    'somebox_lib_20080313133333', # nparts=10
		    } ],
		"dumps' nparts is sorted numerically, not lexically");

got_dumps([ $catalog->sort_dumps(['-level'],
		@dumps{
		    'somebox_lib_20080313133333', # level=0
		    'somebox_usr_bin_20080313133333', # level=1
		    }) ],
	      [ @dumps{
		    'somebox_usr_bin_20080313133333', # level=1
		    'somebox_lib_20080313133333', # level=0
		    } ],
		"sort dumps by level, reversed");

got_dumps([ $catalog->sort_dumps(['dump_timestamp'],
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
recreate_db_catalog2('TESTCONF');
$cat->install();
%dumps = $cat->get_dumps();
%parts = $cat->get_parts();

unlink("$CONFIG_DIR/TESTCONF/SQLite.db");
Amanda::DB::Catalog::_clear_cache();
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, load => 1);

got_dumps([ sortdumps $catalog->get_dumps(parts => 1) ],
    [ sortdumps dumps_named qr/.*/ ],
    "get_dumps returns all dumps when given no parameters (multi-taper)");

SKIP : {
    skip "not running get_parts tests", 1;
got_parts([ sortparts $catalog->get_parts() ],
	[ sortparts parts_named qr/.*/ ],
	"get_parts returns all parts when given no parameters (multi-taper)");
};

$testconf = Installcheck::Run::setup();
$testconf->add_policy('policy_retention_tapes', [
	'retention_tapes' => '5'
]);
$testconf->add_changer('changer_retention_tapes', [
]);
$testconf->add_storage('storage_retention_tapes', [
	'policy' => '"policy_retention_tapes"',
	'tapepool' => '"pool_retention_tapes"',
	'tpchanger' => '"changer_retention_tapes"'
]);

$testconf->add_policy('policy_retention_days', [
	'retention_days' => '5'
]);
$testconf->add_changer('changer_retention_days', [
]);
$testconf->add_storage('storage_retention_days', [
	'policy' => '"policy_retention_days"',
	'tapepool' => '"pool_retention_days"',
	'tpchanger' => '"changer_retention_days"'
]);

$testconf->add_policy('policy_retention_full', [
	'retention_full' => '5'
]);
$testconf->add_changer('changer_retention_full', [
]);
$testconf->add_storage('storage_retention_full', [
	'policy' => '"policy_retention_full"',
	'tapepool' => '"pool_retention_full"',
	'tpchanger' => '"changer_retention_full"'
]);

$testconf->add_policy('policy_retention_recover', [
	'retention_recover' => '5'
]);
$testconf->add_changer('changer_retention_recover', [
]);
$testconf->add_storage('storage_retention_recover', [
	'policy' => '"policy_retention_recover"',
	'tapepool' => '"pool_retention_recover"',
	'tpchanger' => '"changer_retention_recover"'
]);
$testconf->add_param('storage', '"storage_retention_tapes" "storage_retention_days" "storage_retention_full" "storage_retention_recover"');
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load test config");
unlink("$CONFIG_DIR/TESTCONF/SQLite.db");
$catalog = Amanda::DB::Catalog2->new(undef, create => 1);

my $image;
my $copy_tapes;
my $copy_days;
my $copy_full;
my $copy_recover;
my $volume_tapes;
my $volume_days;
my $volume_full;
my $volume_recover;
my @volumes;

my $epoc = time();
my $time1 = $epoc - 9*24*60*60;
my $time2 = $epoc - 8*24*60*60;
my $time3 = $epoc - 7*24*60*60;
my $time4 = $epoc - 6*24*60*60;
my $time5 = $epoc - 5*24*60*60;
my $time6 = $epoc - 4*24*60*60;
my $time7 = $epoc - 3*24*60*60;
my $time8 = $epoc - 2*24*60*60;
my $time9 = $epoc - 1*24*60*60;
my $time10 = $epoc;
my $date1= strftime("%Y%m%d%H%M%S", localtime($time1 - 1));
my $date2= strftime("%Y%m%d%H%M%S", localtime($time2 - 1));
my $date3= strftime("%Y%m%d%H%M%S", localtime($time3 - 1));
my $date4= strftime("%Y%m%d%H%M%S", localtime($time4 - 1));
my $date5= strftime("%Y%m%d%H%M%S", localtime($time5 - 1));
my $date6= strftime("%Y%m%d%H%M%S", localtime($time6 - 1));
my $date7= strftime("%Y%m%d%H%M%S", localtime($time7 - 1));
my $date8= strftime("%Y%m%d%H%M%S", localtime($time8 - 1));
my $date9= strftime("%Y%m%d%H%M%S", localtime($time9 - 1));
my $date10= strftime("%Y%m%d%H%M%S", localtime($time10 - 1));

my $storage_tape = Amanda::Storage->new("storage_retention_tapes", no_changer => 1);
my $storage_days = Amanda::Storage->new("storage_retention_days", no_changer => 1);
my $storage_full = Amanda::Storage->new("storage_retention_full", no_changer => 1);
my $storage_recover = Amanda::Storage->new("storage_retention_recover", no_changer => 1);

$image = $catalog->add_image("localhost", "disk1", "disk1", $date1, 0, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date1, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-001", $date1, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date1, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date1, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-001", $date1, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date1, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date1, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-001", $date1, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date1, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date1, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-001", $date1, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date1, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$image = $catalog->add_image("localhost", "disk1", "disk1", $date2, 1, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date2, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-002", $date2, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date2, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date2, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-002", $date2, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date2, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date2, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-002", $date2, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date2, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date2, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-002", $date2, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date2, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$image = $catalog->add_image("localhost", "disk1", "disk1", $date3, 0, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date3, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-003", $date3, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date3, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date3, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-003", $date3, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date3, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date3, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-003", $date3, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date3, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date3, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-003", $date3, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date3, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$image = $catalog->add_image("localhost", "disk1", "disk1", $date4, 1, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date4, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-004", $date4, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date4, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date4, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-004", $date4, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date4, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date4, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-004", $date4, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date4, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date4, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-004", $date4, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date4, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$image = $catalog->add_image("localhost", "disk1", "disk1", $date5, 1, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date5, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-005", $date5, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date5, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date5, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-005", $date5, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date5, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date5, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-005", $date5, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date5, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date5, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-005", $date5, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date5, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time5);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'tapes-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'days-001',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date1,
             'storage' => 'storage_retention_full',
             'meta' => '',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-001',
             'retention_full' => 1,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-001',
             'retention_recover' => 1,
             'blocksize' => 32,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'write_timestamp' => $date1
           },
#4
           {
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'tapes-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'write_timestamp' => $date2
           },
           {
             'blocksize' => 32,
             'label' => 'days-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date2,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 1,
             'label' => 'recover-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'storage' => 'storage_retention_recover',
             'meta' => ''
           },
#8
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'write_timestamp' => $date3,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_recover' => 0,
             'label' => 'tapes-003',
             'blocksize' => 32
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'days-003',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date3,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-003',
             'retention_full' => 1,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'recover-003',
             'retention_recover' => 1
           },
#12
           {
             'retention_recover' => 0,
             'label' => 'tapes-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_full' => 0,
             'reuse' => 1
           },
           {
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'days-004',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date4
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'full-004',
             'retention_recover' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 1,
             'label' => 'recover-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'meta' => '',
             'storage' => 'storage_retention_recover'
           },
#16
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'tapes-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date5
           },
           {
             'retention_days' => 1,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_days',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'days-005',
             'blocksize' => 32,
             'write_timestamp' => $date5,
             'storage' => 'storage_retention_days',
             'meta' => ''
           },
           {
             'blocksize' => 32,
             'label' => 'full-005',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date5,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-005',
             'retention_recover' => 1,
             'blocksize' => 32,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date5
           }
         ],
         "time5"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date6, 0, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date6, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-006", $date6, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date6, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date6, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-006", $date6, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date6, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date6, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-006", $date6, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date6, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date6, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-006", $date6, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date6, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time6);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'tapes-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'days-001',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date1,
             'storage' => 'storage_retention_full',
             'meta' => '',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-001',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-001',
             'retention_recover' => 1,
             'blocksize' => 32,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'write_timestamp' => $date1
           },
#4
           {
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'tapes-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'write_timestamp' => $date2
           },
           {
             'blocksize' => 32,
             'label' => 'days-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date2,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 1,
             'label' => 'recover-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'storage' => 'storage_retention_recover',
             'meta' => ''
           },
#8
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'write_timestamp' => $date3,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_recover' => 0,
             'label' => 'tapes-003',
             'blocksize' => 32
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'days-003',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date3,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-003',
             'retention_full' => 1,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'recover-003',
             'retention_recover' => 1
           },
#12
           {
             'retention_recover' => 0,
             'label' => 'tapes-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_full' => 0,
             'reuse' => 1
           },
           {
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'days-004',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date4
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'full-004',
             'retention_recover' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 1,
             'label' => 'recover-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'meta' => '',
             'storage' => 'storage_retention_recover'
           },
#16
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'tapes-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date5
           },
           {
             'retention_days' => 1,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_days',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'days-005',
             'blocksize' => 32,
             'write_timestamp' => $date5,
             'storage' => 'storage_retention_days',
             'meta' => ''
           },
           {
             'blocksize' => 32,
             'label' => 'full-005',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date5,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-005',
             'retention_recover' => 1,
             'blocksize' => 32,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date5
           },
#20
           {
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-006',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-006',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date6,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-006',
             'blocksize' => 32,
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date6,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-006',
             'blocksize' => 32
           }
         ],
         "time6"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date7, 1, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date7, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-007", $date7, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date7, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date7, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-007", $date7, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date7, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date7, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-007", $date7, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date7, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date7, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-007", $date7, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date7, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time7);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'tapes-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'days-001',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date1,
             'storage' => 'storage_retention_full',
             'meta' => '',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-001',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-001',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'write_timestamp' => $date1
           },
#4
           {
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'tapes-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'write_timestamp' => $date2
           },
           {
             'blocksize' => 32,
             'label' => 'days-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date2,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'recover-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'storage' => 'storage_retention_recover',
             'meta' => ''
           },
#8
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'write_timestamp' => $date3,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_recover' => 0,
             'label' => 'tapes-003',
             'blocksize' => 32
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'days-003',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date3,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-003',
             'retention_full' => 1,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'recover-003',
             'retention_recover' => 1
           },
#12
           {
             'retention_recover' => 0,
             'label' => 'tapes-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_full' => 0,
             'reuse' => 1
           },
           {
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'days-004',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date4
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'full-004',
             'retention_recover' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 1,
             'label' => 'recover-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'meta' => '',
             'storage' => 'storage_retention_recover'
           },
#16
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'tapes-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date5
           },
           {
             'retention_days' => 1,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_days',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'days-005',
             'blocksize' => 32,
             'write_timestamp' => $date5,
             'storage' => 'storage_retention_days',
             'meta' => ''
           },
           {
             'blocksize' => 32,
             'label' => 'full-005',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date5,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-005',
             'retention_recover' => 1,
             'blocksize' => 32,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date5
           },
#20
           {
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-006',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-006',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date6,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-006',
             'blocksize' => 32,
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date6,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-006',
             'blocksize' => 32
           },
#24
           {
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-007',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-007',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date7,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-007',
             'blocksize' => 32,
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date7,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-007',
             'blocksize' => 32
           }
         ],
         "time7"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date8, 0, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date8, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-008", $date8, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date8, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date8, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-008", $date8, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date8, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date8, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-008", $date8, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date8, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date8, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-008", $date8, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date8, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time8);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'tapes-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'days-001',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date1,
             'storage' => 'storage_retention_full',
             'meta' => '',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-001',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-001',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'write_timestamp' => $date1
           },
#4
           {
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'tapes-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'write_timestamp' => $date2
           },
           {
             'blocksize' => 32,
             'label' => 'days-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date2,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'recover-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'storage' => 'storage_retention_recover',
             'meta' => ''
           },
#8
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'write_timestamp' => $date3,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_recover' => 0,
             'label' => 'tapes-003',
             'blocksize' => 32
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'days-003',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date3,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-003',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'recover-003',
             'retention_recover' => 1
           },
#12
           {
             'retention_recover' => 0,
             'label' => 'tapes-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_tape' => 1,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_full' => 0,
             'reuse' => 1
           },
           {
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'days-004',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date4
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'full-004',
             'retention_recover' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 1,
             'label' => 'recover-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'meta' => '',
             'storage' => 'storage_retention_recover'
           },
#16
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'tapes-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date5
           },
           {
             'retention_days' => 1,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_days',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'days-005',
             'blocksize' => 32,
             'write_timestamp' => $date5,
             'storage' => 'storage_retention_days',
             'meta' => ''
           },
           {
             'blocksize' => 32,
             'label' => 'full-005',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date5,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-005',
             'retention_recover' => 1,
             'blocksize' => 32,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date5
           },
#20
           {
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-006',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-006',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date6,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-006',
             'blocksize' => 32,
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date6,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-006',
             'blocksize' => 32
           },
#24
           {
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-007',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-007',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date7,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-007',
             'blocksize' => 32,
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date7,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-007',
             'blocksize' => 32
           },
#28
           {
             'write_timestamp' => $date8,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-008',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-008',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date8,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-008',
             'blocksize' => 32,
             'write_timestamp' => $date8,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date8,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-008',
             'blocksize' => 32
           }
         ],
         "time8"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date9, 1, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date9, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-009", $date9, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date9, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date9, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-009", $date9, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date9, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date9, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-009", $date9, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date9, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date9, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-009", $date9, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date9, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time9);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'tapes-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'days-001',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date1,
             'storage' => 'storage_retention_full',
             'meta' => '',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-001',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-001',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'write_timestamp' => $date1
           },
#4
           {
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'tapes-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'write_timestamp' => $date2
           },
           {
             'blocksize' => 32,
             'label' => 'days-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date2,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'recover-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'storage' => 'storage_retention_recover',
             'meta' => ''
           },
#8
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'write_timestamp' => $date3,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_recover' => 0,
             'label' => 'tapes-003',
             'blocksize' => 32
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'days-003',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date3,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-003',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'recover-003',
             'retention_recover' => 1
           },
#12
           {
             'retention_recover' => 0,
             'label' => 'tapes-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_full' => 0,
             'reuse' => 1
           },
           {
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'days-004',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date4
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'full-004',
             'retention_recover' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'recover-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'meta' => '',
             'storage' => 'storage_retention_recover'
           },
#16
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 1,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'tapes-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date5
           },
           {
             'retention_days' => 1,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_days',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'days-005',
             'blocksize' => 32,
             'write_timestamp' => $date5,
             'storage' => 'storage_retention_days',
             'meta' => ''
           },
           {
             'blocksize' => 32,
             'label' => 'full-005',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date5,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-005',
             'retention_recover' => 1,
             'blocksize' => 32,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date5
           },
#20
           {
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-006',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-006',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date6,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-006',
             'blocksize' => 32,
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date6,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-006',
             'blocksize' => 32
           },
#24
           {
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-007',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-007',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date7,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-007',
             'blocksize' => 32,
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date7,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-007',
             'blocksize' => 32
           },
#28
           {
             'write_timestamp' => $date8,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-008',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-008',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date8,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-008',
             'blocksize' => 32,
             'write_timestamp' => $date8,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date8,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-008',
             'blocksize' => 32
           },
#32
           {
             'write_timestamp' => $date9,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-009',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-009',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date9,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-009',
             'blocksize' => 32,
             'write_timestamp' => $date9,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date9,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-009',
             'blocksize' => 32
           }
         ],
         "time9"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date10, 0, "0", $$);

$copy_tapes = $image->add_copy("storage_retention_tapes", $date10, 0, 0, 0, $$);
$volume_tapes = $catalog->add_volume("pool_retention_tapes", "tapes-010", $date10, "storage_retention_tapes", undef, undef, 32, 1, 0, 0, 0, 1);
$copy_tapes->add_part($volume_tapes, 0, 1024000, 1, 1, "OK");
$copy_tapes->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date10, $storage_tape);

$copy_days = $image->add_copy("storage_retention_days", $date10, 1, 0, 0, $$);
$volume_days = $catalog->add_volume("pool_retention_days", "days-010", $date10, "storage_retention_days", undef, undef, 32, 1, 1, 0, 0, 0);
$copy_days->add_part($volume_days, 0, 1024000, 1, 1, "OK");
$copy_days->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date10, $storage_days);

$copy_full = $image->add_copy("storage_retention_full", $date10, 0, 1, 0, $$);
$volume_full = $catalog->add_volume("pool_retention_full", "full-010", $date10, "storage_retention_full", undef, undef, 32, 1, 0, 1, 0, 0);
$copy_full->add_part($volume_full, 0, 1024000, 1, 1, "OK");
$copy_full->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date10, $storage_full);

$copy_recover = $image->add_copy("storage_retention_recover", $date10, 0, 0, 1, $$);
$volume_recover = $catalog->add_volume("pool_retention_recover", "recover-010", $date10, "storage_retention_recover", undef, undef, 32, 1, 0, 0, 1, 0);
$copy_recover->add_part($volume_recover, 0, 1024000, 1, 1, "OK");
$copy_recover->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");
$volume_tapes->set_write_timestamp($date10, $storage_recover);

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time10);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'tapes-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'days-001',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date1,
             'storage' => 'storage_retention_full',
             'meta' => '',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-001',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-001',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'write_timestamp' => $date1
           },
#4
           {
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'tapes-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'write_timestamp' => $date2
           },
           {
             'blocksize' => 32,
             'label' => 'days-002',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date2,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'recover-002',
             'blocksize' => 32,
             'write_timestamp' => $date2,
             'storage' => 'storage_retention_recover',
             'meta' => ''
           },
#8
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'write_timestamp' => $date3,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_recover' => 0,
             'label' => 'tapes-003',
             'blocksize' => 32
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'days-003',
             'retention_recover' => 0
           },
           {
             'write_timestamp' => $date3,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'full-003',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF'
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'recover-003',
             'retention_recover' => 0
           },
#12
           {
             'retention_recover' => 0,
             'label' => 'tapes-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'retention_tape' => 0,
             'retention_days' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_full' => 0,
             'reuse' => 1
           },
           {
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'blocksize' => 32,
             'label' => 'days-004',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_days',
             'write_timestamp' => $date4
           },
           {
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'full-004',
             'retention_recover' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'recover-004',
             'blocksize' => 32,
             'write_timestamp' => $date4,
             'meta' => '',
             'storage' => 'storage_retention_recover'
           },
#16
           {
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0,
             'pool' => 'pool_retention_tapes',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'tapes-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_tapes',
             'meta' => '',
             'write_timestamp' => $date5
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_days',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'days-005',
             'blocksize' => 32,
             'write_timestamp' => $date5,
             'storage' => 'storage_retention_days',
             'meta' => ''
           },
           {
             'blocksize' => 32,
             'label' => 'full-005',
             'retention_recover' => 0,
             'meta' => '',
             'storage' => 'storage_retention_full',
             'write_timestamp' => $date5,
             'pool' => 'pool_retention_full',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 0,
             'pool' => 'pool_retention_recover',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0,
             'label' => 'recover-005',
             'retention_recover' => 0,
             'blocksize' => 32,
             'meta' => '',
             'storage' => 'storage_retention_recover',
             'write_timestamp' => $date5
           },
#20
           {
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-006',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-006',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date6,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-006',
             'blocksize' => 32,
             'write_timestamp' => $date6,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date6,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-006',
             'blocksize' => 32
           },
#24
           {
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-007',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-007',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date7,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-007',
             'blocksize' => 32,
             'write_timestamp' => $date7,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date7,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-007',
             'blocksize' => 32
           },
#28
           {
             'write_timestamp' => $date8,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-008',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-008',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date8,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-008',
             'blocksize' => 32,
             'write_timestamp' => $date8,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date8,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-008',
             'blocksize' => 32
           },
#32
           {
             'write_timestamp' => $date9,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-009',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-009',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date9,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 0,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-009',
             'blocksize' => 32,
             'write_timestamp' => $date9,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date9,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-009',
             'blocksize' => 32
           },
#36
           {
             'write_timestamp' => $date10,
             'meta' => '',
             'storage' => 'storage_retention_tapes',
             'blocksize' => 32,
             'retention_recover' => 0,
             'label' => 'tapes-010',
             'retention_full' => 0,
             'reuse' => 1,
             'barcode' => undef,
             'pool' => 'pool_retention_tapes',
             'retention_days' => 0,
             'retention_tape' => 1,
             'config' => 'TESTCONF'
           },
           {
             'label' => 'days-010',
             'retention_recover' => 0,
             'blocksize' => 32,
             'storage' => 'storage_retention_days',
             'meta' => '',
             'write_timestamp' => $date10,
             'config' => 'TESTCONF',
             'retention_tape' => 0,
             'retention_days' => 1,
             'pool' => 'pool_retention_days',
             'barcode' => undef,
             'reuse' => 1,
             'retention_full' => 0
           },
           {
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_full',
             'retention_full' => 1,
             'reuse' => 1,
             'retention_recover' => 0,
             'label' => 'full-010',
             'blocksize' => 32,
             'write_timestamp' => $date10,
             'meta' => '',
             'storage' => 'storage_retention_full'
           },
           {
             'retention_full' => 0,
             'reuse' => 1,
             'retention_days' => 0,
             'retention_tape' => 0,
             'config' => 'TESTCONF',
             'barcode' => undef,
             'pool' => 'pool_retention_recover',
             'write_timestamp' => $date10,
             'storage' => 'storage_retention_recover',
             'meta' => '',
             'retention_recover' => 1,
             'label' => 'recover-010',
             'blocksize' => 32
           }
         ],
         "time10"
) or diag("". Data::Dumper::Dumper(\@volumes));

$testconf = Installcheck::Run::setup();
$testconf->add_policy('policy_retention', [
	'retention_tapes' => '2',
	'retention_days' => '2',
	'retention_full' => '2',
	'retention_recover' => '2'
]);
$testconf->add_changer('changer_retention', [
]);
$testconf->add_storage('storage_retention', [
	'policy' => '"policy_retention"',
	'tapepool' => '"pool_retention"',
	'tpchanger' => '"changer_retention"'
]);

$testconf->add_param('storage', '"storage_retention"');
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load test config");
unlink("$CONFIG_DIR/TESTCONF/SQLite.db");
$catalog = Amanda::DB::Catalog2->new(undef, create => 1);

my $storage_retention = Amanda::Storage->new("storage_retention", no_changer => 1);
my $copy;
my $volume1;
my $volume2;

$image = $catalog->add_image("localhost", "disk1", "disk1", $date1, 0, "0", $$);

$copy = $image->add_copy("storage_retention", $date1, 0, 0, 0, $$);
$volume1 = $catalog->add_volume("pool_retention", "retention-001", $date1, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume1, 0, 1024000, 1, 1, "OK");
$volume1->set_write_timestamp($date1, $storage_retention);
$volume2 = $catalog->add_volume("pool_retention", "retention-002", $date1, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume2, 0, 1024000, 1, 1, "OK");
$volume2->set_write_timestamp($date1, $storage_retention);
$copy->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time1);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-001',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-002',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           }
         ],
         "retention time1"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date2, 1, "0", $$);

$copy = $image->add_copy("storage_retention", $date2, 1, 0, 0, $$);
$volume1 = $catalog->add_volume("pool_retention", "retention-003", $date2, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume1, 0, 1024000, 1, 1, "OK");
$volume1->set_write_timestamp($date2, $storage_retention);
$volume2 = $catalog->add_volume("pool_retention", "retention-004", $date2, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume2, 0, 1024000, 1, 1, "OK");
$volume2->set_write_timestamp($date2, $storage_retention);
$copy->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time2);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-001',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-002',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
#2
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-003',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-004',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           }
         ],
         "retention time2"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date3, 1, "0", $$);

$copy = $image->add_copy("storage_retention", $date3, 0, 0, 0, $$);
$volume1 = $catalog->add_volume("pool_retention", "retention-005", $date3, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume1, 0, 1024000, 1, 1, "OK");
$volume1->set_write_timestamp($date3, $storage_retention);
$volume2 = $catalog->add_volume("pool_retention", "retention-006", $date3, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume2, 0, 1024000, 1, 1, "OK");
$volume2->set_write_timestamp($date3, $storage_retention);
$copy->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time3);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-001',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-002',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
#2
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-003',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-004',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
#4
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'retention-005',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'retention-006',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           }
         ],
         "retention time3"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date4, 0, "0", $$);

$copy = $image->add_copy("storage_retention", $date4, 0, 0, 0, $$);
$volume1 = $catalog->add_volume("pool_retention", "retention-007", $date4, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume1, 0, 1024000, 1, 1, "OK");
$volume1->set_write_timestamp($date4, $storage_retention);
$volume2 = $catalog->add_volume("pool_retention", "retention-008", $date4, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume2, 0, 1024000, 1, 1, "OK");
$volume2->set_write_timestamp($date4, $storage_retention);
$copy->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time4);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-001',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-002',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
#2
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-003',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-004',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
#4
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'retention-005',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'retention-006',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
#6
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'retention-007',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'retention-008',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           }
         ],
         "retention time4"
) or diag("". Data::Dumper::Dumper(\@volumes));

$image = $catalog->add_image("localhost", "disk1", "disk1", $date5, 0, "0", $$);

$copy = $image->add_copy("storage_retention", $date5, 0, 0, 0, $$);
$volume1 = $catalog->add_volume("pool_retention", "retention-009", $date5, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume1, 0, 1024000, 1, 1, "OK");
$volume1->set_write_timestamp($date5, $storage_retention);
$volume2 = $catalog->add_volume("pool_retention", "retention-010", $date5, "storage_retention", undef, undef, 32, 1, 0, 0, 0, 1);
$copy->add_part($volume2, 0, 1024000, 1, 1, "OK");
$volume2->set_write_timestamp($date5, $storage_retention);
$copy->finish_copy(1, 1000, 1024000, "OK", "abcdef12:1024000");

$image->finish_image(1000, "OK", 100, 10, "abcdef12:1024000", "abcdef12:1024000", "abcdef12:1024000");

$catalog->_compute_retention($time5);
@volumes = $catalog->find_volumes(no_bless => 1);
is_deeply(\@volumes,
	  [
#0
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-001',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date1,
             'blocksize' => 32,
             'label' => 'retention-002',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
#2
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-003',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date2,
             'blocksize' => 32,
             'label' => 'retention-004',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
#4
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'retention-005',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date3,
             'blocksize' => 32,
             'label' => 'retention-006',
             'retention_recover' => 0,
             'reuse' => 1,
             'retention_full' => 0,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 0,
             'retention_tape' => 0
           },
#6
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'retention-007',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date4,
             'blocksize' => 32,
             'label' => 'retention-008',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 0
           },
#8
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date5,
             'blocksize' => 32,
             'label' => 'retention-009',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           },
           {
             'storage' => 'storage_retention',
             'meta' => '',
             'write_timestamp' => $date5,
             'blocksize' => 32,
             'label' => 'retention-010',
             'retention_recover' => 1,
             'reuse' => 1,
             'retention_full' => 1,
             'pool' => 'pool_retention',
             'barcode' => undef,
             'config' => 'TESTCONF',
             'retention_days' => 1,
             'retention_tape' => 1
           }
         ],
         "retention time5"
) or diag("". Data::Dumper::Dumper(\@volumes));

