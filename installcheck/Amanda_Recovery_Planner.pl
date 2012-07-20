# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 11;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Run;
use Installcheck::Catalogs;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Debug;
use Amanda::DB::Catalog;
use Amanda::Recovery::Planner;
use Amanda::MainLoop;
use Amanda::Header;
use Amanda::Xfer qw( :constants );

# disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;
$testconf = Installcheck::Run->setup();
$testconf->write();

# install the 'bigdb' catalog to test against
my $cat = Installcheck::Catalogs::load("bigdb");
$cat->install();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

##
## Tests!
###

sub make_plan_sync {
    my $plan;

    Amanda::Recovery::Planner::make_plan(@_,
	debug => 1,
	plan_cb => sub {
	    (my $err, $plan) = @_;
	    die "$err" if $err;
	    Amanda::MainLoop::quit();
	});

    Amanda::MainLoop::run();
    return $plan;
}

sub ds {
    return Amanda::Cmdline::dumpspec_t->new($_[0], $_[1], $_[2], $_[3], undef);
}

sub is_plan {
    my ($got, $exp, $msg) = @_;
    my $got_dumps = $got->{'dumps'};

    # make an "abbreviated" version of the plan for comparison with the
    # expected
    my @got_abbrev;
    for my $d (@$got_dumps) {
	my @parts;
	push @got_abbrev, [
	    $d->{'hostname'},
	    $d->{'diskname'},
	    $d->{'dump_timestamp'},
	    "$d->{'level'}"+0, # strip bigints
	    \@parts ];

	for my $p (@{$d->{'parts'}}) {
	    next unless defined $p;
	    if (exists $p->{'holding_file'}) {
		# extract the last two filename components, since the rest is variable
		my $hf = $p->{'holding_file'};
		$hf =~ s/^.*\/([^\/]*\/[^\/]*)$/$1/;
		push @parts, $hf;
	    } else {
		push @parts,
		    $p->{'label'},
		    "$p->{filenum}"+0; # strip bigints
	    }
	}
    }

    is_deeply(\@got_abbrev, $exp, $msg)
	or diag("got:\n" . Dumper(\@got_abbrev));
}

my $changer = undef; # not needed yet

is_plan(make_plan_sync(
	    dumpspec => ds("no-box-at-all"),
	    changer => $changer),
    [ ],
    "empty plan for nonexistent host");

is_plan(make_plan_sync(
	    dumpspec => ds("oldbox", "^/opt"),
	    changer => $changer),
    [
	[   "oldbox", "/opt", "20080414144444", 0, [
		'20080414144444/oldbox._opt',
	    ],
	],
    ],
    "simple plan for a dump on holding disk");

is_plan(make_plan_sync(
	    dumpspec => ds("somebox", "^/lib", "200801"),
	    changer => $changer),
    [
	[   "somebox", "/lib", "20080111000000", 0, [
		'Conf-001' => 1,
	    ],
	],
    ],
    "simple plan for just one dump");

is_plan(make_plan_sync(
	    dumpspec => ds("somebox", "^/usr/bin"),
	    changer => $changer),
    [
        [   'somebox', '/usr/bin', '20080313133333', 1, [
		'Conf-003' => 1,
	    ],
        ],
        [   'somebox', '/usr/bin', '20080515155555', 1, [
		'Conf-006' => 1,
	    ]
        ],
        [   'somebox', '/usr/bin', '20080616166666', 1, [
		'Conf-007' => 1,
	    ],
        ],
    ],
    "plan for three dumps, in order by tape write time");

is_plan(make_plan_sync(
	    dumpspec => ds("otherbox", "^/lib"),
	    changer => $changer),
    [
	[   "otherbox", "/lib", "20080414144444", 1, [
		'20080414144444/otherbox._lib',
	    ],
	],
	[   'otherbox', '/lib', '20080313133333', 0, [
		'Conf-003' => 14,
	    ],
	],
	[   "otherbox", "/lib", "20080511151555", 0, [
		'Conf-006', 13,
	    ],
	],
    ],
    "plan for three dumps, one on holding disk; holding dumps prioritized first");

is_plan(make_plan_sync(
	    dumpspecs => [
		ds("somebox", "^/lib", "20080111"),
		ds("somebox", "^/lib", "20080222"),
	    ],
	    changer => $changer),
    [
	[   "somebox", "/lib", "20080111000000", 0, [
		'Conf-001' => 1,
	    ],
	],
	[	'somebox', '/lib', '20080222222222', 0, [
		'Conf-002' => 1,
		'Conf-002' => 2,
	    ],
	],
    ],
    "plan for two dumps, one of them spanned, in order by tape write time");

is_plan(make_plan_sync(
	    dumpspec => ds("somebox", "^/lib", "200803"),
	    one_dump_per_part => 1,
	    changer => $changer),
    [
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 2,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 3,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 4,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 5,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 6,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 7,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 8,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 9,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 10,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 11,
	    ],
	],
    ],
    "plan for a multipart dump, one_dump_per_part");

is_plan(make_plan_sync(
	    dumpspec => ds("oldbox", "^/opt", "20080414144444"),
	    holding_file => $cat->holding_filename('oldbox_opt_20080414144444_holding')),
    [
	[   "oldbox", "/opt", "20080414144444", 0, [
		'20080414144444/oldbox._opt',
	    ],
	],
    ],
    "make_plan creates an appropriate plan for an explicit holding-disk recovery");

is_plan(make_plan_sync(
	    holding_file => $cat->holding_filename('oldbox_opt_20080414144444_holding')),
    [
	[   "oldbox", "/opt", "20080414144444", 0, [
		'20080414144444/oldbox._opt',
	    ],
	],
    ],
    "same, without a dumpspec");

is_plan(make_plan_sync(
	    dumpspec => ds("somebox", "/lib", "20080515155555"),
	    filelist => [
		'Conf-006' => [2, 3, 4, 5,       8, 9, 10, 11],
		#  (make_plan should fill in files 6 and 7)
	    ],
	    changer => $changer),
    [
	[   'somebox', '/lib', '20080515155555', 0, [
		'Conf-006' => 2,
		'Conf-006' => 3,
		'Conf-006' => 4,
		'Conf-006' => 5,
		'Conf-006' => 6,
		'Conf-006' => 7,
		'Conf-006' => 8,
		'Conf-006' => 9,
		'Conf-006' => 10,
		'Conf-006' => 11,
	    ],
	],
    ],
    "plan based on filelist, with a dumpspec");

is_plan(make_plan_sync(
	    filelist => [
		'Conf-006' => [2, 3, 4, 5,       8, 9, 10, 11],
		#  (make_plan should fill in files 6 and 7)
	    ],
	    changer => $changer),
    [
	[   'somebox', '/lib', '20080515155555', 0, [
		'Conf-006' => 2,
		'Conf-006' => 3,
		'Conf-006' => 4,
		'Conf-006' => 5,
		'Conf-006' => 6,
		'Conf-006' => 7,
		'Conf-006' => 8,
		'Conf-006' => 9,
		'Conf-006' => 10,
		'Conf-006' => 11,
	    ],
	],
    ],
    "plan based on filelist, without a dumpspec");
