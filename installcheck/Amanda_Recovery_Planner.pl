# Copyright (c) 2010 Zmanda Inc.  All Rights Reserved.
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
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Debug;
use Amanda::DB::Catalog;
use Amanda::Recovery::Planner;
use Amanda::MainLoop;
use Amanda::Header;
use Amanda::Xfer qw( :constants );

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;
$testconf = Installcheck::Run->setup();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

##
# Fill in some fake logfiles

my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
my $tapelist_fn = config_dir_relative(getconf($CNF_TAPELIST));
my $holdingdir = "$Installcheck::TMP/holding";
my %holding_filenames;
my $write_timestamp;
my $output;

sub make_holding_file {
    my ($name, $dump) = @_;

    my $dir = "$holdingdir/$dump->{dump_timestamp}";
    my $safe_disk = $dump->{'diskname'};
    $safe_disk =~ tr{/}{_};
    my $filename = "$dir/$dump->{hostname}.$safe_disk";
    mkpath($dir);

    # save the filename for later
    $holding_filenames{$name} = $filename;

    # (note that multi-chunk holding files are not used at this point)
    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = $dump->{'dump_timestamp'};
    $hdr->{'dumplevel'} = $dump->{'level'};
    $hdr->{'name'} = $dump->{'hostname'};
    $hdr->{'disk'} = $dump->{'diskname'};
    $hdr->{'program'} = "INSTALLCHECK";
    $hdr->{'is_partial'} = ($dump->{'status'} ne 'OK');

    open(my $fh, ">", $filename) or die("opening '$filename': $!");
    print $fh $hdr->to_string(32768,32768);

    # transfer some data to that file
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Pattern->new(1024*$dump->{'kb'}, "+-+-+-+-"),
	Amanda::Xfer::Dest::Fd->new($fh),
    ]);

    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    close($fh);

    return $filename;
}

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

    # new holding-disk file
    if (/^:holding (\S+) (\S+) (\S+) (\S+) (\d+) (\S+) (\d+)/) {
	my $dump = {
	    'dump_timestamp' => $2,	'hostname' => $3,	    'diskname' => $4,
	    'level' => $5+0,		'status' => $6,		    'kb' => $7,
	};
	make_holding_file($1, $dump);
	next;
    }


    die("syntax error") if (/^:/);

    print $output $_;
}
close($output);
close($tapelist);

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
    return Amanda::Cmdline::dumpspec_t->new($_[0], $_[1], $_[2], $_[3]);
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
	    dumpspec => ds("somebox", "^/lib"),
	    changer => $changer),
    [
	[   "somebox", "/lib", "20080111000000", 0, [
		'Conf-001' => 1,
	    ],
	],
	[   "somebox", "/lib", "20080313133333", 0, [
		'Conf-003' => 2,
		'Conf-003' => 3,
		'Conf-003' => 4,
		'Conf-003' => 5,
		'Conf-003' => 6,
		'Conf-003' => 7,
		'Conf-003' => 8,
		'Conf-003' => 9,
		'Conf-003' => 10,
		'Conf-003' => 11,
	    ],
	],
    ],
    "plan for two dumps, in order by tape write time");

is_plan(make_plan_sync(
	    dumpspec => ds("otherbox", "^/lib"),
	    changer => $changer),
    [
	[   "otherbox", "/lib", "20080414144444", 1, [
		'20080414144444/otherbox._lib',
	    ],
	],
	[   "otherbox", "/lib", "20080313133333", 0, [
		'Conf-003', 13,
	    ],
	],
    ],
    "plan for a two dumps, one on holding disk; holding dumps prioritized first");

is_plan(make_plan_sync(
	    dumpspecs => [
		ds("somebox", "^/lib", "20080111"),
		ds("euclid", "/home/dustin/code/backuppc"),
	    ],
	    changer => $changer),
    [
	[   "somebox", "/lib", "20080111000000", 0, [
		'Conf-001' => 1,
	    ],
	],
	[   "euclid", "/home/dustin/code/backuppc", "20100127172011", 0, [
		'Conf-013' => 1,
		'Conf-013' => 2,
		'Conf-013' => 3,
		'Conf-014' => 1,
		'Conf-014' => 2,
		'Conf-014' => 3,
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
	    holding_file => $holding_filenames{'oldbox_opt_20080414144444_holding'}),
    [
	[   "oldbox", "/opt", "20080414144444", 0, [
		'20080414144444/oldbox._opt',
	    ],
	],
    ],
    "make_plan creates an appropriate plan for an explicit holding-disk recovery");

is_plan(make_plan_sync(
	    holding_file => $holding_filenames{'oldbox_opt_20080414144444_holding'}),
    [
	[   "oldbox", "/opt", "20080414144444", 0, [
		'20080414144444/oldbox._opt',
	    ],
	],
    ],
    "same, without a dumpspec");

is_plan(make_plan_sync(
	    dumpspec => ds("euclid", "/home/dustin/code/backuppc"),
	    filelist => [
		'Conf-013' => [1, 2, 3],
		'Conf-014' => [1, 2, 3],
	    ],
	    changer => $changer),
    [
	[   "euclid", "/home/dustin/code/backuppc", "20100127172011", 0, [
		'Conf-013' => 1,
		'Conf-013' => 2,
		'Conf-013' => 3,
		'Conf-014' => 1,
		'Conf-014' => 2,
		'Conf-014' => 3,
	    ],
	],
    ],
    "plan based on filelist, with a dumpspec");

is_plan(make_plan_sync(
	    filelist => [
		'Conf-013' => [1, 2, 3],
		'Conf-014' => [1, 2, 3],
	    ],
	    changer => $changer),
    [
	[   "euclid", "/home/dustin/code/backuppc", "20100127172011", 0, [
		'Conf-013' => 1,
		'Conf-013' => 2,
		'Conf-013' => 3,
		'Conf-014' => 1,
		'Conf-014' => 2,
		'Conf-014' => 3,
	    ],
	],
    ],
    "plan based on filelist, without a dumpspec");

__DATA__
# a short-datestamp logfile with only a single, single-part file in it
::: log.20080111.0
:tapelist 20080111 Conf-001
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
PART taper Conf-001 1 somebox /lib 20080111 1/1 0 [sec 4.813543 kb 419 kps 87.133307]
DONE taper somebox /lib 20080111 1 0 [sec 4.813543 kb 419 kps 87.133307]
FINISH driver date 20080111 time 2167.581

# a logfile with several dumps in it, one of which comes in many parts, and one of which is
# from a previous run
::: log.20080313133333.0
:tapelist 20080313133333 Conf-003
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
PART taper Conf-003 1 somebox /usr/bin 20080313133333 1/1 1 [sec 0.000370 kb 20 kps 54054.054054]
DONE taper somebox /usr/bin 20080313133333 1 1 [sec 0.000370 kb 20 kps 54054.054054]
# a multi-part dump
SUCCESS dumper somebox /lib 20080313133333 0 [sec 0.189 kb 3156 kps 50253.1 orig-kb 3156]
SUCCESS chunker somebox /lib 20080313133333 0 [sec 5.250 kb 3156 kps 1815.5]
STATS driver estimate somebox /lib 20080313133333 0 [sec 1 nkb 3156 ckb 3156 kps 9500]
PART taper Conf-003 2 somebox /lib 20080313133333 1/10 0 [sec 0.005621 kb 1024 kps 182173.990393]
PART taper Conf-003 3 somebox /lib 20080313133333 2/10 0 [sec 0.006527 kb 1024 kps 156886.777999]
PART taper Conf-003 4 somebox /lib 20080313133333 3/10 0 [sec 0.005854 kb 1024 kps 174923.129484]
PART taper Conf-003 5 somebox /lib 20080313133333 4/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
PART taper Conf-003 6 somebox /lib 20080313133333 5/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
PART taper Conf-003 7 somebox /lib 20080313133333 6/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
PART taper Conf-003 8 somebox /lib 20080313133333 7/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
PART taper Conf-003 9 somebox /lib 20080313133333 8/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
PART taper Conf-003 10 somebox /lib 20080313133333 9/10 0 [sec 0.007344 kb 1024 kps 147993.746743]
PART taper Conf-003 11 somebox /lib 20080313133333 10/10 0 [sec 0.001919 kb 284 kps 147993.746743]
DONE taper somebox /lib 20080313133333 10 0 [sec 0.051436 kb 3156 kps 184695.543977]
SUCCESS dumper otherbox /lib 20080313133333 0 [sec 0.001 kb 190 kps 10352.0 orig-kb 20]
SUCCESS chunker otherbox /lib 20080313133333 0 [sec 1.023 kb 190 kps 50.8]
STATS driver estimate otherbox /lib 20080313133333 0 [sec 0 nkb 190 ckb 190 kps 1024]
# this dump is from a previous run, with an older dump_timestamp
PART taper Conf-003 12 otherbox /usr/bin 20080311131133 1/1 0 [sec 0.002733 kb 240 kps 136425.648022]
DONE taper otherbox /usr/bin 20080311131133 1 0 [sec 0.002733 kb 240 kps 136425.648022]
PART taper Conf-003 13 otherbox /lib 20080313133333 1/1 0 [sec 0.001733 kb 190 kps 136425.648022]
DONE taper otherbox /lib 20080313133333 1 0 [sec 0.001733 kb 190 kps 136425.648022]
FINISH driver date 20080313133333 time 24.777

# A logfile with some partial parts (PARTPARTIAL) in it
::: log.20080414144444.0
:tapelist 20080414144444 Conf-004
:tapelist 20080414144444 Conf-005
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
PARTPARTIAL taper Conf-004 1 otherbox /lib 20080414144444 1/1 1 [sec 0.000707 kb 32 kps 45261.669024] ""
INFO taper Will request retry of failed split part.
INFO taper Will write new label `Conf-005' to new (previously non-amanda) tape
START taper datestamp 20080414144444 label Conf-005 tape 2
PARTPARTIAL taper Conf-005 1 otherbox /lib 20080414144444 1/1 1 [sec 0.000540 kb 32 kps 59259.259259] ""
INFO taper Will request retry of failed split part.
WARNING driver Out of tapes; going into degraded mode.
PARTIAL taper otherbox /lib 20080414144444 1 1 [sec 0.000540 kb 32 kps 59259.259259] "full-up"
# a completely failed dump
FAIL taper otherbox /boot 20080414144444 0 error "no-space"
FINISH driver date 20080414144444 time 6.959

# a spanned dump (yep, a real dump)
::: log.20100127172011.0
:tapelist 20100127172011 Conf-013
:tapelist 20100127172011 Conf-014
INFO amdump amdump pid 30186
INFO planner planner pid 30207
START planner date 20100127172011
DISK planner euclid /home/dustin/code/backuppc
INFO planner Adding new disk euclid:/home/dustin/code/backuppc.
INFO driver driver pid 30208
START driver date 20100127172011
STATS driver hostname euclid
INFO dumper dumper pid 30220
STATS driver startup time 0.097
INFO dumper dumper pid 30222
INFO dumper dumper pid 30221
INFO dumper dumper pid 30213
INFO taper taper pid 30210
FINISH planner date 20100127172011 time 1.224
INFO planner pid-done 30207
INFO taper Will write new label `Conf-013' to new tape
INFO chunker chunker pid 30255
INFO dumper gzip pid 30259
SUCCESS dumper euclid /home/dustin/code/backuppc 20100127172011 0 [sec 0.933 kb 2770 kps 2968.5 orig-kb 2770]
SUCCESS chunker euclid /home/dustin/code/backuppc 20100127172011 0 [sec 0.943 kb 2770 kps 2970.1]
INFO chunker pid-done 30255
STATS driver estimate euclid /home/dustin/code/backuppc 20100127172011 0 [sec 2 nkb 2802 ckb 2816 kps 1024]
INFO dumper pid-done 30259
START taper datestamp 20100127172011 label Conf-013 tape 1
PART taper Conf-013 1 euclid /home/dustin/code/backuppc 20100127172011 1/-1 0 [sec 0.000763 kb 512 kps 670972.950092]
PART taper Conf-013 2 euclid /home/dustin/code/backuppc 20100127172011 2/-1 0 [sec 0.000770 kb 512 kps 664770.167400]
PART taper Conf-013 3 euclid /home/dustin/code/backuppc 20100127172011 3/-1 0 [sec 0.000877 kb 512 kps 583952.261903]
PARTPARTIAL taper Conf-013 4 euclid /home/dustin/code/backuppc 20100127172011 4/-1 0 [sec 0.000689 kb 352 kps 510888.307044] "No space left on device"
INFO taper Will request retry of failed split part.
INFO taper tape Conf-013 kb 1536 fm 4 [OK]
INFO taper Will write new label `Conf-014' to new tape
START taper datestamp 20100127172011 label Conf-014 tape 2
PART taper Conf-014 1 euclid /home/dustin/code/backuppc 20100127172011 4/-1 0 [sec 0.001346 kb 512 kps 380377.004130]
PART taper Conf-014 2 euclid /home/dustin/code/backuppc 20100127172011 5/-1 0 [sec 0.001338 kb 512 kps 382524.888399]
PART taper Conf-014 3 euclid /home/dustin/code/backuppc 20100127172011 6/-1 0 [sec 0.000572 kb 210 kps 367336.443449]
DONE taper euclid /home/dustin/code/backuppc 20100127172011 6 0 [sec 0.005666 kb 2770 kps 488860.596548]
INFO dumper pid-done 30213
INFO dumper pid-done 30220
INFO dumper pid-done 30222
INFO taper tape Conf-014 kb 1234 fm 3 [OK]
INFO dumper pid-done 30221
INFO taper pid-done 30210
FINISH driver date 20100127172011 time 4.197
INFO driver pid-done 30208

# holding-disk
:holding otherbox_lib_20080414144444_holding 20080414144444 otherbox /lib 1 OK 256
:holding oldbox_opt_20080414144444_holding 20080414144444 oldbox /opt 0 OK 1298
