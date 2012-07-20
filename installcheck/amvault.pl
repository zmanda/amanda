# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
use strict;
use warnings;

use lib "@amperldir@";
use File::Path;
use Data::Dumper;
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Mock;
use Installcheck::Run qw(run run_err run_get $diskname);
use Amanda::DB::Catalog;
use Amanda::Paths;
use Amanda::Config qw( :init );
use Amanda::Changer;
use Amanda::Debug;

Amanda::Debug::dbopen("installcheck");

my $vtape_root = "$Installcheck::TMP/tertiary";
sub setup_chg_disk {
    rmtree $vtape_root if -d $vtape_root;
    mkpath "$vtape_root/slot1";
    return "chg-disk:$vtape_root";
}

# set up a basic dump
Installcheck::Dumpcache::load("basic");

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    die "config errors";
}

# and then set up a new vtape to vault onto
my $tertiary_chg = setup_chg_disk();

# try a few failures first
like(run_err("$sbindir/amvault",
		'--autolabel=any',
		'--label-template', "TESTCONF%%",
		'--src-timestamp', 'latest',
		'--dst-changer', $tertiary_chg,
		'TESTCONF', 'someotherhost'),
    qr/No dumps to vault/,
    "amvault with a non-matching dumpspec dumps nothing")
    or diag($Installcheck::Run::stderr);

like(run_err("$sbindir/amvault",
		'--autolabel=any',
		'--label-template', "TESTCONF%%",
		'--src-timestamp', 'latest',
		'--fulls-only',
		'--dst-changer', $tertiary_chg,
		'TESTCONF', '*', '*', '*', '1-3'),
    qr/No dumps to vault/,
    "amvault with --fulls-only but specifying non-full dumpspecs dumps nothing")
    or diag($Installcheck::Run::stderr);

like(run_err("$sbindir/amvault",
		'--autolabel=any',
		'--label-template', "TESTCONF%%",
		'--dst-changer', $tertiary_chg,
		'TESTCONF'),
    qr/specify something to select/,
    "amvault without any limiting factors is an error"),
    or diag($Installcheck::Run::stderr);

# now a successful vaulting
ok(run("$sbindir/amvault",
		'--autolabel=any',
		'--label-template', "TESTCONF%%",
		'--src-timestamp', 'latest',
		'--dst-changer', $tertiary_chg,
		'TESTCONF'),
    "amvault runs!")
    or diag($Installcheck::Run::stderr);
my @tert_files = glob("$vtape_root/slot1/0*");
ok(@tert_files > 0,
    "..and files appear on the tertiary volume!");

my @dumps = Amanda::DB::Catalog::sort_dumps([ 'write_timestamp' ],
	Amanda::DB::Catalog::get_dumps());

is(scalar @dumps, 2,
    "now there are two dumps in the catalog");

sub summarize {
    my ($dump) = @_;
    return {
	map { $_ => $dump->{$_} }
	    qw(diskname hostname level dump_timestamp kb orig_kb)
    };
}
is_deeply(summarize($dumps[1]), summarize($dumps[0]),
    "and they match in all the right ways")
    or diag(Dumper(@dumps));

# clean up the tertiary vtapes before moving on
rmtree $vtape_root;
Installcheck::Run::cleanup();

# try the multi dump, to get a better idea of the filtering possibilities
Installcheck::Dumpcache::load("multi");
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    die "config errors";
}

sub get_dry_run {
    my $stdout = run_get(@_);
    if (!$stdout) {
	diag($Installcheck::Run::stderr);
	return 'run-failed';
    }

    my @rv;
    for my $line (split /\n/, $stdout) {
	next if ($line =~ /^Total Size:/);
	my ($tape, $file, $host, $disk, $datestamp, $level) =
	    ($line =~ /^(\S+) (\d*) (\S+) (.+) (\d+) (\d+)$/);
	$tape = 'holding' if $file eq '';
	push @rv, [$tape, $file, $host, $disk,   $level]; # note: no datestamp
    }
    return @rv;
}

is_deeply([ get_dry_run("$sbindir/amvault",
		'--dry-run',
		'--autolabel=any',
		'--label-template', "TESTCONF%%",
		'--fulls-only',
		'--dst-changer', $tertiary_chg,
		'TESTCONF') ], [
    [ "TESTCONF01", "1", "localhost", "$diskname/dir", "0" ],
    [ "TESTCONF01", "2", "localhost", "$diskname",     "0" ],
    [ "TESTCONF02", "2", "localhost", "$diskname",     "0" ]
    ], "amvault with --fulls-only only dumps fulls");

is_deeply([ get_dry_run("$sbindir/amvault",
		'--dry-run',
		'--autolabel=any',
		'--label-template', "TESTCONF%%",
		'--dst-changer', $tertiary_chg,
		'TESTCONF', "localhost", "$diskname/dir") ], [
    [ "holding", "",     "localhost", "$diskname/dir",     "1" ],
    [ "TESTCONF01", "1", "localhost", "$diskname/dir",     "0" ],
    [ "TESTCONF02", "1", "localhost", "$diskname/dir",     "1" ]
    ], "amvault with a disk expression dumps only that disk");

# Test NDMP-to-NDMP vaulting.  This will test all manner of goodness:
#  - specifying a named changer on the amvault command line
#  - exporting
#  - directtcp vaulting (well, not really, since we don't support connecting yet)
SKIP: {
    skip "not built with ndmp and server", 2 unless
	Amanda::Util::built_with_component("ndmp") and Amanda::Util::built_with_component("server");

    Installcheck::Dumpcache::load("ndmp");

    my $ndmp = Installcheck::Mock::NdmpServer->new(no_reset => 1);
    $ndmp->edit_config();

    # append a tertiary changer to the config file - it's just too hard to
    # specify a full ndmp changer on the command line

    my $ndmp_port = $ndmp->{'port'};
    my $chg_dir = "$Installcheck::TMP/vtapes/ndmjob-tert";
    my $chg_spec = "chg-ndmp:127.0.0.1:$ndmp_port\@$chg_dir";
    my $drive_root = "ndmp:127.0.0.1:$ndmp_port\@$chg_dir";

    -d $chg_dir && rmtree($chg_dir);
    mkpath($chg_dir);

    my $amanda_conf_filename = "$CONFIG_DIR/TESTCONF/amanda.conf";
    open(my $fh, ">>", $amanda_conf_filename);
    print $fh <<EOF;
define changer "tertiary" {
    tpchanger "$chg_spec"
    property        "tape-device" "0=$drive_root/drive0"
    property append "tape-device" "1=$drive_root/drive1"
    changerfile "$chg_dir-changerfile"
}
EOF

    $tertiary_chg = "tertiary";
    ok(run("$sbindir/amvault",
		    '--export',
		    '--autolabel=any',
		    '--label-template', "TESTCONF%%",
		    '--src-timestamp', 'latest',
		    '--dst-changer', $tertiary_chg,
		    'TESTCONF'),
	"amvault runs with an NDMP device as secondary and tertiary, with --export")
	or diag($Installcheck::Run::stderr);

    config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
    ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_WARNINGS) {
	config_print_errors();
	die "config errors";
    }

    # query the tertiary changer to see where that dump ended up
    my $chg = Amanda::Changer->new($tertiary_chg);
    my $inventory;
    my $inventory_cb = sub {
	my ($err, $inv) = @_;
	die "$err" if $err;

	$inventory = $inv;
	Amanda::MainLoop::quit();
    };
    Amanda::MainLoop::call_later(sub { $chg->inventory(inventory_cb => $inventory_cb); });
    Amanda::MainLoop::run();
    $chg->quit();

    # find TESTCONF02 in the inventory, and check that it is in an i/e slot
    my $notfound = "tertiary volume not found";
    for my $i (@$inventory) {
	if ($i->{'label'} && $i->{'label'} eq 'TESTCONF02') {
	    if ($i->{'import_export'}) {
		$notfound = undef;
	    } else {
		$notfound = "tertiary volume not properly exported";
	    }
	    #last;
	}
    }

    ok(!$notfound, "tertiary volume exists and was properly exported");
    if ($notfound) {
	diag($notfound);
	diag("amvault stderr:");
	diag($Installcheck::Run::stderr);
    }

}

# clean up
Installcheck::Run::cleanup();
