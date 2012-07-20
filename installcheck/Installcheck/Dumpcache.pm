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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck::Dumpcache;

=head1 NAME

Installcheck::Dumpcache - manage a cached amdump run, to avoid the need to run
amdump repeatedly just to test other applications

=head1 SYNOPSIS

  use Installcheck::Dumpcache;

  Installcheck::Dumpcache::load("basic");

=head1 DESCRIPTION

This single function will make an amdump run, or at least make it look like one
has been done by restoring a cached tarball of a previous run.  This saves the
time of repeated slow 'amdump' invocations to test functionality that requires
an existing dump.

The dump cache captures everything: vtapes, curinfo, indexes, trace logs,
tapelist, timestamp, and configuration.  When a flavor is loaded, the timestamp
for all runs are available in the package variable C<@timestamps>.

The function C<create_all> is called by the special installcheck '=setupcache',
and fills the cache.

=head1 FLAVORS

=head2 basic

Basic runs a single amdump with the default L<Installcheck::Run> configuration,
to which has been added:

  $testconf->add_dle("localhost $diskname installcheck-test");

and a few basic configuration parameters listed below.

=head2 notimestamps

Like 'basic', but with "usetimestamps" set to "no".

=head2 ndmp

Like 'basic', but with an NDMP device.  You will need to use
L<Installcheck::Mock>'s C<edit_config> to use this.

=head2 parts

A single multi-part dump with nine parts (using a fallback_splitsize of 128k).

=head2 compress

A single dump of C<$diskname> with server-side compression enabled.  This 

=head2 multi

This flavor runs three dumps of two DLEs (C<$diskname> and C<$diskname/dir>).
The first two dumps run normally, while the third is in degraded mode (the
taper is disabled).

=cut

use Installcheck;
use Installcheck::Run qw(run $diskname $taperoot amdump_diag);
use Installcheck::Mock;
use Test::More;
use Amanda::Paths;
use Amanda::Constants;
use File::Path qw( mkpath rmtree );
use IPC::Open3;
use Cwd qw(abs_path getcwd);
use Carp;

our @timestamps;

my $tarballdir = "$Installcheck::TMP/dumpcache";
my %flavors;

sub basic_settings {
    my ($testconf) = @_;

    $testconf->add_param('autolabel', '"TESTCONF%%" EMPTY VOLUME_ERROR');
    $testconf->add_param('amrecover_changer', '"changer"');
}

sub use_new_chg_disk {
    my ($testconf) = @_;

    $testconf->remove_param('tapedev');
    $testconf->remove_param('tpchanger');
    $testconf->add_param('tpchanger', "\"chg-disk:$taperoot\"");
}

$flavors{'basic'} = sub {
    my $testconf = Installcheck::Run::setup();
    basic_settings($testconf);
    use_new_chg_disk($testconf);
    $testconf->add_dle("localhost $diskname installcheck-test");
    $testconf->write();

    ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'basic'"),
	or amdump_diag("Amdump run failed for 'basic'");
};

$flavors{'notimestamps'} = sub {
    my $testconf = Installcheck::Run::setup();
    basic_settings($testconf);
    use_new_chg_disk($testconf);
    $testconf->add_dle("localhost $diskname installcheck-test");
    $testconf->add_param('usetimestamps', 'no');
    $testconf->write();

    ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'notimestamps'"),
	or amdump_diag("Amdump run failed for 'notimestamps'");
};

$flavors{'multi'} = sub {
    my $stuff = "abcdefghijkl" x 512;
    my $append_stuff = sub {
	open(my $fh, ">>", "$diskname/extrastuff");
	print $fh $stuff, $stuff;
	close($fh);

	open($fh, ">>", "$diskname/dir/extrastuff");
	print $fh $stuff, $stuff;
	close($fh);
    };

    my $testconf = Installcheck::Run::setup();
    basic_settings($testconf);
    use_new_chg_disk($testconf);
    $testconf->add_dle("localhost $diskname installcheck-test");
    $testconf->add_dle("localhost $diskname/dir installcheck-test");
    # do the smallest dumps first -- $diskname/dir in particular should
    # be smaller than $diskname
    $testconf->add_param("dumporder", '"ssssssssss"');
    $testconf->write();

    ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'multi' step 1"),
	or amdump_diag("Amdump run failed for 'multi' step 1");

    $append_stuff->();

    # XXX note that Amanda will not bump $diskname to level 1 here; other installchecks
    # may depend on this behavior
    ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'multi' step 2"),
	or amdump_diag("Amdump run failed for 'multi' step 2");

    $append_stuff->();

    ok(Installcheck::Run::run('amdump', 'TESTCONF', '-otpchanger=', '-otapedev='),
	"amdump for 'multi' step 3 (degraded mode)"),
	or amdump_diag("Amdump run failed for 'multi' step 3 (degraded mode)");

    # we made a mess of $diskname, so invalidate it
    rmtree("$diskname");
};

$flavors{'parts'} = sub {
    my $testconf = Installcheck::Run::setup();
    basic_settings($testconf);
    use_new_chg_disk($testconf);
    $testconf->add_tapetype("TEST-TAPE", [
	"length", "50M",
	"part_size", "128k",
	"part_cache_type", "memory",
    ]);
    $testconf->add_dle("localhost $diskname installcheck-test");
    $testconf->write();

    ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'parts'"),
	or amdump_diag("Amdump run failed for 'part'");
};

$flavors{'compress'} = sub {
    my $testconf = Installcheck::Run::setup();
    basic_settings($testconf);
    use_new_chg_disk($testconf);
    $testconf->add_dumptype("installcheck-test-comp", [
	"installcheck-test", "",
	"compress", "server fast",
    ]);
    $testconf->add_dle("localhost $diskname installcheck-test-comp");
    $testconf->write();

    # add some compressible data to the dump
    open(my $fh, ">>", "$diskname/compressible");
    my $stuff = <<EOLOREM;
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum.
EOLOREM
    for my $i (1 .. 100) {
	print $fh $stuff, $stuff;
    }
    close($fh);

    ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'compress'"),
	or amdump_diag("Amdump run failed for 'part'");

    # we made a mess of $diskname, so invalidate it
    rmtree("$diskname");
};

if (Amanda::Util::built_with_component("server")
    and Amanda::Util::built_with_component("ndmp")) {

    $flavors{'ndmp'} = sub {
	my $testconf = Installcheck::Run::setup();
	basic_settings($testconf);
	use_new_chg_disk($testconf);
	$testconf->add_dle("localhost $diskname installcheck-test");
	my $ndmp = Installcheck::Mock::NdmpServer->new();
	$ndmp->config($testconf);
	$testconf->write();

	ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump for 'ndmp'"),
	    or amdump_diag("Amdump run failed for 'ndmp'");
    };
}

sub generate_and_store {
    my ($flavor) = @_;

    if (exists $flavors{$flavor}) {
	$flavors{$flavor}->();
    } else {
	die("Invalid flavor '$flavor'");
    }

    # now package that up as a tarball
    mkpath($tarballdir);
    my $tmp_tarball = "$tarballdir/$flavor-tmp.tgz";
    my $conf_tarball = "$tarballdir/$flavor-conf.tgz";

    if (system("$Amanda::Constants::GNUTAR",
		"-C", "$Installcheck::TMP",
		"-zcf", "$tmp_tarball",
		"vtapes",
		"holding")) {
	diag("Error caching dump results (ignored): $?");
	return 0;
    }

    if (system("$Amanda::Constants::GNUTAR",
		"-C", "$CONFIG_DIR",
		"-zcf", "$conf_tarball",
		"TESTCONF")) {
	diag("Error caching dump results (ignored): $?");
	return 0;
    }

    return 1;
}

sub load {
    my ($flavor) = @_;

    croak("Invalid flavor '$flavor'") unless (exists $flavors{$flavor});

    # clean up any remnants first
    Installcheck::Run::cleanup();

    my $tmp_tarball = "$tarballdir/$flavor-tmp.tgz";
    my $conf_tarball = "$tarballdir/$flavor-conf.tgz";

    if (! -f $tmp_tarball || ! -f $conf_tarball) {
	die "Cached dump '$flavor' is not available.  Re-run the '=setupcache' check";
    }
    if (system("$Amanda::Constants::GNUTAR",
		"-zxf", "$tmp_tarball",
		"-C", "$Installcheck::TMP")) {
	die("Error untarring dump results: $?");
    }

    if (system("$Amanda::Constants::GNUTAR",
		"-zxf", "$conf_tarball",
		"-C", "$CONFIG_DIR")) {
	die("Error untarring dump results: $?");
    }

    # calculate the timestamps for this run
    my @logfiles = glob "$CONFIG_DIR/TESTCONF/log/log.*";
    my %timestamps = map { my ($ts) = ($_ =~ /log\.(\d+)\./); $ts?($ts, 1):() } @logfiles;
    @timestamps = keys %timestamps; # set package variable
}

sub create_all {
    for my $flavor (keys %flavors) {
	ok(generate_and_store($flavor), "cached flavor '$flavor'") or return;
    }
}

Installcheck::Run::cleanup();

1;
