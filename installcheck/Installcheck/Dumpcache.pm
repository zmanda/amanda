# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
tapelist, and configuration.

The function C<create_all> is called by the special installcheck _setupcache,
and fills the cache.

=head1 FLAVORS

=head2 basic

Basic runs a single amdump with the default L<Installcheck::Run> configuration,
to which has been added:

  $testconf->add_param('label_new_tapes', '"TESTCONF%%"');
  $testconf->add_dle("localhost $diskname installcheck-test");

=head2 notimestamps

Like 'basic', but with "usetimestamps" set to "no".

=cut

use Installcheck::Run qw(run $diskname);
use Test::More;
use Amanda::Paths;
use Amanda::Constants;
use File::Path qw( mkpath rmtree );
use IPC::Open3;
use Cwd qw(abs_path getcwd);
use Carp;

my $tarballdir = "$AMANDA_TMPDIR/installcheck-dumpcache";
my %flavors;

sub amdump_diag {
    my ($flavor) = @_;

    # try running amreport
    my $report = "failure-report.txt";
    unlink($report);
    my @logfiles = <$CONFIG_DIR/TESTCONF/log/log.*>;
    run('amreport', 'TESTCONF', '-f', $report, '-l', $logfiles[-1]);
    if (-f $report) {
	open(my $fh, "<", $report) or return;
	for my $line (<$fh>) {
	    diag($line);
	}
	unlink($report);
	goto bail;
    }

    # maybe there was a config error
    config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_WARNINGS) {
	foreach (@cfgerr_errors) {
	    diag($_);
	}
	goto bail;
    }

    # huh.
    diag("no amreport available, and no config errors");

bail:
    BAIL_OUT("amdump run for Dumpcache flavor '$flavor' failed");
}

$flavors{'basic'} = sub {
    my $testconf = Installcheck::Run::setup();
    $testconf->add_param('label_new_tapes', '"TESTCONF%%"');
    $testconf->add_dle("localhost $diskname installcheck-test");
    return $testconf;
};

$flavors{'notimestamps'} = sub {
    my $testconf = $flavors{'basic'}->();
    $testconf->add_param('usetimestamps', 'no');
    return $testconf;
};

sub generate_and_store {
    my ($flavor) = @_;

    if (exists $flavors{$flavor}) {
	my $testconf = $flavors{$flavor}->();
	$testconf->write();
    } else {
	die("Invalid flavor '$flavor'");
    }

    Installcheck::Run::run('amdump', 'TESTCONF')
	or amdump_diag($flavor);

    # now package that up as a tarball
    mkpath($tarballdir);
    my $tmp_tarball = "$tarballdir/$flavor-tmp.tgz";
    my $conf_tarball = "$tarballdir/$flavor-conf.tgz";

    if (system("$Amanda::Constants::GNUTAR",
		"-C", "$AMANDA_TMPDIR",
		"-zcf", "$tmp_tarball",
		"installcheck-vtapes",
		"installcheck-holding")) {
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
	generate_and_store($flavor);
    } else {
	if (system("$Amanda::Constants::GNUTAR",
		    "-zxf", "$tmp_tarball",
		    "-C", "$AMANDA_TMPDIR")) {
	    die("Error untarring dump results: $?");
	}

	if (system("$Amanda::Constants::GNUTAR",
		    "-zxf", "$conf_tarball",
		    "-C", "$CONFIG_DIR")) {
	    die("Error untarring dump results: $?");
	}
    }
}

sub create_all {
    for my $flavor (keys %flavors) {
	generate_and_store($flavor) or return;
    }
}

1;
