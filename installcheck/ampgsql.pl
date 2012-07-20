# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 74;

use lib "@amperldir@";
use strict;
use warnings;
use Amanda::Constants;
use Amanda::Paths;
use Amanda::Debug qw( debug );
use Amanda::Util;
use File::Path;
use Installcheck;
use Installcheck::Application;
use Installcheck::Config;
use Installcheck::Run;
use IPC::Open3;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();
my $debug = !exists $ENV{'HARNESS_ACTIVE'};

sub skip_all {
    my $reason = shift @_;
    SKIP: {
        skip($reason, Test::More->builder->expected_tests);
    }
    exit 0;
}

skip_all("GNU tar is not available")
    unless ($Amanda::Constants::GNUTAR and -x $Amanda::Constants::GNUTAR);

my $postgres_prefix = $ENV{'INSTALLCHECK_POSTGRES_PREFIX'};
skip_all("Set INSTALLCHECK_POSTGRES_PREFIX to run tests") unless $postgres_prefix;

sub get_pg_version {
    my $verout = Installcheck::Run::run_get("$postgres_prefix/bin/psql", "-X", "--version");
    my @lines = split(/\n/, $verout);
    my ($maj, $min, $pat) = ($lines[0] =~ / ([0-9]+)\.([0-9]+)\.([0-9]+)$/);
    return $maj * 10000 + $min * 100 + $pat;
}
my $pg_version = get_pg_version();

my $SIGINT = 2;
my $DB_NAME = "installcheck";
my $root_dir = "$Installcheck::TMP/ampgsql";
my $data_dir = "$root_dir/data";
my $config_file = "$data_dir/postgresql.conf";
my $recovery_conf_file = "$data_dir/recovery.conf";
my $recovery_done_file = "$data_dir/recovery.done";
my $socket_dir = "$root_dir/sockets";
my $archive_dir = "$root_dir/archive";
my $tmp_dir = "$root_dir/tmp";
my $log_dir = "$Installcheck::TMP/ampgsql";
my $state_dir = "$root_dir/state";

sub dbg {
    my ($msg) = @_;
    if ($debug) {
	diag($msg);
    } else {
	debug($msg);
    }
}

# run a command with output sent to the debug log
sub run_and_log {
    my ($in_str, $prog, @args) = @_;
    local *IN;

    debug("running $prog " . join(" ", @args));
    debug(".. with input '$in_str'") if $in_str;

    my $dbfd = Amanda::Debug::dbfd();
    my $pid = open3(\*IN, ">&$dbfd", ">&$dbfd", $prog, @args);
    print IN $in_str;
    close(IN);
    waitpid($pid, 0);
    my $status = $? >> 8;
    debug("..exit status $status");

    return $status;
}

# run a sub and report on the result; mostly used for setup/teardown
sub try_eval {
    my ($desc, $code, @args) = @_;
    my $err_str;
    $err_str = "$@" unless eval {$code->(@args); 1;};
    ok(!$err_str, $desc) or diag($err_str);
}

sub write_config_file {
    my ($filename, $cfg) = @_;

    unlink $filename if -f $filename;
    Amanda::Util::burp($filename, $cfg);
}

# run $code while the postmaster is started, shutting down the postmaster afterward,
# logging the result to our debug file
sub do_postmaster {
    my $code = shift @_;
    my $pidfile = "$data_dir/postmaster.pid";
    local *IN;

    die "postmaster already running"
	if -f $pidfile;

    dbg("starting postmaster..");

    my $dbfd = Amanda::Debug::dbfd();
    my $pid = open3(\*IN, ">&$dbfd", ">&$dbfd",
	    "$postgres_prefix/bin/postmaster", "-D", $data_dir);
    close(IN);

    # busy-wait for the pidfile to be created, for up to 120s
    my $ticks = 0;
    while (!-f $pidfile) {
	die "postmaster did not start"
	    if ($ticks++ > 120 or !kill 0, $pid);
	dbg("waiting for postmaster to write its pid");
	sleep(1);
    }

    # and finish out those 120 seconds waiting for the db to actually
    # be ready to roll, using psql -l just like pg_ctl does
    while ($ticks++ < 120) {
	local *IN;

	my $psqlpid = open3(\*IN, ">&$dbfd", ">&$dbfd",
	    "$postgres_prefix/bin/psql", "-X", "-h", $socket_dir, "-l");
	close IN;
	waitpid($psqlpid, 0);
	last if (($? >> 8) == 0);
	sleep(1);
    }

    if ($ticks == 120) {
	die("postmaster never started");
    }

    # use eval to be careful to shut down postgres
    eval { $code->() if $code };
    my $err = $@;

    # kill the postmaster and wait for it to die
    kill $SIGINT, $pid;
    waitpid($pid, 0);
    my $status = $? >> 8;
    dbg("postmaster stopped");

    die "postmaster pid file still exists"
	if -f $pidfile;

    die $err if $err;
}

# count the WAL files.  Note that this may count some extra stuff, too, but
# since it's only used to see the number of WALs *increase*, that's OK.
sub count_wals {
    my @files = glob("$archive_dir/*");
    if (@files) {
	debug("WAL files in archive_dir: " . join(" ", @files));
    } else {
	debug("No WAL files in archive_dir");
    }
    return scalar @files;
}

sub ls_backup_data {
    my ($level, $backup) = @_;

    my $tmpdir = "$Installcheck::TMP/backup_data";
    -d $tmpdir && rmtree $tmpdir;
    mkpath $tmpdir;

    if ($level > 0) {
	debug("contents of level-$level backup:");
	Amanda::Util::burp("$tmpdir/backup.tar", $backup->{'data'});
	run_and_log("", $Amanda::Constants::GNUTAR, "-tvf", "$tmpdir/backup.tar");
    } else {
	debug("contents of level-0 backup:");
	Amanda::Util::burp("$tmpdir/backup.tar", $backup->{'data'});
	run_and_log("", $Amanda::Constants::GNUTAR, "-C", $tmpdir, "-xvf", "$tmpdir/backup.tar");
	debug(".. archive_dir.tar contains:");
	run_and_log("", $Amanda::Constants::GNUTAR, "-tvf", "$tmpdir/archive_dir.tar");
	debug(".. data_dir.tar looks like:\n" . `ls -l "$tmpdir/data_dir.tar"`);
    }

    rmtree $tmpdir;
}

# set up all of our dirs
try_eval("emptied root_dir", \&rmtree, $root_dir);
try_eval("created archive_dir", \&mkpath, $archive_dir);
try_eval("created data_dir", \&mkpath, $data_dir);
try_eval("created socket_dir", \&mkpath, $socket_dir);
try_eval("created log_dir", \&mkpath, $log_dir);
try_eval("created state_dir", \&mkpath, $state_dir);

# create an amanda config for the application
my $conf = Installcheck::Config->new();
$conf->add_client_param('property', "\"PG-DATADIR\" \"$data_dir\"");
$conf->add_client_param('property', "\"PG-ARCHIVEDIR\" \"$archive_dir\"");
$conf->add_client_param('property', "\"PG-CLEANUPWAL\" \"yes\"");
$conf->add_client_param('property', "\"PG-HOST\" \"$socket_dir\"");
$conf->add_client_param('property', "\"PSQL-PATH\" \"$postgres_prefix/bin/psql\"");
$conf->write();

# set up the database
dbg("creating initial database");
run_and_log("", "$postgres_prefix/bin/initdb", "-D", "$data_dir")
    and die("error running initdb");

# enable archive mode for 8.3 and higher
my $archive_mode = '';
if ($pg_version >= 80300) {
    $archive_mode = 'archive_mode = on';
}

# write the postgres config file
write_config_file $config_file, <<EOF;
listen_addresses = ''
unix_socket_directory = '$socket_dir'
archive_command = 'test ! -f $archive_dir/%f && cp %p $archive_dir/%f'
$archive_mode
log_destination = 'stderr'
# checkpoint every 30 seconds (this is the minimum)
checkpoint_timeout = 30
# and don't warn me about that
checkpoint_warning = 0
# and keep 50 segments
checkpoint_segments = 50
# and bundle commits up to one minute
commit_delay = 60
EOF

my $app = new Installcheck::Application("ampgsql");
$app->add_property('statedir', $state_dir);
$app->add_property('tmpdir', $tmp_dir);

# take three backups: a level 0, level 1, and level 2.  The level 2 has
# no database changes, so it contains no WAL files.
my ($backup, $backup_incr, $backup_incr_empty);
sub setup_db_and_backup {
    my $i;

    run_and_log("", "$postgres_prefix/bin/createdb", "-h", $socket_dir, $DB_NAME);
    pass("created db");

    run_and_log(<<EOF, "$postgres_prefix/bin/psql", "-X", "-h", $socket_dir, "-d", $DB_NAME);
CREATE TABLE foo (bar INTEGER, baz INTEGER, longstr CHAR(10240));
INSERT INTO foo (bar, baz) VALUES (1, 2);
EOF
    pass("created test data (table and a row)");

    $backup = $app->backup('device' => $data_dir, 'level' => 0, 'config' => 'TESTCONF');
    ls_backup_data(0, $backup);
    is($backup->{'exit_status'}, 0, "backup error status ok");
    ok(!@{$backup->{'errors'}}, "..no errors")
        or diag(@{$backup->{'errors'}});
    ok(grep(/^\/PostgreSQL-Database-0$/, @{$backup->{'index'}}), "..contains an index entry")
        or diag(@{$backup->{'index'}});
    ok(length($backup->{'data'}) > 0,
	"..got at least one byte");

    # add a database that should be big enough to fill a WAL, then wait for postgres
    # to archive it.
    my $n_wals = count_wals();
    run_and_log(<<EOF, "$postgres_prefix/bin/psql", "-X", "-h", $socket_dir, "-d", $DB_NAME);
INSERT INTO foo (bar, baz) VALUES (1, 2);
CREATE TABLE wal_test AS SELECT * FROM GENERATE_SERIES(1, 500000);
EOF
    sleep(1);
    for ($i = 0; $i < 10; $i++) {
	last if (count_wals() > $n_wals);
	dbg("still $n_wals WAL files in archive directory; sleeping");
	sleep(1);
    }
    die "postgres did not archive any WALs" if $i == 10;
    $n_wals = count_wals();

    $backup_incr = $app->backup('device' => $data_dir, 'level' => 1, 'config' => 'TESTCONF');
    ls_backup_data(1, $backup_incr);
    is($backup_incr->{'exit_status'}, 0, "incr backup error status ok");
    ok(!@{$backup_incr->{'errors'}}, "..no errors")
        or diag(@{$backup_incr->{'errors'}});
    ok(grep(/^\/PostgreSQL-Database-1$/, @{$backup_incr->{'index'}}), "..contains an index entry")
        or diag(@{$backup_incr->{'index'}});
    ok(length($backup_incr->{'data'}) > 0,
	"..got at least one byte");

    die "more WALs appeared during backup (timing error)"
	if count_wals() > $n_wals;
    ok(count_wals() == $n_wals,
	"ampgsql did not clean up the latest bunch of WAL files (as expected)");

    # (no more transactions here -> no more WAL files)

    $backup_incr_empty = $app->backup('device' => $data_dir, 'level' => 2, 'config' => 'TESTCONF');
    ls_backup_data(2, $backup_incr_empty);
    is($backup_incr_empty->{'exit_status'}, 0, "incr backup with no changes: error status ok");
    ok(!@{$backup_incr_empty->{'errors'}}, "..no errors")
        or diag(@{$backup_incr_empty->{'errors'}});
    ok(grep(/^\/PostgreSQL-Database-2$/, @{$backup_incr_empty->{'index'}}),
	"..contains an index entry")
        or diag(@{$backup_incr_empty->{'index'}});
    ok(length($backup_incr_empty->{'data'}) > 0,
	"..got at least one byte");

    ok(count_wals() == $n_wals,
	"ampgsql still did not clean up the latest bunch of WAL files");
}

do_postmaster(\&setup_db_and_backup);
pass("finished setting up db");

sub try_selfcheck {
    my $sc;

    $sc = $app->selfcheck('device' => $data_dir, 'config' => 'TESTCONF');
    is($sc->{'exit_status'}, 0, "selfcheck error status ok");
    ok(!@{$sc->{'errors'}}, "no errors reported");
    ok(@{$sc->{'oks'}}, "got one or more OK messages");

    $app->set_property('statedir', "$state_dir/foo");
    $sc = $app->selfcheck('device' => $data_dir, 'config' => 'TESTCONF');
    is($sc->{'exit_status'}, 0, "selfcheck error status ok");
    ok(grep(/STATEDIR/, @{$sc->{'errors'}}), "got STATEDIR error");

    my $test_state_dir_par = "$root_dir/parent-to-strip";
    my $test_state_dir = "$test_state_dir_par/state";
    $app->set_property('statedir', $test_state_dir);
    try_eval("created state_dir", \&mkpath, $test_state_dir);
    my @par_stat = stat($test_state_dir_par);
    my $old_perms = $par_stat[2] & 0777;
    ok(chmod(0, $test_state_dir_par), "stripped permissions from parent of statedir");
    $sc = $app->selfcheck('device' => $data_dir, 'config' => 'TESTCONF');
    is($sc->{'exit_status'}, 0, "selfcheck error status ok");
    ok(grep(/STATEDIR/, @{$sc->{'errors'}}), "got STATEDIR error");
    ok(grep(/$test_state_dir_par\/ /, @{$sc->{'errors'}}), "got perms error for parent of statedir");
    # repair
    ok(chmod($old_perms, $test_state_dir_par), "restored permissions on parent of statedir");
    $app->set_property('statedir', $state_dir); 
}

do_postmaster(\&try_selfcheck);

## full restore

sub try_restore {
    my ($expected_foo_count, @backups) = @_;

    dbg("*** try_restore from level $#backups");

    try_eval("emptied data_dir", \&rmtree, $data_dir);
    try_eval("emptied archive_dir", \&rmtree, $archive_dir);
    try_eval("recreated data_dir", \&mkpath, $data_dir);

    my $orig_cur_dir = POSIX::getcwd();
    ok($orig_cur_dir, "got current directory");

    ok(chdir($root_dir), "changed working directory (for restore)");

    for my $level (0 .. $#backups) {
	my $backup = $backups[$level];

	my $restore = $app->restore('objects' => ['./'], level => $level,
			    'data' => $backup->{'data'});
	is($restore->{'exit_status'}, 0, "..level $level restore error status ok");
	if ($level == 0) {
	    ok(-f "$data_dir/PG_VERSION", "..data dir has a PG_VERSION file");
	    ok(-d $archive_dir, "..archive dir exists");

	    my $pidfile = "$data_dir/postmaster.pid";
	    ok(! -f $pidfile, "..pidfile is not restored")
		or unlink $pidfile;
	}
    }

    ok(chdir($orig_cur_dir), "changed working directory (back to original)");
    is(system('chmod', '-R', 'go-rwx', $archive_dir, $data_dir) >> 8, 0, 'chmod restored files');

    write_config_file $recovery_conf_file, <<EOF;
restore_command = 'echo restore_cmd invoked for %f >&2; cp $archive_dir/%f %p'
EOF

    my $get_data = sub {
	like(Installcheck::Run::run_get("$postgres_prefix/bin/psql", "-X",
			"-q", "-A", "-t",
			"-h", $socket_dir, "-d", $DB_NAME,
			"-c", "SELECT count(*) FROM foo;"),
	    qr/^$expected_foo_count/,
	    "..got $expected_foo_count rows from recovered database");
    };

    do_postmaster($get_data);
    unlink($recovery_conf_file);
    unlink($recovery_done_file);
}

# try a level-0, level-1, and level-2 restore
try_restore(1, $backup);
try_restore(2, $backup, $backup_incr);
try_restore(2, $backup, $backup_incr, $backup_incr_empty);

try_eval("emptied root_dir", \&rmtree, $root_dir);
