# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 40;

use lib "@amperldir@";
use strict;
use warnings;
use Amanda::Constants;
use Amanda::Paths;
use File::Path;
use Installcheck;
use Installcheck::Application;
use Installcheck::Config;
use Installcheck::Run;
use IPC::Open3;

my $postgres_prefix = $ENV{'INSTALLCHECK_POSTGRES_PREFIX'};

my $SIGQUIT = 3;
my $DB_NAME = "installcheck";

sub skip_all {
    my $reason = shift @_;
    SKIP: {
        skip($reason, Test::More->builder->expected_tests);
    }
    exit 0;
}

skip_all("GNU tar is not available")
    unless ($Amanda::Constants::GNUTAR and -x $Amanda::Constants::GNUTAR);
skip_all("Set INSTALLCHECK_POSTGRES_PREFIX to run tests") unless $postgres_prefix;

my $root_dir = "$Installcheck::TMP/ampgsql";
my $data_dir = "$root_dir/data";
my $config_file = "$data_dir/postgresql.conf";
my $restore_config_file = "$data_dir/restore.conf";
my $socket_dir = "$root_dir/sockets";
my $archive_dir = "$root_dir/archive";
my $tmp_dir = "$root_dir/tmp";
my $log_dir = "$AMANDA_DBGDIR/installcheck/ampgsql";
my $state_dir = "$root_dir/state";

sub run_and_log {
    my ($in_str, $prog, @args) = @_;

    my @parts = split(/\//, $prog);
    my $log_name = "$log_dir/" . pop(@parts) . "-" . time();
    local (*IN, *OUT, *ERR);
    open(OUT, ">", "$log_name-stdout");
    open(ERR, ">", "$log_name-stderr");

    my $pid = open3(\*IN, ">&OUT", ">&ERR", $prog, @args);
    print IN $in_str;
    close(IN);
    my $status = waitpid($pid, 0) >> 8;

    close(OUT);
    close(ERR);

    $status;
}

sub try_eval {
    my ($desc, $code, @args) = @_;
    my $err_str;
    $err_str = "$@" unless eval {$code->(@args); 1;};
    ok(!$err_str, $desc) or diag($err_str);
}

try_eval("emptied root_dir", \&rmtree, $root_dir);
try_eval("created archive_dir", \&mkpath, $archive_dir);
try_eval("created data_dir", \&mkpath, $data_dir);
try_eval("created socket_dir", \&mkpath, $socket_dir);
try_eval("created log_dir", \&mkpath, $log_dir);
try_eval("created state_dir", \&mkpath, $state_dir);

my $conf = Installcheck::Config->new();
$conf->add_client_param('property', "\"PG-DATADIR\" \"$data_dir\"");
$conf->add_client_param('property', "\"PG-ARCHIVEDIR\" \"$archive_dir\"");
$conf->add_client_param('property', "\"PG-HOST\" \"$socket_dir\"");
$conf->add_client_param('property', "\"PSQL-PATH\" \"$postgres_prefix/bin/psql\"");
$conf->write();

sub do_postmaster {
    my $code = shift @_;

    my $log_file = "$log_dir/postmaster-" . time();
    run_and_log("", "$postgres_prefix/bin/pg_ctl", "start", "-w",
        "-l", $log_file, "-D", $data_dir);

    $code->() if $code;

    run_and_log("", "$postgres_prefix/bin/pg_ctl", "stop", "-D", $data_dir);
}

run_and_log("", "$postgres_prefix/bin/initdb", "-D", "$data_dir");
pass("ran initdb");

my $archive_mode = '';
if (Installcheck::Run::run_get("$postgres_prefix/bin/psql", "--version") =~ / 8\.3/) {
    $archive_mode = 'archive_mode = on';
}

my $h = new IO::File($config_file, ">");
ok($h, "opened config file");
print $h <<EOF;
listen_addresses = ''
unix_socket_directory = '$socket_dir'
archive_command = 'test ! -f $archive_dir/%f && cp %p $archive_dir/%f'
$archive_mode
log_destination = 'stderr'
EOF
$h->close();
pass("wrote config file");

my $app = new Installcheck::Application("ampgsql");
$app->add_property('statedir', $state_dir);
$app->add_property('tmpdir', $tmp_dir);

my $backup;
sub setup_db_and_backup {
    run_and_log("", "$postgres_prefix/bin/createdb", "-h", $socket_dir, $DB_NAME);
    pass("created db");

    run_and_log(<<EOF, "$postgres_prefix/bin/psql", "-h", $socket_dir, "-d", $DB_NAME);
CREATE TABLE foo (bar INTEGER, baz INTEGER);
INSERT INTO foo (bar, baz) VALUES (1, 2);
EOF
    pass("created test data (table and a row)");

    $backup = $app->backup('device' => $data_dir, 'level' => 0, 'config' => 'TESTCONF');
    is($backup->{'exit_status'}, 0, "backup error status ok");
    ok(!@{$backup->{'errors'}}, "no errors during backup")
        or diag(@{$backup->{'errors'}});
    ok(grep(/^\/PostgreSQL-Database-0$/, @{$backup->{'index'}}), "contains an index entry")
        or diag(@{$backup->{'index'}});
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

try_eval("emptied data_dir", \&rmtree, $data_dir);
try_eval("emptied archive_dir", \&rmtree, $archive_dir);
try_eval("recreated data_dir", \&mkpath, $data_dir);

my $orig_cur_dir = POSIX::getcwd();
ok($orig_cur_dir, "got current directory");

ok(chdir($root_dir), "changed working directory (for restore)");

my $restore = $app->restore('objects' => ['./'], 'data' => $backup->{'data'});
is($restore->{'exit_status'}, 0, "error status ok");

$h = new IO::File($restore_config_file, ">");
ok($h, "opened restore config file");
print $h <<EOF;
restore_command = 'cp $archive_dir/%f %p'
EOF
$h->close();
pass("wrote restore config file");

ok(chdir($orig_cur_dir), "changed working directory (back to original)");

is(system('chmod', '-R', 'go-rwx', $archive_dir, $data_dir) >> 8, 0, 'chmod restored files');

do_postmaster();
pass("ran postmaster to recover");

sub get_data {
    like(Installcheck::Run::run_get("$postgres_prefix/bin/psql", "-h", $socket_dir, "-d", $DB_NAME, "-c", "SELECT * FROM foo;"),
        qr/1\s+\|\s+2/,
        "get data from recovered database");
}

do_postmaster(\&get_data);
pass("ran postmaster to get data");

try_eval("emptied root_dir", \&rmtree, $root_dir);
