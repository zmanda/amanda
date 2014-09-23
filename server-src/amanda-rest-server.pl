#! @PERL@
# Copyright (c) 2013 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use POSIX qw(WIFEXITED WEXITSTATUS strftime);
use File::Glob qw( :glob );

use Amanda::Config qw( :init :getconf );
use Amanda::Util qw( :constants );
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Debug qw( debug );
use Amanda::Paths;
use Amanda::Amdump;

##
# Main

sub usage {
    my ($msg) = @_;
    print STDERR <<EOF;
Usage: amanda-rest-server [--version] [--help] [ start | stop ]
EOF
    print STDERR "$msg\n" if $msg;
    exit 1;
}

Amanda::Util::setup_application("amanda-rest-server", "server", $CONTEXT_DAEMON);

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;

my $opt_no_taper = 0;
my $opt_from_client = 0;
my $opt_exact_match = 0;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
) or usage();

usage("'start' or 'stop' must be specified.") if (@ARGV < 1);

config_init($CONFIG_INIT_GLOBAL, undef);
Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $dbgdir = $Amanda::Paths::AMANDA_DBGDIR;
my $pid_file = $dbgdir . '/rest-api-pid';
my $pid;
if (-f $pid_file) {
    $pid = Amanda::Util::slurp($pid_file);
    chomp $pid;
}
my $command = shift @ARGV;
if ($command eq 'start') {
    if (defined $pid) {
	my $Amanda_process = Amanda::Process->new(1);
	$Amanda_process->load_ps_table();
	if ($Amanda_process->process_alive($pid, undef)) {
	    print "The Amanda Rest Server is already running\n";
	    exit;
	}
    }

    my $port = getconf($CNF_REST_API_PORT);
    if ($port == 0) {
	debug("The REST-API-PORT must be defined in the global amanda.conf (" . $Amanda::Paths::CONFIG_DIR . "/amanda.conf) and be larger than 1024");
	print "The REST-API-PORT must be defined in the global amanda.conf (" . $Amanda::Paths::CONFIG_DIR . "/amanda.conf) and be larger than 1024\n";
	exit;
    } elsif ($port < 1024) {
	debug("The REST-API-PORT must be larger than 1024");
	print "The REST-API-PORT must be larger than 1024\n";
	exit;
    }
    my @command = ('starman',
		   '@amperldir@' . '/Amanda/Rest/Amanda/bin/app.pl',
		   '--listen', '127.0.0.1:' . $port,
		   '--preload-app',
		   '--max-requests', '1',
		   '--daemonize',
		   '--pid', $pid_file);
    debug("running: " . join(' ', @command));
    system(@command);
    print "Started the Amanda Rest Server\n";
} elsif ($command eq 'stop') {
    if (defined $pid) {
	print "Sending SIGTERM to process $pid\n";
	kill 'SIGTERM', $pid;
    } else {
	print "The Amanda Rest Server is not running\n";
    }
    unlink($pid_file);
} else {
    usage("unknown '$command' command.");
}


