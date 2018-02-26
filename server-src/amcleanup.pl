#!@PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Process;
use Amanda::Logfile;
use Amanda::Holding;
use Amanda::Debug qw( debug );
use Amanda::Cleanup;
my $kill_enable=0;
my $process_alive=0;
my $verbose=0;
my $clean_holding=0;
my @notes;

sub usage() {
    print "Usage: amcleanup [-k] [-v] [-p] [-r] [--note 'STRING']* conf\n";
    exit 1;
}

Amanda::Util::setup_application("amcleanup", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'k' => \$kill_enable,
    'p' => \$process_alive,
    'v' => \$verbose,
    'r' => \$clean_holding,
    'help|usage' => \&usage,
    'note=s'     => \@notes,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

my $config_name = shift @ARGV or usage;

if ($kill_enable && $process_alive) {
    die "amcleanup: Can't use -k and -p simultaneously\n";
}

set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $logdir=config_dir_relative(getconf($CNF_LOGDIR));
my $logfile = "$logdir/log";
my $amreport="$sbindir/amreport";
my $amtrmidx="$amlibexecdir/amtrmidx";
my $amcleanupdisk="$sbindir/amcleanupdisk";

if ( ! -e "$CONFIG_DIR/$config_name" ) {
    die "Configuration directory '$CONFIG_DIR/$config_name' doesn't exist\n";
}
if ( ! -d "$CONFIG_DIR/$config_name" ) {
    die "Configuration directory '$CONFIG_DIR/$config_name' is not a directory\n";
}

if (!@notes) {
    @notes = ("Aborted by amcleanup");
}
my $cleanup = Amanda::Cleanup->new(kill          => $kill_enable,
				   process_alive => $process_alive,
				   verbose	 => $verbose,
				   clean_holding => $clean_holding,
				   user_message  => \&user_message,
				   notes         => \@notes);
my $result_messages = $cleanup->cleanup();

foreach my $message (@{$result_messages}) {
    print "amcleanup: ", $message->message(), "\n";
    if ($message->{'code'} == 3400000) {
	print "amcleanup: Use -k option to stop all the process...\n";
	print "Usage: amcleanup [-k] conf\n";
    }
}

Amanda::Util::finish_application();

sub user_message {
    my $message = shift;

    print "amcleanup: ", $message->message(), "\n";
    if ($message->{'code'} == 3400000) {
        print "amcleanup: Use -k option to stop all the process...\n";
        print "Usage: amcleanup [-k] conf\n";
    }
}
