#! @PERL@
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );
use Amanda::Logfile qw( log_rename get_current_log_timestamp $amanda_log_trace_log );
use Amanda::Debug qw( debug );
use Getopt::Long;

Amanda::Util::setup_application("amlogroll", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

if (@ARGV < 1) {
    die "USAGE: amlogroll <config> <config-overwrites> <ignored-stuff>";
}

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $ARGV[0]);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

# our STDERR may be connected to the amdump log file, so be sure to do unbuffered
# writes to that file
my $old_fh = select(STDERR);
$| = 1;
select($old_fh);

Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# the actual work of the program - very short!
my $timestamp = get_current_log_timestamp();
die "could not get current timestamp" unless $timestamp;
log_rename($timestamp);

Amanda::Util::finish_application();
