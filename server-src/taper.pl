#! @PERL@
# Copyright (c) 2009-2012 Zmanda Inc.  All Rights Reserved.
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

package main;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Logfile qw( :logtype_t log_add $amanda_log_trace_log );
use Amanda::Debug qw( debug );
use Amanda::Taper::Controller;
use Getopt::Long;

Amanda::Util::setup_application("taper", "server", $CONTEXT_DAEMON);

my $config_overrides = new_config_overrides($#ARGV+1);
my $opt_storage_name;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    'storage-name=s' => \$opt_storage_name,
    'log-filename=s' => sub { Amanda::Logfile::set_logname($_[1]); },
) or usage();

if (@ARGV != 1) {
    die "USAGE: taper <config> <config-overwrites>";
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

# our STDERR is connected to the amdump log file, so be sure to do unbuffered
# writes to that file
my $old_fh = select(STDERR);
$| = 1;
select($old_fh);

log_add($L_INFO, "taper pid $$");
Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
my ($tl, $message) = Amanda::Tapelist->new($tlf);
if (defined $message) {
    die "Could not read the tapelist: $message : " . Data::Dumper::Dumper($message);
}
# transfer control to the Amanda::Taper::Controller class implemented above
my $controller = Amanda::Taper::Controller->new(
					storage_name => $opt_storage_name,
					tapelist => $tl);
$controller->start();
Amanda::MainLoop::run();

log_add($L_INFO, "pid-done $$");
Amanda::Util::finish_application();
