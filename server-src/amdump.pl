#! @PERL@
# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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
Usage: amdump <conf> [--no-taper] [--from-client] [--exact-match] [-o configoption]* [host/disk]*
EOF
    print STDERR "$msg\n" if $msg;
    exit 1;
}

Amanda::Util::setup_application("amdump", "server", $CONTEXT_DAEMON, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;

my $opt_no_taper = 0;
my $opt_no_dump = 0;
my $opt_no_flush = 0;
my $opt_no_vault = 0;
my $opt_from_client = 0;
my $opt_exact_match = 0;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'no-taper' => \$opt_no_taper,
    'no-dump' => \$opt_no_dump,
    'no-flush' => \$opt_no_flush,
    'no-vault' => \$opt_no_vault,
    'from-client' => \$opt_from_client,
    'exact-match' => \$opt_exact_match,
    'o=s' => sub {
	push @config_overrides_opts, "-o" . $_[1];
	add_config_override_opt($config_overrides, $_[1]);
    },
) or usage();

usage("No config specified") if (@ARGV < 1);

my $config_name = shift @ARGV;
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

sub user_msg {
    my $msg = shift;

    if ($msg->{'code'} != 2000000 and
	$msg->{'code'} != 2000001) {
	print STDOUT $msg->message() . "\n";
    }
}

my $hostdisk = \@ARGV;
my ($amdump, @messages) = Amanda::Amdump->new(config      => $config_name,
				 no_taper    => $opt_no_taper,
				 no_dump     => $opt_no_dump,
				 no_flush    => $opt_no_flush,
				 no_vault    => $opt_no_vault,
				 from_client => $opt_from_client,
				 exact_match => $opt_exact_match,
				 config_overrides => \@config_overrides_opts,
				 hostdisk    => $hostdisk,
				 user_msg    => \&user_msg);

my $exit_code = $amdump->run(1);
debug("exiting with code $exit_code");
exit($exit_code);

