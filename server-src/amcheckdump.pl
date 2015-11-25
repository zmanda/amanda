#! @PERL@
# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

use Amanda::Config qw( :init :getconf );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Getopt::Long;
use Amanda::Cmdline qw( :constants parse_dumpspecs );
use Amanda::CheckDump;

sub usage {
    print <<EOF;
USAGE:	amcheckdump [ --timestamp|-t timestamp ] [-o configoption]* <conf>
    amcheckdump validates Amanda dump images by reading them from storage
volume(s), and verifying archive integrity if the proper tool is locally
available. amcheckdump does not actually compare the data located in the image
to anything; it just validates that the archive stream is valid.
    Arguments:
	config       - The Amanda configuration name to use.
	-t timestamp - The run of amdump or amflush to check. By default, check
			the most recent dump; if this parameter is specified,
			check the most recent dump matching the given
			date- or timestamp.
	-o configoption	- see the CONFIGURATION OVERRIDE section of amanda(8)
EOF
    exit(1);
}

## Application initialization

Amanda::Util::setup_application("amcheckdump", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $exit_code = 0;

my $opt_timestamp;
my $opt_verbose = 0;
my $opt_assume = 0;
my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'timestamp|t=s' => \$opt_timestamp,
    'verbose|v'     => \$opt_verbose,
    'assume=s'      => \$opt_assume,
    'help|usage|?'  => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

usage() if (@ARGV < 1);

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

# and the disklist
my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
$cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
if ($cfgerr_level >= $CFGERR_ERRORS) {
    print STDERR "errors processing disklist\n";
    exit(1);
}

sub user_msg {
    my $msg = shift;

    print STDOUT $msg . "\n";
}

my $checkdump;
sub main {
    my ($finished_cb) = @_;
    $checkdump = Amanda::CheckDump->new();

    return $finished_cb->(1) if !defined $checkdump;

    $checkdump->run(
	assume    => $opt_assume,
	timestamp => $opt_timestamp,
	verbose   => $opt_verbose,
	finished_cb => $finished_cb);
}

my $exit_status = 0;
sub checkdump_done {
    my $lexit_status = shift;

    $exit_status = $lexit_status if defined $lexit_status;
    Amanda::MainLoop::quit();
}

Amanda::MainLoop::call_later(sub { main(\&checkdump_done); });
Amanda::MainLoop::run();

if ($exit_status == 0) {
    $checkdump->user_message(Amanda::CheckDump::Message->new(
                    source_filename => __FILE__,
                    source_line     => __LINE__,
                    code            => 2700006,
                    severity        => $Amanda::Message::SUCCESS));
} else {
    $checkdump->user_message(Amanda::CheckDump::Message->new(
                    source_filename => __FILE__,
                    source_line     => __LINE__,
                    code            => 2700007,
                    severity        => $Amanda::Message::ERROR));
}
Amanda::Util::finish_application();
exit($exit_status);

