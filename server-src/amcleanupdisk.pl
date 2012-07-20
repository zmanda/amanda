#!@PERL@
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
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
my $kill_enable=0;
my $process_alive=0;
my $verbose=0;
my $clean_holding=0;

sub usage() {
    print "Usage: amcleanupdisk [-v] [-r] conf\n";
    exit 1;
}

Amanda::Util::setup_application("amcleanupdisk", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'v' => \$verbose,
    'r' => \$clean_holding,
    'help|usage' => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

my $config_name = shift @ARGV or usage;

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $amcleanupdisk="$amlibexecdir/amcleanupdisk";

if ( ! -e "$CONFIG_DIR/$config_name" ) {
    die "Configuration directory '$CONFIG_DIR/$config_name' doesn't exist\n";
}
if ( ! -d "$CONFIG_DIR/$config_name" ) {
    die "Configuration directory '$CONFIG_DIR/$config_name' is not a directory\n";
}

my $stdout;
open($stdout, ">&STDOUT") if $verbose;;
my @hfiles = Amanda::Holding::all_files($stdout);
close $stdout if $verbose;
@hfiles = Amanda::Holding::merge_all_files(@hfiles);
while (@hfiles) {
    my $hfile = pop @hfiles;
    if ($hfile->{'header'}->{'type'} == $Amanda::Header::F_DUMPFILE) {
	if ($hfile->{'filename'} =~ /\.tmp$/) {
	    print "Rename tmp holding file: $hfile->{'filename'}\n" if $verbose;
	    Amanda::Holding::rename_tmp($hfile->{'filename'}, 0);
	} else {
	    # normal holding file
	}
    } elsif ($hfile->{'header'}->{'type'} == $Amanda::Header::F_CONT_DUMPFILE) {
	# orphan cont_dumpfile
	if ($clean_holding) {
	    print "Remove orphan chunk file: $hfile->{'filename'}\n" if $verbose;
	    unlink $hfile->{'filename'};
	} else {
	    print "orphan chunk file: $hfile->{'filename'}\n" if $verbose;
	}
    } elsif ($hfile->{'header'}->{'type'} == $Amanda::Header::F_EMPTY) {
	# empty file
	if ($clean_holding) {
	    print "Remove empty file: $hfile->{'filename'}\n" if $verbose;
	    unlink $hfile->{'filename'};
	} else {
	    print "empty holding file: $hfile->{'filename'}\n" if $verbose;
	}
    } elsif ($hfile->{'header'}->{'type'} == $Amanda::Header::F_WEIRD) {
	# weird file
	if ($clean_holding) {
	    print "Remove non amanda file: $hfile->{'filename'}\n" if $verbose;
	    unlink $hfile->{'filename'};
	} else {
	    print "non amanda holding file: $hfile->{'filename'}\n" if $verbose;
	}
    } else {
	# any other file
	if ($clean_holding) {
	    print "Remove file: $hfile->{'filename'}\n" if $verbose;
	    unlink $hfile->{'filename'};
	} else {
	    print "unknown holding file: $hfile->{'filename'}\n" if $verbose;
	}
    }
}

Amanda::Holding::dir_unlink();

Amanda::Util::finish_application();
