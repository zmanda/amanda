#! @PERL@
# Copyright (c) 2012-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use File::Basename;
use IPC::Open3;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Label;
use Amanda::Disklist;
use Amanda::Curinfo;


Amanda::Util::setup_application("amadmin_perl", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);

my ($opt_config, $opt_command);
my ($opt_no_default, $opt_print_source, $opt_exact_match, $opt_sort);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'no-default' => \$opt_no_default,
    'print-source' => \$opt_print_source,
    'exact-match' => \$opt_exact_match,
    'sort' => \$opt_sort,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

usage() unless (@ARGV);
$opt_config = shift @ARGV;

set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die("errors processing config file");
    }
}
Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

usage() unless (@ARGV);
$opt_command = shift @ARGV;
my $exit_status = 0;

sub user_msg_err {
    my $msg = shift;

    print STDERR $msg->message() . "\n";
}

sub user_msg_out {
    my $msg = shift;

    print STDOUT "amadmin: " . $msg->message() . "\n";
}

sub main {
    my ($finished_cb) = @_;
    my $storage;
    my $chg;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $storage->quit() if defined $storage;
			  $chg->quit() if defined $chg };

    step start => sub {
	if ($opt_command eq "reuse" or $opt_command eq "no-reuse") {
	    $steps->{'reuse'}->();
	}

	my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
	my $cfgerr_level += Amanda::Disklist::read_disklist('filename' => $diskfile);
	($cfgerr_level < $CFGERR_ERRORS) || die "Errors processing disklist";

	my $curinfodir = getconf($CNF_INFOFILE);
	my $ci = Amanda::Curinfo->new($curinfodir);
	Amanda::Disklist::do_on_match_disklist(
		user_msg => \&user_msg_out,
		exact_match => $opt_exact_match,
		disk_cb => sub {
				my $dle = shift;
				if ($opt_command eq "force") {
				    $ci->force($dle);
				} elsif ($opt_command eq "unforce") {
				    $ci->unforce($dle);
				} elsif ($opt_command eq "force-level-1") {
				    $ci->force_level_1($dle);
				} elsif ($opt_command eq "force-bump") {
				    $ci->force_bump($dle);
				} elsif ($opt_command eq "force-no-bump") {
				    $ci->force_no_bump($dle);
				} elsif ($opt_command eq "unforce-bump") {
				    $ci->unforce_bump($dle);
				}
			  },
		args   => \@ARGV);
	$finished_cb->();
    };

    step reuse => sub {
	my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	my ($tl, $message) = Amanda::Tapelist->new($tlf);
	if (defined $message) {
            print STDERR "amadmin: Could not read the tapelist: $message";
	    $exit_status = 1;
	    $finished_cb->();
        }
	$storage  = Amanda::Storage->new(tapelist => $tl);
	if ($storage->isa("Amanda::Changer::Error")) {
	    print STDERR "amadmin: $storage\n";
	    $exit_status = 1;
	    $finished_cb->();
	}
	$chg = $storage->{'chg'};
	if ($chg->isa("Amanda::Changer::Error")) {
	    print STDERR "amadmin: $chg\n";
	    $exit_status = 1;
	    $finished_cb->();
	}

	my $Label = Amanda::Label->new(storage  => $storage,
				       tapelist => $tl,
				       user_msg => \&user_msg_err);

	if ($opt_command eq "reuse") {
	    if (@ARGV < 1) {
		print STDERR "amadmin: expecting \"reuse <tapelabel> ...\"\n";
	    } else {
		return $Label->reuse(labels => \@ARGV,
				     finished_cb => $finished_cb);
	    }
	} elsif ($opt_command eq "no-reuse") {
	    if (@ARGV < 1) {
		print STDERR "amadmin: expecting \"no-reuse <tapelabel> ...\"\n";
	    } else {
		return $Label->no_reuse(labels => \@ARGV,
					finished_cb => $finished_cb);
	    }
	} else {
	    print STDERR "Only 'reuse' and 'no-reuse' command\n";
	    usage();
	}
	$finished_cb->();
    };
}

main(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit($exit_status);

