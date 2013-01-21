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
use XML::Simple;
use IPC::Open3;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Xfer qw( :constants );


Amanda::Util::setup_application("amadmin_perl", "server", $CONTEXT_CMDLINE);

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
config_init($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
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

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist->new($tlf);
my $chg = Amanda::Changer->new(undef, tapelist => $tl);
if ($chg->isa("Amanda::Changer::Error")) {
    print STDERR "amadmin: $chg\n";
    exit 1;
}

if ($opt_command eq "reuse") {
    reuse(@ARGV);
} elsif ($opt_command eq "no-reuse") {
    no_reuse(@ARGV);
} else {
    print STDERR "Only 'reuse' and 'no-reuse' command\n";
    usage();
}

$chg->quit();
Amanda::Util::finish_application();

sub quit {
    Amanda::MainLoop::quit();
}

sub reuse {
    my @labels = @_;
    my $finished_cb = \&quit;
    my $need_write = 0;

    if (@ARGV < 1) {
	print STDERR "amadmin: expecting \"reuse <tapelabel> ...\"\n";
    }

    foreach my $label (@labels) {
	my $tle = $tl->lookup_tapelabel($label);

	if (!defined $tle) {
	    print STDERR "amadmin: tape label $label not found in tapelist.\n";
	    next;
	}

	if ($tle->{'reuse'} == 0) {
	    $tl->reload(1) if $need_write == 0;;
	    $tl->remove_tapelabel($label);
	    $tl->add_tapelabel($tle->{'datestamp'}, $label, $tle->{'comment'},
			       1, $tle->{'meta'}, $tle->{'barcode'},
			       $tle->{'blocksize'});
	    $need_write = 1;
	    print STDERR "amadmin: marking tape $label as reusable.\n";
	} else {
	    print STDERR "amadmin: tape $label already reusable.\n";
	}
    }

    $tl->write() if $need_write == 1;

    my $steps = define_steps
	cb_ref =>\$finished_cb;

    step start => sub {
	    return $chg->set_reuse(labels => \@labels,
                                   finished_cb => $finished_cb);
    };


    Amanda::MainLoop::run();
}

sub no_reuse {
    my @labels = @_;
    my $finished_cb = \&quit;
    my $need_write = 0;

    if (@ARGV < 1) {
	print STDERR "amadmin: expecting \"no-reuse <tapelabel> ...\"\n";
    }

    foreach my $label (@labels) {
	my $tle = $tl->lookup_tapelabel($label);

	if (!defined $tle) {
	    print STDERR "amadmin: tape label $label not found in tapelist.\n";
	    next;
	}

	if ($tle->{'reuse'} == 1) {
	    $tl->reload(1) if $need_write == 0;
	    $tl->remove_tapelabel($label);
	    $tl->add_tapelabel($tle->{'datestamp'}, $label, $tle->{'comment'},
			       0, $tle->{'meta'}, $tle->{'barcode'},
			       $tle->{'blocksize'});
	    $need_write = 1;
	    print STDERR "amadmin: marking tape $label as not reusable.\n";
	} else {
	    print STDERR "amadmin: tape $label already not reusable.\n";
	}
    }

    $tl->write() if $need_write == 1;

    my $steps = define_steps
	cb_ref =>\$finished_cb;

    step start => sub {
	    return $chg->set_no_reuse(labels => \@labels,
                                      finished_cb => $finished_cb);
    };

    Amanda::MainLoop::run();
}


