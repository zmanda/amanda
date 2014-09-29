#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use File::Basename;
use Getopt::Long;
use Text::Wrap;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants match_labelstr );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header qw( :constants );
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Label;

my $exit_status = 0;

##
# Subcommand handling

my %subcommands;

sub usage {
    print STDERR "Usage: amlabel [--barcode <barcode>] [--meta <meta>] [--version]\n"
	       . " [--assign [--pool <pool>] [--storage <storage>]] [-f]\n"
	       . " [-o configoption]*\n"
	       . " <conf> [<label>] [slot <slot-number>]\n";
    exit(1);
}

sub user_msg {
    my $msg = shift;

    if ($msg->isa("Amanda::Changer")) {
	print STDOUT $msg->message() . "\n";
    } else {
	print STDOUT "$msg\n";
    }
}

Amanda::Util::setup_application("amlabel", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);
my ($opt_force, $opt_config, $opt_slot, $opt_label);
my ($opt_barcode, $opt_meta, $opt_assign, $opt_pool, $opt_storage);

$opt_force = 0;
$opt_barcode = undef;
$opt_meta = undef;
$opt_assign = undef;
$opt_pool = undef;
$opt_storage = undef;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'o=s'        => sub { add_config_override_opt($config_overrides, $_[1]); },
    'f'          => \$opt_force,
    'barcode=s'  => \$opt_barcode,
    'meta=s'     => \$opt_meta,
    'pool=s'     => \$opt_pool,
    'storage=s'  => \$opt_storage,
    'assign'     => \$opt_assign,
    'version'    => \&Amanda::Util::version_opt,
) or usage();

usage() if @ARGV == 0;
$opt_config = $ARGV[0];
if (@ARGV == 1) {
    $opt_slot = undef;
    $opt_label = undef;
} elsif (@ARGV == 2) {
    $opt_slot = undef;
    $opt_label = $ARGV[1];
} elsif (@ARGV == 3 and $ARGV[1] eq 'slot') {
    $opt_slot = $ARGV[2];
    $opt_label = undef;
} elsif (@ARGV == 4 and $ARGV[2] eq 'slot') {
    $opt_slot = $ARGV[3];
    $opt_label = $ARGV[1];
} else {
    usage();
}

set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	print STDERR "errors processing config file";
	exit 1;
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my ($tlf, $tl);

sub failure {
    my ($msg, $finished_cb) = @_;
    print STDERR "$msg\n";
    $exit_status = 1;
    $finished_cb->();
}

sub main {
    my ($finished_cb) = @_;
    my $storage;
    my $chg;
    my $dev;
    my $dev_ok;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $storage->quit() if defined $storage;
			  $chg->quit() if defined $chg };

    step start => sub {
	$tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	($tl, my $message) = Amanda::Tapelist->new($tlf);
	if (defined $message) {
	    return failure("Can't load tapelist file ($tlf): $message", $finished_cb);
	}

	$storage  = Amanda::Storage->new(tapelist => $tl);
	return failure("$storage", $finished_cb) if $storage->isa("Amanda::Massage");
	$chg = $storage->{'chg'};
	return failure($chg, $finished_cb) if $chg->isa("Amanda::Message");

	my $Label = Amanda::Label->new(storage  => $storage,
				       tapelist => $tl,
				       user_msg => \&user_msg);

	if ($opt_assign) {
	    return $Label->assign(label   => $opt_label,
				  meta    => $opt_meta,
				  force   => $opt_force,
				  barcode => $opt_barcode,
				  pool    => $opt_pool,
				  storage => $opt_storage,
				  finished_cb => $steps->{'assign_finished'});
	}

	if (defined($opt_label) && !$opt_force) {
	    if ($tl->lookup_tapelabel($opt_label)) {
		return failure("Label '$opt_label' already on a volume", $finished_cb);
	    }
	}

	return $Label->label(slot    => $opt_slot,
			     label   => $opt_label,
			     meta    => $opt_meta,
			     force   => $opt_force,
			     barcode => $opt_barcode,
			     finished_cb => $steps->{'label_finished'});
    };

    step assign_finished => sub {
	my $err = @_;

	if ($err) {
	    $exit_status = 1;
            if ($err != 1) {
		print $err;
	    }
	};
	$finished_cb->();
    };

    step label_finished => sub {
	my ($err) = @_;

	return failure($err, $finished_cb) if $err;

	$finished_cb->();
    };

}

main(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit($exit_status);
