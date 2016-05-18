#!@PERL@
#
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
#
use lib '@amperldir@';
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_print_errors
  config_dir_relative new_config_overrides add_config_override);
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Disklist;
use Amanda::Paths;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Util qw( :constants );
use Amanda::Label;
use File::Copy;
use File::Basename;
use Getopt::Long;

my $amadmin = "$sbindir/amadmin";
my $amtrmidx = "$amlibexecdir/amtrmidx";
my $amtrmlog = "$amlibexecdir/amtrmlog";

my $dry_run;
my $cleanup;
my $erase;
my $keep_label;
my $external_copy;
my $verbose = 1;
my $help;
my $list_retention;
my $list_no_retention;
my $remove_no_retention;

sub usage() {
    print <<EOF
$0 [-n] [-v] [-q] [-d] [config-overwrites] <config> [label]*
\t--changer changer-name
\t\tSpecify the name of the changer to use (for --erase).
\t--cleanup
\t\tRemove indexes and logs immediately
\t-n, --dryrun
\t\tDo nothing to original files, leave new ones in database directory.
\t--erase
\t\tErase the media, if possible
\t-h, --help
\t\tDisplay this message.
\t--keep-label
\t\tDo not remove label from the tapelist
\t--external-copy
\t\tErase the volume, keep it in tapelist and log
\t\tAssume an external copy fo the volume was done
\t--list-retention
\t\tList all labels require to satisfy the policy of each storage.
\t--list-no-retention
\t\tList all labels not require to satisfy the policy of each storage.
\t--remove-no-retention
\t\tRemove all labels not require to satisfy the policy of each storage.
\t-q, --quiet
\t\tQuiet, opposite of -v.
\t-v, --verbose
\t\tVerbose, list backups of hosts and disks that are being discarded.

This program allows you to invalidate the contents of an existing
backup tape within the Amanda current tape database.  This is meant as
a recovery mecanism for when a good backup is damaged either by faulty
hardware or user error, i.e. the tape is eaten by the tape drive, or
the tape has been overwritten.
EOF
}

sub vlog(@) {
    foreach my $msg (@_) {
        message($msg);
        print "$0: $msg\n" if $verbose;
    }
}

Amanda::Util::setup_application("amrmtape", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides( scalar(@ARGV) + 1 );

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{ bundling });
my $opts_ok = GetOptions(
    'version' => \&Amanda::Util::version_opt,
    "changer=s" => sub { add_config_override_opt( $config_overrides, "-otpchanger=".$_[1] ); },
    "cleanup" => \$cleanup,
    "dryrun|n" => \$dry_run,
    "erase" => \$erase,
    "help|h" => \$help,
    "keep-label" => \$keep_label,
    "external-copy" => \$external_copy,
    "list-retention" => \$list_retention,
    "list-no-retention" => \$list_no_retention,
    "remove-no-retention" => \$remove_no_retention,
    'o=s' => sub { add_config_override_opt( $config_overrides, $_[1] ); },
    "quiet|q" => sub { undef $verbose; },
    "verbose|v" => \$verbose,
);

if (!$opts_ok) {
    usage();
    exit 1;
}

if ($help) {
    usage();
    exit 0;
}

if(scalar(@ARGV) <= 0) {
    print STDERR "Specify a configuration.\n";
    usage();
    exit 1;
}

if ((!$list_retention && !$list_no_retention && !$remove_no_retention) &&
     scalar(@ARGV) == 1) {
    print STDERR "Specify a configuration and label.\n";
    usage();
    exit 1;
}

my ($config_name, @label) = @ARGV;

set_config_overrides($config_overrides);
my $cfg_ok = config_init_with_global( $CONFIG_INIT_EXPLICIT_NAME, $config_name );

my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

sub user_msg {
    my $msg = shift;

    print STDOUT $msg->message() . "\n";
}

sub main {
    my ($finished_cb) = @_;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	# amadmin may later try to load this and will die if it has errors
	# load it now to catch the problem sooner (before we might erase data)
	my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
	$cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
	if ($cfgerr_level >= $CFGERR_ERRORS) {
	    die "Errors processing disklist";
	}

	my $tapelist_file = config_dir_relative(getconf($CNF_TAPELIST));
	my ($tapelist, $message) = Amanda::Tapelist->new($tapelist_file, !$dry_run);
	if (defined $message) {
	    die "Could not read the tapelist: $message";
	}
	unless ($tapelist) {
	    die "Could not read the tapelist";
	}

	if ($list_retention) {
	    my @list = Amanda::Tapelist::list_retention();
	    foreach my $label (@list) {
		print "$label\n";
	    }
	} elsif ($list_no_retention) {
	    my @list = Amanda::Tapelist::list_no_retention();
	    foreach my $label (@list) {
		print "$label\n";
	    }
	} else {
	    my @list;
	    if ($remove_no_retention) {
		@list = Amanda::Tapelist::list_no_retention();
	    } else {
		@list = @label;
	    }
	    my $Label = Amanda::Label->new(tapelist => $tapelist,
					   user_msg => \&user_msg);

	    return $Label->erase(labels        => \@list,
				 cleanup       => $cleanup,
				 dry_run       => $dry_run,
				 erase         => $erase,
				 keep_label    => $keep_label,
				 external_copy => $external_copy,
				 finished_cb   => $steps->{'erase_finished'});
	}
	$finished_cb->();
    };

    step erase_finished => sub {
	my ($err) = @_;

        print STDERR "$err\n" if $err;

        $finished_cb->();
    };
}

main(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
#exit $exit_status;
