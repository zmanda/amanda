#!@PERL@
#
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
#
use lib '@amperldir@';
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_print_errors
  config_dir_relative new_config_overrides add_config_override);
use Amanda::Changer;
use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Disklist;
use Amanda::Paths;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Util qw( :constants );
use File::Copy;
use File::Basename;
use Getopt::Long;

my $amadmin = "$sbindir/amadmin";
my $amtrmidx = "$amlibexecdir/amtrmidx";
my $amtrmlog = "$amlibexecdir/amtrmlog";

my $dry_run;
my $cleanup;
my $erase;
my $changer_name;
my $keep_label;
my $verbose = 1;
my $help;
my $logdir;
my $log_file;
my $log_created = 0;

sub die_handler {
    if ($log_created == 1) {
        unlink $log_file;
        $log_created = 0;
    }
}
$SIG{__DIE__} = \&die_handler;

sub int_handler {
    if ($log_created == 1) {
        unlink $log_file;
        $log_created = 0;
    }
    die "Interrupted\n";
}
$SIG{INT} = \&int_handler;

sub usage() {
    print <<EOF
$0 [-n] [-v] [-q] [-d] [config-overwrites] <config> <label>
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

Amanda::Util::setup_application("amrmtape", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides( scalar(@ARGV) + 1 );

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{ bundling });
my $opts_ok = GetOptions(
    'version' => \&Amanda::Util::version_opt,
    "changer=s" => \$changer_name,
    "cleanup" => \$cleanup,
    "dryrun|n" => \$dry_run,
    "erase" => \$erase,
    "help|h" => \$help,
    "keep-label" => \$keep_label,
    'o=s' => sub { add_config_override_opt( $config_overrides, $_[1] ); },
    "quiet|q" => sub { undef $verbose; },
    "verbose|v" => \$verbose,
);

unless ($opts_ok && scalar(@ARGV) == 2) {
    unless (scalar(@ARGV) == 2) {
        print STDERR "Specify a configuration and label.\n";
    }
    usage();
    exit 1;
}

if ($help) {
    usage();
    exit 0;
}

my ($config_name, $label) = @ARGV;

set_config_overrides($config_overrides);
my $cfg_ok = config_init( $CONFIG_INIT_EXPLICIT_NAME, $config_name );

my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);
$logdir = config_dir_relative(getconf($CNF_LOGDIR));
$log_file = "$logdir/log";

if ($erase) {
    # Check for log file existance
    if (-e $log_file) {
        `amcleanup -p $config_name`;
    }

    if (-e $log_file) {
        local *LOG;
        open(LOG,  $log_file);
        my $info_line = <LOG>;
        close LOG;
        $info_line =~ /^INFO (.*) .* pid .*$/;
        my $process_name = $1;
        print "$process_name is running, or you must run amcleanup\n";
        exit 1;
    }
}

# amadmin may later try to load this and will die if it has errors
# load it now to catch the problem sooner (before we might erase data)
my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
$cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
if ($cfgerr_level >= $CFGERR_ERRORS) {
    die "Errors processing disklist";
}

my $tapelist_file = config_dir_relative(getconf($CNF_TAPELIST));
my $tapelist = Amanda::Tapelist->new($tapelist_file, !$dry_run);
unless ($tapelist) {
    die "Could not read the tapelist";
}


my $scrub_db = sub {
    my $t = $tapelist->lookup_tapelabel($label);
    if ($keep_label) {
        $t->{'datestamp'} = 0 if $t;
    } elsif (!defined $t) {
	print "label '$label' not found in $tapelist_file\n";
	exit 0;
    } else {
        $tapelist->remove_tapelabel($label);
    }

    #take a copy in case we roolback
    my $backup_tapelist_file = dirname($tapelist_file) . "-backup-amrmtape-" . time();
    if (-x $tapelist_file) {
	unless (copy($tapelist_file, $backup_tapelist_file)) {
	    die "Failed to copy/backup $tapelist_file to $backup_tapelist_file";
	}
    }

    unless ($dry_run) {
        $tapelist->write();
    }

    my $tmp_curinfo_file = "$AMANDA_TMPDIR/curinfo-amrmtape-" . time();
    unless (open(AMADMIN, "$amadmin $config_name export |")) {
        die "Failed to execute $amadmin: $! $?";
    }
    open(CURINFO, ">$tmp_curinfo_file");

    sub info_line($) {
        print CURINFO "$_[0]";
    }

    my $host;
    my $disk;
    my $dead_level = 10;
    while(my $line = <AMADMIN>) {
        my @parts = split(/\s+/, $line);
        if ($parts[0] =~ /^CURINFO|#|(?:command|last_level|consecutive_runs|(?:full|incr)-(?:rate|comp)):$/) {
            info_line $line;
        } elsif ($parts[0] eq 'host:') {
            $host = $parts[1];
            info_line $line;
        } elsif ($parts[0] eq 'disk:') {
            $disk = $parts[1];
            info_line $line;
        } elsif ($parts[0] eq 'history:') {
            info_line $line;
        } elsif ($line eq "//\n") {
            info_line $line;
            $dead_level = 10;
        } elsif ($parts[0] eq 'stats:') {
            if (scalar(@parts) < 6 || scalar(@parts) > 8) {
                die "unexpected number of fields in \"stats\" entry for $host:$disk\n\t$line";
            }
            my $level = $parts[1];
            my $cur_label = $parts[7];
            if (defined $cur_label and $cur_label eq $label) {
                $dead_level = $level;
                vlog "Discarding Host: $host, Disk: $disk, Level: $level\n";
            } elsif ( $level > $dead_level ) {
                vlog "Discarding Host: $host, Disk: $disk, Level: $level\n";
            } else {
                info_line $line;
            }
        } else {
            die "Error: unrecognized line of input:\n\t$line";
        }
    }

    my $rollback_from_curinfo = sub {
            unlink $tmp_curinfo_file;
            return if $keep_label;
            unless (move($backup_tapelist_file, $tapelist_file)) {
                printf STDERR "Failed to rollback new tapelist.\n";
            }
    };

    close CURINFO;

    unless (close AMADMIN) {
        $rollback_from_curinfo->();
        die "$amadmin exited with non-zero while exporting: $! $?";
    }

    unless ($dry_run) {
        if (system("$amadmin $config_name import < $tmp_curinfo_file")) {
            $rollback_from_curinfo->();
            die "$amadmin exited with non-zero while importing: $! $?";
        }
    }

    unlink $tmp_curinfo_file;
    unlink $backup_tapelist_file;

    if ($cleanup && !$dry_run) {
        if (system($amtrmlog, $config_name)) {
            die "$amtrmlog exited with non-zero while scrubbing logs: $! $?";
        }
        if (system($amtrmidx, $config_name)) {
            die "$amtrmidx exited with non-zero while scrubbing indexes: $! $?";
        }
    }

    Amanda::MainLoop::quit();
};

my $erase_volume = make_cb('erase_volume' => sub {
    if ($erase) {
        $log_created = 1;
        local *LOG;
        open(LOG, ">$log_file");
        print LOG "INFO amrmtape amrmtape pid $$\n";
        close LOG;
	my $chg = Amanda::Changer->new($changer_name, tapelist => $tapelist);
	$chg->load(
	    'label' => $label,
	    'res_cb' => sub {
		my ($err, $resv) = @_;
		die $err if $err;

                my $dev = $resv->{'device'};
                die "Can not erase $label because the device doesn't support this feature"
                    unless $dev->property_get('full_deletion');

		my $rel_cb = make_cb('rel_cb' => sub {
		    $resv->release(finished_cb => sub {
			my ($err) = @_;

			$chg->quit();
			die $err if $err;

			$scrub_db->();
		    });
		});

                if (!$dry_run) {
                    $dev->erase()
                        or die "Failed to erase volume";
		    $resv->set_label(finished_cb => sub {
			$dev->finish();

			# label the tape with the same label it had
			if ($keep_label) {
			    $dev->start($ACCESS_WRITE, $label, undef)
				or die "Failed to write tape label";
			    return $resv->set_label(label => $label, finished_cb => $rel_cb);
			}
			$rel_cb->();
		    });
		} else {
		    $rel_cb->();
		}

            });
    } else {
        $scrub_db->();
    }
});

# kick things off
$erase_volume->();
Amanda::MainLoop::run();

if ($log_created == 1) {
    unlink $log_file;
    $log_created = 0;
}

Amanda::Util::finish_application();
