#!@PERL@
#
# Copyright (c) 2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
#
use lib '@amperldir@';
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_print_errors config_dir_relative );
use Amanda::Changer;
use Amanda::Device qw( :constants );
use Amanda::Paths;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Util qw( :constants );
use File::Copy;
use Getopt::Long;

my $USE_VERSION_SUFFIXES='@USE_VERSION_SUFFIXES@';
my $suf = ( $USE_VERSION_SUFFIXES =~ /^yes$/i ) ? '-@VERSION@' : '';

my $amadmin = "$sbindir/amadmin$suf";

my $dry_run;
my $erase;
my $changer_name;
my $verbose = 1;
my $help;

sub usage() {
    print <<EOF
$0 [-n] [-v] [-q] [-d] <configuration> <label>
\t--dryrun
\t-n Do nothing to original files, leave new ones in database directory.
\t--erase Erase the media, if possible
\t--changer changer-name Specify the name of the changer to use (for --erase).
\t--verbose
\t-v Verbose, list backups of hosts and disks that are being discarded.
\t--quiet
\t-q Quiet, opposite of -v.
\t--help
\t-h Display this message.
This program allows you to invalidate the contents of an existing
backup tape within the Amanda current tape database.  This is meant
as a recovery mecanism for when a good backup is damaged either by
faulty hardware or user error, i.e. the tape is eaten by the tape drive,
or the tape has been overwritten.
EOF
}

sub vlog(@) {
    plog(@_) if $verbose;
}

sub plog(@) {
    print "$0: " . join("\n$0: ", @_);
}

Amanda::Util::setup_application("amrmtape", "server", $CONTEXT_CMDLINE);

my $opts_ok = GetOptions(
    "dryrun|n" => \$dry_run,
    "erase" => \$erase,
    "changer=s" => \$changer_name,
    "verbose|v" => \$verbose,
    "quiet|q" => sub {undef $verbose;},
    "help|h" => \$help,
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

config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $scrub_db = sub {
    my $tapelist_file = config_dir_relative(getconf($CNF_TAPELIST));
    my $tapelist = Amanda::Tapelist::read_tapelist($tapelist_file);
    unless ($tapelist) {
        die "Could not read the tapelist";
    }

    $tapelist->remove_tapelabel($label);
    my $tmp_tapelist_file = "$AMANDA_TMPDIR/tapelist-amrmtape-" . time();
    my $backup_tapelist_file = "$AMANDA_TMPDIR/tapelist-backup-amrmtape-" . time();
    # writing to temp and then moving is generally safer than writing directly
    unless ($dry_run) {
        unless (copy($tapelist_file, $backup_tapelist_file)) {
            die "Failed to copy/backup $tapelist_file to $backup_tapelist_file";
        }
        $tapelist->write($tmp_tapelist_file);
        unless (move($tmp_tapelist_file, $tapelist_file)) {
            die "Failed to replace old tapelist  with new tapelist.";
        }
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
            if ($cur_label eq $label) {
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
            unless (move($backup_tapelist_file, $tapelist_file)) {
                printf STDERR "Failed to rollback new tapelist.\n";
            }
            unlink $tmp_curinfo_file;
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

    Amanda::MainLoop::quit();
};

my $erase_volume = sub {
    if ($erase) {
	my $chg = Amanda::Changer->new($changer_name);
	$chg->load(
	    'label' => $label,
	    'res_cb' => sub {
		my ($err, $resv) = @_;
		die $err if $err;

		my $dev = Amanda::Device->new($resv->{device_name});
                die "Can not erase $label because the device doesn't support this feature"
                    unless $dev->property_get('full_deletion');
		if (!$dry_run) {
		    $dev->erase()
			or die "Failed to erase volume";
		    $dev->finish();
		}

		$scrub_db->();
	    });
    } else {
	$scrub_db->();
    }
};

# kick things off
Amanda::MainLoop::call_later($erase_volume);
Amanda::MainLoop::run();
