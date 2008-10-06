#!@PERL@
# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

# PROPERTY:
#
#    DF-PATH	 (Default from PATH): Path to the 'df' binary
#    ZFS-PATH	 (Default from PATH): Path to the 'zfs' binary
#    PFEXEC-PATH (Default from PATH): Path to the 'pfexec' binary
#    PFEXEC	 (Default NO): Set to "YES" if you want to use pfexec
#
use lib '@amperldir@';
use strict;
use Symbol;
use IPC::Open3;
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
use Getopt::Long;
require $APPLICATION_DIR . "/generic-script";

Amanda::Util::setup_application("zfs-snapshot", "client", $CONTEXT_DAEMON);

Amanda::Util::finish_setup($RUNNING_AS_ANY);

debug("program: $0");

my $execute_where;
my $opt_config;
my $opt_host;
my $opt_disk;
my $opt_device;
my @opt_level;
my $opt_index;
my $opt_message;
my $opt_collection;
my $opt_record;
my $df_path  = 'df';
my $zfs_path = 'zfs';
my $pfexec_path = 'pfexec';
my $pfexec = "NO";
my $pfexec_cmd;

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'execute-where=s'  => \$opt_execute_where,
    'config=s'         => \$opt_config,
    'host=s'           => \$opt_host,
    'disk=s'           => \$opt_disk,
    'device=s'         => \$opt_device,
    'level=s'          => \@opt_level,
    'index=s'          => \$opt_index,
    'message=s'        => \$opt_message,
    'collection=s'     => \$opt_collection,
    'record=s'         => \$opt_record,
    'df-path=s'        => \$df_path,
    'zfs-path=s'       => \$zfs_path,
    'pfexec-path=s'    => \$pfexec_path,
    'pfexec=s'         => \$pfexec
) or usage();

sub command_support {
   print "CONFIG YES\n";
   print "HOST YES\n";
   print "DISK YES\n";
   print "MESSAGE-LINE YES\n";
   print "MESSAGE-XML NO\n";
   print "EXECUTE-WHERE YES\n";
}

my $status_good  = 0;
my $status_error = 1;
my $error_status = 0;

#$_[0] action
#$_[1] message
#$_[2] status: 0=good, 1=error
sub print_to_server {
    my($action, $msg, $status) = @_;
    if ($status != 0 ) {
	$error_status = $status;
    }
    if ($action eq "check") {
	if ($status == $status_good) {
	    print STDOUT "OK $msg\n";
	} else {
	    print STDOUT "ERROR $msg\n";
	}
    } elsif ($action eq "estimate") {
	if ($status == $status_good) {
	    #do nothing
	} else {
	    print STDOUT "ERROR $msg\n";
	}
    } elsif ($action eq "backup") {
	if ($status == $status_good) {
	    print STDERR "| $msg\n";
	} else {
	    print STDERR "? $msg\n";
	}
    } elsif ($action eq "restore") {
	print STDOUT "$msg\n";
    } else {
	print STDERR "$msg\n";
    }
}

#$_[0] action
#$_[1] message
#$_[2] status: 0=good, 1=error
sub print_to_server_and_die {
    my $action = $_[0];
    print_to_server( @_ );
    die();
}

sub set_value($) {
    my $action = $_[0];

    if ($opt_execute_where != "client") {
	print_to_server_and_die($action, "amzfs-snapshot must be run on the client 'execute_where client', $status_error);
    }
    if (!defined $opt_device) {
	print_to_server_and_die($action, "'--device' is not provided",
			$status_error);
    }
    if ($df_path ne "df" && !-e $df_path) {
	print_to_server_and_die($action, "Can't execute DF-PATH '$df_path' command",
			$status_error);
    }
    if ($zfs_path ne "zfs" && !-e $zfs_path) {
	print_to_server_and_die($action, "Can't execute ZFS-PATH '$zfs_path' command",
			$status_error);
    }

    if ($pfexec =~ /^YES$/i) {
	$pfexec_cmd = $pfexec_path;
    }
    if (defined $pfexec_cmd && $pfexec_cmd ne "pfexec" && !-e $pfexec_cmd) {
	print_to_server_and_die($action, "Can't execute PFEXEC-PATH '$pfexec_cmd' command",
			$status_error);
    }
    if (!defined $pfexec_cmd) {
	$pfexec_cmd = "";
    }

    debug "running: $df_path $opt_device";
    my @ret = `$df_path $opt_device`;
    if( $? != 0 ) {
	print_to_server_and_die($action, 
				"Failed to find database : $opt_device",
				$status_error);
    }
    chomp @ret;
    @ret = split /:/, $ret[0];
    my $mountpoint;
    my $filesystem;
    if ($ret[0] =~ /(\S*)(\s*)(\()(\S*)(\s*)(\))$/) {
	$mountpoint = $1;
	$filesystem = $4;
    }

    if (!defined $mountpoint || !defined $filesystem) {
	print_to_server_and_die($action,
		"Failed to find mount point for : $opt_device",
		$status_error);
    }

    my $cmd = "$pfexec_cmd $zfs_path get -H mountpoint $filesystem";
    debug "running: $cmd|";
    my $zfs;
    open $zfs, "$cmd|";
    my $line = <$zfs>;
    close $zfs;
    my ($afilesystem, $property, $amountpoint, $source) = split '\t', $line;

    if (!defined $amountpoint) {
	print_to_server_and_die($action,
		"mountpoint not found for zfs-filesystem '$filesystem'",
		$status_error);
    }
    if ($afilesystem != $filesystem) {
	print_to_server_and_die($action,
		"filesystem from 'df' ($filesystem) and 'zfs list' ($afilesystem) differ",
		$status_error);
    }
    if ($amountpoint != $mountpoint) {
	print_to_server_and_die($action,
		"mountpoint from 'df' ($mountpoint) and 'zfs list' ($amountpoint) differ",
		$status_error);
    }

    if (!($opt_device =~ $mountpoint)) {
	print_to_server_and_die($action,
	"mountpoint '$mountpoint' is not a prefix of diskdevice '$opt_device'",
	 $status_error);
    }

    my $dir = $opt_device;
    $dir =~ s,^$mountpoint,,;
    my $snapshot = Amanda::Util::sanitise_filename($opt_device);
    my $directory = "$mountpoint/.zfs/snapshot/$snapshot$dir";

    return ($filesystem, $mountpoint, $dir, $snapshot, $directory);
}

sub create_snapshot {
    my ($filesystem, $snapshot, $action) = @_;
    my $cmd = "$pfexec_cmd $zfs_path snapshot $filesystem\@$snapshot";
    debug "running: $cmd";
    my($wtr, $rdr, $err, $pid);
    my($msg, $errmsg);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    my ($msg) = <$rdr>;
    my ($errmsg) = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if( $? != 0 ) {
	if(defined $msg && defined $errmsg) {
	    print_to_server_and_die($action, "$msg, $errmsg", $status_error);
	} elsif (defined $msg) {
	    print_to_server_and_die($action, $msg, $status_error);
	} elsif (defined $errmsg) {
	    print_to_server_and_die($action, $errmsg, $status_error);
	} else {
	    print_to_server_and_die($action, "cannot create snapshot '$filesystem\@$snapshot': unknown reason", $status_error);
	}
    }
}

sub destroy_snaphot {
    my ($filesystem, $snapshot, $action) = @_;
    my $cmd = "$pfexec_cmd $zfs_path destroy $filesystem\@$snapshot";
    debug "running: $cmd|";
    my($wtr, $rdr, $err, $pid);
    my($msg, $errmsg);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    $msg = <$rdr>;
    $errmsg = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if( $? != 0 ) {
	if(defined $msg && defined $errmsg) {
	    print_to_server_and_die($action, "$msg, $errmsg", $status_error);
	} elsif (defined $msg) {
	    print_to_server_and_die($action, $msg, $status_error);
	} elsif (defined $errmsg) {
	    print_to_server_and_die($action, $errmsg, $status_error);
	} else {
	    print_to_server_and_die($action, "cannot destroy snapshot '$filesystem\@$snapshot': unknown reason", $status_error);
	}
    }
}

#define a execute_on_* function for every execute_on you want the script to do
#something
sub command_pre_dle_amcheck {
    my ($filesystem, $mountpoint, $dir, $snapshot, $directory) =
							set_value("check");
    if ($error_status == $status_good) {
	print_to_server("check", "mountpoint $mountpoint", $status_good);
	print_to_server("check", "dir $dir", $status_good);
	print_to_server("check", "snapshot $snapshot", $status_good);
	print_to_server("check", "directory $directory", $status_good);
	create_snapshot($filesystem, $snapshot, "check");
	print "PROPERTY directory $directory\n";
    }
}

sub command_post_dle_amcheck {
    my ($filesystem, $mountpoint, $dir, $snapshot, $directory) =
							set_value("check");
    destroy_snaphot($filesystem, $snapshot, "check");
}

sub command_pre_dle_estimate {
    my ($filesystem, $mountpoint, $dir, $snapshot, $directory) =
							set_value("estimate");
    if ($error_status == $status_good) {
	create_snapshot($filesystem, $snapshot, "estimate");
	print "PROPERTY directory $directory\n";
    }
}

sub command_post_dle_estimate {
    my ($filesystem, $mountpoint, $dir, $snapshot, $directory) =
							set_value("estimate");
    destroy_snaphot($filesystem, $snapshot, "estimate");
}

sub command_pre_dle_backup {
    my ($filesystem, $mountpoint, $dir, $snapshot, $directory) =
							set_value("backup");
    if ($error_status == $status_good) {
	create_snapshot($filesystem, $snapshot, "backup");
	print "PROPERTY directory $directory\n";
    }
}

sub command_post_dle_backup {
    my ($filesystem, $mountpoint, $dir, $snapshot, $directory) =
							set_value("backup");
    destroy_snaphot($filesystem, $snapshot, "backup");
}

sub usage {
    print <<EOF;
Usage: amzfs-snapshot <command> --execute-where=client --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --df-path=<path/to/df> --zfs-path=<path/to/zfs> --pfexec-path=<path/to/pfexec> --pfexec=<yes|no>.
EOF
    exit(1);
}

do_script($ARGV[0]);
