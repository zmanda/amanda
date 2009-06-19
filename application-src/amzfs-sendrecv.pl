#!@PERL@
# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

use lib '@amperldir@';
use strict;
use Getopt::Long;

package Amanda::Application::Amzfs_sendrecv;
use base qw(Amanda::Application Amanda::Application::Zfs);
use File::Copy;
use File::Path;
use IPC::Open3;
use Sys::Hostname;
use Symbol;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants );

sub new {
    my $class = shift;
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $df_path, $zfs_path, $pfexec_path, $pfexec, $keep_snapshot) = @_;
    my $self = $class->SUPER::new($config);

    $self->{config}     = $config;
    $self->{host}       = $host;
    $self->{disk}       = $disk;
    $self->{device}     = $device;
    $self->{level}      = [ @{$level} ];
    $self->{index}      = $index;
    $self->{message}    = $message;
    $self->{collection} = $collection;
    $self->{record}     = $record;
    $self->{df_path}       = $df_path;
    $self->{zfs_path}      = $zfs_path;
    $self->{pfexec_path}   = $pfexec_path;
    $self->{pfexec}        = $pfexec;
    $self->{keep_snapshot} = $keep_snapshot;
    $self->{pfexec_cmd}    = undef;

    if ($self->{keep_snapshot} =~ /^YES$/i) {
        $self->{keep_snapshot} = "YES";
	if (!defined $self->{record}) {
	    $self->{keep_snapshot} = "NO";
 	}
    }

    return $self;
}

sub check_for_backup_failure {
   my $self = shift;
   my $action = shift;

   $self->zfs_destroy_snapshot($action);
}

sub command_support {
   my $self = shift;

   print "CONFIG YES\n";
   print "HOST YES\n";
   print "DISK YES\n";
   print "MAX-LEVEL 9\n";
   print "INDEX-LINE NO\n";
   print "INDEX-XML NO\n";
   print "MESSAGE-LINE YES\n";
   print "MESSAGE-XML NO\n";
   print "RECORD YES\n";
   print "COLLECTION NO\n";
   print "CLIENT-ESTIMATE YES\n";
}

sub command_selfcheck {
    my $self = shift;

    $self->zfs_set_value("check");

    if (!defined $self->{device}) {
	return;
    }

    if ($self->{error_status} == $Amanda::Script_App::GOOD) {
	$self->zfs_create_snapshot("check");
	$self->zfs_destroy_snapshot("check");
	print "OK " . $self->{disk} . "\n";
	print "OK " . $self->{device} . "\n";
    }
}

sub command_estimate() {
    my $self = shift;

    my $level = 0;

    $self->zfs_set_value("estimate");
    $self->zfs_create_snapshot("estimate");

    while (defined ($level = shift @{$self->{level}})) {
      debug "Estimate of level $level";
      my $size = $self->estimate_snapshot($level);
      output_size($level, $size);
    }

    $self->zfs_destroy_snapshot("estimate");

    exit 0;
}

sub output_size {
   my($level) = shift;
   my($size) = shift;
   if($size == -1) {
      print "$level -1 -1\n";
      #exit 2;
   }
   else {
      my($ksize) = int $size / (1024);
      $ksize=32 if ($ksize<32);
      print "$level $ksize 1\n";
   }
}

sub command_backup {
    my $self = shift;

    my $mesgout_fd;
    open($mesgout_fd, '>&=3') || die();
    $self->{mesgout} = $mesgout_fd;

    $self->zfs_set_value("backup");
    $self->zfs_create_snapshot("backup");

    my $size = -1;
    my $level = $self->{level}[0];
    my $cmd;
    debug "Backup of level $level";
    if ($level == 0) {
       $cmd = "$self->{pfexec_cmd} $self->{zfs_path} send $self->{filesystem}\@$self->{snapshot} | $Amanda::Paths::amlibexecdir/teecount";
    } else {
      my $refsnapshotname = $self->zfs_find_snapshot_level($level-1);
      debug "Referenced snapshot name: $refsnapshotname|";
      if ($refsnapshotname ne "") {
        $cmd = "$self->{pfexec_cmd} $self->{zfs_path} send -i $refsnapshotname $self->{filesystem}\@$self->{snapshot} | $Amanda::Paths::amlibexecdir/teecount";
      } else {
        $self->print_to_server_and_die("sendbackup", "cannot backup snapshot '$self->{filesystem}\@$self->{snapshot}': reference snapshot doesn't exists for level $level", $Amanda::Script_App::ERROR);
      }
    }

    debug "running (backup): $cmd";
    my($wtr, $err, $pid);
    my($errmsg);
    $err = Symbol::gensym;
    $pid = open3($wtr, '>&STDOUT', $err, $cmd);
    close $wtr;
    $errmsg = <$err>;
    waitpid $pid, 0;
    close $err;
    if ($? !=  0) {
        if (defined $errmsg) {
            $self->print_to_server_and_die("sendbackup", $errmsg, $Amanda::Script_App::ERROR);
        } else {
            $self->print_to_server_and_die("sendbackup", "cannot backup snapshot '$self->{filesystem}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
        }
    }
    $size = $errmsg;
    debug "Dump done";

    my($ksize) = int ($size/1024);
    $ksize=32 if ($ksize<32);

    print $mesgout_fd "sendbackup: size $ksize\n";
    print $mesgout_fd "sendbackup: end\n";

    # destroy all snapshot of this level and higher
    $self->zfs_purge_snapshot($level, 9, "backup");

    if ($self->{keep_snapshot} eq 'YES') {
	$self->zfs_rename_snapshot($level, "backup");
    } else {
	$self->zfs_destroy_snapshot("backup");
    }

    exit 0;
}

sub estimate_snapshot
{
    my $self = shift;
    my $level = shift;
    my $action = shift;

    debug "\$filesystem = $self->{filesystem}";
    debug "\$snapshot = $self->{snapshot}";
    debug "\$level = $level";

    my $cmd;
    if ($level == 0) {
      $cmd = "$self->{pfexec_cmd} $self->{zfs_path} get -Hp -o value referenced $self->{filesystem}\@$self->{snapshot}";
    } else {
      my $refsnapshotname = $self->zfs_find_snapshot_level($level-1);
      debug "Referenced snapshot name: $refsnapshotname|";
      if ($refsnapshotname ne "") {
        $cmd = "$self->{pfexec_cmd} $self->{zfs_path} send -i $refsnapshotname $self->{filesystem}\@$self->{snapshot} | /usr/bin/wc -c";
      } else {
        return "-1";
      }
    }
    debug "running (estimate): $cmd";
    my($wtr, $rdr, $err, $pid);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    my ($msg) = <$rdr>;
    my ($errmsg) = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if ($? !=  0) {
        if (defined $msg && defined $errmsg) {
            $self->print_to_server_and_die($action, "$msg, $errmsg", $Amanda::Script_App::ERROR);
        } elsif (defined $msg) {
            $self->print_to_server_and_die($action, $msg, $Amanda::Script_App::ERROR);
        } elsif (defined $errmsg) {
            $self->print_to_server_and_die($action, $errmsg, $Amanda::Script_App::ERROR);
        } else {
	    $self->print_to_server_and_die($action, "cannot estimate snapshot '$self->{snapshot}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
	}
    }
    if ($level == 0) {
	my $compratio = $self->get_compratio();
	chop($compratio);
	$msg *= $compratio;
    }

    return $msg;
}

sub get_compratio
{
    my $self = shift;
    my $action = shift;

    my $cmd;
    $cmd =  "$self->{pfexec_cmd} $self->{zfs_path} get -Hp -o value compressratio $self->{filesystem}\@$self->{snapshot}";
    debug "running (get-compression): $cmd";
    my($wtr, $rdr, $err, $pid);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    my ($msg) = <$rdr>;
    my ($errmsg) = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if ($? !=  0) {
        if (defined $msg && defined $errmsg) {
            $self->print_to_server_and_die($action, "$msg, $errmsg", $Amanda::Script_App::ERROR);
        } elsif (defined $msg) {
            $self->print_to_server_and_die($action, $msg, $Amanda::Script_App::ERROR);
        } elsif (defined $errmsg) {
            $self->print_to_server_and_die($action, $errmsg, $Amanda::Script_App::ERROR);
        } else {
	    $self->print_to_server_and_die($action, "cannot read compression ratio '$self->{snapshot}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
	}
    }
    return $msg
}

sub command_index_from_output {
}

sub command_index_from_image {
}

sub command_restore {
}

sub command_print_command {
}

package main;

sub usage {
    print <<EOF;
Usage: amzfs-sendrecv <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --df-path=<path/to/df> --zfs-path=<path/to/zfs> --pfexec-path=<path/to/pfexec> --pfexec=<yes|no> --keep-snapshot=<yes|no>.
EOF
    exit(1);
}

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
my $opt_keep_snapshot = "YES";

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'config=s'        => \$opt_config,
    'host=s'          => \$opt_host,
    'disk=s'          => \$opt_disk,
    'device=s'        => \$opt_device,
    'level=s'         => \@opt_level,
    'index=s'         => \$opt_index,
    'message=s'       => \$opt_message,
    'collection=s'    => \$opt_collection,
    'record'          => \$opt_record,
    'df-path=s'       => \$df_path,
    'zfs-path=s'      => \$zfs_path,
    'pfexec-path=s'   => \$pfexec_path,
    'pfexec=s'        => \$pfexec,
    'keep-snapshot=s' => \$opt_keep_snapshot
) or usage();

my $application = Amanda::Application::Amzfs_sendrecv->new($opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $df_path, $zfs_path, $pfexec_path, $pfexec, $opt_keep_snapshot);

$application->do($ARGV[0]);

