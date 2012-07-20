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

# PROPERTY:
#
#    DF-PATH	 (Default from PATH): Path to the 'df' binary
#    ZFS-PATH	 (Default from PATH): Path to the 'zfs' binary
#    PFEXEC-PATH (Default from PATH): Path to the 'pfexec' binary
#    PFEXEC	 (Default NO): Set to "YES" if you want to use pfexec
#
use lib '@amperldir@';
use strict;
use warnings;
use Getopt::Long;

package Amanda::Script::Amzfs_snapshot;
use base qw(Amanda::Script Amanda::Application::Zfs);
use Symbol;
use IPC::Open3;
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;

sub new {
    my $class = shift;
    my ($execute_where, $config, $host, $disk, $device, $level, $index, $message, $collection, $record, $df_path, $zfs_path, $pfexec_path, $pfexec) = @_;
    my $self = $class->SUPER::new($execute_where, $config);

    $self->{execute_where} = $execute_where;
    $self->{config}        = $config;
    $self->{host}          = $host;
    if (defined $disk) {
	$self->{disk}      = $disk;
    } else {
	$self->{disk}      = $device;
    }
    if (defined $device) {
	$self->{device}    = $device;
    } else {
	$self->{device}    = $disk;
    }
    $self->{level}         = [ @{$level} ]; # Copy the array
    $self->{index}         = $index;
    $self->{message}       = $message;
    $self->{collection}    = $collection;
    $self->{record}        = $record;
    $self->{df_path}       = $df_path;
    $self->{zfs_path}      = $zfs_path;
    $self->{pfexec_path}   = $pfexec_path;
    $self->{pfexec}        = $pfexec;
    $self->{pfexec_cmd}    = undef;

    return $self;
}

sub zfs_snapshot_set_value() {
   my $self   = shift;

   $self->zfs_set_value();

   if (!defined $self->{device}) {
       return;
   }

   if (!defined $self->{mountpoint}) {
       $self->print_to_server("$self->{disk} is not a directory", $Amanda::Script_App::ERROR);
	
   }
}

sub command_support {
   my $self = shift;

   print "CONFIG YES\n";
   print "HOST YES\n";
   print "DISK YES\n";
   print "MESSAGE-LINE YES\n";
   print "MESSAGE-XML NO\n";
   print "EXECUTE-WHERE YES\n";
}

#define a execute_on_* function for every execute_on you want the script to do
#something
sub command_pre_dle_amcheck {
    my $self = shift;

    $self->zfs_snapshot_set_value();

    if (!defined $self->{device}) {
	return;
    }

    if ($self->{error_status} == $Amanda::Script_App::GOOD) {
	if (defined $self->{mountpoint}) {
	    $self->print_to_server("mountpoint $self->{mountpoint}", $Amanda::Script_App::GOOD);
	    $self->print_to_server("directory $self->{directory}", $Amanda::Script_App::GOOD);
	    $self->print_to_server("dir $self->{dir}", $Amanda::Script_App::GOOD);
	}
	$self->print_to_server("snapshot $self->{snapshot}", $Amanda::Script_App::GOOD);
	$self->zfs_create_snapshot("check");
	print "PROPERTY directory $self->{directory}\n";
    }
}

sub command_post_dle_amcheck {
    my $self = shift;

    $self->zfs_snapshot_set_value();

    if (!defined $self->{device}) {
	return;
    }

    $self->zfs_destroy_snapshot("check");
}

sub command_pre_dle_estimate {
    my $self = shift;

    $self->zfs_snapshot_set_value();
    if ($self->{error_status} == $Amanda::Script_App::GOOD) {
	$self->zfs_create_snapshot("estimate");
	print "PROPERTY directory $self->{directory}\n";
    }
}

sub command_post_dle_estimate {
    my $self = shift;

    $self->zfs_snapshot_set_value();
    $self->zfs_destroy_snapshot("estimate");
}

sub command_pre_dle_backup {
    my $self = shift;

    $self->zfs_snapshot_set_value();
    if ($self->{error_status} == $Amanda::Script_App::GOOD) {
	$self->zfs_create_snapshot("backup");
	print "PROPERTY directory $self->{directory}\n";
    }
}

sub command_post_dle_backup {
    my $self = shift;

    $self->zfs_snapshot_set_value("backup");
    $self->zfs_destroy_snapshot("backup");
}

package main;

sub usage {
    print <<EOF;
Usage: amzfs-snapshot <command> --execute-where=client --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --df-path=<path/to/df> --zfs-path=<path/to/zfs> --pfexec-path=<path/to/pfexec> --pfexec=<yes|no>.
EOF
    exit(1);
}

my $opt_execute_where;
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

my $script = Amanda::Script::Amzfs_snapshot->new($opt_execute_where, $opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $df_path, $zfs_path, $pfexec_path, $pfexec);
$script->do($ARGV[0]);
