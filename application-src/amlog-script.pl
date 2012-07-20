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
use File::Basename;

package Amanda::Script::amlog_script;
use base qw(Amanda::Script);
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;


sub new {
    my $class = shift;
    my ($execute_where, $config, $host, $disk, $device, $level, $index, $message, $collection, $record, $logfile, $text) = @_;
    my $self = $class->SUPER::new($execute_where, $config);

    $self->{execute_where} = $execute_where;
    $self->{config}        = $config;
    $self->{host}          = $host;
    $self->{disk}          = $disk;
    $self->{device}        = $device;
    $self->{level}         = [ @{$level} ]; # Copy the array
    $self->{index}         = $index;
    $self->{message}       = $message;
    $self->{collection}    = $collection;
    $self->{record}        = $record;
    $self->{logfile}       = $logfile;
    $self->{text}          = $text;

    return $self;
}

sub setup() {
    my $self = shift;

    if (!defined $self->{logfile}) {
	$self->print_to_server_and_die("property LOGFILE not set",
				       $Amanda::Script_App::ERROR);
    }

    my $dirname = File::Basename::dirname($self->{logfile});
    if (! -e $dirname) {
	$self->print_to_server_and_die("Directory '$dirname' doesn't exist",
				       $Amanda::Script_App::ERROR);
    }
    if (! -d $dirname) {
	$self->print_to_server_and_die(
				"Directory '$dirname' is not a directory",i
				$Amanda::Script_App::ERROR);
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
sub command_pre_amcheck {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-amcheck");
}

sub command_pre_dle_amcheck {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-dle-amcheck");
}

sub command_pre_host_amcheck {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-host-amcheck");
}

sub command_post_amcheck {
   my $self = shift;

   $self->setup();
   $self->log_data("post-amcheck");
}

sub command_post_dle_amcheck {
   my $self = shift;

   $self->setup();
   $self->log_data("post-dle-amcheck");
}

sub command_post_host_amcheck {
   my $self = shift;

   $self->setup();
   $self->log_data("post-host-amcheck");
}

sub command_pre_estimate {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-estimate");
}

sub command_pre_dle_estimate {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-dle-estimate");
}

sub command_pre_host_estimate {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-host-estimate");
}

sub command_post_estimate {
   my $self = shift;

   $self->setup();
   $self->log_data("post-estimate");
}

sub command_post_dle_estimate {
   my $self = shift;

   $self->setup();
   $self->log_data("post-dle-estimate");
}

sub command_post_host_estimate {
   my $self = shift;

   $self->setup();
   $self->log_data("post-host-estimate");
}

sub command_pre_backup {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-backup");
}

sub command_pre_dle_backup {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-dle-backup");
}

sub command_pre_host_backup {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-host-backup");
}

sub command_post_backup {
   my $self = shift;

   $self->setup();
   $self->log_data("post-backup");
}

sub command_post_dle_backup {
   my $self = shift;

   $self->setup();
   $self->log_data("post-dle-backup");
}

sub command_post_host_backup {
   my $self = shift;

   $self->setup();
   $self->log_data("post-host-backup");
}

sub command_pre_recover {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-recover");
}

sub command_post_recover {
   my $self = shift;

   $self->setup();
   $self->log_data("post-recover");
}

sub command_pre_level_recover {
   my $self = shift;

   $self->setup();
   $self->log_data("pre-level-recover");
}

sub command_post_level_recover {
   my $self = shift;

   $self->setup();
   $self->log_data("post-level-recover");
}

sub command_inter_level_recover {
   my $self = shift;

   $self->setup();
   $self->log_data("inter-level-recover");
}

sub log_data {
   my $self = shift;
   my($function) = shift;
   my $log;

   my $text = $self->{'text'} || "";
   open($log, ">>$self->{logfile}") ||
	$self->print_to_server_and_die(
			"Can't open logfile '$self->{logfile}' for append: $!",
			$Amanda::Script_App::ERROR);
   print $log "$self->{action} $self->{config} $function $self->{execute_where} $self->{host} $self->{disk} $self->{device} ", join (" ", @{$self->{level}}), " $text\n";
   close $log;
}

package main;

sub usage {
    print <<EOF;
Usage: amlog-script <command> --execute-where=<client|server> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --logfile=<filename>.
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
my $opt_logfile;
my $opt_text;

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'execute-where=s' => \$opt_execute_where,
    'config=s'        => \$opt_config,
    'host=s'          => \$opt_host,
    'disk=s'          => \$opt_disk,
    'device=s'        => \$opt_device,
    'level=s'         => \@opt_level,
    'index=s'         => \$opt_index,
    'message=s'       => \$opt_message,
    'collection=s'    => \$opt_collection,
    'record=s'        => \$opt_record,
    'logfile=s'       => \$opt_logfile,
    'text=s'          => \$opt_text
) or usage();

my $script = Amanda::Script::amlog_script->new($opt_execute_where, $opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $opt_logfile, $opt_text);

$script->do($ARGV[0]);

