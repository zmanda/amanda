#!@PERL@
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

use lib '@amperldir@';
use strict;
use warnings;
use Getopt::Long;

package Amanda::Script::Script_email;
use base qw(Amanda::Script);
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;


sub new {
    my $class = shift;
    my ($execute_where, $config, $host, $disk, $device, $level, $index, $message, $collection, $record, $mailto, $success, $failed) = @_;
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
    $self->{mailto}        = [ @{$mailto} ]; # Copy the array
    $self->{success}       = $success;
    $self->{failed}        = $failed;

    return $self;
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

   $self->sendmail("pre-dle-amcheck");
}

sub command_pre_host_amcheck {
   my $self = shift;

   $self->sendmail("pre-host-amcheck");
}

sub command_post_dle_amcheck {
   my $self = shift;

   $self->sendmail("post-dle-amcheck");
}

sub command_post_host_amcheck {
   my $self = shift;

   $self->sendmail("post-host-amcheck");
}

sub command_pre_dle_estimate {
   my $self = shift;

   $self->sendmail("pre-dle-estimate");
}

sub command_pre_host_estimate {
   my $self = shift;

   $self->sendmail("pre-host-estimate");
}

sub command_post_dle_estimate {
   my $self = shift;

   $self->sendmail("post-dle-estimate");
}

sub command_post_host_estimate {
   my $self = shift;

   $self->sendmail("post-host-estimate");
}

sub command_pre_dle_backup {
   my $self = shift;

   $self->sendmail("pre-dle-backup");
}

sub command_pre_host_backup {
   my $self = shift;

   $self->sendmail("pre-host-backup");
}

sub command_post_dle_backup {
   my $self = shift;

   $self->sendmail("post-dle-backup");
}

sub command_post_host_backup {
   my $self = shift;

   $self->sendmail("post-host-backup");
}

sub command_pre_recover {
   my $self = shift;

   $self->sendmail("pre-recover");
}

sub command_post_recover {
   my $self = shift;

   $self->sendmail("post-recover");
}

sub command_pre_level_recover {
   my $self = shift;

   $self->sendmail("pre-level-recover");
}

sub command_post_level_recover {
   my $self = shift;

   $self->sendmail("post-level-recover");
}

sub command_inter_level_recover {
   my $self = shift;

   $self->sendmail("inter-level-recover");
}

sub sendmail {
   my $self = shift;
   my($function) = @_;
   my $dest;
   if ($self->{mailto}) {
      my $destcheck = join ',', @{$self->{mailto}};
      $destcheck =~ /^(.*)$/;
      $dest = $1;
   } else {
      $dest = "root";
   }

   my $subject =  "$self->{config} $function $self->{host} $self->{disk} $self->{device} " . join (" ", @{$self->{level}});
   my @args = ( "-s", "$self->{config} $function $self->{host} $self->{disk} $self->{device} " . join (" ", @{$self->{level}}), $dest );
   debug("cmd: $Amanda::Constants::MAILER -s \"$subject\" " . $dest);
   my $mail;
   my $result = "";
   if (defined $self->{success}) {
      $result = "SUCCESS ";
   } elsif (defined $self->{failed}) {
      $result = "FAILED ";
   }
   open $mail, '|-', $Amanda::Constants::MAILER, '-s', $subject, $dest;
   print $mail "$result$self->{action} $self->{config} $function $self->{host} $self->{disk} $self->{device} ", join (" ", @{$self->{level}}), "\n";
   close $mail;
}

package main;

sub usage {
    print <<EOF;
Usage: script-email <command> --execute-where=<client|server> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --mailto=<email> [--success|--failed].
EOF
    exit(1);
}

my $opt_version,
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
my @opt_mailto;
my $opt_success;
my $opt_failed;

my @orig_argv = @ARGV;

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version'	      => \$opt_version,
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
    'mailto=s'        => \@opt_mailto,
    'success'         => \$opt_success,
    'failed'          => \$opt_failed
) or usage();

if (defined $opt_version) {
    print "script-email-" . $Amanda::Constants::VERSION , "\n";
    exit(0);
}

my $script = Amanda::Script::Script_email->new($opt_execute_where, $opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, \@opt_mailto, $opt_success, $opt_failed);

Amanda::Debug::debug("Arguments: " . join(' ', @orig_argv));

$script->do($ARGV[0]);
# NOTREACHED
