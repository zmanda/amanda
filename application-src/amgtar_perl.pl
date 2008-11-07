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

use lib '@amperldir@';
use strict;
use Getopt::Long;

package Amanda::Application::amgtar_perl;
use base qw(Amanda::Application);
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
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $calcsize) = @_;
    my $self = $class->SUPER::new();

    $self->{runtar}  = ${Amanda::Paths::amlibexecdir} ."/runtar" .
		       $self->{'suf'};
    $self->{gnulist} = $Amanda::Paths::GNUTAR_LISTED_INCREMENTAL_DIR;
    $self->{gnutar}  = $Amanda::Constants::GNUTAR;

    $self->{config}     = $config;
    $self->{host}       = $host;
    $self->{disk}       = $disk;
    $self->{device}     = $device;
    $self->{level}      = [ @{$level} ];
    $self->{index}      = $index;
    $self->{message}    = $message;
    $self->{collection} = $collection;
    $self->{record}     = $record;
    $self->{calcsize}   = $calcsize;

    return $self;
}

sub command_support {
   my $self = shift;

   print "CONFIG YES\n";
   print "HOST YES\n";
   print "DISK YES\n";
   print "MAX-LEVEL 9\n";
   print "INDEX-LINE YES\n";
   print "INDEX-XML NO\n";
   print "MESSAGE-LINE YES\n";
   print "MESSAGE-XML NO\n";
   print "RECORD YES\n";
   print "COLLECTION NO\n";
   print "MULTI-ESTIMATE YES\n";
   print "CALCSIZE YES\n";
}

sub command_selfcheck {
   my $self = shift;

   print "OK " . $self->{disk} . "\n";
   print "OK " . $self->{device} . "\n";
   #check binary
   #check statefile
   #check amdevice
   #check property include/exclude
}

sub command_estimate {
   my $self = shift;

   if (defined $self->{calcsize}) {
      $self->run_calcsize("GNUTAR", undef);
      return;
   }

   my($listdir) = $self->{'host'} . $self->{'disk'};
   $listdir     =~ s/\//_/g;
   my $gnufile;
   my $level;
   while (defined ($level = shift @{$self->{level}})) {
      if($level == 0) {
         open($gnufile, ">$self->{gnulist}/${listdir}_${level}.new") || die();
         close($gnufile) || die();
      }
      else {
         my($prev_level) = $level - 1;
         if (-f "$self->{gnulist}/${listdir}_${prev_level}") {
           copy("$self->{gnulist}/${listdir}_${prev_level}", "$self->{gnulist}/${listdir}_${level}.new");
         } else {
           open($gnufile, ">$self->{gnulist}/${listdir}_${level}.new") || die();
           close($gnufile) || die();
	#print "ERROR file $self->{gnulist}/${listdir}_${level}.new doesn't exist\n";
         }
      }
      my($size) = -1;
      my(@cmd) = ($self->{runtar}, $self->{'config'}, $self->{'gnutar'}, "--create", "--directory", $self->{'device'}, "--listed-incremental", "$self->{gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "/dev/null", ".");
      debug("cmd:" . join(" ", @cmd));
      my $wtrfh;
      my $estimate_fd = Symbol::gensym;
      my $pid = open3($wtrfh, '>&STDOUT', $estimate_fd, @cmd);
      close($wtrfh);

      $size = parse_estimate($estimate_fd);
      close($estimate_fd);
      output_size($level, $size);
      unlink "$self->{gnulist}/${listdir}_${level}.new";
      waitpid $pid, 0;
   }
   exit 0;
}

sub parse_estimate {
   my($fh) = @_;
   my($size) = -1;
   while(<$fh>) {
      if ($_ =~ /^Total bytes written: (\d*)/) {
         $size = $1;
         last;
      }
   }
   return $size;
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

   my($listdir) = $self->{'host'} . $self->{'disk'};
   my($verbose) = "";
   $listdir     =~ s/\//_/g;
   my($level) = $self->{level}[0];
   if($level == 0) {
      open(GNULIST, ">$self->{gnulist}/${listdir}_${level}.new") || die();
      close(GNULIST) || die();
   }
   else {
      my($prev_level) = $level - 1;
      copy("$self->{gnulist}/${listdir}_${prev_level}", 
           "$self->{gnulist}/${listdir}_${level}.new");
   }

   my $mesgout_fd;
   open($mesgout_fd, '>&=3') || die();
   $self->{mesgout} = $mesgout_fd;

   if(defined($self->{index})) {
      $verbose = "--verbose";
   }
   my(@cmd) = ($self->{runtar}, $self->{config}, $self->{gnutar}, "--create", $verbose, "--directory", $self->{device}, "--listed-incremental", "$self->{gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "-", ".");

   debug("cmd:" . join(" ", @cmd));

   my $wtrfh;
   my $index_fd = Symbol::gensym;
   my $pid = open3($wtrfh, '>&STDOUT', $index_fd, @cmd) || die();
   close($wtrfh);

   if(defined($self->{index})) {
      my $indexout_fd;
      open($indexout_fd, '>&=4') || die();
      $self->parse_backup($index_fd, $mesgout_fd, $indexout_fd);
      close($indexout_fd);
   }
   else {
      $self->parse_backup($index_fd, $mesgout_fd, undef);
   }
   close($index_fd);

   if(defined($self->{record})) {
      debug("rename $self->{gnulist}/${listdir}_${level}.new $self->{gnulist}/${listdir}_${level}");
      rename "$self->{gnulist}/${listdir}_${level}.new", 
             "$self->{gnulist}/${listdir}_${level}";
   }
   else {
      debug("unlink $self->{gnulist}/${listdir}_${level}.new");
      unlink "$self->{gnulist}/${listdir}_${level}.new";
   }
   waitpid $pid, 0;
   if( $? != 0 ){
       print $mesgout_fd "? $self->{gnutar} returned error\n";
       die();
   }
   exit 0;
}

sub parse_backup {
   my $self = shift;
   my($fhin, $fhout, $indexout) = @_;
   my $size  = -1;
   my $ksize = -1;
   while(<$fhin>) {
      if ( /^\.\//) {
         if(defined($indexout)) {
	    if(defined($self->{index})) {
               s/^\.//;
               print $indexout $_;
	    }
         }
      }
      else {
            if (/^Total bytes written: (\d*)/) {
               $size = $1;
	       $ksize = int ($size / 1024);
            }
            elsif(defined($fhout)) {
	       if (/: Directory is new$/ ||
		   /: Directory has been renamed/) {
		  # ignore
	       } else { # strange
                  print $fhout "? $_";
	       }
            }
      }
   }
   if(defined($fhout)) {
      if ($size == -1) {
      }
      else {
         my($ksize) = int ($size/1024);
         print $fhout "sendbackup: size $ksize\n";
         print $fhout "sendbackup: end\n";
      }
   }
}

sub command_index_from_output {
   index_from_output(0, 1);
   exit 0;
}

sub index_from_output {
   my($fhin, $fhout) = @_;
   my($size) = -1;
   while(<$fhin>) {
      next if /^Total bytes written:/;
      next if !/^\.\//;
      s/^\.//;
      print $fhout $_;
   }
}

sub command_index_from_image {
   my $self = shift;
   my $index_fd;
   open($index_fd, "$self->{gnutar} --list --file - |") || die();
   index_from_output($index_fd, 1);
}

sub command_restore {
   my $self = shift;

   chdir(Amanda::Util::get_original_cwd());
   my(@cmd) = ($self->{gnutar}, "--numeric-owner", "-xpGvf", "-");
   for(my $i=1;defined $ARGV[$i]; $i++) {
      my $param = $ARGV[$i];
      $param =~ /^(.*)$/;
      push @cmd, $1;
   }
   debug("cmd:" . join(" ", @cmd));
   exec { $cmd[0] } @cmd;
   die("Can't exec '", $cmd[0], "'");
}

sub command_validate {
   my $self = shift;

   my(@cmd) = ($self->{gnutar}, "-tf", "-");
   debug("cmd:" . join(" ", @cmd));
   my $pid = open3('>&STDIN', '>&STDOUT', '>&STDERR', @cmd) || die("validate", "Unable to run @cmd");
   waitpid $pid, 0;
   if( $? != 0 ){
       die("validate", "$self->{gnutar} returned error");
   }
   exit(0);
}

sub command_print_command {
}

package main;

sub usage {
    print <<EOF;
Usage: amgtar_perl <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --calcsize.
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
my $opt_calcsize;

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'config=s'     => \$opt_config,
    'host=s'       => \$opt_host,
    'disk=s'       => \$opt_disk,
    'device=s'     => \$opt_device,
    'level=s'      => \@opt_level,
    'index=s'      => \$opt_index,
    'message=s'    => \$opt_message,
    'collection=s' => \$opt_collection,
    'record'       => \$opt_record,
    'calcsize'     => \$opt_calcsize,
) or usage();

my $application = Amanda::Application::amgtar_perl->new($opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $opt_calcsize);

$application->do($ARGV[0]);
