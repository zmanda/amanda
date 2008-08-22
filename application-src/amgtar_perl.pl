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
use Getopt::Long;

require $APPLICATION_DIR . "/generic-dumper";

Amanda::Util::setup_application("amgtar_perl", "client", $CONTEXT_DAEMON);

#initialize config client to get values from amanda-client.conf
config_init($CONFIG_INIT_CLIENT, undef);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_ANY);

my $suf = '';
if ( $Amanda::Constants::USE_VERSION_SUFFIXES =~ /^yes$/i ) {
        $suf="-$Amanda::Constants::VERSION";
}

debug("program: $0\n");

my $runtar="${Amanda::Paths::amlibexecdir}/runtar${suf}";
my $gnulist = $Amanda::Paths::GNUTAR_LISTED_INCREMENTAL_DIR;
my $gnutar = $Amanda::Constants::GNUTAR;

my $opt_config;
my $opt_host;
my $opt_disk;
my $opt_device;
my @opt_level;
my $opt_index;
my $opt_message;
my $opt_collection;
my $opt_record;

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
) or usage();

sub command_support {
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
}

sub command_selfcheck {
   print "OK $opt_disk\n";
   print "OK $opt_device\n";
   #check binary
   #check statefile
   #check amdevice
   #check property include/exclude
}

sub command_estimate {
   my($listdir) = "$opt_host$opt_disk";
   $listdir     =~ s/\//_/g;
   my $gnufile;
   my $level = $opt_level[0];
   if($level == 0) {
      open($gnufile, ">${gnulist}/${listdir}_${level}.new") || die();
      close($gnufile) || die();
   }
   else {
      my($prev_level) = $level - 1;
      if (-f "${gnulist}/${listdir}_${prev_level}") {
        copy("${gnulist}/${listdir}_${prev_level}", "${gnulist}/${listdir}_${level}.new");
      } else {
        open($gnufile, ">${gnulist}/${listdir}_${level}.new") || die();
        close($gnufile) || die();
	#print "ERROR file ${gnulist}/${listdir}_${level}.new doesn't exist\n";
      }
   }
   my($size) = -1;
   my(@cmd) = ($runtar, $opt_config, $gnutar, "--create", "--directory", $opt_device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "/dev/null", ".");
   debug("cmd:" . join(" ", @cmd));
   my $wtrfh;
   my $estimate_fd = Symbol::gensym;
   my $pid = open3($wtrfh, '>&STDOUT', $estimate_fd, @cmd);
   close($wtrfh);

   $size = parse_estimate($estimate_fd);
   close($estimate_fd);
   output_size($size);
   unlink "${gnulist}/${listdir}_${level}.new";
   waitpid $pid, 0;
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
   my($size) = @_;
   if($size == -1) {
      print "-1 -1\n";
      exit 2;
   }
   else {
      my($ksize) = int $size / (1024);
      $ksize=32 if ($ksize<32);
      print "$ksize 1\n";
   }
}

sub command_backup {
   my($listdir) = "$opt_host$opt_disk";
   my($verbose) = "";
   $listdir     =~ s/\//_/g;
   my($level) = $opt_level[0];
   if($level == 0) {
      open(GNULIST, ">${gnulist}/${listdir}_${level}.new") || die();
      close(GNULIST) || die();
   }
   else {
      my($prev_level) = $level - 1;
      copy("${gnulist}/${listdir}_${prev_level}", 
           "${gnulist}/${listdir}_${level}.new");
   }

   if(defined($opt_index)) {
      $verbose = "--verbose";
   }
   my(@cmd) = ($runtar, $opt_config, $gnutar, "--create", $verbose, "--directory", $opt_device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "-", ".");

   debug("cmd:" . join(" ", @cmd));

   my $wtrfh;
   my $index_fd = Symbol::gensym;
   my $pid = open3($wtrfh, '>&STDOUT', $index_fd, @cmd) || die();
   close($wtrfh);

   if(defined($opt_index)) {
      my $indexout_fd;
      open($indexout_fd, '>&=3') || die();
      parse_backup($index_fd, \*STDERR, $indexout_fd);
      close($indexout_fd);
   }
   else {
      parse_backup($index_fd, \*STDERR, undef);
   }
   close($index_fd);

   if(defined($opt_record)) {
      debug("rename ${gnulist}/${listdir}_${level}.new ${gnulist}/${listdir}_${level}");
      rename "${gnulist}/${listdir}_${level}.new", 
             "${gnulist}/${listdir}_${level}";
   }
   else {
      debug("unlink ${gnulist}/${listdir}_${level}.new");
      unlink "${gnulist}/${listdir}_${level}.new";
   }
   waitpid $pid, 0;
   exit 0;
}

sub parse_backup {
   my($fhin, $fhout, $indexout) = @_;
   my $size  = -1;
   my $ksize = -1;
   while(<$fhin>) {
      if ( /^\.\//) {
         if(defined($indexout)) {
	    if(defined($opt_index)) {
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
		   /: Directory has been renamed/) {) {
		  /* ignore */
	       } else { /* STRANGE */
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
   my $index_fd;
   open($index_fd, "$gnutar --list --file - |") || die();
   index_from_output($index_fd, 1);
}

sub command_restore {
   chdir(Amanda::Util::get_original_cwd());
   my(@cmd) = ($gnutar, "--numeric-owner", "-xpGvf", "-");
   for(my $i=1;defined $ARGV[$i]; $i++) {
      my $param = $ARGV[$i];
      $param =~ /^(.*)$/;
      push @cmd, $1;
   }
   debug("cmd:" . join(" ", @cmd));
   exec { $cmd[0] } @cmd;
   die("Can't exec '", $cmd[0], "'");
}

sub command_print_command {
}

sub usage {
    print <<EOF;
Usage: amgtar_perl <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no>.
EOF
    exit(1);
}

do_application($ARGV[0]);
