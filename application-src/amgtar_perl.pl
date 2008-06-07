#!@PERL@ -T
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

# Run perl.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
	& eval 'exec @PERL@ -S $0 $argv:q'
		if 0;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin";

$debug=1;
push(@INC, "@APPLICATION_DIR@");

use File::Copy;
use IPC::Open3;
use Sys::Hostname;

open(DEBUG,">>@AMANDA_DBGDIR@/amgtar_perl.$$.debug") if ($debug==1);
print DEBUG "program: $0\n" if ($debug==1);

$prefix='@prefix@';
$prefix = $prefix;
$exec_prefix="@exec_prefix@";
$exec_prefix=$exec_prefix;
$libexecdir="@libexecdir@";
$libexecdir=$libexecdir;
$application_dir = '@APPLICATION_DIR@';

$USE_VERSION_SUFFIXES='@USE_VERSION_SUFFIXES@';
$suf = '';
if ( $USE_VERSION_SUFFIXES =~ /^yes$/i ) {
   $suf='-@VERSION@';
}

$myhost = hostname;
$myhost =~ s/\..*$//;
$runtar="${libexecdir}/runtar${suf}";
$gnulist = '@GNUTAR_LISTED_INCREMENTAL_DIR@';
$gnutar = '@GNUTAR@';



$has_config   = 1;
$has_host     = 1;
$has_disk     = 1;
$max_level    = 9;
$index_line   = 1;
$index_xml    = 0;
$message_line = 1;
$message_xml  = 0;
$record       = 1;
$include_file = 1;
$include_list = 1;
$exclude_file = 1;
$exclude_list = 1;
$collection   = 0;

sub command_support {
   my($config, $host, $disk, $device, $level) = @_;
   print "CONFIG YES\n";
   print DEBUG "STDOUT: CONFIG YES\n" if ($debug == 1);
   print "HOST YES\n";
   print DEBUG "STDOUT: HOST YES\n" if ($debug == 1);
   print "DISK YES\n";
   print DEBUG "STDOUT: DISK YES\n" if ($debug == 1);
   print "MAX-LEVEL 9\n";
   print DEBUG "STDOUT: MAX-LEVEL 9\n" if ($debug == 1);
   print "INDEX-LINE YES\n";
   print DEBUG "STDOUT: INDEX-LINE YES\n" if ($debug == 1);
   print "INDEX-XML NO\n";
   print DEBUG "STDOUT: INDEX-XML NO\n" if ($debug == 1);
   print "MESSAGE-LINE YES\n";
   print DEBUG "STDOUT: MESSAGE-LINE YES\n" if ($debug == 1);
   print "MESSAGE-XML NO\n";
   print DEBUG "STDOUT: MESSAGE-XML NO\n" if ($debug == 1);
   print "RECORD YES\n";
   print DEBUG "STDOUT: RECORD YES\n" if ($debug == 1);
   print "INCLUDE-FILE YES\n";
   print "INCLUDE-LIST YES\n";
   print "EXCLUDE-FILE YES\n";
   print "EXCLUDE-LIST YES\n";
   print "COLLECTION NO\n";
}

sub command_selfcheck {
   my($config, $host, $disk, $device, $level) = @_;
   print DEBUG "STDOUT: OK $disk\n" if ($debug == 1);
   print DEBUG "STDOUT: OK $device\n" if ($debug == 1);
   print "OK $disk\n";
   print "OK $device\n";
   #check binary
   #check statefile
   #check amdevice
   #check property include/exclude
}

sub command_estimate {
   my($config, $host, $disk, $device, $level) = @_;
   my($listdir) = "$host$disk";
   $listdir     =~ s/\//_/g;
   if($level == 0) {
      open(GNULIST, ">${gnulist}/${listdir}_${level}.new") || die();
      close(GNULIST) || die();
   }
   else {
      my($prev_level) = $level - 1;
      if (-f "${gnulist}/${listdir}_${prev_level}") {
        copy("${gnulist}/${listdir}_${prev_level}", "${gnulist}/${listdir}_${level}.new");
      } else {
        open(GNULIST, ">${gnulist}/${listdir}_${level}.new") || die();
        close(GNULIST) || die();
	#print "ERROR file ${gnulist}/${listdir}_${level}.new doesn't exist\n";
      }
   }
   my($size) = -1;
   my(@cmd) = ($runtar, $config, $gnutar, "--create", "--directory", $device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "/dev/null", ".");
   print DEBUG "cmd:" , join(" ", @cmd), "\n" if ($debug == 1);
   open3(\*WTRFH, '>&STDOUT', \*ESTIMATE, @cmd);

   $size = parse_estimate(ESTIMATE);
   close(ESTIMATE);
   output_size($size);
   unlink "${gnulist}/${listdir}_${level}.new";
   exit 0;
}

sub parse_estimate {
   my($fh) = @_;
   my($size) = -1;
   while(<$fh>) {
      print DEBUG "READ 2: $_" if ($debug == 1);
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
      print DEBUG "STDOUT A: -1 -1\n" if ($debug == 1);
      print "-1 -1\n";
      exit 2;
   }
   else {
      my($ksize) = int $size / (1024);
      $ksize=32 if ($ksize<32);
      print DEBUG "STDOUT B: $ksize 1\n" if ($debug == 1);
      print "$ksize 1\n";
   }
}

sub command_backup {
   my($config, $host, $disk, $device, $level) = @_;
   my($listdir) = "$host$disk";
   my($verbose) = "";
   $listdir     =~ s/\//_/g;

   print DEBUG "config =" . $config . "\n" if ($debug == 1);
   print DEBUG "host   =" . $host   . "\n" if ($debug == 1);
   print DEBUG "disk   =" . $disk   . "\n" if ($debug == 1);
   print DEBUG "device =" . $device . "\n" if ($debug == 1);
   print DEBUG "level  =" . $level  . "\n" if ($debug == 1);

   if($level == 0) {
print DEBUG "command_backup: level == 0\n";
      open(GNULIST, ">${gnulist}/${listdir}_${level}.new") || die();
      close(GNULIST) || die();
   }
   else {
print DEBUG "command_backup: level != 0 : " . $level . "\n";
      my($prev_level) = $level - 1;
      copy("${gnulist}/${listdir}_${prev_level}", 
           "${gnulist}/${listdir}_${level}.new");
   }

   if(defined($opt_index)) {
      $verbose = "--verbose";
   }
   my(@cmd) = ($runtar, $config, $gnutar, "--create", $verbose, "--directory", $device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "-", ".");

   print DEBUG "cmd:" , join(" ", @cmd), "\n" if ($debug == 1);

   open3(\*WTRFH, '>&STDOUT', \*INDEX, @cmd) || die();

   if(defined($opt_index)) {
      open(INDEXOUT, '>&=3') || die();
      parse_backup(INDEX, STDERR, INDEXOUT);
      close(INDEXOUT);
   }
   else {
      parse_backup(INDEX, STDERR, undef);
   }
   close(INDEX);
   close(WTRFH);

   if(defined($opt_record)) {
print DEBUG "rename ${gnulist}/${listdir}_${level}.new ${gnulist}/${listdir}_${level}\n" if ($debug == 1);
      rename "${gnulist}/${listdir}_${level}.new", 
             "${gnulist}/${listdir}_${level}";
   }
   else {
print DEBUG "unlink ${gnulist}/${listdir}_${level}.new\n" if ($debug == 1);
      unlink "${gnulist}/${listdir}_${level}.new";
   }
   exit 0;
}

sub parse_backup {
   my($fhin, $fhout, $indexout) = @_;
   my($size) = -1;
   while(<$fhin>) {
      print DEBUG "READ 3: $_" if ($debug == 1);
      if ( /^\.\//) {
         if(defined($indexout)) {
	    if(defined($opt_index)) {
               s/^\.//;
               print DEBUG "INDEXOUT: $_" if ($debug == 1);
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
               next if /: Directory is new$/;
               print DEBUG "FHOUT 2: $_" if ($debug == 1);
               print $fhout $_;
            }
      }
   }
   if(defined($fhout)) {
      if ($size == -1) {
         print DEBUG "FHOUT 4: $command -1 -1\n" if ($debug == 1);
         print $fhout "$command -1 -1\n";
      }
      else {
         my($ksize) = int ($size/1024);
         print DEBUG "FHOUT 5: sendbackup: size $ksize\n" if ($debug == 1);
         print $fhout "sendbackup: size $ksize\n";
         print DEBUG "FHOUT 5: sendbackup: end\n" if ($debug == 1);
         print $fhout "sendbackup: end\n";
      }
   }
}

sub command_index_from_output {
   index_from_output(STDIN, STDOUT);
   exit 0;
}

sub index_from_output {
   my($fhin, $fhout) = @_;
   my($size) = -1;
   while(<$fhin>) {
      print DEBUG "READ 4: $_" if ($debug == 1);
      next if /^Total bytes written:/;
      next if !/^\.\//;
      s/^\.//;
      print DEBUG "FHOUT 6: $_" if ($debug == 1);
      print $fhout $_;
   }
}

sub command_index_from_image {
   my($config, $host, $disk, $device, $level) = @_;
   open(INDEX, "$gnutar --list --file - |") || die();
   index_from_output(INDEX, STDOUT);
}

sub command_restore {
   my($config, $host, $disk, $device, $level) = @_;
   my(@cmd) = ($gnutar, "--numeric-owner", "-xpGvf", "-");
   for($i=1;defined $ARGV[$i]; $i++) {
      $param = $ARGV[$i];
      $param =~ /^(.*)$/;
      push @cmd, $1;
   }
   print DEBUG "cmd:" , join(" ", @cmd), "\n" if ($debug == 1);
   exec { $cmd[0] } @cmd;
   die("Can't exec");
}

sub command_print_command {
}

require "$application_dir/generic-dumper"
