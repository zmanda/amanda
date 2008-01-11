#!@PERL@ -T
#

# Run perl.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
	& eval 'exec @PERL@ -S $0 $argv:q'
		if 0;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin";

$debug=1;
push(@INC, ".", "@DUMPER_DIR@");

use File::Copy;
use IPC::Open3;
use Sys::Hostname;


open(DEBUG,">>@AMANDA_DBGDIR@/amgtar.$$.debug") if ($debug==1);

$prefix='@prefix@';
$prefix = $prefix;
$exec_prefix="@exec_prefix@";
$exec_prefix=$exec_prefix;
$amlibexecdir="@amlibexecdir@";
$amlibexecdir=$amlibexecdir;

$USE_VERSION_SUFFIXES='@USE_VERSION_SUFFIXES@';
$suf = '';
if ( $USE_VERSION_SUFFIXES =~ /^yes$/i ) {
   $suf='-@VERSION@';
}

$myhost = hostname;
$myhost =~ s/\..*$//;
$runtar="${amlibexecdir}/runtar${suf}";
$gnulist = '@GNUTAR_LISTED_INCREMENTAL_DIR@';
$gnutar = '@GNUTAR@';



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

#$user_support  = "";
#$group_support = "";

#$user_selfcheck  = "";
#$group_selfcheck = "";

#$user_estimate  = "";
#$group_estimate = "";

#$user_estimate_parse  = "";
#$group_estimate_parse = "";

$user_backup  = "root";
#$group_backup = "";

#$user_backup_parse  = "";
#$group_backup_parse = "";

#$user_index_from_output  = "";
#$group_index_from_output = "";

#$user_index_from_image  = "";
#$group_index_from_image = "";

#$user_restore  = "";
#$group_restore = "";

#$user_print_command  = "";
#$group_print_command = "";

$user_default  = "amanda";
$group_default = "amanda";


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
   command_estimate_opt_direct($config, $host, $disk, $device, $level, $listdir);
}


sub command_estimate_opt_direct {
   my($config, $host, $disk, $device, $level, $listdir) = @_;
   my($size) = -1;
   my(@cmd) = ($runtar, $config, $gnutar, "--create", "--directory", $device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "/dev/null", ".");
   #my(@cmd) = ($gnutar, "--create", "--directory", $device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "/dev/null", ".");
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
   my(@cmd) = ($runtar, $config, $gnutar, "--create", $verbose, "--directory", $device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "-", ".");
   #my(@cmd) = ($gnutar, "--create", $verbose, "--directory", $device, "--listed-incremental", "${gnulist}/${listdir}_${level}.new", "--sparse", "--one-file-system", "--ignore-failed-read", "--totals", "--file", "-", ".");

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
      rename "${gnulist}/${listdir}_${level}.new", 
             "${gnulist}/${listdir}_${level}";
   }
   else {
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

#   $ARGV[0] = undef;   
   my(@cmd) = ($gnutar, "--numeric-owner", "-xpGvf", "-");
   for($i=1;defined $ARGV[$i]; $i++) {
      push @cmd, $ARGV[$i];
   }
   print DEBUG "cmd:" , join(" ", @cmd), "\n" if ($debug == 1);
   exec @cmd;
}

sub command_print_command {
}

require "generic-dumper"
