#!@PERL@ -T
#

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

open(DEBUG,">>@AMANDA_DBGDIR@/amstar_perl.$$.debug") if ($debug==1);
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
$runstar="${libexecdir}/runstar${suf}";
$gnulist = '@GNUTAR_LISTED_INCREMENTAL_DIR@';
$star = '@STAR@';



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
   my($size) = -1;
   my(@cmd) = ($runstar, $config, $star, "-c", "-f", "/dev/null", "-C", $device, "fs-name=$disk", "-xdev", "-link-dirs", "-level=$level", "-xattr", "-acl", "H=exustar", "errctl=WARN|SAMEFILE|DIFF|GROW|SHRINK|SPECIALFILE|GETXATTR|BADACL *", "-sparse", "-dodesc", ".");
   print DEBUG "cmd:" , join(" ", @cmd), "\n" if ($debug == 1);
   open3(\*WTRFH, '>&STDOUT', \*ESTIMATE, @cmd);

   $size = parse_estimate(ESTIMATE);
   close(ESTIMATE);
   output_size($size);
   exit 0;
}

sub parse_estimate {
   my($fh) = @_;
   my($size) = -1;
   while(<$fh>) {
      print DEBUG "READ 2: $_" if ($debug == 1);
      if ($_ =~ /star: (\d*) blocks/) {
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
      $size=32 if ($size<32);
      print DEBUG "STDOUT B: $size 1\n" if ($debug == 1);
      print "$size 1\n";
   }
}

sub command_backup {
   my($config, $host, $disk, $device, $level) = @_;
   my($verbose) = "";

   print DEBUG "config =" . $config . "\n" if ($debug == 1);
   print DEBUG "host   =" . $host   . "\n" if ($debug == 1);
   print DEBUG "disk   =" . $disk   . "\n" if ($debug == 1);
   print DEBUG "device =" . $device . "\n" if ($debug == 1);
   print DEBUG "level  =" . $level  . "\n" if ($debug == 1);

   if(defined($opt_index)) {
      $verbose = "--verbose";
   }
   if(defined($opt_record)) {
	$wtardumps = "-wtardumps";
   } else {
	$wtardumps = "";
   }
   my(@cmd) = ($runstar, $config, $star, "-c", "-v" , "-C", $device, "fs-name=$disk", "-xdev", "-link-dirs", "-level=$level", $wtardumps, "-xattr", "-acl", "H=exustar", "errctl=WARN|SAMEFILE|DIFF|GROW|SHRINK|SPECIALFILE|GETXATTR|BADACL *", "-sparse", "-dodesc", ".");

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
   }
   else {
   }
   exit 0;
}

sub parse_backup {
   my($fhin, $fhout, $indexout) = @_;
   my($ksize) = -1;
   while(<$fhin>) {
      print DEBUG "READ 3: $_" if ($debug == 1);
      if ( /^a \.\/ directory$/) {
	 print $indexout "/\n";
      } elsif ( /^a (.*) directory$/) {
	 print $indexout "/", "$1\n";
      } elsif ( /^a (.*) (\d*) bytes/) {
	 print $indexout "/", "$1\n";
      } elsif ( /^a (.*) special/) {
	 print $indexout "/", "$1\n";
      } elsif ( /^a (.*) symbolic/) {
	 print $indexout "/", "$1\n";
      } elsif (/star: (\d*) blocks/) {
         $ksize = $1 * 10;
      } elsif(defined($fhout)) {
	if (/^Type of this level/ ||
	    /^Date of this level/ ||
	    /^Date of last level/ ||
	    /^Dump record  level/) {
           print DEBUG "FHOUT 2: $_" if ($debug == 1);
           print $fhout "|", $_;
	} else {
           print $fount "?", $_;
	}
      }
   }
   if(defined($fhout)) {
      if ($ksize == -1) {
         print DEBUG "FHOUT 4: $command -1 -1\n" if ($debug == 1);
         print $fhout "$command -1 -1\n";
      }
      else {
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

   my(@cmd) = ($star, "-x", "-v", "-xattr", "-acl", "errctl=WARN|SAMEFILE|SETTIME|DIFF|SETACL|SETXATTR|SETMODE|BADACL *", "-f", "-");
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
