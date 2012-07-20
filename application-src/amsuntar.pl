#!@PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Application::Amsuntar;
use base qw(Amanda::Application);
use File::Copy;
use File::Temp qw( tempfile );
use File::Path;
use IPC::Open2;
use IPC::Open3;
use Sys::Hostname;
use Symbol;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants quote_string );

sub new {
    my $class = shift;
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $exclude_list, $exclude_optional,  $include_list, $include_optional, $bsize, $ext_attrib, $ext_header, $ignore, $normal, $strange, $error_exp, $directory, $suntar_path) = @_;
    my $self = $class->SUPER::new($config);

    $self->{suntar}            = $Amanda::Constants::SUNTAR;
    if (defined $suntar_path) {
	$self->{suntar}        = $suntar_path;
    }
    $self->{pfexec}            = "/usr/bin/pfexec";
    $self->{gnutar}            = $Amanda::Constants::GNUTAR;
    $self->{teecount}          = $Amanda::Paths::amlibexecdir."/teecount";

    $self->{config}            = $config;
    $self->{host}              = $host;
    if (defined $disk) {
	$self->{disk}          = $disk;
    } else {
	$self->{disk}          = $device;
    }
    if (defined $device) {
	$self->{device}        = $device;
    } else {
	$self->{device}        = $disk;
    }
    $self->{level}             = $level;
    $self->{index}             = $index;
    $self->{message}           = $message;
    $self->{collection}        = $collection;
    $self->{record}            = $record;
    $self->{exclude_list}      = [ @{$exclude_list} ];
    $self->{exclude_optional}  = $exclude_optional;
    $self->{include_list}      = [ @{$include_list} ];
    $self->{include_optional}  = $include_optional;
    $self->{block_size}        = $bsize;
    $self->{extended_header}   = $ext_header;
    $self->{extended_attrib}   = $ext_attrib; 
    $self->{directory}         = $directory;

    $self->{regex} = ();
    my $regex;
    for $regex (@{$ignore}) {
	my $a = { regex => $regex, type => "IGNORE" };
	push @{$self->{regex}}, $a;
    }

    for $regex (@{$normal}) {
	my $a = { regex => $regex, type => "NORMAL" };
	push @{$self->{regex}}, $a;
    }

    for $regex (@{$strange}) {
	my $a = { regex => $regex, type => "STRANGE" };
	push @{$self->{regex}}, $a;
    }

    for $regex (@{$error_exp}) {
	my $a = { regex => $regex, type => "ERROR" };
	push @{$self->{regex}}, $a;
    }

    #type can be IGNORE/NORMAL/STRANGE/ERROR
    push @{$self->{regex}}, { regex => "is not a file. Not dumped\$",
			      type  => "NORMAL" };
    push @{$self->{regex}}, { regex => "same as archive file\$",
			      type  => "NORMAL" };
    push @{$self->{regex}}, { regex => ": invalid character in UTF-8 conversion of ",
			      type  => "STRANGE" };
    push @{$self->{regex}}, { regex => ": UTF-8 conversion failed.\$",
			      type  => "STRANGE" };
    push @{$self->{regex}}, { regex => ": Permission denied\$",
			      type  => "ERROR" };

    for $regex (@{$self->{regex}}) {
	debug ($regex->{type} . ": " . $regex->{regex});
    }

    return $self;
}

sub command_support {
   my $self = shift;

   print "CONFIG YES\n";
   print "HOST YES\n";
   print "DISK YES\n";
   print "MAX-LEVEL 0\n";
   print "INDEX-LINE YES\n";
   print "INDEX-XML NO\n";
   print "MESSAGE-LINE YES\n";
   print "MESSAGE-XML NO\n";
   print "RECORD YES\n";
   print "EXCLUDE-FILE NO\n";
   print "EXCLUDE-LIST YES\n";
   print "EXCLUDE-OPTIONAL YES\n";
   print "INCLUDE-FILE NO\n";
   print "INCLUDE-LIST YES\n";
   print "INCLUDE-OPTIONAL YES\n";
   print "COLLECTION NO\n";
   print "MULTI-ESTIMATE NO\n";
   print "CALCSIZE NO\n";
   print "CLIENT-ESTIMATE YES\n";
}

sub command_selfcheck {
   my $self = shift;

   $self->print_to_server("disk " . quote_string($self->{disk}));

   $self->print_to_server("amsuntar version " . $Amanda::Constants::VERSION,
			  $Amanda::Script_App::GOOD);

   if (!-e $self->{suntar}) {
      $self->print_to_server_and_die(
		       "application binary $self->{suntar} doesn't exist",
                       $Amanda::Script_App::ERROR);
   }
   if (!-x $self->{suntar}) {
      $self->print_to_server_and_die(
                       "application binary $self->{suntar} is not a executable",
                       $Amanda::Script_App::ERROR);
   }
   if (!defined $self->{disk} || !defined $self->{device}) {
      return;
   }
   print "OK " . $self->{device} . "\n";
   print "OK " . $self->{directory} . "\n" if defined $self->{directory};
   $self->validate_inexclude();
}

sub command_estimate() {
    my $self = shift;
    my $size = "-1";
    my $level = $self->{level};

    $self->{index} = undef;	#remove verbose flag to suntar.
    my(@cmd) = $self->build_command();
    my(@cmdwc) = ("/usr/bin/wc", "-c");

    debug("cmd:" . join(" ", @cmd) . " | " . join(" ", @cmdwc));
    my($wtr, $rdr, $err, $pid, $rdrwc, $pidwc);
    $err = Symbol::gensym;
    $pid = open3($wtr, \*DATA, $err, @cmd);
    $pidwc = open2($rdrwc, '>&DATA', @cmdwc);
    close $wtr;

    my ($msgsize) = <$rdrwc>;
    my $errmsg;
    my $result;
    while (<$err>) {
	my $matched = 0;
	for my $regex (@{$self->{regex}}) {
	    my $regex1 = $regex->{regex};
	    if (/$regex1/) {
		$result = 1 if ($regex->{type} eq "ERROR");
		$matched = 1;
		last;
	    }
	}
	$result = 1 if ($matched == 0);
	$errmsg = $_ if (!defined $errmsg);
    }
    waitpid $pid, 0;
    close $rdrwc;
    close $err;
    if ($result ==  1) {
        if (defined $errmsg) {
            $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
        } else {
                $self->print_to_server_and_die(
			"cannot estimate archive size': unknown reason",
			$Amanda::Script_App::ERROR);
        }
    }
    output_size($level, $msgsize);
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

   $self->validate_inexclude();

   my(@cmd) = $self->build_command();
   my(@cmdtc) = $self->{teecount};

   debug("cmd:" . join(" ", @cmd) . " | " . join(" ", @cmdtc));

   my($wtr, $pid, $rdrtc, $errtc, $pidtc);
   my $index_fd = Symbol::gensym;
   $errtc = Symbol::gensym;

   $pid = open3($wtr, \*DATA, $index_fd, @cmd) ||
      $self->print_to_server_and_die("Can't run $cmd[0]: $!",
				     $Amanda::Script_App::ERROR);
   $pidtc = open3('<&DATA', '>&STDOUT', $errtc, @cmdtc) ||
      $self->print_to_server_and_die("Can't run $cmdtc[0]: $!",
				     $Amanda::Script_App::ERROR);
   close($wtr);

   unlink($self->{include_tmp}) if(-e $self->{include_tmp});
   unlink($self->{exclude_tmp}) if(-e $self->{exclude_tmp});

   my $result;
   if(defined($self->{index})) {
      my $indexout_fd;
      open($indexout_fd, '>&=4') ||
      $self->print_to_server_and_die("Can't open indexout_fd: $!",
				     $Amanda::Script_App::ERROR);
      $result = $self->parse_backup($index_fd, $self->{mesgout}, $indexout_fd);
      close($indexout_fd);
   }
   else {
      $result = $self->parse_backup($index_fd, $self->{mesgout}, undef);
   }
   close($index_fd);
   my $size = <$errtc>;

   waitpid $pid, 0;

   my $status = $?;
   if( $status != 0 ){
       debug("exit status $status ?" );
   }

   if ($result == 1) {
       debug("$self->{suntar} returned error" );
       $self->print_to_server("$self->{suntar} returned error", 
			      $Amanda::Script_App::ERROR);
   }

   my($ksize) = int ($size/1024);
   print {$self->{mesgout}} "sendbackup: size $ksize\n";
   print {$self->{mesgout}} "sendbackup: end\n";
   debug("sendbackup: size $ksize "); 

   exit 0;
}

sub parse_backup {
   my $self = shift;
   my($fhin, $fhout, $indexout) = @_;
   my $size  = -1;
   my $result = 0;
   while(<$fhin>) {
      if ( /^ ?a\s+(\.\/.*) \d*K/ ||
	   /^a\s+(\.\/.*) symbolic link to/ ||
	   /^a\s+(\.\/.*) link to/ ) {
	 my $name = $1;
         if(defined($indexout)) {
	    if(defined($self->{index})) {
               $name =~ s/^\.//;
               print $indexout $name, "\n";
	    }
         }
      }
      else {
	 my $matched = 0;
	 for my $regex (@{$self->{regex}}) {
	    my $regex1 = $regex->{regex};
	    if (/$regex1/) {
	       $result = 1 if ($regex->{type} eq "ERROR");
	       if (defined($fhout)) {
	          if ($regex->{type} eq "IGNORE") {
	          } elsif ($regex->{type} eq "NORMAL") {
		     print $fhout "| $_";
	          } elsif ($regex->{type} eq "STRANGE") {
		     print $fhout "? $_";
	          } else {
		     print $fhout "? $_";
	          }
	       }
	       $matched = 1;
	       last;
	    }
	 }
	 if ($matched == 0) {
	    $result = 1;
	    if (defined($fhout)) {
               print $fhout "? $_";
	    }
	 }
      }
   }
   return $result;
}

sub validate_inexclude {
   my $self = shift;
   my $fh;
   my @tmp;

   if ($#{$self->{exclude_list}} >= 0 && $#{$self->{include_list}} >= 0 )  {
      $self->print_to_server_and_die("Can't have both include and exclude",
                                     $Amanda::Script_App::ERROR);
   }
    
   foreach my $file (@{$self->{exclude_list}}){
      if (!open($fh, $file)) {
          if ($self->{action} eq "check" && !$self->{exclude_optional}) {
                $self->print_to_server("Open of '$file' failed: $!",
                                       $Amanda::Script_App::ERROR);
          }
          next;
      }
      while (<$fh>) {
          push @tmp, $_;
      }
      close($fh);
   }

   #Merging list into a single file 
   if($self->{action} eq 'backup' && $#{$self->{exculde_list}} >= 0) {
      ($fh, $self->{exclude_tmp}) = tempfile(DIR => $Amanda::paths::AMANDA_TMPDIR);
      unless($fh) {
                $self->print_to_server_and_die(
                          "Open of tmp file '$self->{exclude_tmp}' failed: $!",
                          $Amanda::Script_App::ERROR);
      }
      print $fh @tmp;	
      close $fh;
      undef (@tmp);
   }

   foreach my $file (@{$self->{include_list}}) {
      if (!open($fh, $file)) {
         if ($self->{action} eq "check" && !$self->{include_optional}) {
                $self->print_to_server("Open of '$file' failed: $!",
                                       $Amanda::Script_App::ERROR);
         }
         next;
      }
      while (<$fh>) {
         push @tmp, $_;
      }
      close($fh);
   }

   if($self->{action} eq 'backup' && $#{$self->{include_list}} >= 0) {
      ($fh, $self->{include_tmp}) = tempfile(DIR => $Amanda::paths::AMANDA_TMPDIR);
      unless($fh) {
                $self->print_to_server_and_die(
                          "Open of tmp file '$self->{include_tmp}' failed: $!",
                          $Amanda::Script_App::ERROR);
      }
      print $fh @tmp;
      close $fh;
      undef (@tmp);
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
   open($index_fd, "$self->{suntar} -tf - |") ||
      $self->print_to_server_and_die("Can't run $self->{suntar}: $!",
				     $Amanda::Script_App::ERROR);
   index_from_output($index_fd, 1);
}

sub command_restore {
   my $self = shift;

   chdir(Amanda::Util::get_original_cwd());
   if (defined $self->{directory}) {
      if (!-d $self->{directory}) {
         $self->print_to_server_and_die("Directory $self->{directory}: $!",
				        $Amanda::Script_App::ERROR);
      }
      if (!-w $self->{directory}) {
         $self->print_to_server_and_die("Directory $self->{directory}: $!",
				        $Amanda::Script_App::ERROR);
      }
      chdir($self->{directory});
   }

   my $cmd = "-xpv";

   if($self->{extended_header} eq "YES") {
      $cmd .= "E";
   }
   if($self->{extended_attrib} eq "YES") {
      $cmd .= "\@";
   }

   $cmd .= "f";      

   if (defined($self->{exclude_list}) && (-e $self->{exclude_list}[0])) {
      $cmd .= "X";
   }

   my(@cmd) = ($self->{pfexec},$self->{suntar}, $cmd);

   push @cmd, "-";  # for f argument
   if (defined($self->{exclude_list}) && (-e $self->{exclude_list}[0])) {
      push @cmd, $self->{exclude_list}[0]; # for X argument
   }

   if(defined($self->{include_list}) && (-e $self->{include_list}[0]))  {
      push @cmd, "-I", $self->{include_list}[0];
   }

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
   my @cmd;
   my $program;

   if (-e $self->{suntar}) {
      $program = $self->{suntar};
   } elsif (-e $self->{gnutar}) {
      $program = $self->{gnutar};
   } else {
      return $self->default_validate();
   }
   @cmd = ($program, "-tf", "-");
   debug("cmd:" . join(" ", @cmd));
   my $pid = open3('>&STDIN', '>&STDOUT', '>&STDERR', @cmd) ||
      $self->print_to_server_and_die("Unable to run @cmd",
				     $Amanda::Script_App::ERROR);
   waitpid $pid, 0;
   if( $? != 0 ){
	$self->print_to_server_and_die("$program returned error",
				       $Amanda::Script_App::ERROR);
   }
   exit(0);
}

sub build_command {
  my $self = shift;

   #Careful sun tar options and ordering is very very tricky

   my($cmd) = "-cp";
   my(@optparams) = ();

   $self->validate_inexclude();

   if($self->{extended_header} =~ /^YES$/i) {
      $cmd .= "E";
   }
   if($self->{extended_attrib} =~ /^YES$/i) {
      $cmd .= "\@";
   }
   if(defined($self->{index})) {
      $cmd .= "v";
   }

   if(defined($self->{block_size})) {
      $cmd .= "b";
      push @optparams, $self->{block_size};
   }

   if (defined($self->{exclude_tmp})) {
      $cmd .= "fX";
      push @optparams,"-",$self->{exclude_tmp};
   } else {
      $cmd .= "f";
      push @optparams,"-";
   }
   if ($self->{directory}) {
      push @optparams, "-C", $self->{directory};
   } else {
      push @optparams, "-C", $self->{device};
   }

   if(defined($self->{include_tmp}))  {
      push @optparams,"-I", $self->{include_tmp};
   } else {
      push @optparams,".";
   }

   my(@cmd) = ($self->{pfexec}, $self->{suntar}, $cmd, @optparams);
   return (@cmd);
}

package main;

sub usage {
    print <<EOF;
Usage: Amsuntar <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --exclude-list=<fileList> --include-list=<fileList> --block-size=<size> --extended_attributes=<yes|no> --extended_headers<yes|no> --ignore=<regex> --normal=<regex> --strange=<regex> --error=<regex> --lang=<lang>.
EOF
    exit(1);
}

my $opt_config;
my $opt_host;
my $opt_disk;
my $opt_device;
my $opt_level;
my $opt_index;
my $opt_message;
my $opt_collection;
my $opt_record;
my @opt_exclude_list;
my $opt_exclude_optional;
my @opt_include_list;
my $opt_include_optional;
my $opt_bsize = 256;
my $opt_ext_attrib = "YES";
my $opt_ext_head   = "YES";
my @opt_ignore;
my @opt_normal;
my @opt_strange;
my @opt_error;
my $opt_lang;
my $opt_directory;
my $opt_suntar_path;

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'config=s'     	  => \$opt_config,
    'host=s'       	  => \$opt_host,
    'disk=s'       	  => \$opt_disk,
    'device=s'     	  => \$opt_device,
    'level=s'      	  => \$opt_level,
    'index=s'      	  => \$opt_index,
    'message=s'    	  => \$opt_message,
    'collection=s' 	  => \$opt_collection,
    'exclude-list=s'      => \@opt_exclude_list,
    'exclude-optional=s'  => \$opt_exclude_optional,
    'include-list=s'      => \@opt_include_list,
    'include-optional=s'  => \$opt_include_optional,
    'record'       	  => \$opt_record,
    'block-size=s'        => \$opt_bsize,
    'extended-attributes=s'  => \$opt_ext_attrib,
    'extended-headers=s'     => \$opt_ext_head,
    'ignore=s'               => \@opt_ignore,
    'normal=s'               => \@opt_normal,
    'strange=s'              => \@opt_strange,
    'error=s'                => \@opt_error,
    'lang=s'                 => \$opt_lang,
    'directory=s'            => \$opt_directory,
    'suntar-path=s'          => \$opt_suntar_path,
) or usage();

if (defined $opt_lang) {
    $ENV{LANG} = $opt_lang;
}

my $application = Amanda::Application::Amsuntar->new($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, \@opt_exclude_list, $opt_exclude_optional, \@opt_include_list, $opt_include_optional,$opt_bsize,$opt_ext_attrib,$opt_ext_head, \@opt_ignore, \@opt_normal, \@opt_strange, \@opt_error, $opt_directory, $opt_suntar_path);

$application->do($ARGV[0]);
