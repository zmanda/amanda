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

require "newgetopt.pl";
use Text::ParseWords;

print DEBUG "FHOUT 6: ARGV[ 0]=" . $ARGV[0] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 1]=" . $ARGV[1] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 2]=" . $ARGV[2] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 3]=" . $ARGV[3] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 4]=" . $ARGV[4] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 5]=" . $ARGV[5] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 6]=" . $ARGV[6] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 7]=" . $ARGV[7] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 8]=" . $ARGV[8] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[ 9]=" . $ARGV[9] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[10]=" . $ARGV[10] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[11]=" . $ARGV[11] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[12]=" . $ARGV[12] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[13]=" . $ARGV[13] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[14]=" . $ARGV[14] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[15]=" . $ARGV[15] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[16]=" . $ARGV[16] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[17]=" . $ARGV[17] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[18]=" . $ARGV[18] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: ARGV[19]=" . $ARGV[19] . "\n" if ($debug == 1);

$result = &NGetOpt ("config=s", "host=s", "disk=s", "device=s", "level=s", "index=s", "message=s", "collection", "record");
$result = $result;

print DEBUG "FHOUT 6: config    =" . $opt_config . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: disk      =" . $opt_disk   . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: host      =" . $opt_host   . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: device    =" . $opt_device . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: level     =" . $opt_level  . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: index     =" . $opt_index  . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: message   =" . $opt_message. "\n" if ($debug == 1);
print DEBUG "FHOUT 6: collection=" . $opt_collection. "\n" if ($debug == 1);
print DEBUG "FHOUT 6: record    =" . $opt_record . "\n" if ($debug == 1);

print DEBUG "FHOUT 6: A-ARGV[0]=" . $ARGV[0] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: A-ARGV[1]=" . $ARGV[1] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: A-ARGV[2]=" . $ARGV[2] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: A-ARGV[3]=" . $ARGV[3] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: A-ARGV[4]=" . $ARGV[4] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: A-ARGV[5]=" . $ARGV[5] . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: A-ARGV[6]=" . $ARGV[6] . "\n" if ($debug == 1);

if (defined $opt_config) {
  $opt_config =~ /^([\_\.A-Za-z0-9]*)$/;
  $opt_config = $1;
}

if (defined $opt_host) {
  $opt_host =~ /^([\_\.A-Za-z0-9]*)$/;
  $opt_host = $1;
}

if (defined $opt_device) {
  $opt_device =~ /^([\/\_\:\.A-Za-z0-9]*)$/;
  $opt_device = $1;
}

if (defined $opt_disk) {
  $opt_disk =~ /^([\/\_\:\.A-Za-z0-9]*)$/;
  $opt_disk = $1;
} else {
  $opt_disk = $opt_device;
}

if (defined $opt_level) {
  $opt_level =~ /^(\d)$/;
  $opt_level = $1;
}

# Read tool property

$command = $ARGV[0];

%property = ();
if ($command ne "restore") {
  while($property_line = <STDIN>) {
    chomp $property_line;
    @prop_value = shellwords($property_line);
    $prop_name = shift @prop_value;
    push @{$property{$prop_name}}, @prop_value;
  }

  if ($debug == 1) {
    foreach $prop_name (keys(%property)) {
      print DEBUG "PROPERTY: $prop_name\n";
      print DEBUG "    VALUE: ", join(',',@{$property{$prop_name}}) , "\n";
    }
  }
}

sub wrapper_support();
sub wrapper_selfcheck();
sub wrapper_estimate();
sub wrapper_backup();
sub wrapper_restore();

if ($command eq "support") {
   wrapper_support();
}
elsif ($command eq "selfcheck") {
   wrapper_selfcheck();
}
elsif ($command eq "estimate") {
   wrapper_estimate();
}
elsif ($command eq "backup") {
   wrapper_backup();
}
elsif ($command eq "restore") {
   wrapper_restore();
}
else {
   printf STDERR "Unknown command `$command'.\n";
   exit 1;
}


sub wrapper_support() {
   if(defined(&command_support)) {
      command_support($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
      exit 0;
   }
   print "CONFIG YES\n"       if defined($has_config)   && $has_config   == 1;
   print "HOST YES\n"         if defined($has_host)     && $has_host     == 1;
   print "DISK YES\n"         if defined($has_disk)     && $has_disk     == 1;
   print "LEVEL 0-", $max_level , "\n" if defined($max_level);
   print "INDEX-LINE YES\n"   if defined($index_line)   && $index_line   == 1;
   print "INDEX-XML NO\n"     if defined($index_xml)    && $index_xml    == 1;
   print "MESSAGE-LINE YES\n" if defined($message_line) && $message_line == 1;
   print "MESSAGE-XML NO\n"   if defined($message_xml)  && $message_xml  == 1;
   print "RECORD YES\n"       if defined($record)       && $record       == 1;
   print "INCLUDE-FILE NO\n"  if defined($include_file) && $include_file == 1;
   print "INCLUDE-LIST NO\n"  if defined($include_list) && $include_list == 1;
   print "EXCLUDE-FILE NO\n"  if defined($exclude_file) && $exclude_file == 1;
   print "EXCLUDE-LIST NO\n"  if defined($exclude_list) && $exclude_list == 1;
   print "COLLECTION NO\n"    if defined($collection)   && $collection   == 1;
   exit 1;
}

sub wrapper_selfcheck() {
   if(defined(&command_selfcheck)) {
      command_selfcheck($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_estimate() {
   if(defined(&command_estimate)) {
      command_estimate($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   } else {
      exit 1;
   }
}

sub wrapper_estimate_parse() {
   if(defined(&command_estimate_parse)) {
      command_estimate_parse($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      printf STDERR "`estimate-parse' is not supported.\n";
      exit 1;
   }
}

sub wrapper_backup() {
   if(defined(&command_backup)) {
      command_backup($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   } else {
print DEBUG "wrapper_backup: !defined(command_backup)\n" if ($debug == 1);
      exit 1;
   }
}

sub wrapper_backup_parse() {
   if(defined(&command_backup_parse)) {
      command_backup_parse($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      printf STDERR "`backup-parse' is not supported.\n";
      exit 1;
   }
}

sub wrapper_index_from_output() {
   if(defined(&command_index_from_output)) {
      command_index_from_output($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      printf STDERR "`index-from-output' is not supported.\n";
      exit 1;
   }
}

sub wrapper_index_from_image() {
   if(defined(&command_index_from_image)) {
      command_index_from_image($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      printf STDERR "command `index-from-image' is not supported.\n";
      exit 1;
   }
}

sub wrapper_restore() {
   if(defined(&command_restore)) {
      command_restore($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
     printf STDERR "`restore' is not supported.\n";
     exit 1;
   }
}

sub parse_options() {
   my($no_option) = @_;
   my($options, @options, $option, $name, $option_name, $value);

   while($no_option <= $#ARGV) {
      $options = $ARGV[${no_option}];
      @options = split (/;/,$options);
      foreach $option (@options) {
         if( $option =~ /=/ ) {
            ($name,$value) = split(/=/,$option);
         }
         else {
            $name  = $option;
            $value = 1;
         }
         $option_name = "option_$name";
         $option_name =~ s/\-/\_/g;
	 $$option_name = $value;
      }
      $no_option++;
   }
}

sub check_file {
   my($filename, $mode) = @_;

   stat($filename);

   if($mode eq "e") {
      if( -e _ ) {
         print "OK $filename exists\n";
      }
      else {
         print "ERROR [can not find $filename]\n";
      }
   }
   elsif($mode eq "x") {
      if( -x _ ) {
         print "OK $filename executable\n";
      }
      else {
         print "ERROR [can not execute $filename]\n";
      }
   }
   elsif($mode eq "r") {
      if( -r _ ) {
         print "OK $filename readable\n";
      }
      else {
         print "ERROR [can not read $filename]\n";
      }
   }
   elsif($mode eq "w") {
      if( -w _ ) {
         print "OK $filename writable\n";
      }
      else {
         print "ERROR [can not write $filename]\n";
      }
   }
   else {
      print "ERROR [check_file: unknow mode $mode]\n";
   }
}

sub check_dir {
}

sub check_suid {
}

1;
