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

sub wrapper_pre_dle_selfcheck();
sub wrapper_pre_host_selfcheck();
sub wrapper_post_dle_selfcheck();
sub wrapper_post_host_selfcheck();
sub wrapper_pre_dle_estimate();
sub wrapper_pre_host_estimate();
sub wrapper_post_dle_estimate();
sub wrapper_post_host_estimate();
sub wrapper_pre_dle_backup();
sub wrapper_pre_host_backup();
sub wrapper_post_dle_backup();
sub wrapper_post_host_backup();

if ($command eq "PRE-DLE-SELFCHECK") {
   wrapper_pre_dle_selfcheck();
}
elsif ($command eq "PRE-HOST-SELFCHECK") {
   wrapper_pre_host_selfcheck();
}
elsif ($command eq "POST-DLE-SELFCHECK") {
   wrapper_post_dle_selfcheck();
}
elsif ($command eq "POST-HOST-SELFCHECK") {
   wrapper_post_host_selfcheck();
}
elsif ($command eq "PRE-DLE-ESTIMATE") {
   wrapper_pre_dle_estimate();
}
elsif ($command eq "PRE-HOST-ESTIMATE") {
   wrapper_pre_host_estimate();
}
elsif ($command eq "POST-DLE-ESTIMATE") {
   wrapper_post_dle_estimate();
}
elsif ($command eq "POST-HOST-ESTIMATE") {
   wrapper_post_host_estimate();
}
elsif ($command eq "PRE-DLE-BACKUP") {
   wrapper_pre_dle_backup();
}
elsif ($command eq "PRE-HOST-BACKUP") {
   wrapper_pre_host_backup();
}
elsif ($command eq "POST-DLE-BACKUP") {
   wrapper_post_dle_backup();
}
elsif ($command eq "POST-HOST-BACKUP") {
   wrapper_post_host_backup();
}
else {
   printf STDERR "Unknown command `$command'.\n";
   exit 1;
}


sub wrapper_pre_dle_selfcheck() {
   if(defined(&command_pre_dle_selfcheck)) {
      command_pre_dle_selfcheck($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_host_selfcheck() {
   if(defined(&command_pre_host_selfcheck)) {
      command_pre_host_selfcheck($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_dle_selfcheck() {
   if(defined(&command_post_dle_selfcheck)) {
      command_post_dle_selfcheck($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_host_selfcheck() {
   if(defined(&command_post_host_selfcheck)) {
      command_post_host_selfcheck($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_dle_estimate() {
   if(defined(&command_pre_dle_estimate)) {
      command_pre_dle_estimate($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_host_estimate() {
   if(defined(&command_pre_host_estimate)) {
      command_pre_host_estimate($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_dle_estimate() {
   if(defined(&command_post_dle_estimate)) {
      command_post_dle_estimate($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_host_estimate() {
   if(defined(&command_post_host_estimate)) {
      command_post_host_estimate($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_dle_backup() {
   if(defined(&command_pre_dle_backup)) {
      command_pre_dle_backup($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_host_backup() {
   if(defined(&command_pre_host_backup)) {
      command_pre_host_backup($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_dle_backup() {
   if(defined(&command_post_dle_backup)) {
      command_post_dle_backup($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_host_backup() {
   if(defined(&command_post_host_backup)) {
      command_post_host_backup($opt_config, $opt_host, $opt_disk, $opt_device, $opt_level);
   }
   else {
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
