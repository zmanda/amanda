
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

print DEBUG "FHOUT 6: config    =" . $opt_config . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: disk      =" . $opt_disk   . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: host      =" . $opt_host   . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: device    =" . $opt_device . "\n" if ($debug == 1);
print DEBUG "FHOUT 6: level     =" . join(" ", @opt_level)  . "\n" if ($debug == 1);
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

if (defined @opt_level) {
  my @level = ();
  while (defined($level = shift(@opt_level))) {
    $level =~ /^(\d)$/;
    $level = $1;
    push @level, $level;
  }
  @opt_level = @level;
}

$command = $ARGV[0];

sub wrapper_pre_dle_amcheck();
sub wrapper_pre_host_amcheck();
sub wrapper_post_dle_amcheck();
sub wrapper_post_host_amcheck();
sub wrapper_pre_dle_estimate();
sub wrapper_pre_host_estimate();
sub wrapper_post_dle_estimate();
sub wrapper_post_host_estimate();
sub wrapper_pre_dle_backup();
sub wrapper_pre_host_backup();
sub wrapper_post_dle_backup();
sub wrapper_post_host_backup();

if ($command eq "PRE-DLE-AMCHECK") {
   wrapper_pre_dle_amcheck();
}
elsif ($command eq "PRE-HOST-AMCHECK") {
   wrapper_pre_host_amcheck();
}
elsif ($command eq "POST-DLE-AMCHECK") {
   wrapper_post_dle_amcheck();
}
elsif ($command eq "POST-HOST-AMCHECK") {
   wrapper_post_host_amcheck();
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
elsif ($command eq "PRE-RECOVER") {
   wrapper_pre_recover();
}
elsif ($command eq "POST-RECOVER") {
   wrapper_post_recover();
}
elsif ($command eq "PRE-LEVEL-RECOVER") {
   wrapper_pre_level_recover();
}
elsif ($command eq "POST-LEVEL-RECOVER") {
   wrapper_post_level_recover();
}
elsif ($command eq "INTER-LEVEL-RECOVER") {
   wrapper_inter_level_recover();
}
else {
   printf STDERR "Unknown command `$command'.\n";
   exit 1;
}


sub wrapper_pre_dle_amcheck() {
   if(defined(&command_pre_dle_amcheck)) {
      command_pre_dle_amcheck($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_host_amcheck() {
   if(defined(&command_pre_host_amcheck)) {
      command_pre_host_amcheck($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_dle_amcheck() {
   if(defined(&command_post_dle_amcheck)) {
      command_post_dle_amcheck($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_host_amcheck() {
   if(defined(&command_post_host_amcheck)) {
      command_post_host_amcheck($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_dle_estimate() {
   if(defined(&command_pre_dle_estimate)) {
      command_pre_dle_estimate($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_host_estimate() {
   if(defined(&command_pre_host_estimate)) {
      command_pre_host_estimate($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_dle_estimate() {
   if(defined(&command_post_dle_estimate)) {
      command_post_dle_estimate($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_host_estimate() {
   if(defined(&command_post_host_estimate)) {
      command_post_host_estimate($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_dle_backup() {
   if(defined(&command_pre_dle_backup)) {
      command_pre_dle_backup($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_host_backup() {
   if(defined(&command_pre_host_backup)) {
      command_pre_host_backup($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_dle_backup() {
   if(defined(&command_post_dle_backup)) {
      command_post_dle_backup($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_host_backup() {
   if(defined(&command_post_host_backup)) {
      command_post_host_backup($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_recover() {
   if(defined(&command_pre_recover)) {
print DEBUG "level: ", join(" ", @opt_level), "\n" if ($debug == 1);
      command_pre_recover($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_recover() {
   if(defined(&command_post_recover)) {
      command_post_recover($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_pre_level_recover() {
   if(defined(&command_pre_level_recover)) {
      command_pre_level_recover($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_post_level_recover() {
   if(defined(&command_post_level_recover)) {
      command_post_level_recover($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
   }
   else {
      exit 1;
   }
}

sub wrapper_inter_level_recover() {
   if(defined(&command_inter_level_recover)) {
      command_inter_level_recover($opt_config, $opt_host, $opt_disk, $opt_device, @opt_level);
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
