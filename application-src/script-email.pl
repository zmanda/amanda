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

open(DEBUG,">>@AMANDA_DBGDIR@/script-email.$$.debug") if ($debug==1);
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
$mailer = '@MAILER@';


$has_config   = 1;
$has_host     = 1;
$has_disk     = 1;

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

sub command_pre_dle_selfcheck {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("pre-dle-selfcheck", $config, $host, $disk, $device, $level);
}

sub command_pre_host_selfcheck {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("pre-host-selfcheck", $config, $host, $disk, $device, $level);
}

sub command_post_dle_selfcheck {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("post-dle-selfcheck", $config, $host, $disk, $device, $level);
}

sub command_post_host_selfcheck {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("post-host-selfcheck", $config, $host, $disk, $device, $level);
}

sub command_pre_dle_estimate {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("pre-dle-estimate", $config, $host, $disk, $device, $level);
}

sub command_pre_host_estimate {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("pre-host-estimate", $config, $host, $disk, $device, $level);
}

sub command_post_dle_estimate {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("post-dle-estimate", $config, $host, $disk, $device, $level);
}

sub command_post_host_estimate {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("post-host-estimate", $config, $host, $disk, $device, $level);
}

sub command_pre_dle_backup {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("pre-dle-backup", $config, $host, $disk, $device, $level);
}

sub command_pre_host_backup {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("pre-host-backup", $config, $host, $disk, $device, $level);
}

sub command_post_dle_backup {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("post-dle-backup", $config, $host, $disk, $device, $level);
}

sub command_post_host_backup {
   my($config, $host, $disk, $device, $level) = @_;
   sendmail("post-host-backup", $config, $host, $disk, $device, $level);
}

sub sendmail {
   my($function, $config, $host, $disk, $device, $level) = @_;
   if (defined(@{$property{mailto}})) {
      $destcheck = join ',', @{$property{mailto}};
      $destcheck =~ /^([a-zA-Z,]*)$/;
      $dest = $1;
   } else {
      $dest = "root";
   }
   $cmd = "$mailer -s \"$config $function $host $disk $device\" $dest";
   print DEBUG "cmd: $cmd\n" if ($debug == 1);
   open(MAIL,"|$cmd");
   print MAIL "$config $function $host $disk $device\n";
   close MAIL;
}

require "$application_dir/generic-script"
