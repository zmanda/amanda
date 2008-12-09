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

package Amanda::Application::Amsamba;
use base qw(Amanda::Application);
use File::Copy;
use File::Path;
use IPC::Open2;
use IPC::Open3;
use Sys::Hostname;
use Symbol;
use IO::Handle;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants :quoting);

sub new {
    my $class = shift;
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $calcsize, $gnutar_path, $smbclient_path, $amandapass, $exclude_file, $exclude_list, $exclude_optional, $include_file, $include_list, $include_optional, $recover_mode) = @_;
    my $self = $class->SUPER::new();

    if (defined $gnutar_path) {
	$self->{gnutar}     = $gnutar_path;
    } else {
	$self->{gnutar}     = $Amanda::Constants::GNUTAR;
    }
    if (defined $smbclient_path) {
	$self->{smbclient}  = $smbclient_path;
    } else {
	$self->{smbclient}  = $Amanda::Constants::SAMBA_CLIENT;
    }
    if (defined $amandapass) {
	$self->{amandapass}  = $amandapass;
    } else {
	$self->{amandapass}  = "$Amanda::Paths::sysconfdir/amandapass";
    }

    $self->{config}           = $config;
    $self->{host}             = $host;
    $self->{disk}             = $disk;
    $self->{device}           = $device;
    $self->{level}            = [ @{$level} ];
    $self->{index}            = $index;
    $self->{message}          = $message;
    $self->{collection}       = $collection;
    $self->{record}           = $record;
    $self->{calcsize}         = $calcsize;
    $self->{exclude_file}     = [ @{$exclude_file} ];
    $self->{exclude_list}     = [ @{$exclude_list} ];
    $self->{exclude_optional} = $exclude_optional;
    $self->{include_file}     = [ @{$include_file} ];
    $self->{include_list}     = [ @{$include_list} ];
    $self->{include_optional} = $include_optional;
    $self->{recover_mode}     = $recover_mode;

    return $self;
}

# on entry:
#   $self->{exclude_file}
#   $self->{exclude_list}
#   $self->{include_file}
#   $self->{include_list}
#on exit:
#  $self->{exclude}
#  $self->{include}
sub validate_inexclude {
    my $self = shift;

    if ($#{$self->{exclude_file}} + $#{$self->{exclude_list}} >= -1 &&
	$#{$self->{include_file}} + $#{$self->{include_list}} >= -1) {
	$self->print_to_server_and_die($self->{action},
				       "Can't have both include and exclude",
				       $Amanda::Script_App::ERROR);
    }

    if ($#{$self->{exclude_file}} >= 0) {
	$self->{exclude} = [ @{$self->{exclude_file}} ];
    }
    foreach my $file (@{$self->{exclude_list}}) {
	if (!open(FF, $file)) {
	    if ($self->{action} eq 'check' && !$self->{exclude_optional}) {
		$self->print_to_server($self->{action},
				       "Open of '$file' failed: $!",
				       $Amanda::Script_App::ERROR);
	    }
	    next;
	}
	while (<FF>) {
	    chomp;
	    push @{$self->{exclude}}, $_;
	}
	close(FF);
    }
    if ($#{$self->{include_file}} >= 0) {
	$self->{include} = [ @{$self->{include_file}} ];
    }
    foreach my $file (@{$self->{include_list}}) {
	if (open(FF, $file)) {
	    if ($self->{action} eq 'check') {
		$self->print_to_server($self->{action},
				       "Open of '$file' failed: $!",
				       $Amanda::Script_App::ERROR);
	    }
	    next;
	}
	while (<FF>) {
	    chomp;
	    push @{$self->{include}}, $_;
	}
	close(FF);
    }
}

# on entry:
#   $self->disk == //host/share/subdir
# on exit:
#   self->{cifshost} = //host
#   $self->{share} = //host/share
#   $self->{sambashare} = \\host\share
#   $self->{subdir} = subdir
sub parsesharename {
    my $self = shift;

    return if !defined $self->{disk};

    if ($self->{disk} =~ m,^(//[^/]+/[^/]+)/(.*)$,) {
	$self->{share} = $1;
	$self->{subdir} = $2
    } else {
	$self->{share} = $self->{disk};
    }
    $self->{sambashare} = $self->{share};
    $self->{sambashare} =~ s,/,\\,g;
    $self->{disk} =~ m,^(//[^/]+)/[^/]+,;
    $self->{cifshost} = $1;
}


# Read $self->{amandapass} file.
# on entry:
#   $self->{share} == //host/share
# on exit:
#   $self->{domain}   = domain to connect to.
#   $self->{username} = username (-U)
#   $self->{password} = password
sub findpass {
    my $self = shift;

    my $amandapass;
    my $line;

    open($amandapass, $self->{amandapass});
    while ($line = <$amandapass>) {
	chomp $line;
	next if $line =~ /^#/;
	my ($diskname, $userdomain) = Amanda::Util::skip_quoted_string($line);
	if (defined $diskname &&
	    ($diskname eq '*' ||
	     ($diskname =~ m,^(//[^/]+)/\*$, && $1 eq $self->{cifshost}) ||
	     $diskname eq $self->{share} ||
	     $diskname eq $self->{sambashare})) {
	    my ($userpasswd, $domain) = split ' ', $userdomain;
	    $self->{domain} = $domain;
	    my ($username, $password) = split('%', $userpasswd);
	    $self->{username} = $username;
	    $self->{password} = $password;
	    close($amandapass);
	    return;
	}
    }
    close($amandapass);
    $self->print_to_server_and_die($self->{action},"Cannot find password for share $self->{share} in $self->{amandapass}", $Amanda::Script_App::ERROR);
}

sub command_support {
    my $self = shift;

    print "CONFIG YES\n";
    print "HOST YES\n";
    print "DISK YES\n";
    print "MAX-LEVEL 1\n";
    print "INDEX-LINE YES\n";
    print "INDEX-XML NO\n";
    print "MESSAGE-LINE YES\n";
    print "MESSAGE-XML NO\n";
    print "RECORD YES\n";
    print "COLLECTION NO\n";
    print "MULTI-ESTIMATE NO\n";
    print "CALCSIZE NO\n";
    print "EXCLUDE-FILE YES\n";
    print "EXCLUDE-LIST YES\n";
    print "EXCLUDE-OPTIONAL YES\n";
    print "INCLUDE-FILE YES\n";
    print "INCLUDE-LIST YES\n";
    print "INCLUDE-OPTIONAL YES\n";
    print "RECOVER-MODE SMB\n";
}

sub command_selfcheck {
    my $self = shift;

    $self->{action} = 'check';
    $self->parsesharename();
    $self->findpass();
    $self->validate_inexclude();

    print "OK " . $self->{share} . "\n";
    print "OK " . $self->{disk} . "\n";
    print "OK " . $self->{device} . "\n";

    my ($child_rdr, $parent_wtr);
    $^F=10;
    pipe($child_rdr,  $parent_wtr);
    $parent_wtr->autoflush(1);
    my($wtr, $rdr, $err);
    $err = Symbol::gensym;
    my $pid = open3($wtr, $rdr, $err, "-");
    if ($pid == 0) {
	#child
	my $ff = $child_rdr->fileno;
	debug("child_rdr $ff");
	$parent_wtr->close();
	$ENV{PASSWD_FD} = $child_rdr->fileno;
	close(1);
	close(2);
	my @ARGV = ();
	push @ARGV, $self->{smbclient}, $self->{share},
		    "-U", $self->{username},
		    "-E";
	if (defined $self->{domain}) {
	    push @ARGV, "-W", $self->{domain},
	}
	if (defined $self->{subdir}) {
	    push @ARGV, "-D", $self->{subdir},
	}
	push @ARGV, "-c", "quit";
	debug("execute: " . $self->{smbclient} . " " .
	      join(" ", @ARGV));
	exec {$self->{smbclient}} @ARGV;
    }
    #parent
    my $ff = $parent_wtr->fileno;
    debug("parent_wtr $ff");
    debug("password $self->{password}");
    $parent_wtr->print($self->{password});
    $parent_wtr->close();
    $child_rdr->close();
    close($wtr);
    close($rdr);
    while (<$err>) {
	chomp;
	debug("stderr: " . $_);
	next if /^Domain=/;
	$self->print_to_server($self->{action}, "smbclient: $_",
			       $Amanda::Script_App::ERROR);
    }
    close($err);
    waitpid($pid, 0);
    #check binary
    #check statefile
    #check amdevice
}

sub command_estimate {
    my $self = shift;

    $self->{action} = 'estimate';
    $self->parsesharename();
    $self->findpass();
    $self->validate_inexclude();

    my $level = $self->{level}[0];
    my ($child_rdr, $parent_wtr);
    $^F=10;
    pipe($child_rdr,  $parent_wtr);
    $parent_wtr->autoflush(1);
    my($wtr, $rdr, $err);
    $err = Symbol::gensym;
    my $pid = open3($wtr, $rdr, $err, "-");
    if ($pid == 0) {
	#child
	my $ff = $child_rdr->fileno;
	debug("child_rdr $ff");
	$parent_wtr->close();
	$ENV{PASSWD_FD} = $child_rdr->fileno;
	close(0);
	close(1);
	my @ARGV = ();
	push @ARGV, $self->{smbclient}, $self->{share},
		    "-d", "0",
		    "-U", $self->{username},
		    "-E";
	if (defined $self->{domain}) {
	    push @ARGV, "-W", $self->{domain},
	}
	if (defined $self->{subdir}) {
	    push @ARGV, "-D", $self->{subdir},
	}
	if ($level == 0) {
	    push @ARGV, "-c", "archive 0;recurse;du";
	} else {
	    push @ARGV, "-c", "archive 1;recurse;du";
	}
	debug("execute: " . $self->{smbclient} . " " .
	      join(" ", @ARGV));
	exec {$self->{smbclient}} @ARGV;
    }
    #parent
    my $ff = $parent_wtr->fileno;
    debug("parent_wtr $ff");
    debug("password $self->{password}");
    $parent_wtr->print($self->{password});
    $parent_wtr->close();
    $child_rdr->close();
    close($wtr);
    close($rdr);
    my $size = $self->parse_estimate($err);
    close($err);
    output_size($level, $size);
    waitpid($pid, 0);
}

sub parse_estimate {
    my $self = shift;
    my($fh)  = shift;
    my($size) = -1;
    while(<$fh>) {
	chomp;
	next if /^\s*$/;
	next if /blocks of size/;
	next if /blocks available/;
	next if /^\s*$/;
	next if /^Domain=/;
	next if /dumped \d+ files and directories/;
	debug("stderr: $_");
	if ($_ =~ /^Total number of bytes: (\d*)/) {
	    $size = $1;
	    last;
	} else {
	    $self->print_to_server($self->{action}, "smbclient: $_",
				   $Amanda::Script_App::ERROR);
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

    $self->{action} = 'backup';
    $self->parsesharename();
    $self->findpass();
    $self->validate_inexclude();

    my $level = $self->{level}[0];
    my $mesgout_fd;
    open($mesgout_fd, '>&=3') || die();
    $self->{mesgout} = $mesgout_fd;

    my $pid_tee = open3(\*INDEX_IN, '>&STDOUT', \*INDEX_TEE, "-");
    if ($pid_tee == 0) {
	close(INDEX_IN);
	close(INDEX_TEE);
	my $buf;
	my $size = -1;
	while (($size = POSIX::read(0, $buf, 32768)) > 0) {
	    POSIX::write(1, $buf, $size);
	    POSIX::write(2, $buf, $size);
	}
	exit 0;
    }
    my ($child_rdr, $parent_wtr);
    $^F=10;
    pipe($child_rdr,  $parent_wtr);
    $^F=2;
    $parent_wtr->autoflush(1);
    my($wtr, $err);
    $err = Symbol::gensym;
    my $pid = open3($wtr, ">&INDEX_IN", $err, "-");
    if ($pid == 0) {
	#child
	my $ff = $child_rdr->fileno;
	debug("child_rdr $ff");
	$parent_wtr->close();
	$ENV{PASSWD_FD} = $child_rdr->fileno;
	close(0);
	my @ARGV = ();
	push @ARGV, $self->{smbclient}, $self->{share},
		    "-d", "0",
		    "-U", $self->{username},
		    "-E";
	if (defined $self->{domain}) {
	    push @ARGV, "-W", $self->{domain},
	}
	if (defined $self->{subdir}) {
	    push @ARGV, "-D", $self->{subdir},
	}
	my $comm ;
	if ($level == 0) {
	    $comm = "-Tqca";
	} else {
	    $comm = "-Tqcg";
	}
	if ($#{$self->{exclude}} >= 0) {
	    $comm .= "X";
	}
	if ($#{$self->{include}} >= 0) {
	    $comm .= "I";
	}
	push @ARGV, $comm, "-";
	if ($#{$self->{exclude}} >= 0) {
	    push @ARGV, @{$self->{exclude}};
	}
	if ($#{$self->{include}} >= 0) {
	    push @ARGV, @{$self->{include}};
	}
	debug("execute: " . $self->{smbclient} . " " .
	      join(" ", @ARGV));
	exec {$self->{smbclient}} @ARGV;
    }

    my $ff = $parent_wtr->fileno;
    debug("parent_wtr $ff");
    debug("password $self->{password}");
    $parent_wtr->print($self->{password});
    $parent_wtr->close();
    $child_rdr->close();
    close($wtr);

    #index process 
    my $index;
    debug("$self->{gnutar} -tf -");
    my $pid_index1 = open2($index, '<&INDEX_TEE', $self->{gnutar}, "-tf", "-");
    close(INDEX_IN);
    my $size = -1;
    my $index_fd = $index->fileno;
    debug("index $index_fd");
    if (defined($self->{index})) {
	my $indexout_fd;
	open($indexout_fd, '>&=4') || die();
	$self->parse_backup($index, $mesgout_fd, $indexout_fd);
	close($indexout_fd);
    }
    else {
	$self->parse_backup($index_fd, $mesgout_fd, undef);
    }
    close($index);

    while (<$err>) {
	chomp;
	debug("stderr: " . $_);
	next if /^Domain=/;
	next if /dumped (\d+) files and directories/;
	if (/^Total bytes written: (\d*)/) {
	    $size = $1;
	} else {
	    $self->print_to_server($self->{action}, "smbclient: $_",
			           $Amanda::Script_App::ERROR);
	}
    }
    if ($size >= 0) {
	my $ksize = $size / 1024;
	if ($ksize < 32) {
	    $ksize = 32;
	}
	print $mesgout_fd "sendbackup: size $ksize\n";
	print $mesgout_fd "sendbackup: end\n";
    }

    waitpid $pid, 0;
    if ($? != 0) {
	$self->print_to_server_and_die($self->{action},
				       "smbclient returned error",
				       $Amanda::Script_App::ERROR);
    }
    exit 0;
}

sub parse_backup {
    my $self = shift;
    my($fhin, $fhout, $indexout) = @_;
    my $size  = -1;
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
	    print $fhout "? $_";
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
    my @cmd = ();

    $self->{restore} = 'backup';
    $self->parsesharename();
    chdir(Amanda::Util::get_original_cwd());

    if ($self->{recover_mode} eq "smb") {
	$self->findpass();
	push @cmd, $self->{smbclient}, $self->{share},
		   "-d", "0",
		   "-U", $self->{username};
	
	if (defined $self->{domain}) {
	    push @cmd, "-W", $self->{domain};
	}
	push @cmd, "-Tx", "-";
	for(my $i=1;defined $ARGV[$i]; $i++) {
	    my $param = $ARGV[$i];
	    $param =~ /^(.*)$/;
	    push @cmd, $1;
	}
	my ($parent_rdr, $child_wtr);
	$^F=10;
	pipe($parent_rdr,  $child_wtr);
	$child_wtr->autoflush(1);
	my($wtr, $rdr, $err);
	$err = Symbol::gensym;
	my $pid = open3($wtr, $rdr, $err, "-");
	if ($pid == 0) {
	    $child_wtr->print($self->{password});
	    $child_wtr->close();
	    exit 0;
	}
	$child_wtr->close();
	$ENV{PASSWD_FD} = $parent_rdr->fileno;
	debug("cmd:" . join(" ", @cmd));
	exec { $cmd[0] } @cmd;
	die("Can't exec '", $cmd[0], "'");
    } else {
	push @cmd, $self->{gnutar}, "-xpvf", "-";
	for(my $i=1;defined $ARGV[$i]; $i++) {
	    my $param = $ARGV[$i];
	    $param =~ /^(.*)$/;
	    push @cmd, $1;
	}
	debug("cmd:" . join(" ", @cmd));
	exec { $cmd[0] } @cmd;
	die("Can't exec '", $cmd[0], "'");
    }
}

sub command_validate {
   my $self = shift;

   $self->{validate} = 'backup';
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
Usage: amsamba <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --calcsize.
EOF
    exit(1);
}

my $opt_version;
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
my $opt_gnutar_path;
my $opt_smbclient_path;
my $opt_amandapass;
my @opt_exclude_file;
my @opt_exclude_list;
my $opt_exclude_optional;
my @opt_include_file;
my @opt_include_list;
my $opt_include_optional;
my $opt_recover_mode;

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version'            => \$opt_version,
    'config=s'           => \$opt_config,
    'host=s'             => \$opt_host,
    'disk=s'             => \$opt_disk,
    'device=s'           => \$opt_device,
    'level=s'            => \@opt_level,
    'index=s'            => \$opt_index,
    'message=s'          => \$opt_message,
    'collection=s'       => \$opt_collection,
    'record'             => \$opt_record,
    'calcsize'           => \$opt_calcsize,
    'gnutar_path'        => \$opt_gnutar_path,
    'smbclient_path'     => \$opt_smbclient_path,
    'amandapass'         => \$opt_amandapass,
    'exclude-file=s'     => \@opt_exclude_file,
    'exclude-list=s'     => \@opt_exclude_list,
    'exclude-optional=s' => \$opt_exclude_optional,
    'include-file=s'     => \@opt_include_file,
    'include-list=s'     => \@opt_include_list,
    'include-optional=s' => \$opt_include_optional,
    'recover-mode=s'     => \$opt_recover_mode,
) or usage();

if (defined $opt_version) {
    print "amsamba-" . $Amanda::Constants::VERSION , "\n";
    exit(0);
}

my $application = Amanda::Application::Amsamba->new($opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $opt_calcsize, $opt_gnutar_path, $opt_smbclient_path, $opt_amandapass, \@opt_exclude_file, \@opt_exclude_list, $opt_exclude_optional, \@opt_include_file, \@opt_include_list, $opt_include_optional, $opt_recover_mode);

$application->do($ARGV[0]);
