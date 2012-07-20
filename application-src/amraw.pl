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

package Amanda::Application::Amraw;
use base qw(Amanda::Application);
use IPC::Open3;
use Sys::Hostname;
use Symbol;
use IO::Handle;
use Amanda::Constants;
use Amanda::Debug qw( :logging );
use Amanda::Util qw( quote_string );

sub new {
    my $class = shift;
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $calcsize, $include_list, $exclude_list, $directory) = @_;
    my $self = $class->SUPER::new($config);

    $self->{config}           = $config;
    $self->{host}             = $host;
    if (defined $disk) {
	$self->{disk}         = $disk;
    } else {
	$self->{disk}         = $device;
    }
    if (defined $device) {
	$self->{device}       = $device;
    } else {
	$self->{device}       = $disk;
    }
    $self->{level}            = [ @{$level} ];
    $self->{index}            = $index;
    $self->{message}          = $message;
    $self->{collection}       = $collection;
    $self->{record}           = $record;
    $self->{calcsize}         = $calcsize;
    $self->{exclude_list}     = [ @{$exclude_list} ];
    $self->{include_list}     = [ @{$include_list} ];
    $self->{directory}        = $directory;

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
    print "COLLECTION NO\n";
    print "MULTI-ESTIMATE NO\n";
    print "CALCSIZE NO\n";
    print "CLIENT-ESTIMATE YES\n";
}

sub command_selfcheck {
    my $self = shift;

    $self->print_to_server("disk " . quote_string($self->{disk}),
			   $Amanda::Script_App::GOOD)
		if defined $self->{disk};

    $self->print_to_server("amraw version " . $Amanda::Constants::VERSION,
			   $Amanda::Script_App::GOOD);

    $self->print_to_server(quote_string($self->{device}),
			   $Amanda::Script_App::GOOD)
		if defined $self->{device};

    if (! -r $self->{device}) {
	$self->print_to_server("$self->{device} can't be read",
			       $Amanda::Script_App::ERROR);
    }

    if ($#{$self->{include_list}} >= 0) {
	$self->print_to_server("include-list not supported for backup",
			       $Amanda::Script_App::ERROR);
    }
    if ($#{$self->{exclude_list}} >= 0) {
	$self->print_to_server("exclude-list not supported for backup",
			       $Amanda::Script_App::ERROR);
    }
    if ($self->{directory}) {
	$self->print_to_server("directory PROPERTY not supported for backup",
			       $Amanda::Script_App::ERROR);
    }

    #check statefile
    #check amdevice
}

sub command_estimate {
    my $self = shift;

    my $level = $self->{level}[0];

    if ($level != 0) {
	$self->print_to_server("amraw can only do level 0 backup",
			       $Amanda::Script_App::ERROR);
    }

    if ($#{$self->{include_list}} >= 0) {
	$self->print_to_server("include-list not supported for backup",
			       $Amanda::Script_App::ERROR);
    }
    if ($#{$self->{include_list}} >= 0) {
	$self->print_to_server("exclude-list not supported for backup",
			       $Amanda::Script_App::ERROR);
    }
    if ($self->{directory}) {
	$self->print_to_server("directory PROPERTY not supported for backup",
			       $Amanda::Script_App::ERROR);
    }

    my $fd = POSIX::open($self->{device}, &POSIX::O_RDONLY);
    my $size = 0;
    my $s;
    my $buffer;
    while (($s = POSIX::read($fd, $buffer, 32768)) > 0) {
	$size += $s;
    }
    POSIX::close($fd);
    output_size($level, $size);
}

sub output_size {
   my($level) = shift;
   my($size) = shift;
   if($size == -1) {
      print "$level -1 -1\n";
   }
   else {
      my($ksize) = int $size / (1024);
      $ksize=32 if ($ksize<32);
      print "$level $ksize 1\n";
   }
}

sub command_backup {
    my $self = shift;

    my $level = $self->{level}[0];

    if (defined($self->{index})) {
	$self->{'index_out'} = IO::Handle->new_from_fd(4, 'w');
	$self->{'index_out'} or confess("Could not open index fd");
    }

    if ($level != 0) {
	$self->print_to_server("amraw can only do level 0 backup",
			       $Amanda::Script_App::ERROR);
    }

    if ($#{$self->{include_list}} >= 0) {
	$self->print_to_server("include-list not supported for backup",
			       $Amanda::Script_App::ERROR);
    }
    if ($#{$self->{include_list}} >= 0) {
	$self->print_to_server("exclude-list not supported for backup",
			       $Amanda::Script_App::ERROR);
    }
    if ($self->{directory}) {
	$self->print_to_server("directory PROPERTY not supported for backup",
			       $Amanda::Script_App::ERROR);
    }

    my $fd = POSIX::open($self->{device}, &POSIX::O_RDONLY);
    my $size = 0;
    my $s;
    my $buffer;
    my $out = 1;
    while (($s = POSIX::read($fd, $buffer, 32768)) > 0) {
	Amanda::Util::full_write($out, $buffer, $s);
	$size += $s;
    }
    POSIX::close($fd);
    close($out);
    if (defined($self->{index})) {
	$self->{'index_out'}->print("/\n");
	$self->{'index_out'}->close;
    }
    if ($size >= 0) {
	my $ksize = $size / 1024;
	if ($ksize < 32) {
	    $ksize = 32;
	}
	print {$self->{mesgout}} "sendbackup: size $ksize\n";
	print {$self->{mesgout}} "sendbackup: end\n";
    }

    exit 0;
}

sub command_restore {
    my $self = shift;
    my @cmd = ();

    my $device = $self->{device};
    if (defined $self->{directory}) {
	$device = $self->{directory};
    } else {
	chdir(Amanda::Util::get_original_cwd());
    }

    # include-list and exclude-list are ignored, the complete dle is restored.

    $device = "amraw-restored" if !defined $device;

    my $fd = POSIX::open($device, &POSIX::O_CREAT | &POSIX::O_RDWR, 0600 );
    if ($fd == -1) {
	$self->print_to_server_and_die("Can't open '$device': $!",
				       $Amanda::Script_App::ERROR);
    }
    my $size = 0;
    my $s;
    my $buffer;
    my $in = 0;
    while (($s = POSIX::read($in, $buffer, 32768)) > 0) {
	Amanda::Util::full_write($fd, $buffer, $s);
	$size += $s;
    }
    POSIX::close($fd);
    close($in);
    exit(0);
}

sub command_validate {
    my $self = shift;

    $self->default_validate();
}

package main;

sub usage {
    print <<EOF;
Usage: amraw <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --calcsize.
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
my @opt_include_list;
my @opt_exclude_list;
my $opt_directory;

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
    'include-list=s'     => \@opt_include_list,
    'exclude-list=s'     => \@opt_exclude_list,
    'directory'          => \$opt_directory,
) or usage();

if (defined $opt_version) {
    print "amraw-" . $Amanda::Constants::VERSION , "\n";
    exit(0);
}

my $application = Amanda::Application::Amraw->new($opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $opt_calcsize, \@opt_include_list, \@opt_exclude_list, $opt_directory);

$application->do($ARGV[0]);
