#!@PERL@ 
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;
use Getopt::Long;

package Amanda::Application::Amrandom;
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
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $calcsize, $include_list, $exclude_list, $directory, $size, $min_size, $max_size, $block_size, $min_block_size, $max_block_size) = @_;
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
    $self->{size}	      = $size;
    $self->{min_size}	      = $min_size || 1;
    $self->{max_size}	      = $max_size || 131072;
    $self->{block_size}	      = $block_size;
    $self->{min_block_size}   = $min_block_size || 1;
    $self->{max_block_size}   = $max_block_size || 32768;

    if (!defined $self->{size}) {
	$self->{'size'} = $self->{min_size} + int(rand($self->{max_size}- $self->{min_size}));
    }
    debug("size: $self->{size}");
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

    $self->print_to_server("amrandom version " . $Amanda::Constants::VERSION,
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
}

sub command_estimate {
    my $self = shift;

    my $level = $self->{level}[0];

    if ($level != 0) {
	$self->print_to_server("amrandom can only do level 0 backup",
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

    output_size($level, $self->{size});
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
	$self->print_to_server("amrandom can only do level 0 backup",
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

    my $buffer="A";
    my $buf_size = 1024*1024;
    if (defined $self->{blocksize} && $self->{blocksize} < $buf_size) {
	$buf_size = $self->{blocksize};
    }
    for (my $i=0; $i<$buf_size; $i++) {
	$buffer .= chr(int(rand(256)));
    }

    my $size = $self->{size};
    my $out = fileno(STDOUT);
    while ($size > 0) {
	my $block_size = $self->{block_size} ||
		$self->{min_block_size} + int(rand($self->{max_block_size} - $self->{min_block_size}));

	if ($block_size > $size) {
	    $block_size = $size;
	}
	debug("writting $block_size bytes");
	my $n = POSIX::write($out, $buffer, $block_size);
	if ($n ne $block_size) {
	    debug("Bad write $n != $block_size");
	}
	$size -= $block_size
    }
    POSIX::close($out);
    if (defined($self->{index})) {
	$self->{'index_out'}->print("/\n");
	$self->{'index_out'}->close;
    }
    $size = $self->{size};
    if ($size >= 0) {
	my $ksize = $size / 1024;
	if ($ksize < 32) {
	    $ksize = 32;
	}
	print {$self->{mesgout}} "sendbackup: size $ksize\n";
    }
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

    $device = "amrandom-restored" if !defined $device;
    debug("Restoring to $device");

    my $fd = POSIX::open($device, &POSIX::O_CREAT | &POSIX::O_RDWR, 0600 );
    if (!defined $fd) {
	$self->print_to_server_and_die("Can't open '$device': $!",
				       $Amanda::Script_App::ERROR);
    }
    my $size = 0;
    my $s;
    my $buffer;
    my $in = fileno(STDIN);
    while (($s = POSIX::read($in, $buffer, 32768)) > 0) {
	Amanda::Util::full_write($fd, $buffer, $s);
	$size += $s;
    }
    POSIX::close($fd);
    POSIX::close($in);
}

sub command_validate {
    my $self = shift;

    $self->default_validate();
}

sub command_index {
    my $self = shift;
    my $buffer;

    print "/\n";

    do {
        sysread STDIN, $buffer, 1048576;
    } while (defined $buffer and length($buffer) > 0);
}

package main;

sub usage {
    print <<EOF;
Usage: amrandom <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --calcsize.
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
my $opt_size;
my $opt_min_size;
my $opt_max_size;
my $opt_block_size;
my $opt_min_block_size;
my $opt_max_block_size;

my @orig_argv = @ARGV;

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
    'directory=s'        => \$opt_directory,
    'size=s'		 => \$opt_size,
    'min-size=s'	 => \$opt_min_size,
    'max-size=s'	 => \$opt_max_size,
    'block-size=s'	 => \$opt_block_size,
    'min-block-size=s'	 => \$opt_min_block_size,
    'max-block-size=s'	 => \$opt_max_block_size,
) or usage();

if (defined $opt_version) {
    print "amrandom-" . $Amanda::Constants::VERSION , "\n";
    exit(0);
}

my $application = Amanda::Application::Amrandom->new($opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $opt_calcsize, \@opt_include_list, \@opt_exclude_list, $opt_directory, $opt_size, $opt_min_size, $opt_max_size, $opt_block_size, $opt_min_block_size, $opt_max_block_size);

Amanda::Debug::debug("Arguments: " . join(' ', @orig_argv));

$application->do($ARGV[0]);
# NOTREACHED
