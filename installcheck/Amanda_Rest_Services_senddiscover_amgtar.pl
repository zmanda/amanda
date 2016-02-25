# Copyright (c) 2014-2016 Carbonite, Inc.  All Rights Reserved.
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

use Test::More;
use File::Path;
use strict;
use warnings;

use POSIX;
use Socket;
use IO::Socket::UNIX;
use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

if (!Amanda::Util::built_with_component("client")) {
    plan skip_all => "Not build with client";
    exit 1;
}

eval 'use Installcheck::Rest;';
if ($@) {
    plan skip_all => "Can't load Installcheck::Rest: $@";
    exit 1;
}

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $rest = Installcheck::Rest->new();
if ($rest->{'error'}) {
   plan skip_all => "Can't start JSON Rest server: $rest->{'error'}: see " . Amanda::Debug::dbfn();
   exit 1;
}
plan tests => 6;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

$testconf = Installcheck::Run::setup();
$testconf->write();

#CODE 3700012
#my $test_dir = "$Installcheck::TMP/test_amgtar_discover";
# Using /tmp because socket path length are limited to 108
my $test_dir;

if (File::Temp->can('newdir')) {
    $test_dir = File::Temp->newdir('test_amgtar_discoverXXXXXX',
                                   DIR      => '/tmp',
                                   CLEANUP  => 1);
} else {
    $test_dir = "/tmp/test_amgtar_discover$$";
    rmtree $test_dir;
    mkdir $test_dir;
}

$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=amgtar&diskdevice=$test_dir/does_not_exists");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::ERROR,
		'message' => "Can't open disk '$test_dir/does_not_exists': No such file or directory",
		'diskname' => "$test_dir/does_not_exists",
		'merrno' => '2',
		'errnocode' => 'ENOENT',
		'errnostr' => 'No such file or directory',
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3700012'
	  },
        ],
      http_code => 200,
    },
    "test_dir do not exists") || diag("reply: " . Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=amgtar&diskdevice=$test_dir");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::ERROR,
		'message' => "no senddiscover result to list",
		'diskname' => "$test_dir",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100006'
	  },
        ],
      http_code => 200,
    },
    "test_dir is empty") || diag("reply: " . Data::Dumper::Dumper($reply));

mkdir "$test_dir/dir";
open (my $fh, ">$test_dir/file");
close $fh;
symlink "file", "$test_dir/symlinkfile";
symlink "file", "$test_dir/symlinkdir";
link "$test_dir/file", "$test_dir/linkfile";
mkfifo "$test_dir/fifo", "0700";
my $SOCK_PATH = "$test_dir/socket";
my $server = IO::Socket::UNIX->new(
        Type => SOCK_STREAM(),
        Local => $SOCK_PATH,
        Listen => 1,
);

socket(my $socket, PF_UNIX, SOCK_STREAM, 0);
bind $socket, sockaddr_un("$test_dir/socket");

$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=amgtar&diskdevice=$test_dir");

my @sorted = sort { $a->{dle_name} cmp $b->{dle_name} } @{$reply->{'body'}};
$reply->{body} = \@sorted;
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'diskname' => "$test_dir",
		'file_type' => 'dir',
		'dle_name' => "dir",
		'full_path' => "$test_dir/dir",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'fifo',
		'diskname' => "$test_dir",
		'dle_name' => "fifo",
		'full_path' => "$test_dir/fifo",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'regular',
		'diskname' => "$test_dir",
		'dle_name' => "file",
		'full_path' => "$test_dir/file",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'regular',
		'diskname' => "$test_dir",
		'dle_name' => "linkfile",
		'full_path' => "$test_dir/linkfile",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'socket',
		'diskname' => "$test_dir",
		'dle_name' => "socket",
		'full_path' => "$test_dir/socket",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'link',
		'diskname' => "$test_dir",
		'dle_name' => "symlinkdir",
		'full_path' => "$test_dir/symlinkdir",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'link',
		'diskname' => "$test_dir",
		'dle_name' => "symlinkfile",
		'full_path' => "$test_dir/symlinkfile",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
        ],
      http_code => 200,
    },
    "test_dir") || diag("reply: " . Data::Dumper::Dumper($reply));

#test a diskdevice that end with a /
my $diskname .= $test_dir . '/';
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=amgtar&diskdevice=$diskname");

@sorted = sort { $a->{dle_name} cmp $b->{dle_name} } @{$reply->{'body'}};
$reply->{body} = \@sorted;
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'dir',
		'diskname' => "$diskname",
		'dle_name' => "dir",
		'full_path' => "$test_dir/dir",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'fifo',
		'diskname' => "$diskname",
		'dle_name' => "fifo",
		'full_path' => "$test_dir/fifo",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'regular',
		'diskname' => "$diskname",
		'dle_name' => "file",
		'full_path' => "$test_dir/file",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'regular',
		'diskname' => "$diskname",
		'dle_name' => "linkfile",
		'full_path' => "$test_dir/linkfile",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'socket',
		'diskname' => "$diskname",
		'dle_name' => "socket",
		'full_path' => "$test_dir/socket",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'link',
		'diskname' => "$diskname",
		'dle_name' => "symlinkdir",
		'full_path' => "$test_dir/symlinkdir",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
          {	'source_filename' => "amgtar.c",
		'severity' => $Amanda::Message::INFO,
		'message' => 'senddiscover result',
		'file_type' => 'link',
		'diskname' => "$diskname",
		'dle_name' => "symlinkfile",
		'full_path' => "$test_dir/symlinkfile",
		'process' => 'amgtar',
		'running_on' => 'amanda-client',
		'component' => 'application',
		'module' => 'amgtar',
		'code' => '3100005'
	  },
        ],
      http_code => 200,
    },
    "test_dir/") || diag("reply: " . Data::Dumper::Dumper($reply));
rmtree($test_dir);

my $dev_dir = "/dev";
$dev_dir = '/devices/isa/fdc@1,3f0' if -e '/devices/isa/fdc@1,3f0';
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=amgtar&diskdevice=$dev_dir");

@sorted = sort { $a->{dle_name} cmp $b->{dle_name} } @{$reply->{'body'}};
$reply->{body} = \@sorted;

my $bad_block = 0;
my $bad_char = 0;
foreach my $message (@{$reply->{'body'}}) {
    -l $message->{'full_path'};
    if (-b _) {
	if ($message->{'file_type'} ne 'block') {
	    $bad_block++;
	    diag("message: " . Data::Dumper::Dumper($message));
	} else {
	    #diag("found a block device");
	}
    } elsif ($message->{'file_type'} eq 'block') {
	$bad_block++;
	diag("message: " . Data::Dumper::Dumper($message));
    } elsif (-c _) {
	if ($message->{'file_type'} ne 'char') {
	    $bad_char++;
	    diag("message: " . Data::Dumper::Dumper($message));
	} else {
	    #diag("found a char device");
	}
    } elsif ($message->{'file_type'} eq 'char') {
	$bad_char++;
	diag("message: " . Data::Dumper::Dumper($message));
    }
}

is($bad_block, 0, "found bad block");
is($bad_char, 0, "found bad char");

close($socket);
rmtree $test_dir;

$rest->stop();

