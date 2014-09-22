#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use File::Basename;
use XML::Simple;
use IPC::Open3;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants :quoting );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Header;
use Amanda::Holding;
use Amanda::Cmdline;
use Amanda::Xfer qw( :constants );
use Amanda::Recovery::Planner;
use Amanda::Recovery::Clerk;
use Amanda::Recovery::Scan;
use Amanda::Extract;
use Amanda::FetchDump;

# Interactivity package
package Amanda::Interactivity::amfetchdump;
use POSIX qw( :errno_h );
use Amanda::MainLoop qw( :GIOCondition );
use vars qw( @ISA );
@ISA = qw( Amanda::Interactivity );

sub new {
    my $class = shift;

    if (!-r STDIN) {
	return undef;
    }

    my $self = {
	input_src => undef};
    return bless ($self, $class);
}

sub abort() {
    my $self = shift;

    if ($self->{'input_src'}) {
	$self->{'input_src'}->remove();
	$self->{'input_src'} = undef;
    }
}

sub user_request {
    my $self = shift;
    my %params = @_;
    my $buffer = "";

    my $message  = $params{'message'};
    my $label    = $params{'label'};
    my $err      = $params{'err'};
    my $chg_name = $params{'chg_name'};

    my $data_in = sub {
	my $b;
	my $n_read = POSIX::read(0, $b, 1);
	if (!defined $n_read) {
	    return if ($! == EINTR);
	    $self->abort();
	    return $params{'request_cb'}->(
		Amanda::Changer::Error->new('fatal',
			message => "Fail to read from stdin"));
	} elsif ($n_read == 0) {
	    $self->abort();
	    return $params{'request_cb'}->(
		Amanda::Changer::Error->new('fatal',
			message => "Aborted by user"));
	} else {
	    $buffer .= $b;
	    if ($b eq "\n") {
		my $line = $buffer;
		chomp $line;
		$buffer = "";
		$self->abort();
		return $params{'request_cb'}->(undef, $line);
	    }
	}
    };

    print STDERR "$err\n";
    print STDERR "Insert volume labeled '$label' in $chg_name\n";
    print STDERR "and press enter, or ^D to abort.\n";

    $self->{'input_src'} = Amanda::MainLoop::fd_source(0, $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    $self->{'input_src'}->set_callback($data_in);
    return;
};

package amfetchdump;

use base 'Amanda::Recovery::Clerk::Feedback';

sub set_feedback {
    my $self = shift;
    my %params = @_;

    $self->{'chg'} = $params{'chg'} if exists $params{'chg'};
    $self->{'dev_name'} = $params{'dev_name'} if exists $params{'dev_name'};

    return $self;
}

sub clerk_notif_part {
    my $self = shift;
    my ($label, $filenum, $header) = @_;

    $self->user_message(Amanda::FetchDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 3300003,
		label		=> $label,
		filenum		=> $filenum,
		header_summary	=> $header->summary()));
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    $self->user_message(Amanda::FetchDump::Message->new(
		source_filename	=> __FILE__,
		source_line	=> __LINE__,
		code		=> 3300004,
		holding_file	=> $filename,
		header_summary	=> $header->summary()));
}

package main;

sub usage {
    my ($msg) = @_;
    print STDERR <<EOF;
Usage: amfetchdump [-c|-C|-l] [-p|-n] [-a] [-O directory] [-d device]
    [-h|--header-file file|--header-fd fd]
    [-decrypt|--no-decrypt|--server-decrypt|--client-decrypt]
    [--decompress|--no-decompress|--server-decompress|--client-decompress]
    [--extract --directory directory [--data-path (amanda|directtcp)]
    [--application-property='NAME=VALUE']*]
    [--init] [--restore]
    [-o configoption]* [--exact-match] config
    hostname [diskname [datestamp [hostname [diskname [datestamp ... ]]]]]
EOF
    print STDERR "ERROR: $msg\n" if $msg;
    exit(1);
}

##
# main

Amanda::Util::setup_application("amfetchdump", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);

my ($opt_config, $opt_no_reassembly, $opt_compress, $opt_compress_best, $opt_pipe,
    $opt_assume, $opt_leave, $opt_blocksize, $opt_device, $opt_chdir, $opt_header,
    $opt_header_file, $opt_header_fd, @opt_dumpspecs,
    $opt_decrypt, $opt_server_decrypt, $opt_client_decrypt,
    $opt_decompress, $opt_server_decompress, $opt_client_decompress,
    $opt_init, $opt_restore,
    $opt_extract, $opt_directory, $opt_data_path, %application_property,
    $opt_exact_match);

my $NEVER = 0;
my $ALWAYS = 1;
my $ONLY_SERVER = 2;
my $ONLY_CLIENT = 3;
my $decrypt;
my $decompress;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'n' => \$opt_no_reassembly,
    'c' => \$opt_compress,
    'C' => \$opt_compress_best,
    'p' => \$opt_pipe,
    'a' => \$opt_assume,
    'l' => \$opt_leave,
    'h' => \$opt_header,
    'header-file=s' => \$opt_header_file,
    'header-fd=i' => \$opt_header_fd,
    'decrypt!' => \$opt_decrypt,
    'server-decrypt' => \$opt_server_decrypt,
    'client-decrypt' => \$opt_client_decrypt,
    'decompress!' => \$opt_decompress,
    'server-decompress' => \$opt_server_decompress,
    'client-decompress' => \$opt_client_decompress,
    'extract' => \$opt_extract,
    'directory=s' => \$opt_directory,
    'data-path=s' => \$opt_data_path,
    'application-property=s' => sub {
	    my ($name, $value) = split '=', $_[1];
	    push @{$application_property{$name}}, $value;
	},
    'exact-match' => \$opt_exact_match,
    'init' => \$opt_init,
    'restore!' => \$opt_restore,
    'b=s' => \$opt_blocksize,
    'd=s' => \$opt_device,
    'O=s' => \$opt_chdir,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();
usage() unless (@ARGV);
$opt_config = shift @ARGV;

if (defined $opt_compress and defined $opt_compress_best) {
    print STDERR "Can't use -c and -C\n";
    usage();
}

usage("The -b option is no longer supported; set readblocksize in the tapetype section\n" .
      "of amanda.conf instead.")
    if ($opt_blocksize);
usage("-l is not compatible with -c or -C")
    if ($opt_leave and $opt_compress);
usage("-p is not compatible with -n")
    if ($opt_leave and $opt_no_reassembly);
usage("-h, --header-file, and --header-fd are mutually incompatible")
    if (($opt_header and ($opt_header_file or $opt_header_fd))
	    or ($opt_header_file and $opt_header_fd));

     $opt_data_path = lc($opt_data_path) if defined ($opt_data_path);
usage("--data_path must be 'amanda' or 'directtcp'")
    if (defined $opt_data_path and $opt_data_path ne 'directtcp' and $opt_data_path ne 'amanda');

if (defined $opt_leave) {
    if (defined $opt_decrypt and $opt_decrypt) {
	print STDERR "-l is incompatible with --decrypt\n";
	usage();
    }
    if (defined $opt_server_decrypt) {
	print STDERR "-l is incompatible with --server-decrypt\n";
	usage();
    }
    if (defined $opt_client_decrypt) {
	print STDERR "-l is incompatible with --client-decrypt\n";
	usage();
    }
    if (defined $opt_decompress and $opt_decompress) {
	print STDERR "-l is incompatible with --decompress\n";
	usage();
    }
    if (defined $opt_server_decompress) {
	print STDERR "-l is incompatible with --server-decompress\n";
	usage();
    }
    if (defined $opt_client_decompress) {
	print STDERR "-l is incompatible with --client-decompress\n";
	usage();
    }
}

if (( defined $opt_directory and !defined $opt_extract) or
    (!defined $opt_directory and  defined $opt_extract)) {
    print STDERR "Both --directorty and --extract must be set\n";
    usage();
}
if (defined $opt_directory and defined $opt_extract) {
    $opt_decrypt = 1;
    if (defined $opt_server_decrypt or defined $opt_client_decrypt) {
	print STDERR "--server_decrypt or --client-decrypt is incompatible with --extract\n";
	usage();
    }
    $opt_decompress = 1;
    if (defined $opt_server_decompress || defined $opt_client_decompress) {
	print STDERR "--server-decompress r --client-decompress is incompatible with --extract\n";
	usage();
    }
    if (defined($opt_leave) +
	defined($opt_compress) +
	defined($opt_compress_best)) {
	print STDERR "Can't use -l -c or -C with --extract\n";
	usage();
    }
    if (defined $opt_pipe) {
	print STDERR "--pipe is incompatible with --extract\n";
	usage();
    }
    if (defined $opt_header) {
	print STDERR "--header is incompatible with --extract\n";
	usage();
    }
}

if (defined($opt_decrypt) +
    defined($opt_server_decrypt) +
    defined($opt_client_decrypt) > 1) {
    print STDERR "Can't use only on of --decrypt, --no-decrypt, --server-decrypt or --client-decrypt\n";
    usage();
}
if (defined($opt_decompress) +
    defined($opt_server_decompress) +
    defined($opt_client_decompress) > 1) {
    print STDERR "Can't use only on of --decompress, --no-decompress, --server-decompress or --client-decompress\n";
    usage();
}

if (defined($opt_compress) and
    defined($opt_decompress) +
    defined($opt_server_decompress) +
    defined($opt_client_decompress) > 0) {
    print STDERR "Can't specify -c with one of --decompress, --no-decompress, --server-decompress or --client-decompress\n";
    usage();
}
if (defined($opt_compress_best) and
    defined($opt_decompress) +
    defined($opt_server_decompress) +
    defined($opt_client_decompress) > 0) {
    print STDERR "Can't specify -C with one of --decompress, --no-decompress, --server-decompress or --client-decompress\n";
    usage();
}

#$decompress = $ALWAYS;
#$decrypt = $ALWAYS;
#$decrypt = $NEVER  if defined $opt_leave;
#$decrypt = $NEVER  if defined $opt_decrypt and !$opt_decrypt;
#$decrypt = $ALWAYS if defined $opt_decrypt and $opt_decrypt;
#$decrypt = $ONLY_SERVER if defined $opt_server_decrypt;
#$decrypt = $ONLY_CLIENT if defined $opt_client_decrypt;
#
#$opt_compress = 1 if $opt_compress_best;
#
#$decompress = $NEVER  if defined $opt_compress;
#$decompress = $NEVER  if defined $opt_leave;
#$decompress = $NEVER  if defined $opt_decompress and !$opt_decompress;
#$decompress = $ALWAYS if defined $opt_decompress and $opt_decompress;
#$decompress = $ONLY_SERVER if defined $opt_server_decompress;
#$decompress = $ONLY_CLIENT if defined $opt_client_decompress;

usage("must specify at least a hostname") unless @ARGV;
my $cmd_flags = $Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP |
		$Amanda::Cmdline::CMDLINE_PARSE_LEVEL;
$cmd_flags |= $Amanda::Cmdline::CMDLINE_EXACT_MATCH if $opt_exact_match;
@opt_dumpspecs = Amanda::Cmdline::parse_dumpspecs([@ARGV], $cmd_flags);

set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

package amfetchdump;

sub new {
    my $class = shift;

    my $self = bless {
	is_tty => -t STDERR
    }, $class;

    return $self;
}

sub user_message {
    my $self = shift;
    my $message = shift;

    if ($message->{'code'} == 3300000) { #SIZE
	if ($self->{'is_tty'}) {
	    #print STDERR "\n" if !$self->{'last_is_size'};
	    print STDERR "\r$message    ";
	    $self->{'last_is_size'} = 1;
	} else {
	    print STDERR "READ SIZE: $message\n";
	}
    } elsif ($message->{'code'} == 3300012) { #READ SIZE
	#if ($self->{'is_tty'}) {
	#    print STDERR "A\n" if !$self->{'last_is_size'};
	#}
	print STDERR "\r$message\n";
    } else {
	if ($message->{'code'} == 3300003 || $message->{'code'} == 3300004) {
	    print "\n";
	}
	print STDERR "\n" if $self->{'is_tty'} and $self->{'last_is_size'};
	print STDERR "$message\n";
	$self->{'last_is_size'} = 0;

	if ($message->{'code'} == 3300002 and !$opt_assume) {
	    print STDERR "Press enter when ready\n";
	    my $resp = <STDIN>;
	}
    }
}

package amfetchdump;

use Amanda::MainLoop qw( :GIOCondition );
sub main {
    my $self = shift;
    my ($finished_cb) = @_;

    my $interactivity = Amanda::Interactivity::amfetchdump->new();
    my ($fetchdump, $result_message) = Amanda::FetchDump->new();

    $fetchdump->restore(
		'application_property'	=> \%application_property,
		'assume'		=> $opt_assume,
		'chdir'			=> $opt_chdir,
		'client-decompress'	=> $opt_client_decompress,
		'client-decrypt'	=> $opt_client_decrypt,
		'compress'		=> $opt_compress,
		'compress-best'		=> $opt_compress_best,
		'data-path'		=> $opt_data_path,
		'decompress'		=> $opt_decompress,
		'decrypt'		=> $opt_decrypt,
		'device'		=> $opt_device,
		'directory'		=> $opt_directory,
		'dumpspecs'		=> \@opt_dumpspecs,
		'exact-match'		=> $opt_exact_match,
		'extract'		=> $opt_extract,
		'header'		=> $opt_header,
		'header-fd'		=> $opt_header_fd,
		'header-file'		=> $opt_header_file,
		'init'			=> $opt_init,
		'leave'			=> $opt_leave,
		'no-reassembly'		=> $opt_no_reassembly,
		'pipe-fd'		=> $opt_pipe ? 1 : undef,
		'restore'		=> $opt_restore,
		'server-decompress'	=> $opt_server_decompress,
		'server-decrypt'	=> $opt_server_decrypt,
		'finished_cb'		=> $finished_cb,
		'interactivity'		=> $interactivity,
		'feedback'		=> $self);
}

package main;

my $exit_status;
sub fetchdump_done {
    $exit_status = shift;
    Amanda::MainLoop::quit();
}

my $amfetchdump = amfetchdump->new();
$amfetchdump->main(\&fetchdump_done);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit $exit_status;
