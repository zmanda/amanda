#! @PERL@
# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use File::Basename;
use Getopt::Long;
use IPC::Open3;
use Symbol;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Tapelist;
use Amanda::Logfile;
use Amanda::Util qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Recovery::Clerk;
use Amanda::Recovery::Scan;
use Amanda::Recovery::Planner;
use Amanda::Constants;
use Amanda::DB::Catalog;
use Amanda::Cmdline;
use Amanda::MainLoop;
use Amanda::Xfer qw( :constants );
use XML::Simple;

sub usage {
    print <<EOF;
USAGE:	amcheckdump [ --timestamp|-t timestamp ] [-o configoption]* <conf>
    amcheckdump validates Amanda dump images by reading them from storage
volume(s), and verifying archive integrity if the proper tool is locally
available. amcheckdump does not actually compare the data located in the image
to anything; it just validates that the archive stream is valid.
    Arguments:
	config       - The Amanda configuration name to use.
	-t timestamp - The run of amdump or amflush to check. By default, check
			the most recent dump; if this parameter is specified,
			check the most recent dump matching the given
			date- or timestamp.
	-o configoption	- see the CONFIGURATION OVERRIDE section of amanda(8)
EOF
    exit(1);
}

## Application initialization

Amanda::Util::setup_application("amcheckdump", "server", $CONTEXT_CMDLINE);

my $exit_code = 0;

my $opt_timestamp;
my $opt_verbose = 0;
my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'timestamp|t=s' => \$opt_timestamp,
    'verbose|v'     => \$opt_verbose,
    'help|usage|?'  => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

usage() if (@ARGV < 1);

my $timestamp = $opt_timestamp;

my $config_name = shift @ARGV;
set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# Interactivity package
package Amanda::Interactivity::amcheckdump;
use POSIX qw( :errno_h );
use Amanda::MainLoop qw( :GIOCondition );
use vars qw( @ISA );
@ISA = qw( Amanda::Interactivity );

sub new {
    my $class = shift;

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

package main::Feedback;

use Amanda::Recovery::Clerk;
use base 'Amanda::Recovery::Clerk::Feedback';
use Amanda::MainLoop;

sub new {
    my $class = shift;
    my ($chg, $dev_name) = @_;

    return bless {
	chg => $chg,
	dev_name => $dev_name,
    }, $class;
}

sub clerk_notif_part {
    my $self = shift;
    my ($label, $filenum, $header) = @_;

    print STDERR "Reading volume $label file $filenum\n";
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    print STDERR "Reading '$filename'\n";
}

package main;

use Amanda::MainLoop qw( :GIOCondition );

# Given a dumpfile_t, figure out the command line to validate, specified
# as an argv array
sub find_validation_command {
    my ($header) = @_;

    my @result = ();

    # We base the actual archiver on our own table
    my $program = uc(basename($header->{program}));

    my $validation_program;

    if ($program ne "APPLICATION") {
        my %validation_programs = (
            "STAR" => [ $Amanda::Constants::STAR, qw(-t -f -) ],
            "DUMP" => [ $Amanda::Constants::RESTORE, qw(tbf 2 -) ],
            "VDUMP" => [ $Amanda::Constants::VRESTORE, qw(tf -) ],
            "VXDUMP" => [ $Amanda::Constants::VXRESTORE, qw(tbf 2 -) ],
            "XFSDUMP" => [ $Amanda::Constants::XFSRESTORE, qw(-t -v silent -) ],
            "TAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -tf -) ],
            "GTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -tf -) ],
            "GNUTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -tf -) ],
            "SMBCLIENT" => [ $Amanda::Constants::GNUTAR, qw(-tf -) ],
	    "PKZIP" => undef,
        );
	if (!exists $validation_programs{$program}) {
	    debug("Unknown program '$program' in header; no validation to perform");
	    return undef;
	}
        return $validation_programs{$program};

    } else {
	if (!defined $header->{application}) {
	    warning("Application not set");
	    return undef;
	}
	my $program_path = $Amanda::Paths::APPLICATION_DIR . "/" .
			   $header->{application};
	if (!-x $program_path) {
	    debug("Application '" . $header->{application}.
			 "($program_path)' not available on the server");
	    return undef;
	} else {
	    my $dle_str = $header->{'dle_str'};
	    my $p1 = XML::Simple->new();
	    my $dle;
	    if (defined $dle_str) {
		eval { $dle = $p1->XMLin($dle_str); };
		if ($@) {
		    print "ERROR: XML error\n";
		    debug("XML Error: $@\n$dle_str");
		}
	    }
	    my @argv;
	    push @argv, $program_path, "validate",
			"--config", get_config_name(),
			"--host" , $header->{'name'},
			"--disk" , $header->{'disk'},
			"--level", $header->{'dumplevel'};
	    if ($dle) {
		push @argv, "--device", $dle->{'diskdevice'} if defined $dle->{'diskdevice'};
	    }
	    return [ @argv ];
	}
    }
}

sub main {
    my ($finished_cb) = @_;

    my $tapelist;
    my $interactivity;
    my %scan;
    my $clerk;
    my %clerk;
    my %storage;
    my $plan;
    my $timestamp;
    my $all_success = 1;
    my @xfer_errs;
    my %all_filter;
    my $current_dump;
    my $recovery_done;
    my %recovery_params;
    my $xfer_src;
    my $xfer_dest;
    my $xfer_dest_native;
    my $hdr;
    my $source_crc;
    my $dest_crc;
    my $dest_native_crc;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { foreach my $name (keys %storage) {
			    $storage{$name}->quit();
			  }
			};

    step start => sub {
	# set up the tapelist
	my $tapelist_file = config_dir_relative(getconf($CNF_TAPELIST));
	($tapelist, my $message) = Amanda::Tapelist->new($tapelist_file);
	return $steps->{'quit'}->($message) if defined $message;

	# get the timestamp
	$timestamp = $opt_timestamp;
	$timestamp = Amanda::DB::Catalog::get_latest_write_timestamp()
	    unless defined $opt_timestamp;

	# make an interactivity plugin
	$interactivity = Amanda::Interactivity::amcheckdump->new();

	# make a changer
	my ($storage) = Amanda::Storage->new(tapelist => $tapelist);
	return  $steps->{'quit'}->($storage) if $storage->isa("Amanda::Message");
	$storage{$storage->{"storage_name"}} = $storage;
	my $chg = $storage->{'chg'};
	return $steps->{'quit'}->($chg) if $chg->isa("Amanda::Message");

	# make a scan
	my $scan = Amanda::Recovery::Scan->new(
			    chg => $chg,
			    interactivity => $interactivity);
	return $steps->{'quit'}->($scan)
	    if $scan->isa("Amanda::Changer::Error");
	$scan{$storage->{"storage_name"}} = $scan;

	# make a clerk
	my $clerk = Amanda::Recovery::Clerk->new(
	    feedback => main::Feedback->new($chg),
	    scan     => $scan{$storage->{"storage_name"}});
	$clerk{$storage->{"storage_name"}} = $clerk;

	# make a plan
	my $spec = Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, undef, $timestamp);
	my $storage_list;
	my $only_in_storage = 0;
	if (getconf_linenum($CNF_STORAGE) == -2) {
	    $storage_list = getconf($CNF_STORAGE);
	    $only_in_storage = 1;
	}
        Amanda::Recovery::Planner::make_plan(
            dumpspecs => [ $spec ],
	    storage_list => $storage_list,
	    only_in_storage => $only_in_storage,
            changer => $chg,
	    all_copy => 1,
            plan_cb => $steps->{'plan_cb'});
    };

    step plan_cb => sub {
	(my $err, $plan) = @_;
	$steps->{'quit'}->($err) if $err;

	my @tapes = $plan->get_volume_list();
	my @holding = $plan->get_holding_file_list();
	if (!@tapes && !@holding) {
	    print "Could not find any matching dumps.\n";
	    return $steps->{'quit'}->();
	}

	if (@tapes) {
	    printf("You will need the following volume%s: %s\n", (@tapes > 1) ? "s" : "",
		   join(", ", map { $_->{'label'} } @tapes));
	}
	if (@holding) {
	    printf("You will need the following holding file%s: %s\n", (@tapes > 1) ? "s" : "",
		   join(", ", @holding));
	}

	# nothing else is going on right now, so a blocking "Press enter.." is OK
	print "Press enter when ready\n";
	<STDIN>;

	my $dump = shift @{$plan->{'dumps'}};
	if (!$dump) {
	    return $steps->{'quit'}->("No backup written on timestamp $timestamp.");
	}

	$steps->{'check_dumpfile'}->($dump);
    };

    step check_dumpfile => sub {
	my ($dump) = @_;
	$current_dump = $dump;

	$recovery_done = 0;
	%recovery_params = ();

	print "Validating image " . $dump->{hostname} . ":" .
	    $dump->{diskname} . " dumped " . $dump->{dump_timestamp} . " level ".
	    $dump->{level};
	if ($dump->{'nparts'} > 1) {
	    print " ($dump->{nparts} parts)";
	}
	print "\n";

	my $storage_name = $dump->{'storage'};
	if (!$storage{$storage_name}) {
	    my ($storage) = Amanda::Storage->new(storage_name => $storage_name,
						 tapelist     => $tapelist);
            #return  $steps->{'quit'}->($storage) if $storage->isa("Amanda::Changer::Error");
	    $storage{$storage_name} = $storage;

	};
	my $chg = $storage{$storage_name}->{'chg'};
	if (!$scan{$storage_name}) {
	    my $scan = Amanda::Recovery::Scan->new(
				    chg => $chg,
				    interactivity => $interactivity);
	    #return $steps->{'quit'}->($scan)
	    #    if $scan{$storage->{"storage_name"}}->isa("Amanda::Changer::Error");
	    $scan{$storage_name} = $scan;
	};
	if (!$clerk{$storage_name}) {
	    my $clerk = Amanda::Recovery::Clerk->new(
		feedback => main::Feedback->new($chg),
		scan     => $scan{$storage_name});
	    $clerk{$storage_name} = $clerk;
	};
	$clerk = $clerk{$storage_name};
	@xfer_errs = ();
	$clerk->get_xfer_src(
	    dump => $dump,
	    xfer_src_cb => $steps->{'xfer_src_cb'});
    };

    step xfer_src_cb => sub {
	(my $errs, $hdr, $xfer_src, my $directtcp_supported) = @_;
	return $steps->{'quit'}->(join("; ", @$errs)) if $errs;

	# set up any filters that need to be applied; decryption first
	my @filters;
	if ($hdr->{'encrypted'}) {
	    if ($hdr->{'srv_encrypt'}) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srv_encrypt'}, $hdr->{'srv_decrypt_opt'} ], 0, 0, 0, 0);
	    } elsif ($hdr->{'clnt_encrypt'}) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clnt_encrypt'}, $hdr->{'clnt_decrypt_opt'} ], 0, 0, 0, 0);
	    } else {
		return $steps->quit("could not decrypt encrypted dump: no program specified");
	    }

	    $hdr->{'encrypted'} = 0;
	    $hdr->{'srv_encrypt'} = '';
	    $hdr->{'srv_decrypt_opt'} = '';
	    $hdr->{'clnt_encrypt'} = '';
	    $hdr->{'clnt_decrypt_opt'} = '';
	    $hdr->{'encrypt_suffix'} = 'N';
	}

	if ($hdr->{'compressed'}) {
	    # need to uncompress this file

	    if ($hdr->{'srvcompprog'}) {
		# TODO: this assumes that srvcompprog takes "-d" to decrypt
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srvcompprog'}, "-d" ], 0, 0, 0, 0);
	    } elsif ($hdr->{'clntcompprog'}) {
		# TODO: this assumes that clntcompprog takes "-d" to decrypt
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clntcompprog'}, "-d" ], 0, 0, 0, 0);
	    } else {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $Amanda::Constants::UNCOMPRESS_PATH,
			  $Amanda::Constants::UNCOMPRESS_OPT ], 0, 0, 0, 0);
	    }

	    # adjust the header
	    $hdr->{'compressed'} = 0;
	    $hdr->{'uncompress_cmd'} = '';
	}

	# set up a CRC filter to compute the native CRC
	$xfer_dest_native = Amanda::Xfer::Filter::Crc->new();
	push @filters, $xfer_dest_native;

	# and set up the validation command as a filter element, since
	# we need to throw out its stdout
	my $argv = find_validation_command($hdr);
	if (defined $argv) {
	    push @filters, Amanda::Xfer::Filter::Process->new($argv, 0, 1, 0, 1);
	}

	# we always throw out stdout
	$xfer_dest = Amanda::Xfer::Dest::Null->new(0);

	# start reading all filter stderr
	foreach my $filter (@filters) {
	    next if !$filter->can("get_stderr_fd");
	    my $fd = $filter->get_stderr_fd();
	    $fd.="";
	    $fd = int($fd);
	    my $src = Amanda::MainLoop::fd_source($fd,
						  $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
	    my $buffer = "";
	    $all_filter{$src} = 1;
	    $src->set_callback( sub {
		my $b;
		my $n_read = POSIX::read($fd, $b, 1);
		if (!defined $n_read) {
		    return;
		} elsif ($n_read == 0) {
		    delete $all_filter{$src};
		    $src->remove();
		    POSIX::close($fd);
		    if (!%all_filter and $recovery_done) {
			$steps->{'filter_done'}->();
		    }
		} else {
		    $buffer .= $b;
		    if ($b eq "\n") {
			my $line = $buffer;
			print STDERR "filter stderr: $line";
			chomp $line;
			debug("filter stderr: $line");
			$buffer = "";
		    }
		}
	    });
	}

	my $xfer = Amanda::Xfer->new([ $xfer_src, @filters, $xfer_dest ]);
	$xfer->start($steps->{'handle_xmsg'}, 0, $current_dump->{'bytes'});
	$clerk->start_recovery(
	    xfer => $xfer,
	    recovery_cb => $steps->{'recovery_cb'});
    };

    step handle_xmsg => sub {
	my ($src, $msg, $xfer) = @_;

	if ($msg->{'type'} == $XMSG_CRC) {
	    if ($msg->{'elt'} == $xfer_src) {
		$source_crc = $msg->{'crc'}.":".$msg->{'size'};
		debug("source_crc: $source_crc");
	    } elsif ($msg->{'elt'} == $xfer_dest_native) {
		$dest_native_crc = $msg->{'crc'}.":".$msg->{'size'};
		debug("dest_native_crc: $dest_native_crc");
	    } elsif ($msg->{'elt'} == $xfer_dest) {
		$dest_crc = $msg->{'crc'}.":".$msg->{'size'};
		debug("dest_crc: $dest_crc");
	    } else {
		debug("unhandled XMSG_CRC $msg->{'elt'} $msg->{'crc'}:$msg->{'size'}")
	    }
	} else {
	    $clerk->handle_xmsg($src, $msg, $xfer);
	    if ($msg->{'type'} == $XMSG_INFO) {
		Amanda::Debug::info($msg->{'message'});
	    } elsif ($msg->{'type'} == $XMSG_ERROR) {
		push @xfer_errs, $msg->{'message'};
	    }
	}
    };

    step recovery_cb => sub {
	%recovery_params = @_;
	$recovery_done = 1;

	$steps->{'filter_done'}->() if !%all_filter;
    };

    step filter_done => sub {
	# distinguish device errors from validation errors
	if (@{$recovery_params{'errors'}}) {
	    print STDERR "While reading from volumes:\n";
	    print STDERR "$_\n" for @{$recovery_params{'errors'}};
	    return $steps->{'quit'}->("validation aborted");
	}

	if (@xfer_errs) {
	    print STDERR "Validation errors:\n";
	    print STDERR "$_\n" for @xfer_errs;
	    $all_success = 0;
	}

        my $msg;
        if (defined $hdr->{'native_crc'} and $hdr->{'native_crc'} !~ /^00000000:/ and
            defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
            $hdr->{'native_crc'} ne $current_dump->{'native_crc'}) {
            $msg = "recovery failed: native-crc in header ($hdr->{'native_crc'}) and native-crc in log ($current_dump->{'native_crc'}) differ";
            print STDERR "$msg\n";
            debug($msg);
        }
        if (defined $hdr->{'client_crc'} and $hdr->{'client_crc'} !~ /^00000000:/ and
            defined $current_dump->{'client_crc'} and $current_dump->{'client_crc'} !~ /^00000000:/ and
            $hdr->{'client_crc'} ne $current_dump->{'client_crc'}) {
            $msg = "recovery failed: client-crc in header ($hdr->{'client_crc'}) and client-crc in log ($current_dump->{'client_crc'}) differ";
            print STDERR "$msg\n";
            debug($msg);
        }

        my $hdr_server_crc_size;
        my $current_dump_server_crc_size;
        my $source_crc_size;

        if (defined $hdr->{'server_crc'}) {
            $hdr->{'server_crc'} =~ /[^:]*:(.*)/;
            $hdr_server_crc_size = $1;
        }
        if (defined $current_dump->{'server_crc'}) {
            $current_dump->{'server_crc'} =~ /[^:]*:(.*)/;
            $current_dump_server_crc_size = $1;
        }
        if (defined $source_crc) {
            $source_crc =~ /[^:]*:(.*)/;
            $source_crc_size = $1;
        }

        if (defined $hdr->{'server_crc'} and $hdr->{'server_crc'} !~ /^00000000:/ and
            defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
            $hdr_server_crc_size == $current_dump_server_crc_size and
            $hdr->{'server_crc'} ne $current_dump->{'server_crc'}) {
            $msg = "recovery failed: server-crc in header ($hdr->{'server_crc'}) and server-crc in log ($current_dump->{'server_crc'}) differ";
            print STDERR "$msg\n";
            debug($msg);
        }

        if (defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
            $current_dump_server_crc_size == $source_crc_size and
            $current_dump->{'server_crc'} ne $source_crc) {
            $msg = "recovery failed: server-crc ($current_dump->{'server_crc'}) and source_crc ($source_crc) differ",
            print STDERR "$msg\n";
            debug($msg);
        }

        if (defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
            defined $dest_native_crc and $current_dump->{'native_crc'} ne $dest_native_crc) {
            $msg = "recovery failed: native-crc ($current_dump->{'native_crc'}) and dest-native-crc ($dest_native_crc) differ";
            print STDERR "$msg\n";
            debug($msg);
        }

	my $dump = shift @{$plan->{'dumps'}};
	if (!$dump) {
	    return $steps->{'quit'}->();
	}

	$steps->{'check_dumpfile'}->($dump);
    };

    step quit => sub {
	my ($err) = @_;

	if ($err) {
	    $exit_code = 1;
	    print STDERR $err, "\n";
	} elsif ($all_success) {
	    print "All images successfully validated\n";
	} else {
	    print "Some images failed to be correctly validated.\n";
	    $exit_code = 1;
	}

	return $steps->{'quit2'}->();
    };

    step quit2 => sub {
	my ($storage_name) = keys %clerk;
	if ($storage_name) {
	    my $clerk = $clerk{$storage_name};
	    delete $clerk{$storage_name};
	    return $clerk->quit(finished_cb => $steps->{'quit2'});
	}
	return $finished_cb->();
    };

}

main(sub { Amanda::MainLoop::quit(); });
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit($exit_code);
