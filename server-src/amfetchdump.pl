#! @PERL@
# Copyright (c) 2009, 2010 Zmanda, Inc.  All Rights Reserved.
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
use warnings;

use Getopt::Long;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Header;
use Amanda::Holding;
use Amanda::Cmdline;
use Amanda::Xfer qw( :constants );
use Amanda::Recovery::Planner;
use Amanda::Recovery::Clerk;

sub usage {
    my ($msg) = @_;
    print STDERR <<EOF;
Usage: amfetchdump [-c|-C|-l] [-p|-n] [-a]
    [-O directory] [-d device] [-o configoption]*
    [--header-file file] [--header-fd fd]
    hostname [diskname [datestamp [hostname [diskname [datestamp ... ]]]]]"));
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
    $opt_header_file, $opt_header_fd, @opt_dumpspecs);
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
    'b=s' => \$opt_blocksize,
    'd=s' => \$opt_device,
    'O=s' => \$opt_chdir,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();
usage() unless (@ARGV);
$opt_config = shift @ARGV;

$opt_compress = 1 if $opt_compress_best;

usage("must specify at least a hostname") unless @ARGV;
@opt_dumpspecs = Amanda::Cmdline::parse_dumpspecs([@ARGV],
    $Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP);

usage("The -b option is no longer supported; set readblocksize in the tapetype section\n" .
      "of amanda.conf instead.")
    if ($opt_blocksize);
usage("-l is not compatible with -c or -C")
    if ($opt_leave and $opt_compress);
usage("-p is not compatible with -n")
    if ($opt_leave and $opt_no_reassembly);
usage("-h, --header-file, and --header-fd are mutually incompatible")
    if (($opt_header and $opt_header_file or $opt_header_fd)
	    or ($opt_header_file and $opt_header_fd));

config_init($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
apply_config_overrides($config_overrides);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $exit_status = 0;
my $clerk;
sub failure {
    my ($msg) = @_;
    print STDERR "ERROR: $msg\n";
    $exit_status = 1;
    if ($clerk) {
	$clerk->quit(finished_cb => sub { Amanda::MainLoop::quit(); });
    } else {
	Amanda::MainLoop::quit();
    }
}

package main::Feedback;

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

sub notif_part {
    my $self = shift;
    my ($label, $filenum, $header) = @_;

    print STDERR "amfetchdump: $filenum: restoring ", $header->summary(), "\n";
}

sub notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    print STDERR "Reading '$filename'\n", $header->summary(), "\n";
}

sub volume_not_found {
    my $self = shift;
    my ($err, $label, $res_cb) = @_;

    # $self->{chg} is either the same changer that the Clerk just used to find
    # the device *or* the device give by the user in the -d command-line
    # option.  Either way, we want to prompt the user to insert the volume,
    # wait for a response, and then try to load it.  Note that the blocking
    # wait-for-enter here is not problematic in this case, but bad form all the
    # same.
    my $dev_name = $self->{'dev_name'} || "changer";
    print STDERR "$err\n";
    print STDERR "Insert volume labeled '$label' in $dev_name\n";
    print STDERR "and press enter, or ^D to abort.\n";
    my $resp = <STDIN>;

    if (!defined $resp) { # meaning ^D or EOF
	return $res_cb->("Aborted by user");
    }

    # this is a little bit tricky, because we want to trap any errors and
    # re-prompt the user, instead of failing completely.
    return $self->{'chg'}->load(
	label => $label,
	res_cb => make_cb(res_cb => sub {
	    my ($err, $res) = @_;
	    if ($err) {
		# prompt again, possibly with a new error
		return $self->volume_not_found($err, $label, $res_cb);
	    }

	    # call through to the clerk's callback
	    return $res_cb->($err, $res);
	}));
}

package main;

sub main {
    my %subs;
    my $current_dump;
    my $plan;
    my @xfer_errs;

    $subs{'start'} = make_cb(start => sub {
	my $chg;

	# first, go to opt_directory or the original working directory we
	# were started in
	my $destdir = $opt_chdir || Amanda::Util::get_original_cwd();
	if (!chdir($destdir)) {
	    return failure("Cannot chdir to $destdir: $!");
	}

	# if we have an explicit device, then the clerk doesn't get a changer --
	# we operate the changer via volume_not_found
	if (defined $opt_device) {
	    $chg = Amanda::Changer->new($opt_device);
	    return failure($chg) if $chg->isa("Amanda::Changer::Error");

	    $clerk = Amanda::Recovery::Clerk->new(
		feedback => main::Feedback->new($chg, $opt_device));
	} else {
	    $chg = Amanda::Changer->new();
	    return failure($chg) if $chg->isa("Amanda::Changer::Error");

	    $clerk = Amanda::Recovery::Clerk->new(
		changer => $chg,
		feedback => main::Feedback->new($chg, undef));
	}

	# planner gets to plan against the same changer the user specified
	Amanda::Recovery::Planner::make_plan(
	    dumpspecs => [ @opt_dumpspecs ],
	    changer => $chg,
	    plan_cb => $subs{'plan_cb'},
	    $opt_no_reassembly? (one_dump_per_part => 1) : ());
    });

    $subs{'plan_cb'} = make_cb(plan_cb => sub {
	(my $err, $plan) = @_;
	return failure($err) if $err;

	if (!@{$plan->{'dumps'}}) {
	    return failure("No matching dumps found");
	}

	my @needed_labels = $plan->get_volume_list();
	my @needed_holding = $plan->get_holding_file_list();
	if (@needed_labels) {
	    print STDERR (scalar @needed_labels), " volume(s) needed for restoration\n";
	    print STDERR "The following volumes are needed: ",
		join(" ", map { $_->{'label'} } @needed_labels ), "\n";
	}
	if (@needed_holding) {
	    print STDERR (scalar @needed_holding), " holding file(s) needed for restoration\n";
	    for my $hf (@needed_holding) {
		print "  $hf\n";
	    }
	}

	unless ($opt_assume) {
	    print STDERR "Press enter when ready\n";
	    my $resp = <STDIN>;
	}

	$subs{'start_dump'}->();
    });

    $subs{'start_dump'} = make_cb(start_dump => sub {
	$current_dump = shift @{$plan->{'dumps'}};
	if (!$current_dump) {
	    return $subs{'finished'}->();
	}

	$clerk->get_xfer_src(
	    dump => $current_dump,
	    xfer_src_cb => $subs{'xfer_src_cb'});
    });

    $subs{'xfer_src_cb'} = make_cb(xfer_src_cb => sub {
	my ($errs, $hdr, $xfer_src) = @_;
	return failure(join("; ", @$errs)) if $errs;

	# and set up the destination..
	my $dest_fh;
	if ($opt_pipe) {
	    $dest_fh = \*STDOUT;
	} else {
	    my $filename = sprintf("%s.%s.%s.%d",
		    $hdr->{'name'},
		    Amanda::Util::sanitise_filename("".$hdr->{'disk'}), # workaround SWIG bug
		    $hdr->{'datestamp'},
		    $hdr->{'dumplevel'});
	    if ($current_dump->{'nparts'} > 1) {
		$filename .= sprintf(".%07d", $hdr->{'partnum'});
	    }

	    # add an appropriate suffix
	    if ($opt_compress) {
		$filename .= ($hdr->{'compressed'} && $hdr->{'comp_suffix'})?
		    $hdr->{'comp_suffix'} : $Amanda::Constants::COMPRESS_SUFFIX;
	    }

	    if (!open($dest_fh, ">", $filename)) {
		return failure("Could not open '$filename' for writing: $!");
	    }
	}

	my $xfer_dest = Amanda::Xfer::Dest::Fd->new($dest_fh);

	# set up any filters that need to be applied
	my @filters;
	if ($hdr->{'compressed'} and not $opt_compress and not $opt_leave) {
	    # need to uncompress this file

	    if ($hdr->{'srvcompprog'}) {
		# TODO: this assumes that srvcompprog takes "-d" to decrypt
		@filters = (
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srvcompprog'}, "-d" ], 0)
		);
	    } elsif ($hdr->{'clntcompprog'}) {
		# TODO: this assumes that clntcompprog takes "-d" to decrypt
		@filters = (
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clntcompprog'}, "-d" ], 0)
		);
	    } else {
		@filters = (
		    Amanda::Xfer::Filter::Process->new(
			[ $Amanda::Constants::UNCOMPRESS_PATH,
			  $Amanda::Constants::UNCOMPRESS_OPT ], 0),
		);
	    }

	    # adjust the header
	    $hdr->{'compressed'} = 0;
	    $hdr->{'uncompress_cmd'} = '';
	} elsif (!$hdr->{'compressed'} and $opt_compress and not $opt_leave) {
	    # need to compress this file

	    my $compress_opt = $opt_compress_best?
		$Amanda::Constants::COMPRESS_BEST_OPT :
		$Amanda::Constants::COMPRESS_FAST_OPT;
	    @filters = (
		Amanda::Xfer::Filter::Process->new(
		    [ $Amanda::Constants::COMPRESS_PATH,
		      $compress_opt ], 0),
	    );

	    # adjust the header
	    $hdr->{'compressed'} = 1;
	    $hdr->{'uncompress_cmd'} = " $Amanda::Constants::UNCOMPRESS_PATH " .
		"$Amanda::Constants::UNCOMPRESS_OPT |";
	    $hdr->{'comp_suffix'} = $Amanda::Constants::COMPRESS_SUFFIX;
	}

	# write the header to the destination if requested
	$hdr->{'blocksize'} = Amanda::Holding::DISK_BLOCK_BYTES;
	if (defined $opt_header or defined $opt_header_file or defined $opt_header_fd) {
	    my $hdr_fh = $dest_fh;
	    if (defined $opt_header_file) {
		open($hdr_fh, ">", $opt_header_file)
		    or return failure("could not open '$opt_header_file': $!");
	    } elsif (defined $opt_header_fd) {
		open($hdr_fh, "<&".($opt_header_fd+0))
		    or return failure("could not open fd $opt_header_fd: $!");
	    }
	    print $hdr_fh $hdr->to_string(32768, 32768);
	}

	my $xfer = Amanda::Xfer->new([ $xfer_src, @filters, $xfer_dest ]);
	$xfer->start($subs{'handle_xmsg'});
	$clerk->start_recovery(
	    xfer => $xfer,
	    recovery_cb => $subs{'recovery_cb'});
    });

    $subs{'handle_xmsg'} = make_cb(handle_xmsg => sub {
	my ($src, $msg, $xfer) = @_;

	$clerk->handle_xmsg($src, $msg, $xfer);
	if ($msg->{'type'} == $XMSG_INFO) {
	    Amanda::Debug::info($msg->{'message'});
	} elsif ($msg->{'type'} == $XMSG_ERROR) {
	    push @xfer_errs, $msg->{'message'};
	}
    });

    $subs{'recovery_cb'} = make_cb(recovery_cb => sub {
	my %params = @_;

	@xfer_errs = (@xfer_errs, @{$params{'errors'}})
	    if $params{'errors'};
	return failure(join("; ", @xfer_errs)) if @xfer_errs;

	return failure("recovery failed")
	    if $params{'result'} ne 'DONE';

	$subs{'start_dump'}->();
    });

    $subs{'finished'} = make_cb(finished => sub {
	if ($clerk) {
	    $clerk->quit(finished_cb => $subs{'quit'});
	} else {
	    $subs{'quit'}->();
	}
    });

    $subs{'quit'} = make_cb(quit => sub {
	my ($err) = @_;

	return failure($err) if $err;

	Amanda::MainLoop::quit();
    });

    $subs{'start'}->();
    Amanda::MainLoop::run();
    exit $exit_status;
}

main();
