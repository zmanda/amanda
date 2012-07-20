#! @PERL@
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

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Changer;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Header;
use Amanda::Holding;
use Amanda::Cmdline;
use Amanda::Tapelist;
use Amanda::Xfer qw( :constants );

sub usage {
    my ($msg) = @_;
    print STDERR "$msg\n" if $msg;
    print STDERR <<EOF;
Usage: amrestore [--config config] [-b blocksize] [-r|-c|-C] [-p] [-h]
    [-f filenum] [-l label] [-o configoption]*
    {device | [--holding] holdingfile}
    [hostname [diskname [datestamp [hostname [diskname [datestamp ... ]]]]]]"));
EOF
    exit(1);
}

##
# main

Amanda::Util::setup_application("amrestore", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);

my ($opt_config, $opt_blocksize, $opt_raw, $opt_compress, $opt_compress_best,
    $opt_pipe, $opt_header, $opt_filenum, $opt_label, $opt_holding, $opt_restore_src);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'config=s' => \$opt_config,
    'holding' => \$opt_holding,
    'b=i' => \$opt_blocksize,
    'r' => \$opt_raw,
    'c' => \$opt_compress,
    'C' => \$opt_compress_best,
    'p' => \$opt_pipe,
    'h' => \$opt_header,
    'f=i' => \$opt_filenum,
    'l=s' => \$opt_label,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

$opt_compress = 1 if $opt_compress_best;

# see if we have a holding file or a device
usage("Must specify a device or holding-disk file") unless (@ARGV);
$opt_restore_src = shift @ARGV;
if (!$opt_holding) {
    $opt_holding = 1
	if (Amanda::Holding::get_header($opt_restore_src));
}

my @opt_dumpspecs = Amanda::Cmdline::parse_dumpspecs([@ARGV],
    $Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP);

usage("Cannot check a label on a holding-disk file")
    if ($opt_holding and $opt_label);
usage("Cannot use both -r (raw) and -c/-C (compression) -- use -h instead")
    if ($opt_raw and $opt_compress);

# -r implies -h, plus appending ".RAW" to filenames
$opt_header = 1 if $opt_raw;

set_config_overrides($config_overrides);
if ($opt_config) {
    config_init($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
} else {
    config_init(0, undef);
}
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $exit_status = 0;
my $res;

sub failure {
    my ($msg, $finished_cb) = @_;
    print STDERR "ERROR: $msg\n";
    $exit_status = 1;
    if ($res) {
	$res->release(finished_cb => sub {
	    # ignore error
	    $finished_cb->();
	});
    } else {
	$finished_cb->();
    }
}

sub main {
    my ($finished_cb) = @_;

    my $dev;
    my $hdr;
    my $chg;
    my $filenum = $opt_filenum;
    $filenum = 1 if (!$filenum);
    $filenum = 0 + "$filenum"; # convert to integer
    my %all_filter;
    my $restore_done;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg };

    step start => sub {
	# first, return to the original working directory we were started in
	if (!chdir Amanda::Util::get_original_cwd()) {
	    return failure("Cannot chdir to original working directory", $finished_cb);
	}

	if ($opt_holding) {
	    $steps->{'read_header'}->();
	} else {
	    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	    my $tl = Amanda::Tapelist->new($tlf);
	    $chg = Amanda::Changer->new($opt_restore_src, tapelist => $tl);
	    if ($chg->isa("Amanda::Changer::Error")) {
		return failure($chg, $finished_cb);
	    }

	    $chg->load(relative_slot => "current", mode => "read",
		res_cb => $steps->{'slot_loaded'});
	}
    };

    step slot_loaded => sub {
	(my $err, $res) = @_;
	return failure($err, $finished_cb) if $err;

	$dev = $res->{'device'};

	if ($opt_blocksize) {
	    if ( !$dev->property_set("BLOCK_SIZE", $opt_blocksize)) {
		return failure($dev->error_or_status, $finished_cb);
	    }

	    # re-read the label with the correct blocksize
	    $dev->read_label();
	}

	if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    return failure($dev->error_or_status, $finished_cb);
	}

	$steps->{'check_label'}->();
    };

    step check_label => sub {
	if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    return failure($dev->error_or_status, $finished_cb);
	}

	if ($opt_label) {
	    if ($dev->volume_label ne $opt_label) {
		my $got = $dev->volume_label;
		return failure("Found unexpected label '$got'", $finished_cb);
	    }
	}

	my $lbl = $dev->volume_label;
	print STDERR "Restoring from tape $lbl starting with file $filenum.\n";

	$steps->{'start_device'}->();
    };

    step start_device => sub {
	if (!$dev->start($ACCESS_READ, undef, undef)) {
	    return failure($dev->error_or_status(), $finished_cb);
	}

	$steps->{'read_header'}->();
    };

    step read_header => sub {
	if ($opt_holding) {
	    print STDERR "Reading from '$opt_restore_src'\n";
	    $hdr = Amanda::Holding::get_header($opt_restore_src);
	} else {
	    $hdr = $dev->seek_file($filenum);
	    if (!$hdr) {
		return failure("while reading next header: " . $dev->error_or_status(),
			    $finished_cb);
	    } elsif ($hdr->{'type'} == $Amanda::Header::F_TAPEEND) {
		return $steps->{'finished'}->();
	    }

	    # seek_file may have skipped ahead; plan accordingly
	    $filenum = $dev->file + 1;
	}

	$steps->{'filter_dumpspecs'}->();
    };

    step filter_dumpspecs => sub {
	if (@opt_dumpspecs and not $hdr->matches_dumpspecs([@opt_dumpspecs])) {
	    if (!$opt_holding) {
		my $dev_filenum = $dev->file;
		print STDERR "amrestore: $dev_filenum: skipping ",
			$hdr->summary(), "\n";
	    }

	    # skip to the next file without restoring this one
	    return $steps->{'next_file'}->();
	}

	if (!$opt_holding) {
	    my $dev_filenum = $dev->file;
	    print STDERR "amrestore: $dev_filenum: restoring ";
	}
	print STDERR $hdr->summary(), "\n";

	$steps->{'xfer_dumpfile'}->();
    };

    step xfer_dumpfile => sub {
	my ($src, $dest);

	# set up the source..
	if ($opt_holding) {
	    $src = Amanda::Xfer::Source::Holding->new($opt_restore_src);
	} else {
	    $src = Amanda::Xfer::Source::Device->new($dev);
	}

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
	    if ($hdr->{'partnum'} > 0) {
		$filename .= sprintf(".%07d", $hdr->{'partnum'});
	    }

	    # add an appropriate suffix
	    if ($opt_raw) {
		$filename .= ".RAW";
	    } elsif ($opt_compress) {
		$filename .= ($hdr->{'compressed'} && $hdr->{'comp_suffix'})?
		    $hdr->{'comp_suffix'} : $Amanda::Constants::COMPRESS_SUFFIX;
	    }

	    if (!open($dest_fh, ">", $filename)) {
		return failure("Could not open '$filename' for writing: $!", $finished_cb);
	    }
	}
	$dest = Amanda::Xfer::Dest::Fd->new($dest_fh);

	# set up any filters that need to be applied, decryption first
	my @filters;
	if ($hdr->{'encrypted'} and not $opt_raw) {
	    if ($hdr->{'srv_encrypt'}) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srv_encrypt'}, $hdr->{'srv_decrypt_opt'} ], 0);
	    } elsif ($hdr->{'clnt_encrypt'}) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clnt_encrypt'}, $hdr->{'clnt_decrypt_opt'} ], 0);
	    } else {
		return failure("could not decrypt encrypted dump: no program specified",
			    $finished_cb);
	    }

	    $hdr->{'encrypted'} = 0;
	    $hdr->{'srv_encrypt'} = '';
	    $hdr->{'srv_decrypt_opt'} = '';
	    $hdr->{'clnt_encrypt'} = '';
	    $hdr->{'clnt_decrypt_opt'} = '';
	    $hdr->{'encrypt_suffix'} = 'N';
	}
	if (!$opt_raw and $hdr->{'compressed'} and not $opt_compress) {
	    # need to uncompress this file

	    if ($hdr->{'srvcompprog'}) {
		# TODO: this assumes that srvcompprog takes "-d" to decompress
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'srvcompprog'}, "-d" ], 0);
	    } elsif ($hdr->{'clntcompprog'}) {
		# TODO: this assumes that clntcompprog takes "-d" to decompress
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $hdr->{'clntcompprog'}, "-d" ], 0);
	    } else {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $Amanda::Constants::UNCOMPRESS_PATH,
			  $Amanda::Constants::UNCOMPRESS_OPT ], 0);
	    }
	    
	    # adjust the header
	    $hdr->{'compressed'} = 0;
	    $hdr->{'uncompress_cmd'} = '';
	} elsif (!$opt_raw and !$hdr->{'compressed'} and $opt_compress) {
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
	if ($opt_header) {
	    $hdr->{'blocksize'} = Amanda::Holding::DISK_BLOCK_BYTES;
	    $dest_fh->syswrite($hdr->to_string(32768, 32768));
	}

	# start reading all filter stderr
	foreach my $filter (@filters) {
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
		    if (!%all_filter and $restore_done) {
			$finished_cb->();
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
	
	my $xfer = Amanda::Xfer->new([ $src, @filters, $dest ]);
	my $got_err = undef;
	$xfer->get_source()->set_callback(sub {
	    my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_INFO) {
		Amanda::Debug::info($msg->{'message'});
	    } elsif ($msg->{'type'} == $XMSG_ERROR) {
		$got_err = $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		$src->remove();
		$steps->{'xfer_done'}->($got_err);
	    }
	});
	$xfer->start();
    };

    step xfer_done => sub {
	my ($err) = @_;
	return failure($err, $finished_cb) if $err;

	$steps->{'next_file'}->('extracted');
    };

    step next_file => sub {
	my ($extracted) = @_;
	# amrestore does not loop over multiple files when reading from holding
	# when outputting to a pipe amrestore extracts only the first file
	if ($opt_holding or ($opt_pipe and $extracted)) {
	    return $steps->{'finished'}->();
	}

	# otherwise, try to read the next header from the device
	$steps->{'read_header'}->();
    };

    step finished => sub {
	if ($res) {
	    $res->release(finished_cb => $steps->{'quit'});
	} else {
	    $steps->{'quit'}->();
	}
    };

    step quit => sub {
	my ($err) = @_;
	$res = undef;
	$restore_done = 1;
	return failure($err, $finished_cb) if $err;

	if (!%all_filter) {
	    $finished_cb->();
	}
    };
}
main(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
Amanda::Util::finish_application();
exit $exit_status;
