#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use Data::Dumper;

my $exit_status = 0;

package Amanda::Ambackupd::Message;
use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 5000000) {
        return "DLE $self->{'diskname'}";
    } else {
        return "No mesage for code '$self->{'code'}'";
    }
}

##
# ClientService class

package main::ClientService;
use base 'Amanda::ClientService';

use Symbol;
use IPC::Open3;
use POSIX qw(WIFEXITED WEXITSTATUS strftime);
use FileHandle;
use XML::Simple;
use Fcntl;
use Sys::Hostname;
use JSON -convert_blessed_universally;

use Amanda::MainLoop qw( :GIOCondition );
use Amanda::MainLoop;
use Amanda::Debug qw( debug info warning );
use Amanda::Util qw( :constants );
use Amanda::Feature;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Cmdline;
use Amanda::Paths;
use Amanda::Disklist;
use Amanda::Util qw( match_disk match_host quote_string );
use Amanda::Xfer qw( :constants );
use Amanda::Logfile qw( :logtype_t log_add log_add_full make_chunker_stats log_start_multiline log_end_multiline);
use Amanda::Chunker::Scribe;
use Amanda::Holding;
use Amanda::Curinfo;

# Note that this class performs its control IO synchronously.  This is adequate
# for this service, as it never receives unsolicited input from the remote
# system.

sub run {
    my $self = shift;
    my $finished_cb = \&Amanda::MainLoop::quit;

    $self->{'my_features'} = Amanda::Feature::Set->mine();
    $self->{'their_features'} = Amanda::Feature::Set->old();

    $self->setup_streams($finished_cb);
}

sub setup_streams {
    my $self = shift;
    my $finished_cb = shift;

    my $backup = 0;
    my $disk;

    # always started from amandad.
    my $req = $self->get_req();

    my $peer = $ENV{'AMANDA_AUTHENTICATED_PEER'};
    # make some sanity checks
    my $errors = [];
    if (defined $req->{'options'}{'auth'} and defined $self->amandad_auth()
	    and $req->{'options'}{'auth'} ne $self->amandad_auth()) {
	my $reqauth = $req->{'options'}{'auth'};
	my $amauth = $self->amandad_auth();
	push @$errors, "ambackup program requested auth '$reqauth', " .
		       "but amandad is using auth '$amauth'";
	$main::exit_status = 1;
    }

    if (!defined $req->{'options'}{'config'}) {
	push @$errors, "no config";
	$main::exit_status = 1;
    } else {
	$self->{'config'} = $req->{'options'}{'config'};
	config_init($CONFIG_INIT_EXPLICIT_NAME, $self->{'config'});
	my ($cfgerr_level, @cfgerr_errors) = config_errors();
	if ($cfgerr_level >= $CFGERR_ERRORS) {
	    push @$errors, "configuration errors; aborting connection";
	    $main::exit_status = 1;
	}
	Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER_PREFERRED);

	# and the disklist
	my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
	$cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
	if ($cfgerr_level >= $CFGERR_ERRORS) {
	    push @$errors, "Errors processing disklist";
	    $main::exit_status = 1;
	    return;
	}
    }

    # and pull out the features, if given
    if (defined($req->{'features'})) {
	$self->{'their_features'} = $req->{'features'};
    }

    my $rep = '';
    my @result_messages;
    my $hostname = $peer;
    my $cmd_line = $req->{'lines'}[1];
    if (!$cmd_line) {
	push @$errors, "No command specified in REQ packet";
    } else {
	my ($cmd, $data) = split(' ', $cmd_line);
	if ($cmd eq "LISTDLE") {
	    debug("LISTDLE command for host '$hostname'");
	    my $host = Amanda::Disklist::get_host($hostname);
	    if ($host) {
		my $matched = 0;
		for my $diskname (@{$host->{'disks'}}) {
		    my $disk = Amanda::Disklist::get_disk($hostname, $diskname);
		    if ($disk and
			dumptype_getconf($disk->{'config'}, $DUMPTYPE_STRATEGY) != $DS_SKIP and
			dumptype_getconf($disk->{'config'}, $DUMPTYPE_PROGRAM) eq "APPLICATION") {
			push @result_messages, Amanda::Ambackupd::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 5000000,
				severity        => $Amanda::Message::INFO,
				host            => $hostname,
				diskname        => $diskname,
				device          => $disk->{'device'});
			$rep .= "DLE " . Amanda::Util::quote_string($diskname) . "\n";
			$matched = 1;
		    }
		}
		if (!$matched) {
		    push @$errors, "No dle for host '$hostname'";
		}
	    } else {
		push @$errors, "No host '$hostname'";
	    }
	} elsif ($cmd eq "CHECK") {
	    debug("CHECK command for host '$hostname'");
	    my $host = Amanda::Disklist::get_host($hostname);
	    if ($host) {
		my $matched = 0;
		for my $diskname (@{$host->{'disks'}}) {
		    my $disk = Amanda::Disklist::get_disk($hostname, $diskname);
		    if ($disk and
			dumptype_getconf($disk->{'config'}, $DUMPTYPE_STRATEGY) != $DS_SKIP and
			dumptype_getconf($disk->{'config'}, $DUMPTYPE_PROGRAM) eq "APPLICATION") {
			if ($self->{'their_features'}->has($Amanda::Feature::fe_req_xml)) {
			    my $program = dumptype_getconf($disk->{'config'},
							   $DUMPTYPE_PROGRAM);
			    my $o = $disk->xml_optionstr();
			    $rep .= "<dle>\n";
			    $rep .= "  <program>APPLICATION</program>\n";
			    $rep .= $disk->xml_application($self->{'their_features'});
			    $rep .= $disk->xml_estimate($self->{'their_features'}) . "\n";
			    $rep .= "  " . Amanda::Util::amxml_format_tag("disk", $disk->{'name'}) . "\n";
			    if ($disk->{'device'}) {
				$rep .= "  " . Amanda::Util::amxml_format_tag("diskdevice", $disk->{'device'}) . "\n";
			    }
			    $rep .= $o;
			    $rep .= "</dle>";
			} else {
			    #$rep .= "DLE " . Amanda::Util::quote_string($diskname) . "\n";
			}
			$matched = 1;
		    }
		}
		if ($matched) {
		    $rep = "GOPTIONS features=" . $self->{'my_features'}->as_string() .
			   ";maxdumps=$host->{'maxdumps'}" .
			   ";hostname=$hostname" .
			   ";config=$req->{'options'}{'config'}\n" .
			   $rep;
		} else {
		    push @$errors, "No dle for host '$hostname'";
		}
	    } else {
		push @$errors, "No host '$hostname'";
	    }
	} elsif ($cmd eq "BACKUP") {
	    my $diskname = $data;
	    debug("BACKUP command for host: $hostname disk: $diskname");
	    $self->{'hostname'} = $hostname;
	    $self->{'diskname'} = $diskname;
	    $self->{'qdiskname'} = Amanda::Util::quote_string($diskname);
	    my $host = Amanda::Disklist::get_host($hostname);
	    if ($host) {
		my $matched = 0;
		$disk = Amanda::Disklist::get_disk($hostname, $diskname);
		if ($disk and
		    dumptype_getconf($disk->{'config'}, $DUMPTYPE_STRATEGY) != $DS_SKIP and
		    dumptype_getconf($disk->{'config'}, $DUMPTYPE_PROGRAM) eq "APPLICATION") {
		    if ($self->{'their_features'}->has($Amanda::Feature::fe_req_xml)) {
			my $program = dumptype_getconf($disk->{'config'},
						       $DUMPTYPE_PROGRAM);
			my $o = $disk->xml_optionstr();
			my $connline;
			if ($self->{'their_features'}->has($Amanda::Feature::fe_sendbackup_stream_state)) {
			    $connline = $self->connect_streams('DATA' => 'rw',
							       'MESG' => 'rw',
							       'INDEX' => 'rw',
							       'STATE' => 'rw');
			} else {
			    $connline = $self->connect_streams('DATA' => 'rw',
							       'MESG' => 'rw',
							       'INDEX' => 'rw');
			}
			$rep .= "$connline\n";
			$rep .= "GOPTIONS features=" . $self->{'my_features'}->as_string() .
			   ";maxdumps=$host->{'maxdumps'}" .
			   ";hostname=$hostname" .
			   ";config=$req->{'options'}{'config'}\n";
			my $dle .= "<dle>\n";
			$dle .= "  <program>APPLICATION</program>\n";
			$dle .= $disk->xml_application($self->{'their_features'});
			$dle .= "  " . Amanda::Util::amxml_format_tag("disk", $disk->{'name'}) . "\n";
			if ($disk->{'device'}) {
			    $dle .= "  " . Amanda::Util::amxml_format_tag("diskdevice", $disk->{'device'}) . "\n";
			}
			$dle .= "  <level>0</level>\n";
			$dle .= $o;
			$dle .= "</dle>";
			$rep .= $dle;
			$self->{'dle_str'} = $dle;
			$backup = 1;
		    } else {
		    }
		    $matched = 1;
		}
		if (!$matched) {
		    push @$errors, "No '$diskname' dle for host '$hostname'";
		}
	    } else {
		push @$errors, "No host '$hostname'";
	    }
	} else {
	    push @$errors, "Invalid command '$cmd' specified in REQ packet";
	}
    }
  if (@result_messages) {
    if (@$errors || @{$req->{'errors'}}) {
	for my $err (@$errors) {
	    $rep .= "ERROR $err\n";
	}
	for my $err (@{$req->{'errors'}}) {
	    $rep .= "ERROR $err\n";
	}
	$rep .= "\n";
    }
  } else {
    if (@$errors || @{$req->{'errors'}}) {
	for my $err (@$errors) {
	    $rep .= "ERROR $err\n";
	}
	for my $err (@{$req->{'errors'}}) {
	    $rep .= "ERROR $err\n";
	}
	$rep .= "\n";
    }
  }

  if (@result_messages) {
    my $json = JSON->new->allow_nonref->convert_blessed;
    my $reply = $json->pretty->encode(\@result_messages);;
    $self->senddata('main', $reply);
  } else {
    $self->senddata('main', $rep);
  }
    $self->close('main', 'w');

    if ($backup) {
	return $self->do_backup($rep, $disk, $finished_cb);
    } else {
	$finished_cb->();
    }
}

sub do_backup {
    my $self = shift;
    my $rep = shift;
    my $disk = shift;
    my $finished_cb = shift;
    my $file_to_close = 0;

    my $src_mesg;
    my $mesg_buf;
    my ($xfer_mesg, $xfer_src_mesg, $xfer_dest_mesg);
    my ($xfer_data, $xfer_src_data, $xfer_dest_data);
    my ($xfer_index, $xfer_src_index, $xfer_dest_index);
    my ($xfer_state, $xfer_src_state, $xfer_dest_state);
    my $xfer_compress_data;
    my $xfer_encrypt_data;
    my @now = localtime;
    my $timestamp = strftime "%Y%m%d%H%M%S", @now;
    my $trace_log_filename;
    my $amdump_log_pathname;
    my $amdump_log;
    my $logdir = getconf($CNF_LOGDIR);
    my $index;
    my $state;
    $self->{'mesg_fh'} = FileHandle->new();
    $self->{'mesg_fh'}->fdopen($self->wfd('MESG'), "w");
    $self->{'mesg_fh'}->autoflush(1);

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$timestamp = Amanda::Logfile::make_logname("ambackupd", $timestamp);
	$self->{'timestamp'} = $timestamp;
	$trace_log_filename = Amanda::Logfile::get_logname();
	debug("beginning trace log: $trace_log_filename");
	log_add($L_START, "date $timestamp");
	my $hostname = hostname;
	log_add($L_STATS, "hostname $hostname");
	log_add($L_DISK, "$self->{'hostname'} $self->{'qdiskname'}");

	$self->{'hdr'} = Amanda::Header->new();
	$self->{'hdr'}->{'type'} = $Amanda::Header::F_DUMPFILE;
	$self->{'hdr'}->{'datestamp'} = $timestamp;
	$self->{'hdr'}->{'name'} = $self->{'hostname'};
	$self->{'hdr'}->{'disk'} = $self->{'diskname'};
	$self->{'hdr'}->{'blocksize'} = Amanda::Holding::DISK_BLOCK_BYTES;
	$self->{'hdr'}->{'dumplevel'} = 0;
	#$self->{'hdr'}->{'compressed'} =
	#$self->{'hdr'}->{'encrypted'} =
	$self->{'hdr'}->{'dle_str'} = $self->{'dle_str'};

	my $mesg_handle = FileHandle->new();
	$mesg_handle->fdopen($self->rfd('MESG'), "r");

	$src_mesg = Amanda::MainLoop::fd_source($mesg_handle,
				 $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
#	$file_to_close++;
	$src_mesg->set_callback(sub {
	    my $buf;
	    my $blocksize = -1;
	    $blocksize = sysread($mesg_handle, $buf, 32768);
	    if ($blocksize) {
		$self->{'mesg'} .= $buf;
		$mesg_buf .= $buf;
		while ($mesg_buf =~ /\n/gm) {
		    my ($line, $b) = split ("\n", $mesg_buf, 2);
		    debug("mesg: $line");
		    if ($line =~ /^sendbackup: /) {
			if ($line =~ /^sendbackup: info/) {
			    if ($line eq "sendbackup: info end") {
				$steps->{'start_backup'}->();
			    } elsif ($line =~ /^sendbackup: info BACKUP=(.*)/) {
				$self->{'hdr'}->{'program'} = $1;
			    } elsif ($line =~ /^sendbackup: info APPLICATION=(.*)/) {
				$self->{'hdr'}->{'application'} = $1;
			    } elsif ($line =~ /^sendbackup: info RECOVER_CMD=(.*)/) {
				$self->{'hdr'}->{'recover_cmd'} = $1;
			    } elsif ($line =~ /^sendbackup: info COMPRESS_SUFFIX=(.*)/) {
				$self->{'hdr'}->{'comp_suffix'} = $1;
			    } elsif ($line =~ /^sendbackup: info SERVER_CUSTOM_COMPRESS=(.*)/) {
				$self->{'hdr'}->{'srvcompprog'} = $1;
			    } elsif ($line =~ /^sendbackup: info CLIENT_CUSTOM_COMPRESS=(.*)/) {
				$self->{'hdr'}->{'clntcompprog'} = $1;
			    } elsif ($line =~ /^sendbackup: info SERVER_ENCRYPT=(.*)/) {
				$self->{'hdr'}->{'srv_encrypt'} = $1;
			    } elsif ($line =~ /^sendbackup: info CLIENT_ENCRYPT=(.*)/) {
				$self->{'hdr'}->{'clnt_encrypt'} = $1;
			    } elsif ($line =~ /^sendbackup: info SERVER_DECRYPT_OPTION=(.*)/) {
				$self->{'hdr'}->{'srv_decrypt_opt'} = $1;
			    } elsif ($line =~ /^sendbackup: info CLIENT_DECRYPT_OPTION=(.*)/) {
				$self->{'hdr'}->{'clnt_decrypt_opt'} = $1;
			    }
			} elsif ($line =~ /^sendbackup: size (.*)/) {
			    $self->{'orig_size'} = 0+$1;
			    $self->{'got_size'} = 1;
			} elsif ($line =~ /^sendbackup: end/) {
			    $self->{'got_end'} = 1;
			} elsif ($line =~ /^sendbackup: native-CRC (.*)/) {
			    $self->{'native_crc'} = $1;
			} elsif ($line =~ /^sendbackup: client-CRC (.*)/) {
			    $self->{'client_crc'} = $1;
			} elsif ($line =~ /^sendbackup: start/) {
			} elsif ($line =~ /^sendbackup: no-op/) {
			} elsif ($line =~ /^sendbackup: state/) {
			    # TODO
			    die("inline state is Unsupported");
			} elsif ($line =~ /^sendbackup: retry/) {
			    $line =~ /.*delay (\d*)/;
			    $self->{'retry_delay'} = $1;
			    $line =~ /.*level (\d*)/;
			    $self->{'retry_level'} = $1;
			    $line =~ /.*message (.*)/;
			    $self->{'retry_message'} = $1;
			    if ($self->{'retry_level'}) {
				# TODO Add a command to force the level on next run
			    }
			    if ($self->{'retry_message'}) {
				print {$self->{'mesg_fh'}} "$self->{'retry_message'}\n";
			    }
			    if ($self->{'retry_delay'} &&
				$self->{'retry_level'}) {
				print {$self->{'mesg_fh'}} "Retry the backup at level $self->{'retry_level'} in $self->{'retry_delay'} seconds\n";
			    } elsif ($self->{'retry_delay'}) {
				print {$self->{'mesg_fh'}} "Retry the backup in $self->{'retry_delay'} seconds\n";
			    } elsif ($self->{'retry_level'}) {
				print {$self->{'mesg_fh'}} "Retry the backup at level $self->{'retry_level'}\n";
			    }
			} elsif ($line =~ /^sendbackup: warning/) {
			    $self->{'strange'}++;
			} elsif ($line =~ /^sendbackup: error \[(.*)\]/) {
			    $self->{'error_message'} = $1;
			    $self->{'backup_failed'} = 1;
			} elsif ($line =~ /^sendbackup: error (.*)/) {
			    $self->{'error_message'} = $1;
			    $self->{'backup_failed'} = 1;
			} else {
			    debug("XX: $line");
			}
		    } elsif ($line =~ /^\|/) {
			$self->{'normal'}++;
		    } elsif ($line =~ /^\? (.*)/) {
			$self->{'strange'}++;
			$self->{'strange_line'} = $1 if !defined $self->{'strange_line'};
		    } else {
			$self->{'error'}++;
		    }
		    $mesg_buf = $b;
		}
	    } else {
		if (!defined $blocksize) {
		    debug("read from mesg: $!");
		}
		if ($mesg_buf) {
		    debug("unprocessed MESG stream: $mesg_buf");
		}

		close $mesg_handle;
		return $steps->{'abort_mesg_close'}->() if !$self->{'got_end'};
		$steps->{'mesg_close'};
	    }
	});
    };

    step start_backup => sub {
	my $xml = XML::Simple->new();
	my $dle_xml = $xml->XMLin($self->{'dle_str'});
	$self->{'dle_xml'} = $dle_xml;
	my @data_compress;
	my @data_encrypt;

	print {$self->{'mesg_fh'}} "start backup: " . $self->{'qdiskname'} . "\n";
	$index = dumptype_getconf($disk->{'config'}, $DUMPTYPE_INDEX);

	my $compress = dumptype_getconf($disk->{'config'}, $DUMPTYPE_COMPRESS);
	if ($compress != $COMP_NONE) {
	    $self->{'hdr'}->{'compressed'} = 1;
	    if ($compress == $COMP_SERVER_CUST) {
		$self->{'hdr'}->{'srvcompprog'} = dumptype_getconf($disk->{'config'}, $DUMPTYPE_SRVCOMPPROG);
		$self->{'hdr'}->{'uncompress_cmd'} = " " . $self->{'hdr'}->{'srvcompprog'} . " -d |";
		$self->{'hdr'}->{'comp_suffix'} = "cust";
		push @data_compress, $self->{'hdr'}->{'srvcompprog'};
	    } elsif ($compress == $COMP_CUST) {
		$self->{'hdr'}->{'clntcompprog'} = dumptype_getconf($disk->{'config'}, $DUMPTYPE_CLNTCOMPPROG);
		$self->{'hdr'}->{'uncompress_cmd'} = " " . $self->{'hdr'}->{'clntcompprog'} . " -d |";
		$self->{'hdr'}->{'comp_suffix'} = "cust";
	    } elsif ($compress == $COMP_SERVER_BEST) {
		$self->{'hdr'}->{'uncompress_cmd'} = " $Amanda::Constants::UNCOMPRESS_PATH $Amanda::Constants::UNCOMPRESS_OPT |";
		$self->{'hdr'}->{'comp_suffix'} = $Amanda::Constants::COMPRESS_SUFFIX;
		push @data_compress, $Amanda::Constants::COMPRESS_PATH, $Amanda::Constants::COMPRESS_BEST_OPT;
	    } elsif ($compress == $COMP_SERVER_FAST) {
		$self->{'hdr'}->{'uncompress_cmd'} = " $Amanda::Constants::UNCOMPRESS_PATH $Amanda::Constants::UNCOMPRESS_OPT |";
		$self->{'hdr'}->{'comp_suffix'} = $Amanda::Constants::COMPRESS_SUFFIX;
		push @data_compress, $Amanda::Constants::COMPRESS_PATH, $Amanda::Constants::COMPRESS_FAST_OPT;
	    }
	} elsif ($self->{'hdr'}->{'comp_suffix'}) {
	    $self->{'hdr'}->{'compressed'} = 1;
	} else {
	    $self->{'hdr'}->{'compressed'} = 0;
	    $self->{'hdr'}->{'comp_suffix'} = "N";
	}

	my $encrypt = dumptype_getconf($disk->{'config'}, $DUMPTYPE_ENCRYPT);
	if ($encrypt != $ENCRYPT_NONE) {
	    $self->{'hdr'}->{'encrypted'} = 1;
	    $self->{'hdr'}->{'encrypt_suffix'} = "enc";
	    if ($encrypt ==  $ENCRYPT_SERV_CUST) {
		$self->{'hdr'}->{'srv_encrypt'} = dumptype_getconf($disk->{'config'}, $DUMPTYPE_SRV_ENCRYPT);
		$self->{'hdr'}->{'srv_decrypt_opt'} = dumptype_getconf($disk->{'config'}, $DUMPTYPE_SRV_DECRYPT_OPT);
		$self->{'hdr'}->{'decrypt_cmd'} = " " . $self->{'hdr'}->{'srv_encrypt'} . " " . $self->{'hdr'}->{'srv_decrypt_opt'} . " |";
		push @data_encrypt, $self->{'hdr'}->{'srv_encrypt'};
	    } elsif ($encrypt == $ENCRYPT_CUST) {
		$self->{'hdr'}->{'clnt_encrypt'} = dumptype_getconf($disk->{'config'}, $DUMPTYPE_CLNT_ENCRYPT);
		$self->{'hdr'}->{'clnt_decrypt_opt'} = dumptype_getconf($disk->{'config'}, $DUMPTYPE_CLNT_DECRYPT_OPT);
		$self->{'hdr'}->{'decrypt_cmd'} = " " . $self->{'hdr'}->{'clnt_encrypt'} . " " . $self->{'hdr'}->{'clnt_decrypt_opt'} . " |";
	    }
	} else {
	    $self->{'hdr'}->{'encrypted'} = 0;
	    $self->{'hdr'}->{'encrypt_suffix'} = "N";
	}
	$self->write_header_to_index();

	$xfer_src_data = Amanda::Xfer::Source::Fd->new($self->rfd('DATA'));
	POSIX::close($self->rfd('DATA'));
	$self->{'chunker_scribe'} = Amanda::Chunker::Scribe->new(
				feedback => $self,
				debug => $Amanda::Config::debug_chunker);
	$self->{'chunker_scribe'}->start(write_timestamp => $self->{'timestamp'});
	my @xfer_link_data;
	push @xfer_link_data, $xfer_src_data;

	if (@data_compress) {
	    $xfer_compress_data = Amanda::Xfer::Filter::Process->new(\@data_compress, 0, 0, 0, 0);
	    push @xfer_link_data, $xfer_compress_data;
	}

	if (@data_encrypt) {
	    $xfer_encrypt_data = Amanda::Xfer::Filter::Process->new(\@data_encrypt, 0, 0, 0, 0);
	    push @xfer_link_data, $xfer_encrypt_data;
	}

	$xfer_dest_data = $self->{'chunker_scribe'}->get_xfer_dest(max_memory => 1048576);
	push @xfer_link_data, $xfer_dest_data;
	$xfer_data = Amanda::Xfer->new(\@xfer_link_data);

	my $xfer_compress_index;
	if ($index) {
	    if (getconf($CNF_COMPRESS_INDEX)) {
		$self->{'index_filename'} = Amanda::Logfile::getindex_unsorted_gz_fname(
						$self->{'hostname'},
						$self->{'diskname'},
						$self->{'timestamp'},
						$self->{'hdr'}->{'dumplevel'});
	    } else {
		$self->{'index_filename'} = Amanda::Logfile::getindex_unsorted_fname(
						$self->{'hostname'},
						$self->{'diskname'},
						$self->{'timestamp'},
						$self->{'hdr'}->{'dumplevel'});
	    }
	    $self->{'index_filename_tmp'} = $self->{'index_filename'} . ".tmp";
	    open(INDEX, ">$self->{'index_filename_tmp'}") || die("C");
	    $xfer_src_index = Amanda::Xfer::Source::Fd->new($self->rfd('INDEX'));
	    POSIX::close($self->rfd('INDEX'));
	    $xfer_dest_index = Amanda::Xfer::Dest::Fd->new(fileno(INDEX));
	    if (getconf($CNF_COMPRESS_INDEX)) {
		$xfer_compress_index = Amanda::Xfer::Filter::Process->new(
			[$Amanda::Constants::COMPRESS_PATH,
			 $Amanda::Constants::COMPRESS_BEST_OPT],
			 0, 0, 0, 0);
		$xfer_index = Amanda::Xfer->new([$xfer_src_index, $xfer_compress_index, $xfer_dest_index]);
	    } else {
		$xfer_index = Amanda::Xfer->new([$xfer_src_index, $xfer_dest_index]);
	    }
	}

	my $xfer_compress_state;
	if ($self->{'their_features'}->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	    $self->{'state_filename'} = Amanda::Logfile::getstatefname(
						$self->{'hostname'},
						$self->{'diskname'},
						$self->{'timestamp'},
						$self->{'hdr'}->{'dumplevel'});
	    $self->{'state_filename_gz'} = $self->{'state_filename'} . $Amanda::Constants::COMPRESS_SUFFIX;
	    open(STATE, ">$self->{'state_filename_gz'}") || die("C");
	    $xfer_src_state = Amanda::Xfer::Source::Fd->new($self->rfd('STATE'));
	    POSIX::close($self->rfd('STATE'));
	    $xfer_compress_state = Amanda::Xfer::Filter::Process->new(
		[$Amanda::Constants::COMPRESS_PATH,
		 $Amanda::Constants::COMPRESS_BEST_OPT],
		0, 0, 0, 0);
	    $xfer_dest_state = Amanda::Xfer::Dest::Fd->new(fileno(STATE));
	    $xfer_state = Amanda::Xfer->new([$xfer_src_state, $xfer_compress_state, $xfer_dest_state]);
	}


	$file_to_close++;
	$xfer_data->start(sub {
	    my ($src, $msg, $xfer) = @_;

	    $self->{'chunker_scribe'}->handle_xmsg($src, $msg, $xfer);
	    if ($msg->{'type'} == $XMSG_DONE) {
		$file_to_close--;
		return $steps->{'backup_done'}->() if $file_to_close == 0;
	    } elsif ($msg->{'type'} == $XMSG_ERROR) {
		$self->{'backup_failed'} = 1;
		$steps->{'add_to_mesg'}("$msg->{'message'}");
	    } elsif ($msg->{'type'} == $XMSG_CANCEL) {
		$self->{'backup_failed'} = 1;
	    } elsif ($msg->{'type'} == $XMSG_CRC) {
		if ($msg->{'elt'} == $xfer_dest_data) {
		    $self->{'server_crc'} = $msg->{'crc'}.":".$msg->{'size'};
		}
	    }
	});

	if ($index) {
	    if (defined $xfer_compress_index && $xfer_compress_index->can('get_stderr_fd')) {
		$file_to_close++;
		my $fd = $xfer_compress_index->get_stderr_fd();
		$fd.="";
		$fd = int($fd);
		my $src = Amanda::MainLoop::fd_source($fd,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
		$src->set_callback( sub {
		    my $b;
		    my $n_read = POSIX::read($fd, $b, 1);
		    if (!defined $n_read) {
			return;
		    } elsif ($n_read == 0) {
			$src->remove();
			POSIX::close($fd);
			$file_to_close--;
			return $steps->{'backup_done'}->() if $file_to_close == 0;
		    } else {
			$self->{'index_buffer'} .= $b;
			if ($b eq "\n") {
			    my $line = $self->{'index_buffer'};
			    chomp $line;
			    if (length($line) > 1) {
				$steps->{'add_to_mesg'}("index compression stderr: $line");
				debug("index compression stderr: $line");
			    }
			    $self->{'index_buffer'} = "";
			}
		    }
		});
	    }

	    $file_to_close++;
	    $xfer_index->start(sub {
		my ($src, $msg, $xfer) = @_;

		if ($msg->{'type'} == $XMSG_DONE) {
		    $file_to_close--;
		    return $steps->{'backup_done'}->() if $file_to_close == 0;
		}
	    });
	}
	if ($self->{'their_features'}->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	    if (defined $xfer_compress_state && $xfer_compress_state->can('get_stderr_fd')) {
		$file_to_close++;
		my $fd = $xfer_compress_state->get_stderr_fd();
		$fd.="";
		$fd = int($fd);
		my $src = Amanda::MainLoop::fd_source($fd,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
		$src->set_callback( sub {
		    my $b;
		    my $n_read = POSIX::read($fd, $b, 1);
		    if (!defined $n_read) {
			return;
		    } elsif ($n_read == 0) {
			$src->remove();
			POSIX::close($fd);
			$file_to_close--;
			return $steps->{'backup_done'}->() if $file_to_close == 0;
		    } else {
			$self->{'state_buffer'} .= $b;
			if ($b eq "\n") {
			    my $line = $self->{'state_buffer'};
			    chomp $line;
			    if (length($line) > 1) {
				$steps->{'add_to_mesg'}("state compression stderr: $line");
				debug("state compression stderr: $line");
			    }
			    $self->{'state_buffer'} = "";
			}
		    }
		});
	    }

	    $file_to_close++;
	    $xfer_state->start(sub {
		my ($src, $msg, $xfer) = @_;

		if ($msg->{'type'} == $XMSG_DONE) {
		    $file_to_close--;
		    # empty compressed file have a sizeof 20
		    if (-s $self->{'state_filename_gz'} == 20) {
			unlink($self->{'state_filename_gz'});
		    }
		    return $steps->{'backup_done'}->() if $file_to_close == 0;
		}
	    });
	}

	if ($xfer_compress_data) {
	    if ($xfer_compress_data->can('get_stderr_fd')) {
		$file_to_close++;
		my $fd = $xfer_compress_data->get_stderr_fd();
		$fd.="";
		$fd = int($fd);
		my $src = Amanda::MainLoop::fd_source($fd,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
		$src->set_callback( sub {
		    my $b;
		    my $n_read = POSIX::read($fd, $b, 1);
		    if (!defined $n_read) {
			return;
		    } elsif ($n_read == 0) {
			$src->remove();
			POSIX::close($fd);
			$file_to_close--;
			return $steps->{'backup_done'}->() if $file_to_close == 0;
		    } else {
			$self->{'compress_buffer'} .= $b;
			if ($b eq "\n") {
			    my $line = $self->{'compress_buffer'};
			    chomp $line;
			    if (length($line) > 1) {
				$steps->{'add_to_mesg'}("server data compression stderr: $line");
				debug("server data compression stderr: $line");
			    }
			    $self->{'compress_buffer'} = "";
			}
		    }
		});
	    }
	}

	if ($xfer_encrypt_data) {
	    if ($xfer_encrypt_data->can('get_stderr_fd')) {
		$file_to_close++;
		my $fd = $xfer_encrypt_data->get_stderr_fd();
		$fd.="";
		$fd = int($fd);
		my $src = Amanda::MainLoop::fd_source($fd,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
		$src->set_callback( sub {
		    my $b;
		    my $n_read = POSIX::read($fd, $b, 1);
		    if (!defined $n_read) {
			return;
		    } elsif ($n_read == 0) {
			$src->remove();
			POSIX::close($fd);
			$file_to_close--;
			return $steps->{'backup_done'}->() if $file_to_close == 0;
		    } else {
			$self->{'encrypt_buffer'} .= $b;
			if ($b eq "\n") {
			    my $line = $self->{'encrypt_buffer'};
			    chomp $line;
			    if (length($line) > 1) {
				$steps->{'add_to_mesg'}->("server data encryption stderr: $line");
				debug("server data encryption stderr: $line");
			    }
			    $self->{'encrypt_buffer'} = "";
			}
		    }
		});
	    }
	}

	$self->{'first_holding_file'} = $self->{'holding_file'} = $self->get_holding_file();
	$self->{'chunker_scribe'}->start_dump(
		xfer        => $xfer_data,
		dump_header => $self->{'hdr'},
		dump_cb     => $steps->{'dump_cb'},
		filename    => $self->{'holding_file'},
		use_bytes   => $self->{'hdisk'}->{'avail'} * 1024,
		chunk_size  => $self->{'hdisk'}->{'chunksize'} * 1024);
    };

    step backup_done => sub {
	$self->{'backup_done'} = 1;
	$steps->{'backup_done1'}->();
    };

    step backup_done1 => sub {

	if ($self->{'backup_failed'}) {
	    $steps->{'remove'}->();
	} else {
	    return if !$self->{'backup_done'} || !$self->{'got_end'};
	    $steps->{'backup_good'}->();
	}
    };

    step backup_good => sub {
	$self->write_header_to_index();
	if ($index) {
	    rename $self->{'index_filename_tmp'}, $self->{'index_filename'};
	}

	Amanda::Holding::rename_tmp($self->{'first_holding_file'}, 1);
	$self->rewrite_holding_header();

	# set infofile
	my $infodir = getconf($CNF_INFOFILE);
	my $ci = Amanda::Curinfo->new($infodir);
	my $info = $ci->get_info($self->{'hostname'}, $self->{'diskname'});
	$info->update_dumper($self->{'orig_size'}, $self->{'dump_size'}, $self->{'dump_time'}, $self->{'hdr'}->{'dumplevel'}, $self->{'timestamp'});
	$ci->put_info($self->{'hostname'}, $self->{'diskname'}, $info);
	$self->{'dump_time'} = 1 if !$self->{'dump_time'};
	my $kb = $self->{'dump_size'} / 1024;
	my $time = $self->{'dump_time'};
	my $kps = "$kb" / $time;
	if ($self->{'strange'}) {
	    print {$self->{'mesg_fh'}} "backup strange " . Amanda::Util::quote_string($self->{'diskname'}) . ": " . $self->{'strange_line'} . "\n";
	    log_start_multiline();
	    log_add_full($L_STRANGE, "dumper", "$self->{'hostname'} $self->{'qdiskname'} $self->{'hdr'}->{'dumplevel'} $self->{'native_crc'} $self->{'client_crc'} [sec $self->{'dump_time'} kb $kb kps $kps orig-kb $self->{'orig_size'}]");
	    my @lines = split /\n/, $self->{'mesg'};
	    foreach my $line (@lines) {
		log_add_full($L_STRANGE, "dumper", $line);
	    }
	    log_end_multiline();
	} else {
	    print {$self->{'mesg_fh'}} "backup done: " . Amanda::Util::quote_string($self->{'diskname'}) . "\n";
	    log_add_full($L_SUCCESS, "dumper", "$self->{'hostname'} $self->{'qdiskname'} $self->{'timestamp'} $self->{'hdr'}->{'dumplevel'} $self->{'native_crc'} $self->{'client_crc'} $self->{'server_crc'} [sec $self->{'dump_time'} kb $kb kps $kps orig-kb $self->{'orig_size'}]");
	}
	log_add_full($L_SUCCESS, "chunker", "$self->{'hostname'} $self->{'qdiskname'} $self->{'timestamp'} $self->{'hdr'}->{'dumplevel'} $self->{'server_crc'} [sec $self->{'dump_time'} kb $kb kps $kps]");
	$steps->{'quit'}->();
	print {$self->{'mesg_fh'}} "MESG END\n";
	close $self->{'mesg_fh'};
    };

    step dump_cb => sub {
	my %params = @_;

	debug("dump_cb: " . Data::Dumper::Dumper(\%params));
	if ($params{'result'} eq 'DONE') {
	    $self->{'dump_size'} = $params{'data_size'};
	    $self->{'dump_time'} = $params{'total_duration'};
	} elsif ($params{'result'} eq 'PARTIAL') {
	} elsif ($params{'result'} eq 'FAILED') {
	    $self->{'chunker_failed'} = "CHUNKER ERROR MESSAGE" if defined $self->{'chunker_failed'};  #JLM
	}
    };

    step abort_mesg_close => sub {
	debug("mesg closed prematurely");
	$steps->{'remove'}->();
    };

    step mesg_close => sub {
	debug("mesg closed");
	if ($self->{'got_end'} and !$self->{'backup_failed'}) {
	    $steps->{'backup_done1'}->();
	} else {
	    $steps->{'remove'}->();
	}
    };

    step remove => sub {
	# remove index file
	unlink $self->{'index_filename_tmp'};

	# remove header in index
	my $header_filename = Amanda::Logfile::getheaderfname(
						$self->{'hostname'},
						$self->{'diskname'},
						$self->{'timestamp'},
						$self->{'hdr'}->{'dumplevel'});
	unlink $header_filename;

	# holding files
	Amanda::Holding::filetmp_unlink($self->{'first_holding_file'}.".tmp");
	Amanda::Holding::dir_unlink(); # should handle 'pid' files.

	log_start_multiline();
	log_add_full($L_FAIL, "dumper", "$self->{'hostname'} $self->{'qdiskname'} $self->{'timestamp'} $self->{'hdr'}->{'dumplevel'} [$self->{'error_message'}]");
	my @lines = split /\n/, $self->{'mesg'};
	foreach my $line (@lines) {
	    log_add_full($L_STRANGE, "dumper", $line);
	}
	log_end_multiline();

	$self->{'dump_time'} = 1 if !$self->{'dump_time'};
	my $kb = $self->{'dump_size'} / 1024;
	my $time = $self->{'dump_time'};
	my $kps = "$kb" / $time;
	if (defined $self->{'chunker_failed'}) {
	    log_add_full($L_FAIL, "chunker", sprintf("%s %s %s %s %s",
		quote_string($self->{'hostname'}.""), # " is required for SWIG..
		$self->{'qdiskname'},
		$self->{'timestamp'},
		$self->{'hdr'}->{'dumplevel'},
		"[$self->{'chunker_failed'}]"));
	} else {
	    log_add_full($L_SUCCESS, "chunker",
		sprintf("%s%s %s %s %s [sec %s kb %s kps %s]",
			quote_string($self->{'hostname'}.""),
			$self->{'qdiskname'},
			$self->{'timestamp'},
			$self->{'hdr'}->{'dumplevel'},
			$self->{'server_crc'},
			$self->{'dump_time'},
			$kb,
			$kps));
	}

	$exit_status = 1;
	my $errmsg = $self->{'error_message'};
	$errmsg .= ", ".$self->{'chunker_failed'} if defined $self->{'chunker_failed'};
	print {$self->{'mesg_fh'}} "backup failed: " . Amanda::Util::quote_string($self->{'diskname'}) . ": " . $errmsg . "\n";
	print {$self->{'mesg_fh'}} "MESG END\n";
	close $self->{'mesg_fh'};
	$steps->{'quit'}->();
    };

    step quit => sub {
	log_add($L_FINISH, "date $self->{'timestamp'} time $self->{'dump_time'}");
	log_add($L_INFO, "pid-done $$");
	my @report = ("$sbindir/amreport", $self->{'config'}, '--from-amdump', '-l', $trace_log_filename);
	system(@report);
	$finished_cb->();
    };

    step add_to_mesg => sub {
	my $msg = shift;

	debug("add_to_mesg: $msg");
	print {$self->{'mesg_fh'}} "$msg\n";
    };

}

sub get_holding_file {
    my $self = shift;
    debug("get_holding_file");

    if (!$self->{'hdisks'}) {
	for my $hdname (@{getconf($CNF_HOLDINGDISK)}) {
	    my $cfg = lookup_holdingdisk($hdname);
	    next unless defined $cfg;

	    my $dir = holdingdisk_getconf($cfg, $HOLDING_DISKDIR);
	    next unless defined $dir;
	    next unless -d $dir;

	    my $hdisk;
	    $hdisk->{'dir'} = $dir;
	    $hdisk->{'disksize'} = holdingdisk_getconf($cfg, $HOLDING_DISKSIZE);
	    $hdisk->{'chunksize'} = holdingdisk_getconf($cfg, $HOLDING_CHUNKSIZE);

	    $hdisk->{'avail'} = Amanda::Util::get_fsusage($dir);
	    if ($hdisk->{'disksize'} >= 0) {
		if ($hdisk->{'disksize'} < $hdisk->{'avail'}) {
		    $hdisk->{'avail'} = $hdisk->{'disksize'};
		}
	    } else {
		$hdisk->{'avail'} += $hdisk->{'disksize'};
	    }

	    push @{$self->{'hdisks'}}, $hdisk;
	 }
    }
    debug("hdisks: " . Data::Dumper::Dumper($self->{'hdisks'}));

    foreach my $hdisk (@{$self->{'hdisks'}}) {
	if ($hdisk->{'avail'} > 0) {
	    $self->{'hdisk'} = $hdisk;
	    return $hdisk->{'dir'} . "/" .
		   $self->{'timestamp'} . "/" .
		   $self->{'hostname'} . "." .
		   Amanda::Util::sanitise_filename($self->{'diskname'}) . "." .
		   $self->{'hdr'}->{'dumplevel'};
	}
    }
    delete $self->{'hdisk'};
    return undef;
}

##
## Chunker Scribe feedback

sub request_more_disk {
    my $self = shift;
    debug("request_more_disk");

    $self->{'hdisk'}->{'avail'} = 0;
    $self->{'holding_file'} = $self->get_holding_file();

    if ($self->{'holding_file'}) {
	$self->{'chunker_scribe'}->continue_chunk(
				filename => $self->{'holding_file'},
				use_bytes => $self->{'hdisk'}->{'avail'} * 1024,
				chunk_size => $self->{'hdisk'}->{'chunksize'} * 1024);
    } else {
	$self->{'chunker_failed'} = "No more holding disk space";
	debug($self->{'chunker_failed'});
	print {$self->{'mesg_fh'}} "$self->{'chunker_failed'}\n";
	$self->{'error_message'} = "chunker failed" if !defined $self->{'error_message'};
	$self->{'backup_failed'} = 1;
	my $quit_cb = sub {
	};
	$self->{'chunker_scribe'}->quit(finished_cb => $quit_cb);
    }
}

sub notify_no_room {
    my $self = shift;
    my $use_bytes = shift;
    my $mesg = shift;

    debug("notify_no_room $use_bytes");
    $self->{'hdisk'}->{'avail'} = 0;
}

sub scribe_notif_log_info {
    my $self = shift;
    my %params = @_;

    log_add_full($L_INFO, "chunker", $params{'message'});
    debug("scribe_notif_log_info: $params{'message'}");
}

sub scribe_notif_done {
}

sub rewrite_holding_header {
    my $self = shift;

    my $holding_fh;
    open $holding_fh, "+<$self->{'first_holding_file'}";
    my $block;
    sysread($holding_fh, $block, 31768);
    my $hdr = Amanda::Header->new($block);

    $hdr->{'orig_size'} = $self->{'orig_size'};
    $hdr->{'native_crc'} = $self->{'native_crc'};
    $hdr->{'client_crc'} = $self->{'client_crc'};
    $hdr->{'server_crc'} = $self->{'server_crc'};

    $block = $hdr->to_string(0, 32768);
    seek $holding_fh, 0, Fcntl::SEEK_SET;
    print $holding_fh $block;
    close $holding_fh;
}

sub write_header_to_index {
    my $self = shift;

    my $header_filename = Amanda::Logfile::getheaderfname($self->{'hostname'},
					     $self->{'diskname'},
					     $self->{'timestamp'},
					     $self->{'hdr'}->{'dumplevel'});
    my $header_fh;
    open $header_fh, ">$header_filename";
    my $block = $self->{'hdr'}->to_string(0, 32768);
    print $header_fh $block;
    close $header_fh;
}


sub get_req {
    my $self = shift;

    my $req_str = '';
    while (1) {
	my $buf = Amanda::Util::full_read($self->rfd('main'), 1024);
	last unless $buf;
	$req_str .= $buf;
    }
    # we've read main to EOF, so close it
    $self->close('main', 'r');

    return $self->{'req'} = $self->parse_req($req_str);
}


# helper function to write a data to a stream.  This does not add newline characters.
sub senddata {
    my $self = shift;
    my ($stream, $data) = @_;
    my $fd = $self->wfd($stream);

    Amanda::Util::full_write($fd, $data, length($data))
	    or die "writing to $stream: $!";
}

# send a line on the control stream, or just log it if the ctl stream is gone;
# async callback is just like for senddata
sub sendctlline {
    my $self = shift;
    my ($msg) = @_;

    if ($self->{'ctl_stream'}) {
	debug("CTL >> $msg");
	return $self->senddata($self->{'ctl_stream'}, $msg . "\n");
    } else {
	debug("not sending CTL message as CTL is closed >> $msg");
    }
}

##
# main driver

package main;
use Amanda::Debug qw( debug );
use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );
use Getopt::Long;

my $fd1_in;
my $fd1_out;
my $fd2_in;
my $fd2_out;
my $fd3_in;
my $fd3_out;

Amanda::Util::setup_application("ambackupd", "server", $CONTEXT_DAEMON, "amanda", "amanda");
config_init(0, undef);

Amanda::Debug::debug_dup_stderr_to_debug();

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version'   => \&Amanda::Util::version_opt,
    'fd1-in=s'  => \$fd1_in,
    'fd1-out=s' => \$fd1_out,
    'fd2-in=s'  => \$fd2_in,
    'fd2-out=s' => \$fd2_out,
    'fd3-in=s'  => \$fd3_in,
    'fd3-out=s' => \$fd3_out
) or usage();

sub main {

    my $cs = main::ClientService->new();

    $cs->run();
}

Amanda::MainLoop::call_later(\&main);
Amanda::MainLoop::run();
debug("exiting with $exit_status");
Amanda::Util::finish_application();
exit($exit_status);
