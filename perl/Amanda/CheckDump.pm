#! @PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::CheckDump::Message;
use strict;
use warnings;

use Amanda::Message;
use Amanda::Debug;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2700000) {
	return "Could not find any matching dumps.";
    } elsif ($self->{'code'} == 2700001) {
	return sprintf("You will need the following volume%s: %s",
		       (@{$self->{'labels'}} > 1) ? "s" : "" ,
		       join(", ", map { $_->{'label'} } @{$self->{'labels'}}));
    } elsif ($self->{'code'} == 2700002) {
	return sprintf("You will need the following holding file%s: %s",
		       (@{$self->{'holdings'}} > 1) ? "s" : "",
		       join(", ", @{$self->{'holdings'}}));
    } elsif ($self->{'code'} == 2700003) {
	return "Reading volume $self->{'label'} file $self->{'filenum'}";
    } elsif ($self->{'code'} == 2700004) {
	return "Reading '$self->{'filename'}'";
    } elsif ($self->{'code'} == 2700005) {
	return "Validating image " . $self->{hostname} . ":" .
	    $self->{diskname} . " dumped " . $self->{dump_timestamp} . " level ".
	    $self->{level} . ($self->{'nparts'} > 1 ? " ($self->{nparts} parts)" : "");
    } elsif ($self->{'code'} == 2700006) {
	return "All images successfully validated";
    } elsif ($self->{'code'} == 2700007) {
	return "Some images failed to be correctly validated.";
    } elsif ($self->{'code'} == 2700008) {
	return "While reading from volumes:\n" . join("\n", @{$self->{'errors'}});
    } elsif ($self->{'code'} == 2700009) {
	return "Validation errors:\n" . join("\n", @{$self->{'errors'}});
    } elsif ($self->{'code'} == 2700010) {
	return "recovery failed: native-crc in header ($self->{'hdr_native_crc'}) and native-crc in log ($self->{'log_native_crc'}) differ";
    } elsif ($self->{'code'} == 2700011) {
	return "recovery failed: client-crc in header ($self->{'hdr_client_crc'}) and client-crc in log ($self->{'log_client_crc'}) differ";
    } elsif ($self->{'code'} == 2700012) {
	return "recovery failed: server-crc in header ($self->{'hdr_server_crc'}) and server-crc in log ($self->{'log_server_crc'}) differ";
    } elsif ($self->{'code'} == 2700013) {
	return "recovery failed: server-crc ($self->{'log_server_crc'}) and source_crc ($self->{'source_crc'}) differ";
    } elsif ($self->{'code'} == 2700014) {
	return "recovery failed: native-crc ($self->{'log_native_crc'}) and dest-native-crc ($self->{'dest_native_crc'}) differ";
    } elsif ($self->{'code'} == 2700015) {
	return "ERROR: XML error in header";
    } elsif ($self->{'code'} == 2700016) {
	return "filter stderr: $self->{'line'}";
    } elsif ($self->{'code'} == 2700017) {
	return "could not decrypt encrypted dump: no program specified";
    } elsif ($self->{'code'} == 2700018) {
	return "Running a CheckDump";
    } elsif ($self->{'code'} == 2700019) {
	return "Failed to fork the CheckDump process";
    } elsif ($self->{'code'} == 2700020) {
	return "The message filename is '$self->{'message_filename'}'";
    } elsif ($self->{'code'} == 2700021) {
	return "Can't open message file '$self->{'message_filename'}' for writting: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 2700022) {
	return "No 'message_filename' specified";
    } elsif ($self->{'code'} == 2700023) {
	return "Can't open message file '$self->{'message_filename'}' for reading: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 2700024) {
    } elsif ($self->{'code'} == 2700025) {
    } elsif ($self->{'code'} == 2700026) {
    } elsif ($self->{'code'} == 2700027) {
    } elsif ($self->{'code'} == 2700028) {
    } elsif ($self->{'code'} == 2700029) {
    } elsif ($self->{'code'} == 2700030) {
    }
}


package Amanda::CheckDump;
use strict;
use warnings;

use POSIX qw(strftime);
use File::Temp;
use File::Basename;
use XML::Simple;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Disklist;
use Amanda::Debug qw( :logging debug );
use Amanda::Xfer qw( :constants );
use Amanda::Header qw( :constants );
use Amanda::MainLoop;
use Amanda::Util qw( quote_string );
use Amanda::DB::Catalog;
use Amanda::Recovery::Planner;
use Amanda::Recovery::Scan;
use Amanda::Recovery::Clerk;
use Amanda::Taper::Scan;
use Amanda::Taper::Scribe qw( get_splitting_args_from_config );
use Amanda::Storage qw( :constants );
use Amanda::Changer qw( :constants );
use Amanda::Cmdline;
use Amanda::Paths;
use Amanda::Logfile qw( :logtype_t log_add log_add_full
			log_rename $amanda_log_trace_log make_stats );
use Amanda::Util qw ( match_datestamp match_level );
use MIME::Base64 ();

use base qw(
    Amanda::Recovery::Clerk::Feedback
    Amanda::Taper::Scribe::Feedback
);

sub new {
    my $class = shift;
    my %params = @_;

    my @result_messages;

    my $self = bless {
	timestamp => $params{'timestamp'},
	config_name => $params{'config_name'},
	user_msg => $params{'user_msg'},
	is_tty => $params{'is_tty'},
	delay => $params{'delay'},
	verbose => $params{'verbose'},
	src => undef,
	dst => undef,
	cleanup => {},
	message_count => 0,

	config_overrides_opts => $params{'config_overrides_opts'},

    }, $class;

    $self->{'delay'} = 15000 if !defined $self->{'delay'};
    $self->{'exit_code'} = 0;
    $self->{'amlibexecdir'} = 0;

    my $logdir = $self->{'logdir'} = getconf($CNF_LOGDIR);
    my @now = localtime;
    my $run_timestamp = strftime "%Y%m%d%H%M%S", @now;
    $self->{'pid'} = $$;
    $run_timestamp = Amanda::Logfile::make_logname("checkdump", $run_timestamp);
    $self->{'run_timestamp'} = $run_timestamp;
    $self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
    debug("beginning trace log: $self->{'trace_log_filename'}");
    $self->{'checkdump_log_filename'} = "checkdump.$run_timestamp";
    $self->{'checkdump_log_pathname'} = "$logdir/checkdump.$run_timestamp";

    # Must be opened in append so that all subprocess can write to it.
    if (!open($self->{'message_file'}, ">>", $self->{'checkdump_log_pathname'})) {
	push @result_messages, Amanda::CheckDump::Message->new(
                source_filename  => __FILE__,
                source_line      => __LINE__,
                code             => 2700021,
                message_filename => $self->{'checkdump_log_pathname'},
                errno            => $!,
                severity         => $Amanda::Message::ERROR);
    } else {
	log_add($L_INFO, "message file $self->{'checkdump_log_filename'}");
    }

    return $self, \@result_messages;
}

sub user_msg {
    my $self = shift;
    my $msg = shift;

    if (defined $self->{'message_file'}) {
	my $d;
	if (ref $msg eq "SCALAR") {
	    $d = Data::Dumper->new([$msg], ["MESSAGES[$self->{'message_count'}]"]);
	} else {
	    my %msg_hash = %$msg;
	    $d = Data::Dumper->new([\%msg_hash], ["MESSAGES[$self->{'message_count'}]"]);
	}
	print {$self->{'message_file'}} $d->Dump();
	$self->{'message_count'}++;
    }
    if (defined $self->{'user_msg'}) {
	$self->{'user_msg'}->($msg);
    }
}

sub clerk_notif_part {
    my $self = shift;
    my ($label, $filenum, $header) = @_;

    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700003,
		                severity         => $Amanda::Message::INFO,
				label           => $label,
				filenum         => "$filenum"+0));
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700004,
		                severity         => $Amanda::Message::INFO,
				filename        => $filename));
}

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
		    user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700015,
		                severity         => $Amanda::Message::ERROR));
		    debug("XML Error: $@\n$dle_str");
		}
		if (defined $dle->{'diskdevice'} and UNIVERSAL::isa( $dle->{'diskdevice'}, "HASH" )) {
		    $dle->{'diskdevice'} = MIME::Base64::decode($dle->{'diskdevice'}->{'raw'});
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

sub run {
    my $self = shift;
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
	$timestamp = $self->{'timestamp'};
	$timestamp = Amanda::DB::Catalog::get_latest_write_timestamp()
	    unless defined $timestamp;

	# make a changer
	my ($storage) = Amanda::Storage->new(tapelist => $tapelist);
	return  $steps->{'quit'}->($storage) if $storage->isa("Amanda::Message");
	$storage{$storage->{"storage_name"}} = $storage;
	my $chg = $storage->{'chg'};
	return $steps->{'quit'}->($chg) if $chg->isa("Amanda::Message");

	# make an interactivity plugin
	$interactivity = Amanda::Interactivity->new(
				name => $storage->{'interactivity'});

	# make a scan
	my $scan = Amanda::Recovery::Scan->new(
			    chg => $chg,
			    interactivity => $interactivity);
	return $steps->{'quit'}->($scan)
	    if $scan->isa("Amanda::Changer::Error");
	$scan{$storage->{"storage_name"}} = $scan;

	# make a clerk
	my $clerk = Amanda::Recovery::Clerk->new(
	    feedback => $self,
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
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700000,
		                severity         => $Amanda::Message::ERROR));
	    return $steps->{'quit'}->();
	}

	if (@tapes) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700001,
				severity	=> $Amanda::Message::INFO,
				labels          => \@tapes));
	}
	if (@holding) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700002,
				holdings        => \@holding));
	}

	# nothing else is going on right now, so a blocking "Press enter.." is OK
	if ($self->{'is_tty'}) {
	    print "Press enter when ready\n";
	    <STDIN>;
	}

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

	$self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700005,
				hostname        => $dump->{hostname},
				diskname        => $dump->{diskname},
				dump_timestamp  => $dump->{dump_timestamp},
				level           => "$dump->{level}"+0,
				nparts          => $dump->{nparts}));

	my $storage_name = $dump->{'storage'};
	if (!$storage{$storage_name}) {
	    my ($storage) = Amanda::Storage->new(storage_name => $storage_name,
						 tapelist     => $tapelist);
            return  $steps->{'quit'}->($storage)
		if $storage->isa("Amanda::Changer::Error");

	    $storage{$storage_name} = $storage;
	};

	my $chg = $storage{$storage_name}->{'chg'};
	if (!$scan{$storage_name}) {
	    my $scan = Amanda::Recovery::Scan->new(
				    chg => $chg,
				    interactivity => $interactivity);
	    return $steps->{'quit'}->($scan)
	        if $scan->isa("Amanda::Changer::Error");

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
		return $steps->quit(Amanda::CheckDump::Message->new(
					source_filename => __FILE__,
					source_line     => __LINE__,
					code            => 2700017));
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
			chomp $line;
			$self->user_msg(Amanda::CheckDump::Message->new(
					source_filename => __FILE__,
					source_line     => __LINE__,
					code            => 2700016,
					line            => $line));
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
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700008,
				errors		=> $recovery_params{'errors'}));
	    return $steps->{'quit'}->("validation aborted");
	}

	if (@xfer_errs) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700009,
				errors		=> \@xfer_errs));
	    $all_success = 0;
	}

        if (defined $hdr->{'native_crc'} and $hdr->{'native_crc'} !~ /^00000000:/ and
            defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
            $hdr->{'native_crc'} ne $current_dump->{'native_crc'}) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700010,
				hdr_native_crc  => $hdr->{'native_crc'},
				log_native_crc  => $current_dump->{'native_crc'}));
        }
        if (defined $hdr->{'client_crc'} and $hdr->{'client_crc'} !~ /^00000000:/ and
            defined $current_dump->{'client_crc'} and $current_dump->{'client_crc'} !~ /^00000000:/ and
            $hdr->{'client_crc'} ne $current_dump->{'client_crc'}) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700011,
				hdr_client_crc  => $hdr->{'client_crc'},
				log_client_crc  => $current_dump->{'client_crc'}));
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
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700012,
				hdr_server_crc  => $hdr->{'server_crc'},
				log_server_crc  => $current_dump->{'server_crc'}));
        }

        if (defined $current_dump->{'server_crc'} and $current_dump->{'server_crc'} !~ /^00000000:/ and
            $current_dump_server_crc_size == $source_crc_size and
            $current_dump->{'server_crc'} ne $source_crc) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700013,
				log_server_crc  => $current_dump->{'server_crc'},
				source_crc      => $source_crc));
        }

        if (defined $current_dump->{'native_crc'} and $current_dump->{'native_crc'} !~ /^00000000:/ and
            defined $dest_native_crc and $current_dump->{'native_crc'} ne $dest_native_crc) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700014,
				log_native_crc  => $current_dump->{'native_crc'},
				dest_native_crc => $dest_native_crc));
        }

	my $dump = shift @{$plan->{'dumps'}};
	if (!$dump) {
	    return $steps->{'quit'}->();
	}

	$steps->{'check_dumpfile'}->($dump);
    };

    step quit => sub {
	my ($msg) = @_;

	if ($msg) {
	    $self->{'exit_code'} = 1;
	    $self->user_msg($msg);
	} elsif ($all_success) {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700006));
	} else {
	    $self->user_msg(Amanda::CheckDump::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2700007));
	    $self->{'exit_code'} = 1;
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
	log_add($L_INFO, "pid-done $self->{'pid'}");
	return $finished_cb->($self->{'exit_code'});
    };

}

1;
