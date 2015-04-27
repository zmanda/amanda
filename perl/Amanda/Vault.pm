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

package Amanda::Vault::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2500000) {
	return "The trace log file is '$self->{'trace_log'}'";
    } elsif ($self->{'code'} == 2500001) {
	return "The amdump log file is '$self->{'amdump_log'}'";
    } elsif ($self->{'code'} == 2500002) {
	return "No dumps to vault";
    } elsif ($self->{'code'} == 2500003) {
	return "Running a vault";
    } elsif ($self->{'code'} == 2500004) {
        return "Running a vault";
    } elsif ($self->{'code'} == 2500005) {
	return ($self->{'label'} || $self->{'holding_file'}) . " " .
	       ($self->{'filenum'} || '') . " " .
	       $self->{'hostname'} . " " .
	       $self->{'diskname'} . " " .
	       $self->{'dump_timestamp'} . " " .
	       $self->{'level'};
    } elsif ($self->{'code'} == 2500006) {
        return "Total Size: $self->{'total_size_kb'} KB";
    } elsif ($self->{'code'} == 2500007) {
	return "No dumps found";
    } elsif ($self->{'code'} == 2500008) {
	return "$self->{'bytes_written'} KB";
    } elsif ($self->{'code'} == 2500010) {
	return "No import/export slots available; skipping export";
    } elsif ($self->{'code'} == 2500011) {
	return "Could not find the just-written tape; skipping export";
    } elsif ($self->{'code'} == 2500012) {
	return "Using latest timestamp: $self->{'timestamp'}";
    } elsif ($self->{'code'} == 2500013) {
	return "WARNING: dumpspec $self->{'dumpspec_format'} specifies non-full dumps, contradicting --fulls-only; ignoring dumpspec";
    } elsif ($self->{'code'} == 2500014) {
	return "WARNING: dumpspec $self->{'dumpspec_format'} specifies full dumps, contradicting --incrs-only; ignoring dumpspec";
    } elsif ($self->{'code'} == 2500015) {
	return "Wrote $self->{'label'}:$self->{'fileno'}: $self->{'header_summary'}";
    } elsif ($self->{'code'} == 2500016) {
	return "Reading $self->{'label'}:$self->{'fileno'}: $self->{'header_summary'}";
    } elsif ($self->{'code'} == 2500017) {
	return "Reading '$self->{'holding_filename'}': $self->{'header_summary'}";
    } elsif ($self->{'code'} == 2500018) {
	return "$self->{'errmsg'}";
    }
}


package Amanda::Vault;
use strict;
use warnings;

use POSIX qw(strftime);
use File::Temp;
use Amanda::Config qw( :getconf config_dir_relative );
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

use base qw(
    Amanda::Recovery::Clerk::Feedback
    Amanda::Taper::Scribe::Feedback
);

sub new {
    my $class = shift;
    my %params = @_;

    my @result_messages;

    if (!exists $params{'dst_write_timestamp'}) {
	$params{'dst_write_timestamp'} = Amanda::Util::generate_timestamp();
    }

    my $self = bless {
	quiet => $params{'quiet'},
	fulls_only => $params{'fulls_only'},
	latest_fulls => $params{'latest_fulls'},
	incrs_only => $params{'incrs_only'},
	opt_export => $params{'opt_export'},
	interactivity => $params{'interactivity'},
	opt_dumpspecs => $params{'opt_dumpspecs'},
	opt_dry_run => $params{'opt_dry_run'},
	config => $params{'config'},
	user_msg => $params{'user_msg'},
	is_tty => $params{'is_tty'},
	delay => $params{'delay'},

	src_write_timestamp => $params{'src_write_timestamp'},
	dst_write_timestamp => $params{'dst_write_timestamp'},
	src_labelstr => $params{'src_labelstr'},

	src => undef,
	dst => undef,
	cleanup => {},

	exporting => 0, # is an export in progress?
	call_after_export => undef, # call this when export complete
	config_overrides_opts => $params{'config_overrides_opts'},
	trace_log_filename => config_dir_relative(getconf($CNF_LOGDIR)) . "/log",

	# called when the operation is complete, with the exit
	# status
	exit_cb => undef,
    }, $class;

    $self->{'delay'} = 15000 if !defined $self->{'delay'};
    $self->{'exit_code'} = 0;
    $self->{'amlibexecdir'} = 0;

    # open up a trace log file and put our imprimatur on it, unless dry_runing
    if (!$self->{'opt_dry_run'}) {
	my $logdir = $self->{'logdir'} = config_dir_relative(getconf($CNF_LOGDIR));
	my @now = localtime;
	$self->{'longdate'} = strftime "%a %b %e %H:%M:%S %Z %Y", @now;

	my $timestamp = strftime "%Y%m%d%H%M%S", @now;
	#$self->{'timestamp'} = Amanda::Logfile::make_logname("amflush", $timestamp);
	#$self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
	#debug("beginning trace log: $self->{'trace_log_filename'}");
	#$self->{'cleanup'}{'created_log'} = 1;

	#my $timestamp = $self->{'timestamp'};
	$self->{'datestamp'} = strftime "%Y%m%d", @now;
	$self->{'starttime_locale_independent'} = strftime "%Y-%m-%d %H:%M:%S %Z", @now;
	$self->{'amdump_log_pathname_default'} = "$logdir/amdump";
	$self->{'amdump_log_pathname'} = "$logdir/amdump.$timestamp";
	$self->{'amdump_log_filename'} = "amdump.$timestamp";
	$self->{'dst_write_timestamp'} = Amanda::Logfile::make_logname("amvault", $self->{'dst_write_timestamp'});
	$self->{'timestamp'} = $self->{'dst_write_timestamp'};
	$self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
	log_add($L_START, "date " . $self->{'dst_write_timestamp'});
	Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);
	$self->{'cleanup'}{'created_log'} = 1;

	# Must be opened in append so that all subprocess can write to it.
	open($self->{'amdump_log'}, ">>", $self->{'amdump_log_pathname'})
            or die("could not open amvault log file '$self->{'amdump_log_pathname'}': $!");
	unlink $self->{'amdump_log_pathname_default'};
	symlink $self->{'amdump_log_filename'}, $self->{'amdump_log_pathname_default'};
	push @result_messages, Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 2500001,
				severity    => $Amanda::Message::INFO,
				amdump_log  => $self->{'amdump_log_pathname'});
	push @result_messages, Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 2500000,
				severity    => $Amanda::Message::INFO,
				trace_log   => $self->{'trace_log_filename'});

	# amstatus needs a lot of forms of the time, I guess
	$self->amdump_log("start at $self->{'longdate'}");
	$self->amdump_log("datestamp $self->{'datestamp'}");
	$self->amdump_log("starttime $self->{'timestamp'}");
	$self->amdump_log("starttime-locale-independent $self->{'starttime_locale_independent'}");


    }
    return $self, \@result_messages;
}

sub user_msg {
    my $self = shift;
    my $msg = shift;

    if (defined $self->{'user_msg'}) {
	if (ref $msg eq "") {
	   $msg = Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 2500018,
				severity    => $Amanda::Message::ERROR,
				errmsg      => $msg);
	}
	$self->{'user_msg'}->($msg);
    }
}

sub amdump_log {
    my $self = shift;

    print {$self->{'amdump_log'}} "amvault: ", @_, "\n";
}

sub roll_amdump_logs {
    my $self = shift;

    debug("renaming amdump log and trimming old amdump logs (beyond tapecycle+2)");

    unlink "$self->{'amdump_log_pathname_default'}.1";
    rename $self->{'amdump_log_pathname_default'}, "$self->{'amdump_log_pathname_default'}.1";

    # keep the latest tapecycle files.
    my $logdir = $self->{'logdir'};
    my @files = sort {-M $b <=> -M $a} grep { !/^\./ && -f "$_"} <$logdir/amdump.*>;
    my $days = getconf($CNF_TAPECYCLE) + 2;
    for (my $i = $days-1; $i >= 1; $i--) {
	my $a = pop @files;
    }
    foreach my $name (@files) {
	unlink $name;
	$self->amdump_log("unlink $name");
    }
}

sub create_status_file {
    my $self = shift;

    # create temporary file
    ($self->{status_fh}, $self->{status_filename}) =
	File::Temp::tempfile("taper_status_file_XXXXXX",
				DIR => $Amanda::Paths::AMANDA_TMPDIR,
				UNLINK => 1);

    # tell amstatus about it by writing it to the dump log
    $self->amdump_log("status file $self->{'id'}:" .  "$self->{status_filename}");
    print {$self->{status_fh}} "0";

    # create timer callback, firing every 5s (=5000msec)
    if (!$self->{'timer'}) {
	$self->{timer} = Amanda::MainLoop::timeout_source($self->{'delay'});
	$self->{timer}->set_callback(sub {
	    if ($self->{'dst'}{scribe}) {
		my $size = $self->{'dst'}->{scribe}->get_bytes_written();
		seek $self->{status_fh}, 0, 0;
		print {$self->{status_fh}} $size, '     ';
		$self->{status_fh}->flush();

		if ($self->{'is_tty'}) {
		    $self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500008,
				severity	=> $Amanda::Message::INFO,
				bytes_written   => $size));
	        }
	    }
	});
    }
}

my $ctrl_c = 0;
sub _interrupt {
    $ctrl_c = 1;
    # abort current vault
}

sub run {
    my $self = shift;
    my ($exit_cb) = @_;

    #$SIG{INT} = \&_interrupt;

    die "already called" if $self->{'exit_cb'};
    $self->{'exit_cb'} = $exit_cb;

    $self->setup_src();
}

sub setup_src {
    my $self = shift;

    my $src = $self->{'src'} = {};

    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my ($tl, $message) = Amanda::Tapelist->new($tlf);
    if (defined $message) {
	return $self->failure($message);
    }
    $self->{'tapelist'} = $tl;
    # put together a clerk, which of course requires a changer, scan,
    # interactivity, and feedback
    my $storage = Amanda::Storage->new(tapelist => $self->{'tapelist'});
    return $self->failure($storage)
	if $storage->isa("Amanda::Changer::Error");
    my $chg = $storage->{'chg'};

    if ($chg->isa('Amanda::Changer::Error')) {
	$storage->quit();
	return $self->failure($chg);
    }
    $src->{'storage'} = $storage;
    $src->{'chg'} = $chg;

    $src->{'seen_labels'} = {};

    $src->{'interactivity'} = $self->{'interactivity'};

    $src->{'scan'} = Amanda::Recovery::Scan->new(
	    chg => $src->{'chg'},
	    interactivity => $src->{'interactivity'});

    $src->{'clerk'} = Amanda::Recovery::Clerk->new(
	    changer => $src->{'chg'},
	    feedback => $self,
	    scan => $src->{'scan'});
    $self->{'cleanup'}{'quit_clerk'} = 1;

    # translate "latest" into the most recent timestamp that wasn't created by amvault
    if (defined $self->{'src_write_timestamp'} && $self->{'src_write_timestamp'} eq "latest") {
	my $ts = $self->{'src_write_timestamp'} =
	    Amanda::DB::Catalog::get_latest_write_timestamp(types => ['amdump', 'amflush']);
	if (!defined $ts) {
	    return $self->failure(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500007,
				severity	=> $Amanda::Message::ERROR));
	}

	$self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500012,
				severity	=> $Amanda::Message::INFO,
				timestamp       => $ts));
    }

    # we need to combine fulls_only, latest_fulls, incr_only,
    # src_write_timestamp, and the set of dumpspecs.
    # If they contradict one another, then drop the
    # non-matching dumpspec with a warning.
    my @dumpspecs;
    if ($self->{'opt_dumpspecs'}) {
	my $level = $self->{'fulls_only'}? "=0" :
		    $self->{'latest_fulls'}? "=0" :
		    $self->{'incrs_only'}? "1-399" : undef;
	my $swt = $self->{'src_write_timestamp'};

	# filter and adjust the dumpspecs
	for my $ds (@{$self->{'opt_dumpspecs'}}) {
	    my $ds_host = $ds->{'host'};
	    my $ds_disk = $ds->{'disk'};
	    my $ds_datestamp = $ds->{'datestamp'};
	    my $ds_level = $ds->{'level'};
	    my $ds_write_timestamp = $ds->{'write_timestamp'};

	    if ($swt) {
		# it's impossible for parse_dumpspecs to set write_timestamp,
		# so there's no risk of overlap here
		$ds_write_timestamp = $swt;
	    }

	    if (defined $level) {
		if (defined $ds_level) {
		    if ($self->{'fulls_only'} &&
			!match_level($ds_level, '0')) {
			$self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500013,
				severity	=> $Amanda::Message::WARNING,
				dumpspec_format => $ds->format()));
			next;
		    }
		    if ($self->{'incrs_only'} &&
			$ds_level eq '0' || $ds_level eq '=0') {
			$self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500014,
				severity	=> $Amanda::Message::WARNING,
				dumpspec_format => $ds->format()));
			next;
		    }
		}
		$ds_level = $level;
	    }

	    # create a new dumpspec, since dumpspecs are immutable
	    push @dumpspecs, Amanda::Cmdline::dumpspec_t->new(
		$ds_host, $ds_disk, $ds_datestamp, $ds_level, $ds_write_timestamp);
	}
    } else {
	# convert the timestamp and level to a dumpspec
	my $level = $self->{'fulls_only'}? "=0" :
		    $self->{'latest_fulls'}? "=0" :
		    $self->{'incrs_only'}? "1-399" : undef;
	push @dumpspecs, Amanda::Cmdline::dumpspec_t->new(
		undef, undef, undef, $level, $self->{'src_write_timestamp'});
    }

    # if we ignored all of the dumpspecs and didn't create any, then dump
    # nothing.  We do *not* want the wildcard "vault it all!" behavior.
    if (!@dumpspecs) {
	return $self->failure(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500002,
				severity	=> $Amanda::Message::ERROR));
    }

    if (!$self->{'opt_dry_run'}) {
	# summarize the requested dumps
	my $request;
	if ($self->{'src_write_timestamp'}) {
	    $request = "vaulting from volumes written " . $self->{'src_write_timestamp'};
	} else {
	    $request = "vaulting";
	}
	if ($self->{'opt_dumpspecs'}) {
	    $request .= " dumps matching dumpspecs:";
	}
	if ($self->{'fulls_only'}) {
	    $request .= " (fulls only)";
	}
	log_add($L_INFO, $request);

	# and log the dumpspecs if they were given
	if ($self->{'opt_dumpspecs'}) {
	    for my $ds (@{$self->{'opt_dumpspecs'}}) {
		log_add($L_INFO, "  " . $ds->format());
	    }
	}
    }

    Amanda::Recovery::Planner::make_plan(
	    latest_fulls => $self->{'latest_fulls'},
	    dumpspecs => \@dumpspecs,
	    src_labelstr => $self->{'src_labelstr'},
	    changer => $src->{'chg'},
	    plan_cb => sub { $self->plan_cb(@_) });
}

sub plan_cb {
    my $self = shift;
    my ($err, $plan) = @_;
    my $src = $self->{'src'};

    return $self->failure($err) if $err;

    $src->{'plan'} = $plan;

    if ($self->{'opt_dry_run'}) {
	my $total_kb = Math::BigInt->new(0);

	# iterate over each part of each dump, printing out the basic information
	for my $dump (@{$plan->{'dumps'}}) {
	    my @parts = @{$dump->{'parts'}};
	    shift @parts; # skip partnum 0
	    for my $part (@parts) {
		$self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500005,
				severity        => $Amanda::Message::INFO,
				label           => $part->{'label'},
				holding_file    => $part->{'holding_file'},
				filenum         => $part->{'filenum'},
				hostname        => $dump->{'hostname'},
				diskname        => $dump->{'diskname'},
				dump_timestamp  => $dump->{'dump_timestamp'},
				level           => $dump->{'level'}));
	    }
	    $total_kb += int $dump->{'kb'};
	}

	$self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500006,
				severity        => $Amanda::Message::INFO,
				total_size_kb   => $total_kb));

	return $self->quit(0);
    }

    # output some 'DISK amvault' lines to indicate the disks we will be vaulting
    my %seen;
    for my $dump (@{$plan->{'dumps'}}) {
	my $key = $dump->{'hostname'}."\0".$dump->{'diskname'};
	next if $seen{$key};
	$seen{$key} = 1;
	log_add($L_DISK, quote_string($dump->{'hostname'})
		 . " " . quote_string($dump->{'diskname'}));
    }

    if (@{$plan->{'dumps'}} == 0) {
	return $self->failure(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500002,
				severity	=> $Amanda::Message::ERROR));
    }

    $self->setup_dst();
}

sub setup_dst {
    my $self = shift;
    my $dst = $self->{'dst'} = {};

    $dst->{'label'} = undef;
    $dst->{'tape_num'} = 0;

    my $vault_storages = getconf($CNF_VAULT_STORAGE);
    my $vault_storage = $vault_storages->[0];
    my $storage = Amanda::Storage->new(
				storage_name => $vault_storage,
				tapelist     => $self->{'tapelist'});
    return $self->failure($storage)
	if $storage->isa("Amanda::Changer::Error");
    my $chg = $storage->{'chg'};
    if ($chg->isa('Amanda::Changer::Error')) {
	$storage->quit();
	return $self->failure($chg);
    }
    $dst->{'storage'} = $storage;
    $dst->{'chg'} = $chg;

    my $interactivity = Amanda::Interactivity->new(
					name => $storage->{'interactivity'});
    my $scan_name = $storage->{'taperscan_name'};
    $dst->{'scan'} = Amanda::Taper::Scan->new(
	algorithm => $scan_name,
	storage => $storage,
	changer => $dst->{'chg'},
	interactivity => $interactivity,
	tapelist => $self->{'tapelist'});

    $dst->{'scribe'} = Amanda::Taper::Scribe->new(
	taperscan => $dst->{'scan'},
	feedback => $self);

    $dst->{'scribe'}->start(
	write_timestamp => $self->{'dst_write_timestamp'},
	finished_cb => sub { $self->scribe_started(@_); })
}

sub scribe_started {
    my $self = shift;
    my ($err) = @_;

    return $self->failure($err) if $err;

    $self->{'cleanup'}{'quit_scribe'} = 1;

    my $xfers_finished = sub {
	my ($err) = @_;
	return $self->failure($err) if $err;
	$self->quit(0);
    };

    $self->xfer_dumps($xfers_finished);
}

sub xfer_dumps {
    my $self = shift;
    my ($finished_cb) = @_;

    my $src = $self->{'src'};
    my $dst = $self->{'dst'};
    my ($xfer_src, $xfer_dst, $xfer, $n_threads, $last_partnum);
    my $current;

    my $steps = define_steps
	    cb_ref => \$finished_cb;

    step get_dump => sub {
	# reset tracking for the current dump
	$self->{'current'} = $current = {
	    src_result => undef,
	    src_errors => undef,

	    dst_result => undef,
	    dst_errors => undef,

	    size => 0,
	    duration => 0.0,
	    total_duration => 0.0,
	    nparts => 0,
	    header => undef,
	    dump => undef,
	};

	my $dump = $src->{'plan'}->shift_dump();
	if (!$dump) {
	    return $finished_cb->();
	}

	$current->{'dump'} = $dump;

	$steps->{'get_xfer_src'}->();
    };

    step get_xfer_src => sub {
	$src->{'clerk'}->get_xfer_src(
	    dump => $current->{'dump'},
	    xfer_src_cb => $steps->{'got_xfer_src'})
    };

    step got_xfer_src => sub {
        my ($errors, $header, $xfer_src_, $directtcp_supported) = @_;
	$xfer_src = $xfer_src_;

	if ($errors) {
	    my $msg = join("; ", @$errors);
	    my $dump = $current->{'dump'};
	    $self->amdump_log("Fail vaulting $self->{'id'} $msg");
	    log_add_full($L_FAIL, "taper", sprintf("%s %s %s %s %s %s",
		quote_string($dump->{'hostname'}.""), # " is required for SWIG..
		quote_string($dump->{'diskname'}.""),
		$dump->{'dump_timestamp'},
		$dump->{'level'},
                'error',
                $msg));
	    $self->user_msg($msg);
	    # next dump
	    return $steps->{'get_dump'}->();
	}

	$current->{'header'} = $header;

	# set up splitting args from the tapetype only, since we have no DLEs
	my $tt = $self->{'dst'}->{'storage'}->{'tapetype'};
	sub empty2undef { $_[0]? $_[0] : undef }
	my $dle_allow_split = 1;
	my $dle = Amanda::Disklist::get_disk($header->{'name'},
					     $header->{'disk'});
	if (defined $dle) {
	    $dle_allow_split = dumptype_getconf($dle->{'config'}, $DUMPTYPE_ALLOW_SPLIT);
	}
	my $xdt_first_dev = $dst->{'scribe'}->get_device();
	if (!defined $xdt_first_dev) {
	    return $finished_cb->("no device is available to create an xfer_dest");
	}
	my $leom_supported = $xdt_first_dev->property_get("leom");
	my %xfer_dest_args;
	if ($tt) {
	    %xfer_dest_args = get_splitting_args_from_config(
		dle_allow_split => $dle_allow_split,
		leom_supported => $leom_supported,
		part_size_kb =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_SIZE)),
		part_cache_type_enum =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_CACHE_TYPE)),
		part_cache_dir =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_CACHE_DIR)),
		part_cache_max_size =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_CACHE_MAX_SIZE)),
	    );
	} else {
	    # split only if LEOM is supported.
	    %xfer_dest_args = get_splitting_args_from_config(
		dle_allow_split => $dle_allow_split,
		leom_supported => $leom_supported);
	}
	$xfer_dst = $dst->{'scribe'}->get_xfer_dest(
	    max_memory => $self->{'dst'}->{'storage'}->{'device_output_buffer_size'},
	    can_cache_inform => 0,
	    %xfer_dest_args,
	);

	$self->{'id'}++;
	$self->amdump_log("Vaulting $self->{'id'} $header->{'name'} ".quote_string($header->{'disk'})." $header->{'datestamp'} $header->{'dumplevel'} from storage $src->{'storage'}->{'storage_name'} to storage $dst->{'storage'}->{'storage_name'}");
	$self->create_status_file();

	# create and start the transfer
	$xfer = Amanda::Xfer->new([ $xfer_src, $xfer_dst ]);
	my $size = 0;
	$size = $current->{'dump'}->{'bytes'} if exists $current->{'dump'}->{'bytes'};
	$xfer->start($steps->{'handle_xmsg'}, 0, $size);

	# count the "threads" running here (clerk and scribe)
	$n_threads = 2;

	# and let both the scribe and the clerk know that data is in motion
	$src->{'clerk'}->start_recovery(
	    xfer => $xfer,
	    recovery_cb => $steps->{'recovery_cb'});
	$dst->{'scribe'}->start_dump(
	    xfer => $xfer,
	    dump_header => $header,
	    dump_cb => $steps->{'dump_cb'});
    };

    step handle_xmsg => sub {
	$src->{'clerk'}->handle_xmsg(@_);
	$dst->{'scribe'}->handle_xmsg(@_);
    };

    step recovery_cb => sub {
	my %params = @_;
	$current->{'src_result'} = $params{'result'};
	$current->{'src_errors'} = $params{'errors'};
	$steps->{'maybe_done'}->();
    };

    step dump_cb => sub {
	my %params = @_;
	$current->{'dst_result'} = $params{'result'};
	$current->{'dst_errors'} = $params{'device_errors'};
	$current->{'size'} = $params{'size'};
	$current->{'duration'} = $params{'duration'};
	$current->{'nparts'} = $params{'nparts'};
	$current->{'total_duration'} = $params{'total_duration'};
	$steps->{'maybe_done'}->();
    };

    step maybe_done => sub {
	return unless --$n_threads == 0;
	my @errors = (@{$current->{'src_errors'}}, @{$current->{'dst_errors'}});

	# figure out how to log this, based on the results from the clerk (src)
	# and scribe (dst)
	my $logtype;
	if ($current->{'src_result'} eq 'DONE') {
	    if ($current->{'dst_result'} eq 'DONE') {
		$logtype = $L_DONE;
	    } elsif ($current->{'dst_result'} eq 'PARTIAL') {
		$logtype = $L_PARTIAL;
	    } else { # ($current->{'dst_result'} eq 'FAILED')
		$logtype = $L_FAIL;
	    }
	} else {
	    if ($current->{'size'} > 0) {
		$logtype = $L_PARTIAL;
	    } else {
		$logtype = $L_FAIL;
	    }
	}

	my $dump = $current->{'dump'};
	my $stats = make_stats($current->{'size'}, $current->{'total_duration'},
				$dump->{'orig_kb'});
	my $msg = quote_string(join("; ", @errors));

	# write a DONE/PARTIAL/FAIL log line
	if ($logtype == $L_FAIL) {
	    $self->amdump_log("Fail vaulting $self->{'id'} $msg");
	    log_add_full($L_FAIL, "taper", sprintf("%s %s %s %s %s %s",
		quote_string($dump->{'hostname'}.""), # " is required for SWIG..
		quote_string($dump->{'diskname'}.""),
		$dump->{'dump_timestamp'},
		$dump->{'level'},
		'error',
		$msg));
	} else {
	    if ($logtype == $L_PARTIAL) {
		$self->amdump_log("Partial vaulting $self->{'id'} $stats".(@errors? " $msg" : ""));
	    } else {
		$self->amdump_log("Done vaulting $self->{'id'} $stats");
	    }
	    log_add_full($logtype, "taper", sprintf("%s %s %s %s %s %s %s%s",
		quote_string("ST:" . $self->{'dst'}{'chg'}{'storage'}->{'storage_name'}),
		quote_string($dump->{'hostname'}.""), # " is required for SWIG..
		quote_string($dump->{'diskname'}.""),
		$dump->{'dump_timestamp'},
		$current->{'nparts'},
		$dump->{'level'},
		$stats,
		($logtype == $L_PARTIAL and @errors)? " $msg" : ""));
	}

	# next dump
	return $steps->{'get_dump'}->();
    };
}

sub quit {
    my $self = shift;
    my ($exit_status) = @_;
    my $exit_cb = $self->{'exit_cb'};

    my $steps = define_steps
	    cb_ref => \$exit_cb;

    step quit_timer => sub {
	if (defined $self->{'timer'}) {
	    $self->{'timer'}->remove();
	    $self->{'timer'} = undef;
	}

	$steps->{'check_exporting'}->();
    };

    # the export may not start until we quit the scribe, so wait for it now..
    step check_exporting => sub {
	# if we're exporting the final volume, wait for that to complete
	if ($self->{'exporting'}) {
	    $self->{'call_after_export'} = $steps->{'quit_scribe'};
	} else {
	    $steps->{'quit_scribe'}->();
	}
    };

    # we may have several resources to clean up..
    step quit_scribe => sub {
	if ($self->{'cleanup'}{'quit_scribe'}) {
	    debug("quitting scribe..");
	    $self->{'dst'}{'scribe'}->quit(
		finished_cb => $steps->{'quit_scribe_finished'});
	} else {
	    $steps->{'quit_clerk'}->();
	}
    };

    step quit_scribe_finished => sub {
	$self->{'dst'}{'scan'}->quit();
	my ($err) = @_;
	if ($err) {
	    $self->user_msg($err);
	    debug("scribe error: $err");
	    $exit_status = 1;
	}

	$steps->{'quit_clerk'}->();
    };

    step quit_clerk => sub {
	if ($self->{'cleanup'}{'quit_clerk'}) {
	    debug("quitting clerk..");
	    $self->{'src'}{'clerk'}->quit(
		finished_cb => $steps->{'quit_clerk_finished'});
	} else {
	    $steps->{'roll_log'}->();
	}
    };

    step quit_clerk_finished => sub {
	my ($err) = @_;
	if ($err) {
	    $self->user_msg($err);
	    debug("clerk error: $err");
	    $exit_status = 1;
	}

	$steps->{'roll_log'}->();
    };

    step roll_log => sub {
	if (defined $self->{'src'}->{'storage'}) {
	    $self->{'src'}->{'storage'}->quit();
	    $self->{'src'}->{'storage'} = undef;
	}
	if (defined $self->{'dst'}->{'storage'}) {
	    $self->{'dst'}->{'storage'}->quit();
	    $self->{'dst'}->{'storage'} = undef;
	}
	if (defined $self->{'src'}->{'chg'}) {
	    $self->{'src'}->{'chg'}->quit();
	    $self->{'src'}->{'chg'} = undef;
	}
	if (defined $self->{'dst'}->{'chg'}) {
	    $self->{'dst'}->{'chg'}->quit();
	    $self->{'dst'}->{'chg'} = undef;
	}
	if ($self->{'cleanup'}{'created_log'}) {
	    $self->roll_amdump_logs();
	    log_add_full($L_FINISH, "driver", "fake driver finish");
	    log_add($L_INFO, "pid-done $$");

	    my @amreport_cmd = ("$sbindir/amreport", $self->{'config'},
				"--from-amdump",
				"-l", $self->{'trace_log_filename'},
				@{$self->{'config_overrides_opts'}});
	    debug("invoking amreport (" . join(" ", @amreport_cmd) . ")");
	    system(@amreport_cmd);
	}

	$exit_cb->($exit_status);
    };
}

## utilities

sub failure {
    my $self = shift;
    my ($msg) = shift;

    $self->user_msg($msg);

    debug("failure: $msg");

    # if we've got a logfile open, we might as well log an error.
    if ($self->{'cleanup'}{'created_log'}) {
	log_add($L_FATAL, "$msg");
    }
    $self->quit(1);
}

## scribe feedback methods

# note that the trace log calls here all add "taper", as we're dry_runing
# to be the taper in the logfiles.

sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    # sure, use all the volumes you want, no problem!
    # TODO: limit to a vaulting-specific value of runtapes
    $self->{'dst'}->{'scribe'}->start_scan();
    $params{'perm_cb'}->(allow => 1);
}

sub scribe_ready {
}

sub scribe_notif_new_tape {
    my $self = shift;
    my %params = @_;

    if ($params{'volume_label'}) {
	$self->{'dst'}->{'label'} = $params{'volume_label'};

	# add to the trace log
	log_add_full($L_START, "taper", sprintf("datestamp %s %s label %s tape %s",
		$self->{'dst_write_timestamp'},
		quote_string("ST:" . $self->{'dst'}{'chg'}{'storage'}->{'storage_name'}),
		quote_string($self->{'dst'}->{'label'}),
		++$self->{'dst'}->{'tape_num'}));
    } else {
	$self->{'dst'}->{'label'} = undef;

	$self->user_msg($params{error});
    }
}

sub scribe_notif_part_done {
    my $self = shift;
    my %params = @_;

    $self->{'last_partnum'} = $params{'partnum'};

    my $stats = make_stats($params{'size'}, $params{'duration'}, $self->{'orig_kb'});

    # log the part, using PART or PARTPARTIAL
    my $hdr = $self->{'current'}->{'header'};
    my $logbase = sprintf("%s %s %s %s %s %s %s/%s %s %s",
	quote_string("ST:" . $self->{'dst'}{'chg'}{'storage'}->{'storage_name'}),
	quote_string($self->{'dst'}->{'label'}),
	$params{'fileno'},
	quote_string($hdr->{'name'}.""), # " is required for SWIG..
	quote_string($hdr->{'disk'}.""),
	$hdr->{'datestamp'}."",
	$params{'partnum'}, -1, # totalparts is always -1
	$hdr->{'dumplevel'},
	$stats);
    if ($params{'successful'}) {
	log_add_full($L_PART, "taper", $logbase);
    } else {
	log_add_full($L_PARTPARTIAL, "taper",
		"$logbase \"No space left on device\"");
    }

    if ($params{'successful'}) {
	$self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500015,
				severity	=> $Amanda::Message::INFO,
				label           => $self->{dst}->{label},
				fileno          => $params{'fileno'},
				header_summary  => $hdr->summary()));
    }
}

sub scribe_notif_log_info {
    my $self = shift;
    my %params = @_;

    debug("$params{'message'}");
    log_add_full($L_INFO, "taper", $params{'message'});
}

sub scribe_notif_tape_done {
    my $self = shift;
    my %params = @_;

    # immediately flag that we are busy exporting, to prevent amvault from
    # quitting too soon.  The 'done' step will clear this flag.  We increment
    # and decrement this to allow for the (unlikely) situation that multiple
    # exports are going on simultaneously.
    $self->{'exporting'}++;

    my $finished_cb = $params{'finished_cb'};
    my $steps = define_steps
	cb_ref => \$finished_cb;

    step check_option => sub {
	if (!$self->{'opt_export'}) {
	    return $steps->{'done'}->();
	}

	$steps->{'get_inventory'}->();
    };
    step get_inventory => sub {
	$self->{'dst'}->{'chg'}->inventory(
	    inventory_cb => $steps->{'inventory_cb'});
    };

    step inventory_cb => sub {
	my ($err, $inventory) = @_;
	if ($err) {
	    $self->user_msg($err);
	    return $steps->{'done'}->();
	}

	# find the slots we want in the inventory
	my ($ie_slot, $from_slot);
	for my $info (@$inventory) {
	    if (defined $info->{'state'}
		&& $info->{'state'} != Amanda::Changer::SLOT_FULL
		&& $info->{'import_export'}) {
		$ie_slot = $info->{'slot'};
	    }
	    if ($info->{'label'} and $info->{'label'} eq $params{'volume_label'}) {
		$from_slot = $info->{'slot'};
	    }
	}

	if (!$ie_slot) {
	    $self->user_msg(Amanda::Vault::Message(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500010,
				severity => $Amanda::Message::ERROR));
	    return $steps->{'done'}->();
	} elsif (!$from_slot) {
	    $self->user_msg(Amanda::Vault::Message(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500011,
				severity => $Amanda::Message::ERROR));
	    return $steps->{'done'}->();
	} else {
	    return $steps->{'do_move'}->($ie_slot, $from_slot);
	}
    };

    step do_move => sub {
	my ($ie_slot, $from_slot) = @_;

	# TODO: there is a risk here that the volume is no longer in the slot
	# where we expect it to be, because the taperscan has moved it.  A
	# failure from move() is not fatal, though, so this will only cause the
	# volume to be left un-exported.

	$self->{'dst'}->{'chg'}->move(
	    from_slot => $from_slot,
	    to_slot => $ie_slot,
	    finished_cb => $steps->{'moved'});
    };

    step moved => sub {
	my ($err) = @_;
	if ($err) {
	    $self->user_msg($err);
	}
	$steps->{'done'}->();
    };

    step done => sub {
	if (--$self->{'exporting'} == 0) {
	    if ($self->{'call_after_export'}) {
		my $cae = $self->{'call_after_export'};
		$self->{'call_after_export'} = undef;
		$cae->();
	    }
	}
	$finished_cb->();
    };
}

## clerk feedback methods

sub clerk_notif_part {
    my $self = shift;
    my ($label, $fileno, $header) = @_;

    # see if this is a new label
    if (!exists $self->{'src'}->{'seen_labels'}->{$label}) {
	$self->{'src'}->{'seen_labels'}->{$label} = 1;
	log_add($L_INFO, "reading from source volume '$label'");
    }

    $self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500016,
				severity	=> $Amanda::Message::INFO,
				label           => $label,
				fileno          => $fileno,
				header_summary  => $header->summary()));
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    $self->user_msg(Amanda::Vault::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2500017,
				severity	=> $Amanda::Message::INFO,
				holding_filename => $filename,
				header_summary  => $header->summary()));
}

1;
