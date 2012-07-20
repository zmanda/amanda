#! @PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

package main::Interactivity;
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
    my %subs;
    my $buffer = "";

    my $message  = $params{'message'};
    my $label    = $params{'label'};
    my $err      = $params{'err'};
    my $chg_name = $params{'chg_name'};

    $subs{'data_in'} = sub {
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
    $self->{'input_src'}->set_callback($subs{'data_in'});
    return;
};

package Amvault;

use Amanda::Config qw( :getconf config_dir_relative );
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

    bless {
	quiet => $params{'quiet'},
	fulls_only => $params{'fulls_only'},
	opt_export => $params{'opt_export'},
	opt_dumpspecs => $params{'opt_dumpspecs'},
	opt_dry_run => $params{'opt_dry_run'},
	config_name => $params{'config_name'},

	src_write_timestamp => $params{'src_write_timestamp'},

	dst_changer => $params{'dst_changer'},
	dst_autolabel => $params{'dst_autolabel'},
	dst_write_timestamp => $params{'dst_write_timestamp'},

	src => undef,
	dst => undef,
	cleanup => {},

	exporting => 0, # is an export in progress?
	call_after_export => undef, # call this when export complete
	config_overrides_opts => $params{'config_overrides_opts'},
	trace_log_filename => getconf($CNF_LOGDIR) . "/log",

	# called when the operation is complete, with the exit
	# status
	exit_cb => undef,
    }, $class;
}

sub run_subprocess {
    my ($proc, @args) = @_;

    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($null, 0);
	POSIX::dup2($null, 1);
	POSIX::dup2($null, 2);
	exec $proc, @args;
	die "Could not exec $proc: $!";
    }
    waitpid($pid, 0);
    my $s = $? >> 8;
    debug("$proc exited with code $s: $!");
}

sub do_amcleanup {
    my $self = shift;

    return 1 unless -f $self->{'trace_log_filename'};

    # logfiles are still around.  First, try an amcleanup -p to see if
    # the actual processes are already dead
    debug("runing amcleanup -p");
    run_subprocess("$sbindir/amcleanup", '-p', $self->{'config_name'},
		   $self->{'config_overrides_opts'});

    return 1 unless -f $self->{'trace_log_filename'};

    return 0;
}

sub bail_already_running() {
    my $self = shift;
    my $msg = "An Amanda process is already running - please run amcleanup manually";
    print "$msg\n";
    debug($msg);
    $self->{'exit_cb'}->(1);
}

sub run {
    my $self = shift;
    my ($exit_cb) = @_;

    die "already called" if $self->{'exit_cb'};
    $self->{'exit_cb'} = $exit_cb;

    # check that the label template is valid
    my $dst_label_template = $self->{'dst_autolabel'}->{'template'};
    return $self->failure("Invalid label template '$dst_label_template'")
	if ($dst_label_template =~ /%[^%]+%/
	    or $dst_label_template =~ /^[^%]+$/);

    # open up a trace log file and put our imprimatur on it, unless dry_runing
    if (!$self->{'opt_dry_run'}) {
	if (!$self->do_amcleanup()) {
	    return $self->bail_already_running();
	}
	log_add($L_INFO, "amvault pid $$");

	# Check we own the log file
	open(my $tl, "<", $self->{'trace_log_filename'})
	    or die("could not open trace log file '$self->{'trace_log_filename'}': $!");
	if (<$tl> !~ /^INFO amvault amvault pid $$/) {
	    debug("another amdump raced with this one, and won");
	    close($tl);
	    return $self->bail_already_running();
	}
	close($tl);
	log_add($L_START, "date " . $self->{'dst_write_timestamp'});
	Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);
	$self->{'cleanup'}{'roll_trace_log'} = 1;
    }

    $self->setup_src();
}

sub setup_src {
    my $self = shift;

    my $src = $self->{'src'} = {};

    # put together a clerk, which of course requires a changer, scan,
    # interactivity, and feedback
    my $chg = Amanda::Changer->new();
    return $self->failure("Error opening source changer: $chg")
	if $chg->isa('Amanda::Changer::Error');
    $src->{'chg'} = $chg;

    $src->{'seen_labels'} = {};

    $src->{'interactivity'} = main::Interactivity->new();

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
	return $self->failure("No dumps found")
	    unless defined $ts;

	$self->vlog("Using latest timestamp: $ts");
    }

    # we need to combine fulls_only, src_write_timestamp, and the set
    # of dumpspecs.  If they contradict one another, then drop the
    # non-matching dumpspec with a warning.
    my @dumpspecs;
    if ($self->{'opt_dumpspecs'}) {
	my $level = $self->{'fulls_only'}? "0" : undef;
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
		if (defined $ds_level &&
		    !match_level($ds_level, $level)) {
		    $self->vlog("WARNING: dumpspec " . $ds->format() .
			    " specifies non-full dumps, contradicting --fulls-only;" .
			    " ignoring dumpspec");
		    next;
		}
		$ds_level = $level;
	    }

	    # create a new dumpspec, since dumpspecs are immutable
	    push @dumpspecs, Amanda::Cmdline::dumpspec_t->new(
		$ds_host, $ds_disk, $ds_datestamp, $ds_level, $ds_write_timestamp);
	}
    } else {
	# convert the timestamp and level to a dumpspec
	my $level = $self->{'fulls_only'}? "0" : undef;
	push @dumpspecs, Amanda::Cmdline::dumpspec_t->new(
		undef, undef, undef, $level, $self->{'src_write_timestamp'});
    }

    # if we ignored all of the dumpspecs and didn't create any, then dump
    # nothing.  We do *not* want the wildcard "vault it all!" behavior.
    if (!@dumpspecs) {
	return $self->failure("No dumps to vault");
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
	    dumpspecs => \@dumpspecs,
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
		print STDOUT
		      ($part->{'label'} || $part->{'holding_file'}) . " " .
		      ($part->{'filenum'} || '') . " " .
		      $dump->{'hostname'} . " " .
		      $dump->{'diskname'} . " " .
		      $dump->{'dump_timestamp'} . " " .
		      $dump->{'level'} . "\n";
	    }
	    $total_kb += int $dump->{'kb'};
	}

	print STDOUT "Total Size: $total_kb KB\n";

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
	return $self->failure("No dumps to vault");
    }

    $self->setup_dst();
}

sub setup_dst {
    my $self = shift;
    my $dst = $self->{'dst'} = {};
    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my $tl = Amanda::Tapelist->new($tlf);

    $dst->{'label'} = undef;
    $dst->{'tape_num'} = 0;

    my $chg = Amanda::Changer->new($self->{'dst_changer'},
				   tapelist => $tl,
				   labelstr => getconf($CNF_LABELSTR),
				   autolabel => $self->{'dst_autolabel'});
    return $self->failure("Error opening destination changer: $chg")
	if $chg->isa('Amanda::Changer::Error');
    $dst->{'chg'} = $chg;

    my $interactivity = Amanda::Interactivity->new(
					name => getconf($CNF_INTERACTIVITY));
    my $scan_name = getconf($CNF_TAPERSCAN);
    $dst->{'scan'} = Amanda::Taper::Scan->new(
	algorithm => $scan_name,
	changer => $dst->{'chg'},
	interactivity => $interactivity,
	tapelist => $tl,
	labelstr => getconf($CNF_LABELSTR),
	autolabel => $self->{'dst_autolabel'});

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
	$self->failure($err) if $err;
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
	# reset tracking for teh current dump
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

	return $finished_cb->(join("\n", @$errors))
	    if $errors;

	$current->{'header'} = $header;

	# set up splitting args from the tapetype only, since we have no DLEs
	my $tt = lookup_tapetype(getconf($CNF_TAPETYPE));
	sub empty2undef { $_[0]? $_[0] : undef }
	my %xfer_dest_args;
	if ($tt) {
	    %xfer_dest_args = get_splitting_args_from_config(
		part_size_kb =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_SIZE)),
		part_cache_type_enum =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_CACHE_TYPE)),
		part_cache_dir =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_CACHE_DIR)),
		part_cache_max_size =>
		    empty2undef(tapetype_getconf($tt, $TAPETYPE_PART_CACHE_MAX_SIZE)),
	    );
	}
	# (else leave %xfer_dest_args empty, for no splitting)

	$xfer_dst = $dst->{'scribe'}->get_xfer_dest(
	    max_memory => getconf($CNF_DEVICE_OUTPUT_BUFFER_SIZE),
	    can_cache_inform => 0,
	    %xfer_dest_args,
	);

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
	    log_add_full($L_FAIL, "taper", sprintf("%s %s %s %s %s %s",
		quote_string($dump->{'hostname'}.""), # " is required for SWIG..
		quote_string($dump->{'diskname'}.""),
		$dump->{'dump_timestamp'},
		$dump->{'level'},
		'error',
		$msg));
	} else {
	    log_add_full($logtype, "taper", sprintf("%s %s %s %s %s %s%s",
		quote_string($dump->{'hostname'}.""), # " is required for SWIG..
		quote_string($dump->{'diskname'}.""),
		$dump->{'dump_timestamp'},
		$current->{'nparts'},
		$dump->{'level'},
		$stats,
		($logtype == $L_PARTIAL and @errors)? " $msg" : ""));
	}

	if (@errors) {
	    return $finished_cb->("transfer failed: " .  join("; ", @errors));
	} else {
	    # rinse, wash, and repeat
	    return $steps->{'get_dump'}->();
	}
    };
}

sub quit {
    my $self = shift;
    my ($exit_status) = @_;
    my $exit_cb = $self->{'exit_cb'};

    my $steps = define_steps
	    cb_ref => \$exit_cb;

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
	    print STDERR "$err\n";
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
	    print STDERR "$err\n";
	    $exit_status = 1;
	}

	$steps->{'roll_log'}->();
    };

    step roll_log => sub {
	if (defined $self->{'src'}->{'chg'}) {
	    $self->{'src'}->{'chg'}->quit();
	    $self->{'src'}->{'chg'} = undef;
	}
	if (defined $self->{'dst'}->{'chg'}) {
	    $self->{'dst'}->{'chg'}->quit();
	    $self->{'dst'}->{'chg'} = undef;
	}
	if ($self->{'cleanup'}{'roll_trace_log'}) {
	    log_add_full($L_FINISH, "driver", "fake driver finish");
	    log_add($L_INFO, "pid-done $$");

	    my @amreport_cmd = ("$sbindir/amreport", $self->{'config_name'}, "--from-amdump",
				 @{$self->{'config_overrides_opts'}});
	    debug("invoking amreport (" . join(" ", @amreport_cmd) . ")");
	    system(@amreport_cmd);

	    debug("rolling logfile..");
	    log_rename($self->{'dst_write_timestamp'});
	}

	$exit_cb->($exit_status);
    };
}

## utilities

sub failure {
    my $self = shift;
    my ($msg) = @_;
    print STDERR "$msg\n";

    debug("failure: $msg");

    # if we've got a logfile open that will be rolled, we might as well log
    # an error.
    if ($self->{'cleanup'}{'roll_trace_log'}) {
	log_add($L_FATAL, "$msg");
    }
    $self->quit(1);
}

sub vlog {
    my $self = shift;
    if (!$self->{'quiet'}) {
	print @_, "\n";
    }
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

sub scribe_notif_new_tape {
    my $self = shift;
    my %params = @_;

    if ($params{'volume_label'}) {
	$self->{'dst'}->{'label'} = $params{'volume_label'};

	# add to the trace log
	log_add_full($L_START, "taper", sprintf("datestamp %s label %s tape %s",
		$self->{'dst_write_timestamp'},
		quote_string($self->{'dst'}->{'label'}),
		++$self->{'dst'}->{'tape_num'}));
    } else {
	$self->{'dst'}->{'label'} = undef;

	print STDERR "Could not start new destination volume: $params{error}";
    }
}

sub scribe_notif_part_done {
    my $self = shift;
    my %params = @_;

    $self->{'last_partnum'} = $params{'partnum'};

    my $stats = make_stats($params{'size'}, $params{'duration'}, $self->{'orig_kb'});

    # log the part, using PART or PARTPARTIAL
    my $hdr = $self->{'current'}->{'header'};
    my $logbase = sprintf("%s %s %s %s %s %s/%s %s %s",
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
	$self->vlog("Wrote $self->{dst}->{label}:$params{'fileno'}: " . $hdr->summary());
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
	    print STDERR "Could not get destination inventory: $err\n";
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
	    print STDERR "No import/export slots available; skipping export\n";
	    return $steps->{'done'}->();
	} elsif (!$from_slot) {
	    print STDERR "Could not find the just-written tape; skipping export\n";
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
	    print STDERR "While exporting just-written tape: $err (ignored)\n";
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

    $self->vlog("Reading $label:$fileno: ", $header->summary());
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    $self->vlog("Reading '$filename'", $header->summary());
}

## Application initialization
package main;

use Amanda::Config qw( :init :getconf );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Getopt::Long;
use Amanda::Cmdline qw( :constants parse_dumpspecs );

sub usage {
    my ($msg) = @_;

    print STDERR <<EOF;
**NOTE** this interface is under development and will change in future releases!

Usage: amvault [-o configoption...] [-q] [--quiet] [-n] [--dry-run]
	   [--fulls-only] [--export] [--src-timestamp src-timestamp]
	   --label-template label-template --dst-changer dst-changer
	   [--autolabel autolabel-arg...]
	   config
	   [hostname [ disk [ date [ level [ hostname [...] ] ] ] ]]

    -o: configuration override (see amanda(8))
    -q: quiet progress messages
    --fulls-only: only copy full (level-0) dumps
    --export: move completed destination volumes to import/export slots
    --src-timestamp: the timestamp of the Amanda run that should be vaulted
    --label-template: the template to use for new volume labels
    --dst-changer: the changer to which dumps should be written
    --autolabel: similar to the amanda.conf parameter; may be repeated (default: empty)

Copies data from the run with timestamp <src-timestamp> onto volumes using
the changer <dst-changer>, labeling new volumes with <label-template>.  If
<src-timestamp> is "latest", then the most recent run of amdump or amflush
will be used.  If any dumpspecs are included (<host-expr> and so on), then only
dumps matching those dumpspecs will be dumped.  At least one of --fulls-only,
--src-timestamp, or a dumpspec must be specified.

EOF
    if ($msg) {
	print STDERR "ERROR: $msg\n";
    }
    exit(1);
}

Amanda::Util::setup_application("amvault", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;
my $opt_quiet = 0;
my $opt_dry_run = 0;
my $opt_fulls_only = 0;
my $opt_export = 0;
my $opt_autolabel = {};
my $opt_autolabel_seen = 0;
my $opt_src_write_timestamp;
my $opt_dst_changer;

sub set_label_template {
    usage("only one --label-template allowed") if $opt_autolabel->{'template'};
    $opt_autolabel->{'template'} = $_[1];
}

sub add_autolabel {
    my ($opt, $val) = @_;
    $val = lc($val);
    $val =~ s/-/_/g;

    $opt_autolabel_seen = 1;
    my @ok = qw(other_config non_amanda volume_error empty);
    for (@ok) {
	if ($val eq $_) {
	    $opt_autolabel->{$_} = 1;
	    return;
	}
    }
    if ($val eq 'any') {
	for (@ok) {
	    $opt_autolabel->{$_} = 1;
	}
	return;
    }
    usage("unknown --autolabel value '$val'");
}

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{ bundling });
GetOptions(
    'o=s' => sub {
	push @config_overrides_opts, "-o" . $_[1];
	add_config_override_opt($config_overrides, $_[1]);
    },
    'q|quiet' => \$opt_quiet,
    'n|dry-run' => \$opt_dry_run,
    'fulls-only' => \$opt_fulls_only,
    'export' => \$opt_export,
    'label-template=s' => \&set_label_template,
    'autolabel=s' => \&add_autolabel,
    'src-timestamp=s' => \$opt_src_write_timestamp,
    'dst-changer=s' => \$opt_dst_changer,
    'version' => \&Amanda::Util::version_opt,
    'help' => \&usage,
) or usage("usage error");
$opt_autolabel->{'empty'} = 1 unless $opt_autolabel_seen;

usage("not enough arguments") unless (@ARGV >= 1);

my $config_name = shift @ARGV;
my @opt_dumpspecs = parse_dumpspecs(\@ARGV, $CMDLINE_PARSE_DATESTAMP|$CMDLINE_PARSE_LEVEL)
    if (@ARGV);

usage("no --label-template given") unless $opt_autolabel->{'template'};
usage("no --dst-changer given") unless $opt_dst_changer;
usage("specify something to select the source dumps") unless
    $opt_src_write_timestamp or $opt_fulls_only or @opt_dumpspecs;

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	print STDERR "errors processing config file\n";
	exit(1);
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $exit_status;
my $exit_cb = sub {
    ($exit_status) = @_;
    Amanda::MainLoop::quit();
};

my $vault = Amvault->new(
    config_name => $config_name,
    src_write_timestamp => $opt_src_write_timestamp,
    dst_changer => $opt_dst_changer,
    dst_autolabel => $opt_autolabel,
    dst_write_timestamp => Amanda::Util::generate_timestamp(),
    opt_dumpspecs => @opt_dumpspecs? \@opt_dumpspecs : undef,
    opt_dry_run => $opt_dry_run,
    quiet => $opt_quiet,
    fulls_only => $opt_fulls_only,
    opt_export => $opt_export,
    config_overrides_opts => \@config_overrides_opts);
Amanda::MainLoop::call_later(sub { $vault->run($exit_cb) });
Amanda::MainLoop::run();

Amanda::Util::finish_application();
exit($exit_status);
