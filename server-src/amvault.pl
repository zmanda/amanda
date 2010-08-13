#! @PERL@
# Copyright (c) 2008, 2009, 2010 Zmanda, Inc.  All Rights Reserved.
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

package main::Interactive;
use POSIX qw( :errno_h );
use Amanda::MainLoop qw( :GIOCondition );
use vars qw( @ISA );
@ISA = qw( Amanda::Interactive );

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
	    return $params{'finished_cb'}->(
		Amanda::Changer::Error->new('fatal',
			message => "Fail to read from stdin"));
	} elsif ($n_read == 0) {
	    $self->abort();
	    return $params{'finished_cb'}->(
		Amanda::Changer::Error->new('fatal',
			message => "Aborted by user"));
	} else {
	    $buffer .= $b;
	    if ($b eq "\n") {
		my $line = $buffer;
		chomp $line;
		$buffer = "";
		$self->abort();
		return $params{'finished_cb'}->(undef, $line);
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
use Amanda::Debug qw( :logging );
use Amanda::Xfer qw( :constants );
use Amanda::Header qw( :constants );
use Amanda::MainLoop;
use Amanda::Util qw( quote_string );
use Amanda::DB::Catalog;
use Amanda::Recovery::Planner;
use Amanda::Recovery::Scan;
use Amanda::Recovery::Clerk;
use Amanda::Taper::Scan;
use Amanda::Taper::Scribe;
use Amanda::Tapelist;
use Amanda::Changer;
use Amanda::Cmdline;
use Amanda::Logfile qw( :logtype_t log_add log_add_full
			log_rename $amanda_log_trace_log make_stats );

use base qw(
    Amanda::Recovery::Clerk::Feedback
    Amanda::Taper::Scribe::Feedback
);

sub new {
    my $class = shift;
    my %params = @_;

    bless {
	quiet => $params{'quiet'},

	src_write_timestamp => $params{'src_write_timestamp'},

	dst_changer => $params{'dst_changer'},
	dst_label_template => $params{'dst_label_template'},
	dst_write_timestamp => $params{'dst_write_timestamp'},

	src => undef,
	dst => undef,
	cleanup => {},

	# called when the operation is complete, with the exit
	# status
	exit_cb => undef,
    }, $class;
}

sub run {
    my $self = shift;
    my ($exit_cb) = @_;

    die "already called" if $self->{'exit_cb'};
    $self->{'exit_cb'} = $exit_cb;

    # check that the label template is valid
    my $dst_label_template = $self->{'dst_label_template'};
    return $self->failure("Invalid label template '$dst_label_template'")
	if ($dst_label_template =~ /%[^%]+%/
	    or $dst_label_template =~ /^[^%]+$/);

    # translate "latest" into the most recent timestamp
    if ($self->{'src_write_timestamp'} eq "latest") {
	$self->{'src_write_timestamp'} = Amanda::DB::Catalog::get_latest_write_timestamp();
    }

    return $self->failure("No dumps found")
	unless (defined $self->{'src_write_timestamp'});

    # open up a trace log file
    log_add($L_INFO, "pid $$");
    Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);
    $self->{'cleanup'}{'roll_trace_log'} = 1;

    $self->setup_src();
}

sub setup_src {
    my $self = shift;

    my $src = $self->{'src'} = {};

    # put together a clerk, which of course requires a changer, scan,
    # interactive, and feedback
    my $chg = Amanda::Changer->new();
    return $self->failure("Error opening source changer: $chg")
	if $chg->isa('Amanda::Changer::Error');
    $src->{'chg'} = $chg;

    $src->{'interactive'} = main::Interactive->new();

    $src->{'scan'} = Amanda::Recovery::Scan->new(
	    chg => $src->{'chg'},
	    interactive => $src->{'interactive'});

    $src->{'clerk'} = Amanda::Recovery::Clerk->new(
	    changer => $src->{'chg'},
	    feedback => $self,
	    scan => $src->{'scan'});
    $self->{'cleanup'}{'quit_clerk'} = 1;

    # convert the timestamp to a dumpspec for make_plan
    my $ds = Amanda::Cmdline::dumpspec_t->new(
	    undef, undef, $self->{'src_write_timestamp'}, undef, undef);

    Amanda::Recovery::Planner::make_plan(
	    dumpspec => $ds,
	    changer => $src->{'chg'},
	    plan_cb => sub { $self->plan_cb(@_) });
}

sub plan_cb {
    my $self = shift;
    my ($err, $plan) = @_;
    my $src = $self->{'src'};

    return $self->failure($err) if $err;

    $src->{'plan'} = $plan;

    if (@{$plan->{'dumps'}} == 0) {
	return $self->failure("No dumps to vault");
    }

    $self->setup_dst();
}

sub setup_dst {
    my $self = shift;
    my $dst = $self->{'dst'} = {};

    $dst->{'label'} = undef;
    $dst->{'tape_num'} = 0;

    my $chg = Amanda::Changer->new($self->{'dst_changer'});
    return $self->failure("Error opening destination changer: $chg")
	if $chg->isa('Amanda::Changer::Error');
    $dst->{'chg'} = $chg;

    # TODO: these should be configurable
    my $autolabel = {
	template => $self->{'dst_label_template'},
	other_config => 1,
	non_amanda => 1,
	volume_error => 1,
	empty => 1,
    };
    $dst->{'scan'} = Amanda::Taper::Scan->new(
	changer => $dst->{'chg'},
	labelstr => getconf($CNF_LABELSTR),
	autolabel => $autolabel);

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

	$xfer_dst = $dst->{'scribe'}->get_xfer_dest(
	    max_memory => getconf($CNF_DEVICE_OUTPUT_BUFFER_SIZE),
	    can_cache_inform => 0,
	    part_size => 2*1024*1024, # TODO: get from config
	);

	# create and start the transfer
	$xfer = Amanda::Xfer->new([ $xfer_src, $xfer_dst ]);
	$xfer->start($steps->{'handle_xmsg'});

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
	if ($self->{'cleanup'}{'roll_trace_log'}) {
	    debug("rolling logfile..");
	    log_add($L_INFO, "pid-done $$");
	    log_rename($self->{'dst_write_timestamp'});
	}

	$exit_cb->($exit_status);
    };
}

# TODO: adjust verbosity: log dumpfiles by default, with debug logging of each part

## utilities

sub failure {
    my $self = shift;
    my ($msg) = @_;
    print STDERR "$msg\n";

    debug("failure: $msg");
    $self->quit(1);
}

sub vlog {
    my $self = shift;
    if (!$self->{'quiet'}) {
	print @_, "\n";
    }
}

## scribe feedback methods

# note that the trace log calls here all add "taper", as we're pretending
# to be the taper in the logfiles.

sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    # sure, use all the volumes you want, no problem!
    # TODO: limit to a vaulting-specific value of runtapes
    $params{'perm_cb'}->(undef);
}

sub scribe_notif_new_tape {
    my $self = shift;
    my %params = @_;

    if ($params{'volume_label'}) {
	$self->{'dst'}->{'label'} = $params{'volume_label'};

	# register in the tapelist
	my $tl_file = config_dir_relative(getconf($CNF_TAPELIST));
	my $tl = Amanda::Tapelist::read_tapelist($tl_file);
	my $tle = $tl->lookup_tapelabel($params{'volume_label'});
	$tl->remove_tapelabel($params{'volume_label'});
	$tl->add_tapelabel($self->{'dst_write_timestamp'}, $params{'volume_label'},
		$tle? $tle->{'comment'} : undef);
	$tl->write($tl_file);

	# add to the trace log
	log_add_full($L_START, "taper", sprintf("datestamp %s label %s tape %s",
		$self->{'dst_write_timestamp'},
		quote_string($self->{'dst'}->{'label'}),
		++$self->{'dst'}->{'tape_num'}));

	# and the amdump log
	# TODO: should amvault create one?!
	# print STDERR "taper: wrote label '$self->{label}'\n";
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
	print STDERR "Wrote $self->{dst}->{label}:$params{'fileno'}: " . $hdr->summary() . "\n";
    }
}

sub scribe_notif_log_info {
    my $self = shift;
    my %params = @_;

    log_add_full($L_INFO, "taper", $params{'message'});
}

## clerk feedback methods

sub clerk_notif_part {
    my $self = shift;
    my ($label, $fileno, $header) = @_;

    print STDERR "Reading $label:$fileno: ", $header->summary(), "\n";
}

sub clerk_notif_holding {
    my $self = shift;
    my ($filename, $header) = @_;

    # this used to give the fd from which the holding file was being read.. why??
    print STDERR "Reading '$filename'\n", $header->summary(), "\n";
}

## Application initialization
package main;

use Amanda::Config qw( :init :getconf );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Getopt::Long;

sub usage {
    print <<EOF;
**NOTE** this interface is under development and will change in future releases!

Usage: amvault [-o configoption]* [-q|--quiet]
	<conf> <src-run-timestamp> <dst-changer> <label-template>

    -o: configuration override (see amanda(8))
    -q: quiet progress messages

Copies data from the run with timestamp <src-run-timestamp> onto volumes using
the changer <dst-changer>, labeling new volumes with <label-template>.  If
<src-run-timestamp> is "latest", then the most recent run of amdump, amflush, or
amvault will be used.

EOF
    exit(1);
}

Amanda::Util::setup_application("amvault", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides($#ARGV+1);
my $opt_quiet = 0;
Getopt::Long::Configure(qw{ bundling });
GetOptions(
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    'q|quiet' => \$opt_quiet,
    'version' => \&Amanda::Util::version_opt,
    'help' => \&usage,
) or usage();

usage unless (@ARGV == 4);

my ($config_name, $src_write_timestamp, $dst_changer, $label_template) = @ARGV;

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
    src_write_timestamp => $src_write_timestamp,
    dst_changer => $dst_changer,
    dst_label_template => $label_template,
    dst_write_timestamp => Amanda::Util::generate_timestamp(),
    quiet => $opt_quiet);
Amanda::MainLoop::call_later(sub { $vault->run($exit_cb) });
Amanda::MainLoop::run();

Amanda::Util::finish_application();
exit($exit_status);
