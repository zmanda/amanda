#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

##
# Interactivity class

package main::Interactivity;
use base 'Amanda::Interactivity';
use Amanda::Util qw( weaken_ref );
use Amanda::MainLoop;
use Amanda::Feature;
use Amanda::Debug qw( debug );
use Amanda::Config qw( :getconf );
use Amanda::Recovery::Scan qw( $DEFAULT_CHANGER );

sub new {
    my $class = shift;
    my %params = @_;

    my $self = {
	clientservice => $params{'clientservice'},
    };

    # (weak ref here to eliminate reference loop)
    weaken_ref($self->{'clientservice'});

    return bless ($self, $class);
}

sub abort() {
    my $self = shift;

    debug("ignoring spurious Amanda::Recovery::Scan abort call");
}

sub user_request {
    my $self = shift;
    my %params = @_;
    my $buffer = "";

    my $steps = define_steps
	cb_ref => \$params{'request_cb'};

    step send_message => sub {
	if ($params{'err'}) {
	    $self->{'clientservice'}->sendmessage("$params{err}");
	}

	$steps->{'check_fe_feedme'}->();
    };

    step check_fe_feedme => sub {
	# note that fe_amrecover_FEEDME implies fe_amrecover_splits
	if (!$self->{'clientservice'}->{'their_features'}->has(
				    $Amanda::Feature::fe_amrecover_FEEDME)) {
	    return $params{'request_cb'}->("remote cannot prompt for volumes", undef);
	}
	$steps->{'send_feedme'}->();
    };

    step send_feedme => sub {
	$self->{'clientservice'}->sendctlline("FEEDME $params{label}\r\n", $steps->{'read_response'});
    };

    step read_response => sub {
	my ($err, $written) = @_;
	return $params{'request_cb'}->($err, undef) if $err;

	$self->{'clientservice'}->getline_async(
		$self->{'clientservice'}->{'ctl_stream'}, $steps->{'got_response'});
    };

    step got_response => sub {
	my ($err, $line) = @_;
	return $params{'request_cb'}->($err, undef) if $err;

	if ($line eq "OK\r\n") {
	    return $params{'request_cb'}->(undef, undef); # carry on as you were
	} elsif ($line =~ /^TAPE (.*)\r\n$/) {
	    my $tape = $1;
	    if ($tape eq getconf($CNF_AMRECOVER_CHANGER)) {
		$tape = $Amanda::Recovery::Scan::DEFAULT_CHANGER;
	    }
	    return $params{'request_cb'}->(undef, $tape); # use this device
	} else {
	    return $params{'request_cb'}->("got invalid response from remote", undef);
	}
    };
};

##
# ClientService class

package main::ClientService;
use base 'Amanda::ClientService';

use Sys::Hostname;

use Amanda::Debug qw( debug info warning );
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Util qw( :constants match_disk match_host );
use Amanda::Feature;
use Amanda::Config qw( :init :getconf );
use Amanda::Changer;
use Amanda::Recovery::Scan;
use Amanda::Xfer qw( :constants );
use Amanda::Cmdline;
use Amanda::Recovery::Clerk;
use Amanda::Recovery::Planner;
use Amanda::Recovery::Scan;
use Amanda::DB::Catalog;
use Amanda::Disklist;

# Note that this class performs its control IO synchronously.  This is adequate
# for this service, as it never receives unsolicited input from the remote
# system.

sub run {
    my $self = shift;

    $self->{'my_features'} = Amanda::Feature::Set->mine();
    $self->{'their_features'} = Amanda::Feature::Set->old();
    $self->{'all_filter'} = {};

    $self->setup_streams();
}

sub setup_streams {
    my $self = shift;

    # get started checking security for inetd or processing the REQ/REP
    # for amandad
    if ($self->from_inetd()) {
	if (!$self->check_inetd_security('main')) {
	    $main::exit_status = 1;
	    return $self->quit();
	}
	$self->{'ctl_stream'} = 'main';
	$self->{'data_stream'} = undef; # no data stream yet
    } else {
	my $req = $self->get_req();

	# make some sanity checks
	my $errors = [];
	if (defined $req->{'options'}{'auth'} and defined $self->amandad_auth()
		and $req->{'options'}{'auth'} ne $self->amandad_auth()) {
	    my $reqauth = $req->{'options'}{'auth'};
	    my $amauth = $self->amandad_auth();
	    push @$errors, "recover program requested auth '$reqauth', " .
			   "but amandad is using auth '$amauth'";
	    $main::exit_status = 1;
	}

	# and pull out the features, if given
	if (defined($req->{'features'})) {
	    $self->{'their_features'} = $req->{'features'};
	}

	$self->send_rep(['CTL' => 'rw', 'DATA' => 'w'], $errors);
	return $self->quit() if (@$errors);

	$self->{'ctl_stream'} = 'CTL';
	$self->{'data_stream'} = 'DATA';
    }

    $self->read_command();
}

sub read_command {
    my $self = shift;
    my $ctl_stream = $self->{'ctl_stream'};
    my $command = $self->{'command'} = {};

    my @known_commands = qw(
	HOST DISK DATESTAMP LABEL DEVICE FSF HEADER
	FEATURES CONFIG );
    while (1) {
	$_ = $self->getline($ctl_stream);
	$_ =~ s/\r?\n$//g;

	last if /^END$/;
	last if /^[0-9]+$/;

	if (/^([A-Z]+)(=(.*))?$/) {
	    my ($cmd, $val) = ($1, $3);
	    if (!grep { $_ eq $cmd } @known_commands) {
		$self->sendmessage("invalid command '$cmd'");
		return $self->quit();
	    }
	    if (exists $command->{$cmd}) {
		warning("got duplicate command key '$cmd' from remote");
	    } else {
		$command->{$cmd} = $val || 1;
	    }
	}

	# features are handled specially.  This is pretty weird!
	if (/^FEATURES=/) {
	    my $featreply;
	    my $featurestr = $self->{'my_features'}->as_string();
	    if ($self->from_amandad) {
		$featreply = "FEATURES=$featurestr\r\n";
	    } else {
		$featreply = $featurestr;
	    }

	    $self->senddata($ctl_stream, $featreply);
	}
    }

    # process some info from the command
    if ($command->{'FEATURES'}) {
	$self->{'their_features'} = Amanda::Feature::Set->from_string($command->{'FEATURES'});
    }

    # load the configuration
    if (!$command->{'CONFIG'}) {
	die "no CONFIG line given";
    }
    config_init($CONFIG_INIT_EXPLICIT_NAME, $command->{'CONFIG'});
    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die "configuration errors; aborting connection";
    }
    Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER_PREFERRED);

    # and the disklist
    my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
    $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die "Errors processing disklist";
    }

    $self->setup_data_stream();
}

sub setup_data_stream {
    my $self = shift;

    # if we're using amandad, then this is ready to roll - it's only inetd mode
    # that we need to fix
    if ($self->from_inetd()) {
	if ($self->{'their_features'}->has($Amanda::Feature::fe_recover_splits)) {
	    # remote side is expecting CONNECT
	    my $port = $self->connection_listen('DATA', 0);
	    $self->senddata($self->{'ctl_stream'}, "CONNECT $port\n");
	    $self->connection_accept('DATA', 30, sub { $self->got_connection(@_); });
	} else {
	    $self->{'ctl_stream'} = undef; # don't use this for ctl anymore
	    $self->{'data_stream'} = 'main';
	    $self->make_plan();
	}
    } else {
	$self->make_plan();
    }
}

sub got_connection {
    my $self = shift;
    my ($err) = @_;

    if ($err) {
	$self->sendmessage("$err");
	return $self->quit();
    }

    if (!$self->check_inetd_security('DATA')) {
	$main::exit_status = 1;
	return $self->quit();
    }
    $self->{'data_stream'} = 'DATA';

    $self->make_plan();
}

sub make_plan {
    my $self = shift;

    # put together a dumpspec
    my $spec;
    if (exists $self->{'command'}{'HOST'}
     || exists $self->{'command'}{'DISK'}
     || exists $self->{'command'}{'DATESTAMP'}) {
	my $disk = $self->{'command'}{'DISK'};
	if (!$self->{'their_features'}->has($Amanda::Feature::fe_amrecover_correct_disk_quoting)) {
	    debug("ignoring specified DISK, as it may be badly quoted");
	    $disk = undef;
	}
	$spec = Amanda::Cmdline::dumpspec_t->new(
	    $self->{'command'}{'HOST'},
	    $disk,
	    $self->{'command'}{'DATESTAMP'},
	    undef,  # amidxtaped protocol does not provide a level (!?)
	    undef); # amidxtaped protocol does not provide a write timestamp
    }

    # figure out if this is a holding-disk recovery
    my $is_holding = 0;
    if (!exists $self->{'command'}{'LABEL'} and exists $self->{'command'}{'DEVICE'}) {
	$is_holding = 1;
    }

    my $chg;
    if ($is_holding) {
	# for holding, give the clerk a null; it won't touch it
	$chg = Amanda::Changer->new("chg-null:");
    } else {
	# if not doing a holding-disk recovery, then we will need a changer.
	# If we're using the "default" changer, instantiate that.  There are
	# several ways the user can specify the default changer:
	my $use_default = 0;
	if (!exists $self->{'command'}{'DEVICE'}) {
	    $use_default = 1;
	} elsif ($self->{'command'}{'DEVICE'} eq getconf($CNF_AMRECOVER_CHANGER)) {
	    $use_default = 1;
	}

	my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	my $tl = Amanda::Tapelist->new($tlf);
	if ($use_default) {
	    $chg = Amanda::Changer->new(undef, tapelist => $tl);
	} else {
	    $chg = Amanda::Changer->new($self->{'command'}{'DEVICE'}, tapelist => $tl);
	}

	# if we got a bogus changer, log it to the debug log, but allow the
	# scan algorithm to find a good one later.
	if ($chg->isa("Amanda::Changer::Error")) {
	    warning("$chg");
	    $chg = Amanda::Changer->new("chg-null:");
	}
    }
    $self->{'chg'} = $chg;

    my $interactivity = main::Interactivity->new(clientservice => $self);

    my $scan = Amanda::Recovery::Scan->new(
			chg => $chg,
			interactivity => $interactivity);
    $self->{'scan'} = $scan;

    # XXX temporary
    $scan->{'scan_conf'}->{'driveinuse'} = Amanda::Recovery::Scan::SCAN_ASK;
    $scan->{'scan_conf'}->{'volinuse'} = Amanda::Recovery::Scan::SCAN_ASK;
    $scan->{'scan_conf'}->{'notfound'} = Amanda::Recovery::Scan::SCAN_ASK;

    $self->{'clerk'} = Amanda::Recovery::Clerk->new(
	# note that we don't have any use for clerk_notif's, so we don't pass
	# a feedback object
	scan => $scan);

    if ($is_holding) {
	# if this is a holding recovery, then the plan is pretty easy.  The holding
	# file is given to us in the aptly-named DEVICE command key, with a :0 suffix
	my $holding_file_tapespec = $self->{'command'}{'DEVICE'};
	my $holding_file = $self->tapespec_to_holding($holding_file_tapespec);

	return Amanda::Recovery::Planner::make_plan(
	    holding_file => $holding_file,
	    $spec? (dumpspec => $spec) : (),
	    plan_cb => sub { $self->plan_cb(@_); });
    } else {
	my $filelist = Amanda::Util::unmarshal_tapespec($self->{'command'}{'LABEL'});

	# if LABEL was just a label, then FSF should contain the filenum we want to
	# start with.
	if ($filelist->[1][0] == 0) {
	    if (exists $self->{'command'}{'FSF'}) {
		$filelist->[1][0] = 0+$self->{'command'}{'FSF'};
		# note that if this is a split dump, make_plan will helpfully find the
		# remaining parts and include them in the restore.  Pretty spiffy.
	    } else {
		# we have only a label and (hopefully) a dumpspec, so let's see if the
		# catalog can find a dump for us.
		$filelist = $self->try_to_find_dump(
			$self->{'command'}{'LABEL'},
			$spec);
		if (!$filelist) {
		    return $self->quit();
		}
	    }
	}

	return Amanda::Recovery::Planner::make_plan(
	    filelist => $filelist,
	    chg => $chg,
	    $spec? (dumpspec => $spec) : (),
	    plan_cb => sub { $self->plan_cb(@_); });
    }
}

sub plan_cb {
    my $self = shift;
    my ($err, $plan) = @_;

    if ($err) {
	$self->sendmessage("$err");
	return $self->quit();
    }

    if (@{$plan->{'dumps'}} > 1) {
	$self->sendmessage("multiple matching dumps; cannot recover");
	return $self->quit();
    }

    # check that the request-limit for this DLE allows this recovery.  because
    # of the bass-ackward way that amrecover specifies the dump to us, we can't
    # check the results until *after* the plan was created.
    my $dump = $plan->{'dumps'}->[0];
    my $dle = Amanda::Disklist::get_disk($dump->{'hostname'}, $dump->{'diskname'});
    my $recovery_limit;
    if ($dle && dumptype_seen($dle->{'config'}, $DUMPTYPE_RECOVERY_LIMIT)) {
	debug("using DLE recovery limit");
	$recovery_limit = dumptype_getconf($dle->{'config'}, $DUMPTYPE_RECOVERY_LIMIT);
    } elsif (getconf_seen($CNF_RECOVERY_LIMIT)) {
	debug("using global recovery limit as default");
	$recovery_limit = getconf($CNF_RECOVERY_LIMIT);
    }
    my $peer = $ENV{'AMANDA_AUTHENTICATED_PEER'};
    if (defined $recovery_limit) { # undef -> no recovery limit
	if (!$peer) {
	    warning("a recovery limit is specified for this DLE, but no authenticated ".
		    "peer name is available; rejecting request.");
	    $self->sendmessage("No matching dumps found");
	    return $self->quit();
	}
	my $matched = 0;
	for my $rl (@$recovery_limit) {
	    if ($rl eq $Amanda::Config::LIMIT_SAMEHOST) {
		# handle same-host with a case-insensitive string compare, not match_host
		if (lc($peer) eq lc($dump->{'hostname'})) {
		    $matched = 1;
		    last;
		}
	    } elsif ($rl eq $Amanda::Config::LIMIT_SERVER) {
		# handle server with a case-insensitive string compare, not match_host
		my $myhostname = hostname;
		debug("myhostname: $myhostname");
		if (lc($peer) eq lc($myhostname)) {
		    $matched = 1;
		    last;
		}
	    } else {
		# otherwise use match_host to allow match expressions
		if (match_host($rl, $peer)) {
		    $matched = 1;
		    last;
		}
	    }
	}
	if (!$matched) {
	    warning("authenticated peer '$peer' did not match recovery-limit ".
		    "config; rejecting request");
	    $self->sendmessage("No matching dumps found");
	    return $self->quit();
	}
    }

    if (!$self->{'their_features'}->has($Amanda::Feature::fe_recover_splits)) {
	# if we have greater than one volume, we may need to prompt for a new
	# volume in mid-recovery.  Sadly, we have no way to inform the client of
	# this.  In hopes that this will "just work", we just issue a warning.
	my @vols = $plan->get_volume_list();
	warning("client does not support split dumps; restore may fail if " .
		"interaction is necessary");
    }

    # now set up the transfer
    $self->{'dump'} = $plan->{'dumps'}[0];
    $self->{'clerk'}->get_xfer_src(
	dump => $self->{'dump'},
	xfer_src_cb => sub { $self->xfer_src_cb(@_); });
}

sub xfer_src_cb {
    my $self = shift;
    my ($errors, $header, $xfer_src, $directtcp_supported) = @_;

    if ($errors) {
	for (@$errors) {
	    $self->sendmessage("$_");
	}
	return $self->quit();
    }

    $self->{'xfer_src'} = $xfer_src;
    $self->{'xfer_src_supports_directtcp'} = $directtcp_supported;
    $self->{'header'} = $header;

    debug("recovering from " . $header->summary());

    # set up any filters that need to be applied, decryption first
    my @filters;
    if ($header->{'encrypted'}) {
	if ($header->{'srv_encrypt'}) {
	    push @filters,
		Amanda::Xfer::Filter::Process->new(
		    [ $header->{'srv_encrypt'}, $header->{'srv_decrypt_opt'} ], 0);
	    $header->{'encrypted'} = 0;
	    $header->{'srv_encrypt'} = '';
	    $header->{'srv_decrypt_opt'} = '';
	    $header->{'clnt_encrypt'} = '';
	    $header->{'clnt_decrypt_opt'} = '';
	    $header->{'encrypt_suffix'} = 'N';
	} elsif ($header->{'clnt_encrypt'}) {
	    if (!$self->{'their_features'}->has($Amanda::Feature::fe_amrecover_receive_unfiltered)) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
		        [ $header->{'clnt_encrypt'},
			  $header->{'clnt_decrypt_opt'} ], 0);
		$header->{'encrypted'} = 0;
		$header->{'srv_encrypt'} = '';
		$header->{'srv_decrypt_opt'} = '';
		$header->{'clnt_encrypt'} = '';
		$header->{'clnt_decrypt_opt'} = '';
		$header->{'encrypt_suffix'} = 'N';
	    } else {
		debug("Not decrypting client encrypted stream");
	    }
	} else {
	    $self->sendmessage("could not decrypt encrypted dump: no program specified");
	    return $self->quit();
	}

    }

    if ($header->{'compressed'}) {
	# need to uncompress this file
	debug("..with decompression applied");

	if ($header->{'srvcompprog'}) {
	    # TODO: this assumes that srvcompprog takes "-d" to decrypt
	    push @filters,
		Amanda::Xfer::Filter::Process->new(
		    [ $header->{'srvcompprog'}, "-d" ], 0);
	    # adjust the header
	    $header->{'compressed'} = 0;
	    $header->{'uncompress_cmd'} = '';
	    $header->{'srvcompprog'} = '';
	} elsif ($header->{'clntcompprog'}) {
	    if (!$self->{'their_features'}->has($Amanda::Feature::fe_amrecover_receive_unfiltered)) {
		# TODO: this assumes that clntcompprog takes "-d" to decrypt
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $header->{'clntcompprog'}, "-d" ], 0);
		# adjust the header
		$header->{'compressed'} = 0;
		$header->{'uncompress_cmd'} = '';
		$header->{'clntcompprog'} = '';
	    }
	} else {
	    my $dle = $header->get_dle();
	    if ($dle &&
		(!$self->{'their_features'}->has($Amanda::Feature::fe_amrecover_receive_unfiltered) ||
		 $dle->{'compress'} == $Amanda::Config::COMP_SERVER_FAST ||
		 $dle->{'compress'} == $Amanda::Config::COMP_SERVER_BEST)) {
		push @filters,
		    Amanda::Xfer::Filter::Process->new(
			[ $Amanda::Constants::UNCOMPRESS_PATH,
			  $Amanda::Constants::UNCOMPRESS_OPT ], 0);
		# adjust the header
		$header->{'compressed'} = 0;
		$header->{'uncompress_cmd'} = '';
	    }
	}

    }
    $self->{'xfer_filters'} = [ @filters ];

    # only send the header if requested
    if ($self->{'command'}{'HEADER'}) {
	$self->send_header();
    } else {
	$self->expect_datapath();
    }
}

sub send_header {
    my $self = shift;

    my $header = $self->{'header'};

    # filter out some things the remote might not be able to process
    if (!$self->{'their_features'}->has($Amanda::Feature::fe_amrecover_dle_in_header)) {
	$header->{'dle_str'} = undef;
    } else {
	$header->{'dle_str'} =
	    Amanda::Disklist::clean_dle_str_for_client($header->{'dle_str'},
		   Amanda::Feature::am_features($self->{'their_features'}));
    }
    if (!$self->{'their_features'}->has($Amanda::Feature::fe_amrecover_origsize_in_header)) {
	$header->{'orig_size'} = 0;
    }

    # even with fe_amrecover_splits, amrecover doesn't like F_SPLIT_DUMPFILE.
    $header->{'type'} = $Amanda::Header::F_DUMPFILE;

    my $hdr_str = $header->to_string(32768, 32768);
    Amanda::Util::full_write($self->wfd($self->{'data_stream'}), $hdr_str, length($hdr_str))
	or die "writing to $self->{data_stream}: $!";

    $self->expect_datapath();
}

sub expect_datapath {
    my $self = shift;

    $self->{'datapath'} = 'none';

    # short-circuit this if amrecover doesn't support datapaths
    if (!$self->{'their_features'}->has($Amanda::Feature::fe_amidxtaped_datapath)) {
	return $self->start_xfer();
    }

    my $line = $self->getline($self->{'ctl_stream'});
    if ($line eq "ABORT\r\n") {
	return Amanda::MainLoop::quit();
    }
    my ($dpspec) = ($line =~ /^AVAIL-DATAPATH (.*)\r\n$/);
    die "bad AVAIL-DATAPATH line" unless $dpspec;
    my @avail_dps = split / /, $dpspec;

    if (grep /^DIRECT-TCP$/, @avail_dps) {
	# remote can handle a directtcp transfer .. can we?
	if ($self->{'xfer_src_supports_directtcp'}) {
	    $self->{'datapath'} = 'directtcp';
	} else {
	    $self->{'datapath'} = 'amanda';
	}
    } else {
	# remote can at least handle AMANDA
	die "remote cannot handle AMANDA datapath??"
	    unless grep /^AMANDA$/, @avail_dps;
	$self->{'datapath'} = 'amanda';
    }

    $self->start_xfer();
}

sub start_xfer {
    my $self = shift;

    # create the appropriate destination based on our datapath
    my $xfer_dest;
    if ($self->{'datapath'} eq 'directtcp') {
	$xfer_dest = Amanda::Xfer::Dest::DirectTCPListen->new();
    } else {
	$xfer_dest = Amanda::Xfer::Dest::Fd->new(
		$self->wfd($self->{'data_stream'})),
    }

    if ($self->{'datapath'} eq 'amanda') {
	$self->sendctlline("USE-DATAPATH AMANDA\r\n");
	my $dpline = $self->getline($self->{'ctl_stream'});
	if ($dpline ne "DATAPATH-OK\r\n") {
	    die "expected DATAPATH-OK";
	}
    }

    # start reading all filter stderr
    foreach my $filter (@{$self->{'xfer_filters'}}) {
	my $fd = $filter->get_stderr_fd();
	$fd.="";
	$fd = int($fd);
	my $src = Amanda::MainLoop::fd_source($fd,
					      $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
	my $buffer = "";
	$self->{'all_filter'}{$src} = 1;
	$src->set_callback( sub {
	    my $b;
	    my $n_read = POSIX::read($fd, $b, 1);
	    if (!defined $n_read) {
		return;
	    } elsif ($n_read == 0) {
		delete $self->{'all_filter'}->{$src};
		$src->remove();
		POSIX::close($fd);
		if (!%{$self->{'all_filter'}} and $self->{'fetch_done'}) {
		    Amanda::MainLoop::quit();
		}
	    } else {
		$buffer .= $b;
		if ($b eq "\n") {
		    my $line = $buffer;
		    #print STDERR "filter stderr: $line";
		    chomp $line;
		    $self->sendmessage("filter stderr: $line");
		    debug("filter stderr: $line");
		    $buffer = "";
		}
	    }
	});
    }

    # create and start the transfer
    $self->{'xfer'} = Amanda::Xfer->new([
	$self->{'xfer_src'},
	@{$self->{'xfer_filters'}},
	$xfer_dest,
    ]);
    my $size = 0;
    $size = $self->{'dump'}->{'bytes'} if exists $self->{'dump'}->{'bytes'};
    $self->{'xfer'}->start(sub { $self->handle_xmsg(@_); }, 0, $size);
    debug("started xfer; datapath=$self->{datapath}");

    # send the data-path response, if we have a datapath
    if ($self->{'datapath'} eq 'directtcp') {
	my $addrs = $xfer_dest->get_addrs();
	$addrs = [ map { $_->[0] . ":" . $_->[1] } @$addrs ];
	$addrs = join(" ", @$addrs);
	$self->sendctlline("USE-DATAPATH DIRECT-TCP $addrs\r\n");
	my $dpline = $self->getline($self->{'ctl_stream'});
	if ($dpline ne "DATAPATH-OK\r\n") {
	    die "expected DATAPATH-OK";
	}
    }

    # and let the clerk know
    $self->{'clerk'}->start_recovery(
	xfer => $self->{'xfer'},
	recovery_cb => sub { $self->recovery_cb(@_); });
}

sub handle_xmsg {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    $self->{'clerk'}->handle_xmsg($src, $msg, $xfer);
    if ($msg->{'elt'} != $self->{'xfer_src'}) {
	if ($msg->{'type'} == $XMSG_ERROR) {
	    $self->sendmessage("$msg->{message}");
	}
    }
}

sub recovery_cb {
    my $self = shift;
    my %params = @_;

    debug("recovery complete");
    if (@{$params{'errors'}}) {
	for (@{$params{'errors'}}) {
	    $self->sendmessage("$_");
	}
	return $self->quit();
    }

    # note that the amidxtaped protocol has no way to indicate successful
    # completion of a transfer
    if ($params{'result'} ne 'DONE') {
	warning("NOTE: transfer failed, but amrecover does not know that");
    }

    $self->finish();
}

sub finish {
    my $self = shift;

    # close the data fd for writing to signal EOF
    $self->close($self->{'data_stream'}, 'w');

    $self->quit();
}

sub quit {
    my $self = shift;

    if ($self->{'clerk'}) {
	$self->{'clerk'}->quit(finished_cb => sub {
	    my ($err) = @_;
	    $self->{'chg'}->quit() if defined $self->{'chg'};
	    if ($err) {
		# it's *way* too late to report this to amrecover now!
		warning("while quitting clerk: $err");
	    }
	    $self->quit1();
	});
    } else {
	$self->{'scan'}->quit() if defined $self->{'scan'};
	$self->{'chg'}->quit() if defined $self->{'chg'};
	$self->quit1();
    }

}

sub quit1 {
    my $self = shift;

    $self->{'fetch_done'} = 1;
    if (!%{$self->{'all_filter'}}) {
	Amanda::MainLoop::quit();
    }
}

## utilities

sub check_inetd_security {
    my $self = shift;
    my ($stream) = @_;

    my $firstline = $self->getline($stream);
    if ($firstline !~ /^SECURITY (.*)\n/) {
	warning("did not get security line");
	print "ERROR did not get security line\r\n";
	return 0;
    }

    my $errmsg = $self->check_bsd_security($stream, $1);
    if ($errmsg) {
	print "ERROR $errmsg\r\n";
	return 0;
    }

    return 1;
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

sub send_rep {
    my $self = shift;
    my ($streams, $errors) = @_;
    my $rep = '';

    # first, if there were errors in the REQ, report them
    if (@$errors) {
	for my $err (@$errors) {
	    $rep .= "ERROR $err\n";
	}
    } else {
	my $connline = $self->connect_streams(@$streams);
	$rep .= "$connline\n";
    }
    # rep needs a empty-line terminator, I think
    $rep .= "\n";

    # write the whole rep packet, and close main to signal the end of the packet
    $self->senddata('main', $rep);
    $self->close('main', 'w');
}

# helper function to get a line, including the trailing '\n', from a stream.  This
# reads a character at a time to ensure that no extra characters are consumed.  This
# could certainly be more efficient! (TODO)
sub getline {
    my $self = shift;
    my ($stream) = @_;
    my $fd = $self->rfd($stream);
    my $line = '';

    while (1) {
	my $c;
	POSIX::read($fd, $c, 1)
	    or last;
	$line .= $c;
	last if $c eq "\n";
    }

    my $chopped = $line;
    $chopped =~ s/[\r\n]*$//g;
    debug("CTL << $chopped");

    return $line;
}

# like getline, but async; TODO:
#  - make all uses of getline async
#  - use buffering to read more than one character at a time
sub getline_async {
    my $self = shift;
    my ($stream, $async_read_cb) = @_;
    my $fd = $self->rfd($stream);

    my $data_in;
    my $buf = '';

    $data_in = sub {
	my ($err, $data) = @_;

	return $async_read_cb->($err, undef) if $err;

	$buf .= $data;
	if ($buf =~ /\r\n$/) {
	    my $chopped = $buf;
	    $chopped =~ s/[\r\n]*$//g;
	    debug("CTL << $chopped");

	    $async_read_cb->(undef, $buf);
	} else {
	    Amanda::MainLoop::async_read(fd => $fd, size => 1, async_read_cb => $data_in);
	}
    };
    Amanda::MainLoop::async_read(fd => $fd, size => 1, async_read_cb => $data_in);
}

# helper function to write a data to a stream.  This does not add newline characters.
# If the callback is given, this is async (TODO: all calls should be async)
sub senddata {
    my $self = shift;
    my ($stream, $data, $async_write_cb) = @_;
    my $fd = $self->wfd($stream);

    if (defined $async_write_cb) {
	return Amanda::MainLoop::async_write(
		fd => $fd,
		data => $data,
		async_write_cb => $async_write_cb);
    } else {
	Amanda::Util::full_write($fd, $data, length($data))
	    or die "writing to $stream: $!";
    }
}

# send a line on the control stream, or just log it if the ctl stream is gone;
# async callback is just like for senddata
sub sendctlline {
    my $self = shift;
    my ($msg, $async_write_cb) = @_;

    my $chopped = $msg;
    $chopped =~ s/[\r\n]*$//g;

    if ($self->{'ctl_stream'}) {
	debug("CTL >> $chopped");
	return $self->senddata($self->{'ctl_stream'}, $msg, $async_write_cb);
    } else {
	debug("not sending CTL message as CTL is closed >> $chopped");
	if (defined $async_write_cb) {
	    $async_write_cb->(undef, length($msg));
	}
    }
}

# send a MESSAGE on the CTL stream, but only if the remote has
# fe_amrecover_message
sub sendmessage {
    my $self = shift;
    my ($msg) = @_;

    if ($self->{'their_features'}->has($Amanda::Feature::fe_amrecover_message)) {
	$self->sendctlline("MESSAGE $msg\r\n");
    } else {
	warning("remote does not understand MESSAGE; not sent: MESSAGE $msg");
    }
}

# covert a tapespec to a holding filename
sub tapespec_to_holding {
    my $self = shift;
    my ($tapespec) = @_;

    my $filelist = Amanda::Util::unmarshal_tapespec($tapespec);

    # $filelist should have the form [ $holding_file => [ 0 ] ]
    die "invalid holding tapespec" unless @$filelist == 2;
    die "invalid holding tapespec" unless @{$filelist->[1]} == 1;
    die "invalid holding tapespec" unless $filelist->[1][0] == 0;

    return $filelist->[0];
}

# amrecover didn't give us much to go on, but see if we can find a dump that
# will make it happy.
sub try_to_find_dump {
    my $self = shift;
    my ($label, $spec) = @_;

    # search the catalog; get_dumps cannot search by labels, so we have to use
    # get_parts instead
    my @parts = Amanda::DB::Catalog::get_parts(
	label => $label,
	dumpspecs => [ $spec ]);

    if (!@parts) {
	$self->sendmessage("could not find any matching dumps on volume '$label'");
	return undef;
    }

    # (note that if there is more than one dump in @parts, the planner will
    # catch it later)

    # sort the parts by their order on each volume.  This sorts the volumes
    # lexically by label, but the planner will straighten it out.
    @parts = Amanda::DB::Catalog::sort_dumps([ "label", "filenum" ], @parts);

    # loop over the parts for the dump and make a filelist.
    my $last_label = '';
    my $last_filenums = undef;
    my $filelist = [];
    for my $part (@parts) {
	next unless defined $part; # skip part number 0
	if ($part->{'label'} ne $last_label) {
	    $last_label = $part->{'label'};
	    $last_filenums = [];
	    push @$filelist, $last_label, $last_filenums;
	}
	push @$last_filenums, $part->{'filenum'};
    }

    return $filelist;
}

##
# main driver

package main;
use Amanda::Debug qw( debug );
use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );

our $exit_status = 0;

sub main {
    Amanda::Util::setup_application("amidxtaped", "server", $CONTEXT_DAEMON);
    config_init(0, undef);
    Amanda::Debug::debug_dup_stderr_to_debug();

    my $cs = main::ClientService->new();
    Amanda::MainLoop::call_later(sub { $cs->run(); });
    Amanda::MainLoop::run();

    debug("exiting with $exit_status");
    Amanda::Util::finish_application();
}

main();
exit($exit_status);
