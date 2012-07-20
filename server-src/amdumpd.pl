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

use Data::Dumper;

##
# ClientService class

package main::ClientService;
use base 'Amanda::ClientService';

use Symbol;
use IPC::Open3;

use Amanda::Debug qw( debug info warning );
use Amanda::Util qw( :constants );
use Amanda::Feature;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Cmdline;
use Amanda::Paths;
use Amanda::Disklist;
use Amanda::Util qw( match_disk match_host );

# Note that this class performs its control IO synchronously.  This is adequate
# for this service, as it never receives unsolicited input from the remote
# system.

sub run {
    my $self = shift;

    $self->{'my_features'} = Amanda::Feature::Set->mine();
    $self->{'their_features'} = Amanda::Feature::Set->old();

    $self->setup_streams();
}

sub setup_streams {
    my $self = shift;

    # always started from amandad.
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

    $self->send_rep(['CTL' => 'rw'], $errors);
    return $self->quit() if (@$errors);

    $self->{'ctl_stream'} = 'CTL';

    $self->read_command();
}

sub cmd_config {
    my $self = shift;

    if (defined $self->{'config'}) {
	$self->sendctlline("ERROR duplicate CONFIG command");
	$self->{'abort'} = 1;
	return;
    }
    my $config = $1;
    config_init($CONFIG_INIT_EXPLICIT_NAME, $config);
    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	$self->sendctlline("ERROR configuration errors; aborting connection");
	$self->{'abort'} = 1;
	return;
    }
    Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER_PREFERRED);

    # and the disklist
    my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
    $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	$self->sendctlline("ERROR Errors processing disklist");
	$self->{'abort'} = 1;
	return;
    }
    $self->{'config'} = $config;
    $self->check_host();
}

sub cmd_features {
    my $self = shift;
    my $features;

    $self->{'their_features'} = Amanda::Feature::Set->from_string($features);
    my $featreply;
    my $featurestr = $self->{'my_features'}->as_string();
    $featreply = "FEATURES $featurestr";

    $self->sendctlline($featreply);
}

sub cmd_list {
    my $self = shift;

    if (!defined $self->{'config'}) {
	$self->sendctlline("CONFIG must be set before listing the disk");
	return;
    }

    for my $disk (@{$self->{'host'}->{'disks'}}) {
	$self->sendctlline(Amanda::Util::quote_string($disk));
    }
    $self->sendctlline("ENDLIST");
}

sub cmd_disk {
    my $self = shift;
    my $qdiskname = shift;
    my $diskname = Amanda::Util::unquote_string($qdiskname);
    if (!defined $self->{'config'}) {
	$self->sendctlline("CONFIG must be set before setting the disk");
	return;
    }

    for my $disk (@{$self->{'host'}->{'disks'}}) {
	if ($disk eq $diskname) {
	    push @{$self->{'disk'}}, $diskname;
	    $self->sendctlline("DISK $diskname added");
	    last;
	}
    }
}

sub cmd_dump {
    my $self = shift;

    if (!defined $self->{'config'}) {
	$self->sendctlline("CONFIG must be set before doing a backup");
	return;
    }

    my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
    if (-f "$logdir/log" || -f "$logdir/amdump" || -f "$logdir/amflush") {
	$self->sendctlline("BUSY Amanda is busy, retry later");
	return;
    }

    $self->sendctlline("DUMPING");
    my @command = ("$sbindir/amdump", "--no-taper", "--from-client", $self->{'config'}, $self->{'host'}->{'hostname'});
    if (defined $self->{'disk'}) {
	@command = (@command, @{$self->{'disk'}});
    }

    debug("command: @command");
    my $amdump_out;
    my $amdump_in;
    my $pid = open3($amdump_in, $amdump_out, $amdump_out, @command);
    close($amdump_in);
    while (<$amdump_out>) {
	chomp;
	$self->sendctlline($_);
    }
    $self->sendctlline("ENDDUMP");
}

sub cmd_check {
    my $self = shift;

    if (!defined $self->{'config'}) {
	$self->sendctlline("CONFIG must be set before doing a backup");
	return;
    }

    my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
    if (-f "$logdir/log" || -f "$logdir/amdump" || -f "$logdir/amflush") {
	$self->sendctlline("BUSY Amanda is busy, retry later");
	return;
    }

    $self->sendctlline("CHECKING");
    my @command = ("$sbindir/amcheck", "-c", $self->{'config'}, $self->{'host'}->{'hostname'});
    if (defined $self->{'disk'}) {
	@command = (@command, @{$self->{'disk'}});
    }

    debug("command: @command");
    my $amcheck_out;
    my $amcheck_in;
    my $pid = open3($amcheck_in, $amcheck_out, $amcheck_out, @command);
    close($amcheck_in);
    while (<$amcheck_out>) {
	chomp;
	$self->sendctlline($_);
    }
    $self->sendctlline("ENDCHECK");
}

sub read_command {
    my $self = shift;
    my $ctl_stream = $self->{'ctl_stream'};
    my $command = $self->{'command'} = {};

    my @known_commands = qw(
	CONFIG DUMP FEATURES LIST DISK);
    while (!$self->{'abort'} and ($_ = $self->getline($ctl_stream))) {
	$_ =~ s/\r?\n$//g;

	last if /^END$/;
	last if /^[0-9]+$/;

	if (/^CONFIG (.*)$/) {
	    $self->cmd_config($1);
	} elsif (/^FEATURES (.*)$/) {
	    $self->cmd_features($1);
	} elsif (/^LIST$/) {
	    $self->cmd_list();
	} elsif (/^DISK (.*)$/) {
	    $self->cmd_disk($1);
	} elsif (/^CHECK$/) {
	    $self->cmd_check();
	} elsif (/^DUMP$/) {
	    $self->cmd_dump();
	} elsif (/^END$/) {
	    $self->{'abort'} = 1;
	} else {
	    $self->sendctlline("invalid command '$_'");
	}
    }
}

sub check_host {
    my $self = shift;

    my @hosts = Amanda::Disklist::all_hosts();
    my $peer = $ENV{'AMANDA_AUTHENTICATED_PEER'};

    if (!defined($peer)) {
	debug("no authenticated peer name is available; rejecting request.");
	$self->sendctlline("no authenticated peer name is available; rejecting request.");
	die();
    }

    # try to find the host that match the connection
    my $matched = 0;
    for my $host (@hosts) {
	if (lc($peer) eq lc($host->{'hostname'})) {
	    $matched = 1;
	    $self->{'host'} = $host;
	    last;
	}
    }

    if (!$matched) {
	debug("The peer host '$peer' doesn't match a host in the disklist.");
	$self->sendctlline("The peer host '$peer' doesn't match a host in the disklist.");
	$self->{'abort'} = 1;
    }
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
    my $line = undef;

    while (1) {
	my $c;
	my $a = POSIX::read($fd, $c, 1);
	last if $a != 1;
	$line .= $c;
	last if $c eq "\n";
    }

    if ($line) {
	my $chopped = $line;
	$chopped =~ s/[\r\n]*$//g;
	debug("CTL << $chopped");
    } else {
	debug("CTL << EOF");
    }

    return $line;
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

our $exit_status = 0;

sub main {
    Amanda::Util::setup_application("amdumpd", "server", $CONTEXT_DAEMON);
    config_init(0, undef);
    Amanda::Debug::debug_dup_stderr_to_debug();

    my $cs = main::ClientService->new();
    $cs->run();

    debug("exiting with $exit_status");
    Amanda::Util::finish_application();
}

main();
exit($exit_status);
