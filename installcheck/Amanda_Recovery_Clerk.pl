# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 21;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Dumpcache;
use Amanda::Config qw( :init );
use Amanda::Changer;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::Header;
use Amanda::DB::Catalog;
use Amanda::Xfer qw( :constants );
use Amanda::Recovery::Clerk;
use Amanda::Recovery::Scan;
use Amanda::MainLoop;
use Amanda::Util;
use Amanda::Tapelist;

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;
$testconf = Installcheck::Config->new();
$testconf->add_param('debug_recovery', '9');
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $taperoot = "$Installcheck::TMP/Amanda_Recovery_Clerk";
my $datestamp = "20100101010203";

# set up a 2-tape disk changer with some spanned dumps in it, and add those
# dumps to the catalog, too.  To avoid re-implementing Amanda::Taper::Scribe, this
# uses individual transfers for each part.
sub setup_changer {
    my ($finished_cb, $chg_name, $to_write, $part_len) = @_;
    my $res;
    my $chg;
    my $label;
    my ($slot, $xfer_info, $partnum);

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step setup => sub {
	$chg = Amanda::Changer->new($chg_name);
	die "$chg" if $chg->isa("Amanda::Changer::Error");

	$steps->{'next'}->();
    };

    step next => sub {
	return $steps->{'done'}->() unless @$to_write;

	($slot, $xfer_info, $partnum) = @{shift @$to_write};
	die "xfer len <= 0" if $xfer_info->[0] <= 0;

	if (!$res || $res->{'this_slot'} != $slot) {
	    $steps->{'new_dev'}->();
	} else {
	    $steps->{'run_xfer'}->();
	}
    };

    step new_dev => sub {
	if ($res) {
	    $res->release(finished_cb => $steps->{'released'});
	} else {
	    $steps->{'released'}->();
	}
    };

    step released => sub {
	my ($err) = @_;
	die "$err" if $err;

	$chg->load(slot => $slot, res_cb => $steps->{'loaded'});
    };

    step loaded => sub {
	(my $err, $res) = @_;
	die "$err" if $err;

	my $dev = $res->{'device'};

	# label the device
	$label = "TESTCONF0" . $slot;
	$dev->start($Amanda::Device::ACCESS_WRITE, $label, $datestamp)
	    or die("starting dev: " . $dev->error_or_status());

	$res->set_label(label => $label, finished_cb => $steps->{'run_xfer'});
    };

    step run_xfer => sub {
	my $dev = $res->{'device'};
	my $name = $xfer_info->[2];

	my $hdr = Amanda::Header->new();
	# if the partnum is 0, write a DUMPFILE like Amanda < 3.1 did
	$hdr->{'type'} = $partnum? $Amanda::Header::F_SPLIT_DUMPFILE : $Amanda::Header::F_DUMPFILE;
	$hdr->{'datestamp'} = $datestamp;
	$hdr->{'dumplevel'} = 0;
	$hdr->{'name'} = $name;
	$hdr->{'disk'} = "/$name";
	$hdr->{'program'} = "INSTALLCHECK";
	$hdr->{'partnum'} = $partnum;
	$hdr->{'compressed'} = 0;
	$hdr->{'comp_suffix'} = "N";

	$dev->start_file($hdr)
	    or die("starting file: " . $dev->error_or_status());

	my $len = $xfer_info->[0];
	$len = $part_len if $len > $part_len;
	my $key = $xfer_info->[1];

	my $xsrc = Amanda::Xfer::Source::Random->new($len, $key);
	my $xdst = Amanda::Xfer::Dest::Device->new($dev, 0);
	my $xfer = Amanda::Xfer->new([$xsrc, $xdst]);

	$xfer->start(sub {
	    my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		# fix up $xfer_info
		$xfer_info->[0] -= $len;
		$xfer_info->[1] = $xsrc->get_seed();

		# add the dump to the catalog
		Amanda::DB::Catalog::add_part({
			label => $label,
			filenum => $dev->file() - 1,
			dump_timestamp => $datestamp,
			write_timestamp => $datestamp,
			hostname => $name,
			diskname => "/$name",
			level => 0,
			status => "OK",
			# get the partnum right, even if this wasn't split
			partnum => $partnum? $partnum : ($partnum+1),
			nparts => -1,
			kb => $len / 1024,
			sec => 1.2,
		    });

		# and do the next part
		$steps->{'next'}->();
	    }
	});
    };

    step done => sub {
	if ($res) {
	    $res->release(finished_cb => $steps->{'done_released'});
	} else {
	    $steps->{'done_released'}->();
	}
    };

    step done_released => sub {
	$finished_cb->();
    };
}

{
    # clean out the vtape root
    if (-d $taperoot) {
	rmtree($taperoot);
    }
    mkpath($taperoot);

    for my $slot (1 .. 2) {
	mkdir("$taperoot/slot$slot")
	    or die("Could not mkdir: $!");
    }

    ## specification of the on-tape data
    my @xfer_info = (
	# length,	random, name ]
	[ 1024*288,	0xF000, "home" ],
	[ 1024*1088,	0xF001, "usr" ],
	[ 1024*768,	0xF002, "games" ],
    );
    my @to_write = (
	# slot xfer		partnum
	[ 1,   $xfer_info[0],   0 ], # partnum 0 => old non-split header
	[ 1,   $xfer_info[1],   1 ],
	[ 1,   $xfer_info[1],   2 ],
	[ 2,   $xfer_info[1],   3 ],
	[ 2,   $xfer_info[2],   1 ],
	[ 2,   $xfer_info[2],   2 ],
    );

    setup_changer(\&Amanda::MainLoop::quit, "chg-disk:$taperoot", \@to_write, 512*1024);
    Amanda::MainLoop::run();
    pass("successfully set up test vtapes");
}

# make a holding file
my $holding_file = "$Installcheck::TMP/holding_file";
my $holding_key = 0x797;
my $holding_kb = 64;
{
    open(my $fh, ">", "$holding_file") or die("opening '$holding_file': $!");

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = '21001010101010';
    $hdr->{'dumplevel'} = 1;
    $hdr->{'name'} = 'heldhost';
    $hdr->{'disk'} = '/to/holding';
    $hdr->{'program'} = "INSTALLCHECK";
    $hdr->{'is_partial'} = 0;

    Amanda::Util::full_write(fileno($fh), $hdr->to_string(32768,32768), 32768);

    # transfer some data to that file
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*$holding_kb, $holding_key),
	Amanda::Xfer::Dest::Fd->new($fh),
    ]);

    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    close($fh);
}

# fill out a dump object like that returned from Amanda::DB::Catalog, with all
# of the keys that we don't really need based on a much simpler description
sub fake_dump {
    my ($hostname, $diskname, $dump_timestamp, $level, @parts) = @_;

    my $pldump = {
	dump_timestamp => $dump_timestamp,
	write_timestamp => $dump_timestamp,
	hostname => $hostname,
	diskname => $diskname,
	level => $level,
	status => 'OK',
	message => '',
	nparts => 0, # filled in later
	kb => 128, # ignored by clerk anyway
	secs => 10.0, # ditto
	parts => [ undef ],
    };

    for my $part (@parts) {
	push @{$pldump->{'parts'}}, {
	    %$part,
	    dump => $pldump,
	    status => "OK",
	    partnum => scalar @{$pldump->{'parts'}},
	    kb => 64, # ignored
	    sec => 1.0, # ignored
	};
	$pldump->{'nparts'}++;
    }

    return $pldump;
}

package main::Feedback;

use base 'Amanda::Recovery::Clerk::Feedback';

sub new {
    my $class = shift;
    my %params = @_;

    return bless \%params, $class;
}

sub clerk_notif_part {
    my $self = shift;

    if (exists $self->{'clerk_notif_part'}) {
	$self->{'clerk_notif_part'}->(@_);
    } else {
	$self->SUPER::clerk_notif_part(@_);
    }
}

package main;

# run a recovery with the given plan on the given clerk, expecting a bytestream with
# the given random seed.
sub try_recovery {
    my %params = @_;
    my $clerk = $params{'clerk'};
    my $result;
    my $running_xfers = 0;

    my $finished_cb = \&Amanda::MainLoop::quit;
    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$clerk->get_xfer_src(
	    dump => $params{'dump'},
	    xfer_src_cb => $steps->{'xfer_src_cb'});
    };

    step xfer_src_cb => sub {
	my ($errors, $header, $xfer_src, $dtcp_supp) = @_;

	# simulate errors for xfail, below
	if ($errors) {
	    $result = { result => "FAILED", errors => $errors };
	    return $steps->{'verify'}->();
	}

	# double-check the header; the Clerk should have checked this, so these
	# are die's, for simplicity
	die unless
	    $header->{'name'} eq $params{'dump'}->{'hostname'} &&
	    $header->{'disk'} eq $params{'dump'}->{'diskname'} &&
	    $header->{'datestamp'} eq $params{'dump'}->{'dump_timestamp'} &&
	    $header->{'dumplevel'} == $params{'dump'}->{'level'};

	die if $params{'expect_directtcp_supported'} and !$dtcp_supp;
	die if !$params{'expect_directtcp_supported'} and $dtcp_supp;

	my $xfer;
	my $xfer_dest;
	if ($params{'directtcp'}) {
	    $xfer_dest = Amanda::Xfer::Dest::DirectTCPListen->new();
	} else {
	    $xfer_dest = Amanda::Xfer::Dest::Null->new($params{'seed'});
	}

	$xfer = Amanda::Xfer->new([ $xfer_src, $xfer_dest ]);
	$running_xfers++;
	$xfer->start(sub { $clerk->handle_xmsg(@_); });

	if ($params{'directtcp'}) {
	    # use another xfer to read from that directtcp connection and verify
	    # it with Dest::Null
	    my $dest_xfer = Amanda::Xfer->new([
		Amanda::Xfer::Source::DirectTCPConnect->new($xfer_dest->get_addrs()),
		Amanda::Xfer::Dest::Null->new($params{'seed'}),
	    ]);
	    $running_xfers++;
	    $dest_xfer->start(sub {
		my ($src, $msg, $xfer) = @_;
		if ($msg->{type} == $XMSG_ERROR) {
		    die $msg->{elt} . " failed: " . $msg->{message};
		}
		if ($msg->{'type'} == $XMSG_DONE) {
		    $steps->{'maybe_done'}->();
		}
	    });
	}

	$clerk->start_recovery(
	    xfer => $xfer,
	    recovery_cb => $steps->{'recovery_cb'});
    };

    step recovery_cb => sub {
	$result = { @_ };
	$steps->{'maybe_done'}->();
    };

    step maybe_done => sub {
	$steps->{'verify'}->() unless --$running_xfers;
    };

    step verify => sub {
	# verify the results
	my $msg = $params{'msg'};
	if (@{$result->{'errors'}}) {
	    if ($params{'xfail'}) {
		if ($result->{'result'} ne 'FAILED') {
		    diag("expected failure, but got $result->{result}");
		    fail($msg);
		}
		is_deeply($result->{'errors'}, $params{'xfail'}, $msg);
	    } else {
		diag("errors:");
		for (@{$result->{'errors'}}) {
		    diag("$_");
		}
		if ($result->{'result'} ne 'FAILED') {
		    diag("XXX and result is " . $result->{'result'});
		}
		fail($msg);
	    }
	} else {
	    if ($result->{'result'} ne 'DONE') {
		diag("XXX no errors but result is " . $result->{'result'});
		fail($msg);
	    } else {
		pass($msg);
	    }
	}

	$finished_cb->();
    };

    Amanda::MainLoop::run();
}

sub quit_clerk {
    my ($clerk) = @_;

    $clerk->quit(finished_cb => make_cb(finished_cb => sub {
	my ($err) = @_;
	die "$err" if $err;

	Amanda::MainLoop::quit();
    }));
    Amanda::MainLoop::run();
    pass("clerk quit");
}

##
## Tests!
###

my $clerk;
my $feedback;
my @clerk_notif_parts;
my $chg = Amanda::Changer->new("chg-disk:$taperoot");
my $scan = Amanda::Recovery::Scan->new(chg => $chg);

$clerk = Amanda::Recovery::Clerk->new(scan => $scan, debug => 1);

try_recovery(
    clerk => $clerk,
    seed => 0xF000,
    dump => fake_dump("home", "/home", $datestamp, 0,
	{ label => 'TESTCONF01', filenum => 1 },
    ),
    msg => "one-part recovery successful");

try_recovery(
    clerk => $clerk,
    seed => 0xF001,
    dump => fake_dump("usr", "/usr", $datestamp, 0,
	{ label => 'TESTCONF01', filenum => 2 },
	{ label => 'TESTCONF01', filenum => 3 },
	{ label => 'TESTCONF02', filenum => 1 },
    ),
    msg => "multi-part recovery successful");

quit_clerk($clerk);

# recover from TESTCONF02, then 01, and then 02 again

@clerk_notif_parts = ();
$feedback = main::Feedback->new(
    clerk_notif_part => sub {
	push @clerk_notif_parts, [ $_[0], $_[1] ],
    },
);

$chg = Amanda::Changer->new("chg-disk:$taperoot");
$scan = Amanda::Recovery::Scan->new(chg => $chg);
$clerk = Amanda::Recovery::Clerk->new(scan => $scan, debug => 1,
				      feedback => $feedback);

try_recovery(
    clerk => $clerk,
    seed => 0xF002,
    dump => fake_dump("games", "/games", $datestamp, 0,
	{ label => 'TESTCONF02', filenum => 2 },
	{ label => 'TESTCONF02', filenum => 3 },
    ),
    msg => "two-part recovery from second tape successful");

is_deeply([ @clerk_notif_parts ], [
    [ 'TESTCONF02', 2 ],
    [ 'TESTCONF02', 3 ],
    ], "..and clerk_notif_part calls are correct");

try_recovery(
    clerk => $clerk,
    seed => 0xF001,
    dump => fake_dump("usr", "/usr", $datestamp, 0,
	{ label => 'TESTCONF01', filenum => 2 },
	{ label => 'TESTCONF01', filenum => 3 },
	{ label => 'TESTCONF02', filenum => 1 },
    ),
    msg => "multi-part recovery spanning tapes 1 and 2 successful");

try_recovery(
    clerk => $clerk,
    seed => 0xF001,
    dump => fake_dump("usr", "/usr", $datestamp, 0,
	{ label => 'TESTCONF01', filenum => 2 },
	{ label => 'TESTCONF01', filenum => 3 },
	{ label => 'TESTCONF02', filenum => 1 },
    ),
    directtcp => 1,
    msg => "multi-part recovery spanning tapes 1 and 2 successful, with directtcp");

try_recovery(
    clerk => $clerk,
    seed => $holding_key,
    dump => fake_dump("heldhost", "/to/holding", '21001010101010', 1,
	{ holding_file => $holding_file },
    ),
    msg => "holding-disk recovery");

try_recovery(
    clerk => $clerk,
    seed => $holding_key,
    dump => fake_dump("heldhost", "/to/holding", '21001010101010', 1,
	{ holding_file => $holding_file },
    ),
    directtcp => 1,
    msg => "holding-disk recovery, with directtcp");

# try some expected failures

try_recovery(
    clerk => $clerk,
    seed => $holding_key,
    dump => fake_dump("weldtoast", "/to/holding", '21001010101010', 1,
	{ holding_file => $holding_file },
    ),
    xfail => [ "header on '$holding_file' does not match expectations: " .
	        "got hostname 'heldhost'; expected 'weldtoast'" ],
    msg => "holding-disk recovery expected failure on header disagreement");

try_recovery(
    clerk => $clerk,
    seed => 0xF002,
    dump => fake_dump("XXXgames", "/games", $datestamp, 0,
	{ label => 'TESTCONF02', filenum => 2 },
    ),
    xfail => [ "header on 'TESTCONF02' file 2 does not match expectations: " .
	        "got hostname 'games'; expected 'XXXgames'" ],
    msg => "mismatched hostname detected");

try_recovery(
    clerk => $clerk,
    seed => 0xF002,
    dump => fake_dump("games", "XXX/games", $datestamp, 0,
	{ label => 'TESTCONF02', filenum => 2 },
    ),
    xfail => [ "header on 'TESTCONF02' file 2 does not match expectations: " .
	        "got disk '/games'; expected 'XXX/games'" ],
    msg => "mismatched disk detected");

try_recovery(
    clerk => $clerk,
    seed => 0xF002,
    dump => fake_dump("games", "/games", "XXX", 0,
	{ label => 'TESTCONF02', filenum => 2 },
    ),
    xfail => [ "header on 'TESTCONF02' file 2 does not match expectations: " .
	        "got datestamp '$datestamp'; expected 'XXX'" ],
    msg => "mismatched datestamp detected");

try_recovery(
    clerk => $clerk,
    seed => 0xF002,
    dump => fake_dump("games", "/games", $datestamp, 13,
	{ label => 'TESTCONF02', filenum => 2 },
    ),
    xfail => [ "header on 'TESTCONF02' file 2 does not match expectations: " .
	        "got dumplevel '0'; expected '13'" ],
    msg => "mismatched level detected");

quit_clerk($clerk);
rmtree($taperoot);

# try a recovery from a DirectTCP-capable device.  Note that this is the only real
# test of Amanda::Xfer::Source::Recovery's directtcp mode

SKIP: {
    skip "not built with ndmp and full client/server", 5 unless
	    Amanda::Util::built_with_component("ndmp")
	and Amanda::Util::built_with_component("client")
	and Amanda::Util::built_with_component("server");

    Installcheck::Dumpcache::load("ndmp");

    my $ndmp = Installcheck::Mock::NdmpServer->new(no_reset => 1);

    $ndmp->edit_config();
    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }

    my $tapelist = Amanda::Config::config_dir_relative("tapelist");
    my $tl = Amanda::Tapelist->new($tapelist);

    my $chg = Amanda::Changer->new();
    my $scan = Amanda::Recovery::Scan->new(chg => $chg);
    my $clerk = Amanda::Recovery::Clerk->new(scan => $scan, debug => 1);

    try_recovery(
	clerk => $clerk,
	seed => 0, # no verification
	dump => fake_dump("localhost", $Installcheck::Run::diskname,
			  $Installcheck::Dumpcache::timestamps[0], 0,
	    { label => 'TESTCONF01', filenum => 1 },
	),
	directtcp => 1,
	expect_directtcp_supported => 1,
	msg => "recovery of a real dump via NDMP and directtcp");
    quit_clerk($clerk);

    ## specification of the on-tape data
    my @xfer_info = (
	# length,	random, name ]
	[ 1024*160,	0xB000, "home" ],
    );
    my @to_write = (
	# (note that slots 1 and 2 are i/e slots, and are initially empty)
	# slot xfer		partnum
	[ 3,   $xfer_info[0],   1 ],
	[ 4,   $xfer_info[0],   2 ],
	[ 4,   $xfer_info[0],   3 ],
    );

    setup_changer(\&Amanda::MainLoop::quit, "ndmp_server", \@to_write, 64*1024);
    Amanda::MainLoop::run();
    pass("successfully set up ndmp test data");

    $chg = Amanda::Changer->new();
    $scan = Amanda::Recovery::Scan->new(chg => $chg);
    $clerk = Amanda::Recovery::Clerk->new(scan => $scan, debug => 1);

    try_recovery(
	clerk => $clerk,
	seed => 0xB000,
	dump => fake_dump("home", "/home", $datestamp, 0,
	    { label => 'TESTCONF03', filenum => 1 },
	    { label => 'TESTCONF04', filenum => 1 },
	    { label => 'TESTCONF04', filenum => 2 },
	),
	msg => "multi-part ndmp recovery successful",
	expect_directtcp_supported => 1);
    quit_clerk($clerk);
}

# cleanup
rmtree($taperoot);
unlink($holding_file);
