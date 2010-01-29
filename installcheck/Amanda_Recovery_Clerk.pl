# Copyright (c) 2010 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 14;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Config qw( :init );
use Amanda::Changer;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::Header;
use Amanda::DB::Catalog;
use Amanda::Xfer qw( :constants );
use Amanda::Recovery::Clerk;
use Amanda::MainLoop;

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;
$testconf = Installcheck::Config->new();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $taperoot = "$Installcheck::TMP/Amanda_Recovery_Clerk";
my $datestamp = "20100101010203";

# set up a 2-tape disk changer with some spanned dumps in it, and add those
# dumps to the catalog, too.  To avoid re-implementing Amanda::Taper::Scan, this
# uses individual transfers for each part.

{
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
	[ 1,   $xfer_info[0],   1 ],
	[ 1,   $xfer_info[1],   1 ],
	[ 1,   $xfer_info[1],   2 ],
	[ 2,   $xfer_info[1],   3 ],
	[ 2,   $xfer_info[2],   1 ],
	[ 2,   $xfer_info[2],   2 ],
    );

    my $res;
    my $chg = Amanda::Changer->new("chg-disk:$taperoot");
    my $label;
    my %subs;
    my ($slot, $xfer_info, $partnum);

    $subs{'next'} = make_cb(next => sub {
	return $subs{'done'}->() unless @to_write;

	($slot, $xfer_info, $partnum) = @{shift @to_write};
	die "xfer len <= 0" if $xfer_info->[0] <= 0;

	if (!$res || $res->{'this_slot'} != $slot) {
	    $subs{'new_dev'}->();
	} else {
	    $subs{'run_xfer'}->();
	}
    });

    $subs{'new_dev'} = make_cb(new_dev => sub {
	if ($res) {
	    $res->release(finished_cb => $subs{'released'});
	} else {
	    $subs{'released'}->();
	}
    });

    $subs{'released'} = make_cb(released => sub {
	my ($err) = @_;
	die "$err" if $err;

	$chg->load(slot => $slot, res_cb => $subs{'loaded'});
    });

    $subs{'loaded'} = make_cb(loaded => sub {
	(my $err, $res) = @_;
	die "$err" if $err;

	my $dev = $res->{'device'};
	my $next_write = $to_write[0];

	# label the device
	$label = "TESTCONF0" . $next_write->[0];
	$dev->start($Amanda::Device::ACCESS_WRITE, $label, $datestamp)
	    or die("starting dev: " . $dev->error_or_status());

	$res->set_label(label => $label, finished_cb => $subs{'run_xfer'});
    });

    $subs{'run_xfer'} = make_cb(run_xfer => sub {
	my $dev = $res->{'device'};
	my $name = $xfer_info->[2];

	my $hdr = Amanda::Header->new();
	$hdr->{'type'} = $Amanda::Header::F_SPLIT_DUMPFILE;
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
	$len = 512*1024 if $len > 512*1024;
	my $key = $xfer_info->[1];

	my $xsrc = Amanda::Xfer::Source::Random->new($len, $key);
	my $xdst = Amanda::Xfer::Dest::Device->new($dev, 1024*256);
	my $xfer = Amanda::Xfer->new([$xsrc, $xdst]);

	$xfer->start(sub {
	    my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    }
	    if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
			partnum => $partnum,
			nparts => -1,
			kb => $len / 1024,
			sec => 1.2,
		    });

		# and do the next part
		$subs{'next'}->();
	    }
	});
    });

    $subs{'done'} = make_cb(done => sub {
	if ($res) {
	    $res->release(finished_cb => $subs{'done_released'});
	} else {
	    $subs{'done_released'}->();
	}
    });

    $subs{'done_released'} = make_cb(done_released => sub {
	Amanda::MainLoop::quit();
    });

    $subs{'next'}->();
    Amanda::MainLoop::run();
    pass("successfully set up test data");
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
	my ($label, $filenum) = ($part->{'label'}, $part->{'filenum'});
	push @{$pldump->{'parts'}}, {
	    label => $label,
	    filenum => $filenum,
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

sub notif_part {
    my $self = shift;

    if (exists $self->{'notif_part'}) {
	$self->{'notif_part'}->(@_);
    } else {
	$self->SUPER::notif_part(@_);
    }
}

sub volume_not_found {
    my $self = shift;

    if (exists $self->{'volume_not_found'}) {
	$self->{'volume_not_found'}->(@_);
    } else {
	$self->SUPER::volume_not_found(@_);
    }
}

package main;

# run a recovery with the given plan on the given clerk, expecting a bytestream with
# the given random seed.
sub try_recovery {
    my %params = @_;
    my $clerk = $params{'clerk'};
    my $result;
    my %subs;

    $subs{'start'} = make_cb(start => sub {
	$clerk->get_xfer_src(
	    dump => $params{'dump'},
	    xfer_src_cb => $subs{'xfer_src_cb'});
    });

    $subs{'xfer_src_cb'} = make_cb(xfer_src_cb => sub {
	my ($errors, $header, $xfer_src) = @_;

	# simulate errors for xfail, below
	if ($errors) {
	    $result = { result => "FAILED", errors => $errors };
	    Amanda::MainLoop::quit();
	    return;
	}

	# double-check the header; the Clerk should have checked this, so these
	# are die's, for simplicity
	die unless
	    $header->{'name'} eq $params{'dump'}->{'hostname'} &&
	    $header->{'disk'} eq $params{'dump'}->{'diskname'} &&
	    $header->{'datestamp'} eq $params{'dump'}->{'dump_timestamp'} &&
	    $header->{'dumplevel'} == $params{'dump'}->{'level'};

	my $xfer = Amanda::Xfer->new([
	    $xfer_src,
	    Amanda::Xfer::Dest::Null->new($params{'seed'})
	]);

	$xfer->start(sub { $clerk->handle_xmsg(@_); });

	$clerk->start_recovery(
	    xfer => $xfer,
	    recovery_cb => $subs{'recovery_cb'});
    });

    $subs{'recovery_cb'} = make_cb(recovery_cb => sub {
	$result = { @_ };
	Amanda::MainLoop::quit();
    });

    $subs{'start'}->();
    Amanda::MainLoop::run();

    # verify the results

    my $msg = $params{'msg'};
    if (@{$result->{'errors'}}) {
	if ($params{'xfail'}) {
	    if ($result->{'result'} ne 'FAILED') {
		diag("expected failure, but got $result->{result}");
		fail($msg);
	    }
	    is_deeply($result->{'errors'}, $params{'xfail'}, $msg);
	    return;
	}
	diag("errors:");
	for (@{$result->{'errors'}}) {
	    diag("$_");
	}
	if ($result->{'result'} ne 'FAILED') {
	    diag("XXX and result is " . $result->{'result'});
	}
	fail($msg);
    } else {
	if ($result->{'result'} ne 'DONE') {
	    diag("XXX no errors but result is " . $result->{'result'});
	    fail($msg);
	} else {
	    pass($msg);
	}
    }
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
my @notif_parts;
my $chg = Amanda::Changer->new("chg-disk:$taperoot");

$clerk = Amanda::Recovery::Clerk->new(changer => $chg, debug => 1);

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

@notif_parts = ();
$feedback = main::Feedback->new(
    notif_part => sub {
	push @notif_parts, [ $_[0], $_[1] ],
    },
);

$clerk = Amanda::Recovery::Clerk->new(changer => $chg, debug => 1,
				    feedback => $feedback);

try_recovery(
    clerk => $clerk,
    seed => 0xF002,
    dump => fake_dump("games", "/games", $datestamp, 0,
	{ label => 'TESTCONF02', filenum => 2 },
	{ label => 'TESTCONF02', filenum => 3 },
    ),
    msg => "two-part recovery from second tape successful");

is_deeply([ @notif_parts ], [
    [ 'TESTCONF02', 2 ],
    [ 'TESTCONF02', 3 ],
    ], "..and notif_part calls are correct");

try_recovery(
    clerk => $clerk,
    seed => 0xF001,
    dump => fake_dump("usr", "/usr", $datestamp, 0,
	{ label => 'TESTCONF01', filenum => 2 },
	{ label => 'TESTCONF01', filenum => 3 },
	{ label => 'TESTCONF02', filenum => 1 },
    ),
    msg => "multi-part recovery spanning tapes 1 and 2 successful");

# try some expected failures

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

# use volume_not_found with no changer

my $gave_bogus_device = 0;
$feedback = main::Feedback->new(
    volume_not_found => sub {
	my ($err, $label, $res_cb) = @_;

	if (!$gave_bogus_device) {
	    $gave_bogus_device = 1;
	    # wrong slot (it wants TESETCONF01) should trigger another call to
	    # volume_not_found
	    return $chg->load(label => 'TESTCONF02', res_cb => $res_cb);
	} else {
	    return $chg->load(label => $label, res_cb => $res_cb);
	}
    },
);

quit_clerk($clerk);

$clerk = Amanda::Recovery::Clerk->new(debug => 1,
				    feedback => $feedback);

try_recovery(
    clerk => $clerk,
    seed => 0xF001,
    dump => fake_dump("usr", "/usr", $datestamp, 0,
	{ label => 'TESTCONF01', filenum => 2 },
	{ label => 'TESTCONF01', filenum => 3 },
	{ label => 'TESTCONF02', filenum => 1 },
    ),
    msg => "multi-part recovery using volume_not_found (manual mode)");

quit_clerk($clerk);

# cleanup
rmtree($taperoot);
