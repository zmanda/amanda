# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 35;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Device;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# --------
# define a "test" changer for purposes of this installcheck

package Amanda::Changer::test;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

# monkey-patch our test changer into Amanda::Changer, and indicate that
# the module has already been required by adding a key to %INC
$INC{'Amanda/Changer/test.pm'} = "Amanda_Changer";

sub new {
    my $class = shift;
    my ($cc, $tpchanger) = @_;

    my $self = {
	curslot => 0,
	slots => [ 'TAPE-00', 'TAPE-01', 'TAPE-02', 'TAPE-03' ],
	reserved_slots => [],
        clean => 0,
    };
    bless ($self, $class);
    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;

    my $cb = $params{'res_cb'};

    if (exists $params{'label'}) {
	# search by label
	my $slot = -1;
	my $label = $params{'label'};

	for my $i (0 .. $#{$self->{'slots'}}) {
	    if ($self->{'slots'}->[$i] eq $label) {
		$slot = $i;
		last;
	    }
	}
	if ($slot == -1) {
	    $cb->("No such label '$label'", undef);
	    return;
	}

	# check that it's not in use
	for my $used_slot (@{$self->{'reserved_slots'}}) {
	    if ($used_slot == $slot) {
		$cb->("Volume with label '$label' is already in use", undef);
		return;
	    }
	}

	# ok, let's use it.
	push @{$self->{'reserved_slots'}}, $slot;

        if (exists $params{'set_current'} && $params{'set_current'}) {
            $self->{'curslot'} = $slot;
        }

	$cb->(undef, Amanda::Changer::test::Reservation->new($self, $slot, $label));
    } elsif (exists $params{'slot'}) {
	my $slot = $params{'slot'};
	$slot = $self->{'curslot'}
	    if ($slot eq "current");

	if (grep { $_ == $slot } @{$self->{'reserved_slots'}}) {
	    $cb->("Slot $slot is already in use", undef);
            return;
	}
        my $label = $self->{'slots'}->[$slot];
        push @{$self->{'reserved_slots'}}, $slot;

        if (exists $params{'set_current'} && $params{'set_current'}) {
            $self->{'curslot'} = $slot;
        }

        $cb->(undef, Amanda::Changer::test::Reservation->new($self, $slot, $label));
    } else {
	die "No label or slot parameter given";
    }
}

sub reset {
    my $self = shift;
    my %params = @_;

    $self->{'curslot'} = 0;

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}

sub clean {
    my $self = shift;
    my %params = @_;

    $self->{'clean'} = 1;

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}


package Amanda::Changer::test::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $slot, $label) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;
    $self->{'slot'} = $slot;
    $self->{'label'} = $label;

    $self->{'device_name'} = "test:slot-$slot";
    $self->{'this_slot'} = $slot;
    $self->{'next_slot'} = ($slot + 1) % (scalar @{$chg->{'slots'}});

    return $self;
}

sub release {
    my $self = shift;
    my %params = @_;
    my $slot = $self->{'slot'};
    my $chg = $self->{'chg'};

    $chg->{'reserved_slots'} = [ grep { $_ != $slot } @{$chg->{'reserved_slots'}} ];

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}

sub set_label {
    my $self = shift;
    my %params = @_;
    my $slot = $self->{'slot'};
    my $chg = $self->{'chg'};

    $self->{'chg'}->{'slots'}->[$self->{'slot'}] = $params{'label'};
    $self->{'label'} = $params{'label'};

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}

# --------
# back to the perl tests..

package main;

# work against a config specifying our test changer, to work out the kinks
# when it opens devices to check their labels
my $testconf;
$testconf = Installcheck::Config->new();
$testconf->add_changer("mychanger", [
    'tpchanger' => '"chg-test:/foo"',
]);
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# test loading by label

my $chg = Amanda::Changer->new("mychanger");
{
    my @labels = ( 'TAPE-02', 'TAPE-00', 'TAPE-03' );
    my @reservations = ();
    my $getres;

    $getres = sub {
	my $label = pop @labels;

	$chg->load(label => $label,
                   set_current => ($label eq "TAPE-02"),
		   res_cb => sub {
	    my ($err, $reservation) = @_;
	    ok(!$err, "no error loading $label")
		or diag($err);

	    # keep this reservation
	    if ($reservation) {
		push @reservations, $reservation;
	    }

	    # and start on the next
	    if (@labels) {
		$getres->();
		return;
	    } else {
		# try to load an already-reserved volume
		$chg->load(label => 'TAPE-00',
			   res_cb => sub {
		    my ($err, $reservation) = @_;
		    ok($err, "error when requesting already-reserved volume");
		    Amanda::MainLoop::quit();
		});
	    }
	});
    };

    # start the loop
    Amanda::MainLoop::call_later($getres);
    Amanda::MainLoop::run();

    # ditch the reservations and do it all again
    @reservations = ();
    @labels = ( 'TAPE-00', 'TAPE-01' );
    is_deeply($chg->{'reserved_slots'}, [],
	"reservations are released when the Reservation object goes out of scope");
    Amanda::MainLoop::call_later($getres);
    Amanda::MainLoop::run();

    # explicitly release the reservations (without using the callback)
    for my $res (@reservations) {
        $res->release();
    }
}

# test loading by slot
{
    my ($start, $first_cb, $second_cb);

    # reserves the current slot
    $start = sub {
        $chg->load(res_cb => $first_cb, slot => "current");
    };

    # gets a reservation for the "current" slot
    $first_cb = sub {
        my ($err, $res) = @_;
        die $err if $err;

        is($res->{'this_slot'}, 2,
            "'current' slot loads slot 2");
        is($res->{'device_name'}, "test:slot-2",
            "..device is correct");
        is($res->{'next_slot'}, 3,
            "..and the next slot is slot 3");
        $chg->load(res_cb => $second_cb, slot => $res->{'next_slot'}, set_current => 1);
    };

    # gets a reservation for the "next" slot
    $second_cb = sub {
        my ($err, $res) = @_;
        die $err if $err;

        is($res->{'this_slot'}, 3,
            "next slot loads slot 3");
        is($chg->{'curslot'}, 3,
            "..which is also now the current slot");
        is($res->{'next_slot'}, 0,
            "..and the next slot is slot 0");

        Amanda::MainLoop::quit();
    };

    Amanda::MainLoop::call_later($start);
    Amanda::MainLoop::run();
}

# test set_label
{
    my ($start, $load1_cb, $set_cb, $load2_cb, $load3_cb);

    # load TAPE-00
    $start = sub {
        $chg->load(res_cb => $load1_cb, label => "TAPE-00");
    };

    # rename it to TAPE-99
    $load1_cb = sub {
        my ($err, $res) = @_;
        die $err if $err;

        pass("loaded TAPE-00");
        $res->set_label(label => "TAPE-99", finished_cb => $set_cb);
        $res->release();
    };

    # try to load TAPE-00
    $set_cb = sub {
        my ($err) = @_;
        die $err if $err;

        pass("relabeled TAPE-00 to TAPE-99");
        $chg->load(res_cb => $load2_cb, label => "TAPE-00");
    };

    # try to load TAPE-99
    $load2_cb = sub {
        my ($err, $res) = @_;

        ok($err, "loading TAPE-00 is now an error");
        $chg->load(res_cb => $load3_cb, label => "TAPE-99");
    };

    # check result
    $load3_cb = sub {
        my ($err, $res) = @_;
        die $err if $err;

        pass("but loading TAPE-99 is ok");

        Amanda::MainLoop::quit();
    };

    Amanda::MainLoop::call_later($start);
    Amanda::MainLoop::run();
}

# test reset and clean
{
    my ($do_reset, $do_clean);

    $do_reset = sub {
        $chg->reset(finished_cb => sub {
            is($chg->{'curslot'}, 0,
                "reset() resets to slot 0");
            $do_clean->();
        });
    };

    $do_clean = sub {
        $chg->clean(finished_cb => sub {
            ok($chg->{'clean'}, "clean 'cleaned' the changer");
            Amanda::MainLoop::quit();
        });
    };

    Amanda::MainLoop::call_later($do_reset);
    Amanda::MainLoop::run();
}

# Test the various permutations of configuration setup, with a patched
# _new_from_uri so we can monitor the result
sub my_new_from_uri {
    my ($uri, $cc, $name) = @_;
    return [ $uri, $cc? "cc" : undef ];
}
*saved_new_from_uri = *Amanda::Changer::_new_from_uri;
*Amanda::Changer::_new_from_uri = *my_new_from_uri;

sub loadconfig {
    my ($global_tapedev, $global_tpchanger, $defn_tpchanger) = @_;

    $testconf = Installcheck::Config->new();

    if (defined($global_tapedev)) {
	$testconf->add_param('tapedev', "\"$global_tapedev\"")
    }

    if (defined($global_tpchanger)) {
	$testconf->add_param('tpchanger', "\"$global_tpchanger\"")
    }

    if (defined($defn_tpchanger)) {
	$testconf->add_changer("mychanger", [
	    'tpchanger' => "\"$defn_tpchanger\"",
	]);
    }

    $testconf->write();

    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }
}

sub assert_invalid {
    my ($global_tapedev, $global_tpchanger, $defn_tpchanger, $name, $msg) = @_;
    loadconfig($global_tapedev, $global_tpchanger, $defn_tpchanger);
    eval { Amanda::Changer->new($name); };
    ok($@, $msg);
}

assert_invalid(undef, undef, undef, undef,
    "supplying a nothing is invalid");

loadconfig(undef, "file:/foo", undef);
is_deeply( Amanda::Changer->new(), [ "chg-single:file:/foo", undef ],
    "default changer with global tpchanger naming a device");

loadconfig(undef, "chg-disk:/foo", undef);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/foo", undef ],
    "default changer with global tpchanger naming a changer");

loadconfig(undef, "mychanger", "chg-disk:/bar");
is_deeply( Amanda::Changer->new(), [ "chg-disk:/bar", "cc" ],
    "default changer with global tpchanger naming a defined changer with a uri");

loadconfig(undef, "mychanger", "chg-zd-mtx");
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", "cc" ],
    "default changer with global tpchanger naming a defined changer with a compat script");

loadconfig(undef, "chg-zd-mtx", undef);
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", undef ],
    "default changer with global tpchanger naming a compat script");

loadconfig("tape:/dev/foo", undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-single:tape:/dev/foo", undef ],
    "default changer with global tapedev naming a device and no tpchanger");

assert_invalid("tape:/dev/foo", "tape:/dev/foo", undef, undef,
    "supplying a device for both tpchanger and tapedev is invalid");

assert_invalid("tape:/dev/foo", "chg-disk:/foo", undef, undef,
    "supplying a device for tapedev and a changer for tpchanger is invalid");

loadconfig("tape:/dev/foo", 'chg-zd-mtx', undef);
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", undef ],
    "default changer with global tapedev naming a device and a global tpchanger naming a compat script");

assert_invalid("chg-disk:/foo", "tape:/dev/foo", undef, undef,
    "supplying a changer for tapedev and a device for tpchanger is invalid");

loadconfig("chg-disk:/foo", undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/foo", undef ],
    "default changer with global tapedev naming a device");

loadconfig("mychanger", undef, "chg-disk:/bar");
is_deeply( Amanda::Changer->new(), [ "chg-disk:/bar", "cc" ],
    "default changer with global tapedev naming a defined changer with a uri");

loadconfig("mychanger", undef, "chg-zd-mtx");
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", "cc" ],
    "default changer with global tapedev naming a defined changer with a compat script");

loadconfig(undef, undef, "chg-disk:/foo");
is_deeply( Amanda::Changer->new("mychanger"), [ "chg-disk:/foo", "cc" ],
    "named changer loads the proper definition");

*Amanda::Changer::_new_from_uri = *saved_new_from_uri;
