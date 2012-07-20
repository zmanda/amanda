# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 54;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Device qw( :constants );;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Tapelist;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

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
    my ($config, $tpchanger) = @_;

    my $self = {
	config => $config,
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
    } elsif (exists $params{'slot'} or exists $params{'relative_slot'}) {
	my $slot = $params{'slot'};
	if (exists $params{'relative_slot'}) {
	    if ($params{'relative_slot'} eq "current") {
		$slot = $self->{'curslot'};
	    } elsif ($params{'relative_slot'} eq "next") {
		$slot = ($self->{'curslot'} + 1) % (scalar @{$self->{'slots'}});
	    } else {
		die "invalid relative_slot";
	    }
	}

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

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;

    if ($key eq 'num_slots') {
	$results{$key} = 13;
    } elsif ($key eq 'mkerror1') {
	return $self->make_error("failed", $params{'info_cb'},
	    reason => "unknown",
	    message => "err1");
    } elsif ($key eq 'mkerror2') {
	return $self->make_error("failed", $params{'info_cb'},
	    reason => "unknown",
	    message => "err2");
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
}

sub reset {
    my $self = shift;
    my %params = @_;

    $self->{'curslot'} = 0;

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
}

sub clean {
    my $self = shift;
    my %params = @_;

    $self->{'clean'} = 1;

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
}

sub inventory {
    my $self = shift;
    my %params = @_;

    Amanda::MainLoop::call_later($params{'inventory_cb'},
	undef, [ {
	    slot => 1,
	    empty => 0,
	    label => 'TAPE-99',
	    barcode => '09385A',
	    reserved => 0,
	    import_export => 0,
	    loaded_in => undef,
	}]);
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

    $self->{'device'} = Amanda::Device->new("null:slot-$slot");
    $self->{'this_slot'} = $slot;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;
    my $slot = $self->{'slot'};
    my $chg = $self->{'chg'};

    $chg->{'reserved_slots'} = [ grep { $_ != $slot } @{$chg->{'reserved_slots'}} ];

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
}

sub set_label {
    my $self = shift;
    my %params = @_;
    my $slot = $self->{'slot'};
    my $chg = $self->{'chg'};

    $self->{'chg'}->{'slots'}->[$self->{'slot'}] = $params{'label'};
    $self->{'label'} = $params{'label'};

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
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
    'property' => '"testprop" "testval"',
]);
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# check out the relevant changer properties
my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist->new($tlf);
my $chg = Amanda::Changer->new("mychanger", tapelist => $tl);
is($chg->{'config'}->get_property("testprop"), "testval",
    "changer properties are correctly represented");
is($chg->have_inventory(), 1, "changer have inventory");
my @new_tape_label = $chg->make_new_tape_label();
is_deeply(\@new_tape_label, [undef, "template is not set, you must set autolabel"], "no make_new_tape_label");
is($chg->make_new_meta_label(), undef, "no make_new_meta_label");

$chg = Amanda::Changer->new("mychanger", tapelist => $tl,
			    labelstr => "TESTCONF-[0-9][0-9][0-9]-[a-z][a-z][a-z]-[0-9][0-9][0-9]",
			    autolabel => { template => '$c-$m-$b-%%%',
					   other_config => 1,
					   non_amanda => 1,
					   volume_error => 0,
					   empty => 1 },
			    meta_autolabel => "%%%");
my $meta = $chg->make_new_meta_label();
is($meta, "001", "meta 001");
my $label = $chg->make_new_tape_label(meta => $meta, barcode => 'aaa');
is($label, 'TESTCONF-001-aaa-001', "label TESTCONF-001-aaa-001");

is($chg->volume_is_labelable($DEVICE_STATUS_VOLUME_UNLABELED, $Amanda::Header::F_EMPTY),
   1, "empty volume is labelable");
is($chg->volume_is_labelable($DEVICE_STATUS_VOLUME_ERROR, undef),
   0, "empty volume is labelable");

# test loading by label
{
    my @labels;
    my @reservations;
    my ($getres, $rq_reserved, $relres);

    $getres = make_cb('getres' => sub {
	if (!@labels) {
	    return $rq_reserved->();
	}

	my $label = pop @labels;

	$chg->load(label => $label,
                   set_current => ($label eq "TAPE-02"),
		   res_cb => sub {
	    my ($err, $res) = @_;
	    ok(!$err, "no error loading $label")
		or diag($err);

	    # keep this reservation
	    push @reservations, $res if $res;

	    # and start on the next
	    $getres->();
	});
    });

    $rq_reserved = make_cb(rq_reserved => sub {
	# try to load an already-reserved volume
	$chg->load(label => 'TAPE-00',
		   res_cb => sub {
	    my ($err, $res) = @_;
	    ok($err, "error when requesting already-reserved volume");
	    push @reservations, $res if $res;

	    $relres->();
	});
    });

    $relres = make_cb('relres' => sub {
	if (!@reservations) {
	    return Amanda::MainLoop::quit();
	}

	my $res = pop @reservations;
	$res->release(finished_cb => sub {
	    my ($err) = @_;
	    die $err if $err;

	    $relres->();
	});
    });

    # start the loop
    @labels = ( 'TAPE-02', 'TAPE-00', 'TAPE-03' );
    $getres->();
    Amanda::MainLoop::run();

    $relres->();
    Amanda::MainLoop::run();

    @labels = ( 'TAPE-00', 'TAPE-01' );
    $getres->();
    Amanda::MainLoop::run();

    # explicitly release the reservations (without using the callback)
    for my $res (@reservations) {
        $res->release();
    }
}

# test loading by slot
{
    my ($start, $first_cb, $released, $second_cb, $quit);
    my $slot;

    # reserves the current slot
    $start = make_cb('start' => sub {
        $chg->load(res_cb => $first_cb, relative_slot => "current");
    });

    # gets a reservation for the "current" slot
    $first_cb = make_cb('first_cb' => sub {
        my ($err, $res) = @_;
        die $err if $err;

        is($res->{'this_slot'}, 2,
            "'current' slot loads slot 2");
        is($res->{'device'}->device_name, "null:slot-2",
            "..device is correct");

	$slot = $res->{'this_slot'};
	$res->release(finished_cb => $released);
    });

    $released = make_cb(released => sub {
	my ($err) = @_;

        $chg->load(res_cb => $second_cb, relative_slot => 'next',
		   slot => $slot, set_current => 1);
    });

    # gets a reservation for the "next" slot
    $second_cb = make_cb('second_cb' => sub {
        my ($err, $res) = @_;
        die $err if $err;

        is($res->{'this_slot'}, 3,
            "next slot loads slot 3");
        is($chg->{'curslot'}, 3,
            "..which is also now the current slot");

	$res->release(finished_cb => $quit);
    });

    $quit = make_cb(quit => sub {
	my ($err) = @_;
	die $err if $err;

        Amanda::MainLoop::quit();
    });

    $start->();
    Amanda::MainLoop::run();
}

# test set_label
{
    my ($start, $load1_cb, $set_cb, $released, $load2_cb, $released2, $load3_cb);
    my $res;

    # load TAPE-00
    $start = make_cb('start' => sub {
        $chg->load(res_cb => $load1_cb, label => "TAPE-00");
    });

    # rename it to TAPE-99
    $load1_cb = make_cb('load1_cb' => sub {
        (my $err, $res) = @_;
        die $err if $err;

        pass("loaded TAPE-00");
        $res->set_label(label => "TAPE-99", finished_cb => $set_cb);
    });

    $set_cb = make_cb('set_cb' => sub {
	my ($err) = @_;

	$res->release(finished_cb => $released);
    });

    # try to load TAPE-00
    $released = make_cb('released' => sub {
        my ($err) = @_;
        die $err if $err;

        pass("relabeled TAPE-00 to TAPE-99");
        $chg->load(res_cb => $load2_cb, label => "TAPE-00");
    });

    # try to load TAPE-99
    $load2_cb = make_cb('load2_cb' => sub {
        (my $err, $res) = @_;
        ok($err, "loading TAPE-00 is now an error");

        $chg->load(res_cb => $load3_cb, label => "TAPE-99");
    });

    # check result
    $load3_cb = make_cb('load3_cb' => sub {
        (my $err, $res) = @_;
        die $err if $err;

        pass("but loading TAPE-99 is ok");

	$res->release(finished_cb => $released2);
    });

    $released2 = make_cb(released2 => sub {
	my ($err) = @_;
	die $err if $err;

        Amanda::MainLoop::quit();
    });

    $start->();
    Amanda::MainLoop::run();
}

# test reset and clean and inventory
sub test_simple {
    my ($finished_cb) = @_;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step do_reset => sub {
        $chg->reset(finished_cb => sub {
            is($chg->{'curslot'}, 0,
                "reset() resets to slot 0");
            $steps->{'do_clean'}->();
        });
    };

    step do_clean => sub {
        $chg->clean(finished_cb => sub {
            ok($chg->{'clean'}, "clean 'cleaned' the changer");
	    $steps->{'do_inventory'}->();
        });
    };

    step do_inventory => sub {
        $chg->inventory(inventory_cb => sub {
	    is_deeply($_[1], [ {
		    slot => 1,
		    empty => 0,
		    label => 'TAPE-99',
		    barcode => '09385A',
		    reserved => 0,
		    import_export => 0,
		    loaded_in => undef,
		}], "inventory returns an inventory");
	    $finished_cb->();
        });
    };
}
test_simple(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

# test info
{
    my ($do_info, $check_info, $do_info_err, $check_info_err);

    $do_info = make_cb('do_info' => sub {
        $chg->info(info_cb => $check_info,
	    info => [ 'num_slots' ]);
    });

    $check_info = make_cb('check_info' => sub {
	my ($err, %results) = @_;
	die($err) if $err;
	is_deeply(\%results, { 'num_slots' => 13 },
	    "info() works");
	$do_info_err->();
    });

    $do_info_err = make_cb('do_info_err' => sub {
        $chg->info(info_cb => $check_info_err,
	    info => [ 'mkerror1', 'mkerror2' ]);
    });

    $check_info_err = make_cb('check_info_err' => sub {
	my ($err, %results) = @_;
	is($err,
	  "While getting info key 'mkerror1': err1; While getting info key 'mkerror2': err2",
	  "info errors are handled correctly");
	is($err->{'type'}, 'failed', "error has type 'failed'");
	ok($err->failed, "\$err->failed is true");
	ok(!$err->fatal, "\$err->fatal is false");
	is($err->{'reason'}, 'unknown', "\$err->{'reason'} is 'unknown'");
	ok($err->unknown, "\$err->unknown is true");
	ok(!$err->notimpl, "\$err->notimpl is false");
	Amanda::MainLoop::quit();
    });

    $do_info->();
    Amanda::MainLoop::run();
}
$chg->quit();

# Test the various permutations of configuration setup, with a patched
# _new_from_uri so we can monitor the result
sub my_new_from_uri {
    my ($uri, $cc, $name) = @_;
    return $uri if (ref $uri and $uri->isa("Amanda::Changer::Error"));
    return [ $uri, $cc? "cc" : undef ];
}
*saved_new_from_uri = *Amanda::Changer::_new_from_uri;
*Amanda::Changer::_new_from_uri = *my_new_from_uri;

sub loadconfig {
    my ($global_tapedev, $global_tpchanger, $defn_tpchanger, $custom_defn) = @_;

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

    if (defined($custom_defn)) {
	$testconf->add_changer("customchanger", $custom_defn);
	$testconf->add_param('tpchanger', '"customchanger"');
    }

    $testconf->write();

    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }
}

sub assert_invalid {
    my ($global_tapedev, $global_tpchanger, $defn_tpchanger, $custom_defn,
	$name, $regexp, $msg) = @_;
    loadconfig($global_tapedev, $global_tpchanger, $defn_tpchanger, $custom_defn);
    my $err = Amanda::Changer->new($name);
    if ($err->isa("Amanda::Changer::Error")) {
	like($err->{'message'}, $regexp, $msg);
    } else {
	diag("Amanda::Changer->new did not return an Error object:");
	diag("".Dumper($err));
	fail($msg);
    }
}

assert_invalid(undef, undef, undef, undef, undef,
    qr/You must specify one of 'tapedev' or 'tpchanger'/,
    "supplying a nothing is invalid");

loadconfig(undef, "file:/foo", undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-single:file:/foo", undef ],
    "default changer with global tpchanger naming a device");

loadconfig(undef, "chg-disk:/foo", undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/foo", undef ],
    "default changer with global tpchanger naming a changer");

loadconfig(undef, "mychanger", "chg-disk:/bar", undef);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/bar", "cc" ],
    "default changer with global tpchanger naming a defined changer with a uri");

loadconfig(undef, "mychanger", "chg-zd-mtx", undef);
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", "cc" ],
    "default changer with global tpchanger naming a defined changer with a compat script");

loadconfig(undef, "chg-zd-mtx", undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", undef ],
    "default changer with global tpchanger naming a compat script");

loadconfig("tape:/dev/foo", undef, undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-single:tape:/dev/foo", undef ],
    "default changer with global tapedev naming a device and no tpchanger");

assert_invalid("tape:/dev/foo", "tape:/dev/foo", undef, undef, undef,
    qr/Cannot specify both 'tapedev' and 'tpchanger'/,
    "supplying a device for both tpchanger and tapedev is invalid");

assert_invalid("tape:/dev/foo", "chg-disk:/foo", undef, undef, undef,
    qr/Cannot specify both 'tapedev' and 'tpchanger'/,
    "supplying a device for tapedev and a changer for tpchanger is invalid");

loadconfig("tape:/dev/foo", 'chg-zd-mtx', undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", undef ],
    "default changer with global tapedev naming a device and a global tpchanger naming a compat script");

assert_invalid("chg-disk:/foo", "tape:/dev/foo", undef, undef, undef,
    qr/Cannot specify both 'tapedev' and 'tpchanger'/,
    "supplying a changer for tapedev and a device for tpchanger is invalid");

loadconfig("chg-disk:/foo", undef, undef, undef);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/foo", undef ],
    "default changer with global tapedev naming a device");

loadconfig("mychanger", undef, "chg-disk:/bar", undef);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/bar", "cc" ],
    "default changer with global tapedev naming a defined changer with a uri");

loadconfig("mychanger", undef, "chg-zd-mtx", undef);
is_deeply( Amanda::Changer->new(), [ "chg-compat:chg-zd-mtx", "cc" ],
    "default changer with global tapedev naming a defined changer with a compat script");

loadconfig(undef, undef, "chg-disk:/foo", undef);
is_deeply( Amanda::Changer->new("mychanger"), [ "chg-disk:/foo", "cc" ],
    "named changer loads the proper definition");

loadconfig(undef, undef, undef, [
    tapedev => '"chg-disk:/foo"',
]);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/foo", "cc" ],
    "defined changer with tapedev loads the proper definition");

loadconfig(undef, undef, undef, [
    tpchanger => '"chg-disk:/bar"',
]);
is_deeply( Amanda::Changer->new(), [ "chg-disk:/bar", "cc" ],
    "defined changer with tpchanger loads the proper definition");

assert_invalid(undef, undef, undef, [
	tpchanger => '"chg-disk:/bar"',
	tapedev => '"file:/bar"',
    ], undef,
    qr/Cannot specify both 'tapedev' and 'tpchanger'/,
    "supplying both a new tpchanger and tapedev in a definition is invalid");

assert_invalid(undef, undef, undef, [
	property => '"this" "will not work"',
    ], undef,
    qr/You must specify one of 'tapedev' or 'tpchanger'/,
    "supplying neither a tpchanger nor tapedev in a definition is invalid");

*Amanda::Changer::_new_from_uri = *saved_new_from_uri;

# test with_locked_state *within* a process

sub test_locked_state {
    my ($finished_cb) = @_;
    my $chg;
    my $stfile = "$Installcheck::TMP/test-statefile";
    my $num_outstanding = 0;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg };

    step start => sub {
	$chg = Amanda::Changer->new("chg-null:");

	for my $num (qw( one two three )) {
	    ++$num_outstanding;
	    $chg->with_locked_state($stfile, $steps->{'maybe_done'}, sub {
		my ($state, $maybe_done) = @_;

		$state->{$num} = $num;
		$state->{'count'}++;

		Amanda::MainLoop::call_after(50, $maybe_done, undef, $state);
	    });
	}
    };

    step maybe_done => sub {
	my ($err, $state) = @_;
	die $err if $err;

	return if (--$num_outstanding);

	is_deeply($state, {
	    one => "one",
	    two => "two",
	    three => "three",
	    count => 3,
	}, "state is maintained correctly (within a process)");

	unlink($stfile) if -f $stfile;

	$finished_cb->();
    };
}
test_locked_state(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
