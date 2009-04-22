#! @PERL@
# Copyright (c) 2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;

# This script interfaces the C changer library to Amanda::Perl.  It reads
# commands from its stdin that are identical to those that would be passed as
# arguments to a changer script, and replies with an encoded exit status and
# the response of the script.
#
# Specifically, the conversation is (P = Parent, C = Child)
# P>C: -$cmd $args
# C>P: EXITSTATUS $exitstatus
# C>P: $slot $message
# P>C: -$cmd $args
# C>P: EXITSTATUS $exitstatus
# C>P: $slot $message
# P>C: (EOF)
#
# The script exits as soon as it reads an EOF on its standard input.

use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Config qw( :init );
use Amanda::Util qw( :constants );
use Amanda::Debug qw( :logging );

my $chg;
my $res;

sub release_and_then {
    my ($release_opts, $andthen) = @_;
    if ($res) {
	# release the current reservation, then call andthen
	$res->release(@$release_opts,
	    finished_cb => sub {
		my ($error) = @_;
		$res = undef;

		if ($error) {
		    print "EXITSTATUS 1\n";
		    print "<error> $error\n";
		    Amanda::MainLoop::call_later(\&getcmd);
		} else {
		    $andthen->();
		}
	    }
	);
    } else {
	# no reservation to release
	$andthen->();
    }
}

sub do_slot {
    my ($slot) = @_;

    # handle the special cases we support
    if ($slot eq "next" or $slot eq "advance") {
	if (!$res) {
            $slot = "next";
	} else {
	    $slot = $res->{'next_slot'};
	}
    } elsif ($slot eq "first") {
	do_reset();
	return;
    } elsif ($slot eq "prev" or $slot eq "last") {
	print "EXITSTATUS 1\n";
	print "<error> slot specifier '$slot' is not valid\n";
	Amanda::MainLoop::call_later(\&getcmd);
	return;
    }

    my $load_slot = sub {
	$chg->load(slot => $slot, set_current => 1,
	    res_cb => sub {
		(my $error, $res) = @_;
		if ($error) {
		    print "EXITSTATUS 1\n";
		    print "<error> $error\n";
		} else {
		    print "EXITSTATUS 0\n";
		    print $res->{'this_slot'}, " ", $res->{'device_name'}, "\n";
		}
		Amanda::MainLoop::call_later(\&getcmd);
	    }
	);
    };

    release_and_then([], $load_slot);
}

sub do_info {
    $chg->info(info => [ 'num_slots' ],
        info_cb => sub {
            my $error = shift;
            my %results = @_;

            if ($error) {
                print "EXITSTATUS 1\n";
                print "<error> $error\n";
            } else {
                my $nslots = $results{'num_slots'};
                $nslots = 0 unless defined $nslots;
                print "EXITSTATUS 0\n";
                print "current $nslots 0 1\n";
            }
            Amanda::MainLoop::call_later(\&getcmd);
        }
    );
}

sub do_reset {
    my $do_reset = sub {
	$chg->reset(
	    finished_cb => sub {
		my ($error) = @_;
		if ($error) {
		    print "EXITSTATUS 1\n";
		    print "<error> $error\n";
		    Amanda::MainLoop::call_later(\&getcmd);
		} else {
		    do_slot("current");
		}
	    }
	);
    };
    release_and_then([], $do_reset);
}

sub do_clean {
    my $do_clean = sub {
	$chg->clean(
	    finished_cb => sub {
		my ($error) = @_;
		if ($error) {
		    print "EXITSTATUS 1\n";
		    print "<error> $error\n";
		    Amanda::MainLoop::call_later(\&getcmd);
		} else {
		    print "EXITSTATUS 0\n";
		    print "<none> cleaning operation successful\n";
		    Amanda::MainLoop::call_later(\&getcmd);
		}
	    },
	    drive => '',
	);
    };
    release_and_then([], $do_clean);
}

sub do_eject {
    my $do_eject = sub {
	$chg->eject(
	    finished_cb => sub {
		my ($error) = @_;
		if ($error) {
		    print "EXITSTATUS 1\n";
		    print "<error> $error\n";
		    Amanda::MainLoop::call_later(\&getcmd);
		} else {
		    print "EXITSTATUS 0\n";
		    print "<none> volume ejected\n";
		    Amanda::MainLoop::call_later(\&getcmd);
		}
	    },
	    drive => '',
	);
    };
    release_and_then([], $do_eject);
}

sub do_search {
    my ($label) = @_;
    my $load_label = sub {
	$chg->load(label => $label, set_current => 1,
	    res_cb => sub {
		(my $error, $res) = @_;
		if ($error) {
		    print "EXITSTATUS 1\n";
		    print "<error> $error\n";
		} else {
		    print "EXITSTATUS 0\n";
		    print $res->{'this_slot'}, " ", $res->{'device_name'}, "\n";
		}
		Amanda::MainLoop::call_later(\&getcmd);
	    }
	);
    };

    release_and_then([], $load_label);
}

sub do_label {
    my ($label) = @_;
    if ($res) {
        $res->set_label(label => $label,
            finished_cb => sub {
                my ($err) = @_;
                if ($err) {
		    print "EXITSTATUS 1\n";
		    print "<error> $err\n";
		} else {
		    print "EXITSTATUS 0\n";
		    print $res->{'this_slot'}, " ", $res->{'device_name'}, "\n";
		}
                Amanda::MainLoop::call_later(\&getcmd);
            }
        );
    } else {
	print "EXITSTATUS 1\n";
	print "<error> No volume loaded\n";
	Amanda::MainLoop::call_later(\&getcmd);
    }
}

sub getcmd {
    my ($slot, $label);
    my $command = <STDIN>;
    chomp $command;

    if (!defined($command)) {
	finish();
	return;
    }

    debug("got command '$command'");
    if (($slot) = ($command =~ /^-slot (.*)$/)) {
	do_slot($slot);
    } elsif ($command =~ /^-info$/) {
	do_info();
    } elsif ($command =~ /^-reset$/) {
	do_reset();
    } elsif ($command =~ /^-clean$/) {
	do_clean();
    } elsif ($command =~ /^-eject$/) {
	do_eject();
    } elsif (($label) = ($command =~ /^-search (.*)/)) {
	do_search($label);
    } elsif (($label) = ($command =~ /^-label (.*)/)) {
	do_label($label);
    } else {
	print "EXITSTATUS 2\n";
	print "<error> unknown command '$command'\n";
	finish();
    }
}

sub finish {
    if ($res) {
	$res->release(
	    finished_cb => sub {
		$res = undef;
		Amanda::MainLoop::quit();
	    }
	);
    } else {
	Amanda::MainLoop::quit();
    }
}

Amanda::Util::setup_application("chg-glue", "server", $CONTEXT_DAEMON);

die("$0 is for internal use only") if (@ARGV < 1);
my $config_name = $ARGV[0];

# override die to print a changer-compatible message
$SIG{__DIE__} = sub {
    my ($msg) = @_;
    die $msg unless defined $^S;
    print "EXITSTATUS 2\n";
    print "<error> $msg\n";
    exit 1;
};

config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}
Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# select unbuffered communication
$| = 1;

$chg = Amanda::Changer->new();
if ($chg->isa("Amanda::Changer::Error")) {
    die("Error creating changer: $chg");
}

Amanda::MainLoop::call_later(\&getcmd);
Amanda::MainLoop::run();
if ($res) {
    $res->release();
}
