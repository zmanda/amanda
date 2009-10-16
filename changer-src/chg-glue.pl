#! @PERL@
# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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
use Getopt::Long;

my $chg;
my $res;
my $res_device_name;

sub err_result {
    my ($err, $continuation, @cont_args) = @_;

    my $exitstatus = 1;

    if ($err->isa("Amanda::Changer::Error")) {
	$exitstatus = 2 if $err->fatal;
    } else {
	# if $err is a string, then the error is fatal.
	$exitstatus = 2;
    }

    debug("returning exit status $exitstatus: $err");

    print "EXITSTATUS $exitstatus\n";
    print "<error> $err\n";
    if (defined($continuation)) {
	Amanda::MainLoop::call_later($continuation, @cont_args);
    }
}

sub normal_result {
    my ($slot, $rest, $continuation, @cont_args) = @_;

    debug("returning success: $slot $rest");

    print "EXITSTATUS 0\n";
    print "$slot $rest\n";
    if (defined($continuation)) {
	Amanda::MainLoop::call_later($continuation, @cont_args);
    }
}

sub release_and_then {
    my ($release_opts, $andthen) = @_;
    if ($res) {
	# release the current reservation, then call andthen
	debug("releasing reservation of $res_device_name");
	$res->release(@$release_opts,
	    finished_cb => sub {
		my ($error) = @_;
		$res = undef;

		debug("release completed");
		if ($error) {
		    err_result($error, \&getcmd);
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
    my @slotarg = ();

    # handle the special cases we support
    if ($slot eq "next" or $slot eq "advance") {
	if (!$res) {
            @slotarg = (relative_slot => "next");
	} else {
	    @slotarg = (relative_slot => 'next', slot => $res->{'this_slot'});
	}
    } elsif ($slot eq "first") {
	do_reset(); # best we can do.. most old changers treat "reset" as "go to first"
	return;
    } elsif ($slot eq "current") {
	@slotarg = (relative_slot => "current");
    } elsif ($slot eq "prev" or $slot eq "last") {
	err_result("slot specifier '$slot' is not valid", \&getcmd);
	return;
    } else {
	@slotarg = (slot => $slot);
    }

    debug( join("|", @slotarg) );
    my $load_slot = sub {
	$chg->load(@slotarg, set_current => 1,
	    res_cb => sub {
		(my $error, $res) = @_;
		if ($error) {
		    err_result($error, \&getcmd);
		} else {
		    # get the name of the device so that the Amanda process
		    # can open it.  This is not forward-compatible, but will
		    # work for the current fleet of changers, as long as no
		    # configuration or properties are in effect.
		    $res_device_name = $res->{'device'}->device_name;

		    # close the device so that the parent process will be able
		    # to open a tape device.  This assumes that no other references
		    # to this device are outstanding.
		    $res->{'device'} = undef;

		    normal_result($res->{'this_slot'},
			    $res_device_name,
			    \&getcmd);
		}
	    }
	);
    };

    release_and_then([], $load_slot);
}

sub do_info {
    $chg->info(info => [ 'num_slots', 'fast_search' ],
        info_cb => sub {
            my $error = shift;
            my %results = @_;

            if ($error) {
		err_result($error, \&getcmd);
            } else {
                my $nslots = $results{'num_slots'} or 0;
		my $searchable = $results{'fast_search'}? 1:0;
		normal_result("current", "$nslots 1 $searchable", \&getcmd);
            }
        }
    );
}

sub do_reset {
    my $do_reset = sub {
	$chg->reset(
	    finished_cb => sub {
		my ($error) = @_;
		if ($error) {
		    err_result($error, \&getcmd);
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
		    err_result($error, \&getcmd);
		} else {
		    normal_result("<none>", "cleaning operation successful", \&getcmd);
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
		    err_result($error, \&getcmd);
		} else {
		    normal_result("<none>", "volume ejected", \&getcmd);
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
		    err_result($error, \&getcmd);
		} else {
		    # get the name of the device so that the Amanda process
		    # can open it.  This is not forward-compatible, but will
		    # work for the current fleet of changers, as long as no
		    # configuration or properties are in effect.
		    $res_device_name = $res->{'device'}->device_name;

		    # close the device so that the parent process will be able
		    # to open a tape device.  This assumes that no other references
		    # to this device are outstanding.
		    $res->{'device'} = undef;

		    normal_result($res->{'this_slot'},
				$res_device_name,
				\&getcmd);
		}
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
                my ($error) = @_;
                if ($error) {
		    err_result($error, \&getcmd);
		} else {
		    normal_result($res->{'this_slot'},
				$res_device_name,
				\&getcmd);
		}
            }
        );
    } else {
	err_result("No volume loaded", \&getcmd);
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
	err_exit(2, "unknown command '$command'", \&finish);
    }
}

sub finish {
    release_and_then([], \&Amanda::MainLoop::quit);
}

my $config_overrides = new_config_overrides($#ARGV+1);
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); }
);

Amanda::Util::setup_application("chg-glue", "server", $CONTEXT_DAEMON);

die("$0 is for internal use only") if (@ARGV < 1);
my $config_name = $ARGV[0];

# override die to print a changer-compatible message
$SIG{__DIE__} = sub {
    my ($msg) = @_;
    die $msg unless defined $^S;
    err_result($msg, undef);
    exit 2; # just in case
};

config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
apply_config_overrides($config_overrides);
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
