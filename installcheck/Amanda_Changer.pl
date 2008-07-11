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

use Test::More tests => 20;
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

# work against a basically empty config; Amanda::Changer uses the current config
# when it opens devices to check their labels
my $testconf;
$testconf = Installcheck::Config->new();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $changer_filename = "$AMANDA_TMPDIR/chg-test";

sub setup_changer {
    my ($changer_script) = @_;

    open my $chg_test, ">", $changer_filename or die("Could not create test changer");
    
    $changer_script =~ s/\$AMANDA_TMPDIR/$AMANDA_TMPDIR/g;

    print $chg_test "#! /bin/sh\n";
    print $chg_test $changer_script;

    close $chg_test;
    chmod 0755, $changer_filename;
}

# OK, let's get started with some simple stuff
setup_changer <<'EOC';
case "${1}" in
    -slot)
        case "${2}" in
            1) echo "1 fake:1"; exit 0;;
            2) echo "<ignored> slot 2 is empty"; exit 1;;
            3) echo "1"; exit 0;; # test missing 'device' portion
        esac;;
    -reset) echo "reset ignored";;
    -eject) echo "eject ignored";;
    -clean) echo "clean ignored";;
    -label)
        case "${2}" in
            foo?bar) echo "1 ok"; exit 0;;
            *) echo "<error> bad label"; exit 1;;
        esac;;
    -info) echo "7 10 1 1"; exit 0;;
    -search) 
        case "${2}" in
            TAPE?01) echo "5 fakedev"; exit 0;;
            *) echo "<error> not found"; exit 2;;
        esac;;
esac
EOC

my $chg = Amanda::Changer->new($changer_filename);

# a callback that just stores a ref to its arguments in $result.
my $result;
sub keep_result_cb { 
    $result = [ @_ ]; 
    Amanda::MainLoop::quit();
}

$chg->loadslot(1, \&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [0, "1", "fake:1"],
    "A successful loadslot() returns the right stuff");

$chg->loadslot(2, \&keep_result_cb);
Amanda::MainLoop::run();
is($result->[0], "slot 2 is empty",
    "A loadslot() with a benign error returns the right stuff");

$chg->loadslot(3, \&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [0, "1", undef],
    "a response without a device string returns undef");

$chg->reset(\&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0, "reset" ],
    "reset() calls tapechanger -reset");

$chg->eject(\&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0, "eject" ],
    "eject() calls tapechanger -eject");

$chg->clean(\&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0, "clean" ],
    "clean() calls tapechanger -clean");

$chg->label("foo bar", \&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0 ],
    "label('foo bar') calls tapechanger -label 'foo bar' (note spaces)");

$chg->query(\&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0, 7, 10, 1, 1 ],
    "query() returns the correct values for a 4-value changer script");

$chg->find("TAPE 01", \&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0, "5", "fakedev" ],
    "find on a searchable changer invokes -search");

# TODO
#eval { Amanda::Changer::find("TAPE 02") };
#ok($@, "A searchable changer croaks when the label can't be found");

# Now a simple changer that returns three values for -info
setup_changer <<'EOC';
case "${1}" in
    -info) echo "11 13 0"; exit 0;;
esac
EOC

$chg->query(\&keep_result_cb);
Amanda::MainLoop::run();
is_deeply($result, [ 0, 11, 13, 0, 0 ],
    "query() returns the correct values for a 4-value changer script");

# set up 5 vtapes
for (my $i = 0; $i < 5; $i++) {
    my $vtapedir = "$AMANDA_TMPDIR/chg-test-tapes/$i/data";
    if (-e $vtapedir) {
        rmtree($vtapedir) 
            or die("Could not remove '$vtapedir'");
    }
    mkpath($vtapedir)
        or die("Could not create '$vtapedir'");
}

# label three of them (slot 2 is empty; slot 4 is unlabeled)
for (my $i = 0; $i < 5; $i++) {
    next if $i == 2 || $i == 4;
    my $dev = Amanda::Device->new("file:$AMANDA_TMPDIR/chg-test-tapes/$i")
        or die("Could not open device");
    $dev->start($Amanda::Device::ACCESS_WRITE, "TAPE$i", "19780615010203") 
        or die("Could not write label");
    $dev->finish();
}

# And finally a "stateful" changer that can support "scan" and "find"
setup_changer <<'EOC';
STATEFILE="$AMANDA_TMPDIR/chg-test-state"
SLOT=0
[ -f "$STATEFILE" ] && . "$STATEFILE"

case "${1}" in
    -slot)
        case "${2}" in
            current) ;;
            0|1|2|3|4|5) SLOT="${2}";;
            next|advance) SLOT=`expr $SLOT + 1`;;
            prev) SLOT=`expr $SLOT - 1`;;
            first) SLOT=0;;
            last) SLOT=4;;
        esac

        # normalize 0 <= $SLOT  < 5
        while [ "$SLOT" -ge 5 ]; do SLOT=`expr $SLOT - 5`; done
        while [ "$SLOT" -lt 0 ]; do SLOT=`expr $SLOT + 5`; done

        # signal an empty slot for slot 2
        if [ "$SLOT" = 2 ]; then
            echo "$SLOT slot $SLOT is empty"
            EXIT=1
        else
            echo "$SLOT" "file:$AMANDA_TMPDIR/chg-test-tapes/$SLOT"
        fi
        ;;
    -info) echo "$SLOT 5 1 0";;
esac

echo SLOT=$SLOT > $STATEFILE
exit $EXIT
EOC


$chg->loadslot(0, sub {
    my ($error, $slot, $device) = @_;
    die("Error loading slot 0: $error") if $error;
    $chg->find("TAPE3", \&keep_result_cb);
});
Amanda::MainLoop::run();
is_deeply($result, [0, "3", "file:$AMANDA_TMPDIR/chg-test-tapes/3"],
    "Finds a tape after skipping an empty slot");

$chg->loadslot(3, sub {
    my ($error, $slot, $device) = @_;
    die("Error loading slot 3: $error") if $error;
    $chg->find("TAPE1", \&keep_result_cb);
});
Amanda::MainLoop::run();
is_deeply($result, [0, "1", "file:$AMANDA_TMPDIR/chg-test-tapes/1"],
    "Finds a tape after skipping an unlabeled but filled slot");

my @scanresults;
my $scan_cb;

# scan the whole changer
@scanresults = (
    [ 0,            "0", "file:$AMANDA_TMPDIR/chg-test-tapes/0", 0, "scan starts with slot 0" ],
    [ 0,            "1", "file:$AMANDA_TMPDIR/chg-test-tapes/1", 0, "next in slot 1" ],
    [ "slot 2 is empty", undef, undef,                           0, "slot 2 is empty" ],
    [ 0,            "3", "file:$AMANDA_TMPDIR/chg-test-tapes/3", 0, "next in slot 3" ],
    [ 0,            "4", "file:$AMANDA_TMPDIR/chg-test-tapes/4", 0, "next in slot 4" ],
);

$scan_cb = sub {
    die("Callback called too many times") if (!@scanresults);
    my $expected = shift @scanresults;
    my $descr = pop @$expected;
    my $done = pop @$expected;
    is_deeply([ @_ ], $expected, $descr);
    return $done;
};

my $scan_done_cb = sub {
    my ($error) = @_;
    die ($error) if ($error);
    is_deeply([ @scanresults ], [], "scan_done_cb called when scan is done");
    Amanda::MainLoop::quit();
};

$chg->loadslot(0, sub {
    my ($error, $slot, $device) = @_;
    die("Error loading slot 0: $error") if $error;
    $chg->scan($scan_cb, $scan_done_cb);
});
Amanda::MainLoop::run();

# make sure the scan stops when $scan_cb returns 1

@scanresults = (
    [ 0, "0", "file:$AMANDA_TMPDIR/chg-test-tapes/0", 1, "scan starts with slot 0" ],
);
$chg->loadslot(0, sub {
    my ($error, $slot, $device) = @_;
    die("Error loading slot 0: $error") if $error;
    $chg->scan($scan_cb, $scan_done_cb);
});
Amanda::MainLoop::run();

# cleanup
unlink("$AMANDA_TMPDIR/chg-test");
unlink("$AMANDA_TMPDIR/chg-test-state");
rmtree("$AMANDA_TMPDIR/chg-test-tapes");
