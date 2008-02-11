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

use Test::More tests => 16;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Device;
use Amanda::Debug;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

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

# set up and load a simple config with a tpchanger
my $testconf = Installcheck::Config->new();
$testconf->add_param('tpchanger', "\"$changer_filename\"");
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') or die("Could not load test config");

# some variables we'll need
my ($error, $slot, $device);

# OK, let's get started with some simple stuff
setup_changer <<'EOC';
case "${1}" in
    -slot)
        case "${2}" in
            1) echo "1 fake:1"; exit 0;;
            2) echo "<ignored> slot 2 is empty"; exit 1;;
            3) echo "<error> oh noes!"; exit 2;;
            4) echo "1"; exit 0;; # test missing 'device' portion
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

is_deeply([ Amanda::Changer::loadslot(1) ], [0, "1", "fake:1"],
    "A successful loadslot() returns the right stuff");

($error, $slot, $device) = Amanda::Changer::loadslot(2);
is($error, "slot 2 is empty", "A loadslot() with a benign error returns the right stuff");

eval { Amanda::Changer::loadslot(3); };
like($@, qr/.*oh noes!.*/, "A loadslot() with a serious error croaks");

is_deeply([ Amanda::Changer::loadslot(4) ], [0, "1", undef],
    "a response without a device string returns undef");

is_deeply([ Amanda::Changer::reset() ], [ 0, "reset" ],
    "reset() calls tapechanger -reset");
is_deeply([ Amanda::Changer::eject() ], [ 0, "eject" ],
    "eject() calls tapechanger -eject");
is_deeply([ Amanda::Changer::clean() ], [ 0, "clean" ],
    "clean() calls tapechanger -clean");

is_deeply([ Amanda::Changer::label("foo bar") ], [ 0 ],
    "label('foo bar') calls tapechanger -label 'foo bar' (note spaces)");

is_deeply([ Amanda::Changer::query() ], [ 0, 7, 10, 1, 1 ],
    "query() returns the correct values for a 4-value changer script");

is_deeply([ Amanda::Changer::find("TAPE 01") ], [ 0, "5", "fakedev" ],
    "find on a searchable changer invokes -search");

eval { Amanda::Changer::find("TAPE 02") };
ok($@, "A searchable changer croaks when the label can't be found");

# Now a simple changer that returns three values for -info
setup_changer <<'EOC';
case "${1}" in
    -info) echo "11 13 0"; exit 0;;
esac
EOC

is_deeply([ Amanda::Changer::query() ], [ 0, 11, 13, 0, 0 ],
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

($error, $slot, $device) = Amanda::Changer::loadslot(0);
if ($error) { die("Error loading slot 0: $error"); }
is_deeply([ Amanda::Changer::find("TAPE3") ], [0, "3", "file:$AMANDA_TMPDIR/chg-test-tapes/3"],
    "Finds a tape after skipping an empty slot");

($error, $slot, $device) = Amanda::Changer::loadslot(3);
if ($error) { die("Error loading slot 3: $error"); }
is_deeply([ Amanda::Changer::find("TAPE1") ], [0, "1", "file:$AMANDA_TMPDIR/chg-test-tapes/1"],
    "Finds a tape after skipping an unlabeled but filled slot");

my @scanresults;
sub cb {
    fail("called too many times") if (!@scanresults);
    my $expected = shift @scanresults;
    my $descr = pop @$expected;
    my $done = pop @$expected;
    is_deeply([ @_ ], $expected, $descr);
    return 1;
}

# scan the whole changer
($error, $slot, $device) = Amanda::Changer::loadslot(0);
if ($error) { die("Error loading slot 0: $error"); }
@scanresults = (
    [ "0", "file:$AMANDA_TMPDIR/chg-test-tapes/0", 0, 0, "scan starts with slot 0" ],
    [ "1", "file:$AMANDA_TMPDIR/chg-test-tapes/1", 0, 0, "next in slot 1" ],
    [ undef, undef, "slot 2 is empty",                0, "slot 2 is empty" ],
    [ "3", "file:$AMANDA_TMPDIR/chg-test-tapes/3", 0, 0, "next in slot 3" ],
    [ "4", "file:$AMANDA_TMPDIR/chg-test-tapes/4", 0, 0, "next in slot 4" ],
);
Amanda::Changer::scan(\&cb);

# make sure it stops when "done"
($error, $slot, $device) = Amanda::Changer::loadslot(0);
if ($error) { die("Error loading slot 0: $error"); }
@scanresults = (
    [ "0", "file:$AMANDA_TMPDIR/chg-test-tapes/0", 0, 1, "scan starts with slot 0" ],
);
Amanda::Changer::scan(\&cb);

# cleanup
unlink("$AMANDA_TMPDIR/chg-test");
unlink("$AMANDA_TMPDIR/chg-test-state");
rmtree("$AMANDA_TMPDIR/chg-test-tapes");
