use Test::More tests => 22;

use Amconfig;
use Installdirs;

# wrapper to call amgetconf and return the results
sub amgetconf {
    # open amgetconf and read from it
    my $cmd = "$sbindir/amgetconf " . join(" ", @_) . " 2>&1";
    my $result = `$cmd`;
    chomp $result;
    return $result;
}

# this is re-created for each test
my $testconf;

##
# First, try amgetconf out without a config

like(amgetconf(), qr(\AUsage: )i, 
    "bare 'amgetconf' gives usage message");
like(amgetconf("this-probably-doesnt-exist"), qr(could not open conf file)i, 
    "error message when configuration parameter doesn't exist");

##
# Next, work against a basically empty config

$testconf = Amconfig->new();
$testconf->write();

# test some defaults
is(amgetconf('TESTCONF', "reserve"), "100", 
    "reserve defaults to 100");
is(amgetconf('TESTCONF', "tapelist"), "tapelist", 
    "tapelist defaults to 'tapelist'");
is(amgetconf('TESTCONF', "usetimestamps"), "no", 
    "usetimestamps defaults to 'no'");

# test a nonexistent parameter
like(amgetconf('TESTCONF', "foos_per_bar"), qr/no such parameter/, 
    "handles nonexistent parameters");

# test build parameters (just the most common)
is(amgetconf('TESTCONF', "build.bindir"), $bindir, "build.bindir is correct");
is(amgetconf('TESTCONF', "build.sbindir"), $sbindir, "build.sbindir is correct");
is(amgetconf('TESTCONF', "build.libexecdir"), $libexecdir, "build.libexecdir is correct");
is(amgetconf('TESTCONF', "build.mandir"), $mandir, "build.mandir is correct");
is(amgetconf('TESTCONF', "build.AMANDA_DBGDIR"), $AMANDA_DBGDIR, "build.AMANDA_DBGDIR is correct");
is(amgetconf('TESTCONF', "build.AMANDA_TMPDIR"), $AMANDA_TMPDIR, "build.AMANDA_TMPDIR is correct");
is(amgetconf('TESTCONF', "build.CONFIG_DIR"), $CONFIG_DIR, "build.CONFIG_DIR is correct");

# dbopen, dbclose
my $dbfile = amgetconf('TESTCONF', "dbopen.foo");
like($dbfile, qr(^$AMANDA_DBGDIR/server/foo.[0-9]*.debug$),
    "'amgetconf dbopen.foo' returns a proper debug filename");
ok(-f $dbfile,
    "'amgetconf dbopen.foo' creates the debug file");
like(amgetconf('TESTCONF', "dbclose.foo"), qr/cannot parse/,
    "dbclose without filename fails");
is(amgetconf('TESTCONF', "dbclose.foo:$dbfile"), $dbfile, 
    "'amgetconf dbclose.foo:<filename>' returns the debug filename");

##
# Test an invalid config file

$testconf = Amconfig->new();
$testconf->add_param("foos_per_bar", "10");
$testconf->write();

like(amgetconf('TESTCONF', "foos_per_bar"), qr/errors processing config file/, 
    "gives error on invalid configuration");

##
# Now let's fill in some interesting values

$testconf = Amconfig->new();
$testconf->add_param("reserved-udp-port", '100,200');
$testconf->add_param("printer", '"/dev/lp"');
$testconf->add_param("reserve", '27');
$testconf->write();

is(amgetconf('TESTCONF', "reserved-udp-port"), "100,200", 
    "correctly returns portrange parameters from the file");
is(amgetconf('TESTCONF', "printer"), "/dev/lp", 
    "correctly returns string parameters from the file");
is(amgetconf('TESTCONF', "reserve"), "27", 
    "correctly returns integer parameters from the file");
is(amgetconf('TESTCONF', "rEsErVe"), "27", 
    "is case-insensitive");
