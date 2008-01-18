use Test::More qw( no_plan );
use lib "@amperldir@";

use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err);
use Amanda::Paths;

my $testconf;
my $dumpok;

##
# First, try amgetconf out without a config

ok(!run('amcheckdump'),
    "amcheckdump with no arguments returns an error exit status");
like($Installcheck::Run::stdout, qr/\AUSAGE:/i, 
    ".. and gives usage message");

like(run_err('amcheckdump', 'this-probably-doesnt-exist'), qr(could not open conf file)i, 
    "run with non-existent config fails with an appropriate error message.");

##
# Now use a config with a vtape and without usetimestamps

$testconf = Installcheck::Run::setup();
$testconf->add_param('label_new_tapes', '"TESTCONF%%"');
$testconf->add_param('usetimestamps', 'no');
$testconf->write();

ok(run('amcheckdump', 'TESTCONF'),
    "amcheck with a new config succeeds");
like($Installcheck::Run::stdout, qr(could not find)i,
     "..but finds no dumps.");

ok($dumpok = run('amdump', 'TESTCONF'), "a dump runs successfully without usetimestamps");

SKIP: {
    skip "Dump failed", 1 unless $dumpok;
    like(run_get('amcheckdump', 'TESTCONF'), qr(Validating),
	"amdevcheck succeeds, claims to validate something (usetimestamps=no)");
}

##
# And a config with usetimestamps enabled

$testconf = Installcheck::Run::setup();
$testconf->add_param('label_new_tapes', '"TESTCONF%%"');
$testconf->add_param('usetimestamps', 'yes');
$testconf->write();

ok($dumpok = run('amdump', 'TESTCONF'), "a dump runs successfully with usetimestamps");

SKIP: {
    skip "Dump failed", 1 unless $dumpok;
    like(run_get('amcheckdump', 'TESTCONF'), qr(Validating),
	"amdevcheck succeeds, claims to validate something (usetimestamps=yes)");
}

Installcheck::Run::cleanup();
