use Test::More qw( no_plan );

use Amconfig;

use lib "@amperldir@";
use Amanda::Paths;

sub amcheckdump {
    my $cmd = "$sbindir/amcheckdump " . join(" ", @_) . " 2>&1";
    my $result = `$cmd`;
    chomp $result;
    return $result;
}

my $testconf;

##
# First, try amgetconf out without a config

like(amcheckdump(), qr/\AUSAGE:/i, 
    "bare 'amcheckdump' gives usage message");
like(amcheckdump("this-probably-doesnt-exist"), qr(could not open conf file)i, 
    "error message when configuration parameter doesn't exist");

##
# Now use a config with a vtape

# this is re-created for each test
$testconf = Amconfig->new();
$testconf->setup_vtape();
$testconf->write();

like(amcheckdump("TESTCONF"), qr(could not find)i,
     "'amcheckdump' on a brand-new config finds no dumps.");
