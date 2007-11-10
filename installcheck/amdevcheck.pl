use Test::More qw( no_plan );

use Amconfig;
use Installdirs;

sub amdevcheck {
    my $cmd = "$sbindir/amdevcheck " . join(" ", @_) . " 2>&1";
    my $result = `$cmd`;
    chomp $result;
    return $result;
}

my $testconf;

##
# First, try amgetconf out without a config

like(amdevcheck(), qr(\AUsage: )i, 
    "bare 'amdevcheck' gives usage message");
like(amdevcheck("this-probably-doesnt-exist"), qr(could not open conf file)i, 
    "error message when configuration parameter doesn't exist");

##
# Next, work against a basically empty config

# this is re-created for each test
$testconf = Amconfig->new();
$testconf->add_param("tapedev", '"/dev/null"');
$testconf->write();

# test some defaults
like(amdevcheck('TESTCONF'), qr{File /dev/null is not a tape device},
    "uses tapedev by default");

##
# Now use a config with a vtape

# this is re-created for each test
$testconf = Amconfig->new();
$testconf->setup_vtape();
$testconf->write();

is_deeply([ sort split "\n", amdevcheck('TESTCONF') ],
	  [ sort "VOLUME_UNLABELED", "VOLUME_ERROR", "DEVICE_ERROR" ],
    "empty vtape described as VOLUME_UNLABELED, VOLUME_ERROR, DEVICE_ERROR");

like(amdevcheck('TESTCONF', "/dev/null"), qr{File /dev/null is not a tape device},
    "can override device on the command line");
