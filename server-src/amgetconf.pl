#! @PERL@
use lib '@amperldir@';
use strict;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
use Getopt::Long;

# Implementation note: this application is a bit funny, because it does not
# set up Amanda fully until some time into processing.  This lets it respond
# with build configuration information without a config file, and lets it set
# up debugging for the caller.  
#
# The most obvious consequence is that, rather than calling die (which interfaces
# with Amanda::Debug), this file uses a locally defined 'fail' to print error 
# messages.

sub usage {
    print <<EOF;
Usage: amgetconf [-l|--list] [-o configoption]* <config> <paramname>
  (any ordering of options and arguments is acceptable)
paramname can be one of
  dbopen.APPNAME -- open a debug file
  dbclose.APPNAME:FILENAME -- close debug file FILENAME
  build.PARAM -- get a build parameter
  PARAM -- get an Amanda configuration parameter

For all but Amanda configuration parameters, the <config> option is
ignored, but must be present.  For Amanda configuration parameters,
values in subsections are specified in the form TYPE:NAME:PARAMETER.

With --list, <paramname> can be one of
  build
  tapetype
  dumptype
  interface
  holdingdisk
  application-tool
  script-tool
EOF
    exit(1);
}

sub fail {
    print STDERR @_, "\n";
    exit(1);
}

sub no_such_param {
    my ($parameter) = @_;
    fail("amgetconf: no such parameter \"$parameter\"");
}

## build parameters

my %build_info = (
    # NOTE TO MAINTAINERS:
    #   If you add to this list, be sure to also add the new parameter 
    #   amgetconf(8) manual page.  Note that all keys are lower-case.

    ## directories from Amanda::Paths

    'bindir' => $bindir,
    'sbindir' => $sbindir,
    'libexecdir' => $libexecdir,
    'amlibexecdir' => $amlibexecdir,
    'mandir' => $mandir,
    'amanda_tmpdir' => $AMANDA_TMPDIR,
    'config_dir' => $CONFIG_DIR,
    'amanda_dbgdir' => $AMANDA_DBGDIR,
    'application_dir' => $APPLICATION_DIR,
    'gnutar_listed_incremental_dir' => $GNUTAR_LISTED_INCREMENTAL_DIR,
    'listed_inc_dir' => $GNUTAR_LISTED_INCREMENTAL_DIR, # (historical alias)

    ## constants from Amanda::Constants

    # build environment info

    'cc' => $Amanda::Constants::CC,
    'version' => $Amanda::Constants::VERSION,
    'assertions' => $Amanda::Constants::ASSERTIONS,
    'use_version_suffixes' => $Amanda::Constants::USE_VERSION_SUFFIXES,
    'locking' => $Amanda::Constants::LOCKING,

    # executable paths

    'dump' => $Amanda::Constants::DUMP,
    'restore' => $Amanda::Constants::RESTORE,
    'vdump' => $Amanda::Constants::VDUMP,
    'vrestore' => $Amanda::Constants::VRESTORE,
    'xfsdump' => $Amanda::Constants::XFSDUMP,
    'xfsrestore' => $Amanda::Constants::XFSRESTORE,
    'vxdump' => $Amanda::Constants::VXDUMP,
    'vxrestore' => $Amanda::Constants::VXRESTORE,
    'samba_client' => $Amanda::Constants::SAMBA_CLIENT,
    'gnutar' => $Amanda::Constants::GNUTAR,
    'star' => $Amanda::Constants::STAR,
    'mailer' => $Amanda::Constants::MAILER,
    'compress_path' => $Amanda::Constants::COMPRESS_PATH,
    'uncompress_path' => $Amanda::Constants::UNCOMPRESS_PATH,
    'aix_backup' => $Amanda::Constants::AIX_BACKUP,
    'dump_returns_1' => $Amanda::Constants::DUMP_RETURNS_1,

    # amanda modules

    'bsd_security' => $Amanda::Constants::BSD_SECURITY,
    'bsdudp_security' => $Amanda::Constants::BSDUDP_SECURITY,
    'bsdtcp_security' => $Amanda::Constants::BSDTCP_SECURITY,
    'krb4_security' => $Amanda::Constants::KRB4_SECURITY,
    'krb5_security' => $Amanda::Constants::KRB5_SECURITY,
    'ssh_security' => $Amanda::Constants::SSH_SECURITY,
    'rsh_security' => $Amanda::Constants::RSH_SECURITY,
    'use_amandahosts' => $Amanda::Constants::USE_AMANDAHOSTS,

    # build-time constants
    
    'amanda_debug_days' => $Amanda::Constants::AMANDA_DEBUG_DAYS,
    'default_server' => $Amanda::Constants::DEFAULT_SERVER,
    'default_amandates_file' => $Amanda::Constants::DEFAULT_AMANDATES_FILE,
    'default_config' => $Amanda::Constants::DEFAULT_CONFIG,
    'default_tape_server' => $Amanda::Constants::DEFAULT_TAPE_SERVER,
    'default_tape_device' => $Amanda::Constants::DEFAULT_TAPE_DEVICE,
    'client_login' => $Amanda::Constants::CLIENT_LOGIN,
    'use_rundump' => $Amanda::Constants::USE_RUNDUMP,
    'check_userid' => $Amanda::Constants::CHECK_USERID,

    # compression information

    'compress_suffix' => $Amanda::Constants::COMPRESS_SUFFIX,
    'compress_fast_opt' => $Amanda::Constants::COMPRESS_FAST_OPT,
    'compress_best_opt' => $Amanda::Constants::COMPRESS_BEST_OPT,
    'uncompress_opt' => $Amanda::Constants::UNCOMPRESS_OPT,

    # kerberos information

    'ticket_lifetime' => $Amanda::Constants::TICKET_LIFETIME,
    'server_host_principal' => $Amanda::Constants::SERVER_HOST_PRINCIPAL,
    'server_host_instance' => $Amanda::Constants::SERVER_HOST_INSTANCE,
    'server_host_key_file' => $Amanda::Constants::SERVER_HOST_KEY_FILE,
    'client_host_principal' => $Amanda::Constants::CLIENT_HOST_PRINCIPAL,
    'client_host_instance' => $Amanda::Constants::CLIENT_HOST_INSTANCE,
    'client_host_key_file' => $Amanda::Constants::CLIENT_HOST_KEY_FILE,
    # (historical typos:)
    'server_host_principle' => $Amanda::Constants::SERVER_HOST_PRINCIPAL,
    'client_host_principle' => $Amanda::Constants::CLIENT_HOST_PRINCIPAL,
    # (for testing purposes)
    '__empty' => '',

);

sub build_param {
    my ($parameter, $opt_list) = @_;

    if ($opt_list) {
	usage() unless ($parameter eq "build");

	for my $pname (sort keys %build_info) {
	    print "$pname\n";
	}
    } else {
	my ($pname) = $parameter =~ /^build\.(.*)/;

	my $val = $build_info{lc $pname};
	no_such_param($parameter) unless (defined($val));

	print "$val\n";
    }
}

## dbopen or dbclose

sub db_param {
    my ($parameter, $opt_list) = @_;

    if (my ($appname) = $parameter =~ /^dbopen\.(.*)/) {
	Amanda::Util::setup_application($appname, "server", $CONTEXT_CMDLINE);
	print Amanda::Debug::dbfn(), "\n";
    } elsif (my ($appname, $filename) = $parameter =~ /^dbclose\.([^:]*):(.*)/) {
	fail("debug file $filename does not exist") unless (-f $filename);
	Amanda::Debug::dbreopen($filename, '');
	Amanda::Debug::dbclose();
	print "$filename\n";
    } else {
	fail("cannot parse $parameter");
    }
}

## regular configuration parameters

sub conf_param {
    my ($parameter, $opt_list) = @_;

    if ($opt_list) {
	my @list = getconf_list($parameter);
	no_such_param($parameter) unless (@list);

	for my $subsec (@list) {
	    print "$subsec\n";
	}
    } else {
	my @strs = getconf_byname_strs($parameter, 0);
	no_such_param($parameter) unless (@strs);
	
	for my $str (@strs) {
	    print "$str\n";
	}
    }
}

## Command-line parsing

my $opt_list = '';
my $config_overwrites = new_config_overwrites($#ARGV+1);

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'list|l' => \$opt_list,
    'o=s' => sub { add_config_overwrite_opt($config_overwrites, $_[1]); },
) or usage();

my $config_name;
my $parameter;

if (@ARGV == 1) {
    $parameter = $ARGV[0];
} elsif (@ARGV >= 2) {
    # note that we ignore any arguments past these two.  Amdump lazily passes 
    # such arguments on to us, so we have no choice.
    $config_name = $ARGV[0];
    $parameter = $ARGV[1];
} else {
    usage();
}

## Now start looking at the parameter.

if ($parameter =~ /^build(?:\..*)?/) {
    build_param($parameter, $opt_list);
    exit(0);
} 

if ($parameter =~ /^db(open|close)\./) {
    db_param($parameter, $opt_list);
    exit(0);
}

# finally, finish up the application startup procedure
Amanda::Util::setup_application("amgetconf", "server", $CONTEXT_SCRIPTUTIL);
config_init($CONFIG_INIT_EXPLICIT_NAME | $CONFIG_INIT_USE_CWD, $config_name);
apply_config_overwrites($config_overwrites);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_ANY);

conf_param($parameter, $opt_list);
