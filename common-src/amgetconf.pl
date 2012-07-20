#! @PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
use warnings;
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
eval 'use Amanda::Disklist;';  # can fail if compiled for client only
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
Usage: amgetconf [--client] [--execute-where client|server] [-l|--list] [-o configoption]* <config> <paramname>
  (any ordering of options and arguments is acceptable)

--client is equivalent to --execute-where client

--execute-where tells amgetconf whether to operate on the client or the 
server; the server is the default.

paramname can be one of
  dbopen.APPNAME -- open a debug file
  dbclose.APPNAME:FILENAME -- close debug file FILENAME
  build.PARAM -- get a build parameter
  PARAM -- get an Amanda configuration parameter

For all but Amanda configuration parameters, the <config> option is
ignored, but must be present.  For Amanda configuration parameters,
values in subsections are specified in the form TYPE:NAME:PARAMETER.

With --list, PARAM can be one of
EOF
    for my $name (keys %Amanda::Config::subsection_names) {
	print "    $name\n"
	    if $Amanda::Config::subsection_names{$name};
    }
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
    'use_version_suffixes' => 'no', # for backward compatibility
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
    'compress_path' => $Amanda::Constants::COMPRESS_PATH,
    'uncompress_path' => $Amanda::Constants::UNCOMPRESS_PATH,
    'aix_backup' => $Amanda::Constants::AIX_BACKUP,
    'dump_returns_1' => $Amanda::Constants::DUMP_RETURNS_1,

    # amanda modules

    'bsd_security' => $Amanda::Constants::BSD_SECURITY,
    'bsdudp_security' => $Amanda::Constants::BSDUDP_SECURITY,
    'bsdtcp_security' => $Amanda::Constants::BSDTCP_SECURITY,
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
    my ($appname, $filename);

    # copy amgetconf pname and pcontext
    my $pname = Amanda::Util::get_pname();
    my $pcontext = Amanda::Util::get_pcontext();

    if (($appname) = $parameter =~ /^dbopen\.(.*)/) {
	$appname =~ s/[^[:alnum:]]/_/g;
	# set pname and pcontext for the application
	Amanda::Util::set_pname($appname);
	Amanda::Util::set_pcontext($CONTEXT_CMDLINE);
	Amanda::Debug::dbopen("server");
	print Amanda::Debug::dbfn(), "\n";
    } elsif (($appname, $filename) = $parameter =~ /^dbclose\.([^:]*):(.*)/) {
	fail("debug file $filename does not exist") unless (-f $filename);
	# set pname and pcontext for the application
	Amanda::Util::set_pname($appname);
	Amanda::Util::set_pcontext($CONTEXT_CMDLINE);
	Amanda::Debug::dbreopen($filename, '');
	Amanda::Debug::dbclose();
	print "$filename\n";
    } else {
	fail("cannot parse $parameter");
    }
    # reset pname and pcontext for amgetconf
    Amanda::Util::set_pcontext($pcontext);
    Amanda::Util::set_pname($pname);
}

## regular configuration parameters

sub conf_param {
    my ($parameter, $opt_list) = @_;

    if ($opt_list) {
	# getconf_list will return an empty list for any unrecognized name,
	# so first check that the user has supplied a real subsection
	no_such_param($parameter)
	    unless defined($Amanda::Config::subsection_names{$parameter});
	my @list = getconf_list($parameter);

	for my $subsec (@list) {
	    print "$subsec\n";
	}
    } elsif ($parameter =~ /^property:/i) {
	my %properties = %{ getconf($CNF_PROPERTY)};
	my $propname = $parameter;
	$propname =~ s/^property://i;
	$propname = lc($propname);
	if (exists $properties{$propname}) {
	    print $properties{$propname}->{'values'}[0], "\n";
	}
    } elsif ($parameter =~ /^device-property:/i ||
	     $parameter =~ /^device_property:/i) {
	my %properties = %{ getconf($CNF_DEVICE_PROPERTY) };
	my $propname = $parameter;
	$propname =~ s/^device-property://i;
	$propname =~ s/^device_property://i;
	$propname = lc($propname);
	if (exists $properties{$propname}) {
	    print $properties{$propname}->{'values'}[0], "\n";
	}
    } else {
	no_such_param($parameter)
	    unless defined(getconf_byname($parameter));
	my @strs = getconf_byname_strs($parameter, 0);
	
	for my $str (@strs) {
	    print "$str\n";
	}
    }
}

Amanda::Util::setup_application("amgetconf", "server", $CONTEXT_SCRIPTUTIL);

## Command-line parsing

my $opt_list = '';
my $config_overrides = new_config_overrides($#ARGV+1);
my $execute_where = undef;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'list|l' => \$opt_list,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
    'execute-where=s' => sub {
        my $where = lc($_[1]);
        fail("Invalid value ($_[1]) for --execute-where. Must be client or server.") 
            unless $where eq 'client' or $where eq 'server';
        fail("--execute-where=server conflicts with --execute-where=client or --client.")
            unless !defined($execute_where) || (
                ($where eq 'client' && $execute_where) ||
                ($where eq 'server' && !$execute_where));
        $execute_where = ($where eq 'client')? $CONFIG_INIT_CLIENT : 0;
    },
    'client' => sub {
        fail("--execute-where=server conflicts with --execute-where=client or --client.")
            unless !defined($execute_where) || $execute_where;
        $execute_where = $CONFIG_INIT_CLIENT;
    }
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
    config_init(0|$execute_where, undef);
    build_param($parameter, $opt_list);
    Amanda::Util::finish_application();
    exit(0);
} 

if ($parameter =~ /^db(open|close)\./) {
    config_init(0|$execute_where, undef);
    db_param($parameter, $opt_list);
    Amanda::Util::finish_application();
    exit(0);
}

# finally, finish up the application startup procedure
set_config_overrides($config_overrides);
if ($execute_where == $CONFIG_INIT_CLIENT &&
    defined($config_name) && $config_name eq '.') {
    config_init($CONFIG_INIT_USE_CWD | $execute_where, undef);
} elsif ($execute_where == $CONFIG_INIT_CLIENT &&
    defined($config_name) && $config_name ne '.') {
    config_init($CONFIG_INIT_EXPLICIT_NAME | $execute_where, $config_name);
} elsif ($execute_where == $CONFIG_INIT_CLIENT) {
    config_init($execute_where, undef);
} else {
    config_init($CONFIG_INIT_EXPLICIT_NAME | $CONFIG_INIT_USE_CWD | $execute_where, $config_name);
}
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_ANY);

if ($execute_where != $CONFIG_INIT_CLIENT) {
    my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
    $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
#    if ($cfgerr_level >= $CFGERR_ERRORS) {
#	die "Errors processing disklist";
#    }
}

conf_param($parameter, $opt_list);

Amanda::Util::finish_application();
