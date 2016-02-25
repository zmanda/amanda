#! @PERL@
# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use POSIX qw(WIFEXITED WEXITSTATUS strftime);
use File::Glob qw( :glob );
use File::Compare;
use File::Copy;
use Socket;        # This defines PF_INET and SOCK_STREAM
use IO::Socket::SSL;

use Amanda::Config qw( :init :getconf );
use Amanda::Util qw( :constants );
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Debug qw( debug );
use Amanda::Paths;
use Amanda::Amdump;

##
# Main

sub usage {
    my ($msg) = @_;
    print STDERR <<EOF;
Usage: amssl <conf> [--client] [--init | -create-ca | --create-server-cert NAME | --create-client-cert NAME] [--confirm] [-o configoption]*
                    [--cacert CA-CERT-FILE] [--cakey CA-KEY-FILE]
EOF
    print STDERR "$msg\n" if $msg;
    exit 1;
}

Amanda::Util::setup_application("amssl", "server", $CONTEXT_DAEMON, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;

my $opt_client = 0;
my $opt_init = 0;
my $opt_ca = 0;
my $opt_server_cert = 0;
my $opt_client_cert = 0;
my $opt_country;
my $opt_state;
my $opt_locality;
my $opt_organisation;
my $opt_organisation_unit;
my $opt_common;
my $opt_email;
my $opt_batch;
my $opt_server;
my $opt_port;
my $opt_cacert;
my $opt_cakey;
my $opt_config;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version'              => \&Amanda::Util::version_opt,
    'help|usage|?'         => \&usage,
    'client'               => \$opt_client,
    'init'                 => \$opt_init,
    'create-ca'            => \$opt_ca,
    'create-server-cert:s' => \$opt_server_cert,
    'create-client-cert:s' => \$opt_client_cert,
    'country:s'            => \$opt_country,
    'state:s'              => \$opt_state,
    'locality:s'           => \$opt_locality,
    'organisation:s'       => \$opt_organisation,
    'organisation_unit:s'  => \$opt_organisation_unit,
    'common:s'             => \$opt_common,
    'email:s'              => \$opt_email,
    'batch'                => \$opt_batch,
    'server:s'             => \$opt_server,
    'config:s'             => \$opt_config,
    'port:s'               => \$opt_port,
    'cacert:s'             => \$opt_cacert,
    'cakey:s'              => \$opt_cakey,
) or usage();

set_config_overrides($config_overrides);
if ($opt_client) {
    config_init_with_global($CONFIG_INIT_EXPLICIT_NAME | $CONFIG_INIT_CLIENT, $opt_config);
} else {
    config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
}
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

$opt_port = 10081 if !defined $opt_port;

# never change these values, that will create incompatibility between version
my $MSG_SERVER_FINGERPRINT = 1;
my $MSG_SERVER_CRT  = 2;
my $MSG_CSR         = 3;
my $MSG_CRT         = 4;
my $MSG_CA_CRT      = 5;

my $ssl_config;
my $ca_cert_file;
my $ca_key_file;
my $batch = " -batch " if $opt_batch;

my $SSL_DIR = getconf($CNF_SSL_DIR);
if ($opt_init) {
    mkdir $SSL_DIR;
    write_openssl_cnf_template();
    my $data = { 'config' => { 'COUNTRY_NAME_VALUE'           => $opt_country,
			       'STATE_VALUE'                  => $opt_state,
			       'LOCALITY_NAME_VALUE'          => $opt_locality,
			       'ORGANIZATION_NAME_VALUE'      => $opt_organisation,
			       'ORGANIZATION_UNIT_NAME_VALUE' => $opt_organisation_unit,
			       'COMMON_NAME_VALUE'            => $opt_common,
			       'EMAIL_ADDRESS_VALUE'          => $opt_email,
		             },
		 'CA_CERT_FILE'                 => $opt_cacert,
		 'CA_KEY_FILE'                  => $opt_cakey,
	       };
    open SSL_DATA, ">$SSL_DIR/openssl.data";
    my $dumper = Data::Dumper->new([ $data ], ["ssl_config"]);
    $dumper->Purity(1);
    print SSL_DATA $dumper->Dump;
    close SSL_DATA;

    if ($opt_cacert && -f $opt_cacert) {
	copy($opt_cacert, "$SSL_DIR/CA/crt.pem")
    }
} elsif ($opt_client && $opt_client_cert) {

    if (-l "$SSL_DIR/remote/$opt_client_cert") {
	die("$opt_client_cert is a server");
    }

    my $hostdir = "$SSL_DIR/me";
    mkdir $hostdir;
    mkdir "$hostdir/private";
    $opt_common = $opt_client_cert if !defined $opt_common;
    my $openssl_cnf_file = "$hostdir/openssl.cnf";
    load_data_config();
    write_openssl_cnf($openssl_cnf_file);

    # generate private key
    system("openssl genrsa -out $hostdir/private/key.pem 1024");

    # generate csr
    system("openssl req -config $openssl_cnf_file $batch -new -key $hostdir/private/key.pem -out $hostdir/csr.pem");

    select( ( select(STDOUT), $| = 1 )[0] );

    print "\nConnecting to server... ";
    # connect to client
    my $fd = IO::Socket::SSL->new(
	# where to connect
	PeerHost => $opt_server,
	PeerPort => "$opt_port",
	SSL_verify_mode => 0,
	) or die "failed connect or ssl handshake: $!,$SSL_ERROR";

    print "  connected!\n";

    my $tmp_server_dir = "$SSL_DIR/remote/tmp";
    mkdir "$SSL_DIR/remote";
    mkdir $tmp_server_dir;

    # get server fingerprint
    get_message_file($fd, $MSG_SERVER_FINGERPRINT, "$tmp_server_dir/fingerprint.new");

    # find certificate name
    my $server_hostdir = "$SSL_DIR/remote/$opt_server";
    mkdir "$server_hostdir";

    # compare server fingerprint with the current one
    if (-f "$server_hostdir/fingerprint") {
	if (compare("$tmp_server_dir/fingerprint.new","$server_hostdir/fingerprint") != 0) {
	    die("ERROR: fingerprint from server differ from what we have ($server_hostdir/fingerprint)");
	}
	unlink "$tmp_server_dir/fingerprint.new";
    } else {
	# save server fingerprint
	rename "$tmp_server_dir/fingerprint.new", "$server_hostdir/fingerprint";
    }

    # send $hostdir/csr.pem to server
    send_message_file($fd, $MSG_CSR, "$hostdir/csr.pem");
    unlink "$hostdir/csr.pem";

    # get $hostdir/crt.pem from server
    get_message_file($fd, $MSG_CRT, "$hostdir/crt.pem");

    # get server CA cert
    mkdir "$hostdir/CA";
    get_message_file($fd, $MSG_CA_CRT, "$hostdir/CA/crt.pem");

    # compute my fingerprint
    system("openssl x509 -in $hostdir/crt.pem -fingerprint -noout > $hostdir/fingerprint");

    symlink "../me", "$SSL_DIR/remote/opt_client_cert";


} elsif ($opt_ca) {
    mkdir "$SSL_DIR/CA";
    mkdir "$SSL_DIR/CA/private";
    my $openssl_cnf_file = "$SSL_DIR/CA/openssl.cnf";
    load_data_config();
    write_openssl_cnf($openssl_cnf_file);
    system("openssl genrsa -des3 -out $SSL_DIR/CA/private/key.pem 1024");
    system("openssl req -config $openssl_cnf_file $batch -new -key $SSL_DIR/CA/private/key.pem -out $SSL_DIR/CA/csr.pem");
    system("openssl x509 -days 3650 -signkey $SSL_DIR/CA/private/key.pem -in $SSL_DIR/CA/csr.pem -req -out $SSL_DIR/CA/crt.pem");

} elsif ($opt_server_cert) {
    my $hostdir = "$SSL_DIR/me";
    mkdir $hostdir;
    mkdir "$hostdir/private";
    $opt_common = $opt_server_cert if !defined $opt_common;
    my $openssl_cnf_file = "$hostdir/openssl.cnf";
    load_data_config();
    write_openssl_cnf($openssl_cnf_file);
    system("openssl genrsa -out $hostdir/private/key.pem 1024");
    system("openssl req -config $openssl_cnf_file $batch -new -key $hostdir/private/key.pem -out $hostdir/csr.pem");
    system("openssl x509 -days 3650 -CA $ca_cert_file -CAkey $ca_key_file -set_serial 01 -in $hostdir/csr.pem -req -out $hostdir/crt.pem");
    system("openssl x509 -in $hostdir/crt.pem -fingerprint -noout > $hostdir/fingerprint");

    my $server_hostdir = "$SSL_DIR/remote/$opt_server_cert";
    mkdir "$SSL_DIR/remote";
    symlink "../me", $server_hostdir;

} elsif ($opt_client_cert) {
    if (-l "$SSL_DIR/remote/$opt_client_cert") {
	die("$opt_client_cert is a server");
    }

    load_data_config();
    my $hostdir = "$SSL_DIR/remote/$opt_client_cert";
    mkdir "$SSL_DIR/remote";
    mkdir $hostdir;

    select( ( select(STDOUT), $| = 1 )[0] );

    print "\nWaiting for client to connect... ";

    # wait connection from server
    my $srv = IO::Socket::SSL->new(
	LocalAddr => "0.0.0.0:$opt_port",
	Listen => 10,
	SSL_cert_file => "$SSL_DIR/me/crt.pem",
	SSL_key_file => "$SSL_DIR/me/private/key.pem",
	SSL_verify_mode => 0,
	) or die "failed connect or ssl handshake: $!,$SSL_ERROR";

    my $fd = $srv->accept or
	 die "failed to accept or ssl handshake: $!,$SSL_ERROR";

    # Check peer is $opt_client_cert
    #

    print "  connected!\n";

    # send server fingerprint to client
    send_message_file($fd, $MSG_SERVER_FINGERPRINT, "$SSL_DIR/me/fingerprint");

    # get $hostdir/csr.pem from client
    get_message_file($fd, $MSG_CSR, "$hostdir/csr.pem");

    # check certificate common name is $opt_client_cert
    my $subject = `openssl req -noout -subject -in $hostdir/csr.pem`;
    $subject =~ s/\*//g; # Remove wildcard astricks
    my ($CN) = $subject =~ /CN=([^\/]*)/;
    if(!$CN) {
	die "Unable to read SSL certificate\n";
    }
    elsif($opt_client_cert !~ /$CN/i) {
	die "SSL Certificate Common Name mismatch. Retrieved common name '$CN' does not match hostname '$opt_client_cert'";
    }

    # sign csr
    system("openssl x509 -days 3650 -CA $ca_cert_file -CAkey $ca_key_file -set_serial 01 -in $hostdir/csr.pem -req -out $hostdir/crt.pem");

    # send $hostdir/crt.pem to client
    send_message_file($fd, $MSG_CRT, "$hostdir/crt.pem");

    # send CA CERT to client
    send_message_file($fd, $MSG_CA_CRT, "$ca_cert_file");

    system("openssl x509 -in $hostdir/crt.pem -fingerprint -noout > $hostdir/fingerprint");

    unlink "$hostdir/csr.pem";
    unlink "$hostdir/crt.pem";

} else {
    usage('no command specified');
}

exit;

sub send_message_file {
    my $fd = shift;
    my $msg = shift;
    my $filename = shift;

    my $data;
    {
	local $/ = undef;
	my $DATA;
	open $DATA, "<$filename" || die("$filename: $!");
	$data = <$DATA>;
	close $DATA;
    }
    my $size = length($data);

    print $fd pack("NN", $size, $msg), $data;
}

sub get_message_file {
    my $fd = shift;
    my $msg = shift;
    my $filename = shift;

    my $desc;
    sysread $fd, $desc, 8;
    my ($size, $gmsg) = unpack("NN", $desc);
    my $data;
    sysread $fd, $data, $size;
    die("MSG doesn't match: $msg != $gmsg") if $msg != $gmsg;
    my $DATA;
    open $DATA, ">$filename" || die("$filename: $!");
    print $DATA $data;
    close $DATA;
}

sub load_data_config {
    my $contents;
    {
        local $/ = undef;
        my $SSL_DATA;
        open $SSL_DATA, "<$SSL_DIR/openssl.data" || die("$SSL_DIR/openssl.data: $!");
        $contents = <$SSL_DATA>;
        close $SSL_DATA;
    }

    eval $contents;
    if ($@) {
	die;
    }
    if (!defined $ssl_config or ref($ssl_config) ne 'HASH') {
	die;
    }


    $ssl_config->{'config'}->{'COUNTRY_NAME_VALUE'}           = $opt_country           if defined $opt_country;
    $ssl_config->{'config'}->{'STATE_VALUE'}                  = $opt_state             if defined $opt_state;
    $ssl_config->{'config'}->{'LOCALITY_NAME_VALUE'}          = $opt_locality          if defined $opt_locality;
    $ssl_config->{'config'}->{'ORGANIZATION_NAME_VALUE'}      = $opt_organisation      if defined $opt_organisation;
    $ssl_config->{'config'}->{'ORGANIZATION_UNIT_NAME_VALUE'} = $opt_organisation_unit if defined $opt_organisation_unit;
    $ssl_config->{'config'}->{'COMMON_NAME_VALUE'}            = $opt_common            if defined $opt_common;
    $ssl_config->{'config'}->{'EMAIL_ADDRESS_VALUE'}          = $opt_email             if defined $opt_email;
    $ssl_config->{'CA_CERT_FILE'}                             = $opt_cacert            if defined $opt_cacert;
    $ssl_config->{'CA_KEY_FILE'}                              = $opt_cakey             if defined $opt_cakey;

    $ca_cert_file = $ssl_config->{'CA_CERT_FILE'} || "$SSL_DIR/CA/crt.pem";
    $ca_key_file =  $ssl_config->{'CA_KEY_FILE'}  || "$SSL_DIR/CA/private/key.pem";
}

sub write_openssl_cnf {
    my $openssl_cnf_file = shift;
    my $CNF_TEMPLATE;
    my $template;
    {
        local $/ = undef;
        open $CNF_TEMPLATE, "<$SSL_DIR/openssl.cnf.template";
        $template = <$CNF_TEMPLATE>;
        close $CNF_TEMPLATE;
    }

    if (!$ssl_config->{'config'}->{'COUNTRY_NAME_VALUE'}) {
	$ssl_config->{'config'}->{'COUNTRY_NAME_VALUE'} = "XX";
	$batch = "";
    }
    if (!$ssl_config->{'config'}->{'STATE_VALUE'}) {
	$ssl_config->{'config'}->{'STATE_VALUE'} = "Default State/Province";
	$batch = "";
    }
    if (!$ssl_config->{'config'}->{'LOCALITY_NAME_VALUE'}) {
	$ssl_config->{'config'}->{'LOCALITY_NAME_VALUE'} = "Default City";
	$batch = "";
    }
    if (!$ssl_config->{'config'}->{'ORGANIZATION_NAME_VALUE'}) {
	$ssl_config->{'config'}->{'ORGANIZATION_NAME_VALUE'} = "Default Company Ltd";
	$batch = "";
    }
    if (!$ssl_config->{'config'}->{'ORGANIZATION_UNIT_NAME_VALUE'}) {
	$ssl_config->{'config'}->{'ORGANIZATION_UNIT_NAME_VALUE'} = "Organizational Unit Name (eg, section)";
	$batch = "";
    }
    if (!$ssl_config->{'config'}->{'COMMON_NAME_VALUE'}) {
	$batch = "";
    }
    if (!$ssl_config->{'config'}->{'EMAIL_ADDRESS_VALUE'}) {
	$batch = "";
    }
    my $key;
    my $value;
    while (($key,$value) = each %{$ssl_config->{'config'}}) {
	$template =~ s/$key/$value/;
    }

    my $CNF;
    open $CNF, ">$openssl_cnf_file";
    print $CNF $template;
    close $CNF;
}

sub write_openssl_cnf_template {
    return if -f "$SSL_DIR/openssl.cnf.template";

    my $CNF_TEMPLATE;
    open $CNF_TEMPLATE, ">$SSL_DIR/openssl.cnf.template";
print $CNF_TEMPLATE <<'CNF_TEMPLATE_FILE';
#
# OpenSSL configuration file for amanda.
# This is used for generation of certificate requests.
#

# This definition stops the following lines choking if HOME isn't
# defined.
HOME			= .
RANDFILE		= $ENV::HOME/.rnd

[ req ]
default_bits		= 2048
default_md		= sha256
default_keyfile 	= privkey.pem
distinguished_name	= req_distinguished_name
attributes		= req_attributes
x509_extensions	= v3_ca	# The extentions to add to the self signed cert

# Passwords for private keys if not present they will be prompted for
# input_password = secret
# output_password = secret

# This sets a mask for permitted string types. There are several options. 
# default: PrintableString, T61String, BMPString.
# pkix	 : PrintableString, BMPString (PKIX recommendation before 2004)
# utf8only: only UTF8Strings (PKIX recommendation after 2004).
# nombstr : PrintableString, T61String (no BMPStrings or UTF8Strings).
# MASK:XXXX a literal mask value.
# WARNING: ancient versions of Netscape crash on BMPStrings or UTF8Strings.
string_mask = utf8only

# req_extensions = v3_req # The extensions to add to a certificate request

[ req_distinguished_name ]
countryName			= Country Name (2 letter code)
countryName_default		= COUNTRY_NAME_VALUE
countryName_min			= 2
countryName_max			= 2

stateOrProvinceName		= State or Province Name (full name)
stateOrProvinceName_default	= STATE_VALUE

localityName			= Locality Name (eg, city)
localityName_default		= LOCALITY_NAME_VALUE

0.organizationName		= Organization Name (eg, company)
0.organizationName_default	= ORGANIZATION_NAME_VALUE

# we can do this but it is not needed normally :-)
#1.organizationName		= Second Organization Name (eg, company)
#1.organizationName_default	= World Wide Web Pty Ltd

organizationalUnitName		= Organizational Unit Name (eg, section)
organizationalUnitName_default	= ORGANIZATION_UNIT_NAME_VALUE

commonName			= Common Name (eg, your name or your server\'s hostname)
commonName_default		= COMMON_NAME_VALUE
commonName_max			= 64

emailAddress			= Email Address
emailAddress_default		= EMAIL_ADDRESS_VALUE
emailAddress_max		= 64

# SET-ex3			= SET extension number 3

[ req_attributes ]
#challengePassword		= A challenge password
#challengePassword_min		= 4
#challengePassword_max		= 20
#
#unstructuredName		= An optional company name

[ v3_ca ]


# Extensions for a typical CA


# PKIX recommendation.

subjectKeyIdentifier=hash

authorityKeyIdentifier=keyid:always,issuer

# This is what PKIX recommends but some broken software chokes on critical
# extensions.
#basicConstraints = critical,CA:true
# So we do this instead.
basicConstraints = CA:true

# Key usage: this is typical for a CA certificate. However since it will
# prevent it being used as an test self-signed certificate it is best
# left out by default.
# keyUsage = cRLSign, keyCertSign

# Some might want this also
# nsCertType = sslCA, emailCA

# Include email address in subject alt name: another PKIX recommendation
# subjectAltName=email:copy
# Copy issuer details
# issuerAltName=issuer:copy

# DER hex encoding of an extension: beware experts only!
# obj=DER:02:03
# Where 'obj' is a standard or added object
# You can even override a supported extension:
# basicConstraints= critical, DER:30:03:01:01:FF

CNF_TEMPLATE_FILE

    close $CNF_TEMPLATE;
}

