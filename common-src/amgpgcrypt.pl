#!@PERL@ -w
#
# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
#

# Amanda has problem with gpg mdc(modification detection code) in the binary mode.
# This program encrypt with mdc disabled.
# If mdc is required, use --armor option. 



# Run perl.
eval '(exit $?0)' && eval 'exec /usr/bin/perl -S $0 ${1+"$@"}'
         & eval 'exec /usr/bin/perl -S $0 $argv:q'
                if 0;

use Time::Local;

my $AMANDA='@CLIENT_LOGIN@';
my $saw_sigint = 0;

$AMANDA_HOME = (getpwnam($AMANDA) )[7] || die "Cannot find $AMANDA home directory\n" ;

#The following two ($AM_PASS, $AM_PRIV) are needed only for restore/recover
#They should be protected and stored away during other time.
$AM_PASS = "$AMANDA_HOME/.am_passphrase";
$AM_PRIV = "$AMANDA_HOME/.gnupg/secring.gpg";

$ENV{'PATH'} = '/usr/local/bin:/usr/bin:/usr/sbin:/bin:/sbin:/opt/csw/bin';

$ENV{'GNUPGHOME'} = "$AMANDA_HOME/.gnupg";

sub do_gpg_agent() {
    my $path=`which gpg-agent 2>/dev/null`;
    chomp $path;
    if (-x $path) {
	return "gpg-agent --daemon --";
    }
    return ""
}

sub which_gpg() {
    my $path=`which gpg2 2>/dev/null`;
    if (!$path) {
	$path=`which gpg 2>/dev/null`;
    }
    if (!$path) {
	die("no gpg or gpg2");
    }
    chomp $path;
    return $path;
}

sub encrypt() {
    my $gpg_agent_cmd = do_gpg_agent();
    my $gpg = which_gpg();
    system "$gpg_agent_cmd $gpg  --batch --disable-mdc --encrypt --cipher-algo AES256 --recipient $AMANDA";
}

sub decrypt() {
    my $gpg_agent_cmd = do_gpg_agent();
    my $gpg = which_gpg();
    system "$gpg_agent_cmd $gpg --batch --quiet --no-mdc-warning --secret-keyring $AM_PRIV --decrypt --passphrase-fd 3  3<$AM_PASS";
}

sub my_sig_catcher {
	$saw_sigint = 1;
}

#main



$SIG{'INT'} = 'my_sig_catcher';


if ( $#ARGV > 0 ) {
     die "Usage: $0 [-d]\n";
}

if ( $#ARGV==0 && $ARGV[0] eq "-d" ) {
    decrypt();
}
else {
    encrypt();
}

$SIG{'INT'} = 'DEFAULT';
