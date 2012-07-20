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



# Run perl.
eval '(exit $?0)' && eval 'exec /usr/bin/perl -S $0 ${1+"$@"}'
         & eval 'exec /usr/bin/perl -S $0 $argv:q'
                if 0;

use Time::Local;

my $AMANDA='@CLIENT_LOGIN@';

my $ddebug = 1;   # set to 1 to print signal debug to stderr

my $sigint_seen   = 0;
my $sigpipe_seen  = 0;
my $sighup_seen   = 0;
my $sigill_seen   = 0;
my $sigterm_seen  = 0;
my $sigsegv_seen  = 0;
my $sigquit_seen  = 0;
my $sigfpe_seen   = 0;

$AMANDA_HOME = (getpwnam($AMANDA) )[7] || die "Cannot find $AMANDA home directory\n" ;
$AM_PASS = "$AMANDA_HOME/.am_passphrase";

unless ( -e $AM_PASS ) {
  die "secret key $AM_PASS not found\n";
}


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
    system "$gpg_agent_cmd $gpg --batch --no-secmem-warning --disable-mdc --symmetric --cipher-algo AES256 --passphrase-fd 3  3<$AM_PASS";
    if ($? == -1) {
	print STDERR "failed to execute gpg: $!\n";
	exit (1);
    } elsif ($? & 127) {
	printf STDERR "gpg died with signal %d\n", ($? & 127);
	exit ($?);
    } elsif ($? >> 8) {
	printf STDERR "gpg exited with value %d\n", ($? >> 8);
	exit ($? >> 8);
    }
}

sub decrypt() {
    my $gpg_agent_cmd = do_gpg_agent();
    my $gpg = which_gpg();
    system "$gpg_agent_cmd $gpg --batch --quiet --no-mdc-warning --decrypt --passphrase-fd 3  3<$AM_PASS";
    if ($? == -1) {
	print STDERR "failed to execute gpg: $!\n";
	exit (1);
    } elsif ($? & 127) {
	printf STDERR "gpg died with signal %d\n", ($? & 127);
	exit ($?);
    } elsif ($? >> 8) {
	printf STDERR "gpg exited with value %d\n", ($? >> 8);
	exit ($? >> 8);
    }
}

sub int_catcher {
    $sigint_seen = 1;
}

sub pipe_catcher {
    $sigpipe_seen = 1;
}

sub hup_catcher {
    $sighup_seen = 1;
}

sub ill_catcher {
    $sigill_seen = 1;
}

sub term_catcher {
    $sigterm_seen = 1;
}

sub segv_catcher {
    $sigsegv_seen = 1;
}

sub quit_catcher {
    $sigquit_seen = 1;
}

sub fpe_catcher {
    $sigfpe_seen = 1;
}

#main

$SIG{'INT'}   = 'int_catcher';
$SIG{'PIPE'}  = 'pipe_catcher';
$SIG{'HUP'}   = 'hup_catcher';
$SIG{'ILL'}   = 'ill_catcher';
$SIG{'TERM'}  = 'term_catcher';
$SIG{'SEGV'}  = 'segv_catcher';
$SIG{'QUIT'}  = 'quit_catcher';
$SIG{'FPE'}   = 'FPE_catcher';


if ( $#ARGV > 0 ) {
     die "Usage: $0 [-d]\n";
}

if ( $#ARGV==0 && $ARGV[0] eq "-d" ) {
    decrypt();
}
else {
    encrypt();
}

if ( $ddebug  ) {
    if ( $sigint_seen )  { print STDERR "strange sigint seen = $sigint_seen\n"; }
    if ( $sigpipe_seen ) { print STDERR "strange sigpipe seen = $sigpipe_seen\n"; }
    if ( $sighup_seen )  { print STDERR "strange sighup seen = $sighup_seen\n"; }
    if ( $sigill_seen )  { print STDERR "strange sigill seen = $sigill_seen\n"; }
    
    if ( $sigterm_seen ) { print STDERR "strange sigterm seen = $sigterm_seen\n"; }
    if ( $sigsegv_seen ) { print STDERR "strange sigsegv seen = $sigsegv_seen\n"; }
    if ( $sigquit_seen ) { print STDERR "strange sigquit seen = $sigquit_seen\n"; }
    if ( $sigfpe_seen )  { print STDERR "strange sigfpe seen = $sigfpe_seen\n"; }
    
}

$SIG{'INT'}  = 'DEFAULT';
$SIG{'PIPE'} = 'DEFAULT';
$SIG{'HUP'}  = 'DEFAULT';
$SIG{'ILL'}  = 'DEFAULT';
$SIG{'TERM'} = 'DEFAULT';
$SIG{'SEGV'} = 'DEFAULT';
$SIG{'QUIT'} = 'DEFAULT';
$SIG{'FPE'}  = 'DEFAULT';

