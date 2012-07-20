#! @PERL@
# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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

use Getopt::Long;
use Symbol;
use IPC::Open3;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Paths;
use Amanda::Util qw ( match_disk );
use Amanda::Debug qw( debug );

Amanda::Util::setup_application("amdump_client", "client", $CONTEXT_CMDLINE);

my $config;
my $config_overrides = new_config_overrides($#ARGV+1);

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'config=s' => sub { $config = $_[1]; },
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

if (@ARGV < 1) {
    die "USAGE: amdump_client [--config <config>] <config-overwrites> [list|dump|check] <diskname>";
}

my $cmd = $ARGV[0];

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_CLIENT, undef);
$config = getconf($CNF_CONF) if !defined $config;
print "config: $config\n";
config_init($CONFIG_INIT_CLIENT | $CONFIG_INIT_EXPLICIT_NAME | $CONFIG_INIT_OVERLAY, $config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

#Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $amservice = $sbindir . '/amservice';
my $amdump_server = getconf($CNF_AMDUMP_SERVER);
my $auth = getconf($CNF_AUTH);

my @cmd = ($amservice, '-f', '/dev/null', '-s', $amdump_server, $auth, 'amdumpd');

debug("cmd: @cmd");
my $amservice_out;
my $amservice_in;
my $err = Symbol::gensym;
my $pid = open3($amservice_in, $amservice_out, $err, @cmd);
my @disks;

debug("send: CONFIG $config");
print {$amservice_in} "CONFIG $config\n";
if ($cmd eq 'list') {
    debug("send: LIST");
    print {$amservice_in} "LIST\n";
    get_list(1);
} elsif ($cmd eq 'dump') {
    # check if diskname on the command line
    if ($ARGV[1]) {
	# get the list of dle
	debug ("send: LIST");
	print {$amservice_in} "LIST\n";
	get_list(0);

        #find the diskname that match
	for (my $i=1; $i <= $#ARGV; $i++) {
	    for my $diskname (@disks) {
		if (match_disk($ARGV[$i], $diskname)) {
		    debug("send: DISK " . Amanda::Util::quote_string($diskname));
		    print {$amservice_in} "DISK " . Amanda::Util::quote_string($diskname) . "\n";
		    my $a = <$amservice_out>;
		    print if ($a !~ /^DISK /)
		}
	    }
	}
    }
    debug("send: DUMP");
    print {$amservice_in} "DUMP\n";
    get_server_data();
} elsif ($cmd eq 'check') {
    # check if diskname on the command line
    if ($ARGV[1]) {
	# get the list of dle
	debug ("send: LIST");
	print {$amservice_in} "LIST\n";
	get_list(0);

        #find the diskname that match
	for (my $i=1; $i <= $#ARGV; $i++) {
	    for my $diskname (@disks) {
		if (match_disk($ARGV[$i], $diskname)) {
		    debug("send: DISK " . Amanda::Util::quote_string($diskname));
		    print {$amservice_in} "DISK " . Amanda::Util::quote_string($diskname) . "\n";
		    my $a = <$amservice_out>;
		    print if ($a != /^DISK /)
		}
	    }
	}
    }
    debug("send: CHECK");
    print {$amservice_in} "CHECK\n";
    get_server_data();
} else {
    usage();
}
debug("send: END");
print {$amservice_in} "END\n";

sub get_list {
    my $verbose = shift;

    while (<$amservice_out>) {
	return if /^CONFIG/;
	return if /^ENDLIST/;
	print if $verbose;
	chomp;
	push @disks, Amanda::Util::unquote_string($_);
    }
}

sub get_server_data {
    while (<$amservice_out>) {
	if (/^ENDDUMP/) {
	    print "The backup is finished\n";
	    return;
	}
	if (/^ENDCHECK/) {
	    print "The check is finished\n";
	    return;
	}
	print;
	return if /^CONFIG/;
	return if /^BUSY/;
    }
}

sub usage {
    print STDERR "USAGE: amdump_client [--config <config>] <config-overwrites> [list|dump|check] <diskname>";
}

Amanda::Util::finish_application();
exit;
