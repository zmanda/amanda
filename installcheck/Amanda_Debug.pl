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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 9;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Amanda::Debug qw( :logging );
use Amanda::Config;

## most failures of the debug module will just kill the process, so
## the main goal of this test script is just to make it to the end :)

my $fh;
my $debug_text;
my $pid;
my $kid;

# load default config
Amanda::Config::config_init(0, undef);

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Amanda::Debug::dbrename("TESTCONF", "installcheck");
# note: we don't bother using Installcheck::log_test_output here because
# sometimes the log files aren't open

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $debug_fd = Amanda::Debug::dbfd();
ok($debug_fd, "dbfd() returns something nonzero");

my $debug_file = Amanda::Debug::dbfn();
ok(-f $debug_file, "dbfn() returns a filename that exists");

Amanda::Debug::debug('debug message');
Amanda::Debug::info('info message');
Amanda::Debug::message('message message');
Amanda::Debug::warning('warning message');

Amanda::Debug::dbclose();

open ($fh, "<", $debug_file);
$debug_text = do { local $/; <$fh> };
close($fh);

like($debug_text, qr/debug message/, "debug message is in debug log file");
like($debug_text, qr/info message/, "info message is in debug log file");
like($debug_text, qr/message message/, "message message is in debug log file");
like($debug_text, qr/warning message/, "warning message is in debug log file");

Amanda::Debug::dbreopen($debug_file, "oops, one more thing");
Amanda::Debug::dbclose();

open ($fh, "<", $debug_file);
$debug_text = do { local $/; <$fh> };
close($fh);

like($debug_text, qr/warning message/, "dbreopen doesn't erase existing contents");
like($debug_text, qr/oops, one more thing/, "dbreopen adds 'notation' to the debug log");

Amanda::Debug::dbreopen($debug_file, "I've still got more stuff to test");

# fork a child to call error()
$pid = open($kid, "-|");
die "Can't fork: $!" unless defined($pid);
if (!$pid) {
    add_amanda_log_handler($amanda_log_null); # don't spew to stderr, too, please
    Amanda::Debug::critical("morituri te salutamus");
    exit 1; # just in case
}
close $kid;
waitpid $pid, 0;

# just hope this works -- Perl makes it very difficult to write to fd 2!
Amanda::Debug::debug_dup_stderr_to_debug();
Amanda::Debug::dbclose();

open ($fh, "<", $debug_file);
$debug_text = do { local $/; <$fh> };
close($fh);

like($debug_text, qr/morituri te salutamus/, "critical() writes its message to the debug log");
