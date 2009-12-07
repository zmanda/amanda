#! @PERL@
# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

####
# this changer has two "slots", and swaps these files into the ndmjob tape
# simulator's tapefile.  It's really not intended to support anything more
# complicated than the directtcp spanning tests in the taper installcheck.

use lib '@amperldir@';
use Amanda::Config qw( :init :getconf );
use Amanda::Util qw( :constants );

sub slurp {
    my ($filename) = @_;

    open(my $fh, "<", $filename) or return undef;
    my $result = do { local $/; <$fh> };
    close($fh);

    return $result;
}

Amanda::Util::setup_application("chg-ndmjob", "server", $CONTEXT_SCRIPTUTIL);

config_init($CONFIG_INIT_USE_CWD, undef);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# ok, let's get the parameters we need
my $changerfile = getconf($CNF_CHANGERFILE);

my $tapefile = $changerfile;
my $curfile = "$changerfile-cur";
my $portfile = "$changerfile-port";

# read the ndmp port
my $ndmp_port = slurp($portfile);

# get the current slot
my $cur_slot = slurp($curfile);
$cur_slot = 0 unless defined $cur_slot;
$cur_slot = $cur_slot + 0;

sub load {
    my ($slot) = @_;

    # "unload"
    rename("$tapefile", "$tapefile-slot$cur_slot")
	or die("could not rename $tapefile to $tapefile-slot$cur_slot: $!");

    # touch the new file to be sure it exists
    if (!-f "$tapefile-slot$slot") {
	open(my $fh, ">", "$tapefile-slot$slot");
	close($fh);
    }

    # and "load" it
    rename("$tapefile-slot$slot", "$tapefile" )
	or die("could not rename $tapefile-slot$slot to $tapefile: $!");

    # set the new "cur" pointer
    open(my $fh, ">", "$curfile");
    print $fh "$slot\n";
    close($fh);

    # and output the name of the NDMP device
    print "$slot ndmp:127.0.0.1:$ndmp_port\@$tapefile\n"
}

# and see what we're being asked to do
if ($ARGV[0] eq "-slot") {
    load($ARGV[1]);
} elsif ($ARGV[0] eq "-current") {
    load($cur_slot);
} elsif ($ARGV[0] eq "-next") {
    load($cur_slot+1);
} elsif ($ARGV[0] eq "-info") {
    print "$cur_slot 2 0 0\n";
}
