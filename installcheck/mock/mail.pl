#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use strict;
use warnings;
use Data::Dumper;

my $outfile = $ENV{'INSTALLCHECK_MOCK_MAIL_OUTPUT'};
die "INSTALLCHECK_MOCK_MAIL_OUTPUT not defined" unless defined $outfile;

# just copy data
open(my $out, ">", $outfile) or die("Could not open '$outfile'");

my $dumper = Data::Dumper->new([\@ARGV], [qw(ARGS)]);
$dumper->Indent(0);
print $out $dumper->Dump, "\n";

while (1) {
    my $data = <STDIN>;
    last unless $data;
    print $out $data;
}
close($out);
