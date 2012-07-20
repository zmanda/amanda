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
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Interactivity::tty_email;

use strict;
use warnings;
use POSIX qw( :errno_h );
use vars qw( @ISA );
@ISA = qw( Amanda::Interactivity );

use Amanda::Paths;
use Amanda::Util;
use Amanda::Debug qw( debug );
use Amanda::Changer;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Interactivity::tty;
use Amanda::Interactivity::email;

=head1 NAME

Amanda::Interactivity::tty_email -- Interactivity class to read user request from /dev/tty

=head1 SYNOPSIS

Amanda::Interactivity class combining tty (when it is available) and email (otherwise).

=cut

sub new {
    my $class = shift;
    my $property = shift;

    my $input;

    my $r = open($input, '>', "/dev/tty");
    if ($r) {
	close($input);
	return Amanda::Interactivity::tty->new($property);
    } else {
	return Amanda::Interactivity::email->new($property);
    }
}

1;
