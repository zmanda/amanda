# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, or: http://www.zmanda.com

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

Amanda::Interactivity::tty_email -- Interactivity class combining tty and email classes

=head1 SYNOPSIS

Amanda::Interactivity class combining tty (when it is available) and email (otherwise).

=cut

sub new {
    my $class = shift;
    my $storage_name = shift;
    my $changer_name = shift;
    my $properties = shift;

    my $input;

    my $r = open($input, '>', "/dev/tty");
    if ($r) {
	close($input);
	return Amanda::Interactivity::tty->new($storage_name, $changer_name, $properties);
    } else {
	return Amanda::Interactivity::email->new($storage_name, $changer_name, $properties);
    }
}

1;
