# Copyright (c) 2014-2014 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com


package Amanda::Service::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 3100000) {
        return "No host argument specified";
    } elsif ($self->{'code'} == 3100001) {
        return "No auth argument specified";
    } elsif ($self->{'code'} == 3100002) {
        return "amservice failed: $self->{'errmsg'}";
    } elsif ($self->{'code'} == 3100003) {
        return "amservice failed: $self->{'errmsg'}: $self->{'buffer'}";
    } elsif ($self->{'code'} == 3100004) {
        return "No application argument specified";
    } elsif ($self->{'code'} == 3100005) {
        return "senddiscover result";
    } else {
	return "no message for code $self->{'code'}";
    }
}

