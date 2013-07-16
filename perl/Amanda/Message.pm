#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Message;

use strict;
use warnings;

use Data::Dumper;

require Amanda::Debug;

use overload
    '""'  => sub { $_[0]->message(); },
    'cmp' => sub { $_[0]->message() cmp $_[1]; };

sub new {
    my $class = shift @_;
    my %params = @_;

    die("no code") if !defined $params{'code'};
    die("no source_filename") if !defined $params{'source_filename'};
    die("no source_line") if !defined $params{'source_line'};

    my $self = \%params;
    bless $self, $class;

    $self->{'message'} = "" if $self->{'code'} == 1 and !defined $self->{'message'};
    $self->{'message'} = "" if $self->{'code'} == 2 and !defined $self->{'message'};
    $self->{'message'} = $self->message() if !defined $self->{'message'};

    Amanda::Debug::debug("$params{'source_filename'}:$params{'source_line'}: $self->{'message'}");

    return $self;
}

sub message {
    my $self = shift;

    return $self->{'message'} if defined $self->{'message'};

    my $message = $self->local_message();
    return $message if $message;

    return Data::Dumper::Dumper($self);
}

# Should be overloaded
sub local_message {
    return;
}

1;
