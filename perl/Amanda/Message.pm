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


use Data::Dumper;

require Amanda::Debug;

use overload
    '""'  => sub { $_[0]->message(); },
    'cmp' => sub { $_[0]->message() cmp $_[1]; };


=head1 NAME

Amanda::Message - Amanda object use to return a message

Most API use or should be converted to use it.

=head1 SYNOPSIS

   # create a message
   my $msg = Amanda::Message->new(source_filename => __FILE__,
				  source_line => __LINE__,
				  severity    => $AM_CRITICAL;
				  code        => 1,
				  message     => "This is a message",
				  label       => $label);

   print $msg->message();

=head1 Message Objects

'source_filename' and 'source_line' are use for debuging to find where the
message was generated.

The 'severity' of the message, the default is G_CRITICAL, it must be one of
these predefined constants:
  AM_ERROR
  AM_CRITICAL
  AM_WARNING
  AM_MESSAGE
  AM_INFO
  AM_DEBUG

The 'code' must be unique, it identify the message (0 to 3 are used for message
not handled by Amanda::Message):
       0  GOOD message
       1  ERROR with a message
       2  ERROR without a message
       3  Amanda::Changer::Error   #You should never create it
 1000000  Amanda::Label message
 1100000  Amanda::Changer::Message
 1200000  Amanda::Recovery::Message
 1300000  Amanda::Curinfo::Message
 1400000  Amanda::Disklist::Message
 1500000  Amanda::Config::Message
 1600000  Amanda::Tapelist::Message
 1700000  Amanda::Device::Message
 1800000  Amanda::Status::Message
 1900000  Amanda::Report::Message
 2000000  Amanda::Amdump::Message

general keys:
  code            =>
  source_filename =>
  source_line     =>
  message         => 'default message'  #optional

each code can have it's own set of keys:
  filename        =>
  errno           =>
  label           =>
  config          =>
  barcode         =>
  storage         =>
  pool            =>
  meta            =>
  dev_error       =>

'message' is required only for code 0 and 1.

You must add all required fields to be able to rebuild the message string,
this can include the label, config, barcode, errno, errorstr or any other
fields.

=head1 Using as subclass

Each Amanda perl module should have an Amanda::Message subclass to describe
all messages from the module.

eg. class C<Amanda::Label::Message> is used by class C<Amanda::Label>.

The subclass (C<Amanda::Label::Message>) must overload the local_message
method to return a string version of the message.

=cut

$ERROR    = 32;
$CRITICAL = 16;
$WARNING  =  8;
$MESSAGE  =  4;
$INFO     =  2;
$DEBUG    =  1;

use strict;
use warnings;

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
    $self->{'severity'} = $Amanda::Message::CRITICAL if !defined $self->{'severity'};

    Amanda::Debug::debug("$params{'source_filename'}:$params{'source_line'}:$self->{'severity'}:$self->{'code'} $self->{'message'}");

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
