# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use utf8;

package Amanda::Message;


use Data::Dumper;

require Amanda::Debug;
use JSON;

use overload
    '""'  => sub { $_[0]->full_message(); },
    'cmp' => sub { $_[0]->message() cmp $_[1]; };


=head1 NAME

Amanda::Message - Amanda object use to return a message

Most API use or should be converted to use it.

=head1 SYNOPSIS

   # create a message
   my $msg = Amanda::Message->new(source_filename => __FILE__,
				  source_line => __LINE__,
				  severity    => $CRITICAL;
				  code        => 1,
				  message     => "This is a message",
				  label       => $label);

   print $msg->message();

=head1 Message Objects

'source_filename' and 'source_line' are use for debuging to find where the
message was generated.

The 'severity' of the message, the default is CRITICAL, it must be one of
these predefined constants:
  CRITICAL
  ERROR
  WARNING
  MESSAGE
  INFO
  SUCCESS

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
 2100000  Amanda::Cmdfile::Message
 2200000  Amanda::Amflush::Message
 2400000  Amanda::Index::Message
 2500000  Amanda::Amvault::Message
 2600000  Amanda::DB::Message
 2700000  Amanda::CheckDump::Message
 2800000  amcheck
 2900000  client service - senddiscover, restore, ...
 3000000  Amanda::Amvmware::Message
 3100000  Amanda::Service::Message
 3101000    Amanda::Service::Restore::Message
 3200000  Amanda::Appliance
 3300000  Amanda::FetchDump::Message
 3400000  Amanda::Cleanup::Message
 3500000  Amanda::Process::Message
 3600000  selfcheck
 3700000  applications
  3700000  amgtar
  3701000  amstar
  3702000  ambsdtar
 3800000  Amanda::Extensions::Message
  3801000  Amanda::Extensions::Rest::Application::Amvmware
 3900000  planner
 4000000  sendsize
 4100000  dumper
 4200000  sendbackup
 4300000  Amanda::Chunker::Message
 4400000  Amanda::Taper::Message
 4500000  driver
 4600000  client-util
 4700000  scripts
  4700000  script-email
  4701000  amlog-script
  4702000  amzfs-snapshot
 4800000  Amanda::Extract::Message
 4900000  Amanda::Restore::Message
 5000000  ambackupd
 5100000  Amanda::ScanInventory::Message
 5200000  Amanda::Amcheck_Device::Message;

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

$CRITICAL = "critical";
$ERROR    = "error";
$WARNING  = "warning";
$MESSAGE  = "message";
$INFO     = "info";
$SUCCESS  = "success";

use strict;
use warnings;

use Encode::Locale;
use Encode;
use Scalar::Util qw(blessed refaddr readonly);

use Amanda::Debug;

sub new {
    my $class = shift @_;
    my %params = @_;

    if (!defined $params{'code'}) {
	Amanda::Debug::debug(Data::Dumper::Dumper(\%params));
    }
    die("no code") if !defined $params{'code'};
    die("no source_filename") if !defined $params{'source_filename'};
    die("no source_line") if !defined $params{'source_line'};

    my $self = \%params;
    bless $self, $class;

    $self->{'severity'} = $Amanda::Message::CRITICAL if !defined $self->{'severity'};
    $self->{'process'} = Amanda::Util::get_pname() if !defined $self->{'process'};
    $self->{'running_on'} = Amanda::Config::get_running_on() if !defined $self->{'running_on'};
    $self->{'component'} = Amanda::Util::get_pcomponent() if !defined $self->{'component'};
    $self->{'module'} = Amanda::Util::get_pmodule() if !defined $self->{'module'};
    if (defined $self->{'errno'}) {
	$self->{'errnostr'} = "$self->{'errno'}";
	$self->{'errno'} = $self->{'errno'}+0;
	#$self->{'errnocode'} = "EXXX";
    }
#    $self->{'message'} = "" if $self->{'code'} == 1 and !defined $self->{'message'};
#    $self->{'message'} = "" if $self->{'code'} == 2 and !defined $self->{'message'};
    $self->{'message'} = $self->message() if !defined $self->{'message'};

    Amanda::Debug::debug("$params{'source_filename'}:$params{'source_line'}:$self->{'severity'}:$self->{'code'} $self->{'message'}");

    foreach my $value (values %{$self}) {
	_apply(sub { if (!utf8::is_utf8($_[0]) and
			 !Scalar::Util::readonly($_[0])) {
				$_[0] = decode(locale => $_[0]);
		     }
		   },
	       {}, $value);
    }
    return $self;
}

sub new_from_json_text {
    my $class = shift;
    my $text = shift;

    my $json = JSON->new->allow_nonref;
    my $self = $json->decode($text);
    bless $self, $class;

    return $self;
}
#does not works for blessed object.
sub _apply {
    my $code = shift;
    my $seen = shift;

    my @retval;
    for my $arg (@_) {
        if(my $ref = ref $arg){
            my $refaddr = refaddr($arg);
            my $proto;

            if (defined($proto = $seen->{$refaddr})) {
		#noop
	    } elsif ($ref eq 'ARRAY') {
                $proto = $seen->{$refaddr} = [];
                @{$proto} = _apply($code, $seen, @{$arg});
            } elsif ($ref eq 'HASH') {
                $proto = $seen->{$refaddr} = {};
                %{$proto} = _apply($code, $seen, %{$arg});
            } elsif ($ref eq 'REF' or $ref eq 'SCALAR') {
                $proto = $seen->{$refaddr} = \do{ my $scalar };
                ${$proto} = _apply($code, $seen, ${$arg});
            } else { # CODE, GLOB, IO, LVALUE etc.
                $proto = $seen->{$refaddr} = $arg;
            }

            push @retval, $proto;
        } else {
	    my $arg1 = defined($arg) ? $code->($arg) : $arg;
	    push @retval, $arg1;
            #push @retval, defined($arg) ? $code->($arg) : $arg;
        }
    }

    return wantarray ? @retval : $retval[0];
}


sub message {
    my $self = shift;

    return $self->{'message'} if defined $self->{'message'};

    my $message = $self->local_message();
    if (!utf8::is_utf8($message)) {
	decode(locale => $message);
    }
    return $message if $message;
}

sub full_message {
    my $self = shift;

    my $full_message = $self->local_full_message();
    if (!utf8::is_utf8($full_message)) {
        decode(locale => $full_message);
    }
    return $full_message;
}

# Should be overloaded
sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2850000) {
	return "Amcheck exit code is '$self->{'exit_code'}'";
    } elsif (defined $self->{'message'}) {
	return $self->{'message'};
    } else {
	return "no message for code $self->{'code'}";
    }
}

#Can be overloaded
sub local_full_message {
    my $self = shift;

    return $self->{'message'};
}

sub TO_JSON { return { %{ shift() } }; }

1;
