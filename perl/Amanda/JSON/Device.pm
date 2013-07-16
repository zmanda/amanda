# Copyright (c) 2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::JSON::Device;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Label;
use Amanda::Curinfo;
use Amanda::JSON::Config;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Device -- JSON interface to Amanda::Device

=head1 INTERFACE

=over

=item Amanda::JSON::Device::read_label

Interface to C<Amanda::Device::read_label>
Read the label of a device.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::read_label",
   "params" :{"config":"test",
	      "device":"$device"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Device.pm",
	      "source_line":"1421",
	      "code":1700000,
	      "device_name":"file:/amanda/h1/vtapes/slot1",
	      "dev_status":0,
	      "dev_error":"Success",
	      "message":"device '%self->{'device_name'}: status: Success."}],
   "id":"1"}

=item Amanda::JSON::Device::get_properties

Interface to C<Amanda::Device::get_properties>
Get properties from a device.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::get_properties",
   "params" :{"config":"test",
	      "properties":["leom","partial_deletion"],
	      "device":"$device"},
   "id"     :"2"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Device.pm",
	      "property_name":"leom",
	      "source_line":"158",
	      "code":1700001,
	      "property_value":"1",
	      "message":"property 'leom is '1'."},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Device.pm",
	      "property_name":"partial_deletion",
	      "source_line":"158",
	      "code":1700001,
	      "property_value":"1",
	      "message":"property 'partial_deletion is '1'."}],
   "id":"2"}

=back

=cut

sub do_device {
    my $func_name = shift;
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $storage;
    my $chg;
    my $err;
    my $res;
    my $device_name = $params{'device'};
    my $device;
    if ($device_name) {
	$device = Amanda::Device->new($device_name);
    } else {
	$storage = Amanda::Storage->new();
	return [$storage] if $storage->isa("amanda::Message");
	$chg = $storage->{'chg'};
	return [$chg] if $chg->isa("amanda::Message");
	$chg->load(relative_slot => "current",
	    res_cb => sub {
		(my $err, $res) = @_;
		if ($res) {
		    $device = $res->{'device'};
		    $device_name = $device->device_name();
		}
		Amanda::MainLoop::quit();
	    });
	Amanda::MainLoop::run();
    }

    return [$err] if $err;
    return [$device] if $device->isa("Amanda::Message");

    my $result = $device->status();
    if ($result == $DEVICE_STATUS_SUCCESS ||
	$result == $DEVICE_STATUS_VOLUME_UNLABELED) {
	$device->configure(1);
	my $sub = \&{$func_name};
	push @result_messages, $sub->($device, %params);
    } else {
	push @result_messages, $device->to_message;
    }

    if ($res) {
	$res->release(finished_cb => sub { Amanda::MainLoop::quit() });
	Amanda::MainLoop::run();
    }

    $chg->quit() if $chg;
    $storage->quit() if $storage;

    return \@result_messages;
}

sub _read_label {
    my $device = shift;
    my $result = $device->read_label();
    if ($device->status() != $DEVICE_STATUS_SUCCESS ) {
	$result = $device->status();
    }
    return $device->to_message;
}
sub read_label {
    do_device("_read_label", @_);
}

sub _get_properties {
    my $device = shift;
    my %params = @_;
    my @result_messages;

    my @plist = @{$params{'properties'}} if defined $params{'properties'};

    if (@plist == 0) {
	my @list = $device->property_list();
	foreach my $line (@list) {
	    push @plist, $line->{'name'} if $line->{'name'} ne "";
	}
    }

    foreach my $prop (sort @plist ) {
	my $value = $device->property_get(lc($prop) );
	push @result_messages, Amanda::Device::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 1700001,
			property_name   => lc($prop),
			property_value  => $value);
    }
    return @result_messages;
}

sub get_properties {
    do_device("_get_properties", @_);
}

1;
