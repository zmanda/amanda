# Copyright (c) 2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::JSON::Changer;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );;
use Amanda::Device qw( :constants );
use Amanda::Changer;
use Amanda::Header;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::JSON::Config;
use Amanda::JSON::Tapelist;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);
use Digest::SHA1 qw( sha1_hex );

=head1 NAME

Amanda::JSON::Changer -- JSON interface to Amanda::Changer

=head1 INTERFACE

=over

=item Amanda::JSON::Changer::inventory

Interface to C<Amanda::Changer::inventory>
Return the inventory from the changer.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::inventory",
   "params" :{"config":"test",
              "changer_name":"changer"},
   "id:     :"1"}

changer_name is optional, the default changer is use if it is not specified.

result:

  {"jsonrpc":"2.0",
   "result":[{"device_status":"0",
              "label":"DIRO-TEST-001",
              "f_type":"1",
              "reserved":0,
              "state":1,
              "slot":"1"},
             {"device_status":"0",
              "label":"DIRO-TEST-002",
              "f_type":"1",
              "reserved":0,
              "state":1,
              "slot":"2"}],
   "id":"1"}

=item Amanda::JSON::Changer::load

Interface to C<Amanda::Changer::load>
Return the device status or label

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::cwloadinventory",
   "params" :{"config":"test",
              changer_name":"changer",
              "slot":"1"},
   "id:     :"2"}

changer_name is optional, the default changer is use if it is not specified.

result:
  {"jsonrpc":"2.0",
   "result":{"label":"DIRO-TEST-001",
	     "f_type":"1",
             "datestamp":"X"},
   "id":"2"}

  {"jsonrpc":"2.0",
   "result":{"device_status":"8",
             "device_error":"File 0 not found",
             "device_status_error":"Volume not labeled"},
   "id":"2"}

=back

=cut

sub inventory {
    my %params = @_;
    Amanda::JSON::Config::config_init(@_);

    my $changer_name = $params{'changer_name'};
    my $chg = Amanda::Changer->new($changer_name);
    die [4002, "$chg"] if $chg->isa("Amanda::Changer::Error");

    my $err;
    my $inventory;
    my $inventory_cb = sub {
	($err, $inventory) = @_;
	Amanda::MainLoop::quit();
    };
    my $main = sub {
	$chg->inventory(inventory_cb => $inventory_cb);
    };
    Amanda::MainLoop::call_later($main);
    Amanda::MainLoop::run();
    $main = undef;
    $inventory_cb = undef;

    die [4003, "$err"] if $err;

    return $inventory;
}

sub load {
    my %params = @_;
    Amanda::JSON::Config::config_init(@_);

    my $changer_name = $params{'changer_name'};
    my $chg = Amanda::Changer->new($changer_name);
    my $ret;
    my $err;
    die [4002, "$chg"] if $chg->isa("Amanda::Changer::Error");

    my $finished_cb = sub {
	Amanda::MainLoop::quit();
    };

    my $res_cb = sub {
	($err, my $res) = @_;
	return Amanda::MainLoop::quit() if $err;

	my $dev = $res->{'device'};
	return Amanda::MainLoop::quit() if !defined $dev;
	if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    $ret->{'device_status'} = $dev->status;
	    $ret->{'device_status_error'} = $dev->status_error;
	    $ret->{'device_error'} = $dev->error;
	} else {
	    my $volume_header = $dev->volume_header;
	    $ret->{'f_type'} = $volume_header->{'type'};
	    if ($ret->{'f_type'} == $Amanda::Header::F_TAPESTART) {
		$ret->{'datestamp'} = $volume_header->{'datestamp'};
		$ret->{'label'} = $volume_header->{'name'};
	    }
	}
	$res->release(finished_cb => $finished_cb);
    };

    my $main = sub {
	my %args;
	$args{'label'} = $params{'label'} if defined $params{'label'};
	$args{'slot'} = $params{'slot'} if defined $params{'slot'};
	$args{'relative_slot'} = $params{'relative_slot'} if defined $params{'relative_slot'};
	$args{'res_cb'} = $res_cb;
	$chg->load(%args);
    };

    Amanda::MainLoop::call_later($main);
    Amanda::MainLoop::run();
    $main = undef;
    $finished_cb = undef;
    $res_cb = undef;
    $chg->quit();

    die [4004, "$err"] if $err;

    return $ret;
}

1;
