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

package Amanda::JSON::Status;
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
use Amanda::Status;
use Amanda::JSON::Config;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Status -- JSON interface to Amanda::Status

=head1 INTERFACE

=over

=item Amanda::JSON::Status::current

Interface to C<Amanda::Status::current>
Get the current status.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Status::current",
   "params" :{"config":"test"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Status.pm",
	      "source_line":"1433",
	      "code":1800000,
	      "message":"The status",
	      "status":{...} }],
   "id":"1"}

=back

=cut

sub current {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $status = Amanda::Status->new(%params);
    push @result_messages, $status->current();

    return \@result_messages;
}

#sub stream {
#
#    return sub {
#	my $responder = shift;
#	my $writer = $responder->(
#	    [ 200, [ 'Content-Type', 'application/json' ]]);
#
#	for my $i ("AA","BB","CC") {
#	    my $m = Amanda::Message->new(
#			source_filename => __FILE__,
#			source_line     => __LINE__,
#			code => 0,
#			message => $i);
#	    my $a = JSON->new->convert_blessed->utf8->encode($m);
#	    $writer->write("$a\n");
#	    sleep 5;
#	}
#
#	$writer->close;
#    }
#}

1;
