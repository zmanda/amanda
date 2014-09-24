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

package Amanda::Rest::Version;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Constants;
use Symbol;
use Data::Dumper;
use Scalar::Util;
use vars qw(@ISA);
use Amanda::Debug;
use Amanda::Util qw( :constants );

=head1 NAME

Amanda::Rest::Version - Get the amande version.

=head1 INTERFACE

 request:
  GET /amanda/v1.0

 reply:
  HTTP status: 200 OK
  [
     {
        "BUILT_BRANCH" : "trunk",
        "BUILT_DATE" : "Mon Jan 30 9:37:11 EST 2014",
        "BUILT_REV" : "5613",
        "VERSION" : "4.0.0alpha",
        "code" : "1550000",
        "message" : "The version",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Version.pm",
        "source_line" : "44"
     }
  ]

=cut

sub version {
    my %params = @_;
    my @result_messages;

    push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1550000,
				VERSION  => $Amanda::Constants::VERSION,
				BUILT_DATE => $Amanda::Constants::BUILT_DATE,
				BUILT_REV => $Amanda::Constants::BUILT_REV,
				BUILT_BRANCH => $Amanda::Constants::BUILT_BRANCH);
    return \@result_messages;
}


1;
