# Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Rest::Status;
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
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Status -- Rest interface to Amanda::Status

=head1 INTERFACE

=over

=item Get the status

request:
  GET /amanda/v1.0/configs/:CONF/status?amdump_log=/path/to/amdump_log_file

reply:
  HTTP status 200 OK
    [
     {
        "code" : "1800000",
        "message" : "The status",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Status.pm",
        "source_line" : "1562",
        "status" : {
	    ...
        }
     }
    ]

=back

=cut

sub current {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    $params{'filename'} = $params{'amdump_log'} if defined $params{'amdump_log'};
    my $status = Amanda::Status->new(%params);
    push @result_messages, $status->current();

    return \@result_messages;
}

1;
