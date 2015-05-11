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

package Amanda::Rest::Dumps;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Rest::Configs;
use Amanda::Tapelist;
use Amanda::DB;
use Amanda::DB::Catalog;
use Symbol;
use Data::Dumper;
use URI::Escape;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Dumps -- Rest interface to Amanda::DB::Catalog

=head1 INTERFACE

=over

=item Get the list of restorable dumps

 request:
  GET /amanda/v1.0/configs/:CONF/dumps
  GET /amanda/v1.0/configs/:CONF/dumps/hosts/:HOST
    query arguments:
        host=HOST
        disk=DISK
        dump_timestamp=DUMP_TIMESTAMP
        write_timestamp=WRITE_TIMESTAMP
        level=LEVEL
	status=OK|PARTIAL|FAIL
	holding=0|1
	label=LABEL

 reply:
  [
     {
        "code" : "2600000",
        "dumps" : [
           {
              "bytes" : 81478422,
              "client_crc" : "09dbccef:81478422",
              "diskname" : "/bootAMGTAR",
              "dump_timestamp" : "20140203085941",
              "hostname" : "localhost.localdomain",
              "kb" : 79568.771484375,
              "level" : 0,
              "message" : "",
              "native_crc" : "ff45fab4:87556096",
              "nparts" : 1,
              "orig_kb" : 85504,
              "parts" : [
                 {},
                 {
                    "client_crc" : "09dbccef:81478422",
                    "filenum" : 1,
                    "kb" : 79568,
                    "label" : "test-ORG-AA-vtapes-011",
                    "native_crc" : "ff45fab4:87556096",
                    "orig_kb" : 85504,
                    "partnum" : 1,
                    "sec" : 0.253803,
                    "server_crc" : "09dbccef:81478422",
                    "status" : "OK"
                 }
              ],
              "sec" : 0.1,
              "server_crc" : "09dbccef:81478422",
              "status" : "OK",
              "storage" : "my_vtapes",
              "write_timestamp" : "20140203085941"
           }
        ],
        "message" : "The dumps",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Dumps.pm",
        "source_line" : "194"
     }
  ]

=back

=cut

#
#internal function
#

sub list {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Dumps");
    my ($status, @result_messages) = Amanda::Rest::Configs::config_init(@_);
    return ($status, \@result_messages) if @result_messages;

    if ($params{'HOST'}) {
	$params{'hostname'} = $params{'HOST'}
    }
    if ($params{'disk'}) {
	$params{'diskname'} = $params{'disk'};
    } elsif ($parms{'DISK'}) {
	$params{'diskname'} = uri_unescape($params{'DISK'});
    }

    my @dumps = Amanda::DB::Catalog::get_dumps(%params);

    # Remove cycle
    # uncomment commented line to remove undef parts.
    foreach my $dump (@dumps) {
	my $parts = $dump->{'parts'};
#	my @newparts;
	foreach my $part (@{$parts}) {
#	    next if !defined $part;
	    delete $part->{'dump'};
#	    push @newparts, $part;
	}
#	$dump->{'parts'} = \@newparts;
    }

    push @result_messages, Amanda::DB::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 2600000,
				severity => $Amanda::Message::SUCCESS,
				dumps           => \@dumps);
    return ($status, \@result_messages);
}

1;
