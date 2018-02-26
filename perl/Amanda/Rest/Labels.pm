# Copyright (c) 2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, or: http://www.zmanda.com

package Amanda::Rest::Labels;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Rest::Configs;
use Amanda::DB::Catalog2;
use Amanda::Util qw( match_datestamp );
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Labels -- Rest interface to get a list of labels

=head1 INTERFACE

=over

=item Get a list of all labels.

 request:
  GET /amanda/v1.0/configs/:CONF/labels
  you can filter the labels listed with the following query arguments:
            config=CONF
            datestamp=datestamp=datastamp_range
            storage=STORAGE
            meta=META
            pool=POOL
            reuse=0|1

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "2600001",
        "message" : "List of labels",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Labels.pm",
        "source_line" : "86",
        "tles" : [
           {
              "barcode" : null,
              "blocksize" : "32",
              "comment" : null,
              "config" : "test",
              "datestamp" : "20140121184644",
              "label" : "test-ORG-AC-vtapes2-005",
              "meta" : "test-ORG-AC",
              "pool" : "my_vtapes2",
              "position" : 1,
              "reuse" : "1",
              "storage" : "my_vtapes2"
           },
           ...
        ]
     }
  ]

=back

=cut

#
#internal function
#

sub init {
    my %params = @_;

    my $catalog = Amanda::DB::Catalog2->new();
    return (-1, $catalog);
}

sub list {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Labels");
    my ($status, @result_messages) = Amanda::Rest::Configs::config_init(@_);
    return (status, \@result_messages) if @result_messages;

    my ($status, $catalog) = Amanda::Rest::Labels::init();
    if ($catalog->isa("Amanda::Message")) {
	push @result_messages, $catalog;
	return ($status, \@result_messages);
    }
    my $volumes = $catalog->find_volumes(
			config => $params{'config'},
			storage => $params{'storage'},
			pool => $params{'pool'},
			meta => $params{'meta'},
			reuse => $params{'reuse'},
			order_write_timestamp => $params{'order_write_timestamp'},
			no_bless => 1,
			retention_name => 1);
    if (defined $params{'datestamp'}) {
	my @volumes = grep {defined $_->{'write_timestamp'} and match_datestamp($params{'datestamp'}, $_->{'write_timestamp'})} @$volumes;

	push @result_messages, Amanda::DB::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 2600001,
				severity => $Amanda::Message::SUCCESS,
				volumes => \@volumes);
    } else {
	push @result_messages, Amanda::DB::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 2600001,
				severity => $Amanda::Message::SUCCESS,
				volumes => $volumes);
    }
    return (-1, \@result_messages);
}

1;
