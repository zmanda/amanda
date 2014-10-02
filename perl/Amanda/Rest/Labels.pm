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

package Amanda::Rest::Labels;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Rest::Configs;
use Amanda::Tapelist;
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
        "code" : "1600001",
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

    my $filename = config_dir_relative(getconf($CNF_TAPELIST));

    my ($tl, $message) = Amanda::Tapelist->new($filename);
    if (defined $message) {
	Dancer::status(405);
	return $message;
    } elsif (!defined $tl) {
	Dancer::status(405);
	return Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600000,
				severity => $Amanda::Message::ERROR,
				tapefile => $filename);
    }
    return $tl;
}

sub list {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Labels");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $tl = Amanda::Rest::Labels::init();
    if ($tl->isa("Amanda::Message")) {
	push @result_messages, $tl;
	return \@result_messages;
    }
    @tles = @{$tl->{'tles'}};
    @tles = grep {defined $_->{'config'}    and $_->{'config'}  eq $params{'config'}}                     @tles if defined $params{'config'};
    @tles = grep {defined $_->{'storage'}   and $_->{'storage'} eq $params{'storage'}}                    @tles if defined $params{'storage'};
    @tles = grep {defined $_->{'pool'}      and $_->{'pool'}    eq $params{'pool'}}                       @tles if defined $params{'pool'};
    @tles = grep {defined $_->{'meta'}      and $_->{'meta'}    eq $params{'meta'}}                       @tles if defined $params{'meta'};
    @tles = grep {defined $_->{'reuse'}     and $_->{'reuse'}   eq $params{'reuse'}}                      @tles if defined $params{'reuse'};
    @tles = grep {defined $_->{'datestamp'} and match_datestamp($params{'datestamp'}, $_->{'datestamp'})} @tles if defined $params{'datestamp'};
    push @result_messages, Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600001,
				severity => $Amanda::Message::SUCCESS,
				tles => \@tles);
    return \@result_messages;
}

1;
