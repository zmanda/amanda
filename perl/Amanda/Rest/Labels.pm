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
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Labels -- Rest interface to Amanda::tapelist

=head1 INTERFACE

=over

=item Amanda::Rest::Labels::get

Return the list of all label in the tapelist file.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Labels::get",
   "params" :{"config":"test"},
   "id:     :"1"}

result:

  {"jsonrpc":"2.0",
   "result":{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Rest/Label.pm",
	      "source_line":"178",
	      "code":1600001,
	      "tles":[{"reuse":"1",
                       "comment":null,
                       "blocksize":"262144",
                       "pool":"pool",
                       "config":"config",
                       "position":1,
                       "barcode":null,
                       "label":"JLM-TEST-010",
                       "datestamp":"20120526083220",
                       "meta":null},
		      {"reuse":"1",
                       "comment":null,
                       "blocksize":"262144",
                       "pool":"pool",
                       "config":"config",
                       "position":2,
                       "barcode":null,
                       "label":"JLM-TEST-009",
                       "datestamp":"20120524110634",
                       "meta":null}],
   "message":"tapelist"},
   "id":"1"}

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
				tapefile => $filename);
    }
    return $tl;
}

sub list {
    my %params = @_;

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return @result_messages if @result_messages;

    my $tl = Amanda::Rest::Labels::init();
    if ($tl->isa("Amanda::Message")) {
	return $tl;
    }
    return Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600001,
				tles => $tl->{'tles'});
}

1;
