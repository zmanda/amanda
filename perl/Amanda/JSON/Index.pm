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

package Amanda::JSON::Index;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Header;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Index;
use Amanda::Curinfo;
use Amanda::JSON::Config;
use Symbol;
use Data::Dumper;
use XML::Simple;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Index -- JSON interface to Amanda::Index

=head1 INTERFACE

=over

=item Amanda::JSON::Index::get_header_buffer

Interface to C<Amanda::Index::get_header_buffer

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Index::get_header_buffer",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
	      "datestamp":"$datestamp"},
	      "level":"$level"},
   "id"     :"1"}

The result is an Amanda::Message

   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Index.pm",
	      "disk":"$disk",
	      "source_line":"334",
	      "code":1300003,
	      "host":"$host",
	      "message":"$host:$disk is set to a forced level 0 at next run."}],
   "id":"1"}

=item Amanda::JSON::Index::get_header

Interface to C<Amanda::Index::get_header>

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Index::get_header",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
	      "datestamp":"$datestamp"},
	      "level":"$level"},
   "id"     :"1"}

The result is an Amanda::Message

   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Index.pm",
	      "disk":"$disk",
	      "source_line":"334",
	      "code":1300003,
	      "host":"$host",
	      "message":"$host:$disk is set to a forced level 0 at next run."}],
   "id":"1"}

=cut

sub get_header_buffer {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $index = Amanda::Index->new();
    my $buffer = $index->get_header_buffer(host      => $params{'host'},
					  disk      => $params{'disk'},
					  datestamp => $params{'datestamp'},
					  level     => $params{'level'}	);
    return Amanda::Index::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 2400004,
			buffer   => $buffer);
}

sub get_header {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $index = Amanda::Index->new();
    my $hdr = $index->get_header(host      => $params{'host'},
				 disk      => $params{'disk'},
				 datestamp => $params{'datestamp'},
				 level     => $params{'level'}	);

    my %header = (
	type             => $hdr->{'type'},
	datestamp        => $hdr->{'datestamp'},
	dumplevel        => $hdr->{'dumplevel'},
	compressed       => $hdr->{'compressed'},
	encrypted        => $hdr->{'encrypted'},
	comp_suffix      => $hdr->{'comp_suffix'},
	encrypt_suffix   => $hdr->{'encrypt_suffix'},
	name             => $hdr->{'name'},
	disk             => $hdr->{'disk'},
	program          => $hdr->{'program'},
	application      => $hdr->{'application'},
	srvcompprog      => $hdr->{'srvcompprog'},
	clntcompprog     => $hdr->{'clntcompprog'},
	srv_encrypt      => $hdr->{'srv_encrypt'},
	clnt_encrypt     => $hdr->{'clnt_encrypt'},
	recover_cmd      => $hdr->{'recover_cmd'},
	uncompress_cmd   => $hdr->{'uncompress_cmd'},
	decrypt_cmd      => $hdr->{'decrypt_cmd'},
	srv_decrypt_opt  => $hdr->{'srv_decrypt_opt'},
	clnt_decrypt_opt => $hdr->{'clnt_decrypt_opt'},
	cont_filename    => $hdr->{'cont_filename'},
	dle_str          => $hdr->{'dle_str'},
	is_partial       => $hdr->{'is_partial'},
	partnum          => $hdr->{'partnum'},
	totalparts       => $hdr->{'totalparts'},
	blocksize        => $hdr->{'blocksize'},
	orig_size        => $hdr->{'orig_size'},
    );

    my $dle_str = $hdr->{'dle_str'};
    if ($dle_str) {
	my $p1 = XML::Simple->new();
	my $dle;
	eval { $dle = $p1->XMLin($dle_str); };
	if ($@) {
	    Amanda::Debug::debug("XML Error: $@\n$dle_str");
	} else {
	    $header{'dle'} = $dle;
	}
    }

    return Amanda::Index::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 2400005,
			header   => \%header);
}

sub get_index {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $index = Amanda::Index->new();
    my $buffer = $index->get_index(host      => $params{'host'},
				   disk      => $params{'disk'},
				   datestamp => $params{'datestamp'},
				   level     => $params{'level'}	);

    return Amanda::Index::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 2400006,
			index   => $buffer);
}

1;
