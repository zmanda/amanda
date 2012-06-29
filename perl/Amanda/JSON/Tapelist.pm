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

package Amanda::JSON::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::JSON::Config;
use Amanda::Tapelist;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Tapelist -- JSON interface to Amanda::tapelist

=head1 INTERFACE

=over

=item Amanda::JSON::Tapelist::get

Return the list of all label in the tapelist file.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Tapelist::get",
   "params" :{"config":"test"},
   "id:     :"1"}

result:

  {"jsonrpc":"2.0",
   "result" :{"tles":[{"reuse":"1",
                       "comment":null,
                       "blocksize":"262144",
                       "position":1,
                       "barcode":null,
                       "label":"JLM-TEST-010",
                       "datestamp":"20120526083220",
                       "meta":null},
		      {"reuse":"1",
                       "comment":null,
                       "blocksize":"262144",
                       "position":2,
                       "barcode":null,
                       "label":"JLM-TEST-009",
                       "datestamp":"20120524110634",
                       "meta":null}]},
   "id":"1"}

=item Amanda::JSON::Tapelist::add

Interface to C<Amanda::Tapelist::add_tapelabel>
Add a label to the tapelist file.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Tapelist::add",
   "params" :{"config":"test",
              "label":"new-label",
              "datestamp":"20120526083221",
              "reuse":"1",
              "barcode":"ABCDEF",
              "comment":"a comment",
              "blocksize":"262144",
              "meta":"AB"},
   "id:     :"2"}

config and label are required.

result:

  {"jsonrpc":"2.0",
   "result":"OK",
   "id":"2"}

=item Amanda::JSON::Tapelist::update

Modify a entry in the tapelist file.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Tapelist::add",
   "params" :{"config":"test",
              "label":"new-label",
              "datestamp":"20120526083221",
              "reuse":"1",
              "barcode":"ABCDEF",
              "comment":"a comment",
              "blocksize":"262144",
              "meta":"AB"},
   "id:     :"3"}

config and label are required.

result:

  {"jsonrpc":"2.0",
   "result":"OK",
   "id":"3"}

=item Amanda::JSON::Tapelist:remove

Interface to C<Amanda::Tapelist::remove_tapelabel>
remove an entry from the tapelist file.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Tapelist::remove",
   "params" :{"config":"test",
              "label" :"new-label"},
   "id:     :"4"}

config and label are required.

result:

  {"jsonrpc":"2.0",
   "result":"OK",
   "id":"4"}

=back

=cut

#
#internal function
#

sub init {
    my %params = @_;

    my $filename = config_dir_relative(getconf($CNF_TAPELIST));

    my $tl = Amanda::Tapelist->new($filename);
    if (!defined $tl) {
	die [3002, "failed to read tapelist file", $filename];
    }
    return $tl;
}

sub get {
    my %params = @_;

    Amanda::JSON::Config::config_init(@_);

    my $filename = config_dir_relative(getconf($CNF_TAPELIST));

    my $tl = Amanda::Tapelist->new($filename);
    if (!defined $tl) {
	die [3002, "failed to read tapelist file", $filename];
    }
    return { "tles" => $tl->{'tles'} };
}

sub update {
    my %params = @_;

    Amanda::JSON::Config::config_init(@_);
    $tl = Amanda::JSON::Tapelist::init(@_);

    my $label = $params{'label'};
    my $datestamp = $params{'datestamp'};
    my $reuse = $params{'reuse'};
    my $barcode = $params{'barcode'};
    my $meta = $params{'meta'};
    my $blocksize = $params{'blocksize'};
    my $comment = $params{'comment'};

    $tl->reload(1);
    $tle = $tl->lookup_tapelabel($label);

    if (!$tle) {
	$tl->unlock();
	die [3003, "label does not exist", $label];
    }
    if (defined $datestamp) {
	$tle->{'datestamp'} = $datestamp;
    }
    if (defined $reuse) {
	$tle->{'reuse'} = $reuse;
    }
    if (exists $params{'barcode'}) {
	if (!defined $barcode || $barcode eq "") {
	    $tle->{'barcode'} = undef;
	} else {
	    $tle->{'barcode'} = $barcode;
	}
    }
    if (exists $params{'meta'}) {
	if (!defined $meta || $meta eq "") {
	    $tle->{'meta'} = undef;
	} else {
	    $tle->{'meta'} = $meta;
	}
    }
    if (exists $params{'blocksize'}) {
	if (!defined $blocksize || $blocksize eq "") {
	    $tle->{'blocksize'} = undef;
	} else {
	    $tle->{'blocksize'} = $blocksize;
	}
    }
    if (exists $params{'comment'}) {
	if (!defined $comment || $comment eq "") {
	    $tle->{'comment'} = undef;
	} else {
	    $tle->{'comment'} = $comment;
	}
    }
    $tl->write();

    return "OK";
}

sub add {
    my %params = @_;

    Amanda::JSON::Config::config_init(@_);
    $tl = Amanda::JSON::Tapelist::init(@_);

    my $label = $params{'label'};
    my $datestamp = $params{'datestamp'};
    my $reuse = $params{'reuse'};
    my $barcode = $params{'barcode'};
    my $meta = $params{'meta'};
    my $blocksize = $params{'blocksize'};
    my $comment = $params{'comment'};


    $tl->reload(1);
    $tle = $tl->lookup_tapelabel($label);

    if (defined $tle) {
	$tl->unlock();
	die [3004, "label already exist", $label];
    }
    $tl->add_tapelabel($datestamp, $label, $comment, $reuse, $meta, $barcode, $blocksize);
    $tl->write();

    return "OK";
}

sub remove {
    my %params = @_;

    Amanda::JSON::Config::config_init(@_);
    $tl = Amanda::JSON::Tapelist::init(@_);

    my $label = $params{'label'};

    $tl->reload(1);
    $tl->remove_tapelabel($label);
    $tl->write();

    return "OK";
}

1;
