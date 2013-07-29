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

package Amanda::JSON::Dle;
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
use Amanda::JSON::Config;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Dle -- JSON interface to Amanda::Curinfo and other
		     DLE functionnalities.

=head1 INTERFACE

=over

=item Amanda::JSON::Dle::force

Interface to C<Amanda::Curinfo::force>
Force a full backup of a Dle at next run.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::force",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Curinfo.pm",
	      "disk":"$disk",
	      "source_line":"334",
	      "code":1300003,
	      "host":"$host",
	      "message":"$host:$disk is set to a forced level 0 at next run."}],
   "id":"1"}

=item Amanda::JSON::Dle::force_level_1

Interface to C<Amanda::Curinfo::force_level_1>
Force a level 1 backup of a Dle at next run.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::force_level_1",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Curinfo.pm",
	      "disk":"$disk",
	      "source_line":"432",
	      "code":1300023,
	      "host":"$host",
	      "message":"$host:$disk is set to a forced level 1 at next run."}],
   "id":"1"}

=item Amanda::JSON::Dle::unforce

Interface to C<Amanda::Curinfo::unforce>
Remove force/force_level_1 command for a Dle.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::unforce",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Curinfo.pm",
	      "disk":"$disk",
	      "source_line":"363",
	      "code":1300019,
	      "host":"$host",
	      "message":"force command for $host:$disk cleared."}],
   "id":"1"}

=item Amanda::JSON::Dle::force_bump

Interface to C<Amanda::Curinfo::force_bump>
Force a bump to next higher level of a Dle at next run.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::force_bump",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Curinfo.pm",
	      "disk":"$disk",
	      "source_line":"484",
	      "code":1300025,
	      "host":"$host",
	      "message":"$host:$disk is set to bump at next run."}],
   "id":"1"}

=item Amanda::JSON::Dle::force_no_bump

Interface to C<Amanda::Curinfo::force_no_bump>
Prevent a Dle to bump at next run.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::force_no_bump",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Curinfo.pm",
	      "disk":"$disk",
	      "source_line":"518",
	      "code":1300026,
	      "host":"$host",
	      "message":"$host:$disk is set to not bump at next run."}],
   "id":"1"}

=item Amanda::JSON::Dle::unforce_bump

Interface to C<Amanda::Curinfo::unforce_bump>
Remove force_bump/force_no_bump command for a Dle.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Dle::unforce_bump",
   "params" :{"config":"test",
	      "host":"$host",
	      "disk":"$disk"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Curinfo.pm",
	      "disk":"$disk",
	      "source_line":"545",
	      "code":1300027,
	      "host":"$host",
	      "message":"bump command for $host:$disk cleared."}],
   "id":"1"}

=back

=cut

sub do_dle {
    my $func_name = shift;
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
    Amanda::Disklist::unload_disklist();
    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	return Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400006,
			diskfile     => $diskfile,
			cfgerr_level => $cfgerr_level);
    }

    my $curinfodir = getconf($CNF_INFOFILE);;
    my $ci = Amanda::Curinfo->new($curinfodir);

    my $host = Amanda::Disklist::get_host($params{'host'});
    if (!$host) { # $host->isa("Amanda::Message");
	return Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400007,
			diskfile     => $diskfile,
			host         => $params{'host'});
    }
    my $disk = $host->get_disk($params{'disk'});
    if (!$disk) {  # $disk->isa("Amanda::Message");
	return Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400008,
			diskfile     => $diskfile,
			host         => $params{'host'},
			disk         => $params{'disk'});
    }

    my @a = $ci->$func_name($disk);
    return \@a;
}

sub force {
    return do_dle("force",@_);
}

sub force_level_1 {
    return do_dle("force_level_1",@_);
}

sub unforce {
    return do_dle("unforce",@_);
}

sub force_bump {
    return do_dle("force_bump",@_);
}

sub force_no_bump {
    return do_dle("force_no_bump",@_);
}

sub unforce_bump {
    return do_dle("unforce_bump",@_);
}

1;
