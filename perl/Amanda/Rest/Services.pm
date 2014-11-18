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

package Amanda::Rest::Services;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Debug;
use Amanda::Paths;
use Amanda::Service;
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use JSON;
use IPC::Open3;

use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Services -- Rest interface to amservice

=head1 INTERFACE

=over

=item Run amservice

 request:
  GET localhost:5000/amanda/v1.0/configs/:CONFIG/service/discover
    query arguments:
        host=HOST
        application=APPLICATION
        auth=AUTH

 reply:
  HTTP status: 200 Ok
  [
   {
      "application" : "amgtar",
      "code" : "2900002",
      "message" : "The application 'amgtar' does not support the 'discover' method",
      "severity" : "16",
      "source_filename" : "senddiscover.c",
      "source_line" : "270"
   }
  ]

=back

=cut

sub discover {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Services");
    my @amservice_args;
    my @result_messages;
#    my @result_messages = Amanda::Rest::Configs::config_init(@_);
#    return \@result_messages if @result_messages;

    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    if (!defined $params{'application'} or !$params{'application'}) {
	return Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 3100004,
			severity => $Amanda::Message::ERROR);
    }
    if (!defined $params{'host'} or !$params{'host'}) {
	return Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 3100000,
			severity => $Amanda::Message::ERROR);
    }
    if (!defined $params{'auth'} or !$params{'auth'}) {
	return Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 3100001,
			severity => $Amanda::Message::ERROR);
    }
    push @amservice_args, $params{'host'};
    push @amservice_args, $params{'auth'};
    push @amservice_args, 'senddiscover';

    my $config_overrides = $params{'config_overrides'};
    if (defined $config_overrides) {
	if (ref $config_overrides eq 'ARRAY') {
	    for my $co (@{$config_overrides}) {
		push @amservice_args, "-o", $co;
	    }
	} else {
	    push @amservice_args, "-o", $config_overrides;
	}
    }

    Amanda::Debug::debug("Running: $Amanda::Paths::sbindir/amservice " . join(' ', @amservice_args));
    # fork the amservice process
    my($wtr, $rdr);
    my $pid = open3($wtr, $rdr, undef, "$Amanda::Paths::sbindir/amservice", @amservice_args);
    print $wtr "<dle>\n";
    if (defined $params{'diskdevice'}) {
	print $wtr "  <diskdevice>$params{'diskdevice'}</diskdevice>\n";
    }
    print $wtr "  <program>APPLICATION</program>\n";
    print $wtr "  <backup-program>\n";
    print $wtr "    <plugin>$params{'application'}</plugin>\n";
    if (defined $params{'esxpass'}) {
	print $wtr "    <property>\n";
	print $wtr "      <name>esxpass</name>\n";
	print $wtr "      <value>$params{'esxpass'}</value>\n";
	print $wtr "    </property>\n";
    }
    print $wtr "  </backup-program>\n";
    print $wtr "</dle>\n";
    close($wtr);

    #read stdout in a buffer
    my $first_line = <$rdr>;
    my $buf = "";
    while (my $line = <$rdr>) {
	$buf .= $line;
    }
    close($rdr);
    my $ret;

    if (!defined $first_line) {
	push @{$ret}, Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 3100006,
			severity => $Amanda::Message::ERROR);
    } elsif ($first_line =~ /OPTIONS /) {
	#convert JSON buffer to perl object
	eval { $ret = decode_json $buf };
	if ($@) {
	    $ret = undef;
	    push @{$ret}, Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 3100003,
			severity => $Amanda::Message::ERROR,
			errmsg   => $@,
			buffer   => $first_line . "\n" . $buf);
	}
    }  else {
	push @{$ret}, Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code     => 3100002,
			severity => $Amanda::Message::ERROR,
			errmsg   => $first_line . "\n" . $buf);
    }

    waitpid($pid, 0);
    my $child_error = $?;
    my $exit_code = $? >> 8;
    my $errno = $!;
    if ($child_error == -1) {
	push @{$ret}, Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3100008,
			severity        => $Amanda::Message::ERROR,
			program		=> "$Amanda::Paths::sbindir/amservice",
			errno		=> $errno);
    } elsif ($child_error & 127) {
	push @{$ret}, Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3100009,
			severity        => $Amanda::Message::ERROR,
			program		=> "$Amanda::Paths::sbindir/amservice",
			signal          => ($child_error & 127));
    } elsif ($child_error > 0) {
	push @{$ret}, Amanda::Service::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code            => 3100007,
			severity        => $exit_code == 0 ? $Amanda::Message::SUCCESS : $Amanda::Message::ERROR,
			program		=> "$Amanda::Paths::sbindir/amservice",
			exit_code       => $exit_code);
    }

    #return perl object
    return $ret
}

1;
