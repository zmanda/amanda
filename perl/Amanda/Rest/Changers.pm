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

package Amanda::Rest::Changers;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );;
use Amanda::Changer;
use Amanda::Message;
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Changers -- Rest interface to Amanda::Changer

=head1 INTERFACE

=over

=item Get the list of defined changer

 request:
  GET /amanda/v1.0/configs/:CONF/changers/:CHANGER

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1500021",
        "message" : "Defined changer",
        "severity" : "16",
        "source_filename" : "/usr/linux/lib/amanda/perl/Amanda/Rest/Changers.pm",
        "source_line" : "189",
        "changer" : [
         "my_vtapes",
         "my_robot"
        ]
     }
  ]

=item Get parameters values of a changer

 request:
  GET /amanda/v1.0/configs/:CONF/changers/:CHANGER?fields=runtapes&fields=foo

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1500018",
        "message" : "Not existant parameters in changer 'my_vtapes'",
        "parameters" : [
           "foo"
        ],
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Changers.pm",
        "source_line" : "1063",
        "changer" : "my_vtapes"
     },
     {
        "code" : "1500019",
        "message" : "Parameters values for changer 'my_vtapes'",
        "result" : {
           "runtapes" : 4
        },
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Changers.pm",
        "source_line" : "1071",
        "changer" : "my_vtapes"
     }
  ]

=back

=cut

sub fields {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Changers");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $changer_name = $params{'CHANGER'};
    my $st = Amanda::Config::lookup_changer_config($changer_name);
    if (!$st) {
        push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code      => 1500017,
				severity  => $Amanda::Message::ERROR,
				changer   => $changer_name);
	return \@result_messages;
    }

    my @no_parameters;
    my %values;
    if (defined $params{'fields'}) {
	my $type = Scalar::Util::reftype($params{'fields'});
	if (defined $type and $type eq "ARRAY") {
	    foreach my $name (@{$params{'fields'}}) {
		my $result = Amanda::Config::getconf_byname("changer:$changer_name:$name");
		if (!defined $result) {
		    push @no_parameters, $name;
		} else {
		    $values{$name} = $result;
		}
	    }
	} elsif (!defined $type and defined $params{'fields'} and $params{'fields'} ne '') {
	    my $name = $params{'fields'};
	    my $result = Amanda::Config::getconf_byname("changer:$changer_name:$name");
	    if (!defined $result) {
	        push @no_parameters, $name;
	    } else {
	        $values{$name} = $result;
	    }
	}
    }
    if (@no_parameters) {
        push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code       => 1500018,
				severity  => $Amanda::Message::ERROR,
				changer    => $changer_name,
				parameters => \@no_parameters);
    }
    if (%values) {
        push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code      => 1500019,
				severity  => $Amanda::Message::SUCCESS,
				changer    => $changer_name,
				result    => \%values);
    }
    if (!@no_parameters and !%values) {
        push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				changer   => $changer_name,
				code      => 1500009,
				severity  => $Amanda::Message::ERROR);
    }
    return \@result_messages;
}

sub list {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Changers");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my @changer = Amanda::Config::get_changer_list();
    if (!@changer) {
        push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code       => 1500020,
				severity  => $Amanda::Message::ERROR);
    } else {
        push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code       => 1500021,
				severity  => $Amanda::Message::SUCCESS,
				changer    => \@changer);
    }

    return \@result_messages;
}

1;
