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

package Amanda::Rest::Configs;
use Amanda::Config qw( :getconf config_dir_relative );
use Symbol;
use Data::Dumper;
use Scalar::Util;
use vars qw(@ISA);
use Amanda::Debug;
use Amanda::Util qw( :constants );

=head1 NAME

Amanda::Rest::Configs -- Rest interface to Amanda::Config

=head1 INTERFACE

=over

=item Get a list of config

=begin html

<pre>

=end html

request:
  GET /amanda/v1.0/configs

reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1500003",
        "config" : [
	  "test",
        ],
        "message" : "config name",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Configs.pm",
        "source_line" : "187"
     }
  ]

reply:
  HTTP status: 404 Not found
  [
     {
        "code" : "1500004",
        "message" : "no config",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Configs.pm",
        "source_line" : "193"
     }
  ]

reply:
  HTTP status: 404 Not found
  [
     {
        "code" : "1500006",
        "dir" : "/etc/amanda",
        "errno" : "No such file or directory",
        "message" : "Can't open config directory '/etc/amanda': No such file or directory",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Configs.pm",
        "source_line" : "176"
     }
  ]

=begin html

</pre>

=end html

=item Get the value of global parameters

=begin html

<pre>

=end html

request:
  GET /amanda/v1.0/configs/:CONF?fields=runtapes,foo,tapecycle,bar

result:
  [
     {
        "code" : "1500007",
        "message" : "Not existant parameters",
        "parameters" : [
           "foo",
           "bar"
        ],
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Configs.pm",
        "source_line" : "144"
     },
     {
        "code" : "1500008",
        "message" : "Parameters values",
        "result" : {
           "runtapes" : 3,
           "tapecycle" : 50
        },
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Configs.pm",
        "source_line" : "151"
     }
  ]

request:
  GET /amanda/v1.0/configs/:CONF

result:
  [
     {
        "code" : "1500009",
        "message" : "No fields specified",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Configs.pm",
        "source_line" : "194"
     }
  ]

=begin html

</pre>

=end html

=back

=cut

sub config_init {
    my %params = @_;
    my $config_name      = $params{'CONF'};
    my $config_overrides = $params{'config_overrides'};

    my @result_messages;

    if (!defined $config_name) {
	push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1500002);
    }

    Amanda::Config::config_uninit();
    if (defined $config_overrides and @{$config_overrides}) {
	my $g_config_overrides = Amanda::Config::new_config_overrides(@{$config_overrides} + 1);
	for my $co (@{$config_overrides}) {
	    add_config_override_opt($g_config_overrides, $co);
	}
	Amanda::Config::set_config_overrides($g_config_overrides);
    }
    Amanda::Config::config_init($Amanda::Config::CONFIG_INIT_EXPLICIT_NAME, $config_name);

    my ($cfgerr_level, @cfgerr_errors) = Amanda::Config::config_errors();
    if ($cfgerr_level >= $Amanda::Config::CFGERR_WARNINGS) {
	for my $cfgerr (@cfgerr_errors) {
	    push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => $cfgerr_level == $Amanda::Config::CFGERR_WARNINGS
						? 1500000 : 1500001,
				cfgerror => $cfgerr);
	}
    }

    return @result_messages;
}

sub fields {
    my %params = @_;

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my @no_parameters;
    my %values;
    if (defined $params{'fields'}) {
	foreach my $name (split ',', $params{'fields'}) {
	    my $result = Amanda::Config::getconf_byname($name);
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
				code      => 1500007,
				parameters => \@no_parameters);
    }
    if (%values) {
	push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code      => 1500008,
				result    => \%values);
    }
    if (!@no_parameters and !%values) {
	push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code      => 1500009);
    }
    return \@result_messages;
}

sub list {
    my %params = @_;
    my @result_messages;

    if (!opendir(my $dh, $Amanda::Paths::CONFIG_DIR)) {
	push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1500006,
				errno    => $!,
				dir      => $Amanda::Paths::CONFIG_DIR);
	Dancer::status(404);
    } else {
	my @conf = grep { !/^\./ && -f "$Amanda::Paths::CONFIG_DIR/$_/amanda.conf" } readdir($dh);
	closedir($dh);
	if (@conf) {
	    push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1500003,
				config   => \@conf);
	} else {
	    push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1500004);
	    Dancer::status(404);
	}
    }
    return \@result_messages;
}


1;
