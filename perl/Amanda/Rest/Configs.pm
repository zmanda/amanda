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
use Amanda::Config qw( :init :getconf config_dir_relative );
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

=item Amanda::Rest::Configs::getconf_byname

Interface to C<Amanda::Config::getconf_byname>
Return the value of the configuration keyword.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Configs::getconf_byname",
   "params" :{"config":"test",
              "name":"autolabel"},
   "id:     :"1"}

result:

  {"jsonrpc":"2.0",
   "result":{"other_config":"1",
             "volume_error":"1",
             "non_amanda":"1",
             "template":"JLM-TEST-$3s",
             "empty":"1"},
   "id":"1"}

The result vary with the type of the value.

=item Amanda::Rest::Configs::config_dir_relative

Interface to C<Amanda::Config::config_dir_relative>
Return the path relative to the config directory.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Configs::config_dir_relative",
   "params" :{"config":"test",
              "filename":"testfile"},
   "id:     :"2"}

result:

  {"jsonrpc":"2.0",
   "result":"/etc/amanda/test/testfile"
   "id":"2"}

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
    if (@{$config_overrides}) {
	my $g_config_overrides = Amanda::Config::new_config_overrides(@{$config_overrides} + 1);
	for my $co (@{$config_overrides}) {
	    add_config_override_opt($g_config_overrides, $co);
	}
	Amanda::Config::set_config_overrides($g_config_overrides);
    }
    Amanda::Config::config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);

    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_WARNINGS) {
	for my $cfgerr (@cfgerr_errors) {
	    push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => $cfgerr_level == $CFGERR_WARNINGS
						? 1500000 : 1500001,
				cfgerror => $cfgerr);
	}
    }

    return @result_messages;
}

sub getconf_byname {
    my %params = @_;

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $name             = $params{'name'};

    my $result = Amanda::Config::getconf_byname($name);
    if (!defined $result) {
	die [1003, "parameter do not exists", $name];
    }
    return $result;
}

sub config_dir_relative {
    my %params = @_;

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $filename      = $params{'filename'};

    my $result = Amanda::Config::config_dir_relative($filename);
    if (!defined $result) {
	die [1004, "config_dir_relative failed", $filename];
    }

    return $result;
}

sub configuration {
    my %params = @_;

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    die [1005, "Amanda::Rest::Configs::configuration not implemented yet"];
}

sub disklist {
    my %params = @_;

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    die [1005, "Amanda::Rest::Configs::disklist not implemented yet"];
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
