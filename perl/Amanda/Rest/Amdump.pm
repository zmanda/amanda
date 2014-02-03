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

package Amanda::Rest::Amdump;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Amdump;
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Amdump -- Rest interface to Amanda::Amdump

=head1 INTERFACE

=over

=item Amanda::Rest::Amdump::run

Interface to C<Amanda::Amdump::run>

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Amdump::run",
   "params" :{"config":"test",
	      "no_taper":"0",
	      "from_client":"0",
	      "exact_match":"0",
              "hostdisk":["localhost","/boot"]},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename" : "/usr/lib/amanda/perl/Amanda/Amdump.pm",
              "trace_log" : "/etc/amanda/test/log.20130904144908.0",
              "source_line" : "89",
              "severity" : "16",
              "code" : "2000000",
              "message" : "The trace log file is '/var/amanda/test/log.20130904144908.0'" },
             { "source_filename" : "/usr/lib/amanda/perl/Amanda/Amdump.pm",
              "amdump_log" : "/etc/amanda/test/amdump.20130904144908",
              "source_line" : "94",
              "severity" : "16",
              "code" : "2000001",
              "message" : "The amdump log file is 'amdump.20130904144908'"}],
   "id":"1"}

=back

=cut

sub run {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    Amanda::Util::set_pname("amdump");
    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    my ($amdump, $messages) = Amanda::Amdump->new(@_, user_msg => $user_msg);
    push @result_messages, @{$messages};

    # fork the amdump process and detach
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $exit_code = $amdump->run(0);
	debug("exiting with code $exit_code");
	exit($exit_code);
    }

    push @result_messages, Amanda::Amdump::Message->new(
	source_filename => __FILE__,
	source_line     => __LINE__,
	code         => 2000002,
	severity     => $Amanda::Message::INFO);

    return \@result_messages;
}

1;
