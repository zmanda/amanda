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

package Amanda::JSON::Amflush;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw ( match_datestamp );
use Amanda::Message;
use Amanda::Amflush;
use Amanda::JSON::Config;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Label -- JSON interface to Amanda::Label

=head1 INTERFACE

=over

=item Amanda::JSON::Amflush::run

Interface to C<Amanda::Amflush::run>

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Amflush::run",
   "params" :{"config":"test",
	      "datestamps":["20130909090909"],
	      "exact_match":"0",
              "hostdisk":["localhost","/boot"]},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename" : "/usr/lib/amanda/perl/Amanda/Amflush.pm",
              "trace_log" : "/etc/amanda/test/log.20130904144908.0",
              "source_line" : "89",
              "severity" : "16",
              "code" : "2200000",
              "message" : "The trace log file is '/amanda/h1/var/amanda/test/log.20130904144908.0'" },
             { "source_filename" : "/usr/lib/amanda/perl/Amanda/Amflush.pm",
              "amdump_log" : "/etc/amanda/test/amdump.20130904144908",
              "source_line" : "94",
              "severity" : "16",
              "code" : "2200001",
              "message" : "The amdump log file is 'amdump.20130904144908'"}],
   "id":"1"}

=back

=cut

sub run {
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

    Amanda::Util::set_pname("amflush");
    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    Amanda::Disklist::match_disklist(
        user_msg => \&user_msg,
        exact_match => $params{'exact_match'},
        args        => $params{'hostdisk'});
    my ($amflush, $messages) = Amanda::Amflush->new(@_, user_msg => $user_msg);
    push @result_messages, @{$messages};
    open STDERR, ">>&", $amflush->{'amdump_log'} || die("stdout: $!");

    my $code;
    my @ts = Amanda::Holding::get_all_datestamps();
    my @datestamps;
    if (defined $params{'datestamps'} and @{$params{'datestamps'}}) {
	foreach my $datestamp (@{$params{'datestamps'}}) {
	    my $matched = 0;
	    foreach my $ts (@ts) {
		if (match_datestamp($datestamp, $ts)) {
		    push @datestamps, $ts;
		    $matched = 1;
		    last;
		}
	    }
	    if (!$matched) {
		push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 2200002,
			severity     => $Amanda::Message::INFO,
			datestamp    => $datestamp);
	    }
	}
	$code = 2200003;
    } else {
	@datestamps = @ts;
	$code = 2200004;
    }
    if (!@datestamps) {
	push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => $code,
			severity     => $Amanda::Message::WARNING);
	return \@result_messages;
    }

    # fork the amdump process and detach
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $to_flushs = $amflush->get_flush(datestamps => \@datestamps);
	my $exit_code = $amflush->run(0, $to_flushs);
	debug("exiting with code $exit_code");
	exit($exit_code);
    }
    push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 2200005,
			severity     => $Amanda::Message::INFO);
    return \@result_messages;
}

1;
