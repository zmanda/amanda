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

package Amanda::Rest::Report;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Report;
use Amanda::Report::json;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Label;
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Report -- Rest interface to Amanda::Report

=head1 INTERFACE

=over

=item Get a report

 request:
  GET /amanda/v1.0/configs/:CONF/report?logfile=/path/to/log/file

 reply:
  HTTP status: 200 OK
  [
     {
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Report.pm",
        "source_line" : "95"
        "code" : "1900001",
        "logfile" : "/var/amanda/test/log.20140204090209.0",
        "message" : "The report",
        "report" : {
	   ...
        },
     }
  ]

See perldoc Amanda::Report::json for the report format.

=back

=cut

sub report {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Report");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $config_name = $params{'CONF'};
    my $logfile = $params{'trace_log'} || $params{'logfile'};
    $logfile = "log" if !defined $logfile;

    my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
    $logfile = "$logdir/$logfile" if $logfile !~ /^\//;

    my $report = Amanda::Report->new($logfile);
    if ($report->isa("Amanda::Message")) {
	push @result_messages, $report;
	return \@result_messages;
    }

    my $rep = eval {Amanda::Report::json->new($report, $config_name,
					      $logfile);};
    if ($@) {
	push @result_messages, Amanda::Report::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code   => 1900000,
			severity => $Amanda::Message::ERROR,
			error  => $@);
    } else {
	$rep->generate_report();
	push @result_messages, Amanda::Report::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code    => 1900001,
			severity => $Amanda::Message::SUCCESS,
			logfile => $logfile,
			report => $rep->{'sections'});
    }

    return \@result_messages;
}

1;
