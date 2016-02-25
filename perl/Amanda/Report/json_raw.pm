# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94085, or: http://www.zmanda.com
#

package Amanda::Report::json_raw;
use strict;
use warnings;
use base qw( Amanda::Report::human );
use Carp;

use POSIX;
use Data::Dumper;
use JSON; # imports encode_json, decode_json, to_json and from_json.

use Amanda::Config qw(:getconf config_dir_relative);
use Amanda::Util qw(:constants quote_string );
use Amanda::Holding;
use Amanda::Tapelist;
use Amanda::Debug qw( debug );
use Amanda::Util qw( quote_string );

use Amanda::Report;

## class functions

sub new
{
    my ($class, $report, $config_name, $logfname) = @_;

    my $self = {
        report      => $report,
        config_name => $config_name,
        logfname    => $logfname,
    };

    bless $self, $class;
    return $self;
}

sub write_report
{
    my ( $self, $fh ) = @_;

    $fh || confess "error: no file handle given to Amanda::Report::json_raw::write_report\n";
    $self->{fh} = $fh;

    my $json = JSON->new->allow_nonref;
    print {$self->{'fh'}} $json->pretty->encode($self->{'report'}->{'data'});

    return;
}

1;
