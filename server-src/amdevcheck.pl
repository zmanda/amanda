#! @PERL@
# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :getconf :init );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :running_as_flags );

# try to open the device and read its label, returning the device_read_label
# result (one or more of ReadLabelStatusFlags)
sub try_read_label {
    my ($device_name) = @_;

    if ( !$device_name ) {
        return $READ_LABEL_STATUS_DEVICE_MISSING;
    }

    my $device = Amanda::Device->new($device_name);
    if ( !$device ) {
        return $READ_LABEL_STATUS_DEVICE_MISSING
             | $READ_LABEL_STATUS_DEVICE_ERROR;
    }

    $device->set_startup_properties_from_config();

    return $device->read_label();
}

# print the results, one flag per line
sub print_result {
    my ($flags) = @_;

    print join( "\n", ReadLabelStatusFlags_to_strings($flags) ), "\n";
}

sub usage {
    print <<EOF;
Usage: amdevcheck <config> [ <device name> ]
EOF
    exit(1);
}

## Application initialization

Amanda::Util::setup_application("amdevcheck", "server", "cmdline");

usage() if ( @ARGV < 1 || @ARGV > 2 );
my $config_name = $ARGV[0];
if (!config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name)) {
    die('errors processing config file "' .
	       Amanda::Config::get_config_filename() . '"');
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

## Check the device

my $device_name;
if ( $#ARGV == 1 ) {
    $device_name = $ARGV[1];
} else {
    $device_name = getconf($CNF_TAPEDEV);
}

print_result( try_read_label($device_name) );
