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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94085, or: http://www.zmanda.com

package Amanda::Interactivity::stdin;

use strict;
use warnings;
use POSIX qw( :errno_h );
use vars qw( @ISA );
@ISA = qw( Amanda::Interactivity );

use Amanda::Paths;
use Amanda::Util;
use Amanda::Debug qw( debug );
use Amanda::Changer;
use Amanda::MainLoop qw( :GIOCondition );

=head1 NAME

Amanda::Interactivity::stdin -- Interactivity class to read user request from stdin

=head1 SYNOPSIS

Amanda::Interactivity class to write user request on stdout and read reply
from stdin.

=cut

sub new {
    my $class = shift;
    my $storage_name = shift;
    my $changer_name = shift;
    my $properties = shift;

    if (!-r STDIN) {
	debug("Disabling Amanda::Interactivity::stdin because STDIN is not readable");
	return undef;
    }

    my $self = {
	input_src => undef,
	storage_name => $storage_name,
	changer_name => $changer_name,
	properties   => $properties
    };
    return bless ($self, $class);
}

sub abort {
    my $self = shift @_;

    if ($self->{'input_src'}) {
	$self->{'input_src'}->remove();
	$self->{'input_src'} = undef;
    }
}

sub user_request {
    my $self = shift @_;
    my %params = @_;
    my $buffer = "";

    my $message  = $params{'message'};
    my $label    = $params{'label'};
    my $err      = $params{'err'};
    my $storage_name = $params{'storage_name'};
    my $chg_name = $params{'chg_name'};

    my $data_in = sub {
	my $b;
	my $n_read = POSIX::read(0, $b, 1);
	if (!defined $n_read) {
	    return if ($! == EINTR);
	    $self->abort();
	    return $params{'request_cb'}->(
			Amanda::Changer::Error->new('fatal',
				storage => $storage_name,
				changer_name => $chg_name,
				dev => "stdin",
				code => 1110000));
	} elsif ($n_read == 0) {
	    $self->abort();
	    return $params{'request_cb'}->(
			Amanda::Changer::Error->new('fatal',
				storage => $storage_name,
				changer_name => $chg_name,
				code => 1110001));
	} else {
	    $buffer .= $b;
	    if ($b eq "\n") {
		my $line = $buffer;
		chomp $line;
		$buffer = "";
		$self->abort();
		return $params{'request_cb'}->(undef, $line);
	    }
	}
    };

    print "$err\n";
    print "Insert volume labeled '$label' in $chg_name\n";
    print "and press <enter> enter, or ^D to abort.\n";

    $self->{'input_src'} = Amanda::MainLoop::fd_source(0, $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    $self->{'input_src'}->set_callback($data_in);
    return;
}

1;
