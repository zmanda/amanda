# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Interactivity::tty;

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

Amanda::Interactivity::tty -- Interactivity class to read user request from /dev/tty

=head1 SYNOPSIS

Amanda::Interactivity class to write user request on /dev/tty and read reply
from /dev/tty.

=cut

sub new {
    my $class = shift;
    my $property = shift;

    my $input;
    my $output;
    my $abort_message;

    my $r = open($input, '<', "/dev/tty");
    if (!$r) {
	$abort_message = "Failed to open /dev/tty: $!";
    } else {
        $r = open($output, '>', "/dev/tty");
	if (!$r) {
	    $abort_message = "Failed to open /dev/tty: $!";
	    close $input;
	    $input = undef;
	}
    }
    my $self = {
	input_src     => undef,
	input         => $input,
	output        => $output,
	property      => $property,
	abort_message => $abort_message,
    };
    return bless ($self, $class);
}

sub DESTROY {
    my $self = shift;

    close($self->{'input'})  if $self->{'input'};
    close($self->{'output'}) if $self->{'output'};
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

    my $message    = $params{'message'};
    my $label      = $params{'label'};
    my $new_volume = $params{'new_volume'};
    my $err        = $params{'err'};
    my $chg_name   = $params{'chg_name'};

    my $data_in = sub {
	my $b;
	my $n_read = POSIX::read(fileno($self->{'input'}), $b, 1);
	if (!defined $n_read) {
	    return if ($! == EINTR);
	    $self->abort();
	    return $params{'request_cb'}->(
			Amanda::Changer::Error->new('fatal',
				message => "Fail to read from /dev/tty"));
	} elsif ($n_read == 0) {
	    $self->abort();
	    return $params{'request_cb'}->(
			Amanda::Changer::Error->new('fatal',
				message => "Aborted by user"));
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

    if ($self->{'abort_message'}) {
	return $params{'request_cb'}->(
			Amanda::Changer::Error->new('fatal',
				message => $self->{'abort_message'}));
    }

    print {$self->{'output'}} "$err\n";
    if ($label && $new_volume) {
	print {$self->{'output'}} "Insert volume labeled '$label' or a new volume in $chg_name\n";
    } elsif ($label) {
	print {$self->{'output'}} "Insert volume labeled '$label' in $chg_name\n";
    } else {
	print {$self->{'output'}} "Insert a new volume in $chg_name\n";
    }
    print {$self->{'output'}} "and press <enter> enter, or ^D to abort.\n";

    $self->{'input_src'} = Amanda::MainLoop::fd_source(
						fileno($self->{'input'}),
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    $self->{'input_src'}->set_callback($data_in);
    return;
}

1;
