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

package Amanda::Interactivity::email;

use strict;
use warnings;
use POSIX qw( :errno_h );
use vars qw( @ISA );
use IPC::Open3;
@ISA = qw( Amanda::Interactivity );

use Amanda::Paths;
use Amanda::Util;
use Amanda::Debug qw( debug );
use Amanda::Config qw( :getconf );
use Amanda::Changer;
use Amanda::MainLoop qw( :GIOCondition );

=head1 NAME

Amanda::Interactivity::email -- Interactivity class to send user request by email

=head1 SYNOPSIS

Amanda::Interactivity class to write user request by email

=cut

sub new {
    my $class = shift;
    my $properties = shift;

    my $self = {
	send_email_src => undef,
	check_file_src => undef,
	properties     => $properties,
    };

    return bless ($self, $class);
}

sub abort {
    my $self = shift;

    if ($self->{'send_email_src'}) {
	$self->{'send_email_src'}->remove();
    }
    if ($self->{'check_file_src'}) {
	$self->{'check_file_src'}->remove();
    }
}

sub user_request {
    my $self = shift;
    my %params = @_;
    my $buffer = "";

    my $message    = $params{'message'};
    my $label      = $params{'label'};
    my $new_volume = $params{'new_volume'};
    my $err        = $params{'err'};
    my $chg_name   = $params{'chg_name'};

    my $resend_delay;
    if (defined $self->{'properties'}->{'resend-delay'}) {
	$resend_delay = 1000 * $self->{'properties'}->{'resend-delay'}->{'values'}->[0];
    }
    my $check_file;
    if (defined $self->{'properties'}->{'check-file'}) {
	$check_file = $self->{'properties'}->{'check-file'}->{'values'}->[0];
    }

    my $check_file_delay = 10000;
    if (defined $self->{'properties'}->{'check-file-delay'}) {
	$check_file_delay = 1000 * $self->{'properties'}->{'check_file-delay'}->{'values'}->[0];
    }

    my $mailer  = getconf($CNF_MAILER);
    my $subject;
    if ($label) {
	$subject = "AMANDA VOLUME REQUEST: $label";
    } else {
	$subject = "AMANDA VOLUME REQUEST: new volume";
    }

    my $mailto;
    if (defined $self->{'properties'}->{'mailto'}) {
	$mailto = $self->{'properties'}->{'mailto'}->{'values'};
    } else {
	my $a = getconf($CNF_MAILTO);
	my @mailto = split (/ /, getconf($CNF_MAILTO));
	$mailto = \@mailto;
    }
    my @cmd = ("$mailer", "-s", $subject, @{$mailto});

    my $send_email_cb;
    $send_email_cb = sub {
	$self->{'send_email_src'} = undef;
	debug("cmd: " . join(" ", @cmd) . "\n");
	my ($pid, $fh);
	$pid = open3($fh, ">&2", ">&2", @cmd);
	print {$fh} "$err\n";
	if ($label && $new_volume) {
	    print {$fh} "Insert volume labeled '$label' or a new volume in $chg_name\n";
	} elsif ($label) {
	    print {$fh} "Insert volume labeled '$label' in $chg_name\n";
	} else {
	    print {$fh} "Insert a new volume in $chg_name\n";
	}
	if ($check_file) {
	    print {$fh} "or write the name of a new changer in '$check_file'\n";
	    print {$fh} "or write 'abort' in the file to abort the scan.\n";
	}
	close $fh;
	unlink($check_file);

	if ($resend_delay) {
	    $self->{'send_email_src'} = Amanda::MainLoop::call_after($resend_delay, $send_email_cb);
	}
    };

    my $check_file_cb;
    $check_file_cb = sub {
	$self->{'check_file_src'} = undef;

	if (-f $check_file) {
	    my $fh;
	    open ($fh, '<' , $check_file);
	    my $line = <$fh>;
	    chomp $line;
	    $self->abort();
	    if ($line =~ /^abort$/i) {
		return $params{'request_cb'}->(
			Amanda::Changer::Error->new('fatal',
				message => "Aborted by user"));
	    } else {
		return $params{'request_cb'}->(undef, $line);
	    }
	}
	$self->{'check_file_src'} = Amanda::MainLoop::call_after($check_file_delay, $check_file_cb);
    };

    $send_email_cb->();
    if ($check_file) {
	unlink($check_file);
	$check_file_cb->();
    }
}

1;
