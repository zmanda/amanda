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

package Amanda::Interactivity::email;

use strict;
use warnings;
use POSIX qw( :errno_h );
use vars qw( @ISA );
use IPC::Open3;
use File::stat;
use Time::localtime;
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
    my $storage_name = shift;
    my $changer_name = shift;
    my $properties = shift;

    my $self = {
	send_email_src => undef,
	check_file_src => undef,
	storage_name   => $storage_name,
	changer_name   => $changer_name,
	properties     => $properties,
    };

    if (defined $self->{'properties'}->{'check-file'}) {
	my $check_file = $self->{'properties'}->{'check-file'}->{'values'}->[0];
    }

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
    my $storage_name = $params{'storage_name'};
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
    if ($chg_name) {
	$subject = "AMANDA VOLUME REQUEST ($chg_name):";
    } else {
	$subject = "AMANDA VOLUME REQUEST:";
    }
    if ($label) {
	$subject .= " $label";
    } else {
	$subject .= " new volume";
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
	$self->{'send_email_src'}->remove() if defined $self->{'send_email_src'};
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

	    # check_file_cb actually monitors the file's ctime and mtime
	    # timestamps separately, but it seems clearer for the email
	    # just to report the latest of the two to the user -- in
	    # practice the single date should be enough for the user to
	    # understand what Amanda is waiting for....
	    my $modtime = $self->{'check_file_mtime'};
	    if ( $self->{'check_file_ctime'} > $modtime ) {
		$modtime = $self->{'check_file_ctime'};
	    }
	    if ( $modtime > 0 ) {
		print {$fh} "\n(Waiting for the file to be modified after:\n" .
			ctime($modtime) . ".)\n";
	    } else {
		print {$fh} "\n(Waiting for the file to be created.)\n";
	    }

	    # if check_file_cb detected any warning message, include it
	    # here.
	    if ($self->{'check_file_message'}) {
		print {$fh} $self->{'check_file_message'}
	    }
	}
	close $fh;

	if ($resend_delay) {
	    $self->{'send_email_src'} = Amanda::MainLoop::call_after($resend_delay, $send_email_cb);
	}
    };

    my $check_file_cb;
    $check_file_cb = sub {
	$self->{'check_file_src'}->remove() if $self->{'check_file_src'};
	$self->{'check_file_src'} = undef;

	if (-e $check_file) {
	    my $check_file_mtime = (stat($check_file))->mtime;
	    my $check_file_ctime = (stat($check_file))->ctime;
	    if ($self->{'check_file_mtime'} < $check_file_mtime or
		$self->{'check_file_ctime'} < $check_file_ctime) {

		# The user has modified the file, so we stop
		# the email callback.  If we detect a problem with the
		# file below, the email callback is restarted; otherwise
		# this Interactivity call returns.  (The caller may
		# immediately invoke a new Interactivity call
		# afterwards, if the user hasn't aborted and the caller
		# still hasn't found the desired volume.... in which
		# case the new call will start its own email callback.)
		$self->{'send_email_src'}->remove() if $self->{'send_email_src'};
		$self->{'send_email_src'} = undef;

		# (save updated values, in case we don't return below.)
		$self->{'check_file_ctime'} = $check_file_ctime;
		$self->{'check_file_mtime'} = $check_file_mtime;
		$self->{'check_file_message'} = undef;

		if (!-f $check_file) {
		    $self->{'check_file_message'} = "\nThe check-file '$check_file' is not a flat file.\n";
		} elsif (!-r $check_file) {
		    $self->{'check_file_message'} = "\nThe check-file '$check_file' is not readable.\n";
		}
		if ($self->{'check_file_message'}) {
		    $send_email_cb->();
		} else {
		    my $fh;
		    open ($fh, '<' , $check_file);
		    my $line = <$fh>;
		    close($fh);
		    $send_email_cb = undef;
		    $check_file_cb = undef;
		    if ($line) {
			chomp $line;
			$self->abort();
			if ($line =~ /^abort$/i) {
			    return $params{'request_cb'}->(
				Amanda::Changer::Error->new('fatal',
					storage => $storage_name,
					changer_name => $chg_name,
					code => 1110001));
			} else {
			    return $params{'request_cb'}->(undef, $line);
			}
		    } else {
			return $params{'request_cb'}->(undef, '');
		    }
		}
	    }
	} else {
	    # the file doesn't currently exist
	    $self->{'check_file_mtime'} = 0;
	    $self->{'check_file_ctime'} = 0;
	    $self->{'check_file_message'} = undef
	}
	$self->{'check_file_src'} = Amanda::MainLoop::call_after($check_file_delay, $check_file_cb);
    };

    if ($check_file) {
	# save the initial timestamps of the file, so we can detect
	# when the user updates it later.
	if (-e $check_file) {
	    $self->{'check_file_mtime'} = (stat($check_file))->mtime;
	    $self->{'check_file_ctime'} = (stat($check_file))->ctime;
	    $self->{'check_file_message'} = undef
	} else {
	    $self->{'check_file_mtime'} = 0;
	    $self->{'check_file_ctime'} = 0;
	    $self->{'check_file_message'} = undef
        }
    }

    $send_email_cb->();
    if ($check_file) {
	$check_file_cb->();
    }
}

1;
