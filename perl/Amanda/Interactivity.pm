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

package Amanda::Interactivity;

=head1 NAME

Amanda::Interactivity -- Parent class for user interactivity modules

=head1 SYNOPSIS

    use Amanda::Interactivity;

    my $inter = Amanda::Interactivity->new(name => 'stdin');
    $inter->user_request(
	message => "Insert Volume labelled 'MY_LABEL-001' in changer DLT",
	label => 'MY_LABEL-001',
	new_volume => 0,
	chg_name => 'DLT',
	err => "Not found in the library",
	request_cb => sub {
	    my ($err, $reply) = @_;
	    if ($err) {
		# error from the script
	    } elsif (!defined $reply) {
		# request aborted
	    } else {
		# use reply
	    }
	});

=head1 SUMMARY

This package provides a way for Amanda programs to communicate interactivityly
with the user.  The program can send a message to the user and await a textual
response.  The package operates asynchronously (see L<Amanda::MainLoop>), so
the program may continue with other activities while waiting for an answer from
the user.

Several interactivity modules are (or soon will be) available, and can be
selected by the user.

=head1 INTERFACE

A new object is create with the C<new> function as follows:

    my $inter = Amanda::Interactivity->new(
	name => $interactivity_name);

Where C<$interactivity_name> is the name of the desired interactivity defined
in the config file.

=head2 INTERACTIVITY OBJECTS

=head3 user_request

  $inter->user_request(message     => $message,
                       label       => $label,
                       new_volume  => 0|1,
                       err         => $err,
                       chg_name    => $chg_name,
                       request_cb  => $request_cb);

This method return immediately.  It sends a message to the user and waits for a
reply.
 C<err> is the reason why the volume is needed.
 C<message> is a sentence describing the requested volume.
 The volume can be describe with many parameters:
  C<label> is the requested label or the most prefered label.
  C<new_volume> if a new volume is acceptable.
  C<chg_name> the name of the changer where amanda expect the volume.

A module can print only C<message> or build something prettier with the values
of the other parameters.

The C<request_cb> callback take one or two arguments.  In the even of an
error, it is called with an C<Amanda::Changer::Error> object as first argument.
If the request is answered, then the first argument is C<undef> and the second
argument is the user's response.  If the request is aborted (see C<abort>,
below), then both arguments are C<undef>.

=head3 abort

  $inter->abort()

This method will abort all pending C<user_request> invocations, invoking their
C<request_cb> with C<(undef, undef)>.

=cut

use Amanda::Config qw( :getconf );

sub new {
    shift eq 'Amanda::Interactivity'
	or return;
    my %params = @_;
    my $interactivity_name = $params{'name'};

    return undef if !defined $interactivity_name or $interactivity_name eq '';

    my $interactivity = Amanda::Config::lookup_interactivity($interactivity_name);
    my $plugin;
    my $property;
    if ($interactivity) {
	$plugin = Amanda::Config::interactivity_getconf($interactivity, $INTERACTIVITY_PLUGIN);
	$property = Amanda::Config::interactivity_getconf($interactivity, $INTERACTIVITY_PROPERTY);
    } else {
	$plugin = $interactivity_name;
    }

    die("No name for Amanda::Interactivity->(new)") if !defined $plugin;

    my $pkgname = "Amanda::Interactivity::$plugin";
    my $filename = $pkgname;
    $filename =~ s|::|/|g;
    $filename .= '.pm';

    if (!exists $INC{$filename}) {
	eval "use $pkgname;";
	if ($@) {
	    my $err = $@;
	    die ($err);
	}
    }

    my $self = eval {$pkgname->new($property);};
    if ($@ || !defined $self) {
	print STDERR "Can't instantiate $pkgname\n";
	debug("Can't instantiate $pkgname");
	die("Can't instantiate $pkgname");
    }

    return $self;
}

1;
