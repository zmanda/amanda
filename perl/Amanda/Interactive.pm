# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Interactive;

=head1 NAME

Amanda::Interactive -- Parent class for user interactivity modules

=head1 SYNOPSIS

    use Amanda::Interactive;

    my $inter = Amanda::Interactive->new(name => 'stdin',
					 inter_conf => $inter_conf);
    $inter->user_request(
	message => "Insert Volume labelled 'MY_LABEL-001'",
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

This package provides a way for Amanda programs to communicate interactively
with the user.  The program can send a message to the user and await a textual
response.  The package operates asynchronously (see L<Amanda::MainLoop>), so
the program may continue with other activities while waiting for an answer from
the user.

Several interactivity modules are (or soon will be) available, and can be
selected by the user.

=head1 INTERFACE

A new object is create with the C<new> function as follows:

    my $inter = Amanda::Interactive->new(
	name => $interactive_name,
	inter_conf => $inter_conf);

Where C<$interactive_name> is the name of the desired interactivity module
(e.g., C<'stdin'>).

=head2 INTERACTIVE OBJECTS

=head3 user_request

  $inter->user_request(message     => $message,
                       label       => $label,
                       err         => $err,
                       request_cb  => $request_cb);

This method return immediately.  It sends C<message> to the user and waits for a
reply.  The C<label> and C<err> parameters .. well, what do they do? (TODO)

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

sub new {
    shift eq 'Amanda::Interactive'
	or return;
    my %params = @_;
    my $name = $params{'name'};

    die("No name for Amanda::Interactive->(new)") if !defined $name;

    my $pkgname = "Amanda::Interactive::$name";
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

    my $inter = $pkgname->new(%params);

    return $inter;
}

1;
