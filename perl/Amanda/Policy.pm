# Copyright (c) 2012-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Policy;

use strict;
use warnings;
use Data::Dumper;
use vars qw( @ISA );

use Amanda::Config qw( :getconf );
use Amanda::Debug qw( debug );

=head1 NAME

Amanda::Policy -- interface to policy

=head1 SYNOPSIS

    everything is done by the Amanda::Storage

=head1 INTERFACE

All operations in the module return immediately, and take as an argument a
callback function which will indicate completion of the changer operation -- a
kind of continuation.  The caller should run a main loop (see
L<Amanda::MainLoop>) to allow the interactions with the changer script to
continue.

A new object is created with the C<new> function as follows:

  my ($policy, $err) = Amanda::Policy->new(policy => $policy_name);

to create a named policy (a name provided by the user, either specifying a
policy directly or specifying a storage definition).

If there is a problem creating the new object, then the resulting policy is
undef, and err is set to an error message.

Thus the usual recipe for creating a new policy is

  my ($policy, $err) = Amanda::Policy->new(policy => $policy_name);
  if (!$policy) {
    die("Error creating policy: $err");
  }

=head2 MEMBER VARIABLES

Note that these variables are not set until after the subclass constructor is
finished.

=over 4

=item C<< $policy>{'policy_name'} >>

Gives the name of the policy.  This name will make sense to the user.
It should be used to describe the policy in messages to the user.

=cut

# this is a "virtual" constructor which instantiates objects of different
# classes based on its argument.  Subclasses should not try to chain up!
sub new {
    my $class = shift;
    $class eq 'Amanda::Policy'
	or die("Do not call the Amanda::Policy constructor from subclasses");
    my %params = @_;
    my $policy_name = $params{'policy'};

    my $self = undef;

    # Create a storage
    if (!$policy_name) {
	$self = {
	    policy_name   => "automatic",
	};
	$self->{'retention_tapes'} = getconf($CNF_TAPECYCLE) - 1;
	$self->{'retention_days'} = 0;
	$self->{'retention_recover'} = 0;
	$self->{'retention_full'} = 0;
    } else {
	my $po = Amanda::Config::lookup_policy($policy_name);
	if (!$po) {
	    return (undef, "Policy '$policy_name' not found");
	}
	$self = {
	    policy_name   => $policy_name,
	};
	$self->{'retention_tapes'} = policy_getconf($po, $POLICY_RETENTION_TAPES)
				if policy_seen($po, $POLICY_RETENTION_TAPES);
	$self->{'retention_days'} = policy_getconf($po, $POLICY_RETENTION_DAYS)
				if policy_seen($po, $POLICY_RETENTION_DAYS);
	$self->{'retention_recover'} = policy_getconf($po, $POLICY_RETENTION_RECOVER)
				if policy_seen($po, $POLICY_RETENTION_RECOVER);
	$self->{'retention_full'} = policy_getconf($po, $POLICY_RETENTION_FULL)
				if policy_seen($po, $POLICY_RETENTION_FULL);
	$self->{'retention_tapes'} = getconf($CNF_TAPECYCLE) - 1
				if !defined $self->{'retention_tapes'};
	$self->{'retention_days'} = 0
				if !defined $self->{'retention_days'};
	$self->{'retention_recover'} = 0
				if !defined $self->{'retention_recover'};
	$self->{'retention_full'} = 0
				if !defined $self->{'retention_full'};
    }
    bless $self, $class;
    return $self;

}

sub quit {
    my $self = shift;

    delete $self->{'policy_name'};
    delete $self->{'retention_tapes'};
    delete $self->{'retention_days'};
    delete $self->{'retention_recover'};
    delete $self->{'retention_full'};
}

1;
