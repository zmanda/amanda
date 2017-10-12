#!@PERL@
# This copyright apply to all codes written by authors that made contribution
# made under the BSD license.  Read the AUTHORS file to know which authors made
# contribution made under the BSD license.
#
# The 3-Clause BSD License

# Copyright 2017 Purdue University
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

use lib '@amperldir@';
use strict;
use warnings;

package Amanda::Script::AmLibvirtFSFreeze;

use base 'Amanda::Script::Abstract';

use File::Spec;
use IO::File;

sub new {
    my ( $class, $execute_where, $refopthash ) = @_;
    my $self = $class->SUPER::new($execute_where, $refopthash);

    return $self;
}

sub check_properties {
    my ( $self ) = @_;

    $self->{'virshexecutable'} = $self->{'options'}->{'virshexecutable'};

    $self->{'freezeorthaw'} = $self->{'options'}->{'freezeorthaw'};
    if ( !defined $self->{'freezeorthaw'}
      or $self->{'freezeorthaw'} !~ /^(?:freeze|thaw)$/ ) {
        die Amanda::Script::InvocationError->transitionalError(
	    item => 'property', value => 'freezeorthaw',
	    problem => 'must be freeze or thaw');
    }

    $self->{'domain'} = $self->{'options'}->{'domain'};
    if ( !defined $self->{'domain'} ) {
        die Amanda::Script::InvocationError->transitionalError(
	    item => 'property', value => 'domain', problem => 'missing');
    }

    $self->{'mountpoint'} = $self->{'options'}->{'mountpoint'};
}

sub declare_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_options($refopthash, $refoptspecs);
    push @$refoptspecs, (
       'virshexecutable=s',
       'freezeorthaw=s',
       'domain=s',
       'mountpoint=s@'
       );
    # properties that have defaults and are not mandatory to receive with the
    # request can be initialized here as an alternative to checking for !defined
    # and applying the defaults in check_properties().
    $class->store_option($refopthash, 'virshexecutable', 'virsh');
    $class->store_option($refopthash,      'mountpoint', []);
}

sub command_pre_dle_estimate {
    my ( $self ) = @_;

    my $domain = $self->{'domain'};
    my @args;

    if ( 'freeze' eq $self->{'freezeorthaw'} ) {
        my @mountpoints = @{$self->{'mountpoint'}};
        @args = ( 'virsh', 'domfsfreeze', $domain, @mountpoints );
    } else {
        @args = ( 'virsh', 'domfsthaw', $domain );
    }

    my $rslt = system {$self->{'virshexecutable'}} (@args);

    unless ( 0 == $rslt ) {
	die Amanda::Script::CalledProcessError->transitionalError(
	    cmd => \@args, returncode => $rslt);
    }
}

# In an ideal world, just run at PRE-DLE-ESTIMATE to make one snapshot,
# estimate from it, dump from it, then free it in POST-DLE-BACKUP. But on
# a host with little free space for a snapshot and possibly a long planning wait
# between the estimate and the dump, it may be better to support being called
# at PRE-DLE-BACKUP also, in case a second snapshot is to be made then.

sub command_pre_dle_backup {
    my ( $self ) = @_;
    $self->command_pre_dle_estimate();
}

package main;

Amanda::Script::AmLibvirtFSFreeze->run();
