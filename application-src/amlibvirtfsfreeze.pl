#!@PERL@
# This copyright apply to all codes written by authors that made contribution
# made under the BSD license.  Read the AUTHORS file to know which authors made
# contribution made under the BSD license.
#
# The 3-Clause BSD License

# Copyright 2017-2020 Purdue University
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
      or $self->{'freezeorthaw'} !~ /^(?:freeze|thaw|trim)$/ ) {
        die Amanda::Script::InvocationError->transitionalError(
	    item => 'property', value => 'freezeorthaw',
	    problem => 'must be freeze or thaw (or trim)');
    }

    $self->{'domain'} = $self->{'options'}->{'domain'};
    if ( !defined $self->{'domain'} ) {
        die Amanda::Script::InvocationError->transitionalError(
	    item => 'property', value => 'domain', problem => 'missing');
    }

    $self->{'mountpoint'} = $self->{'options'}->{'mountpoint'};

    if ( 'trim' eq $self->{'freezeorthaw'} ) {
	my @mountpoints = @{$self->{'mountpoint'}};
	die Amanda::Script::InvocationError->transitionalError(
	    item => 'property', value => 'mountpoint',
	    problem => 'trim cannot accept more than one')
	if 1 < scalar(@mountpoints);
    }
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
    } elsif ( 'trim' eq $self->{'freezeorthaw'} ) {
	my @mountpoints = @{$self->{'mountpoint'}};
	unshift @mountpoints, '--mountpoint' if 0 < scalar(@mountpoints);
	@args = ( 'virsh', 'domfstrim', $domain, @mountpoints );
    } else {
        @args = ( 'virsh', 'domfsthaw', $domain );
    }

    unless ( $self->domain_is_running() ) {
        warn Amanda::Script::Message->transitionalGood(
	    message=>"$domain not running, $self->{'freezeorthaw'} skipped\n");
	return;
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

sub domain_is_running {
    # Perl's 'system' is adequate for most of the work of this script, where
    # only a command's exit status is needed, but domain_is_running needs to
    # execute a command, capture its output, and compare to what's expected, and
    # has to do that while possibly invoking a wrapper program with an argv[0]
    # reflecting the real target, which is something Perl's exec and system can
    # do but other 'simple' approaches like open/open2/open3 can't. Hence this
    # simple task quickly gets grotty. Cribbed from the similar get_data_percent
    # in amlvmsnapshot.
    #
    # Having to write subprocess manipulations at this low level is kind of
    # a drag on easy script development, and think it would be preferable for
    # Amanda to provide common facilities for them, or bless a widely-known
    # existing module like IPC::Run and consider it an expected dependency
    # so it could be used without hesitation.
    my ( $self ) = @_;
    my @args = ( 'virsh', 'domstate', $self->{'domain'} );

    unless ( defined (my $pid = open my $pipefh, '-|') ) {
	die Amanda::Script::EnvironmentError->transitionalError(
	    item => 'spawning', value => 'virsh domstate', errno => $!);
    } else {
	unless ( $pid ) {
	    unless ( exec {$self->{'virshexecutable'}} @args ) {
		print STDERR $!."\n";
		exit 1;
	    }
	}
	my $got = <$pipefh>;
	unless ( close $pipefh ) {
	    die Amanda::Script::CalledProcessError->transitionalError(
		cmd => @args, returncode => $?)
		if 0 == $!;
	    die Amanda::Script::EnvironmentError->transitionalError(
		item => 'spawning', value => 'virsh domstate', errno => $!);
	}
	return "running\n" eq $got;
    }
}

package main;

Amanda::Script::AmLibvirtFSFreeze->run();
