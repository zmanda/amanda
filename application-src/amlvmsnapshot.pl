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

package Amanda::Script::AmLvmSnapshot;

use base 'Amanda::Script::Abstract';

use File::Spec;
use File::Path qw(make_path remove_tree);
use IO::File;

sub new {
    my ( $class, $execute_where, $refopthash ) = @_;
    my $self = $class->SUPER::new($execute_where, $refopthash);

    $self->{'lvmexecutable'} = $self->{'options'}->{'lvmexecutable'};
    $self->{'mountexecutable'} = $self->{'options'}->{'mountexecutable'};
    $self->{'umountexecutable'} = $self->{'options'}->{'umountexecutable'};

    $self->{'volumegroup'} = $self->{'options'}->{'volumegroup'};
    if ( !defined $self->{'volumegroup'} ) {
        $self->print_to_server_and_die(
	    'script requires volumegroup property');
    }

    $self->{'logicalvolume'} = $self->{'options'}->{'logicalvolume'};
    if ( !defined $self->{'logicalvolume'} ) {
        $self->print_to_server_and_die(
	    'script requires logicalvolume property');
    }

    $self->{'snapshotname'} = $self->{'options'}->{'snapshotname'};
    if ( !defined $self->{'snapshotname'} ) {
        $self->print_to_server_and_die(
	    'script requires snapshotname property');
    }

    $self->{'extents'} = $self->{'options'}->{'extents'};
    if ( !defined $self->{'extents'} ) {
        $self->print_to_server_and_die(
	    'script requires extents property');
    }

    $self->{'mountopts'} = $self->{'options'}->{'mountopts'};

    return $self;
}

sub declare_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_options($refopthash, $refoptspecs);
    push @$refoptspecs, (
       'lvmexecutable=s',
       'mountexecutable=s',
       'umountexecutable=s',
       'volumegroup=s',
       'logicalvolume=s',
       'snapshotname=s',
       'extents=s',
       'mountopts=s@'
       );
    # properties that have defaults and are not mandatory to receive with the
    # request can be initialized here as an alternative to checking for !defined
    # and applying the defaults in new().
    $refopthash->{   'lvmexecutable'} = 'lvm';
    $refopthash->{ 'mountexecutable'} = 'mount';
    $refopthash->{'umountexecutable'} = 'umount';
    $refopthash->{       'mountopts'} = [];
}

sub command_pre_dle_estimate {
    my ( $self ) = @_;

    my $vg = $self->{'volumegroup'};
    my $lv = $self->{'logicalvolume'};
    my $sn = $self->{'snapshotname'};
    my $extents = $self->{'extents'};

    my $dst = $self->{'options'}->{'device'};

    make_path($dst);

    # NOTE: lvm mumbles a couple lines on stdout, where they'll be mistaken
    # for script output, eventually confusing the Amanda reader of the stream,
    # which will think the sendsize timed out. Simply redirecting stdout to
    # stderr for the lvm invocation fixes that. For now, instead of being done
    # here in Perl, it's being done in the setuid wrapper executable supplied
    # as 'lvmexecutable'. For clarity and generality it would be nicer here.
    # Even nicer would be to handle it all transparently in the superclass.

    my $rslt = system {$self->{'lvmexecutable'}} (
        'lvm', 'lvcreate',
	'--snapshot', $vg.'/'.$lv,
	'--extents', $extents,
	'--name', $sn,
	'--permission', 'r'
    );

    if ( 0 != $rslt ) {
        $self->print_to_server_and_die(
	    'lvmcreate returned nonzero status '.$rslt);
    }

    $rslt = system {$self->{'mountexecutable'}} (
        'mount',
	'-o', join(',', ('ro', @{$self->{'mountopts'}})),
	'/dev/disk/by-id/dm-name-'.$vg.'-'.$sn,
	$dst
    );
}

sub command_post_dle_backup {
    my ( $self ) = @_;

    my $vg = $self->{'volumegroup'};
    my $sn = $self->{'snapshotname'};

    my $dst = $self->{'options'}->{'device'};

    my $rslt = system {$self->{'umountexecutable'}} (
        'umount', $dst
    );

    if ( 0 != $rslt ) {
        $self->print_to_server_and_die(
	    'umount returned nonzero status '.$rslt);
    }

    # TODO: before destroying, run lvs and capture Data% value - see how
    # close we came to using up allocated --extents capacity in the time it
    # took to do the backup.
    $rslt = system {$self->{'lvmexecutable'}} (
        'lvm',
	'lvremove', '--force', $vg.'/'.$sn
    );
}

# In an ideal world, just run at PRE-DLE-ESTIMATE to make one snapshot,
# estimate from it, dump from it, then free it in POST-DLE-BACKUP. But on
# a host with little free space for a snapshot and possibly a long planning wait
# between the estimate and the dump, it may be better to support being called
# at POST-DLE-ESTIMATE (to free one snapshot, same as POST-DLE-BACKUP) and
# at PRE-DLE-BACKUP (to make another, same as PRE-DLE-ESTIMATE).

sub command_post_dle_estimate {
    my ( $self ) = @_;
    $self->command_post_dle_backup();
}

sub command_pre_dle_backup {
    my ( $self ) = @_;
    $self->command_pre_dle_estimate();
}

package main;

Amanda::Script::AmLvmSnapshot->run();
