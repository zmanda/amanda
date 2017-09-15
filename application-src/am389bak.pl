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

package Amanda::Script::Am389Bak;

use base 'Amanda::Script::Abstract';

use File::Spec;
use File::Path qw(make_path remove_tree);
use IO::File;

sub new {
    my ( $class, $execute_where, $refopthash ) = @_;
    my $self = $class->SUPER::new($execute_where, $refopthash);

    $self->{'db2bakexecutable'} = $self->{'options'}->{'db2bakexecutable'};

    $self->{'instance'} = $self->{'options'}->{'instance'};
    if ( !defined $self->{'instance'} ) {
        $self->print_to_server_and_die(
	    'script requires instance property');
    }

    return $self;
}

sub declare_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'db2bakexecutable=s', 'instance=s' );

    $class->store_option($refopthash, 'db2bakexecutable', 'db2bak');
}

sub command_pre_dle_estimate {
    my ( $self ) = @_;

    my $repo = $self->{'instance'};
    my $dst = $self->{'options'}->{'device'};

    make_path($dst);
    remove_tree($dst, {keep_root => 1});

    my $rslt = system {$self->{'db2bakexecutable'}} (
        'db2bak', $dst, '-qZ', $repo );
}

package main;

Amanda::Script::Am389Bak->run();
