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

package Amanda::Application::Amooraw;

use base 'Amanda::Application::Abstract';

use Data::Dumper;
use File::Spec;
use File::Path qw(make_path);

sub supports_message_line { my ( $class ) = @_; return 1; }
sub supports_index_line { my ( $class ) = @_; return 1; }
sub supports_client_estimate { my ( $class ) = @_; return 1; }

sub declare_restore_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_restore_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'filename=s' );
}

sub inner_estimate {
    my ( $self, $level ) = @_;
    my $fn = $self->{'options'}->{'device'};
    return $self->int2big(-s $fn);
}

sub inner_backup {
    my ( $self, $fdout ) = @_;
    my $fn = $self->{'options'}->{'device'};
    my $fdin = POSIX::open($fn, &POSIX::O_RDONLY);

    if (!defined $fdin) {
	$self->print_to_server_and_die("Can't open '$fn': $!",
				       $Amanda::Script_App::ERROR);
    }

    my $size = $self->shovel($fdin, $fdout);

    POSIX::close($fdin);
    $self->emit_index_entry('/');

    return $size;
}

sub inner_restore {
    my $self = shift;
    my $fdin = shift;
    my $dsf = shift;

    if ( 1 != scalar(@_) or $_[0] ne '.' ) {
        $self->print_to_server_and_die(
	    "Only a single restore target (.) supported",
	    $Amanda::Script_App::ERROR);
    }

    my $fn = $self->{'options'}->{'filename'};
    $fn = 'amooraw-restored' if !defined $fn;

    if ( File::Spec->file_name_is_absolute($fn) ) {
        $fn = File::Spec->abs2rel($fn, File::Spec->rootdir());
    }

    my ( $volume, $directories, $file ) = File::Spec->splitpath($fn);
    make_path(File::Spec->catpath($volume, $directories, ''));

    my $fdout = POSIX::open($fn, &POSIX::O_CREAT | &POSIX::O_RDWR, 0600);
    if (!defined $fdout) {
	$self->print_to_server_and_die("Can't open '$fn': $!",
				       $Amanda::Script_App::ERROR);
    }

    $self->shovel($fdin, $fdout);
    POSIX::close($fdout);
    POSIX::close($fdin);
}

package main;

Amanda::Application::Amooraw->run();
