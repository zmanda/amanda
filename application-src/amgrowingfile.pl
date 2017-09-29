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

package Amanda::Application::AmGrowingFile;

use base 'Amanda::Application::Abstract';

use Data::Dumper;
use File::Spec;
use File::Path qw(make_path);

sub supports_host { my ( $class ) = @_; return 1; }
sub supports_disk { my ( $class ) = @_; return 1; }
sub supports_index_line { my ( $class ) = @_; return 1; }
sub supports_message_line { my ( $class ) = @_; return 1; }
sub supports_record { my ( $class ) = @_; return 1; }
sub supports_client_estimate { my ( $class ) = @_; return 1; }
sub supports_multi_estimate { my ( $class ) = @_; return 1; }

sub max_level { my ( $class ) = @_; return 'DEFAULT'; }

sub new {
    my ( $class, $refopthash ) = @_;
    my $self = $class->SUPER::new($refopthash);
    $self->{'localstate'} =
        $self->read_local_state(['level=i', 'byteoffset=s', 'bytes=s']);
    return $self;
}

sub declare_restore_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_restore_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'filename=s' );
}

sub inner_estimate {
    my ( $self, $level ) = @_;
    my $fn = $self->{'options'}->{'device'};
    my $sz = $self->int2big(-s $fn);
    return $sz if 0 == $level;

    my $mxl = $self->{'localstate'}->{'maxlevel'};
    if ( $level > $mxl ) {
        $self->print_to_server("Requested estimate level $level > $mxl",
			       $Amanda::Script_App::ERROR);

        return Math::BigInt->bone('-');
    }
    my $lowerstate = $self->{'localstate'}->{$level - 1};
    my $loweroffset = Math::BigInt->new($lowerstate->{'byteoffset'});
    my $lowersize = Math::BigInt->new($lowerstate->{'bytes'});
    return $sz->bsub($loweroffset)->bsub($lowersize);
}

sub inner_backup {
    # XXX assert level==0 if no --record
    my ( $self, $fdout ) = @_;
    my $fn = $self->{'options'}->{'device'};
    my $level = $self->{'options'}->{'level'};
    my $fdin = POSIX::open($fn, &POSIX::O_RDONLY);

    if (!defined $fdin) {
	$self->print_to_server_and_die("Can't open '$fn': $!",
				       $Amanda::Script_App::ERROR);
    }

    my $start;
    if ( 0 == $level ) {
        $start = Math::BigInt->bzero();
    } else {
        # XXX verify prior size and digest here
        my $lowerstate = $self->{'localstate'}->{$level - 1};
	# XXX don't be stupid if lowerstate isn't there
        my $loweroffset = Math::BigInt->new($lowerstate->{'byteoffset'});
        my $lowersize = Math::BigInt->new($lowerstate->{'bytes'});
	$start = $loweroffset->copy()->badd($lowersize);
	my $istart = $self->big2int($start);
	POSIX::lseek($fdin, $istart, &POSIX::SEEK_SET);

	# sendbackup: HEADER, documented in the Application API/Operations wiki
	# page, wasn't ever implemented, according to Jean-Louis. An option that
	# becomes available in 3.3.8 is 'sendbackup: state' and retrieved with
	# --recover-dump-state-file. The state file is kept on the server, not
	# clear how /it/ is backed up. May not be available if recovering only
	# from the tape.
	#
	# print {$self->{mesgout}} "sendbackup: HEADER startoffset=$start\n";
	#
	# Without doing this, can just /assume/ that the file is currently as
	# the lower-level restore left it, and append to the end. Not ideal,
	# but other incremental strategies also perform restores without
	# rigorous verification of the state they are restoring onto, so when
	# in Rome....
    }
    my $size = $self->shovel($fdin, $fdout);

    POSIX::close($fdin);
    $self->emit_index_entry('/');

    if ( $self->{'options'}->{'record'} ) {
        $self->update_local_state($self->{'localstate'}, $level, {
            'byteoffset' => $start->bstr(), 'bytes' => $size->bstr() });
    }

    return $size;
}

sub inner_restore {
    my $self = shift;
    my $fdin = shift;
    my $dsf = shift;
    my $level = $self->{'options'}->{'level'};

    if ( 1 != scalar(@_) or $_[0] ne '.' ) {
        $self->print_to_server_and_die(
	    "Only a single restore target (.) supported",
	    $Amanda::Script_App::ERROR);
    }

    my $fn = $self->{'options'}->{'filename'};
    $fn = 'amgrowingfile-restored' if !defined $fn;

    if ( File::Spec->file_name_is_absolute($fn) ) {
        $fn = File::Spec->abs2rel($fn, File::Spec->rootdir());
    }

    my ( $volume, $directories, $file ) = File::Spec->splitpath($fn);
    make_path(File::Spec->catpath($volume, $directories, ''));

    # Where to begin writing if applying a level > 0 (incremental) dump?
    # In a cautious world, save the starting offset at dump time, and verify
    # at restore time that that's where the file ends. That could require either
    # saving the offset in the dump stream somehow, or using the server-side
    # state file that appears in 3.3.8 (which still might not be available in a
    # restoration from only the tape). Short of that, just blindly open in
    # append mode and write the increment. After all, does an incremental tar
    # restore actually verify the current directory tree is exactly as the prior
    # dump level left it? No, it just relies on restoration being done carefully
    # and in sequence.
    my $oflags = &POSIX::O_RDWR;
    if ( $level > 0 ) {
        $oflags |= &POSIX::O_APPEND;
    } else {
        $oflags |= ( &POSIX::O_CREAT | &POSIX::O_TRUNC );
    }

    my $fdout = POSIX::open($fn, $oflags, 0600);
    if (!defined $fdout) {
	$self->print_to_server_and_die("Can't open '$fn': $!",
				       $Amanda::Script_App::ERROR);
    }

    $self->shovel($fdin, $fdout);
    POSIX::close($fdout);
    POSIX::close($fdin);
}

package main;

Amanda::Application::AmGrowingFile->run();
