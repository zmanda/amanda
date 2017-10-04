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

package Amanda::Application::AmGrowingZip;

use base 'Amanda::Application::Abstract';

use Data::Dumper;
use Fcntl qw(:flock);
use File::Spec;
use File::Path qw(make_path);
use IO::File;

my $usable;
eval {
    require Archive::Zip;
    $usable = 1;
} or do {
    $usable = 0;
};

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
        $self->read_local_state(
		['level=i', 'length=s', 'centraldiroffset=s']);
    return $self;
}

sub declare_common_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'flock=s' );
    $refopthash->{'flock'} = $class->boolean_property_setter($refopthash);
}

sub declare_restore_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_restore_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'filename=s' );
}

sub command_selfcheck {
    my ( $self ) = @_;
    $self->check($usable, 'Archive::Zip is not installed');
    $self->SUPER::command_selfcheck();
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
    my $lowerlength = Math::BigInt->new($lowerstate->{'length'});
    my $lowercdo = Math::BigInt->new($lowerstate->{'centraldiroffset'});
    return Math::BigInt->bzero() if 0 == $sz->bcmp($lowerlength);
    return $sz->bsub($lowercdo);
}

sub inner_backup {
    # XXX assert level==0 if no --record
    my ( $self, $fdout ) = @_;
    my $fn = $self->{'options'}->{'device'};
    my $level = $self->{'options'}->{'level'};
    my $flock = $self->{'options'}->{'flock'};
    my $fdin = POSIX::open($fn, &POSIX::O_RDONLY);

    if (!defined $fdin) {
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'target', value => $fn, errno => $!);
    }

    my $ioh = IO::File->new();
    $ioh->fdopen($fdin, 'r');
    my $az = Archive::Zip->new();

    if ( $flock and not flock($ioh, LOCK_SH) ) {
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'target', value => $fn, problem => 'lock', errno => $!);
    }
    $az->readFromFileHandle($ioh);
    my $cdo =$self->int2big($az->centralDirectoryOffsetWRTStartingDiskNumber());

    my $start;
    my $currentlength = $self->int2big(-s $ioh);
    if ( 0 == $level ) {
        $start = Math::BigInt->bzero();
    } else {
        # ENHANCEMENT? could save a prior size and digest, and verify here
        my $lowerstate = $self->{'localstate'}->{$level - 1};
	# XXX don't be stupid if lowerstate isn't there
        my $lowerlength = Math::BigInt->new($lowerstate->{'length'});
        my $lowercdo = Math::BigInt->new($lowerstate->{'centraldiroffset'});
	if ( 0 == $currentlength->bcmp($lowerlength) ) {
	    # Length is unchanged -> nothing has changed (the zip file is
	    # assumed never to change except by appending).
	    $start = $currentlength; # In other words, dump nothing.
	}
	else {
	    $start = $lowercdo; # Dump from lowercdo to current length.
	}

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

    my $istart = $self->big2int($start);

    die Amanda::Application::EnvironmentError->transitionalError(
	item => 'target', value => $fn, problem => 'seek', errno => $!)
	unless defined POSIX::lseek($fdin, $istart, &POSIX::SEEK_SET);

    my $size = $self->shovel($fdin, $fdout);
    if ( $flock and not flock($ioh, LOCK_UN) ) {
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'target', value => $fn, problem => 'unlock', errno => $!);
    }

    die Amanda::Application::EnvironmentError->transitionalError(
	item => 'target', value => $fn, problem => 'close', errno => $!)
	unless defined POSIX::close($fdin);

    $self->emit_index_entry('/');

    if ( $self->{'options'}->{'record'} ) {
        $self->update_local_state($self->{'localstate'}, $level, {
            'length' => $currentlength->bstr(),
            'centraldiroffset' => $cdo->bstr() });
    }

    return $size;
}

sub inner_restore {
    my $self = shift;
    my $fdin = shift;
    my $dsf = shift;
    my $level = $self->{'options'}->{'level'};
    #
    # There is no point honoring flock during restore. It would be madness
    # to try to restore a sequence of increments while writes from other
    # sources could intervene. Restoration must always be done into a secluded
    # directory/path, only moving the fully restored file back to where other
    # processes may write it.

    if ( 1 != scalar(@_) or $_[0] ne '.' ) {
        die Amanda::Application::InvocationError->transitionalError(
	    item => 'restore targets',
	    problem => 'Only one (.) supported');
    }

    my $fn = $self->{'options'}->{'filename'};
    $fn = 'amgrowingzip-restored' if !defined $fn;

    if ( File::Spec->file_name_is_absolute($fn) ) {
        $fn = File::Spec->abs2rel($fn, File::Spec->rootdir());
    }

    my ( $volume, $directories, $file ) = File::Spec->splitpath($fn);
    make_path(File::Spec->catpath($volume, $directories, ''));

    # Where to begin writing if applying a level > 0 (incremental) dump? In a
    # cautious world, save the starting offset at dump time, and verify at
    # restore time that that's where the central directory starts. That could
    # require either saving the offset in the dump stream somehow, or using the
    # server-side state file that appears in 3.3.8 (which still might not be
    # available in a restoration from only the tape). Short of that, just
    # blindly open in rdwr mode, seek to its central directory offset, and write
    # the increment. After all, does an incremental tar restore actually verify
    # the current directory tree is exactly as the prior dump level left it? No,
    # it just relies on restoration being done carefully and in sequence.
    my $oflags = &POSIX::O_RDWR;
    if ( $level == 0 ) {
        $oflags |= ( &POSIX::O_CREAT | &POSIX::O_TRUNC );
    }

    my $fdout = POSIX::open($fn, $oflags, 0600);
    if (!defined $fdout) {
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'target', value => $fn, errno => $!);
    }

    my $ioh = IO::File->new(); # don't let out of scope before shovel()
    if ( $level > 0 ) {
        $ioh->fdopen($fdout, 'r');
        my $az = Archive::Zip->new();
        $az->readFromFileHandle($ioh);
        my $cdo =
	    $self->int2big($az->centralDirectoryOffsetWRTStartingDiskNumber());
	my $ioff = $self->big2int($cdo);
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'target', value => $fn, problem => 'seek', errno => $!)
	    unless defined POSIX::lseek($fdout, $ioff, &POSIX::SEEK_SET);
	# We are now positioned at the beginning of the "central" directory
	# found at the end of the zip file, and the file is open for RDWR
	# without TRUNC. If the increment was dumped when more content had been
	# appended to the zip, there will be a stream to write here that is
	# strictly longer than the old directory, so no need to truncate. If
	# the increment was dumped when nothing had changed, there is a zero
	# length stream to shovel here, leaving the file untruncated and intact.
	# (Amanda seems to discard increments that dump zero bytes, anyway, so
	# this case normally should not even arise.)
    }

    $self->shovel($fdin, $fdout);
    die Amanda::Application::EnvironmentError->transitionalError(
	item => 'target', value => $fn, problem => 'close', errno => $!)
	unless defined POSIX::close($fdout);
    POSIX::close($fdin);
}

package main;

Amanda::Application::AmGrowingZip->run();
