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

package Amanda::Application::AmOpaqueTree::DirWrap;
#
# A tiny class that wraps a directory name as an object with a dirname()
# method that returns it ... so it can be treated the same way as a result
# from File::Temp->newdir(), which behaves that way.
#

sub new {
    my ( $class, $dirname ) = @_;
    my $self = { 'dn' => $dirname };
    return bless($self, $class);
}

sub dirname {
    my ( $self ) = @_;
    return $self->{'dn'};
}

package Amanda::Application::AmOpaqueTree;
#
# The main attraction.
#

use base 'Amanda::Application::Abstract';

use Data::Dumper;
use Fcntl;
use File::Spec;
use File::Temp;
use File::Path qw(make_path remove_tree);
use IO::File;
use IPC::Open3;

sub supports_host { my ( $class ) = @_; return 1; }
sub supports_disk { my ( $class ) = @_; return 1; }
sub supports_index_line { my ( $class ) = @_; return 1; }
sub supports_message_line { my ( $class ) = @_; return 1; }
sub supports_record { my ( $class ) = @_; return 1; }
sub supports_client_estimate { my ( $class ) = @_; return 1; }
sub supports_multi_estimate { my ( $class ) = @_; return 1; }

sub max_level { my ( $class ) = @_; return 'DEFAULT'; }

sub rsync_is_unusable {
    my ( $self ) = @_;
    my ( $wtr, $rdr );
    my $pid = eval {
        open3($wtr, $rdr, undef, $self->{'rsyncexecutable'}, '--version');
    };
    return $@ if $@;
    close $wtr;
    my $output = do { local $/; <$rdr> };
    close $rdr;
    waitpid $pid, 0;
    return $output if $?;
    unless ( $output =~ qr/(?:^\s|,\s)hardlinks(?:,\s|$)/m ) {
        return $self->{'rsyncexecutable'} . ' lacks hardlink support.';
    }
    unless ( $output =~ qr/(?:^\s|,\s)batchfiles(?:,\s|$)/m ) {
        return $self->{'rsyncexecutable'} . ' lacks batchfile support.';
    }
    return 0; # hooray, it isn't unusable.
}

sub new {
    my ( $class, $refopthash ) = @_;
    my $self = $class->SUPER::new($refopthash);

    $self->{'rsyncexecutable'} = $self->{'options'}->{'rsyncexecutable'};

    $self->{'localstatedir'} = $self->{'options'}->{'localstatedir'};

    $self->{'rsyncstatesdir'} = $self->{'options'}->{'rsyncstatesdir'};
    # This default is computed here, based on the final value of localstatedir,
    # so it can't just be stored in declare_common_options as a fixed default.
    if ( !defined $self->{'rsyncstatesdir'} ) {
        my ( $dirpart, $filepart ) = $self->local_state_path();
	$self->{'rsyncstatesdir'} =
	    File::Spec->catdir($dirpart, 'rsyncstates');
    }

    $self->{'rsynctempbatchdir'} = $self->{'options'}->{'rsynctempbatchdir'};

    $self->{'localstate'} =
        $self->read_local_state(['level=i', 'rsyncstate=s']);

    return $self;
}

sub declare_common_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs,
        ( 'rsyncexecutable=s', 'rsyncstatesdir=s', 'rsynctempbatchdir=s',
	  'localstatedir=s' );
    $class->store_option($refopthash, 'rsyncexecutable', 'rsync');
}

sub local_state_path {
    my ( $self ) = @_;
    if ( defined $self->{'localstatedir'} ) {
        return $self->build_state_path($self->{'localstatedir'});
    }
    return $self->SUPER::local_state_path();
}

sub generate_rsync_batch {
    my ( $self, $basedOn, $yielding ) = @_;
    my $batch = File::Temp->new(
        DIR => $self->{'rsynctempbatchdir'},
	EXLOCK => 0
    );

    my $rslt = system {$self->{'rsyncexecutable'}} (
        'rsync',
	'-rlpt', '--checksum', '--delete-during', '--compress', '--sparse',
	'--only-write-batch', $batch->filename,
	File::Spec->catfile($yielding, ''),
	File::Spec->catfile($basedOn, '')
    );

    unlink($batch->filename . '.sh');

    return $batch;
}

sub empty_rsync_state_dir {
    my ( $self, $transient ) = @_;
    if ( ! -d $self->{'rsyncstatesdir'} ) {
        make_path($self->{'rsyncstatesdir'});
    }
    return File::Temp->newdir(
        DIR => $self->{'rsyncstatesdir'}, CLEANUP => $transient);
}

sub best_link_dest {
    my ( $self, $level ) = @_;

    my $prior = ( $level > 0 ) ? $level : $self->{'localstate'}->{'maxlevel'};
    $prior -= 1;
    my $linkdest;
    if ( exists $self->{'localstate'}->{$prior} ) {
        $linkdest = $self->{'localstate'}->{$prior}->{'rsyncstate'};
    }
    if ( defined $linkdest ) {
        $linkdest = Amanda::Application::AmOpaqueTree::DirWrap->new($linkdest);
    } else {
        $linkdest = $self->empty_rsync_state_dir(1);
    }
    return $linkdest;
}

sub capture_rsync_state {
    my ( $self, $srcdir, $dstdir, $linkdestdir ) = @_;

    my $rslt = system {$self->{'rsyncexecutable'}} (
        'rsync',
	'-rlp', '--whole-file', '--checksum', '--copy-dirlinks', '--sparse',
	'--link-dest', $linkdestdir,
	File::Spec->catfile($srcdir, ''),
	File::Spec->catfile($dstdir, '')
    );
}

sub rsync_ref_for_level {
    my ( $self, $level ) = @_;
    my $ref;
    if ( 0 == $level ) {
        $ref = $self->empty_rsync_state_dir(1);
    } else {
        $ref = $self->{'localstate'}->{$level - 1}->{'rsyncstate'};
	$ref = Amanda::Application::AmOpaqueTree::DirWrap->new($ref);
    }

    return $ref;
}

sub inner_estimate {
    my ( $self, $level ) = @_;
    my $mxl = $self->{'localstate'}->{'maxlevel'};
    if ( $level > $mxl ) {
        $self->print_to_server("Requested estimate level $level > $mxl",
			       $Amanda::Script_App::ERROR);

        return Math::BigInt->bone('-');
    }

    my $dn = $self->{'options'}->{'device'};
    my $ref = $self->rsync_ref_for_level($level);
    my $batch = $self->generate_rsync_batch($ref->dirname(), $dn);
    $batch->seek(0, &Fcntl::SEEK_END);
    my $sz = Math::BigInt->new($batch->tell()); # XXX precision issues may lurk
    return $sz;
    # $batch is removed once out of scope
}

sub inner_backup {
    # XXX assert level==0 if no --record
    my ( $self, $fdout ) = @_;
    my $dn = $self->{'options'}->{'device'};
    my $level = $self->{'options'}->{'level'};

    my $dst; # only used in --record case, but needed again further below
    if ( $self->{'options'}->{'record'} ) {
        my $bld = $self->best_link_dest($level);
	$dst = $self->empty_rsync_state_dir(0)->dirname();
	$self->{'newrsyncstate'} = $dst; # save in case repair needed
	$self->capture_rsync_state($dn, $dst, $bld->dirname());
	$dn = $dst;
    }

    my $ref = $self->rsync_ref_for_level($level);
    my $batch = $self->generate_rsync_batch($ref->dirname(), $dn);
    $batch->seek(0, &Fcntl::SEEK_SET);
    my $fdin = fileno($batch);
    POSIX::lseek($fdin, 0, &POSIX::SEEK_SET); # probably not needed, but...
    my $size = $self->shovel($fdin, $fdout);

    $self->emit_index_entry('/');

    if ( $self->{'options'}->{'record'} ) {
        $self->update_local_state($self->{'localstate'}, $level, {
            'rsyncstate' => $dst });
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

    my $dn = File::Spec->curdir();

    if ( 0 == $level ) {
	remove_tree($dn, {keep_root => 1});
    }

    my $rslt = system {$self->{'rsyncexecutable'}} (
        'rsync',
	'-rlpt', '--checksum', '--delete-during', '--compress', '--sparse',
	'--read-batch', '-', File::Spec->catfile($dn, '')
    );

    POSIX::close($fdin);
}

sub update_local_state {
    my ( $self, $state, $level, $opthash ) = @_;
    $self->{'orphanedrsyncstates'} = [];
    for ( my ($l, $oh); ($l, $oh) = each %$state; ) {
        next if 'maxlevel' eq $l or (0 + $l) lt $level;
	push @{$self->{'orphanedrsyncstates'}}, $oh->{'rsyncstate'};
    }
    $self->SUPER::update_local_state($state, $level, $opthash);
}

sub write_local_state {
    my ( $self, $levhash ) = @_;
    $self->SUPER::write_local_state($levhash);
    for my $ors ( @{$self->{'orphanedrsyncstates'}} ) {
        remove_tree($ors);
    }
}

# Should write_local_state not be called, the (possibly large) newrsyncstate
# directory would be leaked: not referred to by any saved state, so never
# reclaimed in normal operation. So, remove it here.
sub repair_local_state {
    my ( $self ) = @_;
    remove_tree($self->{'newrsyncstate'});
    $self->SUPER::repair_local_state();
}

sub command_selfcheck {
    my ( $self ) = @_;
    my $why = $self->rsync_is_unusable();
    if ( $why ) {
        $self->print_to_server($why, $Amanda::Script_App::ERROR);
    } else {
        $self->SUPER::command_selfcheck();
    }
}

package main;

Amanda::Application::AmOpaqueTree->run();
