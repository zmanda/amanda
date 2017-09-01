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

package Amanda::Application::AmZfsHoldSend::Dataset;
# A class representing a single ZFS dataset (filesystem, volume, or snapshot).
# name      -- name (last component, relative to name of parent)
# fqname    -- fully-qualified name
# dstype    -- filesystem, volume, or snapshot
# parent    -- another Dataset instance (unless this is the exploration root)
# snapshots -- array of instances representing snapshots (where applicable)
# children  -- array of instances representing children (where applicable)
# nholds    -- for a snapshot, the number of holds recorded on it
# holds()   -- for a snapshot, array of tags placed on it with 'zfs hold'

my $rx_list = qr/
  ^
  (?P<fqname>[^\t]++)
  \t
  (?P<dstype>filesystem|snapshot|volume)
  \t
  (?P<nholds>-|\d++)
  $
/ox;

my $rx_holdtag;
my $zfsexecutable;

# The caller should construct a regular expression that matches the hold tags
# of interest (there may be other hold tags placed on snapshots for purposes
# nothing to do with Amanda). The regex should match a string beginning with
# the HOLDTAGPREFIX property and containing a matching group of digits -- the
# digit string represents an Amanda backup level made from the tagged snapshot.
# It should also allow (and not capture) arbitrary trailing whitespace.
sub set_rx_holdtag {
    my ( $class, $rx ) = @_;
    $rx_holdtag = $rx;
}

sub set_zfsexecutable {
    my ( $class, $zx ) = @_;
    $zfsexecutable = $zx;
}

# Construct a Dataset instance from a single line of output from the appropriate
# zfs list command. The caller should first allocate an empty map and pass it
# as $ns when constructing the first dataset encountered (the DEVICE specified
# for the DLE, the root of recursive exploration). The caller should continue
# passing the same $ns on subsequent constructor calls, and it will be used to
# resolve parent links. Once all of the zfs list output has been processed, $ns
# is no longer needed.
sub new {
    my ( $class, $listline, $ns ) = @_;

    my $self;
    if ( not my ( $fqname, $dstype, $nholds ) = $listline =~ m/$rx_list/ ) {
	die 'Amanda::Application::AmZfsHoldSend: strange zfs list line: '
	    + $listline;
    } else {
	$self = { fqname => $fqname, dstype => $dstype };

	my ( $parent, $name );

	if ( 'snapshot' eq $dstype ) {
	    $self->{'nholds'} = $nholds;
	    $self->{'holds'} = [] if 0 == $nholds;
	    ( $parent, $name ) = split('@', $fqname, 2);
	} else {
	    my @parts = $fqname =~ m,^(?:(.+)/)?([^/]+)$,o;
	    if ( 1 < scalar(@parts) ) {
		( $parent, $name ) = @parts;
	    } else {
		$name = $fqname;
	    }
	}

	$self->{'name'} = $name;
	$self->{'children'}  = [] if 'filesystem' eq $dstype;
	$self->{'snapshots'} = [] if   'snapshot' ne $dstype;
	$self->{'parent'} = $ns->{$parent} if $parent;

	bless $self, $class;

	if ( $self->{'parent'} ) {
	    if ( 'snapshot' eq $dstype ) {
		push @{$self->{'parent'}->{'snapshots'}}, $self;
	    } else {
		push @{$self->{'parent'}->{'children'}}, $self;
	    }
	}
	$ns->{$fqname} = $self;
    }
    return $self;
}

# Called on a snapshot, return array of holds on that snapshot.
# The only holds included are those that match the regular expression
# passed to set_rx_holdtag, which the caller must have done before first
# calling this. Only the tagarg (matching group) portion of each hold
# tag is stored in the array.
# This sub is lazy, and spawns the 'zfs holds' command to gather the
# information the first time it is called.
# The tagargs are assumed to be digit strings (representing Amanda backup
# levels), will have 0 added to numify them, and the constructed array will
# have them in ascending order (for simplicity later).
sub holds {
    my ( $self ) = @_;

    my $hs = $self->{'holds'};
    return $hs if defined $hs;

    die 'Amanda::Application::AmZfsHoldSend: holds() called on non-snapshot'
	if 'snapshot' ne $self->{'dstype'};
    
    $hs = [];

    open my $holdfh, '-|', $zfsexecutable, 'holds', '--', $self->{'fqname'};

    my $header = <$holdfh>;
    my $tagstart = index $header, 'TAG';
    my $timstart = index $header, 'TIMESTAMP', $tagstart;

    while ( my $line = <$holdfh> ) {
	my $tag = substr($line, $tagstart, $timstart - $tagstart);
	if ( my ($tagarg) = $tag =~ m/$rx_holdtag/o ) {
	    push @$hs, 0 + $tagarg;
	}
    }
    $holdfh->close();

    my @sorted = sort {$a <=> $b} @$hs;
    $hs = \@sorted;
    $self->{'holds'} = $hs;
    return $hs;
}

# Called on a filesystem or volume, returns (map, h) where map is a reference
# mapping backup levels to snapshots, and h is the highest level seen.
# Verifies that the recorded levels are consecutive integers starting at zero
# and that no two snapshots have been tagged with a given level.
# If this is a filesystem (can have children), generates the maps for any
# children and verifies they map the same levels to the same snapshot names.
sub levels_to_snapshots {
    my ( $self ) = @_;

    my $lowestlevel;
    my $latestlevel;
    my %levtosnap;

    for my $snap ( @{$self->{'snapshots'}} ) { # zfs lists these in chron. order
	for my $level ( @{$snap->holds()} ) {
	    unless ( defined $lowestlevel ) {
		$lowestlevel = $level;
	    } else {
		die 'Amanda::Application::AmZfsHoldSend: ' .
		    'nonmonotonic level tags: ' . $self->{'fqname'} .
		    ' level ' . $level
		    if $level < $lowestlevel;
	    }
	    unless ( defined $latestlevel ) {
		$latestlevel = $level;
	    } else {
		die 'Amanda::Application::AmZfsHoldSend: ' .
		    'nonconsecutive level tags: ' . $self->{'fqname'} .
		    ' level ' . $level
		    if $level != 1 + $latestlevel;
		$latestlevel = $level;
	    }
	    unless ( exists $levtosnap{$level} ) {
		$levtosnap{$level} = $snap;
	    } else {
		die 'Amanda::Application::AmZfsHoldSend: ' .
		    'nonunique level tags: ' . $self->{'fqname'} .
		    ' level ' . $level;
	    }
	}
    }

    # It is ok to have no tagged snapshots (no prior backup has been done).
    return ( \%levtosnap, $latestlevel ) if not %levtosnap;

    # It is not ok to have some, but no level 0.
    die 'Amanda::Application::AmZfsHoldSend: level 0 hold not found: ' .
	$self->{'fqname'} unless 0 == $lowestlevel;

    if ( 'filesystem' eq $self->{'dstype'} ) {
	for my $kid ( @{$self->{'children'}} ) {
	    my ( $kidmap, $kidh ) = $kid->levels_to_snapshots();
	    while ( my ($lev, $snap) = each %levtosnap ) {
		my $ksnap = $kidmap->{$lev} or die
		    'Amanda::Application::AmZfsHoldSend: missing level ' .
		    $lev . ' in ' . $kid->{'fqname'};
		die 'Amanda::Application::AmZfsHoldSend: level ' . $lev .
		    ' maps to different snapshot name in ' . $kid->{'fqname'}
		    unless $ksnap->{'name'} eq $snap->{'name'};
	    }
	}
    }

    return ( \%levtosnap, $latestlevel );
}

# For a volume, or a filesystem with no children, just its list of snapshots.
# For a filesystem with children, that list, less any snapshots missing from
# common_snapshots of any child. The order of my original snapshot list is
# preserved, so that the concept of "oldest" or "latest" common snapshot can
# mean something (though it is not guaranteed to be so in all children; a
# remaining test that will need to be made, for an incremental backup, is that
# at each node in the tree to back up, the chosen base snapshot precedes the
# chosen goal snapshot. No stronger ordering assumptions necessarily hold.)
sub common_snapshots {
    my ( $self ) = @_;
    my @snapnames = map { $_->{'name'} } @{$self->{'snapshots'}};
    return \@snapnames unless $self->{'children'};

    my %counts;
    for my $kid ( @{$self->{'children'}} ) {
	for my $snap ( map { $_->{'name'} } @{$kid->{'snapshots'}} ) {
	    $counts{$snap}++;
	}
    }

    my $fullcount = scalar(@{$self->{'children'}});
    my @common;
    for my $snap ( @snapnames ) {
	push @common, $snap if $fullcount == $counts{$snap};
    }

    return \@common;
}

# The trivial case snap1 eq snap2 is allowed (Amanda could run before another
# snapshot has been created); else snap1 must be everywhere older than snap2.
# (The case of multiple snapshots with the same name on a single dataset does
# not arise; zfs itself sees to that.)
sub consistently_ordered {
    my ( $self, $snap1, $snap2 ) = @_;

    my @snapnames = map { $_->{'name'} } @{$self->{'snapshots'}};
    my @pair = grep { $_ eq $snap1 or $_ eq $snap2 } @snapnames;
    return 0 unless @pair and $pair[0] eq $snap1;

    my $kids = $self->{'children'} or return 1;

    for my $kid ( @$kids ) {
	return 0 unless $kid->consistently_ordered($snap1, $snap2);
    }

    return 1;
}

sub apply_hold {
    my ( $self, $holdtag ) = @_;

    my $rslt = system {$zfsexecutable} (
	'zfs', 'hold', '-r', '--', $holdtag, $self->{'fqname'}
    );
    die 'Amanda::Application::AmZfsHoldSend: zfs hold nonzero result: ' . $rslt
	unless 0 == $rslt;
}

sub release_hold {
    my ( $self, $holdtag ) = @_;

    # Assume here that levels_to_snapshots() has succeeded, meaning the same
    # hold tag is on the same snapshot throughout the tree, so a simple
    # release -r suffices. Absent that assumption, one could descend the tree
    # and do a single zfs release at each child on the snapshot there mapped
    # to the given level.
    my $rslt = system {$zfsexecutable} (
	'zfs', 'release', '-r', '--', $holdtag, $self->{'fqname'}
    );
    die 'Amanda::Application::AmZfsHoldSend: zfs release nonzero result: ' .
	$rslt
	unless 0 == $rslt;
}


#
#   The main attraction.
#
package Amanda::Application::AmZfsHoldSend;

use base 'Amanda::Application::Abstract';

use IPC::Open3;

sub supports_host { my ( $class ) = @_; return 1; }
sub supports_disk { my ( $class ) = @_; return 1; }
sub supports_index_line { my ( $class ) = @_; return 1; }
sub supports_message_line { my ( $class ) = @_; return 1; }
sub supports_record { my ( $class ) = @_; return 1; }
sub supports_calcsize { my ( $class ) = @_; return 1; }
sub supports_client_estimate { my ( $class ) = @_; return 1; }
sub supports_multi_estimate { my ( $class ) = @_; return 1; }

sub max_level { my ( $self ) = @_; return 9; }

sub new {
    my ( $class, $refopthash ) = @_;
    my $self = $class->SUPER::new($refopthash);

    $self->{'zfsexecutable'} = $self->{'options'}->{'zfsexecutable'};
    $self->{'holdtagprefix'} = $self->{'options'}->{'holdtagprefix'};

    $self->{'localstate'} = $self->read_local_state(); # custom overridden here
    return $self;
}

sub declare_common_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'zfsexecutable=s', 'holdtagprefix=s',
			  'uncompressed=s', 'dedup=s', 'large-block=s',
			  'embed=s', 'raw=s' );

    $refopthash->{'zfsexecutable'} = 'zfs';
    $refopthash->{'holdtagprefix'} = 'org.amanda holdsend';
    $refopthash->{'uncompressed'}= $class->boolean_property_setter($refopthash);
    $refopthash->{'opt_uncompressed'} = 1; # why 1? -c not always supported
    $refopthash->{'dedup'}= $class->boolean_property_setter($refopthash);
    $refopthash->{'large-block'}= $class->boolean_property_setter($refopthash);
    $refopthash->{'embed'}= $class->boolean_property_setter($refopthash);
    $refopthash->{'raw'}= $class->boolean_property_setter($refopthash);
}

sub declare_restore_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->SUPER::declare_restore_options($refopthash, $refoptspecs);
    push @$refoptspecs, ( 'dataset=s' );
    push @$refoptspecs, ( 'destructive=s' );
    push @$refoptspecs, ( 'unmounted=s' );
    push @$refoptspecs, ( 'overrideproperty=s@' );
    push @$refoptspecs, ( 'excludeproperty=s@' );

    $refopthash->{'destructive'} = $class->boolean_property_setter($refopthash);
    $refopthash->{'unmounted'} = $class->boolean_property_setter($refopthash);
    $refopthash->{'opt_destructive'} = 1;
    $refopthash->{'overrideproperty'} = [];
    $refopthash->{'excludeproperty'} = [];
}

# This read_local_state does not take the getopt arguments taken by the
# generic version it overrides ... this version is specific to this class
# and knows exactly what it's retrieving.
sub read_local_state {
    my ( $self ) = @_;

    my $qpfx = quotemeta($self->{'holdtagprefix'});
    Amanda::Application::AmZfsHoldSend::Dataset->set_rx_holdtag(
	qr/^$qpfx (\d++)\s*+$/o
    );
    Amanda::Application::AmZfsHoldSend::Dataset->set_zfsexecutable(
	$self->{'zfsexecutable'}
    );

    my @cmd = (
	$self->{'zfsexecutable'}, 'list', '-Hr',
	'-t', 'filesystem,volume,snapshot',
	'-o', 'name,type,userrefs', '--',
	$self->{'options'}->{'device'}
    );

    my %ns; # initially empty dataset namespace
    my $topds;

    open my $listfh, '-|', @cmd;
    while ( my $line = <$listfh> ) {
	my $ds = Amanda::Application::AmZfsHoldSend::Dataset->new($line, \%ns); 
	$topds //= $ds; 
    }
    $listfh->close();

    my ( $levtosnap, $latestlevel ) = $topds->levels_to_snapshots();

    my %state;
    my $maxlevel = ( $latestlevel // -1) + 1;

    while ( my ( $lev, $snap ) = each %$levtosnap ) {
	$state{$lev} = { level => $lev, snapshot => $snap->{'name'} };
    }

    my $snaps = $topds->common_snapshots();

    $state{'maxlevel'} = $maxlevel;
    $state{'oldestsnapshot'} = $snaps->[0];
    $state{'newestsnapshot'} = $snaps->[scalar(@$snaps)-1];
    $state{'topds'} = $topds;
    return \%state;
}

sub update_local_state {
    my ( $self, $state, $level, $opthash ) = @_;

    $state->{'oldlist'} //= [];
    $state->{'newlist'} //= [];

    push @{$state->{'newlist'}}, $opthash;
    my $oldmax = $state->{'maxlevel'};
    for ( my $lv = $level; $lv < $oldmax; $lv += 1 ) {
	push @{$state->{'oldlist'}}, $state->{$lv};
	delete $state->{$lv};
    }
    $state->{'maxlevel'} = 1 + $level;
    $state->{$level} = $opthash;
}

sub write_local_state {
    my ( $self, $state ) = @_;
    return unless $state->{'oldlist'} and $state->{'newlist'}; # nothing to do

    my ( $levtosnap, $highest ) = $state->{'topds'}->levels_to_snapshots();

    # Remove old holds first. This may be unsatisfying on a rigorous theoretical
    # level, leaving a brief moment when no needed snapshot has a hold--and the
    # concern could be more than theoretical, if an environment has done a lot
    # of zfs destroy -d operations and the released snapshots all vanish before
    # the new hold can be placed. But balancing that is another practical issue:
    # if there has been no new snapshot created, a backup may try to reapply the
    # same hold tag on the same snapshot, and that fails.
    #  An alternative would be to complicate the logic and avoid releasing the
    # hold for $level if it will be going right back on the same snapshot. That
    # would work, but sacrifice the accidental feature that the hold timestamp
    # records when the latest corresponding backup was taken.
    #  Another alternative, not sacrificing the timestamp, would be to generate
    # a random hold tag to apply on the snapshot, then release and reapply the
    # original tag, then release the generated one. But that's still more
    # complexity aimed at a low-likelihood event, so, some other day....
    for my $opthash ( @{$state->{'oldlist'}} ) {
	my $level = $opthash->{'level'};
	my $holdtag = $self->{'holdtagprefix'} . ' ' . $level;
	my $snapobj = $levtosnap->{$level};
	$snapobj->release_hold($holdtag);
    }

    for my $opthash ( @{$state->{'newlist'}} ) {
	my $level = $opthash->{'level'};
	my $holdtag = $self->{'holdtagprefix'} . ' ' . $level;
	my $snapname = $opthash->{'snapshot'};
	my $snapobj = (
	    grep { $_->{'name'} eq $snapname }
	    @{$state->{'topds'}->{'snapshots'}}
	)[0];
	$snapobj->apply_hold($holdtag);
    }
}

my $rx_compratio = qr/^(\d++)\.(\d\d)x$/o;

my $rx_nvPsize = qr/^size\t(\d++)$/o;

sub inner_estimate {
    my ( $self, $level ) = @_;
    my $latestsnapshot = $self->{'localstate'}->{'newestsnapshot'};

    if ( $self->{'options'}->{'calcsize'} and defined $latestsnapshot ) {
        return $self->inner_estimate_nvP($level, $latestsnapshot);
    } else {
        return $self->inner_estimate_brute($level, $latestsnapshot);
    }
}

# The complete output of send -nvP -R includes sizes for all descendant
# datasets and intermediate snapshots, followed by one final line matching
# /^size\t/ with the total. This inner_estimate (which is called in a loop
# by command_estimate, once for each level) ignores all but the last line
# and returns that total as the size for the level. Clearly, it would be
# possible to override command_estimate itself, skip the loop, and calculate
# the estimates for all levels from the output of a single send -nvP. Left for
# future, as this is simple and not dreadfully slow.
# The output of send -nvP goes to stderr, not stdout.
sub inner_estimate_nvP {
    my ( $self, $level, $latestsnapshot ) = @_;

    my @cmd = $self->construct_send_cmd($level, $latestsnapshot);

    splice @cmd, 2, 0, '-nvP';

    my $sizestr;
    my ( $wtr, $rdr );
    my $sendpid = open3($wtr, $rdr, $rdr, @cmd);
    $wtr->close();
    while ( <$rdr> ) {
        ( $sizestr ) = m/$rx_nvPsize/o;
    }
    $rdr->close();
    waitpid($sendpid, 0);

    $self->print_to_server_and_die("Nonzero exit status of zfs send -nvP: ".$?,
                                   $Amanda::Script_App::ERROR)
	unless 0 == $?;

    $self->print_to_server_and_die("Failed to read size from zfs send -nvP",
                                   $Amanda::Script_App::ERROR)
	unless defined($sizestr);

    return Math::BigInt->new($sizestr);
}

sub inner_estimate_brute {
    my ( $self, $level, $latestsnapshot ) = @_;

    if ( 0 == $level ) {
	open my $getfh, '-|', $self->{'zfsexecutable'},
	    'get', '-Hp', '-o', 'value', '--', 'used,compressratio',
	    $self->{'options'}->{'device'};
	my $used = <$getfh>;
	my $compratio = <$getfh>;
	$getfh->close();
	$used = Math::BigInt->new($used);
	if ( $self->{'options'}->{'uncompressed'} ) {
	    my ( $wholes, $cents ) = $compratio =~ m/$rx_compratio/o;
	    $used->bmul($wholes . $cents)->badd(99)->bdiv(100)
	}
	return $used;
    }

    my $mxl = $self->{'localstate'}->{'maxlevel'};
    if ( $level > $mxl ) {
	$self->print_to_server("Requested estimate level $level > $mxl",
			       $Amanda::Script_App::ERROR);

	return Math::BigInt->bone('-');
    }

    my $priorsnapshot = $self->{'localstate'}->{$level - 1}->{'snapshot'};

    # In this easy case, zero isn't quite accurate; if the planner actually
    # does, for some reason, choose this level, inner_backup will in fact do
    # an incremental zfs send from the snapshot to itself. That produces
    # warnings from zfs send, but also a valid, short but non-zero-length,
    # send stream from which zfs receive does nothing, successfully. (Just
    # generating a zero-length stream will make zfs receive do nothing,
    # unsuccessfully.) Almost nothing, anyway: the stream does contain the
    # latest values of properties.
    #  So returning zero here is a lie, but an easy and not-far-from-the-truth
    # one, and this is, after all, an estimate. Perhaps the planner will see
    # this zero estimate and choose a different level, which would be ok.
    return Math::BigInt->bzero() if $latestsnapshot eq $priorsnapshot;

    # Ok, it wasn't any of the easy cases.

    open my $sendfh, '-|', $self->construct_send_cmd($level, $latestsnapshot);
    my $buffer;
    my $size = Math::BigInt->bzero();
    my $s;

    while (($s = sysread($sendfh, $buffer, 32768)) > 0) {
	$size->badd($s);
    }
    $sendfh->close();
    return $size;
}

sub construct_send_cmd {
    my ( $self, $level, $latestsnapshot ) = @_;
    my $dn = $self->{'options'}->{'device'};

    my $mxl = $self->{'localstate'}->{'maxlevel'};

    my @compressed = $self->{'options'}->{'uncompressed'} ? () : ( '-c' );
    my @misc_opts = $self->ozfs_send_options();

    if ( 0 == $level ) {
	return $self->{'zfsexecutable'}, 'send', @compressed, @misc_opts, '-R',
	    '--', $dn . '@' . $latestsnapshot;
    } elsif ( $level > $mxl ) {
	$self->print_to_server_and_die("Requested backup level $level > $mxl",
			       $Amanda::Script_App::ERROR);
    } else {
	my $priorsnapshot = $self->{'localstate'}->{$level - 1}->{'snapshot'};
	unless ( $self->{'localstate'}->{'topds'}->
	  consistently_ordered($priorsnapshot, $latestsnapshot) ) {
	    $self->print_to_server_and_die(
		"Snapshots '$priorsnapshot' and '$latestsnapshot' not " .
		"consistently ordered", $Amanda::Script_App::ERROR);
	}

	return $self->{'zfsexecutable'}, 'send', @compressed, @misc_opts, '-R',
	    '-I', $priorsnapshot,
	    '--', $dn . '@' . $latestsnapshot;
    }
}

sub ozfs_send_options {
    my ( $self ) = @_;

    my @opts;
    push @opts, '-D' if $self->{'options'}->{'dedup'};
    push @opts, '-L' if $self->{'options'}->{'large-block'};
    push @opts, '-e' if $self->{'options'}->{'embed'};
    push @opts, '-w' if $self->{'options'}->{'raw'};
    return @opts;
}

sub inner_backup {
    # XXX assert level==0 if no --record
    my ( $self, $fdout ) = @_;
    my $dn = $self->{'options'}->{'device'};
    my $level = $self->{'options'}->{'level'};

    my $latestsnapshot = $self->{'localstate'}->{'newestsnapshot'};
    unless ( defined $latestsnapshot ) {
	$self->print_to_server_and_die(
	    "At least one snapshot must exist", $Amanda::Script_App::ERROR);
    }

    open my $sendfh, '-|', $self->construct_send_cmd($level, $latestsnapshot);

    my $size = Math::BigInt->bzero();
    my $buffer;
    my $s;

    while (($s = sysread($sendfh, $buffer, 32768)) > 0) {
	Amanda::Util::full_write($fdout, $buffer, $s);
	$size->badd($s);
    }
    $sendfh->close();

    $self->emit_index_entry('/');

    if ( $self->{'options'}->{'record'} ) {
	$self->update_local_state($self->{'localstate'}, $level, {
	    level => $level, snapshot => $latestsnapshot });
    }

    return $size;
}

sub check_restore_options {
    my ( $self ) = @_;
    
    $self->SUPER::check_restore_options();

    $self->{'restoredestination'} = $self->{'options'}->{'dataset'};

    $self->print_to_server_and_die(
	"The 'dataset' property must be supplied; there is no default",
	$Amanda::Script_App::ERROR)
	unless defined $self->{'restoredestination'};

    $self->{'destructive'} = $self->{'options'}->{'destructive'};
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

    my $dn = $self->{'restoredestination'};
    my @force = ( $self->{'destructive'} or 0 == $level ) ? ( '-F' ) : ();
    my @unmounted = $self->{'options'}->{'unmounted'} ? ( '-u' ) : ();
    my @propoverrides =
	map { ('-o', $_) } @{$self->{'options'}->{'overrideproperty'}};
    my @propexcludes =
	map { ('-o', $_) } @{$self->{'options'}->{'excludeproperty'}};
    
    # $fdin happens to be fileno(STDIN), so may as well just spawn zfs receive
    # to read directly, rather than spoonfeeding it through a pipe.
    my $rslt = system {$self->{'zfsexecutable'}} (
	'zfs', 'receive', @force, @unmounted, @propoverrides, @propexcludes,
	'--', $dn
    );
    if ( 0 != $rslt ) {
	$self->print_to_server_and_die(
	    "Nonzero result from zfs receive: $rslt",
	    $Amanda::Script_App::ERROR);
    };
}

package main;

Amanda::Application::AmZfsHoldSend->run();
