# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as
# published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
#
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

=head1 NAME

Amanda::Recovery::Planner - use the catalog to plan recoveries

=head1 SYNOPSIS

    my $plan;

    $subs{'make_plan'} = make_cb(make_plan => sub {
	Amanda::Recovery::Planner::make_plan(
	    dumpspecs => [ $ds1, $ds2 ],
	    algorithm => $algo,
	    changer => $changer,
	    plan_cb => $subs{'plan_cb'});
    };

    $subs{'plan_cb'} = make_cb(plan_cb => sub {
	my ($err, $pl) = @_;
	die $err if $err;

	$plan = $pl;
	$subs{'start_next_dumpfile'}->();
    });

    $subs{'start_next_dumpfile'} = make_cb(start_next_dumpfile => sub {
	my $dump = $plan->shift_dump();
	if (!$dump) {
	    # .. all done!
	}

	print "recovering ", $dump->{'hostname'}, " ", $dump->{'diskname'}, "\n";
	$clerk->get_xfer_src( .. dump => $dump .. );
	# ..
    });

=head1 OVERVIEW

This package determines the optimal way to recover dump files from storage.
Its function is superficially fairly simple: given a collection of desired
dumpfiles, it returns a Plan to recover those dumpfiles, specifying exactly the
volumes and files that are needed, and the order in which they should be
accesed.

=head2 ALGORITHMS

Several algorithms will soon be available for selecting volumes when a dumpfile
appears in several places (e.g., from an amvault operation).  At the moment,
the algorithm argument should be omitted, as this will eventually indicate that
the user-configured algorithm should be applied.

=head2 INSTANTIATING A PLAN

Call C<make_plan> with the desired dumpspecs, a changer, and a callback:

    Amanda::Recovery::Planner::make_plan(
	dumpspecs => [ $ds1, $ds2, .. ],
	changer => $chg,
	plan_cb => $plan_cb);

As a shortcut, you may also specify a single dumpspec:

    Amanda::Recovery::Planner::make_plan(
	dumpspec => $ds,
	changer => $chg,
	plan_cb => $plan_cb);

Note that in this case, the resulting plan may contain more than one dump, if
the dumpspec was not unambiguous.

To select the planner algorithm, pass an C<algorithm> argument.  This argument
is currently ignored and should be omitted.  If the optional argument C<debug>
is given with a true value, then the Planner will log additional debug
information to the Amanda debug logs.

The optional argument C<one_dump_per_part> will create a "no-reassembly" plan,
where each part appears as the only part in a unique dump.  The dump objects
will have the key C<single_part> set to 1.

The C<plan_cb> is called with two arguments:

    $plan_cb->($err, $plan);

If C<$err> is defined, it describes an error that occurred; otherwise, C<$plan>
is the generated plan, as described below.

Some algorithms may consult the changer's inventory to determine what volumes
are available.  It is because of this asynchronous operation that C<make_plan>
takes a callback instead of simply returning the plan.

=head2 PLANS

A Plan is a perl object describing the process for recovering zero or more
dumpfiles.  Its principal components are dumps, in order, that are to be
recovered, but the object presents some other interfaces that return useful
information about the plan.

The C<'dumps'> key holds the list of dumps, in the order they should be
performed.  Callers should shift dumps off this list to present to the Clerk.

To get a list of volumes that the plan requires, in order, use
C<get_volume_list>.  Each volume is represented as a hash:

  { label => 'DATA182', available => 1 }

where C<available> is false if the planner did not find this volume in the
changer.  Planners which do not consult the changer will have a false value for
C<available>.

Similarly, to get a list of holding files that the plan requires, in order, use
C<get_holding_file_list>.  Each file is represented as a string giving the
fully qualified pathname.

=cut

package Amanda::Recovery::Planner;

use strict;
use warnings;
use Carp;

sub make_plan {
    my %params = @_;

    $params{'dumpspecs'} = [ $params{'dumpspec'} ]
	if exists $params{'dumpspec'};

    for my $rq_param qw(changer plan_cb dumpspecs) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    my $plan = Amanda::Recovery::Planner::Plan->new({
	algo => $params{'algorithm'},
	chg => $params{'changer'},
	debug => $params{'debug'},
	one_dump_per_part => $params{'one_dump_per_part'},
    });

    $plan->make_plan($params{'dumpspecs'}, $params{'plan_cb'});
}

package Amanda::Recovery::Planner::Plan;

use strict;
use warnings;
use Data::Dumper;

use Amanda::Device qw( :constants );
use Amanda::Header;
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::MainLoop;
use Amanda::DB::Catalog;
use Amanda::Tapelist;

sub new {
    my $class = shift;
    my $self = shift;
    return bless($self, $class);
}

sub make_plan {
    my $self = shift;
    my ($dumpspecs, $plan_cb) = @_;

    # first, get the set of dumps that match these dumpspecs
    my @dumps = Amanda::DB::Catalog::get_dumps(dumpspecs => $dumpspecs);

    # now "bin" those by host/disk/dump_ts/level
    my %dumps;
    for my $dump (@dumps) {
	my $k = join("\0", $dump->{'hostname'}, $dump->{'diskname'},
			   $dump->{'dump_timestamp'}, $dump->{'level'});
	$dumps{$k} = [] unless exists $dumps{$k};
	push @{$dumps{$k}}, $dump;
    }

    # now select the "best" of each set of dumps, and put that in @dumps
    @dumps = ();
    for my $options (values %dumps) {
	my @options = @$options;
	# if there's only one option, the choice is easy
	if (@options == 1) {
	    push @dumps, $options[0];
	    next;
	}

	# if there are several, narrow to those with an OK status or barring that,
	# those with a PARTIAL status.  FAIL need not apply.
	my @ok_options = grep { $_->{'status'} eq 'OK' } @options;
	my @partial_options = grep { $_->{'status'} eq 'PARTIAL' } @options;

	if (@ok_options) {
	    @options = @ok_options;
	} else {
	    @options = @partial_options;
	}

	# now, take the one written longest ago - this gets us the dump on secondary
	# media if it hasn't been overwritten, otherwise the dump on tertiary media,
	# etc.  Note that this also prefers dumps on holding disk, since they are
	# tagged with a write_timestamp of 0
	@options = Amanda::DB::Catalog::sort_dumps(['write_timestamp'], @options);
	push @dumps, $options[0];
    }

    # at this point we have exactly one instance of each dump in @dumps.

    # If one_dump_per_part was specified, rearrange @dumps to have a distinct
    # dump object for each part.
    if ($self->{'one_dump_per_part'}) {
	@dumps = $self->split_dumps_per_part(\@dumps);
    }

    # now sort the dumps in order by their constituent parts.  This sorts based
    # on write_timestamp, then on the label of the first part of the dump,
    # using the tapelist to order the labels.  Where labels match, it sorts on
    # the part's filenum.  This should sort the dumps into the order in which
    # they were written, with holding dumps coming in at the head of the list.
    my $tapelist_filename = config_dir_relative(getconf($CNF_TAPELIST));
    my $tapelist = Amanda::Tapelist::read_tapelist($tapelist_filename);

    my $sortfn = sub {
	my $rv;
	my $tle;

	return $rv
	    if ($rv = $a->{'write_timestamp'} cmp $b->{'write_timestamp'});

	# above will take care of comparing a holding dump to an on-media dump, but
	# if both are on holding then we need to compare them lexically
	if (exists $a->{'parts'}[1]{'holding_file'}
	and exists $b->{'parts'}[1]{'holding_file'}) {
	    return $a->{'parts'}[1]{'holding_file'} cmp $b->{'parts'}[1]{'holding_file'};
	}

	my ($alabel, $blabel) = (
	    $a->{'parts'}[1]{'label'},
	    $b->{'parts'}[1]{'label'},
	);

	my ($apos, $bpos);
	$apos = $tle->{'position'}
	    if (($tle = $tapelist->lookup_tapelabel($alabel)));
	$bpos = $tle->{'position'}
	    if (($tle = $tapelist->lookup_tapelabel($blabel)));
	return ($bpos <=> $apos) # not: reversed for "oldest to newest"
	    if defined $bpos && defined $apos && ($bpos <=> $apos);

	# if a tape wasn't in the tapelist, just sort the labels lexically (this
	# really shouldn't happen)
	if (!defined $bpos || !defined $apos) {
	    return $alabel cmp $blabel
		if defined $alabel and defined $blabel and $alabel cmp $blabel ;
	}

	# finally, the dumps are on the same volume, so just sort by filenum
	return $a->{'parts'}[1]{'filenum'} <=> $b->{'parts'}[1]{'filenum'};
    };
    @dumps = sort $sortfn @dumps;

    $self->{'dumps'} = \@dumps;

    Amanda::MainLoop::call_later($plan_cb, undef, $self);
}

sub split_dumps_per_part {
    my $self = shift;
    my ($dumps) = @_;

    my @new_dumps;

    for my $dump (@$dumps) {
	for my $part (@{$dump->{'parts'}}) {
	    my ($newdump, $newpart);

	    # skip part 0
	    next unless defined $part;

	    # shallow copy the dump and part objects
	    $newdump = do { my %t = %$dump; \%t; };
	    $newpart = do { my %t = %$part; \%t; };

	    # overwrite the interlinking
	    $newpart->{'dump'} = $newdump;
	    $newdump->{'parts'} = [ undef, $newpart ];

	    $newdump->{'single_part'} = 1;

	    push @new_dumps, $newdump;
	}
    }

    return @new_dumps;
}

sub get_volume_list {
    my $self = shift;
    my $last_label;
    my @volumes;

    for my $dump (@{$self->{'dumps'}}) {
	for my $part (@{$dump->{'parts'}}) {
	    next unless defined $part; # skip parts[0]
	    next unless defined $part->{'label'}; # skip holding parts
	    if (!defined $last_label || $part->{'label'} ne $last_label) {
		$last_label = $part->{'label'};
		push @volumes, { label => $last_label, available => 0 };
	    }
	}
    }

    return @volumes;
}

sub get_holding_file_list {
    my $self = shift;
    my @hfiles;

    for my $dump (@{$self->{'dumps'}}) {
	for my $part (@{$dump->{'parts'}}) {
	    next unless defined $part; # skip parts[0]
	    next unless defined $part->{'holding_file'}; # skip on-media dumps
	    push @hfiles, $part->{'holding_file'};
	}
    }

    return @hfiles;
}

sub dbg {
    my ($self, $msg) = @_;
    if ($self->{'debug'}) {
	debug("Amanda::Recovery::Planner: $msg");
    }
}

1;
