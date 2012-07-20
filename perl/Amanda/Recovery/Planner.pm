# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
	my $dump = shift @{$plan->{'dumps'}};
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

For most purposes, you should call C<make_plan> with the desired dumpspecs, a
changer, and a callback:

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
information to the Amanda debug logs.  Debugging is automatically enabled if
the C<DEBUG_RECOVERY> configuration parameter is set to anything greater than
1.

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

=head3 Pre-defined Plans

In some cases, you already know exactly where the data is, and just need a
proper plan object to hand to L<Amanda::Recovery::Clerk>.  One such case is a
recovery from a holding file.  In this case, use C<make_plan> like this:

    Amanda::Recovery::Planner::make_plan(
	holding_file => $hf,
	dumpspec => $ds,
	plan_cb => $plan_cb);

This will create a plan to recover the data in C<$fh>.  The dumpspec is
optional, but if present will be used to verify that the holding file contains
the appropriate dump.

Similarly, if you have a list of label:fileno pairs to use, call C<make_plan>
like this:

    Amanda::Recovery::Planner::make_plan(
	filelist => [
	    $label => [ $filenum, $filenum, .. ],
	    $label => ..
	],
	dumpspec => $ds,
	plan_cb => $plan_cb);

This will verify the requested files against the catalog and the dumpspec, then
hand back a plan that essentially embodies C<filelist>.

Note that both of these functions will only create a single-dump plan.

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

    my $plan = Amanda::Recovery::Planner::Plan->new({
	algo => $params{'algorithm'},
	chg => $params{'changer'},
	debug => $params{'debug'},
	one_dump_per_part => $params{'one_dump_per_part'},
    });

    if (exists $params{'holding_file'}) {
	$plan->make_holding_plan(%params);
    } elsif (exists $params{'filelist'}) {
	$plan->make_plan_from_filelist(%params);
    } else {
	$plan->make_plan(%params);
    }
}

package Amanda::Recovery::Planner::Plan;

use strict;
use warnings;
use Data::Dumper;
use Carp;

use Amanda::Device qw( :constants );
use Amanda::Holding;
use Amanda::Header;
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::MainLoop;
use Amanda::DB::Catalog;
use Amanda::Tapelist;

sub new {
    my $class = shift;
    my $self = shift;

    $self->{'debug'} = $Amanda::Config::debug_recovery
	if not defined $self->{'debug'}
	    or $Amanda::Config::debug_recovery > $self->{'debug'};

    return bless($self, $class);
}

sub shift_dump {
    my $self = shift;
    return shift @{$self->{'dumps'}};
}

sub make_plan {
    my $self = shift;
    my %params = @_;

    for my $rq_param (qw(changer plan_cb dumpspecs)) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }
    my $dumpspecs = $params{'dumpspecs'};

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
    my $tapelist = Amanda::Tapelist->new($tapelist_filename);

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

    Amanda::MainLoop::call_later($params{'plan_cb'}, undef, $self);
}

sub make_holding_plan {
    my $self = shift;
    my %params = @_;

    for my $rq_param (qw(holding_file plan_cb)) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    # This is a little tricky.  The idea is to open up the holding file and
    # read its header, then find that dump in the catalog.  This may seem like
    # the long way around, but it adds an extra layer of security to the
    # recovery process, as it prevents recovery from arbitrary files on the
    # filesystem that are not under a recognized holding directory.

    my $hdr = Amanda::Holding::get_header($params{'holding_file'});
    if (!$hdr or $hdr->{'type'} != $Amanda::Header::F_DUMPFILE) {
	return $params{'plan_cb'}->(
		"could not open '$params{holding_file}': missing or not a holding file");
    }

    # look up this holding file in the catalog, adding the dumpspec we were
    # given so that get_dumps will compare against it for us.
    my $dump_timestamp = $hdr->{'datestamp'};
    my $hostname = $hdr->{'name'};
    my $diskname = $hdr->{'disk'};
    my $level = $hdr->{'dumplevel'};
    my @dumps = Amanda::DB::Catalog::get_dumps(
	    $params{'dumpspec'}? (dumpspecs => [ $params{'dumpspec'} ]) : (),
	    dump_timestamp => $dump_timestamp,
	    hostname => $hostname,
	    diskname => $diskname,
	    level => $level,
	    holding => 1,
	);

    if (!@dumps) {
	return $params{'plan_cb'}->(
		"Specified holding file does not match dumpspec");
    }

    # this would be weird..
    $self->dbg("got multiple dumps from Amanda::DB::Catalog for a holding file!")
	if (@dumps > 1);

    # arbitrarily keepy the first dump if we got several
    $self->{'dumps'} = [ $dumps[0] ];

    Amanda::MainLoop::call_later($params{'plan_cb'}, undef, $self);
}

sub make_plan_from_filelist {
    my $self = shift;
    my %params = @_;

    for my $rq_param (qw(filelist plan_cb)) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    my $steps = define_steps
	cb_ref => \$params{'plan_cb'};

    step get_inventory => sub {
	if (defined $params{'chg'} and $params{'chg'}->have_inventory()) {
	    return $params{'chg'}->inventory( inventory_cb => $steps->{'got_inventory'});
	} else {
	    return $steps->{'got_inventory'}->(undef, undef);
	}
    };
    step got_inventory => sub {
	my ($err, $inventory) = @_;

	# This is similarly tricky - in this case, we search for dumps matching
	# both the dumpspec and the labels, filter that down to just the parts we
	# want, and then check that only one dump remains.  Then we look up that
	# dump.

	my @labels;
	my %files;
	my @filelist = @{$params{'filelist'}};
	while (@filelist) {
	    my $label = shift @filelist;
	    push @labels, $label;
	    $files{$label} = shift @filelist;
	}

	my @parts = Amanda::DB::Catalog::get_parts(
		$params{'dumpspec'}? (dumpspecs => [ $params{'dumpspec'} ]) : (),
		labels => [ @labels ]);

	# filter down to the parts that match filelist (using %files)
	@parts = grep {
	    my $filenum = $_->{'filenum'};
	    grep { $_ == $filenum } @{$files{$_->{'label'}}};
	} @parts;

	# extract the dumps, using a hash (on the perl identity of the dump) to
	# ensure uniqueness
	my %dumps = map { my $d = $_->{'dump'}; ($d, $d) } @parts;
	my @dumps = values %dumps;

	if (!@dumps) {
	    return $params{'plan_cb'}->(
		"Specified file list does not match dumpspec");
	} elsif (@dumps > 1) {
	    # Check if they are all for the same dump
	    my $dump_timestamp = $dumps[0]->{'dump_timestamp'};
	    my $hostname = $dumps[0]->{'hostname'};
	    my $diskname = $dumps[0]->{'diskname'};
	    my $level = $dumps[0]->{'level'};
	    my $orig_kb = $dumps[0]->{'orig_kb'};

	    foreach my $dump (@dumps) {
		if ($dump_timestamp != $dump->{'dump_timestamp'} ||
		    $hostname ne $dump->{'hostname'} ||
		    $diskname ne $dump->{'diskname'} ||
		    $level != $dump->{'level'} ||
		    $orig_kb != $dump->{'orig_kb'}) {
		    return $params{'plan_cb'}->(
			"Specified file list matches multiple dumps; cannot continue recovery");
		}
	    }

	    # I would prefer the Planner to return alternate dump and the Clerk
	    # choose which one to use
	    if (defined $inventory) {
		for my $dump (@dumps) {
		    my $all_part_found = 0;
		    my $part_found = 1;
		    for my $part (@{$dump->{'parts'}}) {
			next if !defined $part;
		        my $found = 0;
			foreach my $sl (@$inventory) {
			    if (defined $sl->{'label'} and
				$sl->{'label'} eq $part->{'label'}) {
				$found = 1;
				last;
			    }
			}
			if ($found == 0) {
			    $part_found = 0;
			    last;
			}
		    }
		    if ($part_found == 1) {
			@dumps = $dumps[0];
			last;
		    }
		}
		# the first one will be used
	    } else {
		# will uses the first dump.
	    }
	}

	# now, because of the weak linking used by Amanda::DB::Catalog, we need to
	# re-query for this dump.  If we don't do this, the parts will all be
	# garbage-collected when we hand back the plan.  This is, chartiably, "less
	# than ideal".  Note that this has the side-effect of filling in any parts of
	# the dump that were missing from the filelist.
	@dumps = Amanda::DB::Catalog::get_dumps(
	    hostname => $dumps[0]->{'hostname'},
	    diskname => $dumps[0]->{'diskname'},
	    level => $dumps[0]->{'level'},
	    dump_timestamp => $dumps[0]->{'dump_timestamp'},
	    write_timestamp => $dumps[0]->{'write_timestamp'},
	    dumpspecs => $params{'dumpspecs'});

	# sanity check
	confess "no dumps" unless @dumps;
	$self->{'dumps'} = [ $dumps[0] ];

	Amanda::MainLoop::call_later($params{'plan_cb'}, undef, $self);
    };
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
