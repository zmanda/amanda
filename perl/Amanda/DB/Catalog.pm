# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::DB::Catalog;

=head1 NAME

Amanda::DB::Catalog - access to the Amanda catalog: where is that dump?

=head1 SYNOPSIS

  use Amanda::DB::Catalog;

  # get all dump timestamps on record
  my @timestamps = Amanda::DB::Catalog::get_timestamps();

  # loop over those timestamps, printing dump info for each one
  for my $timestamp (@timestamps) {
      my @dumpfiles = Amanda::DB::Catalog::get_parts(
	  timestamp => $timestamp,
	  ok => 1
      );
      print "$timstamp:\n";
      for my $dumpfile (@dumpfiles) {
	  print " ", $dumpfile->{hostname}, ":", $dumpfile->{diskname}, 
		" level ", $dumpfile->{level}, "\n";
      }
  }

=head1 MODEL

The Amanda catalog is modeled as a set of dumps comprised of parts.  A dump is
a complete bytestream received from an application, and is uniquely identified
by the combination of C<hostname>, C<diskname>, C<dump_timestamp>, C<level>,
and C<write_timestamp>.  A dump may be partial, or even a complete failure.

A part corresponds to a single file on a volume, containing a portion of the
data for a dump.  A part, then, is completely specified by a volume label and a
file number (C<filenum>).  Each part has, among other things, a part number
(C<partnum>) which gives its relative position within the dump.  The bytestream
for a dump is recovered by concatenating all of the successful (C<status> = OK)
parts matching the dump.

Files in the holding disk are considered part of the catalog, and are
represented as single-part dumps (holding-disk chunking is ignored, as it is
distinct from split parts).

=head2 DUMPS

The dump table contains one row per dump.  It has the following columns:

=over

=item dump_timestamp

(string) -- timestamp of the run in which the dump was created

=item write_timestamp

(string) -- timestamp of the run in which the part was written to this volume,
or C<"00000000000000"> for dumps in the holding disk.

=item hostname

(string) -- dump hostname

=item diskname

(string) -- dump diskname

=item level

(integer) -- dump level

=item status

(string) -- The status of the dump - "OK", "PARTIAL", or "FAIL".  If a disk
failed to dump at all, then it is not part of the catalog and thus will not
have an associated dump row.

=item message

(string) -- reason for PARTIAL or FAIL status

=item nparts

(integer) -- number of successful parts in this dump

=item bytes

(integer) -- size (in bytes) of the dump on disk, 0 if the size is not known.

=item kb

(integer) -- size (in kb) of the dump on disk

=item orig_kb

(integer) -- size (in kb) of the complete dump (before compression or encryption); undef
if not available

=item sec

(integer) -- time (in seconds) spent writing this part

=item parts

(arrayref) -- array of parts, indexed by partnum (so C<< $parts->[0] >> is
always C<undef>).  When multiple partial parts are available, the choice of the
partial that is included in this array is undefined.

=back

A dump is represented as a hashref with these keys.

The C<write_timestamp> gives the time of the amanda run in which the part was
written to this volume.  The C<write_timestamp> may differ from the
C<dump_timestamp> if, for example, I<amflush> wrote the part to tape after the
initial dump.

=head2 PARTS

The parts table contains one row per part, and has the following columns:

=over

=item label

(string) -- volume label (not present for holding files)

=item filenum

(integer) -- file on that volume (not present for holding files)

=item holding_file

(string) -- fully-qualified pathname of the holding file (not present for
on-media dumps)

=item dump

(object ref) -- a reference to the dump containing this part

=item status

(string) -- The status of the part - "OK", "PARTIAL", or "FAILED".

=item partnum

(integer) -- part number of a split part (1-based)

=item kb

(integer) -- size (in kb) of this part

=item sec

(integer) -- time (in seconds) spent writing this part

=back

A part is represented as a hashref with these keys.  The C<label> and
C<filenum> serve as a primary key. 

Note that parts' C<dump> and dumps' C<parts> create a reference loop.  This is
broken by making the C<parts> array's contents weak references in C<get_dumps>,
and the C<dump> reference weak in C<get_parts>.

=head2 NOTES

All timestamps used in this module are full-length, in the format
C<YYYYMMDDHHMMSS>.  If the underlying data contains only datestamps, they are
zero-extended into timestamps: C<YYYYMMDD000000>.  A C<dump_timestamp> always
corresponds to the initiation of the I<original> dump run, while
C<write_timestamp> gives the time the file was written to the volume.  When
parts are migrated from volume to volume (e.g., by I<amvault>), the
C<dump_timestamp> does not change.  

In Amanda, the tuple (C<hostname>, C<diskname>, C<level>, C<dump_timestamp>)
serves as a unique identifier for a dump bytestream, but because the bytestream
may appear several times in the catalog (due to vaulting) the additional
C<write_timestamp> is required to identify a particular on-storage instance of
a dump.  Note that the part sizes may differ between instances, so it is not
valid to concatenate parts from different dump instances.

=head1 INTERFACES

=head2 SUMMARY DATA

The following functions provide summary data based on the contents of the
catalog.

=over

=item get_write_timestamps()

Get a list of all write timestamps, sorted in chronological order.

=item get_latest_write_timestamp()

Return the most recent write timestamp.

=item get_latest_write_timestamp(type => 'amvault')
=item get_latest_write_timestamp(types => [ 'amvault', .. ])

Return the timestamp of the most recent dump of the given type or types.  The
available types are given below for C<get_run_type>.

=item get_labels_written_at_timestamp($ts)

Return a list of labels for volumes written at the given timestamp.

=item get_run_type($ts)

Return the type of run made at the given timestamp.  The result is one of
C<amvault>, C<amdump>, C<amflush>, or the default, C<unknown>.

=back

=head2 PARTS

=over

=item get_parts(%parameters)

This function returns a sequence of parts.  Values in C<%parameters> restrict
the set of parts that are returned.  The hash can have any of the following
keys:

=over

=item write_timestamp

restrict to parts written at this timestamp

=item write_timestamps

(arrayref) restrict to parts written at any of these timestamps (note that
holding-disk files have no C<write_timestamp>, so this option and the previous
will omit them)

=item dump_timestamp

restrict to parts with exactly this timestamp

=item dump_timestamps

(arrayref) restrict to parts with any of these timestamps

=item dump_timestamp_match

restrict to parts with timestamps matching this expression

=item holding

if true, only return dumps on holding disk.  If false, omit dumps on holding
disk.

=item hostname

restrict to parts with exactly this hostname

=item hostnames

(arrayref) restrict to parts with any of these hostnames

=item hostname_match

restrict to parts with hostnames matching this expression

=item diskname

restrict to parts with exactly this diskname

=item disknames

(arrayref) restrict to parts with any of these disknames

=item diskname_match

restrict to parts with disknames matching this expression

=item label

restrict to parts with exactly this label

=item labels

(arrayref) restrict to parts with any of these labels

=item level

restrict to parts with exactly this level

=item levels

(arrayref) restrict to parts with any of these levels

=item status

restrict to parts with this status

=item dumpspecs

(arrayref of dumpspecs) restruct to parts matching one or more of these dumpspecs

=back

Match expressions are described in the amanda(8) manual page.

=item sort_parts([ $key1, $key2, .. ], @parts)

Given a list of parts, this function sorts that list by the requested keys.
The following keys are available:

=over

=item hostname

=item diskname

=item write_timestamp

=item dump_timestamp

=item level

=item filenum

=item label

Note that this sorts labels I<lexically>, not necessarily in the order they were used!

=item partnum

=item nparts

=back

Keys are processed from left to right: if two dumps have the same value for
C<$key1>, then C<$key2> is examined, and so on.  Key names may be prefixed by a
dash (C<->) to reverse the order.

Note that some of these keys are dump keys; the function will automatically
access those values via the C<dump> attribute.

=back

=head2 DUMPS

=over

=item get_dumps(%parameters)

This function returns a sequence of dumps.  Values in C<%parameters> restrict
the set of dumps that are returned.  The same keys as are used for C<get_parts>
are available here, with the exception of C<label> and C<labels>.  In this
case, the C<status> parameter applies to the dump status, not the status of its
constituent parts.

=item sort_dumps([ $key1, $key2 ], @dumps)

Like C<sort_parts>, this sorts a sequence of dumps generated by C<get_dumps>.
The same keys are available, with the exception of C<label>, C<filenum>, and
C<partnum>.

=back

=head2 ADDING DATA

=over

=item add_part($part)

Add the given part to the database.  In terms of logfiles, this will either
create a new logfile (if the part's C<write_timestamp> has not been seen
before) or append to an existing logfile.  Note that a new logfile will require
a corresponding new entry in the tapelist.

Note that no locking is performed: multiple simultaneous calls to this function
can result in a corrupted or incorrect logfile.

TODO: add_dump

=back

=cut

use Amanda::Logfile qw( :constants );
use Amanda::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( quote_string weaken_ref match_disk match_host match_datestamp match_level);
use File::Glob qw( :glob );
use warnings;
use strict;

# tapelist cache
my $tapelist = undef;

# utility function
sub zeropad {
    my ($timestamp) = @_;
    if (length($timestamp) == 8) {
	return $timestamp."000000";
    }
    return $timestamp;
}

sub get_write_timestamps {
    my @rv;

    # find_log assumes that the tapelist has been loaded, so load it now
    _load_tapelist();

    for (Amanda::Logfile::find_log()) {
	next unless (my ($timestamp) = /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/);
	push @rv, zeropad($timestamp);
    }

    return sort @rv;
}

sub get_latest_write_timestamp {
    my %params = @_;

    if ($params{'type'}) {
	push @{$params{'types'}}, $params{'type'};
    }

    # get all of the timestamps and select the last one
    my @timestamps = get_write_timestamps();

    if (@timestamps) {
	# if we're not looking for a particular type, then this is easy
	if (!exists $params{'types'}) {
	    return $timestamps[-1];
	}

	# otherwise we need to search backward until we find a logfile of
	# the right type
	while (@timestamps) {
	    my $ts = pop @timestamps;
	    my $typ = get_run_type($ts);
	    if (grep { $_ eq $typ } @{$params{'types'}}) {
		return $ts;
	    }
	}
    }

    return undef;
}

sub get_run_type {
    my ($write_timestamp) = @_;

    # find all of the logfiles with that name
    my $logdir = getconf($CNF_LOGDIR);
    my @matches = File::Glob::bsd_glob("$logdir/log.$write_timestamp.*", GLOB_NOSORT);
    if ($write_timestamp =~ /000000$/) {
	my $write_datestamp = substr($write_timestamp, 0, 8);
	push @matches, File::Glob::bsd_glob("$logdir/log.$write_datestamp.*", GLOB_NOSORT);
    }

    for my $lf (@matches) {
	open(my $fh, "<", $lf) or next;
	while (<$fh>) {
	    # amflush and amvault put their own names in
	    return $1 if (/^START (amflush|amvault)/);
	    # but for amdump we see planner
	    return 'amdump' if (/^START planner/);
	}
    }

    return "unknown";
}


# this generic function implements the loop of scanning logfiles to find
# the requested data; get_parts and get_dumps then adjust the results to
# match what the user expects.
sub get_parts_and_dumps {
    my $get_what = shift; # "parts" or "dumps"
    my %params = @_;
    my $logfile_dir = config_dir_relative(getconf($CNF_LOGDIR));

    # find_log assumes that the tapelist has been loaded, so load it now
    _load_tapelist();

    # pre-process params by appending all of the "singular" parameters to the "plurals"
    push @{$params{'write_timestamps'}}, map { zeropad($_) } $params{'write_timestamp'} 
	if exists($params{'write_timestamp'});
    push @{$params{'dump_timestamps'}}, map { zeropad($_) } $params{'dump_timestamp'} 
	if exists($params{'dump_timestamp'});
    push @{$params{'hostnames'}}, $params{'hostname'} 
	if exists($params{'hostname'});
    push @{$params{'disknames'}}, $params{'diskname'} 
	if exists($params{'diskname'});
    push @{$params{'levels'}}, $params{'level'} 
	if exists($params{'level'});
    if ($get_what eq 'parts') {
	push @{$params{'labels'}}, $params{'label'} 
	    if exists($params{'label'});
    } else {
	delete $params{'labels'};
    }

    # specifying write_timestamps implies we won't check holding files
    if ($params{'write_timestamps'}) {
	if (defined $params{'holding'} and $params{'holding'}) {
	    return [], []; # well, that's easy..
	}
	$params{'holding'} = 0;
    }

    # Since we're working from logfiles, we have to pick the logfiles we'll use first.
    # Then we can use search_logfile.
    my @logfiles;
    if ($params{'holding'}) {
	@logfiles = ( 'holding', );
    } elsif (exists($params{'write_timestamps'})) {
	# if we have specific write_timestamps, the job is pretty easy.
	my %timestamps_hash = map { ($_, undef) } @{$params{'write_timestamps'}};
	for my $logfile (Amanda::Logfile::find_log()) {
	    next unless (my ($timestamp) = $logfile =~ /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/);
	    next unless (exists($timestamps_hash{zeropad($timestamp)}));
	    push @logfiles, $logfile;
	}
    } elsif (exists($params{'dump_timestamps'})) {
	# otherwise, we need only look in logfiles at or after the earliest dump timestamp
	my @sorted_timestamps = sort @{$params{'dump_timestamps'}};
	my $earliest_timestamp = $sorted_timestamps[0];
	for my $logfile (Amanda::Logfile::find_log()) {
	    next unless (my ($timestamp) = $logfile =~ /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/);
	    next unless (zeropad($timestamp) ge $earliest_timestamp);
	    push @logfiles, $logfile;
	}
    } else {
	# oh well -- it looks like we'll have to read all existing logfiles.
	@logfiles = Amanda::Logfile::find_log();
    }

    # Set up some hash tables for speedy lookups of various attributes
    my (%dump_timestamps_hash, %hostnames_hash, %disknames_hash, %levels_hash, %labels_hash);
    %dump_timestamps_hash = map { ($_, undef) } @{$params{'dump_timestamps'}}
	if (exists($params{'dump_timestamps'}));
    %hostnames_hash = map { ($_, undef) } @{$params{'hostnames'}}
	if (exists($params{'hostnames'}));
    %disknames_hash = map { ($_, undef) } @{$params{'disknames'}}
	if (exists($params{'disknames'}));
    %levels_hash = map { ($_, undef) } @{$params{'levels'}}
	if (exists($params{'levels'}));
    %labels_hash = map { ($_, undef) } @{$params{'labels'}}
	if (exists($params{'labels'}));

    my %dumps;
    my @parts;

    # *also* scan holding if the holding param wasn't specified
    if (!exists $params{'holding'}) {
	push @logfiles, 'holding';
    }

    # now loop over those logfiles and use search_logfile to load the dumpfiles
    # from them, then process each entry from the logfile
    for my $logfile (@logfiles) {
	my (@find_results, $write_timestamp);

	# get the raw contents from search_logfile, or use holding if
	# $logfile is undef
	if ($logfile ne 'holding') {
	    @find_results = Amanda::Logfile::search_logfile(undef, undef,
							"$logfile_dir/$logfile", 1);
	    # convert to dumpfile hashes, including the write_timestamp from the logfile name
	    my ($timestamp) = $logfile =~ /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/;
	    $write_timestamp = zeropad($timestamp);

	} else {
	    @find_results = Amanda::Logfile::search_holding_disk();
	    $write_timestamp = '00000000000000';
	}

	# filter against *_match with dumps_match
	@find_results = Amanda::Logfile::dumps_match([@find_results],
	    exists($params{'hostname_match'})? $params{'hostname_match'} : undef,
	    exists($params{'diskname_match'})? $params{'diskname_match'} : undef,
	    exists($params{'dump_timestamp_match'})? $params{'dump_timestamp_match'} : undef,
	    undef,
	    0);

	# loop over each entry in the logfile.
	for my $find_result (@find_results) {

	    # filter out the non-dump error messages that find.c produces
	    next unless (defined $find_result->{'label'});

	    # bail out on this result early, if possible
	    next if (%dump_timestamps_hash 
		and !exists($dump_timestamps_hash{zeropad($find_result->{'timestamp'})}));
	    next if (%hostnames_hash 
		and !exists($hostnames_hash{$find_result->{'hostname'}}));
	    next if (%disknames_hash 
		and !exists($disknames_hash{$find_result->{'diskname'}}));
	    next if (%levels_hash 
		and !exists($levels_hash{$find_result->{'level'}}));
	    next if (%labels_hash 
		and !exists($labels_hash{$find_result->{'label'}}));
	    if ($get_what eq 'parts') {
		next if (exists($params{'status'}) 
		    and defined $find_result->{'status'}
		    and $find_result->{'status'} ne $params{'status'});
	    }

	    # filter each result against dumpspecs, to avoid dumps_match_dumpspecs'
	    # tendency to produce duplicate results
	    next if ($params{'dumpspecs'}
		and !Amanda::Logfile::dumps_match_dumpspecs([$find_result],
						    $params{'dumpspecs'}, 0));

	    my $dump_timestamp = zeropad($find_result->{'timestamp'});

	    my $dumpkey = join("\0", $find_result->{'hostname'}, $find_result->{'diskname'},
			             $write_timestamp, $find_result->{'level'}, $dump_timestamp);
	    my $dump = $dumps{$dumpkey};
	    if (!defined $dump) {
		$dump = $dumps{$dumpkey} = {
		    dump_timestamp => $dump_timestamp,
		    write_timestamp => $write_timestamp,
		    hostname => $find_result->{'hostname'},
		    diskname => $find_result->{'diskname'},
		    level => $find_result->{'level'}+0,
		    orig_kb => $find_result->{'orig_kb'},
		    status => $find_result->{'dump_status'},
		    message => $find_result->{'message'},
		    # the rest of these params are unknown until we see a taper
		    # DONE, PARTIAL, or FAIL line, although we count nparts
		    # manually instead of relying on the logfile
		    nparts => 0, # $find_result->{'totalparts'}
		    bytes => -1, # $find_result->{'bytes'}
		    kb => -1,    # $find_result->{'kb'}
		    sec => -1,   # $find_result->{'sec'}
		};
	    }

	    # start setting up a part hash for this result
	    my %part;
	    if ($logfile ne 'holding') {
		# on-media dump
		%part = (
		    label => $find_result->{'label'},
		    filenum => $find_result->{'filenum'},
		    dump => $dump,
		    status => $find_result->{'status'} || 'FAILED',
		    sec => $find_result->{'sec'},
		    kb => $find_result->{'kb'},
		    orig_kb => $find_result->{'orig_kb'},
		    partnum => $find_result->{'partnum'},
		);
	    } else {
		# holding disk
		%part = (
		    holding_file => $find_result->{'label'},
		    dump => $dump,
		    status => $find_result->{'status'} || 'FAILED',
		    sec => 0.0,
		    kb => $find_result->{'kb'},
		    orig_kb => $find_result->{'orig_kb'},
		    partnum => 1,
		);
		# and fix up the dump, too
		$dump->{'status'} = $find_result->{'status'} || 'FAILED';
		$dump->{'bytes'} = $find_result->{'bytes'};
		$dump->{'kb'} = $find_result->{'kb'};
		$dump->{'sec'} = $find_result->{'sec'};
	    }

	    # weaken the dump ref if we're returning dumps
	    weaken_ref($part{'dump'})
		if ($get_what eq 'dumps');

	    # count the number of successful parts in the dump
	    $dump->{'nparts'}++ if $part{'status'} eq 'OK';
	    
	    # and add a ref to the array of parts; if we're getting
	    # parts, then this is a weak ref
	    $dump->{'parts'}[$part{'partnum'}] = \%part;
	    weaken_ref($dump->{'parts'}[$part{'partnum'}])
		if ($get_what eq 'parts');

	    push @parts, \%part;
	}

	# if these dumps were on the holding disk, then we're done
	next if $logfile eq 'holding';

	# re-read the logfile to extract dump-level info that's not captured by
	# search_logfile
	my $logh = Amanda::Logfile::open_logfile("$logfile_dir/$logfile");
	die "logfile '$logfile' not found" unless $logh;
	while (my ($type, $prog, $str) = Amanda::Logfile::get_logline($logh)) {
	    next unless $prog == $P_TAPER;
	    my $status;
	    if ($type == $L_DONE) {
		$status = 'OK';
	    } elsif ($type == $L_PARTIAL) {
		$status = 'PARTIAL';
	    } elsif ($type == $L_FAIL) {
		$status = 'FAIL';
	    } elsif ($type == $L_SUCCESS) {
		$status = "OK";
	    } else {
		next;
	    }

	    # now extract the appropriate info; luckily these log lines have the same
	    # format, more or less
	    my ($hostname, $diskname, $dump_timestamp, $nparts, $level, $secs, $kb, $bytes, $message);
	    ($hostname, $str) = Amanda::Util::skip_quoted_string($str);
	    ($diskname, $str) = Amanda::Util::skip_quoted_string($str);
	    ($dump_timestamp, $str) = Amanda::Util::skip_quoted_string($str);
	    if ($status ne 'FAIL' and $type != $L_SUCCESS) { # nparts is not in SUCCESS lines
		($nparts, my $str1) = Amanda::Util::skip_quoted_string($str);
		if (substr($str1, 0,1) ne '[') {
		    $str = $str1;
		} else { # nparts is not in all PARTIAL lines
		    $nparts = 0;
		}
		
	    } else {
		$nparts = 0;
	    }
	    ($level, $str) = Amanda::Util::skip_quoted_string($str);
	    if ($status ne 'FAIL') {
		my $s = $str;
		my $b_unit;
		($secs, $b_unit, $kb, $str) = ($str =~ /^\[sec ([-0-9.]+) (kb|bytes) ([-0-9]+).*\] ?(.*)$/)
		    or die("'$s'");
		if ($b_unit eq 'bytes') {
		    $bytes = $kb;
		    $kb /= 1024;
		} else {
		    $bytes = 0;
		}
		$secs = 0.1 if ($secs <= 0);
	    }
	    if ($status ne 'OK') {
		$message = $str;
	    } else {
		$message = '';
	    }

	    $hostname = Amanda::Util::unquote_string($hostname);
	    $diskname = Amanda::Util::unquote_string($diskname);
	    $message = Amanda::Util::unquote_string($message) if $message;

	    # filter against dump criteria
	    next if ($params{'dump_timestamp_match'}
		and !match_datestamp($params{'dump_timestamp_match'}, zeropad($dump_timestamp)));
	    next if (%dump_timestamps_hash 
		and !exists($dump_timestamps_hash{zeropad($dump_timestamp)}));

	    next if ($params{'hostname_match'}
		and !match_host($params{'hostname_match'}, $hostname));
	    next if (%hostnames_hash 
		and !exists($hostnames_hash{$hostname}));

	    next if ($params{'diskname_match'}
		and !match_disk($params{'diskname_match'}, $diskname));
	    next if (%disknames_hash 
		and !exists($disknames_hash{$diskname}));

	    next if (%levels_hash 
		and !exists($levels_hash{$level}));
	    # get_dumps filters on status

	    if ($params{'dumpspecs'}) {
		my $ok = 0;
		for my $ds (@{$params{'dumpspecs'}}) {
		    # (the "". are for SWIG's benefit - SWIGged functions don't like
		    # strings generated by SWIG.  Long story.)
		    next if (defined $ds->{'host'}
			    and !match_host("".$ds->{'host'}, $hostname));
		    next if (defined $ds->{'disk'}
			    and !match_disk("".$ds->{'disk'}, $diskname));
		    next if (defined $ds->{'datestamp'}
			    and !match_datestamp("".$ds->{'datestamp'}, $dump_timestamp));
		    next if (defined $ds->{'level'}
			    and !match_level("".$ds->{'level'}, $level));
		    next if (defined $ds->{'write_timestamp'}
			     and !match_datestamp("".$ds->{'write_timestamp'}, $write_timestamp));
		    $ok = 1;
		    last;
		}
		next unless $ok;
	    }

	    my $dumpkey = join("\0", $hostname, $diskname, $write_timestamp,
				     $level, zeropad($dump_timestamp));
	    my $dump = $dumps{$dumpkey};
	    if (!defined $dump) {
		# this will happen when a dump has no parts - a FAILed dump.
		$dump = $dumps{$dumpkey} = {
		    dump_timestamp => zeropad($dump_timestamp),
		    write_timestamp => $write_timestamp,
		    hostname => $hostname,
		    diskname => $diskname,
		    level => $level+0,
		    orig_kb => undef,
		    status => "FAILED",
		    # message set below
		    nparts => $nparts, # hopefully 0?
		    # kb set below
		    # sec set below
		};
	    }

	    $dump->{'message'} = $message;
	    if ($status eq 'FAIL') {
		$dump->{'bytes'} = 0;
		$dump->{'kb'} = 0;
		$dump->{'sec'} = 0.0;
	    } else {
		$dump->{'bytes'} = $bytes+0;
		$dump->{'kb'} = $kb+0;
		$dump->{'sec'} = $secs+0.0;
	    }
	}
	Amanda::Logfile::close_logfile($logh);
    }

    return [ values %dumps], \@parts;
}

sub get_parts {
    my ($dumps, $parts) = get_parts_and_dumps("parts", @_);
    return @$parts;
}

sub get_dumps {
    my %params = @_;
    my ($dumps, $parts) = get_parts_and_dumps("dumps", @_);
    my @dumps = @$dumps;

    if (exists $params{'status'}) {
	@dumps = grep { $_->{'status'} eq $params{'status'} } @dumps;
    }

    return @dumps;
}

sub sort_parts {
    my ($keys, @parts) = @_;

    # TODO: make this more efficient by selecting the comparison
    # functions once, in advance, and just applying them
    return sort {
	my $res;
	for my $key (@$keys) {
	    my ($rev, $k) = ($key =~ /^(-?)(.*)$/);

	    if ($k =~ /^(partnum|filenum)$/) {
		# compare part components numerically
		$res = $a->{$k} <=> $b->{$k};
	    } elsif ($k =~ /^(nparts|level)$/) {
		# compare dump components numerically
		$res = $a->{'dump'}->{$k} <=> $b->{'dump'}->{$k};
	    } elsif ($k =~ /^(hostname|diskname|write_timestamp|dump_timestamp)$/) {
		# compare dump components alphabetically
		$res = $a->{'dump'}->{$k} cmp $b->{'dump'}->{$k};
	    } else { # (label)
		# compare part components alphabetically
		$res = $a->{$k} cmp $b->{$k};
	    }
	    $res = -$res if ($rev eq '-' and $res);
	    return $res if $res;
	}
	return 0;
    } @parts;
}

sub sort_dumps {
    my ($keys, @dumps) = @_;

    # TODO: make this more efficient by selecting the comparison
    # functions once, in advance, and just applying them
    return sort {
	my $res;
	for my $key (@$keys) {
	    my ($rev, $k) = ($key =~ /^(-?)(.*)$/);

	    if ($k =~ /^(nparts|level|filenum)$/) {
		# compare dump components numerically
		$res = $a->{$k} <=> $b->{$k};
	    } else { # ($k =~ /^(hostname|diskname|write_timestamp|dump_timestamp)$/)
		# compare dump components alphabetically
		$res = $a->{$k} cmp $b->{$k};
	    } 
	    $res = -$res if ($rev eq '-' and $res);
	    return $res if $res;
	}
	return 0;
    } @dumps;
}

# caches for add_part() to avoid repeatedly looking up the log
# filename for a particular write_timestamp.
my $add_part_last_label = undef;
my $add_part_last_write_timestamp = undef;
my $add_part_last_logfile = undef;

sub add_part {
    my ($dump) = @_;
    my $found;
    my $logfh;
    my $logfile;
    my $find_result;
    my $logdir = getconf($CNF_LOGDIR);
    my ($last_filenum, $last_secs, $last_kbs);

    # first order of business is to find out whether we need to make a new
    # dumpfile for this.
    my $write_timestamp = zeropad($dump->{'write_timestamp'});
    die "dump has no 'write_timestamp'" unless defined $write_timestamp;

    # consult our one-element cache for this label and write_timestamp
    if (!defined $add_part_last_label
	or $add_part_last_label ne $dump->{'label'}
	or $add_part_last_write_timestamp ne $dump->{'write_timestamp'}) {

	# update the cache
	$add_part_last_logfile = undef;
	LOGFILE:
	for my $lf (Amanda::Logfile::find_log()) {
	    next unless (my ($log_timestamp) = $lf =~ /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/);
	    next unless (zeropad($log_timestamp) eq $write_timestamp);

	    # write timestamp matches; now check the label
	    LOGFILE_DUMP:
	    for $find_result (Amanda::Logfile::search_logfile(undef, undef,
					"$logdir/$lf", 1)) {
		next unless (defined $find_result->{'label'});

		if ($find_result->{'label'} eq $dump->{'label'}) {
		    $add_part_last_label = $dump->{'label'};
		    $add_part_last_write_timestamp = $dump->{'write_timestamp'};
		    $add_part_last_logfile = $lf;
		    last LOGFILE;
		}
	    }
	}
    }
    $logfile = $add_part_last_logfile;

    # truncate the write_timestamp if we're not using timestamps
    if (!getconf($CNF_USETIMESTAMPS)) {
	$write_timestamp = substr($write_timestamp, 0, 8);
    }

    # get the information on the last dump and part in this logfile, or create
    # a new logfile if none exists, then open the logfile for writing.
    if (defined $logfile) {
	$last_filenum = -1;

	# NOTE: this depends on an implementation detail of search_logfile: it
	# returns the results in the reverse order of appearance in the logfile.
	# Since we're concerned with the last elements of this logfile that we
	# will be appending to shortly, we simply reverse this list.  As this
	# package is rewritten to parse logfiles on its own (or access a relational
	# database), this implementation detail will no longer be relevant.
	my @find_results = reverse Amanda::Logfile::search_logfile(undef, undef,
						    "$logdir/$logfile", 1);
	for $find_result (@find_results) {
	    # filter out the non-dump error messages that find.c produces
	    next unless (defined $find_result->{'label'});

	    $last_filenum = $find_result->{'filenum'};

	    # if this is part number 1, reset our secs and kbs counters on the
	    # assumption that this is the beginning of a new dump
	    if ($find_result->{'partnum'} == 1) {
		$last_secs = $last_kbs = 0;
	    }
	    $last_secs += $find_result->{'sec'};
	    $last_kbs += $find_result->{'kb'};
	}

	open($logfh, ">>", "$logdir/$logfile");
    } else {
	$last_filenum = -1;
	$last_secs = 0;
	$last_kbs = 0;

	# pick an unused log filename
	my $i = 0;
	while (1) {
	    $logfile = "log.$write_timestamp.$i";
	    last unless -f "$logdir/$logfile";
	    $i++;
	}

	open($logfh, ">", "$logdir/$logfile")
	    or die("Could not write '$logdir/$logfile': $!");

	print $logfh
	    "INFO taper This logfile was generated by Amanda::DB::Catalog\n";

	print $logfh
	    "START taper datestamp $write_timestamp label $dump->{label} tape $i\n";

	if (!defined $tapelist) {
	    _load_tapelist();
	} else {
	    # reload the tapelist immediately, in case it's been modified
	    $tapelist->reload();
	}

	# see if we need to add an entry to the tapelist for this dump
	if (!grep { $_->{'label'} eq $dump->{'label'}
		    and zeropad($_->{'datestamp'}) eq zeropad($dump->{'write_timestamp'})
		} @{$tapelist->{tles}}) {
	    $tapelist->reload(1);
	    $tapelist->add_tapelabel($write_timestamp, $dump->{'label'}, undef, 1);
	    $tapelist->write();
	}
    }

    if ($last_filenum >= 0 && $last_filenum+1 != $dump->{'filenum'}) {
	warn "Discontinuity in filenums in $logfile: " .
	     "from $last_filenum to $dump->{filenum}";
    }

    my $kps = $dump->{'sec'}? (($dump->{'kb'} + 0.0) / $dump->{'sec'}) : 0.0;

    my $part_line = "PART taper ";
    $part_line .= "$dump->{label} ";
    $part_line .= "$dump->{filenum} ";
    $part_line .= quote_string($dump->{hostname}) . " ";
    $part_line .= quote_string($dump->{diskname}) . " ";
    $part_line .= "$dump->{dump_timestamp} ";
    $part_line .= "$dump->{partnum}/$dump->{nparts} ";
    $part_line .= "$dump->{level} ";
    $part_line .= "[sec $dump->{sec} kb $dump->{kb} kps $kps]";
    print $logfh "$part_line\n";

    # TODO: we don't always know nparts when writing a part, so
    # this is not always an effective way to detect a complete dump.
    # However, it works for purposes of data vaulting.
    if ($dump->{'partnum'} == $dump->{'nparts'}) {
	my $secs = $last_secs + $dump->{'sec'};
	my $kbs = $last_kbs + $dump->{'kb'};
	$kps = $secs? ($kbs + 0.0) / $secs : 0.0;

	my $done_line = "DONE taper ";
	$done_line .= quote_string($dump->{hostname}) ." ";
	$done_line .= quote_string($dump->{diskname}) ." ";
	$done_line .= "$dump->{dump_timestamp} ";
	$done_line .= "$dump->{nparts} ";
	$done_line .= "$dump->{level} ";
	$done_line .= "[sec $secs kb $kbs kps $kps]";
	print $logfh "$done_line\n";
    }

    close($logfh);
}

sub _load_tapelist {
    if (!defined $tapelist) {
	my $tapelist_filename = config_dir_relative(getconf($CNF_TAPELIST));
	$tapelist = Amanda::Tapelist->new($tapelist_filename);
    }
}

sub _clear_cache { # (used by installcheck)
    $tapelist = undef;
}

1;
