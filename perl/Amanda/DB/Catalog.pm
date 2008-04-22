# Copyright (c) 2006 Zmanda Inc.  All Rights Reserved.
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
      my @dumpfiles = Amanda::DB::Catalog::get_dumps(
	  timestamp => $timestamp,
	  ok => 1
      );
      print "$timstamp:\n";
      for my $dumpfile (@dumpfiles) {
	  print " ", $dumpfile->{hostname}, ":", $dumpfile->{diskname}, 
		" level ", $dumpfile->{level}, "\n";
      }
  }
    
=head1 DESCRIPTION

=head2 MODEL

The Amanda catalog is a set of dumpfiles, where each dumpfile corresponds to a
single file in a storage volume.  On tapes, files are separated by filemarks
and numbered sequentially.  This model is preserved on non-tape media such as
the VFS and S3 devices.  A dumpfile, then, is completely specified by a volume
label and a file number (I<filenum>).

The catalog is presented as a single table containing one row per dumpfile.
Each row has the following values:

=over

=item label (string) -- volume label

=item filenum (integer) -- file on that volume

=item dump_timestamp (string) -- timestamp of the run in which the dump was created

=item write_timestamp (string) -- timestamp of the run in which the dump was written to this volume

=item hostname (string) -- dump hostname

=item diskname (string) -- dump diskname

=item level (integer) -- dump level

=item status (string) -- "OK", "PARTIAL" or some other descriptor

=item partnum (integer) -- part number of a split dump (1-based)

=item nparts (integer) -- number of parts in this dump (estimated)

=back

A dumpfile is represented as a hashref with these keys.

The label and filenum serve as a primary key.  The dump_timestamp, hostname,
diskname, and level uniquely identify the dump.  The write_timestamp gives the
time that the dump was written to this volume.  The write_timestamp may differ
from the dump_timestamp if, for example, I<amflush> wrote the dump to tape
after the initial dump.  The remaining fields are informational.

=head2 NOTES

A dumpfile may be a part of a larger (split) dump, or may be partial (due to
end of tape or some other error), so the contents of the catalog require some
interpretation in order to find a particular dump.

All timestamps used in this module are full-length, in the format
C<YYYYMMDDHHMMSS>.  If the underlying data contains only datestamps, they are
zero-extended into timestamps: C<YYYYMMDD000000>.  A dump_timestamp always
corresponds to the initiation of the I<original> dump run, while
write_timestamp gives the time the file was written to the volume.  When
dumpfiles are migrated from volume to volume (e.g., by I<amflush>), the
dump_timestamp does not change.  

In Amanda, the tuple (hostname, diskname, level, dump_timestamp) serves as a unique
identifier for a dump.  Since all of this information is preserved during
migrations, a catalog query with these four terms will return all dumpfiles
relevant to that dump.

=head2 QUERIES

NOTE: the tapelist must be loaded before using this module (see
L<Amanda::Tapelist>).

This API is read-only at the moment.  The following functions are available:

=over

=item get_write_timestamps()

Get a list of all write timestamps, sorted in chronological order.

=item get_latest_write_timestamp()

Return the most recent write timestamp.

=item get_labels_written_at_timestamp($ts)

Return a list of labels for volumes written at the given timestamp.

=item get_dumps(%parameters)

This function is the workhorse query interface, and returns a sequence of
dumpfiles.  Values in C<%parameters> restrict the set of dumpfiles that are
returned.  The hash can have any of the following keys:

=over

=item write_timestamp -- restrict to dumpfiles written at this timestamp

=item write_timestamps -- (arrayref) restrict to dumpfiles written at any of these timestamps

=item dump_timestamp -- restrict to dumpfiles with exactly this timestamp

=item dump_timestamps -- (arrayref) restrict to dumpfiles with any of these timestamps

=item dump_timestamp_match -- restrict to dumpfiles with timestamps matching this expression

=item hostname -- restrict to dumpfiles with exactly this hostname

=item hostnames -- (arrayref) restrict to dumpfiles with any of these hostnames

=item hostname_match -- restrict to dumpfiles with hostnames matching this expression

=item diskname -- restrict to dumpfiles with exactly this diskname

=item disknames -- (arrayref) restrict to dumpfiles with any of these disknames

=item diskname_match -- restrict to dumpfiles with disknames matching this expression

=item label -- restrict to dumpfiles with exactly this label

=item labels -- (arrayref) restrict to dumpfiles with any of these labels

=item level -- restrict to dumpfiles with exactly this level

=item levels -- (arrayref) restrict to dumpfiles with any of these levels

=item status -- restrict to dumpfiles with this status

=back

Match expressions are described in the amanda(8) manual page.

=item sort_dumps([ $key1, $key2, .. ], @dumps)

Given a list of dumps, this function sorts that list by the requested keys.
The following keys are available:

=over

=item hostname

=item diskname

=item write_timestamp

=item dump_timestamp

=item level

=item filenum

=item label

=item partnum

=back

Keys are processed from left to right: if two dumps have the same value for
C<$key1>, then C<$key2> is examined, and so on.  Key names may be prefixed by
"C<->" to reverse the order.

=back

=head1 API STATUS

New summary functions may be added to reduce code duplication in other parts of
Amanda.

=cut

use Amanda::Logfile;
use Amanda::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use warnings;
use strict;

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

    for (Amanda::Logfile::find_log()) {
	next unless (my ($timestamp) = /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/);
	push @rv, zeropad($timestamp);
    }

    return sort @rv;
}

sub get_latest_write_timestamp {
    # get all of the timestamps and select the last one
    my @timestamps = get_write_timestamps();

    if (@timestamps) {
	return $timestamps[-1];
    }

    return undef;
}

sub get_dumps {
    my %params = @_;
    my $logfile_dir = config_dir_relative(getconf($CNF_LOGDIR));

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
    push @{$params{'labels'}}, $params{'label'} 
	if exists($params{'label'});

    # Since we're working from logfiles, we have to pick the logfiles we'll use first.
    # Then we can use search_logfile.
    my @logfiles;
    if (exists($params{'write_timestamps'})) {
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

    # now loop over those logfiles and use search_logfile to load the dumpfiles
    # from them, then process each entry from the logfile
    my @results;
    for my $logfile (@logfiles) {
	# get the raw contents from search_logfile
	my @find_results = Amanda::Logfile::search_logfile(undef, undef,
						    "$logfile_dir/$logfile", 1);

	# filter against *_match with dumps_match
	@find_results = Amanda::Logfile::dumps_match([@find_results],
	    exists($params{'hostname_match'})? $params{'hostname_match'} : undef,
	    exists($params{'diskname_match'})? $params{'diskname_match'} : undef,
	    exists($params{'dump_timestamp_match'})? $params{'dump_timestamp_match'} : undef,
	    undef,
	    0);

	# convert to dumpfile hashes, including the write_timestamp from the logfile name
	my ($timestamp) = $logfile =~ /^log\.([0-9]+)(?:\.[0-9]+|\.amflush)?$/;
	my $write_timestamp = zeropad($timestamp);

	# loop over each entry in the logfile.
	for my $find_result (@find_results) {
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
	    next if (exists($params{'status'}) 
		and $find_result->{'status'} ne $params{'status'});

	    # start setting up a dumpfile hash for this result
	    my %dumpfile = (
		'write_timestamp' => $write_timestamp,
		'dump_timestamp' => zeropad($find_result->{'timestamp'}),
		'hostname' => $find_result->{'hostname'},
		'diskname' => $find_result->{'diskname'},
		'level' => $find_result->{'level'},
		'label' => $find_result->{'label'},
		'filenum' => $find_result->{'filenum'},
		'status' => $find_result->{'status'},
		'sec' => $find_result->{'sec'},
		'kb' => $find_result->{'kb'},
	    );

	    # partnum and nparts takes some special interpretation
	    if (my ($partnum, $nparts) = $find_result->{'partnum'} =~ m$(\d+)/(-?\d+)$) {
		$dumpfile{'partnum'} = $partnum+0;
		$dumpfile{'nparts'} = $nparts+0;
	    } else {
		$dumpfile{'partnum'} = 1;
		$dumpfile{'nparts'} = 1;
	    }

	    # check partnum and nparts
	    next if (defined($params{'partnum'}) and $dumpfile{'partnum'} != $params{'partnum'});
	    next if (defined($params{'nparts'}) and $dumpfile{'nparts'} != $params{'nparts'});

	    push @results, \%dumpfile;
	}
    }

    return @results;
}

sub sort_dumps {
    my ($keys, @dumps) = @_;

    return sort {
	my $r;
	for my $key (@$keys) {
	    if ($key =~ /^-(.*)$/) {
		$r = $b->{$1} cmp $a->{$1}; # note: $a and $b are reversed
	    } else {
		$r = $a->{$key} cmp $b->{$key};
	    }
	    return $r if $r;
	}
	return 0;
    } @dumps;
}

1;
