# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

=item label

(string) -- volume label

=item filenum

(integer) -- file on that volume

=item dump_timestamp

(string) -- timestamp of the run in which the dump was created

=item write_timestamp

(string) -- timestamp of the run in which the dump was written to this volume

=item hostname

(string) -- dump hostname

=item diskname

(string) -- dump diskname

=item level

(integer) -- dump level

=item status

(string) -- "OK", "PARTIAL" or some other descriptor

=item partnum

(integer) -- part number of a split dump (1-based)

=item nparts

(integer) -- number of parts in this dump (estimated)

=item kb

(integer) -- size (in kb) of this dumpfile

=item sec

(integer) -- time (in seconds) spent writing this dumpfile

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

=item write_timestamp

restrict to dumpfiles written at this timestamp

=item write_timestamps

(arrayref) restrict to dumpfiles written at any of these timestamps

=item dump_timestamp

restrict to dumpfiles with exactly this timestamp

=item dump_timestamps

(arrayref) restrict to dumpfiles with any of these timestamps

=item dump_timestamp_match

restrict to dumpfiles with timestamps matching this expression

=item hostname

restrict to dumpfiles with exactly this hostname

=item hostnames

(arrayref) restrict to dumpfiles with any of these hostnames

=item hostname_match

restrict to dumpfiles with hostnames matching this expression

=item diskname

restrict to dumpfiles with exactly this diskname

=item disknames

(arrayref) restrict to dumpfiles with any of these disknames

=item diskname_match

restrict to dumpfiles with disknames matching this expression

=item label

restrict to dumpfiles with exactly this label

=item labels

(arrayref) restrict to dumpfiles with any of these labels

=item level

restrict to dumpfiles with exactly this level

=item levels

(arrayref) restrict to dumpfiles with any of these levels

=item status

restrict to dumpfiles with this status

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

=item kb

=item sec

=back

Keys are processed from left to right: if two dumps have the same value for
C<$key1>, then C<$key2> is examined, and so on.  Key names may be prefixed by
a dash (C<->) to reverse the order.

=item add_dump($dumpfile)

Add the given dumpfile to the database.  In terms of logfiles, this will either
create a new logfile (if the dump's C<write_timestamp> has not been seen
before) or append to an existing logfile.  Note that a new logfile will require
a corresponding new entry in the tapelist.

Note that no locking is performed: multiple simultaneous calls to this function
can result in a corrupted or incorrect logfile.

=back

=head1 API STATUS

New summary functions may be added to reduce code duplication in other parts of
Amanda.

Support for loading and modifying the tapelist may eventually be folded into
this module.

=cut

use Amanda::Logfile;
use Amanda::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( quote_string );
use warnings;
use strict;

# tapelist cache
my $tapelist = undef;
my $tapelist_filename = undef;

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
	    $dumpfile{'partnum'} = $find_result->{'partnum'};
	    $dumpfile{'nparts'} = $find_result->{'totalparts'};

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
	my $res;
	for my $key (@$keys) {
	    my ($rev, $k) = ($key =~ /^(-?)(.*)$/);

	    if ($k =~ /^(kb|nparts|partnum|filenum|level)$/) {
		# compare numerically
		$res = $a->{$k} <=> $b->{$k};
	    } else {
		$res = $a->{$k} cmp $b->{$k};
	    }
	    $res = -$res if ($rev eq '-' and $res);
	    return $res if $res;
	}
	return 0;
    } @dumps;
}

# caches for add_dump() to avoid repeatedly looking up the log
# filename for a particular write_timestamp.
my $add_dump_last_label = undef;
my $add_dump_last_write_timestamp = undef;
my $add_dump_last_logfile = undef;

sub add_dump {
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
    if (!defined $add_dump_last_label
	or $add_dump_last_label ne $dump->{'label'}
	or $add_dump_last_write_timestamp ne $dump->{'write_timestamp'}) {

	# update the cache
	$add_dump_last_logfile = undef;
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
		    $add_dump_last_label = $dump->{'label'};
		    $add_dump_last_write_timestamp = $dump->{'write_timestamp'};
		    $add_dump_last_logfile = $lf;
		    last LOGFILE;
		}
	    }
	}
    }
    $logfile = $add_dump_last_logfile;

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

	if (!defined $tapelist_filename) {
	    $tapelist_filename = config_dir_relative(getconf($CNF_TAPELIST));
	}

	# reload the tapelist immediately, in case it's been modified
	$tapelist = Amanda::Tapelist::read_tapelist($tapelist_filename);

	# see if we need to add an entry to the tapelist for this dump
	if (!grep { $_->{'label'} eq $dump->{'label'}
		    and zeropad($_->{'datestamp'}) eq zeropad($dump->{'write_timestamp'})
		} @$tapelist) {
	    $tapelist->add_tapelabel($write_timestamp, $dump->{'label'});
	    $tapelist->write($tapelist_filename);
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
	$tapelist_filename = config_dir_relative(getconf($CNF_TAPELIST));
	$tapelist = Amanda::Tapelist::read_tapelist($tapelist_filename);
    }
}

sub _clear_cache { # (used by installcheck)
    $tapelist = $tapelist_filename = undef;
}

1;
