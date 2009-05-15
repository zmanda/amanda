# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as
# published by the Free Software Foundation.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Holding;

use base qw( Exporter );
use File::Spec;
use File::stat;
use IO::Dir;
use POSIX qw( :fcntl_h );
use Math::BigInt;
use strict;
use warnings;

use Amanda::Config qw( :getconf );
use Amanda::Debug qw( debug );
use Amanda::Header;
use Amanda::Disklist;
use Amanda::Util;

=head1 NAME

Amanda::Holding -- interface to the holding disks

=head1 SYNOPSIS

    use Amanda::Holding;

Get some statistics:

    my %size_per_host;
    for my $hfile (Amanda::Holding::files()) {
	my $hdr = Amanda::Holding::get_header($hfile);
	next unless $hdr;
	$size_per_host{$hdr->{'name'}} += Amanda::Holding::file_size($hfile);
    }

Schematic for something like C<amflush>:

    for my $ts (sort Amanda::Holding::get_all_timestamps()) {
	print $ts, "\n";
    }
    my @to_dump = <>;
    for my $hfile (Amanda::Holding::get_files_for_flush(@to_dump)) {
	# flush $hfile
    }

=head1 DESCRIPTION

=head2 TERMINOLOGY

=over

=item Holding disk

A holding disk is a directory given in a holdingdisk definition in
C<amanda.conf>.

=item Holding directory

A holding directory is a subdirectory of a holding disk, generally named by
timestamp.  Note, however, that this package does not interpret holding
directory names as timestamps, and does not provide direct access to holding
directories.

=item Holding file

A holding file describes one or more os-level files (holding file chunks) in a
holding directory, together representing a single dump file.

=item Holding chunk

A holding chunk is an individual os-level file representing part of a holding
file.  Chunks are kept small to avoid hitting filesystem size ilmits, and are
linked together internally by filename.

=back

By way of example:

  /data/holding                               <-- holding disk
  /data/holding/20070306123456                <-- holding directory
  /data/holding/20070306123456/raj._video_a   <-- holding file and chunk
  /data/holding/20070306123456/raj._video_a.1 <-- holding chunk

=head2 CONSTANTS

Holding-disk files do not have a block size, so the size of the header is fixed
at 32k.  Rather than hard-code that value, use the constant DISK_BLOCK_BYTES
from this package.

=head2 FUNCTIONS

Note that this package assumes that a config has been loaded (see
L<Amanda::Config>).

These three functions provide basic access to holding disks, files, and chunks:

=over

=item C<disks()>

returns an list of active disks, each represented as a string.  This does not
return holding disks which are defined in C<amanda.conf> but not used.

=item C<files()>

returns a list of active holding files on all disks.  Note that a dump may span
multiple disks, so there is no use in selecting files only on certain holding
disks.

=item C<file_chunks($file)>

returns a list of chunks for the given file.  Chunk filenames are always fully
qualified pathnames.

=back

C<Amanda::Holding> provides a few utility functions on holding files.  Note
that these functions require fully qualified pathnames.

=over

=item C<file_size($file, $ignore_headers)>

returns the size of the holding file I<in kilobytes>, ignoring the size of the
headers if C<$ignore_headers> is true.

=item C<file_unlink($file)>

unlinks (deletes) all chunks comprising C<$file>, returning true on success.

=item C<get_header($file)>

reads and returns the header (see L<Amanda::Header>) for C<$file>.

=back

The remaining two functions are utilities for amflush and related tools:

=over

=item C<get_all_timestamps()>

returns a sorted list of all timestamps with dumps in any active holding disk.

=item C<get_files_for_flush(@timestamps)>

returns a sorted list of files matching any of the supplied timestamps.  Files
for which no DLE exists in the disklist are ignored.  If no timestamps are
provided, then all timestamps are considered.

=back

=cut

use constant DISK_BLOCK_BYTES => 32768;

our @EXPORT_OK = qw(dirs files file_chunks
    get_files_for_flush get_all_datestamps
    file_size file_unlink get_header);

##
# utility subs

sub _is_datestr {
    my ($str) = @_;

    return 0
	unless (my ($year, $month, $day, $hour, $min, $sec) =
	    ($str =~ /(\d{4})(\d{2})(\d{2})(?:(\d{2})(\d{2})(\d{2}))/));

    return 0 if ($year < 1990 || $year > 2999);
    return 0 if ($month < 1 || $month > 12);
    return 0 if ($day < 1 || $day > 31);

    return 0 if (defined $hour and $hour > 23);
    return 0 if (defined $min and $min > 60);
    return 0 if (defined $sec and $sec > 60);

    return 1;
}

sub _walk {
    my ($file_fn) = @_;

    # walk disks, directories, and files with nested loops
    for my $disk (disks()) {
	my $diskh = IO::Dir->new($disk);
	next unless defined $diskh;

	while (defined(my $datestr = $diskh->read())) {
	    next unless (_is_datestr($datestr));

	    my $dirh = IO::Dir->new(File::Spec->catfile($disk, $datestr));
	    while (defined(my $dirent = $dirh->read)) {
		next if $dirent eq '.' or $dirent eq '..';

		my $filename = File::Spec->catfile($disk, $datestr, $dirent);
		next unless -f $filename;

		my $hdr = get_header($filename);
		next unless defined($hdr);

		# ignore chunks and anything bogus
		next if ($hdr->{'type'} != $Amanda::Header::F_DUMPFILE);

		$file_fn->($filename, $hdr);
	    }
	}
    }
}

##
# Package functions

sub disks {
    my @results;

    for my $hdname (@{getconf($CNF_HOLDINGDISK)}) {
	my $cfg = lookup_holdingdisk($hdname);
	next unless defined $cfg;

	my $dir = holdingdisk_getconf($cfg, $HOLDING_DISKDIR);
	next unless defined $dir;
	next unless -d $dir;
	push @results, $dir;
    }

    return @results;
}

sub files {
    my @results;

    my $each_file_fn = sub {
	my ($filename, $header) = @_;
	push @results, $filename;
    };
    _walk($each_file_fn);

    return @results;
}

sub file_chunks {
    my ($filename) = @_;
    my @results;

    while (1) {
	last unless -f $filename;
	my $hdr = get_header($filename);
	last unless defined($hdr);

	push @results, $filename;

	if ($hdr->{'cont_filename'}) {
	    $filename = $hdr->{'cont_filename'};
	} else {
	    # no continuation -> we're done
	    last;
	}
    }

    return @results;
}

sub get_header {
    my ($filename) = @_;
    return unless -f $filename;

    my $fd = POSIX::open($filename, O_RDONLY);
    return unless $fd;

    my $hdr_bytes = Amanda::Util::full_read($fd, DISK_BLOCK_BYTES);
    POSIX::close($fd);
    if (length($hdr_bytes) < DISK_BLOCK_BYTES) {
	return;
    }

    return Amanda::Header->from_string($hdr_bytes);
}

sub file_unlink {
    my ($filename) = @_;

    for my $chunk (file_chunks($filename)) {
	unlink($chunk) or return 0;
    }

    return 1;
}

sub file_size {
    my ($filename, $ignore_headers) = @_;
    my $total = Math::BigInt->new(0);

    for my $chunk (file_chunks($filename)) {
	my $sb = stat($chunk);
	my $size = Math::BigInt->new($sb->size);
	$size -= DISK_BLOCK_BYTES if $ignore_headers;
	$size = ($size + 1023) / 1024;

	$total += $size;
    }

    return $total;
}

sub get_files_for_flush {
    my (@dateargs) = @_;
    my @results;

    my $each_file_fn = sub {
	my ($filename, $header) = @_;
	if (@dateargs && !grep { $_ eq $header->{'datestamp'}; } @dateargs) {
	    return;
	}

	if (!Amanda::Disklist::get_disk($header->{'name'}, $header->{'disk'})) {
	    return;
	}

	push @results, $filename;
    };
    _walk($each_file_fn);

    return sort @results;
}

sub get_all_datestamps {
    my %datestamps;

    my $each_file_fn = sub {
	my ($filename, $header) = @_;
	$datestamps{$header->{'datestamp'}} = 1;
    };
    _walk($each_file_fn);

    return sort keys %datestamps;
}
