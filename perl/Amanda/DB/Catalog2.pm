# Copyright (c) 2011 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::DB::Catalog2;

=head1 NAME

Amanda::DB::Catalog2 - access to the Amanda catalog: where is that dump?

=head1 SYNOPSIS

  use Amanda::DB::Catalog2;

  $catalog = Amanda::DB::Catalog2->new($catalog_conf);
  $catalog->validate();
  $image = $catalog->add_image($host_name, $disk_name, $device, $dump_timestamp,
			       $level, $based_on_timestamp);
  $image = $catalog->find_image($host_name, $disk_name, $device, $dump_timestamp,
				$level);
  $image = $catalog->get_image($image_id);
  $volume = $catalog->add_volume($pool, $label, $write_timestamp, $storage, $meta, $barcode, $block_size);
  $volume = $catalog->find_volume($pool, $label);
  $volume = $catalog->get_volume($copy_id);
  $copy = $image->add_copy($storage_name, $write_timestamp, $retention_days, $retention_full, $retention_recover);
  $copy = $catalog->get_copy($copy_id);
  $image->finish_image($orig_kb, $dump_status, nb_files, nb_directories, $native_crc, $client_crc, $server_crc);
  $part = $copy->add_part($volume, $part_offset, $part_size, $filenum,
			  $part_num, $part_status);
  $copy->finish_copy($nb_parts, $kb, $byte, $copy_status, $server_crc, $copy_message);
  $catalog->rm_volume($pool, $label);
  $catalog->quit();

=head1 MODEL

The Amanda catalog is modeled as a set of dumps comprised of parts.  A dump is
a complete bytestream received from an application, and is uniquely identified
by a C<image_id>, each dump can be written to multiple destination, each
destination have a different copy_id. Each try of a dump get a different
image_id, so it is possible to have to image_id with the same combination
of C<host_name>, C<disk_name>, C<dump_timestamp> and C<level>.  A dump may
be partial, or even a complete failure.

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
or C<"0"> for dumps in the holding disk.

=item host_name

(string) -- dump host_name

=item disk_name

(string) -- dump disk_name

=item level

(integer) -- dump level

=item status

(string) -- The status of the dump - "OK", "PARTIAL", or "FAIL".  If a disk
failed to dump at all, then it is not part of the catalog and thus will not
have an associated dump row.

=item message

(string) -- reason for PARTIAL or FAIL status

=item nb_parts

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

=item allparts

(arrayref) -- list of parts, all parts are included.

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

In Amanda, the tuple (C<host_name>, C<disk_name>, C<level>, C<dump_timestamp>)
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

=item $catalog->get_write_timestamps()

Get a list of all write timestamps, sorted in chronological order.

=item $catalog->get_latest_write_timestamp()

Return the most recent write timestamp.

=item $catalog->get_latest_write_timestamp(type => 'amvault')

=item $catalog->get_latest_write_timestamp(types => [ 'amvault', .. ])

Return the timestamp of the most recent dump of the given type or types.  The
available types are given below for C<get_run_type>.

=item $catalog->get_labels_written_at_timestamp($ts)

Return a list of labels for volumes written at the given timestamp.

=item $catalog->get_run_type($ts)

Return the type of run made at the given timestamp.  The result is one of
C<amvault>, C<amdump>, C<amflush>, or the default, C<unknown>.

=back

=head2 PARTS

=over

=item $catalog->get_parts($dumps, %parameters)

This function returns a sequence of parts.  C<$dumps> is a array ref of
the dump to work with. Values in C<%parameters> restrict
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

=item host_name

restrict to parts with exactly this host_name

=item host_names

(arrayref) restrict to parts with any of these host_names

=item host_name_match

restrict to parts with host_names matching this expression

=item disk_name

restrict to parts with exactly this disk_name

=item disk_names

(arrayref) restrict to parts with any of these disk_names

=item disk_name_match

restrict to parts with disk_names matching this expression

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

=item $catalog->sort_parts([ $key1, $key2, .. ], @parts)

Given a list of parts, this function sorts that list by the requested keys.
The following keys are available:

=over

=item host_name

=item disk_name

=item write_timestamp

=item dump_timestamp

=item level

=item filenum

=item label

Note that this sorts labels I<lexically>, not necessarily in the order they were used!

=item partnum

=item nb_parts

=back

Keys are processed from left to right: if two dumps have the same value for
C<$key1>, then C<$key2> is examined, and so on.  Key names may be prefixed by a
dash (C<->) to reverse the order.

Note that some of these keys are dump keys; the function will automatically
access those values via the C<dump> attribute.

=back

=head2 DUMPS

=over

=item $catalog->get_dumps(%parameters)

This function returns a sequence of dumps.  Values in C<%parameters> restrict
the set of dumps that are returned.  The same keys as are used for C<get_parts>
are available here, with the exception of C<label>, C<labels> and C<holding>.  In this
case, the C<status> parameter applies to the dump status, not the status of its
constituent parts.
If the C<part> is set, get_parts is executed with the resulting dumps, setting the C<parts> and C<allparts> keys.

=item $catalog->sort_dumps([ $key1, $key2 ], @dumps)

Like C<sort_parts>, this sorts a sequence of dumps generated by C<get_dumps>.
The same keys are available, with the exception of C<label>, C<filenum>, and
C<partnum>.

=back

=head2 DATABASE

=over

=item version table

version INTEGER NOT NULL

One row and one column, it is an integer with the version of the database.

=item host table

host_id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
host_name CHAR(256) NOT NULL UNIQUE

=item disks table

disk_id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
host_id INTEGER NOT NULL,
disk_name CHAR(256) NOT NULL,
device CHAR(256) NOT NULL,
UNIQUE (host_id, disk_name),
FOREIGN KEY (host_id) REFERENCES host (host_id)

=item image table

image_id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
disk_id INTEGER NOT NULL,
dump_timestamp CHAR(14) NOT NULL,
level INTEGER NOT NULL,
orig_kb INTEGER,
dump_status VARCHAR(1024),
nb_files INTEGER,
nb_directories INTEGER,
based_on_timestamp CHAR(14),
FOREIGN KEY (disk_id) REFERENCES disks (disk_id)

=item copy table

copy_id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
image_id INTEGER NOT NULL,
write_timestamp CHAR(14) NOT NULL,
nb_parts INTEGER NOT NULL,
kb INTEGER,
bytes INTEGER,
copy_status VARCHAR(1024) NOT NULL,
FOREIGN KEY (image_id) REFERENCES image (image_id)

=item volume table

volume_id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
label CHAR(256) NOT NULL UNIQUE,
write_timestamp CHAR(14) NOT NULL

The label is a full path if it is a holding disk (first character must be '/')

=item part table

part_id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
copy_id INTEGER NOT NULL,
volume_id INTEGER NOT NULL,
part_offset INTEGER NOT NULL,
part_size INTEGER NOT NULL,
filenum INTEGER NOT NULL,
part_num INTEGER NOT NULL,
part_status VARCHAR(1024) NOT NULL,
FOREIGN KEY (copy_id) REFERENCES copy (copy_id),
FOREIGN KEY (volume_id) REFERENCES volume (volume_id)

=back

=cut

use DBI;

use Amanda::Logfile qw( :constants );
use Amanda::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( quote_string weaken_ref :quoting match_disk match_host match_datestamp match_level);
use Amanda::Debug qw( :logging );
use Amanda::DB::Catalog;
use Amanda::DB::Catalog2::log;
use Amanda::Holding;
use Amanda::Index;
use Amanda::Cmdfile;
use File::Glob qw( :glob );
use File::Copy qw( move );
use POSIX qw( strftime );
use warnings;
use strict;

# version of the database
my $DB_VERSION = 5;

sub new {
    my $class = shift;
    my $catalog_conf = shift;
    my %params = @_;
    my $plugin;
    my $catalog_name;

    if (!$catalog_conf) {
	$catalog_name = getconf($CNF_CATALOG);
	if ($catalog_name) {
	    $catalog_conf = lookup_catalog($catalog_name) if $catalog_name;
	} else {
	    $plugin = "log";
	}
    }

    if (!$plugin) {
	$plugin = Amanda::Config::catalog_getconf($catalog_conf,
						  $CATALOG_PLUGIN);
    }
    my $pkgname = "Amanda::DB::Catalog2::" . $plugin;
    my $filename = $pkgname;
    $filename =~ s|::|/|g;
    $filename .= '.pm';
    if (!exists $INC{$filename}) {
	eval "use $pkgname;";
	if ($@) {
	    # handle compile errors
	    die($@) if (exists $INC{$filename});
	    die("No such catalog plugin '$plugin': $@");
	}
    }

    # instantiate it
    my $self = eval {$pkgname->new($catalog_conf, %params);};
    if ($@ || !defined $self) {
	debug("Can't instantiate $pkgname: $@");
	die("Can't instantiate $pkgname: $@");
    }
    $self->{'config_name'} = $params{'config_name'} || Amanda::Config::get_config_name();
    $self->{'catalog_name'} = $catalog_name if !defined $self->{'catalog_name'};

    if ($params{'create'}) {
	$self->{'create_mode'} = 1;
	if ($self->can('_create_table')) {
	    $self->_create_table();
	}
    }

    if ($self->{'tapelist'}) {
	$self->{'tapelist_filename'} = $self->{'tapelist'}->{'filename'};
	$self->{'tapelist_lockname'} = $self->{'tapelist'}->{'lockname'};
	$self->{'tapelist_last_write'} = $self->{'tapelist'}->{'last_write'};
    } else {
	if (!defined $self->{'tapelist_filename'}) {
	    $self->{'tapelist_filename'} = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	}
	$self->{'tapelist_lockname'} = $self->{'tapelist_filename'} . ".lock";
	$self->{'tapelist_last_write'} = $self->{'tapelist_filename'} . ".last_write";
    }

    # config
    if (!$self->{'config_id'} && $self->{'dbh'}) {
	# get/add the config */
	my $sth = $self->{'dbh'}->prepare("SELECT config_id FROM configs where config_name=?")
	    or die "Cannot prepare: " . $self->{'dbh'}->errstr();
	$sth->execute($self->{'config_name'})
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $config_row = $sth->fetchrow_arrayref;
	if (!defined $config_row) {
	    my $sth1 = $self->{'dbh'}->prepare("INSERT INTO configs VALUES (?, ?, ?)")
		or die "Cannot prepare: " . $self->{'dbh'}->errstr();
	    $sth1->execute(undef, $self->{'config_name'}, 0) or die "Can't add config $self->{'config_name'}: " . $sth1->errstr();
	    #$dbh->commit;
	    $self->{'config_id'} = $self->{'dbh'}->last_insert_id(undef, undef, "configs", undef);
	    $sth1->finish();
	} else {
	    $self->{'config_id'} = $config_row->[0];
	}
	$sth->finish();

	# drop temporary tables
	$sth = $self->{'dbh'}->prepare("DROP TABLE IF EXISTS host_ids")
	        or die "Cannot prepare: " . $self->{'dbh'}->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
	$sth->finish;

	$sth = $self->{'dbh'}->prepare("DROP TABLE IF EXISTS disk_ids")
	    or die "Cannot prepare: " . $self->{'dbh'}->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
	$sth->finish;

	$sth = $self->{'dbh'}->prepare("DROP TABLE IF EXISTS image_ids")
	    or die "Cannot prepare: " . $self->{'dbh'}->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
	$sth->finish;

	$sth = $self->{'dbh'}->prepare("DROP TABLE IF EXISTS copy_ids")
	    or die "Cannot prepare: " . $self->{'dbh'}->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
	$sth->finish;
    }

    if ($params{'load'} && $self->{'dbh'}) {
	$self->_load_table();
	$self->_compute_retention();
    }

    my $version = $self->get_version();

    if ($params{'upgrade'} && $self->{'dbh'}) {
	$self->_upgrade_table($version);
	$version = $self->get_version();
    }

    if (!defined $version || $version != $DB_VERSION) {
	print "Version database is '$version', it must be '$DB_VERSION'.\n";
	print "Upgrade the database with the 'amcatalog " . $self->{'config_name'} . " upgrade' command.\n";
	debug("Version database is '$version', it must be '$DB_VERSION'");
	debug("Upgrade the database with the 'amcatalog " . $self->{'config_name'} . " upgrade' command");
	exit 1;
    }

    return $self;
}

sub DESTROY {
    my $self = shift;
    $self->{'in_quit'}=1;
    $self->quit();
}

sub get_DB_VERSION {
    my $self = shift;

    return $DB_VERSION;
}

1;
