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

package Amanda::DB::Catalog2::SQL;

=head1 NAME

Amanda::DB::Catalog2::SQL - generic SQL access to the Amanda catalog

=head1 SYNOPSIS

This package in a generic "SQL" catalog.  See C<amanda-catalog(7)>.

=cut

use strict;
use warnings;
use Amanda::DB::Catalog2;
use base qw( Amanda::DB::Catalog2 );
use POSIX qw( strftime );
use Fcntl;
use Errno;
use File::Copy qw( move );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( quote_string nicedate weaken_ref :quoting match_disk match_host match_datestamp match_level);
use Amanda::Debug qw ( :logging );
use Amanda::Holding;
use Amanda::Header;

my $have_usleep = eval { require Time::HiRes ; 1 };

sub new {
    die("Can't instantiate Amanda::DB::Catalog2::SQL");
}

sub DESTROY {
    my $self = shift;
    $self->{'in_quit'}=1;
    $self->quit() if defined $self->{'dbh'} && !$self->{'dbh'}->{InactiveDestroy};
}

sub quit {
    my $self = shift;

    my $dbh = $self->{'dbh'};
    $self->{'statements'} = undef;
    if ($dbh && $dbh->isa("DBI::db") && !$dbh->{'InactiveDestroy'}) {
	$self->write_tapelist(1) if $dbh && !defined $self->{'in_quit'} && $self->{'need_write_tapelist'};
	$dbh->disconnect if $dbh;
	delete $self->{'dbh'};
    }
    $dbh = undef;

    delete $self->{'tapelist'};
}

sub make_statement {
    my $self = shift;
    my $name = shift;
    my $statement = shift;

    return $self->{'statements'}->{$name} if defined $self->{'statements'}->{$name};
    debug("make_statement($name): $statement\n");
    my $dbh = $self->{'dbh'};
    my $sth = $dbh->prepare($statement, {RaiseError => 0, AutoInactiveDestroy => 1 })
	or die "Cannot prepare: " . $dbh->errstr();

    $self->{'statements'}->{$name} = $sth;
    return $sth;
}

sub get_version {
    my $self = shift;

    return $Amanda::DB::Catalog2::DB_VERSION if !$self->{'dbh'};

    my $dbh = $self->{'dbh'};
    my $sth = $dbh->prepare("SELECT * FROM version")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();
    my $version_row = $sth->fetchrow_arrayref;
    return undef if !defined $version_row;
    return $version_row->[0];
}

sub run_execute {
    my $self = shift;
    my $obj = shift;
    my $fn = shift;
    my $read_lock = shift;
    my $write_lock = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $stop_loop = 0;
    my $result;
    my $first = 1;
    do {
        eval {
            $dbh->{'AutoCommit'} = 0;
            $result = $fn->($obj, @_);

            $dbh->commit;
        };
        my $error = $@;
        my $mysql_errno = $dbh->{'mysql_errno'};
        my $mysql_error = $dbh->{'mysql_error'};
        my $errstr = $dbh->errstr;
        $stop_loop = 1 if $error eq '';
        if ($error) {
            eval { $dbh->rollback; };

            $self->{'statements'} = {};

            debug("error $error") if defined $error;
            debug("mysql_errno: $mysql_errno") if defined $mysql_errno;
            debug("mysql_error: $mysql_error") if defined $mysql_error;
            debug("errstr: $errstr") if defined $errstr;

            sleep 1;
        } else {
            $stop_loop = 1;
        }
    } until $stop_loop;
    return $result;
}


sub rm_pool {
    my $self = shift;
    my %params = @_;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $pool = $params{'pool'};

    $sth = $dbh->prepare("DELETE FROM parts WHERE parts.part_id IN (SELECT part_id FROM parts, volumes, pools WHERE parts.volume_id=volumes.volume_id AND volumes.pool_id=pools.pool_id AND pools.pool_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($pool) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM volumes WHERE volumes.volume_id IN (SELECT volume_id FROM volumes, pools WHERE volumes.pool_id=pools.pool_id AND pools.pool_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($pool) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM pools WHERE pools.pool_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($pool) or die "Cannot execute: " . $sth->errstr();

    $self->clean();
}

sub rm_config {
    my $self = shift;
    my %params = @_;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $config_name = $params{'config_name'};
    $config_name = get_config_name() if !defined $config_name;

    # keep a copy of the tapelist file
    $self->write_tapelist(1,1);

    $sth = $dbh->prepare("DELETE FROM parts WHERE parts.part_id IN (SELECT part_id FROM parts, copys, images, disks, hosts, configs WHERE parts.copy_id=copys.copy_id AND copys.image_id=images.image_id AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id AND hosts.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM commands WHERE commands.command_id IN (SELECT command_id FROM commands, configs WHERE commands.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM copys WHERE copys.copy_id IN (SELECT copy_id FROM copys, images, disks, hosts, configs WHERE copys.image_id=images.image_id AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id AND hosts.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("UPDATE volumes SET storage_id=0 WHERE volumes.volume_id IN (SELECT volume_id FROM volumes, storages, configs WHERE volumes.storage_id=storages.storage_id AND storages.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM storages WHERE storages.storage_id IN (SELECT storage_id FROM storages, configs WHERE storages.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM images WHERE images.image_id IN (SELECT image_id FROM images, disks, hosts, configs WHERE images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id AND hosts.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM disks WHERE disks.disk_id IN (SELECT disk_id FROM disks, hosts, configs WHERE disks.host_id=hosts.host_id AND hosts.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM hosts WHERE hosts.host_id IN (SELECT host_id FROM hosts, configs WHERE hosts.config_id=configs.config_id AND configs.config_name=?)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();

    $self->clean();

    $sth = $dbh->prepare("DELETE FROM configs WHERE configs.config_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($config_name) or die "Cannot execute: " . $sth->errstr();
}

sub _add_simple_dump {
    my $self = shift;
    my $host_name = shift;
    my $disk_name = shift;
    my $device = shift || $disk_name;
    my $dump_timestamp = shift;
    my $level = shift;
    my $pool = shift;
    my $storage = shift;
    my $label = shift;
    my $filenum = shift;
    my $retention_days = shift;
    my $retention_full = shift;
    my $retention_recover = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $image = $self->_add_image($host_name,$disk_name,$device, $dump_timestamp, $level, 0);
    my $copy = $image->_add_copy($storage, $dump_timestamp, $retention_days, $retention_full, $retention_recover);
    my $volume = $self->_find_volume($pool, $label);
    my $part = $copy->_add_part($volume, 0, 1024, $filenum, 1, "OK", "");
    $copy->_finish_copy(1,1,1024,"OK","","");
    $image->_finish_image(1,"OK",undef,undef,"","","","");
}

sub add_simple_dump {
    my $self = shift;

    return $self->run_execute($self, $self->can('_add_simple_dump'), undef, undef, @_);
}

sub _add_image {
    my $self = shift;
    my $host_name = shift;
    my $disk_name = shift;
    my $device = shift || $disk_name;
    my $dump_timestamp = shift;
    my $level = shift;
    my $based_on_timestamp = shift;
    my $pid = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $pid = 0 if !defined $pid;

    # get/add the config
    my $config_id = $self->select_or_add_config();

    # get/add the host */
    my $host_id = $self->select_or_add_host($host_name);

    # get/add the disks */
    my $disk_id = $self->select_or_add_disk($host_id, $disk_name, $device);

    # get/add the image */
    my ($image_id, $dump_status) = $self->select_or_add_image($disk_id, $dump_timestamp, $level, $based_on_timestamp, $pid);

    return Amanda::DB::Catalog2::SQL::image->new($self, $image_id, $disk_id, $dump_timestamp, $level, $dump_status);
}

sub add_image {
    my $self = shift;

    # TODO: Split in many operation, should not take WRITE lock if we don't add
    return $self->run_execute($self, $self->can('_add_image'),
			undef,
			['configs', 'hosts', 'disks', 'images'], @_);
}

sub _find_image {
    my $self = shift;
    my $host_name = shift;
    my $disk_name = shift;
    my $device = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    # get/add the image */
    $sth = $self->make_statement('fi', 'SELECT images.disk_id, image_id, dump_status, image_pid FROM hosts, disks, images WHERE config_id=? AND host_name=? AND hosts.host_id=disks.host_id AND disk_name=? AND disks.disk_id=images.disk_id AND dump_timestamp=? AND level=?');
    $sth->execute($self->{'config_id'}, $host_name, $disk_name, $dump_timestamp, $level)
	or die "Cannot execute " . $sth->errstr();
    # get the first row
    my $image_row = $sth->fetchrow_arrayref;
    return if !defined $image_row;
    my $disk_id = $image_row->[0];
    my $image_id = $image_row->[1];
    my $dump_status = $image_row->[2];
    my $image_pid = $image_row->[3];

    return Amanda::DB::Catalog2::SQL::image->new($self, $image_id, $disk_id, $dump_timestamp, $level, $dump_status);
}

sub find_image {
    my $self = shift;

    # TODO: Split in many operation, should not take WRITE lock if we don't add
    return $self->run_execute($self, $self->can('_find_image'),
			['hosts', 'disks', 'images'],
			undef, @_);
}

sub _find_dumping_images {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my @images;

    # get the images */
    $sth = $self->make_statement('fdi', 'SELECT image_id, images.disk_id, disks.host_id, dump_timestamp, level, dump_status, image_pid FROM images, disks, hosts WHERE dump_status=? AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id AND hosts.config_id=?');
    $sth->execute("DUMPING", $self->{'config_id'})
	or die "Cannot execute " . $sth->errstr();
    # get the first row
    while (my $image_row = $sth->fetchrow_arrayref) {
	my $image_id = $image_row->[0];
	my $disk_id = $image_row->[1];
	my $host_id = $image_row->[2];
	my $dump_timestamp = $image_row->[3];
	my $level = $image_row->[4];
	my $dump_status = $image_row->[5];
	my $image_pid = $image_row->[6];
	push @images, Amanda::DB::Catalog2::SQL::image->new($self, $image_id, $disk_id, $dump_timestamp, $level, $dump_status, $image_pid);
    }
    return @images;
}

sub _find_dumping_copies {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my @copies;

    # get the images */
    $sth = $self->make_statement('fdc', 'SELECT copy_id, copy_status, copy_pid FROM copys, images, disks, hosts WHERE copy_status=? AND copys.image_id=images.image_id AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id AND hosts.config_id=?');
    $sth->execute("DUMPING", $self->{'config_id'})
	or die "Cannot execute " . $sth->errstr();
    # get the first row
    while (my $copy_row = $sth->fetchrow_arrayref) {
	my $copy_id = $copy_row->[0];
	my $copy_status = $copy_row->[1];
	my $copy_pid = $copy_row->[2];
	push @copies, Amanda::DB::Catalog2::SQL::copy->new($self, $copy_id, $copy_status, $copy_pid);
    }
    return @copies;
}

sub _get_image {
    my $self = shift;
    my $image_id = shift;

    return Amanda::DB::Catalog2::SQL::image->new($self, $image_id);
}

sub get_image {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_image'),
			['images'],
			undef, @_);
}


sub _find_copy {
    my $self = shift;
    my $config_id = shift;
    my $hostname = shift;
    my $diskname = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $pool = shift;
    my $label = shift;
    my $holding_file = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $s = "SELECT copys.copy_id FROM copys,images,disks,hosts,parts,volumes,pools WHERE host_name=? AND disk_name=? AND dump_timestamp=? AND pool_name=? AND hosts.host_id=disks.host_id AND disks.disk_id=images.disk_id AND images.image_id=copys.image_id AND parts.copy_id=copys.copy_id AND parts.volume_id=volumes.volume_id AND volumes.pool_id=pools.pool_id AND label=? AND hosts.config_id=?";
    my @args = ($hostname, $diskname, $dump_timestamp, $pool, defined($label)?$label:$holding_file, $config_id);

    if (defined $level) {
	$s .= " AND level=?";
	push @args, $level;
    }
    $sth = $dbh->prepare($s)
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute(@args)
	or die "Cannot execute: " . $sth->errstr();
    my $copy_row = $sth->fetchrow_arrayref;
    return undef if !defined $copy_row;
    my $copy_id = $copy_row->[0];
    return $copy_id;
}

sub _find_copyX {
    my $self = shift;
    my $hostname = shift;
    my $diskname = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $pool = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $s = "SELECT copys.copy_id FROM copys,images,disks,hosts,pools WHERE host_name=? AND disk_name=? AND dump_timestamp=? AND pool_name=? AND hosts.host_id=disks.host_id AND disks.disk_id=images.disk_id AND images.image_id=copys.image_id AND hosts.config_id=?";
    my @args = ($hostname, $diskname, $dump_timestamp, $pool, $self->{'config_id'});

    if (defined $level) {
	$s .= " AND level=?";
	push @args, $level;
    }
    $sth = $dbh->prepare($s)
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute(@args)
	or die "Cannot execute: " . $sth->errstr();
    my $copy_row = $sth->fetchrow_arrayref;
    return undef if !defined $copy_row;
    my $copy_id = $copy_row->[0];
    return Amanda::DB::Catalog2::SQL::copy->new($self, $copy_id);
}

sub get_copy {
    my $self = shift;
    my $copy_id = shift;

    return Amanda::DB::Catalog2::SQL::copy->new($self, $copy_id);
}

sub _reset_volume {
    my $self = shift;
    my $pool_name = shift;
    my $label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $self->make_statement('reset_volume', 'UPDATE volumes SET write_timestamp=?,orig_write_timestamp=?, storage_id=? WHERE label=? AND pool_id IN (SELECT pool_id FROM pools WHERE pool_name=?)');
    $sth->execute(0, 0, $self->{'default_storage_id'}, $label, $pool_name)
	or die "Cannot execute: " . $sth->errstr();

    $self->_write_tapelist();
    # handle error
    #   not found
}

sub reset_volume {
    my $self = shift;

    return $self->run_execute($self, $self->can('_reset_volume'),
			['storages', 'pools', 'metas', 'configs'],
			['volumes'], @_);
}


sub _add_volume {
    my $self = shift;
    my $pool_name = shift;
    my $label = shift;
    my $write_timestamp = shift;
    my $storage_name = shift;
    my $meta_label = shift || '';
    my $barcode = shift;
    my $blocksize = shift || 32;
    my $reuse = shift || 0;
    my $retention_days = shift;
    my $retention_full = shift;
    my $retention_recover = shift;
    my $retention_tape = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    $retention_days = 1 if !defined $retention_days;
    $retention_full = 1 if !defined $retention_full;
    $retention_recover = 1 if !defined $retention_recover;
    $retention_tape = 1 if !defined $retention_tape;

    my $pool_id;
    my $storage_id;
    my $meta_id;

    # get/add the storage
    $storage_name = '' if !defined $storage_name;
    $storage_id = $self->select_or_add_storage($storage_name);

    # get/add the pool */
    $pool_id = $self->select_or_add_pool($pool_name);

    # get/addthe meta
    $meta_id = $self->select_or_add_meta($meta_label);

    $sth = $self->make_statement('sel vpl', 'SELECT volume_id, pool_name, label, reuse, storage_name, config_name, meta_label, write_timestamp, retention_days, retention_full, retention_recover, retention_tape, block_size, barcode FROM volumes,pools,storages,metas,configs WHERE volumes.pool_id=pools.pool_id AND volumes.storage_id=storages.storage_id AND volumes.meta_id=metas.meta_id AND storages.config_id=configs.config_id AND pool_name=? AND label=?');
    $sth->execute($pool_name, $label)
	or die "Can't execute: " . $sth->errstr();
    my $volume_row = $sth->fetchrow_arrayref;
    if (defined $volume_row) {
	return Amanda::DB::Catalog2::SQL::volume->new($self, $volume_row->[0],
				pool => $volume_row->[1],
				label => $volume_row->[2],
				reuse => $volume_row->[3],
				storage => $volume_row->[4],
				config => $volume_row->[5],
				meta => $volume_row->[6],
				write_timestamp => $volume_row->[7],
				retention_days => $volume_row->[8],
				retention_full => $volume_row->[9],
				retention_recover => $volume_row->[10],
				retention_tape => $volume_row->[11],
				blocksize => $volume_row->[12],
				barcode => $volume_row->[13]);
    }


    $retention_tape = 1 if $retention_tape > 1;
    $retention_tape = 0 if $retention_tape eq '';
    $sth = $self->make_statement('in vol', 'INSERT INTO volumes(pool_id,label,reuse,storage_id,meta_id,write_timestamp,orig_write_timestamp,retention_days,retention_full,retention_recover,retention_tape,retention_cmd,block_size,barcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    $sth->execute($pool_id, $label, $reuse, $storage_id, $meta_id, $write_timestamp, $write_timestamp, $retention_days, $retention_full, $retention_recover, $retention_tape, 0, $blocksize, $barcode)
	or die "Can't add volume $pool_name:$label $write_timestamp: M:$meta_label:$meta_id:" . $sth->errstr();
    my $volume_id = $dbh->last_insert_id(undef, undef, "volumes", undef);

    $self->_write_tapelist() if $pool_name ne 'HOLDING';
    return Amanda::DB::Catalog2::SQL::volume->new($self, $volume_id,
				pool => $pool_name,
				label => $label,
				reuse => $reuse,
				storage => $storage_name,
				config => $self->{'config_name'},
				meta => $meta_label,
				write_timestamp => $write_timestamp,
				retention_days => $retention_days,
				retention_full => $retention_full,
				retention_recover => $retention_recover,
				retention_tape => $retention_tape,
				retention_cmd => 0,
				blocksize => $blocksize,
				barcode => $barcode);
}

sub add_volume {
    my $self = shift;

    return $self->run_execute($self, $self->can('_add_volume'),
			['configs'],
			['volumes','pools','storages','metas'], @_);
}


sub _add_volume_tle {
    my $self = shift;
    my $tle = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    # config
    my $config_name = $tle->{'config'};
    if (!$config_name && $tle->{'datestamp'} != '0') {
	$config_name = $self->{'config_name'};
    }
    my $config_id = $self->select_or_add_config($config_name);

    # storage
    my $storage_name = $tle->{'storage'} || $self->{'config_name'};
    my $storage_id = $self->{'default_storage_id'};
    if ($storage_name) {
	$storage_id = $self->select_or_add_storage($storage_name,$config_id);
    }

    # get/add the pool */
    my $pool_name = $tle->{'pool'} || $self->{'config_name'};
    my $pool_id = $self->{'default_pool_id'};
    if ($pool_name) {
	$pool_id = $self->select_or_add_pool($pool_name);
    }

    # get/add the meta
    my $meta_label = $tle->{'meta'};
    my $meta_id = $self->{'default_meta_id'};
    if ($meta_label) {
	$meta_id = $self->select_or_add_meta($meta_label);
    }

    #
    my $volume_id;
    my $timestamp = $tle->{'datestamp'};
    my $orig_timestamp = $tle->{'datestamp'};
    $timestamp .= "000000" if length($timestamp) == 8;
    my $blocksize = $tle->{'blocksize'} || 32;
    $sth = $self->make_statement('in vol', 'INSERT INTO volumes(pool_id,label,reuse,storage_id,meta_id,write_timestamp,orig_write_timestamp,retention_days,retention_full,retention_recover,retention_tape,retention_cmd,block_size,barcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    if ($sth->execute($pool_id, $tle->{'label'}, $tle->{'reuse'},  $storage_id, $meta_id, $timestamp, $orig_timestamp, 1, 1, 1, 1, 0, $blocksize, $tle->{'barcode'})) {
	$volume_id = $dbh->last_insert_id(undef, undef, "volumes", undef);
    } else {
	$sth = $self->make_statement('_add_volume_tle:sel vol', 'SELECT volume_id FROM volumes WHERE pool_id=? AND label=?');
	$sth->execute($pool_id, $tle->{'label'})
	    or die "Can't find volume pool:$pool_id $tle->{'label'}" . $sth->errstr();
	my $volume_row = $sth->fetchrow_arrayref;
	$volume_id = $volume_row->[0];
    }

    $self->_write_tapelist();
    return Amanda::DB::Catalog2::SQL::volume->new($self, $volume_id);
}

sub _find_meta {
    my $self = shift;
    my $meta = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $self->make_statement('sel met_id','SELECT meta_id FROM metas WHERE meta_label=?');
    $sth->execute($meta)
	or die "Cannot execute: " . $dbh->errstr();
    my $meta_row = $sth->fetchrow_arrayref;
    return undef if !defined $meta_row;

    return $meta_row->[0];
}

sub find_meta {
    my $self = shift;

    return $self->run_execute($self, $self->can('_find_meta'),
			['metas'],
			undef, @_);
}

sub _get_last_reusable_volume {
    my $self = shift;
    my $storage = shift;
    my $max_volume = shift || 1;

    my $retention_tapes = $storage->{'policy'}->{'retention_tapes'};
    my $volumes = $self->_find_volumes(
			pool => $storage->{'tapepool'},
			config => get_config_name(),
			storage => $storage->{'storage_name'},
			retention => 0,
#			retention_tape => 0,
			reuse => 1,
			labelstr => $storage->{'labelstr'}->{'template'},
			write_timestamp_set => 1,
			order_write_timestamp => 1,
			max_volume => $retention_tapes+1);
#			max_volume => ($max_volume || 1));
    while (@$volumes <= $retention_tapes) {
	unshift @$volumes, undef;
    }
    my $nb = @$volumes;
    my $to_remove = $retention_tapes-$max_volume+1;
    my $start = $nb-$to_remove;
    if ($start <= 0) {
	return;
    }
    splice @$volumes, $start;
    return $volumes;
}

sub get_last_reusable_volume {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_last_reusable_volume'),
			['volumes','pools','storages','metas','configs'],
			undef, @_);
}

sub _find_volume {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $self->make_statement('fv1', 'SELECT volume_id, pool_name, label, reuse, storage_name, config_name, meta_label, write_timestamp, retention_days, retention_full, retention_recover, retention_tape, block_size, barcode FROM volumes,pools,storages,metas,configs WHERE volumes.pool_id=pools.pool_id AND volumes.storage_id=storages.storage_id AND volumes.meta_id=metas.meta_id AND storages.config_id=configs.config_id AND pool_name=? AND label=?');
    $sth->execute($pool, $label)
	or die "Cannot execute: " . $dbh->errstr();
    my $volume_row = $sth->fetchrow_arrayref;
    return undef if !defined $volume_row;

    my $volume_id = $volume_row->[0];
    return Amanda::DB::Catalog2::SQL::volume->new($self, $volume_id,
				pool => $volume_row->[1],
				label => $volume_row->[2],
				reuse => $volume_row->[3],
				storage => $volume_row->[4],
				config => $volume_row->[5],
				meta => $volume_row->[6],
				write_timestamp => $volume_row->[7],
				retention_days => $volume_row->[8],
				retention_full => $volume_row->[9],
				retention_recover => $volume_row->[10],
				retention_tape => $volume_row->[11],
				blocksize => $volume_row->[12],
				barcode => $volume_row->[13]);
}

sub find_volume {
    my $self = shift;

    return $self->run_execute($self, $self->can('_find_volume'),
			['volumes','pools','storages','metas','configs'],
			undef, @_);
}

sub _find_volume_by_barcode {
    my $self = shift;
    my $barcode = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $self->make_statement('fvbb', 'SELECT volume_id, pool_name, label, reuse, storage_name, config_name, meta_label, write_timestamp, retention_days, retention_full, retention_recover, retention_tape, block_size, barcode FROM volumes,pools,storages,metas,configs WHERE volumes.barcode=?');
    $sth->execute($barcode)
	or die "Cannot execute: " . $dbh->errstr();
    my $volume_row = $sth->fetchrow_arrayref;
    return undef if !defined $volume_row;

    my $volume_id = $volume_row->[0];
    return Amanda::DB::Catalog2::SQL::volume->new($self, $volume_id,
				pool => $volume_row->[1],
				label => $volume_row->[2],
				reuse => $volume_row->[3],
				storage => $volume_row->[4],
				config => $volume_row->[5],
				meta => $volume_row->[6],
				write_timestamp => $volume_row->[7],
				retention_days => $volume_row->[8],
				retention_full => $volume_row->[9],
				retention_recover => $volume_row->[10],
				retention_tape => $volume_row->[11],
				blocksize => $volume_row->[12],
				barcode => $volume_row->[13]);
}

sub find_volume_by_barcode {
    my $self = shift;

    return $self->run_execute($self, $self->can('_find_volume_by_barcode'),
			['volumes','pools','storages','metas','configs'],
			undef, @_);
}

sub _find_volume_all {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my @volumes;

    if (defined $pool) {
	$sth = $self->make_statement('fvap', 'SELECT volume_id, storage_name, meta_label, pool_name, config_name FROM volumes,pools,storages,metas,configs WHERE volumes.pool_id=pools.pool_id AND pool_name=? AND label=? AND volumes.storage_id=storages.storage_id AND volumes.meta_id=metas.meta_id AND storages.config_id=configs.config_id');
	$sth->execute($pool, $label)
	    or die "Cannot execute: " . $dbh->errstr();
    } else {
	$sth = $self->make_statement('fvanop', 'SELECT volume_id, storage_name, meta_label, pool_name, config_name FROM volumes,pools,storages,metas,configs WHERE volumes.pool_id=pools.pool_id AND label=? AND volumes.storage_id=storages.storage_id AND volumes.meta_id=metas.meta_id AND storages.config_id=configs.config_id');
	$sth->execute($label)
	    or die "Cannot execute: " . $dbh->errstr();
    }
    while (my $volume_row = $sth->fetchrow_arrayref) {
	my $volume_id = $volume_row->[0];
	push @volumes, Amanda::DB::Catalog2::SQL::volume->new($self, $volume_id, pool => $volume_row->[3], storage => $volume_row->[1], meta => $volume_row->[2], config => $volume_row->[4]);
    }

    return \@volumes;
}

sub find_volume_all {
    my $self = shift;

    return $self->run_execute($self, $self->can('_find_volume_all'),
			['volumes','pools','storages','metas','configs'],
			undef, @_);
}

sub _find_volumes {
    my $self = shift;
    my %params = @_;

    debug("_find_volumes");
    my $dbh = $self->{'dbh'};
    my $sth;
    my @volumes;
    my $s;
    my @args;
    $s = "SELECT volume_id, pool_name, label, reuse, storage_name, config_name, meta_label, write_timestamp, retention_days, retention_full, retention_recover, retention_tape, block_size, barcode FROM volumes,pools,storages,metas,configs WHERE volumes.pool_id=pools.pool_id AND volumes.storage_id=storages.storage_id AND volumes.meta_id=metas.meta_id AND storages.config_id=configs.config_id";
    if (defined $params{pool}) {
	$s .= " AND pools.pool_name=?";
	push @args, $params{pool};
    }
    if (defined $params{label}) {
	$s .= " AND volumes.label=?";
	push @args, $params{label};
    }
    if (defined $params{reuse}) {
	$s .= " AND volumes.reuse=?";
	push @args, $params{reuse};
    }
    if (defined $params{no_retention}) {
	$s .= " AND volumes.retention_days=0 AND volumes.retention_full=0 AND volumes.retention_recover=0 AND volumes.retention_tape=0";
    }
    if (defined $params{have_retention}) {
	$s .= " AND (volumes.retention_days=1 OR volumes.retention_full=1 OR volumes.retention_recover=1 OR volumes.retention_tape=1";
    }
    if (defined $params{retention}) {
	if ($params{retention}) {
	    $s .= " AND (volumes.retention_days & ? OR volumes.retention_full & ? OR volumes.retention_recover & ? ";
	    push @args, $params{retention}, $params{retention}, $params{retention};
	} else {
	    $s .= " AND volumes.retention_days=0 AND volumes.retention_full=0 AND volumes.retention_recover=0";
	}
    }
    if (defined $params{retention_tape}) {
	$s .= " AND volumes.retention_tape=?";
	push @args, $params{retention_tape};
    }
    if (defined $params{storage}) {
	$s .= " AND (storages.storage_name=? OR storages.storage_name=?)";
	push @args, $params{storage}, '';
    } elsif (defined $params{storage_only}) {
	$s .= " AND storages.storage_name=?";
	push @args, $params{storage};
    }
    if (defined $params{storages}) {
	my $first = 1;
	$s .= " AND (";
	foreach my $storage (@{$params{storages}}) {
	    $s .= " OR" if !$first;
	    $s .= " storages.storage_name=?";
	    push @args, $storage;
	    $first = 0;
	}
	$s .= ")";
    }
    if (defined $params{config}) {
	$s .= " AND (configs.config_name=? OR configs.config_name=?)";
	push @args, $params{config}, '';
    } elsif (defined $params{only_config}) {
	$s .= " AND configs.config_name=?";
	push @args, $params{config};
    }
    if (defined $params{meta}) {
	$s .= " AND metas.meta_label=?";
	push @args, $params{meta};
    }
    if (defined $params{barcode}) {
	$s .= " AND volumes.barcode=?";
	push @args, $params{barcode};
    }
    if (defined $params{write_timestamp}) {
	$s .= " AND volumes.write_timestamp=0";
    }
    if (defined $params{'write_timestamp_set'}) {
	$s .= " AND write_timestamp!=0";
    }
    if (defined $params{'order_write_timestamp'}) {
	if ($params{'order_write_timestamp'} > 0) {
	    $s .= " ORDER BY write_timestamp ASC, label ASC";
	} else {
	    $s .= " ORDER BY write_timestamp DESC, label DESC";
	}
    }
    if (defined $params{'max_volume'}) {
	    $s .= " LIMIT $params{'max_volume'}";
    }
    $sth = $dbh->prepare($s)
	    or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute(@args)
	    or die "Cannot execute: " . $dbh->errstr();
    while (my $volume_row = $sth->fetchrow_arrayref) {
	my $volume_id = $volume_row->[0];
	my $volume;
	if ($params{no_bless}) {
	    $volume = {
					pool => $volume_row->[1],
					label => $volume_row->[2],
					reuse => $volume_row->[3],
					storage => $volume_row->[4],
					config => $volume_row->[5],
					meta => $volume_row->[6],
					write_timestamp => $volume_row->[7],
					retention_days => $volume_row->[8],
					retention_full => $volume_row->[9],
					retention_recover => $volume_row->[10],
					retention_tape => $volume_row->[11],
					blocksize => $volume_row->[12],
					barcode => $volume_row->[13]};
	} else {
	    $volume = Amanda::DB::Catalog2::SQL::volume->new($self, $volume_id,
					pool => $volume_row->[1],
					label => $volume_row->[2],
					reuse => $volume_row->[3],
					storage => $volume_row->[4],
					config => $volume_row->[5],
					meta => $volume_row->[6],
					write_timestamp => $volume_row->[7],
					retention_days => $volume_row->[8],
					retention_full => $volume_row->[9],
					retention_recover => $volume_row->[10],
					retention_tape => $volume_row->[11],
					blocksize => $volume_row->[12],
					barcode => $volume_row->[13]);
	}
	if ($params{'retention_name'}) {
	    Amanda::DB::Catalog2::SQL::volume::set_retention_name($volume);
	}
	push @volumes, $volume;
    }
    if ($params{datestamp}) {
	@volumes = grep {defined $_->{'datestamp'} and match_datestamp($params{'datestamp'}, $_->{'write_timestamp'})} @volumes;
    }

    return \@volumes;
}

sub find_volumes {
    my $self = shift;

    debug("find_volumes");
    my $volumes = $self->run_execute($self, $self->can('_find_volumes'),
			['volumes','pools','storages','metas','configs'],
			undef, @_);
    return $volumes;
}

sub _compute_storage_retention_tape {
    my $self = shift;
    my $storage_name = shift;
    my $pool = shift;
    my $retention_tapes = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my $sth2;
    my $sth3;
    my $result;

#debug("_compute_storage_retention_tape : $storage_name $pool : $retention_tapes");
    my $volume_table = "volume_ids_cstt_$$";
    if ($pool ne 'HOLDING') {
	$sth = $self->make_statement('csrt upt1t', 'UPDATE volumes SET retention_tape=1 WHERE volume_id IN (SELECT volume_id FROM volumes,pools,storages WHERE storage_name=? AND volumes.storage_id=storages.storage_id AND pool_name=? AND volumes.pool_id=pools.pool_id AND retention_tape=0 AND reuse=1 AND write_timestamp!=0)');
	$sth->execute($storage_name, $pool)
	     or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('csrt upt0t', 'UPDATE volumes SET retention_tape=0 WHERE volume_id IN (SELECT volume_id FROM volumes,pools WHERE pool_name=? AND volumes.pool_id=pools.pool_id AND retention_tape=1 AND reuse=1 AND write_timestamp=0)');
	$sth->execute($pool)
	     or die "Cannot execute: " . $sth->errstr();

	do {
	    if ($retention_tapes == 0) {
		$sth = $dbh->prepare("CREATE $self->{'temporary'} TABLE $volume_table AS SELECT volume_id FROM volumes,pools,storages WHERE storage_name=? AND volumes.storage_id=storages.storage_id AND pool_name=? AND volumes.pool_id=pools.pool_id AND reuse=1 AND write_timestamp!=0")
		    or die "Cannot prepare: " . $dbh->errstr();
	    } else {
		$sth = $dbh->prepare("CREATE $self->{'temporary'} TABLE $volume_table AS SELECT volume_id FROM volumes,pools,storages WHERE storage_name=? AND volumes.storage_id=storages.storage_id AND pool_name=? AND volumes.pool_id=pools.pool_id AND reuse=1 AND write_timestamp!=0 ORDER BY write_timestamp DESC, label DESC LIMIT 10000 OFFSET $retention_tapes")
		    or die "Cannot prepare: " . $dbh->errstr();
	    }
	    $sth->execute($storage_name, $pool)
		or die "Cannot execute: " . $sth->errstr();
	    $sth2 = $self->make_statement('csrt upt0r', "UPDATE volumes SET retention_tape=0 WHERE volume_id IN (SELECT volume_id FROM $volume_table)");
	    $result = $sth2->execute()
		or die "Cannot execute: " . $sth->errstr();
	    $retention_tapes += 10000;
	    $sth3 = $self->make_statement('csrt dv', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
	    $sth3->execute()
		or die "Cannot execute: " . $sth->errstr();
	} until $result == 0;
    }
}

sub _set_no_reuse {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $self->make_statement('snr', 'UPDATE volumes SET reuse=0 WHERE label=? AND pool_id IN (SELECT pool_id FROM pools WHERE pool_name=?)');
    $sth->execute($label, $pool)
	or die "Cannot execute: " . $sth->errstr();
}

sub set_no_reuse {
    my $self = shift;

    return $self->run_execute($self, $self->can('_set_no_reuse'),
			['pools'],
			['volumes'], @_);
}

sub _create_triggers {
    my $self = shift;
    my $dbh = $self->{'dbh'};

#    $dbh->do("CREATE TRIGGER volume_delete_parts BEFORE DELETE ON volumes FOR EACH ROW BEGIN DELETE FROM parts WHERE parts.volume_id=OLD.volume_id; END")
#	or die "Cannot do: " . $dbh->errstr();

#    $dbh->do("CREATE TRIGGER part_delete_copys AFTER DELETE ON parts FOR EACH ROW BEGIN DELETE FROM copys WHERE copys.copy_id=OLD.copy_id; END")
#	or die "Cannot do: " . $dbh->errstr();
}

sub _create_tableX {
    my $self = shift;
    my $empty = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    # Do not create the tables if table version exists
    return if $self->table_exists('versions');

    my $autoincrement = $self->{'autoincrement'} || '';
    my $autoincrementX = $self->{'autoincrementX'} || 'INTEGER';
    my $foreign_key = $self->{'foreign_key'};
    my $small_int = $self->{'smallint'} || "INTEGER(1)";
    my $char = $self->{'char'} || "CHAR";

    $sth = $dbh->prepare("CREATE TABLE version (version INTEGER NOT NULL)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();
    $sth = $dbh->prepare("INSERT INTO version VALUES (?)")
	or die "Cannot prepare: " . $dbh->errstr();

    $sth->execute($Amanda::DB::Catalog2::DB_VERSION) or die "Cannot execute: " . $sth->errstr();


    $sth = $dbh->prepare("CREATE TABLE configs (config_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
					        config_name $char(255) NOT NULL UNIQUE,
						created INTEGER)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE storages (storage_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
						 config_id INTEGER NOT NULL,
						 storage_name $char(255) NOT NULL,
						 last_sequence_id INTEGER NOT NULL,
						 UNIQUE (config_id, storage_id),
						 UNIQUE (config_id, storage_name),
						 $foreign_key (config_id) REFERENCES configs (config_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE pools (pool_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
					      pool_name $char(255) NOT NULL UNIQUE)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE metas (meta_id $autoincrementX NOT NULL UNIQUE PRIMARY KEY $autoincrement,
					      meta_label $char(255) NOT NULL UNIQUE)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE volumes (volume_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
						pool_id INTEGER NOT NULL,
						label $char(255) NOT NULL,
						reuse INTEGER NOT NULL,
						storage_id INTEGER NOT NULL,
						meta_id INTEGER NOT NULL,
						write_timestamp BIGINT NOT NULL,
						orig_write_timestamp BIGINT NOT NULL,
						retention_days INTEGER NOT NULL,
						retention_full INTEGER NOT NULL,
						retention_recover INTEGER NOT NULL,
						retention_tape INTEGER NOT NULL,
						retention_cmd INTEGER NOT NULL,
						block_size INTEGER NOT NULL,
						barcode $char(255),
						UNIQUE (pool_id, label),
						$foreign_key (storage_id) REFERENCES storages (storage_id),
						$foreign_key (pool_id) REFERENCES pools (pool_id),
						$foreign_key (meta_id) REFERENCES metas (meta_id))")
	or die "Cannot prepare: " . $dbh->errstr();
						#config_id INTEGER,
						#$foreign_key (config_id) REFERENCES configs (config_id),
    $sth->execute() or die "Cannot execute: " . $sth->errstr();


    $sth = $dbh->prepare("CREATE TABLE hosts (host_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
					      config_id INTEGER NOT NULL,
					      host_name $char(255) NOT NULL,
					      UNIQUE (config_id, host_name),
					      $foreign_key (config_id) REFERENCES configs (config_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE disks (disk_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
					      host_id INTEGER NOT NULL,
					      disk_name $char(255) NOT NULL,
					      device $char(255) NOT NULL,
					      force_full $small_int DEFAULT 0 NOT NULL,
					      force_level_1 $small_int DEFAULT 0 NOT NULL,
					      force_bump $small_int DEFAULT 0 NOT NULL,
					      force_no_bump $small_int DEFAULT 0 NOT NULL,
					      UNIQUE (host_id, disk_name),
					      $foreign_key (host_id) REFERENCES hosts (host_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE images (image_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
disk_id INTEGER NOT NULL,
dump_timestamp BIGINT NOT NULL,
level INTEGER NOT NULL,
parent_image_id INTEGER NOT NULL,
orig_kb BIGINT,
dump_status VARCHAR(1024),
nb_files BIGINT,
nb_directories BIGINT,
based_on_timestamp BIGINT,
native_crc $char(30),
client_crc $char(30),
server_crc $char(30),
dump_message VARCHAR(150),
image_pid INTEGER NOT NULL,
UNIQUE (disk_id, dump_timestamp),
$foreign_key (disk_id) REFERENCES disks (disk_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE copys (copy_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
					      image_id INTEGER NOT NULL,
					      storage_id INTEGER NOT NULL,
					      write_timestamp BIGINT NOT NULL,
					      parent_copy_id INTEGER NOT NULL,
					      nb_parts INTEGER NOT NULL,
					      kb BIGINT,
					      bytes BIGINT,
					      copy_status VARCHAR(1024) NOT NULL,
					      server_crc $char(30),
					      retention_days INTEGER NOT NULL,
					      retention_full INTEGER NOT NULL,
					      retention_recover INTEGER NOT NULL,
					      copy_message VARCHAR(150),
					      copy_pid INTEGER NOT NULL,
					      $foreign_key (image_id) REFERENCES images (image_id),
					      $foreign_key (storage_id) REFERENCES storages (storage_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE parts (part_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
					      copy_id INTEGER NOT NULL,
					      volume_id INTEGER NOT NULL,
					      part_offset BIGINT NOT NULL,
					      part_size BIGINT NOT NULL,
					      filenum INTEGER NOT NULL,
					      part_num INTEGER NOT NULL,
					      part_status VARCHAR(1024) NOT NULL,
					      part_message VARCHAR(150),
					      $foreign_key (copy_id) REFERENCES copys (copy_id),
					      $foreign_key (volume_id) REFERENCES volumes (volume_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("CREATE TABLE commands (command_id $autoincrementX NOT NULL PRIMARY KEY $autoincrement,
						 operation INTEGER NOT NULL,
						 config_id INTEGER NOT NULL,
						 src_copy_id INTEGER NOT NULL,
						 dest_storage_id INTEGER,
						 working_pid INTEGER,
						 status INTEGER NOT NULL,
						 todo INTEGER NOT NULL,
						 size BIGINT NOT NULL,
						 start_time INTEGER NOT NULL,
						 expire INTEGER NOT NULL,
						 count INTEGER NOT NULL,
						 $foreign_key (config_id) REFERENCES configs (config_id),
						 $foreign_key (src_copy_id) REFERENCES copys (copy_id),
						 $foreign_key (dest_storage_id) REFERENCES storages (storage_id))")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $dbh->do("CREATE INDEX VOLUMES_storage_id on volumes (storage_id)")
	or die "Cannot do: " . $dbh->errstr();
    $dbh->do("CREATE INDEX VOLUMES_pool_id on volumes (pool_id)")
	or die "Cannot do: " . $dbh->errstr();
    $dbh->do("CREATE INDEX VOLUMES_meta_id on volumes (meta_id)")
	or die "Cannot do: " . $dbh->errstr();

    $dbh->do("CREATE INDEX HOSTS_config_id on hosts (config_id)")
	or die "Cannot do: " . $dbh->errstr();

    $dbh->do("CREATE INDEX DISKS_host_id on disks (host_id)")
	or die "Cannot do: " . $dbh->errstr();

    $dbh->do("CREATE INDEX IMAGES_disk_id on images (disk_id)")
	or die "Cannot do: " . $dbh->errstr();

    $dbh->do("CREATE INDEX COPYS_image_id on copys (image_id)")
	or die "Cannot do: " . $dbh->errstr();
    $dbh->do("CREATE INDEX COPYS_storage_id on copys (storage_id)")
	or die "Cannot do: " . $dbh->errstr();

    $dbh->do("CREATE INDEX PARTS_copy_id on parts (copy_id)")
	or die "Cannot do: " . $dbh->errstr();
    $dbh->do("CREATE INDEX PARTS_volume_id on parts (volume_id)")
	or die "Cannot do: " . $dbh->errstr();

    $self->_create_triggers();

    if (!$empty) {
	$sth = $self->make_statement('in met', 'INSERT INTO metas(meta_label) values (?)');
	$sth->execute('')
	    or die "Cannot execute: " . $sth->errstr();
	my $default_meta_id = $dbh->last_insert_id(undef, undef, "metas", undef);

	$sth = $self->make_statement('in con', 'INSERT INTO configs(config_name, created)  values (?, ?)');
	$sth->execute('', 0)
	    or die "Cannot execute: " . $sth->errstr();
	my $default_config_id = $dbh->last_insert_id(undef, undef, "configs", undef);

	$sth = $self->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
	$sth->execute($default_config_id, '', 0)
	    or die "Cannot execute: " . $sth->errstr();
	my $default_storage_id = $dbh->last_insert_id(undef, undef, "storages", undef);
    }
}

sub _upgrade_table {
    my $self = shift;
    my $current_version = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    if ($current_version == $Amanda::DB::Catalog2::DB_VERSION) {
	print "Database is already at version '$Amanda::DB::Catalog2::DB_VERSION'.\n";
	debug("Database is already at version '$Amanda::DB::Catalog2::DB_VERSION'.");
	return;
    }

    my $char = $self->{'char'} || "CHAR";

    # upgrade to version 2
    # only the version change.
    if ($current_version == 1) {
	$sth = $dbh->prepare("UPDATE version SET version=?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute(2) or die "Cannot execute: " . $sth->errstr();;
	$current_version = 2;
	print "Upgrading the database to version '2'.\n";
	debug("Upgrading the database to version '2'.");
    }

    if ($current_version == 2) {
	$sth = $dbh->prepare("ALTER TABLE image ADD nb_files INTEGER")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute() or die "Cannot execute: " . $sth->errstr();

	$sth = $dbh->prepare("ALTER TABLE image ADD nb_directories INTEGER")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute() or die "Cannot execute: " . $sth->errstr();

	$sth = $dbh->prepare("ALTER TABLE copy ADD header_str VARCHAR(32768)")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute() or die "Cannot execute: " . $sth->errstr();;

	$sth = $dbh->prepare("UPDATE version SET version=?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute(3) or die "Cannot execute: " . $sth->errstr();;

	$current_version = 3;
	print "Upgrading the database to version '3'.\n";
	debug("Upgrading the database to version '3'.");
    }

    if ($current_version == 3) {
	$sth = $dbh->prepare("ALTER TABLE image ADD based_on_timestamp $char(14)")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute() or die "Cannot execute: " . $sth->errstr();

	$sth = $dbh->prepare("UPDATE version SET version=?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute(4) or die "Cannot execute: " . $sth->errstr();

	$current_version = 4;
	print "Upgrading the database to version '4'.\n";
	debug("Upgrading the database to version '4'.");
    }
    print "Database is now at version '$Amanda::DB::Catalog2::DB_VERSION'.\n";
    debug("Database is now at version '$Amanda::DB::Catalog2::DB_VERSION'.");
}

sub _load_table {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my %volume;
    my $volume;

    my $indexdir = getconf($CNF_INDEXDIR);
    my $index = Amanda::Index->new(indexdir => $indexdir);

    # load the volumes
    my $tapelist_file = config_dir_relative(getconf($CNF_TAPELIST));
    my ($tapelist, $message) = Amanda::Tapelist->new($tapelist_file);
    if (defined $message) {
	print STDERR "amadmin: Could not read the tapelist: $message";
	exit 1;
    }
    foreach my $tle (@{$tapelist->{'tles'}}) {
	$self->_add_volume_tle($tle);
    }

    $sth = $self->make_statement('_load_table:create', 'SELECT created FROM configs WHERE config_name=?');
    $sth->execute($self->{'config_name'}) or die "Cannot execute: " . $sth->errstr();
    my $config_row = $sth->fetchrow_arrayref;
    if (defined $config_row && $config_row->[0] == 1) {
	print "Already created table for config $self->{'config_name'}\n";
	return;
    } elsif (!defined $config_row) {
	$sth = $self->make_statement('_load_table:insert config', 'INSERT INTO configs(config_name, created) VALUES (?, ?)');
	$sth->execute($self->{'config_name'}, 0)
	    or die "Can't add config $self->{'config_name'}: " . $sth->errstr();
    }

    $sth = $self->make_statement('_load_table:sel sid', 'SELECT storage_id FROM storages WHERE config_id=? AND storage_name=?');
    $sth->execute($self->{'config_id'}, "HOLDING") or die "Cannot execute: " . $sth->errstr();
    my $storage_row = $sth->fetchrow_arrayref;
    if (!defined $storage_row) {
	$sth = $self->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
	$sth->execute($self->{'config_id'}, "HOLDING", 0)
	    or die "Can't add storage HOLDING " . $sth->errstr();
    }

    $sth = $self->make_statement('_load_table:sel pid', 'SELECT pool_id FROM pools WHERE pool_name=?');
    $sth->execute("HOLDING") or die "Cannot execute: " . $sth->errstr();
    my $pool_row = $sth->fetchrow_arrayref;
    if (!defined $pool_row) {
	$sth = $self->make_statement('in poo', 'INSERT INTO pools(pool_name) VALUES (?)');
	$sth->execute("HOLDING")
	    or die "Can't add pool HOLDING " . $sth->errstr();
    }

    # load the dumps
    my @dumps = Amanda::DB::Catalog::sort_dumps(['hostname','diskname','dump_timestamp','status'],
						Amanda::DB::Catalog::get_dumps());
    foreach my $dump (@dumps) {
	if (!defined $dump->{'hostname'} and !defined $dump->{'diskname'}) {
	    next;
	}
	my $device = "";
	my $disk = Amanda::Disklist::get_disk($dump->{'hostname'}, $dump->{'diskname'});
	if ($disk && $disk->{'device'}) {
	    $device = $disk->{'device'};
	}

	my $image = $self->_add_image($dump->{'hostname'}, $dump->{'diskname'},
				      $device, $dump->{'dump_timestamp'},
				      $dump->{'level'});
	my $timestamp = $dump->{'write_timestamp'};
	$timestamp = "00000000000000" if $timestamp == 0 &&
					 $dump->{'pool'} eq 'HOLDING';
	$timestamp = $dump->{'dump_timestamp'} if $timestamp == 0 &&
						  $dump->{'pool'} ne 'HOLDING';

	my $storage = lookup_storage($dump->{'storage'});
	my $policy = lookup_policy(Amanda::Config::storage_getconf($storage, $STORAGE_POLICY));
	my $retention_days = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_DAYS);
	my $retention_full = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_FULL);
	my $retention_recover = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_RECOVER);

	my $copy = $image->_add_copy($dump->{'storage'}, $timestamp, $retention_days, $retention_full, $retention_recover);
	my $offset = 0;
	my $nb_parts = 0;
	foreach my $part (@{$dump->{'parts'}}) {
	    next if !$part;
#print "B1 $part $part->{'holding_file'} POOL:$dump->{'pool'} LABEL:$part->{'label'}\n";
	    my $label;
	    if (defined $part->{'holding_file'}) {
		$label = $part->{'holding_file'};
		$part->{'filenum'} = 0;
	    } else {
		$label = $part->{'label'};
	    }
	    my $pool = $dump->{'pool'};
	    if ($dump->{'pool'} eq "HOLDING") {
		$volume = $self->_add_volume("HOLDING", $label, $timestamp, "HOLDING", undef, undef, 32);
		$volume{$pool}{$label} = $volume;
	    } else {
		$volume = $volume{$pool}{$label};
		if (!defined $volume) {
		    $volume = $self->_find_volume($pool, $label, $timestamp);
		    if (!$volume) {
			die "volume '$label' not found in pool '$pool'";
			#$volume = $self->add_volume($storage, $pool, $label, $timestamp);
		    }
		    $volume{$pool}{$label} = $volume;
		}
	    }
	    my $size = $part->{'kb'} * 1024;
	    $copy->_add_part($volume, $offset, $size, $part->{'filenum'},
			    $part->{'partnum'}, $part->{'status'}, $part->{'message'});
	    $nb_parts = $part->{'partnum'} if defined $part->{'partnum'} and
					      $part->{'partnum'} > $nb_parts and
					      $part->{'status'} eq "OK";
					      # and !defined $part->{'holding_file'};
	    $offset += $size;
	}
	$nb_parts = $dump->{'nb_parts'} if defined $dump->{'nb_parts'} and $dump->{'nb_parts'} > $nb_parts;
	$copy->_finish_copy($nb_parts, $dump->{'kb'}, $dump->{'bytes'},
			   $dump->{'status'}, $dump->{'server_crc'}, $dump->{'message'});
	$image->_finish_image($dump->{'orig_kb'}, $dump->{'status'}, undef, undef, $dump->{'native_crc'}, $dump->{'client_crc'}, $dump->{'server_crc'}, $dump->{'message'});
    }

    $sth = $dbh->prepare("UPDATE configs SET created=1 WHERE config_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($self->{'config_name'})
            or die "Can't update config $self->{'config_name'}: " . $sth->errstr();

    # load the command
    my $conf_cmdfile = config_dir_relative(getconf($CNF_CMDFILE));
    my $cmdfile = Amanda::Cmdfile->new($conf_cmdfile);
    my @cmds = $cmdfile->get_all();
    $sth = $dbh->prepare("INSERT INTO commands VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	or die "Cannot prepare: " . $dbh->errstr();
    foreach my $cmd (@cmds){
	my $command_id = $cmd->{'id'};
	my $operation = $cmd->{'operation'};
	next if $operation == $Amanda::Cmdfile::CMD_FLUSH &&
		    !-f $cmd->{'holding_file'};
	my $config_id = $self->select_or_add_config($cmd->{'config'});
	my $src_pool = $cmd->{'src_pool'} || $cmd->{'config'};

	$src_pool = "HOLDING" if $operation == $Amanda::Cmdfile::CMD_FLUSH;
	my $level = $cmd->{'level'};
	$level=undef if $operation == $Amanda::Cmdfile::CMD_FLUSH;
	my $src_copy_id = $self->_find_copy($config_id, $cmd->{'hostname'}, $cmd->{'diskname'}, $cmd->{'dump_timestamp'}, $level, $src_pool, $cmd->{'src_label'}, $cmd->{'holding_file'});
	my $dest_storage_id = $self->select_or_add_storage($cmd->{'dst_storage'}, $config_id) if defined $cmd->{'dst_storage'};
	my $working_pid = $cmd->{'working_pid'};
	my $status = $cmd->{'status'};
	my $todo = $cmd->{'todo'};
	my $size = $cmd->{'size'};
	my $start_time = $cmd->{'start_time'};
	my $expire = $cmd->{'expire'};
	my $count = $cmd->{'count'};

	$sth->execute($command_id, $operation, $config_id, $src_copy_id, $dest_storage_id, $working_pid, $status, $todo, $size, $start_time, $expire, $count);
    }
}

sub load_table {
    my $self = shift;

    return $self->run_execute($self, $self->can('_load_table'), undef, undef, @_);
}

sub _compute_retention {
    my $self = shift;
    my $localtime = shift || time;
    my $dbh = $self->{'dbh'};
    my $sth;

    debug("_compute_retention");

    my $copy_table = "copy_ids_$$";
    my $volume_table = "volume_ids_$$";

    $sth = $self->make_statement('cr dcp', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $copy_table");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    $sth->finish;

    debug("_compute_retention AA");
    # get all copy_id for this config
    $sth = $self->make_statement('cr cct', "CREATE $self->{'temporary'} TABLE $copy_table AS SELECT copy_id,parent_copy_id,storage_id,images.dump_timestamp,level,retention_days,retention_full,retention_recover FROM copys,images,disks,hosts WHERE hosts.config_id=? AND disks.host_id=hosts.host_id AND images.disk_id=disks.disk_id AND copys.image_id=images.image_id");
    $sth->execute($self->{'config_id'})
	or die "Cannot execute: " . $sth->errstr();
    debug("_compute_retention AA1");

    # set their retention to 1
    $sth = $self->make_statement('cr setret=1', "UPDATE copys SET retention_days=1,retention_full=1,retention_recover=1 WHERE copy_id IN (SELECT copy_id FROM $copy_table)");
    $sth->execute()
	or die "Can't select storage with config_id $self->{'config_id'}: " . $sth->errstr();

    debug("_compute_retention AA2");
    $sth = $self->make_statement('sel sto_name', 'SELECT storage_id, storage_name FROM storages WHERE config_id=?');
    $sth->execute($self->{'config_id'})
	or die "Can't select storage with config_id $self->{'config_id'}: " . $sth->errstr();
    my $all_row_storages = $sth->fetchall_arrayref;
    debug("_compute_retention BB");
    foreach my $storage_row (@$all_row_storages) {
	if ($storage_row->[1] ne "HOLDING") {
	    $self->_compute_retention_storage($copy_table, $storage_row->[0], $storage_row->[1], $localtime);
	}
    }

    debug("_compute_retention CC");
    $sth = $self->make_statement('cr uv=0', 'UPDATE volumes SET retention_days=0, retention_full=0, retention_recover=0');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvd=1', 'UPDATE volumes SET retention_days=1 WHERE retention_days=0 AND volume_id IN (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND retention_days=1)');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvf=1', 'UPDATE volumes SET retention_full=1 WHERE retention_full=0 AND volume_id IN (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND retention_full=1)');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvr=1', 'UPDATE volumes SET retention_recover=1 WHERE retention_recover=0 AND volume_id IN (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND retention_recover=1)');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

#    $sth = $self->make_statement('cr uvd=0', 'UPDATE volumes SET retention_days=0 WHERE retention_days=1 AND NOT EXISTS (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND volumes.volume_id=parts.volume_id AND retention_days=1)');
#    $sth->execute()
#	or die "Cannot execute: " . $sth->errstr();
#
#    $sth = $self->make_statement('cr uvf=0', 'UPDATE volumes SET retention_full=0 WHERE retention_full=1 AND NOT EXISTS (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND volumes.volume_id=parts.volume_id AND retention_full=1)');
#    $sth->execute()
#	or die "Cannot execute: " . $sth->errstr();
#
#    $sth = $self->make_statement('cr uvr=0', 'UPDATE volumes SET retention_recover=0 WHERE retention_recover=1 AND NOT EXISTS (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND volumes.volume_id=parts.volume_id AND retention_recover=1)');
#    $sth->execute()
#	or die "Cannot execute: " . $sth->errstr();

    debug("_compute_retention DD");
    if ($self->{subquery_same_table}) {
	$sth = $self->make_statement('cr uvt=0s', 'UPDATE volumes SET retention_tape=0 WHERE volume_id IN (SELECT volume_id FROM volumes,storages WHERE storage_name=? AND volumes.storage_id=storages.storage_id AND write_timestamp=0)');
	$sth->execute('')
	    or die "Cannot execute: " . $sth->errstr();
    } else {
	$sth = $self->make_statement('dvt', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('cvt', "CREATE $self->{'temporary'} TABLE $volume_table AS SELECT volume_id FROM volumes,storages WHERE storage_name=? AND volumes.storage_id=storages.storage_id AND write_timestamp=0");
	$sth->execute('')
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('cr uvt=0', "UPDATE volumes SET retention_tape=0 WHERE volume_id IN (SELECT volume_id FROM $volume_table)");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('dvt', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
    }

    $sth = $self->make_statement('cr dct', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $copy_table");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    debug("_compute_retention EE");
}

sub compute_retention {
    my $self = shift;

    return $self->run_execute($self, $self->can('_compute_retention'),
			['configs','hosts','disks','pools','storages','images','metas','parts','commands'],
			['copys','volumes'], @_);
}

sub _compute_retention_storage {
    my $self = shift;
    my $copy_table = shift;
    my $storage_id = shift;
    my $storage_name = shift;
    my $localtime = shift || time;
    my $dbh = $self->{'dbh'};
    my $sth;

    debug("_compute_retention_storage");
    my $storage = lookup_storage($storage_name);
    return if !$storage;
    my $policy = lookup_policy(Amanda::Config::storage_getconf($storage, $STORAGE_POLICY));
    return if !$policy;

    debug("_compute_retention_storage AA $storage_name");
    # retention_days
    my $retention_days = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_DAYS);
#    if ($retention_days > 0) {
    {
	my $dump_timestamp = strftime "%Y%m%d%H%M%S", localtime($localtime - $retention_days*24*60*60);

	$sth = $self->make_statement('crs ucd=0', "UPDATE copys SET retention_days=0 WHERE copy_id IN (SELECT $copy_table.copy_id FROM $copy_table WHERE storage_id=? AND dump_timestamp < ?) AND retention_days=1");
	$sth->execute($storage_id, $dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();
    }

    debug("_compute_retention_storage BB");
    # retention_full
    my $retention_full = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_FULL);
#    if ($retention_full > 0) {
    {
	my $dump_timestamp = strftime "%Y%m%d%H%M%S", localtime($localtime - $retention_full*24*60*60);

	$sth = $dbh->prepare("UPDATE copys SET retention_full=0 WHERE copy_id IN (SELECT copy_id FROM $copy_table WHERE storage_id=? AND (dump_timestamp < ? OR level!=0)) AND retention_full=1")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($storage_id, $dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();
    }

    $sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE IF EXISTS A_$$")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE IF EXISTS B_$$")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE IF EXISTS C_$$")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    debug("_compute_retention_storage CC");
    # retention_recover
    my $retention_recover = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_RECOVER);
#    if ($retention_recover > 0) {
    {
	my $dump_timestamp = strftime "%Y%m%d%H%M%S", localtime($localtime - $retention_recover*24*60*60);

	# A = table of retention we keep
	$sth = $dbh->prepare("CREATE $self->{'temporary'} TABLE A_$$ AS SELECT copy_id,parent_copy_id FROM $copy_table WHERE storage_id=$storage_id AND dump_timestamp>=?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();

	# B = table of retention we drop
	$sth = $dbh->prepare("CREATE $self->{'temporary'} TABLE B_$$ AS SELECT copy_id,parent_copy_id FROM $copy_table WHERE storage_id=$storage_id AND dump_timestamp<?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();

	while(1) {
	    # C = table to move from drop to keep
	    $sth = $dbh->prepare("CREATE $self->{'temporary'} TABLE C_$$ AS SELECT DISTINCT B_$$.copy_id,B_$$.parent_copy_id FROM A_$$,B_$$ WHERE A_$$.parent_copy_id=B_$$.copy_id")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    $sth = $dbh->prepare("SELECT * FROM C_$$")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    my $parent_row = $sth->fetchrow_arrayref;

	    last if !defined $parent_row;

	    $sth = $dbh->prepare("INSERT INTO A_$$ SELECT * FROM C_$$")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    $sth = $dbh->prepare("DELETE FROM B_$$ WHERE EXISTS (SELECT copy_id FROM C_$$ WHERE C_$$.copy_id=B_$$.copy_id)")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    $sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE C_$$")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();
	};

	$sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE IF EXISTS C_$$")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $dbh->prepare("UPDATE copys SET retention_recover=0 WHERE copy_id IN (SELECT copy_id FROM B_$$)")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE B_$$")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $dbh->prepare("DROP $self->{'drop_temporary'} TABLE A_$$")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
    }

    debug("_compute_retention_storage DD");
    $self->_compute_storage_retention_tape($storage_name,
				storage_getconf($storage, $STORAGE_TAPEPOOL),
				policy_getconf($policy, $POLICY_RETENTION_TAPES));
    debug("_compute_retention_storage EE");
}

sub _remove_volume {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $volume = $self->_find_volume($pool, $label);
    $volume->_remove() if $volume;
}

sub remove_volume {
    my $self = shift;

    return $self->run_execute($self, $self->can('_remove_volume'),
			['pools','storages','metas','configs'],
			['volumes','parts','copys','images'], @_);
}

sub XXXremove_label_old {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    # get the volume_id
    $sth = $dbh->prepare("SELECT volume_id FROM volumes,pools WHERE pool_name=? AND label=? AND volumes.pool_id=pools.pool_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($pool, $label)
	or die "Cannot execute: " . $sth->errstr();
    my $volume_row = $sth->fetchrow_arrayref;
    my $volume_id = $volume_row->[0];

    # get a list of all copy_id modified
    $sth = $dbh->prepare("SELECT DISTINCT copy_id FROM parts WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();
    my %copy_mod;
    while (my $part_row = $sth->fetchrow_arrayref) {
	my $copy_id = $part_row->[0];
	$copy_mod{$copy_id} = 1;
    }

    # delete the part
    $sth = $dbh->prepare("DELETE FROM parts WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();

    # delete the volume
    $sth = $dbh->prepare("DELETE FROM volumes WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();


    my %image_mod;
    my $sth_image;
    my $sth_copy;
    # get a list of all image_id where the copy is modified or deleted
    $sth_copy = $dbh->prepare("SELECT image_id FROM copys WHERE copy_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    for my $copy_id (keys %copy_mod) {
	$sth_copy->execute($copy_id)
	    or die "Cannot execute: " . $sth_copy->errstr();
	my $copy_row = $sth_copy->fetchrow_arrayref();
	my $image_id = $copy_row->[0];
	$image_mod{$image_id} = 1;
    }

    # delete the copy if all their part are removed
    $sth = $dbh->prepare("SELECT copy_id FROM parts WHERE copy_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_remove_copy = $dbh->prepare("DELETE FROM copys WHERE copy_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_update_copy = $dbh->prepare("UPDATE copys SET copy_status=? WHERE copy_id=?")
	or die "Cannot prepare: " . $dbh->errstr();

    for my $copy_id (keys %copy_mod) {
	#do the complete copy is removed
	$sth->execute($copy_id)
	    or die "Cannot execute: " . $sth->errstr();
	my $part_avail = $sth->fetchrow_arrayref();
	if (!defined $part_avail) {
	    $sth_remove_copy->execute($copy_id)
		or die "Cannot execute: " . $sth_remove_copy->errstr();
	} else {
	    # update the copy_status to PARTIAL.
	    $sth_update_copy->execute("PARTIAL", $copy_id)
		or die "Cannot execute: " . $sth_update_copy->errstr();
	}
    }

    my $sth_disk;
    my $sth_host;

    my %deleted_disks;

    # remove image or adjust their status
    $sth_image = $dbh->prepare("SELECT image_id, disk_id, dump_status FROM images WHERE image_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_copy = $dbh->prepare("SELECT copy_status FROM copys WHERE image_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
   my $sth_image_remove = $dbh->prepare("DELETE FROM images WHERE image_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_image_update = $dbh->prepare("UPDATE images SET dump_status=\"PARTIAL\" WHERE image_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    for my $image_id (keys %image_mod) {
	$sth_image->execute($image_id)
	    or die "Cannot execute: " . $sth->errstr();
	my $image_row = $sth_image->fetchrow_arrayref();
	$sth_copy->execute($image_id)
	    or die "Cannot execute: " . $sth->errstr();
	my $copy_row = $sth_copy->fetchall_arrayref();
	if (!defined $copy_row || @{$copy_row} == 0) { #remove the image
	    $sth_image_remove->execute($image_id)
		or die "Cannot execute: " . $sth_image_remove-->errstr();
	    $deleted_disks{$image_row->[1]} = 1;
	} elsif ($image_row->[2] eq "OK") {
	    my $update_status = 1;
	    for my $copy (@{$copy_row}) {
		$update_status = 0 if $copy->[0] eq "OK";
	    }
	    if ($update_status) {
		$sth_image_update->execute($image_id)
		    or die "Cannot execute: " . $sth_image_update->errstr();
	    } else {
	    }
	} else {
	}
    }

    my $sth_remove_disk;
    my %deleted_host;

    #remove dle if needed
    $sth_disk = $dbh->prepare("SELECT disk_id, host_id FROM disks WHERE disk_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_image = $dbh->prepare("SELECT disk_id FROM images WHERE disk_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_remove_disk = $dbh->prepare("DELETE FROM disks WHERE disk_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    for my $disk_id (keys %deleted_disks) {
	$sth_disk->execute($disk_id)
	    or die "Cannot execute: " . $sth_disk->errstr();
	my $disk_row = $sth_disk->fetchrow_arrayref();
	$sth_image->execute($disk_id)
	    or die "Cannot execute: " . $sth_image->errstr();
	my $image_row = $sth_image->fetchall_arrayref();
	if (!defined $image_row || @{$image_row} == 0) {
	    $sth_remove_disk->execute($disk_id)
		or die "Cannot execute: " . $sth_remove_disk->errstr();
	    $deleted_host{$disk_row->[1]} = 1;
	}
    }

    my $sth_remove_host;

    #remove host if needed
    $sth_disk = $dbh->prepare("SELECT disk_id FROM disks WHERE host_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_remove_host = $dbh->prepare("DELETE FROM hosts WHERE host_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    for my $host_id (keys %deleted_host) {
	$sth_disk->execute($host_id)
	    or die "Cannot execute: " . $sth->errstr();
	my $disk_row = $sth_disk->fetchall_arrayref();
	if (!defined $disk_row || @{$disk_row} == 0) {
	    $sth_remove_host->execute($host_id)
		or die "Cannot execute: " . $sth_remove_host->errstr();
	}
    }
}

#sub _add_flush_command {
#    my $self = shift;
#    my %params = @_;
#    my $dbh = $self->{'dbh'};
#    my $sth;
#    my $command_id;
#
#    $sth = $dbh->prepare("SELECT copy_id FROM copys, images, disks, hosts, storages, pools, configs WHERE copys.image_id=images.image_id AND images.dump_timestamp=? AND images.disk_id=disks.disk_id AND disks.disk_name=? AND disks.host_id=hosts.host_id AND hosts.host_name=? AND copys.storage_id=storages.storage_id AND storages.storage_name=? AND storages.config_id=?")
#	or die "Cannot prepare: " . $dbh->errstr();
#    $sth->execute($params{dump_timestamp}, $params{diskname}, $params{hostname}, "HOLDING", $self->{'config_id'})
#	or die "Cannot execute: " . $sth->errstr();
#    my $copy_row = $sth->fetchrow_arrayref;
#    if (!$copy_row) {
#	die("TODO: Must add the copy");
#    }
#    my $src_copy_id = $copy_row->[0];
#
#    $sth = $dbh->prepare("SELECT storage_id FROM storages, configs WHERE storages.storage_name=? AND storages.config_id=?")
#	or die "Cannot prepare: " . $dbh->errstr();
#    $sth->execute($params{dst_storage}, $self->{'config_id'})
#	or die "Cannot execute: " . $sth->errstr();
#    my $storage_row = $sth->fetchrow_arrayref;
#    my $dest_storage_id;
#    if ($storage_row && defined $storage_row->[0]) {
#	$dest_storage_id = $storage_row->[0];
#    } else {
#	$sth = $self->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
#	$sth->execute($self->{'config_id'}, $params{'dst_storage'}, 0)
#	    or die "Cannot execute: " . $sth->errstr();
#	$dest_storage_id = $dbh->last_insert_id(undef, undef, "storages", undef);
#    }
#
#    $sth = $dbh->prepare("INSERT INTO commands VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
#	or die "Cannot prepare: " . $dbh->errstr();
#    $sth->execute(undef, $Amanda::Cmdfile::CMD_FLUSH, $self->{'config_id'},
#	$src_copy_id, $dest_storage_id, 0, 0, $Amanda::Cmdfile::CMD_TODO, 0, 0, 0, 0)
#	or die "Cannot execute: " . $sth->errstr();
#    my $id = $dbh->last_insert_id(undef, undef, "commands", undef);
#    return $id;
#}
#
#sub add_flush_command {
#    my $self = shift;
#
#    return $self->run_execute($self, $self->can('_add_flush_command'),
#			['copys', 'images', 'disks', 'hosts', 'pools', 'configs'],
#			['storage','commands'], @_);
#}

sub _get_cmdflush_ids_for_holding {
    my $self = shift;
    my $holding_file = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my @ids;

    $sth = $dbh->prepare("SELECT DISTINCT command_id, storage_name FROM commands,storages,parts,volumes WHERE label=? AND commands.config_id=? AND operation=? AND storages.storage_id=dest_storage_id AND commands.src_copy_id=parts.copy_id AND parts.volume_id=volumes.volume_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($holding_file, $self->{'config_id'}, $Amanda::Cmdfile::CMD_FLUSH)
	or die "Cannot execute: " . $sth->errstr();
    while (my $cmd_row = $sth->fetchrow_arrayref) {
	my $command_id = $cmd_row->[0];
	my $dest_storage_name = $cmd_row->[1];
	push @ids, "$command_id;$dest_storage_name"
    }
    return \@ids;
}

sub get_cmdflush_ids_for_holding {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_cmdflush_ids_for_holding'),
			['commands', 'storages', 'parts', 'volumes'],
			undef, @_);
}

sub _get_command_ids_for_holding {
    my $self = shift;
    my $holding_file = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my @ids;

    $sth = $dbh->prepare("SELECT DISTINCT command_id, storage_name FROM commands,storages,parts,volumes WHERE label=? AND commands.config_id=? AND storages.storage_id=dest_storage_id AND commands.src_copy_id=parts.copy_id AND parts.volume_id=volumes.volume_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($holding_file, $self->{'config_id'})
	or die "Cannot execute: " . $sth->errstr();
    while (my $cmd_row = $sth->fetchrow_arrayref) {
	my $command_id = $cmd_row->[0];
	my $dest_storage_name = $cmd_row->[1];
	push @ids, "$command_id;$dest_storage_name"
    }
    return \@ids;
}

sub get_command_ids_for_holding {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_command_ids_for_holding'),
			['commands', 'storages', 'parts', 'volumes'],
			undef, @_);
}


sub _merge {
    my $self = shift;
    my $tapelist = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
}

sub merge {
    my $self = shift;

    return $self->run_execute($self, $self->can('_merge'), undef, undef, @_);
}

sub _validate {
    my $self = shift;
    my $tapelist = shift;

    my $dbh = $self->{'dbh'};
    my $sth_host;
    my $sth_disk;
    my $sth_image;
    my $sth_copy;
    my $sth_part;
    my $sth_volume;
    my %host;
    my %disks;
    my %disk_hosts;
    my %image;
    my %image_disks;
    my %copy;
    my %copy_image;
    my %part_copy;
    my %part_volume;
    my %volume;
    my $host_id;
    my $host_name;
    my $disk_id;
    my $disk_disk_name;
    my $image_id;
    my $image_timestamp;
    my $copy_id;
    my $copy_timestamp;
    my $part_id;
    my $volume_id;
    my $volume_label;

    $tapelist = $self->{'tapelist'} if !$tapelist;
    # check all holdingdisk volume still exist
    $sth_volume = $dbh->prepare("SELECT volume_id, label FROM volumes, pools WHERE volumes.pool_id=pools.pool_id AND pool_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume->execute("HOLDING")
	or die "Cannot execute: " . $sth_volume->errstr();
    while (my $volume_row = $sth_volume->fetchrow_arrayref) {
	my $hfile = $volume_row->[1];
	my $hdr = Amanda::Holding::get_header($hfile);
	if (!$hdr) {
	    print "holding file $hfile is in database but do not exists.\n";
	}
    }

    # check all holding file have a corresponding holdingdisk volume.
    $sth_volume = $dbh->prepare("SELECT volume_id FROM volumes WHERE label=?")
	or die "Cannot prepare: " . $dbh->errstr();
    for my $hfile (Amanda::Holding::files()) {
	$sth_volume->execute($hfile)
	    or die "Cannot execute: " . $sth_volume->errstr();
	if (!$sth_volume->fetchrow_arrayref) {
	    print "holding file $hfile is not in the database\n";
	}
    }

    # check volume are still in the tapelist file
    $sth_volume = $dbh->prepare("SELECT volume_id, pool_name, label, write_timestamp FROM volumes, pools WHERE volumes.pool_id=pools.pool_id AND pool_name!=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume->execute("HOLDING")
	or die "Cannot execute: " . $sth_volume->errstr();
    while (my $volume_row = $sth_volume->fetchrow_arrayref) {
	my $pool = $volume_row->[1];
	my $label = $volume_row->[2];
	my $datestamp = $volume_row->[3];
	my $tle = $tapelist->lookup_tape_by_pool_label($pool, $label);
	if (!$tle) {
	    print "Label '$label' is not in the tapelist\n";
	} elsif ($datestamp != $tle->{'datestamp'}) {
	    print "Label '$label' have datestamp '$datestamp' but tapelist have datestamp '$tle->{'datestamp'}'\n";
	}
    }

    # check tapelist entries are in the volumes table
    $sth_volume = $dbh->prepare("SELECT volume_id FROM volumes, pools WHERE volumes.pool_id=pools.pool_id AND pool_name=? AND label=?")
	or die "Cannot prepare: " . $dbh->errstr();
    foreach my $tle (@{$tapelist->{'tles'}}) {
	if ($tle->{'pool'} ne "HOLDING") {
	    $sth_volume->execute($tle->{'pool'}, $tle->{'label'})
		or die "Cannot execute: " . $sth_volume->errstr();
	    if (!$sth_volume->fetchrow_arrayref) {
		print "Tapelist ($tle->{'pool'}:$tle->{'label'}) is not in the volumes table\n";
	    }
	}
    }
    # get a hash of all host_id
    $sth_host = $dbh->prepare("SELECT host_id, host_name FROM hosts")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_host->execute()
	or die "Cannot execute: " . $sth_host->errstr();
    while (my $host_row = $sth_host->fetchrow_arrayref) {
	$host_id   = $host_row->[0];
	$host_name = $host_row->[1];
	$host{$host_id} = $host_name;
    }

    # get a list of all disk_id and host_id listed in disks table.
    $sth_disk = $dbh->prepare("SELECT disk_id, host_id, disk_name FROM disks")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_disk->execute()
	or die "Cannot execute: " . $sth_disk->errstr();
    while (my $disk_row = $sth_disk->fetchrow_arrayref) {
	$disk_id       = $disk_row->[0];
	$host_id      = $disk_row->[1];
	$disk_disk_name = $disk_row->[2];
	$disks{$disk_id} = $disk_disk_name;
	$disk_hosts{$host_id} = $disk_id;
    }

    while (($host_id, $host_name) = each %host) {
	if (!exists $disk_hosts{$host_id}) {
	    print "host $host_id ($host_name) have no disk\n";
	} else {
	    delete $disk_hosts{$host_id}
	}
    }
    while (($disk_id, $host_id) = each %disk_hosts) {
	print "host $host_id in disks table ($disk_id) have no host entry\n";
    }

    # get a list of all image_id and disk_id listed in image table.
    $sth_image = $dbh->prepare("SELECT image_id, disk_id, dump_timestamp FROM images")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_image->execute()
	or die "Cannot execute: " . $sth_image->errstr();
    while (my $image_row = $sth_image->fetchrow_arrayref) {
	$image_id        = $image_row->[0];
	$disk_id          = $image_row->[1];
	$image_timestamp = $image_row->[2];
	$image{$image_id} = $image_timestamp;
	$image_disks{$disk_id} = $image_id;
    }

    while (($disk_id, $disk_disk_name) = each %disks) {
	if (!exists $image_disks{$disk_id}) {
	    print "disk $disk_id ($disk_disk_name) have no image\n";
	} else {
	    delete $image_disks{$disk_id}
	}
    }
    while (($disk_id, $image_id) = each %image_disks) {
	print "disk $disk_id in image table ($image_id) have no disk entry\n";
    }

    # get a list of all copy_id and image_id listed in copy table.
    $sth_copy = $dbh->prepare("SELECT copy_id, image_id, write_timestamp FROM copys")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_copy->execute()
	or die "Cannot execute: " . $sth_copy->errstr();
    while (my $copy_row = $sth_copy->fetchrow_arrayref) {
	$copy_id        = $copy_row->[0];
	$image_id       = $copy_row->[1];
	$copy_timestamp = $copy_row->[2];
	$copy{$copy_id} = $copy_timestamp;
	$copy_image{$image_id} = $copy_id;
    }

    while (($image_id, $image_timestamp) = each %image) {
	if (!exists $copy_image{$image_id}) {
	    print "image $image_id ($image_timestamp) have no copy\n";
	} else {
	    delete $copy_image{$image_id}
	}
    }
    while (($image_id, $copy_id) = each %copy_image) {
	print "image $image_id in copy table ($copy_id) have no image entry\n";
    }

    # get a list of all part_id and copy_id/volume_id listed in part table.
    $sth_part = $dbh->prepare("SELECT part_id, copy_id, volume_id FROM parts")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_part->execute()
	or die "Cannot execute: " . $sth_part->errstr();
    while (my $part_row = $sth_part->fetchrow_arrayref) {
	$part_id        = $part_row->[0];
	$copy_id        = $part_row->[1];
	$volume_id      = $part_row->[2];
	$part_copy{$copy_id} = $part_id;
	$part_volume{$volume_id} = $part_id;
    }

    while (($copy_id, $copy_timestamp) = each %copy) {
	if (!exists $part_copy{$copy_id}) {
	    print "copy $copy_id ($copy_timestamp) have no part\n";
	} else {
	    delete $part_copy{$copy_id}
	}
    }
    while (($copy_id, $volume_id) = each %part_copy) {
	print "copy $copy_id in part table ($part_id) have no copy entry\n";
    }

    #get a list off all volume_id
    $sth_volume = $dbh->prepare("SELECT volume_id, label FROM volumes")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume->execute()
	or die "Cannot execute: " . $sth_volume->errstr();
    while (my $volume_row = $sth_volume->fetchrow_arrayref) {
	$volume_id    = $volume_row->[0];
	$volume_label = $volume_row->[1];
	$volume{$volume_id} = $volume_label;
    }

    while (($volume_id, $volume_label) = each %volume) {
	if (!exists $volume{$volume_id}) {
	    print "volume $volume_id ($volume_label) have no part\n";
	} else {
	    delete $part_volume{$volume_id}
	}
    }
    while (($volume_id, $part_id) = each %part_volume) {
	print "volume $volume_id in part table ($part_id) have no volume entry\n";
    }

    # check copys/parts match Amanda::DB::Catalog
    # ????
    # ????
}

sub validate {
    my $self = shift;

    return $self->run_execute($self, $self->can('_validate'), undef, undef, @_);
}

sub _remove_working_cmd {
    my $self = shift;
    my $pid = shift;

    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("UPDATE commands SET working_pid=0 WHERE working_pid=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($pid)
	or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM commands WHERE status=? AND working_pid=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($Amanda::Cmdfile::CMD_DONE, 0)
	or die "Cannot execute: " . $sth->errstr();
}

sub remove_working_cmd {
    my $self = shift;

    return $self->run_execute($self, $self->can('_remove_working_cmd'),
			undef,
			['commands'], @_);
}

sub _remove_cmd {
    my $self = shift;
    my $id = shift;

    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("DELETE FROM commands WHERE command_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($id)
	or die "Cannot execute: " . $sth->errstr();
}

sub remove_cmd {
    my $self = shift;

    return $self->run_execute($self, $self->can('_remove_cmd'),
			undef,
			['commands'], @_);
}

sub select_or_add_config {
    my $self = shift;
    my $config_name = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $config_id;
    if (defined $config_name) {
	$config_id = $self->{'configs_id'}{$config_name};
    } else {
	$config_id = $self->{'config_id'};
	$config_name = $self->{'config_name'};
    }
    if (!defined $config_id) {
	$sth = $self->make_statement('sel con_id', 'SELECT config_id FROM configs WHERE config_name=?');
	$sth->execute($config_name)
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $config_row = $sth->fetchrow_arrayref;
	if (!defined $config_row) {
	    my $sth1 = $self->make_statement('in con', 'INSERT INTO configs(config_name, created) VALUES (?, ?)');
	    $sth1->execute($config_name, 0)
		or die "Can't add config $config_name: " . $sth1->errstr();
	    $config_id = $dbh->last_insert_id(undef, undef, "configs", undef);
	} else {
	    $config_id = $config_row->[0];
	}
	$self->{'configs_id'}{$config_name} = $config_id;
    }
    return $config_id;
}

sub select_or_add_host {
    my $self = shift;
    my $host_name = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $host_id = $self->{'hosts_id'}{$self->{'config_id'}}{$host_name};
    if (!defined $host_id) {
	$sth = $self->make_statement('sel hos_id', 'SELECT host_id FROM hosts WHERE host_name=? AND config_id=?');
	$sth->execute($host_name, $self->{'config_id'})
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $host_row = $sth->fetchrow_arrayref;
	if (!defined $host_row) {
	    my $sth1 = $self->make_statement('in hos', 'INSERT INTO hosts(config_id, host_name) VALUES (?, ?)');
	    $sth1->execute($self->{'config_id'}, $host_name)
		 or die "Can't add host $host_name: " . $sth1->errstr();
	    $host_id = $dbh->last_insert_id(undef, undef, "hosts", undef);
	} else {
	    $host_id = $host_row->[0];
	}
	$self->{'hosts_id'}{$self->{'config_id'}}{$host_name} = $host_id;
    }
    return $host_id;
}

sub select_or_add_disk {
    my $self = shift;
    my $host_id = shift;
    my $disk_name = shift;
    my $device = shift || $disk_name;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $disk_id = $self->{'disks_id'}{$self->{'config_id'}}{$host_id}{$disk_name};
    if (!defined $disk_id) {
	$sth = $self->make_statement('sel dis_id', 'SELECT disk_id FROM disks WHERE host_id=? AND disk_name=?');
	$sth->execute($host_id, $disk_name)
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $disk_row = $sth->fetchrow_arrayref;
	if (!defined $disk_row) {
	    my $sth1 = $self->make_statement('in dis', 'INSERT INTO disks(host_id, disk_name, device, force_full, force_level_1, force_bump, force_no_bump) VALUES (?, ?, ?, ?, ?, ?, ?)');
	    $sth1->execute($host_id, $disk_name, $device, 0, 0, 0, 0)
		or die "Can't add disk $disk_name: " . $sth1->errstr();
	    $disk_id = $dbh->last_insert_id(undef, undef, "disks", undef);
	} else {
	    $disk_id = $disk_row->[0];
	}
	$self->{'disks_id'}{$self->{'config_id'}}{$host_id}{$disk_name} = $disk_id;
    }
    return $disk_id;
}

sub select_or_add_storage {
    my $self = shift;
    my $storage_name = shift;
    my $config_id = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $config_id = $self->{'config_id'} if !defined $config_id;
    my $storage_id = $self->{'storages_id'}{$config_id}{$storage_name};
    if (!defined $storage_id) {
	$sth = $self->make_statement('sel sto_id', 'SELECT storage_id FROM storages WHERE storage_name=? AND config_id=?');
	$sth->execute($storage_name, $config_id)
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $storage_row = $sth->fetchrow_arrayref;
	if (!defined $storage_row) {
	    my $sth1 = $self->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
	    $sth1->execute($config_id, $storage_name, 0)
		or die "Can't add storage $storage_name: " . $sth1->errstr();
	    $storage_id = $dbh->last_insert_id(undef, undef, "storages", undef);
	} else {
	    $storage_id = $storage_row->[0];
	}
	$self->{'storages_id'}{$config_id}{$storage_name} = $storage_id;
    }
    return $storage_id;
}

sub select_or_add_holding_volume {
    my $self = shift;
    my $pool_id = shift;
    my $storage_id = shift;
    my $meta_id = shift;
    my $holding_file = shift;
    my $write_timestamp = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $volume_id;

    $sth = $self->make_statement('sel vol_id', 'SELECT volume_id FROM volumes WHERE pool_id=? AND label=?');
    $sth->execute($pool_id, $holding_file)
	or die "Can't find volume pool:$pool_id $holding_file" . $sth->errstr();
    my $volume_row = $sth->fetchrow_arrayref;
    if (!defined $volume_row) {
	$sth = $self->make_statement('in vol', 'INSERT INTO volumes(pool_id,label,reuse,storage_id,meta_id,write_timestamp,orig_write_timestamp,retention_days,retention_full,retention_recover,retention_tape,retention_cmd,block_size,barcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
	$sth->execute($pool_id, $holding_file, 0, $storage_id, $meta_id, $write_timestamp, $write_timestamp, 1, 1, 1, 1, 0, 32, undef)
	    or die "Can't add volume $pool_id:$holding_file $write_timestamp: M:$meta_id" . $sth->errstr();
	$volume_id = $dbh->last_insert_id(undef, undef, "volumes", undef);
    } else {
	$volume_id = $volume_row->[0];
    }

    return $volume_id;
}

sub select_or_add_copy {
    my $self = shift;
    my $image_id = shift;
    my $storage_id = shift;
    my $write_timestamp = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my $copy_id;

    $sth = $self->make_statement('sel cop_id', 'SELECT copy_id FROM copys WHERE image_id=? AND storage_id=? AND write_timestamp=?');
    $sth->execute($image_id, $storage_id, $write_timestamp)
	or die "Can't find copy image:$image_id storage:$storage_id write_timestamps:$write_timestamp: " . $sth->errstr();
    my $copy_row = $sth->fetchrow_arrayref;
    if (!defined $copy_row) {
	$sth = $self->make_statement('in cop', 'INSERT INTO copys(image_id,storage_id,write_timestamp,parent_copy_id,nb_parts,kb,bytes,copy_status,server_crc,retention_days,retention_full,retention_recover,copy_message,copy_pid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
	$sth->execute($image_id, $storage_id, $write_timestamp, 0, 1, undef, undef, "OK", undef, 1, 1, 1, undef, 0)
	    or die "Can't add copy $image_id:$storage_id: " . $sth->errstr();
	$copy_id = $dbh->last_insert_id(undef, undef, "copys", undef);
    } else {
	$copy_id = $copy_row->[0];
    }

    return $copy_id;
}

sub select_or_add_pool {
    my $self = shift;
    my $pool_name = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $pool_id = $self->{'pools_id'}{$pool_name};
    if (!defined $pool_id) {
	$sth = $self->make_statement('sel poo_id', 'SELECT pool_id FROM pools WHERE pool_name=?');
	$sth->execute($pool_name)
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $pool_row = $sth->fetchrow_arrayref;
	if (!defined $pool_row) {
	    my $sth1 = $self->make_statement('in poo', 'INSERT INTO pools(pool_name) VALUES (?)');
	    $sth1->execute($pool_name)
		or die "Can't add pool $pool_name: " . $sth1->errstr();
	    $pool_id = $dbh->last_insert_id(undef, undef, "pools", undef);
	} else {
	    $pool_id = $pool_row->[0];
	}
	$self->{'pools_id'}{$pool_name} = $pool_id;
    }
    return $pool_id;
}

sub select_or_add_meta {
    my $self = shift;
    my $meta_label = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $meta_id = $self->{'metas_id'}{$meta_label};
    if (!defined $meta_id) {
	if ($meta_label) {
	    # get/add the meta */
	    $sth = $self->make_statement('sel met_id', 'SELECT meta_id FROM metas WHERE meta_label=?');
	    $sth->execute($meta_label)
		or die "Cannot execute: " . $sth->errstr();
	    # get the first row
	    my $meta_row = $sth->fetchrow_arrayref;
	    if (!defined $meta_row) {
		my $sth1 = $self->make_statement('in met','INSERT INTO metas(meta_label) VALUES (?)');
		$sth1->execute($meta_label) or die "Can't add meta $meta_label " . $sth1->errstr();
		$meta_id = $dbh->last_insert_id(undef, undef, "metas", undef);
	    } else {
		$meta_id = $meta_row->[0];
	    }
	    $self->{'metas_id'}{$meta_label} = $meta_id;
	} else {
	    $meta_id = $self->{'default_meta_id'};
	    $self->{'metas_id'}{''} = $meta_id;
	}
    }

    return $meta_id;
}

sub select_or_add_image {
    my $self = shift;
    my $disk_id = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $based_on_timestamp = shift;
    my $pid = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my $sth1;

    $pid = 0 if !defined $pid;
    $sth = $self->make_statement('sel ima_id_status', 'SELECT image_id, dump_status FROM images WHERE disk_id=? AND dump_timestamp=? AND level=?');
    $sth->execute($disk_id, $dump_timestamp, $level)
        or die "Cannot execute: " . $sth->errstr();
    # get the first row
    my $image_row = $sth->fetchrow_arrayref;
    my $image_id;
    my $dump_status;
    if (!defined $image_row) {
	my $parent_image_id = 0;
	# find the parent
	if (defined $level && $level > 0) {
	    my $parent_image_id = 0;
	    my $parent_level = $level-1;
	    $sth1 = $self->make_statement('find_parent_id', 'SELECT image_id FROM images WHERE images.disk_id=? AND level=? AND dump_timestamp<? ORDER BY dump_timestamp DESC LIMIT 1');
	    $sth1->execute($disk_id, $parent_level, $dump_timestamp)
		or die "Can't execute: " . $sth1->errstr();
	    my $image_row = $sth1->fetchrow_arrayref;
	    if (!defined $image_row) {
		#no parent
	    } else {
		$parent_image_id = $image_row->[0];
	    }
	}

	$sth1 = $self->make_statement('in ima', 'INSERT INTO images(disk_id, dump_timestamp, level, parent_image_id, orig_kb, dump_status, nb_files, nb_directories, based_on_timestamp, native_crc, client_crc, server_crc, dump_message, image_pid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
	$sth1->execute($disk_id, $dump_timestamp, $level, $parent_image_id, undef, "DUMPING", undef, undef, $based_on_timestamp, undef, undef, undef, undef, $pid)
	    or die "Can't add image $disk_id $level $dump_timestamp: " . $sth1->errstr();
	$image_id = $dbh->last_insert_id(undef, undef, "images", undef);
	$dump_status = "DUMPING";
    } else {
	$image_id = $image_row->[0];
        $dump_status = $image_row->[1];
    }

    return ($image_id, $dump_status);
}

sub _add_flush_cmd {
    my $self = shift;
    my %params = @_;
    my $dbh = $self->{'dbh'};
    my $sth;
    my $row;

    $sth = $dbh->prepare("SELECT config_id FROM configs WHERE config_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($params{'config'})
	or die "Cannot execute: " . $sth->errstr();
    $row = $sth->fetchrow_arrayref;
    die ("add_flush_cmd AA") if !$row || !defined $row->[0];
    my $config_id = $row->[0];

    my $src_copy_id;
    my $s = "SELECT copys.copy_id, copy_status, copy_pid, image_pid, dump_status FROM copys, storages, images, disks, hosts, volumes WHERE copys.storage_id=storages.storage_id AND storages.storage_name=? AND copys.image_id=images.image_id AND images.dump_timestamp=? AND images.level=? AND images.disk_id=disks.disk_id AND disks.disk_name=? AND disks.host_id=hosts.host_id AND hosts.host_name=? AND volumes.label=? AND copys.storage_id=volumes.storage_id";
    $sth = $dbh->prepare($s)
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute('HOLDING', $params{'dump_timestamp'}, $params{'level'}, $params{'diskname'}, $params{'hostname'}, $params{'holding_file'})
	or die "Cannot execute: " . $sth->errstr();
    $row = $sth->fetchrow_arrayref;
    if (!$row || !defined $row->[0]) {
	# select or add host
	my $host_id = $self->select_or_add_host($params{'hostname'});

	# select or add disk
	my $disk_id = $self->select_or_add_disk($host_id, $params{'diskname'});

	# select or add images
	my ($image_id, $dump_stataus) = $self->select_or_add_image($disk_id, $params{'dump_timestamp'}, $params{'level'}, $params{'based_on_timestamp'}, $params{'pid'});

	# select or add storage
	my $storage_id = $self->select_or_add_storage('HOLDING');

	# select or add pool
	my $pool_id = $self->select_or_add_pool('HOLDING');

	# select or add meta
	my $meta_id = $self->select_or_add_meta('');

	# select or add volumes
	my $volume_id = $self->select_or_add_holding_volume($pool_id, $storage_id, $meta_id, $params{'holding_file'}, $params{'dump_timestamp'});

	# select or add copys
	$src_copy_id = $self->select_or_add_copy($image_id, $storage_id, $params{'dump_timestamp'})

	# select or add parts

	#die ("add_flush_cmd no copy");
    } else {
	$src_copy_id = $row->[0];
    }

    # select or add dest storage
    my $dest_storage_id = $self->select_or_add_storage($params{'dst_storage'});

    $sth = $self->make_statement('in com', 'INSERT INTO commands(operation,config_id,src_copy_id,dest_storage_id,working_pid,status,todo,size,start_time,expire,count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    $sth->execute($Amanda::Cmdfile::CMD_FLUSH, $config_id, $src_copy_id, $dest_storage_id, $params{'working_pid'}, $params{'status'}, $Amanda::Cmdfile::CMD_TODO, 0, 0, 0, 0)
	or die "Cannot execute: " . $sth->errstr();
    my $cmd_id = $dbh->last_insert_id(undef, undef, "commands", undef);

    return $cmd_id;
}

sub add_flush_cmd {
    my $self = shift;

    return $self->run_execute($self, $self->can('_add_flush_cmd'),
			undef,
			['configs', 'copys', 'images', 'disks', 'hosts', 'pools', 'metas', 'volumes', 'storages', 'commands'], @_);
}

sub _add_copy_cmd {
    my $self = shift;
    my %params = @_;
    my $dbh = $self->{'dbh'};
    my $sth;
    my $row;

    $sth = $dbh->prepare("SELECT config_id FROM configs WHERE config_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($params{'config'})
	or die "Cannot execute: " . $sth->errstr();
    $row = $sth->fetchrow_arrayref;
    die ("add_flush_cmd AA") if !$row || !defined $row->[0];
    my $config_id = $row->[0];

    my $src_copy_id;
    my $s = "SELECT copys.copy_id, copy_status, copy_pid, image_pid, dump_status FROM copys, storages, images, disks, hosts, volumes WHERE copys.storage_id=storages.storage_id AND storages.storage_name=? AND copys.image_id=images.image_id AND images.dump_timestamp=? AND images.level=? AND images.disk_id=disks.disk_id AND disks.disk_name=? AND disks.host_id=hosts.host_id AND hosts.host_name=? AND volumes.label=? AND copys.storage_id=volumes.storage_id";
    $sth = $dbh->prepare($s)
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($params{'src_storage'}, $params{'dump_timestamp'}, $params{'level'}, $params{'diskname'}, $params{'hostname'}, $params{'label'})
	or die "Cannot execute: " . $sth->errstr();
    $row = $sth->fetchrow_arrayref;
    if (!$row || !defined $row->[0]) {
	die ("_add_copy_cmd no image found");
    } else {
	$src_copy_id = $row->[0];
    }

    # select or add dest storage
    my $dest_storage_id = $self->select_or_add_storage($params{'dst_storage'});

    $sth = $self->make_statement('in com', 'INSERT INTO commands(operation,config_id,src_copy_id,dest_storage_id,working_pid,status,todo,size,start_time,expire,count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    $sth->execute($Amanda::Cmdfile::CMD_COPY, $config_id, $src_copy_id, $dest_storage_id, $params{'working_pid'}, $params{'status'}, $Amanda::Cmdfile::CMD_TODO, $params{'size'}, $params{'start_time'}, 0, 0)
	or die "Cannot execute: " . $sth->errstr();
    my $cmd_id = $dbh->last_insert_id(undef, undef, "commands", undef);

    return $cmd_id;
}

sub add_copy_cmd {
    my $self = shift;

    return $self->run_execute($self, $self->can('_add_copy_cmd'),
			undef,
			['configs', 'copys', 'images', 'disks', 'hosts', 'pools', 'metas', 'volumes', 'storages', 'commands'], @_);
}

sub _get_command_from_id {
    my $self = shift;
    my $id = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $cmd;

    $sth = $dbh->prepare("SELECT command_id, operation, commands.config_id, config_name, src_copy_id, storage_name, working_pid, status, todo, size, start_time, expire, count FROM commands,configs,storages WHERE commands.command_id=? AND commands.config_id=configs.config_id AND commands.dest_storage_id=storages.storage_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($id)
	or die "Cannot execute: " . $sth->errstr();
    my $command_row = $sth->fetchrow_arrayref;
    if ($command_row) {
	my $command_id = $command_row->[0];
	my $operation = $command_row->[1];
	my $config_id = $command_row->[2];
	my $config_name = $command_row->[3];
	my $src_copy_id = $command_row->[4];
	my $dest_storage_name = $command_row->[5];
	my $working_pid = $command_row->[6];
	my $status = $command_row->[7];
	my $todo = $command_row->[8];
	my $size = $command_row->[9];
	my $start_time = $command_row->[10];
	my $expire = $command_row->[11];
	my $count = $command_row->[12];
	$sth = $dbh->prepare("SELECT storage_name, pool_name, label, part_num, filenum, host_name, disk_name, dump_timestamp, level FROM copys, parts, volumes, storages, pools, images, disks, hosts WHERE copys.copy_id=? AND parts.copy_id=copys.copy_id AND parts.volume_id=volumes.volume_id AND volumes.storage_id=storages.storage_id AND volumes.pool_id=pools.pool_id AND storages.config_id=? AND copys.image_id=images.image_id AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($src_copy_id, $config_id)
	    or die "Cannot execute: " . $sth->errstr();
	my %labels;
	while (my $row = $sth->fetchrow_arrayref) {
	    my $label = $row->[2];
	    my $part_num = $row->[3];
	    if ($part_num == 1) {
		$cmd->{'id'} = $id;
		$cmd->{'operation'} = $operation;
		$cmd->{'config_name'} = $config_name;
		$cmd->{'src_storage'} = $row->[0];
		$cmd->{'src_pool'} = $row->[1];
		$cmd->{'start_time'} = $start_time;
		$cmd->{'hostname'} = $row->[5];
		$cmd->{'diskname'} = $row->[6];
		$cmd->{'dump_timestamp'} = $row->[7];
		$cmd->{'level'} = $row->[8];
		$cmd->{'dst_storage'} = $dest_storage_name;
		$cmd->{'working_pid'} = $working_pid;
		$cmd->{'status'} = $status;
		if ($operation == $Amanda::Cmdfile::CMD_FLUSH) {
		    $cmd->{'holding_file'} = $row->[2];
		} elsif ($operation == $Amanda::Cmdfile::CMD_COPY ||
			 $operation == $Amanda::Cmdfile::CMD_RESTORE) {
		    $cmd->{'src_label'} = $row->[2];
		    $cmd->{''} = $row->[3];
		    $cmd->{'src_fileno'} = $row->[4];
		}
	    } else {
		$labels{$label} = 1;
	    }
	}
	$cmd->{'src_labels_str'} = join ('; ', keys %labels);
    } else {
    }
    return $cmd;
}

sub get_command_from_id {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_command_from_id'),
			['commands', 'configs', 'storages', 'copys', 'parts', 'volumes', 'pools', 'images', 'disks', 'hosts'],
			undef, @_);
}

sub _get_flush_command {
    my $self = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my @cmds;

    $sth = $dbh->prepare("SELECT command_id, operation, commands.config_id, config_name, src_copy_id, storage_name, working_pid, status, todo, size, start_time, expire, count FROM commands,configs,storages WHERE operation=? AND commands.config_id=configs.config_id AND commands.dest_storage_id=storages.storage_id AND commands.todo=1 AND commands.status=1")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($Amanda::Cmdfile::CMD_FLUSH)
	or die "Cannot execute: " . $sth->errstr();
    while (my $command_row = $sth->fetchrow_arrayref) {
	my $cmd;
	my $command_id = $command_row->[0];
	my $operation = $command_row->[1];
	my $config_id = $command_row->[2];
	my $config_name = $command_row->[3];
	my $src_copy_id = $command_row->[4];
	my $dest_storage_name = $command_row->[5];
	my $working_pid = $command_row->[6];
	my $status = $command_row->[7];
	my $todo = $command_row->[8];
	my $size = $command_row->[9];
	my $start_time = $command_row->[10];
	my $expire = $command_row->[11];
	my $count = $command_row->[12];
	my $sth1 = $dbh->prepare("SELECT storage_name, pool_name, label, part_num, filenum, host_name, disk_name, dump_timestamp, level FROM copys, parts, volumes, storages, pools, images, disks, hosts WHERE copys.copy_id=? AND parts.copy_id=copys.copy_id AND parts.volume_id=volumes.volume_id AND volumes.storage_id=storages.storage_id AND volumes.pool_id=pools.pool_id AND storages.config_id=? AND copys.image_id=images.image_id AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth1->execute($src_copy_id, $config_id)
	    or die "Cannot execute: " . $sth1->errstr();
	my %labels;
	while (my $row = $sth1->fetchrow_arrayref) {
	    my $label = $row->[2];
	    my $part_num = $row->[3];
	    if ($part_num == 1) {
		$cmd->{'id'} = $command_id;
		$cmd->{'operation'} = $operation;
		$cmd->{'config_name'} = $config_name;
		$cmd->{'src_storage'} = $row->[0];
		$cmd->{'src_pool'} = $row->[1];
		$cmd->{'start_time'} = $start_time;
		$cmd->{'hostname'} = $row->[5];
		$cmd->{'diskname'} = $row->[6];
		$cmd->{'dump_timestamp'} = $row->[7];
		$cmd->{'level'} = $row->[8];
		$cmd->{'dst_storage'} = $dest_storage_name;
		$cmd->{'working_pid'} = $working_pid;
		$cmd->{'status'} = $status;
		if ($operation == $Amanda::Cmdfile::CMD_FLUSH) {
		    $cmd->{'holding_file'} = $row->[2];
		} elsif ($operation == $Amanda::Cmdfile::CMD_COPY ||
			 $operation == $Amanda::Cmdfile::CMD_RESTORE) {
		    $cmd->{'src_label'} = $row->[2];
		    $cmd->{''} = $row->[3];
		    $cmd->{'src_fileno'} = $row->[4];
		}
	    } else {
		$labels{$label} = 1;
	    }
	}
	$cmd->{'src_labels_str'} = join ('; ', keys %labels);
	push @cmds, $cmd if $cmd->{'id'};
    }
    return \@cmds;
}

sub get_flush_command {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_flush_command'),
			['commands', 'configs', 'storages', 'copys', 'parts', 'volumes', 'pools', 'images', 'disks', 'hosts'],
			undef, @_);
}

sub _get_copy_command {
    my $self = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my @cmds;

    $sth = $dbh->prepare("SELECT command_id, operation, commands.config_id, config_name, src_copy_id, storage_name, working_pid, status, todo, size, start_time, expire, count FROM commands,configs,storages WHERE operation=? AND commands.config_id=configs.config_id AND commands.dest_storage_id=storages.storage_id AND commands.todo=1 AND commands.status=1")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($Amanda::Cmdfile::CMD_COPY)
	or die "Cannot execute: " . $sth->errstr();
    while (my $command_row = $sth->fetchrow_arrayref) {
	my $cmd;
	my $command_id = $command_row->[0];
	my $operation = $command_row->[1];
	my $config_id = $command_row->[2];
	my $config_name = $command_row->[3];
	my $src_copy_id = $command_row->[4];
	my $dest_storage_name = $command_row->[5];
	my $working_pid = $command_row->[6];
	my $status = $command_row->[7];
	my $todo = $command_row->[8];
	my $size = $command_row->[9];
	my $start_time = $command_row->[10];
	my $expire = $command_row->[11];
	my $count = $command_row->[12];
	my $sth1 = $dbh->prepare("SELECT storage_name, pool_name, label, part_num, filenum, host_name, disk_name, dump_timestamp, level FROM copys, parts, volumes, storages, pools, images, disks, hosts WHERE copys.copy_id=? AND parts.copy_id=copys.copy_id AND parts.volume_id=volumes.volume_id AND volumes.storage_id=storages.storage_id AND volumes.pool_id=pools.pool_id AND storages.config_id=? AND copys.image_id=images.image_id AND images.disk_id=disks.disk_id AND disks.host_id=hosts.host_id")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth1->execute($src_copy_id, $config_id)
	    or die "Cannot execute: " . $sth1->errstr();
	my %labels;
	while (my $row = $sth1->fetchrow_arrayref) {
	    my $label = $row->[2];
	    my $part_num = $row->[3];
	    if ($part_num == 1) {
		$cmd->{'id'} = $command_id;
		$cmd->{'operation'} = $operation;
		$cmd->{'config_name'} = $config_name;
		$cmd->{'src_storage'} = $row->[0];
		$cmd->{'src_pool'} = $row->[1];
		$cmd->{'start_time'} = $start_time;
		$cmd->{'hostname'} = $row->[5];
		$cmd->{'diskname'} = $row->[6];
		$cmd->{'dump_timestamp'} = $row->[7];
		$cmd->{'level'} = $row->[8];
		$cmd->{'dst_storage'} = $dest_storage_name;
		$cmd->{'working_pid'} = $working_pid;
		$cmd->{'status'} = $status;
		if ($operation == $Amanda::Cmdfile::CMD_FLUSH) {
		    $cmd->{'holding_file'} = $row->[2];
		} elsif ($operation == $Amanda::Cmdfile::CMD_COPY ||
			 $operation == $Amanda::Cmdfile::CMD_RESTORE) {
		    $cmd->{'src_label'} = $row->[2];
		    $cmd->{''} = $row->[3];
		    $cmd->{'src_fileno'} = $row->[4];
		}
	    } else {
		$labels{$label} = 1;
	    }
	}
	$cmd->{'src_labels_str'} = join ('; ', keys %labels);
	push @cmds, $cmd if $cmd->{'id'};
    }
    return \@cmds;
}

sub get_copy_command {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_copy_command'),
			['commands', 'configs', 'storages', 'copys', 'parts', 'volumes', 'pools', 'images', 'disks', 'hosts'],
			undef, @_);
}

sub _export_to_file {
    my $self = shift;
    my $filename = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $row;
    my $fh;

    open($fh, ">$filename") or die "Can't open '$filename': $!";

    print $fh "VERSION:\n";
    print $fh "  " . quote_string("$Amanda::DB::Catalog2::DB_VERSION") . "\n";

    print $fh "\nCONFIG\n";
    $sth = $dbh->prepare("SELECT * FROM configs ORDER by config_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . "\n";
    }

    print $fh "\nSTORAGE\n";
    $sth = $dbh->prepare("SELECT * FROM storages ORDER by storage_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . "\n";
    }

    print $fh "\nPOOL\n";
    $sth = $dbh->prepare("SELECT * FROM pools ORDER BY pool_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . "\n";
    }

    print $fh "\nMETA\n";
    $sth = $dbh->prepare("SELECT * FROM metas ORDER BY meta_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . "\n";
    }

    print $fh "\nHOST:\n";
    $sth = $dbh->prepare("SELECT * FROM hosts ORDER BY host_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . "\n";
    }

    print $fh "\nDISK:\n";
    $sth = $dbh->prepare("SELECT * FROM disks ORDER BY disk_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . " "
		       . quote_string("$row->[4]") . " "
		       . quote_string("$row->[5]") . " "
		       . quote_string("$row->[6]") . " "
		       . quote_string("$row->[7]") . "\n";
    }

    print $fh "\nIMAGE:\n";
    $sth = $dbh->prepare("SELECT * FROM images ORDER BY image_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	my $orig_kb = $row->[5] || 0;
	my $dump_status = $row->[6] || "";
	my $nb_files = $row->[7] || 0;
	my $nb_directories = $row->[8] || 0;
	my $based_on_timestamp = $row->[9] || 0;
	my $native_crc = $row->[10] || "";
	my $client_crc = $row->[11] || "";
	my $server_crc = $row->[12] || "";
	my $dump_message = $row->[13] || "";
	my $image_pid = $row->[13] || 0;
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . " "
		       . quote_string("$row->[4]") . " "
		       . quote_string("$orig_kb") . " "
		       . quote_string($dump_status) . " "
		       . quote_string("$nb_files") . " "
		       . quote_string("$nb_directories") . " "
		       . quote_string("$based_on_timestamp") . " "
		       . quote_string($native_crc) . " "
		       . quote_string($client_crc) . " "
		       . quote_string($server_crc) . " "
		       . quote_string($dump_message) . " "
		       . quote_string("$image_pid") . "\n";
    }

    print $fh "\nCOPY:\n";
    $sth = $dbh->prepare("SELECT * FROM copys ORDER BY copy_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	my $kb = $row->[6] || 0;
	my $bytes = $row->[7] || 0;
	my $server_crc = $row->[9] || "";
	my $copy_message = $row->[13] || "";
	my $copy_pid = $row->[14] || 0;
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . " "
		       . quote_string("$row->[4]") . " "
		       . quote_string("$row->[5]") . " "
		       . quote_string("$kb") . " "
		       . quote_string("$bytes") . " "
		       . quote_string("$row->[8]") . " "
		       . quote_string("$server_crc") . " "
		       . quote_string("$row->[10]") . " "
		       . quote_string("$row->[11]") . " "
		       . quote_string("$row->[12]") . " "
		       . quote_string("$copy_message") . " "
		       . quote_string("$copy_pid") . "\n";
    }

    print $fh "\nVOLUME:\n";
    $sth = $dbh->prepare("SELECT * FROM volumes ORDER BY volume_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	my $barcode = $row->[14] || "";
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . " "
		       . quote_string("$row->[4]") . " "
		       . quote_string("$row->[5]") . " "
		       . quote_string("$row->[6]") . " "
		       . quote_string("$row->[7]") . " "
		       . quote_string("$row->[8]") . " "
		       . quote_string("$row->[9]") . " "
		       . quote_string("$row->[10]") . " "
		       . quote_string("$row->[11]") . " "
		       . quote_string("$row->[12]") . " "
		       . quote_string("$row->[13]") . " "
		       . quote_string("$barcode") . "\n";
    }

    print $fh "\nPART:\n";
    $sth = $dbh->prepare("SELECT * FROM parts ORDER BY part_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	my $part_message = $row->[8] || "";
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . " "
		       . quote_string("$row->[4]") . " "
		       . quote_string("$row->[5]") . " "
		       . quote_string("$row->[6]") . " "
		       . quote_string("$row->[7]") . " "
		       . quote_string("$part_message") . "\n";
    }

    print $fh "\nCOMMAND:\n";
    $sth = $dbh->prepare("SELECT * FROM commands ORDER BY command_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while ($row = $sth->fetchrow_arrayref) {
	print $fh "  " . quote_string("$row->[0]") . " "
		       . quote_string("$row->[1]") . " "
		       . quote_string("$row->[2]") . " "
		       . quote_string("$row->[3]") . " "
		       . quote_string("$row->[4]") . " "
		       . quote_string("$row->[5]") . " "
		       . quote_string("$row->[6]") . " "
		       . quote_string("$row->[7]") . " "
		       . quote_string("$row->[8]") . " "
		       . quote_string("$row->[9]") . " "
		       . quote_string("$row->[10]") . "\n";
    }

    print $fh "\n";
}

sub export_to_file {
    my $self = shift;

    return $self->run_execute($self, $self->can('_export_to_file'), undef, undef, @_);
}

sub _import_from_file {
    my $self = shift;
    my $filename = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $line;
    my $fh;

    open($fh, "<$filename") or die "Can't open '$filename': $!";
    $line = <$fh>;
    chomp $line;
    die "not VERSION:$line:" if $line ne "VERSION:";
    $line = <$fh>;
    chomp $line;
    my $file_version = int($line);
    die "Can't import database version $file_version" if $file_version != $Amanda::DB::Catalog2::DB_VERSION;
    print "importing from a version $file_version database\n";

    # these lines was added by create_table
    $dbh->do("DELETE FROM version")
	or die "cannot do: " . $dbh->errstr();
    $dbh->do("DELETE FROM metas")
	or die "cannot do: " . $dbh->errstr();
    $dbh->do("DELETE FROM storages")
	or die "cannot do: " . $dbh->errstr();
    $dbh->do("DELETE FROM configs")
	or die "cannot do: " . $dbh->errstr();
    $dbh->do("INSERT INTO version VALUES ($Amanda::DB::Catalog2::DB_VERSION)")
	or die "cannot do: " . $dbh->errstr();

    $line = <$fh>;
    chomp $line;
    die "not an empty line:" if $line ne "";

    $line = <$fh>;
    chomp $line;
    die "not CONFIG" if $line ne "CONFIG";
    $sth = $dbh->prepare("INSERT INTO configs(config_name, created) VALUES (?, ?)")
	or die "Cannot prepare: " . $dbh->errstr();
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $config_id = shift @data;
	$sth->execute(@data)
	    or die "Can't import config $config_id " . $sth->errstr();
	$self->{'map_config_id'}->{$config_id} = $dbh->last_insert_id(undef, undef, "configs", undef)
    }

    $line = <$fh>;
    chomp $line;
    die "not STORAGE" if $line ne "STORAGE";
    $sth = $self->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $storage_id = shift @data;
	$data[0] = $self->{'map_config_id'}->{$data[0]};
	$sth->execute(@data)
	    or die "Can't import storage $storage_id" . $sth->errstr();
	$self->{'map_storage_id'}->{$storage_id} = $dbh->last_insert_id(undef, undef, "storages", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not POOL" if $line ne "POOL";
    $sth = $self->make_statement('in poo', 'INSERT INTO pools(pool_name) VALUES (?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $pool_id = shift @data;
	$sth->execute(@data)
	    or die "Can't import pool $pool_id " . $sth->errstr();
	$self->{'map_pool_id'}->{$pool_id} = $dbh->last_insert_id(undef, undef, "pools", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not META" if $line ne "META";
    $sth = $self->make_statement('in met','INSERT INTO metas(meta_label) VALUES (?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $meta_id = shift @data;
	$sth->execute(@data)
	    or die "Can't import meta $meta_id " . $sth->errstr();
	$self->{'map_meta_id'}->{$meta_id} = $dbh->last_insert_id(undef, undef, "metas", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not HOST:" if $line ne "HOST:";
    $sth = $self->make_statement('in hos', 'INSERT INTO hosts(config_id, host_name) VALUES (?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $host_id = shift @data;
	$data[0] = $self->{'map_config_id'}->{$data[0]};
	$sth->execute(@data)
	    or die "Can't import host $host_id " . $sth->errstr();
	$self->{'map_host_id'}->{$host_id} = $dbh->last_insert_id(undef, undef, "hosts", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not DISK" if $line ne "DISK:";
    $sth = $self->make_statement('in dis', 'INSERT INTO disks(host_id, disk_name, device, force_full, force_level_1, force_bump, force_no_bump) VALUES (?, ?, ?, ?, ?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $disk_id = shift @data;
	$data[0] = $self->{'map_host_id'}->{$data[0]};
	$sth->execute(@data)
	    or die "Can't import disk $disk_id " . $sth->errstr();
	$self->{'map_disk_id'}->{$disk_id} = $dbh->last_insert_id(undef, undef, "disks", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not IMAGE:" if $line ne "IMAGE:";
    $sth = $self->make_statement('in ima', 'INSERT INTO images(disk_id, dump_timestamp, level, parent_image_id, orig_kb, dump_status, nb_files, nb_directories, based_on_timestamp, native_crc, client_crc, server_crc, dump_message, image_pid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $image_id = shift @data;
	$data[0] = $self->{'map_disk_id'}->{$data[0]};
	$data[1] = $data[1];
	$data[8] = $data[8];
	$sth->execute(@data)
	    or die "Can't import image $image_id " . $sth->errstr();
	$self->{'map_image_id'}->{$image_id} = $dbh->last_insert_id(undef, undef, "images", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not COPY:" if $line ne "COPY:";
    $sth = $self->make_statement('in cop', 'INSERT INTO copys(image_id,storage_id,write_timestamp,parent_copy_id,nb_parts,kb,bytes,copy_status,server_crc,retention_days,retention_full,retention_recover,copy_message,copy_pid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $copy_id = shift @data;
	$data[0] = $self->{'map_image_id'}->{$data[0]};
	$data[1] = $self->{'map_storage_id'}->{$data[1]};
	$sth->execute(@data)
	    or die "Can't import copy $copy_id " . $sth->errstr();
	$self->{'map_copy_id'}->{$copy_id} = $dbh->last_insert_id(undef, undef, "copys", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not VOLUME:" if $line ne "VOLUME:";
    $sth = $self->make_statement('in vol', 'INSERT INTO volumes(pool_id,label,reuse,storage_id,meta_id,write_timestamp,orig_write_timestamp,retention_days,retention_full,retention_recover,retention_tape,retention_cmd,block_size,barcode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $volume_id = shift @data;
	$data[0] = $self->{'map_pool_id'}->{$data[0]};
	$data[3] = $self->{'map_storage_id'}->{$data[3]};
	$data[4] = $self->{'map_meta_id'}->{$data[4]};
	$data[5]=undef if $data[5]=='';
	$sth->execute(@data)
	    or die "Can't import volume $data[0] " . $sth->errstr();
	$self->{'map_volume_id'}->{$volume_id} = $dbh->last_insert_id(undef, undef, "volumes", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not PART:" if $line ne "PART:";
    $sth = $self->make_statement('in par', 'INSERT INTO parts(copy_id,volume_id,part_offset,part_size,filenum,part_num,part_status,part_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $part_id = shift @data;
	$data[0] = $self->{'map_copy_id'}->{$data[0]};
	$data[1] = $self->{'map_volume_id'}->{$data[1]};
	$sth->execute(@data)
	    or die "Can't import part $part_id " . $sth->errstr();
	$self->{'map_part_id'}->{$part_id} = $dbh->last_insert_id(undef, undef, "parts", undef);
    }

    $line = <$fh>;
    chomp $line;
    die "not COMMAND ($line)" if $line ne "COMMAND:";
    $sth = $self->make_statement('in com', 'INSERT INTO commands(operation,config_id,src_copy_id,dest_storage_id,working_pid,status,todosize,start_time,expire,count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    while ($line = <$fh>) {
	chomp $line;
	last if $line eq "";
	$line =~ s/^ +//g;
	my @data = split_quoted_strings($line);
	my $command_id = shift @data;
	$data[1] = $self->{'map_config_id'}->{$data[1]};
	$data[2] = $self->{'map_copy_id'}->{$data[2]};
	$data[3] = $self->{'map_storage_id'}->{$data[3]};
	$sth->execute(@data)
	    or die "Can't import part $data[0] " . $sth->errstr();
	$self->{'map_command_id'}->{$command_id} = $dbh->last_insert_id(undef, undef, "commands", undef);
    }
    close($fh);

    $self->_write_tapelist();
}

sub import_from_file {
    my $self = shift;

    return $self->run_execute($self, $self->can('_import_from_file'), undef, undef, @_);
}

sub ___cleanup_parts_copys_images {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("DELETE FROM parts")
        or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
        or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM copys")
        or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
        or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DELETE FROM images")
        or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
        or die "Cannot execute: " . $sth->errstr();

#    $sth = $dbh->prepare("UPDATE volumes SET retention=0, retention_tape=0")
#        or die "Cannot prepare: " . $dbh->errstr();
#    $sth->execute()
#        or die "Cannot execute: " . $sth->errstr();

}

sub _clean {
    my $self = shift;

    my $dbh = $self->{'dbh'};
    my $sth_volume_delete;
    my $sth_volume;
    my $sth_part_delete;
    my $clean;
    my $nb;

    $clean = 1;

    # cleanup holding disk
    my @hfiles = Amanda::Holding::all_files();
    @hfiles = Amanda::Holding::merge_all_files(@hfiles);
    while (@hfiles) {
	my $hfile = pop @hfiles;
	if ($hfile->{'header'}->{'type'} == $Amanda::Header::F_DUMPFILE) {
	    if ($hfile->{'filename'} =~ /(.*)\.tmp$/) {
		my $filename = $1;
		#print "Rename tmp holding file: $hfile->{'filename'}\n" if $verbose;
		Amanda::Holding::rename_tmp($filename, 0);
	    } else {
		# normal holding file
	    }
	}
    }

    # check all holdingdisk volume still exist
    $sth_volume = $dbh->prepare("SELECT volume_id, label FROM volumes, storages WHERE volumes.storage_id=storages.storage_id AND storages.storage_name='HOLDING' AND storages.config_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume_delete = $dbh->prepare("DELETE FROM volumes WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_part_delete = $dbh->prepare("DELETE FROM parts WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume->execute($self->{'config_id'})
	or die "Cannot execute: " . $sth_volume->errstr();
    while (my $volume_row = $sth_volume->fetchrow_arrayref) {
	my $volume_id = $volume_row->[0];
	my $hfile = $volume_row->[1];
	my $hdr = Amanda::Holding::get_header($hfile);
	if (!$hdr) {
	    debug("Delete volume $volume_id because can't parse the header ($hfile)");
	    $sth_part_delete->execute($volume_id)
		or die "Cannot execute: " . $sth_volume->errstr();
	    $sth_volume_delete->execute($volume_id)
		or die "Cannot execute: " . $sth_volume->errstr();
	    $clean = 0;
	}
    }

    # check all holding file have a corresponding holdingdisk volume.
    for my $hfile (Amanda::Holding::files()) {
	my $hdr = Amanda::Holding::get_header($hfile);
	next unless $hdr;

	my $size = Amanda::Holding::file_size($hfile, 1);
	my $device = "";
	my $disk = Amanda::Disklist::get_disk($hdr->{'name'}, $hdr->{'disk'});
	if ($disk && $disk->{'device'}) {
	    $device = $disk->{'device'};
	}

	my $status = "OK";

	# do the image exist
	my $image = $self->_find_image($hdr->{'name'}, $hdr->{'disk'}, $device, $hdr->{'datestamp'}, $hdr->{'dumplevel'});
	my $need_finish_image;
	if ($image) {
	    if ($image->{'dump_status'} eq "DUMPING") {
		$image->{'dump_status'} = "PARTIAL";
		$need_finish_image = 1;
	    }
	} else {
	    $image = $self->_add_image($hdr->{'name'}, $hdr->{'disk'},
					 $device, $hdr->{'datestamp'},
					 $hdr->{'dumplevel'});
	    if ($hdr->{'is_partial'}) {
		$image->{'dump_status'} = "PARTIAL";
	    } else {
		$image->{'dump_status'} = "OK";
	    }
	    $need_finish_image = 1;
	}

	# do the copy exists
	my $copy = $self->_find_copyX($hdr->{'name'}, $hdr->{'disk'}, $hdr->{'datestamp'}, $hdr->{'dumplevel'}, "HOLDING");
	my $need_finish_copy;
	if ($copy) {
	    if ($copy->{'copy_status'} eq "DUMPING") {
		$copy->{'copy_status'} = "PARTIAL";
	    }
	} else {
	    $copy = $image->_add_copy("HOLDING", $hdr->{'datestamp'}, 1, 1, 1);
	    $copy->{'copy_status'} = $image->{'dump_status'};
	    $need_finish_copy = 1;
	}

	# do the volume exists
	my $volume = $self->_find_volume("HOLDING", $hfile);
	if (!$volume) {
	    $volume = $self->add_volume("HOLDING", $hfile, $hdr->{'datestamp'}, "HOLDING", undef, undef, 32);
	}
	# do the part exists
	my $part = $copy->find_a_part();
	if (!$part) {
	    $copy->_add_part($volume, 0, $size * 1024, 0, 1, "OK", undef);
	}

	$copy->_finish_copy(1, $size, 0, $copy->{'copy_status'}, $hdr->{'server_crc'}) if $need_finish_copy;
	$image->_finish_image($hdr->{'orig_size'}, $image->{'dump_status'}, undef, undef, $hdr->{'native_crc'}, $hdr->{'client_crc'}, $hdr->{'server_crc'}, undef) if $need_finish_image;
	$clean = 0;
    }

    my @images = $self->_find_dumping_images();
    foreach my $image (@images) {
	if (kill(0, $image->{'image_pid'}) == 0) {
	    my @copies = $image->find_copies();
	    foreach my $copy (@copies) {
		if (kill(0, $copy->{'copy_pid'}) == 0) {
		    $copy->_finish_copy(1, 0, 0, "PARTIAL", undef);
		}
	    }
	    $image->_finish_image(0, "PARTIAL", undef, undef, undef, undef, undef, undef);
	}
    }
    my @copies = $self->_find_dumping_copies();
    foreach my $copy (@copies) {
	if (kill(0, $copy->{'copy_pid'}) == 0) {
	    $copy->_finish_copy(1, 0, 0, "PARTIAL", undef);
	}
    }

#    $nb = $dbh->do("DELETE FROM images WHERE dump_status == 'DUMPING'");
#    die "Cannot do: " . $dbh->errstr() if !defined $nb;
#    $clean = 0 if $nb > 0;

#    $nb = $dbh->do("DELETE FROM copys WHERE copy_status == 'DUMPING'");
#    die "Cannot do: " . $dbh->errstr() if !defined $nb;
#    $clean = 0 if $nb > 0;

    $nb = $dbh->do("DELETE FROM volumes WHERE NOT EXISTS (SELECT volume_id FROM parts WHERE volumes.volume_id = parts.volume_id)");
    die "Cannot do: " . $dbh->errstr() if !defined $nb;
    $clean = 0 if $nb > 0;
    debug("deleted $nb volumes") if $nb > 0;

    # remove commands when src_copy have no part
    $nb = $dbh->do("DELETE FROM commands WHERE NOT EXISTS (SELECT copys.copy_id FROM parts,copys WHERE commands.src_copy_id=copys.copy_id AND copys.copy_id = parts.copy_id)");
    die "Cannot do: " . $dbh->errstr() if !defined $nb;
    $clean = 0 if $nb > 0;
    debug("deleted $nb commands") if $nb > 0;

    $nb = $dbh->do("DELETE FROM copys WHERE NOT EXISTS (SELECT copy_id FROM parts WHERE copys.copy_id = parts.copy_id)");
    die "Cannot do: " . $dbh->errstr() if !defined $nb;
    $clean = 0 if $nb > 0;
    debug("deleted $nb copys") if $nb > 0;

    #$nb = $dbh->do("DELETE FROM copys WHERE NOT EXISTS (SELECT image_id FROM images WHERE copys.image_id = images.image_id)");
    #die "Cannot do: " . $dbh->errstr() if !defined $nb;
    #$clean = 0 if $nb > 0;

    $nb = $dbh->do("DELETE FROM images WHERE NOT EXISTS (SELECT image_id FROM copys WHERE images.image_id = copys.image_id)");
    die "Cannot do: " . $dbh->errstr() if !defined $nb;
    $clean = 0 if $nb > 0;
    debug("deleted $nb images") if $nb > 0;

    #$nb = $dbh->do("DELETE FROM images WHERE NOT EXISTS (SELECT disk_id FROM disks WHERE images.disk_id = disks.disk_id)");
    #die "Cannot do: " . $dbh->errstr() if !defined $nb;
    #$clean = 0 if $nb > 0;

    $nb = $dbh->do("DELETE FROM disks WHERE NOT EXISTS (SELECT disk_id FROM images WHERE disks.disk_id = images.disk_id)");
    die "Cannot do: " . $dbh->errstr() if !defined $nb;
    $clean = 0 if $nb > 0;
    debug("deleted $nb disks") if $nb > 0;

    #$nb = $dbh->do("DELETE FROM disks WHERE NOT EXISTS (SELECT host_id FROM hosts WHERE disks.host_id = hosts.host_id)");
    #die "Cannot do: " . $dbh->errstr() if !defined $nb;
    #$clean = 0 if $nb > 0;

    $nb = $dbh->do("DELETE FROM hosts WHERE NOT EXISTS (SELECT host_id FROM disks WHERE hosts.host_id = disks.host_id)");
    die "Cannot do: " . $dbh->errstr() if !defined $nb;
    $clean = 0 if $nb > 0;
    debug("deleted $nb hosts") if $nb > 0;

    #$nb = $dbh->do("DELETE FROM parts WHERE NOT EXISTS (SELECT copy_id FROM copys WHERE parts.copy_id = copys.copy_id)");
    #die "Cannot do: " . $dbh->errstr() if !defined $nb;
    #$clean = 0 if $nb > 0;

    #$nb = $dbh->do("DELETE FROM parts WHERE NOT EXISTS (SELECT volume_id FROM volumes WHERE parts.volume_id = volumes.volume_id)");
    #die "Cannot do: " . $dbh->errstr() if !defined $nb;
    #$clean = 0 if $nb > 0;

    $self->_write_tapelist(1);
}

sub clean {
    my $self = shift;

    return $self->run_execute($self, $self->can('_clean'), undef, undef, @_);
}

sub _get_latest_write_timestamp {
    my $self = shift;
    my %params = @_;

    if ($params{'type'} || $params{'types'}) {
	return Amanda::DB::Catalog::get_latest_write_timestamp(%params);
    }

    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("SELECT max(write_timestamp) FROM volumes")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();
    my $volume_row = $sth->fetchrow_arrayref;
    return $volume_row->[0];
}

sub get_latest_write_timestamp {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_latest_write_timestamp'),
			['volumes'],
			undef, @_);
}

sub get_latest_run_timestamp {
    my $self = shift;

    return Amanda::DB::Catalog::get_latest_write_timestamp();
}

sub _get_dumps {
    my $self = shift;
    my %params = @_;
    my $dbh = $self->{'dbh'};
    my @dumps;
    my @parts;

    my $sth_host = $dbh->prepare("SELECT * FROM hosts ORDER BY host_name")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_disk = $dbh->prepare("SELECT * FROM disks WHERE host_id = ? ORDER BY disk_name")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_image = $dbh->prepare("SELECT * FROM images WHERE disk_id = ? ORDER BY dump_timestamp DESC, level, image_id DESC")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_copy = $dbh->prepare("SELECT * FROM copys,storages WHERE image_id=? AND copys.storage_id=storages.storage_id ORDER BY write_timestamp DESC, copy_id DESC")
	or die "Cannot prepare: " . $dbh->errstr();
    my $sth_volume = $dbh->prepare("SELECT volumes.label,pools.pool_name FROM parts,volumes,pools WHERE parts.copy_id=? AND parts.volume_id=volumes.volume_id AND volumes.pool_id=pools.pool_id LIMIT 1")
	or die "Cannot prepare: " . $dbh->errstr();

    # find matching host
    $sth_host->execute()
	or die "Cannot execute: " . $sth_host->errstr();
    my $all_row_host = $sth_host->fetchall_arrayref;
    for my $row_host (@{$all_row_host}) {
	if (defined $params{'hostname'}) {
	    next if $params{'hostname'} ne $row_host->[2];
	}
	if (defined $params{'hostnames'}) {
	    my $matched = 0;
	    foreach my $host_name (@{$params{'hostnames'}}) {
		if ($host_name eq $row_host->[2]) {
		    $matched = 1;
		    last;
		}
	    }
	    next if $matched == 0;
	}
	if (defined $params{'hostname_match'}) {
	    next if !match_host($params{'hostname_match'}, $row_host->[2]);
	}
	if (defined $params{'dumpspec'}) {
	    next if $params{'dumpspec'}->{'host'} and
		    !match_host($params{'dumpspec'}->{'host'}, $row_host->[2]);
	}
	if (defined $params{'dumpspecs'}) {
	    my $matched = 0;
	    foreach my $dumpspec (@{$params{'dumpspecs'}}) {
		if (!$dumpspec->{'host'} or
		    match_host($dumpspec->{'host'}, $row_host->[2])) {
		    $matched = 1;
		    last;
		}
	    }
	    next if $matched == 0;
	}

	# find matching disk
	$sth_disk->execute($row_host->[0])
	    or die "Cannot execute: " . $sth_disk->errstr();
	my $all_row_disks = $sth_disk->fetchall_arrayref;
	for my $row_disk (@{$all_row_disks}) {
	    if (defined $params{'diskname'}) {
		next if $params{'diskname'} ne $row_disk->[2];
	    }
	    if (defined $params{'disknames'}) {
		my $matched = 0;
		foreach my $disk_name (@{$params{'disknames'}}) {
		    if ($disk_name eq $row_disk->[2]) {
			$matched = 1;
			last;
		    }
		}
		next if $matched == 0;
	    }
	    if (defined $params{'diskname_match'}) {
		next if !match_disk($params{'diskname_match'}, $row_disk->[2]);
	    }
	    if (defined $params{'dumpspec'}) {
		next if $params{'dumpspec'}->{'disk'} and
			!match_disk($params{'dumpspec'}->{'disk'}, $row_disk->[2]);
	    }
	    if (defined $params{'dumpspecs'}) {
		my $matched = 0;
		foreach my $dumpspec (@{$params{'dumpspecs'}}) {
		    if ((!$dumpspec->{'host'} or
			 match_host($dumpspec->{'host'}, $row_host->[2])) and
			(!$dumpspec->{'disk'} or
			 match_disk($dumpspec->{'disk'}, $row_disk->[2]))) {
			$matched = 1;
			last;
		    }
		}
		next if $matched == 0;
	    }

	    # find image
	    $sth_image->execute($row_disk->[0])
		or die "Cannot execute: " . $sth_image->errstr();
	    my $all_row_image = $sth_image->fetchall_arrayref;
	    for my $row_image (@{$all_row_image}) {
		if (defined $params{'dump_timestamp'}) {
		    next if $params{'dump_timestamp'} ne $row_image->[2];
		}
		if (defined $params{'dump_timestamps'}) {
		    my $matched = 0;
		    foreach my $dump_timestamp (@{$params{'dump_timestamps'}}) {
			if ($dump_timestamp eq $row_image->[2]) {
			    $matched = 1;
			    last;
			}
		    }
		    next if $matched == 0;
		}
		if (defined $params{'dump_timestamp_match'}) {
		    next if !match_datestamp($params{'dump_timestamp_match'}, "$row_image->[2]");
		}
		if (defined $params{'level'}) {
		    next if $params{'level'} != $row_image->[3];
		}
		if (defined $params{'levels'}) {
		    my $matched = 0;
		    foreach my $level (@{$params{'levels'}}) {
			if ($level == $row_image->[3]) {
			    $matched = 1;
			    last;
			}
		    }
		    next if $matched == 0;
		}
		if (defined $params{'dumpspec'}) {
		    next if
		        ($params{'dumpspec'}->{'level'} and
			 !match_level("$params{'dumpspec'}->{'level'}", "$row_image->[3]")) or
			($params{'dumpspec'}->{'datestamp'} and
			 match_datestamp($params{'dumpspec'}->{'datestamp'}, "$row_image->[2]"));
		}
		if (defined $params{'dumpspecs'}) {
		    my $matched = 0;
		    foreach my $dumpspec (@{$params{'dumpspecs'}}) {
			if ((!$dumpspec->{'host'} or
			     match_host($dumpspec->{'host'}, $row_host->[2])) and
			    (!$dumpspec->{'disk'} or
			     match_disk($dumpspec->{'disk'}, $row_disk->[2])) and
			    (!$dumpspec->{'level'} or
			     match_level("$dumpspec->{'level'}", "$row_image->[3]")) and
			    (!$dumpspec->{'datestamp'} or
			     match_datestamp($dumpspec->{'datestamp'}, "$row_image->[2]"))) {
			    $matched = 1;
			    last;
			}
		    }
		    next if $matched == 0;
		}

		# find copy
		$sth_copy->execute($row_image->[0])
		    or die "Cannot execute: " . $sth_copy->errstr();
		my $all_row_copy = $sth_copy->fetchall_arrayref;
		for my $row_copy (@{$all_row_copy}) {
		    if ($params{'write_timestamp'}) {
			next if !match_datestamp("$params{'write_timestamp'}", "$row_copy->[3]");
		    }
		    if ($params{'write_timestamps'}) {
			my $matched = 0;
			foreach my $timestamp (@{$params{'write_timestamps'}}) {
			    if (match_datestamp("$timestamp", "$row_copy->[3]")) {
				$matched = 1;
				last;
			    }
			}
			next if $matched == 0;
		    }
		    if ($params{'dumpspec'}) {
			next if defined $params{'dumpspec'}->{'write_timestamp'} and
				match_datestamp($params{'dumpspec'}->{'write_timestamp'}, "$row_copy->[3]");
		    }
		    if ($params{'dumpspecs'}) {
			my $matched = 0;
			foreach my $dumpspec (@{$params{'dumpspecs'}}) {
			    if ((!$dumpspec->{'host'} or
				 match_host($dumpspec->{'host'}, $row_host->[2])) and
				(!$dumpspec->{'disk'} or
				 match_disk($dumpspec->{'disk'}, $row_disk->[2])) and
				(!$dumpspec->{'level'} or
				 match_level("$dumpspec->{'level'}", "$row_image->[3]")) and
				(!$dumpspec->{'datestamp'} or
				 match_datestamp($dumpspec->{'datestamp'}, "$row_image->[2]")) and
				(!$dumpspec->{'write_timestamp'} or
				 match_datestamp($dumpspec->{'write_timestamp'}, "$row_copy->[3]"))) {
				$matched = 1;
				last;
			    }
			}
			next if $matched == 0;
		    }

		    my $dump_status = $row_image->[6];
		    my $copy_status = $row_copy->[8];
		    my $status = $copy_status;
		    $status = $dump_status if $status eq "OK";

		    if ($params{'status'}) {
			next if $status ne $params{'status'};
		    }

		    $sth_volume->execute($row_copy->[0])
			or die "Cannot execute: " . $sth_volume->errstr();
		    my $row_volume = $sth_volume->fetchrow_arrayref;
		    my $hfile = $row_volume->[0] if $row_copy->[16] eq "HOLDING";
		    #put in result
		    my %dump =  (
			status          => $status,
			hostname        => $row_host->[2],
			diskname        => $row_disk->[2],
			dump_timestamp  => $row_image->[2],
			level           => $row_image->[3],
			orig_kb         => $row_image->[5],
			dump_status     => $row_image->[6],
			copy_id         => $row_copy->[0],
			write_timestamp => $row_copy->[3] || "00000000000000",
			nparts          => $row_copy->[5],
			kb              => $row_copy->[6],
			bytes           => $row_copy->[7],
			copy_status     => $row_copy->[8],
			native_crc      => $row_image->[10],
			client_crc      => $row_image->[11],
			server_crc      => $row_copy->[9],
			storage         => $row_copy->[17],
#			holding_file	=> $hfile,
			pool		=> $row_volume->[1],
			message         => $row_copy->[13]
		    );

		    push @dumps, \%dump;
		}
	    }
	}
    }

    if ($params{'parts'} || $params{'allparts'}) {
	$self->_get_parts(\@dumps, %params);
    }
    return \@dumps;
}

sub get_dumps {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_dumps'),
		['hosts', 'disks', 'images', 'copys', 'storages', 'parts', 'volumes', 'pools'],
		undef, @_);
}

sub _get_parts {
    my $self = shift;
    my $dumps = shift;
    my %params = @_;
    my $dbh = $self->{'dbh'};
    my $sth_part = $dbh->prepare("SELECT part_id, copy_id, volume_id, part_offset, part_size, filenum, part_num, part_status, part_message FROM parts WHERE copy_id = ? ORDER BY part_id")
	or die "Cannot prepare: " . $dbh->errstr();

    my @parts;
    # cache the volume table for faster lookup
    my %volume;
    my $sth_volume = $dbh->prepare("SELECT volume_id,label,pool_name FROM volumes,pools WHERE volumes.pool_id=pools.pool_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume->execute()
	or die "Cannot execute: " . $dbh->errstr();
    while (my $row_volume = $sth_volume->fetchrow_arrayref ) {
	$volume{$row_volume->[0]} = { label => $row_volume->[1],
				      pool  => $row_volume->[2] };
    }

    if (ref($dumps) ne 'ARRAY') {
	$dumps = [ $dumps ];
    }
    for my $dump (@$dumps) {
	# find matching part
	my @dump_parts = ();
	my @alldump_parts;
        $sth_part->execute($dump->{'copy_id'})
            or die "Cannot execute: " . $sth_part->errstr();
	my $all_row_part = $sth_part->fetchall_arrayref;
	for my $row_part (@{$all_row_part}) {
	    my $volume_id = $row_part->[2];
	    if ($params{'holding'}) {
		next if $volume{$volume_id}{label} !~ /^\//;
	    }
	    if ($params{'label'}) {
		next if $volume{$volume_id}{label} ne $params{'label'};
	    }
	    if ($params{'labels'}) {
		my $matched = 0;
		my $volume_label = $volume{$volume_id}{label};
		foreach my $label (@{$params{'labels'}}) {
		    if ($label eq $volume_label) {
			$matched = 1;
			last;
		    }
		}
		next if $matched == 0;
	    }

	    if ($params{'status'}) {
		next if $row_part->[7] ne $params{'status'};
	    }

	    my %part = (
		part_offset => $row_part->[3],
		part_size   => $row_part->[4],
		kb	    => int($row_part->[4]/1024),
		partnum     => $row_part->[6],
		status      => $row_part->[7],
		pool        => $volume{$volume_id}{pool},
		dump        => $dump
	    );
	    if ($volume{$volume_id}{label} !~ /^\//) {
		$part{'label'}   = $volume{$volume_id}{label},
		$part{'filenum'} = $row_part->[5];
	    } else {
		$part{'holding_file'} = $volume{$volume_id}{label},
	    }
	    weaken_ref($part{'dump'});

	    $dump_parts[$row_part->[6]] = \%part;
	    push @parts, \%part;
	    push @alldump_parts, \%part;
	}
	$dump->{'parts'} = \@dump_parts if $params{'parts'};
	$dump->{'allparts'} = \@alldump_parts if $params{'allparts'};
    }

    return \@parts;
}

sub get_parts {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_parts'),
		['parts', 'volumes', 'pools'],
		undef, @_);
}

sub sort_dumps {
    my $self = shift;

    return Amanda::DB::Catalog::sort_dumps(@_);
}

sub sort_parts {
    my $self = shift;

    return Amanda::DB::Catalog::sort_parts(@_);
}

sub _get_write_timestamps {
    my $self = shift;

    my $dbh = $self->{'dbh'};
    my $sth;
    my @timestamps;

    $sth = $dbh->prepare("SELECT DISTINCT write_timestamp FROM volumes WHERE write_timestamp != 0 ORDER by write_timestamp ASC")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
    while (my $row_volume = $sth->fetchrow_arrayref) {
	push @timestamps, $row_volume->[0];
    }

    return \@timestamps;
}

sub get_write_timestamps {
    my $self = shift;

    return $self->run_execute($self, $self->can('_get_write_timestamps'),
		['volumes'],
		undef, @_);
}

sub get_run_type {
    my $self = shift;
    my $write_timestamp = shift;

    return Amanda::DB::Catalog::get_run_type($write_timestamp);
}


sub _print_catalog {
    my $self = shift;
    my $dumpspecs = shift;
    my %params = @_;
    my $nb_dumpspec = @$dumpspecs;

    my $dbh = $self->{'dbh'};
    my $sth;
    my $config_check;
    my $config_check_disk;
    my $image_check;

    if (!$params{'all_configs'}) {
	if (!$self->{'config_id'}) {
	# get the config */
	    $sth = $dbh->prepare("SELECT * FROM configs WHERE config_name=?")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute($self->{'config_name'})
		or die "Cannot execute: " . $sth->errstr();
	    # get the first row
	    my $config_row = $sth->fetchrow_arrayref;
	    if (!defined $config_row) {
		return;
	    } else {
		$self->{'config_id'} = $config_row->[0];
	    }
	}
	$config_check = " hosts.config_id=$self->{'config_id'} AND ";
	$config_check_disk = " hosts.config_id=$self->{'config_id'} AND disks.host_id=hosts.host_id AND ";
    } else {
	$config_check = " hosts.config_id=configs.config_id AND";
    }

    if (defined $dumpspecs && @$dumpspecs > 0) {
	my $first_image_check = 1;
	$image_check = " ( 1<>1 ";
	if ($params{'exact_match'}) {
	    foreach my $dumpspec (@$dumpspecs) {
		if ($dumpspec->{'datestamp'} && $dumpspec->{'datestamp'} ne "*") {
		    my $sth_datestamp = $dbh->prepare("SELECT image_id FROM hosts, disks, images WHERE $config_check_disk host_name=? AND (disk_name=? OR device=?) AND disks.disk_id=images.image_id AND images.dump_timestamp=?")
			or die "Cannot prepare: " . $dbh->errstr();
		    $sth_datestamp->execute($dumpspec->{'host'}, $dumpspec->{'disk'}, $dumpspec->{'disk'}, $dumpspec->{'datestamp'})
			or die "Cannot execute: " . $sth_datestamp->errstr();
		    while (my $datestamp_row = $sth_datestamp->fetchrow_arrayref ) {
			$image_check .= " OR (images.image_id=$datestamp_row->[0])";
		    }
		} elsif ($dumpspec->{'disk'} && $dumpspec->{'disk'} ne "*") {
		    if ($dumpspec->{'host'} ne "*") {
			my $sth_host_disk = $dbh->prepare("SELECT disk_id FROM hosts, disks WHERE $config_check_disk host_name=? AND (disk_name=? OR device=?)")
			    or die "Cannot prepare: " . $dbh->errstr();
			$sth_host_disk->execute($dumpspec->{'host'}, $dumpspec->{'disk'}, $dumpspec->{'disk'})
			    or die "Cannot execute: " . $sth_host_disk->errstr();
			while (my $host_disk_row = $sth_host_disk->fetchrow_arrayref ) {
			    $image_check .= " OR (disks.disk_id=$host_disk_row->[0])";
			}
		    } else {
			my $sth_disk = $dbh->prepare("SELECT disk_id FROM hosts, disks WHERE $config_check_disk (disk_name=? OR device=?)")
			    or die "Cannot prepare: " . $dbh->errstr();
			$sth_disk->execute($dumpspec->{'disk'}, $dumpspec->{'disk'})
			    or die "Cannot execute: " . $sth_disk->errstr();
			while (my $disk_row = $sth_disk->fetchrow_arrayref ) {
			    $image_check .= " OR (disks.disk_id=$disk_row->[0])";
			}
		    }
		} elsif ($dumpspec->{'host'} ne "*") {
		    my $sth_host = $dbh->prepare("SELECT host_id FROM hosts WHERE $config_check host_name=?")
			or die "Cannot prepare: " . $dbh->errstr();
		    $sth_host->execute($dumpspec->{'host'})
			or die "Cannot execute: " . $sth_host->errstr();
		    while (my $host_row = $sth_host->fetchrow_arrayref ) {
			$image_check .= " OR (hosts.host_id=$host_row->[0])";
		    }
		} else {
		    $image_check .= " OR 1=1 ";
		}
	    }
	} else {
	    foreach my $dumpspec (@$dumpspecs) {
		if ($dumpspec->{'datestamp'} && $dumpspec->{'datestamp'} ne "*") {
		    my $sth_datestamp = $dbh->prepare("SELECT image_id, host_name, disk_name, device, dump_timestamp FROM hosts, disks, images WHERE $config_check_disk disks.disk_id=images.disk_id AND images.dump_timestamp=?")
			or die "Cannot prepare: " . $dbh->errstr();
		    $sth_datestamp->execute($dumpspec->{'host'}, $dumpspec->{'disk'}, $dumpspec->{'disk'}, $dumpspec->{'datestamp'})
			or die "Cannot execute: " . $sth_datestamp->errstr();
		    while (my $datestamp_row = $sth_datestamp->fetchrow_arrayref ) {
			if (Amanda::Util::match_disk($dumpspec->{'host'}, $datestamp_row->[1]) &&
			    (Amanda::Util::match_disk($dumpspec->{'disk'}, $datestamp_row->[2]) ||
			     Amanda::Util::match_disk($dumpspec->{'disk'}, $datestamp_row->[3])) &&
			    Amanda::Util::match_datestamp($dumpspec->{'datestamp'}, $datestamp_row->[4])) {
			$image_check .= " OR (images.image_id=$datestamp_row->[0])";
			}
		    }
		} elsif ($dumpspec->{'disk'} && $dumpspec->{'disk'} ne "*") {
		    if ($dumpspec->{'host'} ne "*") {
			my $sth_host_disk = $dbh->prepare("SELECT disk_id, host_name, disk_name, device FROM hosts, disks WHERE $config_check_disk 1=1")
			    or die "Cannot prepare: " . $dbh->errstr();
			$sth_host_disk->execute()
			    or die "Cannot execute: " . $sth_host_disk->errstr();
			while (my $host_disk_row = $sth_host_disk->fetchrow_arrayref ) {
			    if (Amanda::Util::match_disk($dumpspec->{'host'}, $host_disk_row->[1]) &&
				(Amanda::Util::match_disk($dumpspec->{'disk'}, $host_disk_row->[2]) ||
				 Amanda::Util::match_disk($dumpspec->{'disk'}, $host_disk_row->[3]))) {
				$image_check .= " OR (disks.disk_id=$host_disk_row->[0])";
			    }
			}
		    } else {
			my $sth_disk = $dbh->prepare("SELECT disk_id, disk_name, device FROM hosts, disks WHERE $config_check_disk 1=1")
			    or die "Cannot prepare: " . $dbh->errstr();
			$sth_disk->execute()
			    or die "Cannot execute: " . $sth_disk->errstr();
			while (my $disk_row = $sth_disk->fetchrow_arrayref ) {
			    if (Amanda::Util::match_disk($dumpspec->{'disk'}, $disk_row->[1]) ||
				Amanda::Util::match_disk($dumpspec->{'disk'}, $disk_row->[2])) {
				$image_check .= " OR (disks.disk_id=$disk_row->[0])";
			    }
			}
		    }
		} elsif ($dumpspec->{'host'} ne "*") {
		    my $sth_host = $dbh->prepare("SELECT host_id, host_name FROM hosts WHERE $config_check 1=1")
			or die "Cannot prepare: " . $dbh->errstr();
		    $sth_host->execute()
			or die "Cannot execute: " . $sth_host->errstr();
		    while (my $host_row = $sth_host->fetchrow_arrayref ) {
			if (Amanda::Util::match_host($dumpspec->{'host'}, $host_row->[1])) {
			    $image_check .= " OR (hosts.host_id=$host_row->[0])";
			}
		    }
		} else {
		    $image_check .= " OR 1=1 ";
		}
	    }
	}
	$image_check .= ") AND ";
    } else {
	$image_check = "";
    }
    if ($params{'parts'}) {
	my $s ="SELECT config_name, dump_timestamp, host_name, disk_name, level, storage_name, pool_name, label, dump_status, copy_status, part_status, filenum, nb_parts, part_num
			      FROM configs, hosts, disks, images, copys, parts, volumes, storages, pools
			      WHERE $config_check $image_check
				    hosts.config_id=configs.config_id AND
				    hosts.host_id=disks.host_id AND
				    disks.disk_id=images.disk_id AND
				    images.image_id=copys.image_id AND
				    copys.copy_id=parts.copy_id AND
				    parts.volume_id=volumes.volume_id AND
				    storages.storage_id=volumes.storage_id AND
				    pools.pool_id=volumes.pool_id
			      ORDER BY host_name, disk_name, dump_timestamp DESC , level, storages.storage_id, part_num, copys.write_timestamp DESC";
	debug("statement: $s");
	$sth = $dbh->prepare($s)
	    or die "Cannot prepare: " . $dbh->errstr();

	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
	my $row_copies = $sth->fetchall_arrayref;
	for my $row_copy (@{$row_copies}) {
	    my $nice_date;
	    if (defined $params{'timestamp'}) {
		$nice_date = $row_copy->[1];
	    } else {
		$nice_date = nicedate($row_copy->[1]);
	    }
	    my $qhost = quote_string($row_copy->[2]) . " ";
	    my $qdisk = quote_string($row_copy->[3]) . " ";
	    my $qlabel = quote_string($row_copy->[7]) . " ";

	    print $row_copy->[0] . " " .
		  $nice_date . " " .
		  $qhost .
		  $qdisk .
		  $row_copy->[4] . " " .
		  $row_copy->[5] . " " .
		  $row_copy->[6] . " " .
		  $qlabel .
		  $row_copy->[8] . " " .
		  $row_copy->[9] . " " .
		  $row_copy->[10] . " " .
		  $row_copy->[11] . " " .
		  $row_copy->[12] . " " .
		  $row_copy->[13] . "\n";
	}
    } else {
	my $s = "SELECT config_name, dump_timestamp, host_name, disk_name, level, storage_name, dump_status, copy_status, nb_files, nb_directories
			      FROM configs, hosts, disks, images, copys, storages
			      WHERE $config_check $image_check
				    hosts.config_id=configs.config_id AND
				    hosts.host_id=disks.host_id AND
				    disks.disk_id=images.disk_id AND
				    images.image_id=copys.image_id AND
				    storages.storage_id=copys.storage_id
			      ORDER BY host_name, disk_name, dump_timestamp DESC, level, storages.storage_id, write_timestamp DESC";
	debug("statement: $s");
	$sth = $dbh->prepare($s)
	    or die "Cannot prepare: " . $dbh->errstr();

	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
	my $row_copies = $sth->fetchall_arrayref;
	for my $row_copy (@{$row_copies}) {
	    my $nice_date;
	    if (defined $params{'timestamp'}) {
		$nice_date = $row_copy->[1];
	    } else {
		$nice_date = nicedate($row_copy->[1]);
	    }
	    my $qhost = quote_string($row_copy->[2]) . " ";
	    my $qdisk = quote_string($row_copy->[3]) . " ";
	    my $nb_files = $row_copy->[8] || 0;
	    my $nb_directories = $row_copy->[9] || 0;

	    print $row_copy->[0] . " " .
		  $nice_date . " " .
		  $qhost .
		  $qdisk .
		  $row_copy->[4] . " " .
		  $row_copy->[5] . " " .
		  $row_copy->[6] . " " .
		  $row_copy->[7] . " " .
		  $nb_files . " " .
		  $nb_directories .
		  "\n";
	}
    }
}

sub print_catalog {
    my $self = shift;

    return $self->run_execute($self, $self->can('_print_catalog'), undef, undef, @_);
}

sub _volume_assign {
    my $self = shift;
    my $force = shift;
    my $pool = shift;
    my $label = shift;
    my $new_pool = shift;
    my $barcode = shift;
    my $storage = shift;
    my $meta = shift;
    my $reuse = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my @result_messages;
    my $nb;
    my $s;
    my @args;

    $sth = $dbh->prepare("SELECT volume_id, reuse, barcode, storage_name, pool_name, meta_label, config_name FROM volumes,storages,pools,metas,configs WHERE volumes.label=? AND volumes.pool_id=pools.pool_id AND pools.pool_name=? AND volumes.storage_id=storages.storage_id AND volumes.meta_id=metas.meta_id AND storages.config_id=configs.config_id")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($label, $pool)
	or die "Cannot execute: " . $sth->errstr();;

    my $row_volume = $sth->fetchrow_arrayref;
    if (!defined $row_volume) {
	# volume not found
	return Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000035,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'});
    }
    my $volume_id = $row_volume->[0];

    if ($row_volume->[6] && #config_name
	$row_volume->[6] ne Amanda::Config::get_config_name()) {
	push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000000,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			config => $row_volume->[6],
			catalog_name => $self->{'catalog_name'});
	return \@result_messages;
    }
    $s = "UPDATE volumes SET";
    my $first_set = 0;
    my $data_set = 0;
    if (defined $reuse) {
	if ($reuse == $row_volume->[1]) {
	    # message same reuse
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => $reuse?1000046:1000048,
			severity => $Amanda::Message::INFO,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'});
	} else {
	    $first_set = 1;
	    $s .= " reuse=?";
	    push @args, $reuse;
	}
    }

    if (defined $barcode) {
	if ($barcode eq $row_volume->[2]) {
		# message same barcode
	} elsif (!$force && $row_volume->[2]) {
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000002,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			barcode => $row_volume->[2],
			catalog_name => $self->{'catalog_name'});
	} else {
	    $s .= ',' if $first_set;
	    $first_set = 1;
	    $data_set = 1;
	    $s .= " barcode=?";
	    push @args, $barcode;
	}
    }

    if (defined $storage) {
	if ($storage eq $row_volume->[3]) {
		# message same storage
	} elsif (!$force && $row_volume->[3]) {
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000004,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			storage => $row_volume->[3],
			catalog_name => $self->{'catalog_name'});
	} else {
	    my $storage_id;
	    $sth = $dbh->prepare("SELECT storage_id FROM storages WHERE config_id=? AND storage_name=?")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute($self->{'config_id'}, $storage)
		or die "Cannot execute: " . $sth->errstr();
	    my $storage_row = $sth->fetchrow_arrayref;
	    if (!defined $storage_row) {
		$sth = $self->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
		$sth->execute($self->{'config_id'}, $storage, 0)
		    or die "Can't add storage '$storage': " . $sth->errstr();
		$storage_id = $dbh->last_insert_id(undef, undef, "storages", undef);
	    } else {
		$storage_id = $storage_row->[0];
	    }

	    $s .= ',' if $first_set;
	    $first_set = 1;
	    $data_set = 1;
	    $s .= " storage_id=?";
	    push @args, $storage_id;
	}
    }

    if (defined $new_pool) {
	if ($new_pool eq $row_volume->[4]) {
		# message same pool
	} elsif (!$force && $row_volume->[4]) {
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000003,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'});
	} else {
	    my $pool_id;
	    $sth = $dbh->prepare("SELECT pool_id FROM pools WHERE pool_name=?")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute($new_pool)
		or die "Cannot execute: " . $sth->errstr();
	    my $pool_row = $sth->fetchrow_arrayref;
	    if (!defined $pool_row) {
		$sth = $self->make_statement('in poo', 'INSERT INTO pools(pool_name) VALUES (?)');
		$sth->execute($new_pool)
		    or die "Can't add pool '$new_pool': " . $sth->errstr();
		$pool_id = $dbh->last_insert_id(undef, undef, "pools", undef);
	    } else {
		$pool_id = $pool_row->[0];
	    }

	    $s .= ',' if $first_set;
	    $first_set = 1;
	    $data_set = 1;
	    $s .= " pool_id=?";
	    push @args, $pool_id;
	}
    }

    if (defined $meta) {
	if ($meta eq $row_volume->[5]) {
		# message same meta
	} elsif (!$force && $row_volume->[5]) {
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000001,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			meta   => $row_volume->[5],
			catalog_name => $self->{'catalog_name'});
	} else {
	    my $meta_id;
	    $sth = $dbh->prepare("SELECT meta_id FROM metas WHERE meta_label=?")
		or die "Cannot prepare: " . $dbh->errstr();
	    $sth->execute($meta)
		or die "Cannot execute: " . $sth->errstr();
	    my $meta_row = $sth->fetchrow_arrayref;
	    if (!defined $meta_row) {
		$sth = $self->make_statement('in met','INSERT INTO metas(meta_label) VALUES (?)');
		$sth->execute($meta)
		    or die "Can't add meta '$meta': " . $sth->errstr();
		$meta_id = $dbh->last_insert_id(undef, undef, "metas", undef);
	    } else {
		$meta_id = $meta_row->[0];
	    }

	    $s .= ',' if $first_set;
	    $first_set = 1;
	    $data_set = 1;
	    $s .= " meta_id=?";
	    push @args, $meta_id;
	}
    }

    if ($first_set) {
	$s .=  " WHERE volume_id=?";
	    push @args, $volume_id;
	    $sth = $dbh->prepare($s)
		or die "Cannot prepare ($s): " . $dbh->errstr();
	    $sth->execute(@args)
		or die "Cannot execute: " . $sth->errstr();

	if (defined $reuse && $reuse != $row_volume->[1]) {
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => ($reuse?1000045:1000047),
			severity => $Amanda::Message::SUCCESS,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'});
	}
	if ($data_set) {
	    push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000006,
			severity => $Amanda::Message::SUCCESS,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'});
	}
	$self->_write_tapelist();
    }


#    if (defined $barcode && $barcode ne $row_volume->[2]) {
#	return Amanda::Label::Message->new(
#			source_filename => __FILE__,
#			source_line => __LINE__,
#			code   => ???
#			severity => $Amanda::Message::INFO,
#			label  => $label,
#			pool   => $pool,
#			barcode => $barcode,
#			catalog_name => $self->{'catalog'}->{'catalog_name'})
#    }

return \@result_messages;

    if ($nb == 1) {
	# success
	return Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => ($reuse?1000045:1000047),
			severity => $Amanda::Message::INFO,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog'}->{'catalog_name'})
    }

    $sth = $dbh->prepare("SELECT volume_id FROM volumes,pools WHERE volumes.label=? AND volumes.pool_id=pools.pool_id AND pools.pool_name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $nb = $sth->execute($label, $pool);

    if ($reuse == 1) {
	# reuse already set
	return Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000046,
			severity => $Amanda::Message::INFO,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'})
    } else {
	# reuse already unset
	return Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000048,
			severity => $Amanda::Message::INFO,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'})
    }
}

sub volume_assign {
    my $self = shift;

    return $self->run_execute($self, $self->can('_volume_assign'),
		['configs'],
		['volumes', 'storages', 'pools', 'metas'], @_);
}

sub _write_tapelist {
    my $self = shift;
    my $force_rewrite = shift;
    my $keep_at_new_name = shift;
    my $dbh = $self->{'dbh'};
    my $sth;
    my $result = 1;

#debug("_write_tapelist aa");
    if ($self->{'create_mode'}) {
	return;
    }
#debug("_write_tapelist bb");

    if (!$force_rewrite && rand(100) < 99) {
	debug("not writing the tapelist because of rand");
	$self->{'need_write_tapelist'} = 1;
	return;
    }
#debug("_write_tapelist cc");
    if (!-e $self->{'tapelist_lockname'}) {
        open(my $fhl, ">>", $self->{'tapelist_lockname'});
        close($fhl);
    }
#debug("_write_tapelist dd");
    my $fl = Amanda::Util::file_lock->new($self->{'tapelist_lockname'});
    while(($fl->lock()) == 1) {
	if ($have_usleep) {
	    Time::HiRes::usleep(100000);
	} else {
	    sleep(1);
	}
    }
#debug("_write_tapelist ee");

    my $date = Amanda::Util::generate_timestamp();
    my $new_tapelist_file = $self->{'tapelist_filename'} . "-new-" . $date;
    my $filename;
    my $r;
    my $count = 0;
    my $fhn;
    do {
	$filename = $new_tapelist_file . "." . $count;
	$r = sysopen ($fhn, $filename, O_WRONLY|O_CREAT|O_EXCL);
	die("Could not open '$filename' for writing: $!") if !defined $r && $! != Errno::EEXIST;;
	$count++;
    } while (!defined $r);
    $new_tapelist_file = $filename;

    $sth = $self->make_statement('write_tapelist 1', 'SELECT write_timestamp, orig_write_timestamp, label, reuse, barcode, meta_label, block_size, pool_name, storage_name, config_name FROM volumes, metas, storages, pools, configs WHERE storages.config_id=configs.config_id AND volumes.storage_id=storages.storage_id AND volumes.pool_id=pools.pool_id AND volumes.meta_id=metas.meta_id ORDER BY write_timestamp DESC, label DESC');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
#debug("_write_tapelist ff");
    while (my $row_volume = $sth->fetchrow_arrayref ) {
#debug("_write_tapelist gg $row_volume->[2]");
	my $datestamp = $row_volume->[1];
	my $label = $row_volume->[2];
	my $reuse = $row_volume->[3] ? 'reuse' : 'no-reuse';
	my $barcode   = ((defined $row_volume->[4])? (" BARCODE:"   . $row_volume->[4]) : '');
	my $meta      = ((        $row_volume->[5])? (" META:"      . $row_volume->[5]) : '');
	my $blocksize = ((defined $row_volume->[6])? (" BLOCKSIZE:" . $row_volume->[6]) : '');
	my $pool      = ((        $row_volume->[7])? (" POOL:"      . $row_volume->[7]) : '');
	my $storage   = ((        $row_volume->[8])? (" STORAGE:"   . $row_volume->[8]) : '');
	my $config    = ((        $row_volume->[9])? (" CONFIG:"    . $row_volume->[9]) : '');
	$result &&= print $fhn "$datestamp $label $reuse$barcode$meta$blocksize$pool$storage$config\n";
    }
#debug("_write_tapelist hh");
    my $result_close = close($fhn);
    $result &&= $result_close;
#debug("_write_tapelist ii $result");

    if ($result && (!defined $keep_at_new_name || !$keep_at_new_name)) {
#debug("_write_tapelist ii1");
	unlink($self->{'tapelist_last_write'});
#debug("_write_tapelist ii2");
	unless (move($new_tapelist_file, $self->{'tapelist_filename'})) {
#debug("_write_tapelist ii2a $self->{'tapelist_filename'}");
	    $fl->unlock();
            die ("failed to rename '$new_tapelist_file' to '$self->{'tapelist_filename'}': $!");
	}
#debug("_write_tapelist ii3");
	symlink ("$$", $self->{'tapelist_last_write'});
    }
#debug("_write_tapelist jj");
    $fl->unlock();
#debug("_write_tapelist kk");
    if (!defined $keep_at_new_name || !$keep_at_new_name) {
	$self->{'need_write_tapelist'} = 0;
	$self->{'tapelist'}->reload(undef, 1) if $self->{'tapelist'};
    }
#debug("_write_tapelist ll");
}

sub write_tapelist {
    my $self = shift;

    return $self->run_execute($self, $self->can('_write_tapelist'),
		['volumes', 'storages', 'pools', 'metas', 'configs'],
		undef, @_);
}

sub list_retention {
    my $self     = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my @volumes = $self->find_volumes(no_retention => 1);
    my @labels = map { $_->{'label'} } @volumes;
    return \@labels;
}

sub list_no_retention {
    my $self     = shift;

    my @volumes = $self->find_volumes(have_retention => 1);
    my @labels = map { $_->{'label'} } @volumes;
    return \@labels;
}

package Amanda::DB::Catalog2::SQL::volume;

use Amanda::Debug qw( :logging );
use Amanda::Config qw( :constants );

sub new {
    my $class     = shift;
    my $catalog   = shift;
    my $volume_id = shift;
    my %params    = @_;

    my $self = bless {
	catalog   => $catalog,
	volume_id => $volume_id,
	%params,
    }, $class;

    return $self;
}

sub _set {
    my $self = shift;
    my $storage_name = shift;
    my $write_timestamp = shift;

    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("SELECT storage_id, write_timestamp FROM volumes WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($self->{'volume_id'})
	or die "Cannot execute: " . $sth->errstr();
    my $volumes_row = $sth->fetchrow_arrayref;
    $self->{'storage_id'} = $volumes_row->[0];
    $self->{'write_timestamp'} = $volumes_row->[1];

    $sth = $dbh->prepare("UPDATE volumes SET storage_id=(SELECT storage_id FROM storages WHERE config_id=? AND storage_name=?),write_timestamp=?,retention_days=1,retention_full=1,retention_recover=1,retention_tape=1 WHERE volume_id=? AND write_timestamp!=1")
	or die "Cannot prepare: " . $dbh->errstr();
    my $nb = $sth->execute($self->{'catalog'}{'config_id'}, $storage_name, $write_timestamp, $self->{'volume_id'})
	or die "Cannot execute: " . $sth->errstr();

    if ($nb == 0) {
	delete $self->{'storage_id'};
	delete $self->{'write_timestamp'};
    }

    $self->{'catalog'}->_write_tapelist();
    return $nb;
}

sub set {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_set'),
		['storages', 'pools', 'metas', 'configs'],
		['volumes'], @_);
}

sub _unset {
    my $self = shift;
    my $storage_name = shift;
    my $write_timestamp = shift;

    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("UPDATE volumes SET storage_id=?, reuse=1, write_timestamp=?, orig_write_timestamp=?, retention_days=0, retention_full=0, retention_recover=0, retention_tape=0 WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($self->{'storage_id'}, $self->{'write_timestamp'}, $self->{'write_timestamp'}, $self->{'volume_id'})
	or die "Cannot execute: " . $sth->errstr();
    delete $self->{'storage_id'};
    delete $self->{'write_timestamp'};
    $self->{'catalog'}->_write_tapelist();
}

sub unset {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_unset'),
		['storages', 'pools', 'metas', 'configs'],
		['volumes'], @_);
}

sub _set_write_timestamp {
    my $self = shift;
    my $write_timestamp = shift;
    my $storage = shift;

    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("UPDATE volumes SET reuse=1, write_timestamp=?, orig_write_timestamp=? WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($write_timestamp, $write_timestamp, $self->{'volume_id'})
	or die "Cannot execute: " . $sth->errstr();
    $catalog->_compute_storage_retention_tape(
				$storage->{'storage_name'},
				$storage->{'tapepool'},
				$storage->{'policy'}->{'retention_tapes'});
    $self->{'catalog'}->_write_tapelist();
}

sub set_write_timestamp {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_set_write_timestamp'),
		['storages', 'pools', 'metas', 'configs'],
		['volumes'], @_);
}

sub set_retention_name {
    my $self = shift;
    my $retention_type = Amanda::DB::Catalog2::SQL::volume::retention_type($self);
    $self->{'retention_name'} = Amanda::Config::get_retention_name($retention_type);
}

sub retention_type {
    my $self = shift;
    return $RETENTION_OTHER_CONFIG if $self->{'config'} ne Amanda::Config::get_config_name();
    return $RETENTION_NO_REUSE if !$self->{'reuse'};
    return $RETENTION_TAPES if $self->{'retention_tape'};
    return $RETENTION_DAYS if $self->{'retention_days'};
    return $RETENTION_FULL if $self->{'retention_full'};
    return $RETENTION_RECOVER if $self->{'retention_recover'};
    # return RETENTION_CMD_COPY;
    # return RETENTION_CMD_FLUSH;
    # return RETENTION_CMD_RESTORE;
    # return RETENTION_OTHER_CONFIG;
    return $RETENTION_NO;
}


sub _remove {
    my $self = shift;

    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;
    my $volume_id = $self->{'volume_id'};

    my $copy_table = "copy_ids_$$";
    my $image_table = "image_ids_$$";
    #my $disk_table = "disk_ids_$$";
    #my $host_table = "host_ids_$$";
    # get a list of all copy_id modified
    $sth = $dbh->prepare("CREATE $catalog->{'temporary'} TABLE $copy_table AS SELECT DISTINCT copy_id FROM parts WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();

    # get a list of all image_id modified
    $sth = $dbh->prepare("CREATE $catalog->{'temporary'} TABLE $image_table AS SELECT DISTINCT image_id FROM copys WHERE copys.copy_id IN (SELECT copy_id FROM $copy_table)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    # delete the part
    $sth = $dbh->prepare("DELETE FROM parts WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();

    # delete the volume
    $sth = $dbh->prepare("DELETE FROM volumes WHERE volume_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();

    # delete the copy if all their part are removed
    $sth = $dbh->prepare("DELETE FROM copys WHERE copy_id IN (SELECT copy_id FROM $copy_table) AND NOT EXISTS (SELECT copy_id FROM parts WHERE copys.copy_id=parts.copy_id)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    # update copy_status if some parts are still there
    $sth = $dbh->prepare("UPDATE copys SET copy_status='PARTIAL' WHERE copy_status='OK' AND copy_id IN (SELECT copy_id FROM $copy_table) AND EXISTS (SELECT copy_id FROM parts WHERE copys.copy_id=parts.copy_id)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    # delete the image if all their copy are removed
    $sth = $dbh->prepare("DELETE FROM images WHERE image_id IN (SELECT image_id FROM $image_table) AND NOT EXISTS (SELECT image_id FROM copys WHERE images.image_id=copys.image_id)")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    # update dump_status if all not one copy with OK copy_status
    $sth = $dbh->prepare("UPDATE images SET dump_status='PARTIAL' WHERE dump_status='OK' AND image_id IN (SELECT image_id FROM $image_table) AND NOT EXISTS (SELECT image_id FROM copys WHERE images.image_id=copys.image_id AND copy_status='OK')")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DROP $catalog->{'drop_temporary'} TABLE $image_table")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("DROP $catalog->{'drop_temporary'} TABLE $copy_table")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $self->{'catalog'}->_write_tapelist() if $self->{'storage'} ne 'HOLDING';
}

sub remove {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_remove'),
			['storages', 'pools', 'metas', 'configs'],
			['volumes','parts','copys','images'], @_);
}

sub _close {
    my $self = shift;

    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;
    my $volume_id = $self->{'volume_id'};

    $sth = $dbh->prepare("SELECT DISTINCT volume_id FROM copys,parts WHERE parts.volume_id=? AND parts.copy_id=copys.copy_id AND ( copys.retention_days=1 OR copys.retention_full=1 OR copys.retention_recover=1 ) LIMIT 1")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($volume_id)
	or die "Cannot execute: " . $sth->errstr();
    my $row = $sth->fetchrow_arrayref;
    if (!defined $row) {
	$sth = $dbh->prepare("UPDATE volumes SET retention_days=0, retention_full=0, retention_recover=0 WHERE volume_id=?")
            or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($volume_id)
            or die "Cannot execute: " . $sth->errstr();
    }
}

sub close {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_close'),
		 ['copys', 'parts'],
		 ['volumes'], @_);
}


package Amanda::DB::Catalog2::SQL::image;

use Amanda::Debug qw( :logging );

sub new {
    my $class     = shift;
    my $catalog   = shift;
    my $image_id  = shift;
    my $disk_id   = shift;
    my $dump_timestamp = shift;
    my $level     = shift;
    my $dump_status = shift;
    my $image_pid = shift;

    if (!defined $disk_id) {
	my $dbh = $catalog->{'dbh'};
	my $sth;

	# get/add the storage */
	$sth = $dbh->prepare("SELECT disk_id, dump_timestamp, level, dump_status FROM images WHERE image_id=?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($image_id)
	    or die "Cannot execute: " . $sth->errstr();
	# get the first row
	my $image_row = $sth->fetchrow_arrayref;
	if (!defined $image_row) {
	    debug("No image $image_id");
	    return undef;
	} else {
	    $disk_id = $image_row->[0];
	    $dump_timestamp = $image_row->[1];
	    $level = $image_row->[2];
	    $dump_status = $image_row->[3];
	}
    }

    my $self = bless {
	catalog  => $catalog,
	image_id => $image_id,
	disk_id  => $disk_id,
	dump_timestamp => $dump_timestamp,
	level    => $level,
	dump_status => $dump_status,
	image_pid => $image_pid,
    }, $class;

    return $self;
}

sub _add_copy {
    my $self = shift;
    my $storage_name = shift;
    my $write_timestamp = shift;
    my $retention_days = shift || 0;
    my $retention_full = shift || 0;
    my $retention_recover = shift || 0;
    my $copy_pid = shift || 0;

    my $image_id = $self->{'image_id'};
    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    # get/add the storage */
    $sth = $catalog->make_statement('sel sto_id', 'SELECT storage_id FROM storages WHERE storage_name=? AND config_id=?');
    $sth->execute($storage_name, $self->{'catalog'}{'config_id'})
	or die "Cannot execute: " . $sth->errstr();
    # get the first row
    my $storage_row = $sth->fetchrow_arrayref;
    my $storage_id;
    if (!defined $storage_row) {
	my $sth1 = $catalog->make_statement('in sto', 'INSERT INTO storages(config_id, storage_name, last_sequence_id) VALUES (?, ?, ?)');
	$sth1->execute($self->{'catalog'}->{'config_id'}, $storage_name, 0) or die "Can't add storage $storage_name: " . $sth1->errstr()
	    or die "Cannot execute: " . $sth1->errstr();
	$storage_id = $dbh->last_insert_id(undef, undef, "storages", undef);
    } else {
	$storage_id = $storage_row->[0];
    }

    my $level = $self->{'level'};
    my $parent_id = 0;
    if ($level > 0) {
	# find the parent
	my $disk_id = $self->{'disk_id'};
	my $dump_timestamp = $self->{'dump_timestamp'};
	$level--;
	$sth = $catalog->make_statement('sel cop parent', 'SELECT copy_id FROM copys,images,storages WHERE copys.image_id=images.image_id AND images.disk_id=? AND level=? AND dump_timestamp<? AND copys.storage_id=? ORDER BY dump_timestamp DESC LIMIT 1');
	$sth->execute($disk_id, $level, $dump_timestamp, $storage_id)
	    or die "Can't execute: " . $sth->errstr();
	my $copy_row = $sth->fetchrow_arrayref;
	if (!defined $copy_row) {
	    #no parent
	} else {
	    $parent_id = $copy_row->[0];
	}
    }

    $sth = $catalog->make_statement('in cop', 'INSERT INTO copys(image_id,storage_id,write_timestamp,parent_copy_id,nb_parts,kb,bytes,copy_status,server_crc,retention_days,retention_full,retention_recover,copy_message,copy_pid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    $sth->execute($image_id, $storage_id, $write_timestamp, $parent_id, 0, 0, 0, "DUMPING", undef, $retention_days, $retention_full, $retention_recover, undef, $copy_pid)
	or die "Can't add copy $image_id $write_timestamp DUMPPING 0: " . $sth->errstr();
    my $copy_id = $dbh->last_insert_id(undef, undef, "copys", undef);

    return Amanda::DB::Catalog2::SQL::copy->new($catalog, $copy_id);
}

sub add_copy  {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_add_copy'),
			['images'],
			['storages', 'copys'], @_);
}
sub _finish_image {
    my $self           = shift;
    my $orig_kb        = shift;
    my $dump_status    = shift;
    my $nb_files       = shift;
    my $nb_directories = shift;
    my $native_crc     = shift;
    my $client_crc     = shift;
    my $server_crc     = shift;
    my $dump_message   = shift;

    my $catalog = $self->{'catalog'};
    my $image_id = $self->{'image_id'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    $dump_message = undef if defined $dump_message && $dump_message eq "";
    if ($dump_status ne "OK") {
	$sth = $catalog->make_statement('_finish_image:sel ima', 'SELECT dump_status, dump_message FROM images WHERE image_id=?');
	$sth->execute($image_id)
	    or die "Cannot execute: " . $sth->errstr();
	my $image_row = $sth->fetchrow_arrayref;
	if ($image_row && $image_row->[0] eq "OK") {
	    $dump_status = "OK";
	    $dump_message = $image_row->[1];
	}
    }

    $sth = $catalog->make_statement('_finish_image:up ima', 'UPDATE images SET orig_kb=?, dump_status=?, nb_files=?, nb_directories=?, native_crc=?, client_crc=?, server_crc=?, dump_message=?, image_pid=0 WHERE image_id=?');
    $sth->execute($orig_kb, $dump_status, $nb_files, $nb_directories, $native_crc, $client_crc, $server_crc, $dump_message, $image_id)
	or die "Cannot execute: " . $sth->errstr();

    return $image_id;
}

sub finish_image  {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_finish_image'),
			undef,
			['images'], @_);
}

sub _find_copies {
    my $self = shift;
    my $catalog = $self->{'catalog'};
    my $dbh = $catalog->{'dbh'};
    my $sth;
    my @copies;

    # get the copy */
    $sth = $dbh->prepare("SELECT copy_id, copy_status, copy_pid FROM copys WHERE image_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($self->{'image_id'})
	or die "Cannot execute " . $sth->errstr();
    # get the first row
    while (my $copy_row = $sth->fetchrow_arrayref) {
	my $copy_id = $copy_row->[0];
	my $copy_status = $copy_row->[1];
	my $copy_pid = $copy_row->[2];
        push @copies, Amanda::DB::Catalog2::SQL::copy->new($self->{'catalog'}, $copy_id, $copy_status, $copy_pid);
    }
    return @copies;
}

sub find_copies  {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_find_copies'),
			['copys'],
			undef, @_);
}

package Amanda::DB::Catalog2::SQL::copy;

use Amanda::Debug qw( :logging );

sub new {
    my $class   = shift;
    my $catalog = shift;
    my $copy_id = shift;
    my $copy_status = shift;
    my $copy_pid = shift;

    my $self = bless {
	catalog => $catalog,
	copy_id => $copy_id,
	copy_status => $copy_status,
	copy_pid => $copy_pid,
    }, $class;

    return $self;
}

sub _find_a_part {
    my $self        = shift;
    my $catalog = $self->{'catalog'};
    my $copy_id = $self->{'copy_id'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("SELECT part_id FROM parts WHERE copy_id=? LIMIT 1")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($copy_id)
	or die "Cannot execute: " . $sth->errstr();
    return 1 if $sth->fetchrow_arrayref;
    return undef;
}

sub find_a_part  {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_find_a_part'),
			['parts'],
			undef, @_);
}

sub _add_part {
    my $self        = shift;
    my $volume      = shift;
    my $part_offset = shift;
    my $part_size   = shift;
    my $filenum     = shift;
    my $part_num    = shift;
    my $part_status = shift;
    my $part_message= shift;

    my $catalog = $self->{'catalog'};
    my $copy_id = $self->{'copy_id'};
    my $volume_id = $volume->{'volume_id'};
    my $dbh = $catalog->{'dbh'};
    my $sth;
    $sth = $catalog->make_statement('in par', 'INSERT INTO parts(copy_id,volume_id,part_offset,part_size,filenum,part_num,part_status,part_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
    $sth->execute($copy_id, $volume_id, $part_offset, $part_size, $filenum, $part_num, $part_status, $part_message)
	or die "Cannot execute: " . $sth->errstr();
}

sub add_part  {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_add_part'),
			undef,
			['parts'], @_);
}

sub _finish_copy {
    my $self        = shift;
    my $nb_parts    = shift;
    my $kb          = shift;
    my $bytes       = shift;
    my $copy_status = shift;
    my $server_crc  = shift;
    my $copy_message= shift;

    my $catalog = $self->{'catalog'};
    my $copy_id = $self->{'copy_id'};
    my $dbh = $catalog->{'dbh'};
    my $sth;

    $kb = int($kb);
    $bytes = int($bytes);
    $sth = $dbh->prepare("UPDATE copys SET nb_parts=?, kb=?, bytes=?, copy_status=?, server_crc=?, copy_message=?, copy_pid=0 WHERE copy_id=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($nb_parts, $kb, $bytes, $copy_status, $server_crc, $copy_message, $copy_id)
	or die "Cannot execute: " . $sth->errstr();

    if ($copy_status ne "OK") {
	$sth = $dbh->prepare("UPDATE copys SET retention_days=0, retention_full=0, retention_recover=0 WHERE copy_id=?")
	    or die "Cannot prepare: " . $dbh->errstr();
	$sth->execute($copy_id)
	    or die "Cannot execute: " . $sth->errstr();
    }
}

sub finish_copy  {
    my $self = shift;
    my $catalog = $self->{'catalog'};

    return $catalog->run_execute($self, $self->can('_finish_copy'),
			undef,
			['copys'], @_);
}

package Amanda::DB::Catalog2::SQL::cmd;

use Amanda::Debug qw( :logging );
use Amanda::Config qw( :constants );

sub new {
    my $class      = shift;
    my $catalog    = shift;
    my $command_id = shift;
    my %params     = @_;

    my $self = bless {
	catalog    => $catalog,
	command_id => $command_id,
	%params,
    }, $class;

    return $self;
}

1;
