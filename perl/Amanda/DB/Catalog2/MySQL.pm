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

package Amanda::DB::Catalog2::MySQL;

=head1 NAME

Amanda::DB::Catalog2::MySQL - access the Amanda catalog with MySQL

=head1 SYNOPSIS

This package implements the "MySQL" catalog.  See C<amanda-catalog(7)>.

=cut

use strict;
use warnings;
use base qw( Amanda::DB::Catalog2 Amanda::DB::Catalog2::SQL );
use POSIX qw( strftime );
use Fcntl;
use Errno;
use File::Copy qw( move );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( quote_string nicedate weaken_ref :quoting match_disk match_host match_datestamp match_level);
use Amanda::Debug qw ( :logging );
use Amanda::Holding;
use Amanda::Header;
use DBD::mysql 4.043;

sub new {
    my $class = shift;
    my $catalog_conf = shift;
    my %params = @_;
    my $autoincrement;
    my $foreign_key;
    my $properties;
    my $username;
    my $password;
    my $database;
    my $connect_str;
    my $catalog_name;

    if (!$catalog_conf) {
	$catalog_name = getconf($CNF_CATALOG);
	return undef if !$catalog_name;
	debug("catalog_name: $catalog_name");
	$catalog_conf = lookup_catalog($catalog_name) if $catalog_name;
	if (!$catalog_conf) {
	    print "No catalog defined\n";
	    debug("No catalog defined");
	    exit 1;
	}
    }

    my $plugin = Amanda::Config::catalog_getconf($catalog_conf,
						 $CATALOG_PLUGIN);
    $properties = Amanda::Config::catalog_getconf($catalog_conf,
						  $CATALOG_PROPERTY);

    $username = $properties->{'username'}->{'values'}[0];
    $password = $properties->{'password'}->{'values'}[0];
    $autoincrement = "AUTO_INCREMENT";
    $autoincrement = $properties->{'autoincrement'}->{'values'}[0] if exists $properties->{'autoincrement'};
    $foreign_key = "FOREIGN KEY";
    $foreign_key = $properties->{'foreign_key'}->{'values'}[0] if exists $properties->{'foreign_key'};

    $connect_str = "DBI:mysql";
    my $sep = ":";
    while (my ($key, $values) = each %{$properties}) {
	if ($key eq 'database' ||
	    $key eq 'host' ||
	    $key eq 'port') {
	    $key =~ s/-/_/g;
	    my $value = $values->{'values'}[0];
	    $connect_str .= "${sep}$key=$value";
	    $sep = ';';
	}
    }
    $connect_str .= ":" if $sep eq ":";
    debug("connect_str $connect_str");
    debug("username $username");
    my %attributes = ( RaiseError => 1, mysql_server_prepare => 1, AutoInactiveDestroy => 1 );

    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my ($tl, $message) = Amanda::Tapelist->new($tlf);
    if (defined $message) {
	return $message;
    }

    my $self = bless {
	connect_str   => $connect_str,
	username      => $username,
	password      => $password,
	attributes    => \%attributes,
        tapelist      => $tl,
        catalog_name  => $catalog_name,
        catalog_conf  => $catalog_conf,
	autoincrement => $autoincrement,
	foreign_key   => $foreign_key,
	limit_in_subquery => 0,
	subquery_same_table => 0,
	temporary     => 'temporary',
	drop_temporary => 'temporary',
    }, $class;

    $self->connect();
    my $dbh = $self->{'dbh'};

    debug("MySQL version: " . $dbh->{'mysql_clientversion'} . " : " . $dbh->{'mysql_serverversion'});

    if ($params{'drop_tables'}) {
	eval { $dbh->do("drop tables parts, commands, volumes, copys, images, metas, pools, storages, disks, hosts, configs, version"); };
    }

    return $self;
}

sub connect {
    my $self = shift;
    my $dbh;

    do {
	eval { $dbh = DBI->connect($self->{connect_str}, $self->{username}, $self->{password},
				   $self->{attributes}); };
	sleep 1 if !$dbh;
    } until $dbh;

    $self->{'dbh'} = $dbh;
    return $self->{'dbh'};
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
    my $lock_query = "LOCK TABLES";
    my $first = 1;
    if (!defined $read_lock && !defined $write_lock) {
	$lock_query .= " configs WRITE, commands WRITE, copys WRITE, disks WRITE, hosts WRITE, images WRITE, metas WRITE, parts WRITE, pools WRITE, storages WRITE, volumes WRITE, version WRITE;"
    } else {
	if ($read_lock) {
	    foreach my $rlock (@$read_lock) {
		if ($first) {
		    $first = 0;
		} else {
		    $lock_query .= ",";
		}
		$lock_query .= " $rlock READ";
	    }
	}
	if ($write_lock) {
	    foreach my $wlock (@$write_lock) {
		if ($first) {
		    $first = 0;
		} else {
		    $lock_query .= ",";
		}
		$lock_query .= " $wlock WRITE";
	    }
	}
    }
    do {
	eval {
	    $dbh->{'AutoCommit'} = 0;
	    $sth = $dbh->prepare($lock_query);
	    $sth->execute();
	    $result = $fn->($obj, @_);

	    $dbh->commit;

	    $sth = $dbh->prepare("UNLOCK TABLES");
	    $sth->execute();
	    $dbh->commit;
	};
	my $error = $@;
	my $mysql_errno = $dbh->{'mysql_errno'};
	my $mysql_error = $dbh->{'mysql_error'};
	my $errstr = $dbh->errstr;
	eval {
	    $sth = $dbh->prepare("UNLOCK TABLES");
	    $sth->execute();
	    $dbh->commit;
	};
        $stop_loop = 1 if $error eq '';
	if ($error) {
	    my $need_reconnect = 0;
	    eval { $dbh->rollback; };

	    $self->{'statements'} = {};

	    debug("error $error") if defined $error;
	    debug("mysql_errno: $mysql_errno") if defined $mysql_errno;
	    debug("mysql_error: $mysql_error") if defined $mysql_error;
	    debug("errstr: $errstr") if defined $errstr;

	    if ($mysql_errno == 2006) {
		$need_reconnect = 1;
	    }
	    if (defined $errstr && $errstr =~ /has gone away/) {
		$need_reconnect = 1;
	    }
	    if (defined $errstr &&  $errstr =~ /Cannot delete or update a parent/) {
		die ($error);
	    }
	    if ($need_reconnect) {
		eval { $self->connect(); };
		if ($@) {
		    debug("$@");;
		}
		$dbh = $self->{'dbh'};
	    }
	    sleep 1;
	} else {
	    $stop_loop = 1;
	}
    } until $stop_loop;
    return $result;
}

sub table_exists {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("SHOW TABLES LIKE 'versions'")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    return $sth->fetchrow_arrayref
}

sub _compute_retention {
    my $self = shift;
    my $localtime = shift || time;
    my $dbh = $self->{'dbh'};
    my $sth;

    debug("_compute_retention");

    my $copy_table = "copy_ids_$$";
    my $volume_table = "volume_ids_$$";

    $sth = $self->make_statement('dcp', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $copy_table");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    # get all copy_id for this config
    $sth = $self->make_statement('cr cct', "CREATE $self->{'temporary'} TABLE $copy_table AS SELECT copy_id,parent_copy_id,storage_id,images.dump_timestamp,level,retention_days,retention_full,retention_recover FROM copys,images,disks,hosts WHERE hosts.config_id=? AND disks.host_id=hosts.host_id AND images.disk_id=disks.disk_id AND copys.image_id=images.image_id");
    $sth->execute($self->{'config_id'})
	or die "Cannot execute: " . $sth->errstr();

    # set their retention to 1
    $sth = $self->make_statement('cr:setret=1-join', 'UPDATE copys JOIN images ON images.image_id=copys.image_id JOIN disks ON disks.disk_id=images.disk_id JOIN hosts ON hosts.host_id=disks.host_id SET copys.retention_days=1,copys.retention_full=1,copys.retention_recover=1 WHERE hosts.config_id=?');
    $sth->execute($self->{'config_id'})
	or die "Can't update copies with config_id $self->{'config_id'}: " . $sth->errstr();

    $sth = $self->make_statement('sel sto_name', 'SELECT storage_id, storage_name FROM storages WHERE config_id=?');
    $sth->execute($self->{'config_id'})
	or die "Can't select storage with config_id $self->{'config_id'}: " . $sth->errstr();
    my $all_row_storages = $sth->fetchall_arrayref;
    foreach my $storage_row (@$all_row_storages) {
	if ($storage_row->[1] ne "HOLDING") {
	    $self->_compute_retention_storage($copy_table, $storage_row->[0], $storage_row->[1], $localtime);
	}
    }

    $sth = $self->make_statement('cr uvd=1', 'UPDATE volumes JOIN parts ON volumes.volume_id=parts.volume_id JOIN copys ON parts.copy_id=copys.copy_id SET volumes.retention_days=1 WHERE copys.retention_days=1');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvf=1', 'UPDATE volumes JOIN parts ON volumes.volume_id=parts.volume_id JOIN copys ON parts.copy_id=copys.copy_id SET volumes.retention_full=1 WHERE copys.retention_full=1');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvr=1', 'UPDATE volumes JOIN parts ON volumes.volume_id=parts.volume_id JOIN copys ON parts.copy_id=copys.copy_id SET volumes.retention_recover=1 WHERE copys.retention_recover=1');
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvd=0', "UPDATE volumes SET retention_days=0 WHERE retention_days=1 AND NOT EXISTS (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND volumes.volume_id=parts.volume_id AND retention_days=1)");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvf=0', "UPDATE volumes SET retention_full=0 WHERE retention_full=1 AND NOT EXISTS (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND volumes.volume_id=parts.volume_id AND retention_full=1)");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvr=0', "UPDATE volumes SET retention_recover=0 WHERE retention_recover=1 AND NOT EXISTS (SELECT DISTINCT volume_id FROM parts,copys WHERE parts.copy_id=copys.copy_id AND volumes.volume_id=parts.volume_id AND retention_recover=1)");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('dvt', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr cvt', "CREATE $self->{'temporary'} TABLE $volume_table AS SELECT volume_id FROM volumes,storages WHERE storage_name=? AND volumes.storage_id=storages.storage_id AND write_timestamp=0");
    $sth->execute('')
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('cr uvt=0fvt', "UPDATE volumes SET retention_tape=0 WHERE volume_id IN (SELECT volume_id FROM $volume_table)");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('dvt', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('dcp', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $copy_table");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();
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

    my $storage = lookup_storage($storage_name);
    return if !$storage;
    my $policy = lookup_policy(Amanda::Config::storage_getconf($storage, $STORAGE_POLICY));
    return if !$policy;

    # retention_days
    my $retention_days = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_DAYS);
#    if ($retention_days > 0) {
    {
	my $dump_timestamp = strftime "%Y%m%d%H%M%S", localtime($localtime - $retention_days*24*60*60);

	$sth = $self->make_statement('_compute_retention:setretdays=0', 'UPDATE copys JOIN images ON images.image_id=copys.image_id JOIN disks ON disks.disk_id=images.disk_id JOIN hosts ON hosts.host_id=disks.host_id SET copys.retention_days=0 WHERE hosts.config_id=? AND storage_id=? AND dump_timestamp < ?');
	$sth->execute($self->{'config_id'}, $storage_id, $dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();
    }

    # retention_full
    my $retention_full = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_FULL);
#    if ($retention_full > 0) {
    {
	my $dump_timestamp = strftime "%Y%m%d%H%M%S", localtime($localtime - $retention_full*24*60*60);

	$sth = $self->make_statement('_compute_retention:setretfull=0', 'UPDATE copys JOIN images ON images.image_id=copys.image_id JOIN disks ON disks.disk_id=images.disk_id JOIN hosts ON hosts.host_id=disks.host_id SET copys.retention_full=0 WHERE hosts.config_id=? AND storage_id=? AND (dump_timestamp < ? OR level!=0)');
	$sth->execute($self->{'config_id'},$storage_id, $dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();
    }

    $sth = $self->make_statement('crs da', "DROP $self->{'drop_temporary'} TABLE IF EXISTS A_$$");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('crs db', "DROP $self->{'drop_temporary'} TABLE IF EXISTS B_$$");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    $sth = $self->make_statement('crs dc', "DROP $self->{'drop_temporary'} TABLE IF EXISTS C_$$");
    $sth->execute()
	or die "Cannot execute: " . $sth->errstr();

    # retention_recover
    my $retention_recover = Amanda::Config::policy_getconf($policy, $POLICY_RETENTION_RECOVER);
#    if ($retention_recover > 0) {
    {
	my $dump_timestamp = strftime "%Y%m%d%H%M%S", localtime($localtime - $retention_recover*24*60*60);
	# A = table of retention we keep
	$sth = $self->make_statement('crs ca', "CREATE $self->{'temporary'} TABLE A_$$ AS SELECT copy_id,parent_copy_id FROM $copy_table WHERE storage_id=? AND dump_timestamp>=?");
	$sth->execute($storage_id, $dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();

	# B = table of retention we drop
	$sth = $self->make_statement('crs cb', "CREATE $self->{'temporary'} TABLE B_$$ AS SELECT copy_id,parent_copy_id FROM $copy_table WHERE storage_id=? AND dump_timestamp<?");
	$sth->execute($storage_id, $dump_timestamp)
	    or die "Cannot execute: " . $sth->errstr();

	while(1) {
	    # C = table to move from drop to keep
	    $sth = $self->make_statement('crs cc', "CREATE $self->{'temporary'} TABLE C_$$ AS SELECT DISTINCT B_$$.copy_id,B_$$.parent_copy_id FROM A_$$,B_$$ WHERE A_$$.parent_copy_id=B_$$.copy_id");
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    $sth = $self->make_statement('crs sel c', "SELECT * FROM C_$$");
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();
	    my $parent_row = $sth->fetchrow_arrayref;
	    last if !defined $parent_row;

	    $sth = $self->make_statement('crs in afc', "INSERT INTO A_$$ SELECT * FROM C_$$");
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    $sth = $self->make_statement('crs del b', "DELETE FROM B_$$ WHERE EXISTS (SELECT copy_id FROM C_$$ WHERE C_$$.copy_id=B_$$.copy_id)");
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();

	    $sth = $self->make_statement('crs dc', "DROP $self->{'drop_temporary'} TABLE IF EXISTS C_$$");
	    $sth->execute()
		or die "Cannot execute: " . $sth->errstr();
	};

	$sth = $self->make_statement('crs dc', "DROP $self->{'drop_temporary'} TABLE IF EXISTS C_$$");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('crs uc', "UPDATE copys JOIN B_$$ ON copys.copy_id=B_$$.copy_id SET retention_recover=0");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('crs db', "DROP $self->{'drop_temporary'} TABLE IF EXISTS B_$$");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('crs da', "DROP $self->{'drop_temporary'} TABLE IF EXISTS A_$$");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();
    }

    $self->_compute_storage_retention_tape($storage_name,
				storage_getconf($storage, $STORAGE_TAPEPOOL),
				policy_getconf($policy, $POLICY_RETENTION_TAPES));
}

sub _compute_storage_retention_tape {
    my $self = shift;
    my $storage_name = shift;
    my $pool = shift;
    my $retention_tapes = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    my $volume_table = "volume_ids_$$";

    my $storage_id = $self->select_or_add_storage($storage_name);
    my $pool_id = $self->select_or_add_pool($pool);
    if ($pool ne 'HOLDING') {

	$sth = $self->make_statement('cdrt dv', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('cdrt srt0', 'UPDATE volumes SET retention_tape=1 WHERE storage_id=? AND pool_id=? AND retention_tape=0 AND reuse=1 AND write_timestamp!=0');
	my $a = $sth->execute($storage_id, $pool_id)
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('csrt upt0t', 'UPDATE volumes SET retention_tape=0 WHERE pool_id=? AND retention_tape=1 AND reuse=1 AND write_timestamp=0');
	$a = $sth->execute($pool_id)
	    or die "Cannot execute: " . $sth->errstr();

	# get all copy_id for this config
	if ($retention_tapes == 0) {
	    $sth = $self->make_statement('csrt cvt', "CREATE $self->{'temporary'} TABLE $volume_table AS SELECT volume_id FROM volumes WHERE storage_id=? AND pool_id=? AND reuse=1 AND write_timestamp!=0");
	} else {
	    $sth = $self->make_statement('csrt cvtl', "CREATE $self->{'temporary'} TABLE $volume_table AS SELECT volume_id FROM volumes WHERE storage_id=? AND pool_id=? AND reuse=1 AND write_timestamp!=0 ORDER BY write_timestamp DESC, label DESC LIMIT 10000 OFFSET $retention_tapes");
	}
	$a = $sth->execute($storage_id, $pool_id)
	    or die "Cannot execute: " . $sth->errstr();


	$sth = $self->make_statement('cdrt srt1', "UPDATE volumes join $volume_table on volumes.volume_id=$volume_table.volume_id SET retention_tape=0");
	$a = $sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

	$sth = $self->make_statement('csrt dv', "DROP $self->{'drop_temporary'} TABLE IF EXISTS $volume_table");
	$sth->execute()
	    or die "Cannot execute: " . $sth->errstr();

    }
}

1;
