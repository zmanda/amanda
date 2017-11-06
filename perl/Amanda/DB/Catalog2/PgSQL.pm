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

package Amanda::DB::Catalog2::PgSQL;

=head1 NAME

Amanda::DB::Catalog2::PgSQL - access the Amanda catalog with PgSQL

=head1 SYNOPSIS

This package implements the "PgSQL" catalog.  See C<amanda-catalog(7)>.

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

    $connect_str = "DBI:Pg";
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
    debug("connect_str: $connect_str");
    debug("username $username");
    my %attributes = ( RaiseError => 1, PrintError => 0, PrintWarn => 0, pg_server_prepare => 1, AutoInactiveDestroy => 1 );

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
        tapelist       => $tl,
        catalog_name   => $catalog_name,
        catalog_conf   => $catalog_conf,
	autoincrement  => '',
	autoincrementX => 'SERIAL',
	foreign_key    => $foreign_key,
	limit_in_subquery => 0,
	subquery_same_table => 0,
	temporary      => 'temporary',
	drop_temporary => '',
	smallint       => 'SMALLINT',
	char           => 'VARCHAR',
    }, $class;

    $self->connect();
    my $dbh = $self->{'dbh'};

    debug("PgSQL version: " . $dbh->{'pg_lib_version'} . " : " . $dbh->{'pg_server_version'});

    if ($params{'drop_tables'}) {
	local $dbh->{RaiseError} = 0;
	$dbh->do("DROP TABLE parts");
	$dbh->do("DROP TABLE commands");
	$dbh->do("DROP TABLE volumes");
	$dbh->do("DROP TABLE copys");
	$dbh->do("DROP TABLE images");
	$dbh->do("DROP TABLE metas");
	$dbh->do("DROP TABLE pools");
	$dbh->do("DROP TABLE storages");
	$dbh->do("DROP TABLE disks");
	$dbh->do("DROP TABLE hosts");
	$dbh->do("DROP TABLE configs");
	$dbh->do("DROP TABLE version");
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
#    if (!defined $read_lock && !defined $write_lock) {
#	$lock_query .= " configs IN ACCESS EXCLUSIVE MODE, commands IN ACCESS EXCLUSIVE MODE, copys IN ACCESS EXCLUSIVE MODE, disks IN ACCESS EXCLUSIVE MODE, hosts IN ACCESS EXCLUSIVE MODE, images IN ACCESS EXCLUSIVE MODE, metas IN ACCESS EXCLUSIVE MODE, parts IN ACCESS EXCLUSIVE MODE, pools IN ACCESS EXCLUSIVE MODE, storages IN ACCESS EXCLUSIVE MODE, volumes IN ACCESS EXCLUSIVE MODE, version IN ACCESS EXCLUSIVE MODE;"
#    } else {
#	if ($read_lock) {
#	    foreach my $rlock (@$read_lock) {
#		if ($first) {
#		    $first = 0;
#		} else {
#		    $lock_query .= ",";
#		}
#		$lock_query .= " $rlock IN SHARE MODE";
#	    }
#	}
#	if ($write_lock) {
#	    foreach my $wlock (@$write_lock) {
#		if ($first) {
#		    $first = 0;
#		} else {
#		    $lock_query .= ",";
#		}
#		$lock_query .= " $wlock IN ACCESS EXCLUSIVE MODE";
#	    }
#	}
#    }
    do {
	$dbh->{'AutoCommit'} = 0;
	eval {
#	    $sth = $dbh->prepare($lock_query);
#	    $sth->execute();

	    $result = $fn->($obj, @_);

	    $dbh->commit;
	};
	if ($@) {
	    $dbh->rollback;
	}
	$dbh->{'AutoCommit'} = 1;
        $stop_loop = 1 if $@ eq '';
	if (!$stop_loop &&
	    ($@ !~ /Deadlock/ ||
	     $@ !~ /Connect/)) {
	    die($@);
	}
    } until $stop_loop;
    return $result;
}

sub table_exists {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    if ($sth = $dbh->prepare("SELECT 1 FROM version LIMIT 1")) {
        return 0;
    }
    return 1;
}

1;
