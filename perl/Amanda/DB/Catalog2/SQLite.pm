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

package Amanda::DB::Catalog2::SQLite;

=head1 NAME

Amanda::DB::Catalog2::SQLite - access the Amanda catalog with SQLite

=head1 SYNOPSIS

This package implements the "SQLite" catalog.  See C<amanda-catalog(7)>.

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

my $SQLITE_DB_VERSION = 5;

my $have_usleep = eval { require Time::HiRes ; 1 };

sub new {
    my $class = shift;
    my $catalog_conf = shift;
    my %params = @_;
    my $autoincrement;
    my $foreign_key;
    my $properties;
    my $username;
    my $password;
    my $connect;
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
    my $dbname = $properties->{'dbname'}->{'values'}[0];
    my $exist_database = -e $dbname;
    if ($params{'drop_tables'}) {
	unlink $dbname;
    }
    $exist_database = -e $dbname;
    if ($params{'create'} and $exist_database) {
	#die("Database '$dbname' already exists");
    } elsif (!$params{'create'} and !$exist_database) {
	die("Database '$dbname' do not exists");
    }

    $username = $properties->{'username'};
    $password = $properties->{'password'};
    $autoincrement = "AUTOINCREMENT";
    $autoincrement = $properties->{'autoincrement'}->{'values'}[0] if exists $properties->{'autoincrement'};
    $foreign_key = "FOREIGN KEY";
    $foreign_key = $properties->{'autoincrement'}->{'values'}[0] if exists $properties->{'autoincrement'};
    $connect = "DBI:$plugin";
    while (my ($key, $values) = each %{$properties}) {
	if ($key ne 'username' &&
	    $key ne 'password' &&
	    $key ne 'autoincrement') {
	    my $value = $values->{'values'}[0];
	    $connect .= ":$key=$value";
	}
    }
    debug("connect: $connect");
    my $dbh = DBI->connect($connect, $username, $password, { sqlite_use_immediate_transaction => 1 })
	or die "Cannot connect: " . $DBI::errstr;

    my $sth = $dbh->prepare("PRAGMA foreign_keys = '1'")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("PRAGMA journal_mode = TRUNCATE")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    $sth = $dbh->prepare("PRAGMA synchronous = OFF")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute() or die "Cannot execute: " . $sth->errstr();

    debug("sqlite version: " . $dbh->{'sqlite_version'});

    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my ($tl, $message) = Amanda::Tapelist->new($tlf);
    if (defined $message) {
	return $message;
    }

    my $self = bless {
	dbh            => $dbh,
        tapelist       => $tl,
        catalog_name   => $catalog_name,
        catalog_conf   => $catalog_conf,
	foreign_key    => $foreign_key,
	limit_in_subquery => 1,
	subquery_same_table => 1,
	temporary      => '',
	drop_temporary => '',
    }, $class;

    return $self;
}

sub run_execute {
    my $self = shift;
    my $obj = shift;
    my $fn = shift;
    my $read_lock = shift;
    my $write_lock = shift;

    return $fn->($obj, @_);
}

sub table_exists {
    my $self = shift;
    my $table_name = shift;
    my $dbh = $self->{'dbh'};
    my $sth;

    $sth = $dbh->prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth->execute($table_name)
	or die "Cannot execute: " . $sth->errstr();
    my $row = $sth->fetchrow_arrayref;
    return 1 if defined $row;
    return 0;
}

1;
