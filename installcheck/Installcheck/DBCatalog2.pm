# vim:ft=perl
# Copyright (c) 2016-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck::DBCatalog2;

use Test::More;
use Installcheck;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Paths;
use Amanda::Config qw( :init :getconf config_dir_relative );

require Exporter;

@ISA = qw(Exporter);
@EXPORT_OK = qw( create_db_catalog2 recreate_db_catalog2 check_db_catalog2 );
@EXPORT = qw( create_db_catalog2 recreate_db_catalog2 check_db_catalog2 );

use strict;
use warnings;

=head1 NAME

Installcheck::DBCatalog2 - interact with DB::Catalog2

=head1 SYNOPSIS

  use Installcheck::DBCatalog2;
  create_db_catalog2('TESTCONF');
  ...
  check_db_catalog2(FILENAME);

=head1 USAGE

=over

=item C< create_db_catalog2 >

Create the DB::Catalog2 on disk

=item C< check_db_catalog2 >

Verify the DB::Catalog2 is what we expect

=back

=cut

sub recreate_db_catalog2 {
    my $confname = shift;

Amanda::Debug::debug("recreate_db_catalog2");
    unlink "$CONFIG_DIR/$confname/SQLite.db";
    create_db_catalog2($confname, 1);
}

sub create_db_catalog2 {
    my $confname = shift;
    my $drop_tables = shift;

Amanda::Debug::debug("create_db_catalog2 $drop_tables");
    #ok(!system("$sbindir/amcatalog $confname create"), "create_db_catalog2")
    if (defined $drop_tables && $drop_tables) {
	ok(!system("$sbindir/amcatalog $confname create drop_tables 2>/dev/null"), "create_db_catalog2")
	    or die("Can't create database");
    } else {
	ok(!system("$sbindir/amcatalog $confname create 2>/dev/null"), "create_db_catalog2")
	    or die("Can't create database");
    }
}

sub based_on_timestamp_epoch {
    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
      localtime 0;

    my $dumpdate = sprintf(
        '%04d%02d%02d%02d%02d%02d',
        $year + 1900,
        $mon + 1, $mday, $hour, $min, $sec);

    return $dumpdate;
}

sub check_db_catalog2 {
    my $confname = shift;
    my $filename = shift;
    my $diskname1_path = shift;
    my $diskname2_path = shift;

    my $temp_filename = "$Amanda::Paths::AMANDA_TMPDIR/check_db_catalog2.$$" . "_" . rand();

    ok(!system("$sbindir/amcatalog $confname export $temp_filename 2>/dev/null"), "export catalog")
	or die("Can't export database");

    my $a = Amanda::Util::slurp($temp_filename);
    my $b = Amanda::Util::slurp("$srcdir/DB_Catalolog2/$filename");
    my $fail = 0;

    my @a = split /\n/, $a;
    my @b = split /\n/, $b;
    my $based_on_timestampo_epoch = based_on_timestamp_epoch();
    while (defined(my $la = shift @a)) {
	my $lb = shift @b;
	$lb =~ s/DISKNAME1_PATH/$diskname1_path/g;
	$lb =~ s/DISKNAME2_PATH/$diskname2_path/g;
	$lb =~ s/19691231190000/$based_on_timestampo_epoch/g;
	if ($la !~ /^$lb$/){
	    $fail=1;
	    diag("-,$la,");
	    diag("+,$lb,");
	}
    }
    foreach my $lb (@b) {
	$fail = 1;
	diag("+$lb");
    }

    unlink $temp_filename;
    ok(!$fail, "catalog: match");
}


1;
