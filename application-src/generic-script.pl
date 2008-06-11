# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use strict;

my @known_execute_ons = (
    "SUPPORT",
    "PRE-DLE-AMCHECK",
    "PRE-HOST-AMCHECK",
    "POST-DLE-AMCHECK",
    "POST-HOST-AMCHECK",
    "PRE-DLE-ESTIMATE",
    "PRE-HOST-ESTIMATE",
    "POST-DLE-ESTIMATE",
    "POST-HOST-ESTIMATE",
    "PRE-DLE-BACKUP",
    "PRE-HOST-BACKUP",
    "POST-DLE-BACKUP",
    "POST-HOST-BACKUP",
    "PRE-RECOVER",
    "POST-RECOVER",
    "PRE-LEVEL-RECOVER",
    "POST-LEVEL-RECOVER",
    "INTER-LEVEL-RECOVER",
);

sub do_script() {
    my($execute_on) = @_;
    no strict qw(refs);

    $execute_on = uc($execute_on);

    # first make sure this is a valid execute_on.
    my @found_command = grep { $_ eq $execute_on } @known_execute_ons;
    if ($#found_command < 0) {
       print STDERR "Unknown execute_on `$execute_on'.\n";
       exit 1;
    }

    # now convert it to a function name and see if it's
    # defined
    $execute_on =~ tr/A-Z-/a-z_/;
    $execute_on = "command_$execute_on";

    if (!defined &$execute_on) {
       print STDERR "Command `$execute_on' is not supported by this script.\n";
       exit 1;
    }

    # it exists -- call it
    &{$execute_on}();
}

sub check_file {
   my($filename, $mode) = @_;

   stat($filename);

   if($mode eq "e") {
      if( -e _ ) {
         print "OK $filename exists\n";
      }
      else {
         print "ERROR [can not find $filename]\n";
      }
   }
   elsif($mode eq "x") {
      if( -x _ ) {
         print "OK $filename executable\n";
      }
      else {
         print "ERROR [can not execute $filename]\n";
      }
   }
   elsif($mode eq "r") {
      if( -r _ ) {
         print "OK $filename readable\n";
      }
      else {
         print "ERROR [can not read $filename]\n";
      }
   }
   elsif($mode eq "w") {
      if( -w _ ) {
         print "OK $filename writable\n";
      }
      else {
         print "ERROR [can not write $filename]\n";
      }
   }
   else {
      print "ERROR [check_file: unknow mode $mode]\n";
   }
}

1;
