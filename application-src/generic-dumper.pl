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

my @knowns_commands = (
    "SUPPORT",
    "SELFCHECK",
    "ESTIMATE",
    "BACKUP",
    "RESTORE",
);

sub do_application() {
    my($command) = @_;
    no strict qw(refs);

    $command =~ tr/a-z-/A-Z_/;
    debug("command: $command");

    # first make sure this is a valid command.
    my @found_command = grep { $_ eq $command } @knowns_commands;
    if ($#found_command < 0) {
        print STDERR "Unknown command `$command'.\n";
        exit 1;
    }

    # now convert it to a function name and see if it's
    # defined
    $command =~ tr/A-Z-/a-z_/;
    my $function_name = "command_$command";

    if (!defined &$function_name) {
        print STDERR "Command `$command' is not supported by this script.\n";
        exit 1;
    }

    # it exists -- call it
    &{$function_name}();
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

sub check_dir {
}

sub check_suid {
}

1;
