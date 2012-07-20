# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Curinfo;

use strict;
use warnings;
use Carp;
use File::Copy;
use File::Path qw( mkpath );

use Amanda::Debug qw( :logging );
use Amanda::Util qw( sanitise_filename );

use Amanda::Curinfo::Info;

=head1 NAME

Amanda::Curinfo - Perl extension for representing the curinfo database

=head1 SYNOPSIS

   use Amanda::Curinfo;
   use Amanda::Curinfo::Info;

   ...

   my $ci = Amanda::Curinfo->new($somedir);
   my $info = $ci->get_info($host, $disk);

   ...

   $ci->put_info($host, $disk, $newinfo);

   ...

   $ci->del_info($oldhost, $olddisk);

=head1 DESCRIPTION

C<Amanda::Curinfo> is a pure perl implementation of the older infofile
libraries.

This package manages a directory of files, referred to in the code as
an C<$infodir>, that contain dump data.  Each of these files is stored
in a nested directory structure by its host and disk name.  These
files can be accessed and modified using the provided functions with
the help of the L<Amanda::Curinfo::Info> class.

Note that this terminology is slightly different from the older
infofile.h implementation.  Users with no experience with infofile.h
can skip to the interface section.

In the API for infofile.h, the term C<infofile> actually refers a
directory.  This directory is called an C<infodir> within the
infofile.c code.  This directory held text files, which are referred
to as both C<infofile> and C<txinfofile> internally to infofile.c.

This rewrite simplifies the terminology by referring to the storage
directory as an C<$infodir> and an individual data-storing file as a
C<$infofile>.

=head1 INTERFACE


C<Amanda::Curinfo> is an interface to retrieve and store info files
regarding the backup history of DLEs.

C<Amanda::Curinfo> provides three major routines for handling info
file data.

The C<Amanda::Curinfo> constructor is pretty straightforward:

   my $ci = Amanda::Curinfo->new($infodir);

Where C<$infodir> is a directory.  In order to retrieve a previously
stored info file if the host and disk are known, one can use

   my $info = $ci->get_info($host, $disk);

Once the structure has been updated, it may be re-written to the
database in a similar fashion:

  $ci->put_info($host, $disk, $info);

If one would like to erase an existing info entry in an infodir, the
usage is the same as retrieving an info object.

  $ci->del_info($host, $disk);

To create a new info object, please see the documentation for
L<Amanda::Curinfo::Info>.

=head1 SEE ALSO

This module is meant to replicate the behavior of the library
described in server-src/infofile.h.  If anyone notices any major
problems, please report them.

=head1 AUTHOR

Paul C. Mantz E<lt>pcmantz@zmanda.comE<gt>

=cut


sub new
{
    my ($class, $infodir) = @_;

    (defined $infodir)
      || croak("error: infodir not provided to Amanda::Curinfo");

    my $self = { infodir => $infodir };

    bless $self, $class;
    return $self;
}

sub get_info
{
    my ($self, $host, $disk) = @_;

    my $infodir  = $self->{infodir};
    my $host_q   = sanitise_filename($host);
    my $disk_q   = sanitise_filename($disk);
    my $infofile = "$infodir/$host_q/$disk_q/info";

    return Amanda::Curinfo::Info->new($infofile);
}

sub put_info
{
    my ($self, $host, $disk, $info) = @_;

    my $infodir     = $self->{infodir};
    my $host_q      = sanitise_filename($host);
    my $disk_q      = sanitise_filename($disk);
    my $infofiledir = "$infodir/$host_q/$disk_q";
    my $infofile    = "$infofiledir/info";
    my $infofile_tmp = "$infofile.tmp";

    if (-e $infofile) {
        copy($infofile, $infofile_tmp)
          || croak "error: couldn't back up $infofile";
    } elsif (!-d $infofiledir) {
        mkpath($infofiledir)
          || croak "error: couldn't make path $infofiledir";
    }

    my $restore = sub {
        if (-e $infofile_tmp) {
            copy($infofile_tmp, $infofile)
              || croak
              "error: couldn't restore infofile from backup $infofile_tmp";
            unlink $infofile_tmp;
        }
        croak "error encountered when writing info to $infofile";
    };

    $info->write_to_file($infofile) || $restore->();
    unlink $infofile_tmp if -e $infofile_tmp;
    return 1;
}

sub del_info
{
    my ($self, $host, $disk) = @_;

    my $infodir  = $self->{infodir};
    my $host_q   = sanitise_filename($host);
    my $disk_q   = sanitise_filename($disk);
    my $infofile = "$infodir/$host_q/$disk_q/info";

    return unlink $infofile;
}

1;
