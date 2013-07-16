# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Curinfo::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 1300000) {
	return "$self->{'host'}:$self->{'disk'} FORCE-LEVEL-1 command was cleared";
    } elsif ($self->{'code'} == 1300001) {
	return "$self->{'host'}:$self->{'disk'} FORCE-BUMP command was cleared";
    } elsif ($self->{'code'} == 1300002) {
	return "$self->{'host'}:$self->{'disk'} full dump done offline, next dump will be at level 1.";
    } elsif ($self->{'code'} == 1300003) {
	return "$self->{'host'}:$self->{'disk'} is set to a forced level 0 at next run.";
    } elsif ($self->{'code'} == 1300004) {
	return "infodir not provided to Amanda::Curinfo.";
    } elsif ($self->{'code'} == 1300005) {
	return "couldn't back up '$self->{'infofile'}': $self->{'error'}.";
    } elsif ($self->{'code'} == 1300006) {
	return "couldn't make path $self->{'infofiledir'}': $self->{'error'}.";
    } elsif ($self->{'code'} == 1300007) {
	return "couldn't restore infofile from backup $self->{'infofile'}: $self->{'error'}.";
    } elsif ($self->{'code'} == 1300008) {
	return "error encountered when writing info to $self->{'infofile'}.";
    } elsif ($self->{'code'} == 1300009) {
	return "malformed infofile header in $self->{'infofile'}:$self->{'line'}.";
    } elsif ($self->{'code'} == 1300010) {
	return "infofile ended prematurely.";
    } elsif ($self->{'code'} == 1300011) {
	return "unexpected end of data in stats section (received //)";
    } elsif ($self->{'code'} == 1300012) {
	return "history line before end of stats section";
    } elsif ($self->{'code'} == 1300013) {
	return "bad line in read_infofile_stats: $self->{'line'}";
    } elsif ($self->{'code'} == 1300014) {
	return "bad line in found in history section: $self->{'line'}";
    } elsif ($self->{'code'} == 1300015) {
	return "couldn't open $self->{'infofile'}: $self->{'error'}";
    } elsif ($self->{'code'} == 1300016) {
	return "bad history line: $self->{'line'}";
    } elsif ($self->{'code'} == 1300017) {
	return "bad perf $self->{'field'} line: $self->{'line'}";
    } elsif ($self->{'code'} == 1300018) {
	return "bad stats line: $self->{'line'}";
    } elsif ($self->{'code'} == 1300019) {
	return "force command for $self->{'host'}:$self->{'disk'} cleared.";
    } elsif ($self->{'code'} == 1300020) {
	return "force-level-1 command for $self->{'host'}:$self->{'disk'} cleared.";
    } elsif ($self->{'code'} == 1300021) {
	return "no force command outstanding for $self->{'host'}:$self->{'disk'}, unchanged.";
    } elsif ($self->{'code'} == 1300022) {
	return "$self->{'host'}:$self->{'disk'} FORCE command was cleared";
    } elsif ($self->{'code'} == 1300023) {
	return "$self->{'host'}:$self->{'disk'} is set to a forced level 1 at next run.";
    } elsif ($self->{'code'} == 1300024) {
	return "$self->{'host'}:$self->{'disk'} FORCE-NO-BUMP command was cleared.";
    } elsif ($self->{'code'} == 1300025) {
	return "$self->{'host'}:$self->{'disk'} is set to bump at next run.";
    } elsif ($self->{'code'} == 1300026) {
	return "$self->{'host'}:$self->{'disk'} is set to not bump at next run.";
    } elsif ($self->{'code'} == 1300027) {
	return "bump command for $self->{'host'}:$self->{'disk'} cleared.";
    } elsif ($self->{'code'} == 1300028) {
	return "no bump command outstanding for $self->{'host'}:$self->{'disk'}, unchanged.";
    } elsif ($self->{'code'} == 1300029) {
	return "couldn't open '$self->{'infofile'}: $self->{'error'}.";
    }
}


package Amanda::Curinfo;

use strict;
use warnings;
use Carp;
use File::Copy;
use File::Path qw( mkpath );

use Amanda::Config qw( :getconf );
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
	|| return Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300004);

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
	  || return Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1300005,
				infofile => $infofile,
				error    => $!);
    } elsif (!-d $infofiledir) {
        mkpath($infofiledir)
	  || return Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code        => 1300006,
				infofiledir => $infofiledir,
				error       => $!);
    }

    my $restore = sub {
        if (-e $infofile_tmp) {
            copy($infofile_tmp, $infofile)
	      || return Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1300007,
				infofile => $infofile,
				error    => $!);
            unlink $infofile_tmp;
        }
	return Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1300008,
				infofile => $infofile);
    };

    $info->write_to_file($infofile) || $restore->();
    unlink $infofile_tmp if -e $infofile_tmp;
    return;
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

sub force {
    my ($self, $dle) = @_;

    my @result_messages;
    my $host = $dle->{'host'}->{'hostname'};
    my $disk = $dle->{'name'};

    my $info = $self->get_info($host, $disk);
    return $info if $info->isa("Amanda::Message");

    $info->set($Amanda::Curinfo::Info::FORCE_FULL);
    if ($info->isset($Amanda::Curinfo::Info::FORCE_LEVEL_1)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_LEVEL_1);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300000,
				host    => $host,
				disk    => $disk)
    }
    if ($info->isset($Amanda::Curinfo::Info::FORCE_BUMP)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_BUMP);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300001,
				host    => $host,
				disk    => $disk)
    }
    my $err = $self->put_info($host, $disk, $info);
    if ($err) {
	push @result_messages, $err;
    } else {
	my $strategy = dumptype_getconf($dle->{config}, $DUMPTYPE_STRATEGY);

        if($strategy == $DS_INCRONLY) {
	   push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300002,
				host    => $host,
				disk    => $disk)
	 } else {
	    push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300003,
				host    => $host,
				disk    => $disk)
	 }
    }
    return @result_messages;
}

sub unforce {
    my ($self, $dle) = @_;

    my @result_messages;
    my $host = $dle->{'host'}->{'hostname'};
    my $disk = $dle->{'name'};
    my $cleared = 0;

    my $info = $self->get_info($host, $disk);
    return $info if $info->isa("Amanda::Message");

    if ($info->isset($Amanda::Curinfo::Info::FORCE_FULL)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_FULL);
	$cleared = 1;
	my $err = $self->put_info($host, $disk, $info);
	if ($err) {
	    push @result_messages, $err;
	} else {
	    push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300019,
				host    => $host,
				disk    => $disk)
	}
    }
    if ($info->isset($Amanda::Curinfo::Info::FORCE_LEVEL_1)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_LEVEL_1);
	$cleared = 1;
	my $err = $self->put_info($host, $disk, $info);
	if ($err) {
	    push @result_messages, $err;
	} else {
	    push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300020,
				host    => $host,
				disk    => $disk)
	}
    }

    if (!$cleared) {
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300021,
				host    => $host,
				disk    => $disk)
    }

    return @result_messages;
}

sub force_level_1 {
    my ($self, $dle) = @_;

    my @result_messages;
    my $host = $dle->{'host'}->{'hostname'};
    my $disk = $dle->{'name'};

    my $info = $self->get_info($host, $disk);
    return $info if $info->isa("Amanda::Message");

    $info->set($Amanda::Curinfo::Info::FORCE_LEVEL_1);
    if ($info->isset($Amanda::Curinfo::Info::FORCE_FULL)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_FULL);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300022,
				host    => $host,
				disk    => $disk)
    }
    if ($info->isset($Amanda::Curinfo::Info::FORCE_BUMP)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_BUMP);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300001,
				host    => $host,
				disk    => $disk)
    }
    my $err = $self->put_info($host, $disk, $info);
    if ($err) {
	push @result_messages, $err;
    } else {
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300023,
				host    => $host,
				disk    => $disk)
    }
    return @result_messages;
}

sub force_bump {
    my ($self, $dle) = @_;

    my @result_messages;
    my $host = $dle->{'host'}->{'hostname'};
    my $disk = $dle->{'name'};

    my $info = $self->get_info($host, $disk);
    return $info if $info->isa("Amanda::Message");

    $info->set($Amanda::Curinfo::Info::FORCE_BUMP);
    if ($info->isset($Amanda::Curinfo::Info::FORCE_NO_BUMP)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_NO_BUMP);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300024,
				host    => $host,
				disk    => $disk)
    }
    if ($info->isset($Amanda::Curinfo::Info::FORCE_FULL)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_FULL);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300022,
				host    => $host,
				disk    => $disk)
    }
    if ($info->isset($Amanda::Curinfo::Info::FORCE_LEVEL_1)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_LEVEL_1);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300000,
				host    => $host,
				disk    => $disk)
    }
    my $err = $self->put_info($host, $disk, $info);
    if ($err) {
	push @result_messages, $err;
    } else {
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300025,
				host    => $host,
				disk    => $disk)
    }
    return @result_messages;
}

sub force_no_bump {
    my ($self, $dle) = @_;

    my @result_messages;
    my $host = $dle->{'host'}->{'hostname'};
    my $disk = $dle->{'name'};

    my $info = $self->get_info($host, $disk);
    return $info if $info->isa("Amanda::Message");

    $info->set($Amanda::Curinfo::Info::FORCE_NO_BUMP);
    if ($info->isset($Amanda::Curinfo::Info::FORCE_BUMP)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_BUMP);
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300001,
				host    => $host,
				disk    => $disk)
    }
    my $err = $self->put_info($host, $disk, $info);
    if ($err) {
	push @result_messages, $err;
    } else {
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300026,
				host    => $host,
				disk    => $disk)
    }
    return @result_messages;
}

sub unforce_bump {
    my ($self, $dle) = @_;

    my @result_messages;
    my $host = $dle->{'host'}->{'hostname'};
    my $disk = $dle->{'name'};
    my $cleared = 0;

    my $info = $self->get_info($host, $disk);
    return $info if $info->isa("Amanda::Message");

    if ($info->isset($Amanda::Curinfo::Info::FORCE_BUMP | $Amanda::Curinfo::Info::FORCE_NO_BUMP)) {
	$info->clear($Amanda::Curinfo::Info::FORCE_BUMP | $Amanda::Curinfo::Info::FORCE_NO_BUMP);
	my $err = $self->put_info($host, $disk, $info);
	if ($err) {
	    push @result_messages, $err;
	} else {
	    push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300027,
				host    => $host,
				disk    => $disk)
	}
    } else {
	push @result_messages, Amanda::Curinfo::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code    => 1300028,
				host    => $host,
				disk    => $disk)
    }

    return @result_messages;
}

1;
