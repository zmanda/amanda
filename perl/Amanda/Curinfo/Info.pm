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


=head1 NAME

Amanda::Curinfo::Info - Perl extension for representing dump
information

=head1 SYNOPSIS

   use Amanda::Curinfo::Info;

   my $info = Amanda::Curinfo::Info->new($infofile);

=head1 DESCRIPTION

C<Amanda::Curinfo::Info> is the format representation for the curinfo
database.  It handles the reading and writing of the individual
entries, while the entry management is left to C<Amanda::Curinfo>.
Further parsing is also dispatched to C<Amanda::Curinfo::History>,
C<Amanda::Curinfo::Stats>, and C<Amanda::Curinfo::Perf>.

=head1 INTERFACE

The constructor for a new info object is very simple.

   my $info = Amanda::Curinfo::Info->new();

Will return an empty info object with the necessary fields all blank.

Given an existing C<$info> object, for example, as provided by
C<Amanda::Curinfo::get_info>, there are other functions present in this
library, but they are helper functions to the previously described
methods, and not to be used directly.

It should also be noted that the reading and writing methods of
C<Amanda::Curinfo::Info> are not meant to be used directly, and should be
left to L<Amanda::Curinfo>.

Reading a previously stored info object is handled with the same
subroutine.

   my $info = Amanda::Curinfo::Info->new($infofile);

Here, C<$info> will contain all the information that was stored in
C<$infofile>.

To write the file to a new location, use the following command:

   $info->write_to_file($infofile);

There are also three corresponding container classes that hold data
and perform parsing functions.  They should only be used when actually
writing info file data.

   my $history =
     Amanda::Curinfo::History->new( $level, $size, $csize, $date, $secs );
   my $stats =
     Amanda::Curinfo::Stats->new( $level, $size, $csize, $secs, $date, $filenum,
       $label );

   my $perf = Amanda::Curinfo::Perf->new();
   $perf->set_rate( $pct1, $pct2, $pct3 );
   $perf->set_comp( $dbl1, $dbl2, $dbl3 );

Note that C<Amanda::Curinfo::Perf> is different.  This is because its
structure is broken up into two lines in the infofile format, and the
length of the C<rate> and C<comp> arrays maybe subject to change in
the future.

You can also instantiate these objects directly from a
properly-formatted line in an infofile:

   my $history = Amanda::Curinfo::History->from_line($hist_line);
   my $stats   = Amanda::Curinfo::Stats->from_line($stat_line);

   my $perf = Amanda::Curinfo::Perf->new();
   $perf->set_rate_from_line($rate_line);
   $perf->set_comp_from_line($comp_line);

Again, creating C<Amanda::Curinfo::Perf> is broken into two calls
because its object appears on two lines.

Writing these objects back to the info file, however, are all identical:

   print $infofh $history->to_line();
   print $infofh $stats->to_line();
   print $infofh $perf_full->to_line("full");
   print $infofh $perf_incr->to_line("incr");

Additionally, the C<$perf> object accepts a prefix to the line.

=head1 SEE ALSO

This package is meant to replace the file reading and writing portions
of server-src/infofile.h.  If you notice any bugs or compatibility
issues, please report them.

=head1 AUTHOR

Paul C. Mantz E<lt>pcmantz@zmanda.comE<gt>

=cut

my $numdot = qr{[.\d]};

package Amanda::Curinfo::Info;

use strict;
use warnings;
use Carp;

use Amanda::Config;

sub new
{
    my ($class, $infofile) = @_;

    my $self = {
        command => undef,
        full    => Amanda::Curinfo::Perf->new(),
        incr    => Amanda::Curinfo::Perf->new(),
        inf              => [],      # contains Amanda::Curinfo::Stats
        history          => [],      # contains Amanda::Curinfo::History
        last_level       => undef,
        consecutive_runs => undef,
    };

    bless $self, $class;
    $self->read_infofile($infofile) if -e $infofile;

    return $self;
}

sub get_dumpdate
{
    my ( $self, $level ) = @_;
    my $inf  = $self->{inf};
    my $date = 0;            # Ideally should be set to the epoch, but 0 is fine

    for ( my $l = 0 ; $l < $level ; $l++ ) {

        my $this_date = $inf->[$l]->{date};
        $date = $this_date if ( $this_date > $date );
    }

    my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
      gmtime $date;

    my $dumpdate = sprintf(
        '%d:%d:%d:%d:%d:%d',
        $year + 1900,
        $mon + 1, $mday, $hour, $min, $sec
    );

    return $dumpdate;
}

sub read_infofile
{
    my ( $self, $infofile ) = @_;

    open my $fh, "<", $infofile or croak "couldn't open $infofile: $!";

    ## read in the fixed-length data
    $self->read_infofile_perfs($fh);

    ## read in the stats data
    $self->read_infofile_stats($fh);

    ## read in the history data
    $self->read_infofile_history($fh);

    close $fh;

    return 1;
}

sub read_infofile_perfs
{
    my ($self, $fh) = @_;

    my $fail = sub {
        my ($line) = @_;
        croak "error: malformed infofile header in $self->infofile:$line\n";
    };

    my $skip_blanks = sub {
        my $line = "";
        while ($line eq "") {
            croak "error: infofile ended prematurely" if eof($fh);
            $line = <$fh>;
        }
        return $line;
    };

    # version not paid attention to right now
    my $line = $skip_blanks->();
    ($line =~ /^version: ($numdot+)/) ? 1 : $fail->($line);

    $line = $skip_blanks->();
    ($line =~ /^command: ($numdot+)/) ? $self->{command} = $1 : $fail->($line);

    $line = $skip_blanks->();
    ($line =~ /^full-rate: ($numdot+) ($numdot+) ($numdot+)/)
      ? $self->{full}->set_rate($1, $2, $3)
      : $fail->($line);

    $line = $skip_blanks->();
    ($line =~ /^full-comp: ($numdot+) ($numdot+) ($numdot+)/)
      ? $self->{full}->set_comp($1, $2, $3)
      : $fail->($line);

    $line = $skip_blanks->();
    ($line =~ /^incr-rate: ($numdot+) ($numdot+) ($numdot+)/)
      ? $self->{incr}->set_rate($1, $2, $3)
      : $fail->($line);

    $line = $skip_blanks->();
    ($line =~ /^incr-comp: ($numdot+) ($numdot+) ($numdot+)/)
      ? $self->{incr}->set_comp($1, $2, $3)
      : $fail->($line);

    return 1;
}

sub read_infofile_stats
{
    my ( $self, $fh ) = @_;

    my $inf = $self->{inf};

    while ( my $line = <$fh> ) {

        ## try next line if blank
        if ( $line eq "" ) {
            next;

        } elsif ( $line =~ m{^//} ) {
            croak "unexpected end of data in stats section (received //)\n";

        } elsif ( $line =~ m{^history:} ) {
            croak "history line before end of stats section\n";

        } elsif ( $line =~ m{^stats:} ) {

            ## make a new Stats object and push it on to the queue
            my $stats = Amanda::Curinfo::Stats->from_line($line);
            push @$inf, $stats;

        } elsif ( $line =~ m{^last_level: (\d+) (\d+)$} ) {

            $self->{last_level}       = $1;
            $self->{consecutive_runs} = $2;
            last;

        } else {
            croak "bad line in read_infofile_stats: $line";
        }
    }

    return 1;
}

sub read_infofile_history
{
    my ( $self, $fh ) = @_;

    my $history = $self->{history};

    while ( my $line = <$fh> ) {

        if ( $line =~ m{^//} ) {
            return;

        } elsif ( $line =~ m{^history:} ) {
            my $hist = Amanda::Curinfo::History->from_line($line);
            push @$history, $hist;

        } else {
            croak "bad line found in history section:$line\n";
        }
    }

    #
    # TODO: make sure there were the right number of history lines
    #

    return 1;
}

sub write_to_file
{
    my ( $self, $infofile ) = @_;

    unlink $infofile if -f $infofile;

    open my $fh, ">", $infofile or die "error: couldn't open $infofile: $!";

    ## print basics

    print $fh "version: 0\n";    # 0 for now, may change in future
    print $fh "command: $self->{command}\n";
    print $fh $self->{full}->to_line("full");
    print $fh $self->{incr}->to_line("incr");

    ## print stats

    foreach my $stat ( @{ $self->{inf} } ) {
        print $fh $stat->to_line();
    }
    print $fh "last_level: $self->{last_level} $self->{consecutive_runs}\n";

    foreach my $hist ( @{ $self->{history} } ) {
        print $fh $hist->to_line();
    }
    print $fh "//\n";

    return 1;
}

1;

#
#
#

package Amanda::Curinfo::History;

use strict;
use warnings;
use Carp;

sub new
{
    my $class = shift;
    my ( $level, $size, $csize, $date, $secs ) = @_;

    my $self = {
        level => $level,
        size  => $size,
        csize => $csize,
        date  => $date,
        secs  => $secs,
    };

    return bless $self, $class;
}

sub from_line
{
    my ( $class, $line ) = @_;

    my $self = undef;

    if (
        $line =~ m{^history:    \s+
                     (\d+)      \s+  # level
                     ($numdot+) \s+  # size
                     ($numdot+) \s+  # csize
                     ($numdot+) \s+  # date
                     ($numdot+) $    # secs
                  }x
      ) {
        $self = {
            level => $1,
            size  => $2,
            csize => $3,
            date  => $4,
            secs  => $5,
        };
    } else {
        croak "bad history line: $line";
    }

    return bless $self, $class;
}

sub to_line
{
    my ($self) = @_;
    return
"history: $self->{level} $self->{size} $self->{csize} $self->{date} $self->{secs}\n";
}

1;

#
#
#

package Amanda::Curinfo::Perf;

use strict;
use warnings;
use Carp;

use Amanda::Config;

sub new
{
    my ($class) = @_;

    my $self = {
        rate => undef,
        comp => undef,
    };

    return bless $self, $class;
}

sub set_rate
{
    my ( $self, @rate ) = @_;
    $self->{rate} = \@rate;
}

sub set_comp
{
    my ( $self, @comp ) = @_;
    $self->{comp} = \@comp;
}

sub set_rate_from_line
{
    my ( $self, $line ) = @_;
    return $self->set_field_from_line( $self, $line, "rate" );

}

sub set_comp_from_line
{
    my ( $self, $line ) = @_;
    return $self->set_field_from_line( $self, $line, "comp" );

}

sub set_field_from_line
{
    my ( $self, $line, $field ) = @_;

    if (
        $line =~ m{\w+-$field\: \s+
                      ($numdot) \s+
                      ($numdot) \s+
                      ($numdot) $
                   }x
      ) {
        $self->{$field} = [ $1, $2, $3 ];

    } else {
        croak "bad perf $field line: $line";
    }

    return;
}

sub to_line
{
    my ( $self, $lvl ) = @_;
    return
        "$lvl-rate: "
      . join( " ", @{ $self->{rate} } ) . "\n"
      . "$lvl-comp: "
      . join( " ", @{ $self->{comp} } ) . "\n";
}

1;

#
#
#

package Amanda::Curinfo::Stats;

use strict;
use warnings;
use Carp;

sub new
{
    my $class = shift;
    my ( $level, $size, $csize, $secs, $date, $filenum, $label ) = @_;

    my $self = {
        level   => $level,
        size    => $size,
        csize   => $csize,
        secs    => $secs,
        date    => $date,
        filenum => $filenum,
        label   => $label,
    };

    bless $self, $class;
    return $self;
}

sub from_line
{
    my ( $class, $line ) = @_;
    my $self = undef;

    $line =~ m{^stats:      \s+
                     (\d+)      \s+   # level
                     ($numdot+) \s+   # size
                     ($numdot+) \s+   # csize
                     ($numdot+) \s+   # sec
                     ($numdot+) \s+   # date
                     ($numdot+) \s+   # filenum
                     (.*) $           # label
              }x
      or croak "bad stats line: $line";

    $self = {
        level   => $1,
        size    => $2,
        csize   => $3,
        secs    => $4,
        date    => $5,
        filenum => $6,
        label   => $7,
    };
    return bless $self, $class;
}

sub to_line
{
    my ($self) = @_;
    return join( " ",
        "stats:",      $self->{level}, $self->{size},    $self->{csize},
        $self->{secs}, $self->{date},  $self->{filenum}, $self->{label} )
      . "\n";
}

1;
