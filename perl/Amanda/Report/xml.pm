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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com


package Amanda::Report::xml;

use strict;
use warnings;

use base qw/Exporter/;

use Amanda::Constants;

our @EXPORT_OK = qw/make_amreport_xml/;

my $indent = " " x 4;
my $depth  = 0;

## Public Interface

sub make_amreport_xml
{
    my ( $report, $org, $config_name ) = @_;
    return make_xml_elt(
        "amreport",
        sub {
            return join(
                "\n",
                make_xml_elt( "org",    $org ),
                make_xml_elt( "config", $config_name ),
                make_xml_elt( "date",   time() ),
                make_programs_xml( $report->{data}{programs} ),
                map {
                    make_dle_xml( $_->[0], $_->[1],
                        $report->get_dle_info( $_->[0], $_->[1] ) )
                  } $report->get_dles()
            );
        },
        { version => $Amanda::Constants::VERSION }
    );
}

## xml printing functions

sub xml_nl
{
    return "\n" . ( $indent x $depth );
}

sub make_xml_elt
{
    my ( $tag, $val, $attribs ) = @_;
    my $text;

    $indent += 1;
    $text = "<$tag";

    if ( defined $attribs ) {
        $text .= ' '
          . join(
            ' ', map { $_ . '="' . $attribs->{$_} . '"' }
              keys %$attribs
          );
    }

    if ( ref $val eq "CODE" ) {
        $text .= ">" . xml_nl() . $val->() . xml_nl() . "</$tag>";
    } else {
        $text .= ( defined $val ) ? ">$val</$tag>" : " />";
    }

    $indent -= 1;
    return $text;
}

sub make_list_xml
{
    my ( $list, $item, @items ) = @_;
    return make_xml_elt(
        $list,
        sub {
            return join( xml_nl(), map { make_xml_elt( $item, $_ ); } @items );
        }
    );
}

## Amanda::Report data elements

sub make_dumper_xml
{
    my ($dumper) = @_;
    return make_xml_elt(
        "dumper",
        sub {
            return join(
                xml_nl(),
                make_xml_elt("insize",  $dumper->{orig_kb} * 1024),
                make_xml_elt("outsize", $dumper->{kb} * 1024),
                make_xml_elt("time",    $dumper->{sec})
            );
        },
        { "result" => $dumper->{status} }
    );
}

sub make_chunker_xml
{
    my ($chunker) = @_;
    return make_xml_elt(
        "chunker",
        sub {
            return join(
                xml_nl(),
                make_xml_elt( "date",  $chunker->{date} ),
                make_xml_elt( "level", $chunker->{level} ),
                make_xml_elt( "time",  $chunker->{sec} ),
                make_xml_elt( "bytes", $chunker->{kb} * 1024 ),
                make_xml_elt( "bps",   $chunker->{kps} * 1024 ),
            );
        },
        { "result" => $chunker->{status} }
    );
}

sub make_taper_xml
{
    my ($taper) = @_;
    return make_xml_elt(
        "taper",
        sub {
            return join(
                xml_nl(),
                make_xml_elt( "date",  $taper->{date} ),
                make_xml_elt( "level", $taper->{level} ),
                make_xml_elt( "time",  $taper->{sec} ),
                make_xml_elt( "bytes", $taper->{kb} * 1024 ),
                make_xml_elt( "bps",   $taper->{kps} * 1024 ),
                map { make_part_xml($_) } @{ $taper->{parts} }
            );
        },
        { result => $taper->{status} }
    );
}

sub make_try_xml
{
    my ($try) = @_;
    return make_xml_elt(
        "try",
        sub {
            return join xml_nl(), map {
                    ($_ eq "dumper")  ? make_dumper_xml($try->{$_})
                  : ($_ eq "chunker") ? make_chunker_xml($try->{$_})
                  : ($_ eq "taper")   ? make_taper_xml($try->{$_})
                  :                   "";
            } keys %$try;
        }
    );
}

sub make_estimate_xml
{
    my ($estimate) = @_;
    return (defined $estimate)
      ? make_xml_elt(
        "estimate",
        sub {
            return join(
                xml_nl(),
                make_xml_elt("level",  $estimate->{level}),
                make_xml_elt("time",   $estimate->{sec}),
                make_xml_elt("nbytes", $estimate->{nkb} * 1024),
                make_xml_elt("cbytes", $estimate->{ckb} * 1024),
                make_xml_elt("bps",    $estimate->{kps} * 1024)
            );
        }
      )
      : "";
}

sub make_part_xml
{
    my ($part) = @_;
    return make_xml_elt(
        "part",
        sub {
            return join( xml_nl(),
                make_xml_elt( "label", $part->{label} ),
                make_xml_elt( "date",  $part->{date} ),
                make_xml_elt( "file",  $part->{file} ),
                make_xml_elt( "time",  $part->{sec} ),
                make_xml_elt( "bytes", $part->{kb} * 1024 ),
                make_xml_elt( "bps",   $part->{kps} * 1024 ),
                make_xml_elt( "partnum", $part->{partnum} )
            );
        }
    );
}

sub make_dump_xml
{
    my ($dle, $timestamp) = @_;

    return make_xml_elt(
	"dump",
	sub {
	    return join( xml_nl(),
		make_xml_elt("date", $timestamp),
		map { make_try_xml($_) } @{$dle->{'dumps'}->{$timestamp}});
	}
    );
}

sub make_dle_xml
{
    my ( $hostname, $disk, $dle ) = @_;
    return make_xml_elt(
        "dle",
        sub {
            return join( xml_nl(),
                make_xml_elt( "hostname", $hostname ),
                make_xml_elt( "disk",     $disk ),
                ( defined $dle->{estimate} && %{ $dle->{estimate} } > 0 )?
		      make_estimate_xml( $dle->{estimate} )
		    : (),
		( keys %{$dle->{'dumps'}} > 0 ) ?
		      map { make_dump_xml($dle, $_) } keys %{$dle->{'dumps'}}
		    : (),
                exists $dle->{parts} ?
		      map { make_part_xml($_) } @{ $dle->{parts} }
		    : ()
	    );
        }
    );
}

sub make_program_xml
{
    my ( $program_name, $program, $content ) = @_;
    return make_xml_elt(
        $program_name,
        sub {
            return join(
                xml_nl(),
                $content->(),
                ( exists $program->{notes} )
                ? make_list_xml( "notes", "note", @{ $program->{notes} } )
                : (),
                ( exists $program->{stranges} )
                ? make_list_xml( "stranges", "strange",
                    @{ $program->{stranges} } )
                : (),
                ( exists $program->{errors} )
                ? make_list_xml( "errors", "error", @{ $program->{errors} } )
                : (),
            );
        }
    );
}

sub make_planner_xml
{
    my ($planner) = @_;
    return make_program_xml(
        "planner", $planner,
        sub {
            return join( xml_nl(),
                make_xml_elt( "time",       $planner->{time} ),
                make_xml_elt( "start",      $planner->{start} ),
                make_xml_elt( "start_time", $planner->{start_time} ) );
        }
    );
}

sub make_driver_xml
{
    my ($driver) = @_;
    return make_program_xml(
        "driver", $driver,
        sub {
            return join( xml_nl(),
                make_xml_elt( "time",  $driver->{time} ),
                make_xml_elt( "start", $driver->{start} ) );
        }
    );
}

sub make_dumper_program_xml
{
    my ($dumper) = @_;
    return make_program_xml( "dumper", $dumper, sub { return ""; } );
}

sub make_chunker_program_xml
{
    my ($chunker) = @_;
    return make_program_xml( "chunker", $chunker, sub { return ""; } );
}

sub make_tape_xml
{
    my ( $tape_name, $tape ) = @_;
    return make_xml_elt(
        "tape",
        sub {
            return join(
                xml_nl(),
                make_xml_elt( "name", $tape_name ),
                make_xml_elt( "date", $tape->{date} ),
                defined $tape->{files}
                ? make_xml_elt( "files", $tape->{files} )
                : (),
                defined $tape->{kb}
                ? make_xml_elt( "bytes", $tape->{kb} * 1024 )
                : ()
            );
        }
    );
}

sub make_tapelist_xml
{
    my ($tapelist) = @_;
    return make_xml_elt(
        "tapelist",
        sub {
            return join(
                xml_nl(),
                map { make_tape_xml( $_, $tapelist->{$_} ) } keys %$tapelist
            );
        }
    );
}

sub make_taper_program_xml
{
    my ($taper) = @_;
    return make_program_xml(
        "taper", $taper,
        sub {
            return
              defined $taper->{tapes}
              ? make_tapelist_xml( $taper->{tapes} )
              : ();
        }
    );
}

#
# Note: make_program_xml is a super-type for the individual programs,
# make_programs_xml is the element container for the programs
#
sub make_programs_xml
{
    my ($programs) = @_;

    return make_xml_elt(
        "programs",
        sub {
            return join( xml_nl(),
                exists $programs->{planner}
                ? make_planner_xml( $programs->{planner} )
                : (),
                exists $programs->{driver}
                ? make_driver_xml( $programs->{driver} )
                : (),
                exists $programs->{dumper}
                ? make_dumper_program_xml( $programs->{dumper} )
                : (),
                exists $programs->{chunker}
                ? make_chunker_program_xml( $programs->{chunker} )
                : (),
                exists $programs->{taper}
                ? make_taper_program_xml( $programs->{taper} )
                : () );
        }
    );
}

1;
__END__

=head1 NAME

Amanda::Report::xml - output Amanda::Report objects in xml format

=head1 SYNOPSIS

   use Amanda::Report;
   my $report = Amanda::Report->new($logfile);
   print $report->output_xml();

=head1 DESCRIPTION

Stub documentation for Amanda::Report::xml,

Blah blah blah.

=head2 EXPORT

None by default.

=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Paul C Mantz, E<lt>pcmantz@zmanda.comE<gt>

=head1 BUGS

None reported... yet.

=cut
