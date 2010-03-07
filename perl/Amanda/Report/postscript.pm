# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Report::postscript;

=head1 NAME

Amanda::Report::postscript -- postscript output for amreport

=head1 DESCRIPTION

This package implements the postscript output for amreport.  See amreport(8)
for more information.

=cut

use strict;
use warnings;

use Amanda::Constants;
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Debug qw( debug );
use Amanda::Util;

sub new
{
    my $class = shift;
    my ($report, $config_name, $logfname) = @_;

    my $self = bless {
	report => $report,
	config_name => $config_name,
	logfname => $logfname,
    }, $class;

    # get some other parameters we'll need
    my $ttyp = getconf($CNF_TAPETYPE);
    my $tt = lookup_tapetype($ttyp) if $ttyp;
    my ($tapelen, $marksize, $template_filename);

    if ($ttyp && $tt) {

        # append null string to get the right context
        $tapelen = "" . tapetype_getconf($tt, $TAPETYPE_LENGTH);
        $marksize = "" . tapetype_getconf($tt, $TAPETYPE_FILEMARK);
        $template_filename = "" . tapetype_getconf($tt, $TAPETYPE_LBL_TEMPL);
    }

    # these values should never be zero, so assign defaults
    $self->{'tapelen'} = $tapelen || 100 * 1024 * 1024;
    $self->{'marksize'} = $marksize || 1 * 1024 * 1024;

    # TODO: this should be a shared method somewhere
    my $timestamp = $report->get_timestamp();
    my ($year, $month, $day) = ($timestamp =~ m/^(\d\d\d\d)(\d\d)(\d\d)/);
    my $date  = POSIX::strftime('%B %e, %Y', 0, 0, 0, $day, $month - 1, $year - 1900);
    $date =~ s/  / /g; # get rid of intervening space
    $self->{'datestr'} = $date;

    # get the template
    $self->{'template'} = $self->_get_template($template_filename);

    return $self;
}

sub write_report
{
    my $self = shift;
    my ($fh) = @_;

    my $tape_labels = $self->{'report'}->get_program_info("taper", "tape_labels", []);
    my $tapes = $self->{'report'}->get_program_info("taper", "tapes", {});

    for my $label (@$tape_labels) {
	my $tape = $tapes->{$label};
        $self->_write_report_tape($fh, $label, $tape);
    }
}

sub _write_report_tape
{
    my $self = shift;
    my ($fh, $label, $tape) = @_;

    # function to quote string literals
    sub psstr {
	my ($str) = @_;
	$str =~ s/([()\\])/\\$1/g;
	return "($str)";
    }

    ## include the template once for each tape (might be overkill, but oh well)
    print $fh $self->{'template'};

    ## header stuff
    print $fh psstr($self->{'datestr'}), " DrawDate\n\n";
    print $fh psstr("Amanda Version $Amanda::Constants::VERSION"), " DrawVers\n";
    print $fh psstr($label), " DrawTitle\n";

    ## pre-calculate everything

    # make a list of the first part in each dumpfile, and at the same
    # time count the origsize and outsize of every file beginning on this
    # tape, and separate sums only of the compressed dumps
    my @first_parts;
    my $total_outsize = 0;
    my $total_origsize = 0;
    my $comp_outsize = 0;
    my $comp_origsize = 0;
    foreach my $dle ($self->{'report'}->get_dles()) {
	my ($host, $disk) = @$dle;
        my $dle_info = $self->{'report'}->get_dle_info($host, $disk);

	# run once for each try for this DLE
	foreach my $try (@{$dle_info->{'tries'}}) {
	    next unless exists $try->{'taper'} and exists $try->{'chunks'};

	    # note that this report only prints a row for the *first* part in the DLE,
	    # and only if that's on this tape.
	    my $chunks = $try->{'chunks'};
	    next unless ($chunks->[0] and $chunks->[0]{'label'} eq $label);

	    # the filenum printed will be this first filenum
	    my $filenum = $chunks->[0]{'file'};

	    # sum the chunk sizes on this label to get the outsize.  Note that the postscript
	    # output does not contain a row for each part, but for each chunk..
	    my $outsize = 0;
	    for my $chunk (@$chunks) {
		next unless $chunk->{'label'} eq $label;
		$outsize += $chunk->{'kb'};
	    }

	    # Get origsize for this try.
	    my $origsize = 0;
	    my $level = -1;

	    # TODO: this is complex and should probably be in a parent-class method
	    if (exists $try->{'dumper'} and $try->{'dumper'}->{'status'} ne 'fail') {
		my $try_dumper = $try->{'dumper'};
		$level = $try_dumper->{'level'};
		$origsize = $try_dumper->{'orig-kb'};
	    } else { # we already know a taper run exists in this try
		my $try_taper = $try->{'taper'};
		$level = $try_taper->{'level'};
		$origsize = $try_taper->{'orig-kb'} if $try_taper->{'orig_kb'};
	    }

	    # TODO TEMPORARY
	    # reporter.c uses the total outsize rather than the sum of the on-tape
	    # parts for all but the final tape containing a split dump.  This is odd,
	    # but we'll be compatible
	    if (@$chunks && $chunks->[-1]->{'label'} ne $label) {
		$outsize = $try->{'taper'}->{'kb'};
	    }

	    $total_outsize += $outsize;
	    $total_origsize += $origsize;

	    if ($outsize != $origsize) {
		$comp_outsize += $outsize;
		$comp_origsize += $origsize;
	    }

	    push @first_parts, [$host, $disk, $level, $filenum, $origsize, $outsize];
	}
    }
    # count filemarks in the tapeused assessment
    my $tapeused = $tape->{'kb'};
    $tapeused += $self->{'marksize'} * (1 + $tape->{'files'});
    # TEMPORARY: add a filemark for each dumpfile, to account for the header .. an approximation
    $tapeused += @first_parts * $self->{'marksize'};

    # sort @first_parts by filenum
    # TEMPORARY: no, sort them by DLE to duplicate reporter.c
    @first_parts = sort {
	($a->[0] cmp $b->[0]) or ($a->[1] cmp $b->[1]) or ($a->[3] <=> $b->[3])
    } @first_parts;

    ## output

    print $fh psstr(sprintf('Total Size:        %6.1lf MB', $tape->{kb} / 1024)),
	    " DrawStat\n";
    print $fh psstr(sprintf('Tape Used (%%)       %4s %%',
				$self->divzero($tapeused * 100, $self->{'tapelen'}))),
	    " DrawStat\n";
# TODO it looks like the C version always calculates corigsize == 0, so this is
# always "--"
# the 'resultsmissing' logfile in the installcheck verifies this: it should have 83.5%
# compression, but the ps output has "--"
# (if this is removed permanently, then $comp_* above can go, too)
    print $fh psstr(sprintf('Compression Ratio:   %4s %%',
	    " -- ")),
#				$self->divzero($total_outsize * 100, $total_origsize))),
	    " DrawStat\n";
    print $fh psstr(sprintf('Filesystems Taped: %4d', $tape->{dle})),
	    " DrawStat\n";

    my $header = ["-", $label, "-", 0, 32, 32];
    for my $ff ($header, @first_parts) {
	my ($host, $name, $level, $filenum, $origsize, $outsize) = @$ff;
	print $fh join(" ",
		psstr($host),
		psstr($name),
		psstr($level),
		psstr(sprintf("%3d", $filenum)),
		psstr(sprintf("%8s", $origsize || "")),
		psstr(sprintf("%8s", $outsize || "")),
		"DrawHost\n");
    }

    print $fh "\nshowpage\n";
}


# copy the user's configured template file into $fh
sub _get_template {
    my $self = shift;
    my ($filename) = @_;

    $filename = config_dir_relative($filename) if $filename;

    if (!$filename || !-r $filename) {
	debug("could not open template file '$filename'");
	return undef;
    }

    return Amanda::Util::slurp($filename);
}

# TODO: this should be a common function somewhere
sub divzero
{
    my $self = shift;
    my ( $a, $b ) = @_;
    my $q;
    return
        ( $b == 0 )              ? "-- "
      : ( ($q = $a / $b) > 99999.95 ) ? "#####"
      : ( $q > 999.95 ) ? sprintf( "%5.0lf", $q )
      :                   sprintf( "%5.1lf", $q );
}

1;
