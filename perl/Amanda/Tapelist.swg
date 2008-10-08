/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

%module "Amanda::Tapelist"
%include "amglue/amglue.swg"
%include "exception.i"

%{
#include "tapefile.h"
%}

%perlcode %{
use Amanda::Debug qw(:logging);

=head1 NAME

Amanda::Tapelist - manipulate the Amanda tapelist

=head1 SYNOPSIS

    use Amanda::Tapelist;

    my $tl = Amanda::Tapelist::read_tapelist("/path/to/tapefile");
    $tl->add_tapelabel($datestamp, $label);
    $tl->add_tapelabel($datestamp2, $label2, $comment);
    $tl->write("/path/to/tapefile");

=head1 API STATUS

Stable

=head1 OBJECT-ORIENTED INTERFACE

The package-level functions C<read_tapelist($filename)> and C<clear_tapelist()>
both return a new tapelist object.  C<read_tapelist> returns C<undef> if the
tapelist does not exist.  Invalid entries are silently ignored.

A tapelist object is a sequence of tapelist
elements (referred to as TLEs in this document).  Each TLE is a hash with the
following keys:

=over

=item C<position> -- the one-based position of the TLE in the tapelist

=item C<datestamp> -- the datestamp on which this was written, or "0" for an
unused tape

=item C<reuse> -- true if this tape can be reused when it is no longer active

=item C<label> -- tape label

=item C<comment> -- the comment for this tape, or undef if no comment was given

=back

The following methods are available on a tapelist object C<$tl>:

=over

=item C<lookup_tapelabel($lbl)> -- look up and return a reference to the TLE
with the given label

=item C<lookup_tapepos($pos)> -- look up and return a reference to the TLE in
the given position

=item C<lookup_tapedate($date)> -- look up and return a reference to the TLE
with the given datestamp

=item C<remove_tapelabel($lbl)> -- remove the tape with the given label

=item C<add_tapelabel($date, $lbl, $comment)> -- add a tape with the given date,
label, and comment to the end of the tapelist, marking it reusable.

=item C<write($filename)> -- write the tapelist out to C<$filename>.

=back

=head1 INTERACTION WITH C CODE

The C portions of Amanda treat the tapelist as a global variable, while this
package treats it as an object (and can thus handle more than one tapelist
simultaneously).  Every call to C<read_tapelist> fills this global variable
with a copy of the tapelist, and likewise C<clear_tapelist> clears the global.
However, any changes made from Perl are not reflected in the C copy, nor are
changes made by C modules reflected in the Perl copy.

=cut

## package functions

sub read_tapelist {
    my ($filename) = @_;

    # let C read the file
    C_read_tapelist($filename);

    # and then read it ourselves
    open(my $fh, "<", $filename) or return undef;
    my @tles;
    while (my $line = <$fh>) {
	my ($datestamp, $label, $reuse, $comment)
	    = $line =~ m/^([0-9]*)\s([^\s]*)\s(reuse|no-reuse)\s*(?:\#(.*))?$/mx;
	next if !defined $datestamp; # silently filter out bogus lines
	push @tles, {
	    'datestamp' => $datestamp,
	    'label' => $label,
	    'reuse' => ($reuse eq 'reuse'),
	    'comment' => $comment,
	};
    }
    close($fh);

    my $self = bless \@tles, "Amanda::Tapelist";
    $self->update_positions();

    return $self;
}

sub clear_tapelist {
    # clear the C version
    C_clear_tapelist();

    # and produce an empty object
    my $self = bless [], "Amanda::Tapelist";
    $self->update_positions();

    return $self;
}

## methods

sub lookup_tapelabel {
    my $self = shift;
    my ($label) = @_;

    for my $tle (@$self) {
	return $tle if ($tle->{'label'} eq $label);
    }

    return undef;
}

sub lookup_tapepos {
    my $self = shift;
    my ($position) = @_;

    $self->update_positions();
    return $self->[$position-1];
}

sub lookup_tapedate {
    my $self = shift;
    my ($datestamp) = @_;

    for my $tle (@$self) {
	return $tle if ($tle->{'datestamp'} eq $datestamp);
    }

    return undef;
}

sub remove_tapelabel {
    my $self = shift;
    my ($label) = @_;

    for (my $i = 0; $i < @$self; $i++) {
	if ($self->[$i]->{'label'} eq $label) {
	    splice @$self, $i, 1;
	    $self->update_positions();
	    return;
	}
    }
}

sub add_tapelabel {
    my $self = shift;
    my ($datestamp, $label, $comment) = @_;

    push @$self, { 
	'datestamp' => $datestamp,
	'label' => $label,
	'reuse' => 1,
	'comment' => $comment,
    };
    $self->update_positions();
}

sub write {
    my $self = shift;
    my ($filename) = @_;

    open(my $fh, ">", $filename) or die("Could not open '$filename' for writing: $!");
    for my $tle (@$self) {
	my $datestamp = $tle->{'datestamp'};
	my $label = $tle->{'label'};
	my $reuse = $tle->{'reuse'} ? 'reuse' : 'no-reuse';
	my $comment = (defined $tle->{'comment'})? (" #" . $tle->{'comment'}) : '';
	print $fh "$datestamp $label $reuse$comment\n";
    }
    close($fh);

    # re-read from the C side to synchronize
    C_read_tapelist($filename);
}

## TODO -- implement this when it's needed
# =item C<lookup_last_reusable_tape($skip)> -- find the (C<$skip>+1)-th least recent
# reusable tape.  For example, C<last_reusable_tape(1)> would return the
# second-oldest reusable tape.

## private methods

# update the 'position' key for each TLE
sub update_positions {
    my $self = shift;
    for (my $i = 0; $i < @$self; $i++) {
	$self->[$i]->{'position'} = $i+1;
    }
}

%}

/* C functions -- should be called *only* from within this module */

%rename(C_read_tapelist) read_tapelist;
int read_tapelist(char *tapefile);

%rename(C_clear_tapelist) clear_tapelist;
void clear_tapelist(void);
