/*
 * Copyright (c) 2008, 2009, 2010 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Tapelist"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Tapelist.pod"

%{
#include "tapefile.h"
%}

%perlcode %{
use Amanda::Debug qw(:logging);
use Amanda::Config qw( config_dir_relative );
use File::Copy;
use File::Basename;

## package functions

sub read_tapelist {
    my ($filename) = @_;

    # let C read the file
    C_read_tapelist($filename);

    # and then read it ourselves
    my @tles;
    open(my $fh, "<", $filename) or return bless \@tles, "Amanda::Tapelist";
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

    # sort in descending order by datestamp, sorting on position, too, to ensure
    # that entries with the same datestamp stay in the right order
    update_positions(\@tles); # call a method with an explicit $self
    @tles = sort {
	   $b->{'datestamp'} cmp $a->{'datestamp'}
	|| $a->{'position'} <=> $b->{'position'}
	} @tles;

    # and re-calculate the positions
    update_positions(\@tles);

    my $self = bless \@tles, "Amanda::Tapelist";

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

    # prepend this (presumably new) volume to the beginning of the list
    unshift @$self, {
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

    my $new_tapelist_file = dirname($filename) . "/-new-" . time();

    open(my $fh, ">", $new_tapelist_file) or die("Could not open '$new_tapelist_file' for writing: $!");
    for my $tle (@$self) {
	my $datestamp = $tle->{'datestamp'};
	my $label = $tle->{'label'};
	my $reuse = $tle->{'reuse'} ? 'reuse' : 'no-reuse';
	my $comment = (defined $tle->{'comment'})? (" #" . $tle->{'comment'}) : '';
	print $fh "$datestamp $label $reuse$comment\n";
    }
    close($fh);
    unless (move($new_tapelist_file, $filename)) {
	die ("failed to rename '$new_tapelist_file' to '$filename': $!");
    }

    # re-read from the C side to synchronize
    C_read_tapelist($filename);
    return undef;
}

## private methods

# update the 'position' key for each TLE
sub update_positions {
    my $self = shift;
    for (my $i = 0; $i < @$self; $i++) {
	$self->[$i]->{'position'} = $i+1;
    }
}

%}

char *get_last_reusable_tape_label(int skip);
char *list_new_tapes(int nb);

/* C functions -- should be called *only* from within this module */

%rename(C_read_tapelist) read_tapelist;
int read_tapelist(char *tapefile);

%rename(C_clear_tapelist) clear_tapelist;
void clear_tapelist(void);
