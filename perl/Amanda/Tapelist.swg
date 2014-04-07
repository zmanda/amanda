/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
use Fcntl qw(:flock); # import LOCK_* constants
use Amanda::Message qw( :severity );

## package functions

sub new {
    my ($class)  = shift;
    my ($filename, $lock) = @_;
    my $self = {
	filename => $filename,
	lockname => $filename . '.lock',
    };
    bless $self, $class;

    my $message = $self->reload($lock);
    return ($self, $message);
}

sub clear_tapelist {
    my $self = shift;

    # clear the C version
    C_clear_tapelist();

    $self->{'tles'} = [];

    return $self;
}

## methods

sub reload {
    my $self = shift;
    my ($lock) = @_;

    if ($lock) {
	$self->_take_lock();
    }

    # clear the C copy
    C_clear_tapelist();

    # let C read the file
    C_read_tapelist($self->{'filename'});

    my $message = $self->_read_tapelist();
    return $message;
}

sub lookup_tapelabel {
    my $self = shift;
    my ($label) = @_;

    for my $tle (@{$self->{'tles'}}) {
	return $tle if ($tle->{'label'} eq $label);
    }

    return undef;
}

sub lookup_by_barcode {
    my $self = shift;
    my ($barcode) = @_;

    for my $tle (@{$self->{'tles'}}) {
	return $tle if ($tle->{'barcode'} eq $barcode);
    }

    return undef;
}

sub lookup_tapepos {
    my $self = shift;
    my ($position) = @_;

    $self->_update_positions();
    return $self->{'tles'}->[$position-1];
}

sub lookup_tapedate {
    my $self = shift;
    my ($datestamp) = @_;

    for my $tle (@{$self->{'tles'}}) {
	return $tle if ($tle->{'datestamp'} eq $datestamp);
    }

    return undef;
}

sub remove_tapelabel {
    my $self = shift;
    my ($label) = @_;

    for (my $i = 0; $i < @{$self->{tles}}; $i++) {
	if ($self->{tles}->[$i]->{'label'} eq $label) {
	    splice @{$self->{tles}}, $i, 1;
	    $self->_update_positions();
	    return;
	}
    }
}

sub add_tapelabel {
    my $self = shift;
    my ($datestamp, $label, $comment, $reuse, $meta, $barcode, $blocksize,
	$pool, $storage, $config) = @_;
    $reuse = 1 if !defined $reuse;
    $datestamp = 0 if !defined $datestamp;

    # prepend this (presumably new) volume to the beginning of the list
    my $tle = {
        'datestamp' => $datestamp,
        'label'     => $label,
        'reuse'     => $reuse,
        'barcode'   => $barcode,
        'meta'      => $meta,
        'blocksize' => $blocksize,
        'pool'      => $pool,
        'storage'   => $storage,
        'config'    => $config,
        'comment'   => $comment,
    };
    my $tles = $self->{'tles'};
    if (!defined $tles->[0] ||
	$tles->[0]->{'datestamp'} le $datestamp) {
	unshift @{$tles}, $tle;
    } elsif (defined $tles->[0] &&
	$tles->[@$tles-1]->{'datestamp'} gt $datestamp) {
	push @{$tles}, $tle;
    } else {
	my $added = 0;
	for my $i (0..(@$tles-1)) {
	    if ($tles->[$i]->{'datestamp'} le $datestamp) {
		splice @{$tles}, $i, 0, $tle;
		$added = 1;
		last;
	    }
	}
	push @{$tles}, $tle if !$added;
    }
    $self->_update_positions();
}

sub write {
    my $self = shift;
    my ($filename) = @_;
    my $result = TRUE;
    $filename = $self->{'filename'} if !defined $filename;

    my $new_tapelist_file = $filename . "-new-" . time();

    open(my $fhn, ">", $new_tapelist_file) or die("Could not open '$new_tapelist_file' for writing: $!");
    for my $tle (@{$self->{tles}}) {
	my $datestamp = $tle->{'datestamp'};
	my $label = $tle->{'label'};
	my $reuse = $tle->{'reuse'} ? 'reuse' : 'no-reuse';
	my $barcode = (defined $tle->{'barcode'})? (" BARCODE:" . $tle->{'barcode'}) : '';
	my $meta = (defined $tle->{'meta'})? (" META:" . $tle->{'meta'}) : '';
	my $blocksize = (defined $tle->{'blocksize'})? (" BLOCKSIZE:" . $tle->{'blocksize'}) : '';
	my $pool = (defined $tle->{'pool'})? (" POOL:" . $tle->{'pool'}) : '';
	my $storage = (defined $tle->{'storage'})? (" STORAGE:" . $tle->{'storage'}) : '';
	my $config = (defined $tle->{'config'})? (" CONFIG:" . $tle->{'config'}) : '';
	my $comment = (defined $tle->{'comment'})? (" #" . $tle->{'comment'}) : '';
	$result &&= print $fhn "$datestamp $label $reuse$barcode$meta$blocksize$pool$storage$config$comment\n";
    }
    my $result_close = close($fhn);
    $result &&= $result_close;

    return if (!$result);

    unless (move($new_tapelist_file, $filename)) {
	die ("failed to rename '$new_tapelist_file' to '$filename': $!");
    }

    # re-read from the C side to synchronize
    C_read_tapelist($filename);

    $self->unlock();

    return undef;
}

sub unlock {
    my $self = shift;

    return if !exists $self->{'fl'};

    $self->{'fl'}->unlock();
    delete $self->{'fl'}
}

## private methods

sub _take_lock {
    my $self = shift;

    if (!-e $self->{'lockname'}) {
	open(my $fhl, ">>", $self->{'lockname'});
	close($fhl);
    }
    my $fl = Amanda::Util::file_lock->new($self->{'lockname'});
    while(($r = $fl->lock()) == 1) {
	sleep(1);
    }
    if ($r == 0) {
	$self->{'fl'} = $fl;
    }
}

sub _read_tapelist {
    my $self = shift;

    my @tles;
    my $linenum = 0;
    open(my $fh, "<", $self->{'filename'}) or return undef;
    while (my $line = <$fh>) {
	$linenum++;
	my ($datestamp, $label, $reuse, $barcode, $meta, $blocksize, $pool, $storage, $config, $comment)
	    = $line =~ m/^([0-9]+)\s*([^\s]*)\s*(?:(reuse|no-reuse))?\s*(?:BARCODE:([^\s]*))?\s*(?:META:([^\s]*))?\s*(?:BLOCKSIZE:([^\s]*))?\s*(?:POOL:([^\s]*))?\s*(?:STORAGE:([^\s]*))?\s*(?:CONFIG:([^\s]*))?\s*(?:\#(.*))?$/mx;
	if (!defined $datestamp) {
	    return Amanda::Tapelist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code    => 1600002,
			tapelist_filename => $self->{'filename'},
			linenum => $linenum,
			line    => $line);
	}
	push @tles, {
	    'datestamp' => $datestamp,
	    'label'     => $label,
	    'reuse'     => (!defined $reuse || $reuse eq 'reuse'),
	    'barcode'   => $barcode,
	    'meta'      => $meta,
	    'blocksize' => $blocksize,
	    'pool'      => $pool,
	    'storage'   => $storage,
	    'config'    => $config,
	    'comment'   => $comment,
	};
    }
    close($fh);

    # sort in descending order by datestamp, sorting on position, too, to ensure
    # that entries with the same datestamp stay in the right order
    $self->{'tles'} = \@tles;
    $self->_update_positions();
    @tles = sort {
	   $b->{'datestamp'} cmp $a->{'datestamp'}
	|| $a->{'position'} <=> $b->{'position'}
	} @tles;

    $self->{'tles'} = \@tles;

    # and re-calculate the positions
    $self->_update_positions(\@tles);

    # check for duplicate labels
    my %labels;
    for my $tle (@{$self->{'tles'}}) {
	my $label = $tle->{'label'};
	if (exists $labels{$label}) {
	    return Amanda::Tapelist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			severity => $Amanda::Message::WARNING,
			code    => 1600003,
			tapelist_filename => $self->{'filename'},
			label   => $label);
	}
	$labels{$label} = 1;
    }
    return undef;
}

# update the 'position' key for each TLE
sub _update_positions {
    my $self = shift;
    my $tles = $self->{'tles'};
    for (my $i = 0; $i < scalar @$tles; $i++) {
	$tles->[$i]->{'position'} = $i+1;
    }
}

%}

void compute_retention(void);
/* for list_retention, list_no_retention and list_new_tapes */
%typemap(out) gchar ** {
    gchar **iter;

    if ($1) {
	/* Count */
	EXTEND(SP, g_strv_length($1)); /* make room for return values */

	/* Note that we set $result several times. the nature of
	 * SWIG's wrapping is such that incrementing argvi points
	 * $result to the next location in perl's argument stack.
	 */

	for (iter = $1; *iter; iter++) {
	    $result = sv_2mortal(newSVpv(*iter, 0));
	    argvi++;
	}
	g_free($1);
    }

}
gchar **list_retention();
gchar **list_no_retention();
gchar **list_new_tapes(char *storage_n, int nb);

char *get_last_reusable_tape_label(const char *l_template, const char *tapepool,
				   const char *storage,
				   int retention_tapes,
				   int retention_days, int retention_recover,
				   int retention_full, int skip);

int volume_is_reusable(const char *label);

/* C functions -- should be called *only* from within this module */

%rename(C_read_tapelist) read_tapelist;
int read_tapelist(char *tapefile);

%rename(C_clear_tapelist) clear_tapelist;
void clear_tapelist(void);

%perlcode %{
package Amanda::Tapelist::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 1600000) {
        return "Failed to read tapelist file '$self->{'tapefile'}'.";
    } elsif ($self->{'code'} == 1600001) {
        return "List of labels";
    } elsif ($self->{'code'} == 1600002) {
        return "Failed to read tapelist file '$self->{'tapelist_filename'}' on line $self->{'linenum'}: $self->{'line'}";
    } elsif ($self->{'code'} == 1600003) {
        return "duplicate label '$self->{'label'} in tapelist file '$self->{'tapelist_filename'}'";
    }
}
%}

