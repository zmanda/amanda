/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

%module "Amanda::Tapelist"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Tapelist.pod"

%{
#include "tapefile.h"
%}

/*
 * Constants
 */

amglue_add_flag_tag_fns(RetentionType);
amglue_add_constant_short(RETENTION_NO, "retention-no", RetentionType);
amglue_add_constant_short(RETENTION_NO_REUSE, "no-reuse", RetentionType);
amglue_add_constant_short(RETENTION_TAPES, "retention-tapes", RetentionType);
amglue_add_constant_short(RETENTION_DAYS, "retention-days", RetentionType);
amglue_add_constant_short(RETENTION_RECOVER, "retention-recover", RetentionType);
amglue_add_constant_short(RETENTION_FULL, "retention-full", RetentionType);
amglue_add_constant_short(RETENTION_CMD_COPY, "command-copy", RetentionType);
amglue_add_constant_short(RETENTION_CMD_FLUSH, "command-flush", RetentionType);
amglue_add_constant_short(RETENTION_CMD_RESTORE, "command-restore", RetentionType);
amglue_add_constant_short(RETENTION_OTHER_CONFIG, "other-config", RetentionType);
amglue_copy_to_tag(RetentionType, constants)

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
	last_write => $filename . '.last_write',
    };
    bless $self, $class;

    my $message = $self->reload($lock, 1);
    return ($self, $message);
}

sub clear_tapelist {
    my $self = shift;

    # clear the C version
    C_clear_tapelist();

    $self->{'tles'} = [];
    $self->{'tle_hash_label'} = undef;
    $self->{'tle_hash_pool_label'} = undef;
    $self->{'tle_hash_barcode'} = undef;
    unlink($self->{'last_write'});

    return $self;
}

sub reset_tapelist {
    my $self = shift;

    # reset the C version
    C_reset_tapelist();

    $self->{'tles'} = [];
    $self->{'tle_hash_label'} = undef;
    $self->{'tle_hash_pool_label'} = undef;
    $self->{'tle_hash_barcode'} = undef;
    unlink($self->{'last_write'});

    return $self;
}

## methods

sub reload {
    my $self = shift;
    my $lock = shift;
    my $force = shift;

    if ($lock) {
	$self->_take_lock();
    }

    if (!$force) {
	$last_write_pid = readlink($self->{'last_write'});
	if (defined $last_write_pid &&
	    $last_write_pid == $$) {
	    return;
	}
	my @mtime = stat($self->{'filename'});
	my $mtime = $mtime[9];
	if (defined $self->{'mtime_tapelist'} &&
	    $self->{'mtime_tapelist'} > $mtime) {
	    return;
	}
	#$self->{'mtime_tapelist'} = max($mtime-1, min($mtime,time()-1));
	$self->{'mtime_tapelist'} = time()-1;
	$self->{'mtime_tapelist'} = $mtime-1 if $mtime-1 >= $self->{'mtime_tapelist'};
    }

    # let C read the file
    C_read_tapelist($self->{'filename'});

    my $message = $self->_read_tapelist();
    return $message;
}

sub lookup_tapelabel {
    my $self = shift;
    my ($label) = @_;

    return $self->{'tle_hash_label'}{$label};
}

sub lookup_tape_by_pool_label {
    my $self = shift;
    my ($pool, $label) = @_;

    if (defined $pool) {
	return $self->{'tle_hash_pool_label'}{$pool}{$label};
    } else {
	return $self->{'tle_hash_pool_label'}{''}{$label};
    }
}

sub lookup_by_barcode {
    my $self = shift;
    my ($barcode) = @_;

    return $self->{'tle_hash_barcode'}{$barcode};
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

    # no hash becasue only installcheck use it
    for my $tle (@{$self->{'tles'}}) {
	return $tle if ($tle->{'datestamp'} eq $datestamp);
    }

    return undef;
}

sub remove_tapelabel {
    my $self = shift;
    my ($label) = @_;

    delete $self->{'tle_hash_label'}{$label};
    for (my $i = 0; $i < @{$self->{tles}}; $i++) {
	if ($self->{tles}->[$i]->{'label'} eq $label) {
	    my $pool = $self->{tles}->[$i]->{'pool'};
	    if (defined $pool) {
		delete $self->{'tle_hash_pool_label'}{$pool}{$label};
	    } else {
		delete $self->{'tle_hash_pool_label'}{''}{$label};
	    }
	    my $barcode = $self->{tles}->[$i]->{'barcode'};
	    if ($barcode) {
		delete $self->{'tle_hash_barcode'}{$barcode};
	    }
	    splice @{$self->{tles}}, $i, 1;
	    $self->_update_positions();
	    last;
	}
    }
    C_remove_tapelabel($label);
}

sub remove_tape_by_pool_label {
    my $self = shift;
    my ($pool, $label) = @_;

    delete $self->{'tle_hash_label'}{$label};
    if (defined $pool) {
	delete $self->{'tle_hash_pool_label'}{$pool}{$label};
    } else {
	delete $self->{'tle_hash_pool_label'}{''}{$label};
    }
    for (my $i = 0; $i < @{$self->{tles}}; $i++) {
	if ($self->{tles}->[$i]->{'pool'} eq $pool &&
	    $self->{tles}->[$i]->{'label'} eq $label) {
	    my $barcode = $self->{tles}->[$i]->{'barcode'};
	    if ($barcode) {
		delete $self->{'tle_hash_barcode'}{$barcode};
	    }
	    splice @{$self->{tles}}, $i, 1;
	    $self->_update_positions();
	    last;
	}
    }
    C_remove_tapelabel($label);
}

sub add_tapelabel {
    my $self = shift;
    my ($datestamp, $label, $comment, $reuse, $meta, $barcode, $blocksize,
	$pool, $storage, $config) = @_;
    $reuse = 1 if !defined $reuse;
    $reuse = 0 if $reuse eq '';
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
    $self->{'tle_hash_label'}{$label} = $tle;
    if (defined $pool) {
	$self->{'tle_hash_pool_label'}{$pool}{$label} = $tle;
    } else {
	$self->{'tle_hash_pool_label'}{''}{$label} = $tle;
    }
    if ($barcode) {
	$self->{'tle_hash_barcode'}{$barcode} = $tle;
    }
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
    C_add_tapelabel($datestamp, $label, $comment, $reuse, $meta, $barcode,
		    (defined $blocksize)? 0+$blocksize : 0,
		    $pool, $storage, $config);
}

sub write {
    my $self = shift;
    my ($filename) = @_;
    my $result = TRUE;
    $filename = $self->{'filename'} if !defined $filename;

    if (!exists $self->{'fl'}) {
	die("Tapelist::write no fl");
    }
    if (!$self->{'fl'}->locked()) {
	die("Tapelist::write not locked");
    }

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

    unlink($self->{'last_write'});
    unless (move($new_tapelist_file, $filename)) {
	die ("failed to rename '$new_tapelist_file' to '$filename': $!");
    }
    symlink ("$$", $self->{'last_write'});

    # re-read from the C side to synchronize
    # C side should already be synchronized
    #C_read_tapelist($filename);

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
    $self->{'tle_hash_label'} = undef;
    $self->{'tle_hash_pool_label'} = undef;
    $self->{'tle_hash_barcode'} = undef;
    my $linenum = 0;
    my $fh;
    if (!open($fh, "<", $self->{'filename'})) {
	$self->{'tles'} = [];
	return undef;
    }

    while (my $line = <$fh>) {
	$linenum++;
	my ($datestamp, $label, $reuse, $barcode, $meta, $blocksize, $pool, $storage, $config, $comment)
	    = $line =~ m/^([0-9]+)\s*([^\s]*)\s*(?:(reuse|no-reuse))?\s*(?:BARCODE:([^\s]*))?\s*(?:META:([^\s]*))?\s*(?:BLOCKSIZE:([^\s]*))?\s*(?:POOL:([^\s]*))?\s*(?:STORAGE:([^\s]*))?\s*(?:CONFIG:([^\s]*))?\s*(?:\#(.*))?$/mx;
	if (!defined $datestamp) {
	    return Amanda::Tapelist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code    => 1600002,
			severity => $Amanda::Message::ERROR,
			tapelist_filename => $self->{'filename'},
			linenum => $linenum,
			line    => $line);
	}
	my $tle = {
	    'datestamp' => $datestamp,
	    'label'     => $label,
	    'reuse'     => (!defined $reuse || $reuse eq 'reuse')?1:0,
	    'barcode'   => $barcode,
	    'meta'      => $meta,
	    'blocksize' => $blocksize,
	    'pool'      => $pool,
	    'storage'   => $storage,
	    'config'    => $config,
	    'comment'   => $comment,
	};
	$self->{'tle_hash_label'}{$label} = $tle;
	if (defined $pool) {
	    $self->{'tle_hash_pool_label'}{$pool}{$label} = $tle;
	} else {
	    $self->{'tle_hash_pool_label'}{''}{$label} = $tle;
	}
	if ($barcode) {
	    $self->{'tle_hash_barcode'}{$barcode} = $tle;
	}
	push @tles, $tle;
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
			code    => 1600003,
			severity => $Amanda::Message::WARNING,
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

sub get_retention_name {
    my $self = shift;
    my $retention_type = shift;

    if ($retention_type == $RETENTION_NO) {
	return "retention-no";
    } elsif ($retention_type == $RETENTION_NO_REUSE) {
	return "no-reuse";
    } elsif ($retention_type == $RETENTION_TAPES) {
	return "retention-tapes";
    } elsif ($retention_type == $RETENTION_DAYS) {
	return "retention-days";
    } elsif ($retention_type == $RETENTION_RECOVER) {
	return "retention-recover";
    } elsif ($retention_type == $RETENTION_FULL) {
	return "retention-full";
    } elsif ($retention_type == $RETENTION_CMD_COPY) {
	return "command-copy";
    } elsif ($retention_type == $RETENTION_CMD_FLUSH) {
	return "command-hlush";
    } elsif ($retention_type == $RETENTION_CMD_RESTORE) {
	return "command-restore";
    } elsif ($retention_type == $RETENTION_OTHER_CONFIG) {
	return "other config";
    } else {
	return "unknown retention $retention_type";
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

RetentionType get_retention_type(char *pool, char *label);

/* C functions -- should be called *only* from within this module */

%rename(C_read_tapelist) read_tapelist;
void read_tapelist(char *tapefile);

%rename(C_add_tapelabel) add_tapelabel;
tape_t *add_tapelabel(const char *datestamp,
                      const char *label,
                      const char *comment,
                      gboolean    reuse,
                      const char *meta,
                      const char *barcode,
                      guint64     blocksize,
                      const char *pool,
                      const char *storage,
                      const char *config);

%rename(C_remove_tapelabel) remove_tapelabel;
void remove_tapelabel(char *label);

%rename(C_clear_tapelist) clear_tapelist;
void clear_tapelist(void);

%rename(C_reset_tapelist) reset_tapelist;
void reset_tapelist(void);

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

