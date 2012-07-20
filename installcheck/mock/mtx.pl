#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use strict;
use Data::Dumper;
use File::Path;

# this script is always run as path/to/script -f <statefile> <commands>, and
# mutates its statefile while giving expected output to the caller.

# the statefile is input via "eval", and re-written via Data::Dumper.  It is a
# hashref with, at a minimum, 'config'.  This, in turn, is a hashref with keys
#  - 'num_drives' -- number of drives
#  - 'first_drive' -- first data transfer element number
#  - 'num_slots' -- number of data storage slots
#  - 'first_slot' -- first data storage element number
#  - 'num_ie' -- number of import/export slots
#  - 'first_ie' -- first i/e slot number
#  - 'barcodes' -- does the changer have a barcode reader
#  - 'track_orig' -- does the changer track orig_slot? (-1 = "guess" like IBM 3573-TL)
#  - 'loaded_slots' -- hash: { slot : barcode }
#  - 'vtape_root' -- root directory for vfs devices

# the 'state' key is for internal use only, and has keys:
#  - 'slots' -- hash: { slot => barcode }
#  - 'drives' -- hash: { slot => [ barcode, orig_slot ] }
#	    (if orig_slot is -1, prints "Unkown")

# if 'vtape_root' is specified, it should be an empty directory in which this
# script will create a 'driveN' subdirectory for each drive and a 'slotN'
# subdirectory for each loaded slot.  All loaded vtapes will be "blank".

my $STATE;
my $CONFIG;
my $S;

my $statefile = $ENV{'CHANGER'};
if ($ARGV[0] eq '-f') {
    $statefile = $ARGV[1];
    shift @ARGV;
    shift @ARGV;
}

sub load_statefile {
    die("'$statefile' doesn't exist") unless (-f $statefile);

    open(my $fh, "<", $statefile);
    my $state = do { local $/; <$fh> };
    eval $state;
    die $@ if $@;
    close $fh;

    die("no state") unless defined($STATE);

    die("no config") unless defined($STATE->{'config'});
    $CONFIG = $STATE->{'config'};

    if (!defined($STATE->{'state'})) {
	$S = $STATE->{'state'} = {};
	$S->{'slots'} = { %{$CONFIG->{'loaded_slots'}} };
	$S->{'drives'} = {};
	setup_vtape_root($CONFIG->{'vtape_root'}) if $CONFIG->{'vtape_root'};
    } else {
	$S = $STATE->{'state'};
    }

    # make sure some things are zero if they're not defined
    for my $k (qw(num_drives num_slots num_ie first_drive first_slot first_ie)) {
	$CONFIG->{$k} = 0 unless defined $CONFIG->{$k};
    }
}

sub write_statefile {
    open(my $fh, ">", $statefile);
    print $fh (Data::Dumper->Dump([$STATE], ["STATE"]));
    close($fh);
}

sub setup_vtape_root {
    my ($vtape_root) = @_;

    # just mkdir slotN/data for each *loaded* slot; these become the "volumes"
    # that we subsequently shuffle around
    for my $slot (keys %{$CONFIG->{'loaded_slots'}}) {
	mkpath("$vtape_root/slot$slot/data");
    }
}

sub lowest_unoccupied_slot {
    my @except = @_;

    for (my $i = 0; $i < $CONFIG->{'num_slots'}; $i++) {
	my $sl = $i + $CONFIG->{'first_slot'};
	if (!defined $S->{'slots'}->{$sl}) {
	    return $sl
		unless grep { "$_" eq "$sl" } @except;
	}
    }

    return undef;
}

sub inquiry {
    # some random data
    print <<EOF
Product Type: Medium Changer
Vendor ID: 'COMPAQ  '
Product ID: 'SSL2000 Series  '
Revision: '0416'
Attached Changer: No
EOF
}

sub status {
    printf "  Storage Changer $statefile:%s Drives, %s Slots ( %s Import/Export )\n",
	$CONFIG->{'num_drives'},
	$CONFIG->{'num_slots'} + $CONFIG->{'num_ie'},
	$CONFIG->{'num_ie'};

    # this is more complicated than you'd think!

    my @made_up_orig_slots;
    for (my $i = 0; $i < $CONFIG->{'num_drives'}; $i++) {
	my $sl = $i + $CONFIG->{'first_drive'};
	my $contents = $S->{'drives'}->{$sl};
	if (defined $contents) {
	    my ($barcode, $orig_slot) = @$contents;
	    $barcode = ($CONFIG->{'barcodes'})? ":VolumeTag=$barcode" : "";
	    # if keeping track of orig_slot ...
	    if ($CONFIG->{'track_orig'}) {
		# implement "guessing"
		if ($CONFIG->{'track_orig'} == -1) {
		    $orig_slot = lowest_unoccupied_slot(@made_up_orig_slots);
		    if (defined $orig_slot) {
			push @made_up_orig_slots, $orig_slot;
		    }
		}

		if (!defined $orig_slot) {
		    $orig_slot = "";
		} elsif ($orig_slot eq -1) {
		    $orig_slot = "(Unknown Storage Element Loaded)";
		} else {
		    $orig_slot = "(Storage Element $orig_slot Loaded)";
		}
	    } else {
		$orig_slot = "";
	    }
	    my $sp = ($barcode or $orig_slot)? " " : "";
	    $contents = "Full$sp$orig_slot$barcode";
	} else {
	    $contents = "Empty";
	}
	print "Data Transfer Element $sl:$contents\n",
    }

    # determine range of slots to print info about
    my $start_sl = $CONFIG->{'first_slot'};
    $start_sl = $CONFIG->{'first_ie'}
	if ($CONFIG->{'num_ie'} and $CONFIG->{'first_ie'} < $start_sl);

    my $stop_sl = $CONFIG->{'first_slot'} + $CONFIG->{'num_slots'};
    $stop_sl = $CONFIG->{'first_ie'} + $CONFIG->{'num_ie'}
	if ($CONFIG->{'first_ie'} + $CONFIG->{'num_ie'} > $stop_sl);

    # print the i/e and storage slots in the right order
    for (my $sl = $start_sl; $sl < $stop_sl; $sl++) {
	my $barcode = $S->{'slots'}->{$sl};
	my $contents = defined($barcode)? "Full" : "Empty";
	if (defined $barcode and $CONFIG->{'barcodes'}) {
	    $contents .= " :VolumeTag=$barcode";
	}
	my $ie = "";
	if ($sl >= $CONFIG->{'first_ie'} and $sl - $CONFIG->{'first_ie'} < $CONFIG->{'num_ie'}) {
	    $ie = " IMPORT/EXPORT";
	}
	print "      Storage Element $sl$ie:$contents\n",
    }
}

sub load {
    my ($src, $dst) = @_;

    # check for a full drive
    if (defined $S->{'drives'}->{$dst}) {
	my ($barcode, $orig_slot) = @{$S->{'drives'}->{$dst}};
	print STDERR "Drive $dst Full";
	if (defined $orig_slot and $CONFIG->{'track_orig'}) {
	    if ($CONFIG->{'track_orig'} == -1) {
		$orig_slot = lowest_unoccupied_slot();
	    }
	    print STDERR " (Storage Element $orig_slot Loaded)";
	}
	print STDERR "\n";
	exit 1;
    }

    # check for an empty slot
    if (!defined $S->{'slots'}->{$src}) {
	print STDERR "source Element Address $src is Empty\n";
	exit 1;
    }

    # ok, good to go
    $S->{'drives'}->{$dst} = [ $S->{'slots'}->{$src}, $src ];
    $S->{'slots'}->{$src} = undef;

    if (my $vr = $CONFIG->{'vtape_root'}) {
	rename("$vr/slot$src", "$vr/drive$dst") or die("renaming slot to drive: $!");
    }
}

sub unload {
    my ($dst, $src) = @_;

    # check for a full slot
    if (defined $S->{'slots'}->{$dst}) {
	print STDERR "Storage Element $dst is Already Full\n";
	exit 1;
    }

    # check for an empty drive
    if (!defined $S->{'drives'}->{$src}) {
	# this is the Linux mtx's output...
	print STDERR "Unloading Data Transfer Element into Storage Element $dst..." .
		"source Element Address 225 is Empty\n";
	exit 1;
    }


    # ok, good to go
    $S->{'slots'}->{$dst} = $S->{'drives'}->{$src}->[0];
    $S->{'drives'}->{$src} = undef;

    if (my $vr = $CONFIG->{'vtape_root'}) {
	rename("$vr/drive$src", "$vr/slot$dst") or die("renaming drive to slot: $!");
    }
}

sub transfer {
    my ($src, $dst) = @_;

    # check for an empty slot
    if (!defined $S->{'slots'}->{$src}) {
	print STDERR "source Element Address $src is Empty\n";
	exit 1;
    }

    # check for a full slot
    if (defined $S->{'slots'}->{$dst}) {
	print STDERR "destination Element Address $dst is Already Full\n";
	exit 1;
    }

    # ok, good to go
    $S->{'slots'}->{$dst} = $S->{'slots'}->{$src};
    $S->{'slots'}->{$src} = undef;

    if (my $vr = $CONFIG->{'vtape_root'}) {
	rename("$vr/slot$src", "$vr/slot$dst") or die("renaming slot to slot: $!");
    }
}

load_statefile();
my $op = $ARGV[0];

# override the config when given 'nobarcode'
if ($op eq 'nobarcode') {
    $CONFIG->{'barcodes'} = 0;
    shift @ARGV;
    $op = $ARGV[0];
}

if ($op eq 'inquiry') {
    inquiry();
} elsif ($op eq 'status') {
    status();
} elsif ($op eq 'load') {
    load($ARGV[1], $ARGV[2]);
} elsif ($op eq 'unload') {
    unload($ARGV[1], $ARGV[2]);
} elsif ($op eq 'transfer') {
    transfer($ARGV[1], $ARGV[2]);
} else {
    if (defined $op) {
	die "Unknown operation: $op";
    } else {
	die "No operation given";
    }
}
write_statefile();
