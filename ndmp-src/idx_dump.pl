#!/usr/bin/perl
#
#

use strict;
use Socket;
use integer;

my $prog = $0;
$prog =~ s/^.*\/(\S+)$/$1/;

my $debug = 0;
my $do_dirs = 0;
my $do_empty_dirs = 0;
my $do_inode = 0;
my $do_fhinfo = 0;


#
# parse switches
#
while ($ARGV[0] =~ /^-\S+/) {
    if ($ARGV[0] eq "-d" || $ARGV[0] eq "-debug") {
	$debug = 1;
    } elsif ($ARGV[0] eq "-dirs") {
	$do_dirs = 1;
    } elsif ($ARGV[0] eq "-empty_dirs") {
	$do_empty_dirs = 1;
    } elsif ($ARGV[0] eq "-i") {
	$do_inode = 1;
    } elsif ($ARGV[0] eq "-fhinfo") {
	$do_fhinfo = 1;
    } else {
	print STDERR "Unknown switch $ARGV[0]\n";
	exit -1;
    }
    shift @ARGV;
}
if (@ARGV != 1) {
    print STDERR "Usage: $prog [-d|-debug] [-dirs] [-i] [-fhinfo] <index file>\n";
    exit 1;
}

#
# parse index file
#
my $idxfile = $ARGV[0];
open(IDXFILE, $idxfile) ||
    die "$prog: can not open '$idxfile': $!";

my $root_node = "";
my %dir_inode;
my %dir_entry_count;
my %inode;
my %first_ino_ref;

sub find_path {
    my ($i) = @_;

    print "find_path called on $i\n" if $debug;

    # check root
    if ($i == $root_node) {
	return "";
    }

    # find the inode of the directory that refers to this inode
    if (!exists $first_ino_ref{$i}) {
	print "find_path failed first_ino_ref on $i\n";
	return "";
    }
    my $dir_i = $first_ino_ref{$i};

    # find the directory entry that refers to this inode
    my $ilist;
    if (exists $dir_inode{$dir_i}) {
	$ilist = $dir_inode{$dir_i};
    } else {
	print "find_path failed dir_inode for $dir_i on $i\n";
	return "";
    }

    # search through the directory for the name entry
    my $inode_entry_name = "";
    foreach my $nam (keys %$ilist) {
	if (($nam eq ".") || ($nam eq "..")) {
	    next;
	}
	if ($ilist->{$nam} == $i) {
	    # found entry
	    $inode_entry_name = $nam;
	    last;
	}
    }
    print "find_path found name '$inode_entry_name' for $i\n" if $debug;

    # get the path for the directory
    if ($dir_i != $i) {
	my $path = &find_path ($dir_i);
	if ($path ne "") {
	    return "$path/$inode_entry_name";
	}
    }
    return $inode_entry_name;
}

# Pass #1: get directory information
while (<IDXFILE>) {
    chomp;
    # emacs sucks with perl
    if (/^#/) {
	next;
    }
    if (/^CM\s+/) {
	next;
    }
    if (/^DE\s+(.+)\s*$/) {
	print "Environment: $1\n" if $debug;
	next;
    }
    if (/^DHr\s+(.+)\s*$/) {
	if ($1 ne "") {
	    $root_node = $1;
	}
	next;
    }
    if (/^DHd\s+([0-9]+)\s+(\S+)\s+UNIX+\s+([0-9]+)\s*$/) {
	# directory entry inode $1 has name $2 pointing to inode $3
	print "$1 => $2 $3\n" if $debug;

	my $ilist;
	if (exists $dir_inode{$1}) {
	    $ilist = $dir_inode{$1};
	} else {
	    $ilist = {};
	}
	$ilist->{$2} = $3;
	$dir_inode{$1} = $ilist;

	# if not "." or ".." then record the first reference to the inode
	if (($2 eq ".") || ($2 eq "..")) {
	    next;
	}

	# count the directory entry
	if (exists $dir_entry_count{$1}) {
	    $dir_entry_count{$1}++;
	} else {
	    $dir_entry_count{$1} = 1;
	}

	if (!exists $first_ino_ref{$3}) {
	    $first_ino_ref{$3} = $1;
	}


	next;
    }

    if (/^DHn\s+([0-9]+)\s+UNIX\s+f([dc])\s+m(\S+)\s+u(\S+)\s+g(\S+)\s+tm(\S+)\s*$/) {
	$inode{$1} = {
	    TYPE => $2,
	    MODE => $3,
	    UID => $4,
	    GID => $5,
	    MTIME => $6,
	    FHINFO => "0",
	};
	next;
    }
    if (/^DHn\s+([0-9]+)\s+UNIX\s+f([dc])\s+m(\S+)\s+u(\S+)\s+g(\S+)\s+tm(\S+)\s+@([-0-9]+)\s*$/) {
	$inode{$1} = {
	    TYPE => $2,
	    MODE => $3,
	    UID => $4,
	    GID => $5,
	    MTIME => $6,
	    FHINFO => $7
	};
	next;
    }
    if (/^DHn\s+([0-9]+)\s+UNIX\s+f(\S+)\s+m(\S+)\s+u(\S+)\s+g(\S+)\s+s(\S+)\s+tm(\S+)\s*$/) {
	$inode{$1} = {
	    TYPE => $2,
	    MODE => $3,
	    UID => $4,
	    GID => $5,
	    SIZE => $6,
	    MTIME => $7,
	    FHINFO => "0"
	};
	next;
    }
    if (/^DHn\s+([0-9]+)\s+UNIX\s+f(\S+)\s+m(\S+)\s+u(\S+)\s+g(\S+)\s+s(\S+)\s+tm(\S+)\s+@([-0-9]+)\s*$/) {
	$inode{$1} = {
	    TYPE => $2,
	    MODE => $3,
	    UID => $4,
	    GID => $5,
	    SIZE => $6,
	    MTIME => $7,
	    FHINFO => $8
	};
	next;
    }
    print "Unknown keyword line: $_\n";
}

print "Root node: '$root_node'\n" if $debug;

# rewind input
seek (IDXFILE, 0, 0);

# Pass 2: get file information including multiple names (hard links) for the
#         same file


my $last_path = "";
my $last_inode = 0;
while (<IDXFILE>) {
    chomp;
    # emacs sucks with perl
    if (/^#/) {
	next;
    }
    if (/^DHd\s+([0-9]+)\s+(\S+)\s+UNIX+\s+([0-9]+)\s*$/) {
	# directory entry inode $1 has name $2 pointing to inode $3
	print "$1 => $2 $3\n" if $debug;

	if (($2 eq ".") || ($2 eq "..")) {
	    next;
	}

	# find the pathname for this directory
	my $path;
	if ($last_inode == $1) {
	    $path = $last_path;
	} else {
	   $last_path = $path = &find_path ($1);
	   $last_inode = $1;
	}


	# see if it is a directory
	if ($inode{$3}{TYPE} eq "d") {
	    if ($do_empty_dirs && !$do_dirs) {
		if (exists $dir_entry_count{$3}) {
		    next;
		}
	    } elsif (!$do_dirs) {
		# ignore directories
		next;
	    }
	}


	# print out path name for this entry
	my $full_path;
	if ($path eq "") {
	    $full_path = "$2";
	} else {
	    $full_path = "$path/$2";
	}

	# translate "=" into %3d".
	$full_path =~ s/=/%3d/g;

	if ($do_inode != 0 || $debug) {
	    if ($do_fhinfo) {
		print "$inode{$3}{FHINFO} " . "$3" . ": $full_path\n";
	    } else {
		print "$3" . ": $full_path\n";
	    }
	} else {
	    if ($do_fhinfo) {
		print "$inode{$3}{FHINFO} " . "$full_path\n";
	    } else {
		print "$full_path\n";
	    }
	}

	next;
    }
}

close(IDXFILE);


#
# all done
#
exit 0;

