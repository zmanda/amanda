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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck::Catalogs;

=head1 NAME

Installcheck::Catalogs - manage catalog info that can be used to test
tools that do not need access to actual vtapes

=head1 SYNOPSIS

  use Installcheck::Catalogs;
  my $cat = Installcheck::Catalogs::load("skipped");
  $cat->install();
  my @tags = $cat->get_tags();

=head1 DESCRIPTION

The C<load> method loads a named set of catalog information from catalog files.

The resulting object just decodes the catalog information into a perl
structure.  To actually write the catalog to disk, use the C<install> method of
the resulting object.

Note that many test catalogs require a configuration to be loaded; this package
does not handle loading configurations.  However, the C<install> method does
take care of erasing the C<logs> subdirectory of the configuration directory as
well as any stray holding-disk files.

A catalog can have multiple, named snippets of text attached, as well.  These
are accessed via the C<get_text($name)> method.

=head2 Database Results

The C<%H>, C<%P>, and C<%D> directives set up a "shadow database" of dumps and
parts that are represented by the catalog.  These are available in two hashes,
one for dumps and one for parts, available from methods C<get_dumps> and
C<get_parts>.  The hashes are keyed by "tags", which are arbitrary strings.
The dumps and parts are built to look like those produced by
L<Amanda::DB::Catalog>; in particular, a dump has keys

  parts (list of parts indexed by partnum)
  dump_timestamp
  hostname
  diskname
  level
  status
  kb
  orig_kb
  write_timestamp
  message
  nparts
  sec

while a part has keys

  dump (points to the parent dump)
  status
  sec
  kb
  orig_kb
  partnum

a part will also have a C<holding_file> key if it is, indeed, a holding
file.  The C<holding_filename($tag)> method will return the filename of a
holding file.

=head2 Catalog Files

Each file in C<installcheck/catalogs> with the suffix C<.cat> represents a
cached catalog.  Since the Amanda catalog consists of many files (curinfo,
trace logs, index, disklist, tapelist, etc.), each catalog acts as a
container for several other named files.  The file is parsed in a line-based
fashion, with the following conventions:

=over 4

=item A line beginning with C<#> is a comment, and is ignored

=item A line beginning with C<%F> begins a new output file, with the rest of
the line (after whitespace) interpreted as a filename relative to the TESTCONF
configuration directory.  Any intervening directories required will be created.

=item A line beginning with C<%T> begins a new text section.  This is simliar
to C<%F>, but instead of a filename, the rest of the line specifies a text
handle.  The text will not be written to the filesystem on C<install>.

=item A line beginning with C<%H> specifies a holding-disk file.  The rest of
the line is a space-separated list:

  %H tag datestamp hostname pathname level status size

A single-chunk holding-disk file of the appropriate size will be created,
filled with garbage, and the corresponding entries will be made in the dump and
part hashes.

=item A line beginning with C<%D> specifies a dump.  The format, all on one line, is:

  %D tag dump_timestamp write_timestamp hostname diskname level status
    message nparts sec kb orig_kb

=item A line beginning with C<%P> specifies a part.  The format, again all on
one line, is:

  %P tag dumptag label filenum partnum status sec kb orig_kb

where C<dumptag> is the tag of the dump of which this is a part.

=item A line beginning with C<%%> is a custom tag, intended for use by scripts
to define their expectations of the logfile.  The results are available from
the C<get_tags> method.

=item A line beginning with C<\> is copied literally into the current output
file, without the leading C<\>.

=item Blank lines are ignored.

=back

=cut

sub load {
    my ($name) = @_;

    return Installcheck::Catalogs::Catalog->new($name);
}

package Installcheck::Catalogs::Catalog;

use warnings;
use strict;

use Installcheck;
use Amanda::Util;
use Amanda::Paths;
use Amanda::Xfer qw( :constants );
use File::Path qw( mkpath rmtree );

my $holdingdir = "$Installcheck::TMP/holding";

sub new {
    my $class = shift;
    my ($name) = @_;

    my $filename = "$srcdir/catalogs/$name.cat";
    die "no catalog file '$filename'" unless -f $filename;

    my $self = bless {
	files => {},
	texts => {},
	tags => [],
	holding_files => {},
	dumps => {},
	parts => {},
    }, $class;

    $self->_parse($filename);

    return $self;
}

sub _parse {
    my $self = shift;
    my ($filename) = @_;
    my $write_timestamp;
    my $fileref;

    open(my $fh, "<", $filename) or die "could not open '$filename'";
    while (<$fh>) {
	## comment or blank
	if (/^#/ or /^$/) {
	    next;

	## new output file
	} elsif (/^(%[TF])\s*(.*)$/) {
	    my $cur_filename = $2;
	    my $kind = ($1 eq '%F')? 'files' : 'texts';
	    die "duplicate file '$cur_filename'"
		if exists $self->{$kind}{$cur_filename};
	    $self->{$kind}{$cur_filename} = '';
	    $fileref = \$self->{$kind}{$cur_filename};

	# holding file
	} elsif (/^%H (\S+) (\S+) (\S+) (\S+) (\d+) (\S+) (\d+)$/) {

	    die "dump tag $1 already exists" if exists $self->{'dumps'}{$1};
	    die "part tag $1 already exists" if exists $self->{'parts'}{$1};

	    my $safe_disk = $4;
	    $safe_disk =~ tr{/}{_};
	    my $hfile = "$holdingdir/$2/$3.$safe_disk";

	    $self->{'holding_files'}->{$1} = [ $hfile, $2, $3, $4, $5, $6, $7 ];

	    my $dump = $self->{'dumps'}{$1} = {
		dump_timestamp => $2,
		hostname => $3,
		diskname => $4,
		level => $5+0,
		status => $6,
		kb => $7,
		orig_kb => 0,
		write_timestamp => '00000000000000',
		message => '',
		nparts => 1,
		sec => 0.0,
	    };
	    my $part = $self->{'parts'}{$1} = {
		holding_file => $hfile,
		dump => $dump,
		status => $dump->{'status'},
		sec => 0.0,
		kb => $dump->{'kb'},
		orig_kb => 0,
		partnum => 1,
	    };
	    $dump->{'parts'} = [ undef, $part ];

	# dump
	} elsif (/^%D (\S+) (\d+) (\d+) (\S+) (\S+) (\d+) (\S+) (\S+) (\d+) (\S+) (\d+) (\d+)/) {
	    die "dump tag $1 already exists" if exists $self->{'dumps'}{$1};
	    my $dump = $self->{'dumps'}{$1} = {
		dump_timestamp => $2,
		write_timestamp => $3,
		hostname => $4,
		diskname => $5,
		level => $6+0,
		status => $7,
		message => $8,
		nparts => $9,
		sec => $10+0.0,
		kb => $11,
		orig_kb => $12,
		parts => [ undef ],
	    };
	    # translate "" to an empty string
	    $dump->{'message'} = '' if $dump->{'message'} eq '""';

	# part
	} elsif (/^%P (\S+) (\S+) (\S+) (\d+) (\d+) (\S+) (\S+) (\d+) (\d+)/) {
	    die "part tag $1 already exists" if exists $self->{'parts'}{$1};
	    die "dump tag $2 does not exist" unless exists $self->{'dumps'}{$2};

	    my $part = $self->{'parts'}{$1} = {
		dump => $self->{dumps}{$2},
		label => $3,
		filenum => $4,
		partnum => $5,
		status => $6,
		sec => $7+0.0,
		kb => $8,
		orig_kb => $9
	    };
	    $self->{'dumps'}->{$2}->{'parts'}->[$5] = $part;

	# processing tag
	} elsif (/^%%\s*(.*)$/) {
	    push @{$self->{'tags'}}, $1;

	# bogus directive
	} elsif (/^%/) {
	    chomp;
	    die "invalid processing instruction '$_'";

	# contents of the file (\-escaped)
	} elsif (/^\\/) {
	    s/^\\//;
	    $$fileref .= $_;

	# contents of the file (copy)
	} else {
	    $$fileref .= $_;
	}
    }
}

sub _make_holding_file {
    my ($filename, $datestamp, $hostname, $diskname, $level, $status, $size) = @_;

    # make the parent dir
    my $dir = $filename;
    $dir =~ s{/[^/]*$}{};
    mkpath($dir);

    # (note that multi-chunk holding files are not used at this point)
    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = $datestamp;
    $hdr->{'dumplevel'} = $level+0;
    $hdr->{'name'} = $hostname;
    $hdr->{'disk'} = $diskname;
    $hdr->{'program'} = "INSTALLCHECK";
    $hdr->{'is_partial'} = ($status ne 'OK');

    open(my $fh, ">", $filename) or die("opening '$filename': $!");
    $fh->syswrite($hdr->to_string(32768,32768));

    # transfer some data to that file
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Pattern->new(1024*$size, "+-+-+-+-"),
	Amanda::Xfer::Dest::Fd->new($fh),
    ]);

    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    close($fh);
}

sub install {
    my $self = shift;

    # first, remove the logdir
    my $logdir = "$Amanda::Paths::CONFIG_DIR/TESTCONF/log";
    rmtree($logdir) if -e $logdir;

    # write the new config files
    for my $filename (keys %{$self->{'files'}}) {
	my $pathname = "$Amanda::Paths::CONFIG_DIR/TESTCONF/$filename";
	my $dirname = $pathname;
	$dirname =~ s{/[^/]+$}{};

	mkpath($dirname) unless -d $dirname;
	Amanda::Util::burp($pathname, $self->{'files'}{$filename});
    }

    # erase holding and create some new holding files
    rmtree($holdingdir);
    for my $hldinfo (values %{$self->{'holding_files'}}) {
	_make_holding_file(@$hldinfo);
    }
}

sub get_tags {
    my $self = shift;
    return @{$self->{'tags'}};
}

sub get_dumps {
    my $self = shift;
    return %{$self->{'dumps'}};
}

sub get_parts {
    my $self = shift;
    return %{$self->{'parts'}};
}

sub get_text {
    my $self = shift;
    my ($name) = @_;

    return $self->{'texts'}->{$name};
}

sub get_file {
    my $self = shift;
    my ($name) = @_;

    return $self->{'files'}->{$name};
}

sub holding_filename {
    my $self = shift;
    my ($tag) = @_;

    my $fn = $self->{'holding_files'}{$tag}[0];
    return $fn;
}

1;
