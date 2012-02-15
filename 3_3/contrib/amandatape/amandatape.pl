#! /usr/bin/perl

# amandatape -- a utility to print amanda tape labels for DAT and CD.
#
# 2005-02-15 Josef Wolf  (jw@raven.inka.de)
#
# Portions of this program which I authored may be used for any purpose
# so long as this notice is left intact.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.


# I wrote this program because I was dissatisfied with the original label
# printing program that comes with the amanda distribution.  I wanted to see
# from one glance on the newest tape which tapes in which order I need to
# recover a specific DLE.
#
# This program prints tapelabels for the amanda backup system.  The output
# can be in plain ASCII or in postscript.  The postscript output is formatted
# so that it can be folded to fit into a DAT case or into a CD jewel case.
#
# An example ASCII output (somewhat stripped to make it fit into 80 columns)
# is attached below.  Here is an explanation of the example output:
#
# The columns in the output have following meanings:
#
# date:  This name seems to be intuitive, but unfortunately, it is somewhat
#        misleading.  Actually, this is the name of the logfile that provided
#        the corresponding information.
# label: The label of the tape.
# fn:    File number on the tape.
# fm:    Filemark
# Osize: Original (that is, uncompressed) size of the dump(s).
# Dsize: Size of the dump(s).  This is usually pretty close to Tsize so it
#        is of very little interest.
# Tsize: The size of dump(s) on the tape.
# Dtime: Dumper time.
# Ttime: Taper Time.
# Dspd:  Dumper speed.
# Tspd:  Taper speed.
# DLE:   Disk list entry.
# lv:    Dump-level.
# dpl:   "Dumps per level".  This is a list of dump levels (starting with
#        level 0).
# error: An error message.
#
# The output is split into four sections:
#
# The first section (if present) lists errors.  In the example below we can see
# that there were two taper errors and a warning that a DLE must be flushed to
# tape.
#
# The second section contains only one line with four fields:
# - date
# - tape label
# - total amount of data on that tape.
# - number of DLEs on that tape and total number of DLEs.
#
# The third section lists all the tapes that are needed to recover all
# DLEs.  In the example below we can see that three level0, four level1
# and one level3 from tape VOL01 are needed to do a full restore of all
# DLEs.  In addition, three level0, one level1 and one level2 from VOL09
# are still needed.  Everything else on VOL09 is obsoleted by the dumps
# on VOL01.  VOL08 contains two level0 and one leve1 that are not obsoleted
# by newer tapes.  Finally, VOL07 contain one level0 dump that is not
# obsoleted by newer tapes.
# Tapes that contain only obsoleted data are not mentioned at all unless you
# supply the -t command line option.
#
# The fourth section is the main section and is itself split into multiple
# sections, one for each DLE.  In this section we can see which tapes we
# need to recover a specific DLE.  For example, we can see that in order
# to recover raven:/u4 we need file 1 from VOL07, file 6 from VOL08,
# file 7 from VOL09 and file 1 from VOL01, in this order.
#
# The Ordering of the sections can be different depending on the choosen
# output format.
#
# Due to lack of space, there is a special handling when output is formatted
# for DAT: The fourth section is printed in such a way that non-important data
# (everything on the left from the Osize column) is cut off from the label.
#
# Here comes the example output:
#
#         date Tsize  label fm lv error
# 2004-02-20.0 1499M  VOL06  8  ? writing file: No space left on device
# 2004-02-23.0 1499M  VOL01  9  ? writing file: No space left on device
# 2004-02-23.0  670M    ???  ?  0 raven:/m/u1 not on tape yet
# 
# 2004-02-23.0   VOL01   Total:834M   DLEs:8/9
# 
#         date  label Osize Tsize    Ttime dpl
# 2004-02-23.0  VOL01 1616M  834M  0:01:06 3/4/0/1
# 2004-02-22.0  VOL09 2885M 1164M  0:01:12 3/1/1
# 2004-02-21.0  VOL08 3427M 1127M  0:01:12 2/1
# 2004-02-20.1  VOL07 2077M 1376M  0:01:24 1
# 
#  Dspd  Tspd   Dtime Osize Tsize   Ttime dumpdate    label:fn  lv DLE
#  803k   11M 0:09:13 1163M  434M 0:00:37 2004-02-21  VOL08:008  0 raven:/
#  461k  105M 0:00:06   15M 2963k 0:00:00 2004-02-23  VOL01:004  1 raven:/
# 
# 2515k   40M 0:00:01 4520k 3487k 0:00:00 2004-02-22  VOL09:003  0 raven:/boot
#    0k 1647k 0:00:00   10k    1k 0:00:00 2004-02-23  VOL01:003  1 raven:/boot
# 
#  971k   19M 0:11:46 1757M  670M 0:00:34 2004-02-21  VOL08:009  0 raven:/u1
#  190k   25M 0:00:00  290k   22k 0:00:00 2004-02-22  VOL09:002  1 raven:/u1
# 
#    0k 1237k 0:00:00   10k    1k 0:00:00 2004-02-23  VOL01:002  0 raven:/u2
# 
# 2913k   13M 0:03:56  884M  672M 0:00:49 2004-02-22  VOL09:008  0 raven:/u3
#  925k   13M 0:00:40   94M   36M 0:00:02 2004-02-23  VOL01:006  1 raven:/u3
# 
# 3254k   16M 0:07:13 2077M 1376M 0:01:24 2004-02-20  VOL07:001  0 raven:/u4
#  314k   42M 0:01:14  505M   22M 0:00:00 2004-02-21  VOL08:006  1 raven:/u4
#  373k   47M 0:00:50  459M   18M 0:00:00 2004-02-22  VOL09:007  2 raven:/u4
#  222k   54M 0:00:00  830k  113k 0:00:00 2004-02-23  VOL01:001  3 raven:/u4
# 
#  593k   21M 0:13:29 1536M  469M 0:00:22 2004-02-22  VOL09:009  0 raven:/u5
#  270k  113M 0:00:14   39M 3885k 0:00:00 2004-02-23  VOL01:005  1 raven:/u5
# 
# 1003k   13M 0:06:04  863M  356M 0:00:26 2004-02-23  VOL01:008  0 raven:/usr
# 
# 1960k   11M 0:03:46  602M  434M 0:00:36 2004-02-23  VOL01:007  0 raven:/var

# bugs:
# - Parses amanda's log files instead of reading its database.
# - Depends on assumption that only one amdump per day is run.
# - Output formats hardcoded.

# todo:
# - remove some kludges in the code.

use strict;
use warnings;
use Data::Dumper;
use PostScript::Simple;
use Getopt::Std; $Getopt::Std::STANDARD_HELP_VERSION=1;

my $version=0.4.1;

sub VERSION_MESSAGE {
    my ($fh) = @_;
    print $fh "$0 Version $version ", '($Rev$)', "\n";
}
sub help_message {
    return "Usage: $0 [options] <conf> [DLE ...]
 -f <format>: Choose output format.  Existing formats are: Text, DAT, DDS, CD,
              DVD.  The formats DDS and DVD are synonyms for DAT/CD.  'Text'
              is the default format.  All formats except 'Text' are postscript
 -p <paper>:  Select A4 or Letter as paper type.  A4 is default.
 -l <label>:  Output information for tape named <label>.  Used to print labels
              other than 'current'.  This comes handy when runtapes>1.
 -d <date>:   Take only logfiles older than <date> into account.  <date> must
              be given in the format yyyymmdd.  This is useful to find out what
              is needed to recover contents from a specific date.
 -o <file>:   Write output into <file> instead of stdout.
 -i <cnt>:    Ignore <cnt> newest logfiles.  Used for debugging.
 -s:          Omit dumper statistics.  Useful to fit output into 80 columns.
 -t:          Output all dumps available on tapes instead of only the latest
              for every dumplevel.
 <conf>:      The amanda configuration.  If configuration <conf> does not
              exist, <conf> is interpreted as a directory where the logfiles
              are expected to be found.
 [DLE ...]:   If DLEs are specified, output will be limited to the named DLEs.
              DLEs are specified as perl-regexes like 'host.do.main:/usr/local'
              This is useful to find out what is needed to recover a specific
              host or a specific DLE.
";
}
sub HELP_MESSAGE {
    my ($fh) = @_;
    print $fh &help_message;
}
sub usage {
    print STDERR &help_message;
    exit 1;
}

my %opt; exit 1 unless getopts("stp:o:f:i:l:d:", \%opt);
my $paper     = $opt{p};
my $output    = $opt{o};
my $format    = $opt{f};
my $ignore    = $opt{i};
my $lignore   = $opt{l};
my $dignore   = $opt{d};
my $dumpstats = ! $opt{s};
my $verbose   = exists $opt{t};
my $config    = shift;
my @DLE       = @ARGV;

&usage unless defined $config;
$output="-"    unless defined $output;
$paper = "a4"  unless defined $paper;
$format="text" unless defined $format;
$paper = lc $paper;
$format= lc $format;
$format="dat"  if $format eq "dds";

my %papertype=(a4=>{yoff=>29}, letter=>{yoff=>27});
die "$0: unknown paper type $paper" unless exists $papertype{$paper};

if ($output ne "-" && $format eq "text") {
    open (STDOUT, ">$output") or die "$output: $!";
}

my $LOGDIR = `amgetconf $config logdir`;
chomp $LOGDIR if defined $LOGDIR;
$LOGDIR = $config unless defined $LOGDIR && -d $LOGDIR;

my %dle;       # All the DLEs we have seen
my $curlabel;  # the label of the latest (i.e. current) tape
my %dumper;    # info about dumped DLEs
my %taper;     # info about tapings of DLEs
my %tape;      # info about tape contents
my %needed;    # still needed taping levels from given logfile

my $dlecnt=0;  # number of DLE's we found
my $cdlecnt=0; # number of DLE's on current tape

my $xoff=3;                        # offset of x-coordinate in postscript
my $yoff=$papertype{$paper}{yoff}; # offset of y-coordinate in postscript
my $width=7.3;                     # width of boxes in postscript output.
my $tapedest="top";                # Destination of the tape summaries

# Default formatting for DAT
#
my %out=(
	 top=>{
	     ys=> 0.13, # distance of lines
	     x => 0.25, # x offset
	     y =>-0.35, # y offset
	     fs=>4,     # font size
	     la=>5.5,   # length of area
	     st=>0.1,   # shrink stepping
	     V =>[],    # array of output sections
	 },
	 bot=>{ys=> 0.13,x=>-2.25,y=>-1.5,fs=>4,la=>  5.5,st=>0.1,V=>[]},
	 err=>{ys=> 0.2 ,x=> 8,   y=>-0.2,fs=>6,la=>120,  st=>0.1,V=>[]},
	 );

# change formatting for CD
#
if ($format eq "cd" || $format eq "dvd") {
    $width = 12;
    $tapedest = "bot";
    $out{"top"}{la}=12;    $out{"bot"}{la}=11;
    $out{"top"}{fs}=5;     $out{"bot"}{fs}=5;
    $out{"top"}{ys}=0.15;  $out{"bot"}{ys}=0.15;
    $out{"err"}{x}=0.25;   $out{"bot"}{x}=0.25;
}

$out{"bot"}{x}=0.25 unless $dumpstats;

# Insert separation into an output field.
#
sub sep {
    my ($which) = @_;
    my $w = $out{$which};
    my $y = $w->{ys}*4/4 + $w->{y};
    $w->{y} = $y;
    push (@{$w->{V}}, $w->{v});
    $w->{v} = [];
}

# Output a line.
#
sub out {
    my ($which, $val) = @_;
    my $w = $out{$which};
    my $y = $w->{ys} + $w->{y};
    if (!exists $w->{v}) {
	$w->{v} = [];
	if ($which eq "err") {
		&sep ("err");
		&out ("err", sprintf("%12s %5s %6s %2s %2s %s",
				     qw/date Tsize label fm lv error/));
		&out;
		return;
	}
    }
    my $v=$w->{v};
    $w->{y} = $y;
    push (@$v, {x=>$w->{x}, y=>$w->{y}, v=>$val, la=>$w->{la}});
}

sub log_bydate ($$) {
    my ($a, $b) = @_;

    my ($ad, $anr) = $a=~m!^.*?/log\.(\d+)\.(\d+)$!;
    my ($bd, $bnr) = $b=~m!^.*?/log\.(\d+)\.(\d+)$!;

    return $ad <=> $bd || $anr <=> $bnr;
}

sub add_dumping {
    my ($host, $filesystem, $logfile, $date, $level, $rest) = @_;
    my $dle="$host:$filesystem";

    return unless &wantdle ($dle);

    $date=~s/(....)(..)(..)/$1-$2-$3/;
    $dle{$dle} = 1;
    $dumper{$dle}{$date} = {
        logfile  => $logfile,
        dumpdate => $date,
        level    => $level,
        taped    => 0,
        %{&hash($rest)}
    };
}

# Determine which logfiles belong to the current dump cycle. This is done by
# searching backwards from newest to oldest logfiles for already seen
# tape-labels.
#
sub collect_logfiles {
    my @logfiles;  # sorted names of logfiles from current dumpcycle
    my %labels;    # which tape labels we already have seen
    my $lastvol=-1;

    my @logs=reverse sort log_bydate (<$LOGDIR/log.[0-9.]*>,
                                      <$LOGDIR/oldlog/log.[0-9.]*>);
    splice @logs, 0, $ignore if defined $ignore;

    FILE: foreach my $logfile (@logs) {
        if (defined $dignore) {
            if (&log_bydate("/$dignore.0", $logfile) >= 0) {
                $dignore = undef;
                splice @logfiles, -$lastvol, $lastvol if $lastvol>0;
                $lastvol = -1;
            }
            next;
	}
	open (IN, $logfile) or die "$logfile: $!";
	foreach my $l (reverse <IN>) {
	    if ($l=~/^START taper\s+.*label\s+(\S+)\s+/) {
		if (defined $lignore && ($lignore eq $1)) {
		    $lignore = undef;
		    splice @logfiles, -$lastvol, $lastvol if $lastvol>0;
		}
		$lastvol=$#logfiles+1;
		last FILE if exists $labels{$1};
		$labels{$1}=1;
	    }
	}
	close (IN);
	if (!defined $lignore && !defined $dignore) {
	    unshift (@logfiles, $logfile);
	}
    }

    if (defined $lignore) {
        print STDERR "Can't find tape '$lignore'.\n";
        exit 1;
    }
    if (defined $dignore) {
        print STDERR "Can't find date '$dignore'.\n";
        exit 1;
    }
    if ($#logfiles<0) {
        print STDERR "Could not find logfiles to parse.\n";
        exit 1;
    }

    return @logfiles;
}

# Parse the logfiles. This time we go from oldest to newest. This pass
# constructs %dumper, %taper, %tape and $curlabel.
# In addition, any errors from taper are remembered.
#
foreach my $logfile (&collect_logfiles) {
    open (IN, $logfile) or die "$logfile: $!";
    my @curchunk;
    my @tapeerr;
    my $driverdate="unknown";
    my $label;
    my $nr=0;
    $logfile =~ s/^.*(\d\d\d\d)(\d\d)(\d\d)\.(\d+)$/$1-$2-$3.$4/;
    while (my $line=<IN>) {
	chomp $line;
	if ($line=~/^START driver date (.*)/) {
	    $driverdate = $1;
	}
        if ($line=~/^(SUCCESS|STRANGE) (dumper|chunker) (.*)/) {
            next if $2 eq "chunker"; # FIXME
            my $date = $driverdate;
            my ($host, $filesystem, $level, $rest) = split (/\s+/, $3, 4);
            if ($1 eq "SUCCESS") {
                ($date, $level, $rest) = ($level, split (/\s+/, $rest, 2));
            }
            &add_dumping ($host, $filesystem, $logfile, $date, $level, $rest);
        }
	if ($line=~/^START taper\s+(.*)/) {
            my $l = $opt{l};
	    my $hash = &hash ($1, (files=>{}));
	    $label = $hash->{"label"};
            if (!defined $l || !defined $curlabel || $curlabel ne $l) {
                $curlabel = $label;
            }
            $nr=0;
	    $tape{$label}{"kb"} = 0;
#	    $tape{$label}{"date"} = $hash->{"datestamp"};
#	    $tape{$label}{"date"} =~ s/^(....)(..)(..)/$1-$2-$3/;
	    $tape{$label}{"date"} = $logfile;
	}
	if ($line=~/^(CHUNK)?SUCCESS taper (.*)/ ||
            $line=~/^(CHUNK) taper (.*)/) {
            my ($host, $filesystem, $date, $chunk, $level, $rest) =
                $2=~/(\S+) (\S+) (\S+) (\S+)? ?(\S+) (\[.*)/;
            my $dle="$host:$filesystem";
            $dle{$dle} = 1;
            $date=~s/(....)(..)(..)/$1-$2-$3/;
            $nr++ if $#curchunk<0 || defined $chunk;
            my $h=&hash($rest, label=>$label, level=>$level, dumpdate=>$date,
                        nr=>$nr, chunk=>$chunk, logfile=>$logfile,
                        dump=>$dumper{$dle}{$date});
            push (@curchunk, $h);
            unless (defined $chunk) {
                $taper{$dle}{$date}=[] unless exists $taper{$dle}{$date};
                push (@{$taper{$dle}{$date}}, [@curchunk]) if &wantdle ($dle);
                @curchunk = ();
            }
            if (&wantdle ($dle)) {
                $tape{$label}{"kb"} += $h->{"kb"};
                $tape{$label}{"count"}{$level}++;
            }
	}
        if ($line=~/^INFO taper tape (.*)/) {
            my ($t, $d2, $kb, $d3, $fm, $rest) = split (/\s+/, $1, 6);
	    if ($rest ne "[OK]") {
                push (@tapeerr,
                      [$tape{$t}{"date"}, &kb($kb), $t, $fm, "?", $rest]);
            }
        }
        if ($line=~/^INFO taper continuing/) {
            if ($tapeerr[$#tapeerr][5] =~ /^writing file: No space/) {
                pop @tapeerr;
            }
        }
    }
    close (IN);
    map { &out ("err", sprintf("%12s %5s %6s %2s %2s %s", @$_)); } @tapeerr;
}

# Create list of tapings ordered by dump date.
#
sub tapinglist {
    my ($dle) = @_;
    my @tl;
    foreach my $date (sort keys %{$taper{$dle}}) {
        foreach my $taping (@{%{$taper{$dle}{$date}}}) {
            next if defined $taping->[$#$taping]{chunk}; # no SUCCESS line

            pop (@$taping) if $#$taping>0;             # CHUNKSUCCESS redundant
            push (@tl, $taping);

            $dumper{$dle}{$taping->[0]{dumpdate}}{taped} = 1;
        }
    }
    return @tl;
}

# Create list of dumps which are not taped yet.
#
sub holdinglist {
    my ($dle) = @_;
    my @hl;
    foreach my $date (sort keys %{$dumper{$dle}}) {
        my $d = $dumper{$dle}{$date};
        next if $d->{taped};

        push (@hl, [{ label    => "-",   nr      => "-",
                      chunk    => "-",   kb      => 0,
                      sec      => 0,     kps     => 0,
                      dump     => $d,    level   => $d->{level},
                      dumpdate => $date, logfile => $d->{logfile},
                 }]);

        &out ("err", sprintf("%12s %5s %6s %2s %2s %s",
                             $d->{logfile}, &kb($d->{kb}), "-", "-",
                             $d->{level}, "$dle not on tape yet"));
    }
    return @hl;
}

# Determine dumps/tapes that are needed in order to make a full restore.
#
sub dumplist {
    my ($dle) = @_;
    my @dumplist;
    my $is_current=0;
    my $lastlevel=10000;

    foreach my $dump (reverse (&tapinglist ($dle), &holdinglist ($dle))) {
        my $lv = $dump->[0]{"level"};
        next if !$verbose && $lv>=$lastlevel;
        if ($lastlevel<10000 && $lv<$lastlevel-1) {
            my $m = sprintf "%d..%d", $lv+1, $lastlevel-1;
            $m = $lv+1 if $lv==$lastlevel-2;
            &out("err",sprintf("%12s %5s %6s %2s %2s %s", $dump->[0]{logfile},
                               "?","?","?","?", "Missing level $m of $dle"));
        }
        $lastlevel=$lv if $lastlevel==10000 || $lv<=$lastlevel-1;
        foreach my $taping (reverse @$dump) {
            my $label = $taping->{"label"};
            my $omit = $taping!=$dump->[$#$dump]; # only last line full info
            $is_current=1 if $label eq $curlabel;

            unshift (@dumplist, [$taping, $omit]);

            $needed{$label} = [] unless exists $needed{$label};

            my $na=$needed{$label};
            my $n = {sec=>0, kb=>0, okb=>0, level=>[]};
            $n = shift @$na if $#$na>=0 && $na->[0]{"label"} eq $label;
            unshift (@$na, $n);

            my $okb = $taping->{"dump"}{"orig-kb"};
            $n->{"label"}   = $label;
            $n->{"logfile"} = $taping->{"logfile"};
            $n->{"sec"}    += $taping->{"sec"};
            $n->{"kb"}     += $taping->{"kb"};
            $n->{"okb"}    += $okb if defined $okb && !$omit;
            $n->{"level"}[$lv]++;
        }
    }

    $dlecnt++;
    $cdlecnt++ if $is_current;

    if ($lastlevel != 0) {
	&out("err",sprintf("%12s %5s %6s %2s %2s %s",
			   "?","?","?","?","?", "Missing level 0 of $dle"));
    }

    return \@dumplist;
}

# Determine the requested dumps.
#
foreach my $dle (sort keys %dle) {
    $dle{$dle} = &dumplist ($dle);
}

# Output the number of still needed tapings for every tape.
#
{
    my $headline = sprintf ("%12s %6s %5s %5s %8s %s",
			    qw/date label Osize Tsize Ttime dpl/);
    my $dir = $format eq "text" || $tapedest eq "bot";
    &out($tapedest, $headline) if $dir==1;
    foreach my $label (reverse sort keys %needed) {
        foreach my $t (@{$needed{$label}}) {
            foreach my $l (@{$t->{"level"}}) {
                $l=0 unless defined $l;
            }
            &out($tapedest, sprintf ("%12s %6s %5s %5s %8s %s",
                                     $t->{"logfile"},
                                     $t->{"label"}, &kb($t->{"okb"}),
                                     &kb($t->{"kb"}), &sec($t->{"sec"}),
                                     join("/", @{$t->{"level"}})));
        }
    }
    &out ($tapedest, $headline) if $dir!=1;
    &sep ($tapedest);
}

# Output the requested dumps.
#
my $dumpformat = $dumpstats ? "%5s %5s %8s %5s    " : "";
$dumpformat .= "%5s %5s %8s %-10s %6s:%-3s %2s %s";
&out("bot", sprintf($dumpformat,
                    $dumpstats ? (qw/Dspd Tspd Dtime Dsize/) : (),
                    qw/Osize Tsize Ttime dumpdate label fn lv DLE/));
foreach my $dle (sort keys %dle) {
    foreach my $d (@{$dle{$dle}}) {
        my ($t, $o) = @$d;
        &out("bot",
             sprintf ($dumpformat,
                      $dumpstats ? (
                                    $o ? '-' : &kb ($t->{"dump"}{"kps"}),
                                    &kb ($t->{"kps"}),
                                    $o ? '-' : &sec($t->{"dump"}{"sec"}),
                                    $o ? '-' : &kb ($t->{"dump"}{"kb"}),
                                    ) : (),
                      $o ? '-' : &kb ($t->{"dump"}{"orig-kb"}),
                      &kb ($t->{"kb"}), &sec($t->{"sec"}),
                      $t->{"dumpdate"}, $t->{"label"}, $t->{"nr"},
                      $t->{"level"}, $dle
                      ));
    }

    &sep("bot");
}

# Start output.
#
my $out;
unless ($format eq "text") {
    $out = new PostScript::Simple(eps=>0, papersize=>$paper, units=>"cm");
    $out->setcolour("black");
    $out->setlinewidth(0.01);
    $out->newpage;
}

# Dispatch the output blocks into appropriate output fields.
#
my ($htop, @topbox) = (0);
if ($tapedest eq "top") {
    ($htop, @topbox) = &shrinking_boxes ($out{top}, @{$out{top}{V}});
}
my ($hbot, @botbox) = &shrinking_boxes ($out{bot}, @{$out{bot}{V}});

# output errors and warnings
#
my $w=$out{"err"};
foreach my $o (@{$w->{v}}) {
    if ($format eq "text") {
	print "$o->{v}\n";
    } else {
	$out->setfont ("Courier-Bold", $w->{fs});
	$out->text ($xoff+$o->{x}, $yoff-$o->{y}, $o->{v});
    }
}
if ($#{$w->{v}} >= 0) {
    if ($format eq "text") {
	print "\n";
    } else {
	if ($w->{x} < 8) {
	    $yoff -= ($w->{ys} * (1.5+$#{$w->{v}}));
	}
    }
}

# Print date, label and total size
#
if ($format eq "text") {
    print "$tape{$curlabel}{'date'}   $curlabel   Total:",
    &kb($tape{$curlabel}{"kb"}), "   DLEs:$cdlecnt/$dlecnt\n\n";
} else {
    $out->box ($xoff, $yoff-$htop, $xoff+$width, $yoff-$htop-1.2);
    $out->setfont ("Helvetica-Bold", 20);
    $out->text($xoff+3.0,$yoff-$htop-0.9, $curlabel);
    $out->setfont ("Helvetica-Bold", 11);
    $out->text($xoff+0.25,$yoff-$htop-0.9, $tape{$curlabel}{"date"});
    $out->setfont ("Courier-Bold", 6);
    $out->text($xoff+0.25,$yoff-$htop-0.52,
	       "Total:" . &kb($tape{$curlabel}{"kb"}) .
	       " DLEs:$cdlecnt/$dlecnt");
}

# Print resulting output boxes.
#
if ($tapedest eq "top") {
    &draw_boxes ($xoff, $yoff, $xoff+$out{top}->{x}, 1,
                 $out{top}->{fs}, @topbox);
}
&draw_boxes ($xoff, $yoff-$htop-1.2, $xoff+$out{bot}->{x}, -1,
	     $out{bot}->{fs}, @botbox);

$out->output($output) unless $format eq "text";

# Dispatch output fields into a set of shrinking boxes that can be folded.
#
sub shrinking_boxes {
    my ($conf, @contents) = @_;
    my ($h, @box) = (0);
    for (my $bh=$conf->{la}; $#contents>=0; $bh-=$conf->{st}) {
	my @l;
	while ($#contents>=0 && ($#l+1+$#{$contents[0]}+2)*0.13<$bh) {
	    push(@l, @{shift @contents}, {v=>""});
	}
	if ($#l<0) {
	    push(@l, splice(@{$contents[0]},0,int($bh/0.13)-2));
	}
	push(@box, {h=>$bh,w=>$width,ys=>0.13,lines=>[@l]});
	$h += $bh;
    }

    return ($h, @box);
}

sub draw_boxes {
    my ($xoff, $yoff, $toff, $dir, $font, @box) = @_;
    my $y;
    foreach my $b (@box) {
	unless ($format eq "text") {
            if ($yoff < $b->{h}) {
                $out->newpage(1);
                $out->setlinewidth(0.01);
                $yoff = $papertype{$paper}{yoff}
            }
	    $out->box($xoff, $yoff, $xoff+$b->{w}, $yoff-$b->{h});
	}
	$yoff-=$b->{h};

        unless ($format eq "text") {
            $out->setfont ("Courier-Bold", $font);
        }

	foreach my $i (0..$#{$b->{lines}}) {
	    if ($dir>0) {
		$y=$yoff+0.05+$i*$b->{ys};
	    } else {
		$y=$yoff-0.26-$i*$b->{ys}+$b->{h};
	    }
	    if ($format eq "text") {
		print "$b->{lines}[$i]{v}\n";
	    } else {
		$out->text ($toff, $y, $b->{lines}[$i]{v});
	    }
	}
    }
}

# construct an associative array from key/value pairs which amanda puts into
# sqare brackets. 
#
sub hash {
    my ($input)=shift;
    $input =~ s/[\[\]]//g;
    $input =~ s/\{.*\}//;
    $input =~ s/\s+$//;
    $input =~ s/\s\(null\)$//;
    my %h = (@_, split (/\s+/, $input));
    return \%h;
}

# translate seconds into h:mm:ss
#
sub sec {
    my ($v)=@_;
    return "?" unless defined $v;
    my $h = int ($v / 3600);
    my $m = int ($v / 60) % 60;
    my $s = $v % 60;
    return sprintf "%2d:%02d:%02d", $h, $m, $s;
}

# translate kbytes into a unit with 2..4 digits
#
sub kb {
    my ($v)=@_;
    return "?" unless defined $v;
    my $app = "k";
    if ($v>9999) { $app="M"; $v /= 1024; }
    if ($v>9999) { $app="G"; $v /= 1024; }
    if ($v>9999) { $app="T"; $v /= 1024; }
    return int ($v) . $app;
}

sub wantdle {
    my ($dle) = @_;
    return 1 if $#DLE<0;
    return scalar grep { $dle=~/$_/ } @DLE;
}

