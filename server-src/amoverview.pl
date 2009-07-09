#!@PERL@

# Catch for sh/csh on systems without #! ability.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
	& eval 'exec @PERL@ -S $0 $argv:q'
		if 0;

require 5.001;

use FileHandle;
use Getopt::Long;
use Text::ParseWords;
use Carp;
use POSIX;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin";

sub Usage {
    print STDERR <<END;
Usage: $0 [[-config] CONFIG] [-hostwidth width] [-diskwidth width] [-skipmissed] [-last] [-num0] [-togo0] [-verbose]

This script generates to standard output an overview of the filesystems
dumped over time and the type of dump done on a particular day, such as
a full dump, or an incremental, or if the dump failed.

You may override the default configuration `@DEFAULT_CONFIG@' by using
the -config command line option.  On larger installations, this script
will take a while to run.  In this case, run it with --verbose to see
how far along it is.
END
    exit 1;
}

# Default paths for this installation of Amanda.
my $prefix='@prefix@';
$prefix=$prefix;		# avoid warnings about possible typo
my $exec_prefix="@exec_prefix@";
$exec_prefix=$exec_prefix;	# ditto
my $amlibexecdir="@amlibexecdir@";
my $sbindir="@sbindir@";
 
# The directory where configurations can be found.
my $confdir="@CONFIG_DIR@";

# The default configuration.
my $config="@DEFAULT_CONFIG@";

my $amadmin	= "$sbindir/amadmin";

# overrideable defaults
my $opt_config		= "$config";
my $opt_hostwidth	= 8;
my $opt_diskwidth	= 20;
my $opt_skipmissed	= 0;
my $opt_last		= 0;
my $opt_num0		= 0;
my $opt_togo0		= 0;
my $opt_verbose		= 0;

GetOptions('config=s'		=> \$opt_config,
	   'hostwidth=i'	=> \$opt_hostwidth,
	   'diskwidth=i'	=> \$opt_diskwidth,
	   'skipmissed'		=> \$opt_skipmissed,
	   'last'		=> \$opt_last,
	   'num0'		=> \$opt_num0,
	   'togo0'		=> \$opt_togo0,
	   'verbose'		=> \$opt_verbose)
or Usage();

if($#ARGV == 0) {
  $opt_config = $ARGV[0];
}
elsif($#ARGV > 0) {
  Usage();
}

#untaint user input $ARGV[0]

if ($opt_config =~ /^([\w.-]+)$/) {          # $1 is untainted
   $opt_config = $1;
} else {
    die "filename '$opt_config' has invalid characters.\n";
}


-d "$confdir/$opt_config" or
	die "$0: directory `$confdir/$opt_config' does not exist.\n";

# read disklist
my %disks = ();
$::host = '';
$::disk = '';
$opt_verbose and
    print STDERR "Running $amadmin $opt_config disklist\n";
my $dlfh = new FileHandle "$amadmin $opt_config disklist|" or
    die "$0: error in opening `$amadmin $opt_config disklist' pipe: $!\n";
$/ = "";
while (<$dlfh>) {
    ($host, $disk) = m/    host (.*?):\n.*    disk (.*?):\n.*strategy (STANDARD|NOFULL|NOINC|HANOI|INCRONLY).*ignore NO/ms;
    next unless $host;
    $disks{$host}{$disk}++;
}

$/ = "\n";
$dlfh->close or
    die "$0: error in closing `$amadmin $opt_config disklist|' pipe: $!\n";

# Get backup dates
%::dates = ();
%::level = ();
$::level = '';
my ($date, $tape, $file, $status);
$opt_verbose and
    print STDERR "Running $amadmin $opt_config find\n";
my $fh = new FileHandle "$amadmin $opt_config find|" or
    die "$0: error in opening `$amadmin $opt_config find' pipe: $!\n";
<$fh>;
while (<$fh>) {
    chomp;
    next if /found Amanda directory/;
    next if /skipping cruft directory/;
    next if /skip-incr/;

    ($date, $time, $host, $disk, $level, $tape, $file, $part, $status) = shellwords($_);

    next if $date eq 'date';
    next if $date eq 'Warning:';
    next if $date eq 'Scanning';
    next if $date eq "";

    if($time !~/^\d\d:\d\d:\d\d$/) {
	$status = $part;
	$part = $file;
	$file = $tape;
	$tape = $level;
	$level = $disk;
	$disk = $host;
	$host = $time;
    }

    if ($date =~ /^\d\d\d\d-\d\d-\d\d$/) {
	if(defined $disks{$host}{$disk}) {
	    defined($level{$host}{$disk}{$date}) or
		$level{$host}{$disk}{$date} = '';
	    $level{$host}{$disk}{$date} .= ($status eq 'OK') ? $level : 'E';
	    $dates{$date}++;
	}
    }
    else {
	print "bad date $date in $_\n";
    }
}
$fh->close or
    die "$0: error in closing `$amadmin $opt_config find|' pipe: $!\n";

# Process the status to arrive at a "last" status
if ($opt_last) {
    for $host (sort keys %disks) {
        for $disk (sort keys %{$disks{$host}}) {
	    $level{$host}{$disk}{"0000-LA-ST"} = '';
	    for $date (sort keys %dates) {
	        if ($level{$host}{$disk}{$date} eq "E"
		     && $level{$host}{$disk}{"0000-LA-ST"} =~ /^\d/ ) {
		    $level{$host}{$disk}{"0000-LA-ST"} .= $level{$host}{$disk}{$date};
	        } elsif ($level{$host}{$disk}{$date} eq "") {
		    $level{$host}{$disk}{"0000-LA-ST"} =~ s/E//;
	        } else {
		    $level{$host}{$disk}{"0000-LA-ST"} = $level{$host}{$disk}{$date};
	        }
	    }
        }
    }
}

# Number of level 0 backups
if ($opt_num0) {
    for $host (sort keys %disks) {
        for $disk (sort keys %{$disks{$host}}) {
            $level{$host}{$disk}{'0000-NM-L0'} = 0;
            for $date (sort keys %dates) {
                if ($level{$host}{$disk}{$date} =~ /0/) {
                    $level{$host}{$disk}{'0000-NM-L0'} += 1;
                }
            }
        }
    }
}

# Runs to the last level 0
if ($opt_togo0) {
    for $host (sort keys %disks) {
        for $disk (sort keys %{$disks{$host}}) {
            $level{$host}{$disk}{'0000-TO-GO'} = 0;
            my $togo=0;
            for $date (sort keys %dates) {
                if ($level{$host}{$disk}{$date} =~ /0/) {
                    $level{$host}{$disk}{'0000-TO-GO'} = $togo;
                }
                $togo++;
            }
        }
    }
}

unless ($opt_skipmissed)
# touch all the dates just in case whole days were missed.
{
    my ($start, $finish) = 
	map {
	    my($y,$m,$d) = split /-/, $_;
	    POSIX::mktime(0,0,0,$d,$m-1,$y-1900);
	} (sort keys %dates)[0,-1];

    while ($start < $finish) {
	my @l = localtime $start;
	$dates{sprintf("%d-%02d-%02d", 1900+$l[5], $l[4]+1, $l[3])}++;
	$start += 86400;
    }
}

#Add the "last" entry    
$dates{"0000-LA-ST"}=1 if ($opt_last);

#Add the "Number of Level 0s" entry
$dates{"0000-NM-L0"}=1 if ($opt_num0);

#Add the "Runs to go" entry
$dates{"0000-TO-GO"}=1 if ($opt_togo0);

# make formats

my $top_format = "format TOP =\n\n" .
    sprintf("%-0${opt_hostwidth}s %-0${opt_diskwidth}s ", '', 'date') .
    join(' ', map((split(/-/, $_))[1], sort keys %dates)) . "\n" .
    sprintf("%-0${opt_hostwidth}s %-0${opt_diskwidth}s ", 'host', 'disk') .
    join(' ', map((split(/-/, $_))[2], sort keys %dates)) . "\n" .
    "\n.\n";

my $out_format = "format STDOUT =\n" .
    "@" . "<" x ($opt_hostwidth - 1) . ' ' .
    "@" . "<" x ($opt_diskwidth - 1) . ' ' .
    '@> ' x scalar(keys %dates) . "\n" .
    join(', ', '$host', '$disk', 
	 map("substr(\$level{\$host}{\$disk}{'$_'},-2)", sort keys %dates)) . "\n" .
    ".\n";

eval $top_format;
die $@ if $@;
$^ = 'TOP';
eval $out_format;
die $@ if $@;

for $host (sort keys %disks) {
    for $disk (sort keys %{$disks{$host}}) {
	write;
    }
}
