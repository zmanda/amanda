#!@PERL@
#

# Run perl.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
	 & eval 'exec @PERL@ -S $0 $argv:q'
		if 0;

use warnings;
use lib '@amperldir@';
use Time::Local;
use Text::ParseWords;
use Amanda::Util;
use Amanda::Process;
use Getopt::Long;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/bin:/usr/bin:/usr/sbin:/sbin";       # force known path

$confdir="@CONFIG_DIR@";
$prefix='@prefix@';
$prefix=$prefix;		# avoid warnings about possible typo
$exec_prefix="@exec_prefix@";
$exec_prefix=$exec_prefix;	# ditto
$sbindir="@sbindir@";

my $Amanda_process = Amanda::Process->new(0);
$Amanda_process->load_ps_table();

#$STATUS_STRANGE =  2;
$STATUS_FAILED  =  4;
$STATUS_MISSING =  8;
$STATUS_TAPE    = 16;
$exit_status    =  0;

my $opt_summary;
my $opt_stats;
my $opt_dumping;
my $opt_waitdumping;
my $opt_waittaper;
my $opt_dumpingtape;
my $opt_writingtape;
my $opt_finished;
my $opt_failed;
my $opt_estimate;
my $opt_gestimate;
my $opt_date;
my $opt_config;
my $opt_file;
my $opt_locale_independent_date_format;

sub usage() {
	print "amstatus [--file amdump_file]\n";
	print "         [--summary] [--dumping] [--waitdumping] [--waittaper]\n";
	print "         [--dumpingtape] [--writingtape] [--finished] [--failed]\n";
	print "         [--estimate] [--gestimate] [--stats] [--date]\n";
	print "         [--locale-independent-date-format]\n";
	print "         [--config] <config>\n";
	exit 0;
}

Getopt::Long::Configure(qw{ bundling });
GetOptions(
    'summary'                        => \$opt_summary,
    'stats|statistics'               => \$opt_stats,
    'dumping|d'                      => \$opt_dumping,
    'waitdumping|wdumping'           => \$opt_waitdumping,
    'waittaper|wtaper'               => \$opt_waittaper,
    'dumpingtape|dtape'              => \$opt_dumpingtape,
    'writingtape|wtape'              => \$opt_writingtape,
    'finished'                       => \$opt_finished,
    'failed|error'                   => \$opt_failed,
    'estimate'                       => \$opt_estimate,
    'gestimate|gettingestimate'      => \$opt_gestimate,
    'date'                           => \$opt_date,
    'config|c:s'                     => \$opt_config,
    'file:s'                         => \$opt_file,
    'locale-independent-date-format' => \$opt_locale_independent_date_format,
    ) or usage();


if( defined $opt_config ) {
	$conf = $opt_config;
}
else {
	if($#ARGV == 0 ) {
		$conf=$ARGV[0];
	}
	else {
		&usage();
	}
}

#untaint user input $ARGV[0]

if ($conf =~ /^([\w.-]+)$/) {          # $1 is untainted
   $conf = $1;
} else {
    die "filename '$conf' has invalid characters.\n";
}

if ( ! -e "$confdir/$conf" ) {
    print "Configuration directory '$confdir/$conf' doesn't exist\n";
    exit 1;
}
if ( ! -d "$confdir/$conf" ) {
    print "Configuration directory '$confdir/$conf' is not a directory\n";
    exit 1;
 }


$pwd = `pwd`;
chomp $pwd;
chdir "$confdir/$conf";

$logdir=`$sbindir/amgetconf logdir`;
exit 1 if $? != 0;
chomp $logdir;
$errfile="$logdir/amdump";

$nb_options = defined( $opt_summary ) +
				  defined( $opt_stats ) +
				  defined( $opt_dumping ) +
				  defined( $opt_waitdumping ) +
				  defined( $opt_waittaper ) +
				  defined( $opt_dumpingtape ) +
				  defined( $opt_writingtape ) +
				  defined( $opt_finished ) +
				  defined( $opt_estimate ) +
				  defined( $opt_gestimate ) +
				  defined( $opt_failed );

if($nb_options == 0 ) {
	$opt_summary     = 1;
	$opt_stats       = 1; 
	$opt_dumping     = 1;
	$opt_waitdumping = 1;
	$opt_waittaper   = 1;
	$opt_dumpingtape = 1;
	$opt_writingtape = 1;
	$opt_finished    = 1;
	$opt_failed      = 1;
	$opt_gestimate   = 1;
	$opt_estimate    = 1;
}

$unit=`$sbindir/amgetconf displayunit`;
chomp($unit);
$unit =~ tr/A-Z/a-z/;
$unitdivisor=1;
if($unit eq 'k') {
  $unitdivisor = 1;
}
elsif($unit eq 'm') {
  $unitdivisor = 1024;
}
elsif($unit eq 'g') {
  $unitdivisor = 1024*1024;
}
elsif($unit eq 't') {
  $unitdivisor = 1024*1024*1024;
}
else {
  $unit = 'k';
  $unitdivisor = 1;
}


my $dead_run = 0;
if( defined $opt_file) {
	if( $opt_file =~ m,^/, ) {
		$errfile = $opt_file;
	} else {
		$errfile = "$pwd/$opt_file";
		$errfile = "$logdir/$opt_file" if ( ! (-f $errfile ));
	}
}
else {
	$errfile="$logdir/amflush" if(! (-f $errfile));
	if (! -f $errfile) {
		if (-f "$logdir/amflush.1" && -f "$logdir/amdump.1" &&
		    -M "$logdir/amflush.1"  < -M "$logdir/amdump.1") {
			$errfile="$logdir/amflush.1";
		} else {
			$errfile="$logdir/amdump.1";
		}
		$dead_run = 1;
	}
}

open(AMDUMP,"<$errfile") || die("$errfile: $!");
print "Using $errfile\n";

my $taper_status_file;

$start_degraded_mode = 0;

$label = "";					# -w fodder
$origsize = 0;					# -w fodder
$idle_dumpers = 0;
$status_driver = "";
$status_taper = "Searching for a new tape";
$estimate_done = 0;
$holding_space = 0;
$start_time = 0;
@dumpers_active = ();
$nb_tape = 0;
$ntpartition{$nb_tape} = 0;
$ntsize{$nb_tape} = 0;
$ntesize{$nb_tape} = 0;
$tape_size = 0;
$driver_finished = 0;
$generating_schedule = 0;

while($lineX = <AMDUMP>) {
	chomp $lineX;
	$lineX =~ s/[:\s]+$//g; #remove separator at end of line
	next if $lineX eq "";
	@line = &quotewords('[:\s]+', 0, $lineX);
	next if !defined $line[0];

	if($line[0] eq "amdump" || $line[0] eq "amflush") {
		if ($line[1] eq "start" && $line[2] eq "at") {
			$datestr = $lineX;
			$datestr =~ s/.*start at //g;
			if (!defined $opt_locale_independent_date_format) {
				print "From " . $datestr . "\n";
			}
		} elsif($line[1] eq "datestamp") {
			$gdatestamp = $line[2];
			if(!defined $datestamp{$gdatestamp}) {
				$datestamp{$gdatestamp} = 1;
				push @datestamp, $gdatestamp;
			}
		} elsif($line[1] eq "starttime") {
			$starttime=&set_starttime($line[2]);
		} elsif($line[1] eq "starttime-locale-independent") {
			if (defined $opt_locale_independent_date_format) {
				printf "From " . $line[2] . " " . $line[3] . ":" . $line[4] . ":" . $line[5] . " " . $line[6] . "\n";
			}
		}
		if($line[0] eq "amflush") {
			$estimate_done=1;
		}
	} elsif($line[0] eq "planner") {
		if($line[1] eq "timestamp") {
			$gdatestamp = $line[2];
			if(!defined $datestamp{$gdatestamp}) {
				$datestamp{$gdatestamp} = 1;
				push @datestamp, $gdatestamp;
			}
		}
		elsif($line[1] eq "FAILED") {
			#2:host 3:disk 4:datestamp 5:level 6:errmsg
			$host=$line[2];
			$partition=$line[3];
			$datestamp=$line[4];
			$hostpart=&make_hostpart($host,$partition,$datestamp);
			$dump_started{$hostpart}=-1;
			$level{$hostpart}=$line[5];
			$error{$hostpart}="planner: " . $line[6];
	} elsif($line[1] eq "time") {
		if($line[3] eq "got") {
				if($line[4] eq "result") {
					$host = $line[7];
					$partition = $line[9];
					$hostpart=&make_hostpart($host,$partition,$gdatestamp);
					$estimate{$hostpart}=1;
					$level{$hostpart}=$line[10];
					$line[12] =~ /(\d+)K/;
					$esize{$hostpart}=$1 / $unitdivisor;
					$partialestimate{$hostpart}=0;
					$getest{$hostpart} = "";
				} elsif($line[4] eq "partial") {
					$host = $line[8];
					$partition = $line[10];
					$hostpart=&make_hostpart($host,$partition,$gdatestamp);
					$level1 = $line[11];
					$line[13] =~ /(-?\d+)K/;
					$size1 = $1;
					$level2 = $line[14];
					$line[16] =~ /(-?\d+)K/;
					$size2 = $1;
					$level3 = $line[17];
					$line[19] =~ /(-?\d+)K/;
					$size3 = $1;
					if($size1 > 0 || $size2 > 0 || $size3 > 0) {
						$estimate{$hostpart}=1;
						$level{$hostpart}=$line[11];
						$esize{$hostpart}=$size1 / $unitdivisor;
						$partialestimate{$hostpart}=1;
						if($size1 > 0) { $getest{$hostpart} =~ s/:$level1://; }
						if($size2 > 0) { $getest{$hostpart} =~ s/:$level2://; }
						if($size3 > 0) { $getest{$hostpart} =~ s/:$level3://; }
						if($getest{$hostpart} eq "") {$partialestimate{$hostpart}=0;}
					}
				}
			} elsif($line[3] eq "getting" &&
					  $line[4] eq "estimates" &&
					  $line[5] eq "took") {
				$estimate_done=1;
			}
		}
	} elsif($line[0] eq "setup_estimate") {
		$host = $line[1];
		$partition = $line[2];
		$hostpart=&make_hostpart($host,$partition,$gdatestamp);
		$estimate{$hostpart}=0;
		$level{$hostpart}=0;
		$degr_level{$hostpart}=-1;
		$esize{$hostpart}=0;
		$dump_started{$hostpart}=0;
		$dump_finished{$hostpart}=0;
		$taper_started{$hostpart}=0;
		$taper_finished{$hostpart}=0;
		$partialestimate{$hostpart}=0;
		$error{$hostpart}="";
		if($line[7] eq "last_level") {
			$getest{$hostpart}="";
			$level1 = $line[15];
			$level2 = $line[17];
			$level3 = $line[19];
			if($level1 != -1) { $getest{$hostpart} .= ":$level1:" };
			if($level2 != -1) { $getest{$hostpart} .= ":$level2:" };
			if($level3 != -1) { $getest{$hostpart} .= ":$level3:" };
		}
	} elsif($line[0] eq "GENERATING" &&
				$line[1] eq "SCHEDULE") {
		$generating_schedule=1;
	} elsif($line[0] eq "--------") {
		if ($generating_schedule == 1) {
			$generating_schedule = 2;
		} elsif ($generating_schedule == 2) {
			$generating_schedule = 3;
		}
	} elsif($line[0] eq "DUMP") {
		if($generating_schedule == 2 ) {
			$host = $line[1];
			$partition = $line[3];
			$datestamp = $line[4];
			$hostpart=&make_hostpart($host,$partition,$datestamp);
			$level{$hostpart}=$line[6];
			$esize=$line[14];	#compressed size
			$esize=32 if $esize<32;
			$esize{$hostpart}=$esize / $unitdivisor;
			if(!defined($line[25])) {
				$degr_level{$hostpart}=-1;
			} else {
				$degr_level{$hostpart}=$line[17];
				$esize=$line[25];	#compressed size
				$esize=32 if $esize<32;
				$degr_size{$hostpart}=$esize / $unitdivisor;
			}
		}
	} elsif($line[0] eq "FLUSH") {
		$host = $line[1];
		$partition = $line[2];
		$datestamp = $line[3];
		$level = $line[4];
		$holding_file = $line[5];
		$hostpart=&make_hostpart($host,$partition,$datestamp);
		$flush{$hostpart}=0;
		$dump_finished{$hostpart}=0;
		$holding_file{$hostpart}=$holding_file;
		$level{$hostpart}=$level;
	} elsif($line[0] eq "driver") {
		if($line[1] eq "pid") {
			$pid = $line[2];
			if (! $Amanda_process->process_alive($pid, "driver")) {
				$dead_run = 1;
			}
		}
		elsif($line[1] eq "start" && $line[2] eq "time") {
			$start_time=$line[3];
			$current_time=$line[3];
			$dumpers_active[0]=0;
			$dumpers_held[0]={};
			$dumpers_active=0;
		}
		elsif($line[1] eq "tape" && $line[2] eq "size") {
			$lineX =~ /^driver: start time (\S+)/;
			$tape_size = $line[3] / $unitdivisor;
		}
		elsif($line[1] eq "adding" &&
			   $line[2] eq "holding" &&
				$line[3] eq "disk") {
			$holding_space += $line[8];
		}
		elsif($line[1] eq "send-cmd" && $line[2] eq "time") {
			#print "send-cmd: " , $line[5] . " " . $line[6] . " " . $line[7] . "\n" if defined $line[5] && defined $line[6] && defined $line[7];
			$current_time = $line[3];
			if($line[5] =~ /dumper\d*/) {
				$dumper = $line[5];
				if($line[6] eq "PORT-DUMP") {
					#7:handle 8:port 9:host 10:amfeatures 11:disk 12:device 13:level ...
					$host = $line[9];
					$partition = $line[11];
					$hostpart=&make_hostpart($host,$partition,$gdatestamp);
					$serial=$line[7];
					$dump_started{$hostpart}=1;
					$dump_time{$hostpart}=$current_time;
					$dump_finished{$hostpart}=0;
					if(     $level{$hostpart} != $line[13] &&
					   $degr_level{$hostpart} == $line[13]) {
						$level{$hostpart}=$degr_level{$hostpart};
						$esize{$hostpart}=$degr_size{$hostpart};
					}
					if(! defined($busy_time{$dumper})) {
						$busy_time{$dumper}=0;
					}
					$running_dumper{$dumper} = $hostpart;
					$error{$hostpart}="";
					$taper_error{$hostpart}="";
					$size{$hostpart} = 0;
					$dumpers_active++;
					if(! defined($dumpers_active[$dumpers_active])) {
						$dumpers_active[$dumpers_active]=0;
					}
					if(! defined($dumpers_held[$dumpers_active])) {
						$dumpers_held[$dumpers_active]={};
					}
				}
			}
			elsif($line[5] =~ /chunker\d*/) {
				if($line[6] eq "PORT-WRITE") {
					$host=$line[9];
					$partition=$line[11];
					$hostpart=&make_hostpart($host,$partition,$gdatestamp);
					$serial=$line[7];
					$serial{$serial}=$hostpart;
					$holding_file{$hostpart}=$line[8];
					#$chunk_started{$hostpart}=1;
					$chunk_time{$hostpart}=$current_time;
					#$chunk_finished{$hostpart}=0;
					$size{$hostpart} = 0;
				}
				elsif($line[6] eq "CONTINUE") {
					#7:handle 8:filename 9:chunksize 10:use
					$serial=$line[7];
					$hostpart=$serial{$serial};
					if($hostpart ne "") {
						$dump_roomq{$hostpart}=undef;
						$error{$hostpart}="";
					}
				}
			}
			elsif($line[5] =~ /taper/) {
				if($line[6] eq "START-TAPER") {
					#7:name 8:timestamp
					$gdatestamp=$line[8];
					if(!defined $datestamp{$gdatestamp}) {
						$datestamp{$gdatestamp} = 1;
						push @datestamp, $gdatestamp;
					}
					$status_taper = "Searching for a new tape";
				}
				elsif($line[6] eq "NEW-TAPE") {
					#7:name 8:handle
					$status_taper = "Searching for a new tape";
				}
				elsif($line[6] eq "NO-NEW-TAPE") {
					#7:name 8:handle 9:errmsg
					$serial=$line[8];
					$error=$line[9];
					$status_taper = $error;
				}
				elsif($line[6] eq "FILE-WRITE") {
					#7:name 8:handle 9:filename 10:host 11:disk 12:level 13:datestamp 14:splitsize
					$serial=$line[8];
					$host=$line[10];
					$partition=$line[11];
					$level=$line[12];
					$ldatestamp=$line[13];
					$status_taper = "Writing $host:$partition";
					if(!defined $datestamp{$ldatestamp}) {
						$datestamp{$ldatestamp} = 1;
						push @datestamp, $ldatestamp;
					}
					$hostpart=&make_hostpart($host,$partition,$ldatestamp);
					$serial{$serial}=$hostpart;
					if(!defined $level{$hostpart}) {
						$level{$hostpart} = $level;
					}
					$taper_started{$hostpart}=1;
					$taper_finished{$hostpart}=0;
					$taper_time{$hostpart}=$current_time;
					$taper_error{$hostpart}="";
					$ntchunk_size = 0;
				}
				elsif($line[6] eq "PORT-WRITE") {
					#7:name 8:handle 9:host 10:disk 11:level 12:datestamp 13:splitsize 14:diskbuffer 15:fallback_splitsize
					$serial=$line[8];
					$host=$line[9];
					$partition=$line[10];
					$level=$line[11];
					$ldatestamp=$line[12];
					$status_taper = "Writing $host:$partition";
					$hostpart=&make_hostpart($host,$partition,$ldatestamp);
					$serial{$serial}=$hostpart;
					$taper_started{$hostpart}=1;
					$taper_finished{$hostpart}=0;
					$taper_time{$hostpart}=$current_time;
					$taper_error{$hostpart}="";
					$size{$hostpart} = 0;
					$ntchunk_size = 0;
				}
			}
		}
		elsif($line[1] eq "result" && $line[2] eq "time") {
			#print "result: " , $line[5] . " " . $line[6] . " " . $line[7] . "\n" if defined $line[5] && defined $line[6] && defined $line[7];
			$current_time = $line[3];
			if($line[5] =~ /dumper\d+/) {
				if($line[6] eq "FAILED" || $line[6] eq "TRY-AGAIN") {
					#7:handle 8:message
					$serial = $line[7];
					$error = $line[8];
					$hostpart=$serial{$serial};
					if ($taper_started{$hostpart} == 1) {
						$dump_finished{$hostpart}=-1;
					} else {
						$dump_finished{$hostpart}=-3;
					}
					$busy_time{$line[5]}+=($current_time-$dump_time{$hostpart});
					$running_dumper{$line[5]} = "0";
					$dump_time{$hostpart}=$current_time;
					if (!$taper_error{$hostpart}) {
						$error{$hostpart}="dumper: $error";
					}
					$dumpers_active--;

				}
				elsif($line[6] eq "DONE") {
					#7:handle 8:origsize 9:size ...
					$serial=$line[7];
					$origsize=$line[8] / $unitdivisor;
					$outputsize=$line[9] / $unitdivisor;
					$hostpart=$serial{$serial};
					$size{$hostpart}=$outputsize;
					$dump_finished{$hostpart}=1;
					$busy_time{$line[5]}+=($current_time-$dump_time{$hostpart});
					$running_dumper{$line[5]} = "0";
					$dump_time{$hostpart}=$current_time;
					$error{$hostpart}="";
					$dumpers_active--;
				}
				elsif($line[6] eq "ABORT-FINISHED") {
					#7:handle
					$serial=$line[7];
					$hostpart=$serial{$serial};
					$dump_started{$hostpart}=0;
					if ($taper_started{$hostpart} == 1) {
						$dump_finished{$hostpart}=-1;
					} else {
						$dump_finished{$hostpart}=-3;
					}
					$busy_time{$line[5]}+=($current_time-$dump_time{$hostpart});
					$running_dumper{$line[5]} = "0";
					$dump_time{$hostpart}=$current_time;
					$error{$hostpart}="dumper: (aborted)";
					$dumpers_active--;
				}
			}
			elsif($line[5] =~ /chunker\d+/) {
				if($line[6] eq "DONE" || $line[6] eq "PARTIAL") {
					#7:handle 8:size
					$serial=$line[7];
					$outputsize=$line[8] / $unitdivisor;
					$hostpart=$serial{$serial};
					$size{$hostpart}=$outputsize;
					if ($line[6] eq "DONE") {
						$dump_finished{$hostpart}=1;
					} else {
						$dump_finished{$hostpart}=-3;
					}
					$busy_time{$line[5]}+=($current_time-$chunk_time{$hostpart});
					$running_dumper{$line[5]} = "0";
					$chunk_time{$hostpart}=$current_time;
					if ($line[6] eq "PARTIAL") {
						$partial{$hostpart} = 1;
					}
					else {
						$partial{$hostpart} = 0;
						$error{$hostpart}="";
					}
				}
				elsif($line[6] eq "FAILED") {
					$serial=$line[7];
					$hostpart=$serial{$serial};
					$dump_finished{$hostpart}=-1;
					$busy_time{$line[5]}+=($current_time-$chunk_time{$hostpart});
					$running_dumper{$line[5]} = "0";
					$chunk_time{$hostpart}=$current_time;
					$error{$hostpart}="chunker: " .$line[8] if $error{$hostpart} eq "";
				}
				elsif($line[6] eq "RQ-MORE-DISK") {
					#7:handle
					$serial=$line[7];
					$hostpart=$serial{$serial};
					$dump_roomq{$hostpart}=1;
					$error{$hostpart}="(waiting for holding disk space)";
				}
			}
			elsif($line[5] eq "taper") {
				if($line[6] eq "DONE" || $line[6] eq "PARTIAL") {
					#DONE:    7:handle 8:label 9:filenum 10:errstr
					#PARTIAL: 7:handle 8:INPUT-* 9:TAPE-* 10:errstr 11:INPUT-MSG 12:TAPE-MSG
					$serial=$line[7];

					$status_taper = "Idle";
					$hostpart=$serial{$serial};
					$line[10] =~ /sec (\S+) (kb|bytes) (\d+) kps/;
					if ($2 eq 'kb') {
						$size=$3 / $unitdivisor;
					} else {
						$size=$3 / ( $unitdivisor * 1024);
					}
					$taper_finished{$hostpart}=1;
					$busy_time{"taper"}+=($current_time-$taper_time{$hostpart});
					$taper_time{$hostpart}=$current_time;
					if(!defined $size{$hostpart}) {
						$size{$hostpart}=$size;
					}
					$ntpartition{$nb_tape}++;
					$ntsize{$nb_tape} += $size{$hostpart} - $ntchunk_size;
					if(defined $esize{$hostpart} && $esize{$hostpart} > 1) {
						$ntesize{$nb_tape} += $esize{$hostpart} - $ntchunk_size;
					}
					else {
						$ntesize{$nb_tape} += $size{$hostpart} - $ntchunk_size;
					}
					if ($line[6] eq "PARTIAL") {
						$partial{$hostpart} = 1;
						if ($line[9] eq "TAPE-ERROR") {
							$error{$hostpart} = "taper: $line[12]";
							$taper_error{$hostpart} = "taper: $line[12]";
						}
					}
					else {
						$partial{$hostpart} = 0;
					}
					if ($ntchunk_size > 0) {
						$ntchunk{$nb_tape}++;
					}
					undef $taper_status_file;
				}
				elsif($line[6] eq "PARTDONE") {
					#7:handle 8:label 9:filenum 10:ksize 11:errstr
					$serial=$line[7];
					$hostpart=$serial{$serial};
					#$line[11] =~ /.*kb (\d*) kps/;
					#$size=$1 / $unitdivisor;
					$size=$line[10] / $unitdivisor;
					$tapedsize{$hostpart} += $size;
					$ntchunk{$nb_tape}++;
					$ntsize{$nb_tape} += $size;
					$ntesize{$nb_tape} += $size;
					$ntchunk_size += $size;
				}
				elsif($line[6] eq "REQUEST-NEW-TAPE") {
					#7:serial
					$serial=$line[7];
					$old_status_taper = $status_taper;
					$status_taper = "Asking for a new tape";
					$hostpart=$serial{$serial};
					if (defined $hostpart) {
						$olderror{$hostpart} = $error{$hostpart};
						$error{$hostpart} = "waiting for a new tape";
					}
				}
				elsif($line[6] eq "NEW-TAPE") {
					#7:serial #8:label
					$serial=$line[7];
					$status_taper = $old_status_taper;
					$hostpart=$serial{$serial};
					if (defined $hostpart) {
						$error{$hostpart} = $olderror{$hostpart};
					}
				}
				elsif($line[6] eq "TAPER-OK") {
					#7:name #8:label
					$status_taper = "Idle";
				}
				elsif($line[6] eq "TAPE-ERROR") {
					#7:name 8:errstr
					$error=$line[8];
					$status_taper = $error;
					$exit_status |= $STATUS_TAPE;
					undef $taper_status_file;
				}
				elsif($line[6] eq "FAILED") {
					#7:handle 8:INPUT- 9:TAPE- 10:input_message 11:tape_message
				   $serial=$line[7];
					$hostpart=$serial{$serial};
					if(defined $hostpart) {
						if($line[9] eq "TAPE-ERROR") {
							$error=$line[11];
							$taper_finished{$hostpart} = -2;
							$status_taper = $error;
						}
						else {
							$error=$line[10];
							$taper_finished{$hostpart} = -1;
							$status_taper = "Idle";
						}
						$busy_time{"taper"}+=($current_time-$taper_time{$hostpart});
						$taper_time{$hostpart}=$current_time;
						$error{$hostpart}="$error";
					}
					undef $taper_status_file;
				}
			}
		}
		elsif($line[1] eq "finished-cmd" && $line[2] eq "time") {
			$current_time=$line[3];
			if($line[4] =~ /dumper\d+/) {
			}
		}
		elsif($line[1] eq "dump" && $line[2] eq "failed") {
			#3:handle 4: 5: 6:"too many dumper retry"
			$serial=$line[3];
			$hostpart=$serial{$serial};
			$dump_started{$hostpart}=-1;
			$dump_finished{$hostpart}=-2;
			$error{$hostpart} .= "(" . $line[6] . ")";
		}
		elsif($line[1] eq "tape" && $line[2] eq "failed") {
			#3:handle 4: 5: 6:"too many dumper retry"
			$serial=$line[3];
			$hostpart=$serial{$serial};
			$taper_started{$hostpart}=-1;
			$taper_finished{$hostpart}=-2;
			$error{$hostpart} .= "(" . $line[6] . ")";
		}
		elsif($line[1] eq "state" && $line[2] eq "time") {
			#3:time 4:"free" 5:"kps" 6:free 7:"space" 8:space 9:"taper" 10:taper 11:"idle-dumpers" 12:idle-dumpers 13:"qlen" 14:"tapeq" 15:tapeq 16:"runq" 17:runq 18:"roomq" 19:roomq 20:"wakeup" 21:wakeup 22:"driver-idle" 23:driver-idle
			$current_time=$line[3];
			$idle_dumpers=$line[12];

			$free{"kps"} = $line[6];
			$free{"space"} = $line[8];
			$qlen{"tapeq"} = $line[15];
			$qlen{"runq"} = $line[17];
			$qlen{"roomq"} = $line[19];

			if(defined($dumpers_active)) {
				if($status_driver ne "") {
					$dumpers_active[$dumpers_active_prev]
						+=$current_time-$state_time_prev;
					$dumpers_held[$dumpers_active_prev]{$status_driver}
						+=$current_time-$state_time_prev;
				}
				$state_time_prev=$current_time;
				$dumpers_active_prev=$dumpers_active;
				$status_driver=$line[23];
				if(! defined($dumpers_held[$dumpers_active]{$status_driver})) {
					$dumpers_held[$dumpers_active]{$status_driver}=0;
				}
			}
		}
	   elsif($line[1] eq "FINISHED") {
			$driver_finished = 1;
		}
	}
	elsif($line[0] eq "dump") {
		if($line[1] eq "of" &&
			$line[2] eq "driver" &&
			$line[3] eq "schedule" &&
			$line[4] eq "after" &&
			$line[5] eq "start" &&
			$line[6] eq "degraded" &&
			$line[7] eq "mode") {
			$start_degraded_mode=1;
		}
	}
	elsif($line[0] eq "taper") {
		if($line[1] eq "wrote") {
			#1:"wrote" 2:"label" 3:label
			$nb_tape++;
			$label = $line[3];
			$ntlabel{$nb_tape} = $label;
			$ntpartition{$nb_tape} = 0;
			$ntsize{$nb_tape} = 0;
			$ntesize{$nb_tape} = 0;
		}
		elsif($line[1] eq "status" && $line[2] eq "file") {
			#1:"status" #2:"file:" #3:hostname #4:diskname #5:filename
			$taper_status_file = $line[5];
		}
	}
	elsif($line[0] eq "splitting" &&
			 $line[1] eq "chunk" &&
			 $line[2] eq "that" &&
			 $line[3] eq "started" &&
			 $line[4] eq "at" &&
			 $line[6] eq "after") {
		$line[7] =~ /(\d*)kb/;
		$size = $1;
		$ntchunk{$nb_tape}++;
		$ntsize{$nb_tape} += $size / $unitdivisor;
		$ntesize{$nb_tape} += $size / $unitdivisor;
		$ntchunk_size += $size / $unitdivisor;
	}
	else {
		#print "Ignoring: $lineX\n";
	}
}

close(AMDUMP);

if(defined $current_time) {
	for ($d = 0; $d < $#dumpers_active; $d++) {
		$the_dumper = "dumper$d";
		if(defined($running_dumper{$the_dumper}) &&
		   $running_dumper{$the_dumper} ne "0") {
			$busy_time{$the_dumper}+=($current_time-$dump_time{$running_dumper{$the_dumper}});
		}
	}
}

print "\n";

$nb_partition = 0;

$epartition = 0;
$estsize = 0;
$fpartition = 0;
$fsize = 0;
$wpartition = 0;
$wsize = 0;

$flpartition = 0;
$flsize = 0;
$wfpartition = 0;
$wfsize = 0;

$dtpartition = 0;
$dtesize = 0;
$dupartition = 0;
$dusize = 0;
$duesize = 0;
$dpartition = 0;
$dsize = 0;
$desize = 0;

$twpartition = 0;
$twsize = 0;
$twesize = 0;
$tapartition = 0;
$tasize = 0;
$taesize = 0;
$tfpartition = 0;
$tfsize = 0;
$tfesize = 0;
$tpartition = 0;
$tsize = 0;
$tesize = 0;

$maxnamelength = 10;
foreach $host (sort @hosts) {
	foreach $partition (sort @$host) {
		foreach $datestamp (sort @datestamp) {
			$hostpart=&make_hostpart($host,$partition,$datestamp);
			next if(!defined $estimate{$hostpart} && !defined $flush{$hostpart});
			if(length("$host:$partition") > $maxnamelength) {
				$maxnamelength = length("$host:$partition");
			}
		}
	}
}

foreach $host (sort @hosts) {
	foreach $partition (sort @$host) {
	   $qpartition = Amanda::Util::quote_string($partition);
	   foreach $datestamp (sort @datestamp) {
			$hostpart=&make_hostpart($host,$partition,$datestamp);
			next if(!defined $estimate{$hostpart} && !defined $flush{$hostpart});
			$nb_partition++;
			if( (!defined $size{$hostpart} || $size{$hostpart} == 0) &&
				 defined $holding_file{$hostpart}) {
				$size{$hostpart} = &dump_size($holding_file{$hostpart}) / (1024 * $unitdivisor);
			}
			$in_flush=0;
			if($estimate_done != 1 && !defined $flush{$hostpart}) {
				if(defined $estimate{$hostpart}) {
					if($estimate{$hostpart} != 1) {
						if( defined $opt_gestimate ||
							 defined $opt_failed && $dead_run != 0) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s", "$host:$qpartition";
							print "             ";
							if ($dead_run) {
								print " failed: killed while";
								$exit_status |= $STATUS_FAILED;
							}
							print " getting estimate\n";
						}
					}
					else {
						if(defined $opt_estimate ||
							(defined $opt_gestimate && $partialestimate{$hostpart} == 1) ||
							(defined $opt_failed && $dead_run != 0 && $partialestimate{$hostpart} == 1)) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s", "$host:$qpartition";
							printf "%2d ",  $level{$hostpart};
							printf "%9d$unit", $esize{$hostpart};
							if($partialestimate{$hostpart} == 1) {
								if ($dead_run) {
									print " failed: killed while";
									$exit_status |= $STATUS_FAILED;
								}
								print " partial";
							}
							print " estimate done\n";
						}
						$epartition++;
						$estsize += $esize{$hostpart};
					}
				}
			}
			else {
				if(defined $estimate{$hostpart}) {
					if($estimate{$hostpart} == 1) {
						$epartition++;
						$estsize += $esize{$hostpart};
					}
					elsif (!defined $dump_started{$hostpart} || $dump_started{$hostpart} == 0) {
						if( defined $opt_failed) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "           no estimate\n";
						}
						$exit_status |= $STATUS_FAILED;
						$fpartition++;
						$fsize+=$esize{$hostpart};
					}
				}
				else {
					$flpartition++;
					$flsize += $size{$hostpart};
					$in_flush=1;
				}
				if(defined $taper_started{$hostpart} &&
						$taper_started{$hostpart}==1 &&
						$dump_finished{$hostpart}!=-3) {
					if(defined $dump_started{$hostpart} &&
						$dump_started{$hostpart} == 1 &&
							$dump_finished{$hostpart} == -1) {
						if(defined $opt_failed) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $esize{$hostpart};
							print " dump to tape failed: " . $error{$hostpart};
							print "\n";
						}
						$exit_status |= $STATUS_FAILED;
						$fpartition++;
						$fsize+=$esize{$hostpart};
					} elsif(defined $dump_started{$hostpart} &&
						$dump_started{$hostpart} == 1 &&
							$dump_finished{$hostpart} == 0 &&
							$taper_started{$hostpart} == 1) {
						if( defined $opt_dumpingtape ||
							 defined $opt_failed && $dead_run != 0) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $esize{$hostpart};
							if ($dead_run) {
								print " failed: killed while";
								$exit_status |= $STATUS_FAILED;
							}
							print " dumping to tape";
							$size = $tapedsize{$hostpart};
							if ($taper_status_file && -f $taper_status_file &&
								open FF, "<$taper_status_file") {
								$line = <FF>;
								if (defined $line) {
									chomp $line;
									$value = $line / ($unitdivisor * 1024);
									if ($value) {
										$size = $value if (!defined($size) || $value > $size);
									}
								}
								close FF;
							}
							if(defined($size)) {
								printf " (%d$unit done (%0.2f%%))", $size, 100.0 * $size/$esize{$hostpart};
								$dtsize += $size;
							}
							if( defined $starttime ) {
								print " (", &showtime($taper_time{$hostpart}), ")";
							}
							print "\n";
						}
						$dtpartition++;
						$dtesize += $esize{$hostpart};
					}
					elsif($taper_finished{$hostpart} == 0) {
						if( defined $opt_writingtape ||
							 defined $opt_failed && $dead_run != 0) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $size{$hostpart};
							if ($dead_run) {
								print " failed: killed while";
								$exit_status |= $STATUS_FAILED;
							}
							if($in_flush == 0) {
								print " dump done," if defined $dump_finished{$hostpart} && $dump_finished{$hostpart} == 1;
								print " writing to tape";
							}
							else {
								print " flushing to tape";
							}
							$size = $tapedsize{$hostpart};
							if ($taper_status_file &&  -f $taper_status_file &&
								open FF, "<$taper_status_file") {
								$line = <FF>;
								if (defined $line) {
									chomp $line;
									$value = $line / ($unitdivisor * 1024);
									if ($value) {
										$size = $value if (!defined($size) || $value > $size);
									}
								}
								close FF;
							}
							if(defined($size)) {
								printf " (%d$unit done (%0.2f%%))", $size, 100.0 * $size/$size{$hostpart};
							}
							if( defined $starttime ) {
								print " (", &showtime($taper_time{$hostpart}), ")";
							}
							print ", ", $error{$hostpart} if (defined($error{$hostpart}) &&
																	    $error{$hostpart} ne "");
							print "\n";
						}
						$tapartition++;
						$tasize += $size{$hostpart};
						if(defined $esize{$hostpart}) {
							$taesize += $esize{$hostpart};
						}
						else {
							$taesize += $size{$hostpart};
						}
						if (defined $dump_finished{$hostpart} && $dump_finished{$hostpart} == 1) {
							$dpartition++;
							$dsize += $size{$hostpart};
							if(defined $esize{$hostpart} && $esize{$hostpart} > 1) {
								$desize += $esize{$hostpart};
							} else {
								$desize += $size{$hostpart};
							}
						}
					}
					elsif($taper_finished{$hostpart} < 0) {

						if(defined $size{$hostpart}) {
							$xsize = $size{$hostpart};
						}
						elsif(defined $esize{$hostpart}) {
							$xsize = $esize{$hostpart};
						}
						else {
							$xsize = 0;
						}

						if(defined $esize{$hostpart}) {
							$exsize += $esize{$hostpart};
						}
						else {
							$exsize += $xsize;
						}

						if( defined $opt_failed  ||
							 (defined $opt_waittaper && ($taper_finished{$hostpart} == -1))) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $xsize;
						    print " dump done," if defined $dump_finished{$hostpart} && $dump_finished{$hostpart} == 1;
							if($in_flush == 0) {
								print " failed to tape";
							}
							else {
								print " failed to flush";
							}
							print ": ",$error{$hostpart} if defined $error{$hostpart};

							print " (will retry)" unless $taper_finished{$hostpart} < -1;
							if( defined $starttime ) {
								print " (", &showtime($taper_time{$hostpart}), ")";
							}
							print "\n";
						}
						$exit_status |= $STATUS_TAPE;

						$tfpartition++;
						$tfsize += $xsize;
						$tfesize += $exsize;

						if($in_flush == 0) {
							$twpartition++;
							$twsize += $xsize;
							$twesize += $exsize;
						}
						else {
							$wfpartition++;
							$wfsize += $xsize;
						}
						if (defined $dump_finished{$hostpart} && $dump_finished{$hostpart} == 1) {
							$dpartition++;
							$dsize += $size{$hostpart};
							if(defined $esize{$hostpart} && $esize{$hostpart} > 1) {
								$desize += $esize{$hostpart};
							} else {
								$desize += $size{$hostpart};
							}
						}
					}
					elsif($taper_finished{$hostpart} == 1) {
						if( defined $opt_finished ) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $size{$hostpart};
							if($in_flush == 0) {
								print " finished";
							}
							else {
								print " flushed";
							}
							if( defined $starttime ) {
								print " (", &showtime($taper_time{$hostpart}), ")";
							}
							if(defined $partial{$hostpart} && $partial{$hostpart} == 1) {
								print ", PARTIAL";
								$exit_status |= $STATUS_FAILED;
							}
							print "\n";
						}
						if (defined $dump_finished{$hostpart} && $dump_finished{$hostpart} == 1) {
							$dpartition++;
							$dsize += $size{$hostpart};
							if(defined $esize{$hostpart} && $esize{$hostpart} > 1) {
								$desize += $esize{$hostpart};
							} else {
								$desize += $size{$hostpart};
							}
						}
						$tpartition++;
						$tsize += $size{$hostpart};
						if(defined $esize{$hostpart} && $esize{$hostpart} > 1) {
							$tesize += $esize{$hostpart};
						}
						else {
							$tesize += $size{$hostpart};
						}
					}
					else {
						printf "%8s ", $datestamp if defined $opt_date;
						printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
						print " unknown state TAPER\n";
					}
				}
				elsif(defined $dump_started{$hostpart}) {
					if($dump_started{$hostpart} == -1) {
						if( defined $opt_failed ) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "failed: " . $error{$hostpart} . "\n";
						}
						$exit_status |= $STATUS_FAILED;

						$fpartition++;
						$fsize+=$esize{$hostpart};
					}
					elsif($dump_started{$hostpart} == 0) {
						if($estimate{$hostpart} == 1) {
							if( defined $opt_waitdumping ) {
								printf "%8s ", $datestamp if defined $opt_date;
								printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
								printf "%9d$unit", $esize{$hostpart};
								if ($dead_run) {
									print " failed: process terminated while";
									$exit_status |= $STATUS_FAILED;
								}
								print " waiting for dumping $error{$hostpart}\n";
							}
							if($driver_finished == 1) {
								$exit_status |= $STATUS_MISSING;
							}
							$wpartition++;
							$wsize += $esize{$hostpart};
						}
					}
					elsif($dump_started{$hostpart} == 1 &&
							($dump_finished{$hostpart} == -1 ||
						    $dump_finished{$hostpart} == -3)) {
						if( defined $opt_failed ) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							print "backup failed: ", $error{$hostpart};
							if( defined $starttime ) {
								print " (", &showtime($dump_time{$hostpart}), ")";
							}
							print "\n";
						}
						$exit_status |= $STATUS_FAILED;
						$fpartition++;
						$fsize+=$esize{$hostpart};
					}
					elsif($dump_started{$hostpart} == 1 &&
							$dump_finished{$hostpart} == 0) {
						if( defined $opt_dumping ||
							 defined $opt_failed && $dead_run != 0) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $esize{$hostpart};
							if ($dead_run) {
								print " failed: killed while";
								$exit_status |= $STATUS_FAILED;
							}
							printf " dumping %8d$unit", $size{$hostpart};
							if($size{$hostpart} != 0) {
								printf " (%6.2f%%)", (100.0*$size{$hostpart})/$esize{$hostpart};
							}
							if( defined $starttime ) {
								print " (", &showtime($dump_time{$hostpart}), ")";
							}
							if(defined $dump_roomq{$hostpart}) {
								print " " . $error{$hostpart};
							}
							print "\n";
						}
						$dupartition++;
						$dusize += $size{$hostpart};
						$duesize += $esize{$hostpart};
					}
					elsif($dump_finished{$hostpart} == 1 &&
							$taper_started{$hostpart} != 1) {
						if( defined $opt_waittaper ) {
							printf "%8s ", $datestamp if defined $opt_date;
							printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
							printf "%9d$unit", $size{$hostpart};
							print " dump done";
							if( defined $starttime ) {
								print " (", &showtime($dump_time{$hostpart}), ")";
							}
							print ",";
							if ($dead_run) {
								print " process terminated while";
							}
							print " waiting for writing to tape";
							if(defined $partial{$hostpart} && $partial{$hostpart} == 1) {
								print ", PARTIAL";
								$exit_status |= $STATUS_FAILED;
							}
							print "\n";
						}
						$dpartition++;
						$dsize += $size{$hostpart};
						$desize += $esize{$hostpart};
						$twpartition++;
						$twsize += $size{$hostpart};
						$twesize += $esize{$hostpart};
					}
					else {
						printf "%8s ", $datestamp if defined $opt_date;
						printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
						print " unknown state DUMPER\n";
					}
				}
				elsif(defined $flush{$hostpart}) {
					if( defined $opt_waittaper ) {
						printf "%8s ", $datestamp if defined $opt_date;
						printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
						printf "%9d$unit", $size{$hostpart};
						if ($dead_run) {
							print " process terminated while";
						}
						print " waiting to flush";
						if(defined $partial{$hostpart} && $partial{$hostpart} == 1) {
							print ", PARTIAL";
							$exit_status |= $STATUS_FAILED;
						}
						print "\n";
					}
					$wfpartition++;
					$wfsize += $size{$hostpart};
				}
				elsif(defined $level{$hostpart}) {
					printf "%8s ", $datestamp if defined $opt_date;
					printf "%-${maxnamelength}s%2d ", "$host:$qpartition", $level{$hostpart};
					print " unknown state\n";
				}
			}
		}
	}
}

if (defined $opt_summary) {
	print "\n";
	print  "SUMMARY          part      real  estimated\n";
	print  "                           size       size\n";
	printf "partition       : %3d\n", $nb_partition;
	printf "estimated       : %3d %20d$unit\n", $epartition , $estsize;
	printf "flush           : %3d %9d$unit\n", $flpartition, $flsize;
	printf "failed          : %3d %20d$unit           (%6.2f%%)\n",
		$fpartition , $fsize,
		$estsize ? ($fsize * 1.0 / $estsize) * 100 : 0.0;
	printf "wait for dumping: %3d %20d$unit           (%6.2f%%)\n",
		$wpartition , $wsize,
		$estsize ? ($wsize * 1.0 / $estsize) * 100 : 0.0;
	if(defined($dtsize)) {
		printf "dumping to tape : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
			$dtpartition, $dtsize, $dtesize,
			$dtsize ? ($dtsize * 1.0 / $dtesize) * 100 : 0.0,
			$estsize ? ($dtesize * 1.0 / $estsize) * 100 : 0.0;
	} else {
		printf "dumping to tape : %3d %20d$unit           (%6.2f%%)\n",
			$dtpartition, $dtesize,
			$estsize ? ($dtesize * 1.0 / $estsize) * 100 : 0.0;
	}
	printf "dumping         : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$dupartition, $dusize, $duesize,
		$duesize ? ($dusize * 1.0 / $duesize) * 100 : 0.0,
		$estsize ? ($dusize * 1.0 / $estsize) * 100 : 0.0;
	printf "dumped          : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$dpartition , $dsize , $desize,
		$desize ? ($dsize * 1.0 / $desize) * 100 : 0.0,
		$estsize ? ($dsize * 1.0 / $estsize) * 100 : 0.0;
	printf "wait for writing: %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$twpartition, $twsize, $twesize,
		$twesize ? ($twsize * 1.0 / $twesize) * 100 : 0.0,
		$estsize ? ($twsize * 1.0 / $estsize) * 100 : 0.0;
	printf "wait to flush   : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$wfpartition, $wfsize, $wfsize, 100, 0;
	printf "writing to tape : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$tapartition, $tasize, $taesize,
		$taesize ? ($tasize * 1.0 / $taesize) * 100 : 0.0,
		$estsize ? ($tasize * 1.0 / $estsize) * 100 : 0.0;
	printf "failed to tape  : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$tfpartition, $tfsize, $tfesize,
		$tfesize ? ($tfsize * 1.0 / $tfesize) * 100 : 0.0,
		$estsize ? ($tfsize * 1.0 / $estsize) * 100 : 0.0;
	printf "taped           : %3d %9d$unit %9d$unit (%6.2f%%) (%6.2f%%)\n",
		$tpartition , $tsize , $tesize,
		$tesize ? ($tsize * 1.0 / $tesize) * 100 : 0.0,
		($estsize+$flsize) ? ($tsize * 1.0 / ($estsize + $flsize)) * 100 : 0.0;
	if($nb_tape > 1 || $tape_size != 0) {
		for($i=1; $i <= $nb_tape; $i++) {
			if($tape_size != 0) {
				printf "  tape %-3d      : %3d %9d$unit %9d$unit (%6.2f%%) %s",
					$i, $ntpartition{$i}, $ntsize{$i}, $ntesize{$i}, 100*$ntsize{$i}/$tape_size, $ntlabel{$i};
			}
			else {
				printf "  tape %-3d      : %3d %9d$unit %9d$unit %s",
					$i, $ntpartition{$i}, $ntsize{$i}, $ntesize{$i}, $ntlabel{$i};
			}
			if(defined($ntchunk{$i}) && $ntchunk{$i} > 0) {
				printf " (%d chunks)", $ntchunk{$i};
			}
			print "\n";
		}
	}
	if($idle_dumpers == 0) {
		printf "all dumpers active\n";
	}
	else {
		$c1 = ($idle_dumpers == 1) ? "" : "s";
		$c2 = ($idle_dumpers < 10) ? " " : "";
		$c3 = ($idle_dumpers == 1) ? " " : "";
		printf "%d dumper%s idle%s %s: %s\n", $idle_dumpers, $c1, $c2, $c3, $status_driver;
	}

	printf "taper status: $status_taper\n";
	if (defined $qlen{"tapeq"}) {
		printf "taper qlen: %d\n", $qlen{"tapeq"};
	}
	if (defined ($free{"kps"})) {
		printf "network free kps: %9d\n", $free{"kps"};
	}
	if (defined ($free{"space"})) {
		if ($holding_space) {
			$hs = ($free{"space"} * 1.0 / $holding_space) * 100;
		} else {
			$hs = 0.0;
		}
		printf "holding space   : %9d$unit (%6.2f%%)\n", ($free{"space"}/$unitdivisor), $hs;
	}
}

if(defined $opt_stats) {
	if(defined($current_time) && $current_time != $start_time) {
		$total_time=$current_time-$start_time;
		foreach $key (sort byprocess keys %busy_time) {
			printf "%8s busy   : %8s  (%6.2f%%)\n",
				$key, &busytime($busy_time{$key}),
				($busy_time{$key} * 1.0 / $total_time) * 100;
		}
		for ($d = 0; $d <= $#dumpers_active; $d++) {
			$l = sprintf "%2d dumper%s busy%s : %8s  (%6.2f%%)",
				$d, ($d == 1) ? "" : "s", ($d == 1) ? " " : "",
				&busytime($dumpers_active[$d]),
				($dumpers_active[$d] * 1.0 / $total_time) * 100;
			print $l;
			$s1 = "";
			$s2 = " " x length($l);
			$r = $dumpers_held[$d];
			foreach $key (sort valuesort keys %$r) {
				next
				  unless $dumpers_held[$d]{$key} >= 1;
				printf "%s%20s: %8s  (%6.2f%%)\n",
					$s1,
					$key,
					&busytime($dumpers_held[$d]{$key}),
					($dumpers_held[$d]{$key} * 1.0 / $dumpers_active[$d]) * 100;
				$s1 = $s2;
			}
			if ($s1 eq "") {
				print "\n";
			}
		}
	}
}

exit $exit_status;

sub make_hostpart() {
	local($host,$partition,$datestamp) = @_;

	if(! defined($hosts{$host})) {
		push @hosts, $host;
		$hosts{$host}=1;
	}
	my($new_part) = 1;
	foreach $pp (sort @$host) {
		$new_part = 0 if ($pp eq $partition);
	}
	push @$host, $partition if $new_part==1;

	my($hostpart) = "$host$partition$datestamp";
	if(!defined $datestamp{$datestamp}) {
		$datestamp{$datestamp} = 1;
		push @datestamp, $datestamp;
	}

	return $hostpart;
}

sub byprocess() {
	my(@tmp_a) = split(/(\d*)$/, $a, 2);
	my(@tmp_b) = split(/(\d*)$/, $b, 2);
	return ($tmp_a[0] cmp $tmp_b[0]) || ($tmp_a[1] <=> $tmp_b[1]);
}                               
 
sub valuesort() {
	$r->{$b} <=> $r->{$a};
}

sub dump_size() {
	local($filename) = @_;
	local($size);
	local($dsize) = 0;
	local($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
		   $atime,$mtime,$ctime,$blksize,$blocks);
	while ($filename ne "") {
		$filename = "$filename.tmp" if (!(-e "$filename"));
		$filename = "/dev/null" if (!(-e "$filename"));
		($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
				$atime,$mtime,$ctime,$blksize,$blocks) = stat($filename);
		$size=$size-32768 if $size > 32768;
		$dsize += $size;
		open(DUMP,$filename);
		$filename = "";
		while(<DUMP>) {
			if(/^CONT_FILENAME=(.*)$/) { $filename = $1; last }
			last if /^To restore, position tape at start of file and run/;
		}
		close(DUMP);
	}
	return $dsize;
}

sub unctime() {
	my (@MoY);
	my (@tl);
	my ($a);
	my ($m);
	my ($month);
	my ($time);

	@MoY = ('Jan','Feb','Mar','Apr','May','Jun',
		'Jul','Aug','Sep','Oct','Nov','Dec');

	# Preset an array of values in case some parts are not passed as
	# arguments.  This lets the date, etc, be omitted and default to
	# today.

	@tl = localtime;

	foreach $a (@_) {
		next
		  if ($a eq '');

		# See if this argument looks like a month name.

		$month = 0;
		foreach $m (@MoY) {
			last
			  if ($m eq $a);
			$month = $month + 1;
		}
		if ($month < 12) {
			$tl[4] = $month;
			next;
		}

		# See if this is a day of the month.

		if ($a =~ /^\d+$/ && $a >= 1 && $a <= 32) {
			$tl[3] = $a;
			next;
		}

		# See if the next argument looks like a time.

		if ($a =~ /^(\d+):(\d+)/) {
			$tl[2] = $1;
			$tl[1] = $2;
			if ($a =~ /^(\d+):(\d+):(\d+)/) {
				$tl[0] = $3;
			}
			next;
		}

		# See if this is a year.

		if ($a =~ /^\d\d\d\d$/ && $a >= 1900) {
			$tl[5] = $a;
			next;
		}
	}

	$time = &timelocal (@tl);

	return $time;
}

sub set_starttime() {
	my (@tl);
	my ($time);
	my ($date);

	# Preset an array of values in case some parts are not passed as
	# arguments.  This lets the date, etc, be omitted and default to
	# today.

	($date)=@_;
	@tl = localtime;

	$tl[5] = substr($date,  0, 4)   if(length($date) >= 4);
	$tl[4] = substr($date,  4, 2)-1 if(length($date) >= 6);
	$tl[3] = substr($date,  6, 2)   if(length($date) >= 8);
	$tl[2] = substr($date,  8, 2)   if(length($date) >= 10);
	$tl[1] = substr($date, 10, 2)   if(length($date) >= 12);
	$tl[0] = substr($date, 12, 2)   if(length($date) >= 14);

	$time = &timelocal (@tl);

	return $time;
}


sub showtime() {
	my($delta)=shift;
	my($oneday)=24*60*60;

	@now=localtime($starttime+$delta);
	if($delta > $oneday) {
		$result=sprintf("%d+",$delta/$oneday);
	} else {
		$result="";
	}
	$result.=sprintf("%d:%02d:%02d",$now[2],$now[1],$now[0]);
	return $result;
}

sub busytime() {
	my($busy)=shift;
	my($oneday)=24*60*60;

	if($busy > $oneday) {
		$days=int($busy/$oneday);
		$result=sprintf("%d+",$busy/$oneday);
		$busy-=$days*$oneday;
	} else {
		$result="";
	}
	$hours=int($busy/60/60);
	$busy-=$hours*60*60;
	$minutes=int($busy/60);
	$busy-=$minutes*60;
	$seconds=$busy;
	$result.=sprintf("%d:%02d:%02d",$hours,$minutes,$seconds);
	return $result;
}

