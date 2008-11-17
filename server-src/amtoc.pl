#!@PERL@ -w

# create a TOC (Table Of Content) file for an amanda dump

# Author: Nicolas.Mayencourt@cui.unige.ch

# release 3.1.4

# HISTORY
# 1.0 19??-??-?? nicolas@cui.unige.ch
#	don't remember :-)
# 2.0 1996-??-?? nicolas@cui.unige.ch
#	amanda 2.2.6 support
# 3.0 1999-02-17 Nicolas.Mayencourt@cui.unige.ch
#	major rewrite, incompatible with release 2.0, amanda 2.4 support
# 3.0.1 1999-02-17 oliva@dcc.unicamp.br
#	minor fixes for multi-tape runs
# 3.0.2 1999-02-28 martineau@IRO.UMontreal.CA
#	output the datestamp of each dump
# 3.0.3 1999-09-01 jrj@purdue.edu
#	allow multiple -s entries
# 3.0.4 1999-09-15 jrj@purdue.edu
#	handle an image failing on one tape...
# 3.1.0 1999-10-06 Nicolas.Mayencourt@cui.unige.ch
#	add new options (-i -t)
# 3.1.1 1999-10-08 Nicolas.Mayencourt@cui.unige.ch
#	print original size, instead of size-on-tape
# 3.1.2 1999-10-11 Nicolas.Mayencourt@cui.unige.ch
#	really print original size, instead of size-on-tape
# 3.1.3 Nicolas.Mayencourt@cui.unige.ch
#	correct a bug for total report
# 3.1.4 2000-01-14 dhw@whistle.com
#	Add a flag (-w) for vertical whitespace


#--------------------------------------------------------
sub pr($$$$$$$) { 
# you can update these proc if you want another formating
# format: filenumber  host:part  date  level  size
# If you use tabular option, modifie the format at the end of the code
  if (defined($tabular)) {
	$fnbr=$_[0];
	$hstprt=$_[1] . ":" . $_[2];
	$dt=$_[3];
	$lvl=$_[4];
	$sz=$_[5];
	$ch=$_[6];
    write($OF);
  } else {
    print $OF "$_[0]  $_[1]:$_[2]  $_[3]  $_[4]  $_[5]  $_[6]\n";
  }
}
#--------------------------------------------------------



#--------------------------------------------------------
sub tfn($) {
  # calculate tocfilename
  $_ = $_[0];
  foreach $s (@subs) {
    eval $s;
  }
  return $dir . $_ ;
}
#--------------------------------------------------------


#--------------------------------------------------------
sub usage($) {
  print STDERR "@_\n\n";
  print STDERR "usage: amtoc [-a] [-i] [-t] [-f file] [-s subs] [-w] [--] logfile\n";
  print STDERR "         -a      : file output to `label`.toc\n";
  print STDERR "         -i      : Start TOC with a small help message\n";
  print STDERR "         -t      : tabular output\n";
  print STDERR "         -f file : output to file\n";
  print STDERR "         -s subs : output file name evaluated to `eval \$subs`\n";
  print STDERR "         -w      : add vertical whitespace after each tape\n";
  print STDERR "         --      : last option\n";
  print STDERR "         logfile : input file ('-' for stdin)\n";
  exit;
}
#--------------------------------------------------------

#--------------------------------------------------------
sub init() {
  &usage("amtoc required at least 'logfile' parameter.") if ($#ARGV==-1) ;

  @subs = ();
  for ($i=0;$i<=$#ARGV;$i++) {
    if ($ARGV[$i] eq '-a') {
        push (@subs, "s/\$/.toc/");
      }
    elsif ($ARGV[$i] eq '-i') {
        $info=1;
      }
    elsif ($ARGV[$i] eq '-t') {
        $tabular=1;
      }
    elsif ($ARGV[$i] eq '-f') {
        $i++;
        &usage("'-f' option require 'file' parameter.")  if ($i > $#ARGV);
        $tocfilename=$ARGV[$i];
      }
    elsif ($ARGV[$i] eq '-s') {
        $i++;
        &usage("'-s' option require 'subs' parameter.")  if ($i > $#ARGV);
        push (@subs, $ARGV[$i]);
      }
    elsif ($ARGV[$i] eq '-w') {
        $vwspace=1;
      }
    elsif ($ARGV[$i] eq '--') {
      # no more options: next arg == logfile
        $i++;
        &usage("amtoc required at least 'logfile' parameter.") if ($i > $#ARGV);
        $logfile=$ARGV[$i];
        &usage("too many parameters.") unless ($i == $#ARGV);
      }
    else {
        $logfile=$ARGV[$i];
        &usage("too many parameters.") unless ($i == $#ARGV);
      }
  }
  &usage("amtoc required at least 'logfile' parameter.") unless ($logfile);
}

#--------------------------------------------------------

&init;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin";

$dir=$logfile;
$dir =~ s/[^\/]*$//;


if ($logfile eq '-') {$IF=STDIN} else 
  {die ("Cannot open logfile $logfile") unless open (IF,"$logfile");$IF=IF;}

$filenumber=0;
$tot_or_size=0;

while ( <$IF> ) {
  $line = $_;
  if ( /^FAIL dumper (\S+) (\S+)/ ) {
    next;
  }
  if ( /^SUCCESS dumper (\S+) (\S+)/ ) {
    $host = $1;
    $disk = $2;
    $line =~ /orig-kb (\d+)/;
    $osize{$host}{$disk} = $1;
    $tot_or_size += $osize{$host}{$disk};
    $fail{$host}{$disk} = 0;
    next;
  }
  if ( /^START amflush/ ) {
    $flash_mode = 1;
    next;
  }
  if ( ! /^([A-Z]+) taper (\S+) (\S+) (\S+) (\S+) (\S+)/) { next;}
  # $_ = $1;
  if (/PART taper/) {
    /^([A-Z]+) taper (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+)/;
    $filenum = $3;
    $host = $4;
    $disk = $5;
    $date = $6;
    $chunk = $7;
    $level = $8;
    if ($filenum != $filenumber) {
      # This should not be possible */
      $filenumber = $filenum;
    }
  } else {
    /^([A-Z]+) taper (\S+) (\S+) (\S+) (\S+) (\S+)/;
    $host = $2;
    $disk = $3;
    $date = $4;
    $chunk = $5;
    $level = $6;
  }
  switch: {
    /START taper/ && do {
      $tocfilename=&tfn($chunk) if ($#subs >= 0);
      if (!$tocfilename || ($tocfilename eq '-')) {$OF=STDOUT;}
      else {
          die ("Cannot open tocfile $tocfilename") unless open(OF,">$tocfilename");
          $OF=OF;
        }

	print $OF "\f" if ($vwspace && $filenumber);
	if (defined($info)) {
	  print $OF "AMANDA: To restore:\n";
	  print $OF "    position tape at start of file and run:\n";
	  print $OF "        dd if=<tape> bs=32k skip=1 [ | zcat ] | restore -...f\n";
	  print $OF "    or run: amrestore -p <tape> [host [partition]] | restore -...f\n";
	  print $OF "\n";
	}



      $filenumber=0;
      &pr("#","Server","/partition","date", "level","size[Kb]","part");
      &pr("$filenumber","$chunk","","$disk","-","-","-");
      last switch; };
    /^(?:SUCCESS|CHUNK|PART|DONE) taper/ && do {
      if(/SUCCESS/){
	$level = $chunk;
	$chunk = "-";
      }
      $filenum = $filenumber;
      if (/DONE/) {
	$chunk = "-";
	$filenumber--;
	$filenum = " ";
      }
      $mysize = 0;
      if(/ kb (\d+) /){
	$mysize = $1;
      }
      if ( $fail{$host}{$disk} ) {
        &pr("$filenum","${host}","${disk}","${date}","${level}","FAIL","${chunk}");
      } else {
  	if (defined($flash_mode)) {
          &pr("$filenum","${host}","${disk}","${date}","${level}","$mysize","${chunk}");
	} else {
  	  if (defined($osize{$host}{$disk}) && !/^CHUNK/ && !/^PART/) {
            &pr("$filenum","${host}","${disk}","${date}","${level}","$osize{$host}{$disk}","${chunk}");
	  } else {
	    $note = "";
	    if(!/^CHUNK/ && !/^PART/){
		# this case should never happend: 
	    } else {
	      $note = "*";
	    }
            &pr("$filenum","${host}","${disk}","${date}","${level}","$note$mysize","${chunk}");
	  }
	}
      }
      last switch;};
    /INFO taper retrying/ && do {
      --$filenumber;
      last switch; };
    /INFO taper tape .* \[OK\]/ && do {
      $line =~ / kb (\d+) /;
      $size = $1;
      $line =~ / fm (\d+) /;
      print "\n\n" if ($vwspace);
      &pr("$1","total","on_tape","-","-","$size","-");
      last switch; };
    /FAIL taper/ && do { next; };
  }
  $filenumber += 1;
}
if (defined($flash_mode)) {
  &pr("-","total","origin","-","not","available","-");
} else {
  &pr("-","total","origin","-","-","$tot_or_size","-");
}
close $IF;
close OF;


format OF =
@>>  @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< @<<<<<<<<<<<<<< @>> @>>>>>>>>
$fnbr,$hstprt,$dt,$lvl,$sz
.

format STDOUT =
@>>  @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< @<<<<<<<<<<<<<< @>> @>>>>>>>> @>>>
$fnbr,$hstprt,$dt,$lvl,$sz,$ch
.
