#!@PERL@
#
# Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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
#

use lib '@amperldir@';
use Getopt::Long;
use Time::Local;
use File::Copy;
use Socket;   # for gethostbyname
use Amanda::Paths;

my $confdir="$CONFIG_DIR";
my $tmpdir="$AMANDA_DBGDIR";
my $amandahomedir="$localstatedir/lib/amanda";
my $templatedir="$amdatadir/template.d"; #rpm install template files here
my $def_tapedev="file:$amandahomedir/vtapes";

my $amanda_user="@CLIENT_LOGIN@";
my $def_config="@DEFAULT_CONFIG@";
my $def_dtimeout="1800";
my $def_ctimeout="30";
my $def_etimeout="300";
my $def_perm=0750;
my $amanda_conf_perm=0600;
my $def_tapecycle=10;
my $config;
my $vtape_err=0;
my $holding_err=0;
my $template_only=0;
my $parentdir;
my $host;


#usage
sub usage {
        print "$0\n";
        print "\t\t <config> [--template <template>]\n";
	print "\t\t[--no-vtape] (do not create virtual tapes)\n";
        print "\t\t[--tapetype <tapetype>] [--tpchanger <tpchanger>]\n";
        print "\t\t[--tapedev <tapedev>] [--changerfile <changerfile>]\n";
        print "\t\t[--changerdev <changerdev>] [--labelstr <labelstr>] \n";
	print "\t\t[--mailto <mailto>] [--dumpcycle <dumpcycle> (ex: 5days, 1week or 2weeks)]\n";
	print "\t\t[--runspercycle <runspercycle>] [--runtapes <runtapes>]\n";
	print "\t\t[--tapecycle <tapecycle>]\n";
	print "\t\t[--help]\n";
}

#print and log
sub mprint {
    for $fh ( STDOUT, LOG ) {
	print $fh @_;
    }
}

sub log_and_die { 
    my ($err, $cleanup) = @_;
    print LOG $err;
    # clean up $config directory if cleanup=1
    # if error in creating vtape or holding disk, 
    # advise user to create manually, no need to cleanup
    if ( $cleanup && defined $config  && -e "$confdir/$config" ) {
	print LOG "cleaning up $confdir/$config\n";
	if ( -e "$confdir/$config/amanda.conf" ) {
	    unlink "$confdir/$config/amanda.conf" || 
	    print LOG "unlink $confdir/$config/amanda.conf failed: $!\n";
	}
	if ( -e "$confdir/$config/advanced.conf" ) {
	    unlink "$confdir/$config/advanced.conf" || 
	    print LOG "unlink $confdir/$config/advanced.conf failed: $!\n";
	}
	if ( -e "$confdir/$config/tapelist" ) {
	    unlink "$confdir/$config/tapelist" || 
	    print LOG "unlink $confdir/$config/tapelist failed: $!\n";
	}
	if ( -e "$confdir/$config/curinfo" ) {
	    rmdir "$confdir/$config/curinfo" || 
	    print LOG "rmdir $confdir/$config failed: $!\n";
	}
	if ( -e "$confdir/$config/index" ) {
	    rmdir "$confdir/$config/index" || 
	    print LOG "rmdir $confdir/$config/index failed: $!\n";
	}
	rmdir "$confdir/$config" || 
	    print LOG "rmdir $confdir/$config failed: $!\n";
    }
    die $err;
}


sub is_user_right {
    my $user = `whoami`;
    chomp($user);
    ( $user eq $amanda_user ) ||
	die ("ERROR: $0 must be run by $amanda_user\n", 0);
}


# rpm installation should have taken care of these. Create one if it's not there
sub check_gnutarlist_dir {
    if ( -e "$amandahomedir/gnutar-lists" ) {
	&mprint ("$amandahomedir/gnutar-lists directory exists\n");
    }
    else {
	mkdir ("$amandahomedir/gnutar-lists", $def_perm) ||
	    &log_and_die ("ERROR: mkdir:$amandahomedir/gnutar-lists failed: $!\n", 0);
    }
}

sub create_conf_dir {
  unless ( -e $confdir ) {
    &log_and_die ("ERROR: $confdir does not exist\n", 0);
  }
  unless ( -e "$confdir/$config" ) {
    mkdir ("$confdir/$config", $def_perm) ||
      &log_and_die ("ERROR: mkdir: $confdir/$config failed: $!\n", 0);	# $! = system error
  } else {
    &log_and_die ("ERROR: Configuration $config exists\n", 0);
  }
  unless ( -e "$confdir/template.d" ) {
    mkdir ("$confdir/template.d", $def_perm)  ||
      &log_and_die ("ERROR: mkdir: $confdir/template.d failed: $!\n", 0);
    &mprint ("$confdir/template.d directory created\n");
  }
}

sub copy_template_file {
    my $tplate = $_[0];
    unless ($tplate) {
	&log_and_die ("ERROR: template is missing\n", 1);
    }
    # create and update amanda.conf
    open(CONF, "$templatedir/amanda-$tplate.conf")
	|| &log_and_die ("ERROR: Cannot open $templatedir/amanda-$tplate.conf: $!\n", 1);
    open(NEWCONF, ">$confdir/$config/amanda.conf") ||
	&log_and_die ("ERROR: Cannot create $confdir/$config/amanda.conf: $!\n", 1);
    chmod ($amanda_conf_perm, "$confdir/$config/amanda.conf") ||
	&log_and_die ("ERROR: Cannot set amanda.conf file access permission: $!\n", 1);
    while (<CONF>) {
	$_ =~ s/$def_config/$config/;
	print NEWCONF $_;
    }
    close(CONF);
    close(NEWCONF);
    &mprint ("$confdir/$config/amanda.conf created and updated\n");
}


sub create_curinfo_index_dir {
    mkdir("$confdir/$config/curinfo", $def_perm) ||
	&log_and_die ("ERROR: mkdir: $confdir/$config/curinfo failed: $!\n", 1);
    mkdir("$confdir/$config/index", $def_perm) || 
	&log_and_die ("ERROR: mkdir: $confdir/$config/index failed: $!\n", 1);
    &mprint ("curinfo and index directory created\n");
}

sub touch_list_files {
    open (TLIST, ">$confdir/$config/tapelist")
	|| &log_and_die ("ERROR: Cannot create tapelist file: $!\n", 1);
    close (TLIST);
    &mprint ("tapelist file created\n");

    open (DLIST, ">$confdir/$config/disklist")
	|| &log_and_die ("ERROR: Cannot create disklist file: $!\n", 1);
    close (DLIST);
    &mprint ("disklist file created\n");
}

# create holding disk directory, check disk space first
sub create_holding { 
  if ( -d "$amandahomedir/holdings/$config" ) {
    my $uid = (stat("$amandahomedir/holdings/$config"))[4];
    my $owner = (getpwuid($uid))[0];
    unless ( $owner eq $amanda_user ) {
      &mprint ("WARNING: holding disk directory exists and is not owned by $amanda_user\n");
      $holding_err++;
    }
    return;
  }
    my $div=1;
    my $out = `df -k $amandahomedir`;
    my @dfout = split(" " , $out);
    unless ( $#dfout == 12 ) {	# df should output 12 elem
	&mprint ("WARNING: df failed, holding disk directory not created\n");
	$holding_err++;
	return;
    }
    unless (( $dfout[1] eq "1K-blocks" ) || ( $dfout[1] eq "kbytes")) {
         $div=2;	# 512-blocks displayed by df
     }
    
    if (( $dfout[10] / $div )  > 1024000 ) { # holding disk is defined 1000 MB
	&mprint ("creating holding disk directory\n");
	unless ( -d "$amandahomedir/holdings" ) { 
	mkdir ( "$amandahomedir/holdings", $def_perm) ||
	    (&mprint ("WARNING: mkdir $amandahomedir/holdings failed: $!\n"), $holding_err++, return );
    }
	mkdir ( "$amandahomedir/holdings/$config", $def_perm) ||
	    (&mprint ("WARNING: mkdir $amandahomedir/holdings/$config failed: $!\n"), $holding_err++, return) ;
    }
}

#create default tape dir
sub create_deftapedir{
    unless ( -e "$amandahomedir/vtapes" ) { 
	mkdir ( "$amandahomedir/vtapes", $def_perm) ||
	    ( &mprint ("WARNING: mkdir $amandahomedir/$config/vtapes failed: $!\n"), return );
    }
    unless ( -e "$amandahomedir/vtapes/$config" ) { 
	mkdir ( "$amandahomedir/vtapes/$config", $def_perm) ||
	    ( &mprint ("WARNING: mkdir $amandahomedir/vtapes/$config failed: $!\n"), return );
    }
	$parentdir="$amandahomedir/vtapes/$config";
}

# create and label vtape
sub create_vtape {
	&mprint ("creating vtape directory\n");
	if ($template_only==0){ #  check $template mode
		$mylabelprefix=$labelstr;   #set labelstr
		if ($tapedev eq "$def_tapedev/$config"){
		&create_deftapedir;
		}
		else {
		$tapedev=~/^(file:\/)/;
		$parentdir=$';
		}
	}
	else {
		$mylabelprefix=$config;
		&create_deftapedir;	
	}
	unless ( -e $parentdir){
		&mprint ("WARNING: tapedev $parentdir does not exists, vtapes creation failed!\n");
		&mprint ("Please create $parentdir and $confdir/$config and rerun the same command or else create vtapes manually.\n");
		$vtape_err++;
		return;
	}

	chdir ("$parentdir") ||
		( &mprint("WARNING: chdir $parentdir failed: $!\n"), $vtape_err++, return );
    my $i;
    &mprint ("amlabel vtapes\n");
	if (defined $tapecycle) {
		$tapecycle=~/^\d+/; 
		$tp_cyclelimit=$&;
			# check space
		my $dfout =`df $parentdir`;
		my $mul=1024;
		@dfdata=split(" ",$dfout);
		unless ( $dfdata[1] eq "1K-blocks" ) {
			$mul=512;	# 512-blocks displayed by df
		}
		if (($dfdata[10]*$mul) < (($tp_cyclelimit*73728)+10240)){
			&mprint ("WARNING: Not enough space for vtapes. Creation of vtapes failed\n");
			$vtape_err++;
			return;
		}
	}
	else {
		$tp_cyclelimit=$def_tapecycle;
	}

	for $i (1..$tp_cyclelimit) {
		unless ( -e "slot$i"){
		mkdir ("slot$i", $def_perm) ||
		( &mprint ("WARNING: mkdir $parentdir/slot$i failed: $!\n"), $vtape_err++, return);
		}
		( @amlabel_out = `$sbindir/amlabel -f $config $mylabelprefix-$i slot $i`) ||
	    ( &mprint ("WARNING: amlabel vtapes failed at slot $i: $!\n"), $vtape_err++, return);
    }
	foreach (@amlabel_out) {
	  print LOG;
        }
	# reset tape to the first slot
	`$sbindir/amtape $config reset`;
}

sub create_customconf{
	   # now create a custom amanda.conf from user input
	unless ( $mailto ) 
	{ $mailto="$amanda_user"; }
	else {  # untaint mailto which can be evil
                # reject mailto with the following * ( ) < > [ ] , ; : ! $ \ / "
	    if ( $mailto =~ /^([^\*\(\)<>\[\]\,\;\:\!\$\\\/\"]+)$/ ) {
		$mailto = $1;                      #  now untainted
	    } else {
		&log_and_die ("ERROR: Invalid data in mailto.\n");  # log this somewhere
	    }
	}
	unless ( $dumpcycle ) { $dumpcycle="1 week"; }
	unless ( $runspercycle ) { $runspercycle="5"; }
	unless ( $tapecycle ) { $tapecycle="10 tapes"; }
	unless ( $runtapes ) { $runtapes="1"; }
	unless ( $labelstr ) {
	  if ($template eq "harddisk") {
	    $labelstr="$config";
	  } else {
	    $labelstr="^$config-[0-9][0-9]*\$";
	  }
	}
	if ((!(defined($template)))||($template eq "harddisk"))	
	  {
		if (defined $tapedev){
		$tapedev="file:/".$tapedev;
		}
		unless ( $tpchanger ) { $tpchanger="chg-disk"; }
		unless ( $tapedev ) { $tapedev="$def_tapedev/$config"; }
		unless ( $changerfile ) { $changerfile="$confdir/$config/changer.conf"; }
		unless ( $changerdev ) { $changerdev="/dev/null";}
		unless ( $tapetype ) { $tapetype="HARDDISK"; }	
	  }
	elsif ($template eq "single-tape")
	  {
		unless ($tpchanger) {$tpchanger="chg-manual";}
		unless ($tapedev)     {$tapedev="/dev/nst0";}
		unless ($changerfile) {$changerfile="$confdir/$config/chg-manual.conf";}
		unless ($changerdev) {$changerdev="/dev/null";}
		unless ($tapetype) {$tapetype="HP-DAT";}
	  }
	elsif ($template eq "tape-changer") 
          {
		unless ($tpchanger){$tpchanger="chg-zd-mtx";}
		unless ($tapedev){ $tapedev="/dev/nst0";}
		unless ($changerfile){$changerfile="$confdir/$config/changer.conf";}
		unless ($changerdev) {$changerdev="/dev/sg1";}
		unless ($tapetype)  {$tapetype="HP-DAT";}
          }
        else # S3 case
	  {
	    unless ($tpchanger){$tpchanger="chg-multi";}
	    unless ($changerfile){$changerfile="$confdir/$config/changer.conf";}
	    unless ($tapetype)  {$tapetype="HP-DAT";}
	  }


	open (CONF, ">$confdir/$config/amanda.conf") ||
	    &log_and_die ("ERROR: Cannot create amanda.conf file: $!\n", 1);
	chmod ($amanda_conf_perm, "$confdir/$config/amanda.conf") ||
	    &log_and_die ("ERROR: Cannot set amanda.conf file access permission: $!\n", 1);

	print CONF "org \"$config\"\t\t# your organization name for reports\n";
	print CONF "mailto \"$mailto\"\t# space separated list of operators at your site\n";
	print CONF "dumpcycle $dumpcycle\t\t# the number of days in the normal dump cycle\n";
        print CONF "runspercycle $runspercycle\t\t# the number of amdump runs in dumpcycle days\n";
	print CONF "tapecycle $tapecycle\t# the number of tapes in rotation\n"; 
	print CONF "runtapes $runtapes\t\t# number of tapes to be used in a single run of amdump\n";
	print CONF "tpchanger \"$tpchanger\"\t# the tape-changer glue script\n";
	print CONF "tapedev \"$tapedev\"\t# the no-rewind tape device\n";
	print CONF "changerfile \"$changerfile\"\t# tape changer configuration parameter file\n";
	print CONF "changerdev \"$changerdev\"\t# tape changer configuration parameter device\n";
	print CONF "tapetype $tapetype\t# what kind of tape it is\n";
	print CONF "labelstr \"$labelstr\"\t# label constraint regex: all tapes must match\n";
	print CONF "dtimeout $def_dtimeout\t# number of idle seconds before a dump is aborted\n";
	print CONF "ctimeout $def_ctimeout\t# max number of secconds amcheck waits for each client\n";
	print CONF "etimeout $def_etimeout\t# number of seconds per filesystem for estimates\n";
	print CONF "define dumptype global {\n";
	print CONF "       comment \"Global definitions\"\n";
	print CONF "       auth \"bsdtcp\"\n}\n";
	print CONF "define dumptype gui-base {\n";
	print CONF "       global\n";
	print CONF "       program \"GNUTAR\"\n";
	print CONF "       comment \"gui base dumptype dumped with tar\"\n";
	print CONF "       compress none\n";
	print CONF "       index yes\n}\n";
	if ($tapetype eq "HARDDISK") {
	  print CONF "define tapetype HARDDISK {\n";
	  print CONF "       comment \"Virtual Tapes\"\n";
	  print CONF "       length 5000 mbytes\n}\n";
	}
	print CONF "includefile \"advanced.conf\"\n";
	print CONF "includefile \"$confdir/template.d/dumptypes\"\n";
	print CONF "includefile \"$confdir/template.d/tapetypes\"\n";
	close (CONF);
	mprint ("custom amanda.conf created\n");
  }


sub check_xinetd{
    &mprint ("/var/lib/amanda/example/xinetd.amandaserver contains the latest Amanda server daemon configuration.\n");
    &mprint ("Please merge it to /etc/xinetd.d/amandaserver.\n");
}


sub build_amanda_ssh_key{
  if ( -e "$amandahomedir/.ssh/id_rsa_amdump.pub" ) {
    if ( -e "$amandahomedir/.ssh/client_authorized_key" ) {
      &mprint ("$amandahomedir/.ssh/client_authorized_keys exists.\n");
    }
    else {
      open(NEWAUTH, ">$amandahomedir/.ssh/client_authorized_keys") ||
	(&mprint("WARNING: open $amandahomedir/.ssh/client_authorized_key failed: $!\n"), return);
      open(PUB, "$amandahomedir/.ssh/id_rsa_amdump.pub") ||
	(&mprint("WARNING: open $amandahomedir/.ssh/id_rsa_amdump.pub failed: $!\n"), return);
      print NEWAUTH "from=\"$host\",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,command=\"/usr/lib/amanda/amandad -auth=ssh amdump\" ";
      while (<PUB>) {
      print NEWAUTH;
    }
      close NEWAUTH;
      close PUB;
      &mprint("$amandahomedir/.ssh/client_authorized_keys created. Please append to /var/lib/amanda/.ssh/authorized_keys file on Amanda clients\n");
      }
  }
}

sub copy_chg_manual_conf {
  if ( $template eq "single-tape" && !defined $changerfile && !defined $tpchanger)
    {
      my $my_changerfile="$confdir/$config/chg-manual.conf";
      copy("$templatedir/chg-manual.conf", $my_changerfile) ||
	&mprint ("copy $templatedir/chg-manual.conf to $my_changerfile failed: $!\n");
    }
}

#main
my $ret=0;

$ret = GetOptions ("template=s"=>\$template,
		   "no-vtape!"=>\$novtape,
	      "tapetype=s"=>\$tapetype,
	      "tpchanger=s"=>\$tpchanger,
	      "tapedev=s"=>\$tapedev,
	      "changerfile=s"=>\$changerfile,
	      "changerdev=s"=>\$changerdev,
	      "labelstr=s"=>\$labelstr,
	      "mailto=s"=>\$mailto,
	      "dumpcycle=s"=>\$dumpcycle,
	      "runspercycle=i"=>\$runspercycle,
	      "runtapes=i"=>\$runtapes,
	      "tapecycle=i"=>\$tapecycle,
	      "help!"=>\$help
	      );

unless ( $ret ) {
    &usage;
    exit 1;
}

if($help) {
    &usage;
    exit 0;
}

unless ( $#ARGV == 0 ) {
    print STDERR "ERROR: config name is required.\n";
    &usage;
    exit 1;
}
else {
    if ( "$ARGV[0]" =~ /^([-\@\w.]+)$/ ) {
	$config = $1;                   #  now untainted
    } else {
	die ("ERROR: Invalid data in config name.\n");  # log this somewhere
    }
}


$oldPATH = $ENV{'PATH'};

$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin:/usr/ucb"; # force known path
delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV'};
$date=`date +%Y%m%d%H%M%S`;
chomp($date);
my $logfile="$tmpdir/amserverconfig.$date.debug";

&is_user_right;
unless ( -e "$tmpdir" ) {
    mkdir ("$tmpdir", $def_perm) ||
	die ("ERROR: mkdir: $tmpdir failed: $!\n");
}

open (LOG, ">$logfile") || die ("ERROR: Cannot create logfile: $!\n");
print STDOUT "Logging to $logfile\n";

my $lhost=`hostname`;
chomp($lhost);
# get our own canonical name, if possible (we don't sweat the IPv6 stuff here)
$host=(gethostbyname($lhost))[0];

unless ( $host ) {
    $host = $lhost;  #gethostbyname() failed, go with hostname output
}


my $need_changer = 0;
if ( defined $template ) {

    # validate user input to template
    chomp($template);
    my $found = 0;
    @valid_templates = ( "harddisk", "single-tape", "tape-changer", "s3" );
    foreach $elt (@valid_templates) {
        if ( $elt eq lc($template) ) {
            $found = 1;
            last;
        }
    }
    unless ($found) {
        print STDERR
            "valid inputs to --templates are harddisk, single-tape, tape-changer or S3\n";
        &usage;
        exit 1;
    }

    # if tape-changer is chosen, check if mtx is installed
    if ( $template eq "tape-changer" ) {
        my $ok = 0;
        for $dir ( "/usr/sbin", "/usr/local/sbin", "/usr/local/bin",
            "/usr/bin", "/bin", "/opt/csw/sbin", split( ":", $oldPATH ) )
        {
            if ( -e "$dir/mtx" ) {
                $ok = 1;
                last;
            }
        }
        unless ($ok) {
            &mprint(
                "ERROR: mtx binary not found, tape-changer template will not work and is not installed.\n"
            );
            &log_and_die(
                "ERROR: Please install mtx and rerun the same command.\n",
                0 );
        }
	$need_changer = 1;
    }
    elsif ( $template eq "S3" ) {
	$need_changer = 1;
    }

}

&create_conf_dir;

if ($need_changer) {
    unless ($changerfile) {
	$changerfile = "$confdir/$config/changer.conf";
    }
    open( CCONF, ">$changerfile" )
	|| &log_and_die( "ERROR: Cannot create $changerfile: $!\n", 1 );
    close(CCONF);
}

&check_gnutarlist_dir;

# copy dumptypes and tapetypes files if none exists.
my $dtype="$confdir/template.d/dumptypes";
my $ttype="$confdir/template.d/tapetypes";

unless ( -e $dtype ) {
    copy("$templatedir/dumptypes", $dtype ) ||
    &log_and_die ("ERROR: copy dumptypes failed: $!\n", 1);
}


unless ( -e $ttype ) {
    copy("$templatedir/tapetypes", $ttype ) ||
    &log_and_die ("ERROR: copy tapetypes file to $ttype failed: $!\n", 1);
}



# update $def_config value to the specified config value in advanced.conf
    open(ADV, "$templatedir/advanced.conf") || &log_and_die ("ERROR: Cannot open advanced.conf file: $!\n", 1);
    open(NEWADV, ">$confdir/$config/advanced.conf") || 
	&log_and_die ("ERROR: Cannot create advanced.conf file: $!\n", 1);
    while (<ADV>) {
	$_ =~ s/$def_config/$config/;
	print NEWADV $_;
    }
    close(ADV);
    close(NEWADV);
    &mprint ("$confdir/$config/advanced.conf created and updated\n");


&create_curinfo_index_dir;
&touch_list_files;


if ( defined $template ) {
# if any other parameters are provided, create a workable custom config
	if ( defined $tapetype || defined $tpchanger || defined $tapedev
	 || defined $changerdev || defined $labelstr || defined $mailto || defined $dumpcycle
	 || defined $runspercycle || defined $runtapes || defined $tapecycle ) {
		&mprint("Creating custom configuration using templates\n");
		create_customconf();
		if ( $template ne "harddisk" ) {
		  &create_holding;
		} else {
		  if (defined $labelstr) {
		    if ($labelstr=~/^([-\w.]+)$/) {
		      &create_vtape unless ( defined $novtape );
		    } else {
		      &mprint ("WARNING: Only alphanumeric string is supported in labelstr when using template to create vtapes. ");
		      &mprint ("If you want to use regex in labelstr, please create vtapes manually.\n");
		    }
		  }
		}
	      } else {
		$template_only=1;
		$tapedev="$def_tapedev/$config";
		&copy_template_file($template);
		if ($template ne "harddisk") {
		  unless ( -e "$amandahomedir/holdings/$config" ) {
		    &create_holding;
		  }
		} else {  # harddisk and template only
		  unless ( -e "$amandahomedir/vtapes/$config" || defined $novtape ) {
		    &create_vtape;
		  }
		}
	      }
	&copy_chg_manual_conf;
      } else {
&create_customconf;
}

&check_xinetd;
&build_amanda_ssh_key;

if ( $vtape_err ) {
  &mprint("Error in creating virtual tape, please check log and create virtual tape manually.\n");
  exit 1;
}

if ( $holding_err ) {
  &mprint("Error in creating holding disk, please check log and create holding disk manually.\n");
  exit 1;
}



if ( $vtape_err==0 && $holding_err==0) {
  &mprint("DONE.\n");
  exit 0;
}


$ENV{'PATH'} = $oldPATH;


# THE END
