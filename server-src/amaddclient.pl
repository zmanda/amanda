#!@PERL@
#
# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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


use Getopt::Long;
use Time::Local;
use File::Copy;
use Socket;   # for gethostbyname

my $confdir="@CONFIG_DIR@";
my $tmpdir="@AMANDA_DBGDIR@";

my $prefix="@prefix@";
my $localstatedir="@localstatedir@";
my $amandahomedir="$localstatedir/lib/amanda";

my $amanda_user="@CLIENT_LOGIN@";
my $amanda_group="disk";
my $def_root_user="root";
my $def_dumptype="user-tar";

my $sp_diskfile=0;

sub usage {
        print "$0\n";      
        print "\t\t--config <config>         Required. Ex: DailySet1\n";       
	print "\t\t--client <FQDN-name>      Required. Ex: server.zmanda.com\n";
        print "\t\t--diskdev <directory>     Required. Ex: /home\n";
	print "\t\t--m                       Modify exisiting entry\n";
	print "\t\t[--dumptype <dumptype>    Default: user-tar]\n";
        print "\t\t[--includefile <string>   glob expression of file(s) to include]\n";
	print "\t\t[--includelist <file>     file contains glob expressions to include]\n";
	print "\t\t[ specify either --includefile or --includelist ]\n";
	print "\t\t[--excludefile <string>   glob expression of file(s) to exclude]\n";
	print "\t\t[--excludelist <file>     file contains glob expressions to exclude]\n";
	print "\t\t[ specify either --excludefile or --excludelist ]\n";
	print "\t\t[--user <username>          name of user running amrecover on the client]\n";
	print "\t\t[--auth <string>            authentication used when running amrecover]\n";
	print "\t\t[--gnutar_list_dir <string> directory where gnutar keep its state file on the client]\n";
	print "\t\t[--amandates <string>       file where amanda keep the date of dumplevel on the client]\n";
	print "\t\t[--batch                    batch mode used when copying file to client]\n";
	print "\t\t[--no-client-update         do not update files on the client";
        print "\t\t[--help]\n";
}

sub mprint {
    for $fh ( STDOUT, LOG ) {
	print $fh @_;
    }
}

sub log_and_die {
    print LOG @_;
    die @_;
}

sub is_user_right {
    my $user = `whoami`;
    chomp($user);
    ( $user eq $amanda_user ) ||
	die ("ERROR: $0 must be run by $amanda_user\n");
}


# alphabetics, numerics, and underscores, 
# hyphen, at sign, dot and "/" are ok
sub is_tainted { 
 local($arg) = @_;
 if ( $arg  =~ /^([-\@\/\w.]+)$/ ) {
        return 0; # ok
    } else {
        return 1; # bad, tainted input
    }
}


# modify existing entry. 
# only got here if -m is used and entry is found.
sub dle_mod {
    my $open_seen=0;     # '{' is seen
    my $include_done=0;  # original include line is parsed
    my $exclude_done=0;  # original exclude line is parsed
    my $ok=0;            # 1 if target entry is found

    @ARGV = ("$confdir/$config/disklist");
    $^I = ".tmp"; # allow inplace editing
    while (<>) {
	my ($one, $two, $three ) = split(/\s+/, $_);

	# if include or exclude is not previously there, 
	# take care of them here
	if ( $one eq "}" ) {
	    $open_seen=0;
		if ( $include_done==0 && $ok ) {
		   print "include list \"$includelist\"\n" if ( $includelist );
		   print "include file \"$includefile\"\n" if ( $includefile );
		    }
		if ( $exclude_done==0 && $ok ) {
		   print "exclude list \"$excludelist\"\n" if ( $excludelist );
		   print "exclude file \"$excludefile\"\n" if ( $excludefile );
		    }
	    $ok=0; # reset, done with one entry
	}   
	
	# take care of entry that has '{'
	if ( $open_seen==1 ) {
	    if ( !$two  && !$three ) {   # inside {, dumptype line has 1 field only
		s/$one/$dumptype/ if ( $dumptype );
	    } elsif ( $two && $three ) { # inside {, include/exclude line
		if ( $one eq "include" ) {
		    if ( $includelist ) {
			s/$two.*$/list "$includelist"/;
		    } elsif ( $includefile ) {
			s/$two.*$/file "$includefile"/;
		    }
		    $include_done=1;
		}
		if ( $one eq "exclude" ) {
		    if ( $excludelist ) {
			s/$two.*$/list "$excludelist"/;
		    } elsif ( $excludefile ) {
			s/$two.*$/file "$excludefile"/;
		    }
		    $exclude_done=1;
		}

	    }
	}  # inside '{'
	
	# entry which previously doesn't have include/exclude
	if (( $one eq $client ) && ($two eq $diskdev) ) {
	    $ok=1;
	    if ( $three && ($three ne "{") ) {
		if ( $sp_diskfile==1 ) {  #previously don't have include/exclude
		    $three = $dumptype if ( $dumptype );
		    $includeline="include list \"$includelist\""   if ( $includelist );
		    $includeline="include file \"$includefile\""   if ( $includefile );
		    $excludeline="exclude list \"$excludelist\"\n" if ( $excludelist );
		    $excludeline="exclude file \"$excludefile\"\n" if ( $excludefile );
		    s/$three/{\n$three\n$includeline\n$excludeline}/;
		} else {
		    s/$three/$dumptype/ if ( $dumptype ); #easy one, just replace dumptype.
		    $ok=0; #done with one entry
		}
	    } else {
		$open_seen=1;
	    }
	}
	print;
    }  # while loop
    unlink("$confdir/$config/disklist.tmp");
    exit 0;
}
    


#main
my $ret=0;
          
$ret= GetOptions (      "config=s"=>\$config,
                        "client=s"=>\$client,
                        "diskdev=s"=>\$diskdev,
                        "dumptype=s"=>\$dumptype,
                        "includefile=s"=>\$includefile,
                        "includelist=s"=>\$includelist,
                        "excludefile=s"=>\$excludefile,
                        "excludelist=s"=>\$excludelist,
			"user=s"=>\$root_user,
			"auth=s"=>\$auth,
			"gnutar_list_dir=s"=>\$tarlist,
			"amandates=s"=>\$amandates,
			"batch!"=>\$batch,
			"m!"=>\$mod,
			"no-client-update!"=>\$no_client_update,
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

unless (defined $config && defined $client && defined $diskdev ) {
    print STDERR "--config, --client and --diskdev are required.\n";
    &usage;
    exit 1;
}
else {
    die ("ERROR: Invalid data in config.\n")  if is_tainted($config);
    die ("ERROR: Invalid data in client.\n")  if is_tainted($client);
}


if ( defined $includefile && defined $includelist ) {
    print STDERR "Specify either --includefile or --includelist, not both.\n";
    &usage;
    exit 1;
}
   
if ( defined $excludefile && defined $excludelist ) {
    print STDERR "Specify either --excludefile or --excludelist, not both.\n";
    &usage;
    exit 1;
}   

$oldPATH = $ENV{'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin:/usr/ucb"; # force known path
$date=`date +%Y%m%d%H%M%S`;
chomp($date);
my $logfile="$tmpdir/amaddclient.$date.debug";

&is_user_right;
open (LOG, ">$logfile") || die "ERROR: Cannot create logfile : $!\n";
print STDOUT "Logging to $logfile\n";

my $lhost=`hostname`;
chomp($lhost);
# get our own canonical name, if possible (we don't sweat the IPv6 stuff here)
my $host=(gethostbyname($lhost))[0];

unless ( $host ) {
    $host = $lhost;  #gethostbyname() failed, go with hostname output
}


my $found=0;
my $fhs;

# make sure dumptype is defined in dumptypes or amanda.conf file

if ( defined $dumptype ) { 
for $fhs ( "$confdir/template.d/dumptypes", "$confdir/$config/amanda.conf" ) {
    open (DTYPE, $fhs) ||
	&log_and_die ("ERROR: Cannot open $fhs file : $!\n");
    while (<DTYPE>) {
	if (/^\s*define\s*dumptype\s*$dumptype\s*{/) {
	    $found=1;
	    last;
	}
	}
	close (DTYPE);
    }

    unless ( $found ) {
        &log_and_die ("ERROR: $dumptype not defined in $confdir/template.d/dumptypes or $confdir/$config/amanda.conf\n");
    }
}

# create disklist file
    unless ( -e "$confdir/$config"  ) {
	&log_and_die ("ERROR: $confdir/$config not found\n");
    }
    $found=0;
    if (defined $includefile || defined $includelist 
		    || defined $excludefile || defined $excludelist) {
	$sp_diskfile=1;
	}

    unless ( -e "$confdir/$config/disklist" ) {	 # create it if necessary
        open (DLE, ">$confdir/$config/disklist") || 
	    &log_and_die ("ERROR: Cannot create $confdir/$config/disklist file : $!\n");
	print DLE "#This file is generated by amaddclient.\n";
	print DLE "#Don't edit it manually, otherwise, 'amaddclient -m ...' might not work\n";
    }

    open (DLE, "+<$confdir/$config/disklist")    # open for read/write
	|| &log_and_die ("ERROR: Cannot open $confdir/$config/disklist file : $!\n");
    while (<DLE>) {
	my ($lclient, $ldiskdev, $dontcare ) = split(/\s+/, $_);
	if (( $lclient eq $client ) && ($ldiskdev eq $diskdev) ) {
	    $found=1;
	    last;
	}
    }

# if found and -m, do modification and exit 
    if ( defined $mod ) {
	if ( $found ) {
	&dle_mod;
    } else {
	&log_and_die ("ERROR: $client $diskdev not found, cannot modify\n");
    }
    }

unless ( defined $dumptype ) {
    $dumptype=$def_dumptype;
} 
    if ( $found==1 ) {
	&mprint("$confdir/$config/disklist has '$client $diskdev ...' entry, file not updated\n"); }
    else {
	print DLE "$client  $diskdev ";
	print DLE "{\n$dumptype\n" if ($sp_diskfile);
	if ( defined $includefile ) {
	    print DLE "include file \"$includefile\"\n";
	}
	elsif ( defined $includelist ) {
	    print DLE "include list \"$includelist\"\n";
	}
	if ( defined $excludefile ) {
	    print DLE "exclude file \"$excludefile\"\n";
	}
	elsif ( defined $excludelist ) {
	    print DLE "exclude list \"$excludelist\"\n";
	}
        print DLE "}\n" if ($sp_diskfile);

        print DLE "  $dumptype\n" if ($sp_diskfile==0);
	&mprint ("$confdir/$config/disklist updated\n");
	close (DLE);
    }


# update .amandahosts on server and client
    my $scp="scp";
    my $scp_opt1="-p";   # p: preserve mode
    my $scp_opt2="-o ConnectTimeout=15";   #timeout after 15 seconds
    my $ssh="ssh";
    my $ssh_opt="-x"; # -x as a placeholder, otherwise ssh complains
    my $mkdir="mkdir -p";
    my $client_conf_dir="$confdir/$config";
    my $amanda_client_conf="$client_conf_dir/amanda-client.conf";
    my $file="$amandahomedir/.amandahosts";
    my $client_file="$amandahomedir/amanda-client.conf-$client";
   
   if ( defined $batch ) {
    $scp_opt1="-Bp";
    $ssh_opt="-o BatchMode=yes";
  }
    
    &mprint ("updating $file on $host\n");
    unless ( defined $root_user ) {
    $root_user=$def_root_user;
  }
    $found=0;
    open (HFILE, "+<$file") 
	|| &log_and_die ("ERROR: Cannot open $file : $!\n");
	
	while (<HFILE>) {
	    if (/^\s*$client\s*$root_user\s*amindexd\s*amidxtaped\s/) {
		$found=1;
		last;
	    }
	}
    if ( $found==1 ) {
	&mprint ("$file contains $client $root_user, file not updated\n") ; }
    else {
	print HFILE "$client  $root_user amindexd amidxtaped\n";
	close (HFILE);
    }

# update client .amandahosts
unless ( $no_client_update ) {
     
    &mprint ("Attempting to update $file on $client\n");

    chdir ("$amandahomedir");
    system "$scp", "$scp_opt1", "$scp_opt2", "$amanda_user\@$client:$file", "$file.tmp";
    $exit_value  = $? >> 8;
    if ( $exit_value !=0 ) {
	&mprint ("WARNING: $scp from $client not successful.\n");
	&mprint ("Check $client:$file file.\n");
	&mprint ("If entry '$host $amanda_user' is not present,\n");
	&mprint ("append the entry to the file manually.\n");
    }
    else { 
    $found=0;
    unless ( -e "$file.tmp" ) {
	&mprint ("WARNING: $file.tmp not found\n"); }
    else {
	open (CFILE, "+<$file.tmp") 
	    || &log_and_die ("ERROR: Cannot open $file.tmp file : $!\n");
	while (<CFILE>) {
	    if (/^\s*$host\s*$amanda_user\s*amdump\s/) {
		$found=1;
		last;
	    }
	}
	if ( $found==1 ) {
	    &mprint ("$file contains $host $amanda_user, file not updated\n") ; }
	else {
	    print CFILE "$host  $amanda_user amdump\n";
	    close (CFILE);
	    
	    #make sure permission mode is correct
	    chmod (0600, "$file.tmp");
	    system "$scp", "$scp_opt1", "$scp_opt2", "$file.tmp", "$client:$file";
	    $exit_value  = $? >> 8;
	    if ( $exit_value !=0 ) {
		&mprint ("WARNING: $scp to $client not successful.\n");
		&mprint ("Check $client:$file file.\n");
		&mprint ("If entry '$host $amanda_user amdump' is not present,\n");
		&mprint ("append the entry to the file manually.\n");
	    }
    
	} 
    }
    unlink ("$file.tmp") || &mprint("unlink $file.tmp failed: $!\n");
    &mprint ("$client:$file updated successfully\n");
  }
  }

# done updating client .amandahosts

#create amanda-client.conf and scp over to client

unless ( $no_client_update ) {
&mprint ("Creating amanda-client.conf for $client\n");

$auth="bsdtcp" unless ( defined $auth ); 

open (ACFILE, ">$client_file") || &log_and_die ("ERROR: Cannot open $client_file file : $!\n");
 print ACFILE "#amanda-client.conf - Amanda client configuration file.\n";
 print ACFILE "conf            \"$config\"\n";
 print ACFILE "index_server    \"$host\"\n";
 print ACFILE "tape_server     \"$host\"\n";
 print ACFILE "#  auth  - authentication scheme to use between server and client.\n";
 print ACFILE "#          Valid values are 'bsdtcp' or 'ssh'\n";
 print ACFILE "auth            \"$auth\"\n";
 print ACFILE "# ssh keys file if ssh auth is used\n";
 print ACFILE "ssh_keys        \"$amandahomedir/.ssh/id_rsa_amrecover\"\n";
 print ACFILE "gnutar_list_dir \"$tarlist\"\n" if ( defined $tarlist );
 print ACFILE "amandates       \"$amandates\"\n" if ( defined $amandates ); 

close (ACFILE);
&mprint ("Creating  $client_conf_dir on $client\n");
system "$ssh", "$ssh_opt", "$amanda_user\@$client", "$mkdir", "$client_conf_dir";
$exit_value  = $? >> 8;
if ( $exit_value !=0 ) {
  &mprint ("WARNING: Cannot create $client_conf_dir on $client\n");
  &mprint ("Please copy $client_file to $client manually\n");
} else { 
  chmod (0600, "$client_file");
  system "$scp", "$scp_opt1", "$scp_opt2", "$client_file", "$amanda_user\@$client:$amanda_client_conf";
  $exit_value  = $? >> 8;
  if ( $exit_value !=0 ) {
    &mprint ("WARNING: Cannot copy $client_file to $client\n");
    &mprint ("Please copy $client_file to $client:$client_conf_dir manually\n");
  } else {
    &mprint ("Copy $client_file to $client successfully\n");
    unlink($client_file);
  }      
}
}

#create gnutar_list_dir
if ( defined $tarlist && !defined $no_client_update ) {
 system "$ssh", "$ssh_opt", "$amanda_user\@$client", "$mkdir", "$gnutar_list_dir";
 $exit_value  = $? >> 8;
if ( $exit_value !=0 ) {
  &mprint ("WARNING: Cannot create $gnutar_list_dir on $client\n"); 
  &mprint ("Please create $gnutar_list_dir on $client manually\n");
} else { 
  &mprint ("$client_file created on $client successfully\n");
}
}

&mprint ("File /var/lib/amanda/example/xinetd.amandaclient contains the latest Amanda client daemon configuration.\n");
&mprint ("Please merge it to /etc/xinetd.d/amandaclient.\n");
 
$ENV{'PATH'} = $oldPATH;
close (LOG);

#THE END				       
    
