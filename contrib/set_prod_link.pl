#!/usr/local/bin/perl
# ========================================================================
# @(#) $Id: set_prod_link.pl,v 1.3 2006/05/25 01:47:13 johnfranks Exp $
# ------------------------------------------------------------------------
# $Source: /cvsroot/amanda/amanda/contrib/set_prod_link.pl,v $
# ------------------------------------------------------------------------
# Description:
#   
#   When installing AMANDA with the option --with-suffix you can use this
#   script to set the a symbolic link from the productive name of the
#   files to this special version.
#   This way you can switch on the fly, from one version to an other.
#
#   Actually I would advice to use the option --prefix and install the
#   the whole software in different paths.
#
#   But if you want for example install a new version in the same
#   directory you can use it.
#
# -----------------------------------------------------------------------
# Author: Ricardo Malta, rmalta@bigfoot.com
# -----------------------------------------------------------------------
# History:
#
# Revision 1.3  2006/05/25 01:47:13  johnfranks
# Allow spaces and arbitrary binary characters in file names
# and configuration files.
#
# 64-bit / type portability clean code.
#
# Add 'make lint' options to appropriate Makefiles.
#
# Fully lint clean code using Sun's lint, and splint code checkers.
#
# Various bug fixes that have not been pushed.
#
# Modified Files:
# 	Modified most of the files...
#
# Revision 1.2  1999/11/02 21:30:10  oliva
# * contrib/set_prod_link.pl: Create the links for a configuration
# with --with-suffix.
#
#
# ========================================================================

$debug = 0;

if ($ARGV[0] ne "doit") {
	print <<"EOD";
usage: $0 doit

	Go to the directory where you have compiled AMANDA.
	Call this programm with the parameter \"doit\".

EOD
	exit 1;
}

# ------------------------------------------------------------------------
# Open the Makefile and search for the entries we need for doing the job.
# ------------------------------------------------------------------------
open(FD,"<Makefile") || die "Cannot open Makefile: $!\n";
while (<FD>) {
	$suffix  = (split(/\s*,\s*/))[2] if /^\s*transform\s*=/;
	$rootdir = (split(/=\s*/))[1] if /^\s*prefix\s*=/;
	last if $suffix && $suffix;
}
close(FD);
chomp $rootdir;
die "Cannot find line containing \"transform =\" in Makefile.\n" if (!$suffix);
die "Cannot find line containing \"prefix =\" in Makefile.\n" if (!$rootdir);

# ------------------------------------------------------------------------
# Last chance ....
# ------------------------------------------------------------------------
print "Starting setting the links to productive version:
    Directory: $rootdir
    Suffix   : $suffix
Confirm with <yes>: ";
chomp($dummy = <STDIN>);
die "\nAborting ...\n" if ($dummy ne "yes");
print "\n";

# ------------------------------------------------------------------------
# Now do the job
# ------------------------------------------------------------------------
$CUR_DIR = "$rootdir";
Make_Prod_Link($rootdir,$suffix) || die "Cannot create links under $rootdir\n";

# ------------------------------------------------------------------------
# We are done ... get out of here
# ------------------------------------------------------------------------
exit 0;

# ************************************************************************
#                    F U N C T I O N S
# ************************************************************************

# ------------------------------------------------------------------------
# Scan the directory for AMANDA-Entries
# ------------------------------------------------------------------------
sub Make_Prod_Link {
	my ($prefix,$suffix) = @_;

	# --------------------------------------------------
	# Just for info
	# --------------------------------------------------
	my $cur_dir = $CUR_DIR;
	print "-> $CUR_DIR\n";

	# --------------------------------------------------
	# Change to given directory and read the inodes
	# --------------------------------------------------
	chdir $prefix or do { warn "$CUR_DIR: $!\n";
			      return;
			    };
	opendir(DIR,".") or do { warn "$CUR_DIR: $!\n";
				 return;
				};
	my @inodes = grep(!/^\.$|^\.\.$/,readdir(DIR));

	# --------------------------------------------------
	# For each inode check if it is a directory or an
	# amanda file
	# --------------------------------------------------
	foreach my $inode (@inodes) {
		if (-d $inode) {
			# ----------------------------------
			# For a directory -> recursion
			# ----------------------------------
			$CUR_DIR .= "/".$inode;
			Make_Prod_Link($inode,$suffix) or return;
			chdir ".." or do { warn "Cannot get back from $inode: $!\n";
					   return;
					 };
			$CUR_DIR = $cur_dir;
		}
		# -----------------------------------------------------
		# Create a symbolic link unless the file already exists
		# -----------------------------------------------------
		if (substr($inode,-length($suffix)) eq $suffix) {
			my $prog_name = substr($inode,0,-length($suffix));
			if (-e $prog_name && ! -l $prog_name) {
				warn "Unexpected real file found: $CUR_DIR/$prog_name\n";
				return;
			}
			unlink $prog_name;
			symlink($inode,$prog_name) or do { warn "Cannot create symbolical link for $prog_name -> $inode: $!\n";
							   return;
							 };
			print "    $prog_name -> $inode\n";
		} else {
			print "let it untouched: $inode\n" if $debug;
		}
	}
	1;
}
