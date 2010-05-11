#!/usr/bin/perl
use strict;
use warnings;
use POSIX;
# Used by make to replace tags delimited by pairs of '%%'.  Each tag should be
# listed below.  Remember, the script reads from the environment for $pkg_type

# ARGV[0] = Source file (ends in .src)
# ARGV[1] = Destination file (usually ARGV[0] - ".src"

#### Checks
# We must run from the root of a source tree, but we can only check that the 
# common files are in the right places
if ( not -e "packaging/common/substitute.pl" ) {
    die "Error: 'substitute.pl' must be run from the root of a source tree"
}

sub get_version {
	# Two build cases: from checkout (svn info works), or dist tarball
	# (configure.ac will have the version). First try configure.ac, then
	# try svn info, which takes more time and processing. We assume our url
	# is structured something like this:
	# http://<server>/<project>/<trunk|branches|tags>/[<branch>|<tag>]
	# The re is tested on http urls with full DNS names, but ssh+svn:// or
	# file:/// and short DNS names should work too.
	
	my $VERSION;
	my $version_file = "VERSION";
	my $version_handle;
	my $versioned_tree = 'packaging/deb';
	if (-e $version_file) {
		# Autogen has been run, search VERSION file.
		if (-e "$version_file") {
			open($version_handle, "<", "$version_file") or
			    die "Could not open VERSION.";
			chomp($VERSION = <$version_handle>);
			close($version_file);
			
		} else {
			die "Could not find VERSION file.";
		}
	}
	if ( ! $VERSION ) {
		# Autogen has not been run or VERSION macro was found.  Try to
		# use svn info.
		my $SVN_URL  = "";
		my $SVN_ROOT = "";
		my $SVN_REV  = "";
		foreach my $info_line (`svn info $versioned_tree`) {
			$SVN_URL  = $1 if $info_line =~ m{^URL: (.*)};
			$SVN_ROOT = $1 if $info_line =~ m{^Repository Root: (.*)};
			$SVN_REV  = $1 if $info_line =~ m{^Revision: (.*)};
		}
		my @paths;
		my $BRANCH;
		my $PROJECT;
		my $svn_version;
		# Only newer versions of svn supply Repository Root.
		if ( $SVN_ROOT ) {
			$SVN_URL =~ m/$SVN_ROOT(.*)/;
			my $SVN_PATH = $1;

			@paths = split "/", $SVN_PATH;
			# We get ( empty, project branch, svn_version...)
			$PROJECT = $paths[1];
			$BRANCH = $paths[2];
			$svn_version = $paths[3];
		} else {
			# This may not work with file or ssh+svn urls.  In an
			# http: url, we get ( Protocol, empty, server, project,
			# branch, svn_version...)
			@paths = split "/", $SVN_URL;
			$PROJECT = $paths[3];
			$BRANCH = $paths[4];
			$svn_version = $paths[5];
		}

		if ( $BRANCH eq "trunk" | $BRANCH eq "branches" ) {
			# Suffix -svn-rev to branch and trunk builds.
		} else {
			# Fix VERSION by stripping up to the first digit
			$svn_version =~ s/^\D*//;
			my $cruft = qr{[_.]};
			if ( $BRANCH eq "branches" ) {
				# Branch names *should* have only 2 digits.
				$svn_version =~ m{^(\d)$cruft?(\d)$cruft?(\w*)?};
				# We throw away anything other than the first
				# two digits.
				$VERSION = "$1.$2";
				# Make sure that the version indicates this is
				# neither an RC or patch build.
				$VERSION .= "branch";
			} else {
				# We should have a tag, which *should* have 3
				# and maybe an rc## suffix
				$svn_version =~ m{^(\d)$cruft?(\d)$cruft?(\d)$cruft?(\w*)?};
				$VERSION = "$1.$2.$3$4";
			}
		}
	}
	return $VERSION;
}

my $pkg_type;
# Check environment to see if it's something else.
if (defined($ENV{'pkg_type'})) {
	$pkg_type = $ENV{"pkg_type"};
}
# Check the file name for a clue
elsif ( $ARGV[0] =~ /deb/ ) {
	$pkg_type = "deb";
}
elsif ( $ARGV[0] =~ /rpm/ ) {
	$pkg_type = "rpm";
}
elsif ( $ARGV[0] =~ /sun/ ) {
	$pkg_type = "sun";
}


# The surrounding line is preserved, and only the tag is replaced.  This
# behavior is somewhat arbitrary, but hopefully keeps replacements in comments
# syntax legal.
my %replacement_strings_common = (
	"%%VERSION%%" => get_version(),
	"%%AMANDAHOMEDIR%%" => "/var/lib/amanda",
	"%%LOGDIR%%" => "/var/log/amanda",
);

my %replacement_strings_deb = (
	# Used in debian changelog
	"%%DISTRO%%" => "",
	# Used in changelog
	"%%DEB_REL%%" => "",
	"%%DATE%%" => "",
	# Used in rules
	"%%PERL%%" => "",
);

my %replacement_strings_rpm = (
);

my %replacement_strings_sun = (
);

my %replacement_strings;
if ( $pkg_type eq "deb" ) {
	%replacement_strings = ( %replacement_strings_deb,
				 %replacement_strings_common );
	# Let's determine the distro:
	# Ubuntu has /etc/lsb-release, debian does not
	open(my $LSB_RELEASE, "<", "/etc/lsb-release") or 
		$replacement_strings{"%%DISTRO%%"} = "Debian";
	my $line;
	if ( $replacement_strings{"%%DISTRO%%"} ne "Debian" ) {
		$replacement_strings{"%%DISTRO%%"} = "Ubuntu";
		# We want the 2nd line
		<$LSB_RELEASE>;
		my @line = split /=/, <$LSB_RELEASE>;
		chomp($line[1]);
		$line[1] =~ s/\.//;
		$replacement_strings{"%%DEB_REL%%"} = $line[1];
		close($LSB_RELEASE);
	} else {
		open(my $DEB_RELEASE, "<", "/etc/debian_version") or die "could not read \"/etc/debian_version\": $!";
		chomp($line = <$DEB_RELEASE>);
		# Releases can have 3 fields.  we want the first 2.
		$line =~ s/(\d+)\.(\d+).*/$1$2/;
		$replacement_strings{"%%DEB_REL%%"} = $line;
		close($DEB_RELEASE);
	}
	# Set the date using date -r
	open(my $DATE_PIPE, "-|", "/bin/date -R") or die "could not read output of date -r";
	chomp($line = <$DATE_PIPE>);
	$replacement_strings{"%%DATE%%"} = $line;
	close($DATE_PIPE);
	# 32bit should use bitrock perl, while 64bit should use builtin.  we 
	# live on the edge and assume it's there.
	my @uname=POSIX::uname();
	my $arch = $uname[4];
	if ( $arch eq "x86_64" ) {
		$replacement_strings{"%%PERL%%"} = $^X;
	}
	else {
		$replacement_strings{"%%PERL%%"} = "/opt/zmanda/amanda/perl/bin/perl";
	}
}
elsif ( $pkg_type eq "rpm" ){
	%replacement_strings = ( %replacement_strings_rpm,
				 %replacement_strings_common );
}
else {
	%replacement_strings = ( %replacement_strings_sun,
				 %replacement_strings_common );
}

open my $src, "<", $ARGV[0] or die "could not read $ARGV[0]: $!";
open my $dst, ">", $ARGV[1] or die "could not write $ARGV[1]: $!";
select $dst;
while (<$src>) {
	chomp;
	# check for tags, using non greedy matching
	if ( m/(%%.+?%%)/ ) {
		# strings just replace the tag.
		if ( defined($replacement_strings{$1})) {
			s/(%%.+?%%)/$replacement_strings{$1}/g;
			print "$_\n";
		}
	}
	else {
		# If we got here, print the line unmolested
		print "$_\n";
	}
}
