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

sub get_date {
    my $date;
    # First parameter should be a date format string.
    open(my $DATE_PIPE, "-|", "/bin/date $_[0]");
    [ $? == 0 ] or die "could not read output of date $_[0]";
    chomp($date = <$DATE_PIPE>);
    close($DATE_PIPE);
    return $date;
};

sub get_arch {
    my @u = POSIX::uname();
    return $u[4];
};

sub read_file {
	# $1 is the file name and must exist.
	my $contents;
	my $file = "$_[0]";
	my $f_handle;
	# Autogen has been run, the file will be there.
	if (-e $file) {
		open($f_handle, "<", "$file") or
		    die "Could not open $file.";
		chomp($contents = <$f_handle>);
		close($f_handle);
		
	} else {
		die "Could not find $file file. run config/set_full_version or ./autogen";
	}
	return $contents;
}

sub fix_pkg_rev {
    my $pkg_rev = "$_[0]";
    # $1 should be a package type, and we build the rest of the regex string
    # here for simplicity
    my $type_match_str = "$_[1]0?";
    # strip pkg_type and maybe a zero, else assign pkg_rev = 1
    $pkg_rev = $pkg_rev =~ s/$type_match_str// || 1;
    return $pkg_rev;
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
else {
    die "Could not determine pkg_type either by environment variable, or
	pathname of files to substitute ($ARGV[0]).";
}

# The keys to the hashes used are the "tags" we try to substitute.  Each
# tag should be on a line by itself in the package file, as the whole line is
# replaced by a set of lines.  The line may be commented.
my %replacement_filenames = (
	"%%COMMON_FUNCTIONS%%" => "packaging/common/common_functions.sh",
	"%%PRE_INST_FUNCTIONS%%" => "packaging/common/pre_inst_functions.sh",
	"%%POST_INST_FUNCTIONS%%" => "packaging/common/post_inst_functions.sh",
	"%%POST_RM_FUNCTIONS%%" => "packaging/common/post_rm_functions.sh",
# TODO: PRE_UNINST?
);

# These are handled slightly differently: The surrounding line is preserved, 
# and only the tag is replaced.  This behavior is somewhat arbitrary, but
# hopefully keeps replacements in comments syntax legal.
my %replacement_strings_common = (
	"%%VERSION%%" => read_file("FULL_VERSION"),
	"%%PKG_REV%%" => read_file("PKG_REV"),
	"%%AMANDAHOMEDIR%%" => "/var/lib/amanda",
	"%%LOGDIR%%" => "/var/log/amanda",
);

my %replacement_strings_deb = (
	# Used in debian changelog
	"%%DISTRO%%" => "",
	# Used in changelog
	"%%DEB_REL%%" => "",
	"%%DATE%%" => "'+%a, %d %b %Y %T %z'",
	# Used in server rules
	"%%PERL%%" => "",
);

my %replacement_strings_rpm = (
	"%%DATE%%" => "'+%a %b %d %Y'",
);

my %replacement_strings_sun = (
    "%%ARCH%%" => "",
    "%%DATE%%" => "'+%a, %d %b %Y %T %z'",
);

my %replacement_strings;
if ( $pkg_type eq "deb" ) {
	%replacement_strings = ( %replacement_strings_deb,
				 %replacement_strings_common );
        $replacement_strings{"%%PKG_REV%%"} =
            fix_pkg_rev($replacement_strings{"%%PKG_REV%%"}, "deb");
	# Let's determine the distro:
        my $release;
        if ( -e "/usr/bin/lsb_release" ) {
            # Yay!  it's easy.
            my $distro_id = `/usr/bin/lsb_release --id --short` or die "Could not run lsb_release!";
            chomp ($replacement_strings{"%%DISTRO%%"} = $distro_id);

            chomp($release = `/usr/bin/lsb_release --release --short`);
        }
	if ( $replacement_strings{"%%DISTRO%%"} eq "" ) {
            # Let's hope it's debian.
            open(my $DEB_RELEASE, "<", "/etc/debian_version") or die "Could not read \"/etc/debian_version\": $!";
            # Whew!
            $replacement_strings{"%%DISTRO%%"} = "Debian";
            chomp($release = <$DEB_RELEASE>);
            close($DEB_RELEASE);
	}
        # Fix the release version string.
        if ( $replacement_strings{"%%DISTRO%%"} eq "Ubuntu" ) {
            $release =~ s/\.//;
        } else {
            # Releases can have 3 fields on Debian.  we want the first 2.
            $release =~ s/(\d+)\.(\d+).*/$1$2/;
        }
        $replacement_strings{"%%DEB_REL%%"} = $release;
	$replacement_strings{"%%DATE%%"} = get_date($replacement_strings{"%%DATE%%"});
	# 32bit should use bitrock perl, while 64bit should use builtin.  we
	# live on the edge and assume it's there.
	my $arch = get_arch();
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
        $replacement_strings{"%%PKG_REV%%"} =
            fix_pkg_rev($replacement_strings{"%%PKG_REV%%"}, "rpm");
	$replacement_strings{"%%DATE%%"} = get_date($replacement_strings{"%%DATE%%"});
}
else {
	%replacement_strings = ( %replacement_strings_sun,
				 %replacement_strings_common );
        $replacement_strings{"%%PKG_REV%%"} =
            fix_pkg_rev($replacement_strings{"%%PKG_REV%%"}, "sun");
	$replacement_strings{"%%DATE%%"} = get_date($replacement_strings{"%%DATE%%"});
	my $arch = get_arch();
	if ( $arch eq "sun4u" ) {
	    $replacement_strings{"%%ARCH%%"} = "sparc";
	}
	elsif ( $arch eq "i86pc" ) {
	    $replacement_strings{"%%ARCH%%"} = "intel";
	}
	else {
	    die "Unknown solaris platform!";
	}
}

# Make a hash of tags and the contents of replacement files
my %replacement_data;
while (my ($tag, $filename) = each %replacement_filenames) {
	open(my $file, "<", $filename) or die "could not read \"$filename\": $!";
	$replacement_data{$tag} = join "", <$file>;
	close($file);
}
open my $src, "<", $ARGV[0] or die "could not read $ARGV[0]: $!";
open my $dst, ">", $ARGV[1] or die "could not write $ARGV[1]: $!";
select $dst;
while (<$src>) {
	chomp;
	# check for tags, using non greedy matching
	if ( m/(%%.+?%%)/ ) {
		# Data replaces the line
		if ( defined($replacement_data{$1})) {
			print $replacement_data{$1};
		} 
		# strings just replace the tag.
		elsif ( defined($replacement_strings{$1})) {
			s/(%%.+?%%)/$replacement_strings{$1}/g;
			print "$_\n";
		}
	}
	else {
		# If we got here, print the line unmolested
		print "$_\n";
	}
}
