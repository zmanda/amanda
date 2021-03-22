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
    my $date = qx{date $_[0]};
    [ $? == 0 ] or die "could not read output of date $_[0]";
    chomp $date;
    return $date;
};

sub get_arch {
    my @u = POSIX::uname();
    return $u[4];
};

sub get_debian_arch {
    my @u = POSIX::uname();
    $u[4] =~ s{i686}{i386};
    return $u[4];
};

sub get_debian_pkg_arch {
    my $arch = qx{command -v dpkg-architecture>/dev/null && dpkg-architecture | eval "\$(cat); echo \\\$DEB_TARGET_ARCH_CPU"};
    chomp $arch;
    return $arch;
};

sub read_file {
	# $1 is the file name and must exist.
	my $contents;
	my $file = "$_[0]";
	# Autogen has been run, the file will be there.
	if (-s $file) {
            local $/;
            my $f_handle;
		open($f_handle, "<", "$file") or
		    die "Could not open $file.";
		chomp($contents = <$f_handle>);
		close($f_handle);
	} else {
		die "Could not find $file file. Run config/set_full_version or ./autogen";
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

sub find_platform_info {
    my $rpmarch = get_arch();
    my $debarch = get_debian_pkg_arch();

    if ( $rpmarch !~ m{x86_64|amd64|i686|i386} || $debarch !~ m{x86_64|amd64|i686|i386} ) {
        print STDERR "ERROR: get_platform_info(): did not find arch \"$rpmarch\" or \"$debarch\"\n"; 
        return [];
    }

    my $RELTYPES = qx{exec 2>/dev/null; lsb_release --short --id};
    chomp $RELTYPES;

    $RELTYPES ||= qx{exec 2>/dev/null; . /etc/os-release; . /etc/lsb-release; echo -n "\$ID \$DISTRIB_ID"};
    chomp $RELTYPES;

    $RELTYPES ||= qx{exec 2>/dev/null; command ls -1 /etc/\{debian*,*release\}};
    chomp $RELTYPES;

    # change to all lowercase
    $RELTYPES = lc($RELTYPES);
    $RELTYPES =~ s/\n//sg;

    my $RELVER = qx{exec 2>/dev/null; lsb_release --release};
    chomp $RELVER;

    $RELVER ||= qx{exec 2>/dev/null; cat /etc/debian_version};
    chomp $RELVER;

    # allow all digits for solaris or centos or rhel 
    $RELVER ||= qx{exec 2>/dev/null; cat /etc/release /etc/centos-release /etc/redhat-release };
    chomp $RELVER;

    # get an obvious ident and use first-digit-range only
    $RELVER ||= qx{exec 2>/dev/null; . /etc/os-release; . /etc/lsb-release; echo "\$VERSION_ID" };
    chomp $RELVER;
    
    $RELVER =~ s/\n//sg;
    $RELVER =~ s/^\D*//; 
    $RELVER =~ s/\D*$//; 
    chomp $RELVER;

    # remove leading non-digits and toss all after first non-digit...
    my $RELVER1 = $RELVER;
    my $RELVER2 = $RELVER;

    $RELVER1 =~ s/\D.*$//;

    # remove leading non-digits and toss all after second non-digit...
    $RELVER2 =~ s/\D(\d+)/$1/; 
    $RELVER2 =~ s/\D.*$//;

    return [ ".$RELVER.pkg", "SunOS", $RELVER ]
       if ( $RELTYPES eq "/etc/release" );

    return [ ".fc${RELVER1}.${rpmarch}.rpm", "Fedora", $RELVER1 ]
       if ( $RELTYPES =~ m/fedora/ );

    return [ ".rhel${RELVER1}.${rpmarch}.rpm", "Centos", $RELVER1 ]
       if ( $RELTYPES =~ m/centos/ );

    # mention redhat only last
    return [ ".rhel${RELVER1}.${rpmarch}.rpm", "RHEL", $RELVER1 ]
       if ( $RELTYPES =~ m/redhat/ );

    return [ ".sles${RELVER2}.${rpmarch}.rpm", "SLES", $RELVER2 ]
       if ( $RELTYPES =~ m/\bsuse\b/ && $RELTYPES =~ m/\benterprise\b/ );

    return [ ".suse${RELVER2}.${rpmarch}.rpm", "SuSE", $RELVER2 ]
       if ( $RELTYPES =~ m/opensuse/ );

    return [ "Ubuntu${RELVER2}_${debarch}.deb", "Ubuntu", $RELVER2 ]
       if ( $RELTYPES =~ m/ubuntu/ );

    # mention debian after ubuntu
    return [ "Debian${RELVER1}_${debarch}.deb", "Debian", $RELVER1 ]
       if ( $RELTYPES =~ m/debian/ );

    return [ ];
}

sub get_platform_info {
    return [ $ENV{"PLATFORM_PKG"}, $ENV{"PLATFORM_DIST"}, $ENV{"PLATFORM_DISTVER"} ]
       if ( $ENV{"PLATFORM_PKG"} && $ENV{"PLATFORM_DIST"} && $ENV{"PLATFORM_DISTVER"} );

    my $arrayp = find_platform_info();
    my ($suffix,$dist,$distver) = @{$arrayp};

    $ENV{"PLATFORM_PKG"} = $suffix if ( ! $ENV{"PLATFORM_PKG"} );
    $ENV{"PLATFORM_DIST"} = $dist if ( ! $ENV{"PLATFORM_DIST"} );
    $ENV{"PLATFORM_DISTVER"} = $distver if ( ! $ENV{"PLATFORM_DISTVER"} );
    return $arrayp;
}

use constant PLATFORM_GLIB2_VERSIONS => qw( 
           rhel6  2.28.8
           rhel7  2.56.1
           rhel8  2.56.4

           fc28   2.56.1
           fc29   2.58.1
           fc30   2.60.1
           fc31   2.62.1

           ubuntu1604   2.48.0
           ubuntu1804   2.56.1
           ubuntu1810   2.58.1
           ubuntu1904   2.60.0

           debian811   2.42.1
           debian99    2.50.3
           debian100   2.58.3
           debian101   2.58.3
    );

sub get_glib2_version {
    my $suffix = lc($ENV{"PLATFORM_PKG"});
    my $dist = lc($ENV{"PLATFORM_DIST"}) . $ENV{"PLATFORM_DISTVER"};
    my %lookup = (PLATFORM_GLIB2_VERSIONS);
    my $ver = $lookup{$dist};

    return $ver if ( defined($ver) );

    if ( $suffix =~ m/\.deb$/ ) {
       $ver = qx(dpkg-query -W libglib2.0-0);
       $ver =~ s/.*\t([\d.]+).*/$1/;
       return $ver;
    }

    return qx(exec 2>/dev/null; rpm -q --qf '%{VERSION}' glib2)
}

sub get_pkg_suffix { return @{get_platform_info()}[0]; }
sub get_pkg_dist { return @{get_platform_info()}[1]; }
sub get_pkg_distver { return @{get_platform_info()}[2]; }
    
# perform test on platform and remember...
my $pkg_suffix = get_pkg_suffix();

my $pkg_type;
# Check environment to see if it's something else.
if (defined($ENV{'pkg_type'})) {
	$pkg_type = $ENV{"pkg_type"};
}
# Check the file name for a clue
elsif ( $pkg_suffix =~ /\.deb$/ ) {
	$pkg_type = "deb";
}
elsif ( $pkg_suffix =~ /\.rpm$/ ) {
	$pkg_type = "rpm";
}
elsif ( $pkg_suffix =~ /\.pkg$/ ) {
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
	"%%PKG_SUFFIX%%" => get_pkg_suffix(),
	"%%PKG_DIST%%" => lc(get_pkg_dist()),
	"%%PKG_DISTVER%%" => get_pkg_distver(),
        "%%ARCH%%" => get_arch(),
        "%%PKG_ARCH%%" => get_arch(),
	"%%DISTRO%%" => get_pkg_dist(),
        "%%DATE%%" => "'+%a, %d %b %Y %T %z'"
);

# add special variables
my %replacement_strings_deb = (
	# Used in changelog
	"%%DEB_REL%%" => get_pkg_distver(),
        "%%PKG_ARCH%%" => get_debian_pkg_arch(),
        "%%ARCH%%" => get_debian_arch(),
	# Used in server rules
	"%%PERL%%" => $^X
);

# override date
my %replacement_strings_rpm = (
	"%%DATE%%" => "'+%a %b %d %Y'",
);

# use all defaults
my %replacement_strings_sun = (
);

my $ref;
eval '$ref = \%replacement_strings_'."$pkg_type;";

# override values with the later of the two...
my %replacement_strings = ( %replacement_strings_common, %{$ref} );

$replacement_strings{"%%ARCH%%"} =~ s/sparc/sun4u/
    if ( $pkg_type eq "sun" );
$replacement_strings{"%%ARCH%%"} =~ s/intel/i86pc/
    if ( $pkg_type eq "sun" );

die "Unknown platform!"
    if ( ! $replacement_strings{"%%ARCH%%"} );

$replacement_strings{"%%PKG_REV%%"} = fix_pkg_rev($replacement_strings{"%%PKG_REV%%"}, $pkg_type);
$replacement_strings{"%%DATE%%"} = get_date($replacement_strings{"%%DATE%%"});

# Make a hash of tags and the contents of replacement files
my %replacement_data;
while (my ($tag, $filename) = each %replacement_filenames) {
	open(my $file, "<", $filename) or die "could not read \"$filename\": $!";
        {
            local $/;  # remove line endings as barrier
            $replacement_data{$tag} = join "", <$file>;
        }
	close($file);
}
open my $src, "<", $ARGV[0] or die "could not read $ARGV[0]: $!";
open my $dst, ">", $ARGV[1] or die "could not write $ARGV[1]: $!";
select $dst;

my $line;

while (<$src>) {
    #
    # check for tags, using non greedy matching
    #
    $line = $_;
    SUBST:
    # for all tags found in the line
    while ( m/%%\w+?%%/g ) {
        my $tag = $&;

        # Data replaces the line
        if ( defined($replacement_data{$tag})) {
            $line = $replacement_data{$tag};
            $line =~ s/\%/%%/g if ( $ARGV[1] =~ m/\.spec$/ );
            last SUBST; # no more patterns
        }

        # strings just replace the tag(s).
        if ( defined($replacement_strings{$tag})) {
            my $replacing = $replacement_strings{$tag};
            $replacing =~ s/\%/%%/g if ( $ARGV[1] =~ m/\.spec$/ );
            $line =~ s/$tag/$replacing/; # replace one tag only...
	}

        # if not... just print the line as is
    }
} continue {
    print $line;
}
