#!/usr/bin/perl
use strict;
use warnings;
use POSIX;
# Used by make to replace tags delimited by pairs of '%%'.  Each tag should be
# listed below.  Remember, the script reads from the environment for $pkg_type
#


# ARGV[0] = Source file (ends in .src)
# ARGV[1] = Destination file (usually ARGV[0] - ".src"

#### Checks
# We must run from the root of a source tree, but we can only check that the
# common files are in the right places
#if ( not -e "packaging/common/substitute.pl" ) {
#    die "Error: 'substitute.pl' must be run from the root of a source tree"
#}

sub get_username {
    return $ENV{'AMANDAUSER'} || "amandabackup";
}

sub get_useridnum {
    return "63998";
}

sub get_groupname {
    return $ENV{'AMANDAGROUP'} || "amandabackup";
}

sub get_cligroupname {
    return $ENV{'AMANDACLIGROUP'} || "amandabackup";
}

sub get_tapegroupname {
    return $ENV{'AMANDATAPEGROUP'} || "tape";
}

sub get_userhomedir {
    return $ENV{"AMANDAHOMEDIR"} || "/var/lib/amanda";
}

sub get_topdir {
    return "/opt/zmanda/amanda";
}

sub get_wwwdir {
    return "/opt/zmanda/amanda/var/www";
}

sub get_logdir {
    return "/var/log/amanda";
}

sub get_date {
    my $date = qx{date $_[0]};
    [ $? == 0 ] or die "could not read output of date $_[0]";
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

sub get_rpm_pkg_arch {
    my $arch = qx{command -v rpmbuild >/dev/null && rpmbuild --showrc | grep '^build arch'};
    $arch =~ s/^.*\s\b//;
    return $arch;
};

sub get_debian_pkg_arch {
    return qx{command -v dpkg-architecture>/dev/null && dpkg-architecture | eval "\$(cat)"'; echo \$DEB_TARGET_ARCH_CPU'};
};

sub read_sh_lib {
    my $filename = "$_[0]";
    return read_file($filename);
}

sub read_file {
    # $1 is the file name and must exist.
    my $filename = "$_[0]";
    # Autogen has been run, the file will be there.
    my $len = ( -s $filename ) || 0;
    if ( ! $len && ( -s "$filename.src" ) ) { 
        my ($rdfilename) = "$filename.src";
        $filename =~ s{.*/}{};
        system("$^X $0 $rdfilename $filename");
    }

    $len = ( -s $filename ) || 0;
    ( $len > 0 ) or die "Could not find $filename file. Run config/set_full_version or ./autogen";

    my $contents;
    {
        open(my $fh, "<", $filename) or die "could not read \"$filename\": $!";
        local $/;  # remove line endings as barrier
        $contents = join "", <$fh>;
        close($fh);
    }
    return $contents;
}


sub find_platform_info {
    my $rpmarch = get_rpm_pkg_arch();
    my $debarch = get_debian_pkg_arch();

    chomp $rpmarch;
    chomp $debarch;

    if ( $rpmarch && $rpmarch !~ m{x86_64|i686|i586} ) {
        print STDERR "ERROR: get_platform_info(): did not find rpm arch \"$rpmarch\"\n"; 
        return [];
    }
    if ( $debarch && $debarch !~ m{x86_64|amd64|i686|i386} ) {
        print STDERR "ERROR: get_platform_info(): did not find deb arch \"$debarch\"\n"; 
        return [];
    }

    my $RELTYPES = qx{exec 2>/dev/null; lsb_release --short --id} . " ";
    chomp $RELTYPES;

    $RELTYPES =~ s/\s+/ /g;
    $RELTYPES =~ s/^\s$//g;

    # non-space?
    if ( ! $RELTYPES ) {
        $RELTYPES .= qx{exec 2>/dev/null; . /etc/os-release; eval 'echo -n "\$ID \$DISTRIB_ID"'} . " ";
        chomp $RELTYPES;
        
        $RELTYPES .= qx{exec 2>/dev/null; . /etc/lsb-release; eval 'echo -n "\$ID \$DISTRIB_ID"'} . " ";
        chomp $RELTYPES;

        $RELTYPES =~ s/\s+/ /g; $RELTYPES =~ s/^\s$//g;
    }
   
    $RELTYPES ||= qx{exec 2>/dev/null; command head -1 /etc/release} . " ";
    chomp $RELTYPES;

    $RELTYPES =~ s/\s+/ /g; $RELTYPES =~ s/^\s$//g;

    $RELTYPES ||= qx{exec 2>/dev/null; command ls -1 /etc/\{debian*,redhat*,fedora*,SUSE*\}} . " ";
    chomp $RELTYPES;

    $RELTYPES =~ s/\s+/ /g; $RELTYPES =~ s/^\s$//g;

    # change to all lowercase
    $RELTYPES = lc($RELTYPES);
    $RELTYPES =~ s/\n//sg;

    my $RELVER = qx{exec 2>/dev/null; lsb_release --release} . " ";
    chomp $RELVER;

    $RELVER ||= qx{exec 2>/dev/null; cat /etc/debian_version} . " ";
    chomp $RELVER;

    $RELVER =~ s/\s+/ /g; $RELVER =~ s/^\s$//g;

    if ( ! $RELVER ) {
        # allow all digits for solaris or centos or rhel 
        $RELVER .= qx{exec 2>/dev/null; cat /etc/release /etc/centos-release /etc/redhat-release } . " ";
        chomp $RELVER;

        # get an obvious ident and use first-digit-range only
        $RELVER .= qx{exec 2>/dev/null; . /etc/os-release; eval 'echo "\$VERSION \$VERSION_ID "'} . " ";
        chomp $RELVER;
        
        # get an obvious ident and use first-digit-range only
        $RELVER .= qx{exec 2>/dev/null; . /etc/lsb-release; eval 'echo "\$VERSION \$VERSION_ID "'} . " ";
        chomp $RELVER;

        $RELVER =~ s/\s+/ /g; $RELVER =~ s/^\s$//g;
    }
    
    $RELVER =~ s/\n//sg;
    $RELVER =~ s/^\D*//;  # remove up to first number
    $RELVER =~ s/[^-0-9.].*$//;  # remove all after non-digit-nor-period-nor-dash
    chomp $RELVER;

    # remove leading non-digits and toss all after first non-digit...
    my $RELVER1 = $RELVER;
    my $RELVER2 = $RELVER;

    $RELVER1 =~ s/(\d)\D.*$/$1/;

    # remove leading non-digits and toss all after second non-digit...
    $RELVER2 =~ s/(\d)\D(\d+)/$1$2/;
    $RELVER2 =~ s/(\d)\D.*$/$1/;

    my $suncpu = qx{gcc -dumpmachine}; 
    chomp $suncpu;
    $suncpu =~ s/-.*//;

    return [ "$suncpu-pc-solaris2.$RELVER1.pkg", "SunOS", $RELVER ]
       if ( $RELTYPES =~ m/solaris/i );

    return [ ".fc${RELVER1}.${rpmarch}.rpm", "Fedora", $RELVER1 ]
       if ( $RELTYPES =~ m/fedora/ );

    return [ ".rhel${RELVER1}.${rpmarch}.rpm", "Centos", $RELVER1 ]
       if ( $RELTYPES =~ m/centos/ );

    # mention redhat only last
    return [ ".rhel${RELVER1}.${rpmarch}.rpm", "RHEL", $RELVER1 ]
       if ( $RELTYPES =~ m/redhat/ );

    # return [ ".sles${RELVER2}.${rpmarch}.rpm", "SLES", $RELVER2 ]
    #    if ( $RELTYPES =~ m/\bsuse\b/ && $RELTYPES =~ m/\benterprise\b/ );

    return [ ".suse${RELVER2}.${rpmarch}.rpm", "SuSE", $RELVER2 ]
       if ( $RELTYPES =~ m/opensuse/ );

    return [ ".suse${RELVER1}.${rpmarch}.rpm", "SuSE", $RELVER2 ]
       if ( $RELTYPES =~ m/\bsles\b/ );

    return [ "Ubuntu${RELVER2}_${debarch}.deb", "Ubuntu", $RELVER2 ]
       if ( $RELTYPES =~ m/ubuntu/ );

    # mention debian after ubuntu
    return [ "Debian${RELVER1}_${debarch}.deb", "Debian", $RELVER1 ]
       if ( $RELTYPES =~ m/debian/ );

    return [ "Manjaro${RELVER1}.tar", "Manjaro", $RELVER1 ]
       if ( $RELTYPES =~ m/manjaro/ );

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

	   sles11      2.22.5
	   sles12      2.48.2
	   sles15      2.54.3
    );

sub get_glib2_version {
    my $suffix = lc($ENV{"PLATFORM_PKG"});
    my $dist = lc($ENV{"PLATFORM_DIST"}) . $ENV{"PLATFORM_DISTVER"};
    my %lookup = (PLATFORM_GLIB2_VERSIONS);
    my $ver = $lookup{$dist};

    return $ver if ( defined($ver) );

    if ( $suffix =~ m/\.deb$/ ) {
       $ver = qx{dpkg-query -W libglib2.0-0};
       $ver =~ s/.*\t([\d.]+).*/$1/;
       return $ver;
    }

    return scalar qx{exec 2>/dev/null; rpm -q --qf '%{VERSION}' glib2-dev glib2-devel | grep -v 'not installed'};
}

sub get_glib2_next_version {
    my $ver = &get_glib2_version();
    $ver =~ s{\.(\d+)$}{ "." . ($1+1) }e; 
    return $ver;
}

sub get_pkg_suffix { return @{get_platform_info()}[0]; }
sub get_pkg_dist { return @{get_platform_info()}[1]; }
sub get_pkg_distver { return @{get_platform_info()}[2]; }

# perform test on platform and remember...
my $pkg_suffix = get_pkg_suffix();

my ($pkg_type, $pkg_name);

# Check environment to see if it's something else.
if (defined($ENV{'pkg_name'})) {
    $pkg_name = $ENV{"pkg_name"};
}

# Check environment to see if it's something else.
#
# NOTE: "//" is NOT compatible with perl 5.10..
if ( ($ENV{'pkg_type'} || 0) && $ENV{'pkg_type'} !~ m{/} ) {
    $pkg_type = $ENV{"pkg_type"};
    $pkg_type =~ s/sun-pkg/pkg/;
}
# Check the file name for a clue
elsif ( $pkg_suffix =~ /\.deb$/ ) {
	$pkg_type = "deb";
}
elsif ( $pkg_suffix =~ /\.rpm$/ ) {
	$pkg_type = "rpm";
}
elsif ( $pkg_suffix =~ /\.pkg$/ ) {
	$pkg_type = "pkg";
}
elsif ( $pkg_suffix =~ /\.tar$/ ) {
	$pkg_type = "tar";
}
else {
    die "Could not determine pkg_type either by environment variable, or
	pathname of files to substitute ($ARGV[0]).";
}

die "failed to recognize platform"
       unless ( $ENV{"PLATFORM_PKG"} && $ENV{"PLATFORM_DIST"} && $ENV{"PLATFORM_DISTVER"} );

my %replacement_includes = (
	"SCRIPT_VARS" =>    	 sub { read_sh_lib("packaging/common/script_vars.sh"); },
	"COMMON_FUNCTIONS" =>    sub { read_sh_lib("packaging/common/common_functions.sh"); },
	"PRE_INST_FUNCTIONS" =>  sub { read_sh_lib("packaging/common/pre_inst_functions.sh"); },
	"PRE_RM_FUNCTIONS" =>   sub { read_sh_lib("packaging/common/pre_rm_functions.sh"); },
	"POST_INST_FUNCTIONS" => sub { read_sh_lib("packaging/common/post_inst_functions.sh"); },
	"POST_RM_FUNCTIONS" =>   sub { read_sh_lib("packaging/common/post_rm_functions.sh"); },
	"PKG_STATE_FUNCTIONS" =>   sub { read_sh_lib("packaging/common/pkg_state_functions.sh"); },
# TODO: PRE_UNINST?
);

# These are handled slightly differently: The surrounding line is preserved,
# and only the tag is replaced.  This behavior is somewhat arbitrary, but
# hopefully keeps replacements in comments syntax legal.
my %replacement_strings_common = (
	"INSTALL_TOPDIR" =>      sub { get_topdir(); },
	"INSTALL_WWWDIR" =>      sub { get_wwwdir();},
	"AMANDAHOMEDIR" =>       sub { get_userhomedir(); },
	"LOGDIR" =>              sub { get_logdir(); },
        "AMANDAUSER" =>          sub { get_username(); },
        "AMANDAUIDNUM" =>        sub { get_useridnum(); },
        "AMANDAGROUP" =>         sub { get_groupname(); },
        "AMANDACLIGROUP" =>      sub { get_cligroupname(); },
        "AMANDATAPEGROUP" =>     sub { get_tapegroupname(); },

	"VERSION" =>             sub { read_file("FULL_VERSION"); },
	"PKG_REV" =>             sub { read_file("PKG_REV"); },
	"PKG_SUFFIX" =>          sub { get_pkg_suffix(); },
	"PKG_DIST" =>            sub { lc(get_pkg_dist()); },
	"PKG_DISTVER" =>         sub { get_pkg_distver(); },
        "ARCH" =>                sub { $_ = get_arch(); s/sparc/sun4u/; s/intel/i86pc/; return $_; },
        "PKG_ARCH" =>            sub { get_arch(); },
	"DISTRO" =>              sub { get_pkg_dist(); },
	"BUILD_GLIB2_VERSION" => sub { get_glib2_version(); },
	"BUILD_GLIB2_NEXT_VERSION" => sub { get_glib2_next_version(); },
        "DATE" =>                sub { get_date("'+%a, %d %b %Y %T %z'"); },
	"SYSCONFDIR" =>		 sub { "/etc"; }
);

# add special variables
my %replacement_strings_deb = (
	# Used in changelog
	"DEB_REL" =>    sub { get_pkg_distver(); },
        "PKG_ARCH" =>   sub { get_debian_pkg_arch(); },
        "ARCH" =>       sub { get_debian_arch(); },
	# Used in server rules
	"PERL" =>       sub { $^X; },
);

# override date
my %replacement_strings_rpm = (
	"DATE" => sub { get_date("'+%a %b %d %Y'"); },
);

# override date
my %replacement_strings_tar = (
);

# use all defaults
my %replacement_strings_pkg = (
	"REL_INSTALL_TOPDIR" =>  sub { substr(get_topdir(),1); },
	"REL_AMANDAHOMEDIR" =>   sub { substr(get_topdir(),1) . "/amanda"; },
	"AMANDAHOMEDIR" =>       sub { get_topdir() . "/amanda"; },
        "AMANDACLIGROUP" =>     sub { "staff"; },
        "AMANDATAPEGROUP" => 	 sub { "sys"; },
	"SYSCONFDIR" =>		 sub { get_topdir() . "/etc"; }
);

my $ref;
eval '$ref = \%replacement_strings_'."$pkg_type;";

$ref || keys(%{$ref}) || die "no hash was found for $pkg_type";

# override values with the later of the two...
my %replacement_strings = ( %replacement_strings_common, %{$ref} );

open my $src, "<", $ARGV[0] or die "could not read $ARGV[0]: $!";
open my $dst, ">", $ARGV[1] or die "could not write $ARGV[1]: $!";
select $dst;

my $src_filename = readlink("/proc/self/fd/". fileno($src)) || $ARGV[0];
my $dst_filename = readlink("/proc/self/fd/". fileno($dst)) || $ARGV[1];

my $line;
my $pkg = "";

while (<$src>) 
{

    #
    # check for tags, using non greedy matching
    #
    $line = $_; # dont change $_ while performing m//

    if ( $line =~ m{^%(pre|post|posttrans|preun|postun)\s+-n\s+(\S+)} ) {
        $pkg = $2;
    } elsif ( $line =~ m{^%(pre|post|posttrans|preun|postun)\s+(\S+)} ) {
        $pkg = "%{name}-$2";
    }
    SUBST:
    # for all tags found in the line
    while ( m/%%(\w+?)%%/g ) {
        my $tag = $1;
        my $tagre = quotemeta($&);

        # Data replaces the line .. and cacheing wont often help with single uses...
        if ( m/^$tagre/ && defined($replacement_includes{$tag}) ) {
            $line = &{ $replacement_includes{$tag} };
	    # do not chomp the output..
	    if ( $dst_filename =~ m/\.spec$/ ) 
	    {
	    	my ($pre);
                $pre = "\nRPM_PACKAGE_NAME=\"$pkg\";";
		$pre .= "\nRPM_PACKAGE_VERSION=\"%{version}\";";
		$pre .= "\nAMLIBEXECDIR=\"%{AMLIBEXECDIR}\";";
		$pre .= "\nexport PYTHON=\"%{INSTALL_PYTHON}\";";

		$line =~ s/\%/%%/g;
		$line = "$pre\n$line"
	    }

            last SUBST; # no more patterns on this line...
        }

        # strings just replace the tag(s).
        if ( defined($replacement_strings{$tag}) ) {
            my $replacing = &{ $replacement_strings{$tag} };
            $replacing =~ s/\%/%%/g 
               if ( $dst_filename =~ m/\.spec$/ );
	    chomp $replacing; # no line-end from string is allowed!
	    chomp $replacing; # no line-end from string is allowed! (just in case 2)
            $line =~ s/$tagre/$replacing/; # replace one tag only...
        }

        # if not... just print the line as is
    } # SUBST:

} continue {
    print $line;
}
