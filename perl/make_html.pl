#! @PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use strict;
use warnings;
use Pod::Html;
use File::Basename;
use File::Path;
use File::Temp;
use Getopt::Long;

my $opt_homeurl;
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'homeurl=s' => \$opt_homeurl,
);

my ($targetdir, @sources) = @ARGV;
@sources = sort @sources;

my $version = "@VERSION@";
my $version_comment = @VERSION_COMMENT@;
my $version_major = "@VERSION_MAJOR@";
my $version_minor = "@VERSION_MINOR@";
my $version_patch = "@VERSION_PATCH@";
my $pod_path;
if ($version_comment eq "") {
    $pod_path = "/pod/$version_major.$version_minor.$version_patch";
} elsif ($version_comment =~ /beta\d*/) {
    $pod_path = "/pod/$version_major.$version_minor.$version_patch$version_comment";
} elsif ($version_comment =~ /alpha/ or $version_comment =~ /beta/) {
    $pod_path = "/pod/beta";
} else {
    $pod_path = "/pod/$version_major.$version_minor";
}

my %dirs = ( '' => 1 );
my ($dir, $pm);

sub usage {
    print STDERR "Usage: make_html [--homeurl]\n";
    exit(1);
}

sub pm2html {
	my ($pm) = @_;
	$pm =~ s{\.pm$}{.html};
	return $pm;
}

sub pm2module {
	my ($pm) = @_;
	$pm =~ s{/}{::}g;
	$pm =~ s{\.pm$}{};
	return $pm;
}

sub pm2css {
	my ($pm) = @_;
	$pm =~ s{[^/]*/}{../}g;
	$pm =~ s{/[^/]*$}{/};
	$pm .= "amperl.css";
	return $pm;
}

# generate the HTML
for $pm (@sources) {
    my $module = pm2module($pm);
    my $html = pm2html($pm);
    my $fh;
    my $generated = gmtime();

    print "Converting $pm to $html\n";

    $dir = dirname($pm);
    $dirs{$dir} = 1;

    mkpath("$targetdir/$dir");

    # slurp the source
    open ($fh, "<", $pm) or die("Error opening $pm: $!");
    my $pod = do { local $/; <$fh> };
    close ($fh);

    # set up a temporary input file for a modified version of the POD
    my $tmp = File::Temp->new();
    open ($fh, ">", $tmp->filename) or die("Error opening $tmp: $!");

    # now prepend and append a header and footer
    print $fh <<HEADER;

=begin html

HEADER

    my $module_parent = $module;
    my $index;
    my $dir1 = $pm;
    $dir1 =~ s/\.pm$//;
    if (-d $dir1) {
	$dir1 =~ s{^.*/}{}g;
	print $fh "<a href=\"$dir1/index.html\">$module_parent modules list</a><br />\n";
    }

    $module_parent =~ s{::[^:]*$}{};
    $index = "index.html";
    my $moduleX;
    my $count = 1;
    do {
	print $fh "<a href=\"$index\">$module_parent modules list</a><br />\n";
	$moduleX = $module_parent;
	$module_parent =~ s{::[^:]*$}{};
	$index = "../" . $index;
	$count++;
	die() if $count > 5;
    } while $moduleX ne $module_parent;

    print $fh "<a href=\"$index\">List of All Modules</a><br />\n";
    if ($opt_homeurl) {
	$index = "../" . $index;
	$index =~ s{/index.html}{};
	print $fh "<a href=\"$index\">$opt_homeurl</a><br />\n";
    }
    print $fh <<HEADER;
<div class="pod">
<h1 class="module">$module</h1>

=end html

=cut
HEADER
    print $fh $pod;
    print $fh <<FOOTER;

=head1 ABOUT THIS PAGE

This page was automatically generated $generated from the Amanda source tree,
and documents the most recent development version of Amanda.  For documentation
specific to the version of Amanda on your system, use the 'perldoc' command.

=begin html

</div>

=end html

=cut
FOOTER
    close ($fh);

    my $css = pm2css($pm);
    pod2html("--podpath=Amanda",
	    "--htmldir=$targetdir",
	    "--infile=$tmp",
	    "--css=$css",
	    "--noindex",
	    "--outfile=$targetdir/$html");

    # post-process that HTML
    postprocess("$targetdir/$html", $module);
}

sub postprocess {
    my ($filename, $module) = @_;

    # slurp it up
    open(my $fh, "<", $filename) or die("open $filename: $!");
    my $html = do { local $/; <$fh> };
    close($fh);

    $html =~ s{<title>.*</title>}{<title>$module</title>};
    $html =~ s{<body>}{<body></div><center>$version<hr></center>};
    $html =~ s{<link rev="made" [^>]*/>}{};
    $html =~ s{html">the (\S+) manpage</a>}{html">$1</a>}g;
    $html =~ s{</body>}{</div><hr><center>$version</center></body>};
    # write it out
    open($fh, ">", $filename) or die("open $filename: $!");
    print $fh $html;
    close($fh);
}

# and generate an index HTML for each new directory
# we created.
for $dir (keys %dirs) {
#for $dir ("") {
    my $css;
    if ($dir) {
	$css = pm2css("$dir/");
    } else {
	$css = "amperl.css";
    }
    my $module = $dir;
    $module =~ s{/}{::}g;
    my $module_name = $module;
    $module_name = "All" if $module eq "";
    open(my $idx, ">", "$targetdir/$dir/index.html") or die("Error opening $dir/index.html: $!");
    print $idx <<HEADER;
<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>$module_name modules list</title>
<link rel="stylesheet" href="$css" type="text/css"
<meta http-equiv="content-type" content="text/html; charset=utf-8" />
</head>
<body></div><center>$version<hr></center>
HEADER
    my $module_parent = $module;
    my $index;
    if ($module_parent =~ /::/) {
	$module_parent =~ s{::[^:]*$}{};
	$index = "../index.html";
    } elsif ($module_parent eq "Amanda") {
	$module_parent = "";
	$index = "../index.html";
    } elsif ($module_parent eq "") {
	$module_parent = "";
	$index = "index.html";
    }
    my $moduleX;
    my $count = 1;
    while ($module_parent ne "") {
	print $idx "<a href=\"$index\">$module_parent modules list</a><br />\n";
	if ($module_parent =~ /::/) {
	    $module_parent =~ s{::[^:]*$}{};
	} else {
	    $module_parent = "";
	}
	$index = "../" . $index;
	$count++;
	die() if $count > 5;
     }

    if ($dir ne "") {
	print $idx "<a href=\"$index\">List of All Modules</a><br />\n";
    }
    if ($opt_homeurl) {
	my $my_index = "../$index";
	$my_index =~ s{/index.html}{};
	print $idx "<a href=\"$my_index\">$opt_homeurl</a><br />\n";
    }
    print $idx <<BODY;
<div class="pod">
<h1 class="module">$module_name modules list</h1>
<ul>
BODY
    my @rdirs;
    for $pm (@sources) {
	my $html = pm2html($pm);
	my $mod = pm2module($pm);
	next unless ($pm =~ /^$dir/);
	if (@rdirs) {
	    while (@rdirs and !($pm =~ /$rdirs[0]/)) {
		shift @rdirs;
		print $idx " </ul>\n";
	    }
	}
	my $pm_dir = $pm;
	$pm_dir =~ s/\.pm//;
	my $my_pm_dir = $pm_dir;
	my @toto;
	while (!@rdirs or $rdirs[0] ne $my_pm_dir) {
	    last if $my_pm_dir le $dir;
	    my $url = $my_pm_dir;
	    $url =~ s{$dir}{}g if $dir ne "";
	    $url .= "/index.html";
	    $url =~ s{^/}{};
	    if (-d $my_pm_dir) {
		unshift @toto, [$my_pm_dir, $url];
	    }
	    last if $my_pm_dir eq "";
	    if ($my_pm_dir =~ /\//) {
		$my_pm_dir =~ s{/[^/]*$}{};
	    } else {
		$my_pm_dir = "";
	    }
	}
	$count = 0;
	while (@toto) {
	    my $a = shift @toto;
	    my $pm_dir = $a->[0];
	    my $url    = $a->[1];
	    my $my_mod = $pm_dir;
	    $my_mod =~ s{/}{::}g;
	    print $idx " <li><a href=\"$url\">$my_mod modules list</a>\n <ul>\n";
	    unshift @rdirs, $pm_dir;
	    $count++;
	    die("XX") if $count > 5;
	}
	if ($dir) {
	    if ($pm =~ /^$dir\//) {
		$html =~ s{^$dir/}{}g;
	    } else {
		my $my_dir = $dir;
		my $my_html = $html;
		$my_dir =~ /^(.*)\//;
		my $a = $1;
		$my_html =~ /^(.*)\//;
		my $b = $1;
		while (defined $a and defined $b and $a eq $b and $a ne "") {
		    $my_dir  =~ s{^$a/}{};
		    $my_html =~ s{^$a/}{};
		    $my_dir =~ /^(.*)\//;
		    $a = $1;
		    $my_html =~ /^(.*)\//;
		    $b = $1;
		}
		#$html =~ s{^[^/]*/}{../};
		$html = "../$my_html";
	    }
	}
	print $idx " <li><a href=\"$html\">$mod</a>\n";
    }
    print $idx <<'FOOTER';
</ul>
</div>
</body>
</html>
FOOTER
}
