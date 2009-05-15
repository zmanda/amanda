#! @PERL@
# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use strict;
use warnings;
use Pod::Html;
use File::Basename;
use File::Path;
use File::Temp;

my ($targetdir, @sources) = @ARGV;
@sources = sort @sources;

my %dirs = ( '' => 1 );
my ($dir, $pm);

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

    pod2html("--podpath=.",
	    "--htmlroot=/pod",
	    "--infile=$tmp",
	    "--css=/pod/amperl.css",
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
    $html =~ s{<link rev="made" [^>]*/>}{};
    $html =~ s{html">the (\S+) manpage</a>}{html">\1</a>}g;

    # write it out
    open(my $fh, ">", $filename) or die("open $filename: $!");
    print $fh $html;
    close($fh);
}

# and generate an index HTML for each new directory
# we created.
for $dir (keys %dirs) {
	open(my $idx, ">", "$targetdir/$dir/index.html") or die("Error opening $dir/index.html: $!");
	print $idx <<'HEADER';
<?xml version="1.0" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<link rel="stylesheet" href="/pod/amperl.css" type="text/css" />
<meta http-equiv="content-type" content="text/html; charset=utf-8" />
</head>
<body>
<div class="pod">
<h1 class="module">Module List</h1>
<ul>
HEADER
	for $pm (@sources) {
		my $html = pm2html($pm);
		my $mod = pm2module($pm);
		next unless ($pm =~ /^$dir/);
		print $idx " <li><a href=\"/pod/$html\">$mod</a>\n";
	}
	print $idx <<'FOOTER';
</ul>
</div>
</body>
</html>
FOOTER
}
