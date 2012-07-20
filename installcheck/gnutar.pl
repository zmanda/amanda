# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 201;
use File::Path;
use Data::Dumper;
use POSIX qw( WIFEXITED );
use warnings;
use strict;

use lib "@amperldir@";
use Installcheck;
use IPC::Open3;
use Amanda::Constants;
use Amanda::Util qw( slurp );

## this is an unusual installcheck, because it does not test anything about
## Amanda itself.  However, it validates the accuracy of our understanding of
## GNU Tar's behavior, as recorded at
##  http://wiki.zmanda.com/index.php/GNU_Tar_Include_and_Exclude_Behavior

my $gnutar = $Amanda::Constants::GNUTAR;
$gnutar = $ENV{'GNUTAR'} if exists $ENV{'GNUTAR'};

## get set up

my @filenames = (qw{A*A AxA B?B BxB C[C CC D]D E\E F'F G"G}, 'H H');

my $tarfile = "$Installcheck::TMP/gnutar-tests.tar";
my $datadir = "$Installcheck::TMP/gnutar-tests";

sub make_tarfile
{
    my @extra_args = @_;

    rmtree($datadir) if -e $datadir;
    mkpath($datadir);

    for my $fn (@filenames) {
	open(my $fh, ">", "$datadir/$fn");
	print $fh "data";
	close($fh);
    }

    system($gnutar, "-C", $datadir, "-cf", $tarfile, @extra_args, '.');
    die "could not run gnutar" unless $? == 0;

    rmtree($datadir) if -e $datadir;
}

## gnutar version

my ($v, $numeric_version);
{
    my $verstring = `$gnutar --version`;
    die "could not run gnutar" unless $? == 0;
    ($v) = ($verstring =~ /tar \(GNU tar\) *([0-9.]+)/);
    my ($maj, $min, $mic) = ($v =~ /([0-9]+)\.([0-9]+)(?:\.([0-9]+))?/);

    $numeric_version = 0;
    $numeric_version += $maj * 10000 if $maj;
    $numeric_version += $min * 100 if $min;
    $numeric_version += $mic if $mic;

}

my ($fc14, $fc15);
{
    my $uname = `uname -a`;
    if ($uname =~ /\.fc14\./) {
	$fc14 = 1;
    }
    if ($uname =~ /\.fc15\./) {
	$fc15 = 1;
    }
    if ($uname =~ /\.fc16\./) { #like fc15
	$fc15 = 1;
    }
}

# see if the default for --wildcards during inclusion has been changed
my $wc_default_changed = 0;
{
    my $help_output = `$gnutar --help`;
    # redhatty patches helpfully change the help message
    if ($help_output =~ /--wildcards\s*use wildcards \(default\)$/m) {
	$wc_default_changed = 1;
    }
}

my %version_classes = (
    '<1.16' => $numeric_version < 11591,
    '>=1.16' => $numeric_version >= 11591,
    '>=1.16-no-wc' => $numeric_version >= 11591 && !$wc_default_changed, # normal
    '>=1.16-wc' => $numeric_version >= 11591 && $wc_default_changed, # stupid distros screw things up!
    '1.16..<1.25' => $numeric_version >= 11591 && $numeric_version < 12500,

    '<1.23' => $numeric_version < 12300,
    '>=1.23' => $numeric_version >= 12300,
    '*' => 1,
    '1.23' => ($numeric_version >= 12290 and $numeric_version <= 12300),
    '1.23fc14' => ($numeric_version == 12300 and $fc14),
    '!1.23' => ($numeric_version < 12290 || ($numeric_version > 12300 && $numeric_version < 12500)),
    '>=1.25' => $numeric_version >= 12500,
    'fc15' => ($numeric_version >= 12500 and $fc15),
);

# include and exclude all use the same set of patterns and filenames
my $patterns = [
	'./A*A' =>	'A*A',
	'./A*A' =>	'AxA',
	'./B?B' =>	'B?B',
	'./B?B' =>	'BxB',
	'./C[C' =>	'C[C',
	'./D]D' =>	'D]D',
	'./E\\E' =>	'E\\E',
	'./F\'F' =>	'F\'F',
	'./G"G' =>	'G"G',
	'./H H' =>	'H H',
	'./A\\*A' =>	'A*A',
	'./A\\*A' =>	'AxA',
	'./B\\?B' =>	'B?B',
	'./B\\?B' =>	'BxB',
	'./C\\[C' =>	'C[C',
	'./D\\]D' =>	'D]D',
	'./E\\\\E' =>	'E\\E',
	'./F\\\'F' =>	'F\'F',
	'./G\\"G' =>	'G"G',
	'./H\\ H' =>	'H H',
];

my $named_expectations = [
    [ 'alpha',
         'beta',
	    'gamma',
	       'delta',
	          'epsilon',
		     'zeta',
		        'eta',
		           'iota',
                              'empty', ],
    #  al be ga de ep ze et io empty
    [  1, 1, 1, 1, 1, 1, 1, 1, 1,     ], # './A*A' =>	'A*A',
    [  1, 1, 1, 1, 0, 1, 1, 1, 0,     ], # './A*A' =>	'AxA',
    [  1, 1, 1, 1, 1, 1, 1, 1, 1,     ], # './B?B' =>	'B?B',
    [  1, 1, 1, 1, 0, 1, 1, 1, 0,     ], # './B?B' =>	'BxB',
    [  0, 0, 0, 0, 1, 1, 1, 1, 1,     ], # './C[C' =>	'C[C',
    [  1, 1, 1, 1, 1, 1, 1, 1, 1,     ], # './D]D' =>	'D]D',
    [  1, 0, 0, 1, 1, 0, 0, 1, 1,     ], # './E\\E' =>	'E\\E',
    [  1, 1, 1, 1, 1, 1, 1, 1, 1,     ], # './F\'F' =>	'F\'F',
    [  1, 1, 1, 1, 1, 1, 1, 1, 1,     ], # './G"G' =>	'G"G',
    [  1, 1, 1, 1, 1, 1, 1, 1, 1,     ], # './H H' =>	'H H',
    [  1, 1, 1, 0, 0, 1, 1, 0, 0,     ], # './A\\*A' =>	'A*A',
    [  0, 0, 0, 0, 0, 0, 0, 0, 0,     ], # './A\\*A' =>	'AxA',
    [  0, 0, 1, 0, 0, 0, 1, 0, 0,     ], # './B\\?B' =>	'B?B',
    [  0, 0, 0, 0, 0, 0, 0, 0, 0,     ], # './B\\?B' =>	'BxB',
    [  1, 1, 1, 0, 0, 1, 1, 0, 0,     ], # './C\\[C' =>	'C[C',
    [  0, 1, 1, 0, 0, 1, 1, 0, 0,     ], # './D\\]D' =>	'D]D',
    [  1, 0, 1, 0, 1, 0, 1, 0, 0,     ], # './E\\\\E' =>	'E\\E',
    [  0, 1, 1, 0, 0, 1, 1, 0, 0,     ], # './F\\\'F' =>	'F\'F',
    [  0, 1, 1, 0, 0, 1, 1, 0, 0,     ], # './G\\"G' =>	'G"G',
    [  0, 1, 1, 0, 0, 1, 1, 0, 0,     ], # './H\\ H' =>	'H H',
];

sub get_expectation {
    my ($name) = @_;
    my @names = @{$named_expectations->[0]};

    # get the index for that greek letter
    my $i;
    for (0 .. $#names) {
	if ($names[$_] eq $name) {
	    $i = $_;
	    last;
	}
    }

    # then assemble the result
    my @rv;
    my @exps = @$named_expectations;
    shift @exps;
    for (@exps) {
	push @rv, $_->[$i];
    }

    return @rv;
}

sub get_matching_type {
    my ($expectations) = @_;

    # find the type for the first matching version
    foreach my $exp (@$expectations) {
	foreach (keys %$exp) {
	    if ($version_classes{$_}) {
		return $exp->{$_};
	    }
	}
    }
    return undef;
}

sub get_version_index {
    my @versions = @{$_[0]};

    my $vi;
    for (0 .. $#versions) {
	if ($version_classes{$versions[$_]}) {
	    return $_;
	}
    }
    return undef;
}

## utils

my ($stderr, $stdout, $exit_code);
sub run_gnutar {
    my %params = @_;
    my @args = @{ $params{'args'} };

    my $errtempfile = "$Installcheck::TMP/stderr$$.out";

    # use a temporary file for error output -- this eliminates synchronization
    # problems between reading stderr and stdout
    local (*INFH, *OUTFH, *ERRFH);
    open(ERRFH, ">", $errtempfile);

    local %ENV;
    if ($params{'env'}) {
	my %env = %{$params{'env'}};
	for (keys %env) {
	    $ENV{$_} = $env{$_};
	}
    }

    my $pid = IPC::Open3::open3("INFH", "OUTFH", ">&ERRFH", $gnutar, @args);
    my $cmdline = "$gnutar " . join(' ', @args);

    # immediately close the child's stdin
    close(INFH);

    # read from stdout until it's closed
    $stdout = do { local $/; <OUTFH> };
    close(OUTFH);

    # and wait for the kid to die
    waitpid $pid, 0 or croak("Error waiting for gnutar die: $@");
    my $status = $?;
    close(ERRFH);

    # fetch stderr from the temporary file
    $stderr = slurp($errtempfile);
    unlink($errtempfile);

    # get the exit status
    $exit_code = WIFEXITED($status)? ($status >> 8) : 0xffff;

    if ($exit_code != 0) {
	return 0;
    } else {
	return 1;
    }
}

## inclusion tests (using -x and filenames on the command line)

sub test_gnutar_inclusion {
    my %params = @_;

    my $matching_type = get_matching_type($params{'expectations'});

    # skip these tests if there's no matching version
    if (!defined $matching_type) {
	SKIP: {
	    my $msg = (join " ", @{$params{'extra_args'}}) .
			" not supported in version $v";
	    my $count = @$patterns / 2;
	    skip $msg, $count;
	}
	return;
    }

    make_tarfile();
    my @patterns = @$patterns;
    my @expectations = get_expectation($matching_type);
    while (@patterns) {
	my $pat = shift @patterns;
	my $file = shift @patterns;
	my $exp = shift @expectations;

	my $eargs = '';
	$eargs = ', ' . join(' ', @{$params{'extra_args'}}) if @{$params{'extra_args'}};
	my $match = $exp? "matches" : "does not match";
	my $msg = "inclusion$eargs, pattern $pat $match file $file";

	rmtree($datadir) if -e $datadir;
	mkpath($datadir);

	my $ok = run_gnutar(args => [ '-C', $datadir, '-x', '-f', $tarfile, @{$params{'extra_args'}}, $pat ]);
	$ok = 0 unless -f "$datadir/$file";
	if ($ok and !$exp) {
	    fail($msg);
	    diag("  unexpected success with version $v");
	} elsif (!$ok and $exp) {
	    fail($msg);
	    diag("  unexpected failure with version $v:\n$stderr");
	} else {
	    pass($msg);
	}
    }
    rmtree($datadir) if -e $datadir;
}

# We'll trust that the following logic is implemented correctly in GNU Tar
# --no-wildcards is the default (same as no args) (but not everywhere!!)
# --unquote is the default (same as no args) (this seems true universally)

test_gnutar_inclusion(
    extra_args => [],
    expectations => [
	{'<1.16' => 'alpha'},
        {'1.23fc14' => 'zeta'},
        {'fc15' => 'zeta'},
	{'>=1.16-no-wc' => 'epsilon'},
	{'>=1.16-wc' => 'beta'}, # acts like --wildcards
    ],
);

test_gnutar_inclusion(
    extra_args => [ '--no-wildcards' ],
    expectations => [
	{'<1.16' => 'alpha'},
	{'>=1.16' => 'epsilon'},
    ],
);

test_gnutar_inclusion(
    extra_args => [ '--no-unquote' ],
    expectations => [
	{'<1.16' => undef},
	{'1.23fc14' => 'eta'},
	{'fc15' => 'eta'},
	{'>=1.16-no-wc' => 'empty'},
	{'>=1.16-wc' => 'gamma'}, # acts like --wildcards --no-unquote
    ],
);

test_gnutar_inclusion(
    extra_args => [ '--no-wildcards', '--no-unquote' ],
    expectations => [
	{'<1.16' => undef},
	{'>=1.16' => 'empty'},
    ],
);

test_gnutar_inclusion(
    extra_args => [ '--wildcards' ],
    expectations => [
	{'<1.16' => 'alpha'},
        {'1.23fc14' => 'zeta'},
	{'1.16..<1.25' => 'beta'},
	{'>=1.25' => 'zeta'},
    ],
);

test_gnutar_inclusion(
    extra_args => [ '--wildcards', '--no-unquote' ],
    expectations => [
	{'<1.16' => undef},
	{'1.23fc14' => 'eta'},
	{'1.16..<1.25' => 'gamma'},
	{'>=1.25' => 'eta'},
    ],
);

## exclusion tests (using -t and filenames on the command line)

sub test_gnutar_exclusion {
    my %params = @_;

    my $matching_type = get_matching_type($params{'expectations'});

    # skip these tests if there's no matching version
    if (!defined $matching_type) {
	SKIP: {
	    my $msg = (join " ", @{$params{'extra_args'}}) .
			" not supported in version $v";
	    my $count = @$patterns; # two elements per test, but we run each one twice
	    skip $msg, $count;
	}
	return;
    }

    make_tarfile();
    my @patterns = @$patterns;
    my @expectations = get_expectation($matching_type);
    while (@patterns) {
	my $pat = shift @patterns;
	my $file = shift @patterns;
	my $exp = shift @expectations;

	my $eargs = '';
	$eargs = ', ' . join(' ', @{$params{'extra_args'}}) if @{$params{'extra_args'}};
	my $match = $exp? "matches" : "does not match";
	my $msg = "exclusion$eargs, extract, pattern $pat $match $file";

	rmtree($datadir) if -e $datadir;
	mkpath($datadir);

	my $ok = run_gnutar(args => [ '-C', $datadir, '-x', '-f', $tarfile, @{$params{'extra_args'}}, "--exclude=$pat" ]);

	# fail if the excluded file was extracted anyway..
	if ($ok) {
	    my $excluded_ok = ! -f "$datadir/$file";
	    if ($excluded_ok and !$exp) {
		fail($msg);
		diag("  exclusion unexpectedly worked with version $v");
	    } elsif (!$excluded_ok and $exp) {
		fail($msg);
		diag("  exclusion unexpectedly failed with version $v");
	    } else {
		pass($msg);
	    }
	} else {
	    fail($msg);
	    diag("  unexpected error exit with version $v:\n$stderr");
	}
    }

    # test again, but this time during a 'c'reate operation
    @patterns = @$patterns;
    @expectations = get_expectation($matching_type);
    while (@patterns) {
	my $pat = shift @patterns;
	my $file = shift @patterns;
	my $exp = shift @expectations;

	my $eargs = '';
	$eargs = ', ' . join(' ', @{$params{'extra_args'}}) if @{$params{'extra_args'}};
	my $match = $exp? "matches" : "does not match";
	my $msg = "exclusion$eargs, create, pattern $pat $match $file";

	# this time around, we create the tarball with the exclude, then extract the whole
	# thing.  We extract rather than using 't' because 't' has a funny habit of backslashing
	# its output that we don't want to deal with here.
	make_tarfile(@{$params{'extra_args'}}, "--exclude=$pat");

	rmtree($datadir) if -e $datadir;
	mkpath($datadir);
	my $ok = run_gnutar(args => [ '-C', $datadir, '-x', '-f', $tarfile]);

	# fail if the excluded file was extracted anyway..
	if ($ok) {
	    my $excluded_ok = ! -f "$datadir/$file";
	    if ($excluded_ok and !$exp) {
		fail($msg);
		diag("  exclusion unexpectedly worked with version $v");
	    } elsif (!$excluded_ok and $exp) {
		fail($msg);
		diag("  exclusion unexpectedly failed with version $v");
	    } else {
		pass($msg);
	    }
	} else {
	    fail($msg);
	    diag("  unexpected error exit with version $v:\n$stderr");
	}
    }

    rmtree($datadir) if -e $datadir;
}

# We'll trust that the following logic is implemented correctly in GNU Tar
# --wildcards is the default (same as no args)
# --no-unquote / --unquote has no effect

# --wildcards
test_gnutar_exclusion(
    extra_args => [],
    expectations => [
	{'!1.23' => 'gamma'},
	{'1.23fc14' => 'iota'},
	{'1.23' => 'delta'},
	{'>=1.25' => 'eta'},
    ],
);

# --no-wildcards
test_gnutar_exclusion(
    extra_args => [ '--no-wildcards' ],
    expectations => [
	{'*' => 'empty'},
    ],
);

## list (-t)

sub test_gnutar_toc {
    my %params = @_;

    my $vi = get_version_index($params{'versions'});

    my @patterns = @{ $params{'patterns'} };
    my @filenames;
    my @expectations;
    while (@patterns) {
	my $file = shift @patterns;
	my $exp = shift @patterns;
	$exp = $exp->[$vi];

	push @filenames, $file;
	push @expectations, $exp;
    }

    my $eargs = '';
    $eargs = ', ' . join(' ', @{$params{'extra_args'}}) if @{$params{'extra_args'}};
    my $msg = "list$eargs, with lots of funny characters";

    # make a tarfile containing the filenames, then run -t over it
    rmtree($datadir) if -e $datadir;
    mkpath($datadir);

    for my $fn (@filenames) {
	open(my $fh, ">", "$datadir/$fn")
	    or die("opening $datadir/$fn: $!");
	print $fh "data";
	close($fh);
    }

    system($gnutar, "-C", $datadir, "-cf", $tarfile, '.');
    die "could not run gnutar" unless $? == 0;

    rmtree($datadir) if -e $datadir;
    my %env;
    if ($params{'env'}) {
	%env = %{$params{'env'}};
    }
    my $ok = run_gnutar(args => [ '-t', '-f', $tarfile, @{$params{'extra_args'}}],
			env => \%env);
    if (!$ok) {
	fail($msg);
	diag("gnutar exited with nonzero status for version $v");
    }

    my @toc_members = sort split(/\n/, $stdout);
    shift @toc_members; # strip off './'
    is_deeply([ @toc_members ], [ @expectations ], $msg);
}

# there are no extra_args that seem to affect this behavior
test_gnutar_toc(
    extra_args => [],
    env => { LC_CTYPE => 'C' }, # avoid any funniness with ctypes
    versions =>  [ '*' ],
    patterns => [
	"A\007", [ './A\a' ],
	"B\010", [ './B\b' ],
	"C\011", [ './C\t' ],
	"D\012", [ './D\n' ],
	"E\013", [ './E\v' ],
	"F\014", [ './F\f' ],
	"G\015", [ './G\r' ],
	"H\\",   [ './H\\\\' ], # H\ -> H\\
	"I\177", [ './I\\177' ],
	"J\317\264", [ './J\\317\\264' ], # use legitimate utf-8, for mac os fs
	"K\\x",  [ './K\\\\x' ],
	"L\\\\", [ './L\\\\\\\\' ],
    ],
);

unlink($tarfile);
