# vim:ft=perl
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

package Installcheck::Run;

=head1 NAME

Installcheck::Run - utilities to set up and run amanda dumps and restores

=head1 SYNOPSIS

  use Installcheck::Run;

  my $testconf = Installcheck::Run::setup();
  # make any modifications you'd like to the configuration
  $testconf->write();

  ok(Installcheck::Run::run('amdump', 'TESTCONF'), "amdump completes successfully");

  # It's generally polite to clean up your mess, although the test
  # framework will clean up if your tests crash
  Installcheck::Run::cleanup();

  SKIP: {
    skip "Expect.pm not installed", 7
	unless $Installcheck::Run::have_expect;

    my $exp = Installcheck::Run::run_expect('amflush', 'TESTCONF');
    $exp->expect(..);
    # ..
  }

=head1 USAGE

High-level tests generally depend on a full-scale run of Amanda --
a fairly messy project.  This module simplifies that process by
abstracting away the mess.  It takes care of:

=over

=item Setting up a holding disk;

=item Setting up several vtapes; and

=item Setting up a DLE pointing to a reasonably-sized subdirectory of the build directory.

=back

Most of this magic is in C<setup()>, which returns a configuration object from
C<Installcheck::Config>, allowing the test to modify that configuration before
writing it out.  The hostname for the DLE is "localhost", and the disk name is
available in C<$Installcheck::Run::diskname>.  This DLE has a subdirectory
C<dir> which can be used as a secondary, smaller DLE if needed.

This module also provides a convenient Perlish interface for running Amanda
commands: C<run($app, $args, ...)>.  This function runs $app (from $sbindir if
$app is not an absolute path), and returns true if the application exited with
a status of zero.  The stdout and stderr of the application are left in
C<$Installcheck::Run::stdout> and C<stderr>, respectively.

To check that a run is successful, and return its stdout (chomped), use
C<run_get($app, $args, ...)>.  This function returns C<''> if the application
returns a nonzero exit status.  Since many Amanda applications send normal
output to stderr, use C<run_get_err($app, $args, ...)> to check that a run is
successful and return its stderr.  Similarly, C<run_err> checks that a run
returns a nonzero exit status, and then returns its stderr, chomped.  If you
need both, use a bare C<run> and then check C<$stderr> and C<$stdout> as needed.

C<run> and friends can be used whether or not this module's C<setup>
was invoked.

Finally, C<cleanup()> cleans up from a run, deleting all backed-up
data, holding disks, and configuration.  It's just good-neighborly
to call this before your test script exits.

=head2 VTAPES

This module sets up a configuration with three 30M vtapes, replete with
the proper vtape directories.  These are controlled by C<chg-disk>.
The tapes are not labeled, and C<autolabel> is not set by
default, although C<labelstr> is set to C<TESTCONF[0-9][0-9]>.

The vtapes are created in <$Installcheck::Run::taperoot>, a subdirectory of
C<$Installcheck::TMP> for ease of later deletion.  The subdirectory for each
slot is available from C<vtape_dir($slot)>, while the parent directory is
available from C<vtape_dir()>.  C<load_vtape($slot)> will "load" the indicated
slot just like chg-disk would, and return the resulting path.

=head2 HOLDING

The holding disk is C<$Installcheck::Run::holdingdir>.  It is a 15M holding disk,
with a chunksize of 1M (to help exercise the chunker).

=head2 DISKLIST

The disklist is empty by default.  Use something like the following
to add an entry:

  $testconf->add_dle("localhost $diskname installcheck-test");

The C<installcheck-test> dumptype specifies
  auth "local"
  compress none
  program "GNUTAR"

but of course, it can be modified by the test module.

=head2 INTERACTIVE APPLICATIONS

This package provides a rudimentary wrapper around C<Expect.pm>, which is not
typically included in a perl installation.  Consult C<$have_expect> to see if
this module is installed, and skip any Expect-based tests if it is not.

Otherwise, C<run_expect> takes arguments just like C<run>, but returns an Expect
object which you can use as you would like.

=head2 DIAGNOSTICS

If your test runs 'amdump', a nonzero exit status may not be very helpful.  The
function C<amdump_diag> will attempt to figure out what went wrong and display
useful information for the user via diag().  If it is given an argument, then
it will C<BAIL_OUT> with that message, causing L<Test::Harness> to stop running
tests.  Otherwise, it will simply die(), which will only terminate this
particular test script.

=cut

use Installcheck;
use Installcheck::Config;
use Amanda::Paths;
use File::Path;
use IPC::Open3;
use Cwd qw(abs_path getcwd);
use Carp;
use POSIX qw( WIFEXITED );
use Test::More;
use Amanda::Config qw( :init );
use Amanda::Util qw(slurp);

require Exporter;

@ISA = qw(Exporter);
@EXPORT_OK = qw(setup
    run run_get run_get_err run_out run_err
    cleanup
    $diskname $taperoot $holdingdir
    $stdout $stderr $exit_code
    clean_taperoot
    load_vtape load_vtape_res vtape_dir
    amdump_diag run_expect
    is_sort_array
    check_amreport check_amstatus );
@EXPORT = qw(exp_continue exp_continue_timeout);

# global variables
our $stdout = '';
our $stderr = '';

our $have_expect;

BEGIN {
    eval "use Expect;";
    if ($@) {
	$have_expect = 0;
	sub ignore() { };
	*exp_continue = *ignore;
	*exp_continue_timeout = *ignore;
    } else {
	$have_expect = 1;
    }
};

# common paths (note that Installcheck::Dumpcache assumes these do not change)
our $diskname = "$Installcheck::TMP/backmeup";
our $taperoot = "$Installcheck::TMP/vtapes";
our $holdingdir ="$Installcheck::TMP/holding";

sub setup {
    my $nb_slot = shift;
    my $testconf = Installcheck::Config->new();

    $nb_slot = 3 if !defined $nb_slot;
    (-d $diskname) or setup_backmeup();
    setup_new_vtapes($testconf, $nb_slot);
    setup_holding($testconf, 25);
    setup_disklist($testconf);

    return $testconf;
}

# create the 'backmeup' data
sub setup_backmeup {
    my $dir_structure = {
	'1megabyte' => 1024*1024,
	'1kilobyte' => 1024,
	'1byte' => 1,
	'dir' => {
	    'ff' => 182,
	    'gg' => 2748,
	    'subdir' => {
		'subsubdir' => {
		    '10k' => 1024*10,
		},
	    },
	},
    };

    rmtree($diskname);
    mkpath($diskname) or die("Could not create $name");

    # pick a file for 'random' data -- /dev/urandom or, failing that,
    # Amanda's ChangeLog.
    my $randomfile = "/dev/urandom";
    if (!-r $randomfile) {
	$randomfile = "../ChangeLog";
    }

    my $rfd;
    $create = sub {
	my ($parent, $contents) = @_;
	while (my ($name, $val) = each(%$contents)) {
	    my $name = "$parent/$name";
	    if (ref($val) eq 'HASH') {
		mkpath($name) or die("Could not create $name");
		$create->($name, $val);
	    } else {
		my $bytes_needed = $val+0;
		open(my $wfd, ">", $name) or die("Could not open $name: $!");

		# read bytes from a source file as a source of "random" data..
		while ($bytes_needed) {
		    my $buf;
		    if (!defined($rfd)) {
			open($rfd, "<", "$randomfile") or die("Could not open $randomfile");
		    }
		    my $to_read = $bytes_needed>10240? 10240:$bytes_needed;
		    my $bytes_read = sysread($rfd, $buf, $to_read);
		    print $wfd $buf;
		    if ($bytes_read < $to_read) {
			close($rfd);
			$rfd = undef;
		    }

		    $bytes_needed -= $bytes_read;
		}
	    }
	}
    };

    $create->($diskname, $dir_structure);
}

sub clean_taperoot {
    my $ntapes = shift;
    if (-d $taperoot) {
	rmtree($taperoot);
	mkdir($taperoot);
	# make each of the tape directories
	for (my $i = 1; $i < $ntapes+1; $i++) {
	    my $tapepath = "$taperoot/slot$i";
	    mkpath("$tapepath");
	}
	load_vtape(1);
    }
}

sub setup_new_vtapes {
    my ($testconf, $ntapes) = @_;
    if (-d $taperoot) {
	rmtree($taperoot);
    }

    # make each of the tape directories
    for (my $i = 1; $i < $ntapes+1; $i++) {
	my $tapepath = "$taperoot/slot$i";
	mkpath("$tapepath");
    }

    # set up the appropriate configuration
    $testconf->add_param("tpchanger", "\"chg-disk:$taperoot\"");
    $testconf->add_param("labelstr", "\"TESTCONF[0-9][0-9]\"");
    $testconf->add_param("tapecycle", "$ntapes");

    # this overwrites the existing TEST-TAPE tapetype
    $testconf->add_tapetype('TEST-TAPE', [
	'length' => '30 mbytes',
	'filemark' => '4 kbytes',
    ]);
$testconf->write();
    load_vtape(1);

}

sub setup_changer {
    my $testconf = shift;
    my $number = shift;
    my $ntapes = shift;

    $testconf->add_changer("changer-$number", [
		'tpchanger', "\"chg-diskflat:$taperoot-$number\"",
		'property', "\"num-slot\" \"$ntapes\"",
		'property', '"auto-create-slot" "yes"'
	]);
    rmtree "$taperoot-$number";
    mkdir "$taperoot-$number";
}

sub setup_storage {
    my $testconf = shift;
    my $number = shift;
    my $ntapes = shift;
    my @setting = @_;

    Installcheck::Run::setup_changer($testconf, $number, $ntapes);
    $testconf->add_storage("storage-$number", [
		'tpchanger', "\"changer-$number\"",
		'autolabel', "\"STO-$number-\$5s\" any",
		'labelstr', 'MATCH-AUTOLABEL',
		'tapepool', "\"POOL-$number\"",
		'max-dle-by-volume', '1',
		'autoflush', 'yes',
		@setting,
	]);
}

sub setup_holding {
    my ($testconf, $mbytes) = @_;

    if (-d $holdingdir) {
	rmtree($holdingdir);
    }
    mkpath($holdingdir);

    $testconf->add_holdingdisk("hd1", [
	'directory' => "\"$holdingdir\"",
	'use' => "$mbytes mbytes",
	'chunksize' => "1 mbyte",
    ]);
}

sub setup_disklist {
    my ($testconf) = @_;

    $testconf->add_dumptype("installcheck-test", [
	'auth' => '"local"',
	'compress' => 'none',
	'program' => '"GNUTAR"',
    ]);
}

sub vtape_dir {
    my ($slot) = @_;
    if (defined($slot)) {
        return "$taperoot/slot$slot";
    } else {
        return "$taperoot";
    }
}

sub load_vtape {
    my ($slot) = @_;

    system("$sbindir/amtape TESTCONF slot $slot > /dev/null 2>/dev/null");
    # make the data/ symlink from our taperoot
#    unlink("$taperoot/data");
#    symlink(vtape_dir($slot), "$taperoot/data")
#	or die("Could not create 'data' symlink: $!");
#
#    return $taperoot;
}

sub load_vtape_res {
    my ($chg, $slot) = @_;
    my $res;

    $chg->load(slot => $slot,
	res_cb => sub {
	    (my $err, $res) = @_;
	    if ($err) {
		BAIL_OUT("load_vtape_res: device error");
            }
	    Amanda::MainLoop::quit();
	});
    Amanda::MainLoop::run();
    return $res;
}

sub run {
    my $app = shift;
    my @args = @_;
    my $errtempfile = "$Installcheck::TMP/stderr$$.out";

    # use a temporary file for error output -- this eliminates synchronization
    # problems between reading stderr and stdout
    local (*INFH, *OUTFH, *ERRFH);
    open(ERRFH, ">", $errtempfile);

    $app = "$sbindir/$app" unless ($app =~ qr{/});
    my $pid = IPC::Open3::open3("INFH", "OUTFH", ">&ERRFH",
	"$app", @args);

    # immediately close the child's stdin
    close(INFH);

    # read from stdout until it's closed
    $stdout = do { local $/; <OUTFH> };
    close(OUTFH);

    # and wait for the kid to die
    waitpid $pid, 0 or croak("Error waiting for child process to die: $@");
    my $status = $?;
    close(ERRFH);

    # fetch stderr from the temporary file
    $stderr = slurp($errtempfile);
    unlink($errtempfile);

    # and return true if the exit status was zero
    $exit_code = $status >> 8;
    return WIFEXITED($status) && $exit_code == 0;
}

sub run_get {
    if (!run @_) {
	my $detail = '';
	# prefer to put stderr in the output
	if ($stderr) {
	    $detail .= "\nstderr is:\n$stderr";
	} else {
	    if ($stdout and length($stdout) < 1024) {
		$detail .= "\nstdout is:\n$stdout";
	    }
	}
	Test::More::diag("run unexpectedly failed; no output to compare$detail");
	return '';
    }

    my $ret = $stdout;
    chomp($ret);
    return $ret;
}

sub run_get_err {
    if (!run @_) {
	my $detail = "\nstderr is:\n$stderr";
	Test::More::diag("run unexpectedly failed; no output to compare$detail");
	return '';
    }

    my $ret = $stderr;
    chomp($ret);
    return $ret;
}

sub run_out {
    if (run @_) {
	Test::More::diag("run unexpectedly succeeded; no output to compare");
	return '';
    }

    my $ret = $stdout;
    chomp($ret);
    return $ret;
}

sub run_err {
    if (run @_) {
	Test::More::diag("run unexpectedly succeeded; no output to compare");
	return '';
    }

    my $ret = $stderr;
    chomp($ret);
    return $ret;
}

sub cleanup {
    Installcheck::Config::cleanup();

    if (-d $taperoot) {
	rmtree($taperoot);
    }
    if (-d $holdingdir) {
	rmtree($holdingdir);
    }
}

sub run_expect {
    my $app = shift;
    my @args = @_;

    die "Expect.pm not found" unless $have_expect;

    $app = "$sbindir/$app" unless ($app =~ qr{^/});
    my $exp = Expect->new("$app", @args);

    return $exp;
}

sub amdump_diag {
    my ($msg) = @_;

    # try running amreport
    my $report = "failure-report.txt";
    unlink($report);
    my @logfiles = <$CONFIG_DIR/TESTCONF/log/log.*>;
    if (@logfiles > 0) {
	run('amreport', 'TESTCONF', '-f', $report, '-l', $logfiles[$#logfiles]);
	if (-f $report) {
	    open(my $fh, "<", $report) or return;
	    for my $line (<$fh>) {
		Test::More::diag($line);
	    }
	    unlink($report);
	    goto bail;
	}
    }

    # maybe there was a config error
    config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_WARNINGS) {
	foreach (@cfgerr_errors) {
	    Test::More::diag($_);
	}
	goto bail;
    }

    # huh.
    Test::More::diag("no amreport available, and no config errors");

bail:
    if ($msg) {
	Test::More::BAIL_OUT($msg);
    } else {
	die("amdump failed; cannot continue");
    }
}

sub get_sortable($) {
    my ($a) = $_[0];
    $a =~ s{(\\.)[+]?}{$1}g;
    $a =~ s{\*|\*\?|\?}{}g;
    $a =~ s{\\s}{ }g;
    $a =~ s{\\d}{0}g;
    $a =~ s{\\}{}g;
    $a =~ s{^\\s*}{};
    return $a;
}

sub diag_diff
{
    my ( $a, $b, $text ) = @_;

# line betwwen '  /--' and '  \--------' can be in varying order

    my $fail = 0;

    my @a = split /\n/, $a;
    my @b = split /\n/, $b;
    my ($na,$nb) = (0, 0);
    my ($blanks) = 0;
    while (defined(my $la = shift @a)) {
        # revert auto-inc for nb
        ++$blanks,--$nb,next
           if ( $la !~ m/\S/ && $b[0] !~ m/\S/ ); # both are blank? absorb recvd line

	my $lb = shift @b;

        # absorb all match-blank lines after some blank<->blank match region is over
        ($lb = shift @b),++$nb
           while ( $blanks && @b && $lb !~ m/\S/ );

        $blanks = 0;

	if ($la =~ /^  \/-- /) {
	    my @ax;
	    my @bx;
	    push @ax, $la;
	    push @bx, $lb;
	    while (($la = shift @a) !~ /  \\--------/) {
		$lb = shift @b;
		push @ax, $la;
		push @bx, $lb;
	    }
	    $lb = shift @b;

            # modify out regex and sort the same way (to keep the same)
	    @ax = grep { m/\S/; } 
                  sort { our ($a,$b); return get_sortable($a) cmp get_sortable($b); } 
                     @ax;
	    @bx = grep { m/\S/; } 
                  sort { our ($a,$b); return get_sortable($a) cmp get_sortable($b); } 
                     @bx;
	    while (defined(my $xa = shift @ax)) {
		my $xb = shift @bx;
		if ($xa !~ /^$xb$/){
		    $fail = 1;
		    diag("-$xa");
		    diag("+$xb");
		#} else {
		#    diag("=$xb");
                }
	    }
	}
	if ($la !~ /^$lb$/){
            $fail = 1;
	    diag("[$na]-$la");
	    diag("[$nb]+$lb");
        #} else {
        #   diag("[$na/$nb]=$lb") if ( $na != $nb );
        #   diag("[$na]=$lb") if ( $na == $nb );
        }
    } continue {
        ++$nb;
        ++$na;
    }
    foreach my $lb (@b) {
	$fail = 1;
	diag("+(extra) $lb");
    }
    ok(!$fail, "$text: match");

}

sub is_sort_array
{
    my $a = shift;
    my $b = shift;
    my $text =shift;

    my @aa = sort @$a;
    my @bb = sort @$b;

    is_deeply(\@aa, \@bb, $text);
}

sub my_quotemeta($) {
    # allow spaces, tabs, commas, colons and dashes to be unquoted to make matching easier in program regexes
    my $v = quotemeta($_[0]);
    $v =~ s{\\([-@%: \t/,\n\r])}{$1}gs;
    $v =~ s{\\$}{}gm;
    return $v;
}

sub check_amreport
{
    my $report = my_quotemeta(shift);
    my $timestamp = shift;
    my $text = shift || 'amreport';
    my $sorting = shift;
    my $skip_size = shift;
    my $got_report;
    $skip_size = 1 if !defined $skip_size;

    # change patterns into regex... so escape fresh regex-style chars first

    # absorb \. here.. (escaped .)
    $report =~ s/999999\\\.9/\\s*\\d+\\\.\\d/g;
    $report =~ s/0:00/\\s*\\d:\\d\\d/g;
    # absorb \* here.. (escaped *)
    $report =~ s/\s+(\\\*)?--\n/\\s*--\n/mg;

    $report =~ s/sendbackup:\s+(\w+-CRC)\b.*?:(\d+)/sendbackup:\\s+$1.*?([[:xdigit:]]+):$2/g;
    $report =~ s/PID/\\s*\\d+/g;

    my ($year, $month, $day) = ($timestamp =~ m/^(\d\d\d\d)(\d\d)(\d\d)/);
    my $date  = POSIX::strftime('%B %e, %Y', 0, 0, 0, $day, $month - 1, $year - 1900);
    $date =~ s/  / /g;
    $date = my_quotemeta($date);
    my $hostname = `hostname`;
    chomp $hostname;
    $hostname = my_quotemeta($hostname);
    my $version = my_quotemeta($Amanda::Constants::VERSION);

    # require correct date, version and hostname to match
    $report =~ s/Date\s+:\s+.*$/Date\\s+:\\s+$date/mg;
    $report =~ s/Hostname:\s+.*$/Hostname:\\s+$hostname/mg;
    $report =~ s/brought to you by Amanda version .*\\/brought to you by Amanda version $version\\/g;

    run("amreport", 'TESTCONF');

    if ($sorting) {
	my @lines = split "\n", $Installcheck::Run::stdout;
	if ($skip_size) {
	    @lines = grep { $_ !~ /^  sendbackup: size/ } @lines;
	}
	my @new_lines;
	my $in_usage_by_tape = 0;
	my $in_notes = 0;
	my @notes;
	my @usage_by_tapes;

	foreach my $line (@lines) {
	    if ($in_usage_by_tape) {
		if ($line eq '') {
		    push @new_lines, sort @usage_by_tapes;
		    push @new_lines, $line;
		    $in_usage_by_tape = 0;
		} else {
		    push @usage_by_tapes, $line;
		}
	    } elsif ($in_notes) {
		if ($line eq '') {
		    push @new_lines, sort @notes;
		    push @new_lines, $line;
		    $in_notes = 0;
		} else {
		    push @notes, $line;
		}
	    } else {
		push @new_lines, $line;
		if ($line =~ /^  Label/) {
		    $in_usage_by_tape = 1;
		} elsif ($line =~ /^NOTES:/) {
		    $in_notes = 1;
		}
	    }
	}
	$got_report = join "\n", @new_lines;
	$got_report .= "\n";
    } else {
	if ($skip_size) {
	    my @lines = split "\n", $Installcheck::Run::stdout;
	    @lines = grep { $_ !~ /^  sendbackup: size/ } @lines;
	    $got_report = join "\n", @lines;
	    $got_report .= "\n";
	} else {
	    $got_report = $Installcheck::Run::stdout;
	}
    }

#    ok($got_report =~ $report, "$text: match") || diag_diff($got_report, $report, $text);
    diag_diff($got_report, $report, $text);
#diag("stdout::::${Installcheck::Run::stdout}::::\n");
#diag("got_report::::${got_report}::::\n");
#diag("report::::${report}::::\n");

}

sub check_amstatus
{
    my $status = my_quotemeta(shift);
    my $tracefile = shift;
    my $text = shift || 'amstatus';

    # ( NN.NN%) --> match a decimal number with backref
    $status =~ s{\s+   \\\( \s* \d+ \\\. \d+ % \\\) }
                {\\s+\\(\\s*(\\d+[.]\\d*)%\\)}gx;

    # 00:00:00 --> allow any time
    $status =~ s/00:00:00/[\\s\\d]\\d:\\d\\d:\\d\\d/g;
    # " *--\n" or " --\n" matches 
    $status =~ s/\s+(\\\*)--\n/\\s+[*]?--\n/mg;

    # match correct tracefile
    $status =~ s/Using:\s+.*$/Using:\\s+${tracefile}/mg;

    # match any "From " line with anything following
    $status =~ s/From\s.*$/From\\s.*/mg;

    # append a "allow-anything" spacing for "dumper/dumpers busy" lines.. after a real backref paren
    $status =~ s/^[\s\d]*dumpers? busy.*?\\\)/$&.*/mg;

    # discard any not-idle line with anything following
    $status =~ s/^\s*not-idle.*\n//mg;

    # match any number for "holding space" and an amount of "NNNk"
    $status =~ s/^holding space\s+:\s+\d+k/holding space\\s+:\\s+\\d+k/mg;

    run("amstatus", 'TESTCONF', '--file', $tracefile);

    my $got_status = $Installcheck::Run::stdout;
    # discard any not-idle received lines
    $got_status =~ s/^\s*not-idle.*\n//mg;

#    ok($got_status =~ $status, "$text: match") || diag_diff($got_status, $status, $text);
    diag_diff($got_status, $status, $text);
#diag("stdout $got_status");
#diag("status: $status");
}
1;
