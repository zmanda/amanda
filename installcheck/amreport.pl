# Copyright (c) 2009, 2010 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 161;

use strict;
use warnings;
use Errno;
use Cwd qw(abs_path);
use lib "@amperldir@";

use Installcheck;
use Installcheck::Run qw( run run_get run_err );
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Util qw( slurp burp );
use Amanda::Debug;
use Amanda::Config qw (:getconf);

# easy knob to twiddle to check amreport_new instead
my $amreport = "amreport";

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# we use Text::Diff for diagnostics if it's installed
my $have_text_diff;
BEGIN {
    eval "use Text::Diff;";
    if ($@) {
	$have_text_diff = 0;
    } else {
	$have_text_diff = 1;
    }
}

# set up a fake "printer" for postscript output
my $printer_output = "$Installcheck::TMP/postscript-output";
$ENV{'INSTALLCHECK_MOCK_LPR_OUTPUT'} = $printer_output;
$ENV{'INSTALLCHECK_MOCK_LPR'} = abs_path("mock") . "/lpr";

# and a fake template
my $ps_template = "$Installcheck::TMP/postscript-template";
burp($ps_template, "--PS-TEMPLATE--\n");

# and a fake "mail" for email output
my $mail_output = "$Installcheck::TMP/mail-output";
$ENV{'INSTALLCHECK_MOCK_MAIL_OUTPUT'} = $mail_output;
my $mail_mock = abs_path("mock") . "/mail";

my $old_log_filename="$Installcheck::TMP/installcheck-log.datastamp.0";
my $current_log_filename="$Installcheck::TMP/log";
my $out_filename="$Installcheck::TMP/installcheck-amreport-output";

sub setup_config {
    my %params = @_;
    my $testconf = Installcheck::Run::setup();

    my $mailer =
        $params{want_mailer}  ? "\"$mail_mock\""
      : $params{bogus_mailer} ? "\"$mail_mock.bogus\""
      :                         '""';                    #default

    $testconf->add_param('mailer', $mailer);
    $testconf->add_param('mailto',
        $params{want_mailto} ? '"nobody\@localhost"' : '""');

    $testconf->add_tapetype('TEST-TAPE-TEMPLATE', [
	'length' => '30 mbytes',
	'filemark' => '4 kbytes',
	$params{'want_template'}? ('lbl_templ' => "\"$ps_template\""):(),
    ]);
    $testconf->add_param('tapetype', "\"TEST-TAPE-TEMPLATE\"");

    $testconf->remove_param('send_amreport_on');
    $testconf->add_param('send_amreport_on',
        exists $params{send_amreport} ? uc($params{send_amreport}) : "ALL"
    );

    $testconf->remove_param('logdir');
    $testconf->add_param('logdir', "\"$Installcheck::TMP\"");
    $testconf->write();
}

sub cleanup {
    unlink $old_log_filename;
    unlink $current_log_filename;
    unlink $out_filename;
    unlink $mail_output;
    unlink $printer_output;
}

# compare two multiline strings, giving a diff if they do not match
sub results_match
{
    my ( $a_filename, $b, $msg ) = @_;

    if ($a_filename eq 'stdout') {
	$a = $Installcheck::Run::stdout;
	if (!$a) {
	    diag("stdout is empty");
	    fail($msg);
	    return;
	}
    } else {
	if (!-f $a_filename) {
	    diag("'$a_filename' does not exist");
	    fail($msg);
	    return;
	}
	$a = slurp($a_filename);
    }

    my $is_ps = ($b =~ m/^--PS-TEMPLATE--/);

    my $cleanup = sub {
        my $str = shift;
        chomp $str;

        # strip out special characters
        $str =~ tr{\f}{};

	if ($is_ps) {
	    # old amreport underquotes the postscript strings
	    $str =~ s{\\\(}{(}mgx;
	    $str =~ s{\\\)}{)}mgx;
	} else {
	    # make human report insensitive to whitespace differences
	    $str =~ s{[\t ]+}{ }mgx;
	    $str =~ s{\s*\n[\n ]*\s*}{\n}mgx;
	}

        # chomp the version lines
        $str =~ s{\n\(brought to you by Amanda version .*$}{\n<versioninfo>}g;
	$str =~ s{\(Amanda Version .*\) DrawVers}{(Amanda Version x.y.z) DrawVers}g;

        return $str;
    };

    $a = $cleanup->($a);
    $b = $cleanup->($b);

    if ( $a eq $b ) {
        pass($msg);
    } else {
        my $diff;
        if ($have_text_diff) {
            $diff = diff( \$a, \$b, { 'STYLE' => "Unified" } );
        } else {
            $diff = "---- GOT: ----\n$a\n---- EXPECTED: ----\n$b\n---- ----";
        }
        fail($msg);
        diag($diff);
    }
}

# convert a regular report into what we expect to see in a mail
sub make_mail {
    my ($report, $config, $error, $date, $addr) = @_;
    my $error_msg ="";
    $error_msg = " FAIL:" if $error;
    return <<EOF . $report;
\$ARGS = ['-s','$config${error_msg} AMANDA MAIL REPORT FOR $date','$addr'];
EOF
}

## read __DATA__ to a hash, keyed by the names following '%%%%'

my %datas;
my $key = undef;
while (<DATA>) {
    if (/^%%%% (.*)/) {
	$key = $1;
    } else {
	$datas{$key} .= $_;
    }
}

## and make a very long logfile
{
    my $trigger_line = "  . /bin/tar: ./var/run/acpid.socket: socket ignored\n";
    my $replacement = "  | /bin/tar: something crazy!\n" x 1000;
    $datas{'longstrange'} = $datas{'shortstrange'};
    $datas{'longstrange'} =~
	s{$trigger_line}{$replacement};

    # this set of 1000 lines is shortened in the report..
    $trigger_line = "--LINE-REPLACED-BY-91-LINES--\n";
    $replacement = "  | /bin/tar: something crazy!\n" x 91;
    $datas{'longstrange-rpt'} =~
	s{$trigger_line}{$replacement};
}

## try a few various options with a pretty normal logfile.  Note that
## these tests all use amreport's script mode

setup_config(want_mailer => 1, want_mailto => 1, want_template => 1);
cleanup();
burp($current_log_filename, $datas{'normal'});

like(run_err($amreport, 'TESTCONF-NOSUCH'),
    qr/could not open conf/,
    "amreport with bogus config exits with error status and error message");

ok(!run($amreport, 'TESTCONF-NOSUCH', '--help'),
    "amreport --help exits with status 1");
like($Installcheck::Run::stdout,
    qr/Usage: amreport conf/,
    "..and prints usage message");

like(run_get($amreport, 'TESTCONF-NOSUCH', '--version'),
    qr/^amreport-.*/,
    "amreport --version gives version");

ok(run($amreport, 'TESTCONF', '--from-amdump'),
    "amreport, as run from amdump, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");

ok(run($amreport, 'TESTCONF', '--from-amdump', '/garbage/directory/'),
    "amreport, as run from amdump, with mailer, mailto, and a template, and  bogus option")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");


cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-M', 'somebody@localhost'),
    "amreport -M, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "somebody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'normal'});

like(run_get($amreport, 'TESTCONF', '-i'),
    qr/nothing to do/,
    "amreport -i, with mailer, mailto, and a template, prints an error but exit==0");

cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-p', $out_filename),
    "amreport -p, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
ok(! -f $printer_output,
    "..doesn't print");
results_match($out_filename, $datas{'normal-postscript'}, "..postscript file matches");

# test a bare 'amreport', which should now output to stdout
cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF'),
    "amreport with no other options outputs to stdout for user convenience")
  or diag($Installcheck::Run::stderr);
results_match('stdout', $datas{'normal-rpt1'},
    "..output matches");
ok(!-f $printer_output, "..no printer output")
  or diag("error: printer output!:\n" . burp($printer_output));
ok(!-f $mail_output, "..no mail output")
  or diag("error: mail output!:\n" . burp($printer_output));

# test long-form file option
cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', "--text=$out_filename"),
    "amreport --text=foo, no other options")
  or diag($Installcheck::Run::stderr);
results_match($out_filename, $datas{'normal-rpt1'},
    "..output matches");
ok(!-f $printer_output, "..no printer output")
  or diag("error: printer output!:\n" . burp($printer_output));
ok(!-f $mail_output, "..no mail output")
  or diag("error: mail output!:\n" . burp($printer_output));

# test long form postscript option
cleanup();
burp($current_log_filename, $datas{'normal'});

ok(
    run($amreport, 'TESTCONF', '--ps', $out_filename),
    "amreport --ps foo, no other options"
);
results_match($out_filename, $datas{"normal-postscript"}, '..results match');
ok(!-f $printer_output, "..no printer output");
ok(!-f $mail_output, "..no mail output");

cleanup();

# test new mail option, using config mailto
setup_config(want_mailer => 1, want_mailto => 1, want_template => 1);
burp($current_log_filename, $datas{'normal'});
ok(run($amreport, 'TESTCONF', '--mail-text'),
    "amreport --mail-text, no other options, built-in mailto");
results_match(
    $mail_output,
    make_mail(
        $datas{'normal-rpt1'}, "DailySet1", 0,
        "February 25, 2009",   "nobody\@localhost"
    ),
    "..mail matches"
);
ok(!-f $printer_output, "..no printer output");
ok(!-f $out_filename,   "..no file output");

cleanup();

# test new mail option, using passed mailto
burp($current_log_filename, $datas{'normal'});
ok(run($amreport, 'TESTCONF', '--mail-text=somebody@localhost',),
    'amreport --mail-text=somebody\@localhost, no other options');
results_match(
    $mail_output,
    make_mail(
        $datas{'normal-rpt1'}, "DailySet1", 0,
        "February 25, 2009",   "somebody\@localhost"
    ),
    "..mail matches"
);
ok(!-f $printer_output, "..no printer output");
ok(!-f $out_filename, "..no file output");

cleanup();

# test long-form old log option
burp($old_log_filename, $datas{'normal'});
ok(
    run($amreport, 'TESTCONF', '--log', $old_log_filename),
    "amreport --log with old log, no other config options"
);
results_match('stdout', $datas{'normal-rpt1'},
    '..stdout output matches');
ok(!-f $mail_output, "..no mail output");
ok(!-f $out_filename, "..no file output");
ok(!-f $printer_output, "..no printer output");

cleanup();

# test long-form print option, without specified printer
setup_config(want_template => 1);
burp($current_log_filename, $datas{'normal'});
ok(run($amreport, 'TESTCONF', '--print'),
    'amreport --print, no other options');
results_match(
    $printer_output,
    $datas{'normal-postscript'},
    "..printer output matches"
);
ok(!-f $mail_output,  "..no mail output");
ok(!-f $out_filename, "..no file output");

cleanup();
burp($current_log_filename, $datas{'normal'});

setup_config(want_mailer => 1, want_mailto => 1, want_template => 1);
ok(run($amreport, 'TESTCONF', '-i', '-p', $out_filename),
    "amreport -i -p, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
ok(! -f $mail_output,
    "..produces no mail");
ok(! -f $printer_output,
    "..doesn't print");
results_match($out_filename,
    $datas{'normal-postscript'},
    "..postscript output in -p file matches");

cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-i', '-f', $out_filename),
    "amreport -i -f, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
ok(! -f $mail_output,
    "..produces no mail");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");
results_match($out_filename,
    $datas{'normal-rpt1'},
    "..report output in -f file matches");

cleanup();
burp($old_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-l', $old_log_filename),
    "amreport -l, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");

setup_config(want_mailer => 1);
cleanup();
burp($current_log_filename, $datas{'normal'});

like(run_get($amreport, 'TESTCONF', '--from-amdump'),
    qr/nothing to do/,
    "amreport --from-amdump, with mailer but no mailto and no template, "
    . "prints an error but exit==0");
ok(! -f $mail_output, "..doesn't mail");
ok(! -f $printer_output, "..doesn't print");

cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '--from-amdump', '-o', 'mailto=hello'),
    "amreport -o to set mailto, with mailer but no mailto and no template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "hello"),
    "..mail matches");
ok(! -f $printer_output,
    "..doesn't print");

cleanup();
burp($current_log_filename, $datas{'normal'});

like(run_err($amreport, 'TESTCONF', '--from-amdump', '-o', 'mailto=ill\egal'),
    qr/mail address has invalid characters/,
    "amreport with illegal email in -o, with mailer but no mailto and no template, errors out");

setup_config(want_mailer => 1, want_template => 1);
cleanup();
burp($current_log_filename, $datas{'normal'});

like(run_get($amreport, 'TESTCONF', '--from-amdump'),
    qr/nothing to do/, ## this is arguably a bug, but we'll keep it
    "no-args amreport with mailer, no mailto, and a template does nothing even though it could "
	. "print a label");
ok(! -f $mail_output, "..doesn't mail");
ok(! -f $printer_output, "..doesn't print");

cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '--from-amdump', '-o', 'mailto=dustin'),
    "amreport with mailer, no mailto, and a template, but mailto in config override works")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "dustin"),
    "..mail matches");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'doublefailure'});

ok(!run($amreport, 'TESTCONF', '-M', 'dustin'),
    "amreport with log in error")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'doublefailure-rpt'}, "DailySet1", 1, "March 26, 2009", "dustin"),
    "..mail matches");

cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-M', 'pcmantz'),
    "amreport with mailer, no mailto, and a template, but mailto in -M works")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "pcmantz"),
    "..mail matches");
results_match($printer_output,
    $datas{'normal-postscript'},
    "..printer output matches");

setup_config(want_template => 1);
cleanup();
burp($current_log_filename, $datas{'normal'});

like(run_get($amreport, 'TESTCONF', '-M', 'martineau'),
    qr/Warning: a mailer is not defined/,
    "amreport with no mailer, no mailto, and a template, but mailto in -M fails with "
	. "warning, but exit==0");
ok(! -f $mail_output, "..doesn't mail");
ok(! -f $printer_output, "..doesn't print");

setup_config(want_mailer => 1, want_mailto => 1);
cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-p', $out_filename), # XXX another probable bug
    "amreport with mailer, mailto, but no template, ignores -p option ")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
ok(! -f $printer_output,
    "..doesn't print");

cleanup();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-o', "tapetype:TEST-TAPE-TEMPLATE:lbl_templ=$ps_template",
					    '-p', $out_filename),
    "amreport with mailer, mailto, but no template, minds -p option if template given via -o")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($datas{'normal-rpt1'}, "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($out_filename,
    $datas{'normal-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'normal'});
setup_config(bogus_mailer => 1, want_mailto => 1, want_template => 1);

ok(run($amreport, 'TESTCONF', '--from-amdump'),
    "amreport with bogus mailer; doesn't mail, still prints")
  or diag($Installcheck::Run::stderr);
ok(!-f $mail_output, "..produces no mail output");
is($Installcheck::Run::stdout, "", "..produces no stdout output");
$! = &Errno::ENOENT;
my $enoent = $!;
like($Installcheck::Run::stderr,
     qr/^error: open3: exec of .*: $enoent$/, "..produces correct stderr output");
results_match(
    $printer_output,
    $datas{'normal-postscript'},
    "..printer output matches"
);

cleanup();
burp($current_log_filename, $datas{'normal'});
setup_config(
    want_mailer   => 1,
    want_mailto   => 1,
    want_template => 1,
    send_amreport => 'error'
);

ok(
    run($amreport, 'TESTCONF', '--from-amdump'),
"amreport with CNF_SEND_AMREPORT_ON set to errors only, no mail, still prints"
) or diag($Installcheck::Run::stderr);
ok(!-f $mail_output, "..produces no mail output") or diag(slurp $mail_output);
is($Installcheck::Run::stdout, "", "..produces no stdout output")
  or diag($Installcheck::Run::stdout);
results_match(
    $printer_output,
    $datas{'normal-postscript'},
    "..printer output matches"
);

## test columnspec adjustments, etc.

cleanup();
setup_config();
burp($current_log_filename, $datas{'normal'});

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'columnspec=OrigKB=::2'),
    "amreport with OrigKB=::2");
results_match($out_filename, $datas{'normal-rpt2'},
    "..result matches");

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'columnspec=OrigKB=:5'),
    "amreport with OrigKB=:5");
results_match($out_filename, $datas{'normal-rpt3'},
    "..result matches");

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'columnspec=OrigKB=:5:6'),
    "amreport with OrigKB=:5:6");
results_match($out_filename, $datas{'normal-rpt4'},
    "..result matches");
# TODO: do a lot more tests of the colspec stuff

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'displayunit=m'),
    "amreport with displayunit=m");
results_match($out_filename, $datas{'normal-rpt5'},
    "..result matches");

## some (anonymized) real logfiles, for regression testing

cleanup();
setup_config(want_template => 1);
burp($current_log_filename, $datas{'strontium'});

ok(run($amreport, 'TESTCONF', '-f', $out_filename),
    "amreport with strontium logfile (simple example with multiple levels)");
results_match($out_filename, $datas{'strontium-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'strontium-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'amflush'});

ok(run($amreport, 'TESTCONF', '-f', $out_filename),
    "amreport with amflush logfile (regression check for flush-related DUMP STATUS)");
results_match($out_filename, $datas{'amflush-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'amflush-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'resultsmissing'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 12,
    "amreport with resultsmissing logfile ('RESULTS MISSING') exit==12");
results_match($out_filename, $datas{'resultsmissing-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'resultsmissing-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'shortstrange'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 2,
    "amreport with shortstrange logfile exit==2");
results_match($out_filename, $datas{'shortstrange-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'shortstrange-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'longstrange'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 2,
    "amreport with longstrange logfile exit==2");
results_match($out_filename, $datas{'longstrange-rpt'},
    "..result matches");

cleanup();
burp($current_log_filename, $datas{'doublefailure'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with doublefailure logfile exit==4");
results_match($out_filename, $datas{'doublefailure-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'doublefailure-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'bigestimate'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with bigestimate logfile exit==0");
results_match($out_filename, $datas{'bigestimate-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'bigestimate-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'retried'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with retried logfile exit==4");
results_match($out_filename, $datas{'retried-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'retried-postscript'},
    "..printer output matches");

cleanup();
# if the 'FINISH DRIVER' line is missing, then the dumps did not finish
# properly, and the runtime is 0:00. We'll simulate that by "adjusting"
# the retried logfile.  First, lose the final line
$datas{'retried'} =~ s/^FINISH driver.*\n//m;
# and correspondingly add "FAILED" message to report
$datas{'retried-rpt'} = "*** THE DUMPS DID NOT FINISH PROPERLY!\n"
			. $datas{'retried-rpt'};
# and adjust the runtime
$datas{'retried-rpt'} =~ s{(Run Time .*)21:32}{$1 0:00};
burp($current_log_filename, $datas{'retried'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with retried logfile where driver did not finish exit==4");
results_match($out_filename, $datas{'retried-rpt'},
    "..result matches");

cleanup();
burp($current_log_filename, $datas{'taperr'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 16,
    "amreport with taperr logfile exit==16");
results_match($out_filename, $datas{'taperr-rpt-holding'},
    "..result matches");
ok((-f $printer_output and -z $printer_output),
    "..printer output exists but is empty");
cleanup();
burp($old_log_filename, $datas{'taperr'});

# use an explicit -l here so amreport doesn't try to look at the holding disk
run($amreport, 'TESTCONF', '-f', $out_filename, '-l', $old_log_filename);
is($Installcheck::Run::exit_code, 16,
    "amreport with taperr logfile specified explicitly exit==16");
results_match($out_filename, $datas{'taperr-rpt-noholding'},
    "..result matches");

cleanup();
burp($current_log_filename, $datas{'spanned'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with spanned logfile");
results_match($out_filename, $datas{'spanned-rpt'},
    "..result matches");
results_match($printer_output,
    $datas{'spanned-postscript'},
    "..printer output matches");

cleanup();
burp($current_log_filename, $datas{'fatal'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 5,
    "amreport with fatal logfile");
results_match($out_filename, $datas{'fatal-rpt'},
    "..result matches");
ok(-f $printer_output && -z $printer_output,
    "..printer output is empty (no dumps, no tapes)");

cleanup();
burp($current_log_filename, $datas{'flush-origsize'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with flush-origsize");
results_match($out_filename, $datas{'flush-origsize-rpt'},
    "..result matches flush-origsize-rpt");

cleanup();
burp($current_log_filename, $datas{'flush-noorigsize'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with flush-noorigsize");
results_match($out_filename, $datas{'flush-noorigsize-rpt'},
    "..result matches flush-origsize-rpt");

cleanup();
burp($current_log_filename, $datas{'plannerfail'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with a planner failure (failed)");
results_match($out_filename, $datas{'plannerfail-rpt'},
    "..result matches plannerfail-rpt");

cleanup();

burp($current_log_filename, $datas{'skipped'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with a planner skipped dump (succes)");
results_match($out_filename, $datas{'skipped-rpt'},
    "..result matches skipped-rpt");

cleanup();

+burp($current_log_filename, $datas{'filesystemstaped'});

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport correctly report filesystem taped (success)");
results_match($out_filename, $datas{'filesystemstaped-rpt'},
    "..result matches filesystemstaped-rpt");

cleanup();

__DATA__
%%%% normal
INFO amdump amdump pid 23649
INFO planner planner pid 23682
DISK planner localhost.localdomain /boot1
DISK planner localhost.localdomain /boot2
DISK planner localhost.localdomain /boot3
DISK planner localhost.localdomain /boot4
DISK planner localhost.localdomain /boot5
DISK planner localhost.localdomain /boot6
DISK planner localhost.localdomain /boot7
DISK planner localhost.localdomain /boot8
DISK planner localhost.localdomain /boot9
START planner date 20090225080737
INFO driver driver pid 23684
START driver date 20090225080737
STATS driver hostname localhost.localdomain
STATS driver startup time 0.004
INFO dumper dumper pid 23686
INFO taper taper pid 23685
FINISH planner date 20090225080737 time 0.084
INFO planner pid-done 23682
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot1 20090225080737 0 [sec 1.585 kb 12 kps 24748.4 orig-kb 16]
STATS driver estimate localhost.localdomain /boot1 20090225080737 0 [sec 1 nkb 12 ckb 12 kps 25715]
SUCCESS chunker localhost.localdomain /boot1 20090225080737 0 [sec 1.607 kb 12 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
START taper datestamp 20090225080737 label DIRO-TEST-003 tape 1
PART taper DIRO-TEST-003 1 localhost.localdomain /boot1 20090225080737 1/1 0 [sec 0.250557 kb 12 kps 156611.070535]
DONE taper localhost.localdomain /boot1 20090225080737 1 0 [sec 0.250557 kb 12 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot2 20090225080737 0 [sec 1.585 kb 123 kps 24748.4 orig-kb 167]
STATS driver estimate localhost.localdomain /boot2 20090225080737 0 [sec 1 nkb 123 ckb 123 kps 25715]
SUCCESS chunker localhost.localdomain /boot2 20090225080737 0 [sec 1.607 kb 123 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 2 localhost.localdomain /boot2 20090225080737 1/1 0 [sec 0.250557 kb 123 kps 156611.070535]
DONE taper localhost.localdomain /boot2 20090225080737 1 0 [sec 0.250557 kb 123 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot3 20090225080737 0 [sec 1.585 kb 1234 kps 24748.4 orig-kb 1678]
STATS driver estimate localhost.localdomain /boot3 20090225080737 0 [sec 1 nkb 1234 ckb 1234 kps 25715]
SUCCESS chunker localhost.localdomain /boot3 20090225080737 0 [sec 1.607 kb 1234 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 3 localhost.localdomain /boot3 20090225080737 1/1 0 [sec 0.250557 kb 1234 kps 156611.070535]
DONE taper localhost.localdomain /boot3 20090225080737 1 0 [sec 0.250557 kb 1234 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot4 20090225080737 0 [sec 1.585 kb 12345 kps 24748.4 orig-kb 16789]
STATS driver estimate localhost.localdomain /boot4 20090225080737 0 [sec 1 nkb 12345 ckb 12345 kps 25715]
SUCCESS chunker localhost.localdomain /boot4 20090225080737 0 [sec 1.607 kb 12345 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 4 localhost.localdomain /boot4 20090225080737 1/1 0 [sec 0.250557 kb 12345 kps 156611.070535]
DONE taper localhost.localdomain /boot4 20090225080737 1 0 [sec 0.250557 kb 12345 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot5 20090225080737 0 [sec 1.585 kb 123456 kps 24748.4 orig-kb 167890]
STATS driver estimate localhost.localdomain /boot5 20090225080737 0 [sec 1 nkb 123456 ckb 123456 kps 25715]
SUCCESS chunker localhost.localdomain /boot5 20090225080737 0 [sec 1.607 kb 123456 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 5 localhost.localdomain /boot5 20090225080737 1/1 0 [sec 0.250557 kb 123456 kps 156611.070535]
DONE taper localhost.localdomain /boot5 20090225080737 1 0 [sec 0.250557 kb 123456 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot6 20090225080737 0 [sec 1.585 kb 1234567 kps 24748.4 orig-kb 1678901]
STATS driver estimate localhost.localdomain /boot6 20090225080737 0 [sec 1 nkb 1234567 ckb 1234567 kps 25715]
SUCCESS chunker localhost.localdomain /boot6 20090225080737 0 [sec 1.607 kb 1234567 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 6 localhost.localdomain /boot6 20090225080737 1/1 0 [sec 0.250557 kb 1234567 kps 156611.070535]
DONE taper localhost.localdomain /boot6 20090225080737 1 0 [sec 0.250557 kb 1234567 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot7 20090225080737 0 [sec 1.585 kb 12345678 kps 24748.4 orig-kb 16789012]
STATS driver estimate localhost.localdomain /boot7 20090225080737 0 [sec 1 nkb 12345678 ckb 12345678 kps 25715]
SUCCESS chunker localhost.localdomain /boot7 20090225080737 0 [sec 1.607 kb 12345678 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 7 localhost.localdomain /boot7 20090225080737 1/1 0 [sec 0.250557 kb 12345678 kps 156611.070535]
DONE taper localhost.localdomain /boot7 20090225080737 1 0 [sec 0.250557 kb 12345678 kps 156611.070535]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot8 20090225080737 0 [sec 1.585 kb 123456789 kps 24748.4 orig-kb 167890123]
STATS driver estimate localhost.localdomain /boot8 20090225080737 0 [sec 1 nkb 123456789 ckb 123456789 kps 25715]
SUCCESS chunker localhost.localdomain /boot8 20090225080737 0 [sec 1.607 kb 123456789 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 8 localhost.localdomain /boot8 20090225080737 1/1 0 [sec 0.250557 kb 123456789 kps 156611.070535 orig-kb 0]
DONE taper localhost.localdomain /boot8 20090225080737 1 0 [sec 0.250557 kb 123456789 kps 156611.070535 orig-kb 0]
INFO chunker chunker pid 23733
INFO dumper gzip pid 23738
SUCCESS dumper localhost.localdomain /boot9 20090225080737 0 [sec 1.585 kb 1234567890 kps 24748.4 orig-kb 1678901234]
STATS driver estimate localhost.localdomain /boot9 20090225080737 0 [sec 1 nkb 1234567890 ckb 1234567890 kps 25715]
SUCCESS chunker localhost.localdomain /boot9 20090225080737 0 [sec 1.607 kb 1234567890 kps 24426.5]
INFO chunker pid-done 23733
INFO dumper pid-done 23738
PART taper DIRO-TEST-003 9 localhost.localdomain /boot9 20090225080737 1/1 0 [sec 0.250557 kb 1234567890 kps 156611.070535 orig-kb -1]
DONE taper localhost.localdomain /boot9 20090225080737 1 0 [sec 0.250557 kb 1234567890 kps 156611.070535 orig-kb -1]
INFO dumper pid-done 23686
INFO taper tape DIRO-TEST-003 kb 39240 fm 10 [OK]
INFO taper pid-done 23685
FINISH driver date 20090225080737 time 5.306
INFO driver pid-done 23684
%%%% normal-postscript
--PS-TEMPLATE--
(February 25, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(DIRO-TEST-003) DrawTitle
(Total Size:        1339591.9 MB) DrawStat
(Tape Used (%)       ##### %) DrawStat
(Number of files:      9) DrawStat
(Filesystems Taped:    9) DrawStat
(-) (DIRO-TEST-003) (-) (  0) (      32) (      32) DrawHost
(localhost.localdomain) (/boot1) (0) (  1) (      16) (      12) DrawHost
(localhost.localdomain) (/boot2) (0) (  2) (     167) (     123) DrawHost
(localhost.localdomain) (/boot3) (0) (  3) (    1678) (    1234) DrawHost
(localhost.localdomain) (/boot4) (0) (  4) (   16789) (   12345) DrawHost
(localhost.localdomain) (/boot5) (0) (  5) (  167890) (  123456) DrawHost
(localhost.localdomain) (/boot6) (0) (  6) ( 1678901) ( 1234567) DrawHost
(localhost.localdomain) (/boot7) (0) (  7) (16789012) (12345678) DrawHost
(localhost.localdomain) (/boot8) (0) (  8) (167890123) (123456789) DrawHost
(localhost.localdomain) (/boot9) (0) (  9) (1678901234) (1234567890) DrawHost

showpage
%%%% normal-rpt1
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        --
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Parts Taped                   9          9          0
Avg Tp Write Rate (k/s) #######    #######        --

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                          DUMPER STATS                    TAPER STATS
HOSTNAME     DISK        L    ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- -------------------------------------------- ---------------
localhost.lo /boot1      0         16         12   75.0    0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0        167        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0       1678       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0      16789      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0     167890     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0    1678901    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0   16789012   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0  167890123  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% normal-rpt2
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        --
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Parts Taped                   9          9          0
Avg Tp Write Rate (k/s) #######    #######        --

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                            DUMPER STATS                     TAPER STATS
HOSTNAME     DISK        L       ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- ----------------------------------------------- ---------------
localhost.lo /boot1      0         16.00         12   75.0    0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0        167.00        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0       1678.00       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0      16789.00      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0     167890.00     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0    1678901.00    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0   16789012.00   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0  167890123.00  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234.00 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% normal-rpt3
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        --
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Parts Taped                   9          9          0
Avg Tp Write Rate (k/s) #######    #######        --

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                        DUMPER STATS                 TAPER STATS
HOSTNAME     DISK        L ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- --------------------------------------- ---------------
localhost.lo /boot1      0    16         12   75.0    0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0   167        123   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0  1678       1234   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0 16789      12345   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0 167890     123456   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0 1678901    1234567   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0 16789012   12345678   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0 167890123  123456789   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1678901234 1234567890   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% normal-rpt4
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        --
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Parts Taped                   9          9          0
Avg Tp Write Rate (k/s) #######    #######        --

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00 1371742094k  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                        DUMPER STATS                 TAPER STATS
HOSTNAME     DISK        L ORIG-kB     OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- --------------------------------------- ---------------
localhost.lo /boot1 0 16.000000 12 75.0 0:02 24748.4 0:00 156611.1
localhost.lo /boot2 0 167.000000 123 73.7 0:02 24748.4 0:00 156611.1
localhost.lo /boot3 0 1678.000000 1234 73.5 0:02 24748.4 0:00 156611.1
localhost.lo /boot4 0 16789.000000 12345 73.5 0:02 24748.4 0:00 156611.1
localhost.lo /boot5 0 167890.000000 123456 73.5 0:02 24748.4 0:00 156611.1
localhost.lo /boot6 0 1678901.000000 1234567 73.5 0:02 24748.4 0:00 156611.1
localhost.lo /boot7 0 16789012.000000 12345678 73.5 0:02 24748.4 0:00 156611.1
localhost.lo /boot8 0 167890123.000000 123456789 73.5 0:02 24748.4 0:00 156611.1
localhost.lo /boot9 0 1678901234.000000 1234567890 73.5 0:02 24748.4 0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% normal-rpt5
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : February 25, 2009

These dumps were to tape DIRO-TEST-003.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)      1339591.9   1339591.9        0.0
Original Size (meg)    1821724.4   1821724.4        0.0
Avg Compressed Size (%)    73.5       73.5        --
Filesystems Dumped            9          9          0
Avg Dump Rate (k/s)     #######    #######        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)        1339591.9   1339591.9        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             9          9          0

Parts Taped                   9          9          0
Avg Tp Write Rate (k/s) #######    #######        --

USAGE BY TAPE:
  Label               Time      Size      %    Nb    Nc
  DIRO-TEST-003       0:00  1339592M  #####     9     9


NOTES:
  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]


DUMP SUMMARY:
                                       DUMPER STATS                 TAPER STATS
HOSTNAME     DISK        L ORIG-MB  OUT-MB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- -------------------------------------- ---------------
localhost.lo /boot1      0       0       0   75.0    0:02 24748.4   0:00 156611.1
localhost.lo /boot2      0       0       0   73.7    0:02 24748.4   0:00 156611.1
localhost.lo /boot3      0       2       1   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot4      0      16      12   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot5      0     164     121   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot6      0    1640    1206   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot7      0   16396   12056   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot8      0  163955  120563   73.5    0:02 24748.4   0:00 156611.1
localhost.lo /boot9      0 1639552 1205633   73.5    0:02 24748.4   0:00 156611.1

(brought to you by Amanda version x.y.z)
%%%% strontium
INFO amdump amdump pid 12920
INFO planner planner pid 12944
START planner date 20100107111335
DISK planner strontium /etc
DISK planner strontium /local
DISK planner strontium /home/elantra
DISK planner strontium /zones/data/strontium.example.com/repositories/repository_13
INFO driver driver pid 12945
START driver date 20100107111335
STATS driver hostname advantium
STATS driver startup time 0.016
INFO dumper dumper pid 12947
INFO dumper dumper pid 12948
INFO dumper dumper pid 12949
INFO dumper dumper pid 12950
INFO taper taper pid 12946
FINISH planner date 20100107111335 time 2.344
INFO planner pid-done 12944
INFO chunker chunker pid 13015
INFO dumper gzip pid 13016
SUCCESS dumper strontium /home/elantra 20100107111335 1 [sec 0.152 kb 10 kps 65.6 orig-kb 10]
SUCCESS chunker strontium /home/elantra 20100107111335 1 [sec 0.184 kb 10 kps 228.0]
INFO chunker pid-done 13015
INFO dumper pid-done 13016
STATS driver estimate strontium /home/elantra 20100107111335 1 [sec 0 nkb 49 ckb 64 kps 1024]
START taper datestamp 20100107111335 label metals-013 tape 1
PART taper metals-013 1 strontium /home/elantra 20100107111335 1/1 1 [sec 0.001107 kb 10 kps 9033.423668]
DONE taper strontium /home/elantra 20100107111335 1 1 [sec 0.001107 kb 10 kps 9033.423668]
INFO chunker chunker pid 13017
INFO dumper gzip pid 13018
SUCCESS dumper strontium /local 20100107111335 0 [sec 0.149 kb 20 kps 133.9 orig-kb 20]
SUCCESS chunker strontium /local 20100107111335 0 [sec 0.183 kb 20 kps 283.3]
INFO chunker pid-done 13017
INFO dumper pid-done 13018
STATS driver estimate strontium /local 20100107111335 0 [sec 0 nkb 46 ckb 64 kps 1024]
PART taper metals-013 2 strontium /local 20100107111335 1/1 0 [sec 0.000724 kb 20 kps 27624.309392]
DONE taper strontium /local 20100107111335 1 0 [sec 0.000724 kb 20 kps 27624.309392]
INFO chunker chunker pid 13026
INFO dumper gzip pid 13027
SUCCESS dumper strontium /etc 20100107111335 1 [sec 0.235 kb 270 kps 1146.3 orig-kb 270]
SUCCESS chunker strontium /etc 20100107111335 1 [sec 0.271 kb 270 kps 1110.9]
INFO chunker pid-done 13026
INFO dumper pid-done 13027
STATS driver estimate strontium /etc 20100107111335 1 [sec 0 nkb 516 ckb 544 kps 1024]
PART taper metals-013 3 strontium /etc 20100107111335 1/1 1 [sec 0.001916 kb 270 kps 140918.580376]
DONE taper strontium /etc 20100107111335 1 1 [sec 0.001916 kb 270 kps 140918.580376]
INFO chunker chunker pid 13034
INFO dumper gzip pid 13035
SUCCESS dumper strontium /zones/data/strontium.example.com/repositories/repository_13 20100107111335 1 [sec 0.525 kb 1350 kps 2568.5 orig-kb 1350]
SUCCESS chunker strontium /zones/data/strontium.example.com/repositories/repository_13 20100107111335 1 [sec 0.561 kb 1350 kps 2461.3]
INFO chunker pid-done 13034
INFO dumper pid-done 13035
STATS driver estimate strontium /zones/data/strontium.example.com/repositories/repository_13 20100107111335 1 [sec 0 nkb 1344 ckb 1344 kps 1350]
PART taper metals-013 4 strontium /zones/data/strontium.example.com/repositories/repository_13 20100107111335 1/1 1 [sec 0.007714 kb 1350 kps 175006.481722]
DONE taper strontium /zones/data/strontium.example.com/repositories/repository_13 20100107111335 1 1 [sec 0.007714 kb 1350 kps 175006.481722]
INFO dumper pid-done 12947
INFO dumper pid-done 12948
INFO dumper pid-done 12949
INFO dumper pid-done 12950
INFO taper tape metals-013 kb 1650 fm 4 [OK]
INFO taper pid-done 12946
FINISH driver date 20100107111335 time 49.037
INFO driver pid-done 12945
%%%% strontium-rpt
Hostname: advantium
Org     : DailySet1
Config  : TESTCONF
Date    : January 7, 2010

These dumps were to tape metals-013.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:01
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)           1.6        0.0        1.6
Original Size (meg)         1.6        0.0        1.6
Avg Compressed Size (%)   100.0      100.0      100.0   (level:#disks ...)
Filesystems Dumped            4          1          3   (1:3)
Avg Dump Rate (k/s)      1555.1      134.2     1787.3

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)             1.6        0.0        1.6
Tape Used (%)               5.5        0.1        5.4   (level:#disks ...)
Filesystems Taped             4          1          3   (1:3)
   (level:#parts ...)
Parts Taped                   4          1          3   (1:3)
Avg Tp Write Rate (k/s)  143966    27624.3     151811

USAGE BY TAPE:
  Label            Time      Size      %    Nb    Nc
  metals-013       0:00     1650k    5.4     4     4


NOTES:
  taper: tape metals-013 kb 1650 fm 4 [OK]


DUMP SUMMARY:
                                       DUMPER STATS                TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS     KB/s
-------------------------- ------------------------------------- ---------------
strontium    /etc        1     270     270    --     0:00 1146.3   0:00 140918.6
strontium    -me/elantra 1      10      10    --     0:00   65.6   0:00   9033.4
strontium    /local      0      20      20    --     0:00  133.9   0:00  27624.3
strontium    -ository_13 1    1350    1350    --     0:01 2568.5   0:00 175006.5

(brought to you by Amanda version x.y.z)
%%%% strontium-postscript
--PS-TEMPLATE--
(January 7, 2010) DrawDate

(Amanda Version x.y.z) DrawVers
(metals-013) DrawTitle
(Total Size:           1.6 MB) DrawStat
(Tape Used (%)         5.4 %) DrawStat
(Number of files:      4) DrawStat
(Filesystems Taped:    4) DrawStat
(-) (metals-013) (-) (  0) (      32) (      32) DrawHost
(strontium) (/home/elantra) (1) (  1) (      10) (      10) DrawHost
(strontium) (/local) (0) (  2) (      20) (      20) DrawHost
(strontium) (/etc) (1) (  3) (     270) (     270) DrawHost
(strontium) (/zones/data/strontium.example.com/repositories/repository_13) (1) (  4) (    1350) (    1350) DrawHost

showpage
%%%% amflush
INFO amflush amflush pid 26036
DISK amflush localhost /usr/local
DISK amflush localhost /opt
DISK amflush localhost /usr/lib
DISK amflush localhost /var/mysql
DISK amflush localhost /home
START amflush date 20090622075550
INFO driver driver pid 26076
START driver date 20090622075550
STATS driver hostname centralcity.zmanda.com
STATS driver startup time 0.011
INFO taper taper pid 26077
START taper datestamp 20090622075550 label Flushy-017 tape 1
PART taper Flushy-017 1 localhost /var/mysql 20090620020002 1/1 1 [sec 2.504314 kb 36980 kps 14766.518895]
DONE taper localhost /var/mysql 20090620020002 1 1 [sec 2.504314 kb 36980 kps 14766.518895]
PART taper Flushy-017 2 localhost /usr/lib 20090620020002 1/1 1 [sec 1.675693 kb 309 kps 184.632684]
DONE taper localhost /usr/lib 20090620020002 1 1 [sec 1.675693 kb 309 kps 184.632684]
INFO taper pid-done 26077
FINISH driver date 20090622075550 time 177.708
INFO driver pid-done 26076
INFO amflush pid-done 26075
%%%% amflush-rpt
Hostname: centralcity.zmanda.com
Org     : DailySet1
Config  : TESTCONF
Date    : June 22, 2009

The dumps were flushed to tape Flushy-017.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:03
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)           0.0        0.0        0.0
Original Size (meg)         0.0        0.0        0.0
Avg Compressed Size (%)     --         --         --
Filesystems Dumped            0          0          0
Avg Dump Rate (k/s)         --         --         --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)            36.4        0.0       36.4
Tape Used (%)             121.4        0.0      121.4   (level:#disks ...)
Filesystems Taped             2          0          2   (1:2)
   (level:#parts ...)
Parts Taped                   2          0          2   (1:2)
Avg Tp Write Rate (k/s)  8920.8        --      8920.8

USAGE BY TAPE:
  Label            Time      Size      %    Nb    Nc
  Flushy-017       0:00    37289k  121.4     2     2


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
-------------------------- ------------------------------------- --------------
localhost    /home         NO FILE TO FLUSH -----------------------------------
localhost    /opt          NO FILE TO FLUSH -----------------------------------
localhost    /usr/lib    1             309    --       FLUSH       0:02   184.6
localhost    /usr/local    NO FILE TO FLUSH -----------------------------------
localhost    /var/mysql  1           36980    --       FLUSH       0:03 14766.5

(brought to you by Amanda version x.y.z)
%%%% amflush-postscript
--PS-TEMPLATE--
(June 22, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(Flushy-017) DrawTitle
(Total Size:          36.4 MB) DrawStat
(Tape Used (%)       121.4 %) DrawStat
(Number of files:      2) DrawStat
(Filesystems Taped:    2) DrawStat
(-) (Flushy-017) (-) (  0) (      32) (      32) DrawHost
(localhost) (/var/mysql) (1) (  1) (        ) (   36980) DrawHost
(localhost) (/usr/lib) (1) (  2) (        ) (     309) DrawHost

showpage
%%%% resultsmissing
START planner date 20090326001503
START driver date 20090326001503
INFO amdump amdump pid 22014
INFO driver driver pid 22043
INFO planner planner pid 22042
DISK planner cnc.slikon.local /boot
DISK planner cnc.slikon.local /
INFO taper taper pid 22044
DISK planner ns-new.slikon.local //usr/local
DISK planner ns-new.slikon.local /home
DISK planner ns-new.slikon.local /boot
FAIL planner ns-new.slikon.local /boot 20090326001503 0 "[planner failed]"
SUCCESS dumper ns-new.slikon.local //usr/local 20090326001503 0 [sec 0.040 kb 1 kps 24.6 orig-kb 30]
START taper datestamp 20090326001503 label Daily-36 tape 1
SUCCESS dumper cnc.slikon.local /boot 20090326001503 0 [sec 4.255 kb 17246 kps 4052.7 orig-kb 20670]
INFO dumper pid-done 7337
SUCCESS chunker ns-new.slikon.local //usr/local 20090326001503 0 [sec 1.109 kb 1 kps 29.8]
SUCCESS chunker cnc.slikon.local /boot 20090326001503 0 [sec 10.093 kb 17246 kps 1711.9]
STATS driver estimate ns-new.slikon.local //usr/local 20090326001503 0 [sec 1 nkb 62 ckb 64 kps 1]
INFO chunker pid-done 7334
STATS driver estimate cnc.slikon.local /boot 20090326001503 0 [sec 2 nkb 20702 ckb 17280 kps 5749]
PART taper Daily-36 1 cnc.slikon.local /boot 20090326001503 1/1 0 [sec 0.742831 kb 17245 kps 23216.462699]
DONE taper cnc.slikon.local /boot 20090326001503 1 0 [sec 0.742831 kb 17245 kps 23216.462699]
PART taper Daily-36 2 ns-new.slikon.local //usr/local 20090326001503 1/1 0 [sec 0.004696 kb 1 kps 153.471705]
DONE taper ns-new.slikon.local //usr/local 20090326001503 1 0 [sec 0.004696 kb 1 kps 153.471705]
INFO taper pid-done 22044
FINISH driver date 20090326001503 time 77506.015
INFO driver pid-done 22043
%%%% resultsmissing-rpt
These dumps were to tape Daily-36.
The next tape Amanda expects to use is: 1 new tape.

FAILURE DUMP SUMMARY:
   cnc.slikon.local    /      RESULTS MISSING
   ns-new.slikon.local /home  RESULTS MISSING
   ns-new.slikon.local /boot  lev 0 FAILED [planner failed]


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)          16.8       16.8        0.0
Original Size (meg)        20.2       20.2        0.0
Avg Compressed Size (%)    83.3       83.3        --
Filesystems Dumped            2          2          0
Avg Dump Rate (k/s)      4015.6     4015.6        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)            16.8       16.8        0.0
Tape Used (%)              56.2       56.2        0.0
Filesystems Taped             2          2          0

Parts Taped                   2          2          0
Avg Tp Write Rate (k/s) 23070.7    23070.7        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Daily-36       0:00    17246k   56.2     2     2


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
-------------------------- ------------------------------------- --------------
cnc.slikon.l /             MISSING --------------------------------------------
cnc.slikon.l /boot       0   20670   17245   83.4    0:04 4052.7   0:01 23216.5
ns-new.sliko //usr/local 0      30       1    3.3    0:00   24.6   0:00   153.5
ns-new.sliko /boot         FAILED
ns-new.sliko /home         MISSING --------------------------------------------

(brought to you by Amanda version x.y.z)
%%%% resultsmissing-postscript
--PS-TEMPLATE--
(March 26, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(Daily-36) DrawTitle
(Total Size:          16.8 MB) DrawStat
(Tape Used (%)        56.2 %) DrawStat
(Number of files:      2) DrawStat
(Filesystems Taped:    2) DrawStat
(-) (Daily-36) (-) (  0) (      32) (      32) DrawHost
(cnc.slikon.local) (/boot) (0) (  1) (   20670) (   17245) DrawHost
(ns-new.slikon.local) (//usr/local) (0) (  2) (      30) (       1) DrawHost

showpage
%%%% shortstrange
START planner date 20090326001503
START driver date 20090326001503
INFO amdump amdump pid 22014
INFO driver driver pid 22043
INFO planner planner pid 22042
DISK planner bsdfw.slikon.local /
START taper datestamp 20090326001503 label Daily-36 tape 1
STRANGE dumper bsdfw.slikon.local / 0 [sec 1775.514 kb 2317814 kps 1305.4 orig-kb 5401240]
  sendbackup: info BACKUP=APPLICATION
  sendbackup: info APPLICATION=amgtar
  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
  sendbackup: info COMPRESS_SUFFIX=.gz
  sendbackup: info end
  | /bin/tar: ./tmp/.gdm_socket: socket ignored
  | /bin/tar: ./tmp/.X11-unix/X0: socket ignored
  | /bin/tar: ./tmp/.font-unix/fs7100: socket ignored
  ? /bin/tar: ./var/log/messages: file changed as we read it
  | /bin/tar: ./var/run/acpid.socket: socket ignored
  | /bin/tar: ./var/run/dbus/system_bus_socket: socket ignored
  | Total bytes written: 5530869760 (5.2GiB, 3.0MiB/s)
  sendbackup: size 5401240
  sendbackup: end
SUCCESS chunker bsdfw.slikon.local / 20090326001503 0 [sec 1782.341 kb 2293471 kps 1286.8]
STATS driver estimate bsdfw.slikon.local / 20090326001503 0 [sec 1715 nkb 5400282 ckb 2294272 kps 1337]
PART taper Daily-36 1 bsdfw.slikon.local / 20090326001503 1/1 0 [sec 157.123731 kb 2293470 kps 14596.586283]
DONE taper bsdfw.slikon.local / 20090326001503 1 0 [sec 157.123731 kb 2293470 kps 14596.586283]
FINISH driver date 20090326001503 time 77506.015
%%%% shortstrange-rpt
These dumps were to tape Daily-36.
The next tape Amanda expects to use is: 1 new tape.

STRANGE DUMP SUMMARY:
   bsdfw.slikon.local / lev 0  STRANGE (see below)


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:30       0:30       0:00
Output Size (meg)        2239.7     2239.7        0.0
Original Size (meg)      5274.6     5274.6        0.0
Avg Compressed Size (%)    42.5       42.5        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)      1291.7     1291.7        --

Tape Time (hrs:min)        0:03       0:03       0:00
Tape Size (meg)          2239.7     2239.7        0.0
Tape Used (%)              7466       7466        0.0
Filesystems Taped             1          1          0

Parts Taped                   1          1          0
Avg Tp Write Rate (k/s) 14596.6    14596.6        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Daily-36       0:03  2293470k   7466     1     1


STRANGE DUMP DETAILS:

/--  bsdfw.slikon.local / lev 0 STRANGE
sendbackup: info BACKUP=APPLICATION
sendbackup: info APPLICATION=amgtar
sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
sendbackup: info COMPRESS_SUFFIX=.gz
sendbackup: info end
| /bin/tar: ./tmp/.gdm_socket: socket ignored
| /bin/tar: ./tmp/.X11-unix/X0: socket ignored
| /bin/tar: ./tmp/.font-unix/fs7100: socket ignored
? /bin/tar: ./var/log/messages: file changed as we read it
| /bin/tar: ./var/run/acpid.socket: socket ignored
| /bin/tar: ./var/run/dbus/system_bus_socket: socket ignored
| Total bytes written: 5530869760 (5.2GiB, 3.0MiB/s)
sendbackup: size 5401240
sendbackup: end
\--------


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
-------------------------- ------------------------------------- --------------
bsdfw.slikon /           0 5401240 2293470   42.5   29:36 1305.4   2:37 14596.6

(brought to you by Amanda version x.y.z)
%%%% shortstrange-postscript
--PS-TEMPLATE--
(March 26, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(Daily-36) DrawTitle
(Total Size:        2239.7 MB) DrawStat
(Tape Used (%)        7466 %) DrawStat
(Number of files:      1) DrawStat
(Filesystems Taped:    1) DrawStat
(-) (Daily-36) (-) (  0) (      32) (      32) DrawHost
(bsdfw.slikon.local) (/) (0) (  1) ( 5401240) ( 2293470) DrawHost

showpage
%%%% longstrange-rpt
These dumps were to tape Daily-36.
The next tape Amanda expects to use is: 1 new tape.

STRANGE DUMP SUMMARY:
   bsdfw.slikon.local / lev 0  STRANGE (see below)


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:30       0:30       0:00
Output Size (meg)        2239.7     2239.7        0.0
Original Size (meg)      5274.6     5274.6        0.0
Avg Compressed Size (%)    42.5       42.5        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)      1291.7     1291.7        --

Tape Time (hrs:min)        0:03       0:03       0:00
Tape Size (meg)          2239.7     2239.7        0.0
Tape Used (%)              7466       7466        0.0
Filesystems Taped             1          1          0

Parts Taped                   1          1          0
Avg Tp Write Rate (k/s) 14596.6    14596.6        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Daily-36       0:03  2293470k   7466     1     1


STRANGE DUMP DETAILS:

/--  bsdfw.slikon.local / lev 0 STRANGE
sendbackup: info BACKUP=APPLICATION
sendbackup: info APPLICATION=amgtar
sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
sendbackup: info COMPRESS_SUFFIX=.gz
sendbackup: info end
| /bin/tar: ./tmp/.gdm_socket: socket ignored
| /bin/tar: ./tmp/.X11-unix/X0: socket ignored
| /bin/tar: ./tmp/.font-unix/fs7100: socket ignored
? /bin/tar: ./var/log/messages: file changed as we read it
--LINE-REPLACED-BY-91-LINES--
\--------
913 lines follow, see the corresponding log.* file for the complete list
\--------


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
-------------------------- ------------------------------------- --------------
bsdfw.slikon /           0 5401240 2293470   42.5   29:36 1305.4   2:37 14596.6

(brought to you by Amanda version x.y.z)
%%%% doublefailure
START planner date 20090326001503
START driver date 20090326001503
INFO amdump amdump pid 22014
INFO driver driver pid 22043
INFO planner planner pid 22042
START taper datestamp 20090326001503 label Daily-13 tape 1
DISK planner ns-new.slikon.local /opt/var
INFO planner Forcing full dump of ns-new.slikon.local:/opt/var as directed.
FAIL dumper ns-new.slikon.local /opt/var 20090326001503 0 [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]
  sendbackup: info BACKUP=APPLICATION
  sendbackup: info APPLICATION=amgtar
  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
  sendbackup: info COMPRESS_SUFFIX=.gz
  sendbackup: info end
  ? /bin/tar: ./gdm: Cannot savedir: Permission denied
  | Total bytes written: 943831040 (901MiB, 4.9MiB/s)
  | /bin/tar: Error exit delayed from previous errors
  sendbackup: error [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]
  sendbackup: size 921710
  sendbackup: end
PARTIAL chunker ns-new.slikon.local /opt/var 20090326001503 0 [sec 187.313 kb 54930 kps 293.4]
FAIL dumper ns-new.slikon.local /opt/var 20090326001503 0 [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]
  sendbackup: info BACKUP=APPLICATION
  sendbackup: info APPLICATION=amgtar
  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
  sendbackup: info COMPRESS_SUFFIX=.gz
  sendbackup: info end
  ? /bin/tar: ./gdm: Cannot savedir: Permission denied
  | Total bytes written: 943851520 (901MiB, 7.4MiB/s)
  | /bin/tar: Error exit delayed from previous errors
  sendbackup: error [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]
  sendbackup: size 921730
  sendbackup: end
PARTIAL chunker ns-new.slikon.local /opt/var 20090326001503 0 [sec 123.421 kb 54930 kps 445.3]
PART taper Daily-13 1 ns-new.slikon.local /opt/var 20090326001503 1/1 0 [sec 3.555085 kb 54929 kps 15451.027696]
PARTIAL taper ns-new.slikon.local /opt/var 20090326001503 1 0 [sec 3.555085 kb 54929 kps 15451.027696]
FINISH driver date 20090326001503 time 77506.015
%%%% doublefailure-rpt
These dumps were to tape Daily-13.
The next tape Amanda expects to use is: 1 new tape.

FAILURE DUMP SUMMARY:
   ns-new.slikon.local /opt/var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]
   ns-new.slikon.local /opt/var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]
   ns-new.slikon.local /opt/var lev 0  partial taper: successfully taped a partial dump


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)         107.3      107.3        0.0
Original Size (meg)         0.0        0.0        0.0
Avg Compressed Size (%)     --         --         --
Filesystems Dumped            0          0          0
Avg Dump Rate (k/s)         --         --         --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)            53.6       53.6        0.0
Tape Used (%)             178.8      178.8        0.0
Filesystems Taped             1          1          0

Parts Taped                   1          1          0
Avg Tp Write Rate (k/s) 15450.8    15450.8        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Daily-13       0:00    54929k  178.8     1     1

FAILED DUMP DETAILS:

/--  ns-new.slikon.local /opt/var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]
sendbackup: info BACKUP=APPLICATION
sendbackup: info APPLICATION=amgtar
sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
sendbackup: info COMPRESS_SUFFIX=.gz
sendbackup: info end
? /bin/tar: ./gdm: Cannot savedir: Permission denied
| Total bytes written: 943831040 (901MiB, 4.9MiB/s)
| /bin/tar: Error exit delayed from previous errors
sendbackup: error [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]
sendbackup: size 921710
sendbackup: end
\--------

/--  ns-new.slikon.local /opt/var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]
sendbackup: info BACKUP=APPLICATION
sendbackup: info APPLICATION=amgtar
sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -
sendbackup: info COMPRESS_SUFFIX=.gz
sendbackup: info end
? /bin/tar: ./gdm: Cannot savedir: Permission denied
| Total bytes written: 943851520 (901MiB, 7.4MiB/s)
| /bin/tar: Error exit delayed from previous errors
sendbackup: error [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]
sendbackup: size 921730
sendbackup: end
\--------


NOTES:
  planner: Forcing full dump of ns-new.slikon.local:/opt/var as directed.


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS   KB/s
-------------------------- ------------------------------------- --------------
ns-new.sliko /opt/var    0           54929    --      PARTIAL      0:04 15451.0 PARTIAL

(brought to you by Amanda version x.y.z)
%%%% doublefailure-postscript
--PS-TEMPLATE--
(March 26, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(Daily-13) DrawTitle
(Total Size:          53.6 MB) DrawStat
(Tape Used (%)       178.8 %) DrawStat
(Number of files:      1) DrawStat
(Filesystems Taped:    1) DrawStat
(-) (Daily-13) (-) (  0) (      32) (      32) DrawHost
(ns-new.slikon.local) (/opt/var) (0) (  1) (        ) (   54929) DrawHost

showpage
%%%% bigestimate
START planner date 20090326001503
START driver date 20090326001503
INFO amdump amdump pid 22014
INFO driver driver pid 22043
INFO planner planner pid 22042
DISK planner home.slikon.local /opt/public
SUCCESS dumper home.slikon.local /opt/public 20090326001503 0 [sec 2816.520 kb 50917370 kps 18078.1 orig-kb 72987320]
START taper datestamp 20090326001503 label Daily-36 tape 1
SUCCESS chunker home.slikon.local /opt/public 20090326001503 0 [sec 2821.633 kb 50917370 kps 18045.9]
STATS driver estimate home.slikon.local /opt/public 20090326001503 0 [sec 0 nkb 72987352 ckb 80286112 kps 4294967295]
PART taper Daily-36 1 home.slikon.local /opt/public 20090326001503 1/3 0 [sec 813.482141 kb 22020096 kps 27068.935985]
PART taper Daily-36 2 home.slikon.local /opt/public 20090326001503 2/3 0 [sec 800.783991 kb 22020096 kps 27498.172101]
PART taper Daily-36 3 home.slikon.local /opt/public 20090326001503 3/3 0 [sec 251.674410 kb 6877177 kps 27325.692199]
DONE taper home.slikon.local /opt/public 20090326001503 3 0 [sec 1865.940542 kb 50917369 kps 27287.777030]
FINISH driver date 20090326001503 time 77506.015
%%%% bigestimate-rpt
These dumps were to tape Daily-36.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:47       0:47       0:00
Output Size (meg)       49724.0    49724.0        0.0
Original Size (meg)     71276.7    71276.7        0.0
Avg Compressed Size (%)    69.8       69.8        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)     18078.1    18078.1        --

Tape Time (hrs:min)        0:31       0:31       0:00
Tape Size (meg)         49724.0    49724.0        0.0
Tape Used (%)             #####      #####        0.0
Filesystems Taped             1          1          0

Parts Taped                   3          3          0
Avg Tp Write Rate (k/s) 27287.8    27287.8        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Daily-36       0:31 50917369k  #####     1     3


NOTES:
  big estimate: home.slikon.local /opt/public 0
                est: 80286112k    out 50917369k


DUMP SUMMARY:
                                        DUMPER STATS                 TAPER STATS
HOSTNAME     DISK        L  ORIG-kB   OUT-kB  COMP%  MMM:SS    KB/s MMM:SS    KB/s
-------------------------- ---------------------------------------- --------------
home.slikon. /opt/public 0 72987320 50917369   69.8   46:57 18078.1  31:06 27287.8

(brought to you by Amanda version x.y.z)
%%%% bigestimate-postscript
--PS-TEMPLATE--
(March 26, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(Daily-36) DrawTitle
(Total Size:        49724.0 MB) DrawStat
(Tape Used (%)       ##### %) DrawStat
(Number of files:      3) DrawStat
(Filesystems Taped:    1) DrawStat
(-) (Daily-36) (-) (  0) (      32) (      32) DrawHost
(home.slikon.local) (/opt/public) (0) (  1) (72987320) (50917369) DrawHost

showpage
%%%% retried
START planner date 20090326001503
START driver date 20090326001503
INFO amdump amdump pid 22014
INFO driver driver pid 22043
INFO planner planner pid 22042
START taper datestamp 20090326001503 label Daily-36 tape 1
DISK planner jamon.slikon.local /var
WARNING planner disk jamon.slikon.local:/var, estimate of level 1 failed.
FAIL dumper jamon.slikon.local /var 20090326001503 0 [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]
  blah blah blah
PARTIAL chunker jamon.slikon.local /var 20090326001503 0 [sec 64.950 kb 268358 kps 4132.2]
SUCCESS dumper jamon.slikon.local /var 20090326001503 0 [sec 53.356 kb 268357 kps 5029.5 orig-kb 2985670]
SUCCESS chunker jamon.slikon.local /var 20090326001503 0 [sec 58.396 kb 268357 kps 4596.0]
STATS driver estimate jamon.slikon.local /var 20090326001503 0 [sec 62 nkb 2950092 ckb 266528 kps 4294]
PART taper Daily-36 1 jamon.slikon.local /var 20090326001503 1/1 0 [sec 15.589804 kb 268356 kps 17213.595632]
DONE taper jamon.slikon.local /var 20090326001503 1 0 [sec 15.589804 kb 268356 kps 17213.595632]
FINISH driver date 20090326001503 time 77506.015
%%%% retried-rpt
These dumps were to tape Daily-36.
The next tape Amanda expects to use is: 1 new tape.

FAILURE DUMP SUMMARY:
   jamon.slikon.local /var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]
   jamon.slikon.local /var lev 0  was successfully retried


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:01       0:01       0:00
Output Size (meg)         524.1      524.1        0.0
Original Size (meg)      2915.7     2915.7        0.0
Avg Compressed Size (%)    18.0       18.0        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)     10059.1    10059.1        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)           262.1      262.1        0.0
Tape Used (%)             873.6      873.6        0.0
Filesystems Taped             1          1          0

Parts Taped                   1          1          0
Avg Tp Write Rate (k/s) 17213.6    17213.6        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Daily-36       0:00   268356k  873.6     1     1


FAILED DUMP DETAILS:

/--  jamon.slikon.local /var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]
blah blah blah
\--------


NOTES:
  planner: disk jamon.slikon.local:/var, estimate of level 1 failed.


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
-------------------------- ------------------------------------- --------------
jamon.slikon /var        0 2985670  268356    9.0    0:53 5029.5   0:16 17213.6

(brought to you by Amanda version x.y.z)
%%%% retried-postscript
--PS-TEMPLATE--
(March 26, 2009) DrawDate

(Amanda Version x.y.z) DrawVers
(Daily-36) DrawTitle
(Total Size:         262.1 MB) DrawStat
(Tape Used (%)       873.6 %) DrawStat
(Number of files:      1) DrawStat
(Filesystems Taped:    1) DrawStat
(-) (Daily-36) (-) (  0) (      32) (      32) DrawHost
(jamon.slikon.local) (/var) (0) (  1) ( 2985670) (  268356) DrawHost

showpage
%%%% taperr
INFO amdump amdump pid 32302
INFO planner planner pid 32323
START planner date 20100303133320
DISK planner euclid /A/p/etc
INFO driver driver pid 32324
START driver date 20100303133320
STATS driver hostname euclid
INFO dumper dumper pid 32331
INFO dumper dumper pid 32337
STATS driver startup time 0.130
INFO dumper dumper pid 32338
INFO dumper dumper pid 32335
INFO taper taper pid 32326
ERROR taper no-tape [Virtual-tape directory /A/p/vtapes does not exist.]
FINISH planner date 20100303133320 time 1.137
INFO planner pid-done 32323
INFO chunker chunker pid 32351
INFO dumper gzip pid 32355
SUCCESS dumper euclid /A/p/etc 20100303133320 0 [sec 0.040 kb 100 kps 2491.0 orig-kb 100]
SUCCESS chunker euclid /A/p/etc 20100303133320 0 [sec 0.064 kb 100 kps 2044.3]
INFO chunker pid-done 32351
STATS driver estimate euclid /A/p/etc 20100303133320 0 [sec 0 nkb 132 ckb 160 kps 1024]
INFO dumper pid-done 32337
INFO dumper pid-done 32335
INFO dumper pid-done 32338
INFO dumper pid-done 32355
INFO dumper pid-done 32331
INFO taper pid-done 32326
FINISH driver date 20100303133320 time 2.247
INFO driver pid-done 32324
%%%% taperr-rpt-holding
Hostname: euclid
Org     : DailySet1
Config  : TESTCONF
Date    : March 3, 2010

*** A TAPE ERROR OCCURRED: [Virtual-tape directory /A/p/vtapes does not exist.].
No dumps are left in the holding disk.

The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)           0.1        0.1        0.0
Original Size (meg)         0.1        0.1        0.0
Avg Compressed Size (%)   100.0      100.0        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)      2500.0     2500.0        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)             0.0        0.0        0.0
Tape Used (%)               0.0        0.0        0.0
Filesystems Taped             0          0          0

Parts Taped                   0          0          0
Avg Tp Write Rate (k/s)     --         --         --


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS   KB/s
-------------------------- ------------------------------------- -------------
euclid       /A/p/etc    0     100     100    --     0:00 2491.0

(brought to you by Amanda version x.y.z)
%%%% taperr-rpt-noholding
Hostname: euclid
Org     : DailySet1
Config  : TESTCONF
Date    : March 3, 2010

*** A TAPE ERROR OCCURRED: [Virtual-tape directory /A/p/vtapes does not exist.].
Some dumps may have been left in the holding disk.

The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)           0.1        0.1        0.0
Original Size (meg)         0.1        0.1        0.0
Avg Compressed Size (%)   100.0      100.0        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)      2500.0     2500.0        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)             0.0        0.0        0.0
Tape Used (%)               0.0        0.0        0.0
Filesystems Taped             0          0          0

Parts Taped                   0          0          0
Avg Tp Write Rate (k/s)     --         --         --


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS   KB/s
-------------------------- ------------------------------------- -------------
euclid       /A/p/etc    0     100     100    --     0:00 2491.0

(brought to you by Amanda version x.y.z)
%%%% spanned
INFO amdump amdump pid 30070
INFO planner planner pid 30091
START planner date 20100303153307
DISK planner euclid /A/b/server-src
INFO driver driver pid 30092
START driver date 20100303153307
STATS driver hostname euclid
INFO dumper dumper pid 30095
STATS driver startup time 0.044
INFO dumper dumper pid 30098
INFO dumper dumper pid 30100
INFO dumper dumper pid 30099
INFO taper taper pid 30094
FINISH planner date 20100303153307 time 5.128
INFO planner pid-done 30091
INFO chunker chunker pid 30160
INFO dumper gzip pid 30164
SUCCESS dumper euclid /A/b/server-src 20100303153307 0 [sec 0.264 kb 21830 kps 82641.8 orig-kb 21830]
INFO dumper pid-done 30164
SUCCESS chunker euclid /A/b/server-src 20100303153307 0 [sec 0.290 kb 21830 kps 75337.9]
INFO chunker pid-done 30160
STATS driver estimate euclid /A/b/server-src 20100303153307 0 [sec 21 nkb 21862 ckb 21888 kps 1024]
START taper datestamp 20100303153307 label Conf-001 tape 1
PART taper Conf-001 1 euclid /A/b/server-src 20100303153307 1/-1 0 [sec 0.020357 kb 5120 kps 251515.911452 orig-kb 21830]
PART taper Conf-001 2 euclid /A/b/server-src 20100303153307 2/-1 0 [sec 0.022239 kb 5120 kps 230222.763006 orig-kb 21830]
PART taper Conf-001 3 euclid /A/b/server-src 20100303153307 3/-1 0 [sec 0.019910 kb 5120 kps 257153.694334 orig-kb 21830]
PARTPARTIAL taper Conf-001 4 euclid /A/b/server-src 20100303153307 4/-1 0 [sec 0.017390 kb 4960 kps 285216.405648 orig-kb 21830] "No space left on device"
INFO taper Will request retry of failed split part.
INFO taper tape Conf-001 kb 15360 fm 4 [OK]
START taper datestamp 20100303153307 label Conf-002 tape 2
PART taper Conf-002 1 euclid /A/b/server-src 20100303153307 4/-1 0 [sec 0.022851 kb 5120 kps 224055.372485 orig-kb 21830]
PART taper Conf-002 2 euclid /A/b/server-src 20100303153307 5/-1 0 [sec 0.004047 kb 1350 kps 333557.846590 orig-kb 21830]
DONE taper euclid /A/b/server-src 20100303153307 5 0 [sec 0.089405 kb 21830 kps 244169.966680 orig-kb 21830]
INFO dumper pid-done 30095
INFO dumper pid-done 30098
INFO dumper pid-done 30099
INFO dumper pid-done 30100
INFO taper tape Conf-002 kb 6470 fm 2 [OK]
INFO taper pid-done 30094
FINISH driver date 20100303153307 time 7.391
INFO driver pid-done 30092
%%%% spanned-rpt
Hostname: euclid
Org     : DailySet1
Config  : TESTCONF
Date    : March 3, 2010

These dumps were to tapes Conf-001, Conf-002.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)          21.3       21.3        0.0
Original Size (meg)        21.3       21.3        0.0
Avg Compressed Size (%)   100.0      100.0        --
Filesystems Dumped            1          1          0
Avg Dump Rate (k/s)     82689.4    82689.4        --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)            21.3       21.3        0.0
Tape Used (%)              71.2       71.2        0.0
Filesystems Taped             1          1          0

Parts Taped                   6          6          0
Avg Tp Write Rate (k/s)  244170     244170        --

USAGE BY TAPE:
  Label          Time      Size      %    Nb    Nc
  Conf-001       0:00    20320k   66.2     1     4
  Conf-002       0:00     6470k   21.1     0     2


NOTES:
  taper: Will request retry of failed split part.
  taper: tape Conf-001 kb 15360 fm 4 [OK]
  taper: tape Conf-002 kb 6470 fm 2 [OK]


DUMP SUMMARY:
                                       DUMPER STATS                 TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS    KB/s MMM:SS     KB/s
-------------------------- -------------------------------------- ---------------
euclid       -server-src 0   21830   21830    --     0:00 82641.8   0:00 244170.0

(brought to you by Amanda version x.y.z)
%%%% spanned-postscript
--PS-TEMPLATE--
(March 3, 2010) DrawDate

(Amanda Version x.y.z) DrawVers
(Conf-001) DrawTitle
(Total Size:          19.8 MB) DrawStat
(Tape Used (%)        66.2 %) DrawStat
(Number of files:      4) DrawStat
(Filesystems Taped:    1) DrawStat
(-) (Conf-001) (-) (  0) (      32) (      32) DrawHost
(euclid) (/A/b/server-src) (0) (  1) (   21830) (   20320) DrawHost

showpage
--PS-TEMPLATE--
(March 3, 2010) DrawDate

(Amanda Version x.y.z) DrawVers
(Conf-002) DrawTitle
(Total Size:           6.3 MB) DrawStat
(Tape Used (%)        21.1 %) DrawStat
(Number of files:      2) DrawStat
(Filesystems Taped:    0) DrawStat
(-) (Conf-002) (-) (  0) (      32) (      32) DrawHost

showpage
%%%% fatal
INFO amdump amdump pid 14564
INFO driver driver pid 14588
INFO planner planner pid 14587
START planner date 20100303144314
START driver date 20100303144314
STATS driver hostname localhost.localdomain
DISK planner localhost /boot
WARNING planner tapecycle (3) <= runspercycle (10)
INFO planner Forcing full dump of localhost:/boot as directed.
INFO dumper dumper pid 14595
INFO dumper dumper pid 14596
INFO dumper dumper pid 14597
INFO dumper dumper pid 14600
INFO dumper dumper pid 14599
INFO dumper dumper pid 14598
INFO dumper dumper pid 14601
INFO dumper dumper pid 14602
INFO dumper dumper pid 14603
STATS driver startup time 0.214
INFO dumper dumper pid 14604
INFO taper taper pid 14590
WARNING planner disk localhost:/boot, full dump (83480KB) will be larger than available tape space
FAIL planner localhost /boot 20100303144314 0 "[dump larger than available tape space, 83480 KB, but cannot incremental dump new disk]"
FATAL planner cannot fit anything on tape, bailing out
WARNING driver WARNING: got empty schedule from planner
INFO dumper pid-done 14595
INFO dumper pid-done 14597
INFO dumper pid-done 14596
INFO dumper pid-done 14598
INFO dumper pid-done 14600
INFO dumper pid-done 14601
INFO dumper pid-done 14604
INFO dumper pid-done 14603
INFO dumper pid-done 14602
INFO dumper pid-done 14599
INFO taper pid-done 14590
FINISH driver date 20100303144314 time 8.150
INFO driver pid-done 14588
%%%% fatal-rpt
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : March 3, 2010

The next tape Amanda expects to use is: 1 new tape.

FAILURE DUMP SUMMARY:
  planner: FATAL cannot fit anything on tape, bailing out
   localhost /boot lev 0  FAILED [dump larger than available tape space, 83480 KB, but cannot incremental dump new disk]


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)         0:00
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)           0.0        0.0        0.0
Original Size (meg)         0.0        0.0        0.0
Avg Compressed Size (%)     --         --         -- 
Filesystems Dumped            0          0          0
Avg Dump Rate (k/s)         --         --         -- 

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)             0.0        0.0        0.0
Tape Used (%)               0.0        0.0        0.0
Filesystems Taped             0          0          0

Parts Taped                   0          0          0
Avg Tp Write Rate (k/s)     --         --         -- 


NOTES:
  planner: tapecycle (3) <= runspercycle (10)
  planner: Forcing full dump of localhost:/boot as directed.
  planner: disk localhost:/boot, full dump (83480KB) will be larger than available tape space
  driver: WARNING: got empty schedule from planner


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS 
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS   KB/s
-------------------------- ------------------------------------- -------------
localhost    /boot         FAILED

(brought to you by Amanda version x.y.z)
%%%% flush-origsize
INFO amflush amflush pid 11753
DISK amflush localhost /boot
START amflush date 20100303132501
INFO driver driver pid 11755
START driver date 20100303132501
STATS driver hostname localhost.localdomain
STATS driver startup time 0.020
INFO taper taper pid 11756
START taper datestamp 20100303132501 label TESTCONF02 tape 1
PART taper TESTCONF02 1 localhost /boot 20100303132432 1/-1 0 [sec 0.493936 kb 83480 kps 169009.900121 orig-kb 148870]
DONE taper localhost /boot 20100303132432 1 0 [sec 0.493936 kb 83480 kps 169009.900121 orig-kb 148870]
INFO taper tape TESTCONF02 kb 83480 fm 9 [OK]
INFO taper pid-done 11756
FINISH driver date 20100303132501 time 1.966
INFO driver pid-done 11755
INFO amflush pid-done 11754
%%%% flush-origsize-rpt
Hostname: localhost.localdomain                   
Org     : DailySet1                               
Config  : TESTCONF                                
Date    : March 3, 2010                           

The dumps were flushed to tape TESTCONF02.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            0.0        0.0        0.0
Original Size (meg)          0.0        0.0        0.0
Avg Compressed Size (%)      --         --         --
Filesystems Dumped             0          0          0
Avg Dump Rate (k/s)          --         --         --

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)             81.5       81.5        0.0
Tape Used (%)              271.8      271.8        0.0
Filesystems Taped              1          1          0
Parts Taped                    1          1          0
Avg Tp Write Rate (k/s)   169010     169010        --

USAGE BY TAPE:
  Label               Time         Size      %    Nb    Nc
  TESTCONF02          0:00       83480k  271.8     1     1

NOTES:
  taper: tape TESTCONF02 kb 83480 fm 9 [OK]


DUMP SUMMARY:
                                       DUMPER STATS                TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS     KB/s
-------------------------- ------------------------------------- ---------------
localhost    /boot       0  148870   83480   56.1     FLUSH        0:00 169009.9

(brought to you by Amanda version x.y.z)
%%%% flush-noorigsize
INFO amflush amflush pid 11753
DISK amflush localhost /boot
START amflush date 20100303132501
INFO driver driver pid 11755
START driver date 20100303132501
STATS driver hostname localhost.localdomain
STATS driver startup time 0.020
INFO taper taper pid 11756
START taper datestamp 20100303132501 label TESTCONF02 tape 1
PART taper TESTCONF02 1 localhost /boot 20100303132432 1/-1 0 [sec 0.493936 kb 83480 kps 169009.900121]
DONE taper localhost /boot 20100303132432 1 0 [sec 0.493936 kb 83480 kps 169009.900121]
INFO taper tape TESTCONF02 kb 83480 fm 9 [OK]
INFO taper pid-done 11756
FINISH driver date 20100303132501 time 1.966
INFO driver pid-done 11755
INFO amflush pid-done 11754
%%%% flush-noorigsize-rpt
Hostname: localhost.localdomain                   
Org     : DailySet1                               
Config  : TESTCONF                                
Date    : March 3, 2010                           

The dumps were flushed to tape TESTCONF02.
The next tape Amanda expects to use is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            0.0        0.0        0.0
Original Size (meg)          0.0        0.0        0.0
Avg Compressed Size (%)      --         --         --
Filesystems Dumped             0          0          0
Avg Dump Rate (k/s)          --         --         --

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)             81.5       81.5        0.0
Tape Used (%)              271.8      271.8        0.0
Filesystems Taped              1          1          0
Parts Taped                    1          1          0
Avg Tp Write Rate (k/s)   169010     169010        --

USAGE BY TAPE:
  Label               Time         Size      %    Nb    Nc
  TESTCONF02          0:00       83480k  271.8     1     1

NOTES:
  taper: tape TESTCONF02 kb 83480 fm 9 [OK]


DUMP SUMMARY:
                                       DUMPER STATS                TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS     KB/s
-------------------------- ------------------------------------- ---------------
localhost    /boot       0           83480    --      FLUSH        0:00 169009.9

(brought to you by Amanda version x.y.z)
%%%% plannerfail
START planner date 20100313212012
START driver date 20100313212012
DISK planner 1.2.3.4 SystemState
STATS driver hostname advantium
DISK planner 1.2.3.4 "C:/"
DISK planner 1.2.3.4 "E:/Replication/Scripts"
DISK planner 1.2.3.4 "G:/"
STATS driver startup time 0.051
INFO dumper dumper pid 11362
INFO dumper dumper pid 11359
INFO dumper dumper pid 11360
INFO dumper dumper pid 11361
INFO taper taper pid 11358
INFO taper Will write new label `winsafe-002' to new tape
FAIL planner 1.2.3.4 "G:/" 20100313212012 0 "[Request to 1.2.3.4 failed: recv error: Connection reset by peer]"
FAIL planner 1.2.3.4 "E:/Replication/Scripts" 20100313212012 0 "[Request to 1.2.3.4 failed: recv error: Connection reset by peer]"
FAIL planner 1.2.3.4 "C:/" 20100313212012 0 "[Request to 1.2.3.4 failed: recv error: Connection reset by peer]"
FAIL planner 1.2.3.4 SystemState 20100313212012 0 "[Request to 1.2.3.4 failed: recv error: Connection reset by peer]"
FINISH planner date 20100313212012 time 2113.308
WARNING driver WARNING: got empty schedule from planner
FINISH driver date 20100313212012 time 2114.332
%%%% plannerfail-rpt
Hostname: advantium
Org     : DailySet1
Config  : TESTCONF
Date    : March 13, 2010

The next tape Amanda expects to use is: 1 new tape.
FAILURE DUMP SUMMARY:
  1.2.3.4 SystemState lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]
  1.2.3.4 "C:/" lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]
  1.2.3.4 "E:/Replication/Scripts" lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]
  1.2.3.4 "G:/" lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]



STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)     0:35
Run Time (hrs:min)          0:35
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            0.0        0.0        0.0
Original Size (meg)          0.0        0.0        0.0
Avg Compressed Size (%)      --         --         --
Filesystems Dumped             0          0          0
Avg Dump Rate (k/s)          --         --         --

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)              0.0        0.0        0.0
Tape Used (%)                0.0        0.0        0.0
Filesystems Taped              0          0          0
Parts Taped                    0          0          0
Avg Tp Write Rate (k/s)      --         --         --

NOTES:
  driver: WARNING: got empty schedule from planner
  taper: Will write new label `winsafe-002' to new tape


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS   KB/s
-------------------------- ------------------------------------- -------------
1.2.3.4      "C:/"         FAILED
1.2.3.4      "-/Scripts"   FAILED
1.2.3.4      "G:/"         FAILED
1.2.3.4      SystemState   FAILED

(brought to you by Amanda version x.y.z)
%%%% skipped
START planner date 20090326001503
START driver date 20090326001503
INFO amdump amdump pid 22014
INFO driver driver pid 22043
INFO planner planner pid 22042
INFO taper taper pid 22044
DISK planner ns-new.slikon.local /boot
SUCCESS planner ns-new.slikon.local /boot 20090326001503 1 "[skipped: skip-incr]"
START taper datestamp 20090326001503 label Daily-36 tape 1
INFO dumper pid-done 7337
FINISH driver date 20090326001503 time 77506.015
INFO driver pid-done 22043
%%%% skipped-rpt
These dumps were to tape Daily-36.
The next tape Amanda expects to use is: 1 new tape.

STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)    0:00
Run Time (hrs:min)        21:32
Dump Time (hrs:min)        0:00       0:00       0:00
Output Size (meg)           0.0        0.0        0.0
Original Size (meg)         0.0        0.0        0.0
Avg Compressed Size (%)     --         --         --
Filesystems Dumped            0          0          0
Avg Dump Rate (k/s)         --         --         --

Tape Time (hrs:min)        0:00       0:00       0:00
Tape Size (meg)             0.0        0.0        0.0
Tape Used (%)               0.0        0.0        0.0
Filesystems Taped             0          0          0

Parts Taped                   0          0          0
Avg Tp Write Rate (k/s)     --         --         --

USAGE BY TAPE:
  Label          Time      Size      %    Nb	Nc
  Daily-36       0:00        0k    0.0     0     0


DUMP SUMMARY:
                                       DUMPER STATS               TAPER STATS
HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
-------------------------- ------------------------------------- -------------
ns-new.sliko /boot         SKIPPED -------------------------------------------

(brought to you by Amanda version x.y.z)
%%%% filesystemstaped
INFO amflush amflush pid 1239
DISK amflush localhost.localdomain /boot
START amflush date 20100713120014
INFO driver driver pid 1240
START driver date 20100713120014
STATS driver hostname localhost.localdomain
STATS driver startup time 0.015
INFO taper taper pid 1241
START taper datestamp 20100713120014 label DAILY-37 tape 1
PART taper DAILY-37 1 localhost.localdomain /boot 20100713111516 1/-1 1 [sec 0.096877 kb 10240 kps 105701.444021 orig-kb 20480]
PART taper DAILY-37 2 localhost.localdomain /boot 20100713111516 2/-1 1 [sec 0.079061 kb 10240 kps 129519.788435 orig-kb 20480]
DONE taper localhost.localdomain /boot 20100713111516 2 1 [sec 0.100000 kb 20480 kps 446100.000000 orig-kb 20480]
PART taper DAILY-37 3 localhost.localdomain /boot 20100713111517 1/-1 2 [sec 0.096877 kb 10240 kps 105701.444021 orig-kb 10240]
DONE taper localhost.localdomain /boot 20100713111517 1 2 [sec 0.100000 kb 10240 kps 446100.000000 orig-kb 10240]
INFO taper tape DAILY-37 kb 30720 fm 3 [OK]
INFO taper pid-done 1241
FINISH driver date 20100713120014 time 2.534
INFO driver pid-done 1240
INFO amflush pid-done 1239
%%%% filesystemstaped-rpt
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : July 13, 2010

The dumps were flushed to tape DAILY-37.
The next tape Amanda expects to use is: 1 new tape.

STATISTICS:
                          Total       Full      Incr.
                        --------   --------   --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            0.0        0.0        0.0
Original Size (meg)          0.0        0.0        0.0
Avg Compressed Size (%)      --         --         --
Filesystems Dumped             0          0          0
Avg Dump Rate (k/s)          --         --         --

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)             30.0        0.0       30.0
Tape Used (%)              100.1        0.0      100.1  (level:#disks ...)
Filesystems Taped              2          0          2  (1:1 2:1)
                                                        (level:#parts ...)
Parts Taped                    3          0          3  (1:2 2:1)
Avg Tp Write Rate (k/s)   153600        --      153600

USAGE BY TAPE:
  Label               Time         Size      %    Nb    Nc
  DAILY-37            0:00       30720k  100.1     2     3

NOTES:
  taper: tape DAILY-37 kb 30720 fm 3 [OK]


DUMP SUMMARY:
                                      DUMPER STATS                TAPER STATS
 HOSTNAME     DISK        L ORIG-kB  OUT-kB  COMP%  MMM:SS   KB/s MMM:SS     KB/s
 -------------------------- ------------------------------------- ---------------
 localhost.lo /boot       2   10240   10240    --      FLUSH        0:00 446100.0
 localhost.lo /boot       1   20480   20480    --      FLUSH        0:00 446100.0

(brought to you by Amanda version x.y.z)
