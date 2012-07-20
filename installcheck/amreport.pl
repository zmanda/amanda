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

use Test::More tests => 167;

use strict;
use warnings;
use Errno;
use Cwd qw(abs_path);
use lib "@amperldir@";

use Installcheck;
use Installcheck::Run qw( run run_get run_err );
use Installcheck::Catalogs;
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Util qw( slurp burp );
use Amanda::Debug;
use Amanda::Config qw ( :init :getconf );

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

my $cat;
my $alternate_log_filename="$Installcheck::TMP/installcheck-log";
my $current_log_filename="$Installcheck::CONFIG_DIR/TESTCONF/log/log";
my $out_filename="$Installcheck::TMP/installcheck-amreport-output";

sub setup_config {
    my %params = @_;
    my $testconf = Installcheck::Run::setup();

    cleanup();

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

    if (defined $params{'runtapes'}) {
	$testconf->remove_param('runtapes');
	$testconf->add_param('runtapes', $params{'runtapes'});
    }
    if (defined $params{'tapecycle'}) {
	$testconf->remove_param('tapecycle');
	$testconf->add_param('tapecycle', $params{'tapecycle'});
    }

    $testconf->write();

    undef $cat;
    if ($params{'catalog'}) {
	$cat = Installcheck::Catalogs::load($params{'catalog'});
	$cat->install();
    }
}

sub cleanup {
    unlink $out_filename;
    unlink $mail_output;
    unlink $printer_output;
    unlink $alternate_log_filename;
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

## try a few various options with a pretty normal logfile.  Note that
## these tests all use amreport's script mode

setup_config(catalog => 'normal',
    want_mailer => 1, want_mailto => 1, want_template => 1);

like(run_err($amreport, 'TESTCONF-NOSUCH'),
    qr/could not open conf/,
    "amreport with bogus config exits with error status and error message");

ok(!run($amreport, 'TESTCONF-NOSUCH', '--help'),
    "amreport --help exits with status 1");
like($Installcheck::Run::stdout,
    qr/Usage: amreport \[--version\]/,
    "..and prints usage message");

like(run_get($amreport, 'TESTCONF-NOSUCH', '--version'),
    qr/^amreport-.*/,
    "amreport --version gives version");

ok(run($amreport, 'TESTCONF', '--from-amdump'),
    "amreport, as run from amdump, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);

is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");

results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

ok(run($amreport, 'TESTCONF', '--from-amdump', '/garbage/directory/'),
    "amreport, as run from amdump, with mailer, mailto, and a template, and  bogus option")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

cleanup();

ok(run($amreport, 'TESTCONF', '-M', 'somebody@localhost'),
    "amreport -M, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "somebody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

cleanup();

ok(run($amreport, 'TESTCONF', '-i'),
    "amreport -i, with mailer, mailto, and a template => no error");
ok(! -f $mail_output,
    "..doesn't mail");
ok(! -f $printer_output,
    "..doesn't print");

cleanup();

ok(run($amreport, 'TESTCONF', '-p', $out_filename),
    "amreport -p, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
ok(! -f $printer_output,
    "..doesn't print");
results_match($out_filename, $cat->get_text('postscript'), "..postscript file matches");

# test a bare 'amreport', which should now output to stdout
cleanup();

ok(run($amreport, 'TESTCONF'),
    "amreport with no other options outputs to stdout for user convenience")
  or diag($Installcheck::Run::stderr);
results_match('stdout', $cat->get_text('rpt1'),
    "..output matches");
ok(!-f $printer_output, "..no printer output")
  or diag("error: printer output!:\n" . burp($printer_output));
ok(!-f $mail_output, "..no mail output")
  or diag("error: mail output!:\n" . burp($printer_output));

# test long-form file option
cleanup();

ok(run($amreport, 'TESTCONF', "--text=$out_filename"),
    "amreport --text=foo, no other options")
  or diag($Installcheck::Run::stderr);
results_match($out_filename, $cat->get_text('rpt1'),
    "..output matches");
ok(!-f $printer_output, "..no printer output")
  or diag("error: printer output!:\n" . burp($printer_output));
ok(!-f $mail_output, "..no mail output")
  or diag("error: mail output!:\n" . burp($printer_output));

# test long form postscript option
cleanup();

ok(
    run($amreport, 'TESTCONF', '--ps', $out_filename),
    "amreport --ps foo, no other options"
);
results_match($out_filename, $cat->get_text('postscript'), '..results match');
ok(!-f $printer_output, "..no printer output");
ok(!-f $mail_output, "..no mail output");

cleanup();

# test new mail option, using config mailto
setup_config(catalog => 'normal',
	     want_mailer => 1, want_mailto => 1, want_template => 1);

ok(run($amreport, 'TESTCONF', '--mail-text'),
    "amreport --mail-text, no other options, built-in mailto");
results_match(
    $mail_output,
    make_mail(
        $cat->get_text('rpt1'), "DailySet1", 0,
        "February 25, 2009",   "nobody\@localhost"
    ),
    "..mail matches"
);
ok(!-f $printer_output, "..no printer output");
ok(!-f $out_filename,   "..no file output");

cleanup();

# test new mail option, using passed mailto
ok(run($amreport, 'TESTCONF', '--mail-text=somebody@localhost',),
    'amreport --mail-text=somebody\@localhost, no other options');
results_match(
    $mail_output,
    make_mail(
        $cat->get_text('rpt1'), "DailySet1", 0,
        "February 25, 2009",   "somebody\@localhost"
    ),
    "..mail matches"
);
ok(!-f $printer_output, "..no printer output");
ok(!-f $out_filename, "..no file output");

cleanup();

# test long-form log option
burp($alternate_log_filename, $cat->get_file('log/log'));
ok(
    run($amreport, 'TESTCONF', '--log', $alternate_log_filename),
    "amreport --log with old log, no other config options"
);
results_match('stdout', $cat->get_text('rpt1'),
    '..stdout output matches');
ok(!-f $mail_output, "..no mail output");
ok(!-f $out_filename, "..no file output");
ok(!-f $printer_output, "..no printer output");

cleanup();

# test long-form print option, without specified printer
setup_config(catalog => 'normal', want_template => 1);
ok(run($amreport, 'TESTCONF', '--print'),
    'amreport --print, no other options');
results_match(
    $printer_output,
    $cat->get_text('postscript'),
    "..printer output matches"
);
ok(!-f $mail_output,  "..no mail output");
ok(!-f $out_filename, "..no file output");

cleanup();

setup_config(catalog => 'normal',
    want_mailer => 1, want_mailto => 1, want_template => 1);
ok(run($amreport, 'TESTCONF', '-i', '-p', $out_filename),
    "amreport -i -p, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
ok(! -f $mail_output,
    "..produces no mail");
ok(! -f $printer_output,
    "..doesn't print");
results_match($out_filename,
    $cat->get_text('postscript'),
    "..postscript output in -p file matches");

cleanup();

ok(run($amreport, 'TESTCONF', '-i', '-f', $out_filename),
    "amreport -i -f, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
ok(! -f $mail_output,
    "..produces no mail");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");
results_match($out_filename,
    $cat->get_text('rpt1'),
    "..report output in -f file matches");

cleanup();

burp($alternate_log_filename, $cat->get_file('log/log'));
ok(run($amreport, 'TESTCONF', '-l', $alternate_log_filename),
    "amreport -l, with mailer, mailto, and a template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'normal',
    want_mailer => 1);

ok(run($amreport, 'TESTCONF', '--from-amdump'),
    "amreport --from-amdump, with mailer but no mailto and no template => exit==0");
ok(! -f $mail_output, "..doesn't mail");
ok(! -f $printer_output, "..doesn't print");

cleanup();

ok(run($amreport, 'TESTCONF', '--from-amdump', '-o', 'mailto=hello'),
    "amreport -o to set mailto, with mailer but no mailto and no template")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "hello"),
    "..mail matches");
ok(! -f $printer_output,
    "..doesn't print");

cleanup();

like(run_err($amreport, 'TESTCONF', '--from-amdump', '-o', 'mailto=ill\egal'),
    qr/mail addresses have invalid characters/,
    "amreport with illegal email in -o, with mailer but no mailto and no template, errors out");

setup_config(catalog => 'normal',
    want_mailer => 1, want_template => 1);

ok(run($amreport, 'TESTCONF', '--from-amdump'),
    "no-args amreport with mailer, no mailto, and a template does nothing even though it could "
	. "print a label"); # arguably a bug, but we'll keep it for now
ok(! -f $mail_output, "..doesn't mail");
ok(! -f $printer_output, "..doesn't print");

cleanup();

ok(run($amreport, 'TESTCONF', '--from-amdump', '-o', 'mailto=dustin'),
    "amreport with mailer, no mailto, and a template, but mailto in config override works")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "dustin"),
    "..mail matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

ok(run($amreport, 'TESTCONF', '-M', 'pcmantz'),
    "amreport with mailer, no mailto, and a template, but mailto in -M works")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "pcmantz"),
    "..mail matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'normal', want_template => 1);

like(run_get($amreport, 'TESTCONF', '-M', 'martineau'),
    qr/Warning: a mailer is not defined/,
    "amreport with no mailer, no mailto, and a template, but mailto in -M fails with "
	. "warning, but exit==0");
ok(! -f $mail_output, "..doesn't mail");
ok(! -f $printer_output, "..doesn't print");

setup_config(catalog => 'normal',
	want_mailer => 1, want_mailto => 1);

ok(run($amreport, 'TESTCONF', '-p', $out_filename), # XXX another probable bug
    "amreport with mailer, mailto, but no template, ignores -p option ")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
ok(! -f $printer_output,
    "..doesn't print");

cleanup();

ok(run($amreport, 'TESTCONF', '-o', "tapetype:TEST-TAPE-TEMPLATE:lbl_templ=$ps_template",
					    '-p', $out_filename),
    "amreport with mailer, mailto, but no template, minds -p option if template given via -o")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('rpt1'), "DailySet1", 0, "February 25, 2009", "nobody\@localhost"),
    "..mail matches");
results_match($out_filename,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'normal',
	bogus_mailer => 1, want_mailto => 1, want_template => 1);

ok(run($amreport, 'TESTCONF', '--from-amdump'),
    "amreport with bogus mailer; doesn't mail, still prints")
  or diag($Installcheck::Run::stderr);
ok(!-f $mail_output, "..produces no mail output");
is($Installcheck::Run::stdout, "", "..produces no stdout output");
$! = &Errno::ENOENT;
my $enoent = $!;
like($Installcheck::Run::stderr,
     qr/^error: the mailer '.*' is not an executable program\.$/,
     "..produces correct stderr output");
results_match(
    $printer_output,
    $cat->get_text('postscript'),
    "..printer output matches"
);

setup_config(
    catalog => 'normal',
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
    $cat->get_text('postscript'),
    "..printer output matches"
);

## test columnspec adjustments, etc.

setup_config(catalog => 'normal');

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'columnspec=OrigKB=::2'),
    "amreport with OrigKB=::2");
results_match($out_filename, $cat->get_text('rpt2'),
    "..result matches");

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'columnspec=OrigKB=:5'),
    "amreport with OrigKB=:5");
results_match($out_filename, $cat->get_text('rpt3'),
    "..result matches");

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'columnspec=OrigKB=:5:6'),
    "amreport with OrigKB=:5:6");
results_match($out_filename, $cat->get_text('rpt4'),
    "..result matches");

ok(run($amreport, 'TESTCONF', '-f', $out_filename, '-o', 'displayunit=m'),
    "amreport with displayunit=m");
results_match($out_filename, $cat->get_text('rpt5'),
    "..result matches");

setup_config(catalog => 'doublefailure',
    want_mailer => 1, want_template => 1);

ok(!run($amreport, 'TESTCONF', '-M', 'dustin'),
    "amreport with log in error")
    or diag($Installcheck::Run::stderr);
is($Installcheck::Run::stdout, "", "..produces no output");
results_match($mail_output,
    make_mail($cat->get_text('report'), "DailySet1", 1, "March 26, 2009", "dustin"),
    "..mail matches");

## some (anonymized) real logfiles, for regression testing

setup_config(catalog => 'strontium', want_template => 1);

ok(run($amreport, 'TESTCONF', '-f', $out_filename),
    "amreport with strontium logfile (simple example with multiple levels)");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'amflush', want_template => 1);

ok(run($amreport, 'TESTCONF', '-f', $out_filename),
    "amreport with amflush logfile (regression check for flush-related DUMP STATUS)");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'resultsmissing', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 12,
    "amreport with resultsmissing logfile ('RESULTS MISSING') exit==12");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'shortstrange', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 2,
    "amreport with shortstrange logfile exit==2");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'longstrange', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 2,
    "amreport with longstrange logfile exit==2");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'doublefailure', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with doublefailure logfile exit==4");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'bigestimate', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with bigestimate logfile exit==0");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'retried', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with retried logfile exit==4");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'retried-strange');

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 6,
    "amreport with retried logfile, with strange exit==6");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'retried-nofinish', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with retried logfile where driver did not finish exit==4");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'taperr', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 16,
    "amreport with taperr logfile exit==16");
results_match($out_filename, $cat->get_text('report-holding'),
    "..result matches");
ok((-f $printer_output and -z $printer_output),
    "..printer output exists but is empty");

burp($alternate_log_filename, $cat->get_file('log/log'));

# use an explicit -l here so amreport doesn't try to look at the holding disk
run($amreport, 'TESTCONF', '-f', $out_filename, '-l', $alternate_log_filename);
is($Installcheck::Run::exit_code, 16,
    "amreport with taperr logfile specified explicitly exit==16");
results_match($out_filename, $cat->get_text('report-noholding'),
    "..result matches");

setup_config(catalog => 'spanned', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with spanned logfile");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
results_match($printer_output,
    $cat->get_text('postscript'),
    "..printer output matches");

setup_config(catalog => 'fatal', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 5,
    "amreport with fatal logfile");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");
ok(-f $printer_output && -z $printer_output,
    "..printer output is empty (no dumps, no tapes)");

setup_config(catalog => 'flush-origsize', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with flush-origsize");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'flush-noorigsize', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with flush-noorigsize");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'plannerfail', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 4,
    "amreport with a planner failure (failed)");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'skipped', want_template => 1);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport with a planner skipped dump (success)");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'filesystemstaped', want_template => 1, runtapes => 3, tapecycle => 5);

run($amreport, 'TESTCONF', '-f', $out_filename);
is($Installcheck::Run::exit_code, 0,
    "amreport correctly report filesystem taped (success)");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

setup_config(catalog => 'multi-taper', want_template => 0);

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $logdir = getconf($CNF_LOGDIR);
my $logfile = $logdir . "/log.20100908110856.0";
run($amreport, 'TESTCONF', '-l', $logfile, '-f', $out_filename, '-o', 'TAPETYPE:TEST-TAPE-TEMPLATE:length=41m');
is($Installcheck::Run::exit_code, 0,
    "amreport correctly report multi-taper (success)");
results_match($out_filename, $cat->get_text('report'),
    "..result matches");

cleanup();
