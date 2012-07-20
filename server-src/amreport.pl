#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use IPC::Open3;
use Cwd qw( abs_path );
use FileHandle;
use POSIX;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Tapelist;
use Amanda::Disklist;
use Amanda::Constants;
use Amanda::Debug qw( debug warning );
use Amanda::Report;
use Amanda::Report::human;
use Amanda::Logfile qw( find_latest_log);

# constants for dealing with outputs
use constant FORMAT  => 0;
use constant FMT_TYP => 0;
use constant FMT_TEMPLATE => 1;

use constant OUTPUT  => 1;
use constant OUT_TYP => 0;
use constant OUT_DST => 1;

# what mode is this running in? MODE_SCRIPT is when run from scripts like
# amdump, while MODE_CMDLINE is when run from the command line
use constant MODE_NONE    => 0;
use constant MODE_SCRIPT  => 1;
use constant MODE_CMDLINE => 2;

## Global Variables

my $opt_nomail = 0;
my ($opt_mailto, $opt_filename, $opt_logfname, $opt_psfname, $opt_xml);
my ($config_name, $report, $outfh);
my $mode = MODE_NONE;

# list of [ report-spec, output-spec ]
my (@outputs, @output_queue);

## Program subroutines

sub usage
{
    print <<EOF;
Usage: amreport [--version] [--help] [-o configoption] <conf>
  command-line mode options:
    [--log=logfile] [--ps=filename] [--text=filename] [--xml=filename]
    [--print=printer] [--mail-text=recipient]
  script-mode options:
    [-i] [-M address] [-f output-file] [-l logfile] [-p postscript-file]
    [--from-amdump]

Amreport uses short options for use from shell scripts (e.g., amreport), or
long options for use on the command line.

If the printer is omitted, the printer from the configuration is used.  If the
filename is omitted or is "-", output is to stdout.  If the recipient is
omitted, then the default mailto from the configuration is used.

If no options are given, a text report is printed to stdout.  The --from-amdump
option triggers script mode, and is used by amdump.
EOF
    exit 1;
}

sub error
{
    my ( $error_msg, $exit_code ) = @_;
    warning("error: $error_msg");
    print STDERR "$error_msg\n";
    exit $exit_code;
}

sub set_mode
{
    my ($new_mode) = @_;

    if ($mode != MODE_NONE && $mode != $new_mode) {
	error("cannot mix long options (command-line mode), and "
	    . "short options (script mode) with each other", 1);
    }

    $mode = $new_mode;
}

# Takes a string specifying an option name (e.g. "M") and a reference to a
# scalar variable. It's return values are suitable for use in the middle of
# option specification, e.g. GetOptions("foo" => \$foo, opt_set_var("bar", \$bar)
# It will only let the option be specified (at most) once, though, and will
# print an error message and exit otherwise.
sub opt_set_var
{
    my ($opt, $ref) = @_;
    error("must pass scalar ref to opt_set_var", 1)
      unless (ref($ref) eq "SCALAR");

    return (
        "$opt=s",
        sub {
            my ($op, $val) = @_;

	    # all short options are legacy options
	    set_mode(MODE_SCRIPT);

            if (defined($$ref)) {
                error("you may specify at most one -$op\n", 1);
            } else {
                $$ref = $val;
            }
        }
    );
}


sub opt_push_queue
{
    my ($output) = @_;

    unless ((ref $output eq "ARRAY")
        && (ref $output->[0] eq "ARRAY")
        && (ref $output->[1] eq "ARRAY")) {
        die "error: bad argument to opt_push_queue()";
    }

    # all queue-pushing options are command-line options
    set_mode(MODE_CMDLINE);

    push @output_queue, $output;
}

sub get_default_logfile
{
    my $logdir  = config_dir_relative(getconf($CNF_LOGDIR));
    my $logfile = "$logdir/log";

    if (-f $logfile) {
        return $logfile;

    } elsif ($mode == MODE_CMDLINE) {

        $logfile = "$logdir/" . find_latest_log($logdir);
        return $logfile if -f $logfile;
    }

    # otherwise, bail out
    error("nothing to report on!", 1);
}

sub apply_output_defaults
{
    my $ttyp         = getconf($CNF_TAPETYPE);
    my $tt           = lookup_tapetype($ttyp) if $ttyp;
    my $cfg_template = "" . tapetype_getconf($tt, $TAPETYPE_LBL_TEMPL) if $tt;

    my $cfg_printer = getconf($CNF_PRINTER);
    my $cfg_mailto = getconf_seen($CNF_MAILTO) ? getconf($CNF_MAILTO) : undef;

    foreach my $job (@output_queue) {

	# supply the configured template if none was given.
        if (   $job->[FORMAT]->[FMT_TYP] eq 'postscript'
            && !$job->[FORMAT]->[FMT_TEMPLATE]) {
            $job->[FORMAT]->[FMT_TEMPLATE] = $cfg_template;
        }

	# apply default destinations for each destination type
        if (!$job->[OUTPUT][OUT_DST]) {
            $job->[OUTPUT][OUT_DST] =
                ($job->[OUTPUT]->[OUT_TYP] eq 'printer') ? $cfg_printer
              : ($job->[OUTPUT]->[OUT_TYP] eq 'mail')    ? $cfg_mailto
              : ($job->[OUTPUT]->[OUT_TYP] eq 'file')    ? '-'
              :   undef;    # will result in error
        }

        push @outputs, $job;
    }
}


sub calculate_legacy_outputs {
    # Part of the "options" is the configuration.  Do we have a template?  And a
    # mailto? And mailer?

    my $ttyp = getconf($CNF_TAPETYPE);
    my $tt = lookup_tapetype($ttyp) if $ttyp;
    my $cfg_template = "" . tapetype_getconf($tt, $TAPETYPE_LBL_TEMPL) if $tt;

    my $cfg_mailer  = getconf($CNF_MAILER);
    my $cfg_printer = getconf($CNF_PRINTER);
    my $cfg_mailto  = getconf_seen($CNF_MAILTO) ? getconf($CNF_MAILTO) : undef;

    if (!defined $opt_mailto) {
	# ignore the default value for mailto
	$opt_mailto = getconf_seen($CNF_MAILTO)? getconf($CNF_MAILTO) : undef;
	# (note that we still may not send mail if CNF_MAILER is not set)
    } else {
	# check that mailer is defined if we got an explicit -M, but go on
	# processing (we will probably do nothing..)
	if (!$cfg_mailer) {
	    warning("a mailer is not defined; will not send mail");
	    print "Warning: a mailer is not defined";
	}
    }

    # should we send a mail?
    if ($cfg_mailer and $opt_mailto) {
        # -i and -f override this
	if (!$opt_nomail and !$opt_filename) {
	    push @outputs, [ [ 'human' ], [ 'mail', $opt_mailto ] ];
	}
    }

    # human/xml output to a file?
    if ($opt_filename) {
	if ($opt_xml) {
	    push @outputs, [ [ 'xml' ], [ 'file', $opt_filename ] ];
	} else {
	    push @outputs, [ [ 'human' ], [ 'file', $opt_filename ] ];
	}
    }

    # postscript output to a printer?
    # (this is just silly)
    if ($Amanda::Constants::LPR and $cfg_template) {
	# oddly, -i ($opt_nomail) will disable printing, but -i -f prints.
	if ((!$opt_nomail and !$opt_psfname) or ($opt_nomail and $opt_filename)) {
	    # but we don't print if the text report isn't going anywhere
	    unless ((!$cfg_mailer or !$opt_mailto) and !($opt_filename and !$opt_xml)) {
		push @outputs, [ [ 'postscript', $cfg_template ], [ 'printer', $cfg_printer ] ]
	    }
	}
    }

    # postscript output to a file?
    if ($opt_psfname and $cfg_template) {
	push @outputs, [ [ 'postscript', $cfg_template ], [ 'file', $opt_psfname ] ];
    }
}

sub legacy_send_amreport
{
    my ($output) = @_;
    my $cfg_send = getconf($CNF_SEND_AMREPORT_ON);

    ## only check $cfg_send if we are in script mode and sending mail
    return 1 if ($mode != MODE_SCRIPT);
    return 1 if !($output->[OUTPUT]->[OUT_TYP] eq "mail");

    ## do not bother checking for errors or stranges if set to 'all' or 'never'
    return 1 if ($cfg_send == $SEND_AMREPORT_ALL);
    return 0 if ($cfg_send == $SEND_AMREPORT_NEVER);

    my $output_name = join(" ", @{ $output->[FORMAT] }, @{ $output->[OUTPUT] });
    my $send_amreport = 0;

    debug("testingamreport_send_on=$cfg_send, output:$output_name");

    if ($cfg_send == $SEND_AMREPORT_STRANGE) {

        if (   !$report->get_flag("got_finish")
	    || ($report->get_flag("dump_failed") != 0)
	    || ($report->get_flag("results_missing") != 0)
	    || ($report->get_flag("dump_strange") != 0)) {

            debug("send-amreport-on=$cfg_send, condition filled for $output_name");
            $send_amreport = 1;

        } else {

            debug("send-amreport-on=$cfg_send, condition not filled for $output_name");
            $send_amreport = 0;
        }

    } elsif ($cfg_send = $SEND_AMREPORT_ERROR) {

        if (   !$report->get_flag("got_finish")
            || ($report->get_flag("exit_status") != 0)
            || ($report->get_flag("dump_failed") != 0)
            || ($report->get_flag("results_missing") != 0)
            || ($report->get_flag("dump_strange") != 0)) {

            debug("send-amreport-on=$cfg_send, condition filled for $output_name");
            $send_amreport = 1;

        } else {

            debug("send-amreport-on=$cfg_send, condition not filled for $output_name");
            $send_amreport = 0;
        }
    }

    return $send_amreport;
}

sub open_file_output {
    my ($report, $outputspec) = @_;

    my $filename = $outputspec->[1];
    $filename = Amanda::Util::get_original_cwd() . "/$filename"
      unless ($filename eq "-" || $filename =~ m{^/});

    if ($filename eq "-") {
	return \*STDOUT;
    } else {
	open my $fh, ">", $filename or die "Cannot open '$filename': $!";
	return $fh;
    }
}

sub open_printer_output
{
    my ($report, $outputspec) = @_;
    my $printer = $outputspec->[1];

    my @cmd;
    if ($printer and $Amanda::Constants::LPRFLAG) {
	@cmd = ( $Amanda::Constants::LPR, $Amanda::Constants::LPRFLAG, $printer );
    } else {
	@cmd = ( $Amanda::Constants::LPR );
    }

    debug("invoking printer: " . join(" ", @cmd));

    # redirect stdout/stderr to stderr, which is usually the amdump log
    my ($pid, $fh);
    if (!-f $Amanda::Constants::LPR || !-x $Amanda::Constants::LPR) {
	my $errstr = "error: the mailer '$Amanda::Constants::LPR' is not an executable program.";
	print STDERR "$errstr\n";
        if ($mode == MODE_SCRIPT) {
            debug($errstr);
        } else {
            error($errstr, 1);
        }
    } else {
	eval { $pid = open3($fh, ">&2", ">&2", @cmd); } or do {
            ($pid, $fh) = (0, undef);
            chomp $@;
            my $errstr = "error: $@: $!";

	    print STDERR "$errstr\n";
            if ($mode == MODE_SCRIPT) {
		debug($errstr);
            } else {
		error($errstr, 1);
            }
        };
    }
    return ($pid, $fh);
}

sub open_mail_output
{
    my ($report, $outputspec) = @_;
    my $mailto = $outputspec->[1];

    if ($mailto =~ /[*<>()\[\];:\\\/"!$|]/) {
        error("mail addresses have invalid characters", 1);
    }

    my $datestamp =
      $report->get_program_info(
        $report->get_flag("amflush_run") ? "amflush" : 
	$report->get_flag("amvault_run") ? "amvault" : "planner", "start" );

    $datestamp /= 1000000 if $datestamp > 99999999;
    $datestamp = int($datestamp);
    my $year  = int( $datestamp / 10000 ) - 1900;
    my $month = int( ( $datestamp / 100 ) % 100 ) - 1;
    my $day   = int( $datestamp % 100 );
    my $date  = POSIX::strftime( '%B %e, %Y', 0, 0, 0, $day, $month, $year );
    $date =~ s/  / /g;

    my $done = "";
    if (  !$report->get_flag("got_finish")
	|| $report->get_flag("dump_failed") != 0) {
	$done = " FAIL:";
    } elsif ($report->get_flag("results_missing") != 0) {
	$done = " MISSING:";
    } elsif ($report->get_flag("dump_strange") != 0) {
	$done = " STRANGE:";
    }

    my $subj_str =
        getconf($CNF_ORG) . $done
      . ( $report->get_flag("amflush_run") ? " AMFLUSH" :
	  $report->get_flag("amvault_run") ? " AMVAULT" : " AMANDA" )
      . " MAIL REPORT FOR "
      . $date;

    my $cfg_mailer = getconf($CNF_MAILER);

    my @cmd = ("$cfg_mailer", "-s", $subj_str, split(/ +/, $mailto));
    debug("invoking mail app: " . join(" ", @cmd));


    my ($pid, $fh);
    if (!-f $cfg_mailer || !-x $cfg_mailer) {
	my $errstr = "error: the mailer '$cfg_mailer' is not an executable program.";
	print STDERR "$errstr\n";
        if ($mode == MODE_SCRIPT) {
            debug($errstr);
        } else {
            error($errstr, 1);
        }
	
    } else {
	eval { $pid = open3($fh, ">&2", ">&2", @cmd) } or do {
            ($pid, $fh) = (0, undef);
            chomp $@;
            my $errstr = "error: $@: $!";

	    print STDERR "$errstr\n";
            if ($mode == MODE_SCRIPT) {
		debug($errstr);
            } else {
		error($errstr, 1);
            }
	};
    }

    return ($pid, $fh);
}

sub run_output {
    my ($output) = @_;
    my ($reportspec, $outputspec) = @$output;

    # get the output
    my ($pid, $fh);
    if ($outputspec->[0] eq 'file') {
	$fh = open_file_output($report, $outputspec);
    } elsif ($outputspec->[0] eq 'printer') {
	($pid, $fh) = open_printer_output($report, $outputspec);
    } elsif ($outputspec->[0] eq 'mail') {
	($pid, $fh) = open_mail_output($report, $outputspec);
    }

    # TODO: add some generic error handling here.  must be compatible
    # with legacy behavior.

    if (defined $fh) {
	# TODO: modularize these better
	if ($reportspec->[0] eq 'xml') {
	    print $fh $report->xml_output("" . getconf($CNF_ORG), $config_name);
	} elsif ($reportspec->[0] eq 'human') {
	    my $hr = Amanda::Report::human->new($report, $fh, $config_name,
						$opt_logfname );
	    $hr->print_human_amreport();
	} elsif ($reportspec->[0] eq 'postscript') {
	    use Amanda::Report::postscript;
	    my $rep = Amanda::Report::postscript->new($report, $config_name,
						      $opt_logfname );
	    $rep->write_report($fh);
	}

	close $fh;
    }

    # clean up any subprocess
    if (defined $pid) {
	debug("waiting for child process to finish..");
	waitpid($pid, 0);
	if ($? != 0) {
	    warning("child exited with status $?");
	}
    }
}


## Application initialization

Amanda::Util::setup_application("amreport", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides( scalar(@ARGV) + 1 );

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw/bundling/);
GetOptions(

    ## old legacy configuration opts
    "i" => sub { set_mode(MODE_SCRIPT); $opt_nomail = 1; },
    opt_set_var("M", \$opt_mailto),
    opt_set_var("f", \$opt_filename),
    opt_set_var("l", \$opt_logfname),
    opt_set_var("p", \$opt_psfname),

    "o=s" => sub { add_config_override_opt($config_overrides, $_[1]); },

    ## trigger default amdump behavior
    "from-amdump" => sub { set_mode(MODE_SCRIPT) },

    ## new configuration opts
    "log=s" => sub { set_mode(MODE_CMDLINE); $opt_logfname = $_[1]; },
    "ps:s" => sub { opt_push_queue([ ['postscript'], [ 'file', $_[1] ] ]); },
    "mail-text:s" => sub { opt_push_queue([ ['human'], [ 'mail', $_[1] ] ]); },
    "text:s"      => sub { opt_push_queue([ ['human'], [ 'file', $_[1] ] ]); },
    "xml:s"       => sub { opt_push_queue([ ['xml'],   [ 'file', $_[1] ] ]); },
    "print:s"     => sub { opt_push_queue([ [ 'postscript' ], [ 'printer', $_[1] ] ]); },

    'version' => \&Amanda::Util::version_opt,
    'help'    => \&usage,
) or usage();

# set command line mode if no options were given
$mode = MODE_CMDLINE if ($mode == MODE_NONE);

if ($mode == MODE_CMDLINE) {
    (scalar @ARGV == 1) or usage();
} else {    # MODE_SCRIPT
    (scalar @ARGV > 0) or usage();
}

$config_name = shift @ARGV;    # only use first argument
$config_name ||= '.';          # default config is current dir

set_config_overrides($config_overrides);
config_init( $CONFIG_INIT_EXPLICIT_NAME, $config_name );

my ( $cfgerr_level, @cfgerr_errors ) = config_errors();
if ( $cfgerr_level >= $CFGERR_WARNINGS ) {
    config_print_errors();
    if ( $cfgerr_level >= $CFGERR_ERRORS ) {
        error( "errors processing config file", 1 );
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# read the tapelist
my $tl_file = config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist->new($tl_file);

# read the disklist
my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
$cfgerr_level += Amanda::Disklist::read_disklist('filename' => $diskfile);
($cfgerr_level < $CFGERR_ERRORS) || die "Errors processing disklist";

# shim for installchecks
$Amanda::Constants::LPR = $ENV{'INSTALLCHECK_MOCK_LPR'}
    if exists $ENV{'INSTALLCHECK_MOCK_LPR'};

# calculate the logfile to read from
$opt_logfname = Amanda::Util::get_original_cwd() . "/" . $opt_logfname
	if defined $opt_logfname and $opt_logfname !~ /^\//;
my $logfile = $opt_logfname || get_default_logfile();
my $historical = defined $opt_logfname;
debug("using logfile: $logfile" . ($historical? " (historical)" : ""));

if ($mode == MODE_CMDLINE) {
    debug("operating in cmdline mode");
    apply_output_defaults();
    push @outputs, [ ['human'], [ 'file', '-' ] ] if !@outputs;
} else {
    debug("operating in script mode");
    calculate_legacy_outputs();
}

## Parse the report & set output

$report = Amanda::Report->new($logfile, $historical);
my $exit_status = $report->get_flag("exit_status");

## filter outputs by errors & stranges

@outputs = grep { legacy_send_amreport($_) } @outputs;

for my $output (@outputs) {
    debug("planned output: " . join(" ", @{ $output->[FORMAT] }, @{ $output->[OUTPUT] }));
}

## Output

for my $output (@outputs) {
    run_output($output);
}

Amanda::Util::finish_application();
exit $exit_status;
