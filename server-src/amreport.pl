#! @PERL@
# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
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
use Amanda::Constants;
use Amanda::Debug qw( debug warning );
use Amanda::Report;
use Amanda::Report::human;

## Global Variables

my $opt_nomail = 0;
my ( $opt_mailto, $opt_filename, $opt_logfname, $opt_psfname, $opt_xml );
my ( $config_name, $report, $outfh );


## Program subroutines

sub usage
{
    print <<EOF;
amreport conf [--xml] [-M address] [-f output-file] [-l logfile] [-p postscript-file] [-o configoption]*
EOF
    exit 1;
}

sub error
{
    my ( $error_msg, $exit_code ) = @_;
    warning("error: $error_msg");
    print STDERR $error_msg;
    exit $exit_code;
}

# Takes a string specifying an option name (e.g. "M") and a reference to a
# scalar variable. It's return values are suitable for use in the middle of
# option specification, e.g. GetOptions("foo" => \$foo, opt_set_var("bar", \$bar)
# It will only let the option be specified (at most) once, though, and will
# print an error message and exit otherwise.
sub opt_set_var
{
    my ( $opt, $ref ) = @_;
    die "must pass scalar ref to opt_set_var"
      unless ( ref($ref) eq "SCALAR" );

    return (
        "$opt=s",
        sub {
            my ( $op, $val ) = @_;
            if ( defined($$ref) ) {
                print "you may specify at most one -$op\n";
                exit 1;
            } else {
                $$ref = $val;
            }
        }
    );
}

sub hrmn
{
    my ($sec) = @_;
    my ( $hr, $mn ) = ( int( $sec / ( 60 * 60 ) ), int( $sec / 60 ) % 60 );
    return sprintf( '%d:%02d', $hr, $mn );
}

sub mnsc
{
    my ($sec) = @_;
    my ( $mn, $sc ) = ( int( $sec / (60) ), int( $sec % 60 ) );
    return sprintf( '%d:%02d', $mn, $sc );
}

sub calculate_outputs {
    # Part of the "options" is the configuration.  Do we have a template?  And a
    # mailto? And mailer?
    my $ttyp = getconf($CNF_TAPETYPE);
    my $tt = lookup_tapetype($ttyp) if $ttyp;
    my $cfg_template = "" . tapetype_getconf($tt, $TAPETYPE_LBL_TEMPL) if $tt;

    my $cfg_mailer = getconf($CNF_MAILER);
    my $cfg_printer = getconf($CNF_PRINTER);
    if (!defined $opt_mailto) {
	# ignore the default value for mailto
	$opt_mailto = getconf_seen($CNF_MAILTO)? getconf($CNF_MAILTO) : undef;
    } else {
	# check that mailer is defined if we got an explicit -M, but go on
	# processing (we will probably do nothing..)
	if (!$cfg_mailer) {
	    warning("a mailer is not defined; will not send mail");
	    print "Warning: a mailer is not defined";
	}
    }

    # list of [ report-spec, output-spec ]
    my @outputs;

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

    for my $output (@outputs) {
	debug("planned output: " . join(" ", @{$output->[0]}, @{$output->[1]}));
    }

    return @outputs;
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

    my $pid = open3( my $fh, undef, undef, @cmd)
      or error("cannot start $cmd[0]: $!", 1);
    return ($pid, $fh);
}

sub open_mail_output
{
    my ($report, $outputspec) = @_;
    my $mailto = $outputspec->[1];

    if ($mailto =~ /[*<>()\[\];:\\\/"!$|]/) {
        error("mail address has invalid characters", 1);
    }

    my $datestamp =
      $report->get_program_info(
        $report->get_flag("amflush_run") ? "amflush" : "planner", "start" );

    $datestamp /= 1000000 if $datestamp > 99999999;
    $datestamp = int($datestamp);
    my $year  = int( $datestamp / 10000 ) - 1900;
    my $month = int( ( $datestamp / 100 ) % 100 ) - 1;
    my $day   = int( $datestamp % 100 );
    my $date  = POSIX::strftime( '%B %e, %Y', 0, 0, 0, $day, $month, $year );
    $date =~ s/  / /g;

    my $subj_str =
        getconf($CNF_ORG)
      . ( $report->get_flag("amflush_run") ? " AMFLUSH" : " AMANDA" )
      . " MAIL REPORT FOR "
      . $date;

    my $cfg_mailer = getconf($CNF_MAILER);

    my @cmd = ("$cfg_mailer", "-s", $subj_str, $mailto);
    debug("invoking mail app: " . join(" ", @cmd));

    my $pid = open3( my $fh, undef, undef, @cmd)
      or error("cannot start $cfg_mailer: $!", 1);
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

    # TODO: modularize these better
    if ($reportspec->[0] eq 'xml') {
	print $fh $report->xml_output();
    } elsif ($reportspec->[0] eq 'human') {
	my $hr =
	  Amanda::Report::human->new( $report, $fh, $config_name, $opt_logfname );
	$hr->print_human_amreport();
    } elsif ($reportspec->[0] eq 'postscript') {
	use Amanda::Report::postscript;
	my $rep =
	  Amanda::Report::postscript->new( $report, $config_name, $opt_logfname );
	$rep->write_report($fh);
    }

    close $fh;

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

Getopt::Long::Configure(qw/bundling/);
GetOptions(
    "i"   => \$opt_nomail,
    opt_set_var("M", \$opt_mailto),
    opt_set_var("f", \$opt_filename),
    opt_set_var("l", \$opt_logfname),
    opt_set_var("p", \$opt_psfname),
    "o=s" => sub { add_config_override_opt( $config_overrides, $_[1] ); },
    "xml" => sub { $opt_xml = 1 },
) or usage();

$opt_logfname = Amanda::Util::get_original_cwd() . "/" . $opt_logfname
	if defined $opt_logfname and $opt_logfname !~ /^\//;

usage() unless ( scalar(@ARGV) == 1 );

$config_name = shift @ARGV;
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

# shim for installchecks
$Amanda::Constants::LPR = $ENV{'INSTALLCHECK_MOCK_PRINTER'}
    if exists $ENV{'INSTALLCHECK_MOCK_PRINTER'};

# Process the options and decide what outputs we will produce
my @outputs = calculate_outputs();
if (!@outputs) {
    print "no output specified, nothing to do\n";
    exit(0);
}

# read the tapelist
my $tl_file = config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist::read_tapelist($tl_file);

## Parse the report & set output

my $logfile = $opt_logfname
           || config_dir_relative( getconf($CNF_LOGDIR) ) . "/log";
my $historical = defined $opt_logfname;

debug("using logfile: $logfile");
$report = Amanda::Report->new($logfile, $historical);
my $exit_status = $report->get_flag("exit_status");

## Output

for my $output (@outputs) {
    run_output($output);
}

Amanda::Util::finish_application();
exit $exit_status;
