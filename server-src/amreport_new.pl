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

use Data::Dumper; # TODO: remove before committing
use Getopt::Long;
use IPC::Open3;
use Cwd qw( abs_path );
use FileHandle;
use POSIX;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Tapelist;

use Amanda::Report;
use Amanda::Report::human;


## Global Variables

my $no_mail = 0;
my ( $mailto, $outfname, $logfname, $psfname, $xmlout );
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

sub get_filefh
{

    $outfname = Amanda::Util::get_original_cwd() . "/$outfname"
      unless ( $outfname eq "-" || $outfname =~ m{^/} );

    Amanda::Debug::debug("Writing to file $outfname");
    ## takes no arguments
    local *fh = undef;
    open *fh, ">$outfname" or die "open: $!";
    return \*fh;
}

sub get_mailfh
{
    ## takes no arguments
    my ($mailer);
    local *mailfh = undef;

    unless ( $mailer = getconf($CNF_MAILER) ) {
        error(
"You must run amreport with '-f <output file>' because a mailer is not defined\n",
            1
        );
    }

    unless ($mailto) {
        error( "mail address has invalid characters", 1 );
    }

    Amanda::Debug::debug("Sending mail to $mailto");
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

    open3( *mailfh, undef, undef, "$mailer -s \"$subj_str\" \"$mailto\"" )
      or die "open3: $!";
    return \*mailfh;
}

sub get_psfh
{
    ## takes no arguments
    # TODO
    error( "error: get_psfh() has not been implemented yet.\n", 1 );
}

sub get_outfh
{
    ## takes no arguments
    my $outcount = defined($mailto) + defined($outfname) + defined($psfname);

    if ( $outcount > 1 ) {
        error(
"you cannot specify more than one output format (-[Mfp]) at a time\n",
            1
        );
    }

    return
        defined $outfname ? get_filefh()
      : defined $mailto   ? get_mailfh()
      : defined $psfname  ? get_psfh()
      : # default case: Use mail from config
      ( $mailto = getconf($CNF_MAILTO) ) && get_mailfh();
}


## Application initialization

Amanda::Util::setup_application("amreport", "server", $CONTEXT_CMDLINE);

my $config_overrides = new_config_overrides( scalar(@ARGV) + 1 );

Getopt::Long::Configure(qw/bundling/);
GetOptions(
    "i"   => \$no_mail,
    opt_set_var("M", \$mailto),
    opt_set_var("f", \$outfname),
    opt_set_var("l", \$logfname),
    opt_set_var("p", \$psfname),
    "o=s" => sub { add_config_override_opt( $config_overrides, $_[1] ); },
    "xml" => sub { $xmlout = 1 },
) or usage();

usage() unless ( scalar(@ARGV) == 1 );

if ( $no_mail and defined $mailto ) {
    error( "you cannot specify both -i & -M at the same time\n", 1 );
}

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

# read the tapelist
my $tl_file = config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist::read_tapelist($tl_file);

## Make sure options are valid before parsing

if ( defined $psfname && ( defined $mailto || $xmlout ) ) {
    error( "you may not specify printer output with -M or --xml.\n", 1 );
}

## apply summary-specific output configuration and set global
## variables based on configuration.

## Parse the report & set output

$logfname ||= config_dir_relative( getconf($CNF_LOGDIR) ) . "/log";
$report = Amanda::Report->new($logfname)
  or error("could not open $logfname: $!");

unless ( $outfh = get_outfh() ) {
    error( "error: $!", 1 );
}

## Output

if ($xmlout) {    #xml output

    print $outfh $report->xml_output();
    close $outfh;
    exit 0;

} elsif ($psfname) {    # postscript output
    error( "this has not been implemented yet.\n", 0 );

} else {                # default is human-readable output

    my $hr =
      Amanda::Report::human->new( $report, $outfh, $config_name, $logfname );
    $hr->print_human_amreport();
}

Amanda::Util::finish_application();

__END__
