# Unit tests for Amanda::Logfile

use Test::More tests => 39;
use Installdirs;

use lib "@amperldir@";
use Amanda::Logfile qw(:logtype_t :program_t open_logfile get_logline close_logfile);

# TODO: test *writing* logs (this only reads them)

# write a logfile and return the filename
sub write_logfile {
    my ($contents) = @_;
    my $filename = "$AMANDA_TMPDIR/Amanda_Logfile_test.log";

    open my $logfile, ">", $filename or die("Could not create temporary log file");
    print $logfile $contents;
    close $logfile;

    return $filename;
}

my $logfile;
my $logdata;

##
# Test out the constant functions

is(logtype_t_to_string($L_MARKER), "L_MARKER", "logtype_t_to_string works");
is(program_t_to_string($P_DRIVER), "P_DRIVER", "program_t_to_string works");

##
# Test a simple logfile

$logdata = <<END;
START planner date 20071026183200
END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a simple logfile");
ok(get_logline($logfile), "can read START line");
is($Amanda::Logfile::curlog, $L_START, "Line recognized as L_START");
is($Amanda::Logfile::curprog, $P_PLANNER, "Line recognized as P_PLANNER");
is($Amanda::Logfile::curstr, "date 20071026183200", "remainder of string parsed properly");
ok(!get_logline($logfile), "no second line");
close_logfile($logfile);

##
# Test continuation lines

$logdata = <<END;
INFO chunker line1
  line2
END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a logfile containing conitinuation lines");
ok(get_logline($logfile), "can read INFO line");
is($Amanda::Logfile::curlog, $L_INFO, "Line recognized as L_INFO");
is($Amanda::Logfile::curprog, $P_CHUNKER, "Line recognized as P_CHUNKER");
is($Amanda::Logfile::curstr, "line1", "remainder of string parsed properly");
ok(get_logline($logfile), "can read continuation line");
is($Amanda::Logfile::curlog, $L_CONT, "Continuation line recognized as L_CONT");
is($Amanda::Logfile::curprog, $P_CHUNKER, "curprog still P_CHUNKER");
is($Amanda::Logfile::curstr, "line2", "continuation line string parsed properly");
ok(!get_logline($logfile), "no third line");
close_logfile($logfile);

##
# Test skipping blank lines

# (retain the two blank lines in the following:)
$logdata = <<END;

STATS taper foo

END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a logfile containing blank lines");
ok(get_logline($logfile), "can read first line");
is($Amanda::Logfile::curlog, $L_STATS, "Line recognized as L_STATS");
is($Amanda::Logfile::curprog, $P_TAPER, "Line recognized as P_TAPER");
is($Amanda::Logfile::curstr, "foo", "remainder of string parsed properly");
ok(!get_logline($logfile), "no second line");
close_logfile($logfile);

##
# Test BOGUS values and short lines

# (retain the two blank lines in the following:)
$logdata = <<END;
SOMETHINGWEIRD somerandomprog bar
MARKER amflush
MARKER amflush put something in curstr
PART
END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a logfile containing bogus entries");
ok(get_logline($logfile), "can read first line");
is($Amanda::Logfile::curlog, $L_BOGUS, "Line recognized as L_BOGUS");
is($Amanda::Logfile::curprog, $P_UNKNOWN, "Line recognized as P_UNKNOWN");
is($Amanda::Logfile::curstr, "bar", "remainder of string parsed properly");
ok(get_logline($logfile), "can read second (two-word) line");
is($Amanda::Logfile::curlog, $L_MARKER, "Line recognized as L_MARKER");
is($Amanda::Logfile::curprog, $P_AMFLUSH, "Line recognized as P_AMFLUSH");
is($Amanda::Logfile::curstr, "", "curstr is empty");
ok(get_logline($logfile), "can read third line");
ok(get_logline($logfile), "can read fourth (one-word) line");
is($Amanda::Logfile::curlog, $L_PART, "Line recognized as L_PART");
is($Amanda::Logfile::curprog, $P_UNKNOWN, "defaults to P_UNKNOWN when no program word");
is($Amanda::Logfile::curstr, "", "curstr is empty");
ok(!get_logline($logfile), "no second line");
close_logfile($logfile);

