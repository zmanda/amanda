#!@PERL@
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;
use Getopt::Long;

package Amanda::Application::ampgsql;
use base qw(Amanda::Application);
use Carp;
use File::Copy;
use File::Path;
use IO::Dir;
use IO::File;
use IPC::Open3;
use POSIX;
use POSIX qw( ceil );
use Sys::Hostname;
use Symbol;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative string_to_boolean );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants :encoding quote_string );
use Amanda::MainLoop qw( :GIOCondition );

my $_DATA_DIR_TAR = "data_dir.tar";
my $_ARCHIVE_DIR_TAR = "archive_dir.tar";
my $_WAL_FILE_PAT = qr/\w{24}/;

my $_DATA_DIR_RESTORE = "data";
my $_ARCHIVE_DIR_RESTORE = "archive";

sub new {
    my $class = shift @_;
    my $args = shift @_;
    my $self = $class->SUPER::new($args->{'config'});
    $self->{'args'} = $args;
    $self->{'label-prefix'} = 'amanda';
    $self->{'runtar'}  = "$Amanda::Paths::amlibexecdir/runtar";

    # default arguments (application properties)
    $self->{'args'}->{'statedir'} ||= $Amanda::Paths::GNUTAR_LISTED_INCREMENTAL_DIR;
    $self->{'args'}->{'tmpdir'} ||= $AMANDA_TMPDIR;
    # XXX: when using runtar, this is not actually honored.
    # So, this only works for restore at the moment
    $self->{'args'}->{'gnutar-path'} ||= $Amanda::Constants::GNUTAR;

    if (!defined $self->{'args'}->{'disk'}) {
	$self->{'args'}->{'disk'} = $self->{'args'}->{'device'};
    }
    if (!defined $self->{'args'}->{'device'}) {
	$self->{'args'}->{'device'} = $self->{'args'}->{'disk'};
    }
    # default properties
    $self->{'props'} = {
        'pg-db' => 'template1',
        'pg-cleanupwal' => 'yes',
	'pg-max-wal-wait' => 60,
    };

    my @PROP_NAMES = qw(pg-host pg-port pg-db pg-user pg-password pg-passfile
			psql-path pg-datadir pg-archivedir pg-cleanupwal
			pg-max-wal-wait);

    # config is loaded by Amanda::Application (and Amanda::Script_App)
    my $conf_props = getconf($CNF_PROPERTY);
    # check for properties like 'pg-host'
    foreach my $pname (@PROP_NAMES) {
        if ($conf_props->{$pname}) {
            debug("More than one value for $pname. Using the first.")
                if scalar(@{$conf_props->{$pname}->{'values'}}) > 1;
            $self->{'props'}->{$pname} = $conf_props->{$pname}->{'values'}->[0];
        }
    }

    # check for properties like 'foo-pg-host' where the diskname is 'foo'
    if ($self->{'args'}->{'disk'}) {
        foreach my $pname (@PROP_NAMES) {
            my $tmp = "$self->{'args'}->{'disk'}-$pname";
            if ($conf_props->{$tmp}) {
                debug("More than one value for $tmp. Using the first.")
                    if scalar(@{$conf_props->{$tmp}->{'values'}}) > 1;
                $self->{'props'}->{$pname} = $conf_props->{$tmp}->{'values'}->[0];
            }
        }
    }

    # overwrite with dumptype properties if they are set.
    foreach my $pname (@PROP_NAMES) {
	my $pdumpname = $pname;
	$pdumpname =~ s/^pg-//g;
	$self->{'props'}->{$pname} = $self->{'args'}->{$pdumpname}
				 if defined $self->{'args'}->{$pdumpname};
debug("prop $pname set from dumpname $pdumpname: $self->{'args'}->{$pdumpname}")
if defined $self->{'args'}->{$pdumpname};
    }

    unless ($self->{'props'}->{'psql-path'}) {
        foreach my $pre (split(/:/, $ENV{PATH})) {
            my $psql = "$pre/psql";
            if (-x $psql) {
                $self->{'props'}{'psql-path'} = $psql;
                last;
            }
        }
    }

    foreach my $aname (keys %{$self->{'args'}}) {
        if (defined($self->{'args'}->{$aname})) {
            debug("app property: $aname $self->{'args'}->{$aname}");
        } else {
            debug("app property: $aname (undef)");
        }
    }

    foreach my $pname (keys %{$self->{'props'}}) {
        if (defined($self->{'props'}->{$pname})) {
            debug("client property: $pname $self->{'props'}->{$pname}");
        } else {
            debug("client property: $pname (undef)");
        }
    }

    if (!exists $self->{'props'}->{'pg-datadir'}) {
	$self->{'props'}->{'pg-datadir'} =  $self->{'args'}->{'device'};
    }

    return $self;
}

sub command_support {
   my $self = shift;

   print <<EOF;
CONFIG YES
HOST YES
DISK YES
MAX-LEVEL 9
INDEX-LINE YES
INDEX-XML NO
MESSAGE-LINE YES
MESSAGE-XML NO
RECORD YES
COLLECTION NO
CLIENT-ESTIMATE YES
MULTI-ESTIMATE NO
CALCSIZE NO
EOF
}

sub _check {
    my ($desc, $succ_suf, $err_suf, $check, @check_args) = @_;
    my $ret = $check->(@check_args);
    my $msg = $ret? "OK $desc $succ_suf" :  "ERROR $desc $err_suf";
    debug($msg);
    print "$msg\n";
    $ret;
}

sub _check_parent_dirs {
    my ($dir) = @_;
    my $ok = 1;
    my $is_abs = substr($dir, 0, 1) eq "/";
    _check("$dir is an absolute path?", "Yes", "No. It should start with '/'",
       sub {$is_abs});

    my @parts = split('/', $dir);
    pop @parts; # don't test the last part
    my $partial_path = '';
    for my $path_part (@parts) {
        $partial_path .= $path_part . (($partial_path || $is_abs)? '/' : '');
        $ok &&=
	    _check("$partial_path is executable?", "Yes", "No",
               sub {-x $_[0]}, $partial_path);
        $ok &&=
	    _check("$partial_path is a directory?", "Yes", "No",
               sub {-d $_[0]}, $partial_path);
    }
    $ok;
}

sub _ok_passfile_perms {
    my $passfile = shift @_;
    # libpq uses stat, so we use stat
    my @fstat = stat($passfile);
    return 0 unless @fstat;
    return 0 if 077 & $fstat[2];
    return -r $passfile;
}

sub _run_psql_command {
    my ($self, $cmd) = @_;

    # n.b. deprecated, passfile recommended for better security
    my $orig_pgpassword = $ENV{'PGPASSWORD'};
   $ENV{'PGPASSWORD'} = $self->{'props'}->{'pg-password'} if $self->{'props'}->{'pg-password'};
    # n.b. supported in 8.1+
    my $orig_pgpassfile = $ENV{'PGPASSFILE'};
    $ENV{'PGPASSFILE'} = $self->{'props'}->{'pg-passfile'} if $self->{'props'}->{'pg-passfile'};

    my @cmd = ($self->{'props'}->{'psql-path'});
    push @cmd, "-X";
    push @cmd, "-h", $self->{'props'}->{'pg-host'} if ($self->{'props'}->{'pg-host'});
    push @cmd, "-p", $self->{'props'}->{'pg-port'} if ($self->{'props'}->{'pg-port'});
    push @cmd, "-U", $self->{'props'}->{'pg-user'} if ($self->{'props'}->{'pg-user'});

    push @cmd, '--quiet', '--output', '/dev/null' if (!($cmd =~ /pg_xlogfile_name_offset/));
    push @cmd, '--command', $cmd, $self->{'props'}->{'pg-db'};
    debug("running " . join(" ", @cmd));

    my ($wtr, $rdr);
    my $err = Symbol::gensym;
    my $pid = open3($wtr, $rdr, $err, @cmd);
    close($wtr);

    my $file_to_close = 2;
    my $psql_stdout_src = Amanda::MainLoop::fd_source($rdr,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    my $psql_stderr_src = Amanda::MainLoop::fd_source($err,
						$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    $psql_stdout_src->set_callback(sub {
	my $line = <$rdr>;
	if (!defined $line) {
	    $file_to_close--;
	    $psql_stdout_src->remove();
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	    return;
	}
	chomp $line;
	return if $line =~ /^\s*$/;
	debug("psql stdout: $line");
	if ($cmd =~ /pg_xlogfile_name_offset/) {
	    return if $line =~ /file_name/;
	    return if $line =~ /------/;
	    return if $line =~ /\(1 row\)/;
	    if ($line =~ /^ ($_WAL_FILE_PAT)/) {
		$self->{'switch_xlog_filename'} = $1;
		return;
	    }
	}
	if ($line =~ /NOTICE: pg_stop_backup complete, all required WAL segments have been archived/) {
	} else {
	    $self->print_to_server("psql stdout: $line",
				   $Amanda::Script_App::GOOD);
	}
    });
    $psql_stderr_src->set_callback(sub {
	my $line = <$err>;
	if (!defined $line) {
	    $file_to_close--;
	    $psql_stderr_src->remove();
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	    return;
	}
	chomp $line;
	debug("psql stderr: $line");
	if ($line =~ /NOTICE: pg_stop_backup complete, all required WAL segments have been archived/) {
	} elsif ($line =~ /could not connect to server/) {
	    $self->print_to_server("psql stderr: $line",
				   $Amanda::Script_App::ERROR);
	} else {
	    $self->print_to_server("psql stderr: $line",
				   $Amanda::Script_App::GOOD);
	}
    });

    close($wtr);
    Amanda::MainLoop::run();
    close($rdr);
    close($err);

    waitpid $pid, 0;
    my $status = $?;

    $ENV{'PGPASSWORD'} = $orig_pgpassword || '';
    $ENV{'PGPASSFILE'} = $orig_pgpassfile || '';

    return 0 == ($status >> 8)
}

sub command_selfcheck {
    my $self = shift;

   # set up to handle errors correctly
   $self->{'die_cb'} = sub {
       my ($msg) = @_;
       debug("$msg");
       print "$msg\n";
       exit(1);
   };

    $self->print_to_server("disk " . quote_string($self->{args}->{disk}));

    $self->print_to_server("ampgsql version " . $Amanda::Constants::VERSION,
			   $Amanda::Script_App::GOOD);

    for my $k (keys %{$self->{'args'}}) {
        print "OK application property: $k = $self->{'args'}->{$k}\n";
    }

    _check("GNUTAR-PATH $self->{'args'}->{'gnutar-path'}",
           "is executable", "is NOT executable",
           sub {-x $_[0]}, $self->{'args'}->{'gnutar-path'});
    _check("GNUTAR-PATH $self->{'args'}->{'gnutar-path'}",
           "is not a directory (okay)", "is a directory (it shouldn't be)",
           sub {!(-d $_[0])}, $self->{'args'}->{'gnutar-path'});
    _check_parent_dirs($self->{'args'}->{'gnutar-path'});

    _check("GNUTAR $Amanda::Constants::GNUTAR",
           "is executable", "is NOT executable",
           sub {-x $_[0]}, $Amanda::Constants::GNUTAR);
    _check("GNUTAR $Amanda::Constants::GNUTAR",
           "is not a directory (okay)", "is a directory (it shouldn't be)",
           sub {!(-d $_[0])}, $Amanda::Constants::GNUTAR);
    _check_parent_dirs($Amanda::Constants::GNUTAR);

    _check("TMPDIR '$self->{'args'}->{'tmpdir'}'",
           "is an acessible directory", "is NOT an acessible directory",
           sub {$_[0] && -d $_[0] && -r $_[0] && -w $_[0] && -x $_[0]},
           $self->{'args'}->{'tmpdir'});

    if (exists $self->{'props'}->{'pg-datadir'}) {
	_check("PG-DATADIR property is",
	       "same as diskdevice", "differrent than diskdevice",
	       sub { $_[0] eq $_[1] },
	       $self->{'props'}->{'pg-datadir'}, $self->{'args'}->{'device'});
    } else {
	$self->{'props'}->{'pg-datadir'} = $self->{'args'}->{'device'};
    }

    _check("PG-DATADIR property", "is set", "is NOT set",
	   sub { $_[0] }, $self->{'props'}->{'pg-datadir'});
       # note that the backup user need not be able ot read this dir

    _check("STATEDIR '$self->{'args'}->{'statedir'}'",
           "is an acessible directory", "is NOT an acessible directory",
           sub {$_[0] && -d $_[0] && -r $_[0] && -w $_[0] && -x $_[0]},
           $self->{'args'}->{'statedir'});
    _check_parent_dirs($self->{'args'}->{'statedir'});

    if ($self->{'args'}->{'device'}) {
	my $try_connect = 1;

        for my $k (keys %{$self->{'props'}}) {
            print "OK client property: $k = $self->{'props'}->{$k}\n";
        }

        if (_check("PG-ARCHIVEDIR property", "is set", "is NOT set",
               sub { $_[0] }, $self->{'props'}->{'pg-archivedir'})) {
	    _check("PG-ARCHIVEDIR $self->{'props'}->{'pg-archivedir'}",
		   "is a directory", "is NOT a directory",
		   sub {-d $_[0]}, $self->{'props'}->{'pg-archivedir'});
	    _check("PG-ARCHIVEDIR $self->{'props'}->{'pg-archivedir'}",
		   "is readable", "is NOT readable",
		   sub {-r $_[0]}, $self->{'props'}->{'pg-archivedir'});
	    _check("PG-ARCHIVEDIR $self->{'props'}->{'pg-archivedir'}",
		   "is executable", "is NOT executable",
		   sub {-x $_[0]}, $self->{'props'}->{'pg-archivedir'});
	    _check_parent_dirs($self->{'props'}->{'pg-archivedir'});
	}

	$try_connect &&=
	    _check("Are both PG-PASSFILE and PG-PASSWORD set?",
		   "No (okay)",
		   "Yes. Please set only one or the other",
		   sub {!($self->{'props'}->{'pg-passfile'} and
			  $self->{'props'}->{'pg-password'})});

        if ($self->{'props'}->{'pg-passfile'}) {
	    $try_connect &&=
		_check("PG-PASSFILE $self->{'props'}->{'pg-passfile'}",
                   "has correct permissions", "does not have correct permissions",
                   \&_ok_passfile_perms, $self->{'props'}->{'pg-passfile'});
	    $try_connect &&=
		_check_parent_dirs($self->{'props'}->{'pg-passfile'});
        }

        if (_check("PSQL-PATH property", "is set", "is NOT set and psql is not in \$PATH",
               sub { $_[0] }, $self->{'props'}->{'psql-path'})) {
	    $try_connect &&=
		_check("PSQL-PATH $self->{'props'}->{'psql-path'}",
		       "is executable", "is NOT executable",
		       sub {-x $_[0]}, $self->{'props'}->{'psql-path'});
	    $try_connect &&=
		_check("PSQL-PATH $self->{'props'}->{'psql-path'}",
		       "is not a directory (okay)", "is a directory (it shouldn't be)",
		       sub {!(-d $_[0])}, $self->{'props'}->{'psql-path'});
	    $try_connect &&=
		_check_parent_dirs($self->{'props'}->{'psql-path'});
	} else {
	    $try_connect = 0;
	}

	if ($try_connect) {
	    my @pv = `$self->{'props'}->{'psql-path'} --version`;
	    if ($? >> 8 == 0) {
		$pv[0] =~ /^[^0-9]*([0-9.]*)[^0-9]*$/;
		my $pv = $1;
		$self->print_to_server("ampgsql psql-version $pv",
				       $Amanda::Script_App::GOOD);
	    } else {
		$self->print_to_server(
		"[Can't get " . $self->{'props'}->{'psql-path'} . " version]\n",
		$Amanda::Script_App::ERROR);
	    }
	}

	if ($try_connect) {
	    $try_connect &&=
		_check("Connecting to database server", "succeeded", "failed",
		   \&_run_psql_command, $self, '');
	}

	{
	    my @gv = `$self->{'args'}->{'gnutar-path'} --version`;
	    if ($? >> 8 == 0) {
		$gv[0] =~ /^[^0-9]*([0-9.]*)[^0-9]*$/;
		my $gv = $1;
		$self->print_to_server("ampgsql gtar-version $gv",
				       $Amanda::Script_App::GOOD);
	    } else {
		$self->print_to_server(
		"[Can't get " . $self->{'props'}->{'gnutar-path'} . " version]\n",
		$Amanda::Script_App::ERROR);
	    }
	}
    }
}

sub _state_filename {
    my ($self, $level) = @_;

    my @parts = ("ampgsql", hexencode($self->{'args'}->{'host'}), hexencode($self->{'args'}->{'disk'}), $level);
    my $statefile = $self->{'args'}->{'statedir'} . '/'  . join("-", @parts);
    debug("statefile: $statefile");
    return $statefile;
}

sub _write_state_file {
    my ($self, $end_wal) = @_;

    my $h = new IO::File(_state_filename($self, $self->{'args'}->{'level'}), "w");
    $h or return undef;

    debug("writing state file");
    $h->print("VERSION: 0\n");
    $h->print("LAST WAL FILE: $end_wal\n");
    $h->close();
    1;
}

sub _get_prev_state {
    my $self = shift @_;
    my $initial_level = shift;
    $initial_level = $self->{'args'}->{'level'} - 1 if !defined $initial_level;

    my $end_wal;
    for (my $level = $initial_level; $level >= 0; $level--) {
        my $fn = _state_filename($self, $level);
        debug("reading state file: $fn");
        my $h = new IO::File($fn, "r");
        next unless $h;
        while (my $l = <$h>) {
	    chomp $l;
	    debug("  $l");
            if ($l =~ /^VERSION: (\d+)/) {
                unless (0 == $1) {
                    $end_wal = undef;
                    last;
                }
            } elsif ($l =~ /^LAST WAL FILE: ($_WAL_FILE_PAT)/) {
                $end_wal = $1;
            }
        }
        $h->close();
        last if $end_wal;
    }
    $end_wal;
}

sub _make_dummy_dir_base {
    my ($self) = @_;

   my $dummydir = "$self->{'args'}->{'tmpdir'}/ampgsql-dummy-$$";
   mkpath("$dummydir/$_ARCHIVE_DIR_RESTORE");

   return $dummydir;
}

sub _make_dummy_dir {
    my ($self) = @_;

   my $dummydir = "$self->{'args'}->{'tmpdir'}/ampgsql-dummy-$$";
   mkpath($dummydir);
   open(my $fh, ">$dummydir/empty-incremental");
   close($fh);

   return $dummydir;
}

sub _run_tar_totals {
    my ($self, @other_args) = @_;

    my @cmd;
    @cmd = ($self->{'runtar'}, $self->{'args'}->{'config'},
        $Amanda::Constants::GNUTAR, '--create', '--totals', @other_args);
    debug("running: " . join(" ", @cmd));

    local (*TAR_IN, *TAR_OUT, *TAR_ERR);
    open TAR_OUT, ">&", $self->{'out_h'};
    my $pid;
    eval { $pid = open3(\*TAR_IN, ">&TAR_OUT", \*TAR_ERR, @cmd); 1;} or
        $self->{'die_cb'}->("failed to run tar. error was $@");
    close(TAR_IN);

    # read stderr
    my $size;
    while (my $l = <TAR_ERR>) {
        if ($l =~ /^Total bytes written: (\d+)/) {
            $size = $1;
        } else {
	    chomp $l;
	    $self->print_to_server($l, $Amanda::Script_App::ERROR);
	    debug("TAR_ERR: $l");
	}
    }
    waitpid($pid, 0);
    my $status = POSIX::WEXITSTATUS($?);

    close(TAR_ERR);
    debug("size of generated tar file: " . (defined($size)? $size : "undef"));
    if ($status == 1) {
	debug("ignored non-fatal tar exit status of 1");
    } elsif ($status) {
	$self->{'die_cb'}->("Tar failed (exit status $status)");
    }
    $size;
}

sub command_estimate {
   my $self = shift;

   $self->{'out_h'} = new IO::File("/dev/null", "w");
   $self->{'out_h'} or die("Could not open /dev/null");
   $self->{'index_h'} = new IO::File("/dev/null", "w");
   $self->{'index_h'} or die("Could not open /dev/null");

   $self->{'done_cb'} = sub {
       my $size = shift @_;
       debug("done. size $size");
       $size = ceil($size/1024);
       debug("sending $self->{'args'}->{'level'} $size 1");
       print("$self->{'args'}->{'level'} $size 1\n");
   };
   $self->{'die_cb'} = sub {
       my $msg = shift @_;
       debug("$msg");
       $self->{'done_cb'}->(-1);
       die($msg);
   };
   $self->{'state_cb'} = sub {
       # do nothing
   };
   $self->{'unlink_cb'} = sub {
       # do nothing
   };

   if ($self->{'args'}->{'level'} > 0) {
       _incr_backup($self);
   } else {
       _base_backup($self);
   }
}

sub _get_backup_info {
    my ($self, $label) = @_;

   my ($fname, $bfile, $start_wal, $end_wal);
   # wait up to 60s for the .backup file to be copied
   for (my $count = 0; $count < 60; $count++) {
       my $adir = new IO::Dir($self->{'props'}->{'pg-archivedir'});
       $adir or $self->{'die_cb'}->("Could not open archive WAL directory");
       while (defined($fname = $adir->read())) {
           if ($fname =~ /\.backup$/) {
               my $blabel;
               # use runtar to read a protected file, then grep the resulting tarfile (yes,
	       # this works!)
               local *TAROUT;
               my $conf = $self->{'args'}->{'config'} || 'NOCONFIG';
               my $cmd = "$self->{'runtar'} $conf $Amanda::Constants::GNUTAR --create --file - --directory $self->{'props'}->{'pg-archivedir'} $fname | $Amanda::Constants::GNUTAR --file - --extract --to-stdout";
               debug("running: $cmd");
               open(TAROUT, "$cmd |");
               my ($start, $end, $lab);
               while (my $l = <TAROUT>) {
                   chomp($l);
                   if ($l =~ /^START WAL LOCATION:.*?\(file ($_WAL_FILE_PAT)\)$/) {
                       $start = $1;
                   } elsif($l =~ /^STOP WAL LOCATION:.*?\(file ($_WAL_FILE_PAT)\)$/) {
                       $end = $1;
                   } elsif ($l =~ /^LABEL: (.*)$/) {
                       $lab = $1;
                   }
               }
	       close TAROUT;
               if ($lab and $lab eq $label) {
                   $start_wal = $start;
                   $end_wal = $end;
                   $bfile = $fname;
                   last;
               } else {
		   debug("logfile had non-matching label");
	       }
           }
       }
       $adir->close();
       if ($start_wal and $end_wal) {
	   debug("$bfile named WALs $start_wal .. $end_wal");

           # try to cleanup a bit, although this may fail and that's ok
	   my $filename = "$self->{'props'}->{'pg-archivedir'}/$bfile";
           if (unlink($filename) == 0) {
               debug("Failed to unlink '$filename': $!");
               $self->print_to_server("Failed to unlink '$filename': $!",
				      $Amanda::Script_App::ERROR);
	   }
           last;
       }
       sleep(1);
   }

   ($start_wal, $end_wal);
}

# return the postgres version as an integer
sub _get_pg_version {
    my $self = shift;

    local *VERSOUT;

    my @cmd = ($self->{'props'}->{'psql-path'});
    push @cmd, "-X";
    push @cmd, "--version";
    my $pid = open3('>&STDIN', \*VERSOUT, '>&STDERR', @cmd)
	or $self->{'die_cb'}->("could not open psql to determine version");
    my @lines = <VERSOUT>;
    waitpid($pid, 0);
    $self->{'die_cb'}->("could not run psql to determine version") if (($? >> 8) != 0);

    my ($maj, $min, $pat) = ($lines[0] =~ / ([0-9]+)\.([0-9]+)\.([0-9]+)$/);
    return $maj * 10000 + $min * 100 + $pat;
}

# create a large table and immediately drop it; this can help to push a WAL file out
sub _write_garbage_to_db {
    my $self = shift;

    debug("writing garbage to database to force a WAL archive");

    # note: lest ye be tempted to add "if exists" to the drop table here, note that
    # the clause was not supported in 8.1
    _run_psql_command($self, <<EOF) or
CREATE TABLE _ampgsql_garbage AS SELECT * FROM GENERATE_SERIES(1, 500000);
DROP TABLE _ampgsql_garbage;
EOF
        $self->{'die_cb'}->("Failed to create or drop table _ampgsql_garbage");
}

# wait up to pg-max-wal-wait seconds for a WAL file to appear
sub _wait_for_wal {
    my ($self, $wal) = @_;
    my $pg_version = $self->_get_pg_version();

    my $archive_dir = $self->{'props'}->{'pg-archivedir'};
    my $maxwait = 0+$self->{'props'}->{'pg-max-wal-wait'};

    if ($maxwait) {
	debug("waiting $maxwait s for WAL $wal to be archived..");
    } else {
	debug("waiting forever for WAL $wal to be archived..");
    }

    my $count = 0; # try at least 4 cycles
    my $stoptime = time() + $maxwait;
    while ($maxwait == 0 || time < $stoptime || $count++ < 4) {
	if (-f "$archive_dir/$wal") {
	    sleep(1);
	    return;
	}

	# for versions 8.0 or 8.1, the only way to "force" a WAL archive is to write
	# garbage to the database.
	if ($pg_version < 80200) {
	    $self->_write_garbage_to_db();
	} else {
	    sleep(1);
	}
    }

    $self->{'die_cb'}->("WAL file $wal was not archived in $maxwait seconds");
}

sub _base_backup {
   my ($self) = @_;

   debug("running _base_backup");

   my $label = "$self->{'label-prefix'}-" . time();

   -d $self->{'props'}->{'pg-archivedir'} or
	die("WAL file archive directory does not exist (or is not a directory)");

   if ($self->{'action'} eq 'backup') {
       _run_psql_command($self, "SELECT pg_start_backup('$label')") or
           $self->{'die_cb'}->("Failed to call pg_start_backup");
   }

   # tar data dir, using symlink to prefix
   # XXX: tablespaces and their symlinks?
   # See: http://www.postgresql.org/docs/8.0/static/manage-ag-tablespaces.html
   my $old_die_cb = $self->{'die_cb'};
   $self->{'die_cb'} = sub {
       my $msg = shift @_;
       if ($self->{'action'} eq 'backup') {
           unless(_run_psql_command($self, "SELECT pg_stop_backup()")) {
               $msg .= " and failed to call pg_stop_backup";
	   }
       }
       $old_die_cb->($msg);
   };
   my $size = _run_tar_totals($self, '--file', "-",
       '--directory', $self->{'props'}->{'pg-datadir'},
       '--exclude', 'postmaster.pid',
       '--exclude', 'pg_xlog/*', # contains WAL files; will be handled below
       '--transform', "s,^,$_DATA_DIR_RESTORE/,S",
       ".");
   $self->{'die_cb'} = $old_die_cb;

   if ($self->{'action'} eq 'backup') {
       unless (_run_psql_command($self, "SELECT pg_stop_backup()")) {
           $self->{'die_cb'}->("Failed to call pg_stop_backup");
       }
   }

   # determine WAL files and append and create their tar file
   my $start_wal;
   my $end_wal;

   if ($self->{'action'} eq 'backup') {
	($start_wal, $end_wal)  = _get_backup_info($self, $label);
	($start_wal and $end_wal)
		or $self->{'die_cb'}->("A .backup file was never found in the archive "
				    . "dir $self->{'props'}->{'pg-archivedir'}");
	$self->_wait_for_wal($end_wal);
   } else {
	$start_wal = undef;
	$end_wal = _get_prev_state($self, 0);
   }

   # now grab all of the WAL files, *inclusive* of $start_wal
   my @wal_files;
   my $adir = new IO::Dir($self->{'props'}->{'pg-archivedir'});
   while (defined(my $fname = $adir->read())) {
       if ($fname =~ /^$_WAL_FILE_PAT$/) {
           if (!defined $end_wal ||
	       (!defined $start_wal and ($fname le $end_wal)) ||
	       (defined $start_wal and ($fname ge $start_wal) and
		($fname le $end_wal))) {
               push @wal_files, $fname;
               debug("will store: $fname");
           } elsif (defined $start_wal and $fname lt $start_wal) {
               $self->{'unlink_cb'}->("$self->{'props'}->{'pg-archivedir'}/$fname");
           }
       }
   }
   $adir->close();

   if (@wal_files) {
       $size += _run_tar_totals($self, '--file', "-",
	   '--directory', $self->{'props'}->{'pg-archivedir'},
	   '--transform', "s,^,$_ARCHIVE_DIR_RESTORE/,S",
	   @wal_files);
   } else {
       my $dummydir = $self->_make_dummy_dir_base();
       $self->{'done_cb'}->(_run_tar_totals($self, '--file', '-',
           '--directory', $dummydir, "$_ARCHIVE_DIR_RESTORE"));
       rmtree($dummydir);
   }

   $self->{'state_cb'}->($self, $end_wal);

   $self->{'done_cb'}->($size);
}

sub _incr_backup {
   my ($self) = @_;

   debug("running _incr_backup");

   if ($self->{'action'} eq 'backup') {
      _run_psql_command($self, "SELECT file_name from pg_xlogfile_name_offset(pg_switch_xlog())");
      if (defined($self->{'switch_xlog_filename'})) {
	 $self->_wait_for_wal($self->{'switch_xlog_filename'});
      }
   }

   my $end_wal = _get_prev_state($self);
   if ($end_wal) {
       debug("previously ended at: $end_wal");
   } else {
       debug("no previous state found!");
       return _base_backup(@_);
   }

   my $adir = new IO::Dir($self->{'props'}->{'pg-archivedir'});
   $adir or $self->{'die_cb'}->("Could not open archive WAL directory");
   my $max_wal = "";
   my ($fname, @wal_files);
   while (defined($fname = $adir->read())) {
       if (($fname =~ /^$_WAL_FILE_PAT$/) and ($fname gt $end_wal)) {
           $max_wal = $fname if $fname gt $max_wal;
           push @wal_files, $fname;
           debug("will store: $fname");
       }
   }

   $self->{'state_cb'}->($self, $max_wal ? $max_wal : $end_wal);

   if (@wal_files) {
       $self->{'done_cb'}->(_run_tar_totals($self, '--file', '-',
           '--directory', $self->{'props'}->{'pg-archivedir'}, @wal_files));
   } else {
       my $dummydir = $self->_make_dummy_dir();
       $self->{'done_cb'}->(_run_tar_totals($self, '--file', '-',
           '--directory', $dummydir, "empty-incremental"));
       rmtree($dummydir);
   }
}

sub command_backup {
   my $self = shift;

   $self->{'out_h'} = IO::Handle->new_from_fd(1, 'w');
   $self->{'out_h'} or die("Could not open data fd");
   my $msg_fd = IO::Handle->new_from_fd(3, 'w');
   $msg_fd or die("Could not open message fd");
   $self->{'index_h'} = IO::Handle->new_from_fd(4, 'w');
   $self->{'index_h'} or die("Could not open index fd");

   $self->{'done_cb'} = sub {
       my $size = shift @_;
       debug("done. size $size");
       $size = ceil($size/1024);
       debug("sending size $size");
       $msg_fd->print("sendbackup: size $size\n");

       $self->{'index_h'}->print("/PostgreSQL-Database-$self->{'args'}->{'level'}\n");

       $msg_fd->print("sendbackup: end\n");
   };
   $self->{'die_cb'} = sub {
       my $msg = shift @_;
       debug("$msg");
       $msg_fd->print("! $msg\n");
       $self->{'done_cb'}->(0);
       exit(1);
   };
   $self->{'state_cb'} = sub {
       my ($self, $end_wal) = @_;
       _write_state_file($self, $end_wal) or $self->{'die_cb'}->("Failed to write state file");
   };
   my $cleanup_wal_val = $self->{'props'}->{'pg-cleanupwal'} || 'yes';
   my $cleanup_wal = string_to_boolean($cleanup_wal_val);
   if (!defined($cleanup_wal)) {
       $self->{'die_cb'}->("couldn't interpret PG-CLEANUPWAL value '$cleanup_wal_val' as a boolean");
   } elsif ($cleanup_wal) {
       $self->{'unlink_cb'} = sub {
           my $filename = shift @_;
	   debug("unlinking WAL file $filename");
           if (unlink($filename) == 0) {
               debug("Failed to unlink '$filename': $!");
               $self->print_to_server("Failed to unlink '$filename': $!",
                                      $Amanda::Script_App::ERROR);
           }
       };
   } else {
       $self->{'unlink_cb'} = sub {
           # do nothing
       };
   }

   if ($self->{'args'}->{'level'} > 0) {
       _incr_backup($self, \*STDOUT);
   } else {
       _base_backup($self, \*STDOUT);
   }
}

sub command_restore {
    my $self = shift;

    chdir(Amanda::Util::get_original_cwd());
    if (defined $self->{'args'}->{directory}) {
	if (!-d $self->{'args'}->{directory}) {
	    $self->print_to_server_and_die("Directory $self->{directory}: $!",
					   $Amanda::Script_App::ERROR);
	}
	if (!-w $self->{'args'}->{directory}) {
	    $self->print_to_server_and_die("Directory $self->{directory}: $!",
					   $Amanda::Script_App::ERROR);
	}
	chdir($self->{'args'}->{directory});
    }
    my $cur_dir = POSIX::getcwd();

    if (!-d $_ARCHIVE_DIR_RESTORE) {
	mkdir($_ARCHIVE_DIR_RESTORE) or die("could not create archive WAL directory: $!");
    }
    my $status;
    if ($self->{'args'}->{'level'} > 0) {
	debug("extracting incremental backup to $cur_dir/$_ARCHIVE_DIR_RESTORE");
	$status = system($self->{'args'}->{'gnutar-path'},
		'--extract',
		'--file', '-',
		'--ignore-zeros',
		'--exclude', 'empty-incremental',
		'--directory', $_ARCHIVE_DIR_RESTORE) >> 8;
	(0 == $status) or die("Failed to extract level $self->{'args'}->{'level'} backup (exit status: $status)");
    } else {
	debug("extracting base of full backup to $cur_dir/$_DATA_DIR_RESTORE");
	debug("extracting archive dir to $cur_dir/$_ARCHIVE_DIR_RESTORE");
	if (!-d $_DATA_DIR_RESTORE) {
	    mkdir($_DATA_DIR_RESTORE) or die("could not create archive WAL directory: $!");
	}
	my @cmd = ($self->{'args'}->{'gnutar-path'}, '--extract',
		'--file', '-',
		'--ignore-zero',
		'--transform', "s,^DATA/,$_DATA_DIR_RESTORE/,S",
		'--transform', "s,^WAL/,$_ARCHIVE_DIR_RESTORE/,S");
	debug("run: " . join ' ',@cmd);
	$status = system(@cmd) >> 8;
	(0 == $status) or die("Failed to extract base backup (exit status: $status)");

	if (-f $_ARCHIVE_DIR_TAR) {
	    debug("extracting archive dir to $cur_dir/$_ARCHIVE_DIR_RESTORE");
	    my @cmd = ($self->{'args'}->{'gnutar-path'}, '--extract',
		'--exclude', 'empty-incremental',
		'--file', $_ARCHIVE_DIR_TAR, '--directory',
		$_ARCHIVE_DIR_RESTORE);
	    debug("run: " . join ' ',@cmd);
	    $status = system(@cmd) >> 8;
	    (0 == $status) or die("Failed to extract archived WAL files from base backup (exit status: $status)");
	    if (unlink($_ARCHIVE_DIR_TAR) == 0) {
		debug("Failed to unlink '$_ARCHIVE_DIR_TAR': $!");
		$self->print_to_server(
				"Failed to unlink '$_ARCHIVE_DIR_TAR': $!",
				$Amanda::Script_App::ERROR);
	    }
	}

	if (-f $_DATA_DIR_TAR) {
	    debug("extracting data dir to $cur_dir/$_DATA_DIR_RESTORE");
	    my @cmd = ($self->{'args'}->{'gnutar-path'}, '--extract',
		'--file', $_DATA_DIR_TAR,
		'--directory', $_DATA_DIR_RESTORE);
	    debug("run: " . join ' ',@cmd);
	    $status = system(@cmd) >> 8;
	    (0 == $status) or die("Failed to extract data directory from base backup (exit status: $status)");
	    if (unlink($_DATA_DIR_TAR) == 0) {
		debug("Failed to unlink '$_DATA_DIR_TAR': $!");
		$self->print_to_server("Failed to unlink '$_DATA_DIR_TAR': $!",
				$Amanda::Script_App::ERROR);
	    }
	}
    }
}

sub command_validate {
   my $self = shift;

   # set up to handle errors correctly
   $self->{'die_cb'} = sub {
       my ($msg) = @_;
       debug("$msg");
       print "$msg\n";
       exit(1);
   };

   if (!defined($self->{'args'}->{'gnutar-path'}) ||
       !-x $self->{'args'}->{'gnutar-path'}) {
      return $self->default_validate();
   }

   my(@cmd) = ($self->{'args'}->{'gnutar-path'}, "--ignore-zeros", "-tf", "-");
   debug("cmd:" . join(" ", @cmd));
   my $pid = open3('>&STDIN', '>&STDOUT', '>&STDERR', @cmd) ||
      $self->print_to_server_and_die("Unable to run @cmd",
				     $Amanda::Application::ERROR);
   waitpid $pid, 0;
   if ($? != 0){
       $self->print_to_server_and_die("$self->{gnutar} returned error",
				      $Amanda::Application::ERROR);
   }
   exit($self->{error_status});
}

package main;

sub usage {
    print <<EOF;
Usage: ampgsql <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --calcsize.
EOF
    exit(1);
}

my $opts = {};
my $opt_version;

GetOptions(
    $opts,
    'version' => \$opt_version,
    'config=s',
    'host=s',
    'disk=s',
    'device=s',
    'level=s',
    'index=s',
    'message=s',
    'collection=s',
    'record',
    'calcsize',
    'exclude-list=s@',
    'include-list=s@',
    'directory=s',
    # ampgsql-specific
    'statedir=s',
    'tmpdir=s',
    'gnutar-path=s',
    'cleanupwal=s',
    'archivedir=s',
    'db=s',
    'host=s',
    'max-wal-wait=s',
    'passfile=s',
    'port=s',
    'user=s',
    'psql-path=s'
) or usage();

if (defined $opt_version) {
    print "ampgsql-" . $Amanda::Constants::VERSION , "\n";
    exit(0);
}

my $application = Amanda::Application::ampgsql->new($opts);

$application->do($ARGV[0]);
