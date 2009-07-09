#!@PERL@
# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Application::ampgsql;
use base qw(Amanda::Application);
use Carp;
use File::Copy;
use File::Path;
use IO::Dir;
use IO::File;
use IPC::Open3;
use POSIX qw( ceil );
use Sys::Hostname;
use Symbol;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants );

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

    # default properties
    $self->{'props'} = {
        'pg-db' => 'template1',
        'PG-CLEANUPWAL' => 'yes',
    };

    my @PROP_NAMES = qw(pg-host pg-port pg-db pg-user pg-password pg-passfile psql-path pg-datadir pg-archivedir pg-cleanupwal);

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
    # check for properties like 'foo-pg-host' where the device is 'foo'
    if ($self->{'args'}->{'device'}) {
        foreach my $pname (@PROP_NAMES) {
            my $tmp = "$self->{'args'}->{'device'}-$pname";
            if ($conf_props->{$tmp}) {
                debug("More than one value for $tmp. Using the first.")
                    if scalar(@{$conf_props->{$tmp}->{'values'}}) > 1;
                $self->{'props'}->{$pname} = $conf_props->{$tmp}->{'values'}->[0];
            }
        }
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
    my $is_abs = substr($dir, 0, 1) eq "/";
    _check("$dir is an absolute path?", "Yes", "No. It should start with '/'",
       sub {$is_abs});

    my @parts = split('/', $dir);
    pop @parts; # don't test the last part
    my $partial_path = '';
    for my $path_part (@parts) {
        $partial_path .= $path_part . (($partial_path || $is_abs)? '/' : '');
        _check("$partial_path is executable?", "Yes", "No",
               sub {-x $_[0]}, $partial_path);
        _check("$partial_path is a directory?", "Yes", "No",
               sub {-d $_[0]}, $partial_path);
    }
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
    push @cmd, "-h", $self->{'props'}->{'pg-host'} if ($self->{'props'}->{'pg-host'});
    push @cmd, "-p", $self->{'props'}->{'pg-port'} if ($self->{'props'}->{'pg-port'});
    push @cmd, "-U", $self->{'props'}->{'pg-user'} if ($self->{'props'}->{'pg-user'});

    push @cmd, '--quiet', '--output', '/dev/null', '--command', $cmd, $self->{'props'}->{'pg-db'};
    debug("running " . join(" ", @cmd));
    my $status = system(@cmd);

    $ENV{'PGPASSWORD'} = $orig_pgpassword || '';
    $ENV{'PGPASSFILE'} = $orig_pgpassfile || '';

    return 0 == ($status >>8)
}

sub command_selfcheck {
    my $self = shift;

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

    _check("TMPDIR $self->{'args'}->{'tmpdir'}",
           "is an acessible directory", "is NOT an acessible directory",
           sub {-d $_[0] && -r $_[0] && -w $_[0] && -x $_[0]},
           $self->{'args'}->{'tmpdir'});
    _check("STATEDIR $self->{'args'}->{'statedir'}",
           "is an acessible directory", "is NOT an acessible directory",
           sub {-d $_[0] && -r $_[0] && -w $_[0] && -x $_[0]},
           $self->{'args'}->{'statedir'});
    _check_parent_dirs($self->{'args'}->{'statedir'});

    if ($self->{'args'}->{'device'}) {
        for my $k (keys %{$self->{'props'}}) {
            print "OK client property: $k = $self->{'props'}->{$k}\n";
        }

        _check("PG-ARCHIVEDIR $self->{'props'}->{'pg-archivedir'}",
               "is a directory", "is NOT a directory",
               sub {-d $_[0]}, $self->{'props'}->{'pg-archivedir'});
        _check_parent_dirs($self->{'props'}->{'pg-archivedir'});
        _check("Are both PG-PASSFILE and PG-PASSWORD set?",
               "No (okay)",
               "Yes. Please set only one or the other",
               sub {!($self->{'props'}->{'pg-passfile'} and
                      $self->{'props'}->{'pg-password'})});
        if ($self->{'props'}->{'pg-passfile'}) {
            _check("PG-PASSFILE $self->{'props'}->{'pg-passfile'}",
                   "has correct permissions", "does not have correct permissions",
                   \&_ok_passfile_perms, $self->{'props'}->{'pg-passfile'});
            _check_parent_dirs($self->{'props'}->{'pg-passfile'});
        }
        _check("PSQL-PATH $self->{'props'}->{'psql-path'}",
               "is executable", "is NOT executable",
               sub {-x $_[0]}, $self->{'props'}->{'psql-path'});
        _check("PSQL-PATH $self->{'props'}->{'psql-path'}",
               "is not a directory (okay)", "is a directory (it shouldn't be)",
               sub {!(-d $_[0])}, $self->{'props'}->{'psql-path'});
        _check_parent_dirs($self->{'props'}->{'psql-path'});
        _check("Connecting to database server", "succeeded", "failed",
               \&_run_psql_command, $self, '');
        
        my $label = "$self->{'label-prefix'}-selfcheck-" . time();
        if (_check("Call pg_start_backup", "succeeded",
                   "failed (is another backup running?)",
                   \&_run_psql_command, $self, "SELECT pg_start_backup('$label')")
            and _check("Call pg_stop_backup", "succeeded", "failed",
                       \&_run_psql_command, $self, "SELECT pg_stop_backup()")) {

            _check("Get info from .backup file", "succeeded", "failed",
                   sub {my ($start, $end) = _get_backup_info($self, $label); $start and $end});
        }
    }
}

sub _encode {
    my $str = shift @_;
    return '' unless $str;
    $str =~ s/([^A-Za-z0-9])/sprintf("%%%02x", ord($1))/eg;
    $str;
}

sub _decode {
    my $str = shift @_;
    return '' unless $str;
    $str =~ s/%(..)/chr(hex($1))/eg;
    $str;
}

sub _state_filename {
    my ($self, $level) = @_;

    my @parts = ("ampgsql", _encode($self->{'args'}->{'host'}), _encode($self->{'args'}->{'disk'}), $level);
    $self->{'args'}->{'statedir'} . '/'  . join("-", @parts);
}

sub _write_state_file {
    my ($self, $end_wal) = @_;

    my $h = new IO::File(_state_filename($self, $self->{'args'}->{'level'}), "w");
    $h or return undef;

    $h->print("VERSION: 0\n");
    $h->print("LAST WAL FILE: $end_wal\n");
    $h->close();
    1;
}

sub _get_prev_state {
    my $self = shift @_;

    my $end_wal;
    for (my $level = $self->{'args'}->{'level'} - 1; $level >= 0; $level--) {
        my $fn = _state_filename($self, $level);
        debug("reading state file: $fn");
        my $h = new IO::File($fn, "r");
        next unless $h;
        while (my $l = <$h>) {
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

sub _run_tar_totals {
    my ($self, @other_args) = @_;

    my @cmd = ($self->{'runtar'}, $self->{'args'}->{'config'},
        $Amanda::Constants::GNUTAR, '--create', '--totals', @other_args);
    debug("running " . join(" ", @cmd));

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
        }
    }
    waitpid($pid, 0);
    my $status = POSIX::WEXITSTATUS($?);

    close(TAR_ERR);
    debug("size of generated tar file: " . (defined($size)? $size : "undef"));
    (0 == $status) or $self->{'die_cb'}->("Tar failed (exit status $status)");
    $size;
}

sub command_estimate {
   my $self = shift;

   $self->{'out_h'} = new IO::File("/dev/null", "w");
   $self->{'out_h'} or confess("Could not /dev/null");
   $self->{'index_h'} = new IO::File("/dev/null", "w");
   $self->{'index_h'} or confess("Could not /dev/null");

   $self->{'done_cb'} = sub {
       my $size = shift @_;
       debug("done. size $size");
       $size = ceil($size/1024);
       debug("sending $self->{'args'}->{'level'} $size 1");
       print("$self->{'args'}->{'level'} $size 1\n");
   };
   $self->{'die_cb'} = sub {
       my $msg = shift @_;
       $self->{'done_cb'}->(-1);
       confess($msg);
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
               # use runtar to read protected file
               local *TAROUT;
               my $conf = $self->{'args'}->{'config'} || 'NOCONFIG';
               my $cmd = "$self->{'runtar'} $conf $Amanda::Constants::GNUTAR --create --directory $self->{'props'}->{'pg-archivedir'} $fname | $Amanda::Constants::GNUTAR --extract --to-stdout";
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
               if ($lab and $lab eq $label) {
                   $start_wal = $start;
                   $end_wal = $end;
                   $bfile = $fname;
                   last;
               }
           }
       }
       $adir->close();
       if ($start_wal and $end_wal) {
           # try to cleanup a bit
           unlink("$self->{'props'}->{'pg-archivedir'}/$bfile");
           last;
       }
       sleep(1);
   }

   ($start_wal, $end_wal);
}

sub _base_backup {
   my ($self) = @_;

   debug("running _base_backup");

   my $label = "$self->{'label-prefix'}-" . time();
   my $tmp = "$self->{'args'}->{'tmpdir'}/$label";

   -d $self->{'props'}->{'pg-archivedir'} or confess("WAL file archive directory does not exist (or is not a directory)");

   # try to protect what we create
   my $old_umask = umask();
   umask(077);

   my $cleanup = sub {
       umask($old_umask);
       eval {rmtree($tmp); 1}
   };
   my $old_die = $self->{'die_cb'};
   $self->{'die_cb'} = sub {
       my $msg = shift @_;
       $cleanup->();
       $old_die->($msg);
   };
   eval {rmtree($tmp,{'keep_root' => 1}); 1} or $self->{'die_cb'}->("Failed to clear tmp directory: $@");
   eval {mkpath($tmp, 0, 0700); 1} or $self->{'die_cb'}->("Failed to create tmp directory: $@");

   _run_psql_command($self, "SELECT pg_start_backup('$label')") or
       $self->{'die_cb'}->("Failed to call pg_start_backup");

   # tar data dir, using symlink to prefix
   # XXX: tablespaces and their symlinks?
   # See: http://www.postgresql.org/docs/8.0/static/manage-ag-tablespaces.html
   my $old_die_cb = $self->{'die_cb'};
   $self->{'die_cb'} = sub {
       my $msg = shift @_;
       unless(_run_psql_command($self, "SELECT pg_stop_backup()")) {
           $msg .= " and failed to call pg_stop_backup";
       }
       $old_die_cb->($msg);
   };
   _run_tar_totals($self, '--file', "$tmp/$_DATA_DIR_TAR",
       '--directory', $self->{'props'}->{'pg-datadir'}, ".");
   $self->{'die_cb'} = $old_die_cb;

   unless (_run_psql_command($self, "SELECT pg_stop_backup()")) {
       $self->{'die_cb'}->("Failed to call pg_stop_backup");
   }

   # determine WAL files and append and create their tar file
   my ($start_wal, $end_wal) = _get_backup_info($self, $label);

   ($start_wal and $end_wal) or $self->{'die_cb'}->("A .backup file was never found in the archive dir $self->{'props'}->{'pg-archivedir'}");
   debug("WAL start: $start_wal end: $end_wal");

   my @wal_files;
   my $adir = new IO::Dir($self->{'props'}->{'pg-archivedir'});
   while (defined(my $fname = $adir->read())) {
       if ($fname =~ /^$_WAL_FILE_PAT$/) {
           if (($fname ge $start_wal) and ($fname le $end_wal)) {
               push @wal_files, $fname;
               debug("will store: $fname");
           } elsif ($fname lt $start_wal) {
               debug("will delete: $fname");
               $self->{'unlink_cb'}->("$self->{'props'}->{'PG-ARCHIVEDIR'}/$fname");
           }
       }
   }
   $adir->close();

   # create an empty archive for uniformity
   @wal_files or @wal_files = ('--files-from', '/dev/null');

   _run_tar_totals($self, '--file', "$tmp/$_ARCHIVE_DIR_TAR",
       '--directory', $self->{'props'}->{'pg-archivedir'}, @wal_files);

   # create the final tar file
   my $size = _run_tar_totals($self, '--directory', $tmp,
       $_ARCHIVE_DIR_TAR, $_DATA_DIR_TAR);

   $self->{'state_cb'}->($self, $end_wal);

   $cleanup->();
   $self->{'done_cb'}->($size);
}

sub _incr_backup {
   my ($self) = @_;

   debug("running _incr_backup");

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
       if (($fname =~ /^$_WAL_FILE_PAT$/) and ($fname ge $end_wal)) {
           $max_wal = $fname if $fname gt $max_wal;
           push @wal_files, $fname;
           debug("will store: $fname");
       }
   }

   $self->{'state_cb'}->($self, $max_wal ? $max_wal : $end_wal);

   if (@wal_files) {
       $self->{'done_cb'}->(_run_tar_totals($self,
           '--directory', $self->{'props'}->{'pg-archivedir'}, @wal_files));
   } else {
       $self->{'done_cb'}->(0);
   }
}

sub command_backup {
   my $self = shift;

   $self->{'out_h'} = IO::Handle->new_from_fd(1, 'w');
   $self->{'out_h'} or confess("Could not open data fd");
   my $msg_fd = IO::Handle->new_from_fd(3, 'w');
   $msg_fd or confess("Could not open message fd");
   $self->{'index_h'} = IO::Handle->new_from_fd(4, 'w');
   $self->{'index_h'} or confess("Could not open index fd");

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
       $msg_fd->print("! $msg\n");
       $self->{'done_cb'}->(0);
       confess($msg);
   };
   $self->{'state_cb'} = sub {
       my ($self, $end_wal) = @_;
       _write_state_file($self, $end_wal) or $self->{'die_cb'}->("Failed to write state file");
   };
   # simulate amanda.conf boolean style
   if ($self->{'props'}->{'pg-cleanupwal'} =~ /^(f|false|n|no|off)/i) {
       $self->{'unlink_cb'} = sub {
           # do nothing
       };
   } else {
       $self->{'unlink_cb'} = sub {
           my $filename = shift @_;
           unlink($filename);
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
   if (!-d $_ARCHIVE_DIR_RESTORE) {
       mkdir($_ARCHIVE_DIR_RESTORE) or confess("could not create archive WAL directory: $!");
   }
   my $status;
   if ($self->{'args'}->{'level'} > 0) {
       $status = system($self->{'args'}->{'gnutar-path'}, '--extract',
           '--directory', $_ARCHIVE_DIR_RESTORE) >> 8;
       (0 == $status) or confess("Failed to extract level $self->{'args'}->{'level'} backup (exit status: $status)");
   } else {
       if (!-d $_DATA_DIR_RESTORE) {
           mkdir($_DATA_DIR_RESTORE) or confess("could not create archive WAL directory: $!");
       }
       $status = system($self->{'args'}->{'gnutar-path'}, '--extract') >> 8;
       (0 == $status) or confess("Failed to extract base backup (exit status: $status)");

       $status = system($self->{'args'}->{'gnutar-path'}, '--extract',
          '--file', $_ARCHIVE_DIR_TAR, '--directory', $_ARCHIVE_DIR_RESTORE) >> 8;
       (0 == $status) or confess("Failed to extract archived WAL files from base backup (exit status: $status)");
       unlink($_ARCHIVE_DIR_TAR);

       $status = system($self->{'args'}->{'gnutar-path'}, '--extract',
          '--file', $_DATA_DIR_TAR, '--directory', $_DATA_DIR_RESTORE) >> 8;
       (0 == $status) or confess("Failed to extract data directory from base backup (exit status: $status)");
       unlink($_DATA_DIR_TAR);
   }
}

sub command_validate {
   my $self = shift;

   if (!defined($self->{'args'}->{'gnutar-path'}) ||
       !-x $self->{'args'}->{'gnutar-path'}) {
      return $self->default_validate();
   }

   my(@cmd) = ($self->{'args'}->{'gnutar-path'}, "-tf", "-");
   debug("cmd:" . join(" ", @cmd));
   my $pid = open3('>&STDIN', '>&STDOUT', '>&STDERR', @cmd) || $self->print_to_server_and_die( "validate", "Unable to run @cmd", $Amanda::Application::ERROR);
   waitpid $pid, 0;
   if ($? != 0){
       $self->print_to_server_and_die("validate", "$self->{gnutar} returned error", $Amanda::Application::ERROR);
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

GetOptions(
    $opts,
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
    # ampgsql-specific
    'statedir=s',
    'tmpdir=s',
    'gnutar-path=s',
) or usage();

my $application = Amanda::Application::ampgsql->new($opts);

$application->do($ARGV[0]);
