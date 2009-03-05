#!@PERL@
# Copyright (c) Zmanda Inc.  All Rights Reserved.
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
    $self->{'runtar'}  = "$Amanda::Paths::amlibexecdir/runtar$self->{'suf'}";

    # default arguments (application properties)
    $self->{'args'}->{'statedir'} ||= $Amanda::Paths::GNUTAR_LISTED_INCREMENTAL_DIR;
    $self->{'args'}->{'tmpdir'} ||= $AMANDA_TMPDIR;
    # XXX: when using runtar, this is not actually honored.
    # So, this only works for restore at the moment
    $self->{'args'}->{'gnutar-path'} ||= $Amanda::Constants::GNUTAR;

    # default properties
    $self->{'props'} = {
        'PG-DB' => 'template1',
        'PG-CLEANUPWAL' => 'yes',
    };

    my @PROP_NAMES = qw(PG-HOST PG-PORT PG-DB PG-USER PG-PASSWORD PG-PASSFILE PSQL-PATH PG-DATADIR PG-ARCHIVEDIR PG-CLEANUPWAL);

    # config is loaded by Amanda::Application (and Amanda::Script_App)
    my $conf_props = getconf($CNF_PROPERTY);
    # check for properties like 'PG-HOST'
    foreach my $pname (@PROP_NAMES) {
        if ($conf_props->{$pname}) {
            debug("More than one value for $pname. Using the first.")
                if scalar(@{$conf_props->{$pname}->{'values'}}) > 1;
            $self->{'props'}->{$pname} = $conf_props->{$pname}->{'values'}->[0];
        }
    }
    # check for properties like 'foo-PG-HOST' where the device is 'foo'
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

    unless ($self->{'props'}->{'PSQL-PATH'}) {
        foreach my $pre (split(/:/, $ENV{PATH})) {
            my $psql = "$pre/psql";
            if (-x $psql) {
                $self->{'props'}{'PSQL-PATH'} = $psql;
                last;
            }
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

sub command_selfcheck {
   my $self = shift;

   # XXX: TODO:
   # * GNU tar
   # * temp dir
   # * state dir
   # if have a diskname:
   # * data dir
   # * archive dir
   # * passfile perms
   # * try to connect
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
        my $h = new IO::File(_state_filename($self, $level-1), "r");
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
    my ($self, $out_h, @other_args) = @_;

    local (*TAR_IN, *TAR_OUT, *TAR_ERR);
    open TAR_OUT, ">&", $out_h;
    my $pid = open3(\*TAR_IN, ">&TAR_OUT", \*TAR_ERR,
        $self->{'runtar'}, $self->{'args'}->{'config'},
        $Amanda::Constants::GNUTAR, '--create', '--totals', @other_args);
    close(TAR_IN);
    waitpid($pid, 0);
    my $status = $? >> 8;
    0 == $status or $self->{'die_cb'}->("Tar failed (exit status $status)");

    my $size;
    while (my $tots = <TAR_ERR>) {
        if ($tots =~ /Total bytes written: (\d+)/) {
            $size = $1;
            last;
        }
    }
    close(TAR_ERR);
    $size;
}

sub command_estimate {
   my $self = shift;

   my $out_h = new IO::File("/dev/null", "w");

   $self->{'done_cb'} = sub {
       my $size = shift @_;
       $size /= 1024;
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
       _base_backup($self, $out_h);
   } else {
       _incr_backup($self, $out_h);
   }
}

sub _base_backup {
   my ($self, $out_h) = @_;

   my $label = "$self->{'label-prefix'}-" . time();
   my $tmp = "$self->{'args'}->{'tmpdir'}/$label";

   -d $self->{'props'}->{'PG-DATADIR'} or confess("Data directory does not exist (or is not a directory)");
   -d $self->{'props'}->{'PG-ARCHIVEDIR'} or confess("WAL file archive directory does not exist (or is not a directory)");

   # try to protect what we create
   my $old_umask = umask();
   umask(077);
   # n.b. deprecated, passfile recommended for better security
   my $orig_pgpassword = $ENV{'PGPASSWORD'};
   $ENV{'PGPASSWORD'} = $self->{'props'}->{'PG-PASSWORD'} if $self->{'props'}->{'PG-PASSWORD'};
   # n.b. supported in 8.1+
   my $orig_pgpassfile = $ENV{'PGPASSFILE'};
   $ENV{'PGPASSFILE'} = $self->{'props'}->{'PG-PASSFILE'} if $self->{'props'}->{'PG-PASSFILE'};

   my $cleanup = sub {
       $ENV{'PGPASSWORD'} = $orig_pgpassword;
       $ENV{'PGPASSFILE'} = $orig_pgpassfile;
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
   eval {mkpath($tmp, {'mode' => 0700}); 1} or $self->{'die_cb'}->("Failed to create tmp directory: $@");

   my @args = ();
   push @args, "-h", $self->{'props'}->{'PG-HOST'} if ($self->{'props'}->{'PG-HOST'});
   push @args, "-p", $self->{'props'}->{'PG-PORT'} if ($self->{'props'}->{'PG-PORT'});
   push @args, "-U", $self->{'props'}->{'PG-USER'} if ($self->{'props'}->{'PG-USER'});

   my $status = system($self->{'props'}->{'PSQL-PATH'}, @args, '--quiet', '--output',
       '/dev/null', '--command', "SELECT pg_start_backup('$label')",
        $self->{'props'}->{'PG-DB'}) >> 8;
   0 == $status or $self->{'die_cb'}->("Failed to call pg_start_backup");

   # tar data dir, using symlink to prefix
   # XXX: tablespaces and their symlinks?
   # See: http://www.postgresql.org/docs/8.0/static/manage-ag-tablespaces.html
   $status = system($self->{'runtar'}, $self->{'args'}->{'config'},
        $Amanda::Constants::GNUTAR, '--create', '--file',
       "$tmp/$_DATA_DIR_TAR", '--directory', $self->{'props'}->{'PG-DATADIR'}, ".") >> 8;
   0 == $status or $self->{'die_cb'}->("Failed to tar data directory (exit status $status)");

   $status = system($self->{'props'}->{'PSQL-PATH'}, @args, '--quiet', '--output',
       '/dev/null', '--command', "SELECT pg_stop_backup()",
        $self->{'props'}->{'PG-DB'}) >> 8;
   0 == $status or $self->{'die_cb'}->("Failed to call pg_stop_backup (exit status $status)");

   # determine WAL files and append and create their tar file
   my ($fname, $bfile, $start_wal, $end_wal, @wal_files);
   # wait up to 60s for the .backup file to be copied
   for (my $count = 0; $count < 60; $count++) {
       my $adir = new IO::Dir($self->{'props'}->{'PG-ARCHIVEDIR'});
       $adir or $self->{'die_cb'}->("Could not open archive WAL directory");
       while (defined($fname = $adir->read())) {
           if ($fname =~ /\.backup$/) {
               my $blabel;
               my $bf = new IO::File("$self->{'props'}->{'PG-ARCHIVEDIR'}/$fname");
               my ($start, $end, $lab);
               while (my $l = <$bf>) {
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
               }
           }
       }
       $adir->close();
       last if $start_wal and $end_wal;
       sleep(1);
   }

   my $adir = new IO::Dir($self->{'props'}->{'PG-ARCHIVEDIR'});
   while (defined($fname = $adir->read())) {
       if ($fname =~ /$_WAL_FILE_PAT/) {
           if (($fname ge $start_wal) and ($fname le $end_wal)) {
           push @wal_files, $fname;
           } elsif ($fname lt $start_wal) {
               $self->{'unlink_cb'}->($fname);
           }
       }
   }
   $adir->close();

   # create an empty archive for uniformity
   @wal_files or @wal_files = ('--files-from', '/dev/null');


   $status = system($self->{'runtar'}, $self->{'args'}->{'config'},
        $Amanda::Constants::GNUTAR,
       '--create', '--file', "$tmp/$_ARCHIVE_DIR_TAR",
       '--directory', $self->{'props'}->{'PG-ARCHIVEDIR'}, @wal_files) >> 8;
   0 == $status or $self->{'die_cb'}->("Failed to tar archived WAL files (exit status $status)");

   # create the final tar file
   my $size = _run_tar_totals($self, $out_h, '--directory', $tmp,
       $_ARCHIVE_DIR_TAR, $_DATA_DIR_TAR);

   $self->{'state_cb'}->($self, $end_wal);

   # try to cleanup a bit
   unlink("$self->{'props'}->{'PG-ARCHIVEDIR'}/$bfile");

   $cleanup->();
   $self->{'done_cb'}->($size);
}

sub _incr_backup {
   my ($self, $out_h) = @_;

   my $end_wal = _get_prev_state($self);
   unless ($end_wal) { _base_backup(@_); return; }

   my $adir = new IO::Dir($self->{'props'}->{'PG-ARCHIVEDIR'});
   $adir or $self->{'die_cb'}->("Could not open archive WAL directory");
   my $max_wal = "";
   my ($fname, @wal_files);
   while (defined($fname = $adir->read())) {
       if (($fname =~ /$_WAL_FILE_PAT/) and ($fname ge $end_wal)) {
           $max_wal = $fname if $fname gt $max_wal;
           push @wal_files, $fname;
       }
   }

   $self->{'state_cb'}->($self, $max_wal ? $max_wal : $end_wal);

   if (@wal_files) {
       $self->{'done_cb'}->(_run_tar_totals($self, $out_h, @wal_files));
   } else {
       $self->{'done_cb'}->(0);
   }
}

sub command_backup {
   my $self = shift;

   my $msg_fd = IO::Handle->new_from_fd(3, 'w');
   $msg_fd or confess("Could not open message fd");

   $self->{'done_cb'} = sub {
       my $size = shift @_;
       $msg_fd->print("sendbackup: size $size\n");
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
   if ($self->{'props'}->{'PG-CLEANUPWAL'} =~ /^(f|false|n|no|off)/i) {
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
       _base_backup($self, \*STDOUT);
   } else {
       _incr_backup($self, \*STDOUT);
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
          '--file', $_ARCHIVE_DIR_TAR, '--directory', $_ARCHIVE_DIR_RESTORE,
          '--preserve-permissions') >> 8;
       (0 == $status) or confess("Failed to extract archived WAL files from base backup (exit status: $status)");
       unlink($_ARCHIVE_DIR_TAR);

       $status = system($self->{'args'}->{'gnutar-path'}, '--extract',
          '--file', $_DATA_DIR_TAR, '--directory', $_DATA_DIR_RESTORE,
          '--preserve-permissions') >> 8;
       (0 == $status) or confess("Failed to extract data directory from base backup (exit status: $status)");
       unlink($_DATA_DIR_TAR);
   }
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
    'gnutar=s',
) or usage();

my $application = Amanda::Application::ampgsql->new($opts);

$application->do($ARGV[0]);
