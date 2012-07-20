# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Application::Zfs;

use strict;
use warnings;
use Symbol;
use IPC::Open3;
use Amanda::Debug qw( :logging );

=head1 NAME

Amanda::Application::Zfs -- collection of function to use with zfs

=head1 SYNOPSIS

=head1 INTERFACE

=cut

sub zfs_set_value {
    my $self = shift;

    if (defined $self->{execute_where} && $self->{execute_where} ne "client") {
	$self->print_to_server_and_die(" Script must be run on the client 'execute-where client'", $Amanda::Script_App::ERROR);
    }
    if ($self->{df_path} ne "df" && !-e $self->{df_path}) {
	$self->print_to_server_and_die("Can't execute DF-PATH '$self->{df_path}' command",
                        $Amanda::Script_App::ERROR);
    }
    if ($self->{zfs_path} ne "zfs" && !-e $self->{zfs_path}) {
        $self->print_to_server_and_die("Can't execute ZFS-PATH '$self->{zfs_path}' command",
                        $Amanda::Script_App::ERROR);
    }

    if ($self->{pfexec} =~ /^YES$/i) {
        $self->{pfexec_cmd} = $self->{pfexec_path};
    }
    if (defined $self->{pfexec_cmd} && $self->{pfexec_cmd} ne "pfexec" && !-e $self->{pfexec_cmd}) {
        $self->print_to_server_and_die("Can't execute PFEXEC-PATH '$self->{pfexec_cmd}' command",
                        $Amanda::Script_App::ERROR);
    }
    if (!defined $self->{pfexec_cmd}) {
        $self->{pfexec_cmd} = "";
    }

    if (!defined $self->{device}) {
	if ($self->{action} eq "check") {
	    return;
	} else {
	    $self->print_to_server_and_die("'--device' is not provided",
					   $Amanda::Script_App::ERROR);
	}
    }

    my $device = $self->{device};
    $device = $self->{directory} if defined $self->{directory};
    # determine if $device is a mountpoint or ZFS dataset
    my $cmd = "$self->{pfexec_cmd} $self->{zfs_path} get -H -o value mountpoint $device";
    debug "running: $cmd";
    my($wtr, $rdr, $err, $pid);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    my $zmountpoint = <$rdr>;
    waitpid $pid, 0;
    close $rdr;
    close $err;

    if ($? == 0) {
        chomp $zmountpoint;

        # zfs dataset supplied
        $self->{filesystem} = $device;

        # check if zfs volume
        if ($zmountpoint ne '-') {
            $self->{mountpoint} = $zmountpoint;
        } else {
            $self->{mountpoint} = undef;
        }
    } else {
        # filesystem, directory or invalid ZFS dataset name
        $cmd = "$self->{df_path} $device";
        debug "running: $self->{df_path} $device";
        $err = Symbol::gensym;
        $pid = open3($wtr, $rdr, $err, $cmd);
        close $wtr;
        my @ret;
        while (<$rdr>) {
            chomp;
            push @ret,$_;
        }
        my $errmsg = <$err>;
        waitpid $pid, 0;
        close $rdr;
        close $err;

        if ($? != 0) {
	    my $ret = $ret[0];
            # invalid filesystem of ZFS dataset name
            if (defined $errmsg) {
                chomp $errmsg;
            }
            if (defined $ret && defined $errmsg) {
                $self->print_to_server_and_die("$ret, $errmsg", $Amanda::Script_App::ERROR);
            } elsif (defined $ret) {
                $self->print_to_server_and_die($ret, $Amanda::Script_App::ERROR);
            } elsif (defined $errmsg) {
                $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
            } else {
                $self->print_to_server_and_die(
                            "Failed to find mount points: $device",
                            $Amanda::Script_App::ERROR);
            }
        }

        my $size = @ret;
        if ($size eq 1) {
            # Solaris type df
            @ret = split /:/, $ret[0];
            if ($ret[0] =~ /(\S*)(\s*)(\()(\S*)(\s*)(\))$/) {
                $self->{mountpoint} = $1;
                $self->{filesystem} = $4;
            } else {
                $self->print_to_server_and_die(
                            "Failed to find mount points: $device",
                            $Amanda::Script_App::ERROR);
            }
        } else {
            # FreeBSD type df with header
            if ($ret[1] =~ /^(\S+)(\s+)((\S+)(\s+))+(\S+)(\s*)$/) {
                $self->{mountpoint} = $6;
                $self->{filesystem} = $1;
            } else {
                $self->print_to_server_and_die(
                            "Failed to find mount points: $device",
                            $Amanda::Script_App::ERROR);
            }
        }

        $cmd = "$self->{pfexec_cmd} $self->{zfs_path} get -H -o value mountpoint $self->{filesystem}";
        debug "running: $cmd|";
        $err = Symbol::gensym;
        $pid = open3($wtr, $rdr, $err, $cmd);
        close $wtr;
        $zmountpoint = <$rdr>;
	chomp $zmountpoint;
        $errmsg = <$err>;
        waitpid $pid, 0;
        close $rdr;
        close $err;

        if ($? != 0) {
            if (defined $errmsg) {
                chomp $errmsg;
            }
            if (defined $zmountpoint && defined $errmsg) {
                $self->print_to_server_and_die($zmountpoint, $errmsg, $Amanda::Script_App::ERROR);
            } elsif (defined $zmountpoint) {
                $self->print_to_server_and_die($zmountpoint, $Amanda::Script_App::ERROR);
            } elsif (defined $errmsg) {
                $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
            } else {
                $self->print_to_server_and_die(
                        "Failed to find mount points: $self->{filesystem}",
                        $Amanda::Script_App::ERROR);
            }
        }
        if ($zmountpoint ne 'legacy' && $zmountpoint ne $self->{mountpoint}) {
            $self->print_to_server_and_die(
                "mountpoint from 'df' ($self->{mountpoint}) and 'zfs list' ($zmountpoint) differ",
                $Amanda::Script_App::ERROR);
        }

        if (!($device =~ /^$self->{mountpoint}/)) {
            $self->print_to_server_and_die(
                "mountpoint '$self->{mountpoint}' is not a prefix of the device '$device'",
                $Amanda::Script_App::ERROR);
        }

    }

    $self->{snapshot} = $self->zfs_build_snapshotname($device);
    if (defined $self->{mountpoint}) {
	if ($device =~ /^$self->{mountpoint}/) {
            $self->{dir} = $device;
            $self->{dir} =~ s,^$self->{mountpoint},,;
            $self->{directory} = $self->{mountpoint} . "/.zfs/snapshot/" .
                                 $self->{snapshot} . $self->{dir};
	} else { # device is not the mountpoint
	    $self->{directory} = $self->{mountpoint} . "/.zfs/snapshot/" .
				 $self->{snapshot};
	}
    }
}

sub zfs_create_snapshot {
    my $self = shift;

    my $cmd = "$self->{pfexec_cmd} $self->{zfs_path} snapshot $self->{filesystem}\@$self->{snapshot}";
    debug "running: $cmd";
    my($wtr, $rdr, $err, $pid);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    my ($msg) = <$rdr>;
    my ($errmsg) = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if( $? != 0 ) {
        if(defined $msg && defined $errmsg) {
            $self->print_to_server_and_die("$msg, $errmsg", $Amanda::Script_App::ERROR);
        } elsif (defined $msg) {
            $self->print_to_server_and_die($msg, $Amanda::Script_App::ERROR);
        } elsif (defined $errmsg) {
            $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
        } else {
            $self->print_to_server_and_die("cannot create snapshot '$self->{filesystem}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
        }
    }
}

sub zfs_destroy_snapshot {
    my $self = shift;

    my $cmd = "$self->{pfexec_cmd} $self->{zfs_path} destroy $self->{filesystem}\@$self->{snapshot}";
    debug "running: $cmd|";
    my($wtr, $rdr, $err, $pid);
    my($msg, $errmsg);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    $msg = <$rdr>;
    $errmsg = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if( $? != 0 ) {
        if(defined $msg && defined $errmsg) {
            $self->print_to_server_and_die("$msg, $errmsg", $Amanda::Script_App::ERROR);
        } elsif (defined $msg) {
            $self->print_to_server_and_die($msg, $Amanda::Script_App::ERROR);
        } elsif (defined $errmsg) {
            $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
        } else {
            $self->print_to_server_and_die("cannot destroy snapshot '$self->{filesystem}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
        }
    }
}

sub zfs_destroy_snapshot_level {
    my $self = shift;
    my $level = shift;

    my $snapshotname = $self->zfs_find_snapshot_level($level);
    debug "zfs_destroy_snapshot_level: Current $snapshotname";
    if ($snapshotname ne "") {
      my $cmd = "$self->{pfexec_cmd} $self->{zfs_path} destroy  $self->{filesystem}\@$snapshotname";
      debug "running: $cmd|";
      my($wtr, $rdr, $err, $pid);
      my($msg, $errmsg);
      $err = Symbol::gensym;
      $pid = open3($wtr, $rdr, $err, $cmd);
      close $wtr;
      $msg = <$rdr>;
      $errmsg = <$err>;
      waitpid $pid, 0;
      close $rdr;
      close $err;
      if( $? != 0 ) {
          if(defined $msg && defined $errmsg) {
              $self->print_to_server_and_die("$msg, $errmsg", $Amanda::Script_App::ERROR);
          } elsif (defined $msg) {
              $self->print_to_server_and_die($msg, $Amanda::Script_App::ERROR);
          } elsif (defined $errmsg) {
              $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
          } else {
              $self->print_to_server_and_die("cannot destroy snapshot '$self->{filesystem}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
          }
      }
    }
}

sub zfs_rename_snapshot {
    my $self = shift;
    my $level = shift;

    my $device = $self->{device};
    $device = $self->{directory} if defined $self->{directory};
    my $newsnapshotname = $self->zfs_build_snapshotname($device, $level);
    my $cmd = "$self->{pfexec_cmd} $self->{zfs_path} rename $self->{filesystem}\@$self->{snapshot} $self->{filesystem}\@$newsnapshotname";
    debug "running: $cmd|";
    my($wtr, $rdr, $err, $pid);
    my($msg, $errmsg);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    $msg = <$rdr>;
    $errmsg = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if( $? != 0 ) {
        if(defined $msg && defined $errmsg) {
            $self->print_to_server_and_die("$msg, $errmsg", $Amanda::Script_App::ERROR);
        } elsif (defined $msg) {
            $self->print_to_server_and_die($msg, $Amanda::Script_App::ERROR);
        } elsif (defined $errmsg) {
            $self->print_to_server_and_die($errmsg, $Amanda::Script_App::ERROR);
        } else {
            $self->print_to_server_and_die("cannot rename snapshot '$self->{filesystem}\@$self->{snapshot}': unknown reason", $Amanda::Script_App::ERROR);
        }
    }
}

sub zfs_purge_snapshot {
    my $self = shift;
    my $minlevel = shift;
    my $maxlevel = shift;

    my $level;
    for ($level = $maxlevel; $level >= $minlevel; $level--) {
	debug "zfs_purge_snapshot: Check for existing snapshot at level $level";
        $self->zfs_destroy_snapshot_level($level);
    }
}

sub zfs_find_snapshot_level {
    my $self = shift;
    my $level = shift;

    my $device = $self->{device};
    $device = $self->{directory} if defined $self->{directory};
    my $snapshotname = $self->zfs_build_snapshotname($device, $level);

    my $cmd =  "$self->{pfexec_cmd} $self->{zfs_path} list -t snapshot $self->{filesystem}\@$snapshotname";
    debug "running: $cmd|";
    my($wtr, $rdr, $err, $pid);
    my($msg, $errmsg);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, $cmd);
    close $wtr;
    $msg = <$rdr>;
    $errmsg = <$err>;
    waitpid $pid, 0;
    close $rdr;
    close $err;
    if( $? != 0 ) {
	return "";
    }
    return $snapshotname;
}

sub zfs_build_snapshotname {
    my $self = shift;
    my $device = shift;
    my $level = shift;

    my $snapshotname = "";

    if ($self->{action} eq 'check') {
	$snapshotname = "amanda-" . Amanda::Util::sanitise_filename($self->{disk}) . "-check";
    } elsif (!defined $level) {
	$snapshotname = "amanda-" . Amanda::Util::sanitise_filename($self->{disk}) . "-current";
    } else {
	$snapshotname = "amanda-" . Amanda::Util::sanitise_filename($self->{disk}) . "-" . $level;
    }

    return $snapshotname;
}

1;
