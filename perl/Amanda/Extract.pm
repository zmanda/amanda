# vim:ft=perl
# Copyright (c) 2008-2013 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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

package Amanda::Extract;

use strict;
use warnings;
use IPC::Open3;

use Amanda::Debug qw( :logging );
use Amanda::Paths;

=head1 NAME

Amanda::Extract - perl utilities to run scripts and applications

=head1 SYNOPSIS

  use Amanda::Extract;

  my (@bsu, @err)= Amanda::Extract::BSU(application => $application,
					config      => $config,
					host        => $host,
					disk        => $disk,
					device      => $device);
  my (@bsu, @err)= Amanda::Extract::Run($application_name, \@g_options,
					$hdr, @properties);

=cut

sub BSU {
    my (%params) = @_;

    my %bsu;
    my @err;
    my @command;

    push @command, $Amanda::Paths::APPLICATION_DIR . '/' . $params{'application'};
    push @command, "support";
    push @command, "--config", $params{'config'} if $params{'config'};
    push @command, "--host"  , $params{'host'}   if $params{'host'};
    push @command, "--disk"  , $params{'disk'}   if $params{'disk'};
    push @command, "--device", $params{'device'} if $params{'device'};
    debug("Running: " . join(' ', @command));

    my $in;
    my $out;
    my $err = Symbol::gensym;
    my $pid = open3($in, $out, $err, @command);

    close($in);
    while (my $line = <$out>) {
	chomp $line;
	debug("support: $line");
	my ($name, $value) = split ' ', $line;

	$name = lc($name);

	if ($name eq 'config' ||
	    $name eq 'host' ||
	    $name eq 'disk' ||
	    $name eq 'index-line' ||
	    $name eq 'index-xml' ||
	    $name eq 'message-line' ||
	    $name eq 'message-xml' ||
	    $name eq 'record' ||
	    $name eq 'include-file' ||
	    $name eq 'include-list' ||
	    $name eq 'include-list-glob' ||
	    $name eq 'include-optional' ||
	    $name eq 'exclude-file' ||
	    $name eq 'exclude-list' ||
	    $name eq 'exclude-list-glob' ||
	    $name eq 'exclude-optional' ||
	    $name eq 'collection' ||
	    $name eq 'caclsize' ||
	    $name eq 'client-estimate' ||
	    $name eq 'multi-estimate' ||
	    $name eq 'amfeatures' ||
	    $name eq 'recover-dump-state-file') {
	    $bsu{$name} = ($value eq "YES");
	} elsif ($name eq 'max-level') {
	    $bsu{$name} = $value;
	} elsif ($name eq 'recover-mode') {
	    $bsu{'smb-recover-mode'} = $value eq 'SMB';
	} elsif ($name eq 'recover-path') {
	    $bsu{'recover-path-cwd'} = $value eq 'CWD';
	    $bsu{'recover-path-remote'} = $value eq 'REMOTE';
	} elsif ($name eq 'data-path') {
	    if ($value eq 'AMANDA') {
		$bsu{'data-path-amanda'} = 1;
	    } elsif ($value eq 'DIRECTTCP') {
		$bsu{'data-path-directtcp'} = 1;
	    }
	}
    }
    close($out);

    while (my $line = <$err>) {
	chomp($line);
	next if $line == '';
	push @err, $line;
    }
    close($err);

    waitpid($pid, 0);
    my $child_exit_status = $? >> 8;

    if ($child_exit_status != 0) {
	push @err, "exited with status $child_exit_status";
    }
    return (\%bsu, \@err);
}

1;
