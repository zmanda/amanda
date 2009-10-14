# vim:ft=perl
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck::Mock;

=head1 NAME

Installcheck::Mock - utilities for the installcheck mocks

=head1 SYNOPSIS

  use Installcheck::Mock;

  my $statefile = Installcheck::Mock::setup_mock_mtx(
	 num_slots => 5,
	 num_ie => 1,
	 barcodes => 1,
	 track_orig => 1,
	 num_drives => 2,
	 loaded_slots => {
	     1 => '023984'
         },
	 first_slot => 1,
	 first_drive => 0,
	 first_ie => 6,
       );

=head1 USAGE

C<setup_mock_mtx> sets up a state file for C<mock/mtx> with the given config
hash, and returns the filename of the state file.  This function must be run
with the current dirctory pointing to 'installcheck/'.

To run an ndmjob instance, replete with a tape simulator, call C<run_ndmjob>
with a port number on which ndmjob should listen.  The ndmjob instance will
automatically be killed, and the temporary tape file deleted, when the test run
is finished.  The function returns the name of the tape "device".

=cut

use Installcheck;
use Cwd qw(abs_path);
use Data::Dumper;
use POSIX;
use IPC::Open3;
use Amanda::Paths;

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw( setup_mock_mtx $mock_mtx_path );

sub setup_mock_mtx {
    my %config = @_;
    my $state_filename = "$Installcheck::TMP/mtx_state";
    open (my $fh, ">", $state_filename) or die $!;
    print $fh (Data::Dumper->Dump([
	    { config => \%config }
	], ["STATE"]));
    close ($fh);

    return $state_filename;
}

our $mock_mtx_path = abs_path("mock/mtx");

sub run_ndmjob {
    my ($port) = @_;
    # fork once to create a "monitor" process
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $tapefile = "$Installcheck::TMP/ndmp-tapefile-$$"; # contains *my* pid

	open(my $fd, ">", $tapefile);
	close($fd);

	# first, launch ndmjob
	local(*INFH, *OUTFH);
	my $ndmjob_pid = IPC::Open3::open3("INFH", "OUTFH", 0,
	    "$amlibexecdir/ndmjob", "-d5", "-o", "daemon",
		    "-T.", "-f", $tapefile,
		    "-p", $port);
	# now wait for our parent process to die
	while (1) {
	    sleep(1);
	    last if (getppid() == 1);
	    if (POSIX::waitpid($ndmjob_pid, POSIX::WNOHANG)) {
		print STDERR "***** ndmjob exited prematurely\n";
		last;
	    }
	}

	# then kill the ndmjob process and delete the tapefile
	kill 9, $ndmjob_pid;
	unlink($tapefile);
	exit(0);
    } else {
	my $tapefile = "$Installcheck::TMP/ndmp-tapefile-$pid"; # contains *child's* pid

	# sleep long enough for the ndmjob process to start listening
	sleep(1);

	return $tapefile;
    }
}

1;
