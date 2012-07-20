# vim:ft=perl
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck;
use File::Path;
use Amanda::Paths;

=head1 NAME

Installcheck - utilities for installchecks (not installed)

=head1 SYNOPSIS

  use Installcheck;

  my $testdir = "$Installcheck::TMP/mystuff/";

  use Amanda::Debug;
  Amanda::Debug::dbopen("installcheck");
  Installcheck::log_test_output();

=head1 DESCRIPTION

Miscellaneous utilities for installchecks. No symbols are exported by default.

=over

=item C<$TMP>

The temporary directory for installcheck data. This directory is created for you.

=item C<log_test_output()>

Calling this function causes status meesages from tests (e.g. "ok 1 - some test")
to be recorded in the debug logs. It should be called exactly once.

=item C<get_unused_port()>

Find a local TCP port that is currently unused and not listed in
C</etc/services>.  This can still fail, if the port in question is bound by
another process between the call to C<get_unused_port()> and the port's
eventual use.

=back

=cut

use strict;
use warnings;
use Socket;
require Exporter;

our @ISA = qw(Exporter);
our @EXPORT = qw( $srcdir );

use Amanda::Util;

our $TMP = "$AMANDA_TMPDIR/installchecks";

# the Makefile provides srcdir to us in most cases; if not, assume it's "."
our $srcdir = $ENV{'srcdir'} || '.';

# run this just before the script actually executes
# (not during syntax checks)
INIT {
    Amanda::Util::set_pname("$0");
    mkpath($TMP);
}

my @used_ports;
sub get_unused_port {
     my ($base, $count) = (10000, 10000);
     my $i;
     my $tcp = getprotobyname('tcp');

     # select ports randomly until we find one that is usable or have tried 1000
     # ports
     for ($i = 0; $i < 1000; $i++) {
	my $port = int(rand($count)) + $base;

	# have we already used it?
	next if (grep { $_ == $port } @used_ports);

	# is it listed in /etc/services?
	next if (getservbyport($port, $tcp));

	# can we bind() to it? (using REUSADDR so that the kernel doesn't reserve
	# the port after we close it)
	next unless socket(SOCK, PF_INET, SOCK_STREAM, $tcp);
	next unless setsockopt(SOCK, SOL_SOCKET, SO_REUSEADDR, pack("l", 1));
	next unless bind(SOCK, sockaddr_in($port, INADDR_ANY));
	close(SOCK);

	# it passed the gauntlet of tests, so the port is good
	push @used_ports, $port;
	return $port;
    }

    die("could not find unused port");
}

sub log_test_output {
    my $builder = Test::More->builder();

    # not supported on perl-5.6
    return if !$^V or $^V lt v5.8.0;

    # wrap each filehandle used for output
    foreach my $out (qw(output failure_output todo_output)) {
        $builder->$out(Installcheck::TestFD->new($builder->$out));
    }
}

package Installcheck::TestFD;

use base qw(Tie::Handle IO::Handle);

use Symbol;
use Amanda::Debug qw(debug);

use strict;
use warnings;

sub new {
    my ($class, $fh) = @_;
    # save the underlying filehandle
    my $o = {'fh' => $fh};
    # must bless before tie()
    bless($o, $class);
    # note that gensym is needed so we have something to tie()
    my $new_fh = gensym;
    tie(*$new_fh, $class, $o);
    # note that the anonymous glob reference must be returned, so
    # when 'print $fh "some string"' is used it works
    return $new_fh;
}

sub TIEHANDLE {
    my ($class, $o) = @_;
    return $o;
}

# other methods of IO::Handle or Tie::Handle may be called in theory,
# but in practice this seems to be all we need

sub print {
    my ($self, @args);
    reutrn $self->PRINT(@args);
}

sub PRINT {
    my ($self, @msgs) = @_;
    # log each line separately
    foreach my $m (split("\n", join($, , @msgs))) {
        debug($m);
    }
    # now call print on the wrapped filehandle
    return $self->{'fh'}->print(@msgs);
}

1;
