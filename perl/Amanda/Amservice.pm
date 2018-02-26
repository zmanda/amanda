# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94085, or: http://www.zmanda.com

package Amanda::Amservice;

=head1 NAME

Amanda::Amservice -- exec amservice and connect multiple stream.

=cut

use strict;
use warnings;
use Data::Dumper;
use IO::Handle;
use IPC::Open3;
use POSIX qw( :fcntl_h);

use Amanda::Util qw( :constants stream_server stream_accept );
use Amanda::Debug qw( debug );
use Amanda::Constants;
use Amanda::Paths;
use Amanda::MainLoop;
use Amanda::Feature;

sub new {
    my $class = shift;

    my $self = bless {
	_streams => {},
    }, $class;

    $self->_add_stream('main', 0, 1);
    return $self;
}

sub run {
    my $self = shift;
    my $req = shift;
    my $host = shift;
    my $auth = shift;
    my $service = shift;
    my $args = shift;
    my @streams = @_;

    my @argv;
    my @parent_close;

    $self->{'req'} = $req;
    $self->{'host'} = $host;
    $self->{'auth'} = $auth;
    $self->{'service'} = $service;

    my $conf = Amanda::Config::get_config_name();
    if ($conf) {
	push @argv, '--config', $conf;
    }
    push @argv, @{$args};

    # open 2 pipes for each stream
    #$^F = 2;
    #$^F = 100;
    foreach my $stream (@streams) {
	my ($rfd1, $wfd1);
	my ($rfd2, $wfd2);
	pipe $rfd1, $wfd1;
	pipe $rfd2, $wfd2;

	my $flags;
	$flags = fcntl($rfd1, F_GETFD, 0) || die("fcntl: $!");
	$flags &= ~FD_CLOEXEC;
	fcntl($rfd1, F_SETFD, $flags) || die("fcntl: $!");

	$flags = fcntl($wfd2, F_GETFD, 0) || die("fcntl: $!");
	$flags &= ~FD_CLOEXEC;
	fcntl($wfd2, F_SETFD, $flags) || die("fcntl: $!");

	$self->_add_stream($stream, $rfd2, $wfd1);
	push @parent_close, $rfd1, $wfd2;
	push @argv, "--stream", "$stream,".fileno($rfd1).",".fileno($wfd2);
    }

    push @argv, $host, $auth, $service;

    my($wtr, $rdr, $err);
    $err = Symbol::gensym;
    my $pid = open3($wtr, $rdr, $err, "$Amanda::Paths::sbindir/amservice", @argv);

    foreach my $fd (@parent_close) {
	close($fd);
    }
    print $wtr  $req;
    close($wtr);

    my $rep;
    my $line;
    while ($line = <$rdr>) {
	$rep .= "$line";
    }
    close($rdr);

    if (!$rep) {
	while ($line = <$err>) {
	    $rep .= $line;
	}
	close($err);
    }

    $self->{'rep'} = $rep;

    return $rep;
}


sub rfd {
    my $self = shift;
    my ($name) = @_;

    return -1 unless exists $self->{'_streams'}{$name};
    my $fd = $self->{'_streams'}{$name}{'rfd'};
    return $fd;
}

sub rfd_fileno {
    my $self = shift;
    my ($name) = @_;

    return undef unless exists $self->{'_streams'}{$name};
    my $fd = $self->rfd($name);
    return defined($fd)? fileno($fd) : -1;
}

sub wfd {
    my $self = shift;
    my ($name) = @_;

    return undef unless exists $self->{'_streams'}{$name};
    my $fd = $self->{'_streams'}{$name}{'wfd'};
    return $fd;
}

sub wfd_fileno {
    my $self = shift;
    my ($name) = @_;

    return -1 unless exists $self->{'_streams'}{$name};
    my $fd = $self->wfd($name);
    return defined($fd)? fileno($fd) : -1;
}

sub close {
    my $self = shift;
    my ($name, $dir) = @_;

    $dir = 'rw' unless defined $dir;
    my $rfd = $self->rfd($name);
    my $wfd = $self->wfd($name);

    # sockets will have the read and write fd's, and are handled differently.  If
    # one end of a socket has been closed, then we can treat it like a regular fd
    if (defined $rfd && defined $wfd && $rfd == $wfd) {
	die "stream is already closed?" if ($rfd == -1);

	if ($dir eq 'rw') {
	    close $rfd ;
	    $rfd = $wfd = undef;
	} elsif ($dir eq 'r') {
	    # perl doesn't provide a fd-compatible shutdown, but luckily shudown
	    # affects dup'd file descriptors, too!  So create a new handle and shut
	    # it down.  When the handle is garbage collected, it will be closed,
	    # but that will not affect the original.  This will look strange in an
	    # strace, but it works without SWIGging shutdown()
	    shutdown(IO::Handle->new_from_fd(POSIX::dup(fileno($rfd)), "r"), 0);
	    $rfd = undef;
	} elsif ($dir eq 'w') {
	    shutdown(IO::Handle->new_from_fd(POSIX::dup(fileno($wfd)), "w"), 1);
	    $wfd = undef;
	}
    } else {
	if ($dir =~ /r/ and $rfd != -1) {
	    close $rfd;
	    $rfd = undef;
	}
	if ($dir =~ /w/ and $wfd != -1) {
	    close $wfd;
	    $wfd = undef;
	}
    }

    if (!defined $rfd and !defined $wfd) {
	delete $self->{'_streams'}{$name};
    } else {
	$self->{'_streams'}{$name}{'rfd'} = $rfd;
	$self->{'_streams'}{$name}{'wfd'} = $wfd;
    }
}

# private methods

sub _add_stream {
    my $self = shift;
    my ($stream, $rfd, $wfd) = @_;

    $self->{'_streams'}{$stream} = {
	rfd => $rfd,
	wfd => $wfd,
    };
}

1;
