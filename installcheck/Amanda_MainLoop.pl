# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 8;
use strict;
use POSIX qw(WIFEXITED WEXITSTATUS);

use lib "@amperldir@";
use Amanda::MainLoop;

{
    my $global = 0;

    my $to = Amanda::MainLoop::timeout_source(200);
    $to->set_callback(sub { 
	if (++$global >= 3) {
	    $to->remove();
	    Amanda::MainLoop::quit();
	}
    });

    Amanda::MainLoop::run();
    is($global, 3, "Timeout source works, calls back repeatedly");
}

{
    my $global = 0;

    my $id = Amanda::MainLoop::idle_source(5);
    $id->set_callback(sub { 
	if (++$global >= 30) {
	    $id->remove();
	    Amanda::MainLoop::quit();
	}
    });

    Amanda::MainLoop::run();
    is($global, 30, "Idle source works, calls back repeatedly");
}

{
    my $global = 0;

    # to1 is removed before it runs, so it should never
    # execute its callback
    my $to1 = Amanda::MainLoop::timeout_source(10);
    $to1->set_callback(sub { ++$global; });
    $to1->remove();

    my $to2 = Amanda::MainLoop::timeout_source(300);
    $to2->set_callback(sub { Amanda::MainLoop::quit(); });

    Amanda::MainLoop::run();
    is($global, 0, "A remove()d source doesn't call back");
}

{
    my $global = 0;

    my $pid = fork();
    if ($pid == 0) {
	## child
	sleep(1);
	exit(9);
    }

    ## parent

    my $cw = Amanda::MainLoop::child_watch_source($pid);
    $cw->set_callback(sub {
	my ($got_pid, $got_status) = @_;
	Amanda::MainLoop::quit();
	$cw->remove();

	if ($got_pid != $pid) {
	    diag("Got pid $got_pid, but expected $pid");
	    return;
	}
	if (!WIFEXITED($got_status)) {
	    diag("Didn't get an 'exited' status");
	    return;
	}
	if (WEXITSTATUS($got_status) != 9) {
	    diag("Didn't get exit status 9");
	    return;
	}
	$global = 1;
    });

    my $to = Amanda::MainLoop::timeout_source(3000);
    $to->set_callback(sub { $global = 7; Amanda::MainLoop::quit(); });

    Amanda::MainLoop::run();
    is($global, 1, "Child watch detects a dead child");
}

{
    my $global = 0;

    my $pid = fork();
    if ($pid == 0) {
	## child
	exit(11);
    }

    ## parent

    sleep(1);
    my $cw = Amanda::MainLoop::child_watch_source($pid);
    $cw->set_callback(sub {
	my ($got_pid, $got_status) = @_;
	Amanda::MainLoop::quit();
	$cw->remove();

	if ($got_pid != $pid) {
	    diag("Got pid $got_pid, but expected $pid");
	    return;
	}
	if (!WIFEXITED($got_status)) {
	    diag("Didn't get an 'exited' status");
	    return;
	}
	if (WEXITSTATUS($got_status) != 11) {
	    diag("Didn't get exit status 11");
	    return;
	}
	$global = 1;
    });

    my $to = Amanda::MainLoop::timeout_source(3000);
    $to->set_callback(sub { $global = 7; Amanda::MainLoop::quit(); });

    Amanda::MainLoop::run();
    is($global, 1, "Child watch detects a dead child that dies before the callback is set");
}

{
    my $global = 0;
    my ($readfh, $writefh);

    pipe $readfh, $writefh;

    my $pid = fork();
    if ($pid == 0) {
	## child
	syswrite($writefh, "HELLO\n", 6);
	sleep(1);
	syswrite($writefh, "WORLD\n", 6);
	sleep(1);
	exit(33);
    }

    ## parent

    my @events;

    my $to = Amanda::MainLoop::timeout_source(700);
    $to->set_callback(sub {
	push @events, "time";
    });

    my $cw = Amanda::MainLoop::child_watch_source($pid);
    $cw->set_callback(sub {
	my ($got_pid, $got_status) = @_;
	Amanda::MainLoop::quit();

	push @events, "died";
    });

    my $fd = Amanda::MainLoop::fd_source(fileno($readfh), $Amanda::MainLoop::G_IO_IN);
    $fd->set_callback(sub {
	my $str = <$readfh>;
	chomp $str;
	push @events, "read $str";
    });

    Amanda::MainLoop::run();
    $cw->remove();
    $to->remove();
    $fd->remove();

    is_deeply([ @events ],
	[ "read HELLO", "time", "read WORLD", "time", "died" ],
	"fd source works for reading from a file descriptor");
}

# test misc. management of sources.  Ideally it won't crash :)

my $src = Amanda::MainLoop::idle_source(1);
$src->set_callback(sub { 1; });
$src->set_callback(sub { 1; });
$src->set_callback(sub { 1; });
pass("Can call set_callback a few times on the same source");

$src->remove();
$src->remove();
pass("Calling remove twice is ok");
