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

use Test::More tests => 17;
use strict;
use warnings;
use POSIX qw(WIFEXITED WEXITSTATUS EINTR);

use lib "@amperldir@";
use Amanda::MainLoop qw( :GIOCondition );

{
    my $global = 0;

    my $to = Amanda::MainLoop::timeout_source(200);
    $to->set_callback(sub { 
	# ignore $src argument
	if (++$global >= 3) {
	    $to->remove();
	    Amanda::MainLoop::quit();
	}
    });

    Amanda::MainLoop::run();
    is($global, 3, "Timeout source works, calls back repeatedly (using a closure)");
}

{
    my $global = 0;

    my $to = Amanda::MainLoop::timeout_source(200);
    $to->set_callback(sub { 
	my ($src) = @_;
	if (++$global >= 3) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $to = undef; # remove the lexical reference to the source

    Amanda::MainLoop::run();
    is($global, 3, "Timeout source works, calls back repeatedly (no external reference to the source)");
}

{
    my $global = 0;

    my $id = Amanda::MainLoop::idle_source(5);
    $id->set_callback(sub { 
	my ($src) = @_;
	if (++$global >= 30) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });

    Amanda::MainLoop::run();
    is($global, 30, "Idle source works, calls back repeatedly");
    $id->remove();
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

    $to2->remove();
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
	my ($src, $got_pid, $got_status) = @_;
	Amanda::MainLoop::quit();
	$src->remove();

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
    $to->set_callback(sub {
	my ($src) = @_;
	$global = 7;

	$src->remove();
	Amanda::MainLoop::quit();
    });

    Amanda::MainLoop::run();
    is($global, 1, "Child watch detects a dead child");

    $cw->remove();
    $to->remove();
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
	my ($src, $got_pid, $got_status) = @_;
	Amanda::MainLoop::quit();
	$src->remove();

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

    $cw->remove();
    $to->remove();
}

{
    my $global = 0;
    my ($readinfd, $writeinfd) = POSIX::pipe();
    my ($readoutfd, $writeoutfd) = POSIX::pipe();

    my $pid = fork();
    if ($pid == 0) {
	## child

	my $data;

	POSIX::close($readinfd);
	POSIX::close($writeoutfd);

	# the read()s here are to synchronize with our parent; the
	# results are ignored.
	POSIX::read($readoutfd, $data, 1024);
	POSIX::write($writeinfd, "HELLO\n", 6);
	POSIX::read($readoutfd, $data, 1024);
	POSIX::write($writeinfd, "WORLD\n", 6);
	POSIX::read($readoutfd, $data, 1024);
	exit(33);
    }

    ## parent

    POSIX::close($writeinfd);
    POSIX::close($readoutfd);

    my @events;

    my $to = Amanda::MainLoop::timeout_source(200);
    my $times = 3;
    $to->set_callback(sub {
	push @events, "time";
	POSIX::write($writeoutfd, "A", 1); # wake up the child
	if (--$times == 0) {
	    $to->remove();
	}
    });

    my $cw = Amanda::MainLoop::child_watch_source($pid);
    $cw->set_callback(sub {
	my ($src, $got_pid, $got_status) = @_;
	$cw->remove();
	Amanda::MainLoop::quit();

	push @events, "died";
    });

    my $fd = Amanda::MainLoop::fd_source($readinfd, $G_IO_IN | $G_IO_HUP);
    $fd->set_callback(sub {
	my $str;
	if (POSIX::read($readinfd, $str, 1024) == 0) {
	    # EOF
	    POSIX::close($readinfd);
	    POSIX::close($writeoutfd);
	    $fd->remove();
	    return;
	}
	chomp $str;
	push @events, "read $str";
    });

    Amanda::MainLoop::run();
    $to->remove();
    $cw->remove();
    $fd->remove();

    is_deeply([ @events ],
	[ "time", "read HELLO", "time", "read WORLD", "time", "died" ],
	"fd source works for reading from a file descriptor");
}

# see if a "looping" callback with some closure values works.  This test teased
# out some memory corruption bugs once upon a time.

{
    my $completed = 0;
    sub loop {
	my ($finished_cb) = @_;
	my $time = 700;
	my $to;

	my $cb;
	$cb = sub {
	    $time -= 300;
	    $to->remove();
	    if ($time <= 0) {
		$finished_cb->();
	    } else {
		$to = Amanda::MainLoop::timeout_source($time);
		$to->set_callback($cb);
	    }
	};
	$to = Amanda::MainLoop::timeout_source($time);
	$to->set_callback($cb);
    };
    loop(sub {
	$completed = 1;
	Amanda::MainLoop::quit();
    });
    Amanda::MainLoop::run();
    is($completed, 1, "looping construct terminates with a callback");
}

# Make sure that a die() in a callback correctly kills the process.  Such
# a die() skips the usual Perl handling, so an eval { } won't do -- we have
# to fork a child.
{
    my $global = 0;
    my ($readfd, $writefd) = POSIX::pipe();

    my $pid = fork();
    if ($pid == 0) {
	## child

	my $data;

	# fix up the file descriptors to hook fd 2 (stderr) to
	# the pipe
	POSIX::close($readfd);
	POSIX::dup2($writefd, 2);
	POSIX::close($writefd);

	# and now die in a callback, using an eval {} in case the
	# exception propagates out of the MainLoop run()
	my $src = Amanda::MainLoop::timeout_source(10);
	$src->set_callback(sub { die("Oh, the humanity"); });
	eval { Amanda::MainLoop::run(); };
	exit(33);
    }

    ## parent

    POSIX::close($writefd);

    # read from the child and wait for it to die.  There's no
    # need to use MainLoop here.
    my $str;
    while (!defined(POSIX::read($readfd, $str, 1024))) {
	# we may be interrupted by a SIGCHLD; keep going
	next if ($! == EINTR);
	die ("POSIX::read failed: $!");
    }
    POSIX::close($readfd);
    waitpid($pid, 0);

    ok($? != 33 && $? != 0, "die() in a callback exits with an error condition");
    like($str, qr/Oh, the humanity/, "..and displays die message on stderr");
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

{
    my ($cb1, $cb2);
    my $gothere = 0;

    $cb1 = sub {
	my ($a, $b) = @_;
	ok(Amanda::MainLoop::is_running(),
	    "call_later waits until mainloop runs");
	is($a+$b, 10,
	    "call_later passes arguments correctly");
	Amanda::MainLoop::call_later($cb2);
	Amanda::MainLoop::quit();
    };

    $cb2 = sub {
	$gothere = 1;
    };

    ok(!Amanda::MainLoop::is_running(), "main loop is correctly recognized as not running");
    Amanda::MainLoop::call_later($cb1, 7, 3);
    Amanda::MainLoop::run();
    ok($gothere, "call_later while already running calls immediately");

    my @actions = ();

    $cb1 = sub {
        push @actions, "cb1 start";
	Amanda::MainLoop::call_later($cb2, "hello");
        push @actions, "cb1 end";
    };

    $cb2 = sub {
	my ($greeting) = @_;

        push @actions, "cb2 start $greeting";
	Amanda::MainLoop::quit();
        push @actions, "cb2 end";
    };

    Amanda::MainLoop::call_later($cb1);
    Amanda::MainLoop::run();
    is_deeply([ @actions ],
              [ "cb1 start", "cb1 end", "cb2 start hello", "cb2 end" ],
              "call_later doesn't call its argument immediately");
}
