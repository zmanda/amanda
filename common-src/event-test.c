/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 * 
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */

#include "amanda.h"
#include "testutils.h"
#include "event.h"

/* a random global variable to flag that some function has been called */
static int global;

/* file descriptor under EV_READFD or EV_WRITEFD */
static int cb_fd;

/* and some easy access to the event handles for callbacks */
static event_handle_t *hdl[10];

/*
 * Utils
 */

/* A common event callback that just decrements 'global', and frees
 * hdl[0] if global reaches zero.
 */
static void
test_decrement_cb(void *up G_GNUC_UNUSED)
{
    global--;
    tu_dbg("Decrement global to %d\n", global);
    if (global == 0) {
	tu_dbg("Release event\n");
	event_release(hdl[0]);
    }
}

/*
 * Tests
 */

/****
 * Test that EV_TIME events fire, repeatedly.
 */
static gboolean
test_ev_time(void)
{
    global = 2;
    hdl[0] = event_register(1, EV_TIME, test_decrement_cb, NULL);

    /* Block waiting for the event to fire.  The event itself eventually
     * unregisters itself, causing the event_loop to finish */
    event_loop(0);

    return (global == 0);
}

/****
 * Test that nonblocking waits don't block.
 */
static gboolean
test_nonblock(void)
{
    global = 1; /* the callback should not be triggered, so this should stay 1 */
    hdl[0] = event_register(1, EV_TIME, test_decrement_cb, NULL);

    event_loop(1); /* non-blocking */

    return (global != 0);
}

/****
 * Test that EV_WAIT events fire when event_wakeup is called, without waiting for
 * another iteration of the event loop.  Security API depends on callbacks occuring
 * immediately.
 */
static gboolean
test_ev_wait(void)
{
    global = 2;
    hdl[0] = event_register(4422, EV_WAIT, test_decrement_cb, NULL);

    if (global != 2) return FALSE;
    event_wakeup(4422);
    if (global != 1) return FALSE;
    event_wakeup(4422);
    if (global != 0) return FALSE;
    event_wakeup(4422); /* the handler has been removed, but this is not an error */
    if (global != 0) return FALSE;

    /* queue should now be empty, so this won't block */
    event_loop(0);

    return TRUE;
}

/****
 * Test that EV_WAIT events with the same ID added during an EV_WAIT callback are not
 * called back immediately, but wait for a subsequent wakeup.  Security API depends on
 * this behavior.  This is a pathological test :)
 */
static void
test_ev_wait_2_cb(void *up G_GNUC_UNUSED)
{
    global--;
    tu_dbg("Decrement global to %d\n", global);

    if (global >= 0) {
	tu_dbg("release EV_WAIT event\n");
	event_release(hdl[0]);
    }
    if (global > 0) {
	tu_dbg("register new EV_WAIT event with same ID\n");
	hdl[0] = event_register(84, EV_WAIT, test_ev_wait_2_cb, NULL);
    }
}

static gboolean
test_ev_wait_2(void)
{
    global = 2;
    hdl[0] = event_register(84, EV_WAIT, test_ev_wait_2_cb, NULL);

    /* Each wakeup should only invoke the callback *once* */
    if (global != 2) return FALSE;
    event_wakeup(84);
    if (global != 1) return FALSE;
    event_wakeup(84);
    if (global != 0) return FALSE;
    event_wakeup(84); /* the handler has been removed, but this is not an error */
    if (global != 0) return FALSE;

    return TRUE;
}

/****
 * Test that event_wait correctly waits for a EV_TIME event to fire, even when
 * other events are running.  */
static void
test_event_wait_cb(void *up G_GNUC_UNUSED)
{
    int *cb_fired = (int *)up;
    (*cb_fired) = 1;

    /* immediately unregister ourselves */
    tu_dbg("test_event_wait_cb called\n");
    event_release(hdl[1]);
}

static gboolean
test_event_wait(void)
{
    int cb_fired = 0;
    global = 3;

    /* this one serves as a "decoy", running in the background while we wait
     * for test_event_wait_cb */
    hdl[0] = event_register(1, EV_TIME, test_decrement_cb, NULL);

    /* this is our own callback */
    hdl[1] = event_register(2, EV_TIME, test_event_wait_cb, (void *)&cb_fired);

    /* wait until our own callback fires */
    event_wait(hdl[1]);

    /* at this point, test_decrement_cb should have fired once or twice, but not
     * three times */
    if (global == 0) {
	tu_dbg("global is already zero!\n");
	return FALSE;
    }

    /* and our own callback should have fired */
    if (!cb_fired) {
	tu_dbg("test_event_wait_cb didn't fire\n");
	return FALSE;
    }

    return TRUE;
}

/****
 * Test that event_wait correctly waits for a EV_WAIT event to be released, not 
 * fired, even when other events are running.  */
static void
test_event_wait_2_cb(void *up)
{
    int *wakeups_remaining = (int *)up;
    tu_dbg("test_event_wait_2_cb called\n");

    if (--(*wakeups_remaining) == 0) {
	/* unregister ourselves if we've awakened enough times */
	event_release(hdl[2]);
	hdl[2] = NULL;
    }
}

static void
test_event_wait_2_wakeup_cb(void *up G_GNUC_UNUSED)
{
    tu_dbg("test_event_wait_2_wakeup_cb called\n");

    /* wake up the EV_WAIT event */
    event_wakeup(9876);
}

static gboolean
test_event_wait_2(void)
{
    int wakeups_remaining = 2;
    global = 3;

    /* this one serves as a "decoy", running in the background while we wait
     * for test_event_wait_2_cb */
    hdl[0] = event_register(1, EV_TIME, test_decrement_cb, NULL);

    /* This one repeatedly calls event_wakeup for the EV_WAIT event */
    hdl[1] = event_register(1, EV_TIME, test_event_wait_2_wakeup_cb, NULL);

    /* this is our own callback */
    hdl[2] = event_register(9876, EV_WAIT, test_event_wait_2_cb, (void *)&wakeups_remaining);

    /* wait until the EV_WAIT is *released*, not just fired. */
    event_wait(hdl[2]);

    /* at this point, test_decrement_cb should have fired twice, but not
     * three times */
    if (global == 0) {
	tu_dbg("global is already zero!\n");
	return FALSE;
    }

    /* and our own callback should have fired twice, not just once */
    if (wakeups_remaining != 0) {
	tu_dbg("test_event_wait_2_cb didn't fire twice\n");
	return FALSE;
    }

    return TRUE;
}

/****
 * Test that EV_READFD is triggered correctly when there's data available
 * for reading.  The source of read events is a spawned child which writes
 * lots of data to a pipe, in hopes of overflowing the pipe buffer.
 */
static void
test_ev_readfd_cb(void *up G_GNUC_UNUSED)
{
    char buf[1024];
    int len;

    /* read from the fd until we're out of bytes */
    tu_dbg("reader: callback executing\n");
    len = read(cb_fd, buf, sizeof(buf));
    if (len == 0) {
	tu_dbg("reader: callback returning\n");
    } else if (len < 0) {
	tu_dbg("reader: read() returned %d: %s\n", len, strerror(errno));
	/* do we need to handle e.g., EAGAIN here? */
    } else {
	tu_dbg("reader: read %d bytes\n", len);
	global -= len;
	/* release this event if we've read all of the available bytes */
	if (global <= 0) {
	    close(cb_fd);
	    event_release(hdl[0]);
	}
    }
}

static void
test_ev_readfd_writer(int fd, size_t count)
{
    char buf[256];
    size_t i;

    for (i = 0; i < sizeof(buf); i++) {
	buf[i] = (char)i;
    }

    while (count > 0) {
	int len;

	len = write(fd, buf, min(sizeof(buf), count));
	tu_dbg("writer wrote %d bytes\n", len);
	count -= len;
    }

    close(fd);
}

#define TEST_EV_READFD_SIZE (1024*1024)

static gboolean
test_ev_readfd(void)
{
    int writer_pid;
    int p[2];

    /* make a pipe */
    if (pipe(p) == -1) {
	exit(1);
    }

    /* fork off the writer */
    switch (writer_pid = fork()) {
	case 0: /* child */
	    close(p[0]);
	    test_ev_readfd_writer(p[1], TEST_EV_READFD_SIZE);
	    exit(0);
	    break;

	case -1: /* error */
	    perror("fork");
	    return FALSE;

	default: /* parent */
	    break;
    }

    /* set up a EV_READFD on the read end of the pipe */
    cb_fd = p[0];
    fcntl(cb_fd, F_SETFL, O_NONBLOCK);
    close(p[1]);
    global = TEST_EV_READFD_SIZE;
    hdl[0] = event_register(p[0], EV_READFD, test_ev_readfd_cb, NULL);

    /* let it run */
    event_loop(0);

    tu_dbg("waiting for writer to die..\n");
    waitpid(writer_pid, NULL, 0);

    if (global != 0) {
	tu_dbg("%d bytes remain unread..\n", global);
	return FALSE;
    }

    return TRUE;
}

/****
 * Test the combination of an EV_TIME and an EV_READFD to peform a 
 * timeout-protected read that times out.
 */
static void
test_read_timeout_slow_writer(int fd)
{
    char buf[] = "OH NO!";

    /* this should exceed the timeout, which is 1s */
    sleep(2);

    if (write(fd, buf, strlen(buf)+1) == -1) {
	exit(1);
    }
    close(fd);
}

static void
test_read_timeout_cb(void *up G_GNUC_UNUSED)
{
    tu_dbg("read timed out (this is supposed to happen)\n");
    global = 1234; /* sentinel value */

    /* free up all of the events so that event_loop returns */
    event_release(hdl[0]);
    event_release(hdl[1]);
}

static gboolean
test_read_timeout(void)
{
    int writer_pid;
    int p[2];

    /* make a pipe */
    if (pipe(p) == -1) {
	exit(1);
    }

    /* fork off the writer */
    switch (writer_pid = fork()) {
	case 0: /* child */
	    close(p[0]);
	    test_read_timeout_slow_writer(p[1]);
	    exit(0);
	    break;

	case -1: /* error */
	    perror("fork");
	    return FALSE;

	default: /* parent */
	    break;
    }

    /* set up a EV_READFD on the read end of the pipe */
    cb_fd = p[0];
    fcntl(cb_fd, F_SETFL, O_NONBLOCK);
    close(p[1]);
    hdl[0] = event_register(p[0], EV_READFD, test_ev_readfd_cb, NULL);

    /* and set up a timeout */
    global = 0;	/* timeout_cb will set this to 1234 */
    hdl[1] = event_register(1, EV_TIME, test_read_timeout_cb, NULL);

    /* let it run */
    event_loop(0);

    /* see if we got the sentinel indicating the timeout fired */
    if (global != 1234)
	return FALSE;

    return TRUE;
}

/****
 * Test that EV_WRITEFD is triggered correctly when there's buffer space to
 * support a write.  
 */

static void
test_ev_writefd_cb(void *up G_GNUC_UNUSED)
{
    char buf[1024];
    int len;
    unsigned int i;

    /* initialize the buffer to something worthwhile */
    for (i = 0; i < sizeof(buf); i++) {
	buf[i] = (char)i;
    }

    /* write some bytes, but no more than global */
    tu_dbg("test_ev_writefd_cb called\n");
    while (1) {
	len = write(cb_fd, buf, min((size_t)global, sizeof(buf)));
	if (len < 0) {
	    tu_dbg("test_ev_writefd_cb: write() returned %d\n", len);
	    return;
	} else if (len == 0) {
	    /* do we need to handle EAGAIN, etc. here? */
	    tu_dbg("test_ev_writefd_cb done\n");
	    return;
	}
	tu_dbg(" write() wrote %d bytes\n", len);
	global -= len;
	if (global <= 0) {
	    close(cb_fd);
	    event_release(hdl[0]);
	    return;
	}
    }
}

static void
test_ev_writefd_consumer(int fd, size_t count)
{
    while (count > 0) {
	char buf[1024];
	int len;

	tu_dbg("reader: calling read(%d)\n", (int)sizeof(buf));
	len = read(fd, buf, sizeof(buf));

	/* exit on a read error or EOF */
	if (len < 1) return;

	tu_dbg("reader: read() returned %d bytes\n", len);

	count -= len;
    }
}

#define TEST_EV_WRITEFD_SIZE (1024*1024)

static gboolean
test_ev_writefd(void)
{
    int reader_pid;
    int p[2];

    /* make a pipe */
    if (pipe(p) == -1) {
	exit(1);
    }

    /* fork off the reader */
    switch (reader_pid = fork()) {
	case 0: /* child */
	    close(p[1]);
	    test_ev_writefd_consumer(p[0], TEST_EV_WRITEFD_SIZE);
	    exit(0);
	    break;

	case -1: /* error */
	    perror("fork");
	    return FALSE;

	default: /* parent */
	    break;
    }

    /* set up a EV_WRITEFD on the write end of the pipe */
    cb_fd = p[1];
    fcntl(cb_fd, F_SETFL, O_NONBLOCK);
    global = TEST_EV_WRITEFD_SIZE;
    close(p[0]);
    hdl[0] = event_register(p[1], EV_WRITEFD, test_ev_writefd_cb, NULL);

    /* let it run */
    event_loop(0);

    tu_dbg("waiting for reader to die..\n");
    waitpid(reader_pid, NULL, 0);

    /* and see what we got */
    if (global != 0) {
	tu_dbg("writes did not complete\n");
	return FALSE;
    }

    return TRUE;
}

/****
 * Test that a child_watch_source works correctly.
 */

static gint test_child_watch_result = 0;
static GMainLoop *test_child_watch_main_loop = NULL;

static void
test_child_watch_callback(
    pid_t pid,
    gint status,
    gpointer data)
{
    static int count = 0;
    gint expected_pid = GPOINTER_TO_INT(data);

    if (pid != expected_pid
	    || !WIFEXITED(status)
	    || WEXITSTATUS(status) != 13)
	test_child_watch_result = FALSE;
    else
	test_child_watch_result = TRUE;

    count++;
    if(count >= 2)
	g_main_loop_quit(test_child_watch_main_loop);
}

static gboolean
test_child_watch_source(void)
{
    int pid, pid2;
    GSource *src, *src2;

    /* fork off the child we want to watch die */
    switch (pid = fork()) {
	case 0: /* child */
	    exit(13);
	    break;

	case -1: /* error */
	    perror("fork");
	    return FALSE;

	default: /* parent */
	    break;
    }

    /* set up a child watch */
    src = new_child_watch_source(pid);
    g_source_set_callback(src, (GSourceFunc)test_child_watch_callback,
	     GINT_TO_POINTER(pid), NULL);
    g_source_attach(src, NULL);
    g_source_unref(src);

    switch (pid2 = fork()) {
	case 0: /* child */
	    exit(13);
	    break;

	case -1: /* error */
	    perror("fork");
	    return FALSE;

	default: /* parent */
	    break;
    }

    sleep(1);
    /* set up a child watch */
    src2 = new_child_watch_source(pid2);
    g_source_set_callback(src2, (GSourceFunc)test_child_watch_callback,
	     GINT_TO_POINTER(pid2), NULL);
    g_source_attach(src2, NULL);
    g_source_unref(src2);

    /* let it run */
    test_child_watch_main_loop = g_main_loop_new(NULL, 1);
    g_main_loop_run(test_child_watch_main_loop);

    return test_child_watch_result;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_ev_time, 90),
	TU_TEST(test_ev_wait, 90),
	TU_TEST(test_ev_wait_2, 90),
	TU_TEST(test_ev_readfd, 120), /* runs slowly on old kernels */
	TU_TEST(test_ev_writefd, 90),
	TU_TEST(test_event_wait, 90),
	TU_TEST(test_event_wait_2, 90),
	TU_TEST(test_nonblock, 90),
	TU_TEST(test_read_timeout, 90),
	TU_TEST(test_child_watch_source, 90),
	/* fdsource is used by ev_readfd/ev_writefd, and is sufficiently tested there */
	TU_END()
    };

    return testutils_run_tests(argc, argv, tests);
}
