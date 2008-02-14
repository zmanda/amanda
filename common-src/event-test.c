/*
 * Copyright (c) 2008 Zmanda Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */

#include "amanda.h"
#include "event.h"

/* a random global variable to flag that some function has been called */
static int global;

/* file descriptor under EV_READFD or EV_WRITEFD */
static int cb_fd;

/* and some easy access to the event handles for callbacks */
static event_handle_t *hdl[10];

/* Debug output (-d on the command line) */
static int dbgmsg = 0;

/*
 * Utils
 */

static void
timeout(int sig G_GNUC_UNUSED)
{
    fprintf(stderr, "-- TEST TIMED OUT --\n");
    exit(1);
}

/* Call testfn in a forked process, such that any failures will trigger a
 * test failure, but allow the other tests to proceed.
 */
static int
callinfork( int (*testfn)(void), char *testname)
{
    pid_t pid;
    int success;
    amwait_t status;

    switch (pid = fork()) {
	case 0:	/* child */
	    /* kill the test after 10s */
	    signal(SIGALRM, timeout);
	    alarm(10);

	    success = testfn();
	    exit(success? 0:1);

	case -1:
	    perror("fork");
	    exit(1);

	default: /* parent */
	    waitpid(pid, &status, 0);
	    if (status == 0) {
		fprintf(stderr, " PASS %s\n", testname);
	    } else {
		fprintf(stderr, " FAIL %s\n", testname);
	    }
	    return status == 0;
    }
}

/* A common event callback that just decrements 'global', and frees
 * hdl[0] if global reaches zero.
 */
static void
test_decrement_cb(void *up G_GNUC_UNUSED)
{
    global--;
    if (dbgmsg) fprintf(stderr, "Decrement global to %d\n", global);
    if (global == 0) {
	if (dbgmsg) fprintf(stderr, "Release event\n");
	event_release(hdl[0]);
    }
}

/*
 * Tests
 */

/****
 * Test that EV_TIME events fire, repeatedly.
 */
static int
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
static int
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
static int
test_ev_wait(void)
{
    global = 2;
    hdl[0] = event_register(4422, EV_WAIT, test_decrement_cb, NULL);

    if (global != 2) return 0;
    event_wakeup(4422);
    if (global != 1) return 0;
    event_wakeup(4422);
    if (global != 0) return 0;
    event_wakeup(4422); /* the handler has been removed, but this is not an error */
    if (global != 0) return 0;

    /* queue should now be empty, so this won't block */
    event_loop(0);

    return 1;
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
    if (dbgmsg) fprintf(stderr, "Decrement global to %d\n", global);

    if (global >= 0) {
	if (dbgmsg) fprintf(stderr, "release EV_WAIT event\n");
	event_release(hdl[0]);
    }
    if (global > 0) {
	if (dbgmsg) fprintf(stderr, "register new EV_WAIT event with same ID\n");
	hdl[0] = event_register(84, EV_WAIT, test_ev_wait_2_cb, NULL);
    }
}

static int
test_ev_wait_2(void)
{
    global = 2;
    hdl[0] = event_register(84, EV_WAIT, test_ev_wait_2_cb, NULL);

    /* Each wakeup should only invoke the callback *once* */
    if (global != 2) return 0;
    event_wakeup(84);
    if (global != 1) return 0;
    event_wakeup(84);
    if (global != 0) return 0;
    event_wakeup(84); /* the handler has been removed, but this is not an error */
    if (global != 0) return 0;

    return 1;
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
    if (dbgmsg) fprintf(stderr, "test_event_wait_cb called\n");
    event_release(hdl[1]);
}

static int
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
	if (dbgmsg) fprintf(stderr, "global is already zero!\n");
	return 0;
    }

    /* and our own callback should have fired */
    if (!cb_fired) {
	if (dbgmsg) fprintf(stderr, "test_event_wait_cb didn't fire\n");
	return 0;
    }

    return 1;
}

/****
 * Test that event_wait correctly waits for a EV_WAIT event to be released, not 
 * fired, even when other events are running.  */
static void
test_event_wait_2_cb(void *up)
{
    int *wakeups_remaining = (int *)up;
    if (dbgmsg) fprintf(stderr, "test_event_wait_2_cb called\n");

    if (--(*wakeups_remaining) == 0) {
	/* unregister ourselves if we've awakened enough times */
	event_release(hdl[2]);
	hdl[2] = NULL;
    }
}

static void
test_event_wait_2_wakeup_cb(void *up G_GNUC_UNUSED)
{
    if (dbgmsg) fprintf(stderr, "test_event_wait_2_wakeup_cb called\n");

    /* wake up the EV_WAIT event */
    event_wakeup(9876);
}

static int
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
	if (dbgmsg) fprintf(stderr, "global is already zero!\n");
	return 0;
    }

    /* and our own callback should have fired twice, not just once */
    if (wakeups_remaining != 0) {
	if (dbgmsg) fprintf(stderr, "test_event_wait_2_cb didn't fire twice\n");
	return 0;
    }

    return 1;
}

/****
 * Test that EV_READFD is triggered correctly when there's data available
 * for reading.  The source of read events is a spawned child which writes
 * bytes to a pipe, separated by sleep() calls.
 */
static void
test_ev_readfd_cb(void *up)
{
    char **string = (char **)up;
    char buf[1024];
    int len;

    if (dbgmsg) fprintf(stderr, "test_ev_readfd_cb called\n");
    /* read from the fd, putting each block read on its own line; release
     * when the last character read is 'Z'. */
    len = read(cb_fd, buf, sizeof(buf));
    if (len <= 0) {
	if (dbgmsg) fprintf(stderr, " read() returned %d\n", len);
	*string = newvstralloc(*string, (*string)?(*string):"", "(empty)\n", NULL);
    } else {
	buf[len] = '\0';
	if (dbgmsg) fprintf(stderr, " read() returned '%s'\n", buf);
	*string = newvstralloc(*string, (*string)?(*string):"", buf, "\n", NULL);

	if (buf[len-1] == 'Z') {
	    event_release(hdl[0]);
	}
    }
}

static void
test_ev_readfd_writer(int fd, char *lines)
{
    while (1) {
	char *p = lines;
	int len;

	while (*p && *p != '\n') p++;
	len = p-lines;

	if (dbgmsg) fprintf(stderr, "writing %d bytes\n", len);
	while (len)
	    len -= write(fd, p-len, len);

	if (!*p)
	    break;

	g_usleep(100000); /* sleep to prevent the OS from "bundling" our writes */

	lines = p+1;
    }

    close(fd);
}

static int
test_ev_readfd(void)
{
    int writer_pid;
    int p[2];
    char *text = "Fourscore and seven years ago\n"
		 "There comes a time in every man's life\n"
		 "This should end the transmission: Z\n";
    char *accumulator = NULL;

    /* make a pipe */
    pipe(p);

    /* fork off the writer */
    switch (writer_pid = fork()) {
	case 0: /* child */
	    close(p[0]);
	    test_ev_readfd_writer(p[1], text);
	    exit(0);
	    break;

	case -1: /* error */
	    perror("fork");
	    return 0;

	default: /* parent */
	    break;
    }

    /* set up a EV_READFD on the read end of the pipe */
    cb_fd = p[0];
    fcntl(cb_fd, F_SETFL, O_NONBLOCK);
    close(p[1]);
    hdl[0] = event_register(p[0], EV_READFD, test_ev_readfd_cb, &accumulator);

    /* let it run */
    event_loop(0);

    if (dbgmsg) fprintf(stderr, "waiting for writer to die..\n");
    waitpid(writer_pid, NULL, 0);

    /* and see what we got */
    if (!accumulator) {
	if (dbgmsg) fprintf(stderr, "no data was read\n");
	return 0;
    }

    if (strcmp(accumulator, text) != 0) {
	if (dbgmsg)
	    fprintf(stderr, "text differs; expected\n---\n%s\n--- but got ---\n%s\n---\n",
		    text, accumulator);
	return 0;
    }

    return 1;
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

    write(fd, buf, strlen(buf)+1);
    close(fd);
}

static void
test_read_timeout_cb(void *up G_GNUC_UNUSED)
{
    if (dbgmsg) fprintf(stderr, "read timed out (this is supposed to happen)\n");
    global = 1234; /* sentinel value */

    /* free up all of the events so that event_loop returns */
    event_release(hdl[0]);
    event_release(hdl[1]);
}

static int
test_read_timeout(void)
{
    int writer_pid;
    int p[2];

    /* make a pipe */
    pipe(p);

    /* fork off the writer */
    switch (writer_pid = fork()) {
	case 0: /* child */
	    close(p[0]);
	    test_read_timeout_slow_writer(p[1]);
	    exit(0);
	    break;

	case -1: /* error */
	    perror("fork");
	    return 0;

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
	return 0;

    return 1;
}

/****
 * Test that EV_WRITEFD is triggered correctly when there's buffer space to
 * support a write, by writing to a "slow" consumer.  If the buffers on this 
 * system are ridiculously large, then this may spuriously succeed. */

/* hopefully this is large enough to exceed the system's pipe buffer */
#define WRITE_CHUNK_SIZE (1024*1024)

static void
test_ev_writefd_cb(void *up G_GNUC_UNUSED)
{
    char buf[WRITE_CHUNK_SIZE];
    int len;
    unsigned int i;

    /* initialize the buffer to something worthwhile */
    for (i = 0; i < sizeof(buf); i++) {
	buf[i] = (char)i;
    }

    /* write some bytes, but no more than global */
    if (dbgmsg) fprintf(stderr, "test_ev_writefd_cb called\n");
    len = write(cb_fd, buf, min((size_t)global, sizeof(buf)));
    if (len <= 0) {
	if (dbgmsg) fprintf(stderr, " write() returned %d\n", len);
	return;
    } else {
	if (dbgmsg) fprintf(stderr, " write() wrote %d bytes\n", len);
    }

    /* and deduct those bytes from the number we're to write */
    global -= len;
    if (global <= 0) {
	close(cb_fd);
	event_release(hdl[0]);
    }
}

static void
test_ev_writefd_consumer(int fd)
{
    unsigned int nsleeps = 0;
    size_t bytes_read = 0;
    /* Read the data "slowly" and consume it. We read WRITE_CHUNK_SIZE per 0.1s */
    while (1) {
	char buf[WRITE_CHUNK_SIZE];
	int len;

	if (dbgmsg) fprintf(stderr, "reader: calling read(%d)\n", (int)sizeof(buf));
	len = read(fd, buf, sizeof(buf));

	/* exit on a read error or EOF */
	if (len < 1) return;

	if (dbgmsg) fprintf(stderr, "reader: read() returned %d bytes\n", len);

	/* now make sure we're not running too quickly.. */
	bytes_read += len;
	while (nsleeps < bytes_read * 10 / WRITE_CHUNK_SIZE) {
	    /* sleep 0.01s */
	    g_usleep(10000);
	    nsleeps++;
	}
    }
}

static int
test_ev_writefd(void)
{
    int reader_pid;
    int p[2];

    /* make a pipe */
    pipe(p);

    /* fork off the reader */
    switch (reader_pid = fork()) {
	case 0: /* child */
	    close(p[1]);
	    test_ev_writefd_consumer(p[0]);
	    exit(0);
	    break;
	
	case -1: /* error */
	    perror("fork");
	    return 0;
	
	default: /* parent */
	    break;
    }

    /* set up a EV_WRITEFD on the write end of the pipe */
    cb_fd = p[1];
    fcntl(cb_fd, F_SETFL, O_NONBLOCK);
    /* set the number of bytes to write, approx. length of tests in seconds */
    global = WRITE_CHUNK_SIZE * 5;
    close(p[0]);
    hdl[0] = event_register(p[1], EV_WRITEFD, test_ev_writefd_cb, NULL);

    /* let it run */
    event_loop(0);

    if (dbgmsg) fprintf(stderr, "waiting for reader to die..\n");
    waitpid(reader_pid, NULL, 0);

    /* and see what we got */
    if (global != 0) {
	if (dbgmsg) fprintf(stderr, "writes did not complete\n");
	return 0;
    }

    return 1;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    int success = 1;

    /* A '-d' flag turns on dbgmsg output */
    if (argc > 1 && strcmp(argv[1], "-d") == 0) {
	dbgmsg = 1;
    }

    success = callinfork(test_ev_time, "test_ev_time") && success;
    success = callinfork(test_ev_wait, "test_ev_wait") && success;
    success = callinfork(test_ev_wait_2, "test_ev_wait_2") && success;
    success = callinfork(test_ev_readfd, "test_ev_readfd") && success;
    success = callinfork(test_ev_writefd, "test_ev_writefd") && success;
    success = callinfork(test_event_wait, "test_event_wait") && success;
    success = callinfork(test_event_wait_2, "test_event_wait_2") && success;
    success = callinfork(test_nonblock, "test_nonblock") && success;
    success = callinfork(test_read_timeout, "test_read_timeout") && success;

    return success? 0:1;
}
