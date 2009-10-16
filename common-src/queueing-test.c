/*
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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
#include "glib-util.h"
#include "queueing.h"

/****
 * Test a simple queue
 */

struct test_queue_simple_data {
    size_t bytes_to_produce;

    /* The counters here are used to produce a slowly changing 
     * bytesequence, which should not align with block or buffer
     * boundaries. */
    size_t bytes_produced;
    guint32 producer_counter;

    size_t bytes_consumed;
    guint32 consumer_counter;
};

static producer_result_t
test_queue_simple_producer(
    gpointer data,
    queue_buffer_t *buffer,
    size_t hint_size)
{
    struct test_queue_simple_data *d = (struct test_queue_simple_data *)data;
    size_t to_write = hint_size;
    size_t i;

    /* just for fun, write a little bit more sometimes */
    to_write += d->producer_counter % 50;

    /* but not too much */
    if (to_write > d->bytes_to_produce - d->bytes_produced)
	to_write = d->bytes_to_produce - d->bytes_produced;

    /* make sure the buffer is big enough */
    if (buffer->data == NULL) {
	buffer->data = g_malloc(to_write);
	buffer->alloc_size = to_write;
    } else if (buffer->alloc_size < to_write) {
	buffer->data = g_realloc(buffer->data, to_write);
	buffer->alloc_size = to_write;
    }
    /* g_debug("Producing %zd bytes in %p (@%p)", to_write, buffer, buffer->data); */

    /* fill in the data with some random junk */
    for (i = 0; i < to_write; i++) {
	buffer->data[i] = (char)(d->producer_counter / 7 + (d->producer_counter >> 10));
	d->producer_counter++;
    }

    /* and call it a day */
    buffer->offset = 0;
    buffer->data_size = to_write;
    d->bytes_produced += to_write;
    return d->bytes_produced >= d->bytes_to_produce?
	 PRODUCER_FINISHED
       : PRODUCER_MORE;
}

static ssize_t
test_queue_simple_consumer(
    gpointer data,
    queue_buffer_t *buffer)
{
    struct test_queue_simple_data *d = (struct test_queue_simple_data *)data;
    size_t to_read = buffer->data_size;
    size_t i;

    g_assert(buffer->data != NULL);
    g_assert(buffer->data_size != 0);

    /* let's not read it all, to make sure that we get called back with the
     * remainder */
    to_read = buffer->data_size;
    if (to_read > 1000) to_read = 1000;

    /* verify the contents of the buffer */
    /* g_debug("Consuming %zd bytes starting at %d in %p (@%p)", to_read, buffer->offset, buffer, buffer->data); */
    for (i = 0; i < to_read; i++) {
	char expected = d->consumer_counter / 7 + (d->consumer_counter >> 10);
	if (buffer->data[buffer->offset + i] != expected) {
	    tu_dbg("expected %d, but got %d at byte position %zd\n",
		(int)expected, buffer->data[i], i);
	    return -1;
	}
	d->consumer_counter++;
    }
    d->bytes_consumed += to_read;

    return to_read;
}

static int
test_queue_simple(StreamingRequirement sr)
{
    queue_result_flags qr;
    gboolean success = TRUE;

    struct test_queue_simple_data d = {
	10*1024*1024, /* bytes_to_produce */
	0, /* bytes_produced */
	0, /* producer_counter */
	0, /* bytes_consumed */
	0 /* consumer_counter */
    };

    qr = do_consumer_producer_queue_full(
	test_queue_simple_producer, (gpointer)&d,
	test_queue_simple_consumer, (gpointer)&d,
	10230, /* almost 10k */
	3*1024*1024, /* 3M */
	sr);

    if (qr != QUEUE_SUCCESS) {
	tu_dbg("Expected result QUEUE_SUCCESS (%d); got %d\n",
	    QUEUE_SUCCESS, qr);
	success = FALSE;
    }

    if (d.bytes_produced != d.bytes_to_produce) {
	tu_dbg("Expected to produce %zd bytes; produced %zd\n",
	    d.bytes_to_produce, d.bytes_produced);
	success = FALSE;
    }

    if (d.bytes_consumed != d.bytes_to_produce) {
	tu_dbg("Expected to consume %zd bytes; consumed %zd\n",
	    d.bytes_to_produce, d.bytes_consumed);
	success = FALSE;
    }

    return success;
}

static int
test_queue_simple_STREAMING_REQUIREMENT_NONE(void)
{
    return test_queue_simple(STREAMING_REQUIREMENT_NONE);
}

static int
test_queue_simple_STREAMING_REQUIREMENT_DESIRED(void)
{
    return test_queue_simple(STREAMING_REQUIREMENT_DESIRED);
}

static int
test_queue_simple_STREAMING_REQUIREMENT_REQUIRED(void)
{
    return test_queue_simple(STREAMING_REQUIREMENT_REQUIRED);
}

/****
 * Test fd_reader and fd_writer
 */

#define TEST_FD_CONSUMER_PRODUCER_BLOCKS (1024)

static gpointer
data_producer_thread(gpointer d)
{
    int fd = GPOINTER_TO_INT(d);
    char buf[1024];
    size_t i;
    int block;

    /* fill in the buffer with some stuff */
    for (i = 0; i < (int)sizeof(buf); i++) {
	buf[i] = (char)i;
    }

    /* and write it out in blocks */
    for (block = 0; block < TEST_FD_CONSUMER_PRODUCER_BLOCKS; block++) {
	size_t written = 0;
	while (written < sizeof(buf)) {
	    int len = write(fd, buf + written, sizeof(buf) - written);
	    if (len < 0) {
		perror("writing pipe to fd_read_producer");
		close(fd);
		return GINT_TO_POINTER(0);
	    }
	    written += len;
	}
    }

    close(fd);
    return GINT_TO_POINTER(1);
}

static gpointer
data_consumer_thread(gpointer d)
{
    int fd = GPOINTER_TO_INT(d);
    char buf[1024];
    size_t i;
    int block;

    /* and read it in in blocks */
    for (block = 0; block < TEST_FD_CONSUMER_PRODUCER_BLOCKS; block++) {
	size_t bytes_read = 0;
	while (bytes_read < sizeof(buf)) {
	    int len = read(fd, buf + bytes_read, sizeof(buf) - bytes_read);
	    if (len < 0) {
		perror("reading pipe from fd_write_consumer");
		return NULL;
	    }
	    bytes_read += len;
	}

	/* verify the block */
	for (i = 0; i < (int)sizeof(buf); i++) {
	    if (buf[i] != (char)i) {
		tu_dbg("result data does not match input; block %d byte %zd", block, i);
		close(fd);
		return GINT_TO_POINTER(0);
	    }
	}
    }

    close(fd);
    return GINT_TO_POINTER(1);
}

static int
test_fd_consumer_producer(void)
{
    gboolean success;
    GThread *rth, *wth;
    int input_pipe[2];
    int output_pipe[2];
    queue_fd_t queue_read = {0, NULL};
    queue_fd_t queue_write = {0, NULL};

    /* create pipes and hook up threads to them */
    if (pipe(input_pipe) < 0) {
	perror("pipe(input_pipe)");
	return FALSE;
    }
    if (pipe(output_pipe) < 0) {
	perror("pipe(output_pipe)");
	return FALSE;
    }

    wth = g_thread_create(data_producer_thread, GINT_TO_POINTER(input_pipe[1]), TRUE, NULL);
    rth = g_thread_create(data_consumer_thread, GINT_TO_POINTER(output_pipe[0]), TRUE, NULL);

    /* run the queue */
    queue_read.fd = input_pipe[0];
    queue_write.fd = output_pipe[1];
    success = do_consumer_producer_queue(
	fd_read_producer, &queue_read,
	fd_write_consumer, &queue_write);
    if (!success)
	tu_dbg("do_consumer_producer_queue returned FALSE");

    /* and examine the results */
    success = GPOINTER_TO_INT(g_thread_join(wth)) && success;
    success = GPOINTER_TO_INT(g_thread_join(rth)) && success;

    /* close stuff up */
    close(input_pipe[0]);
    close(input_pipe[1]);
    close(output_pipe[0]);
    close(output_pipe[1]);

    return success;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_queue_simple_STREAMING_REQUIREMENT_NONE, 90),
	TU_TEST(test_queue_simple_STREAMING_REQUIREMENT_DESIRED, 90),
	TU_TEST(test_queue_simple_STREAMING_REQUIREMENT_REQUIRED, 90),
	TU_TEST(test_fd_consumer_producer, 120), /* runs slowly on old kernels */
	TU_END()
    };

    glib_init();

    return testutils_run_tests(argc, argv, tests);
}
