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

#include "amxfer.h"
#include "glib-util.h"
#include "testutils.h"
#include "amanda.h"
#include "event.h"

/*
 * Tests
 */

/****
 * Test a simple message delivery
 */
static gboolean test_xmsg_simple_got_msg = FALSE;

static gboolean
test_xmsg_simple_sendmsg(
    gpointer p)
{
    XMsgSource *xms = (XMsgSource *)p;
    XferElement *elt = xfer_source_random(100*1024, TRUE, MECH_OUTPUT_HAVE_READ_FD);
    XMsg *msg = xmsg_new(elt, XMSG_INFO, 13);
    msg->message = stralloc("Seems to work");

    /* verify it looks the way we want it to look */
    g_return_val_if_fail(msg->src == elt, 0);
    g_return_val_if_fail(msg->type == XMSG_INFO, 0);
    g_return_val_if_fail(msg->version == 13, 0);

    /* and send it */
    xmsgsource_queue_message(xms, msg);

    return FALSE;
}

static void
test_xmsg_simple_rxmsg(
    gpointer data G_GNUC_UNUSED, 
    XMsg *msg)
{
    tu_dbg("Received message %s\n", xmsg_repr(msg));

    if (msg->type != XMSG_INFO) {
	tu_dbg("msg->type is %d, should be %d\n", msg->type, XMSG_INFO);
	return;
    }

    if (msg->version != 13) {
	tu_dbg("msg->version is %d, should be 13\n", msg->version);
	return;
    }

    if (!msg->message) {
	tu_dbg("msg->message is null\n");
	return;
    }

    if (strcmp(msg->message, "Seems to work") != 0) {
	tu_dbg("msg->message is %s\n", msg->message);
	return;
    }

    /* OK, we got it */
    test_xmsg_simple_got_msg = TRUE;

    /* and kill the mainloop */
    g_main_loop_quit(default_main_loop());
}

static int
test_xmsg_simple(void)
{
    XMsgSource *xms;
    xms = xmsgsource_new();
    g_source_attach((GSource *)xms, NULL);

    /* queue up a timeout to send a message */
    g_timeout_add(20, test_xmsg_simple_sendmsg, (gpointer)xms);

    /* and an XMsgSource to receive it */
    xmsgsource_set_callback(xms, test_xmsg_simple_rxmsg, (gpointer)xms);

    /* now run the mainloop */
    test_xmsg_simple_got_msg = FALSE;
    g_main_loop_run(default_main_loop());
    return test_xmsg_simple_got_msg;
}

/****
 * Run a transfer testing all permutations of file-descriptor-based linkage
 */
static void
test_xfer_simple_callback(
    gpointer data G_GNUC_UNUSED,
    XMsg *msg,
    Xfer *xfer)
{
    tu_dbg("Received message %s\n", xmsg_repr(msg));

    switch (msg->type) {
	case XMSG_DONE:
	    /* are we done? */
	    if (xfer_get_status(xfer) == XFER_DONE) {
		tu_dbg("all elements are done!\n");
		g_main_loop_quit(default_main_loop());
	    }
	    break;

	default:
	    break;
    }
}

static int
test_xfer_simple(void)
{
    XferElement *elements[] = {
	xfer_source_random(100*1024, TRUE, MECH_OUTPUT_HAVE_READ_FD),
	/* output has fd, input reads it */
	xfer_filter_xor(MECH_INPUT_READ_GIVEN_FD, MECH_OUTPUT_WRITE_GIVEN_FD, 'd'),
	/* output writes to fd given by input */
	xfer_filter_xor(MECH_INPUT_HAVE_WRITE_FD, MECH_OUTPUT_WRITE_GIVEN_FD, 'd'),
	/* output writes to fd, input reads from a different fd */
	xfer_dest_null(MECH_INPUT_READ_GIVEN_FD, tu_debugging_enabled)
	/* TODO: output writes to fd, input reads from a different fd 
	 * (requires consumer/producer internally) */
    };

    Xfer *xfer = xfer_new(elements, sizeof(elements)/sizeof(*elements));
    xfer_set_callback(xfer, test_xfer_simple_callback, NULL);
    tu_dbg("Transfer: %s\n", xfer_repr(xfer));

    xfer_start(xfer);

    g_main_loop_run(default_main_loop());
    g_assert(xfer_get_status(xfer) == XFER_DONE);

    xfer_free(xfer);

    return 1;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_xmsg_simple, 10),
	TU_TEST(test_xfer_simple, 10),
	TU_END()
    };

    glib_init();

    return testutils_run_tests(argc, argv, tests);
}
