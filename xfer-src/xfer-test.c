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
    unsigned int i;
    GSource *src;
    XferElement *elements[] = {
	xfer_source_random(100*1024, TRUE, MECH_OUTPUT_HAVE_READ_FD),
	/* output has fd, input reads it */
	xfer_filter_xor('d', MECH_INPUT_READ_GIVEN_FD, MECH_OUTPUT_WRITE_GIVEN_FD),
	/* output writes to fd given by input */
	xfer_filter_xor('d', MECH_INPUT_HAVE_WRITE_FD, MECH_OUTPUT_WRITE_GIVEN_FD),
	/* output writes to fd, input reads from a different fd */
	xfer_dest_null(tu_debugging_enabled, MECH_INPUT_READ_GIVEN_FD)
	/* TODO: output writes to fd, input reads from a different fd 
	 * (requires consumer/producer internally) */
    };

    Xfer *xfer = xfer_new(elements, sizeof(elements)/sizeof(*elements));
    src = xfer_get_source(xfer);
    g_source_set_callback(src, (GSourceFunc)test_xfer_simple_callback, NULL, NULL);
    g_source_attach(src, NULL);
    tu_dbg("Transfer: %s\n", xfer_repr(xfer));

    /* unreference the elements */
    for (i = 0; i < sizeof(elements)/sizeof(*elements); i++) {
	g_object_unref(elements[i]);
	g_assert(G_OBJECT(elements[i])->ref_count == 1);
	elements[i] = NULL;
    }

    xfer_start(xfer);

    g_main_loop_run(default_main_loop());
    g_assert(xfer_get_status(xfer) == XFER_DONE);

    xfer_unref(xfer);

    return 1;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_xfer_simple, 10),
	TU_END()
    };

    glib_init();

    return testutils_run_tests(argc, argv, tests);
}
