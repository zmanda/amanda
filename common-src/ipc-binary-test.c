/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */

#include "amanda.h"
#include "testutils.h"
#include "glib-util.h"
#include "ipc-binary.h"

enum {
    MY_PROTO_CMD1 = 1,
    MY_PROTO_CMD2 = 2,
};

enum {
    MY_PROTO_HOSTNAME = 1,
    MY_PROTO_DISK = 2,
    MY_PROTO_DATA = 3,
};

struct proto_and_fd {
    ipc_binary_proto_t *proto;
    int fd;
};

/*
 * Test a basic synchronous conversation between two endpoints.  Because
 * sync is implemented in terms of the async functions, this tests all of
 * the module's functionality.
 */
static gpointer
test_sync_child(gpointer d)
{
    struct proto_and_fd *data = (struct proto_and_fd *)d;
    ipc_binary_proto_t *proto = data->proto;
    ipc_binary_channel_t *chan;
    ipc_binary_message_t *msg;
    int fd = data->fd;

    g_usleep(G_USEC_PER_SEC / 8);

    chan = ipc_binary_new_channel(proto);
    msg = ipc_binary_new_message(chan, MY_PROTO_CMD1);
    ipc_binary_add_arg(msg, MY_PROTO_HOSTNAME, 0, "localhost", 0);
    if (ipc_binary_write_message(chan, fd, msg) < 0) return NULL;

    g_usleep(G_USEC_PER_SEC / 8);

    msg = ipc_binary_new_message(chan, MY_PROTO_CMD1);
    ipc_binary_add_arg(msg, MY_PROTO_HOSTNAME, 0, "otherhost", 0);
    ipc_binary_add_arg(msg, MY_PROTO_DISK, 0, "/usr", 0);
    if (ipc_binary_write_message(chan, fd, msg) < 0) return NULL;

    msg = ipc_binary_new_message(chan, MY_PROTO_CMD2);
    ipc_binary_add_arg(msg, MY_PROTO_DATA, 9, "some-data", 0);
    if (ipc_binary_write_message(chan, fd, msg) < 0) return NULL;

    return GINT_TO_POINTER(1);
}

static int
test_sync_parent(ipc_binary_proto_t *proto, int fd)
{
    ipc_binary_channel_t *chan;
    ipc_binary_message_t *msg;

    chan = ipc_binary_new_channel(proto);
    tu_dbg("parent: created channel\n");

    msg = ipc_binary_read_message(chan, fd);
    tu_dbg("parent: read message 1\n");
    if (msg->cmd_id != MY_PROTO_CMD1) {
	tu_dbg("got bad cmd_id %d\n", (int)msg->cmd_id);
	return 0;
    }
    if (msg->args[MY_PROTO_HOSTNAME].data == NULL) {
	tu_dbg("got NULL hostname\n");
	return 0;
    }
    if (0 != strcmp((gchar *)msg->args[MY_PROTO_HOSTNAME].data, "localhost")) {
	tu_dbg("got bad hostname %s\n", (gchar *)msg->args[MY_PROTO_HOSTNAME].data);
	return 0;
    }
    if (msg->args[MY_PROTO_DISK].data != NULL) {
	tu_dbg("got non-NULL disk\n");
	return 0;
    }
    ipc_binary_free_message(msg);

    msg = ipc_binary_read_message(chan, fd);
    tu_dbg("parent: read message 2\n");
    if (msg->cmd_id != MY_PROTO_CMD1) {
	tu_dbg("got bad cmd_id %d\n", (int)msg->cmd_id);
	return 0;
    }
    if (msg->args[MY_PROTO_HOSTNAME].data == NULL) {
	tu_dbg("got NULL hostname\n");
	return 0;
    }
    if (0 != strcmp((gchar *)msg->args[MY_PROTO_HOSTNAME].data, "otherhost")) {
	tu_dbg("got bad hostname %s\n", (gchar *)msg->args[MY_PROTO_HOSTNAME].data);
	return 0;
    }
    if (msg->args[MY_PROTO_DISK].data == NULL) {
	tu_dbg("got NULL disk\n");
	return 0;
    }
    if (0 != strcmp((gchar *)msg->args[MY_PROTO_DISK].data, "/usr")) {
	tu_dbg("got bad disk %s\n", (gchar *)msg->args[MY_PROTO_DISK].data);
	return 0;
    }
    ipc_binary_free_message(msg);

    g_usleep(G_USEC_PER_SEC / 8);

    msg = ipc_binary_read_message(chan, fd);
    tu_dbg("parent: read message 3\n");
    if (msg->cmd_id != MY_PROTO_CMD2) {
	tu_dbg("got bad cmd_id %d\n", (int)msg->cmd_id);
	return 0;
    }
    if (msg->args[MY_PROTO_DATA].data == NULL) {
	tu_dbg("got NULL data\n");
	return 0;
    }
    if (msg->args[MY_PROTO_DATA].len != 9) {
	tu_dbg("got data length %d, expected 9\n", (int)msg->args[MY_PROTO_DATA].len);
	return 0;
    }
    if (0 != strncmp((gchar *)msg->args[MY_PROTO_DATA].data, "some-data", 9)) {
	tu_dbg("got bad data\n");
	return 0;
    }
    ipc_binary_free_message(msg);

    return 1;
}

static int
test_sync(void)
{
    int rv;
    int p[2];
    ipc_binary_proto_t *proto;
    ipc_binary_cmd_t *cmd;
    struct proto_and_fd data;
    GThread *child;

    if (pipe(p) == -1) {
	perror("pipe");
	return 0;
    }

    proto = ipc_binary_proto_new(0xE10E);
    cmd = ipc_binary_proto_add_cmd(proto, MY_PROTO_CMD1);
    ipc_binary_cmd_add_arg(cmd, MY_PROTO_HOSTNAME, IPC_BINARY_STRING);
    ipc_binary_cmd_add_arg(cmd, MY_PROTO_DISK, IPC_BINARY_STRING|IPC_BINARY_OPTIONAL);
    cmd = ipc_binary_proto_add_cmd(proto, MY_PROTO_CMD2);
    ipc_binary_cmd_add_arg(cmd, MY_PROTO_DATA, 0);

    /* start the child thread */
    data.proto = proto;
    data.fd = p[1];
    child = g_thread_create(test_sync_child, &data, TRUE, NULL);

    /* run the parent and collect the results */
    rv = test_sync_parent(proto, p[0]) && GPOINTER_TO_INT(g_thread_join(child));
    return rv;
}

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_sync, 60),
	TU_END()
    };

    glib_init();
    return testutils_run_tests(argc, argv, tests);
}
