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
#include "conffile.h"
#include "pipespawn.h"
#include "stream.h"
#include "ndmp-proxy.h"

ipc_binary_proto_t *
get_ndmp_proxy_proto(void)
{
    static ipc_binary_proto_t *proto = NULL;
    static GStaticMutex mutex = G_STATIC_MUTEX_INIT;

    g_static_mutex_lock(&mutex);
    if (!proto) {
	ipc_binary_cmd_t *cmd;

	proto = ipc_binary_proto_new(0xC74F);

	/* Note that numbers here (NDMP_PROXY_COUNT) are represented as
	 * strings.
	 *
	 * NDMP_PROXY_SERVICE is one of "DEVICE", "APPLICATION", or "CHANGER".
	 *
	 * Error codes are stringified versions of the relevant NDMP constants,
	 * e.g., NDMP9_ILLEGAL_ARGS_ERR.  NDMP_PROXY_ERROR is just a string.  Where
	 * both are provided in a reply, either both args are omitted or both are
	 * present.
	 */

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_SELECT_SERVICE);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_SERVICE, IPC_BINARY_STRING);

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_REPLY_GENERIC);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_ERRCODE, IPC_BINARY_STRING | IPC_BINARY_OPTIONAL);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_ERROR, IPC_BINARY_STRING | IPC_BINARY_OPTIONAL);

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_TAPE_OPEN);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_FILENAME, IPC_BINARY_STRING);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_MODE, IPC_BINARY_STRING);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_HOST, IPC_BINARY_STRING);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_PORT, IPC_BINARY_STRING);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_USERNAME, IPC_BINARY_STRING);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_PASSWORD, IPC_BINARY_STRING);
	/* ndmp-proxy gives generic reply */

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_TAPE_CLOSE);
	/* ndmp-proxy gives generic reply */

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_TAPE_MTIO);
	/* COMMAND can be one of "REWIND" or "EOF" */
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_COMMAND, IPC_BINARY_STRING);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_COUNT, IPC_BINARY_STRING);
	/* ndmp-proxy gives generic reply */

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_TAPE_WRITE);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_DATA, 0);
	/* ndmp-proxy gives generic reply */

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_TAPE_READ);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_COUNT, IPC_BINARY_STRING);

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_REPLY_TAPE_READ);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_DATA, IPC_BINARY_OPTIONAL);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_ERRCODE, IPC_BINARY_STRING | IPC_BINARY_OPTIONAL);
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_ERROR, IPC_BINARY_STRING | IPC_BINARY_OPTIONAL);

	cmd = ipc_binary_proto_add_cmd(proto, NDMP_PROXY_CMD_NOOP);
	/* ndmp-proxy gives generic reply */
    }

    g_static_mutex_unlock(&mutex);
    return proto;
}

/* return  == NULL: correct
 *         != NULL: error message
 *
 * The ndmp-proxy is assumed to take a configuration name and a port number
 * on its command line, and to immediately attempt to bind to that port.  If
 * the bind operation succeeds, it should print "OK\n" to stdout and close the
 * file descriptor.  If the operation fails because the address is already
 * in use (and thus, most likely, there's another proxy running already), then
 * it should print "INUSE\n", close the file descriptor, and exit.  For any other
 * failure, it should print a suitable error message and exit.
 */
static char *
start_ndmp_proxy(void)
{
    char      *ndmp_proxy;
    GPtrArray *proxy_argv;
    char       buffer[32769];
    int        proxy_in, proxy_out, proxy_err;
    int        rc;
    pid_t      pid;
    char      *errmsg;
    amwait_t   wait_status;

    proxy_argv = g_ptr_array_new();
    g_ptr_array_add(proxy_argv, g_strdup("ndmp-proxy"));
    g_ptr_array_add(proxy_argv, g_strdup(get_config_name()));
    g_ptr_array_add(proxy_argv, g_strdup_printf("%d", getconf_int(CNF_NDMP_PROXY_PORT)));
    g_ptr_array_add(proxy_argv, NULL);
    ndmp_proxy = g_strdup_printf("%s/ndmp-proxy", amlibexecdir);

    proxy_in = open("/dev/null", O_RDONLY);
    proxy_err = debug_fd();
    pid = pipespawnv(ndmp_proxy, STDOUT_PIPE, 0,
			        &proxy_in, &proxy_out, &proxy_err,
			        (char **)proxy_argv->pdata);

    close(proxy_in);
    g_ptr_array_free_full(proxy_argv);
    g_debug("started ndmp-proxy with pid %d", pid);

    /* wait for the proxy to say "OK" */
    rc = full_read(proxy_out, buffer, sizeof(buffer)-1);
    if (rc == -1) {
	errmsg = g_strdup_printf("Error reading from ndmp-proxy: %s",
				 strerror(errno));
	/* clean up the PID if possible */
	waitpid(pid, NULL, WNOHANG);
	return errmsg;
    } else if (rc == 0) {
	if (waitpid(pid, &wait_status, WNOHANG)) {
	    errmsg = str_exit_status("ndmp-proxy", wait_status);
	} else {
	    errmsg = g_strdup_printf("unexpected EOF from ndmp-proxy");
	}
	return errmsg;
    }

    aclose(proxy_out);

    /* process the output */
    buffer[rc] = '\0';
    if (0 == strcmp(buffer, "OK\n")) {
	return NULL;
    } else if (0 == strcmp(buffer, "INUSE\n")) {
	g_warning("overlapping attempts to start ndmp-proxy; ignoring this attempt");
	/* clean up the pid */
	waitpid(pid, NULL, 0);
	return NULL;
    } else {
	errmsg = g_strdup_printf("ndmp-proxy failed: %s", buffer);
	return errmsg;
    }
}

int
connect_to_ndmp_proxy(char **errmsg)
{
    int   i;
    int   proxy_port;
    int   fd;

    *errmsg = NULL;
    proxy_port = getconf_int(CNF_NDMP_PROXY_PORT);

    if (proxy_port == 0) {
	*errmsg = g_strdup("no NDMP-PROXY-PORT configured; cannot start NDMP proxy");
	return -1;
    }

    /* we loop until getting a successful connection, either from a proxy we
     * launched or a proxy another process launched.  We only do this a few
     * times, though, in case there's some problem starting the proxy, or
     * something already running on that port. */
    for (i = 0; i < 3; i++) {
	ipc_binary_channel_t *proxy_chan = NULL;
	ipc_binary_message_t *msg = NULL;

	g_debug("openning a connection to ndmp-proxy on port %d", proxy_port);
	fd = stream_client("localhost", proxy_port, 32768, 32768, NULL, 0);
	if (fd < 0) {
	    g_debug("Could not connect to ndmp-proxy: %s", strerror(errno));
	    goto try_to_start;
	}

	g_debug("connected to ndmp-proxy; sending NOOP to test connection");

	proxy_chan = ipc_binary_new_channel(get_ndmp_proxy_proto());
	msg = ipc_binary_new_message(proxy_chan, NDMP_PROXY_CMD_NOOP);
	if (ipc_binary_write_message(proxy_chan, fd, msg) < 0) {
	    g_debug("Error writing to ndmp-proxy: %s", strerror(errno));
	    goto try_to_start;
	}

	if (!(msg = ipc_binary_read_message(proxy_chan, fd))) {
	    g_debug("Error reading from ndmp-proxy: %s", strerror(errno));
	    goto try_to_start;
	}

	if (msg->cmd_id != NDMP_PROXY_REPLY_GENERIC) {
	    g_debug("Got bogus response from ndmp-proxy");
	    goto try_to_start;
	}

	ipc_binary_free_message(msg);
	ipc_binary_free_channel(proxy_chan);

	return fd;

try_to_start:
	if (msg) {
	    ipc_binary_free_message(msg);
	    msg = NULL;
	}
	if (proxy_chan) {
	    ipc_binary_free_channel(proxy_chan);
	    proxy_chan = NULL;
	}

	*errmsg = start_ndmp_proxy();
	if (*errmsg)
	    return -1;
    }

    if (!*errmsg) 
	*errmsg = stralloc(_("failed to open a connection to ndmp-proxy"));
    return -1;
}
