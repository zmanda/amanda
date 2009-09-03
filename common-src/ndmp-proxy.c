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

static int ndmp_proxy_pid = -1;
static int ndmp_proxy_connected = FALSE;
static int ndmp_proxy_stdin = -1;

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
	ipc_binary_cmd_add_arg(cmd, NDMP_PROXY_USER_PASS, IPC_BINARY_STRING);
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
    }

    g_static_mutex_unlock(&mutex);
    return proto;
}

/* return  == NULL: correct
 *         != NULL: error message
 */
char *
start_ndmp_proxy(void)
{
    char      *ndmp_proxy;
    GPtrArray *proxy_argv;
    char       buffer[32769];
    int        proxy_in, proxy_out, proxy_err;
    int        rc;
    int        ndmp_proxy_port;
    int        ndmp_proxy_debug_level;
    char      *ndmp_proxy_debug_file;
    char      *errmsg;

    if (ndmp_proxy_connected) {
	return NULL;
    }
    ndmp_proxy_port = getconf_int(CNF_NDMP_PROXY_PORT);
    ndmp_proxy_debug_level = getconf_int(CNF_NDMP_PROXY_DEBUG_LEVEL);
    ndmp_proxy_debug_file = getconf_str(CNF_NDMP_PROXY_DEBUG_FILE);
    proxy_argv = g_ptr_array_new();
    g_ptr_array_add(proxy_argv, stralloc("ndmp-proxy"));
    g_ptr_array_add(proxy_argv, stralloc("-o"));
    g_ptr_array_add(proxy_argv, g_strdup_printf("proxy=%d", ndmp_proxy_port));
    if (ndmp_proxy_debug_level > 0 &&
	ndmp_proxy_debug_file && strlen(ndmp_proxy_debug_file) > 1) {
	g_ptr_array_add(proxy_argv, g_strdup_printf("-d%d",
						    ndmp_proxy_debug_level));
	g_ptr_array_add(proxy_argv, g_strdup_printf("-L%s",
						    ndmp_proxy_debug_file));
    }
    g_ptr_array_add(proxy_argv, NULL);
    proxy_err = debug_fd();
    ndmp_proxy = g_strdup_printf("%s/ndmp-proxy", amlibexecdir);

    ndmp_proxy_pid = pipespawnv(ndmp_proxy, STDIN_PIPE | STDOUT_PIPE, 0,
			        &proxy_in, &proxy_out, &proxy_err,
			        (char **)proxy_argv->pdata);
    ndmp_proxy_stdin = proxy_in;

    g_ptr_array_free_full(proxy_argv);
    rc = read(proxy_out, buffer, sizeof(buffer)-1);
    if (rc == -1) {
	errmsg = g_strdup_printf("Error reading from ndmp-proxy: %s",
				 strerror(errno));
	return errmsg;
    } else if (rc == 0) {
	errmsg = g_strdup_printf("ndmp-proxy ended unexpectedly");
	return errmsg;
    }
    buffer[rc] = '\0';
    if (strncmp(buffer, "PORT ", 5) != 0) {
	if (strcmp(buffer, "BIND: Address already in use\n") == 0) {
	    /* Another ndmp-proxy is running, amanda can connect to it */
	} else {
	    errmsg = g_strdup_printf("ndmp-proxy failed: %s", buffer);
	    return errmsg;
	}
    }
    ndmp_proxy_connected = TRUE;
    return NULL;
}

void
stop_ndmp_proxy(void)
{
    if (ndmp_proxy_stdin > 0) {
	aclose(ndmp_proxy_stdin);
    }
}

int
connect_to_ndmp_proxy(char **errmsg)
{
    int   count = 10;
    int   proxy_port;
    int   fd;

    proxy_port = getconf_int(CNF_NDMP_PROXY_PORT);

    while (count > 0) {
	*errmsg = start_ndmp_proxy();
	if (*errmsg) {
	    return -1;
	}

	g_debug("openning a connection to ndmp-proxy");
	fd = stream_client("localhost", proxy_port, 32768, 32768, NULL, 0);
	if (fd >= 0) {
	    g_debug("connected to ndmp-proxy: %d", fd);
	    return fd;
	}

	g_debug("Temporary failure to connect to ndmp-proxy: %s", strerror(errno));

	sleep(1);
	count--;
    }

    *errmsg = stralloc(_("failed to open a connection to ndmp-proxy"));
    return -1;
}

int
proxy_pid(void)
{
    return ndmp_proxy_pid;
}
