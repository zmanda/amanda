/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "ipc-binary.h"

struct ipc_binary_proto_t {
    guint16 magic;
    guint16 n_cmds;
    ipc_binary_cmd_t *cmds;
};

/* extra flag to indicate that an argument exists */
#define IPC_BINARY_EXISTS (1 << 7)

struct ipc_binary_cmd_t {
    gboolean exists;
    guint8 *arg_flags;
    guint16 n_args;
};

typedef struct msg_hdr_t {
    guint16 magic;
    guint16 cmd_id;
    guint32 length;
    guint16 n_args;
} msg_hdr_t;

typedef struct arg_hdr_t {
    guint32 length;
    guint16 arg_id;
} arg_hdr_t;

/*
 * Utilities
 */

static void
expand_buffer(
    ipc_binary_buf_t *buf,
    gsize size)
{
    gsize new_len = buf->length + size;

    /* allocate space in the buffer if necessary */
    if (buf->offset + new_len > buf->size) {
	if (buf->offset != 0 && new_len <= buf->size) {
	    g_memmove(buf->buf,
		      buf->buf + buf->offset,
		      buf->length);
	    buf->offset = 0;
	} else {
	    buf->size = buf->offset + new_len;
	    buf->buf = g_realloc(buf->buf, buf->size);
	}
    }
}

static void
add_to_buffer(
    ipc_binary_buf_t *buf,
    gsize size,
    gpointer data)
{
    expand_buffer(buf, size);

    g_memmove(buf->buf + buf->offset + buf->length, data, size);
    buf->length += size;
}

static void
consume_from_buffer(
    ipc_binary_buf_t *buf,
    gsize size)
{
    g_assert(size <= buf->length);

    buf->length -= size;
    if (buf->length == 0)
	buf->offset = 0;
    else
	buf->offset += size;
}

static gboolean
all_args_present(
    ipc_binary_message_t *msg)
{
    int i;

    for (i = 0; i < msg->cmd->n_args; i++) {
	if (msg->args[i].data == NULL
		&& (msg->cmd->arg_flags[i] & IPC_BINARY_EXISTS)
		&& !(msg->cmd->arg_flags[i] & IPC_BINARY_OPTIONAL)) {
	    g_debug("ipc-binary message missing mandatory arg %d", i);
	    return FALSE;
	}
    }

    return TRUE;
}

/*
 * Creating a protocol
 */

ipc_binary_proto_t *
ipc_binary_proto_new(
    guint16 magic)
{
    ipc_binary_proto_t *prot = g_new(ipc_binary_proto_t, 1);

    prot->magic = magic;
    prot->n_cmds = 0;
    prot->cmds = NULL;

    return prot;
}

ipc_binary_cmd_t *
ipc_binary_proto_add_cmd(
    ipc_binary_proto_t *proto,
    guint16 id)
{
    g_assert(proto != NULL);
    g_assert(id != 0);

    if (id >= proto->n_cmds) {
	guint16 new_len = id+1;
	int i;

	proto->cmds = g_renew(ipc_binary_cmd_t, proto->cmds, new_len);
	for (i = proto->n_cmds; i < new_len; i++) {
	    proto->cmds[i].n_args = 0;
	    proto->cmds[i].exists = FALSE;
	    proto->cmds[i].arg_flags = NULL;
	}
	proto->n_cmds = new_len;
    }

    /* make sure this command hasn't been defined already */
    g_assert(!proto->cmds[id].exists);
    proto->cmds[id].exists = TRUE;

    return &proto->cmds[id];
}

void
ipc_binary_cmd_add_arg(
    ipc_binary_cmd_t *cmd,
    guint16 id,
    guint8 flags)
{
    g_assert(cmd != NULL);
    g_assert(id != 0);
    flags |= IPC_BINARY_EXISTS;

    if (id >= cmd->n_args) {
	guint16 new_len = id+1;
	int i;

	cmd->arg_flags = g_realloc(cmd->arg_flags, new_len);
	for (i = cmd->n_args; i < new_len; i++) {
	    cmd->arg_flags[i] = 0;
	}
	cmd->n_args = new_len;
    }

    /* make sure this arg hasn't been defined already */
    g_assert(cmd->arg_flags[id] == 0);

    cmd->arg_flags[id] = flags;
}

/*
 * Using a protocol
 */

ipc_binary_channel_t *
ipc_binary_new_channel(
    ipc_binary_proto_t *proto)
{
    ipc_binary_channel_t *chan;

    chan = g_new0(ipc_binary_channel_t, 1);
    chan->proto = proto;

    return chan;
}

void
ipc_binary_free_channel(
    ipc_binary_channel_t *chan)
{
    if (chan->in.buf)
	g_free(chan->in.buf);

    if (chan->out.buf)
	g_free(chan->out.buf);

    g_free(chan);
}

ipc_binary_message_t *
ipc_binary_new_message(
    ipc_binary_channel_t *chan,
    guint16 cmd_id)
{
    ipc_binary_message_t *msg = g_new0(ipc_binary_message_t, 1);
    ipc_binary_cmd_t *cmd;

    /* make sure this is a valid command */
    g_assert(chan != NULL);
    g_assert(cmd_id > 0 && cmd_id < chan->proto->n_cmds);
    g_assert(chan->proto->cmds[cmd_id].exists);
    cmd = &chan->proto->cmds[cmd_id];

    msg->chan = chan;
    msg->cmd = cmd;
    msg->cmd_id = cmd_id;
    msg->args = g_malloc0(sizeof(*(msg->args)) * cmd->n_args);

    return msg;
}

void
ipc_binary_add_arg(
    ipc_binary_message_t *msg,
    guint16 arg_id,
    gsize size,
    gpointer data,
    gboolean take_memory)
{
    /* make sure this arg has not already been set for this message */
    g_assert(msg != NULL);
    g_assert(data != NULL);
    g_assert(arg_id > 0 && arg_id < msg->cmd->n_args);
    g_assert(msg->cmd->arg_flags[arg_id] & IPC_BINARY_EXISTS);
    g_assert(msg->args[arg_id].data == NULL);

    if (size == 0 && msg->cmd->arg_flags[arg_id] & IPC_BINARY_STRING) {
	size = strlen((gchar *)data)+1;
    }

    if (!take_memory) {
	data = g_memdup(data, size);
    }

    msg->args[arg_id].len = size;
    msg->args[arg_id].data = data;
}

void
ipc_binary_free_message(
    ipc_binary_message_t *msg)
{
    int i;

    g_assert(msg != NULL);

    for (i = 0; i < msg->cmd->n_args; i++) {
	gpointer data = msg->args[i].data;
	if (data)
	    g_free(data);
    }

    g_free(msg->args);
    g_free(msg);
}

ipc_binary_message_t *
ipc_binary_read_message(
    ipc_binary_channel_t *chan,
    int fd)
{
    ipc_binary_message_t *msg;

    /* read data until we have a whole packet or until EOF */
    while (!(msg = ipc_binary_poll_message(chan))) {
	gssize bytes;

	if (errno)
	    return NULL;

	/* read directly into the buffer, instead of using ipc_binary_feed_data */
	expand_buffer(&chan->in, 32768);
	bytes = read(fd, chan->in.buf + chan->in.offset + chan->in.length, 32768);
	if (bytes < 0) {
	    /* error on read */
	    return NULL;
	} else if (!bytes) {
	    /* got EOF; if there are bytes left over, this is EIO */
	    if (chan->in.length) {
		g_warning("got EOF reading ipc-binary channel with %zd bytes un-processed",
			  chan->in.length);
		errno = EIO;
	    }

	    return NULL;
	} else {
	    /* add the data to the buffer */
	    chan->in.length += bytes;
	}
    }

    return msg;
}

int
ipc_binary_write_message(
    ipc_binary_channel_t *chan,
    int fd,
    ipc_binary_message_t *msg)
{
    gsize written;

    /* add the message to the queue */
    ipc_binary_queue_message(chan, msg);

    /* and write the outgoing buffer */
    written = full_write(fd, chan->out.buf + chan->out.offset, chan->out.length);
    consume_from_buffer(&chan->out, written);

    if (written < chan->out.length) {
	return -1;
    }

    return 0;
}

void
ipc_binary_feed_data(
    ipc_binary_channel_t *chan,
    gsize size,
    gpointer data)
{
    add_to_buffer(&chan->in, size, data);
}

void
ipc_binary_data_transmitted(
    ipc_binary_channel_t *chan,
    gsize size)
{
    consume_from_buffer(&chan->out, size);
}

ipc_binary_message_t *
ipc_binary_poll_message(
    ipc_binary_channel_t *chan)
{
    msg_hdr_t msg_hdr;
    arg_hdr_t arg_hdr;
    gpointer ptr;
    ipc_binary_message_t *msg;

    if (chan->in.length < sizeof(msg_hdr)) {
	errno = 0;
	return NULL;
    }

    /* get the packet header, using memmove to avoid alignment problems,
     * then fix byte order */
    g_memmove(&msg_hdr, chan->in.buf + chan->in.offset, sizeof(msg_hdr));
    msg_hdr.magic = ntohs(msg_hdr.magic);
    msg_hdr.cmd_id = ntohs(msg_hdr.cmd_id);
    msg_hdr.length = ntohl(msg_hdr.length);
    msg_hdr.n_args = ntohs(msg_hdr.n_args);

    /* check the magic */
    if (msg_hdr.magic != chan->proto->magic) {
	g_debug("ipc-binary got invalid magic 0x%04x", (int)msg_hdr.magic);
	errno = EINVAL;
	return NULL;
    }

    /* see if there's enough data in this buffer for a whole message */
    if (msg_hdr.length > chan->in.length) {
	errno = 0;
	return NULL; /* whole packet isn't here yet */
    }

    /* make sure this is a valid command */
    if (msg_hdr.cmd_id <= 0 || msg_hdr.cmd_id >= chan->proto->n_cmds
		            || !chan->proto->cmds[msg_hdr.cmd_id].exists) {
	errno = EINVAL;
	return NULL;
    }
    msg = ipc_binary_new_message(chan, msg_hdr.cmd_id);

    /* get each of the arguments */
    ptr = chan->in.buf + chan->in.offset + sizeof(msg_hdr);
    while (msg_hdr.n_args--) {
	g_memmove(&arg_hdr, ptr, sizeof(arg_hdr));
	arg_hdr.length = ntohl(arg_hdr.length);
	arg_hdr.arg_id = ntohs(arg_hdr.arg_id);
	ptr += sizeof(arg_hdr);

	if (arg_hdr.arg_id <= 0 || arg_hdr.arg_id >= msg->cmd->n_args
		|| !(msg->cmd->arg_flags[arg_hdr.arg_id] & IPC_BINARY_EXISTS)
		|| msg->args[arg_hdr.arg_id].data != NULL) {
	    g_debug("ipc-binary invalid or duplicate arg");
	    errno = EINVAL;
	    ipc_binary_free_message(msg);
	    return NULL;
	}

	/* check that a string arg is terminated */
	if (msg->cmd->arg_flags[arg_hdr.arg_id] & IPC_BINARY_STRING) {
	    if (((gchar *)ptr)[arg_hdr.length-1] != '\0') {
		g_debug("ipc-binary got unterminated string arg");
		errno = EINVAL;
		ipc_binary_free_message(msg);
		return NULL;
	    }
	}

	msg->args[arg_hdr.arg_id].len = arg_hdr.length;
	msg->args[arg_hdr.arg_id].data = g_memdup(ptr, arg_hdr.length);

	ptr += arg_hdr.length;
    }

    /* check that all mandatory args are here */
    if (!all_args_present(msg)) {
	errno = EINVAL;
	ipc_binary_free_message(msg);
	return NULL;
    }

    consume_from_buffer(&chan->in, msg_hdr.length);

    return msg;
}

void
ipc_binary_queue_message(
    ipc_binary_channel_t *chan,
    ipc_binary_message_t *msg)
{
    gsize msg_len;
    msg_hdr_t msg_hdr;
    arg_hdr_t arg_hdr;
    gpointer p;
    int i;

    g_assert(all_args_present(msg));

    msg_hdr.magic = htons(chan->proto->magic);
    msg_hdr.cmd_id = htons(msg->cmd_id);
    msg_len = sizeof(msg_hdr);
    msg_hdr.n_args = 0;

    for (i = 0; i < msg->cmd->n_args; i++) {
	if (msg->args[i].data) {
	    msg_hdr.n_args++;
	    msg_len += msg->args[i].len + sizeof(arg_hdr);
	}
    }
    msg_hdr.length = htonl(msg_len);
    msg_hdr.n_args = htons(msg_hdr.n_args);

    expand_buffer(&chan->out, msg_len);
    chan->out.length += msg_len;
    p = chan->out.buf + chan->out.offset;

    g_memmove(p, &msg_hdr, sizeof(msg_hdr));
    p += sizeof(msg_hdr);

    for (i = 0; i < msg->cmd->n_args; i++) {
	if (!msg->args[i].data)
	    continue;

	arg_hdr.length = htonl(msg->args[i].len);
	arg_hdr.arg_id = htons(i);

	g_memmove(p, &arg_hdr, sizeof(arg_hdr));
	p += sizeof(arg_hdr);

	g_memmove(p, msg->args[i].data, msg->args[i].len);
	p += msg->args[i].len;
    }

    ipc_binary_free_message(msg);
}
