/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
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

#define MSG_HDR_LEN 10
#define ARG_HDR_LEN 6

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
    msg->n_args = cmd->n_args;
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
	size = strlen((gchar *)data);
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

static guint16
get_guint16(guint8 **p) {
    guint16 v = 0;
    v = *((*p)++);
    v = *((*p)++) | v << 8;
    return v;
}

static guint32
get_guint32(guint8 **p) {
    guint32 v = 0;
    v = *((*p)++);
    v = *((*p)++) | v << 8;
    v = *((*p)++) | v << 8;
    v = *((*p)++) | v << 8;
    return v;
}


ipc_binary_message_t *
ipc_binary_poll_message(
    ipc_binary_channel_t *chan)
{
    guint8 *p;
    ipc_binary_message_t *msg;
    guint16 magic;
    guint16 cmd_id;
    guint32 length;
    guint16 n_args;

    if (chan->in.length < MSG_HDR_LEN) {
	errno = 0;
	return NULL;
    }

    /* read out the pocket header, using shifts to avoid endian and alignment
     * problems, and checking each one as we proceed */
    p = (guint8 *)(chan->in.buf + chan->in.offset);

    magic = get_guint16(&p);

    if (magic != chan->proto->magic) {
	g_debug("ipc-binary got invalid magic 0x%04x", (int)magic);
	errno = EINVAL;
	return NULL;
    }

    cmd_id = get_guint16(&p);

    /* make sure this is a valid command */
    if (cmd_id <= 0 || cmd_id >= chan->proto->n_cmds
		            || !chan->proto->cmds[cmd_id].exists) {
	errno = EINVAL;
	return NULL;
    }

    length = get_guint32(&p);

    /* see if there's enough data in this buffer for a whole message */
    if (length > chan->in.length) {
	errno = 0;
	return NULL; /* whole packet isn't here yet */
    }

    n_args = get_guint16(&p);

    /* looks legit -- start building a message */
    msg = ipc_binary_new_message(chan, cmd_id);

    /* get each of the arguments */
    while (n_args--) {
	guint16 arg_id;
	guint32 arglen;

	arglen = get_guint32(&p);
	arg_id = get_guint16(&p);

	if (arg_id <= 0 || arg_id >= msg->cmd->n_args
		|| !(msg->cmd->arg_flags[arg_id] & IPC_BINARY_EXISTS)
		|| msg->args[arg_id].data != NULL) {
	    g_debug("ipc-binary invalid or duplicate arg");
	    errno = EINVAL;
	    ipc_binary_free_message(msg);
	    return NULL;
	}

	/* properly terminate string args, but do not include the nul byte in
	 * the arglen */
	if (msg->cmd->arg_flags[arg_id] & IPC_BINARY_STRING) {
	    gchar *data;

	    /* copy and terminate the string */
	    data = g_malloc(arglen+1);
	    g_memmove(data, p, arglen);
	    data[arglen] = '\0';
	    msg->args[arg_id].data = (gpointer)data;
	    msg->args[arg_id].len = arglen;
	} else {
	    msg->args[arg_id].data = g_memdup(p, arglen);
	    msg->args[arg_id].len = arglen;
	}

	p += arglen;
    }

    /* check that all mandatory args are here */
    if (!all_args_present(msg)) {
	errno = EINVAL;
	ipc_binary_free_message(msg);
	return NULL;
    }

    consume_from_buffer(&chan->in, length);

    return msg;
}

static guint8 *
put_guint16(guint8 *p, guint16 v)
{
    *(p++) = v >> 8;
    *(p++) = v;
    return p;
}

static guint8 *
put_guint32(guint8 *p, guint32 v)
{
    *(p++) = v >> 24;
    *(p++) = v >> 16;
    *(p++) = v >> 8;
    *(p++) = v;
    return p;
}

void
ipc_binary_queue_message(
    ipc_binary_channel_t *chan G_GNUC_UNUSED,
    ipc_binary_message_t *msg G_GNUC_UNUSED)
{
    gsize msg_len;
    guint8 *p;
    int i;
    guint16 n_args = 0;

    g_assert(all_args_present(msg));

    /* calculate the length and make enough room in the buffer */
    msg_len = MSG_HDR_LEN;
    for (i = 0; i < msg->cmd->n_args; i++) {
	if (msg->args[i].data) {
	    n_args++;
	    msg_len += msg->args[i].len + ARG_HDR_LEN;
	}
    }
    expand_buffer(&chan->out, msg_len);
    p = (guint8 *)(chan->out.buf + chan->out.offset);

    /* write the packet */
    p = put_guint16(p, chan->proto->magic);
    p = put_guint16(p, msg->cmd_id);
    p = put_guint32(p, msg_len);
    p = put_guint16(p, n_args);

    for (i = 0; i < msg->cmd->n_args; i++) {
	if (!msg->args[i].data)
	    continue;

	p = put_guint32(p, msg->args[i].len);
	p = put_guint16(p, i);

	g_memmove(p, msg->args[i].data, msg->args[i].len);
	p += msg->args[i].len;
    }
    chan->out.length += msg_len;

    ipc_binary_free_message(msg);
}
