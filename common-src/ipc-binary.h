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

#ifndef IPC_BINARY_H
#define IPC_BINARY_H

#include "amanda.h"

/* This module implements bidirectional message-oriented protocols which use
 * binary framing, allowing it to transmit significant quantities of binary
 * data efficiently, at the cost of not being easily human-readable.
 *
 * A protocol is a set of messages (identified by distinct small integers),
 * each of which has a variable number of arguments, identified by other small,
 * nonzero integers which are unique within a particular message.  Protocols
 * are assumed to be known completely by both sides of a conversation -- no
 * allowance is made for communication between different "versions" of a
 * protocol.  Arguments are limited to 2^32-1 bytes (just under 4 MB), and each
 * message is similarly limited to a total of 2^32-1 bytes, including all
 * protocol framing.  Users are advised to choose smaller sizes (e.g., 2MB) for
 * data blocks transmitted within arguments.
 *
 * On the wire, each message consists of a 16-bit magic number, followed by a
 * 16-bit command id, a 32-bit message length, and a 32-bit argument count:
 *
 *   +--------|--------|--------|--------+
 *   |      magic      |     command     |
 *   +--------|--------|--------|--------+
 *   |              length               |
 *   +-----------------|-----------------+
 *   |     arg count   | ...
 *   +-----------------+
 *
 * All integers are in network byte order, and the message length includes the
 * length of the header.  This header is followed by a sequence of argument
 * records, each of which consists of a 32-bit length followed by a 16-bit
 * argument id and the corresponding data.  String arguments do not include the
 * NUL terminator byte.  Note that the argument length does not include the
 * argument header.
 *
 *   +--------|--------|--------|--------+
 *   |              length               |
 *   +--------|--------|--------|--------+
 *   |      argid      |      data       |
 *   +-----------------------------------+
 *   |              data...              |
 *   +-----------------------------------+
 */

/* To define a protocol, begin by enumerating the relevant message identifiers
 * and argument identifiers.  Then write an initialization function on the
 * following format: */
#if 0
enum {
    MY_PROTO_BACKUP = 1,
    MY_PROTO_RESTORE = 2,
};

enum {
    MY_PROTO_HOSTNAME = 1,
    MY_PROTO_DISK = 2,
    MY_PROTO_LEVEL = 3,
    MY_PROTO_FILENAMES = 4,
};

ipc_binary_proto_t *
my_proto(void)
{
    static ipc_binary_proto_t *proto = NULL;
    if (!proto) {
        ipc_binary_cmd_t *cmd;

	proto = ipc_binary_proto_new(0xFACE);

	cmd = ipc_binary_proto_add_cmd(proto, MY_PROTO_BACKUP);
        ipc_binary_cmd_add_arg(cmd, MY_PROTO_HOSTNAME, IPC_BINARY_STRING);
        ipc_binary_cmd_add_arg(cmd, MY_PROTO_DISK, IPC_BINARY_STRING);
        ipc_binary_cmd_add_arg(cmd, MY_PROTO_LEVEL, IPC_BINARY_STRING | IPC_BINARY_OPTIONAL);

	cmd = ipc_binary_proto_add_cmd(proto, MY_PROTO_RESTORE);
        ipc_binary_cmd_add_arg(cmd, MY_PROTO_HOSTNAME, IPC_BINARY_STRING);
        ipc_binary_cmd_add_arg(cmd, MY_PROTO_DISK, IPC_BINARY_STRING);
        ipc_binary_cmd_add_arg(cmd, MY_PROTO_FILENAMES, 0);
    }

    return proto;
}
#endif
/* Invoke my_proto in a thread-safe manner if necessary.
 *
 * Note that all of the constants are one-based.  Internally, the module uses these values as
 * array indices, so the constants should be assigned sequentially.  Although C specifies that
 * enumerations will auto-increment, it is best to add explicit values to avoid accidentally
 * changing protocol values between revisions.
 */

/*
 * Creating a new protocol
 */

/* opaque types */
typedef struct ipc_binary_proto_t ipc_binary_proto_t;
typedef struct ipc_binary_cmd_t ipc_binary_cmd_t;

/* Create a new, empty protocol object
 *
 * @param magic: magic number used to identify this protocol
 * @returns: proto object
 */
ipc_binary_proto_t *ipc_binary_proto_new(
    guint16 magic);

/* Create a new command in a protocol.  The resulting object is only
 * valid until the next call to this function for this proto.
 *
 * @param proto: the ipc_proto_t object to which to add this command
 * @param id: the nonzero identifier for this command
 * @returns: a new command object, already linked to PROTO
 */
ipc_binary_cmd_t *ipc_binary_proto_add_cmd(
    ipc_binary_proto_t *proto,
    guint16 id);

/* Flags for arguments */

/* This argument contains a string of non-null, printable characters and should
 * be displayed in debugging messages.  Arguments of this type will have a
 * terminating NUL byte in the ipc_binary_message_t args array, for
 * convenience, but will not count that byte in the length. */
#define IPC_BINARY_STRING               (1 << 0)

/* This argument may be omitted */
#define IPC_BINARY_OPTIONAL             (1 << 1)

/* Add an argument to a command
 *
 * @param cmd: the command object
 * @param id: the argument identifier
 * @param flags: bit flags for the command (see above)
 */
void ipc_binary_cmd_add_arg(
    ipc_binary_cmd_t *cmd,
    guint16 id,
    guint8 flags);

/*
 * Using a protocol
 */

typedef struct ipc_binary_buf_t {
    gpointer buf;
    gsize size;
    gsize offset;
    gsize length;
} ipc_binary_buf_t;

/* A channel represents a running protocol conversation, and encapsulates
 * buffers for incoming and outgoing data, as well as a pointer to the protocol
 * in use. */
typedef struct ipc_binary_channel_t {
    /* protocol for this channel */
    ipc_binary_proto_t *proto;

    /* buffers for incoming and outgoing data */
    ipc_binary_buf_t in, out;
} ipc_binary_channel_t;

/* Create a new channel, ready to send and receive messages.
 *
 * @param proto: protocol to use on this channel
 * @returns: a new channel object
 */
ipc_binary_channel_t *ipc_binary_new_channel(
    ipc_binary_proto_t *proto);

/* Free a channel completely.
 *
 * @param channel: the channel to free
 */
void ipc_binary_free_channel(
    ipc_binary_channel_t *channel);

/* message format; use the argument id as an index into the args array.  If
 * DATA is NULL, then the argument wasn't present. */

typedef struct ipc_binary_message_t {
    ipc_binary_channel_t *chan;
    guint16 cmd_id;
    ipc_binary_cmd_t *cmd;
    guint16 n_args;

    struct {
	gsize len;
	gpointer data;
    } *args;
} ipc_binary_message_t;

/* Create a new, blank message which will later be sent.
 *
 * @param chan: the channel the message will be sent on
 * @param cmd: the command id for this message
 * @returns: new message struct
 */
ipc_binary_message_t *ipc_binary_new_message(
    ipc_binary_channel_t *chan,
    guint16 cmd_id);

/* Add an argument to a message.  If the argument was defined with
 * IPC_BINARY_STRING, then the size will be calculated using strlen.  If
 * TAKE_MEMORY is true, then this module takes ownership of the memory and will
 * free it (with g_free) when the message is freed; otherwise, it will copy the
 * data.
 *
 * @param msg: the message to change
 * @param arg: the argument ID
 * @param size: the argument size
 * @param data: the argument data
 * @param take_memory: take ownership of memory if TRUE
 */
void ipc_binary_add_arg(
    ipc_binary_message_t *msg,
    guint16 arg,
    gsize size,
    gpointer data,
    gboolean take_memory);

/* Free a message structure (including all associated memory)
 *
 * @param msg: message to free
 */
void ipc_binary_free_message(
    ipc_binary_message_t *msg);

/* Synchronous interface
 *
 * This interface assumes that communication takes place over file
 * descriptors, and that blocking on network I/O is OK.  It is much
 * simpler than the asynchronous interface (below).
 */

/* Get the next message from the channel, optionally blocking until such a
 * message is received.  Returns NULL on EOF or on a protcol error.  Errno is
 * set to 0 for EOF, and contains an appropriate code for any other error.
 *
 * @param chan: channel on which to wait for a message
 * @param fd: file descriptor to read from
 * @returns: the message or NULL
 */
ipc_binary_message_t *ipc_binary_read_message(
    ipc_binary_channel_t *chan,
    int fd);

/* Send the given message, blocking until it is completely transmitted.
 * This function automatically frees the message.  Returns -1 on error,
 * with errno set appropriately, or 0 on success.
 *
 * @param chan: channel on which to send the message
 * @param fd: file descriptor to write to
 * @param msg: message to send
 */
int ipc_binary_write_message(
    ipc_binary_channel_t *chan,
    int fd,
    ipc_binary_message_t *msg);

/* Asynchronous interface
 *
 * This interface places data into and extracts data out of the buffers
 * in a channel, but leaves it to the caller to handle sending and receiving
 * data. */

/* Feed the given data into the channel.  Call this when new data is
 * available, and then check ipc_binary_poll_message(..) for any completed
 * messages.
 *
 * @param chan: channel into which to feed data
 * @param size: size of DATA
 * @param data: the new data
 */
void ipc_binary_feed_data(
    ipc_binary_channel_t *chan,
    gsize size,
    gpointer data);

/* Signal that some bytes have been transmitted and need not be kept in the
 * outgoing buffer any longer
 *
 * @param chan: channel from which bytes were sent
 * @param size: number of bytes transmitted
 */
void ipc_binary_data_transmitted(
    ipc_binary_channel_t *chan,
    gsize size);

/* Return the next complete incoming message in the channel, or NULL if there
 * are no complete messages available.  This also returns NULL on an invalid
 * message, with errno set appropriately.
 *
 * @param chan: channel to poll
 * @returns: message or NULL
 */
ipc_binary_message_t *ipc_binary_poll_message(
    ipc_binary_channel_t *chan);

/* Queue the given message for later transmission.  This function will free the
 * message once it is in the outgoing data buffer.  It is up to the caller to
 * ensure that the
 *
 * @param chan: the channel to feed
 * @param msg: the message to send
 */
void ipc_binary_queue_message(
    ipc_binary_channel_t *chan,
    ipc_binary_message_t *msg);

#endif /* IPC_BINARY_H */
