/*
 * Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as
 * published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#ifndef XMSG_H
#define XMSG_H

#include <glib.h>

/* This module handles transmission of discrete messages from transfer
 * elements to the transfer master.  Messages have:
 *   an overall type (xmsg_msg_type)
 *   a version number
 *   a source (an XferElement)
 *   a number of attributes containing the message data
 *
 * Extensions to the protocol may add key/value pairs at any time without
 * affecting old implementations (that is, it is never an error to ignore
 * an unrecognized key).  Changes to the meaning of, or removal of,
 * existing keys from a message requires a version increment, which will
 * preclude older implementations from processing the message.  External
 * measures (such as amfeatures) should be employed in this case to ensure
 * backward compatibility.
 *
 * Note that all integer values provided in these enumerations are used in
 * the on-the-wire protocol, and should not be changed.
 *
 * The implementation of messages is intended to sacrifice memory consumption
 * for speed and serializability.  Relatively few messages will exist at any
 * one time, but over the course of a dump, many messages will be created,
 * serialized, unserialized, and processed.
 */

/*
 * Message types
 */

/* N.B. -- when adding new message types, add the corresponding case label
 * to xfer-src/xmsg.c:xmsg_repr() */
typedef enum {
    /* XMSG_INFO: informational messages suitable for display to user. Attributes:
     *  - message
     */
    XMSG_INFO = 1,

    /* XMSG_ERROR: error message from the source element.  This indicates that the
     * transfer as a whole has failed and should be aborted.  Attributes:
     *  - message
     */
    XMSG_ERROR = 2,

    /* XMSG_DONE: the source element has finished transmitting data.
     * Attributes:
     *  (none)
     */
    XMSG_DONE = 3,
} xmsg_type;

/*
 * Class Declaration
 */

typedef struct XMsg {
    /* General header information */

    /* the origin of the message */
    struct XferElement *src;

    /* the message's overall type */
    xmsg_type type;

    /* the message's version number */
    int version;

    /* internal use only; use xmsg_repr() to get the representation */
    char *repr;

    /* Attributes. Many of these will be zero or null.  See the xmsg_type
     * enumeration for a description of the attributes that are set for each
     * message type.
     *
     * Note that any pointer-based attributes (strings, etc.) become owned
     * by the XMsg object, and will be freed in xmsg_free.  The use of stralloc()
     * is advised for strings. */

    /* free-form string message for display to the users
     *
     * This string is always valid UTF-8.  If it contains pathnames from a
     * bytestring-based filesystem, then non-ASCII bytes will be encoded using
     * quoted-printable.
     */
    char *message;
} XMsg;

/*
 * Methods
 */

/* Create a new XMsg.
 *
 * @param src: element originating this message
 * @param type: message type
 * @param version: message version
 * @return: a new XMsg.
 */
XMsg *xmsg_new(
    XferElement *src,
    xmsg_type type,
    int version);

/* Free all memory associated with an XMsg.
 *
 * @param msg: the XMsg
 */
void xmsg_free(XMsg *msg);

/* Return a printable representation of an XMsg.  This representation
 * is stored with the message, and will be freed when the message is
 * freed.
 *
 * @param msg: the XMsg
 * @return: string representation
 */
char *xmsg_repr(XMsg *msg);

/*
 * XMsgSource -- a GSource for receiving messages
 */

/* XMsgSource objects are GSource "subclasses" which manage
 * a queue of messages, delivering those messages via callback
 * in the mainloop.  Messages can be *sent* from any thread without
 * any concern for locking, but must only be received in the main
 * thread, in the default GMainContext.
 *
 * An XMsgSource pointer can be cast to a GSource pointer as
 * necessary.
 */
typedef void (*XMsgCallback)(gpointer data, XMsg *msg);
typedef struct XMsgSource {
    GSource source; /* must be the first element of the struct */
    GAsyncQueue *queue;
} XMsgSource;

/* Create a new XMsgSource
 *
 * @return: new XMsgSource object
 */
XMsgSource *xmsgsource_new(void);

/* Destroy an XMsgSource.  All messages still in the queue are discarded,
 * and any subsequently queued messages will be leaked.
 *
 * @param xms: XMsgSource object
 */
void xmsgsource_destroy(XMsgSource *xms);

/* Set the callback for an XMsgSource
 *
 * @param xms: message source
 * @param callback: callback function
 * @param data: user data to be supplied to callback
 */
void xmsgsource_set_callback(
    XMsgSource *xms,
    XMsgCallback callback,
    gpointer data);

/* Queue a message for delivery via an XMsgSource.  The caller
 * may not reference the message after calling this function.
 *
 * @param xms: message source
 * @param msg: the message
 */
void xmsgsource_queue_message(
    XMsgSource *xms,
    XMsg *msg);

#endif
