/*
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

#ifndef XMSG_H
#define XMSG_H

#include <glib.h>
#include "xfer-element.h"

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
 * The implementation of messages is intended to sacrifice memory consumption
 * for speed and serializability.  Relatively few messages will exist at any
 * one time, but over the course of a dump, many messages will be created,
 * serialized, unserialized, and processed.
 */

/*
 * Message types
 */

/* N.B. -- when adding new message types, add the corresponding case label
 * to xfer-src/xmsg.c:xmsg_repr() and perl/Amanda/Xfer.swg */
typedef enum {
    /* XMSG_INFO: informational messages suitable for display to user. Attributes:
     *  - message
     */
    XMSG_INFO = 1,

    /* XMSG_ERROR: error message from an element.  Attributes:
     *  - message
     */
    XMSG_ERROR = 2,

    /* XMSG_DONE: the transfer is done.  Only one XMSG_DONE message will be
     * delivered, when all elements are finished.
     * Attributes:
     *  (none)
     */
    XMSG_DONE = 3,

    /* XMSG_CANCEL: this transfer is being cancelled, but data may still be
     * "draining" from buffers.  A subsequent XMSG_DONE indicates that the
     * transfer has actually completed.
     * Attributes:
     *  (none)
     */
    XMSG_CANCEL = 4,

    /* XMSG_PART_DONE: a split part is finished; used by XferDestTaper and
     * XferSourceTaper elements to indicate that the element is paused and
     * awaiting instructions to start a new part.  Not all of the attributes
     * are applicable to both elements.
     *
     * Attributes:
     *  - successful (true if the whole part was written; always false for
     *		XferSourceTaper)
     *  - eom (true if the device is at EOM; always false for XferSourceTaper)
     *  - eof (recipient should not call start_part; always false for
     *		XferSourceTaper)
     *  - size (bytes written to or read from the volume)
     *  - duration (time spent writing, not counting changer ops, etc.)
     *  - partnum (the zero-based number of this part in the overall
     *		dumpfile; always 0 for XferSourceTaper)
     *  - fileno (the on-media file number used for this part, or 0 if no file
     *		  was used)
     */
    XMSG_PART_DONE = 5,

    /* XMSG_READY: some elements do some additional, potentially long-term
     * startup operations after the xfer itself starts.  This message is used
     * to indicate that the startup was successful.
     *
     * Attributes:
     *  (none)
     */
    XMSG_READY = 6,
} xmsg_type;

/*
 * Class Declaration
 */

typedef struct XMsg {
    /* General header information */

    /* the origin of the message */
    struct XferElement *elt;

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
     * is advised for strings.
     *
     * NOTE TO IMPLEMENTERS:
     *
     * When adding a new attribute, make changes in the following places:
     *  - add the attribute to the XMsg struct in xmsg.h
     *  - add the attribute to the comments for the appropriate xmsg_types
     *  - free the attribute in xmsg_free.
     *  - edit perl/Amanda/Xfer.swg:new_sv_for_xmsg
     */

    /* free-form string message for display to the users
     *
     * This string is always valid UTF-8.  If it contains pathnames from a
     * bytestring-based filesystem, then non-ASCII bytes will be encoded using
     * quoted-printable.
     */
    char *message;

    /* true indicates a successful operation */
    gboolean successful;

    /* true if an EOM condition has occurred */
    gboolean eom;

    /* true if an EOF condition has occurred */
    gboolean eof;

    /* size, in bytes */
    guint64 size;

    /* duration, in seconds */
    double duration;

    /* split-part number */
    guint64 partnum;

    /* file number on a volume */
    guint64 fileno;
} XMsg;

/*
 * Methods
 */

/* Create a new XMsg.
 *
 * @param elt: element originating this message
 * @param type: message type
 * @param version: message version
 * @return: a new XMsg.
 */
XMsg *xmsg_new(
    XferElement *elt,
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

#endif
