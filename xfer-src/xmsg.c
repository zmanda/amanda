/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
#include "amxfer.h"

/* TODO: use glib chunk allocator */

/*
 * Methods
 */

XMsg *
xmsg_new(
    XferElement *elt,
    xmsg_type type,
    int version)
{
    XMsg *msg = g_new0(XMsg, 1);
    msg->elt = elt;
    msg->type = type;
    msg->version = version;

    /* messages hold a reference to the XferElement, to avoid dangling
     * pointers. */
    g_object_ref((GObject *)elt);

    return msg;
}

void
xmsg_free(
    XMsg *msg)
{
    /* unreference the source */
    g_object_unref((GObject *)msg->elt);

    /* and free any allocated attributes */
    if (msg->repr) g_free(msg->repr);
    if (msg->message) g_free(msg->message);

    /* then free the XMsg itself */
    g_free(msg);
}

char *
xmsg_repr(
    XMsg *msg)
{
    if (!msg) return "(nil)"; /* better safe than sorry */

    /* this just shows the "header" fields for now */
    if (!msg->repr) {
	char *typ = NULL;
	switch (msg->type) {
	    case XMSG_INFO: typ = "INFO"; break;
	    case XMSG_ERROR: typ = "ERROR"; break;
	    case XMSG_DONE: typ = "DONE"; break;
	    case XMSG_CANCEL: typ = "CANCEL"; break;
	    case XMSG_PART_DONE: typ = "PART_DONE"; break;
	    case XMSG_READY: typ = "READY"; break;
	    case XMSG_CHUNK_DONE: typ = "CHUNK_DONE"; break;
	    case XMSG_CRC: typ = "CRC"; break;
	    case XMSG_NO_SPACE: typ = "NO_SPACE"; break;
	    case XMSG_SEGMENT_DONE: typ = "SEGMENT_DONE"; break;
	    default: typ = "**UNKNOWN**"; break;
	}

	msg->repr = g_strdup_printf("<XMsg@%p type=XMSG_%s elt=%s version=%d>",
	    msg, typ, xfer_element_repr(msg->elt), msg->version);
    }

    return msg->repr;
}
