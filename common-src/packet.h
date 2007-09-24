/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
/*
 * $Id: packet.h,v 1.8 2006/05/25 01:47:12 johnfranks Exp $
 *
 * interfaces for modifying amanda protocol packet type
 */
#ifndef PACKET_H
#define PACKET_H

typedef enum { P_REQ = 0, P_REP = 1, P_PREP = 2, P_ACK = 3, P_NAK = 4 } pktype_t;
typedef struct {
    pktype_t	type;			/* type of packet */
    char *	body;			/* body of packet */
    size_t	size;
    size_t	packet_size;
} pkt_t;

/*
 * Initialize a packet
 */
void pkt_init_empty(pkt_t *pkt, pktype_t type);
void pkt_init(pkt_t *, pktype_t, const char *, ...)
     G_GNUC_PRINTF(3,4);

/*
 * Append data to a packet
 */
void pkt_cat(pkt_t *, const char *, ...)
     G_GNUC_PRINTF(2,3);

/*
 * Convert the packet type to and from a string
 */
const char *pkt_type2str(pktype_t);
pktype_t pkt_str2type(const char *);

#endif /* PACKET_H */
