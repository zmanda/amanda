/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * $Id: packet.c,v 1.8 2006/05/25 01:47:12 johnfranks Exp $
 *
 * Routines for modifying the amanda protocol packet type
 */
#include "amanda.h"
#include "packet.h"

/*
 * Table of packet types and their printable forms
 */
static const struct {
    const char name[5];
    pktype_t type;
} pktypes[] = {
    { "REQ", P_REQ },
    { "REP", P_REP },
    { "PREP", P_PREP },
    { "ACK", P_ACK },
    { "NAK", P_NAK }
};
#define NPKTYPES G_N_ELEMENTS(pktypes)

/*
 * Initialize a packet
 */
void pkt_init_empty(
    pkt_t *pkt,
    pktype_t type)
{
    assert(pkt != NULL);
    assert(!g_str_equal(pkt_type2str(type), "BOGUS"));

    pkt->type = type;
    pkt->packet_size = 1000;
    pkt->body = g_malloc(pkt->packet_size);
    pkt->body[0] = '\0';
    pkt->size = strlen(pkt->body);
}

void pkt_init(pkt_t *pkt, pktype_t type, const char *fmt, ...)
{
    va_list	argp;
    int         len;

    assert(pkt != NULL);
    assert(!g_str_equal(pkt_type2str(type), "BOGUS"));
    if(fmt == NULL)
	fmt = "";

    pkt->type = type;
    pkt->packet_size = 1000;
    pkt->body = g_malloc(pkt->packet_size);
    while(1) {
	arglist_start(argp, fmt);
	len = g_vsnprintf(pkt->body, pkt->packet_size, fmt, argp);
	arglist_end(argp);
	if (len > -1 && len < (int)(pkt->packet_size - 1))
	    break;
	pkt->packet_size *= 2;
	amfree(pkt->body);
	pkt->body = g_malloc(pkt->packet_size);
    }
    pkt->size = strlen(pkt->body);
}

/*
 * Append data to a packet
 */
void pkt_cat(pkt_t *pkt, const char *fmt, ...)
{
    size_t	len;
    int         lenX;
    va_list	argp;
    char *	pktbody;

    assert(pkt != NULL);
    assert(fmt != NULL);

    len = strlen(pkt->body);

    while(1) {
	arglist_start(argp, fmt);
        lenX = g_vsnprintf(pkt->body + len, pkt->packet_size - len, fmt,argp);
	arglist_end(argp);
	if (lenX > -1 && lenX < (int)(pkt->packet_size - len - 1))
	    break;
	pkt->packet_size *= 2;
	pktbody = g_malloc(pkt->packet_size);
	strncpy(pktbody, pkt->body, len);
	pktbody[len] = '\0';
	amfree(pkt->body);
	pkt->body = pktbody;
    }
    pkt->size = strlen(pkt->body);
}

/*
 * Converts a string into a packet type
 */
pktype_t
pkt_str2type(
    const char *typestr)
{
    guint i;

    assert(typestr != NULL);

    for (i = 0; i < NPKTYPES; i++)
	if (g_str_equal(typestr, pktypes[i].name))
	    return (pktypes[i].type);
    return ((pktype_t)-1);
}

/*
 * Converts a packet type into a string
 */
const char *
pkt_type2str(
    pktype_t	type)
{
    guint i;

    for (i = 0; i < NPKTYPES; i++)
	if (pktypes[i].type == type)
	    return (pktypes[i].name);
    return ("BOGUS");
}
