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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: dgram.h,v 1.15 2006/05/25 01:47:11 johnfranks Exp $
 *
 * interface for datagram module
 */
#ifndef DGRAM_H
#define DGRAM_H

#include "amanda.h"

/*
 * Maximum datagram (UDP packet) we can generate.  Size is limited by
 * a 16 bit length field in an IPv4 header (65535), which must include
 * the 24 byte IP header and the 8 byte UDP header.
 */
#define MAX_DGRAM      (((1<<16)-1)-24-8)

typedef struct dgram_s {
    char *cur;
    int socket;
    size_t len;
    char data[MAX_DGRAM+1];
} dgram_t;

int	dgram_bind(dgram_t *dgram, sa_family_t family, in_port_t *portp);
void	dgram_socket(dgram_t *dgram, int sock);
int	dgram_send_addr(sockaddr_union *addr, dgram_t *dgram);
ssize_t	dgram_recv(dgram_t *dgram, int timeout,
		   sockaddr_union *fromaddr);
void	dgram_zero(dgram_t *dgram);
int	dgram_cat(dgram_t *dgram, const char *fmt, ...)
     G_GNUC_PRINTF(2,3);
void dgram_eatline(dgram_t *dgram);

#endif /* ! DGRAM_H */
