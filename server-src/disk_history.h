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
/* $Id: disk_history.h,v 1.6 2006/05/25 01:47:19 johnfranks Exp $
 *
 * interface for obtaining disk backup history
 */
#ifndef DISK_HISTORY_H
#define DISK_HISTORY_H

#include "tapelist.h"

typedef struct DUMP_ITEM
{
    char date[20];
    int  level;
    int  is_split;
    int  maxpart;
    char tape[256];
    tapelist_t *tapes;
    off_t  file;
    char *hostname;

    struct DUMP_ITEM *next;
}
DUMP_ITEM;

#define next_dump(item)	((item)->next)

extern void clear_list(void);
extern void add_dump(char *hostname, char *date, int level, char *tape,
		     off_t file, int partnum, int maxpart);
extern void clean_dump(void);
extern DUMP_ITEM *first_dump(void);
#endif	/* !DISK_HISTORY_H */
