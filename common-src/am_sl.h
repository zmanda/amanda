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
 * Author: James da Silva, Systems Design and Analysis Group
 *                         Computer Science Department
 *                         University of Maryland at College Park
 */
/*
 * $Id: sl.h,v 1.4 2006/05/25 01:47:12 johnfranks Exp $
 *
 * A doubly linked list of string (char *)
 */

/*
 * To scan over all element of the list
 *
 *    for(sle=sl->first; sle != NULL; sle = sle->next) {
 *    }
 */
#ifndef STRINGLIST_H
#define STRINGLIST_H

#include "amanda.h"

typedef struct sle_s {
    struct sle_s *next, *prev;
    char *name;
} sle_t;

typedef struct sl_s {
    struct sle_s *first, *last;
    int nb_element;
} am_sl_t;

void init_sl(am_sl_t *sl);
am_sl_t *new_sl(void);
am_sl_t *insert_sl(am_sl_t *sl, char *name);
am_sl_t *append_sl(am_sl_t *sl, char *name);
am_sl_t *insert_sort_sl(am_sl_t *sl, char *name);
void free_sl(am_sl_t *sl);
void remove_sl(am_sl_t *sl,sle_t *elem);
am_sl_t *duplicate_sl(am_sl_t *sl);
int  is_empty_sl(am_sl_t *sl);

#endif
