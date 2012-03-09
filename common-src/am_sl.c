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
 * $Id: sl.c,v 1.6 2006/05/25 01:47:12 johnfranks Exp $
 *
 * A doubly linked list of string (char *)
 */

#include "amanda.h"
#include "am_sl.h"


void init_sl(
    am_sl_t *sl)
{
    sl->first = NULL;
    sl->last  = NULL;
    sl->nb_element = 0;
}


am_sl_t *
new_sl(void)
{
    am_sl_t *sl;
    sl = alloc(SIZEOF(am_sl_t));
    init_sl(sl);
    return(sl);
}


am_sl_t *
insert_sl(
    am_sl_t *sl,
    char *name)
{
    sle_t *a;

    if(!sl) {
	sl = new_sl();
    }
    a = alloc(SIZEOF(sle_t));
    a->name = stralloc(name);
    a->next = sl->first;
    a->prev = NULL;
    if(a->next)
	a->next->prev = a;
    else
	sl->last = a;
    sl->first = a;
    sl->nb_element++;
    return(sl);
}


am_sl_t *
append_sl(
    am_sl_t *	sl,
    char *	name)
{
    sle_t *a;

    if(!sl) {
	sl = new_sl();
    }
    a = alloc(SIZEOF(sle_t));
    a->name = stralloc(name);
    a->prev = sl->last;
    a->next = NULL;
    if(a->prev)
	a->prev->next = a;
    else
	sl->first = a;
    sl->last = a;
    sl->nb_element++;
    return(sl);
}


am_sl_t *
insert_sort_sl(
    am_sl_t *	sl,
    char *	name)
{
    sle_t *a, *b;

    if(!sl) {
	sl = new_sl();
    }

    for(b=sl->first; b != NULL; b=b->next) {
	int i = strcmp(b->name, name);
	if(i==0) return(sl); /* already there, no need to insert */
	if(i>0) break;
    }

    if(b == sl->first) return insert_sl(sl, name);
    if(b == NULL)      return append_sl(sl, name);

    a = alloc(SIZEOF(sle_t));
    a->name = stralloc(name);

    /* insert before b */
    a->next = b;
    a->prev = b->prev;
    b->prev->next = a;
    b->prev = a;
    sl->nb_element++;
    return(sl);
}


void
free_sl(
    am_sl_t *	sl)
{
    sle_t *a, *b;

    if(!sl) return;

    a = sl->first;
    while(a != NULL) {
	b = a;
	a = a->next;
	amfree(b->name);
	amfree(b);
    }
    amfree(sl);
}


void
remove_sl(
    am_sl_t *	sl,
    sle_t *	elem)
{
    if(elem->prev)
	elem->prev->next = elem->next;
    else
	sl->first = elem->next;

    if(elem->next)
	elem->next->prev = elem->prev;
    else
	sl->last = elem->prev;

    sl->nb_element--;

    amfree(elem->name);
    amfree(elem);
}


am_sl_t *
duplicate_sl(
    am_sl_t *	sl)
{
    am_sl_t *new_sl = NULL;
    sle_t *a;

    if(!sl) return new_sl;

    for(a = sl->first; a != NULL; a = a->next) {
	new_sl = append_sl(new_sl, a->name);
    }

    return new_sl;
}

/*
 * Return "true" iff sl is empty (i.e. contains no elements).
 */
int
is_empty_sl(
    am_sl_t *	sl)
{
    if (sl == NULL)
	return 1;

    return (sl->nb_element == 0);
}
