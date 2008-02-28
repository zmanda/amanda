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
/* $Id: disk_history.c,v 1.13 2006/05/25 01:47:19 johnfranks Exp $
 *
 * functions for obtaining backup history
 */

#include "amanda.h"
#include "disk_history.h"

static DUMP_ITEM *disk_hist = NULL;

void
clear_list(void)
{
    DUMP_ITEM *item, *this;

    item = disk_hist;
    while (item != NULL)
    {
	this = item;
	item = item->next;
	amfree(this->hostname);
	while(this->tapes != NULL) {
	    tapelist_t *tapes = this->tapes;
	    this->tapes = tapes->next;
	    amfree(tapes->label);
	    amfree(tapes->files);
	    amfree(tapes);
	}
	amfree(this);
    }
    disk_hist = NULL;
}

/* add item, maintain list ordered by oldest date last */

void
add_dump(
    char *      hostname,
    char *	date,
    int		level,
    char *	tape,
    off_t	file,
    int		partnum,
    int		maxpart)
{
    DUMP_ITEM *new, *item, *before;
    int isafile = 0;

    if(tape[0] == '/')
	isafile = 1; /* XXX kludgey, like this whole thing */

    /* See if we already have partnum=partnum-1 */
    if (partnum > 1) {
	int partnum_minus_1 = 0;
	for(item = disk_hist, before = NULL; item;
	    before = item, item = item->next) {
	    if (!strcmp(item->date, date) &&
		    item->level == level && item->is_split) {
		tapelist_t *cur_tape;
		for (cur_tape = item->tapes; cur_tape;
					     cur_tape = cur_tape->next) {
		    int files;
		    for(files=0; files<cur_tape->numfiles; files++) {
			if (cur_tape->partnum[files] == partnum - 1)
			    partnum_minus_1 = 1;
		    }
		}
		if (partnum_minus_1 == 1) {
		    item->tapes = append_to_tapelist(item->tapes, tape, file,
						     partnum, isafile);
		    if (maxpart > item->maxpart)
			item->maxpart = maxpart;
		} else {
		    /* some part are missing, remove the item from disk_hist */
		    if (before)
			before->next = item->next;
		    else
			disk_hist = item->next;
		    /* free item */
		    free_tapelist(item->tapes);
		    amfree(item->hostname);
		    amfree(item);
		}
		return;
	    }
	}
	return;
    }

    new = (DUMP_ITEM *)alloc(SIZEOF(DUMP_ITEM));
    strncpy(new->date, date, SIZEOF(new->date)-1);
    new->date[SIZEOF(new->date)-1] = '\0';
    new->level = level;
    strncpy(new->tape, tape, SIZEOF(new->tape)-1);
    new->tape[SIZEOF(new->tape)-1] = '\0';
    new->file = file;
    new->maxpart = maxpart;
    if(partnum == -1)
        new->is_split = 0;
    else
        new->is_split = 1;
    new->tapes = NULL;
    new->hostname = stralloc(hostname);

    new->tapes = append_to_tapelist(new->tapes, tape, file, partnum, isafile);

    if (disk_hist == NULL)
    {
	disk_hist = new;
	new->next = NULL;
	return;
    }

    /* prepend this item to the history list, if it's newer */
    /* XXX this should probably handle them being on the same date with
       datestamp_uax or something */
    if (strcmp(disk_hist->date, new->date) <= 0)
    {
	new->next = disk_hist;
	disk_hist = new;
	return;
    }

    /* append this item to the history list, if it's older */
    before = disk_hist;
    item = disk_hist->next;
    while ((item != NULL) && (strcmp(item->date, new->date) > 0))
    {
	before = item;
	item = item->next;
    }
    new->next = item;
    before->next = new;
}

void
clean_dump(void)
{
    DUMP_ITEM *item, *before;

    /* check if the maxpart part is avaliable */
    for(item = disk_hist, before = NULL; item;
					 before = item, item = item->next) {
	int found_maxpart = 0;
	tapelist_t *cur_tape;

	if (item->maxpart > 1) {
	    for (cur_tape = item->tapes; cur_tape; cur_tape = cur_tape->next) {
		int files;
		for(files=0; files<cur_tape->numfiles; files++) {
		    if (cur_tape->partnum[files] == item->maxpart) {
			found_maxpart = 1;
		    }
		}
	    }
	    if (found_maxpart == 0) {
		DUMP_ITEM *myitem = item; 

		if (before)
		    before->next = item->next;
		else
		    disk_hist = item->next;
		item = item->next;
		/* free myitem */
		free_tapelist(myitem->tapes);
		amfree(myitem->hostname);
		amfree(myitem);
		if (item == NULL)
		    break;
	    }
	}
    }
}

DUMP_ITEM *
first_dump(void)
{
    return disk_hist;
}
