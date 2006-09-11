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
/* $Id: list_dir.c,v 1.19 2006/07/05 11:22:49 martinea Exp $
 *
 * manage directory listings from index files
 */

#include "amanda.h"
#include "disk_history.h"
#include "list_dir.h"

DIR_ITEM *dir_list = NULL; /* first dir entry */
DIR_ITEM *dir_last = NULL; /* last dir entry  */
DIR_ITEM *cur_list = NULL; /* current dir entry,for speeding up search */

DIR_ITEM *
get_dir_list(void)
{
    return dir_list;
}


void
clear_dir_list(void)
{
    DIR_ITEM *this;

    if (dir_list == NULL)
	return;

    do
    {
	this = dir_list;
	dir_list = dir_list->next;
	amfree(this->path);
	amfree(this);
    } while (dir_list != NULL);

    dir_last = NULL;
    cur_list = NULL;
}

/* add item to list if path not already on list                     */
/* Since this function is almost called with increasing path order, */
/* we keep a pointer on the last element added (cur_list), this     */
/* reduce the time for the search of a path.                        */
/* It's true because the output of the index file is sorted         */
/* Maybe it could be more efficient if the index was sorted when    */
/* it is generated                                                  */

int
add_dir_list_item(
    DUMP_ITEM *	dump,
    const char *path)
{
    DIR_ITEM *cur;

    if (dir_list == NULL)
    {
	dir_list = (DIR_ITEM *)alloc(SIZEOF(DIR_ITEM));
	dir_list->next = NULL;
	dir_list->dump = dump;
	dir_list->path = stralloc(path);
	dir_last=dir_list;
	cur_list=dir_list;
	return 0; /* added */
    }

    if(strcmp(path,dir_last->path) == 0)
	return 0; /* found */

    /* add at head of list */
    if(strcmp(path,dir_list->path) < 0)
    {
	cur_list = (DIR_ITEM *)alloc(SIZEOF(DIR_ITEM));
	cur_list->next = dir_list;
	cur_list->dump = dump;
	cur_list->path = stralloc(path);
	dir_list = cur_list;
	return 0; /* added */
    }

    /* if smaller than last path */
    if(strcmp(path,dir_last->path) < 0)
    {
	if(cur_list==NULL)
	    cur_list=dir_list;

	/* reset cur_list if path is smaller than cur_list->path */
	if(strcmp(path,cur_list->path) < 0)
	    cur_list=dir_list;

	if(strcmp(path,cur_list->path) == 0)
	    return 0; /* found */

	while (cur_list->next!=NULL && (strcmp(path,cur_list->next->path) > 0))
	{
	    cur_list=cur_list->next;
	}

	if (cur_list->next && strcmp(path, cur_list->next->path) == 0)
	{
	    cur_list=cur_list->next;
	    return 0; /* found */
	}

	/* add at cur_list */
	cur = (DIR_ITEM *)alloc(SIZEOF(DIR_ITEM));
	cur->next = cur_list->next;
	cur->dump = dump;
	cur->path = stralloc(path);
	cur_list->next=cur;
	cur_list=cur;
	return 0; /* added */
    }
    else /* add at end of list */
    {
	dir_last->next = (DIR_ITEM *)alloc(SIZEOF(DIR_ITEM));
	dir_last=dir_last->next;
	dir_last->next = NULL;
	dir_last->dump = dump;
	dir_last->path = stralloc(path);
	return 0; /* added */
    }
}
