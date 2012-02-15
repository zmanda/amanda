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
/*
 * $Id: tapelist.c,v 1.8 2006/06/12 15:34:48 martinea Exp $
 *
 * Support code for amidxtaped and amindexd.
 */

#include "amanda.h"
#include "tapelist.h"

/*
 * Count the number of entries in this tapelist
 */
int
num_entries(
    tapelist_t *	tapelist)
{
    tapelist_t *cur_tape;
    int count = 0;

    for(cur_tape = tapelist ; cur_tape ; cur_tape = cur_tape->next)
	count++;

    dbprintf("num_entries(tapelist=%p)=%d\n", tapelist, count);
    return(count);
}

void
dump_tapelist(
    tapelist_t *tapelist)
{
    tapelist_t *cur_tape;
    int count = 0;
    int file;

    dbprintf("dump_tapelist(%p):\n", tapelist);
    for(cur_tape = tapelist ; cur_tape != NULL ; cur_tape = cur_tape->next) {
	dbprintf("  %p->next     = %p\n", cur_tape, cur_tape->next);
	dbprintf("  %p->label    = %s\n", cur_tape, cur_tape->label);
	dbprintf("  %p->isafile  = %d\n", cur_tape, cur_tape->isafile);
	dbprintf("  %p->numfiles = %d\n", cur_tape, cur_tape->numfiles);
	for (file=0; file < cur_tape->numfiles; file++) {
	    dbprintf("  %p->files[%d] = %lld, %p->partnum[%d] = %lld\n",
		     cur_tape, file, (long long)cur_tape->files[file],
		     cur_tape, file, (long long)cur_tape->partnum[file]);
	}
	count++;
    }
    dbprintf("  %p count     = %d\n", tapelist, count);
}

/*
 * Add a tape entry with the given label to the given tapelist, creating a new
 * tapelist if handed a NULL one.  Squashes duplicates.
 */
tapelist_t *
append_to_tapelist(
    tapelist_t *tapelist,
    char *	label,
    off_t	file,
    int 	partnum,
    int		isafile)
{
    tapelist_t *new_tape, *cur_tape;
    int c;

    dbprintf("append_to_tapelist(tapelist=%p, label='%s', file=%lld, partnum=%d,  isafile=%d)\n",
		tapelist, label, (long long)file, partnum, isafile);

    /* see if we have this tape already, and if so just add to its file list */
    for(cur_tape = tapelist; cur_tape; cur_tape = cur_tape->next) {
	if(strcmp(label, cur_tape->label) == 0) {
	    int d_idx = 0;
	    off_t *newfiles;
	    int   *newpartnum;

	    if(file >= (off_t)0) {
		newfiles = alloc(SIZEOF(*newfiles) *
				 (cur_tape->numfiles + 1));
		newpartnum = alloc(SIZEOF(*newpartnum) *
				 (cur_tape->numfiles + 1));
		for(c = 0; c < cur_tape->numfiles ; c++) {
		    if(cur_tape->files[c] > file && c == d_idx) {
			newfiles[d_idx] = file;
			newpartnum[d_idx] = partnum;
			d_idx++;
		    }
		    newfiles[d_idx] = cur_tape->files[c];
		    newpartnum[d_idx] = cur_tape->partnum[c];
		    d_idx++;
		}
		if(c == d_idx) {
		    newfiles[d_idx] = file;
		    newpartnum[d_idx] = partnum;
		}
		cur_tape->numfiles++;
		amfree(cur_tape->files);
		amfree(cur_tape->partnum);
		cur_tape->files = newfiles;
		cur_tape->partnum = newpartnum;
	    }
	    return(tapelist);
	}
    }

    new_tape = alloc(SIZEOF(tapelist_t));
    memset(new_tape, 0, SIZEOF(tapelist_t));
    new_tape->label = stralloc(label);
    if(file >= (off_t)0){
	new_tape->files = alloc(SIZEOF(*(new_tape->files)));
	new_tape->files[0] = file;
	new_tape->partnum = alloc(SIZEOF(*(new_tape->partnum)));
	new_tape->partnum[0] = partnum;
	new_tape->numfiles = 1;
	new_tape->isafile = isafile;
    }

    /* first instance of anything, start our tapelist with it */
    if(!tapelist){
	tapelist = new_tape;
    } else {
	/* new tape, tack it onto the end of the list */
	cur_tape = tapelist;
	while (cur_tape->next != NULL)
	    cur_tape = cur_tape->next;
	cur_tape->next = new_tape;
    }

    return(tapelist);
}

/*
 * Backslash-escape all of the commas (and backslashes) in a label string.
 */
char *
escape_label(
    char *	label)
{
    char *cooked_str, *temp_str;
    int s_idx = 0, d_idx = 0;

    if(!label) return(NULL);

    temp_str = alloc(strlen(label) * 2);

    do{
	if(label[s_idx] == ',' || label[s_idx] == '\\' ||
		label[s_idx] == ';' || label[s_idx] == ':'){
	    temp_str[d_idx] = '\\';
	    d_idx++;
	}
	temp_str[d_idx] = label[s_idx];
	s_idx++;
	d_idx++;
    } while(label[s_idx] != '\0');
    temp_str[d_idx] = '\0';

    cooked_str = stralloc(temp_str);
    amfree(temp_str);
    
    return(cooked_str);
}

/*
 * Strip out any escape characters (backslashes)
 */
char *
unescape_label(
    char *	label)
{
    char *cooked_str, *temp_str;
    int s_idx = 0, d_idx = 0, prev_esc = 0;
    
    if(!label) return(NULL);

    temp_str = alloc(strlen(label));

    do{
	if(label[s_idx] == '\\' && !prev_esc){
	    s_idx++;
	    prev_esc = 1;
	    continue;
	}
	prev_esc = 0;
	temp_str[d_idx] = label[s_idx];
	s_idx++;
	d_idx++;
    } while(label[s_idx] != '\0');
    temp_str[d_idx] = '\0';

    cooked_str = stralloc(temp_str);
    amfree(temp_str);
    
    return(cooked_str);
}

/*
 * Convert a tapelist into a parseable string of tape labels and file numbers.
 */
char *
marshal_tapelist(
    tapelist_t *tapelist,
    int		do_escape)
{
    tapelist_t *cur_tape;
    char *str = NULL;

    for(cur_tape = tapelist; cur_tape; cur_tape = cur_tape->next){
	char *esc_label;
	char *files_str = NULL;
	int c;

	if(do_escape) esc_label = escape_label(cur_tape->label);
	else esc_label = stralloc(cur_tape->label);

	for(c = 0; c < cur_tape->numfiles ; c++){
	    char num_str[NUM_STR_SIZE];
	    g_snprintf(num_str, SIZEOF(num_str), "%lld",
			(long long)cur_tape->files[c]);
	    if (!files_str)
		files_str = stralloc(num_str);
	    else
		vstrextend(&files_str, ",", num_str, NULL);
	}

	if (!str)
	    str = vstralloc(esc_label, ":", files_str, NULL);
	else
	    vstrextend(&str, ";", esc_label, ":", files_str, NULL);

	amfree(esc_label);
	amfree(files_str);
    }

    return(str);
}

/*
 * Convert a previously str-ified and escaped list of tapes back into a
 * tapelist structure.
 */
tapelist_t *
unmarshal_tapelist_str(
    char *	tapelist_str)
{
    char *temp_label, *temp_filenum;
    int l_idx, n_idx;
    size_t input_length;
    tapelist_t *tapelist = NULL;

    if(!tapelist_str) return(NULL);

    input_length = strlen(tapelist_str);

    temp_label = alloc(input_length+1);
    temp_filenum = alloc(input_length+1);

    do{
	/* first, read the label part */
	memset(temp_label, '\0', input_length+1);
        l_idx = 0;
	while(*tapelist_str != ':' && *tapelist_str != '\0'){
	    if(*tapelist_str == '\\')
		tapelist_str++; /* skip escapes */
	    temp_label[l_idx] = *tapelist_str;
	    if(*tapelist_str == '\0')
		break; /* bad format, should kvetch */
	    tapelist_str++;
	    l_idx++;
	}
	if(*tapelist_str != '\0')
	    tapelist_str++;
	tapelist = append_to_tapelist(tapelist, temp_label, (off_t)-1, -1, 0);

	/* now read the list of file numbers */
	while(*tapelist_str != ';' && *tapelist_str != '\0'){
	    off_t filenum;

	    memset(temp_filenum, '\0', input_length+1);
	    n_idx = 0;
	    while(*tapelist_str != ';' && *tapelist_str != ',' &&
		    *tapelist_str != '\0'){
		temp_filenum[n_idx] = *tapelist_str; 
		tapelist_str++;
		n_idx++;
	    }
	    filenum = OFF_T_ATOI(temp_filenum);

	    tapelist = append_to_tapelist(tapelist, temp_label, filenum, -1, 0);
	    if(*tapelist_str != '\0' && *tapelist_str != ';')
		tapelist_str++;
	}
	if(*tapelist_str != '\0')
	    tapelist_str++;

    } while(*tapelist_str != '\0');

    amfree(temp_label);
    amfree(temp_filenum);

    return(tapelist);
}

/*
 * Free up a list of tapes
 */
void
free_tapelist(
    tapelist_t *	tapelist)
{
    tapelist_t *cur_tape;
    tapelist_t *prev = NULL;

    for(cur_tape = tapelist ; cur_tape ; cur_tape = cur_tape->next){
	amfree(cur_tape->label);
	amfree(cur_tape->files);
	amfree(cur_tape->partnum);
	amfree(prev);
	prev = cur_tape;
    }
    amfree(prev);
}
