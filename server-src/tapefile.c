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
 * $Id: tapefile.c,v 1.37 2006/07/21 00:25:52 martinea Exp $
 *
 * routines to read and write the amanda active tape list
 */
#include "amanda.h"
#include "match.h"
#include "tapefile.h"
#include "conffile.h"

static tape_t *tape_list = NULL;

/* local functions */
static tape_t *parse_tapeline(int *status, char *line);
static tape_t *insert(tape_t *list, tape_t *tp);
static time_t stamp2time(char *datestamp);

int
read_tapelist(
    char *tapefile)
{
    tape_t *tp;
    FILE *tapef;
    int pos;
    char *line = NULL;
    int status = 0;

    clear_tapelist();
    if((tapef = fopen(tapefile,"r")) == NULL) {
	if (errno == ENOENT) {
	    /* no tapelist is equivalent to an empty tapelist */
	    return 0;
	} else {
	    g_debug("Error opening '%s': %s", tapefile, strerror(errno));
	    return 1;
	}
    }

    while((line = agets(tapef)) != NULL) {
	if (line[0] == '\0') {
	    amfree(line);
	    continue;
	}
	tp = parse_tapeline(&status, line);
	amfree(line);
	if(tp == NULL && status != 0)
	    return 1;
	if(tp != NULL)
	    tape_list = insert(tape_list, tp);
    }
    afclose(tapef);

    for(pos=1,tp=tape_list; tp != NULL; pos++,tp=tp->next) {
	tp->position = pos;
    }

    return 0;
}

int
write_tapelist(
    char *tapefile)
{
    tape_t *tp;
    FILE *tapef;
    char *newtapefile;
    int rc;

    newtapefile = stralloc2(tapefile, ".new");

    if((tapef = fopen(newtapefile,"w")) == NULL) {
	amfree(newtapefile);
	return 1;
    }

    for(tp = tape_list; tp != NULL; tp = tp->next) {
	g_fprintf(tapef, "%s %s", tp->datestamp, tp->label);
	if(tp->reuse) g_fprintf(tapef, " reuse");
	else g_fprintf(tapef, " no-reuse");
	if (tp->barcode)
	    g_fprintf(tapef, " BARCODE:%s", tp->barcode);
	if (tp->meta)
	    g_fprintf(tapef, " META:%s", tp->meta);
	if (tp->blocksize)
	    g_fprintf(tapef, " BLOCKSIZE:%jd", (intmax_t)tp->blocksize);
	if (tp->comment)
	    g_fprintf(tapef, " #%s", tp->comment);
	g_fprintf(tapef, "\n");
    }

    if (fclose(tapef) == EOF) {
	g_fprintf(stderr,_("error [closing %s: %s]"), newtapefile, strerror(errno));
	amfree(newtapefile);
	return 1;
    }
    rc = rename(newtapefile, tapefile);
    amfree(newtapefile);

    return(rc != 0);
}

void
clear_tapelist(void)
{
    tape_t *tp, *next;

    for(tp = tape_list; tp; tp = next) {
	amfree(tp->label);
	amfree(tp->datestamp);
	amfree(tp->barcode);
	amfree(tp->meta);
	amfree(tp->comment);
	next = tp->next;
	amfree(tp);
    }
    tape_list = NULL;
}

tape_t *
lookup_tapelabel(
    const char *label)
{
    tape_t *tp;

    for(tp = tape_list; tp != NULL; tp = tp->next) {
	if(strcmp(label, tp->label) == 0) return tp;
    }
    return NULL;
}



tape_t *
lookup_tapepos(
    int pos)
{
    tape_t *tp;

    for(tp = tape_list; tp != NULL; tp = tp->next) {
	if(tp->position == pos) return tp;
    }
    return NULL;
}


tape_t *
lookup_tapedate(
    char *datestamp)
{
    tape_t *tp;

    for(tp = tape_list; tp != NULL; tp = tp->next) {
	if(strcmp(tp->datestamp, datestamp) == 0) return tp;
    }
    return NULL;
}

int
lookup_nb_tape(void)
{
    tape_t *tp;
    int pos=0;

    for(tp = tape_list; tp != NULL; tp = tp->next) {
	pos=tp->position;
    }
    return pos;
}


char *
get_last_reusable_tape_label(
     int skip)
{
    tape_t *tp = lookup_last_reusable_tape(skip);
    return (tp != NULL) ? tp->label : NULL;
}

tape_t *
lookup_last_reusable_tape(
     int skip)
{
    tape_t *tp, **tpsave;
    int count=0;
    int s;
    int tapecycle = getconf_int(CNF_TAPECYCLE);
    char *labelstr = getconf_str (CNF_LABELSTR);

    /*
     * The idea here is we keep the last "several" reusable tapes we
     * find in a stack and then return the n-th oldest one to the
     * caller.  If skip is zero, the oldest is returned, if it is
     * one, the next oldest, two, the next to next oldest and so on.
     */
    tpsave = alloc((skip + 1) * SIZEOF(*tpsave));
    for(s = 0; s <= skip; s++) {
	tpsave[s] = NULL;
    }
    for(tp = tape_list; tp != NULL; tp = tp->next) {
	if(tp->reuse == 1 && strcmp(tp->datestamp,"0") != 0 && match (labelstr, tp->label)) {
	    count++;
	    for(s = skip; s > 0; s--) {
	        tpsave[s] = tpsave[s - 1];
	    }
	    tpsave[0] = tp;
	}
    }
    s = tapecycle - count;
    if(s < 0) s = 0;
    if(count < tapecycle - skip) tp = NULL;
    else tp = tpsave[skip - s];
    amfree(tpsave);
    return tp;
}

int
reusable_tape(
    tape_t *tp)
{
    int count = 0;

    if(tp == NULL) return 0;
    if(tp->reuse == 0) return 0;
    if( strcmp(tp->datestamp,"0") == 0) return 1;
    while(tp != NULL) {
	if(tp->reuse == 1) count++;
	tp = tp->prev;
    }
    return (count >= getconf_int(CNF_TAPECYCLE));
}

void
remove_tapelabel(
    char *label)
{
    tape_t *tp, *prev, *next;

    tp = lookup_tapelabel(label);
    if(tp != NULL) {
	prev = tp->prev;
	next = tp->next;
	/*@ignore@*/
	if(prev != NULL)
	    prev->next = next;
	else /* begin of list */
	    tape_list = next;
	if(next != NULL)
	    next->prev = prev;
	/*@end@*/
	while (next != NULL) {
	    next->position--;
	    next = next->next;
	}
	amfree(tp->datestamp);
	amfree(tp->label);
	amfree(tp->meta);
	amfree(tp->comment);
	amfree(tp->barcode);
	amfree(tp);
    }
}

tape_t *
add_tapelabel(
    char *datestamp,
    char *label,
    char *comment)
{
    tape_t *cur, *new;

    /* insert a new record to the front of the list */

    new = g_new0(tape_t, 1);

    new->datestamp = stralloc(datestamp);
    new->position = 0;
    new->reuse = 1;
    new->label = stralloc(label);
    new->comment = comment? stralloc(comment) : NULL;

    new->prev  = NULL;
    if(tape_list != NULL) tape_list->prev = new;
    new->next = tape_list;
    tape_list = new;

    /* scan list, updating positions */
    cur = tape_list;
    while(cur != NULL) {
	cur->position++;
	cur = cur->next;
    }

    return new;
}

int
guess_runs_from_tapelist(void)
{
    tape_t *tp;
    int i, ntapes, tape_ndays, dumpcycle, runtapes, runs;
    time_t tape_time, today;

    today = time(0);
    dumpcycle = getconf_int(CNF_DUMPCYCLE);
    runtapes = getconf_int(CNF_RUNTAPES);
    if(runtapes == 0) runtapes = 1;	/* just in case */

    ntapes = 0;
    tape_ndays = 0;
    for(i = 1; i < getconf_int(CNF_TAPECYCLE); i++) {
	if((tp = lookup_tapepos(i)) == NULL) break;

	tape_time  = stamp2time(tp->datestamp);
	tape_ndays = (int)days_diff(tape_time, today);

	if(tape_ndays < dumpcycle) ntapes++;
	else break;
    }

    if(tape_ndays < dumpcycle)	{
	/* scale for best guess */
	if(tape_ndays == 0) ntapes = dumpcycle * runtapes;
	else ntapes = ntapes * dumpcycle / tape_ndays;
    }
    else if(ntapes == 0) {
	/* no dumps within the last dumpcycle, guess as above */
	ntapes = dumpcycle * runtapes;
    }

    runs = (ntapes + runtapes - 1) / runtapes;
    if (runs <= 0)
      runs = 1;
    return runs;
}

static tape_t *
parse_tapeline(
    int *status,
    char *line)
{
    tape_t *tp = NULL;
    char *s, *s1;
    int ch;

    *status = 0;

    s = line;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	return NULL;
    }

    tp = g_new0(tape_t, 1);

    s1 = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    tp->datestamp = stralloc(s1);

    skip_whitespace(s, ch);
    s1 = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    tp->label = stralloc(s1);

    skip_whitespace(s, ch);
    tp->reuse = 1;
    if(strncmp_const(s - 1, "reuse") == 0) {
	tp->reuse = 1;
	s1 = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
    }
    if(strncmp_const(s - 1, "no-reuse") == 0) {
	tp->reuse = 0;
	s1 = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
    }

    if (strncmp_const(s - 1, "BARCODE:") == 0) {
	s1 = s - 1 + 8;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->barcode = stralloc(s1);
    }

    if (strncmp_const(s - 1, "META:") == 0) {
	s1 = s - 1 + 5;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->meta = stralloc(s1);
    }

    if (strncmp_const(s - 1, "BLOCKSIZE:") == 0) {
	s1 = s - 1 + 10;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->blocksize = atol(s1);
    }
    if (*(s - 1) == '#') {
	tp->comment = stralloc(s); /* skip leading '#' */
    }

    return tp;
}


/* insert in reversed datestamp order */
/*@ignore@*/
static tape_t *
insert(
    tape_t *list,
    tape_t *tp)
{
    tape_t *prev, *cur;

    prev = NULL;
    cur = list;

    while(cur != NULL && strcmp(cur->datestamp, tp->datestamp) >= 0) {
	prev = cur;
	cur = cur->next;
    }
    tp->prev = prev;
    tp->next = cur;
    if(prev == NULL) {
	list = tp;
#ifndef __lint
    } else {
	prev->next = tp;
#endif
    }
    if(cur !=NULL)
	cur->prev = tp;

    return list;
}
/*@end@*/

/*
 * Converts datestamp (an char of the form YYYYMMDD or YYYYMMDDHHMMSS) into a real
 * time_t value.
 * Since the datestamp contains no timezone or hh/mm/ss information, the
 * value is approximate.  This is ok for our purposes, since we round off
 * scheduling calculations to the nearest day.
 */

static time_t
stamp2time(
    char *datestamp)
{
    struct tm *tm;
    time_t now;
    char date[9];
    int dateint;

    strncpy(date, datestamp, 8);
    date[8] = '\0';
    dateint = atoi(date);
    now = time(0);
    tm = localtime(&now);	/* initialize sec/min/hour & gmtoff */

    if (!tm) {
	tm = alloc(SIZEOF(struct tm));
	tm->tm_sec   = 0;
	tm->tm_min   = 0;
	tm->tm_hour  = 0;
	tm->tm_wday  = 0;
	tm->tm_yday  = 0;
	tm->tm_isdst = 0;
    }


    tm->tm_year = ( dateint          / 10000) - 1900;
    tm->tm_mon  = ((dateint % 10000) /   100) - 1;
    tm->tm_mday = ((dateint %   100)        );

    return mktime(tm);
}

char *
list_new_tapes(
    int nb)
{
    tape_t *lasttp, *iter;
    char *result = NULL;

    /* Find latest reusable new tape */
    lasttp = lookup_tapepos(lookup_nb_tape());
    while (lasttp && lasttp->reuse == 0)
	lasttp = lasttp->prev;

    if(lasttp && nb > 0 && strcmp(lasttp->datestamp,"0") == 0) {
	int c = 0;
	iter = lasttp;
	/* count the number of tapes we *actually* used */
	while(iter && nb > 0 && strcmp(iter->datestamp,"0") == 0) {
	    if (iter->reuse) {
		c++;
		nb--;
	    }
	    iter = iter->prev;
	}

	if(c == 1) {
	    result = g_strdup_printf(
			_("The next new tape already labelled is: %s."),
			lasttp->label);
	} else {
	    result = g_strdup_printf(
			_("The next %d new tapes already labelled are: %s"),
			c, lasttp->label);
	    iter = lasttp->prev;
	    c--;
	    while(iter && c > 0 && strcmp(iter->datestamp,"0") == 0) {
		if (iter->reuse) {
		    result = vstrextend(&result, ", ", iter->label, NULL);
		    c--;
		}
		iter = iter->prev;
	    }
	}
    }
    return result;
}

void
print_new_tapes(
    FILE *output,
    int   nb)
{
    char *result = list_new_tapes(nb);

    if (result) {
	g_fprintf(output,"%s\n", result);
	amfree(result);
    }
}
