/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "timestamp.h"
#include "find.h"

static tape_t *tape_list = NULL;
static gboolean retention_computed = FALSE;

/* local functions */
static tape_t *parse_tapeline(int *status, char *line);
static tape_t *insert(tape_t *list, tape_t *tp);
static time_t stamp2time(char *datestamp);
static void compute_storage_retention_nb(const char *storage,
					 const char *tapepool,
					 const char *l_template,
					 int retention_tapes);
static void compute_storage_retention(find_result_t *output_find,
				      const char *storage,
				      const char *tapepool,
				      const char *l_template,
				      int   retention_tapes,
				      int   retention_days,
				      int   retention_recover,
				      int   retention_full);

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
	if (tp == NULL && status != 0) {
	    afclose(tapef);
	    return 1;
	}
	if (tp != NULL)
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

    newtapefile = g_strconcat(tapefile, ".new", NULL);

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
	if (tp->pool)
	    g_fprintf(tapef, " POOL:%s", tp->pool);
	if (tp->storage)
	    g_fprintf(tapef, " STORAGE:%s", tp->storage);
	if (tp->config)
	    g_fprintf(tapef, " CONFIG:%s", tp->config);
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
	amfree(tp->config);
	amfree(tp->pool);
	amfree(tp->storage);
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
	if(g_str_equal(label, tp->label)) return tp;
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
	if(g_str_equal(tp->datestamp, datestamp)) return tp;
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
    const char *l_template,
    const char *tapepool,
    const char *storage,
    int   retention_tapes,
    int   retention_days,
    int   retention_recover,
    int   retention_full,
    int   skip)
{
    tape_t *tp = lookup_last_reusable_tape(l_template, tapepool, storage,
					   retention_tapes,
					   retention_days, retention_recover,
					   retention_full, skip);
    return (tp != NULL) ? tp->label : NULL;
}

tape_t *
lookup_last_reusable_tape(
    const char *l_template,
    const char *tapepool,
    const char *storage,
    int   retention_tapes,
    int   retention_days G_GNUC_UNUSED,
    int   retention_recover G_GNUC_UNUSED,
    int   retention_full G_GNUC_UNUSED,
    int   skip)
{
    tape_t *tp, **tpsave;
    int count=0;
    int s;

    /*
     * The idea here is we keep the last "several" reusable tapes we
     * find in a stack and then return the n-th oldest one to the
     * caller.  If skip is zero, the oldest is returned, if it is
     * one, the next oldest, two, the next to next oldest and so on.
     */
    compute_retention();
    tpsave = g_malloc((skip + 1) * sizeof(*tpsave));
    for(s = 0; s <= skip; s++) {
	tpsave[s] = NULL;
    }
    for(tp = tape_list; tp != NULL; tp = tp->next) {
	if (tp->reuse == 1 && !g_str_equal(tp->datestamp, "0") &&
	    (!tp->config || g_str_equal(tp->config, get_config_name())) &&
	    (!tp->storage || g_str_equal(tp->storage, storage)) &&
	    ((tp->pool && g_str_equal(tp->pool, tapepool)) ||
	     (!tp->pool && match_labelstr_template(l_template, tp->label,
						   tp->barcode, tp->meta)))) {
	    count++;
	    for(s = skip; s > 0; s--) {
	        tpsave[s] = tpsave[s - 1];
	    }
	    tpsave[0] = tp;
	}
    }
    s = retention_tapes - count;
    if(s < 0) s = 0;
    if(count < retention_tapes - skip) tp = NULL;
    else tp = tpsave[skip - s];
    amfree(tpsave);
    return tp;
}

int
reusable_tape(
    tape_t *tp)
{
    if (tp == NULL) return 0;
    if (tp->reuse == 0) return 0;
    if (g_str_equal(tp->datestamp, "0")) return 1;
    compute_retention();

    return (!tp->retention && !tp->retention_nb);
}

int
volume_is_reusable(
    const char *label)
{
    tape_t *tp = lookup_tapelabel(label);
    return reusable_tape(tp);
}

void
remove_tapelabel(
    const char *label)
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
	amfree(tp->pool);
	amfree(tp->storage);
	amfree(tp->config);
	amfree(tp->barcode);
	amfree(tp);
    }
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
    char *cline;

    *status = 0;

    s = line;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	return NULL;
    }

    cline = g_strdup(line);
    tp = g_new0(tape_t, 1);

    s1 = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    tp->datestamp = g_strdup(s1);

    skip_whitespace(s, ch);
    s1 = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    tp->label = g_strdup(s1);

    skip_whitespace(s, ch);
    tp->reuse = 1;
    if(strncmp_const(s - 1, "reuse") == 0) {
	tp->reuse = 1;
	s1 = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
    }
    tp->retention = !tp->reuse;
    tp->retention_nb = FALSE;
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
	tp->barcode = g_strdup(s1);
    }

    if (strncmp_const(s - 1, "META:") == 0) {
	s1 = s - 1 + 5;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->meta = g_strdup(s1);
    }

    if (strncmp_const(s - 1, "BLOCKSIZE:") == 0) {
	s1 = s - 1 + 10;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->blocksize = atol(s1);
    }

    if (strncmp_const(s - 1, "POOL:") == 0) {
	s1 = s - 1 + 5;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->pool = g_strdup(s1);
    }

    if (strncmp_const(s - 1, "STORAGE:") == 0) {
	s1 = s - 1 + 8;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->storage = g_strdup(s1);
    }

    if (strncmp_const(s - 1, "CONFIG:") == 0) {
	s1 = s - 1 + 7;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	skip_whitespace(s, ch);
	tp->config = g_strdup(s1);
    }

    if (*(s - 1) == '#') {
	tp->comment = g_strdup(s); /* skip leading '#' */
    } else if (*(s-1)) {
	g_critical("Bogus line in the tapelist file: %s", cline);
    }
    g_free(cline);

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
    struct tm *tm, *tm1;
    time_t now;
    char date[9];
    int dateint;
    time_t tt;

    strncpy(date, datestamp, 8);
    date[8] = '\0';
    dateint = atoi(date);
    now = time(0);
    tm = g_malloc(sizeof(struct tm));
    tm1 = localtime_r(&now, tm);	/* initialize sec/min/hour & gmtoff */

    if (!tm1) {
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

    tt = mktime(tm);
    amfree(tm);
    return (tt);
}

gchar **list_new_tapes(
    char *storage_n,
    int   nb)
{
    tape_t *last_tape, *iter;
    int c;
    labelstr_t *labelstr;
    char       *tapepool;
    GSList     *l_list = NULL;
    GSList     *list1;
    storage_t  *storage;
    char **lists;
    int d;

    if (nb <= 0)
        return NULL;

    storage = lookup_storage(storage_n);

    /* Find latest reusable new tape */
    last_tape = lookup_tapepos(lookup_nb_tape());
    while (last_tape && last_tape->reuse == 0)
	last_tape = last_tape->prev;

    if (!last_tape)
        return NULL;

    if (!g_str_equal(last_tape->datestamp, "0"))
        return NULL;

    labelstr = storage_get_labelstr(storage);
    tapepool = storage_get_tapepool(storage);

    /* count the number of tapes we *actually* used */

    iter = last_tape;
    c = 0;

    while (iter && nb > 0 && g_str_equal(iter->datestamp, "0")) {
	if (iter->reuse &&
	    (!iter->config || g_str_equal(iter->config, get_config_name())) &&
	    (!iter->storage || g_str_equal(iter->storage, storage_n)) &&
	    ((iter->pool && g_str_equal(iter->pool, tapepool)) ||
	     (!iter->pool && match_labelstr_template(labelstr->template, iter->label,
						 iter->barcode, iter->meta)))) {
            c++;
            nb--;
	    l_list = g_slist_append(l_list, iter->label);
        }
        iter = iter->prev;
    }

    lists = g_new0(gchar *, (c+1));
    d = 0;
    list1 = l_list;

    while (list1 != NULL) {
	lists[d] = list1->data;
	list1 = list1->next;
	d++;
    }
    lists[d] = 0;

    g_slist_free(l_list);
    return lists;

}

static find_result_t *output_find = NULL;
void
compute_retention(void)
{
    tape_t     *tp;
    storage_t  *storage;
    disklist_t  diskp;

    for (tp = tape_list; tp != NULL; tp = tp->next) {
	tp->retention_nb = FALSE;
    }

    for (storage = get_first_storage(); storage != NULL;
	 storage = get_next_storage(storage)) {
	char       *policy_name = storage_get_policy(storage);
	policy_t   *policy = lookup_policy(policy_name);
	labelstr_t *labelstr = storage_get_labelstr(storage);
	compute_storage_retention_nb(storage_name(storage),
				     storage_get_tapepool(storage),
				     labelstr->template,
				     policy_get_retention_tapes(policy));
    }

    if (retention_computed)
	return;

    retention_computed = TRUE;
    for (tp = tape_list; tp != NULL; tp = tp->next) {
	tp->retention = !tp->reuse;
	if (tp->config && !g_str_equal(tp->config, get_config_name())) {
	    tp->retention = 1;
	}
    }

    for (storage = get_first_storage(); storage != NULL;
	 storage = get_next_storage(storage)) {
	char       *policy_name = storage_get_policy(storage);
	policy_t   *policy = lookup_policy(policy_name);
	labelstr_t *labelstr = storage_get_labelstr(storage);

	if (!output_find && (policy_get_retention_recover(policy) ||
			     policy_get_retention_full(policy))) {
	    char *conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
	    read_diskfile(conf_diskfile, &diskp);
	    output_find = find_dump(&diskp);
	    sort_find_result("hkDLpbfw", &output_find);
	}

	compute_storage_retention(output_find, storage_name(storage),
				  storage_get_tapepool(storage),
				  labelstr->template,
				  policy_get_retention_tapes(policy),
				  policy_get_retention_days(policy),
				  policy_get_retention_recover(policy),
				  policy_get_retention_full(policy));
    }
}


static void
compute_storage_retention_nb(
    const char *storage,
    const char *tapepool,
    const char *l_template,
    int   retention_tapes)
{
    tape_t *tp;

    if (retention_tapes) {
	int count = 0;
	for (tp = tape_list; tp != NULL; tp = tp->next) {
	    if (tp->reuse == 1 &&
		!g_str_equal(tp->datestamp, "0") &&
		(!tp->config || g_str_equal(tp->config, get_config_name())) &&
		(!tp->storage || g_str_equal(tp->storage, storage)) &&
		((tp->pool && g_str_equal(tp->pool, tapepool)) ||
		 (!tp->pool && match_labelstr_template(l_template, tp->label,
						       tp->barcode, tp->meta)))) {
		count++;
		if (count < retention_tapes) {
		    /* Do not mark them, as it change when a tape is
		     * overwritten */
		    /* tp->retention = TRUE; */
		    tp->retention_nb = TRUE;
		}
	    }
	}
    }
}

static void
compute_storage_retention(
    find_result_t *output_find,
    const char *storage,
    const char *tapepool,
    const char *l_template,
    int   retention_tapes,
    int   retention_days,
    int   retention_recover,
    int   retention_full)
{
    tape_t        *tp;

    if (retention_tapes) {
	/* done in compute_storage_retention_nb */
    }

    if (retention_days) {
	char *datestr = get_timestamp_from_time(time(NULL) -
					retention_days*86400);
	for(tp = tape_list; tp != NULL; tp = tp->prev) {
	    if (tp->reuse == 1 &&
		g_ascii_strcasecmp(tp->datestamp, datestr) > 0 &&
		(!tp->config || g_str_equal(tp->config, get_config_name())) &&
		(!tp->storage || g_str_equal(tp->storage, storage)) &&
		((tp->pool && g_str_equal(tp->pool, tapepool)) ||
		 (!tp->pool && match_labelstr_template(l_template, tp->label,
						       tp->barcode, tp->meta)))) {
		tp->retention = TRUE;
	    }
	}
	g_free(datestr);
    }

    if (retention_recover) {
	find_result_t *ofr; /* output_find_result */
	char *datestr = get_timestamp_from_time(time(NULL) -
				retention_recover*86400);
	char *hostname  = "AlKHDA";
	char *diskname  = "ADJAOLDUIN";
	int   level     = -1;

	for (ofr = output_find;
	     ofr;
	     ofr = ofr->next) {
	    if (!g_str_equal(hostname, ofr->hostname) ||
		!g_str_equal(diskname, ofr->diskname)) {
		/* new dle */
		hostname  = ofr->hostname;
		diskname  = ofr->diskname;
		level     = -1;
	    }

	    if (ofr->level < level ||
		g_ascii_strcasecmp(ofr->timestamp, datestr) > 0) {
		if (ofr->label && ofr->label[0] != '/') {
		    tp = lookup_tapelabel(ofr->label);
		    if ((!tp->config || g_str_equal(tp->config, get_config_name())) &&
			(!tp->storage || g_str_equal(tp->storage, storage)) &&
			((tp->pool && g_str_equal(tp->pool, tapepool)) ||
			 (!tp->pool && match_labelstr_template(l_template, tp->label,
							       tp->barcode, tp->meta)))) {
			/* keep that label */
			tp->retention = 1;
		    }
		}
		level = ofr->level;
	    }
	}
    }

    if (retention_full) {
	find_result_t *ofr; /* output_find_result */
	char *hostname = "AlKHDA";
	char *diskname = "ADJAOLDUIN";
	int   count    = 0;

	sort_find_result("hkDLpbfw", &output_find);

	for (ofr = output_find;
	     ofr;
	     ofr = ofr->next) {
	    if (!g_str_equal(hostname, ofr->hostname) ||
		!g_str_equal(diskname, ofr->diskname)) {
		/* new dle */
		hostname = ofr->hostname;
		diskname = ofr->diskname;
		count    = 0;
	    }

	    if (ofr->level == 0 &&
		count < retention_full) {
		if (ofr->label && ofr->label[0] != '/') {
		    tp = lookup_tapelabel(ofr->label);
		    if ((!tp->config || g_str_equal(tp->config, get_config_name())) &&
			(!tp->storage || g_str_equal(tp->storage, storage)) &&
			((tp->pool && g_str_equal(tp->pool, tapepool)) ||
			 (!tp->pool && match_labelstr_template(l_template, tp->label,
							       tp->barcode, tp->meta)))) {
			/* keep that label */
			tp->retention = 1;
			count++;
		    }
		}
	    }
	}
    }
}

gchar **list_retention(void)
{
    int nb_tapes = 0;
    tape_t *tp;
    gchar **rv;
    int r;

    compute_retention();

    for (tp = tape_list; tp != NULL; tp = tp->next) {
        nb_tapes++;
    }

    rv = g_new0(gchar *, nb_tapes++);
    r = 0;
    for (tp = tape_list; tp != NULL; tp = tp->next) {
        if (tp->retention || tp->retention_nb)
	    rv[r++] = tp->label;
    }
    rv[r] = 0;
    return rv;
}

gchar **list_no_retention(void)
{
    int nb_tapes = 0;
    tape_t *tp;
    gchar **rv;
    int r;

    compute_retention();

    for (tp = tape_list; tp != NULL; tp = tp->next) {
        nb_tapes++;
    }

    rv = g_new0(gchar *, nb_tapes++);
    r = 0;
    for (tp = tape_list; tp != NULL; tp = tp->next) {
        if (!tp->retention && !tp->retention_nb)
	    rv[r++] = tp->label;
    }
    rv[r] = 0;
    return rv;
}

