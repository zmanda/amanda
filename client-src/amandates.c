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
 * $Id: amandates.c,v 1.21 2006/07/25 18:35:21 martinea Exp $
 *
 * manage amandates file, that mimics /etc/dumpdates, but stores
 * GNUTAR dates
 */

#include "amanda.h"
#include "getfsent.h"
#include "util.h"

#include "amandates.h"

static amandates_t *amandates_list = NULL;
static FILE *amdf = NULL;
static int updated, readonly;
static char *g_amandates_file = NULL;
static void import_dumpdates(amandates_t *);
static void enter_record(char *, int , time_t);
static amandates_t *lookup(char *name, int import);

int
start_amandates(
    char *amandates_file,
    int	  open_readwrite)
{
    int rc, level = 0;
    long ldate = 0L;
    char *line;
    char *name;
    char *s;
    int ch;
    char *qname;

    if (amandates_file == NULL) {
	errno = 0;
	return 0;
    }

    /* clean up from previous invocation */

    if(amdf != NULL)
	finish_amandates();
    if(amandates_list != NULL)
	free_amandates();
    amfree(g_amandates_file);

    /* initialize state */

    updated = 0;
    readonly = !open_readwrite;
    amdf = NULL;
    amandates_list = NULL;
    g_amandates_file = stralloc(amandates_file);
    /* open the file */

    if (access(amandates_file,F_OK))
	/* not yet existing */
	if ( (rc = open(amandates_file,(O_CREAT|O_RDWR),0644)) != -1 )
	    /* open/create successfull */
	    aclose(rc);

    if(open_readwrite)
	amdf = fopen(amandates_file, "r+");
    else
	amdf = fopen(amandates_file, "r");

    /* create it if we need to */

    if(amdf == NULL && (errno == EINTR || errno == ENOENT) && open_readwrite)
	amdf = fopen(amandates_file, "w");

    if(amdf == NULL)
	return 0;

    if(open_readwrite)
	rc = amflock(fileno(amdf), amandates_file);
    else
	rc = amroflock(fileno(amdf), amandates_file);

    if(rc == -1) {
	error(_("could not lock %s: %s"), amandates_file, strerror(errno));
	/*NOTREACHED*/
    }

    for(; (line = agets(amdf)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    continue;				/* no name field */
	}
	qname = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the name */
	name = unquote_string(qname);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d %ld", &level, &ldate) != 2) {
	    amfree(name);
	    continue;				/* no more fields */
	}

	if(level < 0 || level >= DUMP_LEVELS) {
	    amfree(name);
	    continue;
	}

	enter_record(name, level, (time_t) ldate);
	amfree(name);
    }

    if(ferror(amdf)) {
	error(_("reading %s: %s"), amandates_file, strerror(errno));
	/*NOTREACHED*/
    }

    updated = 0;	/* reset updated flag */
    return 1;
}

void
finish_amandates(void)
{
    amandates_t *amdp;
    int level;
    char *qname;

    if(amdf == NULL)
	return;

    if(updated) {
	if(readonly) {
	    error(_("updated amandates after opening readonly"));
	    /*NOTREACHED*/
	}

	rewind(amdf);
	for(amdp = amandates_list; amdp != NULL; amdp = amdp->next) {
	    for(level = 0; level < DUMP_LEVELS; level++) {
		if(amdp->dates[level] == EPOCH) continue;
		qname = quote_string(amdp->name);
		g_fprintf(amdf, "%s %d %ld\n",
			qname, level, (long) amdp->dates[level]);
		amfree(qname);
	    }
	}
    }

    if(amfunlock(fileno(amdf), g_amandates_file) == -1) {
	error(_("could not unlock %s: %s"), g_amandates_file, strerror(errno));
	/*NOTREACHED*/
    }
    if (fclose(amdf) == EOF) {
	error(_("error [closing %s: %s]"), g_amandates_file, strerror(errno));
	/*NOTREACHED*/
    }
    amdf = NULL;
}

void
free_amandates(void)
{
    amandates_t *amdp, *nextp;

    for(amdp = amandates_list; amdp != NULL; amdp = nextp) {
	nextp = amdp->next;
	amfree(amdp->name);
	amfree(amdp);
    }
    amandates_list = NULL;
}

static amandates_t *
lookup(
    char *	name,
    int		import)
{
    amandates_t *prevp, *amdp;
    int rc, level;

    (void)import;	/* Quiet unused parameter warning */
    rc = 0;

    prevp = NULL;
    amdp = amandates_list;
    while (amdp != NULL) {
	if ((rc = strcmp(name, amdp->name)) <= 0)
	    break;
	prevp = amdp;
	amdp = amdp->next;
    }
    if (!(amdp && (rc == 0))) {
	amandates_t *newp = alloc(SIZEOF(amandates_t));
	newp->name = stralloc(name);
	for (level = 0; level < DUMP_LEVELS; level++)
	    newp->dates[level] = EPOCH;
	newp->next = amdp;
	if (prevp != NULL) {
#ifndef __lint	/* Remove complaint about NULL pointer assignment */
	    prevp->next = newp;
#else
	    (void)prevp;
#endif
	} else {
	    amandates_list = newp;
	}
	import_dumpdates(newp);
	return newp;
    }
    return amdp;
}

amandates_t *
amandates_lookup(
    char *	name)
{
    return lookup(name, 1);
}

static void
enter_record(
    char *	name,
    int		level,
    time_t	dumpdate)
{
    amandates_t *amdp;
    char *qname;

    amdp = lookup(name, 0);

    if(level < 0 || level >= DUMP_LEVELS || dumpdate < amdp->dates[level]) {
	qname = quote_string(name);
	/* this is not allowed, but we can ignore it */
        dbprintf(_("amandates botch: %s lev %d: new dumpdate %ld old %ld\n"),
		  qname, level, (long) dumpdate, (long) amdp->dates[level]);
	amfree(qname);
	return;
    }

    amdp->dates[level] = dumpdate;
}


void
amandates_updateone(
    char *	name,
    int		level,
    time_t	dumpdate)
{
    amandates_t *amdp;
    char *qname;

    assert(!readonly);

    amdp = lookup(name, 1);

    if(level < 0 || level >= DUMP_LEVELS || dumpdate < amdp->dates[level]) {
	/* this is not allowed, but we can ignore it */
	qname = quote_string(name);
	dbprintf(_("amandates updateone: %s lev %d: new dumpdate %ld old %ld"),
		  name, level, (long) dumpdate, (long) amdp->dates[level]);
	amfree(qname);
	return;
    }

    amdp->dates[level] = dumpdate;
    updated = 1;
}


/* -------------------------- */

static void
import_dumpdates(
    amandates_t *	amdp)
{
    char *devname;
    char *line;
    char *fname;
    int level = 0;
    time_t dumpdate;
    FILE *dumpdf;
    char *s;
    int ch;

    devname = amname_to_devname(amdp->name);

    if((dumpdf = fopen("/etc/dumpdates", "r")) == NULL) {
	amfree(devname);
	return;
    }

    for(; (line = agets(dumpdf)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    continue;				/* no fname field */
	}
	fname = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';				/* terminate fname */

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    continue;				/* no level field */
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    continue;				/* no dumpdate field */
	}
	dumpdate = unctime(s-1);

	if(strcmp(fname, devname) != 0 || level < 0 || level >= DUMP_LEVELS) {
	    continue;
	}

	if(dumpdate != -1 && dumpdate > amdp->dates[level]) {
	    if(!readonly) updated = 1;
	    amdp->dates[level] = dumpdate;
	}
    }
    afclose(dumpdf);
    amfree(devname);
}
