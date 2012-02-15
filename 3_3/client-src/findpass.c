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
 * $Id: findpass.c,v 1.13 2006/05/25 01:47:11 johnfranks Exp $
 *
 * Support routines for Amanda SAMBA support
 */

#include "amanda.h"
#include "findpass.h"
#include "util.h"

/*
 * Find the Samba password and optional domain for a given disk.
 * Returns pointers into an alloc-ed area.  The caller should clear them
 * as soon as reasonable.
 */

char *
findpass(
    char *	disk,
    char **	domain)
{
  FILE *fp;
  static char *buffer = NULL;
  char *s, *d, *pw = NULL;
  int ch;
  char *qname;

  *domain = NULL;				/* just to be sure */
  if ( (fp = fopen("/etc/amandapass", "r")) ) {
    amfree(buffer);
    for (; (buffer = agets(fp)) != NULL; free(buffer)) {
      if (buffer[0] == '\0')
	continue;
      s = buffer;
      ch = *s++;
      skip_whitespace(s, ch);			/* find start of disk name */
      if (!ch || ch == '#') {
	continue;
      }
      qname = s-1;				/* start of disk name */
      skip_quoted_string(s, ch);
      if (ch && ch != '#') {
	s[-1] = '\0';				/* terminate disk name */
	d = unquote_string(qname);
	if ((strcmp(d,"*") == 0) || (strcmp(disk, d) == 0)) {
	  skip_whitespace(s, ch);		/* find start of password */
	  if (ch && ch != '#') {
	    pw = s - 1;				/* start of password */
	    skip_non_whitespace_cs(s, ch);
	    s[-1] = '\0';			/* terminate password */
	    pw = stralloc(pw);
	    skip_whitespace(s, ch);		/* find start of domain */
	    if (ch && ch != '#') {
	      *domain = s - 1;			/* start of domain */
	      skip_non_whitespace_cs(s, ch);
	      s[-1] = '\0';			/* terminate domain */
	      *domain = stralloc(*domain);
	    }
	  }
	  amfree(d);
	  break;
	}
	amfree(d);
      }
    }
    afclose(fp);
  }
  return pw;
}

/* 
 * Convert an amanda disk-name into a Samba sharename,
 * optionally for a shell execution (\'s are escaped).
 * Returns a new name alloc-d that the caller is responsible
 * for free-ing.
 */
char *
makesharename(
    char *	disk,
    int		shell)
{
  char *buffer;
  size_t buffer_size;
  char *s;
  int ch;
  
  buffer_size = 2 * strlen(disk) + 1;		/* worst case */
  buffer = alloc(buffer_size);

  s = buffer;
  while ((ch = *disk++) != '\0') {
    if (s >= buffer+buffer_size-2) {		/* room for escape */
      amfree(buffer);				/* should never happen */
      return NULL;				/* buffer not big enough */
    }
    if (ch == '/') {
      ch = '\\';				/* convert '/' to '\\' */
    }
    if (ch == '\\' && shell) {
      *s++ = '\\';				/* add escape for shell */
    }
    *s++ = ch;
  }
  *s = '\0';					/* terminate the share name */
  return buffer;
}

/*
 * find out if the samba sharename specifies both a share
 * and a target subdirectory or just a share
 *
 * the caller is expected to release share & subdir
 */
void
parsesharename(
    char *	disk,
    char **	share,
    char **	subdir)
{
    char *ch=NULL;
    int slashcnt=0;

    *share = NULL;
    *subdir = NULL;
    if (!disk) {
	return;
    }
    *share = stralloc(disk);
    ch = *share;
    *subdir = NULL;
    while (*ch != '\0') {
	if (*ch == '/') {slashcnt++;}
	if (slashcnt == 4) {
	    *ch = '\0';
	    *subdir = stralloc(++ch);
	    return;
	}
	ch++;
    }
}

