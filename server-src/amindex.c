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
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: amindex.c,v 1.15 2006/05/25 01:47:19 johnfranks Exp $
 *
 * index control
 */

#include "amanda.h"
#include "conffile.h"
#include "amindex.h"

char *
getindexfname(
    char *	host,
    char *	disk,
    char *	date,
    int		level)
{
  char *conf_indexdir;
  char *buf;
  char level_str[NUM_STR_SIZE];
  char datebuf[14 + 1];
  char *dc = NULL;
  char *pc;
  int ch;

  if (date != NULL) {
    dc = date;
    pc = datebuf;
    while (pc < datebuf + SIZEOF(datebuf)) {
      ch = *dc++;
      *pc++ = (char)ch;
      if (ch == '\0') {
        break;
      } else if (! isdigit (ch)) {
        pc--;
      }
    }
    datebuf[SIZEOF(datebuf)-1] = '\0';
    dc = datebuf;

    g_snprintf(level_str, SIZEOF(level_str), "%d", level);
  }

  host = sanitise_filename(host);
  if (disk != NULL) {
    disk = sanitise_filename(disk);
  }

  conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));
  /*
   * Note: vstralloc() will stop at the first NULL, which might be
   * "disk" or "dc" (datebuf) rather than the full file name.
   */
  buf = vstralloc(conf_indexdir, "/",
		  host, "/",
		  disk, "/",
		  dc, "_",
		  level_str, COMPRESS_SUFFIX,
		  NULL);

  amfree(conf_indexdir);
  amfree(host);
  amfree(disk);

  return buf;
}

char *
getoldindexfname(
    char *	host,
    char *	disk,
    char *	date,
    int		level)
{
  char *conf_indexdir;
  char *buf;
  char level_str[NUM_STR_SIZE];
  char datebuf[14 + 1];
  char *dc = NULL;
  char *pc;
  int ch;

  if (date != NULL) {
    dc = date;
    pc = datebuf;
    while (pc < datebuf + SIZEOF(datebuf)) {
      ch = *dc++;
      *pc++ = (char)ch;
      if (ch == '\0') {
        break;
      } else if (! isdigit (ch)) {
        pc--;
      }
    }
    datebuf[SIZEOF(datebuf)-1] = '\0';
    dc = datebuf;

    g_snprintf(level_str, SIZEOF(level_str), "%d", level);
  }

  host = old_sanitise_filename(host);
  if (disk != NULL) {
    disk = old_sanitise_filename(disk);
  }

  conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));
  /*
   * Note: vstralloc() will stop at the first NULL, which might be
   * "disk" or "dc" (datebuf) rather than the full file name.
   */
  buf = vstralloc(conf_indexdir, "/",
		  host, "/",
		  disk, "/",
		  dc, "_",
		  level_str, COMPRESS_SUFFIX,
		  NULL);

  amfree(conf_indexdir);
  amfree(host);
  amfree(disk);

  return buf;
}
