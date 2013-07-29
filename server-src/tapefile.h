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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: tapefile.h,v 1.9 2006/05/25 01:47:20 johnfranks Exp $
 *
 * interface for active tape list manipulation routines
 */
#ifndef TAPEFILE_H
#define TAPEFILE_H

#include "amanda.h"

typedef struct tape_s {
    struct tape_s *next, *prev;
    int position;
    char * datestamp;
    int reuse;
    char *label;
    char *barcode;
    char *meta;
    guint64 blocksize;
    char *pool;
    char *storage;
    char *config;
    char *comment;
    gboolean   retention;	/* use internally */
    gboolean   retention_nb;	/* use internally */
} tape_t;

void compute_retention(void);
gchar **list_retention(void);
gchar **list_no_retention(void);

int read_tapelist(char *tapefile);
int write_tapelist(char *tapefile);
void clear_tapelist(void);
tape_t *lookup_tapelabel(const char *label);
tape_t *lookup_tapepos(int pos);
tape_t *lookup_tapedate(char *datestamp);
int lookup_nb_tape(void);
char *get_last_reusable_tape_label(const char *l_template,
				   const char *tapepool,
				   const char *storage,
				   int retention_tapes,
				   int retention_days, int retention_recover,
				   int retention_full, int skip);
tape_t *lookup_last_reusable_tape(const char *l_template,
				  const char *tapepool,
				   const char *storage,
				  int retention_tapes,
                                  int retention_days, int retention_recover,
                                  int retention_full, int skip);
void remove_tapelabel(const char *label);
int reusable_tape(tape_t *tp);
int volume_is_reusable(const char *label);

int guess_runs_from_tapelist(void);
gchar **list_new_tapes(char *storage_n, int nb);

#endif /* !TAPEFILE_H */
