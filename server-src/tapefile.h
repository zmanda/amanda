/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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

typedef enum {
    RETENTION_NO           = 0,
    RETENTION_NO_REUSE     = (1<<0),
    RETENTION_TAPES        = (1<<1),
    RETENTION_DAYS         = (1<<2),
    RETENTION_RECOVER      = (1<<3),
    RETENTION_FULL         = (1<<4),
    RETENTION_CMD_COPY     = (1<<5),
    RETENTION_CMD_FLUSH    = (1<<6),
    RETENTION_CMD_RESTORE  = (1<<7),
    RETENTION_OTHER_CONFIG = (1<<8)
} RetentionType;

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
    RetentionType retention_type;
} tape_t;

void compute_retention(void);
gchar **list_retention(void);
gchar **list_no_retention(void);

int read_tapelist(char *tapefile);
int write_tapelist(char *tapefile);
void clear_tapelist(void);
void reset_tapelist(void);
tape_t *lookup_tapepoollabel(const char *pool, const char *label);
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
tape_t * add_tapelabel(const char *datestamp,
		       const char *label,
		       const char *comment,
		       gboolean    reuse,
		       const char *meta,
		       const char *barcode,
		       guint64     blocksize,
		       const char *pool,
		       const char *storage,
		       const char *config);
int reusable_tape(tape_t *tp);
int volume_is_reusable(const char *label);

int guess_runs_from_tapelist(void);
gchar **list_new_tapes(char *storage_n, int nb);
RetentionType get_retention_type(char *pool, char *label);

#endif /* !TAPEFILE_H */
