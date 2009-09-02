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
 * $Id: restore.h,v 1.8 2006/06/22 17:16:39 martinea Exp $
 *
 * 
 */

#ifndef RESTORE_H
#define RESTORE_H

#include "fileheader.h"
#include "tapelist.h"
#include "amfeatures.h"
#include "device.h"

#define CREAT_MODE  0640

typedef struct rst_flags_s {
    unsigned int inline_assemble:1;
    unsigned int delay_assemble:1;
    unsigned int compress:1;
    unsigned int leave_comp:1;
    unsigned int raw:1;
    unsigned int headers:1;
    unsigned int isafile:1;
    unsigned int wait_tape_prompt:1; /* for interactive console use */
    unsigned int amidxtaped:1; /* for client-daemon use */
    unsigned int check_labels:1;
    unsigned int mask_splits:1;
    off_t fsf;
    ssize_t blocksize;
    int pipe_to_fd;
    int header_to_fd;
    char *restore_dir;
    char *comp_type;
    char *alt_tapedev;
    char *inventory_log;
} rst_flags_t;

typedef struct {
    enum { HOLDING_MODE, DEVICE_MODE} restore_mode;
    dumpfile_t * header;
    union {
        int holding_fd;
        Device * device;
    } u;
} RestoreSource;

typedef struct seentapes_s seentapes_t;

char *make_filename(dumpfile_t *file);
ssize_t read_file_header(dumpfile_t *file, int tapefd, int isafile,
			 rst_flags_t *flags);
void restore(RestoreSource * source, rst_flags_t * flags);
gboolean restore_holding_disk(FILE * prompt_out,
                              rst_flags_t * flags,
                              am_feature_t * features,
                              tapelist_t * file,
                              seentapes_t ** seen,
                              GSList * dumpspecs,
                              dumpfile_t * this_header,
                              dumpfile_t * last_header);

gboolean search_a_tape(Device * device, FILE *prompt_out, rst_flags_t  *flags,
                       am_feature_t *their_features, 
                       tapelist_t   *desired_tape, GSList *dumpspecs,
                       seentapes_t **tape_seen,
                       dumpfile_t * first_restored_file, int tape_count,
                       FILE * logstream);

void flush_open_outputs(int reassemble, dumpfile_t *only_file);
void search_tapes(FILE *prompt_out, FILE *prompt_in, int use_changer,
		  tapelist_t *tapelist, GSList *dumpspecs,
		  rst_flags_t *flags, am_feature_t *their_features);
int have_all_parts(dumpfile_t *file, int upto);
rst_flags_t *new_rst_flags(void);
int check_rst_flags(rst_flags_t *flags);
void free_rst_flags(rst_flags_t *flags);
int lock_logfile(void);
void send_message(FILE *prompt_out, rst_flags_t *flags,
		  am_feature_t *their_features,
		  char * format, ...) G_GNUC_PRINTF(4,5);
gboolean set_restore_device_read_buffer_size(Device *device, rst_flags_t *flags);

#endif /* RESTORE_H */

