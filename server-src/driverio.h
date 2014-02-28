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
 * $Id: driverio.h,v 1.35 2006/05/25 01:47:19 johnfranks Exp $
 *
 * driver-related helper functions
 */
#ifndef DRIVERIO_H
#define DRIVERIO_H

#include "amanda.h"
#include "event.h"
#include "holding.h"
#include "server_util.h"

#ifndef GLOBAL
#define GLOBAL extern
#endif

typedef enum {
   TAPER_STATE_DEFAULT        = 0,        //   0 Before TAPER-START
   TAPER_STATE_INIT           = (1 << 0), //   1 Between TAPE-START and TAPE-OK
   TAPER_STATE_RESERVATION    = (1 << 1), //   2 Have a reservation
   TAPER_STATE_IDLE           = (1 << 2), //   4 tape started and do nothing
   TAPER_STATE_DUMP_TO_TAPE   = (1 << 3), //   8 Doing a PORT-WRITE
   TAPER_STATE_FILE_TO_TAPE   = (1 << 4), //  16 Doing a FILE-WRITE
   TAPER_STATE_TAPE_REQUESTED = (1 << 5), //  32 REQUEST-NEW-TAPE received
   TAPER_STATE_WAIT_FOR_TAPE  = (1 << 6), //  64 if taper wait for a tape,
                                          //     after a START-SCAN send
   TAPER_STATE_WAIT_NEW_TAPE  = (1 << 7), // 128 AFTER NEW-TAPE sent and before
                                          //     NEW-TAPE received
   TAPER_STATE_TAPE_STARTED   = (1 << 8), // 256 taper already started to write
                                          //     to a tape.
   TAPER_STATE_DONE           = (1 << 9),
} TaperState;

typedef struct job_s {
    int               in_use;
    struct disk_s    *disk;
    struct chunker_s *chunker;
    struct dumper_s  *dumper;
    struct wtaper_s  *wtaper;
} job_t;

/* chunker process structure */

typedef struct chunker_s {
    char *name;			/* name of this chunker */
    pid_t pid;			/* its pid */
    int down;			/* state */
    int fd;			/* read/write */
    int result;
    int sendresult;
    event_handle_t *ev_read;	/* read event handle */
    job_t *job;
} chunker_t;

/* dumper process structure */

typedef struct dumper_s {
    char *name;			/* name of this dumper */
    pid_t pid;			/* its pid */
    int busy, down;		/* state */
    int fd;			/* read/write */
    int result;
    int output_port;		/* output port */
    event_handle_t *ev_read;	/* read event handle */
    job_t *job;
} dumper_t;

typedef struct wtaper_s {
    char       *name;                  /* name of this taper */
    int         sendresult;
    char       *input_error;
    char       *tape_error;
    int         result;
    job_t      *job;
    char       *first_label;
    off_t       first_fileno;
    TaperState  state;
    off_t       left;
    off_t       written;               // Number of kb already written to tape
    int         nb_dle;                /* number of dle on the volume */
    gboolean    ready;
    struct taper_s *taper;
} wtaper_t;

typedef struct taper_s {
    char           *name;
    char           *storage_name;
    pid_t           pid;
    int             fd;
    event_handle_t *ev_read;
    int             nb_wait_reply;
    int             nb_worker;
    int             nb_scan_volume;
    off_t           tape_length;
    int             runtapes;
    int             max_dle_by_volume;
    int             current_tape;
    off_t           flush_threshold_dumped;
    off_t           flush_threshold_scheduled;
    off_t           taperflush;
    wtaper_t       *wtapetable;
    wtaper_t       *last_started_wtaper;
    disklist_t      tapeq;
    gboolean        degraded_mode;
} taper_t;

/* holding disk reservation structure; this is built as a list parallel
 * to the configuration's linked list of holding disks. */

typedef struct holdalloc_s {
    struct holdalloc_s *next;
    holdingdisk_t *hdisk;

    off_t disksize;
    int allocated_dumpers;
    off_t allocated_space;
} holdalloc_t;

typedef struct assignedhd_s {
    holdalloc_t		*disk;
    off_t		used;
    off_t		reserved;
    char		*destname;
} assignedhd_t;

/* schedule structure */

typedef struct sched_s {
    int dump_attempted;
    int taper_attempted;
    int  priority;
    int level, degr_level;
    unsigned long est_time, degr_time;
    off_t est_nsize, est_csize, est_size;
    off_t degr_nsize, degr_csize, act_size;
    off_t origsize, dumpsize;
    time_t dumptime;
    char *dumpdate, *degr_dumpdate;
    unsigned long est_kps, degr_kps;
    char *destname;				/* file/port name */
    assignedhd_t **holdp;
    time_t timestamp;
    char *datestamp;
    int activehd;
    int no_space;
    char *degr_mesg;
    crc_t native_crc;
    crc_t client_crc;
    crc_t server_crc;
    int   nb_flush;
    GHashTable *to_storage;
} sched_t;

#define sched(dp)	((sched_t *) (dp)->up)


GLOBAL dumper_t  *dmptable;
GLOBAL chunker_t *chktable;
GLOBAL taper_t   *tapetable;

/* command/result tokens */

GLOBAL char *log_filename;

void init_driverio(int inparallel, int nb_storage, int taper_parallel_write);
void startup_tape_process(char *taper_program, gboolean no_taper);
void startup_dump_process(dumper_t *dumper, char *dumper_program);
void startup_dump_processes(char *dumper_program, int inparallel, char *timestamp);
void startup_chunk_process(chunker_t *chunker, char *chunker_program);

cmd_t getresult(int fd, int show, int *result_argc, char ***result_argv);

job_t * alloc_job(void);
void free_job(job_t *job);

job_t *serial2job(char *str);
void free_serial(char *str);
void free_serial_job(job_t *job);
void check_unfree_serial(void);
char *job2serial(job_t *job);
void update_info_dumper(disk_t *dp, off_t origsize, off_t dumpsize, time_t dumptime);
void update_info_taper(disk_t *dp, char *label, off_t filenum, int level);
void free_assignedhd(assignedhd_t **holdp);
#endif	/* !DRIVERIO_H */
