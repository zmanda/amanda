/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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
 */
/*
 */
#include "amanda.h"

typedef enum cmdoperation_e {
    CMD_COPY,
    CMD_FLUSH
} cmdoperation_t;

typedef enum cmdstatus_e {
    CMD_DONE,
    CMD_TODO,
    CMD_PARTIAL
} cmdstatus_t;

typedef struct cmddata_label_s {
    char *label;
    int   file;
} cmddata_label_t;

typedef struct cmddata_s {
    /* change here must also be done in Cmdfile.swg */
    int             id;
    cmdoperation_t  operation;
    char           *config;
    char           *src_storage;
    char           *src_pool;
    char           *src_label;
    int             src_fileno;
    char           *src_labels_str;
    GSList         *src_labels;
    char           *holding_file;
    char           *hostname;
    char           *diskname;
    char           *dump_timestamp;
    int             level;
    char           *dst_storage;
    pid_t           working_pid;
    cmdstatus_t     status;
    int             todo;
    off_t           size;
    time_t          start_time;
} cmddata_t;

typedef GHashTable *cmdfile_t; /* hash where each element is a (cmddata_t *) */

typedef struct cmdatas_s {
    int        version;
    int        max_id;
    file_lock *lock;
    cmdfile_t  cmdfile;
} cmddatas_t;

void unlock_cmdfile(cmddatas_t *cmddatas);
cmddatas_t *read_cmdfile(char *filename);
void close_cmdfile(cmddatas_t *cmddatas);
void write_cmdfile(cmddatas_t *cmddatas);
int add_cmd_in_memory(cmddatas_t *cmddatas, cmddata_t *cmddata);
cmddatas_t *add_cmd_in_cmdfile(cmddatas_t *cmddatas, cmddata_t *cmddata);
cmddatas_t *remove_cmd_in_cmdfile(cmddatas_t *cmddatas, int id);
cmddatas_t *change_cmd_in_cmdfile(cmddatas_t *cmddatas, int id, cmdstatus_t status, off_t size);
cmddatas_t *remove_working_in_cmdfile(cmddatas_t *cmddatas, pid_t pid);
gboolean holding_in_cmdfile(cmddatas_t *cmddatas, char *holding_file);
char *cmdfile_get_ids_for_holding(cmddatas_t *cmddatas, char *holding_file);

