/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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
 */
/*
 */
#include "amanda.h"
#include "amutil.h"
#include "cmdfile.h"

static void free_cmddata(gpointer p);
//static cmddata_t *duplicate_cmddata(cmddata_t *cmddata);

static void
free_cmddata(
    gpointer p)
{
    cmddata_t *cmddata = p;
    g_free(cmddata->config);
    g_free(cmddata->src_storage);
    g_free(cmddata->src_pool);
    g_free(cmddata->src_label);
    g_free(cmddata->src_labels_str);
    slist_free_full(cmddata->src_labels, g_free);
    g_free(cmddata->holding_file);
    g_free(cmddata->hostname);
    g_free(cmddata->diskname);
    g_free(cmddata->dump_timestamp);
    g_free(cmddata->dst_storage);
    g_free(cmddata);
}

/*
static cmddata_t *
duplicate_cmddata(
    cmddata_t *cmddata)
{
    cmddata_t *new_cmddata = g_new0(cmddata_t, 1);

    new_cmddata->id = cmddata->id;
    new_cmddata->operation = cmddata->operation;
    new_cmddata->config = g_strdup(cmddata->config);
    new_cmddata->storage = g_strdup(cmddata->storage);
    new_cmddata->pool = g_strdup(cmddata->pool);
    new_cmddata->label = g_strdup(cmddata->label);
    new_cmddata->holding_file = g_strdup(cmddata->holding_file);
    new_cmddata->hostname = g_strdup(cmddata->hostname);
    new_cmddata->diskname = g_strdup(cmddata->diskname);
    new_cmddata->dump_timestamp = g_strdup(cmddata->dump_timestamp);
    new_cmddata->dst_storage = g_strdup(cmddata->dst_storage);
    new_cmddata->working_pid = cmddata->working_pid;
    new_cmddata->status = cmddata->status;
    new_cmddata->size = cmddata->size;
    new_cmddata->start_time = cmddata->start_time;

    return new_cmddata;
}
*/

void
unlock_cmdfile(
    cmddatas_t *cmddatas)
{
    file_lock_unlock(cmddatas->lock);
}

void
close_cmdfile(
    cmddatas_t *cmddatas)
{
    file_lock_free(cmddatas->lock);
    g_hash_table_destroy(cmddatas->cmdfile);
    g_free(cmddatas);
}

static int checked_working_pid = 0;
#define NB_PIDS 10

cmddatas_t *
read_cmdfile(
    char *filename)
{
    cmddatas_t *cmddatas = g_new0(cmddatas_t, 1);
    cmddata_t  *cmddata;
    char  *s, *fp, *operation;
    char   ch;
    char **xlines;
    int    i;
    int    pid;
    pid_t  pids[NB_PIDS];
    pid_t  new_pids[NB_PIDS];
    int    nb_pids = 0;
    int    result;

    cmddatas->lock = file_lock_new(filename);
    cmddatas->cmdfile = g_hash_table_new_full(g_direct_hash, g_direct_equal,
					      NULL, &free_cmddata);
    // open
    while ((result = file_lock_lock(cmddatas->lock)) == 1) {
        sleep(1);
    }
    if (result != 0) {
        g_debug("read_cmdfile open failed: %s", strerror(errno));
    }

    if (!cmddatas->lock->data) {
	cmddatas->version = 1;
	cmddatas->max_id = 0;
	return cmddatas;
    }
    xlines = g_strsplit(cmddatas->lock->data, "\n", 0);

    // read
    if (sscanf(xlines[0], "VERSION %d", &cmddatas->version) != 1) {};
    if (sscanf(xlines[1], "ID %d", &cmddatas->max_id) != 1) {};

    // read cmd
    for (i=2; xlines[i] != NULL; i++) {
	int id;
	s = xlines[i];
	if (*s == '\0') continue;
	ch = *s++;
	skip_whitespace(s, ch);
	if (ch == '\0' || sscanf((s - 1), "%d", &id) != 1) {
	    continue;
	}
	skip_integer(s, ch);
	skip_whitespace(s, ch);
	operation = s - 1;
        skip_non_whitespace(s, ch);
        s[-1] = '\0';
	if (!g_str_equal(operation, "FLUSH") &&
	    !g_str_equal(operation, "COPY")) {
	    g_debug("BAD operation %s: %s", operation, s);
	    continue;
	}
	cmddata = g_new0(cmddata_t, 1);
	cmddata->id = id;
	skip_whitespace(s, ch);
	fp = s - 1;
        skip_non_whitespace(s, ch);
        s[-1] = '\0';
        cmddata->config = unquote_string(fp);
	if (g_str_equal(operation, "FLUSH")) {
	    cmddata->operation = CMD_FLUSH;
	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    cmddata->holding_file = unquote_string(fp);
	} else if (g_str_equal(operation, "COPY")) {
	    char *src_labels;
	    char *slabels;
	    char *a;

	    cmddata->operation = CMD_COPY;
	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    cmddata->src_storage = unquote_string(fp);
	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    cmddata->src_pool = unquote_string(fp);
	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    cmddata->src_label = unquote_string(fp);
	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_integer(s, ch);
	    s[-1] = '\0';
	    cmddata->src_fileno = atoi(fp);

	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    slabels = src_labels = unquote_string(fp);
	    cmddata->src_labels_str = g_strdup(src_labels);
	    a = strstr(slabels, " ;");
	    if (a) {
		slabels = a+2;
		while ((a = strstr(slabels, " ;"))) {
		    *a = '\0';
		    cmddata->src_labels = g_slist_append(cmddata->src_labels, g_strdup(slabels));
		    slabels = a+2;
		}
	    }
	    g_free(src_labels);

	    skip_whitespace(s,ch);
	    fp = s - 1;
	    skip_integer(s, ch);
	    s[-1] = '\0';
	    cmddata->start_time = atoll(fp);
	} else {
	}
	skip_whitespace(s, ch);
	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	cmddata->hostname = unquote_string(fp);
	skip_whitespace(s, ch);
	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	cmddata->diskname = unquote_string(fp);
	skip_whitespace(s, ch);
	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	cmddata->dump_timestamp = unquote_string(fp);
	skip_whitespace(s,ch);
	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	cmddata->level = atoi(fp);
	skip_whitespace(s, ch);
	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	cmddata->dst_storage = unquote_string(fp);
	skip_whitespace(s, ch);
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	if (sscanf(fp, "WORKING:%d", &pid) != 1) {
	}
	cmddata->working_pid = pid;
	skip_whitespace(s, ch);
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	if (g_str_equal(fp, "DONE")) {
	    cmddata->status = CMD_DONE;
	} else if (g_str_equal(fp, "TODO")) {
	    cmddata->status = CMD_TODO;
	} else if (strncmp(fp, "PARTIAL", 7) == 0) {
	    long long lsize;
	    cmddata->status = CMD_PARTIAL;
	    if (sscanf(fp, "PARTIAL:%lld", &lsize) != 1) {
	    } else {
		cmddata->size = lsize;
	    }
	} else {
	}

	/* validate working_pid */
	if (!checked_working_pid && cmddata->working_pid != 0) {
	    int   i;

	    for (i = 0; i < nb_pids; i++) {
		if (pids[i] == cmddata->working_pid) {
		    cmddata->working_pid = new_pids[i];
		    i += 100;
		    continue;
		}
	    }
	    if (nb_pids < NB_PIDS) {
		pids[nb_pids] = cmddata->working_pid;
		if (kill(cmddata->working_pid, 0) != 0)
		    cmddata->working_pid =0;
		new_pids[nb_pids] = cmddata->working_pid;
		nb_pids++;
	    }
	}

	g_hash_table_insert(cmddatas->cmdfile, GINT_TO_POINTER(cmddata->id), cmddata);
    }

    g_strfreev(xlines);
    checked_working_pid = 1;

    return cmddatas;
}

static void
cmdfile_write(
    gpointer key,
    gpointer value,
    gpointer user_data)
{
    int id = GPOINTER_TO_INT(key);
    cmddata_t *cmddata = value;
    GPtrArray *lines = user_data;
    char *line;
    char *config;
    char *hostname;
    char *diskname;
    char *dump_timestamp;
    char *dst_storage;
    char *status;

    assert(id == cmddata->id);

    if (cmddata->status == CMD_DONE && cmddata->working_pid == 0)
	return;

    config = quote_string(cmddata->config);
    hostname = quote_string(cmddata->hostname);
    diskname = quote_string(cmddata->diskname);
    dump_timestamp = quote_string(cmddata->dump_timestamp);
    dst_storage = quote_string(cmddata->dst_storage);

    switch (cmddata->status) {
        case CMD_DONE: status = g_strdup("DONE");
		       break;
        case CMD_TODO: status = g_strdup("TODO");
		       break;
        case CMD_PARTIAL: status = g_strdup_printf(
				"PARTIAL:%lld", (long long)cmddata->size);
		          break;
	default: status = NULL;
		 break;
    }
    if (cmddata->operation == CMD_FLUSH) {
	char *holding_file = quote_string(cmddata->holding_file);
	line = g_strdup_printf("%d FLUSH %s %s %s %s %s %d %s WORKING:%d %s\n",
		id, config, holding_file, hostname, diskname,
		dump_timestamp, cmddata->level, dst_storage, (int)cmddata->working_pid, status);
	g_free(holding_file);
	g_ptr_array_add(lines, line);
    } else if (cmddata->operation == CMD_COPY) {
	char *src_storage = quote_string(cmddata->src_storage);
	char *src_pool = quote_string(cmddata->src_pool);
	char *src_label = quote_string(cmddata->src_label);
	char *src_labels_str = quote_string(cmddata->src_labels_str);

	line = g_strdup_printf("%d COPY %s %s %s %s %d %s %lu %s %s %s %d %s WORKING:%d %s\n",
		id, config, src_storage, src_pool, src_label, cmddata->src_fileno,
		src_labels_str,
		(unsigned long int) cmddata->start_time,
		hostname, diskname, dump_timestamp, cmddata->level,
		dst_storage,
		(int)cmddata->working_pid, status);
	g_free(src_storage);
	g_free(src_pool);
	g_free(src_label);
	g_free(src_labels_str);
	g_ptr_array_add(lines, line);
    } else {
    }
    g_free(config);
    g_free(hostname);
    g_free(diskname);
    g_free(dump_timestamp);
    g_free(dst_storage);
    g_free(status);
}

// we already have the lock
// remove the lock
void
write_cmdfile(
    cmddatas_t *cmddatas)
{
    GPtrArray *lines = g_ptr_array_sized_new(100);
    char *buffer;

    // generate
    g_ptr_array_add(lines, g_strdup_printf("VERSION %d\n", cmddatas->version));
    g_ptr_array_add(lines, g_strdup_printf("ID %d\n", cmddatas->max_id));
    g_hash_table_foreach(cmddatas->cmdfile, &cmdfile_write, lines);
    g_ptr_array_add(lines, NULL);
    buffer = g_strjoinv(NULL, (gchar **)lines->pdata);
    g_ptr_array_free_full(lines);

    // write
    file_lock_write(cmddatas->lock, buffer, strlen(buffer));
    g_free(buffer);

    // unlock
    file_lock_unlock(cmddatas->lock);
}

int
add_cmd_in_memory(
    cmddatas_t *cmddatas,
    cmddata_t  *cmddata)
{
    cmddatas->max_id++;
    cmddata->id = cmddatas->max_id;

    g_hash_table_insert(cmddatas->cmdfile,
		        GINT_TO_POINTER(cmddata->id), cmddata);
    return cmddata->id;
}

cmddatas_t *
add_cmd_in_cmdfile(
    cmddatas_t *cmddatas,
    cmddata_t  *cmddata)
{
    cmddatas_t *new_cmddatas;

    // take the lock and read
    new_cmddatas = read_cmdfile(cmddatas->lock->filename);

    // add the cmd
    new_cmddatas->max_id++;
    cmddata->id = new_cmddatas->max_id;
    g_hash_table_insert(new_cmddatas->cmdfile,
		        GINT_TO_POINTER(new_cmddatas->max_id), cmddata);

    // write
    write_cmdfile(new_cmddatas);
    close_cmdfile(cmddatas);
    return new_cmddatas;
}

cmddatas_t *
remove_cmd_in_cmdfile(
    cmddatas_t *cmddatas,
    int         id)
{
    cmddatas_t *new_cmddatas;

    // take the lock and read
    new_cmddatas = read_cmdfile(cmddatas->lock->filename);

    // remove the cmd id
    g_hash_table_remove(new_cmddatas->cmdfile, GINT_TO_POINTER(id));

    // write
    write_cmdfile(new_cmddatas);
    close_cmdfile(cmddatas);
    return new_cmddatas;
}

cmddatas_t *
change_cmd_in_cmdfile(
    cmddatas_t  *cmddatas,
    int          id,
    cmdstatus_t  status,
    off_t        size)
{
    cmddatas_t *new_cmddatas;
    cmddata_t  *cmddata;

    // take the lock and read
    new_cmddatas = read_cmdfile(cmddatas->lock->filename);

    // update status for cmd id in cmddatas and new_cmddatas
    cmddata = g_hash_table_lookup(new_cmddatas->cmdfile, GINT_TO_POINTER(id));
    cmddata->status = status;
    cmddata->size   = size;

    // write
    write_cmdfile(new_cmddatas);
    close_cmdfile(cmddatas);
    return new_cmddatas;
}

static void
cmdfile_remove_working(
    gpointer key G_GNUC_UNUSED,
    gpointer value,
    gpointer user_data)
{
    cmddata_t *cmddata = value;
    pid_t pid = *(pid_t *)user_data;

    if (cmddata->working_pid == pid) {
	cmddata->working_pid = 0;
    }
}

cmddatas_t *
remove_working_in_cmdfile(
    cmddatas_t *cmddatas,
    pid_t       pid)
{
    cmddatas_t *new_cmddatas;

    // take the lock and read
    new_cmddatas = read_cmdfile(cmddatas->lock->filename);

    // remove if same pid
    g_hash_table_foreach(new_cmddatas->cmdfile, &cmdfile_remove_working, &pid);

    // write
    write_cmdfile(new_cmddatas);
    close_cmdfile(cmddatas);
    return new_cmddatas;
}

typedef struct cmd_holding_s {
    char     *holding_file;
    gboolean  found;
} cmd_holding_t;

static void
cmdfile_holding_file(
    gpointer key G_GNUC_UNUSED,
    gpointer value,
    gpointer user_data)
{
    cmddata_t     *cmddata     = value;
    cmd_holding_t *cmd_holding = (cmd_holding_t *)user_data;

    if (cmddata->operation == CMD_FLUSH &&
	g_str_equal(cmddata->holding_file, cmd_holding->holding_file)) {
	cmd_holding-> found = TRUE;
    }
}

gboolean
holding_in_cmdfile(
    cmddatas_t *cmddatas,
    char       *holding_file)
{
    cmd_holding_t  cmd_holding = { holding_file, FALSE };
    cmddatas_t    *new_cmddatas;

    g_hash_table_foreach(cmddatas->cmdfile, &cmdfile_holding_file,
			 &cmd_holding);

    new_cmddatas = read_cmdfile(cmddatas->lock->filename);
    unlock_cmdfile(new_cmddatas);
    g_hash_table_foreach(new_cmddatas->cmdfile, &cmdfile_holding_file,
			 &cmd_holding);
    close_cmdfile(new_cmddatas);

    return cmd_holding.found;
}

typedef struct cmdfile_data_s {
    char *ids;
    char *holding_file;
} cmdfile_data_t;

static void
cmdfile_flush(
    gpointer key G_GNUC_UNUSED,
    gpointer value,
    gpointer user_data)
{
    int id = GPOINTER_TO_INT(key);
    cmddata_t *cmddata = value;
    cmdfile_data_t *data = user_data;

    if (cmddata->operation == CMD_FLUSH &&
        g_str_equal(data->holding_file, cmddata->holding_file)) {
        if (data->ids) {
            char *ids = g_strdup_printf("%s,%d;%s", data->ids, id, cmddata->dst_storage);
            g_free(data->ids);
            data->ids = ids;
        } else {
            data->ids = g_strdup_printf("%d;%s", id, cmddata->dst_storage);
        }
    }
    cmddata->working_pid = getpid();
}

char *
cmdfile_get_ids_for_holding(
    cmddatas_t *cmddatas,
    char       *holding_file)
{
    cmdfile_data_t data;
    data.ids = NULL;
    data.holding_file = holding_file;
    g_hash_table_foreach(cmddatas->cmdfile, &cmdfile_flush, &data);
    return g_strdup(data.ids);
}


