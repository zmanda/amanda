/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
/* $Id: taper.c 6512 2007-05-24 17:00:24Z ian $
 *
 * moves files from holding disk to tape, or from a socket to tape
 */

/* FIXME: This file needs to use gettext. */

#include <glib.h>
#include "physmem.h"

#include "changer.h"
#include "clock.h"
#include "conffile.h"
#include "device.h"
#include "logfile.h"
#include "server_util.h"
#include "stream.h"
#include "tapefile.h"
#include "taperscan.h"
#include "taper-source.h"
#include "timestamp.h"
#include "util.h"
#include "version.h"
#include "queueing.h"
#include "device-queueing.h"

/* FIXME: This should not be here. */
#define CONNECT_TIMEOUT (2*60)

/* Use this instead of global variables, so that we are reentrant. */
typedef struct {
    Device * device;
    char * driver_start_time;
    int    cur_tape;
    char * next_tape_label;
    char * next_tape_device;
    taper_scan_tracker_t * taper_scan_tracker;
    char * last_errmsg;
    off_t  total_bytes;
    int have_changer;
} taper_state_t;

typedef struct {
    char * handle;
    char * hostname;
    char * diskname;
    int level;
    char * timestamp;
    char * id_string;
    TaperSource * source;
    int current_part;
    GTimeVal total_time;
    guint64 total_bytes;
} dump_info_t;

static gboolean label_new_tape(taper_state_t * state, dump_info_t * dump_info);

static void init_taper_state(taper_state_t* state) {
    state->device = NULL;
    state->driver_start_time = NULL;
    state->taper_scan_tracker = taper_scan_tracker_new();
    state->last_errmsg = NULL;
    state->total_bytes = 0;
}

static void cleanup(taper_state_t * state) {
    amfree(state->driver_start_time);
    amfree(state->next_tape_label);
    amfree(state->next_tape_device);
    amfree(state->last_errmsg);
    taper_scan_tracker_free(state->taper_scan_tracker);
    if (state->device != NULL) {
        g_object_unref(state->device);
        state->device = NULL;
    }
}

static void free_dump_info(dump_info_t * info) {
    amfree(info->handle);
    amfree(info->hostname);
    amfree(info->diskname);
    amfree(info->timestamp);
    amfree(info->id_string);
    if (info->source != NULL) {
        g_object_unref(info->source);
        info->source = NULL;
    }
}

/* Validate that a command has the proper number of arguments, and
   print a meaningful error message if not. It returns only if the
   check is successful. */
static void validate_args(struct cmdargs * cmdargs,
                          char ** argnames) {
    int len = g_strv_length(argnames);

    if (len > cmdargs->argc) {
	error("error [taper %s: not enough args; first missing arg is %s]",
	      cmdstr[cmdargs->cmd], argnames[cmdargs->argc]);
    }

    if (len < cmdargs->argc) {
        error("error [taper %s: Too many args: Got %d, expected %d.]",
              cmdstr[cmdargs->cmd], cmdargs->argc, len);
    }
}

/* Open a socket to the dumper. Returns TRUE if everything is happy, FALSE
   otherwise. */
static gboolean open_read_socket(dump_info_t * info, char * split_diskbuffer,
                             guint64 splitsize, guint64 fallback_splitsize) {
    in_port_t port = 0;
    int socket;
    int fd;
    int result;
    struct addrinfo *res;

    if ((result = resolve_hostname("localhost", 0, &res, NULL) != 0)) {
        char *m;
        char *q;
	int save_errno = errno;
        char *qdiskname = quote_string(info->diskname);

        m = vstralloc("[localhost resolve failure: ",
                      strerror(save_errno),
                      "]",
                      NULL);
        q = quote_string(m);
        putresult(TAPE_ERROR, "%s %s\n", info->handle, q);
        log_add(L_FAIL, "%s %s %s %d %s",
                info->hostname, qdiskname, info->timestamp,
                info->level, q);
        amfree(qdiskname);
        amfree(m);
        amfree(q);
        return FALSE;
    }

    socket = stream_server(res->ai_family, &port, 0, STREAM_BUFSIZE, 0);
    freeaddrinfo(res);

    if (socket < 0) {
        char *m;
        char *q;
	int save_errno = errno;
        char *qdiskname = quote_string(info->diskname);

        m = vstralloc("[port create failure: ",
                      strerror(save_errno),
                      "]",
                      NULL);
        q = quote_string(m);
        putresult(TAPE_ERROR, "%s %s\n", info->handle, q);
        log_add(L_FAIL, "%s %s %s %d %s",
                info->hostname, qdiskname, info->timestamp,
                info->level, q);
        amfree(qdiskname);
        amfree(m);
        amfree(q);
        return FALSE;
    }

    putresult(PORT, "%d\n", port);

    fd = stream_accept(socket, CONNECT_TIMEOUT, 0, STREAM_BUFSIZE);

    if (fd < 0) {
        char *m, *q;
	int save_errno = errno;
        char *qdiskname = quote_string(info->diskname);
        m = vstralloc("[port connect failure: ",
                      strerror(save_errno),
                      "]",
                      NULL);
        q = quote_string(m);
        putresult(TAPE_ERROR, "%s %s\n", info->handle, q);
        log_add(L_FAIL, "%s %s %s %d %s",
                info->hostname, qdiskname, info->timestamp,
                info->level, q);
        amfree(qdiskname);
        aclose(socket);
        amfree(m);
        amfree(q);
        return FALSE;
    } else {
        aclose(socket);
    }

    info->source = taper_source_new(info->handle, PORT_WRITE, NULL, fd,
                                    split_diskbuffer, splitsize,
                                    fallback_splitsize);
    /* FIXME: This should be handled properly. */
    g_assert(info->source != NULL);
    return TRUE;
}

typedef struct {
    ConsumerFunctor next_consumer;
    gpointer next_consumer_data;
    guint64 bytes_written;
} CountingConsumerData;

/* A ConsumerFunctor. This consumer just passes its arguments on to a
   second consumer, but counts the number of bytes successfully
   written. */
static ssize_t counting_consumer(gpointer user_data, queue_buffer_t * buffer) {
    ssize_t result;
    CountingConsumerData * data = user_data;

    result = data->next_consumer(data->next_consumer_data, buffer);
    
    if (result > 0) {
        data->bytes_written += result;
    }

    return result;
}

static gboolean boolean_prolong(void * data) {
    if (data == NULL) {
        return TRUE; /* Do not interrupt. */
    } else {
        return *(gboolean*)data;
    }
}

static double get_kbps(double kb, double secs) {
    /* avoid division by zero */
    if (secs < 0.0001)
	return 0.0;
    return kb / secs;
}

/* A (simpler) wrapper around taper_scan(). */
static gboolean simple_taper_scan(taper_state_t * state,
                                  gboolean* prolong, char ** error_message) {
    char ** label = &(state->next_tape_label);
    char ** device = &(state->next_tape_device);
    char *timestamp = NULL;
    int result;
    result = taper_scan(NULL, label, &timestamp, device,
                        state->taper_scan_tracker,
                        CHAR_taperscan_output_callback,
                        error_message, boolean_prolong, prolong);
    if (prolong != NULL && !*prolong) {
        g_fprintf(stderr, _("Cancelled taper scan.\n"));
        return FALSE;
    } else if (result < 0) {
        g_fprintf(stderr, _("Failed taper scan: %s\n"), (*error_message)?(*error_message):_("(no error message)"));
        amfree(timestamp);
        return FALSE;
    } else {
        g_fprintf(stderr, _("taper: using label `%s' date `%s'\n"), *label,
                state->driver_start_time);
        if (result == 3) {
            log_add(L_INFO,
            _("Will write new label `%s' to new tape"),
                    *label);
        }

    }
    amfree(timestamp);
    return TRUE;
}

typedef struct {
    taper_state_t * state;
    gboolean prolong; /* scan stops when this is FALSE. */
    char *errmsg;
} tape_search_request_t;

/* A GThread that runs taper_scan. */
static gpointer tape_search_thread(gpointer data) {
    tape_search_request_t * request = data;

    if (request->state->next_tape_label != NULL &&
        request->state->next_tape_device != NULL) {
        return GINT_TO_POINTER(TRUE);
    } else {
        amfree(request->state->next_tape_label);
        amfree(request->state->next_tape_device);
    }

    return GINT_TO_POINTER
        (simple_taper_scan(request->state,
                           &(request->prolong),
			   &(request->errmsg)));
}

static void log_taper_scan_errmsg(char * errmsg) {
    char *c, *c1;
    if (errmsg == NULL)
        return;

    c = c1 = errmsg;
    while (*c != '\0') {
        if (*c == '\n') {
            *c = '\0';
            log_add(L_WARNING,"%s", c1);
            c1 = c+1;
        }
        c++;
    }
    if (strlen(c1) > 1 )
        log_add(L_WARNING,"%s", c1);
    amfree(errmsg);
}

/* If handle is NULL, then this function assumes that we are in startup mode.
 * In that case it will wait for a command from driver. If handle is not NULL,
 * this this function will ask for permission with REQUEST-NEW-TAPE. */
static gboolean find_new_tape(taper_state_t * state, dump_info_t * dump) {
    GThread * tape_search = NULL;
    tape_search_request_t search_request;
    gboolean use_threads;
    struct cmdargs *cmdargs;
    cmd_t cmd;

    if (state->device != NULL) {
        return TRUE;
    }

    /* We save the value here in case it changes while we're running. */
    use_threads = g_thread_supported();

    search_request.state = state;
    search_request.prolong = TRUE;
    search_request.errmsg = NULL;
    if (use_threads) {
        tape_search = g_thread_create(tape_search_thread,
                                      &search_request, TRUE, NULL);
    }
    
    putresult(REQUEST_NEW_TAPE, "%s\n", dump->handle);
    cmdargs = getcmd();
    cmd = cmdargs->cmd;

    switch (cmd) {
    default:
        g_fprintf(stderr, "taper: Got odd message from driver, expected NEW-TAPE or NO-NEW-TAPE.\n");
        /* FALLTHROUGH. */
    case NEW_TAPE: {
        gboolean search_result;
        if (use_threads) {
            search_result = GPOINTER_TO_INT(g_thread_join(tape_search));
        } else {
            search_result =
                GPOINTER_TO_INT(tape_search_thread(&search_request));
        }
        if (search_result) {
            /* We don't say NEW_TAPE until we actually write the label. */
	    amfree(search_request.errmsg);
	    free_cmdargs(cmdargs);
            return TRUE;
        } else {
            putresult(NO_NEW_TAPE, "%s\n", dump->handle);
            log_taper_scan_errmsg(search_request.errmsg);
	    log_add(L_ERROR, "no-tape [%s]", "No more writable valid tape found");
	    free_cmdargs(cmdargs);
            return FALSE;
        }
    }
    case NO_NEW_TAPE:
        search_request.prolong = FALSE;
        if (use_threads) {
            g_thread_join(tape_search);
        }
	log_add(L_ERROR, "no-tape [%s]", cmdargs->argv[1]);
	state->last_errmsg = stralloc(cmdargs->argv[1]);
	free_cmdargs(cmdargs);
        return FALSE;
    }
    /* NOTREACHED */
}

/* Returns TRUE if the old volume details are not the same as the new ones. */
static gboolean check_volume_changed(Device * device,
                                     char * old_label, char * old_timestamp) {
    /* If one is NULL and the other is not, something changed. */
    if ((old_label == NULL) != (device->volume_label == NULL))
        return TRUE;
    if ((old_timestamp == NULL) != (device->volume_time == NULL))
        return TRUE;
    /* If details were not NULL and is now different, we have a difference. */
    if (old_label != NULL && strcmp(old_label, device->volume_label) != 0)
        return TRUE;
    if (old_timestamp != NULL &&
        strcmp(old_timestamp, device->volume_time) != 0)
        return TRUE;

    /* If we got here, everything is cool. */
    return FALSE;
}

static void
update_tapelist(
    taper_state_t *state)
{
    char *tapelist_name = NULL;
    char *tapelist_name_old = NULL;
    tape_t *tp;
    char *comment = NULL;

    tapelist_name = config_dir_relative(getconf_str(CNF_TAPELIST));
    if (state->cur_tape == 0) {
	tapelist_name_old = stralloc2(tapelist_name, ".yesterday");
    } else {
	char cur_str[NUM_STR_SIZE];
	g_snprintf(cur_str, SIZEOF(cur_str), "%d", state->cur_tape - 1);
	tapelist_name_old = vstralloc(tapelist_name,
				      ".today.", cur_str, NULL);
    }

   if (read_tapelist(tapelist_name) != 0) {
        log_add(L_INFO, "pid-done %ld", (long)getpid());
        error("could not load tapelist \"%s\"", tapelist_name);
        /*NOTREACHED*/
    }

    /* make a copy of the tapelist file */
    if (write_tapelist(tapelist_name_old)) {
        log_add(L_INFO, "pid-done %ld", (long)getpid());
	error("could not write tapelist: %s", strerror(errno));
	/*NOTREACHED*/
    }
    amfree(tapelist_name_old);

    /* get a copy of the comment, before freeing the old record */
    tp = lookup_tapelabel(state->device->volume_label);
    if (tp && tp->comment)
	comment = stralloc(tp->comment);

    /* edit the tapelist and rewrite it */
    remove_tapelabel(state->device->volume_label);
    add_tapelabel(state->driver_start_time,
                  state->device->volume_label,
		  comment);
    if (write_tapelist(tapelist_name)) {
	error("could not write tapelist: %s", strerror(errno));
	/*NOTREACHED*/
    }
    amfree(tapelist_name);
    amfree(comment);
}

/* Find and label a new tape, if one is not already open. Returns TRUE
 * if a tape could be written. */
static gboolean find_and_label_new_tape(taper_state_t * state,
                                        dump_info_t * dump_info) {
    if (state->device != NULL) {
        return TRUE;
    }
    state->total_bytes = 0;
 
    if (!find_new_tape(state, dump_info)) {
        return FALSE;
    }

    return label_new_tape(state, dump_info);
}

static gboolean label_new_tape(taper_state_t * state, dump_info_t * dump_info) {
    char *old_volume_name = NULL;
    char *old_volume_time = NULL;
    tape_search_request_t request;
    gboolean search_result;
    DeviceStatusFlags status;

    /* If we got here, it means that we have found a tape to label and
     * have gotten permission from the driver to write it. But we
     * still can say NO-NEW-TAPE if a problem shows up, and must still
     * say NEW-TAPE if one doesn't. */

    amfree(state->last_errmsg);
    state->device = device_open(state->next_tape_device);
    g_assert(state->device != NULL);
    amfree(state->next_tape_device);

    if (state->device->status != DEVICE_STATUS_SUCCESS)
	goto skip_volume;

    if (!device_configure(state->device, TRUE))
	goto skip_volume;

    /* if we have an error, and are sure it isn't just an unlabeled volume,
     * then skip this volume */
    status = device_read_label(state->device);
    if ((status & ~DEVICE_STATUS_VOLUME_UNLABELED) &&
	!(status & DEVICE_STATUS_VOLUME_UNLABELED))
	goto skip_volume;

    old_volume_name = g_strdup(state->device->volume_label);
    old_volume_time = g_strdup(state->device->volume_time);

    if (!device_start(state->device, ACCESS_WRITE, state->next_tape_label,
                      state->driver_start_time)) {
        gboolean tape_used;

        /* Something broke, see if we can tell if the volume was erased or
         * not. */
        g_fprintf(stderr, "taper: Error writing label %s to device %s: %s.\n",
                state->next_tape_label, state->device->device_name,
		device_error_or_status(state->device));

        if (!device_finish(state->device))
	    goto request_new_volume;

	/* This time, if we can't read the label, assume we've overwritten
	 * the volume or otherwise corrupted it */
	status = device_read_label(state->device);
	if ((status & ~DEVICE_STATUS_VOLUME_UNLABELED) &&
	    !(status & DEVICE_STATUS_VOLUME_UNLABELED))
	    goto request_new_volume;

        tape_used = check_volume_changed(state->device, old_volume_name, 
                                         old_volume_time);

        if (tape_used)
	    goto request_new_volume;
        else
	    goto skip_volume;
    }

    amfree(old_volume_name);
    amfree(old_volume_time);
    amfree(state->next_tape_label);

    update_tapelist(state);
    state->cur_tape++;

    if (state->have_changer &&
	changer_label("UNKNOWN", state->device->volume_label) != 0) {
	error(_("couldn't update barcode database"));
	/*NOTREACHED*/
    }

    log_add(L_START, "datestamp %s label %s tape %d",
            state->driver_start_time, state->device->volume_label,
            state->cur_tape);
    putresult(NEW_TAPE, "%s %s\n", dump_info->handle,
	      state->device->volume_label);

    return TRUE;

request_new_volume:
    /* Tell the driver we overwrote this volume, even if it was empty, and request
     * a new volume. */
    if (state->device)
	state->last_errmsg = newstralloc(state->last_errmsg, device_error_or_status(state->device));
    else
	state->last_errmsg = newstralloc(state->last_errmsg, "(unknown)");

    putresult(NEW_TAPE, "%s %s\n", dump_info->handle,
	      state->next_tape_label);
    if (old_volume_name) {
	log_add(L_WARNING, "Problem writing label '%s' to volume %s "
		"(volume may be erased): %s\n",
		state->next_tape_label, old_volume_name,
		state->last_errmsg);
    } else {
	log_add(L_WARNING, "Problem writing label '%s' to new volume "
		"(volume may be erased): %s\n", state->next_tape_label,
		state->last_errmsg);
    }

    if (state->device) {
        g_object_unref(state->device);
        state->device = NULL;
    }

    amfree(state->next_tape_label);
    amfree(old_volume_name);
    amfree(old_volume_time);

    return find_and_label_new_tape(state, dump_info);

skip_volume:
    /* grab a new volume without talking to the driver again -- we do this if we're
     * confident we didn't overwrite the last tape we got. */
    if (state->device)
	state->last_errmsg = newstralloc(state->last_errmsg, device_error_or_status(state->device));
    else
	state->last_errmsg = newstralloc(state->last_errmsg, "(unknown)");

    if (old_volume_name) {
	log_add(L_WARNING, "Problem writing label '%s' to volume '%s' "
		"(old volume data intact): %s\n",
		state->next_tape_label, old_volume_name, state->last_errmsg);
    } else {
	log_add(L_WARNING, "Problem writing label '%s' to new volume "
		"(old volume data intact): %s\n", state->next_tape_label,
		state->last_errmsg);
    }

    if (state->device) {
        g_object_unref(state->device);
        state->device = NULL;
    }

    amfree(state->next_tape_label);
    amfree(old_volume_name);
    amfree(old_volume_time);

    request.state = state;
    request.prolong = TRUE;
    request.errmsg = NULL;
    search_result = GPOINTER_TO_INT(tape_search_thread(&request));
    if (search_result) {
	amfree(request.errmsg);
	return label_new_tape(state, dump_info);
    } else {
	/* Problem finding a new tape! */
	log_taper_scan_errmsg(request.errmsg);
	putresult(NO_NEW_TAPE, "%s\n", dump_info->handle);
	return FALSE;
    }
}

/* Find out if the dump is PARTIAL or not, and set the proper driver
   and logfile tags for the dump. */
static void find_completion_tags(dump_info_t * dump_info, /* IN */
                                 cmd_t * result_cmd,      /* OUT */
                                 logtype_t * result_log   /* OUT */) {
    /* result_cmd is always DONE because the taper wrote all the input to
     * the output. driver need to know if the taper completed its job.
     * result_log is set to L_PARTIAL if the image is partial, the log
     * must tell if the image is partial or complete.
     */
       
    if (taper_source_is_partial(dump_info->source)) {
        *result_cmd = DONE;
        *result_log = L_PARTIAL;
    } else {
        *result_cmd = DONE;
        *result_log = L_DONE;
    }
}

/* Put an L_PARTIAL message to the logfile. */
static void put_partial_log(dump_info_t * dump_info, double dump_time,
                            guint64 dump_kbytes, char *errstr) {
    char * qdiskname = quote_string(dump_info->diskname);

    log_add(L_PARTIAL, "%s %s %s %d %d [sec %f kb %ju kps %f] %s",
            dump_info->hostname, qdiskname, dump_info->timestamp,
            dump_info->current_part, dump_info->level, dump_time,
            (uintmax_t)dump_kbytes, get_kbps(dump_kbytes, dump_time),
	    errstr);
    amfree(qdiskname);
}

/* Figure out what to do after a part attempt. Returns TRUE if another
   attempt should proceed for this dump; FALSE if we are done. */
static gboolean finish_part_attempt(taper_state_t * taper_state,
                                    dump_info_t * dump_info,
                                    queue_result_flags queue_result,
                                    GTimeVal run_time, guint64 run_bytes) {
    double part_time = g_timeval_to_double(run_time);
    guint64 part_kbytes = run_bytes / 1024;
    double part_kbps = get_kbps((double)run_bytes / 1024.0, part_time);
        
    char * qdiskname = quote_string(dump_info->diskname);

    if (queue_result == QUEUE_SUCCESS) {
        dump_info->total_time = timesadd(run_time, dump_info->total_time);
        dump_info->total_bytes += run_bytes;

        log_add(L_PART, "%s %d %s %s %s %d/%d %d [sec %f kb %ju kps %f]",
                taper_state->device->volume_label,
                taper_state->device->file, dump_info->hostname, qdiskname,
                dump_info->timestamp, dump_info->current_part,
                taper_source_predict_parts(dump_info->source),
                dump_info->level, part_time, (uintmax_t)part_kbytes, part_kbps);
        putresult(PARTDONE, "%s %s %d %ju \"[sec %f kb %ju kps %f]\"\n",
                  dump_info->handle, taper_state->device->volume_label,
                  taper_state->device->file, (uintmax_t)part_kbytes, part_time,
		  (uintmax_t)part_kbytes, part_kbps);
	taper_state->total_bytes += run_bytes;
        
        if (taper_source_get_end_of_data(dump_info->source)) {
            cmd_t result_cmd;
            logtype_t result_log;
            double dump_time = g_timeval_to_double(dump_info->total_time);
            guint64 dump_kbytes = dump_info->total_bytes / 1024;
            double dump_kbps = get_kbps((double)dump_info->total_bytes / 1024.0, dump_time);

            find_completion_tags(dump_info, &result_cmd, &result_log);

            g_object_unref(dump_info->source);
            dump_info->source = NULL;
        
            log_add(result_log, "%s %s %s %d %d [sec %f kb %ju kps %f]",
                    dump_info->hostname, qdiskname, dump_info->timestamp,
                    dump_info->current_part, dump_info->level, dump_time,
		    (uintmax_t)dump_kbytes, dump_kbps);
            putresult(result_cmd, "%s INPUT-GOOD TAPE-GOOD "
                      "\"[sec %f kb %ju kps %f]\" \"\" \"\"\n",
                      dump_info->handle, dump_time, (uintmax_t)dump_kbytes,
                      dump_kbps);
            
            amfree(qdiskname);
            return FALSE;
        } else if (taper_source_get_end_of_part(dump_info->source)) {
            taper_source_start_new_part(dump_info->source);
            dump_info->current_part ++;
            amfree(qdiskname);
            return TRUE;
        }
        /* If we didn't read EOF or EOP, then an error
           occured. But we read QUEUE_SUCCESS, so something is
           b0rked. */
        g_assert_not_reached();
    } else {
        char * volume_label = strdup(taper_state->device->volume_label);
        int file_number = taper_state->device->file;
        double dump_time, dump_kbps;
        guint64 dump_kbytes;
	char *producer_errstr = quote_string(
				   taper_source_get_errmsg(dump_info->source));
	char *consumer_errstr = quote_string(
				   device_error(taper_state->device));

        log_add(L_PARTPARTIAL,
                "%s %d %s %s %s %d/%d %d [sec %f kb %ju kps %f] %s",
                volume_label, file_number, dump_info->hostname, qdiskname,
                dump_info->timestamp, dump_info->current_part,
                taper_source_predict_parts(dump_info->source),
                dump_info->level, part_time, (uintmax_t)part_kbytes, part_kbps,
		consumer_errstr);
	log_add(L_INFO, "tape %s kb %lld fm %d [OK]\n",
		volume_label,
		(long long)((taper_state->total_bytes+(off_t)1023) / (off_t)1024),
		taper_state->device->file);

        /* A problem occured. */
        if (queue_result & QUEUE_CONSUMER_ERROR) {
	    /* Make a note if this was EOM (we treat EOM the same as any other error,
	     * so it's just for debugging purposes */
	    if (taper_state->device->is_eof)
		g_debug("device %s ran out of space", taper_state->device->device_name);

            /* Close the device. */
            device_finish(taper_state->device);
            g_object_unref(taper_state->device);
            taper_state->device = NULL;
        }
        
        amfree(volume_label);
        
        if ((queue_result & QUEUE_CONSUMER_ERROR) &&
            (!(queue_result & QUEUE_PRODUCER_ERROR)) &&
            taper_source_seek_to_part_start(dump_info->source)) {
            /* It is recoverable. */
            log_add(L_INFO, "Will request retry of failed split part.");
            if (find_and_label_new_tape(taper_state, dump_info)) {
                /* dump_info->current_part is unchanged. */
                amfree(qdiskname);
                return TRUE;
            }
        }

        dump_time = g_timeval_to_double(dump_info->total_time);
        dump_kbytes = dump_info->total_bytes / 1024;
        dump_kbps = get_kbps((double)dump_info->total_bytes / 1024.0, dump_time);
        
        putresult(PARTIAL,
                  "%s INPUT-%s TAPE-%s "
                  "\"[sec %f kb %ju kps %f]\" %s %s\n",
                  dump_info->handle,
                  (queue_result & QUEUE_PRODUCER_ERROR) ? "ERROR" : "GOOD",
                  (queue_result & QUEUE_CONSUMER_ERROR) ? "ERROR" : "GOOD",
                  dump_time, (uintmax_t)dump_kbytes, dump_kbps,
		  producer_errstr, consumer_errstr);
	if (queue_result & QUEUE_CONSUMER_ERROR) {
            put_partial_log(dump_info, dump_time, dump_kbytes,
			    consumer_errstr);
	} else {
            put_partial_log(dump_info, dump_time, dump_kbytes,
			    producer_errstr);
	}
	amfree(producer_errstr);
	amfree(consumer_errstr);
    }

    amfree(qdiskname);
    return FALSE;
}

/* Generate the actual header structure to write to tape. This means dropping
 * bits related to the holding disk, and adding bits for split dumps. */
static dumpfile_t * munge_headers(dump_info_t * dump_info) {
    dumpfile_t * rval;
    int expected_splits;
    
    rval = taper_source_get_first_header(dump_info->source);

    if (rval == NULL) {
        return NULL;
    }

    rval->cont_filename[0] = '\0';

    expected_splits = taper_source_predict_parts(dump_info->source);

    if (expected_splits != 1) {
        rval->type = F_SPLIT_DUMPFILE;
        rval->partnum = dump_info->current_part;
        rval->totalparts = expected_splits;
    }

    return rval;
}

/* We call this when we can't find a tape to write data to. This could
   happen with the first (or only) part of a file, but it could also
   happen with an intermediate part of a split dump. dump_bytes
   is 0 if this is the first part of a dump. */
static void bail_no_volume(
    dump_info_t *dump_info,
    char *errmsg)
{
    char *errstr;
    if (errmsg)
	errstr = quote_string(errmsg);
    else
	errstr = quote_string("no new tape");
    if (dump_info->total_bytes > 0) {
        /* Second or later part of a split dump, so PARTIAL message. */
        double dump_time = g_timeval_to_double(dump_info->total_time);
        guint64 dump_kbytes = dump_info->total_bytes / 1024;
        double dump_kbps = get_kbps(dump_kbytes, dump_time);
        putresult(PARTIAL,
                  "%s INPUT-GOOD TAPE-ERROR "
                  "\"[sec %f kb %ju kps %f]\" \"\" %s\n",
                  dump_info->handle, 
                  dump_time, (uintmax_t)dump_kbytes, dump_kbps, errstr);
        put_partial_log(dump_info, dump_time, dump_kbytes, errstr);
    } else {
        char * qdiskname = quote_string(dump_info->diskname);
        putresult(FAILED,
                  "%s INPUT-GOOD TAPE-ERROR \"\" %s\n",
                  dump_info->handle, errstr);
        log_add(L_FAIL, "%s %s %s %d %s",
                dump_info->hostname, qdiskname, dump_info->timestamp,
                dump_info->level, errstr);
	amfree(qdiskname);
    }
    amfree(errstr);
}

/* Link up the TaperSource with the Device, including retries etc. */
static void run_device_output(taper_state_t * taper_state,
                              dump_info_t * dump_info) {
    GValue val;
    guint file_number;
    dump_info->current_part = 1;
    dump_info->total_time.tv_sec = 0;
    dump_info->total_time.tv_usec = 0;
    dump_info->total_bytes = 0;

    for (;;) {
        GTimeVal start_time, end_time, run_time;
        StreamingRequirement streaming_mode;
        queue_result_flags queue_result;
        CountingConsumerData consumer_data;
        dumpfile_t *this_header;
        size_t max_memory;
        
        this_header = munge_headers(dump_info);
        if (this_header == NULL) {
            char * qdiskname = quote_string(dump_info->diskname);
	    char * errstr = taper_source_get_errmsg(dump_info->source);
	    if (!errstr)
		errstr = "Failed reading dump header.";
	    errstr = quote_string(errstr);
            putresult(FAILED,
             "%s INPUT-ERROR TAPE-GOOD %s \"\"\n",
                      dump_info->handle, errstr);
            log_add(L_FAIL, "%s %s %s %d %s",
                    dump_info->hostname, qdiskname, dump_info->timestamp,
                    dump_info->level, errstr);
            amfree(qdiskname);
	    amfree(errstr);
            return;
        }            

        if (!find_and_label_new_tape(taper_state, dump_info)) {
            bail_no_volume(dump_info, taper_state->last_errmsg);
	    dumpfile_free(this_header);
            return;
        }

	if (this_header->partnum == 1 || debug_taper)
	    dump_dumpfile_t(this_header);

	while (!device_start_file(taper_state->device, this_header)) {
            /* Close the device. */
            device_finish(taper_state->device);
            g_object_unref(taper_state->device);
            taper_state->device = NULL;

            if (!find_and_label_new_tape(taper_state, dump_info)) {
		bail_no_volume(dump_info, taper_state->last_errmsg);
		dumpfile_free(this_header);
		return;
            }
        }
	dumpfile_free(this_header);

        bzero(&val, sizeof(val));
        if (!device_property_get(taper_state->device, PROPERTY_STREAMING, &val)
            || !G_VALUE_HOLDS(&val, STREAMING_REQUIREMENT_TYPE)) {
            g_fprintf(stderr, "taper: Couldn't get streaming type!\n");
            streaming_mode = STREAMING_REQUIREMENT_REQUIRED;
        } else {
            streaming_mode = g_value_get_enum(&val);
        }
    
        file_number = taper_state->device->file;

        consumer_data.next_consumer = device_write_consumer;
        consumer_data.next_consumer_data = taper_state->device;
        consumer_data.bytes_written = 0;

        g_get_current_time(&start_time);

        if (getconf_seen(CNF_DEVICE_OUTPUT_BUFFER_SIZE)) {
            max_memory = getconf_size(CNF_DEVICE_OUTPUT_BUFFER_SIZE);
            if (getconf_seen(CNF_TAPEBUFS)) {
                g_fprintf(stderr,
                        "Configuration directives 'device_output_buffer_size' "
                        "and \n"
                        "'tapebufs' are incompatible; using former.\n");
            }
        } else if (getconf_seen(CNF_TAPEBUFS)) {
            max_memory = getconf_int(CNF_TAPEBUFS) *
                taper_state->device->block_size;
        } else {
            /* Use default. */
            max_memory = getconf_size(CNF_DEVICE_OUTPUT_BUFFER_SIZE);
        }

        queue_result = do_consumer_producer_queue_full
            (taper_source_producer,
             dump_info->source,
             counting_consumer,
             &consumer_data,
             taper_state->device->block_size, max_memory,
             streaming_mode);

        g_get_current_time(&end_time);
        run_time = timesub(end_time, start_time);

        /* The device_write_consumer leaves the file open, so close it now. */
        if (!device_finish_file(taper_state->device)) {
            queue_result = queue_result | QUEUE_CONSUMER_ERROR;
        }

        if (!finish_part_attempt(taper_state, dump_info, queue_result,
                                 run_time, consumer_data.bytes_written)) {
            break;
        }
    }
}

/* Handle a PORT_WRITE command. */
static void process_port_write(taper_state_t * state,
                               struct cmdargs * cmdargs) {
    dump_info_t dump_state;
    guint64 splitsize;
    guint64 fallback_splitsize;
    char * split_diskbuffer;
    char * argnames[] = {"command",               /* 0 */
			 "handle",                /* 1 */
                         "hostname",              /* 2 */
                         "diskname",              /* 3 */
                         "level",                 /* 4 */
                         "datestamp",             /* 5 */
                         "splitsize",             /* 6 */
                         "split_diskbuffer",      /* 7 */
                         "fallback_splitsize",    /* 8 */
                          NULL };

    validate_args(cmdargs, argnames);

    dump_state.handle = g_strdup(cmdargs->argv[1]);
    dump_state.hostname = g_strdup(cmdargs->argv[2]);
    dump_state.diskname = g_strdup(cmdargs->argv[3]);
    
    errno = 0;
    dump_state.level = strtol(cmdargs->argv[4], NULL, 10);
    if (errno != 0) {
        error("error [taper PORT-WRITE: Invalid dump level %s]",
              cmdargs->argv[4]);
        g_assert_not_reached();
    }
    
    dump_state.timestamp = strdup(cmdargs->argv[5]);

    errno = 0;
    splitsize = g_ascii_strtoull(cmdargs->argv[6], NULL, 10);
    if (errno != 0) {
        error("error [taper PORT-WRITE: Invalid splitsize %s]",
              cmdargs->argv[6]);
        g_assert_not_reached();
    }
    
    if (strcmp(cmdargs->argv[7], "NULL") == 0) {
        split_diskbuffer = NULL;
    } else {
        split_diskbuffer = g_strdup(cmdargs->argv[7]);
    }
    
    errno = 0;
    fallback_splitsize = g_ascii_strtoull(cmdargs->argv[8], NULL, 10);
    if (errno != 0) {
        error("error [taper PORT-WRITE: Invalid fallback_splitsize %s]",
              cmdargs->argv[8]);
        g_assert_not_reached();
    }

    dump_state.id_string = g_strdup_printf("%s:%s.%d", dump_state.hostname,
                                           dump_state.diskname,
					   dump_state.level);
    
    if (!open_read_socket(&dump_state, split_diskbuffer, splitsize,
                          fallback_splitsize)) {
        free(split_diskbuffer);
        return;
    }
    free(split_diskbuffer);

    run_device_output(state, &dump_state);

    free_dump_info(&dump_state);
}

/* Handle a FILE_WRITE command. */
static void process_file_write(taper_state_t * state,
                               struct cmdargs * cmdargs) {
    dump_info_t dump_state;
    char * holding_disk_file;
    guint64 splitsize;
    char * argnames[] = {"command",               /* 0 */
			 "handle",                /* 1 */
                         "filename",              /* 2 */
                         "hostname",              /* 3 */
                         "diskname",              /* 4 */
                         "level",                 /* 5 */
                         "datestamp",             /* 6 */
                         "splitsize",             /* 7 */
                          NULL };

    validate_args(cmdargs, argnames);

    dump_state.handle = g_strdup(cmdargs->argv[1]);
    holding_disk_file = g_strdup(cmdargs->argv[2]);
    dump_state.hostname = g_strdup(cmdargs->argv[3]);
    dump_state.diskname = g_strdup(cmdargs->argv[4]);
    
    errno = 0;
    dump_state.level = strtol(cmdargs->argv[5], NULL, 10);
    if (errno != 0) {
        error("error [taper FILE-WRITE: Invalid dump level %s]",
              cmdargs->argv[5]);
        g_assert_not_reached();
    }
    
    dump_state.timestamp = strdup(cmdargs->argv[6]);

    errno = 0;
    splitsize = g_ascii_strtoull(cmdargs->argv[7], NULL, 10);
    if (errno != 0) {
        error("error [taper FILE-WRITE: Invalid splitsize %s]",
              cmdargs->argv[7]);
        g_assert_not_reached();
    }

    dump_state.id_string = g_strdup_printf("%s:%s.%d", dump_state.hostname,
                                           dump_state.diskname,
					   dump_state.level);
    
    dump_state.source = taper_source_new(dump_state.handle, FILE_WRITE,
                                         holding_disk_file, -1,
                                         NULL, splitsize, -1);
    /* FIXME: This should be handled properly. */
    g_assert(dump_state.source != NULL);

    run_device_output(state, &dump_state);

    free_dump_info(&dump_state);
    amfree(holding_disk_file);
}

/* Send QUITTING message to driver and associated logging. Always
   returns false. */
static gboolean send_quitting(taper_state_t * state) {
    putresult(QUITTING, "\n");
    g_fprintf(stderr,"taper: DONE\n");
    cleanup(state);
    return FALSE;
}

/* This function recieves the START_TAPER command from driver, and
   returns the attached timestamp. */
static gboolean find_first_tape(taper_state_t * state) {
    struct cmdargs *cmdargs;
    tape_search_request_t search_request;
    GThread * tape_search = NULL;
    gboolean use_threads;

    /* We save the value here in case it changes while we're running. */
    use_threads = g_thread_supported();

    search_request.state = state;
    search_request.prolong = TRUE;
    search_request.errmsg = NULL;
    
    if (use_threads) {
        tape_search = g_thread_create(tape_search_thread,
                                      &search_request, TRUE, NULL);
    }

    cmdargs = getcmd();

    switch (cmdargs->cmd) {
    case START_TAPER: {
        gboolean search_result;
        state->driver_start_time = strdup(cmdargs->argv[1]);
        if (use_threads) {
            search_result = GPOINTER_TO_INT(g_thread_join(tape_search));
        } else {
            search_result =
                GPOINTER_TO_INT(tape_search_thread(&search_request));
        }
        if (search_result) {
            putresult(TAPER_OK, "\n");
        } else {
	    char *msg = quote_string(_("Could not find a tape to use"));
            putresult(TAPE_ERROR, "99-99999 %s\n", msg);
	    log_add(L_ERROR, "no-tape [%s]", "Could not find a tape to use");
	    if (search_request.errmsg != NULL) {
		char *c, *c1;
		c = c1 = search_request.errmsg;
		while (*c != '\0') {
		    if (*c == '\n') {
			*c = '\0';
			log_add(L_WARNING,"%s", c1);
			c1 = c+1;
		    }
		    c++;
		}
		if (strlen(c1) > 1 )
		    log_add(L_WARNING,"%s", c1);
	    }
        }
	amfree(search_request.errmsg);
	free_cmdargs(cmdargs);
        return TRUE;
    }
    case QUIT:
        search_request.prolong = FALSE;
        if (use_threads) {
            g_thread_join(tape_search);
        }
	free_cmdargs(cmdargs);
        return send_quitting(state);
    default:
        error("error [file_reader_side cmd %d argc %d]", cmdargs->cmd, cmdargs->argc);
    }

    g_assert_not_reached();
}

/* In running mode (not startup mode), get a command from driver and
   deal with it. */
static gboolean process_driver_command(taper_state_t * state) {
    struct cmdargs *cmdargs;
    char * q;

    /* This will return QUIT if driver has died. */
    cmdargs = getcmd();
    switch (cmdargs->cmd) {
    case PORT_WRITE:
        /*
         * PORT-WRITE
         *   handle
         *   hostname
         *   features
         *   diskname
         *   level
         *   datestamp
         *   splitsize
         *   split_diskbuffer
         */
        process_port_write(state, cmdargs);
        break;
        
    case FILE_WRITE:
        /*
         * FILE-WRITE
         *   handle
         *   filename
         *   hostname
         *   features
         *   diskname
         *   level
         *   datestamp
         *   splitsize
         */
        process_file_write(state, cmdargs);
        break;
        
    case QUIT:
	free_cmdargs(cmdargs);
	if (state->device && state->device->volume_label) {
	    log_add(L_INFO, "tape %s kb %lld fm %d [OK]\n",
		    state->device->volume_label,
		    (long long)((state->total_bytes+(off_t)1023) / (off_t)1024),
		    state->device->file);
	}
        return send_quitting(state);
    default:
        if (cmdargs->argc >= 1) {
            q = quote_string(cmdargs->argv[0]);
        } else {
            q = stralloc("(no input?)");
        }
        putresult(BAD_COMMAND, "%s\n", q);
        amfree(q);
        break;
    }
    free_cmdargs(cmdargs);

    return TRUE;
}

int main(int argc, char ** argv) {
    char * tapelist_name;
    taper_state_t state;
    config_overwrites_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda");
    
    safe_fd(-1, 0);
    set_pname("taper");

    dbopen("server");

    device_api_init();
    init_taper_state(&state);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    g_fprintf(stderr, _("%s: pid %ld executable %s version %s\n"),
	    get_pname(), (long) getpid(), argv[0], version());
    dbprintf(_("%s: pid %ld executable %s version %s\n"),
              get_pname(), (long) getpid(), argv[0], version());

    /* Process options */

    cfg_ovr = extract_commandline_config_overwrites(&argc, &argv);

    if(argc > 2) {
        error("Too many arguments!\n");
        g_assert_not_reached();
    }
    if (argc > 1)
	cfg_opt = argv[1];
    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);
    apply_config_overwrites(cfg_ovr);

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
    }

    safe_cd();

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_trace_log);

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());

    tapelist_name = config_dir_relative(getconf_str(CNF_TAPELIST));

    if (read_tapelist(tapelist_name) != 0) {
	log_add(L_INFO, "pid-done %ld", (long)getpid());
        error("could not load tapelist \"%s\"", tapelist_name);
        g_assert_not_reached();
    }
    amfree(tapelist_name);

    state.have_changer = changer_init();
    if (state.have_changer < 0) {
	log_add(L_INFO, "pid-done %ld", (long)getpid());
        error("changer initialization failed: %s", strerror(errno));
        g_assert_not_reached();
    }

    state.next_tape_label = NULL;
    state.next_tape_device = NULL;
    state.cur_tape = 0;
    
    if (!find_first_tape(&state)) {
	log_add(L_INFO, "pid-done %ld", (long)getpid());
        return EXIT_SUCCESS;
    }

    while (process_driver_command(&state));
    log_add(L_INFO, "pid-done %ld", (long)getpid());
    return EXIT_SUCCESS;
}
