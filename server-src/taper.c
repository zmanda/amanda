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
#include "token.h"
#include "version.h"

/* FIXME: This should not be here. */
#define CONNECT_TIMEOUT (2*60)

/* Use this instead of global variables, so that we are reentrant. */
typedef struct {
    Device * device;
    char * driver_start_time;
    int    cur_tape;
    char * next_tape_label;
    char * next_tape_device;
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

static void init_taper_state(taper_state_t* state) {
    state->device = NULL;
    state->driver_start_time = NULL;
}

static void cleanup(taper_state_t * state) {
    amfree(state->driver_start_time);
    amfree(state->next_tape_label);
    amfree(state->next_tape_device);
    if (state->device != NULL) {
        g_object_unref(state->device);
        state->device = NULL;
    }
}

static void free_dump_info(dump_info_t * info) {
    amfree(info->handle);
    amfree(info->hostname);
    amfree(info->diskname);
    amfree(info->id_string);
    if (info->source != NULL) {
        g_object_unref(info->source);
        info->source = NULL;
    }
}

/* Validate that a command has the proper number of arguments, and
   print a meaningful error message if not. It returns only if the
   check is successful. */
static void validate_args(cmd_t cmd, struct cmdargs * args,
                          char ** argnames) {
    int i;
    
    for (i = 0; argnames[i] != NULL; i ++) {
        if (i > args->argc) {
            error("error [taper %s: not enough args: %s]",
                  cmdstr[cmd], argnames[i]);
        }
    }
    if (i < args->argc) {
        error("error [taper %s: Too many args: Got %d, expected %d.]",
              cmdstr[cmd], args->argc, i);
    }
}

/* Open a socket to the dumper. Returns TRUE if everything is happy, FALSE
   otherwise. */
static gboolean open_read_socket(dump_info_t * info, char * split_diskbuffer,
                             guint64 splitsize, guint64 fallback_splitsize) {
    in_port_t port = 0;
    int socket;
    int fd;

    socket = stream_server(&port, 0, STREAM_BUFSIZE, 0);

    if (socket < 0) {
        char *m;
        char *q;
	int save_errno = errno;
        char *qdiskname = quote_string(info->diskname);

        m = vstralloc("[port create failure: ",
                      strerror(save_errno),
                      "]",
                      NULL);
        q = squote(m);
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
        q = squote(m);
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
static int counting_consumer(gpointer user_data, queue_buffer_t * buffer) {
    int result;
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

/* A (simpler) wrapper around taper_scan(). */
static gboolean simple_taper_scan(taper_state_t * state,
                                  gboolean* prolong, char ** error_message) {
    char ** label = &(state->next_tape_label);
    char ** device = &(state->next_tape_device);
    char *timestamp = NULL;
    int result;
    result = taper_scan(NULL, label, &timestamp, device,
                        CHAR_taperscan_output_callback, 
                        error_message, boolean_prolong, prolong);
    if (result < 0) {
        g_fprintf(stderr, "Failed taper scan:\n%s\n", *error_message);
        amfree(timestamp);
        return FALSE;
    } else {
        g_fprintf(stderr, _("taper: using label `%s' date `%s'\n"), *label,
                state->driver_start_time);
        if (result == 3) {
            log_add(L_INFO,
            _("Will write new label `%s' to new (previously non-amanda) tape"),
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

/* If handle is NULL, then this function assumes that we are in startup mode.
 * In that case it will wait for a command from driver. If handle is not NULL,
 * this this function will ask for permission with REQUEST-NEW-TAPE. */
static gboolean find_new_tape(taper_state_t * state, dump_info_t * dump) {
    GThread * tape_search = NULL;
    tape_search_request_t search_request;
    gboolean use_threads;
    cmd_t cmd;
    struct cmdargs args;

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
    cmd = getcmd(&args);
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
            return TRUE;
        } else {
            putresult(NO_NEW_TAPE, "%s\n", dump->handle);
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
		amfree(search_request.errmsg);
	    }
            return FALSE;
        }
    }
    case NO_NEW_TAPE:
        search_request.prolong = FALSE;
        if (use_threads) {
            g_thread_join(tape_search);
        }
        return FALSE;
    }
}

/* Find and label a new tape, if one is not already open. Returns TRUE
 * if a tape could be written. */
static gboolean find_and_label_new_tape(taper_state_t * state,
                                        dump_info_t * dump_info) {
    char *tapelist_name;
    char *tapelist_name_old;

    if (state->device != NULL) {
        return TRUE;
    }
    
    if (!find_new_tape(state, dump_info)) {
        return FALSE;
    }

    /* If we got here, it means that we have found a tape to label and
     * have gotten permission from the driver to write it. But we
     * still can say NO-NEW-TAPE if a problem shows up, and must still
     * say NEW-TAPE if one doesn't. */

    state->device = device_open(state->next_tape_device);
    amfree(state->next_tape_device);
    if (state->device == NULL) {
        amfree(state->next_tape_label);
        return FALSE;
    }
    
    device_set_startup_properties_from_config(state->device);
    
    if (!device_start(state->device, ACCESS_WRITE, state->next_tape_label,
                      state->driver_start_time)) {
        g_fprintf(stderr, "taper: Error writing label %s to device %s.\n",
                state->next_tape_label, state->device->device_name);
        g_object_unref(state->device);
        state->device = NULL;
        amfree(state->next_tape_label);
        return FALSE;
    }
    state->next_tape_label = NULL; /* Taken by device_start. */

    tapelist_name = getconf_str(CNF_TAPELIST);
    if (tapelist_name[0] == '/') {
        tapelist_name = g_strdup(tapelist_name);
    } else {
        tapelist_name = stralloc2(config_dir, tapelist_name);
    }

    if (state->cur_tape == 0) {
	tapelist_name_old = stralloc2(tapelist_name, ".yesterday");
    } else {
	char cur_str[NUM_STR_SIZE];
	g_snprintf(cur_str, SIZEOF(cur_str), "%d", state->cur_tape - 1);
	tapelist_name_old = vstralloc(tapelist_name,
				      ".today.", cur_str, NULL);
    }

    if (write_tapelist(tapelist_name_old)) {
	error("could not write tapelist: %s", strerror(errno));
	/*NOTREACHED*/
    }
    amfree(tapelist_name_old);

    remove_tapelabel(state->device->volume_label);
    add_tapelabel(strdup(state->driver_start_time),
                  state->device->volume_label);
    if (write_tapelist(tapelist_name)) {
	error("could not write tapelist: %s", strerror(errno));
	/*NOTREACHED*/
    }
    amfree(tapelist_name);
    state->cur_tape++;

    log_add(L_START, "datestamp %s label %s tape %d",
            state->driver_start_time, state->device->volume_label,
            state->cur_tape);
    putresult(NEW_TAPE, "%s\n", state->device->volume_label);

    return TRUE;
}

/* Find out if the dump is PARTIAL or not, and set the proper driver
   and logfile tags for the dump. */
static void find_completion_tags(dump_info_t * dump_info, /* IN */
                                 cmd_t * result_cmd,      /* OUT */
                                 logtype_t * result_log   /* OUT */) {
    if (taper_source_is_partial(dump_info->source)) {
        *result_cmd = PARTIAL;
        *result_log = L_PARTIAL;
    } else {
        *result_cmd = DONE;
        *result_log = L_DONE;
    }
}

/* Put an L_PARTIAL message to the logfile. */
static void put_partial_log(dump_info_t * dump_info, double dump_time,
                            guint64 dump_kbytes) {
    char * qdiskname = quote_string(dump_info->diskname);

    log_add(L_PARTIAL, "%s %s %s %d %d [sec %f kb %ju kps %f] \"\"",
            dump_info->hostname, qdiskname, dump_info->timestamp,
            dump_info->current_part, dump_info->level, dump_time,
            (uintmax_t)dump_kbytes, dump_kbytes / dump_time);
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
    double part_kbps = run_bytes / (1024 * part_time);
        
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
        
        if (taper_source_get_end_of_data(dump_info->source)) {
            cmd_t result_cmd;
            logtype_t result_log;
            double dump_time = g_timeval_to_double(dump_info->total_time);
            guint64 dump_kbytes = dump_info->total_bytes / 1024;
            double dump_kbps = dump_info->total_bytes / (1024 * dump_time);

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

        /* A problem occured. */
        if (queue_result & QUEUE_CONSUMER_ERROR) {
            /* Close the device. */
            device_finish(taper_state->device);
            g_object_unref(taper_state->device);
            taper_state->device = NULL;
        }
        
        log_add(L_PARTPARTIAL,
                "%s %d %s %s %s %d/%d %d [sec %f kb %ju kps %f] \"\"",
                volume_label, file_number, dump_info->hostname, qdiskname,
                dump_info->timestamp, dump_info->current_part,
                taper_source_predict_parts(dump_info->source),
                dump_info->level, part_time, (uintmax_t)part_kbytes, part_kbps);
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

        dump_info->total_time = timesadd(run_time, dump_info->total_time);
        dump_info->total_bytes += run_bytes;
        dump_time = g_timeval_to_double(dump_info->total_time);
        dump_kbytes = dump_info->total_bytes / 1024;
        dump_kbps = dump_info->total_bytes / (1024 * dump_time);
        
        putresult(PARTIAL,
                  "%s INPUT-%s TAPE-%s "
                  "\"[sec %f kb %ju kps %f]\" \"\" \"\"\n",
                  dump_info->handle,
                  (queue_result & QUEUE_PRODUCER_ERROR) ? "ERROR" : "GOOD",
                  (queue_result & QUEUE_CONSUMER_ERROR) ? "ERROR" : "GOOD",
                  dump_time, (uintmax_t)dump_kbytes, dump_kbps);
        put_partial_log(dump_info, dump_time, dump_kbytes);
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
static void bail_no_volume(dump_info_t * dump_info) {
    if (dump_info->total_bytes > 0) {
        /* Second or later part of a split dump, so PARTIAL message. */
        double dump_time = g_timeval_to_double(dump_info->total_time);
        guint64 dump_kbytes = dump_info->total_bytes / 1024;
        double dump_kbps = dump_kbytes / dump_time;
        putresult(PARTIAL,
                  "%s INPUT-GOOD TAPE-ERROR "
                  "\"[sec %f kb %ju kps %f]\" \"\" \"no new tape\"\n",
                  dump_info->handle, 
                  dump_time, (uintmax_t)dump_kbytes, dump_kbps);
        put_partial_log(dump_info, dump_time, dump_kbytes);
    } else {
        char * qdiskname = quote_string(dump_info->diskname);
        putresult(FAILED,
                  "%s INPUT-GOOD TAPE-ERROR \"\" \"No new tape.\"\n",
                  dump_info->handle);
        log_add(L_FAIL, "%s %s %s %d \"No new tape.\"",
                dump_info->hostname, qdiskname, dump_info->timestamp,
                dump_info->level);
	amfree(qdiskname);
    }
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
            putresult(FAILED,
             "%s INPUT-ERROR TAPE-GOOD \"Failed reading dump header.\" \"\"\n",
                      dump_info->handle);
            log_add(L_FAIL, "%s %s %s %d \"Failed reading dump header.\"",
                    dump_info->hostname, qdiskname, dump_info->timestamp,
                    dump_info->level);
            amfree(qdiskname);
            return;
        }            

        if (!find_and_label_new_tape(taper_state, dump_info)) {
            bail_no_volume(dump_info);
            return;
        }

        if (!device_start_file(taper_state->device, this_header)) {
            bail_no_volume(dump_info);
            return;
        }

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
                device_write_max_size(taper_state->device);
        } else {
            /* Use default. */
            max_memory = getconf_size(CNF_DEVICE_OUTPUT_BUFFER_SIZE);
        }

        queue_result = do_consumer_producer_queue_full
            (taper_source_producer,
             dump_info->source,
             counting_consumer,
             &consumer_data,
             device_write_max_size(taper_state->device), max_memory,
             streaming_mode);

        g_get_current_time(&end_time);
        run_time = timesub(end_time, start_time);

        /* The device_write_consumer may have closed the file with a short
         * write, so we only finish here if it needs it. */
        if (taper_state->device->in_file &&
            !device_finish_file(taper_state->device)) {
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
    char * argnames[] = {"command",               /* 1 */
			 "handle",                /* 2 */
                         "hostname",              /* 3 */
                         "diskname",              /* 4 */
                         "level",                 /* 5 */
                         "datestamp",             /* 6 */
                         "splitsize",             /* 7 */
                         "split_diskbuffer",      /* 8 */
                         "fallback_splitsize",    /* 9 */
                          NULL };

    validate_args(PORT_WRITE, cmdargs, argnames);

    dump_state.handle = g_strdup(cmdargs->argv[2]);
    dump_state.hostname = g_strdup(cmdargs->argv[3]);
    dump_state.diskname = unquote_string(cmdargs->argv[4]);
    
    errno = 0;
    dump_state.level = strtol(cmdargs->argv[5], NULL, 10);
    if (errno != 0) {
        error("error [taper PORT-WRITE: Invalid dump level %s]",
              cmdargs->argv[5]);
        g_assert_not_reached();
    }
    
    dump_state.timestamp = strdup(cmdargs->argv[6]);

    errno = 0;
    splitsize = g_ascii_strtoull(cmdargs->argv[7], NULL, 10);
    if (errno != 0) {
        error("error [taper PORT-WRITE: Invalid splitsize %s]",
              cmdargs->argv[7]);
        g_assert_not_reached();
    }
    
    if (strcmp(cmdargs->argv[8], "NULL") == 0) {
        split_diskbuffer = NULL;
    } else {
        split_diskbuffer = g_strdup(cmdargs->argv[8]);
    }
    
    errno = 0;
    fallback_splitsize = g_ascii_strtoull(cmdargs->argv[9], NULL, 10);
    if (errno != 0) {
        error("error [taper PORT-WRITE: Invalid fallback_splitsize %s]",
              cmdargs->argv[9]);
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
    char * argnames[] = {"command",               /* 1 */
			 "handle",                /* 2 */
                         "filename",              /* 3 */
                         "hostname",              /* 4 */
                         "diskname",              /* 5 */
                         "level",                 /* 6 */
                         "datestamp",             /* 7 */
                         "splitsize",             /* 8 */
                          NULL };

    validate_args(FILE_WRITE, cmdargs, argnames);

    dump_state.handle = g_strdup(cmdargs->argv[2]);
    holding_disk_file = cmdargs->argv[3];
    dump_state.hostname = g_strdup(cmdargs->argv[4]);
    dump_state.diskname = unquote_string(cmdargs->argv[5]);
    
    errno = 0;
    dump_state.level = strtol(cmdargs->argv[6], NULL, 10);
    if (errno != 0) {
        error("error [taper FILE-WRITE: Invalid dump level %s]",
              cmdargs->argv[5]);
        g_assert_not_reached();
    }
    
    dump_state.timestamp = strdup(cmdargs->argv[7]);

    errno = 0;
    splitsize = g_ascii_strtoull(cmdargs->argv[8], NULL, 10);
    if (errno != 0) {
        error("error [taper FILE-WRITE: Invalid splitsize %s]",
              cmdargs->argv[8]);
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
    cmd_t cmd;
    /* Note: cmdargs.argv is never freed. In the entire Amanda codebase. */
    struct cmdargs cmdargs;
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

    cmd = getcmd(&cmdargs);

    switch (cmd) {
    case START_TAPER: {
        gboolean search_result;
        state->driver_start_time = strdup(cmdargs.argv[2]);
        if (use_threads) {
            search_result = GPOINTER_TO_INT(g_thread_join(tape_search));
        } else {
            search_result =
                GPOINTER_TO_INT(tape_search_thread(&search_request));
        }
        if (search_result) {
            putresult(TAPER_OK, "\n");
        } else {
            putresult(TAPE_ERROR, "Could not find a tape to use.\n");
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
        return TRUE;
    }
    case QUIT:
        search_request.prolong = FALSE;
        if (use_threads) {
            g_thread_join(tape_search);
        }
        return send_quitting(state);
    default:
        error("error [file_reader_side cmd %d argc %d]", cmd, cmdargs.argc);
    }

    g_assert_not_reached();
}

/* In running mode (not startup mode), get a command from driver and
   deal with it. */
static gboolean process_driver_command(taper_state_t * state) {
    cmd_t cmd;
    struct cmdargs cmdargs;
    char * q;

    /* This will return QUIT if driver has died. */
    cmd = getcmd(&cmdargs);
    switch (cmd) {
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
        process_port_write(state, &cmdargs);
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
        process_file_write(state, &cmdargs);
        break;
        
    case QUIT:
        return send_quitting(state);
    default:
        if (cmdargs.argc >= 1) {
            q = squote(cmdargs.argv[1]);
        } else if (cmdargs.argc >= 0) {
            q = squote(cmdargs.argv[0]);
        } else {
            q = stralloc("(no input?)");
        }
        putresult(BAD_COMMAND, "%s\n", q);
        amfree(q);
        break;
    }

    return TRUE;
}

int main(int real_argc, char ** real_argv) {
    char *conffile;
    int argc;
    char ** argv;
    char * config_name;
    char * tapelist_name;
    int have_changer;
    taper_state_t state;

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

    parse_conf(real_argc, real_argv, &argc, &argv);
    g_fprintf(stderr, _("%s: pid %ld executable %s version %s\n"),
	    get_pname(), (long) getpid(), argv[0], version());
    dbprintf(_("%s: pid %ld executable %s version %s\n"),
              get_pname(), (long) getpid(), argv[0], version());

    if(argc > 2) {
        error("Too many arguments!\n");
        g_assert_not_reached();
    }
 
    find_configuration(argc > 1, argv[1], &config_name, &config_dir);

    safe_cd();

    set_logerror(logerror);

    free_new_argv(argc, argv);

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    tapelist_name = getconf_str(CNF_TAPELIST);
    if (tapelist_name[0] == '/') {
        tapelist_name = g_strdup(tapelist_name);
    } else {
        tapelist_name = stralloc2(config_dir, tapelist_name);
    }

    if (read_tapelist(tapelist_name) != 0) {
        error("could not load tapelist \"%s\"", tapelist_name);
        g_assert_not_reached();
    }

    have_changer = changer_init();
    if (have_changer < 0) {
        error("changer initialization failed: %s", strerror(errno));
        g_assert_not_reached();
    }

    state.next_tape_label = NULL;
    state.next_tape_device = NULL;
    state.cur_tape = 0;
    
    if (!find_first_tape(&state)) {
        return EXIT_SUCCESS;
    }

    while (process_driver_command(&state));
    return EXIT_SUCCESS;
}
