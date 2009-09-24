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
 * $Id: restore.c 6512 2007-05-24 17:00:24Z ian $
 *
 * retrieves files from an amanda tape
 */

#include "amanda.h"
#include "util.h"
#include "restore.h"
#include "find.h"
#include "changer.h"
#include "logfile.h"
#include "fileheader.h"
#include "arglist.h"
#include "cmdline.h"
#include "server_util.h"
#include <signal.h>
#include <timestamp.h>

#include <device.h>
#include <queueing.h>
#include <glib.h>

typedef enum {
    LOAD_NEXT = 1,     /* An unknown new slot has been loaded. */
    LOAD_CHANGER = -2, /* The requested slot has been loaded. */
    LOAD_STOP = -1,    /* The search is complete. */
} LoadStatus;

typedef enum {
    RESTORE_STATUS_NEXT_FILE,
    RESTORE_STATUS_NEXT_TAPE,
    RESTORE_STATUS_STOP
} RestoreFileStatus;

int file_number;

/* stuff we're stuck having global */
static int backwards;
static int exitassemble = 0;

char *rst_conf_logdir = NULL;
char *rst_conf_logfile = NULL;
static char *curslot = NULL;

typedef struct open_output_s {
    struct open_output_s *next;
    dumpfile_t *file;
    int lastpartnum;
    pid_t comp_enc_pid;
    int outfd;
} open_output_t;

typedef struct dumplist_s {
    struct dumplist_s *next;
    dumpfile_t *file;
} dumplist_t;

struct seentapes_s {
    struct seentapes_s *next;
    char *slotstr;
    char *label;
    dumplist_t *files;
};

static open_output_t *open_outputs = NULL;
static dumplist_t *alldumps_list = NULL;

/* local functions */

static void append_file_to_fd(char *filename, int fd);
static int headers_equal(dumpfile_t *file1, dumpfile_t *file2, int ignore_partnums);
static int already_have_dump(dumpfile_t *file);
static void handle_sigint(int sig);
static int scan_init(void *ud, int rc, int ns, int bk, int s);
static Device * conditional_device_open(char * tapedev, FILE * orompt_out,
                                        rst_flags_t * flags,
                                        am_feature_t * their_features,
                                        tapelist_t * desired_tape);
int loadlabel_slot(void *ud, int rc, char *slotstr, char *device);
char *label_of_current_slot(char *cur_tapedev, FILE *prompt_out,
			    int *tapefd, dumpfile_t *file, rst_flags_t *flags,
			    am_feature_t *their_features,
			    ssize_t *read_result, tapelist_t *desired_tape);

LoadStatus load_next_tape(char **cur_tapedev, FILE *prompt_out, int backwards,
		   rst_flags_t *flags, am_feature_t *their_features,
		   tapelist_t *desired_tape);
LoadStatus load_manual_tape(char **cur_tapedev, FILE *prompt_out, FILE *prompt_in,
		     rst_flags_t *flags, am_feature_t *their_features,
		     tapelist_t *desired_tape);

/*
 * We might want to flush any open dumps and unmerged splits before exiting
 * on SIGINT, so do so.
 */
static void
handle_sigint(
    int		sig)
{
    (void)sig;	/* Quiet unused parameter warning */

    flush_open_outputs(exitassemble, NULL);
    if (rst_conf_logfile) {
	unlink(rst_conf_logfile);
	log_add(L_INFO, "pid-done %ld\n", (long)getpid());
    }
    dbclose();
    exit(0);
}

int
lock_logfile(void)
{
    rst_conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    rst_conf_logfile = vstralloc(rst_conf_logdir, "/log", NULL);
    if (access(rst_conf_logfile, F_OK) == 0) {
	run_amcleanup(get_config_name());
    }
    if (access(rst_conf_logfile, F_OK) == 0) {
	char *process_name = get_master_process(rst_conf_logfile);
	dbprintf(_("%s exists: %s is already running, "
		  "or you must run amcleanup\n"), rst_conf_logfile,
		 process_name);
	amfree(process_name);
	return 0;
    }
    log_add(L_INFO, "%s", get_pname());
    return 1;
}

/*
 * Return 1 if the two fileheaders match in name, disk, type, split chunk part
 * number, and datestamp, and 0 if not.  The part number can be optionally
 * ignored.
 */
int
headers_equal(
    dumpfile_t *file1,
    dumpfile_t *file2,
    int		ignore_partnums)
{
    if(!file1 || !file2) return(0);
    
    if(file1->dumplevel == file2->dumplevel &&
	   file1->type == file2->type &&
	   !strcmp(file1->datestamp, file2->datestamp) &&
	   !strcmp(file1->name, file2->name) &&
	   !strcmp(file1->disk, file2->disk) &&
	   (ignore_partnums || file1->partnum == file2->partnum)){
	return(1);
    }
    return(0);
}


/*
 * See whether we're already pulled an exact copy of the given file (chunk
 * number and all).  Returns 0 if not, 1 if so.
 */
int
already_have_dump(
    dumpfile_t *file)
{
    dumplist_t *fileentry = NULL;

    if(!file) return(0);
    for(fileentry=alldumps_list;fileentry;fileentry=fileentry->next){
	if(headers_equal(file, fileentry->file, 0)) return(1);
    }
    return(0);
}

/*
 * Open the named file and append its contents to the (hopefully open) file
 * descriptor supplies.
 */
static void
append_file_to_fd(
    char *	filename,
    int		write_fd)
{
    queue_fd_t queue_fd_write = {write_fd, NULL};
    queue_fd_t queue_fd_read = {0, NULL};
    

    queue_fd_read.fd = robust_open(filename, O_RDONLY, 0);
    if (queue_fd_read.fd < 0) {
	error(_("can't open %s: %s"), filename, strerror(errno));
	/*NOTREACHED*/
    }

    if (!do_consumer_producer_queue(fd_read_producer, &queue_fd_read,
                                    fd_write_consumer, &queue_fd_write)) {
	if (queue_fd_read.errmsg && queue_fd_write.errmsg) {
	    error("Error copying data from file \"%s\" to fd %d: %s: %s.\n",
		  filename, queue_fd_write.fd, queue_fd_read.errmsg,
		  queue_fd_write.errmsg);
	} else if (queue_fd_read.errmsg) {
	    error("Error copying data from file \"%s\" to fd %d: %s.\n",
		  filename, queue_fd_write.fd, queue_fd_read.errmsg);
	} else if (queue_fd_write.errmsg) {
	    error("Error copying data from file \"%s\" to fd %d: %s.\n",
		  filename, queue_fd_write.fd, queue_fd_write.errmsg);
	} else {
	    error("Error copying data from file \"%s\" to fd %d.\n",
		  filename, queue_fd_write.fd);
	}
        g_assert_not_reached();
    }

    aclose(queue_fd_read.fd);
}

/* A user_init function for changer_find(). See changer.h for
   documentation. */
static int 
scan_init(G_GNUC_UNUSED void *	ud, int	rc, G_GNUC_UNUSED int ns,
          int bk, G_GNUC_UNUSED int s) {
    if(rc) {
        error(_("could not get changer info: %s"), changer_resultstr);
	/*NOTREACHED*/
    }
    backwards = bk;

    return 0;
}

typedef struct {
    char ** cur_tapedev;
    char * searchlabel;
    rst_flags_t *flags;
} loadlabel_data;

/* DANGER WILL ROBINSON: This function references globals:
          char * curslot;
 */
int
loadlabel_slot(void *	datap,
               int	rc,
               char *	slotstr,
               char *	device_name)
{
    loadlabel_data * data = (loadlabel_data*)datap;
    Device * device;
    DeviceStatusFlags device_status;

    /* if there was no error, we should have a device name */
    g_assert(rc > 0 || device_name != NULL);
    g_assert(slotstr != NULL);

    amfree(curslot);

    if(rc > 1) {
        error(_("could not load slot %s: %s"), slotstr, changer_resultstr);
        g_assert_not_reached();
    }

    if(rc == 1) {
        g_fprintf(stderr, _("%s: slot %s: %s\n"),
                get_pname(), slotstr, changer_resultstr);
        return 0;
    } 
    
    device = device_open(device_name);
    g_assert(device != NULL);
    if (device->status != DEVICE_STATUS_SUCCESS) {
        g_fprintf(stderr, "%s: slot %s: Could not open device: %s.\n",
                get_pname(), slotstr, device_error(device));
        return 0;
    }

    if (!device_configure(device, TRUE)) {
        g_fprintf(stderr, "%s: slot %s: Error configuring device:\n"
                "%s: slot %s: %s\n",
                get_pname(), slotstr, get_pname(), slotstr, device_error_or_status(device));
        g_object_unref(device);
        return 0;
    }

    if (!set_restore_device_read_block_size(device, data->flags)) {
        g_fprintf(stderr, "%s: slot %s: Error setting read block size:\n"
                "%s: slot %s: %s\n",
                get_pname(), slotstr, get_pname(), slotstr, device_error_or_status(device));
        g_object_unref(device);
        return 0;
    }
    device_status = device_read_label(device);
    if (device_status != DEVICE_STATUS_SUCCESS) {
        g_fprintf(stderr, "%s: slot %s: Error reading tape label:\n"
                "%s: slot %s: %s\n",
                get_pname(), slotstr, get_pname(), slotstr, device_error_or_status(device));
        g_object_unref(device);
        return 0;
    }

    if (device->volume_label == NULL) {
        g_fprintf(stderr, "%s: slot %s: Could not read tape label.\n",
                get_pname(), slotstr);
        g_object_unref(device);
        return 0;
    }

    if (!device_start(device, ACCESS_READ, NULL, NULL)) {
        g_fprintf(stderr, "%s: slot %s: Could not open device for reading: %s.\n",
                get_pname(), slotstr, device_error(device));
        return 0;
    }

    g_fprintf(stderr, "%s: slot %s: time %-14s label %s",
            get_pname(), slotstr, device->volume_time, device->volume_label);

    if(strcmp(device->volume_label, data->searchlabel) != 0) {
        g_fprintf(stderr, " (wrong tape)\n");
        g_object_unref(device);
        return 0;
    }

    g_fprintf(stderr, " (exact label match)\n");

    g_object_unref(device);
    curslot = newstralloc(curslot, slotstr);
    amfree(*(data->cur_tapedev));
    *(data->cur_tapedev) = stralloc(device_name);
    return 1;
}


/* non-local functions follow */



/*
 * Check whether we've read all of the preceding parts of a given split dump,
 * generally used to see if we're done and can close the thing.
 */
int
have_all_parts (
    dumpfile_t *file,
    int		upto)
{
    int c;
    int *foundparts = NULL;
    dumplist_t *fileentry = NULL;

    if(!file || file->partnum < 1) return(0);

    if(upto < 1) upto = file->totalparts;

    foundparts = alloc(SIZEOF(*foundparts) * upto); 
    for(c = 0 ; c< upto; c++) foundparts[c] = 0;
    
    for(fileentry=alldumps_list;fileentry; fileentry=fileentry->next){
	dumpfile_t *cur_file = fileentry->file;
	if(headers_equal(file, cur_file, 1)){
	    if(cur_file->partnum > upto){
		amfree(foundparts);
		return(0);
	    }

	    foundparts[cur_file->partnum - 1] = 1;
	}
    }

    for(c = 0 ; c< upto; c++){
	if(!foundparts[c]){
	    amfree(foundparts);
	    return(0);
	}
    }
    
    amfree(foundparts);
    return(1);
}

/*
 * Free up the open filehandles and memory we were using to track in-progress
 * dumpfiles (generally for split ones we're putting back together).  If
 * applicable, also find the ones that are continuations of one another and
 * string them together.  If given an optional file header argument, flush
 * only that dump and do not flush/free any others.
 */
void
flush_open_outputs(
    int		reassemble,
    dumpfile_t *only_file)
{
    open_output_t *cur_out = NULL, *prev = NULL;
    find_result_t *sorted_files = NULL;
    amwait_t compress_status;

    if(!only_file){
	g_fprintf(stderr, "\n");
    }

    /*
     * Deal with any split dumps we've been working on, appending pieces
     * that haven't yet been appended and closing filehandles we've been
     * holding onto.
     */
    if(reassemble){
	find_result_t *cur_find_res = NULL;
	int outfd = -1, lastpartnum = -1;
	dumpfile_t *main_file = NULL;
	cur_out = open_outputs;
	
	/* stick the dumpfile_t's into a list find_result_t's so that we can
	   abuse existing sort functionality */
	for(cur_out=open_outputs; cur_out; cur_out=cur_out->next){
	    find_result_t *cur_find_res = NULL;
	    dumpfile_t *cur_file = cur_out->file;
	    /* if we requested a particular file, do only that one */
	    if(only_file && !headers_equal(cur_file, only_file, 1)){
		continue;
	    }
	    cur_find_res = alloc(SIZEOF(find_result_t));
	    memset(cur_find_res, '\0', SIZEOF(find_result_t));
	    cur_find_res->timestamp = stralloc(cur_file->datestamp);
	    cur_find_res->hostname = stralloc(cur_file->name);
	    cur_find_res->diskname = stralloc(cur_file->disk);
	    cur_find_res->level = cur_file->dumplevel;
	    if(cur_file->partnum < 1) cur_find_res->partnum = stralloc("--");
	    else{
		char part_str[NUM_STR_SIZE];
		g_snprintf(part_str, SIZEOF(part_str), "%d", cur_file->partnum);
		cur_find_res->partnum = stralloc(part_str);
	    }
	    cur_find_res->user_ptr = (void*)cur_out;

	    cur_find_res->next = sorted_files;
	    sorted_files = cur_find_res;
	}
	sort_find_result("hkdlp", &sorted_files);

	/* now we have an in-order list of the files we need to concatenate */
	cur_find_res = sorted_files;
	for(cur_find_res=sorted_files;
		cur_find_res;
		cur_find_res=cur_find_res->next){
	    dumpfile_t *cur_file = NULL;
	    cur_out = (open_output_t*)cur_find_res->user_ptr;
	    cur_file = cur_out->file;

	    /* if we requested a particular file, do only that one */
	    if(only_file && !headers_equal(cur_file, only_file, 1)){
		continue;
	    }

	    if(cur_file->type == F_SPLIT_DUMPFILE) {
		/* is it a continuation of one we've been writing? */
		if(main_file && cur_file->partnum > lastpartnum &&
			headers_equal(cur_file, main_file, 1)){
		    char *cur_filename;
		    char *main_filename;

		    /* effectively changing filehandles */
		    aclose(cur_out->outfd);
		    cur_out->outfd = outfd;

		    cur_filename  = make_filename(cur_file);
		    main_filename = make_filename(main_file);
		    g_fprintf(stderr, _("Merging %s with %s\n"),
		            cur_filename, main_filename);
		    append_file_to_fd(cur_filename, outfd);
		    if(unlink(cur_filename) < 0){
			g_fprintf(stderr, _("Failed to unlink %s: %s\n"),
			             cur_filename, strerror(errno));
		    }
		    amfree(cur_filename);
		    amfree(main_filename);
		}
		/* or a new file? */
		else {
		    if(outfd >= 0) aclose(outfd);
		    amfree(main_file);
		    main_file = alloc(SIZEOF(dumpfile_t));
		    memcpy(main_file, cur_file, SIZEOF(dumpfile_t));
		    outfd = cur_out->outfd;
		    if(outfd < 0) {
			char *cur_filename = make_filename(cur_file);
			open(cur_filename, O_RDWR|O_APPEND);
			if (outfd < 0) {
			  error(_("Couldn't open %s for appending: %s"),
			        cur_filename, strerror(errno));
			  /*NOTREACHED*/
			}
			amfree(cur_filename);
		    }
		}
		lastpartnum = cur_file->partnum;
	    }
	    else {
		aclose(cur_out->outfd);
	    }
	}
	if(outfd >= 0) {
	    aclose(outfd);
	}

	amfree(main_file);
	free_find_result(&sorted_files);
    }

    /*
     * Now that the split dump closure is done, free up resources we don't
     * need anymore.
     */
    for(cur_out=open_outputs; cur_out; cur_out=cur_out->next){
	dumpfile_t *cur_file = NULL;
	amfree(prev);
	cur_file = cur_out->file;
	/* if we requested a particular file, do only that one */
	if(only_file && !headers_equal(cur_file, only_file, 1)){
	    continue;
	}
	if(!reassemble) {
	    aclose(cur_out->outfd);
	}

	if(cur_out->comp_enc_pid > 0){
	    waitpid(cur_out->comp_enc_pid, &compress_status, 0);
	}
	amfree(cur_out->file);
	prev = cur_out;
    }

    open_outputs = NULL;
}

/*
 * Turn a fileheader into a string suited for use on the filesystem.
 */
char *
make_filename(
    dumpfile_t *file)
{
    char number[NUM_STR_SIZE];
    char part[NUM_STR_SIZE];
    char totalparts[NUM_STR_SIZE];
    char *sfn = NULL;
    char *fn = NULL;
    char *pad = NULL;
    size_t padlen = 0;

    g_snprintf(number, SIZEOF(number), "%d", file->dumplevel);
    g_snprintf(part, SIZEOF(part), "%d", file->partnum);

    if(file->totalparts < 0) {
	g_snprintf(totalparts, SIZEOF(totalparts), "UNKNOWN");
    }
    else {
	g_snprintf(totalparts, SIZEOF(totalparts), "%d", file->totalparts);
    }
    padlen = strlen(totalparts) + 1 - strlen(part);
    pad = alloc(padlen);
    memset(pad, '0', padlen);
    pad[padlen - 1] = '\0';

    g_snprintf(part, SIZEOF(part), "%s%d", pad, file->partnum);

    sfn = sanitise_filename(file->disk);
    fn = vstralloc(file->name,
		   ".",
		   sfn, 
		   ".",
		   file->datestamp,
		   ".",
		   number,
		   NULL);
    if (file->partnum > 0) {
	vstrextend(&fn, ".", part, NULL);
    }
    amfree(sfn);
    amfree(pad);
    return fn;
}

/* Returns 1 if the dump file matches the hostname and diskname
 * regular expressions given on the command line, 0 otherwise.  As a 
 * special case, empty regexs and NULLs are considered equivalent to 
 * ".*": they match everything.
 *
 * @param file: the file to examine
 * @param datestamp: the datestamp regex, or NULL for any
 * @param hostname: the hostname regex, or NULL for any
 * @param diskname: the diskname regex, or NULL for any
 * @param level: the level regex, or NULL for any
 * @returns: 1 if the dump file matches
 */
static int
disk_match(
    dumpfile_t *file,
    char *	datestamp,
    char *	hostname,
    char *	diskname,
    char *	level)
{
    char level_str[NUM_STR_SIZE];
    g_snprintf(level_str, SIZEOF(level_str), "%d", file->dumplevel);

    if(file->type != F_DUMPFILE && file->type != F_SPLIT_DUMPFILE) return 0;

    if((!hostname || *hostname == '\0' || match_host(hostname, file->name)) &&
       (!diskname || *diskname == '\0' || match_disk(diskname, file->disk)) &&
       (!datestamp || *datestamp == '\0' || match_datestamp(datestamp, file->datestamp)) &&
       (!level || *level == '\0' || match_level(level, level_str)))
	return 1;
    else
	return 0;
}

/*
 * Reads the first block of a holding disk file.
 */

static gboolean
read_holding_disk_header(
    dumpfile_t *       file,
    int                        tapefd,
    rst_flags_t *      flags)
{
    size_t bytes_read;
    char *buffer;
    size_t blocksize;

    if(flags->blocksize > 0)
        blocksize = (size_t)flags->blocksize;
    else
        blocksize = DISK_BLOCK_BYTES;
    buffer = alloc(blocksize);

    bytes_read = full_read(tapefd, buffer, blocksize);
    if(bytes_read < blocksize) {
	const char *errtxt;
	if(errno == 0)
	    errtxt = "Unexpected EOF";
	else
	    errtxt = strerror(errno);

	if (bytes_read == 0) {
	    g_fprintf(stderr, _("%s: missing file header block: %s\n"), 
		get_pname(), errtxt);
	} else {
	    g_fprintf(stderr,
		    plural(_("%s: short file header block: %zd byte: %s"),
	    		   _("%s: short file header block: %zd bytes: %s\n"),
			   bytes_read),
		    get_pname(), bytes_read, errtxt);
	}
	file->type = F_UNKNOWN;
    } else {
        parse_file_header(buffer, file, bytes_read);
    }
    amfree(buffer);
    return (file->type != F_UNKNOWN &&
            file->type != F_EMPTY &&
            file->type != F_WEIRD);
}

/*
 * Restore the current file from tape.  Depending on the settings of
 * the command line flags, the file might need to be compressed or
 * uncompressed.  If so, a pipe through compress or uncompress is set
 * up.  The final output usually goes to a file named host.disk.date.lev,
 * but with the -p flag the output goes to stdout (and presumably is
 * piped to restore).
 */


/* FIXME: Mondo function that needs refactoring. */
void restore(RestoreSource * source,
             rst_flags_t *	flags)
{
    int dest = -1, out;
    int file_is_compressed;
    int is_continuation = 0;
    int check_for_aborted = 0;
    char *tmp_filename = NULL, *final_filename = NULL;
    struct stat statinfo;
    open_output_t *free_myout = NULL, *myout = NULL, *oldout = NULL;
    dumplist_t *tempdump = NULL, *fileentry = NULL;
    char *buffer;
    int need_compress=0, need_uncompress=0, need_decrypt=0;
    int stage=0;
    struct pipeline {
        int	pipe[2];
    } pipes[3];
    char * filename;

    filename = make_filename(source->header);

    memset(pipes, -1, SIZEOF(pipes));

    if(already_have_dump(source->header)){
	g_fprintf(stderr, _(" *** Duplicate file %s, one is probably an aborted write\n"), filename);
	check_for_aborted = 1;
    }

    /* store a shorthand record of this dump */
    tempdump = malloc(SIZEOF(dumplist_t));
    tempdump->file = malloc(SIZEOF(dumpfile_t));
    tempdump->next = NULL;
    memcpy(tempdump->file, source->header, SIZEOF(dumpfile_t));

    /*
     * If we're appending chunked files to one another, and if this is a
     * continuation of a file we just restored, and we've still got the
     * output handle from that previous restore, we're golden.  Phew.
     */
    if(flags->inline_assemble && source->header->type == F_SPLIT_DUMPFILE){
	myout = open_outputs;
	while(myout != NULL){
	    if(myout->file->type == F_SPLIT_DUMPFILE &&
               headers_equal(source->header, myout->file, 1)){
		if(source->header->partnum == myout->lastpartnum + 1){
		    is_continuation = 1;
		    break;
		}
	    }
	    myout = myout->next;
	}
	if(myout != NULL) myout->lastpartnum = source->header->partnum;
	else if(source->header->partnum != 1){
	    g_fprintf(stderr, _("%s:      Chunk out of order, will save to disk and append to output.\n"), get_pname());
	    flags->pipe_to_fd = -1;
	    flags->compress = 0;
	    flags->leave_comp = 1;
	}
	if(myout == NULL){
	    free_myout = myout = alloc(SIZEOF(open_output_t));
	    memset(myout, 0, SIZEOF(open_output_t));
	}
    }
    else{
      free_myout = myout = alloc(SIZEOF(open_output_t));
      memset(myout, 0, SIZEOF(open_output_t));
    }


    if(is_continuation && flags->pipe_to_fd == -1){
	char *filename;
	filename = make_filename(myout->file);
	g_fprintf(stderr, _("%s:      appending to %s\n"), get_pname(),
		filename);
	amfree(filename);
    }

    /* adjust compression flag */
    file_is_compressed = source->header->compressed;
    if(!flags->compress && file_is_compressed &&
       !known_compress_type(source->header)) {
	g_fprintf(stderr, 
		_("%s: unknown compression suffix %s, can't uncompress\n"),
		get_pname(), source->header->comp_suffix);
	flags->compress = 1;
    }

    /* set up final destination file */

    if(is_continuation && myout != NULL) {
      out = myout->outfd;
    } else {
      if(flags->pipe_to_fd != -1) {
  	  dest = flags->pipe_to_fd;
      } else {
  	  char *filename_ext = NULL;
  
  	  if(flags->compress) {
  	      filename_ext = file_is_compressed ? source->header->comp_suffix
  	  				      : COMPRESS_SUFFIX;
  	  } else if(flags->raw) {
  	      filename_ext = ".RAW";
  	  } else {
  	      filename_ext = "";
  	  }
  	  filename_ext = stralloc2(filename, filename_ext);
	  tmp_filename = stralloc(filename_ext); 
	  if(flags->restore_dir != NULL) {
	      char *tmpstr = vstralloc(flags->restore_dir, "/",
	                               tmp_filename, NULL);
	      amfree(tmp_filename);
	      tmp_filename = tmpstr;
	  } 
	  final_filename = tmp_filename; 
	  tmp_filename = vstralloc(final_filename, ".tmp", NULL);
  	  if((dest = open(tmp_filename, (O_CREAT | O_RDWR | O_TRUNC),
			  CREAT_MODE)) < 0) {
  	      error(_("could not create output file %s: %s"),
	      	    tmp_filename, strerror(errno));
              /*NOTREACHED*/
	  }
  	  amfree(filename_ext);
      }
  
      out = dest;
    }

    /*
     * If -r or -h, write the header before compress or uncompress pipe.
     * Only write DISK_BLOCK_BYTES, regardless of how much was read.
     * This makes the output look like a holding disk image, and also
     * makes it easier to remove the header (e.g. in amrecover) since
     * it has a fixed size.
     */
    if(flags->raw || (flags->headers && !is_continuation)) {
	ssize_t     w;
	dumpfile_t  tmp_hdr;
	char       *dle_str;

	if(flags->compress && !file_is_compressed) {
	    source->header->compressed = 1;
	    g_snprintf(source->header->uncompress_cmd,
                     SIZEOF(source->header->uncompress_cmd),
		        " %s %s |", UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
		        UNCOMPRESS_OPT
#else
		        ""
#endif
		        );
	    strncpy(source->header->comp_suffix,
		    COMPRESS_SUFFIX,
		    SIZEOF(source->header->comp_suffix)-1);
	    source->header->comp_suffix[SIZEOF(source->header->comp_suffix)-1]
                = '\0';
	}

	memcpy(&tmp_hdr, source->header, SIZEOF(dumpfile_t));

	/* remove CONT_FILENAME from header */
	memset(source->header->cont_filename, '\0',
               SIZEOF(source->header->cont_filename));
	dle_str = clean_dle_str_for_client(source->header->dle_str);
	source->header->dle_str = dle_str;
	source->header->blocksize = DISK_BLOCK_BYTES;

	/*
	 * Dumb down split file headers as well, so that older versions of
	 * things like amrecover won't gag on them.
	 */
	if(source->header->type == F_SPLIT_DUMPFILE && flags->mask_splits){
	    source->header->type = F_DUMPFILE;
	}

	buffer = build_header(source->header, NULL, DISK_BLOCK_BYTES);
	if (!buffer) /* this shouldn't happen */
	    error(_("header does not fit in %zd bytes"), (size_t)DISK_BLOCK_BYTES);

	if (flags->header_to_fd != -1) {
	    w = full_write(flags->header_to_fd, buffer, DISK_BLOCK_BYTES);
	} else {
	    w = full_write(out, buffer, DISK_BLOCK_BYTES);
	}
	if (w != DISK_BLOCK_BYTES) {
	    if(errno != 0) {
		error(_("write error: %s"), strerror(errno));
		/*NOTREACHED*/
	    } else {
		error(_("write error: %zd instead of %d"), w, DISK_BLOCK_BYTES);
		/*NOTREACHED*/
	    }
	}
	if (flags->header_to_fd != -1 &&
	    flags->header_to_fd != flags->pipe_to_fd &&
	    flags->pipe_to_fd != -1) {
	    close(flags->header_to_fd);
	    flags->header_to_fd = -1;
	}
	amfree(buffer);
	memcpy(source->header, &tmp_hdr, SIZEOF(dumpfile_t));
    }

    /* find out if compression or uncompression is needed here */
    if(flags->compress && !file_is_compressed && !is_continuation
	  && !flags->leave_comp
	  && (flags->inline_assemble ||
              source->header->type != F_SPLIT_DUMPFILE))
       need_compress=1;
       
    if(!flags->raw && !flags->compress && file_is_compressed
	  && !is_continuation && !flags->leave_comp && (flags->inline_assemble
	  || source->header->type != F_SPLIT_DUMPFILE))
       need_uncompress=1;   

    if(!flags->raw && source->header->encrypted && !is_continuation &&
       (flags->inline_assemble || source->header->type != F_SPLIT_DUMPFILE)) {
       need_decrypt=1;
    }
   
    /* Setup pipes for decryption / compression / uncompression  */
    stage = 0;
    if (need_decrypt) {
      if (pipe(&pipes[stage].pipe[0]) < 0) {
        error(_("error [pipe[%d]: %s]"), stage, strerror(errno));
	/*NOTREACHED*/
      }
      stage++;
    }

    if (need_compress || need_uncompress) {
      if (pipe(&pipes[stage].pipe[0]) < 0) {
        error(_("error [pipe[%d]: %s]"), stage, strerror(errno));
	/*NOTREACHED*/
      }
      stage++;
    }
    pipes[stage].pipe[0] = -1; 
    pipes[stage].pipe[1] = out; 

    stage = 0;

    /* decrypt first if it's encrypted and no -r */
    if(need_decrypt) {
      switch(myout->comp_enc_pid = fork()) {
      case -1:
	error(_("could not fork for decrypt: %s"), strerror(errno));
	/*NOTREACHED*/

      default:
	aclose(pipes[stage].pipe[0]);
	aclose(pipes[stage+1].pipe[1]);
        stage++;
	break;

      case 0:
	if(dup2(pipes[stage].pipe[0], 0) == -1) {
	    error(_("error decrypt stdin [dup2 %d %d: %s]"), stage,
	        pipes[stage].pipe[0], strerror(errno));
		/*NOTREACHED*/
	}

	if(dup2(pipes[stage+1].pipe[1], 1) == -1) {
	    error(_("error decrypt stdout [dup2 %d %d: %s]"), stage + 1,
	        pipes[stage+1].pipe[1], strerror(errno));
		/*NOTREACHED*/
	}

	safe_fd(-1, 0);
	if (source->header->srv_encrypt[0] != '\0') {
	  (void) execlp(source->header->srv_encrypt,
                        source->header->srv_encrypt,
			source->header->srv_decrypt_opt, NULL);
	  error("could not exec %s: %s",
                source->header->srv_encrypt, strerror(errno));
          g_assert_not_reached();
	}  else if (source->header->clnt_encrypt[0] != '\0') {
	  (void) execlp(source->header->clnt_encrypt,
                        source->header->clnt_encrypt,
			source->header->clnt_decrypt_opt, NULL);
	  error("could not exec %s: %s",
                source->header->clnt_encrypt, strerror(errno));
          g_assert_not_reached();
	}
      }
    }

    if (need_compress) {
        /*
         * Insert a compress pipe
         */
	switch(myout->comp_enc_pid = fork()) {
	case -1:
	    error(_("could not fork for %s: %s"), COMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/

	default:
	    aclose(pipes[stage].pipe[0]);
	    aclose(pipes[stage+1].pipe[1]);
            stage++;
	    break;

	case 0:
	    if(dup2(pipes[stage].pipe[0], 0) == -1) {
		error(_("error compress stdin [dup2 %d %d: %s]"), stage,
		  pipes[stage].pipe[0], strerror(errno));
	        /*NOTREACHED*/
	    }

	    if(dup2(pipes[stage+1].pipe[1], 1) == -1) {
		error(_("error compress stdout [dup2 %d %d: %s]"), stage + 1,
		  pipes[stage+1].pipe[1], strerror(errno));
		  /*NOTREACHED*/
	    }
	    if (*flags->comp_type == '\0') {
		flags->comp_type = NULL;
	    }

	    safe_fd(-1, 0);
	    (void) execlp(COMPRESS_PATH, COMPRESS_PATH, flags->comp_type, (char *)0);
	    error(_("could not exec %s: %s"), COMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/
	}
    } else if(need_uncompress) {
        /*
         * If not -r, -c, -l, and file is compressed, and split reassembly 
         * options are sane, insert uncompress pipe
         */

	/* 
	 * XXX for now we know that for the two compression types we
	 * understand, .Z and optionally .gz, UNCOMPRESS_PATH will take
	 * care of both.  Later, we may need to reference a table of
	 * possible uncompress programs.
	 */ 
	switch(myout->comp_enc_pid = fork()) {
	case -1: 
	    error(_("could not fork for %s: %s"),
		  UNCOMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/

	default:
	    aclose(pipes[stage].pipe[0]);
	    aclose(pipes[stage+1].pipe[1]);
            stage++;
	    break;

	case 0:
	    if(dup2(pipes[stage].pipe[0], 0) == -1) {
		error(_("error uncompress stdin [dup2 %d %d: %s]"), stage,
		  pipes[stage].pipe[0], strerror(errno));
	        /*NOTREACHED*/
	    }

	    if(dup2(pipes[stage+1].pipe[1], 1) == -1) {
		error(_("error uncompress stdout [dup2 %d %d: %s]"), stage + 1,
		  pipes[stage+1].pipe[1], strerror(errno));
	        /*NOTREACHED*/
	    }

	    safe_fd(-1, 0);
	    if (source->header->srvcompprog[0] != '\0') {
	      (void) execlp(source->header->srvcompprog,
                            source->header->srvcompprog, "-d", NULL);
	      error("could not exec %s: %s", source->header->srvcompprog,
                    strerror(errno));
              g_assert_not_reached();
	    } else if (source->header->clntcompprog[0] != '\0') {
	      (void) execlp(source->header->clntcompprog,
                            source->header->clntcompprog, "-d", NULL);
	      error("could not exec %s: %s", source->header->clntcompprog,
                    strerror(errno));
              g_assert_not_reached();
	    } else {
	      (void) execlp(UNCOMPRESS_PATH, UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
			  UNCOMPRESS_OPT,
#endif
			  (char *)NULL);
	      error(_("could not exec %s: %s"), UNCOMPRESS_PATH, strerror(errno));
	      /*NOTREACHED*/
	    }
	}
    }

    /* copy the rest of the file from tape to the output */
    if (source->restore_mode == HOLDING_MODE) {
        dumpfile_t file;
	queue_fd_t queue_read = {source->u.holding_fd, NULL};
	queue_fd_t queue_write = {pipes[0].pipe[1], NULL};
        memcpy(& file, source->header, sizeof(file));
        for (;;) {
            if (!do_consumer_producer_queue(fd_read_producer,
                                            &queue_read,
                                            fd_write_consumer,
                                            &queue_write)) {
		if (queue_read.errmsg && queue_write.errmsg) {
		    error("Error copying data from holding file to fd %d: %s: %s.\n",
	                  queue_write.fd, queue_read.errmsg,
	                  queue_write.errmsg);
		} else if (queue_read.errmsg) {
		    error("Error copying data from holding file to fd %d: %s.\n",
	                  queue_write.fd, queue_read.errmsg);
		} else if (queue_write.errmsg) {
		    error("Error copying data from holding file to fd %d: %s.\n",
	                  queue_write.fd, queue_write.errmsg);
		} else {
		    error("Error copying data from holding file to fd %d: %s.\n",
	                  queue_write.fd, "unknown error");
		}
	    }
	    /*
	     * See if we need to switch to the next file in a holding restore
	     */
	    if(file.cont_filename[0] == '\0') {
		break;				/* no more files */
	    }
	    aclose(queue_read.fd);
	    if((queue_read.fd = open(file.cont_filename, O_RDONLY)) == -1) {
		char *cont_filename =
                    strrchr(file.cont_filename,'/');
		if(cont_filename) {
		    cont_filename++;
		    if((queue_read.fd = open(cont_filename,O_RDONLY)) == -1) {
			error(_("can't open %s: %s"), file.cont_filename,
			      strerror(errno));
		        /*NOTREACHED*/
		    }
		    else {
			g_fprintf(stderr, _("cannot open %s: %s\n"),
				file.cont_filename, strerror(errno));
			g_fprintf(stderr, _("using %s\n"),
				cont_filename);
		    }
		}
		else {
		    error(_("can't open %s: %s"), file.cont_filename,
			  strerror(errno));
		    /*NOTREACHED*/
		}
	    }
	    read_holding_disk_header(&file, queue_read.fd, flags);
	    if(file.type != F_DUMPFILE && file.type != F_CONT_DUMPFILE
		    && file.type != F_SPLIT_DUMPFILE) {
		g_fprintf(stderr, _("unexpected header type: "));
		print_header(stderr, source->header);
		dbclose();
		exit(2);
	    }
	}            
    } else {
	queue_fd_t queue_fd = {pipes[0].pipe[1], NULL};
        gboolean result = device_read_to_fd(source->u.device, &queue_fd);
	if (source->u.device->status != DEVICE_STATUS_SUCCESS) {
	    g_fprintf(stderr, "%s\n", device_error_or_status(source->u.device));
	    dbclose();
	    exit(2);
	}
	if (!result) {
	    if (queue_fd.errmsg) {
		g_fprintf(stderr, _("Problem writing data to pipe: %s.\n"), queue_fd.errmsg);
	    } else {
		g_fprintf(stderr, _("Problem writing data to pipe: Unknown reason.\n"));
	    }
	    dbclose();
	    exit(2);
	}
    }

    amfree(free_myout);
    if(!flags->inline_assemble) {
        if(out != dest)
	    aclose(out);
    }
    if(!is_continuation){
	if(tmp_filename && stat(tmp_filename, &statinfo) < 0){
	    error(_("Can't stat the file I just created (%s)!"), tmp_filename);
	    /*NOTREACHED*/
	} else {
	    statinfo.st_size = (off_t)0;
	}
	if (check_for_aborted && final_filename) {
	    char *old_dump = final_filename;
	    struct stat oldstat;
	    if(stat(old_dump, &oldstat) >= 0){
		if(oldstat.st_size <= statinfo.st_size){
		    dumplist_t *prev_fileentry = NULL;
		    open_output_t *prev_out = NULL;
		    g_fprintf(stderr, _("Newer restore is larger, using that\n"));
		    /* nuke the old dump's entry in alldump_list */
		    for(fileentry=alldumps_list;
			    fileentry->next;
			    fileentry=fileentry->next){
			if(headers_equal(source->header,
                                         fileentry->file, 0)){
			    if(prev_fileentry){
				prev_fileentry->next = fileentry->next;
			    }
			    else {
				alldumps_list = fileentry->next;
			    }
			    amfree(fileentry);
			    break;
			}
			prev_fileentry = fileentry;
		    }
		    myout = open_outputs;
		    while(myout != NULL){
			if(headers_equal(source->header, myout->file, 0)){
			    if(myout->outfd >= 0)
				aclose(myout->outfd);
			    if(prev_out){
				prev_out->next = myout->next;
			    }
			    else open_outputs = myout->next;
			    amfree(myout);
			    break;
			}
			prev_out = myout;
			myout = myout->next;
		    }
		}
		else{
		    g_fprintf(stderr, _("Older restore is larger, using that\n"));
		    if (tmp_filename)
			unlink(tmp_filename);
		    amfree(tempdump->file);
		    amfree(tempdump);
		    amfree(tmp_filename);
		    amfree(final_filename);
                    amfree(filename);
                    return;
		}
	    }
	}
	if(tmp_filename && final_filename &&
           rename(tmp_filename, final_filename) < 0) {
	    error(_("Can't rename %s to %s: %s"),
	    	   tmp_filename, final_filename, strerror(errno));
	    /*NOTREACHED*/
	}
    }
    amfree(tmp_filename);
    amfree(final_filename);


    /*
     * actually insert tracking data for this file into our various
     * structures (we waited in case we needed to give up)
     */
    if(!is_continuation){
        oldout = alloc(SIZEOF(open_output_t));
        oldout->file = alloc(SIZEOF(dumpfile_t));
        memcpy(oldout->file, source->header, SIZEOF(dumpfile_t));
        if(flags->inline_assemble) oldout->outfd = pipes[0].pipe[1];
	else oldout->outfd = -1;
        oldout->comp_enc_pid = -1;
        oldout->lastpartnum = source->header->partnum;
        oldout->next = open_outputs;
        open_outputs = oldout;
    }
    if(alldumps_list){
	fileentry = alldumps_list;
	while (fileentry->next != NULL)
	    fileentry=fileentry->next;
	fileentry->next = tempdump;
    }
    else {
	alldumps_list = tempdump;
    }
}

gboolean
set_restore_device_read_block_size(
    Device *device,
    rst_flags_t *flags)
{
    /* if the user specified a blocksize, try to use it */
    if (flags->blocksize) {
	GValue val;
	gboolean success;

	bzero(&val, sizeof(GValue));

	g_value_init(&val, G_TYPE_UINT);
	g_value_set_uint(&val, flags->blocksize);
	success = device_property_set(device, PROPERTY_READ_BLOCK_SIZE, &val);
	g_value_unset(&val);
	if (!success) {
	    if (device->status == DEVICE_STATUS_SUCCESS) {
		/* device doesn't have this property, so quietly ignore it */
		g_warning(_("Device %s does not support PROPERTY_READ_BLOCK_SIZE; ignoring block size %zd"),
			device->device_name, flags->blocksize);
	    } else {
		/* it's a real error */
		return FALSE;
	    }
	}
    }

    return TRUE;
}

/* return NULL if the label is not the expected one                     */
/* returns a Device handle if it is the expected one. */
/* FIXME: Was label_of_current_slot */
static Device *
conditional_device_open(char         *tapedev,
                        FILE         *prompt_out,
                        rst_flags_t  *flags,
                        am_feature_t *their_features,
                        tapelist_t   *desired_tape)
{
    Device * rval;

    if (tapedev == NULL) {
	send_message(prompt_out, flags, their_features,
		     _("Volume labeled '%s' not found."), desired_tape->label);
        return NULL;
    }

    rval = device_open(tapedev);
    g_assert(rval != NULL);
    if (rval->status != DEVICE_STATUS_SUCCESS) {
	send_message(prompt_out, flags, their_features, 
		     "Error opening device '%s': %s.",
		     tapedev, device_error(rval));
        g_object_unref(rval);
        return NULL;
    }

    if (!device_configure(rval, TRUE)) {
        g_fprintf(stderr, "Error configuring device: %s\n", device_error_or_status(rval));
        g_object_unref(rval);
        return NULL;
    }

    if (!set_restore_device_read_block_size(rval, flags)) {
	send_message(prompt_out, flags, their_features,
		     "Error setting read block size on '%s': %s.",
		     tapedev, device_error(rval));
        g_object_unref(rval);
        return NULL;
    }
    device_read_label(rval);

    if (rval->volume_label == NULL) {
	char *errstr = stralloc2("Not an amanda tape: ",
				 device_error(rval));
        send_message(prompt_out, flags, their_features, "%s", errstr);
	amfree(errstr);
        g_object_unref(rval);
        return NULL;
    }

    if (!device_start(rval, ACCESS_READ, NULL, NULL)) {
        send_message(prompt_out, flags, their_features,
                     "Colud not open device %s for reading: %s.\n",
                     tapedev, device_error(rval));
        return NULL;
    }

    if (flags->check_labels && desired_tape &&
        strcmp(rval->volume_label, desired_tape->label) != 0) {
        send_message(prompt_out, flags, their_features,
                     "Label mismatch, got %s and expected %s",
                     rval->volume_label, desired_tape->label);
        g_object_unref(rval);
        return NULL;
    }

    return rval;
}

/* Do the right thing to try and load the next required tape. See
   LoadStatus above for return value meaning. */
LoadStatus
load_next_tape(
    char         **cur_tapedev,
    FILE          *prompt_out,
    int            backwards,
    rst_flags_t   *flags,
    am_feature_t  *their_features,
    tapelist_t    *desired_tape)
{
    if (desired_tape) {
	send_message(prompt_out, flags, their_features,
		     _("Looking for tape %s..."),
		     desired_tape->label);
	if (backwards) {
            loadlabel_data data;
            data.cur_tapedev = cur_tapedev;
            data.searchlabel = desired_tape->label;
	    data.flags = flags;
	    changer_find(&data, scan_init, loadlabel_slot,
			 desired_tape->label);
            return LOAD_CHANGER;
	} else {
	    amfree(curslot);
	    changer_loadslot("next", &curslot,
			     cur_tapedev);
            return LOAD_NEXT;
	}
    } else {
	assert(!flags->amidxtaped);
	amfree(curslot);
	changer_loadslot("next", &curslot, cur_tapedev);
        return LOAD_NEXT;
    }

    g_assert_not_reached();
}


/* will never return LOAD_CHANGER. */
LoadStatus
load_manual_tape(
    char         **tapedev_ptr,
    FILE          *prompt_out,
    FILE          *prompt_in,
    rst_flags_t   *flags,
    am_feature_t  *their_features,
    tapelist_t    *desired_tape)
{
    char *input = NULL;

    if (flags->amidxtaped) {
	if (their_features &&
	    am_has_feature(their_features,
			   fe_amrecover_FEEDME)) {
	    g_fprintf(prompt_out, "FEEDME %s\r\n",
		    desired_tape->label);
	    fflush(prompt_out);
	    input = agets(prompt_in);/* Strips \n but not \r */
	    if(!input) {
		error(_("Connection lost with amrecover"));
		/*NOTREACHED*/
	    } else if (strcmp("OK\r", input) == 0) {
	    } else if (strncmp("TAPE ", input, 5) == 0) {
		amfree(*tapedev_ptr);
		*tapedev_ptr = alloc(1025);
		if (sscanf(input, "TAPE %1024s\r", *tapedev_ptr) != 1) {
		    error(_("Got bad response from amrecover: %s"), input);
		    /*NOTREACHED*/
		}
	    } else {
		send_message(prompt_out, flags, their_features,
			     _("Got bad response from amrecover: %s"), input);
		error(_("Got bad response from amrecover: %s"), input);
		/*NOTREACHED*/
	    }
	} else {
	    send_message(prompt_out, flags, their_features,
			 _("Client doesn't support fe_amrecover_FEEDME"));
	    error(_("Client doesn't support fe_amrecover_FEEDME"));
	    /*NOTREACHED*/
	}
    }
    else {
	if (desired_tape) {
	    g_fprintf(prompt_out,
		    _("Insert tape labeled %s in device %s \n"
		    "and press enter, ^D to finish reading tapes\n"),
		    desired_tape->label, *tapedev_ptr);
	} else {
	    g_fprintf(prompt_out,_("Insert a tape to search and press "
		    "enter, ^D to finish reading tapes\n"));
	}
	fflush(prompt_out);
	if((input = agets(prompt_in)) == NULL)
            return LOAD_STOP;
    }

    amfree(input);
    return LOAD_NEXT;
}

/* Search a seen-tapes list for a particular name, to see if we've already
 * processed this tape. Returns TRUE if this label has already been seen. */
static gboolean check_volume_seen(seentapes_t * list, char * label) {
    seentapes_t * cur_tape;
    for (cur_tape = list; cur_tape != NULL; cur_tape = cur_tape->next) {
        if (strcmp(cur_tape->label, label) == 0) {
            return TRUE;
        }
    }
    return FALSE;
}

/* Add a volume to the seen tapes list. */
static void record_seen_volume(seentapes_t ** list, char * label,
                               char * slotstr) {
    seentapes_t * new_entry;

    if (list == NULL)
        return;

    new_entry = malloc(sizeof(seentapes_t));
    new_entry->label = stralloc(label);
    if (slotstr == NULL) {
        new_entry->slotstr = NULL;
    } else {
        new_entry->slotstr = stralloc(slotstr);
    }
    new_entry->files = NULL;
    new_entry->next = *list;
    *list = new_entry;
}

/* Record a specific dump on a volume. */
static void record_seen_dump(seentapes_t * volume, dumpfile_t * header) {
    dumplist_t * this_dump;

    if (volume == NULL)
        return;
    
    this_dump = malloc(sizeof(*this_dump));
    this_dump->file = g_memdup(header, sizeof(*header));
    this_dump->next = NULL;
    if (volume->files) {
        dumplist_t * tmp_dump = volume->files;
        while (tmp_dump->next != NULL) {
            tmp_dump = tmp_dump->next;
        }
        tmp_dump->next = this_dump;
    } else {
        volume->files = this_dump;
    }
}

static void print_tape_inventory(FILE * logstream, seentapes_t * tape_seen,
                                 char * timestamp, char * label,
                                 int tape_count) {
    char * logline;
    dumplist_t * fileentry;

    logline = log_genstring(L_START, "taper",
                            "datestamp %s label %s tape %d",
                            timestamp, label, tape_count);
    fputs(logline, logstream);
    amfree(logline);
    for(fileentry=tape_seen->files; fileentry; fileentry=fileentry->next){
        switch (fileentry->file->type) {
        case F_DUMPFILE:
            logline = log_genstring(L_SUCCESS, "taper",
                                    "%s %s %s %d [faked log entry]",
                                    fileentry->file->name,
                                    fileentry->file->disk,
                                    fileentry->file->datestamp,
                                    fileentry->file->dumplevel);
            break;
        case F_SPLIT_DUMPFILE:
            logline = log_genstring(L_CHUNK, "taper", 
                                    "%s %s %s %d %d [faked log entry]",
                                    fileentry->file->name,
                                    fileentry->file->disk,
                                    fileentry->file->datestamp,
                                    fileentry->file->partnum,
                                    fileentry->file->dumplevel);
            break;
        default:
            break;
        }
        if(logline != NULL){
            fputs(logline, logstream);
            amfree(logline);
            fflush(logstream);
        }
    }
}

/* Check if the given header matches the given dumpspecs. Returns
   TRUE if dumpspecs is NULL and false if the header is NULL. Returns
   true if the header matches the  match list. */
static gboolean run_dumpspecs(GSList * dumpspecs,
                               dumpfile_t * header) {
    dumpspec_t *ds;

    if (dumpspecs == NULL)
        return TRUE;
    if (header == NULL)
        return FALSE;

    while (dumpspecs) {
	ds = (dumpspec_t *)dumpspecs->data;
        if (disk_match(header, ds->datestamp, ds->host,
                       ds->disk, ds->level) != 0) {
            return TRUE;
        }
	dumpspecs = dumpspecs->next;
    }

    return FALSE;
}

/* A wrapper around restore() above. This function does some extra
   checking to seek to the file in question and ensure that we really,
   really want to use it.

   The next_file argument provides instruction on what to do if the
   requested file does not exist on the volume: If next_file is NULL
   then if the requested file is missing the function will return
   RESTORE_STATUS_NEXT_FILE. If next_file is not NULL then the first
   extant file whose number is equal to or greater than file_num will
   be attempted. *next_file will be filled in with the number of the
   file following the one that was attempted. */
static RestoreFileStatus
try_restore_single_file(Device * device, int file_num, int* next_file,
                        FILE * prompt_out,
                        rst_flags_t * flags,
                        am_feature_t * their_features,
                        dumpfile_t * first_restored_file,
                        GSList * dumpspecs,
                        seentapes_t * tape_seen) {
    char *qdisk;
    RestoreSource source;
    source.u.device = device;
    source.restore_mode = DEVICE_MODE;

    source.header = device_seek_file(device, file_num);

    if (source.header == NULL) {
        /* This definitely indicates an error. */
        send_message(prompt_out, flags, their_features,
                     "Could not seek device %s to file %d: %s.",
                     device->device_name, file_num,
		     device_error(device));
        return RESTORE_STATUS_NEXT_TAPE;
    } else if (source.header->type == F_TAPEEND) {
        amfree(source.header);
        return RESTORE_STATUS_NEXT_TAPE;
    } else if (device->file != file_num) {
        if (next_file == NULL) {
            send_message(prompt_out, flags, their_features,
                         "Requested file %d does not exist.",
                         file_num);
            return RESTORE_STATUS_NEXT_FILE;
        } else {
            send_message(prompt_out, flags, their_features,
                         "Skipped from file %d to file %d.", 
                         file_num, device->file);
            file_num = device->file;
        }
    }
    if (!am_has_feature(their_features, fe_amrecover_dle_in_header)) {
	source.header->dle_str = NULL;
    }

    if (next_file != NULL) {
        *next_file = file_num + 1;
    }
    
    g_return_val_if_fail(source.header->type == F_DUMPFILE ||
                         source.header->type == F_CONT_DUMPFILE ||
                         source.header->type == F_SPLIT_DUMPFILE,
                         RESTORE_STATUS_NEXT_FILE);
    

    if (!run_dumpspecs(dumpspecs, source.header)) {
	if(!flags->amidxtaped) {
            g_fprintf(prompt_out, "%s: %d: skipping ",
		    get_pname(), file_num);
            print_header(prompt_out, source.header);
	}
	qdisk = quote_string(source.header->disk);
	dbprintf("Skipping file %d: date %s host %s disk %s lev %d part %d/%d\n",
		 file_num, source.header->datestamp, source.header->name,
		 qdisk, source.header->dumplevel, source.header->partnum,
		 source.header->totalparts);
	amfree(qdisk);
        return RESTORE_STATUS_NEXT_FILE;
    }

    if (first_restored_file != NULL &&
        first_restored_file->type != F_UNKNOWN &&
	first_restored_file->type != F_EMPTY &&
        !headers_equal(first_restored_file, source.header, 1) &&
        (flags->pipe_to_fd != -1)) {
        return RESTORE_STATUS_STOP;
    }

    if (!flags->amidxtaped) {
	g_fprintf(stderr, "%s: %d: restoring ",
		get_pname(), file_num);
	print_header(stderr, source.header);
    }
    qdisk = quote_string(source.header->disk);
    dbprintf("Restoring file %d: date %s host %s disk %s lev %d part %d/%d\n",
	     file_num, source.header->datestamp, source.header->name,
	     qdisk, source.header->dumplevel, source.header->partnum,
	     source.header->totalparts);
    amfree(qdisk);

    record_seen_dump(tape_seen, source.header);
    restore(&source, flags);
    if (first_restored_file) {
	memcpy(first_restored_file, source.header, sizeof(dumpfile_t));
    }
    return RESTORE_STATUS_NEXT_FILE;
}

/* This function handles processing of a particular tape or holding
   disk file. It returns TRUE if it is useful to load another tape.*/

gboolean
search_a_tape(Device      * device,
              FILE         *prompt_out, /* Where to send any prompts */
              rst_flags_t  *flags,      /* Restore options. */
              am_feature_t *their_features, 
              tapelist_t   *desired_tape, /* A list of desired tape files */
              GSList *dumpspecs, /* What disks to restore. */
              seentapes_t **tape_seen,  /* Where to record data on
                                           this tape. */
              /* May be NULL. If zeroed, will be filled in with the
                 first restored file. If already filled in, then we
                 may only restore other files from the same dump. */
              dumpfile_t   * first_restored_file,
              int           tape_count,
              FILE * logstream) {
    seentapes_t * tape_seen_head = NULL;
    RestoreSource source;
    off_t       filenum;

    int         tapefile_idx = -1;
    int         i;
    RestoreFileStatus restore_status = RESTORE_STATUS_NEXT_TAPE;

    /* if we're doing an inventory (logstream != NULL), then we need
     * somewhere to keep track of our seen tapes */
    g_assert(tape_seen != NULL || logstream == NULL);

    source.restore_mode = DEVICE_MODE;
    source.u.device = device;

    filenum = (off_t)0;
    if(desired_tape && desired_tape->numfiles > 0)
	tapefile_idx = 0;

    if (desired_tape) {
	dbprintf(_("search_a_tape: desired_tape=%p label=%s\n"),
		  desired_tape, desired_tape->label);
	dbprintf(_("tape:   numfiles = %d\n"), desired_tape->numfiles);
	for (i=0; i < desired_tape->numfiles; i++) {
	    dbprintf(_("tape:   files[%d] = %lld\n"),
		      i, (long long)desired_tape->files[i]);
	}
    } else {
	dbprintf(_("search_a_tape: no desired_tape\n"));
    }
    dbprintf(_("current tapefile_idx = %d\n"), tapefile_idx);

    if (tape_seen) {
        if (check_volume_seen(*tape_seen, device->volume_label)) {
            send_message(prompt_out, flags, their_features,
                         "Skipping repeat tape %s in slot %s",
                         device->volume_label, curslot);
            return TRUE;
        }
        record_seen_volume(tape_seen, device->volume_label, curslot);
        tape_seen_head = *tape_seen;
    }
	
    if (desired_tape && desired_tape->numfiles > 0) {
        /* Iterate the tape list, handle each file in order. */
        int file_index;
        for (file_index = 0; file_index < desired_tape->numfiles;
             file_index ++) {
            int file_num = desired_tape->files[file_index];
            restore_status = try_restore_single_file(device, file_num, NULL,
                                                     prompt_out, flags,
                                                     their_features,
                                                     first_restored_file,
                                                     NULL, tape_seen_head);
            if (restore_status != RESTORE_STATUS_NEXT_FILE)
                break;
        }
    } else if(flags->fsf && flags->amidxtaped) {
        /* Restore a single file, then quit. */
        restore_status =
            try_restore_single_file(device, flags->fsf, NULL, prompt_out, flags,
                                    their_features, first_restored_file,
                                    dumpspecs, tape_seen_head);
    } else {
        /* Search the tape from beginning to end. */
        int file_num;

        if (flags->fsf > 0) {
            file_num = flags->fsf;
        } else {
            file_num = 1;
        }

	if (!flags->amidxtaped) {
            g_fprintf(prompt_out, "Restoring from tape %s starting with file %d.\n",
		    device->volume_label, file_num);
	    fflush(prompt_out);
	}

        for (;;) {
            restore_status =
                try_restore_single_file(device, file_num, &file_num,
                                        prompt_out, flags,
                                        their_features, first_restored_file,
                                        dumpspecs, tape_seen_head);
            if (restore_status != RESTORE_STATUS_NEXT_FILE)
                break;
        }
    }
    
    /* spit out our accumulated list of dumps, if we're inventorying */
    if (logstream != NULL) {
        print_tape_inventory(logstream, tape_seen_head, device->volume_time,
                             device->volume_label, tape_count);
    }
    return (restore_status != RESTORE_STATUS_STOP);
}

static void free_seen_tapes(seentapes_t * seentapes) {
    while (seentapes != NULL) {
	seentapes_t *tape_seen = seentapes;
	seentapes = seentapes->next;
	while(tape_seen->files != NULL) {
	    dumplist_t *temp_dump = tape_seen->files;
	    tape_seen->files = temp_dump->next;
	    amfree(temp_dump->file);
	    amfree(temp_dump);
	}
	amfree(tape_seen->label);
	amfree(tape_seen->slotstr);
	amfree(tape_seen);
	
    }
}

/* Spit out a list of expected tapes, so people with manual changers know
   what to load */
static void print_expected_tape_list(FILE* prompt_out, FILE* prompt_in,
                                     tapelist_t *tapelist,
                                     rst_flags_t * flags) {
    tapelist_t * cur_volume;

    g_fprintf(prompt_out, "The following tapes are needed:");
    for(cur_volume = tapelist; cur_volume != NULL;
        cur_volume = cur_volume->next){
	g_fprintf(prompt_out, " %s", cur_volume->label);
    }
    g_fprintf(prompt_out, "\n");
    fflush(prompt_out);
    if(flags->wait_tape_prompt){
	char *input = NULL;
        g_fprintf(prompt_out,"Press enter when ready\n");
	fflush(prompt_out);
        input = agets(prompt_in);
 	amfree(input);
	g_fprintf(prompt_out, "\n");
	fflush(prompt_out);
    }
}

/* Restore a single holding-disk file. We will fill in this_header
   with the header from this restore (if it is not null), and in the
   stdout-pipe case, we abort according to last_header. Returns TRUE
   if the restore should continue, FALSE if we are done. */
gboolean restore_holding_disk(FILE * prompt_out,
                              rst_flags_t * flags,
                              am_feature_t * features,
                              tapelist_t * file,
                              seentapes_t ** seen,
                              GSList * dumpspecs,
                              dumpfile_t * this_header,
                              dumpfile_t * last_header) {
    RestoreSource source;
    gboolean read_result;
    dumpfile_t header;
    char *qdisk;

    source.header = &header;
    source.restore_mode = HOLDING_MODE;
    source.u.holding_fd = robust_open(file->label, 0, 0);
    if (source.u.holding_fd < 0) {
        send_message(prompt_out, flags, features, 
                     "could not open %s: %s",
                     file->label, strerror(errno));
        return TRUE;
    }

    g_fprintf(stderr, "Reading %s from fd %d\n",
            file->label, source.u.holding_fd);
    
    read_result = read_holding_disk_header(source.header,
                                           source.u.holding_fd, flags);
    if (!read_result) {
        send_message(prompt_out, flags, features, 
                     "Invalid header reading %s.",
                     file->label);
        aclose(source.u.holding_fd);
        return TRUE;
    }

    if (!run_dumpspecs(dumpspecs, source.header)) {
        return FALSE;
    }

    if (last_header != NULL && !flags->amidxtaped &&
	flags->pipe_to_fd == STDOUT_FILENO &&
	last_header->type != F_UNKNOWN &&
        !headers_equal(last_header, source.header, 1)) {
        return FALSE;
    } else if (this_header != NULL) {
        memcpy(this_header, source.header, sizeof(*this_header));
    }

    if (seen != NULL) {
        record_seen_volume(seen, file->label, "<none>");
        record_seen_dump(*seen, source.header);
    }

    print_header(stderr, source.header);

    qdisk = quote_string(source.header->disk);
    dbprintf("Restoring from holding disk %s: date %s host %s disk %s lev %d part %d/%d\n",
	     file->label,
	     source.header->datestamp, source.header->name,
	     qdisk, source.header->dumplevel, source.header->partnum,
	     source.header->totalparts);
    amfree(qdisk);
    restore(&source, flags);
    aclose(source.u.holding_fd);
    return TRUE;
}

/* Ask for a specific manual tape. If we find the right one, then open it
 * and return a Device handle. If not, return NULL. Pass a device name, but
 * it might be overridden. */
static Device* manual_find_tape(char ** cur_tapedev, tapelist_t * cur_volume,
                                FILE * prompt_out, FILE * prompt_in,
                                rst_flags_t * flags,
                                am_feature_t * features) {
    LoadStatus status = LOAD_NEXT;
    Device * rval;

    /* if we don't have a tapedev, it's probably because the changer failed.
     * We'll muddle along as well as possible.  Note that if the user supplied
     * a device (e.g., amfetchdump -d), then this won't be NULL. */
    if (*cur_tapedev == NULL) {
	*cur_tapedev = stralloc(getconf_str(CNF_TAPEDEV));
    }

    for (;;) {
        status = load_manual_tape(cur_tapedev, prompt_out, prompt_in,
                                  flags, features, cur_volume);
        
        if (status == LOAD_STOP)
            return NULL;

        rval =  conditional_device_open(*cur_tapedev, prompt_out, flags,
                                        features, cur_volume);
        if (rval != NULL)
            return rval;
    }
}

/* If we have a tapelist, then we mandate restoring in tapelist
   order. The logic is simple: Get the next tape, and deal with it,
   then move on to the next one. */
static void
restore_from_tapelist(FILE * prompt_out,
                      FILE * prompt_in,
                      tapelist_t * tapelist,
                      GSList * dumpspecs,
                      rst_flags_t * flags,
                      am_feature_t * features,
                      char * cur_tapedev,
                      gboolean use_changer,
                      FILE * logstream) {
    tapelist_t * cur_volume;
    dumpfile_t first_restored_file;
    seentapes_t * seentapes = NULL;

    fh_init(&first_restored_file);

    for(cur_volume = tapelist; cur_volume != NULL;
        cur_volume = cur_volume->next){
        if (cur_volume->isafile) {
            /* Restore from holding disk; just go. */
            if (first_restored_file.type == F_UNKNOWN) {
                if (!restore_holding_disk(prompt_out, flags,
                                          features, cur_volume, &seentapes,
                                          NULL, NULL, &first_restored_file)) {
                    break;
                }
            } else {
                restore_holding_disk(prompt_out, flags, features,
                                     cur_volume, &seentapes,
                                     NULL, &first_restored_file, NULL);
            }
	    if (flags->pipe_to_fd != -1) {
		break;
	    }
        } else {
            Device * device = NULL;
            if (use_changer) {
                char * tapedev = NULL;
                loadlabel_data data;
                data.cur_tapedev = &tapedev;
                data.searchlabel =  cur_volume->label;
		data.flags = flags;
                changer_find(&data, scan_init, loadlabel_slot,
                             cur_volume->label);
                device = conditional_device_open(tapedev, prompt_out,
                                                 flags, features,
                                                 cur_volume);
		amfree(tapedev);
            }

            if (device == NULL)
                device = manual_find_tape(&cur_tapedev, cur_volume, prompt_out,
                                          prompt_in, flags, features);

            if (device == NULL)
                break;

            if (use_changer) {
                g_fprintf(stderr, "Scanning volume %s (slot %s)\n",
                          device->volume_label,
                          curslot);
            } else {
                g_fprintf(stderr, "Scanning volume %s\n",
                          device->volume_label);
            }

            if (!search_a_tape(device, prompt_out, flags, features,
                               cur_volume, dumpspecs, &seentapes,
                               &first_restored_file, 0, logstream)) {
                g_object_unref(device);
                break;
            }
            g_object_unref(device);
        }            
    }

    free_seen_tapes(seentapes);
}

/* This function works when we are operating without a tapelist
   (regardless of whether or not we have a changer). This only happens
   when we are using amfetchdump without dump logs, but in the future
   may include amrestore as well. The philosophy is to keep loading
   tapes until we run out. */
static void
restore_without_tapelist(FILE * prompt_out,
                         FILE * prompt_in,
                         GSList * dumpspecs,
                         rst_flags_t * flags,
                         am_feature_t * features,
                         char * cur_tapedev,
                         /* -1 if no changer. */
                         int slot_count,
                         FILE * logstream) {
    int cur_slot = 1;
    seentapes_t * seentapes;
    int tape_count = 0;
    dumpfile_t first_restored_file;

    fh_init(&first_restored_file);

    /* This loop also aborts if we run out of manual tapes, or
       encounter a changer error. */
    for (;;) {
        Device * device = NULL;
        if (slot_count > 0) {
            while (cur_slot < slot_count && device == NULL) {
                amfree(curslot);
                changer_loadslot("next", &curslot, &cur_tapedev);
                device = conditional_device_open(cur_tapedev, prompt_out,
                                                 flags, features,
                                                 NULL);
		amfree(cur_tapedev);
                cur_slot ++;
            }
            if (cur_slot >= slot_count)
                break;
        } else {
            device = manual_find_tape(&cur_tapedev, NULL, prompt_out,
                                      prompt_in, flags, features);
        }
        
        if (device == NULL)
            break;

	g_fprintf(stderr, "Scanning %s (slot %s)\n", device->volume_label,
		curslot);
        
        if (!search_a_tape(device, prompt_out, flags, features,
                           NULL, dumpspecs, &seentapes, &first_restored_file,
                           tape_count, logstream)) {
            g_object_unref(device);
            break;
        }
        g_object_unref(device);
        tape_count ++;
    }
           
    free_seen_tapes(seentapes);
}

/* 
 * Take a pattern of dumps and restore it blind, a la amrestore.  In addition,
 * be smart enough to change tapes and continue with minimal operator
 * intervention, and write out a record of what was found on tapes in the
 * the regular logging format.  Can take a tapelist with a specific set of
 * tapes to search (rather than "everything I can find"), which in turn can
 * optionally list specific files to restore.
 */
 void
search_tapes(
    FILE *		prompt_out,
    FILE *              prompt_in,
    int			use_changer,
    tapelist_t *	tapelist,
    GSList *		dumpspecs,
    rst_flags_t *	flags,
    am_feature_t *	their_features)
{
    char *cur_tapedev;
    int slots = -1;
    FILE *logstream = NULL;
    tapelist_t *desired_tape = NULL;
    struct sigaction act, oact;

    device_api_init();

    if(!prompt_out) prompt_out = stderr;

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /* catch SIGINT with something that'll flush unmerged splits */
    act.sa_handler = handle_sigint;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if(sigaction(SIGINT, &act, &oact) != 0){
	error(_("error setting SIGINT handler: %s"), strerror(errno));
	/*NOTREACHED*/
    }
    if(flags->delay_assemble || flags->inline_assemble) exitassemble = 1;
    else exitassemble = 0;

    /* if given a log file, print an inventory of stuff found */
    if(flags->inventory_log) {
	if(!strcmp(flags->inventory_log, "-")) logstream = stdout;
	else if((logstream = fopen(flags->inventory_log, "w+")) == NULL) {
	    error(_("Couldn't open log file %s for writing: %s"),
		  flags->inventory_log, strerror(errno));
	    /*NOTREACHED*/
	}
    }

    /* Suss what tape device we're using, whether there's a changer, etc. */
    if (use_changer) {
	use_changer = changer_init();
    }
    if (!use_changer) {
        cur_tapedev = NULL;
	if (flags->alt_tapedev) {
	    cur_tapedev = stralloc(flags->alt_tapedev);
	} else if(!cur_tapedev) {
	    cur_tapedev = getconf_str(CNF_TAPEDEV);
	    if (cur_tapedev == NULL) {
		error(_("No tapedev specified"));
	    }
	}
	/* XXX oughta complain if no config is loaded */
	g_fprintf(stderr, _("%s: Using tapedev %s\n"), get_pname(), cur_tapedev);
    }
    else{ /* good, the changer works, see what it can do */
	amfree(curslot);
	changer_info(&slots, &curslot, &backwards);
    }

    if (tapelist && !flags->amidxtaped) {
        print_expected_tape_list(prompt_out, prompt_in, tapelist, flags);
    }
    desired_tape = tapelist;

    if (use_changer) { /* load current slot */
	amfree(curslot);
	cur_tapedev = NULL;
	changer_loadslot("current", &curslot, &cur_tapedev);
    }

    /*
     * If we're not given a tapelist, iterate over everything our changer can
     * find.  If there's no changer, we'll prompt to be handfed tapes.
     *
     * If we *are* given a tapelist, restore from those tapes in the order in
     * which they're listed.  Unless the changer (if we have one) can't go
     * backwards, in which case check every tape we see and restore from it if
     * appropriate.
     *
     * (obnoxious, isn't this?)
     */

    if (tapelist) {
        restore_from_tapelist(prompt_out, prompt_in, tapelist, dumpspecs,
                              flags, their_features, cur_tapedev, use_changer,
                              logstream);
    } else {
        restore_without_tapelist(prompt_out, prompt_in, dumpspecs, flags,
                                 their_features, cur_tapedev,
                                 (use_changer ? slots : -1),
                                 logstream);
    }

    if(logstream && logstream != stderr && logstream != stdout){
	fclose(logstream);
    }
    if(flags->delay_assemble || flags->inline_assemble){
	flush_open_outputs(1, NULL);
    }
    else flush_open_outputs(0, NULL);
}

/*
 * Create a new, clean set of restore flags with some sane default values.
 */
rst_flags_t *
new_rst_flags(void)
{
    rst_flags_t *flags = alloc(SIZEOF(rst_flags_t));

    memset(flags, 0, SIZEOF(rst_flags_t));

    flags->fsf = 0;
    flags->comp_type = COMPRESS_FAST_OPT;
    flags->inline_assemble = 1;
    flags->header_to_fd = -1;
    flags->pipe_to_fd = -1;
    flags->check_labels = 1;

    return(flags);
}

/*
 * Make sure the set of restore options given is sane.  Print errors for
 * things that're odd, and return -1 for fatal errors.
 */
int
check_rst_flags(
    rst_flags_t *	flags)
{
    int ret = 0;	
    
    if(!flags) return(-1);

    if(flags->compress && flags->leave_comp){
	g_fprintf(stderr, _("Cannot specify 'compress output' and 'leave compression alone' together\n"));
	ret = -1;
    }

    if(flags->restore_dir != NULL){
	struct stat statinfo;

	if(flags->pipe_to_fd != -1){
	    g_fprintf(stderr, _("Specifying output directory and piping output are mutually exclusive\n"));
	    ret = -1;
	}
	if(stat(flags->restore_dir, &statinfo) < 0){
	    g_fprintf(stderr, _("Cannot stat restore target dir '%s': %s\n"),
		      flags->restore_dir, strerror(errno));
	    ret = -1;
	}
	if((statinfo.st_mode & S_IFMT) != S_IFDIR){
	    g_fprintf(stderr, _("'%s' is not a directory\n"), flags->restore_dir);
	    ret = -1;
	}
    }

    if((flags->pipe_to_fd != -1 || flags->compress) &&
	    (flags->delay_assemble || !flags->inline_assemble)){
	g_fprintf(stderr, _("Split dumps *must* be automatically reassembled when piping output or compressing/uncompressing\n"));
	ret = -1;
    }

    if(flags->delay_assemble && flags->inline_assemble){
	g_fprintf(stderr, _("Inline split assembling and delayed assembling are mutually exclusive\n"));
	ret = -1;
    }

    return(ret);
}

/*
 * Clean up after a rst_flags_t
 */
void
free_rst_flags(
    rst_flags_t *	flags)
{
    if(!flags) return;

    amfree(flags->restore_dir);
    amfree(flags->alt_tapedev);
    amfree(flags->inventory_log);

    amfree(flags);
}


printf_arglist_function3(
    void send_message,
    FILE *, prompt_out,
    rst_flags_t *, flags,
    am_feature_t *, their_features,
    char *, format)
{
    va_list argp;
    char linebuf[STR_SIZE];

    arglist_start(argp, format);
    g_vsnprintf(linebuf, SIZEOF(linebuf)-1, format, argp);
    arglist_end(argp);

    dbprintf("%s\n", linebuf);
    g_fprintf(stderr,"%s\n", linebuf);
    if (flags->amidxtaped && their_features &&
	am_has_feature(their_features, fe_amrecover_message)) {
	g_fprintf(prompt_out, "MESSAGE %s\r\n", linebuf);
	fflush(prompt_out);
    }
}

