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
 * $Id: restore.c,v 1.49 2006/07/22 12:04:48 martinea Exp $
 *
 * retrieves files from an amanda tape
 */

#include "amanda.h"
#include "tapeio.h"
#include "util.h"
#include "restore.h"
#include "find.h"
#include "changer.h"
#include "logfile.h"
#include "fileheader.h"
#include "arglist.h"
#include <signal.h>

#define LOAD_STOP    -1
#define LOAD_CHANGER -2

int file_number;

/* stuff we're stuck having global */
static size_t blocksize = (size_t)SSIZE_MAX;
static char *cur_tapedev = NULL;
static char *searchlabel = NULL;
static int backwards;
static int exitassemble = 0;
static int tapefd;

char *rst_conf_logdir = NULL;
char *rst_conf_logfile = NULL;
char *curslot = NULL;

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

typedef struct seentapes_s {
    struct seentapes_s *next;
    char *slotstr;
    char *label;
    dumplist_t *files;
} seentapes_t;

static open_output_t *open_outputs = NULL;
static dumplist_t *alldumps_list = NULL;

/* local functions */

static ssize_t get_block(int tapefd, char *buffer, int isafile);
static void append_file_to_fd(char *filename, int fd);
static int headers_equal(dumpfile_t *file1, dumpfile_t *file2, int ignore_partnums);
static int already_have_dump(dumpfile_t *file);
static void handle_sigint(int sig);
static int scan_init(void *ud, int rc, int ns, int bk, int s);
int loadlabel_slot(void *ud, int rc, char *slotstr, char *device);
void drain_file(int tapefd, rst_flags_t *flags);
char *label_of_current_slot(char *cur_tapedev, FILE *prompt_out,
			    int *tapefd, dumpfile_t *file, rst_flags_t *flags,
			    am_feature_t *their_features,
			    ssize_t *read_result, tapelist_t *desired_tape);

int load_next_tape(char **cur_tapedev, FILE *prompt_out, int backwards,
		   rst_flags_t *flags, am_feature_t *their_features,
		   tapelist_t *desired_tape);
int load_manual_tape(char **cur_tapedev, FILE *prompt_out,
		     rst_flags_t *flags, am_feature_t *their_features,
		     tapelist_t *desired_tape);
void search_a_tape(char *cur_tapedev, FILE *prompt_out, rst_flags_t *flags,
		   am_feature_t *their_features, tapelist_t *desired_tape,
		   int isafile, match_list_t *match_list,
		   seentapes_t *tape_seen, dumpfile_t *file,
		   dumpfile_t *prev_rst_file, dumpfile_t *tapestart,
		   int slot_num, ssize_t *read_result);

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
    if(rst_conf_logfile) unlink(rst_conf_logfile);
    exit(0);
}

int
lock_logfile(void)
{
    rst_conf_logdir = getconf_str(CNF_LOGDIR);
    if (*rst_conf_logdir == '/') {
	rst_conf_logdir = stralloc(rst_conf_logdir);
    } else {
	rst_conf_logdir = stralloc2(config_dir, rst_conf_logdir);
    }
    rst_conf_logfile = vstralloc(rst_conf_logdir, "/log", NULL);
    if (access(rst_conf_logfile, F_OK) == 0) {
	dbprintf(("%s exists: amdump or amflush is already running, "
		  "or you must run amcleanup\n", rst_conf_logfile));
	return 0;
    }
    log_add(L_INFO, get_pname());
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
    int		fd)
{
    ssize_t bytes_read;
    ssize_t s;
    off_t wc = (off_t)0;
    char *buffer;

    if(blocksize == SIZE_MAX)
	blocksize = DISK_BLOCK_BYTES;
    buffer = alloc(blocksize);

    if((tapefd = open(filename, O_RDONLY)) == -1) {
	error("can't open %s: %s", filename, strerror(errno));
	/*NOTREACHED*/
    }

    for (;;) {
	bytes_read = get_block(tapefd, buffer, 1); /* same as isafile = 1 */
	if(bytes_read < 0) {
	    error("read error: %s", strerror(errno));
	    /*NOTREACHED*/
	}

	if (bytes_read == 0)
		break;

	s = fullwrite(fd, buffer, (size_t)bytes_read);
	if (s < bytes_read) {
	    fprintf(stderr,"Error (%s) offset " OFF_T_FMT "+" OFF_T_FMT ", wrote " OFF_T_FMT "\n",
		    strerror(errno), (OFF_T_FMT_TYPE)wc,
		    (OFF_T_FMT_TYPE)bytes_read, (OFF_T_FMT_TYPE)s);
	    if (s < 0) {
		if((errno == EPIPE) || (errno == ECONNRESET)) {
		    error("%s: pipe reader has quit in middle of file.",
			get_pname());
		    /*NOTREACHED*/
		}
		error("restore: write error = %s", strerror(errno));
		/*NOTREACHED*/
	    }
	    error("Short write: wrote %d bytes expected %d.", s, bytes_read);
	    /*NOTREACHCED*/
	}
	wc += (off_t)bytes_read;
    }

    amfree(buffer);
    aclose(tapefd);
}

/*
 * Tape changer support routines, stolen brazenly from amtape
 */
static int 
scan_init(
     void *	ud,
     int	rc,
     int	ns,
     int	bk,
     int	s)
{
    (void)ud;	/* Quiet unused parameter warning */
    (void)ns;	/* Quiet unused parameter warning */
    (void)s;	/* Quiet unused parameter warning */

    if(rc) {
        error("could not get changer info: %s", changer_resultstr);
	/*NOTREACHED*/
    }
    backwards = bk;

    return 0;
}

int
loadlabel_slot(
     void *	ud,
     int	rc,
     char *	slotstr,
     char *	device)
{
    char *errstr;
    char *datestamp = NULL;
    char *label = NULL;

    (void)ud;	/* Quiet unused parameter warning */

    if(rc > 1) {
        error("could not load slot %s: %s", slotstr, changer_resultstr);
	/*NOTREACHED*/
    } else if(rc == 1) {
        fprintf(stderr, "%s: slot %s: %s\n",
                get_pname(), slotstr, changer_resultstr);
    } else if((errstr = tape_rdlabel(device, &datestamp, &label)) != NULL) {
        fprintf(stderr, "%s: slot %s: %s\n", get_pname(), slotstr, errstr);
    } else {
	if(strlen(datestamp)>8)
            fprintf(stderr, "%s: slot %s: date %-14s label %s",
		    get_pname(), slotstr, datestamp, label);
	else
            fprintf(stderr, "%s: slot %s: date %-8s label %s",
		    get_pname(), slotstr, datestamp, label);
        if(strcmp(label, FAKE_LABEL) != 0
           && strcmp(label, searchlabel) != 0)
            fprintf(stderr, " (wrong tape)\n");
        else {
            fprintf(stderr, " (exact label match)\n");
            if((errstr = tape_rewind(device)) != NULL) {
                fprintf(stderr,
                        "%s: could not rewind %s: %s",
                        get_pname(), device, errstr);
                amfree(errstr);
            }
	    amfree(cur_tapedev);
	    curslot = newstralloc(curslot, slotstr);
            amfree(datestamp);
            amfree(label);
	    if(device)
		cur_tapedev = stralloc(device);
            return 1;
        }
    }
    amfree(datestamp);
    amfree(label);

    amfree(cur_tapedev);
    curslot = newstralloc(curslot, slotstr);
    if(!device) return(1);
    cur_tapedev = stralloc(device);

    return 0;
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
	fprintf(stderr, "\n");
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
		snprintf(part_str, SIZEOF(part_str), "%d", cur_file->partnum);
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
		    fprintf(stderr, "Merging %s with %s\n",
		            cur_filename, main_filename);
		    append_file_to_fd(cur_filename, outfd);
		    if(unlink(cur_filename) < 0){
			fprintf(stderr, "Failed to unlink %s: %s\n",
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
			  error("Couldn't open %s for appending: %s",
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

    snprintf(number, SIZEOF(number), "%d", file->dumplevel);
    snprintf(part, SIZEOF(part), "%d", file->partnum);

    if(file->totalparts < 0) {
	snprintf(totalparts, SIZEOF(totalparts), "UNKNOWN");
    }
    else {
	snprintf(totalparts, SIZEOF(totalparts), "%d", file->totalparts);
    }
    padlen = strlen(totalparts) + 1 - strlen(part);
    pad = alloc(padlen);
    memset(pad, '0', padlen);
    pad[padlen - 1] = '\0';

    snprintf(part, SIZEOF(part), "%s%d", pad, file->partnum);

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


/*
 * XXX Making this thing a lib functiong broke a lot of assumptions everywhere,
 * but I think I've found them all.  Maybe.  Damn globals all over the place.
 */

static ssize_t
get_block(
    int		tapefd,
    char *	buffer,
    int 	isafile)
{
    if(isafile)
	return (fullread(tapefd, buffer, blocksize));

    return(tapefd_read(tapefd, buffer, blocksize));
}

/*
 * Returns 1 if the current dump file matches the hostname and diskname
 * regular expressions given on the command line, 0 otherwise.  As a 
 * special case, empty regexs are considered equivalent to ".*": they 
 * match everything.
 */

int
disk_match(
    dumpfile_t *file,
    char *	datestamp,
    char *	hostname,
    char *	diskname,
    char *	level)
{
    char level_str[NUM_STR_SIZE];
    snprintf(level_str, SIZEOF(level_str), "%d", file->dumplevel);

    if(file->type != F_DUMPFILE && file->type != F_SPLIT_DUMPFILE) return 0;

    if((*hostname == '\0' || match_host(hostname, file->name)) &&
       (*diskname == '\0' || match_disk(diskname, file->disk)) &&
       (*datestamp == '\0' || match_datestamp(datestamp, file->datestamp)) &&
       (*level == '\0' || match_level(level, level_str)))
	return 1;
    else
	return 0;
}


/*
 * Reads the first block of a tape file.
 */

ssize_t
read_file_header(
    dumpfile_t *	file,
    int			tapefd,
    int			isafile,
    rst_flags_t *	flags)
{
    ssize_t bytes_read;
    char *buffer;
  
    if(flags->blocksize > 0)
	blocksize = (size_t)flags->blocksize;
    else if(blocksize == (size_t)SSIZE_MAX)
	blocksize = DISK_BLOCK_BYTES;
    buffer = alloc(blocksize);

    bytes_read = get_block(tapefd, buffer, isafile);
    if(bytes_read < 0) {
	fprintf(stderr, "%s: error reading file header: %s",
		get_pname(), strerror(errno));
	file->type = F_UNKNOWN;
    } else if((size_t)bytes_read < blocksize) {
	if(bytes_read == 0) {
	    fprintf(stderr, "%s: missing file header block\n", get_pname());
	} else {
	    fprintf(stderr, "%s: short file header block: " OFF_T_FMT " byte%s\n",
		    get_pname(), (OFF_T_FMT_TYPE)bytes_read, (bytes_read == 1) ? "" : "s");
	}
	file->type = F_UNKNOWN;
    } else {
	parse_file_header(buffer, file, (size_t)bytes_read);
    }
    amfree(buffer);
    return bytes_read;
}


void
drain_file(
    int			tapefd,
    rst_flags_t *	flags)
{
    ssize_t bytes_read;
    char *buffer;

    if(flags->blocksize)
	blocksize = (size_t)flags->blocksize;
    else if(blocksize == (size_t)SSIZE_MAX)
	blocksize = DISK_BLOCK_BYTES;
    buffer = alloc(blocksize);

    do {
       bytes_read = get_block(tapefd, buffer, 0);
       if(bytes_read < 0) {
           error("drain read error: %s", strerror(errno));
	   /*NOTREACHED*/
       }
    } while (bytes_read > 0);

    amfree(buffer);
}

/*
 * Restore the current file from tape.  Depending on the settings of
 * the command line flags, the file might need to be compressed or
 * uncompressed.  If so, a pipe through compress or uncompress is set
 * up.  The final output usually goes to a file named host.disk.date.lev,
 * but with the -p flag the output goes to stdout (and presumably is
 * piped to restore).
 */

ssize_t
restore(
    dumpfile_t *	file,
    char *		filename,
    int			tapefd,
    int			isafile,
    rst_flags_t *	flags)
{
    int dest = -1, out;
    ssize_t s;
    int file_is_compressed;
    int is_continuation = 0;
    int check_for_aborted = 0;
    char *tmp_filename = NULL, *final_filename = NULL;
    struct stat statinfo;
    open_output_t *myout = NULL, *oldout = NULL;
    dumplist_t *tempdump = NULL, *fileentry = NULL;
    char *buffer;
    int need_compress=0, need_uncompress=0, need_decrypt=0;
    int stage=0;
    ssize_t bytes_read;
    struct pipeline {
        int	pipe[2];
    } pipes[3];

    memset(pipes, -1, SIZEOF(pipes));
    if(flags->blocksize)
	blocksize = (size_t)flags->blocksize;
    else if(blocksize == (size_t)SSIZE_MAX)
	blocksize = DISK_BLOCK_BYTES;

    if(already_have_dump(file)){
	char *filename = make_filename(file);
	fprintf(stderr, " *** Duplicate file %s, one is probably an aborted write\n", filename);
	amfree(filename);
	check_for_aborted = 1;
    }

    /* store a shorthand record of this dump */
    tempdump = alloc(SIZEOF(dumplist_t));
    tempdump->file = alloc(SIZEOF(dumpfile_t));
    tempdump->next = NULL;
    memcpy(tempdump->file, file, SIZEOF(dumpfile_t));

    /*
     * If we're appending chunked files to one another, and if this is a
     * continuation of a file we just restored, and we've still got the
     * output handle from that previous restore, we're golden.  Phew.
     */
    if(flags->inline_assemble && file->type == F_SPLIT_DUMPFILE){
	myout = open_outputs;
	while(myout != NULL){
	    if(myout->file->type == F_SPLIT_DUMPFILE &&
		    headers_equal(file, myout->file, 1)){
		if(file->partnum == myout->lastpartnum + 1){
		    is_continuation = 1;
		    break;
		}
	    }
	    myout = myout->next;
	}
	if(myout != NULL) myout->lastpartnum = file->partnum;
	else if(file->partnum != 1){
	    fprintf(stderr, "%s:      Chunk out of order, will save to disk and append to output.\n", get_pname());
	    flags->pipe_to_fd = -1;
	    flags->compress = 0;
	    flags->leave_comp = 1;
	}
	if(myout == NULL){
	    myout = alloc(SIZEOF(open_output_t));
	    memset(myout, 0, SIZEOF(open_output_t));
	}
    }
    else{
      myout = alloc(SIZEOF(open_output_t));
      memset(myout, 0, SIZEOF(open_output_t));
    }


    if(is_continuation && flags->pipe_to_fd == -1){
	char *filename;
	filename = make_filename(myout->file);
	fprintf(stderr, "%s:      appending to %s\n", get_pname(),
		filename);
	amfree(filename);
    }

    /* adjust compression flag */
    file_is_compressed = file->compressed;
    if(!flags->compress && file_is_compressed && !known_compress_type(file)) {
	fprintf(stderr, 
		"%s: unknown compression suffix %s, can't uncompress\n",
		get_pname(), file->comp_suffix);
	flags->compress = 1;
    }

    /* set up final destination file */

    if(is_continuation && myout != NULL) {
      out = myout->outfd;
    } else {
      if(flags->pipe_to_fd != -1) {
  	  dest = flags->pipe_to_fd;	/* standard output */
      } else {
  	  char *filename_ext = NULL;
  
  	  if(flags->compress) {
  	      filename_ext = file_is_compressed ? file->comp_suffix
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
	  final_filename = stralloc(tmp_filename); 
	  tmp_filename = newvstralloc(tmp_filename, ".tmp", NULL);
  	  if((dest = open(tmp_filename, (O_CREAT | O_RDWR | O_TRUNC),
			  CREAT_MODE)) < 0) {
  	      error("could not create output file %s: %s",
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
	ssize_t w;
	char *cont_filename;
	dumpfile_t tmp_hdr;

	if(flags->compress && !file_is_compressed) {
	    file->compressed = 1;
	    snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
		        " %s %s |", UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
		        UNCOMPRESS_OPT
#else
		        ""
#endif
		        );
	    strncpy(file->comp_suffix,
		    COMPRESS_SUFFIX,
		    SIZEOF(file->comp_suffix)-1);
	    file->comp_suffix[SIZEOF(file->comp_suffix)-1] = '\0';
	}

	memcpy(&tmp_hdr, file, SIZEOF(dumpfile_t));

	/* remove CONT_FILENAME from header */
	cont_filename = stralloc(file->cont_filename);
	memset(file->cont_filename,'\0',SIZEOF(file->cont_filename));
	file->blocksize = DISK_BLOCK_BYTES;

	/*
	 * Dumb down split file headers as well, so that older versions of
	 * things like amrecover won't gag on them.
	 */
	if(file->type == F_SPLIT_DUMPFILE && flags->mask_splits){
	    file->type = F_DUMPFILE;
	}

	buffer = alloc(DISK_BLOCK_BYTES);
	build_header(buffer, file, DISK_BLOCK_BYTES);

	if((w = fullwrite(out, buffer, DISK_BLOCK_BYTES)) != DISK_BLOCK_BYTES) {
	    if(w < 0) {
		error("write error: %s", strerror(errno));
		/*NOTREACHED*/
	    } else {
		error("write error: %d instead of %d", w, DISK_BLOCK_BYTES);
		/*NOTREACHED*/
	    }
	}
	amfree(buffer);
	/* add CONT_FILENAME to header */
#if 0
//	strncpy(file->cont_filename, cont_filename, SIZEOF(file->cont_filename));
#endif
	amfree(cont_filename);
	memcpy(file, &tmp_hdr, SIZEOF(dumpfile_t));
    }
 
    /* find out if compression or uncompression is needed here */
    if(flags->compress && !file_is_compressed && !is_continuation
	  && !flags->leave_comp
	  && (flags->inline_assemble || file->type != F_SPLIT_DUMPFILE))
       need_compress=1;
       
    if(!flags->raw && !flags->compress && file_is_compressed
	  && !is_continuation && !flags->leave_comp && (flags->inline_assemble
	  || file->type != F_SPLIT_DUMPFILE))
       need_uncompress=1;   

    if(!flags->raw && file->encrypted)
       need_decrypt=1;
   
    /* Setup pipes for decryption / compression / uncompression  */
    stage = 0;
    if (need_decrypt) {
      if (pipe(&pipes[stage].pipe[0]) < 0) {
        error("error [pipe[%d]: %s]", stage, strerror(errno));
	/*NOTREACHED*/
      }
      stage++;
    }

    if (need_compress || need_uncompress) {
      if (pipe(&pipes[stage].pipe[0]) < 0) {
        error("error [pipe[%d]: %s]", stage, strerror(errno));
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
	error("could not fork for decrypt: %s", strerror(errno));
	/*NOTREACHED*/

      default:
	aclose(pipes[stage].pipe[0]);
	aclose(pipes[stage+1].pipe[1]);
        stage++;
	break;

      case 0:
	if(dup2(pipes[stage].pipe[0], 0) == -1) {
	    error("error decrypt stdin [dup2 %d %d: %s]", stage,
	        pipes[stage].pipe[0], strerror(errno));
		/*NOTREACHED*/
	}

	if(dup2(pipes[stage+1].pipe[1], 1) == -1) {
	    error("error decrypt stdout [dup2 %d %d: %s]", stage + 1,
	        pipes[stage+1].pipe[1], strerror(errno));
		/*NOTREACHED*/
	}

	safe_fd(-1, 0);
	if (*file->srv_encrypt) {
	  (void) execlp(file->srv_encrypt, file->srv_encrypt,
			file->srv_decrypt_opt, NULL);
	  error("could not exec %s: %s", file->srv_encrypt, strerror(errno));
	  /*NOTREACHED*/
	}  else if (*file->clnt_encrypt) {
	  (void) execlp(file->clnt_encrypt, file->clnt_encrypt,
			file->clnt_decrypt_opt, NULL);
	  error("could not exec %s: %s", file->clnt_encrypt, strerror(errno));
	  /*NOTREACHED*/
	}
      }
    }

    if (need_compress) {
        /*
         * Insert a compress pipe
         */
	switch(myout->comp_enc_pid = fork()) {
	case -1:
	    error("could not fork for %s: %s", COMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/

	default:
	    aclose(pipes[stage].pipe[0]);
	    aclose(pipes[stage+1].pipe[1]);
            stage++;
	    break;

	case 0:
	    if(dup2(pipes[stage].pipe[0], 0) == -1) {
		error("error compress stdin [dup2 %d %d: %s]", stage,
		  pipes[stage].pipe[0], strerror(errno));
	        /*NOTREACHED*/
	    }

	    if(dup2(pipes[stage+1].pipe[1], 1) == -1) {
		error("error compress stdout [dup2 %d %d: %s]", stage + 1,
		  pipes[stage+1].pipe[1], strerror(errno));
		  /*NOTREACHED*/
	    }
	    if (*flags->comp_type == '\0') {
		flags->comp_type = NULL;
	    }

	    safe_fd(-1, 0);
	    (void) execlp(COMPRESS_PATH, COMPRESS_PATH, flags->comp_type, (char *)0);
	    error("could not exec %s: %s", COMPRESS_PATH, strerror(errno));
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
	    error("could not fork for %s: %s",
		  UNCOMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/

	default:
	    aclose(pipes[stage].pipe[0]);
	    aclose(pipes[stage+1].pipe[1]);
            stage++;
	    break;

	case 0:
	    if(dup2(pipes[stage].pipe[0], 0) == -1) {
		error("error uncompress stdin [dup2 %d %d: %s]", stage,
		  pipes[stage].pipe[0], strerror(errno));
	        /*NOTREACHED*/
	    }

	    if(dup2(pipes[stage+1].pipe[1], 1) == -1) {
		error("error uncompress stdout [dup2 %d %d: %s]", stage + 1,
		  pipes[stage+1].pipe[1], strerror(errno));
	        /*NOTREACHED*/
	    }

	    safe_fd(-1, 0);
	    if (*file->srvcompprog) {
	      (void) execlp(file->srvcompprog, file->srvcompprog, "-d", NULL);
	      error("could not exec %s: %s", file->srvcompprog, strerror(errno));
	      /*NOTREACHED*/
	    } else if (*file->clntcompprog) {
	      (void) execlp(file->clntcompprog, file->clntcompprog, "-d", NULL);
	      error("could not exec %s: %s", file->clntcompprog, strerror(errno));
	      /*NOTREACHED*/
	    } else {
	      (void) execlp(UNCOMPRESS_PATH, UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
			  UNCOMPRESS_OPT,
#endif
			  (char *)0);
	      error("could not exec %s: %s", UNCOMPRESS_PATH, strerror(errno));
	      /*NOTREACHED*/
	    }
	}
    }

    /* copy the rest of the file from tape to the output */
    if(flags->blocksize > 0)
	blocksize = (size_t)flags->blocksize;
    else if(blocksize == SIZE_MAX)
	blocksize = DISK_BLOCK_BYTES;
    buffer = alloc(blocksize);

    do {
	bytes_read = get_block(tapefd, buffer, isafile);
	if(bytes_read < 0) {
	    error("restore read error: %s", strerror(errno));
	    /*NOTREACHED*/
	}

	if(bytes_read > 0) {
	    if((s = fullwrite(pipes[0].pipe[1], buffer, (size_t)bytes_read)) < 0) {
		if ((errno == EPIPE) || (errno == ECONNRESET)) {
		    /*
		     * reading program has ended early
		     * e.g: bzip2 closes pipe when it
		     * trailing garbage after EOF
		     */
		    break;
		}
		error("restore: write error: %s", strerror(errno));
		/* NOTREACHED */
	    } else if (s < bytes_read) {
		error("restore: wrote %d of %d bytes: %s",
		    s, bytes_read, strerror(errno));
		/* NOTREACHED */
	    }
	}
	else if(isafile) {
	    /*
	     * See if we need to switch to the next file in a holding restore
	     */
	    if(file->cont_filename[0] == '\0') {
		break;				/* no more files */
	    }
	    aclose(tapefd);
	    if((tapefd = open(file->cont_filename, O_RDONLY)) == -1) {
		char *cont_filename = strrchr(file->cont_filename,'/');
		if(cont_filename) {
		    cont_filename++;
		    if((tapefd = open(cont_filename,O_RDONLY)) == -1) {
			error("can't open %s: %s", file->cont_filename,
			      strerror(errno));
		        /*NOTREACHED*/
		    }
		    else {
			fprintf(stderr, "cannot open %s: %s\n",
				file->cont_filename, strerror(errno));
			fprintf(stderr, "using %s\n",
				cont_filename);
		    }
		}
		else {
		    error("can't open %s: %s", file->cont_filename,
			  strerror(errno));
		    /*NOTREACHED*/
		}
	    }
	    read_file_header(file, tapefd, isafile, flags);
	    if(file->type != F_DUMPFILE && file->type != F_CONT_DUMPFILE
		    && file->type != F_SPLIT_DUMPFILE) {
		fprintf(stderr, "unexpected header type: ");
		print_header(stderr, file);
		exit(2);
	    }
	}
    } while (bytes_read > 0);

    amfree(buffer);

    if(!flags->inline_assemble) {
        if(out != dest)
	    aclose(out);
    }
    if(!is_continuation){
	if(tmp_filename && stat(tmp_filename, &statinfo) < 0){
	    error("Can't stat the file I just created (%s)!", tmp_filename);
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
		    fprintf(stderr, "Newer restore is larger, using that\n");
		    /* nuke the old dump's entry in alldump_list */
		    for(fileentry=alldumps_list;
			    fileentry->next;
			    fileentry=fileentry->next){
			if(headers_equal(file, fileentry->file, 0)){
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
			if(headers_equal(file, myout->file, 0)){
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
		    fprintf(stderr, "Older restore is larger, using that\n");
		    if (tmp_filename)
			unlink(tmp_filename);
		    amfree(tempdump->file);
		    amfree(tempdump);
		    amfree(tmp_filename);
		    amfree(final_filename);
                    return (bytes_read);
		}
	    }
	}
	if(tmp_filename && final_filename &&
		rename(tmp_filename, final_filename) < 0) {
	    error("Can't rename %s to %s: %s",
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
        memcpy(oldout->file, file, SIZEOF(dumpfile_t));
        if(flags->inline_assemble) oldout->outfd = pipes[0].pipe[1];
	else oldout->outfd = -1;
        oldout->comp_enc_pid = -1;
        oldout->lastpartnum = file->partnum;
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

    return (bytes_read);
}

/* return NULL if the label is not the expected one                     */
/* return the label if it is the expected one, and set *tapefd to a     */
/* file descriptor to the tapedev                                       */
char *
label_of_current_slot(
    char         *cur_tapedev,
    FILE         *prompt_out,
    int          *tapefd,
    dumpfile_t   *file,
    rst_flags_t  *flags,
    am_feature_t *their_features,
    ssize_t      *read_result,
    tapelist_t   *desired_tape)
{
    struct stat stat_tape;
    char *label = NULL;
    int wrongtape = 0;
    char *err;

    if (!cur_tapedev) {
	send_message(prompt_out, flags, their_features,
		     "no tapedev specified");
    } else if (tape_stat(cur_tapedev, &stat_tape) !=0 ) {
	send_message(prompt_out, flags, their_features, 
		     "could not stat '%s': %s",
		     cur_tapedev, strerror(errno));
	wrongtape = 1;
    } else if((err = tape_rewind(cur_tapedev)) != NULL) {
	send_message(prompt_out, flags, their_features, 
			 "Could not rewind device '%s': %s",
			 cur_tapedev, err);
	wrongtape = 1;
	/* err should not be freed */
    } else if((*tapefd = tape_open(cur_tapedev, 0)) < 0){
	send_message(prompt_out, flags, their_features,
			 "could not open tape device %s: %s",
			 cur_tapedev, strerror(errno));
	wrongtape = 1;
    }

    if (!wrongtape) {
 	*read_result = read_file_header(file, *tapefd, 0, flags);
 	if (file->type != F_TAPESTART) {
	    send_message(prompt_out, flags, their_features,
			     "Not an amanda tape");
 	    tapefd_close(*tapefd);
	} else {
	    if (flags->check_labels && desired_tape &&
			 strcmp(file->name, desired_tape->label) != 0) {
		send_message(prompt_out, flags, their_features,
				 "Label mismatch, got %s and expected %s",
				 file->name, desired_tape->label);
		tapefd_close(*tapefd);
	    }
	    else {
		label = stralloc(file->name);
	    }
	}
    }
    return label;
}

/* return >0            the number of slot move            */
/* return LOAD_STOP     if the search must be stopped      */
/* return LOAD_CHANGER  if the changer search the library  */
int
load_next_tape(
    char         **cur_tapedev,
    FILE          *prompt_out,
    int            backwards,
    rst_flags_t   *flags,
    am_feature_t  *their_features,
    tapelist_t    *desired_tape)
{
    int ret = -1;

    if (desired_tape) {
	send_message(prompt_out, flags, their_features,
		     "Looking for tape %s...",
		     desired_tape->label);
	if (backwards) {
	    searchlabel = desired_tape->label; 
	    changer_find(NULL, scan_init, loadlabel_slot,
			 desired_tape->label);
	    ret = LOAD_CHANGER;
	} else {
	    amfree(curslot);
	    changer_loadslot("next", &curslot,
			     cur_tapedev);
	    ret = 1;
	}
    } else {
	assert(!flags->amidxtaped);
	amfree(curslot);
	changer_loadslot("next", &curslot, cur_tapedev);
	ret = 1;
    }
    return ret;
}


/* return  0     a new tape is loaded       */
/* return -1     no new tape                */
int
load_manual_tape(
    char         **cur_tapedev,
    FILE          *prompt_out,
    rst_flags_t   *flags,
    am_feature_t  *their_features,
    tapelist_t    *desired_tape)
{
    int ret = 0;
    char *input = NULL;

    if (flags->amidxtaped) {
	if (their_features &&
	    am_has_feature(their_features,
			   fe_amrecover_FEEDME)) {
	    fprintf(prompt_out, "FEEDME %s\r\n",
		    desired_tape->label);
	    fflush(prompt_out);
	    input = agets(stdin);/* Strips \n but not \r */
	    if(!input) {
		error("Connection lost with amrecover");
		/*NOTREACHED*/
	    } else if (strcmp("OK\r", input) == 0) {
	    } else if (strncmp("TAPE ", input, 5) == 0) {
		amfree(*cur_tapedev);
		*cur_tapedev = alloc(1025);
		if (sscanf(input, "TAPE %1024s\r", *cur_tapedev) != 1) {
		    error("Got bad response from amrecover: %s", input);
		    /*NOTREACHED*/
		}
	    } else {
		send_message(prompt_out, flags, their_features,
			     "Got bad response from amrecover: %s", input);
		error("Got bad response from amrecover: %s", input);
		/*NOTREACHED*/
	    }
	} else {
	    send_message(prompt_out, flags, their_features,
			 "Client doesn't support fe_amrecover_FEEDME");
	    error("Client doesn't support fe_amrecover_FEEDME");
	    /*NOTREACHED*/
	}
    }
    else {
	if (desired_tape) {
	    fprintf(prompt_out,
		    "Insert tape labeled %s in device %s \n"
		    "and press enter, ^D to finish reading tapes\n",
		    desired_tape->label, *cur_tapedev);
	} else {
	    fprintf(prompt_out,"Insert a tape to search and press "
		    "enter, ^D to finish reading tapes\n");
	}
	fflush(prompt_out);
	if((input = agets(stdin)) == NULL)
	    ret = -1;
    }

    amfree(input);
    return ret;
}


void 
search_a_tape(
    char         *cur_tapedev,
    FILE         *prompt_out,
    rst_flags_t  *flags,
    am_feature_t *their_features,
    tapelist_t   *desired_tape,
    int           isafile,
    match_list_t *match_list,
    seentapes_t  *tape_seen,
    dumpfile_t   *file,
    dumpfile_t   *prev_rst_file,
    dumpfile_t   *tapestart,
    int           slot_num,
    ssize_t      *read_result)
{
    off_t       filenum;
    dumplist_t *fileentry = NULL;
    int         tapefile_idx = -1;
    int         i;
    char       *logline = NULL;
    FILE       *logstream = NULL;
    off_t       fsf_by;

    filenum = (off_t)0;
    if(desired_tape && desired_tape->numfiles > 0)
	tapefile_idx = 0;

    if (desired_tape) {
	dbprintf(("search_a_tape: desired_tape=%p label=%s\n",
		  desired_tape, desired_tape->label));
	dbprintf(("tape:   numfiles = %d\n", desired_tape->numfiles));
	for (i=0; i < desired_tape->numfiles; i++) {
	    dbprintf(("tape:   files[%d] = " OFF_T_FMT "\n",
		      i, (OFF_T_FMT_TYPE)desired_tape->files[i]));
	}
    } else {
	dbprintf(("search_a_tape: no desired_tape\n"));
    }
    dbprintf(("current tapefile_idx = %d\n", tapefile_idx));
	
    /* if we know where we're going, fastforward there */
    if(flags->fsf && !isafile){
	/* If we have a tapelist entry, filenums will be store there */
	if(tapefile_idx >= 0) {
	    fsf_by = desired_tape->files[tapefile_idx]; 
	} else {
	    /*
	     * older semantics assume we're restoring one file, with the fsf
	     * flag being the filenum on tape for said file
	     */
	    fsf_by = (flags->fsf == 0) ? (off_t)0 : (off_t)1;
	}
	if(fsf_by > (off_t)0){
	    if(tapefd_rewind(tapefd) < 0) {
		send_message(prompt_out, flags, their_features,
			     "Could not rewind device %s: %s",
			     cur_tapedev, strerror(errno));
		error("Could not rewind device %s: %s",
		      cur_tapedev, strerror(errno));
		/*NOTREACHED*/
	    }

	    if(tapefd_fsf(tapefd, fsf_by) < 0) {
		send_message(prompt_out, flags, their_features,
			     "Could not fsf device %s by " OFF_T_FMT ": %s",
			     cur_tapedev, (OFF_T_FMT_TYPE)fsf_by,
			     strerror(errno));
		error("Could not fsf device %s by " OFF_T_FMT ": %s",
		      cur_tapedev, (OFF_T_FMT_TYPE)fsf_by,
		      strerror(errno));
		/*NOTREACHED*/
	    }
	    else {
		filenum = fsf_by;
	    }
	    *read_result = read_file_header(file, tapefd, isafile, flags);
	}
    }

    while((file->type == F_TAPESTART || file->type == F_DUMPFILE ||
	   file->type == F_SPLIT_DUMPFILE) &&
	  (tapefile_idx < 0 || tapefile_idx < desired_tape->numfiles)) {
	int found_match = 0;
	match_list_t *me;
	dumplist_t *tempdump = NULL;

	/* store record of this dump for inventorying purposes */
	tempdump = alloc(SIZEOF(dumplist_t));
	tempdump->file = alloc(SIZEOF(dumpfile_t));
	tempdump->next = NULL;
	memcpy(tempdump->file, &file, SIZEOF(dumpfile_t));
	if(tape_seen->files){
	    fileentry = tape_seen->files;
	    while (fileentry->next != NULL)
		   fileentry = fileentry->next;
	    fileentry->next = tempdump;
	}
	else {
	    tape_seen->files = tempdump;
	}

	/* see if we need to restore the thing */
	if(isafile)
	    found_match = 1;
	else if(tapefile_idx >= 0){ /* do it by explicit file #s */
	    if(filenum == desired_tape->files[tapefile_idx]){
		found_match = 1;
	   	tapefile_idx++;
	    }
	}
	else{ /* search and match headers */
	    for(me = match_list; me; me = me->next) {
		if(disk_match(file, me->datestamp, me->hostname,
			      me->diskname, me->level) != 0){
		    found_match = 1;
		    break;
		}
	    }
	}

	if(found_match){
	    char *filename = make_filename(file);

	    fprintf(stderr, "%s: " OFF_T_FMT ": restoring ",
		    get_pname(), (OFF_T_FMT_TYPE)filenum);
	    print_header(stderr, file);
	    *read_result = restore(file, filename, tapefd, isafile, flags);
	    filenum++;
	    amfree(filename);
	}

	/* advance to the next file, fast-forwarding where reasonable */
	if (!isafile) {
	    if (*read_result == 0) {
		tapefd_close(tapefd);
		if((tapefd = tape_open(cur_tapedev, 0)) < 0) {
		    send_message(prompt_out, flags, their_features,
				 "could not open %s: %s",
				 cur_tapedev, strerror(errno));
		    error("could not open %s: %s",
			  cur_tapedev, strerror(errno));
		    /*NOTREACHED*/
		}
	    /* if the file is not what we're looking for fsf to next one */
	    }
	    else if (!found_match) {
		if (tapefd_fsf(tapefd, (off_t)1) < 0) {
		    send_message(prompt_out, flags, their_features,
				 "Could not fsf device %s: %s",
				 cur_tapedev, strerror(errno));
		    error("Could not fsf device %s: %s",
			  cur_tapedev, strerror(errno));
		    /*NOTREACHED*/
		}
		filenum ++;
	    }
	    else if (flags->fsf && (tapefile_idx >= 0) && 
		     (tapefile_idx < desired_tape->numfiles)) {
		fsf_by = desired_tape->files[tapefile_idx] - filenum;
		if (fsf_by > (off_t)0) {
		    if(tapefd_fsf(tapefd, fsf_by) < 0) {
			send_message(prompt_out, flags, their_features,
				     "Could not fsf device %s by "
				     OFF_T_FMT ": %s",
				     cur_tapedev, (OFF_T_FMT_TYPE)fsf_by,
				     strerror(errno));
			error("Could not fsf device %s by " OFF_T_FMT ": %s",
			      cur_tapedev, (OFF_T_FMT_TYPE)fsf_by,
			      strerror(errno));
			/*NOTREACHED*/
		    }
		    filenum = desired_tape->files[tapefile_idx];
		}
	    }
	} /* !isafile */

	memcpy(prev_rst_file, file, SIZEOF(dumpfile_t));
	      
	if(isafile)
	    break;
        *read_result = read_file_header(file, tapefd, isafile, flags);

	/* only restore a single dump, if piping to stdout */
	if (!headers_equal(prev_rst_file, file, 1) &&
	    (flags->pipe_to_fd == fileno(stdout)) && found_match) {
	    break;
	}
    } /* while we keep seeing headers */

    if (!isafile) {
	if (file->type == F_EMPTY) {
	    aclose(tapefd);
	    if((tapefd = tape_open(cur_tapedev, 0)) < 0) {
		send_message(prompt_out, flags, their_features,
			     "could not open %s: %s",
			     cur_tapedev, strerror(errno));
		error("could not open %s: %s",
		      cur_tapedev, strerror(errno));
		/*NOTREACHED*/
	    }
	} else {
	    if (tapefd_fsf(tapefd, (off_t)1) < 0) {
		send_message(prompt_out, flags, their_features,
			     "could not fsf %s: %s",
			     cur_tapedev, strerror(errno));;
		error("could not fsf %s: %s",
		      cur_tapedev, strerror(errno));
		/*NOTREACHED*/
	    }
	}
    }
    tapefd_close(tapefd);

    /* spit out our accumulated list of dumps, if we're inventorying */
    if (logstream) {
	logline = log_genstring(L_START, "taper",
				    "datestamp %s label %s tape %d",
				    tapestart->datestamp, tapestart->name,
				    slot_num);
	fprintf(logstream, "%s", logline);
	for(fileentry=tape_seen->files; fileentry; fileentry=fileentry->next){
	    logline = NULL;
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
	    if(logline){
		fprintf(logstream, "%s", logline);
		amfree(logline);
		fflush(logstream);
	    }
        }
    }
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
    int			use_changer,
    tapelist_t *	tapelist,
    match_list_t *	match_list,
    rst_flags_t *	flags,
    am_feature_t *	their_features)
{
    int have_changer = 1;
    int slot_num = -1;
    int slots = -1;
    FILE *logstream = NULL;
    tapelist_t *desired_tape = NULL;
    struct sigaction act, oact;
    ssize_t read_result;
    int slot;
    char *label = NULL;
    seentapes_t *seentapes = NULL;
    int ret;

    dbprintf(("search_tapes(prompt=%p, use_changer=%d, tapelist=%p, "
	      "match_list=%p, flags=%p, features=%p)\n",
	      prompt_out, use_changer, tapelist, match_list,
	      flags, their_features));

    if(!prompt_out) prompt_out = stderr;

    if(flags->blocksize)
	blocksize = (size_t)flags->blocksize;
    else if(blocksize == (size_t)SSIZE_MAX)
	blocksize = DISK_BLOCK_BYTES;

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /* catch SIGINT with something that'll flush unmerged splits */
    act.sa_handler = handle_sigint;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if(sigaction(SIGINT, &act, &oact) != 0){
	error("error setting SIGINT handler: %s", strerror(errno));
	/*NOTREACHED*/
    }
    if(flags->delay_assemble || flags->inline_assemble) exitassemble = 1;
    else exitassemble = 0;

    /* if given a log file, print an inventory of stuff found */
    if(flags->inventory_log) {
	if(!strcmp(flags->inventory_log, "-")) logstream = stdout;
	else if((logstream = fopen(flags->inventory_log, "w+")) == NULL) {
	    error("Couldn't open log file %s for writing: %s",
		  flags->inventory_log, strerror(errno));
	    /*NOTREACHED*/
	}
    }

    /* Suss what tape device we're using, whether there's a changer, etc. */
    if(!use_changer || (have_changer = changer_init()) == 0) {
	if(flags->alt_tapedev) cur_tapedev = stralloc(flags->alt_tapedev);
	else if(!cur_tapedev) cur_tapedev = getconf_str(CNF_TAPEDEV);
	/* XXX oughta complain if no config is loaded */
	fprintf(stderr, "%s: Using tapedev %s\n", get_pname(), cur_tapedev);
 	have_changer = 0;
    } else if (have_changer != 1) {
	error("changer initialization failed: %s", strerror(errno));
	/*NOTREACHED*/
    }
    else{ /* good, the changer works, see what it can do */
	amfree(curslot);
	changer_info(&slots, &curslot, &backwards);
    }

    if(tapelist && !flags->amidxtaped){
      slots = num_entries(tapelist);
      /*
	Spit out a list of expected tapes, so people with manual changers know
	what to load
      */
      fprintf(prompt_out, "The following tapes are needed:");
      for(desired_tape = tapelist; desired_tape != NULL;
	  desired_tape = desired_tape->next){
	fprintf(prompt_out, " %s", desired_tape->label);
      }
      fprintf(prompt_out, "\n");
      fflush(prompt_out);
      if(flags->wait_tape_prompt){
	char *input = NULL;
        fprintf(prompt_out,"Press enter when ready\n");
	fflush(prompt_out);
        input = agets(stdin);
 	amfree(input);
	fprintf(prompt_out, "\n");
	fflush(prompt_out);
      }
    }
    desired_tape = tapelist;

    if(use_changer && !cur_tapedev) { /* load current slot */
	amfree(curslot);
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

    do { /* all desired tape */
	seentapes_t *tape_seen = NULL;
	dumpfile_t file, tapestart, prev_rst_file;
	int isafile = 0;
	read_result = 0;

	slot_num = 0;

	memset(&file, 0, SIZEOF(file));

	if (desired_tape && desired_tape->isafile) {
	    isafile = 1;
	    if ((tapefd = open(desired_tape->label, 0)) == -1) {
		send_message(prompt_out, flags, their_features, 
			     "could not open %s: %s",
			     desired_tape->label, strerror(errno));
		continue;
	    }
	    fprintf(stderr, "Reading %s to fd %d\n",
			    desired_tape->label, tapefd);

	    read_result = read_file_header(&file, tapefd, 1, flags);
	    label = stralloc(desired_tape->label);
	} else {
	    /* check current_slot */
	    label = label_of_current_slot(cur_tapedev, prompt_out,
					  &tapefd, &file, flags,
					  their_features, &read_result,
					  desired_tape);
	    while (label==NULL && slot_num < slots &&
		   use_changer) {
		/*
		 * If we have an incorrect tape loaded, go try to find
		 * the right one
		 * (or just see what the next available one is).
		 */
		slot = load_next_tape(&cur_tapedev, prompt_out,
				      backwards, flags,
				      their_features, desired_tape);
		if(slot == LOAD_STOP) {
		    slot_num = slots;
		    amfree(label);
		} else {
		    if (slot == LOAD_CHANGER)
			slot_num = slots;
		    else /* slot > 0 */
			slot_num += slot;

		    /* check current_slot */
		    label = label_of_current_slot(cur_tapedev, prompt_out,
					          &tapefd, &file, flags,
					          their_features, &read_result,
					          desired_tape);
		}
	    }

	    if (label == NULL) {
		ret = load_manual_tape(&cur_tapedev, prompt_out,
				       flags,
				       their_features, desired_tape);
		if (ret == 0) {
		    label = label_of_current_slot(cur_tapedev, prompt_out,
					          &tapefd, &file, flags,
					          their_features, &read_result,
					          desired_tape);
		}
	    }

	    if (label)
		memcpy(&tapestart, &file, SIZEOF(dumpfile_t));
	}
	
	if (!label)
	    continue;

	/*
	 * Skip this tape if we did it already.  Note that this would let
	 * duplicate labels through, so long as they were in the same slot.
	 * I'm over it, are you?
	 */
	if (!isafile) {
	    for (tape_seen = seentapes; tape_seen;
		 tape_seen = tape_seen->next) {
		if (!strcmp(tape_seen->label, label) &&
		    !strcmp(tape_seen->slotstr, curslot)){
		    send_message(prompt_out, flags, their_features,
				 "Saw repeat tape %s in slot %s",
				 label, curslot);
		    amfree(label);
		    break;
		}
	    }
	}

	if(!label)
	    continue;

	if(!curslot)
	    curslot = stralloc("<none>");

	if(!isafile){
	    fprintf(stderr, "Scanning %s (slot %s)\n", label, curslot);
	    fflush(stderr);
	}

	tape_seen = alloc(SIZEOF(seentapes_t));
	memset(tape_seen, '\0', SIZEOF(seentapes_t));

	tape_seen->label = label;
	tape_seen->slotstr = stralloc(curslot);
	tape_seen->next = seentapes;
	tape_seen->files = NULL;
	seentapes = tape_seen;

	/*
	 * Start slogging through the tape itself.  If our tapelist (if we
	 * have one) contains a list of files to restore, obey that instead
	 * of checking for matching headers on all files.
	 */

	search_a_tape(cur_tapedev, prompt_out, flags, their_features,
		      desired_tape, isafile, match_list, tape_seen,
		      &file, &prev_rst_file, &tapestart, slot_num,
		      &read_result);

	fprintf(stderr, "%s: Search of %s complete\n",
			get_pname(), tape_seen->label);
	if (desired_tape) desired_tape = desired_tape->next;

	/* only restore a single dump, if piping to stdout */
	if (!headers_equal(&prev_rst_file, &file, 1) &&
	    flags->pipe_to_fd == fileno(stdout))
		break;

    } while (desired_tape);

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

    flags->fsf = 1;
    flags->comp_type = COMPRESS_FAST_OPT;
    flags->inline_assemble = 1;
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
	fprintf(stderr, "Cannot specify 'compress output' and 'leave compression alone' together\n");
	ret = -1;
    }

    if(flags->restore_dir != NULL){
	struct stat statinfo;

	if(flags->pipe_to_fd != -1){
	    fprintf(stderr, "Specifying output directory and piping output are mutually exclusive\n");
	    ret = -1;
	}
	if(stat(flags->restore_dir, &statinfo) < 0){
	    fprintf(stderr, "Cannot stat restore target dir '%s': %s\n",
		      flags->restore_dir, strerror(errno));
	    ret = -1;
	}
	if((statinfo.st_mode & S_IFMT) != S_IFDIR){
	    fprintf(stderr, "'%s' is not a directory\n", flags->restore_dir);
	    ret = -1;
	}
    }

    if((flags->pipe_to_fd != -1 || flags->compress) &&
	    (flags->delay_assemble || !flags->inline_assemble)){
	fprintf(stderr, "Split dumps *must* be automatically reassembled when piping output or compressing/uncompressing\n");
	ret = -1;
    }

    if(flags->delay_assemble && flags->inline_assemble){
	fprintf(stderr, "Inline split assembling and delayed assembling are mutually exclusive\n");
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


/*
 * Clean up after a match_list_t
 */
void
free_match_list(
    match_list_t *	match_list)
{
    match_list_t *me;
    match_list_t *prev = NULL;
  
    for(me = match_list; me; me = me->next){
	/* XXX freeing these is broken? can't work out why */
/*	amfree(me->hostname);
	amfree(me->diskname);
	amfree(me->datestamp);
	amfree(me->level); */
	amfree(prev);
	prev = me;
    }
    amfree(prev);
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
    vsnprintf(linebuf, SIZEOF(linebuf)-1, format, argp);
    arglist_end(argp);

    fprintf(stderr,"%s\n", linebuf);
    if (flags->amidxtaped && their_features &&
	am_has_feature(their_features, fe_amrecover_message)) {
	fprintf(prompt_out, "MESSAGE %s\r\n", linebuf);
	fflush(prompt_out);
    }
}

