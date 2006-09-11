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
/* $Id: taper.c,v 1.144 2006/08/24 11:23:32 martinea Exp $
 *
 * moves files from holding disk to tape, or from a socket to tape
 */

#include "amanda.h"
#include "util.h"
#include "conffile.h"
#include "tapefile.h"
#include "clock.h"
#include "stream.h"
#include "holding.h"
#include "logfile.h"
#include "tapeio.h"
#include "changer.h"
#include "version.h"
#include "arglist.h"
#include "token.h"
#include "amfeatures.h"
#include "fileheader.h"
#include "server_util.h"
#include "taperscan.c"

#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#ifdef HAVE_LIBVTBLC
#include <vtblc.h>
#include <strings.h>
#include <math.h>


static int vtbl_no   = -1;
static int len       =  0;
static int offset    =  0;
static char *datestr = NULL;
static char start_datestr[20];
static time_t raw_time;
static struct tm tape_time;
static struct tm backup_time;
static struct tm *tape_timep = &tape_time;
typedef struct vtbl_lbls {
    u_int8_t  label[45];
    u_int8_t  date[20];
} vtbl_lbls;
static vtbl_lbls vtbl_entry[MAX_VOLUMES];
#endif /* HAVE_LIBVTBLC */
/*
 * XXX update stat collection/printing
 * XXX advance to next tape first in next_tape
 * XXX label is being read twice?
 */
static off_t splitsize = (off_t)0; /* max size of dumpfile before split (Kb) */
static off_t mmap_splitsize = (off_t)0;
static char *mmap_filename = NULL;
static char *mmap_splitbuf = NULL;
static char *mem_splitbuf = NULL;
static char *splitbuf = NULL;
static off_t mem_splitsize = (off_t)0;
static char *splitbuf_wr_ptr = NULL; /* the number of Kb we've written into splitbuf */
int orig_holdfile = -1;

/* NBUFS replaced by conf_tapebufs */
/* #define NBUFS		20 */
static int conf_tapebufs;

static off_t maxseek = (off_t)1 << ((SIZEOF(off_t) * 8) - 11);

static char *holdfile_path = NULL;
static char *holdfile_path_thischunk = NULL;
static int num_holdfile_chunks = 0;
static off_t holdfile_offset_thischunk = (off_t)0;
static int mmap_splitbuffer_fd = -1;

#define MODE_NONE 0
#define MODE_FILE_WRITE 1
#define MODE_PORT_WRITE 2

static mode_t mode = MODE_NONE;

/* This is now the number of empties, not full bufs */
#define THRESHOLD	1

#define CONNECT_TIMEOUT 2*60

#define EMPTY 1
#define FILLING 2
#define FULL 3

typedef struct buffer_s {
    long status;
    ssize_t size;
    char *buffer;
} buffer_t;

#define nextbuf(p)    ((p) == buftable+conf_tapebufs-1? buftable : (p)+1)
#define prevbuf(p)    ((p) == buftable? buftable+conf_tapebufs-1 : (p)-1)

/* major modules */
int main(int main_argc, char **main_argv);
void file_reader_side(int rdpipe, int wrpipe);
void tape_writer_side(int rdpipe, int wrpipe);
void put_syncpipe_fault_result(char *handle);

/* shared-memory routines */
char *attach_buffers(size_t size);
void detach_buffers(char *bufp);
void destroy_buffers(void);
#define REMOVE_SHARED_MEMORY() \
    detach_buffers(buffers); \
    if (strcmp(procname, "reader") == 0) { \
	destroy_buffers(); \
    }

/* synchronization pipe routines */
void syncpipe_init(int rd, int wr);
void syncpipe_read_error(ssize_t rc, ssize_t expected);
void syncpipe_write_error(ssize_t rc, ssize_t expected);
int syncpipe_get(int *intp);
int syncpipe_getint(void);
char *syncpipe_getstr(void);
int syncpipe_put(int ch, int intval);
int syncpipe_putint(int i);
int syncpipe_putstr(const char *str);

/* tape manipulation subsystem */
int first_tape(char *new_datestamp);
int next_tape(int writerr);
int end_tape(int writerr);
int write_filemark(void);

/* support crap */
int seek_holdfile(int fd, buffer_t *bp, off_t kbytes);

/* signal handling */
static void install_signal_handlers(void);
static void signal_handler(int);

/* exit routine */
static void cleanup(void);

/*
 * ========================================================================
 * GLOBAL STATE
 *
 */
int interactive;
pid_t writerpid;
times_t total_wait;
#ifdef TAPER_DEBUG
int bufdebug = 1;
#else
int bufdebug = 0;
#endif

char *buffers = NULL;
buffer_t *buftable = NULL;
int err;

char *procname = "parent";

char *taper_timestamp = NULL;
char *label = NULL;
int filenum;
char *errstr = NULL;
int tape_fd = -1;
char *tapedev = NULL;
char *tapetype = NULL;
tapetype_t *tt = NULL;
size_t tt_blocksize;
size_t tt_blocksize_kb;
size_t buffer_size;
int tt_file_pad;
static unsigned long malloc_hist_1, malloc_size_1;
static unsigned long malloc_hist_2, malloc_size_2;
dumpfile_t file;
dumpfile_t *save_holdfile = NULL;
off_t cur_span_chunkstart = (off_t)0; /* start of current split dump chunk (Kb) */
char *holdfile_name;
int num_splits = 0;
int expected_splits = 0;
int num_holdfiles = 0;
times_t curdump_rt;

am_feature_t *their_features = NULL;

int runtapes, cur_tape, have_changer, tapedays;
char *labelstr, *conf_tapelist;
#ifdef HAVE_LIBVTBLC
char *rawtapedev;
int first_seg, last_seg;
#endif /* HAVE_LIBVTBLC */

/*
 * ========================================================================
 * MAIN PROGRAM
 *
 */
int
main(
    int main_argc,
    char **main_argv)
{
    int p2c[2], c2p[2];		/* parent-to-child, child-to-parent pipes */
    char *conffile;
    size_t size;
    int i;
    size_t j;
    size_t page_size;
    char *first_buffer;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

    safe_fd(-1, 0);

    set_pname("taper");

    dbopen("server");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    parse_server_conf(main_argc, main_argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    fprintf(stderr, "%s: pid %ld executable %s version %s\n",
	    get_pname(), (long) getpid(), my_argv[0], version());
    dbprintf(("%s: pid %ld executable %s version %s\n",
	    get_pname(), (long) getpid(), my_argv[0], version()));
    fflush(stderr);

    if (my_argc > 1 && my_argv[1][0] != '-') {
	config_name = stralloc(my_argv[1]);
	config_dir = vstralloc(CONFIG_DIR, "/", my_argv[1], "/", NULL);
	my_argc--;
	my_argv++;
    } else {
	char my_cwd[STR_SIZE];

	if (getcwd(my_cwd, SIZEOF(my_cwd)) == NULL) {
	    error("cannot determine current working directory");
	    /*NOTREACHED*/
	}
	config_dir = stralloc2(my_cwd, "/");
	if ((config_name = strrchr(my_cwd, '/')) != NULL) {
	    config_name = stralloc(config_name + 1);
	}
    }

    safe_cd();

    install_signal_handlers();
    atexit(cleanup);

    /* print prompts and debug messages if running interactive */

    interactive = (my_argc > 1 && strcmp(my_argv[1],"-t") == 0);
    if (interactive) {
	erroutput_type = ERR_INTERACTIVE;
    } else {
	erroutput_type = ERR_AMANDALOG;
	set_logerror(logerror);
    }

    free_new_argv(new_argc, new_argv);

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if (read_tapelist(conf_tapelist)) {
	error("could not load tapelist \"%s\"", conf_tapelist);
	/*NOTREACHED*/
    }

    tapedev	= stralloc(getconf_str(CNF_TAPEDEV));
    tapetype    = getconf_str(CNF_TAPETYPE);
    tt		= lookup_tapetype(tapetype);
#ifdef HAVE_LIBVTBLC
    rawtapedev = stralloc(getconf_str(CNF_RAWTAPEDEV));
#endif /* HAVE_LIBVTBLC */
    tapedays	= getconf_int(CNF_TAPECYCLE);
    labelstr	= getconf_str(CNF_LABELSTR);

    runtapes	= getconf_int(CNF_RUNTAPES);
    cur_tape	= 0;

    conf_tapebufs = getconf_int(CNF_TAPEBUFS);

    tt_blocksize_kb = (size_t)tapetype_get_blocksize(tt);
    tt_blocksize = tt_blocksize_kb * 1024;
    tt_file_pad = tapetype_get_file_pad(tt);

    if (interactive) {
	fprintf(stderr,"taper: running in interactive test mode\n");
	dbprintf(("taper: running in interactive test mode\n"));
	fflush(stderr);
    }

    /* create read/write syncronization pipes */

    if (pipe(p2c)) {
	error("creating sync pipes: %s", strerror(errno));
	/*NOTREACHED*/
    }
    if (pipe(c2p)) {
	error("creating sync pipes: %s", strerror(errno));
	/*NOTREACHED*/
    }

    /* create shared memory segment */

#if defined(HAVE_GETPAGESIZE)
    page_size = (size_t)getpagesize();
    fprintf(stderr, "%s: page size = " SIZE_T_FMT "\n",
		get_pname(), (SIZE_T_FMT_TYPE)page_size);
    dbprintf(("%s: page size = " SIZE_T_FMT "\n", get_pname(),
		(SIZE_T_FMT_TYPE)page_size));
#else
    page_size = 1024;
    fprintf(stderr, "%s: getpagesize() not available, using " SIZE_T_FMT "\n",
	    get_pname(), page_size);
    dbprintf((stderr, "%s: getpagesize() not available, using " SIZE_T_FMT "\n",
	    get_pname(), page_size));
#endif
    buffer_size = am_round(tt_blocksize, page_size);
    fprintf(stderr, "%s: buffer size is " SIZE_T_FMT "\n",
	    get_pname(), (SIZE_T_FMT_TYPE)buffer_size);
    dbprintf(("%s: buffer size is " SIZE_T_FMT "\n",
	    get_pname(), (SIZE_T_FMT_TYPE)buffer_size));
    while (conf_tapebufs > 0) {
	size  = page_size;
	size += conf_tapebufs * buffer_size;
	size += conf_tapebufs * SIZEOF(buffer_t);
	if ((buffers = attach_buffers(size)) != NULL) {
	    break;
	}
	log_add(L_INFO, "attach_buffers: (%d tapebuf%s: %d bytes) %s",
			conf_tapebufs,
			(conf_tapebufs == 1) ? "" : "s",
			size,
			strerror(errno));
	conf_tapebufs--;
    }
    if (buffers == NULL) {
	error("cannot allocate shared memory");
	/*NOTREACHED*/
    }

    /* page boundary offset */
    i = (int)((buffers - (char *)0) & (page_size - 1));
    if (i != 0) {
	first_buffer = buffers + page_size - i;
	dbprintf(("%s: shared memory at %p, first buffer at %p\n",
		get_pname(),
		(void *)buffers,
		(void *)first_buffer));
    } else {
	first_buffer = buffers;
    }

    /*LINTED  first_buffer, conf_tapebufs and buffer size are all * pagesize */
    buftable = (buffer_t *)(first_buffer + (conf_tapebufs * buffer_size));
    memset(buftable, 0, conf_tapebufs * SIZEOF(buffer_t));
    if (conf_tapebufs < 10) {
	j = 1;
    } else if (conf_tapebufs < 100) {
	j = 2;
    } else {
	j = 3;
    }
    for (i = 0; i < conf_tapebufs; i++) {
	buftable[i].buffer = first_buffer + i * buffer_size;
	dbprintf(("%s: buffer[%0*d] at %p\n",
		get_pname(),
		(int)j, i,
		(void *)buftable[i].buffer));
    }
    dbprintf(("%s: buffer structures at %p for %d bytes\n",
	    get_pname(),
	    (void *)buftable,
	    (int)(conf_tapebufs * SIZEOF(buffer_t))));

    /* fork off child writer process, parent becomes reader process */
    switch(writerpid = fork()) {
    case -1:
	error("fork: %s", strerror(errno));
	/*NOTREACHED*/

    case 0:	/* child */
	aclose(p2c[1]);
	aclose(c2p[0]);

	tape_writer_side(p2c[0], c2p[1]);
	error("tape writer terminated unexpectedly");
	/*NOTREACHED*/

    default:	/* parent */
	aclose(p2c[0]);
	aclose(c2p[1]);

	file_reader_side(c2p[0], p2c[1]);
	error("file reader terminated unexpectedly");
	/*NOTREACHED*/
    }

    /*NOTREACHED*/
    return 0;
}


/*
 * ========================================================================
 * FILE READER SIDE
 *
 */
int read_file(int fd, char *handle,
		  char *host, char *disk, char *datestamp, 
		  int level);
ssize_t taper_fill_buffer(int fd, buffer_t *bp, size_t buflen);
void dumpbufs(char *str1);
void dumpstatus(buffer_t *bp);
ssize_t get_next_holding_file(int fd, buffer_t *bp, char **strclosing, size_t rc);
int predict_splits(char *filename);
void create_split_buffer(char *split_diskbuffer, size_t fallback_splitsize, char *id_string);
void free_split_buffer(void);


/*
 * Create a buffer, either in an mmapped file or in memory, where PORT-WRITE
 * dumps can buffer the current split chunk in case of retry.
 */
void
create_split_buffer(
    char *split_diskbuffer,
    size_t fallback_splitsize,
    char *id_string)
{
    char *buff_err = NULL;
    off_t offset;
    char *splitbuffer_path = NULL;
    
    /* don't bother if we're not actually splitting */
    if (splitsize <= (off_t)0) {
	splitbuf = NULL;
	splitbuf_wr_ptr = NULL;
	return;
    }

#ifdef HAVE_MMAP
#ifdef HAVE_SYS_MMAN_H
    if (strcmp(split_diskbuffer, "NULL")) {
	void *nulls = NULL;
	char *quoted;
	off_t c;

	splitbuffer_path = vstralloc(split_diskbuffer,
				     "/splitdump_buffer",
				     NULL);
	/* different file, munmap the previous */
	if (mmap_filename && strcmp(mmap_filename, splitbuffer_path) != 0) {
	    dbprintf(("create_split_buffer: new file %s\n", splitbuffer_path));
	    munmap(splitbuf, (size_t)mmap_splitsize);
	    aclose(mmap_splitbuffer_fd);
	    mmap_splitbuf = NULL;
	    amfree(mmap_filename);
	    mmap_splitsize = 0;
	}
	if (!mmap_filename) {
	    dbprintf(("create_split_buffer: open file %s\n",
		      splitbuffer_path));
	    mmap_splitbuffer_fd = open(splitbuffer_path, O_RDWR|O_CREAT, 0600);
	    if (mmap_splitbuffer_fd == -1) {
		buff_err = newvstralloc(buff_err, "open of ", 
					splitbuffer_path, "failed (",
					strerror(errno), ")", NULL);
		goto fallback;
	    }
	}
	offset = lseek(mmap_splitbuffer_fd, (off_t)0, SEEK_END) / 1024;
	if (offset < splitsize) { /* Increase file size */
	    dbprintf(("create_split_buffer: increase file size of %s to "
		      OFF_T_FMT "kb\n",
		      splitbuffer_path, (OFF_T_FMT_TYPE)splitsize));
	    if (mmap_filename) {
		dbprintf(("create_split_buffer: munmap old file %s\n",
			  mmap_filename));
		munmap(splitbuf, (size_t)mmap_splitsize);
		mmap_splitsize = 0;
		mmap_splitbuf = NULL;
	    }
	    nulls = alloc(1024); /* lame */
	    memset(nulls, 0, 1024);
	    for (c = offset; c < splitsize ; c += (off_t)1) {
		if (fullwrite(mmap_splitbuffer_fd, nulls, 1024) < 1024) {
		    buff_err = newvstralloc(buff_err, "write to ",
					    splitbuffer_path,
					    "failed (", strerror(errno),
					    ")", NULL);
		    c -= 1;
		    if (c <= (off_t)fallback_splitsize) {
			goto fallback;
		    }
		    splitsize = c;
		    break;
		}
	    }
	}
	amfree(nulls);

	if (mmap_splitsize < splitsize*1024) {
	    mmap_splitsize = splitsize*1024;
	    mmap_filename = stralloc(splitbuffer_path);
	    dbprintf(("create_split_buffer: mmap file %s for " OFF_T_FMT "kb\n",
			  mmap_filename,(OFF_T_FMT_TYPE)splitsize));
            mmap_splitbuf = mmap(NULL, (size_t)mmap_splitsize,
				 PROT_READ|PROT_WRITE,
				 MAP_SHARED, mmap_splitbuffer_fd, (off_t)0);
	    if (mmap_splitbuf == (char*)-1) {
		buff_err = newvstralloc(buff_err, "mmap failed (",
					strerror(errno), ")", NULL);
		aclose(mmap_splitbuffer_fd);
		amfree(mmap_filename);
		mmap_splitsize = 0;
		mmap_splitbuf = NULL;
		goto fallback;
	    }
	}
	quoted = quote_string(splitbuffer_path);
	fprintf(stderr,
		"taper: r: buffering " OFF_T_FMT
		"kb split chunks in mmapped file %s\n",
		(OFF_T_FMT_TYPE)splitsize, quoted);
	dbprintf(("taper: r: buffering " OFF_T_FMT
		"kb split chunks in mmapped file %s\n",
		(OFF_T_FMT_TYPE)splitsize, quoted));
	amfree(splitbuffer_path);
	amfree(quoted);
	amfree(buff_err);
	splitbuf = mmap_splitbuf;
	splitbuf_wr_ptr = splitbuf;
	return;
    } else {
	buff_err = stralloc("no split_diskbuffer specified");
    }
#else
    (void)split_diskbuffer;	/* Quite unused parameter warning */
    buff_err = stralloc("mman.h not available");
    goto fallback;
#endif
#else
    (void)split_diskbuffer;	/* Quite unused parameter warning */
    buff_err = stralloc("mmap not available");
    goto fallback;
#endif

    /*
      Buffer split dumps in memory, if we can't use a file.
    */
    fallback:
	amfree(splitbuffer_path);
        splitsize = (off_t)fallback_splitsize;
	dbprintf(("create_split_buffer: fallback size " OFF_T_FMT "\n",
		  (OFF_T_FMT_TYPE)splitsize));
	log_add(L_INFO,
	        "%s: using fallback split size of %dkb to buffer %s in-memory",
		buff_err, splitsize, id_string);
	amfree(buff_err);
	if (splitsize > mem_splitsize) {
	    amfree(mem_splitbuf);
	    mem_splitbuf = alloc(fallback_splitsize * 1024);
	    mem_splitsize = fallback_splitsize;
	    dbprintf(("create_split_buffer: alloc buffer size " OFF_T_FMT "\n",
			  (OFF_T_FMT_TYPE)splitsize *1024));
	}
	splitbuf = mem_splitbuf;
	splitbuf_wr_ptr = splitbuf;
}

/*
 * Free up resources that create_split_buffer eats.
 */
void
free_split_buffer(void)
{
    if (mmap_splitbuffer_fd != -1) {
#ifdef HAVE_MMAP
#ifdef HAVE_SYS_MMAN_H
	if (splitbuf != NULL)
	    munmap(splitbuf, (size_t)mmap_splitsize);
#endif
#endif
	aclose(mmap_splitbuffer_fd);
	amfree(mmap_filename);
	mmap_splitsize = 0;
    }
    if (mem_splitbuf) {
	amfree(splitbuf);
	mem_splitsize = 0;
    }
}

void
put_syncpipe_fault_result(
    char *	handle)
{
    char *q;

    if (handle == NULL)
	handle = "<nohandle>";

    q = squotef("[Taper syncpipe fault]");
    putresult(TAPE_ERROR, "%s %s\n", handle, q);
    log_add(L_ERROR, "tape-error %s %s", handle, q);
    amfree(q);
}

void
file_reader_side(
    int rdpipe,
    int wrpipe)
{
    cmd_t cmd;
    struct cmdargs cmdargs;
    char *handle = NULL;
    char *filename = NULL;
    char *qfilename = NULL;
    char *hostname = NULL;
    char *diskname = NULL;
    char *qdiskname = NULL;
    char *result = NULL;
    char *datestamp = NULL;
    char *split_diskbuffer = NULL;
    char *id_string = NULL;
    int tok;
    char *q = NULL;
    int level, fd;
    in_port_t data_port;
    int data_socket;
    pid_t wpid;
    char level_str[64];
    struct stat stat_file;
    int tape_started;
    int a;
    size_t fallback_splitsize = 0;
    int tmpint;
    char *c, *c1;

    procname = "reader";
    syncpipe_init(rdpipe, wrpipe);

    /* must get START_TAPER before beginning */

    startclock();
    cmd = getcmd(&cmdargs);
    total_wait = stopclock();

    if (cmd != START_TAPER || cmdargs.argc != 2) {
	error("error [file_reader_side cmd %d argc %d]", cmd, cmdargs.argc);
	/*NOTREACHED*/
    }

    /* pass start command on to tape writer */

    taper_timestamp = newstralloc(taper_timestamp, cmdargs.argv[2]);

    tape_started = 0;
    if (syncpipe_put('S', 0) == -1) {
	put_syncpipe_fault_result(NULL);
    }

    if (syncpipe_putstr(taper_timestamp) == -1) {
	put_syncpipe_fault_result(NULL);
    }

    /* get result of start command */

    tok = syncpipe_get(&tmpint);
    switch(tok) {
    case -1:
	put_syncpipe_fault_result(NULL);
	break;

    case 'S':
	putresult(TAPER_OK, "\n");
	tape_started = 1;
	/* start is logged in writer */
	break;

    case 'E':
	/* no tape, bail out */
	if ((result = syncpipe_getstr()) == NULL) {
	    put_syncpipe_fault_result(NULL);
	} else {
	    q = squotef("[%s]", result);
	    putresult(TAPE_ERROR, "<nohandle> %s\n", q);
	    amfree(q);
	    log_add(L_ERROR,"no-tape [%s]", "No writable valid tape found");
	    c = c1 = result;
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
	    amfree(result);
	    (void)syncpipe_put('e', 0);			/* ACK error */
	}
	break;

    case 'H': /* Syncpipe I/O error */
	/* No ACK syncpipe is down just exit */
        put_syncpipe_fault_result(handle);
	break;

    case 'X':
	/*
	 * Pipe read error: Communications is severed at least
	 * back to us.  We send a blind 'Q' (quit) and we don't
	 * wait for a response...
	 */
	syncpipe_put('Q', 0);			/* ACK error */
	error("error [communications pipe from writer severed]");
	/*NOTREACHED*/

    default:
	q = squotef("[syncpipe sequence fault: Expected 'S' or 'E']");
	putresult(TAPE_ERROR, "<nohandle> %s\n", q);
	log_add(L_ERROR, "no-tape %s]", q);
	amfree(q);
    }

    /* process further driver commands */
    while (1) {
	startclock();
	cmd = getcmd(&cmdargs);
	if (cmd != QUIT && !tape_started) {
	    error("error [file_reader_side cmd %d without tape ready]", cmd);
	    /*NOTREACHED*/
	}
	total_wait = timesadd(total_wait, stopclock());

	switch(cmd) {
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
	    mode = MODE_PORT_WRITE;
	    cmdargs.argc++;			/* true count of args */
	    a = 2;

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: handle]");
		/*NOTREACHED*/
	    }
	    handle = newstralloc(handle, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: hostname]");
		/*NOTREACHED*/
	    }
	    hostname = newstralloc(hostname, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: features]");
		/*NOTREACHED*/
	    }
	    am_release_feature_set(their_features);
	    their_features = am_string_to_feature(cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: diskname]");
		/*NOTREACHED*/
	    }
	    qdiskname = newstralloc(qdiskname, cmdargs.argv[a++]);
	    if (diskname != NULL)
		amfree(diskname);
	    diskname = unquote_string(qdiskname);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: level]");
		/*NOTREACHED*/
	    }
	    level = atoi(cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: datestamp]");
		/*NOTREACHED*/
	    }
	    datestamp = newstralloc(datestamp, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: splitsize]");
		/*NOTREACHED*/
	    }
	    splitsize = OFF_T_ATOI(cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: split_diskbuffer]");
		/*NOTREACHED*/
	    }
	    split_diskbuffer = newstralloc(split_diskbuffer, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper PORT-WRITE: not enough args: fallback_splitsize]");
		/*NOTREACHED*/
	    }
	    /* Must fit in memory... */
	    fallback_splitsize = (size_t)atoi(cmdargs.argv[a++]);

	    if (a != cmdargs.argc) {
		error("error [taper file_reader_side PORT-WRITE: too many args: %d != %d]",
		      cmdargs.argc, a);
	        /*NOTREACHED*/
	    }

	    if (fallback_splitsize < 128 ||
		fallback_splitsize > 64 * 1024 * 1024) {
		error("error [bad value for fallback_splitsize]");
		/*NOTREACHED*/
	    }
	    snprintf(level_str, SIZEOF(level_str), "%d", level);
	    id_string = newvstralloc(id_string, hostname, ":", qdiskname, ".",
				     level_str, NULL);

	    create_split_buffer(split_diskbuffer, fallback_splitsize, id_string);
	    amfree(id_string);

	    data_port = 0;
	    data_socket = stream_server(&data_port, 0, STREAM_BUFSIZE, 0);	
	    if (data_socket < 0) {
		char *m;

		m = vstralloc("[port create failure: ",
			      strerror(errno),
			      "]",
			      NULL);
		q = squote(m);
		putresult(TAPE_ERROR, "%s %s\n", handle, q);
		amfree(m);
		amfree(q);
		break;
	    }
	    putresult(PORT, "%d\n", data_port);

	    if ((fd = stream_accept(data_socket, CONNECT_TIMEOUT,
				   0, STREAM_BUFSIZE)) == -1) {
		q = squote("[port connect timeout]");
		putresult(TAPE_ERROR, "%s %s\n", handle, q);
		aclose(data_socket);
		amfree(q);
		break;
	    }
	    expected_splits = -1;

	    while(read_file(fd, handle, hostname, qdiskname, datestamp, level))
		(void)fd;  /* Quiet lint */

	    aclose(data_socket);
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
	    mode = MODE_FILE_WRITE;
	    cmdargs.argc++;			/* true count of args */
	    a = 2;

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: handle]");
		/*NOTREACHED*/
	    }
	    handle = newstralloc(handle, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: filename]");
		/*NOTREACHED*/
	    }
	    qfilename = newstralloc(qfilename, cmdargs.argv[a++]);
	    if (filename != NULL)
		amfree(filename);
	    filename = unquote_string(qfilename);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: hostname]");
		/*NOTREACHED*/
	    }
	    hostname = newstralloc(hostname, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: features]");
		/*NOTREACHED*/
	    }
	    am_release_feature_set(their_features);
	    their_features = am_string_to_feature(cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: diskname]");
		/*NOTREACHED*/
	    }
	    qdiskname = newstralloc(qdiskname, cmdargs.argv[a++]);
	    if (diskname != NULL)
		amfree(diskname);
	    diskname = unquote_string(qdiskname);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: level]");
		/*NOTREACHED*/
	    }
	    level = atoi(cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: datestamp]");
		/*NOTREACHED*/
	    }
	    datestamp = newstralloc(datestamp, cmdargs.argv[a++]);

	    if (a >= cmdargs.argc) {
		error("error [taper FILE-WRITE: not enough args: splitsize]");
		/*NOTREACHED*/
	    }
	    splitsize = OFF_T_ATOI(cmdargs.argv[a++]);

	    if (a != cmdargs.argc) {
		error("error [taper file_reader_side FILE-WRITE: too many args: %d != %d]",
		      cmdargs.argc, a);
	        /*NOTREACHED*/
	    }
	    if (holdfile_name != NULL) {
		filename = newstralloc(filename, holdfile_name);
	    }

	    if ((expected_splits = predict_splits(filename)) < 0) {
		break;
	    }
	    if (stat(filename, &stat_file)!=0) {
		q = squotef("[%s]", strerror(errno));
		putresult(TAPE_ERROR, "%s %s\n", handle, q);
		amfree(q);
		break;
	    }
	    if ((fd = open(filename, O_RDONLY)) == -1) {
		q = squotef("[%s]", strerror(errno));
		putresult(TAPE_ERROR, "%s %s\n", handle, q);
		amfree(q);
		break;
	    }
	    holdfile_path = stralloc(filename);
	    holdfile_path_thischunk = stralloc(filename);
	    holdfile_offset_thischunk = (off_t)0;

	    while (read_file(fd,handle,hostname,qdiskname,datestamp,level)) {
		if (splitsize > (off_t)0 && holdfile_path_thischunk)
		    filename = newstralloc(filename, holdfile_path_thischunk);
		if ((fd = open(filename, O_RDONLY)) == -1) {
		    q = squotef("[%s]", strerror(errno));
		    putresult(TAPE_ERROR, "%s %s\n", handle, q);
		    amfree(q);
		    break;
		}
	    }
	    break;

	case QUIT:
	    putresult(QUITTING, "\n");
	    fprintf(stderr,"taper: DONE [idle wait: %s secs]\n",
		    walltime_str(total_wait));
	    fflush(stderr);
	    (void)syncpipe_put('Q', 0);	/* tell writer we're exiting gracefully */
	    aclose(wrpipe);

	    if ((wpid = wait(NULL)) != writerpid) {
		dbprintf(("taper: writer wait returned %u instead of %u: %s\n",
			(unsigned)wpid, (unsigned)writerpid, strerror(errno)));
		fprintf(stderr,
			"taper: writer wait returned %u instead of %u: %s\n",
			(unsigned)wpid, (unsigned)writerpid, strerror(errno));
		fflush(stderr);
	    }

	    free_split_buffer();
	    amfree(datestamp);
	    clear_tapelist();
	    free_server_config();
	    amfree(taper_timestamp);
	    amfree(label);
	    amfree(errstr);
	    amfree(changer_resultstr);
	    amfree(tapedev);
	    amfree(filename);
	    amfree(conf_tapelist);
	    amfree(config_dir);
	    amfree(config_name);
	    amfree(holdfile_name);

	    malloc_size_2 = malloc_inuse(&malloc_hist_2);

	    if (malloc_size_1 != malloc_size_2) {
		malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
	    }
	    exit(0);
	    /*NOTREACHED*/

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
    }
    /* NOTREACHED */
}

void
dumpbufs(
    char *str1)
{
    int i,j;
    long v;

    fprintf(stderr, "%s: state", str1);
    for (i = j = 0; i < conf_tapebufs; i = j+1) {
	v = buftable[i].status;
	for(j = i; j < conf_tapebufs && buftable[j].status == v; j++)
	    (void)j; /* Quiet lint */
	j--;
	if (i == j) {
	    fprintf(stderr, " %d:", i);
	} else {
	    fprintf(stderr, " %d-%d:", i, j);
	}
	switch(v) {
	case FULL:
	    fputc('F', stderr);
	    break;

	case FILLING:
	    fputc('f', stderr);
	    break;

	case EMPTY:
	    fputc('E', stderr);
	    break;

	default:
	    fprintf(stderr, "%ld", v);
	    break;
	}
    }
    fputc('\n', stderr);
    fflush(stderr);
}

void
dumpstatus(
    buffer_t *bp)
{
    char pn[2];
    char bt[NUM_STR_SIZE];
    char status[NUM_STR_SIZE + 1];
    char *str = NULL;

    pn[0] = procname[0];
    pn[1] = '\0';
    snprintf(bt, SIZEOF(bt), "%d", (int)(bp-buftable));

    switch(bp->status) {
    case FULL:
	snprintf(status, SIZEOF(status), "F" SIZE_T_FMT,
		(SIZE_T_FMT_TYPE)bp->size);
	break;

    case FILLING:
	snprintf(status, SIZEOF(status), "f");
	break;

    case EMPTY:
	snprintf(status, SIZEOF(status), "E");
	break;

    default:
	snprintf(status, SIZEOF(status), "%ld", bp->status);
	break;
    }

    str = vstralloc("taper: ", pn, ": [buf ", bt, ":=", status, "]", NULL);
    dumpbufs(str);
    amfree(str);
}

/*
  Handle moving to the next chunk of holding file, if any.  Returns -1 for
  errors, 0 if there's no more file, or a positive integer for the amount of
  stuff read that'll go into 'rc' (XXX That's fugly, maybe that should just
  be another global.  What is rc anyway, 'read count?' I keep thinking it
  should be 'return code')
*/
ssize_t
get_next_holding_file(
    int fd,
    buffer_t *bp,
    char **strclosing,
    size_t rc)
{
    int save_fd;
    ssize_t rc1;
    struct stat stat_file;
    ssize_t ret = -1;
    
    save_fd = fd;
    close(fd);
    
    /* see if we're fresh out of file */
    if (file.cont_filename[0] == '\0') {
 	err = 0;
 	ret = 0;
    } else if (stat(file.cont_filename, &stat_file) != 0) {
 	err = errno;
 	ret = -1;
 	*strclosing = newvstralloc(*strclosing, "can't stat: ",
				   file.cont_filename, NULL);
    } else if ((fd = open(file.cont_filename,O_RDONLY)) == -1) {
 	err = errno;
 	ret = -1;
 	*strclosing = newvstralloc(*strclosing, "can't open: ",
				   file.cont_filename, NULL);
    } else if ((fd != save_fd) && dup2(fd, save_fd) == -1) {
 	err = errno;
 	ret = -1;
 	*strclosing = newvstralloc(*strclosing, "can't dup2: ",
				   file.cont_filename, NULL);
    } else {
 	buffer_t bp1;
	char *quoted;

 	holdfile_path = stralloc(file.cont_filename);
	quoted = quote_string(holdfile_path);
 	fprintf(stderr, "taper: r: switching to next holding chunk '%s'\n",
		quoted); 
	amfree(quoted);
 	num_holdfile_chunks++;
	
 	bp1.status = EMPTY;
 	bp1.size = DISK_BLOCK_BYTES;
  	bp1.buffer = alloc(DISK_BLOCK_BYTES);
	
 	if (fd != save_fd) {
 	    close(fd);
 	    fd = save_fd;
 	}
	
 	rc1 = taper_fill_buffer(fd, &bp1, DISK_BLOCK_BYTES);
 	if (rc1 <= 0) {
 	    amfree(bp1.buffer);
 	    err = (rc1 < 0) ? errno : 0;
 	    ret = -1;
 	    *strclosing = newvstralloc(*strclosing,
 				       "Can't read header: ",
 				       file.cont_filename,
 				       NULL);
 	} else {
 	    parse_file_header(bp1.buffer, &file, (size_t)rc1);
	    
 	    amfree(bp1.buffer);
 	    bp1.buffer = bp->buffer + rc;
	    
 	    rc1 = taper_fill_buffer(fd, &bp1, (size_t)tt_blocksize - rc);
 	    if (rc1 <= 0) {
 		err = (rc1 < 0) ? errno : 0;
 		ret = -1;
 		if (rc1 < 0) {
 	    	    *strclosing = newvstralloc(*strclosing,
 					       "Can't read data: ",
					       file.cont_filename,
 					       NULL);
 		}
 	    } else {
 		ret = rc1;
 		num_holdfiles++;
 	    }
 	}
    }

    return(ret);
}


int
read_file(
    int		fd,
    char *	handle,
    char *	hostname,
    char *	qdiskname,
    char *	datestamp,
    int		level)
{
    buffer_t *bp;
    int tok;
    ssize_t rc;
#ifdef ASSERTIONS
    int opening;
#endif
    int closing, bufnum, need_closing, nexting;
    off_t filesize;
    times_t runtime;
    char *strclosing = NULL;
    char seekerrstr[STR_SIZE];
    char *str;
    int header_written = 0;
    size_t buflen;
    dumpfile_t first_file;
    dumpfile_t cur_holdfile;
    off_t kbytesread = (off_t)0;
    int header_read = 0;
    char *cur_filename = NULL;
    int retry_from_splitbuf = 0;
    char *splitbuf_rd_ptr = NULL;
    char *q = NULL;

#ifdef HAVE_LIBVTBLC
    static char desc[45];
    static char vol_date[20];
    static char vol_label[45];
#endif /* HAVE_LIBVTBLC */


    /* initialize */
    memset(&first_file, 0, SIZEOF(first_file));
    memset(&cur_holdfile, 0, SIZEOF(cur_holdfile));

    filesize = (off_t)0;
    closing = 0;
    need_closing = 0;
    nexting = 0;
    err = 0;

    /* don't break this if we're still on the same file as a previous init */
    if (cur_span_chunkstart <= (off_t)0) {
	fh_init(&file);
	header_read = 0;
    } else if(mode == MODE_FILE_WRITE){
	memcpy(&file, save_holdfile, SIZEOF(dumpfile_t));
	memcpy(&cur_holdfile, save_holdfile, SIZEOF(dumpfile_t));
    }

    if (bufdebug) {
	fprintf(stderr, "taper: r: start file\n");
	fflush(stderr);
    }

    for (bp = buftable; bp < buftable + conf_tapebufs; bp++) {
	bp->status = EMPTY;
    }

    bp = buftable;
    if (interactive || bufdebug)
	dumpstatus(bp);

    if ((cur_span_chunkstart >= (off_t)0) && (splitsize > (off_t)0)) {
        /* We're supposed to start at some later part of the file, not read the
	   whole thing. "Seek" forward to where we want to be. */
	if (label)
	    putresult(SPLIT_CONTINUE, "%s %s\n", handle, label);
        if ((mode == MODE_FILE_WRITE) && (cur_span_chunkstart > (off_t)0)) {
	    char *quoted = quote_string(holdfile_path_thischunk);
	    fprintf(stderr, "taper: r: seeking %s to " OFF_T_FMT " kb\n",
	                    quoted,
			    (OFF_T_FMT_TYPE)holdfile_offset_thischunk);
	    fflush(stderr);

	    if (holdfile_offset_thischunk > maxseek) {
		snprintf(seekerrstr, SIZEOF(seekerrstr), "Can't seek by "
	      		OFF_T_FMT " kb (compiled for %d-bit file offsets), "
			"recompile with large file support or "
			"set holdingdisk chunksize to <" OFF_T_FMT " Mb",
			(OFF_T_FMT_TYPE)holdfile_offset_thischunk,
			(int)(sizeof(off_t) * 8),
			(OFF_T_FMT_TYPE)(maxseek/(off_t)1024));
		log_add(L_ERROR, "%s", seekerrstr);
		fprintf(stderr, "taper: r: FATAL: %s\n", seekerrstr);
		fflush(stderr);
		if (syncpipe_put('X', 0) == -1) {
			put_syncpipe_fault_result(handle);
		}
		amfree(quoted);
		return -1;
	    }
	    if (lseek(fd, holdfile_offset_thischunk*(off_t)1024, SEEK_SET) == (off_t)-1) {
		fprintf(stderr, "taper: r: FATAL: seek_holdfile lseek error "
	      		"while seeking into %s by "
			OFF_T_FMT "kb: %s\n", quoted,
			(OFF_T_FMT_TYPE)holdfile_offset_thischunk,
			strerror(errno));
		fflush(stderr);
		if (syncpipe_put('X', 0) == -1) {
			put_syncpipe_fault_result(handle);
		}
		amfree(quoted);
		return -1;
	    }
	    amfree(quoted);
        } else if (mode == MODE_PORT_WRITE) {
	    fprintf(stderr, "taper: r: re-reading split dump piece from buffer\n");
	    fflush(stderr);
	    retry_from_splitbuf = 1;
	    splitbuf_rd_ptr = splitbuf;
	    if (splitbuf_rd_ptr >= splitbuf_wr_ptr)
		retry_from_splitbuf = 0;
        }
        if (cur_span_chunkstart > (off_t)0)
	    header_read = 1; /* really initialized in prior run */
    }

    /* tell writer to open tape */

#ifdef ASSERTIONS
    opening = 1;
#endif

    if (syncpipe_put('O', 0) == -1) {
	put_syncpipe_fault_result(handle);
	return -1;
    }
    if (syncpipe_putstr(datestamp) == -1) {
	put_syncpipe_fault_result(handle);
	return -1;
    }
    if (syncpipe_putstr(hostname) == -1) {
	put_syncpipe_fault_result(handle);
	return -1;
    }
    if (syncpipe_putstr(qdiskname) == -1) {
	put_syncpipe_fault_result(handle);
	return -1;
    }
    if (syncpipe_putint(level) == -1) {
	put_syncpipe_fault_result(handle);
	return -1;
    }

    startclock();
    
    /* read file in loop */
    
    while (1) {
	if ((tok = syncpipe_get(&bufnum)) == -1) {
	    put_syncpipe_fault_result(handle);
	    return -1;
	}

	switch(tok) {
	case 'O':
#ifdef ASSERTIONS
	    assert(opening);
	    opening = 0;
#endif
	    err = 0;
	    break;
	    
	case 'R':
	    if (bufdebug) {
		fprintf(stderr, "taper: r: got R%d\n", bufnum);
		fflush(stderr);
	    }
	    
	    if (need_closing) {
		if (syncpipe_put('C', 0) == -1) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
		closing = 1;
		need_closing = 0;
		break;
	    }
	    
	    if (closing)
		break;	/* ignore extra read tokens */
	    
#ifdef ASSERTIONS
	    assert(!opening);
#endif
	    if(bp->status != EMPTY || bufnum != (int)(bp - buftable)) {
		/* XXX this SHOULD NOT HAPPEN.  Famous last words. */
		fprintf(stderr,"taper: panic: buffer mismatch at ofs "
			OFF_T_FMT ":\n", (OFF_T_FMT_TYPE)filesize);
		if(bufnum != (int)(bp - buftable)) {
		    fprintf(stderr, "    my buf %d but writer buf %d\n",
			    (int)(bp-buftable), bufnum);
		} else {
		    fprintf(stderr,"buf %d state %s (%ld) instead of EMPTY\n",
			    (int)(bp-buftable),
			    bp->status == FILLING? "FILLING" :
			    bp->status == FULL? "FULL" : "EMPTY!?!?",
			    (long)bp->status);
		}
		dumpbufs("taper");
		sleep(1);
		dumpbufs("taper: after 1 sec");
		if (bp->status == EMPTY)
		    fprintf(stderr, "taper: result now correct!\n");
		fflush(stderr);
		
		errstr = newstralloc(errstr,
				     "[fatal buffer mismanagement bug]");
		q = squote(errstr);
		putresult(TRYAGAIN, "%s %s\n", handle, q);
		cur_span_chunkstart = (off_t)0;
		amfree(q);
		log_add(L_INFO, "retrying %s:%s.%d on new tape due to: %s",
		        hostname, qdiskname, level, errstr);
		closing = 1;
		if (syncpipe_put('X', 0) == -1) {/* X == buffer snafu, bail */
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
		do {
		    if ((tok = syncpipe_get(&bufnum)) == -1) {
			put_syncpipe_fault_result(handle);
			return (-1);
		    }
		} while (tok != 'x');
		aclose(fd);
		return -1;
	    } /* end 'if (bf->status != EMPTY || bufnum != (int)(bp-buftable))' */

	    bp->status = FILLING;
	    buflen = header_read ? (size_t)tt_blocksize : DISK_BLOCK_BYTES;
	    if (interactive || bufdebug)
		dumpstatus(bp);
 	    if (header_written == 0 &&
	    		(header_read == 1 || cur_span_chunkstart > (off_t)0)) {
 		/* for split dumpfiles, modify headers for the second - nth
 		   pieces that signify that they're continuations of the last
 		   normal one */
 		char *cont_filename;
 		file.type = F_SPLIT_DUMPFILE;
 		file.partnum = num_splits + 1;
 		file.totalparts = expected_splits;
                cont_filename = stralloc(file.cont_filename);
 		file.cont_filename[0] = '\0';
 		build_header(bp->buffer, &file, tt_blocksize);
  
 		if (cont_filename[0] != '\0') {
 		  file.type = F_CONT_DUMPFILE;
                   strncpy(file.cont_filename, cont_filename,
                           SIZEOF(file.cont_filename));
  			}
 		memcpy(&cur_holdfile, &file, SIZEOF(dumpfile_t));
  
 		if (interactive || bufdebug)
		    dumpstatus(bp);
 		bp->size = (ssize_t)tt_blocksize;
 		rc = (ssize_t)tt_blocksize;
 		header_written = 1;
 		amfree(cont_filename);
 	    } else if (retry_from_splitbuf) {
 		/* quietly pull dump data from our in-memory cache, and the
 		   writer side need never know the wiser */
 		memcpy(bp->buffer, splitbuf_rd_ptr, tt_blocksize);
 		bp->size = (ssize_t)tt_blocksize;
 		rc = (ssize_t)tt_blocksize;
 
 		splitbuf_rd_ptr += tt_blocksize;
 		if (splitbuf_rd_ptr >= splitbuf_wr_ptr)
		    retry_from_splitbuf = 0;
 	    } else if ((rc = taper_fill_buffer(fd, bp, buflen)) < 0) {
 		err = errno;
 		closing = 1;
 		strclosing = newvstralloc(strclosing,"Can't read data: ",
					  NULL);
 		if (syncpipe_put('C', 0) == -1) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
 	    }
  
 	    if (!closing) {
 	        if (rc < (ssize_t)buflen) { /* switch to next holding file */
 		    ssize_t ret;

 		    if (file.cont_filename[0] != '\0') {
 		    	cur_filename = newvstralloc(cur_filename, file.cont_filename, NULL);
 		    }
 		    ret = get_next_holding_file(fd, bp, &strclosing, (size_t)rc);
 		    if (ret <= 0) {
 			need_closing = 1;
		    } else {
 		        memcpy(&cur_holdfile, &file, SIZEOF(dumpfile_t));
 		        rc += ret;
  			bp->size = rc;
  		    }
		}
  		if (rc > 0) {
  		    bp->status = FULL;
 		    /* rebuild the header block, which might have CONT junk */
  		    if (header_read == 0) {
  			char *cont_filename;
 			/* write the "real" filename if the holding-file
 			   is a partial one */
  			parse_file_header(bp->buffer, &file, (size_t)rc);
  			parse_file_header(bp->buffer, &first_file, (size_t)rc);
  			cont_filename = stralloc(file.cont_filename);
  			file.cont_filename[0] = '\0';
 			if (splitsize > (off_t)0) {
 			    file.type = F_SPLIT_DUMPFILE;
 			    file.partnum = 1;
 			    file.totalparts = expected_splits;
 			}
  			file.blocksize = tt_blocksize;
  			build_header(bp->buffer, &file, tt_blocksize);
 			kbytesread += (off_t)(tt_blocksize/1024); /* XXX shady */
 
 			file.type = F_CONT_DUMPFILE;
 
  			/* add CONT_FILENAME back to in-memory header */
  			strncpy(file.cont_filename, cont_filename, 
  				SIZEOF(file.cont_filename));
  			if (interactive || bufdebug)
			    dumpstatus(bp);
  			bp->size = (ssize_t)tt_blocksize; /* output a full tape block */
 			/* save the header, we'll need it if we jump tapes */
 			memcpy(&cur_holdfile, &file, SIZEOF(dumpfile_t));
  			header_read = 1;
 			header_written = 1;
  			amfree(cont_filename);
  		    } else {
 			filesize = kbytesread;
  		    }

		    if (bufdebug) {
			fprintf(stderr,"taper: r: put W%d\n",(int)(bp-buftable));
			fflush(stderr);
		    }
		    if (syncpipe_put('W', (int)(bp-buftable)) == -1) {
			put_syncpipe_fault_result(handle);
			return (-1);
		    }
		    bp = nextbuf(bp);
		}

		if (((kbytesread + (off_t)(DISK_BLOCK_BYTES/1024)) >= splitsize)
			&& (splitsize > (off_t)0) && !need_closing) {

		    if (mode == MODE_PORT_WRITE) {
			splitbuf_wr_ptr = splitbuf;
			splitbuf_rd_ptr = splitbuf;
			memset(splitbuf, 0, SIZEOF(splitbuf));
			retry_from_splitbuf = 0;
		    }

		    fprintf(stderr,"taper: r: end %s.%s.%s.%d part %d, "
		    		"splitting chunk that started at "
				OFF_T_FMT "kb after " OFF_T_FMT
				"kb (next chunk will start at "
				OFF_T_FMT "kb)\n",
				hostname, qdiskname, datestamp, level,
				num_splits+1,
				(OFF_T_FMT_TYPE)cur_span_chunkstart,
				(OFF_T_FMT_TYPE)kbytesread,
				(OFF_T_FMT_TYPE)(cur_span_chunkstart+kbytesread));
		    fflush(stderr);

		    nexting = 1;
		    need_closing = 1;
		} /* end '(kbytesread >= splitsize && splitsize > 0)' */
		if (need_closing && rc <= 0) {
		    if (syncpipe_put('C', 0) == -1) {
			put_syncpipe_fault_result(handle);
			return (-1);
		    }
		    need_closing = 0;
		    closing = 1;
		}
                kbytesread += (off_t)(rc / 1024);
	    } /* end the 'if (!closing)' (successful buffer fill) */
	    break;

	case 'T':
	case 'E':
	case 'H':
	    if (syncpipe_put('e', 0) == -1) {	/* ACK error */
		put_syncpipe_fault_result(handle);
		return (-1);
	    }

	    if ((str = syncpipe_getstr()) == NULL) {
		put_syncpipe_fault_result(handle);
		return (-1);
	    }
	    
	    errstr = newvstralloc(errstr, "[", str, "]", NULL);
	    amfree(str);

	    q = squote(errstr);
	    if (tok == 'T') {
		if (splitsize > (off_t)0) {
		    /* we'll be restarting this chunk on the next tape */
		    if (mode == MODE_FILE_WRITE) {
		      aclose(fd);
		    }

		    putresult(SPLIT_NEEDNEXT, "%s " OFF_T_FMT "\n", handle,
		    		(OFF_T_FMT_TYPE)cur_span_chunkstart);
		    log_add(L_INFO, "continuing %s:%s.%d on new tape from "
		    		OFF_T_FMT "kb mark: %s",
				hostname, qdiskname, level,
				(OFF_T_FMT_TYPE)cur_span_chunkstart, errstr);
		    return 1;
		} else {
		    /* restart the entire dump (failure propagates to driver) */
		    aclose(fd);
		    putresult(TRYAGAIN, "%s %s\n", handle, q);
		    cur_span_chunkstart = (off_t)0;
		    log_add(L_INFO, "retrying %s:%s.%d on new tape due to: %s",
			    hostname, qdiskname, level, errstr);
		}
	    } else {
		aclose(fd);
		putresult(TAPE_ERROR, "%s %s\n", handle, q);
		log_add(L_FAIL, "%s %s %s %d [out of tape]",
			hostname, qdiskname, datestamp, level);
		log_add(L_ERROR,"no-tape [%s]", "No more writable valid tape found");
	    }
	    amfree(q);
	    return 0;

	case 'C':
#ifdef ASSERTIONS
	    assert(!opening);
#endif
	    assert(closing);

	    if (nexting) {
	      cur_span_chunkstart += kbytesread; /* XXX possibly wrong */
	      if (cur_filename)
		holdfile_name = newvstralloc(holdfile_name, cur_filename,
					     NULL);
	      else
		amfree(holdfile_name);

	      kbytesread = (off_t)0;
	      amfree(cur_filename);
	    }

	    if ((str = syncpipe_getstr()) == NULL) {
		put_syncpipe_fault_result(handle);
		return (-1);
	    }

	    label = newstralloc(label, str ? str : "(null)");
	    amfree(str);
	    if ((str = syncpipe_getstr()) == NULL) {
		put_syncpipe_fault_result(handle);
		return (-1);
	    }

	    filenum = atoi(str ? str : "-9876");	/* ??? */
	    amfree(str);
	    fprintf(stderr, "taper: reader-side: got label %s filenum %d\n",
		    label, filenum);
	    fflush(stderr);

	    /* we'll need that file descriptor if we're gonna write more */
	    if (!nexting) {
		aclose(fd);
	    }

	    runtime = stopclock();
	    if (nexting)
		startclock();
	    if (err) {
		if (strclosing) {
		    errstr = newvstralloc(errstr,
				          "[input: ", strclosing, ": ",
					  strerror(err), "]", NULL);
		    amfree(strclosing);
		} else
		    errstr = newvstralloc(errstr,
				          "[input: ", strerror(err), "]",
				          NULL);
		q = squote(errstr);
		putresult(TAPE_ERROR, "%s %s\n", handle, q);

		amfree(q);
		if (splitsize != (off_t)0) {
		    log_add(L_FAIL, "%s %s %s.%d %d %s", hostname, qdiskname,
				datestamp, num_splits, level, errstr);
		} else {
		    log_add(L_FAIL, "%s %s %s %d %s",
				hostname, qdiskname, datestamp, level, errstr);
		}
		if ((str = syncpipe_getstr()) == NULL) {	/* reap stats */
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
		amfree(str);
                amfree(errstr);
	    } else {
		char kb_str[NUM_STR_SIZE];
		char kps_str[NUM_STR_SIZE];
		double rt;

		rt = (double)(runtime.r.tv_sec) +
		     ((double)(runtime.r.tv_usec) / 1000000.0);
		curdump_rt = timesadd(runtime, curdump_rt);
		snprintf(kb_str, SIZEOF(kb_str), OFF_T_FMT,
			(OFF_T_FMT_TYPE)filesize);
		snprintf(kps_str, SIZEOF(kps_str), "%3.1lf",
				  (isnormal(rt) ? (double)filesize / rt : 0.0));
		if ((str = syncpipe_getstr()) == NULL) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
		errstr = newvstralloc(errstr,
				      "[sec ", walltime_str(runtime),
				      " kb ", kb_str,
				      " kps ", kps_str,
				      " ", str,
				      "]",
				      NULL);
		if (splitsize == (off_t)0) { /* Ordinary dump */
		    q = squote(errstr);
/*@i@*/		    if (first_file.is_partial) {
			putresult(PARTIAL, "%s %s %d %s\n",
				  handle, label, filenum, q);
			log_add(L_PARTIAL, "%s %s %s %d %s",
				hostname, qdiskname, datestamp, level, errstr);
		    } else {
			putresult(DONE, "%s %s %d %s\n",
				  handle, label, filenum, q);
			log_add(L_SUCCESS, "%s %s %s %d %s",
				hostname, qdiskname, datestamp, level, errstr);
		    }
		    amfree(q);
		} else { /* Chunked dump */
		    num_splits++;
		    if (mode == MODE_FILE_WRITE) {
			holdfile_path_thischunk = stralloc(holdfile_path);
			holdfile_offset_thischunk = (lseek(fd, (off_t)0, SEEK_CUR))/(off_t)1024;
			if(!save_holdfile){
			    save_holdfile = alloc(SIZEOF(dumpfile_t));
			}
			memcpy(save_holdfile, &cur_holdfile,SIZEOF(dumpfile_t));
		    }
		    log_add(L_CHUNK, "%s %s %s %d %d %s", hostname, qdiskname,
			    datestamp, num_splits, level, errstr);
		    if (!nexting) { /* split dump complete */
			rt = (double)(curdump_rt.r.tv_sec) +
			     ((double)(curdump_rt.r.tv_usec) / 1000000.0);
			snprintf(kb_str, SIZEOF(kb_str), OFF_T_FMT,
				(OFF_T_FMT_TYPE)(filesize + cur_span_chunkstart));
			snprintf(kps_str, SIZEOF(kps_str), "%3.1lf",
			    isnormal(rt) ?
			    ((double)(filesize+cur_span_chunkstart)) / rt :
			    0.0);
                        amfree(errstr);
			errstr = newvstralloc(errstr,
					      "[sec ", walltime_str(curdump_rt),
					      " kb ", kb_str,
					      " kps ", kps_str,
					      " ", str,
					      "]",
					      NULL);
                        q = squote(errstr);
			putresult(DONE, "%s %s %d %s\n", handle, label,
				  filenum, q);
			log_add(L_CHUNKSUCCESS, "%s %s %s %d %s",
				hostname, qdiskname, datestamp, level, errstr);
			amfree(save_holdfile);
			amfree(holdfile_path_thischunk);
                        amfree(q);
		    }
 		}
		amfree(str);

 		if (!nexting) {
 		    num_splits = 0;
 		    expected_splits = 0;
 		    amfree(holdfile_name);
 		    num_holdfiles = 0;
 		    cur_span_chunkstart = (off_t)0;
 		    curdump_rt = times_zero;
 		}
 		amfree(errstr);
		
#ifdef HAVE_LIBVTBLC
		/* 
		 *  We have 44 characters available for the label string:
		 *  use max 20 characters for hostname
		 *      max 20 characters for diskname 
		 *             (it could contain a samba share or dos path)
		 *           2 for level
		 */
		memset(desc, '\0', 45);

		strncpy(desc, hostname, 20);

		if ((len = strlen(hostname)) <= 20) {
		    memset(desc + len, ' ', 1);
		    offset = len + 1;
		} else {
		    memset(desc + 20, ' ', 1);
		    offset = 21;
		}

		strncpy(desc + offset, qdiskname, 20);

		if ((len = strlen(qdiskname)) <= 20) {
		    memset(desc + offset + len, ' ', 1);
		    offset = offset + len + 1;
		} else {
		    memset(desc + offset + 20, ' ', 1);
		    offset = offset + 21;
		}

		sprintf(desc + offset, "%i", level);

	        strncpy(vol_label, desc, 44);
		fprintf(stderr, "taper: added vtbl label string %i: \"%s\"\n",
			filenum, vol_label);
		fflush(stderr);

		/* pass label string on to tape writer */
		if (syncpipe_put('L', filenum) == -1) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
		if (syncpipe_putstr(vol_label) == -1) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}

		/* 
		 * reformat datestamp for later use with set_date from vtblc 
		 */
		strptime(datestamp, "%Y%m%d", &backup_time);
		strftime(vol_date, 20, "%T %D", &backup_time);
		fprintf(stderr, 
			"taper: reformatted vtbl date string: \"%s\"->\"%s\"\n",
			datestamp,
			vol_date);

		/* pass date string on to tape writer */		
		if (syncpipe_put('D', filenum) == -1) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}
		if (syncpipe_putstr(vol_date) == -1) {
		    put_syncpipe_fault_result(handle);
		    return (-1);
		}

#endif /* HAVE_LIBVTBLC */
	    }
	    /* reset stuff that assumes we're on a new file */

	    if (!nexting)
		return 0;

#ifdef ASSERTIONS
	    opening = 1;
#endif
	    nexting = 0;
	    closing = 0;
	    filesize = (off_t)0;
	    if (syncpipe_put('O', 0) == -1) {
		put_syncpipe_fault_result(handle);
		return -1;
	    }
	    if (syncpipe_putstr(datestamp) == -1) {
		put_syncpipe_fault_result(handle);
		return -1;
	    }
	    if (syncpipe_putstr(hostname) == -1) {
		put_syncpipe_fault_result(handle);
		return -1;
	    }
	    if (syncpipe_putstr(qdiskname) == -1) {
		put_syncpipe_fault_result(handle);
		return -1;
	    }
	    if (syncpipe_putint(level) == -1) {
		put_syncpipe_fault_result(handle);
		return -1;
	    }
	    for (bp = buftable; bp < buftable + conf_tapebufs; bp++) {
		bp->status = EMPTY;
	    }
	    bp = buftable;
	    header_written = 0;
	    break;

	case 'X':
	    /*
	     * Pipe read error: Communications is severed at least
	     * back to us.  We send a blind 'Q' (quit) and we don't
	     * wait for a response...
	     */
	    syncpipe_put('Q', 0);			/* ACK error */
	    fprintf(stderr, "taper: communications pipe from reader severed\n");
	    return -1;

	default:
	    q = squotef("[Taper syncpipe protocol error]");
	    putresult(TAPE_ERROR, "%s %s\n", handle, q);
	    log_add(L_ERROR, "tape-error %s %s", handle, q);
	    amfree(q);
	    return -1;
	}
    }
    return 0;
}

ssize_t
taper_fill_buffer(
    int fd,
    buffer_t *bp,
    size_t buflen)
{
    char *curptr;
    ssize_t cnt;

    curptr = bp->buffer;

    cnt = fullread(fd, curptr, buflen);
    switch(cnt) {
    case 0:	/* eof */
	if (interactive)
	    fputs("r0", stderr);
	bp->size = 0;
	return (ssize_t)0;
	/*NOTREACHED*/

    case -1:	/* error on read, punt */
	if (interactive)
	    fputs("rE", stderr);
	bp->size = 0;
	return -1;
	/*NOTREACHED*/

    default:
	if ((mode == MODE_PORT_WRITE) && (splitsize > (off_t)0)) {
	    memcpy(splitbuf_wr_ptr, curptr, (size_t)cnt);
	    splitbuf_wr_ptr += cnt;
	}
	bp->size = cnt;
	break;
    }

    if (interactive)
	fputs("R", stderr);
    return ((ssize_t)bp->size);
}

/* Given a dumpfile in holding, determine its size and figure out how many
 * times we'd have to split it.
 */
int
predict_splits(
    char *filename)
{
    int splits = 0;
    off_t total_kb = (off_t)0;
    off_t adj_splitsize = splitsize - (off_t)(DISK_BLOCK_BYTES / 1024);

    if (splitsize <= (off_t)0)
	return(0);

    if (adj_splitsize <= (off_t)0) {
      error("Split size must be > " OFF_T_FMT "k",
      	(OFF_T_FMT_TYPE)(DISK_BLOCK_BYTES/1024));
      /*NOTREACHED*/
    }

    /* should only calculuate this once, not on retries etc */
    if (expected_splits != 0)
	return(expected_splits);

    total_kb = size_holding_files(filename, 1);
    
    if (total_kb <= (off_t)0) {
      fprintf(stderr, "taper: r: " OFF_T_FMT
      		" kb holding file makes no sense, not precalculating splits\n",
		(OFF_T_FMT_TYPE)total_kb);
      fflush(stderr);
      return(0);
    }

    fprintf(stderr, "taper: r: Total dump size should be " OFF_T_FMT
    		"kb, chunk size is " OFF_T_FMT "kb\n",
		(OFF_T_FMT_TYPE)total_kb,
		(OFF_T_FMT_TYPE)splitsize);
    fflush(stderr);

    splits = (int)(total_kb / adj_splitsize);
    if ((splits == 0) || (total_kb % adj_splitsize))
    	splits++;


    fprintf(stderr, "taper: r: Expecting to split into %d parts \n", splits);
    fflush(stderr);

    return(splits);
}

/*
 * ========================================================================
 * TAPE WRITER SIDE
 *
 */
times_t idlewait, rdwait, wrwait, fmwait;
unsigned long total_writes;
off_t total_tape_used;
int total_tape_fm;

void write_file(void);
int write_buffer(buffer_t *bp);

void
tape_writer_side(
    int getp,
    int putp)
{
    int tok;
    int tape_started;
    char *str;
    char *hostname;
    char *diskname;
    char *datestamp;
    int level;
    int tmpint;

#ifdef HAVE_LIBVTBLC
    char *vol_label;
    char *vol_date;
#endif /* HAVE_LIBVTBLC */

    procname = "writer";
    syncpipe_init(getp, putp);
    tape_started = 0;
    idlewait = times_zero;

    while (1) {
	startclock();
	if ((tok = syncpipe_get(&tmpint)) == -1) {
	    error("writer: Syncpipe failure before start");
	    /*NOTREACHED*/
	}

	idlewait = timesadd(idlewait, stopclock());
	if (tok != 'S' && tok != 'Q' && !tape_started) {
	    error("writer: token '%c' before start", tok);
	    /*NOTREACHED*/
	}

	switch(tok) {
	case 'H':		/* Reader read pipe side is down */
	    dbprintf(("writer: Communications with reader is down"));
	    error("writer: Communications with reader is down");
	    /*NOTREACHED*/
	    
	case 'S':		/* start-tape */
	    if (tape_started) {
		error("writer: multiple start requests");
		/*NOTREACHED*/
	    }
	    if ((str = syncpipe_getstr()) == NULL) {
		error("writer: Syncpipe failure");
		/*NOTREACHED*/
	    }
	    if (!first_tape(str ? str : "bad-datestamp")) {
		if (tape_fd >= 0) {
		    tapefd_close(tape_fd);
		    tape_fd = -1;
		}
		if (syncpipe_put('E', 0) == -1) {
		    error("writer: Syncpipe failure passing exit code");
		    /*NOTREACHED*/
		}
		if (syncpipe_putstr(errstr) == -1) {
		    error("writer: Syncpipe failure passing exit string");
		    /*NOTREACHED*/
		}
		/* wait for reader to acknowledge error */
		do {
		    if ((tok = syncpipe_get(&tmpint)) == -1) {
			error("writer: Syncpipe failure waiting for error ack");
			/*NOTREACHED*/
		    }
		    if (tok != 'e') {
			error("writer: got '%c' unexpectedly after error", tok);
			/*NOTREACHED*/
		    }
		} while (tok != 'e');
	    } else {
		if (syncpipe_put('S', 0) == -1) {
		    error("writer: syncpipe failure while starting tape");
		    /*NOTREACHED*/
		}
		tape_started = 1;
	    }
	    amfree(str);
	    break;

	case 'O':		/* open-output */
	    if ((datestamp = syncpipe_getstr()) == NULL) {
		error("writer: Syncpipe failure during open");
		/*NOTREACHED*/
	    }
	    tapefd_setinfo_datestamp(tape_fd, datestamp);
	    amfree(datestamp);

	    if ((hostname = syncpipe_getstr()) == NULL) {
		error("writer: Syncpipe failure fetching hostname");
		/*NOTREACHED*/
	    }
	    tapefd_setinfo_host(tape_fd, hostname);
	    amfree(hostname);

	    if ((diskname = syncpipe_getstr()) == NULL) {
		error("writer: Syncpipe failure fetching diskname");
		/*NOTREACHED*/
	    }
	    tapefd_setinfo_disk(tape_fd, diskname);
	    amfree(diskname);
	    if ((level = syncpipe_getint()) == -1) {
		error("writer: Syncpipe failure fetching level");
		/*NOTREACHED*/
	    }
	    tapefd_setinfo_level(tape_fd, level);
	    write_file();
	    break;

#ifdef HAVE_LIBVTBLC
	case 'L':		/* read vtbl label */
	    vtbl_no = tmpint;
	    if ((vol_label = syncpipe_getstr()) == NULL) {
		error("writer: Syncpipe failure fetching vrbl label");
		/*NOTREACHED*/
	    }
	    fprintf(stderr, "taper: read label string \"%s\" from pipe\n", 
		    vol_label);
	    strncpy(vtbl_entry[vtbl_no].label, vol_label, 45);
	    break;

	case 'D':		/* read vtbl date */
	    vtbl_no = tmpint;
	    if ((vol_date = syncpipe_getstr()) == NULL) {
		error("writer: Syncpipe failure fetching vrbl date");
		/*NOTREACHED*/
	    }
	    fprintf(stderr, "taper: read date string \"%s\" from pipe\n", 
		    vol_date);
	    strncpy(vtbl_entry[vtbl_no].date, vol_date, 20);
	    break;
#endif /* HAVE_LIBVTBLC */

	case 'Q':
	    end_tape(0);	/* XXX check results of end tape ?? */
	    clear_tapelist();
	    free_server_config();
	    amfree(taper_timestamp);
	    amfree(label);
	    amfree(errstr);
	    amfree(changer_resultstr);
	    amfree(tapedev);
	    amfree(conf_tapelist);
	    amfree(config_dir);
	    amfree(config_name);

	    malloc_size_2 = malloc_inuse(&malloc_hist_2);

	    if (malloc_size_1 != malloc_size_2) {
		malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
	    }
	    exit(0);
	    /*NOTREACHED*/

	default:
	    assert(0);
	}
    }
}

void
write_file(void)
{
    buffer_t *bp;
    int full_buffers, i, bufnum;
    int tok;
    char number[NUM_STR_SIZE];
    char *rdwait_str, *wrwait_str, *fmwait_str;
    int tmpint;

    rdwait = wrwait = times_zero;
    total_writes = 0;

    bp = buftable;
    full_buffers = 0;
    tok = '?';

    if (bufdebug) {
	fprintf(stderr, "taper: w: start file\n");
	fflush(stderr);
    }

    /*
     * Tell the reader that the tape is open, and give it all the buffers.
     */
    if (syncpipe_put('O', 0) == -1) {
	error("writer: Syncpipe failure starting write sequence");
	/*NOTREACHED*/
    }
    for (i = 0; i < conf_tapebufs; i++) {
	if (bufdebug) {
	    fprintf(stderr, "taper: w: put R%d\n", i);
	    fflush(stderr);
	}
	if (syncpipe_put('R', i) == -1) {
	    error("writer: Syncpipe failure readying write buffers");
	    /*NOTREACHED*/
	}
    }

    /*
     * We write the filemark at the start of the file rather than at the end,
     * so that it can proceed in parallel with the reader's initial filling
     * up of the buffers.
     */

    startclock();
    if (!write_filemark())
	goto tape_error;
    fmwait = stopclock();

    filenum += 1;

    do {

	/*
	 * STOPPED MODE
	 *
	 * At the start of the file, or if the input can't keep up with the
	 * tape, we enter STOPPED mode, which waits for most of the buffers
	 * to fill up before writing to tape.  This maximizes the amount of
	 * data written in chunks to the tape drive, minimizing the number
	 * of starts/stops, which in turn saves tape and time.
	 */

	if (interactive)
	    fputs("[WS]", stderr);
	startclock();
	while (full_buffers < conf_tapebufs - THRESHOLD) {
	    if ((tok = syncpipe_get(&bufnum)) == -1) {
		error("writer: Syncpipe failure during buffer advance");
		/*NOTREACHED*/
	    }
	    if (tok != 'W')
		break;
	    if (bufdebug) {
		fprintf(stderr,"taper: w: got W%d\n",bufnum);
		fflush(stderr);
	    }
	    full_buffers++;
	}
	rdwait = timesadd(rdwait, stopclock());

	/*
	 * STARTING MODE
	 *
	 * We start output when sufficient buffers have filled up, or at
	 * end-of-file, whichever comes first.  Here we drain all the buffers
	 * that were waited on in STOPPED mode.  If more full buffers come
	 * in, then we will be STREAMING.
	 */

	while (full_buffers) {
	    if (tt_file_pad && bp->size < (ssize_t)tt_blocksize) {
		memset(bp->buffer+bp->size, 0, tt_blocksize - bp->size);
		bp->size = (ssize_t)tt_blocksize;
	    }
	    if (!write_buffer(bp))
		goto tape_error;
	    full_buffers--;
	    bp = nextbuf(bp);
	}

	/*
	 * STREAMING MODE
	 *
	 * With any luck, the input source is faster than the tape drive.  In
	 * this case, full buffers will appear in the circular queue faster
	 * than we can write them, so the next buffer in the queue will always
	 * be marked FULL by the time we get to it.  If so, we'll stay in
	 * STREAMING mode.
	 *
	 * On the other hand, if we catch up to the input and thus would have
	 * to wait for buffers to fill, we are then STOPPED again.
	 */

	while (tok == 'W' && bp->status == FULL) {
	    if ((tok = syncpipe_get(&bufnum)) == -1) {
		error("writer: Syncpipe failure advancing buffer");
		/*NOTREACHED*/
	    }

	    if (tok == 'W') {
		if (bufdebug) {
		    fprintf(stderr,"taper: w: got W%d\n",bufnum);
		    fflush(stderr);
		}
		if(bufnum != (int)(bp - buftable)) {
		    fprintf(stderr,
			    "taper: tape-writer: my buf %d reader buf %d\n",
			    (int)(bp-buftable), bufnum);
		    fflush(stderr);
		    if (syncpipe_put('E', 0) == -1) { 
			error("writer: Syncpipe failure putting error token");
			/*NOTREACHED*/
		    }
		    if (syncpipe_putstr("writer-side buffer mismatch") == -1) {
			error("writer: Syncpipe failure putting error messgae");
			/*NOTREACHED*/
		    }
		    goto error_ack;
		}
		if (tt_file_pad && bp->size < (ssize_t)tt_blocksize) {
		    memset(bp->buffer+bp->size, 0, tt_blocksize - bp->size);
		    bp->size = (ssize_t)tt_blocksize;
		}
		if (!write_buffer(bp))
		    goto tape_error;
		bp = nextbuf(bp);
	    } else if (tok == 'Q') {
		return;
	    } else if (tok == 'X') {
		goto reader_buffer_snafu;
	    } else {
		error("writer-side not expecting token: %c", tok);
		/*NOTREACHED*/
	    }
	}
    } while (tok == 'W');

    /* got close signal from reader, acknowledge it */

    if (tok == 'X')
	goto reader_buffer_snafu;

    assert(tok == 'C');
    if (syncpipe_put('C', 0) == -1) {
	error("writer: Syncpipe failure putting close");
	/*NOTREACHED*/
    }

    /* tell reader the tape and file number */

    if (syncpipe_putstr(label) == -1) {
	error("writer: Syncpipe failure putting label");
	/*NOTREACHED*/
    }
    snprintf(number, SIZEOF(number), "%d", filenum);
    if (syncpipe_putstr(number) == -1) {
	error("writer: Syncpipe failure putting filenum");
	/*NOTREACHED*/
    }

    snprintf(number, SIZEOF(number), "%lu", total_writes);
    rdwait_str = stralloc(walltime_str(rdwait));
    wrwait_str = stralloc(walltime_str(wrwait));
    fmwait_str = stralloc(walltime_str(fmwait));
    errstr = newvstralloc(errstr,
			  "{wr:",
			  " writers ", number,
			  " rdwait ", rdwait_str,
			  " wrwait ", wrwait_str,
			  " filemark ", fmwait_str,
			  "}",
			  NULL);
    amfree(rdwait_str);
    amfree(wrwait_str);
    amfree(fmwait_str);
    if (syncpipe_putstr(errstr) == -1) {
	error("writer: Syncpipe failure putting '%s'", errstr);
	/*NOTREACHED*/
    }

    /* XXX go to next tape if past tape size? */

    return;

 tape_error:
    /* got tape error */
    if (next_tape(1)) {
	if (syncpipe_put('T', 0) == -1) {   /* next tape in place, try again */
	    error("writer: Syncpipe failure during tape advance");
	    /*NOTREACHED*/
	}
    } else {
	if (syncpipe_put('E', 0) == -1) {   /* no more tapes, fail */
	    error("writer: Syncpipe failure during tape error");
	    /*NOTREACHED*/
	}
    }
    if (syncpipe_putstr(errstr) == -1) {
	error("writer: Syncpipe failure putting '%s'", errstr);
	/*NOTREACHED*/
    }

 error_ack:
    /* wait for reader to acknowledge error */
    do {
	if ((tok = syncpipe_get(&tmpint)) == -1) {
	    error("writer: syncpipe failure waiting for error ack");
	    /*NOTREACHED*/
    	}

	if (tok != 'W' && tok != 'C' && tok != 'e') {
	    error("writer: got '%c' unexpectedly after error", tok);
	    /*NOTREACHED*/
	}
    } while (tok != 'e');
    return;

 reader_buffer_snafu:
    if (syncpipe_put('x', 0) == -1) {
	error("writer: syncpipe failure putting buffer snafu");
	/*NOTREACHED*/
    }
    return;
}

int
write_buffer(
    buffer_t *bp)
{
    ssize_t rc;

    assert(bp->status == FULL);

    startclock();
    rc = tapefd_write(tape_fd, bp->buffer, (size_t)bp->size);
    if (rc == (ssize_t)bp->size) {
#if defined(NEED_RESETOFS)
	static double tape_used_modulus_2gb = 0;

	/*
	 * If the next write will go over the 2 GByte boundary, reset
	 * the kernel concept of where we are to make sure it does not
	 * go silly on us.
	 */
	tape_used_modulus_2gb += (double)rc;
	if (tape_used_modulus_2gb + (double)rc > (double)0x7fffffff) {
	    tape_used_modulus_2gb = 0;
	    tapefd_resetofs(tape_fd);
	}
#endif
	wrwait = timesadd(wrwait, stopclock());
	total_writes += 1;
	total_tape_used += (off_t)rc;
	bp->status = EMPTY;
	if (interactive || bufdebug)
	    dumpstatus(bp);
	if (interactive)
	    fputs("W", stderr);

	if (bufdebug) {
	    fprintf(stderr, "taper: w: put R%d\n", (int)(bp-buftable));
	    fflush(stderr);
	}
	if (syncpipe_put('R', (int)(bp-buftable)) == -1) {
	    error("writer: Syncpipe failure during advancing write bufffer");
	    /*NOTREACHED*/
	}
	return 1;
    } else {
	errstr = newvstralloc(errstr,
			      "writing file: ",
			      (rc != -1) ? "short write" : strerror(errno),
			      NULL);
	wrwait = timesadd(wrwait, stopclock());
	if (interactive)
	    fputs("[WE]", stderr);
	return 0;
    }
}


static void 
cleanup(void)
{
    REMOVE_SHARED_MEMORY(); 
}


/*
 * Cleanup shared memory segments 
 */
static void 
signal_handler(
    int signum)
{
    log_add(L_INFO, "Received signal %d", signum);

    exit(1);
}


/*
 * Installing signal handlers for signal whose default action is 
 * process termination so that we can clean up shared memory
 * segments
 */
static void
install_signal_handlers(void)
{
    struct sigaction act;

    act.sa_handler = signal_handler;
    act.sa_flags = 0;
    sigemptyset(&act.sa_mask);

    signal(SIGPIPE, SIG_IGN);

    if (sigaction(SIGINT, &act, NULL) != 0) {
	error("taper: couldn't install SIGINT handler [%s]", strerror(errno));
	/*NOTREACHED*/
    }

    if (sigaction(SIGHUP, &act, NULL) != 0) {
	error("taper: couldn't install SIGHUP handler [%s]", strerror(errno));
	/*NOTREACHED*/
    }
   
    if (sigaction(SIGTERM, &act, NULL) != 0) {
	error("taper: couldn't install SIGTERM handler [%s]", strerror(errno));
	/*NOTREACHED*/
    }

    if (sigaction(SIGUSR1, &act, NULL) != 0) {
	error("taper: couldn't install SIGUSR1 handler [%s]", strerror(errno));
	/*NOTREACHED*/
    }

    if (sigaction(SIGUSR2, &act, NULL) != 0) {
	error("taper: couldn't install SIGUSR2 handler [%s]", strerror(errno));
	/*NOTREACHED*/
    }

    if (sigaction(SIGALRM, &act, NULL) != 0) {
	error("taper: couldn't install SIGALRM handler [%s]", strerror(errno));
	/*NOTREACHED*/
    }
}


/*
 * ========================================================================
 * SHARED-MEMORY BUFFER SUBSYSTEM
 *
 */

#ifdef HAVE_SYSVSHM

int shmid = -1;

char *
attach_buffers(
    size_t size)
{
    char *result;

    shmid = shmget(IPC_PRIVATE, size, IPC_CREAT|0700);
    if (shmid == -1) {
	return NULL;
    }

    result = (char *)shmat(shmid, (SHM_ARG_TYPE *)NULL, 0);

    if (result == (char *)-1) {
	int save_errno = errno;

	destroy_buffers();
	errno = save_errno;
	error("shmat: %s", strerror(errno));
	/*NOTREACHED*/
    }

    return result;
}


void
detach_buffers(
    char *bufp)
{
    if ((bufp != NULL) &&
        (shmdt((SHM_ARG_TYPE *)bufp) == -1)) {
	error("shmdt: %s", strerror(errno));
	/*NOTREACHED*/
    }
}

void
destroy_buffers(void)
{
    if (shmid == -1)
	return;	/* nothing to destroy */
    if (shmctl(shmid, IPC_RMID, NULL) == -1) {
	error("shmctl: %s", strerror(errno));
	/*NOTREACHED*/
    }
}

#else
#ifdef HAVE_MMAP

#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif

#ifndef MAP_ANON
#  ifdef MAP_ANONYMOUS			/* OSF/1-style */
#    define MAP_ANON MAP_ANONYMOUS
#  else					/* SunOS4-style */
#    define MAP_ANON 0
#    define ZERO_FILE "/dev/zero"
#  endif
#endif

int shmfd = -1;
size_t saved_size;

char *
attach_buffers(
    size_t size)
{
    char *shmbuf;

#ifdef ZERO_FILE
    shmfd = open(ZERO_FILE, O_RDWR);
    if (shmfd == -1) {
	error("attach_buffers: could not open %s: %s",
	      ZERO_FILE,
	      strerror(errno));
        /*NOTREACHED*/
    }
#endif

    saved_size = size;
    shmbuf = (char *) mmap((void *) 0,
			   size,
			   PROT_READ|PROT_WRITE,
			   MAP_ANON|MAP_SHARED,
			   shmfd, 0);

    return shmbuf;
}

void
detach_buffers(
    char *bufp)
{
    if ((bufp != NULL) && 
	(munmap((void *)bufp, saved_size) == -1)) {
	error("detach_buffers: munmap: %s", strerror(errno));
	/*NOTREACHED*/
    }

    if (shmfd != -1)
	aclose(shmfd);
}

void
destroy_buffers(void)
{
}

#else
#error: must define either HAVE_SYSVSHM or HAVE_MMAP!
#endif
#endif



/*
 * ========================================================================
 * SYNC-PIPE SUBSYSTEM
 *
 */

int getpipe, putpipe;

void
syncpipe_init(
    int rd,
    int wr)
{
    getpipe = rd;
    putpipe = wr;
}

void
syncpipe_read_error(
    ssize_t	rc,
    ssize_t	expected)
{
    char buf[sizeof(char) + sizeof(int)];

    if (rc == 0) {
	dbprintf(("syncpipe_get %s halting: Unexpected read EOF\n", procname));
	fprintf(stderr, "syncpipe_get %s halting: Unexpected read EOF\n", procname);
    } else if (rc < 0) {
	dbprintf(("syncpipe_get %s halting: Read error - %s\n",
			procname, strerror(errno)));
	fprintf(stderr, "syncpipe_get %s halting: Read error - %s\n",
			procname, strerror(errno));
    } else {
	dbprintf(("syncpipe_get %s halting: Read "
		SSIZE_T_FMT " bytes short of " SSIZE_T_FMT "\n",
		procname, (SSIZE_T_FMT_TYPE)(rc - expected),
		(SSIZE_T_FMT_TYPE)expected));
	fprintf(stderr, "syncpipe_get %s halting: Read "
		SSIZE_T_FMT " bytes short of " SSIZE_T_FMT "\n",
		procname, (SSIZE_T_FMT_TYPE)(rc - expected),
		(SSIZE_T_FMT_TYPE)expected);
    }
    /* Halt the other side if it's still alive */
    buf[0] = 'H';
    memset(&buf[1], 0, SIZEOF(int));
    if (write(putpipe, buf, SIZEOF(buf)))
	return;
}

void
syncpipe_write_error(
    ssize_t	rc,
    ssize_t	expected)
{
    char buf[sizeof(char) + sizeof(int)];

    if (rc == 0) {		/* EOF */
	dbprintf(("syncpipe %s halting: Write EOF\n", procname));
	fprintf(stderr, "syncpipe %s halting: Write EOF\n", procname);
    } else if (rc < 0) {
	dbprintf(("syncpipe %s halting: Write error - %s\n",
			procname, strerror(errno)));
	fprintf(stderr, "syncpipe %s halting: Write error - %s\n",
			procname, strerror(errno));
    } else {
	dbprintf(("syncpipe %s halting: Write "
			SSIZE_T_FMT " bytes short of " SSIZE_T_FMT "\n",
			procname, (SSIZE_T_FMT_TYPE)(rc - expected),
			(SSIZE_T_FMT_TYPE)expected));
	fprintf(stderr, "syncpipe %s halting: Write "
			SSIZE_T_FMT " bytes short of " SSIZE_T_FMT "\n",
			procname, (SSIZE_T_FMT_TYPE)(rc - expected),
			(SSIZE_T_FMT_TYPE)expected);
    }
    /* Halt the other side if it's still alive */
    buf[0] = 'H';
    memset(&buf[1], 0, SIZEOF(int));
    if (write(putpipe, buf, SIZEOF(buf)))
	return;
}

int
syncpipe_get(
    int *intp)
{
    ssize_t rc;
    char buf[SIZEOF(char) + SIZEOF(int)];

    memset(buf, 0, sizeof(buf));
    rc = fullread(getpipe, buf, SIZEOF(buf));
    if (rc != (ssize_t)sizeof(buf)) {
	syncpipe_read_error(rc, (ssize_t)sizeof(buf));
	return (-1);
    }

    if (bufdebug && *buf != 'R' && *buf != 'W') {
	fprintf(stderr,"taper: %c: getc %c\n", *procname, *buf);
	fflush(stderr);
    }

    memcpy(intp, &buf[1], SIZEOF(int));
    return (int)buf[0];
}

int
syncpipe_getint(void)
{
    ssize_t rc;
    int i = 0;

    rc = fullread(getpipe, &i, SIZEOF(i));
    if (rc != (ssize_t)sizeof(i)) {
	syncpipe_read_error(rc, (ssize_t)sizeof(i));
	return (-1);
    }

    return (i);
}


char *
syncpipe_getstr(void)
{
    ssize_t rc;
    int len;
    char *str;

    if ((len = syncpipe_getint()) <= 0) {
	fprintf(stderr, "syncpipe %s halting: Protocol error - "
			"Invalid string length (%d)\n", procname, len);
	syncpipe_put('H', 0); /* Halt the other side */
	exit(1);
	/*NOTREACHED*/
    }

    str = alloc((size_t)len);

    rc = fullread(getpipe, str, (size_t)len);
    if (rc != (ssize_t)len) {
	syncpipe_read_error(rc, (ssize_t)len);
	return (NULL);
    }
    return (str);
}


int
syncpipe_put(
    int chi,
    int intval)
{
    char buf[sizeof(char) + sizeof(int)];
    ssize_t	rc;

    buf[0] = (char)chi;
    memcpy(&buf[1], &intval, SIZEOF(int));
    if (bufdebug && buf[0] != 'R' && buf[0] != 'W') {
	fprintf(stderr,"taper: %c: putc %c\n",*procname,buf[0]);
	fflush(stderr);
    }

    rc = fullwrite(putpipe, buf, SIZEOF(buf));
    if (rc != (ssize_t)sizeof(buf)) {
	syncpipe_write_error(rc, (ssize_t)sizeof(buf));
	return (-1);
    }
    return (0);
}

int
syncpipe_putint(
    int i)
{
    ssize_t	rc;

    rc = fullwrite(putpipe, &i, SIZEOF(i));
    if (rc != (ssize_t)sizeof(i)) {
	syncpipe_write_error(rc, (ssize_t)sizeof(i));
	return (-1);
	/* NOTREACHED */
    }
    return (0);
}

int
syncpipe_putstr(
    const char *str)
{
    ssize_t n, rc;

    if(!str)
	str = "UNKNOWN syncpipe_putstr STRING";

    n = (ssize_t)strlen(str) + 1;			/* send '\0' as well */
    syncpipe_putint((int)n);

    rc = fullwrite(putpipe, str, (size_t)n);
    if (rc != n) {
	syncpipe_write_error(rc, n);
	return (-1);
    }
    return (0);
}

/*
 * ========================================================================
 * TAPE MANIPULATION SUBSYSTEM
 *
 */
int label_tape(void);

/* local functions */

/* return 0 on success              */
/* return 1 on error and set errstr */
int
label_tape(void)
{  
    char *conf_tapelist_old = NULL;
    char *result;
    static int first_call = 1;
    char *timestamp;
    char *error_msg = NULL;
    char *s, *r;
    int slot = -1;

    amfree(label);
    amfree(tapedev);
    if (taper_scan(NULL, &label, &timestamp, &tapedev, CHAR_taperscan_output_callback, &error_msg) < 0) {
	fprintf(stderr, "%s\n", error_msg);
	errstr = error_msg;
	error_msg = NULL;
	amfree(timestamp);
	return 0;
    }
    amfree(timestamp);
    if(error_msg) {
	s = error_msg; r = NULL;
	while((s=strstr(s,"slot "))) { s += 5; r=s; };
	if(r) {
	    slot = atoi(r);
	}
	amfree(error_msg);
    }
    if ((tape_fd = tape_open(tapedev, O_WRONLY)) == -1) {
	if (errno == EACCES) {
	    errstr = newstralloc(errstr,
				 "writing label: tape is write protected");
	} else {
	    errstr = newstralloc2(errstr,
				  "writing label: ", strerror(errno));
	}
	return 0;
    }

    tapefd_setinfo_length(tape_fd, tapetype_get_length(tt));

    tapefd_setinfo_datestamp(tape_fd, taper_timestamp);
    tapefd_setinfo_disk(tape_fd, label);
    result = tapefd_wrlabel(tape_fd, taper_timestamp, label, tt_blocksize);
    if (result != NULL) {
	errstr = newstralloc(errstr, result);
	return 0;
    }

    if(slot > -1) {
	fprintf(stderr, "taper: slot: %d wrote label `%s' date `%s'\n", slot,
		label, taper_timestamp);
    }
    else {
	fprintf(stderr, "taper: wrote label `%s' date `%s'\n", label,
		taper_timestamp);
    }
    fflush(stderr);

#ifdef HAVE_LIBVTBLC
    /* store time for the first volume entry */
    time(&raw_time);
    tape_timep = localtime(&raw_time);
    strftime(start_datestr, 20, "%T %D", tape_timep);
    fprintf(stderr, "taper: got vtbl start time: %s\n", start_datestr);
    fflush(stderr);
#endif /* HAVE_LIBVTBLC */

    if (strcmp(label, FAKE_LABEL) != 0) {
	if (cur_tape == 0) {
	    conf_tapelist_old = stralloc2(conf_tapelist, ".yesterday");
	} else {
	    char cur_str[NUM_STR_SIZE];

	    snprintf(cur_str, SIZEOF(cur_str), "%d", cur_tape - 1);
	    conf_tapelist_old = vstralloc(conf_tapelist,
					  ".today.", cur_str, NULL);
	}

	if (write_tapelist(conf_tapelist_old)) {
	    error("could not write tapelist: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	amfree(conf_tapelist_old);

	remove_tapelabel(label);
	add_tapelabel(taper_timestamp, label);
	if (write_tapelist(conf_tapelist)) {
	    error("could not write tapelist: %s", strerror(errno));
	    /*NOTREACHED*/
	}
    }

    log_add(L_START, "datestamp %s label %s tape %d",
	    taper_timestamp, label, cur_tape);
    if (first_call && strcmp(label, FAKE_LABEL) == 0) {
	first_call = 0;
	log_add(L_WARNING, "tapedev is %s, dumps will be thrown away", tapedev);
    }

    total_tape_used=(off_t)0;
    total_tape_fm = 0;

    return 1;
}

/* return 0 on error and set errstr */
/* return 1 on success              */
int
first_tape(
    char *new_datestamp)
{
    if ((have_changer = changer_init()) < 0) {
	error("changer initialization failed: %s", strerror(errno));
	/*NOTREACHED*/
    }
    changer_debug = 1;

    taper_timestamp = newstralloc(taper_timestamp, new_datestamp);

    if (!label_tape())
	return 0;

    filenum = 0;
    return 1;
}

int
next_tape(
    int writerror)
{
    end_tape(writerror);

    if (++cur_tape >= runtapes)
	return 0;

    if (!label_tape()) {
	return 0;
    }

    filenum = 0;
    return 1;
}


int
end_tape(
    int writerror)
{
    char *result;
    int rc = 0;

    if (tape_fd >= 0) {
	log_add(L_INFO, "tape %s kb " OFF_T_FMT " fm %d %s", 
		label,
		(OFF_T_FMT_TYPE)((total_tape_used+(off_t)1023) / (off_t)1024),
		total_tape_fm,
		writerror? errstr : "[OK]");

	fprintf(stderr, "taper: writing end marker. [%s %s kb "
		OFF_T_FMT " fm %d]\n", label,
		writerror? "ERR" : "OK",
		(OFF_T_FMT_TYPE)((total_tape_used+(off_t)1023) / (off_t)1024),
		total_tape_fm);
	fflush(stderr);
	if (! writerror) {
	    if (! write_filemark()) {
		rc = 1;
		goto common_exit;
	    }

	    result = tapefd_wrendmark(tape_fd, taper_timestamp, tt_blocksize);
	    if (result != NULL) {
		errstr = newstralloc(errstr, result);
		rc = 1;
		goto common_exit;
	    }
	}
    }

#ifdef HAVE_LINUX_ZFTAPE_H
    if (tape_fd >= 0 && is_zftape(tapedev) == 1) {
	/* rewind the tape */

	if (tapefd_rewind(tape_fd) == -1 ) {
	    errstr = newstralloc2(errstr, "rewinding tape: ", strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	/* close the tape */

	if (tapefd_close(tape_fd) == -1) {
	    errstr = newstralloc2(errstr, "closing tape: ", strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	tape_fd = -1;

#ifdef HAVE_LIBVTBLC
	/* update volume table */
	fprintf(stderr, "taper: updating volume table ...\n");
	fflush(stderr);
    
	if ((tape_fd = raw_tape_open(rawtapedev, O_RDWR)) == -1) {
	    if (errno == EACCES) {
		errstr = newstralloc(errstr,
				     "updating volume table: tape is write protected");
	    } else {
		errstr = newstralloc2(errstr,
				      "updating volume table: ", 
				      strerror(errno));
	    }
	    rc = 1;
	    goto common_exit;
	}
	/* read volume table */
	if ((num_volumes = read_vtbl(tape_fd, volumes, vtbl_buffer,
				     &first_seg, &last_seg)) == -1 ) {
	    errstr = newstralloc2(errstr,
				  "reading volume table: ", 
				  strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	/* set volume label and date for first entry */
	vtbl_no = 0;
	if (set_label(label, volumes, num_volumes, vtbl_no)) {
	    errstr = newstralloc2(errstr,
				  "setting label for entry 1: ",
				  strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	/* date of start writing this tape */
	if (set_date(start_datestr, volumes, num_volumes, vtbl_no)) {
	    errstr = newstralloc2(errstr,
				  "setting date for entry 1: ", 
				  strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	/* set volume labels and dates for backup files */
	for (vtbl_no = 1; vtbl_no <= num_volumes - 2; vtbl_no++) { 
	    fprintf(stderr,"taper: label %i: %s, date %s\n", 
		    vtbl_no,
		    vtbl_entry[vtbl_no].label,
		    vtbl_entry[vtbl_no].date);
	    fflush(stderr);
	    if (set_label(vtbl_entry[vtbl_no].label, 
			 volumes, num_volumes, vtbl_no)) {
		errstr = newstralloc2(errstr,
				      "setting label for entry i: ", 
				      strerror(errno));
		rc = 1;
		goto common_exit;
	    }
	    if (set_date(vtbl_entry[vtbl_no].date, 
			volumes, num_volumes, vtbl_no)) {
		errstr = newstralloc2(errstr,
				      "setting date for entry i: ",
				      strerror(errno));
		rc = 1;
		goto common_exit;
	    }
	}
	/* set volume label and date for last entry */
	vtbl_no = num_volumes - 1;
	if (set_label("AMANDA Tape End", volumes, num_volumes, vtbl_no)) {
	    errstr = newstralloc2(errstr,
				  "setting label for last entry: ", 
				  strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	datestr = NULL; /* take current time */ 
	if (set_date(datestr, volumes, num_volumes, vtbl_no)) {
	    errstr = newstralloc2(errstr,
				  "setting date for last entry 1: ", 
				  strerror(errno));
	    rc = 1;
	    goto common_exit;
	}
	/* write volume table back */
	if (write_vtbl(tape_fd, volumes, vtbl_buffer, num_volumes, first_seg,
		       op_mode == trunc)) {
	    errstr = newstralloc2(errstr,
				  "writing volume table: ", 
				  strerror(errno));
	    rc = 1;
	    goto common_exit;
	}  

	fprintf(stderr, "taper: updating volume table: done.\n");
	fflush(stderr);
#endif /* HAVE_LIBVTBLC */
    }
#endif /* !HAVE_LINUX_ZFTAPE_H */

    /* close the tape and let the OS write the final filemarks */

common_exit:

    if (tape_fd >= 0 && tapefd_close(tape_fd) == -1 && ! writerror) {
	errstr = newstralloc2(errstr, "closing tape: ", strerror(errno));
	rc = 1;
    }
    tape_fd = -1;
    amfree(label);

    return rc;
}


int
write_filemark(void)
{
    if (tapefd_weof(tape_fd, (off_t)1) == -1) {
	errstr = newstralloc2(errstr, "writing filemark: ", strerror(errno));
	return 0;
    }
    total_tape_fm++;
    return 1;
}
