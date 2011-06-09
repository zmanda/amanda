/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
/* $Id: chunker.c,v 1.36 2006/08/24 11:23:32 martinea Exp $
 *
 * requests remote amandad processes to dump filesystems
 */
#include "amanda.h"
#include "clock.h"
#include "conffile.h"
#include "event.h"
#include "logfile.h"
#include "packet.h"
#include "protocol.h"
#include "security.h"
#include "stream.h"
#include "fileheader.h"
#include "amfeatures.h"
#include "server_util.h"
#include "util.h"
#include "holding.h"
#include "timestamp.h"
#include "sockaddr-util.h"

#ifndef SEEK_SET
#define SEEK_SET 0
#endif

#ifndef SEEK_CUR
#define SEEK_CUR 1
#endif

#define CONNECT_TIMEOUT	5*60

#define STARTUP_TIMEOUT 60

struct databuf {
    int fd;			/* file to flush to */
    char *filename;		/* name of what fd points to */
    int filename_seq;		/* for chunking */
    off_t split_size;		/* when to chunk */
    off_t chunk_size;		/* size of each chunk */
    off_t use;			/* size to use on this disk */
    char buf[DISK_BLOCK_BYTES];
    char *datain;		/* data buffer markers */
    char *dataout;
    char *datalimit;
};

static char *handle = NULL;

static char *errstr = NULL;
static int abort_pending;
static off_t dumpsize;
static unsigned long headersize;
static off_t dumpbytes;
static off_t filesize;

static char *hostname = NULL;
static char *diskname = NULL;
static char *qdiskname = NULL;
static char *options = NULL;
static char *progname = NULL;
static int level;
static char *dumpdate = NULL;
static struct cmdargs *command_in_transit = NULL;
static char *chunker_timestamp = NULL;

static dumpfile_t file;

/* local functions */
int main(int, char **);
static ssize_t write_tapeheader(int, dumpfile_t *);
static void databuf_init(struct databuf *, int, char *, off_t, off_t);
static int databuf_flush(struct databuf *);

static int startup_chunker(char *, off_t, off_t, struct databuf *, int *, int *);
static int do_chunk(int, struct databuf *, int, int);

/* we use a function pointer for full_write, so that we can "shim" in
 * full_write_with_fake_enospc for testing */
static size_t (*db_full_write)(int fd, const void *buf, size_t count);
static size_t full_write_with_fake_enospc(int fd, const void *buf, size_t count);
static off_t fake_enospc_at_byte = -1;

int
main(
    int		argc,
    char **	argv)
{
    static struct databuf db;
    struct cmdargs *cmdargs;
    int header_fd;
    char *q = NULL;
    char *filename = NULL;
    off_t chunksize, use;
    times_t runtime;
    am_feature_t *their_features = NULL;
    int a;
    config_overrides_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;
    char *m;
    char *env;
    int header_socket;
    int data_socket;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("chunker");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_trace_log);

    cfg_ovr = extract_commandline_config_overrides(&argc, &argv);

    if (argc > 1) 
	cfg_opt = argv[1];

    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    safe_cd(); /* do this *after* config_init() */

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
    g_fprintf(stderr,
	    _("%s: pid %ld executable %s version %s\n"),
	    get_pname(), (long) getpid(),
	    argv[0], VERSION);
    fflush(stderr);

    /* now, make sure we are a valid user */

    signal(SIGPIPE, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);

    cmdargs = getcmd();
    if(cmdargs->cmd == START) {
	if(cmdargs->argc <= 1)
	    error(_("error [dumper START: not enough args: timestamp]"));
	g_free(chunker_timestamp);
	chunker_timestamp = g_strdup(cmdargs->argv[1]);
    }
    else {
	log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
	error(_("Didn't get START command"));
    }
    free_cmdargs(cmdargs);

    /* set up a fake ENOSPC for testing purposes.  Note that this counts
     * headers as well as data written to disk. */
    env = getenv("CHUNKER_FAKE_ENOSPC_AT");
    if (env) {
	fake_enospc_at_byte = (off_t)atoi(env); /* these values are never > MAXINT */
	db_full_write = full_write_with_fake_enospc;
	g_debug("will trigger fake ENOSPC at byte %d", (int)fake_enospc_at_byte);
    } else {
	db_full_write = full_write;
    }

/*    do {*/
	cmdargs = getcmd();

	switch(cmdargs->cmd) {
	case QUIT:
	    break;

	case PORT_WRITE:
	    /*
	     * PORT-WRITE
	     *   handle
	     *   filename
	     *   host
	     *   features
	     *   disk
	     *   level
	     *   dumpdate
	     *   chunksize
	     *   progname
	     *   use
	     *   options
	     */
	    a = 1;

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: handle]"));
		/*NOTREACHED*/
	    }
	    g_free(handle);
	    handle = g_strdup(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: filename]"));
		/*NOTREACHED*/
	    }
	    g_free(filename);
	    filename = g_strdup(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: hostname]"));
		/*NOTREACHED*/
	    }
	    g_free(hostname);
	    hostname = g_strdup(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: features]"));
		/*NOTREACHED*/
	    }
	    am_release_feature_set(their_features);
	    their_features = am_string_to_feature(cmdargs->argv[a++]);
	    if (!their_features) {
		error(_("error [chunker PORT-WRITE: invalid feature string]"));
		/*NOTREACHED*/
	    }

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: diskname]"));
		/*NOTREACHED*/
	    }
	    g_free(diskname);
	    diskname = g_strdup(cmdargs->argv[a++]);
	    if (qdiskname)
		amfree(qdiskname);
	    qdiskname = quote_string(diskname); /* qdiskname is a global */

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: level]"));
		/*NOTREACHED*/
	    }
	    level = atoi(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: dumpdate]"));
		/*NOTREACHED*/
	    }
	    g_free(dumpdate);
	    dumpdate = g_strdup(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: chunksize]"));
		/*NOTREACHED*/
	    }
	    chunksize = OFF_T_ATOI(cmdargs->argv[a++]);
	    chunksize = am_floor(chunksize, (off_t)DISK_BLOCK_KB);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: progname]"));
		/*NOTREACHED*/
	    }
	    g_free(progname);
	    progname = g_strdup(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: use]"));
		/*NOTREACHED*/
	    }
	    use = am_floor(OFF_T_ATOI(cmdargs->argv[a++]), DISK_BLOCK_KB);

	    if(a >= cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: not enough args: options]"));
		/*NOTREACHED*/
	    }
	    g_free(options);
	    options = g_strdup(cmdargs->argv[a++]);

	    if(a != cmdargs->argc) {
		error(_("error [chunker PORT-WRITE: too many args: %d != %d]"),
		      cmdargs->argc, a);
	        /*NOTREACHED*/
	    }

	    if ((header_fd = startup_chunker(filename, use, chunksize, &db,
					     &header_socket, &data_socket)) < 0) {
		q = quote_string(g_strdup_printf(_("[chunker startup failed: %s]"), errstr));
		putresult(TRYAGAIN, "%s %s\n", handle, q);
		error("startup_chunker failed: %s", errstr);
	    }
	    command_in_transit = NULL;
	    if (header_fd >= 0 && do_chunk(header_fd, &db, header_socket, data_socket)) {
		char kb_str[NUM_STR_SIZE];
		char kps_str[NUM_STR_SIZE];
		double rt;

		runtime = stopclock();
                rt = g_timeval_to_double(runtime);
		g_snprintf(kb_str, sizeof(kb_str), "%lld",
			 (long long)(dumpsize - (off_t)headersize));
		g_snprintf(kps_str, sizeof(kps_str), "%3.1lf",
				isnormal(rt) ? (double)dumpsize / rt : 0.0);
		g_free(errstr);
		errstr = g_strdup_printf("sec %s kb %s kps %s",
                                         walltime_str(runtime), kb_str, kps_str);
		m = g_strdup_printf("[%s]", errstr);
		q = quote_string(m);
		amfree(m);
		free_cmdargs(cmdargs);
		if(command_in_transit != NULL) {
		    cmdargs = command_in_transit;
		    command_in_transit = NULL;
		} else {
		    cmdargs = getcmd();
		}
		switch(cmdargs->cmd) {
		case DONE:
		    putresult(DONE, "%s %lld %s\n", handle,
			     (long long)(dumpsize - (off_t)headersize), q);
		    log_add(L_SUCCESS, "%s %s %s %d [%s]",
			    hostname, qdiskname, chunker_timestamp, level, errstr);
		    break;
		case BOGUS:
		case TRYAGAIN:
		case FAILED:
		case ABORT_FINISHED:
		    if(dumpsize > (off_t)DISK_BLOCK_KB) {
			putresult(PARTIAL, "%s %lld %s\n", handle,
				 (long long)(dumpsize - (off_t)headersize),
				 q);
			log_add(L_PARTIAL, "%s %s %s %d [%s]",
				hostname, qdiskname, chunker_timestamp, level, errstr);
		    }
		    else {
			g_free(errstr);
			errstr = g_strdup_printf(_("dumper returned %s"),
                                                 cmdstr[cmdargs->cmd]);
			amfree(q);
			m = g_strdup_printf("[%s]",errstr);
			q = quote_string(m);
			amfree(m);
			putresult(FAILED, "%s %s\n", handle, q);
			log_add(L_FAIL, "%s %s %s %d [%s]",
				hostname, qdiskname, chunker_timestamp, level, errstr);
		    }
		default: break;
		}
		amfree(q);
	    } else if (header_fd != -2) {
		if(q == NULL) {
		    m = g_strdup_printf("[%s]", errstr);
		    q = quote_string(m);
		    amfree(m);
		}
		if(!abort_pending) {
		    putresult(FAILED, "%s %s\n", handle, q);
		}
		log_add(L_FAIL, "%s %s %s %d [%s]",
			hostname, qdiskname, chunker_timestamp, level, errstr);
		amfree(q);
	    }
	    amfree(filename);
	    amfree(db.filename);
	    break;

	default:
	    if(cmdargs->argc >= 1) {
		q = quote_string(cmdargs->argv[0]);
	    } else {
		q = g_strdup(_("(no input?)"));
	    }
	    putresult(BAD_COMMAND, "%s\n", q);
	    amfree(q);
	    break;
	}

/*    } while(cmdargs->cmd != QUIT); */

    log_add(L_INFO, "pid-done %ld", (long)getpid());

    amfree(errstr);
    amfree(chunker_timestamp);
    amfree(handle);
    amfree(hostname);
    amfree(diskname);
    amfree(qdiskname);
    amfree(dumpdate);
    amfree(progname);
    amfree(options);
    free_cmdargs(cmdargs);
    if (command_in_transit)
	free_cmdargs(command_in_transit);
    am_release_feature_set(their_features);
    their_features = NULL;

    dbclose();

    return (0); /* exit */
}

/*
 * Returns a file descriptor to the incoming port
 * on success, or -1 on error.
 */
static int
startup_chunker(
    char *		filename,
    off_t		use,
    off_t		chunksize,
    struct databuf *	db,
    int                *headersocket,
    int                *datasocket)
{
    int header_fd, outfd;
    char *tmp_filename, *pc;
    in_port_t header_port, data_port;
    int header_socket, data_socket;
    int result;
    struct addrinfo *res;
    struct addrinfo *res_addr;
    sockaddr_union  *addr = NULL;
    sockaddr_union   data_addr;

    header_port = 0;
    data_port = 0;
    if ((result = resolve_hostname("localhost", 0, &res, NULL) != 0)) {
	g_free(errstr);
	errstr = g_strdup_printf(_("could not resolve localhost: %s"),
                                 gai_strerror(result));
	return -1;
    }
    for (res_addr = res; res_addr != NULL; res_addr = res_addr->ai_next) {
	g_debug("ra: %s\n", str_sockaddr((sockaddr_union*)res_addr->ai_addr));
	if (res_addr->ai_family == AF_INET) {
	    addr = (sockaddr_union *)res_addr->ai_addr;
	    break;
	}
    }
    if (!addr) {
	addr = (sockaddr_union *)res->ai_addr;
	g_debug("addr: %s\n", str_sockaddr(addr));
    }

    header_socket = stream_server(SU_GET_FAMILY(addr), &header_port, 0,
				STREAM_BUFSIZE, 0);
    data_socket = stream_server(SU_GET_FAMILY(addr), &data_port, 0,
				STREAM_BUFSIZE, 0);
    copy_sockaddr(&data_addr, addr);

    SU_SET_PORT(&data_addr, data_port);

    if (res) freeaddrinfo(res);

    if (header_socket < 0) {
	errstr = g_strdup_printf(_("error creating header stream server: %s"), strerror(errno));
	aclose(data_socket);
	return -1;
    }

    if (data_socket < 0) {
	errstr = g_strdup_printf(_("error creating data stream server: %s"), strerror(errno));
	aclose(header_socket);
	return -1;
    }

    putresult(PORT, "%d %s\n", header_port, str_sockaddr(&data_addr));

    header_fd = stream_accept(header_socket, CONNECT_TIMEOUT, 0,
			      STREAM_BUFSIZE);
    if (header_fd == -1) {
	errstr = g_strdup_printf(_("error accepting header stream: %s"),
			    strerror(errno));
	aclose(header_socket);
	aclose(data_socket);
	return -1;
    }

    tmp_filename = g_strjoin(NULL, filename, ".tmp", NULL);
    pc = strrchr(tmp_filename, '/');
    g_assert(pc != NULL);
    *pc = '\0';
    mkholdingdir(tmp_filename);
    *pc = '/';
    if ((outfd = open(tmp_filename, O_RDWR|O_CREAT|O_TRUNC, 0600)) < 0) {
	int save_errno = errno;
	char *m = g_strdup_printf(_("holding file \"%s\": %s"),
			 tmp_filename,
			 strerror(errno));

	errstr = quote_string(m);
	amfree(m);
	amfree(tmp_filename);
	aclose(header_fd);
	aclose(header_socket);
	aclose(data_socket);
	if(save_errno == ENOSPC) {
	    putresult(NO_ROOM, "%s %lld\n",
	    	      handle, (long long)use);
	    return -2;
	} else {
	    return -1;
	}
    }
    amfree(tmp_filename);
    databuf_init(db, outfd, filename, use, chunksize);
    db->filename_seq++;
    *headersocket = header_socket;
    *datasocket = data_socket;
    return header_fd;
}

static int
do_chunk(
    int			header_fd,
    struct databuf *	db,
    int                 header_socket,
    int                 data_socket)
{
    size_t nread;
    int data_fd, read_error;
    char header_buf[DISK_BLOCK_BYTES];

    startclock();

    dumpsize = dumpbytes = filesize = (off_t)0;
    headersize = 0;
    memset(header_buf, 0, sizeof(header_buf));

    /*
     * The first thing we should receive is the file header, which we
     * need to save into "file", as well as write out.  Later, the
     * chunk code will rewrite it.
     */
    nread = read_fully(header_fd, header_buf, sizeof(header_buf), &read_error);
    aclose(header_fd);
    aclose(header_socket);
    if (nread != sizeof(header_buf)) {
	if(read_error) {
	    errstr = g_strdup_printf(_("cannot read header: %s"), strerror(read_error));
	} else {
	    errstr = g_strdup_printf(_("cannot read header: got %zd bytes instead of %zd"),
				nread, sizeof(header_buf));
	}
	aclose(data_socket);
	return 0;
    }
    parse_file_header(header_buf, &file, (size_t)nread);
    if(write_tapeheader(db->fd, &file)) {
	int save_errno = errno;
	char *m = g_strdup_printf(_("write_tapeheader file %s: %s"),
			 db->filename, strerror(errno));
	errstr = quote_string(m);
	amfree(m);
	if(save_errno == ENOSPC) {
	    putresult(NO_ROOM, "%s %lld\n", handle, 
		      (long long)(db->use+db->split_size-dumpsize));
	}
	aclose(data_socket);
	return 0;
    }
    dumpsize += (off_t)DISK_BLOCK_KB;
    filesize = (off_t)DISK_BLOCK_KB;
    headersize += DISK_BLOCK_KB;

    /* open the data socket */
    data_fd = stream_accept(data_socket, CONNECT_TIMEOUT, 0, STREAM_BUFSIZE);

    if (data_fd == -1) {
	errstr = g_strdup_printf(_("error accepting data stream: %s"),
			    strerror(errno));
	aclose(data_socket);
	return 0;
    }

    /*
     * We've written the file header.  Now, just write data until the
     * end.
     */
    while ((nread = read_fully(data_fd, db->buf,
			     (size_t)(db->datalimit - db->datain), NULL)) > 0) {
	db->datain += nread;
	while(db->dataout < db->datain) {
	    if(!databuf_flush(db)) {
		aclose(data_fd);
		aclose(data_socket);
		return 0;
	    }
	}
    }
    while(db->dataout < db->datain) {
	if(!databuf_flush(db)) {
	    aclose(data_fd);
	    aclose(data_socket);
	    return 0;
	}
    }
    if(dumpbytes > (off_t)0) {
	dumpsize += (off_t)1;			/* count partial final KByte */
	filesize += (off_t)1;
    }
    aclose(data_fd);
    aclose(data_socket);
    return 1;
}

/*
 * Initialize a databuf.  Takes a writeable file descriptor.
 */
static void
databuf_init(
    struct databuf *	db,
    int			fd,
    char *		filename,
    off_t		use,
    off_t		chunk_size)
{
    db->fd = fd;
    db->filename = g_strdup(filename);
    db->filename_seq = (off_t)0;
    db->chunk_size = chunk_size;
    db->split_size = (db->chunk_size > use) ? use : db->chunk_size;
    db->use = (use > db->split_size) ? use - db->split_size : (off_t)0;
    db->datain = db->dataout = db->buf;
    db->datalimit = db->buf + sizeof(db->buf);
}


/*
 * Write out the buffer to the backing file
 */
static int
databuf_flush(
    struct databuf *	db)
{
    struct cmdargs *cmdargs = NULL;
    int rc = 1;
    size_t size_to_write;
    size_t written;
    off_t left_in_chunk;
    char *arg_filename = NULL;
    char *new_filename = NULL;
    char *tmp_filename = NULL;
    char sequence[NUM_STR_SIZE];
    int newfd;
    filetype_t save_type;
    char *q;
    int a;
    char *pc;

    /*
     * If there's no data, do nothing.
     */
    if (db->dataout >= db->datain) {
	goto common_exit;
    }

    /*
     * See if we need to split this file.
     */
    while (db->split_size > (off_t)0 && dumpsize >= db->split_size) {
	if( db->use == (off_t)0 ) {
	    /*
	     * Probably no more space on this disk.  Request some more.
	     */
	    putresult(RQ_MORE_DISK, "%s\n", handle);
	    cmdargs = getcmd();
	    if(command_in_transit == NULL &&
	       (cmdargs->cmd == DONE || cmdargs->cmd == TRYAGAIN || cmdargs->cmd == FAILED)) {
		command_in_transit = cmdargs;
		cmdargs = getcmd();
	    }
	    if(cmdargs->cmd == CONTINUE) {
		/*
		 * CONTINUE
		 *   serial
		 *   filename
		 *   chunksize
		 *   use
		 */
		a = 2; /* skip CONTINUE and serial */

		if(a >= cmdargs->argc) {
		    error(_("error [chunker CONTINUE: not enough args: filename]"));
		    /*NOTREACHED*/
		}
		g_free(arg_filename);
		arg_filename = g_strdup(cmdargs->argv[a++]);

		if(a >= cmdargs->argc) {
		    error(_("error [chunker CONTINUE: not enough args: chunksize]"));
		    /*NOTREACHED*/
		}
		db->chunk_size = OFF_T_ATOI(cmdargs->argv[a++]);
		db->chunk_size = am_floor(db->chunk_size, (off_t)DISK_BLOCK_KB);

		if(a >= cmdargs->argc) {
		    error(_("error [chunker CONTINUE: not enough args: use]"));
		    /*NOTREACHED*/
		}
		db->use = OFF_T_ATOI(cmdargs->argv[a++]);

		if(a != cmdargs->argc) {
		    error(_("error [chunker CONTINUE: too many args: %d != %d]"),
			  cmdargs->argc, a);
		    /*NOTREACHED*/
		}

		if(g_str_equal(db->filename, arg_filename)) {
		    /*
		     * Same disk, so use what room is left up to the
		     * next chunk boundary or the amount we were given,
		     * whichever is less.
		     */
		    left_in_chunk = db->chunk_size - filesize;
		    if(left_in_chunk > db->use) {
			db->split_size += db->use;
			db->use = (off_t)0;
		    } else {
			db->split_size += left_in_chunk;
			db->use -= left_in_chunk;
		    }
		    if(left_in_chunk > (off_t)0) {
			/*
			 * We still have space in this chunk.
			 */
			break;
		    }
		} else {
		    /*
		     * Different disk, so use new file.
		     */
		    g_free(db->filename);
		    db->filename = g_strdup(arg_filename);
		}
	    } else if(cmdargs->cmd == ABORT) {
		abort_pending = 1;
		g_free(errstr);
		errstr = g_strdup(cmdargs->argv[1]);
		putresult(ABORT_FINISHED, "%s\n", handle);
		rc = 0;
		goto common_exit;
	    } else {
		if(cmdargs->argc >= 1) {
		    q = quote_string(cmdargs->argv[0]);
		} else {
		    q = g_strdup(_("(no input?)"));
		}
		error(_("error [bad command after RQ-MORE-DISK: \"%s\"]"), q);
		/*NOTREACHED*/
	    }
	}

	/*
	 * Time to use another file.
	 */

	/*
	 * First, open the new chunk file, and give it a new header
	 * that has no cont_filename pointer.
	 */
	g_snprintf(sequence, sizeof(sequence), "%d", db->filename_seq);

        g_free(new_filename);
        new_filename = g_strconcat(db->filename, ".", sequence, NULL);

        g_free(tmp_filename);
        tmp_filename = g_strconcat(new_filename, ".tmp", NULL);

        pc = strrchr(tmp_filename, '/');
        g_assert(pc != NULL); /* Only a problem if db->filename has no /. */
	*pc = '\0';
	mkholdingdir(tmp_filename);
	*pc = '/';
	newfd = open(tmp_filename, O_RDWR|O_CREAT|O_TRUNC, 0600);
	if (newfd == -1) {
	    int save_errno = errno;
	    char *m;

	    if(save_errno == ENOSPC) {
		putresult(NO_ROOM, "%s %lld\n", handle, 
			  (long long)(db->use+db->split_size-dumpsize));
		db->use = (off_t)0;			/* force RQ_MORE_DISK */
		db->split_size = dumpsize;
		continue;
	    }
	    m = g_strdup_printf(_("creating chunk holding file \"%s\": %s"),
			     tmp_filename,
			     strerror(errno));
	    errstr = quote_string(m);
	    amfree(m);
	    aclose(db->fd);
	    rc = 0;
	    goto common_exit;
	}
	save_type = file.type;
	file.type = F_CONT_DUMPFILE;
	file.cont_filename[0] = '\0';
	if(write_tapeheader(newfd, &file)) {
	    int save_errno = errno;
	    char *m;

	    aclose(newfd);
	    if(save_errno == ENOSPC) {
		if (unlink(tmp_filename) < 0) {
		    g_debug("could not delete '%s'; ignoring", tmp_filename);
		}
		putresult(NO_ROOM, "%s %lld\n", handle, 
			  (long long)(db->use+db->split_size-dumpsize));
		db->use = (off_t)0;			/* force RQ_MORE DISK */
		db->split_size = dumpsize;
		file.type = save_type;
		continue;
	    }
	    m = g_strdup_printf(_("write_tapeheader file %s: %s"),
			     tmp_filename,
			     strerror(errno));
	    errstr = quote_string(m);
	    amfree(m);
	    rc = 0;
	    goto common_exit;
	}

	/*
	 * Now, update the header of the current file to point
	 * to the next chunk, and then close it.
	 */
	if (lseek(db->fd, (off_t)0, SEEK_SET) < (off_t)0) {
	    char *m = g_strdup_printf(_("lseek holding file %s: %s"),
			     db->filename,
			     strerror(errno));
	    errstr = quote_string(m);
	    amfree(m);
	    aclose(newfd);
	    rc = 0;
	    goto common_exit;
	}

	file.type = save_type;
	strncpy(file.cont_filename, new_filename, sizeof(file.cont_filename));
	file.cont_filename[sizeof(file.cont_filename)-1] = '\0';
	if(write_tapeheader(db->fd, &file)) {
	    char * m = g_strdup_printf(_("write_tapeheader file \"%s\": %s"),
			     db->filename,
			     strerror(errno));
	    errstr = quote_string(m);
	    amfree(m);
	    aclose(newfd);
	    unlink(tmp_filename);
	    rc = 0;
	    goto common_exit;
	}
	file.type = F_CONT_DUMPFILE;

	/*
	 * Now shift the file descriptor.
	 */
	aclose(db->fd);
	db->fd = newfd;
	newfd = -1;

	/*
	 * Update when we need to chunk again
	 */
	if(db->use <= (off_t)DISK_BLOCK_KB) {
	    /*
	     * Cheat and use one more block than allowed so we can make
	     * some progress.
	     */
	    db->split_size += (off_t)(2 * DISK_BLOCK_KB);
	    db->use = (off_t)0;
	} else if(db->chunk_size > db->use) {
	    db->split_size += db->use;
	    db->use = (off_t)0;
	} else {
	    db->split_size += db->chunk_size;
	    db->use -= db->chunk_size;
	}


	amfree(tmp_filename);
	amfree(new_filename);
	dumpsize += (off_t)DISK_BLOCK_KB;
	filesize = (off_t)DISK_BLOCK_KB;
	headersize += DISK_BLOCK_KB;
	db->filename_seq++;
    }

    /*
     * Write out the buffer
     */
    size_to_write = (size_t)(db->datain - db->dataout);
    written = db_full_write(db->fd, db->dataout, size_to_write);
    if (written > 0) {
	db->dataout += written;
	dumpbytes += (off_t)written;
    }
    dumpsize += (dumpbytes / (off_t)1024);
    filesize += (dumpbytes / (off_t)1024);
    dumpbytes %= 1024;
    if (written < size_to_write) {
	if (errno != ENOSPC) {
	    char *m = g_strdup_printf(_("data write: %s"), strerror(errno));
	    errstr = quote_string(m);
	    amfree(m);
	    rc = 0;
	    goto common_exit;
	}

	/*
	 * NO-ROOM is informational only.  Later, RQ_MORE_DISK will be
	 * issued to use another holding disk.
	 */
	putresult(NO_ROOM, "%s %lld\n", handle,
		  (long long)(db->use+db->split_size-dumpsize));
	db->use = (off_t)0;				/* force RQ_MORE_DISK */
	db->split_size = dumpsize;
	goto common_exit;
    }
    if (db->datain == db->dataout) {
	/*
	 * We flushed the whole buffer so reset to use it all.
	 */
	db->datain = db->dataout = db->buf;
    }

common_exit:

    if (cmdargs)
	free_cmdargs(cmdargs);
    amfree(new_filename);
    /*@i@*/ amfree(tmp_filename);
    amfree(arg_filename);
    return rc;
}


/*
 * Send an Amanda dump header to the output file and set file->blocksize
 */
static ssize_t
write_tapeheader(
    int		outfd,
    dumpfile_t *file)
{
    char *buffer;
    size_t written;

    file->blocksize = DISK_BLOCK_BYTES;
    if (debug_chunker > 1)
	dump_dumpfile_t(file);
    buffer = build_header(file, NULL, DISK_BLOCK_BYTES);
    if (!buffer) /* this shouldn't happen */
	error(_("header does not fit in %zd bytes"), (size_t)DISK_BLOCK_BYTES);

    written = db_full_write(outfd, buffer, DISK_BLOCK_BYTES);
    amfree(buffer);
    if(written == DISK_BLOCK_BYTES) return 0;

    /* fake ENOSPC when we get a short write without errno set */
    if(errno == 0)
	errno = ENOSPC;

    return (ssize_t)-1;
}

static size_t
full_write_with_fake_enospc(
    int fd,
    const void *buf,
    size_t count)
{
    size_t rc;

    //g_debug("HERE %zd %zd", count, (size_t)fake_enospc_at_byte);

    if (count <= (size_t)fake_enospc_at_byte) {
	fake_enospc_at_byte -= count;
	return full_write(fd, buf, count);
    }

    /* if we get here, the caller has requested a size that is less
     * than fake_enospc_at_byte. */
    count = fake_enospc_at_byte;
    g_debug("returning fake ENOSPC");

    if (fake_enospc_at_byte) {
	rc = full_write(fd, buf, fake_enospc_at_byte);
	if (rc == (size_t)fake_enospc_at_byte) {
	    /* full_write succeeded, so fake a failure */
	    errno = ENOSPC;
	}
    } else {
	/* no bytes to write; just fake an error */
	errno = ENOSPC;
	rc = 0;
    }

    /* switch back to calling full_write directly */
    fake_enospc_at_byte = -1;
    db_full_write = full_write;
    return rc;
}
