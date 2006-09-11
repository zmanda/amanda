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
/* $Id: dumper.c,v 1.190 2006/08/30 19:53:57 martinea Exp $
 *
 * requests remote amandad processes to dump filesystems
 */
#include "amanda.h"
#include "amindex.h"
#include "arglist.h"
#include "clock.h"
#include "conffile.h"
#include "event.h"
#include "logfile.h"
#include "packet.h"
#include "protocol.h"
#include "security.h"
#include "stream.h"
#include "token.h"
#include "version.h"
#include "fileheader.h"
#include "amfeatures.h"
#include "server_util.h"
#include "util.h"

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
    char *buf;
    char *datain;		/* data buffer markers */
    char *dataout;
    char *datalimit;
    pid_t compresspid;		/* valid if fd is pipe to compress */
    pid_t encryptpid;		/* valid if fd is pipe to encrypt */
};

static char *handle = NULL;

static char *errstr = NULL;
static off_t dumpbytes;
static off_t dumpsize, headersize, origsize;

static comp_t srvcompress = COMP_NONE;
char *srvcompprog = NULL;
char *clntcompprog = NULL;

static encrypt_t srvencrypt = ENCRYPT_NONE;
char *srv_encrypt = NULL;
char *clnt_encrypt = NULL;
char *srv_decrypt_opt = NULL;
char *clnt_decrypt_opt = NULL;

static FILE *errf = NULL;
static char *hostname = NULL;
am_feature_t *their_features = NULL;
static char *diskname = NULL;
static char *qdiskname = NULL;
static char *device = NULL;
static char *options = NULL;
static char *progname = NULL;
static char *amandad_path=NULL;
static char *client_username=NULL;
static char *ssh_keys=NULL;
static int level;
static char *dumpdate = NULL;
static char *dumper_timestamp = NULL;
static time_t conf_dtimeout;
static int indexfderror;
static int set_datafd;

static dumpfile_t file;

static struct {
    const char *name;
    security_stream_t *fd;
} streams[] = {
#define	DATAFD	0
    { "DATA", NULL },
#define	MESGFD	1
    { "MESG", NULL },
#define	INDEXFD	2
    { "INDEX", NULL },
};
#define	NSTREAMS	(int)(sizeof(streams) / sizeof(streams[0]))

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;

/* local functions */
int		main(int, char **);
static int	do_dump(struct databuf *);
static void	check_options(char *);
static void	finish_tapeheader(dumpfile_t *);
static ssize_t	write_tapeheader(int, dumpfile_t *);
static void	databuf_init(struct databuf *, int);
static int	databuf_write(struct databuf *, const void *, size_t);
static int	databuf_flush(struct databuf *);
static void	process_dumpeof(void);
static void	process_dumpline(const char *);
static void	add_msg_data(const char *, size_t);
static void	parse_info_line(char *);
static void	log_msgout(logtype_t);
static char *	dumper_get_security_conf (char *, void *);

static int	runcompress(int, pid_t *, comp_t);
static int	runencrypt(int, pid_t *,  encrypt_t);

static void	sendbackup_response(void *, pkt_t *, security_handle_t *);
static int	startup_dump(const char *, const char *, const char *, int,
			const char *, const char *, const char *,
			const char *, const char *, const char *);
static void	stop_dump(void);

static void	read_indexfd(void *, void *, ssize_t);
static void	read_datafd(void *, void *, ssize_t);
static void	read_mesgfd(void *, void *, ssize_t);
static void	timeout(time_t);
static void	timeout_callback(void *);

static void
check_options(
    char *options)
{
  char *compmode = NULL;
  char *compend  = NULL;
  char *encryptmode = NULL;
  char *encryptend = NULL;
  char *decryptmode = NULL;
  char *decryptend = NULL;

    /* parse the compression option */
  if (strstr(options, "srvcomp-best;") != NULL) 
      srvcompress = COMP_BEST;
    else if (strstr(options, "srvcomp-fast;") != NULL)
      srvcompress = COMP_FAST;
    else if ((compmode = strstr(options, "srvcomp-cust=")) != NULL) {
	compend = strchr(compmode, ';');
	if (compend ) {
	    srvcompress = COMP_SERV_CUST;
	    *compend = '\0';
	    srvcompprog = stralloc(compmode + strlen("srvcomp-cust="));
	    *compend = ';';
	}
    } else if ((compmode = strstr(options, "comp-cust=")) != NULL) {
	compend = strchr(compmode, ';');
	if (compend) {
	    srvcompress = COMP_CUST;
	    *compend = '\0';
	    clntcompprog = stralloc(compmode + strlen("comp-cust="));
	    *compend = ';';
	}
    }
    else {
      srvcompress = COMP_NONE;
    }
    

    /* now parse the encryption option */
    if ((encryptmode = strstr(options, "encrypt-serv-cust=")) != NULL) {
      encryptend = strchr(encryptmode, ';');
      if (encryptend) {
	    srvencrypt = ENCRYPT_SERV_CUST;
	    *encryptend = '\0';
	    srv_encrypt = stralloc(encryptmode + strlen("encrypt-serv-cust="));
	    *encryptend = ';';
      }
    } else if ((encryptmode = strstr(options, "encrypt-cust=")) != NULL) {
      encryptend = strchr(encryptmode, ';');
      if (encryptend) {
	    srvencrypt = ENCRYPT_CUST;
	    *encryptend = '\0';
	    clnt_encrypt = stralloc(encryptmode + strlen("encrypt-cust="));
	    *encryptend = ';';
      }
    } else {
      srvencrypt = ENCRYPT_NONE;
    }
    /* get the decryption option parameter */
    if ((decryptmode = strstr(options, "server-decrypt-option=")) != NULL) {
      decryptend = strchr(decryptmode, ';');
      if (decryptend) {
	*decryptend = '\0';
	srv_decrypt_opt = stralloc(decryptmode + strlen("server-decrypt-option="));
	*decryptend = ';';
      }
    } else if ((decryptmode = strstr(options, "client-decrypt-option=")) != NULL) {
      decryptend = strchr(decryptmode, ';');
      if (decryptend) {
	*decryptend = '\0';
	clnt_decrypt_opt = stralloc(decryptmode + strlen("client-decrypt-option="));
	*decryptend = ';';
      }
    }
}


int
main(
    int		main_argc,
    char **	main_argv)
{
    static struct databuf db;
    struct cmdargs cmdargs;
    cmd_t cmd;
    int outfd = -1;
    int rc;
    in_port_t taper_port;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    char *conffile;
    char *q = NULL;
    int a;
    uid_t ruid;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

    safe_fd(-1, 0);

    set_pname("dumper");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    erroutput_type = (ERR_AMANDALOG|ERR_INTERACTIVE);
    set_logerror(logerror);

    parse_server_conf(main_argc, main_argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    if (my_argc > 1) {
	config_name = stralloc(my_argv[1]);
	config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
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

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if(read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();
    /*
     * Make our effective uid nonprivlidged, keeping save uid as root
     * in case we need to get back (to bind privlidged ports, etc).
     */
    ruid = getuid();
    if(geteuid() == 0) {
	seteuid(ruid);
	setgid(getgid());
    }
#if defined BSD_SECURITY && !defined SSH_SECURITY
    else {
    	error("must be run setuid root to communicate correctly");
	/*NOTREACHED*/
    }
#endif

    fprintf(stderr,
	    "%s: pid %ld executable %s version %s\n",
	    get_pname(), (long) getpid(),
	    my_argv[0], version());
    fflush(stderr);

    /* now, make sure we are a valid user */

    if (getpwuid(getuid()) == NULL) {
	error("can't get login name for my uid %ld", (long)getuid());
	/*NOTREACHED*/
    }

    signal(SIGPIPE, SIG_IGN);

    conf_dtimeout = getconf_time(CNF_DTIMEOUT);

    protocol_init();

    do {
	cmd = getcmd(&cmdargs);

	switch(cmd) {
	case START:
	    if(cmdargs.argc <  2)
		error("error [dumper START: not enough args: timestamp]");
	    dumper_timestamp = newstralloc(dumper_timestamp, cmdargs.argv[2]);
	    break;

	case ABORT:
	    break;

	case QUIT:
	    break;

	case PORT_DUMP:
	    /*
	     * PORT-DUMP
	     *   handle
	     *   port
	     *   host
	     *   features
	     *   disk
	     *   device
	     *   level
	     *   dumpdate
	     *   progname
	     *   amandad_path
	     *   client_username
	     *   ssh_keys
	     *   options
	     */
	    cmdargs.argc++;			/* true count of args */
	    a = 2;

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: handle]");
		/*NOTREACHED*/
	    }
	    handle = newstralloc(handle, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: port]");
		/*NOTREACHED*/
	    }
	    taper_port = (in_port_t)atoi(cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: hostname]");
		/*NOTREACHED*/
	    }
	    hostname = newstralloc(hostname, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: features]");
		/*NOTREACHED*/
	    }
	    am_release_feature_set(their_features);
	    their_features = am_string_to_feature(cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: diskname]");
		/*NOTREACHED*/
	    }
	    qdiskname = newstralloc(qdiskname, cmdargs.argv[a++]);
	    if (diskname != NULL)
		amfree(diskname);
	    diskname = unquote_string(qdiskname);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: device]");
		/*NOTREACHED*/
	    }
	    device = newstralloc(device, cmdargs.argv[a++]);
	    if(strcmp(device,"NODEVICE") == 0)
		amfree(device);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: level]");
		/*NOTREACHED*/
	    }
	    level = atoi(cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: dumpdate]");
		/*NOTREACHED*/
	    }
	    dumpdate = newstralloc(dumpdate, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: program]");
		/*NOTREACHED*/
	    }
	    progname = newstralloc(progname, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: amandad_path]");
		/*NOTREACHED*/
	    }
	    amandad_path = newstralloc(amandad_path, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: client_username]");
	    }
	    client_username = newstralloc(client_username, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: ssh_keys]");
	    }
	    ssh_keys = newstralloc(ssh_keys, cmdargs.argv[a++]);

	    if(a >= cmdargs.argc) {
		error("error [dumper PORT-DUMP: not enough args: options]");
	    }
	    options = newstralloc(options, cmdargs.argv[a++]);

	    if(a != cmdargs.argc) {
		error("error [dumper PORT-DUMP: too many args: %d != %d]",
		      cmdargs.argc, a);
	        /*NOTREACHED*/
	    }

	    if ((gethostbyname("localhost")) == NULL) {
		errstr = newstralloc(errstr,
				     "could not resolve localhost");
		q = squotef(errstr);
		putresult(FAILED, "%s %s\n", handle, q);
		log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname,
			dumper_timestamp, level, errstr);
		amfree(q);
		break;
	    }
	    /* connect outf to chunker/taper port */

	    outfd = stream_client("localhost", taper_port,
				  STREAM_BUFSIZE, 0, NULL, 0);
	    if (outfd == -1) {
		
		errstr = newvstralloc(errstr, "port open: ",
				      strerror(errno), NULL);
		q = squotef(errstr);
		putresult(FAILED, "%s %s\n", handle, q);
		log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname,
			dumper_timestamp, level, errstr);
		amfree(q);
		break;
	    }
	    databuf_init(&db, outfd);

	    check_options(options);

	    rc = startup_dump(hostname,
			      diskname,
			      device,
			      level,
			      dumpdate,
			      progname,
			      amandad_path,
			      client_username,
			      ssh_keys,
			      options);
	    if (rc != 0) {
		q = squote(errstr);
		putresult(rc == 2? FAILED : TRYAGAIN, "%s %s\n",
		    handle, q);
		if (rc == 2)
		    log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname,
			dumper_timestamp, level, errstr);
		amfree(q);
	    } else {
		do_dump(&db);
	    }

	    amfree(amandad_path);
	    amfree(client_username);

	    break;

	default:
	    if(cmdargs.argc >= 1) {
		q = squote(cmdargs.argv[1]);
	    } else if(cmdargs.argc >= 0) {
		q = squote(cmdargs.argv[0]);
	    } else {
		q = stralloc("(no input?)");
	    }
	    putresult(BAD_COMMAND, "%s\n", q);
	    amfree(q);
	    break;
	}

	if (outfd != -1)
	    aclose(outfd);
    } while(cmd != QUIT);

    /* make sure root privilege is dropped */
    if ( geteuid() == 0 ) {
      setuid(ruid);
      seteuid(ruid);
    }

    free_new_argv(new_argc, new_argv);
    free_server_config();
    am_release_feature_set(our_features);
    amfree(our_feature_string);
    amfree(errstr);
    amfree(dumper_timestamp);
    amfree(handle);
    amfree(hostname);
    amfree(qdiskname);
    amfree(diskname);
    amfree(device);
    amfree(dumpdate);
    amfree(progname);
    amfree(srvcompprog);
    amfree(clntcompprog);
    amfree(srv_encrypt);
    amfree(clnt_encrypt);
    amfree(srv_decrypt_opt);
    amfree(clnt_decrypt_opt);
    amfree(options);
    amfree(config_dir);
    amfree(config_name);

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if (malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    dbclose();
    return (0); /* exit */
}


/*
 * Initialize a databuf.  Takes a writeable file descriptor.
 */
static void
databuf_init(
    struct databuf *	db,
    int			fd)
{

    db->fd = fd;
    db->datain = db->dataout = db->datalimit = NULL;
    db->compresspid = -1;
    db->encryptpid = -1;
}


/*
 * Updates the buffer pointer for the input data buffer.  The buffer is
 * written regardless of how much data is present, since we know we
 * are writing to a socket (to chunker) and there is no need to maintain
 * any boundaries.
 */
static int
databuf_write(
    struct databuf *	db,
    const void *	buf,
    size_t		size)
{
    db->buf = (char *)buf;
    db->datain = db->datalimit = db->buf + size;
    db->dataout = db->buf;
    return databuf_flush(db);
}

/*
 * Write out the buffer to chunker.
 */
static int
databuf_flush(
    struct databuf *	db)
{
    ssize_t written;

    /*
     * If there's no data, do nothing.
     */
    if (db->dataout >= db->datain) {
	return 0;
    }

    /*
     * Write out the buffer
     */
    written = fullwrite(db->fd, db->dataout,
			(size_t)(db->datain - db->dataout));
    if (written > 0) {
	db->dataout += written;
        dumpbytes += (off_t)written;
    }
    if (dumpbytes >= (off_t)1024) {
	dumpsize += (dumpbytes / (off_t)1024);
	dumpbytes %= (off_t)1024;
    }
    if (written < 0) {
	errstr = squotef("data write: %s", strerror(errno));
	return -1;
    }
    db->datain = db->dataout = db->buf;
    return 0;
}

static int dump_result;
static int status;
#define	GOT_INFO_ENDLINE	(1 << 0)
#define	GOT_SIZELINE		(1 << 1)
#define	GOT_ENDLINE		(1 << 2)
#define	HEADER_DONE		(1 << 3)


static void
process_dumpeof(void)
{
    /* process any partial line in msgbuf? !!! */
    add_msg_data(NULL, 0);
    if(!ISSET(status, GOT_SIZELINE) && dump_result < 2) {
	/* make a note if there isn't already a failure */
	fprintf(errf,
		"? %s: strange [missing size line from sendbackup]\n",
		get_pname());
	if(errstr == NULL) {
	    errstr = stralloc("missing size line from sendbackup");
	}
	dump_result = max(dump_result, 2);
    }

    if(!ISSET(status, GOT_ENDLINE) && dump_result < 2) {
	fprintf(errf,
		"? %s: strange [missing end line from sendbackup]\n",
		get_pname());
	if(errstr == NULL) {
	    errstr = stralloc("missing end line from sendbackup");
	}
	dump_result = max(dump_result, 2);
    }
}

/*
 * Parse an information line from the client.
 * We ignore unknown parameters and only remember the last
 * of any duplicates.
 */
static void
parse_info_line(
    char *str)
{
    static const struct {
	const char *name;
	char *value;
	size_t len;
    } fields[] = {
	{ "BACKUP", file.program, SIZEOF(file.program) },
	{ "RECOVER_CMD", file.recover_cmd, SIZEOF(file.recover_cmd) },
	{ "COMPRESS_SUFFIX", file.comp_suffix, SIZEOF(file.comp_suffix) },
	{ "SERVER_CUSTOM_COMPRESS", file.srvcompprog, SIZEOF(file.srvcompprog) },
	{ "CLIENT_CUSTOM_COMPRESS", file.clntcompprog, SIZEOF(file.clntcompprog) },
	{ "SERVER_ENCRYPT", file.srv_encrypt, SIZEOF(file.srv_encrypt) },
	{ "CLIENT_ENCRYPT", file.clnt_encrypt, SIZEOF(file.clnt_encrypt) },
	{ "SERVER_DECRYPT_OPTION", file.srv_decrypt_opt, SIZEOF(file.srv_decrypt_opt) },
	{ "CLIENT_DECRYPT_OPTION", file.clnt_decrypt_opt, SIZEOF(file.clnt_decrypt_opt) }
    };
    char *name, *value;
    size_t i;

    if (strcmp(str, "end") == 0) {
	SET(status, GOT_INFO_ENDLINE);
	return;
    }

    name = strtok(str, "=");
    if (name == NULL)
	return;
    value = strtok(NULL, "");
    if (value == NULL)
	return;

    for (i = 0; i < SIZEOF(fields) / SIZEOF(fields[0]); i++) {
	if (strcmp(name, fields[i].name) == 0) {
	    strncpy(fields[i].value, value, fields[i].len - 1);
	    fields[i].value[fields[i].len - 1] = '\0';
	    break;
	}
    }
}

static void
process_dumpline(
    const char *	str)
{
    char *buf, *tok;

    buf = stralloc(str);

    switch (*buf) {
    case '|':
	/* normal backup output line */
	break;
    case '?':
	/* sendbackup detected something strange */
	dump_result = max(dump_result, 1);
	break;
    case 's':
	/* a sendbackup line, just check them all since there are only 5 */
	tok = strtok(buf, " ");
	if (tok == NULL || strcmp(tok, "sendbackup:") != 0)
	    goto bad_line;

	tok = strtok(NULL, " ");
	if (tok == NULL)
	    goto bad_line;

	if (strcmp(tok, "start") == 0) {
	    break;
	}

	if (strcmp(tok, "size") == 0) {
	    tok = strtok(NULL, "");
	    if (tok != NULL) {
		origsize = OFF_T_ATOI(tok);
		SET(status, GOT_SIZELINE);
	    }
	    break;
	}

	if (strcmp(tok, "end") == 0) {
	    SET(status, GOT_ENDLINE);
	    break;
	}

	if (strcmp(tok, "warning") == 0) {
	    dump_result = max(dump_result, 1);
	    break;
	}

	if (strcmp(tok, "error") == 0) {
	    SET(status, GOT_ENDLINE);
	    dump_result = max(dump_result, 2);

	    tok = strtok(NULL, "");
	    if (tok == NULL || *tok != '[') {
		errstr = newvstralloc(errstr, "bad remote error: ", str, NULL);
	    } else {
		char *enderr;

		tok++;	/* skip over '[' */
		if ((enderr = strchr(tok, ']')) != NULL)
		    *enderr = '\0';
		errstr = newstralloc(errstr, tok);
	    }
	    break;
	}

	if (strcmp(tok, "info") == 0) {
	    tok = strtok(NULL, "");
	    if (tok != NULL)
		parse_info_line(tok);
	    break;
	}
	/* else we fall through to bad line */
    default:
bad_line:
	/* prefix with ?? */
	fprintf(errf, "??");
	dump_result = max(dump_result, 1);
	break;
    }
    fprintf(errf, "%s\n", str);
    amfree(buf);
}

static void
add_msg_data(
    const char *	str,
    size_t		len)
{
    static struct {
	char *buf;	/* buffer holding msg data */
	size_t size;	/* size of alloced buffer */
    } msg = { NULL, 0 };
    char *line, *ch;
    size_t buflen;
    int	in_quotes = 0;

    if (msg.buf != NULL)
	buflen = strlen(msg.buf);
    else
	buflen = 0;

    /*
     * If our argument is NULL, then we need to flush out any remaining
     * bits and return.
     */
    if (str == NULL) {
	if (buflen == 0)
	    return;
	fprintf(errf,"? %s: error [partial line in msgbuf: "
				SIZE_T_FMT " bytes]\n", get_pname(),
				(SIZE_T_FMT_TYPE)buflen);
	fprintf(errf,"? %s: error [partial line in msgbuf: \"%s\"]\n",
	    get_pname(), msg.buf);
	msg.buf[0] = '\0';
	return;
    }

    /*
     * Expand the buffer if it can't hold the new contents.
     */
    if ((buflen + len + 1) > msg.size) {
	char *newbuf;
	size_t newsize;

/* round up to next y, where y is a power of 2 */
#define	ROUND(x, y)	(((x) + (y) - 1) & ~((y) - 1))

	newsize = ROUND(buflen + (ssize_t)len + 1, 256);
	newbuf = alloc(newsize);

	if (msg.buf != NULL) {
	    strncpy(newbuf, msg.buf, newsize);
	    amfree(msg.buf);
	} else
	    newbuf[0] = '\0';
	msg.buf = newbuf;
	msg.size = newsize;
    }

    /*
     * If there was a partial line from the last call, then
     * append the new data to the end.
     */
    strncat(msg.buf, str, len);

    /*
     * Process all lines in the buffer
     * scanning line for unqouted newline.
     */
    for (ch = line = msg.buf; *ch != '\0'; ch++) {
	if (*ch == '"') {
	    in_quotes = !in_quotes;
	} else if ((*ch == '\\') && (*(ch + 1) == '"')) {
	        ch++;
	} else if (!in_quotes && (*ch == '\n')) {
	    /*
	     * Found an unqouted newline.  Terminate and process line.
	     */
	    *ch = '\0';
	    process_dumpline(line);
	    line = ch + 1;
	}
    }

    /*
     * If we did not process all of the data, move it to the front
     * of the buffer so it is there next time.
     */
    if (*line != '\0') {
	buflen = strlen(line);
	memmove(msg.buf, line, (size_t)buflen + 1);
    } else {
	msg.buf[0] = '\0';
    }
}


static void
log_msgout(
    logtype_t	typ)
{
    char *line;

    fflush(errf);
    if (fseek(errf, 0L, SEEK_SET) < 0) {
	dbprintf(("log_msgout: warning - seek failed: %s\n", strerror(errno)));
    }
    while ((line = agets(errf)) != NULL) {
	if (line[0] != '\0') {
		log_add(typ, "%s", line);
	}
	amfree(line);
    }

    afclose(errf);
}

/* ------------- */

/*
 * Fill in the rest of the tape header
 */
static void
finish_tapeheader(
    dumpfile_t *file)
{

    assert(ISSET(status, HEADER_DONE));

    file->type = F_DUMPFILE;
    strncpy(file->datestamp, dumper_timestamp, sizeof(file->datestamp) - 1);
    strncpy(file->name, hostname, SIZEOF(file->name) - 1);
    strncpy(file->disk, diskname, SIZEOF(file->disk) - 1);
    file->dumplevel = level;

    /*
     * If we're doing the compression here, we need to override what
     * sendbackup told us the compression was.
     */
    if (srvcompress != COMP_NONE) {
	file->compressed = 1;
#ifndef UNCOMPRESS_OPT
#define	UNCOMPRESS_OPT	""
#endif
	if (srvcompress == COMP_SERV_CUST) {
	    snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
		     " %s %s |", srvcompprog, "-d");
	    strncpy(file->comp_suffix, "cust", SIZEOF(file->comp_suffix) - 1);
	    file->comp_suffix[SIZEOF(file->comp_suffix) - 1] = '\0';
	    strncpy(file->srvcompprog, srvcompprog, SIZEOF(file->srvcompprog) - 1);
	    file->srvcompprog[SIZEOF(file->srvcompprog) - 1] = '\0';
	} else if ( srvcompress == COMP_CUST ) {
	    snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
		     " %s %s |", clntcompprog, "-d");
	    strncpy(file->comp_suffix, "cust", SIZEOF(file->comp_suffix) - 1);
	    file->comp_suffix[SIZEOF(file->comp_suffix) - 1] = '\0';
	    strncpy(file->clntcompprog, clntcompprog, SIZEOF(file->clntcompprog));
	    file->clntcompprog[SIZEOF(file->clntcompprog) - 1] = '\0';
	} else {
	    snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
		" %s %s |", UNCOMPRESS_PATH, UNCOMPRESS_OPT);
	    strncpy(file->comp_suffix, COMPRESS_SUFFIX,SIZEOF(file->comp_suffix) - 1);
	    file->comp_suffix[SIZEOF(file->comp_suffix) - 1] = '\0';
	}
    } else {
	if (file->comp_suffix[0] == '\0') {
	    file->compressed = 0;
	    assert(SIZEOF(file->comp_suffix) >= 2);
	    strncpy(file->comp_suffix, "N", SIZEOF(file->comp_suffix) - 1);
	    file->comp_suffix[SIZEOF(file->comp_suffix) - 1] = '\0';
	} else {
	    file->compressed = 1;
	}
    }
    /* take care of the encryption header here */
    if (srvencrypt != ENCRYPT_NONE) {
      file->encrypted= 1;
      if (srvencrypt == ENCRYPT_SERV_CUST) {
	snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		 " %s %s |", srv_encrypt, srv_decrypt_opt); 
	strncpy(file->encrypt_suffix, "enc", SIZEOF(file->encrypt_suffix) - 1);
	file->encrypt_suffix[SIZEOF(file->encrypt_suffix) - 1] = '\0';
	strncpy(file->srv_encrypt, srv_encrypt, SIZEOF(file->srv_encrypt) - 1);
	file->srv_encrypt[SIZEOF(file->srv_encrypt) - 1] = '\0';
	strncpy(file->srv_decrypt_opt, srv_decrypt_opt, SIZEOF(file->srv_decrypt_opt) - 1);
	file->srv_decrypt_opt[SIZEOF(file->srv_decrypt_opt) - 1] = '\0';
      } else if ( srvencrypt == ENCRYPT_CUST ) {
	snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		 " %s %s |", clnt_encrypt, clnt_decrypt_opt);
	strncpy(file->encrypt_suffix, "enc", SIZEOF(file->encrypt_suffix) - 1);
	file->encrypt_suffix[SIZEOF(file->encrypt_suffix) - 1] = '\0';
	strncpy(file->clnt_encrypt, clnt_encrypt, SIZEOF(file->clnt_encrypt) - 1);
	file->clnt_encrypt[SIZEOF(file->clnt_encrypt) - 1] = '\0';
	strncpy(file->clnt_decrypt_opt, clnt_decrypt_opt, SIZEOF(file->clnt_decrypt_opt));
	file->clnt_decrypt_opt[SIZEOF(file->clnt_decrypt_opt) - 1] = '\0';
      }
    } else {
      if (file->encrypt_suffix[0] == '\0') {
	file->encrypted = 0;
	assert(SIZEOF(file->encrypt_suffix) >= 2);
	strncpy(file->encrypt_suffix, "N", SIZEOF(file->encrypt_suffix) - 1);
	file->encrypt_suffix[SIZEOF(file->encrypt_suffix) - 1] = '\0';
      } else {
	file->encrypted= 1;
      }
    }
}

/*
 * Send an Amanda dump header to the output file.
 */
static ssize_t
write_tapeheader(
    int		outfd,
    dumpfile_t *file)
{
    char buffer[DISK_BLOCK_BYTES];
    ssize_t written;

    build_header(buffer, file, SIZEOF(buffer));

    written = write(outfd, buffer, SIZEOF(buffer));
    if(written == (ssize_t)sizeof(buffer))
	return 0;
    if(written < 0)
	return written;

    errno = ENOSPC;
    return -1;
}

static int
do_dump(
    struct databuf *db)
{
    char *indexfile_tmp = NULL;
    char *indexfile_real = NULL;
    char level_str[NUM_STR_SIZE];
    char *fn;
    char *q;
    times_t runtime;
    double dumptime;	/* Time dump took in secs */
    char *errfname = NULL;
    int indexout;
    pid_t indexpid = -1;

    startclock();

    status = 0;
    dump_result = 0;
    dumpbytes = dumpsize = headersize = origsize = (off_t)0;
    fh_init(&file);

    snprintf(level_str, SIZEOF(level_str), "%d", level);
    fn = sanitise_filename(diskname);
    errfname = newvstralloc(errfname,
			    AMANDA_TMPDIR,
			    "/", hostname,
			    ".", fn,
			    ".", level_str,
			    ".errout",
			    NULL);
    amfree(fn);
    if((errf = fopen(errfname, "w+")) == NULL) {
	errstr = newvstralloc(errstr,
			      "errfile open \"", errfname, "\": ",
			      strerror(errno),
			      NULL);
	amfree(errfname);
	goto failed;
    }
    unlink(errfname);				/* so it goes away on close */
    amfree(errfname);

    if (streams[INDEXFD].fd != NULL) {
	indexfile_real = getindexfname(hostname, diskname, dumper_timestamp, level);
	indexfile_tmp = stralloc2(indexfile_real, ".tmp");

	if (mkpdir(indexfile_tmp, 02755, (uid_t)-1, (gid_t)-1) == -1) {
	   errstr = newvstralloc(errstr,
				 "err create ",
				 indexfile_tmp,
				 ": ",
				 strerror(errno),
				 NULL);
	   amfree(indexfile_real);
	   amfree(indexfile_tmp);
	   goto failed;
	}
	indexout = open(indexfile_tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (indexout == -1) {
	    errstr = newvstralloc(errstr, "err open ", indexfile_tmp, ": ",
		strerror(errno), NULL);
	    goto failed;
	} else {
	    if (runcompress(indexout, &indexpid, COMP_BEST) < 0) {
		aclose(indexout);
		goto failed;
	    }
	}
	indexfderror = 0;
	/*
	 * Schedule the indexfd for relaying to the index file
	 */
	security_stream_read(streams[INDEXFD].fd, read_indexfd, &indexout);
    }

    /*
     * We only need to process messages initially.  Once we have done
     * the header, we will start processing data too.
     */
    security_stream_read(streams[MESGFD].fd, read_mesgfd, db);
    set_datafd = 0;

    /*
     * Setup a read timeout
     */
    timeout(conf_dtimeout);

    /*
     * Start the event loop.  This will exit when all three events
     * (read the mesgfd, read the datafd, and timeout) are removed.
     */
    event_loop(0);

    if (dump_result > 1)
	goto failed;

    runtime = stopclock();
    dumptime = (double)(runtime.r.tv_sec) +
	       ((double)(runtime.r.tv_usec) / 1000000.0);

    dumpsize -= headersize;		/* don't count the header */
    if (dumpsize < (off_t)0)		/* XXX - maybe this should be fatal? */
	dumpsize = (off_t)0;

    amfree(errstr);
    errstr = alloc(128);
    snprintf(errstr, 128, "sec %s kb " OFF_T_FMT " kps %3.1lf orig-kb " OFF_T_FMT "",
	walltime_str(runtime),
	(OFF_T_FMT_TYPE)dumpsize,
	(isnormal(dumptime) ? ((double)dumpsize / (double)dumptime) : 0.0),
	(OFF_T_FMT_TYPE)origsize);
    q = squotef("[%s]", errstr);
    putresult(DONE, "%s " OFF_T_FMT " " OFF_T_FMT " %lu %s\n", handle,
    		(OFF_T_FMT_TYPE)origsize,
		(OFF_T_FMT_TYPE)dumpsize,
	        (unsigned long)((double)dumptime+0.5), q);
    amfree(q);

    switch(dump_result) {
    case 0:
	log_add(L_SUCCESS, "%s %s %s %d [%s]", hostname, qdiskname, dumper_timestamp, level, errstr);

	break;

    case 1:
	log_start_multiline();
	log_add(L_STRANGE, "%s %s %d [%s]", hostname, qdiskname, level, errstr);
	log_msgout(L_STRANGE);
	log_end_multiline();

	break;
    }

    if (errf) afclose(errf);

    aclose(db->fd);
    if (indexfile_tmp) {
	amwait_t index_status;

	/*@i@*/ aclose(indexout);
	waitpid(indexpid,&index_status,0);
	if (rename(indexfile_tmp, indexfile_real) != 0) {
	    log_add(L_WARNING, "could not rename \"%s\" to \"%s\": %s",
		    indexfile_tmp, indexfile_real, strerror(errno));
	}
	amfree(indexfile_tmp);
	amfree(indexfile_real);
    }

    if(db->compresspid != -1) {
	waitpid(db->compresspid,NULL,0);
    }
    if(db->encryptpid != -1) {
	waitpid(db->encryptpid,NULL,0);
    }

    amfree(errstr);

    return 1;

failed:
    q = squotef("[%s]", errstr);
    putresult(FAILED, "%s %s\n", handle, q);
    amfree(q);

    aclose(db->fd);
    /* kill all child process */
    if (db->compresspid != -1) {
	fprintf(stderr,"%s: kill compress command\n",get_pname());
	if (kill(db->compresspid, SIGTERM) < 0) {
	    if (errno != ESRCH)
		fprintf(stderr,"%s: can't kill compress command: %s\n", 
		    get_pname(), strerror(errno));
	}
	else {
	    waitpid(db->compresspid,NULL,0);
	}
    }

    if (db->encryptpid != -1) {
	fprintf(stderr,"%s: kill encrypt command\n",get_pname());
	if (kill(db->encryptpid, SIGTERM) < 0) {
	    if (errno != ESRCH)
		fprintf(stderr,"%s: can't kill encrypt command: %s\n", 
		    get_pname(), strerror(errno));
	}
	else {
	    waitpid(db->encryptpid,NULL,0);
	}
    }

    if (indexpid != -1) {
	fprintf(stderr,"%s: kill index command\n",get_pname());
	if (kill(indexpid, SIGTERM) < 0) {
	    if (errno != ESRCH)
		fprintf(stderr,"%s: can't kill index command: %s\n", 
		    get_pname(),strerror(errno));
	}
	else {
	    waitpid(indexpid,NULL,0);
	}
    }

    log_start_multiline();
    log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname, dumper_timestamp,
	    level, errstr);
    if (errf) {
	log_msgout(L_FAIL);
    }
    log_end_multiline();

    if (errf) afclose(errf);

    if (indexfile_tmp) {
	unlink(indexfile_tmp);
	amfree(indexfile_tmp);
	amfree(indexfile_real);
    }

    return 0;
}

/*
 * Callback for reads on the mesgfd stream
 */
static void
read_mesgfd(
    void *	cookie,
    void *	buf,
    ssize_t	size)
{
    struct databuf *db = cookie;

    assert(db != NULL);

    switch (size) {
    case -1:
	errstr = newstralloc2(errstr, "mesg read: ",
	    security_stream_geterror(streams[MESGFD].fd));
	dump_result = 2;
	stop_dump();
	return;

    case 0:
	/*
	 * EOF.  Just shut down the mesg stream.
	 */
	process_dumpeof();
	security_stream_close(streams[MESGFD].fd);
	streams[MESGFD].fd = NULL;
	/*
	 * If the data fd and index fd has also shut down, then we're done.
	 */
	if ((set_datafd == 0 || streams[DATAFD].fd == NULL) && 
	    streams[INDEXFD].fd == NULL)
	    stop_dump();
	return;

    default:
	assert(buf != NULL);
	add_msg_data(buf, (size_t)size);
	security_stream_read(streams[MESGFD].fd, read_mesgfd, cookie);
	break;
    }

    /*
     * Reset the timeout for future reads
     */
    timeout(conf_dtimeout);

    if (ISSET(status, GOT_INFO_ENDLINE) && !ISSET(status, HEADER_DONE)) {
	SET(status, HEADER_DONE);
	/* time to do the header */
	finish_tapeheader(&file);
	if (write_tapeheader(db->fd, &file)) {
	    errstr = newstralloc2(errstr, "write_tapeheader: ", 
				  strerror(errno));
	    dump_result = 2;
	    stop_dump();
	    return;
	}
	dumpsize += (off_t)DISK_BLOCK_KB;
	headersize += (off_t)DISK_BLOCK_KB;

	if (srvencrypt == ENCRYPT_SERV_CUST) {
	    if (runencrypt(db->fd, &db->encryptpid, srvencrypt) < 0) {
		dump_result = 2;
		stop_dump();
		return;
	    }
	}
	/*
	 * Now, setup the compress for the data output, and start
	 * reading the datafd.
	 */
	if ((srvcompress != COMP_NONE) && (srvcompress != COMP_CUST)) {
	    if (runcompress(db->fd, &db->compresspid, srvcompress) < 0) {
		dump_result = 2;
		stop_dump();
		return;
	    }
	}
	security_stream_read(streams[DATAFD].fd, read_datafd, db);
	set_datafd = 1;
    }
}

/*
 * Callback for reads on the datafd stream
 */
static void
read_datafd(
    void *	cookie,
    void *	buf,
    ssize_t	size)
{
    struct databuf *db = cookie;

    assert(db != NULL);

    /*
     * The read failed.  Error out
     */
    if (size < 0) {
	errstr = newstralloc2(errstr, "data read: ",
	    security_stream_geterror(streams[DATAFD].fd));
	dump_result = 2;
	stop_dump();
	return;
    }

    /*
     * Reset the timeout for future reads
     */
    timeout(conf_dtimeout);

    /* The header had better be written at this point */
    assert(ISSET(status, HEADER_DONE));

    /*
     * EOF.  Stop and return.
     */
    if (size == 0) {
	databuf_flush(db);
	if (dumpbytes != (off_t)0) {
	    dumpsize += (off_t)1;
	}
	security_stream_close(streams[DATAFD].fd);
	streams[DATAFD].fd = NULL;
	/*
	 * If the mesg fd and index fd has also shut down, then we're done.
	 */
	if (streams[MESGFD].fd == NULL && streams[INDEXFD].fd == NULL)
	    stop_dump();
	return;
    }

    /*
     * We read something.  Add it to the databuf and reschedule for
     * more data.
     */
    assert(buf != NULL);
    if (databuf_write(db, buf, (size_t)size) < 0) {
	errstr = newstralloc2(errstr, "data write: ", strerror(errno));
	dump_result = 2;
	stop_dump();
	return;
    }
    security_stream_read(streams[DATAFD].fd, read_datafd, cookie);
}

/*
 * Callback for reads on the index stream
 */
static void
read_indexfd(
    void *	cookie,
    void *	buf,
    ssize_t	size)
{
    int fd;

    assert(cookie != NULL);
    fd = *(int *)cookie;

    if (size < 0) {
	errstr = newstralloc2(errstr, "index read: ",
	    security_stream_geterror(streams[INDEXFD].fd));
	dump_result = 2;
	stop_dump();
	return;
    }

    /*
     * EOF.  Stop and return.
     */
    if (size == 0) {
	security_stream_close(streams[INDEXFD].fd);
	streams[INDEXFD].fd = NULL;
	/*
	 * If the mesg fd has also shut down, then we're done.
	 */
	if ((set_datafd == 0 || streams[DATAFD].fd == NULL) &&
	     streams[MESGFD].fd == NULL)
	    stop_dump();
	return;
    }

    assert(buf != NULL);

    /*
     * We ignore error while writing to the index file.
     */
    if (fullwrite(fd, buf, (size_t)size) < 0) {
	/* Ignore error, but schedule another read. */
	if(indexfderror == 0) {
	    indexfderror = 1;
	    log_add(L_INFO, "Index corrupted for %s:%s", hostname, qdiskname);
	}
    }
    security_stream_read(streams[INDEXFD].fd, read_indexfd, cookie);
}

/*
 * Startup a timeout in the event handler.  If the arg is 0,
 * then remove the timeout.
 */
static void
timeout(
    time_t seconds)
{
    static event_handle_t *ev_timeout = NULL;

    /*
     * First, remove a timeout if one is active.
     */
    if (ev_timeout != NULL) {
	event_release(ev_timeout);
	ev_timeout = NULL;
    }

    /*
     * Now, schedule a new one if 'seconds' is greater than 0
     */
    if (seconds > 0)
	ev_timeout = event_register((event_id_t)seconds, EV_TIME, timeout_callback, NULL);
}

/*
 * This is the callback for timeout().  If this is reached, then we
 * have a data timeout.
 */
static void
timeout_callback(
    void *	unused)
{
    (void)unused;	/* Quiet unused parameter warning */

    assert(unused == NULL);
    errstr = newstralloc(errstr, "data timeout");
    dump_result = 2;
    stop_dump();
}

/*
 * This is called when everything needs to shut down so event_loop()
 * will exit.
 */
static void
stop_dump(void)
{
    int i;

    for (i = 0; i < NSTREAMS; i++) {
	if (streams[i].fd != NULL) {
	    security_stream_close(streams[i].fd);
	    streams[i].fd = NULL;
	}
    }
    timeout(0);
}


/*
 * Runs compress with the first arg as its stdout.  Returns
 * 0 on success or negative if error, and it's pid via the second
 * argument.  The outfd arg is dup2'd to the pipe to the compress
 * process.
 */
static int
runcompress(
    int		outfd,
    pid_t *	pid,
    comp_t	comptype)
{
    int outpipe[2], rval;

    assert(outfd >= 0);
    assert(pid != NULL);

    /* outpipe[0] is pipe's stdin, outpipe[1] is stdout. */
    if (pipe(outpipe) < 0) {
	errstr = newstralloc2(errstr, "pipe: ", strerror(errno));
	return (-1);
    }

    switch (*pid = fork()) {
    case -1:
	errstr = newstralloc2(errstr, "couldn't fork: ", strerror(errno));
	aclose(outpipe[0]);
	aclose(outpipe[1]);
	return (-1);
    default:
	rval = dup2(outpipe[1], outfd);
	if (rval < 0)
	    errstr = newstralloc2(errstr, "couldn't dup2: ", strerror(errno));
	aclose(outpipe[1]);
	aclose(outpipe[0]);
	return (rval);
    case 0:
	if (dup2(outpipe[0], 0) < 0) {
	    error("err dup2 in: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	if (dup2(outfd, 1) == -1) {
	    error("err dup2 out: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	safe_fd(-1, 0);
	if (comptype != COMP_SERV_CUST) {
	    execlp(COMPRESS_PATH, COMPRESS_PATH, (  comptype == COMP_BEST ?
		COMPRESS_BEST_OPT : COMPRESS_FAST_OPT), (char *)NULL);
	    error("error: couldn't exec %s: %s", COMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/
	} else if (*srvcompprog) {
	    execlp(srvcompprog, srvcompprog, (char *)0);
	    error("error: couldn't exec server custom filter%s.\n", srvcompprog);
	    /*NOTREACHED*/
	}
    }
    /*NOTREACHED*/
    return (-1);
}

/*
 * Runs encrypt with the first arg as its stdout.  Returns
 * 0 on success or negative if error, and it's pid via the second
 * argument.  The outfd arg is dup2'd to the pipe to the encrypt
 * process.
 */
static int
runencrypt(
    int		outfd,
    pid_t *	pid,
    encrypt_t	encrypttype)
{
    int outpipe[2], rval;

    assert(outfd >= 0);
    assert(pid != NULL);

    /* outpipe[0] is pipe's stdin, outpipe[1] is stdout. */
    if (pipe(outpipe) < 0) {
	errstr = newstralloc2(errstr, "pipe: ", strerror(errno));
	return (-1);
    }

    switch (*pid = fork()) {
    case -1:
	errstr = newstralloc2(errstr, "couldn't fork: ", strerror(errno));
	aclose(outpipe[0]);
	aclose(outpipe[1]);
	return (-1);
    default:
	rval = dup2(outpipe[1], outfd);
	if (rval < 0)
	    errstr = newstralloc2(errstr, "couldn't dup2: ", strerror(errno));
	aclose(outpipe[1]);
	aclose(outpipe[0]);
	return (rval);
    case 0:
	if (dup2(outpipe[0], 0) < 0) {
	    error("err dup2 in: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	if (dup2(outfd, 1) < 0 ) {
	    error("err dup2 out: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	safe_fd(-1, 0);
	if ((encrypttype == ENCRYPT_SERV_CUST) && *srv_encrypt) {
	    execlp(srv_encrypt, srv_encrypt, (char *)0);
	    error("error: couldn't exec server encryption%s.\n", srv_encrypt);
	    /*NOTREACHED*/
	}
    }
    /*NOTREACHED*/
    return (-1);
}


/* -------------------- */

static void
sendbackup_response(
    void *		datap,
    pkt_t *		pkt,
    security_handle_t *	sech)
{
    int ports[NSTREAMS], *response_error = datap, i;
    char *p;
    char *tok;
    char *extra;

    assert(response_error != NULL);
    assert(sech != NULL);

    security_close_connection(sech, hostname);

    if (pkt == NULL) {
	errstr = newvstralloc(errstr, "[request failed: ",
	    security_geterror(sech), "]", NULL);
	*response_error = 1;
	return;
    }

    extra = NULL;
    memset(ports, 0, SIZEOF(ports));
    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	fprintf(stderr, "got nak response:\n----\n%s\n----\n\n", pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || strcmp(tok, "ERROR") != 0)
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    errstr = newvstralloc(errstr, "NAK: ", tok, NULL);
	    *response_error = 1;
	} else {
bad_nak:
	    errstr = newstralloc(errstr, "request NAK");
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	errstr = newvstralloc(errstr, "received strange packet type ",
	    pkt_type2str(pkt->type), ": ", pkt->body, NULL);
	*response_error = 1;
	return;
    }

#if 1
//#if defined(PACKET_DEBUG)
    fprintf(stderr, "got response:\n----\n%s\n----\n\n", pkt->body);
#endif

    for(i = 0; i < NSTREAMS; i++) {
	ports[i] = -1;
	streams[i].fd = NULL;
    }

    p = pkt->body;
    while((tok = strtok(p, " \n")) != NULL) {
	p = NULL;

	/*
	 * Error response packets have "ERROR" followed by the error message
	 * followed by a newline.
	 */
	if (strcmp(tok, "ERROR") == 0) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL)
		tok = "[bogus error packet]";
	    errstr = newstralloc(errstr, tok);
	    *response_error = 2;
	    return;
	}

	/*
	 * Regular packets have CONNECT followed by three streams
	 */
	if (strcmp(tok, "CONNECT") == 0) {

	    /*
	     * Parse the three stream specifiers out of the packet.
	     */
	    for (i = 0; i < NSTREAMS; i++) {
		tok = strtok(NULL, " ");
		if (tok == NULL || strcmp(tok, streams[i].name) != 0) {
		    extra = vstralloc("CONNECT token is \"",
				      tok ? tok : "(null)",
				      "\": expected \"",
				      streams[i].name,
				      "\"",
				      NULL);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = vstralloc("CONNECT ",
				      streams[i].name,
				      " token is \"",
				      tok ? tok : "(null)",
				      "\": expected a port number",
				      NULL);
		    goto parse_error;
		}
	    }
	    continue;
	}

	/*
	 * OPTIONS [options string] '\n'
	 */
	if (strcmp(tok, "OPTIONS") == 0) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
		extra = stralloc("OPTIONS token is missing");
		goto parse_error;
	    }

	    while((p = strchr(tok, ';')) != NULL) {
		*p++ = '\0';
#define sc "features="
		if(strncmp(tok, sc, SIZEOF(sc) - 1) == 0) {
		    tok += SIZEOF(sc) - 1;
#undef sc
		    am_release_feature_set(their_features);
		    if((their_features = am_string_to_feature(tok)) == NULL) {
			errstr = newvstralloc(errstr,
					      "OPTIONS: bad features value: ",
					      tok,
					      NULL);
			goto parse_error;
		    }
		}
		tok = p;
	    }
	    continue;
	}

	extra = vstralloc("next token is \"",
			  tok ? tok : "(null)",
			  "\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\"",
			  NULL);
	goto parse_error;
    }

    /*
     * Connect the streams to their remote ports
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (ports[i] == -1)
	    continue;
	streams[i].fd = security_stream_client(sech, ports[i]);
	if (streams[i].fd == NULL) {
	    errstr = newvstralloc(errstr,
		"[could not connect ", streams[i].name, " stream: ",
		security_geterror(sech), "]", NULL);
	    goto connect_error;
	}
    }

    /*
     * Authenticate the streams
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (streams[i].fd == NULL)
	    continue;
#ifdef KRB4_SECURITY
	/*
	 * XXX krb4 historically never authenticated the index stream!
	 * We need to reproduce this lossage here to preserve compatibility
	 * with old clients.
	 * It is wrong to delve into sech, but we have no choice here.
	 */
	if (strcasecmp(sech->driver->name, "krb4") != 0 && i == INDEXFD)
	    continue;
#endif
	if (security_stream_auth(streams[i].fd) < 0) {
	    errstr = newvstralloc(errstr,
		"[could not authenticate ", streams[i].name, " stream: ",
		security_stream_geterror(streams[i].fd), "]", NULL);
	    goto connect_error;
	}
    }

    /*
     * The MESGFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (streams[MESGFD].fd == NULL || streams[DATAFD].fd == NULL) {
	errstr = newstralloc(errstr, "[couldn't open MESG or INDEX streams]");
	goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    security_close_connection(sech, hostname);
    return;

parse_error:
    errstr = newvstralloc(errstr,
			  "[parse of reply message failed: ",
			  extra ? extra : "(no additional information)",
			  "]",
			  NULL);
    amfree(extra);
    *response_error = 2;
    security_close_connection(sech, hostname);
    return;

connect_error:
    stop_dump();
    *response_error = 1;
    security_close_connection(sech, hostname);
}

static char *
dumper_get_security_conf(
    char *	string,
    void *	arg)
{
        (void)arg;	/* Quiet unused parameter warning */

        if(!string || !*string)
                return(NULL);

        if(strcmp(string, "krb5principal")==0) {
                return(getconf_str(CNF_KRB5PRINCIPAL));
        } else if(strcmp(string, "krb5keytab")==0) {
                return(getconf_str(CNF_KRB5KEYTAB));
        } else if(strcmp(string, "amandad_path")==0) {
                return (amandad_path);
        } else if(strcmp(string, "client_username")==0) {
                return (client_username);
        } else if(strcmp(string, "ssh_keys")==0) {
                return (ssh_keys);
        }
        return(NULL);
}

static int
startup_dump(
    const char *hostname,
    const char *disk,
    const char *device,
    int		level,
    const char *dumpdate,
    const char *progname,
    const char *amandad_path,
    const char *client_username,
    const char *ssh_keys,
    const char *options)
{
    char level_string[NUM_STR_SIZE];
    char *req = NULL;
    char *authopt, *endauthopt, authoptbuf[80];
    int response_error;
    const security_driver_t *secdrv;
    char *dumper_api;
    int has_features;
    int has_hostname;
    int has_device;
    int has_config;

    (void)disk;			/* Quiet unused parameter warning */
    (void)amandad_path;		/* Quiet unused parameter warning */
    (void)client_username;	/* Quiet unused parameter warning */
    (void)ssh_keys;		/* Quiet unused parameter warning */

    has_features = am_has_feature(their_features, fe_req_options_features);
    has_hostname = am_has_feature(their_features, fe_req_options_hostname);
    has_config   = am_has_feature(their_features, fe_req_options_config);
    has_device   = am_has_feature(their_features, fe_sendbackup_req_device);

    /*
     * Default to bsd authentication if none specified.  This is gross.
     *
     * Options really need to be pre-parsed into some sort of structure
     * much earlier, and then flattened out again before transmission.
     */
    authopt = strstr(options, "auth=");
    if (authopt == NULL) {
	authopt = "BSD";
    } else {
	endauthopt = strchr(authopt, ';');
	if ((endauthopt == NULL) ||
	  ((sizeof(authoptbuf) - 1) < (size_t)(endauthopt - authopt))) {
	    authopt = "BSD";
	} else {
	    authopt += strlen("auth=");
	    strncpy(authoptbuf, authopt, (size_t)(endauthopt - authopt));
	    authoptbuf[endauthopt - authopt] = '\0';
	    authopt = authoptbuf;
	}
    }

    snprintf(level_string, SIZEOF(level_string), "%d", level);
    if(strncmp(progname, "DUMP", 4) == 0
       || strncmp(progname, "GNUTAR", 6) == 0) {
	dumper_api = "";
    } else {
	dumper_api = "DUMPER ";
    }
    req = vstralloc("SERVICE sendbackup\n",
		    "OPTIONS ",
		    has_features ? "features=" : "",
		    has_features ? our_feature_string : "",
		    has_features ? ";" : "",
		    has_hostname ? "hostname=" : "",
		    has_hostname ? hostname : "",
		    has_hostname ? ";" : "",
		    has_config   ? "config=" : "",
		    has_config   ? config_name : "",
		    has_config   ? ";" : "",
		    "\n",
		    dumper_api, progname,
		    " ", qdiskname,
		    " ", device && has_device ? device : "",
		    " ", level_string,
		    " ", dumpdate,
		    " OPTIONS ", options,
		    /* compat: if auth=krb4, send krb4-auth */
		    (strcasecmp(authopt, "krb4") ? "" : "krb4-auth"),
		    "\n",
		    NULL);

fprintf(stderr, "send request:\n----\n%s\n----\n\n", req);
    secdrv = security_getdriver(authopt);
    if (secdrv == NULL) {
	error("no '%s' security driver available for host '%s'",
	    authopt, hostname);
	/*NOTREACHED*/
    }

    protocol_sendreq(hostname, secdrv, dumper_get_security_conf, req,
	STARTUP_TIMEOUT, sendbackup_response, &response_error);

    amfree(req);

    protocol_run();
    return (response_error);
}
