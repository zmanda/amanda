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
#include "fileheader.h"
#include "amfeatures.h"
#include "server_util.h"
#include "util.h"
#include "timestamp.h"
#include "amxml.h"

#define dumper_debug(i,x) do {		\
	if ((i) <= debug_dumper) {	\
	    dbprintf(x);		\
	}				\
} while (0)

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

struct databuf *g_databuf = NULL;

typedef struct filter_s {
    int             fd;
    char           *name;
    char           *buffer;
    gint64          first;           /* first byte used */
    gint64          size;            /* number of byte use in the buffer */
    gint64          allocated_size ; /* allocated size of the buffer     */
    event_handle_t *event;
} filter_t;

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
static kencrypt_type dumper_kencrypt;

static FILE *errf = NULL;
static char *hostname = NULL;
am_feature_t *their_features = NULL;
static char *diskname = NULL;
static char *qdiskname = NULL, *b64disk;
static char *device = NULL, *b64device;
static char *options = NULL;
static char *progname = NULL;
static char *amandad_path=NULL;
static char *client_username=NULL;
static char *client_port=NULL;
static char *ssh_keys=NULL;
static char *auth=NULL;
static data_path_t data_path=DATA_PATH_AMANDA;
static char *dataport_list = NULL;
static int level;
static char *dumpdate = NULL;
static char *dumper_timestamp = NULL;
static time_t conf_dtimeout;
static int indexfderror;
static int set_datafd;
static char *dle_str = NULL;
static char *errfname = NULL;
static int   errf_lines = 0;
static int   max_warnings = 0;

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

/* buffer to keep partial line from the MESG stream */
static struct {
    char *buf;		/* buffer holding msg data */
    size_t size;	/* size of alloced buffer */
} msg = { NULL, 0 };


/* local functions */
int		main(int, char **);
static int	do_dump(struct databuf *);
static void	check_options(char *);
static void     xml_check_options(char *optionstr);
static void	finish_tapeheader(dumpfile_t *);
static ssize_t	write_tapeheader(int, dumpfile_t *);
static void	databuf_init(struct databuf *, int);
static int	databuf_write(struct databuf *, const void *, size_t);
static int	databuf_flush(struct databuf *);
static void	process_dumpeof(void);
static void	process_dumpline(const char *);
static void	add_msg_data(const char *, size_t);
static void	parse_info_line(char *);
static int	log_msgout(logtype_t);
static char *	dumper_get_security_conf (char *, void *);

static int	runcompress(int, pid_t *, comp_t, char *);
static int	runencrypt(int, pid_t *,  encrypt_t);

static void	sendbackup_response(void *, pkt_t *, security_handle_t *);
static int	startup_dump(const char *, const char *, const char *, int,
			const char *, const char *, const char *,
			const char *, const char *, const char *,
			const char *, const char *);
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
	    srvcompress = COMP_SERVER_CUST;
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

    if (strstr(options, "kencrypt;") != NULL) {
	dumper_kencrypt = KENCRYPT_WILL_DO;
    } else {
	dumper_kencrypt = KENCRYPT_NONE;
    }
}


static void
xml_check_options(
    char *optionstr)
{
    char *o, *oo;
    char *errmsg = NULL;
    dle_t *dle;

    o = oo = vstralloc("<dle>", strchr(optionstr,'<'), "</dle>", NULL);

    dle = amxml_parse_node_CHAR(o, &errmsg);
    if (dle == NULL) {
	error("amxml_parse_node_CHAR failed: %s\n", errmsg);
    }

    if (dle->compress == COMP_SERVER_FAST) {
	srvcompress = COMP_FAST;
    } else if (dle->compress == COMP_SERVER_BEST) {
	srvcompress = COMP_BEST;
    } else if (dle->compress == COMP_SERVER_CUST) {
	srvcompress = COMP_SERVER_CUST;
	srvcompprog = g_strdup(dle->compprog);
    } else if (dle->compress == COMP_CUST) {
	srvcompress = COMP_CUST;
	clntcompprog = g_strdup(dle->compprog);
    } else {
	srvcompress = COMP_NONE;
    }

    if (dle->encrypt == ENCRYPT_CUST) {
	srvencrypt = ENCRYPT_CUST;
	clnt_encrypt = g_strdup(dle->clnt_encrypt);
	clnt_decrypt_opt = g_strdup(dle->clnt_decrypt_opt);
    } else if (dle->encrypt == ENCRYPT_SERV_CUST) {
	srvencrypt = ENCRYPT_SERV_CUST;
	srv_encrypt = g_strdup(dle->srv_encrypt);
	srv_decrypt_opt = g_strdup(dle->srv_decrypt_opt);
    } else {
	srvencrypt = ENCRYPT_NONE;
    }
    free_dle(dle);
    amfree(o);
}


int
main(
    int		argc,
    char **	argv)
{
    static struct databuf db;
    struct cmdargs *cmdargs = NULL;
    int outfd = -1;
    int rc;
    in_port_t header_port;
    char *q = NULL;
    int a;
    int res;
    config_overrides_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;
    int dumper_setuid;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("dumper-%s\n", VERSION);
	return (0);
    }

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    /* drop root privileges */
    dumper_setuid = set_root_privs(0);

    safe_fd(-1, 0);

    set_pname("dumper");

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

    if (!dumper_setuid) {
	error(_("dumper must be run setuid root"));
    }

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
    }

    safe_cd(); /* do this *after* config_init() */

    check_running_as(RUNNING_AS_ROOT | RUNNING_AS_UID_ONLY);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
    g_fprintf(stderr,
	    _("%s: pid %ld executable %s version %s\n"),
	    get_pname(), (long) getpid(),
	    argv[0], VERSION);
    fflush(stderr);

    /* now, make sure we are a valid user */

    signal(SIGPIPE, SIG_IGN);

    conf_dtimeout = (time_t)getconf_int(CNF_DTIMEOUT);

    protocol_init();

    do {
	if (cmdargs)
	    free_cmdargs(cmdargs);
	cmdargs = getcmd();

	amfree(errstr);
	switch(cmdargs->cmd) {
	case START:
	    if(cmdargs->argc <  2)
		error(_("error [dumper START: not enough args: timestamp]"));
	    dumper_timestamp = newstralloc(dumper_timestamp, cmdargs->argv[1]);
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
	     *   client_port
	     *   ssh_keys
	     *   security_driver
	     *   data_path
	     *   dataport_list
	     *   options
	     */
	    a = 1; /* skip "PORT-DUMP" */

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: handle]"));
		/*NOTREACHED*/
	    }
	    handle = newstralloc(handle, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: port]"));
		/*NOTREACHED*/
	    }
	    header_port = (in_port_t)atoi(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: hostname]"));
		/*NOTREACHED*/
	    }
	    hostname = newstralloc(hostname, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: features]"));
		/*NOTREACHED*/
	    }
	    am_release_feature_set(their_features);
	    their_features = am_string_to_feature(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: diskname]"));
		/*NOTREACHED*/
	    }
	    diskname = newstralloc(diskname, cmdargs->argv[a++]);
	    if (qdiskname != NULL)
		amfree(qdiskname);
	    qdiskname = quote_string(diskname);
	    b64disk = amxml_format_tag("disk", diskname);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: device]"));
		/*NOTREACHED*/
	    }
	    device = newstralloc(device, cmdargs->argv[a++]);
	    b64device = amxml_format_tag("diskdevice", device);
	    if(strcmp(device,"NODEVICE") == 0)
		amfree(device);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: level]"));
		/*NOTREACHED*/
	    }
	    level = atoi(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: dumpdate]"));
		/*NOTREACHED*/
	    }
	    dumpdate = newstralloc(dumpdate, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: program]"));
		/*NOTREACHED*/
	    }
	    progname = newstralloc(progname, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: amandad_path]"));
		/*NOTREACHED*/
	    }
	    amandad_path = newstralloc(amandad_path, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: client_username]"));
	    }
	    client_username = newstralloc(client_username, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: client_port]"));
	    }
	    client_port = newstralloc(client_port, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: ssh_keys]"));
	    }
	    ssh_keys = newstralloc(ssh_keys, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: auth]"));
	    }
	    auth = newstralloc(auth, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: data_path]"));
	    }
	    data_path = data_path_from_string(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: dataport_list]"));
	    }
	    dataport_list = newstralloc(dataport_list, cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: max_warnings]"));
	    }
	    max_warnings = atoi(cmdargs->argv[a++]);

	    if(a >= cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: not enough args: options]"));
	    }
	    options = newstralloc(options, cmdargs->argv[a++]);

	    if(a != cmdargs->argc) {
		error(_("error [dumper PORT-DUMP: too many args: %d != %d]"),
		      cmdargs->argc, a);
	        /*NOTREACHED*/
	    }

	    /* Double-check that 'localhost' resolves properly */
	    if ((res = resolve_hostname("localhost", 0, NULL, NULL) != 0)) {
		errstr = newvstrallocf(errstr,
				     _("could not resolve localhost: %s"),
				     gai_strerror(res));
		q = quote_string(errstr);
		putresult(FAILED, "%s %s\n", handle, q);
		log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname,
			dumper_timestamp, level, errstr);
		amfree(q);
		break;
	    }

	    /* connect outf to chunker/taper port */

	    g_debug(_("Sending header to localhost:%d\n"), header_port);
	    outfd = stream_client("localhost", header_port,
				  STREAM_BUFSIZE, 0, NULL, 0);
	    if (outfd == -1) {
		
		errstr = newvstrallocf(errstr, _("port open: %s"),
				      strerror(errno));
		q = quote_string(errstr);
		putresult(FAILED, "%s %s\n", handle, q);
		log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname,
			dumper_timestamp, level, errstr);
		amfree(q);
		break;
	    }
	    databuf_init(&db, outfd);
	    g_databuf = &db;

	    if (am_has_feature(their_features, fe_req_xml))
		xml_check_options(options); /* note: modifies globals */
	    else
		check_options(options); /* note: modifies globals */

	    rc = startup_dump(hostname,
			      diskname,
			      device,
			      level,
			      dumpdate,
			      progname,
			      amandad_path,
			      client_username,
			      client_port,
			      ssh_keys,
			      auth,
			      options);
	    if (rc != 0) {
		q = quote_string(errstr);
		putresult(rc == 2? FAILED : TRYAGAIN, "%s %s\n",
		    handle, q);
		if (rc == 2)
		    log_add(L_FAIL, "%s %s %s %d [%s]", hostname, qdiskname,
			dumper_timestamp, level, errstr);
		amfree(q);
	    } else {
		do_dump(&db);
		/* try to clean up any defunct processes, since Amanda doesn't
		   wait() for them explicitly */
		while(waitpid(-1, NULL, WNOHANG)> 0);
	    }

	    amfree(amandad_path);
	    amfree(client_username);
	    amfree(client_port);
	    amfree(device);
	    amfree(b64device);
	    amfree(qdiskname);
	    amfree(b64disk);

	    break;

	default:
	    if(cmdargs->argc >= 1) {
		q = quote_string(cmdargs->argv[0]);
	    } else {
		q = stralloc(_("(no input?)"));
	    }
	    putresult(BAD_COMMAND, "%s\n", q);
	    amfree(q);
	    break;
	}

	if (outfd != -1)
	    aclose(outfd);
    } while(cmdargs->cmd != QUIT);
    free_cmdargs(cmdargs);

    log_add(L_INFO, "pid-done %ld", (long)getpid());

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
    size_t written;
    char *m;

    /*
     * If there's no data, do nothing.
     */
    if (db->dataout >= db->datain) {
	return 0;
    }

    /*
     * Write out the buffer
     */
    written = full_write(db->fd, db->dataout,
			(size_t)(db->datain - db->dataout));
    if (written > 0) {
	db->dataout += written;
        dumpbytes += (off_t)written;
    }
    if (dumpbytes >= (off_t)1024) {
	dumpsize += (dumpbytes / (off_t)1024);
	dumpbytes %= (off_t)1024;
    }
    if (written == 0) {
	int save_errno = errno;
	m = vstrallocf(_("data write: %s"), strerror(save_errno));
	amfree(errstr);
	errstr = quote_string(m);
	amfree(m);
	errno = save_errno;
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
	g_fprintf(errf,
		_("? %s: strange [missing size line from sendbackup]\n"),
		get_pname());
	if(errstr == NULL) {
	    errstr = stralloc(_("missing size line from sendbackup"));
	}
	dump_result = max(dump_result, 2);
    }

    if(!ISSET(status, GOT_ENDLINE) && dump_result < 2) {
	g_fprintf(errf,
		_("? %s: strange [missing end line from sendbackup]\n"),
		get_pname());
	if(errstr == NULL) {
	    errstr = stralloc(_("missing end line from sendbackup"));
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
	{ "APPLICATION", file.application, SIZEOF(file.application) },
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

	if (strcmp(tok, "no-op") == 0) {
	    amfree(buf);
	    return;
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
	    if (!errstr) { /* report first error line */
		if (tok == NULL || *tok != '[') {
		    errstr = newvstrallocf(errstr, _("bad remote error: %s"),
					   str);
		} else {
		    char *enderr;

		    tok++;	/* skip over '[' */
		    if ((enderr = strchr(tok, ']')) != NULL)
			*enderr = '\0';
		    errstr = newstralloc(errstr, tok);
		}
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
	g_fprintf(errf, "??");
	dump_result = max(dump_result, 1);
	break;
    }
    g_fprintf(errf, "%s\n", str);
    errf_lines++;
    amfree(buf);
}

static void
add_msg_data(
    const char *	str,
    size_t		len)
{
    char *line, *ch;
    size_t buflen;

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
	g_fprintf(errf,_("? %s: error [partial line in msgbuf: %zu bytes]\n"),
	    get_pname(), buflen);
	g_fprintf(errf,_("? %s: error [partial line in msgbuf: \"%s\"]\n"),
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
	if (*ch == '\n') {
	    /*
	     * Found a newline.  Terminate and process line.
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


static int
log_msgout(
    logtype_t	typ)
{
    char *line;
    int   count = 0;
    int   to_unlink = 1;

    fflush(errf);
    if (fseeko(errf, 0L, SEEK_SET) < 0) {
	dbprintf(_("log_msgout: warning - seek failed: %s\n"), strerror(errno));
    }
    while ((line = agets(errf)) != NULL) {
	if (max_warnings > 0 && errf_lines >= max_warnings && count >= max_warnings) {
	    log_add(typ, "Look in the '%s' file for full error messages", errfname);
	    to_unlink = 0;
	    break;
	}
	if (line[0] != '\0') {
		log_add(typ, "%s", line);
	}
	amfree(line);
	count++;
    }
    amfree(line);

    return to_unlink;
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
    file->blocksize = DISK_BLOCK_BYTES;

    /*
     * If we're doing the compression here, we need to override what
     * sendbackup told us the compression was.
     */
    if (srvcompress != COMP_NONE) {
	file->compressed = 1;
#ifndef UNCOMPRESS_OPT
#define	UNCOMPRESS_OPT	""
#endif
	if (srvcompress == COMP_SERVER_CUST) {
	    g_snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
		     " %s %s |", srvcompprog, "-d");
	    strncpy(file->comp_suffix, "cust", SIZEOF(file->comp_suffix) - 1);
	    file->comp_suffix[SIZEOF(file->comp_suffix) - 1] = '\0';
	    strncpy(file->srvcompprog, srvcompprog, SIZEOF(file->srvcompprog) - 1);
	    file->srvcompprog[SIZEOF(file->srvcompprog) - 1] = '\0';
	} else if ( srvcompress == COMP_CUST ) {
	    g_snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
		     " %s %s |", clntcompprog, "-d");
	    strncpy(file->comp_suffix, "cust", SIZEOF(file->comp_suffix) - 1);
	    file->comp_suffix[SIZEOF(file->comp_suffix) - 1] = '\0';
	    strncpy(file->clntcompprog, clntcompprog, SIZEOF(file->clntcompprog));
	    file->clntcompprog[SIZEOF(file->clntcompprog) - 1] = '\0';
	} else {
	    g_snprintf(file->uncompress_cmd, SIZEOF(file->uncompress_cmd),
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
	if (srv_decrypt_opt) {
	  g_snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		   " %s %s |", srv_encrypt, srv_decrypt_opt); 
	  strncpy(file->srv_decrypt_opt, srv_decrypt_opt, SIZEOF(file->srv_decrypt_opt) - 1);
	  file->srv_decrypt_opt[SIZEOF(file->srv_decrypt_opt) - 1] = '\0';
	} else {
	  g_snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		   " %s |", srv_encrypt); 
	  file->srv_decrypt_opt[0] = '\0';
	}
	strncpy(file->encrypt_suffix, "enc", SIZEOF(file->encrypt_suffix) - 1);
	file->encrypt_suffix[SIZEOF(file->encrypt_suffix) - 1] = '\0';
	strncpy(file->srv_encrypt, srv_encrypt, SIZEOF(file->srv_encrypt) - 1);
	file->srv_encrypt[SIZEOF(file->srv_encrypt) - 1] = '\0';
      } else if ( srvencrypt == ENCRYPT_CUST ) {
	if (clnt_decrypt_opt) {
	  g_snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		   " %s %s |", clnt_encrypt, clnt_decrypt_opt);
	  strncpy(file->clnt_decrypt_opt, clnt_decrypt_opt,
		  SIZEOF(file->clnt_decrypt_opt));
	  file->clnt_decrypt_opt[SIZEOF(file->clnt_decrypt_opt) - 1] = '\0';
	} else {
	  g_snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		   " %s |", clnt_encrypt);
	  file->clnt_decrypt_opt[0] = '\0';
 	}
	g_snprintf(file->decrypt_cmd, SIZEOF(file->decrypt_cmd),
		 " %s %s |", clnt_encrypt, clnt_decrypt_opt);
	strncpy(file->encrypt_suffix, "enc", SIZEOF(file->encrypt_suffix) - 1);
	file->encrypt_suffix[SIZEOF(file->encrypt_suffix) - 1] = '\0';
	strncpy(file->clnt_encrypt, clnt_encrypt, SIZEOF(file->clnt_encrypt) - 1);
	file->clnt_encrypt[SIZEOF(file->clnt_encrypt) - 1] = '\0';
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
    if (dle_str)
	file->dle_str = stralloc(dle_str);
    else
	file->dle_str = NULL;
}

/*
 * Send an Amanda dump header to the output file.
 */
static ssize_t
write_tapeheader(
    int		outfd,
    dumpfile_t *file)
{
    char * buffer;
    size_t written;

    if (debug_dumper > 1)
	dump_dumpfile_t(file);
    buffer = build_header(file, NULL, DISK_BLOCK_BYTES);
    if (!buffer) /* this shouldn't happen */
	error(_("header does not fit in %zd bytes"), (size_t)DISK_BLOCK_BYTES);

    written = full_write(outfd, buffer, DISK_BLOCK_BYTES);
    amfree(buffer);
    if(written == DISK_BLOCK_BYTES)
        return 0;

    return -1;
}

int indexout = -1;

static int
do_dump(
    struct databuf *db)
{
    char *indexfile_tmp = NULL;
    char *indexfile_real = NULL;
    char level_str[NUM_STR_SIZE];
    char *time_str;
    char *fn;
    char *q;
    times_t runtime;
    double dumptime;	/* Time dump took in secs */
    pid_t indexpid = -1;
    char *m;
    int to_unlink = 1;

    startclock();

    if (msg.buf) msg.buf[0] = '\0';	/* reset msg buffer */
    status = 0;
    dump_result = 0;
    dumpbytes = dumpsize = headersize = origsize = (off_t)0;
    fh_init(&file);

    g_snprintf(level_str, SIZEOF(level_str), "%d", level);
    time_str = get_timestamp_from_time(0);
    fn = sanitise_filename(diskname);
    errf_lines = 0;
    errfname = newvstralloc(errfname,
			    AMANDA_DBGDIR,
			    "/log.error", NULL);
    mkdir(errfname, 0700);
    errfname = newvstralloc(errfname,
			    AMANDA_DBGDIR,
			    "/log.error/", hostname,
			    ".", fn,
			    ".", level_str,
			    ".", time_str,
			    ".errout",
			    NULL);
    amfree(fn);
    amfree(time_str);
    if((errf = fopen(errfname, "w+")) == NULL) {
	errstr = newvstrallocf(errstr, "errfile open \"%s\": %s",
			      errfname, strerror(errno));
	amfree(errfname);
	goto failed;
    }

    if (streams[INDEXFD].fd != NULL) {
	indexfile_real = getindexfname(hostname, diskname, dumper_timestamp, level);
	indexfile_tmp = stralloc2(indexfile_real, ".tmp");

	if (mkpdir(indexfile_tmp, 0755, (uid_t)-1, (gid_t)-1) == -1) {
	   errstr = newvstrallocf(errstr,
				 _("err create %s: %s"),
				 indexfile_tmp,
				 strerror(errno));
	   amfree(indexfile_real);
	   amfree(indexfile_tmp);
	   goto failed;
	}
	indexout = open(indexfile_tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (indexout == -1) {
	    errstr = newvstrallocf(errstr, _("err open %s: %s"),
			indexfile_tmp, strerror(errno));
	    goto failed;
	} else {
	    if (runcompress(indexout, &indexpid, COMP_BEST, "index compress") < 0) {
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

    if (!ISSET(status, HEADER_DONE)) {
	dump_result = max(dump_result, 2);
	if (!errstr) errstr = stralloc(_("got no header information"));
    }

    dumpsize -= headersize;		/* don't count the header */
    if (dumpsize <= (off_t)0 && data_path == DATA_PATH_AMANDA) {
	dumpsize = (off_t)0;
	dump_result = max(dump_result, 2);
	if (!errstr) errstr = stralloc(_("got no data"));
    }

    if (data_path == DATA_PATH_DIRECTTCP) {
	dumpsize = origsize;
    }

    if (!ISSET(status, HEADER_DONE)) {
	dump_result = max(dump_result, 2);
	if (!errstr) errstr = stralloc(_("got no header information"));
    }

    if (dumpsize == 0 && data_path == DATA_PATH_AMANDA) {
	dump_result = max(dump_result, 2);
	if (!errstr) errstr = stralloc(_("got no data"));
    }

    if (indexfile_tmp) {
	amwait_t index_status;

	/*@i@*/ aclose(indexout);
	waitpid(indexpid,&index_status,0);
	log_add(L_INFO, "pid-done %ld", (long)indexpid);
	if (rename(indexfile_tmp, indexfile_real) != 0) {
	    log_add(L_WARNING, _("could not rename \"%s\" to \"%s\": %s"),
		    indexfile_tmp, indexfile_real, strerror(errno));
	}
	amfree(indexfile_tmp);
	amfree(indexfile_real);
    }

    /* copy the header in a file on the index dir */
    {
	FILE *a;
	char *s;
	char *f = getheaderfname(hostname, diskname, dumper_timestamp, level);
	a = fopen(f,"w");
	if (a) {
	    s = build_header(&file, NULL, DISK_BLOCK_BYTES);
	    fprintf(a,"%s", s);
	    g_free(s);
	    fclose(a);
	}
	g_free(f);
    }

    if (db->compresspid != -1 && dump_result < 2) {
	amwait_t  wait_status;
	char *errmsg = NULL;

	waitpid(db->compresspid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    errmsg = g_strdup_printf(_("%s terminated with signal %d"),
				     "compress", WTERMSIG(wait_status));
	} else if (WIFEXITED(wait_status)) {
	    if (WEXITSTATUS(wait_status) != 0) {
		errmsg = g_strdup_printf(_("%s exited with status %d"),
					 "compress", WEXITSTATUS(wait_status));
	    }
	} else {
	    errmsg = g_strdup_printf(_("%s got bad exit"),
				     "compress");
	}
	if (errmsg) {
	    g_fprintf(errf, _("? %s\n"), errmsg);
	    g_debug("%s", errmsg);
	    dump_result = max(dump_result, 2);
	    if (!errstr)
		errstr = errmsg;
	    else
		g_free(errmsg);
	}
	log_add(L_INFO, "pid-done %ld", (long)db->compresspid);
	db->compresspid = -1;
    }

    if (db->encryptpid != -1 && dump_result < 2) {
	amwait_t  wait_status;
	char *errmsg = NULL;

	waitpid(db->encryptpid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    errmsg = g_strdup_printf(_("%s terminated with signal %d"),
				     "encrypt", WTERMSIG(wait_status));
	} else if (WIFEXITED(wait_status)) {
	    if (WEXITSTATUS(wait_status) != 0) {
		errmsg = g_strdup_printf(_("%s exited with status %d"),
					 "encrypt", WEXITSTATUS(wait_status));
	    }
	} else {
	    errmsg = g_strdup_printf(_("%s got bad exit"),
				     "encrypt");
	}
	if (errmsg) {
	    g_fprintf(errf, _("? %s\n"), errmsg);
	    g_debug("%s", errmsg);
	    dump_result = max(dump_result, 2);
	    if (!errstr)
		errstr = errmsg;
	    else
		g_free(errmsg);
	}
	log_add(L_INFO, "pid-done %ld", (long)db->encryptpid);
	db->encryptpid  = -1;
    }

    if (dump_result > 1)
	goto failed;

    runtime = stopclock();
    dumptime = g_timeval_to_double(runtime);

    amfree(errstr);
    errstr = alloc(128);
    g_snprintf(errstr, 128, _("sec %s kb %lld kps %3.1lf orig-kb %lld"),
	walltime_str(runtime),
	(long long)dumpsize,
	(isnormal(dumptime) ? ((double)dumpsize / (double)dumptime) : 0.0),
	(long long)origsize);
    m = vstrallocf("[%s]", errstr);
    q = quote_string(m);
    amfree(m);
    putresult(DONE, _("%s %lld %lld %lu %s\n"), handle,
		(long long)origsize,
		(long long)dumpsize,
	        (unsigned long)((double)dumptime+0.5), q);
    amfree(q);

    switch(dump_result) {
    case 0:
	log_add(L_SUCCESS, "%s %s %s %d [%s]", hostname, qdiskname, dumper_timestamp, level, errstr);

	break;

    case 1:
	log_start_multiline();
	log_add(L_STRANGE, "%s %s %d [%s]", hostname, qdiskname, level, errstr);
	to_unlink = log_msgout(L_STRANGE);
	log_end_multiline();

	break;
    }

    if (errf)
	afclose(errf);
    if (errfname) {
	if (to_unlink)
	    unlink(errfname);
	amfree(errfname);
    }

    if (data_path == DATA_PATH_AMANDA)
	aclose(db->fd);

    amfree(errstr);
    dumpfile_free_data(&file);

    return 1;

failed:
    m = vstrallocf("[%s]", errstr);
    q = quote_string(m);
    putresult(FAILED, "%s %s\n", handle, q);
    amfree(q);
    amfree(m);

    aclose(db->fd);
    /* kill all child process */
    if (db->compresspid != -1) {
	g_fprintf(stderr,_("%s: kill compress command\n"),get_pname());
	if (kill(db->compresspid, SIGTERM) < 0) {
	    if (errno != ESRCH) {
		g_fprintf(stderr,_("%s: can't kill compress command: %s\n"), 
		    get_pname(), strerror(errno));
	    } else {
		log_add(L_INFO, "pid-done %ld", (long)db->compresspid);
	    }
	}
	else {
	    waitpid(db->compresspid,NULL,0);
	    log_add(L_INFO, "pid-done %ld", (long)db->compresspid);
	}
    }

    if (db->encryptpid != -1) {
	g_fprintf(stderr,_("%s: kill encrypt command\n"),get_pname());
	if (kill(db->encryptpid, SIGTERM) < 0) {
	    if (errno != ESRCH) {
		g_fprintf(stderr,_("%s: can't kill encrypt command: %s\n"), 
		    get_pname(), strerror(errno));
	    } else {
		log_add(L_INFO, "pid-done %ld", (long)db->encryptpid);
	    }
	}
	else {
	    waitpid(db->encryptpid,NULL,0);
	    log_add(L_INFO, "pid-done %ld", (long)db->encryptpid);
	}
    }

    if (indexpid != -1) {
	g_fprintf(stderr,_("%s: kill index command\n"),get_pname());
	if (kill(indexpid, SIGTERM) < 0) {
	    if (errno != ESRCH) {
		g_fprintf(stderr,_("%s: can't kill index command: %s\n"), 
		    get_pname(),strerror(errno));
	    } else {
		log_add(L_INFO, "pid-done %ld", (long)indexpid);
	    }
	}
	else {
	    waitpid(indexpid,NULL,0);
	    log_add(L_INFO, "pid-done %ld", (long)indexpid);
	}
    }

    log_start_multiline();
    log_add(L_FAIL, _("%s %s %s %d [%s]"), hostname, qdiskname, dumper_timestamp,
	    level, errstr);
    if (errf) {
	to_unlink = log_msgout(L_FAIL);
    }
    log_end_multiline();

    if (errf)
	afclose(errf);
    if (errfname) {
	if (to_unlink)
	    unlink(errfname);
	amfree(errfname);
    }

    if (indexfile_tmp) {
	unlink(indexfile_tmp);
	amfree(indexfile_tmp);
	amfree(indexfile_real);
    }

    amfree(errstr);
    dumpfile_free_data(&file);

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
	errstr = newvstrallocf(errstr, _("mesg read: %s"),
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

    if (ISSET(status, GOT_INFO_ENDLINE) && !ISSET(status, HEADER_DONE)) {
	/* Use the first in the dataport_list */
	in_port_t data_port;
	char *data_host = dataport_list;
	char *s;

	s = strchr(dataport_list, ',');
	if (s) *s = '\0';  /* use first data_port */
	s = strrchr(dataport_list, ':');
	*s = '\0';
	s++;
	data_port = atoi(s);

	SET(status, HEADER_DONE);
	/* time to do the header */
	finish_tapeheader(&file);
	if (write_tapeheader(db->fd, &file)) {
	    errstr = newvstrallocf(errstr, _("write_tapeheader: %s"), 
				  strerror(errno));
	    dump_result = 2;
	    stop_dump();
	    return;
	}
	aclose(db->fd);
	if (data_path == DATA_PATH_AMANDA) {
	    g_debug(_("Sending data to %s:%d\n"), data_host, data_port);
	    db->fd = stream_client(data_host, data_port,
				   STREAM_BUFSIZE, 0, NULL, 0);
	    if (db->fd == -1) {
		errstr = newvstrallocf(errstr,
				       _("Can't open data output stream: %s"),
				       strerror(errno));
		dump_result = 2;
		stop_dump();
		return;
	    }
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
	    if (runcompress(db->fd, &db->compresspid, srvcompress, "data compress") < 0) {
		dump_result = 2;
		stop_dump();
		return;
	    }
	}
	security_stream_read(streams[DATAFD].fd, read_datafd, db);
	set_datafd = 1;
    }

    /*
     * Reset the timeout for future reads
     */
    timeout(conf_dtimeout);
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
	errstr = newvstrallocf(errstr, _("data read: %s"),
	    security_stream_geterror(streams[DATAFD].fd));
	dump_result = 2;
	aclose(db->fd);
	stop_dump();
	return;
    }

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
	aclose(db->fd);
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
	int save_errno = errno;
	errstr = newvstrallocf(errstr, _("data write: %s"), strerror(save_errno));
	dump_result = 2;
	stop_dump();
	return;
    }

    /*
     * Reset the timeout for future reads
     */
    timeout(conf_dtimeout);

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
	errstr = newvstrallocf(errstr, _("index read: %s"),
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
	aclose(indexout);
	return;
    }

    assert(buf != NULL);

    /*
     * We ignore error while writing to the index file.
     */
    if (full_write(fd, buf, (size_t)size) < (size_t)size) {
	/* Ignore error, but schedule another read. */
	if(indexfderror == 0) {
	    indexfderror = 1;
	    log_add(L_INFO, _("Index corrupted for %s:%s"), hostname, qdiskname);
	}
    }
    security_stream_read(streams[INDEXFD].fd, read_indexfd, cookie);
}

static void
handle_filter_stderr(
    void *cookie)
{
    filter_t *filter = cookie;
    ssize_t   nread;
    char     *b, *p;
    gint64    len;

    event_release(filter->event);

    if (filter->buffer == NULL) {
	/* allocate initial buffer */
	filter->buffer = g_malloc(2048);
	filter->first = 0;
	filter->size = 0;
	filter->allocated_size = 2048;
    } else if (filter->first > 0) {
	if (filter->allocated_size - filter->size - filter->first < 1024) {
	    memmove(filter->buffer, filter->buffer + filter->first,
				    filter->size);
	    filter->first = 0;
	}
    } else if (filter->allocated_size - filter->size < 1024) {
	/* double the size of the buffer */
	filter->allocated_size *= 2;
	filter->buffer = g_realloc(filter->buffer, filter->allocated_size);
    }

    nread = read(filter->fd, filter->buffer + filter->first + filter->size,
			     filter->allocated_size - filter->first - filter->size - 2);

    if (nread <= 0) {
	aclose(filter->fd);
	if (filter->size > 0 && filter->buffer[filter->first + filter->size - 1] != '\n') {
	    /* Add a '\n' at end of buffer */
	    filter->buffer[filter->first + filter->size] = '\n';
	    filter->size++;
	}
    } else {
	filter->size += nread;
    }

    /* process all complete lines */
    b = filter->buffer + filter->first;
    filter->buffer[filter->first + filter->size] = '\0';
    while (b < filter->buffer + filter->first + filter->size &&
	   (p = strchr(b, '\n')) != NULL) {
	*p = '\0';
	g_fprintf(errf, _("? %s: %s\n"), filter->name, b);
	if (errstr == NULL) {
	    errstr = stralloc(b);
	}
	len = p - b + 1;
	filter->first += len;
	filter->size -= len;
	b = p + 1;
	dump_result = max(dump_result, 1);
    }

    if (nread <= 0) {
	g_free(filter->buffer);
	g_free(filter);
    } else {
	filter->event = event_register((event_id_t)filter->fd, EV_READFD,
				       handle_filter_stderr, filter);
    }
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
    errstr = newstralloc(errstr, _("data timeout"));
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
    int             i;
    struct cmdargs *cmdargs = NULL;

    /* Check if I have a pending ABORT command */
    cmdargs = get_pending_cmd();
    if (cmdargs) {
	if (cmdargs->cmd != ABORT) {
	    error(_("beurk %d"), cmdargs->cmd);
	}
	amfree(errstr);
	errstr = stralloc(cmdargs->argv[1]);
	free_cmdargs(cmdargs);
    }

    for (i = 0; i < NSTREAMS; i++) {
	if (streams[i].fd != NULL) {
	    security_stream_close(streams[i].fd);
	    streams[i].fd = NULL;
	}
    }
    aclose(indexout);
    aclose(g_databuf->fd);
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
    comp_t	comptype,
    char       *name)
{
    int outpipe[2], rval;
    int errpipe[2];
    filter_t *filter;

    assert(outfd >= 0);
    assert(pid != NULL);

    /* outpipe[0] is pipe's stdin, outpipe[1] is stdout. */
    if (pipe(outpipe) < 0) {
	errstr = newvstrallocf(errstr, _("pipe: %s"), strerror(errno));
	return (-1);
    }

    /* errpipe[0] is pipe's output, outpipe[1] is input. */
    if (pipe(errpipe) < 0) {
	errstr = newvstrallocf(errstr, _("pipe: %s"), strerror(errno));
	return (-1);
    }

    if (comptype != COMP_SERVER_CUST) {
	g_debug("execute: %s %s", COMPRESS_PATH,
		comptype == COMP_BEST ? COMPRESS_BEST_OPT : COMPRESS_FAST_OPT);
    } else {
	g_debug("execute: %s", srvcompprog);
    }
    switch (*pid = fork()) {
    case -1:
	errstr = newvstrallocf(errstr, _("couldn't fork: %s"), strerror(errno));
	aclose(outpipe[0]);
	aclose(outpipe[1]);
	aclose(errpipe[0]);
	aclose(errpipe[1]);
	return (-1);
    default:
	rval = dup2(outpipe[1], outfd);
	if (rval < 0)
	    errstr = newvstrallocf(errstr, _("couldn't dup2: %s"), strerror(errno));
	aclose(outpipe[1]);
	aclose(outpipe[0]);
	aclose(errpipe[1]);
	filter = g_new0(filter_t, 1);
	filter->fd = errpipe[0];
	filter->name = name;
	filter->buffer = NULL;
	filter->size = 0;
	filter->allocated_size = 0;
	filter->event = event_register((event_id_t)filter->fd, EV_READFD,
				       handle_filter_stderr, filter);
	return (rval);
    case 0:
	close(outpipe[1]);
	close(errpipe[0]);
	if (dup2(outpipe[0], 0) < 0) {
	    error(_("err dup2 in: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if (dup2(outfd, 1) == -1) {
	    error(_("err dup2 out: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if (dup2(errpipe[1], 2) == -1) {
	    error(_("err dup2 err: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if (comptype != COMP_SERVER_CUST) {
	    char *base = stralloc(COMPRESS_PATH);
	    log_add(L_INFO, "%s pid %ld", basename(base), (long)getpid());
	    amfree(base);
	    safe_fd(-1, 0);
	    set_root_privs(-1);
	    execlp(COMPRESS_PATH, COMPRESS_PATH, (  comptype == COMP_BEST ?
		COMPRESS_BEST_OPT : COMPRESS_FAST_OPT), (char *)NULL);
	    error(_("error: couldn't exec %s: %s"), COMPRESS_PATH, strerror(errno));
	    /*NOTREACHED*/
	} else if (*srvcompprog) {
	    char *base = stralloc(srvcompprog);
	    log_add(L_INFO, "%s pid %ld", basename(base), (long)getpid());
	    amfree(base);
	    safe_fd(-1, 0);
	    set_root_privs(-1);
	    execlp(srvcompprog, srvcompprog, (char *)0);
	    error(_("error: couldn't exec server custom compression '%s'.\n"), srvcompprog);
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
    int errpipe[2];
    filter_t *filter;

    assert(outfd >= 0);
    assert(pid != NULL);

    /* outpipe[0] is pipe's stdin, outpipe[1] is stdout. */
    if (pipe(outpipe) < 0) {
	errstr = newvstrallocf(errstr, _("pipe: %s"), strerror(errno));
	return (-1);
    }

    /* errpipe[0] is pipe's output, outpipe[1] is input. */
    if (pipe(errpipe) < 0) {
	errstr = newvstrallocf(errstr, _("pipe: %s"), strerror(errno));
	return (-1);
    }

    g_debug("execute: %s", srv_encrypt);
    switch (*pid = fork()) {
    case -1:
	errstr = newvstrallocf(errstr, _("couldn't fork: %s"), strerror(errno));
	aclose(outpipe[0]);
	aclose(outpipe[1]);
	aclose(errpipe[0]);
	aclose(errpipe[1]);
	return (-1);
    default: {
	char *base;
	rval = dup2(outpipe[1], outfd);
	if (rval < 0)
	    errstr = newvstrallocf(errstr, _("couldn't dup2: %s"), strerror(errno));
	aclose(outpipe[1]);
	aclose(outpipe[0]);
	aclose(errpipe[1]);
	filter = g_new0(filter_t, 1);
	filter->fd = errpipe[0];
	base = g_strdup(srv_encrypt);
	filter->name = g_strdup(basename(base));
	amfree(base);
	filter->buffer = NULL;
	filter->size = 0;
	filter->allocated_size = 0;
	filter->event = event_register((event_id_t)filter->fd, EV_READFD,
				       handle_filter_stderr, filter);
	return (rval);
	}
    case 0: {
	char *base;
	if (dup2(outpipe[0], 0) < 0) {
	    error(_("err dup2 in: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if (dup2(outfd, 1) < 0 ) {
	    error(_("err dup2 out: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if (dup2(errpipe[1], 2) == -1) {
	    error(_("err dup2 err: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	close(errpipe[0]);
	base = stralloc(srv_encrypt);
	log_add(L_INFO, "%s pid %ld", basename(base), (long)getpid());
	amfree(base);
	safe_fd(-1, 0);
	if ((encrypttype == ENCRYPT_SERV_CUST) && *srv_encrypt) {
	    set_root_privs(-1);
	    execlp(srv_encrypt, srv_encrypt, (char *)0);
	    error(_("error: couldn't exec server custom encryption '%s'.\n"), srv_encrypt);
	    /*NOTREACHED*/
	}
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
	errstr = newvstrallocf(errstr, _("[request failed: %s]"),
	    security_geterror(sech));
	*response_error = 1;
	return;
    }

    extra = NULL;
    memset(ports, 0, SIZEOF(ports));
    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	g_fprintf(stderr, _("got nak response:\n----\n%s\n----\n\n"), pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || strcmp(tok, "ERROR") != 0)
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    errstr = newvstrallocf(errstr, "NAK: %s", tok);
	    *response_error = 1;
	} else {
bad_nak:
	    errstr = newvstrallocf(errstr, "request NAK");
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	errstr = newvstrallocf(errstr, _("received strange packet type %s: %s"),
	    pkt_type2str(pkt->type), pkt->body);
	*response_error = 1;
	return;
    }

    dbprintf(_("got response:\n----\n%s\n----\n\n"), pkt->body);

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
		tok = _("[bogus error packet]");
	    errstr = newvstrallocf(errstr, "%s", tok);
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
		    extra = vstrallocf(
				_("CONNECT token is \"%s\": expected \"%s\""),
				tok ? tok : "(null)",
				streams[i].name);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = vstrallocf(
			_("CONNECT %s token is \"%s\": expected a port number"),
			streams[i].name, tok ? tok : "(null)");
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
		extra = vstrallocf(_("OPTIONS token is missing"));
		goto parse_error;
	    }

	    while((p = strchr(tok, ';')) != NULL) {
		char ch;
		*p++ = '\0';
		if(strncmp_const_skip(tok, "features=", tok, ch) == 0) {
		    char *u = strchr(tok, ';');
		    ch = ch;
		    if (u)
		       *u = '\0';
		    am_release_feature_set(their_features);
		    if((their_features = am_string_to_feature(tok)) == NULL) {
			errstr = newvstrallocf(errstr,
					      _("OPTIONS: bad features value: %s"),
					      tok);
			goto parse_error;
		    }
		    if (u)
		       *u = ';';
		}
		tok = p;
	    }
	    continue;
	}

	extra = vstrallocf(_("next token is \"%s\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\""),
			  tok ? tok : "(null)");
	goto parse_error;
    }

    if (dumper_kencrypt == KENCRYPT_WILL_DO)
	dumper_kencrypt = KENCRYPT_YES;

    /*
     * Connect the streams to their remote ports
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (ports[i] == -1)
	    continue;
	streams[i].fd = security_stream_client(sech, ports[i]);
	if (streams[i].fd == NULL) {
	    errstr = newvstrallocf(errstr,
		_("[could not connect %s stream: %s]"),
		streams[i].name,
		security_geterror(sech));
	    goto connect_error;
	}
    }

    /*
     * Authenticate the streams
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (streams[i].fd == NULL)
	    continue;
	if (security_stream_auth(streams[i].fd) < 0) {
	    errstr = newvstrallocf(errstr,
		_("[could not authenticate %s stream: %s]"),
		streams[i].name, 
		security_stream_geterror(streams[i].fd));
	    goto connect_error;
	}
    }

    /*
     * The MESGFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (streams[MESGFD].fd == NULL || streams[DATAFD].fd == NULL) {
	errstr = newvstrallocf(errstr, _("[couldn't open MESG or INDEX streams]"));
	goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    return;

parse_error:
    errstr = newvstrallocf(errstr,
			  _("[parse of reply message failed: %s]"),
			  extra ? extra : _("(no additional information)"));
    amfree(extra);
    *response_error = 2;
    return;

connect_error:
    stop_dump();
    *response_error = 1;
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
        } else if(strcmp(string, "client_port")==0) {
                return (client_port);
        } else if(strcmp(string, "ssh_keys")==0) {
                return (ssh_keys);
        } else if(strcmp(string, "kencrypt")==0) {
		if (dumper_kencrypt == KENCRYPT_YES)
                    return ("yes");
		else
		    return (NULL);
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
    const char *client_port,
    const char *ssh_keys,
    const char *auth,
    const char *options)
{
    char level_string[NUM_STR_SIZE];
    char *req = NULL;
    int response_error;
    const security_driver_t *secdrv;
    char *application_api;
    int has_features;
    int has_hostname;
    int has_device;
    int has_config;

    (void)disk;			/* Quiet unused parameter warning */
    (void)amandad_path;		/* Quiet unused parameter warning */
    (void)client_username;	/* Quiet unused parameter warning */
    (void)client_port;		/* Quiet unused parameter warning */
    (void)ssh_keys;		/* Quiet unused parameter warning */
    (void)auth;			/* Quiet unused parameter warning */

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

    g_snprintf(level_string, SIZEOF(level_string), "%d", level);
    if(strcmp(progname, "DUMP") == 0
       || strcmp(progname, "GNUTAR") == 0) {
	application_api = "";
    } else {
	application_api = "BACKUP ";
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
		    has_config   ? get_config_name() : "",
		    has_config   ? ";" : "",
		    "\n",
		    NULL);

    amfree(dle_str);
    if (am_has_feature(their_features, fe_req_xml)) {
	char *p = NULL;
	char *pclean;
	vstrextend(&p, "<dle>\n", NULL);
	if (*application_api != '\0') {
	    vstrextend(&p, "  <program>APPLICATION</program>\n", NULL);
	} else {
	    vstrextend(&p, "  <program>", progname, "</program>\n", NULL);
	}
	vstrextend(&p, "  ", b64disk, "\n", NULL);
	if (device && has_device) {
	    vstrextend(&p, "  ", b64device, "\n",
		       NULL);
	}
	vstrextend(&p, "  <level>", level_string, "</level>\n", NULL);
	vstrextend(&p, options+1, "</dle>\n", NULL);
	pclean = clean_dle_str_for_client(p, their_features);
	vstrextend(&req, pclean, NULL);
	amfree(pclean);
	dle_str = p;
    } else if (*application_api != '\0') {
	errstr = newvstrallocf(errstr,
		_("[does not support application-api]"));
	amfree(req);
	return 2;
    } else {
	if (auth == NULL) {
	    auth = "BSD";
	}
	vstrextend(&req,
		   progname,
		   " ", qdiskname,
		   " ", device && has_device ? device : "",
		   " ", level_string,
		   " ", dumpdate,
		   " OPTIONS ", options,
		   "\n",
		   NULL);
    }

    dbprintf(_("send request:\n----\n%s\n----\n\n"), req);
    secdrv = security_getdriver(auth);
    if (secdrv == NULL) {
	errstr = newvstrallocf(errstr,
		_("[could not find security driver '%s']"), auth);
	amfree(req);
	return 2;
    }

    protocol_sendreq(hostname, secdrv, dumper_get_security_conf, req,
	STARTUP_TIMEOUT, sendbackup_response, &response_error);

    amfree(req);

    protocol_run();
    return (response_error);
}
