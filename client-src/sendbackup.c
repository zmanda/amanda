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
/* 
 * $Id: sendbackup.c,v 1.88 2006/07/25 18:27:56 martinea Exp $
 *
 * common code for the sendbackup-* programs.
 */

#include "amanda.h"
#include "match.h"
#include "sendbackup.h"
#include "clock.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "arglist.h"
#include "getfsent.h"
#include "conffile.h"
#include "amandates.h"
#include "stream.h"

#define sendbackup_debug(i, ...) do {	\
	if ((i) <= debug_sendbackup) {	\
	    dbprintf(__VA_LIST__);	\
	}				\
} while (0)

#define TIMEOUT 30

pid_t comppid = (pid_t)-1;
pid_t dumppid = (pid_t)-1;
pid_t tarpid = (pid_t)-1;
pid_t encpid = (pid_t)-1;
pid_t indexpid = (pid_t)-1;
pid_t application_api_pid = (pid_t)-1;
char *errorstr = NULL;

int datafd;
int mesgfd;
int indexfd;

g_option_t *g_options = NULL;

long dump_size = -1;

backup_program_t *program = NULL;
dle_t *gdle = NULL;

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static char *amandad_auth = NULL;

/* local functions */
int main(int argc, char **argv);
char *childstr(pid_t pid);
int check_status(pid_t pid, amwait_t w, int mesgfd);

pid_t pipefork(void (*func)(void), char *fname, int *stdinfd,
		int stdoutfd, int stderrfd);
int check_result(int mesgfd);
void parse_backup_messages(dle_t *dle, int mesgin);
static void process_dumpline(char *str);
static void save_fd(int *, int);
void application_api_info_tapeheader(int mesgfd, char *prog, dle_t *dle);

int fdprintf(int fd, char *format, ...) G_GNUC_PRINTF(2, 3);

int
fdprintf(
    int   fd,
    char *format,
    ...)
{
    va_list  argp;
    char    *s;
    int      r;

    arglist_start(argp, format);
    s = g_strdup_vprintf(format, argp);
    arglist_end(argp);

    r = full_write(fd, s, strlen(s));
    amfree(s);
    return r;
}

int
main(
    int		argc,
    char **	argv)
{
    int interactive = 0;
    int level = 0;
    int mesgpipe[2];
    dle_t *dle = NULL;
    char *dumpdate, *stroptions;
    char *qdisk = NULL;
    char *qamdevice = NULL;
    char *line = NULL;
    char *err_extra = NULL;
    char *s;
    int i;
    int ch;
    GSList *errlist;
    FILE   *mesgstream;
    am_level_t *alevel;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("sendbackup-%s\n", VERSION);
	return (0);
    }

    /* initialize */
    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(DATA_FD_OFFSET, DATA_FD_COUNT*2);
    openbsd_fd_inform();

    safe_cd();

    set_pname("sendbackup");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /* Don't die when interrupt received */
    signal(SIGINT, SIG_IGN);

    if(argc > 1 && strcmp(argv[1],"-t") == 0) {
	interactive = 1;
	argc--;
	argv++;
    } else {
	interactive = 0;
    }

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_syslog);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    dbprintf(_("Version %s\n"), VERSION);

    if(argc > 2 && strcmp(argv[1], "amandad") == 0) {
	amandad_auth = stralloc(argv[2]);
    }

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    config_init(CONFIG_INIT_CLIENT, NULL);
    /* (check for config errors comes later) */

    check_running_as(RUNNING_AS_CLIENT_LOGIN);

    if(interactive) {
	/*
	 * In interactive (debug) mode, the backup data is sent to
	 * /dev/null and none of the network connections back to driver
	 * programs on the tape host are set up.  The index service is
	 * run and goes to stdout.
	 */
	g_fprintf(stderr, _("%s: running in interactive test mode\n"), get_pname());
	fflush(stderr);
    }

    qdisk = NULL;
    dumpdate = NULL;
    stroptions = NULL;

    for(; (line = agets(stdin)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	if(interactive) {
	    g_fprintf(stderr, "%s> ", get_pname());
	    fflush(stderr);
	}
	if(strncmp_const(line, "OPTIONS ") == 0) {
	    g_options = parse_g_options(line+8, 1);
	    if(!g_options->hostname) {
		g_options->hostname = alloc(MAX_HOSTNAME_LENGTH+1);
		gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
	    }

	    if (g_options->config) {
		/* overlay this configuration on the existing (nameless) configuration */
		config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
			    g_options->config);

		dbrename(get_config_name(), DBG_SUBDIR_CLIENT);
	    }

	    /* check for any config errors now */
	    if (config_errors(&errlist) >= CFGERR_ERRORS) {
		char *errstr = config_errors_to_error_string(errlist);
		g_printf("%s\n", errstr);
		dbclose();
		return 1;
	    }

	    if (am_has_feature(g_options->features, fe_req_xml)) {
		break;
	    }
	    continue;
	}

	if (dle && dle->program != NULL) {
	    err_extra = _("multiple requests");
	    goto err;
	}

	dbprintf(_("  sendbackup req: <%s>\n"), line);
	dle = alloc_dle();

	s = line;
	ch = *s++;

	skip_whitespace(s, ch);			/* find the program name */
	if(ch == '\0') {
	    err_extra = _("no program name");
	    goto err;				/* no program name */
	}
	dle->program = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

        if (strcmp(dle->program, "APPLICATION")==0) {
            dle->program_is_application_api=1;
            skip_whitespace(s, ch);             /* find dumper name */
            if (ch == '\0') {
                goto err;                       /* no program */
            }
            dle->program = s - 1;
            skip_non_whitespace(s, ch);
            s[-1] = '\0';
        }
	dle->program = stralloc(dle->program);

	skip_whitespace(s, ch);			/* find the disk name */
	if(ch == '\0') {
	    err_extra = _("no disk name");
	    goto err;				/* no disk name */
	}

	amfree(qdisk);
	qdisk = s - 1;
	ch = *qdisk;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	qdisk = stralloc(qdisk);
	dle->disk = unquote_string(qdisk);

	skip_whitespace(s, ch);			/* find the device or level */
	if (ch == '\0') {
	    err_extra = _("bad level");
	    goto err;
	}

	if(!isdigit((int)s[-1])) {
	    amfree(qamdevice);
	    qamdevice = s - 1;
	    ch = *qamdevice;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    qamdevice = stralloc(qamdevice);
	    dle->device = unquote_string(qamdevice);
	    skip_whitespace(s, ch);		/* find level number */
	}
	else {
	    dle->device = stralloc(dle->disk);
	    qamdevice = stralloc(qdisk);
	}
						/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    err_extra = _("bad level");
	    goto err;				/* bad level */
	}
	skip_integer(s, ch);
	alevel = g_new0(am_level_t, 1);
	alevel->level = level;
	dle->levellist = g_slist_append(dle->levellist, alevel);

	skip_whitespace(s, ch);			/* find the dump date */
	if(ch == '\0') {
	    err_extra = _("no dumpdate");
	    goto err;				/* no dumpdate */
	}
	amfree(dumpdate);
	dumpdate = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	dumpdate = stralloc(dumpdate);

	skip_whitespace(s, ch);			/* find the options keyword */
	if(ch == '\0') {
	    err_extra = _("no options");
	    goto err;				/* no options */
	}
	if(strncmp_const_skip(s - 1, "OPTIONS ", s, ch) != 0) {
	    err_extra = _("no OPTIONS keyword");
	    goto err;				/* no options */
	}
	skip_whitespace(s, ch);			/* find the options string */
	if(ch == '\0') {
	    err_extra = _("bad options string");
	    goto err;				/* no options */
	}
	amfree(stroptions);
	stroptions = stralloc(s - 1);
    }
    amfree(line);
    if (g_options == NULL) {
	g_printf(_("ERROR [Missing OPTIONS line in sendbackup input]\n"));
	error(_("Missing OPTIONS line in sendbackup input\n"));
	/*NOTREACHED*/
    }

    if (am_has_feature(g_options->features, fe_req_xml)) {
	char *errmsg = NULL;

	dle = amxml_parse_node_FILE(stdin, &errmsg);
	if (errmsg) {
	    err_extra = errmsg;
	    goto err;
	}
	if (!dle) {
	    err_extra = _("One DLE required");
	    goto err;
	} else if (dle->next) {
	    err_extra = _("Only one DLE allowed");
	    goto err;
	}

	qdisk = quote_string(dle->disk);
	if (dle->device == NULL)
	    dle->device = stralloc(dle->disk);
	qamdevice = quote_string(dle->device);
	dumpdate = stralloc("NODATE");
	stroptions = stralloc("");
    } else {
	parse_options(stroptions, dle, g_options->features, 0);
    }
    gdle = dle;

    if (dle->program   == NULL ||
	dle->disk      == NULL ||
	dle->device    == NULL ||
	dle->levellist == NULL ||
	dumpdate       == NULL) {
	err_extra = _("no valid sendbackup request");
	goto err;
    }

    if (g_slist_length(dle->levellist) != 1) {
	err_extra = _("Too many level");
	goto err;
    }

    alevel = (am_level_t *)dle->levellist->data;
    level = alevel->level;
    dbprintf(_("  Parsed request as: program `%s'\n"), dle->program);
    dbprintf(_("                     disk `%s'\n"), qdisk);
    dbprintf(_("                     device `%s'\n"), qamdevice);
    dbprintf(_("                     level %d\n"), level);
    dbprintf(_("                     since %s\n"), dumpdate);
    dbprintf(_("                     options `%s'\n"), stroptions);
    dbprintf(_("                     datapath `%s'\n"),
			    data_path_to_string(dle->data_path));

    if (dle->program_is_application_api==1) {
	/* check that the application_api exist */
    } else {
	for(i = 0; programs[i]; i++) {
	    if (strcmp(programs[i]->name, dle->program) == 0) {
		break;
	    }
	}
	if (programs[i] == NULL) {
	    dbprintf(_("ERROR [%s: unknown program %s]\n"), get_pname(),
		     dle->program);
	    error(_("ERROR [%s: unknown program %s]"), get_pname(),
		  dle->program);
	    /*NOTREACHED*/
	}
	program = programs[i];
    }

    if(!interactive) {
	datafd = DATA_FD_OFFSET + 0;
	mesgfd = DATA_FD_OFFSET + 2;
	indexfd = DATA_FD_OFFSET + 4;
    }
    if (!dle->create_index)
	indexfd = -1;

    if (dle->auth && amandad_auth) {
	if(strcasecmp(dle->auth, amandad_auth) != 0) {
	    g_printf(_("ERROR [client configured for auth=%s while server requested '%s']\n"),
		   amandad_auth, dle->auth);
	    exit(-1);
	}
    }

    if (dle->kencrypt) {
	g_printf("KENCRYPT\n");
    }

    g_printf(_("CONNECT DATA %d MESG %d INDEX %d\n"),
	   DATA_FD_OFFSET, DATA_FD_OFFSET+1,
	   indexfd == -1 ? -1 : DATA_FD_OFFSET+2);
    g_printf(_("OPTIONS "));
    if(am_has_feature(g_options->features, fe_rep_options_features)) {
	g_printf("features=%s;", our_feature_string);
    }
    if(am_has_feature(g_options->features, fe_rep_options_hostname)) {
	g_printf("hostname=%s;", g_options->hostname);
    }
    if (!am_has_feature(g_options->features, fe_rep_options_features) &&
	!am_has_feature(g_options->features, fe_rep_options_hostname)) {
	g_printf(";");
    }
    g_printf("\n");
    fflush(stdout);
    if (freopen("/dev/null", "w", stdout) == NULL) {
	dbprintf(_("Error redirecting stdout to /dev/null: %s\n"),
		 strerror(errno));
        exit(1);
    }

    if(interactive) {
      if((datafd = open("/dev/null", O_RDWR)) < 0) {
	error(_("ERROR [open of /dev/null for debug data stream: %s]\n"),
		strerror(errno));
	/*NOTREACHED*/
      }
      mesgfd = 2;
      indexfd = 1;
    }

    if(!interactive) {
      if(datafd == -1 || mesgfd == -1 || (dle->create_index && indexfd == -1)) {
        dbclose();
        exit(1);
      }
    }

    if (merge_dles_properties(dle, 1) == 0) {
	g_debug("merge_dles_properties failed");
	exit(1);
    }
    mesgstream = fdopen(mesgfd,"w");
    run_client_scripts(EXECUTE_ON_PRE_DLE_BACKUP, g_options, dle, mesgstream);
    fflush(mesgstream);

    if (dle->program_is_application_api==1) {
	guint j;
	char *cmd=NULL;
	GPtrArray *argv_ptr;
	char levelstr[20];
	backup_support_option_t *bsu;
	char *compopt = NULL;
	char *encryptopt = skip_argument;
	int compout, dumpout;
	GSList    *scriptlist;
	script_t  *script;
	time_t     cur_dumptime;
	int        result;
	GPtrArray *errarray;
	int        errfd[2];
	FILE      *dumperr;

	/*  apply client-side encryption here */
	if ( dle->encrypt == ENCRYPT_CUST ) {
	    encpid = pipespawn(dle->clnt_encrypt, STDIN_PIPE, 0,
			       &compout, &datafd, &mesgfd,
			       dle->clnt_encrypt, encryptopt, NULL);
	    dbprintf(_("encrypt: pid %ld: %s\n"), (long)encpid, dle->clnt_encrypt);
	} else {
	    compout = datafd;
	    encpid = -1;
	}

	/*  now do the client-side compression */
	if(dle->compress == COMP_FAST || dle->compress == COMP_BEST) {
	    compopt = skip_argument;
#if defined(COMPRESS_BEST_OPT) && defined(COMPRESS_FAST_OPT)
	    if(dle->compress == COMP_BEST) {
		compopt = COMPRESS_BEST_OPT;
	    } else {
		compopt = COMPRESS_FAST_OPT;
	    }
#endif
	    comppid = pipespawn(COMPRESS_PATH, STDIN_PIPE, 0,
				&dumpout, &compout, &mesgfd,
				COMPRESS_PATH, compopt, NULL);
	    if(compopt != skip_argument) {
		dbprintf(_("compress pid %ld: %s %s\n"),
			 (long)comppid, COMPRESS_PATH, compopt);
	    } else {
		dbprintf(_("compress pid %ld: %s\n"), (long)comppid, COMPRESS_PATH);
	    }
	} else if (dle->compress == COMP_CUST) {
	    compopt = skip_argument;
	    comppid = pipespawn(dle->compprog, STDIN_PIPE, 0,
				&dumpout, &compout, &mesgfd,
				dle->compprog, compopt, NULL);
	    if(compopt != skip_argument) {
		dbprintf(_("pid %ld: %s %s\n"),
			 (long)comppid, dle->compprog, compopt);
	    } else {
		dbprintf(_("pid %ld: %s\n"), (long)comppid, dle->compprog);
	    }
	} else {
	    dumpout = compout;
	    comppid = -1;
	}

	cur_dumptime = time(0);
	bsu = backup_support_option(dle->program, g_options, dle->disk,
				    dle->device, &errarray);
	if (!bsu) {
	    char  *errmsg;
	    char  *qerrmsg;
	    guint  i;
	    for (i=0; i < errarray->len; i++) {
		errmsg = g_ptr_array_index(errarray, i);
		qerrmsg = quote_string(errmsg);
		fdprintf(mesgfd,
			  _("sendbackup: error [Application '%s': %s]\n"),
			  dle->program, errmsg);
		dbprintf("aa: %s\n",qerrmsg);
		amfree(qerrmsg);
	    }
	    if (i == 0) { /* no errarray */
		errmsg = vstrallocf(_("Can't execute application '%s'"),
				    dle->program);
		qerrmsg = quote_string(errmsg);
		fdprintf(mesgfd, _("sendbackup: error [%s]\n"), errmsg);
		dbprintf(_("ERROR %s\n"), qerrmsg);
		amfree(qerrmsg);
		amfree(errmsg);
	    }
	    return 0;
	}

	if (pipe(errfd) < 0) {
	    char  *errmsg;
	    char  *qerrmsg;
	    errmsg = vstrallocf(_("Application '%s': can't create pipe"),
				    dle->program);
	    qerrmsg = quote_string(errmsg);
	    fdprintf(mesgfd, _("sendbackup: error [%s]\n"), errmsg);
	    dbprintf(_("ERROR %s\n"), qerrmsg);
	    amfree(qerrmsg);
	    amfree(errmsg);
	    return 0;
	}

	switch(application_api_pid=fork()) {
	case 0:
	    application_api_info_tapeheader(mesgfd, dle->program, dle);

	    /* find directt-tcp address from indirect direct-tcp */
	    if (dle->data_path == DATA_PATH_DIRECTTCP &&
		bsu->data_path_set & DATA_PATH_DIRECTTCP &&
		strncmp(dle->directtcp_list->data, "255.255.255.255:", 16) == 0) {
		char *indirect_tcp;
		char *str_port;
		in_port_t port;
		int fd;
		char buffer[32770];
		int size;
		char *s, *s1;

		indirect_tcp = g_strdup(dle->directtcp_list->data);
		g_slist_free(dle->directtcp_list);
		dle->directtcp_list = NULL;
		str_port = strchr(indirect_tcp, ':');
		str_port++;
		port = atoi(str_port);
		fd = stream_client("localhost", port, 32768, 32768, NULL, 0);
		if (fd <= 0) {
		    g_debug("Failed to connect to indirect-direct-tcp port: %s",
			    strerror(errno));
		    exit(1);
		}
		size = full_read(fd, buffer, 32768);
		if (size <= 0) {
		    g_debug("Failed to read from indirect-direct-tcp port: %s",
			    strerror(errno));
		    exit(1);
		}
		buffer[size++] = ' ';
		buffer[size] = '\0';
		s1 = buffer;
		while ((s = strchr(s1, ' ')) != NULL) {
		    *s++ = '\0';
		    dle->directtcp_list = g_slist_append(dle->directtcp_list, g_strdup(s1));
		    s1 = s;
		}
		amfree(indirect_tcp);
	    }

	    argv_ptr = g_ptr_array_new();
	    cmd = vstralloc(APPLICATION_DIR, "/", dle->program, NULL);
	    g_ptr_array_add(argv_ptr, stralloc(dle->program));
	    g_ptr_array_add(argv_ptr, stralloc("backup"));
	    if (bsu->message_line == 1) {
		g_ptr_array_add(argv_ptr, stralloc("--message"));
		g_ptr_array_add(argv_ptr, stralloc("line"));
	    }
	    if (g_options->config && bsu->config == 1) {
		g_ptr_array_add(argv_ptr, stralloc("--config"));
		g_ptr_array_add(argv_ptr, stralloc(g_options->config));
	    }
	    if (g_options->hostname && bsu->host == 1) {
		g_ptr_array_add(argv_ptr, stralloc("--host"));
		g_ptr_array_add(argv_ptr, stralloc(g_options->hostname));
	    }
	    if (dle->disk && bsu->disk == 1) {
		g_ptr_array_add(argv_ptr, stralloc("--disk"));
		g_ptr_array_add(argv_ptr, stralloc(dle->disk));
	    }
	    g_ptr_array_add(argv_ptr, stralloc("--device"));
	    g_ptr_array_add(argv_ptr, stralloc(dle->device));
	    if (level <= bsu->max_level) {
		g_ptr_array_add(argv_ptr, stralloc("--level"));
		g_snprintf(levelstr,19,"%d",level);
		g_ptr_array_add(argv_ptr, stralloc(levelstr));
	    }
	    if (indexfd != -1 && bsu->index_line == 1) {
		g_ptr_array_add(argv_ptr, stralloc("--index"));
		g_ptr_array_add(argv_ptr, stralloc("line"));
	    }
	    if (dle->record && bsu->record == 1) {
		g_ptr_array_add(argv_ptr, stralloc("--record"));
	    }
	    application_property_add_to_argv(argv_ptr, dle, bsu,
					     g_options->features);

	    for (scriptlist = dle->scriptlist; scriptlist != NULL;
		 scriptlist = scriptlist->next) {
		script = (script_t *)scriptlist->data;
		if (script->result && script->result->proplist) {
		    property_add_to_argv(argv_ptr, script->result->proplist);
		}
	    }

	    g_ptr_array_add(argv_ptr, NULL);
	    dbprintf(_("%s: running \"%s\n"), get_pname(), cmd);
	    for (j = 1; j < argv_ptr->len - 1; j++)
		dbprintf(" %s\n", (char *)g_ptr_array_index(argv_ptr,j));
	    dbprintf(_("\"\n"));
	    if(dup2(dumpout, 1) == -1) {
		error(_("Can't dup2: %s"),strerror(errno));
		/*NOTREACHED*/
	    }
	    if (dup2(errfd[1], 2) == -1) {
		error(_("Can't dup2: %s"),strerror(errno));
		/*NOTREACHED*/
	    }
	    if(dup2(mesgfd, 3) == -1) {
		error(_("Can't dup2: %s"),strerror(errno));
		/*NOTREACHED*/
	    }
	    if(indexfd > 0) {
		if(dup2(indexfd, 4) == -1) {
		    error(_("Can't dup2: %s"),strerror(errno));
		    /*NOTREACHED*/
		}
		fcntl(indexfd, F_SETFD, 0);
	    }
	    if (indexfd != 0) {
		safe_fd(3, 2);
	    } else {
		safe_fd(3, 1);
	    }
	    execve(cmd, (char **)argv_ptr->pdata, safe_env());
	    exit(1);
	    break;
 
	default:
	    break;
	case -1:
	    error(_("%s: fork returned: %s"), get_pname(), strerror(errno));
	}

	close(errfd[1]);
	dumperr = fdopen(errfd[0],"r");
	if (!dumperr) {
	    error(_("Can't fdopen: %s"), strerror(errno));
	    /*NOTREACHED*/
	}

	result = 0;
	while ((line = agets(dumperr)) != NULL) {
	    if (strlen(line) > 0) {
		fdprintf(mesgfd, "sendbackup: error [%s]\n", line);
		dbprintf("error: %s\n", line);
		result = 1;
	    }
	    amfree(line);
	}

	result |= check_result(mesgfd);
	if (result == 0) {
	    char *amandates_file;

	    amandates_file = getconf_str(CNF_AMANDATES);
	    if(start_amandates(amandates_file, 1)) {
		amandates_updateone(dle->disk, level, cur_dumptime);
		finish_amandates();
		free_amandates();
	    } else {
		if (GPOINTER_TO_INT(dle->estimatelist->data) == ES_CALCSIZE &&
		    bsu->calcsize) {
		    error(_("error [opening %s for writing: %s]"),
			  amandates_file, strerror(errno));
		} else {
		    g_debug(_("non-fatal error opening '%s' for writing: %s]"),
			    amandates_file, strerror(errno));
		}
	    }
	}
	amfree(bsu);
     } else {
	if(!interactive) {
	    /* redirect stderr */
	    if(dup2(mesgfd, 2) == -1) {
		dbprintf(_("Error redirecting stderr to fd %d: %s\n"),
			 mesgfd, strerror(errno));
		dbclose();
		exit(1);
	    }
	}
 
	if(pipe(mesgpipe) == -1) {
	    s = strerror(errno);
	    dbprintf(_("error [opening mesg pipe: %s]\n"), s);
	    error(_("error [opening mesg pipe: %s]"), s);
	}

	program->start_backup(dle, g_options->hostname,
			      datafd, mesgpipe[1], indexfd);
	dbprintf(_("Started backup\n"));
	parse_backup_messages(dle, mesgpipe[0]);
	dbprintf(_("Parsed backup messages\n"));
    }

    run_client_scripts(EXECUTE_ON_POST_DLE_BACKUP, g_options, dle, mesgstream);
    fflush(mesgstream);

    amfree(qdisk);
    amfree(qamdevice);
    amfree(dumpdate);
    amfree(stroptions);
    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;
    free_g_options(g_options);

    dbclose();

    return 0;

 err:
    if (err_extra) {
	g_printf(_("ERROR FORMAT ERROR IN REQUEST PACKET '%s'\n"), err_extra);
	dbprintf(_("REQ packet is bogus: %s\n"), err_extra);
    } else {
	g_printf(_("ERROR FORMAT ERROR IN REQUEST PACKET\n"));
	dbprintf(_("REQ packet is bogus\n"));
    }

    amfree(qdisk);
    amfree(qamdevice);
    amfree(dumpdate);
    amfree(stroptions);
    amfree(our_feature_string);

    dbclose();
    return 1;
}


/*
 * Returns a string for a child process.  Checks the saved dump and
 * compress pids to see which it is.
 */

char *
childstr(
    pid_t pid)
{
    if(pid == dumppid) return program->backup_name;
    if(pid == comppid) return "compress";
    if(pid == encpid) return "encrypt";
    if(pid == indexpid) return "index";
    if(pid == application_api_pid) {
	if (!gdle) {
	    dbprintf("gdle == NULL\n");
	    return "gdle == NULL";
	}
	return gdle->program;
    }
    return "unknown";
}


/*
 * Determine if the child return status really indicates an error.
 * If so, add the error message to the error string; more than one
 * child can have an error.
 */

int
check_status(
    pid_t	pid,
    amwait_t	w,
    int		mesgfd)
{
    char *thiserr = NULL;
    char *str, *strX;
    int ret, sig, rc;

    str = childstr(pid);

    if(WIFSIGNALED(w)) {
	ret = 0;
	rc = sig = WTERMSIG(w);
    } else {
	sig = 0;
	rc = ret = WEXITSTATUS(w);
    }

    if(pid == indexpid) {
	/*
	 * Treat an index failure (other than signal) as a "STRANGE"
	 * rather than an error so the dump goes ahead and gets processed
	 * but the failure is noted.
	 */
	if(ret != 0) {
	    fdprintf(mesgfd, _("? index %s returned %d\n"), str, ret);
	    rc = 0;
	}
	indexpid = -1;
	strX = "index";
    } else if(pid == comppid) {
	/*
	 * compress returns 2 sometimes, but it is ok.
	 */
#ifndef HAVE_GZIP
	if(ret == 2) {
	    rc = 0;
	}
#endif
	comppid = -1;
	strX = "compress";
    } else if(pid == dumppid && tarpid == -1) {
        /*
	 * Ultrix dump returns 1 sometimes, but it is ok.
	 */
#ifdef DUMP_RETURNS_1
        if(ret == 1) {
	    rc = 0;
	}
#endif
	dumppid = -1;
	strX = "dump";
    } else if(pid == tarpid) {
	if (ret == 1) {
	    rc = 0;
	}
	/*
	 * tar bitches about active filesystems, but we do not care.
	 */
#ifdef IGNORE_TAR_ERRORS
        if(ret == 2) {
	    rc = 0;
	}
#endif
	dumppid = tarpid = -1;
	strX = "dump";
    } else if(pid == application_api_pid) {
	strX = "Application";
    } else {
	strX = "unknown";
    }

    if(rc == 0) {
	return 0;				/* normal exit */
    }

    if(ret == 0) {
	thiserr = vstrallocf(_("%s (%d) %s got signal %d"), strX, (int)pid, str,
			     sig);
    } else {
	thiserr = vstrallocf(_("%s (%d) %s returned %d"), strX, (int)pid, str, ret);
    }

    fdprintf(mesgfd, "? %s\n", thiserr);

    if(errorstr) {
	errorstr =  newvstrallocf(errorstr, "%s, %s", errorstr, thiserr);
	amfree(thiserr);
    } else {
	errorstr = thiserr;
	thiserr = NULL;
    }
    return 1;
}


/*
 *Send header info to the message file.
 */
void
info_tapeheader(
    dle_t *dle)
{
    g_fprintf(stderr, "%s: info BACKUP=%s\n", get_pname(), program->backup_name);

    g_fprintf(stderr, "%s: info RECOVER_CMD=", get_pname());
    if (dle->compress == COMP_FAST || dle->compress == COMP_BEST)
	g_fprintf(stderr, "%s %s |", UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
		UNCOMPRESS_OPT
#else
		""
#endif
		);

    g_fprintf(stderr, "%s -xpGf - ...\n", program->restore_name);

    if (dle->compress == COMP_FAST || dle->compress == COMP_BEST)
	g_fprintf(stderr, "%s: info COMPRESS_SUFFIX=%s\n",
			get_pname(), COMPRESS_SUFFIX);

    g_fprintf(stderr, "%s: info end\n", get_pname());
}

void
application_api_info_tapeheader(
    int       mesgfd,
    char     *prog,
    dle_t *dle)
{
    char line[1024];

    g_snprintf(line, 1024, "%s: info BACKUP=APPLICATION\n", get_pname());
    if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	return;
    }

    g_snprintf(line, 1024, "%s: info APPLICATION=%s\n", get_pname(), prog);
    if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	return;
    }

    g_snprintf(line, 1024, "%s: info RECOVER_CMD=", get_pname());
    if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	return;
    }

    if (dle->compress == COMP_FAST || dle->compress == COMP_BEST) {
	g_snprintf(line, 1024, "%s %s |", UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
		 UNCOMPRESS_OPT
#else
		 ""
#endif
		 );
	if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	    dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	    return;
	}
    }
    g_snprintf(line, 1024, "%s/%s restore [./file-to-restore]+\n",
	       APPLICATION_DIR, prog);
    if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	return;
    }

    if (dle->compress) {
	g_snprintf(line, 1024, "%s: info COMPRESS_SUFFIX=%s\n",
		 get_pname(), COMPRESS_SUFFIX);
	if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	    dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	    return;
	}
    }

    g_snprintf(line, 1024, "%s: info end\n", get_pname());
    if (full_write(mesgfd, line, strlen(line)) != strlen(line)) {
	dbprintf(_("error writing to mesgfd socket: %s"), strerror(errno));
	return;
    }
}

pid_t
pipefork(
    void	(*func)(void),
    char *	fname,
    int *	stdinfd,
    int		stdoutfd,
    int		stderrfd)
{
    int inpipe[2];
    pid_t pid;

    dbprintf(_("Forking function %s in pipeline\n"), fname);

    if(pipe(inpipe) == -1) {
	error(_("error [open pipe to %s: %s]"), fname, strerror(errno));
	/*NOTREACHED*/
    }

    switch(pid = fork()) {
    case -1:
	error(_("error [fork %s: %s]"), fname, strerror(errno));
	/*NOTREACHED*/
    default:	/* parent process */
	aclose(inpipe[0]);	/* close input side of pipe */
	*stdinfd = inpipe[1];
	break;
    case 0:		/* child process */
	aclose(inpipe[1]);	/* close output side of pipe */

	if(dup2(inpipe[0], 0) == -1) {
	    error(_("error [fork %s: dup2(%d, in): %s]"),
		  fname, inpipe[0], strerror(errno));
	    /*NOTRACHED*/
	}
	if(dup2(stdoutfd, 1) == -1) {
	    error(_("error [fork %s: dup2(%d, out): %s]"),
		  fname, stdoutfd, strerror(errno));
	    /*NOTRACHED*/
	}
	if(dup2(stderrfd, 2) == -1) {
	    error(_("error [fork %s: dup2(%d, err): %s]"),
		  fname, stderrfd, strerror(errno));
	    /*NOTRACHED*/
	}

	func();
	exit(0);
	/*NOTREACHED*/
    }
    return pid;
}

int
check_result(
    int mesgfd)
{
    int goterror;
    pid_t wpid;
    amwait_t retstat;

    goterror = 0;


    while((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	if(check_status(wpid, retstat, mesgfd)) goterror = 1;
    }

    if (dumppid != -1) {
	sleep(5);
	while((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	    if(check_status(wpid, retstat, mesgfd)) goterror = 1;
	}
    }
    if (dumppid != -1) {
	dbprintf(_("Sending SIGHUP to dump process %d\n"),
		  (int)dumppid);
	if(dumppid != -1) {
	    if(kill(dumppid, SIGHUP) == -1) {
		dbprintf(_("Can't send SIGHUP to %d: %s\n"),
			  (int)dumppid,
			  strerror(errno));
	    }
	}
	sleep(5);
	while((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	    if(check_status(wpid, retstat, mesgfd)) goterror = 1;
	}
    }
    if (dumppid != -1) {
	dbprintf(_("Sending SIGKILL to dump process %d\n"),
		  (int)dumppid);
	if(dumppid != -1) {
	    if(kill(dumppid, SIGKILL) == -1) {
		dbprintf(_("Can't send SIGKILL to %d: %s\n"),
			  (int)dumppid,
			  strerror(errno));
	    }
	}
	sleep(5);
	while((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	    if(check_status(wpid, retstat, mesgfd)) goterror = 1;
	}
    }

    return goterror;
}

void
parse_backup_messages(
    dle_t      *dle,
    int		mesgin)
{
    int goterror;
    char *line;

    amfree(errorstr);

    for(; (line = areads(mesgin)) != NULL; free(line)) {
	process_dumpline(line);
    }

    if(errno) {
	error(_("error [read mesg pipe: %s]"), strerror(errno));
	/*NOTREACHED*/
    }

    goterror = check_result(mesgfd);

    if(errorstr) {
	error(_("error [%s]"), errorstr);
	/*NOTREACHED*/
    } else if(dump_size == -1) {
	error(_("error [no backup size line]"));
	/*NOTREACHED*/
    }

    program->end_backup(dle, goterror);

    fdprintf(mesgfd, _("%s: size %ld\n"), get_pname(), dump_size);
    fdprintf(mesgfd, _("%s: end\n"), get_pname());
}


static void
process_dumpline(
    char *	str)
{
    amregex_t *rp;
    char *type;
    char startchr;

    for(rp = program->re_table; rp->regex != NULL; rp++) {
	if(match(rp->regex, str)) {
	    break;
	}
    }
    if(rp->typ == DMP_SIZE) {
	dump_size = (long)((the_num(str, rp->field)* rp->scale+1023.0)/1024.0);
    }
    switch(rp->typ) {
    case DMP_NORMAL:
	type = "normal";
	startchr = '|';
	break;
    case DMP_STRANGE:
	type = "strange";
	startchr = '?';
	break;
    case DMP_SIZE:
	type = "size";
	startchr = '|';
	break;
    case DMP_ERROR:
	type = "error";
	startchr = '?';
	break;
    default:
	/*
	 * Should never get here.
	 */
	type = "unknown";
	startchr = '!';
	break;
    }
    dbprintf("%3d: %7s(%c): %s\n",
	      rp->srcline,
	      type,
	      startchr,
	      str);
    fdprintf(mesgfd, "%c %s\n", startchr, str);
}


/*
 * start_index.  Creates an index file from the output of dump/tar.
 * It arranges that input is the fd to be written by the dump process.
 * If createindex is not enabled, it does nothing.  If it is not, a
 * new process will be created that tees input both to a pipe whose
 * read fd is dup2'ed input and to a program that outputs an index
 * file to `index'.
 *
 * make sure that the chat from restore doesn't go to stderr cause
 * this goes back to amanda which doesn't expect to see it
 * (2>/dev/null should do it)
 *
 * Originally by Alan M. McIvor, 13 April 1996
 *
 * Adapted by Alexandre Oliva, 1 May 1997
 *
 * This program owes a lot to tee.c from GNU sh-utils and dumptee.c
 * from the DeeJay backup package.
 */

static void
save_fd(
    int *	fd,
    int		min)
{
  int origfd = *fd;

  while (*fd >= 0 && *fd < min) {
    int newfd = dup(*fd);
    if (newfd == -1)
      dbprintf(_("Unable to save file descriptor [%s]\n"), strerror(errno));
    *fd = newfd;
  }
  if (origfd != *fd)
    dbprintf(_("Dupped file descriptor %i to %i\n"), origfd, *fd);
}

void
start_index(
    int		createindex,
    int		input,
    int		mesg,
    int		index,
    char *	cmd)
{
  int pipefd[2];
  FILE *pipe_fp;
  int exitcode;

  if (!createindex)
    return;

  if (pipe(pipefd) != 0) {
    error(_("creating index pipe: %s"), strerror(errno));
    /*NOTREACHED*/
  }

  switch(indexpid = fork()) {
  case -1:
    error(_("forking index tee process: %s"), strerror(errno));
    /*NOTREACHED*/

  default:
    aclose(pipefd[0]);
    if (dup2(pipefd[1], input) == -1) {
      error(_("dup'ping index tee output: %s"), strerror(errno));
      /*NOTREACHED*/
    }
    aclose(pipefd[1]);
    return;

  case 0:
    break;
  }

  /* now in a child process */
  save_fd(&pipefd[0], 4);
  save_fd(&index, 4);
  save_fd(&mesg, 4);
  save_fd(&input, 4);
  dup2(pipefd[0], 0);
  dup2(index, 1);
  dup2(mesg, 2);
  dup2(input, 3);
  for(index = 4; index < (int)FD_SETSIZE; index++) {
    if (index != dbfd()) {
      close(index);
    }
  }

  if ((pipe_fp = popen(cmd, "w")) == NULL) {
    error(_("couldn't start index creator [%s]"), strerror(errno));
    /*NOTREACHED*/
  }

  dbprintf(_("Started index creator: \"%s\"\n"), cmd);
  while(1) {
    char buffer[BUFSIZ], *ptr;
    ssize_t bytes_read;
    size_t bytes_written;
    size_t just_written;

    do {
	bytes_read = read(0, buffer, SIZEOF(buffer));
    } while ((bytes_read < 0) && ((errno == EINTR) || (errno == EAGAIN)));

    if (bytes_read < 0) {
      error(_("index tee cannot read [%s]"), strerror(errno));
      /*NOTREACHED*/
    }

    if (bytes_read == 0)
      break; /* finished */

    /* write the stuff to the subprocess */
    ptr = buffer;
    bytes_written = 0;
    just_written = full_write(fileno(pipe_fp), ptr, (size_t)bytes_read);
    if (just_written < (size_t)bytes_read) {
	/* 
	 * just as we waited for write() to complete.
	 */
	if (errno != EPIPE) {
	    dbprintf(_("Index tee cannot write to index creator [%s]\n"),
			    strerror(errno));
	}
    } else {
	bytes_written += just_written;
	ptr += just_written;
    }

    /* write the stuff to stdout, ensuring none lost when interrupt
       occurs */
    ptr = buffer;
    bytes_written = 0;
    just_written = full_write(3, ptr, bytes_read);
    if (just_written < (size_t)bytes_read) {
	error(_("index tee cannot write [%s]"), strerror(errno));
	/*NOTREACHED*/
    } else {
	bytes_written += just_written;
	ptr += just_written;
    }
  }

  aclose(pipefd[1]);

  /* finished */
  /* check the exit code of the pipe and moan if not 0 */
  if ((exitcode = pclose(pipe_fp)) != 0) {
    char *exitstr = str_exit_status("Index pipe", exitcode);
    dbprintf("%s\n", exitstr);
    amfree(exitstr);
  } else {
    dbprintf(_("Index created successfully\n"));
  }
  pipe_fp = NULL;

  exit(exitcode);
}

extern backup_program_t dump_program, gnutar_program;

backup_program_t *programs[] = {
  &dump_program, &gnutar_program, NULL
};
