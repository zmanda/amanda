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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: reporter.c,v 1.132 2006/08/28 17:02:48 martinea Exp $
 *
 * nightly Amanda Report generator
 */
/*
 * report format
 *     tape label message
 *     error messages
 *     strange messages
 *     summary stats
 *     details for errors
 *     details for strange
 *     notes
 *     success summary
 */

#include "amanda.h"
#include "conffile.h"
#include "columnar.h"
#include "tapefile.h"
#include "diskfile.h"
#include "infofile.h"
#include "logfile.h"
#include "version.h"
#include "util.h"
#include "timestamp.h"
#include "holding.h"

/* don't have (or need) a skipped type except internally to reporter */
#define L_SKIPPED	L_MARKER


#define STATUS_STRANGE   2
#define STATUS_FAILED    4
#define STATUS_MISSING   8
#define STATUS_TAPE     16

typedef struct line_s {
    struct line_s *next, *last;
    char *str;
} line_t;

typedef struct timedata_s {
    logtype_t result;
    double origsize, outsize;
    char *datestamp;
    double sec, kps;
    int filenum;
    char *tapelabel;
    int totpart;
} timedata_t;

typedef struct repdata_s {
    disk_t *disk;
    char *datestamp;
    double est_nsize, est_csize;
    timedata_t taper;
    timedata_t dumper;
    timedata_t chunker;
    timedata_t planner;
    int level;
    struct repdata_s *next;
} repdata_t;

#define data(dp) ((repdata_t *)(dp)->up)

static struct cumulative_stats {
    int dumpdisks, tapedisks, tapechunks;
    double taper_time, dumper_time;
    double outsize, origsize, tapesize;
    double coutsize, corigsize;			/* compressed dump only */
} stats[3];

static int dumpdisks[DUMP_LEVELS], tapedisks[DUMP_LEVELS], tapechunks[DUMP_LEVELS];	/* by-level breakdown of disk count */

typedef struct taper_s {
    char *label;
    double taper_time;
    double coutsize, corigsize;
    int tapedisks, tapechunks;
    struct taper_s *next;
} taper_t;

static taper_t *stats_by_tape = NULL;
static taper_t *current_tape = NULL;

typedef struct X_summary_s {
    char *hostname;
    char *diskname;
    int  level;
    char *str;
    struct X_summary_s *next;
} X_summary_t;

static X_summary_t *first_strange=NULL, *last_strange=NULL;
static X_summary_t *first_failed=NULL, *last_failed=NULL;

static double total_time, startup_time, planner_time;

/* count files to tape */
static int tapefcount = 0;

static int exit_status = 0;
static char *run_datestamp;
static char *tape_labels = NULL;
static int last_run_tapes = 0;
static int degraded_mode = 0; /* defined in driverio too */
static int normal_run = 0;
static int amflush_run = 0;
static int got_finish = 0;
static int cmdlogfname = 0;
static char *ghostname = NULL;

static char *tapestart_error = NULL;

static FILE *logfile, *mailf;

static FILE *postscript;
static char *printer;

static disklist_t diskq;
static disklist_t sortq;

static line_t *errsum = NULL;
static line_t *errdet = NULL;
static line_t *strangedet = NULL;
static line_t *notes = NULL;

static char MaxWidthsRequested = 0;	/* determined via config data */

static char *displayunit;
static long int unitdivisor;

/* local functions */
int main(int argc, char **argv);

static char *	nicedate(const char * datestamp);
static char *	prefix(char *host, char *disk, int level);
static char *	prefixstrange(char *host, char *disk, int level,
			size_t len_host, size_t len_disk);
static char *	Rule(int From, int To);
static char *	sDivZero(double a, double b, int cn);
static char *	TextRule(int From, int To, char *s);
static int	ColWidth(int From, int To);
static int	contline_next(void);
static int	sort_by_name(disk_t *a, disk_t *b);
static repdata_t *find_repdata(disk_t *dp, char *datestamp, int level);
static repdata_t *handle_chunk(logtype_t logtype);
static repdata_t *handle_success(logtype_t logtype);
static void	addline(line_t **lp, char *str);
static void	addtoX_summary(X_summary_t **first, X_summary_t **last,
			       char *host, char *disk, int level, char *str);
static void	bogus_line(const char *);
static void	CalcMaxWidth(void);
static void	CheckFloatMax(ColumnInfo *cd, double d);
static void	CheckIntMax(ColumnInfo *cd, int n);
static void	CheckStringMax(ColumnInfo *cd, char *s);
static void	copy_template_file(char *lbl_templ);
static void	do_postscript_output(void);
static void	generate_missing(void);
static void	generate_bad_estimate(void);
static void	handle_disk(void);
static void	handle_error(void);
static void	handle_failed(void);
static void	handle_finish(void);
static void	handle_note(void);
static void	handle_partial(void);
static void	handle_start(void);
static void	handle_stats(void);
static void	handle_strange(void);
static void	handle_summary(void);
static void	output_lines(line_t *lp, FILE *f);
static void	output_stats(void);
static void	output_X_summary(X_summary_t *first);
static void	output_summary(void);
static void	output_tapeinfo(void);
static void	sort_disks(void);
static void	usage(void);

static int
ColWidth(
    int		From,
    int		To)
{
    int i, Width= 0;
    for (i=From; i<=To && ColumnData[i].Name != NULL; i++) {
    	Width+= ColumnData[i].PrefixSpace + ColumnData[i].Width;
    }
    return Width;
}

static char *
Rule(
    int		From,
    int		To)
{
    int i, ThisLeng;
    int Leng= ColWidth(0, ColumnDataCount());
    char *RuleSpace= alloc((size_t)(Leng+1));
    ThisLeng= ColWidth(From, To);
    for (i=0;i<ColumnData[From].PrefixSpace; i++)
    	RuleSpace[i]= ' ';
    for (; i<ThisLeng; i++)
    	RuleSpace[i]= '-';
    RuleSpace[ThisLeng]= '\0';
    return RuleSpace;
}

static char *
TextRule(
    int		From,
    int		To,
    char *	s)
{
    ColumnInfo *cd= &ColumnData[From];
    int leng;
    int nbrules, i, txtlength;
    int RuleSpaceSize= ColWidth(0, ColumnDataCount());
    char *RuleSpace= alloc((size_t)RuleSpaceSize), *tmp;

    leng = (int)strlen(s);
    if(leng >= (RuleSpaceSize - cd->PrefixSpace))
	leng = RuleSpaceSize - cd->PrefixSpace - 1;
    g_snprintf(RuleSpace, (size_t)RuleSpaceSize, "%*s%*.*s ", cd->PrefixSpace, "", 
	     leng, leng, s);
    txtlength = cd->PrefixSpace + leng + 1;
    nbrules = ColWidth(From,To) - txtlength;
    for(tmp=RuleSpace + txtlength, i=nbrules ; i>0; tmp++,i--)
	*tmp='-';
    *tmp = '\0';
    return RuleSpace;
}

static char *
sDivZero(
    double	a,
    double	b,
    int		cn)
{
    ColumnInfo *cd= &ColumnData[cn];
    static char PrtBuf[256];
    if (!isnormal(b))
    	g_snprintf(PrtBuf, SIZEOF(PrtBuf),
	  "%*s", cd->Width, "-- ");
    else
    	g_snprintf(PrtBuf, SIZEOF(PrtBuf),
	  cd->Format, cd->Width, cd->Precision, a/b);
    return PrtBuf;
}

static int
contline_next(void)
{
    int ch;

    if ((ch = getc(logfile)) != EOF) {
	    if (ungetc(ch, logfile) == EOF) {
		if (ferror(logfile)) {
		    error(_("ungetc failed: %s\n"), strerror(errno));
		    /*NOTREACHED*/
		}
		error(_("ungetc failed: EOF\n"));
		/*NOTREACHED*/
	    }
    }
    return ch == ' ';
}

static void
addline(
    line_t **	lp,
    char *	str)
{
    line_t *new, *p;

    /* allocate new line node */
    new = (line_t *) alloc(SIZEOF(line_t));
    new->next = NULL;
    new->last = NULL;
    new->str = stralloc(str);

    /* add to end of list */
    p = *lp;
    if (p == NULL) {
	*lp = new;
    } else {
	if (p->last) {
	    p->last->next = new;
	} else {
 	    p->next = new;
	}
 	p->last = new;
    }
}

static void
usage(void)
{
    error(_("Usage: amreport conf [-i] [-M address] [-f output-file] [-l logfile] [-p postscript-file] [-o configoption]*"));
    /*NOTREACHED*/
}

int
main(
    int		argc,
    char **	argv)
{
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_infofile;
    char *logfname, *psfname, *outfname, *subj_str = NULL;
    tapetype_t *tp;
    int opt;
    char *mail_cmd = NULL, *printer_cmd = NULL;
    extern int optind;
    char * cwd = NULL;
    char *ColumnSpec = "";
    char *errstr = NULL;
    int cn;
    int mailout = 1;
    char *mailto = NULL;
    char *lbl_templ = NULL;
    config_overwrites_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;
    char *mailer;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("amreport");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /* Process options */
    
    add_amanda_log_handler(amanda_log_stderr);
    outfname = NULL;
    psfname = NULL;
    logfname = NULL;
    cmdlogfname = 0;

    cwd = g_get_current_dir();
    if (cwd == NULL) {
	error(_("Cannot determine current working directory: %s"),
	      strerror(errno));
	/*NOTREACHED*/
    }

    if (argc >= 2) {
	if (argv[1][0] == '-') {
	    usage();
	    return 1;
	}

	/* get the config name and move past it */
	cfg_opt = argv[1];
	--argc; ++argv;

	cfg_ovr = new_config_overwrites(argc/2);
	while((opt = getopt(argc, argv, "o:M:f:l:p:i")) != EOF) {
	    switch(opt) {
	    case 'i': 
		mailout = 0;
		break;
            case 'M':
		if (mailto != NULL) {
		    error(_("you may specify at most one -M"));
		    /*NOTREACHED*/
		}
                mailto = stralloc(optarg);
		if(!validate_mailto(mailto)) {
		    error(_("mail address has invalid characters"));
		    /*NOTREACHED*/
		}
                break;
            case 'f':
		if (outfname != NULL) {
		    error(_("you may specify at most one -f"));
		    /*NOTREACHED*/
		}
		if (*optarg == '/') {
                    outfname = stralloc(optarg);
		} else {
                    outfname = vstralloc(cwd, "/", optarg, NULL);
		}
                break;
            case 'l':
		cmdlogfname = 1;
		if (logfname != NULL) {
		    error(_("you may specify at most one -l"));
		    /*NOTREACHED*/
		}
		if (*optarg == '/') {
		    logfname = stralloc(optarg);
		} else {
                    logfname = vstralloc(cwd, "/", optarg, NULL);
		}
                break;
            case 'p':
		if (psfname != NULL) {
		    error(_("you may specify at most one -p"));
		    /*NOTREACHED*/
		}
		if (*optarg == '/') {
                    psfname = stralloc(optarg);
		} else {
                    psfname = vstralloc(cwd, "/", optarg, NULL);
		}
                break;
	    case 'o':
		add_config_overwrite_opt(cfg_ovr, optarg);
		break;
            case '?':
		usage();
		return 1;
            default:
		break;
	    }
	}

	argc -= optind;
	argv += optind;
    }
    if( !mailout && mailto ){
	g_printf(_("You cannot specify both -i & -M at the same time\n"));
	exit(1);
    }

    amfree(cwd);

    /* read configuration files */

    /* ignore any errors reading the config file (amreport can run without a config) */
    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);
    apply_config_overwrites(cfg_ovr);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
    }

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    safe_cd(); /* must be called *after* config_init() */

    mailer = getconf_str(CNF_MAILER);
    if (mailer && *mailer == '\0')
	mailer = NULL;
    if (!mailer && !outfname) {
	g_printf(_("You must run amreport with '-f <output file>' because a mailer is not defined\n"));
	exit (1);
    }

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    /* Ignore error from read_diskfile */
    read_diskfile(conf_diskfile, &diskq);
    amfree(conf_diskfile);
    if(mailout && !mailto && 
       getconf_seen(CNF_MAILTO) && strlen(getconf_str(CNF_MAILTO)) > 0) {
		mailto = getconf_str(CNF_MAILTO);
                if(!validate_mailto(mailto)){
		   mailto = NULL;
                }
    }
    
    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    /* Ignore error from read_tapelist */
    read_tapelist(conf_tapelist);
    amfree(conf_tapelist);
    conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
    if(open_infofile(conf_infofile)) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);

    displayunit = getconf_str(CNF_DISPLAYUNIT);
    unitdivisor = getconf_unit_divisor();

    ColumnSpec = getconf_str(CNF_COLUMNSPEC);
    if(SetColumnDataFromString(ColumnData, ColumnSpec, &errstr) < 0) {
	curlog = L_ERROR;
	curprog = P_REPORTER;
	curstr = errstr;
	handle_error();
        amfree(errstr);
	curstr = NULL;
	ColumnSpec = "";		/* use the default */
	if(SetColumnDataFromString(ColumnData, ColumnSpec, &errstr) < 0) {
	    curlog = L_ERROR;
	    curprog = P_REPORTER;
	    curstr = errstr;
	    handle_error();
            amfree(errstr);
	    curstr = NULL;
	}
    }
    for (cn = 0; ColumnData[cn].Name != NULL; cn++) {
	if (ColumnData[cn].MaxWidth) {
	    MaxWidthsRequested = 1;
	    break;
	}
    }

    if(!logfname) {
	char *conf_logdir;

	conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
	logfname = vstralloc(conf_logdir, "/", "log", NULL);
	amfree(conf_logdir);
    }

    if((logfile = fopen(logfname, "r")) == NULL) {
	curlog = L_ERROR;
	curprog = P_REPORTER;
	curstr = vstralloc(_("could not open log "),
			   logfname,
			   ": ",
			   strerror(errno),
			   NULL);
	handle_error();
	amfree(curstr);
    }

    while(logfile && get_logline(logfile)) {
	switch(curlog) {
	case L_START:        handle_start(); break;
	case L_FINISH:       handle_finish(); break;

	case L_INFO:         handle_note(); break;
	case L_WARNING:      handle_note(); break;

	case L_SUMMARY:      handle_summary(); break;
	case L_STATS:        handle_stats(); break;

	case L_ERROR:        handle_error(); break;
	case L_FATAL:        handle_error(); break;

	case L_DISK:         handle_disk(); break;

	case L_DONE:         handle_success(curlog); break;
	case L_SUCCESS:      handle_success(curlog); break;
	case L_CHUNKSUCCESS: handle_success(curlog); break;
	case L_PART:         handle_chunk(curlog); break;
	case L_PARTPARTIAL:  handle_chunk(curlog); break;
	case L_CHUNK:        handle_chunk(curlog); break;
	case L_PARTIAL:      handle_partial(); break;
	case L_STRANGE:      handle_strange(); break;
	case L_FAIL:         handle_failed(); break;

	default:
	    curlog = L_ERROR;
	    curprog = P_REPORTER;
	    curstr = vstrallocf(_("unexpected log line: %s"), curstr);
	    handle_error();
	    amfree(curstr);
	}
    }
    afclose(logfile);
    close_infofile();
    if(!amflush_run) {
	generate_missing();
	generate_bad_estimate();
    }

    subj_str = vstralloc(getconf_str(CNF_ORG),
			 " ", amflush_run ? "AMFLUSH" : "AMANDA",
			 " ", "MAIL REPORT FOR",
			 " ", nicedate(run_datestamp ? run_datestamp : "0"),
			 NULL);
	
    /* lookup the tapetype and printer type from the amanda.conf file. */
    tp = lookup_tapetype(getconf_str(CNF_TAPETYPE));
    printer = getconf_str(CNF_PRINTER);

    /* ignore SIGPIPE so if a child process dies we do not also go away */
    signal(SIGPIPE, SIG_IGN);

    /* open pipe to print spooler if necessary) */

    if(psfname) {
	/* if the postscript_label_template (tp->lbl_templ) field is not */
	/* the empty string (i.e. it is set to something), open the      */
	/* postscript debugging file for writing.                        */
	if (tp)
	    lbl_templ = tapetype_get_lbl_templ(tp);
	if (tp && lbl_templ && strcmp(lbl_templ, "") != 0) {
	    if ((postscript = fopen(psfname, "w")) == NULL) {
		curlog = L_ERROR;
		curprog = P_REPORTER;
		curstr = vstrallocf(_("could not open %s: %s"),
				   psfname,
				   strerror(errno));
		handle_error();
		amfree(curstr);
	    }
	}
    } else {
#ifdef LPRCMD
	if (strcmp(printer, "") != 0)	/* alternate printer is defined */
	    /* print to the specified printer */
#ifdef LPRFLAG
	    printer_cmd = vstralloc(LPRCMD, " ", LPRFLAG, printer, NULL);
#else
	    printer_cmd = vstralloc(LPRCMD, NULL);
#endif
	else
	    /* print to the default printer */
	    printer_cmd = vstralloc(LPRCMD, NULL);
#endif
	if (tp)
	    lbl_templ = tapetype_get_lbl_templ(tp);
	if (tp && lbl_templ && strcmp(lbl_templ, "") != 0) {
#ifdef LPRCMD
	    if ((postscript = popen(printer_cmd, "w")) == NULL) {
		curlog = L_ERROR;
		curprog = P_REPORTER;
		curstr = vstrallocf(_("could not open pipe to %s: %s"),
				   printer_cmd, strerror(errno));
		handle_error();
		amfree(curstr);
	    }
#else
	    curlog = L_ERROR;
	    curprog = P_REPORTER;
	    curstr = vstrallocf(_("no printer command defined"));
	    handle_error();
	    amfree(curstr);
#endif
	}
    }

    sort_disks();

    /* open pipe to mailer */

    if(outfname) {
	/* output to a file */
	if((mailf = fopen(outfname,"w")) == NULL) {
	    error(_("could not open output file: %s %s"), outfname, strerror(errno));
	    /*NOTREACHED*/
	}
        if (mailto != NULL) {
		g_fprintf(mailf, "To: %s\n", mailto);
		g_fprintf(mailf, "Subject: %s\n\n", subj_str);
	}

    } else if (mailer) {
    	if(mailto) {
	    send_amreport_t send_amreport;
	    int             do_mail;

	    send_amreport = getconf_send_amreport(CNF_SEND_AMREPORT_ON);
	    do_mail = send_amreport == SEND_AMREPORT_ALL ||
		      (send_amreport == SEND_AMREPORT_STRANGE &&
		       (!got_finish || first_failed || errsum ||
		        first_strange || errdet || strangedet)) ||
		      (send_amreport == SEND_AMREPORT_ERROR &&
		       (!got_finish || first_failed || errsum || errdet));
	    if (do_mail) {
		mail_cmd = vstralloc(mailer,
			     " -s", " \"", subj_str, "\"",
			     " ", mailto, NULL);
		if((mailf = popen(mail_cmd, "w")) == NULL) {
		    error(_("could not open pipe to \"%s\": %s"),
			  mail_cmd, strerror(errno));
		    /*NOTREACHED*/
		}
	    }
	}
	else {
	    if (mailout) {
                g_printf(_("No mail sent! "));
		g_printf(_("No valid mail address has been specified in amanda.conf or on the commmand line\n"));
	    }
	    mailf = NULL;
	}
    }

    amfree(subj_str);

    if(mailf) {

    	if(!got_finish) fputs(_("*** THE DUMPS DID NOT FINISH PROPERLY!\n\n"), mailf);

	if (ghostname) {
	    g_fprintf(mailf, _("Hostname: %s\n"), ghostname);
	    g_fprintf(mailf, _("Org     : %s\n"), getconf_str(CNF_ORG));
	    g_fprintf(mailf, _("Config  : %s\n"), get_config_name());
	    g_fprintf(mailf, _("Date    : %s\n"),
		    nicedate(run_datestamp ? run_datestamp : "0"));
	    g_fprintf(mailf,"\n");
	}

    	output_tapeinfo();

    	if(first_failed || errsum) {
		g_fprintf(mailf,_("\nFAILURE DUMP SUMMARY:\n"));
		if(first_failed) output_X_summary(first_failed);
		if(errsum) output_lines(errsum, mailf);
    	}
    	if(first_strange) {
		g_fprintf(mailf,_("\nSTRANGE DUMP SUMMARY:\n"));
		if(first_strange) output_X_summary(first_strange);
    	}
    	fputs("\n\n", mailf);
	
    	output_stats();
	
    	if(errdet) {
		g_fprintf(mailf,"\n\f\n");
		g_fprintf(mailf,_("FAILED DUMP DETAILS:\n"));
		output_lines(errdet, mailf);
    	}
    	if(strangedet) {
		g_fprintf(mailf,"\n\f\n");
		g_fprintf(mailf,_("STRANGE DUMP DETAILS:\n"));
		output_lines(strangedet, mailf);
    	}
    	if(notes) {
		g_fprintf(mailf,"\n\f\n");
		g_fprintf(mailf,_("NOTES:\n"));
		output_lines(notes, mailf);
    	}
    	if(sortq.head != NULL) {
		g_fprintf(mailf,"\n\f\n");
		g_fprintf(mailf,_("DUMP SUMMARY:\n"));
		output_summary();
    	}
    	g_fprintf(mailf,_("\n(brought to you by Amanda version %s)\n"),
	    	version());
    }

    if (postscript) {
	do_postscript_output();
    }


    /* close postscript file */
    if (psfname && postscript) {
    	/* it may be that postscript is NOT opened */
	afclose(postscript);
    }
    else {
	if (postscript != NULL && pclose(postscript) != 0) {
	    error(_("printer command failed: %s"), printer_cmd);
	    /*NOTREACHED*/
	}
	postscript = NULL;
    }

    /* close output file */
    if(outfname) {
        afclose(mailf);
    }
    else if(mailf) {
	int exitcode;
        if((exitcode = pclose(mailf)) != 0) {
	    char *exitstr = str_exit_status("mail command", exitcode);
            error("%s", exitstr);
	    /*NOTREACHED*/
	}
        mailf = NULL;
    }

    clear_tapelist();
    free_disklist(&diskq);
    amfree(run_datestamp);
    amfree(tape_labels);
    amfree(printer_cmd);
    amfree(mail_cmd);
    amfree(logfname);

    dbclose();
    return exit_status;
}

/* ----- */

#define mb(f)	((f)/1024)		/* kbytes -> mbutes */
#define du(f)	((f)/unitdivisor)	/* kbytes -> displayunit */
#define pct(f)	((f)*100.0)		/* percent */
#define hrmn(f) ((int)(f)+30)/3600, (((int)(f)+30)%3600)/60
#define mnsc(f) ((int)(f+0.5))/60, ((int)(f+0.5)) % 60

#define divzero(fp,a,b)	        	    \
    do {       	       	       	       	    \
	double q = (b);			    \
	if (!isnormal(q))		    \
	    g_fprintf((fp),"  -- ");	    \
	else if ((q = (a)/q) >= 99999.95)   \
	    g_fprintf((fp), "#####");	    \
	else if (q >= 999.95)		    \
	    g_fprintf((fp), "%5.0lf",q);    \
	else				    \
	    g_fprintf((fp), "%5.1lf",q);    \
    } while(0)
#define divzero_wide(fp,a,b)	       	    \
    do {       	       	       	       	    \
	double q = (b);			    \
	if (!isnormal(q))		    \
	    g_fprintf((fp),"    -- ");	    \
	else if ((q = (a)/q) >= 9999999.95) \
	    g_fprintf((fp), "#######");	    \
	else if (q >= 99999.95)		    \
	    g_fprintf((fp), "%7.0lf",q);    \
	else				    \
	    g_fprintf((fp), "%7.1lf",q);    \
    } while(0)

static void
output_stats(void)
{
    double idle_time;
    tapetype_t *tp = lookup_tapetype(getconf_str(CNF_TAPETYPE));
    off_t tapesize;
    off_t marksize;
    int lv, first;

    if (tp) {
	tapesize = tapetype_get_length(tp);
	marksize = tapetype_get_filemark(tp);
    } else {
	tapesize = 100 * 1024 * 1024;
	marksize = 1   * 1024 * 1024;
    }

    stats[2].dumpdisks   = stats[0].dumpdisks   + stats[1].dumpdisks;
    stats[2].tapedisks   = stats[0].tapedisks   + stats[1].tapedisks;
    stats[2].tapechunks  = stats[0].tapechunks  + stats[1].tapechunks;
    stats[2].outsize     = stats[0].outsize     + stats[1].outsize;
    stats[2].origsize    = stats[0].origsize    + stats[1].origsize;
    stats[2].tapesize    = stats[0].tapesize    + stats[1].tapesize;
    stats[2].coutsize    = stats[0].coutsize    + stats[1].coutsize;
    stats[2].corigsize   = stats[0].corigsize   + stats[1].corigsize;
    stats[2].taper_time  = stats[0].taper_time  + stats[1].taper_time;
    stats[2].dumper_time = stats[0].dumper_time + stats[1].dumper_time;

    if(!got_finish)	/* no driver finish line, estimate total run time */
	total_time = stats[2].taper_time + planner_time;

    idle_time = (total_time - startup_time) - stats[2].taper_time;
    if(idle_time < 0) idle_time = 0.0;

    g_fprintf(mailf,_("STATISTICS:\n"));
    g_fprintf(mailf,
	    _("                          Total       Full      Incr.\n"));
    g_fprintf(mailf,
	    _("                        --------   --------   --------\n"));

    g_fprintf(mailf,
	    _("Estimate Time (hrs:min)   %2d:%02d\n"), hrmn(planner_time));

    g_fprintf(mailf,
	    _("Run Time (hrs:min)        %2d:%02d\n"), hrmn(total_time));

    g_fprintf(mailf,
	    _("Dump Time (hrs:min)       %2d:%02d      %2d:%02d      %2d:%02d\n"),
	    hrmn(stats[2].dumper_time), hrmn(stats[0].dumper_time),
	    hrmn(stats[1].dumper_time));

    g_fprintf(mailf,
	    _("Output Size (meg)      %8.1lf   %8.1lf   %8.1lf\n"),
	    mb(stats[2].outsize), mb(stats[0].outsize), mb(stats[1].outsize));

    g_fprintf(mailf,
	    _("Original Size (meg)    %8.1lf   %8.1lf   %8.1lf\n"),
	    mb(stats[2].origsize), mb(stats[0].origsize),
	    mb(stats[1].origsize));

    g_fprintf(mailf, _("Avg Compressed Size (%%)   "));
    divzero(mailf, pct(stats[2].coutsize),stats[2].corigsize);
    fputs(_("      "), mailf);
    divzero(mailf, pct(stats[0].coutsize),stats[0].corigsize);
    fputs(_("      "), mailf);
    divzero(mailf, pct(stats[1].coutsize),stats[1].corigsize);

    if(stats[1].dumpdisks > 0) fputs(_("   (level:#disks ...)"), mailf);
    putc('\n', mailf);

    g_fprintf(mailf,
	    _("Filesystems Dumped         %4d       %4d       %4d"),
	    stats[2].dumpdisks, stats[0].dumpdisks, stats[1].dumpdisks);

    if(stats[1].dumpdisks > 0) {
	first = 1;
	for(lv = 1; lv < DUMP_LEVELS; lv++) if(dumpdisks[lv]) {
	    fputs(first?_("   ("):_(" "), mailf);
	    first = 0;
	    g_fprintf(mailf, _("%d:%d"), lv, dumpdisks[lv]);
	}
	putc(')', mailf);
    }
    putc('\n', mailf);

    g_fprintf(mailf, _("Avg Dump Rate (k/s)     "));
    divzero_wide(mailf, stats[2].outsize,stats[2].dumper_time);
    fputs(_("    "), mailf);
    divzero_wide(mailf, stats[0].outsize,stats[0].dumper_time);
    fputs(_("    "), mailf);
    divzero_wide(mailf, stats[1].outsize,stats[1].dumper_time);
    putc('\n', mailf);

    putc('\n', mailf);
    g_fprintf(mailf,
	    _("Tape Time (hrs:min)       %2d:%02d      %2d:%02d      %2d:%02d\n"),
	    hrmn(stats[2].taper_time), hrmn(stats[0].taper_time),
	    hrmn(stats[1].taper_time));

    g_fprintf(mailf,
	    _("Tape Size (meg)        %8.1lf   %8.1lf   %8.1lf\n"),
	    mb(stats[2].tapesize), mb(stats[0].tapesize),
	    mb(stats[1].tapesize));

    g_fprintf(mailf, _("Tape Used (%%)             "));
    divzero(mailf, pct(stats[2].tapesize+marksize*(stats[2].tapedisks+stats[2].tapechunks)),(double)tapesize);
    fputs(_("      "), mailf);
    divzero(mailf, pct(stats[0].tapesize+marksize*(stats[0].tapedisks+stats[0].tapechunks)),(double)tapesize);
    fputs(_("      "), mailf);
    divzero(mailf, pct(stats[1].tapesize+marksize*(stats[1].tapedisks+stats[1].tapechunks)),(double)tapesize);

    if(stats[1].tapedisks > 0) fputs(_("   (level:#disks ...)"), mailf);
    putc('\n', mailf);

    g_fprintf(mailf,
	    _("Filesystems Taped          %4d       %4d       %4d"),
	    stats[2].tapedisks, stats[0].tapedisks, stats[1].tapedisks);

    if(stats[1].tapedisks > 0) {
	first = 1;
	for(lv = 1; lv < DUMP_LEVELS; lv++) if(tapedisks[lv]) {
	    fputs(first?_("   ("):_(" "), mailf);
	    first = 0;
	    g_fprintf(mailf, _("%d:%d"), lv, tapedisks[lv]);
	}
	putc(')', mailf);
    }
    putc('\n', mailf);

    if(stats[1].tapechunks > 0) fputs(_("   (level:#chunks ...)"), mailf);
    putc('\n', mailf);

    g_fprintf(mailf,
	    _("Chunks Taped               %4d       %4d       %4d"),
	    stats[2].tapechunks, stats[0].tapechunks, stats[1].tapechunks);

    if(stats[1].tapechunks > 0) {
	first = 1;
	for(lv = 1; lv < DUMP_LEVELS; lv++) if(tapechunks[lv]) {
	    fputs(first?_("   ("):_(" "), mailf);
	    first = 0;
	    g_fprintf(mailf, _("%d:%d"), lv, tapechunks[lv]);
	}
	putc(')', mailf);
    }
    putc('\n', mailf);

    g_fprintf(mailf, _("Avg Tp Write Rate (k/s) "));
    divzero_wide(mailf, stats[2].tapesize,stats[2].taper_time);
    fputs(_("    "), mailf);
    divzero_wide(mailf, stats[0].tapesize,stats[0].taper_time);
    fputs(_("    "), mailf);
    divzero_wide(mailf, stats[1].tapesize,stats[1].taper_time);
    putc('\n', mailf);

    if(stats_by_tape) {
	int label_length = (int)strlen(stats_by_tape->label) + 5;
	g_fprintf(mailf,_("\nUSAGE BY TAPE:\n"));
	g_fprintf(mailf,_("  %-*s  Time      Size      %%    Nb    Nc\n"),
		label_length, _("Label"));
	for(current_tape = stats_by_tape; current_tape != NULL;
	    current_tape = current_tape->next) {
	    g_fprintf(mailf, _("  %-*s"), label_length, current_tape->label);
	    g_fprintf(mailf, _(" %2d:%02d"), hrmn(current_tape->taper_time));
	    g_fprintf(mailf, _(" %8.0lf%s  "), du(current_tape->coutsize), displayunit);
	    divzero(mailf, pct(current_tape->coutsize + marksize *
		   (current_tape->tapedisks+current_tape->tapechunks)),
		   (double)tapesize);
	    g_fprintf(mailf, _("  %4d"), current_tape->tapedisks);
	    g_fprintf(mailf, _("  %4d\n"), current_tape->tapechunks);
	}
    }
}

/* ----- */

static void
output_tapeinfo(void)
{
    tape_t *tp;
    int run_tapes;
    int skip = 0;
    int i, nb_new_tape;

    if (last_run_tapes > 0) {
	if(amflush_run)
	    g_fprintf(mailf,
		    plural(_("The dumps were flushed to tape %s.\n"),
			   _("The dumps were flushed to tapes %s.\n"),
			   last_run_tapes),
		    tape_labels ? tape_labels : "");
	else
	    g_fprintf(mailf,
		    plural(_("These dumps were to tape %s.\n"),
			   _("These dumps were to tapes %s.\n"),
			   last_run_tapes),
		    tape_labels ? tape_labels : "");
    }

    if(degraded_mode) {
	g_fprintf(mailf,
		_("*** A TAPE ERROR OCCURRED: %s.\n"), tapestart_error);
    }
    if (cmdlogfname == 1) {
	if(degraded_mode) {
	    fputs(_("Some dumps may have been left in the holding disk.\n"),
		  mailf);
	    g_fprintf(mailf,"\n");
	}
    }  else {
	GSList *holding_list, *holding_file;
	off_t  h_size = 0, mh_size;

	holding_list = holding_get_files_for_flush(NULL);
	for(holding_file=holding_list; holding_file != NULL;
				       holding_file = holding_file->next) {
	    mh_size = holding_file_size((char *)holding_file->data, 1);
	    if (mh_size > 0)
		h_size += mh_size;
	}

	if (h_size > 0) {
	    g_fprintf(mailf,
		    _("There are %lld%s of dumps left in the holding disk.\n"),
		    (long long)du(h_size), displayunit);
	    if (getconf_boolean(CNF_AUTOFLUSH)) {
		g_fprintf(mailf, _("They will be flushed on the next run.\n"));
	    } else {
		g_fprintf(mailf, _("Run amflush to flush them to tape.\n"));
	    }
	    g_fprintf(mailf,"\n");
	} else if (degraded_mode) {
	    g_fprintf(mailf, _("No dumps are left in the holding disk. %lld%s\n"), (long long)h_size, displayunit);
	    g_fprintf(mailf,"\n");
	}
    }

    tp = lookup_last_reusable_tape(skip);

    run_tapes = getconf_int(CNF_RUNTAPES);

    if (run_tapes == 1)
	fputs(_("The next tape Amanda expects to use is: "), mailf);
    else if(run_tapes > 1)
	g_fprintf(mailf, _("The next %d tapes Amanda expects to use are: "),
		run_tapes);

    nb_new_tape = 0;
    for (i=0 ; i < run_tapes ; i++) {
	if(tp != NULL) {
	    if (nb_new_tape > 0) {
		if (nb_new_tape == 1)
		    g_fprintf(mailf, _("1 new tape, "));
		else
		    g_fprintf(mailf, _("%d new tapes, "), nb_new_tape);
		nb_new_tape = 0;
	    }
	    g_fprintf(mailf, "%s", tp->label);
	    if (i < run_tapes-1) fputs(", ", mailf);
	} else {
	    nb_new_tape++;
	}
	skip++;

	tp = lookup_last_reusable_tape(skip);
    }
    if (nb_new_tape > 0) {
	if (nb_new_tape == 1)
	    g_fprintf(mailf, _("1 new tape"));
	else
	    g_fprintf(mailf, _("%d new tapes"), nb_new_tape);
    }
    fputs(".\n", mailf);

    run_tapes = getconf_int(CNF_RUNTAPES);
    print_new_tapes(mailf, run_tapes);
}

/* ----- */
static void
output_X_summary(
    X_summary_t *first)
{
    size_t len_host=0, len_disk=0;
    X_summary_t *strange;
    char *str = NULL;

    for(strange=first; strange != NULL; strange = strange->next) {
	if(strlen(strange->hostname) > len_host)
	    len_host = strlen(strange->hostname);
	if(strlen(strange->diskname) > len_disk)
	    len_disk = strlen(strange->diskname);
    }
    for(strange=first; strange != NULL; strange = strange->next) {
	str = vstralloc("  ", prefixstrange(strange->hostname, strange->diskname, strange->level, len_host, len_disk),
			"  ", strange->str, NULL);
	g_fprintf(mailf, "%s\n", str);
	amfree(str);
    }
}

static void
output_lines(
    line_t *	lp,
    FILE *	f)
{
    line_t *next;

    while(lp) {
	fputs(lp->str, f);
	amfree(lp->str);
	fputc('\n', f);
	next = lp->next;
	amfree(lp);
	lp = next;
    }
}

/* ----- */

static int
sort_by_name(
    disk_t *	a,
    disk_t *	b)
{
    int rc;

    rc = strcmp(a->host->hostname, b->host->hostname);
    if(rc == 0) rc = strcmp(a->name, b->name);
    return rc;
}

static void
sort_disks(void)
{
    disk_t *dp;

    sortq.head = sortq.tail = NULL;
    while(!empty(diskq)) {
	dp = dequeue_disk(&diskq);
	if(data(dp) == NULL) { /* create one */
	    find_repdata(dp, run_datestamp, 0);
	}
	insert_disk(&sortq, dp, sort_by_name);
    }
}

static void
CheckStringMax(
    ColumnInfo *cd,
    char *	s)
{
    if (cd->MaxWidth) {
	int l = (int)strlen(s);

	if (cd->Width < l)
	    cd->Width= l;
    }
}

static void
CheckIntMax(
    ColumnInfo *cd,
    int		n)
{
    if (cd->MaxWidth) {
    	char testBuf[200];
    	int l;

	g_snprintf(testBuf, SIZEOF(testBuf),
	  cd->Format, cd->Width, cd->Precision, n);
	l = (int)strlen(testBuf);
	if (cd->Width < l)
	    cd->Width= l;
    }
}

static void
CheckFloatMax(
    ColumnInfo *cd,
    double	d)
{
    if (cd->MaxWidth) {
    	char testBuf[200];
	int l;

	g_snprintf(testBuf, SIZEOF(testBuf),
	  cd->Format, cd->Width, cd->Precision, d);
	l = (int)strlen(testBuf);
	if (cd->Width < l)
	    cd->Width= l;
    }
}

static int HostName;
static int Disk;
static int Level;
static int OrigKB;
static int OutKB;
static int Compress;
static int DumpTime;
static int DumpRate;
static int TapeTime;
static int TapeRate;

static void
CalcMaxWidth(void)
{
    /* we have to look for columspec's, that require the recalculation.
     * we do here the same loops over the sortq as is done in
     * output_summary. So, if anything is changed there, we have to
     * change this here also.
     *							ElB, 1999-02-24.
     */
    disk_t *dp;
    double f;
    repdata_t *repdata;
    char *qdevname;
    int i, l;

    for (i=0;ColumnData[i].Name != NULL; i++) {
	if (ColumnData[i].MaxWidth) {
	    l = (int)strlen(ColumnData[i].Title);
	    if (ColumnData[i].Width < l)
		ColumnData[i].Width= l;
	}
    }

    for(dp = sortq.head; dp != NULL; dp = dp->next) {
      if(dp->todo) {
	for(repdata = data(dp); repdata != NULL; repdata = repdata->next) {
	    char TimeRateBuffer[40];

	    CheckStringMax(&ColumnData[HostName], dp->host->hostname);
	    qdevname = quote_string(dp->name);
	    CheckStringMax(&ColumnData[Disk], qdevname);
	    amfree(qdevname);
	    if (repdata->dumper.result == L_BOGUS && 
		repdata->taper.result == L_BOGUS)
		continue;
	    CheckIntMax(&ColumnData[Level], repdata->level);
            if(repdata->dumper.result == L_SUCCESS || 
                   repdata->dumper.result == L_CHUNKSUCCESS) {
		CheckFloatMax(&ColumnData[OrigKB],
			      (double)du(repdata->dumper.origsize));
		CheckFloatMax(&ColumnData[OutKB],
			      (double)du(repdata->dumper.outsize));
		if(abs(repdata->dumper.outsize - repdata->dumper.origsize)< 32)
		    f = 0.0;
		else 
		    f = repdata->dumper.origsize;
		CheckStringMax(&ColumnData[Compress], 
			sDivZero(pct(repdata->dumper.outsize), f, Compress));

		if(!amflush_run)
		    g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
				"%3d:%02d", mnsc(repdata->dumper.sec));
		else
		    g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
				" ");
		CheckStringMax(&ColumnData[DumpTime], TimeRateBuffer);

		CheckFloatMax(&ColumnData[DumpRate], repdata->dumper.kps); 
	    }

	    if(repdata->taper.result == L_FAIL) {
		CheckStringMax(&ColumnData[TapeTime], "FAILED");
		continue;
	    }
	    if(repdata->taper.result == L_SUCCESS ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer), 
		  "%3d:%02d", mnsc(repdata->taper.sec));
	    else
		g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  " ");
	    CheckStringMax(&ColumnData[TapeTime], TimeRateBuffer);

	    if(repdata->taper.result == L_SUCCESS ||
		    repdata->taper.result == L_CHUNKSUCCESS)
		CheckFloatMax(&ColumnData[TapeRate], repdata->taper.kps);
	    else
		CheckStringMax(&ColumnData[TapeRate], " ");
	}
      }
    }
}

static void
output_summary(void)
{
    disk_t *dp;
    repdata_t *repdata;
    char *ds="DUMPER STATS";
    char *ts=" TAPER STATS";
    char *tmp;

    int i, h, w1, wDump, wTape;
    double outsize, origsize;
    double f;
    int cdWidth;

    HostName = StringToColumn("HostName");
    Disk = StringToColumn("Disk");
    Level = StringToColumn("Level");
    OrigKB = StringToColumn("OrigKB");
    OutKB = StringToColumn("OutKB");
    Compress = StringToColumn("Compress");
    DumpTime = StringToColumn("DumpTime");
    DumpRate = StringToColumn("DumpRate");
    TapeTime = StringToColumn("TapeTime");
    TapeRate = StringToColumn("TapeRate");

    /* at first determine if we have to recalculate our widths */
    if (MaxWidthsRequested)
	CalcMaxWidth();

    /* title for Dumper-Stats */
    w1= ColWidth(HostName, Level);
    wDump= ColWidth(OrigKB, DumpRate);
    wTape= ColWidth(TapeTime, TapeRate);

    /* print centered top titles */
    h = (int)strlen(ds);
    if (h > wDump) {
	h = 0;
    } else {
	h = (wDump-h)/2;
    }
    g_fprintf(mailf, "%*s", w1+h, "");
    g_fprintf(mailf, "%-*s", wDump-h, ds);
    h = (int)strlen(ts);
    if (h > wTape) {
	h = 0;
    } else {
	h = (wTape-h)/2;
    }
    g_fprintf(mailf, "%*s", h, "");
    g_fprintf(mailf, "%-*s", wTape-h, ts);
    fputc('\n', mailf);

    /* print the titles */
    for (i=0; ColumnData[i].Name != NULL; i++) {
    	char *fmt;
    	ColumnInfo *cd= &ColumnData[i];
    	g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	if (cd->Format[1] == '-')
	    fmt= "%-*s";
	else
	    fmt= "%*s";
	if(strcmp(cd->Title,"ORIG-KB") == 0) {
	    /* cd->Title must be re-allocated in write-memory */
	    cd->Title = stralloc("ORIG-KB");
	    cd->Title[5] = displayunit[0];
	}
	if(strcmp(cd->Title,"OUT-KB") == 0) {
	    /* cd->Title must be re-allocated in write-memory */
	    cd->Title = stralloc("OUT-KB");
	    cd->Title[4] = displayunit[0];
	}
	g_fprintf(mailf, fmt, cd->Width, cd->Title);
    }
    fputc('\n', mailf);

    /* print the rules */
    fputs(tmp=Rule(HostName, Level), mailf); amfree(tmp);
    fputs(tmp=Rule(OrigKB, DumpRate), mailf); amfree(tmp);
    fputs(tmp=Rule(TapeTime, TapeRate), mailf); amfree(tmp);
    fputc('\n', mailf);

    for(dp = sortq.head; dp != NULL; dp = dp->next) {
      if(dp->todo) {
    	ColumnInfo *cd;
	char TimeRateBuffer[40];
	for(repdata = data(dp); repdata != NULL; repdata = repdata->next) {
	    char *devname;
	    char *qdevname;
	    size_t devlen;

	    cd= &ColumnData[HostName];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    g_fprintf(mailf, cd->Format, cd->Width, cd->Width, dp->host->hostname);

	    cd= &ColumnData[Disk];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    devname = sanitize_string(dp->name);
	    qdevname = quote_string(devname);
	    devlen = strlen(qdevname);
	    if (devlen > (size_t)cd->Width) {
		int nb = 1;
		if (strcmp(devname, qdevname)) {
		    nb = 2;
		    fputc('"', mailf); 
		}
		fputc('-', mailf); 
		g_fprintf(mailf, cd->Format, cd->Width-nb, cd->Precision-nb,
			qdevname+devlen - (cd->Width-nb) );
	    }
	    else
		g_fprintf(mailf, cd->Format, cd->Width, cd->Width, qdevname);
	    amfree(devname);
	    amfree(qdevname);
	    cd= &ColumnData[Level];
	    if (repdata->dumper.result == L_BOGUS &&
		repdata->taper.result  == L_BOGUS) {
	      if(amflush_run){
		g_fprintf(mailf, "%*s%s\n", cd->PrefixSpace+cd->Width, "",
			tmp=TextRule(OrigKB, TapeRate, "NO FILE TO FLUSH"));
	      } else {
		g_fprintf(mailf, "%*s%s\n", cd->PrefixSpace+cd->Width, "",
			tmp=TextRule(OrigKB, TapeRate, "MISSING"));
	      }
	      amfree(tmp);
	      continue;
	    }
	    
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    g_fprintf(mailf, cd->Format, cd->Width, cd->Precision,repdata->level);

	    if (repdata->dumper.result == L_SKIPPED) {
		g_fprintf(mailf, "%s\n",
			tmp=TextRule(OrigKB, TapeRate, "SKIPPED"));
		amfree(tmp);
		continue;
	    }
	    if (repdata->dumper.result == L_FAIL && (repdata->chunker.result != L_PARTIAL && repdata->taper.result  != L_PARTIAL)) {
		g_fprintf(mailf, "%s\n",
			tmp=TextRule(OrigKB, TapeRate, "FAILED"));
		amfree(tmp);
		exit_status |= STATUS_FAILED;
		continue;
	    }

	    if(repdata->dumper.result == L_SUCCESS ||
	       repdata->dumper.result == L_CHUNKSUCCESS)
		origsize = repdata->dumper.origsize;
	    else if(repdata->taper.result == L_SUCCESS ||
		    repdata->taper.result == L_PARTIAL)
		origsize = repdata->taper.origsize;
	    else
		origsize = repdata->chunker.origsize;

	    if(repdata->taper.result == L_SUCCESS ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		outsize  = repdata->taper.outsize;
	    else if(repdata->chunker.result == L_SUCCESS ||
		    repdata->chunker.result == L_PARTIAL ||
		    repdata->chunker.result == L_CHUNKSUCCESS)
		outsize  = repdata->chunker.outsize;
	    else if (repdata->taper.result == L_PARTIAL)
		outsize  = repdata->taper.outsize;
	    else
		outsize  = repdata->dumper.outsize;

	    cd= &ColumnData[OrigKB];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(isnormal(origsize))
		g_fprintf(mailf, cd->Format, cd->Width, cd->Precision, du(origsize));
	    else
		g_fprintf(mailf, "%*.*s", cd->Width, cd->Width, "");

	    cd= &ColumnData[OutKB];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");

	    g_fprintf(mailf, cd->Format, cd->Width, cd->Precision, du(outsize));
	    	
	    cd= &ColumnData[Compress];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");

	    if(abs(outsize - origsize) < 32)
		f = 0.0;
	    else if(origsize < 1.0)
		f = 0.0;
	    else
		f = origsize;

	    fputs(sDivZero(pct(outsize), f, Compress), mailf);

	    cd= &ColumnData[DumpTime];
	    cdWidth = 0;
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->dumper.result == L_SUCCESS ||
	       repdata->dumper.result == L_CHUNKSUCCESS) {
		g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "%3d:%02d", mnsc(repdata->dumper.sec)); 
		g_fprintf(mailf, cd->Format, cd->Width, cd->Width,
			  TimeRateBuffer);
	    } else {
		cdWidth = cd->Width;
	    }

	    cd= &ColumnData[DumpRate];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if (repdata->dumper.result == L_SUCCESS ||
		repdata->dumper.result == L_CHUNKSUCCESS) {
		g_fprintf(mailf, cd->Format, cd->Width, cd->Precision,
			  repdata->dumper.kps);
	    } else if (repdata->dumper.result == L_FAIL) {
		if (repdata->chunker.result == L_PARTIAL ||
		    repdata->taper.result == L_PARTIAL) {
		    int i;
		    cdWidth += cd->Width;
		    i = (cdWidth - strlen("PARTIAL")) / 2;
		    g_fprintf(mailf, "%*s%*s", cdWidth-i, "PARTIAL", i, "");
		} else {
		    int i;
		    cdWidth += cd->Width;
		    i = (cdWidth - strlen("FAILED")) / 2;
		    g_fprintf(mailf, "%*s%*s", cdWidth-i, "FAILED", i, "");
		}
	    } else if (repdata->dumper.result == L_BOGUS) {
		int i;
		cdWidth += cd->Width;
		i = (cdWidth - strlen("FLUSH")) / 2;
		g_fprintf(mailf, "%*s%*s", cdWidth-i, "FLUSH", i, "");
	    } else {
		cdWidth += cd->Width;
		g_fprintf(mailf, "%*s", cdWidth, "");
	    }

	    cd= &ColumnData[TapeTime];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->taper.result == L_FAIL) {
		g_fprintf(mailf, "%s\n",
			tmp=TextRule(TapeTime, TapeRate, "FAILED "));
		amfree(tmp);
		continue;
	    }

	    if(repdata->taper.result == L_SUCCESS || 
	       repdata->taper.result == L_PARTIAL ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "%3d:%02d", mnsc(repdata->taper.sec));
	    else
		g_snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  " ");
	    g_fprintf(mailf, cd->Format, cd->Width, cd->Width, TimeRateBuffer);

	    cd= &ColumnData[TapeRate];
	    g_fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->taper.result == L_SUCCESS || 
	       repdata->taper.result == L_PARTIAL ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		g_fprintf(mailf, cd->Format, cd->Width, cd->Precision, repdata->taper.kps);
	    else
		g_fprintf(mailf, "%*s", cd->Width, " ");

	    if (repdata->chunker.result == L_PARTIAL)
		g_fprintf(mailf, " PARTIAL");
	    else if(repdata->taper.result == L_PARTIAL)
		g_fprintf(mailf, " TAPE-PARTIAL");

	    fputc('\n', mailf);
	}
      }
    }
}

static void
bogus_line(
    const char *err_text)
{
    char * s;
    s = g_strdup_printf(_("line %d of log is bogus: <%s %s %s>\n"),
                        curlinenum, 
                        logtype_str[curlog], program_str[curprog], curstr);
    g_printf("%s\n", s);
    g_printf(_("  Scan failed at: <%s>\n"), err_text);
    addline(&errsum, s);
    amfree(s);
}


/*
 * Formats an integer of the form YYYYMMDDHHMMSS into the string
 * "Monthname DD, YYYY".  A pointer to the statically allocated string
 * is returned, so it must be copied to other storage (or just printed)
 * before calling nicedate() again.
 */
static char *
nicedate(
    const char *datestamp)
{
    static char nice[64];
    char date[9];
    int  numdate;
    static char *months[13] = {
		T_("BogusMonth"),
		T_("January"),
		T_("February"),
		T_("March"),
		T_("April"),
		T_("May"),
		T_("June"),
		T_("July"),
		T_("August"),
		T_("September"),
		T_("October"),
		T_("November"),
		T_("December")
    };
    int year, month, day;

    strncpy(date, datestamp, 8);
    date[8] = '\0';
    numdate = atoi(date);
    year  = numdate / 10000;
    day   = numdate % 100;
    month = (numdate / 100) % 100;
    if (month > 12 )
	month = 0;

    g_snprintf(nice, SIZEOF(nice), "%s %d, %d", _(months[month]), day, year);

    return nice;
}

static void
handle_start(void)
{
    static int started = 0;
    char *label;
    char *s, *fp;
    int ch;

    switch(curprog) {
    case P_TAPER:
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "datestamp", s, ch) != 0) {
	    bogus_line(s - 1);
	    return;
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    bogus_line(s - 1);
	    return;
	}
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	run_datestamp = newstralloc(run_datestamp, fp);
	s[-1] = (char)ch;

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "label", s, ch) != 0) {
	    bogus_line(s - 1);
	    return;
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    bogus_line(s - 1);
	    return;
	}
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	label = stralloc(fp);
	s[-1] = (char)ch;

	if(tape_labels) {
	    fp = vstralloc(tape_labels, ", ", label, NULL);
	    amfree(tape_labels);
	    tape_labels = fp;
	} else {
	    tape_labels = stralloc(label);
	}

	last_run_tapes++;

	if(stats_by_tape == NULL) {
	    stats_by_tape = current_tape = (taper_t *)alloc(SIZEOF(taper_t));
	}
	else {
	    current_tape->next = (taper_t *)alloc(SIZEOF(taper_t));
	    current_tape = current_tape->next;
	}
	current_tape->label = label;
	current_tape->taper_time = 0.0;
	current_tape->coutsize = 0.0;
	current_tape->corigsize = 0.0;
	current_tape->tapedisks = 0;
	current_tape->tapechunks = 0;
	current_tape->next = NULL;
	tapefcount = 0;

	return;
    case P_PLANNER:
	normal_run = 1;
	break;
    case P_DRIVER:
	break;
    case P_AMFLUSH:
	amflush_run = 1;
	break;
    default:
	;
    }

    if(!started) {
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "date", s, ch) != 0) {
	    return;				/* ignore bogus line */
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    bogus_line(s - 1);
	    return;
	}
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	run_datestamp = newstralloc(run_datestamp, fp);
	s[-1] = (char)ch;

	started = 1;
    }
    if(amflush_run && normal_run) {
	amflush_run = 0;
	addline(&notes,
     _("  reporter: both amflush and planner output in log, ignoring amflush."));
    }
}


static void
handle_finish(void)
{
    char *s;
    int ch;
    double a_time;

    if(curprog == P_DRIVER || curprog == P_AMFLUSH || curprog == P_PLANNER) {
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "date", s, ch) != 0) {
	    bogus_line(s - 1);
	    return;
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    bogus_line(s - 1);
	    return;
	}
	skip_non_whitespace(s, ch);	/* ignore the date string */

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "time", s, ch) != 0) {
	    /* older planner doesn't write time */
	    if(curprog == P_PLANNER) return;
	    bogus_line(s - 1);
	    return;
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    bogus_line(s - 1);
	    return;
	}
	if(sscanf(s - 1, "%lf", &a_time) != 1) {
	    bogus_line(s - 1);
	    return;
	}
	if(curprog == P_PLANNER) {
	    planner_time = a_time;
	}
	else {
	    total_time = a_time;
	    got_finish = 1;
	}
    }
}

static void
handle_stats(void)
{
    char *s, *fp;
    int ch;
    char *hostname, *diskname, *datestamp, *qdiskname;
    int level = 0;
    double sec, kps, nbytes, cbytes;
    repdata_t *repdata;
    disk_t *dp;

    if(curprog == P_DRIVER) {
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch != '\0' && strncmp_const_skip(s - 1, "startup time", s, ch) == 0) {
	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		bogus_line(s - 1);
		return;
	    }
	    if(sscanf(s - 1, "%lf", &startup_time) != 1) {
		bogus_line(s - 1);
		return;
	    }
	    planner_time = startup_time;
	}
	else if(ch != '\0' && strncmp_const_skip(s - 1, "hostname", s, ch) == 0) {
	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		bogus_line(s - 1);
		return;
	    }
	    ghostname = stralloc(s-1);
	}
	else if(ch != '\0' && strncmp_const_skip(s - 1, "estimate", s, ch) == 0) {
	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		bogus_line(s - 1);
		return;
	    }
	    fp = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    hostname = stralloc(fp);
	    s[-1] = (char)ch;

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		bogus_line(s - 1);
		amfree(hostname);
		return;
	    }

	    qdiskname = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    diskname = unquote_string(qdiskname);
	    s[-1] = (char)ch;

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		bogus_line(s - 1);
		amfree(hostname);
		amfree(diskname);
		return;
	    }
	    fp = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    datestamp = stralloc(fp);
	    s[-1] = (char)ch;
	    skip_whitespace(s, ch);

	    if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
		bogus_line(s - 1);
		amfree(hostname);
		amfree(diskname);
		amfree(datestamp);
		return;
	    }
	    skip_integer(s, ch);
	    if(level < 0 || level >= DUMP_LEVELS) {
		amfree(hostname);
		amfree(diskname);
		amfree(datestamp);
		return;
	    }

	    skip_whitespace(s, ch);

	    if(sscanf(s - 1,"[sec %lf nkb %lf ckb %lf kps %lf",
		      &sec, &nbytes, &cbytes, &kps) != 4)  {
		bogus_line(s - 1);
		amfree(hostname);
		amfree(diskname);
		amfree(datestamp);
		return;
	    }

	    dp = lookup_disk(hostname, diskname);
	    if(dp == NULL) {
		addtoX_summary(&first_failed, &last_failed,
			       hostname, diskname, level,
			       _("ERROR [not in disklist]"));
		exit_status |= STATUS_FAILED;
		amfree(hostname);
		amfree(diskname);
		amfree(datestamp);
		return;
	    }

	    repdata = find_repdata(dp, datestamp, level);

	    repdata->est_nsize = nbytes;
	    repdata->est_csize = cbytes;

	    amfree(hostname);
	    amfree(diskname);
	    amfree(datestamp);
	}
	else {
	    bogus_line(s - 1);
	    return;
	}
#undef sc

    }
}


static void
handle_note(void)
{
    char *str = NULL;
    char *pidstr;

    if (curprog == P_DRIVER &&
	BSTRNCMP(curstr, "Taper protocol error") == 0) {
	exit_status |= STATUS_TAPE;
    }
    pidstr = strchr(curstr,' ');
    if (pidstr) {
	pidstr++;
    }
    /* Don't report the pid lines */
    if ((!pidstr || BSTRNCMP(pidstr, "pid ") != 0) &&
	BSTRNCMP(curstr, "pid-done ") != 0) {
	str = vstrallocf("  %s: %s", program_str[curprog], curstr);
	addline(&notes, str);
	amfree(str);
    }
}


/* ----- */

static void
handle_error(void)
{
    char *s = NULL, *nl;
    int ch;

    if(curlog == L_ERROR && curprog == P_TAPER) {
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch != '\0' && strncmp_const_skip(s - 1, "no-tape", s, ch) == 0) {
	    skip_whitespace(s, ch);
	    if(ch != '\0') {
		if((nl = strchr(s - 1, '\n')) != NULL) {
		    *nl = '\0';
		}
		tapestart_error = newstralloc(tapestart_error, s - 1);
		if(nl) *nl = '\n';
		degraded_mode = 1;
		exit_status |= STATUS_TAPE;;
		return;
	    }
	    /* else some other tape error, handle like other errors */
	}
	/* else some other tape error, handle like other errors */
    }
    s = vstrallocf("  %s: %s %s", program_str[curprog],
		  logtype_str[curlog], curstr);
    addline(&errsum, s);
    amfree(s);
}

/* ----- */

static void
handle_summary(void)
{
    bogus_line(curstr);
}

/* ----- */

static int nb_disk=0;
static void
handle_disk(void)
{
    disk_t *dp;
    char *s, *fp, *qdiskname;
    int ch;
    char *hostname = NULL, *diskname = NULL;

    if(curprog != P_PLANNER && curprog != P_AMFLUSH) {
	bogus_line(curstr);
	return;
    }

    if(nb_disk==0) {
	for(dp = diskq.head; dp != NULL; dp = dp->next)
	    dp->todo = 0;
    }
    nb_disk++;

    s = curstr;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	return;
    }
    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    hostname = newstralloc(hostname, fp);
    s[-1] = (char)ch;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	amfree(hostname);
	return;
    }
    qdiskname = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(qdiskname);
    s[-1] = (char)ch;

    dp = lookup_disk(hostname, diskname);
    if(dp == NULL) {
	dp = add_disk(&diskq, hostname, diskname);
    }

    amfree(hostname);
    amfree(diskname);
    dp->todo = 1;
}

/* XXX Just a placeholder, in case we decide to do something with L_CHUNK
 * log entries.  Right now they're just the equivalent of L_SUCCESS, but only
 * for a split chunk of the overall dumpfile.
 */
static repdata_t *
handle_chunk(
    logtype_t logtype)
{
    disk_t *dp;
    double sec, kps, kbytes;
    timedata_t *sp;
    int i;
    char *s, *fp;
    int ch;
    char *hostname = NULL;
    char *diskname = NULL;
    repdata_t *repdata;
    int level, chunk;
    char *datestamp;
    char *label = NULL;
    int fileno;
    int totpart;
    
    if(curprog != P_TAPER) {
 	bogus_line(curstr);
 	return NULL;
    }
    
    s = curstr;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
 	bogus_line(s - 1);
 	return NULL;
    }

    if (logtype == L_PART || logtype == L_PARTPARTIAL) {
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	label = stralloc(fp);
	s[-1] = (char)ch;
    
	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &fileno) != 1) {
	    bogus_line(s - 1);
	    amfree(label);
	    return NULL;
	}
	/* set tapefcount, it is increased below */
	tapefcount = fileno - 1;
	skip_integer(s, ch);
	skip_whitespace(s, ch);
	if(ch == '\0') {
 	    bogus_line(s - 1);
	    amfree(label);
 	    return NULL;
	}
	amfree(label);
    }

    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    hostname = stralloc(fp);
    s[-1] = (char)ch;
    
    skip_whitespace(s, ch);
    if(ch == '\0') {
 	bogus_line(s - 1);
 	amfree(hostname);
 	return NULL;
    }
    fp = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(fp);
    s[-1] = (char)ch;
    
    skip_whitespace(s, ch);
    if(ch == '\0') {
 	bogus_line(s - 1);
 	amfree(hostname);
 	amfree(diskname);
 	return NULL;
    }
    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    datestamp = stralloc(fp);
    s[-1] = (char)ch;
 
    skip_whitespace(s, ch);
    if(ch == '\0' || sscanf(s - 1, "%d", &chunk) != 1) {
	bogus_line(s - 1);
	amfree(hostname);
	amfree(diskname);
	amfree(datestamp);
	return NULL;
    }
    skip_integer(s, ch);

    if (ch != '\0' && s[-1] == '/') {
	s++; ch = s[-1];
	if (sscanf(s - 1, "%d", &totpart) != 1) {
	    bogus_line(s - 1);
	    amfree(hostname);
	    amfree(diskname);
	    amfree(datestamp);
	    return NULL;
	}
	skip_integer(s, ch);
    }

    skip_whitespace(s, ch);
    if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	bogus_line(s - 1);
	amfree(hostname);
	amfree(diskname);
	amfree(datestamp);
	return NULL;
    }
    skip_integer(s, ch);
    
    /*@ignore@*/
    if(level < 0 || level >= DUMP_LEVELS) {
 	amfree(hostname);
 	amfree(diskname);
 	amfree(datestamp);
 	return NULL;
    }
    /*@end@*/
 
    skip_whitespace(s, ch);
    if(sscanf(s - 1,"[sec %lf kb %lf kps %lf", &sec, &kbytes, &kps) != 3)  {
 	bogus_line(s - 1);
 	amfree(hostname);
 	amfree(diskname);
 	amfree(datestamp);
 	return NULL;
    }
    
    
    dp = lookup_disk(hostname, diskname);
    if(dp == NULL) {
 	char *str = NULL;
	
 	str = vstrallocf(_("  %s ERROR [not in disklist]"),
			prefix(hostname, diskname, level));
 	addline(&errsum, str);
 	amfree(str);
 	amfree(hostname);
 	amfree(diskname);
 	amfree(datestamp);
 	return NULL;
    }
    
    repdata = find_repdata(dp, datestamp, level);
    
    sp = &(repdata->taper);
    
    i = level > 0;
    
    amfree(hostname);
    amfree(diskname);
    amfree(datestamp);
    
    if(current_tape == NULL) {
 	error("current_tape == NULL");
    }
    ++tapefcount;
    if (sp->filenum == 0) {
 	sp->filenum = tapefcount;
 	sp->tapelabel = current_tape->label;
    }
    tapechunks[level] +=1;
    stats[i].tapechunks +=1;
    current_tape->taper_time += sec;
    current_tape->coutsize += kbytes;
    current_tape->tapechunks += 1;
    return repdata;
}

static repdata_t *
handle_success(
    logtype_t	logtype)
{
    disk_t *dp;
    double sec = 0.0;
    double kps = 0.0;
    double kbytes = 0.0;
    double origkb = 0.0;
    timedata_t *sp;
    int i;
    char *s, *fp, *qdiskname;
    int ch;
    char *hostname = NULL;
    char *diskname = NULL;
    repdata_t *repdata;
    int level = 0;
    int totpart = 0;
    char *datestamp;

    (void)logtype;

    if(curprog != P_TAPER && curprog != P_DUMPER && curprog != P_PLANNER &&
       curprog != P_CHUNKER) {
	bogus_line(curstr);
	return NULL;
    }

    s = curstr;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	return NULL;
    }
    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    hostname = stralloc(fp);
    s[-1] = (char)ch;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	amfree(hostname);
	return NULL;
    }
    qdiskname = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(qdiskname);

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	amfree(hostname);
	amfree(diskname);
	return NULL;
    }
    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    datestamp = stralloc(fp);
    s[-1] = (char)ch;

    //datestamp is optional
    if(strlen(datestamp) < 6) {
	totpart = atoi(datestamp);
	datestamp = newstralloc(datestamp, run_datestamp);
    }
    else {
	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &totpart) != 1) {
	    bogus_line(s - 1);
	    amfree(hostname);
	    amfree(diskname);
	    amfree(datestamp);
	    return NULL;
	}
	skip_integer(s, ch);
    }

    skip_whitespace(s, ch);

    //totpart is optional
    if (*(s-1) == '"')
	s++;
    if (*(s-1) == '[') {
	level = totpart;
	totpart = -1;
    } else {
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    bogus_line(s - 1);
	    amfree(hostname);
	    amfree(diskname);
	    amfree(datestamp);
	    return NULL;
	}
	skip_integer(s, ch);
	skip_whitespace(s, ch);
    }


    if(level < 0 || level >= DUMP_LEVELS) {
	amfree(hostname);
	amfree(diskname);
	amfree(datestamp);
	return NULL;
    }

				/* Planner success messages (for skipped
				   dumps) do not contain statistics */
    if(curprog != P_PLANNER) {
	if(*(s - 1) == '"')
	    s++;
	if((curprog != P_DUMPER)
	    || (sscanf(s - 1,"[sec %lf kb %lf kps %lf orig-kb %lf", 
		  &sec, &kbytes, &kps, &origkb) != 4))  {
	    origkb = -1;
	    if(sscanf(s - 1,"[sec %lf kb %lf kps %lf",
		      &sec, &kbytes, &kps) != 3) {
		bogus_line(s - 1);
	        amfree(hostname);
	        amfree(diskname);
	        amfree(datestamp);
		return NULL;
	    }
	}
	else {
	    if(!isnormal(origkb))
		origkb = 0.1;
	}
	if (curprog == P_TAPER && logtype == L_PARTIAL) {
	    char *t = strchr(s-1,']');
	    if (t) {
		char *errmsg, *u;
		errmsg = unquote_string(t+1);
		u = vstrallocf("  %s: partial %s: %s",
			       prefix(hostname, diskname, level),
			       program_str[curprog], errmsg);
		addline(&errsum, u);
	    }
	}
    }


    dp = lookup_disk(hostname, diskname);
    if(dp == NULL) {
	addtoX_summary(&first_failed, &last_failed, hostname, qdiskname, level,
		       _("ERROR [not in disklist]"));
	exit_status |= STATUS_FAILED;
	amfree(hostname);
	amfree(diskname);
	amfree(datestamp);
	return NULL;
    }

    repdata = find_repdata(dp, datestamp, level);

    if(curprog == P_PLANNER) {
	repdata->dumper.result = L_SKIPPED;
	amfree(hostname);
	amfree(diskname);
	amfree(datestamp);
	return repdata;
    }

    if(curprog == P_TAPER)
	sp = &(repdata->taper);
    else if(curprog == P_DUMPER)
	sp = &(repdata->dumper);
    else sp = &(repdata->chunker);

    i = level > 0;

    if (origkb < 0.0 && (curprog == P_CHUNKER || curprog == P_TAPER) &&
	isnormal(repdata->dumper.outsize)) {
	/* take origkb from DUMPER line */
	origkb = repdata->dumper.outsize;
    } else if (origkb < 0.0) {
	/* take origkb from infofile, needed for amflush */
	info_t inf;
	struct tm *tm;
	int Idatestamp;

	get_info(hostname, diskname, &inf);
        tm = localtime(&inf.inf[level].date);
	if (tm) {
            Idatestamp = 10000*(tm->tm_year+1900) +
			 100*(tm->tm_mon+1) + tm->tm_mday;
	} else {
	    Idatestamp = 19000101;
	}

	if(atoi(datestamp) == Idatestamp) {
	    /* grab original size from record */
	    origkb = (double)inf.inf[level].size;
	}
	else
	    origkb = 0.0;
    }

    if (curprog == P_DUMPER &&
	(sp->result == L_FAIL || sp->result == L_PARTIAL)) {
	addtoX_summary(&first_failed, &last_failed, hostname, qdiskname, level,
		       _("was successfully retried"));
    }

    amfree(hostname);
    amfree(diskname);
    amfree(datestamp);

    sp->result = L_SUCCESS;
    sp->datestamp = repdata->datestamp;
    sp->sec = sec;
    sp->kps = kps;
    sp->origsize = origkb;
    sp->outsize = kbytes;

    if(curprog == P_TAPER) {
	if(current_tape == NULL) {
	    error(_("current_tape == NULL"));
	    /*NOTREACHED*/
	}
	stats[i].taper_time += sec;
	if (sp->filenum == 0) {
	    sp->filenum = ++tapefcount;
	    sp->tapelabel = current_tape->label;
	}
	sp->totpart = totpart;
	tapedisks[level] +=1;
	stats[i].tapedisks +=1;
	stats[i].tapesize += kbytes;
	sp->outsize = kbytes;
	if(!isnormal(repdata->chunker.outsize) && isnormal(repdata->dumper.outsize)) { /* dump to tape */
	    stats[i].outsize += kbytes;
	    if (abs(kbytes - origkb) >= 32) {
		/* server compressed */
		stats[i].corigsize += origkb;
		stats[i].coutsize += kbytes;
	    }
	}
	current_tape->tapedisks += 1;
    }

    if(curprog == P_DUMPER) {
	stats[i].dumper_time += sec;
	if (abs(kbytes - origkb) < 32) {
	    /* not client compressed */
	    sp->origsize = kbytes;
	}
	else {
	    /* client compressed */
	    stats[i].corigsize += sp->origsize;
	    stats[i].coutsize += kbytes;
	}
	dumpdisks[level] +=1;
	stats[i].dumpdisks +=1;
	stats[i].origsize += sp->origsize;
    }

    if(curprog == P_CHUNKER) {
	sp->outsize = kbytes;
	stats[i].outsize += kbytes;
	if (abs(kbytes - origkb) >= 32) {
	    /* server compressed */
	    stats[i].corigsize += origkb;
	    stats[i].coutsize += kbytes;
	}
    }
    return repdata;
}

static void
handle_partial(void)
{
    repdata_t *repdata;
    timedata_t *sp;

    repdata = handle_success(L_PARTIAL);
    if (!repdata)
	return;

    if(curprog == P_TAPER)
	sp = &(repdata->taper);
    else if(curprog == P_DUMPER)
	sp = &(repdata->dumper);
    else sp = &(repdata->chunker);

    sp->result = L_PARTIAL;
}

static void
handle_strange(void)
{
    char *str = NULL;
    char *strangestr = NULL;
    repdata_t *repdata;
    char *qdisk;
    int nb_stranges = 0;

    repdata = handle_success(L_SUCCESS);
    if (!repdata)
	return;

    qdisk = quote_string(repdata->disk->name);

    addline(&strangedet,"");
    str = vstrallocf("/-- %s STRANGE",
		prefix(repdata->disk->host->hostname, qdisk, repdata->level));
    addline(&strangedet, str);
    amfree(str);

    while(contline_next()) {
	char *s, ch;
	get_logline(logfile);
	s = curstr;
	if(strncmp_const_skip(curstr, "sendbackup: warning ", s, ch) == 0) {
	    strangestr = newstralloc(strangestr, s);
	}
	if (nb_stranges++ < 100) {
	    addline(&strangedet, curstr);
	}
    }
    if (nb_stranges > 100) {
	char *msg = g_strdup_printf("%d lines follow, see the corresponding log.* file for the complete list", nb_stranges - 100);
	addline(&strangedet, "\\--------");
	addline(&strangedet, msg);
	amfree(msg);
    }
    addline(&strangedet,"\\--------");

    str = vstrallocf("STRANGE %s", strangestr? strangestr : _("(see below)"));
    addtoX_summary(&first_strange, &last_strange,
		   repdata->disk->host->hostname, qdisk, repdata->level, str);
    exit_status |= STATUS_STRANGE;
    amfree(qdisk);
    amfree(str);
    amfree(strangestr);
}

static void
handle_failed(void)
{
    disk_t *dp;
    char *hostname;
    char *diskname;
    char *datestamp;
    char *errstr;
    int level = 0;
    char *s, *fp, *qdiskname;
    int ch;
    char *str = NULL;
    repdata_t *repdata;
    timedata_t *sp;

    hostname = NULL;
    diskname = NULL;

    s = curstr;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	return;
    }
    hostname = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	return;
    }
    qdiskname = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(qdiskname);

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	amfree(diskname);
	return;
    }
    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    datestamp = stralloc(fp);

    if(strlen(datestamp) < 3) { /* there is no datestamp, it's the level */
	level = atoi(datestamp);
	datestamp = newstralloc(datestamp, run_datestamp);
    }
    else { /* read the level */
	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    bogus_line(s - 1);
	    amfree(datestamp);
	    amfree(diskname);
	    return;
	}
	skip_integer(s, ch);
    }

    skip_whitespace(s, ch);
    if(ch == '\0') {
	bogus_line(s - 1);
	amfree(datestamp);
	amfree(diskname);
	return;
    }
    errstr = s - 1;
    if((s = strchr(errstr, '\n')) != NULL) {
	*s = '\0';
    }

    dp = lookup_disk(hostname, diskname);
    amfree(diskname);
    if(dp == NULL) {
	addtoX_summary(&first_failed, &last_failed, hostname, qdiskname, level,
		       _("ERROR [not in disklist]"));
    } else {
	repdata = find_repdata(dp, datestamp, level);

	if(curprog == P_TAPER)
	    sp = &(repdata->taper);
	else if (curprog == P_PLANNER)
	    sp = &(repdata->planner);
	else sp = &(repdata->dumper);

	if(sp->result != L_SUCCESS)
	    sp->result = L_FAIL;
    }
    amfree(datestamp);

    if (!((curprog == P_CHUNKER &&
	   strcmp(errstr, "[dumper returned FAILED]") == 0) ||
	  (curprog == P_CHUNKER &&
	   strcmp(errstr, "[Not enough holding disk space]") == 0) ||
	  (curprog == P_CHUNKER &&
	   strcmp(errstr, "[cannot read header: got 0 bytes instead of 32768]") == 0))) {
	str = vstrallocf(_("FAILED %s"), errstr);
	addtoX_summary(&first_failed, &last_failed, hostname, qdiskname, level,
		       str);
	amfree(str);
    }

    if(curprog == P_DUMPER) {
	int nb_failed = 0;
	addline(&errdet,"");
	str = vstrallocf("/-- %s FAILED %s",
			prefix(hostname, qdiskname, level), 
			errstr);
	addline(&errdet, str);
	amfree(str);
	while(contline_next()) {
	    get_logline(logfile);
	    if (nb_failed++ < 100) {
		addline(&errdet, curstr);
	    }
	}
	if (nb_failed > 100) {
	    char *msg = g_strdup_printf("%d lines follow, see the corresponding log.* file for the complete list", nb_failed - 100);
	    addline(&errdet, "\\--------");
	    addline(&errdet, msg);
	    amfree(msg);
	}
	addline(&errdet,"\\--------");
	exit_status |= STATUS_FAILED;
    }
    return;
}


static void
generate_missing(void)
{
    disk_t *dp;
    char *qdisk;

    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	if(dp->todo && data(dp) == NULL) {
	    qdisk = quote_string(dp->name);
	    addtoX_summary(&first_failed, &last_failed, dp->host->hostname,
			   qdisk, -987, _("RESULTS MISSING"));
	    exit_status |= STATUS_MISSING;
	    amfree(qdisk);
	}
    }
}

static void
generate_bad_estimate(void)
{
    disk_t *dp;
    repdata_t *repdata;
    char s[1000];
    double outsize;

    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	if(dp->todo) {
	    for(repdata = data(dp); repdata != NULL; repdata = repdata->next) {
		if(repdata->est_csize >= 0.1) {
		    if(repdata->taper.result == L_SUCCESS ||
		       repdata->taper.result == L_CHUNKSUCCESS)
			outsize  = repdata->taper.outsize;
		    else if(repdata->chunker.result == L_SUCCESS ||
			    repdata->chunker.result == L_PARTIAL ||
			    repdata->chunker.result == L_CHUNKSUCCESS)
			outsize  = repdata->chunker.outsize;
		    else if(repdata->taper.result == L_PARTIAL)
			outsize  = repdata->taper.outsize;
		    else
			outsize  = repdata->dumper.outsize;

		    if( (repdata->est_csize * 0.9 > outsize) && ( repdata->est_csize - outsize > 1.0e5 ) ) {
			g_snprintf(s, 1000,
				_("  big estimate: %s %s %d"),
				 repdata->disk->host->hostname,
				 repdata->disk->name,
				 repdata->level);
			s[999] = '\0';
			addline(&notes, s);
			g_snprintf(s, 1000,
				 _("                est: %.0lf%s    out %.0lf%s"),
				 du(repdata->est_csize), displayunit,
				 du(outsize), displayunit);
			s[999] = '\0';
			addline(&notes, s);
		    }
		    else if( (repdata->est_csize * 1.1 < outsize) && (outsize - repdata->est_csize > 1.0e5 ) ) {
			g_snprintf(s, 1000,
				_("  small estimate: %s %s %d"),
				 repdata->disk->host->hostname,
				 repdata->disk->name,
				 repdata->level);
			s[999] = '\0';
			addline(&notes, s);
			g_snprintf(s, 1000,
				 _("                  est: %.0lf%s    out %.0lf%s"),
				 du(repdata->est_csize), displayunit,
				 du(outsize), displayunit);
			s[999] = '\0';
			addline(&notes, s);
		    }
		}
	    }
	}
    }
}

static char *
prefix (
    char *	host,
    char *	disk,
    int		level)
{
    static char *str = NULL;

    if (level == -987) {
	str = newvstrallocf(str, " %s %s",
			host ? host : _("(host?)"),
			disk ? disk : _("(disk?)"));
    } else {
	str = newvstrallocf(str, " %s %s lev %d",
			host ? host : _("(host?)"),
			disk ? disk : _("(disk?)"),
			level);
    }
    return str;
}


static char *
prefixstrange (
    char *	host,
    char *	disk,
    int		level,
    size_t	len_host,
    size_t	len_disk)
{
    char *h, *d;
    size_t l;
    static char *str = NULL;

    h=alloc(len_host+1);
    if(host) {
	strncpy(h, host, len_host);
    } else {
	strncpy(h, _("(host?)"), len_host);
    }
    h[len_host] = '\0';
    for(l = strlen(h); l < len_host; l++) {
	h[l] = ' ';
    }
    d=alloc(len_disk+1);
    if(disk) {
	strncpy(d, disk, len_disk);
    } else {
	strncpy(d, _("(disk?)"), len_disk);
    }
    d[len_disk] = '\0';
    for(l = strlen(d); l < len_disk; l++) {
	d[l] = ' ';
    }
    if (level == -987) {
	str = newvstrallocf(str, " %s %s", h, d);
    } else {
	str = newvstrallocf(str, " %s %s lev %d", h, d, level);
    }
    amfree(h);
    amfree(d);
    return str;
}


static void
addtoX_summary (
    X_summary_t **first,
    X_summary_t **last,
    char 	 *host,
    char 	 *disk,
    int		  level,
    char 	 *str)
{
    X_summary_t *X_summary;

    X_summary = alloc(SIZEOF(X_summary_t));
    X_summary->hostname = stralloc(host);
    X_summary->diskname = stralloc(disk);
    X_summary->level    = level;
    X_summary->str      = stralloc(str);
    X_summary->next = NULL;
    if (*first == NULL) {
	*first = X_summary;
    }
    else {
        (*last)->next = X_summary;
    }
    *last = X_summary;
}

static void
copy_template_file(
    char *	lbl_templ)
{
  char buf[BUFSIZ];
  int fd;
  ssize_t numread;

  lbl_templ = config_dir_relative(lbl_templ);
  if ((fd = open(lbl_templ, 0)) < 0) {
    curlog = L_ERROR;
    curprog = P_REPORTER;
    curstr = vstrallocf(_("could not open PostScript template file %s: %s"),
		       lbl_templ, strerror(errno));
    handle_error();
    amfree(curstr);
    amfree(lbl_templ);
    afclose(postscript);
    return;
  }
  while ((numread = read(fd, buf, SIZEOF(buf))) > 0) {
    if (fwrite(buf, (size_t)numread, 1, postscript) != 1) {
      curlog = L_ERROR;
      curprog = P_REPORTER;
      curstr = vstrallocf(_("error copying PostScript template file %s: %s"),
		         lbl_templ, strerror(errno));
      handle_error();
      amfree(curstr);
      amfree(lbl_templ);
      afclose(postscript);
      return;
    }
  }
  if (numread < 0) {
    curlog = L_ERROR;
    curprog = P_REPORTER;
    curstr = vstrallocf(_("error reading PostScript template file %s: %s"),
		       lbl_templ, strerror(errno));
    handle_error();
    amfree(curstr);
    amfree(lbl_templ);
    afclose(postscript);
    return;
  }
  close(fd);
  amfree(lbl_templ);
}

static repdata_t *
find_repdata(
    /*@keep@*/	disk_t *dp,
    		char *	datestamp,
    		int	level)
{
    repdata_t *repdata, *prev;

    if(!datestamp)
	datestamp = run_datestamp;
    prev = NULL;
    for(repdata = data(dp); repdata != NULL && (repdata->level != level || strcmp(repdata->datestamp,datestamp)!=0); repdata = repdata->next) {
	prev = repdata;
    }
    if(!repdata) {
	repdata = (repdata_t *)alloc(SIZEOF(repdata_t));
	memset(repdata, '\0', SIZEOF(repdata_t));
	repdata->disk = dp;
	repdata->datestamp = stralloc(datestamp ? datestamp : "");
	repdata->level = level;
	repdata->dumper.result = L_BOGUS;
	repdata->taper.result = L_BOGUS;
	repdata->next = NULL;
	if(prev)
	    prev->next = repdata;
	else
	    dp->up = (void *)repdata;
    }
    return repdata;
}


static void
do_postscript_output(void)
{
    tapetype_t *tp = lookup_tapetype(getconf_str(CNF_TAPETYPE));
    disk_t *dp;
    repdata_t *repdata;
    double outsize, origsize;
    off_t tapesize;
    off_t marksize;

    if (!tp)
	return;

    tapesize = tapetype_get_length(tp);
    marksize = tapetype_get_filemark(tp);

    for(current_tape = stats_by_tape; current_tape != NULL;
	    current_tape = current_tape->next) {

	if (current_tape->label == NULL) {
	    break;
	}

	copy_template_file(tapetype_get_lbl_templ(tp));

	if (postscript == NULL)
	    return;

	/* generate a few elements */
	g_fprintf(postscript,"(%s) DrawDate\n\n",
		    nicedate(run_datestamp ? run_datestamp : "0"));
	g_fprintf(postscript,_("(Amanda Version %s) DrawVers\n"),version());
	g_fprintf(postscript,"(%s) DrawTitle\n", current_tape->label);

	/* Stats */
	g_fprintf(postscript, "(Total Size:        %6.1lf MB) DrawStat\n",
	      mb(current_tape->coutsize));
	g_fprintf(postscript, _("(Tape Used (%%)       "));
	divzero(postscript, pct(current_tape->coutsize + 
				marksize * (current_tape->tapedisks + current_tape->tapechunks)),
				(double)tapesize);
	g_fprintf(postscript," %%) DrawStat\n");
	g_fprintf(postscript, _("(Compression Ratio:  "));
	divzero(postscript, pct(current_tape->coutsize),current_tape->corigsize);
	g_fprintf(postscript," %%) DrawStat\n");
	g_fprintf(postscript,_("(Filesystems Taped: %4d) DrawStat\n"),
		  current_tape->tapedisks);

	/* Summary */

	g_fprintf(postscript,
	      "(-) (%s) (-) (  0) (      32) (      32) DrawHost\n",
	      current_tape->label);

	for(dp = sortq.head; dp != NULL; dp = dp->next) {
	    if (dp->todo == 0) {
		 continue;
	    }
	    for(repdata = data(dp); repdata != NULL; repdata = repdata->next) {

		if(repdata->taper.tapelabel != current_tape->label) {
		    continue;
		}

		if(repdata->dumper.result == L_SUCCESS ||
		   repdata->dumper.result == L_PARTIAL)
		    origsize = repdata->dumper.origsize;
		else
		    origsize = repdata->taper.origsize;

		if(repdata->taper.result == L_SUCCESS ||
		   repdata->taper.result == L_PARTIAL)
		    outsize  = repdata->taper.outsize;
		else
		    outsize  = repdata->dumper.outsize;

		if (repdata->taper.result == L_SUCCESS ||
		    repdata->taper.result == L_PARTIAL) {
		    if(isnormal(origsize)) {
			g_fprintf(postscript,"(%s) (%s) (%d) (%3.0d) (%8.0lf) (%8.0lf) DrawHost\n",
			    dp->host->hostname, dp->name, repdata->level,
			    repdata->taper.filenum, origsize, 
			    outsize);
		    }
		    else {
			g_fprintf(postscript,"(%s) (%s) (%d) (%3.0d) (%8s) (%8.0lf) DrawHost\n",
			    dp->host->hostname, dp->name, repdata->level,
			    repdata->taper.filenum, "", 
			    outsize);
		    }
		}
	    }
	}
	
	g_fprintf(postscript,"\nshowpage\n");
    }
}
