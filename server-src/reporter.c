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
 *     summary stats
 *     details for errors
 *     notes
 *     success summary
 */

#include "amanda.h"
#include "conffile.h"
#include "tapefile.h"
#include "diskfile.h"
#include "infofile.h"
#include "logfile.h"
#include "version.h"
#include "util.h"

/* don't have (or need) a skipped type except internally to reporter */
#define L_SKIPPED	L_MARKER

typedef struct line_s {
    struct line_s *next;
    char *str;
} line_t;

typedef struct timedata_s {
    logtype_t result;
    double origsize, outsize;
    char *datestamp;
    double sec, kps;
    int filenum;
    char *tapelabel;
} timedata_t;

typedef struct repdata_s {
    disk_t *disk;
    char *datestamp;
    double est_nsize, est_csize;
    timedata_t taper;
    timedata_t dumper;
    timedata_t chunker;
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

static int dumpdisks[10], tapedisks[10], tapechunks[10];	/* by-level breakdown of disk count */

typedef struct taper_s {
    char *label;
    double taper_time;
    double coutsize, corigsize;
    int tapedisks, tapechunks;
    struct taper_s *next;
} taper_t;

static taper_t *stats_by_tape = NULL;
static taper_t *current_tape = NULL;

typedef struct strange_s {
    char *hostname;
    char *diskname;
    int  level;
    char *str;
    struct strange_s *next;
} strange_t;

static strange_t *first_strange=NULL, *last_strange=NULL;

static double total_time, startup_time, planner_time;

/* count files to tape */
static int tapefcount = 0;

static char *run_datestamp;
static char *tape_labels = NULL;
static int last_run_tapes = 0;
static int degraded_mode = 0; /* defined in driverio too */
static int normal_run = 0;
static int amflush_run = 0;
static int got_finish = 0;

static char *tapestart_error = NULL;

static FILE *logfile, *mailf;

static FILE *postscript;
static char *printer;

static disklist_t diskq;
static disklist_t sortq;

static line_t *errsum = NULL;
static line_t *errdet = NULL;
static line_t *notes = NULL;

static char MaxWidthsRequested = 0;	/* determined via config data */

char *displayunit;
long int unitdivisor;

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
static repdata_t *handle_chunk(void);
static repdata_t *handle_success(logtype_t logtype);
static void	addline(line_t **lp, char *str);
static void	addtostrange(char *host, char *disk, int level, char *str);
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
static void	output_strange(void);
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
    snprintf(RuleSpace, (size_t)RuleSpaceSize, "%*s%*.*s ", cd->PrefixSpace, "", 
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
    	snprintf(PrtBuf, SIZEOF(PrtBuf),
	  "%*s", cd->Width, "-- ");
    else
    	snprintf(PrtBuf, SIZEOF(PrtBuf),
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
		    error("ungetc failed: %s\n", strerror(errno));
		    /*NOTREACHED*/
		}
		error("ungetc failed: EOF\n");
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
    new->str = stralloc(str);

    /* add to end of list */
    p = *lp;
    if (p == NULL) {
	*lp = new;
    } else {
	while (p->next != NULL)
	    p = p->next;
	p->next = new;	
    }
}

static void
usage(void)
{
    error("Usage: amreport conf [-i] [-M address] [-f output-file] [-l logfile] [-p postscript-file] [-o configoption]*");
    /*NOTREACHED*/
}

int
main(
    int		argc,
    char **	argv)
{
    char *conffile;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_infofile;
    char *logfname, *psfname, *outfname, *subj_str = NULL;
    tapetype_t *tp;
    int opt;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    char *mail_cmd = NULL, *printer_cmd = NULL;
    extern int optind;
    char my_cwd[STR_SIZE];
    char *ColumnSpec = "";
    char *errstr = NULL;
    int cn;
    int mailout = 1;
    char *mailto = NULL;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;
    char *lbl_templ = NULL;

    safe_fd(-1, 0);

    set_pname("amreport");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    /* Process options */
    
    erroutput_type = ERR_INTERACTIVE;
    outfname = NULL;
    psfname = NULL;
    logfname = NULL;

    if (getcwd(my_cwd, SIZEOF(my_cwd)) == NULL) {
	error("cannot determine current working directory");
	/*NOTREACHED*/
    }

    parse_server_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    if (my_argc < 2) {
	config_dir = stralloc2(my_cwd, "/");
	if ((config_name = strrchr(my_cwd, '/')) != NULL) {
	    config_name = stralloc(config_name + 1);
	}
    } else {
	if (my_argv[1][0] == '-') {
	    usage();
	    return 1;
	}
	config_name = stralloc(my_argv[1]);
	config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
	--my_argc; ++my_argv;
	while((opt = getopt(my_argc, my_argv, "M:f:l:p:i")) != EOF) {
	    switch(opt) {
	    case 'i': 
		mailout = 0;
		break;
            case 'M':
		if (mailto != NULL) {
		    error("you may specify at most one -M");
		    /*NOTREACHED*/
		}
                mailto = stralloc(optarg);
		if(!validate_mailto(mailto)) {
		    error("mail address has invalid characters");
		    /*NOTREACHED*/
		}
                break;
            case 'f':
		if (outfname != NULL) {
		    error("you may specify at most one -f");
		    /*NOTREACHED*/
		}
		if (*optarg == '/') {
                    outfname = stralloc(optarg);
		} else {
                    outfname = vstralloc(my_cwd, "/", optarg, NULL);
		}
                break;
            case 'l':
		if (logfname != NULL) {
		    error("you may specify at most one -l");
		    /*NOTREACHED*/
		}
		if (*optarg == '/') {
		    logfname = stralloc(optarg);
		} else {
                    logfname = vstralloc(my_cwd, "/", optarg, NULL);
		}
                break;
            case 'p':
		if (psfname != NULL) {
		    error("you may specify at most one -p");
		    /*NOTREACHED*/
		}
		if (*optarg == '/') {
                    psfname = stralloc(optarg);
		} else {
                    psfname = vstralloc(my_cwd, "/", optarg, NULL);
		}
                break;
            case '?':
		usage();
		return 1;
            default:
		break;
	    }
	}

	my_argc -= optind;
	my_argv += optind;
    }
    if( !mailout && mailto ){
	printf("You cannot specify both -i & -M at the same time\n");
	exit(1);
    }


#if !defined MAILER
    if(!outfname) {
	printf("You must run amreport with '-f <output file>' because configure\n");
	printf("didn't find a mailer.\n");
	exit (1);
    }
#endif

    safe_cd();

    /* read configuration files */

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    /* Ignore error from read_conffile */
    read_conffile(conffile);
    amfree(conffile);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();
    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
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
    
    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    /* Ignore error from read_tapelist */
    read_tapelist(conf_tapelist);
    amfree(conf_tapelist);
    conf_infofile = getconf_str(CNF_INFOFILE);
    if (*conf_infofile == '/') {
	conf_infofile = stralloc(conf_infofile);
    } else {
	conf_infofile = stralloc2(config_dir, conf_infofile);
    }
    if(open_infofile(conf_infofile)) {
	error("could not open info db \"%s\"", conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);

    displayunit = getconf_str(CNF_DISPLAYUNIT);
    unitdivisor = getconf_unit_divisor();

    ColumnSpec = getconf_str(CNF_COLUMNSPEC);
    if(SetColumDataFromString(ColumnData, ColumnSpec, &errstr) < 0) {
	curlog = L_ERROR;
	curprog = P_REPORTER;
	curstr = errstr;
	handle_error();
        amfree(errstr);
	curstr = NULL;
	ColumnSpec = "";		/* use the default */
	if(SetColumDataFromString(ColumnData, ColumnSpec, &errstr) < 0) {
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

	conf_logdir = getconf_str(CNF_LOGDIR);
	if (*conf_logdir == '/') {
	    conf_logdir = stralloc(conf_logdir);
	} else {
	    conf_logdir = stralloc2(config_dir, conf_logdir);
	}
	logfname = vstralloc(conf_logdir, "/", "log", NULL);
	amfree(conf_logdir);
    }

    if((logfile = fopen(logfname, "r")) == NULL) {
	curlog = L_ERROR;
	curprog = P_REPORTER;
	curstr = vstralloc("could not open log ",
			   logfname,
			   ": ",
			   strerror(errno),
			   NULL);
	handle_error();
	amfree(curstr);
    }

    while(logfile && get_logline(logfile)) {
	switch(curlog) {
	case L_START:   handle_start(); break;
	case L_FINISH:  handle_finish(); break;

	case L_INFO:    handle_note(); break;
	case L_WARNING: handle_note(); break;

	case L_SUMMARY: handle_summary(); break;
	case L_STATS:   handle_stats(); break;

	case L_ERROR:   handle_error(); break;
	case L_FATAL:   handle_error(); break;

	case L_DISK:    handle_disk(); break;

	case L_SUCCESS: handle_success(curlog); break;
	case L_CHUNKSUCCESS: handle_success(curlog); break;
	case L_CHUNK:   handle_chunk(); break;
	case L_PARTIAL: handle_partial(); break;
	case L_STRANGE: handle_strange(); break;
	case L_FAIL:    handle_failed(); break;

	default:
	    curlog = L_ERROR;
	    curprog = P_REPORTER;
	    curstr = stralloc2("unexpected log line: ", curstr);
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

    /* open pipe to mailer */

    if(outfname) {
	/* output to a file */
	if((mailf = fopen(outfname,"w")) == NULL) {
	    error("could not open output file: %s %s", outfname, strerror(errno));
	    /*NOTREACHED*/
	}
	fprintf(mailf, "To: %s\n", mailto);
	fprintf(mailf, "Subject: %s\n\n", subj_str);

    } else {
#ifdef MAILER
    	if(mailto) {
		mail_cmd = vstralloc(MAILER,
			     " -s", " \"", subj_str, "\"",
			     " ", mailto, NULL);
		if((mailf = popen(mail_cmd, "w")) == NULL) {
	    	error("could not open pipe to \"%s\": %s",
		  	mail_cmd, strerror(errno));
	    	/*NOTREACHED*/
		}
	}
	else {
		if(mailout) {
                   printf("No mail sent! ");
		   printf("No valid mail address has been specified in amanda.conf or on the commmand line\n");
		}
		mailf = NULL;
	}
#endif
    }

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
		curstr = vstralloc("could not open ",
				   psfname,
				   ": ",
				   strerror(errno),
				   NULL);
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
		curstr = vstralloc("could not open pipe to ",
				   printer_cmd,
				   ": ",
				   strerror(errno),
				   NULL);
		handle_error();
		amfree(curstr);
	    }
#else
	    curlog = L_ERROR;
	    curprog = P_REPORTER;
	    curstr = stralloc("no printer command defined");
	    handle_error();
	    amfree(curstr);
#endif
	}
    }

    amfree(subj_str);

    if(mailf) {

    	if(!got_finish) fputs("*** THE DUMPS DID NOT FINISH PROPERLY!\n\n", mailf);

    	output_tapeinfo();

    	if(first_strange || errsum) {
		fprintf(mailf,"\nFAILURE AND STRANGE DUMP SUMMARY:\n");
		if(first_strange) output_strange();
		if(errsum) output_lines(errsum, mailf);
    	}
    	fputs("\n\n", mailf);
	
    	output_stats();
	
    	if(errdet) {
		fprintf(mailf,"\n\014\nFAILED AND STRANGE DUMP DETAILS:\n");
		output_lines(errdet, mailf);
    	}
    	if(notes) {
		fprintf(mailf,"\n\014\nNOTES:\n");
		output_lines(notes, mailf);
    	}
    	sort_disks();
    	if(sortq.head != NULL) {
		fprintf(mailf,"\n\014\nDUMP SUMMARY:\n");
		output_summary();
    	}
    	fprintf(mailf,"\n(brought to you by Amanda version %s)\n",
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
	    error("printer command failed: %s", printer_cmd);
	    /*NOTREACHED*/
	}
	postscript = NULL;
    }

    /* close output file */
    if(outfname) {
        afclose(mailf);
    }
    else if(mailf) {
        if(pclose(mailf) != 0) {
            error("mail command failed: %s", mail_cmd);
	    /*NOTREACHED*/
	}
        mailf = NULL;
    }

    clear_tapelist();
    free_disklist(&diskq);
    free_new_argv(new_argc, new_argv);
    free_server_config();
    amfree(run_datestamp);
    amfree(tape_labels);
    amfree(config_dir);
    amfree(config_name);
    amfree(printer_cmd);
    amfree(mail_cmd);
    amfree(logfname);

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    dbclose();
    return 0;
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
	    fprintf((fp),"  -- ");	    \
	else if ((q = (a)/q) >= 999.95)	    \
	    fprintf((fp), "###.#");	    \
	else				    \
	    fprintf((fp), "%5.1lf",q);	    \
    } while(0)
#define divzero_wide(fp,a,b)	       	    \
    do {       	       	       	       	    \
	double q = (b);			    \
	if (!isnormal(q))		    \
	    fprintf((fp),"    -- ");	    \
	else if ((q = (a)/q) >= 99999.95)   \
	    fprintf((fp), "#####.#");	    \
	else				    \
	    fprintf((fp), "%7.1lf",q);	    \
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

    fprintf(mailf,"STATISTICS:\n");
    fprintf(mailf,
	    "                          Total       Full      Incr.\n");
    fprintf(mailf,
	    "                        --------   --------   --------\n");

    fprintf(mailf,
	    "Estimate Time (hrs:min)   %2d:%02d\n", hrmn(planner_time));

    fprintf(mailf,
	    "Run Time (hrs:min)        %2d:%02d\n", hrmn(total_time));

    fprintf(mailf,
	    "Dump Time (hrs:min)       %2d:%02d      %2d:%02d      %2d:%02d\n",
	    hrmn(stats[2].dumper_time), hrmn(stats[0].dumper_time),
	    hrmn(stats[1].dumper_time));

    fprintf(mailf,
	    "Output Size (meg)      %8.1lf   %8.1lf   %8.1lf\n",
	    mb(stats[2].outsize), mb(stats[0].outsize), mb(stats[1].outsize));

    fprintf(mailf,
	    "Original Size (meg)    %8.1lf   %8.1lf   %8.1lf\n",
	    mb(stats[2].origsize), mb(stats[0].origsize),
	    mb(stats[1].origsize));

    fprintf(mailf, "Avg Compressed Size (%%)   ");
    divzero(mailf, pct(stats[2].coutsize),stats[2].corigsize);
    fputs("      ", mailf);
    divzero(mailf, pct(stats[0].coutsize),stats[0].corigsize);
    fputs("      ", mailf);
    divzero(mailf, pct(stats[1].coutsize),stats[1].corigsize);

    if(stats[1].dumpdisks > 0) fputs("   (level:#disks ...)", mailf);
    putc('\n', mailf);

    fprintf(mailf,
	    "Filesystems Dumped         %4d       %4d       %4d",
	    stats[2].dumpdisks, stats[0].dumpdisks, stats[1].dumpdisks);

    if(stats[1].dumpdisks > 0) {
	first = 1;
	for(lv = 1; lv < 10; lv++) if(dumpdisks[lv]) {
	    fputs(first?"   (":" ", mailf);
	    first = 0;
	    fprintf(mailf, "%d:%d", lv, dumpdisks[lv]);
	}
	putc(')', mailf);
    }
    putc('\n', mailf);

    fprintf(mailf, "Avg Dump Rate (k/s)     ");
    divzero_wide(mailf, stats[2].outsize,stats[2].dumper_time);
    fputs("    ", mailf);
    divzero_wide(mailf, stats[0].outsize,stats[0].dumper_time);
    fputs("    ", mailf);
    divzero_wide(mailf, stats[1].outsize,stats[1].dumper_time);
    putc('\n', mailf);

    putc('\n', mailf);
    fprintf(mailf,
	    "Tape Time (hrs:min)       %2d:%02d      %2d:%02d      %2d:%02d\n",
	    hrmn(stats[2].taper_time), hrmn(stats[0].taper_time),
	    hrmn(stats[1].taper_time));

    fprintf(mailf,
	    "Tape Size (meg)        %8.1lf   %8.1lf   %8.1lf\n",
	    mb(stats[2].tapesize), mb(stats[0].tapesize),
	    mb(stats[1].tapesize));

    fprintf(mailf, "Tape Used (%%)             ");
    divzero(mailf, pct(stats[2].tapesize+marksize*(stats[2].tapedisks+stats[2].tapechunks)),(double)tapesize);
    fputs("      ", mailf);
    divzero(mailf, pct(stats[0].tapesize+marksize*(stats[0].tapedisks+stats[0].tapechunks)),(double)tapesize);
    fputs("      ", mailf);
    divzero(mailf, pct(stats[1].tapesize+marksize*(stats[1].tapedisks+stats[1].tapechunks)),(double)tapesize);

    if(stats[1].tapedisks > 0) fputs("   (level:#disks ...)", mailf);
    putc('\n', mailf);

    fprintf(mailf,
	    "Filesystems Taped          %4d       %4d       %4d",
	    stats[2].tapedisks, stats[0].tapedisks, stats[1].tapedisks);

    if(stats[1].tapedisks > 0) {
	first = 1;
	for(lv = 1; lv < 10; lv++) if(tapedisks[lv]) {
	    fputs(first?"   (":" ", mailf);
	    first = 0;
	    fprintf(mailf, "%d:%d", lv, tapedisks[lv]);
	}
	putc(')', mailf);
    }
    putc('\n', mailf);

    if(stats[1].tapechunks > 0) fputs("   (level:#chunks ...)", mailf);
    putc('\n', mailf);

    fprintf(mailf,
	    "Chunks Taped               %4d       %4d       %4d",
	    stats[2].tapechunks, stats[0].tapechunks, stats[1].tapechunks);

    if(stats[1].tapechunks > 0) {
	first = 1;
	for(lv = 1; lv < 10; lv++) if(tapechunks[lv]) {
	    fputs(first?"   (":" ", mailf);
	    first = 0;
	    fprintf(mailf, "%d:%d", lv, tapechunks[lv]);
	}
	putc(')', mailf);
    }
    putc('\n', mailf);

    fprintf(mailf, "Avg Tp Write Rate (k/s) ");
    divzero_wide(mailf, stats[2].tapesize,stats[2].taper_time);
    fputs("    ", mailf);
    divzero_wide(mailf, stats[0].tapesize,stats[0].taper_time);
    fputs("    ", mailf);
    divzero_wide(mailf, stats[1].tapesize,stats[1].taper_time);
    putc('\n', mailf);

    if(stats_by_tape) {
	int label_length = (int)strlen(stats_by_tape->label) + 5;
	fprintf(mailf,"\nUSAGE BY TAPE:\n");
	fprintf(mailf,"  %-*s  Time      Size      %%    Nb    Nc\n",
		label_length, "Label");
	for(current_tape = stats_by_tape; current_tape != NULL;
	    current_tape = current_tape->next) {
	    fprintf(mailf, "  %-*s", label_length, current_tape->label);
	    fprintf(mailf, " %2d:%02d", hrmn(current_tape->taper_time));
	    fprintf(mailf, " %8.0lf%s  ", du(current_tape->coutsize), displayunit);
	    divzero(mailf, pct(current_tape->coutsize + marksize *
		   (current_tape->tapedisks+current_tape->tapechunks)),
		   (double)tapesize);
	    fprintf(mailf, "  %4d", current_tape->tapedisks);
	    fprintf(mailf, "  %4d\n", current_tape->tapechunks);
	}
    }
}

/* ----- */

static void
output_tapeinfo(void)
{
    tape_t *tp, *lasttp;
    int run_tapes;
    int skip = 0;

    if (last_run_tapes > 0) {
	if(amflush_run)
	    fprintf(mailf, "The dumps were flushed to tape%s %s.\n",
		    last_run_tapes == 1 ? "" : "s",
		    tape_labels ? tape_labels : "");
	else
	    fprintf(mailf, "These dumps were to tape%s %s.\n",
		    last_run_tapes == 1 ? "" : "s",
		    tape_labels ? tape_labels : "");
    }

    if(degraded_mode) {
	fprintf(mailf,
		"*** A TAPE ERROR OCCURRED: %s.\n", tapestart_error);
	fputs("Some dumps may have been left in the holding disk.\n", mailf);
	fprintf(mailf,
		"Run amflush%s to flush them to tape.\n",
		amflush_run ? " again" : "");
    }

    tp = lookup_last_reusable_tape(skip);

    run_tapes = getconf_int(CNF_RUNTAPES);

    if (run_tapes == 1)
	fputs("The next tape Amanda expects to use is: ", mailf);
    else if(run_tapes > 1)
	fprintf(mailf, "The next %d tapes Amanda expects to use are: ",
		run_tapes);
    
    while(run_tapes > 0) {
	if(tp != NULL) {
	    fprintf(mailf, "%s", tp->label);
	} else {
	    if (run_tapes == 1)
		fprintf(mailf, "a new tape");
	    else
		fprintf(mailf, "%d new tapes", run_tapes);
	    run_tapes = 1;
	}

	if(run_tapes > 1) fputs(", ", mailf);

	run_tapes -= 1;
	skip++;
	tp = lookup_last_reusable_tape(skip);
    }
    fputs(".\n", mailf);

    lasttp = lookup_tapepos(lookup_nb_tape());
    run_tapes = getconf_int(CNF_RUNTAPES);
    if(lasttp && run_tapes > 0 && strcmp(lasttp->datestamp,"0") == 0) {
	int c = 0;
	while(lasttp && run_tapes > 0 && strcmp(lasttp->datestamp,"0") == 0) {
	    c++;
	    lasttp = lasttp->prev;
	    run_tapes--;
	}
	lasttp = lookup_tapepos(lookup_nb_tape());
	if(c == 1) {
	    fprintf(mailf, "The next new tape already labelled is: %s.\n",
		    lasttp->label);
	}
	else {
	    fprintf(mailf, "The next %d new tapes already labelled are: %s", c,
		    lasttp->label);
	    lasttp = lasttp->prev;
	    c--;
	    while(lasttp && c > 0 && strcmp(lasttp->datestamp,"0") == 0) {
		fprintf(mailf, ", %s", lasttp->label);
		lasttp = lasttp->prev;
		c--;
	    }
	    fprintf(mailf, ".\n");
	}
    }
}

/* ----- */
static void
output_strange(void)
{
    size_t len_host=0, len_disk=0;
    strange_t *strange;
    char *str = NULL;

    for(strange=first_strange; strange != NULL; strange = strange->next) {
	if(strlen(strange->hostname) > len_host)
	    len_host = strlen(strange->hostname);
	if(strlen(strange->diskname) > len_disk)
	    len_disk = strlen(strange->diskname);
    }
    for(strange=first_strange; strange != NULL; strange = strange->next) {
	str = vstralloc("  ", prefixstrange(strange->hostname, strange->diskname, strange->level, len_host, len_disk),
			"  ", strange->str, NULL);
	fprintf(mailf, "%s\n", str);
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

	snprintf(testBuf, SIZEOF(testBuf),
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
	snprintf(testBuf, SIZEOF(testBuf),
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
    for(dp = sortq.head; dp != NULL; dp = dp->next) {
      if(dp->todo) {
	for(repdata = data(dp); repdata != NULL; repdata = repdata->next) {
	    char TimeRateBuffer[40];

	    CheckStringMax(&ColumnData[HostName], dp->host->hostname);
	    CheckStringMax(&ColumnData[Disk], dp->name);
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
		if(dp->compress == COMP_NONE)
		    f = 0.0;
		else 
		    f = repdata->dumper.origsize;
		CheckStringMax(&ColumnData[Disk], 
			sDivZero(pct(repdata->dumper.outsize), f, Compress));

		if(!amflush_run)
		    snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
				"%3d:%02d", mnsc(repdata->dumper.sec));
		else
		    snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
				"N/A ");
		CheckStringMax(&ColumnData[DumpTime], TimeRateBuffer);

		CheckFloatMax(&ColumnData[DumpRate], repdata->dumper.kps); 
	    }

	    if(repdata->taper.result == L_FAIL) {
		CheckStringMax(&ColumnData[TapeTime], "FAILED");
		continue;
	    }
	    if(repdata->taper.result == L_SUCCESS ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer), 
		  "%3d:%02d", mnsc(repdata->taper.sec));
	    else
		snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "N/A ");
	    CheckStringMax(&ColumnData[TapeTime], TimeRateBuffer);

	    if(repdata->taper.result == L_SUCCESS ||
		    repdata->taper.result == L_CHUNKSUCCESS)
		CheckFloatMax(&ColumnData[TapeRate], repdata->taper.kps);
	    else
		CheckStringMax(&ColumnData[TapeRate], "N/A ");
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
    fprintf(mailf, "%*s", w1+h, "");
    fprintf(mailf, "%-*s", wDump-h, ds);
    h = (int)strlen(ts);
    if (h > wTape) {
	h = 0;
    } else {
	h = (wTape-h)/2;
    }
    fprintf(mailf, "%*s", h, "");
    fprintf(mailf, "%-*s", wTape-h, ts);
    fputc('\n', mailf);

    /* print the titles */
    for (i=0; ColumnData[i].Name != NULL; i++) {
    	char *fmt;
    	ColumnInfo *cd= &ColumnData[i];
    	fprintf(mailf, "%*s", cd->PrefixSpace, "");
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
	fprintf(mailf, fmt, cd->Width, cd->Title);
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
	    size_t devlen;

	    cd= &ColumnData[HostName];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    fprintf(mailf, cd->Format, cd->Width, cd->Width, dp->host->hostname);

	    cd= &ColumnData[Disk];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    devname = sanitize_string(dp->name);
	    devlen = strlen(devname);
	    if (devlen > (size_t)cd->Width) {
	   	fputc('-', mailf); 
		fprintf(mailf, cd->Format, cd->Width-1, cd->Precision-1,
		  devname+devlen - (cd->Width-1) );
	    }
	    else
		fprintf(mailf, cd->Format, cd->Width, cd->Width, devname);
	    amfree(devname);
	    cd= &ColumnData[Level];
	    if (repdata->dumper.result == L_BOGUS &&
		repdata->taper.result  == L_BOGUS) {
	      if(amflush_run){
		fprintf(mailf, "%*s%s\n", cd->PrefixSpace+cd->Width, "",
			tmp=TextRule(OrigKB, TapeRate, "NO FILE TO FLUSH"));
	      } else {
		fprintf(mailf, "%*s%s\n", cd->PrefixSpace+cd->Width, "",
			tmp=TextRule(OrigKB, TapeRate, "MISSING"));
	      }
	      amfree(tmp);
	      continue;
	    }
	    
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    fprintf(mailf, cd->Format, cd->Width, cd->Precision,repdata->level);

	    if (repdata->dumper.result == L_SKIPPED) {
		fprintf(mailf, "%s\n",
			tmp=TextRule(OrigKB, TapeRate, "SKIPPED"));
		amfree(tmp);
		continue;
	    }
	    if (repdata->dumper.result == L_FAIL && (repdata->chunker.result != L_PARTIAL && repdata->taper.result  != L_PARTIAL)) {
		fprintf(mailf, "%s\n",
			tmp=TextRule(OrigKB, TapeRate, "FAILED"));
		amfree(tmp);
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
	       repdata->taper.result == L_PARTIAL ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		outsize  = repdata->taper.outsize;
	    else if(repdata->chunker.result == L_SUCCESS ||
		    repdata->chunker.result == L_PARTIAL ||
		    repdata->chunker.result == L_CHUNKSUCCESS)
		outsize  = repdata->chunker.outsize;
	    else
		outsize  = repdata->dumper.outsize;

	    cd= &ColumnData[OrigKB];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(isnormal(origsize))
		fprintf(mailf, cd->Format, cd->Width, cd->Precision, du(origsize));
	    else
		fprintf(mailf, "%*.*s", cd->Width, cd->Width, "N/A");

	    cd= &ColumnData[OutKB];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");

	    fprintf(mailf, cd->Format, cd->Width, cd->Precision, du(outsize));
	    	
	    cd= &ColumnData[Compress];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");

	    if(dp->compress == COMP_NONE)
		f = 0.0;
	    else if(origsize < 1.0)
		f = 0.0;
	    else
		f = origsize;

	    fputs(sDivZero(pct(outsize), f, Compress), mailf);

	    cd= &ColumnData[DumpTime];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->dumper.result == L_SUCCESS ||
	       repdata->dumper.result == L_CHUNKSUCCESS)
		snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "%3d:%02d", mnsc(repdata->dumper.sec));
	    else
		snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "N/A ");
	    fprintf(mailf, cd->Format, cd->Width, cd->Width, TimeRateBuffer);

	    cd= &ColumnData[DumpRate];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->dumper.result == L_SUCCESS ||
		    repdata->dumper.result == L_CHUNKSUCCESS)
		fprintf(mailf, cd->Format, cd->Width, cd->Precision, repdata->dumper.kps);
	    else
		fprintf(mailf, "%*s", cd->Width, "N/A ");

	    cd= &ColumnData[TapeTime];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->taper.result == L_FAIL) {
		fprintf(mailf, "%s\n",
			tmp=TextRule(TapeTime, TapeRate, "FAILED "));
		amfree(tmp);
		continue;
	    }

	    if(repdata->taper.result == L_SUCCESS || 
	       repdata->taper.result == L_PARTIAL ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "%3d:%02d", mnsc(repdata->taper.sec));
	    else
		snprintf(TimeRateBuffer, SIZEOF(TimeRateBuffer),
		  "N/A ");
	    fprintf(mailf, cd->Format, cd->Width, cd->Width, TimeRateBuffer);

	    cd= &ColumnData[TapeRate];
	    fprintf(mailf, "%*s", cd->PrefixSpace, "");
	    if(repdata->taper.result == L_SUCCESS || 
	       repdata->taper.result == L_PARTIAL ||
	       repdata->taper.result == L_CHUNKSUCCESS)
		fprintf(mailf, cd->Format, cd->Width, cd->Precision, repdata->taper.kps);
	    else
		fprintf(mailf, "%*s", cd->Width, "N/A ");

	    if(repdata->chunker.result == L_PARTIAL ||
	       repdata->taper.result == L_PARTIAL) {
		fprintf(mailf, " PARTIAL");
	    }
	    fputc('\n', mailf);
	}
      }
    }
}

static void
bogus_line(
    const char *err_text)
{
    printf("line %d of log is bogus: <%s>\n", curlinenum, curstr);
    printf("  Scan failed at: <%s>\n", err_text);
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
    static char *months[13] = { "BogusMonth",
	"January", "February", "March", "April", "May", "June",
	"July", "August", "September", "October", "November", "December"
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

    snprintf(nice, SIZEOF(nice), "%s %d, %d", months[month], day, year);

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
#define sc "datestamp"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    bogus_line(s - 1);
	    return;
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc
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
#define sc "label"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    bogus_line(s - 1);
	    return;
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc
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
#define sc "date"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    return;				/* ignore bogus line */
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc
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
     "  reporter: both amflush and planner output in log, ignoring amflush.");
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
#define sc "date"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    bogus_line(s - 1);
	    return;
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    bogus_line(s - 1);
	    return;
	}
	skip_non_whitespace(s, ch);	/* ignore the date string */

	skip_whitespace(s, ch);
#define sc "time"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    /* older planner doesn't write time */
	    if(curprog == P_PLANNER) return;
	    bogus_line(s - 1);
	    return;
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc

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
    char *hostname, *diskname, *datestamp;
    int level = 0;
    double sec, kps, nbytes, cbytes;
    repdata_t *repdata;
    disk_t *dp;

    if(curprog == P_DRIVER) {
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
#define sc "startup time"
	if(ch != '\0' && strncmp(s - 1, sc, sizeof(sc)-1) == 0) {
	    s += sizeof(sc)-1;
	    ch = s[-1];
#undef sc

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
#define sc "estimate"
	else if(ch != '\0' && strncmp(s - 1, sc, sizeof(sc)-1) == 0) {
	    s += sizeof(sc)-1;
	    ch = s[-1];
#undef sc

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
	    fp = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    diskname = stralloc(fp);
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
	    if(level < 0 || level > 9) {
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
		addtostrange(hostname, diskname, level,
			     "ERROR [not in disklist]");
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

    str = vstralloc("  ", program_str[curprog], ": ", curstr, NULL);
    addline(&notes, str);
    amfree(str);
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
#define sc "no-tape"
	if(ch != '\0' && strncmp(s - 1, sc, SIZEOF(sc)-1) == 0) {
	    s += SIZEOF(sc)-1;
	    ch = s[-1];
#undef sc

	    skip_whitespace(s, ch);
	    if(ch != '\0') {
		if((nl = strchr(s - 1, '\n')) != NULL) {
		    *nl = '\0';
		}
		tapestart_error = newstralloc(tapestart_error, s - 1);
		if(nl) *nl = '\n';
		degraded_mode = 1;
		return;
	    }
	    /* else some other tape error, handle like other errors */
	}
	/* else some other tape error, handle like other errors */
    }
    s = vstralloc("  ", program_str[curprog], ": ",
		  logtype_str[curlog], " ", curstr, NULL);
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
handle_chunk(void)
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
    if(level < 0 || level > 9) {
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
	
 	str = vstralloc("  ", prefix(hostname, diskname, level),
 			" ", "ERROR [not in disklist]",
 			NULL);
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
    if (sp->filenum == 0) {
 	sp->filenum = ++tapefcount;
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
    char *datestamp;

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

    if(strlen(datestamp) < 3) {
	level = atoi(datestamp);
	datestamp = newstralloc(datestamp, run_datestamp);
    }
    else {
	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    bogus_line(s - 1);
	    amfree(hostname);
	    amfree(diskname);
	    amfree(datestamp);
	    return NULL;
	}
	skip_integer(s, ch);
    }

    if(level < 0 || level > 9) {
	amfree(hostname);
	amfree(diskname);
	amfree(datestamp);
	return NULL;
    }

    skip_whitespace(s, ch);
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
    }


    dp = lookup_disk(hostname, diskname);
    if(dp == NULL) {
	addtostrange(hostname, qdiskname, level, "ERROR [not in disklist]");
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

    if(origkb < 0.0) {
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
	addtostrange(hostname, qdiskname, level, "was successfully retried");
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
	    error("current_tape == NULL");
	    /*NOTREACHED*/
	}
	stats[i].taper_time += sec;
	sp->filenum = ++tapefcount;
	sp->tapelabel = current_tape->label;
	tapedisks[level] +=1;
	stats[i].tapedisks +=1;
	stats[i].tapesize += kbytes;
	sp->outsize = kbytes;
	if(!isnormal(repdata->chunker.outsize) && isnormal(repdata->dumper.outsize)) { /* dump to tape */
	    stats[i].outsize += kbytes;
	    if(dp->compress != COMP_NONE) {
		stats[i].coutsize += kbytes;
	    }
	}
	if (logtype == L_SUCCESS || logtype== L_PARTIAL) {
	    current_tape->taper_time += sec;
	    current_tape->coutsize += kbytes;
	}
	current_tape->corigsize += origkb;
	current_tape->tapedisks += 1;
    }

    if(curprog == P_DUMPER) {
	stats[i].dumper_time += sec;
	if(dp->compress == COMP_NONE) {
	    sp->origsize = kbytes;
	}
	else {
	    stats[i].corigsize += sp->origsize;
	}
	dumpdisks[level] +=1;
	stats[i].dumpdisks +=1;
	stats[i].origsize += sp->origsize;
    }

    if(curprog == P_CHUNKER) {
	sp->outsize = kbytes;
	stats[i].outsize += kbytes;
	if(dp->compress != COMP_NONE) {
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

    repdata = handle_success(L_SUCCESS);
    if (!repdata)
	return;

    qdisk = quote_string(repdata->disk->name);

    addline(&errdet,"");
    str = vstralloc("/-- ", prefix(repdata->disk->host->hostname, 
				   qdisk, repdata->level),
		    " ", "STRANGE",
		    NULL);
    addline(&errdet, str);
    amfree(str);

    while(contline_next()) {
	get_logline(logfile);
#define sc "sendbackup: warning "
	if(strncmp(curstr, sc, SIZEOF(sc)-1) == 0) {
	    strangestr = newstralloc(strangestr, curstr+SIZEOF(sc)-1);
	}
	addline(&errdet, curstr);
    }
    addline(&errdet,"\\--------");

    str = vstralloc("STRANGE", " ", strangestr, NULL);
    addtostrange(repdata->disk->host->hostname, qdisk, repdata->level, str);
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
	addtostrange(hostname, qdiskname, level, "ERROR [not in disklist]");
    } else {
	repdata = find_repdata(dp, datestamp, level);

	if(curprog == P_TAPER)
	    sp = &(repdata->taper);
	else sp = &(repdata->dumper);

	if(sp->result != L_SUCCESS)
	    sp->result = L_FAIL;
    }
    amfree(datestamp);

    str = vstralloc("FAILED", " ", errstr, NULL);
    addtostrange(hostname, qdiskname, level, str);
    amfree(str);

    if(curprog == P_DUMPER) {
	addline(&errdet,"");
	str = vstralloc("/-- ", prefix(hostname, qdiskname, level),
			" ", "FAILED",
			" ", errstr,
			NULL);
	addline(&errdet, str);
	amfree(str);
	while(contline_next()) {
	    get_logline(logfile);
	    addline(&errdet, curstr);
	}
	addline(&errdet,"\\--------");
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
	    addtostrange(dp->host->hostname, qdisk, -987, "RESULTS MISSING");
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
		       repdata->taper.result == L_PARTIAL ||
		       repdata->taper.result == L_CHUNKSUCCESS)
			outsize  = repdata->taper.outsize;
		    else if(repdata->chunker.result == L_SUCCESS ||
			    repdata->chunker.result == L_PARTIAL ||
			    repdata->chunker.result == L_CHUNKSUCCESS)
			outsize  = repdata->chunker.outsize;
		    else
			outsize  = repdata->dumper.outsize;

		    if(repdata->est_csize * 0.9 > outsize) {
			snprintf(s, 1000,
				"  big estimate: %s %s %d",
				 repdata->disk->host->hostname,
				 repdata->disk->name,
				 repdata->level);
			s[999] = '\0';
			addline(&notes, s);
			snprintf(s, 1000,
				 "                est: %.0lf%s    out %.0lf%s",
				 du(repdata->est_csize), displayunit,
				 du(outsize), displayunit);
			s[999] = '\0';
			addline(&notes, s);
		    }
		    else if(repdata->est_csize * 1.1 < outsize) {
			snprintf(s, 1000,
				"  small estimate: %s %s %d",
				 repdata->disk->host->hostname,
				 repdata->disk->name,
				 repdata->level);
			s[999] = '\0';
			addline(&notes, s);
			snprintf(s, 1000,
				 "                  est: %.0lf%s    out %.0lf%s",
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
    char number[NUM_STR_SIZE];
    static char *str = NULL;

    snprintf(number, SIZEOF(number), "%d", level);
    str = newvstralloc(str,
		       " ", host ? host : "(host?)",
		       " ", disk ? disk : "(disk?)",
		       level != -987 ? " lev " : "",
		       level != -987 ? number : "",
		       NULL);
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
    char number[NUM_STR_SIZE];
    static char *str = NULL;

    snprintf(number, SIZEOF(number), "%d", level);
    h=alloc(len_host+1);
    if(host) {
	strncpy(h, host, len_host);
    } else {
	strncpy(h, "(host?)", len_host);
    }
    h[len_host] = '\0';
    for(l = strlen(h); l < len_host; l++) {
	h[l] = ' ';
    }
    d=alloc(len_disk+1);
    if(disk) {
	strncpy(d, disk, len_disk);
    } else {
	strncpy(d, "(disk?)", len_disk);
    }
    d[len_disk] = '\0';
    for(l = strlen(d); l < len_disk; l++) {
	d[l] = ' ';
    }
    str = newvstralloc(str,
		       h,
		       "  ", d,
		       level != -987 ? "  lev " : "",
		       level != -987 ? number : "",
		       NULL);
    amfree(h);
    amfree(d);
    return str;
}


static void
addtostrange (
    char *	host,
    char *	disk,
    int		level,
    char *	str)
{
    strange_t *strange;

    strange = alloc(SIZEOF(strange_t));
    strange->hostname = stralloc(host);
    strange->diskname = stralloc(disk);
    strange->level    = level;
    strange->str      = stralloc(str);
    strange->next = NULL;
    if(first_strange == NULL) {
	first_strange = strange;
    }
    else {
        last_strange->next = strange;
    }
    last_strange = strange;
}


static void
copy_template_file(
    char *	lbl_templ)
{
  char buf[BUFSIZ];
  int fd;
  ssize_t numread;

  if (strchr(lbl_templ, '/') == NULL) {
    lbl_templ = stralloc2(config_dir, lbl_templ);
  } else {
    lbl_templ = stralloc(lbl_templ);
  }
  if ((fd = open(lbl_templ, 0)) < 0) {
    curlog = L_ERROR;
    curprog = P_REPORTER;
    curstr = vstralloc("could not open PostScript template file ",
		       lbl_templ,
		       ": ",
		       strerror(errno),
		       NULL);
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
      curstr = vstralloc("error copying PostScript template file ",
		         lbl_templ,
		         ": ",
		         strerror(errno),
		         NULL);
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
    curstr = vstralloc("error reading PostScript template file ",
		       lbl_templ,
		       ": ",
		       strerror(errno),
		       NULL);
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

	/* generate a few elements */
	fprintf(postscript,"(%s) DrawDate\n\n",
		    nicedate(run_datestamp ? run_datestamp : "0"));
	fprintf(postscript,"(Amanda Version %s) DrawVers\n",version());
	fprintf(postscript,"(%s) DrawTitle\n", current_tape->label);

	/* Stats */
	fprintf(postscript, "(Total Size:        %6.1lf MB) DrawStat\n",
	      mb(current_tape->coutsize));
	fprintf(postscript, "(Tape Used (%%)       ");
	divzero(postscript, pct(current_tape->coutsize + 
				marksize * (current_tape->tapedisks + current_tape->tapechunks)),
				(double)tapesize);
	fprintf(postscript," %%) DrawStat\n");
	fprintf(postscript, "(Compression Ratio:  ");
	divzero(postscript, pct(current_tape->coutsize),current_tape->corigsize);
	fprintf(postscript," %%) DrawStat\n");
	fprintf(postscript,"(Filesystems Taped: %4d) DrawStat\n",
		  current_tape->tapedisks);

	/* Summary */

	fprintf(postscript,
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
			fprintf(postscript,"(%s) (%s) (%d) (%3.0d) (%8.0lf) (%8.0lf) DrawHost\n",
			    dp->host->hostname, dp->name, repdata->level,
			    repdata->taper.filenum, origsize, 
			    outsize);
		    }
		    else {
			fprintf(postscript,"(%s) (%s) (%d) (%3.0d) (%8s) (%8.0lf) DrawHost\n",
			    dp->host->hostname, dp->name, repdata->level,
			    repdata->taper.filenum, "N/A", 
			    outsize);
		    }
		}
	    }
	}
	
	fprintf(postscript,"\nshowpage\n");
    }
}
