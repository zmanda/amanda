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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: amadmin.c,v 1.124 2006/07/26 15:17:37 martinea Exp $
 *
 * controlling process for the Amanda backup system
 */
#include "amanda.h"
#include "cmdline.h"
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "infofile.h"
#include "logfile.h"
#include "version.h"
#include "holding.h"
#include "find.h"
#include "util.h"
#include "timestamp.h"
#include "server_util.h"
#include <getopt.h>

disklist_t diskq;

int main(int argc, char **argv);
void usage(void);
static void estimate(int argc, char **argv);
static void estimate_one(disk_t *dp);
void force(int argc, char **argv);
void force_one(disk_t *dp);
void unforce(int argc, char **argv);
void unforce_one(disk_t *dp);
void force_bump(int argc, char **argv);
void force_bump_one(disk_t *dp);
void force_no_bump(int argc, char **argv);
void force_no_bump_one(disk_t *dp);
void unforce_bump(int argc, char **argv);
void unforce_bump_one(disk_t *dp);
void reuse(int argc, char **argv);
void noreuse(int argc, char **argv);
void info(int argc, char **argv);
void info_one(disk_t *dp);
void due(int argc, char **argv);
void due_one(disk_t *dp);
void find(int argc, char **argv);
void holding(int argc, char **argv);
void delete(int argc, char **argv);
void delete_one(disk_t *dp);
void balance(int argc, char **argv);
void tape(int argc, char **argv);
void bumpsize(int argc, char **argv);
void diskloop(int argc, char **argv, char *cmdname, void (*func)(disk_t *dp));
char *seqdatestr(int seq);
static int next_level0(disk_t *dp, info_t *info);
int bump_thresh(int level);
void export_db(int argc, char **argv);
void import_db(int argc, char **argv);
void hosts(int argc, char **argv);
void dles(int argc, char **argv);
void disklist(int argc, char **argv);
void disklist_one(disk_t *dp);
void show_version(int argc, char **argv);
static void show_config(int argc, char **argv);

static char *conf_tapelist = NULL;
static char *displayunit;
static long int unitdivisor;
static gboolean print_default = 1;
static gboolean print_source = 0;
static int opt_days = -1;
static char *opt_sort = NULL;
static gboolean opt_long = 0;
static gboolean opt_outdated = 0;

static const struct {
    const char *name;
    void (*fn)(int, char **);
    const char *usage;
} cmdtab[] = {
    { "version", show_version,
	T_("\t\t\t\t\t# Show version info.") },
    { "config", show_config,
	T_("\t\t\t\t\t# Show configuration.") },
    { "estimate", estimate,
	T_(" [<hostname> [<disks>]* ]*\t# Print server estimate.") },
    { "force", force,
	T_(" [<hostname> [<disks>]* ]+\t\t# Force level 0 at next run.") },
    { "unforce", unforce,
	T_(" [<hostname> [<disks>]* ]+\t# Clear force command.") },
    { "force-bump", force_bump,
	T_(" [<hostname> [<disks>]* ]+\t# Force bump at next run.") },
    { "force-no-bump", force_no_bump,
	T_(" [<hostname> [<disks>]* ]+\t# Force no-bump at next run.") },
    { "unforce-bump", unforce_bump,
	T_(" [<hostname> [<disks>]* ]+\t# Clear bump command.") },
    { "disklist", disklist,
	T_(" [<hostname> [<disks>]* ]*\t# Debug disklist entries.") },
    { "hosts", hosts,
	T_("\t\t\t\t\t# Show all distinct hosts in disklist.") },
    { "dles", dles,
	T_("\t\t\t\t\t# Show all dles in disklist, one per line.") },
    { "reuse", reuse,
	T_(" <tapelabel> ...\t\t # re-use this tape.") },
    { "no-reuse", noreuse,
	T_(" <tapelabel> ...\t # never re-use this tape.") },
    { "find", find,
	T_(" [<hostname> [<disks>]* ]*\t # Show which tapes these dumps are on.") },
    { "holding", holding,
	T_(" {list [ -l ] |delete} [ <hostname> [ <disk> [ <datestamp> [ .. ] ] ] ]+\t # Show or delete holding disk contents.") },
    { "delete", delete,
	T_(" [<hostname> [<disks>]* ]+ # Delete from database.") },
    { "info", info,
	T_(" [<hostname> [<disks>]* ]*\t # Show current info records.") },
    { "due", due,
	T_(" [<hostname> [<disks>]* ]*\t # Show due date.") },
    { "balance", balance,
	T_(" [--days <num>]\t\t # Show nightly dump size balance.") },
    { "tape", tape,
	T_(" [--days <num>]\t\t # Show which tape is due next.") },
    { "bumpsize", bumpsize,
	T_("\t\t\t # Show current bump thresholds.") },
    { "export", export_db,
	T_(" [<hostname> [<disks>]* ]* # Export curinfo database to stdout.") },
    { "import", import_db,
	T_("\t\t\t\t # Import curinfo database from stdin.") },
};
#define	NCMDS	(int)(sizeof(cmdtab) / sizeof(cmdtab[0]))

static struct option long_options[] = {
    {"version"       , 0, NULL,  1},
    {"no-default"    , 0, NULL,  2},
    {"print-source"  , 0, NULL,  3},
    {"days"          , 1, NULL,  4},
    {"sort"          , 1, NULL,  5},
    {NULL, 0, NULL, 0}
};

int
main(
    int		argc,
    char **	argv)
{
    int i;
    char *conf_diskfile;
    char *conf_infofile;
    config_overrides_t *cfg_ovr = NULL;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amadmin");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    add_amanda_log_handler(amanda_log_stderr);

    cfg_ovr = extract_commandline_config_overrides(&argc, &argv);

    while (1) {
	int option_index = 0;
        int c;
        c = getopt_long(argc, argv, "ld", long_options, &option_index);

	if (c == -1) {
	    break;
	}

	switch(c) {
	case 1: printf("amadmin-%s\n", VERSION);
		return 0;
	case 2: print_default = 0;
		break;
	case 3: print_source = 1;
		break;
	case 4: opt_days = atoi(optarg);
		break;
	case 5: opt_sort = g_strdup(optarg);
		break;
	case 'l': opt_long = TRUE;
		break;
	case 'd': opt_outdated = TRUE;
		break;
	default: usage();
	}
    }
    argc -= optind-1, argv += optind-1;

    if(argc < 3) usage();

    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_EXPLICIT_NAME, argv[1]);

    if(strcmp(argv[2],"version") == 0) {
	show_version(argc, argv);
	goto done;
    }

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &diskq);
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    check_running_as(RUNNING_AS_DUMPUSER);

    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    if(read_tapelist(conf_tapelist)) {
	error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    /* conf_tapelist is not freed yet -- it may be used to write the
     * tapelist later. */

    conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
    if(open_infofile(conf_infofile)) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);

    displayunit = getconf_str(CNF_DISPLAYUNIT);
    unitdivisor = getconf_unit_divisor();

    for (i = 0; i < NCMDS; i++)
	if (strcmp(argv[2], cmdtab[i].name) == 0) {
	    (*cmdtab[i].fn)(argc, argv);
	    break;
	}
    if (i == NCMDS) {
	g_fprintf(stderr, _("%s: unknown command \"%s\"\n"), argv[0], argv[2]);
	usage();
    }

    close_infofile();
    clear_tapelist();
    amfree(conf_tapelist);

done:

    free_disklist(&diskq);
    dbclose();
    return 0;
}


void
usage(void)
{
    int i;

    g_fprintf(stderr, _("\nUsage: %s [--version] [--no-default] [--print-source] [-o configoption]*\n               <conf> <command> {<args>} ...\n"),
	    get_pname());
    g_fprintf(stderr, _("    Valid <command>s are:\n"));
    for (i = 0; i < NCMDS; i++)
	g_fprintf(stderr, "\t%s%s\n", cmdtab[i].name, _(cmdtab[i].usage));
    exit(1);
}


/* ----------------------------------------------- */

#define SECS_PER_DAY (24*60*60)
time_t today;

char *
seqdatestr(
    int		seq)
{
    static char str[16];
    static char *dow[7] = {
			T_("Sun"),
			T_("Mon"),
			T_("Tue"),
			T_("Wed"),
			T_("Thu"),
			T_("Fri"),
			T_("Sat")
		};
    time_t t = today + seq*SECS_PER_DAY;
    struct tm *tm;

    tm = localtime(&t);

    if (tm)
	g_snprintf(str, SIZEOF(str),
		 "%2d/%02d %3s", tm->tm_mon+1, tm->tm_mday, _(dow[tm->tm_wday]));
    else
	strcpy(str, _("BAD DATE"));

    return str;
}

#undef days_diff
#define days_diff(a, b)        (int)(((b) - (a) + SECS_PER_DAY) / SECS_PER_DAY)

/* when is next level 0 due? 0 = tonight, 1 = tommorrow, etc*/
static int
next_level0(
    disk_t *	dp,
    info_t *	info)
{
    if(dp->strategy == DS_NOFULL)
	return 1;	/* fake it */
    if(info->inf[0].date < (time_t)0)
	return 0;	/* new disk */
    else
	return dp->dumpcycle - days_diff(info->inf[0].date, today);
}

/* ----------------------------------------------- */

void
diskloop(
    int		argc,
    char **	argv,
    char *	cmdname,
    void	(*func)(disk_t *dp))
{
    disk_t *dp;
    int count = 0;
    char *errstr;

    if(argc < 4) {
	g_fprintf(stderr,_("%s: expecting \"%s [<hostname> [<disks>]* ]+\"\n"),
		get_pname(), cmdname);
	usage();
    }

    errstr = match_disklist(&diskq, argc-3, argv+3);
    if (errstr) {
	g_printf("%s", errstr);
	amfree(errstr);
    }

    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	if(dp->todo) {
	    count++;
	    func(dp);
	}
    }
    if(count==0) {
	g_fprintf(stderr,_("%s: no disk matched\n"),get_pname());
    }
}

/* ----------------------------------------------- */


static void
estimate_one(
    disk_t *	dp)
{
    char   *hostname = dp->host->hostname;
    char   *diskname = dp->name;
    char   *qhost = quote_string(hostname);
    char   *qdisk = quote_string(diskname);
    info_t  info;
    int     stats;
    gint64  size;

    get_info(hostname, diskname, &info);

    size = internal_server_estimate(dp, &info, 0, &stats);
    if (stats) {
	printf("%s %s %d %jd\n", qhost, qdisk, 0, (intmax_t)size);
    }

    if (info.last_level > 0) {
	size = internal_server_estimate(dp, &info, info.last_level, &stats);
	if (stats) {
	    printf("%s %s %d %jd\n", qhost, qdisk, info.last_level,
		   (intmax_t)size);
	}
    }

    if (info.last_level > -1) {
	size = internal_server_estimate(dp, &info, info.last_level+1, &stats);
	if (stats) {
	    printf("%s %s %d %jd\n", qhost, qdisk, info.last_level+1,
		   (intmax_t)size);
	}
    }

    amfree(qhost);
    amfree(qdisk);
}


static void
estimate(
    int		argc,
    char **	argv)
{
    disk_t *dp;

    if(argc >= 4)
	diskloop(argc, argv, "estimate", estimate_one);
    else
	for(dp = diskq.head; dp != NULL; dp = dp->next)
	    estimate_one(dp);
}


/* ----------------------------------------------- */


void
force_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

    get_info(hostname, diskname, &info);
    SET(info.command, FORCE_FULL);
    if (ISSET(info.command, FORCE_BUMP)) {
	CLR(info.command, FORCE_BUMP);
	g_printf(_("%s: WARNING: %s:%s FORCE_BUMP command was cleared.\n"),
	       get_pname(), hostname, diskname);
    }
    if(put_info(hostname, diskname, &info) == 0) {
	if (dp->strategy == DS_INCRONLY) {
	    g_printf(_("%s: %s:%s, full dump done offline, next dump will be at level 1.\n"),
		     get_pname(), hostname, diskname);
	} else {
	    g_printf(_("%s: %s:%s is set to a forced level 0 at next run.\n"),
		     get_pname(), hostname, diskname);
	}
    } else {
	g_fprintf(stderr, _("%s: %s:%s could not be forced.\n"),
		get_pname(), hostname, diskname);
    }
}


void
force(
    int		argc,
    char **	argv)
{
    diskloop(argc, argv, "force", force_one);
}


/* ----------------------------------------------- */


void
unforce_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

    get_info(hostname, diskname, &info);
    if (ISSET(info.command, FORCE_FULL)) {
	CLR(info.command, FORCE_FULL);
	if(put_info(hostname, diskname, &info) == 0){
	    g_printf(_("%s: force command for %s:%s cleared.\n"),
		   get_pname(), hostname, diskname);
	} else {
	    g_fprintf(stderr,
		    _("%s: force command for %s:%s could not be cleared.\n"),
		    get_pname(), hostname, diskname);
	}
    }
    else {
	g_printf(_("%s: no force command outstanding for %s:%s, unchanged.\n"),
	       get_pname(), hostname, diskname);
    }
}

void
unforce(
    int		argc,
    char **	argv)
{
    diskloop(argc, argv, "unforce", unforce_one);
}


/* ----------------------------------------------- */


void
force_bump_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

    get_info(hostname, diskname, &info);
    SET(info.command, FORCE_BUMP);
    if (ISSET(info.command, FORCE_NO_BUMP)) {
	CLR(info.command, FORCE_NO_BUMP);
	g_printf(_("%s: WARNING: %s:%s FORCE_NO_BUMP command was cleared.\n"),
	       get_pname(), hostname, diskname);
    }
    if (ISSET(info.command, FORCE_FULL)) {
	CLR(info.command, FORCE_FULL);
	g_printf(_("%s: WARNING: %s:%s FORCE_FULL command was cleared.\n"),
	       get_pname(), hostname, diskname);
    }
    if(put_info(hostname, diskname, &info) == 0) {
	g_printf(_("%s: %s:%s is set to bump at next run.\n"),
	       get_pname(), hostname, diskname);
    } else {
	g_fprintf(stderr, _("%s: %s:%s could not be forced to bump.\n"),
		get_pname(), hostname, diskname);
    }
}


void
force_bump(
    int		argc,
    char **	argv)
{
    diskloop(argc, argv, "force-bump", force_bump_one);
}


/* ----------------------------------------------- */


void
force_no_bump_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

    get_info(hostname, diskname, &info);
    SET(info.command, FORCE_NO_BUMP);
    if (ISSET(info.command, FORCE_BUMP)) {
	CLR(info.command, FORCE_BUMP);
	g_printf(_("%s: WARNING: %s:%s FORCE_BUMP command was cleared.\n"),
	       get_pname(), hostname, diskname);
    }
    if(put_info(hostname, diskname, &info) == 0) {
	g_printf(_("%s: %s:%s is set to not bump at next run.\n"),
	       get_pname(), hostname, diskname);
    } else {
	g_fprintf(stderr, _("%s: %s:%s could not be force to not bump.\n"),
		get_pname(), hostname, diskname);
    }
}


void
force_no_bump(
    int		argc,
    char **	argv)
{
    diskloop(argc, argv, "force-no-bump", force_no_bump_one);
}


/* ----------------------------------------------- */


void
unforce_bump_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

    get_info(hostname, diskname, &info);
    if (ISSET(info.command, FORCE_BUMP|FORCE_NO_BUMP)) {
	CLR(info.command, FORCE_BUMP|FORCE_NO_BUMP);
	if(put_info(hostname, diskname, &info) == 0) {
	    g_printf(_("%s: bump command for %s:%s cleared.\n"),
		   get_pname(), hostname, diskname);
	} else {
	    g_fprintf(stderr, _("%s: %s:%s bump command could not be cleared.\n"),
		    get_pname(), hostname, diskname);
	}
    }
    else {
	g_printf(_("%s: no bump command outstanding for %s:%s, unchanged.\n"),
	       get_pname(), hostname, diskname);
    }
}


void
unforce_bump(
    int		argc,
    char **	argv)
{
    diskloop(argc, argv, "unforce-bump", unforce_bump_one);
}


/* ----------------------------------------------- */

void
reuse(
    int		argc,
    char **	argv)
{
    tape_t *tp;
    int count;

    if(argc < 4) {
	g_fprintf(stderr,_("%s: expecting \"reuse <tapelabel> ...\"\n"),
		get_pname());
	usage();
    }

    for(count=3; count< argc; count++) {
	tp = lookup_tapelabel(argv[count]);
	if ( tp == NULL) {
	    g_fprintf(stderr, _("reuse: tape label %s not found in tapelist.\n"),
		argv[count]);
	    continue;
	}
	if( tp->reuse == 0 ) {
	    tp->reuse = 1;
	    g_printf(_("%s: marking tape %s as reusable.\n"),
		   get_pname(), argv[count]);
	} else {
	    g_fprintf(stderr, _("%s: tape %s already reusable.\n"),
		    get_pname(), argv[count]);
	}
    }

    if(write_tapelist(conf_tapelist)) {
	error(_("could not write tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
}

void
noreuse(
    int		argc,
    char **	argv)
{
    tape_t *tp;
    int count;

    if(argc < 4) {
	g_fprintf(stderr,_("%s: expecting \"no-reuse <tapelabel> ...\"\n"),
		get_pname());
	usage();
    }

    for(count=3; count< argc; count++) {
	tp = lookup_tapelabel(argv[count]);
	if ( tp == NULL) {
	    g_fprintf(stderr, _("no-reuse: tape label %s not found in tapelist.\n"),
		argv[count]);
	    continue;
	}
	if( tp->reuse == 1 ) {
	    tp->reuse = 0;
	    g_printf(_("%s: marking tape %s as not reusable.\n"),
		   get_pname(), argv[count]);
	} else {
	    g_fprintf(stderr, _("%s: tape %s already not reusable.\n"),
		    get_pname(), argv[count]);
	}
    }

    if(write_tapelist(conf_tapelist)) {
	error(_("could not write tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
}


/* ----------------------------------------------- */

static int deleted;

void
delete_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

    if(get_info(hostname, diskname, &info)) {
	g_printf(_("%s: %s:%s NOT currently in database.\n"),
	       get_pname(), hostname, diskname);
	return;
    }

    deleted++;
    if(del_info(hostname, diskname)) {
	error(_("couldn't delete %s:%s from database: %s"),
	      hostname, diskname, strerror(errno));
        /*NOTREACHED*/
    } else {
	g_printf(_("%s: %s:%s deleted from curinfo database.\n"),
	       get_pname(), hostname, diskname);
    }
}

void
delete(
    int		argc,
    char **	argv)
{
    deleted = 0;
    diskloop(argc, argv, "delete", delete_one);

   if(deleted)
	g_printf(
	 _("%s: NOTE: you'll have to remove these from the disklist yourself.\n"),
	 get_pname());
}

/* ----------------------------------------------- */

void
info_one(
    disk_t *	dp)
{
    info_t info;
    int lev;
    struct tm *tm;
    stats_t *sp;

    get_info(dp->host->hostname, dp->name, &info);

    g_printf(_("\nCurrent info for %s %s:\n"), dp->host->hostname, dp->name);
    if (ISSET(info.command, FORCE_FULL))
	g_printf(_("  (Forcing to level 0 dump at next run)\n"));
    if (ISSET(info.command, FORCE_BUMP))
	g_printf(_("  (Forcing bump at next run)\n"));
    if (ISSET(info.command, FORCE_NO_BUMP))
	g_printf(_("  (Forcing no-bump at next run)\n"));
    g_printf(_("  Stats: dump rates (kps), Full:  %5.1lf, %5.1lf, %5.1lf\n"),
	   info.full.rate[0], info.full.rate[1], info.full.rate[2]);
    g_printf(_("                    Incremental:  %5.1lf, %5.1lf, %5.1lf\n"),
	   info.incr.rate[0], info.incr.rate[1], info.incr.rate[2]);
    g_printf(_("          compressed size, Full: %5.1lf%%,%5.1lf%%,%5.1lf%%\n"),
	   info.full.comp[0]*100, info.full.comp[1]*100, info.full.comp[2]*100);
    g_printf(_("                    Incremental: %5.1lf%%,%5.1lf%%,%5.1lf%%\n"),
	   info.incr.comp[0]*100, info.incr.comp[1]*100, info.incr.comp[2]*100);

    g_printf(_("  Dumps: lev datestmp  tape             file   origK   compK secs\n"));
    for(lev = 0, sp = &info.inf[0]; lev < 9; lev++, sp++) {
	if(sp->date < (time_t)0 && sp->label[0] == '\0') continue;
	tm = localtime(&sp->date);
	if (tm) {
	    g_printf(_("          %d  %04d%02d%02d  %-15s  %lld %lld %lld %jd\n"),
		   lev, tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
		   sp->label,
		   (long long)sp->filenum,
		   (long long)sp->size,
		   (long long)sp->csize,
		   (intmax_t)sp->secs);
	} else {
	    g_printf(_("          %d  BAD-DATE  %-15s  %lld %lld %lld %jd\n"),
		   lev,
		   sp->label,
		   (long long)sp->filenum,
		   (long long)sp->size,
		   (long long)sp->csize,
		   (intmax_t)sp->secs);
	}
    }
}


void
info(
    int		argc,
    char **	argv)
{
    disk_t *dp;

    if(argc >= 4)
	diskloop(argc, argv, "info", info_one);
    else
	for(dp = diskq.head; dp != NULL; dp = dp->next)
	    info_one(dp);
}

/* ----------------------------------------------- */

void
due_one(
    disk_t *	dp)
{
    am_host_t *hp;
    int days;
    info_t info;

    hp = dp->host;
    if(get_info(hp->hostname, dp->name, &info)) {
	g_printf(_("new disk %s:%s ignored.\n"), hp->hostname, dp->name);
    }
    else {
	days = next_level0(dp, &info);
	if(days < 0) {
	    g_printf(_("Overdue %2d day%s %s:%s\n"),
		   -days, (-days == 1) ? ": " : "s:",
		   hp->hostname, dp->name);
	}
	else if(days == 0) {
	    g_printf(_("Due today: %s:%s\n"), hp->hostname, dp->name);
	}
	else {
	    g_printf(_("Due in %2d day%s %s:%s\n"), days,
		   (days == 1) ? ": " : "s:",
		   hp->hostname, dp->name);
	}
    }
}

void
due(
    int		argc,
    char **	argv)
{
    disk_t *dp;

    time(&today);
    if(argc >= 4)
	diskloop(argc, argv, "due", due_one);
    else
	for(dp = diskq.head; dp != NULL; dp = dp->next)
	    due_one(dp);
}

/* ----------------------------------------------- */

void
tape(
    int		argc,
    char **	argv)
{
    int     nb_days = 1;
    int     runtapes;
    tape_t *tp;
    int     i, j;
    int     skip;
    int     nb_new_tape;

    if(argc > 4 && strcmp(argv[3],"--days") == 0) {
	nb_days = atoi(argv[4]);
	if(nb_days < 1) {
	    g_printf(_("days must be an integer bigger than 0\n"));
	    return;
	}
	if (nb_days > 10000)
	    nb_days = 10000;
    }

    runtapes = getconf_int(CNF_RUNTAPES);
    tp = lookup_last_reusable_tape(0);
    skip = 0;

    for ( j=0 ; j < nb_days ; j++ ) {
	nb_new_tape=0;
	for ( i=0 ; i < runtapes ; i++ ) {
	    if(i==0)
		g_fprintf(stdout, _("The next Amanda run should go onto "));
	    if(tp != NULL) {
		if (nb_new_tape > 0) {
		    if (nb_new_tape == 1)
			g_fprintf(stdout, _("1 new tape.\n"));
		    else
			g_fprintf(stdout, _("%d new tapes.\n"), nb_new_tape);
		    g_fprintf(stdout, "                                   ");
		    nb_new_tape = 0;
		}
		g_fprintf(stdout, _("tape %s or a new tape.\n"), tp->label);
		if (i < runtapes-1)
		    g_fprintf(stdout, "                                   ");
	    } else {
		nb_new_tape++;
	    }
	    skip++;

	    tp = lookup_last_reusable_tape(skip);
	}
	if (nb_new_tape > 0) {
	    if (nb_new_tape == 1)
		g_fprintf(stdout, _("1 new tape.\n"));
	    else
		g_fprintf(stdout, _("%d new tapes.\n"), nb_new_tape);
	}
    }

    print_new_tapes(stdout, nb_days * runtapes);
}

/* ----------------------------------------------- */

void
balance(
    int		argc G_GNUC_UNUSED,
    char **	argv G_GNUC_UNUSED)
{
    disk_t *dp;
    struct balance_stats {
	int disks;
	off_t origsize, outsize;
    } *sp;
    int conf_runspercycle, conf_dumpcycle;
    int seq, runs_per_cycle, overdue, max_overdue;
    int later, total, balance, distinct;
    double fseq, disk_dumpcycle;
    info_t info;
    off_t total_balanced, balanced;
    int empty_day;

    time(&today);
    conf_dumpcycle = getconf_int(CNF_DUMPCYCLE);
    conf_runspercycle = getconf_int(CNF_RUNSPERCYCLE);
    later = conf_dumpcycle;
    overdue = 0;
    max_overdue = 0;

    if (opt_days > 0) {
	later = opt_days;
    } else if (opt_days == 0) {
	later = conf_dumpcycle;
    }
    if(later > 10000) later = 10000;

    if(conf_runspercycle == 0) {
	runs_per_cycle = conf_dumpcycle;
    } else if(conf_runspercycle == -1 ) {
	runs_per_cycle = guess_runs_from_tapelist();
    } else
	runs_per_cycle = conf_runspercycle;

    if (runs_per_cycle <= 0) {
	runs_per_cycle = 1;
    }

    total = later + 1;
    balance = later + 2;
    distinct = later + 3;

    sp = (struct balance_stats *)
	alloc(SIZEOF(struct balance_stats) * (distinct+1));

    for(seq=0; seq <= distinct; seq++) {
	sp[seq].disks = 0;
	sp[seq].origsize = sp[seq].outsize = (off_t)0;
    }

    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	if(get_info(dp->host->hostname, dp->name, &info)) {
	    g_printf(_("new disk %s:%s ignored.\n"), dp->host->hostname, dp->name);
	    continue;
	}
	if (dp->strategy == DS_NOFULL) {
	    continue;
	}
	sp[distinct].disks++;
	sp[distinct].origsize += info.inf[0].size/(off_t)unitdivisor;
	sp[distinct].outsize += info.inf[0].csize/(off_t)unitdivisor;

	sp[balance].disks++;
	if(dp->dumpcycle == 0) {
	    sp[balance].origsize += (info.inf[0].size/(off_t)unitdivisor) * (off_t)runs_per_cycle;
	    sp[balance].outsize += (info.inf[0].csize/(off_t)unitdivisor) * (off_t)runs_per_cycle;
	}
	else {
	    sp[balance].origsize += (info.inf[0].size/(off_t)unitdivisor) *
				    (off_t)(conf_dumpcycle / dp->dumpcycle);
	    sp[balance].outsize += (info.inf[0].csize/(off_t)unitdivisor) *
				   (off_t)(conf_dumpcycle / dp->dumpcycle);
	}

	disk_dumpcycle = (double)dp->dumpcycle;
	if(dp->dumpcycle <= 0)
	    disk_dumpcycle = ((double)conf_dumpcycle) / ((double)runs_per_cycle);

	seq = next_level0(dp, &info);
	fseq = seq + 0.0001;
	do {
	    if(seq < 0) {
		overdue++;
		if (-seq > max_overdue)
		    max_overdue = -seq;
		seq = 0;
		fseq = seq + 0.0001;
	    }
	    if(seq > later) {
	       	seq = later;
	    }
	    
	    sp[seq].disks++;
	    sp[seq].origsize += info.inf[0].size/(off_t)unitdivisor;
	    sp[seq].outsize += info.inf[0].csize/(off_t)unitdivisor;

	    if(seq < later) {
		sp[total].disks++;
		sp[total].origsize += info.inf[0].size/(off_t)unitdivisor;
		sp[total].outsize += info.inf[0].csize/(off_t)unitdivisor;
	    }
	    
	    /* See, if there's another run in this dumpcycle */
	    fseq += disk_dumpcycle;
	    seq = (int)fseq;
	} while (seq < later);
    }

    if(sp[total].outsize == (off_t)0 && sp[later].outsize == (off_t)0) {
	g_printf(_("\nNo data to report on yet.\n"));
	amfree(sp);
	return;
    }

    balanced = sp[balance].outsize / (off_t)runs_per_cycle;
    if(conf_dumpcycle == later) {
	total_balanced = sp[total].outsize / (off_t)runs_per_cycle;
    }
    else {
	total_balanced = (((sp[total].outsize/(off_t)1024) * (off_t)conf_dumpcycle)
			    / (off_t)(runs_per_cycle * later)) * (off_t)1024;
    }

    empty_day = 0;
    g_printf(_("\n due-date  #fs    orig %cB     out %cB   balance\n"),
	   displayunit[0], displayunit[0]);
    g_printf("----------------------------------------------\n");
    for(seq = 0; seq < later; seq++) {
	if(sp[seq].disks == 0 &&
	   ((seq > 0 && sp[seq-1].disks == 0) ||
	    ((seq < later-1) && sp[seq+1].disks == 0))) {
	    empty_day++;
	}
	else {
	    if(empty_day > 0) {
		g_printf("\n");
		empty_day = 0;
	    }
	    g_printf(_("%-9.9s  %3d %10lld %10lld "),
		   seqdatestr(seq), sp[seq].disks,
		   (long long)sp[seq].origsize,
		   (long long)sp[seq].outsize);
	    if(!sp[seq].outsize) g_printf("     --- \n");
	    else g_printf(_("%+8.1lf%%\n"),
			(((double)sp[seq].outsize - (double)balanced) * 100.0 /
			(double)balanced));
	}
    }

    if(sp[later].disks != 0) {
	g_printf(_("later      %3d %10lld %10lld "),
	       sp[later].disks,
	       (long long)sp[later].origsize,
	       (long long)sp[later].outsize);
	if(!sp[later].outsize) g_printf("     --- \n");
	else g_printf(_("%+8.1lf%%\n"),
		    (((double)sp[later].outsize - (double)balanced) * 100.0 /
		    (double)balanced));
    }
    g_printf("----------------------------------------------\n");
    g_printf(_("TOTAL      %3d %10lld %10lld %9lld\n"),
	   sp[total].disks,
	   (long long)sp[total].origsize,
	   (long long)sp[total].outsize,
	   (long long)total_balanced);
    if (sp[balance].origsize != sp[total].origsize ||
        sp[balance].outsize != sp[total].outsize ||
	balanced != total_balanced) {
	g_printf(_("BALANCED       %10lld %10lld %9lld\n"),
	       (long long)sp[balance].origsize,
	       (long long)sp[balance].outsize,
	       (long long)balanced);
    }
    if (sp[distinct].disks != sp[total].disks) {
	g_printf(_("DISTINCT   %3d %10lld %10lld\n"),
	       sp[distinct].disks,
	       (long long)sp[distinct].origsize,
	       (long long)sp[distinct].outsize);
    }
    g_printf(plural(_("  (estimated %d run per dumpcycle)\n"),
		  _("  (estimated %d runs per dumpcycle)\n"),
		  runs_per_cycle),
	   runs_per_cycle);
    if (overdue) {
	g_printf(plural(_(" (%d filesystem overdue."),
		      _(" (%d filesystems overdue."), overdue),
	       overdue);
	g_printf(plural(_(" The most being overdue %d day.)\n"),
	              _(" The most being overdue %d days.)\n"), max_overdue),
	       max_overdue);
    }
    amfree(sp);
}


/* ----------------------------------------------- */

void
find(
    int		argc,
    char **	argv)
{
    int start_argc;
    char *sort_order = NULL;
    find_result_t *output_find;
    char *errstr;
    char **output_find_log;
    char **name;

    if(argc < 3) {
	g_fprintf(stderr,
		_("%s: expecting \"find [--sort <hkdlpbfw>] [hostname [<disk>]]*\"\n"),
		get_pname());
	usage();
    }


    sort_order = newstralloc(sort_order, DEFAULT_SORT_ORDER);
    if (opt_sort) {
	size_t i, valid_sort=1;

	for(i = strlen(opt_sort); i > 0; i--) {
	    switch (opt_sort[i - 1]) {
	    case 'h':
	    case 'H':
	    case 'k':
	    case 'K':
	    case 'd':
	    case 'D':
	    case 'f':
	    case 'F':
	    case 'l':
	    case 'L':
	    case 'p':
	    case 'P':
	    case 'b':
	    case 'B':
	    case 'w':
	    case 'W':
		    break;
	    default: valid_sort=0;
	    }
	}
	if(valid_sort) {
	    sort_order = newstralloc(sort_order, opt_sort);
	} else {
	    g_printf(_("Invalid sort order: %s\n"), opt_sort);
	    g_printf(_("Use default sort order: %s\n"), sort_order);
	}
    }
    start_argc=4;
    errstr = match_disklist(&diskq, argc-(start_argc-1), argv+(start_argc-1));

    /* check all log file exists */
    output_find_log = find_log();
    for (name = output_find_log; *name != NULL; name++) {
        amfree(*name);
    }
    amfree(output_find_log);

    output_find = find_dump(&diskq); /* Add deleted dump to diskq */
    if(argc-(start_argc-1) > 0) {
	find_result_t *afind = NULL;
	find_result_t *afind_next = NULL;
	find_result_t *new_output_find = NULL;
	disk_t *dp;

	amfree(errstr);
	errstr = match_disklist(&diskq, argc-(start_argc-1),
					argv+(start_argc-1));
	if (errstr) {
	    g_printf("%s", errstr);
	    amfree(errstr);
	}
	for (afind = output_find; afind; afind = afind_next) {
	    afind_next = afind->next;
	    dp = lookup_disk(afind->hostname, afind->diskname);
	    if (dp->todo) {
		afind->next = new_output_find;
		new_output_find = afind;
	    } else {
		amfree(afind);
	    }
	}
	output_find = new_output_find;
    } else if (errstr) {
	g_printf("%s", errstr);
	amfree(errstr);
    }

    sort_find_result(sort_order, &output_find);
    print_find_result(output_find);
    free_find_result(&output_find);

    amfree(sort_order);
}


/* ------------------------ */

static GSList *
get_file_list(
    int argc,
    char **argv,
    int allow_empty)
{
    GSList * file_list = NULL;
    GSList * dumplist;
    int flags;

    flags = CMDLINE_PARSE_DATESTAMP;
    if (allow_empty) flags |= CMDLINE_EMPTY_TO_WILDCARD;
    dumplist = cmdline_parse_dumpspecs(argc, argv, flags);

    file_list = cmdline_match_holding(dumplist);
    dumpspec_list_free(dumplist);

    return file_list;
}

/* Given a file header, find the history element in curinfo most likely
 * corresponding to that dump (this is not an exact science).
 *
 * @param info: the info_t element for this DLE
 * @param file: the header of the file
 * @returns: index of the matching history element, or -1 if not found
 */
static int
holding_file_find_history(
    info_t *info,
    dumpfile_t *file)
{
    int matching_hist_idx = -1;
    int nhist;
    int i;

    /* Begin by trying to find the history element matching this dump.
     * The datestamp on the dump is for the entire run of amdump, while the
     * 'date' in the history element of 'info' is the time the dump itself
     * began.  A matching history element, then, is the earliest element
     * with a 'date' equal to or later than the date of the dumpfile.
     *
     * We compare using formatted datestamps; even using seconds since epoch,
     * we would still face timezone issues, and have to do a reverse (timezone
     * to gmt) translation.
     */

    /* get to the end of the history list and search backward */
    for (nhist = 0; info->history[nhist].level > -1; nhist++) /* empty loop */;
    for (i = nhist-1; i > -1; i--) {
        char *info_datestamp = get_timestamp_from_time(info->history[i].date);
        int order = strcmp(file->datestamp, info_datestamp);
        amfree(info_datestamp);

        if (order <= 0) {
            /* only a match if the levels are equal */
            if (info->history[i].level == file->dumplevel) {
                matching_hist_idx = i;
            }
            break;
        }
    }

    return matching_hist_idx;
}

/* A holding file is 'outdated' if a subsequent dump of the same DLE was made
 * at the same level or a lower leve; for example, a level 2 dump is outdated if
 * there is a subsequent level 2, or a subsequent level 0.
 *
 * @param file: the header of the file
 * @returns: true if the file is outdated
 */
static int
holding_file_is_outdated(
    dumpfile_t *file)
{
    info_t info;
    int matching_hist_idx;

    if (get_info(file->name, file->disk, &info) == -1) {
	return 0; /* assume it's not outdated */
    }

    /* if the last level is less than the level of this dump, then
     * it's outdated */
    if (info.last_level < file->dumplevel)
	return 1;

    /* otherwise, we need to see if this dump is the last at its level */
    matching_hist_idx = holding_file_find_history(&info, file);
    if (matching_hist_idx == -1) {
        return 0; /* assume it's not outdated */
    }

    /* compare the date of the history element with the most recent date
     * for this level.  If they match, then this is the last dump at this
     * level, and we checked above for more recent lower-level dumps, so
     * the dump is not outdated. */
    if (info.history[matching_hist_idx].date == 
	info.inf[info.history[matching_hist_idx].level].date) {
	return 0;
    } else {
	return 1;
    }
}

static int
remove_holding_file_from_catalog(
    char *filename)
{
    static int warnings_printed; /* only print once per invocation */
    dumpfile_t file;
    info_t info;
    int matching_hist_idx = -1;
    history_t matching_hist; /* will be a copy */
    int i;

    if (!holding_file_get_dumpfile(filename, &file)) {
        g_printf(_("Could not read holding file %s\n"), filename);
        return 0;
    }

    if (get_info(file.name, file.disk, &info) == -1) {
	g_printf(_("WARNING: No curinfo record for %s:%s\n"), file.name, file.disk);
	dumpfile_free_data(&file);
	return 1; /* not an error */
    }

    matching_hist_idx = holding_file_find_history(&info, &file);

    if (matching_hist_idx == -1) {
        g_printf(_("WARNING: No dump matching %s found in curinfo.\n"), filename);
	dumpfile_free_data(&file);
	return 1; /* not an error */
    }

    /* make a copy */
    matching_hist = info.history[matching_hist_idx];

    /* Remove the history element itself before doing the stats */
    for (i = matching_hist_idx; i < NB_HISTORY; i++) {
        info.history[i] = info.history[i+1];
    }
    info.history[NB_HISTORY].level = -1;

    /* Remove stats for that history element, if necessary.  Doing so
     * will result in an inconsistent set of backups, so we warn the
     * user and adjust last_level to make the next dump get us a 
     * consistent picture. */
    if (matching_hist.date == info.inf[matching_hist.level].date) {
        /* search for an earlier dump at this level */
        for (i = matching_hist_idx; info.history[i].level > -1; i++) {
            if (info.history[i].level == matching_hist.level)
                break;
        }

        if (info.history[i].level < 0) {
            /* not found => zero it out */
            info.inf[matching_hist.level].date = (time_t)-1; /* flag as not set */
            info.inf[matching_hist.level].label[0] = '\0';
        } else {
            /* found => reconstruct stats as best we can */
            info.inf[matching_hist.level].size = info.history[i].size;
            info.inf[matching_hist.level].csize = info.history[i].csize;
            info.inf[matching_hist.level].secs = info.history[i].secs;
            info.inf[matching_hist.level].date = info.history[i].date;
            info.inf[matching_hist.level].filenum = 0; /* we don't know */
            info.inf[matching_hist.level].label[0] = '\0'; /* we don't know */
        }

        /* set last_level to the level we just deleted, and set command
         * appropriately to make sure planner does a new dump at this level
         * or lower */
        info.last_level = matching_hist.level;
        if (info.last_level == 0) {
            g_printf(_("WARNING: Deleting the most recent full dump; forcing a full dump at next run.\n"));
            SET(info.command, FORCE_FULL);
        } else {
            g_printf(_("WARNING: Deleting the most recent level %d dump; forcing a level %d dump or \nWARNING: lower at next run.\n"),
                info.last_level, info.last_level);
            SET(info.command, FORCE_NO_BUMP);
        }

        /* Search for and display any subsequent runs that depended on this one */
        warnings_printed = 0;
        for (i = matching_hist_idx-1; i >= 0; i--) {
            char *datestamp;
            if (info.history[i].level <= matching_hist.level) break;

            datestamp = get_timestamp_from_time(info.history[i].date);
            g_printf(_("WARNING: Level %d dump made %s can no longer be accurately restored.\n"), 
                info.history[i].level, datestamp);
            amfree(datestamp);

            warnings_printed = 1;
        }
        if (warnings_printed)
            g_printf(_("WARNING: (note, dates shown above are for dumps, and may be later than the\nWARNING: corresponding run date)\n"));
    }

    /* recalculate consecutive_runs based on the history: find the first run
     * at this level, and then count the consecutive runs at that level. This
     * number may be zero (if we just deleted the last run at this level) */
    info.consecutive_runs = 0;
    for (i = 0; info.history[i].level >= 0; i++) {
        if (info.history[i].level == info.last_level) break;
    }
    while (info.history[i+info.consecutive_runs].level == info.last_level)
        info.consecutive_runs++;

    /* this function doesn't touch the performance stats */

    /* write out the changes */
    if (put_info(file.name, file.disk, &info) == -1) {
	g_printf(_("Could not write curinfo record for %s:%s\n"), file.name, file.disk);
	dumpfile_free_data(&file);
	return 0;
    }

    dumpfile_free_data(&file);
    return 1;
}

void
holding(
    int		argc,
    char **	argv)
{
    GSList *file_list;
    GSList *li;
    enum { HOLDING_USAGE, HOLDING_LIST, HOLDING_DELETE } action = HOLDING_USAGE;
    int long_list = 0;
    int outdated_list = 0;
    dumpfile_t file;

    if (argc < 4)
        action = HOLDING_USAGE;
    else if (strcmp(argv[3], "list") == 0 && argc >= 4)
        action = HOLDING_LIST;
    else if (strcmp(argv[3], "delete") == 0 && argc > 4)
        action = HOLDING_DELETE;

    switch (action) {
        case HOLDING_USAGE:
            g_fprintf(stderr,
                    _("%s: expecting \"holding list [-l] [-d]\" or \"holding delete <host> [ .. ]\"\n"),
                    get_pname());
            usage();
            return;

        case HOLDING_LIST:
	    long_list = opt_long;
	    outdated_list = opt_outdated;
            argc -= 4; argv += 4;

	    /* header */
            if (long_list) {
                g_printf("%-10s %-2s %-4s %s\n", 
		    _("size (kB)"), _("lv"), _("outd"), _("dump specification"));
            }

            file_list = get_file_list(argc, argv, 1);
            for (li = file_list; li != NULL; li = li->next) {
                char *dumpstr;
		int is_outdated;

                if (!holding_file_get_dumpfile((char *)li->data, &file)) {
                    g_fprintf(stderr, _("Error reading %s\n"), (char *)li->data);
                    continue;
                }

	        is_outdated = holding_file_is_outdated(&file);

                dumpstr = cmdline_format_dumpspec_components(file.name, file.disk, file.datestamp, NULL);
		/* only print this entry if we're printing everything, or if it's outdated and
		 * we're only printing outdated files (-o) */
		if (!outdated_list || is_outdated) {
		    if (long_list) {
			g_printf("%-10lld %-2d %-4s %s\n", 
			       (long long)holding_file_size((char *)li->data, 0),
			       file.dumplevel,
			       is_outdated? " *":"",
			       dumpstr);
		    } else {
			g_printf("%s\n", dumpstr);
		    }
		}
                amfree(dumpstr);
		dumpfile_free_data(&file);
            }
            slist_free_full(file_list, g_free);
            break;

        case HOLDING_DELETE:
            argc -= 4; argv += 4;

            file_list = get_file_list(argc, argv, 0);
            for (li = file_list; li != NULL; li = li->next) {
                g_fprintf(stderr, _("Deleting '%s'\n"), (char *)li->data);
                /* remove it from the catalog */
                if (!remove_holding_file_from_catalog((char *)li->data))
                    exit(1);

                /* unlink it */
                if (!holding_file_unlink((char *)li->data)) {
                    error(_("Could not delete '%s'"), (char *)li->data);
                }
            }
            slist_free_full(file_list, g_free);
            break;
    }
}


/* ------------------------ */


/* shared code with planner.c */

int
bump_thresh(
    int		level)
{
    gint64 bump = getconf_int64(CNF_BUMPSIZE);
    double mult = getconf_real(CNF_BUMPMULT);

    while(--level)
	bump = (int)((double)bump * mult);
    return bump;
}

void
bumpsize(
    int		argc,
    char **	argv)
{
    int l;
    int conf_bumppercent = getconf_int(CNF_BUMPPERCENT);
    double conf_bumpmult = getconf_real(CNF_BUMPMULT);

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    g_printf(_("Current bump parameters:\n"));
    if(conf_bumppercent == 0) {
	g_printf(_("  bumpsize %5jd KB\t- minimum savings (threshold) to bump level 1 -> 2\n"),
	       (intmax_t)getconf_int64(CNF_BUMPSIZE));
	g_printf(_("  bumpdays %5d\t- minimum days at each level\n"),
	       getconf_int(CNF_BUMPDAYS));
	g_printf(_("  bumpmult %5.5lg\t- threshold = bumpsize * bumpmult**(level-1)\n\n"),
	       conf_bumpmult);

	g_printf(_("      Bump -> To  Threshold\n"));
	for(l = 1; l < 9; l++)
	    g_printf(_("\t%d  ->  %d  %9d KB\n"), l, l+1, bump_thresh(l));
	putchar('\n');
    }
    else {
	double bumppercent = (double)conf_bumppercent;

	g_printf(_("  bumppercent %3d %%\t- minimum savings (threshold) to bump level 1 -> 2\n"),
	       conf_bumppercent);
	g_printf(_("  bumpdays %5d\t- minimum days at each level\n"),
	       getconf_int(CNF_BUMPDAYS));
	g_printf(_("  bumpmult %5.5lg\t- threshold = disk_size * bumppercent * bumpmult**(level-1)\n\n"),
	       conf_bumpmult);
	g_printf(_("      Bump -> To  Threshold\n"));
	for(l = 1; l < 9; l++) {
	    g_printf(_("\t%d  ->  %d  %7.2lf %%\n"), l, l+1, bumppercent);
	    bumppercent *= conf_bumpmult;
	    if(bumppercent >= 100.000) { bumppercent = 100.0;}
	}
	putchar('\n');
    }
}

/* ----------------------------------------------- */

void export_one(disk_t *dp);

void
export_db(
    int		argc,
    char **	argv)
{
    disk_t *dp;
    time_t curtime;
    char hostname[MAX_HOSTNAME_LENGTH+1];
    int i;

    g_printf(_("CURINFO Version %s CONF %s\n"), VERSION, getconf_str(CNF_ORG));

    curtime = time(0);
    if(gethostname(hostname, SIZEOF(hostname)-1) == -1) {
	error(_("could not determine host name: %s\n"), strerror(errno));
	/*NOTREACHED*/
    }
    hostname[SIZEOF(hostname)-1] = '\0';
    g_printf(_("# Generated by:\n#    host: %s\n#    date: %s"),
	   hostname, ctime(&curtime));

    g_printf(_("#    command:"));
    for(i = 0; i < argc; i++)
	g_printf(_(" %s"), argv[i]);

    g_printf(_("\n# This file can be merged back in with \"amadmin import\".\n"));
    g_printf(_("# Edit only with care.\n"));

    if(argc >= 4)
	diskloop(argc, argv, "export", export_one);
    else for(dp = diskq.head; dp != NULL; dp = dp->next)
	export_one(dp);
}

void
export_one(
    disk_t *	dp)
{
    info_t info;
    int i,l;
    char *qhost, *qdisk;

    if(get_info(dp->host->hostname, dp->name, &info)) {
	g_fprintf(stderr, _("Warning: no curinfo record for %s:%s\n"),
		dp->host->hostname, dp->name);
	return;
    }
    qhost = quote_string(dp->host->hostname);
    qdisk = quote_string(dp->name);
    g_printf(_("host: %s\ndisk: %s\n"), qhost, qdisk);
    g_printf(_("command: %u\n"), info.command);
    g_printf(_("last_level: %d\n"),info.last_level);
    g_printf(_("consecutive_runs: %d\n"),info.consecutive_runs);
    g_printf(_("full-rate:"));
    for(i=0;i<AVG_COUNT;i++) g_printf(_(" %lf"), info.full.rate[i]);
    g_printf(_("\nfull-comp:"));
    for(i=0;i<AVG_COUNT;i++) g_printf(_(" %lf"), info.full.comp[i]);

    g_printf(_("\nincr-rate:"));
    for(i=0;i<AVG_COUNT;i++) g_printf(_(" %lf"), info.incr.rate[i]);
    g_printf(_("\nincr-comp:"));
    for(i=0;i<AVG_COUNT;i++) g_printf(_(" %lf"), info.incr.comp[i]);
    g_printf("\n");
    for(l=0;l<DUMP_LEVELS;l++) {
	if(info.inf[l].date < (time_t)0 && info.inf[l].label[0] == '\0') continue;
	g_printf(_("stats: %d %lld %lld %jd %jd %lld %s\n"), l,
	       (long long)info.inf[l].size,
	       (long long)info.inf[l].csize,
	       (intmax_t)info.inf[l].secs,
	       (intmax_t)info.inf[l].date,
	       (long long)info.inf[l].filenum,
	       info.inf[l].label);
    }
    for(l=0;info.history[l].level > -1;l++) {
	g_printf(_("history: %d %lld %lld %jd\n"),
	       info.history[l].level,
	       (long long)info.history[l].size,
	       (long long)info.history[l].csize,
	       (intmax_t)info.history[l].date);
    }
    g_printf("//\n");
    amfree(qhost);
    amfree(qdisk);
}

/* ----------------------------------------------- */

int import_one(void);
char *impget_line(void);

void
import_db(
    int		argc,
    char **	argv)
{
    int vers_maj;
    int vers_min;
    int vers_patch;
    int newer;
    char *org;
    char *line = NULL;
    char *hdr;
    char *s;
    int rc;
    int ch;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    /* process header line */

    if((line = agets(stdin)) == NULL) {
	g_fprintf(stderr, _("%s: empty input.\n"), get_pname());
	return;
    }

    s = line;
    ch = *s++;

    hdr = "version";
    if(strncmp_const_skip(s - 1, "CURINFO Version", s, ch) != 0) {
	goto bad_header;
    }
    ch = *s++;
    skip_whitespace(s, ch);
    if(ch == '\0'
       || sscanf(s - 1, "%d.%d.%d", &vers_maj, &vers_min, &vers_patch) != 3) {
	vers_patch = -1;
	if (sscanf(s - 1, "%d.%d", &vers_maj, &vers_min) != 2) {
	    goto bad_header;
	}
    }

    skip_integer(s, ch);			/* skip over major */
    if(ch != '.') {
	goto bad_header;
    }
    ch = *s++;
    skip_integer(s, ch);			/* skip over minor */
    if (vers_patch != -1) {
	if (ch != '.') {
	    goto bad_header;
	}
	ch = *s++;
	skip_integer(s, ch);			/* skip over patch */
    } else {
	vers_patch = 0;
    }

    hdr = "comment";
    if(ch == '\0') {
	goto bad_header;
    }
    skip_non_whitespace(s, ch);
    s[-1] = '\0';

    hdr = "CONF";
    skip_whitespace(s, ch);			/* find the org keyword */
    if(ch == '\0' || strncmp_const_skip(s - 1, "CONF", s, ch) != 0) {
	goto bad_header;
    }
    ch = *s++;

    hdr = "org";
    skip_whitespace(s, ch);			/* find the org string */
    if(ch == '\0') {
	goto bad_header;
    }
    org = s - 1;

    /*@ignore@*/
    newer = (vers_maj != VERSION_MAJOR)? vers_maj > VERSION_MAJOR :
	    (vers_min != VERSION_MINOR)? vers_min > VERSION_MINOR :
					 vers_patch > VERSION_PATCH;
    if(newer)
	g_fprintf(stderr,
	     _("%s: WARNING: input is from newer Amanda version: %d.%d.%d.\n"),
		get_pname(), vers_maj, vers_min, vers_patch);
    /*@end@*/

    if(strcmp(org, getconf_str(CNF_ORG)) != 0) {
	g_fprintf(stderr, _("%s: WARNING: input is from different org: %s\n"),
		get_pname(), org);
    }

    do {
    	rc = import_one();
    } while (rc);

    amfree(line);
    return;

 bad_header:

    /*@i@*/ amfree(line);
    g_fprintf(stderr, _("%s: bad CURINFO header line in input: %s.\n"),
	    get_pname(), hdr);
    g_fprintf(stderr, _("    Was the input in \"amadmin export\" format?\n"));
    return;
}


int
import_one(void)
{
    info_t info;
    stats_t onestat;
    int rc, level;
    char *line = NULL;
    char *s, *fp;
    int ch;
    int nb_history, i;
    char *hostname = NULL;
    char *diskname = NULL;
    long long off_t_tmp;
    long long time_t_tmp;

    memset(&info, 0, SIZEOF(info_t));

    for(level = 0; level < DUMP_LEVELS; level++) {
        info.inf[level].date = (time_t)-1;
    }

    /* get host: disk: command: lines */

    hostname = diskname = NULL;

    if((line = impget_line()) == NULL) return 0;	/* nothing there */
    s = line;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0' || strncmp_const_skip(s - 1, "host:", s, ch) != 0) goto parse_err;
    skip_whitespace(s, ch);
    if(ch == '\0') goto parse_err;
    fp = s-1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    hostname = unquote_string(fp);
    s[-1] = (char)ch;

    skip_whitespace(s, ch);
    while (ch == 0) {
      amfree(line);
      if((line = impget_line()) == NULL) goto shortfile_err;
      s = line;
      ch = *s++;
      skip_whitespace(s, ch);
    }
    if(strncmp_const_skip(s - 1, "disk:", s, ch) != 0) goto parse_err;
    skip_whitespace(s, ch);
    if(ch == '\0') goto parse_err;
    fp = s-1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(fp);
    s[-1] = (char)ch;

    amfree(line);
    if((line = impget_line()) == NULL) goto shortfile_err;
    if(sscanf(line, "command: %u", &info.command) != 1) goto parse_err;

    /* get last_level and consecutive_runs */

    amfree(line);
    if((line = impget_line()) == NULL) goto shortfile_err;
    rc = sscanf(line, "last_level: %d", &info.last_level);
    if(rc == 1) {
	amfree(line);
	if((line = impget_line()) == NULL) goto shortfile_err;
	if(sscanf(line, "consecutive_runs: %d", &info.consecutive_runs) != 1) goto parse_err;
	amfree(line);
	if((line = impget_line()) == NULL) goto shortfile_err;
    }

    /* get rate: and comp: lines for full dumps */

    rc = sscanf(line, "full-rate: %lf %lf %lf",
		&info.full.rate[0], &info.full.rate[1], &info.full.rate[2]);
    if(rc != 3) goto parse_err;

    amfree(line);
    if((line = impget_line()) == NULL) goto shortfile_err;
    rc = sscanf(line, "full-comp: %lf %lf %lf",
		&info.full.comp[0], &info.full.comp[1], &info.full.comp[2]);
    if(rc != 3) goto parse_err;

    /* get rate: and comp: lines for incr dumps */

    amfree(line);
    if((line = impget_line()) == NULL) goto shortfile_err;
    rc = sscanf(line, "incr-rate: %lf %lf %lf",
		&info.incr.rate[0], &info.incr.rate[1], &info.incr.rate[2]);
    if(rc != 3) goto parse_err;

    amfree(line);
    if((line = impget_line()) == NULL) goto shortfile_err;
    rc = sscanf(line, "incr-comp: %lf %lf %lf",
		&info.incr.comp[0], &info.incr.comp[1], &info.incr.comp[2]);
    if(rc != 3) goto parse_err;

    /* get stats for dump levels */

    while(1) {
	amfree(line);
	if((line = impget_line()) == NULL) goto shortfile_err;
	if(strncmp_const(line, "//") == 0) {
	    /* end of record */
	    break;
	}
	if(strncmp_const(line, "history:") == 0) {
	    /* end of record */
	    break;
	}
	memset(&onestat, 0, SIZEOF(onestat));

	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "stats:", s, ch) != 0) {
	    goto parse_err;
	}

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    goto parse_err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%lld", &off_t_tmp) != 1) {
	    goto parse_err;
	}
	onestat.size = (off_t)off_t_tmp;
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%lld", &off_t_tmp) != 1) {
	    goto parse_err;
	}
	onestat.csize = (off_t)off_t_tmp;
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%lld", &time_t_tmp) != 1) {
	    goto parse_err;
	}
        onestat.secs = (time_t)time_t_tmp;
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%lld", &time_t_tmp) != 1) {
	    goto parse_err;
	}
	/* time_t not guarranteed to be long */
	/*@i1@*/ onestat.date = (time_t)time_t_tmp;
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch != '\0') {
	    if(sscanf(s - 1, "%lld", &off_t_tmp) != 1) {
		goto parse_err;
	    }
	    onestat.filenum = (off_t)off_t_tmp;
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		if (onestat.filenum != 0)
		    goto parse_err;
		onestat.label[0] = '\0';
	    } else {
		strncpy(onestat.label, s - 1, SIZEOF(onestat.label)-1);
		onestat.label[SIZEOF(onestat.label)-1] = '\0';
	    }
	}

	if(level < 0 || level > 9) goto parse_err;

	info.inf[level] = onestat;
    }
    nb_history = 0;
    for(i=0;i<=NB_HISTORY;i++) {
	info.history[i].level = -2;
    }
    while(1) {
	history_t onehistory;

	if(line[0] == '/' && line[1] == '/') {
	    info.history[nb_history].level = -2;
	    rc = 0;
	    break;
	}
	memset(&onehistory, 0, SIZEOF(onehistory));
	s = line;
	ch = *s++;
	if(strncmp_const_skip(line, "history:", s, ch) != 0) {
	    break;
	}

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf((s - 1), "%d", &onehistory.level) != 1) {
	    break;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf((s - 1), "%lld", &off_t_tmp) != 1) {
	    break;
	}
	onehistory.size = (off_t)off_t_tmp;
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf((s - 1), "%lld", &off_t_tmp) != 1) {
	    break;
	}
	onehistory.csize = (off_t)off_t_tmp;
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if((ch == '\0') || sscanf((s - 1), "%lld", &time_t_tmp) != 1) {
	    break;
	}
	/* time_t not guarranteed to be long */
	/*@i1@*/ onehistory.date = (time_t)time_t_tmp;
	skip_integer(s, ch);

	info.history[nb_history++] = onehistory;
	amfree(line);
	if((line = impget_line()) == NULL) goto shortfile_err;
    }
    /*@i@*/ amfree(line);

    /* got a full record, now write it out to the database */

    if(put_info(hostname, diskname, &info)) {
	g_fprintf(stderr, _("%s: error writing record for %s:%s\n"),
		get_pname(), hostname, diskname);
    }
    amfree(hostname);
    amfree(diskname);
    return 1;

 parse_err:
    /*@i@*/ amfree(line);
    amfree(hostname);
    amfree(diskname);
    g_fprintf(stderr, _("%s: parse error reading import record.\n"), get_pname());
    return 0;

 shortfile_err:
    /*@i@*/ amfree(line);
    amfree(hostname);
    amfree(diskname);
    g_fprintf(stderr, _("%s: short file reading import record.\n"), get_pname());
    return 0;
}

char *
impget_line(void)
{
    char *line;
    char *s;
    int ch;

    for(; (line = agets(stdin)) != NULL; free(line)) {
	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '#') {
	    /* ignore comment lines */
	    continue;
	} else if(ch) {
	    /* found non-blank, return line */
	    return line;
	}
	/* otherwise, a blank line, so keep going */
    }
    if(ferror(stdin)) {
	g_fprintf(stderr, _("%s: reading stdin: %s\n"),
		get_pname(), strerror(errno));
    }
    return NULL;
}

/* ----------------------------------------------- */

void
disklist_one(
    disk_t *	dp)
{
    am_host_t *hp;
    netif_t *ip;
    dumptype_t *dtype = lookup_dumptype(dp->dtype_name);

    hp = dp->host;
    ip = hp->netif;

    g_printf("line %d (%s):\n", dp->line, dp->filename);

    g_printf("    host %s:\n", hp->hostname);
    g_printf("        interface %s\n",
	   interface_name(ip->config)[0] ? interface_name(ip->config) : "default");
    g_printf("    disk %s:\n", dp->name);
    if (dp->device) g_printf("        device %s\n", dp->device);

    g_printf("        program \"%s\"\n", dp->program);
    if (dp->application)
	g_printf("        application \"%s\"\n", dp->application);

    dump_dumptype(dtype, "  ", print_default, print_source);

    g_printf("        spindle %d\n", dp->spindle);

    g_printf("\n");
}

void
disklist(
    int		argc,
    char **	argv)
{
    disk_t *dp;

    if(argc >= 4)
	diskloop(argc, argv, "disklist", disklist_one);
    else
	for(dp = diskq.head; dp != NULL; dp = dp->next)
	    disklist_one(dp);
}

/* ----------------------------------------------- */

void
hosts(
    int		argc G_GNUC_UNUSED,
    char **	argv G_GNUC_UNUSED)
{
    disk_t *dp;
    gint sentinel = 1;
    GHashTable *seen = g_hash_table_new(g_str_hash, g_str_equal);

    /* enumerate all hosts, skipping those that have been seen (since
     * there may be more than one DLE on a host */
    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	char *hostname = dp->host->hostname;
	if (g_hash_table_lookup(seen, hostname))
	    continue;
	g_printf("%s\n", hostname);
	g_hash_table_insert(seen, hostname, &sentinel);
    }
    g_hash_table_destroy(seen);
}

/* ----------------------------------------------- */

void
dles(
    int		argc G_GNUC_UNUSED,
    char **	argv G_GNUC_UNUSED)
{
    disk_t *dp;

    for(dp = diskq.head; dp != NULL; dp = dp->next)
	g_printf("%s %s\n", dp->host->hostname, dp->name);
}

/* ----------------------------------------------- */

void
show_version(
    int		argc,
    char **	argv)
{
    int i;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    for(i = 0; version_info[i] != NULL; i++)
	g_printf("%s", version_info[i]);
}


void show_config(
    int argc G_GNUC_UNUSED,
    char **argv G_GNUC_UNUSED)
{
    dump_configuration(print_default, print_source);
}

