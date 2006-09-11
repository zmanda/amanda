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
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "infofile.h"
#include "logfile.h"
#include "version.h"
#include "holding.h"
#include "find.h"

disklist_t diskq;

int main(int argc, char **argv);
void usage(void);
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
void disklist(int argc, char **argv);
void disklist_one(disk_t *dp);
void show_version(int argc, char **argv);
static void show_config(int argc, char **argv);
static void check_dumpuser(void);

static char *conf_tapelist = NULL;
static char *displayunit;
static long int unitdivisor;

static const struct {
    const char *name;
    void (*fn)(int, char **);
    const char *usage;
} cmdtab[] = {
    { "version", show_version,
	"\t\t\t\t\t# Show version info." },
    { "config", show_config,
	"\t\t\t\t\t# Show configuration." },
    { "force", force,
	" [<hostname> [<disks>]* ]+\t\t# Force level 0 at next run." },
    { "unforce", unforce,
	" [<hostname> [<disks>]* ]+\t# Clear force command." },
    { "force-bump", force_bump,
	" [<hostname> [<disks>]* ]+\t# Force bump at next run." },
    { "force-no-bump", force_no_bump,
	" [<hostname> [<disks>]* ]+\t# Force no-bump at next run." },
    { "unforce-bump", unforce_bump,
	" [<hostname> [<disks>]* ]+\t# Clear bump command." },
    { "disklist", disklist,
	" [<hostname> [<disks>]* ]*\t# Debug disklist entries." },
    { "reuse", reuse,
	" <tapelabel> ...\t\t # re-use this tape." },
    { "no-reuse", noreuse,
	" <tapelabel> ...\t # never re-use this tape." },
    { "find", find,
	" [<hostname> [<disks>]* ]*\t # Show which tapes these dumps are on." },
    { "delete", delete,
	" [<hostname> [<disks>]* ]+ # Delete from database." },
    { "info", info,
	" [<hostname> [<disks>]* ]*\t # Show current info records." },
    { "due", due,
	" [<hostname> [<disks>]* ]*\t # Show due date." },
    { "balance", balance,
	" [-days <num>]\t\t # Show nightly dump size balance." },
    { "tape", tape,
	" [-days <num>]\t\t # Show which tape is due next." },
    { "bumpsize", bumpsize,
	"\t\t\t # Show current bump thresholds." },
    { "export", export_db,
	" [<hostname> [<disks>]* ]* # Export curinfo database to stdout." },
    { "import", import_db,
	"\t\t\t\t # Import curinfo database from stdin." },
};
#define	NCMDS	(int)(sizeof(cmdtab) / sizeof(cmdtab[0]))

static char *conffile;

int
main(
    int		argc,
    char **	argv)
{
    int i;
    char *conf_diskfile;
    char *conf_infofile;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    int new_argc;
    char **new_argv;

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amadmin");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    erroutput_type = ERR_INTERACTIVE;

    parse_server_conf(argc, argv, &new_argc, &new_argv);

    if(new_argc < 3) usage();

    if(strcmp(new_argv[2],"version") == 0) {
	show_version(new_argc, new_argv);
	goto done;
    }

    config_name = new_argv[1];

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = stralloc2(config_dir, CONFFILE_NAME);

    if(read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    check_dumpuser();

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if (read_diskfile(conf_diskfile, &diskq) < 0) {
	error("could not load disklist \"%s\"", conf_diskfile);
	/*NOTREACHED*/
    }
    amfree(conf_diskfile);

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if(read_tapelist(conf_tapelist)) {
	error("could not load tapelist \"%s\"", conf_tapelist);
	/*NOTREACHED*/
    }
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

    for (i = 0; i < NCMDS; i++)
	if (strcmp(new_argv[2], cmdtab[i].name) == 0) {
	    (*cmdtab[i].fn)(new_argc, new_argv);
	    break;
	}
    if (i == NCMDS) {
	fprintf(stderr, "%s: unknown command \"%s\"\n", new_argv[0], new_argv[2]);
	usage();
    }

    free_new_argv(new_argc, new_argv);

    close_infofile();
    clear_tapelist();
    amfree(conf_tapelist);
    amfree(config_dir);

done:

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    amfree(conffile);
    free_disklist(&diskq);
    free_server_config();
    dbclose();
    return 0;
}


void
usage(void)
{
    int i;

    fprintf(stderr, "\nUsage: %s%s <conf> <command> {<args>} [-o configoption]* ...\n",
	    get_pname(), versionsuffix());
    fprintf(stderr, "    Valid <command>s are:\n");
    for (i = 0; i < NCMDS; i++)
	fprintf(stderr, "\t%s%s\n", cmdtab[i].name, cmdtab[i].usage);
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
    static char *dow[7] = {"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
    time_t t = today + seq*SECS_PER_DAY;
    struct tm *tm;

    tm = localtime(&t);

    if (tm)
	snprintf(str, SIZEOF(str),
		 "%2d/%02d %3s", tm->tm_mon+1, tm->tm_mday, dow[tm->tm_wday]);
    else
	strcpy(str, "BAD DATE");

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

static void
check_dumpuser(void)
{
    static int been_here = 0;
    uid_t uid_me;
    uid_t uid_dumpuser;
    char *dumpuser;
    struct passwd *pw;

    if (been_here) {
       return;
    }
    uid_me = getuid();
    uid_dumpuser = uid_me;
    dumpuser = getconf_str(CNF_DUMPUSER);

    if ((pw = getpwnam(dumpuser)) == NULL) {
	error("cannot look up dump user \"%s\"", dumpuser);
	/*NOTREACHED*/
    }
    uid_dumpuser = pw->pw_uid;
    if ((pw = getpwuid(uid_me)) == NULL) {
	error("cannot look up my own uid %ld", (long)uid_me);
	/*NOTREACHED*/
    }
    if (uid_me != uid_dumpuser) {
	error("ERROR: running as user \"%s\" instead of \"%s\"",
	      pw->pw_name, dumpuser);
        /*NOTREACHED*/
    }
    been_here = 1;
    return;
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
	fprintf(stderr,"%s: expecting \"%s [<hostname> [<disks>]* ]+\"\n",
		get_pname(), cmdname);
	usage();
    }

    errstr = match_disklist(&diskq, argc-3, argv+3);
    if (errstr) {
	printf("%s", errstr);
	amfree(errstr);
    }

    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	if(dp->todo) {
	    count++;
	    func(dp);
	}
    }
    if(count==0) {
	fprintf(stderr,"%s: no disk matched\n",get_pname());
    }
}

/* ----------------------------------------------- */


void
force_one(
    disk_t *	dp)
{
    char *hostname = dp->host->hostname;
    char *diskname = dp->name;
    info_t info;

#if TEXTDB
    check_dumpuser();
#endif
    get_info(hostname, diskname, &info);
    SET(info.command, FORCE_FULL);
    if (ISSET(info.command, FORCE_BUMP)) {
	CLR(info.command, FORCE_BUMP);
	printf("%s: WARNING: %s:%s FORCE_BUMP command was cleared.\n",
	       get_pname(), hostname, diskname);
    }
    if(put_info(hostname, diskname, &info) == 0) {
	printf("%s: %s:%s is set to a forced level 0 at next run.\n",
	       get_pname(), hostname, diskname);
    } else {
	fprintf(stderr, "%s: %s:%s could not be forced.\n",
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
#if TEXTDB
	check_dumpuser();
#endif
	CLR(info.command, FORCE_FULL);
	if(put_info(hostname, diskname, &info) == 0){
	    printf("%s: force command for %s:%s cleared.\n",
		   get_pname(), hostname, diskname);
	} else {
	    fprintf(stderr,
		    "%s: force command for %s:%s could not be cleared.\n",
		    get_pname(), hostname, diskname);
	}
    }
    else {
	printf("%s: no force command outstanding for %s:%s, unchanged.\n",
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

#if TEXTDB
    check_dumpuser();
#endif
    get_info(hostname, diskname, &info);
    SET(info.command, FORCE_BUMP);
    if (ISSET(info.command, FORCE_NO_BUMP)) {
	CLR(info.command, FORCE_NO_BUMP);
	printf("%s: WARNING: %s:%s FORCE_NO_BUMP command was cleared.\n",
	       get_pname(), hostname, diskname);
    }
    if (ISSET(info.command, FORCE_FULL)) {
	CLR(info.command, FORCE_FULL);
	printf("%s: WARNING: %s:%s FORCE_FULL command was cleared.\n",
	       get_pname(), hostname, diskname);
    }
    if(put_info(hostname, diskname, &info) == 0) {
	printf("%s: %s:%s is set to bump at next run.\n",
	       get_pname(), hostname, diskname);
    } else {
	fprintf(stderr, "%s: %s:%s could not be forced to bump.\n",
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

#if TEXTDB
    check_dumpuser();
#endif
    get_info(hostname, diskname, &info);
    SET(info.command, FORCE_NO_BUMP);
    if (ISSET(info.command, FORCE_BUMP)) {
	CLR(info.command, FORCE_BUMP);
	printf("%s: WARNING: %s:%s FORCE_BUMP command was cleared.\n",
	       get_pname(), hostname, diskname);
    }
    if(put_info(hostname, diskname, &info) == 0) {
	printf("%s: %s:%s is set to not bump at next run.\n",
	       get_pname(), hostname, diskname);
    } else {
	fprintf(stderr, "%s: %s:%s could not be force to not bump.\n",
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
#if TEXTDB
	check_dumpuser();
#endif
	CLR(info.command, FORCE_BUMP|FORCE_NO_BUMP);
	if(put_info(hostname, diskname, &info) == 0) {
	    printf("%s: bump command for %s:%s cleared.\n",
		   get_pname(), hostname, diskname);
	} else {
	    fprintf(stderr, "%s: %s:%s bump command could not be cleared.\n",
		    get_pname(), hostname, diskname);
	}
    }
    else {
	printf("%s: no bump command outstanding for %s:%s, unchanged.\n",
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
	fprintf(stderr,"%s: expecting \"reuse <tapelabel> ...\"\n",
		get_pname());
	usage();
    }

    check_dumpuser();
    for(count=3; count< argc; count++) {
	tp = lookup_tapelabel(argv[count]);
	if ( tp == NULL) {
	    fprintf(stderr, "reuse: tape label %s not found in tapelist.\n",
		argv[count]);
	    continue;
	}
	if( tp->reuse == 0 ) {
	    tp->reuse = 1;
	    printf("%s: marking tape %s as reusable.\n",
		   get_pname(), argv[count]);
	} else {
	    fprintf(stderr, "%s: tape %s already reusable.\n",
		    get_pname(), argv[count]);
	}
    }

    if(write_tapelist(conf_tapelist)) {
	error("could not write tapelist \"%s\"", conf_tapelist);
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
	fprintf(stderr,"%s: expecting \"no-reuse <tapelabel> ...\"\n",
		get_pname());
	usage();
    }

    check_dumpuser();
    for(count=3; count< argc; count++) {
	tp = lookup_tapelabel(argv[count]);
	if ( tp == NULL) {
	    fprintf(stderr, "no-reuse: tape label %s not found in tapelist.\n",
		argv[count]);
	    continue;
	}
	if( tp->reuse == 1 ) {
	    tp->reuse = 0;
	    printf("%s: marking tape %s as not reusable.\n",
		   get_pname(), argv[count]);
	} else {
	    fprintf(stderr, "%s: tape %s already not reusable.\n",
		    get_pname(), argv[count]);
	}
    }

    if(write_tapelist(conf_tapelist)) {
	error("could not write tapelist \"%s\"", conf_tapelist);
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
	printf("%s: %s:%s NOT currently in database.\n",
	       get_pname(), hostname, diskname);
	return;
    }

    deleted++;
    if(del_info(hostname, diskname)) {
	error("couldn't delete %s:%s from database: %s",
	      hostname, diskname, strerror(errno));
        /*NOTREACHED*/
    } else {
	printf("%s: %s:%s deleted from curinfo database.\n",
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
	printf(
	 "%s: NOTE: you'll have to remove these from the disklist yourself.\n",
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

    printf("\nCurrent info for %s %s:\n", dp->host->hostname, dp->name);
    if (ISSET(info.command, FORCE_FULL))
	printf("  (Forcing to level 0 dump at next run)\n");
    if (ISSET(info.command, FORCE_BUMP))
	printf("  (Forcing bump at next run)\n");
    if (ISSET(info.command, FORCE_NO_BUMP))
	printf("  (Forcing no-bump at next run)\n");
    printf("  Stats: dump rates (kps), Full:  %5.1lf, %5.1lf, %5.1lf\n",
	   info.full.rate[0], info.full.rate[1], info.full.rate[2]);
    printf("                    Incremental:  %5.1lf, %5.1lf, %5.1lf\n",
	   info.incr.rate[0], info.incr.rate[1], info.incr.rate[2]);
    printf("          compressed size, Full: %5.1lf%%,%5.1lf%%,%5.1lf%%\n",
	   info.full.comp[0]*100, info.full.comp[1]*100, info.full.comp[2]*100);
    printf("                    Incremental: %5.1lf%%,%5.1lf%%,%5.1lf%%\n",
	   info.incr.comp[0]*100, info.incr.comp[1]*100, info.incr.comp[2]*100);

    printf("  Dumps: lev datestmp  tape             file   origK   compK secs\n");
    for(lev = 0, sp = &info.inf[0]; lev < 9; lev++, sp++) {
	if(sp->date < (time_t)0 && sp->label[0] == '\0') continue;
	tm = localtime(&sp->date);
	if (tm) {
	    printf("          %d  %04d%02d%02d  %-15s  "
		   OFF_T_FMT " " OFF_T_FMT " " OFF_T_FMT " " TIME_T_FMT "\n",
		   lev, tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
		   sp->label,
		   (OFF_T_FMT_TYPE)sp->filenum,
		   (OFF_T_FMT_TYPE)sp->size,
		   (OFF_T_FMT_TYPE)sp->csize,
		   (TIME_T_FMT_TYPE)sp->secs);
	} else {
	    printf("          %d  BAD-DATE  %-15s  "
		   OFF_T_FMT " " OFF_T_FMT " " OFF_T_FMT " " TIME_T_FMT "\n",
		   lev,
		   sp->label,
		   (OFF_T_FMT_TYPE)sp->filenum,
		   (OFF_T_FMT_TYPE)sp->size,
		   (OFF_T_FMT_TYPE)sp->csize,
		   (TIME_T_FMT_TYPE)sp->secs);
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
	printf("new disk %s:%s ignored.\n", hp->hostname, dp->name);
    }
    else {
	days = next_level0(dp, &info);
	if(days < 0) {
	    printf("Overdue %2d day%s %s:%s\n",
		   -days, (-days == 1) ? ": " : "s:",
		   hp->hostname, dp->name);
	}
	else if(days == 0) {
	    printf("Due today: %s:%s\n", hp->hostname, dp->name);
	}
	else {
	    printf("Due in %2d day%s %s:%s\n", days,
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
    tape_t *tp, *lasttp;
    int runtapes, i, j;
    int nb_days = 1;

    if(argc > 4 && strcmp(argv[3],"--days") == 0) {
	nb_days = atoi(argv[4]);
	if(nb_days < 1) {
	    printf("days must be an integer bigger than 0\n");
	    return;
	}
	if (nb_days > 10000)
	    nb_days = 10000;
    }

    runtapes = getconf_int(CNF_RUNTAPES);
    tp = lookup_last_reusable_tape(0);

    for ( j=0 ; j < nb_days ; j++ ) {
	for ( i=0 ; i < runtapes ; i++ ) {
	    if(i==0)
		printf("The next Amanda run should go onto ");
	    else
		printf("                                   ");
	    if(tp != NULL) {
		printf("tape %s or a new tape.\n", tp->label);
	    } else {
		if (runtapes - i == 1)
		    printf("1 new tape.\n");
		else
		    printf("%d new tapes.\n", runtapes - i);
		i = runtapes;
	    }
	
	    tp = lookup_last_reusable_tape(i + 1);
	}
    }
    lasttp = lookup_tapepos(lookup_nb_tape());
    i = runtapes;
    if(lasttp && i > 0 && strcmp(lasttp->datestamp,"0") == 0) {
	int c = 0;
	while(lasttp && i > 0 && strcmp(lasttp->datestamp,"0") == 0) {
	    c++;
	    lasttp = lasttp->prev;
	    i--;
	}
	lasttp = lookup_tapepos(lookup_nb_tape());
	i = runtapes;
	if(c == 1) {
	    printf("The next new tape already labelled is: %s.\n",
		   lasttp->label);
	}
	else {
	    printf("The next %d new tapes already labelled are: %s", c,
		   lasttp->label);
	    lasttp = lasttp->prev;
	    c--;
	    while(lasttp && c > 0 && strcmp(lasttp->datestamp,"0") == 0) {
		printf(", %s", lasttp->label);
		lasttp = lasttp->prev;
		c--;
	    }
	    printf(".\n");
	}
    }
}

/* ----------------------------------------------- */

void
balance(
    int		argc,
    char **	argv)
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

    if(argc > 4 && strcmp(argv[3],"--days") == 0) {
	later = atoi(argv[4]);
	if(later < 0) later = conf_dumpcycle;
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
	    printf("new disk %s:%s ignored.\n", dp->host->hostname, dp->name);
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
	printf("\nNo data to report on yet.\n");
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
    printf("\n due-date  #fs    orig %cB     out %cB   balance\n",
	   displayunit[0], displayunit[0]);
    printf("----------------------------------------------\n");
    for(seq = 0; seq < later; seq++) {
	if(sp[seq].disks == 0 &&
	   ((seq > 0 && sp[seq-1].disks == 0) ||
	    ((seq < later-1) && sp[seq+1].disks == 0))) {
	    empty_day++;
	}
	else {
	    if(empty_day > 0) {
		printf("\n");
		empty_day = 0;
	    }
	    printf("%-9.9s  %3d " OFF_T_FMT " " OFF_T_FMT " ",
		   seqdatestr(seq), sp[seq].disks,
		   (OFF_T_FMT_TYPE)sp[seq].origsize,
		   (OFF_T_FMT_TYPE)sp[seq].outsize);
	    if(!sp[seq].outsize) printf("     --- \n");
	    else printf("%+8.1lf%%\n",
			(((double)sp[seq].outsize - (double)balanced) * 100.0 /
			(double)balanced));
	}
    }

    if(sp[later].disks != 0) {
	printf("later      %3d " OFF_T_FMT " " OFF_T_FMT " ",
	       sp[later].disks,
	       (OFF_T_FMT_TYPE)sp[later].origsize,
	       (OFF_T_FMT_TYPE)sp[later].outsize);
	if(!sp[later].outsize) printf("     --- \n");
	else printf("%+8.1lf%%\n",
		    (((double)sp[later].outsize - (double)balanced) * 100.0 /
		    (double)balanced));
    }
    printf("----------------------------------------------\n");
    printf("TOTAL      %3d " OFF_T_FMT " " OFF_T_FMT " " OFF_T_FMT "\n",
	   sp[total].disks,
	   (OFF_T_FMT_TYPE)sp[total].origsize,
	   (OFF_T_FMT_TYPE)sp[total].outsize,
	   (OFF_T_FMT_TYPE)total_balanced);
    if (sp[balance].origsize != sp[total].origsize ||
        sp[balance].outsize != sp[total].outsize ||
	balanced != total_balanced) {
	printf("BALANCED       " OFF_T_FMT " " OFF_T_FMT " " OFF_T_FMT "\n",
	       (OFF_T_FMT_TYPE)sp[balance].origsize,
	       (OFF_T_FMT_TYPE)sp[balance].outsize,
	       (OFF_T_FMT_TYPE)balanced);
    }
    if (sp[distinct].disks != sp[total].disks) {
	printf("DISTINCT   %3d " OFF_T_FMT " " OFF_T_FMT "\n",
	       sp[distinct].disks,
	       (OFF_T_FMT_TYPE)sp[distinct].origsize,
	       (OFF_T_FMT_TYPE)sp[distinct].outsize);
    }
    printf("  (estimated %d run%s per dumpcycle)\n",
	   runs_per_cycle, (runs_per_cycle == 1) ? "" : "s");
    if (overdue) {
	printf(" (%d filesystem%s overdue, the most being overdue %d day%s)\n",
	       overdue, (overdue == 1) ? "" : "s",
	       max_overdue, (max_overdue == 1) ? "" : "s");
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

    if(argc < 3) {
	fprintf(stderr,
		"%s: expecting \"find [--sort <hkdlpb>] [hostname [<disk>]]*\"\n",
		get_pname());
	usage();
    }


    sort_order = newstralloc(sort_order, DEFAULT_SORT_ORDER);
    if(argc > 4 && strcmp(argv[3],"--sort") == 0) {
	size_t i, valid_sort=1;

	for(i = strlen(argv[4]); i > 0; i--) {
	    switch (argv[4][i - 1]) {
	    case 'h':
	    case 'H':
	    case 'k':
	    case 'K':
	    case 'd':
	    case 'D':
	    case 'l':
	    case 'L':
	    case 'p':
	    case 'P':
	    case 'b':
	    case 'B':
		    break;
	    default: valid_sort=0;
	    }
	}
	if(valid_sort) {
	    sort_order = newstralloc(sort_order, argv[4]);
	} else {
	    printf("Invalid sort order: %s\n", argv[4]);
	    printf("Use default sort order: %s\n", sort_order);
	}
	start_argc=6;
    } else {
	start_argc=4;
    }
    errstr = match_disklist(&diskq, argc-(start_argc-1), argv+(start_argc-1));
    if (errstr) {
	printf("%s", errstr);
	amfree(errstr);
    }

    output_find = find_dump(1, &diskq);
    if(argc-(start_argc-1) > 0) {
	free_find_result(&output_find);
	errstr = match_disklist(&diskq, argc-(start_argc-1),
					argv+(start_argc-1));
	if (errstr) {
	    printf("%s", errstr);
	    amfree(errstr);
	}
	output_find = find_dump(0, NULL);
    }

    sort_find_result(sort_order, &output_find);
    print_find_result(output_find);
    free_find_result(&output_find);

    amfree(sort_order);
}


/* ------------------------ */


/* shared code with planner.c */

int
bump_thresh(
    int		level)
{
    int bump = getconf_int(CNF_BUMPSIZE);
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

    printf("Current bump parameters:\n");
    if(conf_bumppercent == 0) {
	printf("  bumpsize %5d KB\t- minimum savings (threshold) to bump level 1 -> 2\n",
	       getconf_int(CNF_BUMPSIZE));
	printf("  bumpdays %5d\t- minimum days at each level\n",
	       getconf_int(CNF_BUMPDAYS));
	printf("  bumpmult %5.5lg\t- threshold = bumpsize * bumpmult**(level-1)\n\n",
	       conf_bumpmult);

	printf("      Bump -> To  Threshold\n");
	for(l = 1; l < 9; l++)
	    printf("\t%d  ->  %d  %9d KB\n", l, l+1, bump_thresh(l));
	putchar('\n');
    }
    else {
	double bumppercent = (double)conf_bumppercent;

	printf("  bumppercent %3d %%\t- minimum savings (threshold) to bump level 1 -> 2\n",
	       conf_bumppercent);
	printf("  bumpdays %5d\t- minimum days at each level\n",
	       getconf_int(CNF_BUMPDAYS));
	printf("  bumpmult %5.5lg\t- threshold = disk_size * bumppercent * bumpmult**(level-1)\n\n",
	       conf_bumpmult);
	printf("      Bump -> To  Threshold\n");
	for(l = 1; l < 9; l++) {
	    printf("\t%d  ->  %d  %7.2lf %%\n", l, l+1, bumppercent);
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

    printf("CURINFO Version %s CONF %s\n", version(), getconf_str(CNF_ORG));

    curtime = time(0);
    if(gethostname(hostname, SIZEOF(hostname)-1) == -1) {
	error("could not determine host name: %s\n", strerror(errno));
	/*NOTREACHED*/
    }
    hostname[SIZEOF(hostname)-1] = '\0';
    printf("# Generated by:\n#    host: %s\n#    date: %s",
	   hostname, ctime(&curtime));

    printf("#    command:");
    for(i = 0; i < argc; i++)
	printf(" %s", argv[i]);

    printf("\n# This file can be merged back in with \"amadmin import\".\n");
    printf("# Edit only with care.\n");

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

    if(get_info(dp->host->hostname, dp->name, &info)) {
	fprintf(stderr, "Warning: no curinfo record for %s:%s\n",
		dp->host->hostname, dp->name);
	return;
    }
    printf("host: %s\ndisk: %s\n", dp->host->hostname, dp->name);
    printf("command: %u\n", info.command);
    printf("last_level: %d\n",info.last_level);
    printf("consecutive_runs: %d\n",info.consecutive_runs);
    printf("full-rate:");
    for(i=0;i<AVG_COUNT;i++) printf(" %lf", info.full.rate[i]);
    printf("\nfull-comp:");
    for(i=0;i<AVG_COUNT;i++) printf(" %lf", info.full.comp[i]);

    printf("\nincr-rate:");
    for(i=0;i<AVG_COUNT;i++) printf(" %lf", info.incr.rate[i]);
    printf("\nincr-comp:");
    for(i=0;i<AVG_COUNT;i++) printf(" %lf", info.incr.comp[i]);
    printf("\n");
    for(l=0;l<DUMP_LEVELS;l++) {
	if(info.inf[l].date < (time_t)0 && info.inf[l].label[0] == '\0') continue;
	printf("stats: %d " OFF_T_FMT " " OFF_T_FMT " " TIME_T_FMT " " TIME_T_FMT " " OFF_T_FMT " %s\n", l,
	       (OFF_T_FMT_TYPE)info.inf[l].size,
	       (OFF_T_FMT_TYPE)info.inf[l].csize,
	       (TIME_T_FMT_TYPE)info.inf[l].secs,
	       (TIME_T_FMT_TYPE)info.inf[l].date,
	       (OFF_T_FMT_TYPE)info.inf[l].filenum,
	       info.inf[l].label);
    }
    for(l=0;info.history[l].level > -1;l++) {
	printf("history: %d " OFF_T_FMT " " OFF_T_FMT " " TIME_T_FMT "\n",
	       info.history[l].level,
	       (OFF_T_FMT_TYPE)info.history[l].size,
	       (OFF_T_FMT_TYPE)info.history[l].csize,
	       (TIME_T_FMT_TYPE)info.history[l].date);
    }
    printf("//\n");
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
	fprintf(stderr, "%s: empty input.\n", get_pname());
	return;
    }

    s = line;
    ch = *s++;

    hdr = "version";
#define sc "CURINFO Version"
    if(strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	goto bad_header;
    }
    s += SIZEOF(sc)-1;
    ch = *s++;
#undef sc
    skip_whitespace(s, ch);
    if(ch == '\0'
       || sscanf(s - 1, "%d.%d.%d", &vers_maj, &vers_min, &vers_patch) != 3) {
	goto bad_header;
    }

    skip_integer(s, ch);			/* skip over major */
    if(ch != '.') {
	goto bad_header;
    }
    ch = *s++;
    skip_integer(s, ch);			/* skip over minor */
    if(ch != '.') {
	goto bad_header;
    }
    ch = *s++;
    skip_integer(s, ch);			/* skip over patch */

    hdr = "comment";
    if(ch == '\0') {
	goto bad_header;
    }
    skip_non_whitespace(s, ch);
    s[-1] = '\0';

    hdr = "CONF";
    skip_whitespace(s, ch);			/* find the org keyword */
#define sc "CONF"
    if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	goto bad_header;
    }
    s += SIZEOF(sc)-1;
    ch = *s++;
#undef sc

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
	fprintf(stderr,
	     "%s: WARNING: input is from newer Amanda version: %d.%d.%d.\n",
		get_pname(), vers_maj, vers_min, vers_patch);
    /*@end@*/

    if(strcmp(org, getconf_str(CNF_ORG)) != 0) {
	fprintf(stderr, "%s: WARNING: input is from different org: %s\n",
		get_pname(), org);
    }

    do {
    	rc = import_one();
    } while (rc);

    amfree(line);
    return;

 bad_header:

    /*@i@*/ amfree(line);
    fprintf(stderr, "%s: bad CURINFO header line in input: %s.\n",
	    get_pname(), hdr);
    fprintf(stderr, "    Was the input in \"amadmin export\" format?\n");
    return;
}


int
import_one(void)
{
    info_t info;
    stats_t onestat;
    int rc, level;
    time_t onedate;
    time_t *onedate_p = &onedate;
    char *line = NULL;
    char *s, *fp;
    int ch;
    int nb_history, i;
    char *hostname = NULL;
    char *diskname = NULL;
    time_t *secs_p;

#if TEXTDB
    check_dumpuser();
#endif

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
#define sc "host:"
    if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) goto parse_err;
    s += SIZEOF(sc)-1;
    ch = s[-1];
#undef sc
    skip_whitespace(s, ch);
    if(ch == '\0') goto parse_err;
    fp = s-1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    hostname = stralloc(fp);
    s[-1] = (char)ch;

    skip_whitespace(s, ch);
    while (ch == 0) {
      amfree(line);
      if((line = impget_line()) == NULL) goto shortfile_err;
      s = line;
      ch = *s++;
      skip_whitespace(s, ch);
    }
#define sc "disk:"
    if(strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) goto parse_err;
    s += SIZEOF(sc)-1;
    ch = s[-1];
#undef sc
    skip_whitespace(s, ch);
    if(ch == '\0') goto parse_err;
    fp = s-1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    diskname = stralloc(fp);
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
	if(strncmp(line, "//", 2) == 0) {
	    /* end of record */
	    break;
	}
	if(strncmp(line, "history:", 8) == 0) {
	    /* end of record */
	    break;
	}
	memset(&onestat, 0, SIZEOF(onestat));

	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
#define sc "stats:"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    goto parse_err;
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    goto parse_err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, OFF_T_FMT,
				(OFF_T_FMT_TYPE *)&onestat.size) != 1) {
	    goto parse_err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, OFF_T_FMT,
				(OFF_T_FMT_TYPE *)&onestat.csize) != 1) {
	    goto parse_err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
        secs_p = &onestat.secs;
	if(ch == '\0' || sscanf(s - 1, TIME_T_FMT,
				(TIME_T_FMT_TYPE *)secs_p) != 1) {
	    goto parse_err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf(s - 1, TIME_T_FMT,
				(TIME_T_FMT_TYPE *)onedate_p) != 1) {
	    goto parse_err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch != '\0') {
	    if(sscanf(s - 1, OFF_T_FMT, (OFF_T_FMT_TYPE *)&onestat.filenum) != 1) {
		goto parse_err;
	    }
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

	/* time_t not guarranteed to be long */
	/*@i1@*/ onestat.date = onedate;
	if(level < 0 || level > 9) goto parse_err;

	info.inf[level] = onestat;
    }
    nb_history = 0;
    for(i=0;i<=NB_HISTORY;i++) {
	info.history[i].level = -2;
    }
    while(1) {
	history_t onehistory;
	time_t date;
        time_t *date_p = &date;

	if(line[0] == '/' && line[1] == '/') {
	    info.history[nb_history].level = -2;
	    rc = 0;
	    break;
	}
	memset(&onehistory, 0, SIZEOF(onehistory));
	s = line;
	ch = *s++;
#define sc "history:"
	if(strncmp(line, sc, SIZEOF(sc)-1) != 0) {
	    break;
	}
	s += SIZEOF(sc)-1;
	ch = s[-1];
#undef sc
	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf((s - 1), "%d", &onehistory.level) != 1) {
	    break;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf((s - 1), OFF_T_FMT,
				(OFF_T_FMT_TYPE *)&onehistory.size) != 1) {
	    break;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if(ch == '\0' || sscanf((s - 1), OFF_T_FMT,
				(OFF_T_FMT_TYPE *)&onehistory.csize) != 1) {
	    break;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if((ch == '\0') || (sscanf((s - 1), TIME_T_FMT,
				(TIME_T_FMT_TYPE *)date_p) != 1)) {
	    break;
	}
	skip_integer(s, ch);
	/*@i1@*/onehistory.date = date; /* time_t not guarranteed to be long */

	info.history[nb_history++] = onehistory;
	amfree(line);
	if((line = impget_line()) == NULL) goto shortfile_err;
    }
    /*@i@*/ amfree(line);

    /* got a full record, now write it out to the database */

    if(put_info(hostname, diskname, &info)) {
	fprintf(stderr, "%s: error writing record for %s:%s\n",
		get_pname(), hostname, diskname);
    }
    amfree(hostname);
    amfree(diskname);
    return 1;

 parse_err:
    /*@i@*/ amfree(line);
    amfree(hostname);
    amfree(diskname);
    fprintf(stderr, "%s: parse error reading import record.\n", get_pname());
    return 0;

 shortfile_err:
    /*@i@*/ amfree(line);
    amfree(hostname);
    amfree(diskname);
    fprintf(stderr, "%s: short file reading import record.\n", get_pname());
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
	fprintf(stderr, "%s: reading stdin: %s\n",
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
    interface_t *ip;
    sle_t *excl;
    time_t st;
    struct tm *stm;

    hp = dp->host;
    ip = hp->netif;

    printf("line %d:\n", dp->line);

    printf("    host %s:\n", hp->hostname);
    printf("        interface %s\n",
	   ip->name[0] ? ip->name : "default");
    printf("    disk %s:\n", dp->name);
    if(dp->device) printf("        device %s\n", dp->device);

    printf("        program \"%s\"\n", dp->program);
    if(dp->exclude_file != NULL && dp->exclude_file->nb_element > 0) {
	printf("        exclude file");
	for(excl = dp->exclude_file->first; excl != NULL; excl = excl->next) {
	    printf(" \"%s\"", excl->name);
	}
	printf("\n");
    }
    if(dp->exclude_list != NULL && dp->exclude_list->nb_element > 0) {
	printf("        exclude list");
	if(dp->exclude_optional) printf(" optional");
	for(excl = dp->exclude_list->first; excl != NULL; excl = excl->next) {
	    printf(" \"%s\"", excl->name);
	}
	printf("\n");
    }
    if(dp->include_file != NULL && dp->include_file->nb_element > 0) {
	printf("        include file");
	for(excl = dp->include_file->first; excl != NULL; excl = excl->next) {
	    printf(" \"%s\"", excl->name);
	}
	printf("\n");
    }
    if(dp->include_list != NULL && dp->include_list->nb_element > 0) {
	printf("        include list");
	if(dp->include_optional) printf(" optional");
	for(excl = dp->include_list->first; excl != NULL; excl = excl->next) {
	    printf(" \"%s\"", excl->name);
	}
	printf("\n");
    }
    printf("        priority %d\n", dp->priority);
    printf("        dumpcycle %d\n", dp->dumpcycle);
    printf("        maxdumps %d\n", dp->maxdumps);
    printf("        maxpromoteday %d\n", dp->maxpromoteday);
    if(dp->bumppercent > 0) {
	printf("        bumppercent %d\n", dp->bumppercent);
    }
    else {
	printf("        bumpsize " OFF_T_FMT "\n",
		(OFF_T_FMT_TYPE)dp->bumpsize);
    }
    printf("        bumpdays %d\n", dp->bumpdays);
    printf("        bumpmult %lf\n", dp->bumpmult);

    printf("        strategy ");
    switch(dp->strategy) {
    case DS_SKIP:
	printf("SKIP\n");
	break;
    case DS_STANDARD:
	printf("STANDARD\n");
	break;
    case DS_NOFULL:
	printf("NOFULL\n");
	break;
    case DS_NOINC:
	printf("NOINC\n");
	break;
    case DS_HANOI:
	printf("HANOI\n");
	break;
    case DS_INCRONLY:
	printf("INCRONLY\n");
	break;
    }

    printf("        estimate ");
    switch(dp->estimate) {
    case ES_CLIENT:
	printf("CLIENT\n");
	break;
    case ES_SERVER:
	printf("SERVER\n");
	break;
    case ES_CALCSIZE:
	printf("CALCSIZE\n");
	break;
    }

    printf("        compress ");
    switch(dp->compress) {
    case COMP_NONE:
	printf("NONE\n");
	break;
    case COMP_FAST:
	printf("CLIENT FAST\n");
	break;
    case COMP_BEST:
	printf("CLIENT BEST\n");
	break;
    case COMP_SERV_FAST:
	printf("SERVER FAST\n");
	break;
    case COMP_SERV_BEST:
	printf("SERVER BEST\n");
	break;
    }
    if(dp->compress != COMP_NONE) {
	printf("        comprate %.2lf %.2lf\n",
	       dp->comprate[0], dp->comprate[1]);
    }

    printf("        encrypt ");
    switch(dp->encrypt) {
    case ENCRYPT_NONE:
	printf("NONE\n");
	break;
    case ENCRYPT_CUST:
	printf("CLIENT\n");
	break;
    case ENCRYPT_SERV_CUST:
	printf("SERVER\n");
	break;
    }

    printf("        auth %s\n", dp->security_driver);
    printf("        kencrypt %s\n", (dp->kencrypt? "YES" : "NO"));
    printf("        amandad_path %s\n", dp->amandad_path);
    printf("        client_username %s\n", dp->client_username);
    printf("        ssh_keys %s\n", dp->ssh_keys);

    printf("        holdingdisk ");
    switch(dp->to_holdingdisk) {
    case HOLD_NEVER:
	printf("NEVER\n");
	break;
    case HOLD_AUTO:
	printf("AUTO\n");
	break;
    case HOLD_REQUIRED:
	printf("REQUIRED\n");
	break;
    }

    printf("        record %s\n", (dp->record? "YES" : "NO"));
    printf("        index %s\n", (dp->index? "YES" : "NO"));
    st = dp->start_t;
        if(st) {
            stm = localtime(&st);
	    if (stm) 
		printf("        starttime %d:%02d:%02d\n",
		       stm->tm_hour, stm->tm_min, stm->tm_sec);
	    else
		printf("        starttime BAD DATE\n");
        }
   
    if(dp->tape_splitsize > (off_t)0) {
	printf("        tape_splitsize " OFF_T_FMT "\n",
	       (OFF_T_FMT_TYPE)dp->tape_splitsize);
    }
    if(dp->split_diskbuffer) {
	printf("        split_diskbuffer %s\n", dp->split_diskbuffer);
    }
    if(dp->fallback_splitsize > (off_t)0) {
	printf("        fallback_splitsize " OFF_T_FMT "Mb\n",
	       (OFF_T_FMT_TYPE)(dp->fallback_splitsize / (off_t)1024));
    }
    printf("        skip-incr %s\n", (dp->skip_incr? "YES" : "NO"));
    printf("        skip-full %s\n", (dp->skip_full? "YES" : "NO"));
    printf("        spindle %d\n", dp->spindle);

    printf("\n");
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

void
show_version(
    int		argc,
    char **	argv)
{
    int i;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    for(i = 0; version_info[i] != NULL; i++)
	printf("%s", version_info[i]);
}


void show_config(
    int argc,
    char **argv)
{
    argc = argc;
    argv = argv;
    dump_configuration(conffile);
}

