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
 * $Id: amfetchdump.c,v 1.16 2006/08/24 01:57:15 paddy_s Exp $
 *
 * retrieves specific dumps from a set of amanda tapes
 */

#include "amanda.h"
#include "fileheader.h"
#include "util.h"
#include "restore.h"
#include "diskfile.h"
#include "tapefile.h"
#include "find.h"
#include "changer.h"
#include "logfile.h"
#include "cmdline.h"

#define CREAT_MODE	0640

extern char *rst_conf_logfile;
extern char *config_dir;
int get_lock = 0;

typedef struct needed_tapes_s {
    char *label;
    int isafile;
    find_result_t *files;
    struct needed_tapes_s *next;
    struct needed_tapes_s *prev;
} needed_tape_t;

/* local functions */

tapelist_t *list_needed_tapes(GSList *dumpspecs);
void usage(void);
int main(int argc, char **argv);

/* exit routine */
static pid_t parent_pid = -1;
static void cleanup(void);


/*
 * Print usage message and terminate.
 */

void
usage(void)
{
    fprintf(stderr, _("Usage: amfetchdump [options] config hostname [diskname [datestamp [level [hostname [diskname [datestamp [level ... ]]]]]]] [-o configoption]*\n\n"));
    fprintf(stderr, _("Goes and grabs a dump from tape, moving tapes around and assembling parts as\n"));
    fprintf(stderr, _("necessary.  Files are restored to the current directory, unless otherwise\nspecified.\n\n"));
    fprintf(stderr, _("  -p Pipe exactly *one* complete dumpfile to stdout, instead of to disk.\n"));
    fprintf(stderr, _("  -O <output dir> Restore files to this directory.\n"));
    fprintf(stderr, _("  -d <device> Force restoration from a particular tape device.\n"));
    fprintf(stderr, _("  -c Compress output, fastest method available.\n"));
    fprintf(stderr, _("  -C Compress output, best filesize method available.\n"));
    fprintf(stderr, _("  -l Leave dumps (un)compressed, whichever way they were originally on tape.\n"));
    fprintf(stderr, _("  -a Assume all tapes are available via changer, do not prompt for initial load.\n"));
    fprintf(stderr, _("  -i <dst_file> Search through tapes and write out an inventory while we\n     restore.  Useful only if normal logs are unavailable.\n"));
    fprintf(stderr, _("  -w Wait to put split dumps together until all chunks have been restored.\n"));
    fprintf(stderr, _("  -n Do not reassemble split dumpfiles.\n"));
    fprintf(stderr, _("  -k Skip the rewind/label read when reading a new tape.\n"));
    fprintf(stderr, _("  -s Do not use fast forward to skip files we won't restore.  Use only if fsf\n     causes your tapes to skip too far.\n"));
    fprintf(stderr, _("  -b <blocksize> Force a particular block size (default is 32kb).\n"));
    exit(1);
}

/*
 * Build the list of tapes we'll be wanting, and include data about the
 * files we want from said tapes while we're at it (the whole find_result
 * should do fine)
 */
tapelist_t *
list_needed_tapes(
    GSList *	dumpspecs)
{
    needed_tape_t *needed_tapes = NULL, *curtape = NULL;
    disklist_t diskqp;
    dumpspec_t *ds = NULL;
    find_result_t *alldumps = NULL;
    tapelist_t *tapes = NULL;
    int numtapes = 0;
    char *conf_diskfile, *conf_tapelist;

    /* For disks and tape lists */
    conf_diskfile = getconf_str(CNF_DISKFILE);
    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_diskfile == '/') {
        conf_diskfile = stralloc(conf_diskfile);
    } else {
        conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if(read_diskfile(conf_diskfile, &diskqp) != 0) {
        error(_("could not load disklist \"%s\""), conf_diskfile);
	/*NOTREACHED*/
    }
    if (*conf_tapelist == '/') {
        conf_tapelist = stralloc(conf_tapelist);
    } else {
        conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if(read_tapelist(conf_tapelist)) {
        error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_diskfile);
    amfree(conf_tapelist);

    /* Grab a find_output_t of all logged dumps */
    alldumps = find_dump(1, &diskqp);
    free_disklist(&diskqp);
    if(alldumps == NULL){
        fprintf(stderr, _("No dump records found\n"));
        exit(1);
    }

    /* Compare all known dumps to our match list, note what we'll need */
    while (dumpspecs) {
	find_result_t *curmatch = NULL;	
	find_result_t *matches = NULL;	
	ds = (dumpspec_t *)dumpspecs->data;

	matches = dumps_match(alldumps, ds->host, ds->disk,
	                         ds->datestamp, ds->level, 1);
	sort_find_result("Dhklp", &matches);
	for(curmatch = matches; curmatch; curmatch = curmatch->next){
	    int havetape = 0;
	    if(strcmp("OK", curmatch->status)){
		fprintf(stderr,_("Dump %s %s %s %d had status '%s', skipping\n"),
		                 curmatch->timestamp, curmatch->hostname,
				 curmatch->diskname, curmatch->level,
				 curmatch->status);
		continue;
	    }
	    for(curtape = needed_tapes; curtape; curtape = curtape->next) {
		if(!strcmp(curtape->label, curmatch->label)){
		    find_result_t *rsttemp = NULL;
		    find_result_t *rstfile = alloc(SIZEOF(find_result_t));
		    int keep = 1;

		    memcpy(rstfile, curmatch, SIZEOF(find_result_t));

		    havetape = 1;

		    for(rsttemp = curtape->files;
			    rsttemp;
			    rsttemp=rsttemp->next){
			if(rstfile->filenum == rsttemp->filenum){
			    fprintf(stderr, _("Seeing multiple entries for tape "
				   "%s file " OFF_T_FMT ", using most recent\n"),
				    curtape->label,
				    (OFF_T_FMT_TYPE)rstfile->filenum);
			    keep = 0;
			}
		    }
		    if(!keep){
			amfree(rstfile);
			break;
		    }
		    rstfile->next = curtape->files;

		    if(curmatch->filenum < 1) curtape->isafile = 1;
		    else curtape->isafile = 0;
		    curtape->files = rstfile;
		    break;
		}
	    }
	    if(!havetape){
		find_result_t *rstfile = alloc(SIZEOF(find_result_t));
		needed_tape_t *newtape =
		                          alloc(SIZEOF(needed_tape_t));
		memcpy(rstfile, curmatch, SIZEOF(find_result_t));
		rstfile->next = NULL;
		newtape->files = rstfile;
		if(curmatch->filenum < 1) newtape->isafile = 1;
		else newtape->isafile = 0;
		newtape->label = curmatch->label;
		if(needed_tapes){
		    needed_tapes->prev->next = newtape;
		    newtape->prev = needed_tapes->prev;
		    needed_tapes->prev = newtape;
		}
		else{
		    needed_tapes = newtape;
		    needed_tapes->prev = needed_tapes;
		}
		newtape->next = NULL;
		numtapes++;
#if 0
//		free_find_result(rstfile);
#endif
	    } /* if(!havetape) */

	} /* for(curmatch = matches ... */
	dumpspecs = dumpspecs->next;
    } /* while (dumpspecs) */

    if(numtapes == 0){
      fprintf(stderr, _("No matching dumps found\n"));
      exit(1);
      /* NOTREACHED */
    }

    /* stick that list in a structure that librestore will understand */
    for(curtape = needed_tapes; curtape; curtape = curtape->next) {
	find_result_t *curfind = NULL;
	for(curfind = curtape->files; curfind; curfind = curfind->next) {
	    tapes = append_to_tapelist(tapes, curtape->label,
				       curfind->filenum, curtape->isafile);
	}
    }

    fprintf(stderr, _("%d tape(s) needed for restoration\n"), numtapes);
    return(tapes);
}


/*
 * Parses command line, then loops through all files on tape, restoring
 * files that match the command line criteria.
 */

int
main(
    int		argc,
    char **	argv)
{
    extern int optind;
    int opt;
    GSList *dumpspecs = NULL;
    int fd;
    char *config_name = NULL;
    char *conffile = NULL;
    tapelist_t *needed_tapes = NULL;
    char *e;
    rst_flags_t *rst_flags;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;
    int minimum_arguments;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    for(fd = 3; fd < (int)FD_SETSIZE; fd++) {
	/*
	 * Make sure nobody spoofs us with a lot of extra open files
	 * that would cause a successful open to get a very high file
	 * descriptor, which in turn might be used as an index into
	 * an array (e.g. an fd_set).
	 */
	close(fd);
    }

    set_pname("amfetchdump");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    erroutput_type = ERR_INTERACTIVE;
    error_exit_status = 2;

    parse_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    rst_flags = new_rst_flags();
    rst_flags->wait_tape_prompt = 1;

    /* handle options */
    while( (opt = getopt(my_argc, my_argv, "alht:scCpb:nwi:d:O:")) != -1) {
	switch(opt) {
	case 'b':
            rst_flags->blocksize = (ssize_t)strtol(optarg, &e, 10);
            if(*e == 'k' || *e == 'K') {
	        rst_flags->blocksize *= 1024;
	    } else if(*e == 'm' || *e == 'M') {
	        rst_flags->blocksize *= 1024 * 1024;
	    } else if(*e != '\0') {
	        error(_("invalid blocksize value \"%s\""), optarg);
		/*NOTREACHED*/
	    }
	    if(rst_flags->blocksize < DISK_BLOCK_BYTES) {
	        error(_("minimum block size is %dk"), DISK_BLOCK_BYTES / 1024);
		/*NOTREACHED*/
	    }
	    break;
	case 'c': rst_flags->compress = 1; break;
	case 'O': rst_flags->restore_dir = stralloc(optarg) ; break;
	case 'd': rst_flags->alt_tapedev = stralloc(optarg) ; break;
	case 'C':
	    rst_flags->compress = 1;
	    rst_flags->comp_type = COMPRESS_BEST_OPT;
	    break;
	case 'p': rst_flags->pipe_to_fd = STDOUT_FILENO; break;
	case 's': rst_flags->fsf = (off_t)0; break;
	case 'l': rst_flags->leave_comp = 1; break;
	case 'i': rst_flags->inventory_log = stralloc(optarg); break;
	case 'n': rst_flags->inline_assemble = 0; break;
	case 'w': rst_flags->delay_assemble = 1; break;
	case 'a': rst_flags->wait_tape_prompt = 0; break;
	case 'h': rst_flags->headers = 1; break;
	default:
	    usage();
	    /*NOTREACHED*/
	}
    }

    /* Check some flags that affect inventorying */
    if(rst_flags->inventory_log){
	if(rst_flags->inline_assemble) rst_flags->delay_assemble = 1;
	rst_flags->inline_assemble = 0;
	rst_flags->leave_comp = 1;
	if(rst_flags->compress){
	    error(_("Cannot force compression when doing inventory/search"));
	    /*NOTREACHED*/
	}
	fprintf(stderr, _("Doing inventory/search, dumps will not be uncompressed or assembled on-the-fly.\n"));
    }
    else{
	if(rst_flags->delay_assemble){
	    fprintf(stderr, _("Using -w, split dumpfiles will *not* be automatically uncompressed.\n"));
	}
    }

    /* make sure our options all make sense otherwise */
    if(check_rst_flags(rst_flags) == -1) {
    	usage();
	/*NOTREACHED*/
    }

    if (rst_flags->inventory_log) {
        minimum_arguments = 1;
    } else {
        minimum_arguments = 2;
    }
    
    if(my_argc - optind < minimum_arguments) {
	usage();
	/*NOTREACHED*/
    }

    config_name = my_argv[optind++];
    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	error(_("errors processing config file \"%s\""), conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    check_running_as(RUNNING_AS_DUMPUSER | RUNNING_WITHOUT_SETUID);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    dumpspecs = cmdline_parse_dumpspecs(my_argc - optind, my_argv + optind, CMDLINE_PARSE_DATESTAMP|CMDLINE_PARSE_LEVEL);

    /*
     * We've been told explicitly to go and search through the tapes the hard
     * way.
     */
    if(rst_flags->inventory_log){
	fprintf(stderr, _("Beginning tape-by-tape search.\n"));
	search_tapes(stderr, stdin, rst_flags->alt_tapedev == NULL,
                     NULL, dumpspecs, rst_flags, NULL);
	exit(0);
    }


    /* Decide what tapes we'll need */
    needed_tapes = list_needed_tapes(dumpspecs);

    parent_pid = getpid();
    atexit(cleanup);
    get_lock = lock_logfile(); /* config is loaded, should be ok here */
    if(get_lock == 0) {
	error(_("%s exists: amdump or amflush is already running, or you must run amcleanup"), rst_conf_logfile);
    }
    search_tapes(NULL, stdin, rst_flags->alt_tapedev != NULL,
                 needed_tapes, dumpspecs, rst_flags, NULL);
    cleanup();

    dumpspec_list_free(dumpspecs);

    if(rst_flags->inline_assemble || rst_flags->delay_assemble)
	flush_open_outputs(1, NULL);
    else flush_open_outputs(0, NULL);

    free_rst_flags(rst_flags);
    free_new_argv(new_argc, new_argv);

    return(0);
}

static void
cleanup(void)
{
    if(parent_pid == getpid()) {
	if(get_lock) unlink(rst_conf_logfile);
    }
}
