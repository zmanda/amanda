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
#include "server_util.h"
#include <getopt.h>

#define CREAT_MODE	0640

extern char *rst_conf_logfile;
extern char *config_dir;
int get_lock = 0;

typedef struct needed_tapes_s {
    char *label;
    int isafile;
    GSList *files;

    /* usage_order helps to determine the order in which multiple tapes were
     * used in a single run; it is set as the tapes are added, as sorted by
     * dumpfile part number */
    int usage_order;
} needed_tape_t;

/* local functions */

tapelist_t *list_needed_tapes(GSList *dumpspecs, int only_one, disklist_t *diskqp);
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
    g_fprintf(stderr, _("Usage: amfetchdump [options] [-o configoption]* config hostname [diskname [datestamp [level [hostname [diskname [datestamp [level ... ]]]]]]]\n\n"));
    g_fprintf(stderr, _("Goes and grabs a dump from tape, moving tapes around and assembling parts as\n"));
    g_fprintf(stderr, _("necessary.  Files are restored to the current directory, unless otherwise\nspecified.\n\n"));
    g_fprintf(stderr, _("  -p Pipe exactly *one* complete dumpfile to stdout, instead of to disk.\n"));
    g_fprintf(stderr, _("  -h Output the amanda header to same output as the image.\n"));
    g_fprintf(stderr, _("  --header-fd <fd> Output the amanda header to the numbered file descriptor.\n"));
    g_fprintf(stderr, _("  --header-file <filename> Output the amanda header to the filename.\n"));
    g_fprintf(stderr, _("  -O <output dir> Restore files to this directory.\n"));
    g_fprintf(stderr, _("  -d <device> Force restoration from a particular tape device.\n"));
    g_fprintf(stderr, _("  -c Compress output, fastest method available.\n"));
    g_fprintf(stderr, _("  -C Compress output, best filesize method available.\n"));
    g_fprintf(stderr, _("  -l Leave dumps (un)compressed, whichever way they were originally on tape.\n"));
    g_fprintf(stderr, _("  -a Assume all tapes are available via changer, do not prompt for initial load.\n"));
    g_fprintf(stderr, _("  -i <dst_file> Search through tapes and write out an inventory while we\n     restore.  Useful only if normal logs are unavailable.\n"));
    g_fprintf(stderr, _("  -w Wait to put split dumps together until all chunks have been restored.\n"));
    g_fprintf(stderr, _("  -n Do not reassemble split dumpfiles.\n"));
    g_fprintf(stderr, _("  -k Skip the rewind/label read when reading a new tape.\n"));
    g_fprintf(stderr, _("  -s Do not use fast forward to skip files we won't restore.  Use only if fsf\n     causes your tapes to skip too far.\n"));
    g_fprintf(stderr, _("  -b <blocksize> Force a particular block size (default is 32kb).\n"));
    dbclose();
    exit(1);
}

static gint
sort_needed_tapes_by_write_timestamp(
	gconstpointer a,
	gconstpointer b)
{
    needed_tape_t *a_nt = (needed_tape_t *)a;
    needed_tape_t *b_nt = (needed_tape_t *)b;
    tape_t *a_t = a_nt->isafile? NULL : lookup_tapelabel(a_nt->label);
    tape_t *b_t = b_nt->isafile? NULL : lookup_tapelabel(b_nt->label);
    char *a_ds = a_t? a_t->datestamp : "none";
    char *b_ds = b_t? b_t->datestamp : "none";

    /* if the tape timestamps match, sort them by usage_order, which is derived
     * from the order the tapes were written in a single run */
    int r = strcmp(a_ds, b_ds);
    if (r != 0)
	return r;
    return (a_nt->usage_order > b_nt->usage_order)? 1 : -1;
}

/*
 * Build the list of tapes we'll be wanting, and include data about the
 * files we want from said tapes while we're at it (the whole find_result
 * should do fine)
 */
tapelist_t *
list_needed_tapes(
    GSList *	dumpspecs,
    int		only_one,
    disklist_t	*diskqp)
{
    GSList *needed_tapes = NULL;
    GSList *seen_dumps = NULL;
    GSList *iter, *iter2;
    find_result_t *alldumps = NULL;
    find_result_t *curmatch = NULL;
    find_result_t *matches = NULL;
    tapelist_t *tapes = NULL;
    int usage_order_counter = 0;
    char *conf_tapelist;

    /* Load the tape list */
    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    if(read_tapelist(conf_tapelist)) {
        error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

    /* Grab a find_output_t of all logged dumps */
    alldumps = find_dump(diskqp);
    if(alldumps == NULL){
        g_fprintf(stderr, _("No dump records found\n"));
	dbclose();
        exit(1);
    }

    /* Compare all known dumps to our match list, note what we'll need */
    matches = dumps_match_dumpspecs(alldumps, dumpspecs, 1);

    /* D = dump_timestamp, newest first
     * h = hostname
     * k = diskname
     * l = level
     * p = partnum
     * w = write_timestamp */
    sort_find_result("Dhklpw", &matches);

    for(curmatch = matches; curmatch; curmatch = curmatch->next) {
	int havetape = 0;

	/* keep only first dump if only_one */
	if (only_one &&
	    curmatch != matches &&
	    (strcmp(curmatch->hostname, matches->hostname) ||
	     strcmp(curmatch->diskname, matches->diskname) ||
	     strcmp(curmatch->timestamp, matches->timestamp) ||
	     curmatch->level != matches->level)) {
	    continue;
	}
	if(strcmp("OK", curmatch->status)){
	    g_fprintf(stderr,_("Dump %s %s %s %d had status '%s', skipping\n"),
		             curmatch->timestamp, curmatch->hostname,
			     curmatch->diskname, curmatch->level,
			     curmatch->status);
	    continue;
	}

	for(iter = needed_tapes; iter; iter = iter->next) {
	    needed_tape_t *curtape = iter->data;
	    if (!strcmp(curtape->label, curmatch->label)) {
		int keep = 1;

		havetape = 1;

		for(iter2 = curtape->files; iter2; iter2 = iter2->next){
		    find_result_t *rsttemp = iter2->data;
		    if(curmatch->filenum == rsttemp->filenum){
			g_fprintf(stderr, _("Seeing multiple entries for tape "
				   "%s file %lld, using most recent\n"),
				    curtape->label,
				    (long long)curmatch->filenum);
			keep = 0;
		    }
		}
		if(!keep){
		    break;
		}

		curtape->isafile = (curmatch->filenum < 1);
		curtape->files = g_slist_prepend(curtape->files, curmatch);
		break;
	    }
	}
	if (!havetape) {
	    needed_tape_t *newtape = g_new0(needed_tape_t, 1);
	    newtape->usage_order = usage_order_counter++;
	    newtape->files = g_slist_prepend(newtape->files, curmatch);
	    newtape->isafile = (curmatch->filenum < 1);
	    newtape->label = curmatch->label;
	    needed_tapes = g_slist_prepend(needed_tapes, newtape);
	} /* if(!havetape) */

    } /* for(curmatch = matches ... */

    if(g_slist_length(needed_tapes) == 0){
      g_fprintf(stderr, _("No matching dumps found\n"));
      exit(1);
      /* NOTREACHED */
    }

    /* sort the tapelist by tape write_timestamp */
    needed_tapes = g_slist_sort(needed_tapes, sort_needed_tapes_by_write_timestamp);

    /* stick that list in a structure that librestore will understand, removing
     * files we have already seen in the process; this prefers the earliest written
     * copy of any dumps which are available on multiple tapes */
    seen_dumps = NULL;
    for(iter = needed_tapes; iter; iter = iter->next) {
	needed_tape_t *curtape = iter->data;
	for(iter2 = curtape->files; iter2; iter2 = iter2->next) {
	    find_result_t *curfind = iter2->data;
	    find_result_t *prev;
	    GSList *iter;
	    int have_part;

	    /* have we already seen this? */
	    have_part = 0;
	    for (iter = seen_dumps; iter; iter = iter->next) {
		prev = iter->data;

		if (prev->partnum == curfind->partnum &&
		    prev->totalparts == curfind->totalparts &&
		    !strcmp(prev->hostname, curfind->hostname) &&
		    !strcmp(prev->diskname, curfind->diskname) &&
		    !strcmp(prev->timestamp, curfind->timestamp) &&
		    prev->level == curfind->level) {
		    have_part = 1;
		    break;
		}
	    }

	    if (!have_part) {
		seen_dumps = g_slist_prepend(seen_dumps, curfind);
		tapes = append_to_tapelist(tapes, curtape->label,
					   curfind->filenum, -1, curtape->isafile);
	    }
	}
    }

    /* free our resources */
    for (iter = needed_tapes; iter; iter = iter->next) {
	needed_tape_t *curtape = iter->data;
	g_slist_free(curtape->files);
	g_free(curtape);
    }
    g_slist_free(seen_dumps);
    g_slist_free(needed_tapes);
    free_find_result(&matches);

    /* and we're done */
    g_fprintf(stderr, _("%d tape(s) needed for restoration\n"), num_entries(tapes));
    return(tapes);
}


/*
 * Parses command line, then loops through all files on tape, restoring
 * files that match the command line criteria.
 */

static int loptions = 0;
static struct option long_options[] = {
    {"header-fd"  , 1, &loptions, 1 },
    {"header-file", 1, &loptions, 2 },
    {NULL, 0, NULL, 0}
};

int
main(
    int		argc,
    char **	argv)
{
    extern int optind;
    int opt;
    GSList *dumpspecs = NULL;
    int fd;
    tapelist_t *needed_tapes = NULL;
    char *e;
    rst_flags_t *rst_flags;
    int minimum_arguments;
    config_overrides_t *cfg_ovr = NULL;
    disklist_t diskq;
    char * conf_diskfile = NULL;
    am_feature_t *our_features = am_init_feature_set();

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    set_pname("amfetchdump");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    add_amanda_log_handler(amanda_log_stderr);
    error_exit_status = 2;

    rst_flags = new_rst_flags();
    rst_flags->wait_tape_prompt = 1;

    /* handle options */
    cfg_ovr = new_config_overrides(argc/2);
    while( (opt = getopt_long(argc, argv, "alht:scCpb:nwi:d:O:o:", long_options, NULL)) != -1) {
	switch(opt) {
	case 0:
	    switch (loptions) {
		case 1:
		    rst_flags->headers = 1;
		    if (strcmp(optarg, "-") == 0)
			rst_flags->header_to_fd = STDOUT_FILENO;
		    else
			rst_flags->header_to_fd = atoi(optarg);
		    if (fcntl(rst_flags->header_to_fd, F_GETFL, NULL) == -1) {
			error(_("fd %d: %s\n"), rst_flags->header_to_fd,
			      strerror(errno));
		    }
		    break;
		case 2:
		    rst_flags->headers = 1;
		    rst_flags->header_to_fd = open(optarg,
						 O_WRONLY | O_CREAT | O_TRUNC,
						 S_IRUSR | S_IWUSR);
		    if (rst_flags->header_to_fd == -1) {
			error(_("Can't create '%s': %s\n"), optarg,
			      strerror(errno));
		    }
		    break;
	    }
	    break;
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
	case 'o': add_config_override_opt(cfg_ovr, optarg); break;
	default:
	    usage();
	    /*NOTREACHED*/
	}
    }

    for(fd = 3; fd < (int)FD_SETSIZE; fd++) {
	if (fd != debug_fd() &&
	    fd != rst_flags->pipe_to_fd &&
	    fd != rst_flags->header_to_fd) {
	    /*
	     * Make sure nobody spoofs us with a lot of extra open files
	     * that would cause a successful open to get a very high file
	     * descriptor, which in turn might be used as an index into
	     * an array (e.g. an fd_set).
	     */
	    close(fd);
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
	g_fprintf(stderr, _("Doing inventory/search, dumps will not be uncompressed or assembled on-the-fly.\n"));
    }
    else{
	if(rst_flags->delay_assemble){
	    g_fprintf(stderr, _("Using -w, split dumpfiles will *not* be automatically uncompressed.\n"));
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
 
    if(argc - optind < minimum_arguments) {
	usage();
	/*NOTREACHED*/
    }

    config_init(CONFIG_INIT_EXPLICIT_NAME, argv[optind++]);
    apply_config_overrides(cfg_ovr);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &diskq);
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    dumpspecs = cmdline_parse_dumpspecs(argc - optind, argv + optind,
					CMDLINE_PARSE_DATESTAMP |
					CMDLINE_PARSE_LEVEL |
					CMDLINE_EMPTY_TO_WILDCARD);

    /*
     * We've been told explicitly to go and search through the tapes the hard
     * way.
     */
    if(rst_flags->inventory_log){
	g_fprintf(stderr, _("Beginning tape-by-tape search.\n"));
	search_tapes(stderr, stdin, rst_flags->alt_tapedev == NULL,
                     NULL, dumpspecs, rst_flags, our_features);
	dbclose();
	exit(0);
    }


    /* Decide what tapes we'll need */
    needed_tapes = list_needed_tapes(dumpspecs,
				     rst_flags->pipe_to_fd == STDOUT_FILENO,
				     &diskq);

    parent_pid = getpid();
    atexit(cleanup);
    get_lock = lock_logfile(); /* config is loaded, should be ok here */
    if(get_lock == 0) {
	char *process_name = get_master_process(rst_conf_logfile);
	error(_("%s exists: %s is already running, or you must run amcleanup"), rst_conf_logfile, process_name);
    }
    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
    search_tapes(NULL, stdin, rst_flags->alt_tapedev == NULL,
                 needed_tapes, dumpspecs, rst_flags, our_features);
    cleanup();

    dumpspec_list_free(dumpspecs);

    if(rst_flags->inline_assemble || rst_flags->delay_assemble)
	flush_open_outputs(1, NULL);
    else flush_open_outputs(0, NULL);

    free_disklist(&diskq);
    free_rst_flags(rst_flags);
    am_release_feature_set(our_features);

    dbclose();

    return(0);
}

static void
cleanup(void)
{
    if (parent_pid == getpid()) {
	if (get_lock) {
	    log_add(L_INFO, "pid-done %ld\n", (long)getpid());
	    unlink(rst_conf_logfile);
	}
    }
}
