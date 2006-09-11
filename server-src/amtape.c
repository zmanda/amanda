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
 * $Id: amtape.c,v 1.47 2006/07/25 18:27:57 martinea Exp $
 *
 * tape changer interface program
 */
#include "amanda.h"
#include "conffile.h"
#include "tapefile.h"
#include "taperscan.h"
#include "tapeio.h"
#include "clock.h"
#include "changer.h"
#include "version.h"

/* local functions */
void usage(void);
int main(int argc, char **argv);
void reset_changer(int argc, char **argv);
void eject_tape(int argc, char **argv);
void clean_tape(int argc, char **argv);
void load_slot(int argc, char **argv);
void load_label(int argc, char **argv);
void show_slots(int argc, char **argv);
void show_current(int argc, char **argv);
void update_labeldb (int argc, char **argv);
void amtape_taper_scan(int argc, char **argv);
void show_device(int argc, char **argv);
int update_one_slot (void *ud, int rc, char *slotstr, char *device);
int loadlabel_slot(void *ud, int rc, char *slotstr, char *device);
int show_init(void *ud, int rc, int ns, int bk, int s);
int show_init_all(void *ud, int rc, int ns, int bk, int s);
int show_init_current(void *ud, int rc, int ns, int bk, int s);
int show_slot(void *ud, int rc, char *slotstr, char *device);

static const struct {
    const char *name;
    void (*fn)(int, char **);
    const char *usage;
} cmdtab[] = {
    { "reset", reset_changer,
	"reset                Reset changer to known state" },
    { "eject", eject_tape,
	"eject                Eject current tape from drive" },
    { "clean", clean_tape,
	"clean                Clean the drive" },
    { "show", show_slots,
	"show                 Show contents of all slots" },
    { "current", show_current,
	"current              Show contents of current slot" },
    { "slot" , load_slot,
	"slot <slot #>        load tape from slot <slot #>" },
    { "slot" , load_slot,
	"slot current         load tape from current slot" },
    { "slot" , load_slot,
	"slot prev            load tape from previous slot" },
    { "slot" , load_slot,
	"slot next            load tape from next slot" },
    { "slot" , load_slot,
	"slot advance         advance to next slot but do not load" },
    { "slot" , load_slot,
	"slot first           load tape from first slot" },
    { "slot" , load_slot,
	"slot last            load tape from last slot" },
    { "label", load_label,
	"label <label>        find and load labeled tape" },
    { "taper", amtape_taper_scan,
	"taper                perform taper's scan alg." },
    { "device", show_device,
	"device               show current tape device" },
    { "update", update_labeldb,
	"update               update the label matchingdatabase"},
};
#define	NCMDS	(int)(sizeof(cmdtab) / sizeof(cmdtab[0]))

void
usage(void)
{
    int i;

    fprintf(stderr, "Usage: amtape%s <conf> <command>\n", versionsuffix());
    fprintf(stderr, "\tValid commands are:\n");
    for (i = 0; i < NCMDS; i++)
	fprintf(stderr, "\t\t%s\n", cmdtab[i].usage);
    exit(1);
}

int
main(
    int		argc,
    char **	argv)
{
    char *conffile;
    char *conf_tapelist;
    char *argv0 = argv[0];
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    int i;
    int have_changer;
    uid_t uid_me;
    uid_t uid_dumpuser;
    char *dumpuser;
    struct passwd *pw;

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amtape");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    erroutput_type = ERR_INTERACTIVE;

    if(argc < 3) usage();

    config_name = argv[1];

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }

    dbrename(config_name, DBG_SUBDIR_SERVER);

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if (read_tapelist(conf_tapelist)) {
	error("could not load tapelist \"%s\"", conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

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
	error("running as user \"%s\" instead of \"%s\"",
	      pw->pw_name, dumpuser);
	/*NOTREACHED*/
    }

    if((have_changer = changer_init()) == 0) {
	error("no tpchanger specified in \"%s\"", conffile);
	/*NOTREACHED*/
    } else if (have_changer != 1) {
	error("changer initialization failed: %s", strerror(errno));
	/*NOTREACHED*/
    }

    /* switch on command name */

    argc -= 2; argv += 2;
    for (i = 0; i < NCMDS; i++)
	if (strcmp(argv[0], cmdtab[i].name) == 0) {
	    (*cmdtab[i].fn)(argc, argv);
	    break;
	}
    if (i == NCMDS) {
	fprintf(stderr, "%s: unknown command \"%s\"\n", argv0, argv[0]);
	usage();
    }

    amfree(changer_resultstr);
    amfree(conffile);
    amfree(config_dir);

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    dbclose();
    return 0;
}

/* ---------------------------- */

void
reset_changer(
    int		argc,
    char **	argv)
{
    char *slotstr = NULL;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    switch(changer_reset(&slotstr)) {
    case 0:
	fprintf(stderr, "%s: changer is reset, slot %s is loaded.\n",
		get_pname(), slotstr);
	break;
    case 1:
	fprintf(stderr, "%s: changer is reset, but slot %s not loaded: %s\n",
		get_pname(), slotstr, changer_resultstr);
	break;
    default:
	error("could not reset changer: %s", changer_resultstr);
	/*NOTREACHED*/
    }
    amfree(slotstr);
}


/* ---------------------------- */
void
clean_tape(
    int		argc,
    char **	argv)
{
    char *devstr = NULL;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    if(changer_clean(&devstr) == 0) {
	fprintf(stderr, "%s: device %s is clean.\n", get_pname(), devstr);
    } else {
	fprintf(stderr, "%s: device %s not clean: %s\n",
		get_pname(), devstr ? devstr : "??", changer_resultstr);
    }
    amfree(devstr);
}


/* ---------------------------- */
void
eject_tape(
    int		argc,
    char **	argv)
{
    char *slotstr = NULL;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    if(changer_eject(&slotstr) == 0) {
	fprintf(stderr, "%s: slot %s is ejected.\n", get_pname(), slotstr);
    } else {
	fprintf(stderr, "%s: slot %s not ejected: %s\n",
		get_pname(), slotstr ? slotstr : "??", changer_resultstr);
    }
    amfree(slotstr);
}


/* ---------------------------- */

void
load_slot(
    int		argc,
    char **	argv)
{
    char *slotstr = NULL, *devicename = NULL;
    char *errstr;
    int is_advance;

    if(argc != 2)
	usage();

    is_advance = (strcmp(argv[1], "advance") == 0);
    if(changer_loadslot(argv[1], &slotstr, &devicename)) {
	error("could not load slot %s: %s", slotstr, changer_resultstr);
	/*NOTREACHED*/
    }
    if(! is_advance && (errstr = tape_rewind(devicename)) != NULL) {
	fprintf(stderr,
		"%s: could not rewind %s: %s", get_pname(), devicename, errstr);
	amfree(errstr);
    }

    fprintf(stderr, "%s: changed to slot %s", get_pname(), slotstr);
    if(! is_advance) {
	fprintf(stderr, " on %s", devicename);
    }
    fputc('\n', stderr);
    amfree(slotstr);
    amfree(devicename);
}


/* ---------------------------- */

int nslots, backwards, found, got_match, tapedays;
char *datestamp;
char *label = NULL, *first_match_label = NULL, *first_match = NULL;
char *searchlabel, *labelstr;
tape_t *tp;
static int scan_init(void *ud, int rc, int ns, int bk, int s);

static int 
scan_init(
    void *	ud,
    int		rc,
    int		ns,
    int		bk,
    int		s)
{
    (void)ud;	/* Quiet unused parameter warning */
    (void)s;	/* Quiet unused parameter warning */

    if(rc) {
	error("could not get changer info: %s", changer_resultstr);
	/*NOTREACHED*/
    }

    nslots = ns;
    backwards = bk;

    return 0;
}

int
loadlabel_slot(
    void *	ud,
    int		rc,
    char *	slotstr,
    char *	device)
{
    char *errstr;

    (void)ud;	/* Quiet unused parameter warning */

    if(rc > 1) {
	error("could not load slot %s: %s", slotstr, changer_resultstr);
	/*NOTREACHED*/
    }
    else if(rc == 1)
	fprintf(stderr, "%s: slot %s: %s\n",
		get_pname(), slotstr, changer_resultstr);
    else if((errstr = tape_rdlabel(device, &datestamp, &label)) != NULL)
	fprintf(stderr, "%s: slot %s: %s\n", get_pname(), slotstr, errstr);
    else {
	fprintf(stderr, "%s: slot %s: date %-8s label %s",
		get_pname(), slotstr, datestamp, label);
	if(strcmp(label, FAKE_LABEL) != 0
	   && strcmp(label, searchlabel) != 0)
	    fprintf(stderr, " (wrong tape)\n");
	else {
	    fprintf(stderr, " (exact label match)\n");
	    if((errstr = tape_rewind(device)) != NULL) {
		fprintf(stderr,
			"%s: could not rewind %s: %s",
			get_pname(), device, errstr);
		amfree(errstr);
	    }
	    found = 1;
	    amfree(datestamp);
	    amfree(label);
	    return 1;
	}
    }
    amfree(datestamp);
    amfree(label);
    return 0;
}

void
load_label(
    int		argc,
    char **	argv)
{
    if(argc != 2)
	usage();

    searchlabel = argv[1];

    fprintf(stderr, "%s: scanning for tape with label %s\n",
	    get_pname(), searchlabel);

    found = 0;

    changer_find(NULL, scan_init, loadlabel_slot, searchlabel);

    if(found)
	fprintf(stderr, "%s: label %s is now loaded.\n",
		get_pname(), searchlabel);
    else
	fprintf(stderr, "%s: could not find label %s in tape rack.\n",
		get_pname(), searchlabel);
}


/* ---------------------------- */

int
show_init(
    void *	ud,
    int		rc,
    int		ns,
    int		bk,
    int		s)
{
    (void)ud;	/* Quiet unused parameter warning */
    (void)s;	/* Quiet unused parameter warning */

    if(rc) {
	error("could not get changer info: %s", changer_resultstr);
	/*NOTREACHED*/
    }

    nslots = ns;
    backwards = bk;
    return 0;
}

int
show_init_all(
    void *	ud,
    int		rc,
    int		ns,
    int		bk,
    int		s)
{
    int ret = show_init(NULL, rc, ns, bk, s);

    (void)ud;	/* Quiet unused parameter warning */

    fprintf(stderr, "%s: scanning all %d slots in tape-changer rack:\n",
	    get_pname(), nslots);
    return ret;
}

int
show_init_current(
    void *	ud,
    int		rc,
    int		ns,
    int		bk,
    int		s)
{
    int ret = show_init(NULL, rc, ns, bk, s);

    (void)ud;	/* Quiet unused parameter warning */

    fprintf(stderr, "%s: scanning current slot in tape-changer rack:\n",
	    get_pname());
    return ret;
}

int
show_slot(
    void *	ud,
    int		rc,
    char *	slotstr,
    char *	device)
{
    char *errstr;

    (void)ud;	/* Quiet unused parameter warning */

    if(rc > 1) {
	error("could not load slot %s: %s", slotstr, changer_resultstr);
	/*NOTREACHED*/
    }
    else if(rc == 1) {
	fprintf(stderr, "slot %s: %s\n", slotstr, changer_resultstr);
    }
    else if((errstr = tape_rdlabel(device, &datestamp, &label)) != NULL) {
	fprintf(stderr, "slot %s: %s\n", slotstr, errstr);
	amfree(errstr);
    } else {
	fprintf(stderr, "slot %s: date %-8s label %s\n",
		slotstr, datestamp, label);
    }
    amfree(datestamp);
    amfree(label);
    return 0;
}

void
show_current(
    int		argc,
    char **	argv)
{
    (void)argv;	/* Quiet unused parameter warning */

    if(argc != 1)
	usage();

    changer_current(NULL, show_init_current, show_slot);
}

void
show_slots(
    int		argc,
    char **	argv)
{
    (void)argv;	/* Quiet unused parameter warning */

    if(argc != 1)
	usage();

    changer_find(NULL, show_init_all, show_slot, NULL);
}


/* ---------------------------- */

void
amtape_taper_scan(
    int		argc,
    char **	argv)
{
    char *device = NULL;
    char *label = NULL;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    if((tp = lookup_last_reusable_tape(0)) == NULL)
	searchlabel = NULL;
    else
	searchlabel = stralloc(tp->label);

    tapedays	= getconf_int(CNF_TAPECYCLE);
    labelstr	= getconf_str(CNF_LABELSTR);
    found = 0;
    got_match = 0;

    fprintf(stderr, "%s: scanning for ", get_pname());
    if(searchlabel) fprintf(stderr, "tape label %s or ", searchlabel);
    fprintf(stderr, "a new tape.\n");

    taper_scan(searchlabel, &label, &datestamp,&device, FILE_taperscan_output_callback, stderr);

    fprintf(stderr, "%s: label %s is now loaded.\n",
            get_pname(), label);

    amfree(label);
    amfree(datestamp);
    amfree(device);
}

/* ---------------------------- */

void
show_device(
    int		argc,
    char **	argv)
{
    char *slot = NULL, *device = NULL;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    if(changer_loadslot("current", &slot, &device)) {
	error("Could not load current slot.\n");
	/*NOTREACHED*/
    }

    printf("%s\n", device);
    amfree(slot);
    amfree(device);
}

/* ---------------------------- */

int
update_one_slot(
    void *	ud,
    int		rc,
    char *	slotstr,
    char *	device)
{
    char *errstr = NULL;
    char *datestamp = NULL;
    char *label = NULL;

    (void)ud;	/* Quiet unused parameter warning */

    if(rc > 1)
	error("could not load slot %s: %s", slotstr, changer_resultstr);
    else if(rc == 1)
	fprintf(stderr, "slot %s: %s\n", slotstr, changer_resultstr);
    else if((errstr = tape_rdlabel(device, &datestamp, &label)) != NULL)
	fprintf(stderr, "slot %s: %s\n", slotstr, errstr);
    else {
	fprintf(stderr, "slot %s: date %-8s label %s\n",
		slotstr, datestamp, label);
	changer_label(slotstr, label);
    }
    amfree(errstr);
    amfree(datestamp);
    amfree(label);
    return 0;
}

void
update_labeldb(
    int		argc,
    char **	argv)
{
    (void)argv;	/* Quiet unused parameter warning */

    if(argc != 1)
	usage();

    changer_find(NULL, show_init_all, update_one_slot, NULL);
}
