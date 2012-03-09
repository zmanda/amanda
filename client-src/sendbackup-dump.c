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
 * $Id: sendbackup-dump.c,v 1.90 2006/07/25 18:10:07 martinea Exp $
 *
 * send backup data using BSD dump
 */

#include "amanda.h"
#include "sendbackup.h"
#include "getfsent.h"
#include "clock.h"

#define LEAF_AND_DIRS "sed -e \'\ns/^leaf[ \t]*[0-9]*[ \t]*\\.//\nt\n/^dir[ \t]/ {\ns/^dir[ \t]*[0-9]*[ \t]*\\.//\ns%$%/%\nt\n}\nd\n\'"

static amregex_t re_table[] = {
  /* the various encodings of dump size */
  /* this should also match BSDI pre-3.0's buggy dump program, that
     produced doubled DUMP: DUMP: messages */
  AM_SIZE_RE("DUMP: [0-9][0-9]* tape blocks", 1024, 1),
  AM_SIZE_RE("dump: Actual: [0-9][0-9]* tape blocks", 1024, 1),
  AM_SIZE_RE("backup: There are [0-9][0-9]* tape blocks on [0-9][0-9]* tapes",
	     1024, 1),
  AM_SIZE_RE("backup: [0-9][0-9]* tape blocks on [0-9][0-9]* tape\\(s\\)",
	     1024, 1),
  AM_SIZE_RE("backup: [0-9][0-9]* 1k blocks on [0-9][0-9]* volume\\(s\\)",
	     1024, 1),
  AM_SIZE_RE("DUMP: [0-9][0-9]* blocks \\([0-9][0-9]*KB\\) on [0-9][0-9]* volume",
	     512, 1),
  AM_SIZE_RE("DUMP: [0-9][0-9]* blocks \\([0-9][0-9]*\\.[0-9][0-9]*MB\\) on [0-9][0-9]* volume",
	     512, 1),
  AM_SIZE_RE("DUMP: [0-9][0-9]* blocks \\([0-9][0-9]*KB\\)",
	     1024, 2),
  AM_SIZE_RE("DUMP: [0-9][0-9]* blocks \\([0-9][0-9]*\\.[0-9][0-9]*MB\\)",
	     1048576, 2),
  AM_SIZE_RE("DUMP: [0-9][0-9]* blocks", 512, 1),
  AM_SIZE_RE("DUMP: [0-9][0-9]* bytes were dumped", 1, 1),
  /* OSF's vdump */
  AM_SIZE_RE("vdump: Dumped  [0-9][0-9]* of [0-9][0-9]* bytes", 1, 1),
  /* DU 4.0a dump */
  AM_SIZE_RE("dump: Actual: [0-9][0-9]* blocks output to pipe", 1024, 1),
  /* DU 4.0 vdump */
  AM_SIZE_RE("dump: Dumped  [0-9][0-9]* of [0-9][0-9]* bytes", 1, 1),
  /* HPUX dump */
  AM_SIZE_RE("DUMP: [0-9][0-9]* KB actual output", 1024, 1),
  /* HPUX 10.20 and above vxdump */
  AM_SIZE_RE("vxdump: [0-9][0-9]* tape blocks", 1024, 1),
  /* UnixWare vxdump */
  AM_SIZE_RE("vxdump: [0-9][0-9]* blocks", 1024, 1),
  /* SINIX vxdump */
  AM_SIZE_RE("   VXDUMP: [0-9][0-9]* blocks", 512, 1),
  /* SINIX ufsdump */
  AM_SIZE_RE("   UFSDUMP: [0-9][0-9]* blocks", 512, 1),
  /* Irix 6.2 xfs dump */
  AM_SIZE_RE("xfsdump: media file size [0-9][0-9]* bytes", 1, 1),
  /* NetApp dump */
  AM_SIZE_RE("DUMP: [0-9][0-9]* KB", 1024, 1),

  /* strange dump lines */
  AM_STRANGE_RE("should not happen"),
  AM_STRANGE_RE("Cannot open"),
  AM_STRANGE_RE("[Ee]rror"),
  AM_STRANGE_RE("[Ff]ail"),
  /* XXX add more ERROR entries here by scanning dump sources? */

  /* any blank or non-strange DUMP: lines are marked as normal */
  AM_NORMAL_RE("^ *DUMP:"),
  AM_NORMAL_RE("^dump:"),					/* OSF/1 */
  AM_NORMAL_RE("^vdump:"),					/* OSF/1 */
  AM_NORMAL_RE("^ *vxdump:"),					/* HPUX10 */
  AM_NORMAL_RE("^ *vxfs *vxdump:"),				/* Solaris */
  AM_NORMAL_RE("^Dumping .* to stdout"),			/* Sol vxdump */
  AM_NORMAL_RE("^xfsdump:"),					/* IRIX xfs */
  AM_NORMAL_RE("^ *VXDUMP:"),					/* Sinix */
  AM_NORMAL_RE("^ *UFSDUMP:"),					/* Sinix */

#ifdef VDUMP	/* this is for OSF/1 3.2's vdump for advfs */
  AM_NORMAL_RE("^The -s option is ignored"),			/* OSF/1 */
  AM_NORMAL_RE("^path"),					/* OSF/1 */
  AM_NORMAL_RE("^dev/fset"),					/* OSF/1 */
  AM_NORMAL_RE("^type"),					/* OSF/1 */
  AM_NORMAL_RE("^advfs id"),					/* OSF/1 */
  AM_NORMAL_RE("^[A-Z][a-z][a-z] [A-Z][a-z][a-z] .[0-9] [0-9]"), /* OSF/1 */
#endif

  AM_NORMAL_RE("^backup:"),					/* AIX */
  AM_NORMAL_RE("^        Use the umount command to unmount the filesystem"),

  AM_NORMAL_RE("^[ \t]*$"),

  /* catch-all; DMP_STRANGE is returned for all other lines */
  AM_STRANGE_RE(NULL)
};

static void start_backup(dle_t *dle, char *host,
			 int dataf, int mesgf, int indexf);
static void end_backup(dle_t *dle, int status);

/*
 *  doing similar to $ dump | compression | encryption
 */

static void
start_backup(
    dle_t      *dle,
    char       *host,
    int		dataf,
    int		mesgf,
    int		indexf)
{
    int dumpin, dumpout, compout;
    char *dumpkeys = NULL;
    char *device = NULL;
    char *fstype = NULL;
    char *cmd = NULL;
    char *cmdX = NULL;
    char *indexcmd = NULL;
    char level_str[NUM_STR_SIZE];
    char *compopt  = NULL;
    char *encryptopt = skip_argument;
    char *qdisk;
    char *config;
    am_level_t *alevel = (am_level_t *)dle->levellist->data;
    int      level  = alevel->level;

    g_snprintf(level_str, SIZEOF(level_str), "%d", level);

    qdisk = quote_string(dle->disk);
    dbprintf(_("start: %s:%s lev %d\n"), host, qdisk, level);

    g_fprintf(stderr, _("%s: start [%s:%s level %d]\n"),
	    get_pname(), host, qdisk, level);
    amfree(qdisk);

    /*  apply client-side encryption here */
    if (dle->encrypt == ENCRYPT_CUST ) {
        encpid = pipespawn(dle->clnt_encrypt, STDIN_PIPE, 0,
	                   &compout, &dataf, &mesgf,
	                   dle->clnt_encrypt, encryptopt, NULL);
        dbprintf(_("gnutar: pid %ld: %s\n"), (long)encpid, dle->clnt_encrypt);
    } else {
        compout = dataf;
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
			    &dumpout, &compout, &mesgf,
			    COMPRESS_PATH, compopt, NULL);
	dbprintf(_("dump: pid %ld: %s"), (long)comppid, COMPRESS_PATH);
	if(compopt != skip_argument) {
	    dbprintf(" %s", compopt);
	}
	dbprintf("\n");
     } else if (dle->compress == COMP_CUST) {
        compopt = skip_argument;
	comppid = pipespawn(dle->compprog, STDIN_PIPE, 0,
			    &dumpout, &compout, &mesgf,
			    dle->compprog, compopt, NULL);
	dbprintf(_("gnutar-cust: pid %ld: %s"),
		(long)comppid, dle->compprog);
	if(compopt != skip_argument) {
	    dbprintf(" %s", compopt);
	}
	dbprintf("\n");
    } else {
	dumpout = compout;
	comppid = -1;
    }

    /* invoke dump */
    device = amname_to_devname(dle->device);
    fstype = amname_to_fstype(dle->device);

    dbprintf(_("dumping device '%s' with '%s'\n"), device, fstype);

#if defined(USE_RUNDUMP) || !defined(DUMP)
    cmd = vstralloc(amlibexecdir, "/", "rundump", NULL);
    cmdX = cmd;
    if (g_options->config)
	config = g_options->config;
    else
	config = "NOCONFIG";
#else
    cmd = stralloc(DUMP);
    cmdX = skip_argument;
    config = skip_argument;
#endif

#ifndef AIX_BACKUP					/* { */
    /* normal dump */
#ifdef XFSDUMP						/* { */
#ifdef DUMP						/* { */
    if (strcmp(amname_to_fstype(dle->device), "xfs") == 0)
#else							/* } { */
    if (1)
#endif							/* } */
    {
        char *progname = cmd = newvstralloc(cmd, amlibexecdir, "/", "rundump",
					    NULL);
	cmdX = cmd;
	if (g_options->config)
	    config = g_options->config;
	else
	    config = "NOCONFIG";

	program->backup_name  = XFSDUMP;
	program->restore_name = XFSRESTORE;

	indexcmd = vstralloc(XFSRESTORE,
			     " -t",
			     " -v", " silent",
			     " -",
			     " 2>/dev/null",
			     " | sed",
			     " -e", " \'s/^/\\//\'",
			     NULL);
	info_tapeheader(dle);

	start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

	dumpkeys = stralloc(level_str);
	dumppid = pipespawn(progname, STDIN_PIPE, 0,
			    &dumpin, &dumpout, &mesgf,
			    cmdX, config,
			    "xfsdump",
			    !dle->record ? "-J" : skip_argument,
			    "-F",
			    "-l", dumpkeys,
			    "-",
			    device,
			    NULL);
    }
    else
#endif							/* } */
#ifdef VXDUMP						/* { */
#ifdef DUMP
    if (strcmp(amname_to_fstype(dle->device), "vxfs") == 0)
#else
    if (1)
#endif
    {
#ifdef USE_RUNDUMP
        char *progname = cmd = newvstralloc(cmd, amlibexecdir, "/", "rundump",
					    NULL);
	cmdX = cmd;
	if (g_options->config)
	    config = g_options->config;
	else
	    config = "NOCONFIG";
#else
	char *progname = cmd = newvstralloc(cmd, VXDUMP, NULL);
	cmdX = skip_argument;
	config = skip_argument;
#endif
	program->backup_name  = VXDUMP;
	program->restore_name = VXRESTORE;

	dumpkeys = vstralloc(level_str,
			     !dle->record ? "" : "u",
			     "s",
			     "f",
			     NULL);

	indexcmd = vstralloc(VXRESTORE,
			     " -tvf", " -",
			     " 2>/dev/null",
			     " | ",
			     LEAF_AND_DIRS,
			     NULL);
	info_tapeheader(dle);

	start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

	dumppid = pipespawn(progname, STDIN_PIPE, 0,
			    &dumpin, &dumpout, &mesgf, 
			    cmdX, config,
			    "vxdump",
			    dumpkeys,
			    "1048576",
			    "-",
			    device,
			    NULL);
    }
    else
#endif							/* } */

#ifdef VDUMP						/* { */
#ifdef DUMP
    if (strcmp(amname_to_fstype(dle->device), "advfs") == 0)
#else
    if (1)
#endif
    {
        cmd = newvstralloc(cmd, amlibexecdir, "/", "rundump", NULL);
	cmdX = cmd;
	if (g_options->config)
	    config = g_options->config;
	else
	    config = "NOCONFIG";
	device = newstralloc(device, amname_to_dirname(dle->device));
	program->backup_name  = VDUMP;
	program->restore_name = VRESTORE;

	dumpkeys = vstralloc(level_str,
			     !dle->record ? "" : "u",
			     "b",
			     "f",
			     NULL);

	indexcmd = vstralloc(VRESTORE,
			     " -tvf", " -",
			     " 2>/dev/null",
			     " | ",
			     "sed -e \'\n/^\\./ {\ns/^\\.//\ns/, [0-9]*$//\ns/^\\.//\ns/ @-> .*$//\nt\n}\nd\n\'",
			     NULL);
	info_tapeheader(dle);

	start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

	dumppid = pipespawn(cmd, STDIN_PIPE, 0,
			    &dumpin, &dumpout, &mesgf, 
			    cmdX, config,
			    "vdump",
			    dumpkeys,
			    "60",
			    "-",
			    device,
			    NULL);
    }
    else
#endif							/* } */

    {
#ifndef RESTORE
#define RESTORE "restore"
#endif

#ifdef HAVE_HONOR_NODUMP
#  define PARAM_HONOR_NODUMP "h"
#else
#  define PARAM_HONOR_NODUMP ""
#endif

#ifdef __FreeBSD__
# if defined(__FreeBSD_version) && (__FreeBSD_version >= 500043)
#  define FREEBSD_EXTRA_KEYS "bL"
# else
#  define FREEBSD_EXTRA_KEYS "b"
# endif
#else
# define FREEBSD_EXTRA_KEYS ""
#endif

	dumpkeys = vstralloc(level_str,
			     !dle->record ? "" : "u",
			     FREEBSD_EXTRA_KEYS,
			     "s",
			     PARAM_HONOR_NODUMP,
			     "f",
			     NULL);

	indexcmd = vstralloc(RESTORE,
			     " -tvf", " -",
			     " 2>&1",
			     /* not to /dev/null because of DU's dump */
			     " | ",
			     LEAF_AND_DIRS,
			     NULL);
	info_tapeheader(dle);

	start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

	dumppid = pipespawn(cmd, STDIN_PIPE, 0,
			    &dumpin, &dumpout, &mesgf, 
			    cmdX, config,
			    "dump",
			    dumpkeys,
#ifdef __FreeBSD__
			    "64",
#endif
			    "1048576",
#ifdef HAVE_HONOR_NODUMP
			    "0",
#endif
			    "-",
			    device,
			    NULL);
    }
#else							/* } { */
    /* AIX backup program */
    dumpkeys = vstralloc("-",
			 level_str,
			 !dle->record ? "" : "u",
			 "f",
			 NULL);

    indexcmd = vstralloc(RESTORE,
			 " -B",
			 " -tvf", " -",
			 " 2>/dev/null",
			 " | ",
			 LEAF_AND_DIRS,
			 NULL);
    info_tapeheader(dle);

    start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

    dumppid = pipespawn(cmd, STDIN_PIPE, 0,
			&dumpin, &dumpout, &mesgf, 
			cmdX, config,
			"backup",
			dumpkeys,
			"-",
			device,
			NULL);
#endif							/* } */

    amfree(dumpkeys);
    amfree(fstype);
    amfree(device);
    amfree(cmd);
    amfree(indexcmd);

    /* close the write ends of the pipes */

    aclose(dumpin);
    aclose(dumpout);
    aclose(compout);
    aclose(dataf);
    aclose(mesgf);
    if (dle->create_index)
	aclose(indexf);
}

static void
end_backup(
    dle_t      *dle,
    int		status)
{
    (void)dle;
    (void)status;	/* Quiet unused parameter warning */

    /* don't need to do anything for dump */
}

backup_program_t dump_program = {
  "DUMP",
#ifdef DUMP
  DUMP
#else
  "dump"
#endif
  ,
  RESTORE
  ,
  re_table, start_backup, end_backup
};
