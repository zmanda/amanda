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
 * $Id: sendbackup-gnutar.c,v 1.98 2006/07/25 18:35:21 martinea Exp $
 *
 * send backup data using GNU tar
 */

#include "amanda.h"
#include "sendbackup.h"
#include "amandates.h"
#include "clock.h"
#include "util.h"
#include "getfsent.h"			/* for amname_to_dirname lookup */
#include "conffile.h"

#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif

static amregex_t re_table[] = {
  /* tar prints the size in bytes */
  AM_SIZE_RE("^ *Total bytes written: [0-9][0-9]*", 1, 1),
  AM_NORMAL_RE("^Elapsed time:"),
  AM_NORMAL_RE("^Throughput"),

  /* GNU tar 1.13.17 will print this warning when (not) backing up a
     Unix named socket.  */
  AM_NORMAL_RE(": socket ignored$"),

  /* GNUTAR produces a few error messages when files are modified or
     removed while it is running.  They may cause data to be lost, but
     then they may not.  We shouldn't consider them NORMAL until
     further investigation.  */
#ifdef IGNORE_TAR_ERRORS
  AM_NORMAL_RE(": File .* shrunk by [0-9][0-9]* bytes, padding with zeros"),
  AM_NORMAL_RE(": Cannot add file .*: No such file or directory$"),
  AM_NORMAL_RE(": Error exit delayed from previous errors"),
#endif
  
  /* samba may produce these output messages */
  AM_NORMAL_RE("^[Aa]dded interface"),
  AM_NORMAL_RE("^session request to "),
  AM_NORMAL_RE("^tar: dumped [0-9][0-9]* (tar )?files"),

#if SAMBA_VERSION < 2
  AM_NORMAL_RE("^doing parameter"),
  AM_NORMAL_RE("^pm_process\\(\\)"),
  AM_NORMAL_RE("^adding IPC"),
  AM_NORMAL_RE("^Opening"),
  AM_NORMAL_RE("^Connect"),
  AM_NORMAL_RE("^Domain="),
  AM_NORMAL_RE("^max"),
  AM_NORMAL_RE("^security="),
  AM_NORMAL_RE("^capabilities"),
  AM_NORMAL_RE("^Sec mode "),
  AM_NORMAL_RE("^Got "),
  AM_NORMAL_RE("^Chose protocol "),
  AM_NORMAL_RE("^Server "),
  AM_NORMAL_RE("^Timezone "),
  AM_NORMAL_RE("^received"),
  AM_NORMAL_RE("^FINDFIRST"),
  AM_NORMAL_RE("^FINDNEXT"),
  AM_NORMAL_RE("^dos_clean_name"),
  AM_NORMAL_RE("^file"),
  AM_NORMAL_RE("^getting file"),
  AM_NORMAL_RE("^Rejected chained"),
  AM_NORMAL_RE("^nread="),
  AM_NORMAL_RE("^\\([0-9][0-9]* kb/s\\)"),
  AM_NORMAL_RE("^\\([0-9][0-9]*\\.[0-9][0-9]* kb/s\\)"),
  AM_NORMAL_RE("^[ \t]*[0-9][0-9]* \\([ \t]*[0-9][0-9]*\\.[0-9][0-9]* kb/s\\)"),
  AM_NORMAL_RE("^[ \t]*directory "),
  AM_NORMAL_RE("^load_client_codepage"),
#endif

#ifdef IGNORE_SMBCLIENT_ERRORS
  /* This will cause amanda to ignore real errors, but that may be
   * unavoidable when you're backing up system disks.  It seems to be
   * a safe thing to do if you know what you're doing.  */
  AM_NORMAL_RE("^ERRDOS - ERRbadshare opening remote file"),
  AM_NORMAL_RE("^ERRDOS - ERRbadfile opening remote file"),
  AM_NORMAL_RE("^ERRDOS - ERRnoaccess opening remote file"),
  AM_NORMAL_RE("^ERRSRV - ERRaccess setting attributes on file"),
  AM_NORMAL_RE("^ERRDOS - ERRnoaccess setting attributes on file"),
#endif

#if SAMBA_VERSION >= 2
  /* Backup attempt of nonexisting directory */
  AM_ERROR_RE("ERRDOS - ERRbadpath (Directory invalid.)"),
  AM_NORMAL_RE("^Domain="),
#endif

  /* catch-all: DMP_STRANGE is returned for all other lines */
  AM_STRANGE_RE(NULL)
};

extern char *efile;

int cur_level;
char *cur_disk;
time_t cur_dumptime;

static char *gnutar_list_dir = NULL;
static char *incrname = NULL;
/*
 *  doing similar to $ gtar | compression | encryption 
 */
static void
start_backup(
    dle_t      *dle,
    char       *host,
    int		dataf,
    int		mesgf,
    int		indexf)
{
    char tmppath[PATH_MAX];
    int dumpin, dumpout, compout;
    char *cmd = NULL;
    char *indexcmd = NULL;
    char *dirname = NULL;
    int l;
    char dumptimestr[80] = "UNUSED";
    struct tm *gmtm;
    amandates_t *amdates = NULL;
    time_t prev_dumptime = 0;
    char *error_pn = NULL;
    char *compopt  = NULL;
    char *encryptopt = skip_argument;
    char *tquoted;
    char *fquoted;
    char *qdisk;
    int infd, outfd;
    ssize_t nb;
    char buf[32768];
    char *amandates_file = NULL;
    am_level_t *alevel = (am_level_t *)dle->levellist->data;
    int      level  = alevel->level;

    error_pn = stralloc2(get_pname(), "-smbclient");

    qdisk = quote_string(dle->disk);
    dbprintf(_("start: %s:%s lev %d\n"), host, qdisk, level);

    g_fprintf(stderr, _("%s: start [%s:%s level %d]\n"),
	    get_pname(), host, qdisk, level);

     /*  apply client-side encryption here */
     if ( dle->encrypt == ENCRYPT_CUST ) {
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
	dbprintf(_("gnutar: pid %ld: %s"), (long)comppid, COMPRESS_PATH);
	if(compopt != skip_argument) {
	    dbprintf(_("pid %ld: %s %s\n"),
			(long)comppid, COMPRESS_PATH, compopt);
	} else {
	    dbprintf(_("pid %ld: %s\n"), (long)comppid, COMPRESS_PATH);
	}
     } else if (dle->compress == COMP_CUST) {
        compopt = skip_argument;
	comppid = pipespawn(dle->compprog, STDIN_PIPE, 0,
			    &dumpout, &compout, &mesgf,
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

    gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
    if (strlen(gnutar_list_dir) == 0)
	gnutar_list_dir = NULL;

#ifdef SAMBA_CLIENT							/* { */
    if (dle->device[0] == '/' && dle->device[1]=='/')
	amfree(incrname);
    else
#endif									/* } */
    if (gnutar_list_dir) {
	char *basename = NULL;
	char number[NUM_STR_SIZE];
	char *inputname = NULL;
	int baselevel;
	char *sdisk = sanitise_filename(dle->disk);

	basename = vstralloc(gnutar_list_dir,
			     "/",
			     host,
			     sdisk,
			     NULL);
	amfree(sdisk);

	g_snprintf(number, SIZEOF(number), "%d", level);
	incrname = vstralloc(basename, "_", number, ".new", NULL);
	unlink(incrname);

	/*
	 * Open the listed incremental file from the previous level.  Search
	 * backward until one is found.  If none are found (which will also
	 * be true for a level 0), arrange to read from /dev/null.
	 */
	baselevel = level;
	infd = -1;
	while (infd == -1) {
	    if (--baselevel >= 0) {
		g_snprintf(number, SIZEOF(number), "%d", baselevel);
		inputname = newvstralloc(inputname,
					 basename, "_", number, NULL);
	    } else {
		inputname = newstralloc(inputname, "/dev/null");
	    }
	    if ((infd = open(inputname, O_RDONLY)) == -1) {
		int save_errno = errno;
		char *qname = quote_string(inputname);

		dbprintf(_("gnutar: error opening '%s': %s\n"),
			  qname,
			  strerror(save_errno));
		if (baselevel < 0) {
		    error(_("error [opening '%s': %s]"), qname, strerror(save_errno));
		    /*NOTREACHED*/
		}
		amfree(qname);
	    }
	}

	/*
	 * Copy the previous listed incremental file to the new one.
	 */
	if ((outfd = open(incrname, O_WRONLY|O_CREAT, 0600)) == -1) {
	    error(_("error [opening '%s': %s]"), incrname, strerror(errno));
	    /*NOTREACHED*/
	}

	while ((nb = read(infd, &buf, SIZEOF(buf))) > 0) {
	    if (full_write(outfd, &buf, (size_t)nb) < (size_t)nb) {
		error(_("error [writing to '%s': %s]"), incrname,
		       strerror(errno));
		/*NOTREACHED*/
	    }
	}

	if (nb < 0) {
	    error(_("error [reading from '%s': %s]"), inputname, strerror(errno));
	    /*NOTREACHED*/
	}

	if (close(infd) != 0) {
	    error(_("error [closing '%s': %s]"), inputname, strerror(errno));
	    /*NOTREACHED*/
	}
	if (close(outfd) != 0) {
	    error(_("error [closing '%s': %s]"), incrname, strerror(errno));
	    /*NOTREACHED*/
	}

	tquoted = quote_string(incrname);
	if(baselevel >= 0) {
	    fquoted = quote_string(inputname);
	    dbprintf(_("doing level %d dump as listed-incremental from '%s' to '%s'\n"),
		     level, fquoted, tquoted);
	    amfree(fquoted);
	} else {
	    dbprintf(_("doing level %d dump as listed-incremental to '%s'\n"),
		     level, tquoted);
	}
	amfree(tquoted);
	amfree(inputname);
	amfree(basename);
    } else {
	/* no gnutar-listdir, so we're using amandates */

	/* find previous dump time, failing completely if there's a problem */
	amandates_file = getconf_str(CNF_AMANDATES);
	if(!start_amandates(amandates_file, 0)) {
	    error(_("error [opening %s: %s]"), amandates_file, strerror(errno));
	    /*NOTREACHED*/
	}

	amdates = amandates_lookup(dle->disk);

	prev_dumptime = EPOCH;
	for(l = 0; l < level; l++) {
	    if(amdates->dates[l] > prev_dumptime)
		prev_dumptime = amdates->dates[l];
	}

	finish_amandates();
	free_amandates();

	gmtm = gmtime(&prev_dumptime);
	g_snprintf(dumptimestr, SIZEOF(dumptimestr),
		    "%04d-%02d-%02d %2d:%02d:%02d GMT",
		    gmtm->tm_year + 1900, gmtm->tm_mon+1, gmtm->tm_mday,
		    gmtm->tm_hour, gmtm->tm_min, gmtm->tm_sec);

	dbprintf(_("gnutar: doing level %d dump from amandates-derived date: %s\n"),
		  level, dumptimestr);
    }

    dirname = amname_to_dirname(dle->device);

    cur_dumptime = time(0);
    cur_level = level;
    cur_disk = stralloc(dle->disk);
#ifdef GNUTAR
#  define PROGRAM_GNUTAR GNUTAR
#else
#  define PROGRAM_GNUTAR "tar"
#endif
    indexcmd = vstralloc(
			 PROGRAM_GNUTAR,
			 " -tf", " -",
			 " 2>/dev/null",
			 " | sed", " -e",
			 " \'s/^\\.//\'",
			 NULL);

#ifdef SAMBA_CLIENT							/* { */
    /* Use sambatar if the disk to back up is a PC disk */
    if (dle->device[0] == '/' && dle->device[1]=='/') {
	char *sharename = NULL, *user_and_password = NULL, *domain = NULL;
	char *share = NULL, *subdir = NULL;
	char *pwtext = NULL;
	char *taropt;
	int passwdf = -1;
	size_t lpass;
	size_t pwtext_len;
	char *pw_fd_env;

	parsesharename(dle->device, &share, &subdir);
	if (!share) {
	    amfree(share);
	    amfree(subdir);
	    set_pname(error_pn);
	    amfree(error_pn);
	    error(_("cannot parse disk entry %s for share/subdir"), qdisk);
	    /*NOTREACHED*/
	}
	if ((subdir) && (SAMBA_VERSION < 2)) {
	    amfree(share);
	    amfree(subdir);
	    set_pname(error_pn);
	    amfree(error_pn);
	    error(_("subdirectory specified for share %s but samba not v2 or better"), qdisk);
	    /*NOTREACHED*/
	}
	if ((user_and_password = findpass(share, &domain)) == NULL) {
	    if(domain) {
		memset(domain, '\0', strlen(domain));
		amfree(domain);
	    }
	    set_pname(error_pn);
	    amfree(error_pn);
	    error(_("error [invalid samba host or password not found?]"));
	    /*NOTREACHED*/
	}
	lpass = strlen(user_and_password);
	if ((pwtext = strchr(user_and_password, '%')) == NULL) {
	    memset(user_and_password, '\0', lpass);
	    amfree(user_and_password);
	    if(domain) {
		memset(domain, '\0', strlen(domain));
		amfree(domain);
	    }
	    set_pname(error_pn);
	    amfree(error_pn);
	    error(_("password field not \'user%%pass\' for %s"), qdisk);
	    /*NOTREACHED*/
	}
	*pwtext++ = '\0';
	pwtext_len = strlen(pwtext);
	if ((sharename = makesharename(share, 0)) == 0) {
	    memset(user_and_password, '\0', lpass);
	    amfree(user_and_password);
	    if(domain) {
		memset(domain, '\0', strlen(domain));
		amfree(domain);
	    }
	    set_pname(error_pn);
	    amfree(error_pn);
	    error(_("error [can't make share name of %s]"), share);
	    /*NOTREACHED*/
	}

	taropt = stralloc("-T");
	if(dle->exclude_file && dle->exclude_file->nb_element == 1) {
	    strappend(taropt, "X");
	}
#if SAMBA_VERSION >= 2
	strappend(taropt, "q");
#endif
	strappend(taropt, "c");
	if (level != 0) {
	    strappend(taropt, "g");
	} else if (dle->record) {
	    strappend(taropt, "a");
	}

	if (subdir) {
	    dbprintf(_("gnutar: backup of %s/%s\n"), sharename, subdir);
	} else {
	    dbprintf(_("gnutar: backup of %s\n"), sharename);
	}

	program->backup_name = program->restore_name = SAMBA_CLIENT;
	cmd = stralloc(program->backup_name);
	info_tapeheader(dle);

	start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

	if (pwtext_len > 0) {
	    pw_fd_env = "PASSWD_FD";
	} else {
	    pw_fd_env = "dummy_PASSWD_FD";
	}
	dumppid = pipespawn(cmd, STDIN_PIPE|PASSWD_PIPE, 0,
			    &dumpin, &dumpout, &mesgf,
			    pw_fd_env, &passwdf,
			    "smbclient",
			    sharename,
			    *user_and_password ? "-U" : skip_argument,
			    *user_and_password ? user_and_password : skip_argument,
			    "-E",
			    domain ? "-W" : skip_argument,
			    domain ? domain : skip_argument,
#if SAMBA_VERSION >= 2
			    subdir ? "-D" : skip_argument,
			    subdir ? subdir : skip_argument,
#endif
			    "-d0",
			    taropt,
			    "-",
			    dle->exclude_file && dle->exclude_file->nb_element == 1 ? dle->exclude_file->first->name : skip_argument,
			    NULL);
	if(domain) {
	    memset(domain, '\0', strlen(domain));
	    amfree(domain);
	}
	if(pwtext_len > 0 && full_write(passwdf, pwtext, pwtext_len) < pwtext_len) {
	    int save_errno = errno;

	    aclose(passwdf);
	    memset(user_and_password, '\0', lpass);
	    amfree(user_and_password);
	    set_pname(error_pn);
	    amfree(error_pn);
	    error(_("error [password write failed: %s]"), strerror(save_errno));
	    /*NOTREACHED*/
	}
	memset(user_and_password, '\0', lpass);
	amfree(user_and_password);
	aclose(passwdf);
	amfree(sharename);
	amfree(share);
	amfree(subdir);
	amfree(taropt);
	tarpid = dumppid;
    } else
#endif			/*end of samba */
    {

	int nb_exclude = 0;
	int nb_include = 0;
	GPtrArray *argv_ptr = g_ptr_array_new();
	char *file_exclude = NULL;
	char *file_include = NULL;

	if (dle->exclude_file) nb_exclude+=dle->exclude_file->nb_element;
	if (dle->exclude_list) nb_exclude+=dle->exclude_list->nb_element;
	if (dle->include_file) nb_include+=dle->include_file->nb_element;
	if (dle->include_list) nb_include+=dle->include_list->nb_element;

	if (nb_exclude > 0) file_exclude = build_exclude(dle, 0);
	if (nb_include > 0) file_include = build_include(dle, 0);

	cmd = vstralloc(amlibexecdir, "/", "runtar", NULL);
	info_tapeheader(dle);

	start_index(dle->create_index, dumpout, mesgf, indexf, indexcmd);

	g_ptr_array_add(argv_ptr, stralloc("runtar"));
	if (g_options->config)
	    g_ptr_array_add(argv_ptr, stralloc(g_options->config));
	else
	    g_ptr_array_add(argv_ptr, stralloc("NOCONFIG"));
#ifdef GNUTAR
	g_ptr_array_add(argv_ptr, stralloc(GNUTAR));
#else
	g_ptr_array_add(argv_ptr, stralloc("tar"));
#endif
	g_ptr_array_add(argv_ptr, stralloc("--create"));
	g_ptr_array_add(argv_ptr, stralloc("--file"));
	g_ptr_array_add(argv_ptr, stralloc("-"));
	g_ptr_array_add(argv_ptr, stralloc("--directory"));
	canonicalize_pathname(dirname, tmppath);
	g_ptr_array_add(argv_ptr, stralloc(tmppath));
	g_ptr_array_add(argv_ptr, stralloc("--one-file-system"));
	if (gnutar_list_dir && incrname) {
	    g_ptr_array_add(argv_ptr, stralloc("--listed-incremental"));
	    g_ptr_array_add(argv_ptr, stralloc(incrname));
	} else {
	    g_ptr_array_add(argv_ptr, stralloc("--incremental"));
	    g_ptr_array_add(argv_ptr, stralloc("--newer"));
	    g_ptr_array_add(argv_ptr, stralloc(dumptimestr));
	}
#ifdef ENABLE_GNUTAR_ATIME_PRESERVE
	/* --atime-preserve causes gnutar to call
	 * utime() after reading files in order to
	 * adjust their atime.  However, utime()
	 * updates the file's ctime, so incremental
	 * dumps will think the file has changed. */
	g_ptr_array_add(argv_ptr, stralloc("--atime-preserve"));
#endif
	g_ptr_array_add(argv_ptr, stralloc("--sparse"));
	g_ptr_array_add(argv_ptr, stralloc("--ignore-failed-read"));
	g_ptr_array_add(argv_ptr, stralloc("--totals"));

	if(file_exclude) {
	    g_ptr_array_add(argv_ptr, stralloc("--exclude-from"));
	    g_ptr_array_add(argv_ptr, stralloc(file_exclude));
	}

	if(file_include) {
	    g_ptr_array_add(argv_ptr, stralloc("--files-from"));
	    g_ptr_array_add(argv_ptr, stralloc(file_include));
	}
	else {
	    g_ptr_array_add(argv_ptr, stralloc("."));
	}
	    g_ptr_array_add(argv_ptr, NULL);
	dumppid = pipespawnv(cmd, STDIN_PIPE, 0,
			     &dumpin, &dumpout, &mesgf,
			     (char **)argv_ptr->pdata);
	tarpid = dumppid;
	amfree(file_exclude);
	amfree(file_include);
	g_ptr_array_free_full(argv_ptr);
    }
    dbprintf(_("gnutar: %s: pid %ld\n"), cmd, (long)dumppid);

    amfree(qdisk);
    amfree(dirname);
    amfree(cmd);
    amfree(indexcmd);
    amfree(error_pn);

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
    int		goterror)
{
    char *amandates_file = NULL;

    if(dle->record && !goterror) {
	if (incrname != NULL && strlen(incrname) > 4) {
	    char *nodotnew;
	
	    nodotnew = stralloc(incrname);
	    nodotnew[strlen(nodotnew)-4] = '\0';
	    if (rename(incrname, nodotnew)) {
		g_fprintf(stderr, _("%s: warning [renaming %s to %s: %s]\n"), 
			get_pname(), incrname, nodotnew, strerror(errno));
	    }
	    amfree(nodotnew);
	    amfree(incrname);
	}

	/* update the amandates file */
	amandates_file = getconf_str(CNF_AMANDATES);
	if(start_amandates(amandates_file, 1)) {
	    amandates_updateone(cur_disk, cur_level, cur_dumptime);
	    finish_amandates();
	    free_amandates();
	} else {
	    /* failure is only fatal if we didn't get a gnutar-listdir */
	    char *gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
	    if (!gnutar_list_dir || !*gnutar_list_dir) {
		error(_("error [opening %s for writing: %s]"), amandates_file, strerror(errno));
		/* NOTREACHED */
	    } else {
		g_debug(_("non-fatal error opening '%s' for writing: %s]"),
			amandates_file, strerror(errno));
	    }
	}
    }
}

backup_program_t gnutar_program = {
  "GNUTAR",
#ifdef GNUTAR
  GNUTAR, GNUTAR,
#else
  "gtar", "gtar",
#endif
  re_table, start_backup, end_backup
};
