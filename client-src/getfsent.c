/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2001 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * $Id: getfsent.c,v 1.38 2006/07/19 17:41:14 martinea Exp $
 *
 * generic version of code to read fstab
 */

#include "amanda.h"
#include "amutil.h"

#ifdef TEST
#  include <stdio.h>
#  include <sys/types.h>
#endif

#include "getfsent.h"

/*
 * You are in a twisty maze of passages, all alike.
 * Geesh.
 */

#if defined(HAVE_FSTAB_H) && !defined(HAVE_MNTENT_H) /* { */
/*
** BSD (GETFSENT_BSD)
*/
#define GETFSENT_TYPE "BSD (Ultrix, AIX)"

#include <fstab.h>

int
open_fstab(void)
{
    return setfsent();
}

void
close_fstab(void)
{
    endfsent();
}


int
get_fstab_nextentry(
    generic_fsent_t *	fsent)
{
    struct fstab *sys_fsent = getfsent();
    static char *xfsname = NULL, *xmntdir = NULL;
    static char *xfstype = NULL, *xmntopts = NULL;

    if(!sys_fsent)
	return 0;
    g_free(xfsname);
    fsent->fsname = xfsname = g_strdup(sys_fsent->fs_spec);
    g_free(xmntdir);
    fsent->mntdir = xmntdir = g_strdup(sys_fsent->fs_file);
    fsent->freq    = sys_fsent->fs_freq;
    fsent->passno  = sys_fsent->fs_passno;
#ifdef STATFS_ULTRIX
    g_free(xfstype);
    fsent->fstype = xfstype = g_strdup(sys_fsent->fs_name);
    g_free(xmntopts);
    fsent->mntopts = xmntopts = g_strdup(sys_fsent->fs_opts);
#else
#if defined(_AIX)
    g_free(xfstype);
    fsent->fstype = xfstype = g_strdup(_("unknown"));
    g_free(xmntopts);
    fsent->mntopts = xmntopts = g_strdup(sys_fsent->fs_type);
#else
    g_free(xfstype);
    fsent->fstype = xfstype = g_strdup(sys_fsent->fs_vfstype);
    g_free(xmntopts);
    fsent->mntopts = xmntopts = g_strdup(sys_fsent->fs_mntops);
#endif
#endif
    return 1;
}

#else
#if defined(HAVE_SYS_VFSTAB_H) /* } { */
/*
** SVR4 (GETFSENT_SOLARIS)
*/
#define GETFSENT_TYPE "SVR4 (Solaris)"

#include <sys/vfstab.h>

static FILE *fstabf = NULL;

int
open_fstab(void)
{
    close_fstab();
    return (fstabf = fopen(VFSTAB, "r")) != NULL;
}

void
close_fstab(void)
{
    if(fstabf)
	afclose(fstabf);
    fstabf = NULL;
}

int
get_fstab_nextentry(
    generic_fsent_t *	fsent)
{
    struct vfstab sys_fsent;

    memset(&sys_fsent, 0, sizeof(sys_fsent));
    if(getvfsent(fstabf, &sys_fsent) != 0)
	return 0;

    fsent->fsname  = sys_fsent.vfs_special;
    fsent->fstype  = sys_fsent.vfs_fstype;
    fsent->mntdir  = sys_fsent.vfs_mountp;
    fsent->mntopts = sys_fsent.vfs_mntopts;
    fsent->freq    = 1;	/* N/A */
    fsent->passno  = sys_fsent.vfs_fsckpass? atoi(sys_fsent.vfs_fsckpass) : 0;
    return 1;
}

#else
#  if defined(HAVE_MNTENT_H) /* } { */

/*
** System V.3 (GETFSENT_SVR3, GETFSENT_LINUX)
*/
#define GETFSENT_TYPE "SVR3 (NeXTstep, Irix, Linux, HP-UX)"

#include <mntent.h>

#if defined(HAVE_ENDMNTENT)
#define AMCLOSE_MNTENT(x)	endmntent(x)
#else
#define AMCLOSE_MNTENT(x)	fclose(x)
#endif

static FILE *fstabf1 = NULL;		/* /proc/mounts */
static FILE *fstabf2 = NULL;		/* MOUNTED */
static FILE *fstabf3 = NULL;		/* MNTTAB */

int
open_fstab(void)
{
    close_fstab();
#if defined(HAVE_SETMNTENT)
    fstabf1 = setmntent("/proc/mounts", "r");
# if defined(MOUNTED)
    fstabf2 = setmntent(MOUNTED, "r");
# endif
# if defined(MNTTAB)
    fstabf3 = setmntent(MNTTAB, "r");
# endif
#else
# if defined(MNTTAB)
    fstabf3 = fopen(MNTTAB, "r");
# endif
#endif
    return (fstabf1 != NULL || fstabf2 != NULL || fstabf3 != NULL);
}

void
close_fstab(void)
{
    if (fstabf1) {
	AMCLOSE_MNTENT(fstabf1);
	fstabf1 = NULL;
    }
    if (fstabf2) {
	AMCLOSE_MNTENT(fstabf2);
	fstabf2 = NULL;
    }
    if (fstabf3) {
	AMCLOSE_MNTENT(fstabf3);
	fstabf3 = NULL;
    }
}

int
get_fstab_nextentry(
    generic_fsent_t *	fsent)
{
    struct mntent *sys_fsent = NULL;

    if(fstabf1) {
	sys_fsent = getmntent(fstabf1);
	if(!sys_fsent) {
	    AMCLOSE_MNTENT(fstabf1);
	    fstabf1 = NULL;
	}
    }
    if(!sys_fsent && fstabf2) {
	sys_fsent = getmntent(fstabf2);
	if(!sys_fsent) {
	    AMCLOSE_MNTENT(fstabf2);
	    fstabf2 = NULL;
	}
    }
    if(!sys_fsent && fstabf3) {
	sys_fsent = getmntent(fstabf3);
	if(!sys_fsent) {
	    AMCLOSE_MNTENT(fstabf3);
	    fstabf3 = NULL;
	}
    }
    if(!sys_fsent) {
	return 0;
    }

    fsent->fsname  = sys_fsent->mnt_fsname;
    fsent->fstype  = sys_fsent->mnt_type;
    fsent->mntdir  = sys_fsent->mnt_dir;
    fsent->mntopts = sys_fsent->mnt_opts;
    fsent->freq    = sys_fsent->mnt_freq;
    fsent->passno  = sys_fsent->mnt_passno;
    return 1;
}

#  else
#    if defined(HAVE_SYS_MNTTAB_H) || defined(STATFS_SCO_OS5) /* } { */

/* we won't actually include mnttab.h, since it contains nothing useful.. */

#define GETFSENT_TYPE "SVR3 (Interactive UNIX)"

#include <stdio.h>
#include <string.h>
#include <ctype.h>

#define FSTAB "/etc/fstab"

static FILE *fstabf = NULL;

int
open_fstab(void)
{
    close_fstab();
    return (fstabf = fopen(FSTAB, "r")) != NULL;
}

void
close_fstab(void)
{
    if(fstabf)
	afclose(fstabf);
    fstabf = NULL;
}

static generic_fsent_t _fsent;

int
get_fstab_nextentry(
    generic_fsent_t *	fsent)
{
    static char *lfsnam = NULL;
    static char *opts = NULL;
    static char *cp = NULL;
    char *s;
    int ch;

    amfree(cp);
    for (; (cp = agets(fstabf)) != NULL; free(cp)) {
	if (cp[0] == '\0')
	    continue;
	fsent->fsname = strtok(cp, " \t");
	if ( fsent->fsname && *fsent->fsname != '#' )
	    break;
    }
    if (cp == NULL) return 0;

    fsent->mntdir = strtok((char *)NULL, " \t");
    fsent->mntopts = strtok((char *)NULL, " \t");
    if ( *fsent->mntopts != '-' )  {
	fsent->fstype = fsent->mntopts;
	fsent->mntopts = "rw";
    } else {
	fsent->fstype = "";
	if (g_str_equal(fsent->mntopts, "-r")) {
	    fsent->mntopts = "ro";
	}
    }
    if ((s = strchr(fsent->fstype, ',')) != NULL) {
	*s++ = '\0';
	strappend(fsent->mntopts, ",");
	strappend(fsent->mntopts, s);
    }

    g_free(lfsnam);
    lfsnam = g_strdup(fsent->fstype);
    s = lfsnam;
    while((ch = *s++) != '\0') {
	if(isupper(ch)) ch = tolower(ch);
	s[-1] = ch;
    }
    fsent->fstype = lfsnam;

    if (strncmp_const(fsent->fstype, "hs") == 0)
	fsent->fstype = "iso9660";

    fsent->freq = 0;
    fsent->passno = 0;

    return 1;
}

#    else
#      if defined(HAVE_MNTTAB_H) /* } { */

#define GETFSENT_TYPE "SVR3 (SCO UNIX)"

#include <mnttab.h>
#include <sys/fstyp.h>
#include <sys/statfs.h>

#define MNTTAB "/etc/mnttab"

/*
 * If these are defined somewhere please let me know.
 */

#define MNT_READONLY 0101
#define MNT_READWRITE 0100

static FILE *fstabf = NULL;

int
open_fstab(void)
{
    close_fstab();
    return (fstabf = fopen(MNTTAB, "r")) != NULL;
}

void
close_fstab(void)
{
    if(fstabf)
	afclose(fstabf);
    fstabf = NULL;
}

static generic_fsent_t _fsent;

int
get_fstab_nextentry(
    generic_fsent_t *fsent)
{
    struct statfs fsd;
    char typebuf[FSTYPSZ];
    static struct mnttab mnt;
    char *dp, *ep;

    if(!fread (&mnt, sizeof(mnt), 1, fstabf))
      return 0;

    fsent->fsname  = mnt.mt_dev;
    fsent->mntdir  = mnt.mt_filsys;
    fsent->fstype = "";

    if (statfs (fsent->mntdir, &fsd, sizeof(fsd), 0) != -1
        && sysfs (GETFSTYP, fsd.f_fstyp, typebuf) != -1) {
       dp = typebuf;
       ep = fsent->fstype = malloc(strlen(typebuf)+2);
       while (*dp)
            *ep++ = tolower(*dp++);
       *ep=0;
    }

    if ( mnt.mt_ro_flg == MNT_READONLY ) {
	fsent->mntopts = "ro";
    } else {
	fsent->mntopts = "rw";
    }

    fsent->freq = 0;
    fsent->passno = 0;
    return 1;
}

#      else /* } { */

#define GETFSENT_TYPE "undefined"

#      endif
#    endif
#  endif
#endif
#endif /* } */

#ifndef IGNORE_FSTAB
static int samefile(struct stat[3], struct stat *);

static int
samefile(
    struct stat stats[2],
    struct stat *estat)
{
  int i;
  for(i = 0; i < 2; ++i) {
    if (stats[i].st_dev == estat->st_dev &&
	stats[i].st_ino == estat->st_ino)
      return 1;
  }
  return 0;
}
#endif /* !IGNORE_FSTAB */

int
search_fstab(
     char *		name,
     generic_fsent_t *	fsent,
     int		check_dev)
{
#ifdef IGNORE_FSTAB
  /* There is no real mount table so this will always fail and
   * we are using GNU tar so we can just return here.
   */
  (void)name;		/* Quiet unused parameter warning */
  (void)fsent;		/* Quiet unused parameter warning */
  (void)check_dev;	/* Quiet unused parameter warning */
  return 0;
#else
  struct stat stats[2];
  char *fullname = NULL;
  int rc;

  if (!name)
    return 0;

  memset(stats, 0, sizeof(stats));
  stats[0].st_dev = stats[1].st_dev = (dev_t)-1;

  if (stat(name, &stats[0]) == -1)
    stats[0].st_dev = (dev_t)-1;

  /*
   * FIXME: who still uses non fully qualified device names today?
   */
  if (name[0] != '/') {
    fullname = g_strconcat(DEV_PREFIX, name, NULL);
    if (stat(fullname, &stats[1]) == -1)
      stats[1].st_dev = (dev_t)-1;
    amfree(fullname);
  }

  if (!open_fstab())
    return 0;

  rc = 0;
  while(get_fstab_nextentry(fsent)) {
    struct stat mntstat;
    struct stat fsstat;
    int smnt = -1, sfs = -1;

    if(fsent->mntdir != NULL)
       smnt = stat(fsent->mntdir, &mntstat);

    if(fsent->fsname != NULL) {
      sfs = stat(fsent->fsname, &fsstat);
      if(check_dev == 1 && sfs == -1)
	continue;
    }

    if((fsent->mntdir != NULL &&
	smnt != -1 &&
        samefile(stats, &mntstat)) || 
       (fsent->fsname != NULL &&
	sfs != -1 &&
        samefile(stats, &fsstat))) {
      rc = 1;
      break;
    }
  }
  close_fstab();
  return rc;
#endif /* !IGNORE_FSTAB */
}

int
is_local_fstype(
    generic_fsent_t *	fsent)
{
    if(fsent->fstype == NULL)	/* unknown, assume local */
	return 1;

    /* just eliminate fstypes known to be remote or unsavable */

    return !g_str_equal(fsent->fstype, "nfs") && /* NFS */
	   !g_str_equal(fsent->fstype, "afs") &&	/* Andrew Filesystem */
	   !g_str_equal(fsent->fstype, "swap") && /* Swap */
	   !g_str_equal(fsent->fstype, "iso9660") && /* CDROM */
	   !g_str_equal(fsent->fstype, "hs") && /* CDROM */
	   !g_str_equal(fsent->fstype, "piofs");	/* an AIX printer thing? */
}


char *
amname_to_devname(
    char *	str)
{
    generic_fsent_t fsent;

    if(search_fstab(str, &fsent, 1) && fsent.fsname != NULL)
	str = fsent.fsname;
    else if(search_fstab(str, &fsent, 0) && fsent.fsname != NULL)
	str = fsent.fsname;

    return g_strdup(str);
}

char *
amname_to_dirname(
    char *	str)
{
    generic_fsent_t fsent;

    if(search_fstab(str, &fsent, 1) && fsent.mntdir != NULL)
	str = fsent.mntdir;
    else if(search_fstab(str, &fsent, 0) && fsent.mntdir != NULL)
	str = fsent.mntdir;

    return g_strdup(str);
}

char *amname_to_fstype(
    char *	str)
{
    generic_fsent_t fsent;

    if (!search_fstab(str, &fsent, 1) && !search_fstab(str, &fsent, 0))
      return g_strdup("");

    return g_strdup(fsent.fstype);
}

#ifdef TEST

void print_entry(generic_fsent_t *fsent);

void
print_entry(
    generic_fsent_t *	fsent)
{
#define nchk(s)	((s)? (s) : "<NULL>")
    g_printf("%-20.20s %-14.14s %-7.7s %4d %5d %s\n",
	   nchk(fsent->fsname), nchk(fsent->mntdir), nchk(fsent->fstype),
	   fsent->freq, fsent->passno, nchk(fsent->mntopts));
}

int
main(
    int		argc,
    char **	argv)
{
    generic_fsent_t fsent;
    char *s;
    char *name = NULL;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("getfsent");

    dbopen(NULL);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    if(!open_fstab()) {
	g_fprintf(stderr, _("getfsent_test: could not open fstab\n"));
	return 1;
    }

    g_printf("getfsent (%s)\n",GETFSENT_TYPE);
    g_printf("l/r fsname               mntdir         fstype  freq pass# mntopts\n");
    while(get_fstab_nextentry(&fsent)) {
	g_printf("%c  ",is_local_fstype(&fsent)? 'l' : 'r');
	print_entry(&fsent);
    }
    g_printf("--------\n");

    close_fstab();

    g_free(name);
    name = g_strdup("/usr");
    if(search_fstab(name, &fsent, 1) || search_fstab(name, &fsent, 0)) {
	g_printf(_("Found %s mount for %s:\n"),
	       is_local_fstype(&fsent)? _("local") : _("remote"), name);
	print_entry(&fsent);
    }
    else 
	g_printf(_("Mount for %s not found\n"), name);

    g_free(name);
    name = g_strdup("/");
    if(search_fstab(name, &fsent, 1) || search_fstab(name, &fsent, 0)) {
	g_printf(_("Found %s mount for %s:\n"),
	       is_local_fstype(&fsent)? _("local") : _("remote"), name);
	print_entry(&fsent);
    }
    else 
	g_printf(_("Mount for %s not found\n"), name);

    g_free(name);
    name = g_strdup("/");
    s = amname_to_fstype(name);
    g_printf(_("fstype of `%s': %s\n"), name, s);
    amfree(s);
    g_free(name);
    name = g_strdup("/dev/root");
    s = amname_to_fstype(name);
    g_printf(_("fstype of `%s': %s\n"), name, s);
    amfree(s);
    g_free(name);
    name = g_strdup("/usr");
    s = amname_to_fstype(name);
    g_printf(_("fstype of `%s': %s\n"), name, s);
    amfree(s);
    g_free(name);
    name = g_strdup("c0t3d0s0");
    s = amname_to_fstype(name);
    g_printf(_("fstype of `%s': %s\n"), name, s);
    amfree(s);

    g_free(name);
    name = g_strdup("/tmp/foo");
    s = amname_to_devname(name);
    g_printf(_("device of `%s': %s\n"), name, s);
    amfree(s);
    s = amname_to_dirname(name);
    g_printf(_("dirname of `%s': %s\n"), name, s);
    amfree(s);
    s = amname_to_fstype(name);
    g_printf(_("fstype of `%s': %s\n"), name, s);
    amfree(s);

    g_free(name);
    name = g_strdup("./foo");
    s = amname_to_devname(name);
    g_printf(_("device of `%s': %s\n"), name, s);
    amfree(s);
    s = amname_to_dirname(name);
    g_printf(_("dirname of `%s': %s\n"), name, s);
    amfree(s);
    s = amname_to_fstype(name);
    g_printf(_("fstype of `%s': %s\n"), name, s);
    amfree(s);

    while (--argc > 0) {
	g_free(name);
	name = g_strdup(*++argv);
	s = amname_to_devname(name);
	g_printf(_("device of `%s': %s\n"), name, s);
	amfree(s);
	s = amname_to_dirname(name);
	g_printf(_("dirname of `%s': %s\n"), name, s);
	amfree(s);
	s = amname_to_fstype(name);
	g_printf(_("fstype of `%s': %s\n"), name, s);
	amfree(s);
    }

    amfree(name);

    dbclose();
    return 0;
}

#endif
