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
 * $Id: rundump.c,v 1.33 2006/07/25 18:27:56 martinea Exp $
 *
 * runs DUMP program as root
 *
 * argv[0] is the rundump program name
 * argv[1] is the config name or NOCONFIG
 * argv[2] will be argv[0] of the DUMP program
 * ...
 */
#include "util.h"
#include "amanda.h"

int main(int argc, char **argv);

#if defined(VDUMP) || defined(XFSDUMP)
#  undef USE_RUNDUMP
#  define USE_RUNDUMP
#endif

#if !defined(USE_RUNDUMP)
#  define ERRMSG _("rundump not enabled on this system.\n")
#else
#  if !defined(DUMP) && !defined(VXDUMP) && !defined(VDUMP) && !defined(XFSDUMP)
#    define ERRMSG _("DUMP not available on this system.\n")
#  else
#    undef ERRMSG
#  endif
#endif

int
main(
    int		argc,
    char **	argv)
{
#ifndef ERRMSG
    char *dump_program;
    int i;
    char *e;
    char *cmdline;
#endif /* ERRMSG */

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

    set_pname("rundump");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);
    if (argc < 3) {
	error(_("Need at least 3 arguments\n"));
	/*NOTREACHED*/
    }

    dbprintf(_("version %s\n"), VERSION);

#ifdef ERRMSG							/* { */

    g_fprintf(stderr, ERRMSG);
    dbprintf("%s: %s", argv[0], ERRMSG);
    dbclose();
    return 1;

#else								/* } { */

#ifdef WANT_SETUID_CLIENT
    check_running_as(RUNNING_AS_CLIENT_LOGIN | RUNNING_AS_UID_ONLY);
    if (!become_root()) {
	error(_("error [%s could not become root (is the setuid bit set?)]\n"), get_pname());
	/*NOTREACHED*/
    }
#else
    check_running_as(RUNNING_AS_CLIENT_LOGIN);
#endif

    /* skip argv[0] */
    argc--;
    argv++;

    dbprintf(_("config: %s\n"), argv[0]);
    if (strcmp(argv[0], "NOCONFIG") != 0)
	dbrename(argv[0], DBG_SUBDIR_CLIENT);
    argc--;
    argv++;

#ifdef XFSDUMP

    if (strcmp(argv[0], "xfsdump") == 0)
        dump_program = XFSDUMP;
    else /* strcmp(argv[0], "xfsdump") != 0 */

#endif

#ifdef VXDUMP

    if (strcmp(argv[0], "vxdump") == 0)
        dump_program = VXDUMP;
    else /* strcmp(argv[0], "vxdump") != 0 */

#endif

#ifdef VDUMP

    if (strcmp(argv[0], "vdump") == 0)
	dump_program = VDUMP;
    else /* strcmp(argv[0], "vdump") != 0 */

#endif

#if defined(DUMP)
        dump_program = DUMP;
#else
# if defined(XFSDUMP)
        dump_program = XFSDUMP;
# else
#  if defined(VXDUMP)
	dump_program = VXDUMP;
#  else
        dump_program = "dump";
#  endif
# endif
#endif

    cmdline = stralloc(dump_program);
    for (i = 1; argv[i]; i++) {
	char *quoted;

	quoted = quote_string(argv[i]);
	cmdline = vstrextend(&cmdline, " ", quoted, NULL);
	amfree(quoted);
    }
    dbprintf(_("running: %s\n"), cmdline);
    amfree(cmdline);

    execve(dump_program, argv, safe_env());

    e = strerror(errno);
    dbprintf(_("failed (%s)\n"), e);
    dbclose();

    g_fprintf(stderr, _("rundump: could not exec %s: %s\n"), dump_program, e);
    return 1;
#endif								/* } */
}
