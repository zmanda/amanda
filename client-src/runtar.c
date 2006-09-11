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
 * $Id: runtar.c,v 1.24 2006/08/25 11:41:31 martinea Exp $
 *
 * runs GNUTAR program as root
 *
 * argv[0] is the runtar program name
 * argv[1] is the config name or NOCONFIG
 * argv[2] will be argv[0] of the gtar program
 * ...
 */
#include "amanda.h"
#include "version.h"
#include "util.h"

int main(int argc, char **argv);

int
main(
    int		argc,
    char **	argv)
{
#ifdef GNUTAR
    int i;
    char *e;
    char *dbf;
#endif

    safe_fd(-1, 0);
    safe_cd();

    set_pname("runtar");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);
    if (argc < 3) {
	error("%s: Need at least 3 arguments\n", debug_prefix(NULL));
	/*NOTREACHED*/
    }

    dbprintf(("%s: version %s\n", debug_prefix(NULL), version()));

    if (strcmp(argv[3], "--create") != 0) {
	error("%s: Can only be used to create tar archives\n",
	      debug_prefix(NULL));
	/*NOTREACHED*/
    }

#ifndef GNUTAR

    fprintf(stderr,"gnutar not available on this system.\n");
    dbprintf(("%s: gnutar not available on this system.\n", argv[0]));
    dbclose();
    return 1;

#else

    /*
     * Print out version information for tar.
     */
    do {
	FILE *	version_file;
	char	version_buf[80];

	if ((version_file = popen(GNUTAR " --version 2>&1", "r")) != NULL) {
	    if (fgets(version_buf, (int)sizeof(version_buf), version_file) != NULL) {
		dbprintf((GNUTAR " version: %s\n", version_buf));
	    } else {
		if (ferror(version_file)) {
		    dbprintf((GNUTAR " version: Read failure: %s\n", strerror(errno)));
		} else {
		    dbprintf((GNUTAR " version: Read failure; EOF\n"));
		}
	    }
	} else {
	    dbprintf((GNUTAR " version: unavailable: %s\n", strerror(errno)));
	}
    } while(0);

    if(client_uid == (uid_t) -1) {
	error("error [cannot find user %s in passwd file]\n", CLIENT_LOGIN);
	/*NOTREACHED*/
    }

#ifdef FORCE_USERID
    if (getuid() != client_uid) {
	error("error [must be invoked by %s]\n", CLIENT_LOGIN);
	/*NOTREACHED*/
    }

    if (geteuid() != 0) {
	error("error [must be setuid root]\n");
	/*NOTREACHED*/
    }
#endif

#if !defined (DONT_SUID_ROOT)
    setuid(0);
#endif

    /* skip argv[0] */
    argc--;
    argv++;

    dbprintf(("config: %s\n", argv[0]));
    if (strcmp(argv[0], "NOCONFIG") != 0)
	dbrename(argv[0], DBG_SUBDIR_CLIENT);
    argc--;
    argv++;


    dbprintf(("running: %s: ",GNUTAR));
    for (i=0; argv[i]; i++) {
	char *quoted;

	quoted = quote_string(argv[i]);
	dbprintf(("'%s' ", quoted));
	amfree(quoted);
    }
    dbprintf(("\n"));
    dbf = dbfn();
    if (dbf) {
	dbf = stralloc(dbf);
    }
    dbclose();

    execve(GNUTAR, argv, safe_env());

    e = strerror(errno);
    dbreopen(dbf, "more");
    amfree(dbf);
    dbprintf(("execve of %s failed (%s)\n", GNUTAR, e));
    dbclose();

    fprintf(stderr, "runtar: could not exec %s: %s\n", GNUTAR, e);
    return 1;
#endif
}
