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
 * $Id: killpgrp.c,v 1.17 2006/07/25 18:27:56 martinea Exp $
 *
 * if it is the process group leader, it kills all processes in its
 * process group when it is killed itself.
 *
 * argv[0] is the killpgrp program name
 * argv[1] is the config name or NOCONFIG
 *
 */
#include "amanda.h"
#include "version.h"

#ifdef HAVE_GETPGRP
#ifdef GETPGRP_VOID
#define AM_GETPGRP() getpgrp()
#else
#define AM_GETPGRP() getpgrp(getpid())
#endif
#else
/* we cannot check it, so let us assume it is ok */
#define AM_GETPGRP() getpid()
#endif
 
int main(int argc, char **argv);
static void term_kill_soft(int sig);
static void term_kill_hard(int sig);

int main(
    int argc,
    char **argv)
{
    int ch;
    amwait_t status;

    safe_fd(-1, 0);
    safe_cd();

    set_pname("killpgrp");

    dbopen(DBG_SUBDIR_CLIENT);
    if (argc < 2) {
	error("%s: Need at least 2 arguments\n", debug_prefix(NULL));
	/*NOTREACHED*/
    }
    dbprintf(("%s: version %s\n", debug_prefix(NULL), version()));
    dbprintf(("config: %s\n", argv[1]));
    if (strcmp(argv[1], "NOCONFIG") != 0)
	dbrename(argv[1], DBG_SUBDIR_CLIENT);

    if(client_uid == (uid_t) -1) {
	error("error [cannot find user %s in passwd file]", CLIENT_LOGIN);
	/*NOTREACHED*/
    }

#ifdef FORCE_USERID
    if (getuid() != client_uid) {
	error("error [must be invoked by %s]", CLIENT_LOGIN);
	/*NOTREACHED*/
    }
    if (geteuid() != 0) {
	error("error [must be setuid root]");
	/*NOTREACHED*/
    }
#endif	/* FORCE_USERID */

#if !defined (DONT_SUID_ROOT)
    setuid(0);
#endif

    if (AM_GETPGRP() != getpid()) {
	error("error [must be the process group leader]");
	/*NOTREACHED*/
    }

    /* Consume any extranious input */
    signal(SIGTERM, term_kill_soft);

    do {
	ch = getchar();
	/* wait until EOF */
    } while (ch != EOF);

    term_kill_soft(0);

    for(;;) {
	if (wait(&status) != -1)
	    break;
	if (errno != EINTR) {
	    error("error [wait() failed: %s]", strerror(errno));
	    /*NOTREACHED*/
	}
    }

    /*@ignore@*/
    dbprintf(("child process exited with status %d\n", WEXITSTATUS(status)));

    return WEXITSTATUS(status);
    /*@end@*/
}

static void term_kill_soft(
    int sig)
{
    pid_t dumppid = getpid();
    int killerr;

    (void)sig;	/* Quiet unused parameter warning */

    signal(SIGTERM, SIG_IGN);
    signal(SIGALRM, term_kill_hard);
    alarm(3);
    /*
     * First, try to kill the dump process nicely.  If it ignores us
     * for three seconds, hit it harder.
     */
    dbprintf(("sending SIGTERM to process group %ld\n", (long) dumppid));
    killerr = kill(-dumppid, SIGTERM);
    if (killerr == -1) {
	dbprintf(("kill failed: %s\n", strerror(errno)));
    }
}

static void term_kill_hard(
    int sig)
{
    pid_t dumppid = getpid();
    int killerr;

    (void)sig;	/* Quiet unused parameter warning */

    dbprintf(("it won\'t die with SIGTERM, but SIGKILL should do\n"));
    dbprintf(("do\'t expect any further output, this will be suicide\n"));
    killerr = kill(-dumppid, SIGKILL);
    /* should never reach this point, but so what? */
    if (killerr == -1) {
	dbprintf(("kill failed: %s\n", strerror(errno)));
	dbprintf(("waiting until child terminates\n"));
    }
}
