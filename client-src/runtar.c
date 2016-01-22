/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "amutil.h"
#include "conffile.h"

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
    char *cmdline;
    GPtrArray *array = g_ptr_array_new();
    gchar **strings;
    char **new_argv;
    char **env;
#endif
    int good_option;

    if (argc > 1 && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("runtar-%s\n", VERSION);
	return (0);
    }

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

    set_pname("runtar");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);
    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);

    if (argc < 3) {
	error(_("Need at least 3 arguments\n"));
	/*NOTREACHED*/
    }

    dbprintf(_("version %s\n"), VERSION);

    if (!g_str_equal(argv[3], "--create")) {
	error(_("Can only be used to create tar archives\n"));
	/*NOTREACHED*/
    }

#ifndef GNUTAR

    g_fprintf(stderr,_("gnutar not available on this system.\n"));
    dbprintf(_("%s: gnutar not available on this system.\n"), argv[0]);
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
		dbprintf(_(GNUTAR " version: %s\n"), version_buf);
	    } else {
		if (ferror(version_file)) {
		    dbprintf(_(GNUTAR " version: Read failure: %s\n"), strerror(errno));
		} else {
		    dbprintf(_(GNUTAR " version: Read failure; EOF\n"));
		}
	    }
	} else {
	    dbprintf(_(GNUTAR " version: unavailable: %s\n"), strerror(errno));
	}
    } while(0);

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
    if (!g_str_equal(argv[0], "NOCONFIG"))
	dbrename(argv[0], DBG_SUBDIR_CLIENT);
    argc--;
    argv++;

    new_argv = g_new0(char *, argc+1);

    new_argv[0] = g_strdup_printf("%s", argv[0]);
    g_ptr_array_add(array, g_strdup(GNUTAR));
    good_option = 0;
    for (i = 1; argv[i]; i++) {
	if (good_option <= 0) {
	    if (g_str_has_prefix(argv[i],"--rsh-command") ||
		g_str_has_prefix(argv[i],"--to-command") ||
		g_str_has_prefix(argv[i],"--info-script") ||
		g_str_has_prefix(argv[i],"--new-volume-script") ||
		g_str_has_prefix(argv[i],"--rmt-command") ||
		g_str_has_prefix(argv[i],"--use-compress-program")) {
		/* Filter potential malicious option */
		good_option = 0;
	    } else if (g_str_has_prefix(argv[i],"--create") ||
		g_str_has_prefix(argv[i],"--totals") ||
		g_str_has_prefix(argv[i],"--dereference") ||
		g_str_has_prefix(argv[i],"--no-recursion") ||
		g_str_has_prefix(argv[i],"--one-file-system") ||
		g_str_has_prefix(argv[i],"--incremental") ||
		g_str_has_prefix(argv[i],"--atime-preserve") ||
		g_str_has_prefix(argv[i],"--sparse") ||
		g_str_has_prefix(argv[i],"--ignore-failed-read") ||
		g_str_has_prefix(argv[i],"--numeric-owner")) {
		/* Accept theses options */
		good_option++;
	    } else if (g_str_has_prefix(argv[i],"--blocking-factor") ||
		g_str_has_prefix(argv[i],"--file") ||
		g_str_has_prefix(argv[i],"--directory") ||
		g_str_has_prefix(argv[i],"--exclude") ||
		g_str_has_prefix(argv[i],"--transform") ||
		g_str_has_prefix(argv[i],"--listed-incremental") ||
		g_str_has_prefix(argv[i],"--newer") ||
		g_str_has_prefix(argv[i],"--exclude-from") ||
		g_str_has_prefix(argv[i],"--files-from")) {
		/* Accept theses options with the following argument */
		good_option += 2;
	    } else if (argv[i][0] != '-') {
		good_option++;
	    }
	}
	if (good_option <= 0) {
	    error("error [%s invalid option: %s]", get_pname(), argv[i]);
	}
        g_ptr_array_add(array, quote_string(argv[i]));
	new_argv[i] = g_strdup_printf("%s", argv[i]);
	good_option--;
    }

    g_ptr_array_add(array, NULL);
    strings = (gchar **)g_ptr_array_free(array, FALSE);

    cmdline = g_strjoinv(" ", strings);
    g_strfreev(strings);

    dbprintf(_("running: %s\n"), cmdline);
    amfree(cmdline);

    dbf = dbfn();
    if (dbf) {
	dbf = g_strdup(dbf);
    }
    dbclose();

    env = safe_env();
    execve(GNUTAR, new_argv, env);
    free_env(env);

    e = strerror(errno);
    dbreopen(dbf, "more");
    amfree(dbf);
    dbprintf(_("execve of %s failed (%s)\n"), GNUTAR, e);
    dbclose();

    g_fprintf(stderr, _("runtar: could not exec %s: %s\n"), GNUTAR, e);
    return 1;
#endif
}
