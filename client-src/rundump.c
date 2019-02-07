/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * $Id: rundump.c,v 1.33 2006/07/25 18:27:56 martinea Exp $
 *
 * runs DUMP program as root
 *
 * argv[0] is the rundump program name
 * argv[1] is the config name or NOCONFIG
 * argv[2] will be argv[0] of the DUMP program
 * ...
 */
#include "amanda.h"
#include "amutil.h"
#include "conffile.h"

int main(int argc, char **argv);
static void validate_dump_option(int argc, char ** argv);
static void validate_xfsdump_options(int argc, char ** argv);

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
    GPtrArray *array = g_ptr_array_new();
    gchar **strings;
    char  **env;
#endif /* ERRMSG */

    glib_init();

    if (argc > 1 && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("rundump-%s\n", VERSION);
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

    set_pname("rundump");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);
    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);

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
    if (!g_str_equal(argv[0], "NOCONFIG"))
	dbrename(argv[0], DBG_SUBDIR_CLIENT);
    argc--;
    argv++;

#ifdef XFSDUMP

    if (g_str_equal(argv[0], "xfsdump"))
        dump_program = XFSDUMP;
    else /* strcmp(argv[0], "xfsdump") != 0 */

#endif

#ifdef VXDUMP

    if (g_str_equal(argv[0], "vxdump"))
        dump_program = VXDUMP;
    else /* strcmp(argv[0], "vxdump") != 0 */

#endif

#ifdef VDUMP

    if (g_str_equal(argv[0], "vdump"))
	dump_program = VDUMP;
    else /* strcmp(argv[0], "vdump") != 0 */

#endif

#if defined(DUMP)
        dump_program = DUMP;
        validate_dump_option(argc, argv);
#else
# if defined(XFSDUMP)
        dump_program = XFSDUMP;
        validate_xfsdump_options(argc, argv);
# else
#  if defined(VXDUMP)
	dump_program = VXDUMP;
#  else
        dump_program = "dump";
        validate_dump_option(argc, argv);
#  endif
# endif
#endif

    /*
     * Build the array
     */	 
    g_ptr_array_add(array, g_strdup(dump_program));
	
    for (i = 1; argv[i]; i++) {
		g_ptr_array_add(array, quote_string(argv[i]));
    }

    g_ptr_array_add(array, NULL);
    strings = (gchar **)g_ptr_array_free(array, FALSE);

    cmdline = g_strjoinv(" ", strings);
    g_strfreev(strings);

    dbprintf(_("running: %s\n"), cmdline);
    amfree(cmdline);

    env = safe_env();
    execve(dump_program, argv, env);
    free_env(env);

    e = strerror(errno);
    dbprintf(_("failed (%s)\n"), e);
    dbclose();

    g_fprintf(stderr, _("rundump: could not exec %s: %s\n"), dump_program, e);
    return 1;
#endif								/* } */
}

void validate_dump_option(int argc, char ** argv)
{
	int c;
	int numargs = argc;
	while (numargs > 0)
	{
		c = getopt(argc, argv, "0123456789ab:cd:e:f:h:j:kmnqs:uvwyz:A:B:D:I:L:MQ:ST:W");
		switch (c) {
			case -1:
				optind++;
			break;
			case '?':
				//option is not valid
				error("error [%s invalid option: %s]\n", get_pname(), argv[optind-1]);
			break;
			// All this options takes another argument
			case 'b':
			case 'd':
			case 'e':
			case 'f':
			case 'h':
			case 'j':
			case 's':
			case 'z':
			case 'A':
			case 'B':
			case 'D':
			case 'I':
			case 'L':
			case 'Q':
			case 'T':
			{
				// get optarg and check it against NULL. If it is null, then return error.
				if (optarg == NULL) {
					error ("error [%s additional parameter is missing for option: %c]\n", get_pname(), c);
				}
				break;
			}
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
			case 'a':
			case 'c':
			case 'k':
			case 'm':
			case 'n':
			case 'q':
			case 'u':
			case 'v':
			case 'w':
			case 'y':
			case 'M':
			case 'S':
			case 'W':
			{
				break;
			}
			default:
				error ("error [%s invalid option: %c]\n", get_pname(), c);
			break;
		}
		numargs--;
	}
}
void validate_xfsdump_options(int argc, char ** argv)
{
	int c;
	int numargs = argc;
	while (numargs > 0)
	{
		c = getopt(argc, argv, "ab:d:ef:l:mop:qs:t:v:z:AB:DFI:JL:M:RT");
		switch (c) {
			case -1:
				optind++;
			break;
			case '?':
				//option is not valid
				error ("error [%s invalid option: %s]\n", get_pname(), argv[optind-1]);
			break;
			// All this options takes another argument
			case 'b':
			case 'd':
			case 'f':
			case 'l':
			case 'p':
			case 's':
			case 't':
			case 'v':
			case 'z':
			case 'B':
			case 'I':
			case 'L':
			case 'M':
			{
				// get optarg and check it against NULL. If it is null, then return error.
				if (optarg == NULL) {
					error ("error [%s additional parameter is missing for option: %c]\n", get_pname(), c);
				}
				break;
			}
			case 'a':
			case 'e':
			case 'm':
			case 'o':
			case 'q':
			case 'A':
			case 'D':
			case 'F':
			case 'J':
			case 'R':
			case 'T':
			{
				break;
			}
			default:
				error ("error [%s invalid option: %c]\n", get_pname(), c);
			break;
		}
		numargs--;
	}
}
