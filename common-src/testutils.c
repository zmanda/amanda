/*
 * Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "testutils.h"

int tu_debugging_enabled = FALSE;

static void
alarm_hdlr(int sig G_GNUC_UNUSED)
{
    g_fprintf(stderr, "-- TEST TIMED OUT --\n");
    exit(1);
}

/* Call testfn in a forked process, such that any failures will trigger a
 * test failure, but allow the other tests to proceed.
 */
static int
callinfork(TestUtilsTest *test, int ignore_timeouts, gboolean skip_fork)
{
    pid_t pid;
    amwait_t status;
    gboolean result;

    if (skip_fork) {
	/* kill the test after a bit */
	signal(SIGALRM, alarm_hdlr);
	if (!ignore_timeouts) alarm(test->timeout);

	result = test->fn();
    } else {
	switch (pid = fork()) {
	    case 0:	/* child */
		/* kill the test after a bit */
		signal(SIGALRM, alarm_hdlr);
		if (!ignore_timeouts) alarm(test->timeout);

		result = test->fn();
		exit(result? 0:1);

	    case -1:
		perror("fork");
		exit(1);

	    default: /* parent */
		waitpid(pid, &status, 0);
		result = status == 0;
		break;
	}
    }

    if (result) {
	g_fprintf(stderr, " PASS %s\n", test->name);
    } else {
	g_fprintf(stderr, " FAIL %s\n", test->name);
    }

    return result;
}

static void
usage(
    TestUtilsTest *tests)
{
    printf("USAGE: <test-script> [-d] [-h] [testname [testname [..]]]\n"
	"\n"
	"\t-h: this message\n"
	"\t-d: print debugging messages\n"
	"\t-t: ignore timeouts\n"
        "\t-n: do not fork\n"
	"\n"
	"If no test names are specified, all tests are run.  Available tests:\n"
	"\n");
    while (tests->fn) {
	printf("\t%s\n", tests->name);
	tests++;
    }
}

static void
ignore_debug_messages(
	    const gchar *log_domain G_GNUC_UNUSED,
	    GLogLevelFlags log_level G_GNUC_UNUSED,
	    const gchar *message G_GNUC_UNUSED,
	    gpointer user_data G_GNUC_UNUSED)
{
}

int
testutils_run_tests(
    int argc,
    char **argv,
    TestUtilsTest *tests)
{
    TestUtilsTest *t;
    int run_all = 1;
    int success;
    int ignore_timeouts = 0;
    gboolean skip_fork = FALSE;

    /* first_parse the command line */
    while (argc > 1) {
	if (strcmp(argv[1], "-d") == 0) {
	    tu_debugging_enabled = TRUE;
	} else if (strcmp(argv[1], "-t") == 0) {
	    ignore_timeouts = TRUE;
	} else if (strcmp(argv[1], "-n") == 0) {
	    skip_fork = TRUE;
	} else if (strcmp(argv[1], "-h") == 0) {
	    usage(tests);
	    return 1;
	} else {
	    int found = 0;

	    for (t = tests; t->fn; t++) {
		if (strcmp(argv[1], t->name) == 0) {
		    found = 1;
		    t->selected = 1;
		    break;
		}
	    }

	    if (!found) {
		g_fprintf(stderr, "Test '%s' not found\n", argv[1]);
		return 1;
	    }

	    run_all = 0;
	}

	argc--; argv++;
    }

    if (run_all) {
        for (t = tests; t->fn; t++)
            t->selected = 1;
    }

    /* if skip_fork is given, only one test can run */
    if (skip_fork) {
        int num_tests = 0;
        for (t = tests; t->fn; t++) {
            if (t->selected)
                num_tests++;
        }

        if (num_tests > 1) {
            g_fprintf(stderr, "Only run one test with '-n'\n");
            return 1;
        }
    }

    /* Make sure g_critical and g_error will exit */
    g_log_set_always_fatal(G_LOG_LEVEL_ERROR |  G_LOG_LEVEL_CRITICAL);

    /* and silently drop debug messages unless we're debugging */
    if (!tu_debugging_enabled) {
	g_log_set_handler(NULL, G_LOG_LEVEL_DEBUG, ignore_debug_messages, NULL);
    }

    /* Now actually run the tests */
    success = 1;
    for (t = tests; t->fn; t++) {
        if (t->selected) {
	    success = callinfork(t, ignore_timeouts, skip_fork) && success;
        }
    }

    return success? 0:1;
}
