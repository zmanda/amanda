/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

gboolean tu_debugging_enabled = FALSE;

static gboolean run_all = TRUE;
static gboolean ignore_timeouts = FALSE;
static gboolean skip_fork = FALSE;
static gboolean only_one = FALSE;
static gboolean loop_forever = FALSE;
static guint64 occurrences = 1;

static void
alarm_hdlr(int sig G_GNUC_UNUSED)
{
    g_fprintf(stderr, "-- TEST TIMED OUT --\n");
    exit(1);
}

/*
 * Run a single test, accouting for the timeout (if timeouts are not ignored)
 * and output runtime information (in milliseconds) at the end of the run.
 * Output avg/min/max only if the number of runs is strictly greater than one.
 */

static gboolean run_one_test(TestUtilsTest *test)
{
    guint64 count = 0;
    gboolean ret = TRUE;
    const char *test_name = test->name;
    GTimer *timer;

    gdouble total = 0.0, thisrun, mintime = G_MAXDOUBLE, maxtime = G_MINDOUBLE;

    signal(SIGALRM, alarm_hdlr);

    timer = g_timer_new();

    while (count++ < occurrences) {
        if (!ignore_timeouts)
            alarm(test->timeout);

        g_timer_start(timer);
        ret = test->fn();
        g_timer_stop(timer);

        thisrun = g_timer_elapsed(timer, NULL);
        total += thisrun;
        if (mintime > thisrun)
            mintime = thisrun;
        if (maxtime < thisrun)
            maxtime = thisrun;

        if (!ret)
            break;
    }

    g_timer_destroy(timer);

    if (loop_forever)
        goto out;

    if (ret) {
        g_fprintf(stderr, " PASS %s (total: %.06f", test_name, total);
        if (occurrences > 1) {
            total /= (gdouble) occurrences;
            g_fprintf(stderr, ", avg/min/max: %.06f/%.06f/%.06f",
                total, mintime, maxtime);
        }
        g_fprintf(stderr, ")\n");
    } else
        g_fprintf(stderr, " FAIL %s (run %ju of %ju, after %.06f secs)\n",
            test_name, (uintmax_t)count, (uintmax_t)occurrences, total);

out:
    return ret;
}

/*
 * Call testfn in a forked process, such that any failures will trigger a
 * test failure, but allow the other tests to proceed. The only exception is if
 * -n is supplied at the command line, but in this case only one test is allowed
 * to run.
 */

static gboolean
callinfork(TestUtilsTest *test)
{
    pid_t pid;
    amwait_t status;
    gboolean result;

    if (skip_fork)
        result = run_one_test(test);
    else {
	switch (pid = fork()) {
	    case 0:	/* child */
		exit(run_one_test(test) ? 0 : 1);

	    case -1:
		perror("fork");
		exit(1);

	    default: /* parent */
		waitpid(pid, &status, 0);
		result = status == 0;
		break;
	}
    }

    return result;
}

static void
usage(
    TestUtilsTest *tests)
{
    printf("USAGE: <test-script> [options] [testname [testname [..]]]\n"
	"\n"
        "Options can be one of:\n"
        "\n"
	"\t-h: this message\n"
	"\t-d: print debugging messages\n"
	"\t-t: ignore timeouts\n"
        "\t-n: do not fork\n"
        "\t-c <count>: run each test <count> times instead of only once\n"
        "\t-l: loop the same test repeatedly (use with -n for leak checks)\n"
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
    gboolean success;

    /* first_parse the command line */
    while (argc > 1) {
	if (strcmp(argv[1], "-d") == 0) {
	    tu_debugging_enabled = TRUE;
	} else if (strcmp(argv[1], "-t") == 0) {
	    ignore_timeouts = TRUE;
	} else if (strcmp(argv[1], "-n") == 0) {
	    skip_fork = TRUE;
	    only_one = TRUE;
	} else if (strcmp(argv[1], "-l") == 0) {
	    loop_forever = TRUE;
	    only_one = TRUE;
	} else if (strcmp(argv[1], "-c") == 0) {
            char *p;
            argv++, argc--;
            occurrences = g_ascii_strtoull(argv[1], &p, 10);
            if (errno == ERANGE) {
                g_fprintf(stderr, "%s is out of range\n", argv[1]);
                exit(1);
            }
            if (*p) {
                g_fprintf(stderr, "The -c option expects a positive integer "
                    "as an argument, but \"%s\" isn't\n", argv[1]);
                exit(1);
            }
            if (occurrences == 0) {
                g_fprintf(stderr, "Sorry, I will not run tests 0 times\n");
                exit(1);
            }
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

	    run_all = FALSE;
	}

	argc--; argv++;
    }

    /*
     * Check whether the -c option has been given. In this case, -l must not be
     * specified at the same time.
     */
    if (occurrences > 1 && loop_forever) {
        g_fprintf(stderr, "-c and -l are incompatible\n");
        exit(1);
    }

    if (run_all) {
        for (t = tests; t->fn; t++)
            t->selected = 1;
    }

    /* check only_one */
    if (only_one) {
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
    success = TRUE;
    for (t = tests; t->fn; t++) {
        if (t->selected) {
	    do {
		success = callinfork(t) && success;
	    } while (loop_forever);
        }
    }

    return success ? 0 : 1;
}
