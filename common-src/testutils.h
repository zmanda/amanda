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

#ifndef TESTUTILS_H
#define TESTUTILS_H

/*
 * A library of utilities for writing 'make check'-based tests.
 *
 * Use this module like this:
 *   int test_one(void) {
 *	...
 *	tu_dbg("yep, worked: %p", someptr);
 *	...
 *	return TRUE;
 *   }
 *
 *   int main(int argc, char **argv)
 *   {
 *	TestUtilsTest tests[] = {
 *	    TU_TEST(test_one, 5),
 *	    TU_TEST(test_two, 6),
 *	    ...
 *	    TU_END()
 *	}
 *
 *	return testutils_run_tests(argc, argv, tests);
 *   }
 */

/*
 * Defining tests
 */

/* A test function, returning a boolean */
typedef int (*TestFunction)(void);

/* A struct for test functions */
typedef struct TestUtilsTest {
    TestFunction fn;
    char *name;
    int timeout;
    int selected;
} TestUtilsTest;

/* Macro to define a test array element */
#define TU_TEST(fn, to) { fn, #fn, to, FALSE }
#define TU_END() { NULL, NULL, 0, FALSE }

/*
 * Debugging
 */

/* Debugging macro taking printf arguments.  This is only enabled if the '-d' flag
 * is given on the commandline.  You can use g_debug, too, if you'd prefer. */
#define tu_dbg(...) if (tu_debugging_enabled) { g_fprintf(stderr, __VA_ARGS__); }

/* Is debugging enabled for this test run? (set internally) */
int tu_debugging_enabled;

/*
 * Main loop
 */

int testutils_run_tests(int argc, char **argv, TestUtilsTest *tests);

#endif /* TESTUTILS_H */
