/*
 * Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "testutils.h"
#include "match.h"

/* NOTE: this is an incomplete set of tests for match.c */

/*
 * Tests
 */

/****
 * Test some host expressions
 */
static int
test_host_match(void)
{
    gboolean ok = TRUE;
    struct { char *expr, *str; gboolean should_match; } tests[] = {
	/* examples from amanda(8) */
	{ "hosta", "hosta", TRUE },
	{ "hosta", "foo.hosta.org", TRUE },
	{ "hosta", "hOsTA.domain.org", TRUE },
	{ "hosta", "hostb", FALSE },
	{ "host", "host", TRUE },
	{ "host", "hosta", FALSE },
	{ "host?", "hosta", TRUE },
	{ "host?", "host", FALSE },
	{ "ho*na", "hoina", TRUE },
	{ "ho*na", "ho.aina.org", FALSE },
	{ "ho**na", "hoina", TRUE },
	{ "ho**na", "ho.aina.org", TRUE },
	{ "^hosta", "hosta", TRUE },
	{ "^hosta", "hosta.foo.org", TRUE },
	{ "^hosta", "foo.hosta.org", FALSE },

	{ ".hosta.", "hosta", TRUE },
	{ ".hosta.", "foo.hosta", TRUE },
	{ ".hosta.", "hosta.org", TRUE },
	{ ".hosta.", "foo.hosta.org", TRUE },
	{ "/hosta", "hosta", FALSE },
	{ "/hosta", "foo.hosta", FALSE },
	{ "/hosta", "hosta.org", FALSE },
	{ "/hosta", "foo.hosta.org", FALSE },

	/* additional checks */
	{ "^hosta$", "hosta", TRUE },
	{ "^hosta$", "foo.hosta", FALSE },
	{ "^hosta$", "hosta.org", FALSE },
	{ "^hosta$", "foo.hosta.org", FALSE },

	{ "^lu.vis.ta$", "lu.vis.ta", TRUE },
	{ "^lu.vis.ta$", "lu-vis.ta", FALSE },
	{ "^lu.vis.ta$", "luvista", FALSE },
	{ "^lu.vis.ta$", "foo.lu.vis.ta", FALSE },
	{ "^lu.vis.ta$", "lu.vis.ta.org", FALSE },
	{ "^lu.vis.ta$", "foo.lu.vis.ta.org", FALSE },

	{ "mo[st]a", "mota", TRUE },
	{ "mo[st]a", "mosa", TRUE },
	{ "mo[st]a", "mosta", FALSE },
	{ "mo[!st]a", "mota", FALSE },
	{ "mo[!st]a", "moma", TRUE },
	{ "mo[!st]a", "momma", FALSE },

	{ NULL, NULL, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match_host(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched host expr %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched host expr %s\n",
			t->str, t->expr);
	    }
	}
    }

    return ok;
}

/****
 * Test some disk expressions
 */
static int
test_disk_match(void)
{
    gboolean ok = TRUE;
    struct { char *expr, *str; gboolean should_match; } tests[] = {
	/* examples from amanda(8) */
	{ "sda*", "/dev/sda1", TRUE },
	{ "sda*", "/dev/sda12", TRUE },
	{ "opt", "opt", TRUE },
	{ "opt", "/opt", TRUE },
	{ "opt", "/opt/foo", TRUE },
	{ "opt", "opt/foo", TRUE },
	{ "/", "/", TRUE },
	{ "/", "/opt", FALSE },
	{ "/", "/opt/var", FALSE },
	{ "/usr", "/", FALSE },
	{ "/usr", "/usr", TRUE },
	{ "/usr", "/usr/local", TRUE },
	{ "/usr$", "/", FALSE },
	{ "/usr$", "/usr", TRUE },
	{ "/usr$", "/usr/local", FALSE },
	{ "share", "//windows1/share", TRUE },
	{ "share", "//windows2/share", TRUE },
	{ "share", "\\\\windows1\\share", TRUE },
	{ "share", "\\\\windows2\\share", TRUE },
	{ "share*", "//windows/share1", TRUE },
	{ "share*", "//windows/share2", TRUE },
	{ "share*", "\\\\windows\\share1", TRUE },
	{ "share*", "\\\\windows\\share2", TRUE },
	{ "//windows/share", "//windows/share", TRUE },
	{ "//windows/share", "\\\\windows\\share", TRUE },

	/* and now things get murky */
	{ "\\\\windows\\share", "//windows/share", FALSE },
	{ "\\\\windows\\share", "\\\\windows\\share", FALSE },
	{ "\\\\\\\\windows\\\\share", "//windows/share", FALSE },
	{ "\\\\\\\\windows\\\\share", "\\\\windows\\share", TRUE },

	/* longer expressions */
	{ "local/var", "/local/var", TRUE },
	{ "local/var", "/opt/local/var", TRUE },
	{ "local/var", "/local/var/lib", TRUE },
	{ "local/var", "/local/usr/var", FALSE },

	/* trailing slashes */
	{ "/usr/bin", "/usr/bin", TRUE },
	{ "/usr/bin", "/usr/bin/", TRUE },
	{ "/usr/bin/", "/usr/bin", TRUE },
	{ "/usr/bin/", "/usr/bin/", TRUE },
	{ "/usr/bin", "/usr/bin//", TRUE },
	{ "/usr/bin//", "/usr/bin", FALSE },
	{ "/usr/bin//", "/usr/bin//", TRUE },

	/* quoting '/' is weird: it doesn't work on the leading slash.  Note that
	 * the documentation does not specify how to quote metacharacters in a host
	 * or disk expression. */
	{ "/usr\\/bin", "/usr/bin", TRUE },
	{ "^/usr\\/bin$", "/usr/bin", TRUE },
	{ "\\/usr\\/bin", "/usr/bin", FALSE },
	{ "^\\/usr\\/bin$", "/usr/bin", FALSE },

	{ NULL, NULL, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match_disk(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched disk expr %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched disk expr %s\n",
			t->str, t->expr);
	    }
	}
    }

    return ok;
}

/****
 * Test make_exact_host_expression
 */
static int
test_make_exact_host_expression(void)
{
    gboolean ok = TRUE;
    guint i, j;
    const char *test_strs[] = {
	"host",
	"host.org",
	"host.host.org",
	/* note that these will inter-match: */
	/*
	".host",
	".host.org",
	".host.host.org",
	"host.",
	"host.org.",
	"host.host.org.",
	*/
	"org",
	"^host",
	"host$",
	"^host$",
	"ho[s]t",
	"ho[!s]t",
	"ho\\st",
	"ho/st",
	"ho?t",
	"h*t",
	"h**t",
    };

    for (i = 0; i < G_N_ELEMENTS(test_strs); i++) {
	for (j = 0; j < G_N_ELEMENTS(test_strs); j++) {
	    char *expr = make_exact_host_expression(test_strs[i]);
	    gboolean matched = match_host(expr, test_strs[j]);
	    if (!!matched != !!(i == j)) {
		ok = FALSE;
		if (matched) {
		    g_fprintf(stderr, "expr %s for str %s unexpectedly matched %s\n",
			    expr, test_strs[i], test_strs[j]);
		} else {
		    g_fprintf(stderr, "expr %s for str %s should have matched %s\n",
			    expr, test_strs[i], test_strs[j]);
		}
	    }
	}
    }

    return ok;
}

/****
 * Test make_exact_disk_expression
 */
static int
test_make_exact_disk_expression(void)
{
    gboolean ok = TRUE;
    guint i, j;
    const char *test_strs[] = {
	"/disk",
	"/disk/disk",
	"d[i]sk",
	"d**k",
	"d*k",
	"d?sk",
	"d.sk",
	"d[!pqr]sk",
	"^disk",
	"disk$",
	"^disk$",
	/* these intermatch due to some special-casing */
	/*
	"//windows/share",
	"\\\\windows\\share",
	*/
    };

    for (i = 0; i < G_N_ELEMENTS(test_strs); i++) {
	for (j = 0; j < G_N_ELEMENTS(test_strs); j++) {
	    char *expr = make_exact_disk_expression(test_strs[i]);
	    gboolean matched = match_disk(expr, test_strs[j]);
	    if (!!matched != !!(i == j)) {
		ok = FALSE;
		if (matched) {
		    g_fprintf(stderr, "expr %s for str %s unexpectedly matched %s\n",
			    expr, test_strs[i], test_strs[j]);
		} else {
		    g_fprintf(stderr, "expr %s for str %s should have matched %s\n",
			    expr, test_strs[i], test_strs[j]);
		}
	    }
	}
    }

    return ok;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_host_match, 90),
	TU_TEST(test_disk_match, 90),
	TU_TEST(test_make_exact_host_expression, 90),
	TU_TEST(test_make_exact_disk_expression, 90),
	TU_END()
    };

    return testutils_run_tests(argc, argv, tests);
}

