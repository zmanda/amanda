/*
 * Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

/*
 * Tests
 */

static gboolean
test_validate_regexp(void)
{
    gboolean ok = TRUE;
    struct {
	char *regexp;
	gboolean should_validate;
    } tests[] = {
	{ ".*", TRUE },
	{ "*", FALSE },
	{ "[abc", FALSE },
	{ "(abc", FALSE },
	{ "{1,}", FALSE },
	{ NULL, FALSE },
    }, *t;

    for (t = tests; t->regexp; t++) {
	char *validated_err = validate_regexp(t->regexp);
	if (!validated_err != !!t->should_validate) {
	    ok = FALSE;
	    if (t->should_validate) {
		g_fprintf(stderr, "should have validated regular expr %s: %s\n",
			t->regexp, validated_err);
	    } else {
		g_fprintf(stderr, "unexpectedly validated regular expr %s\n",
			t->regexp);
	    }
	}
    }

    return ok;
}

static gboolean
test_match(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match, should_match_no_newline;
    } tests[] = {
	/* literal, unanchored matching */
	{ "a", "a", TRUE, TRUE },
	{ "a", "A", FALSE, FALSE },
	{ "a", "ab", TRUE, TRUE },
	{ "a", "ba", TRUE, TRUE },
	{ "a", "bab", TRUE, TRUE },

	/* dot */
	{ ".", "", FALSE, FALSE },
	{ ".", "a", TRUE, TRUE },
	{ "..", "a", FALSE, FALSE },
	{ "..", "bc", TRUE, TRUE },

	/* brackets */
	{ "[abc]", "xbx", TRUE, TRUE },
	{ "[abc]", "xyz", FALSE, FALSE },
	{ "[^abc]", "cba", FALSE, FALSE },
	{ "[^abc]", "xyz", TRUE, TRUE },
	{ "[a-c]", "b", TRUE, TRUE },
	{ "[^a-c]", "-", TRUE, TRUE },
	{ "[1-9-]", "-", TRUE, TRUE },
	{ "[ab\\-cd]", "-", FALSE, FALSE }, /* NOTE! */

	/* anchors */
	{ "^xy", "xyz", TRUE, TRUE },
	{ "^xy", "wxyz", FALSE, FALSE },
	{ "yz$", "xyz", TRUE, TRUE },
	{ "yz$", "yza", FALSE, FALSE },
	{ "^123$", "123", TRUE, TRUE },
	{ "^123$", "0123", FALSE, FALSE },
	{ "^123$", "1234", FALSE, FALSE },

	/* capture groups */
	{ "([a-c])([x-y])", "pqaxyr", TRUE, TRUE },
	{ "([a-c])([x-y])", "paqrxy", FALSE, FALSE },
	{ "([a-c])/\\1", "a/b", FALSE, FALSE },
	{ "([a-c])/\\1", "c/c", TRUE, TRUE },

	/* * */
	{ ">[0-9]*<", "><", TRUE, TRUE },
	{ ">[0-9]*<", ">3<", TRUE, TRUE },
	{ ">[0-9]*<", ">34<", TRUE, TRUE },
	{ ">[0-9]*<", ">345<", TRUE, TRUE },
	{ ">[0-9]*<", ">x<", FALSE, FALSE },

	/* | */
	{ ":(abc|ABC);", ":abc;", TRUE, TRUE },
	{ ":(abc|ABC);", ":ABC;", TRUE, TRUE },
	{ ":(abc|ABC);", ":abcBC;", FALSE, FALSE },

	/* + */
	{ ">[0-9]+<", "><", FALSE, FALSE },
	{ ">[0-9]+<", ">3<", TRUE, TRUE },
	{ ">[0-9]+<", ">34<", TRUE, TRUE },
	{ ">[0-9]+<", ">345<", TRUE, TRUE },
	{ ">[0-9]+<", ">x<", FALSE, FALSE },

	/* { .. } */
	{ ">[0-9]{0,1}<", "><", TRUE, TRUE },
	{ ">[0-9]{0,1}<", ">9<", TRUE, TRUE },
	{ ">[0-9]{0,1}<", ">98<", FALSE, FALSE },
	{ ">[0-9]{2,3}<", "><", FALSE, FALSE },
	{ ">[0-9]{2,3}<", ">5<", FALSE, FALSE },
	{ ">[0-9]{2,3}<", ">55<", TRUE, TRUE },
	{ ">[0-9]{2,3}<", ">555<", TRUE, TRUE },
	{ ">[0-9]{2,3}<", ">5555<", FALSE, FALSE },

	/* quoting metacharacters */
	{ "\\\\", "\\", TRUE, TRUE },
	{ "\\,", ",", TRUE, TRUE },
	{ "\\[", "[", TRUE, TRUE },
	{ "\\*", "*", TRUE, TRUE },
	{ "\\?", "?", TRUE, TRUE },
	{ "\\+", "+", TRUE, TRUE },
	{ "\\.", ".", TRUE, TRUE },
	{ "\\|", "|", TRUE, TRUE },
	{ "\\^", "^", TRUE, TRUE },
	{ "\\$", "$", TRUE, TRUE },

	/* differences between match and match_no_newline */
	{ "x.y", "x\ny", FALSE, TRUE },
	{ "x[^yz]y", "x\ny", FALSE, TRUE },
	{ "^y", "x\ny", TRUE, FALSE },
	{ "x$", "x\ny", TRUE, FALSE },

	{ NULL, NULL, FALSE, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched regular expr %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched regular expr %s\n",
			t->str, t->expr);
	    }
	}

	matched = match_no_newline(t->expr, t->str);
	if (!!matched != !!t->should_match_no_newline) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched (no_newline) regular expr %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched (no_newline) regular expr %s\n",
			t->str, t->expr);
	    }
	}
    }

    return ok;
}

static gboolean
test_validate_glob(void)
{
    gboolean ok = TRUE;
    struct {
	char *glob;
	gboolean should_validate;
    } tests[] = {
	{ "foo.*", TRUE },
	{ "*.txt", TRUE },
	{ "x[abc]y", TRUE },
	{ "x[!abc]y", TRUE },
	{ "[abc", FALSE },
	{ "[!abc", FALSE },
	{ "??*", TRUE },
	{ "**?", TRUE }, /* legal, but weird */
	{ "foo\\", FALSE }, /* un-escaped \ is illegal */
	{ "foo\\\\", TRUE }, /* but escaped is OK */
	{ "(){}+.^$|", TRUE }, /* funny characters OK */
	{ "/usr/bin/*", TRUE }, /* filename seps are OK */
	{ NULL, FALSE },
    }, *t;

    for (t = tests; t->glob; t++) {
	char *validated_err = validate_glob(t->glob);
	if (!validated_err != !!t->should_validate) {
	    ok = FALSE;
	    if (t->should_validate) {
		g_fprintf(stderr, "should have validated glob %s: %s\n",
			t->glob, validated_err);
	    } else {
		g_fprintf(stderr, "unexpectedly validated glob %s\n",
			t->glob);
	    }
	}
    }

    return ok;
}

static gboolean
test_glob_to_regex(void)
{
    gboolean ok = TRUE;
    struct { char *glob, *regex; } tests[] = {
	{ "abc", "^abc$" },
	{ "*.txt", "^[^/]*\\.txt$" },
	{ "?.txt", "^[^/]\\.txt$" },
	{ "?*.txt", "^[^/][^/]*\\.txt$" },
	{ "foo.[tT][xX][tT]", "^foo\\.[tT][xX][tT]$" },
	{ "foo.[tT][!yY][tT]", "^foo\\.[tT][^yY][tT]$" },
	{ "foo\\\\", "^foo\\\\$" },
	{ "(){}+.^$|", "^\\(\\)\\{\\}\\+\\.\\^\\$\\|$" },
	{ "/usr/bin/*", "^/usr/bin/[^/]*$" },
	{ NULL, NULL },
    }, *t;

    for (t = tests; t->glob; t++) {
	char *regex = glob_to_regex(t->glob);
	if (0 != strcmp(regex, t->regex)) {
	    ok = FALSE;
	    g_fprintf(stderr, "glob_to_regex(\"%s\") returned \"%s\"; expected \"%s\"\n",
		    t->glob, regex, t->regex);
	}
    }

    return ok;
}

static gboolean
test_match_glob(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match;
    } tests[] = {
	/* literal, unanchored matching */
	{ "a", "a", TRUE },

	{ "abc", "abc", TRUE },
	{ "abc", "abcd", FALSE },
	{ "abc", "dabc", FALSE },
	{ "abc", "/usr/bin/abc", FALSE },

	{ "*.txt", "foo.txt", TRUE },
	{ "*.txt", ".txt", TRUE },
	{ "*.txt", "txt", FALSE },

	{ "?.txt", "X.txt", TRUE },
	{ "?.txt", ".txt", FALSE },
	{ "?.txt", "XY.txt", FALSE },

	{ "?*.txt", ".txt", FALSE },
	{ "?*.txt", "a.txt", TRUE },
	{ "?*.txt", "aa.txt", TRUE },
	{ "?*.txt", "aaa.txt", TRUE },

	{ "foo.[tT][xX][tT]", "foo.txt", TRUE },
	{ "foo.[tT][xX][tT]", "foo.TXt", TRUE },
	{ "foo.[tT][xX][tT]", "foo.TXT", TRUE },
	{ "foo.[tT][xX][tT]", "foo.TaT", FALSE },

	{ "foo.[tT][!yY][tT]", "foo.TXt", TRUE },
	{ "foo.[tT][!yY][tT]", "foo.TXT", TRUE },
	{ "foo.[tT][!yY][tT]", "foo.TyT", FALSE },

	{ "foo\\\\", "foo", FALSE },
	{ "foo\\\\", "foo\\", TRUE },
	{ "foo\\\\", "foo\\\\", FALSE },

	{ "(){}+.^$|", "(){}+.^$|", TRUE },

	{ "/usr/bin/*", "/usr/bin/tar", TRUE },
	{ "/usr/bin/*", "/usr/bin/local/tar", FALSE },
	{ "/usr/bin/*", "/usr/sbin/tar", FALSE },
	{ "/usr/bin/*", "/opt/usr/bin/tar", FALSE },

	{ "/usr?bin", "/usr/bin", FALSE },
	{ "/usr*bin", "/usr/bin", FALSE },

	{ NULL, NULL, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match_glob(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched glob %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched glob %s\n",
			t->str, t->expr);
	    }
	}
    }

    return ok;
}

static gboolean
test_match_tar(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match;
    } tests[] = {
	/* literal, unanchored matching */
	{ "a", "a", TRUE },

	{ "abc", "abc", TRUE },
	{ "abc", "abcd", FALSE },
	{ "abc", "dabc", FALSE },
	{ "abc", "/usr/bin/abc", TRUE },

	{ "*.txt", "foo.txt", TRUE },
	{ "*.txt", ".txt", TRUE },
	{ "*.txt", "txt", FALSE },

	{ "?.txt", "X.txt", TRUE },
	{ "?.txt", ".txt", FALSE },
	{ "?.txt", "XY.txt", FALSE },

	{ "?*.txt", ".txt", FALSE },
	{ "?*.txt", "a.txt", TRUE },
	{ "?*.txt", "aa.txt", TRUE },
	{ "?*.txt", "aaa.txt", TRUE },

	{ "foo.[tT][xX][tT]", "foo.txt", TRUE },
	{ "foo.[tT][xX][tT]", "foo.TXt", TRUE },
	{ "foo.[tT][xX][tT]", "foo.TXT", TRUE },
	{ "foo.[tT][xX][tT]", "foo.TaT", FALSE },

	{ "foo.[tT][!yY][tT]", "foo.TXt", TRUE },
	{ "foo.[tT][!yY][tT]", "foo.TXT", TRUE },
	{ "foo.[tT][!yY][tT]", "foo.TyT", FALSE },

	{ "foo\\\\", "foo", FALSE },
	{ "foo\\\\", "foo\\", TRUE },
	{ "foo\\\\", "foo\\\\", FALSE },

	{ "(){}+.^$|", "(){}+.^$|", TRUE },

	{ "/usr/bin/*", "/usr/bin/tar", TRUE },
	{ "/usr/bin/*", "/usr/bin/local/tar", TRUE }, /* different from match_glob */
	{ "/usr/bin/*", "/usr/sbin/tar", FALSE },
	{ "/usr/bin/*", "/opt/usr/bin/tar", FALSE },

	{ "/usr?bin", "/usr/bin", FALSE },
	{ "/usr*bin", "/usr/bin", TRUE }, /* different from match_glob */

	/* examples from the amgtar manpage */
	{ "./temp-files", "./temp-files", TRUE },
	{ "./temp-files", "./temp-files/foo", TRUE },
	{ "./temp-files", "./temp-files/foo/bar", TRUE },
	{ "./temp-files", "./temp-files.bak", FALSE },
	{ "./temp-files", "./backup/temp-files", FALSE },

	{ "./temp-files/", "./temp-files", FALSE },
	{ "./temp-files/", "./temp-files/", TRUE },
	{ "./temp-files/", "./temp-files/foo", FALSE },
	{ "./temp-files/", "./temp-files/foo/bar", FALSE },
	{ "./temp-files/", "./temp-files.bak", FALSE },

	{ "/temp-files/", "./temp-files", FALSE },
	{ "/temp-files/", "./temp-files/", FALSE },
	{ "/temp-files/", "./temp-files/foo", FALSE },
	{ "/temp-files/", "./temp-files/foo/bar", FALSE },
	{ "/temp-files/", "./temp-files.bak", FALSE },

	{ "./temp-files/*", "./temp-files", FALSE },
	{ "./temp-files/*", "./temp-files/", TRUE },
	{ "./temp-files/*", "./temp-files/foo", TRUE },
	{ "./temp-files/*", "./temp-files/foo/bar", TRUE },

	{ "temp-files", "./my/temp-files", TRUE },
	{ "temp-files", "./my/temp-files/bar", TRUE },
	{ "temp-files", "./temp-files", TRUE },
	{ "temp-files", "./her-temp-files", FALSE },
	{ "temp-files", "./her/old-temp-files", FALSE },
	{ "temp-files", "./her/old-temp-files/bar", FALSE },

	{ "generated-*", "./my/generated-xyz", TRUE },
	{ "generated-*", "./my/generated-xyz/bar", TRUE },
	{ "generated-*", "./generated-xyz", TRUE },
	{ "generated-*", "./her-generated-xyz", FALSE },
	{ "generated-*", "./her/old-generated-xyz", FALSE },
	{ "generated-*", "./her/old-generated-xyz/bar", FALSE },

	{ "*.iso", "./my/amanda.iso", TRUE },
	{ "*.iso", "./amanda.iso", TRUE },

	{ "proxy/local/cache", "./usr/proxy/local/cache", TRUE },
	{ "proxy/local/cache", "./proxy/local/cache", TRUE },
	{ "proxy/local/cache", "./proxy/local/cache/7a", TRUE },

	{ NULL, NULL, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match_tar(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched tar %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched tar %s\n",
			t->str, t->expr);
	    }
	}
    }

    return ok;
}

static gboolean
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

static gboolean
test_match_host(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match;
    } tests[] = {
	/* from the amanda(8) manpage */
	{ "hosta", "hosta", TRUE },
	{ "hosta", "foo.hosta.org", TRUE },
	{ "hosta", "hoSTA.dOMAIna.ORG", TRUE },
	{ "hosta", "hostb", FALSE },
	{ "hOsta", "hosta", TRUE },
	{ "hOsta", "foo.hosta.org", TRUE },
	{ "hOsta", "hoSTA.dOMAIna.ORG", TRUE },
	{ "hOsta", "hostb", FALSE },

	{ "host", "host", TRUE },
	{ "host", "hosta", FALSE },

	{ "host?", "hosta", TRUE },
	{ "host?", "hostb", TRUE },
	{ "host?", "host", FALSE },
	{ "host?", "hostabc", FALSE },

	{ "ho*na", "hona", TRUE },
	{ "ho*na", "hoina", TRUE },
	{ "ho*na", "hoina.org", TRUE },
	{ "ho*na", "ns.hoina.org", TRUE },
	{ "ho*na", "ho.aina.org", FALSE },

	{ "ho**na", "hona", TRUE },
	{ "ho**na", "hoina", TRUE },
	{ "ho**na", "hoina.org", TRUE },
	{ "ho**na", "ns.hoina.org", TRUE },
	{ "ho**na", "ho.aina.org", TRUE },

	{ "^hosta", "hosta", TRUE },
	{ "^hosta", "hosta.org", TRUE },
	{ "^hosta", "hostabc", FALSE },
	{ "^hosta", "www.hosta", FALSE },
	{ "^hosta", "www.hosta.org", FALSE },

	{ "/opt", "opt", FALSE },

	{ ".hosta.", "hosta", TRUE },
	{ ".hosta.", "foo.hosta", TRUE },
	{ ".hosta.", "hosta.org", TRUE },
	{ ".hosta.", "foo.hosta.org", TRUE },
	{ "/hosta", "hosta", FALSE },
	{ "/hosta", "foo.hosta", FALSE },
	{ "/hosta", "hosta.org", FALSE },
	{ "/hosta", "foo.hosta.org", FALSE },

	{ ".opt.", "opt", TRUE },
	{ ".opt.", "www.opt", TRUE },
	{ ".opt.", "www.opt.com", TRUE },
	{ ".opt.", "opt.com", TRUE },

	/* other examples */
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

	{ "host[acd]", "hosta", TRUE },
	{ "host[acd]", "hostb", FALSE },
	{ "host[acd]", "hostc", TRUE },
	{ "host[!acd]", "hosta", FALSE },
	{ "host[!acd]", "hostb", TRUE },
	{ "host[!acd]", "hostc", FALSE },

	{ "toast", "www.toast.com", TRUE },
	{ ".toast", "www.toast.com", TRUE },
	{ "toast.", "www.toast.com", TRUE },
	{ ".toast.", "www.toast.com", TRUE },

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

static gboolean
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

static gboolean
test_match_disk(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match;
    } tests[] = {
	/* from the amanda(8) manpage */
	{ "sda*", "/dev/sda1", TRUE },
	{ "sda*", "/dev/sda2", TRUE },
	{ "sda*", "/dev/sdb2", FALSE },

	{ "opt", "opt", TRUE },
	{ "opt", "/opt", TRUE },
	{ "opt", "/opt/foo", TRUE },
	{ "opt", "opt/foo", TRUE },

	{ "/opt", "opt", TRUE },
	{ "/opt", "opt/", TRUE },
	{ "/opt", "/opt", TRUE },
	{ "/opt", "/opt/", TRUE },
	{ "/opt", "/local/opt/", TRUE },
	{ "/opt", "/opt/local/", TRUE },

	{ "opt/", "opt", TRUE },
	{ "opt/", "opt/", TRUE },
	{ "opt/", "/opt", TRUE },
	{ "opt/", "/opt/", TRUE },
	{ "opt/", "/local/opt/", TRUE },
	{ "opt/", "/opt/local/", TRUE },

	{ "/", "/", TRUE },
	{ "/", "/opt/local/", FALSE },

	{ "/usr$", "/", FALSE },
	{ "/usr$", "/usr", TRUE },
	{ "/usr$", "/usr/local", FALSE },

	{ "share", "\\\\windows1\\share", TRUE },
	{ "share", "\\\\windows2\\share", TRUE },
	{ "share", "//windows1/share", TRUE },
	{ "share", "//windows2/share", TRUE },

	{ "share*", "\\\\windows\\share1", TRUE },
	{ "share*", "\\\\windows\\share2", TRUE },
	{ "share*", "//windows/share3", TRUE },
	{ "share*", "//windows/share4", TRUE },

	{ "//windows/share", "//windows/share", TRUE },
	{ "//windows/share", "\\\\windows\\share", TRUE },
	{ "\\\\windows\\share", "//windows/share", FALSE },
	{ "\\\\windows\\share", "\\\\windows\\share", FALSE },
	{ "\\\\\\\\windows\\\\share", "//windows/share", FALSE },
	{ "\\\\\\\\windows\\\\share", "\\\\windows\\share", TRUE },

	/* other expressions */
	{ "^local", "/local", FALSE },
	{ "^/local", "/local", TRUE },
	{ "^local", "/local/vore", FALSE },
	{ "^/local", "/local/vore", TRUE },
	{ "^local", "/usr/local", FALSE },

	{ "local/bin", "/local/bin", TRUE },
	{ "local/bin", "/opt/local/bin", TRUE },
	{ "local/bin", "/local/bin/git", TRUE },

	{ "//windows/share", "//windows/share/files", TRUE },
	{ "//windows/share", "\\\\windows\\share\\files", TRUE },
	{ "\\\\windows\\share", "//windows/share/files", FALSE },
	{ "\\\\windows\\share", "\\\\windows\\share\\files", FALSE },

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
	{ "^\\/usr\\/bin$", "/usr/bin", TRUE },
	{ "/usr\\/bin\\/", "/usr/bin/", TRUE },
	{ "^/usr\\/bin\\/$", "/usr/bin/", TRUE },
	{ "\\/usr\\/bin\\/", "/usr/bin/", FALSE },
	{ "^\\/usr\\/bin\\/$", "/usr/bin/", TRUE },

	{ "oracle", "oracle", TRUE },
	{ "oracle", "/oracle", TRUE },
	{ "oracle", "oracle/", TRUE },
	{ "oracle", "/oracle/", TRUE },
	{ "/oracle", "oracle", TRUE },
	{ "/oracle", "/oracle", TRUE },
	{ "/oracle", "oracle/", TRUE },
	{ "/oracle", "/oracle/", TRUE },
	{ "^oracle", "oracle", TRUE },
	{ "^oracle", "/oracle", FALSE },
	{ "^oracle", "oracle/", TRUE },
	{ "^oracle", "/oracle/", FALSE },
	{ "^/oracle", "oracle", FALSE },
	{ "^/oracle", "/oracle", TRUE },
	{ "^/oracle", "oracle/", FALSE },
	{ "^/oracle", "/oracle/", TRUE },

	{ "oracle", "oracle", TRUE },
	{ "oracle", "/oracle", TRUE },
	{ "oracle", "oracle/", TRUE },
	{ "oracle", "/oracle/", TRUE },
	{ "oracle$", "oracle", TRUE },
	{ "oracle$", "/oracle", TRUE },
	{ "oracle$", "oracle/", FALSE },
	{ "oracle$", "/oracle/", FALSE },
	{ "oracle/$", "oracle", FALSE },
	{ "oracle/$", "/oracle", FALSE },
	{ "oracle/$", "oracle/", TRUE },
	{ "oracle/$", "/oracle/", TRUE },

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

static gboolean
test_match_datestamp(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match;
    } tests[] = {
	/* from the amanda(8) manpage */
	{ "20001212-14", "20001212", TRUE },
	{ "20001212-14", "20001212010203", TRUE },
	{ "20001212-14", "20001213", TRUE },
	{ "20001212-14", "20001213010203", TRUE },
	{ "20001212-14", "20001214", TRUE },
	{ "20001212-14", "20001215", FALSE },

	{ "20001212-4", "20001212", TRUE },
	{ "20001212-4", "20001212010203", TRUE },
	{ "20001212-4", "20001213", TRUE },
	{ "20001212-4", "20001213010203", TRUE },
	{ "20001212-4", "20001214", TRUE },
	{ "20001212-4", "20001215", FALSE },

	{ "20001212-214", "20001212", TRUE },
	{ "20001212-214", "20001212010203", TRUE },
	{ "20001212-214", "20001213", TRUE },
	{ "20001212-214", "20001213010203", TRUE },
	{ "20001212-214", "20001214", TRUE },
	{ "20001212-214", "20001215", FALSE },

	{ "20001212-24", "20001211", FALSE },
	{ "20001212-24", "20001214010203", TRUE },
	{ "20001212-24", "20001221010203", TRUE },
	{ "20001212-24", "20001224", TRUE },
	{ "20001212-24", "20001225", FALSE },

	{ "2000121", "20001209", FALSE },
	{ "2000121", "20001210", TRUE },
	{ "2000121", "20001210012345", TRUE },
	{ "2000121", "20001219", TRUE },
	{ "2000121", "20001219012345", TRUE },
	{ "2000121", "20001220", FALSE },

	{ "2", "19991231", FALSE },
	{ "2", "20000101", TRUE },
	{ "2", "20100419", TRUE },

	{ "^2", "19991231", FALSE },
	{ "^2", "20000101", TRUE },
	{ "^2", "20100419", TRUE },

	{ "2000-2010", "19991231235959", FALSE },
	{ "2000-2010", "20001010", TRUE },
	{ "2000-2010", "20101231", TRUE },
	{ "2000-2010", "20111010", FALSE },

	{ "200010$", "200010", TRUE }, /* but it's not a real datestamp */
	{ "200010$", "20001001", FALSE },
	{ "200010$", "20001001061500", FALSE },

	{ "20000615$", "20000615", TRUE },
	{ "20000615$", "20000615000000", FALSE },
	{ "20000615$", "20000615010306", FALSE },

	{ NULL, NULL, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match_datestamp(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched datestamp expr %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched datestamp expr %s\n",
			t->str, t->expr);
	    }
	}
    }

    return ok;
}

static gboolean
test_match_level(void)
{
    gboolean ok = TRUE;
    struct {
	char *expr, *str;
	gboolean should_match;
    } tests[] = {
	/* exact matches, optionally ignoring "^" */
	{ "3$", "2", FALSE },
	{ "3$", "3", TRUE },
	{ "3$", "4", FALSE },
	{ "3$", "32", FALSE },

	{ "^3$", "2", FALSE },
	{ "^3$", "3", TRUE },
	{ "^3$", "4", FALSE },
	{ "^3$", "32", FALSE },

	/* prefix matches */
	{ "3", "2", FALSE },
	{ "3", "3", TRUE },
	{ "3", "4", FALSE },
	{ "3", "32", TRUE },

	/* ranges */
	{ "2-5", "1", FALSE },
	{ "2-5", "13", FALSE },
	{ "2-5", "23", FALSE },
	{ "2-5", "2", TRUE },
	{ "2-5", "4", TRUE },
	{ "2-5", "5", TRUE },
	{ "2-5", "53", FALSE },
	{ "2-5", "63", FALSE },
	{ "2-5", "6", FALSE },

	{ "9-15", "8", FALSE },
	{ "9-15", "19", FALSE },
	{ "9-15", "91", FALSE },
	{ "9-15", "9", TRUE },
	{ "9-15", "14", TRUE },
	{ "9-15", "15", TRUE },
	{ "9-15", "152", FALSE },
	{ "9-15", "16", FALSE },

	{ "19-21", "18", FALSE },
	{ "19-21", "19", TRUE },
	{ "19-21", "21", TRUE },
	{ "19-21", "22", FALSE },

	/* single range is the same as an exact match */
	{ "99-99", "98", FALSE },
	{ "99-99", "99", TRUE },
	{ "99-99", "100", FALSE },

	/* reversed range never matches */
	{ "21-19", "18", FALSE },
	{ "21-19", "19", FALSE },
	{ "21-19", "21", FALSE },
	{ "21-19", "22", FALSE },

	{ NULL, NULL, FALSE },
    }, *t;

    for (t = tests; t->expr; t++) {
	gboolean matched = match_level(t->expr, t->str);
	if (!!matched != !!t->should_match) {
	    ok = FALSE;
	    if (t->should_match) {
		g_fprintf(stderr, "%s should have matched level expr %s\n",
			t->str, t->expr);
	    } else {
		g_fprintf(stderr, "%s unexpectedly matched level expr %s\n",
			t->str, t->expr);
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
	TU_TEST(test_validate_regexp, 90),
	TU_TEST(test_match, 90),
	TU_TEST(test_validate_glob, 90),
	TU_TEST(test_glob_to_regex, 90),
	TU_TEST(test_match_glob, 90),
	TU_TEST(test_match_tar, 90),
	TU_TEST(test_make_exact_host_expression, 90),
	TU_TEST(test_match_host, 90),
	TU_TEST(test_make_exact_disk_expression, 90),
	TU_TEST(test_match_disk, 90),
	TU_TEST(test_match_datestamp, 90),
	TU_TEST(test_match_level, 90),
	TU_END()
    };

    return testutils_run_tests(argc, argv, tests);
}

