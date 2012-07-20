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
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */

#include "amanda.h"
#include "testutils.h"
#include "util.h"

/* Utilities */

static char *
safestr(const char *str) {
    static char hex[] = "0123456789abcdef";
    const char *p;
    char *result = malloc(3 + strlen(str) * 3);
    char *r = result;

    *(r++) = '|';
    for (p = str; *p; p++) {
	if (isprint((int)*p)) {
	    *(r++) = *p;
	} else {
	    *(r++) = '#';
	    *(r++) = hex[((*p)&0xf0) >> 4];
	    *(r++) = hex[(*p)&0xf];
	}
    }
    *(r++) = '|';
    *(r++) = '\0';

    return result;
}

char * quotable_strings[] = {
    "",
    "simple",
    "sp a ces",
    "\"foo bar\"",
    "back\\slash",
    "escaped\\ space",
    "escaped\\\"quote",
    "balanced \"internal\" quotes",
    "\"already quoted\" string",
    "string that's \"already quoted\"",
    "internal\"quote",
    "bs-end\\",
    "backslash\\nletter",
    "backslash\\tletter",
    "\t", "\r", "\n", "\f", "\004",
    "new\nline",
    "newline-end\n",
    "ta\tb",
    "tab-end\t",
    "\\\\\\\\",
    "\"",
    NULL
};

/****
 * Round-trip testing of quoting functions
 */

static gboolean
test_round_trip(void)
{
    char **strp;
    gboolean success = TRUE;

    for (strp = quotable_strings; *strp; strp++) {
	char *quoted, *unquoted;

	quoted = quote_string(*strp);
	unquoted = unquote_string(quoted);

	/* if they're not the same, complain */
	if (0 != strcmp(*strp, unquoted)) {
	    char *safe_orig = safestr(*strp);
	    char *safe_quoted = safestr(quoted);
	    char *safe_unquoted = safestr(unquoted);

	    printf("  bad round-trip: %s -quote_string-> %s -unquote_string-> %s\n",
		safe_orig, safe_quoted, safe_unquoted);

	    amfree(safe_orig);
	    amfree(safe_quoted);
	    amfree(safe_unquoted);

	    success = FALSE;
	}

	amfree(quoted);
	amfree(unquoted);
    }

    return success;
}

/***
 * Test that the new split_quoted_strings acts identically to
 * the old split(), albeit with a different set of arguments and
 * return value.  Note that we only test with a delimiter of " ",
 * as split() is not used with any other delimiter.
 */

static gboolean
compare_strv(
    const char **exp,
    char **got,
    const char *source,
    const char *original)
{
    const char **a = exp;
    char **b = got;
    while (*a && *b) {
	if (0 != strcmp(*a, *b))
	    break;
	a++; b++;
    }

    /* did we exit the loop early, or were they different lengths? */
    if (*a || *b) {
	char *safe;

	safe = safestr(original);
	g_printf("  %s: expected [", safe);
	amfree(safe);
	for (a = exp; *a; a++) {
	    safe = safestr(*a);
	    g_printf("%s%s", safe, *(a+1)? ", " : "");
	    amfree(safe);
	}
	g_printf("] but got [");
	for (b = got; *b; b++) {
	    safe = safestr(*b);
	    g_printf("%s%s", safe, *(b+1)? ", " : "");
	    amfree(safe);
	}
	g_printf("] using %s.\n", source);

	return FALSE;
    }

    return TRUE;
}

static gboolean
test_split_quoted_strings(void)
{
    char **iter1, **iter2, **iter3;
    gboolean success = TRUE;
    char *middle_strings[] = {
	"",
	"foo",
	"\"foo\"",
	"sp aces",
	NULL,
    };

    /* the idea here is to loop over all triples of strings, forming a
     * string by quoting them with quote_string and inserting a space, then
     * re-splitting with split_quoted_string.  This should get us back to our
     * starting point. */

    for (iter1 = quotable_strings; *iter1; iter1++) {
	for (iter2 = middle_strings; *iter2; iter2++) {
	    for (iter3 = quotable_strings; *iter3; iter3++) {
		char *q1 = quote_string(*iter1);
		char *q2 = quote_string(*iter2);
		char *q3 = quote_string(*iter3);
		const char *expected[4] = { *iter1, *iter2, *iter3, NULL };
		char *combined = vstralloc(q1, " ", q2, " ", q3, NULL);
		char **tokens;

		tokens = split_quoted_strings(combined);

		success = compare_strv(expected, tokens, "split_quoted_strings", combined)
			&& success;

		amfree(q1);
		amfree(q2);
		amfree(q3);
		amfree(combined);
		g_strfreev(tokens);
	    }
	}
    }

    return success;
}

/****
 * Test splitting some edge cases and invalid strings
 */

struct trial {
    const char *combined;
    const char *expected[5];
};

static gboolean
test_split_quoted_strings_edge(void)
{
    gboolean success = TRUE;
    struct trial trials[] = {
	{ "", { "", NULL, } },
	{ " ", { "", "", NULL } },
	{ " x", { "", "x", NULL } },
	{ "x ", { "x", "", NULL } },
	{ "x\\ y", { "x y", NULL } },
	{ "\\", { "", NULL } }, /* inv */
	{ "z\\", { "z", NULL } }, /* inv */
	{ "z\"", { "z", NULL } }, /* inv */
	{ "\" \" \"", { " ", "", NULL } }, /* inv */
	{ NULL, { NULL, } },
    };
    struct trial *trial = trials;

    while (trial->combined) {
	char **tokens = split_quoted_strings(trial->combined);

	success = compare_strv(trial->expected, tokens,
			       "split_quoted_strings", trial->combined)
	    && success;

	g_strfreev(tokens);
	trial++;
    }

    return success;
}

/****
 * Test unquoting of some pathological strings
 */
static gboolean
test_unquote_string(void)
{
    gboolean success = TRUE;
    char *tests[] = {
	"simple",              "simple",
	"\"quoted\"",          "quoted",
	"s p a c e",           "s p a c e",

	/* special escape characters */
	"esc \\\" quote",      "esc \" quote",
	"esc \\t tab",         "esc \t tab",
	"esc \\\\ esc",        "esc \\ esc",
	"esc \\02 oct",         "esc \02 oct",
	"esc \\7 oct",         "esc \7 oct",
	"esc \\17 oct",        "esc \17 oct",
	"esc \\117 oct",       "esc \117 oct",
	"esc \\1117 oct",      "esc \1117 oct", /* '7' is distinct char */

	/* same, but pre-quoted */
	"\"esc \\\" quote\"",  "esc \" quote",
	"\"esc \\t tab\"",     "esc \t tab",
	"\"esc \\\\ esc\"",    "esc \\ esc",
	"\"esc \\02 oct\"",     "esc \02 oct",
	"\"esc \\7 oct\"",     "esc \7 oct",
	"\"esc \\17 oct\"",    "esc \17 oct",
	"\"esc \\117 oct\"",   "esc \117 oct",
	"\"esc \\1117 oct\"",  "esc \1117 oct", /* '7' is distinct char */

	/* strips balanced quotes, even inside the string */
	">>\"x\"<<",           ">>x<<",
	">>\"x\"-\"y\"<<",     ">>x-y<<",

	/* pathological, but valid */
	"\\\\",                "\\",
	"\"\\\"\"",            "\"",
	"\"\\\\\"",            "\\",
	"--\\\"",              "--\"",
	"\\\"--",              "\"--",

	/* invalid strings (handling here is arbitrary, but these tests
	 * will alert us if the handling changes) */
	"\\",                  "", /* trailing backslash is ignored */
	"xx\\",                "xx", /* ditto */
	"\\\\\\\\\\\\\\",      "\\\\\\",   /* ditto */
	"\\777",               "\377", /* 0777 & 0xff = 0xff */
	"\"--",                "--", /* leading quote is dropped */
	"--\"",                "--", /* trailing quote is dropped */

	NULL, NULL,
    };
    char **strp;

    for (strp = tests; *strp;) {
	char *quoted = *(strp++);
	char *expected = *(strp++);
	char *unquoted = unquote_string(quoted);

	/* if they're not the same, complain */
	if (0 != strcmp(expected, unquoted)) {
	    char *safe_quoted = safestr(quoted);
	    char *safe_unquoted = safestr(unquoted);
	    char *safe_expected = safestr(expected);

	    printf("  %s unquoted to %s; expected %s.\n",
		safe_quoted, safe_unquoted, safe_expected);

	    amfree(safe_quoted);
	    amfree(safe_unquoted);
	    amfree(safe_expected);

	    success = FALSE;
	}

	amfree(unquoted);
    }

    return success;
}

/****
 * Test the strquotedstr function
 */
static gboolean
test_strquotedstr_skipping(void)
{
    char **iter1, **iter2;
    gboolean success = TRUE;

    /* the idea here is to loop over all pairs of strings, forming a
     * string by quoting them with quote_string and inserting a space, then
     * re-splitting with strquotedstr.  This should get us back to our
     * starting point. Note that we have to begin with a non-quoted identifier,
     * becuse strquotedstr requires that strtok_r has already been called. */

    for (iter1 = quotable_strings; *iter1; iter1++) {
	for (iter2 = quotable_strings; *iter2; iter2++) {
	    char *q1 = quote_string(*iter1);
	    char *q2 = quote_string(*iter2);
	    char *combined = vstralloc("START ", q1, " ", q2, NULL);
	    char *copy = g_strdup(combined);
	    char *saveptr = NULL;
	    char *tok;
	    int i;

	    tok = strtok_r(copy, " ", &saveptr);

	    for (i = 1; i <= 2; i++) {
		char *expected = (i == 1)? q1:q2;
		tok = strquotedstr(&saveptr);
		if (!tok) {
		    g_fprintf(stderr, "while parsing '%s', call %d to strquotedstr returned NULL\n",
			      combined, i);
		    success = FALSE;
		    goto next;
		}
		if (0 != strcmp(tok, expected)) {
		    char *safe = safestr(tok);

		    g_fprintf(stderr, "while parsing '%s', call %d to strquotedstr returned '%s' "
			      "but '%s' was expected.\n",
			      combined, i, safe, expected);
		    success = FALSE;
		    goto next;
		}
	    }

	    if (strquotedstr(&saveptr) != NULL) {
		g_fprintf(stderr, "while parsing '%s', call 3 to strquotedstr did not return NULL\n",
			  combined);
		success = FALSE;
		goto next;
	    }
next:
	    amfree(q1);
	    amfree(q2);
	    amfree(copy);
	    amfree(combined);
	}
    }

    return success;
}

static gboolean
test_strquotedstr_edge_invalid(void)
{
    gboolean success = TRUE;
    char *invalid[] = {
	"X \"abc", /* unterminated */
	"X \"ab cd", /* unterminated second token */
	"X a\"b cd", /* unterminated second token with internal quote */
	"X b\\", /* trailing backslash */
	"X \"b\\", /* trailing backslash in quote */
	"X \"b\\\"", /* backslash'd ending quote */
	NULL
    };
    char **iter;

    /* run strquotedstr on a bunch of invalid tokens.  It should return NULL */

    for (iter = invalid; *iter; iter++) {
	char *copy = g_strdup(*iter);
	char *tok;
	char *saveptr = NULL;

	tok = strtok_r(copy, " ", &saveptr);
	tok = strquotedstr(&saveptr);
	if (tok != NULL) {
	    g_fprintf(stderr, "while parsing invalid '%s', strquotedstr did not return NULL\n",
		      *iter);
	    success = FALSE;
	}

	amfree(copy);
    }

    return success;
}

static gboolean
test_strquotedstr_edge_valid(void)
{
    gboolean success = TRUE;
    char *valid[] = {
	/* input */	    /* expected (omitting "X") */
	"X abc\\ def",      "abc\\ def", /* backslashed space */
	"X \"abc\\ def\"",  "\"abc\\ def\"", /* quoted, backslashed space */
	"X a\"  \"b",       "a\"  \"b", /* quoted spaces */
	NULL, NULL
    };
    char **iter;

    /* run strquotedstr on a bunch of valid, but tricky, tokens.  It should return NULL */

    for (iter = valid; *iter; iter += 2) {
	char *copy = g_strdup(*iter);
	char *expected = *(iter+1);
	char *tok;
	char *saveptr = NULL;

	tok = strtok_r(copy, " ", &saveptr);
	tok = strquotedstr(&saveptr);
	if (tok == NULL) {
	    g_fprintf(stderr, "while parsing valid '%s', strquotedstr returned NULL\n",
		      *iter);
	    success = FALSE;
	} else if (0 != strcmp(tok, expected)) {
	    g_fprintf(stderr, "while parsing valid '%s', strquotedstr returned '%s' while "
		      "'%s' was expected\n",
		      *iter, tok, expected);
	    success = FALSE;
	}

	amfree(copy);
    }

    return success;
}

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_round_trip, 90),
	TU_TEST(test_unquote_string, 90),
	TU_TEST(test_split_quoted_strings, 90),
	TU_TEST(test_split_quoted_strings_edge, 90),
	TU_TEST(test_strquotedstr_skipping, 90),
	TU_TEST(test_strquotedstr_edge_invalid, 90),
	TU_TEST(test_strquotedstr_edge_valid, 90),
	TU_END()
    };

    return testutils_run_tests(argc, argv, tests);
}
