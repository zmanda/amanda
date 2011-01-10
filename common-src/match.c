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
 * $Id: match.c,v 1.23 2006/05/25 01:47:12 johnfranks Exp $
 *
 * functions for checking and matching regular expressions
 */

#include "amanda.h"
#include "match.h"
#include <regex.h>

static int match_word(const char *glob, const char *word, const char separator);
static char *tar_to_regex(const char *glob);

/*
 * REGEX MATCHING FUNCTIONS
 */

/*
 * Define a specific type to hold error messages in case regex compile/matching
 * fails
 */

typedef char regex_errbuf[STR_SIZE];

/*
 * Validate one regular expression. If the regex is invalid, copy the error
 * message into the supplied regex_errbuf pointer. Also, we want to know whether
 * flags should include REG_NEWLINE (See regcomp(3) for details). Since this is
 * the more frequent case, add REG_NEWLINE to the default flags, and remove it
 * only if match_newline is set to FALSE.
 */

static gboolean do_validate_regex(const char *str, regex_t *regex,
	regex_errbuf *errbuf, gboolean match_newline)
{
	int flags = REG_EXTENDED | REG_NOSUB | REG_NEWLINE;
	int result;

	if (!match_newline)
		CLR(flags, REG_NEWLINE);

	result = regcomp(regex, str, flags);

	if (!result)
		return TRUE;

	regerror(result, regex, *errbuf, SIZEOF(*errbuf));
	return FALSE;
}

/*
 * See if a string matches a regular expression. Return one of MATCH_* defined
 * below. If, for some reason, regexec() returns something other than not 0 or
 * REG_NOMATCH, return MATCH_ERROR and print the error message in the supplied
 * regex_errbuf.
 */

#define MATCH_OK (1)
#define MATCH_NONE (0)
#define MATCH_ERROR (-1)

static int try_match(regex_t *regex, const char *str,
    regex_errbuf *errbuf)
{
    int result = regexec(regex, str, 0, 0, 0);

    switch(result) {
        case 0:
            return MATCH_OK;
        case REG_NOMATCH:
            return MATCH_NONE;
        /* Fall through: something went really wrong */
    }

    regerror(result, regex, *errbuf, SIZEOF(*errbuf));
    return MATCH_ERROR;
}

char *
validate_regexp(
    const char *	regex)
{
    regex_t regc;
    static regex_errbuf errmsg;
    gboolean valid;

    valid = do_validate_regex(regex, &regc, &errmsg, TRUE);

    regfree(&regc);
    return (valid) ? NULL : errmsg;
}

char *
clean_regex(
    const char *	str,
    gboolean		anchor)
{
    char *result;
    int j;
    size_t i;
    result = alloc(2*strlen(str)+3);

    j = 0;
    if (anchor)
	result[j++] = '^';
    for(i=0;i<strlen(str);i++) {
	if(!isalnum((int)str[i]))
	    result[j++]='\\';
	result[j++]=str[i];
    }
    if (anchor)
	result[j++] = '$';
    result[j] = '\0';
    return result;
}

/*
 * Check whether a given character should be escaped (that is, prepended with a
 * backslash), EXCEPT for one character.
 */

static gboolean should_be_escaped_except(char c, char not_this_one)
{
    if (c == not_this_one)
        return FALSE;

    switch (c) {
        case '\\':
        case '^':
        case '$':
        case '?':
        case '*':
        case '[':
        case ']':
        case '.':
        case '/':
            return TRUE;
    }

    return FALSE;
}

/*
 * Take a user-supplied argument and turn it into a full-blown regex (with start
 * and end anchors) following rules in amanda-match(7). The not_this_one
 * argument represents a character which is NOT meant to be special in this
 * case: '/' for disks and '.' for hosts.
 */

static char *full_regex_from_expression(const char *str, char not_this_one)
{
    const char *src;
    char *result, *dst;

    result = alloc(2 * strlen(str) + 3);
    dst = result;

    *(dst++) = '^';

    for (src = str; *src; src++) {
        if (should_be_escaped_except(*src, not_this_one))
            *(dst++) = '\\';
        *(dst++) = *src;
    }

    *(dst++) = '$';
    *dst = '\0';
    return result;
}

char *
make_exact_host_expression(
    const char *	host)
{
    return full_regex_from_expression(host, '.');
}

char *
make_exact_disk_expression(
    const char *	disk)
{
    return full_regex_from_expression(disk, '/');
}

int do_match(const char *regex, const char *str, gboolean match_newline)
{
    regex_t regc;
    int result;
    regex_errbuf errmsg;
    gboolean ok;

    ok = do_validate_regex(regex, &regc, &errmsg, match_newline);

    if (!ok)
        error(_("regex \"%s\": %s"), regex, errmsg);
        /*NOTREACHED*/

    result = try_match(&regc, str, &errmsg);

    if (result == MATCH_ERROR)
        error(_("regex \"%s\": %s"), regex, errmsg);
        /*NOTREACHED*/

    regfree(&regc);

    return result;
}

char *
validate_glob(
    const char *	glob)
{
    char *regex, *ret = NULL;
    regex_t regc;
    static regex_errbuf errmsg;

    regex = glob_to_regex(glob);

    if (!do_validate_regex(regex, &regc, &errmsg, TRUE))
        ret = errmsg;

    regfree(&regc);
    amfree(regex);
    return ret;
}

int
match_glob(
    const char *	glob,
    const char *	str)
{
    char *regex;
    regex_t regc;
    int result;
    regex_errbuf errmsg;
    gboolean ok;

    regex = glob_to_regex(glob);
    ok = do_validate_regex(regex, &regc, &errmsg, TRUE);

    if (!ok)
        error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
        /*NOTREACHED*/

    result = try_match(&regc, str, &errmsg);

    if (result == MATCH_ERROR)
        error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
        /*NOTREACHED*/

    regfree(&regc);
    amfree(regex);

    return result;
}

/*
 * Macro to tell whether a character is a regex metacharacter. Note that '*'
 * and '?' are NOT included: they are themselves special in globs.
 */

#define IS_REGEX_META(c) ( \
    (c) == '.' || (c) == '(' || (c) == ')' || (c) == '{' || (c) == '}' || \
    (c) == '+' || (c) == '^' || (c) == '$' || (c) == '|' \
)

char *
glob_to_regex(
    const char *	glob)
{
    char *regex;
    char *r;
    size_t len;
    int ch;
    int last_ch;

    /*
     * Allocate an area to convert into. The worst case is a five to
     * one expansion.
     */
    len = strlen(glob);
    regex = alloc(1 + len * 5 + 1 + 1);

    /*
     * Do the conversion:
     *
     *  ?      -> [^/]
     *  *      -> [^/]*
     *  [...] ->  [...]
     *  [!...] -> [^...]
     *
     * The following are given a leading backslash to protect them
     * unless they already have a backslash:
     *
     *   ( ) { } + . ^ $ |
     *
     * Put a leading ^ and trailing $ around the result.  If the last
     * non-escaped character is \ leave the $ off to cause a syntax
     * error when the regex is compiled.
     */

    r = regex;
    *r++ = '^';
    last_ch = '\0';
    for (ch = *glob++; ch != '\0'; last_ch = ch, ch = *glob++) {
	if (last_ch == '\\') {
	    *r++ = (char)ch;
	    ch = '\0';			/* so last_ch != '\\' next time */
	} else if (last_ch == '[' && ch == '!') {
	    *r++ = '^';
	} else if (ch == '\\') {
	    *r++ = (char)ch;
	} else if (ch == '*' || ch == '?') {
	    *r++ = '[';
	    *r++ = '^';
	    *r++ = '/';
	    *r++ = ']';
	    if (ch == '*') {
		*r++ = '*';
	    }
	} else if (IS_REGEX_META(ch)) {
	    *r++ = '\\';
	    *r++ = (char)ch;
	} else {
	    *r++ = (char)ch;
	}
    }
    if (last_ch != '\\') {
	*r++ = '$';
    }
    *r = '\0';

    return regex;
}

int
match_tar(
    const char *	glob,
    const char *	str)
{
    char *regex;
    regex_t regc;
    int result;
    regex_errbuf errmsg;
    gboolean ok;

    regex = tar_to_regex(glob);
    ok = do_validate_regex(regex, &regc, &errmsg, TRUE);

    if (!ok)
        error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
        /*NOTREACHED*/

    result = try_match(&regc, str, &errmsg);

    if (result == MATCH_ERROR)
        error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
        /*NOTREACHED*/

    regfree(&regc);
    amfree(regex);

    return result;
}

static char *
tar_to_regex(
    const char *	glob)
{
    char *regex;
    char *r;
    size_t len;
    int ch;
    int last_ch;

    /*
     * Allocate an area to convert into. The worst case is a five to
     * one expansion.
     */
    len = strlen(glob);
    regex = alloc(1 + len * 5 + 5 + 5);

    /*
     * Do the conversion:
     *
     *  ?      -> [^/]
     *  *      -> .*
     *  [...]  -> [...]
     *  [!...] -> [^...]
     *
     * The following are given a leading backslash to protect them
     * unless they already have a backslash:
     *
     *   ( ) { } + . ^ $ |
     *
     * The expression must begin and end either at the beginning/end of the string or
     * at at a pathname separator.
     *
     * If the last non-escaped character is \ leave the $ off to cause a syntax
     * error when the regex is compiled.
     */

    r = regex;
    *r++ = '(';
    *r++ = '^';
    *r++ = '|';
    *r++ = '/';
    *r++ = ')';
    last_ch = '\0';
    for (ch = *glob++; ch != '\0'; last_ch = ch, ch = *glob++) {
	if (last_ch == '\\') {
	    *r++ = (char)ch;
	    ch = '\0';			/* so last_ch != '\\' next time */
	} else if (last_ch == '[' && ch == '!') {
	    *r++ = '^';
	} else if (ch == '\\') {
	    *r++ = (char)ch;
	} else if (ch == '*') {
	    *r++ = '.';
	    *r++ = '*';
	} else if (ch == '?') {
	    *r++ = '[';
	    *r++ = '^';
	    *r++ = '/';
	    *r++ = ']';
	} else if (IS_REGEX_META(ch)) {
	    *r++ = '\\';
	    *r++ = (char)ch;
	} else {
	    *r++ = (char)ch;
	}
    }
    if (last_ch != '\\') {
	*r++ = '(';
	*r++ = '$';
	*r++ = '|';
	*r++ = '/';
	*r++ = ')';
    }
    *r = '\0';

    return regex;
}

/*
 * Two utility functions used by match_disk() below: they are used to convert a
 * disk and glob from Windows expressed paths (backslashes) into Unix paths
 * (slashes).
 *
 * Note: the resulting string is dynamically allocated, it is up to the caller
 * to free it.
 *
 * Note 2: UNC in convert_unc_to_unix stands for Uniform Naming Convention.
 */

static char *convert_unc_to_unix(const char *unc)
{
    const char *src;
    char *result, *dst;
    result = alloc(strlen(unc) + 1);
    dst = result;

    for (src = unc; *src; src++)
        *(dst++) = (*src == '\\') ? '/' : *src;

    *dst = '\0';
    return result;
}

static char *convert_winglob_to_unix(const char *glob)
{
    const char *src;
    char *result, *dst;
    result = alloc(strlen(glob) + 1);
    dst = result;

    for (src = glob; *src; src++) {
        if (*src == '\\' && *(src + 1) == '\\') {
            *(dst++) = '/';
            src++;
            continue;
        }
        *(dst++) = *src;
    }
    *dst = '\0';
    return result;
}


/*
 * Check whether a glob passed as an argument to match_word() only looks for the
 * separator
 */

static gboolean glob_is_separator_only(const char *glob, char sep) {
    size_t len = strlen(glob);
    const char len2_1[] = { '^', sep , 0 }, len2_2[] = { sep, '$', 0 },
        len3[] = { '^', sep, '$', 0 };

    switch (len) {
        case 1:
            return (*glob == sep);
        case 2:
            return !(strcmp(glob, len2_1) && strcmp(glob, len2_2));
        case 3:
            return !strcmp(glob, len3);
        default:
            return FALSE;
    }
}

static int
match_word(
    const char *	glob,
    const char *	word,
    const char		separator)
{
    char *regex;
    char *dst;
    size_t  len;
    int  ch;
    int  last_ch;
    int  next_ch;
    size_t  lenword;
    char *nword;
    char *nglob;
    char *g;
    const char *src;
    int ret;

    lenword = strlen(word);
    nword = (char *)alloc(lenword + 3);

    dst = nword;
    src = word;
    if(lenword == 1 && *src == separator) {
	*dst++ = separator;
	*dst++ = separator;
    }
    else {
	if(*src != separator)
	    *dst++ = separator;
	while(*src != '\0')
	    *dst++ = *src++;
	if(*(dst-1) != separator)
	    *dst++ = separator;
    }
    *dst = '\0';

    /*
     * Allocate an area to convert into. The worst case is a five to one
     * expansion.
     */
    len = strlen(glob);
    nglob = stralloc(glob);

    if(glob_is_separator_only(nglob, separator)) {
        regex = alloc(7); /* Length of what is written below plus '\0' */
        dst = regex;
	*dst++ = '^';
	*dst++ = '\\';
	*dst++ = separator;
	*dst++ = '\\';
	*dst++ = separator;
	*dst++ = '$';
    }
    else {
        regex = alloc(1 + len * 5 + 1 + 1 + 2 + 2);
        dst = regex;
        g = nglob;
	/*
	 * Do the conversion:
	 *
	 *  ?      -> [^<separator>]
	 *  *      -> [^<separator>]*
	 *  [!...] -> [^...]
	 *  **     -> .*
	 *
	 * The following are given a leading backslash to protect them
	 * unless they already have a backslash:
	 *
	 *   ( ) { } + . ^ $ |
	 *
	 * If the last
	 * non-escaped character is \ leave it to cause a syntax
	 * error when the regex is compiled.
	 */

	if(*g == '^') {
	    *dst++ = '^';
	    *dst++ = '\\';	/* escape the separator */
	    *dst++ = separator;
	    g++;
	    if(*g == separator) g++;
	}
	else if(*g != separator) {
	    *dst++ = '\\';	/* add a leading \separator */
	    *dst++ = separator;
	}
	last_ch = '\0';
	for (ch = *g++; ch != '\0'; last_ch = ch, ch = *g++) {
	    next_ch = *g;
	    if (last_ch == '\\') {
		*dst++ = (char)ch;
		ch = '\0';		/* so last_ch != '\\' next time */
	    } else if (last_ch == '[' && ch == '!') {
		*dst++ = '^';
	    } else if (ch == '\\') {
		*dst++ = (char)ch;
	    } else if (ch == '*' || ch == '?') {
		if(ch == '*' && next_ch == '*') {
		    *dst++ = '.';
		    g++;
		}
		else {
		    *dst++ = '[';
		    *dst++ = '^';
		    *dst++ = separator;
		    *dst++ = ']';
		}
		if (ch == '*') {
		    *dst++ = '*';
		}
	    } else if (ch == '$' && next_ch == '\0') {
		if(last_ch != separator) {
		    *dst++ = '\\';
		    *dst++ = separator;
		}
		*dst++ = (char)ch;
	    } else if (IS_REGEX_META(ch)) {
		*dst++ = '\\';
		*dst++ = (char)ch;
	    } else {
		*dst++ = (char)ch;
	    }
	}
	if(last_ch != '\\') {
	    if(last_ch != separator && last_ch != '$') {
		*dst++ = '\\';
		*dst++ = separator;		/* add a trailing \separator */
	    }
	}
    }
    *dst = '\0';

    ret = do_match(regex, nword, TRUE);

    amfree(nword);
    amfree(nglob);
    amfree(regex);

    return ret;
}


int
match_host(
    const char *	glob,
    const char *	host)
{
    char *lglob, *lhost;
    int ret;

    
    lglob = g_ascii_strdown(glob, -1);
    lhost = g_ascii_strdown(host, -1);

    ret = match_word(lglob, lhost, '.');

    amfree(lglob);
    amfree(lhost);
    return ret;
}


int
match_disk(
    const char *	glob,
    const char *	disk)
{
    char *glob2 = NULL, *disk2 = NULL;
    const char *g = glob, *d = disk;
    int result;

    /*
     * Check whether our disk potentially refers to a Windows share (the first
     * two characters are '\' and there is no / in the word at all): if yes,
     * convert all double backslashes to slashes in the glob, and simple
     * backslashes into slashes in the disk, and pass these new strings as
     * arguments instead of the originals.
     */
    gboolean windows_share = !(strncmp(disk, "\\\\", 2) || strchr(disk, '/'));

    if (windows_share) {
        glob2 = convert_winglob_to_unix(glob);
        disk2 = convert_unc_to_unix(disk);
        g = (const char *) glob2;
        d = (const char *) disk2;
    }

    result = match_word(g, d, '/');

    /*
     * We can amfree(NULL), so this is "safe"
     */
    amfree(glob2);
    amfree(disk2);

    return result;
}

static int
alldigits(
    const char *str)
{
    while (*str) {
	if (!isdigit((int)*(str++)))
	    return 0;
    }
    return 1;
}

int
match_datestamp(
    const char *	dateexp,
    const char *	datestamp)
{
    char *dash;
    size_t len, len_suffix;
    size_t len_prefix;
    char firstdate[100], lastdate[100];
    char mydateexp[100];
    int match_exact;

    if(strlen(dateexp) >= 100 || strlen(dateexp) < 1) {
	goto illegal;
    }
   
    /* strip and ignore an initial "^" */
    if(dateexp[0] == '^') {
	strncpy(mydateexp, dateexp+1, sizeof(mydateexp)-1);
	mydateexp[sizeof(mydateexp)-1] = '\0';
    }
    else {
	strncpy(mydateexp, dateexp, sizeof(mydateexp)-1);
	mydateexp[sizeof(mydateexp)-1] = '\0';
    }

    if(mydateexp[strlen(mydateexp)-1] == '$') {
	match_exact = 1;
	mydateexp[strlen(mydateexp)-1] = '\0';	/* strip the trailing $ */
    }
    else
	match_exact = 0;

    /* a single dash represents a date range */
    if((dash = strchr(mydateexp,'-'))) {
	if(match_exact == 1 || strchr(dash+1, '-')) {
	    goto illegal;
	}

	/* format: XXXYYYY-ZZZZ, indicating dates XXXYYYY to XXXZZZZ */

	len = (size_t)(dash - mydateexp);   /* length of XXXYYYY */
	len_suffix = strlen(dash) - 1;	/* length of ZZZZ */
	if (len_suffix > len) goto illegal;
	len_prefix = len - len_suffix; /* length of XXX */

	dash++;

	strncpy(firstdate, mydateexp, len);
	firstdate[len] = '\0';
	strncpy(lastdate, mydateexp, len_prefix);
	strncpy(&(lastdate[len_prefix]), dash, len_suffix);
	lastdate[len] = '\0';
	if (!alldigits(firstdate) || !alldigits(lastdate))
	    goto illegal;
	if (strncmp(firstdate, lastdate, strlen(firstdate)) > 0)
	    goto illegal;
	return ((strncmp(datestamp, firstdate, strlen(firstdate)) >= 0) &&
		(strncmp(datestamp, lastdate , strlen(lastdate))  <= 0));
    }
    else {
	if (!alldigits(mydateexp))
	    goto illegal;
	if(match_exact == 1) {
	    return (strcmp(datestamp, mydateexp) == 0);
	}
	else {
	    return (strncmp(datestamp, mydateexp, strlen(mydateexp)) == 0);
	}
    }
illegal:
	error(_("Illegal datestamp expression %s"),dateexp);
	/*NOTREACHED*/
}


int
match_level(
    const char *	levelexp,
    const char *	level)
{
    char *dash;
    long int low, hi, level_i;
    char mylevelexp[100];
    int match_exact;

    if(strlen(levelexp) >= 100 || strlen(levelexp) < 1) {
	error(_("Illegal level expression %s"),levelexp);
	/*NOTREACHED*/
    }
   
    if(levelexp[0] == '^') {
	strncpy(mylevelexp, levelexp+1, strlen(levelexp)-1); 
	mylevelexp[strlen(levelexp)-1] = '\0';
    }
    else {
	strncpy(mylevelexp, levelexp, strlen(levelexp));
	mylevelexp[strlen(levelexp)] = '\0';
    }

    if(mylevelexp[strlen(mylevelexp)-1] == '$') {
	match_exact = 1;
	mylevelexp[strlen(mylevelexp)-1] = '\0';
    }
    else
	match_exact = 0;

    if((dash = strchr(mylevelexp,'-'))) {
	if(match_exact == 1) {
            goto illegal;
	}

        *dash = '\0';
        if (!alldigits(mylevelexp) || !alldigits(dash+1)) goto illegal;

        errno = 0;
        low = strtol(mylevelexp, (char **) NULL, 10);
        if (errno) goto illegal;
        hi = strtol(dash+1, (char **) NULL, 10);
        if (errno) goto illegal;
        level_i = strtol(level, (char **) NULL, 10);
        if (errno) goto illegal;

	return ((level_i >= low) && (level_i <= hi));
    }
    else {
	if (!alldigits(mylevelexp)) goto illegal;
	if(match_exact == 1) {
	    return (strcmp(level, mylevelexp) == 0);
	}
	else {
	    return (strncmp(level, mylevelexp, strlen(mylevelexp)) == 0);
	}
    }
illegal:
    error(_("Illegal level expression %s"),levelexp);
    /*NOTREACHED*/
}
