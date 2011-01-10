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

char *
validate_regexp(
    const char *	regex)
{
    regex_t regc;
    int result;
    static char errmsg[STR_SIZE];

    if ((result = regcomp(&regc, regex,
			  REG_EXTENDED|REG_NOSUB|REG_NEWLINE)) != 0) {
      regerror(result, &regc, errmsg, SIZEOF(errmsg));
      return errmsg;
    }

    regfree(&regc);

    return NULL;
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

char *
make_exact_host_expression(
    const char *	host)
{
    char *result;
    int j;
    size_t i;
    result = alloc(2*strlen(host)+3);

    j = 0;
    result[j++] = '^';
    for(i=0;i<strlen(host);i++) {
	/* quote host expression metcharacters *except* '.'.  Note that
	 * most of these are invalid in a DNS hostname anyway. */
	switch (host[i]) {
	    case '\\':
	    case '/':
	    case '^':
	    case '$':
	    case '?':
	    case '*':
	    case '[':
	    case ']':
	    result[j++]='\\';
	    /* fall through */

	    default:
	    result[j++]=host[i];
	}
    }
    result[j++] = '$';
    result[j] = '\0';
    return result;
}

char *
make_exact_disk_expression(
    const char *	disk)
{
    char *result;
    int j;
    size_t i;
    result = alloc(2*strlen(disk)+3);

    j = 0;
    result[j++] = '^';
    for(i=0;i<strlen(disk);i++) {
	/* quote disk expression metcharacters *except* '/' */
	switch (disk[i]) {
	    case '\\':
	    case '.':
	    case '^':
	    case '$':
	    case '?':
	    case '*':
	    case '[':
	    case ']':
	    result[j++]='\\';
	    /* fall through */

	    default:
	    result[j++]=disk[i];
	}
    }
    result[j++] = '$';
    result[j] = '\0';
    return result;
}

int
match(
    const char *	regex,
    const char *	str)
{
    regex_t regc;
    int result;
    char errmsg[STR_SIZE];

    if((result = regcomp(&regc, regex,
			 REG_EXTENDED|REG_NOSUB|REG_NEWLINE)) != 0) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("regex \"%s\": %s"), regex, errmsg);
	/*NOTREACHED*/
    }

    if((result = regexec(&regc, str, 0, 0, 0)) != 0
       && result != REG_NOMATCH) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("regex \"%s\": %s"), regex, errmsg);
	/*NOTREACHED*/
    }

    regfree(&regc);

    return result == 0;
}

int
match_no_newline(
    const char *	regex,
    const char *	str)
{
    regex_t regc;
    int result;
    char errmsg[STR_SIZE];

    if((result = regcomp(&regc, regex,
			 REG_EXTENDED|REG_NOSUB)) != 0) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("regex \"%s\": %s"), regex, errmsg);
	/*NOTREACHED*/
    }

    if((result = regexec(&regc, str, 0, 0, 0)) != 0
       && result != REG_NOMATCH) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("regex \"%s\": %s"), regex, errmsg);
	/*NOTREACHED*/
    }

    regfree(&regc);

    return result == 0;
}

char *
validate_glob(
    const char *	glob)
{
    char *regex;
    regex_t regc;
    int result;
    static char errmsg[STR_SIZE];

    regex = glob_to_regex(glob);
    if ((result = regcomp(&regc, regex,
			  REG_EXTENDED|REG_NOSUB|REG_NEWLINE)) != 0) {
      regerror(result, &regc, errmsg, SIZEOF(errmsg));
      amfree(regex);
      return errmsg;
    }

    regfree(&regc);
    amfree(regex);

    return NULL;
}

int
match_glob(
    const char *	glob,
    const char *	str)
{
    char *regex;
    regex_t regc;
    int result;
    char errmsg[STR_SIZE];

    regex = glob_to_regex(glob);
    if((result = regcomp(&regc, regex,
			 REG_EXTENDED|REG_NOSUB|REG_NEWLINE)) != 0) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
	/*NOTREACHED*/
    }

    if((result = regexec(&regc, str, 0, 0, 0)) != 0
       && result != REG_NOMATCH) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
	/*NOTREACHED*/
    }

    regfree(&regc);
    amfree(regex);

    return result == 0;
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
    char errmsg[STR_SIZE];

    regex = tar_to_regex(glob);
    if((result = regcomp(&regc, regex,
			 REG_EXTENDED|REG_NOSUB|REG_NEWLINE)) != 0) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
	/*NOTREACHED*/
    }

    if((result = regexec(&regc, str, 0, 0, 0)) != 0
       && result != REG_NOMATCH) {
        regerror(result, &regc, errmsg, SIZEOF(errmsg));
	error(_("glob \"%s\" -> regex \"%s\": %s"), glob, regex, errmsg);
	/*NOTREACHED*/
    }

    regfree(&regc);
    amfree(regex);

    return result == 0;
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
    char  *mword, *nword;
    char  *mglob, *nglob;
    char *g;
    const char *src;
    int ret;

    lenword = strlen(word);
    nword = (char *)alloc(lenword + 3);

    if (separator == '/' && lenword > 2 && word[0] == '\\' && word[1] == '\\' && !strchr(word, '/')) {
	/* Convert all "\" to '/' */
	mword = (char *)alloc(lenword + 1);
	dst = mword;
	src = word;
	while (*src != '\0') {
	    if (*src == '\\') {
		*dst++ = '/';
		src += 1;
	    } else {
		*dst++ = *src++;
	    }
	}
	*dst++ = '\0';

	/* Convert all "\\" to '/' */
	mglob = (char *)alloc(strlen(glob) + 1);
	dst = mglob;
	src = glob;
	while (*src != '\0') {
	    if (*src == '\\' && *(src+1) == '\\') {
		*dst++ = '/';
		src += 2;
	    } else {
		*dst++ = *src++;
	    }
	}
	*dst++ = '\0';
    } else {
	mword = stralloc(word);
	mglob = stralloc(glob);
    }

    dst = nword;
    src = mword;
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
    len = strlen(mglob);
    regex = (char *)alloc(1 + len * 5 + 1 + 1 + 2 + 2);
    dst = regex;
    nglob = stralloc(mglob);
    g = nglob;

    if((len == 1 && nglob[0] == separator) ||
       (len == 2 && nglob[0] == '^' && nglob[1] == separator) ||
       (len == 2 && nglob[0] == separator && nglob[1] == '$') ||
       (len == 3 && nglob[0] == '^' && nglob[1] == separator &&
        nglob[2] == '$')) {
	*dst++ = '^';
	*dst++ = '\\';
	*dst++ = separator;
	*dst++ = '\\';
	*dst++ = separator;
	*dst++ = '$';
    }
    else {
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

    ret = match(regex,nword);

    amfree(mword);
    amfree(mglob);
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
    return match_word(glob, disk, '/');
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
