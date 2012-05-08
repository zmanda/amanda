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
 * See match.h for function prototypes and further explanations.
 */

#include "amanda.h"
#include "match.h"
#include <regex.h>

/*
 * DATA STRUCTURES, MACROS, STATIC DATA
 */

/*
 * Return codes used by try_match()
 */

#define MATCH_OK (1)
#define MATCH_NONE (0)
#define MATCH_ERROR (-1)

/*
 * Macro to tell whether a character is a regex metacharacter. Note that '*'
 * and '?' are NOT included: they are themselves special in globs.
 */

#define IS_REGEX_META(c) ( \
    (c) == '.' || (c) == '(' || (c) == ')' || (c) == '{' || (c) == '}' || \
    (c) == '+' || (c) == '^' || (c) == '$' || (c) == '|' \
)

/*
 * Define a specific type to hold error messages in case regex compile/matching
 * fails
 */

typedef char regex_errbuf[STR_SIZE];

/*
 * Structure used by amglob_to_regex() to expand particular glob characters. Its
 * fields are:
 * - question_mark: what the question mark ('?') should be replaced with;
 * - star: what the star ('*') should be replaced with;
 * - double_star: what two consecutive stars should be replaced with.
 *
 * Note that apart from double_star, ALL OTHER FIELDS MUST NOT BE NULL.
 */

struct subst_table {
    const char *question_mark;
    const char *star;
    const char *double_star;
};

/*
 * Susbtitution data for glob_to_regex()
 */

static struct subst_table glob_subst_stable = {
    "[^/]", /* question_mark */
    "[^/]*", /* star */
    NULL /* double_star */
};

/*
 * Substitution data for tar_to_regex()
 */

static struct subst_table tar_subst_stable = {
    "[^/]", /* question_mark */
    ".*", /* star */
    NULL /* double_star */
};

/*
 * Substitution data for match_word(): dot
 */

static struct subst_table mword_dot_subst_table = {
    "[^.]", /* question_mark */
    "[^.]*", /* star */
    ".*" /* double_star */
};

/*
 * Substitution data for match_word(): slash
 */

static struct subst_table mword_slash_subst_table = {
    "[^/]", /* question_mark */
    "[^/]*", /* star */
    ".*" /* double_star */
};

/*
 * match_word() specific data:
 * - re_double_sep: anchored regex matching two separators;
 * - re_separator: regex matching the separator;
 * - re_begin_full: regex matching the separator, anchored at the beginning;
 * - re_end_full: regex matching the separator, andchored at the end.
 */

struct mword_regexes {
    const char *re_double_sep;
    const char *re_begin_full;
    const char *re_separator;
    const char *re_end_full;
};

static struct mword_regexes mword_dot_regexes = {
    "^\\.\\.$", /* re_double_sep */
    "^\\.", /* re_begin_full */
    "\\.", /* re_separator */
    "\\.$" /* re_end_full */
};

static struct mword_regexes mword_slash_regexes = {
    "^\\/\\/$", /* re_double_sep */
    "^\\/", /* re_begin_full */
    "\\/", /* re_separator */
    "\\/$" /* re_end_full */
};

/*
 * Regular expression caches, and a static mutex to protect initialization and
 * access. This may be unnecessarily coarse, but it is unknown at this time
 * whether GHashTable accesses are thread-safe, and get_regex_from_cache() may
 * be called from within threads, so play it safe.
 */

#if (GLIB_MAJOR_VERSION > 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION >= 31))
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wmissing-field-initializers"
  static GStaticMutex re_cache_mutex = G_STATIC_MUTEX_INIT;
# pragma GCC diagnostic pop
#else
  static GStaticMutex re_cache_mutex = G_STATIC_MUTEX_INIT;
#endif
static GHashTable *regex_cache = NULL, *regex_cache_newline = NULL;

/*
 * REGEX FUNCTIONS
 */

/*
 * Initialize regex caches. NOTE: this function MUST be called with
 * re_cache_mutex LOCKED, see get_regex_from_cache()
 */

static void init_regex_caches(void)
{
    static gboolean initialized = FALSE;

    if (initialized)
        return;

    regex_cache = g_hash_table_new(g_str_hash, g_str_equal);
    regex_cache_newline = g_hash_table_new(g_str_hash, g_str_equal);

    initialized = TRUE;
}

/*
 * Cleanup a regular expression by escaping all non alphanumeric characters, and
 * append beginning/end anchors if need be
 */

char *clean_regex(const char *str, gboolean anchor)
{
    const char *src;
    char *result, *dst;

    result = g_malloc(2 * strlen(str) + 3);
    dst = result;

    if (anchor)
        *dst++ = '^';

    for (src = str; *src; src++) {
        if (!g_ascii_isalnum((int) *src))
            *dst++ = '\\';
        *dst++ = *src;
    }

    if (anchor)
        *dst++ = '$';

    *dst = '\0';
    return result;
}

/*
 * Compile one regular expression. Return TRUE if the regex has been compiled
 * successfully. Otherwise, return FALSE and copy the error message into the
 * supplied regex_errbuf pointer. Also, we want to know whether flags should
 * include REG_NEWLINE (See regcomp(3) for details). Since this is the more
 * frequent case, add REG_NEWLINE to the default flags, and remove it only if
 * match_newline is set to FALSE.
 */

static gboolean do_regex_compile(const char *str, regex_t *regex,
    regex_errbuf *errbuf, gboolean match_newline)
{
    int flags = REG_EXTENDED | REG_NOSUB | REG_NEWLINE;
    int result;

    if (!match_newline)
	flags &= ~REG_NEWLINE;

    result = regcomp(regex, str, flags);

    if (!result)
        return TRUE;

    regerror(result, regex, *errbuf, sizeof(*errbuf));
    return FALSE;
}

/*
 * Get an already compiled buffer from the regex cache. If the regex is not in
 * the cache, allocate a new one and compile it using do_regex_compile(). If the
 * compile fails, call regfree() on the object and return NULL to the caller. If
 * it does succeed, put the regex buffer in cache and return a pointer to it.
 */

static regex_t *get_regex_from_cache(const char *re_str, regex_errbuf *errbuf,
    gboolean match_newline)
{
    regex_t *ret;
    GHashTable *cache;

    g_static_mutex_lock(&re_cache_mutex);

    init_regex_caches();

    cache = (match_newline) ? regex_cache_newline: regex_cache;
    ret = g_hash_table_lookup(cache, re_str);

    if (ret)
        goto out;

    ret = g_new(regex_t, 1);

    if (do_regex_compile(re_str, ret, errbuf, match_newline)) {
        g_hash_table_insert(cache, g_strdup(re_str), ret);
        goto out;
    }

    regfree(ret);
    g_free(ret);
    ret = NULL;

out:
    g_static_mutex_unlock(&re_cache_mutex);
    return ret;
}

/*
 * Validate one regular expression using do_regex_compile(), and return NULL if
 * the regex is valid, or the error message otherwise.
 */

char *validate_regexp(const char *regex)
{
    regex_t regc;
    static regex_errbuf errmsg;
    gboolean valid;

    valid = do_regex_compile(regex, &regc, &errmsg, TRUE);

    regfree(&regc);
    return (valid) ? NULL : errmsg;
}

/*
 * See if a string matches a compiled regular expression. Return one of MATCH_*
 * defined above. If, for some reason, regexec() returns something other than
 * not 0 or REG_NOMATCH, return MATCH_ERROR and print the error message in the
 * supplied regex_errbuf.
 */

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

    regerror(result, regex, *errbuf, sizeof(*errbuf));
    return MATCH_ERROR;
}

/*
 * Try and match a string against a regular expression, using
 * do_regex_compile() and try_match(). Exit early if the regex didn't compile
 * or there was an error during matching.
 */

int do_match(const char *regex, const char *str, gboolean match_newline)
{
    regex_t *re;
    int result;
    regex_errbuf errmsg;

    re = get_regex_from_cache(regex, &errmsg, match_newline);

    if (!re)
        error("regex \"%s\": %s", regex, errmsg);
        /*NOTREACHED*/

    result = try_match(re, str, &errmsg);

    if (result == MATCH_ERROR)
        error("regex \"%s\": %s", regex, errmsg);
        /*NOTREACHED*/

    return result;
}

/*
 * DISK/HOST EXPRESSION HANDLING
 */

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
 * Take a disk/host expression and turn it into a full-blown amglob (with
 * start and end anchors) following rules in amanda-match(7). The not_this_one
 * argument represents a character which is NOT meant to be special in this
 * case: '/' for disks and '.' for hosts.
 */

static char *full_amglob_from_expression(const char *str, char not_this_one)
{
    const char *src;
    char *result, *dst;

    result = g_malloc(2 * strlen(str) + 3);
    dst = result;

    *dst++ = '^';

    for (src = str; *src; src++) {
        if (should_be_escaped_except(*src, not_this_one))
            *dst++ = '\\';
        *dst++ = *src;
    }

    *dst++ = '$';
    *dst = '\0';
    return result;
}

/*
 * Turn a disk/host expression into a regex
 */

char *make_exact_disk_expression(const char *disk)
{
    return full_amglob_from_expression(disk, '/');
}

char *make_exact_host_expression(const char *host)
{
    return full_amglob_from_expression(host, '.');
}

/*
 * GLOB HANDLING, as per amanda-match(7)
 */

/*
 * Turn a glob into a regex.
 */

static char *amglob_to_regex(const char *str, const char *begin,
    const char *end, struct subst_table *table)
{
    const char *src;
    char *result, *dst;
    char c;
    size_t worst_case;
    gboolean double_star = (table->double_star != NULL);

    /*
     * There are two particular cases when building a regex out of a glob:
     * character classes (anything inside [...] or [!...] and quotes (anything
     * preceded by a backslash). We start with none being true.
     */

    gboolean in_character_class = FALSE, in_quote = FALSE;

    /*
     * Allocate enough space for our string. At worst, the allocated space is
     * the length of the following:
     * - beginning of regex;
     * - size of original string multiplied by worst-case expansion;
     * - end of regex;
     * - final 0.
     *
     * Calculate the worst case expansion by walking our struct subst_table.
     */

    worst_case = strlen(table->question_mark);

    if (worst_case < strlen(table->star))
        worst_case = strlen(table->star);

    if (double_star && worst_case < strlen(table->double_star))
        worst_case = strlen(table->double_star);

    result = g_malloc(strlen(begin) + strlen(str) * worst_case + strlen(end) + 1);

    /*
     * Start by copying the beginning of the regex...
     */

    dst = g_stpcpy(result, begin);

    /*
     * ... Now to the meat of it.
     */

    for (src = str; *src; src++) {
        c = *src;

        /*
         * First, check that we're in a character class: each and every
         * character can be copied as is. We only need to be careful is the
         * character is a closing bracket: it will end the character class IF
         * AND ONLY IF it is not preceded by a backslash.
         */

        if (in_character_class) {
            in_character_class = ((c != ']') || (*(src - 1) == '\\'));
            goto straight_copy;
        }

        /*
         * Are we in a quote? If yes, it is really simple: copy the current
         * character, close the quote, the end.
         */

        if (in_quote) {
            in_quote = FALSE;
            goto straight_copy;
        }

        /*
         * The only thing left to handle now is the "normal" case: we are not in
         * a character class nor in a quote.
         */

        if (c == '\\') {
            /*
             * Backslash: append it, and open a new quote.
             */
            in_quote = TRUE;
            goto straight_copy;
        } else if (c == '[') {
            /*
             * Opening bracket: the beginning of a character class.
             *
             * Look ahead the next character: if it's an exclamation mark, then
             * this is a complemented character class; append a caret to make
             * the result string regex-friendly, and forward one character in
             * advance.
             */
            *dst++ = c;
            in_character_class = TRUE;
            if (*(src + 1) == '!') {
                *dst++ = '^';
                src++;
            }
        } else if (IS_REGEX_META(c)) {
            /*
             * Regex metacharacter (except for ? and *, see below): append a
             * backslash, and then the character itself.
             */
            *dst++ = '\\';
            goto straight_copy;
        } else if (c == '?')
            /*
             * Question mark: take the subsitution string out of our subst_table
             * and append it to the string.
             */
            dst = g_stpcpy(dst, table->question_mark);
        else if (c == '*') {
            /*
             * Star: append the subsitution string found in our subst_table.
             * However, look forward the next character: if it's yet another
             * star, then see if there is a substitution string for the double
             * star and append this one instead.
             *
             * FIXME: this means that two consecutive stars in a glob string
             * where there is no substition for double_star can lead to
             * exponential regex execution time: consider [^/]*[^/]*.
             */
            const char *p = table->star;
            if (double_star && *(src + 1) == '*') {
                src++;
                p = table->double_star;
            }
            dst = g_stpcpy(dst, p);
        } else {
            /*
             * Any other character: append each time.
             */
straight_copy:
            *dst++ = c;
        }
    }

    /*
     * Done, now append the end, ONLY if we are not in a quote - a lone
     * backslash at the end of a glob is illegal, just leave it as it, it will
     * make the regex compile fail.
     */

    if (!in_quote)
        dst = g_stpcpy(dst, end);
    /*
     * Finalize, return.
     */

    *dst = '\0';
    return result;
}

/*
 * File globs
 */

char *glob_to_regex(const char *glob)
{
    return amglob_to_regex(glob, "^", "$", &glob_subst_stable);
}

int match_glob(const char *glob, const char *str)
{
    char *regex;
    regex_t *re;
    int result;
    regex_errbuf errmsg;

    regex = glob_to_regex(glob);
    re = get_regex_from_cache(regex, &errmsg, TRUE);

    if (!re)
        error("glob \"%s\" -> regex \"%s\": %s", glob, regex, errmsg);
        /*NOTREACHED*/

    result = try_match(re, str, &errmsg);

    if (result == MATCH_ERROR)
        error("glob \"%s\" -> regex \"%s\": %s", glob, regex, errmsg);
        /*NOTREACHED*/

    g_free(regex);

    return result;
}

char *validate_glob(const char *glob)
{
    char *regex, *ret = NULL;
    regex_t regc;
    static regex_errbuf errmsg;

    regex = glob_to_regex(glob);

    if (!do_regex_compile(regex, &regc, &errmsg, TRUE))
        ret = errmsg;

    regfree(&regc);
    g_free(regex);
    return ret;
}

/*
 * Tar globs
 */

static char *tar_to_regex(const char *glob)
{
    return amglob_to_regex(glob, "(^|/)", "($|/)", &tar_subst_stable);
}

int match_tar(const char *glob, const char *str)
{
    char *regex;
    regex_t *re;
    int result;
    regex_errbuf errmsg;

    regex = tar_to_regex(glob);
    re = get_regex_from_cache(regex, &errmsg, TRUE);

    if (!re)
        error("glob \"%s\" -> regex \"%s\": %s", glob, regex, errmsg);
        /*NOTREACHED*/

    result = try_match(re, str, &errmsg);

    if (result == MATCH_ERROR)
        error("glob \"%s\" -> regex \"%s\": %s", glob, regex, errmsg);
        /*NOTREACHED*/

    g_free(regex);

    return result;
}

/*
 * DISK/HOST MATCHING
 *
 * The functions below wrap input strings with separators and attempt to match
 * the result. The core of the operation is the match_word() function.
 */

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
            return !(!g_str_equal(glob, len2_1) && !g_str_equal(glob, len2_2));
        case 3:
            return g_str_equal(glob, len3);
        default:
            return FALSE;
    }
}

/*
 * Given a word and a separator as an argument, wrap the word with separators -
 * if need be. For instance, if '/' is the separator, the rules are:
 *
 * - "" -> "/"
 * - "/" -> "//"
 * - "//" -> left alone
 * - "xxx" -> "/xxx/"
 * - "/xxx" -> "/xxx/"
 * - "xxx/" -> "/xxx/"
 * - "/xxx/" -> left alone
 *
 * (note that xxx here may contain the separator as well)
 *
 * Note that the returned string is dynamically allocated: it is up to the
 * caller to free it. Note also that the first argument MUST NOT BE NULL.
 */

static char *wrap_word(const char *word, const char separator, const char *glob)
{
    size_t len = strlen(word);
    size_t len_glob = strlen(glob);
    char *result, *p;

    /*
     * We allocate for the worst case, which is two bytes more than the input
     * (have to prepend and append a separator).
     */
    result = g_malloc(len + 3);
    p = result;

    /*
     * Zero-length: separator only
     */

    if (len == 0) {
        *p++ = separator;
        goto out;
    }

    /*
     * Length is one: if the only character is the separator only, the result
     * string is two separators
     */

    if (len == 1 && word[0] == separator) {
        *p++ = separator;
        *p++ = separator;
        goto out;
    }

    /*
     * Otherwise: prepend the separator if needed, append the separator if
     * needed.
     */

    if (word[0] != separator && glob[0] != '^')
        *p++ = separator;

    p = g_stpcpy(p, word);

    if (word[len - 1] != separator && glob[len_glob-1] != '$')
        *p++ = separator;

out:
    *p++ = '\0';
    return result;
}

static int match_word(const char *glob, const char *word, const char separator)
{
    char *wrapped_word = wrap_word(word, separator, glob);
    struct mword_regexes *regexes = &mword_slash_regexes;
    struct subst_table *table = &mword_slash_subst_table;
    gboolean not_slash = (separator != '/');
    int ret;

    /*
     * We only expect two separators: '/' or '.'. If it's not '/', it has to be
     * the other one...
     */
    if (not_slash) {
        regexes = &mword_dot_regexes;
        table = &mword_dot_subst_table;
    }

    if(glob_is_separator_only(glob, separator)) {
        ret = do_match(regexes->re_double_sep, wrapped_word, TRUE);
        goto out;
    } else {
        /*
         * Unlike what happens for tar and disk expressions, we need to
         * calculate the beginning and end of our regex before calling
         * amglob_to_regex().
         */

        const char *begin, *end;
        char *glob_copy = g_strdup(glob);
        char *p, *g = glob_copy;
        char *regex;

        /*
         * Calculate the beginning of the regex:
         * - by default, it is an unanchored separator;
         * - if the glob begins with a caret, make that an anchored separator,
         *   and increment g appropriately;
         * - if it begins with a separator, make it the empty string.
         */

        p = glob_copy;
        begin = regexes->re_separator;

        if (*p == '^') {
            begin = "^";
            p++, g++;
            if (*p == separator) {
		begin = regexes->re_begin_full;
                g++;
	    }
        } else if (*p == separator)
            begin = "";

        /*
         * Calculate the end of the regex:
         * - an unanchored separator by default;
         * - if the last character is a backslash or the separator itself, it
         *   should be the empty string;
         * - if it is a dollar sign, overwrite it with 0 and look at the
         *   character before it: if it is the separator, only anchor at the
         *   end, otherwise, add a separator before the anchor.
         */

        p = &(glob_copy[strlen(glob_copy) - 1]);
        end = regexes->re_separator;
        if (*p == '\\' || *p == separator) {
            end = "";
        } else if (*p == '$') {
            char prev = *(p - 1);
            *p = '\0';
	    if (prev == separator) {
		*(p-1) = '\0';
		if (p-2 >= glob_copy) {
		    prev = *(p - 2);
		    if (prev == '\\') {
			*(p-2) = '\0';
		    }
		}
		end = regexes->re_end_full;
	    } else {
		end = "$";
	    }
        }

        regex = amglob_to_regex(g, begin, end, table);
        ret = do_match(regex, wrapped_word, TRUE);

        g_free(glob_copy);
        g_free(regex);
    }

out:
    g_free(wrapped_word);
    return ret;
}

/*
 * Match a host expression
 */

int match_host(const char *glob, const char *host)
{
    char *lglob, *lhost;
    int ret;

    lglob = g_ascii_strdown(glob, -1);
    lhost = g_ascii_strdown(host, -1);

    ret = match_word(lglob, lhost, '.');

    g_free(lglob);
    g_free(lhost);
    return ret;
}

/*
 * Match a disk expression. Not as straightforward, since Windows paths must be
 * accounted for.
 */

/*
 * Convert a disk and glob from Windows expressed paths (backslashes) into Unix
 * paths (slashes).
 *
 * Note: the resulting string is dynamically allocated, it is up to the caller
 * to free it.
 *
 * Note 2: UNC in convert_unc_to_unix stands for Uniform Naming Convention.
 */

static char *convert_unc_to_unix(const char *unc)
{
    char *result = g_strdup(unc);
    return g_strdelimit(result, "\\", '/');
}

static char *convert_winglob_to_unix(const char *glob)
{
    const char *src;
    char *result, *dst;
    result = g_malloc(strlen(glob) + 1);
    dst = result;

    for (src = glob; *src; src++) {
        if (*src == '\\' && *(src + 1) == '\\') {
            *dst++ = '/';
            src++;
            continue;
        }
        *dst++ = *src;
    }
    *dst = '\0';
    return result;
}

/*
 * Match a disk expression
 */

int match_disk(const char *glob, const char *disk)
{
    char *glob2 = NULL, *disk2 = NULL;
    const char *g = glob, *d = disk;
    int result;

    /*
     * Check whether our disk potentially refers to a Windows share (the first
     * two characters are '\' and there is no / in the word at all): if yes,
     * build Unix paths instead and pass those as arguments to match_word()
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
     * We can g_free(NULL), so this is "safe"
     */
    g_free(glob2);
    g_free(disk2);

    return result;
}

/*
 * TIMESTAMPS/LEVEL MATCHING
 */

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

    if(strlen(dateexp) < 1) {
	goto illegal;
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
	if (len < len_suffix) {
	    goto illegal;
	}
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
	    return (g_str_equal(datestamp, mydateexp));
	}
	else {
	    return (g_str_has_prefix(datestamp, mydateexp));
	}
    }
illegal:
	error("Illegal datestamp expression %s", dateexp);
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
	error("Illegal level expression %s", levelexp);
	/*NOTREACHED*/
    }

    if(levelexp[0] == '^') {
	strncpy(mylevelexp, levelexp+1, strlen(levelexp)-1);
	mylevelexp[strlen(levelexp)-1] = '\0';
	if (strlen(levelexp) == 0) {
	    error("Illegal level expression %s", levelexp);
	    /*NOTREACHED*/
	}
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
	    return (g_str_equal(level, mylevelexp));
	}
	else {
	    return (g_str_has_prefix(level, mylevelexp));
	}
    }
illegal:
    error("Illegal level expression %s", levelexp);
    /*NOTREACHED*/
}
