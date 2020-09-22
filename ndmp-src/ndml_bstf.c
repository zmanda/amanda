/*
 * Copyright 2000
 *	Traakan, Inc., Los Altos, CA
 *	All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Project:  NDMJOB
 * Ident:    $Id: $
 *
 * Description:
 *	Binary Search Text File (BSTF)
 *
 *	Use conventional binary search method on a sorted text
 *	file. The file MUST be sorted in ascending, lexicographic
 *	order. This is the default order of sort(1).
 */


#include "ndmlib.h"



#ifdef SELF_TEST
int	n_seek, n_align, n_line, n_getc;
#endif


#define MIN_DELTA	2048


typedef
struct ndmbstf_probe {
    off_t        pos;
    const char   key[0];
} ndmbstf_probe_t;

typedef
struct ndmbstf_search {
    off_t        lower;
    off_t        upper;
    const char  *key;
} ndmbstf_search_t;

int ndmbstf_tree_compare(gconstpointer aptr, gconstpointer bptr, gpointer ignore) {
    return *((off_t *) aptr) - *((off_t *) bptr); // first in struct...
}

int ndmbstf_tree_search(gconstpointer node, gconstpointer unkn) 
{
    ndmbstf_probe_t *probe = (ndmbstf_probe_t *) node;
    ndmbstf_search_t *srch = (ndmbstf_search_t *) unkn;
    // (ideally ranges from -128 to +127)
    int r = strncmp(srch->key, probe->key, strlen(srch->key)); // move before or after?
    // static char *dir_label[] = { 
    //     " << ", " == ", " >> "
    // };

    // r is positive -> [ 0 | -(-1) ] == 1
    // r is negative -> [ -1 | -(0) ] == -1
    // r is zero     -> [ 0 | -(0) ] == 0
    r = ( r >> 8 ) | -( -r >> 8 );

    // [just overwrite old values until done]
    if ( r <= 0 ) srch->upper = probe->pos; // search here or below
    if ( r >= 0 ) srch->lower = probe->pos; // search here or above

    // fprintf(stderr,"-------- probe %s: %8ld %s %8ld: (%8ld %s)\n",
    //    srch->key, srch->lower, dir_label[1+r], srch->upper, probe->pos, probe->key);

    return r; // range was probably narrowed
}

void *ndmfhdb_create_cache()
{
    // ordered by position.. (assumed to be sorted!)
    return g_tree_new_full(&ndmbstf_tree_compare, NULL, &free, NULL); // must free the key memory upon destroy (only)
}

void ndmfhdb_clear_cache(void **fpcache)
{
    GTree **treep = (GTree **) fpcache;
    g_tree_destroy(*treep);
    *treep = NULL;
}

bool_t ndmfhdb_traverse_check(gpointer key, gpointer *data, gpointer user)
{
    ndmbstf_probe_t *probe = (ndmbstf_probe_t*) key;
    g_assert(!user && !data && probe);
    g_assert(probe->pos < 500000);
    return 0;
}

/*
 * ndmbstf_first()
 * ndmbstf_first_with_bounds()
 *
 * Returns:
 *	<0	Error
 *	   -1	EOF
 *	   -2	Malformed line/file
 *	   -3	fseek() to determine file size failed
 *	   -4	fseek() to hop failed
 *	=0	First line found in buf[], does not match key
 *	>0	Length of first matching line in buf[]
 */

int
ndmbstf_first (
  FILE *fp,			/* the file to search */
  char *key,			/* what we're looking for */
  char *buf,			/* returned line */
  unsigned max_buf,
  void **cachep)		/* maximum lenght of buf (sizeof (buf)) */
{
    GTree **treep = (GTree **) cachep;
    ndmbstf_search_t srch = { .lower=0, .upper=0, .key = key };
    ndmbstf_probe_t *match = NULL;

    // writes lower/upper pair found (or equal if found)
    if ( treep && *treep ) {
        match = g_tree_search(*treep, &ndmbstf_tree_search, &srch);
    }

    // full hit?  line of this length was stored earlier.
    if ( match ) {
        int len = strlen(key);

        if ( len > max_buf-1 ) len = max_buf-1;
        strncpy(buf,match->key,len);
        buf[len] = 0; // in case
        return len;
    }

    // upper of 0 still implies end-of-file
    int r = ndmbstf_first_with_bounds (fp, key, buf, max_buf, srch.lower, srch.upper);

    // cannot do more if we matched a location in our cache
    if ( ! treep || r < 0 )
           return r;                                        /// RETURN (no need to add to cache)

    // no cache-match .. but a valid location!

    if ( !*treep ) {
        *treep = ndmfhdb_create_cache();
    }

    ndmbstf_probe_t *probe = (ndmbstf_probe_t *) malloc(sizeof(ndmbstf_probe_t) + strlen(buf) + 1);
    probe->pos = ftell(fp) - (r+1) - 1;  // get offset *before* this line .. [incl two \ns]
    strcpy((char *)probe->key,buf); // use result to find it

    g_tree_insert(*treep, probe, NULL); // no need for value from *_lookup fxns
    // g_tree_traverse(*treep, ndmfhdb_traverse_check, G_PRE_ORDER, NULL);

    return r; /// hopefully cached limits next time
}

int
ndmbstf_first_with_bounds (
  FILE *fp,			/* the file to search */
  char *key,			/* what we're looking for */
  char *buf,			/* returned line */
  unsigned max_buf,		/* maximum lenght of buf (sizeof (buf)) */
  off_t lower_bound,		/* offset, to skip headers, usually 0 */
  off_t upper_bound)		/* 0->don't know, >0 limit */
{
	off_t		off;
	off_t		lower, upper;		/* bounds */
	off_t		delta;
	int		rc, buf_len;

	if (upper_bound == 0) {
                struct stat st;

		/*
		 * Determine the file size using fseek()/ftell()
		 */
                if ( fstat(fileno(fp),&st) < 0 )
			return -3;
		upper_bound = st.st_size;
	}

	/*
	 * Set lower and upper bounds of the binary search
	 */
	lower = lower_bound;
	upper = upper_bound;
	for (;;) {
		/*
		 * Determine the delta (distance) between the current
		 * lower and upper bounds. If delta is small, it is more
		 * efficient to do a linear search than to continue
		 * seeking. This is because stdio will pre-read
		 * portions no matter what and fseek()ing will discard
		 * the pre-read portions. MIN_DELTA is the approximation
		 * of the stdio pre-read size. Better to just
		 * linearly process the pre-read portion in the
		 * hopes that our answer is already sitting in the
		 * stdio buffer.
		 */
		delta = upper - lower;
		if (delta <= MIN_DELTA)
			break;

		/*
		 * Seek to the first line after the midpoint
		 * between the lower and upper bounds.
		 */

		off = lower + delta / 2;
		rc = ndmbstf_seek_and_align (fp, &off);
		if (rc < 0) {
			if (rc == EOF) {
				/*
				 * Alignment found a very long line without
				 * a \n at the end of the file. All we
				 * can do now is try a linear search
				 * from the current lower bound.
				 */
			}
			return -4;	/* fseek() for hop failed */
		}

		/*
		 * Read the next line up into buf[].
		 */

		buf_len = ndmbstf_getline (fp, buf, max_buf);
		if (buf_len <= 0) {
			/*
			 * EOF, or malformed line. All we can do now
			 * is try a linear search from the current
			 * lower bound.
			 */
			break;
		}

		/*
		 * This is the crucial point.
		 *
		 *   buf[]  contains a line just read.
		 *   off    points the the line we just read.
		 *   key[]  is what we're looking for.
		 *
		 * Is the objective line (what we're trying to find)
		 * somewhere between lower..off or between off..upper.
		 */
		rc = ndmbstf_compare (key, buf);
		if (rc > 0) {
			/* key>buf. Objective somewhere in off..upper */
			lower = off;
		} else {
			/*
			 * key<=buf. Objective somewhere in lower..off
			 * Notice that if key==buf, there still might
			 * be a line ==key before the one we just
			 * read. There might be hundreds of such
			 * lines. The objective is the FIRST line
			 * greater than or equal to the key.
			 * This might be it. It might not.
			 * So, keep searching.
			 */
			upper = off;
		}
	}

	/*
	 * Do an unbounded linear search begining at the
	 * lower bound.
	 */

	off = lower;
	rc = ndmbstf_seek_and_align (fp, &off);
	if (rc < 0) {
		if (rc == EOF) {
			/*
			 * Alignment found a very long line without
			 * a \n at the end of the file. All we
			 * can do is give up.
			 */
			return -2;
		}
		return -4;	/* fseek() for hop failed */
	}

	/*
	 * Read the next line up into buf[].
	 */
	for (;;) {
		buf_len = ndmbstf_getline (fp, buf, max_buf);

		if (buf_len <= 0) {
			if (buf_len == EOF)
				break;		/* at end of file */
			return -2;
		}

		rc = ndmbstf_compare (key, buf);
		if (rc == 0) {
			/* match */
			return buf_len;
		}
		if (rc < 0)
			return 0;	/* have line but it doesn't match */
	}

	return EOF;
}

int
ndmbstf_next (
  FILE *fp,			/* the file to search */
  char *key,			/* what we're looking for */
  char *buf,			/* returned line */
  unsigned max_buf)		/* maximum lenght of buf (sizeof (buf)) */
{
	int		rc, buf_len;

	/*
	 * Read the next line up into buf[].
	 */
	buf_len = ndmbstf_getline (fp, buf, max_buf);

	if (buf_len <= 0) {
		if (buf_len == EOF)
			return EOF;		/* at end of file */
		return -2;			/* malformed line */
	}

	rc = ndmbstf_compare (key, buf);
	if (rc == 0) {
		/* match */
		return buf_len;
	} else {
		return 0;	/* have line but it doesn't match */
	}
}


/*
 * ndmbstr_getline()
 *
 * Returns:
 *	<0	Error
 *	   -1	EOF
 *	   -2	Malformed line
 *	>=0	Length of buf
 */

int
ndmbstf_getline (FILE *fp, char *buf, unsigned max_buf)
{
	char *		p = buf;
	char *		p_end = buf + max_buf - 2;
	int		c;

	while ((c = getc(fp)) != EOF) {
#ifdef SELF_TEST
		n_getc++;
#endif
		if (c == '\n')
			break;
		if (p < p_end)
			*p++ = c;
	}
	*p = 0;

	if (c == EOF) {
		if (p > buf)
			return -2;	/* malformed line */
		return EOF;
	}

#ifdef SELF_TEST
	n_line++;
#endif
	return p - buf;
}

int
ndmbstf_seek_and_align (FILE *fp, off_t *off)
{
	int		c;

	if (fseeko (fp, *off, SEEK_SET) == -1) {
		return -2;
	}
#ifdef SELF_TEST
	n_seek++;
#endif

	/*
	 * There is a slim chance that we're at the
	 * true begining of a line. Too slim.
	 * Scan forward discarding the trailing
	 * portion of the line we just fseek()ed
	 * to, and leave the stdio stream positioned
	 * for the subsequent line. Notice
	 * we keep off upated so that it reflects
	 * the seek position of the stdio stream.
	 */

	while ((c = getc(fp)) != EOF) {
		*off += 1;
#ifdef SELF_TEST
		n_align++;
#endif
		if (c == '\n')
			break;
	}
	if (c == EOF) {
		/* at end of file */
		return EOF;
	}

	return 0;
}



/*
 * ndmbstf_compare()
 *
 * Given a key[] and a buf[], return whether or not they match.
 * This effectively is strncmp (key, buf, strlen(key)).
 * Because of the cost of the call to strlen(), we implement
 * it ndmbstf_compare() here with the exact semantics.
 *
 * Return values are:
 *	<0	No match, key[] less than buf[]
 *	=0	Match, the key[] is a prefix for buf[]
 *	>0	No match, key[] greater than buf[]
 */

int
ndmbstf_compare (char *key, char *buf)
{
	char *		p = key;
	char *		q = buf;

	while (*p != 0 && *p == *q) {
		p++;
		q++;
	}

	if (*p == 0)
		return 0;	/* entire key matched */
	else
		return *p - *q;
}


/*
 * ndmbstf_match()
 *
 * A simple wrapper around ndmbstf_compare() with an
 * easy-to-use return value..
 *
 * Returns:
 *	!0	match
 *	=0	no match
 */

int
ndmbstf_match (char *key, char *buf)
{
	return ndmbstf_compare (key, buf) == 0;
}




#ifdef SELF_TEST
int
main (int ac, char *av[])
{
	int		i;
	FILE *		fp;
	int		verbose = 1;
	int		total_n_match = 0;
	int		total_n_no_match = 0;
	int		total_n_error = 0;

	i = 1;
	if (i < ac && strcmp (av[i], "-q") == 0) {
		i++;
		verbose = 0;
	}

	if (ac < i+2) {
		printf ("usage: %s [-q] FILE KEY ...\n", av[0]);
		return 1;
	}

	fp = fopen (av[i], "r");
	if (!fp) {
		perror (av[i]);
		return 2;
	}

	for (i++; i < ac; i++) {
		char		buf[512];
		char *		key = av[i];
		int		n_match = 0;
		int		rc;

		rc = ndmbstf_first (fp, key, buf, sizeof buf);
		if (rc < 0) {
			printf ("Key '%s' err=%d\n", key, rc);
			total_n_error++;
			continue;
		}

		if (rc == 0) {
			printf ("Key '%s' no matches\n", key);
			total_n_no_match++;
			continue;
		}

		if (verbose)
			printf ("Key '%s'\n", key);
		for (; rc > 0; rc = ndmbstf_next (fp, key, buf, sizeof buf)) {
			n_match++;
			if (verbose)
				printf ("    %2d: '%s'\n", n_match, buf);
		}
		total_n_match += n_match;
	}

	fclose (fp);

	printf ("n_match=%d n_miss=%d n_error=%d\n",
		total_n_match, total_n_no_match, total_n_error);
	printf ("n_seek=%d n_align=%d n_line=%d\n", n_seek, n_align, n_line);

	return 0;
}
#endif /* SELF_TEST */
