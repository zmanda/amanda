/*
 * the outer shell of regexec()
 *
 * This file includes engine.c *twice*, after muchos fiddling with the
 * macros that code uses.  This lets the same code operate on two different
 * representations for state sets.
 */
#include "amanda.h"
#include <regex.h>
#include "utils.h"
#include "regex2.h"

#undef ISSET

/* macros for manipulating states, small version */
#define	states	unsigned long
#define	states1	states		/* for later use in regexec() decision */
#define	CLEAR(v)	(((v) = 0), (void)(v))
#define	SET0(v, n)	((v) &= ~(MAKE_UNSIGNED_LONG(1) << (n)))
#define	SET1(v, n)	((v) |= (MAKE_UNSIGNED_LONG(1)) << (n))
#define	ISSET(v, n)	((v) & ((MAKE_UNSIGNED_LONG(1)) << (n)))
#define	ASSIGN(d, s)	((d) = (s), (void)(d))
#define	EQ(a, b)	((a) == (b))
#define	STATEVARS	int dummy	/* dummy version */
#define	STATESETUP(m, n)	/* nothing */
#define	STATETEARDOWN(m)	/* nothing */
#define	SETUP(v)	((v) = 0)
#define	onestate	sopno
#define	INIT(o, n)	((o) = (sopno)1 << (n))
#define	INC(o)	((o) <<= 1)
#define	ISSTATEIN(v, o)	((v) & (o))
/* some abbreviations; note that some of these know variable names! */
/* do "if I'm here, I can also be there" etc without branches */
#define	FWD(dst, src, n)	((dst) |= ((unsigned long)(src)&(here)) << (n))
#define	BACK(dst, src, n)	((dst) |= ((unsigned long)(src)&(here)) >> (n))
#define	ISSETBACK(v, n)	((v) & ((long)here >> (n)))
/* function names */
#define SNAMES			/* engine.c looks after details */

#include "engine.c"

/* now undo things */
#undef	states
#undef	CLEAR
#undef	SET0
#undef	SET1
#undef	ISSET
#undef	ASSIGN
#undef	EQ
#undef	STATEVARS
#undef	STATESETUP
#undef	STATETEARDOWN
#undef	SETUP
#undef	onestate
#undef	INIT
#undef	INC
#undef	ISSTATEIN
#undef	FWD
#undef	BACK
#undef	ISSETBACK
#undef	SNAMES

/* macros for manipulating states, large version */
#define	states	char *
#define	CLEAR(v)	memset((v), 0, (size_t)m->g->nstates)
#define	SET0(v, n)	((v)[n] = 0)
#define	SET1(v, n)	((v)[n] = 1)
#define	ISSET(v, n)	((v)[n])
#define	ASSIGN(d, s)	memcpy((d), (s), (size_t)m->g->nstates)
#define	EQ(a, b)	(memcmp((a), (b), (size_t)m->g->nstates) == 0)
#define	STATEVARS	int vn; char *space
#define	STATESETUP(m, nv) do {						\
	    (m)->space = malloc((size_t)(nv)*(size_t)(m)->g->nstates);	\
	    if ((m)->space == NULL)					\
		return(REG_ESPACE);					\
	    (m)->vn = 0;						\
} while (0);

#define	STATETEARDOWN(m)	{ free((m)->space); }
#define	SETUP(v)	((v) = &m->space[m->vn++ * m->g->nstates])
#define	onestate	int
#define	INIT(o, n)	((o) = (n))
#define	INC(o)	((o)++)
#define	ISSTATEIN(v, o)	((v)[o])
/* some abbreviations; note that some of these know variable names! */
/* do "if I'm here, I can also be there" etc without branches */
#define	FWD(dst, src, n)	((dst)[here+(n)] |= (src)[here])
#define	BACK(dst, src, n)	((dst)[here-(n)] |= (src)[here])
#define	ISSETBACK(v, n)	((v)[here - (n)])
/* function names */
#define	LNAMES			/* flag */

#include "engine.c"

/*
 - regexec - interface for matching
 = int regexec(regex_t *preg, const char *string, size_t nmatch, regmatch_t pmatch[], int eflags);
 = #define	REG_NOTBOL	00001
 = #define	REG_NOTEOL	00002
 = #define	REG_STARTEND	00004
 = #define	REG_TRACE	00400	// tracing of execution
 = #define	REG_LARGE	01000	// force large representation
 = #define	REG_BACKR	02000	// force use of backref code
 *
 * We put this here so we can exploit knowledge of the state representation
 * when choosing which matcher to call.  Also, by this point the matchers
 * have been prototyped.
 */
int				/* 0 success, REG_NOMATCH failure */
regexec(
    regex_t *preg,
    const char *string,
    size_t nmatch,
    regmatch_t pmatch[],
    int eflags)
{
	/*@ignore@*/
	register struct re_guts *g = preg->re_g;
#ifdef REDEBUG
#	define	GOODFLAGS(f)	(f)
#else
#	define	GOODFLAGS(f)	((f)&(REG_NOTBOL|REG_NOTEOL|REG_STARTEND))
#endif

	if (preg->re_magic != MAGIC1 || g->magic != MAGIC2)
		return(REG_BADPAT);
	/*@end@*/
	assert(!(g->iflags&BAD));
	if (g->iflags&BAD)		/* backstop for no-debug case */
		return(REG_BADPAT);
	eflags = GOODFLAGS(eflags);

	if ((g->nstates <= ((sopno)(CHAR_BIT * SIZEOF(states1))) &&
	    !(eflags & REG_LARGE)))
		return(smatcher(g, string, nmatch, pmatch, eflags));
	return(lmatcher(g, string, nmatch, pmatch, eflags));
}
