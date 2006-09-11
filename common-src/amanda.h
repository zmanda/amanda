/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
 * $Id: amanda.h,v 1.131 2006/07/25 18:27:56 martinea Exp $
 *
 * the central header file included by all amanda sources
 */
#ifndef AMANDA_H
#define AMANDA_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/*
 * Force large file source even if configure guesses wrong.
 */
#ifndef _LARGE_FILE_SOURCE
#define _LARGE_FILES 1
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE 1
#endif

#ifndef  _FILE_OFFSET_BITS
#define	_FILE_OFFSET_BITS 64
#endif

#ifdef HAVE_SYS_TYPES_H
#  include <sys/types.h>
#endif

/*
 * I would prefer that each Amanda module include only those system headers
 * that are locally needed, but on most Unixes the system header files are not
 * protected against multiple inclusion, so this can lead to problems.
 *
 * Also, some systems put key files in different places, so by including 
 * everything here the rest of the system is isolated from such things.
 */
#ifdef HAVE_ALLOCA_H
#  include <alloca.h>
#endif

/* from the autoconf documentation */
#ifdef HAVE_DIRENT_H
#  include <dirent.h>
#  define NAMLEN(dirent) strlen((dirent)->d_name)
#else
#  define dirent direct
#  define NAMLEN(dirent) (dirent)->d_namlen
#  if HAVE_SYS_NDIR_H
#    include <sys/ndir.h>
#  endif
#  if HAVE_SYS_DIR_H
#    include <sys/dir.h>
#  endif
#  if HAVE_NDIR_H
#    include <ndir.h>
#  endif
#endif

#ifdef HAVE_FCNTL_H
#  include <fcntl.h>
#endif

#ifdef HAVE_GRP_H
#  include <grp.h>
#endif

#if defined(USE_DB_H)
#  include <db.h>
#else
#if defined(USE_DBM_H)
#  include <dbm.h>
#else
#if defined(USE_GDBM_H)
#  include <gdbm.h>
#else
#if defined(USE_NDBM_H)
#  include <ndbm.h>
#endif
#endif
#endif
#endif

#ifdef HAVE_NETDB_H
#  include <netdb.h>
#endif

#ifdef TIME_WITH_SYS_TIME
#  include <sys/time.h>
#  include <time.h>
#else
#  ifdef HAVE_SYS_TIME_H
#    include <sys/time.h>
#  else
#    include <time.h>
#  endif
#endif

#ifdef HAVE_LIBC_H
#  include <libc.h>
#endif

#ifdef HAVE_STDLIB_H
#  include <stdlib.h>
#endif

#ifdef HAVE_LIBGEN_H
#  include <libgen.h>
#endif

#ifdef HAVE_STRING_H
#  include <string.h>
#endif

#ifdef HAVE_STRINGS_H
#  include <strings.h>
#endif

#ifdef HAVE_SYSLOG_H
#  include <syslog.h>
#endif

#ifdef HAVE_MATH_H
#  include <math.h>
#endif

#ifdef HAVE_SYS_FILE_H
#  include <sys/file.h>
#endif

#ifdef HAVE_SYS_IOCTL_H
#  include <sys/ioctl.h>
#endif

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#  include <sys/param.h>
#endif

#if defined(HAVE_SYS_IPC_H) && defined(HAVE_SYS_SHM_H)
#  include <sys/ipc.h>
#  include <sys/shm.h>
#else
#  ifdef HAVE_SYS_MMAN_H
#    include <sys/mman.h>
#  endif
#endif

#ifdef HAVE_SYS_SELECT_H
#  include <sys/select.h>
#endif

#ifdef HAVE_SYS_STAT_H
#  include <sys/stat.h>
#endif

#ifdef HAVE_SYS_UIO_H
#  include <sys/uio.h>
#else
struct iovec {
    void *iov_base;
    int iov_len;
};
#endif

#ifdef HAVE_WAIT_H
#  include <wait.h>
#endif

#ifdef HAVE_SYS_WAIT_H
#  include <sys/wait.h>
#endif

#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif

#ifdef WAIT_USES_INT
  typedef int amwait_t;
# ifndef WEXITSTATUS
#  define WEXITSTATUS(stat_val) (*(unsigned*)&(stat_val) >> 8)
# endif
# ifndef WTERMSIG
#  define WTERMSIG(stat_val) (*(unsigned*)&(stat_val) & 0x7F)
# endif
# ifndef WIFEXITED
#  define WIFEXITED(stat_val) ((*(unsigned*)&(stat_val) & 255) == 0)
# endif
#else
# ifdef WAIT_USES_UNION
   typedef union wait amwait_t;
#  ifndef WEXITSTATUS
#  define WEXITSTATUS(stat_val) (((amwait_t*)&(stat_val))->w_retcode)
#  endif
#  ifndef WTERMSIG
#   define WTERMSIG(stat_val) (((amwait_t*)&(stat_val))->w_termsig)
#  endif
#  ifndef WIFEXITED
#   define WIFEXITED(stat_val) (WTERMSIG(stat_val) == 0)
#  endif
# else
   typedef int amwait_t;
#  ifndef WEXITSTATUS
#   define WEXITSTATUS(stat_val) (*(unsigned*)&(stat_val) >> 8)
#  endif
#  ifndef WTERMSIG
#   define WTERMSIG(stat_val) (*(unsigned*)&(stat_val) & 0x7F)
#  endif
#  ifndef WIFEXITED
#   define WIFEXITED(stat_val) ((*(unsigned*)&(stat_val) & 255) == 0)
#  endif
# endif
#endif

#ifndef WIFSIGNALED
# define WIFSIGNALED(stat_val)	(WTERMSIG(stat_val) != 0)
#endif

#ifdef HAVE_UNISTD_H
#  include <unistd.h>
#endif

#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pwd.h>
#include <signal.h>
#include <setjmp.h>
#include <stdio.h>
#include <sys/resource.h>
#include <sys/socket.h>

#if !defined(CONFIGURE_TEST)
#  include "amanda-int.h"
#endif

#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

/*
 * The dbmalloc package comes from:
 *
 *  http://www.clark.net/pub/dickey/dbmalloc/dbmalloc.tar.gz
 *
 * or
 *
 *  ftp://gatekeeper.dec.com/pub/usenet/comp.sources.misc/volume32/dbmalloc/
 *
 * The following functions are sprinkled through the code, but are
 * disabled unless USE_DBMALLOC is defined:
 *
 *  malloc_enter(char *) -- stack trace for malloc reports
 *  malloc_leave(char *) -- stack trace for malloc reports
 *  malloc_mark(void *) -- mark an area as never to be free-d
 *  malloc_chain_check(void) -- check the malloc area now
 *  malloc_dump(int fd) -- report the malloc contents to a file descriptor
 *  malloc_list(int fd, ulong a, ulong b) -- report memory activated since
 *	history stamp a that is still active as of stamp b (leak check)
 *  malloc_inuse(ulong *h) -- create history stamp h and return the amount
 *	of memory currently in use.
 */

#ifdef USE_DBMALLOC
#include "dbmalloc.h"
#else
#define	malloc_enter(func)		((void)0)
#define	malloc_leave(func)		((void)0)
#define	malloc_mark(ptr)		((void)0)
#define	malloc_chain_check()		((void)0)
#define	malloc_dump(fd)			((void)0)
#define	malloc_list(a,b,c)		((void)0)
#define	malloc_inuse(hist)		(*(hist) = 0, 0)
#define	dbmalloc_caller_loc(x,y)	(x)
#endif

#if !defined(HAVE_SIGACTION) && defined(HAVE_SIGVEC)
/* quick'n'dirty hack for NextStep31 */
#  define sa_flags sv_flags
#  define sa_handler sv_handler
#  define sa_mask sv_mask
#  define sigaction sigvec
#  define sigemptyset(mask) /* no way to clear pending signals */
#endif

/*
 * Most Unixen declare errno in <errno.h>, some don't.  Some multithreaded
 * systems have errno as a per-thread macro.  So, we have to be careful.
 */
#ifndef errno
extern int errno;
#endif

/*
 * Some compilers have int for type of sizeof() some use size_t.
 * size_t is the one we want...
 */
#define	SIZEOF(x)	(size_t)sizeof(x)


/*
 * Some older BSD systems don't have these FD_ macros, so if not, provide them.
 */
#if !defined(FD_SET) || defined(LINT) || defined(__lint)
#  undef FD_SETSIZE
#  define FD_SETSIZE      (int)(SIZEOF(fd_set) * CHAR_BIT)

#  undef FD_SET
#  define FD_SET(n, p)    (((fd_set *)(p))->fds_bits[(n)/WORD_BIT] |= (int)((1 << ((n) % WORD_BIT))))

#  undef FD_CLR
#  define FD_CLR(n, p)    (((fd_set *)(p))->fds_bits[(n)/WORD_BIT] &= (int)(~(1 << ((n) % WORD_BIT))))

#  undef FD_ISSET
#  define FD_ISSET(n, p)  (((fd_set *)(p))->fds_bits[(n)/WORD_BIT] & (1 << ((n) % WORD_BIT)))

#  undef FD_ZERO
#  define FD_ZERO(p)      memset((p), 0, SIZEOF(*(p)))
#endif

#ifndef FD_COPY
#  define FD_COPY(p, q)   memcpy((q), (p), SIZEOF(*(p)))
#endif


/*
 * Define MAX_HOSTNAME_LENGTH as the size of arrays to hold hostname's.
 */
#undef  MAX_HOSTNAME_LENGTH
#define MAX_HOSTNAME_LENGTH 1025

/*
 * If void is broken, substitute char.
 */
#ifdef BROKEN_VOID
#  define void char
#endif

#define stringize(x) #x
#define stringconcat(x, y) x ## y

/*
 * So that we can use GNUC attributes (such as to get -Wall warnings
 * for printf-like functions).  Only do this in gcc 2.7 or later ...
 * it may work on earlier stuff, but why chance it.
 */
#if !defined(__GNUC__) || __GNUC__ < 2 || __GNUC_MINOR__ < 7 || defined(S_SPLINT_S) || defined(LINT) || defined(__lint)
#undef __attribute__
#define __attribute__(__x)
#endif

/*
 * assertions, but call error() instead of abort 
 */
#ifndef ASSERTIONS

#define assert(exp) ((void)0)

#else	/* ASSERTIONS */

#define assert(exp)	do {						\
    if (!(exp)) {							\
	onerror(abort);							\
	error("assert: %s false, file %s, line %d",			\
	   stringize(exp), __FILE__, __LINE__);				\
        /*NOTREACHED*/							\
    }									\
} while (0)

#endif	/* ASSERTIONS */

/*
 * print debug output, else compile to nothing.
 */

#ifdef DEBUG_CODE							/* { */
#   define dbopen(a)	debug_open(a)
#   define dbreopen(a,b) debug_reopen(a,b)
#   define dbrename(a,b) debug_rename(a,b)
#   define dbclose()	debug_close()
#   define dbprintf(p)	(debug_printf p)
#   define dbfd()	debug_fd()
#   define dbfp()	debug_fp()
#   define dbfn()	debug_fn()

extern void debug_open(char *subdir);
extern void debug_reopen(char *file, char *notation);
extern void debug_rename(char *config, char *subdir);
extern void debug_close(void);
extern void debug_printf(const char *format, ...)
    __attribute__ ((format (printf, 1, 2)));
extern int  debug_fd(void);
extern FILE *  debug_fp(void);
extern char *  debug_fn(void);
extern void set_debug_prefix_pid(pid_t);
extern char *debug_prefix(char *);
extern char *debug_prefix_time(char *);
#else									/* }{ */
#   define dbopen(a)
#   define dbreopen(a,b)
#   define dbclose()
#   define dbprintf(p)
#   define dbfd()	(-1)
#   define dbfp()	NULL
#   define dbfn()	NULL
#   define set_debug_prefix_pid(x)
#   define debug_prefix(x) get_pname()
#   define debug_prefix_time(x) get_pname()
#endif									/* } */

/* amanda #days calculation, with roundoff */

#define SECS_PER_DAY	(24*60*60)
#define days_diff(a, b)	(int)(((b) - (a) + SECS_PER_DAY/2) / SECS_PER_DAY)

/* Global constants.  */
#ifndef AMANDA_SERVICE_NAME
#define AMANDA_SERVICE_NAME "amanda"
#endif
#ifndef KAMANDA_SERVICE_NAME
#define KAMANDA_SERVICE_NAME "kamanda"
#endif
#ifndef SERVICE_SUFFIX
#define SERVICE_SUFFIX ""
#endif
#ifndef AMANDA_SERVICE_DEFAULT
#define AMANDA_SERVICE_DEFAULT	((in_port_t)10080)
#endif
#ifndef KAMANDA_SERVICE_DEFAULT
#define KAMANDA_SERVICE_DEFAULT	((in_port_t)10081)
#endif

#define am_round(v,u)	((((v) + (u) - 1) / (u)) * (u))
#define am_floor(v,u)	(((v) / (u)) * (u))

/* Holding disk block size.  Do not even think about changint this!  :-) */
#define DISK_BLOCK_KB		32
#define DISK_BLOCK_BYTES	(DISK_BLOCK_KB * 1024)

/* Maximum size of a tape block */
/* MAX_TAPE_BLOCK_KB is defined in config.h */
/* by configure --with-maxtapeblocksize     */
#define MAX_TAPE_BLOCK_BYTES (MAX_TAPE_BLOCK_KB*1024)

/* Maximum length of tape label, plus one for null-terminator. */
#define MAX_TAPE_LABEL_LEN (10240)
#define MAX_TAPE_LABEL_BUF (MAX_TAPE_LABEL_LEN+1)
#define MAX_TAPE_LABEL_FMT "%10240s"

/* Define miscellaneous amanda functions.  */
#define ERR_INTERACTIVE	1
#define ERR_SYSLOG	2
#define ERR_AMANDALOG	4

extern void   set_logerror(void (*f)(char *));
extern void   set_pname(char *pname);
extern char  *get_pname(void);
extern int    erroutput_type;
extern void   error(const char *format, ...)
    __attribute__ ((format (printf, 1, 2), noreturn));
extern void   errordump(const char *format, ...)
    __attribute__ ((format (printf, 1, 2), noreturn));
extern int    onerror(void (*errf)(void));

extern void *debug_alloc      (const char *c, int l, size_t size);
extern void *debug_newalloc   (const char *c, int l, void *old, size_t size);
extern char *debug_stralloc   (const char *c, int l, const char *str);
extern char *debug_newstralloc(const char *c, int l, char *oldstr,
			       const char *newstr);
extern const char *debug_caller_loc (const char *file, int line);
extern int debug_alloc_push (char *file, int line);
extern void debug_alloc_pop (void);

#define	alloc(s)		debug_alloc(__FILE__, __LINE__, (s))
#define	newalloc(p,s)		debug_newalloc(__FILE__, __LINE__, (p), (s))
#define	stralloc(s)		debug_stralloc(__FILE__, __LINE__, (s))
#define	newstralloc(p,s)	debug_newstralloc(__FILE__, __LINE__, (p), (s))

/*
 * Voodoo time.  We want to be able to mark these calls with the source
 * line, but CPP does not handle variable argument lists so we cannot
 * do what we did above (e.g. for alloc()).
 *
 * What we do is call a function to save the file and line number
 * and have it return "false".  That triggers the "?" operator to
 * the right side of the ":" which is a call to the debug version of
 * vstralloc/newvstralloc but without parameters.  The compiler gets
 * those from the next input tokens:
 *
 *  xx = vstralloc(a,b,NULL);
 *
 * becomes:
 *
 *  xx = debug_alloc_push(__FILE__,__LINE__)?0:debug_vstralloc(a,b,NULL);
 *
 * This works as long as vstralloc/newvstralloc are not part of anything
 * very complicated.  Assignment is fine, as is an argument to another
 * function (but you should not do that because it creates a memory leak).
 * This will not work in arithmetic or comparison, but it is unlikely
 * they are used like that.
 *
 *  xx = vstralloc(a,b,NULL);			OK
 *  return vstralloc(j,k,NULL);			OK
 *  sub(a, vstralloc(g,h,NULL), z);		OK, but a leak
 *  if(vstralloc(s,t,NULL) == NULL) { ...	NO, but unneeded
 *  xx = vstralloc(x,y,NULL) + 13;		NO, but why do it?
 */

#define vstralloc debug_alloc_push(__FILE__,__LINE__)?0:debug_vstralloc
#define newvstralloc debug_alloc_push(__FILE__,__LINE__)?0:debug_newvstralloc

extern char  *debug_vstralloc(const char *str, ...);
extern char  *debug_newvstralloc(char *oldstr, const char *newstr, ...);

#define	stralloc2(s1,s2)      vstralloc((s1),(s2),NULL)
#define	newstralloc2(p,s1,s2) newvstralloc((p),(s1),(s2),NULL)

/* Usage: vstrextend(foo, "bar, "baz", NULL). Extends the existing 
 * string, or allocates a brand new one. */
extern char *vstrextend(char **oldstr, ...);

extern /*@only@*/ /*@null@*/ char *debug_agets(const char *c, int l, FILE *file);
extern /*@only@*/ /*@null@*/ char *debug_areads(const char *c, int l, int fd);
#define agets(f)	      debug_agets(__FILE__,__LINE__,(f))
#define areads(f)	      debug_areads(__FILE__,__LINE__,(f))

extern int debug_amtable_alloc(const char *file,
				  int line,
				  void **table,
				  size_t *current,
				  size_t elsize,
				  size_t count,
				  int bump,
				  void (*init_func)(void *));

#define amtable_alloc(t,c,s,n,b,f) debug_amtable_alloc(__FILE__,      \
						     __LINE__,        \
						     (t),             \
						     (c),             \
						     (s),             \
						     (n),             \
						     (b),             \
						     (f))

extern void amtable_free(void **table, size_t *current);

extern uid_t  client_uid;
extern gid_t  client_gid;

void	safe_fd(int fd_start, int fd_count);
void	safe_cd(void);
void	save_core(void);
char **	safe_env(void);
char *	validate_regexp(const char *regex);
char *	validate_glob(const char *glob);
char *	clean_regex(const char *regex);
int	match(const char *regex, const char *str);
int	match_glob(const char *glob, const char *str);
char *	glob_to_regex(const char *glob);
int	match_tar(const char *glob, const char *str);
char *	tar_to_regex(const char *glob);
int	match_host(const char *glob, const char *host);
int	match_disk(const char *glob, const char *disk);
int	match_datestamp(const char *dateexp, const char *datestamp);
int	match_level(const char *levelexp, const char *level);
time_t	unctime(char *timestr);
ssize_t	areads_dataready(int fd);
void	areads_relbuf(int fd);

/*
 * amfree(ptr) -- if allocated, release space and set ptr to NULL.
 *
 * In general, this should be called instead of just free(), unless
 * the very next source line sets the pointer to a new value.
 */

#define	amfree(ptr) do {						\
    if((ptr) != NULL) {							\
	int e__errno = errno;						\
	free(ptr);							\
	(ptr) = NULL;							\
	errno = e__errno;						\
	(void)(ptr);  /* Fix value never used warning at end of routines */ \
    }									\
} while (0)

#define strappend(s1,s2) do {						\
    char *t_t_t = (s1) ? stralloc2((s1),(s2)) : stralloc((s2));		\
    amfree((s1));							\
    (s1) = t_t_t;							\
} while(0)

/*
 * "Safe" close macros.  Close the object then set it to a value that
 * will cause an error if referenced.
 *
 * aclose(fd) -- close a file descriptor and set it to -1.
 * afclose(f) -- close a stdio file and set it to NULL.
 * apclose(p) -- close a stdio pipe file and set it to NULL.
 *
 * Note: be careful not to do the following:
 *
 *  for(fd = low; fd < high; fd++) {
 *      aclose(fd);
 *  }
 *
 * Since aclose() sets the argument to -1, this will loop forever.
 * Just copy fd to a temp variable and use that with aclose().
 *
 * Aclose() interacts with areads() to inform it to release any buffer
 * it has outstanding on the file descriptor.
 */

#define aclose(fd) do {							\
    if((fd) >= 0) {							\
	close(fd);							\
	areads_relbuf(fd);						\
    }									\
    (fd) = -1;								\
    (void)(fd);  /* Fix value never used warning at end of routines */ \
} while(0)

#define afclose(f) do {							\
    if((f) != NULL) {							\
	fclose(f);							\
	(f) = NULL;							\
	(void)(f);  /* Fix value never used warning at end of routines */ \
    }									\
} while(0)

#define apclose(p) do {							\
    if((p) != NULL) {							\
	pclose(p);							\
	(p) = NULL;							\
	(void)(p);  /* Fix value never used warning at end of routines */ \
    }									\
} while(0)

/*
 * Return the number of elements in an array.
 */
#define am_countof(a)	(int)(SIZEOF(a) / SIZEOF((a)[0]))

/*
 * min/max.  Don't do something like
 *
 *    x = min(y++, z);
 *
 * because the increment will be duplicated.
 */
#undef min
#undef max
#define	min(a, b)	((a) < (b) ? (a) : (b))
#define	max(a, b)	((a) > (b) ? (a) : (b))

/*
 * Utility bitmask manipulation macros.
 */
#define	SET(t, f)	((t) |= (f))
#define	CLR(t, f)	((t) &= ~((unsigned)(f)))
#define	ISSET(t, f)	((t) & (f))

/*
 * Utility string macros.  All assume a variable holds the current
 * character and the string pointer points to the next character to
 * be processed.  Typical setup is:
 *
 *  s = buffer;
 *  ch = *s++;
 *  skip_whitespace(s, ch);
 *  ...
 *
 * If you advance the pointer "by hand" to skip over something, do
 * it like this:
 *
 *  s += some_amount;
 *  ch = s[-1];
 *
 * Note that ch has the character at the end of the just skipped field.
 * It is often useful to terminate a string, make a copy, then restore
 * the input like this:
 *
 *  skip_whitespace(s, ch);
 *  fp = s-1;			## save the start
 *  skip_nonwhitespace(s, ch);	## find the end
 *  p[-1] = '\0';		## temporary terminate
 *  field = stralloc(fp);	## make a copy
 *  p[-1] = ch;			## restore the input
 *
 * The scanning macros are:
 *
 *  skip_whitespace (ptr, var)
 *    -- skip whitespace, but stops at a newline
 *  skip_non_whitespace (ptr, var)
 *    -- skip non whitespace
 *  skip_non_whitespace_cs (ptr, var)
 *    -- skip non whitespace, stop at comment
 *  skip_integer (ptr, var)
 *    -- skip an integer field
 *  skip_line (ptr, var)
 *    -- skip just past the next newline
 *
 * where:
 *
 *  ptr -- string pointer
 *  var -- current character
 *
 * These macros copy a non-whitespace field to a new buffer, and should
 * only be used if dynamic allocation is impossible (fixed size buffers
 * are asking for trouble):
 *
 *  copy_string (ptr, var, field, len, fldptr)
 *    -- copy a non-whitespace field
 *  copy_string_cs (ptr, var, field, len, fldptr)
 *    -- copy a non-whitespace field, stop at comment
 *
 * where:
 *
 *  ptr -- string pointer
 *  var -- current character
 *  field -- area to copy to
 *  len -- length of area (needs room for null byte)
 *  fldptr -- work pointer used in move
 *	      if NULL on exit, the field was too small for the input
 */

#define	STR_SIZE	4096		/* a generic string buffer size */
#define	NUM_STR_SIZE	128		/* a generic number buffer size */

#define	skip_whitespace(ptr,c) do {					\
    while((c) != '\n' && isspace(c)) (c) = *(ptr)++;			\
} while(0)

#define	skip_non_whitespace(ptr,c) do {					\
    while((c) != '\0' && !isspace(c)) (c) = *(ptr)++;			\
} while(0)

#define	skip_non_whitespace_cs(ptr,c) do {				\
    while((c) != '\0' && (c) != '#' && !isspace(c)) (c) = *(ptr)++;	\
} while(0)

#define	skip_non_integer(ptr,c) do {					\
    while((c) != '\0' && !isdigit(c)) (c) = *(ptr)++;			\
} while(0)

#define	skip_integer(ptr,c) do {					\
    if((c) == '+' || (c) == '-') (c) = *(ptr)++;			\
    while(isdigit(c)) (c) = *(ptr)++;					\
} while(0)

#define skip_quoted_string(ptr, c) do {					\
    int	iq = 0;								\
    while (((c) != '\0') && !((iq == 0) && isspace(c))) {		\
	if ((c) == '"') {						\
	    iq = !iq;							\
	} else if (((c) == '\\') && (*(ptr) == '"')) {			\
	    (ptr)++;							\
	}								\
	(c) = *(ptr)++;							\
    }									\
} while (0)

#define	skip_quoted_line(ptr, c) do {					\
    int	iq = 0;								\
    while((c) && !((iq == 0) && ((c) == '\n'))) {			\
	if ((c) == '"')							\
	    iq = !iq;							\
	(c) = *(ptr)++;							\
    }									\
    if(c)								\
	(c) = *(ptr)++;							\
} while(0)

#define	skip_line(ptr,c) do {						\
    while((c) && (c) != '\n')						\
	(c) = *(ptr)++;							\
    if(c)								\
	(c) = *(ptr)++;							\
} while(0)

#define	copy_string(ptr,c,f,l,fp) do {					\
    (fp) = (f);								\
    while((c) != '\0' && !isspace(c)) {					\
	if((fp) >= (f) + (l) - 1) {					\
	    *(fp) = '\0';						\
	    (fp) = NULL;						\
	    (void)(fp);  /* Fix value never used warning at end of routines */ \
	    break;							\
	}								\
	*(fp)++ = (c);							\
	(c) = *(ptr)++;							\
    }									\
    if(fp)								\
	*fp = '\0';							\
} while(0)

#define	copy_string_cs(ptr,c,f,l,fp) do {				\
    (fp) = (f);								\
    while((c) != '\0' && (c) != '#' && !isspace(c)) {			\
	if((fp) >= (f) + (l) - 1) {					\
	    *(fp) = '\0';						\
	    (fp) = NULL;						\
	    break;							\
	}								\
	*(fp)++ = (c);							\
	(c) = *(ptr)++;							\
    }									\
    if(fp) *fp = '\0';							\
} while(0)

#define is_dot_or_dotdot(s)						\
    ((s)[0] == '.'							\
     && ((s)[1] == '\0'							\
	 || ((s)[1] == '.' && (s)[2] == '\0')))

/* from amflock.c */
extern int    amflock(int fd, char *resource);
extern int    amroflock(int fd, char *resource);
extern int    amfunlock(int fd, char *resource);

/* from file.c */
extern int    mkpdir(char *file, mode_t mode, uid_t uid, gid_t gid);
extern int    rmpdir(char *file, char *topdir);
extern char  *sanitise_filename(char *inp);

/* from old bsd-security.c */
extern int debug;
extern int check_security(struct sockaddr_in *, char *, unsigned long, char **);

/*
 * Handle functions which are not always declared on all systems.  This
 * stops gcc -Wall and lint from complaining.
 */

/* AIX #defines accept, and provides a prototype for the alternate name */
#if !defined(HAVE_ACCEPT_DECL) && !defined(accept)
extern int accept(int s, struct sockaddr *addr, socklen_t *addrlen);
#endif

#ifndef HAVE_ATOF_DECL
extern double atof(const char *ptr);
#endif

#ifndef HAVE_BCOPY
# define bcopy(from,to,n) ((void)memmove((to), (from), (n)))
#else
# ifndef HAVE_BCOPY_DECL
extern void bcopy(const void *s1, void *s2, size_t n);
# endif
#endif

#ifndef HAVE_BIND_DECL
extern int bind(int s, const struct sockaddr *name, socklen_t namelen);
#endif

#ifndef HAVE_BZERO
#define bzero(s,n) ((void)memset((s),0,(n)))
#else
# ifndef HAVE_BZERO_DECL
extern void bzero(void *s, size_t n);
# endif
#endif

#ifndef HAVE_CLOSELOG_DECL
extern void closelog(void);
#endif

#ifndef HAVE_CONNECT_DECL
extern int connect(int s, struct sockaddr *name, socklen_t namelen);
#endif

#if !defined(TEXTDB) && !defined(HAVE_DBM_OPEN_DECL)
#undef   DBM_INSERT
#define  DBM_INSERT  0

#undef   DBM_REPLACE
#define  DBM_REPLACE 1

    typedef struct {
	int dummy[10];
    } DBM;

#ifndef HAVE_STRUCT_DATUM
    typedef struct {
	char    *dptr;
	int     dsize;
    } datum;
#endif

    extern DBM   *dbm_open(char *file, int flags, int mode);
    extern void   dbm_close(DBM *db);
    extern datum  dbm_fetch(DBM *db, datum key);
    extern datum  dbm_firstkey(DBM *db);
    extern datum  dbm_nextkey(DBM *db);
    extern int    dbm_delete(DBM *db, datum key);
    extern int    dbm_store(DBM *db, datum key, datum content, int flg);
#endif

#ifndef HAVE_FCLOSE_DECL
extern int fclose(FILE *stream);
#endif

#ifndef HAVE_FFLUSH_DECL
extern int fflush(FILE *stream);
#endif

#ifndef HAVE_FPRINTF_DECL
extern int fprintf(FILE *stream, const char *format, ...);
#endif

#ifndef HAVE_FPUTC_DECL
extern int fputc(int c, FILE *stream);
#endif

#ifndef HAVE_FPUTS_DECL
extern int fputs(const char *s, FILE *stream);
#endif

#ifndef HAVE_FREAD_DECL
extern size_t fread(void *ptr, size_t size, size_t nitems, FILE *stream);
#endif

#ifndef HAVE_FSEEK_DECL
extern int fseek(FILE *stream, long offset, int ptrname);
#endif

#ifndef HAVE_FWRITE_DECL
extern size_t fwrite(const void *ptr, size_t size, size_t nitems,
			FILE *stream);
#endif

#ifndef HAVE_GETHOSTNAME_DECL
extern int gethostname(char *name, int namelen);
#endif

#ifndef HAVE_GETOPT_DECL
extern char *optarg;
extern int getopt(int argc, char * const *argv, const char *optstring);
#endif

/* AIX #defines getpeername, and provides a prototype for the alternate name */
#if !defined(HAVE_GETPEERNAME_DECL) && !defined(getpeername)
extern int getpeername(int s, struct sockaddr *name, socklen_t *namelen);
#endif

/* AIX #defines getsockname, and provides a prototype for the alternate name */
#if !defined(HAVE_GETSOCKNAME_DECL) && !defined(getsockname)
extern int getsockname(int s, struct sockaddr *name, socklen_t *namelen);
#endif

#ifndef HAVE_GETSOCKOPT_DECL
extern int getsockopt(int s, int level, int optname, char *optval,
			 socklen_t *optlen);
#endif

#ifndef HAVE_GETTIMEOFDAY_DECL
# ifdef HAVE_TWO_ARG_GETTIMEOFDAY
extern int gettimeofday(struct timeval *tp, struct timezone *tzp);
# else
extern int gettimeofday(struct timeval *tp);
# endif
#endif

#ifndef HAVE_INITGROUPS
# define initgroups(name,basegid) 0
#else
# ifndef HAVE_INITGROUPS_DECL
extern int initgroups(const char *name, gid_t basegid);
# endif
#endif

#ifndef HAVE_IOCTL_DECL
extern int ioctl(int fildes, int request, ...);
#endif

#ifndef isnormal
#ifndef HAVE_ISNORMAL
#define	isnormal(f) (((f) < 0.0) || ((f) > 0.0))
#endif
#endif

#ifndef HAVE_LISTEN_DECL
extern int listen(int s, int backlog);
#endif

#ifndef HAVE_LSTAT_DECL
extern int lstat(const char *path, struct stat *buf);
#endif

#ifndef HAVE_MALLOC_DECL
extern void *malloc (size_t size);
#endif

#ifndef HAVE_MEMMOVE_DECL
#ifdef HAVE_MEMMOVE
extern void *memmove(void *to, const void *from, size_t n);
#else
extern char *memmove(char *to, /*const*/ char *from, size_t n);
#endif
#endif

#ifndef HAVE_MEMSET_DECL
extern void *memset(void *s, int c, size_t n);
#endif

#ifndef HAVE_MKTEMP_DECL
extern char *mktemp(char *template);
#endif

#ifndef HAVE_MKSTEMP_DECL
extern int mkstemp(char *template);
#endif

#ifndef HAVE_MKTIME_DECL
extern time_t mktime(struct tm *timeptr);
#endif

#ifndef HAVE_OPENLOG_DECL
#ifdef LOG_AUTH
extern void openlog(const char *ident, int logopt, int facility);
#else
extern void openlog(const char *ident, int logopt);
#endif
#endif

#ifndef HAVE_PCLOSE_DECL
extern int pclose(FILE *stream);
#endif

#ifndef HAVE_PERROR_DECL
extern void perror(const char *s);
#endif

#ifndef HAVE_PRINTF_DECL
extern int printf(const char *format, ...);
#endif

#ifndef HAVE_PUTS_DECL
extern int puts(const char *s);
#endif

#ifndef HAVE_REALLOC_DECL
extern void *realloc(void *ptr, size_t size);
#endif

/* AIX #defines recvfrom, and provides a prototype for the alternate name */
#if !defined(HAVE_RECVFROM_DECL) && !defined(recvfrom)
extern int recvfrom(int s, char *buf, int len, int flags,
		       struct sockaddr *from, socklen_t *fromlen);
#endif

#ifndef HAVE_REMOVE_DECL
extern int remove(const char *path);
#endif

#ifndef HAVE_RENAME_DECL
extern int rename(const char *old, const char *new);
#endif

#ifndef HAVE_REWIND_DECL
extern void rewind(FILE *stream);
#endif

#ifndef HAVE_RUSEROK_DECL
extern int ruserok(const char *rhost, int suser,
		      const char *ruser, const char *luser);
#endif

#ifndef HAVE_SELECT_DECL
extern int select(int nfds,
		     SELECT_ARG_TYPE *readfds,
		     SELECT_ARG_TYPE *writefds,
		     SELECT_ARG_TYPE *exceptfds,
		     struct timeval *timeout);
#endif

#ifndef HAVE_SENDTO_DECL
extern int sendto(int s, const char *msg, int len, int flags,
		     const struct sockaddr *to, int tolen);
#endif

#ifdef HAVE_SETRESGID
#define	setegid(x)	setresgid((gid_t)-1,(x),(gid_t)-1)
#ifndef HAVE_SETRESGID_DECL
extern int setresgid(gid_t rgid, gid_t egid, gid_t sgid);
#endif
#else
#ifndef HAVE_SETEGID_DECL
extern int setegid(gid_t egid);
#endif
#endif

#ifdef HAVE_SETRESUID
#define	seteuid(x)	setresuid((uid_t)-1,(x),(uid_t)-1)
#ifndef HAVE_SETRESUID_DECL
extern int setresuid(uid_t ruid, uid_t euid, uid_t suid);
#endif
#else
#ifndef HAVE_SETEUID_DECL
extern int seteuid(uid_t euid);
#endif
#endif

#ifndef HAVE_SETPGID_DECL
#ifdef HAVE_SETPGID
extern int setpgid(pid_t pid, pid_t pgid);
#endif
#endif

#ifndef HAVE_SETPGRP_DECL
#ifdef SETPGRP_VOID
extern pid_t setpgrp(void);
#else
extern pid_t setpgrp(pid_t pgrp, pid_t pid);
#endif
#endif

#ifndef HAVE_SETSOCKOPT_DECL
extern int setsockopt(int s, int level, int optname,
			 const char *optval, int optlen);
#endif

#ifdef HAVE_SHMGET
#ifndef HAVE_SHMAT_DECL
extern void *shmat(int shmid, const SHM_ARG_TYPE *shmaddr, int shmflg);
#endif

#ifndef HAVE_SHMCTL_DECL
extern int shmctl(int shmid, int cmd, struct shmid_ds *buf);
#endif

#ifndef HAVE_SHMDT_DECL
extern int shmdt(SHM_ARG_TYPE *shaddr);
#endif

#ifndef HAVE_SHMGET_DECL
extern int shmget(key_t key, size_t size, int shmflg);
#endif
#endif

#ifndef HAVE_SNPRINTF_DECL
#include "arglist.h"
int snprintf(char *buf, size_t len, const char *format,...)
		    __attribute__((format(printf,3,4)));
#endif
#ifndef HAVE_VSNPRINTF_DECL
#include "arglist.h"
int vsnprintf(char *buf, size_t len, const char *format, va_list ap);
#endif

#ifndef HAVE_SOCKET_DECL
extern int socket(int domain, int type, int protocol);
#endif

#ifndef HAVE_SOCKETPAIR_DECL
extern int socketpair(int domain, int type, int protocol, int sv[2]);
#endif

#ifndef HAVE_SSCANF_DECL
extern int sscanf(const char *s, const char *format, ...);
#endif

#ifndef HAVE_STRCASECMP_DECL
extern int strcasecmp(const char *s1, const char *s2);
#endif

#ifndef HAVE_STRERROR_DECL
extern char *strerror(int errnum);
#endif

#ifndef HAVE_STRFTIME_DECL
extern size_t strftime(char *s, size_t maxsize, const char *format,
			  const struct tm *timeptr);
#endif

#ifndef HAVE_STRNCASECMP_DECL
extern int strncasecmp(const char *s1, const char *s2, int n);
#endif

#ifndef HAVE_SYSLOG_DECL
extern void syslog(int priority, const char *logstring, ...)
    __attribute__ ((format (printf, 2, 3)));
#endif

#ifndef HAVE_SYSTEM_DECL
extern int system(const char *string);
#endif

#ifndef HAVE_TIME_DECL
extern time_t time(time_t *tloc);
#endif

#ifndef HAVE_TOLOWER_DECL
extern int tolower(int c);
#endif

#ifndef HAVE_TOUPPER_DECL
extern int toupper(int c);
#endif

#ifndef HAVE_UNGETC_DECL
extern int ungetc(int c, FILE *stream);
#endif

#ifndef HAVE_VFPRINTF_DECL
#include "arglist.h"
extern int vfprintf(FILE *stream, const char *format, va_list ap);
#endif

#ifndef HAVE_VPRINTF_DECL
#include "arglist.h"
extern int vprintf(const char *format, va_list ap);
#endif

#if !defined(S_ISCHR) && defined(_S_IFCHR) && defined(_S_IFMT)
#define S_ISCHR(mode) (((mode) & _S_IFMT) == _S_IFCHR)
#endif

#if !defined(S_ISREG) && defined(_S_IFREG) && defined(_S_IFMT)
#define S_ISREG(mode) (((mode) & _S_IFMT) == _S_IFREG)
#endif

#ifndef HAVE_WAITPID
#ifdef HAVE_WAIT4
#define waitpid(pid,status,options) wait4(pid,status,options,0)
#else
extern pid_t waitpid(pid_t pid, amwait_t *stat_loc, int options);
#endif
#endif

#ifndef HAVE_WRITEV_DECL
extern ssize_t writev(int fd, const struct iovec *iov, int iovcnt);
#endif

#ifndef STDIN_FILENO
#define STDIN_FILENO 0
#endif

#ifndef STDOUT_FILENO
#define STDOUT_FILENO 1
#endif

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

/* S_ISDIR is not defined on Nextstep */
#ifndef S_ISDIR
#if defined(_S_IFMT) && defined(_S_IFDIR)
#define S_ISDIR(mode)   (((mode) & (_S_IFMT)) == (_S_IFDIR))
#else
#error Don t know how to define S_ISDIR
#endif
#endif

#if SIZEOF_SIZE_T == SIZEOF_INT
#  define        SIZE_T_FMT	"%u"
#  define        SIZE_T_FMT_TYPE unsigned
#  define        SIZE_T_ATOI	(size_t)atoi
#  ifndef SIZE_MAX
#    define      SIZE_MAX	UINT_MAX
#  endif
#else
#  define        SIZE_T_FMT	"%lu"
#  define        SIZE_T_FMT_TYPE unsigned long
#  define        SIZE_T_ATOI	(size_t)atol
#  ifndef SIZE_MAX
#    define      SIZE_MAX	ULONG_MAX
#  endif
#endif

#if SIZEOF_SSIZE_T == SIZEOF_INT
#  define        SSIZE_T_FMT	"%d"
#  define        SSIZE_T_FMT_TYPE int
#  define        SSIZE_T_ATOI	(ssize_t)atoi
#  ifndef SSIZE_MAX
#    define      SSIZE_MAX	INT_MAX
#  endif
#  ifndef SSIZE_MIN
#    define      SSIZE_MIN	INT_MIN
#  endif
#else
#  define        SSIZE_T_FMT	"%ld"
#  define        SSIZE_T_FMT_TYPE long
#  define        SSIZE_T_ATOI	(ssize_t)atol
#  ifndef SSIZE_MAX
#    define      SSIZE_MAX	LONG_MAX
#  endif
#  ifndef SSIZE_MIN
#    define      SSIZE_MIN	LONG_MIN
#  endif
#endif

#if SIZEOF_TIME_T == SIZEOF_INT
#  define        TIME_T_FMT	"%u"
#  define        TIME_T_FMT_TYPE unsigned
#  define        TIME_T_ATOI	(time_t)atoi
#  ifndef TIME_MAX
#    define      TIME_MAX	UINT_MAX
#  endif
#else
#  define        TIME_T_FMT	"%lu"
#  define        TIME_T_FMT_TYPE unsigned long
#  define        TIME_T_ATOI	(time_t)atol
#  ifndef TIME_MAX
#    define      TIME_MAX	ULONG_MAX
#  endif
#endif

#if SIZEOF_OFF_T > SIZEOF_LONG
#  define        OFF_T_FMT       "%lld"
#  define        OFF_T_RFMT       "lld"
#  define        OFF_T_FMT_TYPE  long long
#  define        OFF_T_ATOI	 (off_t)atoll
#  define        OFF_T_STRTOL	 (off_t)strtoll
#else
#  if SIZEOF_OFF_T == SIZEOF_LONG
#    define        OFF_T_FMT       "%ld"
#    define        OFF_T_RFMT      "ld"
#    define        OFF_T_FMT_TYPE  long
#    define        OFF_T_ATOI	 (off_t)atol
#    define        OFF_T_STRTOL	 (off_t)strtol
#  else
#    define        OFF_T_FMT       "%d"
#    define        OFF_T_RFMT      "d"
#    define        OFF_T_FMT_TYPE  int
#    define        OFF_T_ATOI	 (off_t)atoi
#    define        OFF_T_STRTOL	 (off_t)strtol
#  endif
#endif

#if SIZEOF_OFF_T == 8
#  ifdef OFF_MAX
#    define AM64_MAX (off_t)(OFF_MAX)
#  else
#    define AM64_MAX (off_t)(9223372036854775807LL)
#  endif
#  ifdef OFF_MIN
#    define AM64_MIN (off_t)(OFF_MIN)
#  else
#    define AM64_MIN (off_t)(-9223372036854775807LL -1LL)
#  endif
#  define AM64_FMT OFF_T_FMT
#else
#if SIZEOF_LONG == 8
#  ifdef LONG_MAX
#    define AM64_MAX (off_t)(LONG_MAX)
#  else
#    define AM64_MAX (off_t)9223372036854775807L
#  endif
#  ifdef LONG_MIN
#    define AM64_MIN (off_t)(LONG_MIN)
#  else
#    define AM64_MIN (off_t)(-9223372036854775807L -1L)
#  endif
#  define AM64_FMT "%ld"
#else
#if SIZEOF_LONG_LONG == 8
#  ifdef LONG_LONG_MAX
#    define AM64_MAX (off_t)(LONG_LONG_MAX)
#  else
#    define AM64_MAX (off_t)9223372036854775807LL
#  endif
#  ifdef LONG_LONG_MIN
#    define AM64_MIN (off_t)(LONG_LONG_MIN)
#  else
#    define AM64_MIN (off_t)(-9223372036854775807LL -1LL)
#  endif
#  define AM64_FMT LL_FMT
#else
#if SIZEOF_INTMAX_T == 8
#  ifdef INTMAX_MAX
#    define AM64_MAX (off_t)(INTMAX_MAX)
#  else
#    define AM64_MAX (off_t)9223372036854775807LL
#  endif
#  ifdef INTMAX_MIN
#    define AM64_MIN (off_t)(INTMAX_MIN)
#  else
#    define AM64_MIN (off_t)(-9223372036854775807LL -1LL)
#  endif
#  define AM64_FMT LL_FMT
#else  /* no 64 bits type found, use long. */
#  ifdef LONG_MAX
#    define AM64_MAX (off_t)(LONG_MAX)
#  else
#    define AM64_MAX (off_t)2147483647
#  endif
#  ifdef LONG_MIN
#    define AM64_MIN (off_t)(LONG_MIN)
#  else
#    define AM64_MIN (off_t)(-2147483647 -1)
#  endif
#  define AM64_FMT "%ld"
#endif
#endif
#endif
#endif

#ifdef HAVE_LIBREADLINE
#  ifdef HAVE_READLINE_READLINE_H
#    include <readline/readline.h>
#    ifdef HAVE_READLINE_HISTORY_H
#      include <readline/history.h>
#    endif
#  else
#    ifdef HAVE_READLINE_H
#      include <readline.h>
#      ifdef HAVE_HISTORY_H
#        include <history.h>
#      endif
#    else
#      undef HAVE_LIBREADLINE
#    endif
#  endif
#else

char *	readline(const char *prompt);
void	add_history(const char *line);

#endif

#define BIND_CYCLE_RETRIES	120		/* Total of 30 minutes */

#define DBG_SUBDIR_SERVER  "server"
#define DBG_SUBDIR_CLIENT  "client"
#define DBG_SUBDIR_AMANDAD "amandad"

#endif	/* !AMANDA_H */
