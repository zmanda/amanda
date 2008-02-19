/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
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
 * $Id: util.c,v 1.42 2006/08/24 01:57:15 paddy_s Exp $
 */

#include "amanda.h"
#include "util.h"
#include <regex.h>
#include "arglist.h"
#include "clock.h"
#include "sockaddr-util.h"
#include "conffile.h"

#ifdef HAVE_LIBCURL
#include <curl/curl.h>
#endif

static int make_socket(sa_family_t family);
static int connect_port(struct sockaddr_storage *addrp, in_port_t port, char *proto,
			struct sockaddr_storage *svaddr, int nonblock);

/*
 * Keep calling read() until we've read buflen's worth of data, or EOF,
 * or we get an error.
 *
 * Returns the number of bytes read, 0 on EOF, or negative on error.
 */
ssize_t
fullread(
    int		fd,
    void *	vbuf,
    size_t	buflen)
{
    ssize_t nread, tot = 0;
    char *buf = vbuf;	/* cast to char so we can ++ it */

    while (buflen > 0) {
	nread = read(fd, buf, buflen);
	if (nread < 0) {
	    if ((errno == EINTR) || (errno == EAGAIN))
		continue;
	    return ((tot > 0) ? tot : -1);
	}

	if (nread == 0)
	    break;

	tot += nread;
	buf += nread;
	buflen -= nread;
    }
    return (tot);
}

/*
 * Keep calling write() until we've written buflen's worth of data,
 * or we get an error.
 *
 * Returns the number of bytes written, or negative on error.
 */
ssize_t
fullwrite(
    int		fd,
    const void *vbuf,
    size_t	buflen)
{
    ssize_t nwritten, tot = 0;
    const char *buf = vbuf;	/* cast to char so we can ++ it */

    while (buflen > 0) {
	nwritten = write(fd, buf, buflen);
	if (nwritten < 0) {
	    if ((errno == EINTR) || (errno == EAGAIN))
		continue;
	    return ((tot > 0) ? tot : -1);
	}
	tot += nwritten;
	buf += nwritten;
	buflen -= nwritten;
    }
    return (tot);
}

static int
make_socket(
    sa_family_t family)
{
    int s;
    int save_errno;
#if defined(SO_KEEPALIVE) || defined(USE_REUSEADDR)
    int on=1;
    int r;
#endif

    s = socket(family, SOCK_STREAM, 0);
    if (s == -1) {
        save_errno = errno;
        dbprintf(_("make_socket: socket() failed: %s\n"), strerror(save_errno));
        errno = save_errno;
        return -1;
    }
    if (s < 0 || s >= (int)FD_SETSIZE) {
        aclose(s);
        errno = EMFILE;                         /* out of range */
        return -1;
    }

#ifdef USE_REUSEADDR
    r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    if (r < 0) {
	save_errno = errno;
	dbprintf(_("make_socket: setsockopt(SO_REUSEADDR) failed: %s\n"),
		  strerror(errno));
	errno = save_errno;
    }
#endif

#ifdef SO_KEEPALIVE
    r = setsockopt(s, SOL_SOCKET, SO_KEEPALIVE,
		   (void *)&on, SIZEOF(on));
    if (r == -1) {
	save_errno = errno;
	dbprintf(_("make_socket: setsockopt() failed: %s\n"),
                  strerror(save_errno));
	aclose(s);
	errno = save_errno;
	return -1;
    }
#endif

    return s;
}

/* addrp is my address */
/* svaddr is the address of the remote machine */
/* return socket on success */
/* return -1     on failure */
int
connect_portrange(
    struct sockaddr_storage *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto,
    struct sockaddr_storage *svaddr,
    int			nonblock)
{
    int			s;
    in_port_t		port;
    static in_port_t	port_in_use[1024];
    static int		nb_port_in_use = 0;
    int			i;

    assert(first_port <= last_port);
    /* Try a port already used */
    for(i=0; i < nb_port_in_use; i++) {
	port = port_in_use[i];
	if(port >= first_port && port <= last_port) {
	    s = connect_port(addrp, port, proto, svaddr, nonblock);
	    if(s == -2) return -1;
	    if(s > 0) {
		return s;
	    }
	}
    }

    /* Try a port in the range */
    for (port = first_port; port <= last_port; port++) {
	s = connect_port(addrp, port, proto, svaddr, nonblock);
	if(s == -2) return -1;
	if(s > 0) {
	    port_in_use[nb_port_in_use++] = port;
	    return s;
	}
    }

    dbprintf(_("connect_portrange: All ports between %d and %d are busy.\n"),
	      first_port,
	      last_port);
    errno = EAGAIN;
    return -1;
}

/* addrp is my address */
/* svaddr is the address of the remote machine */
/* return -2: Don't try again */
/* return -1: Try with another port */
/* return >0: this is the connected socket */
int
connect_port(
    struct sockaddr_storage *addrp,
    in_port_t  		port,
    char *		proto,
    struct sockaddr_storage *svaddr,
    int			nonblock)
{
    int			save_errno;
    struct servent *	servPort;
    socklen_t		len;
    socklen_t		socklen;
    int			s;

    servPort = getservbyport((int)htons(port), proto);
    if (servPort != NULL && !strstr(servPort->s_name, "amanda")) {
	dbprintf(_("connect_port: Skip port %d: owned by %s.\n"),
		  port, servPort->s_name);
	return -1;
    }

    if ((s = make_socket(addrp->ss_family)) == -1) return -2;

    SS_SET_PORT(addrp, port);
    socklen = SS_LEN(addrp);
    if (bind(s, (struct sockaddr *)addrp, socklen) != 0) {
	save_errno = errno;
	aclose(s);
	if(servPort == NULL) {
	    dbprintf(_("connect_port: Try  port %d: available - %s\n"),
		     port, strerror(errno));
	} else {
	    dbprintf(_("connect_port: Try  port %d: owned by %s - %s\n"),
		     port, servPort->s_name, strerror(errno));
	}
	if (save_errno != EADDRINUSE) {
	    errno = save_errno;
	    return -2;
	}

	errno = save_errno;
	return -1;
    }
    if(servPort == NULL) {
	dbprintf(_("connect_port: Try  port %d: available - Success\n"), port);
    } else {
	dbprintf(_("connect_port: Try  port %d: owned by %s - Success\n"),
		  port, servPort->s_name);
    }

    /* find out what port was actually used */

    len = sizeof(*addrp);
    if (getsockname(s, (struct sockaddr *)addrp, &len) == -1) {
	save_errno = errno;
	dbprintf(_("connect_port: getsockname() failed: %s\n"),
		  strerror(save_errno));
	aclose(s);
	errno = save_errno;
	return -1;
    }

    if (nonblock)
	fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0)|O_NONBLOCK);
    if (connect(s, (struct sockaddr *)svaddr, SS_LEN(svaddr)) == -1 && !nonblock) {
	save_errno = errno;
	dbprintf(_("connect_portrange: Connect from %s failed: %s\n"),
		  str_sockaddr(addrp),
		  strerror(save_errno));
	dbprintf(_("connect_portrange: connect to %s failed: %s\n"),
		  str_sockaddr(svaddr),
		  strerror(save_errno));
	aclose(s);
	errno = save_errno;
	if (save_errno == ECONNREFUSED ||
	    save_errno == EHOSTUNREACH ||
	    save_errno == ENETUNREACH ||
	    save_errno == ETIMEDOUT)  {
	    return -2	;
	}
	return -1;
    }

    dbprintf(_("connected to %s\n"),
              str_sockaddr(svaddr));
    dbprintf(_("our side is %s\n"),
              str_sockaddr(addrp));
    return s;
}


/*
 * Bind to a port in the given range.  Takes a begin,end pair of port numbers.
 *
 * Returns negative on error (EGAIN if all ports are in use).  Returns 0
 * on success.
 */
int
bind_portrange(
    int			s,
    struct sockaddr_storage *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto)
{
    in_port_t port;
    in_port_t cnt;
    socklen_t socklen;
    struct servent *servPort;
    const in_port_t num_ports = (in_port_t)(last_port - first_port + 1);

    assert(first_port <= last_port);

    /*
     * We pick a different starting port based on our pid and the current
     * time to avoid always picking the same reserved port twice.
     */
    port = (in_port_t)(((getpid() + time(0)) % num_ports) + first_port);

    /*
     * Scan through the range, trying all available ports that are either 
     * not taken in /etc/services or registered for *amanda*.  Wrap around
     * if we don't happen to start at the beginning.
     */
    for (cnt = 0; cnt < num_ports; cnt++) {
	servPort = getservbyport((int)htons(port), proto);
	if ((servPort == NULL) || strstr(servPort->s_name, "amanda")) {
	    SS_SET_PORT(addrp, port);
	    socklen = SS_LEN(addrp);
	    if (bind(s, (struct sockaddr *)addrp, socklen) >= 0) {
		if (servPort == NULL) {
		    dbprintf(_("bind_portrange2: Try  port %d: Available - Success\n"), port);
		} else {
		    dbprintf(_("bind_portrange2: Try  port %d: Owned by %s - Success.\n"), port, servPort->s_name);
		}
		return 0;
	    }
	    if (servPort == NULL) {
		dbprintf(_("bind_portrange2: Try  port %d: Available - %s\n"),
			port, strerror(errno));
	    } else {
		dbprintf(_("bind_portrange2: Try  port %d: Owned by %s - %s\n"),
			port, servPort->s_name, strerror(errno));
	    }
	} else {
	        dbprintf(_("bind_portrange2: Skip port %d: Owned by %s.\n"),
		      port, servPort->s_name);
	}
	if (++port > last_port)
	    port = first_port;
    }
    dbprintf(_("bind_portrange: all ports between %d and %d busy\n"),
		  first_port,
		  last_port);
    errno = EAGAIN;
    return -1;
}


int
needs_quotes(
    const char * str)
{
    return (match("[ \t\f\r\n\"]", str) != 0);
}


/*
 * For backward compatibility we are trying for minimal quoting.
 * We only quote a string if it contains whitespace or is misquoted...
 */

char *
quote_string(
    const char *str)
{
    char *  s;
    char *  ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = stralloc("\"\"");
    } else if ((match("[:\'\\\"[:space:][:cntrl:]]", str)) == 0) {
	/*
	 * String does not need to be quoted since it contains
	 * neither whitespace, control or quote characters.
	 */
	ret = stralloc(str);
    } else {
	/*
	 * Allocate maximum possible string length.
	 * (a string of all quotes plus room for leading ", trailing " and NULL)
	 */
	ret = s = alloc((strlen(str) * 2) + 2 + 1);
	*(s++) = '"';
	while (*str != '\0') {
            if (*str == '\t') {
                *(s++) = '\\';
                *(s++) = 't';
		str++;
		continue;
	    } else if (*str == '\n') {
                *(s++) = '\\';
                *(s++) = 'n';
		str++;
		continue;
	    } else if (*str == '\r') {
                *(s++) = '\\';
                *(s++) = 'r';
		str++;
		continue;
	    } else if (*str == '\f') {
                *(s++) = '\\';
                *(s++) = 'f';
		str++;
		continue;
	    } else if (*str == '\\') {
                *(s++) = '\\';
                *(s++) = '\\';
		str++;
		continue;
	    }
            if (*str == '"')
                *(s++) = '\\';
            *(s++) = *(str++);
        }
        *(s++) = '"';
        *s = '\0';
    }
    return (ret);
}


char *
unquote_string(
    const char *str)
{
    char * ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = stralloc("");
    } else {
	char * in;
	char * out;

	ret = in = out = stralloc(str);
	while (*in != '\0') {
	    if (*in == '"') {
	        in++;
		continue;
	    }

	    if (*in == '\\') {
		in++;
		if (*in == 'n') {
		    in++;
		    *(out++) = '\n';
		    continue;
		} else if (*in == 't') {
		    in++;
		    *(out++) = '\t';
		    continue;
		} else if (*in == 'r') {
		    in++;
		    *(out++) = '\r';
		    continue;
		} else if (*in == 'f') {
		    in++;
		    *(out++) = '\f';
		    continue;
		}
	    }
	    *(out++) = *(in++);
	}
        *out = '\0';
    }
    return (ret);
}

char *
sanitize_string(
    const char *str)
{
    char * s;
    char * ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = stralloc("");
    } else {
	ret = stralloc(str);
	for (s = ret; *s != '\0'; s++) {
	    if (iscntrl((int)*s))
		*s = '?';
	}
    }
    return (ret);
}

/*
   Return 0 if the following characters are present
   * ( ) < > [ ] , ; : ! $ \ / "
   else returns 1
*/

int
validate_mailto(
    const char *mailto)
{
    return !match("\\*|<|>|\\(|\\)|\\[|\\]|,|;|:|\\\\|/|\"|\\!|\\$|\\|", mailto);
}

int copy_file(
    char  *dst,
    char  *src,
    char **errmsg)
{
    int     infd, outfd;
    int     save_errno;
    ssize_t nb;
    char    buf[32768];
    char   *quoted;

    if ((infd = open(src, O_RDONLY)) == -1) {
	save_errno = errno;
	quoted = quote_string(src);
	*errmsg = vstrallocf(_("Can't open file '%s' for reading: %s"),
			    quoted, strerror(save_errno));
	amfree(quoted);
	return -1;
    }

    if ((outfd = open(dst, O_WRONLY|O_CREAT, 0600)) == -1) {
	save_errno = errno;
	quoted = quote_string(dst);
	*errmsg = vstrallocf(_("Can't open file '%s' for writting: %s"),
			    quoted, strerror(save_errno));
	amfree(quoted);
	close(infd);
	return -1;
    }

    while((nb=read(infd, &buf, SIZEOF(buf))) > 0) {
	if(fullwrite(outfd,&buf,(size_t)nb) < nb) {
	    save_errno = errno;
	    quoted = quote_string(dst);
	    *errmsg = vstrallocf(_("Error writing to '%s': %s"),
				quoted, strerror(save_errno));
	    amfree(quoted);
	    close(infd);
	    close(outfd);
	    return -1;
	}
    }

    if (nb < 0) {
	save_errno = errno;
	quoted = quote_string(src);
	*errmsg = vstrallocf(_("Error reading from '%s': %s"),
			    quoted, strerror(save_errno));
	amfree(quoted);
	close(infd);
	close(outfd);
	return -1;
    }

    close(infd);
    close(outfd);
    return 0;
}

#ifndef HAVE_READLINE
/*
 * simple readline() replacements, used when we don't have readline
 * support from the system.
 */

char *
readline(
    const char *prompt)
{
    g_printf("%s", prompt);
    fflush(stdout);
    fflush(stderr);
    return agets(stdin);
}

void 
add_history(
    const char *line)
{
    (void)line; 	/* Quiet unused parameter warning */
}

#endif

/* Order of preference: readdir64(), readdir(). */
#if HAVE_DECL_READDIR64
#  define USE_DIRENT64
#  define USE_READDIR64
#elif HAVE_DECL_READDIR
#  define USE_READDIR
#else
# error No readdir() or readdir64() available!
#endif

char * portable_readdir(DIR* handle) {

#ifdef USE_DIRENT64
    struct dirent64 *entry_p;
#else
    struct dirent *entry_p;
#endif

    static GStaticMutex mutex = G_STATIC_MUTEX_INIT;

    g_static_mutex_lock(&mutex);

#ifdef USE_READDIR
    entry_p = readdir(handle);
#endif
#ifdef USE_READDIR64
    entry_p = readdir64(handle);
#endif

    g_static_mutex_unlock(&mutex);
    
    if (entry_p == NULL)
        return NULL;

    /* FIXME: According to glibc documentation, d_name may not be
       null-terminated in some cases on some very old platforms. Not
       sure what to do about that case. */
    return strdup(entry_p->d_name);
}

int search_directory(DIR * handle, const char * regex,
                     SearchDirectoryFunctor functor, gpointer user_data) {
    int rval = 0;
    regex_t compiled_regex;
    gboolean done = FALSE;

    if (regcomp(&compiled_regex, regex, REG_EXTENDED | REG_NOSUB) != 0) {
        regfree(&compiled_regex);
        return -1;
    }

    rewinddir(handle);

    while (!done) {
        char * read_name;
        int result;
        read_name = portable_readdir(handle);
        if (read_name == NULL) {
            regfree(&compiled_regex);
            return rval;
	}
        result = regexec(&compiled_regex, read_name, 0, NULL, 0);
        if (result == 0) {
            rval ++;
            done = !functor(read_name, user_data);
        }
        amfree(read_name);
    }
    regfree(&compiled_regex);
    return rval;
}

char* find_regex_substring(const char* base_string, const regmatch_t match) {
    char * rval;
    int size;

    size = match.rm_eo - match.rm_so;
    rval = malloc(size+1);
    memcpy(rval, base_string + match.rm_so, size);
    rval[size] = '\0';

    return rval;
}

int compare_possibly_null_strings(const char * a, const char * b) {
    if (a == b) {
        /* NULL or otherwise, they're the same. */
        return 0;
    } else if (a == NULL) {
        /* b != NULL */
        return -1;
    } else if (b == NULL) {
        /* a != NULL */
        return 1;
    } else {
        /* a != NULL != b */
        return strcmp(a, b);
    }
}

gboolean amanda_thread_init(void) {
    gboolean success = FALSE;
#ifdef HAVE_LIBCURL
    static gboolean did_curl_init = FALSE;
    if (!did_curl_init) {
# ifdef G_THREADS_ENABLED
        g_assert(!g_thread_supported());
# endif
        g_assert(curl_global_init(CURL_GLOBAL_ALL) == 0);
        did_curl_init = TRUE;
    }
#endif
#if defined(G_THREADS_ENABLED) && !defined(G_THREADS_IMPL_NONE)
    if (g_thread_supported()) {
        return TRUE;
    }
    g_thread_init(NULL);
    success = TRUE;
#endif
    return success;
}

int
resolve_hostname(const char *hostname,
	int socktype,
	struct addrinfo **res,
	char **canonname)
{
    struct addrinfo hints;
    struct addrinfo *myres;
    int flags = 0;
    int result;

    if (res) *res = NULL;
    if (canonname) {
	*canonname = NULL;
	flags = AI_CANONNAME;
    }

#ifdef AI_ADDRCONFIG
    flags |= AI_ADDRCONFIG;
#endif

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = flags;
    hints.ai_socktype = socktype;
    result = getaddrinfo(hostname, NULL, &hints, &myres);
    if (result != 0) {
	return result;
    }

    if (canonname && myres && myres->ai_canonname) {
	*canonname = stralloc(myres->ai_canonname);
    }

    if (res) {
	*res = myres;
    } else {
	freeaddrinfo(myres);
    }

    return result;
}

char *
_str_exit_status(
    char *subject,
    amwait_t status)
{
    if (WIFEXITED(status)) {
	int exitstatus = WEXITSTATUS(status);
	if (exitstatus == 0)
	    return vstrallocf(_("%s exited normally"), subject);
	else
	    return vstrallocf(_("%s exited with status %d"), subject, exitstatus);
    }

    if (WIFSIGNALED(status)) {
	int signal = WTERMSIG(status);
#ifdef WCOREDUMP
	if (WCOREDUMP(status))
	    return vstrallocf(_("%s exited after receiving signal %d (core dumped)"),
		subject, signal);
	else
#endif
	    return vstrallocf(_("%s exited after receiving signal %d"),
		subject, signal);
    }

    if (WIFSTOPPED(status)) {
	int signal = WSTOPSIG(status);
	return vstrallocf(_("%s stopped temporarily after receiving signal %d"),
	    subject, signal);
    }

#ifdef WIFCONTINUED
    if (WIFCONTINUED(status)) {
	return vstrallocf(_("%s was resumed"), subject);
    }
#endif

    return vstrallocf(_("%s exited in unknown circumstances"), subject);
}

void
check_running_as(running_as_flags who)
{
#ifdef CHECK_USERID
    struct passwd *pw;
    uid_t uid_me;
    uid_t uid_target;
    char *uname_me = NULL;
    char *uname_target = NULL;
    char *dumpuser;

    uid_me = getuid();
    if ((pw = getpwuid(uid_me)) == NULL) {
        error(_("current userid %ld not found in password database"), (long)uid_me);
	/* NOTREACHED */
    }
    uname_me = stralloc(pw->pw_name);

#ifndef SINGLE_USERID
    if (!(who & RUNNING_AS_UID_ONLY) && uid_me != geteuid()) {
	error(_("euid (%lld) does not match uid (%lld); is this program setuid-root when it shouldn't be?"),
		(long long int)geteuid(), (long long int)uid_me);
	/* NOTREACHED */
    }
#endif

    switch (who & RUNNING_AS_USER_MASK) {
	case RUNNING_AS_ANY:
	    uid_target = uid_me;
	    uname_target = uname_me;
	    return;

	case RUNNING_AS_ROOT:
	    uid_target = 0;
	    uname_target = "root";
	    break;

	case RUNNING_AS_DUMPUSER_PREFERRED:
	    dumpuser = getconf_str(CNF_DUMPUSER);
	    if ((pw = getpwnam(CLIENT_LOGIN)) != NULL &&
                    uid_me == pw->pw_uid) {
                /* uid == CLIENT_LOGIN: not ideal, but OK */
                dbprintf(_("NOTE: running as '%s', which the client user, not the "
                    "dumpuser ('%s'); forging on anyway\n"),
                    CLIENT_LOGIN, dumpuser);
                uid_target = pw->pw_uid; /* force success below */
                break;
            }
            /* FALLTHROUGH */

	case RUNNING_AS_DUMPUSER:
	    uname_target = getconf_str(CNF_DUMPUSER);
	    if ((pw = getpwnam(uname_target)) == NULL) {
		error(_("cannot look up dumpuser \"%s\""), uname_target);
		/*NOTREACHED*/
	    }
	    uid_target = pw->pw_uid;
	    break;

	case RUNNING_AS_CLIENT_LOGIN:
	    uname_target = CLIENT_LOGIN;
	    if ((pw = getpwnam(uname_target)) == NULL) {
		error(_("cannot look up client user \"%s\""), uname_target);
		/*NOTREACHED*/
	    }
	    uid_target = pw->pw_uid;
	    break;

	default:
	    error(_("Unknown check_running_as() call"));
	    /* NOTREACHED */
    }

    if (uid_me != uid_target) {
	error(_("running as user \"%s\" instead of \"%s\""), uname_me, uname_target);
	/*NOTREACHED*/
    }
    amfree(uname_me);

#else
    /* Quiet unused variable warning */
    (void)who;
#endif
}

int
set_root_privs(int need_root)
{
#ifndef SINGLE_USERID
    if (need_root) {
        if (seteuid(0) == -1) return 0;
        /* (we don't switch the group back) */
    } else {
	if (geteuid() != 0) return 0;
        if (seteuid(getuid()) == -1) return 0;
        if (setegid(getgid()) == -1) return 0;
    }
#else
    (void)need_root; /* Quiet unused variable warning */
#endif
    return 1;
}

int
become_root(void)
{
#ifndef SINGLE_USERID
    if (setuid(0) == -1) return 0;
#endif
    return 1;
}

/*
 * Process parameters
 */

/* current process name */
#define MAX_PNAME 128
static char pname[MAX_PNAME] = "unknown";

void
set_pname(char *p)
{
    g_strlcpy(pname, p, sizeof(pname));
}

char *
get_pname(void)
{
    return pname;
}

