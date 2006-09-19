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
#include "arglist.h"
#include "clock.h"

/*#define NET_READ_DEBUG*/

#ifdef NET_READ_DEBUG
#define netprintf(x)    dbprintf(x)
#else
#define netprintf(x)
#endif

static int make_socket(void);
static int connect_port(struct sockaddr_in *addrp, in_port_t port, char *proto,
			struct sockaddr_in *svaddr, int nonblock);

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
make_socket(void)
{
    int s;
    int save_errno;
#if defined(SO_KEEPALIVE) || defined(USE_REUSEADDR)
    int on=1;
    int r;
#endif

    if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        save_errno = errno;
        dbprintf(("%s: make_socket: socket() failed: %s\n",
                  debug_prefix_time(NULL),
                  strerror(save_errno)));
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
	dbprintf(("%s: stream_server: setsockopt(SO_REUSEADDR) failed: %s\n",
		  debug_prefix_time(NULL),
		  strerror(errno)));
	errno = save_errno;
    }
#endif

#ifdef SO_KEEPALIVE
    r = setsockopt(s, SOL_SOCKET, SO_KEEPALIVE,
		   (void *)&on, SIZEOF(on));
    if (r == -1) {
	save_errno = errno;
	dbprintf(("%s: make_socket: setsockopt() failed: %s\n",
                  debug_prefix_time(NULL),
                  strerror(save_errno)));
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
    struct sockaddr_in *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto,
    struct sockaddr_in *svaddr,
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

    dbprintf(("%s: connect_portrange: all ports between %d and %d busy\n",
	      debug_prefix_time(NULL),
	      first_port,
	      last_port));
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
    struct sockaddr_in *addrp,
    in_port_t  		port,
    char *		proto,
    struct sockaddr_in *svaddr,
    int			nonblock)
{
    int			save_errno;
    struct servent *	servPort;
    socklen_t		len;
    int			s;

    servPort = getservbyport((int)htons(port), proto);
    if (servPort != NULL && !strstr(servPort->s_name, "amanda")) {
	dbprintf(("%s: connect_port: Skip port %d: Owned by %s.\n",
		  debug_prefix_time(NULL), port, servPort->s_name));
	return -1;
    }

    if(servPort == NULL)
	dbprintf(("%s: connect_port: Try  port %d: Available   - \n",
		  debug_prefix_time(NULL), port));
    else {
	dbprintf(("%s: connect_port: Try  port %d: Owned by %s - \n",
		  debug_prefix_time(NULL), port, servPort->s_name));
    }

    if ((s = make_socket()) == -1) return -2;

    addrp->sin_port = (in_port_t)htons(port);

    if (bind(s, (struct sockaddr *)addrp, sizeof(*addrp)) != 0) {
	save_errno = errno;
	aclose(s);
	if (save_errno != EADDRINUSE) {
	    dbprintf(("errno %d strerror %s\n",
		      errno, strerror(errno)));
	    errno = save_errno;
	    return -2;
	}
	errno = save_errno;
	return -1;
    }

    /* find out what port was actually used */

    len = sizeof(*addrp);
    if (getsockname(s, (struct sockaddr *)addrp, &len) == -1) {
	save_errno = errno;
	dbprintf(("%s: connect_port: getsockname() failed: %s\n",
		  debug_prefix_time(NULL),
		  strerror(save_errno)));
	aclose(s);
	errno = save_errno;
	return -1;
    }

    if (nonblock)
	fcntl(s, F_SETFL, fcntl(s, F_GETFL, 0)|O_NONBLOCK);
    if (connect(s, (struct sockaddr *)svaddr,
		(socklen_t)sizeof(*svaddr)) == -1 && !nonblock) {
	save_errno = errno;
	dbprintf(("%s: connect_portrange: connect from %s.%d failed\n",
		  debug_prefix_time(NULL),
		  inet_ntoa(addrp->sin_addr),
		  ntohs(addrp->sin_port),
		  strerror(save_errno)));
	dbprintf(("%s: connect_portrange: connect to %s.%d failed: %s\n",
		  debug_prefix_time(NULL),
		  inet_ntoa(svaddr->sin_addr),
		  ntohs(svaddr->sin_port),
		  strerror(save_errno)));
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

    dbprintf(("%s: connected to %s.%d\n",
              debug_prefix_time(NULL),
              inet_ntoa(svaddr->sin_addr),
              ntohs(svaddr->sin_port)));
    dbprintf(("%s: our side is %s.%d\n",
              debug_prefix_time(NULL),
              inet_ntoa(addrp->sin_addr),
              ntohs(addrp->sin_port)));
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
    struct sockaddr_in *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto)
{
    in_port_t port;
    in_port_t cnt;
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
	    if (servPort == NULL) {
		dbprintf(("%s: bind_portrange2: Try  port %d: Available   - ",
		      debug_prefix_time(NULL), port));
	    } else {
		dbprintf(("%s: bind_portrange2: Try  port %d: Owned by %s - ",
		      debug_prefix_time(NULL), port, servPort->s_name));
	    }
	    addrp->sin_port = (in_port_t)htons(port);
	    if (bind(s, (struct sockaddr *)addrp, (socklen_t)sizeof(*addrp)) >= 0) {
	        dbprintf(("Success\n"));
		return 0;
	    }
	    dbprintf(("%s\n", strerror(errno)));
	} else {
	        dbprintf(("%s: bind_portrange2: Skip port %d: Owned by %s.\n",
		      debug_prefix_time(NULL), port, servPort->s_name));
	}
	if (++port > last_port)
	    port = first_port;
    }
    dbprintf(("%s: bind_portrange: all ports between %d and %d busy\n",
		  debug_prefix_time(NULL),
		  first_port,
		  last_port));
    errno = EAGAIN;
    return -1;
}

/*
 * Construct a datestamp (YYYYMMDD) from a time_t.
 */
char *
construct_datestamp(
    time_t *t)
{
    struct tm *tm;
    char datestamp[3 * NUM_STR_SIZE];
    time_t when;

    if (t == NULL) {
	when = time((time_t *)NULL);
    } else {
	when = *t;
    }
    tm = localtime(&when);
    if (!tm)
	return stralloc("19000101");

    snprintf(datestamp, SIZEOF(datestamp),
             "%04d%02d%02d", tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
    return stralloc(datestamp);
}

/*
 * Construct a timestamp (YYYYMMDDHHMMSS) from a time_t.
 */
char *
construct_timestamp(
    time_t *t)
{
    struct tm *tm;
    char timestamp[6 * NUM_STR_SIZE];
    time_t when;

    if (t == NULL) {
	when = time((time_t *)NULL);
    } else {
	when = *t;
    }
    tm = localtime(&when);
    if (!tm)
	return stralloc("19000101000000");

    snprintf(timestamp, SIZEOF(timestamp),
	     "%04d%02d%02d%02d%02d%02d",
	     tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
	     tm->tm_hour, tm->tm_min, tm->tm_sec);
    return stralloc(timestamp);
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
    } else if ((match("[\\\"[:space:][:cntrl:]]", str)) == 0) {
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
	    if (iscntrl(*s))
		*s = '?';
	}
    }
    return (ret);
}

char *
strquotedstr(void)
{
    char *  tok = strtok(NULL, " ");

    if ((tok != NULL) && (tok[0] == '"')) {
	char *	t;
	size_t	len;

        len = strlen(tok);
	do {
	    t = strtok(NULL, " ");
	    tok[len] = ' ';
	    len = strlen(tok);
	} while ((t != NULL) &&
	         (tok[len - 1] != '"') && (tok[len - 2] != '\\'));
    }
    return tok;
}

ssize_t
hexdump(
    const char *buffer,
    size_t	len)
{
    ssize_t rc = -1;

    FILE *stream = popen("od -w10 -c -x -", "w");
	
    if (stream != NULL) {
	fflush(stdout);
	rc = (ssize_t)fwrite(buffer, len, 1, stream);
	if (ferror(stream))
	    rc = -1;
	fclose(stream);
    }
    return rc;
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


void
dump_sockaddr(
	struct sockaddr_in *	sa)
{
	dbprintf(("%s: (sockaddr_in *)%p = { %d, %d, %s }\n",
		debug_prefix_time(NULL), sa, sa->sin_family,
		(int)ntohs(sa->sin_port),
		inet_ntoa(sa->sin_addr)));
}


#ifndef HAVE_LIBREADLINE
/*
 * simple readline() replacements
 */

char *
readline(
    const char *prompt)
{
    printf("%s", prompt);
    fflush(stdout);
    fflush(stderr);
    return agets(stdin);
}

void 
add_history(
    const char *line)
{
    (void)line; 	/* Quite unused parameter warning */
}
#endif
