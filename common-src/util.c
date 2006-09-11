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

int allow_overwrites;
int token_pushed;

tok_t tok, pushed_tok;
val_t tokenval;

int conf_line_num, got_parserror;
FILE *conf_conf = (FILE *)NULL;
char *conf_confname = NULL;
char *conf_line = NULL;
char *conf_char = NULL;

/*#define NET_READ_DEBUG*/

#ifdef NET_READ_DEBUG
#define netprintf(x)    dbprintf(x)
#else
#define netprintf(x)
#endif

static int make_socket(void);
static int connect_port(struct sockaddr_in *addrp, in_port_t port, char *proto,
			struct sockaddr_in *svaddr, int nonblock);

char conftoken_getc(void);
int conftoken_ungetc(int c);

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
                  debug_prefix(NULL),
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
		  debug_prefix(NULL),
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
                  debug_prefix(NULL),
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
	dbprintf(("%s: connect_port: Try  port %d: Available   - ",
		  debug_prefix_time(NULL), port));
    else {
	dbprintf(("%s: connect_port: Try  port %d: Owned by %s - ",
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
		  debug_prefix(NULL),
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
              debug_prefix(NULL),
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


t_conf_var *
get_np(
    t_conf_var	*get_var,
    int 	parm)
{
    t_conf_var *np;

    for(np = get_var; np->token != CONF_UNKNOWN; np++) {
	if(np->parm == parm)
	    break;
    }

    if(np->token == CONF_UNKNOWN) {
	error("error [unknown getconf_np parm: %d]", parm);
	/* NOTREACHED */
    }
    return np;
}

void
get_simple(
    val_t  *var,
    tok_t  type)
{
    ckseen(&var->seen);

    switch(type) {
    case CONF_STRING:
    case CONF_IDENT:
	get_conftoken(type);
	var->v.s = newstralloc(var->v.s, tokenval.v.s);
	malloc_mark(var->v.s);
	break;

    case CONF_INT:
	var->v.i = get_int();
	break;

    case CONF_LONG:
	var->v.l = get_long();
	break;

    case CONF_SIZE:
	var->v.size = get_size();
	break;

    case CONF_AM64:
	var->v.am64 = get_am64_t();
	break;

    case CONF_BOOL:
	var->v.i = get_bool();
	break;

    case CONF_REAL:
	get_conftoken(CONF_REAL);
	var->v.r = tokenval.v.r;
	break;

    case CONF_TIME:
	var->v.t = get_time();
	break;

    default:
	error("error [unknown get_simple type: %d]", type);
	/* NOTREACHED */
    }
    return;
}

time_t
get_time(void)
{
    time_t hhmm;

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INT:
#if SIZEOF_TIME_T < SIZEOF_INT
	if ((off_t)tokenval.v.i >= (off_t)TIME_MAX)
	    conf_parserror("value too large");
#endif
	hhmm = (time_t)tokenval.v.i;
	break;

    case CONF_LONG:
#if SIZEOF_TIME_T < SIZEOF_LONG
	if ((off_t)tokenval.v.l >= (off_t)TIME_MAX)
	    conf_parserror("value too large");
#endif
	hhmm = (time_t)tokenval.v.l;
	break;

    case CONF_SIZE:
#if SIZEOF_TIME_T < SIZEOF_SSIZE_T
	if ((off_t)tokenval.v.size >= (off_t)TIME_MAX)
	    conf_parserror("value too large");
#endif
	hhmm = (time_t)tokenval.v.size;
	break;

    case CONF_AM64:
#if SIZEOF_TIME_T < SIZEOF_LONG_LONG
	if ((off_t)tokenval.v.am64 >= (off_t)TIME_MAX)
	    conf_parserror("value too large");
#endif
	hhmm = (time_t)tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	hhmm = TIME_MAX;
	break;

    default:
	conf_parserror("a time is expected");
	hhmm = 0;
	break;
    }
    return hhmm;
}

keytab_t numb_keytable[] = {
    { "B", CONF_MULT1 },
    { "BPS", CONF_MULT1 },
    { "BYTE", CONF_MULT1 },
    { "BYTES", CONF_MULT1 },
    { "DAY", CONF_MULT1 },
    { "DAYS", CONF_MULT1 },
    { "INF", CONF_AMINFINITY },
    { "K", CONF_MULT1K },
    { "KB", CONF_MULT1K },
    { "KBPS", CONF_MULT1K },
    { "KBYTE", CONF_MULT1K },
    { "KBYTES", CONF_MULT1K },
    { "KILOBYTE", CONF_MULT1K },
    { "KILOBYTES", CONF_MULT1K },
    { "KPS", CONF_MULT1K },
    { "M", CONF_MULT1M },
    { "MB", CONF_MULT1M },
    { "MBPS", CONF_MULT1M },
    { "MBYTE", CONF_MULT1M },
    { "MBYTES", CONF_MULT1M },
    { "MEG", CONF_MULT1M },
    { "MEGABYTE", CONF_MULT1M },
    { "MEGABYTES", CONF_MULT1M },
    { "G", CONF_MULT1G },
    { "GB", CONF_MULT1G },
    { "GBPS", CONF_MULT1G },
    { "GBYTE", CONF_MULT1G },
    { "GBYTES", CONF_MULT1G },
    { "GIG", CONF_MULT1G },
    { "GIGABYTE", CONF_MULT1G },
    { "GIGABYTES", CONF_MULT1G },
    { "MPS", CONF_MULT1M },
    { "TAPE", CONF_MULT1 },
    { "TAPES", CONF_MULT1 },
    { "WEEK", CONF_MULT7 },
    { "WEEKS", CONF_MULT7 },
    { NULL, CONF_IDENT }
};

int
get_int(void)
{
    int val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INT:
	val = tokenval.v.i;
	break;

    case CONF_LONG:
#if SIZEOF_INT < SIZEOF_LONG
	if ((off_t)tokenval.v.l > (off_t)INT_MAX)
	    conf_parserror("value too large");
	if ((off_t)tokenval.v.l < (off_t)INT_MIN)
	    conf_parserror("value too small");
#endif
	val = (int)tokenval.v.l;
	break;

    case CONF_SIZE:
#if SIZEOF_INT < SIZEOF_SSIZE_T
	if ((off_t)tokenval.v.size > (off_t)INT_MAX)
	    conf_parserror("value too large");
	if ((off_t)tokenval.v.size < (off_t)INT_MIN)
	    conf_parserror("value too small");
#endif
	val = (int)tokenval.v.size;
	break;

    case CONF_AM64:
#if SIZEOF_INT < SIZEOF_LONG_LONG
	if (tokenval.v.am64 > (off_t)INT_MAX)
	    conf_parserror("value too large");
	if (tokenval.v.am64 < (off_t)INT_MIN)
	    conf_parserror("value too small");
#endif
	val = (int)tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	val = INT_MAX;
	break;

    default:
	conf_parserror("an int is expected");
	val = 0;
	break;
    }

    /* get multiplier, if any */
    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_END:
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > (INT_MAX / 7))
	    conf_parserror("value too large");
	if (val < (INT_MIN / 7))
	    conf_parserror("value too small");
	val *= 7;
	break;

    case CONF_MULT1M:
	if (val > (INT_MAX / 1024))
	    conf_parserror("value too large");
	if (val < (INT_MIN / 1024))
	    conf_parserror("value too small");
	val *= 1024;
	break;

    case CONF_MULT1G:
	if (val > (INT_MAX / (1024 * 1024)))
	    conf_parserror("value too large");
	if (val < (INT_MIN / (1024 * 1024)))
	    conf_parserror("value too small");
	val *= 1024 * 1024;
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;
    return val;
}

long
get_long(void)
{
    long val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_LONG:
	val = tokenval.v.l;
	break;

    case CONF_INT:
#if SIZEOF_LONG < SIZEOF_INT
	if ((off_t)tokenval.v.i > (off_t)LONG_MAX)
	    conf_parserror("value too large");
	if ((off_t)tokenval.v.i < (off_t)LONG_MIN)
	    conf_parserror("value too small");
#endif
	val = (long)tokenval.v.i;
	break;

    case CONF_SIZE:
#if SIZEOF_LONG < SIZEOF_SSIZE_T
	if ((off_t)tokenval.v.size > (off_t)LONG_MAX)
	    conf_parserror("value too large");
	if ((off_t)tokenval.v.size < (off_t)LONG_MIN)
	    conf_parserror("value too small");
#endif
	val = (long)tokenval.v.size;
	break;

    case CONF_AM64:
#if SIZEOF_LONG < SIZEOF_LONG_LONG
	if (tokenval.v.am64 > (off_t)LONG_MAX)
	    conf_parserror("value too large");
	if (tokenval.v.am64 < (off_t)LONG_MIN)
	    conf_parserror("value too small");
#endif
	val = (long)tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	val = (long)LONG_MAX;
	break;

    default:
	conf_parserror("a long is expected");
	val = 0;
	break;
    }

    /* get multiplier, if any */
    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > (LONG_MAX / 7L))
	    conf_parserror("value too large");
	if (val < (LONG_MIN / 7L))
	    conf_parserror("value too small");
	val *= 7L;
	break;

    case CONF_MULT1M:
	if (val > (LONG_MAX / 1024L))
	    conf_parserror("value too large");
	if (val < (LONG_MIN / 1024L))
	    conf_parserror("value too small");
	val *= 1024L;
	break;

    case CONF_MULT1G:
	if (val > (LONG_MAX / (1024L * 1024L)))
	    conf_parserror("value too large");
	if (val < (LONG_MIN / (1024L * 1024L)))
	    conf_parserror("value too small");
	val *= 1024L * 1024L;
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;
    return val;
}

ssize_t
get_size(void)
{
    ssize_t val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_SIZE:
	val = tokenval.v.size;
	break;

    case CONF_INT:
#if SIZEOF_SIZE_T < SIZEOF_INT
	if ((off_t)tokenval.v.i > (off_t)SSIZE_MAX)
	    conf_parserror("value too large");
	if ((off_t)tokenval.v.i < (off_t)SSIZE_MIN)
	    conf_parserror("value too small");
#endif
	val = (ssize_t)tokenval.v.i;
	break;

    case CONF_LONG:
#if SIZEOF_SIZE_T < SIZEOF_LONG
	if ((off_t)tokenval.v.l > (off_t)SSIZE_MAX)
	    conf_parserror("value too large");
	if ((off_t)tokenval.v.l < (off_t)SSIZE_MIN)
	    conf_parserror("value too small");
#endif
	val = (ssize_t)tokenval.v.l;
	break;

    case CONF_AM64:
#if SIZEOF_SIZE_T < SIZEOF_LONG_LONG
	if (tokenval.v.am64 > (off_t)SSIZE_MAX)
	    conf_parserror("value too large");
	if (tokenval.v.am64 < (off_t)SSIZE_MIN)
	    conf_parserror("value too small");
#endif
	val = (ssize_t)tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	val = (ssize_t)SSIZE_MAX;
	break;

    default:
	conf_parserror("an integer is expected");
	val = 0;
	break;
    }

    /* get multiplier, if any */
    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > (ssize_t)(SSIZE_MAX / 7))
	    conf_parserror("value too large");
	if (val < (ssize_t)(SSIZE_MIN / 7))
	    conf_parserror("value too small");
	val *= (ssize_t)7;
	break;

    case CONF_MULT1M:
	if (val > (ssize_t)(SSIZE_MAX / (ssize_t)1024))
	    conf_parserror("value too large");
	if (val < (ssize_t)(SSIZE_MIN / (ssize_t)1024))
	    conf_parserror("value too small");
	val *= (ssize_t)1024;
	break;

    case CONF_MULT1G:
	if (val > (ssize_t)(SSIZE_MAX / (1024 * 1024)))
	    conf_parserror("value too large");
	if (val < (ssize_t)(SSIZE_MIN / (1024 * 1024)))
	    conf_parserror("value too small");
	val *= (ssize_t)(1024 * 1024);
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;
    return val;
}

off_t
get_am64_t(void)
{
    off_t val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_INT:
	val = (off_t)tokenval.v.i;
	break;

    case CONF_LONG:
	val = (off_t)tokenval.v.l;
	break;

    case CONF_SIZE:
	val = (off_t)tokenval.v.size;
	break;

    case CONF_AM64:
	val = tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	val = AM64_MAX;
	break;

    default:
	conf_parserror("an am64 is expected %d", tok);
	val = 0;
	break;
    }

    /* get multiplier, if any */
    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > AM64_MAX/7 || val < AM64_MIN/7)
	    conf_parserror("value too large");
	val *= 7;
	break;

    case CONF_MULT1M:
	if (val > AM64_MAX/1024 || val < AM64_MIN/1024)
	    conf_parserror("value too large");
	val *= 1024;
	break;

    case CONF_MULT1G:
	if (val > AM64_MAX/(1024*1024) || val < AM64_MIN/(1024*1024))
	    conf_parserror("value too large");
	val *= 1024*1024;
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;

    return val;
}

keytab_t bool_keytable[] = {
    { "Y", CONF_ATRUE },
    { "YES", CONF_ATRUE },
    { "T", CONF_ATRUE },
    { "TRUE", CONF_ATRUE },
    { "ON", CONF_ATRUE },
    { "N", CONF_AFALSE },
    { "NO", CONF_AFALSE },
    { "F", CONF_AFALSE },
    { "FALSE", CONF_AFALSE },
    { "OFF", CONF_AFALSE },
    { NULL, CONF_IDENT }
};

int
get_bool(void)
{
    int val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = bool_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_INT:
	if (tokenval.v.i != 0)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_LONG:
	if (tokenval.v.l != 0L)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_SIZE:
	if (tokenval.v.size != (size_t)0)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_AM64:
	if (tokenval.v.am64 != (off_t)0)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_ATRUE:
	val = 1;
	break;

    case CONF_AFALSE:
	val = 0;
	break;

    case CONF_NL:
	unget_conftoken();
	val = 2; /* no argument - most likely TRUE */
	break;
    default:
	unget_conftoken();
	val = 3; /* a bad argument - most likely TRUE */
	conf_parserror("YES, NO, TRUE, FALSE, ON, OFF expected");
	break;
    }

    keytable = save_kt;
    return val;
}

void
ckseen(
    int *seen)
{
    if (*seen && !allow_overwrites && conf_line_num != -2) {
	conf_parserror("duplicate parameter, prev def on line %d", *seen);
    }
    *seen = conf_line_num;
}

printf_arglist_function(void conf_parserror, const char *, format)
{
    va_list argp;

    /* print error message */

    if(conf_line)
	fprintf(stderr, "argument \"%s\": ", conf_line);
    else
	fprintf(stderr, "\"%s\", line %d: ", conf_confname, conf_line_num);
    arglist_start(argp, format);
    vfprintf(stderr, format, argp);
    arglist_end(argp);
    fputc('\n', stderr);

    got_parserror = 1;
}

tok_t
lookup_keyword(
    char *	str)
{
    keytab_t *kwp;

    /* switch to binary search if performance warrants */

    for(kwp = keytable; kwp->keyword != NULL; kwp++) {
	if (strcmp(kwp->keyword, str) == 0) break;
    }
    return kwp->token;
}

char tkbuf[4096];

/* push the last token back (can only unget ANY tokens) */
void
unget_conftoken(void)
{
    token_pushed = 1;
    pushed_tok = tok;
    tok = CONF_UNKNOWN;
    return;
}

char
conftoken_getc(void)
{
    if(conf_line == NULL)
	return getc(conf_conf);
    if(*conf_char == '\0')
	return -1;
    return(*conf_char++);
}

int
conftoken_ungetc(
    int c)
{
    if(conf_line == NULL)
	return ungetc(c, conf_conf);
    else if(conf_char > conf_line) {
	if(c == -1)
	    return c;
	conf_char--;
	if(*conf_char != c) {
	    error("*conf_char != c   : %c %c", *conf_char, c);
	    /* NOTREACHED */
	}
    } else {
	error("conf_char == conf_line");
	/* NOTREACHED */
    }
    return c;
}

void
get_conftoken(
    tok_t	exp)
{
    int ch, d;
    off_t am64;
    char *buf;
    char *tmps;
    int token_overflow;
    int inquote = 0;
    int escape = 0;
    int sign;

    if (token_pushed) {
	token_pushed = 0;
	tok = pushed_tok;

	/*
	** If it looked like a key word before then look it
	** up again in the current keyword table.
	*/
	switch(tok) {
	case CONF_LONG:    case CONF_AM64:    case CONF_SIZE:
	case CONF_INT:     case CONF_REAL:    case CONF_STRING:
	case CONF_LBRACE:  case CONF_RBRACE:  case CONF_COMMA:
	case CONF_NL:      case CONF_END:     case CONF_UNKNOWN:
	case CONF_TIME:
	    break;

	default:
	    if (exp == CONF_IDENT)
		tok = CONF_IDENT;
	    else
		tok = lookup_keyword(tokenval.v.s);
	    break;
	}
    }
    else {
	ch = conftoken_getc();

	while(ch != EOF && ch != '\n' && isspace(ch))
	    ch = conftoken_getc();
	if (ch == '#') {	/* comment - eat everything but eol/eof */
	    while((ch = conftoken_getc()) != EOF && ch != '\n') {
		(void)ch; /* Quiet empty loop complaints */	
	    }
	}

	if (isalpha(ch)) {		/* identifier */
	    buf = tkbuf;
	    token_overflow = 0;
	    do {
		if (islower(ch)) ch = toupper(ch);
		if (buf < tkbuf+sizeof(tkbuf)-1) {
		    *buf++ = (char)ch;
		} else {
		    *buf = '\0';
		    if (!token_overflow) {
			conf_parserror("token too long: %.20s...", tkbuf);
		    }
		    token_overflow = 1;
		}
		ch = conftoken_getc();
	    } while(isalnum(ch) || ch == '_' || ch == '-');

	    if (ch != EOF && conftoken_ungetc(ch) == EOF) {
		if (ferror(conf_conf)) {
		    conf_parserror("Pushback of '%c' failed: %s",
				   ch, strerror(ferror(conf_conf)));
		} else {
		    conf_parserror("Pushback of '%c' failed: EOF", ch);
		}
	    }
	    *buf = '\0';

	    tokenval.v.s = tkbuf;

	    if (token_overflow) tok = CONF_UNKNOWN;
	    else if (exp == CONF_IDENT) tok = CONF_IDENT;
	    else tok = lookup_keyword(tokenval.v.s);
	}
	else if (isdigit(ch)) {	/* integer */
	    sign = 1;

negative_number: /* look for goto negative_number below sign is set there */
	    am64 = 0;
	    do {
		am64 = am64 * 10 + (ch - '0');
		ch = conftoken_getc();
	    } while (isdigit(ch));

	    if (ch != '.') {
		if (exp == CONF_INT) {
		    tok = CONF_INT;
		    tokenval.v.i = sign * (int)am64;
		} else if (exp == CONF_LONG) {
		    tok = CONF_LONG;
		    tokenval.v.l = (long)sign * (long)am64;
		} else if (exp != CONF_REAL) {
		    tok = CONF_AM64;
		    tokenval.v.am64 = (off_t)sign * am64;
		} else {
		    /* automatically convert to real when expected */
		    tokenval.v.r = (double)sign * (double)am64;
		    tok = CONF_REAL;
		}
	    } else {
		/* got a real number, not an int */
		tokenval.v.r = sign * (double) am64;
		am64 = 0;
		d = 1;
		ch = conftoken_getc();
		while (isdigit(ch)) {
		    am64 = am64 * 10 + (ch - '0');
		    d = d * 10;
		    ch = conftoken_getc();
		}
		tokenval.v.r += sign * ((double)am64) / d;
		tok = CONF_REAL;
	    }

	    if (ch != EOF &&  conftoken_ungetc(ch) == EOF) {
		if (ferror(conf_conf)) {
		    conf_parserror("Pushback of '%c' failed: %s",
				   ch, strerror(ferror(conf_conf)));
		} else {
		    conf_parserror("Pushback of '%c' failed: EOF", ch);
		}
	    }
	} else switch(ch) {
	case '"':			/* string */
	    buf = tkbuf;
	    token_overflow = 0;
	    inquote = 1;
	    *buf++ = (char)ch;
	    while (inquote && ((ch = conftoken_getc()) != EOF)) {
		if (ch == '\n') {
		    if (!escape)
			break;
		    escape = 0;
		    buf--; /* Consume escape in buffer */
		} else if (ch == '\\') {
		    escape = 1;
		} else {
		    if (ch == '"') {
			if (!escape)
			    inquote = 0;
		    }
		    escape = 0;
		}

		if(buf >= &tkbuf[sizeof(tkbuf) - 1]) {
		    if (!token_overflow) {
			conf_parserror("string too long: %.20s...", tkbuf);
		    }
		    token_overflow = 1;
		    break;
		}
		*buf++ = (char)ch;
	    }
	    *buf = '\0';

	    /*
	     * A little manuver to leave a fully unquoted, unallocated  string
	     * in tokenval.v.s
	     */
	    tmps = unquote_string(tkbuf);
	    strncpy(tkbuf, tmps, sizeof(tkbuf));
	    amfree(tmps);
	    tokenval.v.s = tkbuf;

	    tok = (token_overflow) ? CONF_UNKNOWN :
			(exp == CONF_IDENT) ? CONF_IDENT : CONF_STRING;
	    break;

	case '-':
	    ch = conftoken_getc();
	    if (isdigit(ch)) {
		sign = -1;
		goto negative_number;
	    }
	    else {
		if (ch != EOF && conftoken_ungetc(ch) == EOF) {
		    if (ferror(conf_conf)) {
			conf_parserror("Pushback of '%c' failed: %s",
				       ch, strerror(ferror(conf_conf)));
		    } else {
			conf_parserror("Pushback of '%c' failed: EOF", ch);
		    }
		}
		tok = CONF_UNKNOWN;
	    }
	    break;

	case ',':
	    tok = CONF_COMMA;
	    break;

	case '{':
	    tok = CONF_LBRACE;
	    break;

	case '}':
	    tok = CONF_RBRACE;
	    break;

	case '\n':
	    tok = CONF_NL;
	    break;

	case EOF:
	    tok = CONF_END;
	    break;

	default:
	    tok = CONF_UNKNOWN;
	    break;
	}
    }

    if (exp != CONF_ANY && tok != exp) {
	char *str;
	keytab_t *kwp;

	switch(exp) {
	case CONF_LBRACE:
	    str = "\"{\"";
	    break;

	case CONF_RBRACE:
	    str = "\"}\"";
	    break;

	case CONF_COMMA:
	    str = "\",\"";
	    break;

	case CONF_NL:
	    str = "end of line";
	    break;

	case CONF_END:
	    str = "end of file";
	    break;

	case CONF_INT:
	    str = "an integer";
	    break;

	case CONF_REAL:
	    str = "a real number";
	    break;

	case CONF_STRING:
	    str = "a quoted string";
	    break;

	case CONF_IDENT:
	    str = "an identifier";
	    break;

	default:
	    for(kwp = keytable; kwp->keyword != NULL; kwp++) {
		if (exp == kwp->token)
		    break;
	    }
	    if (kwp->keyword == NULL)
		str = "token not";
	    else
		str = kwp->keyword;
	    break;
	}
	conf_parserror("%s is expected", str);
	tok = exp;
	if (tok == CONF_INT)
	    tokenval.v.i = 0;
	else
	    tokenval.v.s = "";
    }
}


void
read_string(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    get_conftoken(CONF_STRING);
    val->v.s = newstralloc(val->v.s, tokenval.v.s);
}

void
read_ident(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    get_conftoken(CONF_IDENT);
    val->v.s = newstralloc(val->v.s, tokenval.v.s);
}

void
read_int(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.i = get_int();
}

void
read_long(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.l = get_long();
}

void
read_size(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.size = get_size();
}

void
read_am64(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.am64 = get_am64_t();
}

void
read_bool(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.i = get_bool();
}

void
read_real(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    get_conftoken(CONF_REAL);
    val->v.r = tokenval.v.r;
}

void
read_time(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.t = get_time();
}

void
copy_val_t(
    val_t *valdst,
    val_t *valsrc)
{
    if(valsrc->seen) {
	valdst->type = valsrc->type;
	valdst->seen = valsrc->seen;
	switch(valsrc->type) {
	case CONFTYPE_INT:
	case CONFTYPE_BOOL:
	case CONFTYPE_COMPRESS:
	case CONFTYPE_ENCRYPT:
	case CONFTYPE_HOLDING:
	case CONFTYPE_ESTIMATE:
	case CONFTYPE_STRATEGY:
	case CONFTYPE_TAPERALGO:
	case CONFTYPE_PRIORITY:
	    valdst->v.i = valsrc->v.i;
	    break;

	case CONFTYPE_LONG:
	    valdst->v.l = valsrc->v.l;
	    break;

	case CONFTYPE_SIZE:
	    valdst->v.size = valsrc->v.size;
	    break;

	case CONFTYPE_AM64:
	    valdst->v.am64 = valsrc->v.am64;
	    break;

	case CONFTYPE_REAL:
	    valdst->v.r = valsrc->v.r;
	    break;

	case CONFTYPE_RATE:
	    valdst->v.rate[0] = valsrc->v.rate[0];
	    valdst->v.rate[1] = valsrc->v.rate[1];
	    break;

	case CONFTYPE_IDENT:
	case CONFTYPE_STRING:
	    valdst->v.s = stralloc(valsrc->v.s);
	    break;

	case CONFTYPE_TIME:
	    valdst->v.t = valsrc->v.t;
	    break;

	case CONFTYPE_SL:
	    valdst->v.sl = duplicate_sl(valsrc->v.sl);
	    break;

	case CONFTYPE_EXINCLUDE:
	    valdst->v.exinclude.type = valsrc->v.exinclude.type;
	    valdst->v.exinclude.optional = valsrc->v.exinclude.optional;
	    valdst->v.exinclude.sl = duplicate_sl(valsrc->v.exinclude.sl);
	    break;
	}
    }
}

void
free_val_t(
    val_t *val)
{
    switch(val->type) {
	case CONFTYPE_INT:
	case CONFTYPE_BOOL:
	case CONFTYPE_COMPRESS:
	case CONFTYPE_ENCRYPT:
	case CONFTYPE_HOLDING:
	case CONFTYPE_ESTIMATE:
	case CONFTYPE_STRATEGY:
	case CONFTYPE_SIZE:
	case CONFTYPE_TAPERALGO:
	case CONFTYPE_PRIORITY:
	case CONFTYPE_LONG:
	case CONFTYPE_AM64:
	case CONFTYPE_REAL:
	case CONFTYPE_RATE:
	    break;

	case CONFTYPE_IDENT:
	case CONFTYPE_STRING:
	    amfree(val->v.s);
	    break;

	case CONFTYPE_TIME:
	    break;

	case CONFTYPE_SL:
	    free_sl(val->v.sl);
	    break;

	case CONFTYPE_EXINCLUDE:
	    free_sl(val->v.exinclude.sl);
	    break;
    }
    val->seen = 0;
}

char *
taperalgo2str(
    int taperalgo)
{
    if(taperalgo == ALGO_FIRST) return "FIRST";
    if(taperalgo == ALGO_FIRSTFIT) return "FIRSTFIT";
    if(taperalgo == ALGO_LARGEST) return "LARGEST";
    if(taperalgo == ALGO_LARGESTFIT) return "LARGESTFIT";
    if(taperalgo == ALGO_SMALLEST) return "SMALLEST";
    if(taperalgo == ALGO_LAST) return "LAST";
    return "UNKNOWN";
}

static char buffer_conf_print[1025];

char *
conf_print(
    val_t *val)
{
    struct tm *stm;
    int pos;

    buffer_conf_print[0] = '\0';
    switch(val->type) {
    case CONFTYPE_INT:
	snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), "%d", val->v.i);
	break;

    case CONFTYPE_LONG:
	snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), "%ld", val->v.l);
	break;

    case CONFTYPE_SIZE:
	snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), SSIZE_T_FMT,
		(SSIZE_T_FMT_TYPE)val->v.size);
	break;

    case CONFTYPE_AM64:
	snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), OFF_T_FMT ,
		 (OFF_T_FMT_TYPE)val->v.am64);
	break;

    case CONFTYPE_REAL:
	snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), "%0.5f" , val->v.r);
	break;

    case CONFTYPE_RATE:
	snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), "%0.5f %0.5f" , val->v.rate[0], val->v.rate[1]);
	break;

    case CONFTYPE_IDENT:
	if(val->v.s) {
	    strncpy(buffer_conf_print, val->v.s, SIZEOF(buffer_conf_print));
	    buffer_conf_print[SIZEOF(buffer_conf_print) - 1] = '\0';
	} else
	    buffer_conf_print[0] = '\0';
	break;

    case CONFTYPE_STRING:
	buffer_conf_print[0] = '"';
	if(val->v.s) {
	    strncpy(&buffer_conf_print[1], val->v.s,
	    		SIZEOF(buffer_conf_print) - 1);
	    buffer_conf_print[SIZEOF(buffer_conf_print) - 2] = '\0';
	    buffer_conf_print[strlen(buffer_conf_print)] = '"';
	} else {
	    buffer_conf_print[1] = '"';
	    buffer_conf_print[2] = '\0';
	}
	break;

    case CONFTYPE_TIME:
	stm = localtime(&val->v.t);
	if (stm) {
	    snprintf(buffer_conf_print, SIZEOF(buffer_conf_print),
		     "%d%02d%02d", stm->tm_hour, stm->tm_min, stm->tm_sec);
	} else {
	    strcpy(buffer_conf_print, "00000");
	}
	break;

    case CONFTYPE_SL:
	buffer_conf_print[0] = '\0';
	break;

    case CONFTYPE_EXINCLUDE:
	buffer_conf_print[0] = '\0';
	if(val->v.exinclude.type == 0)
	    strncpy(buffer_conf_print, "LIST ", SIZEOF(buffer_conf_print));
	else
	    strncpy(buffer_conf_print, "FILE ", SIZEOF(buffer_conf_print));
	pos = 5;
	if(val->v.exinclude.optional == 1)
	    strncpy(&buffer_conf_print[pos], "OPTIONAL ", SIZEOF(buffer_conf_print));
	pos += 9;
	break;

    case CONFTYPE_BOOL:
	if(val->v.i)
	    strncpy(buffer_conf_print, "yes", SIZEOF(buffer_conf_print));
	else
	    strncpy(buffer_conf_print, "no", SIZEOF(buffer_conf_print));
	break;

    case CONFTYPE_STRATEGY:
	switch(val->v.i) {
	case DS_SKIP:
	    strncpy(buffer_conf_print, "SKIP", SIZEOF(buffer_conf_print));
	    break;

	case DS_STANDARD:
	    strncpy(buffer_conf_print, "STANDARD", SIZEOF(buffer_conf_print));
	    break;

	case DS_NOFULL:
	    strncpy(buffer_conf_print, "NOFULL", SIZEOF(buffer_conf_print));
	    break;

	case DS_NOINC:
	    strncpy(buffer_conf_print, "NOINC", SIZEOF(buffer_conf_print));
	    break;

	case DS_HANOI:
	    strncpy(buffer_conf_print, "HANOI", SIZEOF(buffer_conf_print));
	    break;

	case DS_INCRONLY:
	    strncpy(buffer_conf_print, "INCRONLY", SIZEOF(buffer_conf_print));
	    break;
	}
	break;

    case CONFTYPE_COMPRESS:
	switch(val->v.i) {
	case COMP_NONE:
	    strncpy(buffer_conf_print, "NONE", SIZEOF(buffer_conf_print));
	    break;

	case COMP_FAST:
	    strncpy(buffer_conf_print, "CLIENT FAST", SIZEOF(buffer_conf_print));
	    break;

	case COMP_BEST:
	    strncpy(buffer_conf_print, "CLIENT BEST", SIZEOF(buffer_conf_print));
	    break;

	case COMP_CUST:
	    strncpy(buffer_conf_print, "CLIENT CUSTOM", SIZEOF(buffer_conf_print));
	    break;

	case COMP_SERV_FAST:
	    strncpy(buffer_conf_print, "SERVER FAST", SIZEOF(buffer_conf_print));
	    break;

	case COMP_SERV_BEST:
	    strncpy(buffer_conf_print, "SERVER FAST", SIZEOF(buffer_conf_print));
	    break;

	case COMP_SERV_CUST:
	    strncpy(buffer_conf_print, "SERVER CUSTOM", SIZEOF(buffer_conf_print));
	    break;
	}
	break;

    case CONFTYPE_ESTIMATE:
	switch(val->v.i) {
	case ES_CLIENT:
	    strncpy(buffer_conf_print, "CLIENT", SIZEOF(buffer_conf_print));
	    break;

	case ES_SERVER:
	    strncpy(buffer_conf_print, "SERVER", SIZEOF(buffer_conf_print));
	    break;

	case ES_CALCSIZE:
	    strncpy(buffer_conf_print, "CALCSIZE", SIZEOF(buffer_conf_print));
	    break;
	}
	break;

     case CONFTYPE_ENCRYPT:
	switch(val->v.i) {
	case ENCRYPT_NONE:
	    strncpy(buffer_conf_print, "NONE", SIZEOF(buffer_conf_print));
	    break;

	case ENCRYPT_CUST:
	    strncpy(buffer_conf_print, "CLIENT", SIZEOF(buffer_conf_print));
	    break;

	case ENCRYPT_SERV_CUST:
	    strncpy(buffer_conf_print, "SERVER", SIZEOF(buffer_conf_print));
	    break;
	}
	break;

     case CONFTYPE_HOLDING:
	switch(val->v.i) {
	case HOLD_NEVER:
	    strncpy(buffer_conf_print, "NEVER", SIZEOF(buffer_conf_print));
	    break;

	case HOLD_AUTO:
	    strncpy(buffer_conf_print, "AUTO", SIZEOF(buffer_conf_print));
	    break;

	case HOLD_REQUIRED:
	    strncpy(buffer_conf_print, "REQUIRED", SIZEOF(buffer_conf_print));
	    break;
	}
	break;

     case CONFTYPE_TAPERALGO:
	strncpy(buffer_conf_print, taperalgo2str(val->v.i), SIZEOF(buffer_conf_print));
	break;

     case CONFTYPE_PRIORITY:
	switch(val->v.i) {
	case 0:
	    strncpy(buffer_conf_print, "LOW", SIZEOF(buffer_conf_print));
	    break;

	case 1:
	    strncpy(buffer_conf_print, "MEDIUM", SIZEOF(buffer_conf_print));
	    break;

	case 2:
	    strncpy(buffer_conf_print, "HIGH", SIZEOF(buffer_conf_print));
	    break;
	}
	break;
    }
    buffer_conf_print[SIZEOF(buffer_conf_print) - 1] = '\0';
    return buffer_conf_print;
}

void
conf_init_string(
    val_t *val,
    char  *s)
{
    val->seen = 0;
    val->type = CONFTYPE_STRING;
    if(s)
	val->v.s = stralloc(s);
    else
	val->v.s = NULL;
}

void
conf_init_ident(
    val_t *val,
    char  *s)
{
    val->seen = 0;
    val->type = CONFTYPE_IDENT;
    if(s)
	val->v.s = stralloc(s);
    else
	val->v.s = NULL;
}

void
conf_init_int(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_INT;
    val->v.i = i;
}

void
conf_init_bool(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_BOOL;
    val->v.i = i;
}

void
conf_init_strategy(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_STRATEGY;
    val->v.i = i;
}

void
conf_init_estimate(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_ESTIMATE;
    val->v.i = i;
}

void
conf_init_taperalgo(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_TAPERALGO;
    val->v.i = i;
}

void
conf_init_priority(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_PRIORITY;
    val->v.i = i;
}

void
conf_init_compress(
    val_t *val,
    comp_t    i)
{
    val->seen = 0;
    val->type = CONFTYPE_COMPRESS;
    val->v.i = (int)i;
}

void
conf_init_encrypt(
    val_t *val,
    encrypt_t    i)
{
    val->seen = 0;
    val->type = CONFTYPE_ENCRYPT;
    val->v.i = (int)i;
}

void
conf_init_holding(
    val_t              *val,
    dump_holdingdisk_t  i)
{
    val->seen = 0;
    val->type = CONFTYPE_HOLDING;
    val->v.i = (int)i;
}

void
conf_init_long(
    val_t *val,
    long   l)
{
    val->seen = 0;
    val->type = CONFTYPE_LONG;
    val->v.l = l;
}

void
conf_init_size(
    val_t *val,
    ssize_t   sz)
{
    val->seen = 0;
    val->type = CONFTYPE_SIZE;
    val->v.size = sz;
}

void
conf_init_am64(
    val_t *val,
    off_t   l)
{
    val->seen = 0;
    val->type = CONFTYPE_AM64;
    val->v.am64 = l;
}

void
conf_init_real(
    val_t  *val,
    double r)
{
    val->seen = 0;
    val->type = CONFTYPE_REAL;
    val->v.r = r;
}

void
conf_init_rate(
    val_t  *val,
    double r1,
    double r2)
{
    val->seen = 0;
    val->type = CONFTYPE_RATE;
    val->v.rate[0] = r1;
    val->v.rate[1] = r2;
}

void
conf_init_time(
    val_t *val,
    time_t   t)
{
    val->seen = 0;
    val->type = CONFTYPE_TIME;
    val->v.t = t;
}

void
conf_init_sl(
    val_t *val,
    sl_t  *sl)
{
    val->seen = 0;
    val->type = CONFTYPE_AM64;
    val->v.sl = sl;
}

void
conf_init_exinclude(
    val_t *val)
{
    val->seen = 0;
    val->type = CONFTYPE_EXINCLUDE;
    val->v.exinclude.type = 0;
    val->v.exinclude.optional = 0;
    val->v.exinclude.sl = NULL;
}

void
conf_set_string(
    val_t *val,
    char *s)
{
    val->seen = -1;
    val->type = CONFTYPE_STRING;
    amfree(val->v.s);
    val->v.s = stralloc(s);
}

void
conf_set_int(
    val_t *val,
    int    i)
{
    val->seen = -1;
    val->type = CONFTYPE_INT;
    val->v.i = i;
}

void
conf_set_bool(
    val_t *val,
    int    i)
{
    val->seen = -1;
    val->type = CONFTYPE_BOOL;
    val->v.i = i;
}

void
conf_set_compress(
    val_t *val,
    comp_t    i)
{
    val->seen = -1;
    val->type = CONFTYPE_COMPRESS;
    val->v.i = (int)i;
}

void
conf_set_encrypt(
    val_t *val,
    encrypt_t    i)
{
    val->seen = -1;
    val->type = CONFTYPE_COMPRESS;
    val->v.i = (int)i;
}

void
conf_set_holding(
    val_t              *val,
    dump_holdingdisk_t  i)
{
    val->seen = -1;
    val->type = CONFTYPE_HOLDING;
    val->v.i = (int)i;
}

void
conf_set_strategy(
    val_t *val,
    int    i)
{
    val->seen = -1;
    val->type = CONFTYPE_STRATEGY;
    val->v.i = i;
}


int
get_conftype_int(
    val_t *val)
{
    if (val->type != CONFTYPE_INT) {
	error("get_conftype_int: val.type is not CONFTYPE_INT");
	/*NOTREACHED*/
    }
    return val->v.i;
}

long
get_conftype_long(
    val_t *val)
{
    if (val->type != CONFTYPE_LONG) {
	error("get_conftype_long: val.type is not CONFTYPE_LONG");
	/*NOTREACHED*/
    }
    return val->v.l;
}

off_t
get_conftype_am64(
    val_t *val)
{
    if (val->type != CONFTYPE_AM64) {
	error("get_conftype_am64: val.type is not CONFTYPE_AM64");
	/*NOTREACHED*/
    }
    return val->v.am64;
}

double
get_conftype_real(
    val_t *val)
{
    if (val->type != CONFTYPE_REAL) {
	error("get_conftype_real: val.type is not CONFTYPE_REAL");
	/*NOTREACHED*/
    }
    return val->v.r;
}

char *
get_conftype_string(
    val_t *val)
{
    if (val->type != CONFTYPE_STRING) {
	error("get_conftype_string: val.type is not CONFTYPE_STRING");
	/*NOTREACHED*/
    }
    return val->v.s;
}

char *
get_conftype_ident(
    val_t *val)
{
    if (val->type != CONFTYPE_IDENT) {
	error("get_conftype_ident: val.type is not CONFTYPE_IDENT");
	/*NOTREACHED*/
    }
    return val->v.s;
}

time_t
get_conftype_time(
    val_t *val)
{
    if (val->type != CONFTYPE_TIME) {
	error("get_conftype_time: val.type is not CONFTYPE_TIME");
	/*NOTREACHED*/
    }
    return val->v.t;
}

ssize_t
get_conftype_size(
    val_t *val)
{
    if (val->type != CONFTYPE_SIZE) {
	error("get_conftype_size: val.type is not CONFTYPE_SIZE");
	/*NOTREACHED*/
    }
    return val->v.size;
}

sl_t *
get_conftype_sl(
    val_t *val)
{
    if (val->type != CONFTYPE_SL) {
	error("get_conftype_size: val.type is not CONFTYPE_SL");
	/*NOTREACHED*/
    }
    return val->v.sl;
}

int
get_conftype_bool(
    val_t *val)
{
    if (val->type != CONFTYPE_BOOL) {
	error("get_conftype_bool: val.type is not CONFTYPE_BOOL");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_hold(
    val_t *val)
{
    if (val->type != CONFTYPE_HOLDING) {
	error("get_conftype_hold: val.type is not CONFTYPE_HOLDING");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_compress(
    val_t *val)
{
    if (val->type != CONFTYPE_COMPRESS) {
	error("get_conftype_compress: val.type is not CONFTYPE_COMPRESS");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_encrypt(
    val_t *val)
{
    if (val->type != CONFTYPE_ENCRYPT) {
	error("get_conftype_encrypt: val.type is not CONFTYPE_ENCRYPT");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_estimate(
    val_t *val)
{
    if (val->type != CONFTYPE_ESTIMATE) {
	error("get_conftype_extimate: val.type is not CONFTYPE_ESTIMATE");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_strategy(
    val_t *val)
{
    if (val->type != CONFTYPE_STRATEGY) {
	error("get_conftype_strategy: val.type is not CONFTYPE_STRATEGY");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_taperalgo(
    val_t *val)
{
    if (val->type != CONFTYPE_TAPERALGO) {
	error("get_conftype_taperalgo: val.type is not CONFTYPE_TAPERALGO");
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_priority(
    val_t *val)
{
    if (val->type != CONFTYPE_PRIORITY) {
	error("get_conftype_priority: val.type is not CONFTYPE_PRIORITY");
	/*NOTREACHED*/
    }
    return val->v.i;
}

exinclude_t
get_conftype_exinclude(
    val_t *val)
{
    if (val->type != CONFTYPE_EXINCLUDE) {
	error("get_conftype_exinclude: val.type is not CONFTYPE_EXINCLUDE");
	/*NOTREACHED*/
    }
    return val->v.exinclude;
}


void
dump_sockaddr(
	struct sockaddr_in *	sa)
{
	dbprintf(("%s: (sockaddr_in *)%p = { %d, %hd, %s }\n",
		debug_prefix(NULL), sa, sa->sin_family, sa->sin_port,
		inet_ntoa(sa->sin_addr)));
}

void
read_block(
    command_option_t *command_options,
    t_conf_var    *read_var,
    keytab_t *keytab,
    val_t    *valarray,
    char     *prefix,
    char     *errormsg,
    int       read_brace,
    void      (*copy_function)(void))
{
    t_conf_var *np;
    int    saved_conf_line_num;
    int    done;

    if(read_brace) {
	get_conftoken(CONF_LBRACE);
	get_conftoken(CONF_NL);
    }

    done = 0;
    do {
	conf_line_num += 1;
	get_conftoken(CONF_ANY);
	switch(tok) {
	case CONF_RBRACE:
	    done = 1;
	    break;
	case CONF_NL:	/* empty line */
	    break;
	case CONF_END:	/* end of file */
	    done = 1;
	    break;
        case CONF_IDENT:
        case CONF_STRING:
	    if(copy_function) 
		copy_function();
	    else
		conf_parserror("ident not expected");
	    break;
	default:
	    {
		for(np = read_var; np->token != CONF_UNKNOWN; np++)
		    if(np->token == tok) break;

		if(np->token == CONF_UNKNOWN)
		    conf_parserror(errormsg);
		else {
		    np->read_function(np, &valarray[np->parm]);
		    if(np->validate)
			np->validate(np, &valarray[np->parm]);
		}
	    }
	}
	if(tok != CONF_NL && tok != CONF_END && tok != CONF_RBRACE)
	    get_conftoken(CONF_NL);
    } while(!done);

    /* overwrite with command line option */
    saved_conf_line_num = conf_line_num;
    command_overwrite(command_options, read_var, keytab, valarray, prefix);
    conf_line_num = saved_conf_line_num;
}

void
command_overwrite(
    command_option_t *command_options,
    t_conf_var    *overwrite_var,
    keytab_t *keytab,
    val_t    *valarray,
    char     *prefix)
{
    t_conf_var	     *np;
    keytab_t	     *kt;
    char	     *myprefix;
    command_option_t *command_option;

    if(!command_options) return;

    for(np = overwrite_var; np->token != CONF_UNKNOWN; np++) {
	for(kt = keytab; kt->token != CONF_UNKNOWN; kt++)
	    if(kt->token == np->token) break;

	if(kt->token == CONF_UNKNOWN) {
	    error("read_conf: invalid token");
	    /* NOTREACHED */
	}

        for(command_option = command_options; command_option->name != NULL;
							    command_option++) {
	    myprefix = stralloc2(prefix, kt->keyword);
	    if(strcasecmp(myprefix, command_option->name) == 0) {
		command_option->used = 1;
		valarray[np->parm].seen = -2;
		if(np->type == CONFTYPE_STRING &&
		   command_option->value[0] != '"') {
		    conf_line = vstralloc("\"", command_option->value, "\"",
					  NULL);
		}
		else {
		    conf_line = stralloc(command_option->value);
		}
		conf_char = conf_line;
		token_pushed = 0;
		conf_line_num = -2;
		np->read_function(np, &valarray[np->parm]);
		amfree(conf_line);
		conf_line = conf_char = NULL;

		if(np->validate)
		    np->validate(np, &valarray[np->parm]);
	    }
	    amfree(myprefix);
	}
    }
}

void
free_new_argv(
    int new_argc,
    char **new_argv)
{
    int i;
    for(i=0; i<new_argc; i++)
	amfree(new_argv[i]);
    amfree(new_argv);
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
