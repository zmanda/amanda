/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
#include "amutil.h"
#include "match.h"
#include <regex.h>
#include "clock.h"
#include "sockaddr-util.h"
#include "conffile.h"
#include "base64.h"
#include "stream.h"
#include "pipespawn.h"
#include <glib.h>
#include <string.h>
#include "fsusage.h"

GMutex *priv_mutex = NULL;
static int make_socket(sa_family_t family);
static int connect_port(sockaddr_union *addrp, in_port_t port, char *proto,
			sockaddr_union *svaddr, int nonblock, int priv,
			char **msg);

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

    g_debug("make_socket opening socket with family %d", family);
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

    g_debug("make_socket opening socket with family %d: %d", family, s);
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
		   (void *)&on, sizeof(on));
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

GQuark am_util_error_quark(void)
{
    return g_quark_from_static_string("am-util-error-quark");
}

/* addrp is my address */
/* svaddr is the address of the remote machine */
/* return socket on success */
/* return -1     on failure */
int
connect_portrange(
    sockaddr_union *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto,
    sockaddr_union *svaddr,
    int			nonblock,
    int			priv,
    char              **stream_msg)
{
    int			s;
    in_port_t		port;
    static in_port_t	port_in_use[1024];
    static int		nb_port_in_use = 0;
    int			i;
    int			save_errno = EAGAIN;

    assert(first_port <= last_port);
    /* Try a port already used */
    for(i=0; i < nb_port_in_use; i++) {
	port = port_in_use[i];
	if(port >= first_port && port <= last_port) {
	    s = connect_port(addrp, port, proto, svaddr, nonblock, priv, stream_msg);
	    if(s == -2) return -1;
	    if(s >= 0) {
		return s;
	    }
	    if (errno != EAGAIN && errno != EBUSY)
		save_errno = errno;
	}
    }

    /* Try a port in the range */
    for (port = first_port; port <= last_port; port++) {
	s = connect_port(addrp, port, proto, svaddr, nonblock, priv, stream_msg);
	if(s == -2) return -1;
	if(s >= 0) {
	    port_in_use[nb_port_in_use++] = port;
	    return s;
	}
	if (errno != EAGAIN && errno != EBUSY)
	    save_errno = errno;
    }

    dbprintf(_("connect_portrange: All ports between %d and %d are busy.\n"),
	      first_port,
	      last_port);
    errno = save_errno;
    return -1;
}

static int
ambind(
    int s,
    sockaddr_union *addrp,
    socklen_t_equiv socklen,
    char **msg)
{
    ambind_t ambind = {*addrp, socklen };
    int sockfd[2];
    int rc;
    int pid;
    struct msghdr msg_socket;
    struct msghdr msg_ambind_data;
    struct cmsghdr *cmsg;
    char cmsgbuf[CMSG_SPACE(sizeof(s))];
    struct iovec iov[2];
    int r;
    int pipe_stderr[2];
    fd_set readSet;
    int max_set;

#ifdef SOCK_NONBLOCK
    r = socketpair(AF_UNIX, SOCK_DGRAM|SOCK_NONBLOCK, 0, sockfd);
#else
    r = socketpair(AF_UNIX, SOCK_DGRAM, 0, sockfd);
    if (r == 0) {
	int r0 = fcntl(sockfd[0], F_GETFL, 0);
	int r1 = fcntl(sockfd[1], F_GETFL, 0);
	r0 = fcntl(sockfd[0], F_SETFL, r0|O_NONBLOCK);
	r1 = fcntl(sockfd[1], F_SETFL, r1|O_NONBLOCK);
    }
#endif
    if (r < 0) {
	*msg = g_strdup_printf("socketpair failed: %s\n", strerror(errno));
	return -2;
    }
    if (pipe(pipe_stderr) < 0) {
	shutdown(sockfd[0], SHUT_RDWR);
	shutdown(sockfd[1], SHUT_RDWR);
	*msg = g_strdup_printf("pipe failed: %s\n", strerror(errno));
	return -2;
    }

    switch (pid = fork()) {
	case -1: *msg = g_strdup_printf("fork ambind failed: %s",
					strerror(errno));
		 close(sockfd[0]);
		 close(sockfd[1]);
		 close(pipe_stderr[0]);
		 close(pipe_stderr[1]);
		 return -2;

	case  0: //child
		{
		 char *ambind_path = g_strdup_printf("%s/ambind", amlibexecdir);
		 char *socket_name = g_strdup_printf("%d", sockfd[1]);
		 close(sockfd[0]);
		 dup2(pipe_stderr[1], 2);
		 execl(ambind_path, ambind_path, socket_name, NULL);
		 error("error [exec %s: %s]", ambind_path, strerror(errno));
		 exit(1);
		}

	default: //parent
		 close(sockfd[1]);
		 break;
    }

    close(pipe_stderr[1]);
    memset(&msg_socket, 0, sizeof(msg_socket));
    msg_socket.msg_control = cmsgbuf;
    msg_socket.msg_controllen = sizeof(cmsgbuf); // necessary for CMSG_FIRSTHDR to return the correct value
    cmsg = CMSG_FIRSTHDR(&msg_socket);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(s));
    memcpy(CMSG_DATA(cmsg), &s, sizeof(s));
    msg_socket.msg_controllen = cmsg->cmsg_len;

    // send the socket
    if ((sendmsg(sockfd[0], &msg_socket, 0)) < 0) {
	*msg = g_strdup_printf("sendmsg failed A: %s\n",
			        strerror(errno));
	shutdown(sockfd[0], SHUT_RDWR);
	close(pipe_stderr[1]);
	return -2;
    }

    memset(&msg_ambind_data, 0, sizeof(msg_ambind_data));
    iov[0].iov_base = &ambind;
    iov[0].iov_len = sizeof(ambind_t);
    iov[1].iov_base = NULL;
    iov[1].iov_len = 0;
    msg_ambind_data.msg_iov = iov;
    msg_ambind_data.msg_iovlen = 1;

    // send ambind data
    if ((sendmsg(sockfd[0], &msg_ambind_data, 0)) < 0) {
	*msg = g_strdup_printf("sendmsg failed B: %s\n",
			        strerror(errno));
	shutdown(sockfd[0], SHUT_RDWR);
	close(pipe_stderr[0]);
	return -2;
    }

    shutdown(sockfd[0], SHUT_WR);
    max_set = sockfd[0];
    if (max_set < pipe_stderr[0]) max_set = pipe_stderr[0];
    max_set++;

    do {
	struct timeval timeout = { 5, 0 };
	FD_ZERO(&readSet);
	FD_SET(sockfd[0], &readSet);
	FD_SET(pipe_stderr[0], &readSet);

	rc = select(max_set, &readSet, NULL, NULL, &timeout);
    } while (rc < 0 && errno == EINTR);

    if (!FD_ISSET(sockfd[0], &readSet)) {
	FILE *err;
	shutdown(sockfd[0], SHUT_RDWR);
	waitpid(pid, NULL, 0);
	err = fdopen(pipe_stderr[0], "r");
	*msg = agets(err);
	close(pipe_stderr[0]);
	if (strncmp(*msg, "WARNING:", 8) == 0) {
	    return -1;
	}
	return -2;
    }
    close(pipe_stderr[0]);

    // read the message socket msg
    memset(&msg_socket, 0, sizeof(msg_socket));
    msg_socket.msg_control = cmsgbuf;
    msg_socket.msg_controllen = sizeof(cmsgbuf);
    rc = recvmsg(sockfd[0], &msg_socket, 0);
    if (rc == -1) {
	*msg = g_strdup_printf("first recvmsg failed: %s", strerror(errno));
	return -1;
    }
    cmsg = CMSG_FIRSTHDR(&msg_socket);
    if (cmsg == NULL || cmsg -> cmsg_type != SCM_RIGHTS) {
	*msg = g_strdup_printf("The first control structure contains no file descriptor.\n");
	return -2;
    }
    memcpy(&s, CMSG_DATA(cmsg), sizeof(s));

    shutdown(sockfd[0], SHUT_RDWR);
    waitpid(pid, NULL, 0);
    return s;
}

/* addrp is my address */
/* svaddr is the address of the remote machine */
/* return -2: Don't try again */
/* return -1: Try with another port */
/* return >0: this is the connected socket */
int
connect_port(
    sockaddr_union *addrp,
    in_port_t  		port,
    char *		proto,
    sockaddr_union *svaddr,
    int			nonblock,
    int			priv,
    char              **msg)
{
    int			save_errno;
    struct servent *	result;
    int			r;
    socklen_t_equiv	len;
    socklen_t_equiv	socklen;
    int			s;

#ifdef HAVE_GETSERVBYPORT_R
    struct servent	servPort;
    char		buf[2048];
# ifdef GETSERVBYPORT_R5
    result = getservbyport_r((int)htons(port), proto, &servPort, buf, 2048);
    if (result == 0) {
	assert(errno != ERANGE);
    }
# else
    r = getservbyport_r((int)htons(port), proto, &servPort, buf, 2048, &result);
    assert(r != ERANGE);
# endif
#else
    result = getservbyport((int)htons(port), proto);
#endif

    if (result != NULL && !strstr(result->s_name, AMANDA_SERVICE_NAME)) {
	dbprintf(_("connect_port: Skip port %d: owned by %s.\n"),
		  port, result->s_name);
	errno = EBUSY;
	return -1;
    }

    if ((s = make_socket(SU_GET_FAMILY(addrp))) == -1) return -2;

    SU_SET_PORT(addrp, port);
    socklen = SS_LEN(addrp);
    if (!priv) {
	r = bind(s, (struct sockaddr *)addrp, socklen);
#if !defined BROKEN_SENDMSG
    } else if (1) { // if use ambind
	int  old_s = s;
	amfree(*msg);
	r = s = ambind(s, addrp, socklen, msg);
	close(old_s);
	if (*msg) {
	    g_debug("ambind failed: %s", *msg);
	}
	if (r == -2) {
	    return -2;
	}
#endif
    } else { // setuid root
	g_mutex_lock(priv_mutex);
	set_root_privs(1);
	r = bind(s, (struct sockaddr *)addrp, socklen);
	set_root_privs(0);
	g_mutex_unlock(priv_mutex);
    }

    if (r < 0) {
	save_errno = errno;
	aclose(s);
	if( result == NULL) {
	    dbprintf(_("connect_port: Try  port %d: available - %s\n"),
		     port, strerror(save_errno));
	} else {
	    dbprintf(_("connect_port: Try  port %d: owned by %s - %s\n"),
		     port, result->s_name, strerror(save_errno));
	}
	if (save_errno != EADDRINUSE) {
	    errno = save_errno;
	    return -2;
	}

	errno = save_errno;
	return -1;
    }
    if (result == NULL) {
	dbprintf(_("connect_port: Try  port %d: available - Success\n"), port);
    } else {
	dbprintf(_("connect_port: Try  port %d: owned by %s - Success\n"),
		  port, result->s_name);
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

    if (nonblock) {
	int r = fcntl(s, F_GETFL, 0);
	if (r < 0) {
	    save_errno = errno;
	    g_debug("Can't fcntl(F_GETFL): %s", strerror(errno));
	    aclose(s);
	    errno = save_errno;
	    return -1;
	}
	r = fcntl(s, F_SETFL, r|O_NONBLOCK);
	if (r < 0) {
	    save_errno = errno;
	    g_debug("Can't fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
	    errno = save_errno;
	    aclose(s);
	    return -1;
	}
    }
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
	    return -2;
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
 * Returns negative on error (EGAIN if all ports are in use).
 * Returns the new socket on success
 */
int
bind_portrange(
    int			s,
    sockaddr_union *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto,
    int                 priv,
    char              **bind_msg)
{
    in_port_t port;
    in_port_t cnt;
    socklen_t_equiv socklen;
    struct servent *result;
    int             r;
    const in_port_t num_ports = (in_port_t)(last_port - first_port + 1);
    int save_errno = EAGAIN;
    int new_s;

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
#ifdef HAVE_GETSERVBYPORT_R
	struct servent  servPort;
	char            buf[2048];

# ifdef GETSERVBYPORT_R5
	result = getservbyport_r((int)htons(port), proto, &servPort, buf, 2048);
	if (result == 0) {
	    assert(errno != ERANGE);
	}
# else
	r = getservbyport_r((int)htons(port), proto, &servPort, buf, 2048, &result);
	assert(r != ERANGE);
# endif
#else
	result = getservbyport((int)htons(port), proto);
#endif
	amfree(*bind_msg);
g_debug("bind_portrange2: Try  port %d", port);
	if ((result == NULL) || strstr(result->s_name, AMANDA_SERVICE_NAME)) {
	    SU_SET_PORT(addrp, port);
	    socklen = SS_LEN(addrp);
	    if (!priv) {
		r = bind(s, (struct sockaddr *)addrp, socklen);
		new_s = s;
		*bind_msg = g_strdup(strerror(errno));
#if !defined BROKEN_SENDMSG
	    } else if (1) { // if use ambind
		r = new_s = ambind(s, addrp, socklen, bind_msg);
		if (*bind_msg) {
		    g_debug("ambind failed: %s", *bind_msg);
		}
		if (r == -2) {
		    amfree(*bind_msg);
		    return -1;
		}
#endif
	    } else {
		g_mutex_lock(priv_mutex);
		set_root_privs(1);
		r = bind(s, (struct sockaddr *)addrp, socklen);
		new_s = s;
		*bind_msg = g_strdup(strerror(errno));
		set_root_privs(0);
		g_mutex_unlock(priv_mutex);
	    }
	    if (r >= 0) {
		if (result == NULL) {
		    g_debug(_("bind_portrange2: Try  port %d: Available - Success"), port);
		} else {
		    g_debug(_("bind_portrange2: Try  port %d: Owned by %s - Success."), port, result->s_name);
		}
		return new_s;
	    }
	    if (errno != EAGAIN && errno != EBUSY)
		save_errno = errno;
	    if (result == NULL) {
		g_debug(_("bind_portrange2: Try  port %d: Available - %s"),
			port, *bind_msg);
	    } else {
		g_debug(_("bind_portrange2: Try  port %d: Owned by %s - %s"),
			port, result->s_name, *bind_msg);
	    }
	} else {
	        g_debug(_("bind_portrange2: Skip port %d: Owned by %s."),
		      port, result->s_name);
	}
	if (++port > last_port)
	    port = first_port;
    }
    g_debug(_("bind_portrange: all ports between %d and %d busy"),
		  first_port,
		  last_port);
    errno = save_errno;
    return -2;
}

int
interruptible_accept(
    int sock,
    struct sockaddr *addr,
    socklen_t *addrlen,
    gboolean (*prolong)(gpointer data),
    gpointer prolong_data,
    time_t timeout)
{
    SELECT_ARG_TYPE readset;
    struct timeval tv;
    int nfound;

    if (sock < 0 || sock >= FD_SETSIZE) {
	g_debug("interruptible_accept: bad socket %d", sock);
	return EBADF;
    }

    memset(&readset, 0, sizeof(readset));

    while (1) {
	if (prolong && !prolong(prolong_data)) {
	    errno = 0;
	    return -1;
	}
	if (time(NULL) > timeout) {
	    errno = ETIMEDOUT;
	    return -1;
	}

	FD_ZERO(&readset);
	FD_SET(sock, &readset);

	/* try accepting for 1s */
	memset(&tv, 0, sizeof(tv));
	tv.tv_sec = 1;

	nfound = select(sock+1, &readset, NULL, NULL, &tv);
	if (nfound < 0) {
	    return -1;
	} else if (nfound == 0) {
	    continue;
	} else if (!FD_ISSET(sock, &readset)) {
	    g_debug("interruptible_accept: select malfunction");
	    errno = EBADF;
	    return -1;
	} else {
	    int rv = accept(sock, addr, addrlen);
	    if (rv < 0 && errno == EAGAIN)
		continue;
	    return rv;
	}
    }
}

/*
 * Writes out the entire iovec
 */
ssize_t
full_writev(
    int			fd,
    struct iovec *	iov,
    int			iovcnt)
{
    ssize_t delta, n, total;

    assert(iov != NULL);

    total = 0;
    while (iovcnt > 0) {
	/*
	 * Write the iovec
	 */
	n = writev(fd, iov, iovcnt);
	if (n < 0) {
	    if (errno != EINTR)
		return (-1);
	}
	else if (n == 0) {
	    errno = EIO;
	    return (-1);
	} else {
	    total += n;
	    /*
	     * Iterate through each iov.  Figure out what we still need
	     * to write out.
	     */
	    for (; n > 0; iovcnt--, iov++) {
		/* 'delta' is the bytes written from this iovec */
		delta = ((size_t)n < (size_t)iov->iov_len) ? n : (ssize_t)iov->iov_len;
		/* subtract from the total num bytes written */
		n -= delta;
		assert(n >= 0);
		/* subtract from this iovec */
		iov->iov_len -= delta;
		iov->iov_base = (char *)iov->iov_base + delta;
		/* if this iovec isn't empty, run the writev again */
		if (iov->iov_len > 0)
		    break;
	    }
	}
    }
    return (total);
}


/*
 * For backward compatibility we are trying for minimal quoting.  Unless ALWAYS
 * is true, we only quote a string if it contains whitespace or is misquoted...
 */

char *
quote_string_maybe(
    const char *str,
    gboolean always)
{
    char *  s;
    char *  ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = g_strdup("\"\"");
    } else {
	const char *r;
	for (r = str; *r; r++) {
	    if (*r == ':' || *r == '\'' || *r == '\\' || *r == '\"' ||
		*r <= ' ' || *r == 0x7F )
		always = 1;
	}
	if (!always) {
	    /*
	     * String does not need to be quoted since it contains
	     * neither whitespace, control or quote characters.
	     */
	    ret = g_strdup(str);
	} else {
	    /*
	     * Allocate maximum possible string length.
	     * (a string of all quotes plus room for leading ", trailing " and
	     *  NULL)
	     */
	    ret = s = g_malloc((strlen(str) * 2) + 2 + 1);
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
    }
    return (ret);
}


int
len_quote_string_maybe(
    const char *str,
    gboolean always)
{
    int   ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = 0;
    } else {
	const char *r;
	for (r = str; *r; r++) {
	    if (*r == ':' || *r == '\'' || *r == '\\' || *r == '\"' ||
		*r <= ' ' || *r == 0x7F )
		always = 1;
	}
	if (!always) {
	    /*
	     * String does not need to be quoted since it contains
	     * neither whitespace, control or quote characters.
	     */
	    ret = strlen(str);
	} else {
	    /*
	     * Allocate maximum possible string length.
	     * (a string of all quotes plus room for leading ", trailing " and
	     *  NULL)
	     */
	    ret = 1;
	        while (*str != '\0') {
                if (*str == '\t') {
                    ret++;
                    ret++;
		    str++;
		    continue;
	        } else if (*str == '\n') {
                    ret++;
                    ret++;
		    str++;
		    continue;
	        } else if (*str == '\r') {
                    ret++;
                    ret++;
		    str++;
		    continue;
	        } else if (*str == '\f') {
                    ret++;
                    ret++;
		    str++;
		    continue;
	        } else if (*str == '\\') {
                    ret++;
                    ret++;
		    str++;
		    continue;
	        }
                if (*str == '"')
		    ret++;
	        ret++;
                str++;
            }
	    ret++;
	}
    }
    return (ret);
}


char *
unquote_string(
    const char *str)
{
    char * ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = g_strdup("");
    } else {
	char * in;
	char * out;

	ret = in = out = g_strdup(str);
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
		} else if (*in >= '0' && *in <= '7') {
		    char c = 0;
		    int i = 0;

		    while (i < 3 && *in >= '0' && *in <= '7') {
			c = (c << 3) + *(in++) - '0';
			i++;
		    }
		    if (c)
			*(out++) = c;
		} else if (*in == '\0') {
		    /* trailing backslash -- ignore */
		    break;
		}
	    }
	    *(out++) = *(in++);
	}
        *out = '\0';
    }
    return (ret);
}

gchar **
split_quoted_strings(
    const gchar *string)
{
    char *local;
    char *start;
    char *p;
    char **result;
    GPtrArray *strs;
    int iq = 0;

    if (!string)
	return NULL;

    p = start = local = g_strdup(string);
    strs = g_ptr_array_new();

    while (*p) {
	if (!iq && *p == ' ') {
	    *p = '\0';
	    g_ptr_array_add(strs, unquote_string(start));
	    start = p+1;
	} else if (*p == '\\') {
	    /* next character is taken literally; if it's a multicharacter
	     * escape (e.g., \171), that doesn't bother us here */
	    p++;
	    if (!*p) break;
	} else if (*p == '\"') {
	    iq = ! iq;
	}

	p++;
    }
    if (start != string)
	g_ptr_array_add(strs, unquote_string(start));

    /* now convert strs into a strv, by stealing its references to the underlying
     * strings */
    result = g_new0(char *, strs->len + 1);
    memmove(result, strs->pdata, sizeof(char *) * strs->len);

    g_ptr_array_free(strs, TRUE); /* TRUE => free pdata, strings are not freed */
    g_free(local);

    return result;
}

gchar **
split_quoted_strings_for_amstatus(
    const gchar *string)
{
    char *local;
    char *start;
    char *p;
    char **result;
    GPtrArray *strs;
    int iq = 0;

    if (!string)
	return NULL;

    p = start = local = g_strdup(string);
    strs = g_ptr_array_new();

    while (*p) {
	if (!iq && (*p == ' ' || *p == ':')) {
	    *p = '\0';
	    if (start != p) {
		g_ptr_array_add(strs, unquote_string(start));
	    }
	    start = p+1;
	} else if (*p == '\\') {
	    /* next character is taken literally; if it's a multicharacter
	     * escape (e.g., \171), that doesn't bother us here */
	    p++;
	    if (!*p) break;
	} else if (*p == '\"') {
	    iq = ! iq;
	}

	p++;
    }
    if (start != string)
	g_ptr_array_add(strs, unquote_string(start));

    /* now convert strs into a strv, by stealing its references to the underlying
     * strings */
    result = g_new0(char *, strs->len + 1);
    memmove(result, strs->pdata, sizeof(char *) * strs->len);

    g_ptr_array_free(strs, TRUE); /* TRUE => free pdata, strings are not freed */
    g_free(local);

    return result;
}

char *
strquotedstr(char **saveptr)
{
    char *  tok = strtok_r(NULL, " ", saveptr);
    size_t	len;
    int         in_quote;
    int         in_backslash;
    char       *p, *t;

    if (!tok)
	return tok;
    len = strlen(tok);
    in_quote = 0;
    in_backslash = 0;
    p = tok;
    while (in_quote || in_backslash || *p != '\0') {
	if (*p == '\0') {
	    /* append a new token */
	    t = strtok_r(NULL, " ", saveptr);
	    if (!t)
		return NULL;
	    tok[len] = ' ';
	    len = strlen(tok);
	}
	if (!in_backslash) {
	    if (*p == '"')
		in_quote = !in_quote;
	    else if (*p == '\\') {
		in_backslash = 1;
	    }
	} else {
	   in_backslash = 0;
	}
	p++;
    }
    return tok;
}

char *
sanitize_string(
    const char *str)
{
    char * s;
    char * ret;

    if ((str == NULL) || (*str == '\0')) {
	ret = g_strdup("");
    } else {
	ret = g_strdup(str);
	for (s = ret; *s != '\0'; s++) {
	    if (iscntrl((int)*s))
		*s = '?';
	}
    }
    return (ret);
}

char *hexencode_string(const char *str)
{
    size_t orig_len, new_len, i;
    GString *s;
    gchar *ret;
    if (!str) {
        s = g_string_sized_new(0);
        goto cleanup;
    }
    new_len = orig_len = strlen(str);
    for (i = 0; i < orig_len; i++) {
        if (!g_ascii_isalnum(str[i])) {
            new_len += 2;
        }
    }
    s = g_string_sized_new(new_len);

    for (i = 0; i < orig_len; i++) {
        if (g_ascii_isalnum(str[i])) {
            g_string_append_c(s, str[i]);
        } else {
            g_string_append_printf(s, "%%%02hhx", str[i]);
        }
    }

cleanup:
    ret = s->str;
    g_string_free(s, FALSE);
    return ret;
}

char *hexdecode_string(const char *str, GError **err)
{
    size_t orig_len, new_len, i;
    GString *s;
    gchar *ret;
    if (!str) {
        s = g_string_sized_new(0);
        goto cleanup;
    }
    new_len = orig_len = strlen(str);
    for (i = 0; i < orig_len; i++) {
        if (str[i] == '%') {
            new_len -= 2;
        }
    }
    s = g_string_sized_new(new_len);

    for (i = 0; (orig_len > 2) && (i < orig_len-2); i++) {
        if (str[i] == '%') {
            gchar tmp = 0;
            size_t j;
            for (j = 1; j < 3; j++) {
                tmp <<= 4;
                if (str[i+j] >= '0' && str[i+j] <= '9') {
                    tmp += str[i+j] - '0';
                } else if (str[i+j] >= 'a' && str[i+j] <= 'f') {
                    tmp += str[i+j] - 'a' + 10;
                } else if (str[i+j] >= 'A' && str[i+j] <= 'F') {
                    tmp += str[i+j] - 'A' + 10;
                } else {
                    /* error */
                    g_set_error(err, am_util_error_quark(), AM_UTIL_ERROR_HEXDECODEINVAL,
                        "Illegal character (non-hex) 0x%02hhx at offset %zd", str[i+j], i+j);
                    g_string_truncate(s, 0);
                    goto cleanup;
                }
            }
            if (!tmp) {
                g_set_error(err, am_util_error_quark(), AM_UTIL_ERROR_HEXDECODEINVAL,
                    "Encoded NULL at starting offset %zd", i);
                g_string_truncate(s, 0);
                goto cleanup;
            }
            g_string_append_c(s, tmp);
            i += 2;
        } else {
            g_string_append_c(s, str[i]);
        }
    }
    for ( /*nothing*/; i < orig_len; i++) {
        if (str[i] == '%') {
            g_set_error(err, am_util_error_quark(), AM_UTIL_ERROR_HEXDECODEINVAL,
                "'%%' found at offset %zd, but fewer than two characters follow it (%zd)", i, orig_len-i-1);
            g_string_truncate(s, 0);
            goto cleanup;
        } else {
            g_string_append_c(s, str[i]);
        }
    }

cleanup:
    ret = s->str;
    g_string_free(s, FALSE);
    return ret;
}

/* Helper for parse_braced_component; this will turn a single element array
 * matching /^\d+\.\.\d+$/ into a sequence of numbered array elements. */
static GPtrArray *
expand_braced_sequence(GPtrArray *arr)
{
    char *elt, *p;
    char *l, *r;
    int ldigits, rdigits, ndigits;
    guint64 start, end;
    gboolean leading_zero;

    /* check whether the element matches the pattern */
    /* expand last element of the array only */
    elt = g_ptr_array_index(arr, arr->len-1);
    ldigits = 0;
    for (l = p = elt; *p && g_ascii_isdigit(*p); p++)
	ldigits++;
    if (ldigits == 0)
	return arr;
    if (*(p++) != '.')
	return arr;
    if (*(p++) != '.')
	return arr;
    rdigits = 0;
    for (r = p; *p && g_ascii_isdigit(*p); p++)
	rdigits++;
    if (rdigits == 0)
	return arr;
    if (*p)
	return arr;

    /* we have a match, so extract start and end */
    start = g_ascii_strtoull(l, NULL, 10);
    end = g_ascii_strtoull(r, NULL, 10);
    leading_zero = *l == '0';
    ndigits = MAX(ldigits, rdigits);
    if (start > end)
	return arr;

    /* sanity check.. */
    if (end - start > 100000)
	return arr;

    /* remove last from the array */
    g_ptr_array_remove_index(arr, arr->len - 1);

    /* Add new elements */
    while (start <= end) {
	if (leading_zero) {
	    g_ptr_array_add(arr, g_strdup_printf("%0*ju",
			ndigits, (uintmax_t)start));
	} else {
	    g_ptr_array_add(arr, g_strdup_printf("%ju", (uintmax_t)start));
	}
	start++;
    }

    g_free(elt);
    return arr;
}

/* Helper for expand_braced_alternates; returns a list of un-escaped strings
 * for the first "component" of str, where a component is a plain string or a
 * brace-enclosed set of alternatives.  str is pointing to the first character
 * of the next component on return. */
static GPtrArray *
parse_braced_component(char **str)
{
    GPtrArray *result = g_ptr_array_new();

    if (**str == '{') {
	char *p = (*str)+1;
	char *local = g_malloc(strlen(*str)+1);
	char *current = local;
	char *c = current;

	while (1) {
	    if (*p == '\0' || *p == '{') {
		/* unterminated { .. } or extra '{' */
		amfree(local);
		g_ptr_array_free(result, TRUE);
		return NULL;
	    }

	    if (*p == '}' || *p == ',') {
		*c = '\0';
		g_ptr_array_add(result, g_strdup(current));
		result = expand_braced_sequence(result);
		current = ++c;

		if (*p == '}')
		    break;
		else
		    p++;
	    }

	    if (*p == '\\') {
		if (*(p+1) == '{' || *(p+1) == '}' || *(p+1) == '\\' || *(p+1) == ',')
		    p++;
	    }
	    *(c++) = *(p++);
	}

	amfree(local);

	if (*p)
	    *str = p+1;
	else
	    *str = p;
    } else {
	/* no braces -- just un-escape a plain string */
	char *local = g_malloc(strlen(*str)+1);
	char *r = local;
	char *p = *str;

	while (*p && *p != '{') {
	    if (*p == '\\') {
		if (*(p+1) == '{' || *(p+1) == '}' || *(p+1) == '\\' || *(p+1) == ',')
		    p++;
	    }
	    *(r++) = *(p++);
	}
	*r = '\0';
	g_ptr_array_add(result, local);
	*str = p;
    }

    return result;
}

GPtrArray *
expand_braced_alternates(
    char * source)
{
    GPtrArray *rval = g_ptr_array_new();
    gpointer *pdata;

    g_ptr_array_add(rval, g_strdup(""));

    while (*source) {
	GPtrArray *new_components;
	GPtrArray *new_rval;
	guint i, j;

	new_components = parse_braced_component(&source);
	if (!new_components) {
	    /* parse error */
	    for (i = 0, pdata = rval->pdata; i < rval->len; i++)
		g_free(*pdata++);
	    g_ptr_array_free(rval, TRUE);
	    return NULL;
	}

	new_rval = g_ptr_array_new();

	/* do a cartesian join of rval and new_components */
	for (i = 0; i < rval->len; i++) {
	    for (j = 0; j < new_components->len; j++) {
		g_ptr_array_add(new_rval, g_strconcat(
		    g_ptr_array_index(rval, i),
		    g_ptr_array_index(new_components, j),
		    NULL));
	    }
	}

	for (i = 0, pdata = rval->pdata; i < rval->len; i++)
	    g_free(*pdata++);
	g_ptr_array_free(rval, TRUE);
	for (i = 0, pdata = new_components->pdata; i < new_components->len; i++)
	    g_free(*pdata++);
	g_ptr_array_free(new_components, TRUE);
	rval = new_rval;
    }

    return rval;
}

char *
collapse_braced_alternates(
    GPtrArray *source)
{
    GString *result = NULL;
    guint i;

    result = g_string_new("{");

    for (i = 0; i < source->len; i ++) {
	const char *str = g_ptr_array_index(source, i);
	char *qstr = NULL;

	if (strchr(str, ',') || strchr(str, '\\') ||
	    strchr(str, '{') || strchr(str, '}')) {
	    const char *s;
	    char *d;

	    s = str;
	    qstr = d = g_malloc(strlen(str)*2+1);
	    while (*s) {
		if (*s == ',' || *s == '\\' || *s == '{' || *s == '}')
		    *(d++) = '\\';
		*(d++) = *(s++);
	    }
	    *(d++) = '\0';
	}
	g_string_append_printf(result, "%s%s", qstr? qstr : str,
		(i < source->len-1)? "," : "");
	if (qstr)
	    g_free(qstr);
    }

    g_string_append(result, "}");
    return g_string_free(result, FALSE);
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
	*errmsg = g_strdup_printf(_("Can't open file '%s' for reading: %s"),
			    quoted, strerror(save_errno));
	amfree(quoted);
	return -1;
    }

    if ((outfd = open(dst, O_WRONLY|O_CREAT, 0600)) == -1) {
	save_errno = errno;
	quoted = quote_string(dst);
	*errmsg = g_strdup_printf(_("Can't open file '%s' for writing: %s"),
			    quoted, strerror(save_errno));
	amfree(quoted);
	close(infd);
	return -1;
    }

    while ((nb=read(infd, &buf, sizeof(buf))) > 0) {
	if (full_write(outfd,&buf,nb) < (size_t)nb) {
	    save_errno = errno;
	    quoted = quote_string(dst);
	    *errmsg = g_strdup_printf(_("Error writing to '%s': %s"),
				quoted, strerror(save_errno));
	    amfree(quoted);
	    close(infd);
	    close(outfd);
	    return -1;
	}
    }

    if (errno != 0) {
	save_errno = errno;
	quoted = quote_string(src);
	*errmsg = g_strdup_printf(_("Error reading from '%s': %s"),
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

#ifndef HAVE_LIBREADLINE
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
    return pgets(stdin);
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

#if (GLIB_MAJOR_VERSION > 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION >= 31))
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wmissing-field-initializers"
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
#if (GLIB_MAJOR_VERSION > 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION >= 31))
# pragma GCC diagnostic pop
#endif

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
#ifdef WORKING_IPV6
    /* get any kind of addresss */
    hints.ai_family = AF_UNSPEC;
#else
    /* even if getaddrinfo supports IPv6, don't let it return
     * such an address */
    hints.ai_family = AF_INET;
#endif
    hints.ai_flags = flags;
    hints.ai_socktype = socktype;
    result = getaddrinfo(hostname, NULL, &hints, &myres);
    if (result != 0) {
	return result;
    }

    if (canonname && myres && myres->ai_canonname) {
	*canonname = g_strdup(myres->ai_canonname);
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
	    return g_strdup_printf(_("%s exited normally"), subject);
	else
	    return g_strdup_printf(_("%s exited with status %d"), subject, exitstatus);
    }

    if (WIFSIGNALED(status)) {
	int signal = WTERMSIG(status);
#ifdef WCOREDUMP
	if (WCOREDUMP(status))
	    return g_strdup_printf(_("%s exited after receiving signal %d (core dumped)"),
		subject, signal);
	else
#endif
	    return g_strdup_printf(_("%s exited after receiving signal %d"),
		subject, signal);
    }

    if (WIFSTOPPED(status)) {
	int signal = WSTOPSIG(status);
	return g_strdup_printf(_("%s stopped temporarily after receiving signal %d"),
	    subject, signal);
    }

#ifdef WIFCONTINUED
    if (WIFCONTINUED(status)) {
	return g_strdup_printf(_("%s was resumed"), subject);
    }
#endif

    return g_strdup_printf(_("%s exited in unknown circumstances"), subject);
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
    uname_me = g_strdup(pw->pw_name);

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
	    amfree(uname_me);
	    return;

	case RUNNING_AS_ROOT:
	    uid_target = 0;
	    uname_target = "root";
	    break;

	case RUNNING_AS_DUMPUSER_PREFERRED:
	    dumpuser = getconf_str(CNF_DUMPUSER);
	    if ((pw = getpwnam(dumpuser)) != NULL &&
                    uid_me != pw->pw_uid) {
		if ((pw = getpwnam(CLIENT_LOGIN)) != NULL &&
		    uid_me == pw->pw_uid) {
		    /* uid == CLIENT_LOGIN: not ideal, but OK */
		    dbprintf(_("NOTE: running as '%s', which is the client"
			       " user, not the dumpuser ('%s'); forging"
			       " on anyway\n"),
			     CLIENT_LOGIN, dumpuser);
		    uid_target = uid_me; /* force success below */
		   break;
		}
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
	error("must be executed as the \"%s\" user instead of the \"%s\" user", uname_target, uname_me);
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
    static gboolean first_call = TRUE;
    static uid_t unpriv = 1;

    if (first_call) {
	/* save the original real userid (that of our invoker) */
	unpriv = getuid();

	/* and set all of our userids (including, importantly, the saved
	 * userid) to 0 */
	setuid(0);

	/* don't need to do this next time */
	first_call = FALSE;
    }

    if (need_root == 1) {
	if (geteuid() == 0) return 1; /* already done */

        if (seteuid(0) == -1) return 0;
        /* (we don't switch the group back) */
    } else if (need_root == -1) {
	/* make sure the euid is 0 so that we can set the uid */
	if (geteuid() != 0) {
	    if (seteuid(0) == -1) return 0;
	}

	/* now set the uid to the unprivileged userid */
	if (setuid(unpriv) == -1) return 0;
    } else {
	if (geteuid() != 0) return 1; /* already done */

	/* set the *effective* userid only */
        if (seteuid(unpriv) == -1) return 0;
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
    /* first, set the effective userid to 0 */
    if (seteuid(0) == -1) return 0;

    /* then, set all of the userids to 0 */
    if (setuid(0) == -1) return 0;
#endif
    return 1;
}

char *
base64_decode_alloc_string(
    char *in)
{
    char   *out;
    size_t  in_len = strlen(in);
    size_t  out_len = 3 * (in_len / 4) + 3;

    out = malloc(out_len);
    if (!base64_decode(in, in_len, out, &out_len)) {
	amfree(out);
	return NULL;
    }
    out[out_len] = '\0';

    return out;
}


/* A GHFunc (callback for g_hash_table_foreach),
 * Store a property and it's value in an ARGV.
 *
 * @param key_p: (char *) property name.
 * @param value_p: (GSList *) property values list.
 * @param user_data_p: (char ***) pointer to ARGV.
 */
static void
proplist_add_to_argv(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p)
{
    char         *property_s = key_p;
    property_t   *value_s = value_p;
    GPtrArray    *argv_ptr = user_data_p;
    GSList       *value;
    char         *q, *w, *qprop;

    q = g_strdup(property_s);
    /* convert to lower case */
    for (w=q; *w != '\0'; w++) {
	*w = tolower(*w);
	if (*w == '_')
	    *w = '-';
    }
    qprop = g_strconcat("--", q, NULL);
    amfree(q);
    for(value=value_s->values; value != NULL; value = value->next) {
	g_ptr_array_add(argv_ptr, g_strdup(qprop));
	g_ptr_array_add(argv_ptr, g_strdup((char *)value->data));
    }
    amfree(qprop);
}

void
property_add_to_argv(
    GPtrArray  *argv_ptr,
    GHashTable *proplist)
{
    g_hash_table_foreach(proplist, &proplist_add_to_argv, argv_ptr);
}


/*
 * Process parameters
 */

static char *pname = NULL;
static char *ptype = NULL;
static pcontext_t pcontext = CONTEXT_DEFAULT;
static char *pcomponent = NULL;
static char *pmodule = NULL;
static GQueue *component_stack;
static GQueue *module_stack;

void
set_pname(char *p)
{
    g_free(pname);
    pname = g_strdup(p);
}

char *
get_pname(void)
{
    if (!pname) pname = g_strdup("unknown");
    return pname;
}

void
set_ptype(char *p)
{
    g_free(ptype);
    ptype = g_strdup(p);
}

char *
get_ptype(void)
{
    if (!ptype) ptype = g_strdup("unknown");
    return ptype;
}

void
set_pcontext(pcontext_t pc)
{
    pcontext = pc;
}

pcontext_t
get_pcontext(void)
{
    return pcontext;
}

void
set_pcomponent(char *component)
{
    g_free(pcomponent);
    pcomponent = g_strdup(component);
}

char *
get_pcomponent(void)
{
    if (!pcomponent) pcomponent = g_strdup("amanda");
    return pcomponent;
}

void
set_pmodule(char *module)
{
    g_free(pmodule);
    pmodule = g_strdup(module);
}

char *
get_pmodule(void)
{
    if (!pmodule) pmodule = g_strdup(get_pcomponent());
    return pmodule;
}

void
push_component_module(
    char *component,
    char *module)
{
    if (component_stack == NULL)
	component_stack = g_queue_new();
    g_queue_push_head(component_stack, pcomponent);
    pcomponent = g_strdup(component);

    if (module_stack == NULL)
	module_stack = g_queue_new();
    g_queue_push_head(module_stack, pmodule);
    pmodule = g_strdup(module);
}

void
pop_component_module(void)
{
    pcomponent = g_queue_pop_head(component_stack);
    pmodule = g_queue_pop_head(module_stack);
}


#ifdef __OpenBSD__
void
openbsd_fd_inform(void)
{
    int i;
    for (i = DATA_FD_OFFSET; i < DATA_FD_OFFSET + DATA_FD_COUNT*2; i++) {
	/* a simple fcntl() will cause the library to "look" at this file
	 * descriptor, which is good enough */
	(void)fcntl(i, F_GETFL);
    }
}
#endif

void
debug_executing(
    GPtrArray *argv_ptr)
{
    char *cmdline;
    GString *strbuf;
    gsize i, len = argv_ptr->len - 1;

    /*
     * This is ugly, but we have no choice: we cannot count on the caller to
     * supply a NULL-terminated GPtrArray (he'll have other problems than this
     * function, anyway), so we have to display all but the last argument - and
     * therefore need to peek into the array directly to knows its length.
     *
     * To make things even funnier, we have to quote all arguments _except_ the
     * command name (ie, the first one).
     */

    strbuf = g_string_new((char *)g_ptr_array_index(argv_ptr, 0));

    for (i = 1; i < len; i++) {
        cmdline = g_shell_quote((char *)g_ptr_array_index(argv_ptr, i));
        g_string_append_printf(strbuf, " %s", cmdline);
        g_free(cmdline);
    }

    cmdline = g_string_free(strbuf, FALSE);

    g_debug("Executing: %s", cmdline);
    g_free(cmdline);
}

char *
get_first_line(
    GPtrArray *argv_ptr)
{
    char *output_string = NULL;
    int   inpipe[2], outpipe[2], errpipe[2];
    int   pid;
    FILE *out, *err;

    assert(argv_ptr != NULL);
    assert(argv_ptr->pdata != NULL);
    assert(argv_ptr->len >= 1);

    if (pipe(inpipe) == -1) {
	error(_("error [open pipe: %s]"), strerror(errno));
	/*NOTREACHED*/
    }
    if (pipe(outpipe) == -1) {
	error(_("error [open pipe: %s]"), strerror(errno));
	/*NOTREACHED*/
    }
    if (pipe(errpipe) == -1) {
	error(_("error [open pipe: %s]"), strerror(errno));
	/*NOTREACHED*/
    }

    fflush(stdout);
    switch(pid = fork()) {
    case -1:
	error(_("error [fork: %s]"), strerror(errno));
	/*NOTREACHED*/

    default:	/* parent process */
	aclose(inpipe[0]);
	aclose(outpipe[1]);
	aclose(errpipe[1]);
	break;

    case 0: /* child process */
	aclose(inpipe[1]);
	aclose(outpipe[0]);
	aclose(errpipe[0]);

	dup2(inpipe[0], 0);
	dup2(outpipe[1], 1);
	dup2(errpipe[1], 2);

	debug_executing(argv_ptr);
	g_fprintf(stdout, "unknown\n");
	execv((char *)*argv_ptr->pdata, (char **)argv_ptr->pdata);
	error(_("error [exec %s: %s]"), (char *)*argv_ptr->pdata, strerror(errno));
    }

    aclose(inpipe[1]);

    out = fdopen(outpipe[0],"r");
    err = fdopen(errpipe[0],"r");

    if (out) {
	output_string = pgets(out);
	fclose(out);
    }

    if (err) {
	if (!output_string)
	    output_string = pgets(err);
	fclose(err);
    }

    waitpid(pid, NULL, 0);

    return output_string;
}

gboolean
make_amanda_tmpdir(void)
{
    struct stat stat_buf;

    if (mkdir(AMANDA_TMPDIR, 0700) != 0) {
	if (errno != EEXIST) {
	    g_debug("Error mkdir of AMANDA_TMPDIR (%s): %s", AMANDA_TMPDIR, strerror(errno));
	    return FALSE;
	}
    } else {
	if (chown(AMANDA_TMPDIR, (int)get_client_uid(), (int)get_client_gid()) < 0) {
	    g_debug("Error chown of AMANDA_TMPDIR (%s): %s", AMANDA_TMPDIR, strerror(errno));
	    return FALSE;
	}
    }

    if (stat(AMANDA_TMPDIR, &stat_buf) != 0) {
	g_debug("Error doing a stat of AMANDA_TMPDIR (%s): %s", AMANDA_TMPDIR, strerror(errno));
        return FALSE;
    }

#ifdef CHECK_USERID
    if (stat_buf.st_uid != get_client_uid()) {
	g_debug("Error: Owner of AMANDA_TMPDIR (%s) is not %s\n", AMANDA_TMPDIR, CLIENT_LOGIN);
	return FALSE;
    }
#endif

    if (stat_buf.st_mode & S_IWOTH) {
	g_debug("Error: AMANDA_TMPDIR (%s) must not be writable by other\n", AMANDA_TMPDIR);
	return FALSE;
    }

    return TRUE;
}

#define POLY 0x82F63B78
#if defined __x86_64__ || defined __i386__ || defined __i486__ || defined __i586__ || defined __i686__
static int get_sse42(void)
{
    uint32_t op, eax, ebx, ecx, edx;
    op = 1;
#ifdef __i386__
    __asm__ volatile(
		"pushl %%ebx;\n\t"
		"cpuid;\n\t"
		"movl %%ebx, %1;\n\t"
		"popl %%ebx;\n\t"
                : "=a" (eax), "=r" (ebx), "=c" (ecx), "=d" (edx)
		: "a" (op)
		: "cc"
    );
#else
    __asm__ volatile(
		"cpuid;\n\t"
                : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
		: "a" (op)
		: "cc"
    );
#endif
    return (ecx >> 20) & 1;
}
#else
static int get_sse42(void)
{
    return 0;
}
#endif

static uint32_t crc_table[16][256];
static gboolean crc_initialized = FALSE;
gboolean have_sse42 = FALSE;
void (* crc32_function)(uint8_t *buf, size_t len, crc_t *crc);

  #include "amcrc32chw.h"

/* Run this function previously */
void
make_crc_table(void)
{
    int i;
    int j;
    int slice;

    if (!crc_initialized) {
	if (compiled_with_sse4_2) {
	    have_sse42 = get_sse42();
	}
	if (have_sse42) {
	    crc32c_init_hw();
	    crc32_function = &crc32c_add_hw;
	} else {
            crc32_function = &crc32_add_16bytes;
	}

        for (i = 0; i < 256; i++) {
            uint32_t c = i;
            for (j = 0; j < 8; j++) {
                c = (c & 1) ? (POLY ^ (c >> 1)) : (c >> 1);
            }
            crc_table[0][i] = c;
        }
	for (i = 0; i < 256; i++) {
	    for (slice = 1; slice < 16; slice++) {
		crc_table[slice][i] = (crc_table[slice - 1][i] >> 8) ^ crc_table[0][crc_table[slice - 1][i] & 0xFF];
	    }
	}
        crc_initialized = TRUE;
    }
}

void
crc32_init(
    crc_t *crc)
{
    make_crc_table();
    crc->crc = 0xFFFFFFFF;
    crc->size = 0;
}

#if defined __GNUC__ || defined __clang__
  #define PREFETCH(location) __builtin_prefetch(location)
#else
  #define PREFETCH(location)
#endif

void
crc32_add_1byte(
    uint8_t *buf,
    size_t len,
    crc_t *crc)
{
    crc->size += len;
    while (len-- > 0) {
	crc->crc = crc_table[0][(crc->crc ^ *buf++) & 0xFF] ^ (crc->crc >> 8);
     }
}

#ifdef WORDS_BIGENDIAN
static inline uint32_t swap(uint32_t data);
static inline uint32_t
swap(
    uint32_t data)
{
#if (defined(__GNUC__) && GCC_VERSION >= 40300) || defined __clang__
    return __builtin_bswap32(data);
#else
    return (data >> 24) |
          ((data >> 8) & 0x0000FF00) |
          ((data << 8) & 0x00FF0000) |
           (data << 24);
#endif
}
#endif

void
crc32_add_16bytes(
     uint8_t *buf,
     size_t len,
     crc_t *crc)
{
    uint32_t *buf32 = (uint32_t *)buf;
    size_t i;
    crc->size += len;

    while (len >= 256) {
	PREFETCH(((const char*) buf) + 256);
	for (i = 0; i < 4; i++) {
#ifdef WORDS_BIGENDIAN
	    uint32_t one   = *buf32++ ^ swap(crc->crc);
	    uint32_t two   = *buf32++;
	    uint32_t three = *buf32++;
	    uint32_t four  = *buf32++;
	    crc->crc  = crc_table[ 0][ four         & 0xFF] ^
			crc_table[ 1][(four  >>  8) & 0xFF] ^
			crc_table[ 2][(four  >> 16) & 0xFF] ^
			crc_table[ 3][(four  >> 24) & 0xFF] ^
			crc_table[ 4][ three        & 0xFF] ^
			crc_table[ 5][(three >>  8) & 0xFF] ^
			crc_table[ 6][(three >> 16) & 0xFF] ^
			crc_table[ 7][(three >> 24) & 0xFF] ^
			crc_table[ 8][ two          & 0xFF] ^
			crc_table[ 9][(two   >>  8) & 0xFF] ^
			crc_table[10][(two   >> 16) & 0xFF] ^
			crc_table[11][(two   >> 24) & 0xFF] ^
			crc_table[12][ one          & 0xFF] ^
			crc_table[13][(one   >>  8) & 0xFF] ^
			crc_table[14][(one   >> 16) & 0xFF] ^
			crc_table[15][(one   >> 24) & 0xFF];
#else
	    uint32_t one   = *buf32++ ^ crc->crc;
	    uint32_t two   = *buf32++;
	    uint32_t three = *buf32++;
	    uint32_t four  = *buf32++;
	    crc->crc  = crc_table[ 0][(four  >> 24) & 0xFF] ^
			crc_table[ 1][(four  >> 16) & 0xFF] ^
			crc_table[ 2][(four  >>  8) & 0xFF] ^
			crc_table[ 3][ four         & 0xFF] ^
			crc_table[ 4][(three >> 24) & 0xFF] ^
			crc_table[ 5][(three >> 16) & 0xFF] ^
			crc_table[ 6][(three >>  8) & 0xFF] ^
			crc_table[ 7][ three        & 0xFF] ^
			crc_table[ 8][(two   >> 24) & 0xFF] ^
			crc_table[ 9][(two   >> 16) & 0xFF] ^
			crc_table[10][(two   >>  8) & 0xFF] ^
			crc_table[11][ two          & 0xFF] ^
			crc_table[12][(one   >> 24) & 0xFF] ^
			crc_table[13][(one   >> 16) & 0xFF] ^
			crc_table[14][(one   >>  8) & 0xFF] ^
			crc_table[15][ one          & 0xFF];
#endif
	}
	len -= 4*16;
    }
    buf = (uint8_t *)buf32;

    while (len-- > 0) {
	       crc->crc = crc_table[0][(crc->crc ^ *buf++) & 0xFF] ^ (crc->crc >> 8);
    }
}

void
crc32_add(
    uint8_t *buf,
    size_t len,
    crc_t *crc)
{
    crc32_function(buf, len, crc);
    return;
 }

uint32_t
crc32_finish(
    crc_t *crc)
{
    return crc->crc ^ 0xFFFFFFFF;
}

void
parse_crc(
    char *s,
    crc_t *crc)
{
    unsigned int c;
    long long size;

    if (sscanf(s, "%x:%lld", &c, &size) == 2) {
        crc->crc = c;
        crc->size = size;
    }
}

gint64
get_fsusage(
   char *dir)
{
    gint64 avail = 0;
    struct fs_usage fsusage;

    if (get_fs_usage(dir, NULL, &fsusage) == -1) {
	return 0;
    }

    if (fsusage.fsu_bavail_top_bit_set)
	avail = 0;
    else {
        avail = fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;
    }

    return avail;
}

void
get_platform_and_distro(
    char **r_platform,
    char **r_distro)
{
    char *uname = NULL;
    char *distro = NULL;
    char *platform = NULL;
    char *productName = NULL;
    char *productVersion = NULL;
    char  line[1025];
    GPtrArray *argv_ptr;
    FILE *release;
    struct stat stat_buf;

    if (!stat("/usr/bin/lsb_release", &stat_buf)) {
	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/lsb_release");
	g_ptr_array_add(argv_ptr, "--id");
	g_ptr_array_add(argv_ptr, "-s");
	g_ptr_array_add(argv_ptr, NULL);
	distro = get_first_line(argv_ptr);
	if (distro && distro[0] == '"') {
	    char *p= g_strdup(distro+1);
	    p[strlen(p)-1] = '\0';
	    g_free(distro);
	    distro = p;
	}
	g_ptr_array_free(argv_ptr, TRUE);

	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/lsb_release");
	g_ptr_array_add(argv_ptr, "--description");
	g_ptr_array_add(argv_ptr, "-s");
	g_ptr_array_add(argv_ptr, NULL);
	platform = get_first_line(argv_ptr);
	if (platform && platform[0] == '"') {
	    char *p= g_strdup(platform+1);
	    p[strlen(p)-1] = '\0';
	    g_free(platform);
	    platform = p;
	}
	g_ptr_array_free(argv_ptr, TRUE);
	goto return_platorm;
    }
    release = fopen("/etc/redhat-release", "r");
    if (release) {
	char *result;
	distro = g_strdup("RPM");
	result = fgets(line, 1024, release);
	if (result) {
	    platform = g_strdup(line);
	}
	fclose(release);
	goto return_platorm;
    }

    release = fopen("/etc/lsb-release", "r");
    if (release) {
	distro = g_strdup("Ubuntu");
	while (fgets(line, 1024, release)) {
	    if (strstr(line, "DISTRIB_ID")) {
		char *p = strchr(line, '=');
		if (p) {
		    g_free(distro);
		    distro = g_strdup(p+1);
		}
	    }
	    if (strstr(line, "DESCRIPTION")) {
		char *p = strchr(line, '=');
		if (p) {
		    g_free(platform);
		    platform = g_strdup(p+1);
		}
	    }
	}
	fclose(release);
	goto return_platorm;
    }

    release = fopen("/etc/debian_version", "r");
    if (release) {
	char *result;
	distro = g_strdup("Debian");
	result = fgets(line, 1024, release);
	if (result) {
	    platform = g_strdup(line);
	}
	fclose(release);
	goto return_platorm;
    }

    argv_ptr = g_ptr_array_new();
    g_ptr_array_add(argv_ptr, UNAME_PATH);
    g_ptr_array_add(argv_ptr, "-s");
    g_ptr_array_add(argv_ptr, NULL);
    uname = get_first_line(argv_ptr);
    g_ptr_array_free(argv_ptr, TRUE);
    if (uname) {
	if (strncmp(uname, "SunOS", 5) == 0) {
	    FILE *release = fopen("/etc/release", "r");
	    distro = g_strdup("Solaris");
	    g_free(uname);
	    if (release) {
		char *result;
		result = fgets(line, 1024, release);
		if (result) {
		   platform = g_strdup(line);
		}
		fclose(release);
		goto return_platorm;
	    }
	} else if (strlen(uname) >= 3 &&
		   g_strcasecmp(uname+strlen(uname)-3, "bsd") == 0) {
	    distro = uname;
	    argv_ptr = g_ptr_array_new();
	    g_ptr_array_add(argv_ptr, UNAME_PATH);
	    g_ptr_array_add(argv_ptr, "-r");
	    g_ptr_array_add(argv_ptr, NULL);
	    platform = get_first_line(argv_ptr);
	    g_ptr_array_free(argv_ptr, TRUE);
	} else {
	    g_free(uname);
	}
    }
    if (!stat("/usr/bin/sw_vers", &stat_buf)) {
	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/sw_vers");
	g_ptr_array_add(argv_ptr, "-productName");
	g_ptr_array_add(argv_ptr, NULL);
	productName = get_first_line(argv_ptr);
	g_ptr_array_free(argv_ptr, TRUE);
	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/sw_vers");
	g_ptr_array_add(argv_ptr, "-productVersion");
	g_ptr_array_add(argv_ptr, NULL);
	productVersion = get_first_line(argv_ptr);
	g_ptr_array_free(argv_ptr, TRUE);
	if (productName && productVersion &&
	    !g_str_equal(productName, "unknown") &&
	    !g_str_equal( productVersion, "unknown")) {
	    distro = g_strdup("mac");
	    platform = g_strdup_printf("%s %s", productVersion, productVersion);
	    goto return_platorm;
	}
    }

return_platorm:
    if (!distro) {
	distro = g_strdup("Unknown");
    }
    if (!platform) {
	platform = g_strdup("Unknown");
    }
    if (platform[strlen(platform) -1] == '\n') {
	platform[strlen(platform) -1] = '\0';
    }

    if (r_platform) {
	*r_platform = platform;
	platform = NULL;
    } else {
	amfree(platform);
    }
    if (r_distro) {
	*r_distro = distro;
	distro = NULL;
    } else {
	amfree(distro);
    }
    amfree(productName);
    amfree(productVersion);
}

char *
get_platform(void)
{
    char *platform;
    char *distro;

    get_platform_and_distro(&platform, &distro);
    amfree(distro);
    return platform;
}

char *
get_distro(void)
{
    char *platform;
    char *distro;

    get_platform_and_distro(&platform, &distro);
    amfree(platform);
    return distro;
}
