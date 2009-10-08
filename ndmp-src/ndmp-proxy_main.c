/*
 * Copyright (c) 1998,1999,2000
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
 *
 */


#define GLOBAL
#include "ndmjob.h"

static void
proxy_usage(void) {
    g_fprintf(stderr, "USAGE: ndmp-proxy CONFIG PORT\n");
    exit(1);
}

static void
ndma_proxy_session (struct ndm_session *sess, int proxy_port)
{
	int			conn_sock, len, rc;
	struct sockaddr		sa;
	int			proxy_sock;
	int			fd;

	sess->proxy_starting = TRUE;
	sess->proxy_connections = 0;

	proxy_sock = socket (AF_INET, SOCK_STREAM, 0);
	if (proxy_sock < 0) {
		fprintf(stdout, "opening socket: %s\n", strerror(errno));
		exit(1);
	}

	ndmalogf (sess, 0, 2, "set sess->protocol_listen");

	ndmos_condition_listen_socket (sess, proxy_sock);

	NDMOS_MACRO_SET_SOCKADDR(&sa, 0, proxy_port);

	if (bind (proxy_sock, &sa, sizeof sa) < 0) {
		int err = errno;
		ndmalogf (sess, 0, 2, "Can't bind the socket(%d): %s\n", proxy_port, strerror(err));
		if (err == EADDRINUSE) {
		    fprintf(stdout, "INUSE\n");
		    fflush(stdout);
		    exit(0);
		} else {
		    fprintf(stdout, "while binding tcp port %d: %s\n", proxy_port, strerror(err));
		    fflush(stdout);
		    exit(1);
		}
	}

	if (listen (proxy_sock, 5) < 0) {
		fprintf(stdout, "listening on socket: %s\n", strerror(errno));
		exit(1);
	}

	/* set up to listen on this new socket */
	ndmchan_initialize(&sess->listen_chan, "proxy-listen");
	ndmchan_start_listen(&sess->listen_chan, proxy_sock);

	/* tell our invoker that we are OK */
	if (full_write(1, "OK\n", 3) != 3) {
		fprintf(stderr, "ndmp-proxy writing to stdout: %s\n", strerror(errno));
		exit(1);
	}

	/* send an EOF on stdout */
	close(1);

	/* open /dev/null on fds 0 and 1 */
	fd = open("/dev/null", O_RDONLY);
	if (fd < 0) {
	    fprintf(stderr, "cannot open /dev/null\n");
	    /* ignore the error */
	} else if (fd != 1) {
	    dup2(fd, 1);
	    close(fd);
	}

	fd = open("/dev/null", O_WRONLY);
	if (fd < 0) {
	    fprintf(stderr, "cannot open /dev/null\n");
	    /* ignore the error */
	} else if (fd != 0) {
	    dup2(fd, 0);
	    close(fd);
	}
}

int
main (int ac, char *av[])
{
	int rc;
	int port;

	NDMOS_MACRO_ZEROFILL(&the_session);
	d_debug = -1;

	/* ready the_param early so logging works during process_args() */
	NDMOS_MACRO_ZEROFILL (&the_param);
	the_param.log.deliver = ndmjob_log_deliver;
	the_param.log_level = d_debug;
	the_param.log_tag = "SESS";
	the_param.config_file_name = g_strdup_printf("%s/ndmjob.conf", amdatadir);

#ifndef NDMOS_OPTION_NO_CONTROL_AGENT
	b_bsize = 20;
	index_fp = stderr;
	o_tape_addr = -1;
	o_from_addr = -1;
	o_to_addr = -1;
#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */
	log_fp = stderr;

	if (ac != 3)
	    proxy_usage();
	/* (config arg is ignored for now) */
	port = atoi(av[2]);

	if (!port)
	    proxy_usage();

	the_session.param = the_param;

	ndma_proxy_session (&the_session, port);

	/* run until all open connections have closed; note that
	 * ndma_dispatch_proxy in ndma_comm_proxy.c will be called in each
	 * quantum */
	while (the_session.proxy_starting || the_session.proxy_connections > 0) {
		ndma_session_quantum(&the_session, 10000);
	}

	g_fprintf(stderr, "DONE\n");

	/* NOTREACHED */
	return 0;
}
