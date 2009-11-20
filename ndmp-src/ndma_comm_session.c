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


#include "ndmagents.h"




#ifndef NDMOS_OPTION_NO_CONTROL_AGENT

int
ndma_client_session (struct ndm_session *sess)
{
	struct ndm_job_param *	job = &sess->control_acb.job;
	int			rc;

	rc = ndma_job_audit (job, 0, 0);
	if (rc)
		return -1;

	rc = ndma_session_initialize (sess);
	if (rc) return rc;

	rc = ndma_session_commission (sess);
	if (rc) return rc;

	rc = ndmca_connect_control_agent (sess);
	if (rc) return rc;	/* already tattled */

	sess->conn_open = 1;
	sess->conn_authorized = 1;

	rc = ndmca_control_agent (sess);

	ndma_session_decommission (sess);

	return rc;
}

#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */


#ifndef NDMOS_EFFECT_NO_SERVER_AGENTS

int
ndma_server_session (struct ndm_session *sess, int control_sock)
{
	struct ndmconn *	conn;
	int			rc;
	struct sockaddr		sa;
	int			len;

	rc = ndma_session_initialize (sess);
	if (rc) return rc;

	rc = ndma_session_commission (sess);
	if (rc) return rc;

	len = sizeof sa;
	rc = getpeername (control_sock, &sa, &len);
	if (rc < 0) {
		perror ("getpeername");
	} else {
		ndmalogf (sess, 0, 2, "Connection accepted from %s",
			inet_ntoa ( ((struct sockaddr_in *)&sa)->sin_addr));
	}

	len = sizeof sa;
	rc = getsockname (control_sock, &sa, &len);
	if (rc < 0) {
		perror ("getsockname");
	} else {
		ndmalogf (sess, 0, 2, "Connection accepted to %s",
			inet_ntoa ( ((struct sockaddr_in *)&sa)->sin_addr));
	}

	conn = ndmconn_initialize (0, "#C");
	if (!conn) {
		ndmalogf (sess, 0, 0, "can't init connection");
		close (control_sock);
		return -1;
	}

	ndmos_condition_control_socket (sess, control_sock);

	ndmconn_set_snoop (conn, &sess->param.log, sess->param.log_level);
	ndmconn_accept (conn, control_sock);

	conn->call = ndma_call;
	conn->context = sess;

	sess->plumb.control = conn;

	while (!conn->chan.eof) {
		ndma_session_quantum (sess, 1000);
	}

#if 0
	ndmalogf (sess, 0, 2, "Connection close %s",
		inet_ntoa ( ((struct sockaddr_in *)sa)->sin_addr));
#endif

	ndmconn_destruct (conn);

	ndma_session_decommission (sess);

	return 0;
}

static gpointer
exit_on_stdin_eof_thread(gpointer data G_GNUC_UNUSED)
{
    char buf[32];

    for (;;) {
	if (read(0, buf, sizeof(buf)) <= 0) {
	    printf("DONE\n");
	    fflush(stdout);
	    exit(0);
	}
    }
}

int
ndma_daemon_session (struct ndm_session *sess, int port, int is_test_daemon)
{
	int			listen_sock;
	int			conn_sock, len, rc;
	struct sockaddr		sa;

	listen_sock = socket (AF_INET, SOCK_STREAM, 0);
	if (listen_sock < 0) {
		perror ("socket");
		return 1;
	}

	ndmos_condition_listen_socket (sess, listen_sock);

	NDMOS_MACRO_SET_SOCKADDR(&sa, 0, port);

	if (bind (listen_sock, &sa, sizeof sa) < 0) {
		perror ("bind");
		return 2;
	}

	if (listen (listen_sock, 1) < 0) {
		perror ("listen");
		return 3;
	}

	if (is_test_daemon) {
	    /* the listen socket is running, so tell our invoker */
	    printf("READY\n");
	    fflush(stdout);

	    /* and exit when our stdin goes away */
	    g_thread_init(NULL);
	    g_thread_create(exit_on_stdin_eof_thread, NULL, FALSE, NULL);
	}

	for (;;) {
		len = sizeof sa;
		conn_sock = accept (listen_sock, &sa, &len);
		if (conn_sock < 0) {
			perror ("accept");
			return 4;
		}

		rc = fork();
		if (rc < 0) {
			perror ("fork");
			return 5;
		}

		if (rc == 0) {
			close (listen_sock);
			ndma_server_session (sess, conn_sock);
			exit (0);
		}
		close (conn_sock);
	}

	return 0;
}

#endif /* !NDMOS_EFFECT_NO_SERVER_AGENTS */


int
ndma_session_distribute_quantum (struct ndm_session *sess)
{
	int		total_did_something = 0;
	int		did_something;

	do {
		did_something = 0;

		did_something |= ndmis_quantum (sess);

#ifndef NDMOS_OPTION_NO_TAPE_AGENT
		if (sess->tape_acb.mover_state.state != NDMP9_MOVER_STATE_IDLE)
			did_something |= ndmta_quantum (sess);
#endif /* !NDMOS_OPTION_NO_TAPE_AGENT */

#ifndef NDMOS_OPTION_NO_DATA_AGENT
		if (sess->data_acb.data_state.state != NDMP9_DATA_STATE_IDLE)
			did_something |= ndmda_quantum (sess);
#endif /* !NDMOS_OPTION_NO_DATA_AGENT */

		total_did_something |= did_something;

	} while (did_something);

	return total_did_something;
}


int
ndma_session_quantum (struct ndm_session *sess, int max_delay_secs)
{
	struct ndm_image_stream *is = &sess->plumb.image_stream;
	struct ndmconn *	conn;
	struct ndmconn *	conntab[5];
	int			n_conntab;
	struct ndmchan *	chtab[16];
	int			n_chtab;
	int			i;
	int			max_delay_usec = max_delay_secs * 1000;

	/*
	 * Gather distinct connections
	 */
	n_conntab = 0;
	if ((conn = sess->plumb.control))
		conntab[n_conntab++] = conn;
	if ( (conn = sess->plumb.data)
	 && conn != sess->plumb.control)
		conntab[n_conntab++] = conn;
	if ( (conn = sess->plumb.tape)
	 && conn != sess->plumb.data
	 && conn != sess->plumb.control)
		conntab[n_conntab++] = conn;
	if ( (conn = sess->plumb.robot)
	 && conn != sess->plumb.tape
	 && conn != sess->plumb.data
	 && conn != sess->plumb.control)
		conntab[n_conntab++] = conn;

	/*
	 * Add connections to channel table
	 */
	n_chtab = 0;
	for (i = 0; i < n_conntab; i++) {
		conn = conntab[i];
		chtab[n_chtab++] = &conn->chan;
	}

#ifndef NDMOS_OPTION_NO_DATA_AGENT
	/*
	 * Add DATA AGENT channels to table if active (!IDLE)
	 */
	if (sess->data_acb.data_state.state != NDMP9_DATA_STATE_IDLE) {
		chtab[n_chtab++] = &sess->data_acb.formatter_image;
		chtab[n_chtab++] = &sess->data_acb.formatter_error;
		chtab[n_chtab++] = &sess->data_acb.formatter_wrap;
	}
#endif /* !NDMOS_OPTION_NO_DATA_AGENT */

	/*
	 * Add image stream to channel table
	 */
	if (is->remote.connect_status == NDMIS_CONN_LISTEN)
		chtab[n_chtab++] = &is->remote.listen_chan;

	chtab[n_chtab++] = &is->chan;

	/*
	 * Let TAPE and DATA AGENTS get a bit of work done.
	 * This fills channel buffers as much as possible prior to blocking.
	 */
	if (ndma_session_distribute_quantum (sess))
		max_delay_usec = 0;

#if 0
#ifndef NDMOS_OPTION_NO_DATA_AGENT
	/* bogus */
	if (sess->data_acb.data_state.state == NDMP9_DATA_STATE_ACTIVE
	 && sess->data_acb.data_state.data_connection_addr.addr_type
						== NDMP9_ADDR_LOCAL) {
		/*
		 * There is no remote connection to cue forward
		 * progress between local DATA/MOVER.
		 * So, sniff all the connections, and immediately
		 * attempt the next tape record.
		 */
		 max_delay_usec = 0;
	}
#endif /* !NDMOS_OPTION_NO_DATA_AGENT */
#endif

	/*
	 * Block awaiting ready I/O. Many channel buffers
	 * will have actual I/O (read/write) performed.
	 */
	ndmchan_quantum (chtab, n_chtab, max_delay_usec);

	/*
	 * Tattle for debug
	 */
	if (sess->param.log_level > 7) {
		for (i = 0; i < n_chtab; i++) {
			struct ndmchan *	ch = chtab[i];
			char			buf[80];

			ndmchan_pp (ch, buf);
			ndmalogf (sess, 0, 7, "ch %s", buf);
		}
	}

	/*
	 * Let TAPE and DATA AGENTS get a bit more work done.
	 * This will mostly digest whatever data just arrived.
	 */
	ndma_session_distribute_quantum (sess);

	/*
	 * Dispatch any pending activity on the control connections
	 */
	for (i = 0; i < n_conntab; i++) {
		conn = conntab[i];
		if (conn->chan.ready) {
			conn->chan.ready = 0;
			ndma_dispatch_conn (sess, conn);
		}
	}

	return 0;
}




int
ndma_session_initialize (struct ndm_session *sess)
{
	ndmis_initialize (sess);

#ifndef NDMOS_OPTION_NO_CONTROL_AGENT
	/* ndmca_initialize (sess); */
#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */

#ifndef NDMOS_OPTION_NO_DATA_AGENT
	ndmda_initialize (sess);
#endif /* !NDMOS_OPTION_NO_DATA_AGENT */

#ifndef NDMOS_OPTION_NO_TAPE_AGENT
	ndmta_initialize (sess);
#endif /* !NDMOS_OPTION_NO_TAPE_AGENT */

#ifndef NDMOS_OPTION_NO_ROBOT_AGENT
	/* ndmra_initialize (sess); */
#endif /* !NDMOS_OPTION_NO_ROBOT_AGENT */

	return 0;
}

int
ndma_session_commission (struct ndm_session *sess)
{
	ndmis_commission (sess);

#ifndef NDMOS_OPTION_NO_CONTROL_AGENT
	/* ndmca_commission (sess); */
#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */

#ifndef NDMOS_OPTION_NO_DATA_AGENT
	ndmda_commission (sess);
#endif /* !NDMOS_OPTION_NO_DATA_AGENT */

#ifndef NDMOS_OPTION_NO_TAPE_AGENT
	ndmta_commission (sess);
#endif /* !NDMOS_OPTION_NO_TAPE_AGENT */

#ifndef NDMOS_OPTION_NO_ROBOT_AGENT
	/* ndmra_commission (sess); */
#endif /* !NDMOS_OPTION_NO_ROBOT_AGENT */

	return 0;
}

int
ndma_session_decommission (struct ndm_session *sess)
{
	ndmis_decommission (sess);

#ifndef NDMOS_OPTION_NO_CONTROL_AGENT
	/* ndmca_decommission (sess); */
#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */

#ifndef NDMOS_OPTION_NO_DATA_AGENT
	ndmda_decommission (sess);
#endif /* !NDMOS_OPTION_NO_DATA_AGENT */

#ifndef NDMOS_OPTION_NO_TAPE_AGENT
	ndmta_decommission (sess);
#endif /* !NDMOS_OPTION_NO_TAPE_AGENT */

#ifndef NDMOS_OPTION_NO_ROBOT_AGENT
	/* ndmra_decommission (sess); */
#endif /* !NDMOS_OPTION_NO_ROBOT_AGENT */

	return 0;
}

