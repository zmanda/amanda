/* Copyright (c) 2009 Zmanda Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 *
 * controlling proxy communication with amanda
 */

#include "ndmagents.h"
#include "ndmp-proxy.h"
#include "ipc-binary.h"

int
ndma_proxy_session (struct ndm_session *sess)
{
	int			conn_sock, len, rc;
	struct sockaddr		sa;
	int			proxy_sock;

	proxy_sock = socket (AF_INET, SOCK_STREAM, 0);
	if (proxy_sock < 0) {
		perror ("socket");
		return 1;
	}

	ndmalogf (sess, 0, 2, "set sess->protocol_listen");

	ndmos_condition_listen_socket (sess, proxy_sock);

	NDMOS_MACRO_SET_SOCKADDR(&sa, 0, sess->proxy_port);

	if (bind (proxy_sock, &sa, sizeof sa) < 0) {
		ndmalogf (sess, 0, 2, "Can't bind the socket(%d): %s\n", sess->proxy_port, strerror(errno));
		if (errno == EADDRINUSE) {
		    fprintf(stdout, "BIND: Address already in use\n");
		} else {
		    fprintf(stdout, "Can't bind the socket(%d): %s\n", sess->proxy_port, strerror(errno));
		}
		fflush(stdout);
		perror ("bind");
		return 2;
	}

	if (listen (proxy_sock, 1) < 0) {
		ndmalogf (sess, 0, 2, "Can't listen a socket: %s\n", strerror(errno));
		fprintf(stdout, "Can't listen a socket: %s\n", strerror(errno));
		fflush(stdout);
		perror ("listen");
		return 3;
	}

	/* set up to listen on this new socket */
	ndmchan_initialize(&sess->listen_chan, "proxy-listen");
	ndmchan_start_listen(&sess->listen_chan, proxy_sock);

	/* start reading on stdin */
	ndmchan_initialize(&sess->stdin_chan, "proxy-stdin");
	ndmchan_setbuf(&sess->stdin_chan, malloc(65536), 65536);
        ndmchan_start_read(&sess->stdin_chan, 0);

	/* tell our invoker that we are OK */
	fprintf(stdout, "PORT %d\n", sess->proxy_port);
	fflush(stdout);

	/* and run forever */
	for (;;) {
		ndma_session_quantum(sess, 10000);
	}

	/* NOTREACHED */
	/* close (proxy_sock); */

	return 0;
}

static void
ndma_dispatch_proxy_stdin(
    struct ndm_session *sess)
{
	/* TODO: stop more gracefully */

	/* exit if our stdin is closed */
	if (sess->stdin_chan.eof) {
		exit(1);
	}

	/* exit if there's any data on stdin */
	if (sess->stdin_chan.ready) {
		exit(1);
	}
}

static void
ndma_dispatch_proxy_listen(
    struct ndm_session *sess)
{
	int			conn_sock, len;
	struct sockaddr_in	sa;
	char			*name;
	struct proxy_channel	*pxchan;
	ipc_binary_message_t	*msg;
	struct proxy_channel	**pxchanp;
	char			*service;
	char			*errstr = NULL;

	/* if we have a new incoming connection, use it */
	if (!sess->listen_chan.ready)
	    return;

	len = sizeof(struct sockaddr);
	conn_sock = accept(sess->listen_chan.fd, (struct sockaddr *)&sa, &len);
	name = inet_ntoa(sa.sin_addr);
	ndmalogf (sess, 0, 7, "got a connection from %s", name);

	/* demand that it be from localhost */
	if (strcmp(name, "127.0.0.1") != 0) {
	    ndmalogf(sess, 0, 7, "Address '%s' is not 127.0.0.1; disconnecting", name);
	    close(conn_sock);
	    return;
	}

	/* check connection from who: device, changer or application */
	pxchan = g_new0(struct proxy_channel, 1);
	pxchan->sock = conn_sock;
	pxchan->ipc.proto = get_ndmp_proxy_proto();

	/* TODO: this is a synchronous read and should be replaced with
	 * something async */
	msg = ipc_binary_read_message(&pxchan->ipc, conn_sock);
	if (!msg) {
		ndmalogf(sess, 0, 7, "No message received; disconnecting");
		g_free(pxchan);
		close(conn_sock);
		return;
	}

	if (msg->cmd_id != NDMP_PROXY_CMD_SELECT_SERVICE) {
		ndmalogf(sess, 0, 7, "Invalid message received; disconnecting");
		g_free(pxchan);
		close(conn_sock);
		return;
	}

	/* identify the service */
	service = (char *)msg->args[NDMP_PROXY_SERVICE].data;
	if (0 == strcmp(service, "DEVICE")) {
		pxchanp = &sess->proxy_device_chan;
	} else if (0 == strcmp(service, "APPLICATION")) {
		pxchanp = &sess->proxy_application_chan;
	} else if (0 == strcmp(service, "CHANGER")) {
		pxchanp = &sess->proxy_changer_chan;
	} else {
		errstr = ndmp9_error_to_str(NDMP9_ILLEGAL_ARGS_ERR);
	}

	ipc_binary_free_message(msg);

	/* is the service already active? */
	if (!errstr && *pxchanp != NULL) {
		ndmalogf(sess, 0, 7, "Service is already in use");
		errstr = ndmp9_error_to_str(NDMP9_DEVICE_BUSY_ERR);
	}

	/* send the response; TODO: do this asynchronously */
	msg = ipc_binary_new_message(&pxchan->ipc, NDMP_PROXY_REPLY_GENERIC);
	if (errstr) {
		ipc_binary_add_arg(msg, NDMP_PROXY_ERROR, 0, errstr, 0);
		ipc_binary_add_arg(msg, NDMP_PROXY_ERRCODE, 0, errstr, 0);
	}

	if (ipc_binary_write_message(&pxchan->ipc, conn_sock, msg) < 0) {
		ndmalogf(sess, 0, 7, "Error writing to socket: %s", strerror(errno));
		g_free(pxchan);
		close(conn_sock);
		return;
	}

	if (errstr) {
		g_free(pxchan);
		close(conn_sock);
		return;
	}

	/* put this proxy_channel in the appropriate place in the session */
	*pxchanp = pxchan;

	/* set up the ndmp communication channel to listen for incoming messages */
	ndmchan_initialize(&pxchan->ndm, "proxy-service");
	ndmchan_setbuf(&pxchan->ndm, malloc(65536), 65536);
	ndmchan_start_read(&pxchan->ndm, conn_sock);
}

static void
ndma_dispatch_proxy_device(
    struct ndm_session *sess)
{
	int			conn_sock, len;
	struct sockaddr		sa;
	ipc_binary_message_t *msg;

	/* TODO: handle EOF */
	if (!sess->proxy_device_chan || !sess->proxy_device_chan->ndm.ready)
	    return;

	/* feed data into the ipc_binary channel */
	ipc_binary_feed_data(&sess->proxy_device_chan->ipc,
		ndmchan_n_ready(&sess->proxy_device_chan->ndm),
		&sess->proxy_device_chan->ndm.data[sess->proxy_device_chan->ndm.beg_ix]);
	sess->proxy_device_chan->ndm.beg_ix += ndmchan_n_ready(&sess->proxy_device_chan->ndm);

	/* loop over any available incoming messages */
	while ((msg = ipc_binary_poll_message(&sess->proxy_device_chan->ipc))) {
		switch (msg->cmd_id) {

		case NDMP_PROXY_CMD_TAPE_OPEN: {
			struct ndm_control_agent *ca = &sess->control_acb;
			char *errstr = NULL;
			ndmp9_error errcode = 0;
			ipc_binary_message_t *send_msg;
			int rc;
			char *tape_agent_str;
			tape_agent_str = g_strdup_printf("%s/%s",
			    (char *)msg->args[NDMP_PROXY_HOST].data,
			    (char *)msg->args[NDMP_PROXY_USER_PASS].data);

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_OPEN request");

			ca->tape_mode = NDMP9_TAPE_RDWR_MODE;
			ca->is_label_op = 1;
			ca->job.tape_device = g_strdup((char *)msg->args[NDMP_PROXY_FILENAME].data);

			rc = ndmagent_from_str(&ca->job.tape_agent, tape_agent_str);
			g_free(tape_agent_str);

			if (!rc) {
				rc = ndmca_connect_tape_agent(sess);
				if (rc) {
					/* save errstr before deleting it */
					if (ndmconn_get_err_msg(sess->plumb.tape) != NULL) {
						errstr = g_strdup(ndmconn_get_err_msg(sess->plumb.tape));
					}
					ndmconn_destruct (sess->plumb.tape);
				}
			}

			if (!rc)
				rc = ndmca_tape_open (sess);

			if (!rc)
				rc = ndmca_tape_get_state (sess);

			if (rc && !errcode)
				errcode = sess->plumb.tape->last_reply_error;

			if (rc && !errstr)
				errstr = g_strdup(ndmp9_error_to_str(errcode));

			send_msg = ipc_binary_new_message(&sess->proxy_device_chan->ipc,
						    NDMP_PROXY_REPLY_GENERIC);
			if (rc) {
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERRCODE, 0,
					ndmp9_error_to_str(errcode), FALSE);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERROR, 0,
					errstr, TRUE);
			}
			if (ipc_binary_write_message(&sess->proxy_device_chan->ipc,
				    sess->proxy_device_chan->sock, send_msg) < 0) {
				ndmalogf (sess, 0, 7, "error writing to device channel: %s",
							strerror(errno));
				/* TODO: shut down the connection */
			}
			break;
		}

		case NDMP_PROXY_CMD_TAPE_CLOSE: {
			ipc_binary_message_t *send_msg;

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_CLOSE request");
			ndmca_tape_close(sess);

			send_msg = ipc_binary_new_message(&sess->proxy_device_chan->ipc,
						    NDMP_PROXY_REPLY_GENERIC);
			if (ipc_binary_write_message(&sess->proxy_device_chan->ipc,
				    sess->proxy_device_chan->sock, send_msg) < 0) {
				ndmalogf (sess, 0, 7, "error writing to device channel: %s",
							strerror(errno));
				/* TODO: shut down the connection */
			}
			break;
		}

		case NDMP_PROXY_CMD_TAPE_READ: {
			char *buf;
			int   read_count;
			ipc_binary_message_t *send_msg;
			long count;
			int rc;

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_READ request");
			count = strtol((char *)msg->args[NDMP_PROXY_COUNT].data, NULL, 10);
			/* TODO: sanity check count */

			buf = malloc(count);
			rc = ndmca_tape_read_partial(sess, buf, count, &read_count);

			send_msg = ipc_binary_new_message(&sess->proxy_device_chan->ipc,
						    NDMP_PROXY_REPLY_TAPE_READ);
			ndmalogf (sess, 0, 7, "tata: %d %d", rc, read_count);
			if (rc) {
				char *errstr = ndmp9_error_to_str(sess->plumb.tape->last_reply_error);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERRCODE, 0,
					errstr, FALSE);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERROR, 0,
					errstr, FALSE);
			} else {
				ipc_binary_add_arg(send_msg, NDMP_PROXY_DATA, read_count,
					buf, 0);
			}

			if (ipc_binary_write_message(&sess->proxy_device_chan->ipc,
				    sess->proxy_device_chan->sock, send_msg) < 0) {
				ndmalogf (sess, 0, 7, "error writing to device channel: %s",
							strerror(errno));
				/* TODO: shut down the connection */
			}
			ndmalogf (sess, 0, 7, "sent NDMP_PROXY_REPLY_TAPE_READ");

			break;
		}

		case NDMP_PROXY_CMD_TAPE_WRITE: {
			char *buf = msg->args[NDMP_PROXY_DATA].data;;
			ipc_binary_message_t *send_msg;
			int   write_count;
			char *error = NULL;
			int rc;

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_WRITE request");

			rc = ndmca_tape_write (sess, buf, msg->args[NDMP_PROXY_DATA].len);

			send_msg = ipc_binary_new_message(&sess->proxy_device_chan->ipc,
						    NDMP_PROXY_REPLY_GENERIC);
			if (rc) {
				ndmp9_error errcode = sess->plumb.tape->last_reply_error;
				char *errstr = ndmp9_error_to_str(errcode);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERRCODE, 0,
					errstr, FALSE);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERROR, 0,
					errstr, TRUE);
			}
			if (ipc_binary_write_message(&sess->proxy_device_chan->ipc,
				    sess->proxy_device_chan->sock, send_msg) < 0) {
				ndmalogf (sess, 0, 7, "error writing to device channel: %s",
							strerror(errno));
				/* TODO: shut down the connection */
			}
			break;
		}

		case NDMP_PROXY_CMD_TAPE_MTIO: {
			struct ndm_control_agent *ca = &sess->control_acb;
			char *command = (char *)msg->args[NDMP_PROXY_COMMAND].data;
			ipc_binary_message_t *send_msg;
			long count;
			ndmp9_error errcode;
			int rc;

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_MTIO %s %d request", command, count);

			count = strtol((char *)msg->args[NDMP_PROXY_COUNT].data, NULL, 10);

			if (strcmp(command, "REWIND") == 0) {
				rc = ndmca_media_mtio_tape (sess, NDMP9_MTIO_REW, 1, NULL);
			} else if (strcmp(command, "EOF") == 0) {
				rc = ndmca_media_mtio_tape (sess, NDMP9_MTIO_EOF, 1, NULL);
			} else {
				errcode = NDMP9_CLASS_NOT_SUPPORTED;
			}
			if (rc)
				errcode = sess->plumb.tape->last_reply_error;
			send_msg = ipc_binary_new_message(&sess->proxy_device_chan->ipc,
						    NDMP_PROXY_REPLY_GENERIC);
			if (rc) {
				ndmp9_error errcode = sess->plumb.tape->last_reply_error;
				char *errstr = ndmp9_error_to_str(errcode);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERRCODE, 0,
					errstr, FALSE);
				ipc_binary_add_arg(send_msg, NDMP_PROXY_ERROR, 0,
					errstr, TRUE);
			}
			if (ipc_binary_write_message(&sess->proxy_device_chan->ipc,
				    sess->proxy_device_chan->sock, send_msg) < 0) {
				ndmalogf (sess, 0, 7, "error writing to device channel: %s",
							strerror(errno));
				/* TODO: shut down the connection */
			}
			break;
		}

		default: {
		    /* TODO */
		}
		}

		ipc_binary_free_message(msg);
	}
}

static void
ndma_dispatch_proxy_application(
    struct ndm_session *sess G_GNUC_UNUSED)
{
}

static void
ndma_dispatch_proxy_changer(
    struct ndm_session *sess G_GNUC_UNUSED)
{
}

int
ndma_dispatch_proxy(
    struct ndm_session *sess)
{
	/* monitor each of our potential connections */
	ndma_dispatch_proxy_stdin(sess);
	ndma_dispatch_proxy_listen(sess);
	ndma_dispatch_proxy_device(sess);
	ndma_dispatch_proxy_application(sess);
	ndma_dispatch_proxy_changer(sess);

	return 0;
}
