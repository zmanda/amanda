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
#include "ndmp-protocol.h"
#include "ipc-binary.h"

int
ndma_proxy_session (struct ndm_session *sess)
{
	int			conn_sock, len, rc;
	struct sockaddr		sa;
	int			proxy_sock;

	ndmchan_initialize(&sess->proxy_listen, "proxy-listen");
	proxy_sock = socket (AF_INET, SOCK_STREAM, 0);
	if (proxy_sock < 0) {
		perror ("socket");
		return 1;
	}

	ndmalogf (sess, 0, 2, "set sess->protocol_listen");
	sess->protocol_listen = malloc(sizeof(listen_ndmp));
	memmove(sess->protocol_listen, &listen_ndmp, sizeof(listen_ndmp));
	sess->protocol_listen->fd = proxy_sock;

	ndmos_condition_listen_socket (sess, proxy_sock);

	NDMOS_MACRO_SET_SOCKADDR(&sa, 0, sess->proxy_port);

	if (bind (proxy_sock, &sa, sizeof sa) < 0) {
		ndmalogf (sess, 0, 2, "Can't bind the socket(%d): %s\n", sess->proxy_port, strerror(errno));
		fprintf(stdout, "Can't bind the socket(%d): %s\n", sess->proxy_port, strerror(errno));
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

	ndmchan_start_listen(&sess->proxy_listen, proxy_sock);

	ndmchan_initialize(&sess->proxy_input, "proxy-input");
	ndmchan_setbuf(&sess->proxy_input, malloc(65536), 65536);
        ndmchan_start_read(&sess->proxy_input, 0);

	fprintf(stdout, "OK\n");
	fflush(stdout);

	for (;;) {
		ndma_session_quantum(sess, 10000);
	}

	close (proxy_sock);

	return 0;
}

int
ndma_dispatch_proxy(
    struct ndm_session *sess)
{
	char                *errstr = NULL;
	char                 errbuf[1024];
	int                  rc = 0;
	int                  error_code = NDMP9_NO_ERR;
	amprotocol_packet_t *c_packet;

	if (sess->proxy_input.eof) {
		exit(1);
	}

	if (sess->proxy_input.ready) {
		exit(1);
	}

	if (sess->proxy_listen.ready) {
		int			conn_sock, len;
		struct sockaddr_in	sa;
		int			bad_addr = 0;
		char			*name;

		len = sizeof(struct sockaddr);
		conn_sock = accept(sess->proxy_listen.fd, (struct sockaddr *)&sa, &len);
		name = inet_ntoa(sa.sin_addr);
		ndmalogf (sess, 0, 7, "got a connection from %s", name);
		if (strcmp(name, "127.0.0.1") != 0) {
			snprintf(errbuf, 1023, "Address '%s' is not 127.0.0.1", name);
			errstr = errbuf;
			bad_addr = 1;
			rc = -1;
		}

		/* check connection from who: device, changer or application */
		sess->protocol_listen->fd = conn_sock;
		c_packet = amprotocol_get(sess->protocol_listen);

		if (!c_packet) {
			errstr = "Got no packet";
		} else if (c_packet->command == CMD_DEVICE) {
			ndmalogf (sess, 0, 7, "It is a connection from a device-api");
			if (!rc) {
				ndmchan_initialize(&sess->proxy_device, "proxy-device");
				ndmchan_setbuf(&sess->proxy_device, malloc(65536), 65536);
				ndmchan_start_read(&sess->proxy_device, conn_sock);
				sess->protocol_device = malloc(sizeof(device_ndmp));
				memmove(sess->protocol_device, &device_ndmp, sizeof(device_ndmp));
				sess->protocol_device->fd = conn_sock;
			}
			if (!errstr) {
				errstr = ndmp9_error_to_str(NDMP9_NO_ERR);
			}
			rc = amprotocol_send_list(sess->protocol_listen,
					REPLY_DEVICE, 1, errstr);
		} else {
			if (!errstr)
				errstr = "Got invalid packet";
		}
		ndmalogf (sess, 0, 7, "%s", errstr);
		free_amprotocol_packet(c_packet);
	}

	if (sess->proxy_device.ready) {
		int			conn_sock, len;
		struct sockaddr		sa;

		c_packet = amprotocol_parse(sess->protocol_device,
				&sess->proxy_device.data[sess->proxy_device.beg_ix],
				ndmchan_n_ready(&sess->proxy_device));
		if (!c_packet) {
			/* a packet is not completely read, waiting for more data */
			goto end_device;
		}

		/* consume the packet */
		sess->proxy_device.beg_ix += c_packet->block_size;

		if (c_packet->command == CMD_TAPE_OPEN) {
			struct ndm_control_agent *ca = &sess->control_acb;
			char *tape_agent_str;
			int int2 = strlen((char *)c_packet->arguments[2].data);
			int int3 = strlen((char *)c_packet->arguments[3].data);

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_OPEN request");

			tape_agent_str = malloc(int2 + 1 + int3 + 1);
			strcpy(tape_agent_str, (char *)c_packet->arguments[2].data);
			tape_agent_str[int2] = '/';
			strcpy(tape_agent_str+int2+1, (char *)c_packet->arguments[3].data);
			ca->tape_mode = NDMP9_TAPE_RDWR_MODE;
		        ca->is_label_op = 1;
			ca->job.tape_device = strdup((char *)c_packet->arguments[0].data);

			rc = ndmagent_from_str(&ca->job.tape_agent, tape_agent_str);

			if (!rc) {
				rc = ndmca_connect_tape_agent(sess);
				if (rc) {
					/* save errstr before deleting it */
					if (ndmconn_get_err_msg(sess->plumb.tape) == NULL) {
						strcpy(errbuf, "Unknown error");
					} else {
						strcpy(errbuf, ndmconn_get_err_msg(sess->plumb.tape));
					}
					errstr = errbuf;
					ndmconn_destruct (sess->plumb.tape);
				}
			}

			if (!rc)
				rc = ndmca_tape_open (sess);

			if (!rc)
				rc = ndmca_tape_get_state (sess);

			if (!errstr)
				errstr = ndmp9_error_to_str(sess->plumb.tape->last_reply_error);
			rc = amprotocol_send_list(sess->protocol_device, REPLY_TAPE_OPEN, 1, errstr);
			free(tape_agent_str);
		} else if (c_packet->command == CMD_TAPE_CLOSE) {
			ndmca_tape_close(sess);
			ndmalogf (sess, 0, 7, "got a CMD_TAPE_CLOSE request");
			rc = amprotocol_send_list(sess->protocol_device, REPLY_TAPE_CLOSE, 1, ndmp9_error_to_str(NDMP9_NO_ERR));
		} else if (c_packet->command == CMD_TAPE_READ) {
			char *buf;
			int   read_count;
			guint32 count = ntohl(*((guint32 *)c_packet->arguments[0].data));

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_READ request");
			buf = malloc(count);
			rc = ndmca_tape_read_partial (sess, buf, count, &read_count);
			errstr = ndmp9_error_to_str(sess->plumb.tape->last_reply_error);
			if (rc) {
				amprotocol_send_list(sess->protocol_device, REPLY_TAPE_READ, 2, errstr, "");
			} else {
				amprotocol_send_binary(sess->protocol_device, REPLY_TAPE_READ, 2, strlen(errstr)+1, errstr, read_count, buf);
			}
		} else if (c_packet->command == CMD_TAPE_WRITE) {
			char *buf = c_packet->arguments[0].data;
			int   write_count;
			int   count = c_packet->arguments[0].size;
			char *error = NULL;

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_WRITE request");
			rc = ndmca_tape_write (sess, buf, count);
			amprotocol_send_list(sess->protocol_device, REPLY_TAPE_WRITE, 1, ndmp9_error_to_str(sess->plumb.tape->last_reply_error));
		} else if (c_packet->command == CMD_TAPE_MTIO) {
			struct ndm_control_agent *ca = &sess->control_acb;
			char *command = c_packet->arguments[0].data;
			int   count   = atoi(c_packet->arguments[1].data);

			ndmalogf (sess, 0, 7, "got a CMD_TAPE_MTIO %s %d request", command, count);
			if (strcmp(command, "REWIND") == 0) {
				rc = ndmca_media_mtio_tape (sess, NDMP9_MTIO_REW, 1, NULL);
			} else if (strcmp(command, "EOF") == 0) {
				rc = ndmca_media_mtio_tape (sess, NDMP9_MTIO_EOF, 1, NULL);
			} else {
				error_code = NDMP9_CLASS_NOT_SUPPORTED;
			}
			if (rc)
				error_code = sess->plumb.tape->last_reply_error;
			amprotocol_send_list(sess->protocol_device, REPLY_TAPE_MTIO, 1, ndmp9_error_to_str(error_code));
		} else {
		}
	}
end_device:

	if (sess->proxy_application.ready) {
	}

	if (sess->proxy_changer.ready) {
	}

	return 0;
}

