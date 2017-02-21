/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2016-2016 Carbonite, Inc.  All Rights Reserved.
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

#include <config.h>
#include "sockaddr-util.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "security-file.h"

int
main(
    int         argc,
    char **     argv)
{
    int s;
    int r;
    int rc;
    struct msghdr msg;
    struct msghdr msg_socket;
    struct msghdr msg_ambind_data;
    struct cmsghdr *cmsg;
    char cmsg_socket[CMSG_SPACE(sizeof(s))];
    char cmsgbuf[CMSG_SPACE(sizeof(s))];
    ambind_t ambind;
    struct iovec iov[2];
    int sockfd;

    if (argc < 2) {
    }
    sockfd = atoi(argv[1]);

    do {
        struct timeval timeout = { 5, 0 };
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(sockfd, &readSet);

        rc = select(sockfd+1, &readSet, NULL, NULL, &timeout);
    } while (rc < 0 && errno == EINTR);

    if (rc <= 0) {
	fprintf(stderr, "ambind: select failed: %s\n", strerror(errno));
        shutdown(sockfd, SHUT_RDWR);
        return -1;
    }

    // read the message socket msg
    memset(&msg_socket, 0, sizeof(msg_socket));
    msg_socket.msg_control = cmsg_socket;
    msg_socket.msg_controllen = sizeof(cmsg_socket);
    rc = recvmsg(sockfd, &msg_socket, 0);
    if (rc == -1) {
	fprintf(stderr, "ambind: first recvmsg failed: %s\n", strerror(errno));
	exit(1);
    }
    cmsg = CMSG_FIRSTHDR(&msg_socket);
    if (cmsg == NULL || cmsg -> cmsg_type != SCM_RIGHTS) {
	fprintf(stderr, "ambind: The first control structure contains no file descriptor.\n");
	exit(2);
    }
    memcpy(&s, CMSG_DATA(cmsg), sizeof(s));

    do {
        struct timeval timeout = { 5, 0 };
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(sockfd, &readSet);

        rc = select(sockfd+1, &readSet, NULL, NULL, &timeout);
    } while (rc < 0 && errno == EINTR);

    if (rc <= 0) {
	fprintf(stderr, "ambind: select failed: %s\n", strerror(errno));
        shutdown(sockfd, SHUT_RDWR);
        return -1;
    }

    // read the ambind msg
    memset(&msg_ambind_data, 0, sizeof(msg_ambind_data));
    iov[0].iov_base = &ambind;
    iov[0].iov_len = sizeof(ambind_t);
    iov[1].iov_base = NULL;
    iov[1].iov_len = 0;
    msg_ambind_data.msg_iov = iov;
    msg_ambind_data.msg_iovlen = 1;

    rc = recvmsg(sockfd, &msg_ambind_data, 0);
    if (rc == -1) {
	fprintf(stderr, "ambind: second recvmsg failed: %s\n", strerror(errno));
	shutdown(sockfd, SHUT_RDWR);
	exit(2);
    }
    if (rc != sizeof(ambind_t)) {
	fprintf(stderr, "ambind: recvmsg failed: size == %d\n", rc);
	shutdown(sockfd, SHUT_RDWR);
	exit(2);
    }

    if (!security_allow_bind(s, &ambind.addr)) {
	shutdown(sockfd, SHUT_RDWR);
	exit(2);
    }

    r = bind(s, (struct sockaddr *)&ambind.addr, ambind.socklen);
    if (r != 0) {
	if (errno != EADDRINUSE) {
	    fprintf(stderr, "ambind: bind failed A: %s\n", strerror(errno));
	    shutdown(sockfd, SHUT_RDWR);
	    exit(2);
	}
	fprintf(stderr, "WARNING: ambind: bind failed B: %s\n", strerror(errno));
	if (shutdown(sockfd, SHUT_RDWR) < 0) {
	    fprintf(stderr, "ambind: shutdown failed B: %s\n", strerror(errno));
	    exit(1);
	}
	exit(1);
    }

    memset(&msg,0, sizeof(msg));
    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);
    cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(s));
    memcpy(CMSG_DATA(cmsg), &s, sizeof(s));
    msg.msg_controllen = cmsg->cmsg_len;

    // send the socket
    if ((sendmsg(sockfd, &msg, 0)) < 0) {
	fprintf(stderr, "ambind: sendmsg failed: %s\n", strerror(errno));
	exit(1);
    }
    if (shutdown(sockfd, SHUT_RDWR) < 0) {
	fprintf(stderr, "ambind: shutdown failed C: %s\n", strerror(errno));
	exit(1);
    }
    exit(0);
}
