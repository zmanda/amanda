/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "amxfer.h"
#include "element-glue.h"
#include "directtcp.h"
#include "amutil.h"
#include "sockaddr-util.h"
#include "stream.h"
#include "debug.h"
#include "conffile.h"
#include "mem-ring.h"
#include "shm-ring.h"

/*
 * Instance definition
 */

typedef struct XferElementGlue_ {
    XferElement __parent__;

    /* instructions to push_buffer_impl */
    enum {
	PUSH_TO_RING_BUFFER,
	PUSH_TO_FD, /* write to write_fd */
	PUSH_INVALID,

	PUSH_ACCEPT_FIRST = (1 << 16),
	PUSH_CONNECT_FIRST = (2 << 16),
    } on_push;

    /* instructions to pull_buffer_impl */
    enum {
	PULL_FROM_RING_BUFFER,
	PULL_FROM_FD, /* read from read_fd */
	PULL_INVALID,

	PULL_ACCEPT_FIRST = (1 << 16),
	PULL_CONNECT_FIRST = (2 << 16),
    } on_pull;

    int *write_fdp;
    int *read_fdp;

    mem_ring_t *mem_ring;

    gboolean need_thread;

    /* the stuff we might use, depending on what flavor of glue we're
     * providing.. */
    int pipe[2];
    int input_listen_socket, output_listen_socket;
    int input_data_socket, output_data_socket;
    int read_fd, write_fd;

    /* a ring buffer of ptr/size pairs with semaphores */
    struct { gpointer buf; size_t size; } *ring;
    amsemaphore_t *ring_used_sem, *ring_free_sem;
    gint ring_head, ring_tail;

    GThread *thread;
    GThreadFunc threadfunc;
} XferElementGlue;

/*
 * Class definition
 */

typedef struct XferElementGlueClass_ {
    XferElementClass __parent__;
} XferElementGlueClass;

static GObjectClass *parent_class = NULL;

/*
 * Utility functions, etc.
 */

static void
make_pipe(
    XferElementGlue *self)
{
    if (pipe(self->pipe) < 0)
	g_critical(_("Could not create pipe: %s"), strerror(errno));
}

static void
send_xfer_done(
    XferElementGlue *self)
{
    xfer_queue_message(XFER_ELEMENT(self)->xfer,
	    xmsg_new((XferElement *)self, XMSG_DONE, 0));
}

static gboolean
do_directtcp_listen(
    XferElement *elt,
    int *sockp,
    DirectTCPAddr **addrsp)
{
    int sock;
    sockaddr_union data_addr;
    DirectTCPAddr *addrs;
    socklen_t len;
    struct addrinfo *res;
    struct addrinfo *res_addr;
    sockaddr_union *addr = NULL;

    if (resolve_hostname("localhost", 0, &res, NULL) != 0) {
	xfer_cancel_with_error(elt, "resolve_hostname(): %s", strerror(errno));
	return FALSE;
    }
    for (res_addr = res; res_addr != NULL; res_addr = res_addr->ai_next) {
	if (res_addr->ai_family == AF_INET) {
            addr = (sockaddr_union *)res_addr->ai_addr;
            break;
        }
    }
    if (!addr) {
        addr = (sockaddr_union *)res->ai_addr;
    }

    sock = *sockp = socket(SU_GET_FAMILY(addr), SOCK_STREAM, 0);
    if (sock < 0) {
	xfer_cancel_with_error(elt, "socket(): %s", strerror(errno));
	freeaddrinfo(res);
	return FALSE;
    }

    len = SS_LEN(addr);
    if (bind(sock, (struct sockaddr *)addr, len) != 0) {
	xfer_cancel_with_error(elt, "bind(): %s", strerror(errno));
	freeaddrinfo(res);
	close(sock);
	*sockp = -1;
	return FALSE;
    }

    if (listen(sock, 1) < 0) {
	xfer_cancel_with_error(elt, "listen(): %s", strerror(errno));
	freeaddrinfo(res);
	close(sock);
	*sockp = -1;
	return FALSE;
    }

    /* TODO: which addresses should this display? all ifaces? localhost? */
    len = sizeof(data_addr);
    if (getsockname(sock, (struct sockaddr *)&data_addr, &len) < 0)
	error("getsockname(): %s", strerror(errno));

    addrs = g_new0(DirectTCPAddr, 2);
    copy_sockaddr(&addrs[0], &data_addr);
    *addrsp = addrs;
    freeaddrinfo(res);

    return TRUE;
}

static gboolean
prolong_accept(
    gpointer data)
{
    return !XFER_ELEMENT(data)->cancelled;
}

static int
do_directtcp_accept(
    XferElementGlue *self,
    int *socketp)
{
    int sock;
    time_t timeout_time;
    time_t dtimeout = (time_t)getconf_int(CNF_DTIMEOUT);

    timeout_time = time(NULL) + dtimeout;
    g_assert(*socketp != -1);

    if ((sock = interruptible_accept(*socketp, NULL, NULL,
				     prolong_accept, self, timeout_time)) == -1) {
	close(*socketp);
	*socketp = -1;
	/* if the accept was interrupted due to a cancellation, then do not
	 * add a further error message */
	if (errno == 0 && XFER_ELEMENT(self)->cancelled)
	    return -1;

	xfer_cancel_with_error(XFER_ELEMENT(self),
	    _("Error accepting incoming connection: %s"), strerror(errno));
	wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	return -1;
    }

    /* close the listening socket now, for good measure */
    close(*socketp);
    *socketp = -1;

    g_debug("do_directtcp_accept: %d", sock);

    return sock;
}

static int
do_directtcp_connect(
    XferElementGlue *self,
    DirectTCPAddr *addrs)
{
    XferElement *elt = XFER_ELEMENT(self);
    sockaddr_union addr;
    int sock;
#ifdef WORKING_IPV6
    char strsockaddr[INET6_ADDRSTRLEN + 20];
#else
    char strsockaddr[INET_ADDRSTRLEN + 20];
#endif

    if (!addrs) {
	g_debug("element-glue got no directtcp addresses to connect to!");
	if (!elt->cancelled) {
	    xfer_cancel_with_error(elt,
		"%s got no directtcp addresses to connect to",
		xfer_element_repr(elt));
	}
	goto cancel_wait;
    }

    /* set up the sockaddr -- IPv4 only */
    copy_sockaddr(&addr, addrs);

    str_sockaddr_r(&addr, strsockaddr, sizeof(strsockaddr));

    if (strncmp(strsockaddr,"255.255.255.255:", 16) == 0) {
	char  buffer[32770];
	char *s;
	int   size;
	char *data_host;
	int   data_port;

	g_debug("do_directtcp_connect making indirect data connection to %s",
		strsockaddr);
	data_port = SU_GET_PORT(&addr);
	sock = stream_client(NULL, "localhost", data_port,
                                   STREAM_BUFSIZE, 0, NULL, 0);
	if (sock < 0) {
	    xfer_cancel_with_error(elt, "stream_client(): %s", strerror(errno));
	    goto cancel_wait;
	}
	size = full_read(sock, buffer, 32768);
	if (size < 0 ) {
	    xfer_cancel_with_error(elt, "failed to read from indirecttcp: %s",
				   strerror(errno));
	    goto cancel_wait;
	}
	close(sock);
	buffer[size++] = ' ';
	buffer[size] = '\0';
	if ((s = strchr(buffer, ':')) == NULL) {
	    xfer_cancel_with_error(elt,
				   "Failed to parse indirect data stream: %s",
				   buffer);
	    goto cancel_wait;
	}
	*s++ = '\0';
	data_host = buffer;
	data_port = atoi(s);

	str_to_sockaddr(data_host, &addr);
	SU_SET_PORT(&addr, data_port);

	str_sockaddr_r(&addr, strsockaddr, sizeof(strsockaddr));
    }

    sock = socket(SU_GET_FAMILY(&addr), SOCK_STREAM, 0);

    g_debug("do_directtcp_connect making data connection to %s", strsockaddr);

    if (sock < 0) {
	xfer_cancel_with_error(elt,
	    "socket(): %s", strerror(errno));
	goto cancel_wait;
    }
    if (connect(sock, (struct sockaddr *)&addr, SS_LEN(&addr)) < 0) {
	xfer_cancel_with_error(elt,
	    "connect(): %s", strerror(errno));
	close(sock);
	goto cancel_wait;
    }

    g_debug("do_directtcp_connect: connected to %s, fd %d", strsockaddr, sock);

    return sock;

cancel_wait:
    wait_until_xfer_cancelled(elt->xfer);
    return -1;
}

#define GLUE_BUFFER_SIZE 32768
#define GLUE_RING_BUFFER_SIZE 32

#define mech_pair(IN,OUT) ((IN)*XFER_MECH_MAX+(OUT))

/*
 * fd handling
 */

/* if self->read_fdp or self->write_fdp are pointing to this integer, then they
 * should be redirected to point to the upstream's output_fd or downstream's
 * input_fd, respectively, at the first call to get_read_fd or get_write_fd,
 * respectively. */
static int neighboring_element_fd = -1;

#define get_read_fd(self) (((self)->read_fd == -1)? _get_read_fd((self)) : (self)->read_fd)
static int
_get_read_fd(XferElementGlue *self)
{
    assert(self->read_fdp);

    if (self->read_fdp == &neighboring_element_fd) {
	XferElement *elt = XFER_ELEMENT(self);
	self->read_fd = xfer_element_swap_output_fd(elt->upstream, -1);
    } else {
	self->read_fd = *self->read_fdp;
	*self->read_fdp = -1;
    }
    self->read_fdp = NULL;
    return self->read_fd;
}

#define get_write_fd(self) (((self)->write_fd == -1)? _get_write_fd((self)) : (self)->write_fd)
static int
_get_write_fd(XferElementGlue *self)
{

    assert(self->write_fdp);

    if (self->write_fdp == &neighboring_element_fd) {
	XferElement *elt = XFER_ELEMENT(self);
	self->write_fd = xfer_element_swap_input_fd(elt->downstream, -1);
    } else {
	self->write_fd = *self->write_fdp;
	*self->write_fdp = -1;
    }
    self->write_fdp = NULL;
    return self->write_fd;
}

static int
close_read_fd(XferElementGlue *self)
{
    int fd = get_read_fd(self);
    self->read_fd = -1;
    return close(fd);
}

static int
close_write_fd(XferElementGlue *self)
{
    int fd = get_write_fd(self);
    self->write_fd = -1;
    return close(fd);
}

/*
 * Worker thread utility functions
 */

static void
pull_and_write(XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    int fd = get_write_fd(self);
    XMsg *msg;
    size_t written;

    g_debug("pull_and_write");
    self->write_fdp = NULL;

    while (!elt->cancelled) {
	size_t len;
	char *buf;

	/* get a buffer from upstream */
	buf = xfer_element_pull_buffer(elt->upstream, &len);
	if (!buf)
	    break;

	/* write it */
	if (!elt->downstream->drain_mode) {
	    written = full_write(fd, buf, len);
	    if (written < len) {
		if (elt->downstream->must_drain) {
		    g_debug("Error writing to fd %d: %s", fd, strerror(errno));
		} else if (elt->downstream->ignore_broken_pipe && errno == EPIPE) {
		} else {
		    if (!elt->cancelled) {
			xfer_cancel_with_error(elt,
			    _("Error writing to fd %d: %s"), fd, strerror(errno));
			wait_until_xfer_cancelled(elt->xfer);
		    }
		    amfree(buf);
		    break;
		}
		elt->downstream->drain_mode = TRUE;
	    }
        }
	crc32_add((uint8_t *)buf, len, &elt->crc);

	amfree(buf);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_buffers(elt->upstream);

    g_debug("sending XMSG_CRC message %p", elt->downstream);
    g_debug("pull_and_write CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->downstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    /* close the fd we've been writing, as an EOF signal to downstream, and
     * set it to -1 to avoid accidental re-use */
    close_write_fd(self);
}

static void
pull_static_and_write(XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    int fd = get_write_fd(self);
    XMsg *msg;
    size_t written;
    size_t block_size_up = xfer_element_get_block_size(elt->upstream);
    size_t block_size;
    char  *buf, *buf1;

    g_debug("pull_static_and_write");
    if (block_size_up != 0)
	block_size = block_size_up;
    else
	block_size = NETWORK_BLOCK_BYTES;

    buf = malloc(block_size);
    self->write_fdp = NULL;

    while (!elt->cancelled) {
	size_t len;

	/* get a buffer from upstream */
	buf1 = xfer_element_pull_buffer_static(elt->upstream, buf, block_size, &len);
	if (!buf1)
	    break;

	/* write it */
	if (!elt->downstream->drain_mode) {
	    written = full_write(fd, buf, len);
	    if (written < len) {
		if (elt->downstream->must_drain) {
		    g_debug("Error writing to fd %d: %s", fd, strerror(errno));
		} else if (elt->downstream->ignore_broken_pipe && errno == EPIPE) {
		} else {
		    if (!elt->cancelled) {
			xfer_cancel_with_error(elt,
			    _("Error writing to fd %d: %s"), fd, strerror(errno));
			wait_until_xfer_cancelled(elt->xfer);
		    }
		    amfree(buf);
		    break;
		}
		elt->downstream->drain_mode = TRUE;
	    }
        }
	crc32_add((uint8_t *)buf, len, &elt->crc);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_buffers(elt->upstream);

    g_debug("sending XMSG_CRC message %p", elt->downstream);
    g_debug("pull_static_and_write CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->downstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);
    amfree(buf);

    /* close the fd we've been writing, as an EOF signal to downstream, and
     * set it to -1 to avoid accidental re-use */
    close_write_fd(self);
}

static void
read_and_write(XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    /* dynamically allocate a buffer, in case this thread has
     * a limited amount of stack allocated */
    char *buf = g_malloc(GLUE_BUFFER_SIZE);
    int rfd = get_read_fd(self);
    int wfd = get_write_fd(self);
    XMsg *msg;
    crc32_init(&elt->crc);

    g_debug("read_and_write: read from %d, write to %d", rfd, wfd);
    while (!elt->cancelled) {
	size_t len;

	/* read from upstream */
	len = read_fully(rfd, buf, GLUE_BUFFER_SIZE, NULL);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		if (!elt->cancelled) {
		    xfer_cancel_with_error(elt,
			_("Error reading from fd %d: %s"), rfd, strerror(errno));
		    wait_until_xfer_cancelled(elt->xfer);
		}
		break;
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		break;
	    }
	}

	/* write the buffer fully */
	if (!elt->downstream->drain_mode && full_write(wfd, buf, len) < len) {
	    if (elt->downstream->must_drain) {
		g_debug("Could not write to fd %d: %s",  wfd, strerror(errno));
	    } else if (elt->downstream->ignore_broken_pipe && errno == EPIPE) {
	    } else {
		if (!elt->cancelled) {
		    xfer_cancel_with_error(elt,
			_("Could not write to fd %d: %s"),
			wfd, strerror(errno));
		    wait_until_xfer_cancelled(elt->xfer);
		}
		break;
	    }
	}
	crc32_add((uint8_t *)buf, len, &elt->crc);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_fd(rfd);

    /* close the read fd.  If it's not at EOF, then upstream will get EPIPE, which will hopefully
     * kill it and complete the cancellation */
    close_read_fd(self);

    /* close the fd we've been writing, as an EOF signal to downstream */
    close_write_fd(self);

    g_debug("read_and_write upstream CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    g_debug("sending XMSG_CRC message");
    msg = xmsg_new(elt->upstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    g_debug("read_and_write downstream CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    g_debug("sending XMSG_CRC message");
    msg = xmsg_new(elt->downstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    amfree(buf);
}

static void
read_and_push(
    XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    int fd = get_read_fd(self);
    XMsg *msg;

    crc32_init(&elt->crc);

    while (!elt->cancelled) {
	char *buf = g_malloc(GLUE_BUFFER_SIZE);
	gsize len;
	int read_error;

	/* read a buffer from upstream */
	len = read_fully(fd, buf, GLUE_BUFFER_SIZE, &read_error);
	if (len < GLUE_BUFFER_SIZE) {
	    if (read_error) {
		if (!elt->cancelled) {
		    xfer_cancel_with_error(elt,
			_("Error reading from fd %d: %s"), fd, strerror(read_error));
		    g_debug("element-glue: error reading from fd %d: %s",
                         fd, strerror(read_error));
		    wait_until_xfer_cancelled(elt->xfer);
		}
                amfree(buf);
		break;
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		amfree(buf);
		break;
	    }
	}
	crc32_add((uint8_t *)buf, len, &elt->crc);

	xfer_element_push_buffer(elt->downstream, buf, len);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_fd(fd);

    /* send an EOF indication downstream */
    xfer_element_push_buffer(elt->downstream, NULL, 0);

    /* close the read fd, since it's at EOF */
    close_read_fd(self);

    g_debug("sending XMSG_CRC message");
    g_debug("read_and_push CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->upstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);
}

static void
read_and_push_static(
    XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    int fd = get_read_fd(self);
    XMsg *msg;
    char *buf = g_malloc(GLUE_BUFFER_SIZE);

    g_debug("read_and_push_static");
    crc32_init(&elt->crc);

    while (!elt->cancelled) {
	gsize len;
	int read_error;

	/* read a buffer from upstream */
	len = read_fully(fd, buf, GLUE_BUFFER_SIZE, &read_error);
	if (len < GLUE_BUFFER_SIZE) {
	    if (read_error) {
		if (!elt->cancelled) {
		    xfer_cancel_with_error(elt,
			_("Error reading from fd %d: %s"), fd, strerror(read_error));
		    g_debug("element-glue: error reading from fd %d: %s",
                         fd, strerror(read_error));
		    wait_until_xfer_cancelled(elt->xfer);
		}
                amfree(buf);
		break;
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		amfree(buf);
		break;
	    }
	}
	crc32_add((uint8_t *)buf, len, &elt->crc);

	xfer_element_push_buffer_static(elt->downstream, buf, len);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_fd(fd);

    /* send an EOF indication downstream */
    xfer_element_push_buffer_static(elt->downstream, NULL, 0);

    /* close the read fd, since it's at EOF */
    close_read_fd(self);

    g_debug("sending XMSG_CRC message");
    g_debug("read_and_push_static CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->upstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);
}

static void
read_to_mem_ring(
    XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    int fd = get_read_fd(self);
    XMsg *msg;
    uint64_t read_offset;
    uint64_t write_offset;
    uint64_t producer_block_size;
    uint64_t consumer_block_size;
    uint64_t mem_ring_size;

    g_debug("read_to_mem_ring");
    mem_ring_producer_set_size(self->mem_ring, GLUE_BUFFER_SIZE*4, GLUE_BUFFER_SIZE);
    mem_ring_size = self->mem_ring->ring_size;
    producer_block_size = self->mem_ring->producer_block_size;
    consumer_block_size = self->mem_ring->consumer_block_size;
    crc32_init(&elt->crc);

    while (!elt->cancelled) {
	gsize len;
	gsize len2;
	int read_error;

	g_mutex_lock(self->mem_ring->mutex);
	write_offset = self->mem_ring->write_offset;
        read_offset = self->mem_ring->read_offset;
	g_mutex_unlock(self->mem_ring->mutex);
	while (!(write_offset == read_offset) &&
	       !((write_offset < read_offset) &&
		 (read_offset - write_offset > producer_block_size)) &&
	       !((write_offset > read_offset) &&
		 (mem_ring_size - write_offset + read_offset > producer_block_size))) {
	    if (elt->cancelled) {
		goto return_eof;
	    }
	    g_mutex_lock(self->mem_ring->mutex);
	    g_cond_wait(self->mem_ring->free_cond, self->mem_ring->mutex);
	    write_offset = self->mem_ring->write_offset;
	    read_offset = self->mem_ring->read_offset;
	    g_mutex_unlock(self->mem_ring->mutex);
	}

	/* read a buffer from upstream */
	if (write_offset + self->mem_ring->producer_block_size <= mem_ring_size) {
	    len = read_fully(fd, self->mem_ring->buffer+write_offset, producer_block_size, &read_error);
	    if (len > 0) {
		crc32_add((uint8_t *)self->mem_ring->buffer+write_offset, len, &elt->crc);
		write_offset += len;
		write_offset %= mem_ring_size;
		self->mem_ring->data_avail += len;
		g_mutex_lock(self->mem_ring->mutex);
		self->mem_ring->written += len;
		self->mem_ring->write_offset = write_offset;
		if (self->mem_ring->data_avail >= consumer_block_size) {
		    g_cond_broadcast(self->mem_ring->add_cond);
		    self->mem_ring->data_avail -= consumer_block_size;
		}
		g_mutex_unlock(self->mem_ring->mutex);
	    }
	    if (len < producer_block_size) {
		if (read_error) {
		    if (!elt->cancelled) {
			xfer_cancel_with_error(elt,
			    _("Error reading from fd %d: %s"), fd, strerror(read_error));
			g_debug("element-glue: error reading from fd %d: %s",
                             fd, strerror(read_error));
			wait_until_xfer_cancelled(elt->xfer);
		    }
		    break;
		} else if (len == 0) { /* we only count a zero-length read as EOF */
		    break;
		}
	    }
	} else {
	    len = read_fully(fd, self->mem_ring->buffer+write_offset, mem_ring_size - write_offset, &read_error);
	    if (len > 0) {
		crc32_add((uint8_t *)self->mem_ring->buffer+write_offset, len, &elt->crc);
	    }
	    len2 = 0;
	    if (len == mem_ring_size - write_offset) {
		len2 = read_fully(fd, self->mem_ring->buffer, producer_block_size - (mem_ring_size - write_offset), &read_error);
		if (len2 > 0) {
		    crc32_add((uint8_t *)self->mem_ring->buffer, len2, &elt->crc);
		    len += len2;
		}
	    }
	    if (len > 0) {
		write_offset += len;
		write_offset %= mem_ring_size;
		g_mutex_lock(self->mem_ring->mutex);
		self->mem_ring->write_offset = write_offset;
		self->mem_ring->data_avail += len;
		if (self->mem_ring->data_avail >= consumer_block_size) {
		    g_cond_broadcast(self->mem_ring->add_cond);
		    self->mem_ring->data_avail -= consumer_block_size;
		}
		g_mutex_unlock(self->mem_ring->mutex);
	    }
	    if (len < producer_block_size) {
		if (read_error) {
		    if (!elt->cancelled) {
			xfer_cancel_with_error(elt,
			    _("Error reading from fd %d: %s"), fd, strerror(read_error));
			g_debug("element-glue: error reading from fd %d: %s",
                             fd, strerror(read_error));
			wait_until_xfer_cancelled(elt->xfer);
		    }
		    break;
		} else if (len == 0 || len2 == 0) { /* we only count a zero-length read as EOF */
		    break;
		}
	    }
	}
    }

return_eof:
    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_fd(fd);

    /* send an EOF indication downstream */
    g_mutex_lock(self->mem_ring->mutex);
    self->mem_ring->eof_flag = TRUE;
    g_cond_broadcast(self->mem_ring->add_cond);
    g_mutex_unlock(self->mem_ring->mutex);

    /* close the read fd, since it's at EOF */
    close_read_fd(self);

    g_debug("sending XMSG_CRC message");
    g_debug("read_to_mem_ring CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->upstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);
}

static void
read_to_shm_ring(
    XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    int fd = get_read_fd(self);
    XMsg *msg;
    uint64_t write_offset;
    uint64_t written;
    uint64_t readx;
    uint64_t shm_ring_size;
    struct iovec iov[2];
    int          iov_count;
    ssize_t      n;
    size_t      consumer_block_size;

    g_debug("read_to_shm_ring");

    elt->shm_ring = shm_ring_link(xfer_element_get_shm_ring(elt->downstream)->shm_control_name);
    shm_ring_producer_set_size(elt->shm_ring, GLUE_BUFFER_SIZE*4, GLUE_BUFFER_SIZE);
    shm_ring_size = elt->shm_ring->mc->ring_size;
    consumer_block_size = elt->shm_ring->mc->consumer_block_size;
    crc32_init(&elt->crc);

    while (!elt->cancelled && !elt->shm_ring->mc->cancelled) {
	write_offset = elt->shm_ring->mc->write_offset;
	written = elt->shm_ring->mc->written;
	while (!elt->cancelled && !elt->shm_ring->mc->cancelled) {
	    readx = elt->shm_ring->mc->readx;
	    if (shm_ring_size - (written - readx) > elt->shm_ring->block_size)
		break;
	    if (shm_ring_sem_wait(elt->shm_ring, elt->shm_ring->sem_write) != 0)
		break;
	}

	if (elt->cancelled || elt->shm_ring->mc->cancelled) {
	    break;
	}
	iov[0].iov_base = elt->shm_ring->data + write_offset;
	if (write_offset + elt->shm_ring->block_size <= shm_ring_size) {
	    iov[0].iov_len = elt->shm_ring->block_size;
	    iov_count = 1;
	} else {
	    iov[0].iov_len = shm_ring_size - write_offset;
	    iov[1].iov_base = elt->shm_ring->data;
	    iov[1].iov_len = elt->shm_ring->block_size - iov[0].iov_len;
	    iov_count = 2;
	}

	n = readv(fd, iov, iov_count);
	if (n > 0) {

	    write_offset += n;
	    write_offset %= shm_ring_size;
	    elt->shm_ring->mc->write_offset = write_offset;
	    elt->shm_ring->mc->written += n;
	    elt->shm_ring->data_avail += n;
	    if (elt->shm_ring->data_avail >= consumer_block_size) {
		sem_post(elt->shm_ring->sem_read);
		elt->shm_ring->data_avail -= consumer_block_size;
	    }
	    if (n <= (ssize_t)iov[0].iov_len) {
		crc32_add((uint8_t *)iov[0].iov_base, n, &elt->crc);
	    } else {
		crc32_add((uint8_t *)iov[0].iov_base, iov[0].iov_len, &elt->crc);
		crc32_add((uint8_t *)iov[1].iov_base, n - iov[0].iov_len, &elt->crc);
	    }
	} else {
	    elt->shm_ring->mc->eof_flag = TRUE;
	    break;
	}
    }

    if (elt->cancelled) {
	elt->shm_ring->mc->cancelled = TRUE;
	g_debug("read_to_shm_ring: cancel shm-ring because elt cancelled");
    } else if (elt->shm_ring->mc->cancelled) {
	xfer_cancel_with_error(elt, "shm_ring cancelled");
    }

    sem_post(elt->shm_ring->sem_read);
    sem_post(elt->shm_ring->sem_read);

    // wait for the consumer to read everything
    while (!elt->cancelled &&
	   !elt->shm_ring->mc->cancelled &&
	   (elt->shm_ring->mc->written != elt->shm_ring->mc->readx ||
	    !elt->shm_ring->mc->eof_flag)) {
	if (shm_ring_sem_wait(elt->shm_ring, elt->shm_ring->sem_write) != 0)
	    break;
    }

    /* close the read fd, since it's at EOF */
    close_read_fd(self);

    g_debug("sending XMSG_CRC message");
    g_debug("read_to_shm_ring CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->upstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    close_producer_shm_ring(elt->shm_ring);
    elt->shm_ring = NULL;
    return;
}

static void
pull_static_to_shm_ring(
    XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    XMsg *msg;
    uint64_t write_offset;
    uint64_t written;
    uint64_t readx;
    uint64_t shm_ring_size;
    size_t   block_size;
    size_t   len;
    size_t   consumer_block_size;
    gpointer base;

    g_debug("pull_static_to_shm_ring");

    elt->shm_ring = shm_ring_link(xfer_element_get_shm_ring(elt->downstream)->shm_control_name);
    shm_ring_producer_set_size(elt->shm_ring, GLUE_BUFFER_SIZE*4, GLUE_BUFFER_SIZE);
    shm_ring_size = elt->shm_ring->mc->ring_size;
    consumer_block_size = elt->shm_ring->mc->consumer_block_size;
    crc32_init(&elt->crc);

    while (!elt->cancelled && !elt->shm_ring->mc->cancelled) {
	write_offset = elt->shm_ring->mc->write_offset;
	written = elt->shm_ring->mc->written;

	while (!elt->cancelled && !elt->shm_ring->mc->cancelled) {
	    readx = elt->shm_ring->mc->readx;
	    if (shm_ring_size - (written - readx) > elt->shm_ring->block_size)
		break;
	    if (shm_ring_sem_wait(elt->shm_ring, elt->shm_ring->sem_write) != 0)
		break;
	}

	if (elt->cancelled || elt->shm_ring->mc->cancelled)
	    break;

	if (write_offset + elt->shm_ring->block_size <= shm_ring_size) {
	    block_size = elt->shm_ring->block_size;
	} else {
	    block_size = shm_ring_size - write_offset;
	}
	base = elt->shm_ring->data + write_offset;
	xfer_element_pull_buffer_static(elt->upstream, base, block_size, &len);

	if (len > 0) {
	    write_offset += len;
	    write_offset %= shm_ring_size;
	    elt->shm_ring->mc->write_offset = write_offset;
	    elt->shm_ring->mc->written += len;
	    elt->shm_ring->data_avail += len;
	    if (elt->shm_ring->data_avail >= consumer_block_size) {
		sem_post(elt->shm_ring->sem_read);
		elt->shm_ring->data_avail -= consumer_block_size;
	    }
	    crc32_add((uint8_t *)base, len, &elt->crc);
	} else {
	    elt->shm_ring->mc->eof_flag = TRUE;
	    break;
	}
    }

    if (elt->cancelled) {
	elt->shm_ring->mc->cancelled = TRUE;
	g_debug("pull_static_to_shm_ring: cancel shm-ring because elt cancelled");
    } else if (elt->shm_ring->mc->cancelled) {
	xfer_cancel_with_error(elt, "shm_ring cancelled");
    }
    sem_post(elt->shm_ring->sem_read); // for the last block
    sem_post(elt->shm_ring->sem_read); // for the eof_flag

    // wait for the consumer to read everything
    while (!elt->cancelled &&
	   !elt->shm_ring->mc->cancelled &&
	   (elt->shm_ring->mc->written != elt->shm_ring->mc->readx ||
	    !elt->shm_ring->mc->eof_flag)) {
	if (shm_ring_sem_wait(elt->shm_ring, elt->shm_ring->sem_write) != 0)
	    break;
    }

    g_debug("sending XMSG_CRC message");
    g_debug("pull_static_to_shm_ring CRC: %08x      size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(elt->upstream, XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    return;
}

static void
shm_ring_and_push_buffer_static(XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    uint64_t read_offset;
    uint64_t shm_ring_size;
    gsize    usable = 0;
    gboolean eof_flag = FALSE;

    g_debug("shm_ring_and_push_buffer_static");

    shm_ring_consumer_set_size(elt->shm_ring, SHM_RING_SIZE, SHM_RING_BLOCK_SIZE);
    shm_ring_size = elt->shm_ring->mc->ring_size;
    sem_post(elt->shm_ring->sem_write);
    while (!elt->shm_ring->mc->cancelled) {
	do {
	    usable = elt->shm_ring->mc->written - elt->shm_ring->mc->readx;
	    eof_flag = elt->shm_ring->mc->eof_flag;
            if (shm_ring_sem_wait(elt->shm_ring, elt->shm_ring->sem_read) != 0)
                break;
        } while (!elt->shm_ring->mc->cancelled &&
                 usable < elt->shm_ring->block_size && !eof_flag);
	read_offset = elt->shm_ring->mc->read_offset;

	while (usable >= elt->shm_ring->block_size || eof_flag) {
	    gsize to_write = usable;
            if (to_write > elt->shm_ring->block_size)
                to_write = elt->shm_ring->block_size;

	    if (to_write > 0) {
		xfer_element_push_buffer_static(elt->downstream,
						elt->shm_ring->data +read_offset,
						to_write);
	    }

	    if (to_write) {
		read_offset += to_write;
		if (read_offset >= shm_ring_size)
		    read_offset -= shm_ring_size;
		elt->shm_ring->mc->read_offset = read_offset;
		elt->shm_ring->mc->readx += to_write;
		sem_post(elt->shm_ring->sem_write);
		usable -= to_write;
	    }
	    if (elt->shm_ring->mc->write_offset == elt->shm_ring->mc->read_offset &&
                elt->shm_ring->mc->eof_flag) {
		// notify the producer that everythinng is read
		xfer_element_push_buffer_static(elt->downstream, NULL, 0);
		sem_post(elt->shm_ring->sem_write);
		return;
	    }
	}
    }
}

static void
pull_and_push(XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    gboolean eof_sent = FALSE;

    g_debug("pull_and_push");

    while (!elt->cancelled) {
	char *buf;
	size_t len;

	/* get a buffer from upstream */
	buf = xfer_element_pull_buffer(elt->upstream, &len);

	/* and push it downstream */
	xfer_element_push_buffer(elt->downstream, buf, len);

	if (!buf) {
	    eof_sent = TRUE;
	    break;
	}
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_buffers(elt->upstream);

    if (!eof_sent)
	xfer_element_push_buffer(elt->downstream, NULL, 0);
}

static void
pull_and_push_static(
    XferElementGlue *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    gboolean eof_sent = FALSE;
    size_t block_size_up = xfer_element_get_block_size(elt->upstream);
    size_t block_size_down = xfer_element_get_block_size(elt->downstream);
    size_t block_size;
    char  *buf;

    g_debug("pull_and_push_static");
    if (block_size_up != 0 && block_size_down != 0 && block_size_up != block_size_down) {
	g_critical("pull_and_push_static with different block_size (%zu, %zu)", block_size_up, block_size_down);
    }
    if (block_size_up != 0)
	block_size = block_size_up;
    else if (block_size_down != 0)
	block_size = block_size_down;
    else
	block_size = NETWORK_BLOCK_BYTES;

    buf = malloc(block_size);

    while (!elt->cancelled) {
	size_t len;

	/* get a buffer from upstream */
	xfer_element_pull_buffer_static(elt->upstream, buf, block_size, &len);

	/* and push it downstream */
	if (len == 0) {
	    xfer_element_push_buffer_static(elt->downstream, NULL, len);
	    eof_sent = TRUE;
	    break;
	} else {
	    xfer_element_push_buffer_static(elt->downstream, buf, len);
	}
    }
    amfree(buf);

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_buffers(elt->upstream);

    if (!eof_sent)
	xfer_element_push_buffer_static(elt->downstream, NULL, 0);
}

static gpointer
worker_thread(
    gpointer data)
{
    XferElement *elt = XFER_ELEMENT(data);
    XferElementGlue *self = XFER_ELEMENT_GLUE(data);

    switch (mech_pair(elt->input_mech, elt->output_mech)) {
    case mech_pair(XFER_MECH_READFD, XFER_MECH_WRITEFD):
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER):
	read_and_push(self);
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER_STATIC):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER_STATIC):
	read_and_push_static(self);
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_MEM_RING):
	read_to_mem_ring(self);
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_SHM_RING):
	read_to_shm_ring(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_WRITEFD):
	pull_and_write(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_WRITEFD):
	pull_static_and_write(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_PUSH_BUFFER):
	pull_and_push(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_PUSH_BUFFER_STATIC):
	pull_and_push_static(self);
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_DIRECTTCP_LISTEN):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_DIRECTTCP_LISTEN):
	if ((self->output_data_socket = do_directtcp_connect(self,
				    elt->downstream->input_listen_addrs)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_DIRECTTCP_LISTEN):
	if ((self->output_data_socket = do_directtcp_connect(self,
				    elt->downstream->input_listen_addrs)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	pull_and_write(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_DIRECTTCP_LISTEN):
	if ((self->output_data_socket = do_directtcp_connect(self,
				    elt->downstream->input_listen_addrs)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	pull_static_and_write(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_WRITEFD):
	if ((self->input_data_socket = do_directtcp_accept(self, &self->input_listen_socket)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PUSH_BUFFER):
	if ((self->input_data_socket = do_directtcp_accept(self,
					    &self->input_listen_socket)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_push(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PUSH_BUFFER_STATIC):
	if ((self->input_data_socket = do_directtcp_accept(self,
					    &self->input_listen_socket)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_push_static(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_MEM_RING):
	if ((self->input_data_socket = do_directtcp_accept(self, &self->input_listen_socket)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_to_mem_ring(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_SHM_RING):
	if ((self->input_data_socket = do_directtcp_accept(self, &self->input_listen_socket)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_to_shm_ring(self);
	break;

    /* The following pair have no glue threads */
    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PULL_BUFFER_STATIC):
    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PULL_BUFFER_STATIC):
    case mech_pair(XFER_MECH_READFD, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER_STATIC):
    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_WRITEFD):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_WRITEFD):
    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_PULL_BUFFER_STATIC):
    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_DIRECTTCP_LISTEN):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_DIRECTTCP_LISTEN):
    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_DIRECTTCP_CONNECT):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_DIRECTTCP_CONNECT):
    default:
	g_debug("Worker no thread: %d %d", elt->input_mech, elt->output_mech);
	g_assert_not_reached();
	break;

//    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_MEM_RING):
//	pull_static_to_mem_ring(self);
//	break;

    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_SHM_RING):
	pull_static_to_shm_ring(self);
	break;

    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_DIRECTTCP_CONNECT):
    case mech_pair(XFER_MECH_READFD, XFER_MECH_DIRECTTCP_CONNECT):
	if ((self->output_data_socket = do_directtcp_accept(self,
					    &self->output_listen_socket)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_WRITEFD):
    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_READFD):
	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PUSH_BUFFER):
	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_push(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PUSH_BUFFER_STATIC):
	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_push_static(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_MEM_RING):
	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_to_mem_ring(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_SHM_RING):
	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_to_shm_ring(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_DIRECTTCP_CONNECT):
	if ((self->output_data_socket = do_directtcp_accept(self,
					    &self->output_listen_socket)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	pull_and_write(self);
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_DIRECTTCP_CONNECT):
	if ((self->output_data_socket = do_directtcp_accept(self,
					    &self->output_listen_socket)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	pull_static_and_write(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_DIRECTTCP_CONNECT):
	/* TODO: use async accept's here to avoid order dependency */
	if ((self->output_data_socket = do_directtcp_accept(self,
					    &self->output_listen_socket)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	if ((self->input_data_socket = do_directtcp_accept(self,
					    &self->input_listen_socket)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_DIRECTTCP_LISTEN):
	/* TODO: use async connects and select() to avoid order dependency here */
	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1)
	    break;
	self->read_fdp = &self->input_data_socket;
	if ((self->output_data_socket = do_directtcp_connect(self,
				    elt->downstream->input_listen_addrs)) == -1)
	    break;
	self->write_fdp = &self->output_data_socket;
	read_and_write(self);
	break;

    case mech_pair(XFER_MECH_SHM_RING, XFER_MECH_PUSH_BUFFER_STATIC):
	shm_ring_and_push_buffer_static(self);
	break;

    }

    send_xfer_done(self);

    return NULL;
}

/*
 * Implementation
 */

static gboolean
setup_impl(
    XferElement *elt)
{
    XferElementGlue *self = (XferElementGlue *)elt;
    gboolean need_ring = FALSE;
    gboolean need_listen_input = FALSE;
    gboolean need_listen_output = FALSE;

    g_assert(elt->input_mech != XFER_MECH_NONE);
    g_assert(elt->output_mech != XFER_MECH_NONE);
    g_assert(elt->input_mech != elt->output_mech);

    self->read_fdp = NULL;
    self->write_fdp = NULL;
    self->on_push = PUSH_INVALID;
    self->on_pull = PULL_INVALID;
    self->need_thread = FALSE;

    g_debug("setup_impl: %d, %d", elt->input_mech, elt->output_mech);
    switch (mech_pair(elt->input_mech, elt->output_mech)) {
    case mech_pair(XFER_MECH_READFD, XFER_MECH_WRITEFD):
	/* thread will read from one fd and write to the other */
	self->read_fdp = &neighboring_element_fd;
	self->write_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER):
    case mech_pair(XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER_STATIC):
	/* thread will read from one fd and call push_buffer downstream */
	self->read_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_READFD, XFER_MECH_PULL_BUFFER_STATIC):
	self->read_fdp = &neighboring_element_fd;
	self->on_pull = PULL_FROM_FD;
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_DIRECTTCP_LISTEN):
	/* thread will connect for output, then read from fd and write to the
	 * socket. */
	self->read_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_DIRECTTCP_CONNECT):
	/* thread will accept output conn, then read from upstream and write to socket */
	self->read_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	need_listen_output = TRUE;
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_MEM_RING):
	self->read_fdp = &neighboring_element_fd;
	self->mem_ring = create_mem_ring();
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_READFD, XFER_MECH_SHM_RING):
	self->read_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_READFD):
	make_pipe(self);
	g_assert(xfer_element_swap_input_fd(elt, self->pipe[1]) == -1);
	self->pipe[1] = -1; /* upstream will close this for us */
	g_assert(xfer_element_swap_output_fd(elt, self->pipe[0]) == -1);
	self->pipe[0] = -1; /* downstream will close this for us */
	break;

    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER_STATIC):
	/* thread will read from pipe and call downstream's push_buffer */
	make_pipe(self);
	g_assert(xfer_element_swap_input_fd(elt, self->pipe[1]) == -1);
	self->pipe[1] = -1; /* upstream will close this for us */
	self->read_fdp = &self->pipe[0];
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER_STATIC):
	make_pipe(self);
	g_assert(xfer_element_swap_input_fd(elt, self->pipe[1]) == -1);
	self->pipe[1] = -1; /* upstream will close this for us */
	self->on_pull = PULL_FROM_FD;
	self->read_fdp = &self->pipe[0];
	break;

    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_DIRECTTCP_LISTEN):
	/* thread will connect for output, then read from pipe and write to socket */
	make_pipe(self);
	g_assert(xfer_element_swap_input_fd(elt, self->pipe[1]) == -1);
	self->pipe[1] = -1; /* upstream will close this for us */
	self->read_fdp = &self->pipe[0];
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_WRITEFD, XFER_MECH_DIRECTTCP_CONNECT):
	/* thread will accept output conn, then read from pipe and write to socket */
	make_pipe(self);
	g_assert(xfer_element_swap_input_fd(elt, self->pipe[1]) == -1);
	self->pipe[1] = -1; /* upstream will close this for us */
	self->read_fdp = &self->pipe[0];
	self->need_thread = TRUE;
	need_listen_output = TRUE;
	break;

    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_READFD):
	make_pipe(self);
	g_assert(xfer_element_swap_output_fd(elt, self->pipe[0]) == -1);
	self->pipe[0] = -1; /* downstream will close this for us */
	self->on_push = PUSH_TO_FD;
	self->write_fdp = &self->pipe[1];
	break;

    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_WRITEFD):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_WRITEFD):
	self->on_push = PUSH_TO_FD;
	self->write_fdp = &neighboring_element_fd;
	break;

    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_PULL_BUFFER):
	self->on_push = PUSH_TO_RING_BUFFER;
	self->on_pull = PULL_FROM_RING_BUFFER;
	need_ring = TRUE;
	break;

    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_DIRECTTCP_LISTEN):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_DIRECTTCP_LISTEN):
	/* push will connect for output first */
	self->on_push = PUSH_TO_FD | PUSH_CONNECT_FIRST;
	break;

    case mech_pair(XFER_MECH_PUSH_BUFFER, XFER_MECH_DIRECTTCP_CONNECT):
    case mech_pair(XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_DIRECTTCP_CONNECT):
	/* push will accept for output first */
	self->on_push = PUSH_TO_FD | PUSH_ACCEPT_FIRST;
	need_listen_output = TRUE;
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_READFD):
    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_READFD):
	/* thread will pull from upstream and write to pipe */
	make_pipe(self);
	g_assert(xfer_element_swap_output_fd(elt, self->pipe[0]) == -1);
	self->pipe[0] = -1; /* downstream will close this for us */
	self->write_fdp = &self->pipe[1];
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_WRITEFD):
    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_WRITEFD):
	/* thread will pull from upstream and write to downstream */
	self->write_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_PUSH_BUFFER):
    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_PUSH_BUFFER_STATIC):
	/* thread will pull from upstream and push to downstream */
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_DIRECTTCP_LISTEN):
    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_DIRECTTCP_LISTEN):
	/* thread will connect for output, then pull from upstream and write to socket */
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_PULL_BUFFER, XFER_MECH_DIRECTTCP_CONNECT):
    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_DIRECTTCP_CONNECT):
	/* thread will accept for output, then pull from upstream and write to socket */
	self->need_thread = TRUE;
	need_listen_output = TRUE;
	break;

//    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_MEM_RING):
//	/* thread will pull_static from upstream and add to a mem_ring */
//	self->need_thread = TRUE;
//	break;

    case mech_pair(XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_SHM_RING):
	/* thread will pull_static from upstream and add to a mem_ring */
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_READFD):
	/* thread will accept for input, then read from socket and write to pipe */
	make_pipe(self);
	g_assert(xfer_element_swap_output_fd(elt, self->pipe[0]) == -1);
	self->pipe[0] = -1; /* downstream will close this for us */
	self->write_fdp = &self->pipe[1];
	self->need_thread = TRUE;
	need_listen_input = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_WRITEFD):
	/* thread will accept for input, then read from socket and write to downstream */
	self->write_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	need_listen_input = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PUSH_BUFFER):
    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PUSH_BUFFER_STATIC):
	/* thread will accept for input, then read from socket and push downstream */
	self->need_thread = TRUE;
	need_listen_input = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PULL_BUFFER_STATIC):
	/* first pull will accept for input, then read from socket */
	self->on_pull = PULL_FROM_FD | PULL_ACCEPT_FIRST;
	need_listen_input = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_DIRECTTCP_CONNECT):
	/* thread will accept on both sides, then copy from socket to socket */
	self->need_thread = TRUE;
	need_listen_input = TRUE;
	need_listen_output = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_MEM_RING):
	need_listen_input = TRUE;
	self->mem_ring = create_mem_ring();
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_MEM_RING):
	self->mem_ring = create_mem_ring();
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_SHM_RING):
	need_listen_input = TRUE;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_SHM_RING):
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_READFD):
	/* thread will connect for input, then read from socket and write to pipe */
	make_pipe(self);
	g_assert(xfer_element_swap_output_fd(elt, self->pipe[0]) == -1);
	self->pipe[0] = -1; /* downstream will close this for us */
	self->write_fdp = &self->pipe[1];
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_WRITEFD):
	/* thread will connect for input, then read from socket and write to downstream */
	self->write_fdp = &neighboring_element_fd;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PUSH_BUFFER):
    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PUSH_BUFFER_STATIC):
	/* thread will connect for input, then read from socket and push downstream */
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PULL_BUFFER):
    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PULL_BUFFER_STATIC):
	/* first pull will connect for input, then read from socket */
	self->on_pull = PULL_FROM_FD | PULL_CONNECT_FIRST;
	break;

    case mech_pair(XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_DIRECTTCP_LISTEN):
	/* thread will connect on both sides, then copy from socket to socket */
	self->on_pull = PULL_FROM_FD | PULL_ACCEPT_FIRST;
	self->need_thread = TRUE;
	break;

    case mech_pair(XFER_MECH_SHM_RING, XFER_MECH_PUSH_BUFFER_STATIC):
	elt->shm_ring = shm_ring_create();
	self->need_thread = TRUE;
	break;

    default:
	g_debug("setup_impl: %d, %d", elt->input_mech, elt->output_mech);
	g_assert_not_reached();
	break;
    }

    /* set up ring if desired */
    if (need_ring) {
	self->ring = g_try_malloc(sizeof(*self->ring) * GLUE_RING_BUFFER_SIZE);
	if (self->ring == NULL) {
	    xfer_cancel_with_error(elt, "Can't allocate memory for ring");
	    return FALSE;
	}
	self->ring_used_sem = amsemaphore_new_with_value(0);
	self->ring_free_sem = amsemaphore_new_with_value(GLUE_RING_BUFFER_SIZE);
    }

    if (need_listen_input) {
	if (!do_directtcp_listen(elt,
		    &self->input_listen_socket, &elt->input_listen_addrs))
	    return FALSE;
    }
    if (need_listen_output) {
	if (!do_directtcp_listen(elt,
		    &self->output_listen_socket, &elt->output_listen_addrs))
	    return FALSE;
    }

    return TRUE;
}

static mem_ring_t *
get_mem_ring_impl(
    XferElement *elt)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    return self->mem_ring;
}

static gboolean
start_impl(
    XferElement *elt)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    if (self->need_thread)
	self->thread = g_thread_create(worker_thread, (gpointer)self, TRUE, NULL);

    /* we're active if we have a thread that will eventually die */
    return self->need_thread;
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferElementGlue *self = XFER_ELEMENT_GLUE(elt);

    g_debug("pUll_buffer_impl");
    /* accept first, if required */
    if (self->on_pull & PULL_ACCEPT_FIRST) {
	/* don't accept the next time around */
	self->on_pull &= ~PULL_ACCEPT_FIRST;

	if (elt->cancelled) {
	    *size = 0;
	    return NULL;
	}

	if ((self->input_data_socket = do_directtcp_accept(self,
					    &self->input_listen_socket)) == -1) {
	    /* do_directtcp_accept already signalled an error; xfer
	     * is cancelled */
	    *size = 0;
	    return NULL;
	}

	/* read from this new socket */
	self->read_fdp = &self->input_data_socket;
    } else if (self->on_pull & PULL_CONNECT_FIRST) {
	/* or connect first, if required */
	/* don't connect the next time around */
	self->on_pull &= ~PULL_CONNECT_FIRST;

	if (elt->cancelled) {
	    *size = 0;
	    return NULL;
	}

	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1) {
	    /* do_directtcp_connect already signalled an error; xfer
	     * is cancelled */
	    *size = 0;
	    return NULL;
	}

	/* read from this new socket */
	self->read_fdp = &self->input_data_socket;
    }

    switch (self->on_pull) {
	case PULL_FROM_RING_BUFFER: {
	    gpointer buf;

	    if (elt->cancelled) {
		/* the finalize method will empty the ring buffer */
		*size = 0;
		return NULL;
	    }

	    /* make sure there's at least one element available */
	    amsemaphore_down(self->ring_used_sem);

	    /* get it */
	    buf = self->ring[self->ring_tail].buf;
	    *size = self->ring[self->ring_tail].size;
	    self->ring_tail = (self->ring_tail + 1) % GLUE_RING_BUFFER_SIZE;

	    /* and mark this element as free to be overwritten */
	    amsemaphore_up(self->ring_free_sem);

	    return buf;
	}

	case PULL_FROM_FD: {
	    int fd = get_read_fd(self);
	    char *buf;
	    ssize_t len;

	    /* if the fd is already closed, it's possible upstream bailed out
	     * so quickly that we didn't even get a look at the fd */
	    if (elt->cancelled || fd == -1) {
		if (fd != -1) {
		    if (elt->expect_eof)
			xfer_element_drain_fd(fd);

		    close_read_fd(self);
		}

		*size = 0;
		return NULL;
	    }

	    buf = g_malloc(GLUE_BUFFER_SIZE);

	    /* read from upstream */
	    len = read_fully(fd, buf, GLUE_BUFFER_SIZE, NULL);
	    if (len < GLUE_BUFFER_SIZE) {
		if (errno) {
		    if (!elt->cancelled) {
			xfer_cancel_with_error(elt,
			    _("Error reading from fd %d: %s"), fd, strerror(errno));
			wait_until_xfer_cancelled(elt->xfer);
		    }

		    /* return an EOF */
		    amfree(buf);
		    len = 0;

		    /* and finish off the upstream */
		    if (elt->expect_eof) {
			xfer_element_drain_fd(fd);
		    }
		    close_read_fd(self);
		} else if (len == 0) {
		    /* EOF */
		    g_free(buf);
		    buf = NULL;
		    *size = 0;

		    /* signal EOF to downstream */
		    close_read_fd(self);
		}
	    }

	    *size = (size_t)len;

	    return buf;
	}

	default:
	case PULL_INVALID:
	    g_assert_not_reached();
	    return NULL;
    }
}

static gpointer
pull_buffer_static_impl(
    XferElement *elt,
    gpointer buf,
    size_t block_size,
    size_t *size)
{
    XferElementGlue *self = XFER_ELEMENT_GLUE(elt);

    g_debug("pUll_buffer_impl");
    /* accept first, if required */
    if (self->on_pull & PULL_ACCEPT_FIRST) {
	/* don't accept the next time around */
	self->on_pull &= ~PULL_ACCEPT_FIRST;

	if (elt->cancelled) {
	    *size = 0;
	    return NULL;
	}

	if ((self->input_data_socket = do_directtcp_accept(self,
					    &self->input_listen_socket)) == -1) {
	    /* do_directtcp_accept already signalled an error; xfer
	     * is cancelled */
	    *size = 0;
	    return NULL;
	}

	/* read from this new socket */
	self->read_fdp = &self->input_data_socket;
    } else if (self->on_pull & PULL_CONNECT_FIRST) {
	/* or connect first, if required */
	/* don't connect the next time around */
	self->on_pull &= ~PULL_CONNECT_FIRST;

	if (elt->cancelled) {
	    *size = 0;
	    return NULL;
	}

	if ((self->input_data_socket = do_directtcp_connect(self,
				    elt->upstream->output_listen_addrs)) == -1) {
	    /* do_directtcp_connect already signalled an error; xfer
	     * is cancelled */
	    *size = 0;
	    return NULL;
	}

	/* read from this new socket */
	self->read_fdp = &self->input_data_socket;
    }

    switch (self->on_pull) {
	case PULL_FROM_RING_BUFFER: {
	    gpointer buf;
g_critical("PULL_FROM_RING_BUFFER unimplemented");

	    if (elt->cancelled) {
		/* the finalize method will empty the ring buffer */
		*size = 0;
		return NULL;
	    }

	    /* make sure there's at least one element available */
	    amsemaphore_down(self->ring_used_sem);

	    /* get it */
	    buf = self->ring[self->ring_tail].buf;
	    *size = self->ring[self->ring_tail].size;
	    self->ring_tail = (self->ring_tail + 1) % GLUE_RING_BUFFER_SIZE;

	    /* and mark this element as free to be overwritten */
	    amsemaphore_up(self->ring_free_sem);

	    return buf;
	}

	case PULL_FROM_FD: {
	    int fd = get_read_fd(self);
	    ssize_t len;

	    /* if the fd is already closed, it's possible upstream bailed out
	     * so quickly that we didn't even get a look at the fd */
	    if (elt->cancelled || fd == -1) {
		if (fd != -1) {
		    if (elt->expect_eof)
			xfer_element_drain_fd(fd);

		    close_read_fd(self);
		}

		*size = 0;
		return NULL;
	    }

	    /* read from upstream */
	    len = read_fully(fd, buf, block_size, NULL);
	    if (len < (ssize_t)block_size) {
		if (errno) {
		    if (!elt->cancelled) {
			xfer_cancel_with_error(elt,
			    _("Error reading from fd %d: %s"), fd, strerror(errno));
			wait_until_xfer_cancelled(elt->xfer);
		    }

		    /* return an EOF */
		    buf = NULL;
		    len = 0;

		    /* and finish off the upstream */
		    if (elt->expect_eof) {
			xfer_element_drain_fd(fd);
		    }
		    close_read_fd(self);
		} else if (len == 0) {
		    /* EOF */
		    buf = NULL;
		    len = 0;

		    /* signal EOF to downstream */
		    close_read_fd(self);
		}
	    }

	    *size = (size_t)len;

	    return buf;
	}

	default:
	case PULL_INVALID:
	    g_assert_not_reached();
	    return NULL;
    }
}

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t len)
{
    XferElementGlue *self = (XferElementGlue *)elt;
    XMsg *msg;

    g_debug("push_buffer_impl");
    /* accept first, if required */
    if (self->on_push & PUSH_ACCEPT_FIRST) {
	/* don't accept the next time around */
	self->on_push &= ~PUSH_ACCEPT_FIRST;

	if (elt->cancelled) {
	    return;
	}

	if ((self->output_data_socket = do_directtcp_accept(self,
					    &self->output_listen_socket)) == -1) {
	    /* do_directtcp_accept already signalled an error; xfer
	     * is cancelled */
	    return;
	}

	/* write to this new socket */
	self->write_fdp = &self->output_data_socket;
    }

    /* or connect first, if required */
    if (self->on_push & PUSH_CONNECT_FIRST) {
	/* don't accept the next time around */
	self->on_push &= ~PUSH_CONNECT_FIRST;

	if (elt->cancelled) {
	    return;
	}

	if ((self->output_data_socket = do_directtcp_connect(self,
				    elt->downstream->input_listen_addrs)) == -1) {
	    /* do_directtcp_connect already signalled an error; xfer
	     * is cancelled */
	    return;
	}

	/* read from this new socket */
	self->write_fdp = &self->output_data_socket;
    }

    switch (self->on_push) {
	case PUSH_TO_RING_BUFFER:
	    /* just drop packets if the transfer has been cancelled */
	    if (elt->cancelled) {
		amfree(buf);
		return;
	    }

	    /* make sure there's at least one element free */
	    amsemaphore_down(self->ring_free_sem);

	    /* set it */
	    self->ring[self->ring_head].buf = buf;
	    self->ring[self->ring_head].size = len;
	    self->ring_head = (self->ring_head + 1) % GLUE_RING_BUFFER_SIZE;

	    /* and mark this element as available for reading */
	    amsemaphore_up(self->ring_used_sem);

	    return;

	case PUSH_TO_FD: {
	    int fd = get_write_fd(self);

	    /* if the fd is already closed, it's possible upstream bailed out
	     * so quickly that we didn't even get a look at the fd.  In this
	     * case we can assume the xfer has been cancelled and just discard
	     * the data. */
	    if (fd == -1)
		return;

	    if (elt->cancelled) {
		if (!elt->expect_eof || !buf) {
		    close_write_fd(self);

		    /* hack to ensure we won't close the fd again, if we get another push */
		    elt->expect_eof = TRUE;
		}

		amfree(buf);

		return;
	    }

	    /* write the full buffer to the fd, or close on EOF */
	    if (buf) {
		if (!elt->downstream->drain_mode &&
		    full_write(fd, buf, len) < len) {
		    if (elt->downstream->must_drain) {
			g_debug("Error writing to fd %d: %s",
				fd, strerror(errno));
		    } else if (elt->downstream->ignore_broken_pipe &&
			       errno == EPIPE) {
		    } else {
			if (!elt->cancelled) {
			    xfer_cancel_with_error(elt,
				_("Error writing to fd %d: %s"),
				fd, strerror(errno));
			    wait_until_xfer_cancelled(elt->xfer);
			}
			/* nothing special to do to handle a cancellation */
		    }
		    elt->downstream->drain_mode = TRUE;
		}
		crc32_add((uint8_t *)buf, len, &elt->crc);
		amfree(buf);
	    } else {
		g_debug("sending XMSG_CRC message");
		g_debug("push_to_fd CRC: %08x", crc32_finish(&elt->crc));
		msg = xmsg_new(elt->downstream, XMSG_CRC, 0);
		msg->crc = crc32_finish(&elt->crc);
		msg->size = elt->crc.size;
		xfer_queue_message(elt->xfer, msg);

		close_write_fd(self);
	    }

	    return;
	}

	default:
	case PUSH_INVALID:
	    g_assert_not_reached();
	    break;
    }
}

static void
push_buffer_static_impl(
    XferElement *elt,
    gpointer buf,
    size_t len)
{
    XferElementGlue *self = (XferElementGlue *)elt;
    XMsg *msg;

    /* accept first, if required */
    if (self->on_push & PUSH_ACCEPT_FIRST) {
	/* don't accept the next time around */
	self->on_push &= ~PUSH_ACCEPT_FIRST;

	if (elt->cancelled) {
	    return;
	}

	if ((self->output_data_socket = do_directtcp_accept(self,
					    &self->output_listen_socket)) == -1) {
	    /* do_directtcp_accept already signalled an error; xfer
	     * is cancelled */
	    return;
	}

	/* write to this new socket */
	self->write_fdp = &self->output_data_socket;
    }

    /* or connect first, if required */
    if (self->on_push & PUSH_CONNECT_FIRST) {
	/* don't accept the next time around */
	self->on_push &= ~PUSH_CONNECT_FIRST;

	if (elt->cancelled) {
	    return;
	}

	if ((self->output_data_socket = do_directtcp_connect(self,
				    elt->downstream->input_listen_addrs)) == -1) {
	    /* do_directtcp_connect already signalled an error; xfer
	     * is cancelled */
	    return;
	}

	/* read from this new socket */
	self->write_fdp = &self->output_data_socket;
    }

    switch (self->on_push) {
	case PUSH_TO_RING_BUFFER:
	    /* just drop packets if the transfer has been cancelled */
	    if (elt->cancelled) {
		amfree(buf);
		return;
	    }
g_critical("PUSH_TO_RING_BUFFER not implemented");
	    /* make sure there's at least one element free */
	    amsemaphore_down(self->ring_free_sem);

	    /* set it */
	    self->ring[self->ring_head].buf = buf;
	    self->ring[self->ring_head].size = len;
	    self->ring_head = (self->ring_head + 1) % GLUE_RING_BUFFER_SIZE;

	    /* and mark this element as available for reading */
	    amsemaphore_up(self->ring_used_sem);

	    return;

	case PUSH_TO_FD: {
	    int fd = get_write_fd(self);

	    /* if the fd is already closed, it's possible upstream bailed out
	     * so quickly that we didn't even get a look at the fd.  In this
	     * case we can assume the xfer has been cancelled and just discard
	     * the data. */
	    if (fd == -1)
		return;

	    if (elt->cancelled) {
		if (!elt->expect_eof || !buf) {
		    close_write_fd(self);

		    /* hack to ensure we won't close the fd again, if we get another push */
		    elt->expect_eof = TRUE;
		}

		return;
	    }

	    /* write the full buffer to the fd, or close on EOF */
	    if (buf) {
		if (!elt->downstream->drain_mode &&
		    full_write(fd, buf, len) < len) {
		    if (elt->downstream->must_drain) {
			g_debug("Error writing to fd %d: %s",
				fd, strerror(errno));
		    } else if (elt->downstream->ignore_broken_pipe &&
			       errno == EPIPE) {
		    } else {
			if (!elt->cancelled) {
			    xfer_cancel_with_error(elt,
				_("Error writing to fd %d: %s"),
				fd, strerror(errno));
			    wait_until_xfer_cancelled(elt->xfer);
			}
			/* nothing special to do to handle a cancellation */
		    }
		    elt->downstream->drain_mode = TRUE;
		}
		crc32_add((uint8_t *)buf, len, &elt->crc);
	    } else {
		g_debug("sending XMSG_CRC message");
		g_debug("push_to_fd CRC: %08x", crc32_finish(&elt->crc));
		msg = xmsg_new(elt->downstream, XMSG_CRC, 0);
		msg->crc = crc32_finish(&elt->crc);
		msg->size = elt->crc.size;
		xfer_queue_message(elt->xfer, msg);

		close_write_fd(self);
	    }

	    return;
	}

	default:
	case PUSH_INVALID:
	    g_assert_not_reached();
	    break;
    }
}

static void
instance_init(
    XferElementGlue *self)
{
    XferElement *elt = (XferElement *)self;
    elt->can_generate_eof = TRUE;
    self->pipe[0] = self->pipe[1] = -1;
    self->input_listen_socket = -1;
    self->output_listen_socket = -1;
    self->input_data_socket = -1;
    self->output_data_socket = -1;
    self->read_fd = -1;
    self->write_fd = -1;
    crc32_init(&elt->crc);
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferElementGlue *self = XFER_ELEMENT_GLUE(obj_self);

    /* first make sure the worker thread has finished up */
    if (self->thread)
	g_thread_join(self->thread);

    /* close our pipes and fd's if they're still open */
    if (self->pipe[0] != -1) close(self->pipe[0]);
    if (self->pipe[1] != -1) close(self->pipe[1]);
    if (self->input_data_socket != -1) close(self->input_data_socket);
    if (self->output_data_socket != -1) close(self->output_data_socket);
    if (self->input_listen_socket != -1) close(self->input_listen_socket);
    if (self->output_listen_socket != -1) close(self->output_listen_socket);
    if (self->read_fd != -1) close(self->read_fd);
    if (self->write_fd != -1) close(self->write_fd);

    if (self->ring) {
	/* empty the ring buffer, ignoring syncronization issues */
	while (self->ring_used_sem->value) {
	    if (self->ring[self->ring_tail].buf)
		amfree(self->ring[self->ring_tail].buf);
	    self->ring_tail = (self->ring_tail + 1) % GLUE_RING_BUFFER_SIZE;
	}

	amfree(self->ring);
	amsemaphore_free(self->ring_used_sem);
	amsemaphore_free(self->ring_free_sem);
    }

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static xfer_element_mech_pair_t _pairs[] = {
    { XFER_MECH_READFD, XFER_MECH_WRITEFD, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy */
    { XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(1) }, /* read and call */
    { XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and call */
    { XFER_MECH_READFD, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* read on demand */
    { XFER_MECH_READFD, XFER_MECH_PULL_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* read on demand */
    { XFER_MECH_READFD, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy */
    { XFER_MECH_READFD, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy */
    { XFER_MECH_READFD, XFER_MECH_MEM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and add to mem ring */
    { XFER_MECH_READFD, XFER_MECH_SHM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and add to shm ring */

    { XFER_MECH_WRITEFD, XFER_MECH_READFD, XFER_NROPS(0), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* pipe */
    { XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(1) }, /* pipe + read and call*/
    { XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* pipe + read and call*/
    { XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* pipe + read on demand */
    { XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* pipe + read on demand */
    { XFER_MECH_WRITEFD, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* pipe + splice or copy*/
    { XFER_MECH_WRITEFD, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy + pipe */
    { XFER_MECH_WRITEFD, XFER_MECH_MEM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* pipe + read and add to mem ring */
    { XFER_MECH_WRITEFD, XFER_MECH_SHM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* pipe + read and add to shm ring */

    { XFER_MECH_PUSH_BUFFER, XFER_MECH_READFD, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand + pipe */
    { XFER_MECH_PUSH_BUFFER, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand */
    { XFER_MECH_PUSH_BUFFER, XFER_MECH_PULL_BUFFER, XFER_NROPS(0), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* async queue */
    { XFER_MECH_PUSH_BUFFER, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand */
    { XFER_MECH_PUSH_BUFFER, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand */

    { XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_READFD, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand + pipe */
    { XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand */
    { XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_PULL_BUFFER_STATIC, XFER_NROPS(0), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* async queue */
    { XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand */
    { XFER_MECH_PUSH_BUFFER_STATIC, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }, /* write on demand */

    { XFER_MECH_PULL_BUFFER, XFER_MECH_READFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write + pipe */
    { XFER_MECH_PULL_BUFFER, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write */
    { XFER_MECH_PULL_BUFFER, XFER_MECH_PUSH_BUFFER, XFER_NROPS(0), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and call */
    { XFER_MECH_PULL_BUFFER, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write */
    { XFER_MECH_PULL_BUFFER, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write */

    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_READFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write + pipe */
    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write */
    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_PUSH_BUFFER_STATIC, XFER_NROPS(0), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and call */
//    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write */
    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* call and write */
//    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_MEM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* pull and add to mem ring*/
    { XFER_MECH_PULL_BUFFER_STATIC, XFER_MECH_SHM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* pull and add to shm ring*/

    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_READFD, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy + pipe */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_WRITEFD, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PUSH_BUFFER, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(1) }, /* read and call */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PUSH_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and call */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* read on demand */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_PULL_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* read on demand */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_MEM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and add to mem ring */
    { XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_SHM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and add to shm ring */

    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_READFD, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy + pipe */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_WRITEFD, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy + pipe */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PUSH_BUFFER, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(1) }, /* read and call */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PUSH_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and call */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* read on demand */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_PULL_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) }, /* read on demand */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* splice or copy  */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_MEM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and add to mem ring */
    { XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_SHM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /* read and add to shm ring */

    { XFER_MECH_MEM_RING, XFER_MECH_READFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
    { XFER_MECH_MEM_RING, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
//TODO    { XFER_MECH_MEM_RING, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
//TODO    { XFER_MECH_MEM_RING, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */

    { XFER_MECH_SHM_RING, XFER_MECH_READFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
    { XFER_MECH_SHM_RING, XFER_MECH_WRITEFD, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
    { XFER_MECH_SHM_RING, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
    { XFER_MECH_SHM_RING, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */
    { XFER_MECH_SHM_RING, XFER_MECH_PUSH_BUFFER_STATIC, XFER_NROPS(0), XFER_NTHREADS(1), XFER_NALLOC(0) }, /*  */

    /* terminator */
    { XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0), XFER_NALLOC(0) },
};
xfer_element_mech_pair_t *xfer_element_glue_mech_pairs = _pairs;

static void
class_init(
    XferElementGlueClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);

    klass->setup = setup_impl;
    klass->start = start_impl;
    klass->get_mem_ring = get_mem_ring_impl;
    klass->push_buffer = push_buffer_impl;
    klass->push_buffer_static = push_buffer_static_impl;
    klass->pull_buffer = pull_buffer_impl;
    klass->pull_buffer_static = pull_buffer_static_impl;

    klass->perl_class = "Amanda::Xfer::Element::Glue";
    klass->mech_pairs = xfer_element_glue_mech_pairs;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_element_glue_get_type (void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (XferElementGlueClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferElementGlue),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferElementGlue", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_element_glue(void)
{
    XferElementGlue *self = (XferElementGlue *)g_object_new(XFER_ELEMENT_GLUE_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    return elt;
}
