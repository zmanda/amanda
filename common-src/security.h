/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
 * $Id: security.h,v 1.17 2006/05/26 14:00:58 martinea Exp $
 *
 * security api
 */
#ifndef SECURITY_H
#define	SECURITY_H

#include "packet.h"

struct security_handle;

/*
 * Overview
 *
 * The Security API consists of handles (also called connections), each of
 * which represents a connection to a particular host.  A handle is implemented
 * by a driver.  Each handle supports a packet-based communication protocol, as
 * well as an arbitrary number of bidirectional tcp-like streams.
 */

/*
 * This is a type that gets passed to the security_recvpkt() and
 * security_connect() callbacks. It details what the status of this callback
 * is.
 */
typedef enum {
    S_OK,	/* the pkt_t was received fine */
    S_TIMEOUT,	/* no pkt_t was received within the time specified in the
		 * timeout argument to security_recvpkt() */
    S_ERROR,	/* an error occurred during reception. Call security_geterror()
		 * for more information */
} security_status_t;

/*
 * Drivers
 */

/*
 * This structure defines a security driver.  This driver abstracts
 * common security actions behind a set of function pointers.  Macros
 * mask this.
 */
typedef struct security_driver {

    /*
     * The name of this driver, eg, "BSD", "BSDTCP", "KRB5", etc...  This is
     * used by security_getdriver() to associate a name with a driver type.
     */
    const char *name;

    /*
     * This is the implementation of security_connect(). It actually sets up
     * the connection, and then returns a structure describing the connection.
     * The first element of this structure MUST be a security_handle_t, because
     * it will be cast to that after it is passed up to the caller.
     *
     * The first argument is the host to connect to. The second argument is a
     * function to call when a connection is made. The third argument is passed
     * to the callback.
     *
     * The callback takes three arguments. The first is the caller supplied
     * void pointer. The second is a newly allocated security handle. The third
     * is a security_status_t flag indicating the success or failure of the
     * operation.
     */
    void (*connect)(const char *, char *(*)(char *, void *),
	    void (*)(void *, struct security_handle *, security_status_t),
	    void *, void *);

    /*
     * This form sets up a callback that returns new handles as they are
     * received.  It is passed the input and output file descriptors and a
     * callback. The callback takes a security handle argument and also an
     * initial packet received for that handle.
     */
    void (*accept)(const struct security_driver *, char *(*)(char *, void *),
	    int, int, void (*)(struct security_handle *, pkt_t *), void *);

    /* get the remote hostname */
    char *(*get_authenticated_peer_name)(struct security_handle *handle);

    /*
     * Frees up handles allocated by the previous methods
     */
    void (*close)(void *);

    /*
     * This transmits a packet after adding the security information
     * Returns 0 on success, negative on error.
     */
    ssize_t (*sendpkt)(void *, pkt_t *);

    /*
     * This creates an event in the event handler for receiving pkt_t's on a
     * security_handle.  The given callback will be called with the given arg
     * when the driver determines that it has data for that handle.  The last
     * argument is a timeout, in seconds.  This may be -1 to indicate no
     * timeout.  This method should assume that the caller will invoke
     * event_loop
     *
     * If there was an error or timeout, this will be indicated in the status
     * argument.
     *
     * Only one recvpkt request can exist per handle.
     */
    void (*recvpkt)(void *, void (*)(void *, pkt_t *, security_status_t), void
	    *, int);

    /*
     * Cancel an outstanding recvpkt request on a handle. Drivers should allow
     * this to be run even if no recvpkt was scheduled, or if one was
     * previously cancelled.
     */
    void (*recvpkt_cancel)(void *);

    /*
     * Get a stream given a security handle. This function returns a object
     * describing the stream. The first member of this object MUST be a
     * security_stream_t, because it will be cast to that.
     */
    void *(*stream_server)(void *);

    /*
     * Accept a stream created by stream_server
     */
    int (*stream_accept)(void *);

    /*
     * Get a stream and connect it to a remote given a security handle and a
     * stream id.  This function returns a object describing the stream. The
     * first member of this object MUST be a security_stream_t, because it will
     * be cast to that.
     */
    void *(*stream_client)(void *, int);

    /*
     * Close a stream opened with stream_server or stream_client
     */
    void (*stream_close)(void *);

    /*
     * Authenticate a stream.
     */
    int (*stream_auth)(void *);

    /*
     * Return a numeric id for a stream.  This is to be used by stream_client
     * on the other end of the connection to connect to this stream.
     */
    int (*stream_id)(void *);

    /*
     * Write to a stream.
     */
    int (*stream_write)(void *, const void *, size_t);

    /*
     * Read asyncronously from a stream.  Only one request can exist
     * per stream.
     */
    void (*stream_read)(void *, void (*)(void *, void *, ssize_t), void *);

    /*
     * Read syncronously from a stream.
     */
    ssize_t (*stream_read_sync)(void *, void **);

    /*
     * Cancel a stream read request
     */
    void (*stream_read_cancel)(void *);

    void (*close_connection)(void *, char *);

    int (*data_encrypt)(void *, void *, ssize_t, void **, ssize_t *);
    int (*data_decrypt)(void *, void *, ssize_t, void **, ssize_t *);

    ssize_t (*data_write)(void *, struct iovec *iov, int iovcnt);
    ssize_t (*data_read)(void *, void *, size_t, int timeout);
} security_driver_t;

/* Given a security type ("KRB4", "BSD", "SSH", etc), returns a pointer to that
 * type's security_driver_t, or NULL if no driver exists.  */
const security_driver_t *security_getdriver(const char *);

/*
 * Handles
 */

/*
 * This structure is a handle to a connection to a host for transmission
 * of protocol packets (pkt_t's).  The underlying security type defines
 * the actual protocol and transport.
 *
 * This handle is reference counted so that it can be used inside of
 * security streams after it has been closed by our callers.
 */
typedef struct security_handle {
    const security_driver_t *driver;
    char *error;
} security_handle_t;

/* void security_connect(
 *  const security_driver_t *driver,
 *  const char *hostname,
 *  char *(*conf_fn)(char *, void *),
 *  void (*fn)(void *, security_handle_t *, security_status_t),
 *  void *arg,
 *  void *datap);
 *
 * Given a security driver, and a hostname, calls back with a security_handle_t
 * that can be used to communicate with that host. The status arg to the
 * callback is reflects the success of the request. Error messages can be had
 * via security_geterror().  The conf_fn is used to determine configuration
 * information, with its second argument being the datap. If conf_fn is NULL,
 * no configuration information is available.
 */
#define	security_connect(driver, hostname, conf_fn, fn, arg, datap)	\
    (*(driver)->connect)(hostname, conf_fn, fn, arg, datap)

/* void security_accept(
 *  const security_driver_t *driver,
 *  char *(*conf_fn)(char *, void *),
 *  int in,
 *  int out,
 *  void (*fn)(security_handle_t *, pkt_t *),
 *  void *datap);
 *
 * Given a security driver, an input file descriptor, and an output file
 * descriptor, and a callback, when new connections are detected on the given
 * file descriptors, the function is called with a newly created security
 * handle and the initial packet received.  This is amandad's interface for
 * accepting incoming connections from the Amanda server. The file descriptors
 * are typically 0 and 1 (stdin/stdout).  This function uses the event
 * interface, and only works properly when event_loop() is called later in the
 * program.
 */
#define	security_accept(driver, conf_fn, in, out, fn, datap)	\
    (*(driver)->accept)(driver, conf_fn, in, out, fn, datap)

/* char *security_get_authenticated_peer_name(
 *  security_handle_t *handle);
 *
 * Returns the fully qualified, authenticated hostname of the peer, or
 * "localhost" for a local system.  The string is statically allocated and need
 * not be freed.  The string will never be NULL, but may be an empty string if
 * the remote identity is not known, not defined, or could not be
 * authenticated.
 */
#define	security_get_authenticated_peer_name(handle) \
    (*(handle)->driver->get_authenticated_peer_name)(handle)

/* Closes a security stream created by a security_connect() or
 * security_accept() and frees up resources associated with it. */
void security_close(security_handle_t *);

/* ssize_t security_sendpkt(security_handle_t *, const pkt_t *);
 *
 * Transmits a pkt_t over a security handle. Returns 0 on success, or negative
 * on error. A descriptive error message can be obtained via
 * security_geterror(). */
#define	security_sendpkt(handle, pkt)		\
    (*(handle)->driver->sendpkt)(handle, pkt)

/* void security_recvpkt(
 *  security_handle_t *handle,
 *  void (*fn)(void *, pkt_t *, security_status_t),
 *  void *arg,
 *  int timeout);
 *
 * Requests that when incoming packets arrive for this handle, the given
 * function is called with the given argument, the received packet, and the
 * status of the reception.  If a packet does not arrive within the number of
 * seconds specified in the 'timeout' argument, RECV_TIMEOUT is passed in the
 * status argument of the timeout.  On receive error, the callback's status
 * argument will be set to RECV_ERROR. An error message can be retrieved via
 * security_geterror().  On successful reception, RECV_OK will be passed in the
 * status argument, and the pkt argument will point to a valid packet.  This
 * function uses the event interface. Callbacks will only be generated when
 * event_loop() is called. */
#define	security_recvpkt(handle, fn, arg, timeout)	\
    (*(handle)->driver->recvpkt)(handle, fn, arg, timeout)

/* void security_recvpkt_cancel(security_handle_t *);
 *
 * Cancels a previous recvpkt request for this handle. */
#define	security_recvpkt_cancel(handle)		\
    (*(handle)->driver->recvpkt_cancel)(handle)

/* const char *security_geterror(security_handle_t *);
 *
 * Returns a descriptive error message for the last error condition on this
 * handle. */
#define	security_geterror(handle)	((handle)->error)

/* Sets the string that security_geterror() returns.  For use by security
 * drivers. */
void security_seterror(security_handle_t *, const char *, ...)
    G_GNUC_PRINTF(2, 3);

/* Initializes a security_handle_t. This is meant to be called only by security
 * drivers to initialize the common part of a newly allocated
 * security_handle_t.  */
void security_handleinit(security_handle_t *, const security_driver_t *);

/*
 * Streams
 */

/*
 * This structure is a handle to a stream connection to a host for
 * transmission of random data such as dumps or index data.
 */
typedef struct security_stream {
    const security_driver_t *driver;
    char *error;
} security_stream_t;

/* Initializes a security_stream_t. This is meant to be called only by security
 * drivers to initialize the common part of a newly allocated
 * security_stream_t. */
void security_streaminit(security_stream_t *, const security_driver_t *);

/* const char *security_stream_geterror(security_stream_t *);
 *
 * Returns a descriptive error message for the last error condition on this
 * stream. */
#define	security_stream_geterror(stream)	((stream)->error)

/* Sets the string that security_stream_geterror() returns. */
void security_stream_seterror(security_stream_t *, const char *, ...)
    G_GNUC_PRINTF(2, 3);

/* security_stream_t *security_stream_server(security_handle_t *);
 *
 * Creates the server end of a security stream, and will prepare to receive a
 * connection from the host on the other end of the security handle passed.
 * Returns a security_stream_t on success, and NULL on error. Error messages
 * can be obtained by calling security_geterror() on the security handle
 * associated with this stream. */
#define	security_stream_server(handle)	\
    (*(handle)->driver->stream_server)(handle)

/* int security_stream_accept(security_stream_t *);
 *
 * Given a security stream created by security_stream_server, blocks until a
 * connection is made from the remote end.  After calling stream_server,
 * stream_accept must be called on the stream before it is fully connected.
 * Returns 0 on success, and -1 on error. Error messages can be obtained by
 * calling security_stream_geterror().
 */
#define	security_stream_accept(stream)		\
    (*(stream)->driver->stream_accept)(stream)

/* security_stream_t *security_stream_client(security_handle_t *, int);
 *
 * Creates the client end of a security stream, and connects it to the machine
 * on the other end of the security handle. The 'id' argument identifies which
 * stream on the other end to connect to, and should have come from
 * security_stream_id on the other end of the connection.  Returns a
 * security_stream_t on success, and NULL on error. Error messages can be
 * obtained by calling security_geterror() on the security handle associated
 * with this stream. */
#define	security_stream_client(handle, id)	\
    (*(handle)->driver->stream_client)(handle, id)

/* Closes a security stream and frees up resources associated with it. */
void security_stream_close(security_stream_t *);

/* int security_stream_auth(security_stream_t *);
 *
 * Authenticate a connected security stream.  This should be called by the
 * target after security_stream_accept returns successfully, and by the client
 * after security_stream_connect returns successfullly. Returns 0 on success,
 * and -1 on error. Error messages can be obtained by calling
 * security_stream_geterror().
 */
#define	security_stream_auth(stream)		\
    (*(stream)->driver->stream_auth)(stream)

/* int security_stream_id(security_stream_t *);
 *
 * Returns an identifier which can be used to connect to this security stream
 * with security_stream_client().  Typical usage is for one end of a connection
 * to create a stream with security_stream_server(), and then transmit the id
 * for that stream to the other side. The other side will then connect to that
 * id with security_stream_client(). */
#define	security_stream_id(stream)		\
    (*(stream)->driver->stream_id)(stream)

/* int security_stream_write(security_stream_t *, const void *, size_t);
 *
 * Writes a chunk of data to the security stream. Returns 0 on success, or
 * negative on error. Error messages can be obtained by calling
 * security_stream_geterror().
 */
#define	security_stream_write(stream, buf, size)	\
    (*(stream)->driver->stream_write)(stream, buf, size)

/* void security_stream_read(
 *  security_stream_t *stream,
 *  void (*fn)(void *, void *, size_t),
 *  void *arg);

 * Requests that when data is ready to be read on this stream, the given
 * function is called with the given arg, a buffer full of data, and the size
 * of that buffer. On error, the bufsize will be negative. An error message can
 * be retrieved by calling security_stream_geterror().  This function uses the
 * event interface. Callbacks will only be generated while in event_loop(). */
#define	security_stream_read(stream, fn, arg)		\
    (*(stream)->driver->stream_read)(stream, fn, arg)

/* void security_stream_read_sync(security_stream_t *, void **);
 *
 * Return a buffer of data read from the stream. This function will block until
 * something can be read, but other event will be fired. A pointer to the data
 * is returned in *buf and the size of the buffer is returned.  On error, the
 * size will be negative. An error message can be retrieved by calling
 * security_stream_geterror(). This function uses the event interface.  */
#define	security_stream_read_sync(stream, buf)		\
    (*(stream)->driver->stream_read_sync)(stream, buf)

/* void security_stream_read_cancel(security_stream_t *);
 *
 * Cancels a previous read request. */
#define	security_stream_read_cancel(stream)		\
    (*(stream)->driver->stream_read_cancel)(stream)

/* void security_close_connection(security_handle_t *, hostname *);
 *
 * Close a security handle, freeing associated resources.  The hostname
 * argument is ignored. */
#define security_close_connection(handle, hostname) \
    (*(handle)->driver->close_connection)(handle, hostname)

#endif	/* SECURITY_H */
