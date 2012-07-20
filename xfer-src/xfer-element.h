/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* Base classes for transfer elements.
 */

#ifndef XFER_ELEMENT_H
#define XFER_ELEMENT_H

#include <glib.h>
#include <glib-object.h>
#include "xfer.h"
#include "amanda.h"
#include "directtcp.h"

typedef enum {
    /* sources have no input mechanisms and destinations have no output
     * mechansisms. */
    XFER_MECH_NONE,

    /* downstream element will read() from elt->upstream's output_fd; EOF
     * is indicated by the usual OS mechanism resulting in a zero-length
     * read, in response to which the downstream element must close
     * the fd. */
    XFER_MECH_READFD,

    /* upstream element will write() to elt->downstream's input_fd.  EOF
     * is indicated by closing the file descriptor. */
    XFER_MECH_WRITEFD,

    /* downstream element will call elt->upstream->pull_buffer() to
     * pull a buffer.  EOF is indicated by returning a NULL buffer */
    XFER_MECH_PULL_BUFFER,

    /* upstream element will call elt->downstream->push_buffer(buf) to push
     * a buffer.  EOF is indicated by passing a NULL buffer. */
    XFER_MECH_PUSH_BUFFER,

    /* DirectTCP: downstream sends an array of IP:PORT addresses to which a TCP
     * connection should be made, then upstream connects to one of the addreses
     * and sends the data over that connection */
    XFER_MECH_DIRECTTCP_LISTEN,

    /* DirectTCP: downstream gets IP:PORT addresses from upstream to which a
     * TCP connection should be made, then connects to one of the addreses and
     * receives the data over that connection */
    XFER_MECH_DIRECTTCP_CONNECT,

    /* (sentinel value) */
    XFER_MECH_MAX,
} xfer_mech;

/*
 * Description of a pair (input, output) of xfer mechanisms that an
 * element can support, along with the associated costs.  An array of these
 * pairs is stored in the class-level variable 'mech_pairs', describing
 * all of the mechanisms that an element supports.
 *
 * Use the XFER_NROPS() and XFER_NTHREADS() macros below in declarations in
 * order to make declarations more understandable.
 */

#define XFER_NROPS(x) (x)
#define XFER_NTHREADS(x) (x)

typedef struct {
    xfer_mech input_mech;
    xfer_mech output_mech;
    guint8 ops_per_byte;	/* number of byte copies or other operations */
    guint8 nthreads;		/* number of additional threads created */
} xfer_element_mech_pair_t;

/***********************
 * XferElement
 *
 * The virtual base class for all transfer elements
 */

GType xfer_element_get_type(void);
#define XFER_ELEMENT_TYPE (xfer_element_get_type())
#define XFER_ELEMENT(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_element_get_type(), XferElement)
#define XFER_ELEMENT_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_element_get_type(), XferElement const)
#define XFER_ELEMENT_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_element_get_type(), XferElementClass)
#define IS_XFER_ELEMENT(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_element_get_type ())
#define XFER_ELEMENT_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_element_get_type(), XferElementClass)

/*
 * Main object structure
 */

typedef struct XferElement {
    GObject __parent__;

    /* The transfer to which this element is attached */
    Xfer *xfer; /* set by xfer_new */

    /* assigned input and output mechanisms */
    xfer_mech input_mech;
    xfer_mech output_mech;

    /* neighboring xfer elements */
    struct XferElement *upstream;
    struct XferElement *downstream;

    /* Information regarding cancellation.  Cancelled and expect_eof are set by
     * the default cancel() method.  Can_generate_eof should be set during
     * initialization, and is returned by the default cancel implementation */
    gboolean cancelled;
    gboolean expect_eof;
    gboolean can_generate_eof;

    /* file descriptors for XFER_MECH_READFD and XFER_MECH_WRITEFD.  These
     * should be set during setup(), and can be accessed by neighboring
     * elements during start().  These values are shared among multiple
     * elements, and thus must be accessed with xfer_element_swap_input_fd and
     * xfer_element_swap_output_fd.  Any file descriptors remaining here at
     * finalize time will be closed. */
    gint _input_fd;
    gint _output_fd;

    /* array of IP:PORT pairs that can be used to connect to this element,
     * terminated by a 0.0.0.0:0.  The first is set by elements with an input
     * mech of XFER_MECH_DIRECTTCP_LISTEN and accessed by their upstream
     * neighbor; the second is set by elements with an output mech of
     * XFER_MECH_DIRECTTCP_CONNECT and accessed by their downstream neighbor.
     * */

    DirectTCPAddr *input_listen_addrs;
    DirectTCPAddr *output_listen_addrs;

    /* cache for repr() */
    char *repr;

    /* maximum size to transfer */
    gint64 size;
} XferElement;

/*
 * Class definition
 */

typedef struct {
    GObjectClass __parent__;

    /* Get a string representation of this element.  The returned string will be freed
     * when the element is finalized, and is static until that time.  This method is
     * implemented by XferElement, but can be overridden by classes that can provide
     * additional useful information about themselves.  Overriding methods can use
     * the 'repr' instance variable as a cache -- it will be freed on finalize().
     *
     * @param elt: the XferElement
     * @return: statically allocated string
     */
    char *(*repr)(XferElement *elt);

    /* Set up this element.  This function is called for all elements in a
     * transfer before start() is called for any elements.  For mechanisms
     * where this element supplies a file descriptor, it should set its
     * input_fd and/or output_fd appropriately; neighboring elements will use
     * that value in start().  Elements which supply IP:PORT pairs should set
     * their input_addrs, for neighboring elements to use in start().
     *
     * elt->input_mech and elt->output_mech are already set when this function
     * is called, but upstream and downstream are not.
     *
     * If the setup operation fails, the method should send an XMSG_ERROR and
     * call XMSG_CANCEL, and return False.  In this situation, the start method
     * will not be called.  The Xfer will appear to the user to start and
     * immediately fail.
     *
     * Note that this method may never be called if other elements' setup methods
     * fail first.
     *
     * @param elt: the XferElement
     * @return: false on failure, true on success
     */
    gboolean (*setup)(XferElement *elt);

    /* set the size of data to transfer, to skip NUL padding bytes
     * @param elt: the XferElement
     * @param size: the size of data to transfer
     * @return: TRUE
     */
    gboolean (*set_size)(XferElement *elt, gint64 size);

    /* Start transferring data.  The element downstream of this one will
     * already be started, while the upstream element will not, so data will
     * not begin flowing immediately.  It is safe to access attributes of
     * neighboring elements during this call.
     *
     * This method will *not* be called if all elements do not set up
     * correctly.
     *
     * @param elt: the XferElement
     * @return: TRUE if this element will send XMSG_DONE
     */
    gboolean (*start)(XferElement *elt);

    /* Stop transferring data.  The upstream element's cancel method will
     * already have been called, but due to buffering and synchronization
     * issues, data may yet arrive.  The element may discard any such data, but
     * must not fail.  This method is only called for abnormal terminations;
     * elements should normally stop processing on receiving an EOF indication
     * from upstream.
     *
     * If expect_eof is TRUE, then this element should expect an EOF from its
     * upstream element, and should drain any remaining data until that EOF
     * arrives and generate an EOF to the downstream element.  The utility
     * functions xfer_element_drain_fd and xfer_element_drain_buffers may be
     * useful for this purpose. This draining is important in order to avoid
     * hung threads or unexpected SIGPIPEs.
     *
     * If expect_eof is FALSE, then the upstream elements are unable to
     * generate an early EOF, so this element should *not* attempt to drain any
     * remaining data.  As an example, an FdSource is not active and thus
     * cannot generate an EOF on request.
     *
     * If this element can generate an EOF, it should return TRUE, otherwise
     * FALSE.
     *
     * This method may be called before start or setup if an error is
     * encountered during setup.
     *
     * The default implementation sets self->expect_eof and self->cancelled
     * appropriately and returns self->can_generate_eof.
     *
     * This method is always called from the main thread.  It must not block.
     *
     * @param elt: the XferElement
     * @param expect_eof: element should expect an EOF
     * @returns: true if this element can return EOF
     */
    gboolean (*cancel)(XferElement *elt, gboolean expect_eof);

    /* Get a buffer full of data from this element.  This function is called by
     * the downstream element under XFER_MECH_PULL_CALL.  It can block indefinitely,
     * and must only return NULL on EOF.  Responsibility to free the buffer transfers
     * to the caller.
     *
     * @param elt: the XferElement
     * @param size (output): size of resulting buffer
     * @returns: buffer pointer
     */
    gpointer (*pull_buffer)(XferElement *elt, size_t *size);

    /* A buffer full of data is being sent to this element for processing; this
     * function is called by the upstream element under XFER_MECH_PUSH_CALL.
     * It can block indefinitely if the data cannot be processed immediately.
     * An EOF condition is signaled by call with a NULL buffer.  Responsibility to
     * free the buffer transfers to the callee.
     *
     * @param elt: the XferElement
     * @param buf: buffer
     * @param size: size of buffer
     */
    void (*push_buffer)(XferElement *elt, gpointer buf, size_t size);

    /* Returns the mech_pairs that this element supports.  The default
     * implementation just returns the class attribute 'mech_pairs', but
     * subclasses can dynamically select the available mechanisms by overriding
     * this method.  Note that the method is called before the setup() method.
     *
     * @param elt: the XferElement
     * @returns: array of mech pairs, terminated by <NONE,NONE>
     */
    xfer_element_mech_pair_t *(*get_mech_pairs)(XferElement *elt);

    /* class variables */

    /* This is used by the perl bindings -- it is a class variable giving the
     * appropriate perl class to wrap this XferElement.  It should be set by
     * each class's class_init.
     */
    const char *perl_class;

    /* Statically allocated array of input/output mechanisms supported by this
     * class (terminated by <XFER_MECH_NONE,XFER_MECH_NONE>).  The default
     * get_mech_pairs method returns this. */
    xfer_element_mech_pair_t *mech_pairs;
} XferElementClass;

/*
 * Method stubs
 */

void xfer_element_unref(XferElement *elt);
gboolean xfer_element_link_to(XferElement *elt, XferElement *successor);
char *xfer_element_repr(XferElement *elt);
gboolean xfer_element_setup(XferElement *elt);
gboolean xfer_element_set_size(XferElement *elt, gint64 size);
gboolean xfer_element_start(XferElement *elt);
void xfer_element_push_buffer(XferElement *elt, gpointer buf, size_t size);
gpointer xfer_element_pull_buffer(XferElement *elt, size_t *size);
gboolean xfer_element_cancel(XferElement *elt, gboolean expect_eof);
xfer_element_mech_pair_t *xfer_element_get_mech_pairs(XferElement *elt);

/****
 * Subclass utilities
 *
 * These are utilities for subclasses
 */

/* Drain UPSTREAM by pulling buffers until EOF
 *
 * @param upstream: the element to drain
 */
void xfer_element_drain_buffers(XferElement *upstream);

/* Drain UPSTREAM by reading until EOF.  This does not close
 * the file descriptor.
 *
 * @param fd: the file descriptor to drain
 */
void xfer_element_drain_fd(int fd);

/* Atomically swap a value into elt->_input_fd and _output_fd, respectively.
 * Always use these methods to access the field.
 *
 * @param elt: xfer element
 * @param newfd: new value for the fd field
 * @returns: old value of the fd field
 */
#define xfer_element_swap_input_fd(elt, newfd) \
    xfer_atomic_swap_fd((elt)->xfer, &(elt)->_input_fd, newfd)
#define xfer_element_swap_output_fd(elt, newfd) \
    xfer_atomic_swap_fd((elt)->xfer, &(elt)->_output_fd, newfd)

/***********************
 * XferElement subclasses
 *
 * These simple subclasses do not introduce any additional public members or
 * methods, so they do not have their own header file.  The functions here
 * provide their only public interface.  The implementation of these elements
 * can also provide a good prototype for new elements.
 */

/* A transfer source that produces LENGTH bytes of random data, for testing
 * purposes.
 *
 * Implemented in source-random.c
 *
 * @param length: bytes to produce, or zero for no limit
 * @param prng_seed: initial value for random number generator
 * @return: new element
 */
XferElement *xfer_source_random(guint64 length, guint32 prng_seed);

/* Get the ending random seed for the xfer_source_random.  Call this after a
 * transfer has finished, and construct a new xfer_source_random with the seed.
 * The new source will continue the same random sequence at the next byte.  This
 * is useful for constructing spanned dumps in testing.
 *
 * @param src: XferSourceRandom object
 * @returns: seed
 */
guint32 xfer_source_random_get_seed(XferElement *src);

/* A transfer source that produces LENGTH bytes containing repeated
 * copies of the provided pattern, for testing purposes.
 *
 * Implemented in source-pattern.c
 *
 * @param length: bytes to produce, or zero for no limit
 * @param pattern: Pointer to memory containing the desired byte pattern.
 * @param pattern_length: Size of pattern to repeat.
 * @return: new element
 */
XferElement *xfer_source_pattern(guint64 length, void * pattern,
                                 size_t pattern_length);

/* A transfer source that provides bytes read from a file descriptor.
 * Reading continues until EOF, but the file descriptor is not closed.
 *
 * Implemented in source-fd.c
 *
 * @param fd: the file descriptor from which to read
 * @return: new element
 */
XferElement * xfer_source_fd(
    int fd);

/* A transfer source that exposes its listening DirectTCPAddrs (via
 * elt->input_listen_addrs) for external use
 *
 * Implemented in source-directtcp-listen.c
 *
 * @return: new element
 */
XferElement * xfer_source_directtcp_listen(void);

/* A transfer source that connects to a DirectTCP address and pulls data
 * from it into the transfer.
 *
 * Implemented in source-directtcp-listen.c
 *
 * @param addrs: DirectTCP addresses to connect to
 * @return: new element
 */
XferElement * xfer_source_directtcp_connect(DirectTCPAddr *addrs);

/* A transfer filter that executes an external application, feeding it data on
 * stdin and taking the results on stdout.
 *
 * The memory for ARGV becomes the property of the transfer element and will be
 * g_strfreev'd when the xfer is destroyed.
 *
 * Implemented in filter-process.c
 *
 * @param argv: NULL-terminated command-line arguments
 * @param need_root: become root before exec'ing the subprocess
 * @return: new element
 */
XferElement *xfer_filter_process(gchar **argv,
    gboolean need_root);

/* A transfer filter that just applies a bytewise XOR transformation to the data
 * that passes through it.
 *
 * Implemented in filter-xor.c
 *
 * @param xor_key: key for xor operations
 * @return: new element
 */
XferElement *xfer_filter_xor(
    unsigned char xor_key);

/* A transfer destination that consumes all bytes it is given, optionally
 * validating that they match those produced by source_random
 *
 * Implemented in dest-null.c
 *
 * @param prng_seed: if nonzero, validate that the datastream matches
 *	that produced by a random source with this random seed.  If zero,
 *	no validation is performed.
 * @return: new element
 */
XferElement *xfer_dest_null(
    guint32 prng_seed);

/* A transfer destination that writes bytes to a file descriptor.  The file
 * descriptor is not closed when the transfer is complete.
 *
 * Implemented in dest-fd.c
 *
 * @param fd: file descriptor to which to write
 * @return: new element
 */
XferElement *xfer_dest_fd(
    int fd);

/* A transfer destination that writes bytes to an in-memory buffer.
 *
 * Implemented in dest-buffer.c
 *
 * @param max_size: maximum size for the buffer, or zero for no limit
 * @return: new element
 */
XferElement *xfer_dest_buffer(
    gsize max_size);

/* Get the buffer and size from an XferDestBuffer.  The resulting buffer
 * will remain allocated until the XDB itself is freed.
 *
 * Implemented in dest-buffer.c
 *
 * @param elt: the element
 * @param buf (output): buffer pointer
 * @param size (output): buffer size
 */
void xfer_dest_buffer_get(
    XferElement *elt,
    gpointer *buf,
    gsize *size);

/* A transfer dest that connects to a DirectTCPAddr and sends data to
 * it
 *
 * Implemented in dest-directtcp-connect.c
 *
 * @param addrs: DirectTCP addresses to connect to
 * @return: new element
 */
XferElement * xfer_dest_directtcp_connect(DirectTCPAddr *addrs);

/* A transfer dest that listens for a DirecTCP connection and sends data to it
 * when connected.  Listening addresses are exposed at
 * elt->output_listen_addrs.
 *
 * Implemented in dest-directtcp-listen.c
 *
 * @return: new element
 */
XferElement * xfer_dest_directtcp_listen(void);

#endif
