/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

/* Base classes and interfaces for transfer elements.  There are two interfaces
 * defined here: IXferProducer and IXferConsumer.  The former is for elements
 * which produce data, and the latter is for those which consume it.  There is
 * a top-level XferElement base class, and three base classes that are used for
 * actual implementations: XferSource, XferFilter, and XferDest.
 *
 * Unless you're well-acquainted with GType and GObject, this file will be a
 * difficult read.  It is really only of use to those implementing new subclasses.
 */

#ifndef XFER_ELEMENT_H
#define XFER_ELEMENT_H

#include <glib.h>
#include <glib-object.h>
#include "xfer.h"

/* The mechanisms by which a transfer element can communicate, both with its
 * upstream and downstream neighbors.  */
typedef enum {
    /* can be given a file descriptor to read from */
    MECH_INPUT_READ_GIVEN_FD = 1 << 0,

    /* can supply a file descriptor to write to */
    MECH_INPUT_HAVE_WRITE_FD = 1 << 1,

    /* can provide a consumer for a c/p queue */
    /* TODO: MECH_INPUT_CONSUMER = 1 << 2, */
} xfer_input_mech;

typedef enum {
    /* can be given a file descriptor to write to */
    MECH_OUTPUT_WRITE_GIVEN_FD = 1 << 16,

    /* can supply a file descriptor to read from */
    MECH_OUTPUT_HAVE_READ_FD = 1 << 17,

    /* can provide a producer for a c/p queue */
    /* TODO: MECH_OUTPUT_PRODUCER = 1 << 18, */
} xfer_output_mech;

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

    /* The input and output mechanisms this element supports */
    xfer_output_mech output_mech; /* read-only */
    xfer_input_mech input_mech; /* read-only */

    /* fields for use by link_to */
    int pipe[2];

    /* cache for repr() */
    char *repr;
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

    /* Link this element to the downstream element, to which it will send data.  This
     * function examines its own output_mech and its successor's input_mech in order
     * to make an optimal link between the elements.  It should not need to be 
     * overridden.
     *
     * @param elt: the XferElement
     * @param downstream: the XferElement to which ELT will send data
     * @return: true if the link was successful
     */
    gboolean (*link_to)(XferElement *elt, XferElement *downstream);

    /* Start transferring data.  The element downstream of this one will already be
     * started, while the upstream element will not, so data will not begin flowing
     * immediately.
     *
     * @param elt: the XferElement
     */
    void (*start)(XferElement *elt);

    /* Stop transferring data.  The upstream element's stop method will already have
     * been called, but due to OS buffering, data may yet arrive.  The element may
     * discard any such data, but must not fail.  This function is only called for
     * abnormal terminations; elements should normally stop processing on receiving 
     * an EOF indication from upstream.
     *
     * @param elt: the XferElement
     */
    void (*abort)(XferElement *elt);

    /* Set up the output side of the element.  MECH will only have one bit set, and
     * the expected behavior for each flag position is:
     *   MECH_OUTPUT_HAVE_READ_FD: set *FDP to a readable file descriptor
     *   MECH_OUTPUT_WRITE_GIVEN_FD: *FDP is an fd to which the element can write
     * This function will be called before set_input.
     *
     * @param elt: the XferElement
     * @param mech: the selected mechanism (only one bit is set)
     * @param fdp (input or result): the file descriptor
     */
    void (*setup_output)(XferElement *elt, xfer_output_mech mech, int *fdp);

    /* Set up the input side of the element.  MECH will only have one bit set, and
     * the expected behavior for each flag position is:
     *   MECH_OUTPUT_HAVE_WRITE_FD: set *FDP to a writable file descriptor
     *   MECH_OUTPUT_READ_GIVEN_FD: *FDP is an fd from which the element can read
     * This function will be called before set_input.
     *
     * @param elt: the XferElement
     * @param mech: the selected mechanism (only one bit is set)
     * @param fdp (input or result): the file descriptor
     */
    void (*setup_input)(XferElement *elt, xfer_input_mech mech, int *fdp);
} XferElementClass;

/*
 * Method stubs
 */

gboolean xfer_element_link_to(XferElement *elt, XferElement *successor);
char *xfer_element_repr(XferElement *elt);
void xfer_element_start(XferElement *elt);
void xfer_element_abort(XferElement *elt);

/***********************
 * XferSource
 *
 * Base class for transfer sources.
 */

GType xfer_source_get_type(void);
#define XFER_SOURCE_TYPE (xfer_source_get_type())
#define XFER_SOURCE(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_get_type(), XferSource)
#define XFER_SOURCE_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_get_type(), XferSource const)
#define XFER_SOURCE_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_get_type(), XferSourceClass)
#define IS_XFER_SOURCE(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_get_type ())
#define XFER_SOURCE_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_get_type(), XferSourceClass)

/*
 * Main object structure
 */

typedef struct XferSource {
    XferElement __parent__;
} XferSource;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceClass;

/*
 * Methods
 */

/***********************
 * XferFilter
 *
 * Base class for transfer filters.
 */

GType xfer_filter_get_type(void);
#define XFER_FILTER_TYPE (xfer_filter_get_type())
#define XFER_FILTER(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_get_type(), XferFilter)
#define XFER_FILTER_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_get_type(), XferFilter const)
#define XFER_FILTER_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_filter_get_type(), XferFilterClass)
#define IS_XFER_FILTER(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_filter_get_type ())
#define XFER_FILTER_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_filter_get_type(), XferFilterClass)

/*
 * Main object structure
 */

typedef struct XferFilter {
    XferElement __parent__;
} XferFilter;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferFilterClass;

/*
 * Methods
 */

/***********************
 * XferDest
 *
 * Base class for transfer destinations.
 */

GType xfer_dest_get_type(void);
#define XFER_DEST_TYPE (xfer_dest_get_type())
#define XFER_DEST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_get_type(), XferDest)
#define XFER_DEST_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_get_type(), XferDest const)
#define XFER_DEST_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_get_type(), XferDestClass)
#define IS_XFER_DEST(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_get_type ())
#define XFER_DEST_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_get_type(), XferDestClass)

/*
 * Main object structure
 */

typedef struct XferDest {
    XferElement __parent__;
} XferDest;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferDestClass;

/*
 * Methods
 */

/***********************
 * XferElement subclasses
 *
 * These simple subclasses do not introduce any additional public members or
 * methods, so they do not have their own header file.  The functions here
 * provide their only public interface.  The implementation of these elements
 * can also provide a good prototype for new elements.
 */

/* A transfer source that produces LENGTH bytes of random data, for testing
 * purposes.  The class supports all output mechanisms, but its advertized
 * mechanisms can be limited with the MECHANISMS parameter.
 *
 * Implemented in source-random.c
 *
 * @param length: bytes to produce
 * @param text_only: output should be in short, textual lines (for debugging)
 * @param mechanisms: output mechanisms to advertize
 * @return: new element
 */
XferElement *xfer_source_random(
    size_t length,
    gboolean text_only,
    xfer_output_mech mechanisms);

/* A transfer filter that just applies a bytewise XOR transformation to the data
 * that passes through it.  It supports all input mechanisms, but its advertized
 * mechanisms can be set with the MECHANISMS parameter.
 *
 * Implemented in filter-xor.c
 *
 * @param mechanisms: input mechanisms to advertize
 * @return: new element
 */
XferElement *xfer_filter_xor(
    xfer_input_mech input_mech,
    xfer_output_mech output_mech,
    char xor_key);

/* A transfer destination that consumes all bytes it is given.  The class
 * supports all input mechanisms, but its advertized mechanisms can be set
 * with the MECHANISMS parameter.
 *
 * Implemented in dest-null.c
 *
 * @param mechanisms: input mechanisms to advertize
 * @param debug_print: if TRUE, all data will be printed to stdout
 * @return: new element
 */
XferElement *xfer_dest_null(
    xfer_input_mech mechanisms,
    gboolean debug_print);

#endif
