/*
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */

#include "amxfer.h"
#include "glib-util.h"
#include "testutils.h"
#include "amanda.h"
#include "event.h"
#include "simpleprng.h"

/* Having tests repeat exactly is an advantage, so we use a hard-coded
 * random seed. */
#define RANDOM_SEED 0xf00d

/*
 * XferElement subclasses
 *
 * This file defines a few "private" element classes that each have only one
 * mechanism pair.  These classes are then used to test all of the possible
 * combinations of glue.
 */

/* constants to determine the total amount of data to be transfered; EXTRA is
 * to test out partial-block handling; it should be prime. */
#define TEST_BLOCK_SIZE 1024
#define TEST_BLOCK_COUNT 10
#define TEST_BLOCK_EXTRA 97
#define TEST_XFER_SIZE ((TEST_BLOCK_SIZE*TEST_BLOCK_COUNT)+TEST_BLOCK_EXTRA)

/* READFD */

static GType xfer_source_readfd_get_type(void);
#define XFER_SOURCE_READFD_TYPE (xfer_source_readfd_get_type())
#define XFER_SOURCE_READFD(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_readfd_get_type(), XferSourceReadfd)
#define XFER_SOURCE_READFD_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_readfd_get_type(), XferSourceReadfd const)
#define XFER_SOURCE_READFD_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_readfd_get_type(), XferSourceReadfdClass)
#define IS_XFER_SOURCE_READFD(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_readfd_get_type ())
#define XFER_SOURCE_READFD_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_readfd_get_type(), XferSourceReadfdClass)

typedef struct XferSourceReadfd {
    XferElement __parent__;

    int write_fd;
    GThread *thread;
    simpleprng_state_t prng;
} XferSourceReadfd;

typedef struct {
    XferElementClass __parent__;
} XferSourceReadfdClass;

static gpointer
source_readfd_thread(
    gpointer data)
{
    XferSourceReadfd *self = (XferSourceReadfd *)data;
    char buf[TEST_XFER_SIZE];
    int fd = self->write_fd;

    simpleprng_fill_buffer(&self->prng, buf, sizeof(buf));

    if (full_write(fd, buf, sizeof(buf)) < sizeof(buf)) {
	error("error in full_write(): %s", strerror(errno));
    }

    close(fd);

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

static void
source_readfd_setup_impl(
    XferElement *elt)
{
    XferSourceReadfd *self = (XferSourceReadfd *)elt;
    int p[2];

    simpleprng_seed(&self->prng, RANDOM_SEED);

    if (pipe(p) < 0)
	g_critical("Error from pipe(): %s", strerror(errno));

    self->write_fd = p[1];
    XFER_ELEMENT(self)->output_fd = p[0];
}

static gboolean
source_readfd_start_impl(
    XferElement *elt)
{
    XferSourceReadfd *self = (XferSourceReadfd *)elt;
    self->thread = g_thread_create(source_readfd_thread, (gpointer)self, FALSE, NULL);

    return TRUE;
}

static void
source_readfd_class_init(
    XferSourceReadfdClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_READFD, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->setup = source_readfd_setup_impl;
    xec->start = source_readfd_start_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_source_readfd_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceReadfdClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) source_readfd_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceReadfd),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceReadfd", &info, 0);
    }

    return type;
}

/* WRITEFD */

static GType xfer_source_writefd_get_type(void);
#define XFER_SOURCE_WRITEFD_TYPE (xfer_source_writefd_get_type())
#define XFER_SOURCE_WRITEFD(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_writefd_get_type(), XferSourceWritefd)
#define XFER_SOURCE_WRITEFD_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_writefd_get_type(), XferSourceWritefd const)
#define XFER_SOURCE_WRITEFD_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_writefd_get_type(), XferSourceWritefdClass)
#define IS_XFER_SOURCE_WRITEFD(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_writefd_get_type ())
#define XFER_SOURCE_WRITEFD_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_writefd_get_type(), XferSourceWritefdClass)

typedef struct XferSourceWritefd {
    XferElement __parent__;

    GThread *thread;
    simpleprng_state_t prng;
} XferSourceWritefd;

typedef struct {
    XferElementClass __parent__;
} XferSourceWritefdClass;

static gpointer
source_writefd_thread(
    gpointer data)
{
    XferSourceWritefd *self = (XferSourceWritefd *)data;
    char buf[TEST_XFER_SIZE];
    int fd = XFER_ELEMENT(self)->downstream->input_fd;

    simpleprng_fill_buffer(&self->prng, buf, sizeof(buf));

    if (full_write(fd, buf, sizeof(buf)) < sizeof(buf)) {
	error("error in full_write(): %s", strerror(errno));
    }

    close(fd);
    XFER_ELEMENT(self)->downstream->input_fd = -1;

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

static gboolean
source_writefd_start_impl(
    XferElement *elt)
{
    XferSourceWritefd *self = (XferSourceWritefd *)elt;

    simpleprng_seed(&self->prng, RANDOM_SEED);

    self->thread = g_thread_create(source_writefd_thread, (gpointer)self, FALSE, NULL);

    return TRUE;
}

static void
source_writefd_class_init(
    XferSourceWritefdClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_WRITEFD, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->start = source_writefd_start_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_source_writefd_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceWritefdClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) source_writefd_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceWritefd),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceWritefd", &info, 0);
    }

    return type;
}

/* PUSH_BUFFER */

static GType xfer_source_push_get_type(void);
#define XFER_SOURCE_PUSH_TYPE (xfer_source_push_get_type())
#define XFER_SOURCE_PUSH(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_push_get_type(), XferSourcePush)
#define XFER_SOURCE_PUSH_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_push_get_type(), XferSourcePush const)
#define XFER_SOURCE_PUSH_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_push_get_type(), XferSourcePushClass)
#define IS_XFER_SOURCE_PUSH(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_push_get_type ())
#define XFER_SOURCE_PUSH_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_push_get_type(), XferSourcePushClass)

typedef struct XferSourcePush {
    XferElement __parent__;

    GThread *thread;
    simpleprng_state_t prng;
} XferSourcePush;

typedef struct {
    XferElementClass __parent__;
} XferSourcePushClass;

static gpointer
source_push_thread(
    gpointer data)
{
    XferSourcePush *self = (XferSourcePush *)data;
    char *buf;
    int i;

    for (i = 0; i < TEST_BLOCK_COUNT; i++) {
	buf = g_malloc(TEST_BLOCK_SIZE);
	simpleprng_fill_buffer(&self->prng, buf, TEST_BLOCK_SIZE);
	xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, buf, TEST_BLOCK_SIZE);
	buf = NULL;
    }

    /* send a smaller block */
    buf = g_malloc(TEST_BLOCK_EXTRA);
    simpleprng_fill_buffer(&self->prng, buf, TEST_BLOCK_EXTRA);
    xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, buf, TEST_BLOCK_EXTRA);
    buf = NULL;

    /* send EOF */
    xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, NULL, 0);

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

static gboolean
source_push_start_impl(
    XferElement *elt)
{
    XferSourcePush *self = (XferSourcePush *)elt;

    simpleprng_seed(&self->prng, RANDOM_SEED);

    self->thread = g_thread_create(source_push_thread, (gpointer)self, FALSE, NULL);

    return TRUE;
}

static void
source_push_class_init(
    XferSourcePushClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PUSH_BUFFER, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->start = source_push_start_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_source_push_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourcePushClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) source_push_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourcePush),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourcePush", &info, 0);
    }

    return type;
}

/* PULL_BUFFER */

static GType xfer_source_pull_get_type(void);
#define XFER_SOURCE_PULL_TYPE (xfer_source_pull_get_type())
#define XFER_SOURCE_PULL(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_pull_get_type(), XferSourcePull)
#define XFER_SOURCE_PULL_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_pull_get_type(), XferSourcePull const)
#define XFER_SOURCE_PULL_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_pull_get_type(), XferSourcePullClass)
#define IS_XFER_SOURCE_PULL(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_pull_get_type ())
#define XFER_SOURCE_PULL_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_pull_get_type(), XferSourcePullClass)

typedef struct XferSourcePull {
    XferElement __parent__;

    gint nbuffers;
    GThread *thread;
    simpleprng_state_t prng;
} XferSourcePull;

typedef struct {
    XferElementClass __parent__;
} XferSourcePullClass;

static gpointer
source_pull_pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourcePull *self = (XferSourcePull *)elt;
    char *buf;
    size_t bufsiz;

    if (self->nbuffers > TEST_BLOCK_COUNT) {
	*size = 0;
	return NULL;
    }
    bufsiz = (self->nbuffers != TEST_BLOCK_COUNT)? TEST_BLOCK_SIZE : TEST_BLOCK_EXTRA;

    self->nbuffers++;

    buf = g_malloc(bufsiz);
    simpleprng_fill_buffer(&self->prng, buf, bufsiz);
    *size = bufsiz;
    return buf;
}

static void
source_pull_setup_impl(
    XferElement *elt)
{
    XferSourcePull *self = (XferSourcePull *)elt;

    simpleprng_seed(&self->prng, RANDOM_SEED);
}

static void
source_pull_class_init(
    XferSourcePullClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, 1, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->pull_buffer = source_pull_pull_buffer_impl;
    xec->setup = source_pull_setup_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_source_pull_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourcePullClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) source_pull_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourcePull),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourcePull", &info, 0);
    }

    return type;
}

/* READFD */

static GType xfer_dest_readfd_get_type(void);
#define XFER_DEST_READFD_TYPE (xfer_dest_readfd_get_type())
#define XFER_DEST_READFD(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_readfd_get_type(), XferDestReadfd)
#define XFER_DEST_READFD_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_readfd_get_type(), XferDestReadfd const)
#define XFER_DEST_READFD_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_readfd_get_type(), XferDestReadfdClass)
#define IS_XFER_DEST_READFD(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_readfd_get_type ())
#define XFER_DEST_READFD_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_readfd_get_type(), XferDestReadfdClass)

typedef struct XferDestReadfd {
    XferElement __parent__;

    GThread *thread;
    simpleprng_state_t prng;
} XferDestReadfd;

typedef struct {
    XferElementClass __parent__;
} XferDestReadfdClass;

static gpointer
dest_readfd_thread(
    gpointer data)
{
    XferDestReadfd *self = (XferDestReadfd *)data;
    char buf[TEST_XFER_SIZE];
    size_t remaining;
    int fd = XFER_ELEMENT(self)->upstream->output_fd;

    remaining = sizeof(buf);
    while (remaining) {
	ssize_t nread;
	if ((nread = read(fd, buf+sizeof(buf)-remaining, remaining)) <= 0) {
	    error("error in read(): %s", strerror(errno));
	}
	remaining -= nread;
    }

    /* we should be at EOF here */
    if (read(fd, buf, 10) != 0)
	g_critical("too much data entering XferDestReadfd");

    if (!simpleprng_verify_buffer(&self->prng, buf, TEST_XFER_SIZE))
	g_critical("data entering XferDestReadfd does not match");

    close(fd);
    XFER_ELEMENT(self)->upstream->output_fd = -1;

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

static gboolean
dest_readfd_start_impl(
    XferElement *elt)
{
    XferDestReadfd *self = (XferDestReadfd *)elt;

    simpleprng_seed(&self->prng, RANDOM_SEED);

    self->thread = g_thread_create(dest_readfd_thread, (gpointer)self, FALSE, NULL);

    return TRUE;
}

static void
dest_readfd_class_init(
    XferDestReadfdClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_READFD, XFER_MECH_NONE, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->start = dest_readfd_start_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_dest_readfd_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestReadfdClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) dest_readfd_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestReadfd),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestReadfd", &info, 0);
    }

    return type;
}

/* WRITEFD */

static GType xfer_dest_writefd_get_type(void);
#define XFER_DEST_WRITEFD_TYPE (xfer_dest_writefd_get_type())
#define XFER_DEST_WRITEFD(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_writefd_get_type(), XferDestWritefd)
#define XFER_DEST_WRITEFD_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_writefd_get_type(), XferDestWritefd const)
#define XFER_DEST_WRITEFD_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_writefd_get_type(), XferDestWritefdClass)
#define IS_XFER_DEST_WRITEFD(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_writefd_get_type ())
#define XFER_DEST_WRITEFD_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_writefd_get_type(), XferDestWritefdClass)

typedef struct XferDestWritefd {
    XferElement __parent__;

    int read_fd;
    GThread *thread;
    simpleprng_state_t prng;
} XferDestWritefd;

typedef struct {
    XferElementClass __parent__;
} XferDestWritefdClass;

static gpointer
dest_writefd_thread(
    gpointer data)
{
    XferDestWritefd *self = (XferDestWritefd *)data;
    char buf[TEST_XFER_SIZE];
    size_t remaining;
    int fd = self->read_fd;

    remaining = sizeof(buf);
    while (remaining) {
	ssize_t nwrite;
	if ((nwrite = read(fd, buf+sizeof(buf)-remaining, remaining)) <= 0) {
	    error("error in read(): %s", strerror(errno));
	}
	remaining -= nwrite;
    }

    /* we should be at EOF here */
    if (read(fd, buf, 10) != 0)
	g_critical("too much data entering XferDestWritefd");

    if (!simpleprng_verify_buffer(&self->prng, buf, TEST_XFER_SIZE))
	g_critical("data entering XferDestWritefd does not match");

    close(fd);
    XFER_ELEMENT(self)->upstream->output_fd = -1;

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

static void
dest_writefd_setup_impl(
    XferElement *elt)
{
    XferDestWritefd *self = (XferDestWritefd *)elt;
    int p[2];

    simpleprng_seed(&self->prng, RANDOM_SEED);

    if (pipe(p) < 0)
	g_critical("Error from pipe(): %s", strerror(errno));

    self->read_fd = p[0];
    XFER_ELEMENT(self)->input_fd = p[1];
}

static gboolean
dest_writefd_start_impl(
    XferElement *elt)
{
    XferDestWritefd *self = (XferDestWritefd *)elt;
    self->thread = g_thread_create(dest_writefd_thread, (gpointer)self, FALSE, NULL);

    return TRUE;
}

static void
dest_writefd_class_init(
    XferDestWritefdClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_WRITEFD, XFER_MECH_NONE, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->setup = dest_writefd_setup_impl;
    xec->start = dest_writefd_start_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_dest_writefd_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestWritefdClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) dest_writefd_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestWritefd),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestWritefd", &info, 0);
    }

    return type;
}

/* PUSH_BUFFER */

static GType xfer_dest_push_get_type(void);
#define XFER_DEST_PUSH_TYPE (xfer_dest_push_get_type())
#define XFER_DEST_PUSH(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_push_get_type(), XferDestPush)
#define XFER_DEST_PUSH_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_push_get_type(), XferDestPush const)
#define XFER_DEST_PUSH_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_push_get_type(), XferDestPushClass)
#define IS_XFER_DEST_PUSH(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_push_get_type ())
#define XFER_DEST_PUSH_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_push_get_type(), XferDestPushClass)

typedef struct XferDestPush {
    XferElement __parent__;

    char buf[TEST_XFER_SIZE];
    size_t bufpos;

    GThread *thread;
    simpleprng_state_t prng;
} XferDestPush;

typedef struct {
    XferElementClass __parent__;
} XferDestPushClass;

static void
dest_push_push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t size)
{
    XferDestPush *self = (XferDestPush *)elt;

    if (buf == NULL) {
	/* if we're at EOF, verify we got the right bytes */
	g_assert(self->bufpos == TEST_XFER_SIZE);
	if (!simpleprng_verify_buffer(&self->prng, self->buf, TEST_XFER_SIZE))
	    g_critical("data entering XferDestPush does not match");
	return;
    }

    g_assert(self->bufpos + size <= TEST_XFER_SIZE);
    memcpy(self->buf + self->bufpos, buf, size);
    self->bufpos += size;
}

static void
dest_push_setup_impl(
    XferElement *elt)
{
    XferDestPush *self = (XferDestPush *)elt;

    simpleprng_seed(&self->prng, RANDOM_SEED);
}

static void
dest_push_class_init(
    XferDestPushClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PUSH_BUFFER, XFER_MECH_NONE, 1, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->push_buffer = dest_push_push_buffer_impl;
    xec->setup = dest_push_setup_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_dest_push_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestPushClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) dest_push_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestPush),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestPush", &info, 0);
    }

    return type;
}

/* PULL_BUFFER */

static GType xfer_dest_pull_get_type(void);
#define XFER_DEST_PULL_TYPE (xfer_dest_pull_get_type())
#define XFER_DEST_PULL(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_pull_get_type(), XferDestPull)
#define XFER_DEST_PULL_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_pull_get_type(), XferDestPull const)
#define XFER_DEST_PULL_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_pull_get_type(), XferDestPullClass)
#define IS_XFER_DEST_PULL(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_pull_get_type ())
#define XFER_DEST_PULL_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_pull_get_type(), XferDestPullClass)

typedef struct XferDestPull {
    XferElement __parent__;

    GThread *thread;
    simpleprng_state_t prng;
} XferDestPull;

typedef struct {
    XferElementClass __parent__;
} XferDestPullClass;

static gpointer
dest_pull_thread(
    gpointer data)
{
    XferDestPull *self = (XferDestPull *)data;
    char fullbuf[TEST_XFER_SIZE];
    char *buf;
    size_t bufpos = 0;
    size_t size;

    while ((buf = xfer_element_pull_buffer(XFER_ELEMENT(self)->upstream, &size))) {
	g_assert(bufpos + size <= TEST_XFER_SIZE);
	memcpy(fullbuf + bufpos, buf, size);
	bufpos += size;
    }

    /* we're at EOF, so verify we got the right bytes */
    g_assert(bufpos == TEST_XFER_SIZE);
    if (!simpleprng_verify_buffer(&self->prng, fullbuf, TEST_XFER_SIZE))
	g_critical("data entering XferDestPull does not match");

    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

static gboolean
dest_pull_start_impl(
    XferElement *elt)
{
    XferDestPull *self = (XferDestPull *)elt;

    simpleprng_seed(&self->prng, RANDOM_SEED);

    self->thread = g_thread_create(dest_pull_thread, (gpointer)self, FALSE, NULL);

    return TRUE;
}

static void
dest_pull_class_init(
    XferDestPullClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PULL_BUFFER, XFER_MECH_NONE, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->start = dest_pull_start_impl;
    xec->mech_pairs = mech_pairs;
}

GType
xfer_dest_pull_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestPullClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) dest_pull_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestPull),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestPull", &info, 0);
    }

    return type;
}


/*
 * Tests
 */

static void
test_xfer_generic_callback(
    gpointer data G_GNUC_UNUSED,
    XMsg *msg,
    Xfer *xfer)
{
    tu_dbg("Received message %s\n", xmsg_repr(msg));

    switch (msg->type) {
	case XMSG_DONE:
	    /* are we done? */
	    if (xfer->status == XFER_DONE) {
		tu_dbg("all elements are done!\n");
		g_main_loop_quit(default_main_loop());
	    }
	    break;

	default:
	    break;
    }
}

/****
 * Run a simple transfer with some xor filters
 */

static int
test_xfer_simple(void)
{
    unsigned int i;
    GSource *src;
    XferElement *elements[] = {
	xfer_source_random(100*1024, RANDOM_SEED),
	xfer_filter_xor('d'),
	xfer_filter_xor('d'),
	xfer_dest_null(RANDOM_SEED),
    };

    Xfer *xfer = xfer_new(elements, sizeof(elements)/sizeof(*elements));
    src = xfer_get_source(xfer);
    g_source_set_callback(src, (GSourceFunc)test_xfer_generic_callback, NULL, NULL);
    g_source_attach(src, NULL);
    tu_dbg("Transfer: %s\n", xfer_repr(xfer));

    /* unreference the elements */
    for (i = 0; i < sizeof(elements)/sizeof(*elements); i++) {
	g_object_unref(elements[i]);
	g_assert(G_OBJECT(elements[i])->ref_count == 1);
	elements[i] = NULL;
    }

    xfer_start(xfer);

    g_main_loop_run(default_main_loop());
    g_assert(xfer->status == XFER_DONE);

    xfer_unref(xfer);

    return 1;
}

/****
 * Run a transfer between two files, with or without filters
 */

static int
test_xfer_files(gboolean add_filters)
{
    unsigned int i;
    unsigned int elts;
    GSource *src;
    char *in_filename = __FILE__;
    char *out_filename = "xfer-test.tmp"; /* current directory is writeable */
    int rfd, wfd;
    Xfer *xfer;
    XferElement *elements[4];

    rfd = open(in_filename, O_RDONLY, 0);
    if (rfd < 0)
	g_critical("Could not open '%s': %s", in_filename, strerror(errno));

    wfd = open(out_filename, O_WRONLY|O_CREAT, 0777);
    if (wfd < 0)
	g_critical("Could not open '%s': %s", out_filename, strerror(errno));

    elts = 0;
    elements[elts++] = xfer_source_fd(rfd);
    if (add_filters) {
	elements[elts++] = xfer_filter_xor(0xab);
	elements[elts++] = xfer_filter_xor(0xab);
    }
    elements[elts++] = xfer_dest_fd(wfd);

    xfer = xfer_new(elements, elts);
    src = xfer_get_source(xfer);
    g_source_set_callback(src, (GSourceFunc)test_xfer_generic_callback, NULL, NULL);
    g_source_attach(src, NULL);
    tu_dbg("Transfer: %s\n", xfer_repr(xfer));

    /* unreference the elements */
    for (i = 0; i < elts; i++) {
	g_object_unref(elements[i]);
	g_assert(G_OBJECT(elements[i])->ref_count == 1);
	elements[i] = NULL;
    }

    xfer_start(xfer);

    g_main_loop_run(default_main_loop());
    g_assert(xfer->status == XFER_DONE);

    xfer_unref(xfer);

    unlink(out_filename); /* ignore any errors */

    return 1;
}

static int
test_xfer_files_simple(void)
{
    return test_xfer_files(FALSE);
}

static int
test_xfer_files_filter(void)
{
    return test_xfer_files(TRUE);
}

/*****
 * test each possible combination of source and destination mechansim
 */

static int
test_glue_combo(
    XferElement *source,
    XferElement *dest)
{
    unsigned int i;
    GSource *src;
    XferElement *elements[] = { source, dest };

    Xfer *xfer = xfer_new(elements, sizeof(elements)/sizeof(*elements));
    src = xfer_get_source(xfer);
    g_source_set_callback(src, (GSourceFunc)test_xfer_generic_callback, NULL, NULL);
    g_source_attach(src, NULL);

    /* unreference the elements */
    for (i = 0; i < sizeof(elements)/sizeof(*elements); i++) {
	g_object_unref(elements[i]);
	g_assert(G_OBJECT(elements[i])->ref_count == 1);
	elements[i] = NULL;
    }

    xfer_start(xfer);

    g_main_loop_run(default_main_loop());
    g_assert(xfer->status == XFER_DONE);

    xfer_unref(xfer);

    return 1;
}

#define make_test_glue(n, s, d) static int n(void) \
{\
    return test_glue_combo((XferElement *)g_object_new(s, NULL), \
			   (XferElement *)g_object_new(d, NULL)); \
}
make_test_glue(test_glue_READFD_READFD, XFER_SOURCE_READFD_TYPE, XFER_DEST_READFD_TYPE)
make_test_glue(test_glue_READFD_WRITE, XFER_SOURCE_READFD_TYPE, XFER_DEST_WRITEFD_TYPE)
make_test_glue(test_glue_READFD_PUSH, XFER_SOURCE_READFD_TYPE, XFER_DEST_PUSH_TYPE)
make_test_glue(test_glue_READFD_PULL, XFER_SOURCE_READFD_TYPE, XFER_DEST_PULL_TYPE)
make_test_glue(test_glue_WRITEFD_READFD, XFER_SOURCE_WRITEFD_TYPE, XFER_DEST_READFD_TYPE)
make_test_glue(test_glue_WRITEFD_WRITE, XFER_SOURCE_WRITEFD_TYPE, XFER_DEST_WRITEFD_TYPE)
make_test_glue(test_glue_WRITEFD_PUSH, XFER_SOURCE_WRITEFD_TYPE, XFER_DEST_PUSH_TYPE)
make_test_glue(test_glue_WRITEFD_PULL, XFER_SOURCE_WRITEFD_TYPE, XFER_DEST_PULL_TYPE)
make_test_glue(test_glue_PUSH_READFD, XFER_SOURCE_PUSH_TYPE, XFER_DEST_READFD_TYPE)
make_test_glue(test_glue_PUSH_WRITE, XFER_SOURCE_PUSH_TYPE, XFER_DEST_WRITEFD_TYPE)
make_test_glue(test_glue_PUSH_PUSH, XFER_SOURCE_PUSH_TYPE, XFER_DEST_PUSH_TYPE)
make_test_glue(test_glue_PUSH_PULL, XFER_SOURCE_PUSH_TYPE, XFER_DEST_PULL_TYPE)
make_test_glue(test_glue_PULL_READFD, XFER_SOURCE_PULL_TYPE, XFER_DEST_READFD_TYPE)
make_test_glue(test_glue_PULL_WRITE, XFER_SOURCE_PULL_TYPE, XFER_DEST_WRITEFD_TYPE)
make_test_glue(test_glue_PULL_PUSH, XFER_SOURCE_PULL_TYPE, XFER_DEST_PUSH_TYPE)
make_test_glue(test_glue_PULL_PULL, XFER_SOURCE_PULL_TYPE, XFER_DEST_PULL_TYPE)

/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_xfer_simple, 90),
	TU_TEST(test_xfer_files_simple, 90),
	TU_TEST(test_xfer_files_filter, 90),
        TU_TEST(test_glue_READFD_READFD, 90),
        TU_TEST(test_glue_READFD_WRITE, 90),
        TU_TEST(test_glue_READFD_PUSH, 90),
        TU_TEST(test_glue_READFD_PULL, 90),
        TU_TEST(test_glue_WRITEFD_READFD, 90),
        TU_TEST(test_glue_WRITEFD_WRITE, 90),
        TU_TEST(test_glue_WRITEFD_PUSH, 90),
        TU_TEST(test_glue_WRITEFD_PULL, 90),
        TU_TEST(test_glue_PUSH_READFD, 90),
        TU_TEST(test_glue_PUSH_WRITE, 90),
        TU_TEST(test_glue_PUSH_PUSH, 90),
        TU_TEST(test_glue_PUSH_PULL, 90),
        TU_TEST(test_glue_PULL_READFD, 90),
        TU_TEST(test_glue_PULL_WRITE, 90),
        TU_TEST(test_glue_PULL_PUSH, 90),
        TU_TEST(test_glue_PULL_PULL, 90),
	TU_END()
    };

    glib_init();

    return testutils_run_tests(argc, argv, tests);
}
