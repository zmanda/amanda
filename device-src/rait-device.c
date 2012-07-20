/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

/* The RAIT device encapsulates some number of other devices into a single
 * redundant device. */

#include "amanda.h"
#include "property.h"
#include "util.h"
#include <glib.h>
#include "glib-util.h"
#include "device.h"
#include "fileheader.h"
#include "amsemaphore.h"

/* Just a note about the failure mode of different operations:
   - Recovers from a failure (enters degraded mode)
     open_device()
     seek_file() -- explodes if headers don't match.
     seek_block() -- explodes if headers don't match.
     read_block() -- explodes if data doesn't match.

   - Operates in degraded mode (but dies if a new problem shows up)
     read_label() -- but dies on label mismatch.
     start() -- but dies when writing in degraded mode.
     property functions
     finish()

   - Dies in degraded mode (even if remaining devices are OK)
     start_file()
     write_block()
     finish_file()
     recycle_file()
*/

/*
 * Type checking and casting macros
 */
#define TYPE_RAIT_DEVICE	(rait_device_get_type())
#define RAIT_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), rait_device_get_type(), RaitDevice)
#define RAIT_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), rait_device_get_type(), RaitDevice const)
#define RAIT_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), rait_device_get_type(), RaitDeviceClass)
#define IS_RAIT_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), rait_device_get_type ())

#define RAIT_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), rait_device_get_type(), RaitDeviceClass)
static GType	rait_device_get_type	(void);

/*
 * Main object structure
 */
typedef struct RaitDevice_s {
    Device __parent__;

    struct RaitDevicePrivate_s * private;
} RaitDevice;

/*
 * Class definition
 */
typedef struct _RaitDeviceClass RaitDeviceClass;
struct _RaitDeviceClass {
    DeviceClass __parent__;
};

typedef enum {
    RAIT_STATUS_COMPLETE, /* All subdevices OK. */
    RAIT_STATUS_DEGRADED, /* One subdevice failed. */
    RAIT_STATUS_FAILED    /* Two or more subdevices failed. */
} RaitStatus;

/* Older versions of glib have a deadlock in their thread pool implementations,
 * so we include a simple thread-pool implementation here to replace it.
 *
 * This implementation assumes that threads are used for paralellizing a single
 * operation, so all threads run a function to completion before the main thread
 * continues.  This simplifies some of the locking semantics, and in particular
 * there is no need to wait for stray threads to finish an operation when
 * finalizing the RaitDevice object or when beginning a new operation.
 */
#if !(GLIB_CHECK_VERSION(2,10,0))
#define USE_INTERNAL_THREADPOOL
#endif

typedef struct RaitDevicePrivate_s {
    GPtrArray * children;
    /* These flags are only relevant for reading. */
    RaitStatus status;
    /* If status == RAIT_STATUS_DEGRADED, this holds the index of the
       failed node. It holds a negative number otherwise. */
    int failed;

    /* the child block size */
    gsize child_block_size;

#ifdef USE_INTERNAL_THREADPOOL
    /* array of ThreadInfo for performing parallel operations */
    GArray *threads;

    /* value of this semaphore is the number of threaded operations
     * in progress */
    amsemaphore_t *threads_sem;
#endif
} RaitDevicePrivate;

#ifdef USE_INTERNAL_THREADPOOL
typedef struct ThreadInfo {
    GThread *thread;

    /* struct fields below are protected by this mutex and condition variable */
    GMutex *mutex;
    GCond *cond;

    gboolean die;
    GFunc func;
    gpointer data;

    /* give threads access to active_threads and its mutex/cond */
    struct RaitDevicePrivate_s *private;
} ThreadInfo;
#endif

/* This device uses a special sentinel node to indicate that the child devices
 * will be set later (in rait_device_open).  It contains a control character to
 * make it difficult to enter accidentally in an Amanda config. */
#define DEFER_CHILDREN_SENTINEL "DEFER\1"

#define PRIVATE(o) (o->private)

#define rait_device_in_error(dev) \
    (device_in_error((dev)) || PRIVATE(RAIT_DEVICE((dev)))->status == RAIT_STATUS_FAILED)

void rait_device_register (void);

/* here are local prototypes */
static void rait_device_init (RaitDevice * o);
static void rait_device_class_init (RaitDeviceClass * c);
static void rait_device_base_init (RaitDeviceClass * c);
static void rait_device_open_device (Device * self, char * device_name, char * device_type, char * device_node);
static gboolean rait_device_start (Device * self, DeviceAccessMode mode,
                                   char * label, char * timestamp);
static gboolean rait_device_configure(Device * self, gboolean use_global_config);
static gboolean rait_device_start_file(Device * self, dumpfile_t * info);
static gboolean rait_device_write_block (Device * self, guint size, gpointer data);
static gboolean rait_device_finish_file (Device * self);
static dumpfile_t * rait_device_seek_file (Device * self, guint file);
static gboolean rait_device_seek_block (Device * self, guint64 block);
static int      rait_device_read_block (Device * self, gpointer buf,
                                        int * size);
static gboolean rait_device_recycle_file (Device * self, guint filenum);
static gboolean rait_device_finish (Device * self);
static DeviceStatusFlags rait_device_read_label(Device * dself);
static void find_simple_params(RaitDevice * self, guint * num_children,
                               guint * data_children);

/* property handlers */

static gboolean property_get_block_size_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_set_block_size_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean property_get_canonical_name_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_get_concurrency_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_get_streaming_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_get_boolean_and_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_get_medium_access_type_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_get_max_volume_usage_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source);

static gboolean property_set_max_volume_usage_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);


/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

static GType
rait_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (RaitDeviceClass),
            (GBaseInitFunc) rait_device_base_init,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) rait_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (RaitDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) rait_device_init,
            NULL
        };

        type = g_type_register_static (TYPE_DEVICE, "RaitDevice", &info,
                                       (GTypeFlags)0);
	}

    return type;
}

static void g_object_unref_foreach(gpointer data,
                                   gpointer user_data G_GNUC_UNUSED) {
    if (data != NULL && G_IS_OBJECT(data)) {
        g_object_unref(data);
    }
}

static void
rait_device_finalize(GObject *obj_self)
{
    RaitDevice *self = RAIT_DEVICE (obj_self);
    if(G_OBJECT_CLASS(parent_class)->finalize) \
           (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);
    if(self->private->children) {
        g_ptr_array_foreach(self->private->children,
                            g_object_unref_foreach, NULL);
        g_ptr_array_free (self->private->children, TRUE);
        self->private->children = NULL;
    }
#ifdef USE_INTERNAL_THREADPOOL
    g_assert(PRIVATE(self)->threads_sem == NULL || PRIVATE(self)->threads_sem->value == 0);

    if (PRIVATE(self)->threads) {
	guint i;

	for (i = 0; i < PRIVATE(self)->threads->len; i++) {
	    ThreadInfo *inf = &g_array_index(PRIVATE(self)->threads, ThreadInfo, i);
	    if (inf->thread) {
		/* NOTE: the thread is waiting on this condition right now, not
		 * executing an operation. */

		/* ask the thread to die */
		g_mutex_lock(inf->mutex);
		inf->die = TRUE;
		g_cond_signal(inf->cond);
		g_mutex_unlock(inf->mutex);

		/* and wait for it to die, which should happen soon */
		g_thread_join(inf->thread);
	    }

	    if (inf->mutex)
		g_mutex_free(inf->mutex);
	    if (inf->cond)
		g_cond_free(inf->cond);
	}
    }

    if (PRIVATE(self)->threads_sem)
	amsemaphore_free(PRIVATE(self)->threads_sem);
#endif
    amfree(self->private);
}

static void
rait_device_init (RaitDevice * o G_GNUC_UNUSED)
{
    PRIVATE(o) = g_new(RaitDevicePrivate, 1);
    PRIVATE(o)->children = g_ptr_array_new();
    PRIVATE(o)->status = RAIT_STATUS_COMPLETE;
    PRIVATE(o)->failed = -1;
#ifdef USE_INTERNAL_THREADPOOL
    PRIVATE(o)->threads = NULL;
    PRIVATE(o)->threads_sem = NULL;
#endif
}

static void
rait_device_class_init (RaitDeviceClass * c)
{
    GObjectClass *g_object_class = (GObjectClass*) c;
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = rait_device_open_device;
    device_class->configure = rait_device_configure;
    device_class->start = rait_device_start;
    device_class->start_file = rait_device_start_file;
    device_class->write_block = rait_device_write_block;
    device_class->finish_file = rait_device_finish_file;
    device_class->seek_file = rait_device_seek_file;
    device_class->seek_block = rait_device_seek_block;
    device_class->read_block = rait_device_read_block;
    device_class->recycle_file = rait_device_recycle_file;
    device_class->finish = rait_device_finish;
    device_class->read_label = rait_device_read_label;

    g_object_class->finalize = rait_device_finalize;

#ifndef USE_INTERNAL_THREADPOOL
#if !GLIB_CHECK_VERSION(2,10,2)
    /* Versions of glib before 2.10.2 crash if
     * g_thread_pool_set_max_unused_threads is called before the first
     * invocation of g_thread_pool_new.  So we make up a thread pool, but don't
     * start any threads in it, and free it */
    {
	GThreadPool *pool = g_thread_pool_new((GFunc)-1, NULL, -1, FALSE, NULL);
	g_thread_pool_free(pool, TRUE, FALSE);
    }
#endif

    g_thread_pool_set_max_unused_threads(-1);
#endif
}

static void
rait_device_base_init (RaitDeviceClass * c)
{
    DeviceClass *device_class = (DeviceClass *)c;

    /* the RAIT device overrides most of the standard properties, so that it
     * can calculate them by querying the same property on the children */
    device_class_register_property(device_class, PROPERTY_BLOCK_SIZE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    property_get_block_size_fn,
	    property_set_block_size_fn);

    device_class_register_property(device_class, PROPERTY_CANONICAL_NAME,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_canonical_name_fn, NULL);

    device_class_register_property(device_class, PROPERTY_CONCURRENCY,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_concurrency_fn, NULL);

    device_class_register_property(device_class, PROPERTY_STREAMING,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_streaming_fn, NULL);

    device_class_register_property(device_class, PROPERTY_APPENDABLE,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_boolean_and_fn, NULL);

    device_class_register_property(device_class, PROPERTY_PARTIAL_DELETION,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_boolean_and_fn, NULL);

    device_class_register_property(device_class, PROPERTY_FULL_DELETION,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_boolean_and_fn, NULL);

    device_class_register_property(device_class, PROPERTY_LEOM,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_boolean_and_fn, NULL);

    device_class_register_property(device_class, PROPERTY_MEDIUM_ACCESS_TYPE,
	    PROPERTY_ACCESS_GET_MASK,
	    property_get_medium_access_type_fn, NULL);

    device_class_register_property(device_class, PROPERTY_MAX_VOLUME_USAGE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    property_get_max_volume_usage_fn,
	    property_set_max_volume_usage_fn);
}

/* This function does something a little clever and a little
 * complicated. It takes an array of operations and runs the given
 * function on each element in the array. The trick is that it runs them
 * all in parallel, in different threads. This is more efficient than it
 * sounds because we use a GThreadPool, which means calling this function
 * will probably not start any new threads at all, but rather use
 * existing ones. The func is called with two gpointer arguments: The
 * first from the array, the second is the data argument.
 *
 * When it returns, all the operations have been successfully
 * executed. If you want results from your operations, do it yourself
 * through the array.
 */

#ifdef USE_INTERNAL_THREADPOOL
static gpointer rait_thread_pool_func(gpointer data) {
    ThreadInfo *inf = data;

    g_mutex_lock(inf->mutex);
    while (TRUE) {
	while (!inf->die && !inf->func)
	    g_cond_wait(inf->cond, inf->mutex);

	if (inf->die)
	    break;

	if (inf->func) {
	    /* invoke the function */
	    inf->func(inf->data, NULL);
	    inf->func = NULL;
	    inf->data = NULL;

            /* indicate that we're finished; will not block */
	    amsemaphore_down(inf->private->threads_sem);
	}
    }
    g_mutex_unlock(inf->mutex);
    return NULL;
}

static void do_thread_pool_op(RaitDevice *self, GFunc func, GPtrArray * ops) {
    guint i;

    if (PRIVATE(self)->threads_sem == NULL)
	PRIVATE(self)->threads_sem = amsemaphore_new_with_value(0);

    if (PRIVATE(self)->threads == NULL)
	PRIVATE(self)->threads = g_array_sized_new(FALSE, TRUE,
					    sizeof(ThreadInfo), ops->len);

    g_assert(PRIVATE(self)->threads_sem->value == 0);

    if (PRIVATE(self)->threads->len < ops->len)
	g_array_set_size(PRIVATE(self)->threads, ops->len);

    /* the semaphore will hit zero when each thread has decremented it */
    amsemaphore_force_set(PRIVATE(self)->threads_sem, ops->len);

    for (i = 0; i < ops->len; i++) {
	ThreadInfo *inf = &g_array_index(PRIVATE(self)->threads, ThreadInfo, i);
	if (!inf->thread) {
	    inf->mutex = g_mutex_new();
	    inf->cond = g_cond_new();
	    inf->private = PRIVATE(self);
	    inf->thread = g_thread_create(rait_thread_pool_func, inf, TRUE, NULL);
	}

	/* set up the info the thread needs and trigger it to start */
	g_mutex_lock(inf->mutex);
	inf->data = g_ptr_array_index(ops, i);
	inf->func = func;
	g_cond_signal(inf->cond);
	g_mutex_unlock(inf->mutex);
    }

    /* wait until semaphore hits zero */
    amsemaphore_wait_empty(PRIVATE(self)->threads_sem);
}

#else /* USE_INTERNAL_THREADPOOL */

static void do_thread_pool_op(RaitDevice *self G_GNUC_UNUSED, GFunc func, GPtrArray * ops) {
    GThreadPool * pool;
    guint i;

    pool = g_thread_pool_new(func, NULL, -1, FALSE, NULL);
    for (i = 0; i < ops->len; i ++) {
        g_thread_pool_push(pool, g_ptr_array_index(ops, i), NULL);
    }

    g_thread_pool_free(pool, FALSE, TRUE);
}

#endif /* USE_INTERNAL_THREADPOOL */

/* This does the above, in a serial fashion (and without using threads) */
static void do_unthreaded_ops(RaitDevice *self G_GNUC_UNUSED, GFunc func, GPtrArray * ops) {
    guint i;

    for (i = 0; i < ops->len; i ++) {
        func(g_ptr_array_index(ops, i), NULL);
    }
}

/* This is the one that code below should call. It switches
   automatically between do_thread_pool_op and do_unthreaded_ops,
   depending on g_thread_supported(). */
static void do_rait_child_ops(RaitDevice *self, GFunc func, GPtrArray * ops) {
    if (g_thread_supported()) {
        do_thread_pool_op(self, func, ops);
    } else {
        do_unthreaded_ops(self, func, ops);
    }
}

static char *
child_device_names_to_rait_name(RaitDevice * self) {
    GPtrArray *kids;
    char *braced, *result;
    guint i;

    kids = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
	Device *child = g_ptr_array_index(self->private->children, i);
	const char *child_name = NULL;
        GValue val;
	gboolean got_prop = FALSE;

        bzero(&val, sizeof(val));

        if ((signed)i != self->private->failed) {
	    if (device_property_get(child, PROPERTY_CANONICAL_NAME, &val)) {
		child_name = g_value_get_string(&val);
		got_prop = TRUE;
	    }
	}

	if (!got_prop)
            child_name = "MISSING";

	g_ptr_array_add(kids, g_strdup(child_name));

	if (got_prop)
	    g_value_unset(&val);
    }

    braced = collapse_braced_alternates(kids);
    result = g_strdup_printf("rait:%s", braced);
    g_free(braced);

    return result;
}

/* Find a workable child block size, based on the block size ranges of our
 * child devices.
 *
 * The algorithm is to construct the intersection of all child devices'
 * [min,max] block size ranges, and then pick the block size closest to 32k
 * that is in the resulting range.  This avoids picking ridiculously small (1
 * byte) or large (INT_MAX) block sizes when using devices with wide-open block
 * size ranges.

 * This function returns the calculated child block size directly, and the RAIT
 * device's blocksize via rait_size, if not NULL.  It is resilient to errors in
 * a single child device, but sets the device's error status and returns 0 if
 * it cannot determine an agreeable block size.
 */
static gsize
calculate_block_size_from_children(RaitDevice * self, gsize *rait_size)
{
    gsize min = 0;
    gsize max = SIZE_MAX;
    gboolean found_one = FALSE;
    gsize result;
    guint i;

    for (i = 0; i < self->private->children->len; i ++) {
        gsize child_min = SIZE_MAX, child_max = 0;
	Device *child;
        GValue property_result;
	PropertySource source;

        bzero(&property_result, sizeof(property_result));

	if ((signed)i == self->private->failed)
	    continue;

	child = g_ptr_array_index(self->private->children, i);
        if (!device_property_get_ex(child, PROPERTY_BLOCK_SIZE,
				 &property_result, NULL, &source)) {
	    g_warning("Error getting BLOCK_SIZE from %s: %s",
		    child->device_name, device_error_or_status(child));
            continue;
	}

	/* if the block size has been set explicitly, then we need to use that blocksize;
	 * otherwise (even if it was DETECTED), override it. */
	if (source == PROPERTY_SOURCE_USER) {
	    child_min = child_max = g_value_get_int(&property_result);
	} else {
	    if (!device_property_get(child, PROPERTY_MIN_BLOCK_SIZE,
				     &property_result)) {
		g_warning("Error getting MIN_BLOCK_SIZE from %s: %s",
			child->device_name, device_error_or_status(child));
		continue;
	    }
	    child_min = g_value_get_uint(&property_result);

	    if (!device_property_get(child, PROPERTY_MAX_BLOCK_SIZE,
				     &property_result)) {
		g_warning("Error getting MAX_BLOCK_SIZE from %s: %s",
			child->device_name, device_error_or_status(child));
		continue;
	    }
	    child_max = g_value_get_uint(&property_result);

	    if (child_min == 0 || child_max == 0 || (child_min > child_max)) {
		g_warning("Invalid min, max block sizes from %s", child->device_name);
		continue;
	    }
	}

	found_one = TRUE;
	min = MAX(min, child_min);
	max = MIN(max, child_max);
    }

    if (!found_one) {
	device_set_error((Device*)self,
	    stralloc(_("Could not find any child devices' block size ranges")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return 0;
    }

    if (min > max) {
	device_set_error((Device*)self,
	    stralloc(_("No block size is acceptable to all child devices")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return 0;
    }

    /* Now pick a number.  If 32k is in range, we use that; otherwise, we use
     * the nearest acceptable size. */
    result = CLAMP(32768, min, max);

    if (rait_size) {
	guint data_children;
	find_simple_params(self, NULL, &data_children);
	*rait_size = result * data_children;
    }

    return result;
}

/* Set BLOCK_SIZE on all children */
static gboolean
set_block_size_on_children(RaitDevice *self, gsize child_block_size)
{
    GValue val;
    guint i;
    PropertySource source;

    bzero(&val, sizeof(val));

    g_assert(child_block_size < INT_MAX);
    g_value_init(&val, G_TYPE_INT);
    g_value_set_int(&val, (gint)child_block_size);

    for (i = 0; i < self->private->children->len; i ++) {
	Device *child;
	GValue property_result;

	bzero(&property_result, sizeof(property_result));

	if ((signed)i == self->private->failed)
	    continue;

	child = g_ptr_array_index(self->private->children, i);

	/* first, make sure the block size is at its default, or is already
	 * correct */
        if (device_property_get_ex(child, PROPERTY_BLOCK_SIZE,
				 &property_result, NULL, &source)) {
	    gsize from_child = g_value_get_int(&property_result);
	    g_value_unset(&property_result);
	    if (source != PROPERTY_SOURCE_DEFAULT
		    && from_child != child_block_size) {
		device_set_error((Device *)self,
		    vstrallocf(_("Child device %s already has its block size set to %zd, not %zd"),
				child->device_name, from_child, child_block_size),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	} else {
	    /* failing to get the block size isn't necessarily fatal.. */
	    g_warning("Error getting BLOCK_SIZE from %s: %s",
		    child->device_name, device_error_or_status(child));
	}

	if (!device_property_set(child, PROPERTY_BLOCK_SIZE, &val)) {
	    device_set_error((Device *)self,
		vstrallocf(_("Error setting block size on %s"), child->device_name),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}
    }

    return TRUE;
}

/* The time for users to specify block sizes has ended; set this device's
 * block-size attributes for easy access by other RAIT functions.  Returns
 * FALSE on error, with the device's error status already set. */
static gboolean
fix_block_size(RaitDevice *self)
{
    Device *dself = (Device *)self;
    gsize my_block_size, child_block_size;

    if (dself->block_size_source == PROPERTY_SOURCE_DEFAULT) {
	child_block_size = calculate_block_size_from_children(self, &my_block_size);
	if (child_block_size == 0)
	    return FALSE;

	self->private->child_block_size = child_block_size;
	dself->block_size = my_block_size;
	dself->block_size_surety = PROPERTY_SURETY_GOOD;
	dself->block_size_source = PROPERTY_SOURCE_DETECTED;
    } else {
	guint data_children;

	find_simple_params(self, NULL, &data_children);
	g_assert((dself->block_size % data_children) == 0);
	child_block_size = dself->block_size / data_children;
    }

    /* now tell the children we mean it */
    if (!set_block_size_on_children(self, child_block_size))
	return FALSE;

    return TRUE;
}

/* This structure contains common fields for many operations. Not all
   operations use all fields, however. */
typedef struct {
    gpointer result; /* May be a pointer; may be an integer or boolean
                        stored with GINT_TO_POINTER. */
    Device * child;  /* The device in question. Used by all
                        operations. */
    guint child_index; /* For recoverable operations (read-related
                          operations), this field provides the number
                          of this child in the self->private->children
                          array. */
} GenericOp;

typedef gboolean (*BooleanExtractor)(gpointer data);

/* A BooleanExtractor */
static gboolean extract_boolean_generic_op(gpointer data) {
    GenericOp * op = data;
    return GPOINTER_TO_INT(op->result);
}

/* A BooleanExtractor */
static gboolean extract_boolean_pointer_op(gpointer data) {
    GenericOp * op = data;
    return op->result != NULL;
}

/* Does the equivalent of this perl command:
     ! (first { !extractor($_) } @_
   That is, calls extractor on each element of the array, and returns
   TRUE if and only if all calls to extractor return TRUE. This function
   stops as soon as an extractor returns false, so it's best if extractor
   functions have no side effects.
*/
static gboolean g_ptr_array_and(GPtrArray * array,
                                BooleanExtractor extractor) {
    guint i;
    if (array == NULL || array->len <= 0)
        return FALSE;

    for (i = 0; i < array->len; i ++) {
        if (!extractor(g_ptr_array_index(array, i)))
            return FALSE;
    }

    return TRUE;
}

/* Takes a RaitDevice, and makes a GPtrArray of GenericOp. */
static GPtrArray * make_generic_boolean_op_array(RaitDevice* self) {
    GPtrArray * rval;
    guint i;

    rval = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        GenericOp * op;

        if ((signed)i == self->private->failed) {
            continue;
        }

        op = g_new(GenericOp, 1);
        op->child = g_ptr_array_index(self->private->children, i);
        op->child_index = i;
        g_ptr_array_add(rval, op);
    }

    return rval;
}

/* Takes a GPtrArray of GenericOp, and a BooleanExtractor, and does
   all the proper handling for the result of operations that allow
   device isolation. Returns FALSE only if an unrecoverable error
   occured. */
static gboolean g_ptr_array_union_robust(RaitDevice * self, GPtrArray * ops,
                                         BooleanExtractor extractor) {
    int nfailed = 0;
    int lastfailed = 0;
    guint i;

    /* We found one or more failed elements.  See which elements failed, and
     * isolate them*/
    for (i = 0; i < ops->len; i ++) {
	GenericOp * op = g_ptr_array_index(ops, i);
	if (!extractor(op)) {
	    self->private->failed = op->child_index;
	    g_warning("RAIT array %s isolated device %s: %s",
		    DEVICE(self)->device_name,
		    op->child->device_name,
		    device_error(op->child));
	    nfailed++;
            lastfailed = i;
	}
    }

    /* no failures? great! */
    if (nfailed == 0)
	return TRUE;

    /* a single failure in COMPLETE just puts us in DEGRADED mode */
    if (self->private->status == RAIT_STATUS_COMPLETE && nfailed == 1) {
	self->private->status = RAIT_STATUS_DEGRADED;
        self->private->failed = lastfailed;
	g_warning("RAIT array %s DEGRADED", DEVICE(self)->device_name);
	return TRUE;
    } else {
	self->private->status = RAIT_STATUS_FAILED;
	g_warning("RAIT array %s FAILED", DEVICE(self)->device_name);
	return FALSE;
    }
}

typedef struct {
    RaitDevice * self;
    char *rait_name;
    char * device_name; /* IN */
    Device * result;    /* OUT */
} OpenDeviceOp;

/* A GFunc. */
static void device_open_do_op(gpointer data,
                              gpointer user_data G_GNUC_UNUSED) {
    OpenDeviceOp * op = data;

    if (strcmp(op->device_name, "ERROR") == 0 ||
        strcmp(op->device_name, "MISSING") == 0 ||
        strcmp(op->device_name, "DEGRADED") == 0) {
        g_warning("RAIT device %s contains a missing element, attempting "
                  "degraded mode.\n", op->rait_name);
        op->result = NULL;
    } else {
        op->result = device_open(op->device_name);
    }
}

/* Returns TRUE if and only if the volume label and time are equal. */
static gboolean compare_volume_results(Device * a, Device * b) {
    return (0 == compare_possibly_null_strings(a->volume_time, b->volume_time)
	 && 0 == compare_possibly_null_strings(a->volume_label, b->volume_label));
}

/* Stickes new_message at the end of *old_message; frees new_message and
 * may change *old_message. */
static void append_message(char ** old_message, char * new_message) {
    char * rval;
    if (*old_message == NULL || **old_message == '\0') {
        rval = new_message;
    } else {
        rval = g_strdup_printf("%s; %s", *old_message, new_message);
        amfree(new_message);
    }
    amfree(*old_message);
    *old_message = rval;
}

static gboolean
open_child_devices (Device * dself, char * device_name,
	    char * device_node) {
    GPtrArray *device_names;
    GPtrArray * device_open_ops;
    guint i;
    gboolean failure;
    char *failure_errmsgs;
    DeviceStatusFlags failure_flags;
    RaitDevice * self;

    self = RAIT_DEVICE(dself);

    device_names = expand_braced_alternates(device_node);

    if (device_names == NULL) {
	device_set_error(dself,
	    vstrallocf(_("Invalid RAIT device name '%s'"), device_name),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    /* Open devices in a separate thread, in case they have to rewind etc. */
    device_open_ops = g_ptr_array_new();

    for (i = 0; i < device_names->len; i++) {
        OpenDeviceOp *op;
	char *name = g_ptr_array_index(device_names, i);

        op = g_new(OpenDeviceOp, 1);
        op->device_name = name;
        op->result = NULL;
        op->self = self;
	op->rait_name = device_name;
        g_ptr_array_add(device_open_ops, op);
    }

    g_ptr_array_free(device_names, TRUE);
    do_rait_child_ops(self, device_open_do_op, device_open_ops);

    failure = FALSE;
    failure_errmsgs = NULL;
    failure_flags = 0;

    /* Check results of opening devices. */
    for (i = 0; i < device_open_ops->len; i ++) {
        OpenDeviceOp *op = g_ptr_array_index(device_open_ops, i);

        if (op->result != NULL &&
            op->result->status == DEVICE_STATUS_SUCCESS) {
	    g_ptr_array_add(self->private->children, op->result);
        } else {
            char * this_failure_errmsg =
                g_strdup_printf("%s: %s", op->device_name,
                                device_error_or_status(op->result));
            DeviceStatusFlags status =
                op->result == NULL ?
                    DEVICE_STATUS_DEVICE_ERROR : op->result->status;
            append_message(&failure_errmsgs,
                           strdup(this_failure_errmsg));
	    failure_flags |= status;
            if (self->private->status == RAIT_STATUS_COMPLETE) {
                /* The first failure just puts us in degraded mode. */
                g_warning("%s: %s",
                          device_name, this_failure_errmsg);
		g_warning("%s: %s failed, entering degraded mode.",
                          device_name, op->device_name);
                g_ptr_array_add(self->private->children, op->result);
                self->private->status = RAIT_STATUS_DEGRADED;
                self->private->failed = i;
            } else {
                /* The second and further failures are fatal. */
                failure = TRUE;
            }
        }
        amfree(op->device_name);
    }

    g_ptr_array_free_full(device_open_ops);

    if (failure) {
        self->private->status = RAIT_STATUS_FAILED;
	device_set_error(dself, failure_errmsgs, failure_flags);
        return FALSE;
    }

    return TRUE;
}

static void
rait_device_open_device (Device * dself, char * device_name,
	    char * device_type G_GNUC_UNUSED, char * device_node) {

    if (0 != strcmp(device_node, DEFER_CHILDREN_SENTINEL)) {
	if (!open_child_devices(dself, device_name, device_node))
	    return;

	/* Chain up. */
	if (parent_class->open_device) {
	    parent_class->open_device(dself, device_name, device_type, device_node);
	}
    }
}

Device *
rait_device_open_from_children (GSList *child_devices) {
    Device *dself;
    RaitDevice *self;
    GSList *iter;
    char *device_name;
    int nfailures;
    int i;

    /* first, open a RAIT device using the DEFER_CHILDREN_SENTINEL */
    dself = device_open("rait:" DEFER_CHILDREN_SENTINEL);
    if (!IS_RAIT_DEVICE(dself)) {
	return dself;
    }

    /* set its children */
    self = RAIT_DEVICE(dself);
    nfailures = 0;
    for (i=0, iter = child_devices; iter; i++, iter = iter->next) {
	Device *kid = iter->data;

	/* a NULL kid is OK -- it opens the device in degraded mode */
	if (!kid) {
	    nfailures++;
	    self->private->failed = i;
	} else {
	    g_assert(IS_DEVICE(kid));
	    g_object_ref((GObject *)kid);
	}

	g_ptr_array_add(self->private->children, kid);
    }

    /* and set the status based on the children */
    switch (nfailures) {
	case 0:
	    self->private->status = RAIT_STATUS_COMPLETE;
	    break;

	case 1:
	    self->private->status = RAIT_STATUS_DEGRADED;
	    break;

	default:
	    self->private->status = RAIT_STATUS_FAILED;
	    device_set_error(dself,
		    _("more than one child device is missing"),
		    DEVICE_STATUS_DEVICE_ERROR);
	    break;
    }

    /* create a name from the children's names and use it to chain up
     * to open_device (we skipped this step in rait_device_open_device) */
    device_name = child_device_names_to_rait_name(self);

    if (parent_class->open_device) {
	parent_class->open_device(dself,
	    device_name, "rait",
	    device_name+5); /* (+5 skips "rait:") */
    }

    return dself;
}

/* A GFunc. */
static void read_label_do_op(gpointer data,
                             gpointer user_data G_GNUC_UNUSED) {
    GenericOp * op = data;
    op->result = GINT_TO_POINTER(device_read_label(op->child));
}

static DeviceStatusFlags rait_device_read_label(Device * dself) {
    RaitDevice * self;
    GPtrArray * ops;
    DeviceStatusFlags failed_result = 0;
    char *failed_errmsg = NULL;
    unsigned int i;
    Device * first_success = NULL;

    self = RAIT_DEVICE(dself);

    amfree(dself->volume_time);
    amfree(dself->volume_label);
    dumpfile_free(dself->volume_header);
    dself->volume_header = NULL;

    if (rait_device_in_error(self))
        return dself->status | DEVICE_STATUS_DEVICE_ERROR;

    /* nail down our block size, if we haven't already */
    if (!fix_block_size(self))
	return FALSE;

    ops = make_generic_boolean_op_array(self);

    do_rait_child_ops(self, read_label_do_op, ops);

    for (i = 0; i < ops->len; i ++) {
        GenericOp * op = g_ptr_array_index(ops, i);
        DeviceStatusFlags result = GPOINTER_TO_INT(op->result);
        if (op->result == DEVICE_STATUS_SUCCESS) {
            if (first_success == NULL) {
                /* This is the first successful device. */
                first_success = op->child;
            } else if (!compare_volume_results(first_success, op->child)) {
                /* Doesn't match. :-( */
		failed_errmsg = vstrallocf("Inconsistent volume labels/datestamps: "
                        "Got %s/%s on %s against %s/%s on %s.",
                        first_success->volume_label,
                        first_success->volume_time,
			first_success->device_name,
                        op->child->volume_label,
                        op->child->volume_time,
			op->child->device_name);
		g_warning("%s", failed_errmsg);
                failed_result |= DEVICE_STATUS_VOLUME_ERROR;
            }
        } else {
            failed_result |= result;
        }
    }

    if (failed_result != DEVICE_STATUS_SUCCESS) {
        /* We had multiple failures or an inconsistency. */
	device_set_error(dself, failed_errmsg, failed_result);
    } else {
        /* Everything peachy. */
	amfree(failed_errmsg);

        g_assert(first_success != NULL);
        if (first_success->volume_label != NULL) {
            dself->volume_label = g_strdup(first_success->volume_label);
        }
        if (first_success->volume_time != NULL) {
            dself->volume_time = g_strdup(first_success->volume_time);
        }
        if (first_success->volume_header != NULL) {
            dself->volume_header = dumpfile_copy(first_success->volume_header);
        }
	dself->header_block_size = first_success->header_block_size;
    }

    g_ptr_array_free_full(ops);

    return dself->status;
}

typedef struct {
    GenericOp base;
    DeviceAccessMode mode; /* IN */
    char * label;          /* IN */
    char * timestamp;      /* IN */
} StartOp;

/* A GFunc. */
static void start_do_op(gpointer data, gpointer user_data G_GNUC_UNUSED) {
    DeviceClass *klass;
    StartOp * param = data;

    klass = DEVICE_GET_CLASS(param->base.child);
    if (klass->start) {
        param->base.result =
            GINT_TO_POINTER((klass->start)(param->base.child,
                                            param->mode, param->label,
                                            param->timestamp));
    } else {
        param->base.result = FALSE;
    }
}

static gboolean
rait_device_configure(Device * dself, gboolean use_global_config)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    guint i;

    for (i = 0; i < self->private->children->len; i ++) {
	Device *child;

	if ((signed)i == self->private->failed)
	    continue;

	child = g_ptr_array_index(self->private->children, i);
	/* unconditionally configure the child without the global
	 * configuration */
	if (!device_configure(child, FALSE))
	    return FALSE;
    }

    if (parent_class->configure) {
        return parent_class->configure(dself, use_global_config);
    }

    return TRUE;
}

static gboolean
rait_device_start (Device * dself, DeviceAccessMode mode, char * label,
                   char * timestamp) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    RaitDevice * self;
    DeviceStatusFlags total_status;
    char *failure_errmsgs = NULL;
    char * label_from_device = NULL;

    self = RAIT_DEVICE(dself);

    if (rait_device_in_error(self)) return FALSE;

    /* No starting in degraded mode. */
    if (self->private->status != RAIT_STATUS_COMPLETE &&
        (mode == ACCESS_WRITE || mode == ACCESS_APPEND)) {
        device_set_error(dself,
                         g_strdup_printf(_("RAIT device %s is read-only "
                                           "because it is in degraded mode.\n"),
                                         dself->device_name),
                         DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    /* nail down our block size, if we haven't already */
    if (!fix_block_size(self))
	return FALSE;

    dself->access_mode = mode;
    g_mutex_lock(dself->device_mutex);
    dself->in_file = FALSE;
    g_mutex_unlock(dself->device_mutex);
    amfree(dself->volume_label);
    amfree(dself->volume_time);
    dumpfile_free(dself->volume_header);
    dself->volume_header = NULL;

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        StartOp * op;

        if ((signed)i == self->private->failed) {
            continue;
        }

        op = g_new(StartOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->mode = mode;
        op->label = g_strdup(label);
        op->timestamp = g_strdup(timestamp);
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, start_do_op, ops);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    /* Check results of starting devices; this is mostly about the
     * VOLUME_UNLABELED flag. */
    total_status = 0;
    for (i = 0; i < ops->len; i ++) {
        StartOp * op = g_ptr_array_index(ops, i);
        Device *child = op->base.child;

        total_status |= child->status;
	if (child->status != DEVICE_STATUS_SUCCESS) {
	    /* record the error message and move on. */
            append_message(&failure_errmsgs,
                           g_strdup_printf("%s: %s",
                                           child->device_name,
                                           device_error_or_status(child)));
        } else {
	    if (child->volume_label != NULL && child->volume_time != NULL) {
                if (label_from_device) {
                    if (strcmp(child->volume_label, dself->volume_label) != 0 ||
                        strcmp(child->volume_time, dself->volume_time) != 0) {
                        /* Mismatch! (Two devices provided different labels) */
                        char * this_message =
                            g_strdup_printf("%s: Label (%s/%s) is different "
                                            "from label (%s/%s) found at "
                                            "device %s",
                                            child->device_name,
                                            child->volume_label,
                                            child->volume_time,
                                            dself->volume_label,
                                            dself->volume_time,
                                            label_from_device);
                        append_message(&failure_errmsgs, this_message);
                        total_status |= DEVICE_STATUS_DEVICE_ERROR;
			g_warning("RAIT device children have different labels or timestamps");
                    }
                } else {
                    /* First device with a volume. */
                    dself->volume_label = g_strdup(child->volume_label);
                    dself->volume_time = g_strdup(child->volume_time);
                    dself->volume_header = dumpfile_copy(child->volume_header);
                    label_from_device = g_strdup(child->device_name);
                }
            } else {
                /* Device problem, it says it succeeded but sets no label? */
                char * this_message =
                    g_strdup_printf("%s: Says label read, but no volume "
                                     "label found.", child->device_name);
		g_warning("RAIT device child has NULL volume or label");
                append_message(&failure_errmsgs, this_message);
                total_status |= DEVICE_STATUS_DEVICE_ERROR;
            }
	}
    }

    if (total_status == DEVICE_STATUS_SUCCESS) {
	StartOp * op = g_ptr_array_index(ops, 0);
	Device *child = op->base.child;
	dself->header_block_size = child->header_block_size;
    }

    amfree(label_from_device);
    g_ptr_array_free_full(ops);

    dself->status = total_status;

    if (total_status != DEVICE_STATUS_SUCCESS || !success) {
	device_set_error(dself, failure_errmsgs, total_status);
        return FALSE;
    }
    amfree(failure_errmsgs);
    return TRUE;
}

typedef struct {
    GenericOp base;
    dumpfile_t * info; /* IN */
    int fileno;
} StartFileOp;

/* a GFunc */
static void start_file_do_op(gpointer data, gpointer user_data G_GNUC_UNUSED) {
    StartFileOp * op = data;
    op->base.result = GINT_TO_POINTER(device_start_file(op->base.child,
                                                        op->info));
    op->fileno = op->base.child->file;
    if (op->fileno < 1) {
        op->base.result = FALSE;
    }
}

static gboolean
rait_device_start_file (Device * dself, dumpfile_t * info) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    RaitDevice * self;
    int actual_file = -1;

    self = RAIT_DEVICE(dself);

    if (rait_device_in_error(self)) return FALSE;
    if (self->private->status != RAIT_STATUS_COMPLETE) return FALSE;

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        StartFileOp * op;
        op = g_new(StartFileOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
	/* each child gets its own copy of the header, to munge as it
	 * likes (setting blocksize, at least) */
        op->info = dumpfile_copy(info);
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, start_file_do_op, ops);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    for (i = 0; i < self->private->children->len && success; i ++) {
        StartFileOp * op = g_ptr_array_index(ops, i);
        if (!op->base.result)
            continue;
        g_assert(op->fileno >= 1);
        if (actual_file < 1) {
            actual_file = op->fileno;
        }
        if (actual_file != op->fileno) {
            /* File number mismatch! Aah, my hair is on fire! */
            device_set_error(dself,
                             g_strdup_printf("File number mismatch in "
                                             "rait_device_start_file(): "
                                             "Child %s reported file number "
                                             "%d, another child reported "
                                             "file number %d.",
                                             op->base.child->device_name,
                                             op->fileno, actual_file),
                             DEVICE_STATUS_DEVICE_ERROR);
            success = FALSE;
            op->base.result = FALSE;
        }
    }

    for (i = 0; i < ops->len && success; i ++) {
        StartFileOp * op = g_ptr_array_index(ops, i);
	if (op->info) dumpfile_free(op->info);
    }
    g_ptr_array_free_full(ops);

    if (!success) {
        if (!device_in_error(dself)) {
            device_set_error(dself, stralloc("One or more devices "
                                             "failed to start_file"),
                             DEVICE_STATUS_DEVICE_ERROR);
        }
        return FALSE;
    }

    g_assert(actual_file >= 1);
    dself->file = actual_file;
    g_mutex_lock(dself->device_mutex);
    dself->in_file = TRUE;
    dself->bytes_written = 0;
    g_mutex_unlock(dself->device_mutex);

    return TRUE;
}

static void find_simple_params(RaitDevice * self,
                               guint * num_children,
                               guint * data_children) {
    int num, data;

    num = self->private->children->len;
    if (num > 1)
        data = num - 1;
    else
        data = num;
    if (num_children != NULL)
        *num_children = num;
    if (data_children != NULL)
        *data_children = data;
}

typedef struct {
    GenericOp base;
    guint size;           /* IN */
    gpointer data;        /* IN */
    gboolean data_needs_free; /* bookkeeping */
} WriteBlockOp;

/* a GFunc. */
static void write_block_do_op(gpointer data,
                              gpointer user_data G_GNUC_UNUSED) {
    WriteBlockOp * op = data;

    op->base.result =
        GINT_TO_POINTER(device_write_block(op->base.child, op->size, op->data));
}

/* Parity block generation. Performance of this function can be improved
   considerably by using larger-sized integers or
   assembly-coded vector instructions. Parameters are:
   % data       - All data chunks in series (chunk_size * num_chunks bytes)
   % parity     - Allocated space for parity block (chunk_size bytes)
 */
static void make_parity_block(char * data, char * parity,
                              guint chunk_size, guint num_chunks) {
    guint i;
    bzero(parity, chunk_size);
    for (i = 0; i < num_chunks - 1; i ++) {
        guint j;
        for (j = 0; j < chunk_size; j ++) {
            parity[j] ^= data[chunk_size*i + j];
        }
    }
}

/* Does the same thing as make_parity_block, but instead of using a
   single memory chunk holding all chunks, it takes a GPtrArray of
   chunks. */
static void make_parity_block_extents(GPtrArray * data, char * parity,
                                      guint chunk_size) {
    guint i;
    bzero(parity, chunk_size);
    for (i = 0; i < data->len; i ++) {
        guint j;
        char * data_chunk;
        data_chunk = g_ptr_array_index(data, i);
        for (j = 0; j < chunk_size; j ++) {
            parity[j] ^= data_chunk[j];
        }
    }
}

/* Does the parity creation algorithm. Allocates and returns a single
   device block from a larger RAIT block. chunks and chunk are 1-indexed. */
static char * extract_data_block(char * data, guint size,
                                 guint chunks, guint chunk) {
    char * rval;
    guint chunk_size;

    g_assert(chunks > 0 && chunk > 0 && chunk <= chunks);
    g_assert(data != NULL);
    g_assert(size > 0 && size % (chunks - 1) == 0);

    chunk_size = size / (chunks - 1);
    rval = g_malloc(chunk_size);
    if (chunks != chunk) {
        /* data block. */
        memcpy(rval, data + chunk_size * (chunk - 1), chunk_size);
    } else {
        make_parity_block(data, rval, chunk_size, chunks);
    }

    return rval;
}

static gboolean
rait_device_write_block (Device * dself, guint size, gpointer data) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    guint data_children, num_children;
    gsize blocksize = dself->block_size;
    RaitDevice * self;
    gboolean last_block = (size < blocksize);

    self = RAIT_DEVICE(dself);

    if (rait_device_in_error(self)) return FALSE;
    if (self->private->status != RAIT_STATUS_COMPLETE) return FALSE;

    find_simple_params(RAIT_DEVICE(self), &num_children, &data_children);
    num_children = self->private->children->len;
    if (num_children != 1)
        data_children = num_children - 1;
    else
        data_children = num_children;

    g_assert(size % data_children == 0 || last_block);

    /* zero out to the end of a short block -- tape devices only write
     * whole blocks. */
    if (last_block) {
        char *new_data;

        new_data = g_malloc(blocksize);
        memcpy(new_data, data, size);
        bzero(new_data + size, blocksize - size);

        data = new_data;
        size = blocksize;
    }

    ops = g_ptr_array_sized_new(num_children);
    for (i = 0; i < self->private->children->len; i ++) {
        WriteBlockOp * op;
        op = g_malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->size = size / data_children;
        if (num_children <= 2) {
            op->data = data;
            op->data_needs_free = FALSE;
        } else {
            op->data_needs_free = TRUE;
            op->data = extract_data_block(data, size, num_children, i + 1);
        }
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, write_block_do_op, ops);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    for (i = 0; i < self->private->children->len; i ++) {
        WriteBlockOp * op = g_ptr_array_index(ops, i);
        if (op->data_needs_free)
            free(op->data);
    }

    if (last_block) {
        amfree(data);
    }

    g_ptr_array_free_full(ops);

    if (!success) {
	/* TODO be more specific here */
	/* TODO: handle EOM here -- if one or more (or two or more??)
	 * children have is_eom set, then reflect that in our error
	 * status. What's more fun is when one device fails and must be isolated at
	 * the same time another hits EOF. */
	device_set_error(dself,
	    stralloc("One or more devices failed to write_block"),
	    DEVICE_STATUS_DEVICE_ERROR);
        /* this is EOM or an error, so call it EOM */
        dself->is_eom = TRUE;
        return FALSE;
    } else {
        dself->block ++;
	g_mutex_lock(dself->device_mutex);
	dself->bytes_written += size;
	g_mutex_unlock(dself->device_mutex);

        return TRUE;
    }
}

/* A GFunc */
static void finish_file_do_op(gpointer data,
                              gpointer user_data G_GNUC_UNUSED) {
    GenericOp * op = data;
    if (op->child) {
        op->result = GINT_TO_POINTER(device_finish_file(op->child));
    } else {
        op->result = FALSE;
    }
}

static gboolean
rait_device_finish_file (Device * dself) {
    GPtrArray * ops;
    gboolean success;
    RaitDevice * self = RAIT_DEVICE(dself);

    g_assert(self != NULL);
    if (rait_device_in_error(dself)) return FALSE;
    if (self->private->status != RAIT_STATUS_COMPLETE) return FALSE;

    ops = make_generic_boolean_op_array(self);

    do_rait_child_ops(self, finish_file_do_op, ops);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
	/* TODO: be more specific here */
	device_set_error(dself,
                         g_strdup("One or more devices failed to finish_file"),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    g_mutex_lock(dself->device_mutex);
    dself->in_file = FALSE;
    g_mutex_unlock(dself->device_mutex);
    return TRUE;
}

typedef struct {
    GenericOp base;
    guint requested_file;                /* IN */
    guint actual_file;                   /* OUT */
} SeekFileOp;

/* a GFunc. */
static void seek_file_do_op(gpointer data, gpointer user_data G_GNUC_UNUSED) {
    SeekFileOp * op = data;
    op->base.result = device_seek_file(op->base.child, op->requested_file);
    op->actual_file = op->base.child->file;
}

static dumpfile_t *
rait_device_seek_file (Device * dself, guint file) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    dumpfile_t * rval;
    RaitDevice * self = RAIT_DEVICE(dself);
    guint actual_file = 0;
    gboolean in_file = FALSE;

    if (rait_device_in_error(self)) return NULL;

    dself->is_eof = FALSE;
    dself->block = 0;
    g_mutex_lock(dself->device_mutex);
    dself->in_file = FALSE;
    dself->bytes_read = 0;
    g_mutex_unlock(dself->device_mutex);

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        SeekFileOp * op;
        if ((int)i == self->private->failed)
            continue; /* This device is broken. */
        op = g_new(SeekFileOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->base.child_index = i;
        op->requested_file = file;
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, seek_file_do_op, ops);

    /* This checks for NULL values, but we still have to check for
       consistant headers. */
    success = g_ptr_array_union_robust(RAIT_DEVICE(self),
                                       ops, extract_boolean_pointer_op);

    rval = NULL;
    for (i = 0; i < ops->len; i ++) {
        SeekFileOp * this_op;
        dumpfile_t * this_result;
        guint this_actual_file;
	gboolean this_in_file;

        this_op = (SeekFileOp*)g_ptr_array_index(ops, i);

        if ((signed)this_op->base.child_index == self->private->failed)
            continue;

        this_result = this_op->base.result;
        this_actual_file = this_op->actual_file;
	this_in_file = this_op->base.child->in_file;

        if (rval == NULL) {
            rval = this_result;
            actual_file = this_actual_file;
	    in_file = this_in_file;
        } else {
            if (headers_are_equal(rval, this_result) &&
                actual_file == this_actual_file &&
		in_file == this_in_file) {
                /* Do nothing. */
            } else {
                success = FALSE;
            }
            free(this_result);
        }
    }

    g_ptr_array_free_full(ops);

    if (!success) {
        amfree(rval);
	/* TODO: be more specific here */
	device_set_error(dself,
                         g_strdup("One or more devices failed to seek_file"),
	    DEVICE_STATUS_DEVICE_ERROR);
        return NULL;
    }

    /* update our state */
    g_mutex_lock(dself->device_mutex);
    dself->in_file = in_file;
    g_mutex_unlock(dself->device_mutex);
    dself->file = actual_file;

    return rval;
}

typedef struct {
    GenericOp base;
    guint64 block; /* IN */
} SeekBlockOp;

/* a GFunc. */
static void seek_block_do_op(gpointer data, gpointer user_data G_GNUC_UNUSED) {
    SeekBlockOp * op = data;
    op->base.result =
        GINT_TO_POINTER(device_seek_block(op->base.child, op->block));
}

static gboolean
rait_device_seek_block (Device * dself, guint64 block) {
    GPtrArray * ops;
    guint i;
    gboolean success;

    RaitDevice * self = RAIT_DEVICE(dself);

    if (rait_device_in_error(self)) return FALSE;

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        SeekBlockOp * op;
        if ((int)i == self->private->failed)
            continue; /* This device is broken. */
        op = g_new(SeekBlockOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->base.child_index = i;
        op->block = block;
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, seek_block_do_op, ops);

    success = g_ptr_array_union_robust(RAIT_DEVICE(self),
                                       ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
	/* TODO: be more specific here */
	device_set_error(dself,
	    stralloc("One or more devices failed to seek_block"),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    dself->block = block;
    return TRUE;
}

typedef struct {
    GenericOp base;
    gpointer buffer; /* IN */
    int read_size;      /* IN/OUT -- note not a pointer */
    int desired_read_size; /* bookkeeping */
} ReadBlockOp;

/* a GFunc. */
static void read_block_do_op(gpointer data,
                             gpointer user_data G_GNUC_UNUSED) {
    ReadBlockOp * op = data;
    op->base.result =
        GINT_TO_POINTER(device_read_block(op->base.child, op->buffer,
                                          &(op->read_size)));
    if (op->read_size > op->desired_read_size) {
	g_warning("child device %s tried to return an oversized block, which the RAIT device does not support",
		  op->base.child->device_name);
    }
}

/* A BooleanExtractor. This one checks for a successful read. */
static gboolean extract_boolean_read_block_op_data(gpointer data) {
    ReadBlockOp * op = data;
    return GPOINTER_TO_INT(op->base.result) == op->desired_read_size;
}

/* A BooleanExtractor. This one checks for EOF. */
static gboolean extract_boolean_read_block_op_eof(gpointer data) {
    ReadBlockOp * op = data;
    return op->base.child->is_eof;
}

/* Counts the number of elements in an array matching a given proposition. */
static int g_ptr_array_count(GPtrArray * array, BooleanExtractor filter) {
    int rval;
    unsigned int i;
    rval = 0;
    for (i = 0; i < array->len ; i++) {
        if (filter(g_ptr_array_index(array, i)))
            rval ++;
    }
    return rval;
}

static gboolean raid_block_reconstruction(RaitDevice * self, GPtrArray * ops,
                                      gpointer buf, size_t bufsize) {
    guint num_children, data_children;
    gsize blocksize;
    gsize child_blocksize;
    guint i;
    int parity_child;
    gpointer parity_block = NULL;
    gboolean success;

    success = TRUE;

    blocksize = DEVICE(self)->block_size;
    find_simple_params(self, &num_children, &data_children);

    if (num_children > 1)
        parity_child = num_children - 1;
    else
        parity_child = -1;

    child_blocksize = blocksize / data_children;

    for (i = 0; i < ops->len; i ++) {
        ReadBlockOp * op = g_ptr_array_index(ops, i);
        if (!extract_boolean_read_block_op_data(op))
            continue;
        if ((int)(op->base.child_index) == parity_child) {
            parity_block = op->buffer;
        } else {
	    g_assert(child_blocksize * (op->base.child_index+1) <= bufsize);
            memcpy((char *)buf + child_blocksize * op->base.child_index, op->buffer,
                   child_blocksize);
        }
    }
    if (self->private->status == RAIT_STATUS_COMPLETE) {
	g_assert(parity_block != NULL); /* should have found parity_child */

        if (num_children >= 2) {
            /* Verify the parity block. This code is inefficient but
               does the job for the 2-device case, too. */
            gpointer constructed_parity;
            GPtrArray * data_extents;

            constructed_parity = g_malloc(child_blocksize);
            data_extents = g_ptr_array_sized_new(data_children);
            for (i = 0; i < data_children; i ++) {
                ReadBlockOp * op = g_ptr_array_index(ops, i);
                g_assert(extract_boolean_read_block_op_data(op));
                if ((int)op->base.child_index == parity_child)
                    continue;
                g_ptr_array_add(data_extents, op->buffer);
            }
            make_parity_block_extents(data_extents, constructed_parity,
                                      child_blocksize);

            if (0 != memcmp(parity_block, constructed_parity,
                            child_blocksize)) {
                device_set_error(DEVICE(self),
		    stralloc(_("RAIT is inconsistent: Parity block did not match data blocks.")),
		    DEVICE_STATUS_DEVICE_ERROR);
		/* TODO: can't we just isolate the device in this case? */
                success = FALSE;
            }
            g_ptr_array_free(data_extents, TRUE);
            amfree(constructed_parity);
        } else { /* do nothing. */ }
    } else if (self->private->status == RAIT_STATUS_DEGRADED) {
	g_assert(self->private->failed >= 0 && self->private->failed < (int)num_children);
        /* We are in degraded mode. What's missing? */
        if (self->private->failed == parity_child) {
            /* do nothing. */
        } else if (num_children >= 2) {
            /* Reconstruct failed block from parity block. */
            GPtrArray * data_extents = g_ptr_array_new();

            for (i = 0; i < data_children; i ++) {
                ReadBlockOp * op = g_ptr_array_index(ops, i);
                if (!extract_boolean_read_block_op_data(op))
                    continue;
                g_ptr_array_add(data_extents, op->buffer);
            }

            /* Conveniently, the reconstruction is the same procedure
               as the parity generation. This even works if there is
               only one remaining device! */
            make_parity_block_extents(data_extents,
                                      (char *)buf + (child_blocksize *
                                             self->private->failed),
                                      child_blocksize);

            /* The array members belong to our ops argument. */
            g_ptr_array_free(data_extents, TRUE);
        } else {
            g_assert_not_reached();
        }
    } else {
	/* device is already in FAILED state -- we shouldn't even be here */
        success = FALSE;
    }
    return success;
}

static int
rait_device_read_block (Device * dself, gpointer buf, int * size) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    guint num_children, data_children;
    gsize blocksize = dself->block_size;
    gsize child_blocksize;

    RaitDevice * self = RAIT_DEVICE(dself);

    if (rait_device_in_error(self)) return -1;

    find_simple_params(self, &num_children, &data_children);

    /* tell caller they haven't given us a big enough buffer */
    if (blocksize > (gsize)*size) {
	g_assert(blocksize < INT_MAX);
	*size = (int)blocksize;
	return 0;
    }

    g_assert(blocksize % data_children == 0); /* see find_block_size */
    child_blocksize = blocksize / data_children;

    ops = g_ptr_array_sized_new(num_children);
    for (i = 0; i < num_children; i ++) {
        ReadBlockOp * op;
        if ((int)i == self->private->failed)
            continue; /* This device is broken. */
        op = g_new(ReadBlockOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->base.child_index = i;
        op->buffer = g_malloc(child_blocksize);
        op->desired_read_size = op->read_size = child_blocksize;
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, read_block_do_op, ops);

    if (g_ptr_array_count(ops, extract_boolean_read_block_op_data)) {
        if (!g_ptr_array_union_robust(RAIT_DEVICE(self),
                                     ops,
                                     extract_boolean_read_block_op_data)) {
	    /* TODO: be more specific */
	    device_set_error(dself,
		stralloc(_("Error occurred combining blocks from child devices")),
		DEVICE_STATUS_DEVICE_ERROR);
	    success = FALSE;
	} else {
	    /* raid_block_reconstruction sets the error status if necessary */
	    success = raid_block_reconstruction(RAIT_DEVICE(self),
                                                ops, buf, (size_t)*size);
	}
    } else {
        success = FALSE;
        if (g_ptr_array_union_robust(RAIT_DEVICE(self),
                                     ops,
                                     extract_boolean_read_block_op_eof)) {
	    device_set_error(dself,
		stralloc(_("EOF")),
		DEVICE_STATUS_SUCCESS);
            dself->is_eof = TRUE;
	    g_mutex_lock(dself->device_mutex);
	    dself->in_file = FALSE;
	    g_mutex_unlock(dself->device_mutex);
        } else {
	    device_set_error(dself,
		stralloc(_("All child devices failed to read, but not all are at eof")),
		DEVICE_STATUS_DEVICE_ERROR);
	}
    }

    for (i = 0; i < ops->len; i ++) {
        ReadBlockOp * op = g_ptr_array_index(ops, i);
        amfree(op->buffer);
    }
    g_ptr_array_free_full(ops);

    if (success) {
	dself->block++;
	*size = blocksize;
	g_mutex_lock(dself->device_mutex);
	dself->bytes_read += blocksize;
	g_mutex_unlock(dself->device_mutex);
        return blocksize;
    } else {
        return -1;
    }
}

/* property utility functions */

typedef struct {
    GenericOp base;
    DevicePropertyId id;   /* IN */
    GValue value;          /* IN/OUT */
    PropertySurety surety; /* IN (for set) */
    PropertySource source; /* IN (for set) */
} PropertyOp;

/* Creates a GPtrArray of PropertyOf for a get or set operation. */
static GPtrArray * make_property_op_array(RaitDevice * self,
                                          DevicePropertyId id,
                                          GValue * value,
					  PropertySurety surety,
					  PropertySource source) {
    guint i;
    GPtrArray * ops;
    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        PropertyOp * op;

        if ((signed)i == self->private->failed) {
            continue;
        }

        op = g_new(PropertyOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->id = id;
        bzero(&(op->value), sizeof(op->value));
        if (value != NULL) {
            g_value_unset_copy(value, &(op->value));
        }
	op->surety = surety;
	op->source = source;
        g_ptr_array_add(ops, op);
    }

    return ops;
}

/* A GFunc. */
static void property_get_do_op(gpointer data,
                               gpointer user_data G_GNUC_UNUSED) {
    PropertyOp * op = data;

    bzero(&(op->value), sizeof(op->value));
    op->base.result =
        GINT_TO_POINTER(device_property_get(op->base.child, op->id,
                                            &(op->value)));
}

/* A GFunc. */
static void property_set_do_op(gpointer data,
                               gpointer user_data G_GNUC_UNUSED) {
    PropertyOp * op = data;

    op->base.result =
        GINT_TO_POINTER(device_property_set_ex(op->base.child, op->id,
					       &(op->value), op->surety,
					       op->source));
    g_value_unset(&(op->value));
}

/* PropertyGetFns and PropertySetFns */

static gboolean
property_get_block_size_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    gsize my_block_size;

    if (dself->block_size_source != PROPERTY_SOURCE_DEFAULT) {
	my_block_size = dself->block_size;

	if (surety)
	    *surety = dself->block_size_surety;
    } else {
	gsize child_block_size;
	child_block_size = calculate_block_size_from_children(self,
						    &my_block_size);
	if (child_block_size == 0)
	    return FALSE;

	if (surety)
	    *surety = PROPERTY_SURETY_BAD; /* may still change */
    }

    if (val) {
	g_value_unset_init(val, G_TYPE_INT);
	g_assert(my_block_size < G_MAXINT); /* gsize -> gint */
	g_value_set_int(val, (gint)my_block_size);
    }

    if (source)
	*source = dself->block_size_source;

    return TRUE;
}

static gboolean
property_set_block_size_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety surety, PropertySource source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    gint my_block_size = g_value_get_int(val);
    guint data_children;

    find_simple_params(self, NULL, &data_children);
    if ((my_block_size % data_children) != 0) {
	device_set_error(dself,
	    vstrallocf(_("Block size must be a multiple of %d"), data_children),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    dself->block_size = my_block_size;
    dself->block_size_source = source;
    dself->block_size_surety = surety;

    if (!fix_block_size(self))
	return FALSE;

    return TRUE;
}

static gboolean
property_get_canonical_name_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    char *canonical = child_device_names_to_rait_name(self);

    if (val) {
	g_value_unset_init(val, G_TYPE_STRING);
	g_value_set_string(val, canonical);
	g_free(canonical);
    }

    if (surety)
	*surety = PROPERTY_SURETY_GOOD;

    if (source)
	*source = PROPERTY_SOURCE_DETECTED;

    return TRUE;
}

static gboolean
property_get_concurrency_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    ConcurrencyParadigm result;
    guint i;
    GPtrArray * ops;
    gboolean success;

    ops = make_property_op_array(self, PROPERTY_CONCURRENCY, NULL, 0, 0);
    do_rait_child_ops(self, property_get_do_op, ops);

    /* find the most restrictive paradigm acceptable to all
     * child devices */
    result = CONCURRENCY_PARADIGM_RANDOM_ACCESS;
    success = TRUE;
    for (i = 0; i < ops->len; i ++) {
        ConcurrencyParadigm cur;
        PropertyOp * op = g_ptr_array_index(ops, i);

        if (!op->base.result
	    || G_VALUE_TYPE(&(op->value)) != CONCURRENCY_PARADIGM_TYPE) {
	    success = FALSE;
	    break;
	}

        cur = g_value_get_enum(&(op->value));
        if (result == CONCURRENCY_PARADIGM_EXCLUSIVE ||
            cur == CONCURRENCY_PARADIGM_EXCLUSIVE) {
            result = CONCURRENCY_PARADIGM_EXCLUSIVE;
        } else if (result == CONCURRENCY_PARADIGM_SHARED_READ ||
                   cur == CONCURRENCY_PARADIGM_SHARED_READ) {
            result = CONCURRENCY_PARADIGM_SHARED_READ;
        } else if (result == CONCURRENCY_PARADIGM_RANDOM_ACCESS &&
                   cur == CONCURRENCY_PARADIGM_RANDOM_ACCESS) {
            result = CONCURRENCY_PARADIGM_RANDOM_ACCESS;
        } else {
            success = FALSE;
	    break;
        }
    }

    g_ptr_array_free_full(ops);

    if (success) {
	if (val) {
	    g_value_unset_init(val, CONCURRENCY_PARADIGM_TYPE);
	    g_value_set_enum(val, result);
	}

	if (surety)
	    *surety = PROPERTY_SURETY_GOOD;

	if (source)
	    *source = PROPERTY_SOURCE_DETECTED;
    }

    return success;
}

static gboolean
property_get_streaming_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    StreamingRequirement result;
    guint i;
    GPtrArray * ops;
    gboolean success;

    ops = make_property_op_array(self, PROPERTY_STREAMING, NULL, 0, 0);
    do_rait_child_ops(self, property_get_do_op, ops);

    /* combine the child streaming requirements, selecting the strongest
     * requirement of the bunch. */
    result = STREAMING_REQUIREMENT_NONE;
    success = TRUE;
    for (i = 0; i < ops->len; i ++) {
        StreamingRequirement cur;
        PropertyOp * op = g_ptr_array_index(ops, i);

        if (!op->base.result
	    || G_VALUE_TYPE(&(op->value)) != STREAMING_REQUIREMENT_TYPE) {
	    success = FALSE;
	    break;
	}

        cur = g_value_get_enum(&(op->value));
        if (result == STREAMING_REQUIREMENT_REQUIRED ||
            cur == STREAMING_REQUIREMENT_REQUIRED) {
            result = STREAMING_REQUIREMENT_REQUIRED;
        } else if (result == STREAMING_REQUIREMENT_DESIRED ||
                   cur == STREAMING_REQUIREMENT_DESIRED) {
            result = STREAMING_REQUIREMENT_DESIRED;
        } else if (result == STREAMING_REQUIREMENT_NONE &&
                   cur == STREAMING_REQUIREMENT_NONE) {
            result = STREAMING_REQUIREMENT_NONE;
        } else {
            success = FALSE;
	    break;
        }
    }

    g_ptr_array_free_full(ops);

    if (success) {
	if (val) {
	    g_value_unset_init(val, STREAMING_REQUIREMENT_TYPE);
	    g_value_set_enum(val, result);
	}

	if (surety)
	    *surety = PROPERTY_SURETY_GOOD;

	if (source)
	    *source = PROPERTY_SOURCE_DETECTED;
    }

    return success;
}

static gboolean
property_get_boolean_and_fn(Device *dself,
    DevicePropertyBase *base, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    gboolean result;
    guint i;
    GPtrArray * ops;
    gboolean success;

    ops = make_property_op_array(self, base->ID, NULL, 0, 0);
    do_rait_child_ops(self, property_get_do_op, ops);

    /* combine the child values, applying a simple AND */
    result = TRUE;
    success = TRUE;
    for (i = 0; i < ops->len; i ++) {
        PropertyOp * op = g_ptr_array_index(ops, i);

        if (!op->base.result || !G_VALUE_HOLDS_BOOLEAN(&(op->value))) {
	    success = FALSE;
	    break;
	}

	if (!g_value_get_boolean(&(op->value))) {
	    result = FALSE;
	    break;
	}
    }

    g_ptr_array_free_full(ops);

    if (success) {
	if (val) {
	    g_value_unset_init(val, G_TYPE_BOOLEAN);
	    g_value_set_boolean(val, result);
	}

	if (surety)
	    *surety = PROPERTY_SURETY_GOOD;

	if (source)
	    *source = PROPERTY_SOURCE_DETECTED;
    }

    return success;
}

static gboolean
property_get_medium_access_type_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    MediaAccessMode result;
    guint i;
    GPtrArray * ops;
    gboolean success;

    ops = make_property_op_array(self, PROPERTY_MEDIUM_ACCESS_TYPE, NULL, 0, 0);
    do_rait_child_ops(self, property_get_do_op, ops);

    /* combine the modes as best we can */
    result = 0;
    success = TRUE;
    for (i = 0; i < ops->len; i ++) {
        MediaAccessMode cur;
        PropertyOp * op = g_ptr_array_index(ops, i);

        if (!op->base.result || G_VALUE_TYPE(&(op->value)) != MEDIA_ACCESS_MODE_TYPE) {
	    success = FALSE;
	    break;
	}

        cur = g_value_get_enum(&(op->value));

	if (i == 0) {
	    result = cur;
	} else if ((result == MEDIA_ACCESS_MODE_READ_ONLY &&
		    cur == MEDIA_ACCESS_MODE_WRITE_ONLY) ||
		   (result == MEDIA_ACCESS_MODE_WRITE_ONLY &&
		    cur == MEDIA_ACCESS_MODE_READ_ONLY)) {
	    /* Invalid combination; one device can only read, other
	       can only write. */
	    success = FALSE;
	    break;
	} else if (result == MEDIA_ACCESS_MODE_READ_ONLY ||
		   cur == MEDIA_ACCESS_MODE_READ_ONLY) {
	    result = MEDIA_ACCESS_MODE_READ_ONLY;
	} else if (result == MEDIA_ACCESS_MODE_WRITE_ONLY ||
		   cur == MEDIA_ACCESS_MODE_WRITE_ONLY) {
	    result = MEDIA_ACCESS_MODE_WRITE_ONLY;
	} else if (result == MEDIA_ACCESS_MODE_WORM ||
		   cur == MEDIA_ACCESS_MODE_WORM) {
	    result = MEDIA_ACCESS_MODE_WORM;
	} else if (result == MEDIA_ACCESS_MODE_READ_WRITE &&
		   cur == MEDIA_ACCESS_MODE_READ_WRITE) {
	    result = MEDIA_ACCESS_MODE_READ_WRITE;
	} else {
	    success = FALSE;
	    break;
	}
    }

    g_ptr_array_free_full(ops);

    if (success) {
	if (val) {
	    g_value_unset_init(val, MEDIA_ACCESS_MODE_TYPE);
	    g_value_set_enum(val, result);
	}

	if (surety)
	    *surety = PROPERTY_SURETY_GOOD;

	if (source)
	    *source = PROPERTY_SOURCE_DETECTED;
    }

    return success;
}

static gboolean
property_get_max_volume_usage_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety *surety, PropertySource *source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    guint64 result;
    guint i;
    GPtrArray * ops;
    guint data_children;

    ops = make_property_op_array(self, PROPERTY_MAX_VOLUME_USAGE, NULL, 0, 0);
    do_rait_child_ops(self, property_get_do_op, ops);

    /* look for the smallest value that is set */
    result = 0;
    for (i = 0; i < ops->len; i ++) {
        guint64 cur;
        PropertyOp * op = g_ptr_array_index(ops, i);

        if (!op->base.result || !G_VALUE_HOLDS_UINT64(&(op->value))) {
	    continue; /* ignore children without this property */
	}

        cur = g_value_get_uint64(&(op->value));

	if (!result || (cur && cur < result)) {
	    result = cur;
	}
    }

    g_ptr_array_free_full(ops);

    if (result) {
	/* result contains the minimum usage on any child.  We can use that space
	 * on each of our data children, so the total is larger */
	find_simple_params(self, NULL, &data_children);
	result *= data_children;

	if (val) {
	    g_value_unset_init(val, G_TYPE_UINT64);
	    g_value_set_uint64(val, result);
	}

	if (surety)
	    *surety = PROPERTY_SURETY_GOOD;

	if (source)
	    *source = PROPERTY_SOURCE_DETECTED;

	return TRUE;
    } else {
	/* no result from any children, so we effectively don't have this property */
	return FALSE;
    }
}

static gboolean
property_set_max_volume_usage_fn(Device *dself,
    DevicePropertyBase *base G_GNUC_UNUSED, GValue *val,
    PropertySurety surety, PropertySource source)
{
    RaitDevice *self = RAIT_DEVICE(dself);
    guint64 parent_usage;
    guint64 child_usage;
    GValue child_val;
    guint i;
    gboolean success;
    GPtrArray * ops;
    guint data_children;

    parent_usage = g_value_get_uint64(val);
    find_simple_params(self, NULL, &data_children);

    child_usage = parent_usage / data_children;

    bzero(&child_val, sizeof(child_val));
    g_value_init(&child_val, G_TYPE_UINT64);
    g_value_set_uint64(&child_val, child_usage);

    ops = make_property_op_array(self, PROPERTY_MAX_VOLUME_USAGE,
				&child_val, surety, source);
    do_rait_child_ops(self, property_set_do_op, ops);

    /* if any of the kids succeeded, then we did too */
    success = FALSE;
    for (i = 0; i < ops->len; i ++) {
        PropertyOp * op = g_ptr_array_index(ops, i);

        if (op->base.result) {
	    success = TRUE;
	    break;
	}
    }

    g_ptr_array_free_full(ops);

    return success;
}

typedef struct {
    GenericOp base;
    guint filenum;
} RecycleFileOp;

/* A GFunc */
static void recycle_file_do_op(gpointer data,
                               gpointer user_data G_GNUC_UNUSED) {
    RecycleFileOp * op = data;
    op->base.result =
        GINT_TO_POINTER(device_recycle_file(op->base.child, op->filenum));
}

static gboolean
rait_device_recycle_file (Device * dself, guint filenum) {
    GPtrArray * ops;
    guint i;
    gboolean success;

    RaitDevice * self = RAIT_DEVICE(dself);

    if (rait_device_in_error(self)) return FALSE;

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        RecycleFileOp * op;
        op = g_new(RecycleFileOp, 1);
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->filenum = filenum;
        g_ptr_array_add(ops, op);
    }

    do_rait_child_ops(self, recycle_file_do_op, ops);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
	/* TODO: be more specific here */
	device_set_error(dself,
	    stralloc(_("One or more devices failed to recycle_file")),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }
    return TRUE;
}

/* GFunc */
static void finish_do_op(gpointer data, gpointer user_data G_GNUC_UNUSED) {
    GenericOp * op = data;
    op->result = GINT_TO_POINTER(device_finish(op->child));
}

static gboolean
rait_device_finish (Device * self) {
    GPtrArray * ops;
    gboolean success;
    gboolean rval = TRUE;

    rval = !rait_device_in_error(self);

    ops = make_generic_boolean_op_array(RAIT_DEVICE(self));

    do_rait_child_ops(RAIT_DEVICE(self), finish_do_op, ops);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);
    if (!success)
	rval = FALSE;

    g_ptr_array_free_full(ops);

    self->access_mode = ACCESS_NULL;

    return rval;
}

static Device *
rait_device_factory (char * device_name, char * device_type, char * device_node) {
    Device * rval;
    g_assert(0 == strcmp(device_type, "rait"));
    rval = DEVICE(g_object_new(TYPE_RAIT_DEVICE, NULL));
    device_open_device(rval, device_name, device_type, device_node);
    return rval;
}

void
rait_device_register (void) {
    static const char * device_prefix_list[] = {"rait", NULL};
    register_device(rait_device_factory, device_prefix_list);
}
