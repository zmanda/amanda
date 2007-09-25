/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* The RAIT device encapsulates some number of other devices into a single
 * redundant device. */

#include "rait-device.h"
#include <amanda.h>
#include "property.h"
#include <util.h>

typedef enum {
    RAIT_STATUS_COMPLETE, /* All subdevices OK. */
    RAIT_STATUS_DEGRADED, /* One subdevice failed. */
    RAIT_STATUS_FAILED    /* Two or more subdevices failed. */
} RaitStatus;

struct RaitDevicePrivate_s {
    GPtrArray * children;
    /* These flags are only relevant for reading. */
    RaitStatus status;
    /* If status == RAIT_STATUS_DEGRADED, this holds the index of the
       failed node. It holds a negative number otherwise. */
    int failed;
    guint block_size;
};

#define PRIVATE(o) (o->private)

/* here are local prototypes */
static void rait_device_init (RaitDevice * o);
static void rait_device_class_init (RaitDeviceClass * c);
static gboolean rait_device_open_device (Device * self, char * device_name);
static gboolean rait_device_start (Device * self, DeviceAccessMode mode,
                                   char * label, char * timestamp);
static gboolean rait_device_start_file(Device * self, const dumpfile_t * info);
static gboolean rait_device_write_block (Device * self, guint size,
                                         gpointer data, gboolean last_block);
static gboolean rait_device_finish_file (Device * self);
static dumpfile_t * rait_device_seek_file (Device * self, guint file);
static gboolean rait_device_seek_block (Device * self, guint64 block);
static int      rait_device_read_block (Device * self, gpointer buf,
                                        int * size);
static gboolean rait_device_property_get (Device * self, DevicePropertyId id,
                                          GValue * val);
static gboolean rait_device_property_set (Device * self, DevicePropertyId id,
                                          GValue * val);
static gboolean rait_device_recycle_file (Device * self, guint filenum);
static gboolean rait_device_finish (Device * self);
static ReadLabelStatusFlags rait_device_read_label(Device * dself);
static void find_simple_params(RaitDevice * self, guint * num_children,
                               guint * data_children, int * blocksize);

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

/* This function is replicated here in case we have GLib from before 2.4.
 * It should probably go eventually. */
#if !GLIB_CHECK_VERSION(2,4,0)
static void
g_ptr_array_foreach (GPtrArray *array,
                     GFunc      func,
                     gpointer   user_data)
{
  guint i;

  g_return_if_fail (array);

  for (i = 0; i < array->len; i++)
    (*func) (array->pdata[i], user_data);
}
#endif

GType
rait_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (RaitDeviceClass),
            (GBaseInitFunc) NULL,
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
    g_return_if_fail(G_IS_OBJECT(data));
    g_object_unref(data);
}

static void
rait_device_finalize(GObject *obj_self)
{
    RaitDevice *self G_GNUC_UNUSED = RAIT_DEVICE (obj_self);
    if(G_OBJECT_CLASS(parent_class)->finalize) \
           (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);
    if(self->private->children) {
        g_ptr_array_foreach(self->private->children,
                            g_object_unref_foreach, NULL);
        g_ptr_array_free (self->private->children, TRUE);
        self->private->children = NULL;
    }
    amfree(self->private);
}

static void 
rait_device_init (RaitDevice * o G_GNUC_UNUSED)
{
    PRIVATE(o) = malloc(sizeof(RaitDevicePrivate));
    PRIVATE(o)->children = g_ptr_array_new();
    PRIVATE(o)->status = RAIT_STATUS_COMPLETE;
    PRIVATE(o)->failed = -1;
}

static void 
rait_device_class_init (RaitDeviceClass * c G_GNUC_UNUSED)
{
    GObjectClass *g_object_class G_GNUC_UNUSED = (GObjectClass*) c;
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = rait_device_open_device;
    device_class->start = rait_device_start;
    device_class->start_file = rait_device_start_file;
    device_class->write_block = rait_device_write_block;
    device_class->finish_file = rait_device_finish_file;
    device_class->seek_file = rait_device_seek_file;
    device_class->seek_block = rait_device_seek_block;
    device_class->read_block = rait_device_read_block;
    device_class->property_get = rait_device_property_get;
    device_class->property_set = rait_device_property_set;
    device_class->recycle_file = rait_device_recycle_file; 
    device_class->finish = rait_device_finish;
    device_class->read_label = rait_device_read_label;

    g_object_class->finalize = rait_device_finalize;

    g_thread_pool_set_max_unused_threads(-1);
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
 * through the array. */
static void do_thread_pool_op(GFunc func, GPtrArray * ops, gpointer data) {
    GThreadPool * pool;
    guint i;

    pool = g_thread_pool_new(func, data, -1, FALSE, NULL);
    for (i = 0; i < ops->len; i ++) {
        g_thread_pool_push(pool, g_ptr_array_index(ops, i), NULL);
    }

    g_thread_pool_free(pool, FALSE, TRUE);
}

/* This does the above, in a serial fashion (and without using threads) */
static void do_unthreaded_ops(GFunc func, GPtrArray * ops,
                              gpointer data G_GNUC_UNUSED) {
    guint i;

    for (i = 0; i < ops->len; i ++) {
        func(g_ptr_array_index(ops, i), NULL);
    }
}

/* This is the one that code below should call. It switches
   automatically between do_thread_pool_op and do_unthreaded_ops,
   depending on g_thread_supported(). */
static void do_rait_child_ops(GFunc func, GPtrArray * ops, gpointer data) {
    if (g_thread_supported()) {
        do_thread_pool_op(func, ops, data);
    } else {
        do_unthreaded_ops(func, ops, data);
    }
}

/* Take a text string user_name, and break it out into an argv-style
   array of strings. For example, {foo,{bar,baz},bat} would return the
   strings "foo", "{bar,baz}", "bat", and NULL. Returns NULL on
   error. */
static char ** parse_device_name(char * user_name) {
    GPtrArray * rval;
    char * cur_end = user_name;
    char * cur_begin = user_name;
    
    rval = g_ptr_array_new();
    
    /* Check opening brace. */
    if (*cur_begin != '{')
        return NULL;
    cur_begin ++;
    
    cur_end = cur_begin;
    for (;;) {
        switch (*cur_end) {
        case ',': {
            g_ptr_array_add(rval, g_strndup(cur_begin, cur_end - cur_begin));
            cur_end ++;
            cur_begin = cur_end;
            continue;
        }

        case '{':
            /* We read until the matching closing brace. */
            while (*cur_end != '}' && *cur_end != '\0')
                cur_end ++;
            if (*cur_end == '}')
                cur_end ++;
            continue;
            
        case '}':
            g_ptr_array_add(rval, g_strndup(cur_begin, cur_end - cur_begin));
            goto OUTER_END; /* break loop, not switch */

        case '\0':
            /* Unexpected NULL; abort. */
            fprintf(stderr, "Invalid RAIT device name %s\n", user_name);
            g_ptr_array_free_full(rval);
            return NULL;

        default:
            cur_end ++;
            continue;
        }
        g_assert_not_reached();
    }
 OUTER_END:
    
    if (cur_end[1] != '\0') {
        fprintf(stderr, "Invalid RAIT device name %s\n", user_name);
        g_ptr_array_free_full(rval);
        return NULL;
    }

    g_ptr_array_add(rval, NULL);

    return (char**) g_ptr_array_free(rval, FALSE);
}

/* Find a workable block size. */
static gboolean find_block_size(RaitDevice * self) {
    uint min = 0;
    uint max = G_MAXUINT;
    uint result;
    GValue val;
    gboolean rval;
    guint i;
    guint data_children;
    
    for (i = 0; i < self->private->children->len; i ++) {
        uint child_min, child_max;
        GValue property_result;
        bzero(&property_result, sizeof(property_result));

        if (!device_property_get(g_ptr_array_index(self->private->children, i),
                                 PROPERTY_MIN_BLOCK_SIZE, &property_result))
            return FALSE;
        child_min = g_value_get_uint(&property_result);
        g_return_val_if_fail(child_min > 0, FALSE);
        if (!device_property_get(g_ptr_array_index(self->private->children, i),
                                 PROPERTY_MAX_BLOCK_SIZE, &property_result))
            return FALSE;
        child_max = g_value_get_uint(&property_result);
        g_return_val_if_fail(child_max > 0, FALSE);
        
        if (child_min > max || child_max < min || child_min == 0) {
            return FALSE;
        } else {
            min = MAX(min, child_min);
            max = MIN(max, child_max);
        }
    }

    /* Now pick a number. */
    g_assert(min <= max);
    if (max < MAX_TAPE_BLOCK_BYTES)
        result = max;
    else if (min > MAX_TAPE_BLOCK_BYTES)
        result = min;
    else
        result = MAX_TAPE_BLOCK_BYTES;

    /* User reads and writes bigger blocks. */
    find_simple_params(self, NULL, &data_children, NULL);
    self->private->block_size = result * data_children;

    bzero(&val, sizeof(val));
    g_value_init(&val, G_TYPE_INT);
    g_value_set_int(&val, result);
    /* We can't do device_property_set because it's disallowed
       according to the registered property base. */
    rval = rait_device_property_set(DEVICE(self), PROPERTY_BLOCK_SIZE, &val);
    g_value_unset(&val);
    return rval;
}

/* Register properties that belong to the RAIT device proper, and not
   to subdevices. */
static void register_rait_properties(RaitDevice * self) {
    Device * o = DEVICE(self);
    DeviceProperty prop;

    prop.access = PROPERTY_ACCESS_GET_MASK;

    prop.base = &device_property_min_block_size;
    device_add_property(o, &prop, NULL);

    prop.base = &device_property_max_block_size;
    device_add_property(o, &prop, NULL);
  
    prop.base = &device_property_block_size;
    device_add_property(o, &prop, NULL);

    prop.base = &device_property_canonical_name;
    device_add_property(o, &prop, NULL);
}

static void property_hash_union(GHashTable * properties,
                                DeviceProperty * prop) {
    PropertyAccessFlags before, after;
    gpointer tmp;
    gboolean found;
    
    found = g_hash_table_lookup_extended(properties,
                                         GUINT_TO_POINTER(prop->base->ID),
                                         NULL, &tmp);
    before = GPOINTER_TO_UINT(tmp);
    
    if (!found) {
        after = prop->access;
    } else {
        after = before & prop->access;
    }
    
    g_hash_table_insert(properties, GUINT_TO_POINTER(prop->base->ID),
                        GUINT_TO_POINTER(after));
}

/* A GHRFunc. */
static gboolean zero_value(gpointer key G_GNUC_UNUSED, gpointer value,
                           gpointer user_data G_GNUC_UNUSED) {
    return (0 == GPOINTER_TO_UINT(value));
}

/* A GHFunc */
static void register_property_hash(gpointer key, gpointer value,
                                   gpointer user_data) {
    DevicePropertyId id = GPOINTER_TO_UINT(key);
    DeviceProperty prop;
    Device * device = (Device*)user_data;

    g_assert(IS_DEVICE(device));

    prop.access = GPOINTER_TO_UINT(value);
    prop.base = device_property_get_by_id(id);

    device_add_property(device, &prop, NULL);
}

/* This function figures out which properties exist for all children, and 
 * exports the unioned access mask. */
static void register_properties(RaitDevice * self) {
    GHashTable * properties; /* PropertyID => PropertyAccessFlags */
    guint j;
    
    properties = g_hash_table_new(g_direct_hash, g_direct_equal);

    /* Iterate the device list, find all properties. */
    for (j = 0; j < self->private->children->len; j ++) {
        int i;
        Device * child = g_ptr_array_index(self->private->children, j);
        const DeviceProperty* device_property_list;

        device_property_list = device_property_get_list(child);
        for (i = 0; device_property_list[i].base != NULL; i ++) {
            property_hash_union(properties, (gpointer)&(device_property_list[i]));
        }
    }

    /* Then toss properties that can't be accessed. */
    g_hash_table_foreach_remove(properties, zero_value, NULL);
    g_hash_table_remove(properties, GINT_TO_POINTER(PROPERTY_BLOCK_SIZE));
    g_hash_table_remove(properties, GINT_TO_POINTER(PROPERTY_MIN_BLOCK_SIZE));
    g_hash_table_remove(properties, GINT_TO_POINTER(PROPERTY_MAX_BLOCK_SIZE));
    g_hash_table_remove(properties, GINT_TO_POINTER(PROPERTY_CANONICAL_NAME));

    /* Finally, register the lot. */
    g_hash_table_foreach(properties, register_property_hash, self);

    g_hash_table_destroy(properties);

    /* Then we have some of our own properties to register. */
    register_rait_properties(self);
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
   TRUE if and only if all calls to extractor return TRUE.
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
        op = malloc(sizeof(*op));
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
    gboolean success;
    gpointer isolated_op = NULL;

    for (;;) {
        success = g_ptr_array_and(ops, extractor);
        
        if (success || self->private->status != RAIT_STATUS_COMPLETE) {
            break;
        } else {
            guint i;
            /* First device failure, note the device and march on. */
            self->private->status = RAIT_STATUS_DEGRADED;
            for (i = 0; i < ops->len; i ++) {
                GenericOp * op = g_ptr_array_index(ops, i);
                if (!(op->result)) {
                    isolated_op = g_ptr_array_remove_index_fast(ops, i);
                    self->private->failed = op->child_index;
                    fprintf(stderr, "RAIT array %s Isolated device %s.\n",
                            DEVICE(self)->device_name,
                            op->child->device_name);
                    break;
                }
            }
        }
    }

    /* Return isolated op so any data members can be freed. */
    if (isolated_op != NULL) {
        g_ptr_array_add(ops, isolated_op);
    }
    return success;
}

typedef struct {
    Device * result;    /* IN */
    char * device_name; /* OUT */
} OpenDeviceOp;

/* A GFunc. */
static void open_device_do_op(gpointer data,
                              gpointer user_data G_GNUC_UNUSED) {
    OpenDeviceOp * op = data;

    op->result = device_open(op->device_name);
    amfree(op->device_name);
}

/* Returns TRUE if and only if the volume label and time are equal. */
static gboolean compare_volume_results(Device * a, Device * b) {
    if (a->volume_time != b->volume_time)
        return FALSE;
    if (a->volume_label == NULL && b->volume_label == NULL)
        return TRUE;
    if (a->volume_label == NULL || b->volume_label == NULL)
        return FALSE;
    return 0 == strcmp(a->volume_label, b->volume_label);
}

static gboolean 
rait_device_open_device (Device * dself, char * device_name) {
    char ** device_names;
    GPtrArray * open_device_ops;
    guint i;
    gboolean failure;
    RaitDevice * self;

    self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, FALSE);
    g_return_val_if_fail (device_name != NULL, FALSE);

    device_names = parse_device_name(device_name);
    
    if (device_names == NULL)
        return FALSE;

    /* Open devices in a separate thread, in case they have to rewind etc. */
    open_device_ops = g_ptr_array_new();

    for (i = 0; device_names[i] != NULL; i ++) {
        OpenDeviceOp *op;

        op = malloc(sizeof(*op));
        op->device_name = device_names[i];
        op->result = NULL;
        g_ptr_array_add(open_device_ops, op);
    }

    free(device_names);
    do_rait_child_ops(open_device_do_op, open_device_ops, NULL);

    failure = FALSE;
    /* Check results of opening devices. */
    for (i = 0; i < open_device_ops->len; i ++) {
        OpenDeviceOp *op = g_ptr_array_index(open_device_ops, i);
        
        if (op->result != NULL) {
            g_ptr_array_add(self->private->children, op->result);
        } else {
            failure = TRUE;
        }
    }

    g_ptr_array_free_full(open_device_ops);

    failure = failure || !find_block_size(self);
    if (failure)
        return FALSE; /* This will clean up any created children. */

    register_properties(self);

    /* Chain up. */
    if (parent_class->open_device) {
        return parent_class->open_device(dself, device_name);
    } else {
        return TRUE;
    }
}

/* A GFunc. */
static void read_label_do_op(gpointer data,
                             gpointer user_data G_GNUC_UNUSED) {
    GenericOp * op = data;
    op->result = GINT_TO_POINTER(device_read_label(op->child));
}

static ReadLabelStatusFlags rait_device_read_label(Device * dself) {
    RaitDevice * self;
    GPtrArray * ops;
    ReadLabelStatusFlags failed_result = 0;
    ReadLabelStatusFlags rval;
    GenericOp * failed_op = NULL; /* If this is non-null, we will isolate. */
    unsigned int i;
    Device * first_success = NULL;

    self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, FALSE);

    amfree(dself->volume_label);

    ops = make_generic_boolean_op_array(self);
    
    do_rait_child_ops(read_label_do_op, ops, NULL);
    
    for (i = 0; i < ops->len; i ++) {
        GenericOp * op = g_ptr_array_index(ops, i);
        ReadLabelStatusFlags result = GPOINTER_TO_INT(op->result);
        if (op->result == READ_LABEL_STATUS_SUCCESS) {
            if (first_success == NULL) {
                /* This is the first successful device. */
                first_success = op->child;
            } else if (!compare_volume_results(first_success, op->child)) {
                /* Doesn't match. :-( */
                fprintf(stderr, "Inconsistant volume labels: "
                        "Got %s/%s against %s/%s.\n",
                        first_success->volume_label,
                        first_success->volume_time, 
                        op->child->volume_label,
                        op->child->volume_time);
                failed_result |= READ_LABEL_STATUS_VOLUME_ERROR;
                failed_op = NULL;
            }
        } else {
            if (failed_result == 0 &&
                self->private->status == RAIT_STATUS_COMPLETE) {
                /* This is the first failed device; note it and we'll isolate
                   later. */
                failed_op = op;
                failed_result = result;
            } else {
                /* We've encountered multiple failures. OR them together. */
                failed_result |= result;
                failed_op = NULL;
            }
        }
    }

    if (failed_op != NULL) {
        /* We have a single device to isolate. */
        failed_result = READ_LABEL_STATUS_SUCCESS; /* Recover later */
        self->private->failed = failed_op->child_index;
        fprintf(stderr, "RAIT array %s Isolated device %s.\n",
                dself->device_name,
                failed_op->child->device_name);
    }

    if (failed_result != READ_LABEL_STATUS_SUCCESS) {
        /* We had multiple failures or an inconsistency. */
        rval = failed_result;
    } else {
        /* Everything peachy. */
        rval = READ_LABEL_STATUS_SUCCESS;
        g_assert(first_success != NULL);
        if (first_success->volume_label != NULL) {
            dself->volume_label = strdup(first_success->volume_label);
        }
        if (first_success->volume_time != NULL) {
            dself->volume_time = strdup(first_success->volume_time);
        }
    }
    
    g_ptr_array_free_full(ops);

    return rval;
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
rait_device_start (Device * dself, DeviceAccessMode mode, char * label,
                   char * timestamp) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    RaitDevice * self;

    self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, FALSE);

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        StartOp * op;
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->mode = mode;
        op->label = strdup(label);
        op->timestamp = strdup(timestamp);
        g_ptr_array_add(ops, op);
    }
    
    do_rait_child_ops(start_do_op, ops, NULL);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else if (parent_class->start) {
        return parent_class->start(dself, mode, label, timestamp);
    } else {
        return TRUE;
    }
}

typedef struct {
    GenericOp base;
    const dumpfile_t * info; /* IN */
} StartFileOp;

/* a GFunc */
static void start_file_do_op(gpointer data, gpointer user_data G_GNUC_UNUSED) {
    StartFileOp * op = data;
    op->base.result = GINT_TO_POINTER(device_start_file(op->base.child,
                                                        op->info));
}

static gboolean
rait_device_start_file (Device * dself, const dumpfile_t * info) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    RaitDevice * self;

    self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, FALSE);

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        StartFileOp * op;
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->info = info;
        g_ptr_array_add(ops, op);
    }
    
    do_rait_child_ops(start_file_do_op, ops, NULL);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else if (parent_class->start_file) {
        return parent_class->start_file(dself, info);
    } else {
        return TRUE;
    }
}

static void find_simple_params(RaitDevice * self,
                               guint * num_children,
                               guint * data_children,
                               int * blocksize) {
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

    if (blocksize != NULL) {
        *blocksize = device_write_min_size(DEVICE(self));
    }
}

typedef struct {
    GenericOp base;
    guint size;           /* IN */
    gpointer data;        /* IN */
    gboolean short_block; /* IN */
    gboolean data_needs_free; /* bookkeeping */
} WriteBlockOp;

/* a GFunc. */
static void write_block_do_op(gpointer data,
                              gpointer user_data G_GNUC_UNUSED) {
    WriteBlockOp * op = data;

    op->base.result =
        GINT_TO_POINTER(device_write_block(op->base.child, op->size, op->data,
                                           op->short_block));
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

    g_return_val_if_fail(chunks > 0 && chunk > 0 && chunk <= chunks, NULL);
    g_return_val_if_fail(data != NULL, NULL);
    g_return_val_if_fail(size > 0 && size % (chunks - 1) == 0, NULL);

    chunk_size = size / (chunks - 1);
    rval = malloc(chunk_size);
    if (chunks != chunk) {
        /* data block. */
        memcpy(rval, data + chunk_size * (chunk - 1), chunk_size);
    } else {
        make_parity_block(data, rval, chunk_size, chunks);
    }
    
    return rval;
}

static gboolean 
rait_device_write_block (Device * dself, guint size, gpointer data,
                         gboolean last_block) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    guint data_children, num_children;
    int blocksize;
    RaitDevice * self;

    self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, FALSE);

    find_simple_params(RAIT_DEVICE(self), &num_children, &data_children,
                       &blocksize);
    num_children = self->private->children->len;
    if (num_children != 1)
        data_children = num_children - 1;
    else
        data_children = num_children;
    
    g_return_val_if_fail(size % data_children == 0 || last_block, FALSE);

    if (last_block) {
        gpointer new_data;

        new_data = malloc(blocksize);
        memcpy(new_data, data, size);
        bzero(new_data + size, blocksize - size);

        data = new_data;
        size = blocksize;
    }

    ops = g_ptr_array_sized_new(num_children);
    for (i = 0; i < self->private->children->len; i ++) {
        WriteBlockOp * op;
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->short_block = last_block;
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

    if (last_block) {
        amfree(data);
    }
    
    do_rait_child_ops(write_block_do_op, ops, NULL);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    for (i = 0; i < self->private->children->len; i ++) {
        WriteBlockOp * op = g_ptr_array_index(ops, i);
        if (op->data_needs_free)
            free(op->data);
    }

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else {
        /* We don't chain up here because we must handle finish_file
           differently. If we were called with last_block, then the
           children have already called finish_file themselves. So we
           update the device block numbers manually. */
        dself->block ++;
        if (last_block)
            dself->in_file = FALSE;

        return TRUE;
    }
}

/* A GFunc */
static void finish_file_do_op(gpointer data,
                              gpointer user_data G_GNUC_UNUSED) {
    GenericOp * op = data;
    op->result = GINT_TO_POINTER(device_finish_file(op->child));
}

static gboolean 
rait_device_finish_file (Device * self) {
    GPtrArray * ops;
    gboolean success;

    ops = make_generic_boolean_op_array(RAIT_DEVICE(self));
    
    do_rait_child_ops(finish_file_do_op, ops, NULL);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else if (parent_class->finish_file) {
        return parent_class->finish_file(self);
    } else {
        return TRUE;
    }
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
    g_return_val_if_fail(self != NULL, FALSE);

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        SeekFileOp * op;
        if ((int)i == self->private->failed)
            continue; /* This device is broken. */
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->base.child_index = i;
        op->requested_file = file;
        g_ptr_array_add(ops, op);
    }
    
    do_rait_child_ops(seek_file_do_op, ops, NULL);

    /* This checks for NULL values, but we still have to check for
       consistant headers. */
    success = g_ptr_array_union_robust(RAIT_DEVICE(self),
                                       ops, extract_boolean_pointer_op);

    rval = NULL;
    for (i = 0; i < self->private->children->len; i ++) {
        SeekFileOp * this_op;
        dumpfile_t * this_result;
        guint this_actual_file;
        if ((int)i == self->private->failed)
            continue;
        
        this_op = (SeekFileOp*)g_ptr_array_index(ops, i);
        this_result = this_op->base.result;
        this_actual_file = this_op->actual_file;

        if (rval == NULL) {
            rval = this_result;
            actual_file = this_actual_file;
        } else {
            if (headers_are_equal(rval, this_result) &&
                actual_file == this_actual_file) {
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
        return NULL;
    } else if (parent_class->seek_file) {
        parent_class->seek_file(dself, file);
    }

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
    g_return_val_if_fail(self != NULL, FALSE);

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        SeekBlockOp * op;
        if ((int)i == self->private->failed)
            continue; /* This device is broken. */
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->base.child_index = i;
        op->block = block;
        g_ptr_array_add(ops, op);
    }
    
    do_rait_child_ops(seek_block_do_op, ops, NULL);

    success = g_ptr_array_union_robust(RAIT_DEVICE(self),
                                       ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else if (parent_class->seek_block) {
        return parent_class->seek_block(dself, block);
    } else {
        return success;
    }
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
}

/* A BooleanExtractor. This one checks for a successful read. */
static gboolean extract_boolean_read_block_op_data(gpointer data) {
    ReadBlockOp * op = data;
    return GPOINTER_TO_INT(op->base.result) == op->desired_read_size;
}

/* A BooleanExtractor. This one checks for EOF. */
static gboolean extract_boolean_read_block_op_eof(gpointer data) {
    ReadBlockOp * op = data;
    return device_is_eof(op->base.child);
}

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
                                      gpointer buf) {
    guint num_children, data_children;
    int blocksize, child_blocksize;
    guint i;
    int parity_child;
    gpointer parity_block = NULL;
    gboolean success;

    success = TRUE;
    find_simple_params(self, &num_children, &data_children, &blocksize);
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
            memcpy(buf + child_blocksize * op->base.child_index, op->buffer,
                   child_blocksize);
        }
    }

    if (self->private->status == RAIT_STATUS_COMPLETE) {
        if (num_children >= 2) {
            /* Verify the parity block. This code is inefficient but
               does the job for the 2-device case, too. */
            gpointer constructed_parity;
            GPtrArray * data_extents;
            
            constructed_parity = malloc(child_blocksize);
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
                fprintf(stderr, "RAIT is inconsistant: "
                        "Parity block did not match data blocks.\n");
                success = FALSE;
            }
            g_ptr_array_free(data_extents, TRUE);
            amfree(constructed_parity);
        } else { /* do nothing. */ }
    } else if (self->private->status == RAIT_STATUS_DEGRADED) {
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
                                      buf + (child_blocksize *
                                             self->private->failed),
                                      child_blocksize);

            /* The array members belong to our ops argument. */
            g_ptr_array_free(data_extents, TRUE);
        } else {
            g_assert_not_reached();
        }
    } else {
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
    int blocksize;

    RaitDevice * self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, -1);

    find_simple_params(self, &num_children, &data_children,
                       &blocksize);

    g_return_val_if_fail(*size >= (int)device_write_min_size(dself), -1);
    g_assert(blocksize % data_children == 0); /* If not we are screwed */


    ops = g_ptr_array_sized_new(num_children);
    for (i = 0; i < num_children; i ++) {
        ReadBlockOp * op;
        if ((int)i == self->private->failed)
            continue; /* This device is broken. */
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->base.child_index = i;
        op->buffer = malloc(blocksize / data_children);
        op->desired_read_size = op->read_size = blocksize / data_children;
        g_ptr_array_add(ops, op);
    }
    
    do_rait_child_ops(read_block_do_op, ops, NULL);

    if (g_ptr_array_count(ops, extract_boolean_read_block_op_data)) {
        success =
            g_ptr_array_union_robust(RAIT_DEVICE(self),
                                     ops,
                                     extract_boolean_read_block_op_data) &&
            raid_block_reconstruction(RAIT_DEVICE(self),
                                      ops, buf);
    } else {
        success = FALSE;
        if (g_ptr_array_union_robust(RAIT_DEVICE(self),
                                     ops,
                                     extract_boolean_read_block_op_eof)) {
            /* We hit EOF. */
            dself->is_eof = TRUE;
        }
    }

    for (i = 0; i < ops->len; i ++) {
        ReadBlockOp * op = g_ptr_array_index(ops, i);
        amfree(op->buffer);
    }
    g_ptr_array_free_full(ops);

    if (success) {
        if (parent_class->read_block)
            parent_class->read_block(dself, buf, size);
        return blocksize;
    } else {
        return -1;
    }
}

typedef struct {
    GenericOp base;
    DevicePropertyId id; /* IN */
    GValue value;        /* IN/OUT */
} PropertyOp;

/* Creates a GPtrArray of PropertyOf for a get or set operation. */
static GPtrArray * make_property_op_array(RaitDevice * self,
                                          DevicePropertyId id,
                                          GValue * value) {
    guint i;
    GPtrArray * ops;
    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        PropertyOp * op;
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->id = id;
        bzero(&(op->value), sizeof(op->value));
        if (value != NULL) {
            g_value_unset_copy(value, &(op->value));
        }
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

/* Merge ConcurrencyParadigm results. */
static gboolean property_get_concurrency(GPtrArray * ops, GValue * val) {
    ConcurrencyParadigm result = CONCURRENCY_PARADIGM_RANDOM_ACCESS;
    guint i = 0;
    
    for (i = 0; i < ops->len; i ++) {
        ConcurrencyParadigm cur;
        PropertyOp * op = g_ptr_array_index(ops, i);
        g_return_val_if_fail(G_VALUE_TYPE(&(op->value)) ==
                             CONCURRENCY_PARADIGM_TYPE, FALSE);
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
            g_return_val_if_fail(FALSE, FALSE);
        }
    }

    g_value_unset_init(val, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(val, result);
    return TRUE;
}

/* Merge StreamingRequirement results. */
static gboolean property_get_streaming(GPtrArray * ops, GValue * val) {
    StreamingRequirement result = STREAMING_REQUIREMENT_NONE;
    guint i = 0;
    
    for (i = 0; i < ops->len; i ++) {
        StreamingRequirement cur;
        PropertyOp * op = g_ptr_array_index(ops, i);
        g_return_val_if_fail(G_VALUE_TYPE(&(op->value)) ==
                             STREAMING_REQUIREMENT_TYPE, FALSE);
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
            g_return_val_if_fail(FALSE, FALSE);
        }
    }

    g_value_unset_init(val, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(val, result);
    return TRUE;
}
    
/* Merge MediaAccessMode results. */
static gboolean property_get_medium_type(GPtrArray * ops, GValue * val) {
    MediaAccessMode result = 0;
    guint i = 0;

    for (i = 0; i < ops->len; i ++) {
        StreamingRequirement cur;
        PropertyOp * op = g_ptr_array_index(ops, i);
        g_return_val_if_fail(G_VALUE_TYPE(&(op->value)) ==
                             MEDIA_ACCESS_MODE_TYPE, FALSE);
        cur = g_value_get_enum(&(op->value));
        
        if (i == 0) {
            result = cur;
        } else if ((result == MEDIA_ACCESS_MODE_READ_ONLY &&
                    cur == MEDIA_ACCESS_MODE_WRITE_ONLY) ||
                   (result == MEDIA_ACCESS_MODE_WRITE_ONLY &&
                    cur == MEDIA_ACCESS_MODE_READ_ONLY)) {
            /* Invalid combination; one device can only read, other
               can only write. */
            return FALSE;
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
            g_return_val_if_fail(FALSE, FALSE);
        }
    }
    
    g_value_unset_init(val, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(val, result);
    return TRUE;
}
    
/* Merge QualifiedSize results. */
static gboolean property_get_free_space(GPtrArray * ops, GValue * val) {
    QualifiedSize result;
    guint i = 0;

    for (i = 0; i < ops->len; i ++) {
        QualifiedSize cur;
        PropertyOp * op = g_ptr_array_index(ops, i);
        g_return_val_if_fail(G_VALUE_TYPE(&(op->value)) ==
                             QUALIFIED_SIZE_TYPE, FALSE);
        cur = *(QualifiedSize*)(g_value_get_boxed(&(op->value)));

        if (result.accuracy != cur.accuracy) {
            result.accuracy = SIZE_ACCURACY_ESTIMATE;
        }

        if (result.accuracy == SIZE_ACCURACY_UNKNOWN &&
            cur.accuracy != SIZE_ACCURACY_UNKNOWN) {
            result.bytes = cur.bytes;
        } else if (result.accuracy != SIZE_ACCURACY_UNKNOWN &&
                   cur.accuracy == SIZE_ACCURACY_UNKNOWN) {
            /* result.bytes unchanged. */
        } else {
            result.bytes = MIN(result.bytes, cur.bytes);
        }
    }

    g_value_unset_init(val, QUALIFIED_SIZE_TYPE);
    g_value_set_boxed(val, &result);
    return TRUE;
}
    
/* Merge boolean results by ANDing them together. */
static gboolean property_get_boolean_and(GPtrArray * ops, GValue * val) {
    gboolean result = FALSE;
    guint i = 0;

    for (i = 0; i < ops->len; i ++) {
        gboolean cur;
        PropertyOp * op = g_ptr_array_index(ops, i);
        g_return_val_if_fail(G_VALUE_HOLDS_BOOLEAN(&(op->value)), FALSE);
        cur = g_value_get_boolean(&(op->value));

        result = result && cur;
    }

    g_value_unset_init(val, G_TYPE_BOOLEAN);
    g_value_set_boolean(val, result);
    return TRUE;
}
    

static gboolean 
rait_device_property_get (Device * dself, DevicePropertyId id, GValue * val) {
    GPtrArray * ops;
    guint i;
    gboolean success;
    GValue result;
    GValue * first_value;
    RaitDevice * self = RAIT_DEVICE(dself);
    g_return_val_if_fail(self != NULL, FALSE);

    /* Some properties are handled completely differently. */
    if (id == PROPERTY_BLOCK_SIZE) {
        g_value_unset_init(val, G_TYPE_INT);
        g_value_set_int(val, self->private->block_size);
        return TRUE;
    } else if (id == PROPERTY_MIN_BLOCK_SIZE ||
        id == PROPERTY_MAX_BLOCK_SIZE) {
        g_value_unset_init(val, G_TYPE_UINT);
        g_value_set_uint(val, self->private->block_size);
        return TRUE;
    } else if (id == PROPERTY_CANONICAL_NAME) {
        if (parent_class->property_get != NULL) {
            return parent_class->property_get(dself, id, val);
        } else {
            return FALSE;
        }
    }

    ops = make_property_op_array(self, id, NULL);
    
    do_rait_child_ops(property_get_do_op, ops, NULL);

    if (id == PROPERTY_CONCURRENCY) {
        success = property_get_concurrency(ops, val);
    } else if (id == PROPERTY_STREAMING) { 
        success = property_get_streaming(ops, val);
    } else if (id == PROPERTY_APPENDABLE ||
               id == PROPERTY_PARTIAL_DELETION) {
        success = property_get_boolean_and(ops, val);
    } else if (id == PROPERTY_MEDIUM_TYPE) {
        success = property_get_medium_type(ops, val);
    } else if (id == PROPERTY_FREE_SPACE) {
        success = property_get_free_space(ops, val);
    } else {
        /* Generic handling; if all results are the same, we succeed
           and return that result. If not, we fail. */
        success = TRUE;
        
        /* Set up comparison value. */
        bzero(&result, sizeof(result));
        first_value = &(((PropertyOp*)g_ptr_array_index(ops,0))->value);
        if (G_IS_VALUE(first_value)) {
            g_value_unset_copy(first_value, &result);
        } else {
            success = FALSE;
        }
        
        for (i = 0; i < ops->len; i ++) {
            PropertyOp * op = g_ptr_array_index(ops, i);
            if (!GPOINTER_TO_INT(op->base.result) ||
                !G_IS_VALUE(first_value) ||
                !g_value_compare(&result, &(op->value))) {
                success = FALSE;
            }
            g_value_unset(&(op->value));
        }

        if (success) {
            memcpy(val, &result, sizeof(result));
        } else if (G_IS_VALUE(&result)) {
            g_value_unset(&result);
        }
    }

    g_ptr_array_free_full(ops);

    return success;
}

/* A GFunc. */
static void property_set_do_op(gpointer data,
                               gpointer user_data G_GNUC_UNUSED) {
    PropertyOp * op = data;
    op->base.result =
        GINT_TO_POINTER(device_property_set(op->base.child, op->id,
                                            &(op->value)));
}

static gboolean 
rait_device_property_set (Device * self, DevicePropertyId id, GValue * val) {
    GPtrArray * ops;
    gboolean success;

    ops = make_property_op_array(RAIT_DEVICE(self), id, val);
    
    do_rait_child_ops(property_set_do_op, ops, NULL);

    success = g_ptr_array_union_robust(RAIT_DEVICE(self),
                                       ops, extract_boolean_generic_op);
    
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
    g_return_val_if_fail(self != NULL, FALSE);

    ops = g_ptr_array_sized_new(self->private->children->len);
    for (i = 0; i < self->private->children->len; i ++) {
        RecycleFileOp * op;
        op = malloc(sizeof(*op));
        op->base.child = g_ptr_array_index(self->private->children, i);
        op->filenum = filenum;
        g_ptr_array_add(ops, op);
    }
    
    do_rait_child_ops(recycle_file_do_op, ops, NULL);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else if (parent_class->recycle_file) {
        return parent_class->recycle_file(dself, filenum);
    } else {
        return TRUE;
    }
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

    ops = make_generic_boolean_op_array(RAIT_DEVICE(self));
    
    do_rait_child_ops(finish_do_op, ops, NULL);

    success = g_ptr_array_and(ops, extract_boolean_generic_op);

    g_ptr_array_free_full(ops);

    if (!success) {
        return FALSE;
    } else if (parent_class->finish) {
        return parent_class->finish(self);
    } else {
        return TRUE;
    }
}

Device *
rait_device_factory (char * type, char * name) {
    Device * rval;
    g_assert(0 == strcmp(type, "rait"));
    rval = DEVICE(g_object_new(TYPE_RAIT_DEVICE, NULL));
    if (!device_open_device(rval, name)) {
        g_object_unref(rval);
        return NULL;
    } else {
        return rval;
    }
}

Device * rait_device_new_from_devices(Device ** devices) {
    RaitDevice * rval;
    int i;
    gboolean success = TRUE;

    g_return_val_if_fail(devices != NULL && *devices != NULL, NULL);

    rval = RAIT_DEVICE(g_object_new(TYPE_RAIT_DEVICE, NULL));

    for (i = 0; devices[i] != NULL; i ++) {
        g_assert(IS_DEVICE(devices[i]));
        if (devices[i]->access_mode != ACCESS_NULL) {
            success = FALSE;
            break;
        }
        g_object_ref(devices[i]);
        g_ptr_array_add(PRIVATE(rval)->children, devices[i]);
    }

    success = success && find_block_size(rval);

    if (!success) {
        g_ptr_array_free(PRIVATE(rval)->children, TRUE);
        return NULL;
    } else {
        register_properties(rval);

        return DEVICE(rval);
    }
}

void 
rait_device_register (void) {
    static const char * device_prefix_list[] = {"rait", NULL};
    register_device(rait_device_factory, device_prefix_list);
}
