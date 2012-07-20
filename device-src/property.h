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

#ifndef DEVICE_PROPERTY_H
#define DEVICE_PROPERTY_H

#include <glib.h>
#include <glib-object.h>

/* The properties interface defines define capabilities and other interesting
 * properties. */

typedef enum {
    PROPERTY_PHASE_BEFORE_START       = (1 << 0),
    PROPERTY_PHASE_BETWEEN_FILE_WRITE = (1 << 1),
    PROPERTY_PHASE_INSIDE_FILE_WRITE  = (1 << 2),
    PROPERTY_PHASE_BETWEEN_FILE_READ  = (1 << 3),
    PROPERTY_PHASE_INSIDE_FILE_READ   = (1 << 4),
    PROPERTY_PHASE_MAX                = (1 << 5)
} PropertyPhaseFlags;

#define PROPERTY_PHASE_MASK (PROPERTY_PHASE_MAX-1)
#define PROPERTY_PHASE_SHIFT 8

typedef enum {
    PROPERTY_ACCESS_GET_BEFORE_START = (PROPERTY_PHASE_BEFORE_START),
    PROPERTY_ACCESS_GET_BETWEEN_FILE_WRITE =
        (PROPERTY_PHASE_BETWEEN_FILE_WRITE),
    PROPERTY_ACCESS_GET_INSIDE_FILE_WRITE = (PROPERTY_PHASE_INSIDE_FILE_WRITE),
    PROPERTY_ACCESS_GET_BETWEEN_FILE_READ =
        (PROPERTY_PHASE_BETWEEN_FILE_READ),
    PROPERTY_ACCESS_GET_INSIDE_FILE_READ = (PROPERTY_PHASE_INSIDE_FILE_READ),

    PROPERTY_ACCESS_SET_BEFORE_START =
        (PROPERTY_PHASE_BEFORE_START << PROPERTY_PHASE_SHIFT),
    PROPERTY_ACCESS_SET_BETWEEN_FILE_WRITE =
        (PROPERTY_PHASE_BETWEEN_FILE_WRITE << PROPERTY_PHASE_SHIFT),
    PROPERTY_ACCESS_SET_INSIDE_FILE_WRITE =
        (PROPERTY_PHASE_INSIDE_FILE_WRITE << PROPERTY_PHASE_SHIFT),
    PROPERTY_ACCESS_SET_BETWEEN_FILE_READ =
        (PROPERTY_PHASE_BETWEEN_FILE_READ << PROPERTY_PHASE_SHIFT),
    PROPERTY_ACCESS_SET_INSIDE_FILE_READ =
        (PROPERTY_PHASE_INSIDE_FILE_READ << PROPERTY_PHASE_SHIFT)
} PropertyAccessFlags;

#define PROPERTY_ACCESS_GET_MASK (PROPERTY_PHASE_MASK)
#define PROPERTY_ACCESS_SET_MASK (PROPERTY_PHASE_MASK << PROPERTY_PHASE_SHIFT)

/* Some properties can only be occasionally (or unreliably) detected, so
 * this enum allows the user to override the detected or default
 * setting.  Surety indicates a level of confidence in the value, while
 * source describes how we found out about it. */
typedef enum {
    /* Support is not based on conclusive evidence. */
    PROPERTY_SURETY_BAD,
    /* Support is based on conclusive evidence. */
    PROPERTY_SURETY_GOOD,
} PropertySurety;

typedef enum {
    /* property is from default setting. */
    PROPERTY_SOURCE_DEFAULT,
    /* property is from device query. */
    PROPERTY_SOURCE_DETECTED,
    /* property is from user override (configuration). */
    PROPERTY_SOURCE_USER,
} PropertySource;

/*****
 * Initialization
 */

/* This should be called exactly once from device_api_init(). */
extern void device_property_init(void);

/* This structure is usually statically allocated.  It holds information about
 * a property that is common across all devices.
 */
typedef guint DevicePropertyId;
typedef struct {
    DevicePropertyId ID; /* Set by device_property_register() */
    GType type;
    const char *name;
    const char *description;
} DevicePropertyBase;

/* Registers a new property and returns its ID. This function takes ownership
 * of its argument; it must not be freed later.  It should be called from a
 * device driver's registration function. */
extern DevicePropertyId device_property_register(DevicePropertyBase*);

/* Does the same thing, but fills in a new DevicePropertyBase with the given
 * values first, and does not return the ID.  This is more convenient for
 * device-specific properties. */
extern void device_property_fill_and_register(
    DevicePropertyBase * base,
    GType type,
    const char * name,
    const char * desc);

/* Gets a DevicePropertyBase from its ID. */
DevicePropertyBase* device_property_get_by_id(DevicePropertyId);
DevicePropertyBase* device_property_get_by_name(const char*);

/*****
 * Class-level Property Information
 */

/* This structure is held inside a Device object. It holds information about a
 * property that is specific to the device driver, but not to a specific
 * instance of the driver. */
struct Device; /* forward declaration */
typedef gboolean (*PropertySetFn)(
    struct Device *self,
    DevicePropertyBase *base,
    GValue *val,
    PropertySurety surety,
    PropertySource source);
typedef gboolean (*PropertyGetFn)(
    struct Device *self,
    DevicePropertyBase *base,
    GValue *val,
    PropertySurety *surety,
    PropertySource *source);

typedef struct {
    DevicePropertyBase *base;
    PropertyAccessFlags access;
    PropertySetFn setter;
    PropertyGetFn getter;
} DeviceProperty;

/*****
 * Property-specific Types, etc.
 */

/* Standard property value types here.
 * Important: see property.c for the other half of type declarations.*/
typedef enum {
    CONCURRENCY_PARADIGM_EXCLUSIVE,
    CONCURRENCY_PARADIGM_SHARED_READ,
    CONCURRENCY_PARADIGM_RANDOM_ACCESS
} ConcurrencyParadigm;
#define CONCURRENCY_PARADIGM_TYPE concurrency_paradigm_get_type()
GType concurrency_paradigm_get_type (void);

#define STREAMING_REQUIREMENT_TYPE streaming_requirement_get_type()
GType streaming_requirement_get_type (void);

typedef enum {
    MEDIA_ACCESS_MODE_READ_ONLY,
    MEDIA_ACCESS_MODE_WORM,
    MEDIA_ACCESS_MODE_READ_WRITE,
    MEDIA_ACCESS_MODE_WRITE_ONLY
} MediaAccessMode;
#define MEDIA_ACCESS_MODE_TYPE media_access_mode_get_type()
GType media_access_mode_get_type (void);

/* Standard property definitions follow. See also property.c. */

/* Value is a ConcurrencyParadigm */
extern DevicePropertyBase device_property_concurrency;
#define PROPERTY_CONCURRENCY (device_property_concurrency.ID)

/* Value is a StreamingRequirement */
typedef enum {
    STREAMING_REQUIREMENT_NONE,
    STREAMING_REQUIREMENT_DESIRED,
    STREAMING_REQUIREMENT_REQUIRED
} StreamingRequirement;
extern DevicePropertyBase device_property_streaming;
#define PROPERTY_STREAMING (device_property_streaming.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_compression;
#define PROPERTY_COMPRESSION (device_property_compression.ID)

/* Value is a gdouble, representing (compressed size)/(original
   size). The period over which this value is measured is undefined. */
extern DevicePropertyBase device_property_compression_rate;
#define PROPERTY_COMPRESSION_RATE (device_property_compression_rate.ID)

/* Value is a gint; gives the write block size. */
extern DevicePropertyBase device_property_block_size;
#define PROPERTY_BLOCK_SIZE (device_property_block_size.ID)

/* Read-only.  Value is a guint. */
extern DevicePropertyBase device_property_min_block_size;
extern DevicePropertyBase device_property_max_block_size;
#define PROPERTY_MIN_BLOCK_SIZE (device_property_min_block_size.ID)
#define PROPERTY_MAX_BLOCK_SIZE (device_property_max_block_size.ID)

/* Value is a guint; gives the minimum buffer size for reads. Only
 * the tape device implements this, but it corresponds to the tapetype
 * readblocksize parameter, so it's a global property*/
extern DevicePropertyBase device_property_read_block_size;
#define PROPERTY_READ_BLOCK_SIZE (device_property_read_block_size.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_appendable;
#define PROPERTY_APPENDABLE (device_property_appendable.ID)

/* Value is a string. */
extern DevicePropertyBase device_property_canonical_name;
#define PROPERTY_CANONICAL_NAME (device_property_canonical_name.ID)

/* Value is MediaAccessMode. */
extern DevicePropertyBase device_property_medium_access_type;
#define PROPERTY_MEDIUM_ACCESS_TYPE (device_property_medium_access_type.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_partial_deletion;
#define PROPERTY_PARTIAL_DELETION (device_property_partial_deletion.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_full_deletion;
#define PROPERTY_FULL_DELETION (device_property_full_deletion.ID)

/* Value is a guint64. On devices that support it, this property will
   limit the total amount of data written to a volume; attempts to
   write beyond this point will cause the device to simulate "out of
   space". Zero means no limit. */
extern DevicePropertyBase device_property_max_volume_usage;
#define PROPERTY_MAX_VOLUME_USAGE (device_property_max_volume_usage.ID)

/* For devices supporting max_volume_usage this property will be used 
disable/enable property max_volume_usage. If FALSE, max_volume_usage 
will not be verified while writing to the device */
extern DevicePropertyBase device_property_enforce_max_volume_usage;
#define PROPERTY_ENFORCE_MAX_VOLUME_USAGE (device_property_enforce_max_volume_usage.ID)
/* Should the device produce verbose output?  Value is a gboolean.  Not
 * present in all devices. */
extern DevicePropertyBase device_property_verbose;
#define PROPERTY_VERBOSE (device_property_verbose.ID)

/* A comment for the use of the user. */
extern DevicePropertyBase device_property_comment;
#define PROPERTY_COMMENT (device_property_comment.ID)

/* Does this device support LEOM? */
extern DevicePropertyBase device_property_leom;
#define PROPERTY_LEOM (device_property_leom.ID)

#endif
