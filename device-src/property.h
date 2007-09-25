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
#define PROPERTY_PHASE_SHIFT (PROPERTY_PHASE_MASK/2)

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


/* This structure is usually statically allocated.
 * It holds information about a property that is common to all devices of
 * a given type. */

typedef guint DevicePropertyId;

typedef struct {
    DevicePropertyId ID; /* Set by device_property_register() */
    GType type;
    const char *name;
    const char *description;
} DevicePropertyBase;

/* This structure is usually held inside a Device object. It holds
 * information about a property that is specific to the device/medium
 * in question. */
typedef struct {
    const DevicePropertyBase *base;
    PropertyAccessFlags access;
} DeviceProperty;

/* Registers a new property and returns its ID. This function takes ownership
 * of its argument; it must not be freed later. */
extern DevicePropertyId device_property_register(DevicePropertyBase*);

/* This should be called exactly once from device_api_init(). */
extern void device_property_init(void);

/* Gets a DevicePropertyBase from its ID. */
extern const DevicePropertyBase* device_property_get_by_id(DevicePropertyId);
extern const DevicePropertyBase* device_property_get_by_name(const char*);

/* Standard property value types here.
 * Important: see property.c for the other half of type declarations.*/
typedef enum {
    CONCURRENCY_PARADIGM_EXCLUSIVE,
    CONCURRENCY_PARADIGM_SHARED_READ,
    CONCURRENCY_PARADIGM_RANDOM_ACCESS
} ConcurrencyParadigm;
#define CONCURRENCY_PARADIGM_TYPE concurrency_paradigm_get_type()
GType concurrency_paradigm_get_type (void);

typedef enum {
    STREAMING_REQUIREMENT_NONE,
    STREAMING_REQUIREMENT_DESIRED,
    STREAMING_REQUIREMENT_REQUIRED
} StreamingRequirement;
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

/* This one is not a Glibified enum */
typedef enum {
    SIZE_ACCURACY_UNKNOWN,
    SIZE_ACCURACY_ESTIMATE,
    SIZE_ACCURACY_REAL
} SizeAccuracy;

/* But SizeAccuracy does apear in this Glibified (gBoxed) struct. */
typedef struct {
    SizeAccuracy accuracy;
    guint64           bytes;
} QualifiedSize;
#define QUALIFIED_SIZE_TYPE qualified_size_get_type()
GType qualified_size_get_type (void);

/* Some features can only be occasionally (or unreliably) detected, so
   this enum allows the user to override the detected or default
   setting. */
typedef enum {
    /* Feature support status. (exactly one of these is set) */
        /* Feature is supported & will be used */
        FEATURE_STATUS_ENABLED   = (1 << 0),
        /* Features will not be used. */
        FEATURE_STATUS_DISABLED  = (1 << 1),

    /* Feature support confidence. (exactly one of these is set). */
        /* Support is not based on conclusive evidence. */
        FEATURE_SURETY_BAD       = (1 << 2),
        /* Support is based on conclusive evidence. */
        FEATURE_SURETY_GOOD      = (1 << 3),

   /* Source of this information. (exactly one of these is set). */
        /* Source of status is from default setting. */
        FEATURE_SOURCE_DEFAULT   = (1 << 4),
        /* Source of status is from device query. */
        FEATURE_SOURCE_DETECTED  = (1 << 5),
        /* Source of status is from user override. */
        FEATURE_SOURCE_USER      = (1 << 6),

    FEATURE_SUPPORT_FLAGS_MAX = (1 << 7)
} FeatureSupportFlags;

#define FEATURE_SUPPORT_FLAGS_MASK (FEATURE_SUPPORT_FLAGS_MAX-1)
#define FEATURE_SUPPORT_FLAGS_STATUS_MASK (FEATURE_STATUS_ENABLED |  \
                                           FEATURE_STATUS_DISABLED)
#define FEATURE_SUPPORT_FLAGS_SURETY_MASK (FEATURE_SURETY_BAD |      \
                                           FEATURE_SURETY_GOOD)
#define FEATURE_SUPPORT_FLAGS_SOURCE_MASK (FEATURE_SOURCE_DEFAULT |  \
                                           FEATURE_SOURCE_DETECTED | \
                                           FEATURE_SOURCE_USER)
/* Checks that mutually exclusive flags are not set. */
gboolean feature_support_flags_is_valid(FeatureSupportFlags);
#define FEATURE_SUPPORT_FLAGS_TYPE feature_support_get_type()
GType feature_support_get_type (void);    

/* Standard property definitions follow. See also property.c. */

/* Value is a ConcurrencyParadigm */
extern DevicePropertyBase device_property_concurrency;
#define PROPERTY_CONCURRENCY (device_property_concurrency.ID)

/* Value is a StreamingRequirement */
extern DevicePropertyBase device_property_streaming;
#define PROPERTY_STREAMING (device_property_streaming.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_compression;
#define PROPERTY_COMPRESSION (device_property_compression.ID)

/* Value is a gdouble, representing (compressed size)/(original
   size). The period over which this value is measured is undefined. */
extern DevicePropertyBase device_property_compression_rate;
#define PROPERTY_COMPRESSION_RATE (device_property_compression_rate.ID)

/* Value is a gint, where a negative number indicates variable block size. */
extern DevicePropertyBase device_property_block_size;
#define PROPERTY_BLOCK_SIZE (device_property_block_size.ID)

/* Value is a guint. */
extern DevicePropertyBase device_property_min_block_size;
extern DevicePropertyBase device_property_max_block_size;
#define PROPERTY_MIN_BLOCK_SIZE (device_property_min_block_size.ID)
#define PROPERTY_MAX_BLOCK_SIZE (device_property_max_block_size.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_appendable;
#define PROPERTY_APPENDABLE (device_property_appendable.ID)

/* Value is a string. */
extern DevicePropertyBase device_property_canonical_name;
#define PROPERTY_CANONICAL_NAME (device_property_canonical_name.ID)

/* Value is MediaAccessMode. */
extern DevicePropertyBase device_property_medium_access_type;
#define PROPERTY_MEDIUM_TYPE (device_property_medium_access_type.ID)

/* Value is a gboolean. */
extern DevicePropertyBase device_property_partial_deletion;
#define PROPERTY_PARTIAL_DELETION (device_property_partial_deletion.ID)

/* Value is a QualifiedSize, though the accuracy may be SIZE_ACCURACY_NONE. */
extern DevicePropertyBase device_property_free_space;
#define PROPERTY_FREE_SPACE (device_property_free_space.ID)

/* Value is a guint64. On devices that support it, this property will
   limit the total amount of data written to a volume; attempts to
   write beyond this point will cause the device to simulate "out of
   space". Zero means no limit. */
extern DevicePropertyBase device_property_max_volume_usage;
#define PROPERTY_MAX_VOLUME_USAGE (device_property_max_volume_usage.ID)

/* Tape device properties. These properties do not exist on non-linear
   devices. All of them have a value type of FeatureSupportFlags. */
extern DevicePropertyBase device_property_fsf;
#define PROPERTY_FSF (device_property_fsf.ID)

extern DevicePropertyBase device_property_bsf;
#define PROPERTY_BSF (device_property_bsf.ID)

extern DevicePropertyBase device_property_fsr;
#define PROPERTY_FSR (device_property_fsr.ID)

extern DevicePropertyBase device_property_bsr;
#define PROPERTY_BSR (device_property_bsr.ID)

/* Is EOM supported? Must be able to read file number afterwards as
   well. */
extern DevicePropertyBase device_property_eom;
#define PROPERTY_EOM (device_property_eom.ID)

/* Is it necessary to perform a BSF after EOM? */
extern DevicePropertyBase device_property_bsf_after_eom;
#define PROPERTY_BSF_AFTER_EOM (device_property_bsf_after_eom.ID)

/* How many filemarks to write at EOD? (Default is 2).
 * This property is a G_TYPE_UINT, but can only really be set to 1 or 2. */
extern DevicePropertyBase device_property_final_filemarks;
#define PROPERTY_FINAL_FILEMARKS (device_property_final_filemarks.ID)

#endif
