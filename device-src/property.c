/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amanda.h"

#include "property.h"

static const GEnumValue _concurrency_paradigm_values[] = {
        { CONCURRENCY_PARADIGM_EXCLUSIVE,
          "CONCURRENCY_PARADIGM_EXCLUSIVE",
          "exclusive" },
        { CONCURRENCY_PARADIGM_SHARED_READ, 
          "CONCURRENCY_PARADIGM_SHARED_READ",
          "shared-read" },
        { CONCURRENCY_PARADIGM_RANDOM_ACCESS,
          "CONCURRENCY_PARADIGM_RANDOM_ACCESS",
          "random-access" },
        { 0, NULL, NULL }
};

GType concurrency_paradigm_get_type (void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_enum_register_static ("ConcurrencyParadigm",
                                       _concurrency_paradigm_values);
    }
    return type;
}

static const GEnumValue _streaming_requirement_values[] = {
        { STREAMING_REQUIREMENT_NONE,
          "STREAMING_REQUIREMENT_NONE",
          "none" },
        { STREAMING_REQUIREMENT_DESIRED,
          "STREAMING_REQUIREMENT_DESIRED",
          "desired" },
        { STREAMING_REQUIREMENT_REQUIRED,
          "STREAMING_REQUIREMENT_REQUIRED",
          "required" },
        { 0, NULL, NULL }
};

GType streaming_requirement_get_type (void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_enum_register_static ("StreamingRequirement",
                                       _streaming_requirement_values);
    }
    return type;
}

static const GEnumValue _media_access_mode_values[] = {
        { MEDIA_ACCESS_MODE_READ_ONLY,
          "MEDIA_ACCESS_MODE_READ_ONLY",
          (char *)"read-only" },
        { MEDIA_ACCESS_MODE_WORM,
          "MEDIA_ACCESS_MODE_WORM",
          (char *)"write-once-read-many" },
        { MEDIA_ACCESS_MODE_READ_WRITE,
          "MEDIA_ACCESS_MODE_READ_WRITE",
          (char *)"read-write" },
        { MEDIA_ACCESS_MODE_WRITE_ONLY,
          "MEDIA_ACCESS_MODE_WRITE_ONLY",
          (char *)"write-many-read-never" },
        { 0, NULL, NULL }
};

GType media_access_mode_get_type (void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_enum_register_static ("MediaAccessMode",
                                       _media_access_mode_values);
    }
    return type;
}

/* Copy function for GBoxed QualifiedSize. */
static gpointer qualified_size_copy(gpointer source) {
    gpointer rval = malloc(sizeof(QualifiedSize));
    memcpy(rval, source, sizeof(QualifiedSize));
    return rval;
}

GType qualified_size_get_type (void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_boxed_type_register_static ("QualifiedSize",
                                             qualified_size_copy,
                                             free);
    }
    return type;
}

static const GFlagsValue _feature_support_flags_values[] = {
    { FEATURE_STATUS_ENABLED,
      "FEATURE_STATUS_ENABLED",
      "enabled" },
    { FEATURE_STATUS_DISABLED,
      "FEATURE_STATUS_DISABLED",
      "disabled" },
    { FEATURE_SURETY_BAD,
      "FEATURE_SURETY_BAD",
      "bad" },
    { FEATURE_SURETY_GOOD,
      "FEATURE_SURETY_GOOD",
      "good" },
    { FEATURE_SOURCE_DEFAULT,
      "FEATURE_SOURCE_DEFAULT",
      "default" },
    { FEATURE_SOURCE_DETECTED,
      "FEATURE_SOURCE_DETECTED",
      "detected" },
    { FEATURE_SOURCE_USER,
      "FEATURE_SOURCE_USER",
      "user"},
    { 0, NULL, NULL }
};

GType feature_support_get_type (void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_flags_register_static ("FeatureSupportFlags",
                                        _feature_support_flags_values);
    }
    return type;
}

gboolean feature_support_flags_is_valid(FeatureSupportFlags f) {
    int status = 0, surety = 0, source = 0;

    if (f & FEATURE_STATUS_ENABLED)
        status ++;
    if (f & FEATURE_STATUS_DISABLED)
        status ++;
    if (f & FEATURE_SURETY_BAD)
        surety ++;
    if (f & FEATURE_SURETY_GOOD)
        surety ++;
    if (f & FEATURE_SOURCE_DEFAULT)
        source ++;
    if (f & FEATURE_SOURCE_DETECTED)
        source ++;
    if (f & FEATURE_SOURCE_USER)
        source ++;

    return (!(f & ~FEATURE_SUPPORT_FLAGS_MASK) &&
            status == 1  &&  surety == 1  &&  source == 1);
}

static GSList* device_property_base_list = NULL;

const DevicePropertyBase* device_property_get_by_id(DevicePropertyId id) {
    GSList *iter;

    iter = device_property_base_list;
    while (iter != NULL) {
        DevicePropertyBase* rval = (DevicePropertyBase*)(iter->data);
        if (rval->ID == id) {
            return rval;
        }
        iter = g_slist_next(iter);
    }

    return NULL;
}

const DevicePropertyBase* device_property_get_by_name(const char *name) {
    GSList *iter = device_property_base_list;

    g_return_val_if_fail(name != NULL, NULL);

    while (iter != NULL) {
        DevicePropertyBase* rval = (DevicePropertyBase*)(iter->data);
        if (strcasecmp(rval->name, name) == 0) {
            return rval;
        }
        iter = g_slist_next(iter);
    }

    return NULL;
}

DevicePropertyId device_property_register(DevicePropertyBase* base) {
    static guint id = 0;
    g_assert(base != NULL);
    g_assert(base->ID == -1);
    g_assert(base->name != NULL);
    g_assert(base->description != NULL);
    
    base->ID = id++;

    device_property_base_list = g_slist_prepend(device_property_base_list,
                                                base);
    return id;
}

/* Does the same thing, but fills in a new DevicePropertyBase. */
static void
device_property_fill_and_register(DevicePropertyBase * base,
                                  GType type,
                                  const char * name,
                                  const char * desc) {
    base->type = type;
    base->name = name;
    base->description = desc;
    base->ID = -1;
    device_property_register(base);
}


void device_property_init(void) {
    device_property_fill_and_register(&device_property_concurrency,
                                      CONCURRENCY_PARADIGM_TYPE, "concurrency",
      "Supported concurrency mode (none, multiple readers, multiple writers)");
    device_property_fill_and_register(&device_property_streaming,
                                      STREAMING_REQUIREMENT_TYPE, "streaming",
      "Streaming desirability (unnecessary, desired, required)");
    device_property_fill_and_register(&device_property_compression,
                                      G_TYPE_BOOLEAN, "compression",
      "Is device performing data compression?");
    device_property_fill_and_register(&device_property_compression_rate,
                                      G_TYPE_DOUBLE, "compression_rate",
      "Compression rate, "
          "averaged for some (currently undefined) period of time)");
    device_property_fill_and_register(&device_property_block_size,
                                      G_TYPE_INT, "block_size",
                                      "Device blocking factor in bytes.");
    device_property_fill_and_register(&device_property_min_block_size,
                                      G_TYPE_UINT, "min_block_size",
      "Minimum supported blocking factor.");
    device_property_fill_and_register(&device_property_max_block_size,
                                      G_TYPE_UINT, "max_block_size",
      "Maximum supported blocking factor.");
    device_property_fill_and_register(&device_property_appendable,
                                      G_TYPE_BOOLEAN, "appendable",
      "Does device support appending to previously-written media?");
    device_property_fill_and_register(&device_property_canonical_name,
                                      G_TYPE_STRING, "canonical_name",
      "The most reliable device name to use to refer to this device.");
    device_property_fill_and_register(&device_property_medium_access_type,
                                      MEDIA_ACCESS_MODE_TYPE,
                                      "medium_access_type",
      "What kind of media (RO/WORM/RW/WORN) do we have here?");
    device_property_fill_and_register(&device_property_partial_deletion,
                                     G_TYPE_BOOLEAN, "partial_deletion",
      "Does this device support recycling just part of a volume?" );
    device_property_fill_and_register(&device_property_free_space,
                                      QUALIFIED_SIZE_TYPE, "free_space",
      "Remaining capacity of the device.");
    device_property_fill_and_register(&device_property_max_volume_usage,
                                      G_TYPE_UINT64, "max_volume_usage",
      "Artificial limit to data written to volume.");
    device_property_fill_and_register(&device_property_broken_gmt_online,
                                      G_TYPE_BOOLEAN, "broken_gmt_online",
      "Does this drive support the GMT_ONLINE macro?");
    device_property_fill_and_register(&device_property_fsf,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "fsf",
      "Does this drive support the MTFSF command?");
    device_property_fill_and_register(&device_property_bsf,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "bsf",
      "Does this drive support the MTBSF command?" );
    device_property_fill_and_register(&device_property_fsr,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "fsr",
      "Does this drive support the MTFSR command?");
    device_property_fill_and_register(&device_property_bsr,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "bsr",
      "Does this drive support the MTBSR command?");
    /* FIXME: Is this feature even useful? */
    device_property_fill_and_register(&device_property_eom,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "eom",
      "Does this drive support the MTEOM command?");
    device_property_fill_and_register(&device_property_bsf_after_eom,
                                      FEATURE_SUPPORT_FLAGS_TYPE,
                                      "bsf_after_eom",
      "Does this drive require an MTBSF after MTEOM in order to append?" );
    device_property_fill_and_register(&device_property_final_filemarks,
                                      G_TYPE_UINT, "final_filemarks",
      "How many filemarks to write after the last tape file?" );
    device_property_fill_and_register(&device_property_read_buffer_size,
                                      G_TYPE_UINT, "read_buffer_size",
      "What buffer size should be used for reading?");
    device_property_fill_and_register(&device_property_s3_secret_key,
                                      G_TYPE_STRING, "s3_secret_key",
       "Secret access key to authenticate with Amazon S3");
    device_property_fill_and_register(&device_property_s3_access_key,
                                      G_TYPE_STRING, "s3_access_key",
       "Access key ID to authenticate with Amazon S3");
#ifdef WANT_DEVPAY
    device_property_fill_and_register(&device_property_s3_user_token,
                                      G_TYPE_STRING, "s3_user_token",
       "User token for authentication Amazon devpay requests");
#endif
    device_property_fill_and_register(&device_property_verbose,
                                     G_TYPE_BOOLEAN, "verbose",
       "Should the device produce verbose output?");
}

DevicePropertyBase device_property_concurrency;
DevicePropertyBase device_property_streaming;
DevicePropertyBase device_property_compression;
DevicePropertyBase device_property_compression_rate;
DevicePropertyBase device_property_block_size;
DevicePropertyBase device_property_min_block_size;
DevicePropertyBase device_property_max_block_size;
DevicePropertyBase device_property_appendable;
DevicePropertyBase device_property_canonical_name;
DevicePropertyBase device_property_medium_access_type;
DevicePropertyBase device_property_partial_deletion;
DevicePropertyBase device_property_free_space;
DevicePropertyBase device_property_max_volume_usage;
DevicePropertyBase device_property_broken_gmt_online;
DevicePropertyBase device_property_fsf;
DevicePropertyBase device_property_bsf;
DevicePropertyBase device_property_fsr;
DevicePropertyBase device_property_bsr;
DevicePropertyBase device_property_eom;
DevicePropertyBase device_property_bsf_after_eom;
DevicePropertyBase device_property_final_filemarks;
DevicePropertyBase device_property_read_buffer_size;
DevicePropertyBase device_property_s3_access_key;
DevicePropertyBase device_property_s3_secret_key;
DevicePropertyBase device_property_s3_user_token;
DevicePropertyBase device_property_verbose;
