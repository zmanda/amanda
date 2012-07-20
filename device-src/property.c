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

#include "amanda.h"

#include "property.h"
#include "glib-util.h"

/*****
 * Property-specific Types, etc.
 */

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

/******
 * Property registration and lookup
 */

static GPtrArray *device_property_bases = NULL;
static GHashTable *device_property_bases_by_name = NULL;

DevicePropertyBase* device_property_get_by_id(DevicePropertyId id) {
    if (!device_property_bases || id >= device_property_bases->len)
	return NULL;

    return g_ptr_array_index(device_property_bases, id);
}

DevicePropertyBase* device_property_get_by_name(const char *name) {
    gpointer rv;

    if (!device_property_bases_by_name)
	return NULL;

    rv = g_hash_table_lookup(device_property_bases_by_name, name);
    if (rv)
	return (DevicePropertyBase *)rv;

    return NULL;
}

#define toupper_and_underscore(c) (((c)=='-')? '_' : g_ascii_toupper((c)))
static guint
device_property_hash(
	gconstpointer key)
{
    /* modified version of glib's hash function, copyright
     * GLib Team and others 1997-2000. */
    const char *p = key;
    guint h = toupper_and_underscore(*p);

    if (h)
	for (p += 1; *p != '\0'; p++)
	    h = (h << 5) - h + toupper_and_underscore(*p);

    return h;
}

static gboolean
device_property_equal(
	gconstpointer v1,
	gconstpointer v2)
{
    const char *s1 = v1, *s2 = v2;

    while (*s1 && *s2) {
	if (toupper_and_underscore(*s1) != toupper_and_underscore(*s2))
	    return FALSE;
	s1++, s2++;
    }
    if (*s1 || *s2)
	return FALSE;

    return TRUE;
}

void
device_property_fill_and_register(DevicePropertyBase *base,
		    GType type, const char * name, const char * desc) {

    /* create the hash table and array if necessary */
    if (!device_property_bases) {
	device_property_bases = g_ptr_array_new();
	device_property_bases_by_name = g_hash_table_new(device_property_hash, device_property_equal);
    }

    /* check for a duplicate */
    if (device_property_get_by_name(name)) {
	g_critical("A property named '%s' already exists!", name);
    }

    /* allocate space for this DPB and fill it in */
    base->ID = device_property_bases->len;
    base->type = type;
    base->name = name; /* no strdup -- it's statically allocated */
    base->description = desc; /* ditto */

    /* add it to the array and hash table; note that its array index and its
     * ID are the same. */
    g_ptr_array_add(device_property_bases, base);
    g_hash_table_insert(device_property_bases_by_name, (gpointer)name, (gpointer)base);
}

/******
 * Initialization
 */

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
                                      "Block size to use while writing.");
    device_property_fill_and_register(&device_property_min_block_size,
                                      G_TYPE_UINT, "min_block_size",
      "Minimum supported blocking factor.");
    device_property_fill_and_register(&device_property_max_block_size,
                                      G_TYPE_UINT, "max_block_size",
      "Maximum supported blocking factor.");
    device_property_fill_and_register(&device_property_read_block_size,
                                      G_TYPE_UINT, "read_block_size",
      "Minimum size of a read for this device (maximum expected block size)");
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
    device_property_fill_and_register(&device_property_full_deletion,
                                     G_TYPE_BOOLEAN, "full_deletion",
      "Does this device support recycling the entire volume?" );
    device_property_fill_and_register(&device_property_max_volume_usage,
                                      G_TYPE_UINT64, "max_volume_usage",
      "Artificial limit to data written to volume.");
    device_property_fill_and_register(&device_property_enforce_max_volume_usage,
                                      G_TYPE_BOOLEAN, "enforce_max_volume_usage",
      "Does max_volume_usage enabled?");
    device_property_fill_and_register(&device_property_verbose,
                                     G_TYPE_BOOLEAN, "verbose",
       "Should the device produce verbose output?");
    device_property_fill_and_register(&device_property_comment,
                                     G_TYPE_STRING, "comment",
       "User-specified comment for the device");
    device_property_fill_and_register(&device_property_leom,
                                     G_TYPE_BOOLEAN, "leom",
       "Does this device support LEOM?");
}

DevicePropertyBase device_property_concurrency;
DevicePropertyBase device_property_streaming;
DevicePropertyBase device_property_compression;
DevicePropertyBase device_property_compression_rate;
DevicePropertyBase device_property_block_size;
DevicePropertyBase device_property_min_block_size;
DevicePropertyBase device_property_max_block_size;
DevicePropertyBase device_property_read_block_size;
DevicePropertyBase device_property_appendable;
DevicePropertyBase device_property_canonical_name;
DevicePropertyBase device_property_medium_access_type;
DevicePropertyBase device_property_partial_deletion;
DevicePropertyBase device_property_full_deletion;
DevicePropertyBase device_property_max_volume_usage;
DevicePropertyBase device_property_enforce_max_volume_usage;
DevicePropertyBase device_property_comment;
DevicePropertyBase device_property_leom;
DevicePropertyBase device_property_verbose;
