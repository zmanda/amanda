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

/* The Device API abstracts device workings, interaction, properties, and
 * capabilities from the rest of the Amanda code base. It supports
 * pluggable modules for different kinds of devices. */

#ifndef DEVICE_H
#define DEVICE_H

#include <glib.h>
#include <glib-object.h>

#include "property.h"
#include "fileheader.h"
#include "directtcp.h"
#include "directtcp-connection.h"

/* Device API version. */
#define DEVICE_API_VERSION 0

extern void device_api_init(void);

/* Different access modes */
typedef enum {
    ACCESS_NULL, /* Device is not yet opened. */
    ACCESS_READ,
    ACCESS_WRITE,
    ACCESS_APPEND
} DeviceAccessMode;

#define IS_WRITABLE_ACCESS_MODE(mode) ((mode) == ACCESS_WRITE || \
                                       (mode) == ACCESS_APPEND)

/* Device object definition follows. */

/*
 * Type checking and casting macros
 */
GType	device_get_type	(void);
#define TYPE_DEVICE	(device_get_type())
#define DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), device_get_type(), Device)
#define DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), device_get_type(), Device const)
#define DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), device_get_type(), DeviceClass)
#define IS_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), device_get_type ())
#define DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), device_get_type(), DeviceClass)

typedef struct DevicePrivate_s DevicePrivate;

/* See Amanda::Device POD for a description of these constants */
typedef enum {
    DEVICE_STATUS_SUCCESS          = 0,
    DEVICE_STATUS_DEVICE_ERROR     = (1 << 0),
    DEVICE_STATUS_DEVICE_BUSY      = (1 << 1),
    DEVICE_STATUS_VOLUME_MISSING   = (1 << 2),
    DEVICE_STATUS_VOLUME_UNLABELED = (1 << 3),
    DEVICE_STATUS_VOLUME_ERROR     = (1 << 4),
    DEVICE_STATUS_FLAGS_MAX        = (1 << 5)
} DeviceStatusFlags;

#define DEVICE_STATUS_FLAGS_MASK (DEVICE_STATUS_MAX-1)
#define DEVICE_STATUS_FLAGS_TYPE (device_status_flags_get_type())
GType device_status_flags_get_type(void);

/* a callback to prolong an operation */
typedef gboolean (* ProlongProc)(gpointer data);

/*
 * Main object structure
 */
typedef struct Device {
    GObject __parent__;

    /* You can peek at the stuff below, but only subclasses should
       change these values.*/

    /* A mutex to protect field accessed from another thread.
     * Only get_bytes_read and get_bytes_written are allowed from another
     * Only in_file, bytes_read and bytes_written are protected */
    GMutex  *device_mutex;

    /* What file, block are we at? (and are we in the middle of a file?) */
    int file;
    guint64 block;
    gboolean in_file;

    /* Holds the user-specified device name, which may be an alias */
    char * device_name;

    /* Holds the user-specified access-mode, or ACCESS_NULL if the device
     * has not yet been started*/
    DeviceAccessMode access_mode;

    /* In reading mode, FALSE unless all the data from the current file
     * was successfully read.  In writing mode, TRUE if the last byte
     * of a file has been written by write_from_connection. */
    gboolean is_eof;

    /* In writing mode, indicates that the volume is at (or near, if possible)
     * EOM. */
    gboolean is_eom;

    /* Holds the label and time of the currently-inserted volume,
     * or NULL if it has not been read/written yet. */
    char * volume_label;
    char * volume_time;

    /* The most recently read volume header, or NULL if no header was
     * read from this device.  Callers can use this to glean information
     * about the volume beyond volume_label and volume_time.  */
    dumpfile_t *volume_header;

    /* The latest status for the device */
    DeviceStatusFlags status;

    /* device block-size ranges.  These are also available as properties,
     * and by default users can set block_size via property BLOCK_SIZE.
     * Writers should use block_size, and readers should initially use
     * block_size, and expand buffers as directed by read_block. */
    gsize min_block_size;
    gsize max_block_size;
    gsize block_size;
    gsize header_block_size;

    guint64 bytes_read;
    guint64 bytes_written;

    /* surety and source for the block size; if you set block_size directly,
     * set these, too! */
    PropertySurety block_size_surety;
    PropertySource block_size_source;

    DevicePrivate * private;
} Device;

/* Pointer to factory function for device types.
 *
 * device_name is the full name ("tape:/dev/nst0")
 * device_prefix is the prefix ("tape")
 * device_node is what follows the prefix ("/dev/nst0")
 *
 * The caller retains responsibility to free or otherwise handle
 * the passed strings.
 */
typedef Device* (*DeviceFactory)(char *device_name,
				 char * device_prefix,
				 char * device_node);

/* This function registers a new device with the allocation system.
 * Call it after you register your type with the GLib type system.
 * This function assumes that the strings in device_prefix_list are
 * statically allocated. */
extern void register_device(DeviceFactory factory,
                            const char ** device_prefix_list);

/*
 * Class definition
 */
typedef struct _DeviceClass DeviceClass;
struct _DeviceClass {
    GObjectClass __parent__;
    void (* open_device) (Device * self, char * device_name,
		    char * device_prefix, char * device_node);
    gboolean (* configure) (Device * self, gboolean use_global_config);
    DeviceStatusFlags (* read_label)(Device * self);
    gboolean (* start) (Device * self, DeviceAccessMode mode,
                        char * label, char * timestamp);
    gboolean (* start_file) (Device * self, dumpfile_t * info);
    gboolean (* write_block) (Device * self, guint size, gpointer data);
    gboolean (* finish_file) (Device * self);
    dumpfile_t* (* seek_file) (Device * self, guint file);
    gboolean (* seek_block) (Device * self, guint64 block);
    int (* read_block) (Device * self, gpointer buf, int * size);
    gboolean (* property_get_ex) (Device * self, DevicePropertyId id,
				  GValue * val,
				  PropertySurety *surety,
				  PropertySource *source);
    gboolean (* property_set_ex) (Device * self,
				  DevicePropertyId id,
				  GValue * val,
				  PropertySurety surety,
				  PropertySource source);
    gboolean (* recycle_file) (Device * self, guint filenum);
    gboolean (* erase) (Device * self);
    gboolean (* eject) (Device * self);
    gboolean (* finish) (Device * self);
    guint64  (* get_bytes_read) (Device * self);
    guint64  (* get_bytes_written) (Device * self);

    gboolean (* listen)(Device *self, gboolean for_writing, DirectTCPAddr **addrs);
    gboolean (* accept)(Device *self, DirectTCPConnection **conn,
			ProlongProc prolong, gpointer prolong_data);
    int (* accept_with_cond)(Device *self, DirectTCPConnection **conn,
				  GMutex *abort_mutex, GCond *abort_cond);
    gboolean (* connect)(Device *self, gboolean for_writing, DirectTCPAddr *addrs,
			DirectTCPConnection **conn, ProlongProc prolong,
			gpointer prolong_data);
    gboolean (* connect_with_cond)(Device *self, gboolean for_writing,
			DirectTCPAddr *addrs, DirectTCPConnection **conn,
			GMutex *abort_mutex, GCond *abort_cond);
    gboolean (* write_from_connection)(Device *self, guint64 size, guint64 *actual_size);
    gboolean (* read_to_connection)(Device *self, guint64 size, guint64 *actual_size);
    gboolean (* use_connection)(Device *self, DirectTCPConnection *conn);

    /* array of DeviceProperty objects for this class, keyed by ID */
    GArray *class_properties;

    /* The return value of device_property_get_list */
    GSList * class_properties_list;

    /* TRUE if the directtcp methods are implemented by this device class */
    gboolean directtcp_supported;
};

/*
 * Device Instantiation
 */

/* Return the unaliased device name of a device.
 * The returned string must not be freed by the caller.
 */
char*		device_unaliased_name(char * device_name);

/* This is how you get a new Device. Pass in a device name or alias.
 *
 * A Device is *always* returned, even for an invalid device name. You
 * must check the resulting device->status to know if the device is valid
 * to be used. If device->status is not DEVICE_STATUS_SUCCESS, then there
 * was an error opening the device.
 *
 * Note that the Amanda configuration must be initialized, as this function
 * looks for device definitions and other configuration information.
 */
Device*		device_open	(char * device_name);

/* As a special case, a RAIT device can be created from a collection of child
 * devices.  This is used by the RAIT changer, for example.  This function is
 * implemented in rait-device.c.  */
Device*		rait_device_open_from_children(GSList *child_devices);

/* Once you have a new device, you should configure it.  This sets properties
 * on the device based on the user's configuation.  If USE_GLOBAL_CONFIG is
 * true, then any global device_property parameters are processed, along with
 * tapetype and othe relevant parameters.
 */
gboolean device_configure(Device *self, gboolean use_global_config);

/*
 * Error Handling
 */

/* return the error message or the string "Unknown Device error".  The
 * string remains the responsibility of the Device, and should not
 * be freed by the caller. */
char *device_error(Device * self);

/* return a string version of the status.  The string remains the
 * responsibility of the Device, and should not be freed by the
 * caller. */
char *device_status_error(Device * self);

/* Return errmsg if it is set or a string version of the status.  The
 * string remains the responsibility of the Device, and should not
 * be freed by the caller. */
char *device_error_or_status(Device * self);

/* Set the error message for this device; for use internally to the
 * API.  The string becomes the responsibility of the Device.  If
 * ERRMSG is NULL, the message is cleared.  Note that the given flags
 * are OR'd with any existing status flags. */
void device_set_error(Device * self, char *errmsg, DeviceStatusFlags new_flags);

/* Mostly for internal use, this is a boolean check to see whether a given
 * device is in an error state.  If this is TRUE, most operations on the
 * device will fail.
 *
 * The check is for DEVICE_STATUS_DEVICE_ERROR *alone*; if any other bits
 * (e.g., VOLUME_UNLABELED) are set, then the device may not actually be in
 * an error state.
 */
#define device_in_error(dev) \
    ((DEVICE(dev))->status == DEVICE_STATUS_DEVICE_ERROR)

/*
 * Public methods
 *
 * See the Amanda::Device POD for more information here
 */

DeviceStatusFlags        device_read_label (Device * self);
gboolean 	device_start	(Device * self,
                                 DeviceAccessMode mode, char * label,
                                 char * timestamp);
gboolean 	device_finish	(Device * self);
guint64 	device_get_bytes_read	(Device * self);
guint64 	device_get_bytes_written(Device * self);
gboolean        device_start_file       (Device * self,
                                         dumpfile_t * jobInfo);
gboolean 	device_write_block	(Device * self,
                                         guint size,
                                         gpointer data);
gboolean 	device_finish_file	(Device * self);
dumpfile_t* 	device_seek_file	(Device * self,
					guint file);
gboolean 	device_seek_block	(Device * self,
					guint64 block);
int 	device_read_block	(Device * self, gpointer buffer, int * size);
const GSList *	device_property_get_list	(Device * self);
gboolean 	device_property_get_ex	(Device * self,
                                         DevicePropertyId id,
                                         GValue * val,
					 PropertySurety *surety,
					 PropertySource *source);
#define		device_property_get(self, id, val) \
    device_property_get_ex((self), (id), (val), NULL, NULL)
gboolean 	device_property_set_ex	(Device * self,
                                         DevicePropertyId id,
                                         GValue * val,
					 PropertySurety surety,
					 PropertySource source);
#define		device_property_set(self, id, val) \
    device_property_set_ex((self), (id), (val), \
	    PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_USER)
gboolean 	device_recycle_file	(Device * self,
					guint filenum);

gboolean 	device_erase	(Device * self);
gboolean 	device_eject	(Device * self);

#define device_directtcp_supported(self) (DEVICE_GET_CLASS((self))->directtcp_supported)
gboolean device_listen(Device *self, gboolean for_writing, DirectTCPAddr **addrs);
gboolean device_accept(Device *self, DirectTCPConnection **conn,
                ProlongProc prolong, gpointer prolong_data);
int device_accept_with_cond(Device *self, DirectTCPConnection **conn,
				 GMutex *abort_mutex, GCond *abort_cond);
gboolean device_connect(Device *self, gboolean for_writing, DirectTCPAddr *addrs,
			DirectTCPConnection **conn, ProlongProc prolong,
			gpointer prolong_data);
gboolean device_connect_with_cond(Device *self, gboolean for_writing,
			DirectTCPAddr *addrs, DirectTCPConnection **conn,
			GMutex *abort_mutex, GCond *abort_cond);
gboolean device_write_from_connection(Device *self, guint64 size, guint64 *actual_size);
gboolean device_read_to_connection(Device *self, guint64 size, guint64 *actual_size);
gboolean device_use_connection(Device *self, DirectTCPConnection *conn);

/* Protected methods. Don't call these except in subclass implementations. */

/* This method provides post-construction initalization once the
 * device name is known. It should only be used by Device
 * factories. It is provided here as a virtual method (instead of
 * a static function) because some devices may want to chain
 * initilization to their parents. */
void device_open_device (Device * self, char *device_name, char *device_type, char *device_node);

/* Builds a proper header of between *size and self->block_size bytes.
 * Returns NULL if the header does not fit in a single block.  The result
 * must be free'd.  If size is NULL, the block size is used.
 *
 * If size is not NULL, *size is set to the actual size of the generated header.
 */
char * device_build_amanda_header(Device * self, const dumpfile_t * jobinfo,
                                  size_t *size);

/* Does what you expect. You have to free the returned header. Ensures
   that self->volume_time matches the header written to tape. */
dumpfile_t * make_tapestart_header(Device * self, char * label,
                                   char * timestamp);

/* Does what you expect. Uses the current time. */
dumpfile_t * make_tapeend_header(void);

/* Erase any stored volume information. Use this if something happens (e.g.,
 * a property is set) that voids previously-read volume details.
 * This function is a NOOP unless the device is in the NULL state. */
void device_clear_volume_details(Device * device);

/* Property Handling */

/* Registers a property for a new device class; device drivers' GClassInitFunc
 * should call this function for each device-specific property of the class.
 * If either getter or setter is NULL, then the corresponding operation will
 * return FALSE.
 *
 * Note that this will replace any existing registration (e.g., from a parent
 * class).
 */
void device_class_register_property(DeviceClass *klass, DevicePropertyId id,
				    PropertyAccessFlags access,
				    PropertyGetFn getter,
				    PropertySetFn setter);

/* Set a 'simple' property on the device.  This tucks the value away in the
 * object, to be retrieved by device_simple_property_get_fn.  This is most
 * often used in GInstanceInit functions, but can be used at any time to set or
 * change the value of a simple property */
gboolean device_set_simple_property(Device *self, DevicePropertyId id,
				GValue *val, PropertySurety surety,
				PropertySource source);

/* Get a simple property set with device_set_simple_property.  This is a little
 * bit quicker than calling device_property_get_ex(), and does not affect the
 * device's error state.  Returns FALSE if the property has not been set.
 * Surety and source are output parameters and will be ignored if they are
 * NULL. */
gboolean device_get_simple_property(Device *self, DevicePropertyId id,
				    GValue *val, PropertySurety *surety,
				    PropertySource *source);

/* A useful PropertySetFn.  If your subclass also needs to intercept sets, for
 * example to flush a cache or update a member variable, then write a stub
 * function which "calls up" to this function. */
gboolean device_simple_property_set_fn(Device *self, DevicePropertyBase *base,
				       GValue *val, PropertySurety surety,
				       PropertySource source);

/* A useful PropertyGetFn -- returns the value, source, and surety set with
 * device_set_simple_property */
gboolean device_simple_property_get_fn(Device *self, DevicePropertyBase *base,
				       GValue *val, PropertySurety *surety,
				       PropertySource *source);

#endif /* DEVICE_H */
