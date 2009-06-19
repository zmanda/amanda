/*
 * Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
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

/* This structure is a Flags (bitwise OR of values). Zero indicates success;
 * any other value indicates some kind of problem reading the label. If
 * multiple bits are set, it does not necessarily indicate that /all/ of
 * the specified issues occured, but rather that /at least one/ did. */
typedef enum {
    /* When changing, Also update device_status_flags_values in
     * device-src/device.c and perl/Amanda/Device.swg */
    DEVICE_STATUS_SUCCESS          = 0,

    /* The device is in an unresolvable error state, and
     * further retries are unlikely to change the status */
    DEVICE_STATUS_DEVICE_ERROR     = (1 << 0),

    /* The device is in use, and should be retried later */
    DEVICE_STATUS_DEVICE_BUSY      = (1 << 1),

    /* The device itself is OK, but has no media loaded.  This
     * may change if media is loaded by the user or a changer */
    DEVICE_STATUS_VOLUME_MISSING   = (1 << 2),

    /* The device is OK and media is laoded, but there is
     * no Amanda header or an invalid header on the media. */
    DEVICE_STATUS_VOLUME_UNLABELED = (1 << 3),

    /* The device is OK, but there was an unresolvable error
     * loading the header from the media, so subsequent reads
     * or writes will probably fail. */
    DEVICE_STATUS_VOLUME_ERROR     = (1 << 4),

    DEVICE_STATUS_FLAGS_MAX        = (1 << 5)
} DeviceStatusFlags;

#define DEVICE_STATUS_FLAGS_MASK (DEVICE_STATUS_MAX-1)
#define DEVICE_STATUS_FLAGS_TYPE (device_status_flags_get_type())
GType device_status_flags_get_type(void);

/*
 * Main object structure
 */
typedef struct Device {
    GObject __parent__;

    /* You can peek at the stuff below, but only subclasses should
       change these values.*/

    /* What file, block are we at? (and are we in the middle of a * file?) */
    int file;
    guint64 block;
    gboolean in_file;

    /* Holds the user-specified device name, which may be an alias */
    char * device_name;

    /* Holds the user-specified access-mode, or ACCESS_NULL if the device
     * has not yet been started*/
    DeviceAccessMode access_mode;

    /* In reading mode, FALSE unless all the data from the current file
     * was successfully read.  In writing mode, TRUE if the end of tape
     * has been reached. */
    gboolean is_eof;

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
    gboolean (* write_from_fd) (Device * self, queue_fd_t *queue_fd);
    gboolean (* finish_file) (Device * self);
    dumpfile_t* (* seek_file) (Device * self, guint file);
    gboolean (* seek_block) (Device * self, guint64 block);
    int (* read_block) (Device * self, gpointer buf, int * size);
    gboolean (* read_to_fd) (Device * self, queue_fd_t *queue_fd);
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
    gboolean (* finish) (Device * self);

    /* array of DeviceProperty objects for this class, keyed by ID */
    GArray *class_properties;

    /* The return value of device_property_get_list */
    GSList * class_properties_list;
};

/*
 * Device Instantiation
 */

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
 */

/* This instructs the device to read the label on the current volume.
 * device->volume_label will not be initalized until read_label or start is
 * called. You are encouraged to read the label only after setting any
 * properties that may affect the label-reading process. Also, after
 * calling this function, device->volume_label and device->volume_time
 * will be non-NULL if and only if this function returns
 * DEVICE_STATUS_SUCCESS. */
DeviceStatusFlags        device_read_label (Device * self);

/* This tells the Device that it's OK to start reading and writing
 * data. Before you call this, you can only call
 * device_property_{get, set} and device_read_label. You can only call
 * this a second time if you call device_finish() first.
 *
 * You should pass a label and timestamp if and only if you are
 * opening in WRITE mode (not READ or APPEND). The label and timestamp
 * remain the caller's responsibility in terms of memory management. The
 * passed timestamp may be NULL, in which case it will be filled in with
 * the current time.
 *
 * Note that implementations need not calculate a the current time: the
 * dispatch function does it for you. */
gboolean 	device_start	(Device * self,
                                 DeviceAccessMode mode, char * label,
                                 char * timestamp);

/* This undoes device_start, returning you to the NULL state. Do this
 * if you want to (for example) change access mode.
 *
 * Note to subclass implementors: Call this function first from your
 * finalization function. */
gboolean 	device_finish	(Device * self);

/* But you can't write any data until you call this function, too.  This
 * function does not take ownership of the passed dumpfile_t; you must free it
 * yourself.  Note that this function *does* set the blocksize field of the
 * header properly, based on the size of the header block.  */
gboolean        device_start_file       (Device * self,
                                         dumpfile_t * jobInfo);

/* Does what you expect. Size must be device->block_size or less.
 * If less, then this is the final block in the file, and no more blocks
 * may be written until finish_file and start_file have been called. */
gboolean 	device_write_block	(Device * self,
                                         guint size,
                                         gpointer data);

/* This will drain the given fd (reading until EOF), and write the
 * resulting data out to the device using maximally-sized blocks.
 * This function does not call device_finish_file automatically.
 */
gboolean 	device_write_from_fd	(Device * self,
					queue_fd_t *queue_fd);

/* Call this when you are finished writing a file.
 * This function will write a filemark or the local
 * equivalent, flush the buffers, and do whatever dirty work needs
 * to be done at such a point in time. */
gboolean 	device_finish_file	(Device * self);

/* For reading only: Seeks to the beginning of a particular
 * filemark. Only do this when reading; opening in
 * ACCESS_WRITE will start you out at the first file, and opening in
 * ACCESS_APPEND will automatically seek to the end of the medium.
 *
 * If the requested file doesn't exist, as might happen when a volume has
 * had files recycled, then this function will seek to the next file that
 * does exist. You can check which file this function selected by
 * examining the file field of the Device structure. If the requested
 * file number is *exactly* one more than the last valid file, this
 * function returns a TAPEEND header.
 *
 * If an error occurs or if the requested file is two or more beyond the
 * last valid file, this function returns NULL.
 *
 * Example results for a volume that has only files 1 and 3:
 * 1 -> Seeks to file 1
 * 2 -> Seeks to file 3
 * 3 -> Seeks to file 3
 * 4 -> Returns TAPEEND
 * 5 -> Returns NULL
 *
 * The returned dumpfile_t is yours to keep, at no extra charge. */
dumpfile_t* 	device_seek_file	(Device * self,
					guint file);

/* After you have called device_seek_file (and /only/ after having
 * called device_seek_file), you can call this to seek to a particular
 * block inside the file. It works like SEEK_SET, only in blocks. */
gboolean 	device_seek_block	(Device * self,
					guint64 block);

/* After you have called device_seek_file and/or device_seek_block,
 * you can start calling this function. It always reads exactly one whole
 * block at a time, however big that might be. You must pass in a buffer and
 * specify its size. If the buffer is big enough, the read is
 * performed, and both *size and the return value are equal to the
 * number of bytes actually read. If the buffer is not big enough, then
 * no read is performed, the function returns 0, and *size is set
 * to the minimum buffer size required to read the next block. If an
 * error occurs, the function returns -1  and *size is left unchanged.
 *
 * Note that this function may request a block size bigger than
 * dev->block_size, if it discovers an oversized block.  This allows Amanda to
 * read from volumes regardless of the block size used to write them. It is not
 * an error if buffer == NULL and *size == 0. This should be treated as a query
 * as to the possible size of the next block, although it is not an error for
 * the next read to request an even larger block size.  */
int 	device_read_block	(Device * self, gpointer buffer, int * size);

/* This is the reading equivalent of device_write_from_fd(). It will
 * read from the device from the current location until end of file,
 * and drains the results out into the specified fd. Returns FALSE if
 * there is a problem writing to the fd. */
gboolean 	device_read_to_fd	(Device * self,
					queue_fd_t *queue_fd);

/* This function tells you what properties are supported by this device, and
 * when you are allowed to get and set them. The return value is an list of
 * DeviceProperty structs.  Do not free the resulting list. */
const GSList *	device_property_get_list	(Device * self);

/* These functions get or set a particular property. The val should be
 * compatible with the DevicePropertyBase associated with the given
 * DevicePropertyId, and these functions should only be called when
 * DeviceProperty.access says it is OK. Otherwise you will get an error and not
 * the tasty property action you wanted.
 *
 * All device_property_get_ex parameters but the first two are output
 * parameters, and can be left NULL if you are not interested in their value.
 * If you only need the value, use the simpler device_property_get macro. */

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

/* On devices that support it (check PROPERTY_PARTIAL_DELETION),
 * this will free only the space associated with a particular file.
 * This way, you can apply a different retention policy to every file
 * on the volume, appending new data at the end and recycling anywhere
 * in the middle -- even simultaneously (via different Device
 * handles)! Note that you generally can't recycle a file that is presently in
 * use (being read or written).
 *
 * To use this, open the device as DEVICE_MODE_APPEND. But you don't
 * have to call device_start_file(), unless you want to write some
 * data, too. */
gboolean 	device_recycle_file	(Device * self,
					guint filenum);

/* On devices that support it (check PROPERTY_FULL_DELETION),
 * this will free all space used by the device.
 *
 * To use this, open the device as DEVICE_MODE_APPEND. But you don't
 * have to call device_start_file(), unless you want to write some
 * data, too. */
gboolean 	device_erase	(Device * self);

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
