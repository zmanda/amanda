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
typedef struct {
    GObject __parent__;

    /* You can peek at the stuff below, but only subclasses should
       change these values.*/

    /* What file, block are we at? (and are we in the middle of a
     * file?) This is automatically updated by
     * the default implementations of start_file, finish_file,
     * write_block, read_block, seek_file, and seek_block. */
    int file;
    guint64 block;
    gboolean in_file;
    /* Holds the user-specified device name. */
    char * device_name;
    /* Holds the user-specified access-mode. */
    DeviceAccessMode access_mode;
    /* In reading mode, FALSE unless all the data from the current file
     * was successfully read. */
    gboolean is_eof;
    /* Holds the label and time of the currently-inserted volume,
     * or NULL if it has not been read/written yet. */
    char * volume_label;
    char * volume_time;

    /* Holds an error message if the function returned an error. Private --
     * use device_error and friends to access these */
    char * errmsg;
    DeviceStatusFlags status;

    DevicePrivate * private;
} Device;

/* Pointer to factory function for device types. The factory functions
   take control of their arguments, which should be dynamically
   allocated. The factory should call open_device() with this
   device_name. */
typedef Device* (*DeviceFactory)(char * device_type,
                                 char * device_name);

/* This function registers a new device with the allocation system.
 * Call it after you register your type with the GLib type system.
 * This function takes ownership of the strings inside device_prefix_list,
 * but not the device_prefix_list itself. */
extern void register_device(DeviceFactory factory,
                            const char ** device_prefix_list);

/*
 * Class definition
 */
typedef struct _DeviceClass DeviceClass;
struct _DeviceClass {
    GObjectClass __parent__;
    gboolean (* open_device) (Device * self,
                              char * device_name); /* protected */
    DeviceStatusFlags (* read_label)(Device * self);
    gboolean (* start) (Device * self, DeviceAccessMode mode,
                        char * label, char * timestamp);
    gboolean (* start_file) (Device * self, const dumpfile_t * info);
    gboolean (* write_block) (Device * self, guint size, gpointer data,
                              gboolean last_block);
    gboolean (* write_from_fd) (Device * self, queue_fd_t *queue_fd);
    gboolean (* finish_file) (Device * self);
    dumpfile_t* (* seek_file) (Device * self, guint file);
    gboolean (* seek_block) (Device * self, guint64 block);
    gboolean (* read_block) (Device * self, gpointer buf, int * size);
    gboolean (* read_to_fd) (Device * self, queue_fd_t *queue_fd);
    gboolean (* property_get) (Device * self, DevicePropertyId id,
                               GValue * val);
    gboolean (* property_set) (Device * self, DevicePropertyId id,
                               GValue * val);
    gboolean (* recycle_file) (Device * self, guint filenum);
    gboolean (* finish) (Device * self);
};


/*
 * Public methods
 *
 * Note to implementors: The default implementation of many of these
 * methods does not follow the documentation. For example, the default
 * implementation of device_read_block will always return -1, but
 * nonetheless update the block index in the Device structure. In
 * general, it is OK to chain up to the default implmentation after
 * successfully implementing whatever appears below. The particulars
 * of what the default implementations do is documented in device.c.
 */
GType	device_get_type	(void);

/* This is how you get a new Device. Pass in a device name like
 * file:/path/to/storage, a Device is always returned, you must check
 * the device->status to know if the device is valid to be used.
 * If device->status is not DEVICE_STATUS_SUCCESS, then device->errmsg
 * is a pointer to an error message. */
Device* 	device_open	(char * device_name);

/* return the error message or the string "Unkonwn Device error" */
char *device_error(Device * self);

/* return a string version of the status */
char *device_status_error(Device * self);

/* Return errmsg if it is set or a string version of the status */
char *device_error_or_status(Device * self);

/* This instructs the device to read the label on the current
 * volume. device->volume_label will not be initalized until after this
 * is called. You are encouraged to read the label only after setting any
 * properties that may affect the label-reading process. */
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
 * the current time. */
gboolean 	device_start	(Device * self,
                                 DeviceAccessMode mode, char * label,
                                 char * timestamp);

/* This undoes device_start, returning you to the NULL state. Do this
 * if you want to (for example) change access mode.
 * 
 * Note to subclass implementors: Call this function first from your
 * finalization function. */
gboolean 	device_finish	(Device * self);

/* But you can't write any data until you call this function, too.
 * This function does not take ownership of the passed dumpfile_t; you must
 * free it yourself. */
gboolean        device_start_file       (Device * self,
                                         const dumpfile_t * jobInfo);

guint           device_write_min_size   (Device * self);
guint           device_write_max_size   (Device * self);
guint           device_read_max_size   (Device * self);

/* Does what you expect. size had better be inside the block size
 * range, or this function will write nothing.
 *
 * The short_block parameter needs some additional explanation: If
 * short_block is set to TRUE, then this function will accept a write
 * smaller than the minimum block size, subject to the following
 * caveats:
 * % The block may be padded with NULL bytes, which will be present on
 *   restore.
 * % device_write_block will automatically call device_finish_file()
 *   after writing this short block.
 * It is permitted to use short_block with a block that is not short;
 * in this case, it is equivalent to calling device_write() and then
 * calling device_finish_file(). */
gboolean 	device_write_block	(Device * self,
                                         guint size,
                                         gpointer data,
                                         gboolean short_block);

/* This will drain the given fd (reading until EOF), and write the
 * resulting data out to the device using maximally-sized blocks. */
gboolean 	device_write_from_fd	(Device * self,
					queue_fd_t *queue_fd);

/* Call this when you are finished writing a file. This function will
 * write a filemark or the local equivalent, flush the buffers, and do
 * whatever dirty work needs to be done at such a point in time. */
gboolean 	device_finish_file	(Device * self);

/* For reading only: Seeks to the beginning of a particular
 * filemark. Only do this when reading; opening in
 * ACCESS_WRITE will start you out at the first file, and opening in
 * ACCESS_APPEND will automatically seek to the end of the medium.
 * 
 * If the requested file doesn't exist, this function will seek to the
 * next-numbered valid file. You can check where this function seeked to
 * by examining the file field of the Device structure. If the requested
 * file number is exactly one more than the last valid file, this
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
 * It is not an error if buffer == NULL and *size == 0. This should be
 * treated as a query as to the possible size of the next block. */
int 	device_read_block	(Device * self,
                                 gpointer buffer,
                                 int * size);

/* This is the reading equivalent of device_write_from_fd(). It will
 * read from the device from the current location until end of file,
 * and drains the results out into the specified fd. Returns FALSE if
 * there is a problem writing to the fd. */
gboolean 	device_read_to_fd	(Device * self,
					queue_fd_t *queue_fd);

/* This function tells you what properties are supported by this
 * device, and when you are allowed to get and set them. The return
 * value is an array of DeviceProperty structs. The last struct in
 * the array is zeroed, so you know when the end is (check the
 * pointer element "base"). The return value from this function on any
 * given object (or physical device) should be invariant. */
const DeviceProperty * 	device_property_get_list	(Device * self);

/* These functions get or set a particular property. The val should be
 * compatible with the DevicePropertyBase associated with the given
 * DevicePropertyId, and this function should only be called when
 * DeviceProperty.access says it is OK. Otherwise you will get an
 * error and not the tasty property action you wanted. */
gboolean 	device_property_get	(Device * self,
                                         DevicePropertyId id,
                                         GValue * val);
gboolean 	device_property_set	(Device * self,
                                         DevicePropertyId id,
                                         GValue * val);

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

/* Protected methods. Don't call these except in subclass implementations. */

/* Registers a new device / property pair. Every superclass of Device
 * should call this in its init() function. At the moment, any
 * particular property Id can only be registered once per object.
 *
 * If you want to register a standard response to a property (e.g.,
 * whether or not the device supports compression), you can pass a
 * non-NULL response. Then the default implementation of
 * device_get_property (which you may override) will return this
 * response.
 * The contents of prop and response are copied into a private array, so the
 * calling function retains ownership of all arguments.
 */
void            device_add_property(Device * self, DeviceProperty * prop,
                                    GValue * response);

/* This method provides post-construction initalization once the
 * device name is known. It should only be used by Device
 * factories. It is provided here as a virtual method (instead of
 * a static function) because some devices may want to chain
 * initilization to their parents. */
gboolean device_open_device (Device * self,
                             char * device_name);

/* Builds a proper header based on device block size possibilities.
 * If non-null, size is filled in with the number of bytes that should
 * be written.
 * If non-null, oneblock is filled in with TRUE if the header will fit
 * in a single Device block (FALSE otherwise). */
char * device_build_amanda_header(Device * self, const dumpfile_t * jobinfo,
                                  int * size, gboolean * oneblock);

/* Does what you expect. You have to free the returned header. Ensures
   that self->volume_time matches the header written to tape. */
dumpfile_t * make_tapestart_header(Device * self, char * label,
                                   char * timestamp);

/* Does what you expect. Uses the current time. */
dumpfile_t * make_tapeend_header(void);

/* Set up first-run properties from loaded configuration file, including
 * DEVICE_MAX_VOLUME_USAGE property based on the tapetype. */
void device_set_startup_properties_from_config(Device * device);

/* Erase any stored volume information. Use this if something happens (e.g.,
 * a property is set) that voids previously-read volume details.
 * This function is a NOOP unless the device is in the NULL state. */
void device_clear_volume_details(Device * device);

#endif /* DEVICE_H */
