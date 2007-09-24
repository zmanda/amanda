/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2005 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

/* The Device API abstracts device workings, interaction, properties, and
 * capabilities from the rest of the Amanda code base. It supports
 * pluggable modules for different kinds of devices. */

#include <glib.h>
#include <glib-object.h>

#ifndef DEVICE_H
#define DEVICE_H

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
    /* Is all the data for this file read? */
    gboolean is_eof;
    /* Holds the label and time of the currently-inserted volume,
     * or NULL if it has not been read/written yet. */
    char * volume_label;
    char * volume_time;

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
    gboolean (* read_label)(Device * self);
    gboolean (* start) (Device * self, DeviceAccessMode mode,
                        char * label, char * timestamp);
    gboolean (* start_file) (Device * self, const dumpfile_t * info);
    gboolean (* write_block) (Device * self, guint size, gpointer data,
                              gboolean last_block);
    gboolean (* write_from_fd) (Device * self, int fd);
    gboolean (* finish_file) (Device * self);
    dumpfile_t* (* seek_file) (Device * self, guint file);
    gboolean (* seek_block) (Device * self, guint64 block);
    gboolean (* read_block) (Device * self, gpointer buf, int * size);
    gboolean (* read_to_fd) (Device * self, int fd);
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
 * file:/path/to/storage, and (assuming everything goes OK) you will get
 * back a nice happy Device* that you can do operations on. Note that you
 * must device_start() it before you can do anything besides talk about
 * properties or read the label. */
Device* 	device_open	(char * device_name);

/* This instructs the device to read the label on the current
   volume. It is called automatically after device_open() and before
   device_start(). You can call it yourself anytime between the
   two. It may return FALSE if no label could be read (as in the case
   of an unlabeled volume). */
gboolean        device_read_label (Device * self);

/* This tells the Device that it's OK to start reading and writing
 * data. Before you call this, you can only call
 * device_property_{get, set} and device_read_label. You can only call
 * this a second time if you call device_finish() first.
 *
 * You should pass a label and timestamp if and only if you are
 * opening in WRITE mode (not READ or APPEND). The label and timestamp
 * should both be allocated with malloc(), and the Device will free()
 * it on cleanup. The passed timestamp may be NULL, in which case it
 * will be filled in with the current time. */
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
					int fd);

/* Call this when you are finished writing a file. This function will
 * write a filemark or the local equivalent, flush the buffers, and do
 * whatever dirty work needs to be done at such a point in time. */
gboolean 	device_finish_file	(Device * self);

/* For reading only: Seeks to the beginning of a particular
 * filemark. You don't have to do this when writing; opening in
 * ACCESS_WRITE will start you out at the first file, and opening in
 * ACCESS_APPEND will automatically seek to the end of the medium.
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
					int fd);

/* Returns TRUE if the last device_read_block returned -1 because
 * there is no data left to read. */
gboolean        device_is_eof           (Device * self);

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
 * DEVICE_MAX_VOLUME_USAGE property based on the tapetype. The reading
 * parameter indicates whether or not read-mode is anticipated. */
void device_set_startup_properties_from_config(Device * device,
                                               gboolean reading);


#endif /* DEVICE_H */
