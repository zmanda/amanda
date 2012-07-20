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

%module "Amanda::Device"
%include "amglue/amglue.swg"
%include "exception.i"
%import "Amanda/Header.swg";

%include "Amanda/Device.pod"

%{
#include "device.h"
#include "property.h"
#include "fileheader.h"
#include "glib-util.h"
#include "simpleprng.h"
#include "amanda.h"
#include "sockaddr-util.h"
%}

%init %{
    /* Initialize the Device API on load */
    device_api_init();
%}

%{

/* Utility functions for typemaps, below */

/* return a new, mortal SV corresponding to the given GValue
 *
 * @param value: the value to convert
 * @returns: a new, mortal SV
 */
static SV *
set_sv_from_gvalue(GValue *value)
{
    GType fundamental = G_TYPE_FUNDAMENTAL(G_VALUE_TYPE(value));
    SV *sv = NULL;

    /* complex reference types */
    switch (fundamental) {
	case G_TYPE_LONG:
	    return sv_2mortal(amglue_newSVi64(g_value_get_long(value)));

	case G_TYPE_ULONG:
	    return sv_2mortal(amglue_newSVu64(g_value_get_ulong(value)));

	case G_TYPE_INT64:
	    return sv_2mortal(amglue_newSVi64(g_value_get_int64(value)));

	case G_TYPE_UINT64:
	    return sv_2mortal(amglue_newSVu64(g_value_get_uint64(value)));
    }

    /* simple types that can be constructed with sv_set*v */
    sv = sv_newmortal();
    switch (fundamental) {
	case G_TYPE_CHAR:
	    sv_setiv(sv, g_value_get_char(value));
	    break;

	case G_TYPE_UCHAR:
	    sv_setuv(sv, g_value_get_uchar(value));
	    break;

	case G_TYPE_BOOLEAN:
	    sv_setiv(sv, g_value_get_boolean(value));
	    break;

	case G_TYPE_INT:
	    sv_setiv(sv, g_value_get_int(value));
	    break;

	case G_TYPE_UINT:
	    sv_setuv(sv, g_value_get_uint(value));
	    break;

	case G_TYPE_FLOAT:
	    sv_setnv(sv, g_value_get_float(value));
	    break;

	case G_TYPE_DOUBLE:
	    sv_setnv(sv, g_value_get_double(value));
	    break;

	case G_TYPE_STRING:
	    sv_setpv(sv, g_value_get_string(value));
	    break;

	case G_TYPE_ENUM:
	    sv_setiv(sv, g_value_get_enum(value));
	    break;

	case G_TYPE_FLAGS:
	    sv_setiv(sv, g_value_get_flags(value));
	    break;

	/* Unsupported */
	default:
	case G_TYPE_POINTER:
	case G_TYPE_INTERFACE:
	case G_TYPE_OBJECT:
	case G_TYPE_PARAM:
	    warn("Unsupported fundamental property type #%d", (int)fundamental);
	    sv_setsv(sv, &PL_sv_undef);
	    break;
    }

    return sv;
}

/* Given an SV and an initialized GValue, set the GValue to the value
 * represented by the SV.  The GValue's type must already be set.
 *
 * For basic corresponding types (string -> string, integer -> integer),
 * the translation is straightforward.  However, if the GValue is not a
 * string, but the SV has a string value, then g_value_set_from_string will
 * be used to parse the string.
 *
 * @param sv: SV to convert
 * @param value: (input/output) destination
 * @returns: TRUE on success
 */
static gboolean
set_gvalue_from_sv(SV *sv, GValue *value)
{
    GType fundamental = G_TYPE_FUNDAMENTAL(G_VALUE_TYPE(value));

    /* if we got a string, use g_value_set_from_string to parse any funny
     * values or suffixes */
    if (SvPOK(sv)) {
	if (g_value_set_from_string(value, SvPV_nolen(sv)))
	    return TRUE;
    }

    /* otherwise, handle numeric types with SvIV, SvNV, or the amglue_* functions */
    switch (fundamental) {
	case G_TYPE_BOOLEAN:
	    g_value_set_boolean(value, SvIV(sv));
	    return TRUE;

	case G_TYPE_CHAR:
	    g_value_set_char(value, amglue_SvI8(sv));
	    return TRUE;

	case G_TYPE_UCHAR:
	    g_value_set_uchar(value, amglue_SvU8(sv));
	    return TRUE;

	case G_TYPE_INT:
	    g_value_set_int(value, amglue_SvI32(sv));
	    return TRUE;

	case G_TYPE_UINT:
	    g_value_set_uint(value, amglue_SvU32(sv));
	    return TRUE;

	case G_TYPE_LONG:
	    g_value_set_int64(value, amglue_SvI64(sv));
	    return TRUE;

	case G_TYPE_ULONG:
	    g_value_set_uint64(value, amglue_SvU64(sv));
	    return TRUE;

	case G_TYPE_INT64:
	    g_value_set_int64(value, amglue_SvI64(sv));
	    return TRUE;

	case G_TYPE_UINT64:
	    g_value_set_uint64(value, amglue_SvU64(sv));
	    return TRUE;

	case G_TYPE_FLOAT:
	    g_value_set_float(value, SvNV(sv));
	    return TRUE;

	case G_TYPE_DOUBLE:
	    g_value_set_double(value, SvNV(sv));
	    return TRUE;

	case G_TYPE_ENUM:
	    g_value_set_enum(value, SvIV(sv));
	    return TRUE;

	case G_TYPE_FLAGS:
	    g_value_set_flags(value, SvIV(sv));
	    return TRUE;

	default:
	    /* for anything else, let perl stringify it for us and try parsing it */
	    return g_value_set_from_string(value, SvPV_nolen(sv));
    }
}

%}

/*
 * DirectTCPConnection object
 */

typedef struct DirectTCPConnection {
    %extend {
	~DirectTCPConnection() {
	    g_object_unref(self);
	};

	%newobject close;
	char *close() {
	    return directtcp_connection_close(self);
	}
    };
} DirectTCPConnection;

/*
 * Device struct, %extend-ed into a Perl class
 */

%name(unaliased_name) extern char *device_unaliased_name(char *);

typedef struct Device {

    /* methods */
    %extend {
	/* constructor */
	Device(char *device_name) {
	    return device_open(device_name);
	}

	~Device() {
	    g_object_unref(self);
	}

	gboolean
	configure(gboolean use_global_config) {
	    return device_configure(self, use_global_config);
	}

	char *
	error() {
	    return device_error(self);
	}

	char *
	status_error() {
	    return device_status_error(self);
	}

	char *
	error_or_status() {
	    return device_error_or_status(self);
	}

	DeviceStatusFlags
	read_label() {
	    return device_read_label(self);
	}

	gboolean
	start(DeviceAccessMode mode, char *label, char *timestamp) {
	    return device_start(self, mode, label, timestamp);
	}

	gboolean
	finish() {
	    return device_finish(self);
	}

	guint64
	get_bytes_read() {
	    return device_get_bytes_read(self);
	}

	guint64
	get_bytes_written() {
	    return device_get_bytes_written(self);
	}

	gboolean
	start_file(dumpfile_t *jobInfo) {
	    return device_start_file(self, jobInfo);
	}

	gboolean
	write_block(guint size, gpointer data) {
	    return device_write_block(self, size, data);
	}

	gboolean
	finish_file() {
	    return device_finish_file(self);
	}

	dumpfile_t*
	seek_file(guint file) {
	    return device_seek_file(self, file);
	}

	gboolean
	seek_block(guint64 block) {
	    return device_seek_block(self, block);
	}

	int
	read_block(gpointer buffer, int *size) {
	    return device_read_block(self, buffer, size);
	}

	gboolean
	erase() {
	    return device_erase(self);
	}

	gboolean
	eject() {
	    return device_eject(self);
	}

	gboolean
	directtcp_supported() {
	    return device_directtcp_supported(self);
	}

	void
	listen(gboolean for_writing, DirectTCPAddr **addrs) {
	    /* ensure that the addresses are empty if there was an error */
	    if (!device_listen(self, for_writing, addrs))
		*addrs = NULL;
	}

	%newobject accept; /* connection is already ref'd, so we own it */
	DirectTCPConnection *
	accept() {
	    DirectTCPConnection *conn = NULL;
	    gboolean rv;

	    rv = device_accept(self, &conn, NULL, NULL);
	    if (!rv && conn) {
		/* conn is ref'd for our convenience, but we don't want it */
		g_object_unref(conn);
		conn = NULL;
	    }
	    return conn;
	}

	%newobject accept_with_cond; /* connection is already ref'd, so we own it */
	DirectTCPConnection *
	accept_with_cond(GMutex *abort_mutex, GCond *abort_cond) {
	    DirectTCPConnection *conn = NULL;
	    gboolean rv;

	    rv = device_accept_with_cond(self, &conn, abort_mutex, abort_cond);
	    if (!rv && conn) {
		/* conn is ref'd for our convenience, but we don't want it */
		g_object_unref(conn);
		conn = NULL;
	    }
	    return conn;
	}

	%newobject connect; /* connection is already ref'd, so we own it */
	DirectTCPConnection *
	connect(gboolean for_writing, DirectTCPAddr *addrs) {
	    DirectTCPConnection *conn = NULL;
	    gboolean rv;

	    rv = device_connect(self, for_writing, addrs, &conn, NULL, NULL);
	    if (!rv && conn) {
		/* conn is ref'd for our convenience, but we don't want it */
		g_object_unref(conn);
		conn = NULL;
	    }
	    return conn;
	}

	%newobject connect_with_cond; /* connection is already ref'd, so we own it */
	DirectTCPConnection *
	connect_with_cond(gboolean for_writing, DirectTCPAddr *addrs,
			  GMutex *abort_mutex, GCond *abort_cond) {
	    DirectTCPConnection *conn = NULL;
	    gboolean rv;

	    rv = device_connect_with_cond(self, for_writing, addrs, &conn,
					  abort_mutex, abort_cond);
	    if (!rv && conn) {
		/* conn is ref'd for our convenience, but we don't want it */
		g_object_unref(conn);
		conn = NULL;
	    }
	    return conn;
	}

	gboolean
	use_connection(DirectTCPConnection *conn) {
	    return device_use_connection(self, conn);
	}

	%typemap(in,numinputs=0) guint64 *actual_size (guint64 sz) {
	    sz = 0;
	    $1 = &sz;
	}
	%typemap(argout) guint64 *actual_size {
	    SP += argvi; PUTBACK;
	    $result = sv_2mortal(amglue_newSVu64(*$1));
	    SPAGAIN; SP -= argvi; argvi++;
	}
	gboolean
	write_from_connection(guint64 size, guint64 *actual_size) {
	    return device_write_from_connection(self, size, actual_size);
	}

	gboolean
	read_to_connection(guint64 size, guint64 *actual_size) {
	    return device_read_to_connection(self, size, actual_size);
	}

	%typemap(out) const GSList * {
	    GSList *iter;

	    /* Count the DeviceProperties */
	    EXTEND(SP, g_slist_length($1)); /* make room for return values */

	    /* Note that we set $result several times. the nature of
	     * SWIG's wrapping is such that incrementing argvi points
	     * $result to the next location in perl's argument stack.
             */

	    for (iter = $1; iter; iter = g_slist_next(iter)) {
		DeviceProperty *prop = iter->data;
		HV *hash = newHV();
		SV *rv = newRV_noinc((SV *)hash);

		hv_store(hash, "name", 4,
			newSVpv(prop->base->name, 0), 0);
		hv_store(hash, "description", 11,
			newSVpv(prop->base->description, 0), 0);
		hv_store(hash, "access", 6,
			newSViv(prop->access), 0);
		$result = sv_2mortal(rv);
		argvi++;
	    }
	}
	const GSList * property_list(void) {
	    return device_property_get_list(self);
	}

	%typemap(out) const GSList *; /* remove typemap */

	/* A typemap to convert a property name to a DevicePropertyBase. */
	%typemap(in) DevicePropertyBase * {
	    char *pname = NULL;

	    if (SvPOK($input))
		pname = SvPV_nolen($input);

	    if (pname)
		$1 = (DevicePropertyBase *)device_property_get_by_name(pname);
	    else
		$1 = NULL;
	}

	/* A typemap to convert the GValue in property_get to a return value.  The
	 * (in) typemap sets up storage for the parameters, while the (argout) converts
	 * them to a perl SV. */
	%typemap(in,numinputs=0) (GValue *out_val, PropertySurety *surety,
				  PropertySource *source, gboolean *val_found)
			    (GValue val,
			     PropertySurety surety,
			     PropertySource source,
			     gboolean found) {
	    memset(&val, 0, sizeof(val));
	    $1 = &val;
	    if (GIMME_V == G_ARRAY) {
		$2 = &surety;
		$3 = &source;
	    }
	    $4 = &found;
	}
	%typemap(argout) (GValue *out_val, PropertySurety *surety,
			  PropertySource *source, gboolean *val_found) {
	    /* if the result is valid */
	    if (*$4) {
		/* move data from $1 to $result, somehow, being careful to
		 * save the perl stack while doing so */
		SP += argvi; PUTBACK;
		$result = set_sv_from_gvalue($1);
		SPAGAIN; SP -= argvi; argvi++;

		/* free any memory for the GValue */
		g_value_unset($1);

		if (GIMME_V == G_ARRAY) {
		    $result = newSViv(*$2);
		    argvi++;
		    $result = newSViv(*$3);
		    argvi++;
		}
	    }
	    /* otherwise, return nothing */
	}

	void
	property_get(DevicePropertyBase *pbase, GValue *out_val, PropertySurety *surety,
		     PropertySource *source, gboolean *val_found) {
	    if (pbase) {
		*val_found = device_property_get_ex(self, pbase->ID, out_val, surety, source);
	    } else {
		*val_found = FALSE;
	    }
	}

	/* delete typemaps */
	%typemap(in) (GValue *out_val, gboolean *val_found);
	%typemap(argout) (GValue *out_val, gboolean *val_found);

	/* We cheat a little bit here and just pass the native Perl type in to
	 * the function.  This is the easiest way to make sure we know the property
	 * information (in particular, its type) before trying to convert the SV.  */
	%typemap(in) SV *sv "$1 = $input;"

	gboolean
	property_set(DevicePropertyBase *pbase, SV *sv) {
	    GValue gval;

	    if (!pbase)
		goto fail;
	    memset(&gval, 0, sizeof(gval));
	    g_value_init(&gval, pbase->type);
	    if (!set_gvalue_from_sv(sv, &gval))
		goto failunset;

	    if (!device_property_set(self, pbase->ID, &gval))
		goto failunset;

	    g_value_unset(&gval);
	    return TRUE;
	failunset:
	    g_value_unset(&gval);
	fail:
	    return FALSE;
	}

	gboolean
	property_set_ex(DevicePropertyBase *pbase, SV *sv,
			PropertySurety surety, PropertySource source) {
	    GValue gval;
	    memset(&gval, 0, sizeof(gval));
	    g_value_init(&gval, pbase->type);
	    if (!set_gvalue_from_sv(sv, &gval))
		goto fail;

	    if (!device_property_set_ex(self, pbase->ID, &gval, surety, source))
		goto fail;

	    g_value_unset(&gval);
	    return TRUE;
	fail:
	    g_value_unset(&gval);
	    return FALSE;
	}

	gboolean recycle_file(guint filenum) {
	    return device_recycle_file(self, filenum);
	}

	/* accessor functions */

	int file(void) { return self->file; }
	guint64 block(void) { return self->block; }
	gboolean in_file(void) { return self->in_file; }
	char * device_name(void) { return self->device_name; }
	DeviceAccessMode access_mode(void) { return self->access_mode; }
	gboolean is_eof(void) { return self->is_eof; }
	gboolean is_eom(void) { return self->is_eom; }
	char * volume_label(void) { return self->volume_label; }
	char * volume_time(void) { return self->volume_time; }
	DeviceStatusFlags status(void) { return self->status; }
	gsize min_block_size(void) { return self->min_block_size; }
	gsize max_block_size(void) { return self->max_block_size; }
	gsize block_size(void) { return self->block_size; }
	gsize header_block_size(void) { return self->header_block_size; }
	dumpfile_t *volume_header(void) { return self->volume_header; }
    };

} Device;

/* An alternate constructor for RAIT devices */
%typemap(in) GSList *child_devices {
    AV *av;
    int i, len;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "Expected an arrayref");
    }
    av = (AV *)SvRV($input);

    $1 = NULL;
    len = av_len(av);
    for (i = 0; i <= len; i++) {
	SV **elt = av_fetch(av, i, 0);
	Device *d;

	if (elt && !SvOK(*elt)) {
	    $1 = g_slist_append($1, NULL); /* 'undef' => NULL */
	} else if (!elt || SWIG_ConvertPtr(*elt, (void **)&d, $descriptor(Device *), 0) == -1) {
	    SWIG_exception(SWIG_TypeError, "array member is not a Device");
	} else {
	    $1 = g_slist_append($1, d);
	}
    }
}
%typemap(freearg) GSList *child_devices {
    g_slist_free($1);
}
%newobject rait_device_open_from_children;
Device *rait_device_open_from_children(GSList *child_devices);
%perlcode %{
sub new_rait_from_children {
    my $class = shift; # strip the $class from the arguments
    return rait_device_open_from_children([@_]);
}
%}

/*
 * Utilities for installchecks (not described in POD)
 */

%inline %{

/* write LENGTH bytes of random data to FILENAME, seeded with SEED */
gboolean
write_random_to_device(guint32 seed, size_t length, Device *device) {
    simpleprng_state_t prng;
    char *buf;
    gsize block_size = device->block_size;
    g_assert(block_size < G_MAXUINT);

    buf = g_malloc(block_size);
    simpleprng_seed(&prng, seed);

    while (length) {
	size_t to_write = min(block_size, length);

	simpleprng_fill_buffer(&prng, buf, to_write);
	if (!device_write_block(device, (guint)block_size, buf)) {
	    g_free(buf);
	    return FALSE;
	}
	length -= to_write;
    }

    g_free(buf);
    return TRUE;
}

/* read LENGTH bytes of random data from FILENAME verifying it against
 * a PRNG seeded with SEED.  Sends any error messages to stderr.
 */
gboolean
verify_random_from_device(guint32 seed, size_t length, Device *device) {
    simpleprng_state_t prng;
    char *buf = NULL; /* first device_read_block will get the size */
    int block_size = 0;

    simpleprng_seed(&prng, seed);

    while (length) {
	int bytes_read;
	int size = block_size;

	bytes_read = device_read_block(device, buf, &size);
	if (bytes_read == 0 && size > block_size) {
	    g_free(buf);
	    block_size = size;
	    buf = g_malloc(block_size);
	    continue;
	}
	if (bytes_read == -1) {
	    if (device->status == DEVICE_STATUS_SUCCESS) {
		g_assert(device->is_eof);
		g_debug("verify_random_from_device got unexpected EOF");
	    }
	    goto error;
	}

	/* strip padding */
	bytes_read = min(bytes_read, length);

	if (!simpleprng_verify_buffer(&prng, buf, bytes_read))
	    goto error;

	length -= bytes_read;
    }

    g_free(buf);
    return TRUE;

error:
    g_free(buf);
    return FALSE;
}
%}

/*
 * Constants
 */

amglue_add_flag_tag_fns(DeviceAccessMode);
amglue_add_constant_short(ACCESS_NULL, "NULL", DeviceAccessMode);
amglue_add_constant_short(ACCESS_READ, "READ", DeviceAccessMode);
amglue_add_constant_short(ACCESS_WRITE, "WRITE", DeviceAccessMode);
amglue_add_constant_short(ACCESS_APPEND, "APPEND", DeviceAccessMode);

/* (this is really a macro, but SWIG will Do The Right Thing */
gboolean IS_WRITABLE_ACCESS_MODE(DeviceAccessMode mode);
amglue_export_tag(DeviceAccessMode, IS_WRITABLE_ACCESS_MODE);
amglue_copy_to_tag(DeviceAccessMode, constants);

amglue_add_flag_tag_fns(DeviceStatusFlags);
amglue_add_constant_short(DEVICE_STATUS_SUCCESS, "SUCCESS", DeviceStatusFlags);
amglue_add_constant_short(DEVICE_STATUS_DEVICE_ERROR, "DEVICE_ERROR", DeviceStatusFlags);
amglue_add_constant_short(DEVICE_STATUS_DEVICE_BUSY, "DEVICE_BUSY", DeviceStatusFlags);
amglue_add_constant_short(DEVICE_STATUS_VOLUME_MISSING, "VOLUME_MISSING", DeviceStatusFlags);
amglue_add_constant_short(DEVICE_STATUS_VOLUME_UNLABELED, "VOLUME_UNLABELED", DeviceStatusFlags);
amglue_add_constant_short(DEVICE_STATUS_VOLUME_ERROR, "VOLUME_ERROR", DeviceStatusFlags);
amglue_add_constant_noshort(DEVICE_STATUS_FLAGS_MAX, DeviceStatusFlags);
amglue_copy_to_tag(DeviceStatusFlags, constants);

amglue_add_flag_tag_fns(PropertyPhaseFlags);
amglue_add_constant_short(PROPERTY_PHASE_BEFORE_START, "BEFORE_START", PropertyPhaseFlags);
amglue_add_constant_short(PROPERTY_PHASE_BETWEEN_FILE_WRITE, "BETWEEN_FILE_WRITE", PropertyPhaseFlags);
amglue_add_constant_short(PROPERTY_PHASE_INSIDE_FILE_WRITE, "INSIDE_FILE_WRITE", PropertyPhaseFlags);
amglue_add_constant_short(PROPERTY_PHASE_BETWEEN_FILE_READ, "BETWEEN_FILE_READ", PropertyPhaseFlags);
amglue_add_constant_short(PROPERTY_PHASE_INSIDE_FILE_READ, "INSIDE_FILE_READ", PropertyPhaseFlags);
amglue_add_constant_noshort(PROPERTY_PHASE_MAX, PropertyPhaseFlags);
amglue_add_constant_noshort(PROPERTY_PHASE_MASK, PropertyPhaseFlags);
amglue_add_constant_noshort(PROPERTY_PHASE_SHIFT, PropertyPhaseFlags);
amglue_copy_to_tag(PropertyPhaseFlags, constants);

amglue_add_flag_tag_fns(PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_GET_BEFORE_START,
		    "GET_BEFORE_START", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_GET_BETWEEN_FILE_WRITE,
		    "GET_BETWEEN_FILE_WRITE", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_GET_INSIDE_FILE_WRITE,
		    "GET_INSIDE_FILE_WRITE", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_GET_BETWEEN_FILE_READ,
		    "GET_BETWEEN_FILE_READ", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_GET_INSIDE_FILE_READ,
		    "GET_INSIDE_FILE_READ", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_SET_BEFORE_START,
		    "SET_BEFORE_START", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_SET_BETWEEN_FILE_WRITE,
		    "SET_BETWEEN_FILE_WRITE", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_SET_INSIDE_FILE_WRITE,
		    "SET_INSIDE_FILE_WRITE", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_SET_BETWEEN_FILE_READ,
		    "SET_BETWEEN_FILE_READ", PropertyAccessFlags);
amglue_add_constant_short(PROPERTY_ACCESS_SET_INSIDE_FILE_READ,
		    "SET_INSIDE_FILE_READ", PropertyAccessFlags);
amglue_add_constant_noshort(PROPERTY_ACCESS_GET_MASK, PropertyAccessFlags);
amglue_add_constant_noshort(PROPERTY_ACCESS_SET_MASK, PropertyAccessFlags);
amglue_copy_to_tag(PropertyAccessFlags, constants);

amglue_add_enum_tag_fns(ConcurrencyParadigm);
amglue_add_constant_short(CONCURRENCY_PARADIGM_EXCLUSIVE, "EXCLUSIVE", ConcurrencyParadigm);
amglue_add_constant_short(CONCURRENCY_PARADIGM_SHARED_READ, "SHARED_READ", ConcurrencyParadigm);
amglue_add_constant_short(CONCURRENCY_PARADIGM_RANDOM_ACCESS, "RANDOM_ACCESS", ConcurrencyParadigm);
amglue_copy_to_tag(ConcurrencyParadigm, constants);

amglue_add_enum_tag_fns(StreamingRequirement);
amglue_add_constant_short(STREAMING_REQUIREMENT_NONE, "NONE", StreamingRequirement);
amglue_add_constant_short(STREAMING_REQUIREMENT_DESIRED, "DESIRED", StreamingRequirement);
amglue_add_constant_short(STREAMING_REQUIREMENT_REQUIRED, "REQUIRED", StreamingRequirement);
amglue_copy_to_tag(StreamingRequirement, constants);

amglue_add_enum_tag_fns(MediaAccessMode);
amglue_add_constant_short(MEDIA_ACCESS_MODE_READ_ONLY, "READ_ONLY", MediaAccessMode);
amglue_add_constant_short(MEDIA_ACCESS_MODE_WORM, "WORM", MediaAccessMode);
amglue_add_constant_short(MEDIA_ACCESS_MODE_READ_WRITE, "READ_WRITE", MediaAccessMode);
amglue_add_constant_short(MEDIA_ACCESS_MODE_WRITE_ONLY, "WRITE_ONLY", MediaAccessMode);
amglue_copy_to_tag(MediaAccessMode, constants);

amglue_add_flag_tag_fns(PropertySurety);
amglue_add_constant_short(PROPERTY_SURETY_BAD, "SURETY_BAD", PropertySurety);
amglue_add_constant_short(PROPERTY_SURETY_GOOD, "SURETY_GOOD", PropertySurety);
amglue_copy_to_tag(PropertySurety, constants);

amglue_add_flag_tag_fns(PropertySource);
amglue_add_constant_short(PROPERTY_SOURCE_DEFAULT, "SOURCE_DEFAULT", PropertySource);
amglue_add_constant_short(PROPERTY_SOURCE_DETECTED, "SOURCE_DETECTED", PropertySource);
amglue_add_constant_short(PROPERTY_SOURCE_USER, "SOURCE_USER", PropertySource);
amglue_copy_to_tag(PropertySource, constants);

%perlcode %{

# SWIG produces a sub-package for the Device "class", in this case named
# Amanda::Device::Device.  For user convenience, we allow Amanda::Device->new(..) to
# do the same thing.  This is a wrapper function, and not just a typeglob assignment,
# because we want to get the right blessing.
sub new {
    my $pkg = shift;
    Amanda::Device::Device->new(@_);
}
%}
