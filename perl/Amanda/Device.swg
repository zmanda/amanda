/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
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

%module "Amanda::Device"
%include "amglue/amglue.swg"
%include "exception.i"

%{
#include "device.h"
#include "property.h"
#include "fileheader.h"
%}

/* import dumptype_t, among others */
%import "Amanda/Types.swg";

%perlcode %{
=head1 NAME

Amanda::Device - interact with Amanda data-storage devices

=head1 SYNOPSIS

  use Amanda::Device qw( :constants );

  my $dev = Amanda::Device->new($device_name);
  $dev->set_startup_properties_from_config();
  if ($dev->read_label() == $READ_LABEL_STATUS_SUCCESS) {
      print "Label on $device_name is '$dev->volume_label'\n";
  }
  
See L<http://wiki.zmanda.com/index.php/Device_API> for details on how Devices are used.

=head1 API STATUS

Stable

=head1 Amanda::Device Objects

=head2 Instance Variables

=over

=item C<$file>

=item C<$block>

=item C<$in_file>

=item C<$device_name>

=item C<$access_mode>

=item C<$is_eof>

=item C<$volume_label>

=item C<$volume_time>

=back

=head2 Methods

See the wiki for descriptions of these functions

=over

=item C<read_label()>

=item C<start($mode, $label, $timestamp)>

=item C<finish()>

=item C<start_file($jobinfo)>

where C<$jobinfo> is a C<dumpfile_t> (see L<Amanda::Datatypes)

=item C<write_min_size()>

=item C<write_max_size()>

=item C<read_max_size()>

=item C<write_block($size, $data, $short_block)>

Note that Perl code is not expected to handle on-device data, so there
is currently no way to provide data to this function from Perl.  This may
change in future revisions.

=item C<write_from_fd($fd)>

where C<$fd> is an integer file descriptor, not a filehandle

=item C<finish_file()>

=item C<seek_file($file)>

=item C<seek_block($block)>

=item C<read_block($size)>

=item C<read_to_fd($fd)>

where C<$fd> is an integer file descriptor, not a filehandle

Note that Perl code is not expected to handle on-device data, so there
is currently no way to access the data this function returns.  This may
change in future revisions.

=item C<property_list()>

returns a list of property names.

=item C<property_get($property_name)>

returns the property as the appropriate Perl type.

=item C<property_set($property_name, $value)>

where $value is of an appropriate type for the given property

=item C<recycle_file($filenum)>

=item C<set_startup_properties_from_config()>

=back

=head1 CONSTANTS

This module defines a large number of constants.  Again, consult the
wiki or C<device.h> for the details on their meaning.  These constants
are available from the package namespace (e.g.,
C<Amanda::Device::ACCESS_WRITE>), of imported with the C<:constant>
import tag.

=cut
%}

%init %{
    /* Initialize the Device API on load */
    device_api_init();
%}

%{

/* Utility functions for typemaps, below */

static SV *
set_sv_from_gvalue(GValue *value)
{
    GType fundamental = G_TYPE_FUNDAMENTAL(G_VALUE_TYPE(value));
    SV *sv = NULL;

    /* complex reference types */
    switch (fundamental) {
	case G_TYPE_LONG:
	    sv = sv_2mortal(amglue_newSVi64(g_value_get_long(value)));
	    break;

	case G_TYPE_ULONG:
	    sv = sv_2mortal(amglue_newSVu64(g_value_get_ulong(value)));
	    break;

	case G_TYPE_INT64:
	    sv = sv_2mortal(amglue_newSVi64(g_value_get_int64(value)));
	    break;

	case G_TYPE_UINT64:
	    sv = sv_2mortal(amglue_newSVu64(g_value_get_uint64(value)));
	    break;

	case G_TYPE_BOXED: {
	    GType boxed_type = G_VALUE_TYPE(value);
	    QualifiedSize qs;
	    HV *hv;

	    if (boxed_type == QUALIFIED_SIZE_TYPE) {
		qs = *(QualifiedSize*)(g_value_get_boxed(value));
		
		/* build a hash */
		hv = (HV *)sv_2mortal((SV *)newHV());
		hv_store(hv, "accuracy", 8, newSViv(qs.accuracy), 0);
		hv_store(hv, "bytes", 5, amglue_newSVi64(qs.bytes), 0);

		sv = newRV((SV *)hv);
		return newRV((SV *)hv);
	    } else {
		warn("Unsupported boxed property type #%d", boxed_type);

		sv = sv_newmortal();
		sv_setsv(sv, &PL_sv_undef);
		return sv;
	    }
	}
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
	    warn("Unsupported fundamental property type #%d", fundamental);
	    sv_setsv(sv, &PL_sv_undef);
	    break;
    }

    return sv;
}

static gboolean
set_gvalue_from_sv(SV *sv, GValue *value)
{
    GType fundamental = G_TYPE_FUNDAMENTAL(G_VALUE_TYPE(value));
    switch (fundamental) {
	case G_TYPE_CHAR:
	    if (!SvIOK(sv)) return FALSE;
	    g_value_set_char(value, SvIV(sv));
	    break;

	case G_TYPE_UCHAR:
	    if (!SvIOK(sv)) return FALSE;
	    g_value_set_uchar(value, SvUV(sv));
	    break;

	case G_TYPE_BOOLEAN:
	    if (!SvIOK(sv)) return FALSE;
	    g_value_set_boolean(value, SvIV(sv));
	    break;

	case G_TYPE_INT:
	    g_value_set_int(value, amglue_SvI32(sv));
	    break;

	case G_TYPE_UINT:
	    g_value_set_uint(value, amglue_SvU32(sv));
	    break;

	case G_TYPE_LONG:
	    g_value_set_int64(value, amglue_SvI64(sv));
	    break;

	case G_TYPE_ULONG:
	    g_value_set_uint64(value, amglue_SvU64(sv));
	    break;

	case G_TYPE_INT64:
	    g_value_set_int64(value, amglue_SvI64(sv));
	    break;

	case G_TYPE_UINT64:
	    g_value_set_uint64(value, amglue_SvU64(sv));
	    break;

	case G_TYPE_FLOAT:
	    if (!SvNOK(sv)) return FALSE;
	    g_value_set_float(value, SvNV(sv));
	    break;

	case G_TYPE_DOUBLE:
	    if (!SvNOK(sv)) return FALSE;
	    g_value_set_double(value, SvNV(sv));
	    break;

	case G_TYPE_STRING:
	    if (!SvPOK(sv)) return FALSE;
	    g_value_set_string(value, SvPV_nolen(sv));
	    break;

	case G_TYPE_ENUM: 
	    if (!SvIOK(sv)) return FALSE;
	    g_value_set_enum(value, SvIV(sv));
	    break;

	case G_TYPE_FLAGS:
	    if (!SvIOK(sv)) return FALSE;
	    g_value_set_flags(value, SvIV(sv));
	    break;

	/* Unsupported */
	default:
	case G_TYPE_POINTER:
	case G_TYPE_INTERFACE:
	case G_TYPE_BOXED: /* note: *getting* boxed values is supported */
	case G_TYPE_OBJECT:
	case G_TYPE_PARAM:
	    return FALSE;
    }

    return TRUE;
}

%}

/*
 * Device struct, %extend-ed into a Perl class
 */

typedef struct Device {
    /* Instance variables -- all readonly */
    %immutable;
    int file;
    guint64 block;
    gboolean in_file;
    char * device_name;
    DeviceAccessMode access_mode;
    gboolean is_eof;
    char * volume_label;
    char * volume_time;
    %mutable;

    /* methods */
    %extend {
	/* constructor */
	Device(char *device_name) {
	    return device_open(device_name);
	}

	~Device() {
	    g_object_unref(self);
	}

	ReadLabelStatusFlags
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

	gboolean
	start_file(const dumpfile_t *jobInfo) {
	    return device_start_file(self, jobInfo);
	}

	guint
	write_min_size() {
	    return device_write_min_size(self);
	}

	guint
	write_max_size() {
	    return device_write_max_size(self);
	}

	guint
	read_max_size() {
	    return device_read_max_size(self);
	}

	gboolean
	write_block(guint size, gpointer data, gboolean short_block) {
	    return device_write_block(self, size, data, short_block);
	}

	gboolean
	write_from_fd(int fd) {
	    return device_write_from_fd(self, fd);
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

	gboolean read_to_fd(int fd) {
	    return device_read_to_fd(self, fd);
	}

	%typemap(out) const DeviceProperty * {
	    int i = 0;
	    int len = 0;

	    /* Count the DeviceProperties */
	    while ($1[len].base) len++;
	    EXTEND(SP, len); /* make room for return values */

	    /* Note that we set $result several times. the nature of
	     * SWIG's wrapping is such that incrementing argvi points
	     * $result to the next location in perl's argument stack.
             */

	    for (i = 0; i < len ; i++) { 
		$result = sv_newmortal(); 
		sv_setpv($result, $1[i].base->name); 
		argvi++;
	    }; 
	}
	const DeviceProperty * property_list(void) {
	    return device_property_get_list(self);
	}

	%typemap(out) const DeviceProperty *; /* remove typemap */

	/* A typemap to convert a property name to a DevicePropertyBase. */
	%typemap(in) DevicePropertyBase * {
	    char *pname = NULL;

	    if (SvPOK($input))
		pname = SvPV_nolen($input);

	    if (pname) $1 = (DevicePropertyBase *)device_property_get_by_name(pname);
	    if (!pname || !$1) {
		SWIG_exception_fail(SWIG_ValueError, "Invalid property name");
	    }
	}

	/* A typemap to convert the GValue in property_get to a return value.  The
	 * (in) typemap sets up storage for the parameters, while the (argout) converts
	 * them to a perl SV. */
	%typemap(in,numinputs=0) (GValue *out_val, gboolean *val_found)
			    (GValue val, gboolean found) {
	    memset(&val, 0, sizeof(val));
	    $1 = &val;
	    $2 = &found;
	}
	%typemap(argout) (GValue *out_val, gboolean *val_found) {
	    /* if the result is valid */
	    if (*$2) {
		/* move data from $1 to $result, somehow */
		$result = set_sv_from_gvalue($1);

		/* free any memory for the GValue */
		g_value_unset($1);
	    } else {
		/* silently return 'undef', the sentinel for "undefined" */
		$result = sv_newmortal();
		sv_setsv($result, &PL_sv_undef);
	    }
	    argvi++;
	}

	void
	property_get(DevicePropertyBase *pbase, GValue *out_val, gboolean *val_found) {
	    *val_found = device_property_get(self, pbase->ID, out_val);
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
	    memset(&gval, 0, sizeof(gval));
	    g_value_init(&gval, pbase->type);
	    if (!set_gvalue_from_sv(sv, &gval))
		goto fail;

	    if (!device_property_set(self, pbase->ID, &gval))
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
	
	void set_startup_properties_from_config(void) {
	    device_set_startup_properties_from_config(self);
	}
    };
} Device;

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

amglue_add_flag_tag_fns(ReadLabelStatusFlags);
amglue_add_constant_short(READ_LABEL_STATUS_SUCCESS, "SUCCESS", ReadLabelStatusFlags);
amglue_add_constant_short(READ_LABEL_STATUS_DEVICE_MISSING, "DEVICE_MISSING", ReadLabelStatusFlags);
amglue_add_constant_short(READ_LABEL_STATUS_DEVICE_ERROR, "DEVICE_ERROR", ReadLabelStatusFlags);
amglue_add_constant_short(READ_LABEL_STATUS_VOLUME_MISSING, "VOLUME_MISSING", ReadLabelStatusFlags);
amglue_add_constant_short(READ_LABEL_STATUS_VOLUME_UNLABELED, "VOLUME_UNLABELED", ReadLabelStatusFlags);
amglue_add_constant_short(READ_LABEL_STATUS_VOLUME_ERROR, "VOLUME_ERROR", ReadLabelStatusFlags);
amglue_add_constant_noshort(READ_LABEL_STATUS_FLAGS_MAX, ReadLabelStatusFlags);
amglue_copy_to_tag(ReadLabelStatusFlags, constants);

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

amglue_add_enum_tag_fns(SizeAccuracy);
amglue_add_constant_short(SIZE_ACCURACY_UNKNOWN, "UNKNOWN", SizeAccuracy);
amglue_add_constant_short(SIZE_ACCURACY_ESTIMATE, "ESTIMATE", SizeAccuracy);
amglue_add_constant_short(SIZE_ACCURACY_REAL, "REAL", SizeAccuracy);
amglue_copy_to_tag(SizeAccuracy, constants);

amglue_add_flag_tag_fns(FeatureSupportFlags);
amglue_add_constant_short(FEATURE_STATUS_ENABLED, "STATUS_ENABLED", FeatureSupportFlags);
amglue_add_constant_short(FEATURE_STATUS_DISABLED, "STATUS_DISABLED", FeatureSupportFlags);
amglue_add_constant_short(FEATURE_SURETY_BAD, "SURETY_BAD", FeatureSupportFlags);
amglue_add_constant_short(FEATURE_SURETY_GOOD, "SURETY_GOOD", FeatureSupportFlags);
amglue_add_constant_short(FEATURE_SOURCE_DEFAULT, "SOURCE_DEFAULT", FeatureSupportFlags);
amglue_add_constant_short(FEATURE_SOURCE_DETECTED, "SOURCE_DETECTED", FeatureSupportFlags);
amglue_add_constant_short(FEATURE_SOURCE_USER, "SOURCE_USER", FeatureSupportFlags);
amglue_add_constant_noshort(FEATURE_SUPPORT_FLAGS_MAX, FeatureSupportFlags);
amglue_add_constant_noshort(FEATURE_SUPPORT_FLAGS_MASK, FeatureSupportFlags);
amglue_add_constant_noshort(FEATURE_SUPPORT_FLAGS_STATUS_MASK, FeatureSupportFlags);
amglue_add_constant_noshort(FEATURE_SUPPORT_FLAGS_SURETY_MASK, FeatureSupportFlags);
amglue_add_constant_noshort(FEATURE_SUPPORT_FLAGS_SOURCE_MASK, FeatureSupportFlags);

gboolean feature_support_flags_is_valid(FeatureSupportFlags);
amglue_export_tag(FeatureSupportFlags, feature_support_flags_is_valid);
amglue_copy_to_tag(FeatureSupportFlags, constants);

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
