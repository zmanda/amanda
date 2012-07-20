/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::Archive"
%include "amglue/amglue.swg"
%include "exception.i"
%include "cstring.i"

%include "Amanda/Archive.pod"

%{
#include "amar.h"
%}

%{
/* Support code (not directly available from perl) */

#define AMANDA_ARCHIVE_ERROR_DOMAIN "Amanda archive"

/* A C object to contain all of the relevant callbacks and other state during a
 * read operation; this becomes the user_data during the read */
typedef struct perl_read_data_s {
    SV *user_data;
    SV *file_start_sub;
    SV *file_finish_sub;

    amar_attr_handling_t *handling_array;
} perl_read_data_t;

static gboolean
read_start_file_cb(
	gpointer user_data,
	uint16_t filenum,
	gpointer filename,
	gsize filename_len,
	gboolean *ignore,
	gpointer *file_data)
{
    dSP;
    perl_read_data_t *dat = user_data;
    SV *rv = NULL;
    STRLEN len;
    int count;

    *file_data = NULL;

    g_assert(dat->file_start_sub != NULL);

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    XPUSHs(dat->user_data);
    XPUSHs(sv_2mortal(newSViv(filenum)));
    XPUSHs(sv_2mortal(newSVpvn(filename, filename_len)));
    PUTBACK;

    count = call_sv(dat->file_start_sub, G_EVAL|G_SCALAR);

    SPAGAIN;

    if (count != 1)
	croak("file_start_sub returned nothing");

    rv = POPs;

    /* if it's the string "IGNORE", then ignore it */
    if (SvPOK(rv)) {
	static const char *ign = "IGNORE";
	char *rvstr = SvPV(rv, len);
	if (strlen(ign) == len && 0 == strncmp(ign, rvstr, len))
	    *ignore = TRUE;
    }

    /* otherwise, keep the value */
    if (!*ignore)
	*(SV **)(file_data) = SvREFCNT_inc(rv);

    PUTBACK;
    FREETMPS;
    LEAVE;

    if (SvTRUE(ERRSV))
	return FALSE;
    return TRUE;
}

static gboolean
read_finish_file_cb(
	gpointer user_data,
	uint16_t filenum,
	gpointer *file_data,
	gboolean truncated)
{
    dSP;
    perl_read_data_t *dat = user_data;

    g_assert(dat->file_finish_sub != NULL);

    ENTER;
    SAVETMPS;

    PUSHMARK(SP); XPUSHs(dat->user_data); XPUSHs(*(SV **)file_data);
    XPUSHs(sv_2mortal(newSViv(filenum)));
    XPUSHs(sv_2mortal(newSViv(truncated))); PUTBACK;

    call_sv(dat->file_finish_sub, G_EVAL|G_DISCARD);

    /* we're done with this file's file_data */
    SvREFCNT_dec(*(SV **)file_data);

    FREETMPS;
    LEAVE;

    if (SvTRUE(ERRSV))
	return FALSE;
    return TRUE;
}

static gboolean
read_frag_cb(
	gpointer user_data,
	uint16_t filenum,
	gpointer file_data,
	uint16_t attrid,
	gpointer attrid_data,
	gpointer *attr_data,
	gpointer data,
	gsize size,
	gboolean eoa,
	gboolean truncated)
{
    dSP;
    perl_read_data_t *dat = user_data;
    SV *rv;
    int count;

    if (!attrid_data)
	return TRUE;

    ENTER;
    SAVETMPS;

    PUSHMARK(SP);
    XPUSHs(dat->user_data);
    XPUSHs(sv_2mortal(newSViv(filenum)));
    XPUSHs((SV *)file_data);
    XPUSHs(sv_2mortal(newSViv(attrid)));
    if (*attr_data)
        XPUSHs((SV *)(*attr_data));
    else
        XPUSHs(&PL_sv_undef);
    XPUSHs(sv_2mortal(newSVpvn(data, size)));
    XPUSHs(sv_2mortal(newSViv(eoa)));
    XPUSHs(sv_2mortal(newSViv(truncated)));
    PUTBACK;

    count = call_sv(attrid_data, G_EVAL|G_SCALAR);

    SPAGAIN;

    if (count != 1)
	croak("fragment callback returned nothing");

    rv = POPs;

    if (eoa) {
        SvREFCNT_dec(*attr_data);
    } else {
        /* increment before decrement here, in case they're the same object */
        SvREFCNT_inc(rv);
        SvREFCNT_dec(*attr_data);
        *attr_data = rv;
    }

    FREETMPS;
    LEAVE;

    if (SvTRUE(ERRSV))
	return FALSE;
    return TRUE;
}

/* generic function to recognize when a string+len represents a number and
 * incidentally return the resulting value.  Note that this does not handle
 * negative numbers. */
static gboolean
is_number(char *str, int len, int *result)
{
    char *end = str+len;
    int r = 0;

    while (str < end) {
	if (!g_ascii_isdigit(*str)) return FALSE;
	r = r * 10 + (int)(*str - '0');
	if (r < 0) {
	    /* overflow */
	    return FALSE;
	}
	str++;
    }

    *result = r;
    return TRUE;
}

%}

/* Rename all of the below wrapper functions (suffixed with '_') for
 * consumption by perl */
%rename(amar_new) amar_new_;
%rename(amar_close) amar_close_;
%rename(amar_new_file) amar_new_file_;
%rename(amar_file_close) amar_file_close_;
%rename(amar_new_attr) amar_new_attr_;
%rename(amar_attr_close) amar_attr_close_;
%rename(amar_attr_add_data_buffer) amar_attr_add_data_buffer_;
%rename(amar_attr_add_data_fd) amar_attr_add_data_fd_;
%rename(amar_read) amar_read_;

/* typemaps for the below */
%apply (char *STRING, int LENGTH) { (char *filename, gsize filename_len) };
%apply (char *STRING, int LENGTH) { (char *buffer, gsize size) };
%typemap(in) SV * "$1 = $input;"

%typemap(in) off_t *want_position (off_t position) {
    if (SvTRUE($input)) {
	position = 0;
	$1 = &position;
    } else {
	$1 = NULL;
    }
}
%typemap(argout) off_t *want_position {
    if ($1) {
	SP += argvi; PUTBACK;
	$result = sv_2mortal(amglue_newSVi64(*$1));
	SPAGAIN; SP -= argvi; argvi++;
    }
}

%inline %{

/* Wrapper functions, mostly dealing with error handling */

amar_t *amar_new_(int fd, char *modestr) {
    GError *error = NULL;
    amar_t *rv;
    int mode;

    if (strcmp(modestr, ">") == 0)
	mode = O_WRONLY;
    else if (strcmp(modestr, "<") == 0)
	mode = O_RDONLY;
    else
	croak("mode must be '<' or '>'");

    if ((rv = amar_new(fd, mode, &error))) {
	return rv;
    }

    croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
    return NULL;
}

void amar_close_(amar_t *arch) {
    GError *error = NULL;
    if (!amar_close(arch, &error))
	croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
}

amar_file_t *
amar_new_file_(amar_t *arch, char *filename, gsize filename_len, off_t *want_position) {
    GError *error = NULL;
    amar_file_t *file;
    g_assert(arch != NULL);

    file = amar_new_file(arch, filename, filename_len, want_position, &error);
    if (file)
	return file;

    croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
    return NULL;
}

void amar_file_close_(amar_file_t *file) {
    GError *error = NULL;
    if (!amar_file_close(file, &error))
	croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
}

amar_attr_t *
amar_new_attr_(amar_file_t *file, guint16 attrid) {
    GError *error = NULL;
    amar_attr_t *attr;

    g_assert(file != NULL);

    attr = amar_new_attr(file, attrid, &error);
    if (attr)
	return attr;

    croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
    return NULL;
}

void amar_attr_close_(amar_attr_t *attr) {
    GError *error = NULL;
    if (!amar_attr_close(attr, &error))
	croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
}

void amar_attr_add_data_buffer_(amar_attr_t *attr, char *buffer, gsize size, gboolean eoa) {
    GError *error = NULL;
    if (!amar_attr_add_data_buffer(attr, buffer, size, eoa, &error))
	croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
}

size_t
amar_attr_add_data_fd_(amar_attr_t *attr, int fd, gboolean eoa) {
    GError *error = NULL;
    size_t rv = amar_attr_add_data_fd(attr, fd, eoa, &error);
    if (rv < 0)
	croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
    return rv;
}

/* reading */

void amar_read_(amar_t *archive, SV *params_hashref) {
    perl_read_data_t *dat = g_new0(perl_read_data_t, 1);
    GError *error = NULL;
    gboolean success;
    HV *params;
    HE *param;
    I32 len;
    int maxhandlers;
    int hdl_idx;

    /* make sure we got a hashref */
    if (!SvROK(params_hashref) || SvTYPE(SvRV(params_hashref)) != SVt_PVHV)
	croak("read() expects a single hashref");
    params = (HV *)SvRV(params_hashref);
    len = hv_iterinit(params);

    maxhandlers = hdl_idx = len;
    dat->handling_array = g_new0(amar_attr_handling_t, len+1);

    /* loop through the parameters */
    while ((param = hv_iternext(params))) {
	I32 keylen;
	char *key = hv_iterkey(param, &keylen);
	int attrid;

	/* if it's a number, it's handling information for an attrid */
	if (is_number(key, keylen, &attrid)) {
	    SV *val = hv_iterval(params, param);
	    SV *coderef;
	    UV bufsize = 0;
	    int i;

	    if (!SvROK(val)) goto croak_hdl;

	    switch (SvTYPE(SvRV(val))) {
		case SVt_PVCV:
		    coderef = val;
		    break;

		case SVt_PVAV: {
		    AV *arr = (AV *)SvRV(val);
		    SV **svp;

		    if (av_len(arr) != 1) /* av_len == largest index, not length */
			goto croak_hdl;

		    /* get the bufsize */
		    svp = av_fetch(arr, 0, 0);
		    if (!SvIOK(*svp))
			goto croak_hdl;
		    bufsize = SvUV(*svp);

		    /* and the coderef */
		    svp = av_fetch(arr, 1, 0);
		    if (!SvROK(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVCV)
			goto croak_hdl;
		    coderef = *svp;
		    break;
		}

		default:
		    goto croak_hdl;
	    }

	    /* fill in the handling array, putting attrid 0 at the end, and
	     * filling in entries backward from there */
	    i = (attrid == 0)? maxhandlers : --hdl_idx;
	    dat->handling_array[i].attrid = attrid;
	    dat->handling_array[i].min_size = bufsize;
	    dat->handling_array[i].callback = read_frag_cb;
	    dat->handling_array[i].attrid_data = coderef;
	    SvREFCNT_inc(coderef);
	    continue;

	croak_hdl:
	    croak("Expected CODEREF or [ MIN_SIZE, CODEREF ] for attrid %d", attrid);
	}

#define key_compare(key, val, keylen) \
    (keylen == sizeof(val)-1) && (0 == strncmp(key, val, keylen))

	if (key_compare(key, "file_start", keylen)) {
	    SV *val = hv_iterval(params, param);
	    if (!SvROK(val) || SvTYPE(SvRV(val)) != SVt_PVCV)
		croak("Expected a CODEREF for file_start");
	    dat->file_start_sub = val;
	    SvREFCNT_inc(val);
	    continue;
	}

	if (key_compare(key, "file_finish", keylen)) {
	    SV *val = hv_iterval(params, param);
	    if (!SvROK(val) || SvTYPE(SvRV(val)) != SVt_PVCV)
		croak("Expected a CODEREF for file_finish");
	    dat->file_finish_sub = val;
	    SvREFCNT_inc(val);
	    continue;
	}

	if (key_compare(key, "user_data", keylen)) {
	    SV *val = hv_iterval(params, param);
	    dat->user_data = val;
	    SvREFCNT_inc(val);
	    continue;
	}
#undef key_compare
	croak("Invalid parameter named '%*s'", (int)keylen, key);
    }

    if (!dat->user_data)
	dat->user_data = &PL_sv_undef;

    success = amar_read(archive, dat, dat->handling_array + hdl_idx,
	dat->file_start_sub? read_start_file_cb : NULL,
	dat->file_finish_sub? read_finish_file_cb : NULL,
	&error);

    /* now unreference and free everything we referenced earlier */
    if (dat->file_start_sub)
	SvREFCNT_dec(dat->file_start_sub);
    if (dat->file_finish_sub)
	SvREFCNT_dec(dat->file_finish_sub);
    if (dat->user_data && dat->user_data != &PL_sv_undef)
	SvREFCNT_dec(dat->user_data);

    for (hdl_idx = 0; hdl_idx <= maxhandlers; hdl_idx++) {
	if (dat->handling_array[hdl_idx].attrid_data)
	    SvREFCNT_dec(dat->handling_array[hdl_idx].attrid_data);
    }

    g_free(dat->handling_array);
    g_free(dat);

    /* if amar_read returned FALSE, then either we hit an internal
     * error, or one of the perl callbacks raised an exception, and $@
     * is still set */
    if (!success) {
	if (error)
	    croak_gerror(AMANDA_ARCHIVE_ERROR_DOMAIN, &error);
	else
	    croak(NULL);
    }
}

%}

/* now wrap those flat functions in Perl classes, depending on the perl
 * refcounting to close objects in the right order */

%perlcode %{
package Amanda::Archive;

# Expose the Archive constructor at Amanda::Archive->new
sub new {
    my $pkg = shift;
    Amanda::Archive::Archive->new(@_);
}

package Amanda::Archive::Archive;

sub new {
    my ($class, $fd, $mode) = @_;
    my $arch = Amanda::Archive::amar_new($fd, $mode);
    return bless (\$arch, $class);
}

sub close {
    my $self = shift;
    if ($$self) {
	Amanda::Archive::amar_close($$self);
	$$self = undef;
    }
}

sub DESTROY {
    my $self = shift;
    $self->close();
}

sub new_file {
    my ($self, $filename, $want_offset) = @_;
    return Amanda::Archive::File->new($self, $filename, $want_offset);
}

sub Amanda::Archive::Archive::read {
    my $self = shift;
    die "Archive is not open" unless ($$self);
    # pass a hashref to the C code
    my %h = @_;
    Amanda::Archive::amar_read($$self, \%h);
}

package Amanda::Archive::File;

sub new {
    my ($class, $arch, $filename, $want_offset) = @_;
    die "Archive is not open" unless ($$arch);
    if ($want_offset) {
	# note that posn is returned first by the SWIG wrapper
	my ($file, $posn) = Amanda::Archive::amar_new_file($$arch, $filename, $want_offset);
	return (bless([ $file, $arch ], $class), $posn);
    } else {
	my $file = Amanda::Archive::amar_new_file($$arch, $filename, $want_offset);
	return bless([ $file, $arch ], $class);
    }
}

sub close {
    my $self = shift;
    if ($self->[0]) {
	Amanda::Archive::amar_file_close($self->[0]);
	$self->[0] = undef;
    }
}

sub DESTROY {
    my $self = shift;
    $self->close();
}

sub new_attr {
    my ($self, $attrid) = @_;
    return Amanda::Archive::Attr->new($self, $attrid);
}

package Amanda::Archive::Attr;

sub new {
    my ($class, $file, $attrid) = @_;
    die "File is not open" unless ($file->[0]);
    my $attr = Amanda::Archive::amar_new_attr($file->[0], $attrid);
    return bless ([$attr, $file], $class);
}

sub close {
    my $self = shift;
    if ($self->[0]) {
	Amanda::Archive::amar_attr_close($self->[0]);
	$self->[0] = undef;
    }
}

sub DESTROY {
    my $self = shift;
    $self->close();
}

sub add_data {
    my ($self, $data, $eoa) = @_;
    die "Attr is not open" unless ($self->[0]);
    Amanda::Archive::amar_attr_add_data_buffer($self->[0], $data, $eoa);
}

sub add_data_fd {
    my ($self, $fd, $eoa) = @_;
    die "Attr is not open" unless ($self->[0]);
    return Amanda::Archive::amar_attr_add_data_fd($self->[0], $fd, $eoa);
}
%}
