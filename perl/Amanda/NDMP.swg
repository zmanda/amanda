/*
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::NDMP"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/NDMP.pod"

%{
#include "ndmpconnobj.h"
#include "glib-util.h"
%}

/* initialize glib, and in particular GType */
%init %{
    glib_init();
%}

/* supporting typemaps */

/* treat all enums in this file as regular UV's - no need for BigInt */
%{
typedef guint ndmp_enum;
%}

/* Any enumeration, input */
%typemap(in) ndmp_enum "$1 = SvIV($input);";

/* Any enumeration, output argument */
%typemap(in,numinputs=0) ndmp_enum * (int temp) "$1 = &temp; temp = 0;";
%typemap(argout) ndmp_enum * {
    if (argvi >= items) {
	EXTEND(sp,1);
    }

    $result = sv_newmortal();
    sv_setuv($result, (UV)(*$1));
    argvi++;
}

/* gsize, output argument */
%typemap(in,numinputs=0) gsize * (gsize temp) "$1 = &temp; temp = 0;"
%typemap(argout) gsize * {
    if (argvi >= items) {
	EXTEND(sp,1);
    }

    SP += argvi; PUTBACK;
    $result = sv_2mortal(amglue_newSVu64(*$1));
    SP -= argvi; argvi++;
}

/* guint64, output argument */
%typemap(in,numinputs=0) guint64 * (guint64 temp) "$1 = &temp; temp = 0;"
%typemap(argout) guint64 * {
    if (argvi >= items) {
	EXTEND(sp,1);
    }

    SP += argvi; PUTBACK;
    $result = sv_2mortal(amglue_newSVu64(*$1));
    SP -= argvi; argvi++;
}

/* guint, output argument */
%typemap(in,numinputs=0) guint * (guint temp) "$1 = &temp; temp = 0;"
%typemap(argout) guint * {
    if (argvi >= items) {
	EXTEND(sp,1);
    }

    SP += argvi; PUTBACK;
    $result = sv_2mortal(amglue_newSVu64(*$1));
    SP -= argvi; argvi++;
}

typedef struct NDMPConnection {
    /* methods */
    %extend {
	/* constructor */
	NDMPConnection(
		gchar *hostname,
		gint port,
		gchar *username,
		gchar *password,
		gchar *auth) {
	    return ndmp_connection_new(
		    hostname, port, username, password, auth);
	}

	~NDMPConnection() {
	    g_object_unref(self);
	}

	/* error handling */

	int
	err_code() {
	    return ndmp_connection_err_code(self);
	}

	%newobject err_msg;
	gchar *
	err_msg() {
	    return ndmp_connection_err_msg(self);
	}

	void
	set_verbose(gboolean verbose) {
	    ndmp_connection_set_verbose(self, verbose);
	}

	/* operations */

	gboolean
	scsi_open(gchar *device) {
	    return ndmp_connection_scsi_open(
		    self,
		    device);
	}

	gboolean
	scsi_close() {
	    return ndmp_connection_scsi_close(self);
	}

	/* NOTE: this method is wrapped with a more perlish interface; see below */

	/* handle cdb */
	%typemap(in) (gpointer cdb, gsize cdb_len) {
	    /* cdb */
	    int alloc = SWIG_OLDOBJ;
	    SWIG_AsCharPtrAndSize($input, (char **)&$1, &$2, &alloc);
	    if ($2) $2--; /* strip trailing NUL byte */
	}

	/* handle dataout and its response */
	%typemap(in,numinputs=1)
	    (gpointer dataout, gsize dataout_len, gsize *actual_dataout_len)
	    (gsize dataout_len) {
	    /* dataout */

	    /* handle the actual input here */
	    int alloc = SWIG_OLDOBJ;
	    SWIG_AsCharPtrAndSize($input, (char **)&$1, &$2, &alloc);
	    if ($2) $2--; /* strip trailing NUL byte */
	    /* set up for argout typemap, below */
	    dataout_len = 0;
	    $3 = &dataout_len;
	}
	%typemap(argout)
	    (gpointer dataout, gsize dataout_len, gsize *actual_dataout_len) {
	    if (argvi >= items) {
		EXTEND(sp,1);
	    }

	    SP += argvi; PUTBACK;
	    $result = sv_2mortal(amglue_newSVu64(*$3));
	    SP -= argvi; argvi++;
	}

	/* handle datain and its response */
	%typemap(in,numinputs=1)
	    (gpointer datain, gsize datain_max_len, gsize *actual_datain_len)
	    (gpointer datain_buf, gsize actual_datain_len) {
	    /* datain */
	    gsize max_size = amglue_SvU64($input);
	    $2 = max_size;
	    if (max_size) {
		$1 = g_malloc(max_size);
	    } else {
		$1 = NULL;
	    }
	    actual_datain_len = 0;
	    $3 = &actual_datain_len;
	}
	%typemap(argout)
	    (gpointer datain, gsize datain_max_len, gsize *actual_datain_len) {
	    if (argvi >= items) {
		EXTEND(sp,1);
	    }

	    /* convert the value, even if it's NULL */
	    $result = SWIG_FromCharPtrAndSize($1, *$3);
	    argvi++;
	    if ($1) g_free($1);
	}

	/* handle status */
	%typemap(in,numinputs=0) (guint8 *status) (guint8 temp) "$1 = &temp;";
	%typemap(argout) (guint8 *status) {
	    if (argvi >= items) {
		EXTEND(sp,1);
	    }

	    $result = sv_newmortal();
	    sv_setuv($result, (UV)(*$1));
	    argvi++;
	}

	/* handle ext_status */
	%typemap(in,numinputs=0)
	    (gpointer ext_sense, gsize ext_sense_max_len, gsize *actual_ext_sense_len)
	    (guint8 ext_sense_buf[128], gsize actual_ext_sense_len) {
	    $1 = ext_sense_buf;
	    $2 = sizeof(ext_sense_buf); /* always allocate 128 bytes for sense */
	    actual_ext_sense_len = 0;
	    $3 = &actual_ext_sense_len;
	}
	%typemap(argout)
	    (gpointer ext_sense, gsize ext_sense_max_len, gsize *actual_ext_sense_len) {
	    if (argvi >= items) {
		EXTEND(sp,1);
	    }

	    $result = SWIG_FromCharPtrAndSize($1, *$3);
	    argvi++;
	}

	gboolean
	scsi_execute_cdb_C(
		ndmp_enum flags,
		guint32 timeout,
		gpointer cdb,
		gsize cdb_len,
		gpointer dataout,
		gsize dataout_len,
		gsize *actual_dataout_len,
		gpointer datain,
		gsize datain_max_len,
		gsize *actual_datain_len,
		guint8 *status,
		gpointer ext_sense,
		gsize ext_sense_max_len,
		gsize *actual_ext_sense_len
		) {
	    return ndmp_connection_scsi_execute_cdb(
		    self,
		    flags,
		    timeout,
		    cdb,
		    cdb_len,
		    dataout,
		    dataout_len,
		    actual_dataout_len,
		    datain,
		    datain_max_len,
		    actual_datain_len,
		    status,
		    ext_sense,
		    ext_sense_max_len,
		    actual_ext_sense_len
		    );
	}

	gboolean
	tape_open(
		gchar *device,
		ndmp_enum mode) {
	    return ndmp_connection_tape_open(
		    self,
		    device,
		    mode);
	}

	gboolean
	tape_close() {
	    return ndmp_connection_tape_close(self);
	}

	gboolean
	tape_mtio(
		ndmp_enum tape_op,
		gint count,
		guint *resid) {
	    return ndmp_connection_tape_mtio(
		    self,
		    tape_op,
		    count,
		    resid);
	}

	%typemap(in) (gpointer buf, guint64 len) {
	    int alloc = SWIG_OLDOBJ;
	    size_t len_tmp; /* use a temporary with the right type */
	    SWIG_AsCharPtrAndSize($input, (char **)&$1, &len_tmp, &alloc);
	    if (len_tmp) len_tmp--; /* strip trailing NUL byte */
	    $2 = len_tmp;
	}
	gboolean
	tape_write(
		gpointer buf,
		guint64 len,
		guint64 *count) {
	    return ndmp_connection_tape_write(
		    self,
		    buf,
		    len,
		    count);
	}

	%typemap(in,numinputs=1)
	    (gpointer buf, guint64 count, guint64 *out_count)
	    (guint64 out_count) {
	    $2 = amglue_SvU64($input);
	    $1 = $2?g_malloc($2):NULL;
	    out_count = 0;
	    $3 = &out_count;
	}
	%typemap(argout)
	    (gpointer buf, guint64 count, guint64 *out_count) {
	    if (argvi >= items) {
		EXTEND(sp,1);
	    }

	    /* convert the value, even if it's NULL */
	    $result = SWIG_FromCharPtrAndSize($1, *$3);
	    argvi++;
	    if ($1) g_free($1);
	}
	gboolean
	tape_read(
		gpointer buf,
		guint64 count,
		guint64 *out_count) {
	    return ndmp_connection_tape_read(
		    self,
		    buf,
		    count,
		    out_count);
	}

	gboolean
	tape_get_state(
		guint64 *blocksize,
		guint64 *file_num,
		guint64 *blockno) {
	    return ndmp_connection_tape_get_state(
		    self,
		    blocksize,
		    file_num,
		    blockno);
	}

	/* mover interface is not yet SWIGged */
    };
} NDMPConnection;

/* perlish wrappers */
%perlcode %{
package Amanda::NDMP::NDMPConnection;

sub scsi_execute_cdb {
    my $self = shift;
    my %params = @_;

    die "no 'flags' parameter'" unless defined $params{'flags'};
    die "no 'timeout' parameter'" unless defined $params{'timeout'};
    die "no 'cdb' parameter'" unless defined $params{'cdb'};
    if ($params{'flags'} & $Amanda::NDMP::NDMP9_SCSI_DATA_DIR_IN) {
	die "no 'datain_len' parameter'" unless defined $params{'datain_len'};
    } else {
	$params{'datain_len'} = 0;
    }
    if ($params{'flags'} & $Amanda::NDMP::NDMP9_SCSI_DATA_DIR_OUT) {
	die "no 'dataout' parameter'" unless defined $params{'dataout'};
    } else {
	$params{'dataout'} = undef;
    }

    my ($ok, $dataout_len, $datain, $status, $ext_sense) =
	$self->scsi_execute_cdb_C(
	    $params{'flags'}, $params{'timeout'},
	    $params{'cdb'}, $params{'dataout'},
	    $params{'datain_len'});

    return 0 unless ($ok);

    my %result = (
	status => $status,
	ext_sense => $ext_sense);
    if ($params{'flags'} & $Amanda::NDMP::NDMP9_SCSI_DATA_DIR_IN) {
	$result{'datain'} = $datain;
    }
    if ($params{'flags'} & $Amanda::NDMP::NDMP9_SCSI_DATA_DIR_OUT) {
	$result{'dataout_len'} = $dataout_len;
    }
    return \%result;
}

package Amanda::NDMP;
%}

/* selected NDMP constants; note that the "NDMP9_" prfix is required */

amglue_add_flag_tag_fns(scsi_data_dir);
amglue_add_constant_short(NDMP9_SCSI_DATA_DIR_NONE, "NDMP9_SCSI_DATA_DIR_NONE", scsi_data_dir);
amglue_add_constant_short(NDMP9_SCSI_DATA_DIR_IN, "NDMP9_SCSI_DATA_DIR_IN", scsi_data_dir);
amglue_add_constant_short(NDMP9_SCSI_DATA_DIR_OUT, "NDMP9_SCSI_DATA_DIR_OUT", scsi_data_dir);
amglue_copy_to_tag(scsi_data_dir, constants);

amglue_add_flag_tag_fns(tape_open_mode);
amglue_add_constant_short(NDMP9_TAPE_READ_MODE, "NDMP9_TAPE_READ_MODE", tape_open_mode);
amglue_add_constant_short(NDMP9_TAPE_RDWR_MODE, "NDMP9_TAPE_RDRW_MODE", tape_open_mode);
amglue_add_constant_short(NDMP9_TAPE_RAW_MODE, "NDMP9_TAPE_RAW_MODE", tape_open_mode);
amglue_copy_to_tag(tape_open_mode, constants);

amglue_add_flag_tag_fns(tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_FSF, "NDMP9_MTIO_FSF", tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_BSF, "NDMP9_MTIO_BSF", tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_FSR, "NDMP9_MTIO_FSR", tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_BSR, "NDMP9_MTIO_BSR", tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_REW, "NDMP9_MTIO_REW", tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_EOF, "NDMP9_MTIO_EOF", tape_mtio_op);
amglue_add_constant_short(NDMP9_MTIO_OFF, "NDMP9_MTIO_OFF", tape_mtio_op);
amglue_copy_to_tag(tape_mtio_op, constants);

amglue_add_flag_tag_fns(mover_mode);
amglue_add_constant_short(NDMP9_MOVER_MODE_READ, "NDMP9_MOVER_MODE_READ", mover_mode);
amglue_add_constant_short(NDMP9_MOVER_MODE_WRITE, "NDMP9_MOVER_MODE_WRITE", mover_mode);
amglue_copy_to_tag(mover_mode, constants);

amglue_add_flag_tag_fns(addr_type);
amglue_add_constant_short(NDMP9_ADDR_LOCAL, "NDMP9_ADDR_LOCAL", addr_type);
amglue_add_constant_short(NDMP9_ADDR_TCP, "NDMP9_ADDR_TCP", addr_type);
amglue_add_constant_short(NDMP9_ADDR_AS_CONNECTED, "NDMP9_ADDR_AS_CONNECTED", addr_type);
amglue_copy_to_tag(addr_type, constants);

amglue_add_flag_tag_fns(mover_state);
amglue_add_constant_short(NDMP9_MOVER_STATE_IDLE, "NDMP9_MOVER_STATE_IDLE", mover_state);
amglue_add_constant_short(NDMP9_MOVER_STATE_LISTEN, "NDMP9_MOVER_STATE_LISTEN", mover_state);
amglue_add_constant_short(NDMP9_MOVER_STATE_ACTIVE, "NDMP9_MOVER_STATE_ACTIVE", mover_state);
amglue_add_constant_short(NDMP9_MOVER_STATE_PAUSED, "NDMP9_MOVER_STATE_PAUSED", mover_state);
amglue_add_constant_short(NDMP9_MOVER_STATE_HALTED, "NDMP9_MOVER_STATE_HALTED", mover_state);
amglue_add_constant_short(NDMP9_MOVER_STATE_STANDBY, "NDMP9_MOVER_STATE_STANDBY", mover_state);
amglue_copy_to_tag(mover_state, constants);

amglue_add_flag_tag_fns(data_halt_reason);
amglue_add_constant_short(NDMP9_DATA_HALT_NA, "NDMP9_DATA_HALT_NA", data_halt_reason);
amglue_add_constant_short(NDMP9_DATA_HALT_SUCCESSFUL, "NDMP9_DATA_HALT_SUCCESSFUL", data_halt_reason);
amglue_add_constant_short(NDMP9_DATA_HALT_ABORTED, "NDMP9_DATA_HALT_ABORTED", data_halt_reason);
amglue_add_constant_short(NDMP9_DATA_HALT_INTERNAL_ERROR, "NDMP9_DATA_HALT_INTERNAL_ERROR", data_halt_reason);
amglue_add_constant_short(NDMP9_DATA_HALT_CONNECT_ERROR, "NDMP9_DATA_HALT_CONNECT_ERROR", data_halt_reason);
amglue_copy_to_tag(data_halt_reason, constants);

amglue_add_flag_tag_fns(mover_halt_reason);
amglue_add_constant_short(NDMP9_MOVER_HALT_NA, "NDMP9_MOVER_HALT_NA", mover_halt_reason);
amglue_add_constant_short(NDMP9_MOVER_HALT_CONNECT_CLOSED, "NDMP9_MOVER_HALT_CONNECT_CLOSED", mover_halt_reason);
amglue_add_constant_short(NDMP9_MOVER_HALT_ABORTED, "NDMP9_MOVER_HALT_ABORTED", mover_halt_reason);
amglue_add_constant_short(NDMP9_MOVER_HALT_INTERNAL_ERROR, "NDMP9_MOVER_HALT_INTERNAL_ERROR", mover_halt_reason);
amglue_add_constant_short(NDMP9_MOVER_HALT_CONNECT_ERROR, "NDMP9_MOVER_HALT_CONNECT_ERROR", mover_halt_reason);
amglue_copy_to_tag(mover_halt_reason, constants);

amglue_add_flag_tag_fns(mover_pause_reason);
amglue_add_constant_short(NDMP9_MOVER_PAUSE_NA, "NDMP9_MOVER_PAUSE_NA", mover_pause_reason);
amglue_add_constant_short(NDMP9_MOVER_PAUSE_EOM, "NDMP9_MOVER_PAUSE_EOM", mover_pause_reason);
amglue_add_constant_short(NDMP9_MOVER_PAUSE_EOF, "NDMP9_MOVER_PAUSE_EOF", mover_pause_reason);
amglue_add_constant_short(NDMP9_MOVER_PAUSE_SEEK, "NDMP9_MOVER_PAUSE_SEEK", mover_pause_reason);
amglue_add_constant_short(NDMP9_MOVER_PAUSE_MEDIA_ERROR, "NDMP9_MOVER_PAUSE_MEDIA_ERROR", mover_pause_reason);
amglue_add_constant_short(NDMP9_MOVER_PAUSE_EOW, "NDMP9_MOVER_PAUSE_EOW", mover_pause_reason);
amglue_copy_to_tag(mover_pause_reason, constants);
