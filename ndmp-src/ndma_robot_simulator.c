/*
 * Copyright (c) 1998,1999,2000
 *	Traakan, Inc., Los Altos, CA
 *	All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Project:  NDMJOB
 * Ident:    $Id: $
 *
 * Description:
 *
 */


#include "ndmagents.h"

#ifndef NDMOS_OPTION_NO_ROBOT_AGENT
#ifdef NDMOS_OPTION_ROBOT_SIMULATOR

#include "scsiconst.h"

#define ROBOT_CONTROLLER 0
#define ROBOT_ID 7
#define ROBOT_LUN 1

/*
 * interface
 */

int
ndmos_scsi_initialize (struct ndm_session *sess)
{
	struct ndm_robot_agent *	ra = &sess->robot_acb;

	NDMOS_MACRO_ZEROFILL(&ra->sim_dir);
	NDMOS_MACRO_ZEROFILL(&ra->scsi_state);
	ra->scsi_state.error = NDMP9_DEV_NOT_OPEN_ERR;
	ra->scsi_state.target_controller = ROBOT_CONTROLLER;
	ra->scsi_state.target_id = ROBOT_ID;
	ra->scsi_state.target_lun = ROBOT_LUN;

	return 0;
}

void
ndmos_scsi_sync_state (struct ndm_session *sess)
{
}

ndmp9_error
ndmos_scsi_open (struct ndm_session *sess, char *name)
{
	struct stat			st;
	struct ndm_robot_agent *	ra = &sess->robot_acb;

	if (!name || strlen(name) > sizeof(ra->sim_dir)-1)
		return NDMP9_NO_DEVICE_ERR;

	/* check that it's a directory */
	if (stat (name, &st) < 0)
		return NDMP9_NO_DEVICE_ERR;
	if (!S_ISDIR(st.st_mode))
		return NDMP9_NO_DEVICE_ERR;

	strncpy(ra->sim_dir, name, sizeof(ra->sim_dir)-1);
	ra->scsi_state.error = NDMP9_NO_ERR;

	return NDMP9_NO_ERR;
}

ndmp9_error
ndmos_scsi_close (struct ndm_session *sess)
{
	ndmos_scsi_initialize(sess);
	return NDMP9_NO_ERR;
}

/* deprecated */
ndmp9_error
ndmos_scsi_set_target (struct ndm_session *sess)
{
	return NDMP9_NOT_SUPPORTED_ERR;
}


ndmp9_error
ndmos_scsi_reset_device (struct ndm_session *sess)
{
	struct ndm_robot_agent *	ra = &sess->robot_acb;

	/* this is easy.. */
	return ra->scsi_state.error;
}

/* deprecated */
ndmp9_error
ndmos_scsi_reset_bus (struct ndm_session *sess)
{
	return NDMP9_NOT_SUPPORTED_ERR;
}

/*
 * Robot state management
 ****************************************************************
 */

/* xxx_FIRST must be in order! */
#define IE_FIRST 0
#define IE_COUNT 2
#define MTE_FIRST 16
#define MTE_COUNT 1
#define DTE_FIRST 128
#define DTE_COUNT 2
#define STORAGE_FIRST 1024
#define STORAGE_COUNT 10

#if (IE_FIRST+IE_COUNT > MTE_FIRST) \
 || (MTE_FIRST+MTE_COUNT > DTE_FIRST) \
 || (DTE_FIRST+MTE_COUNT > STORAGE_FIRST)
#error element addresses overlap or are in the wrong order
#endif

#define IS_IE_ADDR(a) ((a) >= IE_FIRST && (a) < IE_FIRST+IE_COUNT)
#define IS_MTE_ADDR(a) ((a) >= MTE_FIRST && (a) < MTE_FIRST+MTE_COUNT)
#define IS_DTE_ADDR(a) ((a) >= DTE_FIRST && (a) < DTE_FIRST+DTE_COUNT)
#define IS_STORAGE_ADDR(a) ((a) >= STORAGE_FIRST && (a) < STORAGE_FIRST+STORAGE_COUNT)

struct element_state {
	int full;
	int medium_type;
	int source_element;
	char pvoltag[32];
	char avoltag[32];
};

struct robot_state {
	struct element_state mte[MTE_COUNT];
	struct element_state storage[STORAGE_COUNT];
	struct element_state ie[IE_COUNT];
	struct element_state dte[DTE_COUNT];
};

static void
robot_state_init(struct robot_state *rs)
{
	int i;

	/* invent some nice data, with some nice voltags and whatnot */

	NDMOS_API_BZERO(rs, sizeof(*rs));

	/* (nothing to do for MTEs) */

	for (i = 0; i < STORAGE_COUNT; i++) {
		struct element_state *es = &rs->storage[i];
		es->full = 1;

		es->medium_type = 1; /* data */
		es->source_element = 0;
		snprintf(es->pvoltag, sizeof(es->pvoltag), "PTAG%02XXX                        ", i);
		snprintf(es->avoltag, sizeof(es->avoltag), "ATAG%02XXX                        ", i);
	}

	/* (i/e are all empty) */

	/* (dte's are all empty) */
}

static void
robot_state_load(struct ndm_session *sess, struct robot_state *rs)
{
	int fd;
	char filename[PATH_MAX];

	/* N.B. writing a struct to disk like this isn't portable, but this
	 * is test code, so it's OK for now. */

	snprintf(filename, sizeof filename, "%s/state", sess->robot_acb.sim_dir);
	fd = open(filename, O_RDONLY, 0666);
	if (fd < 0) {
		robot_state_init(rs);
		return;
	}
	if (read(fd, (void *)rs, sizeof(*rs)) < sizeof(*rs)) {
		robot_state_init(rs);
		return;
	}

	close(fd);
}

static int
robot_state_save(struct ndm_session *sess, struct robot_state *rs)
{
	int fd;
	char filename[PATH_MAX];

	/* N.B. writing a struct to disk like this isn't portable, but this
	 * is test code, so it's OK for now. */

	snprintf(filename, sizeof filename, "%s/state", sess->robot_acb.sim_dir);
	fd = open(filename, O_WRONLY|O_TRUNC|O_CREAT, 0666);
	if (fd < 0)
		return -1;
	if (write(fd, (void *)rs, sizeof(*rs)) < sizeof(*rs))
		return -1;
	close(fd);

	return 0;
}

static int
robot_state_move(struct ndm_session *sess, struct robot_state *rs, int src, int dest)
{
	char src_filename[PATH_MAX];
	struct element_state *src_elt;
	char dest_filename[PATH_MAX];
	struct element_state *dest_elt;
	struct stat st;
	char pos[PATH_MAX];

	/* TODO: audit that the tape device is not using this volume right now */

	ndmalogf(sess, 0, 3, "moving medium from %d to %d", src, dest);

	if (IS_IE_ADDR(src)) {
		src_elt = &rs->ie[src - IE_FIRST];
		snprintf(src_filename, sizeof(src_filename), "%s/ie%d",
		    sess->robot_acb.sim_dir, src - IE_FIRST);
	} else if (IS_DTE_ADDR(src)) {
		src_elt = &rs->dte[src - DTE_FIRST];
		snprintf(src_filename, sizeof(src_filename), "%s/drive%d",
		    sess->robot_acb.sim_dir, src - DTE_FIRST);
	} else if (IS_STORAGE_ADDR(src)) {
		src_elt = &rs->storage[src - STORAGE_FIRST];
		snprintf(src_filename, sizeof(src_filename), "%s/slot%d",
		    sess->robot_acb.sim_dir, src - STORAGE_FIRST);
	} else {
		ndmalogf(sess, 0, 3, "invalid src address %d", src);
		return -1;
	}

	if (IS_IE_ADDR(dest)) {
		dest_elt = &rs->ie[dest - IE_FIRST];
		snprintf(dest_filename, sizeof(dest_filename), "%s/ie%d",
		    sess->robot_acb.sim_dir, dest - IE_FIRST);
	} else if (IS_DTE_ADDR(dest)) {
		dest_elt = &rs->dte[dest - DTE_FIRST];
		snprintf(dest_filename, sizeof(dest_filename), "%s/drive%d",
		    sess->robot_acb.sim_dir, dest - DTE_FIRST);
	} else if (IS_STORAGE_ADDR(dest)) {
		dest_elt = &rs->storage[dest - STORAGE_FIRST];
		snprintf(dest_filename, sizeof(dest_filename), "%s/slot%d",
		    sess->robot_acb.sim_dir, dest - STORAGE_FIRST);
	} else {
		ndmalogf(sess, 0, 3, "invalid dst address %d", src);
		return -1;
	}

	if (!src_elt->full) {
		ndmalogf(sess, 0, 3, "src not full");
		return -1;
	}

	if (dest_elt->full) {
		ndmalogf(sess, 0, 3, "dest full");
		return -1;
	}

	/* OK, enough checking, let's do it */
	/* delete the destination, if it exists */
	if (stat (dest_filename, &st) >= 0) {
		ndmalogf(sess, 0, 3, "unlink %s", dest_filename);
		if (unlink(dest_filename) < 0) {
			ndmalogf(sess, 0, 0, "error unlinking: %s", strerror(errno));
			return -1;
		}
	}

	/* and move the source if it exists */
	if (stat (src_filename, &st) >= 0) {
		ndmalogf(sess, 0, 3, "move %s to %s", src_filename, dest_filename);
		if (rename(src_filename, dest_filename) < 0) {
			ndmalogf(sess, 0, 0, "error renaming: %s", strerror(errno));
			return -1;
		}
	} else {
		/* otherwise touch the destination file */
		ndmalogf(sess, 0, 3, "touch %s", dest_filename);
		int fd = open(dest_filename, O_CREAT | O_WRONLY, 0666);
		if (fd < 0) {
			ndmalogf(sess, 0, 0, "error touching: %s", strerror(errno));
			return -1;
		}
		close(fd);
	}

	/* blow away any tape-drive .pos files */
	snprintf(pos, sizeof(pos), "%s.pos", src_filename);
	unlink(pos); /* ignore errors */
	snprintf(pos, sizeof(pos), "%s.pos", dest_filename);
	unlink(pos); /* ignore errors */

	/* update state */
	*dest_elt = *src_elt;
	ndmalogf(sess, 0, 3, "setting dest's source_element to %d", src);
	dest_elt->source_element = src;
	src_elt->full = 0;


	ndmalogf(sess, 0, 3, "move successful");
	return 0;
}

/*
 * SCSI commands
 ****************************************************************
 */

/*
 * Utilities
 */

static ndmp9_error
scsi_fail_with_sense_code(struct ndm_session *sess,
	    ndmp9_execute_cdb_reply *reply,
	    int status, int sense_key, int asq)
{
	unsigned char ext_sense[] = {
		0x72, /* current errors */
		sense_key & SCSI_SENSE_SENSE_KEY_MASK,
		(asq >> 8) & 0xff,
		(asq     ) & 0xff,
		0,
		0,
		0,
		0 };

	ndmalogf(sess, 0, 3, "sending failure; status=0x%02x sense_key=0x%02x asq=0x%04x",
		    status, sense_key, asq);

	reply->status = status;
	reply->ext_sense.ext_sense_len = sizeof(ext_sense);
	reply->ext_sense.ext_sense_val = NDMOS_API_MALLOC(sizeof(ext_sense));
	NDMOS_API_BCOPY(ext_sense, reply->ext_sense.ext_sense_val, sizeof(ext_sense));

	return NDMP9_NO_ERR;
}

/*
 * Command implementations
 */

static ndmp9_error
execute_cdb_test_unit_ready (struct ndm_session *sess,
  ndmp9_execute_cdb_request *request,
  ndmp9_execute_cdb_reply *reply)
{
	unsigned char *cdb = (unsigned char *)request->cdb.cdb_val;
	char *response;
	int response_len;
	char *p;

	if (request->cdb.cdb_len != 6)
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);

	/* yep, we're ready! */

	return NDMP9_NO_ERR;
}

static ndmp9_error
execute_cdb_inquiry (struct ndm_session *sess,
  ndmp9_execute_cdb_request *request,
  ndmp9_execute_cdb_reply *reply)
{
	unsigned char *cdb = (unsigned char *)request->cdb.cdb_val;
	char *response;
	int response_len;
	char *p;

	/* N.B.: only page code 0 is supported */
	if (request->cdb.cdb_len != 6
	    || request->data_dir != NDMP9_SCSI_DATA_DIR_IN
	    || cdb[1] & 0x01
	    || cdb[2] != 0
	    || request->datain_len < 96
	    || ((cdb[3] << 8) + cdb[4]) < 96)
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);

	response_len = 96;
	p = response = NDMOS_API_MALLOC(response_len);
	NDMOS_API_BZERO(response, response_len);
	*(p++) = 0x08;  /* media changer */
	*(p++) = 0;	/* RMB=0 */
	*(p++) = 6;	/* VERSION=SPC-4 */
	*(p++) = 2;	/* !NORMACA, !HISUP, RESPONSE DATA FORMAT = 2 */
	*(p++) = 92;	/* remaining bytes */
	*(p++) = 0;	/* lots of flags, all 0 */
	*(p++) = 0;	/* lots of flags, all 0 */
	*(p++) = 0;	/* lots of flags, all 0 */
	NDMOS_API_BCOPY("NDMJOB  ", p, 8); p += 8;
	NDMOS_API_BCOPY("FakeRobot        ", p, 16); p += 16;
	NDMOS_API_BCOPY("1.0 ", p, 4); p += 4;
	/* remainder is zero */

	reply->datain.datain_len = response_len;
	reply->datain.datain_val = response;

	return NDMP9_NO_ERR;
}

static ndmp9_error
execute_cdb_mode_sense_6 (struct ndm_session *sess,
  ndmp9_execute_cdb_request *request,
  ndmp9_execute_cdb_reply *reply)
{
	unsigned char *cdb = (unsigned char *)request->cdb.cdb_val;
	int page, subpage;
	char *response;
	int response_len;
	char *p;

	if (request->cdb.cdb_len != 6
	    || request->data_dir != NDMP9_SCSI_DATA_DIR_IN)
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);
	page = cdb[2] & 0x3f;
	subpage = cdb[3];

	switch ((page << 8) + subpage) {
	case 0x1D00: /* Element Address Assignment */
		if (request->datain_len < 20 || cdb[4] < 20)
			return scsi_fail_with_sense_code(sess, reply,
			    SCSI_STATUS_CHECK_CONDITION,
			    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
			    ASQ_INVALID_FIELD_IN_CDB);

		response_len = 24;
		p = response = NDMOS_API_MALLOC(response_len);
		NDMOS_API_BZERO(response, response_len);
		*(p++) = response_len;
		*(p++) = 0; /* reserved medium type */
		*(p++) = 0; /* reserved device-specific parameter */
		*(p++) = 0; /* block descriptor length (DBD = 0 above)*/
		*(p++) = 0x1D; /* page code */
		*(p++) = 18; /* remaining bytes */
		*(p++) = (MTE_FIRST >> 8) & 0xff;
		*(p++) = MTE_FIRST & 0xff;
		*(p++) = (MTE_COUNT >> 8) & 0xff;
		*(p++) = MTE_COUNT & 0xff;
		*(p++) = (STORAGE_FIRST >> 8) & 0xff;
		*(p++) = STORAGE_FIRST & 0xff;
		*(p++) = (STORAGE_COUNT >> 8) & 0xff;
		*(p++) = STORAGE_COUNT & 0xff;
		*(p++) = (IE_FIRST >> 8) & 0xff;
		*(p++) = IE_FIRST & 0xff;
		*(p++) = (IE_COUNT >> 8) & 0xff;
		*(p++) = IE_COUNT & 0xff;
		*(p++) = (DTE_FIRST >> 8) & 0xff;
		*(p++) = DTE_FIRST & 0xff;
		*(p++) = (DTE_COUNT >> 8) & 0xff;
		*(p++) = DTE_COUNT & 0xff;
		/* remainder is zero */
		break;

	default:
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);
	}

	reply->datain.datain_len = response_len;
	reply->datain.datain_val = response;

	return NDMP9_NO_ERR;
}

static ndmp9_error
execute_cdb_read_element_status (struct ndm_session *sess,
  ndmp9_execute_cdb_request *request,
  ndmp9_execute_cdb_reply *reply)
{
	unsigned char *cdb = (unsigned char *)request->cdb.cdb_val;
	struct robot_state rs;
	int min_addr, max_elts;
	char *response;
	int response_len;
	int required_len;
	int num_elts = IE_COUNT + MTE_COUNT + DTE_COUNT + STORAGE_COUNT;
	char *p;

	if (request->cdb.cdb_len != 12
	    || request->data_dir != NDMP9_SCSI_DATA_DIR_IN)
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);
	min_addr = (cdb[2] << 8) + cdb[3];
	max_elts = (cdb[4] << 8) + cdb[5];
	response_len = (cdb[7] << 16) + (cdb[8] << 8) + cdb[9];

	if (response_len < 8) {
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);
	}

	/* this is bogus, but we don't allow "partial" status requests */
	if (min_addr > IE_FIRST || max_elts < num_elts) {
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);
	}

	robot_state_load(sess, &rs);
	robot_state_save(sess, &rs);

	/* calculate the total space required */
	required_len = 8; /* element status data header */
	if (MTE_COUNT) {
		required_len += 8; /* element status page header */
		required_len += 12 * MTE_COUNT; /* element status descriptor w/o tags */
	}
	if (STORAGE_COUNT) {
		required_len += 8; /* element status page header */
		required_len += 84 * STORAGE_COUNT; /* element status descriptor w/ tags */
	}
	if (IE_COUNT) {
		required_len += 8; /* element status page header */
		required_len += 84 * IE_COUNT; /* element status descriptor w/ tags */
	}
	if (DTE_COUNT) {
		required_len += 8; /* element status page header */
		required_len += 84 * DTE_COUNT; /* element status descriptor w/ tags */
	}

	p = response = NDMOS_API_MALLOC(response_len);
	NDMOS_API_BZERO(response, response_len);

	/* write the element status data header */
	*(p++) = IE_FIRST >> 8; /* first element address */
	*(p++) = IE_FIRST & 0xff;
	*(p++) = num_elts >> 8; /* number of elements */
	*(p++) = num_elts & 0xff;
	*(p++) = 0; /* reserved */
	*(p++) = (required_len-8) >> 16; /* remaining byte count of report */
	*(p++) = ((required_len-8) >> 8) & 0xff;
	*(p++) = (required_len-8) & 0xff;

	/* only fill in the rest if we have space */
	if (required_len <= response_len) {
		int i;
		struct {
			int first, count, have_voltags, eltype;
			int empty_flags, full_flags;
			struct element_state *es;
		} page[4] = {
			{ IE_FIRST, IE_COUNT, 1, 3, 0x38, 0x39, &rs.ie[0] },
			{ MTE_FIRST, MTE_COUNT, 0, 1, 0x00, 0x01, &rs.mte[0] },
			{ DTE_FIRST, DTE_COUNT, 1, 4, 0x08, 0x81, &rs.dte[0] },
			{ STORAGE_FIRST, STORAGE_COUNT, 1, 2, 0x08, 0x09, &rs.storage[0] },
		};

		for (i = 0; i < 4; i++) {
			int descr_size = page[i].have_voltags? 84 : 12;
			int totalsize = descr_size * page[i].count;
			int j;

			if (page[i].count == 0)
				continue;

			/* write the page header */
			*(p++) = page[i].eltype;
			*(p++) = page[i].have_voltags? 0xc0 : 0;
			*(p++) = 0;
			*(p++) = descr_size;
			*(p++) = 0; /* reserved */
			*(p++) = totalsize >> 16;
			*(p++) = (totalsize >> 8) & 0xff;
			*(p++) = totalsize & 0xff;

			/* and write each descriptor */
			for (j = 0; j < page[i].count; j++) {
				int elt_addr = page[i].first + j;
				int src_elt = page[i].es[j].source_element;
				unsigned char byte9 = page[i].es[j].medium_type;
				if (src_elt!= 0)
					byte9 |= 0x80; /* SVALID */

				*(p++) = elt_addr >> 8;
				*(p++) = elt_addr & 0xff;
				*(p++) = page[i].es[j].full?
					    page[i].full_flags : page[i].empty_flags;
				*(p++) = 0;
				*(p++) = 0;
				*(p++) = 0;
				*(p++) = 0;
				*(p++) = 0;
				*(p++) = 0;
				*(p++) = byte9;
				*(p++) = src_elt >> 8;
				*(p++) = src_elt & 0xff;

				if (page[i].have_voltags) {
					int k;
					if (page[i].es[j].full) {
						for (k = 0; k < 32; k++) {
							if (!page[i].es[j].pvoltag[k])
								break;
							p[k] = page[i].es[j].pvoltag[k];
						}
						for (k = 0; k < 32; k++) {
							if (!page[i].es[j].avoltag[k])
								break;
							p[k+36] = page[i].es[j].avoltag[k];
						}
					} else {
						for (k = 0; k < 32; k++) {
							p[k] = p[k+36] = ' ';
						}
					}
					p += 72;
				}
			}
		}
	}

	reply->datain.datain_len = response_len;
	reply->datain.datain_val = response;

	return NDMP9_NO_ERR;
}

static ndmp9_error
execute_cdb_move_medium (struct ndm_session *sess,
  ndmp9_execute_cdb_request *request,
  ndmp9_execute_cdb_reply *reply)
{
	unsigned char *cdb = (unsigned char *)request->cdb.cdb_val;
	struct robot_state rs;
	int mte, src, dest;

	if (request->cdb.cdb_len != 12)
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_FIELD_IN_CDB);
	mte = (cdb[2] << 8) + cdb[3];
	src = (cdb[4] << 8) + cdb[5];
	dest = (cdb[6] << 8) + cdb[7];

	if (!IS_MTE_ADDR(mte))
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_ELEMENT_ADDRESS);

	robot_state_load(sess, &rs);
	if (robot_state_move(sess, &rs, src, dest) < 0)
		return scsi_fail_with_sense_code(sess, reply,
		    SCSI_STATUS_CHECK_CONDITION,
		    SCSI_SENSE_KEY_ILLEGAL_REQUEST,
		    ASQ_INVALID_ELEMENT_ADDRESS);
	robot_state_save(sess, &rs);

	return NDMP9_NO_ERR;
}

static struct {
	char cdb_byte;
	ndmp9_error (* execute_cdb)(
		  struct ndm_session *sess,
		  ndmp9_execute_cdb_request *request,
		  ndmp9_execute_cdb_reply *reply);
} cdb_executors[] = {
	{ SCSI_CMD_TEST_UNIT_READY, execute_cdb_test_unit_ready },
	{ SCSI_CMD_INQUIRY, execute_cdb_inquiry },
	{ SCSI_CMD_MODE_SENSE_6, execute_cdb_mode_sense_6 },
	{ SCSI_CMD_READ_ELEMENT_STATUS, execute_cdb_read_element_status },
	{ SCSI_CMD_MOVE_MEDIUM, execute_cdb_move_medium },
	{ 0, 0 },
};

ndmp9_error
ndmos_scsi_execute_cdb (struct ndm_session *sess,
  ndmp9_execute_cdb_request *request,
  ndmp9_execute_cdb_reply *reply)
{
	struct ndm_robot_agent *	ra = &sess->robot_acb;
	char cdb_byte;
	int i;

	if (ra->scsi_state.error != NDMP9_NO_ERR)
		return ra->scsi_state.error;

	if (request->cdb.cdb_len < 1)
		return NDMP9_ILLEGAL_ARGS_ERR;

	cdb_byte = request->cdb.cdb_val[0];
	for (i = 0; cdb_executors[i].execute_cdb; i++) {
		if (cdb_executors[i].cdb_byte == cdb_byte)
			return cdb_executors[i].execute_cdb(sess, request, reply);
	}

	return NDMP9_ILLEGAL_ARGS_ERR;
}

#endif /* NDMOS_OPTION_ROBOT_SIMULATOR */

#endif /* !NDMOS_OPTION_NO_ROBOT_AGENT */
