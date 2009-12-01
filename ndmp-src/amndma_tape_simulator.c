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


#ifndef NDMOS_OPTION_NO_TAPE_AGENT


int	simu_back_one (struct ndm_session *sess, int over_file_mark);
int	simu_forw_one (struct ndm_session *sess, int over_file_mark);
int	simu_flush_weof (struct ndm_session *sess);


#ifdef NDMOS_OPTION_TAPE_SIMULATOR

#define SIMU_GAP_MAGIC		0x0BEEFEE0
#define SIMU_GAP_RT_(a,b,c,d) ((a<<0)+(b<<8)+(c<<16)+(d<<24))
#define SIMU_GAP_RT_BOT		SIMU_GAP_RT_('B','O','T','_')
#define SIMU_GAP_RT_DATA	SIMU_GAP_RT_('D','A','T','A')
#define SIMU_GAP_RT_FILE	SIMU_GAP_RT_('F','I','L','E')
#define SIMU_GAP_RT_EOT		SIMU_GAP_RT_('E','O','T','_')



int
ndmos_tape_initialize (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;

	ta->tape_fd = -1;
	NDMOS_MACRO_ZEROFILL (&ta->tape_state);
	ta->tape_state.error = NDMP9_DEV_NOT_OPEN_ERR;
	ta->tape_state.state = NDMP9_TAPE_STATE_IDLE;

	return 0;
}

ndmp9_error
ndmos_tape_open (struct ndm_session *sess, char *drive_name, int will_write)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	struct stat		st;
	int			read_only, omode;
	int			fd;

	if (ta->tape_fd >= 0) {
		return NDMP9_DEVICE_OPENED_ERR;
	}

	if (*drive_name >= '0' && *drive_name <= '9') {
		fd = atoi(drive_name);
		goto skip_header_check;
	}

	if (stat (drive_name, &st) < 0) {
		return NDMP9_NO_DEVICE_ERR;
	}

	read_only = (st.st_mode & 0222) == 0;

	if (!will_write) {
		omode = 0;
	} else {
		if (read_only)
			return NDMP9_WRITE_PROTECT_ERR;
		omode = 2;		/* ndmp_write means read/write */
	}

	fd = open (drive_name, omode);
	if (fd < 0) {
		return NDMP9_PERMISSION_ERR;
	}

	if (st.st_size == 0) {
		if (will_write) {
			lseek (fd, (off_t)0, 0);
		} else {
			goto skip_header_check;
		}
	}

  skip_header_check:
	ta->tape_fd = fd;
	NDMOS_API_BZERO (ta->drive_name, sizeof ta->drive_name);
	strcpy (ta->drive_name, drive_name);
	bzero (&ta->tape_state, sizeof ta->tape_state);
	ta->tape_state.error = NDMP9_NO_ERR;
	ta->tape_state.state = NDMP9_TAPE_STATE_OPEN;
	ta->tape_state.open_mode =
		will_write ? NDMP9_TAPE_RDWR_MODE : NDMP9_TAPE_READ_MODE;
	ta->tape_state.file_num.valid = NDMP9_VALIDITY_VALID;
	ta->tape_state.soft_errors.valid = NDMP9_VALIDITY_VALID;
	ta->tape_state.block_size.valid = NDMP9_VALIDITY_VALID;
	ta->tape_state.blockno.valid = NDMP9_VALIDITY_VALID;
	ta->tape_state.total_space.valid = NDMP9_VALIDITY_INVALID;
	ta->tape_state.space_remain.valid = NDMP9_VALIDITY_INVALID;

	return NDMP9_NO_ERR;
}

ndmp9_error
ndmos_tape_close (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	simu_flush_weof(sess);

#if 0
	u_long			resid;
	ndmos_tape_mtio (sess, NDMP9_MTIO_REW, 1, &resid);
#endif

	close (ta->tape_fd);
	ta->tape_fd = -1;

	ndmos_tape_initialize (sess);

	return NDMP9_NO_ERR;
}

void
ndmos_tape_sync_state (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;

	if (ta->tape_fd < 0) {
		ta->tape_state.error = NDMP9_DEV_NOT_OPEN_ERR;
		ta->tape_state.state = NDMP9_TAPE_STATE_IDLE;
		ta->tape_state.file_num.valid = NDMP9_VALIDITY_INVALID;
		ta->tape_state.soft_errors.valid = NDMP9_VALIDITY_INVALID;
		ta->tape_state.block_size.valid = NDMP9_VALIDITY_INVALID;
		ta->tape_state.blockno.valid = NDMP9_VALIDITY_INVALID;
		ta->tape_state.total_space.valid = NDMP9_VALIDITY_INVALID;
		ta->tape_state.space_remain.valid = NDMP9_VALIDITY_INVALID;
	} else {
		ta->tape_state.error = NDMP9_NO_ERR;
		if (ta->mover_state.state == NDMP9_MOVER_STATE_ACTIVE)
			ta->tape_state.state = NDMP9_TAPE_STATE_MOVER;
		else
			ta->tape_state.state = NDMP9_TAPE_STATE_OPEN;
		ta->tape_state.file_num.valid = NDMP9_VALIDITY_VALID;
		ta->tape_state.soft_errors.valid = NDMP9_VALIDITY_VALID;
		ta->tape_state.block_size.valid = NDMP9_VALIDITY_VALID;
		ta->tape_state.blockno.valid = NDMP9_VALIDITY_VALID;
		ta->tape_state.total_space.valid = NDMP9_VALIDITY_INVALID;
		ta->tape_state.space_remain.valid = NDMP9_VALIDITY_INVALID;
	}

	return;
}

int
simu_back_one (struct ndm_session *sess, int over_file_mark)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	off_t			cur_pos;

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);
	lseek (ta->tape_fd, 0, 0);
return 0;
}

int
simu_forw_one (struct ndm_session *sess, int over_file_mark)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	off_t			cur_pos;

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);
return 0;

}

int
simu_flush_weof (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;

	if (ta->weof_on_close) {
		/* best effort */
		ndmos_tape_wfm (sess);
	}
	return 0;
}


ndmp9_error
ndmos_tape_mtio (struct ndm_session *sess,
  ndmp9_tape_mtio_op op, u_long count, u_long *resid)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	int			rc;

	*resid = 0;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	
	/* audit for valid op and for tape mode */
	switch (op) {
	case NDMP9_MTIO_FSF:
		return NDMP9_NO_ERR;
		while (*resid > 0) {
			simu_flush_weof(sess);
			rc = simu_forw_one (sess, 1);
			if (rc < 0)
				return NDMP9_IO_ERR;
			if (rc == 0)
				break;
			if (rc == SIMU_GAP_RT_FILE)
				*resid -= 1;
		}
		break;

	case NDMP9_MTIO_BSF:
		return NDMP9_NO_ERR;
		while (*resid > 0) {
			simu_flush_weof(sess);
			rc = simu_back_one (sess, 1);
			if (rc < 0)
				return NDMP9_IO_ERR;
			if (rc == 0)
				break;
			if (rc == SIMU_GAP_RT_FILE)
				*resid -= 1;
		}
		break;

	case NDMP9_MTIO_FSR:
		return NDMP9_NO_ERR;
		while (*resid > 0) {
			simu_flush_weof(sess);
			rc = simu_forw_one (sess, 0);
			if (rc < 0)
				return NDMP9_IO_ERR;
			if (rc == 0)
				break;
			*resid -= 1;
		}
		break;

	case NDMP9_MTIO_BSR:
		return NDMP9_NO_ERR;
		while (*resid > 0) {
			simu_flush_weof(sess);
			rc = simu_back_one (sess, 0);
			if (rc < 0)
				return NDMP9_IO_ERR;
			if (rc == 0)
				break;
			*resid -= 1;
		}
		break;

	case NDMP9_MTIO_REW:
		simu_flush_weof(sess);
		*resid = 0;
		ta->tape_state.file_num.value = 0;
		ta->tape_state.blockno.value = 0;
		//lseek (ta->tape_fd, (off_t)(sizeof (struct simu_gap)), 0);
		lseek (ta->tape_fd, (off_t)0, 0);
		ndmalogf(sess, 0, 7, "NDMP9_MTIO_REW");
		sleep(1);
		break;

	case NDMP9_MTIO_OFF:
		return NDMP9_NO_ERR;
		simu_flush_weof(sess);
		/* Hmmm. */
		break;

	case NDMP9_MTIO_EOF:		/* should be "WFM" write-file-mark */
		return NDMP9_NO_ERR;
		if (!NDMTA_TAPE_IS_WRITABLE(ta)) {
			return NDMP9_PERMISSION_ERR;
		}
		while (*resid > 0) {
			ndmp9_error	err;

			err = ndmos_tape_wfm (sess);
			if (err != NDMP9_NO_ERR)
				return err;

			*resid -= 1;
		}
		break;

	default:
		return NDMP9_ILLEGAL_ARGS_ERR;
	}

	return NDMP9_NO_ERR;
}

ndmp9_error
ndmos_tape_write (struct ndm_session *sess,
  char *buf, u_long count, u_long *done_count)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	off_t			cur_pos;
	ndmp9_error		err;
	u_long			prev_size;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	if (!NDMTA_TAPE_IS_WRITABLE(ta)) {
		return NDMP9_PERMISSION_ERR;
	}

	if (count == 0) {
		/*
		 * NDMPv4 clarification -- a tape read or write with
		 * a count==0 is a no-op. This is undoubtedly influenced
		 * by the SCSI Sequential Access specification which
		 * says much the same thing.
		 */
		*done_count = 0;
		return NDMP9_NO_ERR;
	}

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);
	lseek (ta->tape_fd, cur_pos, 0);

	if (write (ta->tape_fd, buf, count) == count) {
		cur_pos += count;

		prev_size = count;

		ta->tape_state.blockno.value++;

		*done_count = count;

		err = NDMP9_NO_ERR;
	} else {
		err = NDMP9_IO_ERR;
	}


	ftruncate (ta->tape_fd, cur_pos);

	lseek (ta->tape_fd, cur_pos, 0);

	ta->weof_on_close = 1;

	return err;
}

ndmp9_error
ndmos_tape_wfm (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	off_t			cur_pos;
	ndmp9_error		err;

	ta->weof_on_close = 0;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	if (!NDMTA_TAPE_IS_WRITABLE(ta)) {
		return NDMP9_PERMISSION_ERR;
	}

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);

	lseek (ta->tape_fd, cur_pos, 0);
	err = NDMP9_NO_ERR;

	ftruncate (ta->tape_fd, cur_pos);
	lseek (ta->tape_fd, cur_pos, 0);

	return err;
}

ndmp9_error
ndmos_tape_read (struct ndm_session *sess,
  char *buf, u_long count, u_long *done_count)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	int			rc;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	if (count == 0) {
		/*
		 * NDMPv4 clarification -- a tape read or write with
		 * a count==0 is a no-op. This is undoubtedly influenced
		 * by the SCSI Sequential Access specification which
		 * says much the same thing.
		 */

		*done_count = 0;
		return NDMP9_NO_ERR;
	}

	unsigned	nb;

	nb = count;

	rc = read (ta->tape_fd, buf, nb);
	if (rc < 0) {
		return NDMP9_IO_ERR;
	}
	ta->tape_state.blockno.value++;

	*done_count = rc;

	if (rc == 0) {
		return NDMP9_EOF_ERR;
	}
	return NDMP9_NO_ERR;
}

#endif /* NDMOS_OPTION_TAPE_SIMULATOR */

#endif /* !NDMOS_OPTION_NO_TAPE_AGENT */
