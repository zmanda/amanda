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

struct simu_gap {
	u_long		magic;
	u_long		rectype;
	u_long		prev_size;
	u_long		size;
};

#define SIMU_GAP_MAGIC		0x0BEEFEE0
#define SIMU_GAP_RT_(a,b,c,d) ((a<<0)+(b<<8)+(c<<16)+(d<<24))
#define SIMU_GAP_RT_BOT		SIMU_GAP_RT_('B','O','T','_')
#define SIMU_GAP_RT_DATA	SIMU_GAP_RT_('D','A','T','A')
#define SIMU_GAP_RT_FILE	SIMU_GAP_RT_('F','I','L','E')
#define SIMU_GAP_RT_EOT		SIMU_GAP_RT_('E','O','T','_')

/* send logical EOM with a bit less than 2 32k blocks left (due to SIMU_GAPs) */
#define TAPE_SIM_LOGICAL_EOM	32768*2

/* we sneak a peek at this global variable - probably not the best way, but
 * it works */
extern off_t o_tape_limit;

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

static int
touch_tape_lockfile(char *drive_name)
{
    char *lockfile_name;
    int fd;

    lockfile_name = g_strdup_printf("%s.lck", drive_name);
    if ((fd = open(lockfile_name, O_CREAT|O_EXCL, 0666)) < 0) {
	g_free(lockfile_name);
	return -1;
    }

    close(fd);
    g_free(lockfile_name);
    return 0;
}

static void
unlink_tape_lockfile(char *drive_name)
{
    char *lockfile_name;

    lockfile_name = g_strdup_printf("%s.lck", drive_name);
    unlink(lockfile_name);
    g_free(lockfile_name);
}

ndmp9_error
ndmos_tape_open (struct ndm_session *sess, char *drive_name, int will_write)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	struct simu_gap		gap;
	struct stat		st;
	int			read_only, omode;
	int			rc, fd;
	char			*pos_symlink_name;
	char			pos_buf[32];
	off_t			pos = -1;

        if (ta->tape_fd >= 0) {
                ndma_send_logmsg(sess, NDMP9_LOG_ERROR, sess->plumb.control,
                         "device simulator is already open");
                return NDMP9_DEVICE_OPENED_ERR;
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

	if (touch_tape_lockfile(drive_name) < 0)
	    return NDMP9_DEVICE_BUSY_ERR;

	fd = open (drive_name, omode);
	if (fd < 0) {
		return NDMP9_PERMISSION_ERR;
	}

	pos_symlink_name = g_strdup_printf("%s.pos", drive_name);

	if (st.st_size == 0) {
		remove (pos_symlink_name);
		if (will_write) {
			gap.magic = SIMU_GAP_MAGIC;
			gap.rectype = SIMU_GAP_RT_BOT;
			gap.size = 0;
			gap.prev_size = 0;
			if (write (fd, &gap, sizeof gap) < (int)sizeof gap) {
			    close(fd);
			    return NDMP9_IO_ERR;
			}

			gap.rectype = SIMU_GAP_RT_EOT;
			if (write (fd, &gap, sizeof gap) < (int)sizeof gap) {
			    close(fd);
			    return NDMP9_IO_ERR;
			}
			lseek (fd, (off_t)0, 0);
		} else {
			goto skip_header_check;
		}
	}

	rc = read (fd, &gap, sizeof gap);
	if (rc != sizeof gap) {
		close (fd);
		return NDMP9_NO_TAPE_LOADED_ERR;
	}

#if 1
	if (gap.magic != SIMU_GAP_MAGIC) {
		close (fd);
		return NDMP9_IO_ERR;
	}
#else
	if (gap.magic != SIMU_GAP_MAGIC
	 || gap.rectype != SIMU_GAP_RT_BOT
	 || gap.size != 0) {
		close (fd);
		return NDMP9_IO_ERR;
	}
#endif

	rc = readlink (pos_symlink_name, pos_buf, sizeof pos_buf);
	if (rc > 0) {
		pos_buf[sizeof pos_buf - 1] = 0;
		if (rc < sizeof pos_buf)
		    pos_buf[rc] = 0;
		pos = strtol (pos_buf, 0, 0);
		lseek (fd, pos, 0);
		rc = read (fd, &gap, sizeof gap);
		if (rc == sizeof gap && gap.magic == SIMU_GAP_MAGIC) {
		} else {
			pos = sizeof gap;
		}
		lseek (fd, pos, 0);
	}

  skip_header_check:
	remove (pos_symlink_name);
	g_free(pos_symlink_name);

	ta->tape_fd = fd;
	NDMOS_API_BZERO (ta->drive_name, sizeof ta->drive_name);
	g_strlcpy (ta->drive_name, drive_name, sizeof ta->drive_name);
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

	ta->sent_leom = 0;
	if (o_tape_limit) {
	    g_assert(o_tape_limit > st.st_size);

	    ta->tape_state.total_space.valid = NDMP9_VALIDITY_VALID;
	    ta->tape_state.total_space.value = o_tape_limit;
	    ta->tape_state.space_remain.valid = NDMP9_VALIDITY_VALID;
	    ta->tape_state.space_remain.value = o_tape_limit - st.st_size;
	}

	return NDMP9_NO_ERR;
}

ndmp9_error
ndmos_tape_close (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	off_t			cur_pos;

	/* TODO this is not called on an EOF from the DMA, so the lockfile
	 * will remain, although the spec says the tape service should be
	 * automatically closed */

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	simu_flush_weof(sess);

#if 0
	u_long			resid;
	ndmos_tape_mtio (sess, NDMP9_MTIO_REW, 1, &resid);
#endif

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);
	if (cur_pos != -1) {
		char		*pos_symlink_name;
		char		pos_buf[32];

		pos_symlink_name = g_strdup_printf("%s.pos", ta->drive_name);
		sprintf (pos_buf, "%ld", (long) cur_pos);
		if (symlink (pos_buf, pos_symlink_name) < 0) {
		    ; /* ignore error during close */
		}
		g_free(pos_symlink_name);
	}

	close (ta->tape_fd);
	ta->tape_fd = -1;

	unlink_tape_lockfile(ta->drive_name);

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
	}

	return;
}

int
simu_back_one (struct ndm_session *sess, int over_file_mark)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	struct simu_gap		gap;
	off_t			cur_pos;
	off_t			new_pos;
	int			rc;

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);

	rc = read (ta->tape_fd, &gap, sizeof gap);
	if (rc != sizeof gap || gap.magic != SIMU_GAP_MAGIC)
		goto bail_out;

	new_pos = cur_pos;
	new_pos -= sizeof gap + gap.prev_size;

	ta->sent_leom = 0;

	/*
	 * This is the new position. We need to update simu_prev_gap.
	 */

	lseek (ta->tape_fd, new_pos, 0);

	rc = read (ta->tape_fd, &gap, sizeof gap);
	if (rc != sizeof gap || gap.magic != SIMU_GAP_MAGIC)
		goto bail_out;

	switch (gap.rectype) {
	case SIMU_GAP_RT_BOT:
		/* can't actually back up to this, but update stuff */
		ta->tape_state.file_num.value = 0;
		ta->tape_state.blockno.value = 0;
		/* cur_pos is now just right */
		return 0;		/* can't back up */

	case SIMU_GAP_RT_EOT:
		/* this just isn't suppose to happen */
		goto bail_out;

	case SIMU_GAP_RT_DATA:
		/* this is always OK */
		if (ta->tape_state.blockno.value > 0)
			ta->tape_state.blockno.value--;
		lseek (ta->tape_fd, new_pos, 0);
		return SIMU_GAP_RT_DATA;

	case SIMU_GAP_RT_FILE:
		ta->tape_state.blockno.value = 0;
		if (!over_file_mark) {
			lseek (ta->tape_fd, cur_pos, 0);
			return 0;
		}
		if (ta->tape_state.file_num.value > 0)
			ta->tape_state.file_num.value--;
		lseek (ta->tape_fd, new_pos, 0);
		return SIMU_GAP_RT_FILE;

	default:
		/* this just isn't suppose to happen */
		goto bail_out;
	}

  bail_out:
	lseek (ta->tape_fd, cur_pos, 0);
	return -1;
}

int
simu_forw_one (struct ndm_session *sess, int over_file_mark)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	struct simu_gap		gap;
	off_t			cur_pos;
	off_t			new_pos;
	int			rc;

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);

	rc = read (ta->tape_fd, &gap, sizeof gap);
	if (rc != sizeof gap || gap.magic != SIMU_GAP_MAGIC)
		goto bail_out;

	ta->sent_leom = 0;

	new_pos = cur_pos;
	new_pos += gap.size + sizeof gap;

	switch (gap.rectype) {
	case SIMU_GAP_RT_BOT:
		/* this just isn't suppose to happen */
		goto bail_out;

	case SIMU_GAP_RT_EOT:
		lseek (ta->tape_fd, cur_pos, 0);
		return 0;	/* can't go forward */

	case SIMU_GAP_RT_DATA:
		/* this is always OK */
		ta->tape_state.blockno.value++;
		lseek (ta->tape_fd, new_pos, 0);
		return SIMU_GAP_RT_DATA;

	case SIMU_GAP_RT_FILE:
		if (!over_file_mark) {
			lseek (ta->tape_fd, cur_pos, 0);
			return 0;
		}
		ta->tape_state.blockno.value = 0;
		ta->tape_state.file_num.value++;
		/* cur_pos is just right */
		return SIMU_GAP_RT_FILE;

	default:
		/* this just isn't suppose to happen */
		goto bail_out;
	}

  bail_out:
	lseek (ta->tape_fd, cur_pos, 0);
	return -1;
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

	*resid = count;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	/* audit for valid op and for tape mode */
	switch (op) {
	case NDMP9_MTIO_FSF:
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
		lseek (ta->tape_fd, (off_t)(sizeof (struct simu_gap)), 0);
		break;

	case NDMP9_MTIO_OFF:
		simu_flush_weof(sess);
		/* Hmmm. */
		break;

	case NDMP9_MTIO_EOF:		/* should be "WFM" write-file-mark */
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
	int			rc;
	struct simu_gap		gap;
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

	if (o_tape_limit) {
	    /* if cur_pos is past LEOM, but we haven't sent NDMP9_EOM_ERR yet,
	     * then do so now */
	    if (!ta->sent_leom && cur_pos > o_tape_limit - TAPE_SIM_LOGICAL_EOM) {
		ta->sent_leom = 1;
		return NDMP9_EOM_ERR;
	    }

	    /* if this write will put us over PEOM, then send NDMP9_IO_ERR */
	    if ((off_t)(cur_pos + sizeof gap + count) > o_tape_limit) {
		return NDMP9_IO_ERR;
	    }
	}

	rc = read (ta->tape_fd, &gap, sizeof gap);
	if (rc != sizeof gap
	 || gap.magic != SIMU_GAP_MAGIC) {
		lseek (ta->tape_fd, cur_pos, 0);
		return NDMP9_IO_ERR;
	}

	prev_size = gap.prev_size;

	gap.magic = SIMU_GAP_MAGIC;
	gap.rectype = SIMU_GAP_RT_DATA;
	gap.prev_size = prev_size;
	gap.size = count;

	lseek (ta->tape_fd, cur_pos, 0);

	if (write (ta->tape_fd, &gap, sizeof gap) == sizeof gap
	 && (u_long)write (ta->tape_fd, buf, count) == count) {
		cur_pos += count + sizeof gap;

		prev_size = count;

		ta->tape_state.blockno.value++;

		*done_count = count;

		err = NDMP9_NO_ERR;
	} else {
		err = NDMP9_IO_ERR;
	}


	if (ftruncate (ta->tape_fd, cur_pos) < 0)
	    return NDMP9_IO_ERR;

	lseek (ta->tape_fd, cur_pos, 0);

	gap.rectype = SIMU_GAP_RT_EOT;
	gap.size = 0;
	gap.prev_size = prev_size;

	if (write (ta->tape_fd, &gap, sizeof gap) < (int)sizeof gap)
	    return NDMP9_IO_ERR;
	lseek (ta->tape_fd, cur_pos, 0);

	if (o_tape_limit) {
	    ta->tape_state.space_remain.value = o_tape_limit - cur_pos;
	}

	ta->weof_on_close = 1;

	return err;
}

ndmp9_error
ndmos_tape_wfm (struct ndm_session *sess)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	int			rc;
	struct simu_gap		gap;
	off_t			cur_pos;
	ndmp9_error		err;
	u_long			prev_size;

	ta->weof_on_close = 0;

	if (ta->tape_fd < 0) {
		return NDMP9_DEV_NOT_OPEN_ERR;
	}

	if (!NDMTA_TAPE_IS_WRITABLE(ta)) {
		return NDMP9_PERMISSION_ERR;
	}

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);

	if (o_tape_limit) {
	    /* note: filemarks *never* trigger NDMP9_EOM_ERR */

	    /* if this write will put us over PEOM, then send NDMP9_IO_ERR */
	    if ((off_t)(cur_pos + sizeof gap) > o_tape_limit) {
		return NDMP9_IO_ERR;
	    }
	}

	rc = read (ta->tape_fd, &gap, sizeof gap);
	if (rc != sizeof gap
	 || gap.magic != SIMU_GAP_MAGIC) {
		lseek (ta->tape_fd, cur_pos, 0);
		return NDMP9_IO_ERR;
	}

	prev_size = gap.prev_size;

	gap.magic = SIMU_GAP_MAGIC;
	gap.rectype = SIMU_GAP_RT_FILE;
	gap.prev_size = prev_size;
	gap.size = 0;

	lseek (ta->tape_fd, cur_pos, 0);

	if (write (ta->tape_fd, &gap, sizeof gap) == sizeof gap) {

		cur_pos += sizeof gap;

		prev_size = 0;

		ta->tape_state.file_num.value++;
		ta->tape_state.blockno.value = 0;

		err = NDMP9_NO_ERR;
	} else {
		err = NDMP9_IO_ERR;
	}

	if (ftruncate (ta->tape_fd, cur_pos) < 0)
	    return NDMP9_IO_ERR;
	lseek (ta->tape_fd, cur_pos, 0);

	gap.rectype = SIMU_GAP_RT_EOT;
	gap.size = 0;
	gap.prev_size = prev_size;

	if (write (ta->tape_fd, &gap, sizeof gap) < (int)sizeof gap)
		return NDMP9_IO_ERR;
	lseek (ta->tape_fd, cur_pos, 0);

	if (o_tape_limit) {
	    ta->tape_state.space_remain.value = o_tape_limit - cur_pos;
	}

	return err;
}

ndmp9_error
ndmos_tape_read (struct ndm_session *sess,
  char *buf, u_long count, u_long *done_count)
{
	struct ndm_tape_agent *	ta = &sess->tape_acb;
	int			rc;
	struct simu_gap		gap;
	off_t			cur_pos;

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

	cur_pos = lseek (ta->tape_fd, (off_t)0, 1);

	rc = read (ta->tape_fd, &gap, sizeof gap);
	if (rc != sizeof gap
	 || gap.magic != SIMU_GAP_MAGIC) {
		lseek (ta->tape_fd, cur_pos, 0);
		return NDMP9_IO_ERR;
	}

	if (gap.rectype == SIMU_GAP_RT_DATA) {
		unsigned	nb;

		nb = count;
		if (nb > gap.size)
			nb = gap.size;

		rc = read (ta->tape_fd, buf, nb);
		if (rc != (int)nb) {
			lseek (ta->tape_fd, cur_pos, 0);
			return NDMP9_IO_ERR;
		}

		if (gap.size != nb) {
			cur_pos += sizeof gap + gap.size;
			lseek (ta->tape_fd, cur_pos, 0);
		}

		ta->tape_state.blockno.value++;

		*done_count = nb;
	} else {
		/* all other record types are interpretted as EOF */
		lseek (ta->tape_fd, cur_pos, 0);
		*done_count = 0;
		return NDMP9_EOF_ERR;
	}
	return NDMP9_NO_ERR;
}

#endif /* NDMOS_OPTION_TAPE_SIMULATOR */

#endif /* !NDMOS_OPTION_NO_TAPE_AGENT */
