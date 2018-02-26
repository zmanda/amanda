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
 *	CONTROL agent entry point.
 */


#include "ndmagents.h"


#ifndef NDMOS_OPTION_NO_CONTROL_AGENT


int
ndmca_control_agent (struct ndm_session *sess)
{
	struct ndm_job_param *	job = &sess->control_acb.job;
	int			rc;

	switch (job->operation) {
	default:
		ndmalogf (sess, 0, 0, "Job operation invalid");
		rc = -1;
		break;

	case NDM_JOB_OP_INIT_LABELS:
		rc = ndmca_op_init_labels (sess);
		break;

	case NDM_JOB_OP_LIST_LABELS:
		rc = ndmca_op_list_labels (sess);
		break;

	case NDM_JOB_OP_BACKUP:
		rc = ndmca_op_create_backup (sess);
		break;

	case NDM_JOB_OP_EXTRACT:
		rc = ndmca_op_recover_files (sess);
		break;

	case NDM_JOB_OP_TOC:
		rc = ndmca_op_recover_fh (sess);
		break;

	case NDM_JOB_OP_REMEDY_ROBOT:
		rc = ndmca_op_robot_remedy (sess);
		break;

	case NDM_JOB_OP_QUERY_AGENTS:
		rc = ndmca_op_query (sess);
		break;

	case NDM_JOB_OP_TEST_TAPE:
		rc = ndmca_op_test_tape (sess);
		break;

	case NDM_JOB_OP_TEST_MOVER:
		rc = ndmca_op_test_mover (sess);
		break;

	case NDM_JOB_OP_TEST_DATA:
		rc = ndmca_op_test_data (sess);
		break;

	case NDM_JOB_OP_REWIND_TAPE:
		rc = ndmca_op_rewind_tape (sess);
		break;

	case NDM_JOB_OP_EJECT_TAPE:
		rc = ndmca_op_eject_tape (sess);
		break;

	case NDM_JOB_OP_MOVE_TAPE:
		rc = ndmca_op_move_tape (sess);
		break;

	case NDM_JOB_OP_LOAD_TAPE:
		rc = ndmca_op_load_tape (sess);
		break;

	case NDM_JOB_OP_UNLOAD_TAPE:
		rc = ndmca_op_unload_tape (sess);
		break;

	case NDM_JOB_OP_IMPORT_TAPE:
		rc = ndmca_op_import_tape (sess);
		break;

	case NDM_JOB_OP_EXPORT_TAPE:
		rc = ndmca_op_export_tape (sess);
		break;

	case NDM_JOB_OP_INIT_ELEM_STATUS:
		rc = ndmca_op_init_elem_status (sess);
		break;
	}

	return rc;
}
#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */
