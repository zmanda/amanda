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


#include "ndmjob.h"


#ifndef NDMOS_OPTION_NO_CONTROL_AGENT


int
build_job (void)
{
	struct ndm_job_param *	job = &the_job;
	int			i, rc, n_err;
	char			errbuf[100];

	NDMOS_MACRO_ZEROFILL(job);

	args_to_job ();

	ndma_job_auto_adjust (job);

	if (o_rules)
		apply_rules (job, o_rules);

	i = n_err = 0;
	do {
		rc = ndma_job_audit (job, errbuf, i);
		if (rc > n_err || rc < 0) {
			ndmjob_log (0, "error: %s", errbuf);
		}
		n_err = rc;
	} while (i++ < n_err);

	if (n_err) {
		error_byebye ("can't proceed");
		/* no return */
	}

	return 0;
}


int
args_to_job (void)
{
	struct ndm_job_param *	job = &the_job;
	int			i;

	switch (the_mode) {
	case NDM_JOB_OP_QUERY_AGENTS:
	case NDM_JOB_OP_INIT_LABELS:
	case NDM_JOB_OP_LIST_LABELS:
	case NDM_JOB_OP_REMEDY_ROBOT:
	case NDM_JOB_OP_TEST_TAPE:
	case NDM_JOB_OP_TEST_MOVER:
	case NDM_JOB_OP_TEST_DATA:
	case NDM_JOB_OP_REWIND_TAPE:
	case NDM_JOB_OP_EJECT_TAPE:
	case NDM_JOB_OP_MOVE_TAPE:
	case NDM_JOB_OP_IMPORT_TAPE:
	case NDM_JOB_OP_EXPORT_TAPE:
	case NDM_JOB_OP_LOAD_TAPE:
	case NDM_JOB_OP_UNLOAD_TAPE:
	case NDM_JOB_OP_INIT_ELEM_STATUS:
		break;

	case NDM_JOB_OP_BACKUP:
		args_to_job_backup_env();
		break;

	case NDM_JOB_OP_TOC:
		args_to_job_recover_env();
		args_to_job_recover_nlist();
		if (J_index_file)
			jndex_doit();
		break;

	case NDM_JOB_OP_EXTRACT:
		args_to_job_recover_env();
		args_to_job_recover_nlist();
		jndex_doit();
		break;

	case 'D':		/* -o daemon */
		return 0;

	default:
		printf ("mode -%c not implemented yet\n", the_mode);
		break;
	}
	job->operation = the_mode;

	/* DATA agent */
	job->data_agent  = D_data_agent;
	job->bu_type = B_bu_type;
	for (i = 0; i < nn_E_environment; i++)
		job->env_tab.env[i] = E_environment[i];
	job->env_tab.n_env = nn_E_environment;
	if (the_mode == NDM_JOB_OP_EXTRACT || the_mode == NDM_JOB_OP_TOC) {
	        for (i = 0; (i < n_file_arg) && (i < NDM_MAX_NLIST); i++) {
			job->nlist_tab.nlist[i] = nlist[i];
			job->nlist_tab.nlist_new[i] = nlist_new[i];
			job->nlist_tab.n_nlist = i + 1;
		}
	}
	job->index_log.deliver = ndmjob_ixlog_deliver;

	/* TAPE agent */
	job->tape_agent  = T_tape_agent;
	job->tape_device = f_tape_device;
	job->record_size = b_bsize * 512;
	job->tape_timeout = o_tape_timeout;
	job->use_eject = o_use_eject;
	job->tape_target = o_tape_scsi;

	/* ROBOT agent */
	job->robot_agent = R_robot_agent;
	job->robot_target = r_robot_target;
	job->robot_timeout = o_robot_timeout;
	if (o_tape_addr >= 0) {
		job->drive_addr = o_tape_addr;
		job->drive_addr_given = 1;
	}
	if (o_from_addr >= 0) {
		job->from_addr = o_from_addr;
		job->from_addr_given = 1;
	}
	if (o_to_addr >= 0) {
		job->to_addr = o_to_addr;
		job->to_addr_given = 1;
	}
	if (ROBOT_GIVEN())
		job->have_robot = 1;

	/* media */
	for (i = 0; i < n_m_media; i++)
		job->media_tab.media[i] = m_media[i];
	job->media_tab.n_media = n_m_media;

	return 0;
}


int
args_to_job_backup_env (void)
{
	int		n_env = n_E_environment;
	int		i;

	if (C_chdir) {
		E_environment[n_env].name = "FILESYSTEM";
		E_environment[n_env].value = C_chdir;
		n_env++;
	}

	E_environment[n_env].name = "HIST";
	E_environment[n_env].value = I_index_file ? "y" : "n";
	n_env++;

	E_environment[n_env].name = "TYPE";
	E_environment[n_env].value = B_bu_type;
	n_env++;

	if (U_user) {
		E_environment[n_env].name = "USER";
		E_environment[n_env].value = U_user;
		n_env++;
	}

	for (i = 0; (i < n_e_exclude_pattern) && (n_env < NDM_MAX_ENV-2); i++) {
		E_environment[n_env].name = "EXCLUDE";
		E_environment[n_env].value = e_exclude_pattern[i];
		n_env++;
	}
	for (i = 0; (i < n_file_arg) && (n_env < NDM_MAX_ENV-1); i++) {
		E_environment[n_env].name = "FILES";
		E_environment[n_env].value = file_arg[i];
		n_env++;
	}

	if (o_rules) {
		E_environment[n_env].name = "RULES";
		E_environment[n_env].value = o_rules;
	}

	nn_E_environment = n_env;

	return n_env;
}

int
args_to_job_recover_env (void)
{
	int		n_env = n_E_environment;
	int		i;

	if (C_chdir) {
		E_environment[n_env].name = "PREFIX";
		E_environment[n_env].value = C_chdir;
		n_env++;
	}

	E_environment[n_env].name = "HIST";
	E_environment[n_env].value = I_index_file ? "y" : "n";
	n_env++;

	E_environment[n_env].name = "TYPE";
	E_environment[n_env].value = B_bu_type;
	n_env++;

	if (U_user) {
		E_environment[n_env].name = "USER";
		E_environment[n_env].value = U_user;
		n_env++;
	}

	for (i = 0; i < n_e_exclude_pattern; i++) {
		E_environment[n_env].name = "EXCLUDE";
		E_environment[n_env].value = e_exclude_pattern[i];
		n_env++;
	}

	if (o_rules) {
		E_environment[n_env].name = "RULES";
		E_environment[n_env].value = o_rules;
	}

	nn_E_environment = n_env;

	/* file_arg[]s are done in nlist[] */

	jndex_merge_environment ();

	return nn_E_environment;
}

void
normalize_name (char *name)
{
	char *		p = name;

	while (*p) {
		if (*p == '/' && p[1] == '/') {
			strcpy (p, p+1);
			continue;
		}
		if (p[0] == '/' && p[1] == '.' && (p[2] == '/' || p[2] == 0)) {
			strcpy (p, p+2);
			continue;
		}

		p++;
	}
}

int
args_to_job_recover_nlist (void)
{
	int		not_found = 0;
	int		i, prefix_len, len;
	char *		dest;

	if (C_chdir) {
		prefix_len = strlen (C_chdir) + 2;
	} else {
		prefix_len = 0;
	}

	for (i = 0; (i < n_file_arg) && (i < NDM_MAX_NLIST); i++) {
	    if (file_arg_new[i]) {
		len = strlen (file_arg_new[i]) + prefix_len + 1;

		dest = NDMOS_API_MALLOC (len);
		*dest = 0;
		if (C_chdir) {
			strcpy (dest, C_chdir);
		}
		if (file_arg_new[i][0] != '/') {
			strcat (dest, "/");
		}
		strcat (dest, file_arg_new[i]);

		normalize_name (file_arg_new[i]);
		normalize_name (file_arg[i]);
		normalize_name (dest);

		nlist[i].original_path = file_arg[i];
		nlist[i].destination_path = dest;
	    } else {
		len = strlen (file_arg[i]) + prefix_len + 1;

		dest = NDMOS_API_MALLOC (len);
		*dest = 0;
		if (C_chdir) {
			strcpy (dest, C_chdir);
		}
		if (file_arg[i][0] != '/') {
			strcat (dest, "/");
		}

		strcat (dest, file_arg[i]);

		normalize_name (file_arg[i]);
		normalize_name (dest);

		nlist[i].original_path = file_arg[i];
		nlist[i].destination_path = dest;
	    }
	}

	return not_found;	/* should ALWAYS be 0 */
}


/*
 * Index files are sequentially searched. They can be VERY big.
 * There is a credible effort for efficiency here.
 * Probably lots and lots and lots of room for improvement.
 */

FILE *		jndex_open (void);


int
jndex_doit (void)
{
	FILE *		fp;
	int		rc;

	fp = jndex_open();
	if (!fp) {
		/* error messages already given */
		return -1;
	}

	ndmjob_log (1, "Processing input index (-J%s)", J_index_file);

	if (n_file_arg > 0) {
		rc = ndmfhdb_add_fh_info_to_nlist (fp, nlist, n_file_arg);
		if (rc < 0) {
			/* toast one way or another */
		}
	}

	jndex_fetch_post_backup_data_env(fp);
	jndex_fetch_post_backup_media(fp);

	jndex_tattle();

	if (jndex_audit_not_found ()) {
		ndmjob_log (1,
			"Warning: Missing index entries, valid file name(s)?");
	}

	jndex_merge_media ();

	return 0;
}

FILE *
jndex_open (void)
{
	char		buf[256];
	FILE *		fp;

	if (!J_index_file) {
		/* Hmmm. */
		ndmjob_log (1, "Warning: No -J input index?");
		return 0;
	}

	ndmjob_log (1, "Reading input index (-I%s)", J_index_file);
	fp = fopen(J_index_file, "r");
	if (!fp) {
		perror (J_index_file);
		error_byebye ("Can not open -J%s input index", J_index_file);
		/* no return */
	}

	if (fgets (buf, sizeof buf, fp) == NULL) {
		fclose (fp);
		error_byebye ("Failed read 1st line of -J%s", J_index_file);
		/* no return */
	}

	if (strcmp (buf, "##ndmjob -I\n") != 0) {
		fclose (fp);
		error_byebye ("Bad 1st line in -J%s", J_index_file);
		/* no return */
	}

	if (fgets (buf, sizeof buf, fp) == NULL) {
		fclose (fp);
		error_byebye ("Failed read 2nd line of -J%s", J_index_file);
		/* no return */
	}

	if (strcmp (buf, "##ndmjob -J\n") != 0) {
		fclose (fp);
		error_byebye ("Bad 2nd line in -J%s", J_index_file);
		/* no return */
	}

	ndmjob_log (2, "Opened index (-J%s)", J_index_file);

	return fp;
}


int
jndex_tattle (void)
{
	char		buf[100];
	int		i;

	for (i = 0; i < n_ji_media; i++) {
		struct ndmmedia *	me = &ji_media[i];

		ndmmedia_to_str (me, buf);
		ndmjob_log (3, "ji me[%d] %s", i, buf);
	}

	for (i = 0; i < n_ji_environment; i++) {
		ndmp9_pval *		pv = &ji_environment[i];

		ndmjob_log (3, "ji env[%d] %s=%s", i, pv->name, pv->value);
	}

	for (i = 0; (i < n_file_arg) && (i < NDM_MAX_NLIST); i++) {
		if (nlist[i].fh_info.valid) {
			ndmjob_log (3, "ji fil[%d] fi=%lld %s",
				i, nlist[i].fh_info.value, file_arg[i]);
		} else {
			ndmjob_log (3, "ji fil[%d] not-found %s",
				i, file_arg[i]);
		}
	}

	return 0;
}

int
jndex_merge_media (void)
{
	struct ndmmedia *	me;
	struct ndmmedia *	jme;
	int			i, j;

	for (j = 0; j < n_ji_media; j++) {
		jme = &ji_media[j];

		if (! jme->valid_label)
			continue;	/* can't match it up */

		for (i = 0; i < n_m_media; i++) {
			me = &m_media[i];

			if (! me->valid_label)
				continue;	/* can't match it up */

			if (strcmp (jme->label, me->label) != 0)
				continue;

			if (!jme->valid_slot &&  me->valid_slot) {
				jme->slot_addr = me->slot_addr;
				jme->valid_slot = 1;
			}
		}
	}

	for (i = 0; i < n_ji_media; i++) {
		m_media[i] = ji_media[i];
	}
	n_m_media = i;

	ndmjob_log (3, "After merging input -J index with -m entries");
	for (i = 0; i < n_m_media; i++) {
		char		buf[40];

		me = &m_media[i];
		ndmmedia_to_str (me, buf);
		ndmjob_log (3, "%d: -m %s", i+1, buf);
	}

	return 0;
}

int
jndex_audit_not_found (void)
{
	int		i;
	int		not_found = 0;

	for (i = 0; (i < n_file_arg) && (i < NDM_MAX_NLIST); i++) {
		if (!nlist[i].fh_info.valid) {
			ndmjob_log (0, "No index entry for %s", file_arg[i]);
			not_found++;
		}
	}

	return not_found;
}

int
jndex_merge_environment (void)
{
	int		i;

	for (i = 0; i < n_ji_environment; i++) {
		E_environment[nn_E_environment++] = ji_environment[i];
	}

	return 0;
}

int
jndex_fetch_post_backup_data_env (FILE *fp)
{
	int		rc;
	char		buf[512];
	char *		p;
	char *		q;

	rc = ndmbstf_first (fp, "DE ", buf, sizeof buf);
	if (rc <= 0) {
		return rc;	/* error or not found */
	}

	/* DE HIST=Yes */
	while (buf[0] == 'D' && buf[1] == 'E' && buf[2] == ' ') {
		if (n_ji_environment >= NDM_MAX_ENV) {
			goto overflow;
		}

		p = &buf[2];
		while (*p == ' ') p++;

		if (!strchr (p, '=')) {
			goto malformed;
		}

		p = NDMOS_API_STRDUP (p);
		q = strchr (p, '=');
		*q++ = 0;

		ji_environment[n_ji_environment].name = p;
		ji_environment[n_ji_environment].value = q;

		n_ji_environment++;

		rc = ndmbstf_getline (fp, buf, sizeof buf);
		if (rc <= 0) {
			break;
		}
		continue;

  malformed:
		ndmjob_log (1, "Malformed in -J%s: %s", J_index_file, buf);
		continue;

  overflow:
		ndmjob_log (1, "Overflow in -J%s: %s", J_index_file, buf);
	}

	return 0;
}

int
jndex_fetch_post_backup_media (FILE *fp)
{
	int		rc;
	char		buf[512];

	rc = ndmbstf_first (fp, "CM ", buf, sizeof buf);
	if (rc <= 0) {
		return rc;	/* error or not found */
	}

	/* CM 01 T103/10850K */
	while (buf[0] == 'C' && buf[1] == 'M' && buf[2] == ' ') {
		struct ndmmedia *	me;

		if (n_ji_media >= NDM_MAX_MEDIA) {
			goto overflow;
		}

		me = &ji_media[n_ji_media];
		if (ndmmedia_from_str (me, &buf[6])) {
			goto malformed;
		}
		n_ji_media++;

		rc = ndmbstf_getline (fp, buf, sizeof buf);
		if (rc <= 0) {
			break;
		}
		continue;

  malformed:
		ndmjob_log (1, "Malformed in -J%s: %s", J_index_file, buf);
		continue;

  overflow:
		ndmjob_log (1, "Overflow in -J%s: %s", J_index_file, buf);
	}

	return 0;
}

#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */
