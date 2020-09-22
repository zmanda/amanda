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

#include <glib.h>


#ifndef NDMOS_OPTION_NO_CONTROL_AGENT
int
build_job (ref_ndm_job_param_t job)
{
	int			i, rc, n_err;
	char			errbuf[100];

	args_to_job (job);

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
args_to_job (ref_ndm_job_param_t job)
{
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
		args_to_job_backup_env(&job->nlist_tab, &job->env_tab);
		break;

	case NDM_JOB_OP_TOC:
		args_to_job_recover_env(&job->env_tab);
		args_to_job_recover_nlist(&job->nlist_tab);
		if (J_index_file) {
			jndex_doit(&job->nlist_tab);
			jndex_merge_environment(&job->env_tab);
		}
		break;

	case NDM_JOB_OP_EXTRACT:
		args_to_job_recover_env(&job->env_tab);
		args_to_job_recover_nlist(&job->nlist_tab);
		jndex_doit(&job->nlist_tab);
		jndex_merge_environment(&job->env_tab);
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
	job->index_log.deliver = ndmjob_ixlog_deliver;

	/* TAPE agent */
	job->tape_agent  = T_tape_agent;
	job->tape_device = f_tape_device;
	job->record_size = b_bsize * 512;
	job->tape_timeout = o_tape_timeout;
	job->use_eject = o_use_eject;
	job->tape_target = o_tape_scsi;
	job->tape_tcp = o_tape_tcp;

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


static void 
append_global_args (ref_ndm_env_table_t job_envp)
{
    int i;

    ENV_ARRAY_APPEND(job_envp, "HIST", (I_index_file ? "y" : "n") );
    ENV_ARRAY_APPEND(job_envp, "TYPE",  B_bu_type);

    if (U_user) {
        ENV_ARRAY_APPEND(job_envp, "USER",  U_user);
    }

    for (i = 0; i < n_e_exclude_pattern ; i++) {
        ENV_ARRAY_APPEND(job_envp, "EXCLUDE",  e_exclude_pattern[i]);
    }

    if (o_rules) {
        ENV_ARRAY_APPEND(job_envp, "RULES", o_rules);
    }

}

int
args_to_job_backup_env (ref_ndm_nlist_table_t nlist, ref_ndm_env_table_t job_envp)
{
	int		i;

	if (C_chdir) {
            ENV_ARRAY_APPEND(job_envp, "FILESYSTEM", C_chdir);
	}

        append_global_args(job_envp);

        for (i = 0; i < nlist->n_nlist ; i++) {
            ENV_ARRAY_APPEND(job_envp, "FILES", nlist->nlist[i].original_path);
        }

	return job_envp->n_env;
}

int
args_to_job_recover_env (ref_ndm_env_table_t job_envp)
{
	if (C_chdir) {
           ENV_ARRAY_APPEND(job_envp, "PREFIX", C_chdir);
	}

        append_global_args(job_envp);

	/* file_arg[]s are done in nlist[] */

	return job_envp->n_env;
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
args_to_job_recover_nlist (ref_ndm_nlist_table_t nlist)
{
	int		i, prefix_len = 0;

	if (C_chdir) {
            prefix_len = strlen (C_chdir) + 2;
	} 

	for (i = 0; i < nlist->n_nlist ; i++) 
        {
            char *destpath;
            char *buff;
            name_value_t *const pair = &nlist->nlist[i];

            // complete original path 
            normalize_name (pair->original_path);

            // transform destination path (or create from original_path)
            destpath = ( pair->destination_path ? : pair->original_path );
            buff = alloca(strlen(destpath) + prefix_len + 1);

            strcpy(buff, ( C_chdir ? : "" ) );
            strcat(buff, destpath);

            // complete destination path 
            normalize_name (buff);
            if ( pair->destination_path && pair->destination_path != pair->original_path ) {
                free(pair->destination_path);
            }
            pair->destination_path = NDMOS_API_STRDUP(buff);
	}

	return 0;	/* should ALWAYS be 0 */
}


/*
 * Index files are sequentially searched. They can be VERY big.
 * There is a credible effort for efficiency here.
 * Probably lots and lots and lots of room for improvement.
 */

FILE *		jndex_open (void);


int
jndex_doit (ref_ndm_nlist_table_t nlist)
{
	FILE *		fp;
        void *          fpcache = NULL;
	int		rc;

	fp = jndex_open();
	if (!fp) {
		/* error messages already given */
		return -1;
	}

	ndmjob_log (1, "Processing input index (-J%s)", J_index_file);

	if (nlist->n_nlist > 0) {
		rc = ndmfhdb_add_fh_info_to_nlist (fp, nlist, &fpcache);
		if (rc < 0) {
			/* toast one way or another */
		}
	}

	jndex_fetch_post_backup_data_env(fp, &fpcache);
	jndex_fetch_post_backup_media(fp, &fpcache);

	ndmjob_log (1, "Completed input index (-J%s)", J_index_file);

	jndex_tattle(nlist);

	if (jndex_audit_not_found (nlist)) {
		ndmjob_log (1,
			"Warning: Missing index entries, valid file name(s)?");
	}

	jndex_merge_media ();

        ndmfhdb_clear_cache(&fpcache);

	fclose(fp);
	return 0;
}

FILE *
jndex_open (void)
{
	char		buf[256];
	FILE *		fp;
        struct stat     st;
        int             bufsize;
        char            *indexvbuf;

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

        if (fstat(fileno(fp), &st) <0) {
		perror (J_index_file);
		error_byebye ("Can not fstat -J%s input index", J_index_file);
        }

        // load a narrow 100K window if it is very huge.. 
        //       otherwise load the whole thing immediately
        bufsize = ( st.st_size > NDM_INDEX_HOLDING_LIMIT ? NDM_INDEX_PROBE_BUFFER : st.st_size );
        indexvbuf = malloc(bufsize);

        // attmempt to use the max buffer (up to a sane limit)
        if ( ! indexvbuf ) { 
            indexvbuf = malloc(NDM_INDEX_PROBE_BUFFER);
            bufsize = NDM_INDEX_PROBE_BUFFER;
        }

        // try *only* if memory was received...
        if ( indexvbuf ) {
            setvbuf(fp, indexvbuf, _IOFBF, bufsize);
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
jndex_tattle (ref_ndm_nlist_table_t nlist)
{
	char		buf[100];
	int		i;

	for (i = 0; i < n_ji_media; i++) {
		struct ndmmedia *	me = &ji_media[i];

		ndmmedia_to_str (me, buf);
		ndmjob_log (3, "ji me[%d] %s", i, buf);
	}

	for (i = 0; i < ji_env.n_env; i++) {
		env_value_t		*pv = &ji_env.env[i];

		ndmjob_log (3, "ji env[%d] %s=%s", i, pv->name, pv->value);
	}

	for (i = 0; i < nlist->n_nlist ; i++) {
		if (nlist->nlist[i].fh_info.valid) {
			ndmjob_log (3, "ji fil[%d] fi=%lld %s",
				i, nlist->nlist[i].fh_info.value, nlist->nlist[i].original_path);
		} else {
			ndmjob_log (3, "ji fil[%d] not-found %s",
				i, nlist->nlist[i].original_path);
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
jndex_audit_not_found (ref_ndm_nlist_table_t nlist)
{
	int		i;
	int		not_found = 0;

	for (i = 0; i < nlist->n_nlist ; i++) {
		if (!nlist->nlist[i].fh_info.valid) {
			ndmjob_log (0, "No index entry for %s", nlist->nlist[i].original_path);
			not_found++;
		}
	}

	return not_found;
}

int
jndex_merge_environment (ref_ndm_env_table_t job_envp) 
{
	int		i;

	for (i = 0; i < ji_env.n_env; i++) {
		if (strcmp(ji_env.env[i].name, "FILESYSTEM") != 0 &&
		    strcmp(ji_env.env[i].name, "PREFIX") != 0 &&
		    strcmp(ji_env.env[i].name, "HIST") != 0 &&
		    strcmp(ji_env.env[i].name, "TYPE") != 0) {
                       ENV_ARRAY_APPEND(job_envp, ji_env.env[i].name, ji_env.env[i].value);
		}
	}

	return 0;
}

int
jndex_fetch_post_backup_data_env (FILE *fp, void **fpcachep)
{
	int		rc;
	char		buf[512];
	char *		p;
	char *		q;

	rc = ndmbstf_first (fp, "DE ", buf, sizeof buf, fpcachep);
	if (rc <= 0) {
		return rc;	/* error or not found */
	}

	/* DE HIST=Yes */
	while (buf[0] == 'D' && buf[1] == 'E' && buf[2] == ' ') {
		p = &buf[2];
		while (*p == ' ') p++;

		if (!strchr (p, '=')) {
			goto malformed;
		}

		p = NDMOS_API_STRDUP (p);
		q = strchr (p, '=');
		if (!q) {
			goto malformed;
		}
		*q++ = 0;

                ENV_ARRAY_APPEND(&ji_env, p, q);

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
jndex_fetch_post_backup_media (FILE *fp, void **fpcachep)
{
	int		rc;
	char		buf[512];

	rc = ndmbstf_first (fp, "CM ", buf, sizeof buf, fpcachep);
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
