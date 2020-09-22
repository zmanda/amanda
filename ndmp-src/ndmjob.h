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
#pragma once

/*
 * Project:  NDMJOB
 * Ident:    $Id: $
 *
 * Description:
 *
 */

#include "ndmagents.h"
#include "ndmprotocol.h"
#include "ndmlib.h"

#define NDM_INDEX_HOLDING_LIMIT      5*1024*1024
#define NDM_INDEX_PROBE_BUFFER       100*1024

#ifndef GLOBAL
#define GLOBAL extern
#endif

GLOBAL char *		progname;

extern struct ndm_session_param *const the_logpparams;

GLOBAL int		the_mode;
GLOBAL int		d_debug;
GLOBAL char *		L_log_file;
GLOBAL int		n_noop;
GLOBAL int		v_verbose;
GLOBAL int		o_no_time_stamps;
GLOBAL char *		o_config_file;
GLOBAL char *		o_tape_tcp;
GLOBAL char *		o_load_files_file;

extern void		error_byebye (char *fmt, ...);

extern ndmp_enum_str_table_t	mode_long_name_table[];

extern int		process_args (int argc, char *argv[], ref_ndm_nlist_table_t nlist, ref_ndm_env_table_t env);
extern int		handle_long_option (char *str);
extern void		set_job_mode (int mode);
extern void		usage (void);
extern void		help (void);
extern void		ndmjob_version_info (void);
extern void		dump_settings (ref_ndm_nlist_table_t nlist, ref_ndm_env_table_t env);

extern int		copy_args_expanding_macros (int argc, char *argv[],
					char *av[], int max_ac);
extern int		lookup_and_snarf (char *av[], char *name);
extern int		snarf_macro (char *av[], char *val);

extern void		ndmjob_log_deliver(struct ndmlog *log, char *tag,
					int lev, char *msg);

extern void		ndmjob_log (int level, char *fmt, ...);


#ifndef NDMOS_OPTION_NO_CONTROL_AGENT

#define MAX_EXCLUDE_PATTERN	100

GLOBAL char *		B_bu_type;
GLOBAL int		b_bsize;
GLOBAL char *		C_chdir;
GLOBAL ndmagent_t	D_data_agent;
GLOBAL char *		e_exclude_pattern[MAX_EXCLUDE_PATTERN];
GLOBAL int		n_e_exclude_pattern;
GLOBAL char *		f_tape_device;
GLOBAL char *		I_index_file;	/* output */
GLOBAL char *		J_index_file;	/* input */
GLOBAL ndmmedia_t	m_media[NDM_MAX_MEDIA];
GLOBAL int		n_m_media;
GLOBAL ndmagent_t	R_robot_agent;
GLOBAL ndmscsi_target_t r_robot_target;
GLOBAL ndmagent_t	T_tape_agent;
GLOBAL char *		U_user;

GLOBAL int		o_time_limit;    // currently ignored
GLOBAL int		o_swap_connect;
GLOBAL int		o_use_eject;
GLOBAL int		o_tape_addr;
GLOBAL int		o_from_addr;
GLOBAL int		o_to_addr;
GLOBAL ndmscsi_target_t o_tape_scsi;
GLOBAL int		o_tape_timeout;
GLOBAL int		o_robot_timeout;
GLOBAL char *		o_rules;
GLOBAL off_t		o_tape_limit;
GLOBAL int		p_ndmp_port;

/* The ji_ variables are set according to the -J input index */
GLOBAL ndmmedia_t	ji_media[NDM_MAX_MEDIA];
GLOBAL int		n_ji_media;

GLOBAL ndm_env_table_t	ji_env;

GLOBAL FILE *		index_fp;

#define AGENT_GIVEN(AGENT)	(AGENT.conn_type != NDMCONN_TYPE_NONE)
#define ROBOT_GIVEN()		(r_robot_target.dev_name[0] != 0)

extern void		ndmjob_ixlog_deliver(struct ndmlog *log, char *tag,
					int lev, char *msg);

#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */







#ifndef NDMOS_OPTION_NO_CONTROL_AGENT
extern int		start_index_file (void);
extern int		sort_index_file (void);
extern int		build_job (ref_ndm_job_param_t job);
extern int		args_to_job (ref_ndm_job_param_t job);
extern int		args_to_job_backup_env (ref_ndm_nlist_table_t nlist, 
                                                ref_ndm_env_table_t env);
extern int		args_to_job_recover_env (ref_ndm_env_table_t env);
extern int		args_to_job_recover_nlist (ref_ndm_nlist_table_t nlist);
extern int		jndex_doit (ref_ndm_nlist_table_t nlist);
extern int		jndex_tattle (ref_ndm_nlist_table_t nlist);
extern int		jndex_merge_media (void);
extern int		jndex_audit_not_found (ref_ndm_nlist_table_t nlist);
extern int		jndex_merge_environment (ref_ndm_env_table_t env);
extern int		jndex_fetch_post_backup_data_env (FILE *fp, void **fpcachep);
extern int		jndex_fetch_post_backup_media (FILE *fp, void **fpcachep);

extern int		apply_rules (ref_ndm_job_param_t job, 
                                     char *rules);
extern int		help_rules (void);
#endif /* !NDMOS_OPTION_NO_CONTROL_AGENT */
