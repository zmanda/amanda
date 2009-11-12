/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */

/*
 * $Id: amfeatures.h,v 1.21 2006/07/19 17:46:07 martinea Exp $
 *
 * Define feature test related items.
 */

#ifndef AMFEATURES_H
#define AMFEATURES_H

/*
 * !!!WARNING!!!    !!!WARNING!!!    !!!WARNING!!!    !!!WARNING!!!
 *
 * No matter **WHAT**, you **MUST** enter new features at the **END**
 * of this list (just before "last_feature").  If you do not, mass
 * confusion will ensue.
 *
 * And features must **NEVER** be removed (that is, their code number
 * must remain).  The bits are cheap.
 *
 * If you add a feature here, you probably also need to add a line to
 * am_init_feature_set() in features.c unless it is dynamic in some way.
 *
 * !!!WARNING!!!    !!!WARNING!!!    !!!WARNING!!!    !!!WARNING!!!
 */
typedef enum {
    /*
     * This bit will be set if the feature test code is supported.  It
     * will only be off for "old" (2.4.2p2 and earlier) systems.
     */
    have_feature_support = 0,

    /*
     * Amanda used to send authorization type information around like
     * this in the OPTIONS string:
     *
     *	bsd-auth
     *	krb4-auth
     *
     * To make it easier to add new authorization methods and parse,
     * this was changed to a keyword=value syntax:
     *
     *	auth=BSD
     *	auth=RSH
     *	auth=krb5
     *
     * and so on.
     */

    fe_options_auth, /* amanda_feature_auth_keyword */

    fe_selfcheck_req,
    fe_selfcheck_req_device,
    fe_selfcheck_rep,

    fe_sendsize_req_no_options,
    fe_sendsize_req_options,
    fe_sendsize_req_device,		/* require fe_sendsize_req_options */
    fe_sendsize_rep,

    fe_sendbackup_req,
    fe_sendbackup_req_device,
    fe_sendbackup_rep,

    fe_noop_req,
    fe_noop_rep,

    fe_program_dump,
    fe_program_gnutar,
    fe_program_application_api,		/* require fe_sendsize_req_options */

    fe_options_compress_fast,
    fe_options_compress_best,
    fe_options_srvcomp_fast,
    fe_options_srvcomp_best,
    fe_options_no_record,
    fe_options_index,
    fe_options_exclude_file,
    fe_options_exclude_list,
    fe_options_multiple_exclude,	/* require fe_sendsize_req_options */
    fe_options_optional_exclude,	/* require fe_sendsize_req_options */
    fe_options_include_file,		/* require fe_sendsize_req_options */
    fe_options_include_list,		/* require fe_sendsize_req_options */
    fe_options_multiple_include,	/* require fe_sendsize_req_options */
    fe_options_optional_include,	/* require fe_sendsize_req_options */
    fe_options_bsd_auth,
    fe_options_krb4_auth,
    fe_options_kencrypt,

    fe_req_options_maxdumps,
    fe_req_options_hostname,
    fe_req_options_features,

    fe_rep_options_maxdumps,
    fe_rep_options_hostname,
    fe_rep_options_features,
    fe_rep_options_sendbackup_options,

    fe_amindexd_fileno_in_OLSD,
    fe_amindexd_fileno_in_ORLD,
    fe_amidxtaped_fsf,
    fe_amidxtaped_label,
    fe_amidxtaped_device,
    fe_amidxtaped_host,
    fe_amidxtaped_disk,
    fe_amidxtaped_datestamp,
    fe_amidxtaped_header,
    fe_amidxtaped_nargs,
    fe_amidxtaped_config,

    fe_partial_estimate,
    fe_calcsize_estimate,
    fe_selfcheck_calcsize,

    fe_recover_splits,
    fe_amidxtaped_exchange_features,

    fe_options_compress_cust,
    fe_options_srvcomp_cust,
    fe_options_encrypt_cust,
    fe_options_encrypt_serv_cust,
    fe_options_client_decrypt_option,
    fe_options_server_decrypt_option,

    fe_amindexd_marshall_in_OLSD,
    fe_amindexd_marshall_in_ORLD,
    fe_amindexd_marshall_in_DHST,
    fe_amrecover_FEEDME,
    fe_amrecover_timestamp,

    fe_interface_quoted_text,

    fe_program_star,

    fe_amindexd_options_hostname,
    fe_amindexd_options_features,
    fe_amindexd_options_auth,

    fe_amidxtaped_options_hostname,
    fe_amidxtaped_options_features,
    fe_amidxtaped_options_auth,

    fe_amrecover_message,
    fe_amrecover_feedme_tape,

    fe_req_options_config,

    fe_rep_sendsize_quoted_error,
    fe_req_xml,
    fe_pp_script,	// only in XML
    fe_amindexd_DLE,
    fe_amrecover_dle_in_header,
    fe_xml_estimate,
    fe_xml_property_priority,
    fe_sendsize_rep_warning,
    fe_xml_estimatelist,
    fe_xml_level_server,
    fe_xml_data_path,
    fe_xml_directtcp_list,
    fe_amidxtaped_datapath,

    /*
     * All new features must be inserted immediately *before* this entry.
     */
    last_feature
} am_feature_e;

typedef struct am_feature_s {
    size_t		size;
    unsigned char	*bytes;
} am_feature_t;

/*
 * Functions.
 */
extern am_feature_t *am_init_feature_set(void);
extern am_feature_t *am_set_default_feature_set(void);
extern am_feature_t *am_allocate_feature_set(void);
extern void am_release_feature_set(am_feature_t *);
extern int am_add_feature(am_feature_t *f, am_feature_e n);
extern int am_remove_feature(am_feature_t *f, am_feature_e n);
extern int am_has_feature(am_feature_t *f, am_feature_e n);
extern char *am_feature_to_string(am_feature_t *f);
extern am_feature_t *am_string_to_feature(char *s);

#endif	/* !AMFEATURES_H */
