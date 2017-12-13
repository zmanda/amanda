/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * $Id: amfeatures.c,v 1.24 2006/07/19 17:46:07 martinea Exp $
 *
 * Feature test related code.
 */

#include "amanda.h"
#include "amfeatures.h"

/*
 *=====================================================================
 * Initialize the base feature set for this version of Amanda.
 *
 * am_feature_t *am_init_feature_set()
 *
 * entry:	none
 * exit:	dynamically allocated feature set structure
 *=====================================================================
 */

am_feature_t *
am_init_feature_set(void)
{
    am_feature_t		*f;

    if ((f = am_allocate_feature_set()) != NULL) {
	/*
	 * Whenever a new feature is added, a new line usually needs
	 * to be added here to show that we support it.
	 */
	am_add_feature(f, have_feature_support);
	am_add_feature(f, fe_options_auth);

	am_add_feature(f, fe_selfcheck_req);
	am_add_feature(f, fe_selfcheck_req_device);
	am_add_feature(f, fe_selfcheck_rep);
	am_add_feature(f, fe_sendsize_req_no_options);
	am_add_feature(f, fe_sendsize_req_options);
	am_add_feature(f, fe_sendsize_req_device);
	am_add_feature(f, fe_sendsize_rep);
	am_add_feature(f, fe_sendbackup_req);
	am_add_feature(f, fe_sendbackup_req_device);
	am_add_feature(f, fe_sendbackup_rep);
	am_add_feature(f, fe_noop_req);
	am_add_feature(f, fe_noop_rep);

	am_add_feature(f, fe_program_dump);
	am_add_feature(f, fe_program_gnutar);
	am_add_feature(f, fe_program_application_api);

	am_add_feature(f, fe_options_compress_fast);
	am_add_feature(f, fe_options_compress_best);
	am_add_feature(f, fe_options_srvcomp_fast);
	am_add_feature(f, fe_options_srvcomp_best);
	am_add_feature(f, fe_options_no_record);
	am_add_feature(f, fe_options_bsd_auth);
	am_add_feature(f, fe_options_index);
	am_add_feature(f, fe_options_exclude_file);
	am_add_feature(f, fe_options_exclude_list);
	am_add_feature(f, fe_options_multiple_exclude);
	am_add_feature(f, fe_options_optional_exclude);
	am_add_feature(f, fe_options_include_file);
	am_add_feature(f, fe_options_include_list);
	am_add_feature(f, fe_options_multiple_include);
	am_add_feature(f, fe_options_optional_include);
	am_add_feature(f, fe_options_kencrypt);

	am_add_feature(f, fe_req_options_maxdumps);
	am_add_feature(f, fe_req_options_hostname);
	am_add_feature(f, fe_req_options_features);

	am_add_feature(f, fe_rep_options_features);

	am_add_feature(f, fe_amindexd_fileno_in_OLSD);
	am_add_feature(f, fe_amindexd_fileno_in_ORLD);
	am_add_feature(f, fe_amidxtaped_fsf);
	am_add_feature(f, fe_amidxtaped_label);
	am_add_feature(f, fe_amidxtaped_device);
	am_add_feature(f, fe_amidxtaped_host);
	am_add_feature(f, fe_amidxtaped_disk);
	am_add_feature(f, fe_amidxtaped_datestamp);
	am_add_feature(f, fe_amidxtaped_header);
	am_add_feature(f, fe_amidxtaped_config);

	am_add_feature(f, fe_recover_splits);
	am_add_feature(f, fe_amidxtaped_exchange_features);
	am_add_feature(f, fe_partial_estimate);
	am_add_feature(f, fe_calcsize_estimate);
	am_add_feature(f, fe_selfcheck_calcsize);
	am_add_feature(f, fe_options_compress_cust);
	am_add_feature(f, fe_options_srvcomp_cust);
	am_add_feature(f, fe_options_encrypt_cust);
	am_add_feature(f, fe_options_encrypt_serv_cust);
	am_add_feature(f, fe_options_client_decrypt_option);
	am_add_feature(f, fe_options_server_decrypt_option);

	am_add_feature(f, fe_amindexd_marshall_in_OLSD);
	am_add_feature(f, fe_amindexd_marshall_in_ORLD);
	am_add_feature(f, fe_amindexd_marshall_in_DHST);

        am_add_feature(f, fe_amrecover_FEEDME);
        am_add_feature(f, fe_amrecover_timestamp);

        am_add_feature(f, fe_interface_quoted_text);

	am_add_feature(f, fe_program_star);

	am_add_feature(f, fe_amindexd_options_hostname);
	am_add_feature(f, fe_amindexd_options_features);
	am_add_feature(f, fe_amindexd_options_auth);

	am_add_feature(f, fe_amidxtaped_options_hostname);
	am_add_feature(f, fe_amidxtaped_options_features);
	am_add_feature(f, fe_amidxtaped_options_auth);

	am_add_feature(f, fe_amrecover_message);
	am_add_feature(f, fe_amrecover_feedme_tape);

	am_add_feature(f, fe_req_options_config);

	am_add_feature(f, fe_rep_sendsize_quoted_error);
	am_add_feature(f, fe_req_xml);
	am_add_feature(f, fe_pp_script);
	am_add_feature(f, fe_amindexd_DLE);
	am_add_feature(f, fe_amrecover_dle_in_header);
	am_add_feature(f, fe_xml_estimate);
	am_add_feature(f, fe_xml_property_priority);
	am_add_feature(f, fe_sendsize_rep_warning);
	am_add_feature(f, fe_xml_estimatelist);
	am_add_feature(f, fe_xml_level_server);
	am_add_feature(f, fe_xml_data_path);
	am_add_feature(f, fe_xml_directtcp_list);
	am_add_feature(f, fe_amidxtaped_datapath);
	am_add_feature(f, fe_sendbackup_noop);
	am_add_feature(f, fe_amrecover_origsize_in_header);
	am_add_feature(f, fe_amidxtaped_abort);
	am_add_feature(f, fe_amrecover_correct_disk_quoting);
	am_add_feature(f, fe_amindexd_quote_label);
	am_add_feature(f, fe_amrecover_receive_unfiltered);
	am_add_feature(f, fe_application_client_name);
	am_add_feature(f, fe_script_client_name);
	am_add_feature(f, fe_dumptype_property);
	am_add_feature(f, fe_sendbackup_crc);
	am_add_feature(f, fe_amrecover_crc_in_header);
	am_add_feature(f, fe_amrecover_data_status);
	am_add_feature(f, fe_amrecover_data_crc);
	am_add_feature(f, fe_sendbackup_state);
	am_add_feature(f, fe_amrecover_storage_in_marshall);
	am_add_feature(f, fe_amidxtaped_storage_in_marshall);
	am_add_feature(f, fe_amindexd_STORAGE);
	am_add_feature(f, fe_sendbackup_retry);
	am_add_feature(f, fe_sendbackup_retry_delay);
	am_add_feature(f, fe_sendbackup_retry_level);
	am_add_feature(f, fe_sendbackup_retry_message);
	am_add_feature(f, fe_selfcheck_message);
	//am_add_feature(f, fe_sendsize_message);
	//am_add_feature(f, fe_sendbackup_message);
	am_add_feature(f, fe_sendbackup_rep_retry);
	am_add_feature(f, fe_sendbackup_stream_state);
	am_add_feature(f, fe_amrecover_stream_state);
	am_add_feature(f, fe_amidxtaped_dar);
	am_add_feature(f, fe_restore_mesg_json);
	am_add_feature(f, fe_restore_state_stream);
	am_add_feature(f, fe_amrecover_header_send_size);
	am_add_feature(f, fe_amrecover_header_ready);
	am_add_feature(f, fe_amrecover_header_done);
	am_add_feature(f, fe_amrecover_state_send);
	am_add_feature(f, fe_amrecover_state_ready);
	am_add_feature(f, fe_amrecover_state_done);
	am_add_feature(f, fe_amrecover_data_send);
	am_add_feature(f, fe_amrecover_data_ready);
	//am_add_feature(f, fe_amrecover_data_done);
	am_add_feature(f, fe_restore_header_send_size);
	am_add_feature(f, fe_restore_header_ready);
	am_add_feature(f, fe_restore_header_done);
	am_add_feature(f, fe_restore_state_send);
	am_add_feature(f, fe_restore_state_ready);
	am_add_feature(f, fe_restore_state_done);
	am_add_feature(f, fe_restore_data_send);
	am_add_feature(f, fe_restore_data_ready);
	//am_add_feature(f, fe_amrecover_data_done);
	am_add_feature(f, fe_restore_dar);
	am_add_feature(f, fe_restore_datapath);
	am_add_feature(f, fe_restore_include);
	am_add_feature(f, fe_restore_include_glob);
	am_add_feature(f, fe_restore_exclude);
	am_add_feature(f, fe_restore_exclude_glob);
	am_add_feature(f, fe_restore_prev_next_level);

	am_add_feature(f, fe_sendbackup_req_options_data_shm_control_name);
	am_add_feature(f, fe_req_options_timestamp);
	am_add_feature(f, fe_sendbackup_statedone);
    }
    return f;
}

/*
 *=====================================================================
 * Set a default feature set for client that doesn't have noop service.
 * This is all the features available in 2.4.2p2.
 *
 * entry:	none
 * exit: dynamically allocated feature set
 *=====================================================================
 */
 
am_feature_t *
am_set_default_feature_set(void)
{
    am_feature_t		*f;

    if ((f = am_allocate_feature_set()) != NULL) {

	am_add_feature(f, fe_selfcheck_req);
	am_add_feature(f, fe_selfcheck_rep);
	am_add_feature(f, fe_sendsize_req_no_options);
	am_add_feature(f, fe_sendsize_rep);
	am_add_feature(f, fe_sendbackup_req);
	am_add_feature(f, fe_sendbackup_rep);

	am_add_feature(f, fe_program_dump);
	am_add_feature(f, fe_program_gnutar);

	am_add_feature(f, fe_options_compress_fast);
	am_add_feature(f, fe_options_compress_best);
	am_add_feature(f, fe_options_srvcomp_fast);
	am_add_feature(f, fe_options_srvcomp_best);
	am_add_feature(f, fe_options_no_record);
	am_add_feature(f, fe_options_bsd_auth);
	am_add_feature(f, fe_options_index);
	am_add_feature(f, fe_options_exclude_file);
	am_add_feature(f, fe_options_exclude_list);
	am_add_feature(f, fe_options_kencrypt);

	am_add_feature(f, fe_req_options_maxdumps);
	am_add_feature(f, fe_req_options_hostname);
	am_add_feature(f, fe_req_options_features);

	am_add_feature(f, fe_rep_options_sendbackup_options);
    }
    return f;
}

/*
 *=====================================================================
 * Allocate space for a feature set.
 *
 * am_feature_t *am_allocate_feature_set()
 *
 * entry:	none
 * exit:	dynamically allocated feature set structure
 *=====================================================================
 */

am_feature_t *
am_allocate_feature_set(void)
{
    size_t			nbytes;
    am_feature_t		*result;

    result = (am_feature_t *)g_malloc(sizeof(*result));
    memset(result, 0, sizeof(*result));
    nbytes = (((size_t)last_feature) + 8) >> 3;
    result->size = nbytes;
    result->bytes = (unsigned char *)g_malloc(nbytes);
    memset(result->bytes, 0, nbytes);
    return result;
}

/*
 *=====================================================================
 * Release space allocated to a feature set.
 *
 * void am_release_feature_set(am_feature_t *f)
 *
 * entry:	f = feature set to release
 * exit:	none
 *=====================================================================
 */

void
am_release_feature_set(
    am_feature_t	*f)
{
    if (f != NULL) {
	amfree(f->bytes);
	f->size = 0;
    }
    amfree(f);
}

/*
 *=====================================================================
 * Add a feature to a feature set.
 *
 * int am_add_feature(am_feature_t *f, am_feature_e n)
 *
 * entry:	f = feature set to add to
 *		n = feature to add
 * exit:	non-zero if feature added, else zero (e.g. if the feature
 *		is beyond what is currently supported)
 *=====================================================================
 */

int
am_add_feature(
    am_feature_t	*f,
    am_feature_e	n)
{
    size_t			byte;
    int				bit;
    int				result = 0;

    if (f != NULL && (int)n >= 0) {
	byte = ((size_t)n) >> 3;
	if (byte < f->size) {
	    bit = ((int)n) & 0x7;
	    f->bytes[byte] = (unsigned char)((int)f->bytes[byte] | (unsigned char)(1 << bit));
	    result = 1;
	}
    }
    return result;
}

/*
 *=====================================================================
 * Remove a feature from a feature set.
 *
 * int am_remove_feature(am_feature_t *f, am_feature_e n)
 *
 * entry:	f = feature set to remove from
 *		n = feature to remove
 * exit:	non-zero if feature removed, else zero (e.g. if the feature
 *		is beyond what is currently supported)
 *=====================================================================
 */

int
am_remove_feature(
    am_feature_t	*f,
    am_feature_e	n)
{
    size_t			byte;
    int				bit;
    int				result = 0;

    if (f != NULL && (int)n >= 0) {
	byte = ((size_t)n) >> 3;
	if (byte < f->size) {
	    bit = ((int)n) & 0x7;
	    f->bytes[byte] = (unsigned char)((int)f->bytes[byte] & (unsigned char)~(1 << bit));
	    result = 1;
	}
    }
    return result;
}

/*
 *=====================================================================
 * Return true if a given feature is available.
 *
 * int am_has_feature(am_feature_t *f, am_feature_e n)
 *
 * entry:	f = feature set to test
 *		n = feature to test
 * exit:	non-zero if feature is enabled
 *=====================================================================
 */

int
am_has_feature(
    am_feature_t	*f,
    am_feature_e	n)
{
    size_t			byte;
    int				bit;
    int				result = 0;

    if (f != NULL && (int)n >= 0) {
	byte = ((size_t)n) >> 3;
	if (byte < f->size) {
	    bit = ((int)n) & 0x7;
	    result = ((f->bytes[byte] & (1 << bit)) != 0);
	}
    }
    return result;
}

/*
 *=====================================================================
 * Convert a feature set to string.
 *
 * char *am_feature_to_string(am_feature_t *f)
 *
 * entry:	f = feature set to convet
 * exit:	dynamically allocated string
 *=====================================================================
 */

char *
am_feature_to_string(
    am_feature_t	*f)
{
    char			*result;
    size_t			i;

    if (f == NULL) {
	result = g_strdup(_("UNKNOWNFEATURE"));
    } else {
	result = g_malloc((f->size * 2) + 1);
	for (i = 0; i < f->size; i++) {
	    g_snprintf(result + (i * 2), 2 + 1, "%02x", f->bytes[i]);
	}
	result[i * 2] = '\0';
    }
    return result;
}

/*
 *=====================================================================
 * Convert a sting back to a feature set.
 *
 * am_feature_t *am_string_to_feature(char *s)
 *
 * entry:	s = string to convert
 * exit:	dynamically allocated feature set
 *
 * Note: if the string is longer than the list of features we support,
 * the remaining input features are ignored.  If it is shorter, the
 * missing features are disabled.
 *
 * If the string is not formatted properly (not a multiple of two bytes),
 * NULL is returned.
 *
 * Conversion stops at the first non-hex character.
 *=====================================================================
 */

am_feature_t *
am_string_to_feature(
    char		*s)
{
    am_feature_t		*f = NULL;
    size_t			i;
    int				ch1, ch2;
    char *			orig = s;

    if (s != NULL && !g_str_equal(s, "UNKNOWNFEATURE")) {
	f = am_allocate_feature_set();
	for (i = 0; i < f->size && (ch1 = *s++) != '\0'; i++) {
	    if (isdigit(ch1)) {
		ch1 -= '0';
	    } else if (ch1 >= 'a' && ch1 <= 'f') {
		ch1 -= 'a';
		ch1 += 10;
	    } else if (ch1 >= 'A' && ch1 <= 'F') {
		ch1 -= 'A';
		ch1 += 10;
	    } else {
		goto bad;
	    }
	    ch2 = *s++;
	    if (isdigit(ch2)) {
		ch2 -= '0';
	    } else if (ch2 >= 'a' && ch2 <= 'f') {
		ch2 -= 'a';
		ch2 += 10;
	    } else if (ch2 >= 'A' && ch2 <= 'F') {
		ch2 -= 'A';
		ch2 += 10;
	    } else if (ch2 == '\0') {
		g_warning("odd number of digits in amfeature string; truncating");
		break;
	    } else {
		goto bad;
	    }
	    f->bytes[i] = (unsigned char)((ch1 << 4) | ch2);
	}
    }
    return f;

bad:
    g_warning("Bad feature string '%s'", orig);
    am_release_feature_set(f);
    return NULL;
}

am_feature_t *
am_features(
    am_feature_t *f)
{
    return f;
}
