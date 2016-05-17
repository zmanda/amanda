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
 * $Id: amxml.c 5151 2007-02-06 15:41:53Z martineau $
 *
 * xml parsing of amanda protocol packet
 */

#include "amanda.h"
#include "amutil.h"
#include "amxml.h"
#include "match.h"
#include "glib.h"
#include "conffile.h"
#include "base64.h"

typedef struct amgxml_s {
    dle_t   *dles;
    dle_t   *dle;
    GSList  *element_names;
    int      has_calcsize;
    int      has_estimate;
    int      has_record;
    int      has_spindle;
    int      has_compress;
    int      has_encrypt;
    int      has_kencrypt;
    int      has_datapath;
    int      has_exclude;
    int      has_include;
    int      has_index;
    int      has_backup_program;
    int      has_plugin;
    int      has_optional;
    char    *property_name;
    property_t *property_data;
    proplist_t  property;
    script_t   *script;
    am_level_t    *alevel;
    char       *encoding;
    char       *raw;
} amgxml_t;


dle_t *
alloc_dle(void)
{
    dle_t *dle;

    dle = g_new0(dle_t, 1);
    init_dle(dle);
    return dle;
}

void
free_dle(
    dle_t *dle)
{
    scriptlist_t scriptlist;

    if (!dle)
	return;

    amfree(dle->disk);
    amfree(dle->device);
    amfree(dle->program);
    g_slist_free(dle->estimatelist);
    slist_free_full(dle->levellist, g_free);
    amfree(dle->dumpdate);
    amfree(dle->compprog);
    amfree(dle->srv_encrypt);
    amfree(dle->clnt_encrypt);
    amfree(dle->srv_decrypt_opt);
    amfree(dle->clnt_decrypt_opt);
    amfree(dle->auth);
    amfree(dle->application_client_name);
    free_sl(dle->exclude_file);
    free_sl(dle->exclude_list);
    free_sl(dle->include_file);
    free_sl(dle->include_list);
    if (dle->property)
	g_hash_table_destroy(dle->property);
    if (dle->application_property)
	g_hash_table_destroy(dle->application_property);
    for(scriptlist = dle->scriptlist; scriptlist != NULL;
				      scriptlist = scriptlist->next) {
	free_script_data((script_t *)scriptlist->data);
    }
    slist_free_full(dle->scriptlist, g_free);
    slist_free_full(dle->directtcp_list, g_free);
    amfree(dle);
}

void
free_script_data(
    script_t *script)
{
    amfree(script->plugin);
    amfree(script->client_name);
    if (script->property)
	g_hash_table_destroy(script->property);
}

void
init_dle(
    dle_t *dle)
{
    dle->disk = NULL;
    dle->device = NULL;
    dle->program_is_application_api = 0;
    dle->program = NULL;
    dle->estimatelist = NULL;
    dle->record = 1;
    dle->spindle = 0;
    dle->compress = COMP_NONE;
    dle->encrypt = ENCRYPT_NONE;
    dle->kencrypt = 0;
    dle->levellist = NULL;
    dle->dumpdate = NULL;
    dle->compprog = NULL;
    dle->srv_encrypt = NULL;
    dle->clnt_encrypt = NULL;
    dle->srv_decrypt_opt = NULL;
    dle->clnt_decrypt_opt = NULL;
    dle->create_index = 0;
    dle->auth = NULL;
    dle->exclude_file = NULL;
    dle->exclude_list = NULL;
    dle->include_file = NULL;
    dle->include_list = NULL;
    dle->exclude_optional = 0;
    dle->include_optional = 0;
    dle->property = NULL;
    dle->application_property = NULL;
    dle->scriptlist = NULL;
    dle->data_path = DATA_PATH_AMANDA;
    dle->directtcp_list = NULL;
    dle->application_client_name = NULL;
    dle->next = NULL;
}


/* Called for open tags <foo bar="baz"> */
static void amstart_element(GMarkupParseContext *context,
			    const gchar         *element_name,
			    const gchar        **attribute_names,
			    const gchar        **attribute_values,
			    gpointer             user_data,
			    GError             **gerror);

/* Called for close tags </foo> */
static void amend_element(GMarkupParseContext *context,
	                  const gchar         *element_name,
			  gpointer             user_data,
			  GError             **gerror);

/* Called for character data */
/* text is not nul-terminated */
static void amtext(GMarkupParseContext *context,
		   const gchar         *text,
		   gsize                text_len,  
		   gpointer             user_data,
		   GError             **gerror);

/* Called for open tags <foo bar="baz"> */
static void
amstart_element(
    G_GNUC_UNUSED GMarkupParseContext *context,
		  const gchar         *element_name,
    G_GNUC_UNUSED const gchar        **attribute_names,
    G_GNUC_UNUSED const gchar        **attribute_values,
		  gpointer             user_data,
		  GError             **gerror)
{
    amgxml_t *data_user = user_data;
    dle_t    *adle;
    GSList   *last_element = data_user->element_names;
    char     *last_element_name = NULL;
    dle_t    *dle = data_user->dle;
    const gchar   **at_names, **at_values;

    if (last_element)
	last_element_name = last_element->data;

    amfree(data_user->raw);
    amfree(data_user->encoding);

    if (attribute_names) {
	for(at_names = attribute_names, at_values = attribute_values;
	    *at_names != NULL && at_values != NULL;
	    at_names++, at_values++) {
	    if (g_str_equal(*at_names, "encoding")) {
		amfree(data_user->encoding);
		data_user->encoding = g_strdup(*at_values);
	    } else if (g_str_equal(*at_names, "raw")) {
		amfree(data_user->raw);
		data_user->raw = base64_decode_alloc_string((char *)*at_values);
	    } else {
		g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			    "XML: Invalid attribute '%s' for %s element",
			    *at_names, element_name);
		return;
	    }
	}
    }

    if (g_str_equal(element_name, "dle")) {
	if (last_element != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid dle element");
	    return;
	}
	for(adle = data_user->dles; adle != NULL && adle->next != NULL;
	    adle = adle->next);
	data_user->dle = alloc_dle();
	if (adle == NULL) {
	    data_user->dles = data_user->dle;
	} else {
	    adle->next = data_user->dle;
	}
	data_user->has_calcsize = 0;
	data_user->has_estimate = 0;
	data_user->has_record = 0;
	data_user->has_spindle = 0;
	data_user->has_compress = 0;
	data_user->has_encrypt = 0;
	data_user->has_kencrypt = 0;
	data_user->has_datapath = 0;
	data_user->has_exclude = 0;
	data_user->has_include = 0;
	data_user->has_index = 0;
	data_user->has_backup_program = 0;
	data_user->has_plugin = 0;
	data_user->has_optional = 0;
	data_user->property_name = NULL;
	data_user->property_data = NULL;
	data_user->property =
	            g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_property_t);
	data_user->script = NULL;
	data_user->alevel = NULL;
	data_user->dle->property = data_user->property;
	amfree(data_user->encoding);
	amfree(data_user->raw);
    } else if(g_str_equal(element_name, "disk") ||
	      g_str_equal(element_name, "diskdevice") ||
	      g_str_equal(element_name, "calcsize") ||
	      g_str_equal(element_name, "estimate") ||
	      g_str_equal(element_name, "program") ||
	      g_str_equal(element_name, "auth") ||
	      g_str_equal(element_name, "index") ||
	      g_str_equal(element_name, "dumpdate") ||
	      g_str_equal(element_name, "level") ||
	      g_str_equal(element_name, "record") ||
	      g_str_equal(element_name, "spindle") ||
	      g_str_equal(element_name, "compress") ||
	      g_str_equal(element_name, "encrypt") ||
	      g_str_equal(element_name, "kencrypt") ||
	      g_str_equal(element_name, "datapath") ||
	      g_str_equal(element_name, "exclude") ||
	      g_str_equal(element_name, "include")) {
	if (!last_element_name || !g_str_equal(last_element_name, "dle")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if ((g_str_equal(element_name, "disk") && dle->disk) ||
	    (g_str_equal(element_name, "diskdevice") && dle->device) ||
	    (g_str_equal(element_name, "calcsize") && data_user->has_calcsize) ||
	    (g_str_equal(element_name, "estimate") && data_user->has_estimate) ||
	    (g_str_equal(element_name, "record") && data_user->has_record) ||
	    (g_str_equal(element_name, "spindle") && data_user->has_spindle) ||
	    (g_str_equal(element_name, "program") && dle->program) ||
	    (g_str_equal(element_name, "auth") && dle->auth) ||
	    (g_str_equal(element_name, "index") && data_user->has_index) ||
	    (g_str_equal(element_name, "dumpdate") && dle->dumpdate) ||
	    (g_str_equal(element_name, "compress") && data_user->has_compress) ||
	    (g_str_equal(element_name, "encrypt") && data_user->has_encrypt) ||
	    (g_str_equal(element_name, "kencrypt") && data_user->has_kencrypt) ||
	    (g_str_equal(element_name, "datapath") && data_user->has_datapath) ||
	    (g_str_equal(element_name, "exclude") && data_user->has_exclude) ||
	    (g_str_equal(element_name, "include") && data_user->has_include)) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
	if (g_str_equal(element_name, "calcsize")) data_user->has_calcsize       = 1;
	if (g_str_equal(element_name, "estimate")) data_user->has_estimate       = 1;
	if (g_str_equal(element_name, "record")) data_user->has_record         = 1;
	if (g_str_equal(element_name, "spindle")) data_user->has_spindle        = 1;
	if (g_str_equal(element_name, "index")) data_user->has_index          = 1;
	if (g_str_equal(element_name, "compress")) data_user->has_compress       = 1;
	if (g_str_equal(element_name, "encrypt")) data_user->has_encrypt        = 1;
	if (g_str_equal(element_name, "kencrypt")) data_user->has_kencrypt       = 1;
	if (g_str_equal(element_name, "datapath")) data_user->has_datapath       = 1;
	if (g_str_equal(element_name, "exclude")) data_user->has_exclude        = 1;
	if (g_str_equal(element_name, "include")) data_user->has_include        = 1;
	if (g_str_equal(element_name, "exclude") || g_str_equal(element_name,
                                                                "include"))
	   data_user->has_optional = 0;
	if (g_str_equal(element_name, "level")) {
	    data_user->alevel = g_new0(am_level_t, 1);
	}
    } else if (g_str_equal(element_name, "server")) {
	if (!last_element_name || !g_str_equal(last_element_name, "level")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if(g_str_equal(element_name, "custom-compress-program")) {
	if (!last_element_name || !g_str_equal(last_element_name, "compress")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (dle->compprog) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
    } else if (g_str_equal(element_name, "custom-encrypt-program") ||
	       g_str_equal(element_name, "decrypt-option")) {
	if (!last_element_name || !g_str_equal(last_element_name, "encrypt")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (g_str_equal(element_name, "custom-encrypt-program") &&
		   dle->clnt_encrypt) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
	if (g_str_equal(element_name, "decrypt-option") &&
		   dle->clnt_decrypt_opt) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
    } else if(g_str_equal(element_name, "plugin")) {
	if (!last_element_name ||
	    (!g_str_equal(last_element_name, "backup-program") &&
	     !g_str_equal(last_element_name, "script"))) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (data_user->has_plugin) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element in '%s'", element_name,
			last_element_name);
	    return;
	}
    } else if(g_str_equal(element_name, "property")) {
	if (!last_element_name ||
	    (!g_str_equal(last_element_name, "backup-program") &&
	     !g_str_equal(last_element_name, "script") &&
	     !g_str_equal(last_element_name, "dle"))) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	data_user->property_data = malloc(sizeof(property_t));
	data_user->property_data->append = 0;
	data_user->property_data->priority = 0;
	data_user->property_data->values = NULL;
    } else if(g_str_equal(element_name, "name")) {
	if (!last_element_name || !g_str_equal(last_element_name, "property")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (data_user->property_name) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element in '%s'", element_name,
			last_element_name);
	    return;
	}
    } else if(g_str_equal(element_name, "priority")) {
	if (!last_element_name || !g_str_equal(last_element_name, "property")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if(g_str_equal(element_name, "value")) {
	if (!last_element_name || !g_str_equal(last_element_name, "property")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if(g_str_equal(element_name, "file") ||
	      g_str_equal(element_name, "list") ||
	      g_str_equal(element_name, "optional")) {
	if (!last_element_name ||
	    (!g_str_equal(last_element_name, "exclude") &&
	     !g_str_equal(last_element_name, "include"))) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (g_str_equal(element_name, "optional") && data_user->has_optional) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
	if (g_str_equal(element_name, "optional")) data_user->has_optional = 1;
    } else if (g_str_equal(element_name, "backup-program")) {
	if (data_user->has_backup_program) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	} else {
	    data_user->has_backup_program = 1;
	    data_user->property =
	            g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_property_t);
	    data_user->has_plugin = 0;
	}
    } else if (g_str_equal(element_name, "script")) {
	data_user->property =
	            g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_property_t);
	data_user->script = malloc(sizeof(script_t));
	data_user->script->plugin = NULL;
	data_user->script->execute_on = 0;
	data_user->script->execute_where = ES_CLIENT;
	data_user->script->property = NULL;
	data_user->script->client_name = NULL;
	data_user->script->result = NULL;
	data_user->has_plugin = 0;
    } else if (g_str_equal(element_name, "execute_on")) {
    } else if (g_str_equal(element_name, "execute_where")) {
    } else if (g_str_equal(element_name, "directtcp")) {
	if (!last_element_name || !g_str_equal(last_element_name, "datapath")) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if (g_str_equal(element_name, "client_name")) {
    } else {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: Invalid %s element", element_name);
	return;
    }
    data_user->element_names = g_slist_prepend(data_user->element_names,
					       g_strdup(element_name));
}

/* Called for close tags </foo> */
static void
amend_element(
    G_GNUC_UNUSED GMarkupParseContext *context,
		  const gchar         *element_name,
		  gpointer             user_data,
		  GError             **gerror)
{
    amgxml_t *data_user = user_data;
    GSList   *last_element = data_user->element_names;
    char     *last_element_name = NULL;
    dle_t    *dle = data_user->dle;

    if (!last_element) {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: Invalid closing tag");
	return;
    }
    last_element_name = last_element->data;
    if (!g_str_equal(last_element_name, element_name)) {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: Invalid closing tag '%s'", element_name);
	return;
    }

    if (g_str_equal(element_name, "property")) {
	g_hash_table_insert(data_user->property,
			    data_user->property_name,
			    data_user->property_data);
	data_user->property_name = NULL;
	data_user->property_data = NULL;
    } else if (g_str_equal(element_name, "dle")) {
	if (dle->program_is_application_api &&
	    !dle->program) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: program set to APPLICATION but no application set");
	    return;
	}
	if (dle->device == NULL && dle->disk)
	    dle->device = g_strdup(dle->disk);
	if (dle->estimatelist == NULL)
	    dle->estimatelist = g_slist_append(dle->estimatelist, ES_CLIENT);
/* Add check of required field */
	data_user->property = NULL;
	data_user->dle = NULL;
    } else if (g_str_equal(element_name, "backup-program")) {
	if (dle->program == NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: No plugin set for application");
	    return;
	}
	dle->application_property = data_user->property;
	data_user->property = dle->property;
    } else if (g_str_equal(element_name, "script")) {
	if (data_user->script->plugin == NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: No plugin set for script");
	    return;
	}
	data_user->script->property = data_user->property;
	data_user->property = dle->property;
	dle->scriptlist = g_slist_append(dle->scriptlist, data_user->script);
	data_user->script = NULL;
    } else if (g_str_equal(element_name, "level")) {
	dle->levellist = g_slist_append(dle->levellist, data_user->alevel);
	data_user->alevel = NULL;
    }
    g_free(data_user->element_names->data);
    data_user->element_names = g_slist_delete_link(data_user->element_names,
						   data_user->element_names);
}

/* Called for character data */
/* text is not nul-terminated */
static void
amtext(
    G_GNUC_UNUSED GMarkupParseContext *context,
		  const gchar         *text,
		  gsize                text_len,  
		  gpointer             user_data,
		  GError             **gerror)
{
    char     *tt;
    amgxml_t *data_user = user_data;
    GSList   *last_element = data_user->element_names;
    char     *last_element_name;
    GSList   *last_element2;
    char     *last_element2_name;
    dle_t    *dle = data_user->dle;

    if (!last_element) {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: Invalid text");
	return;
    }
    last_element_name = last_element->data;

    tt = malloc(text_len + 8 + 1);
    strncpy(tt,text,text_len);
    tt[text_len] = '\0';

    //check if it is only space
    if (match_no_newline("^[ \f\n\r\t\v]*$", tt)) {
	amfree(tt);
	return;
    }

    if (data_user->raw) {
	amfree(tt);
	tt = g_strdup(data_user->raw);
    } else if (strlen(tt) > 0) {
	/* remove trailing space */
	char *ttt = tt + strlen(tt) - 1;
	while(*ttt == ' ') {
	    ttt--;
	}
	ttt++;
	*ttt = '\0';
    }

    //check if it is only space
    if (match_no_newline("^[ \f\n\r\t\v]*$", tt)) {
	amfree(tt);
	return;
    }

    if (g_str_equal(last_element_name, "dle") ||
	g_str_equal(last_element_name, "backup-program") ||
	g_str_equal(last_element_name, "exclude") ||
	g_str_equal(last_element_name, "include")) {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: %s doesn't have text '%s'", last_element_name, tt);
	amfree(tt);
	return;
    } else if(g_str_equal(last_element_name, "disk")) {
	if (dle->disk != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->disk = tt;
    } else if(g_str_equal(last_element_name, "diskdevice")) {
	if (dle->device != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->device = tt;
    } else if(g_str_equal(last_element_name, "calcsize")) {
	if (strcasecmp(tt,"yes") == 0) {
	    dle->estimatelist = g_slist_append(dle->estimatelist,
					       GINT_TO_POINTER(ES_CALCSIZE));
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "estimate")) {
	char *ttt = tt;
	while (strlen(ttt) > 0) {
	    if (BSTRNCMP(ttt,"CLIENT") == 0) {
		dle->estimatelist = g_slist_append(dle->estimatelist,
						   GINT_TO_POINTER(ES_CLIENT));
		ttt += strlen("client");
	    } else if (BSTRNCMP(ttt,"CALCSIZE") == 0) {
		if (!data_user->has_calcsize)
		    dle->estimatelist = g_slist_append(dle->estimatelist,
						 GINT_TO_POINTER(ES_CALCSIZE));
		ttt += strlen("calcsize");
	    } else if (BSTRNCMP(ttt,"SERVER") == 0) {
		dle->estimatelist = g_slist_append(dle->estimatelist,
						   GINT_TO_POINTER(ES_SERVER));
		ttt += strlen("server");
	    } else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: bad estimate: %s", tt);
		return;
	    }
	    while (*ttt == ' ')
		ttt++;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "program")) {
	if (dle->program != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	if (g_str_equal(tt, "APPLICATION")) {
	    dle->program_is_application_api = 1;
	    dle->program = NULL;
	    amfree(tt);
	} else {
	    dle->program = tt;
	}
    } else if(g_str_equal(last_element_name, "plugin")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid name text");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "backup-program")) {
	    dle->program = tt;
	} else if (g_str_equal(last_element2_name, "script")) {
	    data_user->script->plugin = tt;
	} else {
	    error("plugin outside of backup-program");
	}
	data_user->has_plugin = 1;
    } else if(g_str_equal(last_element_name, "name")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid name text");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "property")) {
	    data_user->property_name = tt;
	} else {
	    error("name outside of property");
	}
    } else if(g_str_equal(last_element_name, "priority")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid priority text");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "property")) {
	    if (strcasecmp(tt,"yes") == 0) {
		data_user->property_data->priority = 1;
	    }
	} else {
	    error("priority outside of property");
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "value")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid name text");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "property")) {
	    data_user->property_data->values =
			g_slist_append(data_user->property_data->values, tt);
	} else {
	    error("value outside of property");
	}
    } else if(g_str_equal(last_element_name, "auth")) {
	if (dle->auth != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    return;
	}
	dle->auth = tt;
    } else if(g_str_equal(last_element_name, "level")) {
	data_user->alevel->level = atoi(tt);
	amfree(tt);
    } else if (g_str_equal(last_element_name, "server")) {
	if (strcasecmp(tt,"no") == 0) {
	    data_user->alevel->server = 0;
	} else if (strcasecmp(tt,"yes") == 0) {
	    data_user->alevel->server = 1;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s (%s)", last_element_name, tt);
	    amfree(tt);
	    return;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "index")) {
	if (strcasecmp(tt,"no") == 0) {
	    dle->create_index = 0;
	} else if (strcasecmp(tt,"yes") == 0) {
	    dle->create_index = 1;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s (%s)", last_element_name, tt);
	    amfree(tt);
	    return;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "dumpdate")) {
	if (dle->dumpdate != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->dumpdate = tt;
    } else if(g_str_equal(last_element_name, "record")) {
	if (strcasecmp(tt, "no") == 0) {
	    dle->record = 0;
	} else if (strcasecmp(tt, "yes") == 0) {
	    dle->record = 1;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s (%s)", last_element_name, tt);
	    amfree(tt);
	    return;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "spindle")) {
	dle->spindle = atoi(tt);
	amfree(tt);
    } else if(g_str_equal(last_element_name, "compress")) {
	if (g_str_equal(tt, "FAST")) {
	    dle->compress = COMP_FAST;
	} else if (g_str_equal(tt, "BEST")) {
	    dle->compress = COMP_BEST;
	} else if (BSTRNCMP(tt, "CUSTOM") == 0) {
	    dle->compress = COMP_CUST;
	} else if (g_str_equal(tt, "SERVER-FAST")) {
	    dle->compress = COMP_SERVER_FAST;
	} else if (g_str_equal(tt, "SERVER-BEST")) {
	    dle->compress = COMP_SERVER_BEST;
	} else if (BSTRNCMP(tt, "SERVER-CUSTOM") == 0) {
	    dle->compress = COMP_SERVER_CUST;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s (%s)", last_element_name, tt);
	    amfree(tt);
	    return;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "custom-compress-program")) {
	if (dle->compprog != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->compprog = tt;
    } else if(g_str_equal(last_element_name, "encrypt")) {
	if (BSTRNCMP(tt,"NO") == 0) {
	    dle->encrypt = ENCRYPT_NONE;
	} else if (BSTRNCMP(tt, "CUSTOM") == 0) {
	    dle->encrypt = ENCRYPT_CUST;
	} else if (BSTRNCMP(tt, "SERVER-CUSTOM") == 0) {
	    dle->encrypt = ENCRYPT_SERV_CUST;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s (%s)", last_element_name, tt);
	    amfree(tt);
	    return;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "kencrypt")) {
	if (strcasecmp(tt,"no") == 0) {
	    dle->kencrypt = 0;
	} else if (strcasecmp(tt,"yes") == 0) {
	    dle->kencrypt = 1;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s (%s)", last_element_name, tt);
	    amfree(tt);
	    return;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "custom-encrypt-program")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (dle->encrypt == ENCRYPT_SERV_CUST)
	    dle->srv_encrypt = tt;
	else
	    dle->clnt_encrypt = tt;
    } else if(g_str_equal(last_element_name, "decrypt-option")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (dle->encrypt == ENCRYPT_SERV_CUST)
	    dle->srv_decrypt_opt = tt;
	else
	    dle->clnt_decrypt_opt = tt;
    } else if(g_str_equal(last_element_name, "exclude") ||
	      g_str_equal(last_element_name, "include")) {
	data_user->has_optional = 0;
	amfree(tt);
    } else if(g_str_equal(last_element_name, "file")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "exclude")) {
	    dle->exclude_file = append_sl(dle->exclude_file, tt);
	} else if (g_str_equal(last_element2_name, "include")) {
	    dle->include_file = append_sl(dle->include_file, tt);
	} else {
	    error("bad file");
	}
    } else if(g_str_equal(last_element_name, "list")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "exclude")) {
	    dle->exclude_list = append_sl(dle->exclude_list, tt);
	} else if (g_str_equal(last_element2_name, "include")) {
	    dle->include_list = append_sl(dle->include_list, tt);
	} else {
	    error("bad list");
	}
    } else if(g_str_equal(last_element_name, "optional")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "exclude")) {
	    dle->exclude_optional = 1;
	} else if (g_str_equal(last_element2_name, "include")) {
	    dle->include_optional = 1;
	} else {
	    error("bad optional");
	}
	data_user->has_optional = 1;
	amfree(tt);
    } else if(g_str_equal(last_element_name, "script")) {
	amfree(tt);
    } else if(g_str_equal(last_element_name, "execute_on")) {
	char *sep;
	char *tt1 = tt;
	do {
	    sep = strchr(tt1,',');
	    if (sep)
		*sep = '\0';
	    if (g_str_equal(tt1, "PRE-AMCHECK"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_AMCHECK;
	    else if (g_str_equal(tt1, "PRE-DLE-AMCHECK"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_DLE_AMCHECK;
	    else if (g_str_equal(tt1, "PRE-HOST-AMCHECK"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_HOST_AMCHECK;
	    else if (g_str_equal(tt1, "POST-AMCHECK"))
		data_user->script->execute_on |= EXECUTE_ON_POST_AMCHECK;
	    else if (g_str_equal(tt1, "POST-DLE-AMCHECK"))
		data_user->script->execute_on |= EXECUTE_ON_POST_DLE_AMCHECK;
	    else if (g_str_equal(tt1, "POST-HOST-AMCHECK"))
		data_user->script->execute_on |= EXECUTE_ON_POST_HOST_AMCHECK;
	    else if (g_str_equal(tt1, "PRE-ESTIMATE"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_ESTIMATE;
	    else if (g_str_equal(tt1, "PRE-DLE-ESTIMATE"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_DLE_ESTIMATE;
	    else if (g_str_equal(tt1, "PRE-HOST-ESTIMATE"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_HOST_ESTIMATE;
	    else if (g_str_equal(tt1, "POST-ESTIMATE"))
		data_user->script->execute_on |= EXECUTE_ON_POST_ESTIMATE;
	    else if (g_str_equal(tt1, "POST-DLE-ESTIMATE"))
		data_user->script->execute_on |= EXECUTE_ON_POST_DLE_ESTIMATE;
	    else if (g_str_equal(tt1, "POST-HOST-ESTIMATE"))
		data_user->script->execute_on |= EXECUTE_ON_POST_HOST_ESTIMATE;
	    else if (g_str_equal(tt1, "PRE-BACKUP"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_BACKUP;
	    else if (g_str_equal(tt1, "PRE-DLE-BACKUP"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_DLE_BACKUP;
	    else if (g_str_equal(tt1, "PRE-HOST-BACKUP"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_HOST_BACKUP;
	    else if (g_str_equal(tt1, "POST-BACKUP"))
		data_user->script->execute_on |= EXECUTE_ON_POST_BACKUP;
	    else if (g_str_equal(tt1, "POST-DLE-BACKUP"))
		data_user->script->execute_on |= EXECUTE_ON_POST_DLE_BACKUP;
	    else if (g_str_equal(tt1, "POST-HOST-BACKUP"))
		data_user->script->execute_on |= EXECUTE_ON_POST_HOST_BACKUP;
	    else if (g_str_equal(tt1, "PRE-RECOVER"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_RECOVER;
	    else if (g_str_equal(tt1, "POST-RECOVER"))
		data_user->script->execute_on |= EXECUTE_ON_POST_RECOVER;
	    else if (g_str_equal(tt1, "PRE-LEVEL-RECOVER"))
		data_user->script->execute_on |= EXECUTE_ON_PRE_LEVEL_RECOVER;
	    else if (g_str_equal(tt1, "POST-LEVEL-RECOVER"))
		data_user->script->execute_on |= EXECUTE_ON_POST_LEVEL_RECOVER;
	    else if (g_str_equal(tt1, "INTER-LEVEL-RECOVER"))
		data_user->script->execute_on |= EXECUTE_ON_INTER_LEVEL_RECOVER;
	    else 
		dbprintf("BAD EXECUTE_ON: %s\n", tt1);
	    if (sep)
		tt1 = sep+1;
	} while (sep);
	amfree(tt);
    } else if(g_str_equal(last_element_name, "execute_where")) {
	if (g_str_equal(tt, "CLIENT")) {
	    data_user->script->execute_where = ES_CLIENT;
	} else {
	    data_user->script->execute_where = ES_SERVER;
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "datapath")) {
	if (g_str_equal(tt, "AMANDA")) {
	    dle->data_path = DATA_PATH_AMANDA;
	} else if (g_str_equal(tt, "DIRECTTCP")) {
	    dle->data_path = DATA_PATH_DIRECTTCP;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: bad datapath value '%s'", tt);
	}
	amfree(tt);
    } else if(g_str_equal(last_element_name, "directtcp")) {
	dle->directtcp_list = g_slist_append(dle->directtcp_list, tt);
    } else if(g_str_equal(last_element_name, "client_name")) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid client_name text");
	}
	last_element2_name = last_element2->data;
	if (g_str_equal(last_element2_name, "backup-program")) {
	    dle->application_client_name = tt;
	} else if (g_str_equal(last_element2_name, "script")) {
	    data_user->script->client_name = tt;
	} else {
	    error("client_name outside of script or backup-program");
	}
    } else {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: amtext not defined for '%s'", last_element_name);
	amfree(tt);
	return;
    }
}

dle_t *
amxml_parse_node_CHAR(
    char *txt,
    char **errmsg)
{
    amgxml_t             amgxml = {NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
    GMarkupParser        parser = {&amstart_element, &amend_element, &amtext,
				   NULL, NULL};
    GMarkupParseFlags    flags = 0;
    GMarkupParseContext *context;
    GError		*gerror = NULL;

    (void)errmsg;

    context = g_markup_parse_context_new(&parser, flags, &amgxml, NULL);

    g_markup_parse_context_parse(context, txt, strlen(txt), &gerror);
    if (!gerror)
	g_markup_parse_context_end_parse(context, &gerror);
    g_markup_parse_context_free(context);
    if (gerror) {
	if (errmsg)
	    *errmsg = g_strdup(gerror->message);
	g_error_free(gerror);
    }
    return amgxml.dles;
	
}

dle_t *
amxml_parse_node_FILE(
    FILE *file,
    char **errmsg)
{
    amgxml_t             amgxml = {NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
    GMarkupParser        parser = {&amstart_element, &amend_element, &amtext,
				   NULL, NULL};
    GMarkupParseFlags    flags = 0;
    GMarkupParseContext *context;
    GError		*gerror = NULL;
    char                *line;

    (void)errmsg;

    context = g_markup_parse_context_new(&parser, flags, &amgxml, NULL);

    while ((line = pgets(file)) != NULL && !gerror) {
g_debug("line: %s",line);
	g_markup_parse_context_parse(context, line, strlen(line), &gerror);
	amfree(line);
    }
    amfree(line);
    if (!gerror)
	g_markup_parse_context_end_parse(context, &gerror);
    g_markup_parse_context_free(context);
    if (gerror) {
	if (errmsg)
	    *errmsg = g_strdup(gerror->message);
	g_error_free(gerror);
    }
    return amgxml.dles;
}

char *
amxml_format_tag(
    char *tag,
    char *value)
{
    char *b64value;
    char *c;
    int   need_raw;
    char *result;
    char *quoted_value;
    char *q;

    quoted_value = malloc(strlen(value)+1);
    q = quoted_value;
    need_raw = 0;
    for(c=value; *c != '\0'; c++) {
	// Check include negative value, with the 8th bit set.
	if (*c <= ' ' ||
	    (unsigned char)*c > 127 ||
	    *c == '<' ||
	    *c == '>' ||
	    *c == '"' ||
	    *c == '&' ||
	    *c == '\\' ||
	    *c == '\'' ||
	    *c == '\t' ||
	    *c == '\f' ||
	    *c == '\r' ||
	    *c == '\n') {
	    need_raw = 1;
	    *q++ = '_';
	} else {
	    *q++ = *c;
	}
    }
    *q = '\0';

    if (need_raw) {
	base64_encode_alloc(value, strlen(value), &b64value);
	result = g_strjoin(NULL, "<", tag,
			   " encoding=\"raw\" raw=\"", b64value, "\">",
			   quoted_value,
			   "</", tag, ">",
			   NULL);
	amfree(b64value);
    } else {
	result = g_strjoin(NULL, "<", tag, ">",
			   value,
			   "</", tag, ">",
			   NULL);
    }
    amfree(quoted_value);

    return result;
}
