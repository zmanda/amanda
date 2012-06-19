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
 * $Id: amxml.c 5151 2007-02-06 15:41:53Z martineau $
 *
 * xml parsing of amanda protocol packet
 */

#include "amanda.h"
#include "util.h"
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

    dle = malloc(sizeof(dle_t));
    init_dle(dle);
    return dle;
}

void
free_dle(
    dle_t *dle)
{
    scriptlist_t scriptlist;

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

    data_user->raw = NULL;
    data_user->encoding = NULL;

    if (attribute_names) {
	for(at_names = attribute_names, at_values = attribute_values;
	    *at_names != NULL && at_values != NULL;
	    at_names++, at_values++) {
	    if (strcmp(*at_names, "encoding") == 0) {
		data_user->encoding = stralloc(*at_values);
	    } else if (strcmp(*at_names, "raw") == 0) {
		data_user->raw = base64_decode_alloc_string((char *)*at_values);
	    } else {
		g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			    "XML: Invalid attribute '%s' for %s element",
			    *at_names, element_name);
		return;
	    }
	}
    }

    if (strcmp(element_name, "dle") == 0) {
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
	data_user->encoding = NULL;
	data_user->raw = NULL;
    } else if(strcmp(element_name, "disk"          ) == 0 ||
	      strcmp(element_name, "diskdevice"    ) == 0 ||
	      strcmp(element_name, "calcsize"      ) == 0 ||
	      strcmp(element_name, "estimate"      ) == 0 ||
	      strcmp(element_name, "program"       ) == 0 ||
	      strcmp(element_name, "auth"          ) == 0 ||
	      strcmp(element_name, "index"         ) == 0 ||
	      strcmp(element_name, "dumpdate"      ) == 0 ||
	      strcmp(element_name, "level"         ) == 0 ||
	      strcmp(element_name, "record"        ) == 0 ||
	      strcmp(element_name, "spindle"       ) == 0 ||
	      strcmp(element_name, "compress"      ) == 0 ||
	      strcmp(element_name, "encrypt"       ) == 0 ||
	      strcmp(element_name, "kencrypt"      ) == 0 ||
	      strcmp(element_name, "datapath"      ) == 0 ||
	      strcmp(element_name, "exclude"       ) == 0 ||
	      strcmp(element_name, "include"       ) == 0) {
	if (strcmp(last_element_name, "dle") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if ((strcmp(element_name, "disk"          ) == 0 && dle->disk) ||
	    (strcmp(element_name, "diskdevice"    ) == 0 && dle->device) ||
	    (strcmp(element_name, "calcsize"      ) == 0 && data_user->has_calcsize) ||
	    (strcmp(element_name, "estimate"      ) == 0 && data_user->has_estimate) ||
	    (strcmp(element_name, "record"        ) == 0 && data_user->has_record) ||
	    (strcmp(element_name, "spindle"       ) == 0 && data_user->has_spindle) ||
	    (strcmp(element_name, "program"       ) == 0 && dle->program) ||
	    (strcmp(element_name, "auth"          ) == 0 && dle->auth) ||
	    (strcmp(element_name, "index"         ) == 0 && data_user->has_index) ||
	    (strcmp(element_name, "dumpdate"      ) == 0 && dle->dumpdate) ||
	    (strcmp(element_name, "compress"      ) == 0 && data_user->has_compress) ||
	    (strcmp(element_name, "encrypt"       ) == 0 && data_user->has_encrypt) ||
	    (strcmp(element_name, "kencrypt"      ) == 0 && data_user->has_kencrypt) ||
	    (strcmp(element_name, "datapath"      ) == 0 && data_user->has_datapath) ||
	    (strcmp(element_name, "exclude"       ) == 0 && data_user->has_exclude) ||
	    (strcmp(element_name, "include"       ) == 0 && data_user->has_include)) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
	if (strcmp(element_name, "calcsize"      ) == 0) data_user->has_calcsize       = 1;
	if (strcmp(element_name, "estimate"      ) == 0) data_user->has_estimate       = 1;
	if (strcmp(element_name, "record"        ) == 0) data_user->has_record         = 1;
	if (strcmp(element_name, "spindle"       ) == 0) data_user->has_spindle        = 1;
	if (strcmp(element_name, "index"         ) == 0) data_user->has_index          = 1;
	if (strcmp(element_name, "compress"      ) == 0) data_user->has_compress       = 1;
	if (strcmp(element_name, "encrypt"       ) == 0) data_user->has_encrypt        = 1;
	if (strcmp(element_name, "kencrypt"      ) == 0) data_user->has_kencrypt       = 1;
	if (strcmp(element_name, "datapath"      ) == 0) data_user->has_datapath       = 1;
	if (strcmp(element_name, "exclude"       ) == 0) data_user->has_exclude        = 1;
	if (strcmp(element_name, "include"       ) == 0) data_user->has_include        = 1;
	if (strcmp(element_name, "exclude") == 0 || strcmp(element_name, "include") == 0)
	   data_user->has_optional = 0;
	if (strcmp(element_name, "level") == 0) {
	    data_user->alevel = g_new0(am_level_t, 1);
	}
    } else if (strcmp(element_name, "server") == 0) {
	if (strcmp(last_element_name, "level") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if(strcmp(element_name, "custom-compress-program") == 0) {
	if (strcmp(last_element_name, "compress") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (dle->compprog) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
    } else if (strcmp(element_name, "custom-encrypt-program") == 0 ||
	       strcmp(element_name, "decrypt-option") == 0) {
	if (strcmp(last_element_name, "encrypt") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (strcmp(element_name, "custom-encrypt-program") == 0 &&
		   dle->clnt_encrypt) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
	if (strcmp(element_name, "decrypt-option") == 0 &&
		   dle->clnt_decrypt_opt) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
    } else if(strcmp(element_name, "plugin") == 0) {
	if (strcmp(last_element_name, "backup-program") != 0 &&
	    strcmp(last_element_name, "script") != 0) {
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
    } else if(strcmp(element_name, "property") == 0) {
	if (!last_element ||
	    (strcmp(last_element_name, "backup-program") != 0 &&
	     strcmp(last_element_name, "script") != 0 &&
	     strcmp(last_element_name, "dle") != 0)) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	data_user->property_data = malloc(sizeof(property_t));
	data_user->property_data->append = 0;
	data_user->property_data->priority = 0;
	data_user->property_data->values = NULL;
    } else if(strcmp(element_name, "name") == 0) {
	if (strcmp(last_element_name, "property") != 0) {
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
    } else if(strcmp(element_name, "priority") == 0) {
	if (strcmp(last_element_name, "property") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if(strcmp(element_name, "value") == 0) {
	if (strcmp(last_element_name, "property") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if(strcmp(element_name, "file") == 0 ||
	      strcmp(element_name, "list") == 0 ||
	      strcmp(element_name, "optional") == 0) {
	if (strcmp(last_element_name, "exclude") != 0 &&
	    strcmp(last_element_name, "include") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
	if (strcmp(element_name, "optional") == 0 && data_user->has_optional) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Duplicate %s element", element_name);
	    return;
	}
	if (strcmp(element_name, "optional") == 0) data_user->has_optional = 1;
    } else if (strcmp(element_name, "backup-program") == 0) {
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
    } else if (strcmp(element_name, "script") == 0) {
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
    } else if (strcmp(element_name, "execute_on") == 0) {
    } else if (strcmp(element_name, "execute_where") == 0) {
    } else if (strcmp(element_name, "directtcp") == 0) {
	if (strcmp(last_element_name, "datapath") != 0) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: Invalid %s element", element_name);
	    return;
	}
    } else if (strcmp(element_name, "client_name") == 0) {
    } else {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: Invalid %s element", element_name);
	return;
    }
    data_user->element_names = g_slist_prepend(data_user->element_names,
					       stralloc(element_name));
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
    if (strcmp(last_element_name, element_name) != 0) {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: Invalid closing tag '%s'", element_name);
	return;
    }

    if (strcmp(element_name, "property") == 0) {
	g_hash_table_insert(data_user->property,
			    data_user->property_name,
			    data_user->property_data);
	data_user->property_name = NULL;
	data_user->property_data = NULL;
    } else if (strcmp(element_name, "dle") == 0) {
	if (dle->program_is_application_api &&
	    !dle->program) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: program set to APPLICATION but no application set");
	    return;
	}
	if (dle->device == NULL && dle->disk)
	    dle->device = stralloc(dle->disk);
	if (dle->estimatelist == NULL)
	    dle->estimatelist = g_slist_append(dle->estimatelist, ES_CLIENT);
/* Add check of required field */
	data_user->property = NULL;
	data_user->dle = NULL;
    } else if (strcmp(element_name, "backup-program") == 0) {
	if (dle->program == NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: No plugin set for application");
	    return;
	}
	dle->application_property = data_user->property;
	data_user->property = dle->property;
    } else if (strcmp(element_name, "script") == 0) {
	if (data_user->script->plugin == NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: No plugin set for script");
	    return;
	}
	data_user->script->property = data_user->property;
	data_user->property = dle->property;
	dle->scriptlist = g_slist_append(dle->scriptlist, data_user->script);
	data_user->script = NULL;
    } else if (strcmp(element_name, "level") == 0) {
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
	tt = stralloc(data_user->raw);
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

    if (strcmp(last_element_name, "dle") == 0 ||
	strcmp(last_element_name, "backup-program") == 0 ||
	strcmp(last_element_name, "exclude") == 0 ||
	strcmp(last_element_name, "include") == 0) {
	g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
		    "XML: %s doesn't have text '%s'", last_element_name, tt);
	amfree(tt);
	return;
    } else if(strcmp(last_element_name, "disk") == 0) {
	if (dle->disk != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->disk = tt;
    } else if(strcmp(last_element_name, "diskdevice") == 0) {
	if (dle->device != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->device = tt;
    } else if(strcmp(last_element_name, "calcsize") == 0) {
	if (strcasecmp(tt,"yes") == 0) {
	    dle->estimatelist = g_slist_append(dle->estimatelist,
					       GINT_TO_POINTER(ES_CALCSIZE));
	}
	amfree(tt);
    } else if(strcmp(last_element_name, "estimate") == 0) {
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
    } else if(strcmp(last_element_name, "program") == 0) {
	if (dle->program != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	if (strcmp(tt, "APPLICATION") == 0) {
	    dle->program_is_application_api = 1;
	    dle->program = NULL;
	    amfree(tt);
	} else {
	    dle->program = tt;
	}
    } else if(strcmp(last_element_name, "plugin") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid name text");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "backup-program") == 0) {
	    dle->program = tt;
	} else if (strcmp(last_element2_name, "script") == 0) {
	    data_user->script->plugin = tt;
	} else {
	    error("plugin outside of backup-program");
	}
	data_user->has_plugin = 1;
    } else if(strcmp(last_element_name, "name") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid name text");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "property") == 0) {
	    data_user->property_name = tt;
	} else {
	    error("name outside of property");
	}
    } else if(strcmp(last_element_name, "priority") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid priority text");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "property") == 0) {
	    if (strcasecmp(tt,"yes") == 0) {
		data_user->property_data->priority = 1;
	    }
	} else {
	    error("priority outside of property");
	}
	amfree(tt);
    } else if(strcmp(last_element_name, "value") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid name text");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "property") == 0) {
	    data_user->property_data->values =
			g_slist_append(data_user->property_data->values, tt);
	} else {
	    error("value outside of property");
	}
    } else if(strcmp(last_element_name, "auth") == 0) {
	if (dle->auth != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    return;
	}
	dle->auth = tt;
    } else if(strcmp(last_element_name, "level") == 0) {
	data_user->alevel->level = atoi(tt);
	amfree(tt);
    } else if (strcmp(last_element_name, "server") == 0) {
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
    } else if(strcmp(last_element_name, "index") == 0) {
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
    } else if(strcmp(last_element_name, "dumpdate") == 0) {
	if (dle->dumpdate != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->dumpdate = tt;
    } else if(strcmp(last_element_name, "record") == 0) {
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
    } else if(strcmp(last_element_name, "spindle") == 0) {
	dle->spindle = atoi(tt);
	amfree(tt);
    } else if(strcmp(last_element_name, "compress") == 0) {
	if (strcmp(tt, "FAST") == 0) {
	    dle->compress = COMP_FAST;
	} else if (strcmp(tt, "BEST") == 0) {
	    dle->compress = COMP_BEST;
	} else if (BSTRNCMP(tt, "CUSTOM") == 0) {
	    dle->compress = COMP_CUST;
	} else if (strcmp(tt, "SERVER-FAST") == 0) {
	    dle->compress = COMP_SERVER_FAST;
	} else if (strcmp(tt, "SERVER-BEST") == 0) {
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
    } else if(strcmp(last_element_name, "custom-compress-program") == 0) {
	if (dle->compprog != NULL) {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: multiple text in %s", last_element_name);
	    amfree(tt);
	    return;
	}
	dle->compprog = tt;
    } else if(strcmp(last_element_name, "encrypt") == 0) {
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
    } else if(strcmp(last_element_name, "kencrypt") == 0) {
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
    } else if(strcmp(last_element_name, "custom-encrypt-program") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (dle->encrypt == ENCRYPT_SERV_CUST)
	    dle->srv_encrypt = tt;
	else
	    dle->clnt_encrypt = tt;
    } else if(strcmp(last_element_name, "decrypt-option") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (dle->encrypt == ENCRYPT_SERV_CUST)
	    dle->srv_decrypt_opt = tt;
	else
	    dle->clnt_decrypt_opt = tt;
    } else if(strcmp(last_element_name, "exclude") == 0 ||
	      strcmp(last_element_name, "include") == 0) {
	data_user->has_optional = 0;
	amfree(tt);
    } else if(strcmp(last_element_name, "file") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "exclude") == 0) {
	    dle->exclude_file = append_sl(dle->exclude_file, tt);
	} else if (strcmp(last_element2_name, "include") == 0) {
	    dle->include_file = append_sl(dle->include_file, tt);
	} else {
	    error("bad file");
	}
    } else if(strcmp(last_element_name, "list") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "exclude") == 0) {
	    dle->exclude_list = append_sl(dle->exclude_list, tt);
	} else if (strcmp(last_element2_name, "include") == 0) {
	    dle->include_list = append_sl(dle->include_list, tt);
	} else {
	    error("bad list");
	}
    } else if(strcmp(last_element_name, "optional") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("XML: optional");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "exclude") == 0) {
	    dle->exclude_optional = 1;
	} else if (strcmp(last_element2_name, "include") == 0) {
	    dle->include_optional = 1;
	} else {
	    error("bad optional");
	}
	data_user->has_optional = 1;
	amfree(tt);
    } else if(strcmp(last_element_name, "script") == 0) {
	amfree(tt);
    } else if(strcmp(last_element_name, "execute_on") == 0) {
	char *sep;
	char *tt1 = tt;
	do {
	    sep = strchr(tt1,',');
	    if (sep)
		*sep = '\0';
	    if (strcmp(tt1,"PRE-AMCHECK") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_AMCHECK;
	    else if (strcmp(tt1,"PRE-DLE-AMCHECK") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_DLE_AMCHECK;
	    else if (strcmp(tt1,"PRE-HOST-AMCHECK") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_HOST_AMCHECK;
	    else if (strcmp(tt1,"POST-AMCHECK") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_AMCHECK;
	    else if (strcmp(tt1,"POST-DLE-AMCHECK") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_DLE_AMCHECK;
	    else if (strcmp(tt1,"POST-HOST-AMCHECK") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_HOST_AMCHECK;
	    else if (strcmp(tt1,"PRE-ESTIMATE") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_ESTIMATE;
	    else if (strcmp(tt1,"PRE-DLE-ESTIMATE") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_DLE_ESTIMATE;
	    else if (strcmp(tt1,"PRE-HOST-ESTIMATE") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_HOST_ESTIMATE;
	    else if (strcmp(tt1,"POST-ESTIMATE") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_ESTIMATE;
	    else if (strcmp(tt1,"POST-DLE-ESTIMATE") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_DLE_ESTIMATE;
	    else if (strcmp(tt1,"POST-HOST-ESTIMATE") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_HOST_ESTIMATE;
	    else if (strcmp(tt1,"PRE-BACKUP") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_BACKUP;
	    else if (strcmp(tt1,"PRE-DLE-BACKUP") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_DLE_BACKUP;
	    else if (strcmp(tt1,"PRE-HOST-BACKUP") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_HOST_BACKUP;
	    else if (strcmp(tt1,"POST-BACKUP") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_BACKUP;
	    else if (strcmp(tt1,"POST-DLE-BACKUP") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_DLE_BACKUP;
	    else if (strcmp(tt1,"POST-HOST-BACKUP") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_HOST_BACKUP;
	    else if (strcmp(tt1,"PRE-RECOVER") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_RECOVER;
	    else if (strcmp(tt1,"POST-RECOVER") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_RECOVER;
	    else if (strcmp(tt1,"PRE-LEVEL-RECOVER") == 0)
		data_user->script->execute_on |= EXECUTE_ON_PRE_LEVEL_RECOVER;
	    else if (strcmp(tt1,"POST-LEVEL-RECOVER") == 0)
		data_user->script->execute_on |= EXECUTE_ON_POST_LEVEL_RECOVER;
	    else if (strcmp(tt1,"INTER-LEVEL-RECOVER") == 0)
		data_user->script->execute_on |= EXECUTE_ON_INTER_LEVEL_RECOVER;
	    else 
		dbprintf("BAD EXECUTE_ON: %s\n", tt1);
	    if (sep)
		tt1 = sep+1;
	} while (sep);
	amfree(tt);
    } else if(strcmp(last_element_name, "execute_where") == 0) {
	if (strcmp(tt, "CLIENT") == 0) {
	    data_user->script->execute_where = ES_CLIENT;
	} else {
	    data_user->script->execute_where = ES_SERVER;
	}
	amfree(tt);
    } else if(strcmp(last_element_name, "datapath") == 0) {
	if (strcmp(tt, "AMANDA") == 0) {
	    dle->data_path = DATA_PATH_AMANDA;
	} else if (strcmp(tt, "DIRECTTCP") == 0) {
	    dle->data_path = DATA_PATH_DIRECTTCP;
	} else {
	    g_set_error(gerror, G_MARKUP_ERROR, G_MARKUP_ERROR_INVALID_CONTENT,
			"XML: bad datapath value '%s'", tt);
	}
	amfree(tt);
    } else if(strcmp(last_element_name, "directtcp") == 0) {
	dle->directtcp_list = g_slist_append(dle->directtcp_list, tt);
    } else if(strcmp(last_element_name, "client_name") == 0) {
	last_element2 = g_slist_nth(data_user->element_names, 1);
	if (!last_element2) {
	    error("Invalid client_name text");
	}
	last_element2_name = last_element2->data;
	if (strcmp(last_element2_name, "backup-program") == 0) {
	    dle->application_client_name = tt;
g_debug("set dle->application_client_name: %s", dle->application_client_name);
	} else if (strcmp(last_element2_name, "script") == 0) {
	    data_user->script->client_name = tt;
g_debug("set data_user->script->client_name: %s", data_user->script->client_name);
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
	    *errmsg = stralloc(gerror->message);
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

    while ((line = agets(file)) != NULL && !gerror) {
	g_markup_parse_context_parse(context, line, strlen(line), &gerror);
	amfree(line);
    }
    if (!gerror)
	g_markup_parse_context_end_parse(context, &gerror);
    g_markup_parse_context_free(context);
    if (gerror) {
	if (errmsg)
	    *errmsg = stralloc(gerror->message);
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
	result = vstralloc("<", tag,
			   " encoding=\"raw\" raw=\"", b64value, "\">",
			   quoted_value,
			   "</", tag, ">",
			   NULL);
	amfree(b64value);
    } else {
	result = vstralloc("<", tag, ">",
			   value,
			   "</", tag, ">",
			   NULL);
    }
    amfree(quoted_value);

    return result;
}
