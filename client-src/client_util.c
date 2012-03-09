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
 * $Id: client_util.c,v 1.34 2006/05/25 01:47:11 johnfranks Exp $
 *
 */

#include "amanda.h"
#include "conffile.h"
#include "client_util.h"
#include "getfsent.h"
#include "util.h"
#include "glib-util.h"
#include "timestamp.h"
#include "pipespawn.h"
#include "amxml.h"
#include "glob.h"
#include "clock.h"
#include "amandates.h"

#define MAXMAXDUMPS 16

static int add_exclude(FILE *file_exclude, char *aexc, int verbose);
static int add_include(char *disk, char *device, FILE *file_include, char *ainc, int verbose);
static char *build_name(char *disk, char *exin, int verbose);
static char *get_name(char *diskname, char *exin, time_t t, int n);


char *
fixup_relative(
    char *	name,
    char *	device)
{
    char *newname;
    if(*name != '/') {
	char *dirname = amname_to_dirname(device);
	newname = vstralloc(dirname, "/", name , NULL);
	amfree(dirname);
    }
    else {
	newname = stralloc(name);
    }
    return newname;
}

/* GDestroyFunc for a hash table whose values are GSLists contianing malloc'd
 * strings */
static void
destroy_slist_free_full(gpointer list) {
    slist_free_full((GSList *)list, g_free);
}


static char *
get_name(
    char *	diskname,
    char *	exin,
    time_t	t,
    int		n)
{
    char number[NUM_STR_SIZE];
    char *filename;
    char *ts;

    ts = get_timestamp_from_time(t);
    if(n == 0)
	number[0] = '\0';
    else
	g_snprintf(number, SIZEOF(number), "%03d", n - 1);
	
    filename = vstralloc(get_pname(), ".", diskname, ".", ts, number, ".",
			 exin, NULL);
    amfree(ts);
    return filename;
}


static char *
build_name(
    char *	disk,
    char *	exin,
    int		verbose)
{
    int n;
    int fd;
    char *filename = NULL;
    char *afilename = NULL;
    char *diskname;
    time_t curtime;
    char *dbgdir;
    char *e = NULL;
    DIR *d;
    struct dirent *entry;
    char *test_name;
    size_t match_len, d_name_len;
    char *quoted;

    time(&curtime);
    diskname = sanitise_filename(disk);

    dbgdir = stralloc2(AMANDA_TMPDIR, "/");
    if((d = opendir(AMANDA_TMPDIR)) == NULL) {
	error(_("open debug directory \"%s\": %s"),
		AMANDA_TMPDIR, strerror(errno));
	/*NOTREACHED*/
    }
    test_name = get_name(diskname, exin,
			 curtime - (getconf_int(CNF_DEBUG_DAYS) * 24 * 60 * 60), 0);
    match_len = strlen(get_pname()) + strlen(diskname) + 2;
    while((entry = readdir(d)) != NULL) {
	if(is_dot_or_dotdot(entry->d_name)) {
	    continue;
	}
	d_name_len = strlen(entry->d_name);
	if(strncmp(test_name, entry->d_name, match_len) != 0
	   || d_name_len < match_len + 14 + 8
	   || strcmp(entry->d_name+ d_name_len - 7, exin) != 0) {
	    continue;				/* not one of our files */
	}
	if(strcmp(entry->d_name, test_name) < 0) {
	    e = newvstralloc(e, dbgdir, entry->d_name, NULL);
	    (void) unlink(e);                   /* get rid of old file */
	}
    }
    amfree(test_name);
    amfree(e);
    closedir(d);

    n=0;
    do {
	filename = get_name(diskname, exin, curtime, n);
	afilename = newvstralloc(afilename, dbgdir, filename, NULL);
	if((fd=open(afilename, O_WRONLY|O_CREAT|O_APPEND, 0600)) < 0){
	    amfree(afilename);
	    n++;
	}
	else {
	    close(fd);
	}
	amfree(filename);
    } while(!afilename && n < 1000);

    if(afilename == NULL) {
	filename = get_name(diskname, exin, curtime, 0);
	afilename = newvstralloc(afilename, dbgdir, filename, NULL);
	quoted = quote_string(afilename);
	dbprintf(_("Cannot create %s (%s)\n"), quoted, strerror(errno));
	if(verbose) {
	    g_printf(_("ERROR [cannot create %s (%s)]\n"),
			quoted, strerror(errno));
	}
	amfree(quoted);
	amfree(afilename);
	amfree(filename);
    }

    amfree(dbgdir);
    amfree(diskname);

    return afilename;
}


static int
add_exclude(
    FILE *	file_exclude,
    char *	aexc,
    int		verbose)
{
    size_t l;
    char *quoted, *file;

    (void)verbose;	/* Quiet unused parameter warning */

    l = strlen(aexc);
    if(aexc[l-1] == '\n') {
	aexc[l-1] = '\0';
	l--;
    }
    file = quoted = quote_string(aexc);
    if (*file == '"') {
	file[strlen(file) - 1] = '\0';
	file++;
    }
    g_fprintf(file_exclude, "%s\n", file);
    amfree(quoted);
    return 1;
}

static int
add_include(
    char *	disk,
    char *	device,
    FILE *	file_include,
    char *	ainc,
    int		verbose)
{
    size_t l;
    int nb_exp=0;
    char *quoted, *file;

    (void)disk;		/* Quiet unused parameter warning */
    (void)device;	/* Quiet unused parameter warning */

    l = strlen(ainc);
    if(ainc[l-1] == '\n') {
	ainc[l-1] = '\0';
	l--;
    }
    if (strncmp(ainc, "./", 2) != 0) {
        quoted = quote_string(ainc);
        dbprintf(_("include must start with './' (%s)\n"), quoted);
	if(verbose) {
	    g_printf(_("ERROR [include must start with './' (%s)]\n"), quoted);
	}
	amfree(quoted);
    }
    else {
	char *incname = ainc+2;
	int set_root;

        set_root = set_root_privs(1);
	/* Take as is if not root && many '/' */
	if(!set_root && strchr(incname, '/')) {
            file = quoted = quote_string(ainc);
	    if (*file == '"') {
		file[strlen(file) - 1] = '\0';
		file++;
	    }
	    g_fprintf(file_include, "%s\n", file);
	    amfree(quoted);
	    nb_exp++;
	}
	else {
	    int nb;
	    glob_t globbuf;
	    char *cwd;

	    globbuf.gl_offs = 0;

	    cwd = g_get_current_dir();
	    if (chdir(device) != 0) {
		error(_("Failed to chdir(%s): %s\n"), device, strerror(errno));
	    }
	    glob(incname, 0, NULL, &globbuf);
	    if (chdir(cwd) != 0) {
		error(_("Failed to chdir(%s): %s\n"), cwd, strerror(errno));
	    }
	    if (set_root)
		set_root_privs(0);
	    nb_exp = globbuf.gl_pathc;
	    for (nb=0; nb < nb_exp; nb++) {
		file = stralloc2("./", globbuf.gl_pathv[nb]);
		quoted = quote_string(file);
		if (*file == '"') {
		    file[strlen(file) - 1] = '\0';
		    file++;
		}
		g_fprintf(file_include, "%s\n", file);
		amfree(quoted);
		amfree(file);
	    }
	}
    }
    return nb_exp;
}

char *
build_exclude(
    dle_t   *dle,
    int	     verbose)
{
    char *filename;
    FILE *file_exclude;
    FILE *exclude;
    char *aexc;
    sle_t *excl;
    int nb_exclude = 0;
    char *quoted;

    if (dle->exclude_file) nb_exclude += dle->exclude_file->nb_element;
    if (dle->exclude_list) nb_exclude += dle->exclude_list->nb_element;

    if (nb_exclude == 0) return NULL;

    if ((filename = build_name(dle->disk, "exclude", verbose)) != NULL) {
	if ((file_exclude = fopen(filename,"w")) != NULL) {

	    if (dle->exclude_file) {
		for(excl = dle->exclude_file->first; excl != NULL;
		    excl = excl->next) {
		    add_exclude(file_exclude, excl->name,
				verbose && dle->exclude_optional == 0);
		}
	    }

	    if (dle->exclude_list) {
		for(excl = dle->exclude_list->first; excl != NULL;
		    excl = excl->next) {
		    char *exclname = fixup_relative(excl->name, dle->device);
		    if((exclude = fopen(exclname, "r")) != NULL) {
			while ((aexc = agets(exclude)) != NULL) {
			    if (aexc[0] == '\0') {
				amfree(aexc);
				continue;
			    }
			    add_exclude(file_exclude, aexc,
				        verbose && dle->exclude_optional == 0);
			    amfree(aexc);
			}
			fclose(exclude);
		    }
		    else {
			quoted = quote_string(exclname);
			dbprintf(_("Can't open exclude file %s (%s)\n"),
				  quoted, strerror(errno));
			if(verbose && (dle->exclude_optional == 0 ||
				       errno != ENOENT)) {
			    g_printf(_("ERROR [Can't open exclude file %s (%s)]\n"),
				   quoted, strerror(errno));
			}
			amfree(quoted);
		    }
		    amfree(exclname);
		}
	    }
            fclose(file_exclude);
	} else {
	    quoted = quote_string(filename);
	    dbprintf(_("Can't create exclude file %s (%s)\n"),
		      quoted, strerror(errno));
	    if (verbose) {
		g_printf(_("ERROR [Can't create exclude file %s (%s)]\n"),
			quoted, strerror(errno));
	    }
	    amfree(quoted);
	}
    }

    return filename;
}

char *
build_include(
    dle_t   *dle,
    int	     verbose)
{
    char *filename;
    FILE *file_include;
    FILE *include;
    char *ainc = NULL;
    sle_t *incl;
    int nb_include = 0;
    int nb_exp = 0;
    char *quoted;

    if (dle->include_file) nb_include += dle->include_file->nb_element;
    if (dle->include_list) nb_include += dle->include_list->nb_element;

    if (nb_include == 0) return NULL;

    if ((filename = build_name(dle->disk, "include", verbose)) != NULL) {
	if ((file_include = fopen(filename,"w")) != NULL) {

	    if (dle->include_file) {
		for (incl = dle->include_file->first; incl != NULL;
		    incl = incl->next) {
		    nb_exp += add_include(dle->disk, dle->device, file_include,
				  incl->name,
				  verbose && dle->include_optional == 0);
		}
	    }

	    if (dle->include_list) {
		for (incl = dle->include_list->first; incl != NULL;
		    incl = incl->next) {
		    char *inclname = fixup_relative(incl->name, dle->device);
		    if ((include = fopen(inclname, "r")) != NULL) {
			while ((ainc = agets(include)) != NULL) {
			    if (ainc[0] == '\0') {
				amfree(ainc);
				continue;
			    }
			    nb_exp += add_include(dle->disk, dle->device,
						  file_include, ainc,
						  verbose && dle->include_optional == 0);
			    amfree(ainc);
			}
			fclose(include);
		    }
		    else {
			quoted = quote_string(inclname);
			dbprintf(_("Can't open include file %s (%s)\n"),
				  quoted, strerror(errno));
			if (verbose && (dle->include_optional == 0 ||
				       errno != ENOENT)) {
			    g_printf(_("ERROR [Can't open include file %s (%s)]\n"),
				   quoted, strerror(errno));
			}
			amfree(quoted);
		   }
		   amfree(inclname);
		}
	    }
            fclose(file_include);
	} else {
	    quoted = quote_string(filename);
	    dbprintf(_("Can't create include file %s (%s)\n"),
		      quoted, strerror(errno));
	    if (verbose) {
		g_printf(_("ERROR [Can't create include file %s (%s)]\n"),
			quoted, strerror(errno));
	    }
	    amfree(quoted);
	}
    }
	
    if (nb_exp == 0) {
	quoted = quote_string(dle->disk);
	dbprintf(_("Nothing found to include for disk %s\n"), quoted);
	if (verbose && dle->include_optional == 0) {
	    g_printf(_("ERROR [Nothing found to include for disk %s]\n"), quoted);
	}
	amfree(quoted);
    }

    return filename;
}


void
parse_options(
    char         *str,
    dle_t        *dle,
    am_feature_t *fs,
    int           verbose)
{
    char *exc;
    char *inc;
    char *p, *tok;
    char *quoted;

    p = stralloc(str);
    tok = strtok(p,";");

    while (tok != NULL) {
	if(am_has_feature(fs, fe_options_auth)
	   && BSTRNCMP(tok,"auth=") == 0) {
	    if (dle->auth != NULL) {
		quoted = quote_string(tok + 5);
		dbprintf(_("multiple auth option %s\n"), quoted);
		if(verbose) {
		    g_printf(_("ERROR [multiple auth option %s]\n"), quoted);
		}
		amfree(quoted);
	    }
	    dle->auth = stralloc(&tok[5]);
	}
	else if(am_has_feature(fs, fe_options_bsd_auth)
	   && BSTRNCMP(tok, "bsd-auth") == 0) {
	    if (dle->auth != NULL) {
		dbprintf(_("multiple auth option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple auth option]\n"));
		}
	    }
	    dle->auth = stralloc("bsd");
	}
	else if (BSTRNCMP(tok, "compress-fast") == 0) {
	    if (dle->compress != COMP_NONE) {
		dbprintf(_("multiple compress option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple compress option]\n"));
		}
	    }
	    dle->compress = COMP_FAST;
	}
	else if (BSTRNCMP(tok, "compress-best") == 0) {
	    if (dle->compress != COMP_NONE) {
		dbprintf(_("multiple compress option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple compress option]\n"));
		}
	    }
	    dle->compress = COMP_BEST;
	}
	else if (BSTRNCMP(tok, "srvcomp-fast") == 0) {
	    if (dle->compress != COMP_NONE) {
		dbprintf(_("multiple compress option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple compress option]\n"));
		}
	    }
	    dle->compress = COMP_SERVER_FAST;
	}
	else if (BSTRNCMP(tok, "srvcomp-best") == 0) {
	    if (dle->compress != COMP_NONE) {
		dbprintf(_("multiple compress option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple compress option]\n"));
		}
	    }
	    dle->compress = COMP_SERVER_BEST;
	}
	else if (BSTRNCMP(tok, "srvcomp-cust=") == 0) {
	    if (dle->compress != COMP_NONE) {
		dbprintf(_("multiple compress option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple compress option]\n"));
		}
	    }
	    dle->compprog = stralloc(tok + SIZEOF("srvcomp-cust=") -1);
	    dle->compress = COMP_SERVER_CUST;
	}
	else if (BSTRNCMP(tok, "comp-cust=") == 0) {
	    if (dle->compress != COMP_NONE) {
		dbprintf(_("multiple compress option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple compress option]\n"));
		}
	    }
	    dle->compprog = stralloc(tok + SIZEOF("comp-cust=") -1);
	    dle->compress = COMP_CUST;
	    /* parse encryption options */
	} 
	else if (BSTRNCMP(tok, "encrypt-serv-cust=") == 0) {
	    if (dle->encrypt != ENCRYPT_NONE) {
		dbprintf(_("multiple encrypt option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple encrypt option]\n"));
		}
	    }
	    dle->srv_encrypt = stralloc(tok + SIZEOF("encrypt-serv-cust=") -1);
	    dle->encrypt = ENCRYPT_SERV_CUST;
	} 
	else if (BSTRNCMP(tok, "encrypt-cust=") == 0) {
	    if (dle->encrypt != ENCRYPT_NONE) {
		dbprintf(_("multiple encrypt option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple encrypt option]\n"));
		}
	    }
	    dle->clnt_encrypt= stralloc(tok + SIZEOF("encrypt-cust=") -1);
	    dle->encrypt = ENCRYPT_CUST;
	} 
	else if (BSTRNCMP(tok, "server-decrypt-option=") == 0) {
	  dle->srv_decrypt_opt = stralloc(tok + SIZEOF("server-decrypt-option=") -1);
	}
	else if (BSTRNCMP(tok, "client-decrypt-option=") == 0) {
	  dle->clnt_decrypt_opt = stralloc(tok + SIZEOF("client-decrypt-option=") -1);
	}
	else if (BSTRNCMP(tok, "no-record") == 0) {
	    if (dle->record != 1) {
		dbprintf(_("multiple no-record option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple no-record option]\n"));
		}
	    }
	    dle->record = 0;
	}
	else if (BSTRNCMP(tok, "index") == 0) {
	    if (dle->create_index != 0) {
		dbprintf(_("multiple index option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple index option]\n"));
		}
	    }
	    dle->create_index = 1;
	}
	else if (BSTRNCMP(tok, "exclude-optional") == 0) {
	    if (dle->exclude_optional != 0) {
		dbprintf(_("multiple exclude-optional option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple exclude-optional option]\n"));
		}
	    }
	    dle->exclude_optional = 1;
	}
	else if (strcmp(tok, "include-optional") == 0) {
	    if (dle->include_optional != 0) {
		dbprintf(_("multiple include-optional option\n"));
		if (verbose) {
		    g_printf(_("ERROR [multiple include-optional option]\n"));
		}
	    }
	    dle->include_optional = 1;
	}
	else if (BSTRNCMP(tok,"exclude-file=") == 0) {
	    exc = unquote_string(&tok[13]);
	    dle->exclude_file = append_sl(dle->exclude_file, exc);
	    amfree(exc);
	}
	else if (BSTRNCMP(tok,"exclude-list=") == 0) {
	    exc = unquote_string(&tok[13]);
	    dle->exclude_list = append_sl(dle->exclude_list, exc);
	    amfree(exc);
	}
	else if (BSTRNCMP(tok,"include-file=") == 0) {
	    inc = unquote_string(&tok[13]);
	    dle->include_file = append_sl(dle->include_file, inc);
	    amfree(inc);
	}
	else if (BSTRNCMP(tok,"include-list=") == 0) {
	    inc = unquote_string(&tok[13]);
	    dle->include_list = append_sl(dle->include_list, inc);
	    amfree(inc);
	}
	else if (BSTRNCMP(tok,"kencrypt") == 0) {
	    dle->kencrypt = 1;
	}
	else if (strcmp(tok,"|") != 0) {
	    quoted = quote_string(tok);
	    dbprintf(_("unknown option %s\n"), quoted);
	    if (verbose) {
		g_printf(_("ERROR [unknown option: %s]\n"), quoted);
	    }
	    amfree(quoted);
	}
	tok = strtok(NULL, ";");
    }
    amfree(p);
}

void
application_property_add_to_argv(
    GPtrArray *argv_ptr,
    dle_t *dle,
    backup_support_option_t *bsu,
    am_feature_t *amfeatures)
{
    sle_t *incl, *excl;

    if (bsu) {
	if (bsu->include_file && dle->include_file) {
	    for (incl = dle->include_file->first; incl != NULL;
		 incl = incl->next) {
		g_ptr_array_add(argv_ptr, stralloc("--include-file"));
		g_ptr_array_add(argv_ptr, stralloc(incl->name));
	    }
	}
	if (bsu->include_list && dle->include_list) {
	    for (incl = dle->include_list->first; incl != NULL;
		 incl = incl->next) {
		g_ptr_array_add(argv_ptr, stralloc("--include-list"));
		g_ptr_array_add(argv_ptr, stralloc(incl->name));
	    }
	}
	if (bsu->include_optional && dle->include_optional) {
	    g_ptr_array_add(argv_ptr, stralloc("--include-optional"));
	    g_ptr_array_add(argv_ptr, stralloc("yes"));
	}

	if (bsu->exclude_file && dle->exclude_file) {
	    for (excl = dle->exclude_file->first; excl != NULL;
	 	 excl = excl->next) {
		g_ptr_array_add(argv_ptr, stralloc("--exclude-file"));
		g_ptr_array_add(argv_ptr, stralloc(excl->name));
	    }
	}
	if (bsu->exclude_list && dle->exclude_list) {
	    for (excl = dle->exclude_list->first; excl != NULL;
		excl = excl->next) {
		g_ptr_array_add(argv_ptr, stralloc("--exclude-list"));
		g_ptr_array_add(argv_ptr, stralloc(excl->name));
	    }
	}
	if (bsu->exclude_optional && dle->exclude_optional) {
	    g_ptr_array_add(argv_ptr, stralloc("--exclude-optional"));
	    g_ptr_array_add(argv_ptr, stralloc("yes"));
	}

	if (bsu->features && amfeatures) {
	    char *feature_string = am_feature_to_string(amfeatures);
	    g_ptr_array_add(argv_ptr, stralloc("--amfeatures"));
	    g_ptr_array_add(argv_ptr, feature_string);
	}

	if (dle->data_path == DATA_PATH_DIRECTTCP &&
	    bsu->data_path_set & DATA_PATH_DIRECTTCP) {
	    GSList *directtcp;

	    g_ptr_array_add(argv_ptr, stralloc("--data-path"));
	    g_ptr_array_add(argv_ptr, stralloc("directtcp"));
	    for (directtcp = dle->directtcp_list; directtcp != NULL;
						  directtcp = directtcp->next) {
		g_ptr_array_add(argv_ptr, stralloc("--direct-tcp"));
		g_ptr_array_add(argv_ptr, stralloc(directtcp->data));
		break; /* XXX temporary; apps only support one ip:port pair */
	    }
	}
    }

    property_add_to_argv(argv_ptr, dle->application_property);
    return;
}

typedef struct {
    dle_t *dle;
    char *name;
    proplist_t dle_proplist;
    int verbose;
    int good;
} merge_property_t;

static void
merge_property(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p)
{
    char *property_s = key_p;
    property_t *conf_property = value_p;
    merge_property_t *merge_p = user_data_p;
    property_t *dle_property = g_hash_table_lookup(merge_p->dle_proplist,
						   property_s);
    GSList *value;
    char *qdisk = quote_string(merge_p->dle->disk);

    if (dle_property) {
	if (dle_property->priority && conf_property->priority) {
	    if (merge_p->verbose) {
		g_fprintf(stdout,
			 _("ERROR %s (%s) Both server client have priority for property '%s'.\n"),
			 qdisk, merge_p->name, property_s);
	    }
	    g_debug("ERROR %s (%s) Both server client have priority for property '%s'.", qdisk, merge_p->name, property_s);
	    merge_p->good = 0;
	    /* Use client property */
	    g_hash_table_remove(merge_p->dle_proplist, key_p);
            g_hash_table_insert(merge_p->dle_proplist, key_p, conf_property);
	} else if (dle_property->priority) {
	    if (merge_p->verbose) {
		g_fprintf(stdout,
			 _("ERROR %s (%s) Server set priority for property '%s' but client set the property.\n"),
			 qdisk, merge_p->name, property_s);
	    }
	    g_debug("%s (%s) Server set priority for property '%s' but client set the property.", qdisk, merge_p->name, property_s);
	    /* use server property */
	} else if (conf_property->priority) {
	    if (merge_p->verbose) {
		g_fprintf(stdout,
			 _("ERROR %s (%s) Client set priority for property '%s' but server set the property.\n"),
			 qdisk, merge_p->name, property_s);
	    }
	    g_debug("%s (%s) Client set priority for property '%s' but server set the property.", qdisk, merge_p->name, property_s);
	    /* Use client property */
	    g_hash_table_remove(merge_p->dle_proplist, key_p);
            g_hash_table_insert(merge_p->dle_proplist, key_p, conf_property);
	} else if (!conf_property->append) {
	    if (merge_p->verbose) {
		g_fprintf(stdout,
			 _("ERROR %s (%s) Both server and client set property '%s', using client value.\n"),
			 qdisk, merge_p->name, property_s);
	    }
	    g_debug("%s (%s) Both server and client set property '%s', using client value.", qdisk, merge_p->name, property_s);
	    /* Use client property */
	    g_hash_table_remove(merge_p->dle_proplist, key_p);
            g_hash_table_insert(merge_p->dle_proplist, key_p, conf_property);
	} else { /* merge */
	    for (value = conf_property->values; value != NULL;
		 value = value->next) {
		dle_property->values = g_slist_append(dle_property->values,
						      value->data);
	    }
	}
    } else { /* take value from conf */
        g_hash_table_insert(merge_p->dle_proplist, key_p, conf_property);
    }
}

int
merge_properties(
    dle_t      *dle,
    char       *name,
    proplist_t  dle_proplist,
    proplist_t  conf_proplist,
    int         verbose)
{
    merge_property_t merge_p = {dle, name, dle_proplist, verbose, 1};

    if (conf_proplist != NULL) {
	g_hash_table_foreach(conf_proplist,
                             &merge_property,
                             &merge_p);
    }

    return merge_p.good;
}

int
merge_dles_properties(
    dle_t *dles,
    int verbose)
{
    dle_t         *dle;
    application_t *app;
    GSList        *scriptlist;
    pp_script_t   *pp_script;
    int            good = 1;

    for (dle=dles; dle != NULL; dle=dle->next) {
        if (dle->program_is_application_api) {
	    app = NULL;
	    if (dle->application_client_name &&
		strlen(dle->application_client_name) > 0) {
		app = lookup_application(dle->application_client_name);
		if (!app) {
		    char *qamname = quote_string(dle->disk);
		    char *errmsg = vstrallocf("Application '%s' not found on client",
					      dle->application_client_name);
		    char *qerrmsg = quote_string(errmsg);
		    good = 0;
		    if (verbose) {
			g_fprintf(stdout, _("ERROR %s %s\n"), qamname, qerrmsg);
		    }
		    g_debug("%s: %s", qamname, qerrmsg);
		    amfree(qamname);
		    amfree(errmsg);
		    amfree(qerrmsg);
		}
	    } else {
		app = lookup_application(dle->program);
	    }
            if (app) {
                merge_properties(dle, dle->program,
				 dle->application_property,
				 application_get_property(app),
				 verbose);
            }
        }
        for (scriptlist = dle->scriptlist; scriptlist != NULL;
             scriptlist = scriptlist->next) {
            script_t *script =  scriptlist->data;
	    pp_script = NULL;
	    if (script->client_name && strlen(script->client_name) > 0) {
		pp_script = lookup_pp_script(script->client_name);
		if (!pp_script) {
		    char *qamname = quote_string(dle->disk);
		    char *errmsg = vstrallocf("Script '%s' not found on client",
					      script->client_name);
		    char *qerrmsg = quote_string(errmsg);
		    good = 0;
		    if (verbose) {
			g_fprintf(stderr, _("ERROR %s %s\n"), qamname, qerrmsg);
		    }
		    g_debug("%s: %s", qamname, qerrmsg);
		    amfree(qamname);
		    amfree(errmsg);
		    amfree(qerrmsg);
		}
	    } else {
		pp_script = lookup_pp_script(script->plugin);
	    }
            if (pp_script) {
		merge_properties(dle, script->plugin,
				 script->property,
				 pp_script_get_property(pp_script),
				 verbose);
	    }
        }
    }
    return good;
}

backup_support_option_t *
backup_support_option(
    char       *program,
    g_option_t *g_options,
    char       *disk,
    char       *amdevice,
    GPtrArray **errarray)
{
    pid_t   supportpid;
    int     supportin, supportout, supporterr;
    char   *cmd;
    GPtrArray *argv_ptr = g_ptr_array_new();
    FILE   *streamout;
    FILE   *streamerr;
    char   *line;
    int     status;
    char   *err = NULL;
    backup_support_option_t *bsu;

    *errarray = g_ptr_array_new();
    cmd = vstralloc(APPLICATION_DIR, "/", program, NULL);
    g_ptr_array_add(argv_ptr, stralloc(program));
    g_ptr_array_add(argv_ptr, stralloc("support"));
    if (g_options->config) {
	g_ptr_array_add(argv_ptr, stralloc("--config"));
	g_ptr_array_add(argv_ptr, stralloc(g_options->config));
    }
    if (g_options->hostname) {
	g_ptr_array_add(argv_ptr, stralloc("--host"));
	g_ptr_array_add(argv_ptr, stralloc(g_options->hostname));
    }
    if (disk) {
	g_ptr_array_add(argv_ptr, stralloc("--disk"));
	g_ptr_array_add(argv_ptr, stralloc(disk));
    }
    if (amdevice) {
	g_ptr_array_add(argv_ptr, stralloc("--device"));
	g_ptr_array_add(argv_ptr, stralloc(amdevice));
    }
    g_ptr_array_add(argv_ptr, NULL);

    supporterr = fileno(stderr);
    supportpid = pipespawnv(cmd, STDIN_PIPE|STDOUT_PIPE|STDERR_PIPE, 0,
			    &supportin, &supportout, &supporterr,
			    (char **)argv_ptr->pdata);

    aclose(supportin);

    bsu = g_new0(backup_support_option_t, 1);
    bsu->config = 1;
    bsu->host = 1;
    bsu->disk = 1;
    streamout = fdopen(supportout, "r");
    if (!streamout) {
	error(_("Error opening pipe to child: %s"), strerror(errno));
	/* NOTREACHED */
    }
    while((line = agets(streamout)) != NULL) {
	dbprintf(_("support line: %s\n"), line);
	if (strncmp(line,"CONFIG ", 7) == 0) {
	    if (strcmp(line+7, "YES") == 0)
		bsu->config = 1;
	} else if (strncmp(line,"HOST ", 5) == 0) {
	    if (strcmp(line+5, "YES") == 0)
	    bsu->host = 1;
	} else if (strncmp(line,"DISK ", 5) == 0) {
	    if (strcmp(line+5, "YES") == 0)
		bsu->disk = 1;
	} else if (strncmp(line,"INDEX-LINE ", 11) == 0) {
	    if (strcmp(line+11, "YES") == 0)
		bsu->index_line = 1;
	} else if (strncmp(line,"INDEX-XML ", 10) == 0) {
	    if (strcmp(line+10, "YES") == 0)
		bsu->index_xml = 1;
	} else if (strncmp(line,"MESSAGE-LINE ", 13) == 0) {
	    if (strcmp(line+13, "YES") == 0)
		bsu->message_line = 1;
	} else if (strncmp(line,"MESSAGE-XML ", 12) == 0) {
	    if (strcmp(line+12, "YES") == 0)
		bsu->message_xml = 1;
	} else if (strncmp(line,"RECORD ", 7) == 0) {
	    if (strcmp(line+7, "YES") == 0)
		bsu->record = 1;
	} else if (strncmp(line,"INCLUDE-FILE ", 13) == 0) {
	    if (strcmp(line+13, "YES") == 0)
		bsu->include_file = 1;
	} else if (strncmp(line,"INCLUDE-LIST ", 13) == 0) {
	    if (strcmp(line+13, "YES") == 0)
		bsu->include_list = 1;
	} else if (strncmp(line,"INCLUDE-LIST-GLOB ", 17) == 0) {
	    if (strcmp(line+17, "YES") == 0)
		bsu->include_list_glob = 1;
	} else if (strncmp(line,"INCLUDE-OPTIONAL ", 17) == 0) {
	    if (strcmp(line+17, "YES") == 0)
		bsu->include_optional = 1;
	} else if (strncmp(line,"EXCLUDE-FILE ", 13) == 0) {
	    if (strcmp(line+13, "YES") == 0)
		bsu->exclude_file = 1;
	} else if (strncmp(line,"EXCLUDE-LIST ", 13) == 0) {
	    if (strcmp(line+13, "YES") == 0)
		bsu->exclude_list = 1;
	} else if (strncmp(line,"EXCLUDE-LIST-GLOB ", 17) == 0) {
	    if (strcmp(line+17, "YES") == 0)
		bsu->exclude_list_glob = 1;
	} else if (strncmp(line,"EXCLUDE-OPTIONAL ", 17) == 0) {
	    if (strcmp(line+17, "YES") == 0)
		bsu->exclude_optional = 1;
	} else if (strncmp(line,"COLLECTION ", 11) == 0) {
	    if (strcmp(line+11, "YES") == 0)
		bsu->collection = 1;
	} else if (strncmp(line,"CALCSIZE ", 9) == 0) {
	    if (strcmp(line+9, "YES") == 0)
		bsu->calcsize = 1;
	} else if (strncmp(line,"CLIENT-ESTIMATE ", 16) == 0) {
	    if (strcmp(line+16, "YES") == 0)
		bsu->client_estimate = 1;
	} else if (strncmp(line,"MULTI-ESTIMATE ", 15) == 0) {
	    if (strcmp(line+15, "YES") == 0)
		bsu->multi_estimate = 1;
	} else if (strncmp(line,"MAX-LEVEL ", 10) == 0) {
	    bsu->max_level  = atoi(line+10);
	} else if (strncmp(line,"RECOVER-MODE ", 13) == 0) {
	    if (strcasecmp(line+13, "SMB") == 0)
		bsu->smb_recover_mode = 1;
	} else if (strncmp(line,"DATA-PATH ", 10) == 0) {
	    if (strcasecmp(line+10, "AMANDA") == 0)
		bsu->data_path_set |= DATA_PATH_AMANDA;
	    else if (strcasecmp(line+10, "DIRECTTCP") == 0)
		bsu->data_path_set |= DATA_PATH_DIRECTTCP;
	} else if (strncmp(line,"RECOVER-PATH ", 13) == 0) {
	    if (strcasecmp(line+13, "CWD") == 0)
		bsu->recover_path = RECOVER_PATH_CWD;
	    else if (strcasecmp(line+13, "REMOTE") == 0)
		bsu->recover_path = RECOVER_PATH_REMOTE;
	} else if (strncmp(line,"AMFEATURES ", 11) == 0) {
	    if (strcmp(line+11, "YES") == 0)
		bsu->features = 1;
	} else {
	    dbprintf(_("Invalid support line: %s\n"), line);
	}
	amfree(line);
    }
    fclose(streamout);

    if (bsu->data_path_set == 0)
	bsu->data_path_set = DATA_PATH_AMANDA;

    streamerr = fdopen(supporterr, "r");
    if (!streamerr) {
	error(_("Error opening pipe to child: %s"), strerror(errno));
	/* NOTREACHED */
    }
    while((line = agets(streamerr)) != NULL) {
	if (strlen(line) > 0) {
	    g_ptr_array_add(*errarray, line);
	    dbprintf("Application '%s': %s\n", program, line);
	}
	amfree(bsu);
    }
    fclose(streamerr);

    if (waitpid(supportpid, &status, 0) < 0) {
	err = vstrallocf(_("waitpid failed: %s"), strerror(errno));
    } else if (!WIFEXITED(status)) {
	err = vstrallocf(_("exited with signal %d"), WTERMSIG(status));
    } else if (WEXITSTATUS(status) != 0) {
	err = vstrallocf(_("exited with status %d"), WEXITSTATUS(status));
    }

    if (err) {
	g_ptr_array_add(*errarray, err);
	dbprintf("Application '%s': %s\n", program, err);
	amfree(bsu);
    }
    g_ptr_array_free_full(argv_ptr);
    amfree(cmd);
    return bsu;
}

void
run_client_script(
    script_t     *script,
    execute_on_t  execute_on,
    g_option_t   *g_options,
    dle_t	 *dle)
{
    pid_t     scriptpid;
    int       scriptin, scriptout, scripterr;
    char     *cmd;
    GPtrArray *argv_ptr = g_ptr_array_new();
    FILE     *streamout;
    FILE     *streamerr;
    char     *line;
    amwait_t  wait_status;
    char     *command = NULL;

    if ((script->execute_on & execute_on) == 0)
	return;
    if (script->execute_where != ES_CLIENT)
	return;

    cmd = vstralloc(APPLICATION_DIR, "/", script->plugin, NULL);
    g_ptr_array_add(argv_ptr, stralloc(script->plugin));

    switch (execute_on) {
    case EXECUTE_ON_PRE_DLE_AMCHECK:
	command = "PRE-DLE-AMCHECK";
	break;
    case EXECUTE_ON_PRE_HOST_AMCHECK:
	command = "PRE-HOST-AMCHECK";
	break;
    case EXECUTE_ON_POST_DLE_AMCHECK:
	command = "POST-DLE-AMCHECK";
	break;
    case EXECUTE_ON_POST_HOST_AMCHECK:
	command = "POST-HOST-AMCHECK";
	break;
    case EXECUTE_ON_PRE_DLE_ESTIMATE:
	command = "PRE-DLE-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_HOST_ESTIMATE:
	command = "PRE-HOST-ESTIMATE";
	break;
    case EXECUTE_ON_POST_DLE_ESTIMATE:
	command = "POST-DLE-ESTIMATE";
	break;
    case EXECUTE_ON_POST_HOST_ESTIMATE:
	command = "POST-HOST-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_DLE_BACKUP:
	command = "PRE-DLE-BACKUP";
	break;
    case EXECUTE_ON_PRE_HOST_BACKUP:
	command = "PRE-HOST-BACKUP";
	break;
    case EXECUTE_ON_POST_DLE_BACKUP:
	command = "POST-DLE-BACKUP";
	break;
    case EXECUTE_ON_POST_HOST_BACKUP:
	command = "POST-HOST-BACKUP";
	break;
    case EXECUTE_ON_PRE_RECOVER:
	command = "PRE-RECOVER";
	break;
    case EXECUTE_ON_POST_RECOVER:
	command = "POST-RECOVER";
	break;
    case EXECUTE_ON_PRE_LEVEL_RECOVER:
	command = "PRE-LEVEL-RECOVER";
	break;
    case EXECUTE_ON_POST_LEVEL_RECOVER:
	command = "POST-LEVEL-RECOVER";
	break;
    case EXECUTE_ON_INTER_LEVEL_RECOVER:
	command = "INTER-LEVEL-RECOVER";
	break;
    }
    g_ptr_array_add(argv_ptr, stralloc(command));
    g_ptr_array_add(argv_ptr, stralloc("--execute-where"));
    g_ptr_array_add(argv_ptr, stralloc("client"));

    if (g_options->config) {
	g_ptr_array_add(argv_ptr, stralloc("--config"));
	g_ptr_array_add(argv_ptr, stralloc(g_options->config));
    }
    if (g_options->hostname) {
	g_ptr_array_add(argv_ptr, stralloc("--host"));
	g_ptr_array_add(argv_ptr, stralloc(g_options->hostname));
    }
    if (dle->disk) {
	g_ptr_array_add(argv_ptr, stralloc("--disk"));
	g_ptr_array_add(argv_ptr, stralloc(dle->disk));
    }
    if (dle->device) {
	g_ptr_array_add(argv_ptr, stralloc("--device"));
	g_ptr_array_add(argv_ptr, stralloc(dle->device));
    }
    if (dle->levellist) {
	levellist_t levellist;
	char number[NUM_STR_SIZE];
	for (levellist=dle->levellist; levellist; levellist=levellist->next) {
	    am_level_t *alevel = (am_level_t *)levellist->data;
	    g_ptr_array_add(argv_ptr, stralloc("--level"));
	    g_snprintf(number, SIZEOF(number), "%d", alevel->level);
	    g_ptr_array_add(argv_ptr, stralloc(number));
	}
    }
    property_add_to_argv(argv_ptr, script->property);
    g_ptr_array_add(argv_ptr, NULL);

    scriptpid = pipespawnv(cmd, STDIN_PIPE|STDOUT_PIPE|STDERR_PIPE, 0,
			   &scriptin, &scriptout, &scripterr,
			   (char **)argv_ptr->pdata);

    close(scriptin);

    script->result = g_new0(client_script_result_t, 1);
    script->result->proplist =
		  g_hash_table_new_full(g_str_hash, g_str_equal,
					&g_free, &destroy_slist_free_full);
    script->result->output = g_ptr_array_new();
    script->result->err = g_ptr_array_new();

    streamout = fdopen(scriptout, "r");
    if (streamout) {
        while((line = agets(streamout)) != NULL) {
            dbprintf("script: %s\n", line);
            if (BSTRNCMP(line, "PROPERTY ") == 0) {
		char *property_name, *property_value;
		property_name = line + 9;
		property_value = strchr(property_name,' ');
		if (property_value == NULL) {
		    char *msg = g_strdup_printf(
					"ERROR %s: Bad output property: %s",
					script->plugin, line);
		    g_ptr_array_add(script->result->output, msg);
		} else {
		    property_t *property;

		    *property_value++ = '\0';
		    property_name = stralloc(property_name);
		    property_value = stralloc(property_value);
		    property = g_hash_table_lookup(script->result->proplist,
						   property_name);
		    if (!property) {
			property = g_new0(property_t, 1);
			g_hash_table_insert(script->result->proplist,
					    property_name, property);
		    }
		    property->values = g_slist_append(property->values,
						      property_value);
		}
		amfree(line);
            } else {
                g_ptr_array_add(script->result->output, line);
            }
        }
    }
    fclose(streamout);

    streamerr = fdopen(scripterr, "r");
    if (streamerr) {
        while((line = agets(streamerr)) != NULL) {
	    g_ptr_array_add(script->result->err,
			    g_strdup_printf(_("Script '%s' command '%s': %s"),
					    script->plugin, command, line));
	    amfree(line);
	}
    }

    waitpid(scriptpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	g_ptr_array_add(script->result->err,
			g_strdup_printf(_("Script '%s' command '%s' terminated with signal %d: see %s"),
					script->plugin, command,
					WTERMSIG(wait_status),
					dbfn()));
    } else if (WIFEXITED(wait_status)) {
        if (WEXITSTATUS(wait_status) != 0) {
	    g_ptr_array_add(script->result->err,
			    g_strdup_printf(_("Script '%s' command '%s' exited with status %d: see %s"),
					    script->plugin, command,
					    WEXITSTATUS(wait_status),
					    dbfn()));
        } else {
            /* Normal exit */
        }
    }
    amfree(cmd);
    g_ptr_array_free_full(argv_ptr);
}

void run_client_script_output(gpointer data, gpointer user_data);
void run_client_script_err_amcheck(gpointer data, gpointer user_data);
void run_client_script_err_estimate(gpointer data, gpointer user_data);
void run_client_script_err_backup(gpointer data, gpointer user_data);
void run_client_script_err_recover(gpointer data, gpointer user_data);

typedef struct script_output_s {
    FILE  *stream;
    dle_t *dle;
} script_output_t;

void
run_client_script_output(
    gpointer data,
    gpointer user_data)
{
    char            *line = data;
    script_output_t *so   = user_data;

    if (line && so->stream) {
	g_fprintf(so->stream, "%s\n", line);
    }
}

void
run_client_script_err_amcheck(
    gpointer data,
    gpointer user_data)
{
    char            *line  = data;
    script_output_t *so    = user_data;

    if (line && so->stream) {
	g_fprintf(so->stream, "ERROR %s\n", line);
    }
}

void
run_client_script_err_estimate(
    gpointer data,
    gpointer user_data)
{
    char            *line  = data;
    script_output_t *so    = user_data;

    if (line && so->stream) {
	char *qdisk = quote_string(so->dle->disk);
	g_fprintf(so->stream, "%s 0 WARNING \"%s\"\n", qdisk, line);
	amfree(qdisk);
    }
}

void
run_client_script_err_backup(
    gpointer data,
    gpointer user_data)
{
    char            *line  = data;
    script_output_t *so    = user_data;

    if (line && so->stream) {
	g_fprintf(so->stream, "? %s\n", line);
    }
}

void
run_client_script_err_recover(
    gpointer data,
    gpointer user_data)
{
    char            *line  = data;
    script_output_t *so    = user_data;

    if (line && so->stream) {
	g_fprintf(so->stream, "%s\n", line);
    }
}

void
run_client_scripts(
    execute_on_t  execute_on,
    g_option_t   *g_options,
    dle_t	 *dle,
    FILE         *streamout)
{
    GSList          *scriptlist;
    script_t        *script;
    GFunc            client_script_err = NULL;
    script_output_t  so = { streamout, dle };

    for (scriptlist = dle->scriptlist; scriptlist != NULL;
	 scriptlist = scriptlist->next) {
	script = (script_t *)scriptlist->data;
	run_client_script(script, execute_on, g_options, dle);
	if (script->result && script->result->output) {
	    g_ptr_array_foreach(script->result->output,
				run_client_script_output,
				&so);
	    g_ptr_array_free(script->result->output, TRUE);
	    script->result->output = NULL;
	}
	if (script->result && script->result->err) {
	    switch (execute_on) {
	    case EXECUTE_ON_PRE_DLE_AMCHECK:
	    case EXECUTE_ON_PRE_HOST_AMCHECK:
	    case EXECUTE_ON_POST_DLE_AMCHECK:
	    case EXECUTE_ON_POST_HOST_AMCHECK:
		 client_script_err = run_client_script_err_amcheck;
		 break;
	    case EXECUTE_ON_PRE_DLE_ESTIMATE:
	    case EXECUTE_ON_PRE_HOST_ESTIMATE:
	    case EXECUTE_ON_POST_DLE_ESTIMATE:
	    case EXECUTE_ON_POST_HOST_ESTIMATE:
		 if (am_has_feature(g_options->features,
				    fe_sendsize_rep_warning)) {
		     client_script_err = run_client_script_err_estimate;
		 }
		 break;
	    case EXECUTE_ON_PRE_DLE_BACKUP:
	    case EXECUTE_ON_PRE_HOST_BACKUP:
	    case EXECUTE_ON_POST_DLE_BACKUP:
	    case EXECUTE_ON_POST_HOST_BACKUP:
		 client_script_err = run_client_script_err_backup;
		 break;
	    case EXECUTE_ON_PRE_RECOVER:
	    case EXECUTE_ON_POST_RECOVER:
	    case EXECUTE_ON_PRE_LEVEL_RECOVER:
	    case EXECUTE_ON_POST_LEVEL_RECOVER:
	    case EXECUTE_ON_INTER_LEVEL_RECOVER:
		 client_script_err = run_client_script_err_recover;
	    }
	    if (client_script_err != NULL) {
		g_ptr_array_foreach(script->result->err,
				    client_script_err,
				    &so);
	    }
	    g_ptr_array_free(script->result->err, TRUE);
	    script->result->err = NULL;
	}
    }
}


void
run_calcsize(
    char   *config,
    char   *program,
    char   *disk,
    char   *dirname,
    GSList *levels,
    char   *file_exclude,
    char   *file_include)
{
    char        *cmd, *cmdline;
    char	*command;
    GPtrArray   *argv_ptr = g_ptr_array_new();
    char         tmppath[PATH_MAX];
    char         number[NUM_STR_SIZE];
    GSList      *alevel;
    guint        level;
    guint        i;
    char        *match_expr;
    int          pipefd = -1, nullfd = -1;
    pid_t        calcpid;
    times_t      start_time;
    FILE        *dumpout = NULL;
    int          dumpsince;
    char        *errmsg = NULL;
    char        *line = NULL;
    amwait_t     wait_status;
    int          len;
    char        *qdisk;
    amandates_t *amdp;
    char        *amandates_file;

    qdisk = quote_string(disk);

    amandates_file = getconf_str(CNF_AMANDATES);
    if(!start_amandates(amandates_file, 0)) {
	char *errstr = strerror(errno);
	char *errmsg = vstrallocf(_("could not open %s: %s"), amandates_file, errstr);
	char *qerrmsg = quote_string(errmsg);
	g_printf(_("ERROR %s\n"), qerrmsg);
	amfree(qdisk);
	amfree(errmsg);
	amfree(qerrmsg);
	return;
    }

    startclock();
    cmd = vstralloc(amlibexecdir, "/", "calcsize", NULL);


    g_ptr_array_add(argv_ptr, stralloc("calcsize"));
    if (config)
	g_ptr_array_add(argv_ptr, stralloc(config));
    else
	g_ptr_array_add(argv_ptr, stralloc("NOCONFIG"));

    g_ptr_array_add(argv_ptr, stralloc(program));

    canonicalize_pathname(disk, tmppath);
    g_ptr_array_add(argv_ptr, stralloc(tmppath));
    canonicalize_pathname(dirname, tmppath);
    g_ptr_array_add(argv_ptr, stralloc(tmppath));

    if (file_exclude) {
	g_ptr_array_add(argv_ptr, stralloc("-X"));
	g_ptr_array_add(argv_ptr, stralloc(file_exclude));
    }

    if (file_include) {
	g_ptr_array_add(argv_ptr, stralloc("-I"));
	g_ptr_array_add(argv_ptr, stralloc(file_include));
    }

    for (alevel = levels; alevel != NULL; alevel = alevel->next) {
	amdp = amandates_lookup(disk);
	level = GPOINTER_TO_INT(alevel->data);
	dbprintf("level: %d\n", level);
	dumpsince = 0;
	for (i=0; i < level; i++) {
	    if (dumpsince < amdp->dates[i])
		dumpsince = amdp->dates[i];
	}
	g_snprintf(number, SIZEOF(number), "%d", level);
	g_ptr_array_add(argv_ptr, stralloc(number));
	g_snprintf(number, SIZEOF(number), "%d", dumpsince);
	g_ptr_array_add(argv_ptr, stralloc(number));
    }

    g_ptr_array_add(argv_ptr, NULL);
    command = (char *)g_ptr_array_index(argv_ptr, 0);
    cmdline = stralloc(command);
    for(i = 1; i < argv_ptr->len - 1; i++)
	cmdline = vstrextend(&cmdline, " ",
			     (char *)g_ptr_array_index(argv_ptr,i), NULL);
    dbprintf(_("running: \"%s\"\n"), cmdline);
    amfree(cmdline);

    start_time = curclock();

    fflush(stderr); fflush(stdout);

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	errmsg = vstrallocf(_("Cannot access /dev/null : %s"),
			    strerror(errno));
	dbprintf("%s\n", errmsg);
	goto common_exit;
    }

    calcpid = pipespawnv(cmd, STDERR_PIPE, 0,
			 &nullfd, &nullfd, &pipefd, (char **)argv_ptr->pdata);
    amfree(cmd);

    dumpout = fdopen(pipefd,"r");
    if (!dumpout) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    match_expr = vstralloc(" %d SIZE %lld", NULL);
    len = strlen(qdisk);
    for(; (line = agets(dumpout)) != NULL; free(line)) {
	long long size_ = (long long)0;
	if (line[0] == '\0' || (int)strlen(line) <= len)
	    continue;
	/* Don't use sscanf for qdisk because it can have a '%'. */
	if (strncmp(line, qdisk, len) == 0 &&
	    sscanf(line+len, match_expr, &level, &size_) == 2) {
	    g_printf("%d %lld %d\n", level, size_, 1); /* write to sendsize */
	    dbprintf(_("estimate size for %s level %d: %lld KB\n"),
		     qdisk, level, size_);
	}
    }
    amfree(match_expr);

    dbprintf(_("waiting for %s %s child (pid=%d)\n"),
	     command, qdisk, (int)calcpid);
    waitpid(calcpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			    "calcsize", WTERMSIG(wait_status),
			    dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    errmsg = vstrallocf(_("%s exited with status %d: see %s"),
				"calcsize", WEXITSTATUS(wait_status),
				dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = vstrallocf(_("%s got bad exit: see %s"),
			    "calcsize", dbfn());
    }

    dbprintf(_("after %s %s wait: child pid=%d status=%d\n"),
	     command, qdisk,
	     (int)calcpid, WEXITSTATUS(wait_status));

    dbprintf(_(".....\n"));
    dbprintf(_("estimate time for %s: %s\n"),
	     qdisk,
	     walltime_str(timessub(curclock(), start_time)));

common_exit:
    if (errmsg && errmsg[0] != '\0') {
	char *qerrmsg = quote_string(errmsg);
	dbprintf(_("errmsg is %s\n"), errmsg);
	g_printf("ERROR %s\n", qerrmsg);
	amfree(qerrmsg);
    }
    amfree(qdisk);
    amfree(errmsg);
    g_ptr_array_free_full(argv_ptr);
    amfree(cmd);
}


gboolean
check_access(
    char *	filename,
    int		mode)
{
    char *noun, *adjective;
    char *quoted = quote_string(filename);

    if(mode == F_OK)
        noun = "find", adjective = "exists";
    else if((mode & X_OK) == X_OK)
	noun = "execute", adjective = "executable";
    else if((mode & (W_OK|R_OK)) == (W_OK|R_OK))
	noun = "read/write", adjective = "read/writable";
    else 
	noun = "access", adjective = "accessible";

    if(access(filename, mode) == -1) {
	g_printf(_("ERROR [can not %s %s: %s]\n"), noun, quoted, strerror(errno));
	amfree(quoted);
	return FALSE;
    } else {
	g_printf(_("OK %s %s\n"), quoted, adjective);
    }
    amfree(quoted);
    return TRUE;
}

gboolean
check_file(
    char *	filename,
    int		mode)
{
    struct stat stat_buf;
    char *quoted;

    if(!stat(filename, &stat_buf)) {
	if(!S_ISREG(stat_buf.st_mode)) {
	    quoted = quote_string(filename);
	    g_printf(_("ERROR [%s is not a file]\n"), quoted);
	    amfree(quoted);
	    return FALSE;
	}
    } else {
	int save_errno = errno;
	quoted = quote_string(filename);
	g_printf(_("ERROR [can not stat %s: %s]\n"), quoted,
		 strerror(save_errno));
	amfree(quoted);
	return FALSE;
    }
    if (getuid() == geteuid()) {
	return check_access(filename, mode);
    } else {
	quoted = quote_string(filename);
	g_printf("OK %s\n", quoted);
	amfree(quoted);
    }
    return TRUE;
}

gboolean
check_dir(
    char *	dirname,
    int		mode)
{
    struct stat stat_buf;
    char *quoted;
    char *dir;

    if(!stat(dirname, &stat_buf)) {
	if(!S_ISDIR(stat_buf.st_mode)) {
	    quoted = quote_string(dirname);
	    g_printf(_("ERROR [%s is not a directory]\n"), quoted);
	    amfree(quoted);
	    return FALSE;
	}
    } else {
	int save_errno = errno;
	quoted = quote_string(dirname);
	g_printf(_("ERROR [can not stat %s: %s]\n"), quoted,
		 strerror(save_errno));
	amfree(quoted);
	return FALSE;
    }
    if (getuid() == geteuid()) {
	gboolean  result;
	dir = stralloc2(dirname, "/.");
	result = check_access(dir, mode);
	amfree(dir);
	return result;
    } else {
	quoted = quote_string(dirname);
	g_printf("OK %s\n", quoted);
	amfree(quoted);
    }
    return TRUE;
}

gboolean
check_suid(
    char *	filename)
{
#ifndef SINGLE_USERID
    struct stat stat_buf;
    char *quoted = quote_string(filename);

    if(!stat(filename, &stat_buf)) {
	if(stat_buf.st_uid != 0 ) {
	    g_printf(_("ERROR [%s is not owned by root]\n"), quoted);
	    amfree(quoted);
	    return FALSE;
	}
	if((stat_buf.st_mode & S_ISUID) != S_ISUID) {
	    g_printf(_("ERROR [%s is not SUID root]\n"), quoted);
	    amfree(quoted);
	    return FALSE;
	}
    }
    else {
	g_printf(_("ERROR [can not stat %s: %s]\n"), quoted, strerror(errno));
	amfree(quoted);
	return FALSE;
    }
    amfree(quoted);
#else
    (void)filename;	/* Quiet unused parameter warning */
#endif
    return TRUE;
}

/*
 * Returns the value of the first integer in a string.
 */

double
the_num(
    char *      str,
    int         pos)
{
    char *num;
    int ch;
    double d;

    do {
	ch = *str++;
	while(ch && !isdigit(ch)) ch = *str++;
	if (pos == 1) break;
	pos--;
	while(ch && (isdigit(ch) || ch == '.')) ch = *str++;
    } while (ch);
    num = str - 1;
    while(isdigit(ch) || ch == '.') ch = *str++;
    str[-1] = '\0';
    d = atof(num);
    str[-1] = (char)ch;
    return d;
}


char *
config_errors_to_error_string(
    GSList *errlist)
{
    char *errmsg;
    gboolean multiple_errors = FALSE;

    if (errlist) {
	errmsg = (char *)errlist->data;
	if (errlist->next)
	    multiple_errors = TRUE;
    } else {
	errmsg = _("(no error message)");
    }

    return vstrallocf("ERROR %s%s", errmsg,
	multiple_errors? _(" (additional errors not displayed)"):"");
}


void
add_type_table(
    dmpline_t   typ,
    amregex_t **re_table,
    amregex_t  *orig_re_table,
    GSList     *normal_message,
    GSList     *ignore_message,
    GSList     *strange_message)
{
    amregex_t *rp;

    for(rp = orig_re_table; rp->regex != NULL; rp++) {
	if (rp->typ == typ) {
	    int     found = 0;
	    GSList *mes;

	    for (mes = normal_message; mes != NULL; mes = mes->next) {
		if (strcmp(rp->regex, (char *)mes->data) == 0)
		    found = 1;
	    }
	    for (mes = ignore_message; mes != NULL; mes = mes->next) {
		if (strcmp(rp->regex, (char *)mes->data) == 0)
		    found = 1;
	    }
	    for (mes = strange_message; mes != NULL; mes = mes->next) {
		if (strcmp(rp->regex, (char *)mes->data) == 0)
		    found = 1;
	    }
	    if (found == 0) {
		(*re_table)->regex   = rp->regex;
		(*re_table)->srcline = rp->srcline;
		(*re_table)->scale   = rp->scale;
		(*re_table)->field   = rp->field;
		(*re_table)->typ     = rp->typ;
		(*re_table)++;
	    }
	}
    }
}

void
add_list_table(
    dmpline_t   typ,
    amregex_t **re_table,
    GSList     *message)
{
    GSList *mes;

    for (mes = message; mes != NULL; mes = mes->next) {
	(*re_table)->regex = (char *)mes->data;
	(*re_table)->srcline = 0;
	(*re_table)->scale   = 0;
	(*re_table)->field   = 0;
	(*re_table)->typ     = typ;
	(*re_table)++;
    }
}

amregex_t *
build_re_table(
    amregex_t *orig_re_table,
    GSList    *normal_message,
    GSList    *ignore_message,
    GSList    *strange_message)
{
    int        nb = 0;
    amregex_t *rp;
    amregex_t *re_table, *new_re_table;

    for(rp = orig_re_table; rp->regex != NULL; rp++) {
	nb++;
    }
    nb += g_slist_length(normal_message);
    nb += g_slist_length(ignore_message);
    nb += g_slist_length(strange_message);
    nb ++;

    re_table =  new_re_table = malloc(nb * sizeof(amregex_t));
    
    /* add SIZE from orig_re_table */
    add_type_table(DMP_SIZE, &re_table, orig_re_table,
		   normal_message, ignore_message, strange_message);

    /* add ignore_message */
    add_list_table(DMP_IGNORE, &re_table, ignore_message);

    /* add IGNORE from orig_re_table */
    add_type_table(DMP_IGNORE, &re_table, orig_re_table,
		   normal_message, ignore_message, strange_message);

    /* add normal_message */
    add_list_table(DMP_NORMAL, &re_table, normal_message);

    /* add NORMAL from orig_re_table */
    add_type_table(DMP_NORMAL, &re_table, orig_re_table,
		   normal_message, ignore_message, strange_message);

    /* add strange_message */
    add_list_table(DMP_STRANGE, &re_table, strange_message);

    /* add STRANGE from orig_re_table */
    add_type_table(DMP_STRANGE, &re_table, orig_re_table,
		   normal_message, ignore_message, strange_message);

    /* Add DMP_STRANGE with NULL regex,       */
    /* it is not copied by previous statement */
    re_table->regex = NULL;
    re_table->srcline = 0;
    re_table->scale = 0;
    re_table->field = 0;
    re_table->typ = DMP_STRANGE;

    return new_re_table;
}

