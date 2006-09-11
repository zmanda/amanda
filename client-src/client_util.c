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
#include "client_util.h"
#include "getfsent.h"
#include "util.h"

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

    ts = construct_timestamp(&t);
    if(n == 0)
	number[0] = '\0';
    else
	snprintf(number, SIZEOF(number), "%03d", n - 1);
	
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
	error("open debug directory \"%s\": %s",
		AMANDA_TMPDIR, strerror(errno));
	/*NOTREACHED*/
    }
    test_name = get_name(diskname, exin,
			 curtime - (AMANDA_DEBUG_DAYS * 24 * 60 * 60), 0);
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
	dbprintf(("%s: Cannot create %s (%s)\n",
			debug_prefix(NULL), quoted, strerror(errno)));
	if(verbose) {
	    printf("ERROR [cannot create %s (%s)]\n",
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
    fprintf(file_exclude, "%s\n", file);
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

    (void)disk;	/* Quiet unused parameter warning */

    l = strlen(ainc);
    if(ainc[l-1] == '\n') {
	ainc[l-1] = '\0';
	l--;
    }
    if (strncmp(ainc, "./", 2) != 0) {
        quoted = quote_string(ainc);
        dbprintf(("%s: include must start with './' (%s)\n",
		  debug_prefix(NULL), quoted));
	if(verbose) {
	    printf("ERROR [include must start with './' (%s)]\n", quoted);
	}
	amfree(quoted);
    }
    else {
	char *incname = ainc+2;

	if(strchr(incname, '/')) {
            file = quoted = quote_string(ainc);
	    if (*file == '"') {
		file[strlen(file) - 1] = '\0';
		file++;
	    }
	    fprintf(file_include, "%s\n", file);
	    amfree(quoted);
	    nb_exp++;
	}
	else {
	    char *regex;
	    DIR *d;
	    struct dirent *entry;

	    regex = glob_to_regex(incname);
	    if((d = opendir(device)) == NULL) {
		quoted = quote_string(device);
		dbprintf(("%s: Can't open disk %s\n",
		      debug_prefix(NULL), quoted));
		if(verbose) {
		    printf("ERROR [Can't open disk %s]\n", quoted);
		}
		amfree(quoted);
	    }
	    else {
		while((entry = readdir(d)) != NULL) {
		    if(is_dot_or_dotdot(entry->d_name)) {
			continue;
		    }
		    if(match(regex, entry->d_name)) {
			incname = vstralloc("./", entry->d_name, NULL);
			file = quoted = quote_string(incname);
			if (*file == '"') {
			    file[strlen(file) - 1] = '\0';
			    file++;
			}
			fprintf(file_include, "%s\n", file);
			amfree(quoted);
			amfree(incname);
			nb_exp++;
		    }
		}
		closedir(d);
	    }
	    amfree(regex);
	}
    }
    return nb_exp;
}

char *
build_exclude(
    char *	disk,
    char *	device,
    option_t *	options,
    int		verbose)
{
    char *filename;
    FILE *file_exclude;
    FILE *exclude;
    char *aexc;
    sle_t *excl;
    int nb_exclude = 0;
    char *quoted;

    if(options->exclude_file) nb_exclude += options->exclude_file->nb_element;
    if(options->exclude_list) nb_exclude += options->exclude_list->nb_element;

    if(nb_exclude == 0) return NULL;

    if((filename = build_name(disk, "exclude", verbose)) != NULL) {
	if((file_exclude = fopen(filename,"w")) != NULL) {

	    if(options->exclude_file) {
		for(excl = options->exclude_file->first; excl != NULL;
		    excl = excl->next) {
		    add_exclude(file_exclude, excl->name,
				verbose && options->exclude_optional == 0);
		}
	    }

	    if(options->exclude_list) {
		for(excl = options->exclude_list->first; excl != NULL;
		    excl = excl->next) {
		    char *exclname = fixup_relative(excl->name, device);
		    if((exclude = fopen(exclname, "r")) != NULL) {
			while ((aexc = agets(exclude)) != NULL) {
			    if (aexc[0] == '\0') {
				amfree(aexc);
				continue;
			    }
			    add_exclude(file_exclude, aexc,
				        verbose && options->exclude_optional == 0);
			    amfree(aexc);
			}
			fclose(exclude);
		    }
		    else {
			quoted = quote_string(exclname);
			dbprintf(("%s: Can't open exclude file %s (%s)\n",
				  debug_prefix(NULL),
				  quoted, strerror(errno)));
			if(verbose && (options->exclude_optional == 0 ||
				       errno != ENOENT)) {
			    printf("ERROR [Can't open exclude file %s (%s)]\n",
				   quoted, strerror(errno));
			}
			amfree(quoted);
		    }
		    amfree(exclname);
		}
	    }
            fclose(file_exclude);
	}
	else {
	    quoted = quote_string(filename);
	    dbprintf(("%s: Can't create exclude file %s (%s)\n",
		      debug_prefix(NULL),
		      quoted, strerror(errno)));
	    if(verbose) {
		printf("ERROR [Can't create exclude file %s (%s)]\n",
			quoted, strerror(errno));
	    }
	    amfree(quoted);
	}
    }

    return filename;
}

char *
build_include(
    char *	disk,
    char *	device,
    option_t *	options,
    int		verbose)
{
    char *filename;
    FILE *file_include;
    FILE *include;
    char *ainc = NULL;
    sle_t *incl;
    int nb_include = 0;
    int nb_exp = 0;
    char *quoted;

    if(options->include_file) nb_include += options->include_file->nb_element;
    if(options->include_list) nb_include += options->include_list->nb_element;

    if(nb_include == 0) return NULL;

    if((filename = build_name(disk, "include", verbose)) != NULL) {
	if((file_include = fopen(filename,"w")) != NULL) {

	    if(options->include_file) {
		for(incl = options->include_file->first; incl != NULL;
		    incl = incl->next) {
		    nb_exp += add_include(disk, device, file_include,
				  incl->name,
				  verbose && options->include_optional == 0);
		}
	    }

	    if(options->include_list) {
		for(incl = options->include_list->first; incl != NULL;
		    incl = incl->next) {
		    char *inclname = fixup_relative(incl->name, device);
		    if((include = fopen(inclname, "r")) != NULL) {
			while ((ainc = agets(include)) != NULL) {
			    if (ainc[0] == '\0') {
				amfree(ainc);
				continue;
			    }
			    nb_exp += add_include(disk, device,
						  file_include, ainc,
						  verbose && options->include_optional == 0);
			    amfree(ainc);
			}
			fclose(include);
		    }
		    else {
			quoted = quote_string(inclname);
			dbprintf(("%s: Can't open include file %s (%s)\n",
				  debug_prefix(NULL), quoted, strerror(errno)));
			if(verbose && (options->include_optional == 0 ||
				       errno != ENOENT)) {
			    printf("ERROR [Can't open include file %s (%s)]\n",
				   quoted, strerror(errno));
			}
			amfree(quoted);
		   }
		   amfree(inclname);
		}
	    }
            fclose(file_include);
	}
	else {
	    quoted = quote_string(filename);
	    dbprintf(("%s: Can't create include file %s (%s)\n",
		      debug_prefix(NULL), quoted, strerror(errno)));
	    if(verbose) {
		printf("ERROR [Can't create include file %s (%s)]\n",
			quoted, strerror(errno));
	    }
	    amfree(quoted);
	}
    }
	
    if(nb_exp == 0) {
	quoted = quote_string(disk);
	dbprintf(("%s: No include for %s\n", debug_prefix(NULL), quoted));
	if(verbose && options->include_optional == 0) {
	    printf("ERROR [No include for %s]\n", quoted);
	}
	amfree(quoted);
    }

    return filename;
}


void
init_options(
    option_t *options)
{
    options->str = NULL;
    options->compress = NO_COMPR;
    options->srvcompprog = NULL;
    options->clntcompprog = NULL;
    options->encrypt = ENCRYPT_NONE;
    options->srv_encrypt = NULL;
    options->clnt_encrypt = NULL;
    options->srv_decrypt_opt = NULL;
    options->clnt_decrypt_opt = NULL;
    options->no_record = 0;
    options->createindex = 0;
    options->auth = NULL;
    options->exclude_file = NULL;
    options->exclude_list = NULL;
    options->include_file = NULL;
    options->include_list = NULL;
    options->exclude_optional = 0;
    options->include_optional = 0;
}


option_t *
parse_options(
    char *str,
    char *disk,
    char *device,
    am_feature_t *fs,
    int verbose)
{
    char *exc;
    char *inc;
    option_t *options;
    char *p, *tok;
    char *quoted;

    (void)disk;		/* Quiet unused parameter warning */
    (void)device;	/* Quiet unused parameter warning */

    options = alloc(SIZEOF(option_t));
    init_options(options);
    options->str = stralloc(str);

    p = stralloc(str);
    tok = strtok(p,";");

    while (tok != NULL) {
	if(am_has_feature(fs, fe_options_auth)
	   && BSTRNCMP(tok,"auth=") == 0) {
	    if(options->auth != NULL) {
		quoted = quote_string(tok + 5);
		dbprintf(("%s: multiple auth option %s\n",
			  debug_prefix(NULL), quoted));
		if(verbose) {
		    printf("ERROR [multiple auth option %s]\n", quoted);
		}
		amfree(quoted);
	    }
	    options->auth = stralloc(&tok[5]);
	}
	else if(am_has_feature(fs, fe_options_bsd_auth)
	   && BSTRNCMP(tok, "bsd-auth") == 0) {
	    if(options->auth != NULL) {
		dbprintf(("%s: multiple auth option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple auth option]\n");
		}
	    }
	    options->auth = stralloc("bsd");
	}
	else if(am_has_feature(fs, fe_options_krb4_auth)
	   && BSTRNCMP(tok, "krb4-auth") == 0) {
	    if(options->auth != NULL) {
		dbprintf(("%s: multiple auth option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple auth option]\n");
		}
	    }
	    options->auth = stralloc("krb4");
	}
	else if(BSTRNCMP(tok, "compress-fast") == 0) {
	    if(options->compress != NO_COMPR) {
		dbprintf(("%s: multiple compress option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple compress option]\n");
		}
	    }
	    options->compress = COMPR_FAST;
	}
	else if(BSTRNCMP(tok, "compress-best") == 0) {
	    if(options->compress != NO_COMPR) {
		dbprintf(("%s: multiple compress option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple compress option]\n");
		}
	    }
	    options->compress = COMPR_BEST;
	}
	else if(BSTRNCMP(tok, "srvcomp-fast") == 0) {
	    if(options->compress != NO_COMPR) {
		dbprintf(("%s: multiple compress option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple compress option]\n");
		}
	    }
	    options->compress = COMPR_SERVER_FAST;
	}
	else if(BSTRNCMP(tok, "srvcomp-best") == 0) {
	    if(options->compress != NO_COMPR) {
		dbprintf(("%s: multiple compress option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple compress option]\n");
		}
	    }
	    options->compress = COMPR_SERVER_BEST;
	}
	else if(BSTRNCMP(tok, "srvcomp-cust=") == 0) {
	    if(options->compress != NO_COMPR) {
		dbprintf(("%s: multiple compress option\n", 
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple compress option]\n");
		}
	    }
	    options->srvcompprog = stralloc(tok + SIZEOF("srvcomp-cust=") -1);
	    options->compress = COMPR_SERVER_CUST;
	}
	else if(BSTRNCMP(tok, "comp-cust=") == 0) {
	    if(options->compress != NO_COMPR) {
		dbprintf(("%s: multiple compress option\n", 
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple compress option]\n");
		}
	    }
	    options->clntcompprog = stralloc(tok + SIZEOF("comp-cust=") -1);
	    options->compress = COMPR_CUST;
	    /* parse encryption options */
	} 
	else if(BSTRNCMP(tok, "encrypt-serv-cust=") == 0) {
	    if(options->encrypt != ENCRYPT_NONE) {
		dbprintf(("%s: multiple encrypt option\n", 
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple encrypt option]\n");
		}
	    }
	    options->srv_encrypt = stralloc(tok + SIZEOF("encrypt-serv-cust=") -1);
	    options->encrypt = ENCRYPT_SERV_CUST;
	} 
	else if(BSTRNCMP(tok, "encrypt-cust=") == 0) {
	    if(options->encrypt != ENCRYPT_NONE) {
		dbprintf(("%s: multiple encrypt option\n", 
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple encrypt option]\n");
		}
	    }
	    options->clnt_encrypt= stralloc(tok + SIZEOF("encrypt-cust=") -1);
	    options->encrypt = ENCRYPT_CUST;
	} 
	else if(BSTRNCMP(tok, "server-decrypt-option=") == 0) {
	  options->srv_decrypt_opt = stralloc(tok + SIZEOF("server-decrypt-option=") -1);
	}
	else if(BSTRNCMP(tok, "client-decrypt-option=") == 0) {
	  options->clnt_decrypt_opt = stralloc(tok + SIZEOF("client-decrypt-option=") -1);
	}
	else if(BSTRNCMP(tok, "no-record") == 0) {
	    if(options->no_record != 0) {
		dbprintf(("%s: multiple no-record option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple no-record option]\n");
		}
	    }
	    options->no_record = 1;
	}
	else if(BSTRNCMP(tok, "index") == 0) {
	    if(options->createindex != 0) {
		dbprintf(("%s: multiple index option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple index option]\n");
		}
	    }
	    options->createindex = 1;
	}
	else if(BSTRNCMP(tok, "exclude-optional") == 0) {
	    if(options->exclude_optional != 0) {
		dbprintf(("%s: multiple exclude-optional option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple exclude-optional option]\n");
		}
	    }
	    options->exclude_optional = 1;
	}
	else if(strcmp(tok, "include-optional") == 0) {
	    if(options->include_optional != 0) {
		dbprintf(("%s: multiple include-optional option\n",
			  debug_prefix(NULL)));
		if(verbose) {
		    printf("ERROR [multiple include-optional option]\n");
		}
	    }
	    options->include_optional = 1;
	}
	else if(BSTRNCMP(tok,"exclude-file=") == 0) {
	    exc = unquote_string(&tok[13]);
	    options->exclude_file = append_sl(options->exclude_file, exc);
	    amfree(exc);
	}
	else if(BSTRNCMP(tok,"exclude-list=") == 0) {
	    exc = unquote_string(&tok[13]);
	    options->exclude_list = append_sl(options->exclude_list, exc);
	    amfree(exc);
	}
	else if(BSTRNCMP(tok,"include-file=") == 0) {
	    inc = unquote_string(&tok[13]);
	    options->include_file = append_sl(options->include_file, inc);
	    amfree(inc);
	}
	else if(BSTRNCMP(tok,"include-list=") == 0) {
	    inc = unquote_string(&tok[13]);
	    options->include_list = append_sl(options->include_list, inc);
	    amfree(inc);
	}
	else if(strcmp(tok,"|") != 0) {
	    quoted = quote_string(tok);
	    dbprintf(("%s: unknown option %s\n",
			debug_prefix(NULL), quoted));
	    if(verbose) {
		printf("ERROR [unknown option: %s]\n", quoted);
	    }
	    amfree(quoted);
	}
	tok = strtok(NULL, ";");
    }
    amfree(p);
    return options;
}
