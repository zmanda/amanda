/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: find.c,v 1.33 2006/07/06 13:13:15 martinea Exp $
 *
 * controlling process for the Amanda backup system
 */
#include "amanda.h"
#include "match.h"
#include "conffile.h"
#include "tapefile.h"
#include "logfile.h"
#include "holding.h"
#include "find.h"
#include <regex.h>
#include "cmdline.h"

int find_match(char *host, char *disk);
static char *find_nicedate(char *datestamp);
static int len_find_nicedate(char *datestamp);
static int find_compare(const void *, const void *);
static int parse_taper_datestamp_log(char *logline, char **datestamp, char **level,
				     char **storage);
static gboolean logfile_has_tape(char * label, char * datestamp,
                                 char * logfile);

static char *find_sort_order = NULL;
static GStringChunk *string_chunk = NULL;

find_result_t * find_dump(disklist_t* diskqp) {
    char *conf_logdir, *logfile = NULL;
    int tape, maxtape, logs;
    unsigned seq;
    tape_t *tp;
    find_result_t *output_find = NULL;
    GHashTable *tape_seen = g_hash_table_new(g_str_hash, g_str_equal);

    if (string_chunk == NULL) {
	string_chunk = g_string_chunk_new(32768);
    }
    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    maxtape = lookup_nb_tape();

    for(tape = 1; tape <= maxtape; tape++) {

	tp = lookup_tapepos(tape);
	if(tp == NULL) continue;

	/* Do not search the log file if we already searched that datestamp */
	if (g_hash_table_lookup(tape_seen, tp->datestamp)) {
	    continue;
	}
	g_hash_table_insert(tape_seen, tp->datestamp, GINT_TO_POINTER(1));

	/* search log files */

	logs = 0;

	/* new-style log.<date>.<seq> */

	for(seq = 0; 1; seq++) {
	    char seq_str[NUM_STR_SIZE];

	    g_snprintf(seq_str, sizeof(seq_str), "%u", seq);
	    g_free(logfile);
	    logfile = g_strconcat(conf_logdir, "/log.", tp->datestamp, ".",
	        seq_str, NULL);
	    if(access(logfile, R_OK) != 0) break;
	    if (search_logfile(&output_find, NULL, tp->datestamp,
                               logfile, diskqp)) {
                logs ++;
            }
	}

	/* search old-style amflush log, if any */

	g_free(logfile);
	logfile = g_strconcat(conf_logdir, "/log.", tp->datestamp, ".amflush",
	    NULL);
	if(access(logfile,R_OK) == 0) {
	    if (search_logfile(&output_find, NULL, tp->datestamp,
                               logfile, diskqp)) {
                logs ++;
            }
        }
        
	/* search old-style main log, if any */

	g_free(logfile);
	logfile = g_strconcat(conf_logdir, "/log.", tp->datestamp, NULL);
	if(access(logfile,R_OK) == 0) {
	    if (search_logfile(&output_find, NULL, tp->datestamp,
                               logfile, diskqp)) {
                logs ++;
            }
	}
    }
    g_hash_table_destroy(tape_seen);
    amfree(logfile);
    amfree(conf_logdir);

    search_holding_disk(&output_find, diskqp);

    return(output_find);
}

char **
find_log(void)
{
    char *conf_logdir, *logfile = NULL;
    char *pathlogfile = NULL;
    int tape, maxtape, logs;
    unsigned seq;
    tape_t *tp;
    char **output_find_log = NULL;
    char **current_log;

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    maxtape = lookup_nb_tape();

    output_find_log = g_malloc((maxtape*5+10) * sizeof(char *));
    current_log = output_find_log;

    for(tape = 1; tape <= maxtape; tape++) {

	tp = lookup_tapepos(tape);
	if(tp == NULL) continue;

	/* search log files */

	logs = 0;

	/* new-style log.<date>.<seq> */

	for(seq = 0; 1; seq++) {
	    char seq_str[NUM_STR_SIZE];

	    g_snprintf(seq_str, sizeof(seq_str), "%u", seq);
	    g_free(logfile);
	    logfile = g_strconcat("log.", tp->datestamp, ".", seq_str, NULL);

	    g_free(pathlogfile);
	    pathlogfile = g_strconcat(conf_logdir, "/", logfile, NULL);
	    if (access(pathlogfile, R_OK) != 0) break;
	    if (logfile_has_tape(tp->label, tp->datestamp, pathlogfile)) {
		if (current_log == output_find_log || !g_str_equal(*(current_log - 1),
                                                                   logfile)) {
		    *current_log = g_strdup(logfile);
		    current_log++;
		}
		logs++;
		break;
	    }
	}

	/* search old-style amflush log, if any */

	g_free(logfile);
	logfile = g_strconcat("log.", tp->datestamp, ".amflush", NULL);

	g_free(pathlogfile);
	pathlogfile = g_strconcat(conf_logdir, "/", logfile, NULL);
	if (access(pathlogfile, R_OK) == 0) {
	    if (logfile_has_tape(tp->label, tp->datestamp, pathlogfile)) {
		if (current_log == output_find_log || !g_str_equal(*(current_log - 1),
                                                                   logfile)) {
		    *current_log = g_strdup(logfile);
		    current_log++;
		}
		logs++;
	    }
	}

	/* search old-style main log, if any */

	g_free(logfile);
	logfile = g_strconcat("log.", tp->datestamp, NULL);
	g_free(pathlogfile);
	pathlogfile = g_strconcat(conf_logdir, "/", logfile, NULL);
	if (access(pathlogfile, R_OK) == 0) {
	    if (logfile_has_tape(tp->label, tp->datestamp, pathlogfile)) {
		if (current_log == output_find_log || !g_str_equal(*(current_log - 1),
                                                                   logfile)) {
		    *current_log = g_strdup(logfile);
		    current_log++;
		}
		logs++;
	    }
	}

	if(logs == 0 && !g_str_equal(tp->datestamp, "0"))
	    g_fprintf(stderr, _("Warning: no log files found for tape %s written %s\n"),
		   tp->label, find_nicedate(tp->datestamp));
    }
    amfree(logfile);
    amfree(pathlogfile);
    amfree(conf_logdir);
    *current_log = NULL;
    return(output_find_log);
}

void
search_holding_disk(
    find_result_t **output_find,
    disklist_t * dynamic_disklist)
{
    GSList *holding_file_list;
    GSList *e;
    char   *holding_file;
    disk_t *dp;
    char   *orig_name;

    holding_file_list = holding_get_files(NULL, 1);

    if (string_chunk == NULL) {
	string_chunk = g_string_chunk_new(32768);
    }

    for(e = holding_file_list; e != NULL; e = e->next) {
	dumpfile_t file;

	holding_file = (char *)e->data;

	if (!holding_file_get_dumpfile(holding_file, &file))
	    continue;

	if (file.dumplevel < 0 || file.dumplevel >= DUMP_LEVELS) {
	    dumpfile_free_data(&file);
	    continue;
	}

	dp = NULL;
	orig_name = g_strdup(file.name);
	for (;;) {
	    char *s;
	    if ((dp = lookup_disk(orig_name, file.disk)))
		break;
	    if ((s = strrchr(orig_name,'.')) == NULL)
		break;
	    *s = '\0';
	}
	g_free(orig_name);

	if ( dp == NULL ) {
	    if (dynamic_disklist == NULL) {
		dumpfile_free_data(&file);
		continue;
	    }
	    add_disk(dynamic_disklist, file.name, file.disk);
	}

	if(find_match(file.name,file.disk)) {
	    find_result_t *new_output_find = g_new0(find_result_t, 1);
	    new_output_find->next=*output_find;
	    new_output_find->timestamp = g_string_chunk_insert_const(string_chunk, file.datestamp);
	    new_output_find->write_timestamp = g_string_chunk_insert_const(string_chunk, "00000000000000");
	    new_output_find->hostname = g_string_chunk_insert_const(string_chunk, file.name);
	    new_output_find->diskname = g_string_chunk_insert_const(string_chunk, file.disk);
	    new_output_find->storage = g_string_chunk_insert_const(string_chunk, "HOLDING");
	    new_output_find->level=file.dumplevel;
	    new_output_find->label=g_string_chunk_insert_const(string_chunk, holding_file);
	    new_output_find->partnum = -1;
	    new_output_find->totalparts = -1;
	    new_output_find->filenum=0;
	    if (file.is_partial) {
		new_output_find->status="PARTIAL";
		new_output_find->dump_status="PARTIAL";
	    } else {
		new_output_find->status="OK";
		new_output_find->dump_status="OK";
	    }
	    new_output_find->message="";
	    new_output_find->kb = holding_file_size(holding_file, 1);
	    new_output_find->bytes = 0;

	    new_output_find->orig_kb = file.orig_size;

	    *output_find=new_output_find;
	}
	dumpfile_free_data(&file);
    }

    slist_free_full(holding_file_list, g_free);
}

static int
find_compare(
    const void *i1,
    const void *j1)
{
    int compare=0;
    find_result_t *i, *j;

    size_t nb_compare=strlen(find_sort_order);
    size_t k;

    for(k=0;k<nb_compare;k++) {
        char sort_key = find_sort_order[k];
        if (isupper((int)sort_key)) {
            /* swap */
            sort_key = tolower(sort_key);
            j = *(find_result_t **)i1;
            i = *(find_result_t **)j1;
        } else {
            i = *(find_result_t **)i1;
            j = *(find_result_t **)j1;
        }

	switch (sort_key) {
	case 'h' : compare=strcmp(i->hostname,j->hostname);
		   break;
	case 'k' : compare=strcmp(i->diskname,j->diskname);
		   break;
	case 'd' : compare=strcmp(i->timestamp,j->timestamp);
		   break;
	case 'l' : compare=j->level - i->level;
		   break;
	case 'f' : compare=(i->filenum == j->filenum) ? 0 :
		           ((i->filenum < j->filenum) ? -1 : 1);
		   break;
	case 'b' : compare=compare_possibly_null_strings(i->label,
                                                         j->label);
                   break;
	case 'w': compare=strcmp(i->write_timestamp, j->write_timestamp);
		   break;
	case 'p' :
		   compare=i->partnum - j->partnum;
		   break;
	case 's' : compare=i->storage_id - j->storage_id;
		   break;
	}
	if(compare != 0)
	    return compare;
    }
    return 0;
}

void
sort_find_result(
    char *sort_order,
    find_result_t **output_find)
{
    sort_find_result_with_storage(sort_order, NULL, output_find);
}

void
sort_find_result_with_storage(
    char *sort_order,
    char **storage_list,
    find_result_t **output_find)
{
    find_result_t *output_find_result;
    find_result_t **array_find_result = NULL;
    size_t nb_result=0;
    size_t no_result;
    int    i;
    identlist_t il;

    find_sort_order = sort_order;
    /* qsort core dump if nothing to sort */
    if(*output_find==NULL)
	return;

    /* How many result */
    for(output_find_result=*output_find;
	output_find_result;
	output_find_result=output_find_result->next) {
	nb_result++;
	if (!storage_list) {
	    for (i = 1, il = getconf_identlist(CNF_STORAGE); il != NULL;
		i++, il = il->next) {
		char *storage_n = il->data;
		if (g_str_equal(output_find_result->storage,
				storage_n)) {
		    output_find_result->storage_id = i;
		}
	    }
	} else {
	    char **storage_l;
	    for (i=1, storage_l = storage_list; *storage_l != NULL;
		 storage_l++, i++) {
		if (g_str_equal(output_find_result->storage, *storage_l)) {
		    output_find_result->storage_id = i;
		}
	    }
	}
    }

    /* put the list in an array */
    array_find_result=g_malloc(nb_result * sizeof(find_result_t *));
    for(output_find_result=*output_find,no_result=0;
	output_find_result;
	output_find_result=output_find_result->next,no_result++) {
	array_find_result[no_result]=output_find_result;
    }

    /* sort the array */
    qsort(array_find_result,nb_result,sizeof(find_result_t *),
	  find_compare);

    /* put the sorted result in the list */
    for(no_result=0;
	no_result<nb_result-1; no_result++) {
	array_find_result[no_result]->next = array_find_result[no_result+1];
    }
    array_find_result[nb_result-1]->next=NULL;
    *output_find=array_find_result[0];
    amfree(array_find_result);
}

void
print_find_result(
    find_result_t *output_find)
{
    find_result_t *output_find_result;
    int max_len_datestamp = 4;
    int max_len_hostname  = 4;
    int max_len_diskname  = 4;
    int max_len_level     = 2;
    int max_len_storage   = 7;
    int max_len_label     =12;
    int max_len_filenum   = 4;
    int max_len_part      = 4;
    int max_len_status    = 6;
    size_t len;

    for(output_find_result=output_find;
	output_find_result;
	output_find_result=output_find_result->next) {
	char *s;

	len=len_find_nicedate(output_find_result->timestamp);
	if((int)len > max_len_datestamp)
	    max_len_datestamp=(int)len;

	len=strlen(output_find_result->hostname);
	if((int)len > max_len_hostname)
	    max_len_hostname = (int)len;

	len = len_quote_string(output_find_result->diskname);
	if((int)len > max_len_diskname)
	    max_len_diskname = (int)len;

        if (output_find_result->label != NULL) {
	    len = len_quote_string(output_find_result->label);
            if((int)len > max_len_label)
                max_len_label = (int)len;
        }

        if (output_find_result->storage != NULL) {
	    len = len_quote_string(output_find_result->storage);
            if((int)len > max_len_storage)
                max_len_storage = (int)len;
        }

	len=strlen(output_find_result->status) + 1 + strlen(output_find_result->dump_status);
	if((int)len > max_len_status)
	    max_len_status = (int)len;

	s = g_strdup_printf("%d/%d", output_find_result->partnum,
				     output_find_result->totalparts);
	len=strlen(s);
	if((int)len > max_len_part)
	    max_len_part = (int)len;
	amfree(s);
    }

    /*
     * Since status is the rightmost field, we zap the maximum length
     * because it is not needed.  The code is left in place in case
     * another column is added later.
     */
    max_len_status = 1;

    if(output_find==NULL) {
	g_printf(_("\nNo dump to list\n"));
    }
    else {
	g_printf(_("\ndate%*s host%*s disk%*s lv%*s storage%*s tape or file%*s file%*s part%*s status\n"),
	       max_len_datestamp-4,"",
	       max_len_hostname-4 ,"",
	       max_len_diskname-4 ,"",
	       max_len_level-2    ,"",
	       max_len_storage-7  ,"",
	       max_len_label-12   ,"",
	       max_len_filenum-4  ,"",
	       max_len_part-4  ,"");
        for(output_find_result=output_find;
	        output_find_result;
	        output_find_result=output_find_result->next) {
	    char *qdiskname;
            char * formatted_label;
	    char *s;
	    char *status;

	    qdiskname = quote_string(output_find_result->diskname);
            if (output_find_result->label == NULL)
                formatted_label = g_strdup("");
	    else
		formatted_label = quote_string(output_find_result->label);

	    if (!g_str_equal(output_find_result->status, "OK") ||
		!g_str_equal(output_find_result->dump_status, "OK")) {
		status = g_strjoin(NULL, output_find_result->status, " ",
				   output_find_result->dump_status, NULL);
	    } else {
		status = g_strdup(output_find_result->status);
	    }

	    /*@ignore@*/
	    /* sec and kb are omitted here, for compatibility with the existing
	     * output from 'amadmin' */
	    s = g_strdup_printf("%d/%d", output_find_result->partnum,
					 output_find_result->totalparts);
	    g_printf("%-*s %-*s %-*s %*d %-*s %-*s %*lld %*s %s %s\n",
                     max_len_datestamp,
                     find_nicedate(output_find_result->timestamp),
                     max_len_hostname,  output_find_result->hostname,
                     max_len_diskname,  qdiskname,
                     max_len_level,     output_find_result->level,
                     max_len_storage,   output_find_result->storage,
                     max_len_label,     formatted_label,
                     max_len_filenum,   (long long)output_find_result->filenum,
                     max_len_part,      s,
                                        status,
					output_find_result->message
		    );
	    amfree(status);
	    amfree(s);
	    /*@end@*/
	    amfree(qdiskname);
	    amfree(formatted_label);
	}
    }
}

void
free_find_result(
    find_result_t **output_find)
{
    find_result_t *output_find_result, *prev;

    prev=NULL;
    for(output_find_result=*output_find;
	    output_find_result;
	    output_find_result=output_find_result->next) {
	amfree(prev);
	prev = output_find_result;
    }
    amfree(prev);
    *output_find = NULL;
}

int
find_match(
    char *host,
    char *disk)
{
    disk_t *dp = lookup_disk(host,disk);
    return (dp && dp->todo);
}

static char *
find_nicedate(
    char *datestamp)
{
    static char nice[20];
    int year, month, day;
    int hours, minutes, seconds;
    char date[9], atime[7];
    int  numdate, numtime;

    strncpy(date, datestamp, 8);
    date[8] = '\0';
    numdate = atoi(date);
    year  = numdate / 10000;
    month = (numdate / 100) % 100;
    day   = numdate % 100;

    if(strlen(datestamp) <= 8) {
	g_snprintf(nice, sizeof(nice), "%4d-%02d-%02d",
		year, month, day);
    }
    else {
	strncpy(atime, &(datestamp[8]), 6);
	atime[6] = '\0';
	numtime = atoi(atime);
	hours = numtime / 10000;
	minutes = (numtime / 100) % 100;
	seconds = numtime % 100;

	g_snprintf(nice, sizeof(nice), "%4d-%02d-%02d %02d:%02d:%02d",
		year, month, day, hours, minutes, seconds);
    }

    return nice;
}

static int
len_find_nicedate(
    char *datestamp)
{
    if(strlen(datestamp) <= 8) {
	return 10;
    } else {
	return 19;
    }
}

static int
parse_taper_datestamp_log(
    char *logline,
    char **datestamp,
    char **label,
    char **storage)
{
    char *s;
    int ch;
    char *qstorage, *uqstorage;

    s = logline;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') {
	return 0;
    }
    if(strncmp_const_skip(s - 1, "datestamp", s, ch) != 0) {
	return 0;
    }

    skip_whitespace(s, ch);
    if(ch == '\0') {
	return 0;
    }
    *datestamp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';

    skip_whitespace(s, ch);
    if(ch == '\0') {
	return 0;
    }
    qstorage = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';

    uqstorage = unquote_string(qstorage);
    if (strncmp(uqstorage, "ST:", 3) == 0) {
	skip_whitespace(s, ch);
	if(ch == '\0') {
	    return 0;
	}
	if(strncmp_const_skip(s - 1, "label", s, ch) != 0) {
	    g_free(uqstorage);
	    return 0;
	}
	*storage = g_strdup(uqstorage+3);
	g_free(uqstorage);
    } else if (strcmp(uqstorage, "label") == 0) {
	g_free(uqstorage);
	*storage = g_strdup(get_config_name());
    } else {
	g_free(uqstorage);
	*storage = NULL;
	return 0;
    }

    skip_whitespace(s, ch);
    if(ch == '\0') {
	return 0;
    }
    *label = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';

    *label = unquote_string(*label);

    return 1;
}

/* Returns TRUE if the given logfile mentions the given tape. */
static gboolean logfile_has_tape(char * label, char * datestamp,
                                 char * logfile) {
    FILE * logf;
    char * ck_datestamp, *ck_label = NULL, *ck_storage = NULL;
    if((logf = fopen(logfile, "r")) == NULL) {
	error(_("could not open logfile %s: %s"), logfile, strerror(errno));
	/*NOTREACHED*/
    }

    while(get_logline(logf)) {
	if(curlog == L_START && curprog == P_TAPER) {
	    if(parse_taper_datestamp_log(curstr,
					 &ck_datestamp, &ck_label, &ck_storage) == 0) {
		g_printf(_("strange log line \"start taper %s\" curstr='%s'\n"),
                         logfile, curstr);
	    } else if(g_str_equal(ck_datestamp, datestamp)
		      && g_str_equal(ck_label, label)) {
		amfree(ck_label);
		amfree(ck_storage);
                afclose(logf);
                return TRUE;
	    }
	    amfree(ck_label);
	    amfree(ck_storage);
	}
    }

    afclose(logf);
    return FALSE;
}

static gboolean
volume_matches(
    const char *label1,
    const char *label2,
    const char *datestamp)
{
    tape_t *tp;

    if (!label2)
	return TRUE;

    if (label1)
	return (g_str_equal(label1, label2));

    /* check in tapelist */
    if (!(tp = lookup_tapelabel(label2)))
	return FALSE;

    if (!g_str_equal(tp->datestamp, datestamp))
	return FALSE;

    return TRUE;
}

/* WARNING: Function accesses globals find_diskqp, curlog, curlog, curstr,
 * dynamic_disklist */
gboolean
search_logfile(
    find_result_t **output_find,
    const char *label,
    const char *passed_datestamp,
    const char *logfile,
    disklist_t * dynamic_disklist)
{
    FILE *logf;
    char *host = NULL;
    char *disk = NULL, *qdisk, *disk_undo;
    char *date = NULL, *date_undo;
    int  partnum;
    int  totalparts;
    int  maxparts = -1;
    char *number;
    int fileno;
    char *current_label;
    char *current_storage;
    char *rest, *rest_undo;
    char *ck_label=NULL;
    char *ck_storage=NULL;
    int level = 0;
    off_t filenum;
    char *ck_datestamp=NULL;
    char *datestamp;
    char *s;
    int ch;
    disk_t *dp;
    GHashTable* valid_label;
    GHashTable* part_by_dle;
    find_result_t *part_find;
    find_result_t *a_part_find;
    gboolean right_label = FALSE;
    gboolean found_something = FALSE;
    double sec;
    off_t kb;
    off_t bytes;
    off_t orig_kb;
    int   taper_part = 0;
    crc_t crc1, crc2, crc3;
    crc_t native_crc;
    crc_t client_crc;
    crc_t server_crc;

    g_return_val_if_fail(output_find != NULL, 0);
    g_return_val_if_fail(logfile != NULL, 0);

    current_label = g_strdup("");
    current_storage = g_strdup("");
    if (string_chunk == NULL) {
	string_chunk = g_string_chunk_new(32768);
    }
    valid_label = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    part_by_dle = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    datestamp = g_strdup(passed_datestamp);

    if((logf = fopen(logfile, "r")) == NULL) {
	error(_("could not open logfile %s: %s"), logfile, strerror(errno));
	/*NOTREACHED*/
    }

    filenum = (off_t)0;
    while(get_logline(logf)) {
	if (curlog == L_START && curprog == P_TAPER) {
	    amfree(ck_label);
	    amfree(ck_storage);
	    ck_datestamp = NULL;
	    if(parse_taper_datestamp_log(curstr, &ck_datestamp,
                                         &ck_label, &ck_storage) == 0) {
		g_printf(_("strange log line in %s \"start taper %s\"\n"),
                         logfile, curstr);
                continue;
	    }
            if (datestamp != NULL) {
                if (!g_str_equal(datestamp, ck_datestamp)) {
                    g_printf(_("Log file %s stamped %s, expecting %s!\n"),
                             logfile, ck_datestamp, datestamp);
		    amfree(ck_label);
                    break;
                }
            }

            right_label = volume_matches(label, ck_label, ck_datestamp);
	    if (right_label && ck_label) {
		g_hash_table_insert(valid_label, g_strdup(ck_label),
				    GINT_TO_POINTER(1));
	    }
	    if (label && datestamp && right_label) {
		found_something = TRUE;
	    }
            amfree(current_label);
            current_label = ck_label;
	    ck_label = NULL;
            amfree(current_storage);
            current_storage = ck_storage;
	    ck_storage = NULL;
            if (datestamp == NULL) {
                datestamp = g_strdup(ck_datestamp);
            }
	    filenum = (off_t)0;
	}
	if (!datestamp)
	    continue;
	if (right_label &&
	    (curlog == L_SUCCESS ||
	     curlog == L_CHUNK || curlog == L_PART || curlog == L_PARTPARTIAL) &&
	    curprog == P_TAPER) {
	    filenum++;
	} else if (right_label && curlog == L_PARTIAL && curprog == P_TAPER &&
		   taper_part == 0) {
	    filenum++;
	}
	partnum = -1;
	totalparts = -1;
	if (curlog == L_SUCCESS || curlog == L_CHUNKSUCCESS ||
	    curlog == L_DONE    || curlog == L_FAIL ||
	    curlog == L_CHUNK   || curlog == L_PART || curlog == L_PARTIAL ||
	    curlog == L_PARTPARTIAL ) {
	    s = curstr;
	    ch = *s++;

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		g_printf(_("strange log line in %s \"%s\"\n"),
		    logfile, curstr);
		continue;
	    }

	    if (curlog == L_PART || curlog == L_PARTPARTIAL ||
		curlog == L_DONE || curlog == L_FAIL || curlog == L_PARTIAL) {
		char *part_storage;
		char *qpart_storage = s - 1;
		char *part_label;
		char *qnext_string;
		char *next_string;
		taper_part++;
		skip_quoted_string(s, ch);
		s[-1] = '\0';

		part_storage = unquote_string(qpart_storage);
		if (strncmp(part_storage, "ST:", 3) == 0) {
		    char *ps = part_storage;
		    part_storage = g_strdup(ps+3);
		    g_free(ps);
		    skip_whitespace(s, ch);
		    qnext_string = s - 1;
		    skip_quoted_string(s, ch);
		    s[-1] = '\0';
		    next_string = unquote_string(qnext_string);
		} else {
		    next_string = part_storage;
		    part_storage = g_strdup(get_config_name());
		}

		if (curlog == L_PART || curlog == L_PARTPARTIAL) {
		    part_label = next_string;
		    if (!g_hash_table_lookup(valid_label, part_label)) {
			amfree(part_label);
			amfree(part_storage);
			continue;
		    }
		    amfree(current_label);
		    current_label = part_label;
		    amfree(current_storage);
		    current_storage = part_storage;

		    skip_whitespace(s, ch);
		    if (ch == '\0') {
			g_printf("strange log line in %s \"%s\"\n",
			   logfile, curstr);
			continue;
		    }

		    number = s - 1;
		    skip_non_whitespace(s, ch);
		    s[-1] = '\0';
		    fileno = atoi(number);
		    filenum = fileno;
		    if (filenum == 0)
			continue;

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			g_printf("strange log line in %s \"%s\"\n",
			   logfile, curstr);
			continue;
		    }
		    amfree(host);
		    host = s - 1;
		    skip_non_whitespace(s, ch);
		    s[-1] = '\0';
		    host = g_strdup(host);
		} else {
		    amfree(part_storage);
		    amfree(host);
		    host = next_string;
		}
	    } else {
		taper_part = 0;
		    amfree(host);
		host = s - 1;
		skip_non_whitespace(s, ch);
		s[-1] = '\0';
		host = g_strdup(host);
	    }

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		g_printf(_("strange log line in %s \"%s\"\n"),
		    logfile, curstr);
		continue;
	    }
	    qdisk = s - 1;
	    skip_quoted_string(s, ch);
	    disk_undo = s - 1;
	    *disk_undo = '\0';
	    amfree(disk);
	    disk = unquote_string(qdisk);

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		g_printf(_("strange log line in %s \"%s\"\n"),
                         logfile, curstr);
		amfree(disk);
		continue;
	    }
	    amfree(date);
	    date = s - 1;
	    skip_non_whitespace(s, ch);
	    date_undo = s - 1;
	    *date_undo = '\0';
	    date = g_strdup(date);

	    if(strlen(date) < 3) { /* old log didn't have datestamp */
		level = atoi(date);
		date = g_strdup(datestamp);
		partnum = 1;
		totalparts = 1;
	    } else {
		if (curprog == P_TAPER &&
			(curlog == L_CHUNK || curlog == L_PART ||
			 curlog == L_PARTPARTIAL || curlog == L_PARTIAL ||
			 curlog == L_DONE)) {
		    char *s1, ch1;
		    skip_whitespace(s, ch);
		    number = s - 1;
		    skip_non_whitespace(s, ch);
		    s1 = &s[-1];
		    ch1 = *s1;
		    skip_whitespace(s, ch);
		    if (*(s-1) != '[') {
			*s1 = ch1;
			sscanf(number, "%d/%d", &partnum, &totalparts);
			if (partnum > maxparts)
			    maxparts = partnum;
			if (totalparts > maxparts)
			    maxparts = totalparts;
		    } else { /* nparts is not in all PARTIAL lines */
			partnum = 1;
			totalparts = 1;
			s = number + 1;
		    }
		} else {
		    skip_whitespace(s, ch);
		}
		if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
		    g_printf(_("Fstrange log line in %s \"%s\"\n"),
		    logfile, s-1);
		    amfree(disk);
		    continue;
		}
		skip_integer(s, ch);
	    }

	    skip_whitespace(s, ch);
	    if(ch == '\0') {
		g_printf(_("strange log line in %s \"%s\"\n"),
		    logfile, curstr);
		amfree(disk);
		continue;
	    }
	    rest = s - 1;
	    skip_non_whitespace(s, ch);
	    rest_undo = s - 1;
	    *rest_undo = '\0';
	    crc1.crc = 0;
	    crc1.size = 0;
	    crc2.crc = 0;
	    crc2.size = 0;
	    crc3.crc = 0;
	    crc3.size = 0;
	    if (curlog == L_DONE) {
		if (!g_str_equal(rest, "[sec")) {
		    // CRC
		    parse_crc(rest, &crc1);
		    skip_whitespace(s, ch);
		    rest = s - 1;
		    skip_non_whitespace(s, ch);
		    rest_undo = s - 1;
		    *rest_undo = '\0';
		}
		if (!g_str_equal(rest, "[sec")) {
		    // CRC
		    parse_crc(rest, &crc2);
		    skip_whitespace(s, ch);
		    rest = s - 1;
		    skip_non_whitespace(s, ch);
		    rest_undo = s - 1;
		    *rest_undo = '\0';
		}
		if (!g_str_equal(rest, "[sec")) {
		    // CRC
		    parse_crc(rest, &crc3);
		    skip_whitespace(s, ch);
		    rest = s - 1;
		    skip_non_whitespace(s, ch);
		    rest_undo = s - 1;
		    *rest_undo = '\0';
		}
	    }
	    native_crc.crc = 0;
	    native_crc.size = 0;
	    client_crc.crc = 0;
	    client_crc.size = 0;
	    server_crc.crc = 0;
	    server_crc.size = 0;
	    if (curprog == P_DUMPER) {
		native_crc = crc1;
		client_crc = crc2;
	    } else if (curprog == P_CHUNKER) {
		server_crc = crc1;
	    } else if (curprog == P_TAPER) {
		native_crc = crc1;
		client_crc = crc2;
		server_crc = crc3;
	    }
	    if (g_str_equal(rest, "[sec")) {
		skip_whitespace(s, ch);
		if(ch == '\0') {
		    g_printf(_("strange log line in %s \"%s\"\n"),
			     logfile, curstr);
		    amfree(disk);
		    continue;
		}
		sec = atof(s - 1);
		skip_non_whitespace(s, ch);
		skip_whitespace(s, ch);
		rest = s - 1;
		skip_non_whitespace(s, ch);
		rest_undo = s - 1;
		*rest_undo = '\0';
		if (!g_str_equal(rest, "kb") &&
		    !g_str_equal(rest, "bytes")) {
		    g_printf(_("Bstrange log line in %s \"%s\"\n"),
			     logfile, curstr);
		    amfree(disk);
		    continue;
		}

		skip_whitespace(s, ch);
		if (ch == '\0') {
		     g_printf(_("strange log line in %s \"%s\"\n"),
			      logfile, curstr);
		     amfree(disk);
		     continue;
		}
		if (g_str_equal(rest, "kb")) {
		    kb = atof(s - 1);
		    bytes = 0;
		} else {
		    bytes = atof(s - 1);
		    kb = bytes / 1024;
		}
		skip_non_whitespace(s, ch);
		skip_whitespace(s, ch);
		rest = s - 1;
		skip_non_whitespace(s, ch);
		rest_undo = s - 1;
		*rest_undo = '\0';
		if (!g_str_equal(rest, "kps")) {
		    g_printf(_("Cstrange log line in %s \"%s\"\n"),
			     logfile, curstr);
		    amfree(disk);
		    continue;
		}

		skip_whitespace(s, ch);
		if (ch == '\0') {
		    g_printf(_("strange log line in %s \"%s\"\n"),
			     logfile, curstr);
		    amfree(disk);
		    continue;
		}
		/* kps = atof(s - 1); */
		skip_non_whitespace(s, ch);
		skip_whitespace(s, ch);
		rest = s - 1;
		skip_non_whitespace(s, ch);
		rest_undo = s - 1;
		*rest_undo = '\0';
		if (!g_str_equal(rest, "orig-kb")) {
		    orig_kb = 0;
		} else {

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			g_printf(_("strange log line in %s \"%s\"\n"),
				 logfile, curstr);
			amfree(disk);
			continue;
		    }
		    orig_kb = atof(s - 1);
		}
	    } else {
		sec = 0;
		kb = 0;
		bytes = 0;
		orig_kb = 0;
		*rest_undo = ' ';
	    }

	    if (g_str_has_prefix(rest, "error")) rest += 6;
	    if (g_str_has_prefix(rest, "config")) rest += 7;

	    dp = lookup_disk(host,disk);
	    if ( dp == NULL ) {
		if (dynamic_disklist == NULL) {
		    amfree(disk);
		    continue;
		}
		add_disk(dynamic_disklist, host, disk);
	    }
            if (find_match(host, disk)) {
		if(curprog == P_TAPER) {
		    char *key = g_strdup_printf(
					"HOST:%s DISK:%s: DATE:%s LEVEL:%d",
					host, disk, date, level);
		    find_result_t *new_output_find = g_new0(find_result_t, 1);
		    part_find = g_hash_table_lookup(part_by_dle, key);
		    maxparts = partnum;
		    if (maxparts < totalparts)
			maxparts = totalparts;
		    for (a_part_find = part_find;
			 a_part_find;
			 a_part_find = a_part_find->next) {
			if (maxparts < a_part_find->partnum)
			    maxparts = a_part_find->partnum;
			if (maxparts < a_part_find->totalparts)
			    maxparts = a_part_find->totalparts;
		    }
		    new_output_find->timestamp = g_string_chunk_insert_const(string_chunk, date);
		    new_output_find->write_timestamp = g_string_chunk_insert_const(string_chunk, datestamp);
		    new_output_find->hostname=g_string_chunk_insert_const(string_chunk, host);
		    new_output_find->diskname=g_string_chunk_insert_const(string_chunk, disk);
		    new_output_find->level=level;
		    new_output_find->partnum = partnum;
		    new_output_find->totalparts = totalparts;
		    new_output_find->label=g_string_chunk_insert_const(string_chunk, current_label);
		    if (current_storage) {
			new_output_find->storage=g_string_chunk_insert_const(string_chunk, current_storage);
		    }
		    new_output_find->status=NULL;
		    new_output_find->dump_status=NULL;
		    new_output_find->message="";
		    new_output_find->filenum=filenum;
		    new_output_find->sec=sec;
		    new_output_find->kb=kb;
		    new_output_find->bytes=bytes;
		    new_output_find->orig_kb=orig_kb;
		    new_output_find->next=NULL;
		    new_output_find->native_crc = native_crc;
		    new_output_find->client_crc = client_crc;
		    new_output_find->server_crc = server_crc;
		    if (curlog == L_SUCCESS) {
			new_output_find->status = "OK";
			new_output_find->dump_status = "OK";
			new_output_find->next = *output_find;
			new_output_find->partnum = 1; /* L_SUCCESS is pre-splitting */
			*output_find = new_output_find;
                        found_something = TRUE;
		    } else if (curlog == L_CHUNKSUCCESS || curlog == L_DONE ||
			       curlog == L_PARTIAL      || curlog == L_FAIL) {
			/* result line */
			if (curlog == L_PARTIAL || curlog == L_FAIL) {
			    /* set dump_status of each part */
			    for (a_part_find = part_find;
				 a_part_find;
				 a_part_find = a_part_find->next) {
				if (curlog == L_PARTIAL)
				    a_part_find->dump_status = "PARTIAL";
				else {
				    a_part_find->dump_status = "FAIL";
				    a_part_find->message = g_string_chunk_insert_const(string_chunk, rest);
				}
			    }
			} else {
			    if (maxparts > -1) { /* format with part */
				/* must check if all part are there */
				int num_part = maxparts;
				for (a_part_find = part_find;
				     a_part_find;
				     a_part_find = a_part_find->next) {
				    if (a_part_find->partnum == num_part &&
					g_str_equal(a_part_find->status, "OK"))
					num_part--;
			        }
				/* set dump_status of each part */
				for (a_part_find = part_find;
				     a_part_find;
				     a_part_find = a_part_find->next) {
				    if (num_part == 0) {
					a_part_find->dump_status = "OK";
				    } else {
					a_part_find->dump_status = "FAIL";
					a_part_find->message =
						g_string_chunk_insert_const(string_chunk, "Missing part");
				    }
				}
			    }
			}
			if (curlog == L_DONE) {
			    for (a_part_find = part_find;
				 a_part_find;
			         a_part_find = a_part_find->next) {
				if (a_part_find->totalparts == -1) {
				    a_part_find->totalparts = maxparts;
				}
				if (a_part_find->orig_kb == 0) {
				    a_part_find->orig_kb = orig_kb;
				}
				a_part_find->native_crc = native_crc;
				a_part_find->client_crc = client_crc;
				a_part_find->server_crc = server_crc;
			    }
			}
			if (part_find) { /* find last element */
			    for (a_part_find = part_find;
				 a_part_find->next != NULL;
				 a_part_find=a_part_find->next) {
			    }
			    /* merge part_find to *output_find */
			    a_part_find->next = *output_find;
			    *output_find = part_find;
			    part_find = NULL;
			    maxparts = -1;
                            found_something = TRUE;
			    g_hash_table_remove(part_by_dle, key);
			}
			free_find_result(&new_output_find);
		    } else { /* part line */
			if (curlog == L_PART || curlog == L_CHUNK) {
			    new_output_find->status = "OK";
			    new_output_find->dump_status = "OK";
			} else { /* PARTPARTIAL */
			    new_output_find->status = "PARTIAL";
			    new_output_find->dump_status = "PARTIAL";
			}
			/* Add to part_find list */
			if (part_find) {
			    new_output_find->next = part_find;
			    part_find = new_output_find;
			} else {
			    new_output_find->next = NULL;
			    part_find = new_output_find;
			}
			g_hash_table_insert(part_by_dle, g_strdup(key),
					    part_find);
			found_something = TRUE;
		    }
		    amfree(key);
		}
		else if(curlog == L_FAIL) {
		    char *status_failed;
		    /* print other failures too -- this is a hack to ensure that failures which
		     * did not make it to tape are also listed in the output of 'amadmin x find';
		     * users that do not want this information (e.g., Amanda::DB::Catalog) should
		     * filter dumps with a NULL label. */
		    find_result_t *new_output_find = g_new0(find_result_t, 1);
		    new_output_find->next = *output_find;
		    new_output_find->timestamp = g_string_chunk_insert_const(string_chunk, date);
		    new_output_find->write_timestamp = g_string_chunk_insert_const(string_chunk, "00000000000000"); /* dump was not written.. */
		    new_output_find->hostname=g_string_chunk_insert_const(string_chunk, host);
		    new_output_find->diskname=g_string_chunk_insert_const(string_chunk, disk);
		    if (current_storage != NULL) {
			new_output_find->storage=g_string_chunk_insert_const(string_chunk, current_storage);
		    }
		    new_output_find->level=level;
		    new_output_find->label=NULL;
		    new_output_find->partnum=partnum;
		    new_output_find->totalparts=totalparts;
		    new_output_find->filenum=0;
		    new_output_find->sec=sec;
		    new_output_find->kb=kb;
		    new_output_find->bytes=bytes;
		    new_output_find->orig_kb=orig_kb;
		    status_failed = g_strjoin(NULL, 
			 "FAILED (",
			 program_str[(int)curprog],
			 ") ",
			 rest,
			 NULL);
		    new_output_find->status = g_string_chunk_insert_const(string_chunk, status_failed);
		    amfree(status_failed);
		    new_output_find->dump_status="";
		    new_output_find->message="";
		    *output_find=new_output_find;
                    found_something = TRUE;
		    maxparts = -1;
		}
	    }
	}
    }
    amfree(host);
    amfree(date);
    amfree(disk);

    g_hash_table_destroy(valid_label);
    g_hash_table_destroy(part_by_dle);
    afclose(logf);
    amfree(datestamp);
    amfree(current_label);
    amfree(current_storage);
    amfree(disk);

    return found_something;
}


/*
 * Return the set of dumps that match *all* of the given patterns (we consider
 * an empty pattern to match .*, though).  If 'ok' is true, will only match
 * dumps with SUCCESS status.
 *
 * Returns a newly allocated list of results, where all strings are also newly
 * allocated.  Apparently some part of Amanda leaks under this condition.
 */
find_result_t *
dumps_match(
    find_result_t *output_find,
    char *hostname,
    char *diskname,
    char *datestamp,
    char *level,
    int ok)
{
    find_result_t *cur_result;
    find_result_t *matches = NULL;

    for(cur_result=output_find;
	cur_result;
	cur_result=cur_result->next) {
	char level_str[NUM_STR_SIZE];
	g_snprintf(level_str, sizeof(level_str), "%d", cur_result->level);
	if((!hostname || *hostname == '\0' || match_host(hostname, cur_result->hostname)) &&
	   (!diskname || *diskname == '\0' || match_disk(diskname, cur_result->diskname)) &&
	   (!datestamp || *datestamp== '\0' || match_datestamp(datestamp, cur_result->timestamp)) &&
	   (!level || *level== '\0' || match_level(level, level_str)) &&
	   (!ok || g_str_equal(cur_result->status, "OK")) &&
	   (!ok || g_str_equal(cur_result->dump_status, "OK"))){

	    find_result_t *curmatch = g_new0(find_result_t, 1);
	    memcpy(curmatch, cur_result, sizeof(find_result_t));

	    curmatch->timestamp = cur_result->timestamp;
	    curmatch->write_timestamp = cur_result->write_timestamp;
	    curmatch->hostname = cur_result->hostname;
	    curmatch->diskname = cur_result->diskname;
	    curmatch->level = cur_result->level;
	    curmatch->label = cur_result->label? cur_result->label : NULL;
	    curmatch->filenum = cur_result->filenum;
	    curmatch->sec = cur_result->sec;
	    curmatch->kb = cur_result->kb;
	    curmatch->bytes = cur_result->bytes;
	    curmatch->orig_kb = cur_result->orig_kb;
	    curmatch->status = cur_result->status;
	    curmatch->dump_status = cur_result->dump_status;
	    curmatch->message = cur_result->message;
	    curmatch->partnum = cur_result->partnum;
	    curmatch->totalparts = cur_result->totalparts;
	    curmatch->next = matches;
	    matches = curmatch;
	}
    }

    return(matches);
}

/*
 * Return the set of dumps that match one or more of the given dumpspecs,
 * If 'ok' is true, only dumps with a SUCCESS status will be matched.
 * 
 * Returns a newly allocated list of results, where all strings are also newly
 * allocated.  Apparently some part of Amanda leaks under this condition.
 */
find_result_t *
dumps_match_dumpspecs(
    find_result_t *output_find,
    GSList        *dumpspecs,
    int ok)
{
    find_result_t *cur_result;
    find_result_t *matches = NULL;
    GSList        *dumpspec;
    dumpspec_t    *ds;

    for(cur_result=output_find;
	cur_result;
	cur_result=cur_result->next) {
	char level_str[NUM_STR_SIZE];
	char *zeropad_ts = NULL;
	char *zeropad_w_ts = NULL;
	g_snprintf(level_str, sizeof(level_str), "%d", cur_result->level);

	/* get the timestamp padded to full width */
	if (strlen(cur_result->timestamp) < 14) {
	    zeropad_ts = g_new0(char, 15);
	    memset(zeropad_ts, '0', 14);
	    memcpy(zeropad_ts, cur_result->timestamp, strlen(cur_result->timestamp));
	}
	if (strlen(cur_result->write_timestamp) < 14) {
	    zeropad_w_ts = g_new0(char, 15);
	    memset(zeropad_w_ts, '0', 14);
	    memcpy(zeropad_w_ts, cur_result->write_timestamp, strlen(cur_result->write_timestamp));
	}

	for (dumpspec = dumpspecs; dumpspec; dumpspec = dumpspec->next) {
	    ds = (dumpspec_t *)dumpspec->data;
	    if((!ds->host || *ds->host == '\0' || match_host(ds->host, cur_result->hostname)) &&
	       (!ds->disk || *ds->disk == '\0' || match_disk(ds->disk, cur_result->diskname)) &&
	       (!ds->datestamp || *ds->datestamp== '\0'
			|| match_datestamp(ds->datestamp, cur_result->timestamp)
			|| (zeropad_ts && match_datestamp(ds->datestamp, zeropad_ts))) &&
	       (!ds->write_timestamp || *ds->write_timestamp== '\0'
			|| match_datestamp(ds->write_timestamp, cur_result->write_timestamp)
			|| (zeropad_w_ts && match_datestamp(ds->write_timestamp, zeropad_w_ts))) &&
	       (!ds->level || *ds->level== '\0' || match_level(ds->level, level_str)) &&
	       (!ok || g_str_equal(cur_result->status, "OK")) &&
	       (!ok || g_str_equal(cur_result->dump_status, "OK"))) {

		find_result_t *curmatch = g_malloc(sizeof(find_result_t));
		memcpy(curmatch, cur_result, sizeof(find_result_t));

		curmatch->timestamp = cur_result->timestamp;
		curmatch->write_timestamp = cur_result->write_timestamp;
		curmatch->hostname = cur_result->hostname;
		curmatch->diskname = cur_result->diskname;
		curmatch->storage = cur_result->storage;
		curmatch->level = cur_result->level;
		curmatch->label = cur_result->label? cur_result->label : NULL;
		curmatch->filenum = cur_result->filenum;
		curmatch->status = cur_result->status;
		curmatch->dump_status =  cur_result->dump_status;
		curmatch->message = cur_result->message;
		curmatch->partnum = cur_result->partnum;
		curmatch->totalparts = cur_result->totalparts;

		curmatch->next = matches;
		matches = curmatch;
		break;
	    }
	}

	amfree(zeropad_ts);
    }

    return(matches);
}

find_result_t *
dump_exist(
    find_result_t *output_find,
    char *hostname,
    char *diskname,
    char *datestamp,
    int level)
{
    find_result_t *output_find_result;

    for(output_find_result=output_find;
	output_find_result;
	output_find_result=output_find_result->next) {
	if( g_str_equal(output_find_result->hostname, hostname) &&
	    g_str_equal(output_find_result->diskname, diskname) &&
	    g_str_equal(output_find_result->timestamp, datestamp) &&
	    output_find_result->level == level) {

	    return output_find_result;
	}
    }
    return(NULL);
}
