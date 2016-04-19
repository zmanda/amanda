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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: diskfile.c,v 1.95 2006/07/26 15:17:37 martinea Exp $
 *
 * read disklist file
 */
#include "amanda.h"
#include "match.h"
#include "conffile.h"
#include "diskfile.h"
#include "amutil.h"
#include "amxml.h"

static am_host_t *hostlist = NULL;
static  disklist_t dlist = { NULL, NULL };
static netif_t *all_netifs = NULL;

/* local functions */
static char *upcase(char *st);
static int parse_diskline(disklist_t *, const char *, FILE *, int *, char **);
static void disk_parserror(const char *, int, const char *, ...)
			    G_GNUC_PRINTF(3, 4);


disklist_t *
get_disklist(void)
{
    return &dlist;
}

cfgerr_level_t
read_diskfile(
    const char *filename,
    disklist_t *lst)
{
    FILE *diskf;
    int line_num;
    char *line = NULL;

    /* initialize */
    if (hostlist == NULL) {
	lst->head = lst->tail = NULL;
    }
    line_num = 0;

    /* if we already have config errors, then don't bother */
    if (config_errors(NULL) >= CFGERR_ERRORS) {
	return config_errors(NULL);
    }

    if ((diskf = fopen(filename, "r")) == NULL) {
	config_add_error(CFGERR_ERRORS,
	    g_strdup_printf(_("Could not open '%s': %s"), filename, strerror(errno)));
	goto end;
        /*NOTREACHED*/
    }

    while ((line = agets(diskf)) != NULL) {
	line_num++;
	if (line[0] != '\0') {
	    if (parse_diskline(lst, filename, diskf, &line_num, &line) < 0) {
		goto end;
	    }
	}
	amfree(line);
    }

end:
    amfree(line);
    afclose(diskf);
    dlist = *lst;
    return config_errors(NULL);
}

am_host_t *
get_hostlist(void)
{
    return hostlist;
}

am_host_t *
lookup_host(
    const char *hostname)
{
    am_host_t *p;

    for (p = hostlist; p != NULL; p = p->next) {
	if(strcasecmp(p->hostname, hostname) == 0) return p;
    }
    return (NULL);
}

disk_t *
lookup_disk(
    const char *hostname,
    const char *diskname)
{
    am_host_t *host;
    disk_t *disk;

    host = lookup_host(hostname);
    if (host == NULL)
	return (NULL);

    for (disk = host->disks; disk != NULL; disk = disk->hostnext) {
	if (g_str_equal(disk->name, diskname))
	    return (disk);
    }
    return (NULL);
}


/*
 * put disk on end of queue
 */

void
enqueue_disk(
    disklist_t *list,
    disk_t *	disk)
{
    list->head = g_am_list_insert_after(list->head, list->tail, disk);
    if (list->tail) {
	list->tail = list->tail->next;
    } else {
	list->tail = list->head;
    }
}


/*
 * put disk on head of queue
 */

void
headqueue_disk(
    disklist_t *list,
    disk_t *	disk)
{
    list->head = g_list_prepend(list->head, disk);
    if (!list->tail) {
	list->tail = list->head;
    }
}


/*
 * insert in sorted order
 */

void
insert_disk(
    disklist_t *list,
    disk_t *	disk,
    int		(*cmp)(disk_t *a, disk_t *b))
{
    GList *ptr;

    ptr = list->head;

    while(ptr != NULL) {
	if(cmp(disk, ptr->data) < 0) break;
	ptr = ptr->next;
    }
    if (ptr) {
	list->head = g_list_insert_before(list->head, ptr, disk);
	if (!list->tail) {
	    list->tail = list->head;
	}
    } else {
	enqueue_disk(list, disk);
    }
}

disk_t *
add_disk(
    disklist_t *list,
    char *	hostname,
    char *	diskname)
{
    disk_t *disk;
    am_host_t *host;

    disk = g_malloc(sizeof(disk_t));
    bzero(disk, sizeof(disk_t));
    disk->line = 0;
    disk->allow_split = 0;
    disk->max_warnings = 20;
    disk->tape_splitsize = (off_t)0;
    disk->split_diskbuffer = NULL;
    disk->fallback_splitsize = (off_t)0;
    disk->hostname = g_strdup(hostname);
    disk->name = g_strdup(diskname);
    disk->device = g_strdup(diskname);
    disk->spindle = -1;
    disk->status = 0;
    disk->compress = COMP_NONE;
    disk->encrypt  = ENCRYPT_NONE;
    disk->start_t = 0;
    disk->todo = 1;
    disk->index = 1;
    disk->exclude_list = NULL;
    disk->exclude_file = NULL;
    disk->include_list = NULL;
    disk->include_file = NULL;
    disk->application = NULL;
    disk->pp_scriptlist = NULL;

    host = lookup_host(hostname);
    if(host == NULL) {
	host = g_malloc(sizeof(am_host_t));
	host->next = hostlist;
	hostlist = host;

	host->hostname = g_strdup(hostname);
	host->disks = NULL;
	host->inprogress = 0;
	host->maxdumps = 1;
	host->netif = NULL;
	host->start_t = 0;
	host->status = 0;
	host->features = NULL;
	host->pre_script = 0;
	host->post_script = 0;
    }
    enqueue_disk(list, disk);

    disk->host = host;
    disk->hostnext = host->disks;
    host->disks = disk;

    return disk;
}


/*
 * check if disk is present in list. Return true if so, false otherwise.
 */

int
find_disk(
    disklist_t *list,
    disk_t *	disk)
{
    GList *glist = list->head;

    while (glist && glist->data != disk) {
        glist = glist->next;
    }
    return (glist && glist->data == disk);
}


/*
 * sort a whole queue
 */

void
sort_disk(
    disklist_t *in,
    disklist_t *out,
    int		(*cmp)(disk_t *a, disk_t *b))
{
    disklist_t *tmp;
    disk_t *disk;

    tmp = in;		/* just in case in == out */

    out->head = NULL;
    out->tail = NULL;

    while((disk = dequeue_disk(tmp)))
	insert_disk(out, disk, cmp);
}


/*
 * remove disk from front of queue
 */

disk_t *
dequeue_disk(
    disklist_t *list)
{
    disk_t *disk;

    if(list->head == NULL) return NULL;

    disk = list->head->data;
    list->head = g_list_delete_link(list->head, list->head);

    if(list->head == NULL) list->tail = NULL;

    return disk;
}

void
remove_disk(
    disklist_t *list,
    disk_t *	disk)
{
    GList *ltail;

    if (list->tail && list->tail->data == disk) {
	ltail = list->tail;
	list->tail = list->tail->prev;
	list->head = g_list_delete_link(list->head, ltail);
    } else {
	list->head = g_list_remove(list->head, disk);
    }
}

void
free_disklist(
    disklist_t* dl)
{
    if (dlist.head == dl->head &&
	dlist.tail == dl->tail) {
	dlist.head = NULL;
	dlist.tail = NULL;
    }
    while (dequeue_disk(dl));
}


void
unload_disklist(void)
{
    disk_t    *dp, *dpnext;
    am_host_t *host, *hostnext;
    netif_t *netif, *next_if;

    for(host=hostlist; host != NULL; host = hostnext) {
	amfree(host->hostname);
	am_release_feature_set(host->features);
	host->features = NULL;
	hostnext = host->next;
	for (dp = host->disks; dp != NULL ; dp = dpnext) {
	    dpnext = dp->hostnext;
	    amfree(dp->filename);
	    amfree(dp->name);
	    amfree(dp->hostname);
	    amfree(dp->device);
	    free_sl(dp->exclude_file);
	    free_sl(dp->exclude_list);
	    free_sl(dp->include_file);
	    free_sl(dp->include_list);
	    free(dp);
	}
	amfree(host);
    }
    hostlist=NULL;
    dlist.head = NULL;
    dlist.tail = NULL;

    for (netif = all_netifs; netif != NULL; netif = next_if) {
	next_if = netif->next;
	amfree(netif);
    }
    all_netifs = NULL;
}

static char *
upcase(
    char *st)
{
    char *s = st;

    while(*s) {
	if(islower((int)*s)) *s = (char)toupper((int)*s);
	s++;
    }
    return st;
}


/* return  0 on success */
/* return -1 on error   */
static int
parse_diskline(
    disklist_t *lst,
    const char *filename,
    FILE *	diskf,
    int *	line_num_p,
    /*@keep@*/ char **	line_p)
{
    am_host_t *host;
    disk_t *disk;
    dumptype_t *dtype;
    netif_t *netif = NULL;
    interface_t *cfg_if = NULL;
    char *hostname = NULL;
    char *diskname, *diskdevice;
    char *dumptype;
    char *s, *fp;
    int ch, dup = 0;
    char *line = *line_p;
    int line_num = *line_num_p;
    struct tm *stm;
    time_t st;
    char *shost, *sdisk;
    am_host_t *p;
    disk_t *dp;
    identlist_t pp_iter;

    assert(filename != NULL);
    assert(line_num > 0);
    assert(line != NULL);

    s = line;
    ch = *s++;
    skip_whitespace(s, ch);
    if(ch == '\0' || ch == '#')
	return (0);

    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    if (g_str_equal(fp, "includefile")) {
	char *include_name;
	char *include_filename;
	skip_whitespace(s, ch);
	if (ch == '\0' || ch == '#') {
	    disk_parserror(filename, line_num, _("include filename name expected"));
	    return (-1);
	}
	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	include_name = unquote_string(fp);
	include_filename = config_dir_relative(include_name);
	read_diskfile(include_filename, lst);
	g_free(include_filename);
	g_free(include_name);
	if (config_errors(NULL) >= CFGERR_WARNINGS) {
	    return -1;
	} else {
	    return 0;
	}
    }
    host = lookup_host(fp);
    if (host == NULL) {
	hostname = g_strdup(fp);
    } else {
	hostname = g_strdup(host->hostname);
	if (!g_str_equal(host->hostname, fp)) {
	    disk_parserror(filename, line_num, _("Same host with different case: \"%s\" and \"%s\"."), host->hostname, fp);
	    return -1;
	}
    }

    shost = sanitise_filename(hostname);
    for (p = hostlist; p != NULL; p = p->next) {
	char *shostp = sanitise_filename(p->hostname);
	if (!g_str_equal(hostname, p->hostname) &&
	    g_str_equal(shost, shostp)) {
	    disk_parserror(filename, line_num, _("Two hosts are mapping to the same name: \"%s\" and \"%s\""), p->hostname, hostname);
	    amfree(shost);
	    amfree(shostp);
	    return(-1);
	}
	else if (strcasecmp(hostname, p->hostname) &&
		 match_host(hostname, p->hostname) &&
		 match_host(p->hostname, hostname)) {
	    disk_parserror(filename, line_num, _("Duplicate host name: \"%s\" and \"%s\""), p->hostname, hostname);
	    amfree(shost);
	    amfree(shostp);
	    return(-1);
	}
	amfree(shostp);
    }
    amfree(shost);

    skip_whitespace(s, ch);
    if(ch == '\0' || ch == '#') {
	disk_parserror(filename, line_num, _("disk device name expected"));
	amfree(hostname);
	return (-1);
    }

    fp = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(fp);
    if (strlen(diskname) == 0) {
	disk_parserror(filename, line_num, _("invalid empty diskname"));
	amfree(hostname);
	return (-1);
    }
    skip_whitespace(s, ch);
    if(ch == '\0' || ch == '#') {
	disk_parserror(filename, line_num, _("disk dumptype expected"));
	amfree(hostname);
	amfree(diskname);
	return (-1);
    }
    fp = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';

    /* diskdevice */
    dumptype = NULL;
    diskdevice = NULL;
    if(fp[0] != '{') {
	dumptype = unquote_string(fp);
	if (strlen(dumptype) == 0) {
	    disk_parserror(filename, line_num, _("invalid empty diskdevice"));
	    amfree(hostname);
	    return (-1);
	}
	if (lookup_dumptype(dumptype) == NULL) {
	    diskdevice = dumptype;
	    skip_whitespace(s, ch);
	    if(ch == '\0' || ch == '#') {
		disk_parserror(filename, line_num,
			_("disk dumptype '%s' not found"), dumptype);
		amfree(hostname);
		amfree(diskdevice);
		amfree(diskname);
		return (-1);
	    }

	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    if (fp[0] != '{') {
		dumptype = unquote_string(fp);
	    }
	}
    }

    /* check for duplicate disk */
    disk = NULL;
    if (host) {
	if ((disk = lookup_disk(hostname, diskname)) != NULL) {
	    dup = 1;
	} else {
	    disk = host->disks;
	    do {
		char *a1, *a2;
		a1 = clean_regex(diskname, 1);
		a2 = clean_regex(disk->name, 1);

		if (match_disk(a1, disk->name) && match_disk(a2, diskname)) {
		    dup = 1;
		} else {
		    disk = disk->hostnext;
		}
		amfree(a1);
		amfree(a2);
	    }
	    while (dup == 0 && disk != NULL);
	}
	if (dup == 1) {
	    disk_parserror(filename, line_num,
			   _("duplicate disk record, previous on line %d"),
			   disk->line);
	}
    }
    if (!disk) {
	disk = g_malloc(sizeof(disk_t));
	disk->filename = g_strdup(filename);
	disk->line = line_num;
	disk->hostname = hostname;
	disk->name = diskname;
	disk->device = diskdevice;
	disk->spindle = -1;
	disk->status = 0;
	disk->inprogress = 0;
	disk->application = NULL;
	disk->pp_scriptlist = NULL;
	disk->dataport_list = NULL;
	disk->shm_name = NULL;
    }

    if (host) {
	sdisk = sanitise_filename(diskname);
	for (dp = host->disks; dp != NULL; dp = dp->hostnext) {
	    char *sdiskp = sanitise_filename(dp->name);
	    if (!g_str_equal(diskname, dp->name) &&
		 g_str_equal(sdisk, sdiskp)) {
		disk_parserror(filename, line_num,
		 _("Two disks are mapping to the same name: \"%s\" and \"%s\"; you must use different diskname"),
			       dp->name, diskname);
		amfree(sdiskp);
		amfree(sdisk);
		return(-1);
	    }
	    amfree(sdiskp);
	}
	amfree(sdisk);
    }

    if (fp[0] == '{') {
	s[-1] = (char)ch;
	s = fp+2;
	skip_whitespace(s, ch);
	if (ch != '\0' && ch != '#') {
	    disk_parserror(filename, line_num,
		      _("expected line break after `{\', ignoring rest of line"));
	}

	if (strchr(s-1, '}') &&
	    (strchr(s-1, '#') == NULL ||
	     strchr(s-1, '}') < strchr(s-1, '#'))) {
	    disk_parserror(filename, line_num,_("'}' on same line than '{'"));
	    amfree(hostname);
	    if(!dup) {
		amfree(disk->device);
		amfree(disk->name);
		amfree(disk);
	    } else {
		amfree(diskdevice);
		amfree(diskname);
	    }
	    return (-1);
	}
	dtype = read_dumptype(custom_escape(g_strjoin(NULL, "custom(", hostname,
					":", disk->name, ")",
					".", anonymous_value(), NULL)),
			      diskf, (char*)filename, line_num_p);
	if (dtype == NULL || dup) {
	    disk_parserror(filename, line_num,
			   _("read of custom dumptype failed"));
	    amfree(hostname);
	    if(!dup) {
		amfree(disk->device);
	        amfree(disk->name);
	        amfree(disk);
	    } else {
		amfree(diskdevice);
		amfree(diskname);
	    }
	    return (-1);
	}
	amfree(line);

	*line_p = line = agets(diskf);
	line_num = *line_num_p; /* no incr, read_dumptype did it already */

	if (line == NULL)
	    *line_p = line = g_strdup("");
	s = line;
	ch = *s++;
    } else {
	if((dtype = lookup_dumptype(dumptype)) == NULL) {
	    char *qdt = quote_string(dumptype);

	    disk_parserror(filename, line_num, _("undefined dumptype `%s'"), qdt);
	    amfree(qdt);
	    amfree(hostname);
	    if (!dup) {
		amfree(disk->device);
		amfree(disk->name);
		amfree(disk);
	    } else {
		amfree(diskdevice);
		amfree(diskname);
	    }
	    return (-1);
	}
	amfree(dumptype);
    }

    if (dup) {
	/* disk_parserror already called, above */
	g_assert(config_errors(NULL) != CFGERR_OK);
	amfree(hostname);
	amfree(diskdevice);
	amfree(diskname);
	return (-1);
    }

    disk->dtype_name	     = dumptype_name(dtype);
    disk->config	     = dtype;
    disk->program	     = dumptype_get_program(dtype);
    disk->exclude_list     = duplicate_sl(dumptype_get_exclude(dtype).sl_list);
    disk->exclude_file     = duplicate_sl(dumptype_get_exclude(dtype).sl_file);
    disk->exclude_optional   = dumptype_get_exclude(dtype).optional;
    disk->include_list     = duplicate_sl(dumptype_get_include(dtype).sl_list);
    disk->include_file     = duplicate_sl(dumptype_get_include(dtype).sl_file);
    disk->include_optional   = dumptype_get_include(dtype).optional;
    disk->priority	     = dumptype_get_priority(dtype);
    disk->dumpcycle	     = dumptype_get_dumpcycle(dtype);
    disk->auth               = dumptype_get_auth(dtype);
    disk->maxdumps	     = dumptype_get_maxdumps(dtype);
    disk->allow_split        = dumptype_get_allow_split(dtype);
    disk->max_warnings       = dumptype_get_max_warnings(dtype);
    disk->tape_splitsize     = dumptype_get_tape_splitsize(dtype);
    disk->split_diskbuffer   = dumptype_get_split_diskbuffer(dtype);
    disk->fallback_splitsize = dumptype_get_fallback_splitsize(dtype);
    disk->maxpromoteday	     = dumptype_get_maxpromoteday(dtype);
    disk->bumppercent	     = dumptype_get_bumppercent(dtype);
    disk->bumpsize	     = dumptype_get_bumpsize(dtype);
    disk->bumpdays	     = dumptype_get_bumpdays(dtype);
    disk->bumpmult	     = dumptype_get_bumpmult(dtype);
    disk->starttime          = dumptype_get_starttime(dtype);
    disk->application        = dumptype_get_application(dtype);
    disk->pp_scriptlist      = dumptype_get_scriptlist(dtype);
    disk->start_t = 0;
    if (disk->starttime > 0) {
	st = time(NULL);
	disk->start_t = st;
	stm = localtime(&st);
	disk->start_t -= stm->tm_sec + 60 * stm->tm_min + 3600 * stm->tm_hour;
	disk->start_t += disk->starttime / 100 * 3600 +
			 disk->starttime % 100 * 60;
	if ((disk->start_t - st) < -43200)
	    disk->start_t += 86400;
    }
    disk->strategy	     = dumptype_get_strategy(dtype);
    disk->ignore	     = dumptype_get_ignore(dtype);
    disk->estimatelist	     = dumptype_get_estimatelist(dtype);
    disk->compress	     = dumptype_get_compress(dtype);
    disk->srvcompprog	     = dumptype_get_srvcompprog(dtype);
    disk->clntcompprog	     = dumptype_get_clntcompprog(dtype);
    disk->encrypt            = dumptype_get_encrypt(dtype);
    disk->srv_decrypt_opt    = dumptype_get_srv_decrypt_opt(dtype);
    disk->clnt_decrypt_opt   = dumptype_get_clnt_decrypt_opt(dtype);
    disk->srv_encrypt        = dumptype_get_srv_encrypt(dtype);
    disk->clnt_encrypt       = dumptype_get_clnt_encrypt(dtype);
    disk->amandad_path       = dumptype_get_amandad_path(dtype);
    disk->client_username    = dumptype_get_client_username(dtype);
    disk->ssl_fingerprint_file= dumptype_get_ssl_fingerprint_file(dtype);
    disk->ssl_cert_file      = dumptype_get_ssl_cert_file(dtype);
    disk->ssl_key_file       = dumptype_get_ssl_key_file(dtype);
    disk->ssl_ca_cert_file   = dumptype_get_ssl_ca_cert_file(dtype);
    disk->ssl_cipher_list    = dumptype_get_ssl_cipher_list(dtype);
    if (dumptype_seen(dtype, DUMPTYPE_SSL_CHECK_HOST)) {
	disk->ssl_check_host     = dumptype_get_ssl_check_host(dtype);
    } else {
	disk->ssl_check_host     = getconf_boolean(CNF_SSL_CHECK_HOST);
    }
    if (dumptype_seen(dtype, DUMPTYPE_SSL_CHECK_CERTIFICATE_HOST)) {
        disk->ssl_check_certificate_host = dumptype_get_ssl_check_certificate_host(dtype);
    } else {
	disk->ssl_check_certificate_host = getconf_boolean(CNF_SSL_CHECK_CERTIFICATE_HOST);
    }
    if (dumptype_seen(dtype, DUMPTYPE_SSL_CHECK_FINGERPRINT)) {
        disk->ssl_check_fingerprint  = dumptype_get_ssl_check_fingerprint(dtype);
    } else {
	disk->ssl_check_fingerprint  = getconf_boolean(CNF_SSL_CHECK_FINGERPRINT);
    }
    disk->client_port        = dumptype_get_client_port(dtype);
    disk->ssh_keys           = dumptype_get_ssh_keys(dtype);
    disk->comprate[0]	     = dumptype_get_comprate(dtype)[0];
    disk->comprate[1]	     = dumptype_get_comprate(dtype)[1];
    disk->data_path	     = dumptype_get_data_path(dtype);
    disk->dump_limit	     = dumptype_get_dump_limit(dtype);
    disk->retry_dump	     = dumptype_get_retry_dump(dtype);
    disk->tags		     = dumptype_get_tags(dtype);

    /*
     * Boolean parameters with no value (Appears here as value 2) defaults
     * to TRUE for backward compatibility and for logical consistency.
     */
    disk->record	     = dumptype_get_record(dtype) != 0;
    disk->skip_incr	     = dumptype_get_skip_incr(dtype) != 0;
    disk->skip_full	     = dumptype_get_skip_full(dtype) != 0;
    disk->orig_holdingdisk   = dumptype_get_to_holdingdisk(dtype);
    disk->to_holdingdisk     = disk->orig_holdingdisk;
    disk->kencrypt	     = dumptype_get_kencrypt(dtype) != 0;
    disk->index		     = dumptype_get_index(dtype) != 0;

    disk->todo		     = 1;

    skip_whitespace(s, ch);
    fp = s - 1;
    if(ch && ch != '#') {		/* get optional spindle number */
	char *fp1;
	int is_digit=1;

	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	fp1=fp;
	if (*fp1 == '-') fp1++;
	for(;*fp1!='\0';fp1++) {
	    if(!isdigit((int)*fp1)) {
		is_digit = 0;
	    }
	}
	if(is_digit == 0) {
	    disk_parserror(filename, line_num, _("non-integer spindle `%s'"), fp);
	    amfree(hostname);
	    amfree(disk->name);
	    amfree(disk);
	    return (-1);
	}
	disk->spindle = atoi(fp);
	skip_integer(s, ch);
    }

    skip_whitespace(s, ch);
    fp = s - 1;
    if(ch && ch != '#') {		/* get optional network interface */
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	if((cfg_if = lookup_interface(upcase(fp))) == NULL) {
	    disk_parserror(filename, line_num,
		_("undefined network interface `%s'"), fp);
	    amfree(hostname);
	    amfree(disk->name);
	    amfree(disk);
	    return (-1);
	}
    } else {
	cfg_if = lookup_interface("default");
    }

    /* see if we already have a netif_t for this interface */
    for (netif = all_netifs; netif != NULL; netif = netif->next) {
	if (netif->config == cfg_if)
	    break;
    }

    /* nope; make up a new one */
    if (!netif) {
	netif = g_malloc(sizeof(*netif));
	netif->next = all_netifs;
	all_netifs = netif;
	netif->config = cfg_if;
	netif->curusage = 0;
    }

    skip_whitespace(s, ch);
    if(ch && ch != '#') {		/* now we have garbage, ignore it */
	disk_parserror(filename, line_num, _("end of line expected"));
    }

    if (disk->program && disk->application &&
	!g_str_equal(disk->program, "APPLICATION")) {
	disk_parserror(filename, line_num,
		       _("Both program and application set"));
    }

    if (disk->program && g_str_equal(disk->program, "APPLICATION") &&
	!disk->application) {
	disk_parserror(filename, line_num,
		       _("program set to APPLICATION but no application set"));
    }

    if (disk->application) {
	application_t *application;
	char          *plugin;

	application = lookup_application(disk->application);
	g_assert(application != NULL);
	plugin = application_get_plugin(application);
	if (!plugin || strlen(plugin) == 0) {
	    disk_parserror(filename, line_num,
			   _("plugin not set for application"));
	}
    }

    for (pp_iter = disk->pp_scriptlist; pp_iter != NULL;
	 pp_iter = pp_iter->next) {
	pp_script_t *pp_script;
	char        *plugin;
	char        *pp_script_name;

	pp_script_name = (char*)pp_iter->data;
	pp_script = lookup_pp_script(pp_script_name);
	g_assert(pp_script != NULL);
	plugin = pp_script_get_plugin(pp_script);
	if (!plugin || strlen(plugin) == 0) {
	    disk_parserror(filename, line_num, _("plugin not set for script"));
	}
    }

    /* success, add disk to lists */

    if(host == NULL) {			/* new host */
	host = g_malloc(sizeof(am_host_t));
	host->next = hostlist;
	hostlist = host;

	host->hostname = g_strdup(hostname);
	hostname = NULL;
	host->disks = NULL;
	host->inprogress = 0;
	host->maxdumps = 1;		/* will be overwritten */
	host->netif = NULL;
	host->start_t = 0;
	host->status = 0;
	host->features = NULL;
	host->pre_script = 0;
	host->post_script = 0;
    }

    host->netif = netif;

    enqueue_disk(lst, disk);

    disk->host = host;
    disk->hostnext = host->disks;
    host->disks = disk;
    host->maxdumps = disk->maxdumps;

    return (0);
}

G_GNUC_PRINTF(3, 4)
static void disk_parserror(const char *filename, int line_num,
    const char *format, ...)
{
    va_list argp;
    char * msg;
    char * errstr;

    /* format the error message and hand it off to conffile */

    arglist_start(argp, format);
    msg = g_strdup_vprintf(format, argp);
    errstr = g_strdup_printf("\"%s\", line %d: %s", filename, line_num, msg);
    amfree(msg);
    arglist_end(argp);

    config_add_error(CFGERR_ERRORS, errstr);
}


void
dump_queue(
    char *	st,
    disklist_t	q,
    int		npr,	/* we print first npr disks on queue, plus last two */
    FILE *	f)
{
    GList *dl, *pl;
    disk_t *d;
    int pos;
    char *qname;

    if (empty(q)) {
	g_fprintf(f, _("%s QUEUE: empty\n"), st);
	return;
    }
    g_fprintf(f, _("%s QUEUE:\n"), st);
    for (pos = 0, dl = q.head, pl = NULL; dl != NULL; pl = dl, dl = dl->next, pos++) {
	d = dl->data;
	qname = quote_string(d->name);
	if(pos < npr) g_fprintf(f, "%3d: %-10s %-4s\n",
			      pos, d->host->hostname, qname);
	amfree(qname);
    }
    if(pos > npr) {
	if(pos > npr+2) g_fprintf(f, "  ...\n");
	if(pos > npr+1) {
	    dl = pl->prev;
	    d = dl->data;
	    g_fprintf(f, "%3d: %-10s %-4s\n", pos-2, d->host->hostname, d->name);
	}
	dl = pl;
	d = dl->data;
	g_fprintf(f, "%3d: %-10s %-4s\n", pos-1, d->host->hostname, d->name);
    }
}

/**
 * Validate an option string for a given DLE. Returns NULL if all is OK.
 * Otherwise, return a string array to be freed by the caller using
 * g_strfreev().
 *
 * @param dp: the DLE
 * @returns: a gchar ** if any errors, or NULL
 */

gchar **validate_optionstr(disk_t *dp)
{
    GPtrArray *errarray;
    gchar **ret;
    int nb_exclude, nb_include;
    am_feature_t *their_features = dp->host->features;

    assert(dp != NULL);
    assert(dp->host != NULL);

    errarray = g_ptr_array_new();

    if (!am_has_feature(their_features, fe_options_auth)) {
	if (strcasecmp(dp->auth, "bsd") == 0)
	    if (!am_has_feature(their_features, fe_options_bsd_auth))
		g_ptr_array_add(errarray, g_strdup("does not support auth"));
    }

    switch(dp->compress) {
    case COMP_FAST:
	if (!am_has_feature(their_features, fe_options_compress_fast)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support fast compression"));
	}
	break;
    case COMP_BEST:
	if (!am_has_feature(their_features, fe_options_compress_best)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support best compression"));
	}
	break;
    case COMP_CUST:
        if (am_has_feature(their_features, fe_options_compress_cust)) {
	    if (dp->clntcompprog == NULL || strlen(dp->clntcompprog) == 0) {
		g_ptr_array_add(errarray,
                                g_strdup("client custom compression with no compression program specified"));
	    }
	} else {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support client custom compression"));
	}
	break;
    case COMP_SERVER_FAST:
	break;
    case COMP_SERVER_BEST:
	break;
    case COMP_SERVER_CUST:
	if (dp->srvcompprog == NULL || strlen(dp->srvcompprog) == 0) {
	    g_ptr_array_add(errarray,
                            g_strdup("server custom compression with no compression program specified"));
	}
	break;
    }

    switch(dp->encrypt) {
    case ENCRYPT_CUST:
	if (am_has_feature(their_features, fe_options_encrypt_cust)) {
	    if (dp->clnt_decrypt_opt) {
		if (!am_has_feature(their_features, fe_options_client_decrypt_option)) {
		    g_ptr_array_add(errarray,
                                    g_strdup("does not support client decrypt option"));
		}
	    }
	    if (dp->clnt_encrypt == NULL || strlen(dp->clnt_encrypt) == 0) {
		g_ptr_array_add(errarray,
                                g_strdup("encrypt client with no encryption program specified"));
	    }
	    if (dp->compress == COMP_SERVER_FAST ||
		dp->compress == COMP_SERVER_BEST ||
		dp->compress == COMP_SERVER_CUST ) {
		g_ptr_array_add(errarray,
                                g_strdup("Client encryption with server compression is not supported. See amanda.conf(5) for detail"));
	    }
	} else {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support client data encryption"));
	}
	break;
    case ENCRYPT_SERV_CUST:
	if (dp->srv_encrypt == NULL || strlen(dp->srv_encrypt) == 0) {
	    g_ptr_array_add(errarray,
                            g_strdup("No encryption program specified in dumptypes, Change the dumptype in the disklist or mention the encryption program to use in the dumptypes file"));
	}
	break;
    }

    if (!dp->record) {
	if (!am_has_feature(their_features, fe_options_no_record)) {
	    g_ptr_array_add(errarray, g_strdup("does not support no record"));
	}
    }

    if (dp->index) {
	if (!am_has_feature(their_features, fe_options_index)) {
	    g_ptr_array_add(errarray, g_strdup("does not support index"));
	}
    }

    if (dp->kencrypt) {
	if (!am_has_feature(their_features, fe_options_kencrypt)) {
	    g_ptr_array_add(errarray, g_strdup("does not support kencrypt"));
	}
    }

    nb_exclude = 0;
    if (dp->exclude_file != NULL && dp->exclude_file->nb_element > 0) {
	nb_exclude = dp->exclude_file->nb_element;
	if (!am_has_feature(their_features, fe_options_exclude_file)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support exclude file"));
	}
    }

    if (dp->exclude_list != NULL && dp->exclude_list->nb_element > 0) {
	nb_exclude += dp->exclude_list->nb_element;
	if (!am_has_feature(their_features, fe_options_exclude_list)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support exclude list"));
	}
    }

    if (nb_exclude > 1 &&
	!am_has_feature(their_features, fe_options_multiple_exclude)) {
	g_ptr_array_add(errarray,
                        g_strdup("does not support multiple exclude"));
    }

    nb_include = 0;
    if (dp->include_file != NULL && dp->include_file->nb_element > 0) {
	nb_include = dp->include_file->nb_element;
	if (!am_has_feature(their_features, fe_options_include_file)) {
	    g_ptr_array_add(errarray, ("does not support include file"));
	}
    }

    if (dp->include_list != NULL && dp->include_list->nb_element > 0) {
	nb_include += dp->include_list->nb_element;
	if (!am_has_feature(their_features, fe_options_include_list)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support include list"));
	}
    }

    if (nb_include > 1 &&
	!am_has_feature(their_features, fe_options_multiple_exclude)) {
	g_ptr_array_add(errarray,
                        g_strdup("does not support multiple include"));
    }

    if (dp->exclude_optional) {
	if (!am_has_feature(their_features, fe_options_optional_exclude)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support optional exclude"));
	}
    }
    if (dp->include_optional) {
	if (!am_has_feature(their_features, fe_options_optional_include)) {
	    g_ptr_array_add(errarray,
                            g_strdup("does not support optional include"));
	}
    }

    g_ptr_array_add(errarray, NULL);

    ret = (gchar **)g_ptr_array_free(errarray, FALSE);

    if (!*ret) { /* No errors */
        g_strfreev(ret);
        ret = NULL;
    }

    return ret;
}

char *optionstr(disk_t *dp)
{
    GPtrArray *array = g_ptr_array_new();
    gchar **strings;
    char *result;
    sle_t *excl;
    char *qname;
    am_feature_t *their_features;

    g_assert(dp != NULL);
    g_assert(dp->host != NULL);

    their_features = dp->host->features;

    if (am_has_feature(their_features, fe_options_auth))
        g_ptr_array_add(array, g_strdup_printf("auth=%s", dp->auth));
    else if(strcasecmp(dp->auth, "bsd") == 0)
	if(am_has_feature(their_features, fe_options_bsd_auth))
            g_ptr_array_add(array, g_strdup("bsd-auth"));

    switch(dp->compress) {
    case COMP_FAST:
        g_ptr_array_add(array, g_strdup("compress-fast"));
	break;
    case COMP_BEST:
        g_ptr_array_add(array, g_strdup("compress-best"));
	break;
    case COMP_CUST:
        g_ptr_array_add(array, g_strdup_printf("comp-cust=%s",
            dp->clntcompprog));
	break;
    case COMP_SERVER_FAST:
        g_ptr_array_add(array, g_strdup("srvcomp-fast"));
	break;
    case COMP_SERVER_BEST:
        g_ptr_array_add(array, g_strdup("srvcomp-best"));
	break;
    case COMP_SERVER_CUST:
        g_ptr_array_add(array, g_strdup_printf("srvcomp-cust=%s",
            dp->srvcompprog));
	break;
    }

    switch(dp->encrypt) {
    case ENCRYPT_CUST:
        g_ptr_array_add(array, g_strdup_printf("encrypt-cust=%s",
            dp->clnt_encrypt));
	if (dp->clnt_decrypt_opt)
             g_ptr_array_add(array, g_strdup_printf("client-decrypt-option=%s",
                 dp->clnt_decrypt_opt));
	break;
    case ENCRYPT_SERV_CUST:
        g_ptr_array_add(array, g_strdup_printf("encrypt-serv-cust=%s",
            dp->srv_encrypt));
	if (dp->srv_decrypt_opt)
            g_ptr_array_add(array, g_strdup_printf("server-decrypt-option=%s",
	        dp->srv_decrypt_opt));
	break;
    }

    if (!dp->record)
        g_ptr_array_add(array, g_strdup("no-record"));

    if (dp->index)
        g_ptr_array_add(array, g_strdup("index"));

    if (dp->kencrypt)
        g_ptr_array_add(array, g_strdup("kencrypt"));

    if (dp->exclude_file && dp->exclude_file->nb_element > 0)
	for(excl = dp->exclude_file->first; excl; excl = excl->next) {
	    qname = quote_string(excl->name);
            g_ptr_array_add(array, g_strdup_printf("exclude-file=%s", qname));
	    g_free(qname);
	}

    if (dp->exclude_list && dp->exclude_list->nb_element > 0)
	for(excl = dp->exclude_list->first; excl; excl = excl->next) {
	    qname = quote_string(excl->name);
            g_ptr_array_add(array, g_strdup_printf("exclude-list=%s", qname));
	    g_free(qname);
	}

    if (dp->include_file && dp->include_file->nb_element > 0)
	for(excl = dp->include_file->first; excl; excl = excl->next) {
	    qname = quote_string(excl->name);
            g_ptr_array_add(array, g_strdup_printf("include-file=%s", qname));
	    g_free(qname);
	}

    if (dp->include_list && dp->include_list->nb_element > 0)
	for(excl = dp->include_list->first; excl; excl = excl->next) {
	    qname = quote_string(excl->name);
            g_ptr_array_add(array, g_strdup_printf("include-list=%s", qname));
	    g_free(qname);
	}

    if (dp->exclude_optional)
        g_ptr_array_add(array, g_strdup("exclude-optional"));

    if (dp->include_optional)
        g_ptr_array_add(array, g_strdup("include-optional"));

    /*
     * We always want a semicolon-terminated string, this will do the trick for
     * g_strjoinv() to do what is needed. And, of course, don't forget the NULL
     * pointer at the end.
     */
    g_ptr_array_add(array, g_strdup(""));
    g_ptr_array_add(array, NULL);

    strings = (gchar **)g_ptr_array_free(array, FALSE);

    result = g_strjoinv(";", strings);
    g_strfreev(strings);

    /* result contains at least 'auth=...' */
    return result;
}
 
typedef struct {
    am_feature_t  *features;
    char          *result;
} xml_app_t;

/* A GHFunc (callback for g_hash_table_foreach) */
static void xml_property(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p)
{
    char       *tmp;
    property_t *property = value_p;
    xml_app_t  *xml_app = user_data_p;
    GSList     *value;
    GString    *strbuf;

    strbuf = g_string_new(xml_app->result);

    tmp = amxml_format_tag("name", (char *)key_p);
    g_string_append_printf(strbuf, "    <property>\n      %s\n", tmp);
    g_free(tmp);

    // TODO if client have fe_xml_property_priority
    if (property->priority
        && am_has_feature(xml_app->features, fe_xml_property_priority))
        g_string_append(strbuf, "      <priority>yes</priority>\n");

    for (value = property->values; value != NULL; value = value->next) {
	tmp = amxml_format_tag("value", value->data);
        g_string_append_printf(strbuf, "      %s", tmp);
	g_free(tmp);
    }
    g_string_append_printf(strbuf, "\n    </property>\n");

    g_free(xml_app->result);
    xml_app->result = g_string_free(strbuf, FALSE);
}

char *xml_optionstr_disk(
    char *hostname,
    char *diskname)
{
    disk_t *dp = lookup_disk(hostname, diskname);

    return xml_optionstr(dp, 0);
}


char * xml_optionstr(disk_t *dp, int to_server)
{
    GPtrArray *array = g_ptr_array_new();
    gchar **strings;
    GString *strbuf;
    char *tmp;
    char *result;
    sle_t *excl;
    am_feature_t *their_features;

    g_assert(dp != NULL);
    g_assert(dp->host != NULL);

    their_features = dp->host->features;

    if (am_has_feature(their_features, fe_options_auth))
        g_ptr_array_add(array, g_strdup_printf("  <auth>%s</auth>", dp->auth));

    switch(dp->compress) {
    case COMP_FAST:
        g_ptr_array_add(array, g_strdup("  <compress>FAST</compress>"));
	break;
    case COMP_BEST:
        g_ptr_array_add(array, g_strdup("  <compress>BEST</compress>"));
	break;
    case COMP_CUST:
        g_ptr_array_add(array, g_strdup_printf("  <compress>CUSTOM"
            "<custom-compress-program>%s</custom-compress-program>\n"
            "  </compress>", dp->clntcompprog));
	break;
    case COMP_SERVER_FAST:
        g_ptr_array_add(array, g_strdup("  <compress>SERVER-FAST</compress>"));
	break;
    case COMP_SERVER_BEST:
        g_ptr_array_add(array, g_strdup("  <compress>SERVER-BEST</compress>"));
	break;
    case COMP_SERVER_CUST:
        g_ptr_array_add(array, g_strdup_printf("  <compress>SERVER-CUSTOM"
            "<custom-compress-program>%s</custom-compress-program>\n"
            "  </compress>", dp->srvcompprog));
	break;
    }

    switch(dp->encrypt) {
    case ENCRYPT_CUST:
        strbuf = g_string_new("  <encrypt>CUSTOM<custom-encrypt-program>");
        g_string_append_printf(strbuf, "%s</custom-encrypt-program>\n",
            dp->clnt_encrypt);
	if (dp->clnt_decrypt_opt)
            g_string_append_printf(strbuf, "    "
                "<decrypt-option>%s</decrypt-option>\n", dp->clnt_decrypt_opt);

        g_string_append(strbuf, "  </encrypt>");
        tmp = g_string_free(strbuf, FALSE);
        g_ptr_array_add(array, tmp);
	break;
    case ENCRYPT_SERV_CUST:
	if (to_server) {
            tmp = g_strdup_printf(
                "  <encrypt>SERVER-CUSTOM"
                "<custom-encrypt-program>%s</custom-encrypt-program>\n"
                "    <decrypt-option>%s</decrypt-option>\n"
                "  </encrypt>", dp->srv_encrypt, dp->srv_decrypt_opt
            );
            g_ptr_array_add(array, tmp);
	}
	break;
    }

    g_ptr_array_add(array, g_strdup_printf("  <record>%s</record>",
        (dp->record) ? "YES" : "NO"));

    if(dp->index)
        g_ptr_array_add(array, g_strdup("  <index>YES</index>"));

    if (dp->kencrypt)
        g_ptr_array_add(array, g_strdup("  <kencrypt>YES</kencrypt>"));

    if (am_has_feature(their_features, fe_xml_data_path)) {
	switch(dp->data_path) {
	case DATA_PATH_AMANDA:
            g_ptr_array_add(array, g_strdup("  <datapath>AMANDA</datapath>"));
	    break;
	case DATA_PATH_DIRECTTCP:
	  /* dp->dataport_list is not set for selfcheck/sendsize */
	    if (am_has_feature(their_features, fe_xml_directtcp_list)) {
		char *s, *sc;
		char *value, *b64value;

                strbuf = g_string_new("  <datapath>DIRECTTCP");
		if (dp->dataport_list) {
		    s = sc = g_strdup(dp->dataport_list);
		    do {
			value = s;
			s = strchr(s, ';');
			if (s)
			    *s++ = '\0';

			b64value = amxml_format_tag("directtcp", value);
                        g_string_append_printf(strbuf, "\n    %s", b64value);
			g_free(b64value);
		    } while (s);
		    g_free(sc);
                    g_string_append(strbuf, "\n  ");
		}
                g_string_append(strbuf, "</datapath>");
                tmp = g_string_free(strbuf, FALSE);
                g_ptr_array_add(array, tmp);
	    }
	  break;
	}
    }

    if (dp->exclude_file || dp->exclude_list) {
        strbuf = g_string_new("  <exclude>\n");

        if (dp->exclude_file && dp->exclude_file->nb_element > 0)
            for (excl = dp->exclude_file->first; excl; excl = excl->next) {
                tmp = amxml_format_tag("file", excl->name);
                g_string_append_printf(strbuf, "    %s\n", tmp);
                g_free(tmp);
            }

        if (dp->exclude_list && dp->exclude_list->nb_element > 0)
            for (excl = dp->exclude_list->first; excl; excl = excl->next) {
                tmp = amxml_format_tag("list", excl->name);
                g_string_append_printf(strbuf, "    %s\n", tmp);
                g_free(tmp);
            }

        if (dp->exclude_optional)
            g_string_append(strbuf, "    <optional>YES</optional>\n");

        g_string_append(strbuf, "  </exclude>");
        tmp = g_string_free(strbuf, FALSE);
        g_ptr_array_add(array, tmp);
    }

    if (dp->include_file || dp->include_list) {
        strbuf = g_string_new("  <include>\n");

        if (dp->include_file && dp->include_file->nb_element > 0)
            for (excl = dp->include_file->first; excl; excl = excl->next) {
                tmp = amxml_format_tag("file", excl->name);
                g_string_append_printf(strbuf, "    %s\n", tmp);
                g_free(tmp);
            }

        if (dp->include_list && dp->include_list->nb_element > 0)
            for (excl = dp->include_list->first; excl; excl = excl->next) {
                tmp = amxml_format_tag("list", excl->name);
                g_string_append_printf(strbuf, "    %s\n", tmp);
                g_free(tmp);
            }

        if (dp->include_optional)
            g_string_append(strbuf, "    <optional>YES</optional>\n");

        g_string_append(strbuf, "  </include>");
        tmp = g_string_free(strbuf, FALSE);
        g_ptr_array_add(array, tmp);
    }

    g_ptr_array_add(array, xml_scripts(dp->pp_scriptlist, their_features));
    g_ptr_array_add(array, NULL);

    strings = (gchar **)g_ptr_array_free(array, FALSE);
    result = g_strjoinv("\n", strings);
    g_strfreev(strings);

    return result;
}

char *
xml_dumptype_properties(
    disk_t *dp)
{
    xml_app_t xml_dumptype;

    xml_dumptype.result = g_strdup("");
    xml_dumptype.features = NULL;
    if (dp && dp->config) {
	g_hash_table_foreach(dumptype_get_property(dp->config), xml_property,
			     &xml_dumptype);
    }
    return xml_dumptype.result;
}

char *
xml_estimate_disk(
    char          *hostname,
    char          *diskname,
    am_feature_t  *their_features)
{
    disk_t        *dp  = lookup_disk(hostname, diskname);

    return xml_estimate(dp->estimatelist, their_features);
}

char *
xml_estimate(
    estimatelist_t estimatelist,
    am_feature_t *their_features)
{
    estimatelist_t el;
    GString *strbuf = g_string_new(NULL);
    char *p;

    if (am_has_feature(their_features, fe_xml_estimatelist)) {
	g_string_append(strbuf, "  <estimate>");
	for (el=estimatelist; el != NULL; el = el->next) {
            p = NULL;
	    switch (GPOINTER_TO_INT(el->data)) {
	    case ES_CLIENT:
                p = "CLIENT ";
                break;
	    case ES_SERVER:
                p = "SERVER ";
                break;
	    case ES_CALCSIZE:
                p = "CALCSIZE ";
                break;
	    }
            /* Can p ever be NULL at this point? */
            if (p)
                g_string_append(strbuf, p);
	}
        g_string_append(strbuf, "</estimate>");
    } else { /* add the first estimate only */
	if (am_has_feature(their_features, fe_xml_estimate)) {
            p = NULL;
	    g_string_append(strbuf, "  <estimate>");
	    switch (GPOINTER_TO_INT(estimatelist->data)) {
	    case ES_CLIENT:
                p = "CLIENT";
                break;
	    case ES_SERVER:
                p = "SERVER";
                break;
	    case ES_CALCSIZE:
                p = "CALCSIZE";
                break;
	    }
            /* Can p ever be NULL at this point? */
            if (p)
                g_string_append_printf(strbuf, "%s</estimate>", p);
	}
	if (GPOINTER_TO_INT(estimatelist->data) == ES_CALCSIZE)
            g_string_append(strbuf, "  <calcsize>YES</calcsize>");
    }

    return g_string_free(strbuf, FALSE);
}

char *
clean_dle_str_for_client(
    char *dle_str,
    am_feature_t *their_features)
{
    char *rval_dle_str;
    char *hack1, *hack2;
    char *pend, *pscript, *pproperty, *eproperty;
    int len;

    if (!dle_str)
	return NULL;

    rval_dle_str = g_strdup(dle_str);

    /* Remove everything between "  <encrypt>SERVER-CUSTOM" and "</encrypt>\n"
     */
#define SC "</encrypt>\n"
#define SC_LEN strlen(SC)
    hack1 = strstr(rval_dle_str, "  <encrypt>SERVER-CUSTOM");
    if (hack1) {
	hack2 = strstr(hack1, SC);
	/* +1 is to also move the trailing '\0' */
	memmove(hack1, hack2 + SC_LEN, strlen(hack2 + SC_LEN) + 1);
    }
#undef SC
#undef SC_LEN

    if (!am_has_feature(their_features, fe_dumptype_property)) {
#define SC "</property>\n"
#define SC_LEN strlen(SC)
	/* remove all dle properties, they are before backup-program or script
	   properties */
	hack1 = rval_dle_str;
	pend = strstr(rval_dle_str, "<backup-program>");
	pscript = strstr(rval_dle_str, "<script>");
	if (pscript && pscript < pend)
	    pend = pscript;
	if (!pend) /* the complete string */
	    pend = rval_dle_str + strlen(rval_dle_str);
	while (hack1) {
	    pproperty = strstr(hack1, "    <property>");
	    if (pproperty && pproperty < pend) { /* remove it */
		eproperty = strstr(pproperty, SC);
		len = eproperty + SC_LEN - pproperty;
		memmove(pproperty, eproperty + SC_LEN, strlen(eproperty + SC_LEN) + 1);
		pend  -= len;
		hack1 = pproperty;
	    } else {
		hack1 = NULL;
	    }
	}
    }
#undef SC
#undef SC_LEN

    return rval_dle_str;
}

char *
xml_application_disk(
    char          *hostname,
    char          *diskname,
    am_feature_t  *their_features)
{
    disk_t        *dp  = lookup_disk(hostname, diskname);
    application_t *app = lookup_application(dp->application);

    return xml_application(dp, app, their_features);
}

char *
xml_application(
    disk_t        *dp G_GNUC_UNUSED,
    application_t *application,
    am_feature_t  *their_features)
{
    char       *tmp;
    char       *p;
    xml_app_t   xml_app;
    proplist_t  proplist;
    GString    *strbuf;

    xml_app.features = their_features;

    p = application_get_plugin(application);
    tmp = amxml_format_tag("plugin", p);

    xml_app.result = g_strdup_printf("  <backup-program>\n    %s\n", tmp);
    g_free(tmp);

    /*
     * Unfortunately, the g_hash_table_foreach() below modifies xml_app.result
     * :/ This is why we have to delay allocation of strbuf until after it
     * completes.
     */
    proplist = application_get_property(application);
    g_hash_table_foreach(proplist, xml_property, &xml_app);

    strbuf = g_string_new(xml_app.result);
    g_free(xml_app.result);

    p = application_get_client_name(application);
    if (p && strlen(p) > 0 &&
	am_has_feature(their_features, fe_application_client_name)) {
	tmp = amxml_format_tag("client_name", p);
        g_string_append_printf(strbuf, "    %s\n", tmp);
	g_free(tmp);
    }

    g_string_append(strbuf, "  </backup-program>\n");

    xml_app.result = g_string_free(strbuf, FALSE);

    return xml_app.result;
}

 
char *
xml_scripts(
    identlist_t pp_scriptlist,
    am_feature_t  *their_features)
{
    char        *client_name;
    execute_on_t execute_on;
    int          execute_where;
    proplist_t   proplist;
    identlist_t  pp_iter;
    pp_script_t *pp_script;
    xml_app_t    xml_app;
    GString     *strbuf = g_string_new(NULL);
    GString     *tmpbuf;
    char        *tmp;

    xml_app.features = their_features;

    for (pp_iter = pp_scriptlist; pp_iter; pp_iter = pp_iter->next) {
	char *pp_script_name = pp_iter->data;
	pp_script = lookup_pp_script(pp_script_name);
	g_assert(pp_script != NULL);

        execute_where = pp_script_get_execute_where(pp_script);
        execute_on = pp_script_get_execute_on(pp_script);
        proplist = pp_script_get_property(pp_script);
        client_name = pp_script_get_client_name(pp_script);

        g_string_append(strbuf, "  <script>\n");

        tmp = amxml_format_tag("plugin", pp_script_get_plugin(pp_script));
        g_string_append_printf(strbuf, "    %s\n", tmp);
        g_free(tmp);


        g_string_append(strbuf, "    <execute_where>");

	switch (execute_where) {
            case EXECUTE_WHERE_CLIENT:
                g_string_append(strbuf, "CLIENT");
                break;
            case EXECUTE_WHERE_SERVER:
                g_string_append(strbuf, "SERVER");
                break;
	}
        g_string_append(strbuf, "</execute_where>\n");

        if (execute_on) {
            tmp = execute_on_to_string(execute_on, ",");
            g_string_append_printf(strbuf, "    <execute_on>%s</execute_on>\n",
                tmp);
            g_free(tmp);
        }

        /*
         * Unfortunately, the g_hash_table_foreach() invocation _modifies_
         * xmlapp.result :/ We have no choice but to do that.
         */

	xml_app.result = g_strdup("");
	g_hash_table_foreach(proplist, xml_property, &xml_app);
        tmpbuf = g_string_new(xml_app.result);
        g_free(xml_app.result);

        if (client_name && strlen(client_name) > 0
            && am_has_feature(their_features, fe_script_client_name)) {
            tmp = amxml_format_tag("client_name", client_name);
            g_string_append_printf(tmpbuf, "    %s\n", tmp);
            g_free(tmp);
        }

        tmp = g_string_free(tmpbuf, FALSE);
        g_string_append_printf(strbuf, "%s  </script>\n", tmp);
        g_free(tmp);
    }
    return g_string_free(strbuf, FALSE);
}


void
disable_skip_disk(
    disklist_t *origqp)
{
    GList  *dlist;
    disk_t *dp;

    for (dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	if (dp->ignore || dp->strategy == DS_SKIP)
	    dp->todo = 0;
    }
}


GPtrArray *
match_disklist(
    disklist_t *origqp,
    gboolean    exact_match,
    int		sargc,
    char **	sargv)
{
    char *prevhost = NULL;
    GPtrArray *err_array = g_ptr_array_new();
    int i;
    int match_a_host;
    int match_a_disk;
    int prev_match;
    GList  *dlist;
    disk_t *dp_skip;
    disk_t *dp;
    char **new_sargv = NULL;

    if (sargc <= 0)
	return err_array;

    if (exact_match) {
	new_sargv = g_new0(char *, sargc+1);
	for (i=0; i<sargc; i++) {
	    if (*sargv[i] == '=') {
		new_sargv[i] = g_strdup(sargv[i]);
	    } else {
		new_sargv[i] = g_strconcat("=", sargv[i], NULL);
	    }
	}
	sargv = new_sargv;
    }

    for(dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	if(dp->todo == 1)
	    dp->todo = -1;
    }

    prev_match = 0;
    for (i = 0; i < sargc; i++) {
	match_a_host = 0;
	for(dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
	    dp = dlist->data;
	    if (match_host(sargv[i], dp->host->hostname))
		match_a_host = 1;
	}
	match_a_disk = 0;
	dp_skip = NULL;
	for(dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
	    dp = dlist->data;
	    if (prevhost != NULL &&
	        match_host(prevhost, dp->host->hostname) &&
	        (match_disk(sargv[i], dp->name) ||
		(dp->device && match_disk(sargv[i], dp->device)))) {
		if (match_a_host) {
		    error(_("Argument %s cannot be both a host and a disk"), sargv[i]);
		    /*NOTREACHED*/
		}
		else {
		    if (dp->todo == -1) {
			dp->todo = 1;
			match_a_disk = 1;
			prev_match = 0;
		    } else if (dp->todo == 0) {
			match_a_disk = 1;
			prev_match = 0;
			dp_skip = dp;
		    } else { /* dp->todo == 1 */
			match_a_disk = 1;
			prev_match = 0;
		    }
		}
	    }
	}
	if (!match_a_disk) {
	    if (match_a_host == 1) {
		if (prev_match == 1) { /* all disk of the previous host */
		    for(dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
			dp = dlist->data;
			if (match_host(prevhost,dp->host->hostname))
			    if (dp->todo == -1) {
				dp->todo = 1;
				match_a_disk = 1;
			    }
		    }
		    if (!match_a_disk)
			g_ptr_array_add(err_array, g_strdup_printf("All disks on host '%s' are ignored or have strategy \"skip\".", prevhost));
		}
		prevhost = sargv[i];
		prev_match = 1;
	    }
	    else {
                g_ptr_array_add(err_array, g_strdup_printf("Argument '%s' matches neither a host nor a disk%s",
                    sargv[i], (strchr(sargv[i], '\\')) ? "; quoting may be incorrect." : "."));
		prev_match = 0;
	    }
	} else if (dp_skip) {
		g_ptr_array_add(err_array, g_strdup_printf("Argument '%s' matches a disk %s.",
                   sargv[i], (dp_skip->strategy == DS_SKIP) ? "with strategy \"skip\"" : "marked \"ignore\""));
		prev_match = 0;
	}
    }

    if (prev_match == 1) { /* all disk of the previous host */
	match_a_disk = 0;
	for(dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
	    dp = dlist->data;
	    if (match_host(prevhost,dp->host->hostname))
		if (dp->todo == -1) {
		    dp->todo = 1;
		    match_a_disk = 1;
		}
	}
        if (!match_a_disk)
            g_ptr_array_add(err_array, g_strdup_printf("All disks on host '%s' are ignored or have strategy \"skip\".", prevhost));
    }

    for(dlist = origqp->head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	if (dp->todo == -1)
	    dp->todo = 0;
    }

    if (new_sargv) {
	for (i=0; i<sargc; i++)
	    g_free(new_sargv[i]);
	g_free(new_sargv);
    }
    return err_array;
}

gboolean
match_dumpfile(
    dumpfile_t  *file,
    gboolean	exact_match,
    int		sargc,
    char **	sargv)
{
    disk_t d;
    am_host_t h;
    disklist_t dl;
    GPtrArray *err_array;
    guint i;

    /* rather than try to reproduce the adaptive matching logic in
     * match_disklist, this simply creates a new, fake disklist with one
     * element in it, and calls match_disklist directly */

    bzero(&h, sizeof(h));
    h.hostname = file->name;
    h.disks = &d;

    bzero(&d, sizeof(d));
    d.host = &h;
    d.hostname = file->name;
    d.name = file->disk;
    d.device = file->disk;
    d.todo = 1;

    dl.head = dl.tail = g_list_prepend(NULL, &d);

    err_array = match_disklist(&dl, exact_match, sargc, sargv);
    if (err_array->len > 0) {
	for (i = 0; i < err_array->len; i++) {
	    char *errstr = g_ptr_array_index(err_array, i);
	    g_debug("%s", errstr);
	}
    }
    g_ptr_array_free(err_array, TRUE);
    if (g_list_delete_link(dl.head, dl.head) != NULL) {
    };
    return d.todo;
}

netif_t *
disklist_netifs(void)
{
    return all_netifs;
}

#ifdef TEST

static void dump_disk(const disk_t *);
static void dump_disklist(const disklist_t *);
int main(int, char *[]);

static void
dump_disk(
    const disk_t *	dp)
{
    g_printf(_("  DISK %s (HOST %s, LINE %d) TYPE %s NAME %s SPINDLE %d\n"),
	   dp->name, dp->host->hostname, dp->line, dp->dtype_name,
	   dp->name == NULL? "(null)": dp->name,
	   dp->spindle);
}

static void
dump_disklist(
    const disklist_t *	lst)
{
    GList *dlist, *prev;
    const disk_t *dp;
    const am_host_t *hp;

    if(hostlist == NULL) {
	g_printf(_("DISKLIST not read in\n"));
	return;
    }

    g_printf(_("DISKLIST BY HOSTNAME:\n"));

    for(hp = hostlist; hp != NULL; hp = hp->next) {
	char *if_name = NULL;
	if (hp->netif && hp->netif->config)
	    if_name = interface_name(hp->netif->config);

	g_printf(_("HOST %s INTERFACE %s\n"),
	       hp->hostname,
	       if_name ? _("(null)") : if_name);
	for(dp = hp->disks; dp != NULL; dp = dp->hostnext)
	    dump_disk(dp);
	putchar('\n');
    }


    g_printf(_("DISKLIST IN FILE ORDER:\n"));

    prev = NULL;
    for(dlist = lst->head; dlist != NULL; prev = dlist, dlist = dlist->next) {
	dp = get_disk(dlist);
	dump_disk(dp);
	/* check pointers */
	if(dlist->prev != prev) g_printf(_("*** prev pointer mismatch!\n"));
	if(dlist->next == NULL && lst->tail != dlist) g_printf(_("tail mismatch!\n"));
    }
}

int
main(
    int		argc,
    char **	argv)
{
  char *conf_diskfile;
  disklist_t lst;
  int result;

  /*
   * Configure program for internationalization:
   *   1) Only set the message locale for now.
   *   2) Set textdomain for all amanda related programs to "amanda"
   *      We don't want to be forced to support dozens of message catalogs.
   */  
  setlocale(LC_MESSAGES, "C");
  textdomain("amanda"); 

  safe_fd(-1, 0);

  set_pname("diskfile");

  dbopen(DBG_SUBDIR_SERVER);

  /* Don't die when child closes pipe */
  signal(SIGPIPE, SIG_IGN);

  if (argc>1) {
    config_init_with_global(CONFIG_INIT_EXPLICIT_NAME, argv[1]);
  } else {
    config_init_with_global(CONFIG_INIT_USE_CWD, NULL);
  }

  if (config_errors(NULL) >= CFGERR_WARNINGS) {
    config_print_errors();
    if (config_errors(NULL) >= CFGERR_ERRORS) {
      g_critical(_("errors processing config file"));
    }
  }

  conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
  result = read_diskfile(conf_diskfile, &lst);
  if(result == CFGERR_OK) {
    dump_disklist(&lst);
  } else {
    config_print_errors();
  }
  amfree(conf_diskfile);

  return result;
}
#endif /* TEST */
