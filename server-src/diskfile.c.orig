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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: diskfile.c,v 1.93 2006/07/22 23:06:26 martinea Exp $
 *
 * read disklist file
 */
#include "amanda.h"
#include "arglist.h"
#include "conffile.h"
#include "diskfile.h"
#include "util.h"

static am_host_t *hostlist;

/* local functions */
static char *upcase(char *st);
static int parse_diskline(disklist_t *, const char *, FILE *, int *, char **);
static void disk_parserror(const char *, int, const char *, ...)
    __attribute__ ((format (printf, 3, 4)));


int
read_diskfile(
    const char *filename,
    disklist_t *lst)
{
    FILE *diskf;
    int line_num;
    char *line;

    /* initialize */
    hostlist = NULL;
    lst->head = lst->tail = NULL;
    line_num = 0;

    if ((diskf = fopen(filename, "r")) == NULL) {
	return -1;
        /*NOTREACHED*/
    }

    while ((line = agets(diskf)) != NULL) {
	line_num++;
	if (line[0] != '\0') {
	    if (parse_diskline(lst, filename, diskf, &line_num, &line) < 0) {
		amfree(line);
		afclose(diskf);
		return (-1);
	    }
	}
	amfree(line);
    }

    afclose(diskf);
    return (0);
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
	if (strcmp(disk->name, diskname) == 0)
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
    if(list->tail == NULL) list->head = disk;
    else list->tail->next = disk;
    disk->prev = list->tail;

    list->tail = disk;
    disk->next = NULL;
}


/*
 * put disk on head of queue
 */

void
headqueue_disk(
    disklist_t *list,
    disk_t *	disk)
{
    if(list->head == NULL) list->tail = disk;
    else list->head->prev = disk;
    disk->next = list->head;

    list->head = disk;
    disk->prev = NULL;
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
    disk_t *prev, *ptr;

    prev = NULL;
    ptr = list->head;

    while(ptr != NULL) {
	if(cmp(disk, ptr) < 0) break;
	prev = ptr;
	ptr = ptr->next;
    }
    disk->next = ptr;
    disk->prev = prev;

    if(prev == NULL) list->head = disk;
    else prev->next = disk;
    if(ptr == NULL) list->tail = disk;
    else ptr->prev = disk;
}

disk_t *
add_disk(
    disklist_t *list,
    char *	hostname,
    char *	diskname)
{
    disk_t *disk;
    am_host_t *host;

    disk = alloc(SIZEOF(disk_t));
    disk->line = 0;
    disk->tape_splitsize = (off_t)0;
    disk->split_diskbuffer = NULL;
    disk->fallback_splitsize = (off_t)0;
    disk->name = stralloc(diskname);
    disk->device = stralloc(diskname);
    disk->spindle = -1;
    disk->up = NULL;
    disk->compress = COMP_NONE;
    disk->encrypt  = ENCRYPT_NONE;
    disk->start_t = 0;
    disk->todo = 1;
    disk->index = 1;
    disk->exclude_list = NULL;
    disk->exclude_file = NULL;
    disk->include_list = NULL;
    disk->include_file = NULL;

    host = lookup_host(hostname);
    if(host == NULL) {
	host = alloc(SIZEOF(am_host_t));
	host->next = hostlist;
	hostlist = host;

	host->hostname = stralloc(hostname);
	host->disks = NULL;
	host->inprogress = 0;
	host->maxdumps = 1;
	host->netif = NULL;
	host->start_t = 0;
	host->up = NULL;
	host->features = NULL;
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
    disk_t *t;

    t = list->head;
    while ((t != NULL) && (t != disk)) {
        t = t->next;
    }
    return (t == disk);
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

    out->head = (disk_t *)0;
    out->tail = (disk_t *)0;

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

    disk = list->head;
    list->head = disk->next;

    if(list->head == NULL) list->tail = NULL;
    else list->head->prev = NULL;

    disk->prev = disk->next = NULL;	/* for debugging */
    return disk;
}

void
remove_disk(
    disklist_t *list,
    disk_t *	disk)
{
    if(disk->prev == NULL) list->head = disk->next;
    else disk->prev->next = disk->next;

    if(disk->next == NULL) list->tail = disk->prev;
    else disk->next->prev = disk->prev;

    disk->prev = disk->next = NULL;
}

void
free_disklist(
    disklist_t* dl)
{
    disk_t    *dp;
    am_host_t *host, *hostnext;

    while (dl->head != NULL) {
	dp = dequeue_disk(dl);
	amfree(dp->name);
	free_sl(dp->exclude_file);
	free_sl(dp->exclude_list);
	free_sl(dp->include_file);
	free_sl(dp->include_list);
	free(dp);
    }

    for(host=hostlist; host != NULL; host = hostnext) {
	amfree(host->hostname);
	am_release_feature_set(host->features);
	host->features = NULL;
	hostnext = host->next;
	amfree(host);
    }
    hostlist=NULL;
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
    interface_t *netif = 0;
    char *hostname = NULL;
    char *diskname, *diskdevice;
    char *dumptype;
    char *s, *fp;
    int ch, dup = 0;
    char *line = *line_p;
    int line_num = *line_num_p;

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
    host = lookup_host(fp);
    if (host == NULL) {
      hostname = stralloc(fp);
      malloc_mark(hostname);
    } else {
      hostname = host->hostname;
    }

    skip_whitespace(s, ch);
    if(ch == '\0' || ch == '#') {
	disk_parserror(filename, line_num, "disk device name expected");
	amfree(hostname);
	return (-1);
    }

    fp = s - 1;
    skip_quoted_string(s, ch);
    s[-1] = '\0';
    diskname = unquote_string(fp);

    skip_whitespace(s, ch);
    if(ch == '\0' || ch == '#') {
	disk_parserror(filename, line_num, "disk dumptype expected");
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
    dumptype = unquote_string(fp);
    if(fp[0] != '{') {
	if ((dtype = lookup_dumptype(dumptype)) == NULL) {
	    diskdevice = dumptype;
	    skip_whitespace(s, ch);
	    if(ch == '\0' || ch == '#') {
		disk_parserror(filename, line_num,
			"disk dumptype '%s' not found", dumptype);
		amfree(hostname);
		amfree(diskdevice);
		amfree(diskname);
		return (-1);
	    }

	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    dumptype = unquote_string(fp);
	}
    }
    else
	amfree(dumptype);

    /* check for duplicate disk */
    if(host && (disk = lookup_disk(hostname, diskname)) != NULL) {
	disk_parserror(filename, line_num,
	    "duplicate disk record, previous on line %d", disk->line);
	dup = 1;
    } else {
	disk = alloc(SIZEOF(disk_t));
	malloc_mark(disk);
	disk->line = line_num;
	disk->name = diskname;
	disk->device = diskdevice;
	malloc_mark(disk->name);
	disk->spindle = -1;
	disk->up = NULL;
	disk->inprogress = 0;
    }

    if (fp[0] == '{') {
	s[-1] = (char)ch;
	s = fp+2;
	skip_whitespace(s, ch);
	if (ch != '\0' && ch != '#') {
	    disk_parserror(filename, line_num,
		      "expected line break after `{\', ignoring rest of line");
	}

	if (strchr(s-1, '}') &&
	    (strchr(s-1, '#') == NULL ||
	     strchr(s-1, '}') < strchr(s-1, '#'))) {
	    disk_parserror(filename, line_num,"'}' on same line than '{'");
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

	dtype = read_dumptype(vstralloc("custom(", hostname,
					":", disk->name, ")", 0),
			      diskf, (char*)filename, line_num_p);
	if (dtype == NULL || dup) {
	    disk_parserror(filename, line_num,
			   "read of custom dumptype failed");
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

	*line_p = line = agets(diskf);
	line_num = *line_num_p; /* no incr, read_dumptype did it already */

	if (line == NULL)
	    *line_p = line = stralloc("");
	s = line;
	ch = *s++;
    } else {
	if((dtype = lookup_dumptype(dumptype)) == NULL) {
	    char *qdt = quote_string(dumptype);

	    disk_parserror(filename, line_num, "undefined dumptype `%s'", qdt);
	    amfree(qdt);
	    amfree(dumptype);
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
    }

    if (dup) {
	amfree(hostname);
	amfree(diskdevice);
	amfree(diskname);
	return (-1);
    }

    disk->dtype_name	     = dtype->name;
    disk->program	     = dumptype_get_program(dtype);
    if(dumptype_get_exclude(dtype).type == 0) {
	disk->exclude_list   = duplicate_sl(dumptype_get_exclude(dtype).sl);
	disk->exclude_file   = NULL;
    }
    else {
	disk->exclude_file   = duplicate_sl(dumptype_get_exclude(dtype).sl);
	disk->exclude_list   = NULL;
    }
    disk->exclude_optional   = dumptype_get_exclude(dtype).optional;
    if(dumptype_get_include(dtype).type == 0) {
	disk->include_list   = duplicate_sl(dumptype_get_include(dtype).sl);
	disk->include_file   = NULL;
    }
    else {
	disk->include_file   = duplicate_sl(dumptype_get_include(dtype).sl);
	disk->include_list   = NULL;
    }
    disk->include_optional   = dumptype_get_include(dtype).optional;
    disk->priority	     = dumptype_get_priority(dtype);
    disk->dumpcycle	     = dumptype_get_dumpcycle(dtype);
/*    disk->frequency	     = dumptype_get_frequency(dtype);*/
    disk->security_driver    = dumptype_get_security_driver(dtype);
    disk->maxdumps	     = dumptype_get_maxdumps(dtype);
    disk->tape_splitsize     = dumptype_get_tape_splitsize(dtype);
    disk->split_diskbuffer   = dumptype_get_split_diskbuffer(dtype);
    disk->fallback_splitsize = dumptype_get_fallback_splitsize(dtype);
    disk->maxpromoteday	     = dumptype_get_maxpromoteday(dtype);
    disk->bumppercent	     = dumptype_get_bumppercent(dtype);
    disk->bumpsize	     = dumptype_get_bumpsize(dtype);
    disk->bumpdays	     = dumptype_get_bumpdays(dtype);
    disk->bumpmult	     = dumptype_get_bumpmult(dtype);
    disk->start_t	     = dumptype_get_start_t(dtype);
    disk->strategy	     = dumptype_get_strategy(dtype);
    disk->estimate	     = dumptype_get_estimate(dtype);
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
    disk->ssh_keys           = dumptype_get_ssh_keys(dtype);
    disk->comprate[0]	     = dumptype_get_comprate(dtype)[0];
    disk->comprate[1]	     = dumptype_get_comprate(dtype)[1];

    /*
     * Boolean parameters with no value (Appears here as value 2) defaults
     * to TRUE for backward compatibility and for logical consistency.
     */
    disk->record	     = dumptype_get_record(dtype) != 0;
    disk->skip_incr	     = dumptype_get_skip_incr(dtype) != 0;
    disk->skip_full	     = dumptype_get_skip_full(dtype) != 0;
    disk->to_holdingdisk     = dumptype_get_to_holdingdisk(dtype) != 0;
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
	    disk_parserror(filename, line_num, "non-integer spindle `%s'", fp);
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
	if((netif = lookup_interface(upcase(fp))) == NULL) {
	    disk_parserror(filename, line_num,
		"undefined network interface `%s'", fp);
	    amfree(hostname);
	    amfree(disk->name);
	    amfree(disk);
	    return (-1);
	}
    } else {
	netif = lookup_interface("default");
    }

    skip_whitespace(s, ch);
    if(ch && ch != '#') {		/* now we have garbage, ignore it */
	disk_parserror(filename, line_num, "end of line expected");
    }

    if(dumptype_get_ignore(dtype) || dumptype_get_strategy(dtype) == DS_SKIP) {
	amfree(hostname);
	amfree(disk->name);
	amfree(disk);
	return (0);
    }

    /* success, add disk to lists */

    if(host == NULL) {			/* new host */
	host = alloc(SIZEOF(am_host_t));
	malloc_mark(host);
	host->next = hostlist;
	hostlist = host;

	host->hostname = hostname;
	hostname = NULL;
	host->disks = NULL;
	host->inprogress = 0;
	host->maxdumps = 1;		/* will be overwritten */
	host->netif = NULL;
	host->start_t = 0;
	host->up = NULL;
	host->features = NULL;
    }

    host->netif = netif;

    enqueue_disk(lst, disk);

    disk->host = host;
    disk->hostnext = host->disks;
    host->disks = disk;
    host->maxdumps = disk->maxdumps;

    return (0);
}


printf_arglist_function2(void disk_parserror, const char *, filename,
    int, line_num, const char *, format)
{
    va_list argp;

    /* print error message */

    fprintf(stderr, "\"%s\", line %d: ", filename, line_num);
    arglist_start(argp, format);
    vfprintf(stderr, format, argp);
    arglist_end(argp);
    fputc('\n', stderr);
}


void
dump_queue(
    char *	st,
    disklist_t	q,
    int		npr,	/* we print first npr disks on queue, plus last two */
    FILE *	f)
{
    disk_t *d,*p;
    int pos;
    char *qname;

    if(empty(q)) {
	fprintf(f, "%s QUEUE: empty\n", st);
	return;
    }
    fprintf(f, "%s QUEUE:\n", st);
    for(pos = 0, d = q.head, p = NULL; d != NULL; p = d, d = d->next, pos++) {
	qname = quote_string(d->name);
	if(pos < npr) fprintf(f, "%3d: %-10s %-4s\n",
			      pos, d->host->hostname, qname);
	amfree(qname);
    }
    if(pos > npr) {
	if(pos > npr+2) fprintf(f, "  ...\n");
	if(pos > npr+1) {
	    d = p->prev;
	    fprintf(f, "%3d: %-10s %-4s\n", pos-2, d->host->hostname, d->name);
	}
	d = p;
	fprintf(f, "%3d: %-10s %-4s\n", pos-1, d->host->hostname, d->name);
    }
}

char *
optionstr(
    disk_t *		dp,
    am_feature_t *	their_features,
    FILE *		fdout)
{
    char *auth_opt = NULL;
    char *kencrypt_opt = "";
    char *compress_opt = "";
    char *encrypt_opt = stralloc("");
    char *decrypt_opt = stralloc("");
    char *record_opt = "";
    char *index_opt = "";
    char *exclude_file = NULL;
    char *exclude_list = NULL;
    char *include_file = NULL;
    char *include_list = NULL;
    char *excl_opt = "";
    char *incl_opt = "";
    char *exc = NULL;
    char *result = NULL;
    sle_t *excl;
    int nb_exclude_file;
    int nb_include_file;
    char *qdpname;
    char *qname;
    int err=0;

    assert(dp != NULL);
    assert(dp->host != NULL);

    qdpname = quote_string(dp->name);
    if(am_has_feature(dp->host->features, fe_options_auth)) {
	auth_opt = vstralloc("auth=", dp->security_driver, ";", NULL);
    } else if(strcasecmp(dp->security_driver, "bsd") == 0) {
	if(am_has_feature(dp->host->features, fe_options_bsd_auth))
	    auth_opt = stralloc("bsd-auth;");
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support auth or bsd-auth\n",
		    dp->host->hostname, qdpname);
	}
    } else if(strcasecmp(dp->security_driver, "krb4") == 0) {
	if(am_has_feature(dp->host->features, fe_options_krb4_auth))
	    auth_opt = stralloc("krb4-auth;");
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support auth or krb4-auth\n",
		    dp->host->hostname, qdpname);
	}
	if(dp->kencrypt) {
	    if(am_has_feature(dp->host->features, fe_options_kencrypt)) {
		kencrypt_opt = "kencrypt;";
	    }
	    else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support kencrypt\n",
		    dp->host->hostname, qdpname);
	    }
	}
    }

    switch(dp->compress) {
    case COMP_FAST:
	if(am_has_feature(their_features, fe_options_compress_fast)) {
	    compress_opt = "compress-fast;";
	}
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support fast compression\n",
		    dp->host->hostname, qdpname);
	}
	break;
    case COMP_BEST:
	if(am_has_feature(their_features, fe_options_compress_best)) {
	    compress_opt = "compress-best;";
	}
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support best compression\n",
		    dp->host->hostname, qdpname);
	}
	break;
    case COMP_CUST:
        if(am_has_feature(their_features, fe_options_compress_cust)) {
	  compress_opt = vstralloc("comp-cust=", dp->clntcompprog, ";", NULL);
	  if (BSTRNCMP(compress_opt, "comp-cust=;") == 0){
	    if(fdout) {
	      fprintf(fdout,
		      "ERROR: %s:%s client custom compression with no compression program specified\n",
		      dp->host->hostname, qdpname);
	    }
	    err++;
	  }
	}
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support client custom compression\n",
		    dp->host->hostname, qdpname);
	}
	break;
    case COMP_SERV_FAST:
	if(am_has_feature(their_features, fe_options_srvcomp_fast)) {
	    compress_opt = "srvcomp-fast;";
	}
	break;
    case COMP_SERV_BEST:
	if(am_has_feature(their_features, fe_options_srvcomp_best)) {
            compress_opt = "srvcomp-best;";
	}
	break;
    case COMP_SERV_CUST:
        if(am_has_feature(their_features, fe_options_srvcomp_cust)) {
	  compress_opt = vstralloc("srvcomp-cust=", dp->srvcompprog, ";", NULL);
	  if (BSTRNCMP(compress_opt, "srvcomp-cust=;") == 0){
	    if(fdout) {
	      fprintf(fdout,
		      "ERROR: %s:%s server custom compression with no compression program specified\n",
		      dp->host->hostname, qdpname);
	    }
	    err++;
	  }
	}
	else if(fdout) {
	  fprintf(fdout,
		  "WARNING: %s:%s does not support server custom compression\n",
		  dp->host->hostname, qdpname);
	}
	break;
    }

    switch(dp->encrypt) {
    case ENCRYPT_CUST:
      if(am_has_feature(their_features, fe_options_encrypt_cust)) {
	 encrypt_opt = newvstralloc(encrypt_opt, "encrypt-cust=",
				    dp->clnt_encrypt, ";", NULL);
	 if (BSTRNCMP(encrypt_opt, "encrypt-cust=;") == 0) {
	    if(fdout) {
	      fprintf(fdout,
		      "ERROR: %s:%s encrypt client with no encryption program specified\n",
		      dp->host->hostname, qdpname);
	    }
	    err++;
	  }
	 if ( dp->compress == COMP_SERV_FAST || 
	      dp->compress == COMP_SERV_BEST ||
	      dp->compress == COMP_SERV_CUST ) {
	   if(fdout) {
	      fprintf(fdout,
		      "ERROR: %s:Client encryption with server compression is not supported. See amanda.conf(5) for detail.\n", dp->host->hostname);
	    }
	    err++;
	  }
	 if(dp->clnt_decrypt_opt) {
	   if(am_has_feature(their_features, fe_options_client_decrypt_option)) {
	     decrypt_opt = newvstralloc(decrypt_opt, "client-decrypt-option=",
					dp->clnt_decrypt_opt, ";", NULL);
	   }
	   else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support client decrypt option\n",
		    dp->host->hostname, qdpname);
	   }
	 }
      }
      else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support client data encryption\n",
		    dp->host->hostname, qdpname);
     }
	 break;
    case ENCRYPT_SERV_CUST:
      if(am_has_feature(their_features, fe_options_encrypt_serv_cust)) {
	 encrypt_opt = newvstralloc(encrypt_opt, "encrypt-serv-cust=",
				    dp->srv_encrypt, ";", NULL);
	 if (BSTRNCMP(encrypt_opt, "encrypt-serv-cust=;") == 0){
	    if(fdout) {
	      fprintf(fdout,
		      "ERROR: %s:%s encrypt server with no encryption program specified\n",
		      dp->host->hostname, qdpname);
	    }
	    err++;
	  }
	 if(dp->srv_decrypt_opt) {
	   if(am_has_feature(their_features, fe_options_server_decrypt_option)) {
	     decrypt_opt = newvstralloc(decrypt_opt, "server-decrypt-option=",
					dp->srv_decrypt_opt, ";", NULL);
	   }
	   else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support server decrypt option\n",
		    dp->host->hostname, qdpname);
	   }
	 }
      }
      else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support server data encryption\n",
		    dp->host->hostname, qdpname);
      }
	 break;
    }
    
    if(!dp->record) {
	if(am_has_feature(their_features, fe_options_no_record)) {
	    record_opt = "no-record;";
	}
	else if(fdout) {
	    fprintf(fdout, "WARNING: %s:%s does not support no record\n",
		    dp->host->hostname, qdpname);
	}
    }

    if(dp->index) {
	if(am_has_feature(their_features, fe_options_index)) {
	    index_opt = "index;";
	}
	else if(fdout) {
	    fprintf(fdout, "WARNING: %s:%s does not support index\n",
		    dp->host->hostname, qdpname);
	}
    }

    if(dp->kencrypt) kencrypt_opt = "kencrypt;";


    exclude_file = stralloc("");
    nb_exclude_file = 0;
    if(dp->exclude_file != NULL && dp->exclude_file->nb_element > 0) {
	nb_exclude_file = dp->exclude_file->nb_element;
	if(am_has_feature(their_features, fe_options_exclude_file)) {
	    if(am_has_feature(their_features, fe_options_multiple_exclude) ||
	       dp->exclude_file->nb_element == 1) {
		for(excl = dp->exclude_file->first; excl != NULL;
						    excl = excl->next) {
		    qname = quote_string(excl->name);
		    exc = newvstralloc( exc, "exclude-file=", qname, ";", NULL);
		    strappend(exclude_file, exc);
		    amfree(qname);
		}
	    } else {
		qname = quote_string(dp->exclude_file->last->name);
		exc = newvstralloc(exc, "exclude-file=", qname, ";", NULL);
		strappend(exclude_file, exc);
		if(fdout) {
		    fprintf(fdout,
		       "WARNING: %s:%s does not support multiple exclude\n",
		       dp->host->hostname, qdpname);
		}
		amfree(qname);
	    }
	} else if(fdout) {
	    fprintf(fdout, "WARNING: %s:%s does not support exclude file\n",
		    dp->host->hostname, qdpname);
	}
    }
    exclude_list = stralloc("");
    if(dp->exclude_list != NULL && dp->exclude_list->nb_element > 0) {
	if(am_has_feature(their_features, fe_options_exclude_list)) {
	    if(am_has_feature(their_features, fe_options_multiple_exclude) ||
	       (dp->exclude_list->nb_element == 1 && nb_exclude_file == 0)) {
		for(excl = dp->exclude_list->first; excl != NULL;
						    excl = excl->next) {
		    qname = quote_string(excl->name);
		    exc = newvstralloc( exc, "exclude-list=", qname, ";", NULL);
		    strappend(exclude_list, exc);
		    amfree(qname);
		}
	    } else {
		qname = quote_string(dp->exclude_list->last->name);
		exc = newvstralloc(exc, "exclude-list=", qname, ";", NULL);
		strappend(exclude_list, exc);
		if(fdout) {
			fprintf(fdout,
			 "WARNING: %s:%s does not support multiple exclude\n",
			 dp->host->hostname, qdpname);
		}
		amfree(qname);
	    }
	} else if(fdout) {
	    fprintf(fdout, "WARNING: %s:%s does not support exclude list\n",
		    dp->host->hostname, qdpname);
	}
    }

    include_file = stralloc("");
    nb_include_file = 0;
    if(dp->include_file != NULL && dp->include_file->nb_element > 0) {
	nb_include_file = dp->include_file->nb_element;
	if(am_has_feature(their_features, fe_options_include_file)) {
	    if(am_has_feature(their_features, fe_options_multiple_include) ||
	       dp->include_file->nb_element == 1) {
		for(excl = dp->include_file->first; excl != NULL;
						    excl = excl->next) {
		    qname = quote_string(excl->name);
		    exc = newvstralloc(exc, "include-file=", qname, ";", NULL);
		    strappend(include_file, exc);
		    amfree(qname);
		}
	    } else {
		qname = quote_string(dp->include_file->last->name);
		exc = newvstralloc(exc, "include-file=", qname, ";", NULL);
		strappend(include_file, exc);
		if(fdout) {
		    fprintf(fdout,
			 "WARNING: %s:%s does not support multiple include\n",
			 dp->host->hostname, qdpname);
		}
		amfree(qname);
	    }
	} else if(fdout) {
	    fprintf(fdout, "WARNING: %s:%s does not support include file\n",
		    dp->host->hostname, qdpname);
	}
    }
    include_list = stralloc("");
    if(dp->include_list != NULL && dp->include_list->nb_element > 0) {
	if(am_has_feature(their_features, fe_options_include_list)) {
	    if(am_has_feature(their_features, fe_options_multiple_include) ||
	       (dp->include_list->nb_element == 1 && nb_include_file == 0)) {
		for(excl = dp->include_list->first; excl != NULL;
						    excl = excl->next) {
		    qname = quote_string(excl->name);
		    exc = newvstralloc(exc, "include-list=", qname, ";", NULL);
		    strappend(include_list, exc);
		    amfree(qname);
		}
	    } else {
		qname = quote_string(dp->include_list->last->name);
		exc = newvstralloc(exc, "include-list=", qname, ";", NULL);
		strappend(include_list, exc);
		if(fdout) {
			fprintf(fdout,
			 "WARNING: %s:%s does not support multiple include\n",
			 dp->host->hostname, qdpname);
		}
		amfree(qname);
	    }
	} else if(fdout) {
	    fprintf(fdout, "WARNING: %s:%s does not support include list\n",
		    dp->host->hostname, qdpname);
	}
    }

    if(dp->exclude_optional) {
	if(am_has_feature(their_features, fe_options_optional_exclude)) {
	    excl_opt = "exclude-optional;";
	}
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support optional exclude\n",
		    dp->host->hostname, qdpname);
	}
    }
    if(dp->include_optional) {
	if(am_has_feature(their_features, fe_options_optional_include)) {
	   incl_opt = "include-optional;";
	}
	else if(fdout) {
	    fprintf(fdout,
		    "WARNING: %s:%s does not support optional include\n",
		    dp->host->hostname, qdpname);
	}
    }

    result = vstralloc(";",
		       auth_opt,
		       kencrypt_opt,
		       compress_opt,
		       encrypt_opt,
		       decrypt_opt,
		       record_opt,
		       index_opt,
		       exclude_file,
		       exclude_list,
		       include_file,
		       include_list,
		       excl_opt,
		       incl_opt,
		       NULL);
    amfree(qdpname);
    amfree(auth_opt);
    amfree(exclude_list);
    amfree(exclude_file);
    amfree(include_file);
    amfree(include_list);
    amfree(exc);
    amfree(decrypt_opt);
    amfree(encrypt_opt);

    /* result contains at least 'auth=...' */
    if ( err ) {
	amfree(result);
	return NULL;
    } else {
	return result;
    }
}

 
char *
match_disklist(
    disklist_t *origqp,
    int		sargc,
    char **	sargv)
{
    char *prevhost = NULL;
    char *errstr = NULL;
    int i;
    int match_a_host;
    int match_a_disk;
    int prev_match;
    disk_t *dp;

    if(sargc <= 0)
	return NULL;

    for(dp = origqp->head; dp != NULL; dp = dp->next) {
	if(dp->todo == 1)
	    dp->todo = -1;
    }

    prev_match = 0;
    for(i=0;i<sargc;i++) {
	match_a_host = 0;
	for(dp = origqp->head; dp != NULL; dp = dp->next) {
	    if(match_host(sargv[i], dp->host->hostname))
		match_a_host = 1;
	}
	match_a_disk = 0;
	for(dp = origqp->head; dp != NULL; dp = dp->next) {
	    if(prevhost != NULL &&
	       match_host(prevhost, dp->host->hostname) &&
	       (match_disk(sargv[i], dp->name) ||
		(dp->device && match_disk(sargv[i], dp->device)))) {
		if(match_a_host) {
		    error("Argument %s match a host and a disk",sargv[i]);
		    /*NOTREACHED*/
		}
		else {
		    if(dp->todo == -1) {
			dp->todo = 1;
			match_a_disk = 1;
			prev_match = 0;
		    }
		}
	    }
	}
	if(!match_a_disk) {
	    if(match_a_host == 1) {
		if(prev_match == 1) { /* all disk of the previous host */
		    for(dp = origqp->head; dp != NULL; dp = dp->next) {
			if(match_host(prevhost,dp->host->hostname))
			    if(dp->todo == -1)
				dp->todo = 1;
		    }
		}
		prevhost = sargv[i];
		prev_match = 1;
	    }
	    else {
		vstrextend(&errstr, "Argument '", sargv[i], "' match neither a host nor a disk.\n", NULL);
	    }
	}
    }

    if(prev_match == 1) { /* all disk of the previous host */
	for(dp = origqp->head; dp != NULL; dp = dp->next) {
	    if(match_host(prevhost,dp->host->hostname))
		if(dp->todo == -1)
		    dp->todo = 1;
	}
    }

    for(dp = origqp->head; dp != NULL; dp = dp->next) {
	if(dp->todo == -1)
	    dp->todo = 0;
    }

    return errstr;
}


#ifdef TEST

static void dump_disk(const disk_t *);
static void dump_disklist(const disklist_t *);
int main(int, char *[]);

static void
dump_disk(
    const disk_t *	dp)
{
    printf("  DISK %s (HOST %s, LINE %d) TYPE %s NAME %s SPINDLE %d\n",
	   dp->name, dp->host->hostname, dp->line, dp->dtype_name,
	   dp->name == NULL? "(null)": dp->name,
	   dp->spindle);
}

static void
dump_disklist(
    const disklist_t *	lst)
{
    const disk_t *dp, *prev;
    const am_host_t *hp;

    if(hostlist == NULL) {
	printf("DISKLIST not read in\n");
	return;
    }

    printf("DISKLIST BY HOSTNAME:\n");

    for(hp = hostlist; hp != NULL; hp = hp->next) {
	printf("HOST %s INTERFACE %s\n",
	       hp->hostname,
	       (hp->netif == NULL||hp->netif->name == NULL) ? "(null)"
							    : hp->netif->name);
	for(dp = hp->disks; dp != NULL; dp = dp->hostnext)
	    dump_disk(dp);
	putchar('\n');
    }


    printf("DISKLIST IN FILE ORDER:\n");

    prev = NULL;
    for(dp = lst->head; dp != NULL; prev = dp, dp = dp->next) {
	dump_disk(dp);
	/* check pointers */
	if(dp->prev != prev) printf("*** prev pointer mismatch!\n");
	if(dp->next == NULL && lst->tail != dp) printf("tail mismatch!\n");
    }
}

int
main(
    int		argc,
    char **	argv)
{
  char *conffile;
  char *conf_diskfile;
  disklist_t lst;
  int result;
  unsigned long malloc_hist_1, malloc_size_1;
  unsigned long malloc_hist_2, malloc_size_2;

  safe_fd(-1, 0);

  set_pname("diskfile");

  dbopen("server");

  /* Don't die when child closes pipe */
  signal(SIGPIPE, SIG_IGN);

  malloc_size_1 = malloc_inuse(&malloc_hist_1);

  if (argc>1) {
    config_name = argv[1];
    if (strchr(config_name, '/') != NULL) {
      config_dir = stralloc2(argv[1], "/");
      config_name = strrchr(config_name, '/') + 1;
    } else {
      config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    }
  } else {
    config_dir = stralloc("");
  }
  conffile = stralloc2(config_dir, CONFFILE_NAME);
  if((result = read_conffile(conffile)) == 0) {
    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
      conf_diskfile = stralloc(conf_diskfile);
    } else {
      conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    result = read_diskfile(conf_diskfile, &lst);
    if(result == 0) {
      dump_disklist(&lst);
    }
    amfree(conf_diskfile);
  }
  amfree(conffile);
  amfree(config_dir);

  malloc_size_2 = malloc_inuse(&malloc_hist_2);

  if(malloc_size_1 != malloc_size_2) {
    malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
  }

  return result;
}
#endif /* TEST */
