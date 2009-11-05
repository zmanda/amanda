/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id: set_commands.c,v 1.26 2006/07/05 13:14:58 martinea Exp $
 *
 * implements the "set" commands in amrecover
 */

#include "amanda.h"
#include "util.h"
#include "amrecover.h"
#include "amxml.h"

extern unsigned short samba_extract_method;

/* sets a date, mapping given date into standard form if needed */
int
set_date(
    char *	date)
{
    char *cmd = NULL;
    char *qdisk_path;

    clear_dir_list();

    cmd = stralloc2("DATE ", date);
    if (converse(cmd) == -1)
	exit(1);

    /* if a host/disk/directory is set, then check if that directory
       is still valid at the new date, and if not set directory to
       mount_point */
    if (disk_path != NULL) {
	qdisk_path = quote_string(disk_path);
	cmd = newstralloc2(cmd, "OISD ", qdisk_path);
	amfree(qdisk_path);
	if (exchange(cmd) == -1)
	    exit(1);
	if (server_happy())
	{
	    suck_dir_list_from_server();
	}
	else
	{
	    g_printf(_("No index records for cwd on new date\n"));
	    g_printf(_("Setting cwd to mount point\n"));
	    disk_path = newstralloc(disk_path, "/");	/* fake it */
	    clear_dir_list();
	}
    }
    amfree(cmd);
    return 0;
}


void
set_host(
    const char *host)
{
    char *cmd = NULL;
    struct hostent *hp = NULL;
    char **hostp;
    int found_host = 0;
    char *uqhost = unquote_string(host);

    if (is_extract_list_nonempty())
    {
	g_printf(_("Must clear extract list before changing host\n"));
	amfree(uqhost);
	return;
    }

    /*
     * The idea here is to try as many permutations of the hostname
     * as we can imagine.  The server will reject anything it doesn't
     * recognize.
     */

    cmd = stralloc2("HOST ", uqhost);
    if (converse(cmd) == -1)
	exit(1);
    if (server_happy())
	found_host = 1;

    /*
     * Try converting the given host to a fully qualified, canonical
     * name.
     */
    if (!found_host) {
	if ((hp = gethostbyname(uqhost)) != NULL) {
	    host = hp->h_name;
	    g_printf(_("Trying host %s ...\n"), host);
	    cmd = newstralloc2(cmd, "HOST ", host);
	    if (converse(cmd) == -1)
		exit(1);
	    if(server_happy())
		found_host = 1;
	}
    }

    /*
     * Since we have them, try any CNAMEs that were traversed from uqhost
     * to the canonical name (this assumes gethostbyname was called above)
     */
    if (!found_host) {
	if (hp) {
	    for (hostp = hp->h_aliases; (host = *hostp) != NULL; hostp++)
	    {
		g_printf(_("Trying host %s ...\n"), host);
		cmd = newstralloc2(cmd, "HOST ", host);
		if (converse(cmd) == -1)
		    exit(1);
		if(server_happy())
		{
		    found_host = 1;
		    break;
		}
	    }
	}
    }

    /* Try looking up the canonical name of the host */
    if (!found_host) {
	char *canonname;
	int result;

	result = resolve_hostname(uqhost, 0, NULL, &canonname);
	if (result == 0 && canonname) {
	    host = canonname;
	    g_printf(_("Trying host %s ...\n"), host);
	    cmd = newstralloc2(cmd, "HOST ", host);
	    if (converse(cmd) == -1)
		exit(1);
	    if(server_happy())
		found_host = 1;
	}
    }

    if(found_host) {
	dump_hostname = newstralloc(dump_hostname, host);
	amfree(disk_name);
	amfree(mount_point);
	amfree(disk_path);
	clear_dir_list();
    }
    amfree(cmd);
    amfree(uqhost);
}

void
list_host(void)
{
    char *cmd = NULL;

    cmd = stralloc("LISTHOST");
    if (converse(cmd) == -1)
        exit(1);
    amfree(cmd);
}

void
set_disk(
    char *	dsk,
    char *	mtpt)
{
    char *cmd = NULL;
    char *qdsk;
    char *uqdsk;
    char *uqmtpt = NULL;

    if (is_extract_list_nonempty())
    {
	g_printf(_("Must clear extract list before changing disk\n"));
	return;
    }

    /* if mount point specified, check it is valid */
    if (mtpt != NULL) {
	uqmtpt = unquote_string(mtpt);
	if (*mtpt != '/') {
	    g_printf(_("Mount point \"%s\" invalid - must start with /\n"), uqmtpt);
	    amfree(uqmtpt);
	    return;
	}
    }

    clear_dir_list();
    uqdsk = unquote_string(dsk);
    qdsk = quote_string(uqdsk);
    cmd = stralloc2("DISK ", qdsk);
    amfree(qdsk);
    if (converse(cmd) == -1)
	exit(1);
    amfree(cmd);

    if (!server_happy()) {
	amfree(uqmtpt);
	amfree(uqdsk);
	return;
    }

    disk_name = newstralloc(disk_name, uqdsk);
    if (mtpt == NULL)
    {
	/* mount point not specified */
	if (*uqdsk == '/')
	{
	    /* disk specified by mount point, hence use it */
	    mount_point = newstralloc(mount_point, uqdsk);
	}
	else
	{
	    /* device name given, use '/' because nothing better */
	    mount_point = newstralloc(mount_point, "/");
	}
    }
    else
    {
	/* mount point specified */
	mount_point = newstralloc(mount_point, uqmtpt);
    }

    /* set the working directory to the mount point */
    /* there is the possibility that there are no index records for the
       disk for the given date, hence setting the directory to the
       mount point will fail. Preempt this by checking first so we can write
       a more informative message. */
    if (exchange("OISD /") == -1)
	exit(1);
    if (server_happy())
    {
	disk_path = newstralloc(disk_path, "/");
	suck_dir_list_from_server();	/* get list of directory contents */
    }
    else
    {
	g_printf(_("No index records for disk for specified date\n"));
	g_printf(_("If date correct, notify system administrator\n"));
	disk_path = newstralloc(disk_path, "/");	/* fake it */
	clear_dir_list();
    }
    amfree(uqmtpt);
    amfree(uqdsk);

    if (am_has_feature(indexsrv_features, fe_amindexd_DLE)) {
	char *dle_str;
	char *errmsg = NULL;

	cmd = stralloc("DLE");
	if (exchange(cmd) == -1)
	    exit(1);
	amfree(cmd);

	if (!server_happy())
	    return;

	dle_str = reply_line();
	if (BSTRNCMP(dle_str+4, "NODLE") == 0) {
	    dump_dle = NULL;
	} else {
	    dle_str = unquote_string(dle_str+4);
	    dump_dle = amxml_parse_node_CHAR(dle_str, &errmsg);
	    amfree(dle_str);
	}
    }
}

void
list_disk(
    char *	amdevice)
{
    char *cmd = NULL;
    char *qamdevice, *uqamdevice;

    if(amdevice) {
	uqamdevice = unquote_string(amdevice);
	qamdevice = quote_string(uqamdevice);
	cmd = stralloc2("LISTDISK ", qamdevice);
	amfree(uqamdevice);
	amfree(qamdevice);
	if (converse(cmd) == -1)
	    exit(1);
	amfree(cmd);
    }
    else {
	cmd = stralloc("LISTDISK");
	if (converse(cmd) == -1)
	    exit(1);
	amfree(cmd);
    }
}

static GSList *prop_values = NULL;

void
set_property_name(
    char *name,
    int   append)
{
    property_t *prop;
    char       *property_name, *pn;

    pn = unquote_string(name);
    property_name = amandaify_property_name(pn);
    amfree(pn);

    if (!append) {
	g_hash_table_remove(proplist, property_name);
	prop = NULL;
    } else {
	prop = g_hash_table_lookup(proplist, property_name);
    }
    
    if (!prop) {
	prop = malloc(sizeof(property_t));
	prop->append = 0;
	prop->priority = 1;
	prop->values = NULL;
	g_hash_table_insert(proplist, stralloc(property_name), prop);
    }

    /* add prop_values to prop->values */
    if (!prop->values) {
	prop->values = prop_values;
	prop_values = NULL;
    } else {
	GSList *pv;

	for(pv = prop_values; pv != NULL; pv = pv->next) {
	    prop->values = g_slist_append(prop->values, pv->data);
	}
	g_slist_free(prop_values);
	prop_values = NULL;
    }
    amfree(property_name);
}

void
add_property_value(
    char *value)
{
    if (value) {
	prop_values = g_slist_prepend(prop_values, unquote_string(value));
    }
}

/* A GHFunc (callback for g_hash_table_foreach) */
static void list_one_property(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p G_GNUC_UNUSED)
{
    char       *property_s = key_p;
    char       *qproperty;
    property_t *property = value_p;
    GSList     *value;
    char       *qvalue;

    qproperty = quote_string_always(property_s);
    printf("property %s", qproperty);
    amfree(qproperty);
    for (value = property->values; value != NULL; value = value->next) {
	qvalue = quote_string_always((char*)value->data);
	printf(" %s", qvalue);
	amfree(qvalue);
    }
    printf("\n");
}

void
list_property(void)
{
    if (proplist) {
	g_hash_table_foreach(proplist, list_one_property, NULL);
    } else {
	printf("No property set\n");
    }
}

void
local_cd(
    char *dir)
{
    char *uqdir = unquote_string(dir);
    if (chdir(uqdir) == -1) {
	perror(uqdir);
    }
    amfree(uqdir);
}

int
cd_glob(
    char *	glob,
    int		verbose)
{
    char *regex;
    char *regex_path;
    char *s;
    char *uqglob;
    int   result;

    char *path_on_disk = NULL;

    if (disk_name == NULL) {
	g_printf(_("Must select disk before changing directory\n"));
	return 0;
    }

    uqglob = unquote_string(glob);
    regex = glob_to_regex(uqglob);
    dbprintf(_("cd_glob (%s) -> %s\n"), uqglob, regex);
    if ((s = validate_regexp(regex)) != NULL) {
        g_printf(_("\"%s\" is not a valid shell wildcard pattern: "), glob);
        puts(s);
	amfree(regex);
	amfree(uqglob);
        return 0;
    }
    /*
     * glob_to_regex() anchors the beginning of the pattern with ^,
     * but we will be tacking it onto the end of the current directory
     * in add_file, so strip that off.  Also, it anchors the end with
     * $, but we need to match a trailing /, add it if it is not there
     */
    regex_path = stralloc(regex + 1);
    amfree(regex);
    if(regex_path[strlen(regex_path) - 2] != '/' ) {
	regex_path[strlen(regex_path) - 1] = '\0';
	strappend(regex_path, "/$");
    }

    /* convert path (assumed in cwd) to one on disk */
    if (strcmp(disk_path, "/") == 0)
        path_on_disk = stralloc2("/", regex_path);
    else {
        char *clean_disk_path = clean_regex(disk_path);
        path_on_disk = vstralloc(clean_disk_path, "/", regex_path, NULL);
        amfree(clean_disk_path);
    }

    result = cd_dir(path_on_disk, uqglob, verbose);

    amfree(regex_path);
    amfree(path_on_disk);
    amfree(uqglob);

    return result;
}

int
cd_regex(
    char *	regex,
    int		verbose)
{
    char *s;
    char *uq_orig_regex;
    char *uqregex;
    int  len_uqregex;
    int  result;

    char *path_on_disk = NULL;

    if (disk_name == NULL) {
	g_printf(_("Must select disk before changing directory\n"));
	return 0;
    }

    uq_orig_regex = unquote_string(regex);
    uqregex = stralloc(uq_orig_regex);

    /* Add a terminating '/' if it is not there, maybe before a '$' */
    len_uqregex = strlen(uqregex);
    if (uqregex[len_uqregex-1] == '$') {
	if (uqregex[len_uqregex-2] != '/') {
	    uqregex[len_uqregex-1] = '\0';
	    strappend(uqregex, "/$");
	}
    } else if (uqregex[len_uqregex-1] != '/') {
	//uqregex[len_uqregex-1] = '\0';
	strappend(uqregex, "/");
    }
    if ((s = validate_regexp(uqregex)) != NULL) {
	g_printf(_("\"%s\" is not a valid regular expression: "), uq_orig_regex);
	amfree(uqregex);
	amfree(uq_orig_regex);
	puts(s);
	return 0;
    }

    /* convert path (assumed in cwd) to one on disk */
    if (strcmp(disk_path, "/") == 0)
        path_on_disk = stralloc2("/", uqregex);
    else {
        char *clean_disk_path = clean_regex(disk_path);
        path_on_disk = vstralloc(clean_disk_path, "/", regex, NULL);
        amfree(clean_disk_path);
    }

    result = cd_dir(path_on_disk, uq_orig_regex, verbose);

    amfree(path_on_disk);
    amfree(uqregex);
    amfree(uq_orig_regex);

    return result;
}

int
cd_dir(
    char *	path_on_disk,
    char *	default_dir,
    int		verbose)
{
    char *dir = NULL;
    char *s;
    int nb_found;
    int result;
    size_t i;

    DIR_ITEM *ditem;

    if ((s = validate_regexp(path_on_disk)) != NULL) {
	result = set_directory(default_dir, verbose);
	return result;
    }

    nb_found = 0;

    for (ditem=get_dir_list(); ditem!=NULL && nb_found <= 1; 
			       ditem=get_next_dir_item(ditem))
    {
	if (match(path_on_disk, ditem->path))
	{
	    i = strlen(ditem->path);
	    if((i > 0 && ditem->path[i-1] == '/')
               || (i > 1 && ditem->path[i-2] == '/' && ditem->path[i-1] == '.'))
            {   /* It is a directory */
		char *dir1, *dir2;
		nb_found++;
		dir = newstralloc(dir,ditem->path);
		if(dir[strlen(dir)-1] == '/')
		    dir[strlen(dir)-1] = '\0'; /* remove last / */
		/* remove everything before the last / */
		dir1 = strrchr(dir,'/');
		if (dir1) {
		    dir1++;
		    dir2 = stralloc(dir1);
		    amfree(dir);
		    dir = dir2;
		}
	    }
	}
    }

    if(nb_found==0) {
	result = set_directory(default_dir, verbose);
    }
    else if(nb_found==1) {
	result = set_directory(dir, verbose);
    }
    else {
	g_printf(_("Too many directories matching '%s'\n"), default_dir);
	result = 0;
    }
    amfree(dir);
    return result;
}

int
set_directory(
    char *	dir,
    int		verbose)
{
    char *cmd = NULL;
    char *new_dir = NULL;
    char *qnew_dir;
    char *dp, *de;
    char *ldir = NULL;
    int   result;

    /* do nothing if "." */
    if(strcmp(dir,".")==0) {
	show_directory();		/* say where we are */
	return 1;
	/*NOTREACHED*/
    }

    if (disk_name == NULL) {
	g_printf(_("Must select disk before setting directory\n"));
	return 0;
	/*NOTREACHED*/
    }

    ldir = stralloc(dir);
    clean_pathname(ldir);

    /* convert directory into absolute path relative to disk mount point */
    if (ldir[0] == '/')
    {
	/* absolute path specified, must start with mount point */
	if (strcmp(mount_point, "/") == 0)
	{
	    new_dir = stralloc(ldir);
	}
	else
	{
	    if (strncmp(mount_point, ldir, strlen(mount_point)) != 0)
	    {
		g_printf(_("Invalid directory - Can't cd outside mount point \"%s\"\n"),
		       mount_point);
		amfree(ldir);
		return 0;
		/*NOTREACHED*/
	    }
	    new_dir = stralloc(ldir+strlen(mount_point));
	    if (strlen(new_dir) == 0) {
		new_dir = newstralloc(new_dir, "/");
					/* i.e. ldir == mount_point */
	    }
	}
    }
    else
    {
	new_dir = stralloc(disk_path);
	dp = ldir;
	/* strip any leading ..s */
	while (strncmp(dp, "../", 3) == 0)
	{
	    de = strrchr(new_dir, '/');	/* always at least 1 */
	    if (de == new_dir)
	    {
		/* at top of disk */
		*(de + 1) = '\0';
		dp = dp + 3;
	    }
	    else
	    {
		*de = '\0';
		dp = dp + 3;
	    }
	}
	if (strcmp(dp, "..") == 0) {
	    if (strcmp(new_dir, "/") == 0) {
		/* at top of disk */
		g_printf(_("Invalid directory - Can't cd outside mount point \"%s\"\n"),
		       mount_point);
		/*@ignore@*/
		amfree(new_dir);
		/*@end@*/
		amfree(ldir);
		return 0;
		/*NOTREACHED*/
	    }
	    de = strrchr(new_dir, '/');	/* always at least 1 */
	    if (de == new_dir)
	    {
		/* at top of disk */
		*(de+1) = '\0';
	    }
	    else
	    {
		*de = '\0';
 	    }
	} else {
	    /*@ignore@*/
	    if (strcmp(new_dir, "/") != 0) {
		strappend(new_dir, "/");
	    }
	    strappend(new_dir, ldir);
	    /*@end@*/
	}
    }

    qnew_dir = quote_string(new_dir);
    cmd = stralloc2("OISD ", qnew_dir);
    amfree(qnew_dir);
    if (exchange(cmd) == -1) {
	exit(1);
	/*NOTREACHED*/
    }
    amfree(cmd);

    if (server_happy())
    {
	disk_path = newstralloc(disk_path, new_dir);
	suck_dir_list_from_server();	/* get list of directory contents */
	if (verbose)
	    show_directory();		/* say where we moved to */
	result = 1;
    }
    else
    {
	g_printf(_("Invalid directory - %s\n"), dir);
	result = 0;
    }

    /*@ignore@*/
    amfree(new_dir);
    amfree(ldir);
    /*@end@*/

    return result;
}


/* prints the current working directory */
void
show_directory(void)
{
    if (mount_point == NULL || disk_path == NULL)
        g_printf(_("Must select disk first\n"));
    else if (strcmp(mount_point, "/") == 0)
	g_printf("%s\n", disk_path);
    else if (strcmp(disk_path, "/") == 0)
	g_printf("%s\n", mount_point);
    else
	g_printf("%s%s\n", mount_point, disk_path);
}


/* set the tape server and device (deprecated version) */
void
set_tape(
    char *	tape)
{
    char *uqtape = unquote_string(tape);
    char *tapedev = strchr(uqtape, ':');
    char *host = NULL;

    g_printf(_("NOTE: 'settape' is deprecated; use setdevice instead.\n"));

    if (tapedev)
    {
	/* This command is deprecated because this parsing is going to fall 
	 * behind the list of available device names at some point, or to shadow
	 * an interesting hostname (wouldn't 'tape' be a good name for a 
	 * tape server?) */
	if (tapedev != uqtape) {
	    if((strchr(tapedev+1, ':') == NULL) &&
	       (strncmp_const(uqtape, "null:") == 0 ||
		strncmp_const(uqtape, "rait:") == 0 ||
		strncmp_const(uqtape, "file:") == 0 ||
		strncmp_const(uqtape, "s3:") == 0 ||
		strncmp_const(uqtape, "tape:") == 0)) {
		tapedev = uqtape;
	    }
	    else {
		*tapedev = '\0';
		host = stralloc(uqtape);
		++tapedev;
	    }
	} else {
	    ++tapedev;
	}
    } else
	tapedev = uqtape;
    
    if (tapedev[0])
    {
	if (strcmp(tapedev, "default") == 0)
	    tapedev = NULL;
    }

    /* call out to the new version */
    set_device(host, tapedev);

    amfree(host);
    amfree(uqtape);
}

/* set the tape server and device, for real */
void
set_device(
    char *	host,
    char *	device)
{
    if (host)
	tape_server_name = newstralloc(tape_server_name, host);
    else
	amfree(tape_server_name);

    if (device)
	tape_device_name = newstralloc(tape_device_name, device);
    else
	amfree(tape_device_name);

    /* print the current status */
    if (tape_device_name)
	g_printf (_("Using tape \"%s\""), tape_device_name);
    else
	g_printf (_("Using default tape"));

    if (tape_server_name)
	g_printf (_(" from server %s.\n"), tape_server_name);
    else
	g_printf (_(".\nTape server unspecified, assumed to be %s.\n"),
		server_name);
}

void
set_mode(
    int		mode)
{
  if (mode == SAMBA_SMBCLIENT) {
    g_printf (_("SAMBA dumps will be extracted using smbclient\n"));
    samba_extract_method = SAMBA_SMBCLIENT;
  } else {
    if (mode == SAMBA_TAR) {
      g_printf (_("SAMBA dumps will be extracted as TAR dumps\n"));
      samba_extract_method = SAMBA_TAR;
    }
  }
}

void
show_mode(void) 
{
#ifdef SAMBA_CLIENT
  g_printf (_("SAMBA dumps are extracted "));

  if (samba_extract_method == SAMBA_TAR) {
    g_printf (_(" as TAR dumps\n"));
  } else {
    g_printf (_("using smbclient\n"));
  }
#endif /* SAMBA_CLIENT */
}
