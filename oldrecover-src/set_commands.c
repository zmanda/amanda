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
 * $Id: set_commands.c,v 1.3 2006/07/05 13:14:58 martinea Exp $
 *
 * implements the "set" commands in amrecover
 */

#include "amanda.h"
#include "amrecover.h"

#ifdef SAMBA_CLIENT
extern unsigned short samba_extract_method;
#endif /* SAMBA_CLIENT */

/* sets a date, mapping given date into standard form if needed */
int
set_date(
    char *	date)
{
    char *cmd = NULL;

    clear_dir_list();

    cmd = stralloc2("DATE ", date);
    if (converse(cmd) == -1)
	exit(1);

    /* if a host/disk/directory is set, then check if that directory
       is still valid at the new date, and if not set directory to
       mount_point */
    if (disk_path != NULL) {
	cmd = newstralloc2(cmd, "OISD ", disk_path);
	if (exchange(cmd) == -1)
	    exit(1);
	if (server_happy())
	{
	    suck_dir_list_from_server();
	}
	else
	{
	    printf("No index records for cwd on new date\n");
	    printf("Setting cwd to mount point\n");
	    disk_path = newstralloc(disk_path, "/");	/* fake it */
	    clear_dir_list();
	}
    }
    amfree(cmd);
    return 0;
}


void
set_host(
    const char *	host)
{
    char *cmd = NULL;
    struct hostent *hp;
    char **hostp;
    int found_host = 0;

    if (is_extract_list_nonempty())
    {
	printf("Must clear extract list before changing host\n");
	return;
    }

    cmd = stralloc2("HOST ", host);
    if (converse(cmd) == -1)
	exit(1);
    if (server_happy())
    {
	found_host = 1;
    }
    else
    {
	/*
	 * Try converting the given host to a fully qualified name
	 * and then try each of the aliases.
	 */
	if ((hp = gethostbyname(host)) != NULL) {
	    host = hp->h_name;
	    printf("Trying host %s ...\n", host);
	    cmd = newstralloc2(cmd, "HOST ", host);
	    if (converse(cmd) == -1)
		exit(1);
	    if(server_happy())
	    {
		found_host = 1;
	    }
	    else
	    {
	        for (hostp = hp->h_aliases; (host = *hostp) != NULL; hostp++)
	        {
		    printf("Trying host %s ...\n", host);
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
    }
    if(found_host)
    {
	dump_hostname = newstralloc(dump_hostname, host);
	amfree(disk_name);
	amfree(mount_point);
	amfree(disk_path);
	clear_dir_list();
    }
    amfree(cmd);
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

    if (is_extract_list_nonempty())
    {
	printf("Must clear extract list before changing disk\n");
	return;
    }

    /* if mount point specified, check it is valid */
    if ((mtpt != NULL) && (*mtpt != '/'))
    {
	printf("Mount point \"%s\" invalid - must start with /\n", mtpt);
	return;
    }

    clear_dir_list();
    cmd = stralloc2("DISK ", dsk);
    if (converse(cmd) == -1)
	exit(1);
    amfree(cmd);

    if (!server_happy())
	return;

    disk_name = newstralloc(disk_name, dsk);
    if (mtpt == NULL)
    {
	/* mount point not specified */
	if (*dsk == '/')
	{
	    /* disk specified by mount point, hence use it */
	    mount_point = newstralloc(mount_point, dsk);
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
	mount_point = newstralloc(mount_point, mtpt);
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
	printf("No index records for disk for specified date\n");
	printf("If date correct, notify system administrator\n");
	disk_path = newstralloc(disk_path, "/");	/* fake it */
	clear_dir_list();
    }
}

void
list_disk(
    char *	amdevice)
{
    char *cmd = NULL;

    if(amdevice) {
	cmd = stralloc2("LISTDISK ", amdevice);
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

void
cd_glob(
    char *	glob)
{
    char *regex;
    char *regex_path;
    char *s;

    char *path_on_disk = NULL;

    if (disk_name == NULL) {
	printf("Must select disk before changing directory\n");
	return;
    }

    regex = glob_to_regex(glob);
    dbprintf(("cd_glob (%s) -> %s\n", glob, regex));
    if ((s = validate_regexp(regex)) != NULL) {
        printf("\"%s\" is not a valid shell wildcard pattern: ", glob);
        puts(s);
	amfree(regex);
        return;
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

    cd_dir(path_on_disk, glob);

    amfree(regex_path);
    amfree(path_on_disk);
}

void
cd_regex(
    char *	regex)
{
    char *s;

    char *path_on_disk = NULL;

    if (disk_name == NULL) {
	printf("Must select disk before changing directory\n");
	return;
    }

    if ((s = validate_regexp(regex)) != NULL) {
	printf("\"%s\" is not a valid regular expression: ", regex);
	puts(s);
	return;
    }

    /* convert path (assumed in cwd) to one on disk */
    if (strcmp(disk_path, "/") == 0)
        path_on_disk = stralloc2("/", regex);
    else {
        char *clean_disk_path = clean_regex(disk_path);
        path_on_disk = vstralloc(clean_disk_path, "/", regex, NULL);
        amfree(clean_disk_path);
    }

    cd_dir(path_on_disk, regex);

    amfree(path_on_disk);
}

void
cd_dir(
    char *	path_on_disk,
    char *	default_dir)
{
    char *path_on_disk_slash = NULL;
    char *dir = NULL;

    int nb_found;
    size_t i;

    DIR_ITEM *ditem;

    path_on_disk_slash = stralloc2(path_on_disk, "/");

    nb_found = 0;

    for (ditem=get_dir_list(); ditem!=NULL && nb_found <= 1; 
			       ditem=get_next_dir_item(ditem))
    {
	if (match(path_on_disk, ditem->path)
	    || match(path_on_disk_slash, ditem->path))
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
		dir1 = rindex(dir,'/');
		if (dir1) {
		    dir1++;
		    dir2 = stralloc(dir1);
		    amfree(dir);
		    dir = dir2;
		}
	    }
	}
    }
    amfree(path_on_disk_slash);

    if(nb_found==0) {
	set_directory(default_dir);
    }
    else if(nb_found==1) {
	set_directory(dir);
    }
    else {
	printf("Too many directory\n");
    }
    amfree(dir);
}

void
set_directory(
    char *	dir)
{
    char *cmd = NULL;
    char *new_dir = NULL;
    char *dp, *de;
    char *ldir = NULL;

    /* do nothing if "." */
    if(strcmp(dir,".")==0) {
	show_directory();		/* say where we are */
	return;
	/*NOTREACHED*/
    }

    if (disk_name == NULL) {
	printf("Must select disk before setting directory\n");
	return;
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
		printf("Invalid directory - Can't cd outside mount point \"%s\"\n",
		       mount_point);
		amfree(ldir);
		return;
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
		printf("Invalid directory - Can't cd outside mount point \"%s\"\n",
		       mount_point);
		/*@ignore@*/
		amfree(new_dir);
		/*@end@*/
		amfree(ldir);
		return;
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

    cmd = stralloc2("OISD ", new_dir);
    if (exchange(cmd) == -1) {
	exit(1);
	/*NOTREACHED*/
    }
    amfree(cmd);

    if (server_happy())
    {
	disk_path = newstralloc(disk_path, new_dir);
	suck_dir_list_from_server();	/* get list of directory contents */
	show_directory();		/* say where we moved to */
    }
    else
    {
	printf("Invalid directory - %s\n", dir);
    }

    /*@ignore@*/
    amfree(new_dir);
    amfree(ldir);
    /*@end@*/
}


/* prints the current working directory */
void
show_directory(void)
{
    if (mount_point == NULL || disk_path == NULL)
        printf("Must select disk first\n");
    else if (strcmp(mount_point, "/") == 0)
	printf("%s\n", disk_path);
    else if (strcmp(disk_path, "/") == 0)
	printf("%s\n", mount_point);
    else
	printf("%s%s\n", mount_point, disk_path);
}


/* set the tape server and device */
void
set_tape(
    char *	tape)
{
    char *tapedev = strchr(tape, ':');

    if (tapedev)
    {
	if (tapedev != tape) {
	    if((strchr(tapedev+1, ':') == NULL) &&
	       (strncmp(tape, "null:", 5) == 0 ||
		strncmp(tape, "rait:", 5) == 0 ||
		strncmp(tape, "file:", 5) == 0 ||
		strncmp(tape, "tape:", 5) == 0)) {
		tapedev = tape;
	    }
	    else {
		*tapedev = '\0';
		tape_server_name = newstralloc(tape_server_name, tape);
		++tapedev;
	    }
	} else { /* reset server_name if start with : */
	    amfree(tape_server_name);
	    ++tapedev;
	}
    } else
	tapedev = tape;
    
    if (tapedev[0])
    {
	if (strcmp(tapedev, "default") == 0)
	    amfree(tape_device_name);
	else
	    tape_device_name = newstralloc(tape_device_name, tapedev);
    }

    if (tape_device_name)
	printf ("Using tape \"%s\"", tape_device_name);
    else
	printf ("Using default tape");

    if (tape_server_name)
	printf (" from server %s.\n", tape_server_name);
    else
	printf (".\nTape server unspecified, assumed to be %s.\n",
		server_name);
}

void
set_mode(
    int		mode)
{
#ifdef SAMBA_CLIENT
  if (mode == SAMBA_SMBCLIENT) {
    printf ("SAMBA dumps will be extracted using smbclient\n");
    samba_extract_method = SAMBA_SMBCLIENT;
  } else {
    if (mode == SAMBA_TAR) {
      printf ("SAMBA dumps will be extracted as TAR dumps\n");
      samba_extract_method = SAMBA_TAR;
    }
  }
#else
  (void)mode;	/* Quiet unused parameter warning */
#endif /* SAMBA_CLIENT */
}

void
show_mode(void) 
{
#ifdef SAMBA_CLIENT
  printf ("SAMBA dumps are extracted ");

  if (samba_extract_method == SAMBA_TAR) {
    printf (" as TAR dumps\n");
  } else {
    printf ("using smbclient\n");
  }
#endif /* SAMBA_CLIENT */
}
