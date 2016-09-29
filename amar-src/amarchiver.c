/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "event.h"
#include "getopt.h"
#include "amar.h"
#include "amutil.h"

static struct option long_options[] = {
    {"create"          , 0, NULL,  1},
    {"extract"         , 0, NULL,  2},
    {"list"            , 0, NULL,  3},
    {"verbose"         , 0, NULL,  4},
    {"file"            , 1, NULL,  5},
    {"version"         , 0, NULL,  6},
    {NULL, 0, NULL, 0}
};

static void
usage(void)
{
    printf("Usage: amarchiver [--version|--create|--list|--extract] [--verbose]* [--file file]\n");
    printf("            [filename]*\n");
    exit(1);
}

static void
error_exit(const char *action, GError *gerror)
{
    const char *msg = gerror->message? gerror->message : "(unknown)";
    g_fprintf(stderr, "%s: %s\n", action, msg);
    exit(1);
}

static void
do_create(char *opt_file, int opt_verbose, int argc, char **argv)
{
    FILE *output = stdout;
    amar_t *archive;
    amar_file_t *file;
    amar_attr_t *attribute;
    GError *gerror = NULL;
    int i, fd_out, fd_in;
    off_t filesize = 0;

    if (opt_file != NULL && !g_str_equal(opt_file, "-")) {
	fd_out = open(opt_file, O_CREAT|O_WRONLY|O_TRUNC, 0660);
	if (fd_out <= 0) {
	    error("open of '%s' failed: %s\n", opt_file, strerror(errno));
	}
    } else {
	fd_out = fileno(stdout);
	output = stderr;
    }
    archive = amar_new(fd_out, O_WRONLY, &gerror);
    if (!archive)
	error_exit("amar_new", gerror);

    i = 0;
    while (i<argc) {
	fd_in = open(argv[i], O_RDONLY);
	if (fd_in < 0) {
	    g_fprintf(stderr, "open of '%s' failed: %s\n", argv[i], strerror(errno));
	    i++;
	    continue;
	}
	filesize = 0;
	file = amar_new_file(archive, argv[i], strlen(argv[i]), NULL, &gerror);
	if (!file)
	    error_exit("amar_new_file", gerror);
	attribute = amar_new_attr(file, AMAR_ATTR_GENERIC_DATA, &gerror);
	if (!attribute)
	    error_exit("amar_new_attr", gerror);

	filesize += amar_attr_add_data_fd(attribute, fd_in, 1, &gerror);
	if (gerror)
	    error_exit("amar_attr_add_data_fd", gerror);

	if (!amar_attr_close(attribute, &gerror))
	    error_exit("amar_attr_close", gerror);
	if (!amar_file_close(file, &gerror))
	    error_exit("amar_file_close", gerror);

	if (opt_verbose == 1) {
	    g_fprintf(output,"%s\n", argv[i]);
	} else if (opt_verbose > 1) {
	    g_fprintf(output,"%llu %s\n", (unsigned long long)filesize, argv[i]);
	}
	close(fd_in);
	i++;
    }

    if (!amar_close(archive, &gerror))
	error_exit("amar_close", gerror);
    close(fd_out);
}

struct read_user_data {
    gboolean verbose;
    char **argv;
    int argc;
};

static gboolean
extract_file_start_cb(
	gpointer user_data,
	uint16_t filenum G_GNUC_UNUSED,
	gpointer filename_buf,
	gsize filename_len,
	gboolean *ignore G_GNUC_UNUSED,
	gpointer *file_data)
{
    struct read_user_data *ud = user_data;
    int i;

    /* keep the filename for later */
    *file_data = g_strndup(filename_buf, filename_len);

    if (ud->argc) {
	*ignore = TRUE;
	for (i = 0; i < ud->argc; i++) {
	    if (strlen(ud->argv[i]) == filename_len
		&& g_str_equal(ud->argv[i], *file_data))
		*ignore = FALSE;
	}
    }

    return TRUE;
}

static gboolean
extract_file_finish_cb(
	gpointer user_data G_GNUC_UNUSED,
	uint16_t filenum G_GNUC_UNUSED,
	gpointer *file_data,
	gboolean truncated)
{
    if (truncated)
	g_fprintf(stderr, _("Data for '%s' may have been truncated\n"),
		(char *)*file_data);

    g_free(*file_data);

    return TRUE;
}

static int
mkpath(
    const char *s,
    mode_t mode)
{
    char *path = NULL;
    char *r = NULL;
    int rv = -1;

    if (strcmp(s, ".") == 0 || strcmp(s, "/") == 0)
	return 0;

    path = g_strdup(s);
    r = dirname(path);

    if ((mkpath(r, mode) == -1) && (errno != EEXIST))
	goto out;

    if ((mkdir(s, mode) == -1) && (errno != EEXIST))
	rv = -1;
    else
	rv = 0;

out:
    g_free(path);

    return rv;
}

static gboolean
extract_frag_cb(
	gpointer user_data G_GNUC_UNUSED,
	uint16_t filenum G_GNUC_UNUSED,
	gpointer file_data,
	uint16_t attrid,
	gpointer attrid_data G_GNUC_UNUSED,
	gpointer *attr_data,
	gpointer data,
	gsize datasize,
	gboolean eoa,
	gboolean truncated)
{
    struct read_user_data *ud = user_data;
    int fd = GPOINTER_TO_INT(*attr_data);

    if (!fd) {
	char *filename;
	char *dir;
	if (attrid == AMAR_ATTR_GENERIC_DATA) {
	    filename = g_strdup((char *)file_data);
	} else {
	    filename = g_strdup_printf("%s.%d", (char *)file_data, attrid);
	}
	dir = g_strdup(filename);
	mkpath(dirname(dir), 0770);
	g_free(dir);
	fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0660);
	if (fd < 0) {
	    g_fprintf(stderr, _("Could not open '%s' for writing: %s"),
		    filename, strerror(errno));
	    return FALSE;
	}
	if (ud->verbose)
	    g_fprintf(stderr, "%s\n", filename);
	g_free(filename);
	*attr_data = GINT_TO_POINTER(fd);
    }

    if (full_write(fd, data, datasize) != datasize) {
	g_fprintf(stderr, _("while writing '%s.%d': %s"),
		(char *)file_data, attrid, strerror(errno));
	return FALSE;
    }

    if (eoa) {
	if (truncated) {
	    g_fprintf(stderr, _("'%s.%d' may be truncated\n"),
		    (char *)file_data, attrid);
	}
	close(fd);
    }

    return TRUE;
}

static void
do_extract(
	char *opt_file,
	int opt_verbose,
	int argc,
	char **argv)
{
    amar_t *archive;
    GError *gerror = NULL;
    int fd_in;
    amar_attr_handling_t handling[] = {
	{ 0, 0, extract_frag_cb, NULL },
    };
    struct read_user_data ud;

    ud.argv = argv;
    ud.argc = argc;
    ud.verbose = opt_verbose;

    if (opt_file && !g_str_equal(opt_file, "-")) {
	fd_in = open(opt_file, O_RDONLY);
	if (fd_in <= 0) {
	    error("open of '%s' failed: %s\n", opt_file, strerror(errno));
	}
    } else {
	fd_in = fileno(stdin);
    }

    archive = amar_new(fd_in, O_RDONLY, &gerror);
    if (!archive)
	error_exit("amar_new", gerror);

//    if (!amar_read(archive, &ud, handling, extract_file_start_cb,
//		   extract_file_finish_cb, NULL, &gerror)) {
//	if (gerror)
//	    error_exit("amar_read", gerror);
//	else
//	    /* one of the callbacks already printed an error message */
//	    exit(1);
//    }

    set_amar_read_cb(archive, &ud, handling, extract_file_start_cb,
		      extract_file_finish_cb, NULL, &gerror);
    event_loop(0);
    if (gerror) {
	error_exit("amar_read", gerror);
    }

    amar_close(archive, NULL);
}

static gboolean
list_file_start_cb(
	gpointer user_data G_GNUC_UNUSED,
	uint16_t filenum G_GNUC_UNUSED,
	gpointer filename_buf,
	gsize filename_len,
	gboolean *ignore,
	gpointer *file_data G_GNUC_UNUSED)
{
    g_printf("%.*s\n", (int)filename_len, (char *)filename_buf);
    *ignore = TRUE;

    return TRUE;
}

static void
do_list(
	char *opt_file,
	int opt_verbose G_GNUC_UNUSED)
{
    amar_t *archive;
    GError *gerror = NULL;
    int fd_in;
    amar_attr_handling_t handling[] = {
	{ 0, 0, NULL, NULL },
    };

    if (opt_file && !g_str_equal(opt_file, "-")) {
	fd_in = open(opt_file, O_RDONLY);
	if (fd_in <= 0) {
	    error("open of '%s' failed: %s\n", opt_file, strerror(errno));
	}
    } else {
	fd_in = fileno(stdin);
    }

    archive = amar_new(fd_in, O_RDONLY, &gerror);
    if (!archive)
	error_exit("amar_new", gerror);

    if (!amar_read(archive, NULL, handling, list_file_start_cb,
		   NULL, NULL, &gerror)) {
	if (gerror)
	    error_exit("amar_read", gerror);
	else
	    /* one of the callbacks already printed an error message */
	    exit(1);
    }
    printf("size: %lld\n", (long long)amar_size(archive));
    amar_close(archive, NULL);
}

int main(
    int    argc,
    char **argv)
{
    int   opt_create    = 0;
    int   opt_extract   = 0;
    int   opt_list      = 0;
    int   opt_verbose   = 0;
    char *opt_file      = NULL;

    glib_init();

    while(1) {
	int option_index = 0;
	int c = getopt_long (argc, argv, "", long_options, &option_index);
	if (c == -1) {
	    break;
	}
	switch (c) {
	case 1: opt_create = 1;
		break;
	case 2: opt_extract = 1;
		break;
	case 3: opt_list = 1;
		break;
	case 4: opt_verbose += 1;
		break;
	case 5: amfree(opt_file);
		opt_file = g_strdup(optarg);
		break;
	case 6: printf("amarchiver %s\n", VERSION);
		exit(0);
		break;
	}
    }
    argc -= optind;
    argv += optind;

    /* check those arguments */
    if (opt_create + opt_extract + opt_list == 0) {
	g_fprintf(stderr,"--create, --list or --extract must be provided\n");
	usage();
    }
    if (opt_create + opt_extract + opt_list > 1) {
	g_fprintf(stderr,"Only one of --create, --list or --extract must be provided\n");
	usage();
    }

    if (opt_create > 0)
	do_create(opt_file, opt_verbose, argc, argv);
    else if (opt_extract > 0)
	do_extract(opt_file, opt_verbose, argc, argv);
    else if (opt_list > 0)
	do_list(opt_file, opt_verbose);

    amfree(opt_file);
    return 0;
}
