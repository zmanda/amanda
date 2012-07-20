/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "getopt.h"
#include "amar.h"

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
}

static void
error_exit(const char *action, GError *error)
{
    const char *msg = error->message? error->message : "(unknown)";
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
    GError *error = NULL;
    int i, fd_out, fd_in;
    off_t filesize = 0;

    if (opt_file != NULL && strcmp(opt_file,"-") != 0) {
	fd_out = open(opt_file, O_CREAT|O_WRONLY|O_TRUNC, 0660);
	if (fd_out <= 0) {
	    error("open of '%s' failed: %s\n", opt_file, strerror(errno));
	}
    } else {
	fd_out = fileno(stdout);
	output = stderr;
    }
    archive = amar_new(fd_out, O_WRONLY, &error);
    if (!archive)
	error_exit("amar_new", error);

    i = 0;
    while (i<argc) {
	fd_in = open(argv[i], O_RDONLY);
	if (fd_in <= 0) {
	    g_fprintf(stderr, "open of '%s' failed: %s\n", argv[i], strerror(errno));
	    i++;
	    continue;
	}
	filesize = 0;
	file = amar_new_file(archive, argv[i], strlen(argv[i]), NULL, &error);
	if (error)
	    error_exit("amar_new_file", error);
	attribute = amar_new_attr(file, AMAR_ATTR_GENERIC_DATA, &error);
	if (error)
	    error_exit("amar_new_attr", error);

	filesize += amar_attr_add_data_fd(attribute, fd_in, 1, &error);
	if (error)
	    error_exit("amar_attr_add_data_fd", error);

	if (!amar_attr_close(attribute, &error))
	    error_exit("amar_attr_close", error);
	if (!amar_file_close(file, &error))
	    error_exit("amar_file_close", error);

	if (opt_verbose == 1) {
	    g_fprintf(output,"%s\n", argv[i]);
	} else if (opt_verbose > 1) {
	    g_fprintf(output,"%llu %s\n", (unsigned long long)filesize, argv[i]);
	}
	close(fd_in);
	i++;
    }

    if (!amar_close(archive, &error))
	error_exit("amar_close", error);
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
		&& 0 == strcmp(ud->argv[i], *file_data))
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
	char *filename = g_strdup_printf("%s.%d", (char *)file_data, attrid);
	fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0660);
	if (fd < 0) {
	    g_fprintf(stderr, _("Could not open '%s' for writing: %s"),
		    filename, strerror(errno));
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
    GError *error = NULL;
    int fd_in;
    amar_attr_handling_t handling[] = {
	{ 0, 0, extract_frag_cb, NULL },
    };
    struct read_user_data ud;

    ud.argv = argv;
    ud.argc = argc;
    ud.verbose = opt_verbose;

    if (opt_file && strcmp(opt_file,"-") != 0) {
	fd_in = open(opt_file, O_RDONLY);
	if (fd_in <= 0) {
	    error("open of '%s' failed: %s\n", opt_file, strerror(errno));
	}
    } else {
	fd_in = fileno(stdin);
    }

    archive = amar_new(fd_in, O_RDONLY, &error);
    if (!archive)
	error_exit("amar_new", error);

    if (!amar_read(archive, &ud, handling, extract_file_start_cb,
		   extract_file_finish_cb, &error)) {
	if (error)
	    error_exit("amar_read", error);
	else
	    /* one of the callbacks already printed an error message */
	    exit(1);
    }
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
    GError *error = NULL;
    int fd_in;
    amar_attr_handling_t handling[] = {
	{ 0, 0, NULL, NULL },
    };

    if (opt_file && strcmp(opt_file,"-") != 0) {
	fd_in = open(opt_file, O_RDONLY);
	if (fd_in <= 0) {
	    error("open of '%s' failed: %s\n", opt_file, strerror(errno));
	}
    } else {
	fd_in = fileno(stdin);
    }

    archive = amar_new(fd_in, O_RDONLY, &error);
    if (!archive)
	error_exit("amar_new", error);

    if (!amar_read(archive, NULL, handling, list_file_start_cb,
		   NULL, &error)) {
	if (error)
	    error_exit("amar_read", error);
	else
	    /* one of the callbacks already printed an error message */
	    exit(1);
    }
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
	case 5: opt_file = stralloc(optarg);
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
    if (opt_list > 1) {
	if (argc) {
	    g_fprintf(stderr, "--list does not take any additional filenames\n");
	    usage();
	}
    }

    if (opt_create > 0)
	do_create(opt_file, opt_verbose, argc, argv);
    else if (opt_extract > 0)
	do_extract(opt_file, opt_verbose, argc, argv);
    else if (opt_list > 0)
	do_list(opt_file, opt_verbose);

    return 0;
}
