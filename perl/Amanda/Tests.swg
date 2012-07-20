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

%module "Amanda::Tests"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Tests.pod"

%{
#include "simpleprng.h"
%}

%inline %{

/*
 * exercise bigint.c / integer.swg
 */

char *take_guint64(guint64 input) {
    if (input == G_MAXUINT64) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint64(gint64 input) {
    if (input == G_MAXINT64) return "MAX";
    if (input == G_MININT64) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_guint32(guint32 input) {
    if (input == G_MAXUINT32) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint32(gint32 input) {
    if (input == G_MAXINT32) return "MAX";
    if (input == G_MININT32) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_guint16(guint16 input) {
    if (input == G_MAXUINT16) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint16(gint16 input) {
    if (input == G_MAXINT16) return "MAX";
    if (input == G_MININT16) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_guint8(guint8 input) {
    if (input == G_MAXUINT8) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint8(gint8 input) {
    if (input == G_MAXINT8) return "MAX";
    if (input == G_MININT8) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}


guint64 give_guint64(char *input) {
    if (input[0] == '+') return G_MAXUINT64;
    return 0;
}

gint64 give_gint64(char *input) {
    if (input[0] == '-') return G_MININT64;
    if (input[0] == '+') return G_MAXINT64;
    return 0;
}

guint32 give_guint32(char *input) {
    if (input[0] == '+') return G_MAXUINT32;
    return 0;
}

gint32 give_gint32(char *input) {
    if (input[0] == '-') return G_MININT32;
    if (input[0] == '+') return G_MAXINT32;
    return 0;
}

guint16 give_guint16(char *input) {
    if (input[0] == '+') return G_MAXUINT16;
    return 0;
}

gint16 give_gint16(char *input) {
    if (input[0] == '-') return G_MININT16;
    if (input[0] == '+') return G_MAXINT16;
    return 0;
}

guint8 give_guint8(char *input) {
    if (input[0] == '+') return G_MAXUINT8;
    return 0;
}

gint8 give_gint8(char *input) {
    if (input[0] == '-') return G_MININT8;
    if (input[0] == '+') return G_MAXINT8;
    return 0;
}
%}

/*
 * Various compiler/system characteristics
 */

%inline %{

int sizeof_size_t(void) {
    return sizeof(size_t);
}

%}

/*
 * simpleprng interface
 */

%inline %{

/* write LENGTH bytes of random data to FILENAME, seeded with SEED */
void
write_random_file(guint32 seed, size_t length, char *filename) {
    simpleprng_state_t prng;
    int fd;
    char buf[10240];

    simpleprng_seed(&prng, seed);

    fd = open(filename, O_CREAT|O_WRONLY|O_TRUNC, 0666);
    if (fd < 0)
	g_critical(_("Could not open test file '%s': %s"), filename, strerror(errno));

    while (length) {
	size_t to_write = min(sizeof(buf), length);
	size_t written;

	simpleprng_fill_buffer(&prng, buf, to_write);

	written = full_write(fd, buf, to_write);
	if (written < to_write)
	    g_critical(_("Error writing test file: %s"), strerror(errno));

	length -= written;
    }

    close(fd);
}

/* read LENGTH bytes of random data from FILENAME verifying it against
 * a PRNG seeded with SEED.  Sends any error messages to stderr.
 *
 * If check_eof is true, then check that the file is exactly LENGTH bytes long;
 * otherwise, trailing bytes (such as zero padding from a Device) are ignored.
 */
gboolean
verify_random_file(guint32 seed, size_t length, char *filename, gboolean check_eof) {
    simpleprng_state_t prng;
    int fd;
    char buf[10240];

    simpleprng_seed(&prng, seed);

    fd = open(filename, O_RDONLY, 0666);
    if (fd < 0)
	g_critical(_("Could not open test file '%s': %s"), filename, strerror(errno));

    while (length) {
	size_t to_read = min(sizeof(buf), length);
	size_t bytes_read;

	bytes_read = full_read(fd, buf, to_read);
	if (bytes_read < to_read) {
	    if (errno) {
		g_critical(_("Error reading test file: %s"), strerror(errno));
	    } else {
		g_fprintf(stderr, _("Verify of '%s' failed: early EOF with %zd bytes left\n"),
			filename, length - bytes_read);
		goto error;
	    }
	}

	if (!simpleprng_verify_buffer(&prng, buf, bytes_read))
	    goto error;

	length -= bytes_read;
    }

    /* verify that the file contains no extra bytes */
    if (check_eof) {
	if (read(fd, buf, 1)) {
	    g_fprintf(stderr, _("Verify of '%s' failed: file is too long\n"), filename);
	    goto error;
	}
    }

    close(fd);
    return TRUE;

error:
    close(fd);
    return FALSE;
}
%}

/*
 * Simple threading test (start a thread and join it)
 */

%{
static gpointer
thread_fn(gpointer data)
{
    guint *d = data;
    *d = 1;
    return NULL;
}
%}

%inline %{
void
try_threads(void)
{
    guint data = 0;
    GThread *thd;

    glib_init();

    thd = g_thread_create(thread_fn, (gpointer)&data, TRUE, NULL);
    g_thread_join(thd);
    g_assert(data == 1);
}
%}
