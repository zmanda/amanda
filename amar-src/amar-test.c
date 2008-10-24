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

#include "amanda.h"
#include "amar.h"
#include "testutils.h"
#include "simpleprng.h"

static char *temp_filename = NULL;

/****
 * Macros for creating files with a particular structure
 */

#define WRITE_HEADER(fd, version) do { \
    char hdr[28]; \
    bzero(hdr, 28); \
    snprintf(hdr, 28, "AMANDA ARCHIVE FORMAT %d", (version)); \
    g_assert(full_write((fd), hdr, 28) == 28); \
} while(0);

#define WRITE_RECORD(fd, filenum, attrid, size, eoa, data) do { \
    struct { uint16_t f; uint16_t a; uint32_t s; } rec; \
    rec.f = htons((filenum)); \
    rec.a = htons((attrid)); \
    rec.s = htonl((size) | (eoa? 0x80000000 : 0)); \
    g_assert(full_write((fd), &rec, sizeof(rec)) == sizeof(rec)); \
    g_assert(full_write((fd), (data), (size)) == (size)); \
} while(0);

#define WRITE_RECORD_STR(fd, filenum, attrid, eoa, str) do { \
    size_t len = strlen((str)); \
    WRITE_RECORD((fd), (filenum), (attrid), len, (eoa), (str)); \
} while(0);

/****
 * Assertions for amanda_read_archive callbacks
 */

typedef enum {
    EXP_END,
    EXP_START_FILE,
    EXP_ATTRDATA,
    EXP_FINISH_FILE,
} expected_kind_t;

typedef struct {
    expected_kind_t kind;
    uint16_t filenum;
    uint16_t attrid;
    char *data;
    size_t datasize;
    gboolean multipart_ok;
    gboolean eoa;
    gboolean truncated;
    gboolean should_ignore;
    gboolean isstr;
} expected_step_t;

typedef struct {
    expected_step_t *steps;
    int curstep;
} expected_state_t;

#define EXPECT_START_FILE(filenum, data, datasize, should_ignore) \
    { EXP_START_FILE, (filenum), 0, (data), (datasize), 0, 0, 0, (should_ignore), 0 }

#define EXPECT_START_FILE_STR(filenum, filename, should_ignore) \
    { EXP_START_FILE, (filenum), 0, (filename), strlen((filename)), 0, 0, 0, (should_ignore), 1 }

#define EXPECT_ATTR_DATA(filenum, attrid, data, datasize, eoa, truncated) \
    { EXP_ATTRDATA, (filenum), (attrid), (data), (datasize), 0, (eoa), (truncated), 0, 0 }

#define EXPECT_ATTR_DATA_MULTIPART(filenum, attrid, data, datasize, eoa, truncated) \
    { EXP_ATTRDATA, (filenum), (attrid), (data), (datasize), 1, (eoa), (truncated), 0, 0 }

#define EXPECT_ATTR_DATA_STR(filenum, attrid, datastr, eoa, truncated) \
    { EXP_ATTRDATA, (filenum), (attrid), (datastr), strlen((datastr)), 0, (eoa), (truncated), 0, 1 }

#define EXPECT_FINISH_FILE(filenum, truncated) \
    { EXP_FINISH_FILE, (filenum), 0, 0, 0, 0, 0, (truncated), 0, 0 }

#define EXPECT_END() \
    { EXP_END, 0, 0, 0, 0, 0, 0, 0, 0, 0 }

#define EXPECT_FAILURE(fmt, ...) do { \
    fprintf(stderr, fmt "\n", __VA_ARGS__); \
    exit(1); \
} while(0)

static gboolean
file_start_cb(
	gpointer user_data,
	uint16_t filenum,
	gpointer filename,
	gsize filename_len,
	gboolean *ignore,
	gpointer *file_data G_GNUC_UNUSED)
{
    expected_state_t *state = user_data;
    expected_step_t *step = state->steps + state->curstep;

    tu_dbg("file_start_cb(NULL, %d, '%s', %zd, .., ..)\n",
	(int)filenum, (char *)filename, filename_len);

    if (step->kind != EXP_START_FILE)
	EXPECT_FAILURE("step %d: unexpected new file with fileid %d",
			state->curstep, (int)filenum);

    if (step->filenum != filenum)
	EXPECT_FAILURE("step %d: expected new file with filenum %d; got filenum %d",
			state->curstep, (int)step->filenum, (int)filenum);

    if (filename_len != step->datasize)
	EXPECT_FAILURE("step %d: filename lengths do not match: got %zd, expected %zd",
			state->curstep, filename_len, step->datasize);

    if (memcmp(filename, step->data, filename_len)) {
	if (step->isstr) {
	    EXPECT_FAILURE("step %d: new file's filename does not match: got '%*s', expected '%*s'",
			    state->curstep, (int)filename_len, (char *)filename,
			    (int)step->datasize, (char *)step->data);
	} else {
	    EXPECT_FAILURE("step %d: new file's filename does not match",
			    state->curstep);
	}
    }

    *ignore = step->should_ignore;
    state->curstep++;

    return TRUE;
}

static gboolean
file_finish_cb(
	gpointer user_data,
	uint16_t filenum,
	gpointer *file_data G_GNUC_UNUSED,
	gboolean truncated)
{
    expected_state_t *state = user_data;
    expected_step_t *step = state->steps + state->curstep;

    tu_dbg("file_finish_cb(NULL, %d, NULL, %d)\n",
	(int)filenum, truncated);

    if (step->kind != EXP_FINISH_FILE)
	EXPECT_FAILURE("step %d: unexpected file finish with fileid %d",
			state->curstep, (int)filenum);

    if (step->truncated && !truncated)
	EXPECT_FAILURE("step %d: file %d was unexpectedly not truncated",
			state->curstep, (int)filenum);

    if (step->truncated && !truncated)
	EXPECT_FAILURE("step %d: file %d was unexpectedly truncated",
			state->curstep, (int)filenum);

    state->curstep++;

    return TRUE;
}

static gboolean
frag_cb(
	gpointer user_data,
	uint16_t filenum,
	gpointer file_data G_GNUC_UNUSED,
	uint16_t attrid,
	gpointer attrid_data G_GNUC_UNUSED,
	gpointer *attr_data G_GNUC_UNUSED,
	gpointer data,
	gsize datasize,
	gboolean eoa,
	gboolean truncated)
{
    expected_state_t *state = user_data;
    expected_step_t *step = state->steps + state->curstep;

    tu_dbg("file_finish_cb(NULL, %d, NULL, %d, %p, %zd, %d, %d)\n",
	(int)filenum, (int)attrid, data, datasize, eoa, truncated);

    if (step->kind != EXP_ATTRDATA)
	EXPECT_FAILURE("step %d: unexpected attribute data with fileid %d, attrid %d",
			state->curstep, (int)filenum, (int)attrid);

    if (step->filenum != filenum)
	EXPECT_FAILURE("step %d: expected attribute data with filenum %d; got filenum %d",
			state->curstep, (int)step->filenum, (int)filenum);

    if (step->attrid != attrid)
	EXPECT_FAILURE("step %d: expected attribute data with attrid %d; got attrid %d",
			state->curstep, (int)step->attrid, (int)attrid);

    /* if we're accepting multiple fragments of the attribute here (due to internal
     * buffering by the reader), then handle that specially */
    if (step->multipart_ok && datasize < step->datasize) {
	if (eoa)
	    EXPECT_FAILURE("step %d: file %d attribute %d: early EOA in multipart attribute",
			    state->curstep, (int)filenum, (int)attrid);

	if (memcmp(data, step->data, datasize)) {
	    EXPECT_FAILURE("step %d: attribute's data does not match",
			    state->curstep);
	}
	step->data += datasize;
	step->datasize -= datasize;
	return TRUE;
    }

    if (step->eoa && !eoa)
	EXPECT_FAILURE("step %d: file %d attribute %d: expected EOA did not appear",
			state->curstep, (int)filenum, (int)attrid);

    if (!step->eoa && eoa)
	EXPECT_FAILURE("step %d: file %d attribute %d: unexpected EOA",
			state->curstep, (int)filenum, (int)attrid);

    if (!step->truncated && truncated)
	EXPECT_FAILURE("step %d: file %d attribute %d was unexpectedly truncated",
			state->curstep, (int)filenum, (int)attrid);

    if (step->truncated && !truncated)
	EXPECT_FAILURE("step %d: file %d attribute %d was unexpectedly not truncated",
			state->curstep, (int)filenum, (int)attrid);

    if (datasize != step->datasize)
	EXPECT_FAILURE("step %d: file %d attribute %d lengths do not match: "
		       "got %zd, expected %zd",
			state->curstep, (int)filenum, (int)attrid,
			datasize, step->datasize);

    if (memcmp(data, step->data, datasize)) {
	if (step->isstr) {
	    EXPECT_FAILURE("step %d: attribute's data does not match: got '%*s', expected '%*s'",
			    state->curstep, (int)datasize, (char *)data,
			    (int)step->datasize, (char *)step->data);
	} else {
	    EXPECT_FAILURE("step %d: attribute's data does not match",
			    state->curstep);
	}
    }

    state->curstep++;

    return TRUE;
}

/****
 * Utilities
 */

static int
open_temp(gboolean write)
{
    int fd = open(temp_filename, write? O_WRONLY|O_CREAT|O_TRUNC : O_RDONLY, 0777);
    if (fd < 0) {
	perror("open temporary file");
	exit(1);
    }

    return fd;
}

static void
check_gerror_(
    gboolean ok,
    GError *error,
    const char *fn)
{
    if (ok && !error)
	return;

    if (ok)
	EXPECT_FAILURE(
		"'%s' set 'error' but did not indicate an error condition: %s (%s)\n",
		fn, error->message, strerror(error->code));
    else if (!error)
	EXPECT_FAILURE(
		"'%s' indicated an error condition but did not set 'error'.\n", fn);
    else
	EXPECT_FAILURE(
		"'%s' error: %s (%s)\n", fn, error->message, strerror(error->code));

    exit(1);
}

#define check_gerror(ok, error, fn) check_gerror_((ok)!=0, (error), (fn))

static void
check_gerror_matches_(
    gboolean ok,
    GError *error,
    const char *matches,
    const char *fn)
{
    if (!ok && error) {
	if (0 != strcmp(matches, error->message)) {
	    EXPECT_FAILURE(
		    "%s produced error '%s' but expected '%s'\n",
		    fn, error->message, matches);
	    exit(1);
	}
	return;
    }

    if (ok)
	EXPECT_FAILURE(
		"'%s' correctly set 'error' but did not indicate an error condition: %s (%s)\n",
		fn, error->message, strerror(error->code));
    else /* (!error) */
	EXPECT_FAILURE(
		"'%s' correctly indicated an error condition but did not set 'error'.\n", fn);

    exit(1);
}

#define check_gerror_matches(ok, error, match, fn) \
    check_gerror_matches_((ok)!=0, (error), (match), (fn))

static void
try_reading_fd(
	expected_step_t *steps,
	amar_attr_handling_t *handling,
        int fd)
{
    amar_t *ar;
    expected_state_t state = { steps, 0 };
    GError *error = NULL;
    gboolean ok;

    ar = amar_new(fd, O_RDONLY, &error);
    check_gerror(ar, error, "amar_new");
    ok = amar_read(ar, &state, handling, file_start_cb, file_finish_cb, &error);
    if (ok || error)
	check_gerror(ok, error, "amar_read");
    if (steps[state.curstep].kind != EXP_END)
	EXPECT_FAILURE("Stopped reading early at step %d", state.curstep);
    ok = amar_close(ar, &error);
    check_gerror(ok, error, "amar_close");
}

static void
try_reading(
	expected_step_t *steps,
	amar_attr_handling_t *handling)
{
    int fd;

    fd = open_temp(0);
    try_reading_fd(steps, handling, fd);
    close(fd);
}

static void
try_reading_with_error(
	expected_step_t *steps,
	amar_attr_handling_t *handling,
	const char *message)
{
    amar_t *ar;
    expected_state_t state = { steps, 0 };
    int fd;
    GError *error = NULL;
    gboolean ok;

    fd = open_temp(0);
    ar = amar_new(fd, O_RDONLY, &error);
    check_gerror(ar, error, "amar_new");
    ok = amar_read(ar, &state, handling, file_start_cb, file_finish_cb, &error);
    check_gerror_matches(ok, error, message, "amar_read");
    amar_close(ar, NULL);
    close(fd);
}

/****
 * Test various valid inputs
 */

static int
test_simple_read(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "/first/filename");
    WRITE_RECORD_STR(fd, 1, 18, 1, "eighteen");
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, 19, 0, "nine");
    WRITE_RECORD_STR(fd, 1, 20, 0, "twen");
    WRITE_RECORD_STR(fd, 1, 19, 1, "teen");
    WRITE_RECORD_STR(fd, 1, 20, 1, "ty");
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_EOF, 1, "");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 19, 256, frag_cb, NULL }, /* reassemble this attribute */
	    { 20, 0, frag_cb, NULL }, /* but pass along each fragment of this */
	    { 0, 256, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "/first/filename", 0),
	    EXPECT_ATTR_DATA_STR(1, 18, "eighteen", 1, 0),
	    EXPECT_ATTR_DATA_STR(1, 20, "twen", 0, 0),
	    EXPECT_ATTR_DATA_STR(1, 19, "nineteen", 1, 0),
	    EXPECT_ATTR_DATA_STR(1, 20, "ty", 1, 0),
	    EXPECT_FINISH_FILE(1, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

static int
test_read_buffering(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "file1");
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 2, AMAR_ATTR_FILENAME, 1, "file2");
    WRITE_RECORD_STR(fd, 2, 19, 0, "1"); /* one byte at a time, for 12 bytes */
    WRITE_RECORD_STR(fd, 2, 19, 0, "9");
    WRITE_RECORD_STR(fd, 2, 21, 1, "012345678901234567890123456789"); /* thirty bytes exactly */
    WRITE_RECORD_STR(fd, 2, 19, 0, "1");
    WRITE_RECORD_STR(fd, 1, 18, 0, "ATTR");
    WRITE_RECORD_STR(fd, 2, 19, 0, "9");
    WRITE_RECORD_STR(fd, 2, 19, 0, "1");
    WRITE_RECORD_STR(fd, 2, 19, 0, "9");
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, 20, 0, "TWENTYTWE"); /* nine bytes, then three in the next frag */
    WRITE_RECORD_STR(fd, 2, 19, 0, "1");
    WRITE_RECORD_STR(fd, 1, 20, 1, "NTY");
    WRITE_RECORD_STR(fd, 2, 19, 0, "9");
    WRITE_RECORD_STR(fd, 1, 18, 0, "181818"); /* hit ten bytes exactly */
    WRITE_RECORD_STR(fd, 2, 19, 0, "1");
    WRITE_RECORD_STR(fd, 2, 19, 0, "9");
    WRITE_RECORD_STR(fd, 1, 18, 0, "ATTR");
    WRITE_RECORD_STR(fd, 1, 22, 0, "012345678"); /* nine bytes followed by 20 */
    WRITE_RECORD_STR(fd, 1, 18, 1, "18");
    WRITE_RECORD_STR(fd, 1, 22, 1, "01234567890123456789");
    WRITE_RECORD_STR(fd, 2, 19, 0, "1");
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_EOF, 1, "");
    WRITE_RECORD_STR(fd, 2, 19, 1, "9");
    WRITE_RECORD_STR(fd, 2, AMAR_ATTR_EOF, 1, "");

    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 10, frag_cb, NULL },	/* reassemble all fragments in 10-byte chunks */
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "file1", 0),
	    EXPECT_START_FILE_STR(2, "file2", 0),
	    EXPECT_ATTR_DATA_STR(2, 21, "012345678901234567890123456789", 1, 0),
	    EXPECT_ATTR_DATA_STR(1, 20, "TWENTYTWENTY", 1, 0),
	    EXPECT_ATTR_DATA_STR(1, 18, "ATTR181818", 0, 0),
	    EXPECT_ATTR_DATA_STR(2, 19, "1919191919", 0, 0),
	    EXPECT_ATTR_DATA_STR(1, 18, "ATTR18", 1, 0),
	    EXPECT_ATTR_DATA_STR(1, 22, "01234567801234567890123456789", 1, 0),
	    EXPECT_FINISH_FILE(1, 0),
	    EXPECT_ATTR_DATA_STR(2, 19, "19", 1, 0),
	    EXPECT_FINISH_FILE(2, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

static int
test_missing_eoa(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "file1");
    WRITE_RECORD_STR(fd, 1, 21, 0, "attribu"); /* note no EOA */
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_EOF, 1, "");

    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 1024, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "file1", 0),
	    EXPECT_ATTR_DATA_STR(1, 21, "attribu", 1, 1),
	    EXPECT_FINISH_FILE(1, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

static int
test_ignore(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "file1");
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 2, AMAR_ATTR_FILENAME, 1, "file2");
    WRITE_RECORD_STR(fd, 2, 20, 1, "attr20");
    WRITE_RECORD_STR(fd, 1, 21, 0, "attr");
    WRITE_RECORD_STR(fd, 1, 21, 1, "21");
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 3, AMAR_ATTR_FILENAME, 1, "file3");
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 4, AMAR_ATTR_FILENAME, 1, "file4");
    WRITE_RECORD_STR(fd, 3, 22, 1, "attr22");
    WRITE_RECORD_STR(fd, 4, 23, 1, "attr23");
    WRITE_RECORD_STR(fd, 4, AMAR_ATTR_EOF, 1, "");
    WRITE_RECORD_STR(fd, 3, AMAR_ATTR_EOF, 1, "");
    WRITE_RECORD_STR(fd, 2, AMAR_ATTR_EOF, 1, "");
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_EOF, 1, "");

    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 10, frag_cb, NULL },	/* reassemble all fragments in 10-byte chunks */
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "file1", 1),
	    EXPECT_START_FILE_STR(2, "file2", 0),
	    EXPECT_ATTR_DATA_STR(2, 20, "attr20", 1, 0),
	    EXPECT_START_FILE_STR(3, "file3", 1),
	    EXPECT_START_FILE_STR(4, "file4", 0),
	    EXPECT_ATTR_DATA_STR(4, 23, "attr23", 1, 0),
	    EXPECT_FINISH_FILE(4, 0),
	    EXPECT_FINISH_FILE(2, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

static int
test_missing_eof(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "file!");
    WRITE_RECORD_STR(fd, 1, 20, 1, "attribute");
    WRITE_RECORD_STR(fd, 1, 21, 0, "attribu"); /* note no EOA */

    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 1024, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "file!", 0),
	    EXPECT_ATTR_DATA_STR(1, 20, "attribute", 1, 0),
	    EXPECT_ATTR_DATA_STR(1, 21, "attribu", 1, 1),
	    EXPECT_FINISH_FILE(1, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

static int
test_extra_records(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 4, AMAR_ATTR_EOF, 1, "");
    WRITE_RECORD_STR(fd, 5, 20, 1, "old attribute");
    WRITE_RECORD_STR(fd, 6, AMAR_ATTR_FILENAME, 1, "file!");
    WRITE_RECORD_STR(fd, 6, 21, 0, "attribu"); /* note no EOA */
    WRITE_RECORD_STR(fd, 5, AMAR_ATTR_EOF, 1, "");
    WRITE_RECORD_STR(fd, 6, 21, 1, "te");
    WRITE_RECORD_STR(fd, 6, AMAR_ATTR_EOF, 1, "");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 1024, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(6, "file!", 0),
	    EXPECT_ATTR_DATA_STR(6, 21, "attribute", 1, 0),
	    EXPECT_FINISH_FILE(6, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

static gboolean
early_exit_frag_cb(
	gpointer user_data G_GNUC_UNUSED,
	uint16_t filenum G_GNUC_UNUSED,
	gpointer file_data G_GNUC_UNUSED,
	uint16_t attrid G_GNUC_UNUSED,
	gpointer attrid_data G_GNUC_UNUSED,
	gpointer *attr_data G_GNUC_UNUSED,
	gpointer data G_GNUC_UNUSED,
	gsize datasize G_GNUC_UNUSED,
	gboolean eoa G_GNUC_UNUSED,
	gboolean truncated G_GNUC_UNUSED)
{
    return FALSE;
}

static int
test_early_exit(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 6, AMAR_ATTR_FILENAME, 1, "file!");
    WRITE_RECORD_STR(fd, 6, 21, 1, "attribu");
    WRITE_RECORD_STR(fd, 6, AMAR_ATTR_EOF, 1, "");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 0, early_exit_frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(6, "file!", 0),
	    EXPECT_FINISH_FILE(6, 1),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

/****
 * Test the write side, using round trips.
 */

/* just try to execute most of the writing code */
static int
test_writing_coverage(void)
{
    int fd, fd2;
    off_t posn, fdsize;
    char buf[16300];
    char buf2[16300];
    char *bigbuf;
    size_t bigbuf_size = 1024*50+93;
    simpleprng_state_t prng;
    gsize i;
    guint16 attrid = 20;
    amar_t *arch = NULL;
    amar_file_t *af = NULL, *af2 = NULL;
    amar_attr_t *at = NULL, *at2 = NULL;
    GError *error = NULL;
    gboolean ok;

    /* set up some data buffers */
    for (i = 0; i < sizeof(buf); i++) {
	buf[i] = 0xfe;
	buf2[i] = 0xaa;
    }

    bigbuf = g_malloc(bigbuf_size);
    simpleprng_seed(&prng, 0xfeaa);
    simpleprng_fill_buffer(&prng, bigbuf, bigbuf_size);

    fd = open("amar-test.big", O_CREAT|O_WRONLY|O_TRUNC, 0777);
    g_assert(fd >= 0);
    g_assert(full_write(fd, bigbuf, bigbuf_size) == bigbuf_size);
    close(fd);

    fd = open_temp(1);

    arch = amar_new(fd, O_WRONLY, &error);
    check_gerror(arch, error, "amar_new");
    g_assert(arch != NULL);

    af = amar_new_file(arch, "MyFile", 0, &posn, &error);
    check_gerror(af, error, "amar_new_file");
    tu_dbg("MyFile starts at 0x%x\n", (int)posn)
    g_assert(af != NULL);

    /* by character with EOA */
    at = amar_new_attr(af, attrid++, &error);
    check_gerror(at, error, "amar_new_attr");
    g_assert(at != NULL);
    ok = amar_attr_add_data_buffer(at, buf, sizeof(buf), 1, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_close(at, &error);
    check_gerror(ok, error, "amar_attr_close");

    /* by character without EOA */
    at = amar_new_attr(af, attrid++, &error);
    check_gerror(at, error, "amar_new_attr");
    g_assert(at != NULL);
    ok = amar_attr_add_data_buffer(at, buf2, sizeof(buf2), 0, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_close(at, &error);
    check_gerror(ok, error, "amar_attr_close");

    /* open up a new file, for fun */
    af2 = amar_new_file(arch, "MyOtherFile", 0, &posn, &error);
    check_gerror(af2, error, "amar_new_file");
    tu_dbg("MyOtherFile starts at 0x%x\n", (int)posn)

    /* by file descriptor, to the first file */
    at = amar_new_attr(af, attrid++, &error);
    check_gerror(at, error, "amar_new_attr");
    fd2 = open("amar-test.big", O_RDONLY);
    g_assert(fd2 >= 0);
    fdsize = amar_attr_add_data_fd(at, fd2, 0, &error);
    check_gerror(fdsize != -1, error, "amar_attr_add_data_fd");
    g_assert(fdsize > 0);
    close(fd2);
    unlink("amar-test.big");
    ok = amar_attr_close(at, &error);
    check_gerror(ok, error, "amar_attr_close");

    ok = amar_file_close(af, &error);
    check_gerror(ok, error, "amar_file_close");

    /* interlaeave two attributes */
    at = amar_new_attr(af2, attrid++, &error);
    check_gerror(at, error, "amar_new_attr");
    at2 = amar_new_attr(af2, attrid++, &error);
    check_gerror(at2, error, "amar_new_attr");
    ok = amar_attr_add_data_buffer(at, buf, 72, 0, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_add_data_buffer(at2, buf2, 72, 0, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_add_data_buffer(at, buf, 13, 0, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_add_data_buffer(at2, buf2, 13, 1, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_close(at, &error);
    check_gerror(ok, error, "amar_attr_close");
    ok = amar_attr_close(at2, &error);
    check_gerror(ok, error, "amar_attr_close");

    ok = amar_file_close(af2, &error);
    check_gerror(ok, error, "amar_file_close");

    ok = amar_close(arch, &error);
    check_gerror(ok, error, "amar_close");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 22, bigbuf_size+1, frag_cb, NULL }, /* buffer the big attr */
	    { 0, 0, frag_cb, NULL },	/* don't buffer other records */
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "MyFile", 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 20, buf, sizeof(buf), 1, 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 21, buf2, sizeof(buf2), 0, 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 21, buf2, 0, 1, 0), /* trailing EOA */
	    EXPECT_START_FILE_STR(2, "MyOtherFile", 0),
	    EXPECT_ATTR_DATA(1, 22, bigbuf, bigbuf_size, 1, 0),
	    EXPECT_FINISH_FILE(1, 0),
	    EXPECT_ATTR_DATA_MULTIPART(2, 23, buf, 72, 0, 0),
	    EXPECT_ATTR_DATA_MULTIPART(2, 24, buf2, 72, 0, 0),
	    EXPECT_ATTR_DATA_MULTIPART(2, 23, buf+72, 13, 0, 0),
	    EXPECT_ATTR_DATA_MULTIPART(2, 24, buf2+72, 13, 1, 0),
	    EXPECT_ATTR_DATA_MULTIPART(2, 23, buf, 0, 1, 0),
	    EXPECT_FINISH_FILE(2, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

/* test big attributes */
static int
test_big_attr(void)
{
    int fd, fd2;
    off_t fdsize;
    char *bigbuf;
    const size_t max_record_data_size = 4*1024*1024;
    size_t bigbuf_size = max_record_data_size + 1274; /* a record and a bit */
    simpleprng_state_t prng;
    guint16 attrid = 20;
    amar_t *arch = NULL;
    amar_file_t *af = NULL;
    amar_attr_t *at = NULL;
    GError *error = NULL;
    gboolean ok;

    /* set up some data buffers */
    bigbuf = g_malloc(bigbuf_size);
    simpleprng_seed(&prng, 0xb001);
    simpleprng_fill_buffer(&prng, bigbuf, bigbuf_size);

    fd = open("amar-test.big", O_CREAT|O_WRONLY|O_TRUNC, 0777);
    g_assert(fd >= 0);
    g_assert(full_write(fd, bigbuf, bigbuf_size) == bigbuf_size);
    close(fd);

    fd = open_temp(1);

    arch = amar_new(fd, O_WRONLY, &error);
    check_gerror(arch, error, "amar_new");

    af = amar_new_file(arch, "bigstuff", 0, NULL, &error);
    check_gerror(af, error, "amar_new_file");

    /* by character */
    at = amar_new_attr(af, attrid++, &error);
    check_gerror(at, error, "amar_new_attr");
    ok = amar_attr_add_data_buffer(at, bigbuf, bigbuf_size, 1, &error);
    check_gerror(ok, error, "amar_attr_add_data_buffer");
    ok = amar_attr_close(at, &error);
    check_gerror(ok, error, "amar_attr_close");

    /* by file descriptor */
    at = amar_new_attr(af, attrid++, &error);
    check_gerror(at, error, "amar_new_attr");
    fd2 = open("amar-test.big", O_RDONLY);
    g_assert(fd2 >= 0);
    fdsize = amar_attr_add_data_fd(at, fd2, 1, &error);
    check_gerror(fdsize != -1, error, "amar_attr_add_data_fd");
    g_assert(fdsize > 0);
    close(fd2);
    unlink("amar-test.big");
    ok = amar_attr_close(at, &error);
    check_gerror(ok, error, "amar_attr_close");

    ok = amar_file_close(af, &error);
    check_gerror(ok, error, "amar_file_close");

    ok = amar_close(arch, &error);
    check_gerror(ok, error, "amar_close");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 0, frag_cb, NULL },	/* don't buffer records */
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "bigstuff", 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 20, bigbuf, max_record_data_size, 0, 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 20, bigbuf+max_record_data_size, bigbuf_size-max_record_data_size, 1, 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 21, bigbuf, max_record_data_size, 0, 0),
	    EXPECT_ATTR_DATA_MULTIPART(1, 21, bigbuf+max_record_data_size, bigbuf_size-max_record_data_size, 1, 0),
	    EXPECT_FINISH_FILE(1, 0),
	    EXPECT_END(),
	};
	try_reading(steps, handling);
    }

    return 1;
}

/* like test_big_attr, but using a pipe and ignoring one of the attrs in hopes
 * of triggering an lseek(), which will fail on a pipe. */
static int
test_pipe(void)
{
    int fd;
    int p[2];
    off_t fdsize;
    char *bigbuf;
    const size_t max_record_data_size = 4*1024*1024;
    size_t bigbuf_size = max_record_data_size + 1274; /* a record and a bit */
    simpleprng_state_t prng;
    guint16 attrid = 20;
    amar_t *arch = NULL;
    amar_file_t *af = NULL;
    amar_attr_t *at = NULL;
    GError *error = NULL;
    gboolean ok;

    /* set up some data buffers */
    bigbuf = g_malloc(bigbuf_size);
    simpleprng_seed(&prng, 0xb001);
    simpleprng_fill_buffer(&prng, bigbuf, bigbuf_size);

    fd = open("amar-test.big", O_CREAT|O_WRONLY|O_TRUNC, 0777);
    g_assert(fd >= 0);
    g_assert(full_write(fd, bigbuf, bigbuf_size) == bigbuf_size);
    close(fd);

    g_assert(pipe(p) >= 0);

    switch (fork()) {
	case 0: /* child */
	    close(p[0]);
	    arch = amar_new(p[1], O_WRONLY, &error);
	    check_gerror(arch, error, "amar_new");
	    g_assert(arch != NULL);

	    af = amar_new_file(arch, "bigstuff", 0, NULL, &error);
	    check_gerror(af, error, "amar_new_file");

	    /* by character */
	    at = amar_new_attr(af, attrid++, &error);
	    check_gerror(at, error, "amar_new_attr");
	    ok = amar_attr_add_data_buffer(at, bigbuf, bigbuf_size, 1, &error);
	    check_gerror(ok, error, "amar_attr_add_data_buffer");
	    ok = amar_attr_close(at, &error);
	    check_gerror(ok, error, "amar_attr_close");

	    /* by file descriptor */
	    at = amar_new_attr(af, attrid++, &error);
	    check_gerror(at, error, "amar_new_attr");
	    fd = open("amar-test.big", O_RDONLY);
	    g_assert(fd >= 0);
	    fdsize = amar_attr_add_data_fd(at, fd, 1, &error);
	    check_gerror(fdsize != -1, error, "amar_attr_add_data_fd");
	    g_assert(fdsize > 0);
	    close(fd);
	    unlink("amar-test.big");
	    ok = amar_attr_close(at, &error);
	    check_gerror(ok, error, "amar_attr_close");

	    ok = amar_file_close(af, &error);
	    check_gerror(ok, error, "amar_file_close");

	    ok = amar_close(arch, &error);
	    check_gerror(ok, error, "amar_close");
	    close(p[1]);
	    exit(0);

	case -1:
	    perror("fork");
	    exit(1);

	default: { /* parent */
	    amar_attr_handling_t handling[] = {
		{ 20, 0, NULL, NULL },		/* ignore attr 20 */
		{ 0, 0, frag_cb, NULL },	/* don't buffer records */
	    };
	    expected_step_t steps[] = {
		EXPECT_START_FILE_STR(1, "bigstuff", 0),
		EXPECT_ATTR_DATA_MULTIPART(1, 21, bigbuf, max_record_data_size, 0, 0),
		EXPECT_ATTR_DATA_MULTIPART(1, 21, bigbuf+max_record_data_size, bigbuf_size-max_record_data_size, 1, 0),
		EXPECT_FINISH_FILE(1, 0),
		EXPECT_END(),
	    };
            int status;
	    close(p[1]);
	    try_reading_fd(steps, handling, p[0]);
	    close(p[0]);
            wait(&status);
            if(WIFSIGNALED(status)) {
                printf("child was terminated by signal %d\n", WTERMSIG(status));
                exit(1);
            }
	}
    }

    return 1;
}

/****
 * Invalid inputs - test error returns
 */

static int
test_no_header(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "/first/filename");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 0, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_END(),
	};
	try_reading_with_error(steps, handling,
		    "Archive read does not begin at a header record");
    }

    return 1;
}

static int
test_invalid_eof(void)
{
    int fd;

    fd = open_temp(1);
    WRITE_HEADER(fd, 1);
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_FILENAME, 1, "hi");
    WRITE_RECORD_STR(fd, 1, AMAR_ATTR_EOF, 1, "abc");
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 0, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_START_FILE_STR(1, "hi", 0),
	    EXPECT_END(),
	};
	try_reading_with_error(steps, handling,
		    "Archive contains an EOF record with nonzero size");
    }

    return 1;
}

static int
test_header_vers(void)
{
    int fd;
    char hdr[32];

    bzero(hdr, sizeof(hdr));
    strcpy(hdr, "AMANDA ARCHIVE FORMAT 2");

    fd = open_temp(1);
    if (full_write(fd, hdr, sizeof(hdr)) < sizeof(hdr)) {
	perror("full_write");
	exit(1);
    }
    close(fd);

    {
	amar_attr_handling_t handling[] = {
	    { 0, 0, frag_cb, NULL },
	};
	expected_step_t steps[] = {
	    EXPECT_END(),
	};
	try_reading_with_error(steps, handling,
		    "Archive version 2 is not supported");
    }

    return 1;
}

/****
 * Driver
 */

int
main(int argc, char **argv)
{
    int rv;
    char *cwd = g_get_current_dir();
    static TestUtilsTest tests[] = {
	TU_TEST(test_simple_read, 10),
	TU_TEST(test_read_buffering, 10),
	TU_TEST(test_missing_eoa, 10),
	TU_TEST(test_ignore, 10),
	TU_TEST(test_missing_eof, 10),
	TU_TEST(test_extra_records, 10),
	TU_TEST(test_early_exit, 10),
	TU_TEST(test_writing_coverage, 10),
	TU_TEST(test_big_attr, 20),
	TU_TEST(test_pipe, 20),
	TU_TEST(test_no_header, 10),
	TU_TEST(test_invalid_eof, 10),
	TU_TEST(test_header_vers, 10),
	TU_END()
    };

    temp_filename = vstralloc(cwd, "/amar-test.tmp", NULL);

    rv = testutils_run_tests(argc, argv, tests);
    unlink(temp_filename);
    return rv;
}
