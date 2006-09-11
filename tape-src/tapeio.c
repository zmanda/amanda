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
 * $Id: tapeio.c,v 1.57 2006/07/06 15:04:18 martinea Exp $
 *
 * implements generic tape I/O functions
 */

#include "amanda.h"
#include "tapeio.h"
#include "fileheader.h"

#ifndef R_OK
#define R_OK 4
#define W_OK 2
#endif

#include "output-tape.h"
#include "output-null.h"
#include "output-rait.h"
#include "output-file.h"

static struct virtualtape {
    char *prefix;
    int (*xxx_tape_access)(char *, int);
    int (*xxx_tape_open)(char *, int, mode_t);
    int (*xxx_tape_stat)(char *, struct stat *);
    int (*xxx_tapefd_close)(int);
    int (*xxx_tapefd_fsf)(int, off_t);
    ssize_t (*xxx_tapefd_read)(int, void *, size_t);
    int (*xxx_tapefd_rewind)(int);
    void (*xxx_tapefd_resetofs)(int);
    int (*xxx_tapefd_unload)(int);
    int (*xxx_tapefd_status)(int, struct am_mt_status *);
    int (*xxx_tapefd_weof)(int, off_t);
    ssize_t (*xxx_tapefd_write)(int, const void *, size_t);
    int (*xxx_tapefd_can_fork)(int);
} vtable[] = {
  /* note: "tape" has to be the first entry because it is the
  **        default if no prefix match is found.
  */
  {"tape", tape_tape_access, tape_tape_open, tape_tape_stat,
	tape_tapefd_close, tape_tapefd_fsf,
	tape_tapefd_read, tape_tapefd_rewind, tape_tapefd_resetofs,
	tape_tapefd_unload, tape_tapefd_status, tape_tapefd_weof,
        tape_tapefd_write, tape_tapefd_can_fork },
  {"null", null_tape_access, null_tape_open, null_tape_stat,
	null_tapefd_close, null_tapefd_fsf,
	null_tapefd_read, null_tapefd_rewind, null_tapefd_resetofs,
	null_tapefd_unload, null_tapefd_status, null_tapefd_weof,
        null_tapefd_write, null_tapefd_can_fork },
  {"rait", rait_access, rait_tape_open, rait_stat,
	rait_close, rait_tapefd_fsf,
	rait_read, rait_tapefd_rewind, rait_tapefd_resetofs,
	rait_tapefd_unload, rait_tapefd_status, rait_tapefd_weof,
        rait_write, rait_tapefd_can_fork },
  {"file", file_tape_access, file_tape_open, file_tape_stat,
	file_tapefd_close, file_tapefd_fsf,
	file_tapefd_read, file_tapefd_rewind, file_tapefd_resetofs,
	file_tapefd_unload, file_tapefd_status, file_tapefd_weof,
        file_tapefd_write, file_tapefd_can_fork },
  {NULL, NULL, NULL, NULL,
        NULL, NULL,
	NULL, NULL, NULL,
	NULL, NULL, NULL,
	NULL, NULL}
};

static struct tape_info {
    int vtape_index;
    char *host;
    char *disk;
    int level;
    char *datestamp;
    off_t length;
    char *tapetype;
    int fake_label;
    int ioctl_fork;
    int master_fd;
} *tape_info = NULL;
static struct tape_info **tape_info_p = &tape_info;

static size_t tape_info_count = 0;

static char *errstr = NULL;

static void tape_info_init(void *ptr);
static int name2slot(char *name, char **ntrans);

/*
 * Additional initialization function for tape_info table.
 */

static void
tape_info_init(
    void *ptr)
{
    struct tape_info *t = ptr;

    t->level = -1;
    t->vtape_index = -1;
    t->ioctl_fork = 1;
    t->master_fd = -1;
}

/*
 * Convert the "name" part of a device to a vtape slot.
 */

static int
name2slot(
    char *name,
    char **ntrans)
{
    char *pc;
    size_t len;
    int i;

    if(0 != (pc = strchr(name, ':'))) {
        len = (size_t)(pc - name);
	for( i = 0 ; vtable[i].prefix && vtable[i].prefix[0]; i++ ) {
	    if(0 == strncmp(vtable[i].prefix, name , len)
		&& '\0' == vtable[i].prefix[len]) {
		*ntrans = pc + 1;
		return i;
            }
        }
    }
    *ntrans = name;
    return 0;
}

/*
 * Routines for parsing a device name.
 */

/*
 * Initialize parsing.  The text in the "dev" parameter will be altered,
 * so a copy should be passed to us.
 */

int
tapeio_init_devname(
    char * dev,
    char **dev_left,
    char **dev_right,
    char **dev_next)
{
    int ch;
    char *p;
    int depth;

    *dev_left = *dev_right = *dev_next = NULL;	/* defensive coding */

    /*
     * See if there is a '{' and find the matching '}'.
     */
    if((*dev_next = p = strchr(dev, '{')) != NULL) {
	depth = 1;
	p++;
	while(depth > 0) {
	    ch = *p++;
	    while((ch != '\0') && (ch != '{') && (ch != '}'))
	        ch = *p++;
	    if(ch == '\0') {
		/*
		 * Did not find a matching '}'.
		 */
		amfree(dev);
		errno = EINVAL;
		return -1;
	    } else if(ch == '{') {
		depth++;
	    } else if(ch == '}') {
		depth--;
	    }
	}
	if(strchr(p, '{') != NULL || strchr(p, '}') != NULL) {
	    amfree(dev);
	    errno = EINVAL;
	    return -1;				/* only one list allowed */
	}
	*dev_left = dev;			/* text before the '{' */
	**dev_next = '\0';			/* zap the '{' */
	(*dev_next)++;				/* point to the first name */
	p[-1] = '\0';				/* zap the '}' */
	*dev_right = p;				/* text after the '}' */
    } else {
	/*
	 * Arrange to return just one name.
	 */
	*dev_next = dev;
	*dev_left = *dev_right = "";
    }
    return 0;
}

/*
 * Return the next device name.  A dynamic area is returned that the
 * caller is responsible for freeing.
 */

char *
tapeio_next_devname(
    char * dev_left,
    char * dev_right,
    char **dev_next)
{
    int ch;
    char *next;
    char *p;
    int depth;

    p = next = *dev_next;			/* remember the start point */
    depth = 0;
    do {
	ch = *p++;
	while((ch != '\0') && (ch != '{') && (ch != '}') && (ch != ','))
	    ch = *p++;
	if(ch == '\0') {
	    /*
	     * Found the end of a name.
	     */
	    assert(depth == 0);
	    if(*next == '\0') {
		return NULL;			/* end of the list */
	    }
	    p--;				/* point to the null byte */
	    break;
	} else if(ch == '{') {
	    depth++;
	} else if(ch == '}') {
	    assert(depth > 0);
	    depth--;
	}
    } while(depth != 0 || ch != ',');
    if(ch == ',') {
	p[-1] = '\0';				/* zap the ',' */
    }
    *dev_next = p;				/* set up for the next call */
    return vstralloc(dev_left, next, dev_right, NULL);
}

/*
 * The following functions get/set fields in the tape_info structure.
 * To allow them to be called (e.g. set) from lower level open functions
 * started by tape_open, we check and allocate the tape_info structure
 * here as well as in tape_open.
 */

char *
tapefd_getinfo_host(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    if(tape_info[fd].master_fd != -1)
	return tapefd_getinfo_host(tape_info[fd].master_fd);
    return tape_info[fd].host;
}

void
tapefd_setinfo_host(
    int fd,
    char *v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    amfree(tape_info[fd].host);
    if(v) {
	tape_info[fd].host = stralloc(v);
    }
}

char *
tapefd_getinfo_disk(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    if(tape_info[fd].master_fd != -1)
	return tapefd_getinfo_disk(tape_info[fd].master_fd);
    return tape_info[fd].disk;
}

void
tapefd_setinfo_disk(
    int fd,
    char *v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    amfree(tape_info[fd].disk);
    if(v) {
	tape_info[fd].disk = stralloc(v);
    }
}

int
tapefd_getinfo_level(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    if(tape_info[fd].master_fd != -1)
	return tapefd_getinfo_level(tape_info[fd].master_fd);
    return tape_info[fd].level;
}

void
tapefd_setinfo_level(
    int fd,
    int v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].level = v;
}

char *
tapefd_getinfo_datestamp(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    return tape_info[fd].datestamp;
}

void
tapefd_setinfo_datestamp(
    int fd,
    char *v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].datestamp = newstralloc(tape_info[fd].datestamp, v);
}

off_t
tapefd_getinfo_length(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    return tape_info[fd].length;
}

void
tapefd_setinfo_length(
    int fd,
    off_t v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].length = v;
}

char *
tapefd_getinfo_tapetype(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    return tape_info[fd].tapetype;
}

void
tapefd_setinfo_tapetype(
    int fd,
    char *v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].tapetype = newstralloc(tape_info[fd].tapetype, v);
}

int
tapefd_getinfo_fake_label(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    return tape_info[fd].fake_label;
}

void
tapefd_setinfo_fake_label(
    int fd,
    int v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].fake_label = v;
}

int
tapefd_getinfo_ioctl_fork(
    int fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    return tape_info[fd].ioctl_fork;
}

void
tapefd_setinfo_ioctl_fork(
    int fd,
    int v)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].ioctl_fork = v;
}

void
tapefd_set_master_fd(
    int fd,
    int master_fd)
{
    amtable_alloc((void **)tape_info_p,
		  &tape_info_count,
		  SIZEOF(*tape_info),
		  (size_t)fd + 1,
		  10,
		  tape_info_init);
    tape_info[fd].master_fd = master_fd;
}


/*
 * The normal tape operation functions.
 */

int
tape_access(
    char *filename,
    int mode)
{
    char *tname;
    int vslot;

    vslot = name2slot(filename, &tname);
    return vtable[vslot].xxx_tape_access(tname, mode);
}

int
tape_stat(
    char *filename,
    struct stat *buf)
{
    char *tname;
    int vslot;

    vslot = name2slot(filename, &tname);
    return vtable[vslot].xxx_tape_stat(tname, buf);
}

int
tape_open(
    char *filename,
    int mode, ...)
{
    char *tname;
    int vslot;
    int fd;
    mode_t mask;
    va_list ap;

    va_start(ap, mode);
    mask = (mode_t)va_arg(ap, int);
    va_end(ap);

    vslot = name2slot(filename, &tname);
    if((fd = vtable[vslot].xxx_tape_open(tname, mode, mask)) >= 0) {
	amtable_alloc((void **)tape_info_p,
		      &tape_info_count,
		      SIZEOF(*tape_info),
		      (size_t)(fd + 1),
		      10,
		      tape_info_init);
	/*
	 * It is possible to recurse in the above open call and come
	 * back here twice for the same file descriptor.  Set the vtape
	 * index only if it is not already set, i.e. the first call wins.
	 */
	if(tape_info[fd].vtape_index < 0) {
	    tape_info[fd].vtape_index = vslot;
	}
    }
    return fd;
}

int
tapefd_close(
    int fd)
{
    int	res;
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || ((vslot = tape_info[fd].vtape_index) < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    if((res = vtable[vslot].xxx_tapefd_close(fd)) == 0) {
	amfree(tape_info[fd].host);
	amfree(tape_info[fd].disk);
	amfree(tape_info[fd].datestamp);
	amfree(tape_info[fd].tapetype);
	memset(tape_info + fd, 0, SIZEOF(*tape_info));
        tape_info_init((void *)(tape_info + fd));
    }
    return res;
}

int
tapefd_can_fork(
    int fd)
{
    int	vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_can_fork(fd);
}

int
tapefd_fsf(
    int fd,
    off_t count)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_fsf(fd, count);
}

int
tapefd_rewind(
    int fd)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_rewind(fd);
}

void
tapefd_resetofs(
    int fd)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;				/* not that it matters */
	return;
    }

    vslot = tape_info[fd].vtape_index;
    vtable[vslot].xxx_tapefd_resetofs(fd);
}

int
tapefd_unload(
    int fd)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_unload(fd);
}

int
tapefd_status(
    int fd,
    struct am_mt_status *stat)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_status(fd, stat);
}

int
tapefd_weof(
    int fd,
    off_t count)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_weof(fd, count);
} 


ssize_t
tapefd_read(
    int fd,
    void *buffer,
    size_t count)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_read(fd, buffer, count);
}

ssize_t
tapefd_write(
    int fd,
    const void *buffer,
    size_t count)
{
    int vslot;

    if ((fd < 0) || ((size_t)fd >= tape_info_count)
       || (tape_info[fd].vtape_index < 0)) {
	errno = EBADF;
	return -1;
    }

    vslot = tape_info[fd].vtape_index;
    return vtable[vslot].xxx_tapefd_write(fd, buffer, count);
}

char *
tape_rewind(
    char *devname)
{
    int fd;
    char *r = NULL;

    if((fd = tape_open(devname, O_RDONLY)) < 0) {
	r = errstr = newvstralloc(errstr,
				  "tape_rewind: tape open: ",
				  devname,
				  ": ",
				  strerror(errno),
				  NULL);
    } else if(tapefd_rewind(fd) == -1) {
	r = errstr = newvstralloc(errstr,
				  "tape_rewind: rewinding tape: ",
				  devname,
				  ": ",
				  strerror(errno),
				  NULL);
    }
    if(fd >= 0) {
	tapefd_close(fd);
    }
    return r;
}

char *
tape_unload(
    char *devname)
{
    int fd;
    char *r = NULL;

    if((fd = tape_open(devname, O_RDONLY)) < 0) {
	r = errstr = newvstralloc(errstr,
				  "tape_unload: tape open: ",
				  devname,
				  ": ",
				  strerror(errno),
				  NULL);
    } else if(tapefd_unload(fd) == -1) {
	r = errstr = newvstralloc(errstr,
				  "tape_unload: unloading tape: ",
				  devname,
				  ": ",
				  strerror(errno),
				  NULL);
    }
    if(fd >= 0) {
	tapefd_close(fd);
    }
    return r;
}

char *
tape_fsf(
    char *devname,
    off_t count)
{
    int fd;
    char count_str[NUM_STR_SIZE];
    char *r = NULL;

    if((fd = tape_open(devname, O_RDONLY)) < 0) {
	r = errstr = newvstralloc(errstr,
				  "tape_fsf: tape open: ",
				  devname,
				  ": ",
				  strerror(errno),
				  NULL);
    } else if(tapefd_fsf(fd, count) == -1) {
	snprintf(count_str, SIZEOF(count_str), OFF_T_FMT,
				 (OFF_T_FMT_TYPE)count);
	r = errstr = newvstralloc(errstr,
			          "tape_fsf: fsf ",
				  count_str,
				  "file", (count == 1) ? "" : "s",
			          ": ",
			          strerror(errno),
			          NULL);
    }
    if(fd >= 0) {
	tapefd_close(fd);
    }
    return r;
}

/* Reads the tape label, like you expect. If failure, returns an error
   string. If the tape might not be an Amanda tape, the returned
   string will start with NOT_AMANDA_TAPE_MSG. */

char *
tapefd_rdlabel(
    int fd,
    char **datestamp,
    char **label)
{
    ssize_t rc;
    size_t buflen;
    char *buffer = NULL;
    dumpfile_t file;
    char *r = NULL;

    amfree(*datestamp);
    amfree(*label);
    buflen = MAX_TAPE_BLOCK_BYTES;
    buffer = alloc(buflen + 1);

    if(tapefd_getinfo_fake_label(fd)) {
	*datestamp = stralloc("X");
	*label = stralloc(FAKE_LABEL);
    } else if(tapefd_rewind(fd) == -1) {
	r = stralloc2("rewinding tape: ", strerror(errno));
    } else if((rc = tapefd_read(fd, buffer, buflen)) == -1) {
	r = vstralloc(NOT_AMANDA_TAPE_MSG, " (",
                      strerror(errno), ")", NULL);
    } else if (rc == 0) {
        r = stralloc2(NOT_AMANDA_TAPE_MSG, " (Read 0 bytes)");
    } else {
	/* make sure buffer is null-terminated */
	buffer[rc] = '\0';

	parse_file_header(buffer, &file, (size_t)rc);
	if(file.type != F_TAPESTART) {
	    r = stralloc(NOT_AMANDA_TAPE_MSG);
	} else {
	    *datestamp = stralloc(file.datestamp);
	    *label = stralloc(file.name);
	}
    }
    amfree(buffer);
    if (r)
	errstr = newvstralloc(errstr, r, NULL);
    return r;
}

char *
tape_rdlabel(
    char *devname,
    char **datestamp,
    char **label)
{
    int fd;
    char *r = NULL;

    if((fd = tape_open(devname, O_RDONLY)) < 0) {
	r = vstralloc("tape_rdlabel: tape open: ",
                      devname,
                      ": ",
                      strerror(errno),
                      NULL);
    } else
        r = tapefd_rdlabel(fd, datestamp, label);

    if(fd >= 0) {
        tapefd_close(fd);
    }
    if (r)
	errstr = newvstralloc(errstr, r, NULL);
    return r;
}

char *
tapefd_wrlabel(
    int fd,
    char *datestamp,
    char *label,
    size_t size)
{
    ssize_t rc;
    char *buffer = NULL;
    dumpfile_t file;
    char *r = NULL;

    if(tapefd_rewind(fd) == -1) {
	r = errstr = newstralloc2(errstr, "rewinding tape: ", strerror(errno));
    } else {
	fh_init(&file);
	file.type = F_TAPESTART;
	strncpy(file.datestamp, datestamp, SIZEOF(file.datestamp) - 1);
	file.datestamp[SIZEOF(file.datestamp) - 1] = '\0';
	strncpy(file.name, label, SIZEOF(file.name) - 1);
	file.name[SIZEOF(file.name) - 1] = '\0';
	buffer = alloc(size);
	file.blocksize = size;
	build_header(buffer, &file, size);
	tapefd_setinfo_host(fd, NULL);
	tapefd_setinfo_disk(fd, label);
	tapefd_setinfo_level(fd, -1);
	if((rc = tapefd_write(fd, buffer, size)) != (ssize_t)size) {
	    r = errstr = newstralloc2(errstr,
				      "writing label: ",
			              (rc != -1) ? "short write"
						 : strerror(errno));
	}
	amfree(buffer);
    }
    return r;
}

char *
tape_wrlabel(
    char *devname,
    char *datestamp,
    char *label,
    size_t size)
{
    int fd;
    char *r = NULL;

    if((fd = tape_open(devname, O_WRONLY)) < 0) {
	r = errstr = newstralloc2(errstr,
				  "writing label: ",
				  (errno == EACCES) ? "tape is write-protected"
						    : strerror(errno));
    } else if(tapefd_wrlabel(fd, datestamp, label, size) != NULL) {
	r = errstr;
    }
    if(fd >= 0) {
	tapefd_close(fd);
    }
    return r;
}

char *
tapefd_wrendmark(
    int fd,
    char *datestamp,
    size_t size)
{
    ssize_t rc;
    char *buffer = NULL;
    dumpfile_t file;
    char *r = NULL;

    fh_init(&file);
    file.type = F_TAPEEND;
    strncpy(file.datestamp, datestamp, SIZEOF(file.datestamp) - 1);
    file.datestamp[SIZEOF(file.datestamp) - 1] = '\0';
    buffer = alloc(size);
    file.blocksize = size;
    build_header(buffer, &file, size);
    tapefd_setinfo_host(fd, NULL);
    tapefd_setinfo_disk(fd, "TAPEEND");
    tapefd_setinfo_level(fd, -1);

    if((rc = tapefd_write(fd, buffer, size)) != (ssize_t)size) {
	r = errstr = newstralloc2(errstr, "writing endmark: ",
			          (rc != -1) ? "short write" : strerror(errno));
    }
    amfree(buffer);

    return r;
}

char *
tape_wrendmark(
    char *devname,
    char *datestamp,
    size_t size)
{
    int fd;
    char *r = NULL;

    if((fd = tape_open(devname, O_WRONLY)) < 0) {
	r = errstr = newstralloc2(errstr,
				  "writing endmark: ",
				  (errno == EACCES) ? "tape is write-protected"
						    : strerror(errno));
    } else if(tapefd_wrendmark(fd, datestamp, size) != NULL) {
	r = errstr;
    }
    if(fd >= 0) {
	tapefd_close(fd);
    }
    return r;
}

char *
tape_writable(
    char *devname)
{
    int fd = -1;
    char *r = NULL;

    /* first, make sure the file exists and the permissions are right */

    if(tape_access(devname, R_OK|W_OK) == -1) {
	r = errstr = newstralloc(errstr, strerror(errno));
    } else if((fd = tape_open(devname, O_WRONLY)) < 0) {
	r = errstr = newstralloc(errstr,
			         (errno == EACCES) ? "tape write-protected"
					           : strerror(errno));
    }
    if(fd >= 0) {
	tapefd_close(fd);
    }
    return r;
}

#ifdef TEST

/*
 * The following test program may be used to exercise I/O patterns through
 * the tapeio interface.  Commands may either be on the command line or
 * read from stdin (e.g. for a test suite).
 */

#include "token.h"

#if USE_RAND
/* If the C library does not define random(), try to use rand() by
   defining USE_RAND, but then make sure you are not using hardware
   compression, because the low-order bits of rand() may not be that
   random... :-( */
#define random() rand()
#define srandom(seed) srand(seed)
#endif

static char *pgm;

static void
do_help(void)
{
    fprintf(stderr, "  ?|help\n");
    fprintf(stderr, "  open [\"file\"|$TAPE [\"mode\":O_RDONLY]]\n");
    fprintf(stderr, "  read [\"records\":\"all\"]\n");
    fprintf(stderr, "  write [\"records\":1] [\"file#\":\"+\"] [\"record#\":\"+\"] [\"host\"] [\"disk\"] [\"level\"]\n");
    fprintf(stderr, "  eof|weof [\"count\":1]\n");
    fprintf(stderr, "  fsf [\"count\":1]\n");
    fprintf(stderr, "  rewind\n");
    fprintf(stderr, "  unload\n");
}

static void
usage(void)
{
    fprintf(stderr, "usage: %s [-c cmd [args] [%% cmd [args] ...]]\n", pgm);
    do_help();
}

#define TEST_BLOCKSIZE	(32 * 1024)

#define MAX_TOKENS	10

extern int optind;

static char *token_area[MAX_TOKENS + 1];
static char **token;
static int token_count;

static int fd = -1;
static off_t current_file = (off_t)0;
static off_t current_record = (off_t)0;

static int have_length = 0;
static int length = (off_t)0;

static int show_timestamp = 0;

char write_buf[TEST_BLOCKSIZE];

static void
do_open(void)
{
    mode_t mode;
    char *file;

    if(token_count < 2
       || (token_count >= 2 && strcmp(token[1], "$TAPE") == 0)) {
	if((file = getenv("TAPE")) == NULL) {
	    fprintf(stderr, "tape_open: no file name and $TAPE not set\n");
	    return;
	}
    } else {
	file = token[1];
    }
    if(token_count > 2) {
	mode = atoi(token[2]);
    } else {
	mode = O_RDONLY;
    }

    fprintf(stderr, "tapefd_open(\"%s\", %d): ", file, mode);
    if((fd = tape_open(file, mode, 0644)) < 0) {
	perror("");
    } else {
	fprintf(stderr, "%d (OK)\n", fd);
	if(have_length) {
	    tapefd_setinfo_length(fd, length);
	}
    }
}

static void
do_close(void)
{
    int	result;

    fprintf(stderr, "tapefd_close(): ");
    if((result = tapefd_close(fd)) < 0) {
	perror("");
    } else {
	fprintf(stderr, "%d (OK)\n", result);
    }
}

static void
do_read(void)
{
    ssize_t	result;
    off_t count = (off_t)0;
    int have_count = 0;
    char buf[SIZEOF(write_buf)];
    int *p;
    off_t i;
    char *s;
    time_t then;
    struct tm *tm;

    if(token_count > 1 && strcmp(token[1], "all") != 0) {
	count = OFF_T_ATOI(token[1]);
	have_count = 1;
    }

    p = (int *)buf;
    for(i = 0; (! have_count) || (i < count); i++) {
	fprintf(stderr, "tapefd_read(" OFF_T_FMT "): ", (OFF_T_FMT_TYPE)i);
	if((result = tapefd_read(fd, buf, SIZEOF(buf))) < 0) {
	    perror("");
	    break;
	} else if(result == 0) {
	    fprintf(stderr,  SSIZE_T_FMT" (EOF)\n", result);
	    /*
	     * If we were not given a count, EOF breaks the loop, otherwise
	     * we keep trying (to test read after EOF handling).
	     */
	    if(! have_count) {
		break;
	    }
	} else {
	    if(result == (ssize_t)sizeof(buf)) {
		s = "OK";
	    } else {
		s = "short read";
	    }

	    /*
	     * If the amount read is really short, we may refer to junk
	     * when displaying the record data, but things are pretty
	     * well screwed up at this point anyway so it is not worth
	     * the effort to deal with.
	     */
	    fprintf(stderr,
		    SSIZE_T_FMT " (%s): file %d: record %d",
		    result,
		    s,
		    p[0],
		    p[1]);
	    if(show_timestamp) {
		then = p[2];
		tm = localtime(&then);
		fprintf(stderr,
			": %04d/%02d/%02d %02d:%02d:%02d\n",
			tm->tm_year + 1900,
			tm->tm_mon + 1,
			tm->tm_mday,
			tm->tm_hour,
			tm->tm_min,
			tm->tm_sec);
	    }
	    fputc('\n', stderr);
	}
    }
}

static void
do_write(void)
{
    int	result;
    off_t count;
    off_t *p;
    off_t i;
    char *s;
    time_t now;
    struct tm *tm;

    if(token_count > 1) {
	count = OFF_T_ATOI(token[1]);
    } else {
	count = (off_t)1;
    }

    if(token_count > 2 && strcmp(token[2], "+") != 0) {
	current_file = OFF_T_ATOI(token[2]);
    }

    if(token_count > 3 && strcmp(token[3], "+") != 0) {
	current_record = OFF_T_ATOI(token[3]);
    }

    if(token_count > 4 && token[4][0] != '\0') {
	tapefd_setinfo_host(fd, token[4]);
    }

    if(token_count > 5 && token[5][0] != '\0') {
	tapefd_setinfo_disk(fd, token[5]);
    }

    if(token_count > 6 && token[6][0] != '\0') {
	tapefd_setinfo_level(fd, atoi(token[6]));
    }

    p = (off_t *)write_buf;
    time(&now);
    p[2] = now;
    tm = localtime(&now);
    for(i = 0; i < count; i++, (current_record += (off_t)1)) {
	p[0] = current_file;
	p[1] = current_record;
	fprintf(stderr, "tapefd_write(" OFF_T_FMT "): ", i);
	if((result = tapefd_write(fd, write_buf, SIZEOF(write_buf))) < 0) {
	    perror("");
	    break;
	} else {
	    if(result == (ssize_t)sizeof(write_buf)) {
		s = "OK";
	    } else {
		s = "short write";
	    }
	    fprintf(stderr,
		    "%d (%s): file " OFF_T_FMT ": record " OFF_T_FMT,
		    result,
		    s,
		    p[0],
		    p[1]);
	    if(show_timestamp) {
		fprintf(stderr,
			": %04d/%02d/%02d %02d:%02d:%02d\n",
			tm->tm_year + 1900,
			tm->tm_mon + 1,
			tm->tm_mday,
			tm->tm_hour,
			tm->tm_min,
			tm->tm_sec);
	    }
	    fputc('\n', stderr);
	}
    }
}

static void
do_fsf(void)
{
    int	result;
    off_t count;

    if(token_count > 1) {
	count = OFF_T_ATOI(token[1]);
    } else {
	count = (off_t)1;
    }

    fprintf(stderr, "tapefd_fsf(" OFF_T_FMT "): ", (OFF_T_FMT_TYPE)count);
    if((result = tapefd_fsf(fd, count)) < 0) {
	perror("");
    } else {
	fprintf(stderr, "%d (OK)\n", result);
	current_file += count;
	current_record = (off_t)0;
    }
}

static void
do_weof(void)
{
    int	result;
    off_t count;

    if(token_count > 1) {
	count = OFF_T_ATOI(token[1]);
    } else {
	count = (off_t)1;
    }

    fprintf(stderr, "tapefd_weof(" OFF_T_FMT "): ", count);
    if((result = tapefd_weof(fd, count)) < 0) {
	perror("");
    } else {
	fprintf(stderr, "%d (OK)\n", result);
	current_file += count;
	current_record = (off_t)0;
    }
}

static void
do_rewind(void)
{
    int	result;

    fprintf(stderr, "tapefd_rewind(): ");
    if((result = tapefd_rewind(fd)) < 0) {
	perror("");
    } else {
	fprintf(stderr, "%d (OK)\n", result);
	current_file = (off_t)0;
	current_record = (off_t)0;
    }
}

static void
do_unload(void)
{
    int	result;

    fprintf(stderr, "tapefd_unload(): ");
    if((result = tapefd_unload(fd)) < 0) {
	perror("");
    } else {
	fprintf(stderr, "%d (OK)\n", result);
	current_file = (off_t)-1;
	current_record = (off_t)-1;
    }
}

struct cmd {
    char *name;
    int min_chars;
    void (*func)(void);
} cmd[] = {
    { "?",		0,	do_help },
    { "help",		0,	do_help },
    { "eof",		0,	do_weof },
    { "weof",		0,	do_weof },
    { "fsf",		0,	do_fsf },
    { "rewind",		0,	do_rewind },
    { "offline",	0,	do_unload },
    { "open",		0,	do_open },
    { "close",		0,	do_close },
    { "read",		0,	do_read },
    { "write",		0,	do_write },
    { NULL,		0,	NULL }
};

int
main(
    int argc,
    char **argv)
{
    int ch;
    int cmdline = 0;
    char *line = NULL;
    char *s;
    int i;
    int j;
    time_t now;

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    if((pgm = strrchr(argv[0], '/')) != NULL) {
	pgm++;
    } else {
	pgm = argv[0];
    }

    /*
     * Compute the minimum abbreviation for each command.
     */
    for(i = 0; cmd[i].name; i++) {
	cmd[i].min_chars = 1;
	while(1) {
	    for(j = 0; cmd[j].name; j++) {
		if(i == j) {
		    continue;
		}
		if(0 == strncmp(cmd[i].name, cmd[j].name, cmd[i].min_chars)) {
		    break;
		}
	    }
	    if(0 == cmd[j].name) {
		break;
	    }
	    cmd[i].min_chars++;
	}
    }

    /*
     * Process the command line flags.
     */
    while((ch = getopt(argc, argv, "hcl:t")) != EOF) {
	switch (ch) {
	case 'c':
	    cmdline = 1;
	    break;
	case 'l':
	    have_length = 1;
	    length = OFF_T_ATOI(optarg);
	    j = strlen(optarg);
	    if(j > 0) {
		switch(optarg[j-1] ) {
		case 'k':				break;
		case 'b': length /= (off_t)2;	 	break;
		case 'M': length *= (off_t)1024;	break;
		default:  length /= (off_t)1024;	break;
		}
	    } else {
		length /= (off_t)1024;
	    }
	    break;
	case 't':
	    show_timestamp = 1;
	    break;
	case 'h':
	default:
	    usage();
	    return 1;
	}
    }

    /*
     * Initialize the write buffer.
     */
    time(&now);
    srandom(now);
    for(j = 0; j < (int)SIZEOF(write_buf); j++) {
	write_buf[j] = (char)random();
    }

    /*
     * Do the tests.
     */
    token = token_area + 1;
    token_area[0] = "";				/* if cmdline */
    while(1) {
	if(cmdline) {
	    for(token_count = 1;
		token_count < (int)(SIZEOF(token_area) / SIZEOF(token_area[0]))
		&& optind < argc;
		token_count++, optind++) {
		if(strcmp(argv[optind], "%") == 0) {
		    optind++;
		    break;
		}
		token_area[token_count] = argv[optind];
	    }
	    token_count--;
	    if(token_count == 0 && optind >= argc) {
		break;
	    }
	} else {
	    if((line = areads(0)) == NULL) {
		break;
	    }
	    if((s = strchr(line, '#')) != NULL) {
		*s = '\0';
	    }
	    s = line + strlen(line) - 1;
	    while(s >= line && isspace(*s)) {
	        *s-- = '\0';
	    }
	    token_count = split(line,
				token_area,
				SIZEOF(token_area) / SIZEOF(token_area[0]),
				" ");
	}
	amfree(line);

	/*
	 * Truncate tokens at first comment indicator, then test for
	 * empty command.
	 */
	for(i = 0; i < token_count; i++) {
	    if(token[i][0] == '#') {
		token_count = i;
		break;
	    }
	}
	if(token_count <= 0) {
	    continue;				/* blank/comment input line */
	}

	/*
	 * Find the command to run, the do it.
	 */
	j = strlen(token[0]);
	for(i = 0; cmd[i].name; i++) {
	    if(strncmp(cmd[i].name, token[0], j) == 0
	       && j >= cmd[i].min_chars) {
		break;
	    }
	}
	if(cmd[i].name == NULL) {
	    fprintf(stderr, "%s: unknown command: %s\n", pgm, token[0]);
	    exit(1);
	}
	(*cmd[i].func)();
    }

    return 0;
}

#endif /* TEST */
