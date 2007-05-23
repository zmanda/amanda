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
 * $Id: tapeio.h,v 1.21 2006/05/25 01:47:27 johnfranks Exp $
 *
 * interface for tapeio.c
 */
#ifndef TAPEIO_H
#define TAPEIO_H

#include "amanda.h"
#include "util.h" /* For BSTRNCMP */

/*
 * Tape drive status structure.  This abstracts the things we are
 * interested in from the free-for-all of what the various drivers
 * supply.
 */

struct am_mt_status {
    char online_valid;			/* is the online flag valid? */
    char bot_valid;			/* is the BOT flag valid? */
    char eot_valid;			/* is the EOT flag valid? */
    char protected_valid;		/* is the protected flag valid? */
    char flags_valid;			/* is the flags field valid? */
    char fileno_valid;			/* is the fileno field valid? */
    char blkno_valid;			/* is the blkno field valid? */
    char device_status_valid;		/* is the device status field valid? */
    char error_status_valid;		/* is the device status field valid? */

    char online;			/* true if device is online/ready */
    char bot;				/* true if tape is at the beginning */
    char eot;				/* true if tape is at end of medium */
    char protected;			/* true if tape is write protected */
    long flags;				/* device flags, whatever that is */
    long fileno;			/* tape file number */
    long blkno;				/* block within file */
    int device_status_size;		/* size of orig device status field */
    unsigned long device_status;	/* "device status", whatever that is */
    int error_status_size;		/* size of orig error status field */
    unsigned long error_status;		/* "error status", whatever that is */
};

#define	FAKE_LABEL	"[fake-label]"
#define NO_LABEL        "[no-label-yet]"

int tape_open(char *, int, ...);

int tapefd_rewind(int tapefd);
int tapefd_unload(int tapefd);
int tapefd_fsf(int tapefd, off_t count);
int tapefd_weof(int tapefd, off_t count);

int tapefd_status(int tapefd, struct am_mt_status *);

void tapefd_resetofs(int tapefd);

ssize_t tapefd_read(int, void *, size_t);
ssize_t tapefd_write(int tapefd, const void *buffer, size_t count);

char *tapefd_rdlabel(int tapefd, char **datestamp, char **label);
char *tapefd_wrlabel(int tapefd,
			char  *datestamp,
			char  *label,
			size_t s);

char *auto_tapefd_label(int tapefd, char **datestamp, char **label);
char *auto_tape_label(char *dev, char **datestamp, char **label);

char *tapefd_wrendmark(int tapefd, char *datestamp, size_t s);

int tapefd_eof(int tapefd);		/* just used in tapeio-test */
int tapefd_close(int tapefd);
int tapefd_can_fork(int tapefd);

char *tape_unload(char *dev);
char *tape_rewind(char *dev);
char *tape_fsf(char *dev, off_t count);
char *tape_rdlabel(char *dev, char **datestamp, char **label);
char *tape_wrlabel(char *dev,
		      char  *datestamp,
		      char  *label,
		      size_t size);
char *tape_wrendmark(char *dev,
			char *datestamp,
			size_t size);
char *tape_writable(char *dev);

int tape_access(char *dev, int mode);
int tape_stat(char *filename, struct stat *buf);

char *tapefd_getinfo_label(int fd);
void tapefd_setinfo_label(int fd, char *v);
char *tapefd_getinfo_host(int fd);
void tapefd_setinfo_host(int fd, char *v);
char *tapefd_getinfo_disk(int fd);
void tapefd_setinfo_disk(int fd, char *v);
int tapefd_getinfo_level(int fd);
void tapefd_setinfo_level(int fd, int v);
char *tapefd_getinfo_datestamp(int fd);
void tapefd_setinfo_datestamp(int fd, char *v);
off_t tapefd_getinfo_length(int fd);
void tapefd_setinfo_length(int fd, off_t v);
char *tapefd_getinfo_tapetype(int fd);
void tapefd_setinfo_tapetype(int fd, char *v);
int tapefd_getinfo_fake_label(int fd);
void tapefd_setinfo_fake_label(int fd, int v);
int tapefd_getinfo_ioctl_fork(int fd);
void tapefd_setinfo_ioctl_fork(int fd, int v);
void tapefd_set_master_fd(int tapefd, int master_fd);

#ifdef HAVE_LINUX_ZFTAPE_H
int is_zftape(const char *filename);
#endif

int tapeio_init_devname(char * dev,
			   char **dev_left,
			   char **dev_right,
			   char **dev_next);
char *tapeio_next_devname(char * dev_left,
			     char * dev_right,
			     char **dev_next);

#define NOT_AMANDA_TAPE_MSG "not an amanda tape"
#define CHECK_NOT_AMANDA_TAPE_MSG(x) (!BSTRNCMP(x, NOT_AMANDA_TAPE_MSG))

#endif /* ! TAPEIO_H */
