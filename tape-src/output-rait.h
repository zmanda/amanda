#ifndef RAIT_H

#define RAIT_H

typedef struct {
    int nopen;
    int nfds;
    size_t fd_count;
    int *fds;
    ssize_t *readres;
    size_t xorbuflen;
    char *xorbuf;
} RAIT;

#ifdef NO_AMANDA

#define stralloc strdup

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
    off_t fileno;			/* tape file number */
    off_t blkno;			/* block within file */
    int device_status_size;		/* size of orig device status field */
    unsigned long device_status;	/* "device status", whatever that is */
    int error_status_size;		/* size of orig error status field */
    unsigned long error_status;		/* "error status", whatever that is */
};
#endif

int rait_open(char *dev, int flags, mode_t mask);
int rait_access(char *, int);
int rait_stat(char *, struct stat *);
int rait_close(int);
off_t rait_lseek(int, off_t, int);
ssize_t rait_write(int, const void *, size_t);
ssize_t rait_read(int, void *, size_t);
int rait_ioctl(int, int, void *);
int rait_copy(char *f1, char *f2, size_t buflen);
char *rait_init_namelist(char * dev, char **dev_left, char **dev_right, char **dev_next);
int rait_next_name(char * dev_left, char * dev_right, char **dev_next, char * dev_real);
int  rait_tape_open(char *, int, mode_t);
int  rait_tapefd_fsf(int rait_tapefd, off_t count);
int  rait_tapefd_rewind(int rait_tapefd);
void rait_tapefd_resetofs(int rait_tapefd);
int  rait_tapefd_unload(int rait_tapefd);
int  rait_tapefd_status(int rait_tapefd, struct am_mt_status *stat);
int  rait_tapefd_weof(int rait_tapefd, off_t count);
int  rait_tapefd_can_fork(int);

#ifdef RAIT_REDIRECT

/* handle ugly Solaris stat mess */

#ifdef _FILE_OFFSET_BITS
#include <sys/stat.h>
#undef stat
#undef open
#if _FILE_OFFSET_BITS == 64
struct	stat {
	dev_t	st_dev;
	long	st_pad1[3];	/* reserved for network id */
	ino_t	st_ino;
	mode_t	st_mode;
	nlink_t st_nlink;
	uid_t 	st_uid;
	gid_t 	st_gid;
	dev_t	st_rdev;
	long	st_pad2[2];
	off_t	st_size;
	timestruc_t st_atim;
	timestruc_t st_mtim;
	timestruc_t st_ctim;
	long	st_blksize;
	blkcnt_t st_blocks;
	char	st_fstype[_ST_FSTYPSZ];
	long	st_pad4[8];	/* expansion area */
};
#endif

#endif

#define access(p,f)	rait_access(p,f)
#define stat(a,b)	rait_stat(a,b)
#define open		rait_open
#define	close(a)	rait_close(a)
#define read(f,b,l)	rait_read(f,b,l)
#define write(f,b,l)	rait_write(f,b,l)
#define	ioctl(f,n,x)	rait_ioctl(f,n,x)
#endif

#endif
