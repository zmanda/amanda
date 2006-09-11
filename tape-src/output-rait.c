#ifdef NO_AMANDA
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/ioctl.h>
#else
#include "amanda.h"
#include "tapeio.h"
#endif

#include "output-rait.h"
#include "output-tape.h"

#ifdef NO_AMANDA
#define	amfree(x)	do {						\
	if (x) {							\
	    int save_errno = errno;					\
	    free(x);							\
	    (x) = NULL;							\
	    errno = save_errno;						\
	}
} while(0)
#define	tape_open	open
#define tapefd_read	read
#define tapefd_write	write
#define tapefd_close	close
#define tape_access	access
#define tape_stat	stat
#define tapefd_fsf	tape_tapefd_fsf
#define tapefd_rewind	tape_tapefd_rewind
#define tapefd_status	tape_tapefd_status
#define tapefd_unload	tape_tapefd_unload
#define tapefd_weof	tape_tapefd_weof

int tapeio_init_devname (char * dev,
			 char **dev_left,
			 char **dev_right,
			 char **dev_next);
char *tapeio_next_devname (char * dev_left,
			   char * dev_right,
			   char **dev_next);
#endif

/*
** RAIT -- redundant array of (inexpensive?) tapes
**
** Author: Marc Mengel <mengel@fnal.gov>
**
** This package provides for striping input/output across
** multiple tape drives.
**
		 Table of Contents

  rait.c..................................................1
	MAX_RAITS.........................................2
        rait_table........................................2
	rait_open(char *dev, int flags, mode_t mode)......2
	rait_close(int fd)................................3
	rait_lseek(int fd, long pos, int whence)..........4
	rait_write(int fd, const char *buf, size_t len) ..5
	rait_read(int fd, char *buf, size_t len)..........6
	rait_ioctl(int fd, int op, void *p)...............8
	rait_access(devname, R_OK|W_OK)...................8
	rait_stat(devname, struct statbuf*)...............8
	rait_copy(char *f1, char *f2).....................9
	ifndef NO_AMANDA
	    rait_tapefd_fsf(rait_tapefd, count)..........10
	    rait_tapefd_rewind(rait_tapefd)..............10
	    rait_tapefd_resetofs(rait_tapefd)............10
	    rait_tapefd_unload(rait_tapefd)..............10
	    rait_tapefd_status(rait_tapefd, stat)........10
	    rait_tapefd_weof(rait_tapefd, count).........10

   rait.h.................................................1
        typedef RAIT......................................1
        ifdef RAIT_REDIRECT...............................1
             open.........................................1
	     close........................................1
             ioctl........................................1
	     read.........................................1
             write........................................1
*/

/**/

/*
** rait_open takes a string like:
** "/dev/rmt/tps0d{3,5,7,19}nrnsv"
** and opens
** "/dev/rmt/tps0d3nrnsv"
** "/dev/rmt/tps0d5nrnsv"
** "/dev/rmt/tps0d7nrnsv"
** "/dev/rmt/tps0d19nrnsv"
** as a RAIT.
**
** If it has no curly brace, we treat it as a plain device,
** and do a normal open, and do normal operations on it.
*/

#ifdef RAIT_DEBUG
#define rait_debug(p) do {						\
  int save_errno = errno;						\
									\
  if (0!=getenv("RAIT_DEBUG")) {					\
    fprintf p;								\
  }									\
  errno = save_errno;							\
} while (0)
#else
#define rait_debug(p)
#endif

static RAIT *rait_table = 0;		/* table to keep track of RAITS */
static size_t rait_table_count;

#ifdef NO_AMANDA
/*
 * amtable_alloc -- (re)allocate enough space for some number of elements.
 *
 * input:	table -- pointer to pointer to table
 *		current -- pointer to current number of elements
 *		elsize -- size of a table element
 *		count -- desired number of elements
 *		bump -- round up factor
 * output:	table -- possibly adjusted to point to new table area
 *		current -- possibly adjusted to new number of elements
 */

static int
amtable_alloc(
    void **	table,
    int *	current,
    size_t	elsize,
    int		count,
    int		bump,
    void *	dummy)
{
    void *table_new;
    int table_count_new;

    if (count >= *current) {
	table_count_new = ((count + bump) / bump) * bump;
	table_new = alloc(table_count_new * elsize);
	if (0 != *table) {
	    memcpy(table_new, *table, *current * elsize);
	    amfree(*table);
	}
	*table = table_new;
	memset(((char *)*table) + *current * elsize,
	       0,
	       (table_count_new - *current) * elsize);
	*current = table_count_new;
    }
    return 0;
}

/*
 * amtable_free -- release a table.
 *
 * input:	table -- pointer to pointer to table
 *		current -- pointer to current number of elements
 * output:	table -- possibly adjusted to point to new table area
 *		current -- possibly adjusted to new number of elements
 */

void
amtable_free(
    void **	table,
    int *	current)
{
    amfree(*table);
    *current = 0;
}
#endif

#define rait_table_alloc(fd)	amtable_alloc((void **)rait_table_p,	     \
					      &rait_table_count,	     \
					      SIZEOF(*rait_table),   \
					      (size_t)(fd),		     \
					      10,			     \
					      NULL)

int
rait_open(
    char *	dev,
    int		flags,
    mode_t	mask)
{
    int fd;			/* the file descriptor number to return */
    RAIT *res;			/* resulting RAIT structure */
    char *dev_left;		/* string before { */
    char *dev_right;		/* string after } */
    char *dev_next;		/* string inside {} */
    char *dev_real;		/* parsed device name */
    int rait_flag;		/* true if RAIT syntax in dev */
    int save_errno;
    int r;
    RAIT **rait_table_p = &rait_table;
    int **fds_p;

    rait_debug((stderr,"rait_open( %s, %d, %d )\n", dev, flags, mask));

    rait_flag = (0 != strchr(dev, '{'));

    if (rait_flag) {

	/*
	** we have to return a valid file descriptor, so use
	** a dummy one to /dev/null
	*/
	fd = open("/dev/null",flags,mask);
    } else {

	/*
	** call the normal tape_open function if we are not
	** going to do RAIT
	*/
	fd = tape_open(dev,flags,mask);
    }
    if(-1 == fd) {
	rait_debug((stderr, "rait_open:returning %d: %s\n",
			    fd,
			    strerror(errno)));
	return fd;
    }

    if(0 != rait_table_alloc(fd + 1)) {
	save_errno = errno;
	(void)tapefd_close(fd);
	errno = save_errno;
	rait_debug((stderr, "rait_open:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    res = &rait_table[fd];

    memset(res, 0, SIZEOF(*res));
    res->nopen = 1;

    res->fd_count = 0;
    if (rait_flag) {

	/* copy and parse the dev string so we can scribble on it */
	dev = stralloc(dev);
	if (0 == dev) {
	    rait_debug((stderr, "rait_open:returning %d: %s\n",
			        -1,
			        "out of stralloc memory"));
	    return -1;
        }
        if (0 != tapeio_init_devname(dev, &dev_left, &dev_right, &dev_next)) {
	    rait_debug((stderr, "rait_open:returning %d: %s\n",
			        -1,
			        strerror(errno)));
	    return -1;
        }

	while (0 != (dev_real = tapeio_next_devname(dev_left, dev_right, &dev_next))) {
            fds_p = &(res->fds);
	    r = amtable_alloc((void **)fds_p,
			    &res->fd_count,
			    SIZEOF(*res->fds),
			    (size_t)res->nfds + 1,
			    10,
			    NULL);
	    if (0 != r) {
		(void)rait_close(fd);
		fd = -1;
		amfree(dev_real);
		break;
	    }
	    res->fds[ res->nfds ] = tape_open(dev_real,flags,mask);
	    rait_debug((stderr,"rait_open:opening %s yields %d\n",
			dev_real, res->fds[res->nfds] ));
	    if ( res->fds[res->nfds] < 0 ) {
		save_errno = errno;
		(void)rait_close(fd);
		amfree(dev_real);
		errno = save_errno;
		fd = -1;
		break;
	    }
	    tapefd_set_master_fd(res->fds[res->nfds], fd);
	    amfree(dev_real);
	    res->nfds++;
	}

	/* clean up our copied string */
	amfree(dev);

    } else {

	/*
	** set things up to treat this as a normal tape if we ever
	** come in here again
	*/

	res->nfds = 0;
        fds_p = &(res->fds);
	r = amtable_alloc((void **)fds_p,
			  &res->fd_count,
			  SIZEOF(*res->fds),
			  (size_t)res->nfds + 1,
			  1,
			  NULL);
	if (0 != r) {
	    (void)tapefd_close(fd);
	    memset(res, 0, SIZEOF(*res));
	    errno = ENOMEM;
	    fd = -1;
	} else {
	    res->fds[res->nfds] = fd;
	    res->nfds++;
	}
    }

    if (fd >= 0 && res->nfds > 0) {
	res->readres = alloc(res->nfds * SIZEOF(*res->readres));
	memset(res->readres, 0, res->nfds * SIZEOF(*res->readres));
    }

    rait_debug((stderr, "rait_open:returning %d%s%s\n",
			fd,
			(fd < 0) ? ": " : "",
			(fd < 0) ? strerror(errno) : ""));

    return fd;
}

#ifdef NO_AMANDA
int
tapeio_init_devname(
    char *	dev,
    char **	dev_left,
    char **	dev_right,
    char **	dev_next)
{
    /*
    ** find the first { and then the first } that follows it
    */
    if ( 0 == (*dev_next = strchr(dev, '{'))
	 || 0 == (*dev_right = strchr(*dev_next + 1, '}')) ) {
	/* we dont have a {} pair */
	amfree(dev);
	errno = EINVAL;
	return -1;
    }

    *dev_left = dev;				/* before the { */
    **dev_next = 0;				/* zap the { */
    (*dev_next)++;
    (*dev_right)++;				/* after the } */
    return 0;
}

char *
tapeio_next_devname(
    char *	dev_left,
    char *	dev_right,
    char **	dev_next)
{
    char *dev_real = 0;
    char *next;
    int len;

    next = *dev_next;
    if (0 != (*dev_next = strchr(next, ','))
	|| 0 != (*dev_next = strchr(next, '}'))){

	**dev_next = 0;				/* zap the terminator */
	(*dev_next)++;

	/*
	** we have one string picked out, build it into the buffer
	*/
	len = strlen(dev_left) + strlen(next) + strlen(dev_right) + 1;
	dev_real = alloc(len);
	strcpy(dev_real, dev_left);		/* safe */
	strcat(dev_real, next);		/* safe */
	strcat(dev_real, dev_right);	/* safe */
    }
    return dev_real;
}
#endif

/*
** close everything we opened and free our memory.
*/
int
rait_close(
    int fd)
{
    int i;			/* index into RAIT drives */
    int j;			/* individual tapefd_close result */
    int res;			/* result from close */
    RAIT *pr;			/* RAIT entry from table */
    int save_errno = errno;
    pid_t kid;
    int **fds_p;

    rait_debug((stderr,"rait_close( %d )\n", fd));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_close:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_close:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    if (0 == pr->readres && 0 < pr->nfds) {
	pr->readres = alloc(pr->nfds * SIZEOF(*pr->readres));
	memset(pr->readres, 0, pr->nfds * SIZEOF(*pr->readres));
    }

    res = 0;
    /*
    ** this looks strange, but we start kids who are going to close the
    ** drives in parallel just after the parent has closed their copy of
    ** the descriptor. ('cause closing tape devices usually causes slow
    ** activities like filemark writes, etc.)
    */
    for( i = 0; i < pr->nfds; i++ ) {
	if(tapefd_can_fork(pr->fds[i])) {
	    if ((kid = fork()) == 0) {
		/* we are the child process */
		sleep(0);
		j = tapefd_close(pr->fds[i]);
		exit(j);
            } else {
		/* remember who the child is or that an error happened */
	  	pr->readres[i] = (ssize_t)kid;
            }
	}
	else {
	    j = tapefd_close(pr->fds[i]);
	    if ( j != 0 )
		res = j;
	    pr->readres[i] = -1;
	}
    }

    for( i = 0; i < pr->nfds; i++ ) {
	j = tapefd_close(pr->fds[i]);
	if ( j != 0 )
           res = j;
    }

    for( i = 0; i < pr->nfds; i++ ) {
        int stat;
	if(pr->readres[i] != -1) {
	    waitpid((pid_t)pr->readres[i], &stat, 0);
	    if( WEXITSTATUS(stat) != 0 ) {
		res = WEXITSTATUS(stat);
		if( res == 255 )
		    res = -1;
	    }
        }
    }
    if (pr->nfds > 1) {
	(void)close(fd);	/* close the dummy /dev/null descriptor */
    }
    if (0 != pr->fds) {
        fds_p = &pr->fds;
	amtable_free((void **)fds_p, &pr->fd_count);
    }
    if (0 != pr->readres) {
	amfree(pr->readres);
    }
    if (0 != pr->xorbuf) {
	amfree(pr->xorbuf);
    }
    pr->nopen = 0;
    errno = save_errno;
    rait_debug((stderr, "rait_close:returning %d%s%s\n",
			res,
			(res < 0) ? ": " : "",
			(res < 0) ? strerror(errno) : ""));
    return res;
}

/**/

/*
** seek out to the nth byte on the RAIT set.
** this is assumed to be evenly divided across all the stripes
*/
off_t
rait_lseek(
    int		fd,
    off_t	pos,
    int		whence)
{
    int i;			/* drive number in RAIT */
    off_t res, 			/* result of lseeks */
	 total;			/* total of results */
    RAIT *pr;			/* RAIT slot in table */

    rait_debug((stderr, "rait_lseek(%d," OFF_T_FMT ",%d)\n",
		fd, (OFF_T_FMT_TYPE)pos, whence));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_lseek:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return (off_t)-1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_lseek:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return (off_t)-1;
    }

    if ((pr->nfds > 1) && ((pos % (off_t)(pr->nfds-1)) != (off_t)0)) {
	errno = EDOM;
	total = (off_t)-1;
    } else {
	total = (off_t)0;
	pos = pos / (off_t)pr->nfds;
	for( i = 0; i < pr->nfds; i++ ) {
	    if ((off_t)0 >= (res = lseek(pr->fds[i], pos, whence))) {
		total = res;
		break;
	    }
	    total += res;
	}
    }
    rait_debug((stderr, "rait_lseek:returning %ld%s%s\n",
			total,
			(total < 0) ? ": " : "",
			(total < 0) ? strerror(errno) : ""));
    return total;
}

/**/

/*
** if we only have one stream, just do a write,
** otherwise compute an xor sum, and do several
** writes...
*/
ssize_t
rait_write(
    int		fd,
    const void *bufptr,
    size_t	len)
{
    const char *buf = bufptr;
    int i;	/* drive number */
    size_t j;	/* byte offset */
    RAIT *pr;	/* RAIT structure for this RAIT */
    ssize_t res;
    ssize_t total = 0;
    int data_fds;	/* number of data stream file descriptors */

    rait_debug((stderr, "rait_write(%d,%lx,%d)\n",fd,(unsigned long)buf,len));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_write:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_write:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    /* need to be able to slice it up evenly... */
    if (pr->nfds > 1) {
	data_fds = pr->nfds - 1;
	if (0 != len % data_fds) {
	    errno = EDOM;
	    rait_debug((stderr, "rait_write:returning %d: %s\n",
			        -1,
			        strerror(errno)));
	    return -1;
	}
	/* each slice gets an even portion */
	len = len / data_fds;

	/* make sure we have enough buffer space */
	if (len > (size_t)pr->xorbuflen) {
	    if (0 != pr->xorbuf) {
		amfree(pr->xorbuf);
	    }
	    pr->xorbuf = alloc(len);
	    pr->xorbuflen = len;
	}

	/* compute the sum */
	memcpy(pr->xorbuf, buf, len);
	for( i = 1; i < data_fds; i++ ) {
	    for( j = 0; j < len; j++ ) {
		pr->xorbuf[j] ^= buf[len * i + j];
	    }
	}
    } else {
	data_fds = pr->nfds;
    }

    /* write the chunks in the main buffer */
    for( i = 0; i < data_fds; i++ ) {
	res = tapefd_write(pr->fds[i], buf + len*i , len);
	rait_debug((stderr, "rait_write: write(%d,%lx,%d) returns %d%s%s\n",
		        pr->fds[i],
			(unsigned long)(buf + len*i),
			len,
			res,
			(res < 0) ? ": " : "",
			(res < 0) ? strerror(errno) : ""));
	if (res < 0) {
	    total = res;
	    break;
	}
	total += res;
    }
    if (total >= 0 && pr->nfds > 1) {
        /* write the sum, don't include it in the total bytes written */
	res = tapefd_write(pr->fds[i], pr->xorbuf, len);
	rait_debug((stderr, "rait_write: write(%d,%lx,%d) returns %d%s%s\n",
		    pr->fds[i],
		    (unsigned long)pr->xorbuf,
		    len,
		    res,
		    (res < 0) ? ": " : "",
		    (res < 0) ? strerror(errno) : ""));
	if (res < 0) {
	    total = res;
	}
    }

    rait_debug((stderr, "rait_write:returning %d%s%s\n",
			total,
			(total < 0) ? ": " : "",
			(total < 0) ? strerror(errno) : ""));

    return total;
}

/**/

/*
** once again, if there is one data stream do a read, otherwise
** do all n reads, and if any of the first n - 1 fail, compute
** the missing block from the other three, then return the data.
** there's some silliness here for reading tape with bigger buffers
** than we wrote with, (thus the extra bcopys down below).  On disk if
** you read with a bigger buffer size than you wrote with, you just
** garble the data...
*/
ssize_t
rait_read(
    int		fd,
    void *	bufptr,
    size_t	len)
{
    char *buf = bufptr;
    int nerrors, neofs, errorblock;
    ssize_t    total;
    int i;
    size_t j;
    RAIT *pr;
    int data_fds;
    int save_errno = errno;
    ssize_t maxreadres = 0;
    int sum_mismatch = 0;

    rait_debug((stderr, "rait_read(%d,%lx,%d)\n",fd,(unsigned long)buf,len));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_read:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_read:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    nerrors = 0;
    neofs = 0;
    errorblock = -1;
    /* once again , we slice it evenly... */
    if (pr->nfds > 1) {
	data_fds = pr->nfds - 1;
	if (0 != len % data_fds) {
	    errno = EDOM;
	    rait_debug((stderr, "rait_read:returning %d: %s\n",
			        -1,
			        strerror(errno)));
	    return -1;
	}
	len = len / data_fds;
    } else {
	data_fds = 1;
    }

    /* try all the reads, save the result codes */
    /* count the eof/errors */
    for( i = 0; i < data_fds; i++ ) {
	pr->readres[i] = tapefd_read(pr->fds[i], buf + len*i , len);
	rait_debug((stderr, "rait_read: read on fd %d returns %d%s%s\n",
		    pr->fds[i],
		    pr->readres[i],
		    (pr->readres[i] < 0) ? ": " : "",
		    (pr->readres[i] < 0) ? strerror(errno) : ""));
	if ( pr->readres[i] <= 0 ) {
	    if ( pr->readres[i] == 0 ) {
		neofs++;
	    } else {
	        if (0 == nerrors) {
		    save_errno = errno;
	        }
	        nerrors++;
	    }
	    errorblock = i;
	} else if (pr->readres[i] > maxreadres) {
	    maxreadres = pr->readres[i];
	}
    }
    if (pr->nfds > 1) {
	/* make sure we have enough buffer space */
	if (len > (size_t)pr->xorbuflen) {
	    if (0 != pr->xorbuf) {
		amfree(pr->xorbuf);
	    }
	    pr->xorbuf = alloc(len);
	    pr->xorbuflen = len;
	}
	pr->readres[i] = tapefd_read(pr->fds[i], pr->xorbuf , len);
	rait_debug((stderr, "rait_read: read on fd %d returns %d%s%s\n",
		    pr->fds[i],
		    pr->readres[i],
		    (pr->readres[i] < 0) ? ": " : "",
		    (pr->readres[i] < 0) ? strerror(errno) : ""));
    }

    /*
     * Make sure all the reads were the same length
     */
    for (j = 0; j < (size_t)pr->nfds; j++) {
	if (pr->readres[j] != maxreadres) {
	    nerrors++;
	    errorblock = (int)j;
	}
    }

    /*
     * If no errors, check that the xor sum matches
     */
    if ( nerrors == 0 && pr->nfds > 1  ) {
       for(i = 0; i < (int)maxreadres; i++ ) {
	   int sum = 0;
	   for(j = 0; (j + 1) < (size_t)pr->nfds; j++) {
	       sum ^= (buf + len * j)[i];
           }
	   if (sum != pr->xorbuf[i]) {
	      sum_mismatch = 1;
	   }
       }
    }

    /*
    ** now decide what "really" happened --
    ** all n getting eof is a "real" eof
    ** just one getting an error/eof is recoverable if we are doing RAIT
    ** anything else fails
    */

    if (neofs == pr->nfds) {
	rait_debug((stderr, "rait_read:returning 0\n"));
	return 0;
    }

    if (sum_mismatch) {
	errno = EDOM;
	rait_debug((stderr, "rait_read:returning %d: %s\n",
			    -1,
			    "XOR block mismatch"));
	return -1;
    }

    if (nerrors > 1 || (pr->nfds <= 1 && nerrors > 0)) {
	errno = save_errno;
	rait_debug((stderr, "rait_read:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    /*
    ** so now if we failed on a data block, we need to do a recovery
    ** if we failed on the xor block -- who cares?
    */
    if (nerrors == 1 && pr->nfds > 1 && errorblock != pr->nfds-1) {

	rait_debug((stderr, "rait_read: fixing data from fd %d\n",
			    pr->fds[errorblock]));

	/* the reads were all *supposed* to be the same size, so... */
	pr->readres[errorblock] = maxreadres;

	/* fill it in first with the xor sum */
	memcpy(buf + len * errorblock, pr->xorbuf, len);

	/* xor back out the other blocks */
	for( i = 0; i < data_fds; i++ ) {
	    if( i != errorblock ) {
		for( j = 0; j < len ; j++ ) {
		    buf[j + len * errorblock] ^= buf[j + len * i];
		}
	    }
	}
	/* there, now the block is back as if it never failed... */
    }

    /* pack together partial reads... */
    total = pr->readres[0];
    for( i = 1; i < data_fds; i++ ) {
	if (total != (ssize_t)(len * i)) {
	    memmove(buf + total, buf + len*i, (size_t)pr->readres[i]);
        }
	total += pr->readres[i];
    }

    rait_debug((stderr, "rait_read:returning %d%s%s\n",
			total,
			(total < 0) ? ": " : "",
			(total < 0) ? strerror(errno) : ""));

    return total;
}

/**/

int
rait_ioctl(
    int		fd,
    int		op,
    void *	p)
{
    int i, res = 0;
    RAIT *pr;
    int errors = 0;

    rait_debug((stderr, "rait_ioctl(%d,%d)\n",fd,op));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_ioctl:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_ioctl:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    for( i = 0; i < pr->nfds ; i++ ) {
	/*@ignore@*/
	res = ioctl(pr->fds[i], op, p);
	/*@end@*/
	if ( res != 0 ) {
	    errors++;
	    if (errors > 1) {
		break;
	    }
	    res = 0;
	}
    }

    rait_debug((stderr, "rait_ioctl: returning %d%s%s\n",
			res,
			(res < 0) ? ": " : "",
			(res < 0) ? strerror(errno) : ""));

    return res;
}

/*
** access() all the devices, returning if any fail
*/
int
rait_access(
    char *	devname,
    int		flags)
{
    int res = 0;
    char *dev_left;		/* string before { */
    char *dev_right;		/* string after } */
    char *dev_next;		/* string inside {} */
    char *dev_real;		/* parsed device name */

    /* copy and parse the dev string so we can scribble on it */
    devname = stralloc(devname);
    if (0 == devname) {
	rait_debug((stderr, "rait_access:returning %d: %s\n",
			    -1,
			    "out of stralloc memory"));
	return -1;
    }
    if ( 0 != tapeio_init_devname(devname, &dev_left, &dev_right, &dev_next)) {
	rait_debug((stderr, "rait_access:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    while( 0 != (dev_real = tapeio_next_devname(dev_left, dev_right, &dev_next))) {
	res = tape_access(dev_real, flags);
	rait_debug((stderr,"rait_access:access( %s, %d ) yields %d\n",
		dev_real, flags, res ));
	amfree(dev_real);
	if (res < 0) {
	    break;
        }
    }
    amfree(devname);

    rait_debug((stderr, "rait_access: returning %d%s%s\n",
			res,
			(res < 0) ? ": " : "",
			(res < 0) ? strerror(errno) : ""));

    return res;
}

/*
** stat all the devices, returning the last one unless one fails
*/
int
rait_stat(
    char *	 devname,
    struct stat *buf)
{
    int res = 0;
    char *dev_left;		/* string before { */
    char *dev_right;		/* string after } */
    char *dev_next;		/* string inside {} */
    char *dev_real;		/* parsed device name */

    /* copy and parse the dev string so we can scribble on it */
    devname = stralloc(devname);
    if (0 == devname) {
	rait_debug((stderr, "rait_access:returning %d: %s\n",
			    -1,
			    "out of stralloc memory"));
	return -1;
    }
    if ( 0 != tapeio_init_devname(devname, &dev_left, &dev_right, &dev_next)) {
	rait_debug((stderr, "rait_access:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    while( 0 != (dev_real = tapeio_next_devname(dev_left, dev_right, &dev_next))) {
	res = tape_stat(dev_real, buf);
	rait_debug((stderr,"rait_stat:stat( %s ) yields %d (%s)\n",
		dev_real, res, (res != 0) ? strerror(errno) : "no error" ));
	amfree(dev_real);
	if (res != 0) {
	    break;
        }
    }
    amfree(devname);

    rait_debug((stderr, "rait_access: returning %d%s%s\n",
			res,
			(res < 0) ? ": " : "",
			(res < 0) ? strerror(errno) : ""));

    return res;
}

/**/

int
rait_copy(
    char *	f1,
    char *	f2,
    size_t	buflen)
{
    int t1, t2;
    ssize_t len;
    ssize_t wres;
    char *buf;
    int save_errno;

    t1 = rait_open(f1,O_RDONLY,0644);
    if (t1 < 0) {
	return t1;
    }
    t2 = rait_open(f2,O_CREAT|O_RDWR,0644);
    if (t2 < 0) {
	save_errno = errno;
	(void)rait_close(t1);
	errno = save_errno;
	return -1;
    }
    buf = alloc(buflen);
    do {
	len = rait_read(t1,buf,buflen);
	if (len > 0 ) {
	    wres = rait_write(t2, buf, (size_t)len);
	    if (wres < 0) {
		len = -1;
		break;
	    }
	}
    } while( len > 0 );
    save_errno = errno;
    amfree(buf);
    (void)rait_close(t1);
    (void)rait_close(t2);
    errno = save_errno;
    return (len < 0) ? -1 : 0;
}

/**/

/*
** Amanda Tape API routines:
*/

static int
rait_tapefd_ioctl(
    int		(*func0)(int),
    int		(*func1)(int, off_t),
    int		fd,
    off_t	count)
{
    int i, j, res = 0;
    RAIT *pr;
    int errors = 0;
    pid_t kid;
    int status = 0;

    rait_debug((stderr, "rait_tapefd_ioctl(%d,%d)\n",fd,count));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_tapefd_ioctl:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_tapefd_ioctl:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    if (0 == pr->readres && 0 < pr->nfds) {
	pr->readres = alloc(pr->nfds * SIZEOF(*pr->readres));
	memset(pr->readres, 0, pr->nfds * SIZEOF(*pr->readres));
    }

    for( i = 0; i < pr->nfds ; i++ ) {
	if(tapefd_can_fork(pr->fds[i])) {
            if ((kid = fork()) < 1) {
		rait_debug((stderr, "in kid, fork returned %d\n", kid));
		/* if we are the kid, or fork failed do the action */
		if (func0 != NULL) {
		    res = (*func0)(pr->fds[i]);
		} else {
		    res = (*func1)(pr->fds[i], count);
		}
		rait_debug((stderr, "in kid, func (%d) returned %d errno %s\n",
				pr->fds[i], res, strerror(errno)));
		if (kid == 0)
		    exit(res);
            } else {
		rait_debug((stderr, "in parent, fork returned %d\n", kid));
		pr->readres[i] = (ssize_t)kid;
            }
	}
	else {
	    if(func0 != NULL) {
		j = (*func0)(pr->fds[i]);
	    } else {
		j = (*func1)(pr->fds[i], count);
	    }
	    if( j != 0) {
		errors++;
	    }
	    pr->readres[i] = -1;
	}
    }
    for( i = 0; i < pr->nfds ; i++ ) {
	if(tapefd_can_fork(pr->fds[i])) {
            rait_debug((stderr, "in parent, waiting for %d\n", pr->readres[i]));
	    waitpid((pid_t)pr->readres[i], &status, 0);
	    if( WEXITSTATUS(status) != 0 ) {
		res = WEXITSTATUS(status);
		if( res == 255 )
		    res = -1;
            }
            rait_debug((stderr, "in parent, return code was %d\n", res));
	    if ( res != 0 ) {
		errors++;
		res = 0;
	    }
	}
    }
    if (errors > 0) {
	res = -1;
    }

    rait_debug((stderr, "rait_tapefd_ioctl: returning %d%s%s\n",
			res,
			(res < 0) ? ": " : "",
			(res < 0) ? strerror(errno) : ""));

    return res;
}

int
rait_tapefd_fsf(
    int		fd,
    off_t	count)
{
    return rait_tapefd_ioctl(NULL, tapefd_fsf, fd, count);
}

int
rait_tapefd_rewind(
    int		fd)
{
    return rait_tapefd_ioctl(tapefd_rewind, NULL, fd, (off_t)-1);
}

int
rait_tapefd_unload(
    int		fd)
{
    return rait_tapefd_ioctl(tapefd_unload, NULL, fd, (off_t)-1);
}

int
rait_tapefd_weof(
    int		fd,
    off_t	count)
{
    return rait_tapefd_ioctl(NULL, tapefd_weof, fd, count);
}

int
rait_tape_open(
    char *	name,
    int		flags,
    mode_t	mask)
{
    return rait_open(name, flags, mask);
}

int
rait_tapefd_status(
    int			 fd,
    struct am_mt_status *stat)
{
    int i;
    RAIT *pr;
    int res = 0;
    int errors = 0;

    rait_debug((stderr, "rait_tapefd_status(%d)\n",fd));

    if ((fd < 0) || ((size_t)fd >= rait_table_count)) {
	errno = EBADF;
	rait_debug((stderr, "rait_tapefd_status:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    pr = &rait_table[fd];
    if (0 == pr->nopen) {
	errno = EBADF;
	rait_debug((stderr, "rait_tapefd_status:returning %d: %s\n",
			    -1,
			    strerror(errno)));
	return -1;
    }

    for( i = 0; i < pr->nfds ; i++ ) {
	res = tapefd_status(pr->fds[i], stat);
	if(res != 0) {
	    errors++;
	}
    }
    if (errors > 0) {
	res = -1;
    }
    return res;
}

void
rait_tapefd_resetofs(
    int		fd)
{
    (void)rait_lseek(fd, (off_t)0, SEEK_SET);
}

int
rait_tapefd_can_fork(
    int		fd)
{
    (void)fd;	/* Quiet unused parameter warning */

    return 0;
}

