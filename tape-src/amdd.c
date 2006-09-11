#ifdef NO_AMANDA
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "output-rait.h"

#define	tape_open	rait_open
#define	tapefd_read	rait_read
#define	tapefd_write	rait_write
#define tapefd_setinfo_length(outfd, length)
#define	tapefd_close	rait_close

#else
#include "amanda.h"
#include "tapeio.h"
#endif

extern int optind;

static int debug_amdd = 0;
static char *pgm = NULL;

static void usage(void);

static void
usage(void)
{
    fprintf(stderr, "usage: %s ", pgm);
    fprintf(stderr, " [-d]");
    fprintf(stderr, " [-l length]");
    fprintf(stderr, " [if=input]");
    fprintf(stderr, " [of=output]");
    fprintf(stderr, " [bs=blocksize]");
    fprintf(stderr, " [count=count]");
    fprintf(stderr, " [skip=count]");
    fprintf(stderr, "\n");
    exit(1);
}

static ssize_t (*read_func)(int, void *, size_t);
static ssize_t (*write_func)(int, const void *, size_t);

int
main(
    int		argc,
    char **	argv)
{
    int infd = 0;				/* stdin */
    int outfd = 1;				/* stdout */
    size_t blocksize = 512;
    off_t skip = (off_t)0;
    ssize_t len;
    int pread, fread, pwrite, fwrite;
    int res = 0;
    char *buf;
    off_t count = (off_t)0;
    int have_count = 0;
    int save_errno;
    int ch;
    char *eq;
    off_t length = (off_t)0;
    int have_length = 0;

    if((pgm = strrchr(argv[0], '/')) != NULL) {
	pgm++;
    } else {
	pgm = argv[0];
    }
    while(-1 != (ch = getopt(argc, argv, "hdl:"))) {
	switch(ch) {
	case 'd':
	    debug_amdd = 1;
	    fprintf(stderr, "debug mode!\n");
	    break;

#ifndef __lint
	case 'l':
	    have_length = 1;
	    length = OFF_T_ATOI(optarg);
	    len = (ssize_t)strlen(optarg);
	    if(len > 0) {
		switch(optarg[len-1] ) {
		case 'k':				break;
		case 'b': length /= (off_t)2;	 	break;
		case 'M': length *= (off_t)1024;	break;
		default:  length /= (off_t)1024;	break;
		}
	    } else {
		length /= (off_t)1024;
	    }
	    break;
#endif
	case 'h':
	default:
	    usage();
	    /*NOTREACHED*/
	}
    }

    /*@ignore@*/
    read_func = read;
    write_func = write;
    /*@end@*/
    for( ; optind < argc; optind++) {
	if(0 == (eq = strchr(argv[optind], '='))) {
	    usage();
	    /*NOTREACHED*/
	}
	len = (ssize_t)(eq - argv[optind]);
	if(0 == strncmp("if", argv[optind], (size_t)len)) {
	    if((infd = tape_open(eq + 1, O_RDONLY, 0)) < 0) {
		save_errno = errno;
		fprintf(stderr, "%s: %s: ", pgm, eq + 1);
		errno = save_errno;
		perror("open");
		return 1;
	    }
	    read_func = tapefd_read;
            if(debug_amdd) {
		fprintf(stderr, "input opened \"%s\", got fd %d\n",
				eq + 1, infd);
	    }
	} else if(0 == strncmp("of", argv[optind], (size_t)len)) {
	    if((outfd = tape_open(eq + 1, O_RDWR|O_CREAT|O_TRUNC, 0644)) < 0) {
		save_errno = errno;
		fprintf(stderr, "%s: %s: ", pgm, eq + 1);
		errno = save_errno;
		perror("open");
		return 1;
	    }
	    write_func = tapefd_write;
            if(debug_amdd) {
		fprintf(stderr, "output opened \"%s\", got fd %d\n",
				eq + 1, outfd);
	    }
	    if(have_length) {
		if(debug_amdd) {
		    fprintf(stderr, "length set to " OFF_T_FMT "\n",
			(OFF_T_FMT_TYPE)length);
		}
		tapefd_setinfo_length(outfd, length);
	    }
	} else if(0 == strncmp("bs", argv[optind], (size_t)len)) {
	    blocksize = SIZE_T_ATOI(eq + 1);
	    len = (ssize_t)strlen(argv[optind]);
	    if(len > 0) {
		switch(argv[optind][len-1] ) {
		case 'k': blocksize *= 1024;		break;
		case 'b': blocksize *= 512; 		break;
		case 'M': blocksize *= 1024 * 1024;	break;
		}
	    }
	    if(debug_amdd) {
		fprintf(stderr, "blocksize set to " SIZE_T_FMT "\n",
			(SIZE_T_FMT_TYPE)blocksize);
	    }
	} else if(0 == strncmp("count", argv[optind], (size_t)len)) {
	    count = OFF_T_ATOI(eq + 1);
	    have_count = 1;
	    if(debug_amdd) {
		fprintf(stderr, "count set to " OFF_T_FMT "\n",
			(OFF_T_FMT_TYPE)count);
	    }
	} else if(0 == strncmp("skip", argv[optind], (size_t)len)) {
	    skip = OFF_T_ATOI(eq + 1);
	    if(debug_amdd) {
		fprintf(stderr, "skip set to " OFF_T_FMT "\n",
			(OFF_T_FMT_TYPE)skip);
	    }
	} else {
	    fprintf(stderr, "%s: bad argument: \"%s\"\n", pgm, argv[optind]);
	    return 1;
	}
    }

    if(0 == (buf = malloc(blocksize))) {
	save_errno = errno;
	fprintf(stderr, "%s: ", pgm);
	errno = save_errno;
	perror("malloc error");
	return 1;
    }

    eq = "read error";
    pread = fread = pwrite = fwrite = 0;
    while(0 < (len = (*read_func)(infd, buf, blocksize))) {
	if((skip -= (off_t)1) > (off_t)0) {
	    continue;
	}
	if((size_t)len == blocksize) {
	    fread++;
	} else if(len > 0) {
	    pread++;
	}
	len = (*write_func)(outfd, buf, (size_t)len);
	if(len < 0) {
	    eq = "write error";
	    break;
	} else if((size_t)len == blocksize) {
	    fwrite++;
	} else if(len > 0) {
	    pwrite++;
	}
	if(have_count) {
	    if((count -= (off_t)1) <= (off_t)0) {
		len = 0;
		break;
	    }
	}
    }
    if(len < 0) {
	save_errno = errno;
	fprintf(stderr, "%s: ", pgm);
	errno = save_errno;
	perror(eq);
	res = 1;
    }
    fprintf(stderr, "%d+%d in\n%d+%d out\n", fread, pread, fwrite, pwrite);
    if(read_func == tapefd_read) {
	if(0 != tapefd_close(infd)) {
	    save_errno = errno;
	    fprintf(stderr, "%s: ", pgm);
	    errno = save_errno;
	    perror("input close");
	    res = 1;
	}
    }
    if(write_func == tapefd_write) {
	if(0 != tapefd_close(outfd)) {
	    save_errno = errno;
	    fprintf(stderr, "%s: ", pgm);
	    errno = save_errno;
	    perror("output close");
	    res = 1;
	}
    }
    return res;
}
