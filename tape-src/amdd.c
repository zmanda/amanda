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
    g_fprintf(stderr, _("usage: %s "), pgm);
    g_fprintf(stderr, _(" [-d]"));
    g_fprintf(stderr, _(" [-l length]"));
    g_fprintf(stderr, _(" [if=input]"));
    g_fprintf(stderr, _(" [of=output]"));
    g_fprintf(stderr, _(" [bs=blocksize]"));
    g_fprintf(stderr, _(" [count=count]"));
    g_fprintf(stderr, _(" [skip=count]"));
    g_fprintf(stderr, _("\n"));
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

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    fprintf(stderr, _("amdd is deprecated\n"));

    if((pgm = strrchr(argv[0], '/')) != NULL) {
	pgm++;
    } else {
	pgm = argv[0];
    }
    while(-1 != (ch = getopt(argc, argv, "hdl:"))) {
	switch(ch) {
	case 'd':
	    debug_amdd = 1;
	    g_fprintf(stderr, _("debug mode!\n"));
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
		g_fprintf(stderr, "%s: %s: ", pgm, eq + 1);
		errno = save_errno;
		perror("open");
		return 1;
	    }
	    read_func = tapefd_read;
            if(debug_amdd) {
		g_fprintf(stderr, _("input opened \"%s\", got fd %d\n"),
				eq + 1, infd);
	    }
	} else if(0 == strncmp("of", argv[optind], (size_t)len)) {
	    if((outfd = tape_open(eq + 1, O_RDWR|O_CREAT|O_TRUNC, 0644)) < 0) {
		save_errno = errno;
		g_fprintf(stderr, "%s: %s: ", pgm, eq + 1);
		errno = save_errno;
		perror("open");
		return 1;
	    }
	    write_func = tapefd_write;
            if(debug_amdd) {
		g_fprintf(stderr, _("output opened \"%s\", got fd %d\n"),
				eq + 1, outfd);
	    }
	    if(have_length) {
		if(debug_amdd) {
		    g_fprintf(stderr, _("length set to %lld\n"),
			(long long)length);
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
		g_fprintf(stderr, _("blocksize set to %zu\n"), blocksize);
	    }
	} else if(0 == strncmp("count", argv[optind], (size_t)len)) {
	    count = OFF_T_ATOI(eq + 1);
	    have_count = 1;
	    if(debug_amdd) {
		g_fprintf(stderr, _("count set to %lld\n"), (long long)count);
	    }
	} else if(0 == strncmp("skip", argv[optind], (size_t)len)) {
	    skip = OFF_T_ATOI(eq + 1);
	    if(debug_amdd) {
		g_fprintf(stderr, _("skip set to %lld\n"), (long long)skip);
	    }
	} else {
	    g_fprintf(stderr, _("%s: bad argument: \"%s\"\n"), pgm, argv[optind]);
	    return 1;
	}
    }

    if(0 == (buf = malloc(blocksize))) {
	save_errno = errno;
	g_fprintf(stderr, "%s: ", pgm);
	errno = save_errno;
	perror(_("malloc error"));
	return 1;
    }

    eq = _("read error");
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
	    eq = _("write error");
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
	g_fprintf(stderr, "%s: ", pgm);
	errno = save_errno;
	perror(eq);
	res = 1;
    }
    g_fprintf(stderr, _("%d+%d in\n%d+%d out\n"), fread, pread, fwrite, pwrite);
    if(read_func == tapefd_read) {
	if(0 != tapefd_close(infd)) {
	    save_errno = errno;
	    g_fprintf(stderr, "%s: ", pgm);
	    errno = save_errno;
	    perror(_("input close"));
	    res = 1;
	}
    }
    if(write_func == tapefd_write) {
	if(0 != tapefd_close(outfd)) {
	    save_errno = errno;
	    g_fprintf(stderr, "%s: ", pgm);
	    errno = save_errno;
	    perror(_("output close"));
	    res = 1;
	}
    }
    return res;
}
