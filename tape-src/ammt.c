#ifdef NO_AMANDA
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "output-rait.h"

extern char *getenv();

#define	tape_open	rait_open
#define	tapefd_read	rait_read
#define	tapefd_write	rait_write
#define	tapefd_fsf	rait_tapefd_fsf
#define	tapefd_rewind	rait_tapefd_rewind
#define	tapefd_status	rait_tapefd_status
#define	tapefd_unload	rait_tapefd_unload
#define	tapefd_weof	rait_tapefd_weof
#define tapefd_setinfo_length(outfd, length)
#define	tapefd_close	rait_close

#else
#include "amanda.h"
#include "tapeio.h"
#endif

extern int optind;

static int do_asf(int fd, off_t count);
static int do_bsf(int fd, off_t count);
static int do_status(int fd, off_t count);
static void usage(void);

struct cmd {
    char *name;
    size_t min_chars;
    int count;
    int (*func)(int, off_t);
    int flags;
} cmd[] = {
    { "eof",		0,	1,	tapefd_weof,	O_RDWR },
    { "weof",		0,	1,	tapefd_weof,	O_RDWR },
    { "fsf",		0,	1,	tapefd_fsf,	O_RDONLY },
    { "asf",		0,	0,	do_asf,		O_RDONLY },
    { "bsf",		0,	1,	do_bsf,		O_RDONLY },
    { "rewind",		0,	0,	(int (*)(int, off_t))tapefd_rewind,
							O_RDONLY },
    { "offline",	0,	0,	(int (*)(int, off_t))tapefd_unload,
							O_RDONLY },
    { "rewoffl",	0,	0,	(int (*)(int, off_t))tapefd_unload,
							O_RDONLY },
    { "status",		0,	0,	do_status,	O_RDONLY },
    { NULL,		0,	0,	NULL,		0 }
};

static char *pgm;
static int debug_ammt = 0;

static char *tapename;

static int
do_asf(
    int		fd,
    off_t	count)
{
    int r;

    if(debug_ammt) {
	g_fprintf(stderr, _("calling tapefd_rewind()\n"));
    }
    if(0 != (r = tapefd_rewind(fd))) {
	return r;
    }
    if(debug_ammt) {
	g_fprintf(stderr, _("calling tapefd_fsf(%lld)\n"), (long long)count);
    }
    return tapefd_fsf(fd, count);
}

static int
do_bsf(
    int		fd,
    off_t	count)
{
    if(debug_ammt) {
	g_fprintf(stderr, _("calling tapefd_fsf(%lld)\n"), (long long)-count);
    }
    return tapefd_fsf(fd, -count);
}

static int
do_status(
    int		fd,
    off_t	count)
{
    int ret;
    struct am_mt_status stat;

    (void)count;	/* Quiet unused parameter warning */

    if(debug_ammt) {
	g_fprintf(stderr, _("calling tapefd_status()\n"));
    }
    if((ret = tapefd_status(fd, &stat)) != 0) {
	return ret;
    }
    g_printf(_("%s status:"), tapename);
    if(stat.online_valid) {
	if(stat.online) {
	    fputs(_(" ONLINE"), stdout);
	} else {
	    fputs(_(" OFFLINE"), stdout);
	}
    }
    if(stat.bot_valid && stat.bot) {
	fputs(_(" BOT"), stdout);
    }
    if(stat.eot_valid && stat.eot) {
	fputs(_(" EOT"), stdout);
    }
    if(stat.protected_valid && stat.protected) {
	fputs(_(" PROTECTED"), stdout);
    }
    if(stat.device_status_valid) {
	g_printf(_(" ds == 0x%0*lx"),
	       stat.device_status_size * 2,
	       (unsigned long)stat.device_status);
    }
    if(stat.error_status_valid) {
	g_printf(_(" er == 0x%0*lx"),
	       stat.error_status_size * 2,
	       (unsigned long)stat.error_status);
    }
    if(stat.fileno_valid) {
	g_printf(_(" fileno == %ld"), stat.fileno);
    }
    if(stat.blkno_valid) {
	g_printf(_(" blkno == %ld"), stat.blkno);
    }

    putchar('\n');
    return 0;
}

static void
usage(void)
{
    g_fprintf(stderr, _("usage: %s [-d] [-f|-t device] command [count]\n"), pgm);
    exit(1);
}

int
main(
    int		argc,
    char **	argv)
{
    int ch;
    off_t count;
    size_t i;
    size_t j;
    int fd;
    int save_errno;
    char *s;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    fprintf(stderr, _("ammt is deprecated\n"));

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    if((pgm = strrchr(argv[0], '/')) != NULL) {
	pgm++;
    } else {
	pgm = argv[0];
    }
    tapename = getenv("TAPE");
    while(-1 != (ch = getopt(argc, argv, "df:t:"))) {
	switch(ch) {
	case 'd':
	    debug_ammt = 1;
	    g_fprintf(stderr, _("debug mode!\n"));
	    break;
	case 'f':
	case 't':
	    tapename = stralloc(optarg);
	    break;
	default:
	    usage();
	    /*NOTREACHED*/
	}
    }
    if(optind >= argc) {
	usage();
	/*NOTREACHED*/
    }

    /*
     * Compute the minimum abbreviation for each command.
     */
    for(i = 0; cmd[i].name; i++) {
	cmd[i].min_chars = (size_t)1;
	while (1) {
	    for(j = 0; cmd[j].name; j++) {
		if(i == j) {
		    continue;
		}
		if(0 == strncmp(cmd[i].name, cmd[j].name,
				cmd[i].min_chars)) {
		    break;
		}
	    }
	    if(0 == cmd[j].name) {
		break;
	    }
	    cmd[i].min_chars++;
	}
	if(debug_ammt) {
	    g_fprintf(stderr, _("syntax: %-20s -> %*.*s\n"),
			    cmd[i].name,
			    (int)cmd[i].min_chars,
			    (int)cmd[i].min_chars,
			    cmd[i].name);
	}
    }

    /*
     * Process the command.
     */
    s = _("unknown");
    j = strlen(argv[optind]);
    for(i = 0; cmd[i].name; i++) {
	if(0 == strncmp(cmd[i].name, argv[optind], j)) {
	    if(j >= cmd[i].min_chars) {
		break;
	    }
	    s = _("ambiguous");
	}
    }
    if(0 == cmd[i].name) {
	g_fprintf(stderr, _("%s: %s command: %s\n"), pgm, s, argv[optind]);
	exit(1);
    }
    optind++;
    if(0 == tapename) {
	g_fprintf(stderr, _("%s: -f device or -t device is required\n"), pgm);
	exit(1);
    }
    if(debug_ammt) {
	g_fprintf(stderr, _("tapename is \"%s\"\n"), tapename);
    }

    count = (off_t)1;
    if(optind < argc && cmd[i].count) {
	count = OFF_T_ATOI(argv[optind]);
    }

    if(debug_ammt) {
	g_fprintf(stderr, _("calling tape_open(\"%s\",%d)\n"), tapename, cmd[i].flags);
    }
    if((fd = tape_open(tapename, cmd[i].flags, 0)) < 0) {
	goto report_error;
    }

    if(debug_ammt) {
	g_fprintf(stderr, _("processing %s(%lld)\n"),
		cmd[i].name, (long long)count);
    }
    if(0 != (*cmd[i].func)(fd, count)) {
	goto report_error;
    }

    (void)tapefd_close(fd);

    exit(0);

report_error:

    save_errno = errno;
    g_fprintf(stderr, _("%s %s"), tapename, cmd[i].name);
    if(cmd[i].count) {
	g_fprintf(stderr, " %lld", (long long)count);
    }
    errno = save_errno;
    perror(_(" failed"));
    return (1); /* exit */
}
