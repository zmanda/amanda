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
 * $Id: getconf.c,v 1.26 2006/07/25 19:00:56 martinea Exp $
 *
 * a little wrapper to extract config variables for shell scripts
 */
#include "amanda.h"
#include "version.h"
#include "genversion.h"
#include "conffile.h"

int main(int argc, char **argv);

/*
 * HOSTNAME_INSTANCE may not be defined at this point.
 * We define it locally if it is needed...
 *
 * If CLIENT_HOST_PRINCIPLE is defined as HOSTNAME_INSTANCE
 * then local host is the client host principle.
 */
#ifndef HOSTNAME_INSTANCE
#  define HOSTNAME_INSTANCE "localhost"
#endif

#ifndef KEYFILE
#  define KEYFILE "id_rsa"
#endif

static struct build_info {
    char *symbol;
    char *value;
} build_info[] = {
    { "VERSION",			"" },	/* must be [0] */
    { "AMANDA_DEBUG_DAYS",		"" },	/* must be [1] */
    { "TICKET_LIFETIME",		"" },	/* must be [2] */

    { "bindir",				bindir },
    { "sbindir",			sbindir },
    { "libexecdir",			libexecdir },
    { "mandir",				mandir },
    { "AMANDA_TMPDIR",			AMANDA_TMPDIR },
    { "CONFIG_DIR",			CONFIG_DIR },
    { "MAILER",				MAILER },
    { "DEFAULT_SERVER",			DEFAULT_SERVER },
    { "DEFAULT_CONFIG",			DEFAULT_CONFIG },
    { "DEFAULT_TAPE_SERVER",		DEFAULT_TAPE_SERVER },
#ifdef DEFAULT_TAPE_DEVICE
    { "DEFAULT_TAPE_DEVICE",		DEFAULT_TAPE_DEVICE },
#endif
    { "CLIENT_LOGIN",			CLIENT_LOGIN },

    { "BUILT_DATE",
#if defined(BUILT_DATE)
	BUILT_DATE
#else
	NULL
#endif
    },
    { "BUILT_MACH",
#if defined(BUILT_MACH)
	BUILT_MACH
#else
	NULL
#endif
    },
    { "CC",
#if defined(CC)
	CC
#else
	NULL
#endif
    },

    { "AMANDA_DBGDIR",
#if defined(AMANDA_DBGDIR)
	AMANDA_DBGDIR
#else
	NULL
#endif
    },
    { "DEV_PREFIX",
#if defined(DEV_PREFIX)
	DEV_PREFIX
#else
	NULL
#endif
    },
    { "RDEV_PREFIX",
#if defined(RDEV_PREFIX)
	RDEV_PREFIX
#else
	NULL
#endif
    },
    { "DUMP",
#if defined(DUMP)
	DUMP
#else
	NULL
#endif
    },
    { "RESTORE",
#if defined(DUMP)
	RESTORE
#else
	NULL
#endif
    },
    { "VDUMP",
#if defined(VDUMP)
	VDUMP
#else
	NULL
#endif
    },
    { "VRESTORE",
#if defined(VDUMP)
	VRESTORE
#else
	NULL
#endif
    },
    { "XFSDUMP",
#if defined(XFSDUMP)
	XFSDUMP
#else
	NULL
#endif
    },
    { "XFSRESTORE",
#if defined(XFSDUMP)
	XFSRESTORE
#else
	NULL
#endif
    },
    { "VXDUMP",
#if defined(VXDUMP)
	VXDUMP
#else
	NULL
#endif
    },
    { "VXRESTORE",
#if defined(VXDUMP)
	VXRESTORE
#else
	NULL
#endif
    },
    { "SAMBA_CLIENT",
#if defined(SAMBA_CLIENT)
	SAMBA_CLIENT
#else
	NULL
#endif
    },
    { "GNUTAR",
#if defined(GNUTAR)
	GNUTAR
#else
	NULL
#endif
    },
    { "COMPRESS_PATH",
#if defined(COMPRESS_PATH)
	COMPRESS_PATH
#else
	NULL
#endif
    },
    { "UNCOMPRESS_PATH",
#if defined(UNCOMPRESS_PATH)
	UNCOMPRESS_PATH
#else
	NULL
#endif
    },
    { "listed_incr_dir",
#if defined(GNUTAR_LISTED_INCREMENTAL_DIR)
	GNUTAR_LISTED_INCREMENTAL_DIR
#else
	NULL
#endif
    },
    { "GNUTAR_LISTED_INCREMENTAL_DIR",
#if defined(GNUTAR_LISTED_INCREMENTAL_DIR)
	GNUTAR_LISTED_INCREMENTAL_DIR
#else
	NULL
#endif
    },

    { "AIX_BACKUP",
#if defined(AIX_BACKUP)
	"1"
#else
	NULL
#endif
    },
    { "AIX_TAPEIO",
#if defined(AIX_TAPEIO)
	"1"
#else
	NULL
#endif
    },
    { "DUMP_RETURNS_1",
#if defined(DUMP_RETURNS_1)
	"1"
#else
	NULL
#endif
    },

    { "LOCKING",
#if defined(USE_POSIX_FCNTL)
	"POSIX_FCNTL"
#elif defined(USE_FLOCK)
	"FLOCK"
#elif defined(USE_LOCKF)
	"LOCKF"
#elif defined(USE_LNLOCK)
	"LNLOCK"
#else
	"NONE"
#endif
    },

    { "STATFS_BSD",
#if defined(STATFS_BSD)
	"1"
#else
	NULL
#endif
    },
    { "STATFS_OSF1",
#if defined(STATFS_OSF1)
	"1"
#else
	NULL
#endif
    },
    { "STATFS_ULTRIX",
#if defined(STATFS_ULTRIX)
	"1"
#else
	NULL
#endif
    },
    { "ASSERTIONS",
#if defined(ASSERTIONS)
	"1"
#else
	NULL
#endif
    },
    { "DEBUG_CODE",
#if defined(DEBUG_CODE)
	"1"
#else
	NULL
#endif
    },
    { "BSD_SECURITY",
#if defined(BSD_SECURITY)
	"1"
#else
	NULL
#endif
    },
    { "USE_AMANDAHOSTS",
#if defined(USE_AMANDAHOSTS)
	"1"
#else
	NULL
#endif
    },
    { "USE_RUNDUMP",
#if defined(USE_RUNDUMP)
	"1"
#else
	NULL
#endif
    },
    { "FORCE_USERID",
#if defined(FORCE_USERID)
	"1"
#else
	NULL
#endif
    },
    { "USE_VERSION_SUFFIXES",
#if defined(USE_VERSION_SUFFIXES)
	"1"
#else
	NULL
#endif
    },
    { "HAVE_GZIP",
#if defined(HAVE_GZIP)
	"1"
#else
	NULL
#endif
    },

    { "KRB4_SECURITY",
#if defined(KRB4_SECURITY)
	"1"
#else
	NULL
#endif
    },
    { "SERVER_HOST_PRINCIPLE",
#if defined(KRB4_SECURITY)
	SERVER_HOST_PRINCIPLE
#else
	NULL
#endif
    },
    { "SERVER_HOST_INSTANCE",
#if defined(KRB4_SECURITY)
	SERVER_HOST_INSTANCE
#else
	NULL
#endif
    },
    { "SERVER_HOST_KEY_FILE",
#if defined(KRB4_SECURITY)
	SERVER_HOST_KEY_FILE
#else
	NULL
#endif
    },
    { "CLIENT_HOST_PRINCIPLE",
#if defined(KRB4_SECURITY)
	CLIENT_HOST_PRINCIPLE
#else
	NULL
#endif
    },
    { "CLIENT_HOST_INSTANCE",
#if defined(KRB4_SECURITY)
	CLIENT_HOST_INSTANCE
#else
	NULL
#endif
    },
    { "CLIENT_HOST_KEY_FILE",
#if defined(KRB4_SECURITY)
	CLIENT_HOST_KEY_FILE
#else
	NULL
#endif
    },

    { "COMPRESS_SUFFIX",
#if defined(COMPRESS_SUFFIX)
	COMPRESS_SUFFIX
#else
	NULL
#endif
    },
    { "COMPRESS_FAST_OPT",
#if defined(COMPRESS_FAST_OPT)
	COMPRESS_FAST_OPT
#else
	NULL
#endif
    },
    { "COMPRESS_BEST_OPT",
#if defined(COMPRESS_BEST_OPT)
	COMPRESS_BEST_OPT
#else
	NULL
#endif
    },
    { "UNCOMPRESS_OPT",
#if defined(UNCOMPRESS_OPT)
	UNCOMPRESS_OPT
#else
	NULL
#endif
    },

    { NULL,			NULL }
};

int
main(
    int		argc,
    char **	argv)
{
    char *result;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    char *pgm;
    char *conffile;
    char *parmname;
    int i;
    char number[NUM_STR_SIZE];
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

    safe_fd(-1, 0);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    parse_server_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    if((pgm = strrchr(my_argv[0], '/')) == NULL) {
	pgm = my_argv[0];
    } else {
	pgm++;
    }
    set_pname(pgm);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    if(my_argc < 2) {
	fprintf(stderr, "Usage: %s [config] <parmname> [-o configoption]*\n", pgm);
	exit(1);
    }

    if (my_argc > 2) {
	config_name = stralloc(my_argv[1]);
	config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
	parmname = my_argv[2];
    } else {
	char my_cwd[STR_SIZE];

	if (getcwd(my_cwd, SIZEOF(my_cwd)) == NULL) {
	    error("cannot determine current working directory");
	    /*NOTREACHED*/
	}
	config_dir = stralloc2(my_cwd, "/");
	if ((config_name = strrchr(my_cwd, '/')) != NULL) {
	    config_name = stralloc(config_name + 1);
	}
	parmname = my_argv[1];
    }

    safe_cd();

    /*
     * Fill in the build values that need runtime help.
     */
    build_info[0].value = stralloc(version());
#if defined(AMANDA_DEBUG_DAYS)
    i = AMANDA_DEBUG_DAYS;
#else
    i = -1;
#endif
    snprintf(number, SIZEOF(number), "%ld", (long)i);
    build_info[1].value = stralloc(number);
#if defined(KRB4_SECURITY)
    i = TICKET_LIFETIME;
#else
    i = -1;
#endif
    snprintf(number, SIZEOF(number), "%ld", (long)i);
    build_info[2].value = stralloc(number);

#undef p
#define	p	"build."

    if(strncmp(parmname, p, SIZEOF(p) - 1) == 0) {
	char *s;
	char *t;

	t = stralloc(parmname + SIZEOF(p) - 1);
	for(i = 0; (s = build_info[i].symbol) != NULL; i++) {
	    if(strcasecmp(s, t) == 0) {
		break;
	    }
	}
	if(s == NULL) {
	    result = NULL;
	} else {
	    result = build_info[i].value;
	    result = stralloc(result ? result : "");
	}

#undef p
#define	p	"dbopen."

    } else if(strncmp(parmname, p, SIZEOF(p) - 1) == 0) {
	char *pname;
	char *dbname;

	if((pname = strrchr(parmname + SIZEOF(p) - 1, '/')) == NULL) {
	    pname = parmname + SIZEOF(p) - 1;
	} else {
	    pname++;
	}
	set_pname(pname);
	dbopen(DBG_SUBDIR_SERVER);
	if((dbname = dbfn()) == NULL) {
	    result = stralloc("/dev/null");
	} else {
	    result = stralloc(dbname);
	}
	/*
	 * Note that we deliberately do *not* call dbclose to prevent
	 * the end line from being added to the file.
	 */

#undef p
#define	p	"dbclose."

    } else if(strncmp(parmname, p, SIZEOF(p) - 1) == 0) {
	char *t;
	char *pname;
	char *dbname;

	t = stralloc(parmname + SIZEOF(p) - 1);
	if((dbname = strchr(t, ':')) == NULL) {
	    error("cannot parse %s", parmname);
	    /*NOTREACHED*/
	}
	*dbname++ = '\0';
	if((pname = strrchr(t, '/')) == NULL) {
	    pname = t;
	} else {
	    pname++;
	}
	fflush(stderr);
	set_pname(pname);
	dbreopen(dbname, NULL);
	dbclose();
	result = stralloc(dbname);
	amfree(t);

    } else {
	conffile = stralloc2(config_dir, CONFFILE_NAME);
	if(read_conffile(conffile)) {
	    error("errors processing config file \"%s\"", conffile);
	    /*NOTREACHED*/
	}
	amfree(conffile);
	dbrename(config_name, DBG_SUBDIR_SERVER);
	report_bad_conf_arg();
	result = getconf_byname(parmname);
    }

    if (result == NULL) {
	fprintf(stderr, "%s: no such parameter \"%s\"\n",
		get_pname(), parmname);
	fflush(stderr);
    } else {
	puts(result);
    }

    free_new_argv(new_argc, new_argv);
    free_server_config();
    amfree(result);
    amfree(config_dir);
    amfree(config_name);
    for(i = 0; i < 3; i++) {
	amfree(build_info[i].value);
    }

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    return 0;
}
