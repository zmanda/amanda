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
/* 
 * $Id: calcsize.c,v 1.44 2006/07/25 18:27:56 martinea Exp $
 *
 * traverse directory tree to get backup size estimates
 *
 * argv[0] is the calcsize program name
 * argv[1] is the config name or NOCONFIG
 */
#include "amanda.h"
#include "match.h"
#include "conffile.h"
#include "fsusage.h"
#include "am_sl.h"
#include "util.h"

#define ROUND(n,x)	((x) + (n) - 1 - (((x) + (n) - 1) % (n)))

/*
static off_t
round_function(n, x)
    off_t	n,
    off_t	x)
{
  unsigned long remainder = x % n;
  if (remainder)
    x += n - remainder;
  return x;
}
*/

#define ST_BLOCKS(s)							       \
	    (((((off_t)(s).st_blocks * (off_t)512) <= (s).st_size)) ?	       \
	      ((off_t)(s).st_blocks + (off_t)1) :			       \
	      ((s).st_size / (off_t)512 +				       \
		(off_t)((((s).st_size % (off_t)512) != (off_t)0) ?	       \
		(off_t)1 : (off_t)0)))

#define	FILETYPES	(S_IFREG|S_IFLNK|S_IFDIR)

typedef struct name_s {
    struct name_s *next;
    char *str;
} Name;

Name *name_stack;

#define MAXDUMPS 10

struct {
    int max_inode;
    int total_dirs;
    int total_files;
    off_t total_size;
    off_t total_size_name;
} dumpstats[MAXDUMPS];

time_t dumpdate[MAXDUMPS];
int  dumplevel[MAXDUMPS];
int ndumps;

void (*add_file_name)(int, char *);
void (*add_file)(int, struct stat *);
off_t (*final_size)(int, char *);


int main(int, char **);
void traverse_dirs(char *, char *);


void add_file_name_dump(int, char *);
void add_file_dump(int, struct stat *);
off_t final_size_dump(int, char *);

void add_file_name_star(int, char *);
void add_file_star(int, struct stat *);
off_t final_size_star(int, char *);

void add_file_name_gnutar(int, char *);
void add_file_gnutar(int, struct stat *);
off_t final_size_gnutar(int, char *);

void add_file_name_unknown(int, char *);
void add_file_unknown(int, struct stat *);
off_t final_size_unknown(int, char *);

am_sl_t *calc_load_file(char *filename);
int calc_check_exclude(char *filename);

int use_star_excl = 0;
int use_gtar_excl = 0;
am_sl_t *include_sl=NULL, *exclude_sl=NULL;

int
main(
    int		argc,
    char **	argv)
{
#ifdef TEST
/* standalone test to ckeck wether the calculated file size is ok */
    struct stat finfo;
    int i;
    off_t dump_total = (off_t)0;
    off_t gtar_total = (off_t)0;
    char *d;
    int l, w;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("calcsize");

    dbopen(NULL);
    config_init(CONFIG_INIT_CLIENT, NULL);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2) {
	g_fprintf(stderr,_("Usage: %s file[s]\n"),argv[0]);
	return 1;
    }
    for(i=1; i<argc; i++) {
	if(lstat(argv[i], &finfo) == -1) {
	    g_fprintf(stderr, "%s: %s\n", argv[i], strerror(errno));
	    continue;
	}
	g_printf("%s: st_size=%lu", argv[i],(unsigned long)finfo.st_size);
	g_printf(": blocks=%llu\n", ST_BLOCKS(finfo));
	dump_total += (ST_BLOCKS(finfo) + (off_t)1) / (off_t)2 + (off_t)1;
	gtar_total += ROUND(4,(ST_BLOCKS(finfo) + (off_t)1));
    }
    g_printf("           gtar           dump\n");
    g_printf("total      %-9lu         %-9lu\n",gtar_total,dump_total);
    return 0;
#else
    int i;
    char *dirname=NULL;
    char *amname=NULL, *qamname=NULL;
    char *filename=NULL, *qfilename = NULL;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("calcsize-%s\n", VERSION);
	return (0);
    }

    safe_fd(-1, 0);
    safe_cd();

    set_pname("calcsize");

    dbopen(DBG_SUBDIR_CLIENT);
    config_init(CONFIG_INIT_CLIENT, NULL);
    dbprintf(_("version %s\n"), VERSION);

    /* drop root privileges; we'll regain them for the required operations */
#ifdef WANT_SETUID_CLIENT
    check_running_as(RUNNING_AS_CLIENT_LOGIN | RUNNING_AS_UID_ONLY);
    if (!set_root_privs(0)) {
	error(_("calcsize must be run setuid root"));
    }
#else
    check_running_as(RUNNING_AS_CLIENT_LOGIN);
#endif

    argc--, argv++;	/* skip program name */

    /* need at least program, amname, and directory name */

    if(argc < 4) {
	error(_("Usage: %s config [DUMP|STAR|GNUTAR] name dir [-X exclude-file] [-I include-file] [level date]*"),
	      get_pname());
        /*NOTREACHED*/
    }

    dbprintf(_("config: %s\n"), *argv);
    if (strcmp(*argv, "NOCONFIG") != 0) {
	dbrename(*argv, DBG_SUBDIR_CLIENT);
    }
    argc--;
    argv++;

    /* parse backup program name */

    if(strcmp(*argv, "DUMP") == 0) {
#if !defined(DUMP) && !defined(XFSDUMP)
	error("dump not available on this system");
	/*NOTREACHED*/
#else
	add_file_name = add_file_name_dump;
	add_file = add_file_dump;
	final_size = final_size_dump;
#endif
    }
    else if(strcmp(*argv, "GNUTAR") == 0) {
#ifndef GNUTAR
	error("gnutar not available on this system");
	/*NOTREACHED*/
#else
	add_file_name = add_file_name_gnutar;
	add_file = add_file_gnutar;
	final_size = final_size_gnutar;
	use_gtar_excl++;
#endif
    }
    else {
	add_file_name = add_file_name_unknown;
	add_file = add_file_unknown;
	final_size = final_size_unknown;
    }
    argc--, argv++;

    /* the amanda name can be different from the directory name */

    if (argc > 0) {
	amname = *argv;
	qamname = quote_string(amname);
	argc--, argv++;
    } else {
	error("missing <name>");
	/*NOTREACHED*/
    }

    /* the toplevel directory name to search from */
    if (argc > 0) {
	dirname = *argv;
	argc--, argv++;
    } else {
	error("missing <dir>");
	/*NOTREACHED*/
    }

    if ((argc > 1) && strcmp(*argv,"-X") == 0) {
	argv++;

	if (!(use_gtar_excl || use_star_excl)) {
	  error("exclusion specification not supported");
	  /*NOTREACHED*/
	}
	
	filename = stralloc(*argv);
	qfilename = quote_string(filename);
	if (access(filename, R_OK) != 0) {
	    g_fprintf(stderr,"Cannot open exclude file %s\n", qfilename);
	    use_gtar_excl = use_star_excl = 0;
	} else {
	    exclude_sl = calc_load_file(filename);
	    if (!exclude_sl) {
		g_fprintf(stderr,"Cannot open exclude file %s: %s\n", qfilename,
			strerror(errno));
		use_gtar_excl = use_star_excl = 0;
	    }
	}
	amfree(qfilename);
	amfree(filename);
	argc -= 2;
	argv++;
    } else {
	use_gtar_excl = use_star_excl = 0;
    }

    if ((argc > 1) && strcmp(*argv,"-I") == 0) {
	argv++;
	
	filename = stralloc(*argv);
	qfilename = quote_string(filename);
	if (access(filename, R_OK) != 0) {
	    g_fprintf(stderr,"Cannot open include file %s\n", qfilename);
	    use_gtar_excl = use_star_excl = 0;
	} else {
	    include_sl = calc_load_file(filename);
	    if (!include_sl) {
		g_fprintf(stderr,"Cannot open include file %s: %s\n", qfilename,
			strerror(errno));
		use_gtar_excl = use_star_excl = 0;
	    }
	}
	amfree(qfilename);
	amfree(filename);
	argc -= 2;
	argv++;
    }

    /* the dump levels to calculate sizes for */

    ndumps = 0;
    while(argc >= 2) {
	if(ndumps < MAXDUMPS) {
	    dumplevel[ndumps] = atoi(argv[0]);
	    dumpdate [ndumps] = (time_t) atol(argv[1]);
	    ndumps++;
	    argc -= 2, argv += 2;
	}
    }

    if(argc) {
	error("leftover arg \"%s\", expected <level> and <date>", *argv);
	/*NOTREACHED*/
    }

    if(is_empty_sl(include_sl)) {
	traverse_dirs(dirname,".");
    }
    else {
	sle_t *an_include = include_sl->first;
	while(an_include != NULL) {
/*
	    char *adirname = stralloc2(dirname, an_include->name+1);
	    traverse_dirs(adirname);
	    amfree(adirname);
*/
	    traverse_dirs(dirname, an_include->name);
	    an_include = an_include->next;
	}
    }
    for(i = 0; i < ndumps; i++) {

	amflock(1, "size");

	dbprintf("calcsize: %s %d SIZE %lld\n",
	       qamname, dumplevel[i],
	       (long long)final_size(i, dirname));
	g_fprintf(stderr, "%s %d SIZE %lld\n",
	       qamname, dumplevel[i],
	       (long long)final_size(i, dirname));
	fflush(stderr);

	amfunlock(1, "size");
    }
    amfree(qamname);

    return 0;
#endif
}

/*
 * =========================================================================
 */

#if !defined(HAVE_BASENAME) && defined(BUILTIN_EXCLUDE_SUPPORT)

static char *
basename(
    char *	file)
{
    char *cp;

    if ( (cp = strrchr(file,'/')) )
	return cp+1;
    return file;
}
#endif

void push_name(char *str);
char *pop_name(void);

void
traverse_dirs(
    char *	parent_dir,
    char *	include)
{
    DIR *d;
    struct dirent *f;
    struct stat finfo;
    char *dirname, *newname = NULL;
    char *newbase = NULL;
    dev_t parent_dev = (dev_t)0;
    int i;
    size_t l;
    size_t parent_len;
    int has_exclude;
    char *aparent;

    if(parent_dir == NULL || include == NULL)
	return;

    has_exclude = !is_empty_sl(exclude_sl) && (use_gtar_excl || use_star_excl);
    aparent = vstralloc(parent_dir, "/", include, NULL);

    /* We (may) need root privs for the *stat() calls here. */
    set_root_privs(1);
    if(stat(parent_dir, &finfo) != -1)
	parent_dev = finfo.st_dev;

    parent_len = strlen(parent_dir);

    push_name(aparent);

    for(; (dirname = pop_name()) != NULL; free(dirname)) {
	if(has_exclude && calc_check_exclude(dirname+parent_len+1)) {
	    continue;
	}
	if((d = opendir(dirname)) == NULL) {
	    perror(dirname);
	    continue;
	}

	l = strlen(dirname);
	if(l > 0 && dirname[l - 1] != '/') {
	    newbase = newstralloc2(newbase, dirname, "/");
	} else {
	    newbase = newstralloc(newbase, dirname);
	}

	while((f = readdir(d)) != NULL) {
	    int is_symlink = 0;
	    int is_dir;
	    int is_file;
	    if(is_dot_or_dotdot(f->d_name)) {
		continue;
	    }

	    newname = newstralloc2(newname, newbase, f->d_name);
	    if(lstat(newname, &finfo) == -1) {
		g_fprintf(stderr, "%s/%s: %s\n",
			dirname, f->d_name, strerror(errno));
		continue;
	    }

	    if(finfo.st_dev != parent_dev)
		continue;

#ifdef S_IFLNK
	    is_symlink = ((finfo.st_mode & S_IFMT) == S_IFLNK);
#endif
	    is_dir = ((finfo.st_mode & S_IFMT) == S_IFDIR);
	    is_file = ((finfo.st_mode & S_IFMT) == S_IFREG);

	    if (!(is_file || is_dir || is_symlink)) {
		continue;
	    }

	    {
		int is_excluded = -1;
		for(i = 0; i < ndumps; i++) {
		    add_file_name(i, newname);
		    if(is_file && (time_t)finfo.st_ctime >= dumpdate[i]) {

			if(has_exclude) {
			    if(is_excluded == -1)
				is_excluded =
				       calc_check_exclude(newname+parent_len+1);
			    if(is_excluded == 1) {
				i = ndumps;
				continue;
			    }
			}
			add_file(i, &finfo);
		    }
		}
		if(is_dir) {
		    if(has_exclude && calc_check_exclude(newname+parent_len+1))
			continue;
		    push_name(newname);
		}
	    }
	}

#ifdef CLOSEDIR_VOID
	closedir(d);
#else
	if(closedir(d) == -1)
	    perror(dirname);
#endif
    }

    /* drop root privs -- we're done with the permission-sensitive calls */
    set_root_privs(0);

    amfree(newbase);
    amfree(newname);
    amfree(aparent);
}

void
push_name(
    char *	str)
{
    Name *newp;

    newp = alloc(SIZEOF(*newp));
    newp->str = stralloc(str);

    newp->next = name_stack;
    name_stack = newp;
}

char *
pop_name(void)
{
    Name *newp = name_stack;
    char *str;

    if(!newp) return NULL;

    name_stack = newp->next;
    str = newp->str;
    amfree(newp);
    return str;
}


/*
 * =========================================================================
 * Backup size calculations for DUMP program
 *
 * Given the system-dependent nature of dump, it's impossible to pin this
 * down accurately.  Luckily, that's not necessary.
 *
 * Dump rounds each file up to TP_BSIZE bytes, which is 1k in the BSD dump,
 * others are unknown.  In addition, dump stores three bitmaps at the
 * beginning of the dump: a used inode map, a dumped dir map, and a dumped
 * inode map.  These are sized by the number of inodes in the filesystem.
 *
 * We don't take into account the complexities of BSD dump's indirect block
 * requirements for files with holes, nor the dumping of directories that
 * are not themselves modified.
 */
void
add_file_name_dump(
    int		level,
    char *	name)
{
    (void)level;	/* Quiet unused parameter warning */
    (void)name;		/* Quiet unused parameter warning */

    return;
}

void
add_file_dump(
    int			level,
    struct stat *	sp)
{
    /* keep the size in kbytes, rounded up, plus a 1k header block */
    if((sp->st_mode & S_IFMT) == S_IFREG || (sp->st_mode & S_IFMT) == S_IFDIR)
    	dumpstats[level].total_size +=
			(ST_BLOCKS(*sp) + (off_t)1) / (off_t)2 + (off_t)1;
}

off_t
final_size_dump(
    int		level,
    char *	topdir)
{
    struct fs_usage fsusage;
    off_t mapsize;
    char *s;

    /* calculate the map sizes */

    s = stralloc2(topdir, "/.");
    if(get_fs_usage(s, NULL, &fsusage) == -1) {
	error("statfs %s: %s", s, strerror(errno));
	/*NOTREACHED*/
    }
    amfree(s);

    mapsize = (fsusage.fsu_files + (off_t)7) / (off_t)8;    /* in bytes */
    mapsize = (mapsize + (off_t)1023) / (off_t)1024;  /* in kbytes */

    /* the dump contains three maps plus the files */

    return (mapsize * (off_t)3) + dumpstats[level].total_size;
}

/*
 * =========================================================================
 * Backup size calculations for GNUTAR program
 *
 * Gnutar's basic blocksize is 512 bytes.  Each file is rounded up to that
 * size, plus one header block.  Gnutar stores directories' file lists in
 * incremental dumps - we'll pick up size of the modified dirs here.  These
 * will be larger than a simple filelist of their contents, but that's ok.
 *
 * As with DUMP, we only need a reasonable estimate, not an exact figure.
 */
void
add_file_name_gnutar(
    int		level,
    char *	name)
{
    (void)name;	/* Quiet unused parameter warning */

/*  dumpstats[level].total_size_name += strlen(name) + 64;*/
    dumpstats[level].total_size += (off_t)1;
}

void
add_file_gnutar(
    int			level,
    struct stat *	sp)
{
    /* the header takes one additional block */
    dumpstats[level].total_size += ST_BLOCKS(*sp);
}

off_t
final_size_gnutar(
    int		level,
    char *	topdir)
{
    (void)topdir;	/* Quiet unused parameter warning */

    /* divide by two to get kbytes, rounded up */
    /* + 4 blocks for security */
    return (dumpstats[level].total_size + (off_t)5 +
		(dumpstats[level].total_size_name/(off_t)512)) / (off_t)2;
}

/*
 * =========================================================================
 * Backup size calculations for unknown backup programs.
 *
 * Here we'll just add up the file sizes and output that.
 */

void
add_file_name_unknown(
    int		level,
    char *	name)
{
    (void)level;	/* Quiet unused parameter warning */
    (void)name;		/* Quiet unused parameter warning */

    return;
}

void
add_file_unknown(
    int			level,
    struct stat *	sp)
{
    /* just add up the block counts */
    if((sp->st_mode & S_IFMT) == S_IFREG || (sp->st_mode & S_IFMT) == S_IFDIR)
    	dumpstats[level].total_size += ST_BLOCKS(*sp);
}

off_t
final_size_unknown(
    int		level,
    char *	topdir)
{
    (void)topdir;	/* Quiet unused parameter warning */

    /* divide by two to get kbytes, rounded up */
    return (dumpstats[level].total_size + (off_t)1) / (off_t)2;
}

/*
 * =========================================================================
 */
am_sl_t *
calc_load_file(
    char *	filename)
{
    char pattern[1025];

    am_sl_t *sl_list;

    FILE *file = fopen(filename, "r");

    if (!file) {
	return NULL;
    }

    sl_list = new_sl();

    while(fgets(pattern, 1025, file)) {
	if(strlen(pattern)>0 && pattern[strlen(pattern)-1] == '\n')
	    pattern[strlen(pattern)-1] = '\0';
	sl_list = append_sl(sl_list, pattern);
    }  
    fclose(file);

    return sl_list;
}

int
calc_check_exclude(
    char *	filename)
{
    sle_t *an_exclude;
    if(is_empty_sl(exclude_sl)) return 0;

    an_exclude=exclude_sl->first;
    while(an_exclude != NULL) {
	if(match_tar(an_exclude->name, filename)) {
	    return 1;
	}
	an_exclude=an_exclude->next;
    }
    return 0;
}
