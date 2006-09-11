/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
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
 * $Id: util.h,v 1.17 2006/07/26 15:17:36 martinea Exp $
 */
#ifndef UTIL_H
#define	UTIL_H

#include "amanda.h"
#include "sl.h"

/* */
typedef enum {
    CONFTYPE_INT,
    CONFTYPE_LONG,
    CONFTYPE_AM64,
    CONFTYPE_REAL,
    CONFTYPE_STRING,
    CONFTYPE_IDENT,
    CONFTYPE_TIME,
    CONFTYPE_SIZE,
    CONFTYPE_SL,
    CONFTYPE_BOOL,
    CONFTYPE_COMPRESS,
    CONFTYPE_ENCRYPT,
    CONFTYPE_HOLDING,
    CONFTYPE_ESTIMATE,
    CONFTYPE_STRATEGY,
    CONFTYPE_TAPERALGO,
    CONFTYPE_PRIORITY,
    CONFTYPE_RATE,
    CONFTYPE_EXINCLUDE,
} conftype_t;

/* Compression types */
typedef enum {
    COMP_NONE,          /* No compression */
    COMP_FAST,          /* Fast compression on client */
    COMP_BEST,          /* Best compression on client */
    COMP_CUST,          /* Custom compression on client */
    COMP_SERV_FAST,     /* Fast compression on server */
    COMP_SERV_BEST,     /* Best compression on server */
    COMP_SERV_CUST      /* Custom compression on server */
} comp_t;

/* Encryption types */
typedef enum {
    ENCRYPT_NONE,               /* No encryption */
    ENCRYPT_CUST,               /* Custom encryption on client */
    ENCRYPT_SERV_CUST,          /* Custom encryption on server */
} encrypt_t;

/* holdingdisk types */
typedef enum {
    HOLD_NEVER,			/* Always direct to tape  */
    HOLD_AUTO,			/* If possible            */
    HOLD_REQUIRED		/* Always to holding disk */
} dump_holdingdisk_t;

/* Dump strategies */
#define DS_SKIP		0	/* Don't do any dumps at all */
#define DS_STANDARD	1	/* Standard (0 1 1 1 1 2 2 2 ...) */
#define DS_NOFULL	2	/* No full's (1 1 1 ...) */
#define DS_NOINC	3	/* No inc's (0 0 0 ...) */
#define DS_4		4	/* ? (0 1 2 3 4 5 6 7 8 9 10 11 ...) */
#define DS_5		5	/* ? (0 1 1 1 1 1 1 1 1 1 1 1 ...) */
#define DS_HANOI	6	/* Tower of Hanoi (? ? ? ? ? ...) */
#define DS_INCRONLY	7	/* Forced fulls (0 1 1 2 2 FORCE0 1 1 ...) */

/* Estimate strategies */
#define ES_CLIENT	0	/* client estimate */
#define ES_SERVER	1	/* server estimate */
#define ES_CALCSIZE	2	/* calcsize estimate */

#define ALGO_FIRST	0
#define ALGO_FIRSTFIT	1
#define ALGO_LARGEST	2
#define ALGO_LARGESTFIT	3
#define ALGO_SMALLEST	4
#define ALGO_LAST	5

#define BSTRNCMP(a,b)  strncmp(a, b, strlen(b)) 
						  
typedef enum {
    CONF_UNKNOWN,		CONF_ANY,		CONF_COMMA,
    CONF_LBRACE,		CONF_RBRACE,		CONF_NL,
    CONF_END,			CONF_IDENT,		CONF_INT,
    CONF_LONG,			CONF_AM64,		CONF_BOOL,
    CONF_REAL,			CONF_STRING,		CONF_TIME,
    CONF_SIZE,

    /* config parameters */
    CONF_INCLUDEFILE,		CONF_ORG,		CONF_MAILTO,
    CONF_DUMPUSER,		CONF_TAPECYCLE,		CONF_TAPEDEV,
    CONF_CHNGRDEV,		CONF_CHNGRFILE,		CONF_LABELSTR,
    CONF_BUMPPERCENT,		CONF_BUMPSIZE,		CONF_BUMPDAYS,
    CONF_BUMPMULT,		CONF_ETIMEOUT,		CONF_DTIMEOUT,
    CONF_CTIMEOUT,		CONF_TAPEBUFS,		CONF_TAPELIST,
    CONF_DISKFILE,		CONF_INFOFILE,		CONF_LOGDIR,
    CONF_LOGFILE,		CONF_DISKDIR,		CONF_DISKSIZE,
    CONF_INDEXDIR,		CONF_NETUSAGE,		CONF_INPARALLEL,
    CONF_DUMPORDER,		CONF_TIMEOUT,		CONF_TPCHANGER,
    CONF_RUNTAPES,		CONF_DEFINE,		CONF_DUMPTYPE,
    CONF_TAPETYPE,		CONF_INTERFACE,		CONF_PRINTER,
    CONF_AUTOFLUSH,		CONF_RESERVE,		CONF_MAXDUMPSIZE,
    CONF_COLUMNSPEC,		CONF_AMRECOVER_DO_FSF,	CONF_AMRECOVER_CHECK_LABEL,
    CONF_AMRECOVER_CHANGER,	CONF_LABEL_NEW_TAPES,	CONF_USETIMESTAMPS,

    CONF_TAPERALGO,		CONF_FIRST,		CONF_FIRSTFIT,
    CONF_LARGEST,		CONF_LARGESTFIT,	CONF_SMALLEST,
    CONF_LAST,			CONF_DISPLAYUNIT,

    /* kerberos 5 */
    CONF_KRB5KEYTAB,		CONF_KRB5PRINCIPAL,

    /* holding disk */
    CONF_COMMENT,		CONF_DIRECTORY,		CONF_USE,
    CONF_CHUNKSIZE,

    /* dump type */
    /*COMMENT,*/		CONF_PROGRAM,		CONF_DUMPCYCLE,
    CONF_RUNSPERCYCLE,		CONF_MAXCYCLE,		CONF_MAXDUMPS,
    CONF_OPTIONS,		CONF_PRIORITY,		CONF_FREQUENCY,
    CONF_INDEX,			CONF_MAXPROMOTEDAY,	CONF_STARTTIME,
    CONF_COMPRESS,		CONF_ENCRYPT,		CONF_AUTH,
    CONF_STRATEGY,		CONF_ESTIMATE,		CONF_SKIP_INCR,
    CONF_SKIP_FULL,		CONF_RECORD,		CONF_HOLDING,
    CONF_EXCLUDE,		CONF_INCLUDE,		CONF_KENCRYPT,
    CONF_IGNORE,		CONF_COMPRATE,		CONF_TAPE_SPLITSIZE,
    CONF_SPLIT_DISKBUFFER,	CONF_FALLBACK_SPLITSIZE,CONF_SRVCOMPPROG,
    CONF_CLNTCOMPPROG,		CONF_SRV_ENCRYPT,	CONF_CLNT_ENCRYPT,
    CONF_SRV_DECRYPT_OPT,	CONF_CLNT_DECRYPT_OPT,	CONF_AMANDAD_PATH,
    CONF_CLIENT_USERNAME,

    /* tape type */
    /*COMMENT,*/		CONF_BLOCKSIZE,		CONF_FILE_PAD,
    CONF_LBL_TEMPL,		CONF_FILEMARK,		CONF_LENGTH,
    CONF_SPEED,

    /* client conf */
    CONF_CONF,			CONF_INDEX_SERVER,	CONF_TAPE_SERVER,
    CONF_SSH_KEYS,		CONF_GNUTAR_LIST_DIR,	CONF_AMANDATES,

    /* network interface */
    /* COMMENT, */		/* USE, */

    /* dump options (obsolete) */
    CONF_EXCLUDE_FILE,		CONF_EXCLUDE_LIST,

    /* compress, estimate, encryption */
    CONF_NONE,			CONF_FAST,		CONF_BEST,
    CONF_SERVER,		CONF_CLIENT,		CONF_CALCSIZE,
    CONF_CUSTOM,

    /* holdingdisk */
    CONF_NEVER,			CONF_AUTO,		CONF_REQUIRED,

    /* priority */
    CONF_LOW,			CONF_MEDIUM,		CONF_HIGH,

    /* dump strategy */
    CONF_SKIP,			CONF_STANDARD,		CONF_NOFULL,
    CONF_NOINC,			CONF_HANOI,		CONF_INCRONLY,

    /* exclude list */
    CONF_LIST,			CONF_EFILE,		CONF_APPEND,
    CONF_OPTIONAL,

    /* numbers */
    CONF_AMINFINITY,		CONF_MULT1,		CONF_MULT7,
    CONF_MULT1K,		CONF_MULT1M,		CONF_MULT1G,

    /* boolean */
    CONF_ATRUE,			CONF_AFALSE,

    CONF_RAWTAPEDEV
} tok_t;

#define BIGINT  INT_MAX

/* internal types and variables */

typedef struct {        /* token table entry */
    char *keyword;
    tok_t token;
} keytab_t;

keytab_t *keytable;

typedef struct {
    char *name;
    char *value;
    int   used;
} command_option_t;

typedef struct exinclude_s {
    int  type;  /* 0=list   1=file */
    sl_t *sl;
    int  optional;
} exinclude_t;

typedef struct val_s {
    union {
       int		i;
       long		l;
       off_t		am64;
       double		r;
       char		*s;
       sl_t		*sl;
       ssize_t		size;
       time_t		t;
       float		rate[2];
       exinclude_t	exinclude;
    } v;
    int seen;
    conftype_t type;
} val_t;

typedef struct s_conf_var {
    tok_t	token;
    conftype_t	type;
    void	(*read_function) (struct s_conf_var *, val_t*);
    int		parm;
    void	(*validate) (struct s_conf_var *, val_t *);
} t_conf_var;

extern int	allow_overwrites;
extern int	token_pushed;

extern tok_t	tok, pushed_tok;
extern val_t	tokenval;

extern int	conf_line_num, got_parserror;
extern FILE	*conf_conf;
extern char	*conf_confname;
extern char	*conf_line;
extern char	*conf_char;

/* predeclare local functions */

t_conf_var  *get_np(t_conf_var *get_var, int parm);
void	get_simple(val_t *var, tok_t type);
int	get_int(void);
long	get_long(void);
time_t	get_time(void);
ssize_t	get_size(void);
off_t 	get_am64_t(void);
int	get_bool(void);
void	ckseen(int *seen);
void	conf_parserror(const char *format, ...)
		__attribute__ ((format (printf, 1, 2)));
tok_t	lookup_keyword(char *str);
void	unget_conftoken(void);
void	get_conftoken(tok_t exp);

void read_string(t_conf_var *, val_t *);
void read_ident(t_conf_var *, val_t *);
void read_int(t_conf_var *, val_t *);
void read_long(t_conf_var *, val_t *);
void read_size(t_conf_var *, val_t *);
void read_am64(t_conf_var *, val_t *);
void read_bool(t_conf_var *, val_t *);
void read_real(t_conf_var *, val_t *);
void read_time(t_conf_var *, val_t *);
void copy_val_t(val_t *, val_t *);
void free_val_t(val_t *);
char *conf_print(val_t *);
void conf_init_string(val_t *, char *);
void conf_init_ident(val_t *, char *);
void conf_init_int(val_t *, int);
void conf_init_bool(val_t *, int);
void conf_init_strategy(val_t *, int);
void conf_init_estimate(val_t *, int);
void conf_init_taperalgo(val_t *, int);
void conf_init_priority(val_t *, int);
void conf_init_strategy(val_t *, int);
void conf_init_compress(val_t *, comp_t);
void conf_init_encrypt(val_t *, encrypt_t);
void conf_init_holding(val_t *, dump_holdingdisk_t);
void conf_init_long(val_t *, long);
void conf_init_size(val_t *, ssize_t);
void conf_init_am64(val_t *, off_t);
void conf_init_real(val_t *, double);
void conf_init_rate(val_t *, double, double);
void conf_init_time(val_t *, time_t);
void conf_init_sl(val_t *, sl_t *);
void conf_init_exinclude(val_t *);
void conf_set_string(val_t *, char *);
void conf_set_int(val_t *, int);
void conf_set_bool(val_t *, int);
void conf_set_compress(val_t *, comp_t);
void conf_set_encrypt(val_t *, encrypt_t);
void conf_set_holding(val_t *, dump_holdingdisk_t);
void conf_set_strategy(val_t *, int);
int          get_conftype_int      (val_t *);
long         get_conftype_long     (val_t *);
off_t        get_conftype_am64     (val_t *);
double       get_conftype_real     (val_t *);
char        *get_conftype_string   (val_t *);
char        *get_conftype_ident    (val_t *);
time_t       get_conftype_time     (val_t *);
ssize_t      get_conftype_size     (val_t *);
sl_t        *get_conftype_sl       (val_t *);
int          get_conftype_bool     (val_t *);
int          get_conftype_hold     (val_t *);
int          get_conftype_compress (val_t *);
int          get_conftype_encrypt  (val_t *);
int          get_conftype_estimate (val_t *);
int          get_conftype_strategy (val_t *);
int          get_conftype_taperalgo(val_t *);
int          get_conftype_priority (val_t *);
float       *get_conftype_rate     (val_t *);
exinclude_t  get_conftype_exinclude(val_t *);

void read_block(command_option_t *command_options, t_conf_var *read_var,
		keytab_t *keytab, val_t *valarray, char *prefix, char *errormsg,
		int read_brace, void (*copy_function)(void));
void command_overwrite(command_option_t *command_options, t_conf_var *overwrite_var,
		       keytab_t *keytab, val_t *valarray, char *prefix);



ssize_t	fullread(int, void *, size_t);
ssize_t	fullwrite(int, const void *, size_t);

int	connect_portrange(struct sockaddr_in *, in_port_t, in_port_t, char *,
			  struct sockaddr_in *, int);
int	bind_portrange(int, struct sockaddr_in *, in_port_t, in_port_t,
		       char *);

char *	construct_datestamp(time_t *t);
char *	construct_timestamp(time_t *t);

/*@only@*//*@null@*/char *quote_string(const char *str);
/*@only@*//*@null@*/char *unquote_string(const char *str);
int	needs_quotes(const char * str);

char *	sanitize_string(const char *str);
char *	strquotedstr(void);
ssize_t	hexdump(const char *buffer, size_t bytes);
void	dump_sockaddr(struct sockaddr_in *	sa);

/*
 *   validate_email return 0 if the following characters are present
 *   * ( ) < > [ ] , ; : ! $ \ / "
 *   else returns 1
 */
int validate_mailto(const char *mailto);

char *taperalgo2str(int taperalgo);

void free_new_argv(int new_argc, char **new_argv);
#endif	/* UTIL_H */
