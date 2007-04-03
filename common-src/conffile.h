/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * $Id: conffile.h,v 1.72 2006/07/26 15:17:37 martinea Exp $
 *
 * interface for config file reading code
 */
#ifndef CONFFILE_H
#define CONFFILE_H

#include "amanda.h"
#include "conffile.h"
#include "util.h"

#define CONFFILE_NAME "amanda.conf"

typedef enum {
    CNF_ORG,
    CNF_CONF,
    CNF_INDEX_SERVER,
    CNF_TAPE_SERVER,
    CNF_AUTH,
    CNF_SSH_KEYS,
    CNF_AMANDAD_PATH,
    CNF_CLIENT_USERNAME,
    CNF_GNUTAR_LIST_DIR,
    CNF_AMANDATES,
    CNF_MAILTO,
    CNF_DUMPUSER,
    CNF_TAPEDEV,
    CNF_CHNGRDEV,
    CNF_CHNGRFILE,
    CNF_LABELSTR,
    CNF_TAPELIST,
    CNF_DISKFILE,
    CNF_INFOFILE,
    CNF_LOGDIR,
    CNF_INDEXDIR,
    CNF_TAPETYPE,
    CNF_DUMPCYCLE,
    CNF_RUNSPERCYCLE,
    CNF_TAPECYCLE,
    CNF_NETUSAGE,
    CNF_INPARALLEL,
    CNF_DUMPORDER,
    CNF_BUMPPERCENT,
    CNF_BUMPSIZE,
    CNF_BUMPMULT,
    CNF_BUMPDAYS,
    CNF_TPCHANGER,
    CNF_RUNTAPES,
    CNF_MAXDUMPS,
    CNF_ETIMEOUT,
    CNF_DTIMEOUT,
    CNF_CTIMEOUT,
    CNF_TAPEBUFS,
    CNF_RAWTAPEDEV,
    CNF_PRINTER,
    CNF_AUTOFLUSH,
    CNF_RESERVE,
    CNF_MAXDUMPSIZE,
    CNF_COLUMNSPEC,
    CNF_AMRECOVER_DO_FSF,
    CNF_AMRECOVER_CHECK_LABEL,
    CNF_AMRECOVER_CHANGER,
    CNF_TAPERALGO,
    CNF_DISPLAYUNIT,
    CNF_KRB5KEYTAB,
    CNF_KRB5PRINCIPAL,
    CNF_LABEL_NEW_TAPES,
    CNF_USETIMESTAMPS,
    CNF_REP_TRIES,
    CNF_CONNECT_TRIES,
    CNF_REQ_TRIES,
    CNF_DEBUG_AMANDAD,
    CNF_DEBUG_AMIDXTAPED,
    CNF_DEBUG_AMINDEXD,
    CNF_DEBUG_AMRECOVER,
    CNF_DEBUG_AUTH,
    CNF_DEBUG_EVENT,
    CNF_DEBUG_HOLDING,
    CNF_DEBUG_PROTOCOL,
    CNF_DEBUG_PLANNER,
    CNF_DEBUG_DRIVER,
    CNF_DEBUG_DUMPER,
    CNF_DEBUG_CHUNKER,
    CNF_DEBUG_TAPER,
    CNF_DEBUG_SELFCHECK,
    CNF_DEBUG_SENDSIZE,
    CNF_DEBUG_SENDBACKUP,
    CNF_RESERVED_UDP_PORT,
    CNF_RESERVED_TCP_PORT,
    CNF_UNRESERVED_TCP_PORT,
    CNF_CNF
} confparm_t;

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
    CONF_LAST,			CONF_DISPLAYUNIT,	CONF_RESERVED_UDP_PORT,
    CONF_RESERVED_TCP_PORT,	CONF_UNRESERVED_TCP_PORT,

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
    CONF_SPEED,			CONF_READBLOCKSIZE,

    /* client conf */
    CONF_CONF,			CONF_INDEX_SERVER,	CONF_TAPE_SERVER,
    CONF_SSH_KEYS,		CONF_GNUTAR_LIST_DIR,	CONF_AMANDATES,

    /* protocol config */
    CONF_REP_TRIES,		CONF_CONNECT_TRIES,	CONF_REQ_TRIES,

    /* debug config */
    CONF_DEBUG_AMANDAD,		CONF_DEBUG_AMIDXTAPED,	CONF_DEBUG_AMINDEXD,
    CONF_DEBUG_AMRECOVER,	CONF_DEBUG_AUTH,	CONF_DEBUG_EVENT,
    CONF_DEBUG_HOLDING,		CONF_DEBUG_PROTOCOL,	CONF_DEBUG_PLANNER,
    CONF_DEBUG_DRIVER,		CONF_DEBUG_DUMPER,	CONF_DEBUG_CHUNKER,
    CONF_DEBUG_TAPER,		CONF_DEBUG_SELFCHECK,	CONF_DEBUG_SENDSIZE,
    CONF_DEBUG_SENDBACKUP,

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

/* internal types and variables */

/* */
typedef enum {
    CONFTYPE_INT,
    CONFTYPE_LONG,
    CONFTYPE_AM64,
    CONFTYPE_REAL,
    CONFTYPE_STRING,
    CONFTYPE_IDENT,
    CONFTYPE_TIME,		/* hhmm */
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
    CONFTYPE_INTRANGE,
    CONFTYPE_EXINCLUDE
} conftype_t;

/* Compression types */
typedef enum {
    COMP_NONE,          /* No compression */
    COMP_FAST,          /* Fast compression on client */
    COMP_BEST,          /* Best compression on client */
    COMP_CUST,          /* Custom compression on client */
    COMP_SERVER_FAST,   /* Fast compression on server */
    COMP_SERVER_BEST,   /* Best compression on server */
    COMP_SERVER_CUST    /* Custom compression on server */
} comp_t;

/* Encryption types */
typedef enum {
    ENCRYPT_NONE,               /* No encryption */
    ENCRYPT_CUST,               /* Custom encryption on client */
    ENCRYPT_SERV_CUST           /* Custom encryption on server */
} encrypt_t;

/* holdingdisk types */
typedef enum {
    HOLD_NEVER,			/* Always direct to tape  */
    HOLD_AUTO,			/* If possible            */
    HOLD_REQUIRED		/* Always to holding disk */
} dump_holdingdisk_t;

typedef struct {        /* token table entry */
    char *keyword;
    tok_t token;
} keytab_t;

typedef struct {
    char *name;
    char *value;
    int   used;
} command_option_t;

typedef struct exinclude_s {
    sl_t *sl_list;
    sl_t *sl_file;
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
       int		intrange[2];
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

typedef enum tapetype_e  {
    TAPETYPE_COMMENT,
    TAPETYPE_LBL_TEMPL,
    TAPETYPE_BLOCKSIZE,
    TAPETYPE_READBLOCKSIZE,
    TAPETYPE_LENGTH,
    TAPETYPE_FILEMARK,
    TAPETYPE_SPEED,
    TAPETYPE_FILE_PAD,
    TAPETYPE_TAPETYPE
} tapetype_ee;

typedef struct tapetype_s {
    struct tapetype_s *next;
    int seen;
    char *name;

    val_t value[TAPETYPE_TAPETYPE];
} tapetype_t;

#define tapetype_get(tapetype, field) (tapetype->field)
#define tapetype_get_name(tapetype) tapetype->name
#define tapetype_get_seen(tapetype) tapetype->seen
#define tapetype_get_comment(tapetype)       get_conftype_string(&tapetype->value[TAPETYPE_COMMENT])
#define tapetype_get_lbl_templ(tapetype)     get_conftype_string(&tapetype->value[TAPETYPE_LBL_TEMPL])
#define tapetype_get_blocksize(tapetype)     get_conftype_size  (&tapetype->value[TAPETYPE_BLOCKSIZE])
#define tapetype_get_readblocksize(tapetype) get_conftype_size  (&tapetype->value[TAPETYPE_READBLOCKSIZE])
#define tapetype_get_length(tapetype)        get_conftype_am64  (&tapetype->value[TAPETYPE_LENGTH])
#define tapetype_get_filemark(tapetype)      get_conftype_am64  (&tapetype->value[TAPETYPE_FILEMARK])
#define tapetype_get_speed(tapetype)         get_conftype_int   (&tapetype->value[TAPETYPE_SPEED])
#define tapetype_get_file_pad(tapetype)      get_conftype_bool  (&tapetype->value[TAPETYPE_FILE_PAD])

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

typedef enum dumptype_e  {
    DUMPTYPE_COMMENT,
    DUMPTYPE_PROGRAM,
    DUMPTYPE_SRVCOMPPROG,
    DUMPTYPE_CLNTCOMPPROG,
    DUMPTYPE_SRV_ENCRYPT,
    DUMPTYPE_CLNT_ENCRYPT,
    DUMPTYPE_AMANDAD_PATH,
    DUMPTYPE_CLIENT_USERNAME,
    DUMPTYPE_SSH_KEYS,
    DUMPTYPE_SECURITY_DRIVER,
    DUMPTYPE_EXCLUDE,
    DUMPTYPE_INCLUDE,
    DUMPTYPE_PRIORITY,
    DUMPTYPE_DUMPCYCLE,
    DUMPTYPE_MAXDUMPS,
    DUMPTYPE_MAXPROMOTEDAY,
    DUMPTYPE_BUMPPERCENT,
    DUMPTYPE_BUMPSIZE,
    DUMPTYPE_BUMPDAYS,
    DUMPTYPE_BUMPMULT,
    DUMPTYPE_STARTTIME,
    DUMPTYPE_STRATEGY,
    DUMPTYPE_ESTIMATE,
    DUMPTYPE_COMPRESS,
    DUMPTYPE_ENCRYPT,
    DUMPTYPE_SRV_DECRYPT_OPT,
    DUMPTYPE_CLNT_DECRYPT_OPT,
    DUMPTYPE_COMPRATE,
    DUMPTYPE_TAPE_SPLITSIZE,
    DUMPTYPE_FALLBACK_SPLITSIZE,
    DUMPTYPE_SPLIT_DISKBUFFER,
    DUMPTYPE_RECORD,
    DUMPTYPE_SKIP_INCR,
    DUMPTYPE_SKIP_FULL,
    DUMPTYPE_HOLDINGDISK,
    DUMPTYPE_KENCRYPT,
    DUMPTYPE_IGNORE,
    DUMPTYPE_INDEX,
    DUMPTYPE_DUMPTYPE
} dumptype_ee;

typedef struct dumptype_s {
    struct dumptype_s *next;
    int seen;
    char *name;

    val_t value[DUMPTYPE_DUMPTYPE];
} dumptype_t;

#define dumptype_get_name(dumptype) dumptype->name
#define dumptype_get_seen(dumptype) dumptype->seen
#define dumptype_get_comment(dumptype)            get_conftype_string   (&dumptype->value[DUMPTYPE_COMMENT])
#define dumptype_get_program(dumptype)            get_conftype_string   (&dumptype->value[DUMPTYPE_PROGRAM])
#define dumptype_get_srvcompprog(dumptype)        get_conftype_string   (&dumptype->value[DUMPTYPE_SRVCOMPPROG])
#define dumptype_get_clntcompprog(dumptype)       get_conftype_string   (&dumptype->value[DUMPTYPE_CLNTCOMPPROG])
#define dumptype_get_srv_encrypt(dumptype)        get_conftype_string   (&dumptype->value[DUMPTYPE_SRV_ENCRYPT])
#define dumptype_get_clnt_encrypt(dumptype)       get_conftype_string   (&dumptype->value[DUMPTYPE_CLNT_ENCRYPT])
#define dumptype_get_amandad_path(dumptype)       get_conftype_string   (&dumptype->value[DUMPTYPE_AMANDAD_PATH])
#define dumptype_get_client_username(dumptype)    get_conftype_string   (&dumptype->value[DUMPTYPE_CLIENT_USERNAME])
#define dumptype_get_ssh_keys(dumptype)           get_conftype_string   (&dumptype->value[DUMPTYPE_SSH_KEYS])
#define dumptype_get_security_driver(dumptype)    get_conftype_string   (&dumptype->value[DUMPTYPE_SECURITY_DRIVER])
#define dumptype_get_exclude(dumptype)            get_conftype_exinclude(&dumptype->value[DUMPTYPE_EXCLUDE])
#define dumptype_get_include(dumptype)            get_conftype_exinclude(&dumptype->value[DUMPTYPE_INCLUDE])
#define dumptype_get_priority(dumptype)           get_conftype_priority (&dumptype->value[DUMPTYPE_PRIORITY])
#define dumptype_get_dumpcycle(dumptype)          get_conftype_int      (&dumptype->value[DUMPTYPE_DUMPCYCLE])
#define dumptype_get_maxcycle(dumptype)           get_conftype_int      (&dumptype->value[DUMPTYPE_MAXCYCLE])
#define dumptype_get_frequency(dumptype)          get_conftype_int      (&dumptype->value[DUMPTYPE_FREQUENCY])
#define dumptype_get_maxdumps(dumptype)           get_conftype_int      (&dumptype->value[DUMPTYPE_MAXDUMPS])
#define dumptype_get_maxpromoteday(dumptype)      get_conftype_int      (&dumptype->value[DUMPTYPE_MAXPROMOTEDAY])
#define dumptype_get_bumppercent(dumptype)        get_conftype_int      (&dumptype->value[DUMPTYPE_BUMPPERCENT])
#define dumptype_get_bumpsize(dumptype)           get_conftype_am64     (&dumptype->value[DUMPTYPE_BUMPSIZE])
#define dumptype_get_bumpdays(dumptype)           get_conftype_int      (&dumptype->value[DUMPTYPE_BUMPDAYS])
#define dumptype_get_bumpmult(dumptype)           get_conftype_real     (&dumptype->value[DUMPTYPE_BUMPMULT])
#define dumptype_get_starttime(dumptype)          get_conftype_time     (&dumptype->value[DUMPTYPE_STARTTIME])
#define dumptype_get_strategy(dumptype)           get_conftype_strategy (&dumptype->value[DUMPTYPE_STRATEGY])
#define dumptype_get_estimate(dumptype)           get_conftype_estimate (&dumptype->value[DUMPTYPE_ESTIMATE])
#define dumptype_get_compress(dumptype)           get_conftype_compress (&dumptype->value[DUMPTYPE_COMPRESS])
#define dumptype_get_encrypt(dumptype)            get_conftype_encrypt  (&dumptype->value[DUMPTYPE_ENCRYPT])
#define dumptype_get_srv_decrypt_opt(dumptype)    get_conftype_string   (&dumptype->value[DUMPTYPE_SRV_DECRYPT_OPT])
#define dumptype_get_clnt_decrypt_opt(dumptype)   get_conftype_string   (&dumptype->value[DUMPTYPE_CLNT_DECRYPT_OPT])
#define dumptype_get_comprate(dumptype)                                   dumptype->value[DUMPTYPE_COMPRATE].v.rate
#define dumptype_get_tape_splitsize(dumptype)     get_conftype_am64     (&dumptype->value[DUMPTYPE_TAPE_SPLITSIZE])
#define dumptype_get_fallback_splitsize(dumptype) get_conftype_am64     (&dumptype->value[DUMPTYPE_FALLBACK_SPLITSIZE])
#define dumptype_get_split_diskbuffer(dumptype)   get_conftype_string   (&dumptype->value[DUMPTYPE_SPLIT_DISKBUFFER])
#define dumptype_get_record(dumptype)             get_conftype_bool     (&dumptype->value[DUMPTYPE_RECORD])
#define dumptype_get_skip_incr(dumptype)          get_conftype_bool     (&dumptype->value[DUMPTYPE_SKIP_INCR])
#define dumptype_get_skip_full(dumptype)          get_conftype_bool     (&dumptype->value[DUMPTYPE_SKIP_FULL])
#define dumptype_get_to_holdingdisk(dumptype)     get_conftype_hold     (&dumptype->value[DUMPTYPE_HOLDINGDISK])
#define dumptype_get_kencrypt(dumptype)           get_conftype_bool     (&dumptype->value[DUMPTYPE_KENCRYPT])
#define dumptype_get_ignore(dumptype)             get_conftype_bool     (&dumptype->value[DUMPTYPE_IGNORE])
#define dumptype_get_index(dumptype)              get_conftype_bool     (&dumptype->value[DUMPTYPE_INDEX])

/* A network interface */
typedef enum interface_e  {
    INTER_COMMENT,
    INTER_MAXUSAGE,
    INTER_INTER
} interface_ee;


typedef struct interface_s {
    struct interface_s *next;
    int seen;
    char *name;

    val_t value[INTER_INTER];

    unsigned long curusage;		/* current usage */
} interface_t;

#define interface_get_name(interface) interface->name
#define interface_get_seen(interface) interface->seen
#define interface_get_comment(interface)  get_conftype_string(&interface->value[INTER_COMMENT])
#define interface_get_maxusage(interface) get_conftype_int   (&interface->value[INTER_MAXUSAGE])

/* A holding disk */
typedef enum holdingdisk_e  {
    HOLDING_COMMENT,
    HOLDING_DISKDIR,
    HOLDING_DISKSIZE,
    HOLDING_CHUNKSIZE,
    HOLDING_HOLDING
} holdingdisk_ee;

typedef struct holdingdisk_s {
    struct holdingdisk_s *next;
    int seen;
    char *name;

    val_t value[HOLDING_HOLDING];

    void *up;			/* generic user pointer */
    off_t disksize;
} holdingdisk_t;

#define holdingdisk_get_name(holdingdisk) (holdingdisk)->name
#define holdingdisk_get_seen(holdingdisk) (holdingdisk)->seen
#define holdingdisk_get_comment(holdingdisk)   get_conftype_string(&(holdingdisk)->value[HOLDING_COMMENT])
#define holdingdisk_get_diskdir(holdingdisk)   get_conftype_string(&(holdingdisk)->value[HOLDING_DISKDIR])
#define holdingdisk_get_disksize(holdingdisk)  get_conftype_am64  (&(holdingdisk)->value[HOLDING_DISKSIZE])
#define holdingdisk_get_chunksize(holdingdisk) get_conftype_am64  (&(holdingdisk)->value[HOLDING_CHUNKSIZE])

/* for each column we define some values on how to
 * format this column element
 */
typedef struct {
    char *Name;		/* column name */
    int PrefixSpace;	/* the blank space to print before this
   			 * column. It is used to get the space
			 * between the colums
			 */
    int Width;		/* the width of the column itself */
    int Precision;	/* the precision if its a float */
    int MaxWidth;	/* if set, Width will be recalculated
    			 * to the space needed */
    char *Format;	/* the printf format string for this
   			 * column element
			 */
    char *Title;	/* the title to use for this column */
} ColumnInfo;


/* predeclare local functions */

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
int         *get_conftype_intrange (val_t *);

void command_overwrite(command_option_t *command_options, t_conf_var *overwrite_var,
		       keytab_t *keytab, val_t *valarray, char *prefix);

void free_new_argv(int new_argc, char **new_argv);
/* this corresponds to the normal output of amanda, but may
 * be adapted to any spacing as you like.
 */
extern ColumnInfo ColumnData[];

extern char *config_name;
extern char *config_dir;

extern int debug_amandad;
extern int debug_amidxtaped;
extern int debug_amindexd;
extern int debug_amrecover;
extern int debug_auth;
extern int debug_event;
extern int debug_holding;
extern int debug_protocol;
extern int debug_planner;
extern int debug_driver;
extern int debug_dumper;
extern int debug_chunker;
extern int debug_taper;
extern int debug_selfcheck;
extern int debug_sendsize;
extern int debug_sendbackup;

extern holdingdisk_t *holdingdisks;
extern int num_holdingdisks;

void parse_conf(int parse_argc, char **parse_argv, int *new_argc,
		       char ***new_argv);
char **get_config_options(int);
void report_bad_conf_arg(void);
void free_server_config(void);

int read_conffile(char *filename);

#define CLIENTCONFFILE_NAME "client.conf"

int  add_client_conf(confparm_t parm, char *value);
int read_clientconf(char *filename);
char *generic_client_get_security_conf(char *, void *);

int getconf_seen(confparm_t parameter);
int getconf_boolean(confparm_t parameter);
int getconf_int(confparm_t parameter);
long getconf_long(confparm_t parameter);
ssize_t getconf_size(confparm_t parameter);
time_t getconf_time(confparm_t parameter);
off_t getconf_am64(confparm_t parameter);
double getconf_real(confparm_t parameter);
char *getconf_str(confparm_t parameter);
int getconf_taperalgo(confparm_t parameter);
int *getconf_intrange(confparm_t parameter);
char *getconf_byname(char *confname);
char *getconf_list(char *listname);
dumptype_t *lookup_dumptype(char *identifier);
dumptype_t *read_dumptype(char *name, FILE *from, char *fname, int *linenum);
tapetype_t *lookup_tapetype(char *identifier);
holdingdisk_t *lookup_holdingdisk(char *identifier);
interface_t *lookup_interface(char *identifier);
holdingdisk_t *getconf_holdingdisks(void);
long int getconf_unit_divisor(void);
void dump_configuration(char *filename);
int ColumnDataCount(void);
int StringToColumn(char *s);
char LastChar(char *s);
int SetColumDataFromString(ColumnInfo* ci, char *s, char **errstr);
ssize_t getconf_readblocksize(void);

/* this is in securityconf.h */
char *generic_get_security_conf(char *, void *);
#endif /* ! CONFFILE_H */
