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
 * $Id: conffile.c,v 1.156 2006/07/26 15:17:37 martinea Exp $
 *
 * read configuration file
 */

#include "amanda.h"
#include "arglist.h"
#include "util.h"
#include "conffile.h"
#include "clock.h"
#include <glib.h>

/*
 * Lexical analysis
 */

/* This module implements its own quixotic lexer and parser, present for historical
 * reasons.  If this were written from scratch, it would use flex/bison. */

/* An enumeration of the various tokens that might appear in a configuration file.
 *
 * - CONF_UNKNOWN has special meaning as an unrecognized token.
 * - CONF_ANY can be used to request any token, rather than requiring a specific
 *   token.
 */
typedef enum {
    CONF_UNKNOWN,		CONF_ANY,		CONF_COMMA,
    CONF_LBRACE,		CONF_RBRACE,		CONF_NL,
    CONF_END,			CONF_IDENT,		CONF_INT,
    CONF_INT64,			CONF_BOOL,		CONF_REAL,
    CONF_STRING,		CONF_TIME,		CONF_SIZE,

    /* config parameters */
    CONF_INCLUDEFILE,		CONF_ORG,		CONF_MAILTO,
    CONF_DUMPUSER,		CONF_TAPECYCLE,		CONF_TAPEDEV,
    CONF_CHANGERDEV,		CONF_CHANGERFILE,	CONF_LABELSTR,
    CONF_BUMPPERCENT,		CONF_BUMPSIZE,		CONF_BUMPDAYS,
    CONF_BUMPMULT,		CONF_ETIMEOUT,		CONF_DTIMEOUT,
    CONF_CTIMEOUT,		CONF_TAPELIST,
    CONF_DEVICE_OUTPUT_BUFFER_SIZE,
    CONF_DISKFILE,		CONF_INFOFILE,		CONF_LOGDIR,
    CONF_LOGFILE,		CONF_DISKDIR,		CONF_DISKSIZE,
    CONF_INDEXDIR,		CONF_NETUSAGE,		CONF_INPARALLEL,
    CONF_DUMPORDER,		CONF_TIMEOUT,		CONF_TPCHANGER,
    CONF_RUNTAPES,		CONF_DEFINE,		CONF_DUMPTYPE,
    CONF_TAPETYPE,		CONF_INTERFACE,		CONF_PRINTER,
    CONF_MAILER,
    CONF_AUTOFLUSH,		CONF_RESERVE,		CONF_MAXDUMPSIZE,
    CONF_COLUMNSPEC,		CONF_AMRECOVER_DO_FSF,	CONF_AMRECOVER_CHECK_LABEL,
    CONF_AMRECOVER_CHANGER,	CONF_LABEL_NEW_TAPES,	CONF_USETIMESTAMPS,
    CONF_CHANGER,

    CONF_TAPERALGO,		CONF_FIRST,		CONF_FIRSTFIT,
    CONF_LARGEST,		CONF_LARGESTFIT,	CONF_SMALLEST,
    CONF_LAST,			CONF_DISPLAYUNIT,	CONF_RESERVED_UDP_PORT,
    CONF_RESERVED_TCP_PORT,	CONF_UNRESERVED_TCP_PORT,
    CONF_TAPERFLUSH,
    CONF_FLUSH_THRESHOLD_DUMPED,
    CONF_FLUSH_THRESHOLD_SCHEDULED,
    CONF_DEVICE_PROPERTY,      CONF_PROPERTY,		CONF_PLUGIN,
    CONF_APPLICATION,          CONF_APPLICATION_TOOL,
    CONF_SCRIPT,               CONF_SCRIPT_TOOL,
    CONF_EXECUTE_ON,           CONF_EXECUTE_WHERE,	CONF_SEND_AMREPORT_ON,
    CONF_DEVICE,               CONF_ORDER,		CONF_SINGLE_EXECUTION,
    CONF_DATA_PATH,            CONF_AMANDA,		CONF_DIRECTTCP,
    CONF_TAPER_PARALLEL_WRITE, CONF_INTERACTIVITY,	CONF_TAPERSCAN,
    CONF_MAX_DLE_BY_VOLUME,    CONF_EJECT_VOLUME,	CONF_TMPDIR,

    /* execute on */
    CONF_PRE_AMCHECK,          CONF_POST_AMCHECK,
    CONF_PRE_DLE_AMCHECK,      CONF_PRE_HOST_AMCHECK,
    CONF_POST_DLE_AMCHECK,     CONF_POST_HOST_AMCHECK,
    CONF_PRE_ESTIMATE,         CONF_POST_ESTIMATE,
    CONF_PRE_DLE_ESTIMATE,     CONF_PRE_HOST_ESTIMATE,
    CONF_POST_DLE_ESTIMATE,    CONF_POST_HOST_ESTIMATE,
    CONF_PRE_BACKUP,           CONF_POST_BACKUP,
    CONF_PRE_DLE_BACKUP,       CONF_PRE_HOST_BACKUP,
    CONF_POST_DLE_BACKUP,      CONF_POST_HOST_BACKUP,
    CONF_PRE_RECOVER,	       CONF_POST_RECOVER,
    CONF_PRE_LEVEL_RECOVER,    CONF_POST_LEVEL_RECOVER,
    CONF_INTER_LEVEL_RECOVER,

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
    CONF_CLIENT_USERNAME,	CONF_CLIENT_PORT,	CONF_ALLOW_SPLIT,
    CONF_MAX_WARNINGS,

    /* tape type */
    /*COMMENT,*/		CONF_BLOCKSIZE,
    CONF_LBL_TEMPL,		CONF_FILEMARK,		CONF_LENGTH,
    CONF_SPEED,			CONF_READBLOCKSIZE,

    /* client conf */
    CONF_CONF,			CONF_INDEX_SERVER,	CONF_TAPE_SERVER,
    CONF_SSH_KEYS,		CONF_GNUTAR_LIST_DIR,	CONF_AMANDATES,
    CONF_AMDUMP_SERVER,

    /* protocol config */
    CONF_REP_TRIES,		CONF_CONNECT_TRIES,	CONF_REQ_TRIES,

    /* debug config */
    CONF_DEBUG_DAYS,
    CONF_DEBUG_AMANDAD,		CONF_DEBUG_AMIDXTAPED,	CONF_DEBUG_AMINDEXD,
    CONF_DEBUG_AMRECOVER,	CONF_DEBUG_AUTH,	CONF_DEBUG_EVENT,
    CONF_DEBUG_HOLDING,		CONF_DEBUG_PROTOCOL,	CONF_DEBUG_PLANNER,
    CONF_DEBUG_DRIVER,		CONF_DEBUG_DUMPER,	CONF_DEBUG_CHUNKER,
    CONF_DEBUG_TAPER,		CONF_DEBUG_SELFCHECK,	CONF_DEBUG_SENDSIZE,
    CONF_DEBUG_SENDBACKUP,	CONF_DEBUG_RECOVERY,

    /* network interface */
    /* COMMENT, */		/* USE, */

    /* dump options (obsolete) */
    CONF_EXCLUDE_FILE,		CONF_EXCLUDE_LIST,

    /* compress, estimate, encryption */
    CONF_NONE,			CONF_FAST,		CONF_BEST,
    CONF_SERVER,		CONF_CLIENT,		CONF_CALCSIZE,
    CONF_CUSTOM,

    /* autolabel */
    CONF_AUTOLABEL,		CONF_ANY_VOLUME,	CONF_OTHER_CONFIG,
    CONF_NON_AMANDA,		CONF_VOLUME_ERROR,	CONF_EMPTY,
    CONF_META_AUTOLABEL,

    /* part_cache_type */
    CONF_PART_SIZE,		CONF_PART_CACHE_TYPE,	CONF_PART_CACHE_DIR,
    CONF_PART_CACHE_MAX_SIZE,	CONF_DISK,		CONF_MEMORY,

    /* host-limit */
    CONF_RECOVERY_LIMIT,	CONF_SAME_HOST,		CONF_DUMP_LIMIT,

    /* holdingdisk */
    CONF_NEVER,			CONF_AUTO,		CONF_REQUIRED,

    /* send_amreport */
    CONF_ALL,			CONF_STRANGE,		CONF_ERROR,

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
    CONF_MULT1T,

    /* boolean */
    CONF_ATRUE,			CONF_AFALSE,

    CONF_CLIENT_NAME,
} tok_t;

/* A keyword table entry, mapping the given keyword to the given token.
 * Note that punctuation, integers, and quoted strings are handled 
 * internally to the lexer, so they do not appear here. */
typedef struct {
    char *keyword;
    tok_t token;
} keytab_t;

/* The current keyword table, used by all token-related functions */
static keytab_t *keytable = NULL;

/* Has a token been "ungotten", and if so, what was it? */
static int token_pushed;
static tok_t pushed_tok;

/* The current token and its value.  Note that, unlike most other val_t*,
 * tokenval's v.s points to statically allocated memory which cannot be
 * free()'d. */
static tok_t tok;
static val_t tokenval;

/* The current input information: file, filename, line, and character 
 * (which points somewhere within current_line) */
static FILE *current_file = NULL;
static char *current_filename = NULL;
static char *current_line = NULL;
static char *current_char = NULL;
static char *current_block = NULL;
static int current_line_num = 0; /* (technically, managed by the parser) */

/* A static buffer for storing tokens while they are being scanned. */
static char tkbuf[4096];

/* Return a token formated for output */
static char *str_keyword(keytab_t *kt);

static char *str_keyword(keytab_t *kt);
/* Look up the name of the given token in the current keytable */
static char *get_token_name(tok_t);

/* Look up a token in keytable, given a string, returning CONF_UNKNOWN 
 * for unrecognized strings.  Search is case-insensitive. */
static tok_t lookup_keyword(char *str);

/* Get the next token.  If exp is anything but CONF_ANY, and the next token
 * does not match, then a parse error is flagged.  This function reads from the
 * current_* static variables, recognizes keywords against the keytable static
 * variable, and places its result in tok and tokenval. */
static void get_conftoken(tok_t exp);

/* "Unget" the current token; this supports a 1-token lookahead. */
static void unget_conftoken(void);

/* Tokenizer character-by-character access. */
static int  conftoken_getc(void);
static int  conftoken_ungetc(int c);

static void merge_proplist_foreach_fn(gpointer key_p,
                                      gpointer value_p,
                                      gpointer user_data_p);
static void copy_proplist_foreach_fn(gpointer key_p,
                                     gpointer value_p,
                                     gpointer user_data_p);

/*
 * Parser
 */

/* A parser table entry.  Read as "<token> introduces parameter <parm>,
 * the data for which will be read by <read_function> and validated by
 * <validate_function> (if not NULL).  <type> is only used in formatting 
 * config overwrites. */
typedef struct conf_var_s {
    tok_t	token;
    conftype_t	type;
    void	(*read_function) (struct conf_var_s *, val_t*);
    int		parm;
    void	(*validate_function) (struct conf_var_s *, val_t *);
} conf_var_t;

/* This is a list of filenames that are used in 'seen_t' structs. */
static GSList *seen_filenames = NULL;

/* get a copy of filename that's stored in seen_filenames so that it won't go
 * away until config_uninit. */
static char *get_seen_filename(char *filename);

/* If allow_overwrites is true, the a parameter which has already been
 * seen will simply overwrite the old value, rather than triggering an 
 * error.  Note that this does not apply to all parameters, e.g., 
 * device_property */
static int allow_overwrites;

/* subsection structs
 *
 * The 'seen' fields in these structs are useless outside this module;
 * they are only used to generate error messages for multiply defined
 * subsections.
 */
struct tapetype_s {
    struct tapetype_s *next;
    seen_t seen;
    char *name;

    val_t value[TAPETYPE_TAPETYPE];
};

struct dumptype_s {
    struct dumptype_s *next;
    seen_t seen;
    char *name;

    val_t value[DUMPTYPE_DUMPTYPE];
};

struct interface_s {
    struct interface_s *next;
    seen_t seen;
    char *name;

    val_t value[INTER_INTER];
};

struct holdingdisk_s {
    seen_t seen;
    char *name;

    val_t value[HOLDING_HOLDING];
};

struct application_s {
    struct application_s *next;
    seen_t seen;
    char *name;

    val_t value[APPLICATION_APPLICATION];
};

struct pp_script_s {
    struct pp_script_s *next;
    seen_t seen;
    char *name;

    val_t value[PP_SCRIPT_PP_SCRIPT];
};

struct device_config_s {
    struct device_config_s *next;
    seen_t seen;
    char *name;

    val_t value[DEVICE_CONFIG_DEVICE_CONFIG];
};

struct changer_config_s {
    struct changer_config_s *next;
    seen_t seen;
    char *name;

    val_t value[CHANGER_CONFIG_CHANGER_CONFIG];
};

struct interactivity_s {
    struct interactivity_s *next;
    seen_t seen;
    char *name;

    val_t value[INTERACTIVITY_INTERACTIVITY];
};

struct taperscan_s {
    struct taperscan_s *next;
    seen_t seen;
    char *name;

    val_t value[TAPERSCAN_TAPERSCAN];
};

/* The current parser table */
static conf_var_t *parsetable = NULL;

/* Read and parse a configuration file, recursively reading any included
 * files.  This function sets the keytable and parsetable appropriately
 * according to is_client.
 *
 * @param filename: configuration file to read
 * @param is_client: true if this is a client
 * @param missing_ok: is it OK if the file is missing?
 */
static void read_conffile(char *filename,
			  gboolean is_client,
			  gboolean missing_ok);

/* Read and process a line of input from the current file, using the 
 * current keytable and parsetable.  For blocks, this recursively
 * reads the entire block.
 *
 * @param is_client: true if this is a client
 * @returns: true on success, false on EOF
 */
static gboolean read_confline(gboolean is_client);

/* Handle an invalid token, recognizing deprecated tokens as such,
 * and producing an appropriate error message.
 *
 * @param token: the identifier
 */
static void handle_invalid_keyword(const char * token);

/* Check whether token is deprecated, and issue a warning if it
 * is.  This consults the global variables 'tok' and 'tokenval'
 */
static void handle_deprecated_keyword(void);

/* Read a brace-delimited block using the given parse table.  This
 * function is used to read brace-delimited subsections in the config
 * files and also (via read_dumptype) to read dumptypes from
 * the disklist.
 *
 * This function implements "inheritance" as follows: if a bare
 * identifier occurs within the braces, it calls copy_function (if
 * not NULL), which looks up an existing subsection using the
 * identifier from tokenval and copies any values not already seen
 * into valarray.
 *
 * @param read_var: the parse table to use
 * @param valarray: the (pre-initialized) val_t array to fill in
 * @param errormsg: error message to display for unrecognized keywords
 * @param read_brace: if true, read the opening brace
 * @param copy_function: function to copy configuration from
 *     another subsection into this one.
 */
static void read_block(conf_var_t *read_var, val_t *valarray, 
		       char *errormsg, int read_brace,
		       void (*copy_function)(void),
		       char *type, char *name);

/* For each subsection type, we have a global and  four functions:
 *  - foocur is a temporary struct used to assemble new subsections
 *  - get_foo is called after reading "DEFINE FOO", and
 *    is responsible for reading the entire block, using
 *    read_block()
 *  - init_foo_defaults initializes a new subsection struct
 *    to its default values
 *  - save_foo copies foocur to a newly allocated struct and
 *    inserts that into the relevant list.
 *  - copy_foo implements inheritance as described in read_block()
 */
static holdingdisk_t hdcur;
static void get_holdingdisk(int is_define);
static void init_holdingdisk_defaults(void);
static void save_holdingdisk(void);
static void copy_holdingdisk(void);

static dumptype_t dpcur;
static void get_dumptype(void);
static void init_dumptype_defaults(void);
static void save_dumptype(void);
static void copy_dumptype(void);

static tapetype_t tpcur;
static void get_tapetype(void);
static void init_tapetype_defaults(void);
static void save_tapetype(void);
static void copy_tapetype(void);

static interface_t ifcur;
static void get_interface(void);
static void init_interface_defaults(void);
static void save_interface(void);
static void copy_interface(void);

static application_t apcur;
static void get_application(void);
static void init_application_defaults(void);
static void save_application(void);
static void copy_application(void);

static pp_script_t pscur;
static void get_pp_script(void);
static void init_pp_script_defaults(void);
static void save_pp_script(void);
static void copy_pp_script(void);

static device_config_t dccur;
static void get_device_config(void);
static void init_device_config_defaults(void);
static void save_device_config(void);
static void copy_device_config(void);

static changer_config_t cccur;
static void get_changer_config(void);
static void init_changer_config_defaults(void);
static void save_changer_config(void);
static void copy_changer_config(void);

static interactivity_t ivcur;
static void get_interactivity(void);
static void init_interactivity_defaults(void);
static void save_interactivity(void);
static void copy_interactivity(void);

static taperscan_t tscur;
static void get_taperscan(void);
static void init_taperscan_defaults(void);
static void save_taperscan(void);
static void copy_taperscan(void);

/* read_functions -- these fit into the read_function slot in a parser
 * table entry, and are responsible for calling getconf_token as necessary
 * to consume their arguments, and setting their second argument with the
 * result.  The first argument is a copy of the parser table entry, if
 * needed. */
static void read_int(conf_var_t *, val_t *);
static void read_int64(conf_var_t *, val_t *);
static void read_real(conf_var_t *, val_t *);
static void read_str(conf_var_t *, val_t *);
static void read_ident(conf_var_t *, val_t *);
static void read_time(conf_var_t *, val_t *);
static void read_size(conf_var_t *, val_t *);
static void read_bool(conf_var_t *, val_t *);
static void read_no_yes_all(conf_var_t *, val_t *);
static void read_compress(conf_var_t *, val_t *);
static void read_encrypt(conf_var_t *, val_t *);
static void read_holding(conf_var_t *, val_t *);
static void read_estimatelist(conf_var_t *, val_t *);
static void read_strategy(conf_var_t *, val_t *);
static void read_taperalgo(conf_var_t *, val_t *);
static void read_send_amreport_on(conf_var_t *, val_t *);
static void read_data_path(conf_var_t *, val_t *);
static void read_priority(conf_var_t *, val_t *);
static void read_rate(conf_var_t *, val_t *);
static void read_exinclude(conf_var_t *, val_t *);
static void read_intrange(conf_var_t *, val_t *);
static void read_dapplication(conf_var_t *, val_t *);
static void read_dinteractivity(conf_var_t *, val_t *);
static void read_dtaperscan(conf_var_t *, val_t *);
static void read_dpp_script(conf_var_t *, val_t *);
static void read_property(conf_var_t *, val_t *);
static void read_execute_on(conf_var_t *, val_t *);
static void read_execute_where(conf_var_t *, val_t *);
static void read_holdingdisk(conf_var_t *, val_t *);
static void read_int_or_str(conf_var_t *, val_t *);
static void read_autolabel(conf_var_t *, val_t *);
static void read_part_cache_type(conf_var_t *, val_t *);
static void read_host_limit(conf_var_t *, val_t *);

static application_t *read_application(char *name, FILE *from, char *fname,
				       int *linenum);
static pp_script_t *read_pp_script(char *name, FILE *from, char *fname,
				   int *linenum);
static device_config_t *read_device_config(char *name, FILE *from, char *fname,
					   int *linenum);
static changer_config_t *read_changer_config(char *name, FILE *from,
					     char *fname, int *linenum);
static interactivity_t *read_interactivity(char *name, FILE *from,
					   char *fname, int *linenum);
static taperscan_t *read_taperscan(char *name, FILE *from, char *fname,
				   int *linenum);
/* Functions to get various types of values.  These are called by
 * read_functions to take care of any variations in the way that these
 * values can be written: integers can have units, boolean values can be
 * specified with a number of names, etc.  They form utility functions
 * for the read_functions, below. */
static gint64 get_multiplier(gint64 val, confunit_t unit);
static time_t  get_time(void);
static int     get_int(confunit_t unit);
static ssize_t get_size(confunit_t unit);
static gint64  get_int64(confunit_t unit);
static int     get_bool(void);
static int     get_no_yes_all(void);

/* Check the given 'seen', flagging an error if this value has already
 * been seen and allow_overwrites is false.  Also marks the value as
 * seen on the current line.
 *
 * @param seen: (in/out) seen value to adjust
 */
static void ckseen(seen_t *seen);

/* validate_functions -- these fit into the validate_function solt in
 * a parser table entry.  They call conf_parserror if the value in their
 * second argument is invalid.  */
static void validate_nonnegative(conf_var_t *, val_t *);
static void validate_non_zero(conf_var_t *, val_t *);
static void validate_positive(conf_var_t *, val_t *);
static void validate_runspercycle(conf_var_t *, val_t *);
static void validate_bumppercent(conf_var_t *, val_t *);
static void validate_bumpmult(conf_var_t *, val_t *);
static void validate_inparallel(conf_var_t *, val_t *);
static void validate_displayunit(conf_var_t *, val_t *);
static void validate_reserve(conf_var_t *, val_t *);
static void validate_use(conf_var_t *, val_t *);
static void validate_chunksize(conf_var_t *, val_t *);
static void validate_blocksize(conf_var_t *, val_t *);
static void validate_debug(conf_var_t *, val_t *);
static void validate_port_range(val_t *, int, int);
static void validate_reserved_port_range(conf_var_t *, val_t *);
static void validate_unreserved_port_range(conf_var_t *, val_t *);
static void validate_program(conf_var_t *, val_t *);
static void validate_dump_limit(conf_var_t *, val_t *);
static void validate_tmpdir(conf_var_t *, val_t *);

gint compare_pp_script_order(gconstpointer a, gconstpointer b);

/*
 * Initialization
 */

/* The name of the configuration under which this application is running.
 * This variable is initialized by config_init.
 */
static char *config_name = NULL;

/* The directory containing the configuration for this application.  This
 * variable is initialized by config_init
 */
static char *config_dir = NULL;

/* The most recently read top-level configuration file.  This variable is
 * initialized by config_init
 */
static char *config_filename = NULL;

/* Has the config been initialized? */
static gboolean config_initialized = FALSE;

/* Are we running a client? (true if last init was
 * with CONFIG_INIT_CLIENT) */
static gboolean config_client = FALSE;

/* What config overwrites to use? */
static config_overrides_t *config_overrides = NULL;

/* All global parameters */
static val_t conf_data[CNF_CNF];

/* Linked list of holding disks */
static GSList *holdinglist = NULL;
static dumptype_t *dumplist = NULL;
static tapetype_t *tapelist = NULL;
static interface_t *interface_list = NULL;
static application_t *application_list = NULL;
static pp_script_t *pp_script_list = NULL;
static device_config_t *device_config_list = NULL;
static changer_config_t *changer_config_list = NULL;
static interactivity_t *interactivity_list = NULL;
static taperscan_t *taperscan_list = NULL;

/* storage for derived values */
static long int unit_divisor = 1;

int debug_amandad    = 0;
int debug_recovery   = 0;
int debug_amidxtaped = 0;
int debug_amindexd   = 0;
int debug_amrecover  = 0;
int debug_auth       = 0;
int debug_event      = 0;
int debug_holding    = 0;
int debug_protocol   = 0;
int debug_planner    = 0;
int debug_driver     = 0;
int debug_dumper     = 0;
int debug_chunker    = 0;
int debug_taper      = 0;
int debug_selfcheck  = 0;
int debug_sendsize   = 0;
int debug_sendbackup = 0;

/* Reset all configuration values to their defaults (which, in many
 * cases, come from --with-foo options at build time) */
static void init_defaults(void);

/* Update all dervied values based on the current configuration.  This
 * function can be called multiple times, once after each adjustment
 * to the current configuration.
 *
 * @param is_client: are we running a client?
 */
static void update_derived_values(gboolean is_client);

static cfgerr_level_t apply_config_overrides(config_overrides_t *co,
					     char *key_ovr);

/* per-type conf_init functions, used as utilities for init_defaults
 * and for each subsection's init_foo_defaults.
 *
 * These set the value's type and seen flags, as well as copying
 * the relevant value into the 'v' field.
 */
static void conf_init_int(val_t *val, confunit_t unit, int i);
static void conf_init_int64(val_t *val, confunit_t unit, gint64 l);
static void conf_init_real(val_t *val, float r);
static void conf_init_str(val_t *val, char *s);
static void conf_init_ident(val_t *val, char *s);
static void conf_init_identlist(val_t *val, char *s);
static void conf_init_time(val_t *val, time_t t);
static void conf_init_size(val_t *val, confunit_t unit, ssize_t sz);
static void conf_init_bool(val_t *val, int i);
static void conf_init_no_yes_all(val_t *val, int i);
static void conf_init_compress(val_t *val, comp_t i);
static void conf_init_encrypt(val_t *val, encrypt_t i);
static void conf_init_data_path(val_t *val, data_path_t i);
static void conf_init_holding(val_t *val, dump_holdingdisk_t i);
static void conf_init_estimatelist(val_t *val, estimate_t i);
static void conf_init_execute_on(val_t *, int);
static void conf_init_execute_where(val_t *, int);
static void conf_init_send_amreport(val_t *val, send_amreport_t i);
static void conf_init_strategy(val_t *val, strategy_t);
static void conf_init_taperalgo(val_t *val, taperalgo_t i);
static void conf_init_priority(val_t *val, int i);
static void conf_init_rate(val_t *val, float r1, float r2);
static void conf_init_exinclude(val_t *val); /* to empty list */
static void conf_init_intrange(val_t *val, int i1, int i2);
static void conf_init_proplist(val_t *val); /* to empty list */
static void conf_init_application(val_t *val);
static void conf_init_autolabel(val_t *val);
static void conf_init_part_cache_type(val_t *val, part_cache_type_t i);
static void conf_init_host_limit(val_t *val);
static void conf_init_host_limit_server(val_t *val);

/*
 * Command-line Handling
 */

typedef struct config_override_s {
    char     *key;
    char     *value;
    gboolean  applied;
} config_override_t;

struct config_overrides_s {
    int n_allocated;
    int n_used;
    config_override_t *ovr;
};

/*
 * val_t Management
 */

static void merge_val_t(val_t *, val_t *);
static void copy_val_t(val_t *, val_t *);
static void free_val_t(val_t *);

/*
 * Utilities
 */

/* memory handling */
void free_property_t(gpointer p);

/* Utility functions/structs for val_t_display_strs */
static char *exinclude_display_str(val_t *val, int file);
static void proplist_display_str_foreach_fn(gpointer key_p, gpointer value_p, gpointer user_data_p);
static void val_t_print_token(gboolean print_default, gboolean print_source, FILE *output, char *prefix, char *format, keytab_t *kt, val_t *val);

/* Given a key name as used in config overwrites, return a pointer to the corresponding
 * conf_var_t in the current parsetable, and the val_t representing that value.  This
 * function will access subsections if key has the form  TYPE:SUBSEC:KEYWORD.  Returns
 * false if the value does not exist.
 *
 * Assumes keytable and parsetable are set correctly, which is generally OK after 
 * config_init has been called.
 *
 * @param key: the key to look up
 * @param parm: (result) the parse table entry
 * @param val: (result) the parameter value
 * @returns: true on success
 */
static int parm_key_info(char *key, conf_var_t **parm, val_t **val);

/*
 * Error handling
 */

/* Have we seen a parse error yet?  Parsing continues after an error, so this
 * flag is checked after the parse is complete.
 */
static cfgerr_level_t cfgerr_level;
static GSList *cfgerr_errors = NULL;

static void conf_error_common(cfgerr_level_t level, const char * format, va_list argp);
static void    conf_parserror(const char *format, ...)
                __attribute__ ((format (printf, 1, 2)));

static void    conf_parswarn(const char *format, ...)
                __attribute__ ((format (printf, 1, 2)));

/*
 * Tables
 */

/* First, the keyword tables for client and server */
keytab_t client_keytab[] = {
    { "CONF", CONF_CONF },
    { "AMDUMP_SERVER", CONF_AMDUMP_SERVER },
    { "INDEX_SERVER", CONF_INDEX_SERVER },
    { "TAPE_SERVER", CONF_TAPE_SERVER },
    { "TAPEDEV", CONF_TAPEDEV },
    { "AUTH", CONF_AUTH },
    { "SSH_KEYS", CONF_SSH_KEYS },
    { "AMANDAD_PATH", CONF_AMANDAD_PATH },
    { "CLIENT_NAME", CONF_CLIENT_NAME },
    { "CLIENT_USERNAME", CONF_CLIENT_USERNAME },
    { "CLIENT_PORT", CONF_CLIENT_PORT },
    { "GNUTAR_LIST_DIR", CONF_GNUTAR_LIST_DIR },
    { "AMANDATES", CONF_AMANDATES },
    { "KRB5KEYTAB", CONF_KRB5KEYTAB },
    { "KRB5PRINCIPAL", CONF_KRB5PRINCIPAL },
    { "INCLUDEFILE", CONF_INCLUDEFILE },
    { "CONNECT_TRIES", CONF_CONNECT_TRIES },
    { "REP_TRIES", CONF_REP_TRIES },
    { "REQ_TRIES", CONF_REQ_TRIES },
    { "CLIENT", CONF_CLIENT },
    { "DEBUG_DAYS", CONF_DEBUG_DAYS },
    { "DEBUG_AMANDAD", CONF_DEBUG_AMANDAD },
    { "DEBUG_RECOVERY", CONF_DEBUG_RECOVERY },
    { "DEBUG_AMIDXTAPED", CONF_DEBUG_AMIDXTAPED },
    { "DEBUG_AMINDEXD", CONF_DEBUG_AMINDEXD },
    { "DEBUG_AMRECOVER", CONF_DEBUG_AMRECOVER },
    { "DEBUG_AUTH", CONF_DEBUG_AUTH },
    { "DEBUG_EVENT", CONF_DEBUG_EVENT },
    { "DEBUG_HOLDING", CONF_DEBUG_HOLDING },
    { "DEBUG_PROTOCOL", CONF_DEBUG_PROTOCOL },
    { "DEBUG_PLANNER", CONF_DEBUG_PLANNER },
    { "DEBUG_DRIVER", CONF_DEBUG_DRIVER },
    { "DEBUG_DUMPER", CONF_DEBUG_DUMPER },
    { "DEBUG_CHUNKER", CONF_DEBUG_CHUNKER },
    { "DEBUG_TAPER", CONF_DEBUG_TAPER },
    { "DEBUG_SELFCHECK", CONF_DEBUG_SELFCHECK },
    { "DEBUG_SENDSIZE", CONF_DEBUG_SENDSIZE },
    { "DEBUG_SENDBACKUP", CONF_DEBUG_SENDBACKUP },
    { "EXECUTE_ON", CONF_EXECUTE_ON },
    { "EXECUTE_WHERE", CONF_EXECUTE_WHERE },
    { "RESERVED_UDP_PORT", CONF_RESERVED_UDP_PORT },
    { "RESERVED_TCP_PORT", CONF_RESERVED_TCP_PORT },
    { "UNRESERVED_TCP_PORT", CONF_UNRESERVED_TCP_PORT },
    { "DEFINE", CONF_DEFINE },
    { "COMMENT", CONF_COMMENT },
    { "MAILER", CONF_MAILER },
    { "ORDER", CONF_ORDER },
    { "SCRIPT", CONF_SCRIPT },
    { "SCRIPT_TOOL", CONF_SCRIPT_TOOL },
    { "PLUGIN", CONF_PLUGIN },
    { "PRE_AMCHECK", CONF_PRE_AMCHECK },
    { "PRE_DLE_AMCHECK", CONF_PRE_DLE_AMCHECK },
    { "PRE_HOST_AMCHECK", CONF_PRE_HOST_AMCHECK },
    { "POST_AMCHECK", CONF_POST_AMCHECK },
    { "POST_DLE_AMCHECK", CONF_POST_DLE_AMCHECK },
    { "POST_HOST_AMCHECK", CONF_POST_HOST_AMCHECK },
    { "PRE_ESTIMATE", CONF_PRE_ESTIMATE },
    { "PRE_DLE_ESTIMATE", CONF_PRE_DLE_ESTIMATE },
    { "PRE_HOST_ESTIMATE", CONF_PRE_HOST_ESTIMATE },
    { "POST_ESTIMATE", CONF_POST_ESTIMATE },
    { "POST_DLE_ESTIMATE", CONF_POST_DLE_ESTIMATE },
    { "POST_HOST_ESTIMATE", CONF_POST_HOST_ESTIMATE },
    { "POST_BACKUP", CONF_POST_BACKUP },
    { "POST_DLE_BACKUP", CONF_POST_DLE_BACKUP },
    { "POST_HOST_BACKUP", CONF_POST_HOST_BACKUP },
    { "PRE_BACKUP", CONF_PRE_BACKUP },
    { "PRE_DLE_BACKUP", CONF_PRE_DLE_BACKUP },
    { "PRE_HOST_BACKUP", CONF_PRE_HOST_BACKUP },
    { "PRE_RECOVER", CONF_PRE_RECOVER },
    { "POST_RECOVER", CONF_POST_RECOVER },
    { "PRE_LEVEL_RECOVER", CONF_PRE_LEVEL_RECOVER },
    { "POST_LEVEL_RECOVER", CONF_POST_LEVEL_RECOVER },
    { "INTER_LEVEL_RECOVER", CONF_INTER_LEVEL_RECOVER },
    { "PRIORITY", CONF_PRIORITY },
    { "PROPERTY", CONF_PROPERTY },
    { "APPLICATION", CONF_APPLICATION },
    { "APPLICATION_TOOL", CONF_APPLICATION_TOOL },
    { "SERVER", CONF_SERVER },
    { "APPEND", CONF_APPEND },
    { NULL, CONF_IDENT },
    { NULL, CONF_UNKNOWN }
};

keytab_t server_keytab[] = {
    { "ALL", CONF_ALL },
    { "ALLOW_SPLIT", CONF_ALLOW_SPLIT },
    { "AMANDA", CONF_AMANDA },
    { "AMANDAD_PATH", CONF_AMANDAD_PATH },
    { "AMRECOVER_CHANGER", CONF_AMRECOVER_CHANGER },
    { "AMRECOVER_CHECK_LABEL", CONF_AMRECOVER_CHECK_LABEL },
    { "AMRECOVER_DO_FSF", CONF_AMRECOVER_DO_FSF },
    { "ANY", CONF_ANY_VOLUME },
    { "APPEND", CONF_APPEND },
    { "AUTH", CONF_AUTH },
    { "AUTO", CONF_AUTO },
    { "AUTOFLUSH", CONF_AUTOFLUSH },
    { "AUTOLABEL", CONF_AUTOLABEL },
    { "APPLICATION", CONF_APPLICATION },
    { "APPLICATION_TOOL", CONF_APPLICATION_TOOL },
    { "BEST", CONF_BEST },
    { "BLOCKSIZE", CONF_BLOCKSIZE },
    { "BUMPDAYS", CONF_BUMPDAYS },
    { "BUMPMULT", CONF_BUMPMULT },
    { "BUMPPERCENT", CONF_BUMPPERCENT },
    { "BUMPSIZE", CONF_BUMPSIZE },
    { "CALCSIZE", CONF_CALCSIZE },
    { "CHANGER", CONF_CHANGER },
    { "CHANGERDEV", CONF_CHANGERDEV },
    { "CHANGERFILE", CONF_CHANGERFILE },
    { "CHUNKSIZE", CONF_CHUNKSIZE },
    { "CLIENT", CONF_CLIENT },
    { "CLIENT_CUSTOM_COMPRESS", CONF_CLNTCOMPPROG },
    { "CLIENT_DECRYPT_OPTION", CONF_CLNT_DECRYPT_OPT },
    { "CLIENT_ENCRYPT", CONF_CLNT_ENCRYPT },
    { "CLIENT_NAME", CONF_CLIENT_NAME },
    { "CLIENT_USERNAME", CONF_CLIENT_USERNAME },
    { "COLUMNSPEC", CONF_COLUMNSPEC },
    { "COMMENT", CONF_COMMENT },
    { "COMPRATE", CONF_COMPRATE },
    { "COMPRESS", CONF_COMPRESS },
    { "CONNECT_TRIES", CONF_CONNECT_TRIES },
    { "CTIMEOUT", CONF_CTIMEOUT },
    { "CUSTOM", CONF_CUSTOM },
    { "DATA_PATH", CONF_DATA_PATH },
    { "DEBUG_DAYS"       , CONF_DEBUG_DAYS },
    { "DEBUG_AMANDAD"    , CONF_DEBUG_AMANDAD },
    { "DEBUG_RECOVERY"   , CONF_DEBUG_RECOVERY },
    { "DEBUG_AMIDXTAPED" , CONF_DEBUG_AMIDXTAPED },
    { "DEBUG_AMINDEXD"   , CONF_DEBUG_AMINDEXD },
    { "DEBUG_AMRECOVER"  , CONF_DEBUG_AMRECOVER },
    { "DEBUG_AUTH"       , CONF_DEBUG_AUTH },
    { "DEBUG_EVENT"      , CONF_DEBUG_EVENT },
    { "DEBUG_HOLDING"    , CONF_DEBUG_HOLDING },
    { "DEBUG_PROTOCOL"   , CONF_DEBUG_PROTOCOL },
    { "DEBUG_PLANNER"    , CONF_DEBUG_PLANNER },
    { "DEBUG_DRIVER"     , CONF_DEBUG_DRIVER },
    { "DEBUG_DUMPER"     , CONF_DEBUG_DUMPER },
    { "DEBUG_CHUNKER"    , CONF_DEBUG_CHUNKER },
    { "DEBUG_TAPER"      , CONF_DEBUG_TAPER },
    { "DEBUG_SELFCHECK"  , CONF_DEBUG_SELFCHECK },
    { "DEBUG_SENDSIZE"   , CONF_DEBUG_SENDSIZE },
    { "DEBUG_SENDBACKUP" , CONF_DEBUG_SENDBACKUP },
    { "DEFINE", CONF_DEFINE },
    { "DEVICE", CONF_DEVICE },
    { "DEVICE_PROPERTY", CONF_DEVICE_PROPERTY },
    { "DIRECTORY", CONF_DIRECTORY },
    { "DIRECTTCP", CONF_DIRECTTCP },
    { "DISK", CONF_DISK },
    { "DISKFILE", CONF_DISKFILE },
    { "DISPLAYUNIT", CONF_DISPLAYUNIT },
    { "DTIMEOUT", CONF_DTIMEOUT },
    { "DUMPCYCLE", CONF_DUMPCYCLE },
    { "DUMPORDER", CONF_DUMPORDER },
    { "DUMPTYPE", CONF_DUMPTYPE },
    { "DUMPUSER", CONF_DUMPUSER },
    { "DUMP_LIMIT", CONF_DUMP_LIMIT },
    { "EJECT_VOLUME", CONF_EJECT_VOLUME },
    { "EMPTY", CONF_EMPTY },
    { "ENCRYPT", CONF_ENCRYPT },
    { "ERROR", CONF_ERROR },
    { "ESTIMATE", CONF_ESTIMATE },
    { "ETIMEOUT", CONF_ETIMEOUT },
    { "EXCLUDE", CONF_EXCLUDE },
    { "EXCLUDE_FILE", CONF_EXCLUDE_FILE },
    { "EXCLUDE_LIST", CONF_EXCLUDE_LIST },
    { "EXECUTE_ON", CONF_EXECUTE_ON },
    { "EXECUTE_WHERE", CONF_EXECUTE_WHERE },
    { "FALLBACK_SPLITSIZE", CONF_FALLBACK_SPLITSIZE },
    { "FAST", CONF_FAST },
    { "FILE", CONF_EFILE },
    { "FILEMARK", CONF_FILEMARK },
    { "FIRST", CONF_FIRST },
    { "FIRSTFIT", CONF_FIRSTFIT },
    { "HANOI", CONF_HANOI },
    { "HIGH", CONF_HIGH },
    { "HOLDINGDISK", CONF_HOLDING },
    { "IGNORE", CONF_IGNORE },
    { "INCLUDE", CONF_INCLUDE },
    { "INCLUDEFILE", CONF_INCLUDEFILE },
    { "INCRONLY", CONF_INCRONLY },
    { "INDEX", CONF_INDEX },
    { "INDEXDIR", CONF_INDEXDIR },
    { "INFOFILE", CONF_INFOFILE },
    { "INPARALLEL", CONF_INPARALLEL },
    { "INTERACTIVITY", CONF_INTERACTIVITY },
    { "INTERFACE", CONF_INTERFACE },
    { "KENCRYPT", CONF_KENCRYPT },
    { "KRB5KEYTAB", CONF_KRB5KEYTAB },
    { "KRB5PRINCIPAL", CONF_KRB5PRINCIPAL },
    { "LABELSTR", CONF_LABELSTR },
    { "LABEL_NEW_TAPES", CONF_LABEL_NEW_TAPES },
    { "LARGEST", CONF_LARGEST },
    { "LARGESTFIT", CONF_LARGESTFIT },
    { "LAST", CONF_LAST },
    { "LBL_TEMPL", CONF_LBL_TEMPL },
    { "LENGTH", CONF_LENGTH },
    { "LIST", CONF_LIST },
    { "LOGDIR", CONF_LOGDIR },
    { "LOW", CONF_LOW },
    { "MAILER", CONF_MAILER },
    { "MAILTO", CONF_MAILTO },
    { "READBLOCKSIZE", CONF_READBLOCKSIZE },
    { "MAX_DLE_BY_VOLUME", CONF_MAX_DLE_BY_VOLUME },
    { "MAXDUMPS", CONF_MAXDUMPS },
    { "MAXDUMPSIZE", CONF_MAXDUMPSIZE },
    { "MAXPROMOTEDAY", CONF_MAXPROMOTEDAY },
    { "MAX_WARNINGS", CONF_MAX_WARNINGS },
    { "MEMORY", CONF_MEMORY },
    { "MEDIUM", CONF_MEDIUM },
    { "META_AUTOLABEL", CONF_META_AUTOLABEL },
    { "NETUSAGE", CONF_NETUSAGE },
    { "NEVER", CONF_NEVER },
    { "NOFULL", CONF_NOFULL },
    { "NOINC", CONF_NOINC },
    { "NONE", CONF_NONE },
    { "NON_AMANDA", CONF_NON_AMANDA },
    { "OPTIONAL", CONF_OPTIONAL },
    { "ORDER", CONF_ORDER },
    { "ORG", CONF_ORG },
    { "OTHER_CONFIG", CONF_OTHER_CONFIG },
    { "PART_CACHE_DIR", CONF_PART_CACHE_DIR },
    { "PART_CACHE_MAX_SIZE", CONF_PART_CACHE_MAX_SIZE },
    { "PART_CACHE_TYPE", CONF_PART_CACHE_TYPE },
    { "PART_SIZE", CONF_PART_SIZE },
    { "PLUGIN", CONF_PLUGIN },
    { "PRE_AMCHECK", CONF_PRE_AMCHECK },
    { "PRE_DLE_AMCHECK", CONF_PRE_DLE_AMCHECK },
    { "PRE_HOST_AMCHECK", CONF_PRE_HOST_AMCHECK },
    { "POST_AMCHECK", CONF_POST_AMCHECK },
    { "POST_DLE_AMCHECK", CONF_POST_DLE_AMCHECK },
    { "POST_HOST_AMCHECK", CONF_POST_HOST_AMCHECK },
    { "PRE_ESTIMATE", CONF_PRE_ESTIMATE },
    { "PRE_DLE_ESTIMATE", CONF_PRE_DLE_ESTIMATE },
    { "PRE_HOST_ESTIMATE", CONF_PRE_HOST_ESTIMATE },
    { "POST_ESTIMATE", CONF_POST_ESTIMATE },
    { "POST_DLE_ESTIMATE", CONF_POST_DLE_ESTIMATE },
    { "POST_HOST_ESTIMATE", CONF_POST_HOST_ESTIMATE },
    { "POST_BACKUP", CONF_POST_BACKUP },
    { "POST_DLE_BACKUP", CONF_POST_DLE_BACKUP },
    { "POST_HOST_BACKUP", CONF_POST_HOST_BACKUP },
    { "PRE_BACKUP", CONF_PRE_BACKUP },
    { "PRE_DLE_BACKUP", CONF_PRE_DLE_BACKUP },
    { "PRE_HOST_BACKUP", CONF_PRE_HOST_BACKUP },
    { "PRE_RECOVER", CONF_PRE_RECOVER },
    { "POST_RECOVER", CONF_POST_RECOVER },
    { "PRE_LEVEL_RECOVER", CONF_PRE_LEVEL_RECOVER },
    { "POST_LEVEL_RECOVER", CONF_POST_LEVEL_RECOVER },
    { "INTER_LEVEL_RECOVER", CONF_INTER_LEVEL_RECOVER },
    { "PRINTER", CONF_PRINTER },
    { "PRIORITY", CONF_PRIORITY },
    { "PROGRAM", CONF_PROGRAM },
    { "PROPERTY", CONF_PROPERTY },
    { "RECORD", CONF_RECORD },
    { "RECOVERY_LIMIT", CONF_RECOVERY_LIMIT },
    { "REP_TRIES", CONF_REP_TRIES },
    { "REQ_TRIES", CONF_REQ_TRIES },
    { "REQUIRED", CONF_REQUIRED },
    { "RESERVE", CONF_RESERVE },
    { "RESERVED_UDP_PORT", CONF_RESERVED_UDP_PORT },
    { "RESERVED_TCP_PORT", CONF_RESERVED_TCP_PORT },
    { "RUNSPERCYCLE", CONF_RUNSPERCYCLE },
    { "RUNTAPES", CONF_RUNTAPES },
    { "SAME_HOST", CONF_SAME_HOST },
    { "SCRIPT", CONF_SCRIPT },
    { "SCRIPT_TOOL", CONF_SCRIPT_TOOL },
    { "SEND_AMREPORT_ON", CONF_SEND_AMREPORT_ON },
    { "CLIENT_PORT", CONF_CLIENT_PORT },
    { "SERVER", CONF_SERVER },
    { "SERVER_CUSTOM_COMPRESS", CONF_SRVCOMPPROG },
    { "SERVER_DECRYPT_OPTION", CONF_SRV_DECRYPT_OPT },
    { "SERVER_ENCRYPT", CONF_SRV_ENCRYPT },
    { "SKIP", CONF_SKIP },
    { "SKIP_FULL", CONF_SKIP_FULL },
    { "SKIP_INCR", CONF_SKIP_INCR },
    { "SINGLE_EXECUTION", CONF_SINGLE_EXECUTION },
    { "SMALLEST", CONF_SMALLEST },
    { "SPEED", CONF_SPEED },
    { "SPLIT_DISKBUFFER", CONF_SPLIT_DISKBUFFER },
    { "SSH_KEYS", CONF_SSH_KEYS },
    { "STANDARD", CONF_STANDARD },
    { "STARTTIME", CONF_STARTTIME },
    { "STRANGE", CONF_STRANGE },
    { "STRATEGY", CONF_STRATEGY },
    { "DEVICE_OUTPUT_BUFFER_SIZE", CONF_DEVICE_OUTPUT_BUFFER_SIZE },
    { "TAPECYCLE", CONF_TAPECYCLE },
    { "TAPEDEV", CONF_TAPEDEV },
    { "TAPELIST", CONF_TAPELIST },
    { "TAPERALGO", CONF_TAPERALGO },
    { "TAPERSCAN", CONF_TAPERSCAN },
    { "TAPER_PARALLEL_WRITE", CONF_TAPER_PARALLEL_WRITE },
    { "FLUSH_THRESHOLD_DUMPED", CONF_FLUSH_THRESHOLD_DUMPED },
    { "FLUSH_THRESHOLD_SCHEDULED", CONF_FLUSH_THRESHOLD_SCHEDULED },
    { "TAPERFLUSH", CONF_TAPERFLUSH },
    { "TAPETYPE", CONF_TAPETYPE },
    { "TAPE_SPLITSIZE", CONF_TAPE_SPLITSIZE },
    { "TMPDIR", CONF_TMPDIR },
    { "TPCHANGER", CONF_TPCHANGER },
    { "UNRESERVED_TCP_PORT", CONF_UNRESERVED_TCP_PORT },
    { "USE", CONF_USE },
    { "USETIMESTAMPS", CONF_USETIMESTAMPS },
    { "VOLUME_ERROR", CONF_VOLUME_ERROR },
    { NULL, CONF_IDENT },
    { NULL, CONF_UNKNOWN }
};

/* A keyword table for recognizing unit suffixes.  No distinction is made for kinds
 * of suffixes: 1024 weeks = 7 k. */
keytab_t numb_keytable[] = {
    { "B", CONF_MULT1 },
    { "BPS", CONF_MULT1 },
    { "BYTE", CONF_MULT1 },
    { "BYTES", CONF_MULT1 },
    { "DAY", CONF_MULT1 },
    { "DAYS", CONF_MULT1 },
    { "INF", CONF_AMINFINITY },
    { "K", CONF_MULT1K },
    { "KB", CONF_MULT1K },
    { "KBPS", CONF_MULT1K },
    { "KBYTE", CONF_MULT1K },
    { "KBYTES", CONF_MULT1K },
    { "KILOBYTE", CONF_MULT1K },
    { "KILOBYTES", CONF_MULT1K },
    { "KPS", CONF_MULT1K },
    { "M", CONF_MULT1M },
    { "MB", CONF_MULT1M },
    { "MBPS", CONF_MULT1M },
    { "MBYTE", CONF_MULT1M },
    { "MBYTES", CONF_MULT1M },
    { "MEG", CONF_MULT1M },
    { "MEGABYTE", CONF_MULT1M },
    { "MEGABYTES", CONF_MULT1M },
    { "G", CONF_MULT1G },
    { "GB", CONF_MULT1G },
    { "GBPS", CONF_MULT1G },
    { "GBYTE", CONF_MULT1G },
    { "GBYTES", CONF_MULT1G },
    { "GIG", CONF_MULT1G },
    { "GIGABYTE", CONF_MULT1G },
    { "GIGABYTES", CONF_MULT1G },
    { "T", CONF_MULT1T },
    { "TB", CONF_MULT1T },
    { "TBPS", CONF_MULT1T },
    { "TBYTE", CONF_MULT1T },
    { "TBYTES", CONF_MULT1T },
    { "TERA", CONF_MULT1T },
    { "TERABYTE", CONF_MULT1T },
    { "TERABYTES", CONF_MULT1T },
    { "MPS", CONF_MULT1M },
    { "TAPE", CONF_MULT1 },
    { "TAPES", CONF_MULT1 },
    { "WEEK", CONF_MULT7 },
    { "WEEKS", CONF_MULT7 },
    { NULL, CONF_IDENT }
};

/* Boolean keywords -- all the ways to say "true" and "false" in amanda.conf */
keytab_t bool_keytable[] = {
    { "Y", CONF_ATRUE },
    { "YES", CONF_ATRUE },
    { "T", CONF_ATRUE },
    { "TRUE", CONF_ATRUE },
    { "ON", CONF_ATRUE },
    { "N", CONF_AFALSE },
    { "NO", CONF_AFALSE },
    { "F", CONF_AFALSE },
    { "FALSE", CONF_AFALSE },
    { "OFF", CONF_AFALSE },
    { NULL, CONF_IDENT }
};

/* no_yes_all keywords -- all the ways to say "true" and "false" in amanda.conf */
keytab_t no_yes_all_keytable[] = {
    { "Y", CONF_ATRUE },
    { "YES", CONF_ATRUE },
    { "T", CONF_ATRUE },
    { "TRUE", CONF_ATRUE },
    { "ON", CONF_ATRUE },
    { "N", CONF_AFALSE },
    { "NO", CONF_AFALSE },
    { "F", CONF_AFALSE },
    { "FALSE", CONF_AFALSE },
    { "OFF", CONF_AFALSE },
    { "ALL", CONF_ALL },
    { NULL, CONF_IDENT }
};

/* Now, the parser tables for client and server global parameters, and for
 * each of the server subsections */
conf_var_t client_var [] = {
   { CONF_CONF               , CONFTYPE_STR     , read_str     , CNF_CONF               , NULL },
   { CONF_AMDUMP_SERVER      , CONFTYPE_STR     , read_str     , CNF_AMDUMP_SERVER      , NULL },
   { CONF_INDEX_SERVER       , CONFTYPE_STR     , read_str     , CNF_INDEX_SERVER       , NULL },
   { CONF_TAPE_SERVER        , CONFTYPE_STR     , read_str     , CNF_TAPE_SERVER        , NULL },
   { CONF_TAPEDEV            , CONFTYPE_STR     , read_str     , CNF_TAPEDEV            , NULL },
   { CONF_AUTH               , CONFTYPE_STR     , read_str     , CNF_AUTH               , NULL },
   { CONF_SSH_KEYS           , CONFTYPE_STR     , read_str     , CNF_SSH_KEYS           , NULL },
   { CONF_AMANDAD_PATH       , CONFTYPE_STR     , read_str     , CNF_AMANDAD_PATH       , NULL },
   { CONF_CLIENT_USERNAME    , CONFTYPE_STR     , read_str     , CNF_CLIENT_USERNAME    , NULL },
   { CONF_CLIENT_PORT        , CONFTYPE_STR     , read_int_or_str, CNF_CLIENT_PORT      , NULL },
   { CONF_GNUTAR_LIST_DIR    , CONFTYPE_STR     , read_str     , CNF_GNUTAR_LIST_DIR    , NULL },
   { CONF_AMANDATES          , CONFTYPE_STR     , read_str     , CNF_AMANDATES          , NULL },
   { CONF_MAILER             , CONFTYPE_STR     , read_str     , CNF_MAILER             , NULL },
   { CONF_KRB5KEYTAB         , CONFTYPE_STR     , read_str     , CNF_KRB5KEYTAB         , NULL },
   { CONF_KRB5PRINCIPAL      , CONFTYPE_STR     , read_str     , CNF_KRB5PRINCIPAL      , NULL },
   { CONF_CONNECT_TRIES      , CONFTYPE_INT     , read_int     , CNF_CONNECT_TRIES      , validate_positive },
   { CONF_REP_TRIES          , CONFTYPE_INT     , read_int     , CNF_REP_TRIES          , validate_positive },
   { CONF_REQ_TRIES          , CONFTYPE_INT     , read_int     , CNF_REQ_TRIES          , validate_positive },
   { CONF_DEBUG_DAYS         , CONFTYPE_INT     , read_int     , CNF_DEBUG_DAYS         , NULL },
   { CONF_DEBUG_AMANDAD      , CONFTYPE_INT     , read_int     , CNF_DEBUG_AMANDAD      , validate_debug },
   { CONF_DEBUG_RECOVERY     , CONFTYPE_INT     , read_int     , CNF_DEBUG_RECOVERY     , validate_debug },
   { CONF_DEBUG_AMIDXTAPED   , CONFTYPE_INT     , read_int     , CNF_DEBUG_AMIDXTAPED   , validate_debug },
   { CONF_DEBUG_AMINDEXD     , CONFTYPE_INT     , read_int     , CNF_DEBUG_AMINDEXD     , validate_debug },
   { CONF_DEBUG_AMRECOVER    , CONFTYPE_INT     , read_int     , CNF_DEBUG_AMRECOVER    , validate_debug },
   { CONF_DEBUG_AUTH         , CONFTYPE_INT     , read_int     , CNF_DEBUG_AUTH         , validate_debug },
   { CONF_DEBUG_EVENT        , CONFTYPE_INT     , read_int     , CNF_DEBUG_EVENT        , validate_debug },
   { CONF_DEBUG_HOLDING      , CONFTYPE_INT     , read_int     , CNF_DEBUG_HOLDING      , validate_debug },
   { CONF_DEBUG_PROTOCOL     , CONFTYPE_INT     , read_int     , CNF_DEBUG_PROTOCOL     , validate_debug },
   { CONF_DEBUG_PLANNER      , CONFTYPE_INT     , read_int     , CNF_DEBUG_PLANNER      , validate_debug },
   { CONF_DEBUG_DRIVER       , CONFTYPE_INT     , read_int     , CNF_DEBUG_DRIVER       , validate_debug },
   { CONF_DEBUG_DUMPER       , CONFTYPE_INT     , read_int     , CNF_DEBUG_DUMPER       , validate_debug },
   { CONF_DEBUG_CHUNKER      , CONFTYPE_INT     , read_int     , CNF_DEBUG_CHUNKER      , validate_debug },
   { CONF_DEBUG_TAPER        , CONFTYPE_INT     , read_int     , CNF_DEBUG_TAPER        , validate_debug },
   { CONF_DEBUG_SELFCHECK    , CONFTYPE_INT     , read_int     , CNF_DEBUG_SELFCHECK    , validate_debug },
   { CONF_DEBUG_SENDSIZE     , CONFTYPE_INT     , read_int     , CNF_DEBUG_SENDSIZE     , validate_debug },
   { CONF_DEBUG_SENDBACKUP   , CONFTYPE_INT     , read_int     , CNF_DEBUG_SENDBACKUP   , validate_debug },
   { CONF_RESERVED_UDP_PORT  , CONFTYPE_INTRANGE, read_intrange, CNF_RESERVED_UDP_PORT  , validate_reserved_port_range },
   { CONF_RESERVED_TCP_PORT  , CONFTYPE_INTRANGE, read_intrange, CNF_RESERVED_TCP_PORT  , validate_reserved_port_range },
   { CONF_UNRESERVED_TCP_PORT, CONFTYPE_INTRANGE, read_intrange, CNF_UNRESERVED_TCP_PORT, validate_unreserved_port_range },
   { CONF_PROPERTY           , CONFTYPE_PROPLIST, read_property, CNF_PROPERTY           , NULL },
   { CONF_APPLICATION        , CONFTYPE_STR     , read_dapplication, DUMPTYPE_APPLICATION, NULL },
   { CONF_SCRIPT             , CONFTYPE_STR     , read_dpp_script, DUMPTYPE_SCRIPTLIST, NULL },
   { CONF_UNKNOWN            , CONFTYPE_INT     , NULL         , CNF_CNF                , NULL }
};

conf_var_t server_var [] = {
   { CONF_ORG                  , CONFTYPE_STR      , read_str         , CNF_ORG                  , NULL },
   { CONF_MAILTO               , CONFTYPE_STR      , read_str         , CNF_MAILTO               , NULL },
   { CONF_DUMPUSER             , CONFTYPE_STR      , read_str         , CNF_DUMPUSER             , NULL },
   { CONF_PRINTER              , CONFTYPE_STR      , read_str         , CNF_PRINTER              , NULL },
   { CONF_MAILER               , CONFTYPE_STR      , read_str         , CNF_MAILER               , NULL },
   { CONF_TAPEDEV              , CONFTYPE_STR      , read_str         , CNF_TAPEDEV              , NULL },
   { CONF_DEVICE_PROPERTY      , CONFTYPE_PROPLIST , read_property    , CNF_DEVICE_PROPERTY      , NULL },
   { CONF_PROPERTY             , CONFTYPE_PROPLIST , read_property    , CNF_PROPERTY             , NULL },
   { CONF_TPCHANGER            , CONFTYPE_STR      , read_str         , CNF_TPCHANGER            , NULL },
   { CONF_CHANGERDEV           , CONFTYPE_STR      , read_str         , CNF_CHANGERDEV           , NULL },
   { CONF_CHANGERFILE          , CONFTYPE_STR      , read_str         , CNF_CHANGERFILE          , NULL },
   { CONF_LABELSTR             , CONFTYPE_STR      , read_str         , CNF_LABELSTR             , NULL },
   { CONF_TAPELIST             , CONFTYPE_STR      , read_str         , CNF_TAPELIST             , NULL },
   { CONF_DISKFILE             , CONFTYPE_STR      , read_str         , CNF_DISKFILE             , NULL },
   { CONF_INFOFILE             , CONFTYPE_STR      , read_str         , CNF_INFOFILE             , NULL },
   { CONF_LOGDIR               , CONFTYPE_STR      , read_str         , CNF_LOGDIR               , NULL },
   { CONF_INDEXDIR             , CONFTYPE_STR      , read_str         , CNF_INDEXDIR             , NULL },
   { CONF_TAPETYPE             , CONFTYPE_IDENT    , read_ident       , CNF_TAPETYPE             , NULL },
   { CONF_HOLDING              , CONFTYPE_IDENTLIST, read_holdingdisk , CNF_HOLDINGDISK          , NULL },
   { CONF_DUMPCYCLE            , CONFTYPE_INT      , read_int         , CNF_DUMPCYCLE            , validate_nonnegative },
   { CONF_RUNSPERCYCLE         , CONFTYPE_INT      , read_int         , CNF_RUNSPERCYCLE         , validate_runspercycle },
   { CONF_RUNTAPES             , CONFTYPE_INT      , read_int         , CNF_RUNTAPES             , validate_nonnegative },
   { CONF_TAPECYCLE            , CONFTYPE_INT      , read_int         , CNF_TAPECYCLE            , validate_positive },
   { CONF_BUMPDAYS             , CONFTYPE_INT      , read_int         , CNF_BUMPDAYS             , validate_positive },
   { CONF_BUMPSIZE             , CONFTYPE_INT64    , read_int64       , CNF_BUMPSIZE             , validate_positive },
   { CONF_BUMPPERCENT          , CONFTYPE_INT      , read_int         , CNF_BUMPPERCENT          , validate_bumppercent },
   { CONF_BUMPMULT             , CONFTYPE_REAL     , read_real        , CNF_BUMPMULT             , validate_bumpmult },
   { CONF_NETUSAGE             , CONFTYPE_INT      , read_int         , CNF_NETUSAGE             , validate_positive },
   { CONF_INPARALLEL           , CONFTYPE_INT      , read_int         , CNF_INPARALLEL           , validate_inparallel },
   { CONF_DUMPORDER            , CONFTYPE_STR      , read_str         , CNF_DUMPORDER            , NULL },
   { CONF_MAXDUMPS             , CONFTYPE_INT      , read_int         , CNF_MAXDUMPS             , validate_positive },
   { CONF_MAX_DLE_BY_VOLUME    , CONFTYPE_INT      , read_int         , CNF_MAX_DLE_BY_VOLUME    , validate_positive },
   { CONF_ETIMEOUT             , CONFTYPE_INT      , read_int         , CNF_ETIMEOUT             , validate_non_zero },
   { CONF_DTIMEOUT             , CONFTYPE_INT      , read_int         , CNF_DTIMEOUT             , validate_positive },
   { CONF_CTIMEOUT             , CONFTYPE_INT      , read_int         , CNF_CTIMEOUT             , validate_positive },
   { CONF_DEVICE_OUTPUT_BUFFER_SIZE, CONFTYPE_SIZE , read_size        , CNF_DEVICE_OUTPUT_BUFFER_SIZE, validate_positive },
   { CONF_COLUMNSPEC           , CONFTYPE_STR      , read_str         , CNF_COLUMNSPEC           , NULL },
   { CONF_TAPERALGO            , CONFTYPE_TAPERALGO, read_taperalgo   , CNF_TAPERALGO            , NULL },
   { CONF_TAPER_PARALLEL_WRITE , CONFTYPE_INT      , read_int         , CNF_TAPER_PARALLEL_WRITE , NULL },
   { CONF_SEND_AMREPORT_ON     , CONFTYPE_SEND_AMREPORT_ON, read_send_amreport_on, CNF_SEND_AMREPORT_ON       , NULL },
   { CONF_FLUSH_THRESHOLD_DUMPED, CONFTYPE_INT     , read_int         , CNF_FLUSH_THRESHOLD_DUMPED, validate_nonnegative },
   { CONF_FLUSH_THRESHOLD_SCHEDULED, CONFTYPE_INT  , read_int         , CNF_FLUSH_THRESHOLD_SCHEDULED, validate_nonnegative },
   { CONF_TAPERFLUSH           , CONFTYPE_INT      , read_int         , CNF_TAPERFLUSH           , validate_nonnegative },
   { CONF_DISPLAYUNIT          , CONFTYPE_STR      , read_str         , CNF_DISPLAYUNIT          , validate_displayunit },
   { CONF_AUTOFLUSH            , CONFTYPE_NO_YES_ALL,read_no_yes_all  , CNF_AUTOFLUSH            , NULL },
   { CONF_RESERVE              , CONFTYPE_INT      , read_int         , CNF_RESERVE              , validate_reserve },
   { CONF_MAXDUMPSIZE          , CONFTYPE_INT64    , read_int64       , CNF_MAXDUMPSIZE          , NULL },
   { CONF_KRB5KEYTAB           , CONFTYPE_STR      , read_str         , CNF_KRB5KEYTAB           , NULL },
   { CONF_KRB5PRINCIPAL        , CONFTYPE_STR      , read_str         , CNF_KRB5PRINCIPAL        , NULL },
   { CONF_LABEL_NEW_TAPES      , CONFTYPE_STR      , read_str         , CNF_LABEL_NEW_TAPES      , NULL },
   { CONF_AUTOLABEL            , CONFTYPE_AUTOLABEL, read_autolabel   , CNF_AUTOLABEL            , NULL },
   { CONF_META_AUTOLABEL       , CONFTYPE_STR      , read_str         , CNF_META_AUTOLABEL       , NULL },
   { CONF_EJECT_VOLUME         , CONFTYPE_BOOLEAN  , read_bool        , CNF_EJECT_VOLUME         , NULL },
   { CONF_TMPDIR               , CONFTYPE_STR      , read_str         , CNF_TMPDIR               , validate_tmpdir },
   { CONF_USETIMESTAMPS        , CONFTYPE_BOOLEAN  , read_bool        , CNF_USETIMESTAMPS        , NULL },
   { CONF_AMRECOVER_DO_FSF     , CONFTYPE_BOOLEAN  , read_bool        , CNF_AMRECOVER_DO_FSF     , NULL },
   { CONF_AMRECOVER_CHANGER    , CONFTYPE_STR      , read_str         , CNF_AMRECOVER_CHANGER    , NULL },
   { CONF_AMRECOVER_CHECK_LABEL, CONFTYPE_BOOLEAN  , read_bool        , CNF_AMRECOVER_CHECK_LABEL, NULL },
   { CONF_CONNECT_TRIES        , CONFTYPE_INT      , read_int         , CNF_CONNECT_TRIES        , validate_positive },
   { CONF_REP_TRIES            , CONFTYPE_INT      , read_int         , CNF_REP_TRIES            , validate_positive },
   { CONF_REQ_TRIES            , CONFTYPE_INT      , read_int         , CNF_REQ_TRIES            , validate_positive },
   { CONF_DEBUG_DAYS           , CONFTYPE_INT      , read_int         , CNF_DEBUG_DAYS           , NULL },
   { CONF_DEBUG_AMANDAD        , CONFTYPE_INT      , read_int         , CNF_DEBUG_AMANDAD        , validate_debug },
   { CONF_DEBUG_RECOVERY       , CONFTYPE_INT      , read_int         , CNF_DEBUG_RECOVERY       , validate_debug },
   { CONF_DEBUG_AMIDXTAPED     , CONFTYPE_INT      , read_int         , CNF_DEBUG_AMIDXTAPED     , validate_debug },
   { CONF_DEBUG_AMINDEXD       , CONFTYPE_INT      , read_int         , CNF_DEBUG_AMINDEXD       , validate_debug },
   { CONF_DEBUG_AMRECOVER      , CONFTYPE_INT      , read_int         , CNF_DEBUG_AMRECOVER      , validate_debug },
   { CONF_DEBUG_AUTH           , CONFTYPE_INT      , read_int         , CNF_DEBUG_AUTH           , validate_debug },
   { CONF_DEBUG_EVENT          , CONFTYPE_INT      , read_int         , CNF_DEBUG_EVENT          , validate_debug },
   { CONF_DEBUG_HOLDING        , CONFTYPE_INT      , read_int         , CNF_DEBUG_HOLDING        , validate_debug },
   { CONF_DEBUG_PROTOCOL       , CONFTYPE_INT      , read_int         , CNF_DEBUG_PROTOCOL       , validate_debug },
   { CONF_DEBUG_PLANNER        , CONFTYPE_INT      , read_int         , CNF_DEBUG_PLANNER        , validate_debug },
   { CONF_DEBUG_DRIVER         , CONFTYPE_INT      , read_int         , CNF_DEBUG_DRIVER         , validate_debug },
   { CONF_DEBUG_DUMPER         , CONFTYPE_INT      , read_int         , CNF_DEBUG_DUMPER         , validate_debug },
   { CONF_DEBUG_CHUNKER        , CONFTYPE_INT      , read_int         , CNF_DEBUG_CHUNKER        , validate_debug },
   { CONF_DEBUG_TAPER          , CONFTYPE_INT      , read_int         , CNF_DEBUG_TAPER          , validate_debug },
   { CONF_DEBUG_SELFCHECK      , CONFTYPE_INT      , read_int         , CNF_DEBUG_SELFCHECK      , validate_debug },
   { CONF_DEBUG_SENDSIZE       , CONFTYPE_INT      , read_int         , CNF_DEBUG_SENDSIZE       , validate_debug },
   { CONF_DEBUG_SENDBACKUP     , CONFTYPE_INT      , read_int         , CNF_DEBUG_SENDBACKUP     , validate_debug },
   { CONF_RESERVED_UDP_PORT    , CONFTYPE_INTRANGE , read_intrange    , CNF_RESERVED_UDP_PORT    , validate_reserved_port_range },
   { CONF_RESERVED_TCP_PORT    , CONFTYPE_INTRANGE , read_intrange    , CNF_RESERVED_TCP_PORT    , validate_reserved_port_range },
   { CONF_UNRESERVED_TCP_PORT  , CONFTYPE_INTRANGE , read_intrange    , CNF_UNRESERVED_TCP_PORT  , validate_unreserved_port_range },
   { CONF_RECOVERY_LIMIT       , CONFTYPE_HOST_LIMIT, read_host_limit , CNF_RECOVERY_LIMIT       , NULL },
   { CONF_INTERACTIVITY        , CONFTYPE_STR      , read_dinteractivity, CNF_INTERACTIVITY      , NULL },
   { CONF_TAPERSCAN            , CONFTYPE_STR      , read_dtaperscan  , CNF_TAPERSCAN            , NULL },
   { CONF_UNKNOWN              , CONFTYPE_INT      , NULL             , CNF_CNF                  , NULL }
};

conf_var_t tapetype_var [] = {
   { CONF_COMMENT              , CONFTYPE_STR     , read_str          , TAPETYPE_COMMENT         , NULL },
   { CONF_LBL_TEMPL            , CONFTYPE_STR     , read_str          , TAPETYPE_LBL_TEMPL       , NULL },
   { CONF_BLOCKSIZE            , CONFTYPE_SIZE    , read_size         , TAPETYPE_BLOCKSIZE       , validate_blocksize },
   { CONF_READBLOCKSIZE        , CONFTYPE_SIZE    , read_size         , TAPETYPE_READBLOCKSIZE   , validate_blocksize },
   { CONF_LENGTH               , CONFTYPE_INT64   , read_int64        , TAPETYPE_LENGTH          , validate_nonnegative },
   { CONF_FILEMARK             , CONFTYPE_INT64   , read_int64        , TAPETYPE_FILEMARK        , NULL },
   { CONF_SPEED                , CONFTYPE_INT     , read_int          , TAPETYPE_SPEED           , validate_nonnegative },
   { CONF_PART_SIZE            , CONFTYPE_INT64    , read_int64       , TAPETYPE_PART_SIZE       , validate_nonnegative },
   { CONF_PART_CACHE_TYPE      , CONFTYPE_PART_CACHE_TYPE, read_part_cache_type, TAPETYPE_PART_CACHE_TYPE, NULL },
   { CONF_PART_CACHE_DIR       , CONFTYPE_STR      , read_str         , TAPETYPE_PART_CACHE_DIR	 , NULL },
   { CONF_PART_CACHE_MAX_SIZE  , CONFTYPE_INT64    , read_int64       , TAPETYPE_PART_CACHE_MAX_SIZE, validate_nonnegative },
   { CONF_UNKNOWN              , CONFTYPE_INT     , NULL              , TAPETYPE_TAPETYPE        , NULL }
};

conf_var_t dumptype_var [] = {
   { CONF_COMMENT           , CONFTYPE_STR      , read_str      , DUMPTYPE_COMMENT           , NULL },
   { CONF_AUTH              , CONFTYPE_STR      , read_str      , DUMPTYPE_AUTH              , NULL },
   { CONF_BUMPDAYS          , CONFTYPE_INT      , read_int      , DUMPTYPE_BUMPDAYS          , NULL },
   { CONF_BUMPMULT          , CONFTYPE_REAL     , read_real     , DUMPTYPE_BUMPMULT          , NULL },
   { CONF_BUMPSIZE          , CONFTYPE_INT64    , read_int64    , DUMPTYPE_BUMPSIZE          , NULL },
   { CONF_BUMPPERCENT       , CONFTYPE_INT      , read_int      , DUMPTYPE_BUMPPERCENT       , NULL },
   { CONF_COMPRATE          , CONFTYPE_REAL     , read_rate     , DUMPTYPE_COMPRATE          , NULL },
   { CONF_COMPRESS          , CONFTYPE_INT      , read_compress , DUMPTYPE_COMPRESS          , NULL },
   { CONF_ENCRYPT           , CONFTYPE_INT      , read_encrypt  , DUMPTYPE_ENCRYPT           , NULL },
   { CONF_DUMPCYCLE         , CONFTYPE_INT      , read_int      , DUMPTYPE_DUMPCYCLE         , validate_nonnegative },
   { CONF_EXCLUDE           , CONFTYPE_EXINCLUDE, read_exinclude, DUMPTYPE_EXCLUDE           , NULL },
   { CONF_INCLUDE           , CONFTYPE_EXINCLUDE, read_exinclude, DUMPTYPE_INCLUDE           , NULL },
   { CONF_IGNORE            , CONFTYPE_BOOLEAN  , read_bool     , DUMPTYPE_IGNORE            , NULL },
   { CONF_HOLDING           , CONFTYPE_HOLDING  , read_holding  , DUMPTYPE_HOLDINGDISK       , NULL },
   { CONF_INDEX             , CONFTYPE_BOOLEAN  , read_bool     , DUMPTYPE_INDEX             , NULL },
   { CONF_KENCRYPT          , CONFTYPE_BOOLEAN  , read_bool     , DUMPTYPE_KENCRYPT          , NULL },
   { CONF_MAXDUMPS          , CONFTYPE_INT      , read_int      , DUMPTYPE_MAXDUMPS          , validate_positive },
   { CONF_MAXPROMOTEDAY     , CONFTYPE_INT      , read_int      , DUMPTYPE_MAXPROMOTEDAY     , validate_nonnegative },
   { CONF_PRIORITY          , CONFTYPE_PRIORITY , read_priority , DUMPTYPE_PRIORITY          , NULL },
   { CONF_PROGRAM           , CONFTYPE_STR      , read_str      , DUMPTYPE_PROGRAM           , validate_program },
   { CONF_PROPERTY          , CONFTYPE_PROPLIST , read_property , DUMPTYPE_PROPERTY          , NULL },
   { CONF_RECORD            , CONFTYPE_BOOLEAN  , read_bool     , DUMPTYPE_RECORD            , NULL },
   { CONF_SKIP_FULL         , CONFTYPE_BOOLEAN  , read_bool     , DUMPTYPE_SKIP_FULL         , NULL },
   { CONF_SKIP_INCR         , CONFTYPE_BOOLEAN  , read_bool     , DUMPTYPE_SKIP_INCR         , NULL },
   { CONF_STARTTIME         , CONFTYPE_TIME     , read_time     , DUMPTYPE_STARTTIME         , NULL },
   { CONF_STRATEGY          , CONFTYPE_INT      , read_strategy , DUMPTYPE_STRATEGY          , NULL },
   { CONF_TAPE_SPLITSIZE    , CONFTYPE_INT64    , read_int64    , DUMPTYPE_TAPE_SPLITSIZE    , validate_nonnegative },
   { CONF_SPLIT_DISKBUFFER  , CONFTYPE_STR      , read_str      , DUMPTYPE_SPLIT_DISKBUFFER  , NULL },
   { CONF_ESTIMATE          , CONFTYPE_ESTIMATELIST, read_estimatelist , DUMPTYPE_ESTIMATELIST  , NULL },
   { CONF_SRV_ENCRYPT       , CONFTYPE_STR      , read_str      , DUMPTYPE_SRV_ENCRYPT       , NULL },
   { CONF_CLNT_ENCRYPT      , CONFTYPE_STR      , read_str      , DUMPTYPE_CLNT_ENCRYPT      , NULL },
   { CONF_AMANDAD_PATH      , CONFTYPE_STR      , read_str      , DUMPTYPE_AMANDAD_PATH      , NULL },
   { CONF_CLIENT_USERNAME   , CONFTYPE_STR      , read_str      , DUMPTYPE_CLIENT_USERNAME   , NULL },
   { CONF_CLIENT_PORT       , CONFTYPE_STR      , read_int_or_str, DUMPTYPE_CLIENT_PORT      , NULL },
   { CONF_SSH_KEYS          , CONFTYPE_STR      , read_str      , DUMPTYPE_SSH_KEYS          , NULL },
   { CONF_SRVCOMPPROG       , CONFTYPE_STR      , read_str      , DUMPTYPE_SRVCOMPPROG       , NULL },
   { CONF_CLNTCOMPPROG      , CONFTYPE_STR      , read_str      , DUMPTYPE_CLNTCOMPPROG      , NULL },
   { CONF_FALLBACK_SPLITSIZE, CONFTYPE_INT64    , read_int64    , DUMPTYPE_FALLBACK_SPLITSIZE, NULL },
   { CONF_SRV_DECRYPT_OPT   , CONFTYPE_STR      , read_str      , DUMPTYPE_SRV_DECRYPT_OPT   , NULL },
   { CONF_CLNT_DECRYPT_OPT  , CONFTYPE_STR      , read_str      , DUMPTYPE_CLNT_DECRYPT_OPT  , NULL },
   { CONF_APPLICATION       , CONFTYPE_STR      , read_dapplication, DUMPTYPE_APPLICATION    , NULL },
   { CONF_SCRIPT            , CONFTYPE_STR      , read_dpp_script, DUMPTYPE_SCRIPTLIST       , NULL },
   { CONF_DATA_PATH         , CONFTYPE_DATA_PATH, read_data_path, DUMPTYPE_DATA_PATH         , NULL },
   { CONF_ALLOW_SPLIT       , CONFTYPE_BOOLEAN  , read_bool    , DUMPTYPE_ALLOW_SPLIT        , NULL },
   { CONF_MAX_WARNINGS      , CONFTYPE_INT      , read_int     , DUMPTYPE_MAX_WARNINGS       , validate_nonnegative },
   { CONF_RECOVERY_LIMIT    , CONFTYPE_HOST_LIMIT, read_host_limit, DUMPTYPE_RECOVERY_LIMIT  , NULL },
   { CONF_DUMP_LIMIT        , CONFTYPE_HOST_LIMIT, read_host_limit, DUMPTYPE_DUMP_LIMIT      , validate_dump_limit },
   { CONF_UNKNOWN           , CONFTYPE_INT      , NULL          , DUMPTYPE_DUMPTYPE          , NULL }
};

conf_var_t holding_var [] = {
   { CONF_DIRECTORY, CONFTYPE_STR   , read_str   , HOLDING_DISKDIR  , NULL },
   { CONF_COMMENT  , CONFTYPE_STR   , read_str   , HOLDING_COMMENT  , NULL },
   { CONF_USE      , CONFTYPE_INT64 , read_int64 , HOLDING_DISKSIZE , validate_use },
   { CONF_CHUNKSIZE, CONFTYPE_INT64 , read_int64 , HOLDING_CHUNKSIZE, validate_chunksize },
   { CONF_UNKNOWN  , CONFTYPE_INT   , NULL       , HOLDING_HOLDING  , NULL }
};

conf_var_t interface_var [] = {
   { CONF_COMMENT, CONFTYPE_STR   , read_str   , INTER_COMMENT , NULL },
   { CONF_USE    , CONFTYPE_INT   , read_int   , INTER_MAXUSAGE, validate_positive },
   { CONF_UNKNOWN, CONFTYPE_INT   , NULL       , INTER_INTER   , NULL }
};


conf_var_t application_var [] = {
   { CONF_COMMENT  , CONFTYPE_STR     , read_str     , APPLICATION_COMMENT    , NULL },
   { CONF_PLUGIN   , CONFTYPE_STR     , read_str     , APPLICATION_PLUGIN     , NULL },
   { CONF_PROPERTY , CONFTYPE_PROPLIST, read_property, APPLICATION_PROPERTY   , NULL },
   { CONF_CLIENT_NAME, CONFTYPE_STR   , read_str     , APPLICATION_CLIENT_NAME, NULL },
   { CONF_UNKNOWN  , CONFTYPE_INT     , NULL         , APPLICATION_APPLICATION, NULL }
};

conf_var_t pp_script_var [] = {
   { CONF_COMMENT      , CONFTYPE_STR     , read_str     , PP_SCRIPT_COMMENT      , NULL },
   { CONF_PLUGIN       , CONFTYPE_STR     , read_str     , PP_SCRIPT_PLUGIN       , NULL },
   { CONF_PROPERTY     , CONFTYPE_PROPLIST, read_property, PP_SCRIPT_PROPERTY     , NULL },
   { CONF_EXECUTE_ON   , CONFTYPE_EXECUTE_ON  , read_execute_on  , PP_SCRIPT_EXECUTE_ON   , NULL },
   { CONF_EXECUTE_WHERE, CONFTYPE_EXECUTE_WHERE  , read_execute_where  , PP_SCRIPT_EXECUTE_WHERE, NULL },
   { CONF_ORDER        , CONFTYPE_INT     , read_int     , PP_SCRIPT_ORDER        , NULL },
   { CONF_SINGLE_EXECUTION, CONFTYPE_BOOLEAN, read_bool    , PP_SCRIPT_SINGLE_EXECUTION, NULL },
   { CONF_CLIENT_NAME  , CONFTYPE_STR     , read_str     , PP_SCRIPT_CLIENT_NAME  , NULL },
   { CONF_UNKNOWN      , CONFTYPE_INT     , NULL         , PP_SCRIPT_PP_SCRIPT    , NULL }
};

conf_var_t device_config_var [] = {
   { CONF_COMMENT         , CONFTYPE_STR      , read_str      , DEVICE_CONFIG_COMMENT        , NULL },
   { CONF_DEVICE_PROPERTY , CONFTYPE_PROPLIST , read_property , DEVICE_CONFIG_DEVICE_PROPERTY, NULL },
   { CONF_TAPEDEV         , CONFTYPE_STR      , read_str      , DEVICE_CONFIG_TAPEDEV        , NULL },
   { CONF_UNKNOWN         , CONFTYPE_INT      , NULL          , DEVICE_CONFIG_DEVICE_CONFIG  , NULL }
};

conf_var_t changer_config_var [] = {
   { CONF_COMMENT         , CONFTYPE_STR      , read_str      , CHANGER_CONFIG_COMMENT        , NULL },
   { CONF_TAPEDEV         , CONFTYPE_STR      , read_str      , CHANGER_CONFIG_TAPEDEV        , NULL },
   { CONF_TPCHANGER       , CONFTYPE_STR      , read_str      , CHANGER_CONFIG_TPCHANGER      , NULL },
   { CONF_CHANGERDEV      , CONFTYPE_STR      , read_str      , CHANGER_CONFIG_CHANGERDEV     , NULL },
   { CONF_CHANGERFILE     , CONFTYPE_STR      , read_str      , CHANGER_CONFIG_CHANGERFILE    , NULL },
   { CONF_PROPERTY        , CONFTYPE_PROPLIST , read_property , CHANGER_CONFIG_PROPERTY       , NULL },
   { CONF_DEVICE_PROPERTY , CONFTYPE_PROPLIST , read_property , CHANGER_CONFIG_DEVICE_PROPERTY, NULL },
   { CONF_UNKNOWN         , CONFTYPE_INT      , NULL          , CHANGER_CONFIG_CHANGER_CONFIG , NULL }
};

conf_var_t interactivity_var [] = {
   { CONF_COMMENT         , CONFTYPE_STR      , read_str      , INTERACTIVITY_COMMENT        , NULL },
   { CONF_PLUGIN          , CONFTYPE_STR      , read_str      , INTERACTIVITY_PLUGIN         , NULL },
   { CONF_PROPERTY        , CONFTYPE_PROPLIST , read_property , INTERACTIVITY_PROPERTY       , NULL },
   { CONF_UNKNOWN         , CONFTYPE_INT      , NULL          , INTERACTIVITY_INTERACTIVITY  , NULL }
};

conf_var_t taperscan_var [] = {
   { CONF_COMMENT         , CONFTYPE_STR      , read_str      , TAPERSCAN_COMMENT        , NULL },
   { CONF_PLUGIN          , CONFTYPE_STR      , read_str      , TAPERSCAN_PLUGIN         , NULL },
   { CONF_PROPERTY        , CONFTYPE_PROPLIST , read_property , TAPERSCAN_PROPERTY       , NULL },
   { CONF_UNKNOWN         , CONFTYPE_INT      , NULL          , TAPERSCAN_TAPERSCAN      , NULL }
};

/*
 * Lexical Analysis Implementation
 */

static char *
get_token_name(
    tok_t token)
{
    keytab_t *kt;

    if (keytable == NULL) {
	error(_("keytable == NULL"));
	/*NOTREACHED*/
    }

    for(kt = keytable; kt->token != CONF_UNKNOWN; kt++)
	if(kt->token == token) break;

    if(kt->token == CONF_UNKNOWN)
	return("");
    return(kt->keyword);
}

static tok_t
lookup_keyword(
    char *	str)
{
    keytab_t *kwp;
    char *str1 = stralloc(str);
    char *p = str1;

    /* Fold '-' to '_' in the token.  Note that this modifies str1
     * in place. */
    while (*p) {
	if (*p == '-') *p = '_';
	p++;
    }

    for(kwp = keytable; kwp->keyword != NULL; kwp++) {
	if (strcasecmp(kwp->keyword, str1) == 0) break;
    }

    amfree(str1);
    return kwp->token;
}

static void
get_conftoken(
    tok_t	exp)
{
    int ch, d;
    gint64 int64;
    char *buf;
    char *tmps;
    int token_overflow;
    int inquote = 0;
    int escape = 0;
    int sign;

    if (token_pushed) {
	token_pushed = 0;
	tok = pushed_tok;

	/*
	** If it looked like a keyword before then look it
	** up again in the current keyword table.
	*/
	switch(tok) {
	case CONF_INT64:   case CONF_SIZE:
	case CONF_INT:     case CONF_REAL:    case CONF_STRING:
	case CONF_LBRACE:  case CONF_RBRACE:  case CONF_COMMA:
	case CONF_NL:      case CONF_END:     case CONF_UNKNOWN:
	case CONF_TIME:
	    break; /* not a keyword */

	default:
	    if (exp == CONF_IDENT)
		tok = CONF_IDENT;
	    else
		tok = lookup_keyword(tokenval.v.s);
	    break;
	}
    }
    else {
	ch = conftoken_getc();

	/* note that we're explicitly assuming this file is ASCII.  Someday
	 * maybe we'll support UTF-8? */
	while(ch != EOF && ch != '\n' && g_ascii_isspace(ch))
	    ch = conftoken_getc();
	if (ch == '#') {	/* comment - eat everything but eol/eof */
	    while((ch = conftoken_getc()) != EOF && ch != '\n') {
		(void)ch; /* Quiet empty loop complaints */	
	    }
	}

	if (isalpha(ch)) {		/* identifier */
	    buf = tkbuf;
	    token_overflow = 0;
	    do {
		if (buf < tkbuf+sizeof(tkbuf)-1) {
		    *buf++ = (char)ch;
		} else {
		    *buf = '\0';
		    if (!token_overflow) {
			conf_parserror(_("token too long: %.20s..."), tkbuf);
		    }
		    token_overflow = 1;
		}
		ch = conftoken_getc();
	    } while(isalnum(ch) || ch == '_' || ch == '-');

	    if (ch != EOF && conftoken_ungetc(ch) == EOF) {
		if (ferror(current_file)) {
		    conf_parserror(_("Pushback of '%c' failed: %s"),
				   ch, strerror(ferror(current_file)));
		} else {
		    conf_parserror(_("Pushback of '%c' failed: EOF"), ch);
		}
	    }
	    *buf = '\0';

	    tokenval.v.s = tkbuf;

	    if (token_overflow) tok = CONF_UNKNOWN;
	    else if (exp == CONF_IDENT) tok = CONF_IDENT;
	    else tok = lookup_keyword(tokenval.v.s);
	}
	else if (isdigit(ch)) {	/* integer */
	    sign = 1;

negative_number: /* look for goto negative_number below sign is set there */
	    int64 = 0;
	    do {
		int64 = int64 * 10 + (ch - '0');
		ch = conftoken_getc();
	    } while (isdigit(ch));

	    if (ch != '.') {
		if (exp == CONF_INT) {
		    tok = CONF_INT;
		    tokenval.v.i = sign * (int)int64;
		} else if (exp != CONF_REAL) {
		    tok = CONF_INT64;
		    tokenval.v.int64 = (gint64)sign * int64;
		} else {
		    /* automatically convert to real when expected */
		    tokenval.v.r = (double)sign * (double)int64;
		    tok = CONF_REAL;
		}
	    } else {
		/* got a real number, not an int */
		tokenval.v.r = sign * (double) int64;
		int64 = 0;
		d = 1;
		ch = conftoken_getc();
		while (isdigit(ch)) {
		    int64 = int64 * 10 + (ch - '0');
		    d = d * 10;
		    ch = conftoken_getc();
		}
		tokenval.v.r += sign * ((double)int64) / d;
		tok = CONF_REAL;
	    }

	    if (ch != EOF &&  conftoken_ungetc(ch) == EOF) {
		if (ferror(current_file)) {
		    conf_parserror(_("Pushback of '%c' failed: %s"),
				   ch, strerror(ferror(current_file)));
		} else {
		    conf_parserror(_("Pushback of '%c' failed: EOF"), ch);
		}
	    }
	} else switch(ch) {
	case '"':			/* string */
	    buf = tkbuf;
	    token_overflow = 0;
	    inquote = 1;
	    *buf++ = (char)ch;
	    while (inquote && ((ch = conftoken_getc()) != EOF)) {
		if (ch == '\n') {
		    if (!escape) {
			conf_parserror(_("string not terminated"));
			conftoken_ungetc(ch);
			break;
		    }
		    escape = 0;
		    buf--; /* Consume escape in buffer */
		} else if (ch == '\\' && !escape) {
		    escape = 1;
		} else {
		    if (ch == '"') {
			if (!escape)
			    inquote = 0;
		    }
		    escape = 0;
		}

		if(buf >= &tkbuf[sizeof(tkbuf) - 1]) {
		    if (!token_overflow) {
			conf_parserror(_("string too long: %.20s..."), tkbuf);
		    }
		    token_overflow = 1;
		    break;
		}
		*buf++ = (char)ch;
	    }
	    *buf = '\0';

	    /*
	     * A little manuver to leave a fully unquoted, unallocated  string
	     * in tokenval.v.s
	     */
	    tmps = unquote_string(tkbuf);
	    strncpy(tkbuf, tmps, sizeof(tkbuf));
	    amfree(tmps);
	    tokenval.v.s = tkbuf;

	    tok = (token_overflow) ? CONF_UNKNOWN :
			(exp == CONF_IDENT) ? CONF_IDENT : CONF_STRING;
	    break;

	case '-':
	    ch = conftoken_getc();
	    if (isdigit(ch)) {
		sign = -1;
		goto negative_number;
	    }
	    else {
		if (ch != EOF && conftoken_ungetc(ch) == EOF) {
		    if (ferror(current_file)) {
			conf_parserror(_("Pushback of '%c' failed: %s"),
				       ch, strerror(ferror(current_file)));
		    } else {
			conf_parserror(_("Pushback of '%c' failed: EOF"), ch);
		    }
		}
		tok = CONF_UNKNOWN;
	    }
	    break;

	case ',':
	    tok = CONF_COMMA;
	    break;

	case '{':
	    tok = CONF_LBRACE;
	    break;

	case '}':
	    tok = CONF_RBRACE;
	    break;

	case '\n':
	    tok = CONF_NL;
	    break;

	case EOF:
	    tok = CONF_END;
	    break;

	default:
	    tok = CONF_UNKNOWN;
	    break;
	}
    }

    if (exp != CONF_ANY && tok != exp) {
	char *str;
	keytab_t *kwp;

	switch(exp) {
	case CONF_LBRACE:
	    str = "\"{\"";
	    break;

	case CONF_RBRACE:
	    str = "\"}\"";
	    break;

	case CONF_COMMA:
	    str = "\",\"";
	    break;

	case CONF_NL:
	    str = _("end of line");
	    break;

	case CONF_END:
	    str = _("end of file");
	    break;

	case CONF_INT:
	    str = _("an integer");
	    break;

	case CONF_REAL:
	    str = _("a real number");
	    break;

	case CONF_STRING:
	    str = _("a quoted string");
	    break;

	case CONF_IDENT:
	    str = _("an identifier");
	    break;

	default:
	    for(kwp = keytable; kwp->keyword != NULL; kwp++) {
		if (exp == kwp->token)
		    break;
	    }
	    if (kwp->keyword == NULL)
		str = _("token not");
	    else
		str = str_keyword(kwp);
	    break;
	}
	conf_parserror(_("%s is expected"), str);
	tok = exp;
	if (tok == CONF_INT)
	    tokenval.v.i = 0;
	else
	    tokenval.v.s = "";
    }
}

static void
unget_conftoken(void)
{
    assert(!token_pushed);
    token_pushed = 1;
    pushed_tok = tok;
    tok = CONF_UNKNOWN;
}

static int
conftoken_getc(void)
{
    if(current_line == NULL)
	return getc(current_file);
    if(*current_char == '\0')
	return -1;
    return(*current_char++);
}

static int
conftoken_ungetc(
    int c)
{
    if(current_line == NULL)
	return ungetc(c, current_file);
    else if(current_char > current_line) {
	if(c == -1)
	    return c;
	current_char--;
	if(*current_char != c) {
	    error(_("*current_char != c   : %c %c"), *current_char, c);
	    /* NOTREACHED */
	}
    } else {
	error(_("current_char == current_line"));
	/* NOTREACHED */
    }
    return c;
}

/*
 * Parser Implementation
 */

static void
read_conffile(
    char *filename,
    gboolean is_client,
    gboolean missing_ok)
{
    /* Save global locations. */
    FILE *save_file     = current_file;
    char *save_filename = current_filename;
    int  save_line_num  = current_line_num;
    int	rc;

    if (is_client) {
	keytable = client_keytab;
	parsetable = client_var;
    } else {
	keytable = server_keytab;
	parsetable = server_var;
    }
    filename = config_dir_relative(filename);
    current_filename = get_seen_filename(filename);
    amfree(filename);

    if ((current_file = fopen(current_filename, "r")) == NULL) {
	if (!missing_ok)
	    conf_parserror(_("could not open conf file \"%s\": %s"), 
		    current_filename, strerror(errno));
	goto finish;
    }

    current_line_num = 0;

    do {
	/* read_confline() can invoke us recursively via "includefile" */
	rc = read_confline(is_client);
    } while (rc != 0);

    afclose(current_file);

finish:

    /* Restore servers */
    current_line_num = save_line_num;
    current_file     = save_file;
    current_filename = save_filename;
}

static gboolean
read_confline(
    gboolean is_client)
{
    conf_var_t *np;

    current_line_num += 1;
    get_conftoken(CONF_ANY);
    handle_deprecated_keyword();

    switch(tok) {
    case CONF_INCLUDEFILE:
	get_conftoken(CONF_STRING);
	read_conffile(tokenval.v.s, is_client, FALSE);
	break;

    case CONF_DEFINE:
	if (is_client) {
	    get_conftoken(CONF_ANY);
	    /* accept application-tool here, too, for backward compatibility */
	    if(tok == CONF_APPLICATION_TOOL || tok == CONF_APPLICATION) get_application();
	    else if(tok == CONF_SCRIPT_TOOL || tok == CONF_SCRIPT) get_pp_script();
	    else conf_parserror(_("APPLICATION-TOOL or SCRIPT-TOOL expected"));
	} else {
	    get_conftoken(CONF_ANY);
	    if(tok == CONF_DUMPTYPE) get_dumptype();
	    else if(tok == CONF_TAPETYPE) get_tapetype();
	    else if(tok == CONF_INTERFACE) get_interface();
	    else if(tok == CONF_APPLICATION_TOOL || tok == CONF_APPLICATION) get_application();
	    else if(tok == CONF_SCRIPT_TOOL || tok == CONF_SCRIPT) get_pp_script();
	    else if(tok == CONF_DEVICE) get_device_config();
	    else if(tok == CONF_CHANGER) get_changer_config();
	    else if(tok == CONF_HOLDING) get_holdingdisk(1);
	    else if(tok == CONF_INTERACTIVITY) get_interactivity();
	    else if(tok == CONF_TAPERSCAN) get_taperscan();
	    else conf_parserror(_("DUMPTYPE, INTERFACE, TAPETYPE, HOLDINGDISK, APPLICATION, SCRIPT, DEVICE, CHANGER, INTERACTIVITY or TAPERSCAN expected"));
	    current_block = NULL;
	}
	break;

    case CONF_NL:	/* empty line */
	break;

    case CONF_END:	/* end of file */
	return 0;

    /* These should never be at the begining of a line */
    case CONF_LBRACE:
    case CONF_RBRACE:
    case CONF_IDENT:
    case CONF_INT:
    case CONF_INT64:
    case CONF_BOOL:
    case CONF_REAL:
    case CONF_STRING:
    case CONF_TIME:
    case CONF_SIZE:
	conf_parserror("error: not a keyword.");
	break;

    /* if it's not a known punctuation mark, then check the parse table and use the
     * read_function we find there. */
    default:
	{
	    for(np = parsetable; np->token != CONF_UNKNOWN; np++) 
		if(np->token == tok) break;

	    if(np->token == CONF_UNKNOWN) {
                handle_invalid_keyword(tokenval.v.s);
	    } else {
		np->read_function(np, &conf_data[np->parm]);
		if(np->validate_function)
		    np->validate_function(np, &conf_data[np->parm]);
	    }
	}
    }
    if(tok != CONF_NL)
	get_conftoken(CONF_NL);
    return 1;
}

static void
handle_deprecated_keyword(void)
{
    /* Procedure for deprecated keywords:
     *
     * 1) At time of deprecation, add to warning_deprecated below.  Note the
     *    version in which deprecation will expire.  The keyword will still be
     *    parsed, and can still be used from other parts of Amanda, during this
     *    time.
     * 2) After it has expired, move the keyword (as a string) to
     *    error_deprecated below. Remove the token (CONF_XXX) and
     *    config parameter (CNF_XXX) from the rest of the module.
     *    Note the date of the move.
     */

    static struct { tok_t tok; gboolean warned; }
    warning_deprecated[] = {
	{ CONF_LABEL_NEW_TAPES, 0 },	/* exp in Amanda-3.2 */
	{ CONF_AMRECOVER_DO_FSF, 0 },	/* exp in Amanda-3.3 */
	{ CONF_AMRECOVER_CHECK_LABEL, 0 }, /* exp in Amanda-3.3 */
	{ CONF_TAPE_SPLITSIZE, 0 },	/* exp. in Amanda-3.3 */
	{ CONF_SPLIT_DISKBUFFER, 0 },	/* exp. in Amanda-3.3 */
	{ CONF_FALLBACK_SPLITSIZE, 0 }, /* exp. in Amanda-3.3 */
	{ 0, 0 },
    }, *dep;

    for (dep = warning_deprecated; dep->tok; dep++) {
	if (tok == dep->tok) {
	    if (!dep->warned)
		conf_parswarn(_("warning: Keyword %s is deprecated."),
			       tokenval.v.s);
	    dep->warned = 1;
	    break;
	}
    }
}

static void
handle_invalid_keyword(
    const char * token)
{
    static const char * error_deprecated[] = {
	"rawtapedev",
	"tapebufs", /* deprecated: 2007-10-15; invalid: 2010-04-14 */
	"file-pad", /* deprecated: 2008-07-01; invalid: 2010-04-14 */
        NULL
    };
    const char ** s;
    char *folded_token, *p;

    /* convert '_' to '-' in TOKEN */
    folded_token = g_strdup(token);
    for (p = folded_token; *p; p++) {
	if (*p == '_') *p = '-';
    }

    for (s = error_deprecated; *s != NULL; s ++) {
	if (g_ascii_strcasecmp(*s, folded_token) == 0) {
	    conf_parserror(_("error: Keyword %s is deprecated."),
			   token);
	    g_free(folded_token);
	    return;
	}
    }
    g_free(folded_token);

    if (*s == NULL) {
        conf_parserror(_("configuration keyword expected"));
    }

    for (;;) {
        char c = conftoken_getc();
        if (c == '\n' || c == -1) {
            conftoken_ungetc(c);
            return;
        }
    }

    g_assert_not_reached();
}

static char *
get_seen_filename(
    char *filename)
{
    GSList *iter;
    char *istr;

    for (iter = seen_filenames; iter; iter = iter->next) {
	istr = iter->data;
	if (istr == filename || 0 == strcmp(istr, filename))
	    return istr;
    }

    istr = stralloc(filename);
    seen_filenames = g_slist_prepend(seen_filenames, istr);
    return istr;
}

static void
read_block(
    conf_var_t    *read_var,
    val_t    *valarray,
    char     *errormsg,
    int       read_brace,
    void      (*copy_function)(void),
    char     *type,
    char     *name)
{
    conf_var_t *np;
    int         done;
    char       *key_ovr;
    int         i;

    if(read_brace) {
	get_conftoken(CONF_LBRACE);
	get_conftoken(CONF_NL);
    }

    done = 0;
    do {
	current_line_num += 1;
	get_conftoken(CONF_ANY);
	handle_deprecated_keyword();

	switch(tok) {
	case CONF_RBRACE:
	    done = 1;
	    break;
	case CONF_NL:	/* empty line */
	    break;
	case CONF_END:	/* end of file */
	    done = 1;
	    break;

	/* inherit from a "parent" */
        case CONF_IDENT:
        case CONF_STRING:
	    if(copy_function) 
		copy_function();
	    else
		conf_parserror(_("ident not expected"));
	    break;
	default:
	    {
		for(np = read_var; np->token != CONF_UNKNOWN; np++)
		    if(np->token == tok) break;

		if(np->token == CONF_UNKNOWN)
		    conf_parserror("%s", errormsg);
		else {
		    np->read_function(np, &valarray[np->parm]);
		    if(np->validate_function)
			np->validate_function(np, &valarray[np->parm]);
		}
	    }
	}
	if(tok != CONF_NL && tok != CONF_END && tok != CONF_RBRACE)
	    get_conftoken(CONF_NL);
    } while(!done);

    if (!config_overrides)
	return;

    key_ovr = vstralloc(type, ":", name, NULL);
    for (i = 0; i < config_overrides->n_used; i++) {
	config_override_t *co = &config_overrides->ovr[i];
	char              *key = co->key;
	char              *keyword;
	char              *value;
	keytab_t          *kt;

	if (key_ovr && strncasecmp(key_ovr, key, strlen(key_ovr)) != 0)
	    continue;

	if (strlen(key) <= strlen(key_ovr) + 1)
	    continue;

	keyword = key + strlen(key_ovr) + 1;
	value = co->value;

	/* find the token in keytable */
	for (kt = keytable; kt->token != CONF_UNKNOWN; kt++) {
	    if (kt->keyword && strcasecmp(kt->keyword, keyword) == 0)
		break;
	}
	if (kt->token == CONF_UNKNOWN)
	     continue;

	/* find the var in read_var */
	for (np = read_var; np->token != CONF_UNKNOWN; np++)
	    if (np->token == kt->token) break;
	if (np->token == CONF_UNKNOWN)
	    continue;

	/* now set up a fake line and use the relevant read_function to
	 * parse it.  This is sneaky! */
	if (np->type == CONFTYPE_STR) {
	    current_line = quote_string_always(value);
	} else {
	    current_line = stralloc(value);
	}

	current_char = current_line;
	token_pushed = 0;
	current_line_num = -2;
	allow_overwrites = 1;
	co->applied = TRUE;

	np->read_function(np, &valarray[np->parm]);
	if (np->validate_function)
	    np->validate_function(np, &valarray[np->parm]);

	amfree(current_line);
	current_char = NULL;
    }
    amfree(key_ovr);

}

static void
read_holdingdisk(
    conf_var_t *np  G_GNUC_UNUSED,
    val_t      *val G_GNUC_UNUSED)
{
    assert (val == &conf_data[CNF_HOLDINGDISK]);
    get_holdingdisk(0);
}

static void
get_holdingdisk(
    int is_define)
{
    int save_overwrites;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_holdingdisk_defaults();

    get_conftoken(CONF_IDENT);
    hdcur.name = stralloc(tokenval.v.s);
    current_block = g_strconcat("holdingdisk ", hdcur.name, NULL);
    hdcur.seen.block = current_block;
    hdcur.seen.filename = current_filename;
    hdcur.seen.linenum = current_line_num;

    get_conftoken(CONF_ANY);
    if (tok == CONF_LBRACE) {
	holdingdisk_t *hd;
	hd = lookup_holdingdisk(hdcur.name);
	if (hd) {
	    conf_parserror(_("holding disk '%s' already defined"),
			       hdcur.name);
	} else {
	    unget_conftoken();
	    read_block(holding_var, hdcur.value,
		     _("holding disk parameter expected"), 1, copy_holdingdisk,
		     "HOLDINGDISK", hdcur.name);
	    get_conftoken(CONF_NL);
            save_holdingdisk();
	    if (!is_define) {
		conf_data[CNF_HOLDINGDISK].v.identlist = g_slist_append(
				conf_data[CNF_HOLDINGDISK].v.identlist,
				stralloc(hdcur.name));
	    }
	}
    } else { /* use the already defined holding disk */
	unget_conftoken();
	if (is_define) {
	    conf_parserror(_("holdingdisk definition must specify holdingdisk parameters"));
	}
	do {
	    identlist_t il;

	    for (il = conf_data[CNF_HOLDINGDISK].v.identlist; il != NULL;
							      il = il->next) {
		if (strcmp((char *)il->data, hdcur.name) == 0) {
		    break;
		}
	    }
	    if (il) {
		conf_parserror(_("holding disk '%s' already in use"),
			       hdcur.name);
	    } else {
		conf_data[CNF_HOLDINGDISK].v.identlist = g_slist_append(
				conf_data[CNF_HOLDINGDISK].v.identlist,
				stralloc(hdcur.name));
	    }
	    amfree(hdcur.name);
	    get_conftoken(CONF_ANY);
	    if (tok == CONF_IDENT || tok == CONF_STRING) {
		hdcur.name = stralloc(tokenval.v.s);
	    } else if (tok != CONF_NL) {
		conf_parserror(_("IDENT or NL expected"));
	    }
	} while (tok == CONF_IDENT || tok == CONF_STRING);
    }

    allow_overwrites = save_overwrites;
}

static void
init_holdingdisk_defaults(
    void)
{
    conf_init_str(&hdcur.value[HOLDING_COMMENT]  , "");
    conf_init_str(&hdcur.value[HOLDING_DISKDIR]  , "");
    conf_init_int64(&hdcur.value[HOLDING_DISKSIZE] , CONF_UNIT_K, (gint64)0);
                    /* 1 Gb = 1M counted in 1Kb blocks */
    conf_init_int64(&hdcur.value[HOLDING_CHUNKSIZE], CONF_UNIT_K, (gint64)1024*1024);
}

static void
save_holdingdisk(
    void)
{
    holdingdisk_t *hp;

    hp = alloc(sizeof(holdingdisk_t));
    *hp = hdcur;
    holdinglist = g_slist_append(holdinglist, hp);
}

static void
copy_holdingdisk(
    void)
{
    holdingdisk_t *hp;
    int i;

    hp = lookup_holdingdisk(tokenval.v.s);

    if (hp == NULL) {
        conf_parserror(_("holdingdisk parameter expected"));
        return;
    }

    for(i=0; i < HOLDING_HOLDING; i++) {
        if(hp->value[i].seen.linenum) {
            merge_val_t(&hdcur.value[i], &hp->value[i]);
        }
    }

}


/* WARNING:
 * This function is called both from this module and from diskfile.c. Modify
 * with caution. */
dumptype_t *
read_dumptype(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = get_seen_filename(fname);
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_dumptype_defaults();
    if (name) {
	dpcur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	dpcur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("dumptype ", dpcur.name, NULL);
    dpcur.seen.block = current_block;
    dpcur.seen.filename = current_filename;
    dpcur.seen.linenum = current_line_num;

    read_block(dumptype_var, dpcur.value,
	       _("dumptype parameter expected"),
	       (name == NULL), copy_dumptype,
	       "DUMPTYPE", dpcur.name);

    if(!name) /* !name => reading disklist, not conffile */
	get_conftoken(CONF_NL);

    /* XXX - there was a stupidity check in here for skip-incr and
    ** skip-full.  This check should probably be somewhere else. */

    save_dumptype();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_dumptype(dpcur.name);
}

static void
get_dumptype(void)
{
    read_dumptype(NULL, NULL, NULL, NULL);
}

static void
init_dumptype_defaults(void)
{
    dpcur.name = NULL;
    conf_init_str   (&dpcur.value[DUMPTYPE_COMMENT]           , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_PROGRAM]           , "DUMP");
    conf_init_str   (&dpcur.value[DUMPTYPE_SRVCOMPPROG]       , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_CLNTCOMPPROG]      , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_SRV_ENCRYPT]       , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_CLNT_ENCRYPT]      , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_AMANDAD_PATH]      , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_CLIENT_USERNAME]   , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_CLIENT_PORT]       , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_SSH_KEYS]          , "");
    conf_init_str   (&dpcur.value[DUMPTYPE_AUTH]   , "BSDTCP");
    conf_init_exinclude(&dpcur.value[DUMPTYPE_EXCLUDE]);
    conf_init_exinclude(&dpcur.value[DUMPTYPE_INCLUDE]);
    conf_init_priority (&dpcur.value[DUMPTYPE_PRIORITY]          , 1);
    conf_init_int      (&dpcur.value[DUMPTYPE_DUMPCYCLE]         , CONF_UNIT_NONE, conf_data[CNF_DUMPCYCLE].v.i);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAXDUMPS]          , CONF_UNIT_NONE, conf_data[CNF_MAXDUMPS].v.i);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAXPROMOTEDAY]     , CONF_UNIT_NONE, 10000);
    conf_init_int      (&dpcur.value[DUMPTYPE_BUMPPERCENT]       , CONF_UNIT_NONE, conf_data[CNF_BUMPPERCENT].v.i);
    conf_init_int64    (&dpcur.value[DUMPTYPE_BUMPSIZE]          , CONF_UNIT_K   , conf_data[CNF_BUMPSIZE].v.int64);
    conf_init_int      (&dpcur.value[DUMPTYPE_BUMPDAYS]          , CONF_UNIT_NONE, conf_data[CNF_BUMPDAYS].v.i);
    conf_init_real     (&dpcur.value[DUMPTYPE_BUMPMULT]          , conf_data[CNF_BUMPMULT].v.r);
    conf_init_time     (&dpcur.value[DUMPTYPE_STARTTIME]         , (time_t)0);
    conf_init_strategy (&dpcur.value[DUMPTYPE_STRATEGY]          , DS_STANDARD);
    conf_init_estimatelist(&dpcur.value[DUMPTYPE_ESTIMATELIST]   , ES_CLIENT);
    conf_init_compress (&dpcur.value[DUMPTYPE_COMPRESS]          , COMP_FAST);
    conf_init_encrypt  (&dpcur.value[DUMPTYPE_ENCRYPT]           , ENCRYPT_NONE);
    conf_init_data_path(&dpcur.value[DUMPTYPE_DATA_PATH]         , DATA_PATH_AMANDA);
    conf_init_str   (&dpcur.value[DUMPTYPE_SRV_DECRYPT_OPT]   , "-d");
    conf_init_str   (&dpcur.value[DUMPTYPE_CLNT_DECRYPT_OPT]  , "-d");
    conf_init_rate     (&dpcur.value[DUMPTYPE_COMPRATE]          , 0.50, 0.50);
    conf_init_int64    (&dpcur.value[DUMPTYPE_TAPE_SPLITSIZE]    , CONF_UNIT_K, (gint64)0);
    conf_init_int64    (&dpcur.value[DUMPTYPE_FALLBACK_SPLITSIZE], CONF_UNIT_K, (gint64)10 * 1024);
    conf_init_str   (&dpcur.value[DUMPTYPE_SPLIT_DISKBUFFER]  , NULL);
    conf_init_bool     (&dpcur.value[DUMPTYPE_RECORD]            , 1);
    conf_init_bool     (&dpcur.value[DUMPTYPE_SKIP_INCR]         , 0);
    conf_init_bool     (&dpcur.value[DUMPTYPE_SKIP_FULL]         , 0);
    conf_init_holding  (&dpcur.value[DUMPTYPE_HOLDINGDISK]       , HOLD_AUTO);
    conf_init_bool     (&dpcur.value[DUMPTYPE_KENCRYPT]          , 0);
    conf_init_bool     (&dpcur.value[DUMPTYPE_IGNORE]            , 0);
    conf_init_bool     (&dpcur.value[DUMPTYPE_INDEX]             , 1);
    conf_init_application(&dpcur.value[DUMPTYPE_APPLICATION]);
    conf_init_identlist(&dpcur.value[DUMPTYPE_SCRIPTLIST], NULL);
    conf_init_proplist(&dpcur.value[DUMPTYPE_PROPERTY]);
    conf_init_bool     (&dpcur.value[DUMPTYPE_ALLOW_SPLIT]       , 1);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAX_WARNINGS]      , CONF_UNIT_NONE, 20);
    conf_init_host_limit(&dpcur.value[DUMPTYPE_RECOVERY_LIMIT]);
    conf_init_host_limit_server(&dpcur.value[DUMPTYPE_DUMP_LIMIT]);
}

static void
save_dumptype(void)
{
    dumptype_t *dp, *dp1;;

    dp = lookup_dumptype(dpcur.name);

    if(dp != (dumptype_t *)0) {
	if (dp->seen.linenum == -1) {
	    conf_parserror(_("dumptype %s is defined by default and cannot be redefined"), dp->name);
	} else {
	    conf_parserror(_("dumptype %s already defined at %s:%d"), dp->name,
			   dp->seen.filename, dp->seen.linenum);
	}
	return;
    }

    dp = alloc(sizeof(dumptype_t));
    *dp = dpcur;
    dp->next = NULL;
    /* add at end of list */
    if(!dumplist)
	dumplist = dp;
    else {
	dp1 = dumplist;
	while (dp1->next != NULL) {
	     dp1 = dp1->next;
	}
	dp1->next = dp;
    }
}

static void
copy_dumptype(void)
{
    dumptype_t *dt;
    int i;

    dt = lookup_dumptype(tokenval.v.s);

    if(dt == NULL) {
	conf_parserror(_("dumptype parameter expected"));
	return;
    }

    for(i=0; i < DUMPTYPE_DUMPTYPE; i++) {
	if(dt->value[i].seen.linenum) {
	    merge_val_t(&dpcur.value[i], &dt->value[i]);
	    if (i == DUMPTYPE_SCRIPTLIST) {
		/* sort in 'order' */
		dpcur.value[i].v.identlist = g_slist_sort(dpcur.value[i].v.identlist, &compare_pp_script_order);
	    }
	}
    }
}

static void
get_tapetype(void)
{
    int save_overwrites;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_tapetype_defaults();

    get_conftoken(CONF_IDENT);
    tpcur.name = stralloc(tokenval.v.s);
    current_block = g_strconcat("tapetype ", tpcur.name, NULL);
    tpcur.seen.block = current_block;
    tpcur.seen.filename = current_filename;
    tpcur.seen.linenum = current_line_num;

    read_block(tapetype_var, tpcur.value,
	       _("tapetype parameter expected"), 1, copy_tapetype,
	       "TAPETYPE", tpcur.name);
    get_conftoken(CONF_NL);

    if (tapetype_get_readblocksize(&tpcur) <
	tapetype_get_blocksize(&tpcur)) {
	conf_init_size(&tpcur.value[TAPETYPE_READBLOCKSIZE], CONF_UNIT_K,
		       tapetype_get_blocksize(&tpcur));
    }
    save_tapetype();

    allow_overwrites = save_overwrites;
}

static void
init_tapetype_defaults(void)
{
    conf_init_str(&tpcur.value[TAPETYPE_COMMENT]      , "");
    conf_init_str(&tpcur.value[TAPETYPE_LBL_TEMPL]    , "");
    conf_init_size  (&tpcur.value[TAPETYPE_BLOCKSIZE]    , CONF_UNIT_K, DISK_BLOCK_KB);
    conf_init_size  (&tpcur.value[TAPETYPE_READBLOCKSIZE], CONF_UNIT_K, DISK_BLOCK_KB);
    conf_init_int64 (&tpcur.value[TAPETYPE_LENGTH]       , CONF_UNIT_K, ((gint64)2000));
    conf_init_int64 (&tpcur.value[TAPETYPE_FILEMARK]     , CONF_UNIT_K, (gint64)1);
    conf_init_int   (&tpcur.value[TAPETYPE_SPEED]        , CONF_UNIT_NONE, 200);
    conf_init_int64(&tpcur.value[TAPETYPE_PART_SIZE], CONF_UNIT_K, 0);
    conf_init_part_cache_type(&tpcur.value[TAPETYPE_PART_CACHE_TYPE], PART_CACHE_TYPE_NONE);
    conf_init_str(&tpcur.value[TAPETYPE_PART_CACHE_DIR], "");
    conf_init_int64(&tpcur.value[TAPETYPE_PART_CACHE_MAX_SIZE], CONF_UNIT_K, 0);
}

static void
save_tapetype(void)
{
    tapetype_t *tp, *tp1;

    tp = lookup_tapetype(tpcur.name);

    if(tp != (tapetype_t *)0) {
	amfree(tpcur.name);
	conf_parserror(_("tapetype %s already defined at %s:%d"),
		tp->name, tp->seen.filename, tp->seen.linenum);
	return;
    }

    tp = alloc(sizeof(tapetype_t));
    *tp = tpcur;

    /* add at end of list */
    if(!tapelist)
	tapelist = tp;
    else {
	tp1 = tapelist;
	while (tp1->next != NULL) {
	    tp1 = tp1->next;
	}
	tp1->next = tp;
    }
}

static void
copy_tapetype(void)
{
    tapetype_t *tp;
    int i;

    tp = lookup_tapetype(tokenval.v.s);

    if(tp == NULL) {
	conf_parserror(_("tape type parameter expected"));
	return;
    }

    for(i=0; i < TAPETYPE_TAPETYPE; i++) {
	if(tp->value[i].seen.linenum) {
	    merge_val_t(&tpcur.value[i], &tp->value[i]);
	}
    }
}

static void
get_interface(void)
{
    int save_overwrites;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_interface_defaults();

    get_conftoken(CONF_IDENT);
    ifcur.name = stralloc(tokenval.v.s);
    current_block = g_strconcat("interface ", ifcur.name, NULL);
    ifcur.seen.block = current_block;
    ifcur.seen.filename = current_filename;
    ifcur.seen.linenum = current_line_num;

    read_block(interface_var, ifcur.value,
	       _("interface parameter expected"), 1, copy_interface,
	       "INTERFACE", ifcur.name);
    get_conftoken(CONF_NL);

    save_interface();

    allow_overwrites = save_overwrites;

    return;
}

static void
init_interface_defaults(void)
{
    conf_init_str(&ifcur.value[INTER_COMMENT] , "");
    conf_init_int   (&ifcur.value[INTER_MAXUSAGE], CONF_UNIT_K, 80000);
}

static void
save_interface(void)
{
    interface_t *ip, *ip1;

    ip = lookup_interface(ifcur.name);

    if(ip != (interface_t *)0) {
	conf_parserror(_("interface %s already defined at %s:%d"),
		ip->name, ip->seen.filename, ip->seen.linenum);
	return;
    }

    ip = alloc(sizeof(interface_t));
    *ip = ifcur;
    /* add at end of list */
    if(!interface_list) {
	interface_list = ip;
    } else {
	ip1 = interface_list;
	while (ip1->next != NULL) {
	    ip1 = ip1->next;
	}
	ip1->next = ip;
    }
}

static void
copy_interface(void)
{
    interface_t *ip;
    int i;

    ip = lookup_interface(tokenval.v.s);

    if(ip == NULL) {
	conf_parserror(_("interface parameter expected"));
	return;
    }

    for(i=0; i < INTER_INTER; i++) {
	if(ip->value[i].seen.linenum) {
	    merge_val_t(&ifcur.value[i], &ip->value[i]);
	}
    }
}


static application_t *
read_application(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = get_seen_filename(fname);
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_application_defaults();
    if (name) {
	apcur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	apcur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("application ", apcur.name, NULL);
    apcur.seen.block = current_block;
    apcur.seen.filename = current_filename;
    apcur.seen.linenum = current_line_num;

    read_block(application_var, apcur.value,
	       _("application parameter expected"),
	       (name == NULL), *copy_application,
	       "APPLICATION", apcur.name);
    if(!name)
	get_conftoken(CONF_NL);

    save_application();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_application(apcur.name);
}

static void
get_application(
    void)
{
    read_application(NULL, NULL, NULL, NULL);
}

static void
init_application_defaults(
    void)
{
    apcur.name = NULL;
    conf_init_str(&apcur.value[APPLICATION_COMMENT] , "");
    conf_init_str(&apcur.value[APPLICATION_PLUGIN]  , "");
    conf_init_proplist(&apcur.value[APPLICATION_PROPERTY]);
    conf_init_str(&apcur.value[APPLICATION_CLIENT_NAME] , "");
}

static void
save_application(
    void)
{
    application_t *ap, *ap1;

    ap = lookup_application(apcur.name);

    if(ap != (application_t *)0) {
	conf_parserror(_("application %s already defined at %s:%d"),
		       ap->name, ap->seen.filename, ap->seen.linenum);
	return;
    }

    ap = alloc(sizeof(application_t));
    *ap = apcur;
    ap->next = NULL;
    /* add at end of list */
    if (!application_list)
	application_list = ap;
    else {
	ap1 = application_list;
	while (ap1->next != NULL) {
	    ap1 = ap1->next;
	}
	ap1->next = ap;
    }
}

static void
copy_application(void)
{
    application_t *ap;
    int i;

    ap = lookup_application(tokenval.v.s);

    if(ap == NULL) {
	conf_parserror(_("application parameter expected"));
	return;
    }

    for(i=0; i < APPLICATION_APPLICATION; i++) {
	if(ap->value[i].seen.linenum) {
	    merge_val_t(&apcur.value[i], &ap->value[i]);
	}
    }
}

static interactivity_t *
read_interactivity(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = get_seen_filename(fname);
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_interactivity_defaults();
    if (name) {
	ivcur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	ivcur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("interactivity ", ivcur.name, NULL);
    ivcur.seen.block = current_block;
    ivcur.seen.filename = current_filename;
    ivcur.seen.linenum = current_line_num;

    read_block(interactivity_var, ivcur.value,
	       _("interactivity parameter expected"),
	       (name == NULL), *copy_interactivity,
	       "INTERACTIVITY", ivcur.name);
    if(!name)
	get_conftoken(CONF_NL);

    save_interactivity();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_interactivity(ivcur.name);
}

static void
get_interactivity(
    void)
{
    read_interactivity(NULL, NULL, NULL, NULL);
}

static void
init_interactivity_defaults(
    void)
{
    ivcur.name = NULL;
    conf_init_str(&ivcur.value[INTERACTIVITY_COMMENT] , "");
    conf_init_str(&ivcur.value[INTERACTIVITY_PLUGIN]  , "");
    conf_init_proplist(&ivcur.value[INTERACTIVITY_PROPERTY]);
}

static void
save_interactivity(
    void)
{
    interactivity_t *iv, *iv1;

    iv = lookup_interactivity(ivcur.name);

    if (iv != (interactivity_t *)0) {
	conf_parserror(_("interactivity %s already defined at %s:%d"),
		       iv->name, iv->seen.filename, iv->seen.linenum);
	return;
    }

    iv = alloc(sizeof(interactivity_t));
    *iv = ivcur;
    iv->next = NULL;
    /* add at end of list */
    if (!interactivity_list)
	interactivity_list = iv;
    else {
	iv1 = interactivity_list;
	while (iv1->next != NULL) {
	    iv1 = iv1->next;
	}
	iv1->next = iv;
    }
}

static void
copy_interactivity(void)
{
    interactivity_t *iv;
    int i;

    iv = lookup_interactivity(tokenval.v.s);

    if (iv == NULL) {
	conf_parserror(_("interactivity parameter expected"));
	return;
    }

    for (i=0; i < INTERACTIVITY_INTERACTIVITY; i++) {
	if(iv->value[i].seen.linenum) {
	    merge_val_t(&ivcur.value[i], &iv->value[i]);
	}
    }
}

static taperscan_t *
read_taperscan(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = get_seen_filename(fname);
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_taperscan_defaults();
    if (name) {
	tscur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	tscur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("taperscan ", tscur.name, NULL);
    tscur.seen.block = current_block;
    tscur.seen.filename = current_filename;
    tscur.seen.linenum = current_line_num;

    read_block(taperscan_var, tscur.value,
	       _("taperscan parameter expected"),
	       (name == NULL), *copy_taperscan,
	       "TAPERSCAN", tscur.name);
    if(!name)
	get_conftoken(CONF_NL);

    save_taperscan();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_taperscan(tscur.name);
}

static void
get_taperscan(
    void)
{
    read_taperscan(NULL, NULL, NULL, NULL);
}

static void
init_taperscan_defaults(
    void)
{
    tscur.name = NULL;
    conf_init_str(&tscur.value[TAPERSCAN_COMMENT] , "");
    conf_init_str(&tscur.value[TAPERSCAN_PLUGIN]  , "");
    conf_init_proplist(&tscur.value[TAPERSCAN_PROPERTY]);
}

static void
save_taperscan(
    void)
{
    taperscan_t *ts, *ts1;

    ts = lookup_taperscan(tscur.name);

    if (ts != (taperscan_t *)0) {
	conf_parserror(_("taperscan %s already defined at %s:%d"),
		       ts->name, ts->seen.filename, ts->seen.linenum);
	return;
    }

    ts = alloc(sizeof(taperscan_t));
    *ts = tscur;
    ts->next = NULL;
    /* add at end of list */
    if (!taperscan_list)
	taperscan_list = ts;
    else {
	ts1 = taperscan_list;
	while (ts1->next != NULL) {
	    ts1 = ts1->next;
	}
	ts1->next = ts;
    }
}

static void
copy_taperscan(void)
{
    taperscan_t *ts;
    int i;

    ts = lookup_taperscan(tokenval.v.s);

    if (ts == NULL) {
	conf_parserror(_("taperscan parameter expected"));
	return;
    }

    for (i=0; i < TAPERSCAN_TAPERSCAN; i++) {
	if(ts->value[i].seen.linenum) {
	    merge_val_t(&tscur.value[i], &ts->value[i]);
	}
    }
}

static pp_script_t *
read_pp_script(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = get_seen_filename(fname);
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_pp_script_defaults();
    if (name) {
	pscur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	pscur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("script ", pscur.name, NULL);
    pscur.seen.block = current_block;
    pscur.seen.filename = current_filename;
    pscur.seen.linenum = current_line_num;

    read_block(pp_script_var, pscur.value,
	       _("script parameter expected"),
	       (name == NULL), *copy_pp_script,
	       "SCRIPT", pscur.name);
    if(!name)
	get_conftoken(CONF_NL);

    save_pp_script();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_pp_script(pscur.name);
}

static void
get_pp_script(
    void)
{
    read_pp_script(NULL, NULL, NULL, NULL);
}

static void
init_pp_script_defaults(
    void)
{
    pscur.name = NULL;
    conf_init_str(&pscur.value[PP_SCRIPT_COMMENT] , "");
    conf_init_str(&pscur.value[PP_SCRIPT_PLUGIN]  , "");
    conf_init_proplist(&pscur.value[PP_SCRIPT_PROPERTY]);
    conf_init_execute_on(&pscur.value[PP_SCRIPT_EXECUTE_ON], 0);
    conf_init_execute_where(&pscur.value[PP_SCRIPT_EXECUTE_WHERE], ES_CLIENT);
    conf_init_int(&pscur.value[PP_SCRIPT_ORDER], CONF_UNIT_NONE, 5000);
    conf_init_bool(&pscur.value[PP_SCRIPT_SINGLE_EXECUTION], 0);
    conf_init_str(&pscur.value[PP_SCRIPT_CLIENT_NAME], "");
}

static void
save_pp_script(
    void)
{
    pp_script_t *ps, *ps1;

    ps = lookup_pp_script(pscur.name);

    if(ps != (pp_script_t *)0) {
	conf_parserror(_("script %s already defined at %s:%d"),
		       ps->name, ps->seen.filename, ps->seen.linenum);
	return;
    }

    ps = alloc(sizeof(pp_script_t));
    *ps = pscur;
    ps->next = NULL;
    /* add at end of list */
    if (!pp_script_list)
	pp_script_list = ps;
    else {
	ps1 = pp_script_list;
	while (ps1->next != NULL) {
	    ps1 = ps1->next;
	}
	ps1->next = ps;
    }
}

static void
copy_pp_script(void)
{
    pp_script_t *ps;
    int i;

    ps = lookup_pp_script(tokenval.v.s);

    if(ps == NULL) {
	conf_parserror(_("script parameter expected"));
	return;
    }

    for(i=0; i < PP_SCRIPT_PP_SCRIPT; i++) {
	if(ps->value[i].seen.linenum) {
	    merge_val_t(&pscur.value[i], &ps->value[i]);
	}
    }
}

static device_config_t *
read_device_config(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = get_seen_filename(fname);
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_device_config_defaults();
    if (name) {
	dccur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	dccur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("device ", dccur.name, NULL);
    dccur.seen.block = current_block;
    dccur.seen.filename = current_filename;
    dccur.seen.linenum = current_line_num;

    read_block(device_config_var, dccur.value,
	       _("device parameter expected"),
	       (name == NULL), *copy_device_config,
	       "DEVICE", dccur.name);
    if(!name)
	get_conftoken(CONF_NL);

    save_device_config();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_device_config(dccur.name);
}

static void
get_device_config(
    void)
{
    read_device_config(NULL, NULL, NULL, NULL);
}

static void
init_device_config_defaults(
    void)
{
    dccur.name = NULL;
    conf_init_str(&dccur.value[DEVICE_CONFIG_COMMENT] , "");
    conf_init_str(&dccur.value[DEVICE_CONFIG_TAPEDEV]  , "");
    conf_init_proplist(&dccur.value[DEVICE_CONFIG_DEVICE_PROPERTY]);
}

static void
save_device_config(
    void)
{
    device_config_t *dc, *dc1;

    dc = lookup_device_config(dccur.name);

    if(dc != (device_config_t *)0) {
	conf_parserror(_("device %s already defined at %s:%d"),
		       dc->name, dc->seen.filename, dc->seen.linenum);
	return;
    }

    dc = alloc(sizeof(device_config_t));
    *dc = dccur;
    dc->next = NULL;
    /* add at end of list */
    if (!device_config_list)
	device_config_list = dc;
    else {
	dc1 = device_config_list;
	while (dc1->next != NULL) {
	    dc1 = dc1->next;
	}
	dc1->next = dc;
    }
}

static void
copy_device_config(void)
{
    device_config_t *dc;
    int i;

    dc = lookup_device_config(tokenval.v.s);

    if(dc == NULL) {
	conf_parserror(_("device parameter expected"));
	return;
    }

    for(i=0; i < DEVICE_CONFIG_DEVICE_CONFIG; i++) {
	if(dc->value[i].seen.linenum) {
	    merge_val_t(&dccur.value[i], &dc->value[i]);
	}
    }
}

static changer_config_t *
read_changer_config(
    char *name,
    FILE *from,
    char *fname,
    int *linenum)
{
    int save_overwrites;
    FILE *saved_conf = NULL;
    char *saved_fname = NULL;

    if (from) {
	saved_conf = current_file;
	current_file = from;
    }

    if (fname) {
	saved_fname = current_filename;
	current_filename = fname;
    }

    if (linenum)
	current_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_changer_config_defaults();
    if (name) {
	cccur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	cccur.name = stralloc(tokenval.v.s);
    }
    current_block = g_strconcat("changer ", cccur.name, NULL);
    cccur.seen.block = current_block;
    cccur.seen.filename = current_filename;
    cccur.seen.linenum = current_line_num;

    read_block(changer_config_var, cccur.value,
	       _("changer parameter expected"),
	       (name == NULL), *copy_changer_config,
	       "CHANGER", cccur.name);
    if(!name)
	get_conftoken(CONF_NL);

    save_changer_config();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = current_line_num;

    if (fname)
	current_filename = saved_fname;

    if (from)
	current_file = saved_conf;

    return lookup_changer_config(cccur.name);
}

static void
get_changer_config(
    void)
{
    read_changer_config(NULL, NULL, NULL, NULL);
}

static void
init_changer_config_defaults(
    void)
{
    cccur.name = NULL;
    conf_init_str(&cccur.value[CHANGER_CONFIG_COMMENT] , "");
    conf_init_str(&cccur.value[CHANGER_CONFIG_TAPEDEV]  , "");
    conf_init_str(&cccur.value[CHANGER_CONFIG_TPCHANGER]  , "");
    conf_init_str(&cccur.value[CHANGER_CONFIG_CHANGERDEV]  , "");
    conf_init_str(&cccur.value[CHANGER_CONFIG_CHANGERFILE]  , "");
    conf_init_proplist(&cccur.value[CHANGER_CONFIG_PROPERTY]);
    conf_init_proplist(&cccur.value[CHANGER_CONFIG_DEVICE_PROPERTY]);
}

static void
save_changer_config(
    void)
{
    changer_config_t *dc, *dc1;

    dc = lookup_changer_config(cccur.name);

    if(dc != (changer_config_t *)0) {
	conf_parserror(_("changer %s already defined at %s:%d"),
		       dc->name, dc->seen.filename, dc->seen.linenum);
	return;
    }

    dc = alloc(sizeof(changer_config_t));
    *dc = cccur;
    dc->next = NULL;
    /* add at end of list */
    if (!changer_config_list)
	changer_config_list = dc;
    else {
	dc1 = changer_config_list;
	while (dc1->next != NULL) {
	    dc1 = dc1->next;
	}
	dc1->next = dc;
    }
}

static void
copy_changer_config(void)
{
    changer_config_t *dc;
    int i;

    dc = lookup_changer_config(tokenval.v.s);

    if(dc == NULL) {
	conf_parserror(_("changer parameter expected"));
	return;
    }

    for(i=0; i < CHANGER_CONFIG_CHANGER_CONFIG; i++) {
	if(dc->value[i].seen.linenum) {
	    merge_val_t(&cccur.value[i], &dc->value[i]);
	}
    }
}

/* Read functions */

static void
read_int(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    val_t__int(val) = get_int(val->unit);
}

static void
read_int64(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    val_t__int64(val) = get_int64(val->unit);
}

static void
read_real(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    get_conftoken(CONF_REAL);
    val_t__real(val) = tokenval.v.r;
}

static void
read_str(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    get_conftoken(CONF_STRING);
    val->v.s = newstralloc(val->v.s, tokenval.v.s);
}

static void
read_ident(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    get_conftoken(CONF_IDENT);
    val->v.s = newstralloc(val->v.s, tokenval.v.s);
}

static void
read_time(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    val_t__time(val) = get_time();
}

static void
read_size(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    val_t__size(val) = get_size(val->unit);
}

static void
read_bool(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    val_t__boolean(val) = get_bool();
}

static void
read_no_yes_all(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);
    val_t__int(val) = get_no_yes_all();
}

static void
read_compress(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    int serv, clie, none, fast, best, custom;
    int done;
    comp_t comp;

    ckseen(&val->seen);

    serv = clie = none = fast = best = custom  = 0;

    done = 0;
    do {
	get_conftoken(CONF_ANY);
	switch(tok) {
	case CONF_NONE:   none = 1; break;
	case CONF_FAST:   fast = 1; break;
	case CONF_BEST:   best = 1; break;
	case CONF_CLIENT: clie = 1; break;
	case CONF_SERVER: serv = 1; break;
	case CONF_CUSTOM: custom=1; break;
	case CONF_NL:     done = 1; break;
	case CONF_END:    done = 1; break;
	default:
	    done = 1;
	    serv = clie = 1; /* force an error */
	}
    } while(!done);

    if(serv + clie == 0) clie = 1;	/* default to client */
    if(none + fast + best + custom  == 0) fast = 1; /* default to fast */

    comp = -1;

    if(!serv && clie) {
	if(none && !fast && !best && !custom) comp = COMP_NONE;
	if(!none && fast && !best && !custom) comp = COMP_FAST;
	if(!none && !fast && best && !custom) comp = COMP_BEST;
	if(!none && !fast && !best && custom) comp = COMP_CUST;
    }

    if(serv && !clie) {
	if(none && !fast && !best && !custom) comp = COMP_NONE;
	if(!none && fast && !best && !custom) comp = COMP_SERVER_FAST;
	if(!none && !fast && best && !custom) comp = COMP_SERVER_BEST;
	if(!none && !fast && !best && custom) comp = COMP_SERVER_CUST;
    }

    if((int)comp == -1) {
	conf_parserror(_("NONE, CLIENT FAST, CLIENT BEST, CLIENT CUSTOM, SERVER FAST, SERVER BEST or SERVER CUSTOM expected"));
	comp = COMP_NONE;
    }

    val_t__compress(val) = (int)comp;
}

static void
read_encrypt(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
   encrypt_t encrypt;

   ckseen(&val->seen);

   get_conftoken(CONF_ANY);
   switch(tok) {
   case CONF_NONE:  
     encrypt = ENCRYPT_NONE; 
     break;

   case CONF_CLIENT:  
     encrypt = ENCRYPT_CUST;
     break;

   case CONF_SERVER: 
     encrypt = ENCRYPT_SERV_CUST;
     break;

   default:
     conf_parserror(_("NONE, CLIENT or SERVER expected"));
     encrypt = ENCRYPT_NONE;
     break;
   }

   val_t__encrypt(val) = (int)encrypt;
}

static void
read_holding(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
   dump_holdingdisk_t holding;

   ckseen(&val->seen);

   get_conftoken(CONF_ANY);
   switch(tok) {
   case CONF_NEVER:  
     holding = HOLD_NEVER; 
     break;

   case CONF_AUTO:  
     holding = HOLD_AUTO;
     break;

   case CONF_REQUIRED: 
     holding = HOLD_REQUIRED;
     break;

   default: /* can be a BOOLEAN */
     unget_conftoken();
     holding =  (dump_holdingdisk_t)get_bool();
     if (holding == 0)
	holding = HOLD_NEVER;
     else if (holding == 1 || holding == 2)
	holding = HOLD_AUTO;
     else
	conf_parserror(_("NEVER, AUTO or REQUIRED expected"));
     break;
   }

   val_t__holding(val) = (int)holding;
}

static void
read_estimatelist(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    estimatelist_t estimates = NULL;

    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    do {
	switch(tok) {
	case CONF_CLIENT:
	    estimates = g_slist_append(estimates, GINT_TO_POINTER(ES_CLIENT));
	    break;
	case CONF_SERVER:
	    estimates = g_slist_append(estimates, GINT_TO_POINTER(ES_SERVER));
	    break;
	case CONF_CALCSIZE:
	    estimates = g_slist_append(estimates, GINT_TO_POINTER(ES_CALCSIZE));
	    break;
	default:
	    conf_parserror(_("CLIENT, SERVER or CALCSIZE expected"));
	}
	get_conftoken(CONF_ANY);
	if (tok == CONF_NL)
	    break;
    } while (1);
    val_t__estimatelist(val) = estimates;
}

static void
read_strategy(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    int strat;

    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_SKIP:
	strat = DS_SKIP;
	break;
    case CONF_STANDARD:
	strat = DS_STANDARD;
	break;
    case CONF_NOFULL:
	strat = DS_NOFULL;
	break;
    case CONF_NOINC:
	strat = DS_NOINC;
	break;
    case CONF_HANOI:
	strat = DS_HANOI;
	break;
    case CONF_INCRONLY:
	strat = DS_INCRONLY;
	break;
    default:
	conf_parserror(_("dump strategy expected"));
	strat = DS_STANDARD;
    }
    val_t__strategy(val) = strat;
}

static void
read_taperalgo(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_FIRST:      val_t__taperalgo(val) = ALGO_FIRST;      break;
    case CONF_FIRSTFIT:   val_t__taperalgo(val) = ALGO_FIRSTFIT;   break;
    case CONF_LARGEST:    val_t__taperalgo(val) = ALGO_LARGEST;    break;
    case CONF_LARGESTFIT: val_t__taperalgo(val) = ALGO_LARGESTFIT; break;
    case CONF_SMALLEST:   val_t__taperalgo(val) = ALGO_SMALLEST;   break;
    case CONF_LAST:       val_t__taperalgo(val) = ALGO_LAST;       break;
    default:
	conf_parserror(_("FIRST, FIRSTFIT, LARGEST, LARGESTFIT, SMALLEST or LAST expected"));
    }
}

static void
read_send_amreport_on(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_ALL:     val_t__send_amreport(val) = SEND_AMREPORT_ALL;     break;
    case CONF_STRANGE: val_t__send_amreport(val) = SEND_AMREPORT_STRANGE; break;
    case CONF_ERROR:   val_t__send_amreport(val) = SEND_AMREPORT_ERROR;   break;
    case CONF_NEVER:   val_t__send_amreport(val) = SEND_AMREPORT_NEVER;   break;
    default:
	conf_parserror(_("ALL, STRANGE, ERROR or NEVER expected"));
    }
}

static void
read_data_path(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_AMANDA   : val_t__data_path(val) = DATA_PATH_AMANDA   ; break;
    case CONF_DIRECTTCP: val_t__data_path(val) = DATA_PATH_DIRECTTCP; break;
    default:
	conf_parserror(_("AMANDA or DIRECTTCP expected"));
    }
}

static void
read_priority(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    int pri;

    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_LOW: pri = 0; break;
    case CONF_MEDIUM: pri = 1; break;
    case CONF_HIGH: pri = 2; break;
    case CONF_INT: pri = tokenval.v.i; break;
    default:
	conf_parserror(_("LOW, MEDIUM, HIGH or integer expected"));
	pri = 0;
    }
    val_t__priority(val) = pri;
}

static void
read_rate(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    get_conftoken(CONF_REAL);
    val_t__rate(val)[0] = tokenval.v.r;
    val_t__rate(val)[1] = tokenval.v.r;
    val->seen = tokenval.seen;
    if(tokenval.v.r < 0) {
	conf_parserror(_("full compression rate must be >= 0"));
    }

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_NL:
	return;

    case CONF_END:
	return;

    case CONF_COMMA:
	break;

    default:
	unget_conftoken();
    }

    get_conftoken(CONF_REAL);
    val_t__rate(val)[1] = tokenval.v.r;
    if(tokenval.v.r < 0) {
	conf_parserror(_("incremental compression rate must be >= 0"));
    }
}

static void
read_exinclude(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    int file, got_one = 0;
    am_sl_t *exclude;
    int optional = 0;

    get_conftoken(CONF_ANY);
    if(tok == CONF_LIST) {
	file = 0;
	get_conftoken(CONF_ANY);
	exclude = val_t__exinclude(val).sl_list;
    }
    else {
	file = 1;
	if(tok == CONF_EFILE) get_conftoken(CONF_ANY);
	exclude = val_t__exinclude(val).sl_file;
    }
    ckseen(&val->seen);

    if(tok == CONF_OPTIONAL) {
	get_conftoken(CONF_ANY);
	optional = 1;
    }

    if(tok == CONF_APPEND) {
	get_conftoken(CONF_ANY);
    }
    else {
	free_sl(exclude);
	exclude = NULL;
    }

    while(tok == CONF_STRING) {
	exclude = append_sl(exclude, tokenval.v.s);
	got_one = 1;
	get_conftoken(CONF_ANY);
    }
    unget_conftoken();

    if(got_one == 0) { free_sl(exclude); exclude = NULL; }

    if (file == 0)
	val_t__exinclude(val).sl_list = exclude;
    else
	val_t__exinclude(val).sl_file = exclude;
    val_t__exinclude(val).optional = optional;
}

static void
read_intrange(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    get_conftoken(CONF_INT);
    val_t__intrange(val)[0] = tokenval.v.i;
    val_t__intrange(val)[1] = tokenval.v.i;
    val->seen = tokenval.seen;

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_NL:
	return;

    case CONF_END:
	return;

    case CONF_COMMA:
	break;

    default:
	unget_conftoken();
    }

    get_conftoken(CONF_INT);
    val_t__intrange(val)[1] = tokenval.v.i;
}

static void
read_property(
    conf_var_t *np G_GNUC_UNUSED,
    val_t      *val)
{
    char *key;
    gboolean set_seen = TRUE;
    property_t *property = malloc(sizeof(property_t));
    property_t *old_property;
    property->append = 0; 
    property->priority = 0; 
    property->values = NULL;

    get_conftoken(CONF_ANY);
    if (tok == CONF_PRIORITY) {
	property->priority = 1;
	get_conftoken(CONF_ANY);
    }
    if (tok == CONF_APPEND) {
	property->append = 1;
	get_conftoken(CONF_ANY);
    }
    if (tok != CONF_STRING) {
	conf_parserror(_("key expected"));
	return;
    }
    key = amandaify_property_name(tokenval.v.s);

    get_conftoken(CONF_ANY);
    if (tok == CONF_NL ||  tok == CONF_END) {
	g_hash_table_remove(val->v.proplist, key);
	unget_conftoken();
	return;
    }
    if (tok != CONF_STRING) {
	conf_parserror(_("value expected"));
	return;
    }

    if(val->seen.linenum == 0) {
	ckseen(&val->seen); // first property
    }

    old_property = g_hash_table_lookup(val->v.proplist, key);
    if (property->append) {
	/* old_property will be freed by g_hash_table_insert, so
	 * steal its values */
	if (old_property) {
	    if (old_property->priority)
		property->priority = 1;
	    property->values = old_property->values;
	    old_property->values = NULL;
	    set_seen = FALSE;
	}
    }
    while(tok == CONF_STRING) {
	property->values = g_slist_append(property->values,
					  strdup(tokenval.v.s));
	get_conftoken(CONF_ANY);
    }
    unget_conftoken();
    g_hash_table_insert(val->v.proplist, key, property);
    if (set_seen) {
	property->seen.linenum = 0;
	property->seen.filename = NULL;
	property->seen.block = NULL;
	ckseen(&property->seen);
    }
}


static void
read_dapplication(
    conf_var_t *np G_GNUC_UNUSED,
    val_t      *val)
{
    application_t *application;

    get_conftoken(CONF_ANY);
    if (tok == CONF_LBRACE) {
	current_line_num -= 1;
	application = read_application(vstralloc("custom(DUMPTYPE:",
						 dpcur.name, ")", ".",
						 anonymous_value(),NULL),
				       NULL, NULL, NULL);
	current_line_num -= 1;
    } else if (tok == CONF_STRING) {
	application = lookup_application(tokenval.v.s);
	if (application == NULL) {
	    conf_parserror(_("Unknown application named: %s"), tokenval.v.s);
	    return;
	}
    } else {
	conf_parserror(_("application name expected: %d %d"), tok, CONF_STRING);
	return;
    }
    amfree(val->v.s);
    val->v.s = stralloc(application->name);
    ckseen(&val->seen);
}

static void
read_dinteractivity(
    conf_var_t *np G_GNUC_UNUSED,
    val_t      *val)
{
    interactivity_t *interactivity;

    get_conftoken(CONF_ANY);
    if (tok == CONF_LBRACE) {
	current_line_num -= 1;
	interactivity = read_interactivity(vstralloc("custom(iv)", ".",
						     anonymous_value(),NULL),
				       NULL, NULL, NULL);
	current_line_num -= 1;
    } else if (tok == CONF_STRING) {
	interactivity = lookup_interactivity(tokenval.v.s);
	if (interactivity == NULL) {
	    conf_parserror(_("Unknown interactivity named: %s"), tokenval.v.s);
	    return;
	}
    } else {
	conf_parserror(_("interactivity name expected: %d %d"), tok, CONF_STRING);
	return;
    }
    amfree(val->v.s);
    val->v.s = stralloc(interactivity->name);
    ckseen(&val->seen);
}

static void
read_dtaperscan(
    conf_var_t *np G_GNUC_UNUSED,
    val_t      *val)
{
    taperscan_t *taperscan;

    get_conftoken(CONF_ANY);
    if (tok == CONF_LBRACE) {
	current_line_num -= 1;
	taperscan = read_taperscan(vstralloc("custom(ts)", ".",
				   anonymous_value(),NULL),
				   NULL, NULL, NULL);
	current_line_num -= 1;
    } else if (tok == CONF_STRING) {
	taperscan = lookup_taperscan(tokenval.v.s);
	if (taperscan == NULL) {
	    conf_parserror(_("Unknown taperscan named: %s"), tokenval.v.s);
	    return;
	}
    } else {
	conf_parserror(_("taperscan name expected: %d %d"), tok, CONF_STRING);
	return;
    }
    amfree(val->v.s);
    val->v.s = stralloc(taperscan->name);
    ckseen(&val->seen);
}

static void
read_dpp_script(
    conf_var_t *np G_GNUC_UNUSED,
    val_t      *val)
{
    pp_script_t *pp_script;
    get_conftoken(CONF_ANY);
    if (tok == CONF_LBRACE) {
	current_line_num -= 1;
	pp_script = read_pp_script(vstralloc("custom(DUMPTYPE:", dpcur.name,
					     ")", ".", anonymous_value(),NULL),
				   NULL, NULL, NULL);
	current_line_num -= 1;
	val->v.identlist = g_slist_insert_sorted(val->v.identlist,
			stralloc(pp_script->name), &compare_pp_script_order);
    } else if (tok == CONF_STRING || tok == CONF_IDENT) {
	while (tok == CONF_STRING || tok == CONF_IDENT) {
	    pp_script = lookup_pp_script(tokenval.v.s);
	    if (pp_script == NULL) {
		conf_parserror(_("Unknown pp_script named: %s"), tokenval.v.s);
		return;
	    }
    	    val->v.identlist = g_slist_insert_sorted(val->v.identlist,
			 stralloc(pp_script->name), &compare_pp_script_order);
	    get_conftoken(CONF_ANY);
	}
	unget_conftoken();
    } else {
	conf_parserror(_("pp_script name expected: %d %d"), tok, CONF_STRING);
	return;
    }
    ckseen(&val->seen);
}
static void
read_execute_on(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    val->v.i = 0;
    do {
	switch(tok) {
	case CONF_PRE_AMCHECK:         val->v.i |= EXECUTE_ON_PRE_AMCHECK;          break;
	case CONF_PRE_DLE_AMCHECK:     val->v.i |= EXECUTE_ON_PRE_DLE_AMCHECK;     break;
	case CONF_PRE_HOST_AMCHECK:    val->v.i |= EXECUTE_ON_PRE_HOST_AMCHECK;    break;
	case CONF_POST_DLE_AMCHECK:    val->v.i |= EXECUTE_ON_POST_DLE_AMCHECK;    break;
	case CONF_POST_HOST_AMCHECK:   val->v.i |= EXECUTE_ON_POST_HOST_AMCHECK;   break;
	case CONF_POST_AMCHECK:        val->v.i |= EXECUTE_ON_POST_AMCHECK;          break;
	case CONF_PRE_ESTIMATE:        val->v.i |= EXECUTE_ON_PRE_ESTIMATE;          break;
	case CONF_PRE_DLE_ESTIMATE:    val->v.i |= EXECUTE_ON_PRE_DLE_ESTIMATE;    break;
	case CONF_PRE_HOST_ESTIMATE:   val->v.i |= EXECUTE_ON_PRE_HOST_ESTIMATE;   break;
	case CONF_POST_DLE_ESTIMATE:   val->v.i |= EXECUTE_ON_POST_DLE_ESTIMATE;   break;
	case CONF_POST_HOST_ESTIMATE:  val->v.i |= EXECUTE_ON_POST_HOST_ESTIMATE;  break;
	case CONF_POST_ESTIMATE:       val->v.i |= EXECUTE_ON_POST_ESTIMATE;          break;
	case CONF_PRE_BACKUP:          val->v.i |= EXECUTE_ON_PRE_BACKUP;          break;
	case CONF_PRE_DLE_BACKUP:      val->v.i |= EXECUTE_ON_PRE_DLE_BACKUP;      break;
	case CONF_PRE_HOST_BACKUP:     val->v.i |= EXECUTE_ON_PRE_HOST_BACKUP;     break;
	case CONF_POST_BACKUP:         val->v.i |= EXECUTE_ON_POST_BACKUP;         break;
	case CONF_POST_DLE_BACKUP:     val->v.i |= EXECUTE_ON_POST_DLE_BACKUP;     break;
	case CONF_POST_HOST_BACKUP:    val->v.i |= EXECUTE_ON_POST_HOST_BACKUP;    break;
	case CONF_PRE_RECOVER:         val->v.i |= EXECUTE_ON_PRE_RECOVER;         break;
	case CONF_POST_RECOVER:        val->v.i |= EXECUTE_ON_POST_RECOVER;        break;
	case CONF_PRE_LEVEL_RECOVER:   val->v.i |= EXECUTE_ON_PRE_LEVEL_RECOVER;   break;
	case CONF_POST_LEVEL_RECOVER:  val->v.i |= EXECUTE_ON_POST_LEVEL_RECOVER;  break;
	case CONF_INTER_LEVEL_RECOVER: val->v.i |= EXECUTE_ON_INTER_LEVEL_RECOVER; break;
	default:
	conf_parserror(_("Execute-on expected"));
	}
	get_conftoken(CONF_ANY);
	if (tok != CONF_COMMA) {
	    unget_conftoken();
	    break;
	}
	get_conftoken(CONF_ANY);
    } while (1);
}

static void
read_execute_where(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_CLIENT:      val->v.i = ES_CLIENT;   break;
    case CONF_SERVER:      val->v.i = ES_SERVER;   break;
    default:
	conf_parserror(_("CLIENT or SERVER expected"));
    }
}

static void
read_int_or_str(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INT:
	amfree(val->v.s);
	val->v.s = g_strdup_printf("%d", tokenval.v.i);
	break;

    case CONF_SIZE:
	amfree(val->v.s);
	val->v.s = g_strdup_printf("%zu", tokenval.v.size);
	break;

    case CONF_INT64:
	amfree(val->v.s);
	val->v.s = g_strdup_printf("%jd", (intmax_t)tokenval.v.int64);
	break;

    case CONF_STRING:
	val->v.s = newstralloc(val->v.s, tokenval.v.s);
	break;
    default:
	conf_parserror(_("an integer or a quoted string is expected"));
    }
}

static void
read_autolabel(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    int data = 0;
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    if (tok == CONF_STRING) {
	data++;
	val->v.autolabel.template = newstralloc(val->v.autolabel.template,
						tokenval.v.s);
	get_conftoken(CONF_ANY);
    }
    val->v.autolabel.autolabel = 0;
    while (tok != CONF_NL && tok != CONF_END) {
	data++;
	if (tok == CONF_ANY_VOLUME)
	    val->v.autolabel.autolabel |= AL_OTHER_CONFIG | AL_NON_AMANDA |
					  AL_VOLUME_ERROR | AL_EMPTY;
	else if (tok == CONF_OTHER_CONFIG)
	    val->v.autolabel.autolabel |= AL_OTHER_CONFIG;
	else if (tok == CONF_NON_AMANDA)
	    val->v.autolabel.autolabel |= AL_NON_AMANDA;
	else if (tok == CONF_VOLUME_ERROR)
	    val->v.autolabel.autolabel |= AL_VOLUME_ERROR;
	else if (tok == CONF_EMPTY)
	    val->v.autolabel.autolabel |= AL_EMPTY;
	else {
	    conf_parserror(_("ANY, NEW-VOLUME, OTHER-CONFIG, NON-AMANDA, VOLUME-ERROR or EMPTY is expected"));
	}
	get_conftoken(CONF_ANY);
    }
    if (data == 0) {
	amfree(val->v.autolabel.template);
	val->v.autolabel.autolabel = 0;
    } else if (val->v.autolabel.autolabel == 0) {
	val->v.autolabel.autolabel = AL_VOLUME_ERROR | AL_EMPTY;
    }
}

static void
read_part_cache_type(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
   part_cache_type_t part_cache_type;

   ckseen(&val->seen);

   get_conftoken(CONF_ANY);
   switch(tok) {
   case CONF_NONE:
     part_cache_type = PART_CACHE_TYPE_NONE;
     break;

   case CONF_DISK:
     part_cache_type = PART_CACHE_TYPE_DISK;
     break;

   case CONF_MEMORY:
     part_cache_type = PART_CACHE_TYPE_MEMORY;
     break;

   default:
     conf_parserror(_("NONE, DISK or MEMORY expected"));
     part_cache_type = PART_CACHE_TYPE_NONE;
     break;
   }

   val_t__part_cache_type(val) = (int)part_cache_type;
}

static void
read_host_limit(
    conf_var_t *np G_GNUC_UNUSED,
    val_t *val)
{
    host_limit_t *rl = &val_t__host_limit(val);
    ckseen(&val->seen);

    rl->match_pats = NULL;
    rl->same_host  = FALSE;
    rl->server     = FALSE;

    while (1) {
	get_conftoken(CONF_ANY);
	switch(tok) {
	case CONF_STRING:
	    rl->match_pats = g_slist_append(rl->match_pats, g_strdup(tokenval.v.s));
	    break;
	case CONF_SAME_HOST:
	    rl->same_host = TRUE;
	    break;

	case CONF_SERVER:
	    rl->server = TRUE;
	    break;

	case CONF_NL:
	case CONF_END:
	    return;

        default:
	    conf_parserror("SAME-HOST or a string expected");
	    break;
	}
    }
}

/* get_* functions */

/* these functions use precompiler conditionals to skip useless size checks
 * when casting from one type to another.  SIZEOF_GINT64 is pretty simple to
 * calculate; the others are calculated by configure. */

#define SIZEOF_GINT64 8

static time_t
get_time(void)
{
    time_t hhmm;

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INT:
#if SIZEOF_TIME_T < SIZEOF_INT
	if ((gint64)tokenval.v.i >= (gint64)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.i;
	break;

    case CONF_SIZE:
#if SIZEOF_TIME_T < SIZEOF_SSIZE_T
	if ((gint64)tokenval.v.size >= (gint64)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.size;
	break;

    case CONF_INT64:
#if SIZEOF_TIME_T < SIZEOF_GINT64
	if ((gint64)tokenval.v.int64 >= (gint64)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.int64;
	break;

    case CONF_AMINFINITY:
	hhmm = TIME_MAX;
	break;

    default:
	conf_parserror(_("a time is expected"));
	hhmm = 0;
	break;
    }
    return hhmm;
}

static int
get_int(
    confunit_t unit)
{
    int val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INT:
	val = tokenval.v.i;
	break;

    case CONF_SIZE:
#if SIZEOF_INT < SIZEOF_SSIZE_T
	if ((gint64)tokenval.v.size > (gint64)INT_MAX)
	    conf_parserror(_("value too large"));
	if ((gint64)tokenval.v.size < (gint64)INT_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (int)tokenval.v.size;
	break;

    case CONF_INT64:
#if SIZEOF_INT < SIZEOF_GINT64
	if (tokenval.v.int64 > (gint64)INT_MAX)
	    conf_parserror(_("value too large"));
	if (tokenval.v.int64 < (gint64)INT_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (int)tokenval.v.int64;
	break;

    case CONF_AMINFINITY:
	val = INT_MAX;
	break;

    default:
	conf_parserror(_("an integer is expected"));
	val = 0;
	break;
    }

    /* get multiplier, if any */
    val = get_multiplier(val, unit);

    keytable = save_kt;
    return val;
}

static ssize_t
get_size(
    confunit_t unit)
{
    ssize_t val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_SIZE:
	val = tokenval.v.size;
	break;

    case CONF_INT:
#if SIZEOF_SIZE_T < SIZEOF_INT
	if ((gint64)tokenval.v.i > (gint64)SSIZE_MAX)
	    conf_parserror(_("value too large"));
	if ((gint64)tokenval.v.i < (gint64)SSIZE_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (ssize_t)tokenval.v.i;
	break;

    case CONF_INT64:
#if SIZEOF_SIZE_T < SIZEOF_GINT64
	if (tokenval.v.int64 > (gint64)SSIZE_MAX)
	    conf_parserror(_("value too large"));
	if (tokenval.v.int64 < (gint64)SSIZE_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (ssize_t)tokenval.v.int64;
	break;

    case CONF_AMINFINITY:
	val = (ssize_t)SSIZE_MAX;
	break;

    default:
	conf_parserror(_("an integer is expected"));
	val = 0;
	break;
    }

    /* get multiplier, if any */
    val = get_multiplier(val, unit);

    keytable = save_kt;
    return val;
}

static gint64
get_int64(
    confunit_t unit)
{
    gint64 val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_INT:
	val = (gint64)tokenval.v.i;
	break;

    case CONF_SIZE:
	val = (gint64)tokenval.v.size;
	break;

    case CONF_INT64:
	val = tokenval.v.int64;
	break;

    case CONF_AMINFINITY:
	val = G_MAXINT64;
	break;

    default:
	conf_parserror(_("an integer is expected"));
	val = 0;
	break;
    }

    /* get multiplier, if any */
    val = get_multiplier(val, unit);

    keytable = save_kt;

    return val;
}

gint64
get_multiplier(
    gint64 val,
    confunit_t unit)
{
    /* get multiplier, if any */
    get_conftoken(CONF_ANY);

    if (tok == CONF_MULT1 && unit == CONF_UNIT_K) {
	val /= 1024;
    } else if (tok == CONF_NL || tok == CONF_MULT1 ||
	(tok == CONF_MULT1K && unit == CONF_UNIT_K)) {
	val *= 1;	/* multiply by one */
    } else if (tok == CONF_MULT7) {
	if (val > G_MAXINT64/7 || val < ((gint64)G_MININT64)/7)
	    conf_parserror(_("value too large"));
	val *= 7;
    } else if (tok == CONF_MULT1K ||
	       (tok == CONF_MULT1M && unit == CONF_UNIT_K)) {
	if (val > G_MAXINT64/1024 || val < ((gint64)G_MININT64)/1024)
	    conf_parserror(_("value too large"));
	val *= 1024;
    } else if (tok == CONF_MULT1M ||
	       (tok == CONF_MULT1G && unit == CONF_UNIT_K)) {
	if (val > G_MAXINT64/(1024*1024) || val < ((gint64)G_MININT64)/(1024*1024))
	    conf_parserror(_("value too large"));
	val *= 1024*1024;
    } else if (tok == CONF_MULT1G ||
	       (tok == CONF_MULT1T && unit == CONF_UNIT_K)) {
	if (val > G_MAXINT64/(1024*1024*1024) || val < ((gint64)G_MININT64)/(1024*1024*1024))
	    conf_parserror(_("value too large"));
	val *= 1024*1024*1024;
    } else if (tok == CONF_MULT1T) {
	if (val > G_MAXINT64/(1024*1024*1024*1024LL) || val < ((gint64)G_MININT64)/(1024*1024*1024*1024LL))
	    conf_parserror(_("value too large"));
	val *= 1024*1024*1024*1024LL;
    } else {
	val *= 1;
	unget_conftoken();
    }

    return val;
}

static int
get_bool(void)
{
    int val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = bool_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_INT:
	if (tokenval.v.i != 0)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_SIZE:
	if (tokenval.v.size != (size_t)0)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_INT64:
	if (tokenval.v.int64 != (gint64)0)
	    val = 1;
	else
	    val = 0;
	break;

    case CONF_ATRUE:
	val = 1;
	break;

    case CONF_AFALSE:
	val = 0;
	break;

    case CONF_NL:
	unget_conftoken();
	val = 2; /* no argument - most likely TRUE */
	break;
    default:
	unget_conftoken();
	val = 3; /* a bad argument - most likely TRUE */
	conf_parserror(_("YES, NO, TRUE, FALSE, ON, OFF, 0, 1 expected"));
	break;
    }

    keytable = save_kt;
    return val;
}

static int
get_no_yes_all(void)
{
    int val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = no_yes_all_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_INT:
	val = tokenval.v.i;
	break;

    case CONF_SIZE:
	val = tokenval.v.size;
	break;

    case CONF_INT64:
	val = tokenval.v.int64;
	break;

    case CONF_ALL:
	val = 2;
	break;

    case CONF_ATRUE:
	val = 1;
	break;

    case CONF_AFALSE:
	val = 0;
	break;

    case CONF_NL:
	unget_conftoken();
	val = 3; /* no argument - most likely TRUE */
	break;
    default:
	unget_conftoken();
	val = 3; /* a bad argument - most likely TRUE */
	conf_parserror(_("%d: YES, NO, ALL, TRUE, FALSE, ON, OFF, 0, 1, 2 expected"), tok);
	break;
    }

    if (val > 2 || val < 0)
	val = 1;
    keytable = save_kt;
    return val;
}

void
ckseen(
    seen_t *seen)
{
    if (seen->linenum && !allow_overwrites && current_line_num != -2) {
	conf_parserror(_("duplicate parameter; previous definition %s:%d"),
		seen->filename, seen->linenum);
    }
    seen->block = current_block;
    seen->filename = current_filename;
    seen->linenum = current_line_num;
}

/* Validation functions */

static void
validate_nonnegative(
    struct conf_var_s *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val_t__int(val) < 0)
	    conf_parserror(_("%s must be nonnegative"), get_token_name(np->token));
	break;
    case CONFTYPE_INT64:
	if(val_t__int64(val) < 0)
	    conf_parserror(_("%s must be nonnegative"), get_token_name(np->token));
	break;
    case CONFTYPE_SIZE:
	if(val_t__size(val) < 0)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    default:
	conf_parserror(_("validate_nonnegative invalid type %d\n"), val->type);
    }
}

static void
validate_non_zero(
    struct conf_var_s *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val_t__int(val) == 0)
	    conf_parserror(_("%s must not be 0"), get_token_name(np->token));
	break;
    case CONFTYPE_INT64:
	if(val_t__int64(val) == 0)
	    conf_parserror(_("%s must not be 0"), get_token_name(np->token));
	break;
    case CONFTYPE_TIME:
	if(val_t__time(val) == 0)
	    conf_parserror(_("%s must not be 0"), get_token_name(np->token));
	break;
    case CONFTYPE_SIZE:
	if(val_t__size(val) == 0)
	    conf_parserror(_("%s must not be 0"), get_token_name(np->token));
	break;
    default:
	conf_parserror(_("validate_non_zero invalid type %d\n"), val->type);
    }
}

static void
validate_positive(
    struct conf_var_s *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val_t__int(val) < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_INT64:
	if(val_t__int64(val) < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_TIME:
	if(val_t__time(val) < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_SIZE:
	if(val_t__size(val) < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    default:
	conf_parserror(_("validate_positive invalid type %d\n"), val->type);
    }
}

static void
validate_runspercycle(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__int(val) < -1)
	conf_parserror(_("runspercycle must be >= -1"));
}

static void
validate_bumppercent(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__int(val) < 0 || val_t__int(val) > 100)
	conf_parserror(_("bumppercent must be between 0 and 100"));
}

static void
validate_inparallel(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__int(val) < 1 || val_t__int(val) >MAX_DUMPERS)
	conf_parserror(_("inparallel must be between 1 and MAX_DUMPERS (%d)"),
		       MAX_DUMPERS);
}

static void
validate_bumpmult(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__real(val) < 0.999) {
	conf_parserror(_("bumpmult must one or more"));
    }
}

static void
validate_displayunit(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val G_GNUC_UNUSED)
{
    char *s = val_t__str(val);
    if (strlen(s) == 1) {
	switch (s[0]) {
	    case 'K':
	    case 'M':
	    case 'G':
	    case 'T':
		return; /* all good */

	    /* lower-case values should get folded to upper case */
	    case 'k':
	    case 'm':
	    case 'g':
	    case 't':
		s[0] = toupper(s[0]);
		return;

	    default:	/* bad */
		break;
	}
    }
    conf_parserror(_("displayunit must be k,m,g or t."));
}

static void
validate_reserve(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__int(val) < 0 || val_t__int(val) > 100)
	conf_parserror(_("reserve must be between 0 and 100"));
}

static void
validate_use(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    val_t__int64(val) = am_floor(val_t__int64(val), DISK_BLOCK_KB);
}

static void
validate_chunksize(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    /* NOTE: this function modifies the target value (rounding) */
    if(val_t__int64(val) == 0) {
	val_t__int64(val) = ((G_MAXINT64 / 1024) - (2 * DISK_BLOCK_KB));
    }
    else if(val_t__int64(val) < 0) {
	conf_parserror(_("Negative chunksize (%lld) is no longer supported"), (long long)val_t__int64(val));
    }
    val_t__int64(val) = am_floor(val_t__int64(val), (gint64)DISK_BLOCK_KB);
    if (val_t__int64(val) < 2*DISK_BLOCK_KB) {
	conf_parserror("chunksize must be at least %dkb", 2*DISK_BLOCK_KB);
    }
}

static void
validate_blocksize(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__size(val) < DISK_BLOCK_KB) {
	conf_parserror(_("Tape blocksize must be at least %d KBytes"),
		  DISK_BLOCK_KB);
    }
}

static void
validate_debug(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    if(val_t__int(val) < 0 || val_t__int(val) > 9) {
	conf_parserror(_("Debug must be between 0 and 9"));
    }
}

static void
validate_port_range(
    val_t        *val,
    int		 smallest,
    int		 largest)
{
    int i;
    /* check both values are in range */
    for (i = 0; i < 2; i++) {
        if(val_t__intrange(val)[0] < smallest || val_t__intrange(val)[0] > largest) {
	    conf_parserror(_("portrange must be in the range %d to %d, inclusive"), smallest, largest);
	}
     }

    /* and check they're in the right order and not equal */
    if (val_t__intrange(val)[0] > val_t__intrange(val)[1]) {
	conf_parserror(_("portranges must be in order from low to high"));
    }
}

static void
validate_reserved_port_range(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    validate_port_range(val, 1, IPPORT_RESERVED-1);
}

static void
validate_unreserved_port_range(
    struct conf_var_s *np G_GNUC_UNUSED,
    val_t        *val)
{
    validate_port_range(val, IPPORT_RESERVED, 65535);
}

/*
 * Validate tmpdir, the directory must exist and be writable by the user.
 */

static void validate_tmpdir(conf_var_t *var G_GNUC_UNUSED, val_t *value)
{
    struct stat stat_buf;
    gchar *tmpdir = val_t_to_str(value);

    if (stat(tmpdir, &stat_buf)) {
	conf_parserror(_("invalid TMPDIR: directory '%s': %s."),
		      tmpdir, strerror(errno));
    } else if (!S_ISDIR(stat_buf.st_mode)) {
	conf_parserror(_("invalid TMPDIR: '%s' is not a directory."),
		      tmpdir);
    } else {
	gchar *dir = g_strconcat(tmpdir, "/.", NULL);
	if (access(dir, R_OK|W_OK) == -1) {
	    conf_parserror(_("invalid TMPDIR: '%s': can not read/write: %s."),
			   tmpdir, strerror(errno));
	}
	g_free(dir);
    }
}

/*
 * Initialization Implementation
 */

cfgerr_level_t
config_init(
    config_init_flags flags,
    char *arg_config_name)
{
    if (!(flags & CONFIG_INIT_OVERLAY)) {
	/* Clear out anything that's already in there */
	config_uninit();

	/* and set everything to default values */
	init_defaults();

	allow_overwrites = FALSE;
    } else {
	if (!config_initialized) {
	    error(_("Attempt to overlay configuration with no existing configuration"));
	    /* NOTREACHED */
	}

	allow_overwrites = TRUE;
    }

    /* store away our client-ness for later reference */
    config_client = flags & CONFIG_INIT_CLIENT;

    /* if we're using an explicit name, but the name is '.', then we're using the
     * current directory */
    if ((flags & CONFIG_INIT_EXPLICIT_NAME) && arg_config_name) {
	if (0 == strcmp(arg_config_name, "."))
	    flags = (flags & (~CONFIG_INIT_EXPLICIT_NAME)) | CONFIG_INIT_USE_CWD;
    }

    if ((flags & CONFIG_INIT_EXPLICIT_NAME) && arg_config_name) {
	config_name = newstralloc(config_name, arg_config_name);
	config_dir = newvstralloc(config_dir, CONFIG_DIR, "/", arg_config_name, NULL);
    } else if (flags & CONFIG_INIT_USE_CWD) {
        char * cwd;
        
        cwd = get_original_cwd();
	if (!cwd) {
	    /* (this isn't a config error, so it's always fatal) */
	    error(_("Cannot determine current working directory"));
	    /* NOTREACHED */
	}

	config_dir = stralloc2(cwd, "/");
	if ((config_name = strrchr(cwd, '/')) != NULL) {
	    config_name = stralloc(config_name + 1);
	}

    } else if (flags & CONFIG_INIT_CLIENT) {
	amfree(config_name);
	config_dir = newstralloc(config_dir, CONFIG_DIR);
    } else {
	/* ok, then, we won't read anything (for e.g., amrestore) */
	amfree(config_name);
	amfree(config_dir);
    }

    /* setup for apply_config_overrides */
    if (flags & CONFIG_INIT_CLIENT) {
	keytable = client_keytab;
	parsetable = client_var;
    } else {
	keytable = server_keytab;
	parsetable = server_var;
    }

    if (config_overrides) {
	int i;
	for (i = 0; i < config_overrides->n_used; i++) {
	    config_overrides->ovr[i].applied = FALSE;
	}
    }

    /* apply config overrides to default setting */
    apply_config_overrides(config_overrides, NULL);

    /* If we have a config_dir, we can try reading something */
    if (config_dir) {
	if (flags & CONFIG_INIT_CLIENT) {
	    config_filename = newvstralloc(config_filename, config_dir, "/amanda-client.conf", NULL);
	} else {
	    config_filename = newvstralloc(config_filename, config_dir, "/amanda.conf", NULL);
	}

	read_conffile(config_filename,
		flags & CONFIG_INIT_CLIENT,
		flags & CONFIG_INIT_CLIENT);
    } else {
	amfree(config_filename);
    }

    /* apply config overrides to default setting */
    apply_config_overrides(config_overrides, NULL);

    if (config_overrides) {
	int i;
	for (i = 0; i < config_overrides->n_used; i++) {
	    if (config_overrides->ovr[i].applied == FALSE) {
		conf_parserror(_("unknown parameter '%s'"),
			       config_overrides->ovr[i].key);
	    }
	}
    }

    update_derived_values(flags & CONFIG_INIT_CLIENT);

    return cfgerr_level;
}

void
config_uninit(void)
{
    GSList           *hp;
    holdingdisk_t    *hd;
    dumptype_t       *dp, *dpnext;
    tapetype_t       *tp, *tpnext;
    interface_t      *ip, *ipnext;
    application_t *ap, *apnext;
    pp_script_t   *pp, *ppnext;
    device_config_t *dc, *dcnext;
    changer_config_t *cc, *ccnext;
    interactivity_t  *iv, *ivnext;
    taperscan_t      *ts, *tsnext;
    int               i;

    if (!config_initialized) return;

    for(hp=holdinglist; hp != NULL; hp = hp->next) {
	hd = hp->data;
	amfree(hd->name);
	for(i=0; i<HOLDING_HOLDING; i++) {
	   free_val_t(&hd->value[i]);
	}
    }
    slist_free_full(holdinglist, g_free);
    holdinglist = NULL;

    for(dp=dumplist; dp != NULL; dp = dpnext) {
	amfree(dp->name);
	for(i=0; i<DUMPTYPE_DUMPTYPE; i++) {
	   free_val_t(&dp->value[i]);
	}
	dpnext = dp->next;
	amfree(dp);
    }
    dumplist = NULL;

    for(tp=tapelist; tp != NULL; tp = tpnext) {
	amfree(tp->name);
	for(i=0; i<TAPETYPE_TAPETYPE; i++) {
	   free_val_t(&tp->value[i]);
	}
	tpnext = tp->next;
	amfree(tp);
    }
    tapelist = NULL;

    for(ip=interface_list; ip != NULL; ip = ipnext) {
	amfree(ip->name);
	for(i=0; i<INTER_INTER; i++) {
	   free_val_t(&ip->value[i]);
	}
	ipnext = ip->next;
	amfree(ip);
    }
    interface_list = NULL;

    for(ap=application_list; ap != NULL; ap = apnext) {
	amfree(ap->name);
	for(i=0; i<APPLICATION_APPLICATION; i++) {
	   free_val_t(&ap->value[i]);
	}
	apnext = ap->next;
	amfree(ap);
    }
    application_list = NULL;

    for(pp=pp_script_list; pp != NULL; pp = ppnext) {
	amfree(pp->name);
	for(i=0; i<PP_SCRIPT_PP_SCRIPT; i++) {
	   free_val_t(&pp->value[i]);
	}
	ppnext = pp->next;
	amfree(pp);
    }
    pp_script_list = NULL;

    for(dc=device_config_list; dc != NULL; dc = dcnext) {
	amfree(dc->name);
	for(i=0; i<DEVICE_CONFIG_DEVICE_CONFIG; i++) {
	   free_val_t(&dc->value[i]);
	}
	dcnext = dc->next;
	amfree(dc);
    }
    device_config_list = NULL;

    for(cc=changer_config_list; cc != NULL; cc = ccnext) {
	amfree(cc->name);
	for(i=0; i<CHANGER_CONFIG_CHANGER_CONFIG; i++) {
	   free_val_t(&cc->value[i]);
	}
	ccnext = cc->next;
	amfree(cc);
    }
    changer_config_list = NULL;

    for(iv=interactivity_list; iv != NULL; iv = ivnext) {
	amfree(iv->name);
	for(i=0; i<INTERACTIVITY_INTERACTIVITY; i++) {
	   free_val_t(&iv->value[i]);
	}
	ivnext = iv->next;
	amfree(iv);
    }
    interactivity_list = NULL;

    for(ts=taperscan_list; ts != NULL; ts = tsnext) {
	amfree(ts->name);
	for(i=0; i<TAPERSCAN_TAPERSCAN; i++) {
	   free_val_t(&ts->value[i]);
	}
	tsnext = ts->next;
	amfree(ts);
    }
    taperscan_list = NULL;

    for(i=0; i<CNF_CNF; i++)
	free_val_t(&conf_data[i]);

    if (config_overrides) {
	free_config_overrides(config_overrides);
	config_overrides = NULL;
    }

    amfree(config_name);
    amfree(config_dir);
    amfree(config_filename);

    slist_free_full(seen_filenames, g_free);
    seen_filenames = NULL;

    config_client = FALSE;

    config_clear_errors();
    config_initialized = FALSE;
}

static void
init_defaults(
    void)
{
    assert(!config_initialized);

    /* defaults for exported variables */
    conf_init_str(&conf_data[CNF_ORG], DEFAULT_CONFIG);
    conf_init_str(&conf_data[CNF_CONF], DEFAULT_CONFIG);
    conf_init_str(&conf_data[CNF_AMDUMP_SERVER], DEFAULT_SERVER);
    conf_init_str(&conf_data[CNF_INDEX_SERVER], DEFAULT_SERVER);
    conf_init_str(&conf_data[CNF_TAPE_SERVER], DEFAULT_TAPE_SERVER);
    conf_init_str(&conf_data[CNF_AUTH], "bsdtcp");
    conf_init_str(&conf_data[CNF_SSH_KEYS], "");
    conf_init_str(&conf_data[CNF_AMANDAD_PATH], "");
    conf_init_str(&conf_data[CNF_CLIENT_USERNAME], "");
    conf_init_str(&conf_data[CNF_CLIENT_PORT], "");
    conf_init_str(&conf_data[CNF_GNUTAR_LIST_DIR], GNUTAR_LISTED_INCREMENTAL_DIR);
    conf_init_str(&conf_data[CNF_AMANDATES], DEFAULT_AMANDATES_FILE);
    conf_init_str(&conf_data[CNF_MAILTO], "");
    conf_init_str(&conf_data[CNF_DUMPUSER], CLIENT_LOGIN);
    conf_init_str(&conf_data[CNF_TAPEDEV], DEFAULT_TAPE_DEVICE);
    conf_init_proplist(&conf_data[CNF_DEVICE_PROPERTY]);
    conf_init_proplist(&conf_data[CNF_PROPERTY]);
    conf_init_str(&conf_data[CNF_CHANGERDEV], NULL);
    conf_init_str(&conf_data[CNF_CHANGERFILE]             , "changer");
    conf_init_str   (&conf_data[CNF_LABELSTR]             , ".*");
    conf_init_str   (&conf_data[CNF_TAPELIST]             , "tapelist");
    conf_init_str   (&conf_data[CNF_DISKFILE]             , "disklist");
    conf_init_str   (&conf_data[CNF_INFOFILE]             , "/usr/adm/amanda/curinfo");
    conf_init_str   (&conf_data[CNF_LOGDIR]               , "/usr/adm/amanda");
    conf_init_str   (&conf_data[CNF_INDEXDIR]             , "/usr/adm/amanda/index");
    conf_init_ident    (&conf_data[CNF_TAPETYPE]             , "DEFAULT_TAPE");
    conf_init_identlist(&conf_data[CNF_HOLDINGDISK]          , NULL);
    conf_init_int      (&conf_data[CNF_DUMPCYCLE]            , CONF_UNIT_NONE, 10);
    conf_init_int      (&conf_data[CNF_RUNSPERCYCLE]         , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_TAPECYCLE]            , CONF_UNIT_NONE, 15);
    conf_init_int      (&conf_data[CNF_NETUSAGE]             , CONF_UNIT_K   , 80000);
    conf_init_int      (&conf_data[CNF_INPARALLEL]           , CONF_UNIT_NONE, 10);
    conf_init_str   (&conf_data[CNF_DUMPORDER]            , "ttt");
    conf_init_int      (&conf_data[CNF_BUMPPERCENT]          , CONF_UNIT_NONE, 0);
    conf_init_int64    (&conf_data[CNF_BUMPSIZE]             , CONF_UNIT_K   , (gint64)10*1024);
    conf_init_real     (&conf_data[CNF_BUMPMULT]             , 1.5);
    conf_init_int      (&conf_data[CNF_BUMPDAYS]             , CONF_UNIT_NONE, 2);
    conf_init_str   (&conf_data[CNF_TPCHANGER]            , "");
    conf_init_int      (&conf_data[CNF_RUNTAPES]             , CONF_UNIT_NONE, 1);
    conf_init_int      (&conf_data[CNF_MAXDUMPS]             , CONF_UNIT_NONE, 1);
    conf_init_int      (&conf_data[CNF_MAX_DLE_BY_VOLUME]    , CONF_UNIT_NONE, 1000000000);
    conf_init_int      (&conf_data[CNF_ETIMEOUT]             , CONF_UNIT_NONE, 300);
    conf_init_int      (&conf_data[CNF_DTIMEOUT]             , CONF_UNIT_NONE, 1800);
    conf_init_int      (&conf_data[CNF_CTIMEOUT]             , CONF_UNIT_NONE, 30);
    conf_init_size     (&conf_data[CNF_DEVICE_OUTPUT_BUFFER_SIZE], CONF_UNIT_NONE, 40*32768);
    conf_init_str   (&conf_data[CNF_PRINTER]              , "");
    conf_init_str   (&conf_data[CNF_MAILER]               , DEFAULT_MAILER);
    conf_init_no_yes_all(&conf_data[CNF_AUTOFLUSH]            , 0);
    conf_init_int      (&conf_data[CNF_RESERVE]              , CONF_UNIT_NONE, 100);
    conf_init_int64    (&conf_data[CNF_MAXDUMPSIZE]          , CONF_UNIT_K   , (gint64)-1);
    conf_init_str   (&conf_data[CNF_COLUMNSPEC]           , "");
    conf_init_bool     (&conf_data[CNF_AMRECOVER_DO_FSF]     , 1);
    conf_init_str   (&conf_data[CNF_AMRECOVER_CHANGER]    , "");
    conf_init_bool     (&conf_data[CNF_AMRECOVER_CHECK_LABEL], 1);
    conf_init_taperalgo(&conf_data[CNF_TAPERALGO]            , 0);
    conf_init_int      (&conf_data[CNF_TAPER_PARALLEL_WRITE]     , CONF_UNIT_NONE, 1);
    conf_init_int      (&conf_data[CNF_FLUSH_THRESHOLD_DUMPED]   , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_FLUSH_THRESHOLD_SCHEDULED], CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_TAPERFLUSH]               , CONF_UNIT_NONE, 0);
    conf_init_str   (&conf_data[CNF_DISPLAYUNIT]          , "k");
    conf_init_str   (&conf_data[CNF_KRB5KEYTAB]           , "/.amanda-v5-keytab");
    conf_init_str   (&conf_data[CNF_KRB5PRINCIPAL]        , "service/amanda");
    conf_init_str   (&conf_data[CNF_LABEL_NEW_TAPES]      , "");
    conf_init_bool     (&conf_data[CNF_EJECT_VOLUME]         , 0);
    conf_init_str      (&conf_data[CNF_TMPDIR]               , "");
    conf_init_bool     (&conf_data[CNF_USETIMESTAMPS]        , 1);
    conf_init_int      (&conf_data[CNF_CONNECT_TRIES]        , CONF_UNIT_NONE, 3);
    conf_init_int      (&conf_data[CNF_REP_TRIES]            , CONF_UNIT_NONE, 5);
    conf_init_int      (&conf_data[CNF_REQ_TRIES]            , CONF_UNIT_NONE, 3);
    conf_init_int      (&conf_data[CNF_DEBUG_DAYS]           , CONF_UNIT_NONE, AMANDA_DEBUG_DAYS);
    conf_init_int      (&conf_data[CNF_DEBUG_AMANDAD]        , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_RECOVERY]       , CONF_UNIT_NONE, 1);
    conf_init_int      (&conf_data[CNF_DEBUG_AMIDXTAPED]     , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AMINDEXD]       , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AMRECOVER]      , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AUTH]           , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_EVENT]          , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_HOLDING]        , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_PROTOCOL]       , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_PLANNER]        , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_DRIVER]         , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_DUMPER]         , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_CHUNKER]        , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_TAPER]          , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_SELFCHECK]      , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_SENDSIZE]       , CONF_UNIT_NONE, 0);
    conf_init_int      (&conf_data[CNF_DEBUG_SENDBACKUP]     , CONF_UNIT_NONE, 0);
#ifdef UDPPORTRANGE
    conf_init_intrange (&conf_data[CNF_RESERVED_UDP_PORT]    , UDPPORTRANGE);
#else
    conf_init_intrange (&conf_data[CNF_RESERVED_UDP_PORT]    , 512, IPPORT_RESERVED-1);
#endif
#ifdef LOW_TCPPORTRANGE
    conf_init_intrange (&conf_data[CNF_RESERVED_TCP_PORT]    , LOW_TCPPORTRANGE);
#else
    conf_init_intrange (&conf_data[CNF_RESERVED_TCP_PORT]    , 512, IPPORT_RESERVED-1);
#endif
#ifdef TCPPORTRANGE
    conf_init_intrange (&conf_data[CNF_UNRESERVED_TCP_PORT]  , TCPPORTRANGE);
#else
    conf_init_intrange (&conf_data[CNF_UNRESERVED_TCP_PORT]  , IPPORT_RESERVED, 65535);
#endif
    conf_init_send_amreport (&conf_data[CNF_SEND_AMREPORT_ON], SEND_AMREPORT_ALL);
    conf_init_autolabel(&conf_data[CNF_AUTOLABEL]);
    conf_init_str(&conf_data[CNF_META_AUTOLABEL], NULL);
    conf_init_host_limit(&conf_data[CNF_RECOVERY_LIMIT]);
    conf_init_str(&conf_data[CNF_INTERACTIVITY], NULL);
    conf_init_str(&conf_data[CNF_TAPERSCAN], NULL);

    /* reset internal variables */
    config_clear_errors();
    cfgerr_level = CFGERR_OK;
    allow_overwrites = 0;
    token_pushed = 0;

    /* create some predefined dumptypes for backwards compatability */
    init_dumptype_defaults();
    dpcur.name = stralloc("NO-COMPRESS");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_COMPRESS]);
    val_t__compress(&dpcur.value[DUMPTYPE_COMPRESS]) = COMP_NONE;
    val_t__seen(&dpcur.value[DUMPTYPE_COMPRESS]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("COMPRESS-FAST");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_COMPRESS]);
    val_t__compress(&dpcur.value[DUMPTYPE_COMPRESS]) = COMP_FAST;
    val_t__seen(&dpcur.value[DUMPTYPE_COMPRESS]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("COMPRESS-BEST");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_COMPRESS]);
    val_t__compress(&dpcur.value[DUMPTYPE_COMPRESS]) = COMP_BEST;
    val_t__seen(&dpcur.value[DUMPTYPE_COMPRESS]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("COMPRESS-CUST");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_COMPRESS]);
    val_t__compress(&dpcur.value[DUMPTYPE_COMPRESS]) = COMP_CUST;
    val_t__seen(&dpcur.value[DUMPTYPE_COMPRESS]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("SRVCOMPRESS");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_COMPRESS]);
    val_t__compress(&dpcur.value[DUMPTYPE_COMPRESS]) = COMP_SERVER_FAST;
    val_t__seen(&dpcur.value[DUMPTYPE_COMPRESS]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("BSD-AUTH");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_AUTH]);
    val_t__str(&dpcur.value[DUMPTYPE_AUTH]) = stralloc("BSD");
    val_t__seen(&dpcur.value[DUMPTYPE_AUTH]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("BSDTCP-AUTH");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_AUTH]);
    val_t__str(&dpcur.value[DUMPTYPE_AUTH]) = stralloc("BSDTCP");
    val_t__seen(&dpcur.value[DUMPTYPE_AUTH]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("NO-RECORD");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_RECORD]);
    val_t__int(&dpcur.value[DUMPTYPE_RECORD]) = 0;
    val_t__seen(&dpcur.value[DUMPTYPE_RECORD]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("NO-HOLD");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_HOLDINGDISK]);
    val_t__holding(&dpcur.value[DUMPTYPE_HOLDINGDISK]) = HOLD_NEVER;
    val_t__seen(&dpcur.value[DUMPTYPE_HOLDINGDISK]).linenum = -1;
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("NO-FULL");
    dpcur.seen.linenum = -1;
    free_val_t(&dpcur.value[DUMPTYPE_STRATEGY]);
    val_t__strategy(&dpcur.value[DUMPTYPE_STRATEGY]) = DS_NOFULL;
    val_t__seen(&dpcur.value[DUMPTYPE_STRATEGY]).linenum = -1;
    save_dumptype();

    /* And we're initialized! */
    config_initialized = 1;
}

char **
get_config_options(
    int first)
{
    char             **config_options;
    char	     **config_option;
    int		     n_config_overrides = 0;
    int		     i;

    if (config_overrides)
	n_config_overrides = config_overrides->n_used;

    config_options = alloc((first+n_config_overrides+1)*SIZEOF(char *));
    config_option = config_options + first;

    for (i = 0; i < n_config_overrides; i++) {
	char *key = config_overrides->ovr[i].key;
	char *value = config_overrides->ovr[i].value;
	*config_option = vstralloc("-o", key, "=", value, NULL);
	config_option++;
    }

    *config_option = NULL; /* add terminating sentinel */

    return config_options;
}

static void
update_derived_values(
    gboolean is_client)
{
    interface_t   *ip;
    identlist_t    il;
    holdingdisk_t *hd;

    if (!is_client) {
	/* Add a 'default' interface if one doesn't already exist */
	if (!(ip = lookup_interface("default"))) {
	    init_interface_defaults();
	    ifcur.name = stralloc("default");
	    ifcur.seen = val_t__seen(getconf(CNF_NETUSAGE));
	    save_interface();

	    ip = lookup_interface("default");
	}

	/* .. and set its maxusage from 'netusage' */
	if (!interface_seen(ip, INTER_MAXUSAGE)) {
	    val_t *v;

	    v = interface_getconf(ip, INTER_COMMENT);
	    free_val_t(v);
	    val_t__str(v) = stralloc(_("implicit from NETUSAGE"));
	    val_t__seen(v) = val_t__seen(getconf(CNF_NETUSAGE));

	    v = interface_getconf(ip, INTER_MAXUSAGE);
	    free_val_t(v);
	    val_t__int(v) = getconf_int(CNF_NETUSAGE);
	    val_t__seen(v) = val_t__seen(getconf(CNF_NETUSAGE));
	}

	/* Check the tapetype is defined */
	if (lookup_tapetype(getconf_str(CNF_TAPETYPE)) == NULL) {
	    /* Create a default tapetype so that other code has
	     * something to refer to, but don't pretend it's real */
	    if (!getconf_seen(CNF_TAPETYPE) &&
		strcmp(getconf_str(CNF_TAPETYPE), "DEFAULT_TAPE") == 0 &&
		!lookup_tapetype("DEFAULT_TAPE")) {
		init_tapetype_defaults();
		tpcur.name = stralloc("DEFAULT_TAPE");
		tpcur.seen = val_t__seen(getconf(CNF_TAPETYPE));
		save_tapetype();
	    } else {
		conf_parserror(_("tapetype %s is not defined"),
			       getconf_str(CNF_TAPETYPE));
	    }
	}

	/* Check the holdingdisk are defined */
	for (il = getconf_identlist(CNF_HOLDINGDISK);
		 il != NULL; il = il->next) {
	    hd = lookup_holdingdisk(il->data);
	    if (!hd) {
		conf_parserror(_("holdingdisk %s is not defined"),
			       (char *)il->data);
	    }
	}

	if ((getconf_seen(CNF_LABEL_NEW_TAPES) > 0 &&
	     getconf_seen(CNF_AUTOLABEL) > 0)  ||
	    (getconf_seen(CNF_LABEL_NEW_TAPES) < 0 &&
	     getconf_seen(CNF_AUTOLABEL) < 0)) {
		conf_parserror(_("Can't specify both label-new-tapes and autolabel"));
	}
	if ((getconf_seen(CNF_LABEL_NEW_TAPES) != 0 &&
	     getconf_seen(CNF_AUTOLABEL) == 0) ||
	    (getconf_seen(CNF_LABEL_NEW_TAPES) < 0 &&
	     getconf_seen(CNF_AUTOLABEL) >= 0)) {
	    autolabel_t *autolabel = &(conf_data[CNF_AUTOLABEL].v.autolabel);
	    autolabel->template = g_strdup(getconf_str(CNF_LABEL_NEW_TAPES));
	    if (!autolabel->template || autolabel->template == '\0') {
		autolabel->template = NULL;
		autolabel->autolabel = 0;
	    } else {
		autolabel->autolabel = AL_VOLUME_ERROR | AL_EMPTY;
	    }
	}
    }

    /* fill in the debug_* values */
    debug_amandad    = getconf_int(CNF_DEBUG_AMANDAD);
    debug_recovery   = getconf_int(CNF_DEBUG_RECOVERY);
    debug_amidxtaped = getconf_int(CNF_DEBUG_AMIDXTAPED);
    debug_amindexd   = getconf_int(CNF_DEBUG_AMINDEXD);
    debug_amrecover  = getconf_int(CNF_DEBUG_AMRECOVER);
    debug_auth       = getconf_int(CNF_DEBUG_AUTH);
    debug_event      = getconf_int(CNF_DEBUG_EVENT);
    debug_holding    = getconf_int(CNF_DEBUG_HOLDING);
    debug_protocol   = getconf_int(CNF_DEBUG_PROTOCOL);
    debug_planner    = getconf_int(CNF_DEBUG_PLANNER);
    debug_driver     = getconf_int(CNF_DEBUG_DRIVER);
    debug_dumper     = getconf_int(CNF_DEBUG_DUMPER);
    debug_chunker    = getconf_int(CNF_DEBUG_CHUNKER);
    debug_taper      = getconf_int(CNF_DEBUG_TAPER);
    debug_selfcheck  = getconf_int(CNF_DEBUG_SELFCHECK);
    debug_sendsize   = getconf_int(CNF_DEBUG_SENDSIZE);
    debug_sendbackup = getconf_int(CNF_DEBUG_SENDBACKUP);

    /* And finally, display unit */
    switch (getconf_str(CNF_DISPLAYUNIT)[0]) {
	case 'k':
	case 'K':
	    unit_divisor = 1;
	    break;

	case 'm':
	case 'M':
	    unit_divisor = 1024;
	    break;

	case 'g':
	case 'G':
	    unit_divisor = 1024*1024;
	    break;

	case 't':
	case 'T':
	    unit_divisor = 1024*1024*1024;
	    break;

	default:
	    error(_("Invalid displayunit missed by validate_displayunit"));
	    /* NOTREACHED */
    }
}

static void
conf_init_int(
    val_t *val,
    confunit_t unit,
    int    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_INT;
    val->unit = unit;
    val_t__int(val) = i;
}

static void
conf_init_int64(
    val_t *val,
    confunit_t unit,
    gint64   l)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_INT64;
    val->unit = unit;
    val_t__int64(val) = l;
}

static void
conf_init_real(
    val_t  *val,
    float r)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_REAL;
    val->unit = CONF_UNIT_NONE;
    val_t__real(val) = r;
}

static void
conf_init_str(
    val_t *val,
    char  *s)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_STR;
    val->unit = CONF_UNIT_NONE;
    if(s)
	val->v.s = stralloc(s);
    else
	val->v.s = NULL;
}

static void
conf_init_ident(
    val_t *val,
    char  *s)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_IDENT;
    val->unit = CONF_UNIT_NONE;
    if(s)
	val->v.s = stralloc(s);
    else
	val->v.s = NULL;
}

static void
conf_init_identlist(
    val_t *val,
    char  *s)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_IDENTLIST;
    val->unit = CONF_UNIT_NONE;
    val->v.identlist = NULL;
    if (s)
	val->v.identlist = g_slist_append(val->v.identlist, stralloc(s));
}

static void
conf_init_time(
    val_t *val,
    time_t   t)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_TIME;
    val->unit = CONF_UNIT_NONE;
    val_t__time(val) = t;
}

static void
conf_init_size(
    val_t *val,
    confunit_t unit,
    ssize_t   sz)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_SIZE;
    val->unit = unit;
    val_t__size(val) = sz;
}

static void
conf_init_bool(
    val_t *val,
    int    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_BOOLEAN;
    val->unit = CONF_UNIT_NONE;
    val_t__boolean(val) = i;
}

static void
conf_init_no_yes_all(
    val_t *val,
    int    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_NO_YES_ALL;
    val->unit = CONF_UNIT_NONE;
    val_t__int(val) = i;
}

static void
conf_init_compress(
    val_t *val,
    comp_t    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_COMPRESS;
    val->unit = CONF_UNIT_NONE;
    val_t__compress(val) = (int)i;
}

static void
conf_init_encrypt(
    val_t *val,
    encrypt_t    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_ENCRYPT;
    val->unit = CONF_UNIT_NONE;
    val_t__encrypt(val) = (int)i;
}

static void
conf_init_part_cache_type(
    val_t *val,
    part_cache_type_t    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_PART_CACHE_TYPE;
    val->unit = CONF_UNIT_NONE;
    val_t__part_cache_type(val) = (int)i;
}

static void
conf_init_host_limit(
    val_t *val)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_HOST_LIMIT;
    val->unit = CONF_UNIT_NONE;
    val_t__host_limit(val).match_pats = NULL;
    val_t__host_limit(val).same_host  = FALSE;
    val_t__host_limit(val).server     = FALSE;
}

static void
conf_init_host_limit_server(
    val_t *val)
{
    conf_init_host_limit(val);
    val_t__host_limit(val).server = TRUE;
}

static void
conf_init_data_path(
    val_t *val,
    data_path_t    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_DATA_PATH;
    val->unit = CONF_UNIT_NONE;
    val_t__data_path(val) = (int)i;
}

static void
conf_init_holding(
    val_t              *val,
    dump_holdingdisk_t  i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_HOLDING;
    val->unit = CONF_UNIT_NONE;
    val_t__holding(val) = (int)i;
}

static void
conf_init_estimatelist(
    val_t *val,
    estimate_t    i)
{
    GSList *estimates = NULL;
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_ESTIMATELIST;
    val->unit = CONF_UNIT_NONE;
    estimates = g_slist_append(estimates, GINT_TO_POINTER(i));
    val_t__estimatelist(val) = estimates;
}

static void
conf_init_strategy(
    val_t *val,
    strategy_t    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->unit = CONF_UNIT_NONE;
    val->type = CONFTYPE_STRATEGY;
    val_t__strategy(val) = i;
}

static void
conf_init_taperalgo(
    val_t *val,
    taperalgo_t    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_TAPERALGO;
    val->unit = CONF_UNIT_NONE;
    val_t__taperalgo(val) = i;
}

static void
conf_init_priority(
    val_t *val,
    int    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_PRIORITY;
    val->unit = CONF_UNIT_NONE;
    val_t__priority(val) = i;
}

static void
conf_init_rate(
    val_t  *val,
    float r1,
    float r2)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_RATE;
    val->unit = CONF_UNIT_NONE;
    val_t__rate(val)[0] = r1;
    val_t__rate(val)[1] = r2;
}

static void
conf_init_exinclude(
    val_t *val)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_EXINCLUDE;
    val->unit = CONF_UNIT_NONE;
    val_t__exinclude(val).optional = 0;
    val_t__exinclude(val).sl_list = NULL;
    val_t__exinclude(val).sl_file = NULL;
}

static void
conf_init_intrange(
    val_t *val,
    int    i1,
    int    i2)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_INTRANGE;
    val->unit = CONF_UNIT_NONE;
    val_t__intrange(val)[0] = i1;
    val_t__intrange(val)[1] = i2;
}

static void
conf_init_autolabel(
    val_t *val) {
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_AUTOLABEL;
    val->unit = CONF_UNIT_NONE;
    val->v.autolabel.template = NULL;
    val->v.autolabel.autolabel = 0;
}

void
free_property_t(
    gpointer p)
{
    property_t *propery = (property_t *)p;
    slist_free_full(propery->values, g_free);
    amfree(propery);
}

static void
conf_init_proplist(
    val_t *val)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_PROPLIST;
    val->unit = CONF_UNIT_NONE;
    val_t__proplist(val) =
        g_hash_table_new_full(g_str_amanda_hash, g_str_amanda_equal,
			      &g_free, &free_property_t);
}

static void
conf_init_execute_on(
    val_t *val,
    int    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_EXECUTE_ON;
    val->unit = CONF_UNIT_NONE;
    val->v.i = i;
}

static void
conf_init_execute_where(
    val_t *val,
    int    i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_EXECUTE_WHERE;
    val->unit = CONF_UNIT_NONE;
    val->v.i = i;
}

static void
conf_init_send_amreport(
    val_t *val,
    send_amreport_t i)
{
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_SEND_AMREPORT_ON;
    val->unit = CONF_UNIT_NONE;
    val->v.i = i;
}

static void conf_init_application(val_t *val) {
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
    val->type = CONFTYPE_APPLICATION;
    val->unit = CONF_UNIT_NONE;
    val->v.s = NULL;
}


/*
 * Config access implementation
 */

val_t *
getconf(confparm_key key)
{
    assert(key < CNF_CNF);
    return &conf_data[key];
}

GSList *
getconf_list(
    char *listname)
{
    tapetype_t *tp;
    dumptype_t *dp;
    interface_t *ip;
    holdingdisk_t *hd;
    GSList        *hp;
    application_t *ap;
    pp_script_t   *pp;
    device_config_t *dc;
    changer_config_t *cc;
    interactivity_t  *iv;
    taperscan_t      *ts;
    GSList *rv = NULL;

    if (strcasecmp(listname,"tapetype") == 0) {
	for(tp = tapelist; tp != NULL; tp=tp->next) {
	    rv = g_slist_append(rv, tp->name);
	}
    } else if (strcasecmp(listname,"dumptype") == 0) {
	for(dp = dumplist; dp != NULL; dp=dp->next) {
	    rv = g_slist_append(rv, dp->name);
	}
    } else if (strcasecmp(listname,"holdingdisk") == 0) {
	for(hp = holdinglist; hp != NULL; hp=hp->next) {
	    hd = hp->data;
	    rv = g_slist_append(rv, hd->name);
	}
    } else if (strcasecmp(listname,"interface") == 0) {
	for(ip = interface_list; ip != NULL; ip=ip->next) {
	    rv = g_slist_append(rv, ip->name);
	}
    } else if (strcasecmp(listname,"application_tool") == 0
	    || strcasecmp(listname,"application-tool") == 0
	    || strcasecmp(listname,"application") == 0) {
	for(ap = application_list; ap != NULL; ap=ap->next) {
	    rv = g_slist_append(rv, ap->name);
	}
    } else if (strcasecmp(listname,"script_tool") == 0
	    || strcasecmp(listname,"script-tool") == 0
	    || strcasecmp(listname,"script") == 0) {
	for(pp = pp_script_list; pp != NULL; pp=pp->next) {
	    rv = g_slist_append(rv, pp->name);
	}
    } else if (strcasecmp(listname,"device") == 0) {
	for(dc = device_config_list; dc != NULL; dc=dc->next) {
	    rv = g_slist_append(rv, dc->name);
	}
    } else if (strcasecmp(listname,"changer") == 0) {
	for(cc = changer_config_list; cc != NULL; cc=cc->next) {
	    rv = g_slist_append(rv, cc->name);
	}
    } else if (strcasecmp(listname,"interactivity") == 0) {
	for(iv = interactivity_list; iv != NULL; iv=iv->next) {
	    rv = g_slist_append(rv, iv->name);
	}
    } else if (strcasecmp(listname,"taperscan") == 0) {
	for(ts = taperscan_list; ts != NULL; ts=ts->next) {
	    rv = g_slist_append(rv, ts->name);
	}
    }
    return rv;
}

val_t *
getconf_byname(
    char *key)
{
    val_t *rv = NULL;

    if (!parm_key_info(key, NULL, &rv))
	return NULL;

    return rv;
}

tapetype_t *
lookup_tapetype(
    char *str)
{
    tapetype_t *p;

    for(p = tapelist; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

val_t *
tapetype_getconf(
    tapetype_t *ttyp,
    tapetype_key key)
{
    assert(ttyp != NULL);
    assert(key < TAPETYPE_TAPETYPE);
    return &ttyp->value[key];
}

static void
validate_program(
    conf_var_t *np G_GNUC_UNUSED,
    val_t        *val)
{
    if (strcmp(val->v.s, "DUMP") != 0 &&
    	strcmp(val->v.s, "GNUTAR") != 0 &&
    	strcmp(val->v.s, "STAR") != 0 &&
    	strcmp(val->v.s, "APPLICATION") != 0)
       conf_parserror("program must be \"DUMP\", \"GNUTAR\", \"STAR\" or \"APPLICATION\"");
}

static void
validate_dump_limit(
    conf_var_t *np G_GNUC_UNUSED,
    val_t        *val)
{
    if (val->v.host_limit.match_pats) {
	conf_parserror("dump-limit can't specify hostname");
    }
}

char *
tapetype_name(
    tapetype_t *ttyp)
{
    assert(ttyp != NULL);
    return ttyp->name;
}

dumptype_t *
lookup_dumptype(
    char *str)
{
    dumptype_t *p;

    for(p = dumplist; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

val_t *
dumptype_getconf(
    dumptype_t *dtyp,
    dumptype_key key)
{
    assert(dtyp != NULL);
    assert(key < DUMPTYPE_DUMPTYPE);
    return &dtyp->value[key];
}

char *
dumptype_name(
    dumptype_t *dtyp)
{
    assert(dtyp != NULL);
    return dtyp->name;
}

interface_t *
lookup_interface(
    char *str)
{
    interface_t *p;

    for(p = interface_list; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

val_t *
interface_getconf(
    interface_t *iface,
    interface_key key)
{
    assert(iface != NULL);
    assert(key < INTER_INTER);
    return &iface->value[key];
}

char *
interface_name(
    interface_t *iface)
{
    assert(iface != NULL);
    return iface->name;
}

holdingdisk_t *
lookup_holdingdisk(
    char *str)
{
    GSList        *hp;
    holdingdisk_t *hd;

    for (hp = holdinglist; hp != NULL; hp = hp->next) {
	hd = hp->data;
	if (strcasecmp(hd->name, str) == 0) return hd;
    }
    return NULL;
}

GSList *
getconf_holdingdisks(
    void)
{
    return holdinglist;
}

val_t *
holdingdisk_getconf(
    holdingdisk_t *hdisk,
    holdingdisk_key key)
{
    assert(hdisk != NULL);
    assert(key < HOLDING_HOLDING);
    return &hdisk->value[key];
}

char *
holdingdisk_name(
    holdingdisk_t *hdisk)
{
    assert(hdisk != NULL);
    return hdisk->name;
}

application_t *
lookup_application(
    char *str)
{
    application_t *p;

    for(p = application_list; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

val_t *
application_getconf(
    application_t *ap,
    application_key key)
{
    assert(ap != NULL);
    assert(key < APPLICATION_APPLICATION);
    return &ap->value[key];
}

char *
application_name(
    application_t *ap)
{
    assert(ap != NULL);
    return ap->name;
}

interactivity_t *
lookup_interactivity(
    char *str)
{
    interactivity_t *p;

    for(p = interactivity_list; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

val_t *
interactivity_getconf(
    interactivity_t *iv,
    interactivity_key key)
{
    assert(iv != NULL);
    assert(key < INTERACTIVITY_INTERACTIVITY);
    return &iv->value[key];
}

char *
interactivity_name(
    interactivity_t *iv)
{
    assert(iv != NULL);
    return iv->name;
}

taperscan_t *
lookup_taperscan(
    char *str)
{
    taperscan_t *p;

    for(p = taperscan_list; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

val_t *
taperscan_getconf(
    taperscan_t *ts,
    taperscan_key key)
{
    assert(ts != NULL);
    assert(key < TAPERSCAN_TAPERSCAN);
    return &ts->value[key];
}

char *
taperscan_name(
    taperscan_t *ts)
{
    assert(ts != NULL);
    return ts->name;
}

pp_script_t *
lookup_pp_script(
    char *str)
{
    pp_script_t *pps;

    for(pps = pp_script_list; pps != NULL; pps = pps->next) {
	if(strcasecmp(pps->name, str) == 0) return pps;
    }
    return NULL;
}

val_t *
pp_script_getconf(
    pp_script_t *pps,
    pp_script_key key)
{
    assert(pps != NULL);
    assert(key < PP_SCRIPT_PP_SCRIPT);
    return &pps->value[key];
}

char *
pp_script_name(
    pp_script_t *pps)
{
    assert(pps != NULL);
    return pps->name;
}

device_config_t *
lookup_device_config(
    char *str)
{
    device_config_t *devconf;

    for(devconf = device_config_list; devconf != NULL; devconf = devconf->next) {
	if(strcasecmp(devconf->name, str) == 0) return devconf;
    }
    return NULL;
}

val_t *
device_config_getconf(
    device_config_t *devconf,
    device_config_key key)
{
    assert(devconf != NULL);
    assert(key < DEVICE_CONFIG_DEVICE_CONFIG);
    return &devconf->value[key];
}

char *
device_config_name(
    device_config_t *devconf)
{
    assert(devconf != NULL);
    return devconf->name;
}

changer_config_t *
lookup_changer_config(
    char *str)
{
    changer_config_t *devconf;

    for(devconf = changer_config_list; devconf != NULL; devconf = devconf->next) {
	if(strcasecmp(devconf->name, str) == 0) return devconf;
    }
    return NULL;
}

val_t *
changer_config_getconf(
    changer_config_t *devconf,
    changer_config_key key)
{
    assert(devconf != NULL);
    assert(key < CHANGER_CONFIG_CHANGER_CONFIG);
    return &devconf->value[key];
}

char *
changer_config_name(
    changer_config_t *devconf)
{
    assert(devconf != NULL);
    return devconf->name;
}

long int
getconf_unit_divisor(void)
{
    return unit_divisor;
}

/*
 * Command-line Handling Implementation
 */

config_overrides_t *
new_config_overrides(
    int size_estimate)
{
    config_overrides_t *co;

    if (size_estimate <= 0)
	size_estimate = 10;

    co = alloc(sizeof(*co));
    co->ovr = alloc(sizeof(*co->ovr) * size_estimate);
    co->n_allocated = size_estimate;
    co->n_used = 0;

    return co;
}

void
free_config_overrides(
    config_overrides_t *co)
{
    int i;

    if (!co) return;
    for (i = 0; i < co->n_used; i++) {
	amfree(co->ovr[i].key);
	amfree(co->ovr[i].value);
    }
    amfree(co->ovr);
    amfree(co);
}

void add_config_override(
    config_overrides_t *co,
    char *key,
    char *value)
{
    /* reallocate if necessary */
    if (co->n_used == co->n_allocated) {
	co->n_allocated *= 2;
	co->ovr = realloc(co->ovr, co->n_allocated * sizeof(*co->ovr));
	if (!co->ovr) {
	    error(_("Cannot realloc; out of memory"));
	    /* NOTREACHED */
	}
    }

    co->ovr[co->n_used].key = stralloc(key);
    co->ovr[co->n_used].value = stralloc(value);
    co->n_used++;
}

void
add_config_override_opt(
    config_overrides_t *co,
    char *optarg)
{
    char *value;
    assert(optarg != NULL);

    value = strchr(optarg, '=');
    if (value == NULL) {
	error(_("Must specify a value for %s."), optarg);
	/* NOTREACHED */
    }

    *value = '\0';
    add_config_override(co, optarg, value+1);
    *value = '=';
}

config_overrides_t *
extract_commandline_config_overrides(
    int *argc,
    char ***argv)
{
    int i, j, moveup;
    config_overrides_t *co = new_config_overrides(*argc/2);

    i = 0;
    while (i<*argc) {
	if(strncmp((*argv)[i],"-o",2) == 0) {
	    if(strlen((*argv)[i]) > 2) {
		add_config_override_opt(co, (*argv)[i]+2);
		moveup = 1;
	    }
	    else {
		if (i+1 >= *argc) error(_("expect something after -o"));
		add_config_override_opt(co, (*argv)[i+1]);
		moveup = 2;
	    }

	    /* move up remaining argment array */
	    for (j = i; j+moveup<*argc; j++) {
		(*argv)[j] = (*argv)[j+moveup];
	    }
	    *argc -= moveup;
	} else {
	    i++;
	}
    }

    return co;
}

void
set_config_overrides(
    config_overrides_t *co)
{
    int i;

    config_overrides = co;

    for (i = 0; i < co->n_used; i++) {
	g_debug("config_overrides: %s %s", co->ovr[i].key, co->ovr[i].value);
    }

    return;
}

static cfgerr_level_t
apply_config_overrides(
    config_overrides_t *co,
    char *key_ovr)
{
    int i;

    if(!co) return cfgerr_level;
    assert(keytable != NULL);
    assert(parsetable != NULL);

    for (i = 0; i < co->n_used; i++) {
	char *key = co->ovr[i].key;
	char *value = co->ovr[i].value;
	val_t *key_val;
	conf_var_t *key_parm;

	if (key_ovr && strncasecmp(key_ovr, key, strlen(key_ovr)) != 0) {
	    continue;
	}

	if (!parm_key_info(key, &key_parm, &key_val)) {
	    /* not an error, only default config is loaded */
	    continue;
	}

	/* now set up a fake line and use the relevant read_function to
	 * parse it.  This is sneaky! */
	if (key_parm->type == CONFTYPE_STR) {
	    current_line = quote_string_always(value);
	} else {
	    current_line = stralloc(value);
	}

	current_char = current_line;
	token_pushed = 0;
	current_line_num = -2;
	allow_overwrites = 1;
        co->ovr[i].applied = TRUE;

	key_parm->read_function(key_parm, key_val);
	if ((key_parm)->validate_function)
	    key_parm->validate_function(key_parm, key_val);

	amfree(current_line);
	current_char = NULL;
    }

    return cfgerr_level;
}

/*
 * val_t Management Implementation
 */

int
val_t_to_int(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_INT) {
	error(_("val_t_to_int: val.type is not CONFTYPE_INT"));
	/*NOTREACHED*/
    }
    return val_t__int(val);
}

gint64
val_t_to_int64(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_INT64) {
	error(_("val_t_to_int64: val.type is not CONFTYPE_INT64"));
	/*NOTREACHED*/
    }
    return val_t__int64(val);
}

float
val_t_to_real(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_REAL) {
	error(_("val_t_to_real: val.type is not CONFTYPE_REAL"));
	/*NOTREACHED*/
    }
    return val_t__real(val);
}

char *
val_t_to_str(
    val_t *val)
{
    assert(config_initialized);
    /* support CONFTYPE_IDENT, too */
    if (val->type != CONFTYPE_STR && val->type != CONFTYPE_IDENT) {
	error(_("val_t_to_str: val.type is not CONFTYPE_STR nor CONFTYPE_IDENT"));
	/*NOTREACHED*/
    }
    return val_t__str(val);
}

char *
val_t_to_ident(
    val_t *val)
{
    assert(config_initialized);
    /* support CONFTYPE_STR, too */
    if (val->type != CONFTYPE_STR && val->type != CONFTYPE_IDENT) {
	error(_("val_t_to_ident: val.type is not CONFTYPE_IDENT nor CONFTYPE_STR"));
	/*NOTREACHED*/
    }
    return val_t__str(val);
}

identlist_t
val_t_to_identlist(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_IDENTLIST) {
	error(_("val_t_to_ident: val.type is not CONFTYPE_IDENTLIST"));
	/*NOTREACHED*/
    }
    return val_t__identlist(val);
}

time_t
val_t_to_time(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_TIME) {
	error(_("val_t_to_time: val.type is not CONFTYPE_TIME"));
	/*NOTREACHED*/
    }
    return val_t__time(val);
}

ssize_t
val_t_to_size(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_SIZE) {
	error(_("val_t_to_size: val.type is not CONFTYPE_SIZE"));
	/*NOTREACHED*/
    }
    return val_t__size(val);
}

int
val_t_to_boolean(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_BOOLEAN) {
	error(_("val_t_to_bool: val.type is not CONFTYPE_BOOLEAN"));
	/*NOTREACHED*/
    }
    return val_t__boolean(val);
}

int
val_t_to_no_yes_all(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_NO_YES_ALL) {
	error(_("val_t_to_no_yes_all: val.type is not CONFTYPE_NO_YES_ALL"));
	/*NOTREACHED*/
    }
    return val_t__no_yes_all(val);
}

comp_t
val_t_to_compress(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_COMPRESS) {
	error(_("val_t_to_compress: val.type is not CONFTYPE_COMPRESS"));
	/*NOTREACHED*/
    }
    return val_t__compress(val);
}

encrypt_t
val_t_to_encrypt(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_ENCRYPT) {
	error(_("val_t_to_encrypt: val.type is not CONFTYPE_ENCRYPT"));
	/*NOTREACHED*/
    }
    return val_t__encrypt(val);
}

part_cache_type_t
val_t_to_part_cache_type(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_PART_CACHE_TYPE) {
	error(_("val_t_to_part_cache_type: val.type is not CONFTYPE_PART_CACHE_TYPE"));
	/*NOTREACHED*/
    }
    return val_t__part_cache_type(val);
}

host_limit_t *
val_t_to_host_limit(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_HOST_LIMIT) {
	error(_("val_t_to_host_limit: val.type is not CONFTYPE_HOST_LIMIT"));
	/*NOTREACHED*/
    }
    return &val_t__host_limit(val);
}

dump_holdingdisk_t
val_t_to_holding(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_HOLDING) {
	error(_("val_t_to_hold: val.type is not CONFTYPE_HOLDING"));
	/*NOTREACHED*/
    }
    return val_t__holding(val);
}

estimatelist_t
val_t_to_estimatelist(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_ESTIMATELIST) {
	error(_("val_t_to_estimatelist: val.type is not CONFTYPE_ESTIMATELIST"));
	/*NOTREACHED*/
    }
    return val_t__estimatelist(val);
}

strategy_t
val_t_to_strategy(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_STRATEGY) {
	error(_("val_t_to_strategy: val.type is not CONFTYPE_STRATEGY"));
	/*NOTREACHED*/
    }
    return val_t__strategy(val);
}

taperalgo_t
val_t_to_taperalgo(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_TAPERALGO) {
	error(_("val_t_to_taperalgo: val.type is not CONFTYPE_TAPERALGO"));
	/*NOTREACHED*/
    }
    return val_t__taperalgo(val);
}

send_amreport_t
val_t_to_send_amreport(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_SEND_AMREPORT_ON) {
	error(_("val_t_to_send_amreport: val.type is not CONFTYPE_SEND_AMREPORT_ON"));
	/*NOTREACHED*/
    }
    return val_t__send_amreport(val);
}

data_path_t
val_t_to_data_path(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_DATA_PATH) {
	error(_("val_t_to_data_path: val.type is not CONFTYPE_DATA_PATH"));
	/*NOTREACHED*/
    }
    return val_t__data_path(val);
}

int
val_t_to_priority(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_PRIORITY) {
	error(_("val_t_to_priority: val.type is not CONFTYPE_PRIORITY"));
	/*NOTREACHED*/
    }
    return val_t__priority(val);
}

float *
val_t_to_rate(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_RATE) {
	error(_("val_t_to_rate: val.type is not CONFTYPE_RATE"));
	/*NOTREACHED*/
    }
    return val_t__rate(val);
}

exinclude_t
val_t_to_exinclude(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_EXINCLUDE) {
	error(_("val_t_to_exinclude: val.type is not CONFTYPE_EXINCLUDE"));
	/*NOTREACHED*/
    }
    return val_t__exinclude(val);
}


int *
val_t_to_intrange(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_INTRANGE) {
	error(_("val_t_to_intrange: val.type is not CONFTYPE_INTRANGE"));
	/*NOTREACHED*/
    }
    return val_t__intrange(val);
}

proplist_t
val_t_to_proplist(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_PROPLIST) {
	error(_("val_t_to_proplist: val.type is not CONFTYPE_PROPLIST"));
	/*NOTREACHED*/
    }
    return val_t__proplist(val);
}

autolabel_t
val_t_to_autolabel(
    val_t *val)
{
    assert(config_initialized);
    if (val->type != CONFTYPE_AUTOLABEL) {
	error(_("val_t_to_autolabel: val.type is not CONFTYPE_AUTOLABEL"));
	/*NOTREACHED*/
    }
    return val_t__autolabel(val);
}

static void
merge_val_t(
    val_t *valdst,
    val_t *valsrc)
{
    if (valsrc->type == CONFTYPE_PROPLIST) {
	if (valsrc->v.proplist) {
	    if (valdst->v.proplist == NULL ||
		g_hash_table_size(valdst->v.proplist) == 0) {
		valdst->seen.block = current_block;
		valdst->seen.filename = current_filename;
		valdst->seen.linenum = current_line_num;
	    }
	    if (valdst->v.proplist == NULL) {
	        valdst->v.proplist = g_hash_table_new_full(g_str_amanda_hash,
							   g_str_amanda_equal,
							   &g_free,
							   &free_property_t);
	        g_hash_table_foreach(valsrc->v.proplist,
				     &copy_proplist_foreach_fn,
				     valdst->v.proplist);
	    } else {
		g_hash_table_foreach(valsrc->v.proplist,
				     &merge_proplist_foreach_fn,
				     valdst->v.proplist);
	    }
	}
    } else if (valsrc->type == CONFTYPE_IDENTLIST) {
	if (valsrc->v.identlist) {
	    identlist_t il;
	    for (il = valsrc->v.identlist; il != NULL; il = il->next) {
		 valdst->v.identlist = g_slist_append(valdst->v.identlist,
						   stralloc((char *)il->data));
	    }
	}
    } else {
	free_val_t(valdst);
	copy_val_t(valdst, valsrc);
    }
}

static void
copy_val_t(
    val_t *valdst,
    val_t *valsrc)
{
    GSList *ia;

    if(valsrc->seen.linenum) {
	valdst->type = valsrc->type;
	valdst->seen = valsrc->seen;
	switch(valsrc->type) {
	case CONFTYPE_INT:
	case CONFTYPE_BOOLEAN:
	case CONFTYPE_NO_YES_ALL:
	case CONFTYPE_COMPRESS:
	case CONFTYPE_ENCRYPT:
	case CONFTYPE_HOLDING:
	case CONFTYPE_EXECUTE_ON:
	case CONFTYPE_EXECUTE_WHERE:
	case CONFTYPE_SEND_AMREPORT_ON:
	case CONFTYPE_DATA_PATH:
	case CONFTYPE_STRATEGY:
	case CONFTYPE_TAPERALGO:
	case CONFTYPE_PRIORITY:
	case CONFTYPE_PART_CACHE_TYPE:
	    valdst->v.i = valsrc->v.i;
	    break;

	case CONFTYPE_SIZE:
	    valdst->v.size = valsrc->v.size;
	    break;

	case CONFTYPE_INT64:
	    valdst->v.int64 = valsrc->v.int64;
	    break;

	case CONFTYPE_REAL:
	    valdst->v.r = valsrc->v.r;
	    break;

	case CONFTYPE_RATE:
	    valdst->v.rate[0] = valsrc->v.rate[0];
	    valdst->v.rate[1] = valsrc->v.rate[1];
	    break;

	case CONFTYPE_IDENT:
	case CONFTYPE_STR:
	    valdst->v.s = stralloc(valsrc->v.s);
	    break;

	case CONFTYPE_IDENTLIST:
	    valdst->v.identlist = NULL;
	    for (ia = valsrc->v.identlist; ia != NULL; ia = ia->next) {
		valdst->v.identlist = g_slist_append(valdst->v.identlist,
						     stralloc(ia->data));
	    }
	    break;

	case CONFTYPE_HOST_LIMIT:
	    valdst->v.host_limit = valsrc->v.host_limit;
	    valdst->v.host_limit.match_pats = NULL;
	    for (ia = valsrc->v.host_limit.match_pats; ia != NULL; ia = ia->next) {
		valdst->v.host_limit.match_pats =
		    g_slist_append(valdst->v.host_limit.match_pats, g_strdup(ia->data));
	    }
	    break;

	case CONFTYPE_TIME:
	    valdst->v.t = valsrc->v.t;
	    break;

	case CONFTYPE_ESTIMATELIST: {
	    estimatelist_t estimates = valsrc->v.estimatelist;
	    estimatelist_t dst_estimates = NULL;
	    while (estimates != NULL) {
		dst_estimates = g_slist_append(dst_estimates, estimates->data);
		estimates = estimates->next;
	    }
	    valdst->v.estimatelist = dst_estimates;
	    break;
	}

	case CONFTYPE_EXINCLUDE:
	    valdst->v.exinclude.optional = valsrc->v.exinclude.optional;
	    valdst->v.exinclude.sl_list = duplicate_sl(valsrc->v.exinclude.sl_list);
	    valdst->v.exinclude.sl_file = duplicate_sl(valsrc->v.exinclude.sl_file);
	    break;

	case CONFTYPE_INTRANGE:
	    valdst->v.intrange[0] = valsrc->v.intrange[0];
	    valdst->v.intrange[1] = valsrc->v.intrange[1];
	    break;

        case CONFTYPE_PROPLIST:
	    if (valsrc->v.proplist) {
		valdst->v.proplist = g_hash_table_new_full(g_str_amanda_hash,
							   g_str_amanda_equal,
							   &g_free,
							   &free_property_t);

		g_hash_table_foreach(valsrc->v.proplist,
				     &copy_proplist_foreach_fn,
				     valdst->v.proplist);
	    } else {
		valdst->v.proplist = NULL;
	    }
	    break;

	case CONFTYPE_APPLICATION:
	    valdst->v.s = stralloc(valsrc->v.s);
	    break;

	case CONFTYPE_AUTOLABEL:
	    valdst->v.autolabel.template = stralloc(valsrc->v.autolabel.template);
	    valdst->v.autolabel.autolabel = valsrc->v.autolabel.autolabel;
	    break;
	}
    }
}

static void
merge_proplist_foreach_fn(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p)
{
    char *property_s = key_p;
    property_t *property = value_p;
    proplist_t proplist = user_data_p;
    GSList *elem = NULL;
    int new_prop = 0;
    property_t *new_property = g_hash_table_lookup(proplist, property_s);
    if (new_property && !property->append) {
	g_hash_table_remove(proplist, property_s);
	new_property = NULL;
    }
    if (!new_property) {
        new_property = malloc(sizeof(property_t));
	new_property->seen = property->seen;
	new_property->append = property->append;
	new_property->priority = property->priority;
	new_property->values = NULL;
	new_prop = 1;
    }

    for(elem = property->values;elem != NULL; elem=elem->next) {
	new_property->values = g_slist_append(new_property->values,
					      stralloc(elem->data));
    }
    if (new_prop)
	g_hash_table_insert(proplist, stralloc(property_s), new_property);
}

static void
copy_proplist_foreach_fn(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p)
{
    char *property_s = key_p;
    property_t *property = value_p;
    proplist_t proplist = user_data_p;
    GSList *elem = NULL;
    property_t *new_property = malloc(sizeof(property_t));
    new_property->append = property->append;
    new_property->priority = property->priority;
    new_property->seen = property->seen;
    new_property->values = NULL;

    for(elem = property->values;elem != NULL; elem=elem->next) {
	new_property->values = g_slist_append(new_property->values,
					      stralloc(elem->data));
    }
    g_hash_table_insert(proplist, stralloc(property_s), new_property);
}

static void
free_val_t(
    val_t *val)
{
    switch(val->type) {
	case CONFTYPE_INT:
	case CONFTYPE_BOOLEAN:
	case CONFTYPE_NO_YES_ALL:
	case CONFTYPE_COMPRESS:
	case CONFTYPE_ENCRYPT:
	case CONFTYPE_HOLDING:
	case CONFTYPE_EXECUTE_WHERE:
	case CONFTYPE_EXECUTE_ON:
	case CONFTYPE_SEND_AMREPORT_ON:
	case CONFTYPE_DATA_PATH:
	case CONFTYPE_STRATEGY:
	case CONFTYPE_SIZE:
	case CONFTYPE_TAPERALGO:
	case CONFTYPE_PRIORITY:
	case CONFTYPE_INT64:
	case CONFTYPE_REAL:
	case CONFTYPE_RATE:
	case CONFTYPE_INTRANGE:
	case CONFTYPE_PART_CACHE_TYPE:
	    break;

	case CONFTYPE_IDENT:
	case CONFTYPE_STR:
	case CONFTYPE_APPLICATION:
	    amfree(val->v.s);
	    break;

	case CONFTYPE_IDENTLIST:
	    slist_free_full(val->v.identlist, g_free);
	    break;

	case CONFTYPE_HOST_LIMIT:
	    slist_free_full(val->v.host_limit.match_pats, g_free);
	    break;

	case CONFTYPE_TIME:
	    break;

	case CONFTYPE_ESTIMATELIST:
	    g_slist_free(val->v.estimatelist);
	    break;

	case CONFTYPE_EXINCLUDE:
	    free_sl(val_t__exinclude(val).sl_list);
	    free_sl(val_t__exinclude(val).sl_file);
	    break;

        case CONFTYPE_PROPLIST:
            g_hash_table_destroy(val_t__proplist(val));
            break;

	case CONFTYPE_AUTOLABEL:
	    amfree(val->v.autolabel.template);
	    break;
    }
    val->seen.linenum = 0;
    val->seen.filename = NULL;
    val->seen.block = NULL;
}

/*
 * Utilities Implementation
 */

char *
generic_get_security_conf(
	char *string,
	void *arg)
{
	arg = arg;
	if(!string || !*string)
		return(NULL);

	if(strcmp(string, "krb5principal")==0) {
		return(getconf_str(CNF_KRB5PRINCIPAL));
	} else if(strcmp(string, "krb5keytab")==0) {
		return(getconf_str(CNF_KRB5KEYTAB));
	}
	return(NULL);
}

char *
generic_client_get_security_conf(
    char *	string,
    void *	arg)
{
	(void)arg;	/* Quiet unused parameter warning */

	if(!string || !*string)
		return(NULL);

	if(strcmp(string, "conf")==0) {
		return(getconf_str(CNF_CONF));
	} else if(strcmp(string, "amdump_server")==0) {
		return(getconf_str(CNF_AMDUMP_SERVER));
	} else if(strcmp(string, "index_server")==0) {
		return(getconf_str(CNF_INDEX_SERVER));
	} else if(strcmp(string, "tape_server")==0) {
		return(getconf_str(CNF_TAPE_SERVER));
	} else if(strcmp(string, "tapedev")==0) {
		return(getconf_str(CNF_TAPEDEV));
        } else if(strcmp(string, "auth")==0) {
		return(getconf_str(CNF_AUTH));
	} else if(strcmp(string, "ssh_keys")==0) {
		return(getconf_str(CNF_SSH_KEYS));
	} else if(strcmp(string, "amandad_path")==0) {
		return(getconf_str(CNF_AMANDAD_PATH));
	} else if(strcmp(string, "client_username")==0) {
		return(getconf_str(CNF_CLIENT_USERNAME));
	} else if(strcmp(string, "client_port")==0) {
		return(getconf_str(CNF_CLIENT_PORT));
	} else if(strcmp(string, "gnutar_list_dir")==0) {
		return(getconf_str(CNF_GNUTAR_LIST_DIR));
	} else if(strcmp(string, "amandates")==0) {
		return(getconf_str(CNF_AMANDATES));
	} else if(strcmp(string, "krb5principal")==0) {
		return(getconf_str(CNF_KRB5PRINCIPAL));
	} else if(strcmp(string, "krb5keytab")==0) {
		return(getconf_str(CNF_KRB5KEYTAB));
	}
	return(NULL);
}

void
dump_configuration(
    gboolean print_default,
    gboolean print_source)
{
    tapetype_t *tp;
    dumptype_t *dp;
    interface_t *ip;
    holdingdisk_t *hd;
    GSList        *hp;
    application_t *ap;
    pp_script_t *ps;
    device_config_t *dc;
    changer_config_t *cc;
    interactivity_t  *iv;
    taperscan_t      *ts;
    int i;
    conf_var_t *np;
    keytab_t *kt;
    char *prefix;

    if (config_client) {
	error(_("Don't know how to dump client configurations."));
	/* NOTREACHED */
    }

    g_printf(_("# AMANDA CONFIGURATION FROM FILE \"%s\":\n\n"), config_filename);

    for(np=server_var; np->token != CONF_UNKNOWN; np++) {
	for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++) 
	    if (np->token == kt->token) break;

	if(kt->token == CONF_UNKNOWN)
	    error(_("server bad token"));

        val_t_print_token(print_default, print_source, stdout, NULL, "%-21s ", kt, &conf_data[np->parm]);
    }

    for(hp = holdinglist; hp != NULL; hp = hp->next) {
	hd = hp->data;
	g_printf("\nDEFINE HOLDINGDISK %s {\n", hd->name);
	for(i=0; i < HOLDING_HOLDING; i++) {
	    for(np=holding_var; np->token != CONF_UNKNOWN; np++) {
		if(np->parm == i)
			break;
	    }
	    if(np->token == CONF_UNKNOWN)
		error(_("holding bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++) {
		if(kt->token == np->token)
		    break;
	    }
	    if(kt->token == CONF_UNKNOWN)
		error(_("holding bad token"));

            val_t_print_token(print_default, print_source, stdout, NULL, "      %-9s ", kt, &hd->value[i]);
	}
	g_printf("}\n");
    }

    for(tp = tapelist; tp != NULL; tp = tp->next) {
	if(tp->seen.linenum == -1)
	    prefix = "#";
	else
	    prefix = "";
	g_printf("\n%sDEFINE TAPETYPE %s {\n", prefix, tp->name);
	for(i=0; i < TAPETYPE_TAPETYPE; i++) {
	    for(np=tapetype_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("tapetype bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("tapetype bad token"));

            val_t_print_token(print_default, print_source, stdout, prefix, "      %-9s ", kt, &tp->value[i]);
	}
	g_printf("%s}\n", prefix);
    }

    for(dp = dumplist; dp != NULL; dp = dp->next) {
	if (strncmp_const(dp->name, "custom(") != 0) { /* don't dump disklist-derived dumptypes */
	    if(dp->seen.linenum == -1)
		prefix = "#";
	    else
		prefix = "";
	    g_printf("\n%sDEFINE DUMPTYPE %s {\n", prefix, dp->name);
	    for(i=0; i < DUMPTYPE_DUMPTYPE; i++) {
		for(np=dumptype_var; np->token != CONF_UNKNOWN; np++)
		    if(np->parm == i) break;
		if(np->token == CONF_UNKNOWN)
		    error(_("dumptype bad value"));

		for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		    if(kt->token == np->token) break;
		if(kt->token == CONF_UNKNOWN)
		    error(_("dumptype bad token"));

		val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &dp->value[i]);
	    }
	    g_printf("%s}\n", prefix);
	}
    }

    for(ip = interface_list; ip != NULL; ip = ip->next) {
	seen_t *netusage_seen = &val_t__seen(getconf(CNF_NETUSAGE));
	if (ip->seen.linenum == netusage_seen->linenum &&
	    ip->seen.filename && netusage_seen->filename &&
	    0 == strcmp(ip->seen.filename, netusage_seen->filename))
	    prefix = "#";
	else
	    prefix = "";
	g_printf("\n%sDEFINE INTERFACE %s {\n", prefix, ip->name);
	for(i=0; i < INTER_INTER; i++) {
	    for(np=interface_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("interface bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("interface bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &ip->value[i]);
	}
	g_printf("%s}\n",prefix);
    }

    for(ap = application_list; ap != NULL; ap = ap->next) {
	if(strcmp(ap->name,"default") == 0)
	    prefix = "#";
	else
	    prefix = "";
	g_printf("\n%sDEFINE APPLICATION %s {\n", prefix, ap->name);
	for(i=0; i < APPLICATION_APPLICATION; i++) {
	    for(np=application_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("application bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("application bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &ap->value[i]);
	}
	g_printf("%s}\n",prefix);
    }

    for(ps = pp_script_list; ps != NULL; ps = ps->next) {
	if(strcmp(ps->name,"default") == 0)
	    prefix = "#";
	else
	    prefix = "";
	g_printf("\n%sDEFINE SCRIPT %s {\n", prefix, ps->name);
	for(i=0; i < PP_SCRIPT_PP_SCRIPT; i++) {
	    for(np=pp_script_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("script bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("script bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &ps->value[i]);
	}
	g_printf("%s}\n",prefix);
    }

    for(dc = device_config_list; dc != NULL; dc = dc->next) {
	prefix = "";
	g_printf("\n%sDEFINE DEVICE %s {\n", prefix, dc->name);
	for(i=0; i < DEVICE_CONFIG_DEVICE_CONFIG; i++) {
	    for(np=device_config_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("device bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("device bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &dc->value[i]);
	}
	g_printf("%s}\n",prefix);
    }

    for(cc = changer_config_list; cc != NULL; cc = cc->next) {
	prefix = "";
	g_printf("\n%sDEFINE CHANGER %s {\n", prefix, cc->name);
	for(i=0; i < CHANGER_CONFIG_CHANGER_CONFIG; i++) {
	    for(np=changer_config_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("changer bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("changer bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &cc->value[i]);
	}
	g_printf("%s}\n",prefix);
    }

    for(iv = interactivity_list; iv != NULL; iv = iv->next) {
	prefix = "";
	g_printf("\n%sDEFINE INTERACTIVITY %s {\n", prefix, iv->name);
	for(i=0; i < INTERACTIVITY_INTERACTIVITY; i++) {
	    for(np=interactivity_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("interactivity bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("interactivity bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &iv->value[i]);
	}
	g_printf("%s}\n",prefix);
    }

    for(ts = taperscan_list; ts != NULL; ts = ts->next) {
	prefix = "";
	g_printf("\n%sDEFINE TAPERSCAN %s {\n", prefix, ts->name);
	for(i=0; i < TAPERSCAN_TAPERSCAN; i++) {
	    for(np=taperscan_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("taperscan bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("taperscan bad token"));

	    val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &ts->value[i]);
	}
	g_printf("%s}\n",prefix);
    }
}

void dump_dumptype(
    dumptype_t *dp,
    char       *prefix,
    gboolean    print_default,
    gboolean    print_source)
{
    int i;
    conf_var_t *np;
    keytab_t *kt;

    for(i=0; i < DUMPTYPE_DUMPTYPE; i++) {
	for(np=dumptype_var; np->token != CONF_UNKNOWN; np++)
	    if(np->parm == i) break;
	if(np->token == CONF_UNKNOWN)
	    error(_("dumptype bad value"));

	for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
	    if(kt->token == np->token) break;
	if(kt->token == CONF_UNKNOWN)
	    error(_("dumptype bad token"));

	val_t_print_token(print_default, print_source, stdout, prefix, "      %-19s ", kt, &dp->value[i]);
    }
}

static void
val_t_print_token(
    gboolean  print_default,
    gboolean  print_source,
    FILE     *output,
    char     *prefix,
    char     *format,
    keytab_t *kt,
    val_t    *val)
{
    char       **dispstrs, **dispstr;

    if (print_default == 0 && !val_t_seen(val)) {
	return;
    }

    dispstrs = val_t_display_strs(val, 1, print_source, TRUE);

    /* For most configuration types, this outputs
     *   PREFIX KEYWORD DISPSTR
     * for each of the display strings.  For identifiers, however, it
     * simply prints the first line of the display string.
     */

    /* Print the keyword for anything that is not itself an identifier */
    if (kt->token != CONF_IDENT) {
        for(dispstr=dispstrs; *dispstr!=NULL; dispstr++) {
	    if (prefix)
		g_fprintf(output, "%s", prefix);
	    g_fprintf(output, format, str_keyword(kt));
	    g_fprintf(output, "%s\n", *dispstr);
	}
    } else {
	/* for identifiers, assume there's at most one display string */
	assert(g_strv_length(dispstrs) <= 1);
	if (*dispstrs) {
	    g_fprintf(output, "%s\n", *dispstrs);
	}
    }

    g_strfreev(dispstrs);
}

typedef struct proplist_display_str_foreach_user_data {
    char     **msg;
    gboolean   print_source;
} proplist_display_str_foreach_user_data;

char *source_string(seen_t *seen);
char *source_string(
    seen_t *seen)
{
    char *buf;

    if (seen->linenum) {
	if (seen->block) {
	    buf = g_strdup_printf("     (%s file %s line %d)",
			seen->block, seen->filename, seen->linenum);
	} else {
	    buf = g_strdup_printf("     (file %s line %d)",
			seen->filename, seen->linenum);
	}
    } else {
	buf = g_strdup("     (default)");
    }
    return buf;
}

char **
val_t_display_strs(
    val_t *val,
    int    str_need_quote,
    gboolean print_source,
    gboolean print_unit)
{
    gboolean add_source = TRUE;
    int    i;
    char **buf;
    buf = malloc(3*SIZEOF(char *));
    buf[0] = NULL;
    buf[1] = NULL;
    buf[2] = NULL;

    switch(val->type) {
    case CONFTYPE_INT:
	buf[0] = vstrallocf("%d ", val_t__int(val));
	i = strlen(buf[0]) - 1;
	if (print_unit && val->unit == CONF_UNIT_K) {
	    buf[0][i] = 'K';
	} else {
	    buf[0][i] = '\0';
	}
	break;

    case CONFTYPE_SIZE:
	buf[0] = vstrallocf("%zd ", (ssize_t)val_t__size(val));
	i = strlen(buf[0]) - 1;
	if (print_unit && val->unit == CONF_UNIT_K) {
	    buf[0][i] = 'K';
	} else {
	    buf[0][i] = '\0';
	}
	break;

    case CONFTYPE_INT64:
	buf[0] = vstrallocf("%lld ", (long long)val_t__int64(val));
	i = strlen(buf[0]) - 1;
	if (print_unit && val->unit == CONF_UNIT_K) {
	    buf[0][i] = 'K';
	} else {
	    buf[0][i] = '\0';
	}
	break;

    case CONFTYPE_REAL:
	buf[0] = vstrallocf("%0.5f", val_t__real(val));
	break;

    case CONFTYPE_RATE:
	buf[0] = vstrallocf("%0.5f %0.5f", val_t__rate(val)[0], val_t__rate(val)[1]);
	break;

    case CONFTYPE_INTRANGE:
	buf[0] = vstrallocf("%d,%d", val_t__intrange(val)[0], val_t__intrange(val)[1]);
	break;

    case CONFTYPE_IDENT:
	if(val->v.s) {
	    buf[0] = stralloc(val->v.s);
        } else {
	    buf[0] = stralloc("");
	}
	break;

    case CONFTYPE_IDENTLIST:
	{
	    GSList *ia;
	    int     first = 1;

	    buf[0] = NULL;
	    for (ia = val->v.identlist; ia != NULL; ia = ia->next) {
		if (first) {
		    buf[0] = stralloc(ia->data);
		    first = 0;
		} else {
		    strappend(buf[0], " ");
		    strappend(buf[0],  ia->data);
		}
	    }
	}
	break;

    case CONFTYPE_STR:
	if(str_need_quote) {
            if(val->v.s) {
		buf[0] = quote_string_always(val->v.s);
            } else {
		buf[0] = stralloc("\"\"");
            }
	} else {
	    if(val->v.s) {
		buf[0] = stralloc(val->v.s);
            } else {
		buf[0] = stralloc("");
            }
	}
	break;

    case CONFTYPE_AUTOLABEL:
	{
	    buf[0] = quote_string_always(val->v.autolabel.template);
	    if (val->v.autolabel.autolabel & AL_OTHER_CONFIG) {
		buf[0] = vstrextend(&buf[0], " OTHER-CONFIG", NULL);
	    }
	    if (val->v.autolabel.autolabel & AL_NON_AMANDA) {
		buf[0] = vstrextend(&buf[0], " NON-AMANDA", NULL);
	    }
	    if (val->v.autolabel.autolabel & AL_VOLUME_ERROR) {
		buf[0] = vstrextend(&buf[0], " VOLUME-ERROR", NULL);
	    }
	    if (val->v.autolabel.autolabel & AL_EMPTY) {
		buf[0] = vstrextend(&buf[0], " EMPTY", NULL);
	    }
	}
	break;

    case CONFTYPE_TIME:
	buf[0] = vstrallocf("%2d%02d",
			 (int)val_t__time(val)/100, (int)val_t__time(val) % 100);
	break;

    case CONFTYPE_EXINCLUDE: {
        buf[0] = exinclude_display_str(val, 0);
        buf[1] = exinclude_display_str(val, 1);
	break;
    }

    case CONFTYPE_BOOLEAN:
	if(val_t__boolean(val))
	    buf[0] = stralloc("yes");
	else
	    buf[0] = stralloc("no");
	break;

    case CONFTYPE_NO_YES_ALL:
	switch(val_t__no_yes_all(val)) {
	case 0:
	    buf[0] = stralloc("no");
	    break;
	case 1:
	    buf[0] = stralloc("yes");
	    break;
	case 2:
	    buf[0] = stralloc("all");
	    break;
	}
	break;

    case CONFTYPE_STRATEGY:
	switch(val_t__strategy(val)) {
	case DS_SKIP:
	    buf[0] = vstrallocf("SKIP");
	    break;

	case DS_STANDARD:
	    buf[0] = vstrallocf("STANDARD");
	    break;

	case DS_NOFULL:
	    buf[0] = vstrallocf("NOFULL");
	    break;

	case DS_NOINC:
	    buf[0] = vstrallocf("NOINC");
	    break;

	case DS_HANOI:
	    buf[0] = vstrallocf("HANOI");
	    break;

	case DS_INCRONLY:
	    buf[0] = vstrallocf("INCRONLY");
	    break;
	}
	break;

    case CONFTYPE_COMPRESS:
	switch(val_t__compress(val)) {
	case COMP_NONE:
	    buf[0] = vstrallocf("NONE");
	    break;

	case COMP_FAST:
	    buf[0] = vstrallocf("CLIENT FAST");
	    break;

	case COMP_BEST:
	    buf[0] = vstrallocf("CLIENT BEST");
	    break;

	case COMP_CUST:
	    buf[0] = vstrallocf("CLIENT CUSTOM");
	    break;

	case COMP_SERVER_FAST:
	    buf[0] = vstrallocf("SERVER FAST");
	    break;

	case COMP_SERVER_BEST:
	    buf[0] = vstrallocf("SERVER BEST");
	    break;

	case COMP_SERVER_CUST:
	    buf[0] = vstrallocf("SERVER CUSTOM");
	    break;
	}
	break;

    case CONFTYPE_ESTIMATELIST: {
	estimatelist_t es = val_t__estimatelist(val);
	buf[0] = stralloc("");
	while (es) {
	    switch((estimate_t)GPOINTER_TO_INT(es->data)) {
	    case ES_CLIENT:
		strappend(buf[0], "CLIENT");
		break;

	    case ES_SERVER:
		strappend(buf[0], "SERVER");
		break;

	    case ES_CALCSIZE:
		strappend(buf[0], "CALCSIZE");
		break;

	    case ES_ES:
		break;
	    }
	    es = es->next;
	    if (es)
		strappend(buf[0], " ");
	}
	break;
    }

    case CONFTYPE_EXECUTE_WHERE:
	switch(val->v.i) {
	case ES_CLIENT:
	    buf[0] = vstrallocf("CLIENT");
	    break;

	case ES_SERVER:
	    buf[0] = vstrallocf("SERVER");
	    break;
	}
	break;

    case CONFTYPE_SEND_AMREPORT_ON:
	switch(val->v.i) {
	case SEND_AMREPORT_ALL:
	    buf[0] = vstrallocf("ALL");
	    break;
	case SEND_AMREPORT_STRANGE:
	    buf[0] = vstrallocf("STRANGE");
	    break;
	case SEND_AMREPORT_ERROR:
	    buf[0] = vstrallocf("ERROR");
	    break;
	case SEND_AMREPORT_NEVER:
	    buf[0] = vstrallocf("NEVER");
	    break;
	}
	break;

    case CONFTYPE_DATA_PATH:
	buf[0] = g_strdup(data_path_to_string(val->v.i));
	break;

     case CONFTYPE_ENCRYPT:
	switch(val_t__encrypt(val)) {
	case ENCRYPT_NONE:
	    buf[0] = vstrallocf("NONE");
	    break;

	case ENCRYPT_CUST:
	    buf[0] = vstrallocf("CLIENT");
	    break;

	case ENCRYPT_SERV_CUST:
	    buf[0] = vstrallocf("SERVER");
	    break;
	}
	break;

     case CONFTYPE_PART_CACHE_TYPE:
	switch(val_t__part_cache_type(val)) {
	case PART_CACHE_TYPE_NONE:
	    buf[0] = vstrallocf("NONE");
	    break;

	case PART_CACHE_TYPE_DISK:
	    buf[0] = vstrallocf("DISK");
	    break;

	case PART_CACHE_TYPE_MEMORY:
	    buf[0] = vstrallocf("MEMORY");
	    break;
	}
	break;

     case CONFTYPE_HOST_LIMIT: {
	GSList *iter = val_t__host_limit(val).match_pats;

	if (val_t__host_limit(val).same_host)
	    buf[0] = stralloc("SAME-HOST ");
	else
	    buf[0] = stralloc("");

	if (val_t__host_limit(val).server)
	    strappend(buf[0], "SERVER ");

	while (iter) {
	    strappend(buf[0], quote_string_always((char *)iter->data));
	    strappend(buf[0], " ");
	    iter = iter->next;
	}
	break;
     }

     case CONFTYPE_HOLDING:
	switch(val_t__holding(val)) {
	case HOLD_NEVER:
	    buf[0] = vstrallocf("NEVER");
	    break;

	case HOLD_AUTO:
	    buf[0] = vstrallocf("AUTO");
	    break;

	case HOLD_REQUIRED:
	    buf[0] = vstrallocf("REQUIRED");
	    break;
	}
	break;

     case CONFTYPE_TAPERALGO:
	buf[0] = vstrallocf("%s", taperalgo2str(val_t__taperalgo(val)));
	break;

     case CONFTYPE_PRIORITY:
	switch(val_t__priority(val)) {
	case 0:
	    buf[0] = vstrallocf("LOW");
	    break;

	case 1:
	    buf[0] = vstrallocf("MEDIUM");
	    break;

	case 2:
	    buf[0] = vstrallocf("HIGH");
	    break;
	}
	break;

    case CONFTYPE_PROPLIST: {
	int    nb_property;
	proplist_display_str_foreach_user_data user_data;

	nb_property = g_hash_table_size(val_t__proplist(val));
	g_free(buf);
	buf = malloc((nb_property+1)*SIZEOF(char*));
	buf[nb_property] = NULL;
	user_data.msg = buf;
	user_data.print_source = print_source;
	g_hash_table_foreach(val_t__proplist(val),
			     proplist_display_str_foreach_fn,
			     &user_data);
	add_source = FALSE;
        break;
    }

    case CONFTYPE_APPLICATION: {
	if (val->v.s) {
	    buf[0] = quote_string_always(val->v.s);
	} else {
	    buf[0] = stralloc("");
	}
	break;
    }

    case CONFTYPE_EXECUTE_ON:
	buf[0] = stralloc("");
	if (val->v.i != 0) {
	    char *sep = "";
	    if (val->v.i & EXECUTE_ON_PRE_AMCHECK) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-AMCHECK", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_DLE_AMCHECK) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-DLE-AMCHECK", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_HOST_AMCHECK) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-HOST-AMCHECK", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_DLE_AMCHECK) {
		buf[0] = vstrextend(&buf[0], sep, "POST-DLE-AMCHECK", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_HOST_AMCHECK) {
		buf[0] = vstrextend(&buf[0], sep, "POST-HOST-AMCHECK", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_AMCHECK) {
		buf[0] = vstrextend(&buf[0], sep, "POST-AMCHECK", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_ESTIMATE) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-ESTIMATE", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_DLE_ESTIMATE) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-DLE-ESTIMATE", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_HOST_ESTIMATE) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-HOST-ESTIMATE", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_DLE_ESTIMATE) {
		buf[0] = vstrextend(&buf[0], sep, "POST-DLE-ESTIMATE", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_HOST_ESTIMATE) {
		buf[0] = vstrextend(&buf[0], sep, "POST-HOST-ESTIMATE", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_ESTIMATE) {
		buf[0] = vstrextend(&buf[0], sep, "POST-ESTIMATE", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_BACKUP) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-BACKUP", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_DLE_BACKUP) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-DLE-BACKUP", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_HOST_BACKUP) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-HOST-BACKUP", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_BACKUP) {
		buf[0] = vstrextend(&buf[0], sep, "POST-BACKUP", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_DLE_BACKUP) {
		buf[0] = vstrextend(&buf[0], sep, "POST-DLE-BACKUP", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_HOST_BACKUP) {
		buf[0] = vstrextend(&buf[0], sep, "POST-HOST-BACKUP", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_RECOVER) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-RECOVER", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_RECOVER) {
		buf[0] = vstrextend(&buf[0], sep, "POST-RECOVER", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_PRE_LEVEL_RECOVER) {
		buf[0] = vstrextend(&buf[0], sep, "PRE-LEVEL-RECOVER", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_POST_LEVEL_RECOVER) {
		buf[0] = vstrextend(&buf[0], sep, "POST-LEVEL-RECOVER", NULL);
		sep = ", ";
	    }
	    if (val->v.i & EXECUTE_ON_INTER_LEVEL_RECOVER) {
		buf[0] = vstrextend(&buf[0], sep, "INTER-LEVEL-RECOVER", NULL);
		sep = ", ";
	    }
	}
        break;

    }

    /* add source */
    if (print_source && add_source) {
	char **buf1;
	for (buf1 = buf; *buf1 != NULL; buf1++) {
	    char *buf2 = g_strjoin("", *buf1, source_string(&val->seen), NULL);
	    g_free(*buf1);
	    *buf1 = buf2;
	}
    }
    return buf;
}

int
val_t_to_execute_on(
    val_t *val)
{
    if (val->type != CONFTYPE_EXECUTE_ON) {
	error(_("get_conftype_execute_on: val.type is not CONFTYPE_EXECUTE_ON"));
	/*NOTREACHED*/
    }
    return val_t__execute_on(val);
}

int
val_t_to_execute_where(
    val_t *val)
{
    if (val->type != CONFTYPE_EXECUTE_WHERE) {
	error(_("get_conftype_execute_where: val.type is not CONFTYPE_EXECUTE_WHERE"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

char *
val_t_to_application(
    val_t *val)
{
    if (val->type != CONFTYPE_APPLICATION) {
	error(_("get_conftype_applicaiton: val.type is not CONFTYPE_APPLICATION"));
	/*NOTREACHED*/
    }
    return val->v.s;
}


static void
proplist_display_str_foreach_fn(
    gpointer key_p,
    gpointer value_p,
    gpointer user_data_p)
{
    char         *property_s = quote_string_always(key_p);
    property_t   *property   = value_p;
    GSList       *value;
    proplist_display_str_foreach_user_data *user_data = user_data_p;
    char       ***msg        = (char ***)&user_data->msg;

    /* What to do with property->append? it should be printed only on client */
    if (property->priority) {
	**msg = vstralloc("priority ", property_s, NULL);
	amfree(property_s);
    } else {
	**msg = property_s;
	property_s = NULL;
    }
    for(value=property->values; value != NULL; value = value->next) {
	char *qstr = quote_string_always((char *)value->data);
	**msg = vstrextend(*msg, " ", qstr, NULL);
	amfree(qstr);
    }
    if (user_data->print_source) {
	**msg = vstrextend(*msg, source_string(&property->seen));
    }
    (*msg)++;
}

static char *
exinclude_display_str(
    val_t *val,
    int    file)
{
    am_sl_t  *sl;
    sle_t *excl;
    char *rval;

    assert(val->type == CONFTYPE_EXINCLUDE);

    rval = stralloc("");

    if (file == 0) {
	sl = val_t__exinclude(val).sl_list;
        strappend(rval, "LIST");
    } else {
	sl = val_t__exinclude(val).sl_file;
        strappend(rval, "FILE");
    }

    if (val_t__exinclude(val).optional == 1) {
        strappend(rval, " OPTIONAL");
    }

    if (sl != NULL) {
	for(excl = sl->first; excl != NULL; excl = excl->next) {
	    char *qstr = quote_string_always(excl->name);
            vstrextend(&rval, " ", qstr, NULL);
	    amfree(qstr);
	}
    }

    return rval;
}

char *
taperalgo2str(
    taperalgo_t taperalgo)
{
    if(taperalgo == ALGO_FIRST) return "FIRST";
    if(taperalgo == ALGO_FIRSTFIT) return "FIRSTFIT";
    if(taperalgo == ALGO_LARGEST) return "LARGEST";
    if(taperalgo == ALGO_LARGESTFIT) return "LARGESTFIT";
    if(taperalgo == ALGO_SMALLEST) return "SMALLEST";
    if(taperalgo == ALGO_LAST) return "LAST";
    return "UNKNOWN";
}

char *
config_dir_relative(
    char *filename)
{
    if (*filename == '/' || config_dir == NULL) {
	return stralloc(filename);
    } else {
	if (config_dir[strlen(config_dir)-1] == '/') {
	    return vstralloc(config_dir, filename, NULL);
	} else {
	    return vstralloc(config_dir, "/", filename, NULL);
	}
    }
}

static int
parm_key_info(
    char *key,
    conf_var_t **parm,
    val_t **val)
{
    conf_var_t *np;
    keytab_t *kt;
    char *s;
    char ch;
    char *subsec_type;
    char *subsec_name;
    char *subsec_key;
    tapetype_t *tp;
    dumptype_t *dp;
    interface_t *ip;
    holdingdisk_t *hp;
    application_t *ap;
    pp_script_t   *pp;
    device_config_t   *dc;
    changer_config_t   *cc;
    taperscan_t        *ts;
    interactivity_t    *iv;
    int success = FALSE;

    /* WARNING: assumes globals keytable and parsetable are set correctly. */
    assert(keytable != NULL);
    assert(parsetable != NULL);

    /* make a copy we can stomp on */
    key = stralloc(key);

    /* uppercase the key */
    for (s = key; (ch = *s) != 0; s++) {
	if (islower((int)ch))
	    *s = (char)toupper(ch);
    }

    subsec_name = strchr(key, ':');
    if (subsec_name) {
	subsec_type = key;

	*subsec_name = '\0';
	subsec_name++;

	/* convert subsec_type '-' to '_' */
	for (s = subsec_type; (ch = *s) != 0; s++) {
	    if (*s == '-') *s = '_';
	}

	subsec_key = strrchr(subsec_name,':');
	if(!subsec_key) goto out; /* failure */

	*subsec_key = '\0';
	subsec_key++;

	/* convert subsec_key '-' to '_' */
	for (s = subsec_key; (ch = *s) != 0; s++) {
	    if (*s == '-') *s = '_';
	}

	/* If the keyword doesn't exist, there's no need to look up the
	 * subsection -- we know it's invalid */
	for(kt = keytable; kt->token != CONF_UNKNOWN; kt++) {
	    if(kt->keyword && strcmp(kt->keyword, subsec_key) == 0)
		break;
	}
	if(kt->token == CONF_UNKNOWN) goto out;

	/* Otherwise, figure out which kind of subsection we're dealing with,
	 * and parse against that. */
	if (strcmp(subsec_type, "TAPETYPE") == 0) {
	    tp = lookup_tapetype(subsec_name);
	    if (!tp) goto out;
	    for(np = tapetype_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &tp->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (strcmp(subsec_type, "DUMPTYPE") == 0) {
	    dp = lookup_dumptype(subsec_name);
	    if (!dp) goto out;
	    for(np = dumptype_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &dp->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (strcmp(subsec_type, "HOLDINGDISK") == 0) {
	    hp = lookup_holdingdisk(subsec_name);
	    if (!hp) goto out;
	    for(np = holding_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &hp->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (strcmp(subsec_type, "INTERFACE") == 0) {
	    ip = lookup_interface(subsec_name);
	    if (!ip) goto out;
	    for(np = interface_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &ip->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	/* accept the old name here, too */
	} else if (strcmp(subsec_type, "APPLICATION_TOOL") == 0
		|| strcmp(subsec_type, "APPLICATION") == 0) {
	    ap = lookup_application(subsec_name);
	    if (!ap) goto out;
	    for(np = application_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &ap->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	/* accept the old name here, too */
	} else if (strcmp(subsec_type, "SCRIPT_TOOL") == 0
		|| strcmp(subsec_type, "SCRIPT") == 0) {
	    pp = lookup_pp_script(subsec_name);
	    if (!pp) goto out;
	    for(np = pp_script_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &pp->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (strcmp(subsec_type, "DEVICE") == 0) {
	    dc = lookup_device_config(subsec_name);
	    if (!dc) goto out;
	    for(np = device_config_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &dc->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (strcmp(subsec_type, "CHANGER") == 0) {
	    cc = lookup_changer_config(subsec_name);
	    if (!cc) goto out;
	    for(np = changer_config_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &cc->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (g_str_equal(subsec_type, "INTERACTIVITY")) {
	    iv = lookup_interactivity(subsec_name);
	    if (!iv) goto out;
	    for(np = interactivity_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &iv->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	} else if (g_str_equal(subsec_type, "TAPERSCAN")) {
	    ts = lookup_taperscan(subsec_name);
	    if (!ts) goto out;
	    for(np = taperscan_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) goto out;

	    if (val) *val = &ts->value[np->parm];
	    if (parm) *parm = np;
	    success = TRUE;
	}

    /* No delimiters -- we're referencing a global config parameter */
    } else {
	/* convert key '-' to '_' */
	for (s = key; (ch = *s) != 0; s++) {
	    if (*s == '-') *s = '_';
	}

	/* look up the keyword */
	for(kt = keytable; kt->token != CONF_UNKNOWN; kt++) {
	    if(kt->keyword && strcmp(kt->keyword, key) == 0)
		break;
	}
	if(kt->token == CONF_UNKNOWN) goto out;

	/* and then look that up in the parse table */
	for(np = parsetable; np->token != CONF_UNKNOWN; np++) {
	    if(np->token == kt->token)
		break;
	}
	if(np->token == CONF_UNKNOWN) goto out;

	if (val) *val = &conf_data[np->parm];
	if (parm) *parm = np;
	success = TRUE;
    }

out:
    amfree(key);
    return success;
}

gint64 
find_multiplier(
    char * str)
{
    keytab_t * table_entry;

    str = g_strdup(str);
    g_strstrip(str);

    if (*str == '\0') {
        g_free(str);
        return 1;
    }

    for (table_entry = numb_keytable; table_entry->keyword != NULL;
         table_entry ++) {
        if (strcasecmp(str, table_entry->keyword) == 0) {
            g_free(str);
            switch (table_entry->token) {
            case CONF_MULT1K:
                return 1024;
            case CONF_MULT1M:
                return 1024*1024;
            case CONF_MULT1G:
                return 1024*1024*1024;
            case CONF_MULT1T:
                return (gint64)1024*1024*1024*1024;
            case CONF_MULT7:
                return 7;
            case CONF_AMINFINITY:
                return G_MAXINT64;
            case CONF_MULT1:
            case CONF_IDENT:
                return 1;
            default:
                /* Should not happen. */
                return 0;
            }
        }
    }

    /* None found; this is an error. */
    g_free(str);
    return 0;
}

int
string_to_boolean(
    const char *str)
{
    keytab_t * table_entry;

    if (str == NULL || *str == '\0') {
        return -1;
    }

    /* 0 and 1 are not in the table, as they are parsed as ints */
    if (0 == strcmp(str, "0"))
	return 0;
    if (0 == strcmp(str, "1"))
	return 1;

    for (table_entry = bool_keytable; table_entry->keyword != NULL;
         table_entry ++) {
        if (strcasecmp(str, table_entry->keyword) == 0) {
            switch (table_entry->token) {
            case CONF_ATRUE:
                return 1;
            case CONF_AFALSE:
                return 0;
            default:
                return -1;
            }
        }
    }

    return -1;
}

/*
 * Error Handling Implementaiton
 */

void config_add_error(
    cfgerr_level_t level,
    char * errmsg)
{
    cfgerr_level = max(cfgerr_level, level);

    g_debug("%s", errmsg);
    cfgerr_errors = g_slist_append(cfgerr_errors, errmsg);
}

static void conf_error_common(
    cfgerr_level_t level,
    const char * format,
    va_list argp)
{
    char *msg = g_strdup_vprintf(format, argp);
    char *errstr = NULL;

    if(current_line)
	errstr = g_strdup_printf(_("argument \"%s\": %s"),
		    current_line, msg);
    else if (current_filename && current_line_num > 0)
	errstr = g_strdup_printf(_("\"%s\", line %d: %s"),
		    current_filename, current_line_num, msg);
    else
	errstr = g_strdup_printf(_("parse error: %s"), msg);
    amfree(msg);

    config_add_error(level, errstr);
}

printf_arglist_function(void conf_parserror, const char *, format)
{
    va_list argp;
    
    arglist_start(argp, format);
    conf_error_common(CFGERR_ERRORS, format, argp);
    arglist_end(argp);
}

printf_arglist_function(void conf_parswarn, const char *, format) {
    va_list argp;
    
    arglist_start(argp, format);
    conf_error_common(CFGERR_WARNINGS, format, argp);
    arglist_end(argp);
}

cfgerr_level_t
config_errors(GSList **errstr)
{
    if (errstr)
	*errstr = cfgerr_errors;
    return cfgerr_level;
}

void
config_clear_errors(void)
{
    slist_free_full(cfgerr_errors, g_free);

    cfgerr_errors = NULL;
    cfgerr_level = CFGERR_OK;
}

void
config_print_errors(void)
{
    GSList *iter;

    for (iter = cfgerr_errors; iter; iter = g_slist_next(iter)) {
	g_fprintf(stderr, "%s\n", (char *)iter->data);
    }
}

/* Get the config name */
char *get_config_name(void)
{
    return config_name;
}

/* Get the config directory */
char *get_config_dir(void)
{
    return config_dir;
}

/* Get the config filename */
char *get_config_filename(void)
{
    return config_filename;
}

char *
anonymous_value(void)
{
    static char number[NUM_STR_SIZE];
    static int value=1;

    g_snprintf(number, sizeof(number), "%d", value);

    value++;
    return number;
}

gint compare_pp_script_order(
    gconstpointer a,
    gconstpointer b)
{
    return pp_script_get_order(lookup_pp_script((char *)a)) > pp_script_get_order(lookup_pp_script((char *)b));
}

char *
data_path_to_string(
    data_path_t data_path)
{
    switch (data_path) {
	case DATA_PATH_AMANDA   : return "AMANDA";
	case DATA_PATH_DIRECTTCP: return "DIRECTTCP";
    }
    error(_("datapath is not DATA_PATH_AMANDA or DATA_PATH_DIRECTTCP"));
    /* NOTREACHED */
}

data_path_t
data_path_from_string(
    char *data)
{
    if (strcmp(data, "AMANDA") == 0)
	return DATA_PATH_AMANDA;
    if (strcmp(data, "DIRECTTCP") == 0)
	return DATA_PATH_DIRECTTCP;
    error(_("datapath is not AMANDA or DIRECTTCP :%s:"), data);
    /* NOTREACHED */
}

gchar *
amandaify_property_name(
    const gchar *name)
{
    gchar *ret, *cur_r;
    const gchar *cur_o;
    if (!name) return NULL;

    ret = g_malloc0(strlen(name)+1);
    cur_r = ret;
    for (cur_o = name; *cur_o; cur_o++) {
	if ('_' == *cur_o)
	    *cur_r = '-';
	else
	    *cur_r = g_ascii_tolower(*cur_o);

	cur_r++;
    }

    return ret;
}

static char keyword_str[1024];

static char *
str_keyword(
    keytab_t *kt)
{
    char *p = kt->keyword;
    char *s = keyword_str;

    while(*p != '\0') {
	if (*p == '_') {
	    *s = '-';
	} else {
	    *s = *p;
	}
	p++;
	s++;
    }
    *s = '\0';

    return keyword_str;
}
