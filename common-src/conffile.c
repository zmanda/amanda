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

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifndef AMANDATES_FILE
#define AMANDATES_FILE "/etc/amandates"
#endif

#ifndef INT_MAX
#define INT_MAX 2147483647
#endif

/* this corresponds to the normal output of amanda, but may
 * be adapted to any spacing as you like.
 */
ColumnInfo ColumnData[] = {
    { "HostName",   0, 12, 12, 0, "%-*.*s", "HOSTNAME" },
    { "Disk",       1, 11, 11, 0, "%-*.*s", "DISK" },
    { "Level",      1, 1,  1,  0, "%*.*d",  "L" },
    { "OrigKB",     1, 7,  0,  0, "%*.*lf", "ORIG-KB" },
    { "OutKB",      1, 7,  0,  0, "%*.*lf", "OUT-KB" },
    { "Compress",   1, 6,  1,  0, "%*.*lf", "COMP%" },
    { "DumpTime",   1, 7,  7,  0, "%*.*s",  "MMM:SS" },
    { "DumpRate",   1, 6,  1,  0, "%*.*lf", "KB/s" },
    { "TapeTime",   1, 6,  6,  0, "%*.*s",  "MMM:SS" },
    { "TapeRate",   1, 6,  1,  0, "%*.*lf", "KB/s" },
    { NULL,         0, 0,  0,  0, NULL,     NULL }
};

char *config_name = NULL;
char *config_dir = NULL;

int debug_amandad    = 0;
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

/* visible holding disk variables */

holdingdisk_t *holdingdisks;
int num_holdingdisks;

long int unit_divisor = 1;

/* configuration parameters */

val_t conf_data[CNF_CNF];
int conffile_init = 0;

command_option_t *program_options      = NULL;
int               program_options_size = 0;

/* other internal variables */
static holdingdisk_t hdcur;

static tapetype_t tpcur;

static dumptype_t dpcur;

static interface_t ifcur;

static dumptype_t *dumplist = NULL;
static tapetype_t *tapelist = NULL;
static interface_t *interface_list = NULL;

static int allow_overwrites;
static int token_pushed;
static tok_t tok, pushed_tok;
static val_t tokenval;

static int conf_line_num;
static int got_parserror;
static FILE *conf_conf = (FILE *)NULL;
static char *conf_confname = NULL;
static char *conf_line = NULL;
static char *conf_char = NULL;
static keytab_t *keytable = NULL;

/* predeclare local functions */

char *get_token_name(tok_t);


static void validate_positive0            (t_conf_var *, val_t *);
static void validate_positive1            (t_conf_var *, val_t *);
static void validate_runspercycle         (t_conf_var *, val_t *);
static void validate_bumppercent          (t_conf_var *, val_t *);
static void validate_bumpmult             (t_conf_var *, val_t *);
static void validate_inparallel           (t_conf_var *, val_t *);
static void validate_displayunit          (t_conf_var *, val_t *);
static void validate_reserve              (t_conf_var *, val_t *);
static void validate_use                  (t_conf_var *, val_t *);
static void validate_chunksize            (t_conf_var *, val_t *);
static void validate_blocksize            (t_conf_var *, val_t *);
static void validate_debug                (t_conf_var *, val_t *);
static void validate_reserved_port_range  (t_conf_var *, val_t *);
static void validate_unreserved_port_range(t_conf_var *, val_t *);

/*static t_conf_var  *get_np(t_conf_var *get_var, int parm);*/
static int     get_int(void);
/*static long    get_long(void);*/
static time_t  get_time(void);
static ssize_t get_size(void);
static off_t   get_am64_t(void);
static int     get_bool(void);
static void    ckseen(int *seen);
static void    conf_parserror(const char *format, ...)
                __attribute__ ((format (printf, 1, 2)));
static tok_t   lookup_keyword(char *str);

static void read_string(t_conf_var *, val_t *);
static void read_ident(t_conf_var *, val_t *);
static void read_int(t_conf_var *, val_t *);
/*static void read_long(t_conf_var *, val_t *);*/
static void read_size(t_conf_var *, val_t *);
static void read_am64(t_conf_var *, val_t *);
static void read_bool(t_conf_var *, val_t *);
static void read_real(t_conf_var *, val_t *);
static void read_time(t_conf_var *, val_t *);
static void read_intrange(t_conf_var *, val_t *);
static void conf_init_string(val_t *, char *);
static void conf_init_ident(val_t *, char *);
static void conf_init_int(val_t *, int);
static void conf_init_bool(val_t *, int);
static void conf_init_strategy(val_t *, int);
static void conf_init_estimate(val_t *, int);
static void conf_init_taperalgo(val_t *, int);
static void conf_init_priority(val_t *, int);
static void conf_init_strategy(val_t *, int);
static void conf_init_compress(val_t *, comp_t);
static void conf_init_encrypt(val_t *, encrypt_t);
static void conf_init_holding(val_t *, dump_holdingdisk_t);
/*static void conf_init_long(val_t *, long);*/
static void conf_init_size(val_t *, ssize_t);
static void conf_init_am64(val_t *, off_t);
static void conf_init_real(val_t *, double);
static void conf_init_rate(val_t *, double, double);
static void conf_init_intrange(val_t *, int, int);
static void conf_init_time(val_t *, time_t);
/*static void conf_init_sl(val_t *, sl_t *);*/
static void conf_init_exinclude(val_t *);
static void conf_set_string(val_t *, char *);
/*static void conf_set_int(val_t *, int);*/
static void conf_set_bool(val_t *, int);
static void conf_set_compress(val_t *, comp_t);
/*static void conf_set_encrypt(val_t *, encrypt_t);*/
static void conf_set_holding(val_t *, dump_holdingdisk_t);
static void conf_set_strategy(val_t *, int);

static void init_defaults(void);
static void read_conffile_recursively(char *filename);
static void read_client_conffile_recursively(char *filename);

static int read_confline(void);
static int read_client_confline(void);

static void read_block(command_option_t *command_options, t_conf_var *read_var,
                       keytab_t *keytab, val_t *valarray, char *prefix,
		       char *errormsg, int read_brace,
		       void (*copy_function)(void));

static void copy_val_t(val_t *, val_t *);
static void free_val_t(val_t *);
static char *conf_print(val_t *, int, char *);
static void conf_print_exinclude(val_t *, int, int, char *prefix, char **buf, int *free_space);

static void get_holdingdisk(void);
static void init_holdingdisk_defaults(void);
static void save_holdingdisk(void);
static void get_dumptype(void);
static void init_dumptype_defaults(void);
static void save_dumptype(void);
static void copy_dumptype(void);
static void get_tapetype(void);
static void init_tapetype_defaults(void);
static void save_tapetype(void);
static void copy_tapetype(void);
static void get_interface(void);
static void init_interface_defaults(void);
static void save_interface(void);
static void copy_interface(void);
static void get_comprate(t_conf_var *, val_t *);
static void get_compress(t_conf_var *, val_t *);
static void get_encrypt (t_conf_var *, val_t *);
static void get_holding (t_conf_var *, val_t *);
static void get_priority(t_conf_var *, val_t *);
static void get_strategy(t_conf_var *, val_t *);
static void get_estimate(t_conf_var *, val_t *);
static void get_exclude (t_conf_var *, val_t *);
/*static void get_include(t_conf_var *, val_t *);*/
static void get_taperalgo(t_conf_var *, val_t *);

static int  conftoken_getc(void);
static int  conftoken_ungetc(int c);
static void unget_conftoken(void);
static void get_conftoken(tok_t exp);

keytab_t   *my_keytab = NULL;
t_conf_var *my_var = NULL;

keytab_t client_keytab[] = {
    { "CONF", CONF_CONF },
    { "INDEX_SERVER", CONF_INDEX_SERVER },
    { "TAPE_SERVER", CONF_TAPE_SERVER },
    { "TAPEDEV", CONF_TAPEDEV },
    { "AUTH", CONF_AUTH },
    { "SSH_KEYS", CONF_SSH_KEYS },
    { "AMANDAD_PATH", CONF_AMANDAD_PATH },
    { "CLIENT_USERNAME", CONF_CLIENT_USERNAME },
    { "GNUTAR_LIST_DIR", CONF_GNUTAR_LIST_DIR },
    { "AMANDATES", CONF_AMANDATES },
    { "KRB5KEYTAB", CONF_KRB5KEYTAB },
    { "KRB5PRINCIPAL", CONF_KRB5PRINCIPAL },
    { "INCLUDEFILE", CONF_INCLUDEFILE },
    { "CONNECT_TRIES", CONF_CONNECT_TRIES },
    { "REP_TRIES", CONF_REP_TRIES },
    { "REQ_TRIES", CONF_REQ_TRIES },
    { "DEBUG_AMANDAD", CONF_DEBUG_AMANDAD },
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
    { "UNRESERVED-TCP-PORT", CONF_UNRESERVED_TCP_PORT },
    { NULL, CONF_UNKNOWN },
};

t_conf_var client_var [] = {
   { CONF_CONF               , CONFTYPE_STRING  , read_string  , CNF_CONF               , NULL },
   { CONF_INDEX_SERVER       , CONFTYPE_STRING  , read_string  , CNF_INDEX_SERVER       , NULL },
   { CONF_TAPE_SERVER        , CONFTYPE_STRING  , read_string  , CNF_TAPE_SERVER        , NULL },
   { CONF_TAPEDEV            , CONFTYPE_STRING  , read_string  , CNF_TAPEDEV            , NULL },
   { CONF_AUTH               , CONFTYPE_STRING  , read_string  , CNF_AUTH               , NULL },
   { CONF_SSH_KEYS           , CONFTYPE_STRING  , read_string  , CNF_SSH_KEYS           , NULL },
   { CONF_AMANDAD_PATH       , CONFTYPE_STRING  , read_string  , CNF_AMANDAD_PATH       , NULL },
   { CONF_CLIENT_USERNAME    , CONFTYPE_STRING  , read_string  , CNF_CLIENT_USERNAME    , NULL },
   { CONF_GNUTAR_LIST_DIR    , CONFTYPE_STRING  , read_string  , CNF_GNUTAR_LIST_DIR    , NULL },
   { CONF_AMANDATES          , CONFTYPE_STRING  , read_string  , CNF_AMANDATES          , NULL },
   { CONF_KRB5KEYTAB         , CONFTYPE_STRING  , read_string  , CNF_KRB5KEYTAB         , NULL },
   { CONF_KRB5PRINCIPAL      , CONFTYPE_STRING  , read_string  , CNF_KRB5PRINCIPAL      , NULL },
   { CONF_CONNECT_TRIES      , CONFTYPE_INT     , read_int     , CNF_CONNECT_TRIES      , validate_positive1 },
   { CONF_REP_TRIES          , CONFTYPE_INT     , read_int     , CNF_REP_TRIES          , validate_positive1 },
   { CONF_REQ_TRIES          , CONFTYPE_INT     , read_int     , CNF_REQ_TRIES          , validate_positive1 },
   { CONF_DEBUG_AMANDAD      , CONFTYPE_INT     , read_int     , CNF_DEBUG_AMANDAD      , validate_debug },
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
   { CONF_UNRESERVED_TCP_PORT, CONFTYPE_INTRANGE, read_intrange, CNF_UNRESERVED_TCP_PORT, validate_unreserved_port_range },
   { CONF_UNKNOWN            , CONFTYPE_INT     , NULL         , CNF_CNF                , NULL }
};

keytab_t server_keytab[] = {
    { "AMANDAD_PATH", CONF_AMANDAD_PATH },
    { "AMRECOVER_CHANGER", CONF_AMRECOVER_CHANGER },
    { "AMRECOVER_CHECK_LABEL", CONF_AMRECOVER_CHECK_LABEL },
    { "AMRECOVER_DO_FSF", CONF_AMRECOVER_DO_FSF },
    { "APPEND", CONF_APPEND },
    { "AUTH", CONF_AUTH },
    { "AUTO", CONF_AUTO },
    { "AUTOFLUSH", CONF_AUTOFLUSH },
    { "BEST", CONF_BEST },
    { "BLOCKSIZE", CONF_BLOCKSIZE },
    { "BUMPDAYS", CONF_BUMPDAYS },
    { "BUMPMULT", CONF_BUMPMULT },
    { "BUMPPERCENT", CONF_BUMPPERCENT },
    { "BUMPSIZE", CONF_BUMPSIZE },
    { "CALCSIZE", CONF_CALCSIZE },
    { "CHANGERDEV", CONF_CHNGRDEV },
    { "CHANGERFILE", CONF_CHNGRFILE },
    { "CHUNKSIZE", CONF_CHUNKSIZE },
    { "CLIENT", CONF_CLIENT },
    { "CLIENT_CUSTOM_COMPRESS", CONF_CLNTCOMPPROG },
    { "CLIENT_DECRYPT_OPTION", CONF_CLNT_DECRYPT_OPT },
    { "CLIENT_ENCRYPT", CONF_CLNT_ENCRYPT },
    { "CLIENT_USERNAME", CONF_CLIENT_USERNAME },
    { "COLUMNSPEC", CONF_COLUMNSPEC },
    { "COMMENT", CONF_COMMENT },
    { "COMPRATE", CONF_COMPRATE },
    { "COMPRESS", CONF_COMPRESS },
    { "CONNECT_TRIES", CONF_CONNECT_TRIES },
    { "CTIMEOUT", CONF_CTIMEOUT },
    { "CUSTOM", CONF_CUSTOM },
    { "DEBUG_AMANDAD"    , CONF_DEBUG_AMANDAD },
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
    { "DIRECTORY", CONF_DIRECTORY },
    { "DISKFILE", CONF_DISKFILE },
    { "DISPLAYUNIT", CONF_DISPLAYUNIT },
    { "DTIMEOUT", CONF_DTIMEOUT },
    { "DUMPCYCLE", CONF_DUMPCYCLE },
    { "DUMPORDER", CONF_DUMPORDER },
    { "DUMPTYPE", CONF_DUMPTYPE },
    { "DUMPUSER", CONF_DUMPUSER },
    { "ENCRYPT", CONF_ENCRYPT },
    { "ESTIMATE", CONF_ESTIMATE },
    { "ETIMEOUT", CONF_ETIMEOUT },
    { "EXCLUDE", CONF_EXCLUDE },
    { "EXCLUDE-FILE", CONF_EXCLUDE_FILE },
    { "EXCLUDE-LIST", CONF_EXCLUDE_LIST },
    { "FALLBACK_SPLITSIZE", CONF_FALLBACK_SPLITSIZE },
    { "FAST", CONF_FAST },
    { "FILE", CONF_EFILE },
    { "FILE-PAD", CONF_FILE_PAD },
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
    { "INTERFACE", CONF_INTERFACE },
    { "KENCRYPT", CONF_KENCRYPT },
    { "KRB5KEYTAB", CONF_KRB5KEYTAB },
    { "KRB5PRINCIPAL", CONF_KRB5PRINCIPAL },
    { "LABELSTR", CONF_LABELSTR },
    { "LABEL_NEW_TAPES", CONF_LABEL_NEW_TAPES },
    { "LARGEST", CONF_LARGEST },
    { "LARGESTFIT", CONF_LARGESTFIT },
    { "LAST", CONF_LAST },
    { "LBL-TEMPL", CONF_LBL_TEMPL },
    { "LENGTH", CONF_LENGTH },
    { "LIST", CONF_LIST },
    { "LOGDIR", CONF_LOGDIR },
    { "LOW", CONF_LOW },
    { "MAILTO", CONF_MAILTO },
    { "READBLOCKSIZE", CONF_READBLOCKSIZE },
    { "MAXDUMPS", CONF_MAXDUMPS },
    { "MAXDUMPSIZE", CONF_MAXDUMPSIZE },
    { "MAXPROMOTEDAY", CONF_MAXPROMOTEDAY },
    { "MEDIUM", CONF_MEDIUM },
    { "NETUSAGE", CONF_NETUSAGE },	/* XXX - historical */
    { "NEVER", CONF_NEVER },
    { "NOFULL", CONF_NOFULL },
    { "NOINC", CONF_NOINC },
    { "NONE", CONF_NONE },
    { "OPTIONAL", CONF_OPTIONAL },
    { "ORG", CONF_ORG },
    { "PRINTER", CONF_PRINTER },
    { "PRIORITY", CONF_PRIORITY },
    { "PROGRAM", CONF_PROGRAM },
    { "RAWTAPEDEV", CONF_RAWTAPEDEV },
    { "RECORD", CONF_RECORD },
    { "REP_TRIES", CONF_REP_TRIES },
    { "REQ_TRIES", CONF_REQ_TRIES },
    { "REQUIRED", CONF_REQUIRED },
    { "RESERVE", CONF_RESERVE },
    { "RESERVED-UDP-PORT", CONF_RESERVED_UDP_PORT },
    { "RESERVED-TCP-PORT", CONF_RESERVED_TCP_PORT },
    { "RUNSPERCYCLE", CONF_RUNSPERCYCLE },
    { "RUNTAPES", CONF_RUNTAPES },
    { "SERVER", CONF_SERVER },
    { "SERVER_CUSTOM_COMPRESS", CONF_SRVCOMPPROG },
    { "SERVER_DECRYPT_OPTION", CONF_SRV_DECRYPT_OPT },
    { "SERVER_ENCRYPT", CONF_SRV_ENCRYPT },
    { "SKIP", CONF_SKIP },
    { "SKIP-FULL", CONF_SKIP_FULL },
    { "SKIP-INCR", CONF_SKIP_INCR },
    { "SMALLEST", CONF_SMALLEST },
    { "SPEED", CONF_SPEED },
    { "SPLIT_DISKBUFFER", CONF_SPLIT_DISKBUFFER },
    { "SSH_KEYS", CONF_SSH_KEYS },
    { "STANDARD", CONF_STANDARD },
    { "STARTTIME", CONF_STARTTIME },
    { "STRATEGY", CONF_STRATEGY },
    { "TAPEBUFS", CONF_TAPEBUFS },
    { "TAPECYCLE", CONF_TAPECYCLE },
    { "TAPEDEV", CONF_TAPEDEV },
    { "TAPELIST", CONF_TAPELIST },
    { "TAPERALGO", CONF_TAPERALGO },
    { "TAPETYPE", CONF_TAPETYPE },
    { "TAPE_SPLITSIZE", CONF_TAPE_SPLITSIZE },
    { "TPCHANGER", CONF_TPCHANGER },
    { "UNRESERVED-TCP-PORT", CONF_UNRESERVED_TCP_PORT },
    { "USE", CONF_USE },
    { "USETIMESTAMPS", CONF_USETIMESTAMPS },
    { NULL, CONF_IDENT },
    { NULL, CONF_UNKNOWN }
};

t_conf_var server_var [] = {
   { CONF_ORG                  , CONFTYPE_STRING   , read_string  , CNF_ORG                  , NULL },
   { CONF_MAILTO               , CONFTYPE_STRING   , read_string  , CNF_MAILTO               , NULL },
   { CONF_DUMPUSER             , CONFTYPE_STRING   , read_string  , CNF_DUMPUSER             , NULL },
   { CONF_PRINTER              , CONFTYPE_STRING   , read_string  , CNF_PRINTER              , NULL },
   { CONF_TAPEDEV              , CONFTYPE_STRING   , read_string  , CNF_TAPEDEV              , NULL },
   { CONF_TPCHANGER            , CONFTYPE_STRING   , read_string  , CNF_TPCHANGER            , NULL },
   { CONF_CHNGRDEV             , CONFTYPE_STRING   , read_string  , CNF_CHNGRDEV             , NULL },
   { CONF_CHNGRFILE            , CONFTYPE_STRING   , read_string  , CNF_CHNGRFILE            , NULL },
   { CONF_LABELSTR             , CONFTYPE_STRING   , read_string  , CNF_LABELSTR             , NULL },
   { CONF_TAPELIST             , CONFTYPE_STRING   , read_string  , CNF_TAPELIST             , NULL },
   { CONF_DISKFILE             , CONFTYPE_STRING   , read_string  , CNF_DISKFILE             , NULL },
   { CONF_INFOFILE             , CONFTYPE_STRING   , read_string  , CNF_INFOFILE             , NULL },
   { CONF_LOGDIR               , CONFTYPE_STRING   , read_string  , CNF_LOGDIR               , NULL },
   { CONF_INDEXDIR             , CONFTYPE_STRING   , read_string  , CNF_INDEXDIR             , NULL },
   { CONF_TAPETYPE             , CONFTYPE_IDENT    , read_ident   , CNF_TAPETYPE             , NULL },
   { CONF_DUMPCYCLE            , CONFTYPE_INT      , read_int     , CNF_DUMPCYCLE            , validate_positive0 },
   { CONF_RUNSPERCYCLE         , CONFTYPE_INT      , read_int     , CNF_RUNSPERCYCLE         , validate_runspercycle },
   { CONF_RUNTAPES             , CONFTYPE_INT      , read_int     , CNF_RUNTAPES             , validate_positive0 },
   { CONF_TAPECYCLE            , CONFTYPE_INT      , read_int     , CNF_TAPECYCLE            , validate_positive1 },
   { CONF_BUMPDAYS             , CONFTYPE_INT      , read_int     , CNF_BUMPDAYS             , validate_positive1 },
   { CONF_BUMPSIZE             , CONFTYPE_AM64     , read_am64    , CNF_BUMPSIZE             , validate_positive1 },
   { CONF_BUMPPERCENT          , CONFTYPE_INT      , read_int     , CNF_BUMPPERCENT          , validate_bumppercent },
   { CONF_BUMPMULT             , CONFTYPE_REAL     , read_real    , CNF_BUMPMULT             , validate_bumpmult },
   { CONF_NETUSAGE             , CONFTYPE_INT      , read_int     , CNF_NETUSAGE             , validate_positive1 },
   { CONF_INPARALLEL           , CONFTYPE_INT      , read_int     , CNF_INPARALLEL           , validate_inparallel },
   { CONF_DUMPORDER            , CONFTYPE_STRING   , read_string  , CNF_DUMPORDER            , NULL },
   { CONF_MAXDUMPS             , CONFTYPE_INT      , read_int     , CNF_MAXDUMPS             , validate_positive1 },
   { CONF_ETIMEOUT             , CONFTYPE_INT      , read_int     , CNF_ETIMEOUT             , NULL },
   { CONF_DTIMEOUT             , CONFTYPE_INT      , read_int     , CNF_DTIMEOUT             , validate_positive1 },
   { CONF_CTIMEOUT             , CONFTYPE_INT      , read_int     , CNF_CTIMEOUT             , validate_positive1 },
   { CONF_TAPEBUFS             , CONFTYPE_INT      , read_int     , CNF_TAPEBUFS             , validate_positive1 },
   { CONF_RAWTAPEDEV           , CONFTYPE_STRING   , read_string  , CNF_RAWTAPEDEV           , NULL },
   { CONF_COLUMNSPEC           , CONFTYPE_STRING   , read_string  , CNF_COLUMNSPEC           , NULL },
   { CONF_TAPERALGO            , CONFTYPE_TAPERALGO, get_taperalgo, CNF_TAPERALGO            , NULL },
   { CONF_DISPLAYUNIT          , CONFTYPE_STRING   , read_string  , CNF_DISPLAYUNIT          , validate_displayunit },
   { CONF_AUTOFLUSH            , CONFTYPE_BOOL     , read_bool    , CNF_AUTOFLUSH            , NULL },
   { CONF_RESERVE              , CONFTYPE_INT      , read_int     , CNF_RESERVE              , validate_reserve },
   { CONF_MAXDUMPSIZE          , CONFTYPE_AM64     , read_am64    , CNF_MAXDUMPSIZE          , NULL },
   { CONF_KRB5KEYTAB           , CONFTYPE_STRING   , read_string  , CNF_KRB5KEYTAB           , NULL },
   { CONF_KRB5PRINCIPAL        , CONFTYPE_STRING   , read_string  , CNF_KRB5PRINCIPAL        , NULL },
   { CONF_LABEL_NEW_TAPES      , CONFTYPE_STRING   , read_string  , CNF_LABEL_NEW_TAPES      , NULL },
   { CONF_USETIMESTAMPS        , CONFTYPE_BOOL     , read_bool    , CNF_USETIMESTAMPS        , NULL },
   { CONF_AMRECOVER_DO_FSF     , CONFTYPE_BOOL     , read_bool    , CNF_AMRECOVER_DO_FSF     , NULL },
   { CONF_AMRECOVER_CHANGER    , CONFTYPE_STRING   , read_string  , CNF_AMRECOVER_CHANGER    , NULL },
   { CONF_AMRECOVER_CHECK_LABEL, CONFTYPE_BOOL     , read_bool    , CNF_AMRECOVER_CHECK_LABEL, NULL },
   { CONF_CONNECT_TRIES        , CONFTYPE_INT      , read_int     , CNF_CONNECT_TRIES        , validate_positive1 },
   { CONF_REP_TRIES            , CONFTYPE_INT      , read_int     , CNF_REP_TRIES            , validate_positive1 },
   { CONF_REQ_TRIES            , CONFTYPE_INT      , read_int     , CNF_REQ_TRIES            , validate_positive1 },
   { CONF_DEBUG_AMANDAD        , CONFTYPE_INT      , read_int     , CNF_DEBUG_AMANDAD        , validate_debug },
   { CONF_DEBUG_AMIDXTAPED     , CONFTYPE_INT      , read_int     , CNF_DEBUG_AMIDXTAPED     , validate_debug },
   { CONF_DEBUG_AMINDEXD       , CONFTYPE_INT      , read_int     , CNF_DEBUG_AMINDEXD       , validate_debug },
   { CONF_DEBUG_AMRECOVER      , CONFTYPE_INT      , read_int     , CNF_DEBUG_AMRECOVER      , validate_debug },
   { CONF_DEBUG_AUTH           , CONFTYPE_INT      , read_int     , CNF_DEBUG_AUTH           , validate_debug },
   { CONF_DEBUG_EVENT          , CONFTYPE_INT      , read_int     , CNF_DEBUG_EVENT          , validate_debug },
   { CONF_DEBUG_HOLDING        , CONFTYPE_INT      , read_int     , CNF_DEBUG_HOLDING        , validate_debug },
   { CONF_DEBUG_PROTOCOL       , CONFTYPE_INT      , read_int     , CNF_DEBUG_PROTOCOL       , validate_debug },
   { CONF_DEBUG_PLANNER        , CONFTYPE_INT      , read_int     , CNF_DEBUG_PLANNER        , validate_debug },
   { CONF_DEBUG_DRIVER         , CONFTYPE_INT      , read_int     , CNF_DEBUG_DRIVER         , validate_debug },
   { CONF_DEBUG_DUMPER         , CONFTYPE_INT      , read_int     , CNF_DEBUG_DUMPER         , validate_debug },
   { CONF_DEBUG_CHUNKER        , CONFTYPE_INT      , read_int     , CNF_DEBUG_CHUNKER        , validate_debug },
   { CONF_DEBUG_TAPER          , CONFTYPE_INT      , read_int     , CNF_DEBUG_TAPER          , validate_debug },
   { CONF_DEBUG_SELFCHECK      , CONFTYPE_INT      , read_int     , CNF_DEBUG_SELFCHECK      , validate_debug },
   { CONF_DEBUG_SENDSIZE       , CONFTYPE_INT      , read_int     , CNF_DEBUG_SENDSIZE       , validate_debug },
   { CONF_DEBUG_SENDBACKUP     , CONFTYPE_INT      , read_int     , CNF_DEBUG_SENDBACKUP     , validate_debug },
   { CONF_RESERVED_UDP_PORT    , CONFTYPE_INTRANGE , read_intrange, CNF_RESERVED_UDP_PORT    , validate_reserved_port_range },
   { CONF_RESERVED_TCP_PORT    , CONFTYPE_INTRANGE , read_intrange, CNF_RESERVED_TCP_PORT    , validate_reserved_port_range },
   { CONF_UNRESERVED_TCP_PORT  , CONFTYPE_INTRANGE , read_intrange, CNF_UNRESERVED_TCP_PORT  , validate_unreserved_port_range },
   { CONF_UNKNOWN              , CONFTYPE_INT      , NULL         , CNF_CNF                  , NULL }
};

t_conf_var tapetype_var [] = {
   { CONF_COMMENT     , CONFTYPE_STRING, read_string, TAPETYPE_COMMENT      , NULL },
   { CONF_LBL_TEMPL   , CONFTYPE_STRING, read_string, TAPETYPE_LBL_TEMPL    , NULL },
   { CONF_BLOCKSIZE   , CONFTYPE_SIZE  , read_size  , TAPETYPE_BLOCKSIZE    , validate_blocksize },
   { CONF_READBLOCKSIZE, CONFTYPE_SIZE  , read_size , TAPETYPE_READBLOCKSIZE, validate_blocksize },
   { CONF_LENGTH      , CONFTYPE_AM64  , read_am64  , TAPETYPE_LENGTH       , validate_positive0 },
   { CONF_FILEMARK    , CONFTYPE_AM64  , read_am64  , TAPETYPE_FILEMARK     , NULL },
   { CONF_SPEED       , CONFTYPE_INT   , read_int   , TAPETYPE_SPEED        , validate_positive0 },
   { CONF_FILE_PAD    , CONFTYPE_BOOL  , read_bool  , TAPETYPE_FILE_PAD     , NULL },
   { CONF_UNKNOWN     , CONFTYPE_INT   , NULL       , TAPETYPE_TAPETYPE     , NULL }
};

t_conf_var dumptype_var [] = {
   { CONF_COMMENT           , CONFTYPE_STRING   , read_string , DUMPTYPE_COMMENT           , NULL },
   { CONF_AUTH              , CONFTYPE_STRING   , read_string , DUMPTYPE_SECURITY_DRIVER   , NULL },
   { CONF_BUMPDAYS          , CONFTYPE_INT      , read_int    , DUMPTYPE_BUMPDAYS          , NULL },
   { CONF_BUMPMULT          , CONFTYPE_REAL     , read_real   , DUMPTYPE_BUMPMULT          , NULL },
   { CONF_BUMPSIZE          , CONFTYPE_AM64     , read_am64   , DUMPTYPE_BUMPSIZE          , NULL },
   { CONF_BUMPPERCENT       , CONFTYPE_INT      , read_int    , DUMPTYPE_BUMPPERCENT       , NULL },
   { CONF_COMPRATE          , CONFTYPE_REAL     , get_comprate, DUMPTYPE_COMPRATE          , NULL },
   { CONF_COMPRESS          , CONFTYPE_INT      , get_compress, DUMPTYPE_COMPRESS          , NULL },
   { CONF_ENCRYPT           , CONFTYPE_INT      , get_encrypt , DUMPTYPE_ENCRYPT           , NULL },
   { CONF_DUMPCYCLE         , CONFTYPE_INT      , read_int    , DUMPTYPE_DUMPCYCLE         , validate_positive0 },
   { CONF_EXCLUDE           , CONFTYPE_EXINCLUDE, get_exclude , DUMPTYPE_EXCLUDE           , NULL },
   { CONF_INCLUDE           , CONFTYPE_EXINCLUDE, get_exclude , DUMPTYPE_INCLUDE           , NULL },
   { CONF_IGNORE            , CONFTYPE_BOOL     , read_bool   , DUMPTYPE_IGNORE            , NULL },
   { CONF_HOLDING           , CONFTYPE_HOLDING  , get_holding , DUMPTYPE_HOLDINGDISK       , NULL },
   { CONF_INDEX             , CONFTYPE_BOOL     , read_bool   , DUMPTYPE_INDEX             , NULL },
   { CONF_KENCRYPT          , CONFTYPE_BOOL     , read_bool   , DUMPTYPE_KENCRYPT          , NULL },
   { CONF_MAXDUMPS          , CONFTYPE_INT      , read_int    , DUMPTYPE_MAXDUMPS          , validate_positive1 },
   { CONF_MAXPROMOTEDAY     , CONFTYPE_INT      , read_int    , DUMPTYPE_MAXPROMOTEDAY     , validate_positive0 },
   { CONF_PRIORITY          , CONFTYPE_PRIORITY , get_priority, DUMPTYPE_PRIORITY          , NULL },
   { CONF_PROGRAM           , CONFTYPE_STRING   , read_string , DUMPTYPE_PROGRAM           , NULL },
   { CONF_RECORD            , CONFTYPE_BOOL     , read_bool   , DUMPTYPE_RECORD            , NULL },
   { CONF_SKIP_FULL         , CONFTYPE_BOOL     , read_bool   , DUMPTYPE_SKIP_FULL         , NULL },
   { CONF_SKIP_INCR         , CONFTYPE_BOOL     , read_bool   , DUMPTYPE_SKIP_INCR         , NULL },
   { CONF_STARTTIME         , CONFTYPE_TIME     , read_time   , DUMPTYPE_STARTTIME         , NULL },
   { CONF_STRATEGY          , CONFTYPE_INT      , get_strategy, DUMPTYPE_STRATEGY          , NULL },
   { CONF_TAPE_SPLITSIZE    , CONFTYPE_AM64     , read_am64   , DUMPTYPE_TAPE_SPLITSIZE    , validate_positive0 },
   { CONF_SPLIT_DISKBUFFER  , CONFTYPE_STRING   , read_string , DUMPTYPE_SPLIT_DISKBUFFER  , NULL },
   { CONF_ESTIMATE          , CONFTYPE_INT      , get_estimate, DUMPTYPE_ESTIMATE          , NULL },
   { CONF_SRV_ENCRYPT       , CONFTYPE_STRING   , read_string , DUMPTYPE_SRV_ENCRYPT       , NULL },
   { CONF_CLNT_ENCRYPT      , CONFTYPE_STRING   , read_string , DUMPTYPE_CLNT_ENCRYPT      , NULL },
   { CONF_AMANDAD_PATH      , CONFTYPE_STRING   , read_string , DUMPTYPE_AMANDAD_PATH      , NULL },
   { CONF_CLIENT_USERNAME   , CONFTYPE_STRING   , read_string , DUMPTYPE_CLIENT_USERNAME   , NULL },
   { CONF_SSH_KEYS          , CONFTYPE_STRING   , read_string , DUMPTYPE_SSH_KEYS          , NULL },
   { CONF_SRVCOMPPROG       , CONFTYPE_STRING   , read_string , DUMPTYPE_SRVCOMPPROG       , NULL },
   { CONF_CLNTCOMPPROG      , CONFTYPE_STRING   , read_string , DUMPTYPE_CLNTCOMPPROG      , NULL },
   { CONF_FALLBACK_SPLITSIZE, CONFTYPE_AM64     , read_am64   , DUMPTYPE_FALLBACK_SPLITSIZE, NULL },
   { CONF_SRV_DECRYPT_OPT   , CONFTYPE_STRING   , read_string , DUMPTYPE_SRV_DECRYPT_OPT   , NULL },
   { CONF_CLNT_DECRYPT_OPT  , CONFTYPE_STRING   , read_string , DUMPTYPE_CLNT_DECRYPT_OPT  , NULL },
   { CONF_UNKNOWN           , CONFTYPE_INT      , NULL        , DUMPTYPE_DUMPTYPE          , NULL }
};

t_conf_var holding_var [] = {
   { CONF_DIRECTORY, CONFTYPE_STRING, read_string, HOLDING_DISKDIR  , NULL },
   { CONF_COMMENT  , CONFTYPE_STRING, read_string, HOLDING_COMMENT  , NULL },
   { CONF_USE      , CONFTYPE_AM64  , read_am64  , HOLDING_DISKSIZE , validate_use },
   { CONF_CHUNKSIZE, CONFTYPE_AM64  , read_am64  , HOLDING_CHUNKSIZE, validate_chunksize },
   { CONF_UNKNOWN  , CONFTYPE_INT   , NULL       , HOLDING_HOLDING  , NULL }
};

/*
** ------------------------
**  External entry points
** ------------------------
*/

int
read_conffile(
    char *filename)
{
    interface_t *ip;

    my_keytab = server_keytab;
    my_var = server_var;
    init_defaults();

    /* We assume that conf_confname & conf are initialized to NULL above */
    read_conffile_recursively(filename);

    /* overwrite with command line option */
    command_overwrite(program_options, my_var, my_keytab, conf_data,
		      "");

    if(got_parserror != -1 ) {
	if(lookup_tapetype(conf_data[CNF_TAPETYPE].v.s) == NULL) {
	    char *save_confname = conf_confname;

	    conf_confname = filename;
	    if(!conf_data[CNF_TAPETYPE].seen)
		conf_parserror(_("default tapetype %s not defined"), conf_data[CNF_TAPETYPE].v.s);
	    else {
		conf_line_num = conf_data[CNF_TAPETYPE].seen;
		conf_parserror(_("tapetype %s not defined"), conf_data[CNF_TAPETYPE].v.s);
	    }
	    conf_confname = save_confname;
	}
    }

    ip = alloc(SIZEOF(interface_t));
    ip->name = stralloc("default");
    ip->seen = conf_data[CNF_NETUSAGE].seen;
    conf_init_string(&ip->value[INTER_COMMENT], _("implicit from NETUSAGE"));
    conf_init_int(&ip->value[INTER_MAXUSAGE], conf_data[CNF_NETUSAGE].v.i);
    ip->curusage = 0;
    ip->next = interface_list;
    interface_list = ip;

    debug_amandad    = getconf_int(CNF_DEBUG_AMANDAD);
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

    return got_parserror;
}

static void
validate_positive0(
    struct s_conf_var *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val->v.i < 0)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_LONG:
	if(val->v.l < 0)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_AM64:
	if(val->v.am64 < 0)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    default:
	conf_parserror(_("validate_positive0 invalid type %d\n"), val->type);
    }
}

static void
validate_positive1(
    struct s_conf_var *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val->v.i < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_LONG:
	if(val->v.l < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_AM64:
	if(val->v.am64 < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    case CONFTYPE_TIME:
	if(val->v.t < 1)
	    conf_parserror(_("%s must be positive"), get_token_name(np->token));
	break;
    default:
	conf_parserror(_("validate_positive1 invalid type %d\n"), val->type);
    }
}

static void
validate_runspercycle(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < -1)
	conf_parserror(_("runspercycle must be >= -1"));
}

static void
validate_bumppercent(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 0 || val->v.i > 100)
	conf_parserror(_("bumppercent must be between 0 and 100"));
}

static void
validate_inparallel(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 1 || val->v.i >MAX_DUMPERS)
	conf_parserror(_("inparallel must be between 1 and MAX_DUMPERS (%d)"),
		       MAX_DUMPERS);
}

static void
validate_bumpmult(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.r < 0.999) {
	conf_parserror(_("bumpmult must be positive"));
    }
}

static void
validate_displayunit(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(strcmp(val->v.s, "k") == 0 ||
       strcmp(val->v.s, "K") == 0) {
	val->v.s[0] = (char)toupper(val->v.s[0]);
	unit_divisor=1;
    }
    else if(strcmp(val->v.s, "m") == 0 ||
       strcmp(val->v.s, "M") == 0) {
	val->v.s[0] = (char)toupper(val->v.s[0]);
	unit_divisor=1024;
    }
    else if(strcmp(val->v.s, "g") == 0 ||
       strcmp(val->v.s, "G") == 0) {
	val->v.s[0] = (char)toupper(val->v.s[0]);
	unit_divisor=1024*1024;
    }
    else if(strcmp(val->v.s, "t") == 0 ||
       strcmp(val->v.s, "T") == 0) {
	val->v.s[0] = (char)toupper(val->v.s[0]);
	unit_divisor=1024*1024*1024;
    }
    else {
	conf_parserror(_("displayunit must be k,m,g or t."));
    }
}

static void
validate_reserve(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 0 || val->v.i > 100)
	conf_parserror(_("reserve must be between 0 and 100"));
}

static void
validate_use(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    val->v.am64 = am_floor(val->v.am64, DISK_BLOCK_KB);
}

static void
validate_chunksize(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.am64 == 0) {
	val->v.am64 = ((AM64_MAX / 1024) - (2 * DISK_BLOCK_KB));
    }
    else if(val->v.am64 < 0) {
	conf_parserror(_("Negative chunksize ("OFF_T_FMT") is no longer supported"), (OFF_T_FMT_TYPE)val->v.am64);
    }
    val->v.am64 = am_floor(val->v.am64, (off_t)DISK_BLOCK_KB);
    if (val->v.am64 < 2*DISK_BLOCK_KB) {
	conf_parserror("chunksize must be at least %dkb", 2*DISK_BLOCK_KB);
    }
}

static void
validate_blocksize(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.l < DISK_BLOCK_KB) {
	conf_parserror(_("Tape blocksize must be at least %d KBytes"),
		  DISK_BLOCK_KB);
    } else if(val->v.l > MAX_TAPE_BLOCK_KB) {
	conf_parserror(_("Tape blocksize must not be larger than %d KBytes"),
		  MAX_TAPE_BLOCK_KB);
    }
}

static void
validate_debug(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 0 || val->v.i > 9) {
	conf_parserror(_("Debug must be between 0 and 9"));
    }
}

static void
validate_reserved_port_range(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.intrange[0] < 1 || val->v.intrange[0] > IPPORT_RESERVED-1) {
	conf_parserror(_("portrange must be between 1 and %d"), IPPORT_RESERVED-1);
    } else if(val->v.intrange[1] < 1 || val->v.intrange[1] > IPPORT_RESERVED-1) {
	conf_parserror(_("portrange must be between 1 and IPPORT_RESERVED-1"));
    }
}

static void
validate_unreserved_port_range(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.intrange[0] < IPPORT_RESERVED+1 || val->v.intrange[0] > 65536) {
	conf_parserror(_("portrange must be between %d and 65536"), IPPORT_RESERVED+1);
    } else if(val->v.intrange[1] < IPPORT_RESERVED+1 || val->v.intrange[1] > 65536) {
	conf_parserror(_("portrange must be between %d and 65536"), IPPORT_RESERVED+1);
    }
}

char *
getconf_byname(
    char *str)
{
    static char *tmpstr;
    t_conf_var *np;
    keytab_t *kt;
    char *s;
    char ch;
    char *first_delim;
    char *second_delim;
    tapetype_t *tp;
    dumptype_t *dp;
    interface_t *ip;
    holdingdisk_t *hp;

    tmpstr = stralloc(str);
    s = tmpstr;
    while((ch = *s++) != '\0') {
	if(islower((int)ch))
	    s[-1] = (char)toupper(ch);
    }

    first_delim = strchr(tmpstr, ':');
    if (first_delim) {
	*first_delim = '\0';
	first_delim++;
	second_delim = strchr(first_delim,':');
	if(!second_delim) {
	    amfree(tmpstr);
	    return(NULL);
	}
	*second_delim = '\0';
	second_delim++;

	for(kt = my_keytab; kt->token != CONF_UNKNOWN; kt++) {
	    if(kt->keyword && strcmp(kt->keyword, second_delim) == 0)
		break;
	}

	if(kt->token == CONF_UNKNOWN)
	    return NULL;

	if (strcmp(tmpstr, "TAPETYPE") == 0) {
	    tp = lookup_tapetype(first_delim);
	    if (!tp) {
		amfree(tmpstr);
		return(NULL);
	    }
	    for(np = tapetype_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) return NULL;
	    tmpstr = stralloc(conf_print(&tp->value[np->parm], 0, ""));
	} else if (strcmp(tmpstr, "DUMPTYPE") == 0) {
	    dp = lookup_dumptype(first_delim);
	    if (!dp) {
		amfree(tmpstr);
		return(NULL);
	    }
	    for(np = dumptype_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) return NULL;
	    tmpstr = stralloc(conf_print(&dp->value[np->parm], 0, ""));
	} else if (strcmp(tmpstr, "HOLDINGDISK") == 0) {
	    hp = lookup_holdingdisk(first_delim);
	    if (!hp) {
		amfree(tmpstr);
		return(NULL);
	    }
	    for(np = holding_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) return NULL;
	    tmpstr = stralloc(conf_print(&hp->value[np->parm], 0, ""));
	} else if (strcmp(tmpstr, "INTERFACE") == 0) {
	    ip = lookup_interface(first_delim);
	    if (!ip) {
		amfree(tmpstr);
		return(NULL);
	    }
	    for(np = holding_var; np->token != CONF_UNKNOWN; np++) {
		if(np->token == kt->token)
		   break;
	    }
	    if (np->token == CONF_UNKNOWN) return NULL;
	    tmpstr = stralloc(conf_print(&ip->value[np->parm], 0, ""));
	} else {
	    amfree(tmpstr);
	    return(NULL);
	}
    } else {
	for(kt = my_keytab; kt->token != CONF_UNKNOWN; kt++) {
	    if(kt->keyword && strcmp(kt->keyword, tmpstr) == 0)
		break;
	}

	if(kt->token == CONF_UNKNOWN)
	    return NULL;

	for(np = my_var; np->token != CONF_UNKNOWN; np++) {
	    if(np->token == kt->token)
		break;
	}

	if(np->token == CONF_UNKNOWN) return NULL;

	tmpstr = stralloc(conf_print(&conf_data[np->parm], 0, ""));
    }

    return tmpstr;
}


char *
getconf_list(
    char *listname)
{
    char *result = NULL;
    tapetype_t *tp;
    dumptype_t *dp;
    interface_t *ip;
    holdingdisk_t *hp;

    if (strcasecmp(listname,"tapetype") == 0) {
	result = stralloc("");
	for(tp = tapelist; tp != NULL; tp=tp->next) {
	    result = vstrextend(&result, tp->name, "\n", NULL);
	}
    } else if (strcasecmp(listname,"dumptype") == 0) {
	result = stralloc("");
	for(dp = dumplist; dp != NULL; dp=dp->next) {
	    result = vstrextend(&result, dp->name, "\n", NULL);
	}
    } else if (strcasecmp(listname,"holdingdisk") == 0) {
	result = stralloc("");
	for(hp = holdingdisks; hp != NULL; hp=hp->next) {
	    result = vstrextend(&result, hp->name, "\n", NULL);
	}
    } else if (strcasecmp(listname,"interface") == 0) {
	result = stralloc("");
	for(ip = interface_list; ip != NULL; ip=ip->next) {
	    result = vstrextend(&result, ip->name, "\n", NULL);
	}
    }
    return result;
}


int
getconf_seen(
    confparm_t parm)
{
    return(conf_data[parm].seen);
}

int
getconf_boolean(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_BOOL) {
	error(_("getconf_boolean: parm is not a CONFTYPE_BOOL"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.i != 0);
}

int
getconf_int(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_INT) {
	error(_("getconf_int: parm is not a CONFTYPE_INT"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.i);
}

long
getconf_long(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_LONG) {
	error(_("getconf_long: parm is not a CONFTYPE_LONG"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.l);
}

time_t
getconf_time(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_TIME) {
	error(_("getconf_time: parm is not a CONFTYPE_TIME"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.t);
}

ssize_t
getconf_size(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_SIZE) {
	error(_("getconf_size: parm is not a CONFTYPE_SIZE"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.size);
}

off_t
getconf_am64(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_AM64) {
	error(_("getconf_am64: parm is not a CONFTYPE_AM64"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.am64);
}

double
getconf_real(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_REAL) {
	error(_("getconf_real: parm is not a CONFTYPE_REAL"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.r);
}

char *
getconf_str(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_STRING &&
        conf_data[parm].type != CONFTYPE_IDENT) {
	error(_("getconf_str: parm is not a CONFTYPE_STRING|CONFTYPE_IDENT: %d"), parm);
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.s);
}

int
getconf_taperalgo(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_TAPERALGO) {
	error(_("getconf_taperalgo: parm is not a CONFTYPE_TAPERALGO"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.i);
}

int*
getconf_intrange(
    confparm_t parm)
{
    if (conf_data[parm].type != CONFTYPE_INTRANGE) {
	error(_("getconf_intrange: parm is not a CONFTYPE_INTRANGE"));
	/*NOTREACHED*/
    }
    return(conf_data[parm].v.intrange);
}

holdingdisk_t *
getconf_holdingdisks(
    void)
{
    return holdingdisks;
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

holdingdisk_t *
lookup_holdingdisk(
    char *str)
{
    holdingdisk_t *p;

    for(p = holdingdisks; p != NULL; p = p->next) {
	if(strcasecmp(p->name, str) == 0) return p;
    }
    return NULL;
}

interface_t *
lookup_interface(
    char *str)
{
#ifndef __lint
    interface_t *p;
#endif

    if (str == NULL)
	return interface_list;

#ifndef __lint
    for (p = interface_list; p != NULL; p = p->next) {
	if (strcasecmp(p->name, str) == 0)
	    return p;
    }
#endif
    return NULL;
}


/*
** ------------------------
**  Internal routines
** ------------------------
*/


static void
init_defaults(
    void)
{
    char *s;

    /* defaults for exported variables */

#ifdef DEFAULT_CONFIG
    s = DEFAULT_CONFIG;
#else
    s = "YOUR ORG";
#endif
#ifdef DEFAULT_CONFIG
    s = DEFAULT_CONFIG;
#else
    s = "";
#endif
    conf_init_string(&conf_data[CNF_CONF], s);
#ifdef DEFAULT_SERVER
    s = DEFAULT_SERVER;
#else
    s = "";
#endif
    conf_init_string(&conf_data[CNF_INDEX_SERVER], s);


#ifdef DEFAULT_TAPE_SERVER
    s = DEFAULT_TAPE_SERVER;
#else
#ifdef DEFAULT_SERVER
    s = DEFAULT_SERVER;
#else
    s = "";
#endif
#endif
    conf_init_string(&conf_data[CNF_TAPE_SERVER], s);
    conf_init_string(&conf_data[CNF_AUTH], "bsd");
    conf_init_string(&conf_data[CNF_SSH_KEYS], "");
    conf_init_string(&conf_data[CNF_AMANDAD_PATH], "");
    conf_init_string(&conf_data[CNF_CLIENT_USERNAME], "");
#ifdef GNUTAR_LISTED_INCREMENTAL_DIR
    conf_init_string(&conf_data[CNF_GNUTAR_LIST_DIR],
                     GNUTAR_LISTED_INCREMENTAL_DIR);
#else
    conf_init_string(&conf_data[CNF_GNUTAR_LIST_DIR], NULL);
#endif
    conf_init_string(&conf_data[CNF_AMANDATES], AMANDATES_FILE);
    conf_init_string(&conf_data[CNF_KRB5KEYTAB], "/.amanda-v5-keytab");
    conf_init_string(&conf_data[CNF_KRB5PRINCIPAL], "service/amanda");

    conf_init_string(&conf_data[CNF_ORG], s);
    conf_init_string(&conf_data[CNF_MAILTO], "operators");
    conf_init_string(&conf_data[CNF_DUMPUSER], CLIENT_LOGIN);
#ifdef DEFAULT_TAPE_DEVICE
    s = DEFAULT_TAPE_DEVICE;
#else
    s = NULL;
#endif
    conf_init_string(&conf_data[CNF_TAPEDEV], s);
#ifdef DEFAULT_CHANGER_DEVICE
    s = DEFAULT_CHANGER_DEVICE;
#else
    s = "/dev/null";
#endif
    conf_init_string(&conf_data[CNF_CHNGRDEV], s);
    conf_init_string(&conf_data[CNF_CHNGRFILE], "/usr/adm/amanda/changer-status");
#ifdef DEFAULT_RAW_TAPE_DEVICE
    s = DEFAULT_RAW_TAPE_DEVICE;
#else
    s = "/dev/rawft0";
#endif
    conf_init_string   (&conf_data[CNF_LABELSTR]             , ".*");
    conf_init_string   (&conf_data[CNF_TAPELIST]             , "tapelist");
    conf_init_string   (&conf_data[CNF_DISKFILE]             , "disklist");
    conf_init_string   (&conf_data[CNF_INFOFILE]             , "/usr/adm/amanda/curinfo");
    conf_init_string   (&conf_data[CNF_LOGDIR]               , "/usr/adm/amanda");
    conf_init_string   (&conf_data[CNF_INDEXDIR]             , "/usr/adm/amanda/index");
    conf_init_ident    (&conf_data[CNF_TAPETYPE]             , "EXABYTE");
    conf_init_int      (&conf_data[CNF_DUMPCYCLE]            , 10);
    conf_init_int      (&conf_data[CNF_RUNSPERCYCLE]         , 0);
    conf_init_int      (&conf_data[CNF_TAPECYCLE]            , 15);
    conf_init_int      (&conf_data[CNF_NETUSAGE]             , 300);
    conf_init_int      (&conf_data[CNF_INPARALLEL]           , 10);
    conf_init_string   (&conf_data[CNF_DUMPORDER]            , "ttt");
    conf_init_int      (&conf_data[CNF_BUMPPERCENT]          , 0);
    conf_init_am64     (&conf_data[CNF_BUMPSIZE]             , (off_t)10*1024);
    conf_init_real     (&conf_data[CNF_BUMPMULT]             , 1.5);
    conf_init_int      (&conf_data[CNF_BUMPDAYS]             , 2);
    conf_init_string   (&conf_data[CNF_TPCHANGER]            , "");
    conf_init_int      (&conf_data[CNF_RUNTAPES]             , 1);
    conf_init_int      (&conf_data[CNF_MAXDUMPS]             , 1);
    conf_init_int      (&conf_data[CNF_ETIMEOUT]             , 300);
    conf_init_int      (&conf_data[CNF_DTIMEOUT]             , 1800);
    conf_init_int      (&conf_data[CNF_CTIMEOUT]             , 30);
    conf_init_int      (&conf_data[CNF_TAPEBUFS]             , 20);
    conf_init_string   (&conf_data[CNF_RAWTAPEDEV]           , s);
    conf_init_string   (&conf_data[CNF_PRINTER]              , "");
    conf_init_bool     (&conf_data[CNF_AUTOFLUSH]            , 0);
    conf_init_int      (&conf_data[CNF_RESERVE]              , 100);
    conf_init_am64     (&conf_data[CNF_MAXDUMPSIZE]          , (off_t)-1);
    conf_init_string   (&conf_data[CNF_COLUMNSPEC]           , "");
    conf_init_bool     (&conf_data[CNF_AMRECOVER_DO_FSF]     , 1);
    conf_init_string   (&conf_data[CNF_AMRECOVER_CHANGER]    , "");
    conf_init_bool     (&conf_data[CNF_AMRECOVER_CHECK_LABEL], 1);
    conf_init_taperalgo(&conf_data[CNF_TAPERALGO]            , 0);
    conf_init_string   (&conf_data[CNF_DISPLAYUNIT]          , "k");
    conf_init_string   (&conf_data[CNF_KRB5KEYTAB]           , "/.amanda-v5-keytab");
    conf_init_string   (&conf_data[CNF_KRB5PRINCIPAL]        , "service/amanda");
    conf_init_string   (&conf_data[CNF_LABEL_NEW_TAPES]      , "");
    conf_init_bool     (&conf_data[CNF_USETIMESTAMPS]        , 0);
    conf_init_int      (&conf_data[CNF_CONNECT_TRIES]        , 3);
    conf_init_int      (&conf_data[CNF_REP_TRIES]            , 5);
    conf_init_int      (&conf_data[CNF_REQ_TRIES]            , 3);
    conf_init_int      (&conf_data[CNF_DEBUG_AMANDAD]        , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AMIDXTAPED]     , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AMINDEXD]       , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AMRECOVER]      , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_AUTH]           , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_EVENT]          , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_HOLDING]        , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_PROTOCOL]       , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_PLANNER]        , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_DRIVER]         , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_DUMPER]         , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_CHUNKER]        , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_TAPER]          , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_SELFCHECK]      , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_SENDSIZE]       , 0);
    conf_init_int      (&conf_data[CNF_DEBUG_SENDBACKUP]     , 0);
#ifdef UDPPORTRANGE
    conf_init_intrange (&conf_data[CNF_RESERVED_UDP_PORT]    , UDPPORTRANGE);
#else
    conf_init_intrange (&conf_data[CNF_RESERVED_UDP_PORT]    , 512, 1023);
#endif
#ifdef LOW_TCPPORTRANGE
    conf_init_intrange (&conf_data[CNF_RESERVED_TCP_PORT]    , LOW_TCPPORTRANGE);
#else
    conf_init_intrange (&conf_data[CNF_RESERVED_TCP_PORT]    , 512, 1023);
#endif
#ifdef TCPPORTRANGE
    conf_init_intrange (&conf_data[CNF_UNRESERVED_TCP_PORT]  , TCPPORTRANGE);
#else
    conf_init_intrange (&conf_data[CNF_UNRESERVED_TCP_PORT]  , 0, 0);
#endif

    /* defaults for internal variables */

    conf_line_num = got_parserror = 0;
    allow_overwrites = 0;
    token_pushed = 0;

    while(holdingdisks != NULL) {
	holdingdisk_t *hp;

	hp = holdingdisks;
	holdingdisks = holdingdisks->next;
	amfree(hp);
    }
    num_holdingdisks = 0;

    /* free any previously declared dump, tape and interface types */

    while(dumplist != NULL) {
	dumptype_t *dp;

	dp = dumplist;
	dumplist = dumplist->next;
	amfree(dp);
    }
    while(tapelist != NULL) {
	tapetype_t *tp;

	tp = tapelist;
	tapelist = tapelist->next;
	amfree(tp);
    }
    while(interface_list != NULL) {
	interface_t *ip;

	ip = interface_list;
	interface_list = interface_list->next;
	amfree(ip);
    }

    /* create some predefined dumptypes for backwards compatability */
    init_dumptype_defaults();
    dpcur.name = stralloc("NO-COMPRESS");
    dpcur.seen = -1;
    conf_set_compress(&dpcur.value[DUMPTYPE_COMPRESS], COMP_NONE);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("COMPRESS-FAST");
    dpcur.seen = -1;
    conf_set_compress(&dpcur.value[DUMPTYPE_COMPRESS], COMP_FAST);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("COMPRESS-BEST");
    dpcur.seen = -1;
    conf_set_compress(&dpcur.value[DUMPTYPE_COMPRESS], COMP_BEST);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("COMPRESS-CUST");
    dpcur.seen = -1;
    conf_set_compress(&dpcur.value[DUMPTYPE_COMPRESS], COMP_CUST);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("SRVCOMPRESS");
    dpcur.seen = -1;
    conf_set_compress(&dpcur.value[DUMPTYPE_COMPRESS], COMP_SERVER_FAST);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("BSD-AUTH");
    dpcur.seen = -1;
    conf_set_string(&dpcur.value[DUMPTYPE_SECURITY_DRIVER], "BSD");
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("KRB4-AUTH");
    dpcur.seen = -1;
    conf_set_string(&dpcur.value[DUMPTYPE_SECURITY_DRIVER], "KRB4");
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("NO-RECORD");
    dpcur.seen = -1;
    conf_set_bool(&dpcur.value[DUMPTYPE_RECORD], 0);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("NO-HOLD");
    dpcur.seen = -1;
    conf_set_holding(&dpcur.value[DUMPTYPE_HOLDINGDISK], HOLD_NEVER);
    save_dumptype();

    init_dumptype_defaults();
    dpcur.name = stralloc("NO-FULL");
    dpcur.seen = -1;
    conf_set_strategy(&dpcur.value[DUMPTYPE_STRATEGY], DS_NOFULL);
    save_dumptype();
    conffile_init = 1;
}

static void
read_conffile_recursively(
    char *filename)
{
    /* Save globals used in read_confline(), elsewhere. */
    int  save_line_num  = conf_line_num;
    FILE *save_conf     = conf_conf;
    char *save_confname = conf_confname;
    int	rc;

    if (*filename == '/' || config_dir == NULL) {
	conf_confname = stralloc(filename);
    } else {
	conf_confname = stralloc2(config_dir, filename);
    }

    if((conf_conf = fopen(conf_confname, "r")) == NULL) {
	fprintf(stderr, _("could not open conf file \"%s\": %s\n"), conf_confname,
		strerror(errno));
	amfree(conf_confname);
	got_parserror = -1;
	return;
    }

    conf_line_num = 0;

    /* read_confline() can invoke us recursively via "includefile" */
    do {
	rc = read_confline();
    } while (rc != 0);
    afclose(conf_conf);

    amfree(conf_confname);

    /* Restore servers */
    conf_line_num = save_line_num;
    conf_conf     = save_conf;
    conf_confname = save_confname;
}


/* ------------------------ */


static int
read_confline(
    void)
{
    t_conf_var *np;

    keytable = server_keytab;

    conf_line_num += 1;
    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INCLUDEFILE:
	{
	    char *fn;
	    char *cname;

	    get_conftoken(CONF_STRING);
	    fn = tokenval.v.s;
	    if (*fn == '/' || config_dir == NULL) {
		cname = stralloc(fn);
	    } else {
		cname = stralloc2(config_dir, fn);
	    }
	    if ( cname != NULL &&  (access(cname, R_OK) == 0)) {
		read_conffile_recursively(cname);
		amfree(cname);
	    } else {
		conf_parserror(_("cannot open %s: %s\n"), fn, strerror(errno));
	    }
	    amfree(cname);
	    
	}
	break;

    case CONF_HOLDING:
	get_holdingdisk();
	break;

    case CONF_DEFINE:
	get_conftoken(CONF_ANY);
	if(tok == CONF_DUMPTYPE) get_dumptype();
	else if(tok == CONF_TAPETYPE) get_tapetype();
	else if(tok == CONF_INTERFACE) get_interface();
	else conf_parserror(_("DUMPTYPE, INTERFACE or TAPETYPE expected"));
	break;

    case CONF_NL:	/* empty line */
	break;

    case CONF_END:	/* end of file */
	return 0;

    default:
	{
	    for(np = server_var; np->token != CONF_UNKNOWN; np++) 
		if(np->token == tok) break;

	    if(np->token == CONF_UNKNOWN) {
		conf_parserror(_("configuration keyword expected"));
	    } else {
		np->read_function(np, &conf_data[np->parm]);
		if(np->validate)
		    np->validate(np, &conf_data[np->parm]);
	    }
	}
    }
    if(tok != CONF_NL)
	get_conftoken(CONF_NL);
    return 1;
}

static void
get_holdingdisk(
    void)
{
    char *prefix;
    int save_overwrites;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_holdingdisk_defaults();

    get_conftoken(CONF_IDENT);
    hdcur.name = stralloc(tokenval.v.s);
    hdcur.seen = conf_line_num;

    prefix = vstralloc( "HOLDINGDISK:", hdcur.name, ":", NULL);
    read_block(program_options, holding_var, server_keytab, hdcur.value, prefix,
	       _("holding disk parameter expected"), 1, NULL);
    amfree(prefix);
    get_conftoken(CONF_NL);

    hdcur.disksize = holdingdisk_get_disksize(&hdcur);
    save_holdingdisk();

    allow_overwrites = save_overwrites;
}

static void
init_holdingdisk_defaults(
    void)
{
    conf_init_string(&hdcur.value[HOLDING_COMMENT]  , "");
    conf_init_string(&hdcur.value[HOLDING_DISKDIR]  , "");
    conf_init_am64(&hdcur.value[HOLDING_DISKSIZE] , (off_t)0);
                    /* 1 Gb = 1M counted in 1Kb blocks */
    conf_init_am64(&hdcur.value[HOLDING_CHUNKSIZE], (off_t)1024*1024);

    hdcur.up = (void *)0;
    hdcur.disksize = 0LL;
}

static void
save_holdingdisk(
    void)
{
    holdingdisk_t *hp;

    hp = alloc(sizeof(holdingdisk_t));
    *hp = hdcur;
    hp->next = holdingdisks;
    holdingdisks = hp;

    num_holdingdisks++;
}


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
    char *prefix;

    if (from) {
	saved_conf = conf_conf;
	conf_conf = from;
    }

    if (fname) {
	saved_fname = conf_confname;
	conf_confname = fname;
    }

    if (linenum)
	conf_line_num = *linenum;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_dumptype_defaults();
    if (name) {
	dpcur.name = name;
    } else {
	get_conftoken(CONF_IDENT);
	dpcur.name = stralloc(tokenval.v.s);
    }
    dpcur.seen = conf_line_num;

    prefix = vstralloc( "DUMPTYPE:", dpcur.name, ":", NULL);
    read_block(program_options, dumptype_var, server_keytab, dpcur.value,
	       prefix, _("dumptype parameter expected"),
	       (name == NULL), *copy_dumptype);
    amfree(prefix);
    if(!name)
	get_conftoken(CONF_NL);

    /* XXX - there was a stupidity check in here for skip-incr and
    ** skip-full.  This check should probably be somewhere else. */

    save_dumptype();

    allow_overwrites = save_overwrites;

    if (linenum)
	*linenum = conf_line_num;

    if (fname)
	conf_confname = saved_fname;

    if (from)
	conf_conf = saved_conf;

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
    conf_init_string   (&dpcur.value[DUMPTYPE_COMMENT]           , "");
    conf_init_string   (&dpcur.value[DUMPTYPE_PROGRAM]           , "DUMP");
    conf_init_string   (&dpcur.value[DUMPTYPE_SRVCOMPPROG]       , "");
    conf_init_string   (&dpcur.value[DUMPTYPE_CLNTCOMPPROG]      , "");
    conf_init_string   (&dpcur.value[DUMPTYPE_SRV_ENCRYPT]       , "");
    conf_init_string   (&dpcur.value[DUMPTYPE_CLNT_ENCRYPT]      , "");
    conf_init_string   (&dpcur.value[DUMPTYPE_AMANDAD_PATH]      , "X");
    conf_init_string   (&dpcur.value[DUMPTYPE_CLIENT_USERNAME]   , "X");
    conf_init_string   (&dpcur.value[DUMPTYPE_SSH_KEYS]          , "X");
    conf_init_string   (&dpcur.value[DUMPTYPE_SECURITY_DRIVER]   , "BSD");
    conf_init_exinclude(&dpcur.value[DUMPTYPE_EXCLUDE]);
    conf_init_exinclude(&dpcur.value[DUMPTYPE_INCLUDE]);
    conf_init_priority (&dpcur.value[DUMPTYPE_PRIORITY]          , 1);
    conf_init_int      (&dpcur.value[DUMPTYPE_DUMPCYCLE]         , conf_data[CNF_DUMPCYCLE].v.i);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAXDUMPS]          , conf_data[CNF_MAXDUMPS].v.i);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAXPROMOTEDAY]     , 10000);
    conf_init_int      (&dpcur.value[DUMPTYPE_BUMPPERCENT]       , conf_data[CNF_BUMPPERCENT].v.i);
    conf_init_am64     (&dpcur.value[DUMPTYPE_BUMPSIZE]          , conf_data[CNF_BUMPSIZE].v.am64);
    conf_init_int      (&dpcur.value[DUMPTYPE_BUMPDAYS]          , conf_data[CNF_BUMPDAYS].v.i);
    conf_init_real     (&dpcur.value[DUMPTYPE_BUMPMULT]          , conf_data[CNF_BUMPMULT].v.r);
    conf_init_time     (&dpcur.value[DUMPTYPE_STARTTIME]         , (time_t)0);
    conf_init_strategy (&dpcur.value[DUMPTYPE_STRATEGY]          , DS_STANDARD);
    conf_init_estimate (&dpcur.value[DUMPTYPE_ESTIMATE]          , ES_CLIENT);
    conf_init_compress (&dpcur.value[DUMPTYPE_COMPRESS]          , COMP_FAST);
    conf_init_encrypt  (&dpcur.value[DUMPTYPE_ENCRYPT]           , ENCRYPT_NONE);
    conf_init_string   (&dpcur.value[DUMPTYPE_SRV_DECRYPT_OPT]   , "-d");
    conf_init_string   (&dpcur.value[DUMPTYPE_CLNT_DECRYPT_OPT]  , "-d");
    conf_init_rate     (&dpcur.value[DUMPTYPE_COMPRATE]          , 0.50, 0.50);
    conf_init_am64     (&dpcur.value[DUMPTYPE_TAPE_SPLITSIZE]    , (off_t)0);
    conf_init_am64     (&dpcur.value[DUMPTYPE_FALLBACK_SPLITSIZE], (off_t)10 * 1024);
    conf_init_string   (&dpcur.value[DUMPTYPE_SPLIT_DISKBUFFER]  , NULL);
    conf_init_bool     (&dpcur.value[DUMPTYPE_RECORD]            , 1);
    conf_init_bool     (&dpcur.value[DUMPTYPE_SKIP_INCR]         , 0);
    conf_init_bool     (&dpcur.value[DUMPTYPE_SKIP_FULL]         , 0);
    conf_init_holding  (&dpcur.value[DUMPTYPE_HOLDINGDISK]       , HOLD_AUTO);
    conf_init_bool     (&dpcur.value[DUMPTYPE_KENCRYPT]          , 0);
    conf_init_bool     (&dpcur.value[DUMPTYPE_IGNORE]            , 0);
    conf_init_bool     (&dpcur.value[DUMPTYPE_INDEX]             , 1);
}

static void
save_dumptype(void)
{
    dumptype_t *dp, *dp1;;

    dp = lookup_dumptype(dpcur.name);

    if(dp != (dumptype_t *)0) {
	conf_parserror(_("dumptype %s already defined on line %d"), dp->name, dp->seen);
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
	if(dt->value[i].seen) {
	    free_val_t(&dpcur.value[i]);
	    copy_val_t(&dpcur.value[i], &dt->value[i]);
	}
    }
}

static void
get_tapetype(void)
{
    int save_overwrites;
    char *prefix;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_tapetype_defaults();

    get_conftoken(CONF_IDENT);
    tpcur.name = stralloc(tokenval.v.s);
    tpcur.seen = conf_line_num;

    prefix = vstralloc( "TAPETYPE:", tpcur.name, ":", NULL);
    read_block(program_options, tapetype_var, server_keytab, tpcur.value,
	       prefix, _("tapetype parameter expected"), 1, &copy_tapetype);
    amfree(prefix);
    get_conftoken(CONF_NL);

    save_tapetype();

    allow_overwrites = save_overwrites;
}

static void
init_tapetype_defaults(void)
{
    conf_init_string(&tpcur.value[TAPETYPE_COMMENT]      , "");
    conf_init_string(&tpcur.value[TAPETYPE_LBL_TEMPL]    , "");
    conf_init_size  (&tpcur.value[TAPETYPE_BLOCKSIZE]    , DISK_BLOCK_KB);
    conf_init_size  (&tpcur.value[TAPETYPE_READBLOCKSIZE], MAX_TAPE_BLOCK_KB);
    conf_init_am64  (&tpcur.value[TAPETYPE_LENGTH]       , (off_t)2000);
    conf_init_am64  (&tpcur.value[TAPETYPE_FILEMARK]     , (off_t)1);
    conf_init_int   (&tpcur.value[TAPETYPE_SPEED]        , 200);
    conf_init_bool  (&tpcur.value[TAPETYPE_FILE_PAD]     , 1);
}

static void
save_tapetype(void)
{
    tapetype_t *tp, *tp1;

    tp = lookup_tapetype(tpcur.name);

    if(tp != (tapetype_t *)0) {
	amfree(tpcur.name);
	conf_parserror(_("tapetype %s already defined on line %d"), tp->name, tp->seen);
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
	if(tp->value[i].seen) {
	    free_val_t(&tpcur.value[i]);
	    copy_val_t(&tpcur.value[i], &tp->value[i]);
	}
    }
}

t_conf_var interface_var [] = {
   { CONF_COMMENT, CONFTYPE_STRING, read_string, INTER_COMMENT , NULL },
   { CONF_USE    , CONFTYPE_INT   , read_int   , INTER_MAXUSAGE, validate_positive1 },
   { CONF_UNKNOWN, CONFTYPE_INT   , NULL       , INTER_INTER   , NULL }
};

static void
get_interface(void)
{
    int save_overwrites;
    char *prefix;

    save_overwrites = allow_overwrites;
    allow_overwrites = 1;

    init_interface_defaults();

    get_conftoken(CONF_IDENT);
    ifcur.name = stralloc(tokenval.v.s);
    ifcur.seen = conf_line_num;

    prefix = vstralloc( "INTERFACE:", ifcur.name, ":", NULL);
    read_block(program_options, interface_var, server_keytab, ifcur.value,
	       prefix, _("interface parameter expected"), 1, &copy_interface);
    amfree(prefix);
    get_conftoken(CONF_NL);

    save_interface();

    allow_overwrites = save_overwrites;

    return;
}

static void
init_interface_defaults(void)
{
    conf_init_string(&ifcur.value[INTER_COMMENT] , "");
    conf_init_int   (&ifcur.value[INTER_MAXUSAGE], 300);

    ifcur.curusage = 0;
}

static void
save_interface(void)
{
    interface_t *ip, *ip1;

    ip = lookup_interface(ifcur.name);

    if(ip != (interface_t *)0) {
	conf_parserror(_("interface %s already defined on line %d"), ip->name,
		       ip->seen);
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
/*
    int i;
    t_xxx *np;
    keytab_t *kt;
    
    val_t val;
*/
    interface_t *ip;
    int i;

    ip = lookup_interface(tokenval.v.s);

    if(ip == NULL) {
	conf_parserror(_("interface parameter expected"));
	return;
    }

    for(i=0; i < INTER_INTER; i++) {
	if(ip->value[i].seen) {
	    free_val_t(&ifcur.value[i]);
	    copy_val_t(&ifcur.value[i], &ip->value[i]);
	}
    }
}

static void
get_comprate(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    get_conftoken(CONF_REAL);
    val->v.rate[0] = tokenval.v.r;
    val->v.rate[1] = tokenval.v.r;
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
    val->v.rate[1] = tokenval.v.r;
    if(tokenval.v.r < 0) {
	conf_parserror(_("incremental compression rate must be >= 0"));
    }
}

static void
read_intrange(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    get_conftoken(CONF_INT);
    val->v.intrange[0] = tokenval.v.i;
    val->v.intrange[1] = tokenval.v.i;
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
    val->v.intrange[1] = tokenval.v.i;
}

static void
get_compress(
    t_conf_var *np,
    val_t *val)
{
    int serv, clie, none, fast, best, custom;
    int done;
    comp_t comp;

    np = np;
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

    val->v.i = (int)comp;
}

static void
get_encrypt(
    t_conf_var *np,
    val_t *val)
{
   encrypt_t encrypt;

   np = np;
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

   val->v.i = (int)encrypt;
}

static void
get_holding(
    t_conf_var *np,
    val_t *val)
{
   dump_holdingdisk_t holding;

   np = np;
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

   val->v.i = (int)holding;
}

static void
get_taperalgo(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_FIRST:      val->v.i = ALGO_FIRST;      break;
    case CONF_FIRSTFIT:   val->v.i = ALGO_FIRSTFIT;   break;
    case CONF_LARGEST:    val->v.i = ALGO_LARGEST;    break;
    case CONF_LARGESTFIT: val->v.i = ALGO_LARGESTFIT; break;
    case CONF_SMALLEST:   val->v.i = ALGO_SMALLEST;   break;
    case CONF_LAST:       val->v.i = ALGO_LAST;       break;
    default:
	conf_parserror(_("FIRST, FIRSTFIT, LARGEST, LARGESTFIT, SMALLEST or LAST expected"));
    }
}

static void
get_priority(
    t_conf_var *np,
    val_t *val)
{
    int pri;

    np = np;
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
    val->v.i = pri;
}

static void
get_strategy(
    t_conf_var *np,
    val_t *val)
{
    int strat;

    np = np;
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
	conf_parserror(_("STANDARD or NOFULL expected"));
	strat = DS_STANDARD;
    }
    val->v.i = strat;
}

static void
get_estimate(
    t_conf_var *np,
    val_t *val)
{
    int estime;

    np = np;
    ckseen(&val->seen);

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_CLIENT:
	estime = ES_CLIENT;
	break;
    case CONF_SERVER:
	estime = ES_SERVER;
	break;
    case CONF_CALCSIZE:
	estime = ES_CALCSIZE;
	break;
    default:
	conf_parserror(_("CLIENT, SERVER or CALCSIZE expected"));
	estime = ES_CLIENT;
    }
    val->v.i = estime;
}

static void
get_exclude(
    t_conf_var *np,
    val_t *val)
{
    int file, got_one = 0;
    sl_t *exclude;
    int optional = 0;

    np = np;
    get_conftoken(CONF_ANY);
    if(tok == CONF_LIST) {
	file = 0;
	get_conftoken(CONF_ANY);
	exclude = val->v.exinclude.sl_list;
    }
    else {
	file = 1;
	if(tok == CONF_EFILE) get_conftoken(CONF_ANY);
	exclude = val->v.exinclude.sl_file;
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
	val->v.exinclude.sl_list = exclude;
    else
	val->v.exinclude.sl_file = exclude;
    val->v.exinclude.optional = optional;
}

/*
static void get_include(np, val)
    t_conf_var *np;
    val_t *val;
{
    int list, got_one = 0;
    sl_t *include;
    int optional = 0;
    int append = 0;

    get_conftoken(CONF_ANY);
    if(tok == CONF_LIST) {
	list = 1;
	include = dpcur.value[DUMPTYPE_INCLUDE_LIST].v.sl;
	ckseen(&dpcur.value[DUMPTYPE_INCLUDE_LIST].seen);
	get_conftoken(CONF_ANY);
    }
    else {
	list = 0;
	include = dpcur.value[DUMPTYPE_INCLUDE_FILE].v.sl;
	ckseen(&dpcur.value[DUMPTYPE_INCLUDE_FILE].seen);
	if(tok == CONF_EFILE) get_conftoken(CONF_ANY);
    }

    if(tok == CONF_OPTIONAL) {
	get_conftoken(CONF_ANY);
	optional = 1;
    }

    if(tok == CONF_APPEND) {
	get_conftoken(CONF_ANY);
	append = 1;
    }
    else {
	free_sl(include);
	include = NULL;
	append = 0;
    }

    while(tok == CONF_STRING) {
	include = append_sl(include, tokenval.v.s);
	got_one = 1;
	get_conftoken(CONF_ANY);
    }
    unget_conftoken();

    if(got_one == 0) { free_sl(include); include = NULL; }

    if(list == 0)
	dpcur.value[DUMPTYPE_INCLUDE_FILE].v.sl = include;
    else {
	dpcur.value[DUMPTYPE_INCLUDE_LIST].v.sl = include;
	if(!append || optional)
	    dpcur.value[DUMPTYPE_INCLUDE_OPTIONAL].v.i = optional;
    }
}
*/

/* ------------------------ */

int
ColumnDataCount(void )
{
    return (int)(SIZEOF(ColumnData) / SIZEOF(ColumnData[0]));
}

/* conversion from string to table index
 */
int
StringToColumn(
    char *s)
{
    int cn;

    for (cn=0; ColumnData[cn].Name != NULL; cn++) {
    	if (strcasecmp(s, ColumnData[cn].Name) == 0) {
	    break;
	}
    }
    return cn;
}

char
LastChar(
    char *s)
{
    return s[strlen(s)-1];
}

int
SetColumDataFromString(
    ColumnInfo* ci,
    char *s,
    char **errstr)
{
#ifdef TEST
    char *myname= "SetColumDataFromString";
#endif
    ci = ci;

    /* Convert from a Columspec string to our internal format
     * of columspec. The purpose is to provide this string
     * as configuration paramter in the amanda.conf file or
     * (maybe) as environment variable.
     * 
     * This text should go as comment into the sample amanda.conf
     *
     * The format for such a ColumnSpec string s is a ',' seperated
     * list of triples. Each triple consists of
     *   -the name of the column (as in ColumnData.Name)
     *   -prefix before the column
     *   -the width of the column
     *       if set to -1 it will be recalculated
     *	 to the maximum length of a line to print.
     * Example:
     * 	"Disk=1:17,HostName=1:10,OutKB=1:7"
     * or
     * 	"Disk=1:-1,HostName=1:10,OutKB=1:7"
     *	
     * You need only specify those colums that should be changed from
     * the default. If nothing is specified in the configfile, the
     * above compiled in values will be in effect, resulting in an
     * output as it was all the time.
     *							ElB, 1999-02-24.
     */

    while (s && *s) {
	int Space, Width;
	int cn;
    	char *eon= strchr(s, '=');

	if (eon == NULL) {
	    *errstr = stralloc2(_("invalid columnspec: "), s);
#ifdef TEST
	    fprintf(stderr, "%s: %s\n", myname, *errstr);
#endif
	    return -1;
	}
	*eon= '\0';
	cn=StringToColumn(s);
	if (ColumnData[cn].Name == NULL) {
	    *errstr = stralloc2(_("invalid column name: "), s);
#ifdef TEST
	    fprintf(stderr, "%s: %s\n", myname, *errstr);
#endif
	    return -1;
	}
	if (sscanf(eon+1, "%d:%d", &Space, &Width) != 2) {
	    *errstr = stralloc2(_("invalid format: "), eon + 1);
#ifdef TEST
	    fprintf(stderr, "%s: %s\n", myname, *errstr);
#endif
	    return -1;
	}
	ColumnData[cn].Width= Width;
	ColumnData[cn].PrefixSpace = Space;
	if (LastChar(ColumnData[cn].Format) == 's') {
	    if (Width < 0)
		ColumnData[cn].MaxWidth= 1;
	    else
		if (Width > ColumnData[cn].Precision)
		    ColumnData[cn].Precision= Width;
	}
	else if (Width < ColumnData[cn].Precision)
	    ColumnData[cn].Precision = Width;
	s= strchr(eon+1, ',');
	if (s != NULL)
	    s++;
    }
    return 0;
}


long int
getconf_unit_divisor(void)
{
    return unit_divisor;
}

/* ------------------------ */


void
dump_configuration(
    char *filename)
{
    tapetype_t *tp;
    dumptype_t *dp;
    interface_t *ip;
    holdingdisk_t *hp;
    int i;
    t_conf_var *np;
    keytab_t *kt;
    char *prefix;
    char kt_prefix[100];

    printf(_("AMANDA CONFIGURATION FROM FILE \"%s\":\n\n"), filename);

    for(np=server_var; np->token != CONF_UNKNOWN; np++) {
	for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++) 
	    if (np->token == kt->token) break;

	if(kt->token == CONF_UNKNOWN)
	    error(_("server bad token"));

	if (kt->token != CONF_IDENT)
	    snprintf(kt_prefix, 100, "%-21s ", kt->keyword);
	    printf("%s\n",
		   conf_print(&conf_data[np->parm], 1, kt_prefix));
    }

    for(hp = holdingdisks; hp != NULL; hp = hp->next) {
	printf("\nHOLDINGDISK %s {\n", hp->name);
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

	    snprintf(kt_prefix, 100, "      %-9s ", kt->keyword);
	    printf("%s\n", conf_print(&hp->value[i], 1, kt_prefix));
	}
	printf("}\n");
    }

    for(tp = tapelist; tp != NULL; tp = tp->next) {
	printf("\nDEFINE TAPETYPE %s {\n", tp->name);
	for(i=0; i < TAPETYPE_TAPETYPE; i++) {
	    for(np=tapetype_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("tapetype bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("tapetype bad token"));

	    snprintf(kt_prefix, 100, "      %-9s ", kt->keyword);
	    printf("%s\n", conf_print(&tp->value[i], 1, kt_prefix));
	}
	printf("}\n");
    }

    for(dp = dumplist; dp != NULL; dp = dp->next) {
	if (strncmp(dp->name, "custom(", 7) != 0) {
	    if(dp->seen == -1)
		prefix = "#";
	    else
		prefix = "";
	    printf("\n%sDEFINE DUMPTYPE %s {\n", prefix, dp->name);
	    for(i=0; i < DUMPTYPE_DUMPTYPE; i++) {
		for(np=dumptype_var; np->token != CONF_UNKNOWN; np++)
		    if(np->parm == i) break;
		if(np->token == CONF_UNKNOWN)
		    error(_("dumptype bad value"));

		for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		    if(kt->token == np->token) break;
		if(kt->token == CONF_UNKNOWN)
		    error(_("dumptype bad token"));

		snprintf(kt_prefix, 100, "%s      %-19s ", prefix,kt->keyword);
		printf("%s\n", conf_print(&dp->value[i], 1, kt_prefix));
	    }
	    printf("%s}\n", prefix);
	}
    }

    for(ip = interface_list; ip != NULL; ip = ip->next) {
	if(strcmp(ip->name,"default") == 0)
	    prefix = "#";
	else
	    prefix = "";
	printf("\n%sDEFINE INTERFACE %s {\n", prefix, ip->name);
	for(i=0; i < INTER_INTER; i++) {
	    for(np=interface_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error(_("interface bad value"));

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error(_("interface bad token"));

	    snprintf(kt_prefix, 100, "%s      %-19s ", prefix, kt->keyword);
	    printf("%s\n", conf_print(&ip->value[i], 1, kt_prefix));
	}
	printf("%s}\n",prefix);
    }

}

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
get_token_name(
    tok_t token)
{
    keytab_t *kt;

    if (my_keytab == NULL) {
	error(_("my_keytab == NULL"));
	/*NOTREACHED*/
    }

    for(kt = my_keytab; kt->token != CONF_UNKNOWN; kt++)
	if(kt->token == token) break;

    if(kt->token == CONF_UNKNOWN)
	return("");
    return(kt->keyword);
}

void
parse_conf(
    int parse_argc,
    char **parse_argv,
    int *new_argc,
    char ***new_argv)
{
    int i;
    char **my_argv;
    char *myarg, *value;
    command_option_t *program_option;

    program_options = alloc((size_t)(parse_argc+1) * SIZEOF(*program_options));
    program_options_size = parse_argc+1;
    program_option = program_options;
    program_option->name = NULL;

    my_argv = alloc((size_t)parse_argc * SIZEOF(char *));
    *new_argv = my_argv;
    *new_argc = 0;
    i=0;
    while(i<parse_argc) {
	if(strncmp(parse_argv[i],"-o",2) == 0) {
	    if(strlen(parse_argv[i]) > 2)
		myarg = &parse_argv[i][2];
	    else {
		i++;
		if(i >= parse_argc)
		    error(_("expect something after -o"));
		myarg = parse_argv[i];
	    }
	    value = index(myarg,'=');
	    if (value == NULL) {
		conf_parserror(_("Must specify a value for %s.\n"), myarg);
	    } else {
		*value = '\0';
		value++;
		program_option->used = 0;
		program_option->name = stralloc(myarg);
		program_option->value = stralloc(value);
		program_option++;
		program_option->name = NULL;
	    }
	}
	else {
	    my_argv[*new_argc] = stralloc(parse_argv[i]);
	    *new_argc += 1;
	}
	i++;
    }
}

char **
get_config_options(
    int first)
{
    char             **config_options;
    char	     **config_option;
    command_option_t  *command_options;

    config_options = alloc((first+program_options_size+1)*SIZEOF(char *));
    for(command_options = program_options,
        config_option = config_options + first;
	command_options->name != NULL; command_options++) {
	*config_option = vstralloc("-o", command_options->name, "=",
				   command_options->value, NULL);
	config_option++;
    }
    *config_option = NULL;
    return(config_options);
}

void
report_bad_conf_arg(void)
{
    command_option_t *command_option;

    for(command_option = program_options; command_option->name != NULL;
							command_option++) {
	if(command_option->used == 0) {
	    fprintf(stderr,_("argument -o%s=%s not used\n"),
		    command_option->name, command_option->value);
	}
    }
}

void
free_server_config(void)
{
    holdingdisk_t    *hp, *hpnext;
    dumptype_t       *dp, *dpnext;
    tapetype_t       *tp, *tpnext;
    interface_t      *ip, *ipnext;
    command_option_t *server_option;
    int               i;

    for(hp=holdingdisks; hp != NULL; hp = hpnext) {
	amfree(hp->name);
	for(i=0; i<HOLDING_HOLDING-1; i++) {
	   free_val_t(&hp->value[i]);
	}
	hpnext = hp->next;
	amfree(hp);
    }

    for(dp=dumplist; dp != NULL; dp = dpnext) {
	amfree(dp->name);
	for(i=0; i<DUMPTYPE_DUMPTYPE-1; i++) {
	   free_val_t(&dp->value[i]);
	}
	dpnext = dp->next;
	amfree(dp);
    }

    for(tp=tapelist; tp != NULL; tp = tpnext) {
	amfree(tp->name);
	for(i=0; i<TAPETYPE_TAPETYPE-1; i++) {
	   free_val_t(&tp->value[i]);
	}
	tpnext = tp->next;
	amfree(tp);
    }

    for(ip=interface_list; ip != NULL; ip = ipnext) {
	amfree(ip->name);
	for(i=0; i<INTER_INTER-1; i++) {
	   free_val_t(&ip->value[i]);
	}
	ipnext = ip->next;
	amfree(ip);
    }

    if(program_options) {
	for(server_option = program_options; server_option->name != NULL;
						server_option++) {
	    amfree(server_option->name);
	    amfree(server_option->value);
        }
	amfree(program_options);
    }

    for(i=0; i<CNF_CNF-1; i++)
	free_val_t(&conf_data[i]);
}



/* configuration parameters */
static char *cln_config_dir = NULL;

/* predeclare local functions */

static void read_client_conffile_recursively(char *filename);
static int read_client_confline(void);

static int first_file = 1;

/*
** ------------------------
**  External entry points
** ------------------------
*/

/* return  0 on success        */
/* return  1 on error          */
/* return -1 if file not found */

int read_clientconf(
    char *filename)
{
    my_keytab = server_keytab;
    my_var = client_var;

    if(first_file == 1) {
	init_defaults();
	first_file = 0;
    } else {
	allow_overwrites = 1;
    }

    /* We assume that conf_confname & conf are initialized to NULL above */
    read_client_conffile_recursively(filename);

    command_overwrite(program_options, client_var, client_keytab, conf_data,
		      "");

    debug_amandad    = getconf_int(CNF_DEBUG_AMANDAD);
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

    return got_parserror;
}


/*
** ------------------------
**  Internal routines
** ------------------------
*/


static void
read_client_conffile_recursively(
    char *	filename)
{
    /* Save globals used in read_client_confline(), elsewhere. */
    int  save_line_num  = conf_line_num;
    FILE *save_conf     = conf_conf;
    char *save_confname = conf_confname;
    int	rc;

    if (*filename == '/' || cln_config_dir == NULL) {
	conf_confname = stralloc(filename);
    } else {
	conf_confname = stralloc2(cln_config_dir, filename);
    }

    if((conf_conf = fopen(conf_confname, "r")) == NULL) {
	dbprintf(_("Could not open conf file \"%s\": %s\n"), conf_confname,
		  strerror(errno));
	amfree(conf_confname);
	got_parserror = -1;
	return;
    }
    dbprintf(_("Reading conf file \"%s\".\n"), conf_confname);

    conf_line_num = 0;

    /* read_client_confline() can invoke us recursively via "includefile" */
    do {
	rc = read_client_confline();
    } while (rc != 0);
    afclose(conf_conf);

    amfree(conf_confname);

    /* Restore globals */
    conf_line_num = save_line_num;
    conf_conf     = save_conf;
    conf_confname = save_confname;
}


/* ------------------------ */


static int
read_client_confline(void)
{
    t_conf_var *np;

    keytable = client_keytab;

    conf_line_num += 1;
    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INCLUDEFILE:
	{
	    char *fn;

	    get_conftoken(CONF_STRING);
	    fn = tokenval.v.s;
	    read_client_conffile_recursively(fn);
	}
	break;

    case CONF_NL:	/* empty line */
	break;

    case CONF_END:	/* end of file */
	return 0;

    default:
	{
	    for(np = client_var; np->token != CONF_UNKNOWN; np++)
		if(np->token == tok) break;

	    if(np->token == CONF_UNKNOWN) {
		conf_parserror(_("configuration keyword expected"));
	    } else {
		np->read_function(np, &conf_data[np->parm]);
		if(np->validate)
		    np->validate(np, &conf_data[np->parm]);
	    }
	}
    }
    if(tok != CONF_NL)
	get_conftoken(CONF_NL);
    return 1;
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


/* return  0 on success             */
/* return -1 if it is already there */
/* return -2 if other failure       */
int
add_client_conf(
    confparm_t parm,
    char *value)
{
    t_conf_var *np;
    keytab_t *kt;
    command_option_t *command_option;
    int nb_option;

    for(np = client_var; np->token != CONF_UNKNOWN; np++)
	if(np->parm == (int)parm) break;

    if(np->token == CONF_UNKNOWN) return -2;

    for(kt = client_keytab; kt->token != CONF_UNKNOWN; kt++)
	if(kt->token == np->token) break;

    if(kt->token == CONF_UNKNOWN) return -2;

    /* Try to find it */
    nb_option = 0;
    for(command_option = program_options; command_option->name != NULL;
							command_option++) {
	nb_option++;
    }

    /* Increase size of program_options if needed */
    if(nb_option >= program_options_size-1) {
	program_options_size *= 2;
	program_options = realloc(program_options,
			        program_options_size * SIZEOF(*program_options));
	if (program_options == NULL) {
	    error(_("Can't realloc program_options: %s\n"), strerror(errno));
	    /*NOTREACHED*/
	}
	for(command_option = program_options; command_option->name != NULL;
							command_option++) {
	}
    }

    /* add it */
    command_option->used = 0;
    command_option->name = stralloc(kt->keyword);
    command_option->value = stralloc(value);
    command_option++;
    command_option->name = NULL;
    return 0;
}

/*
static t_conf_var *
get_np(
    t_conf_var	*get_var,
    int 	parm)
{
    t_conf_var *np;

    for(np = get_var; np->token != CONF_UNKNOWN; np++) {
	if(np->parm == parm)
	    break;
    }

    if(np->token == CONF_UNKNOWN) {
	error(_("error [unknown get_np parm: %d]"), parm);
	NOTREACHED
    }
    return np;
}
*/

static time_t
get_time(void)
{
    time_t hhmm;

    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_INT:
#if SIZEOF_TIME_T < SIZEOF_INT
	if ((off_t)tokenval.v.i >= (off_t)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.i;
	break;

    case CONF_LONG:
#if SIZEOF_TIME_T < SIZEOF_LONG
	if ((off_t)tokenval.v.l >= (off_t)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.l;
	break;

    case CONF_SIZE:
#if SIZEOF_TIME_T < SIZEOF_SSIZE_T
	if ((off_t)tokenval.v.size >= (off_t)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.size;
	break;

    case CONF_AM64:
#if SIZEOF_TIME_T < SIZEOF_LONG_LONG
	if ((off_t)tokenval.v.am64 >= (off_t)TIME_MAX)
	    conf_parserror(_("value too large"));
#endif
	hhmm = (time_t)tokenval.v.am64;
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
    { "MPS", CONF_MULT1M },
    { "TAPE", CONF_MULT1 },
    { "TAPES", CONF_MULT1 },
    { "WEEK", CONF_MULT7 },
    { "WEEKS", CONF_MULT7 },
    { NULL, CONF_IDENT }
};

static int
get_int(void)
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

    case CONF_LONG:
#if SIZEOF_INT < SIZEOF_LONG
	if ((off_t)tokenval.v.l > (off_t)INT_MAX)
	    conf_parserror(_("value too large"));
	if ((off_t)tokenval.v.l < (off_t)INT_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (int)tokenval.v.l;
	break;

    case CONF_SIZE:
#if SIZEOF_INT < SIZEOF_SSIZE_T
	if ((off_t)tokenval.v.size > (off_t)INT_MAX)
	    conf_parserror(_("value too large"));
	if ((off_t)tokenval.v.size < (off_t)INT_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (int)tokenval.v.size;
	break;

    case CONF_AM64:
#if SIZEOF_INT < SIZEOF_LONG_LONG
	if (tokenval.v.am64 > (off_t)INT_MAX)
	    conf_parserror(_("value too large"));
	if (tokenval.v.am64 < (off_t)INT_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (int)tokenval.v.am64;
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
    get_conftoken(CONF_ANY);
    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_END:
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > (INT_MAX / 7))
	    conf_parserror(_("value too large"));
	if (val < (INT_MIN / 7))
	    conf_parserror(_("value too small"));
	val *= 7;
	break;

    case CONF_MULT1M:
	if (val > (INT_MAX / 1024))
	    conf_parserror(_("value too large"));
	if (val < (INT_MIN / 1024))
	    conf_parserror(_("value too small"));
	val *= 1024;
	break;

    case CONF_MULT1G:
	if (val > (INT_MAX / (1024 * 1024)))
	    conf_parserror(_("value too large"));
	if (val < (INT_MIN / (1024 * 1024)))
	    conf_parserror(_("value too small"));
	val *= 1024 * 1024;
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;
    return val;
}

/*
static long
get_long(void)
{
    long val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_LONG:
	val = tokenval.v.l;
	break;

    case CONF_INT:
#if SIZEOF_LONG < SIZEOF_INT
	if ((off_t)tokenval.v.i > (off_t)LONG_MAX)
	    conf_parserror(_("value too large"));
	if ((off_t)tokenval.v.i < (off_t)LONG_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (long)tokenval.v.i;
	break;

    case CONF_SIZE:
#if SIZEOF_LONG < SIZEOF_SSIZE_T
	if ((off_t)tokenval.v.size > (off_t)LONG_MAX)
	    conf_parserror(_("value too large"));
	if ((off_t)tokenval.v.size < (off_t)LONG_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (long)tokenval.v.size;
	break;

    case CONF_AM64:
#if SIZEOF_LONG < SIZEOF_LONG_LONG
	if (tokenval.v.am64 > (off_t)LONG_MAX)
	    conf_parserror(_("value too large"));
	if (tokenval.v.am64 < (off_t)LONG_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (long)tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	val = (long)LONG_MAX;
	break;

    default:
	conf_parserror(_("an integer is expected"));
	val = 0;
	break;
    }

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_NL:
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > (LONG_MAX / 7L))
	    conf_parserror(_("value too large"));
	if (val < (LONG_MIN / 7L))
	    conf_parserror(_("value too small"));
	val *= 7L;
	break;

    case CONF_MULT1M:
	if (val > (LONG_MAX / 1024L))
	    conf_parserror(_("value too large"));
	if (val < (LONG_MIN / 1024L))
	    conf_parserror(_("value too small"));
	val *= 1024L;
	break;

    case CONF_MULT1G:
	if (val > (LONG_MAX / (1024L * 1024L)))
	    conf_parserror(_("value too large"));
	if (val < (LONG_MIN / (1024L * 1024L)))
	    conf_parserror(_("value too small"));
	val *= 1024L * 1024L;
	break;

    default:
	unget_conftoken();
	break;
    }

    keytable = save_kt;
    return val;
}
*/

static ssize_t
get_size(void)
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
	if ((off_t)tokenval.v.i > (off_t)SSIZE_MAX)
	    conf_parserror(_("value too large"));
	if ((off_t)tokenval.v.i < (off_t)SSIZE_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (ssize_t)tokenval.v.i;
	break;

    case CONF_LONG:
#if SIZEOF_SIZE_T < SIZEOF_LONG
	if ((off_t)tokenval.v.l > (off_t)SSIZE_MAX)
	    conf_parserror(_("value too large"));
	if ((off_t)tokenval.v.l < (off_t)SSIZE_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (ssize_t)tokenval.v.l;
	break;

    case CONF_AM64:
#if SIZEOF_SIZE_T < SIZEOF_LONG_LONG
	if (tokenval.v.am64 > (off_t)SSIZE_MAX)
	    conf_parserror(_("value too large"));
	if (tokenval.v.am64 < (off_t)SSIZE_MIN)
	    conf_parserror(_("value too small"));
#endif
	val = (ssize_t)tokenval.v.am64;
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
    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > (ssize_t)(SSIZE_MAX / 7))
	    conf_parserror(_("value too large"));
	if (val < (ssize_t)(SSIZE_MIN / 7))
	    conf_parserror(_("value too small"));
	val *= (ssize_t)7;
	break;

    case CONF_MULT1M:
	if (val > (ssize_t)(SSIZE_MAX / (ssize_t)1024))
	    conf_parserror(_("value too large"));
	if (val < (ssize_t)(SSIZE_MIN / (ssize_t)1024))
	    conf_parserror(_("value too small"));
	val *= (ssize_t)1024;
	break;

    case CONF_MULT1G:
	if (val > (ssize_t)(SSIZE_MAX / (1024 * 1024)))
	    conf_parserror(_("value too large"));
	if (val < (ssize_t)(SSIZE_MIN / (1024 * 1024)))
	    conf_parserror(_("value too small"));
	val *= (ssize_t)(1024 * 1024);
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;
    return val;
}

static off_t
get_am64_t(void)
{
    off_t val;
    keytab_t *save_kt;

    save_kt = keytable;
    keytable = numb_keytable;

    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_INT:
	val = (off_t)tokenval.v.i;
	break;

    case CONF_LONG:
	val = (off_t)tokenval.v.l;
	break;

    case CONF_SIZE:
	val = (off_t)tokenval.v.size;
	break;

    case CONF_AM64:
	val = tokenval.v.am64;
	break;

    case CONF_AMINFINITY:
	val = AM64_MAX;
	break;

    default:
	conf_parserror(_("an integer is expected"));
	val = 0;
	break;
    }

    /* get multiplier, if any */
    get_conftoken(CONF_ANY);

    switch(tok) {
    case CONF_NL:			/* multiply by one */
    case CONF_MULT1:
    case CONF_MULT1K:
	break;

    case CONF_MULT7:
	if (val > AM64_MAX/7 || val < AM64_MIN/7)
	    conf_parserror(_("value too large"));
	val *= 7;
	break;

    case CONF_MULT1M:
	if (val > AM64_MAX/1024 || val < AM64_MIN/1024)
	    conf_parserror(_("value too large"));
	val *= 1024;
	break;

    case CONF_MULT1G:
	if (val > AM64_MAX/(1024*1024) || val < AM64_MIN/(1024*1024))
	    conf_parserror(_("value too large"));
	val *= 1024*1024;
	break;

    default:	/* it was not a multiplier */
	unget_conftoken();
	break;
    }

    keytable = save_kt;

    return val;
}

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

    case CONF_LONG:
	if (tokenval.v.l != 0L)
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

    case CONF_AM64:
	if (tokenval.v.am64 != (off_t)0)
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
	conf_parserror(_("YES, NO, TRUE, FALSE, ON, OFF expected"));
	break;
    }

    keytable = save_kt;
    return val;
}

void
ckseen(
    int *seen)
{
    if (*seen && !allow_overwrites && conf_line_num != -2) {
	conf_parserror(_("duplicate parameter, prev def on line %d"), *seen);
    }
    *seen = conf_line_num;
}

printf_arglist_function(void conf_parserror, const char *, format)
{
    va_list argp;

    /* print error message */

    if(conf_line)
	fprintf(stderr, _("argument \"%s\": "), conf_line);
    else
	fprintf(stderr, "\"%s\", line %d: ", conf_confname, conf_line_num);
    arglist_start(argp, format);
    vfprintf(stderr, format, argp);
    arglist_end(argp);
    fputc('\n', stderr);

    got_parserror = 1;
}

tok_t
lookup_keyword(
    char *	str)
{
    keytab_t *kwp;

    /* switch to binary search if performance warrants */

    for(kwp = keytable; kwp->keyword != NULL; kwp++) {
	if (strcasecmp(kwp->keyword, str) == 0) break;
    }
    return kwp->token;
}

char tkbuf[4096];

/* push the last token back (can only unget ANY tokens) */
static void
unget_conftoken(void)
{
    token_pushed = 1;
    pushed_tok = tok;
    tok = CONF_UNKNOWN;
    return;
}

static int
conftoken_getc(void)
{
    if(conf_line == NULL)
	return getc(conf_conf);
    if(*conf_char == '\0')
	return -1;
    return(*conf_char++);
}

static int
conftoken_ungetc(
    int c)
{
    if(conf_line == NULL)
	return ungetc(c, conf_conf);
    else if(conf_char > conf_line) {
	if(c == -1)
	    return c;
	conf_char--;
	if(*conf_char != c) {
	    error(_("*conf_char != c   : %c %c"), *conf_char, c);
	    /* NOTREACHED */
	}
    } else {
	error(_("conf_char == conf_line"));
	/* NOTREACHED */
    }
    return c;
}

static void
get_conftoken(
    tok_t	exp)
{
    int ch, d;
    off_t am64;
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
	** If it looked like a key word before then look it
	** up again in the current keyword table.
	*/
	switch(tok) {
	case CONF_LONG:    case CONF_AM64:    case CONF_SIZE:
	case CONF_INT:     case CONF_REAL:    case CONF_STRING:
	case CONF_LBRACE:  case CONF_RBRACE:  case CONF_COMMA:
	case CONF_NL:      case CONF_END:     case CONF_UNKNOWN:
	case CONF_TIME:
	    break;

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

	while(ch != EOF && ch != '\n' && isspace(ch))
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
		if (ferror(conf_conf)) {
		    conf_parserror(_("Pushback of '%c' failed: %s"),
				   ch, strerror(ferror(conf_conf)));
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
	    am64 = 0;
	    do {
		am64 = am64 * 10 + (ch - '0');
		ch = conftoken_getc();
	    } while (isdigit(ch));

	    if (ch != '.') {
		if (exp == CONF_INT) {
		    tok = CONF_INT;
		    tokenval.v.i = sign * (int)am64;
		} else if (exp == CONF_LONG) {
		    tok = CONF_LONG;
		    tokenval.v.l = (long)sign * (long)am64;
		} else if (exp != CONF_REAL) {
		    tok = CONF_AM64;
		    tokenval.v.am64 = (off_t)sign * am64;
		} else {
		    /* automatically convert to real when expected */
		    tokenval.v.r = (double)sign * (double)am64;
		    tok = CONF_REAL;
		}
	    } else {
		/* got a real number, not an int */
		tokenval.v.r = sign * (double) am64;
		am64 = 0;
		d = 1;
		ch = conftoken_getc();
		while (isdigit(ch)) {
		    am64 = am64 * 10 + (ch - '0');
		    d = d * 10;
		    ch = conftoken_getc();
		}
		tokenval.v.r += sign * ((double)am64) / d;
		tok = CONF_REAL;
	    }

	    if (ch != EOF &&  conftoken_ungetc(ch) == EOF) {
		if (ferror(conf_conf)) {
		    conf_parserror(_("Pushback of '%c' failed: %s"),
				   ch, strerror(ferror(conf_conf)));
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
		    if (!escape)
			break;
		    escape = 0;
		    buf--; /* Consume escape in buffer */
		} else if (ch == '\\') {
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
		    if (ferror(conf_conf)) {
			conf_parserror(_("Pushback of '%c' failed: %s"),
				       ch, strerror(ferror(conf_conf)));
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
		str = kwp->keyword;
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
read_string(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    get_conftoken(CONF_STRING);
    val->v.s = newstralloc(val->v.s, tokenval.v.s);
}

static void
read_ident(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    get_conftoken(CONF_IDENT);
    val->v.s = newstralloc(val->v.s, tokenval.v.s);
}

static void
read_int(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.i = get_int();
}

/*
static void
read_long(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.l = get_long();
}
*/

static void
read_size(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.size = get_size();
}

static void
read_am64(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.am64 = get_am64_t();
}

static void
read_bool(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.i = get_bool();
}

static void
read_real(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    get_conftoken(CONF_REAL);
    val->v.r = tokenval.v.r;
}

static void
read_time(
    t_conf_var *np,
    val_t *val)
{
    np = np;
    ckseen(&val->seen);
    val->v.t = get_time();
}

static void
copy_val_t(
    val_t *valdst,
    val_t *valsrc)
{
    if(valsrc->seen) {
	valdst->type = valsrc->type;
	valdst->seen = valsrc->seen;
	switch(valsrc->type) {
	case CONFTYPE_INT:
	case CONFTYPE_BOOL:
	case CONFTYPE_COMPRESS:
	case CONFTYPE_ENCRYPT:
	case CONFTYPE_HOLDING:
	case CONFTYPE_ESTIMATE:
	case CONFTYPE_STRATEGY:
	case CONFTYPE_TAPERALGO:
	case CONFTYPE_PRIORITY:
	    valdst->v.i = valsrc->v.i;
	    break;

	case CONFTYPE_LONG:
	    valdst->v.l = valsrc->v.l;
	    break;

	case CONFTYPE_SIZE:
	    valdst->v.size = valsrc->v.size;
	    break;

	case CONFTYPE_AM64:
	    valdst->v.am64 = valsrc->v.am64;
	    break;

	case CONFTYPE_REAL:
	    valdst->v.r = valsrc->v.r;
	    break;

	case CONFTYPE_RATE:
	    valdst->v.rate[0] = valsrc->v.rate[0];
	    valdst->v.rate[1] = valsrc->v.rate[1];
	    break;

	case CONFTYPE_IDENT:
	case CONFTYPE_STRING:
	    valdst->v.s = stralloc(valsrc->v.s);
	    break;

	case CONFTYPE_TIME:
	    valdst->v.t = valsrc->v.t;
	    break;

	case CONFTYPE_SL:
	    valdst->v.sl = duplicate_sl(valsrc->v.sl);
	    break;

	case CONFTYPE_EXINCLUDE:
	    valdst->v.exinclude.optional = valsrc->v.exinclude.optional;
	    valdst->v.exinclude.sl_list = duplicate_sl(valsrc->v.exinclude.sl_list);
	    valdst->v.exinclude.sl_file = duplicate_sl(valsrc->v.exinclude.sl_file);
	    break;

	case CONFTYPE_INTRANGE:
	    valdst->v.intrange[0] = valsrc->v.intrange[0];
	    valdst->v.intrange[1] = valsrc->v.intrange[1];
	    break;

	}
    }
}

static void
free_val_t(
    val_t *val)
{
    switch(val->type) {
	case CONFTYPE_INT:
	case CONFTYPE_BOOL:
	case CONFTYPE_COMPRESS:
	case CONFTYPE_ENCRYPT:
	case CONFTYPE_HOLDING:
	case CONFTYPE_ESTIMATE:
	case CONFTYPE_STRATEGY:
	case CONFTYPE_SIZE:
	case CONFTYPE_TAPERALGO:
	case CONFTYPE_PRIORITY:
	case CONFTYPE_LONG:
	case CONFTYPE_AM64:
	case CONFTYPE_REAL:
	case CONFTYPE_RATE:
	case CONFTYPE_INTRANGE:
	    break;

	case CONFTYPE_IDENT:
	case CONFTYPE_STRING:
	    amfree(val->v.s);
	    break;

	case CONFTYPE_TIME:
	    break;

	case CONFTYPE_SL:
	    free_sl(val->v.sl);
	    break;

	case CONFTYPE_EXINCLUDE:
	    free_sl(val->v.exinclude.sl_list);
	    free_sl(val->v.exinclude.sl_file);
	    break;
    }
    val->seen = 0;
}

char *
taperalgo2str(
    int taperalgo)
{
    if(taperalgo == ALGO_FIRST) return "FIRST";
    if(taperalgo == ALGO_FIRSTFIT) return "FIRSTFIT";
    if(taperalgo == ALGO_LARGEST) return "LARGEST";
    if(taperalgo == ALGO_LARGESTFIT) return "LARGESTFIT";
    if(taperalgo == ALGO_SMALLEST) return "SMALLEST";
    if(taperalgo == ALGO_LAST) return "LAST";
    return "UNKNOWN";
}

static char buffer_conf_print[2049];

static char *
conf_print(
    val_t *val,
    int    str_need_quote,
    char  *prefix)
{
    char *buf;
    int   free_space;

    buffer_conf_print[0] = '\0';
    snprintf(buffer_conf_print, SIZEOF(buffer_conf_print), prefix);
    free_space = SIZEOF(buffer_conf_print) - strlen(buffer_conf_print);
    buf = buffer_conf_print + strlen(buffer_conf_print);
    switch(val->type) {
    case CONFTYPE_INT:
	snprintf(buf, free_space, "%d", val->v.i);
	break;

    case CONFTYPE_LONG:
	snprintf(buf, free_space, "%ld", val->v.l);
	break;

    case CONFTYPE_SIZE:
	snprintf(buf, free_space, SSIZE_T_FMT, (SSIZE_T_FMT_TYPE)val->v.size);
	break;

    case CONFTYPE_AM64:
	snprintf(buf, free_space, OFF_T_FMT, (OFF_T_FMT_TYPE)val->v.am64);
	break;

    case CONFTYPE_REAL:
	snprintf(buf, free_space, "%0.5f" , val->v.r);
	break;

    case CONFTYPE_RATE:
	snprintf(buf, free_space, "%0.5f %0.5f",
		 val->v.rate[0], val->v.rate[1]);
	break;

    case CONFTYPE_INTRANGE:
	snprintf(buf, free_space, "%d,%d",
		 val->v.intrange[0], val->v.intrange[1]);
	break;

    case CONFTYPE_IDENT:
	if(val->v.s) {
	    strncpy(buf, val->v.s, free_space);
	}
	break;

    case CONFTYPE_STRING:
	if(str_need_quote) {
	    *buf++ = '"';
	    free_space++;
            if(val->v.s) {
		strncpy(buf, val->v.s, free_space);
		buffer_conf_print[SIZEOF(buffer_conf_print) - 2] = '\0';
		buffer_conf_print[strlen(buffer_conf_print)] = '"';
		buffer_conf_print[strlen(buffer_conf_print) + 1] = '\0';
            } else {
		*buf++ = '"';
		*buf++ = '\0';
		free_space -= 2;
            }
	} else {
	    if(val->v.s) {
		strncpy(buf, val->v.s, free_space);
	    }
	}
	break;

    case CONFTYPE_TIME:
	snprintf(buf, free_space, "%2d%02d",
		 (int)val->v.t/100, (int)val->v.t % 100);
	break;

    case CONFTYPE_SL:
	break;

    case CONFTYPE_EXINCLUDE:
	buf = buffer_conf_print;
	free_space = SIZEOF(buffer_conf_print);

	conf_print_exinclude(val, 1, 0, prefix, &buf ,&free_space);
	*buf++ = '\n';
	free_space -= 1;

	conf_print_exinclude(val, 1, 1, prefix, &buf, &free_space);
	break;

    case CONFTYPE_BOOL:
	if(val->v.i)
	    strncpy(buf, "yes", free_space);
	else
	    strncpy(buf, "no", free_space);
	break;

    case CONFTYPE_STRATEGY:
	switch(val->v.i) {
	case DS_SKIP:
	    strncpy(buf, "SKIP", free_space);
	    break;

	case DS_STANDARD:
	    strncpy(buf, "STANDARD", free_space);
	    break;

	case DS_NOFULL:
	    strncpy(buf, "NOFULL", free_space);
	    break;

	case DS_NOINC:
	    strncpy(buf, "NOINC", free_space);
	    break;

	case DS_HANOI:
	    strncpy(buf, "HANOI", free_space);
	    break;

	case DS_INCRONLY:
	    strncpy(buf, "INCRONLY", free_space);
	    break;
	}
	break;

    case CONFTYPE_COMPRESS:
	switch(val->v.i) {
	case COMP_NONE:
	    strncpy(buf, "NONE", free_space);
	    break;

	case COMP_FAST:
	    strncpy(buf, "CLIENT FAST", free_space);
	    break;

	case COMP_BEST:
	    strncpy(buf, "CLIENT BEST", free_space);
	    break;

	case COMP_CUST:
	    strncpy(buf, "CLIENT CUSTOM", free_space);
	    break;

	case COMP_SERVER_FAST:
	    strncpy(buf, "SERVER FAST", free_space);
	    break;

	case COMP_SERVER_BEST:
	    strncpy(buf, "SERVER FAST", free_space);
	    break;

	case COMP_SERVER_CUST:
	    strncpy(buf, "SERVER CUSTOM", free_space);
	    break;
	}
	break;

    case CONFTYPE_ESTIMATE:
	switch(val->v.i) {
	case ES_CLIENT:
	    strncpy(buf, "CLIENT", free_space);
	    break;

	case ES_SERVER:
	    strncpy(buf, "SERVER", free_space);
	    break;

	case ES_CALCSIZE:
	    strncpy(buf, "CALCSIZE", free_space);
	    break;
	}
	break;

     case CONFTYPE_ENCRYPT:
	switch(val->v.i) {
	case ENCRYPT_NONE:
	    strncpy(buf, "NONE", free_space);
	    break;

	case ENCRYPT_CUST:
	    strncpy(buf, "CLIENT", free_space);
	    break;

	case ENCRYPT_SERV_CUST:
	    strncpy(buf, "SERVER", free_space);
	    break;
	}
	break;

     case CONFTYPE_HOLDING:
	switch(val->v.i) {
	case HOLD_NEVER:
	    strncpy(buf, "NEVER", free_space);
	    break;

	case HOLD_AUTO:
	    strncpy(buf, "AUTO", free_space);
	    break;

	case HOLD_REQUIRED:
	    strncpy(buf, "REQUIRED", free_space);
	    break;
	}
	break;

     case CONFTYPE_TAPERALGO:
	strncpy(buf, taperalgo2str(val->v.i), free_space);
	break;

     case CONFTYPE_PRIORITY:
	switch(val->v.i) {
	case 0:
	    strncpy(buf, "LOW", free_space);
	    break;

	case 1:
	    strncpy(buf, "MEDIUM", free_space);
	    break;

	case 2:
	    strncpy(buf, "HIGH", free_space);
	    break;
	}
	break;
    }
    buffer_conf_print[SIZEOF(buffer_conf_print) - 1] = '\0';
    return buffer_conf_print;
}

void  conf_print_exinclude(
    val_t *val,
    int    str_need_quote,
    int    file,
    char  *prefix,
    char **buf,
    int   *free_space)
{
    sl_t  *sl;
    sle_t *excl;

    (void)str_need_quote;

    snprintf(*buf, *free_space, prefix);
    *free_space -= strlen(prefix);
    *buf += strlen(prefix);

    if (val->type != CONFTYPE_EXINCLUDE) {
	strcpy(*buf,
	  "ERROR: conf_print_exinclude called for type != CONFTYPE_EXINCLUDE");
	return;
    }

    if (file == 0) {
	sl = val->v.exinclude.sl_list;
	strncpy(*buf, "LIST ", *free_space);
	*buf += 5;
	*free_space -= 5;
    } else {
	sl = val->v.exinclude.sl_file;
	strncpy(*buf, "FILE ", *free_space);
	*buf += 5;
	*free_space -= 5;
    }

    if (val->v.exinclude.optional == 1) {
	strncpy(*buf, "OPTIONAL ", *free_space);
	*buf += 9;
	*free_space -= 9;
    }

    if (sl != NULL) {
	for(excl = sl->first; excl != NULL; excl = excl->next) {
	    if (3 + (int)strlen(excl->name) < *free_space) {
		*(*buf)++ = ' ';
		*(*buf)++ = '"';
		strcpy(*buf, excl->name);
		*buf += strlen(excl->name);
		*(*buf)++ = '"';
		*free_space -= 3 + strlen(excl->name);
	    }
	}
    }

    return;
}

static void
conf_init_string(
    val_t *val,
    char  *s)
{
    val->seen = 0;
    val->type = CONFTYPE_STRING;
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
    val->seen = 0;
    val->type = CONFTYPE_IDENT;
    if(s)
	val->v.s = stralloc(s);
    else
	val->v.s = NULL;
}

static void
conf_init_int(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_INT;
    val->v.i = i;
}

static void
conf_init_bool(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_BOOL;
    val->v.i = i;
}

static void
conf_init_strategy(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_STRATEGY;
    val->v.i = i;
}

static void
conf_init_estimate(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_ESTIMATE;
    val->v.i = i;
}

static void
conf_init_taperalgo(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_TAPERALGO;
    val->v.i = i;
}

static void
conf_init_priority(
    val_t *val,
    int    i)
{
    val->seen = 0;
    val->type = CONFTYPE_PRIORITY;
    val->v.i = i;
}

static void
conf_init_compress(
    val_t *val,
    comp_t    i)
{
    val->seen = 0;
    val->type = CONFTYPE_COMPRESS;
    val->v.i = (int)i;
}

static void
conf_init_encrypt(
    val_t *val,
    encrypt_t    i)
{
    val->seen = 0;
    val->type = CONFTYPE_ENCRYPT;
    val->v.i = (int)i;
}

static void
conf_init_holding(
    val_t              *val,
    dump_holdingdisk_t  i)
{
    val->seen = 0;
    val->type = CONFTYPE_HOLDING;
    val->v.i = (int)i;
}

/*
static void
conf_init_long(
    val_t *val,
    long   l)
{
    val->seen = 0;
    val->type = CONFTYPE_LONG;
    val->v.l = l;
}
*/

static void
conf_init_size(
    val_t *val,
    ssize_t   sz)
{
    val->seen = 0;
    val->type = CONFTYPE_SIZE;
    val->v.size = sz;
}

static void
conf_init_am64(
    val_t *val,
    off_t   l)
{
    val->seen = 0;
    val->type = CONFTYPE_AM64;
    val->v.am64 = l;
}

static void
conf_init_real(
    val_t  *val,
    double r)
{
    val->seen = 0;
    val->type = CONFTYPE_REAL;
    val->v.r = r;
}

static void
conf_init_rate(
    val_t  *val,
    double r1,
    double r2)
{
    val->seen = 0;
    val->type = CONFTYPE_RATE;
    val->v.rate[0] = r1;
    val->v.rate[1] = r2;
}

static void
conf_init_intrange(
    val_t *val,
    int    i1,
    int    i2)
{
    val->seen = 0;
    val->type = CONFTYPE_INTRANGE;
    val->v.intrange[0] = i1;
    val->v.intrange[1] = i2;
}

static void
conf_init_time(
    val_t *val,
    time_t   t)
{
    val->seen = 0;
    val->type = CONFTYPE_TIME;
    val->v.t = t;
}

/*
static void
conf_init_sl(
    val_t *val,
    sl_t  *sl)
{
    val->seen = 0;
    val->type = CONFTYPE_AM64;
    val->v.sl = sl;
}
*/

static void
conf_init_exinclude(
    val_t *val)
{
    val->seen = 0;
    val->type = CONFTYPE_EXINCLUDE;
    val->v.exinclude.optional = 0;
    val->v.exinclude.sl_list = NULL;
    val->v.exinclude.sl_file = NULL;
}

static void
conf_set_string(
    val_t *val,
    char *s)
{
    val->seen = -1;
    val->type = CONFTYPE_STRING;
    amfree(val->v.s);
    val->v.s = stralloc(s);
}

/*
static void
conf_set_int(
    val_t *val,
    int    i)
{
    val->seen = -1;
    val->type = CONFTYPE_INT;
    val->v.i = i;
}
*/

static void
conf_set_bool(
    val_t *val,
    int    i)
{
    val->seen = -1;
    val->type = CONFTYPE_BOOL;
    val->v.i = i;
}

static void
conf_set_compress(
    val_t *val,
    comp_t    i)
{
    val->seen = -1;
    val->type = CONFTYPE_COMPRESS;
    val->v.i = (int)i;
}

/*
static void
conf_set_encrypt(
    val_t *val,
    encrypt_t    i)
{
    val->seen = -1;
    val->type = CONFTYPE_COMPRESS;
    val->v.i = (int)i;
}
*/

static void
conf_set_holding(
    val_t              *val,
    dump_holdingdisk_t  i)
{
    val->seen = -1;
    val->type = CONFTYPE_HOLDING;
    val->v.i = (int)i;
}

static void
conf_set_strategy(
    val_t *val,
    int    i)
{
    val->seen = -1;
    val->type = CONFTYPE_STRATEGY;
    val->v.i = i;
}


int
get_conftype_int(
    val_t *val)
{
    if (val->type != CONFTYPE_INT) {
	error(_("get_conftype_int: val.type is not CONFTYPE_INT"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

long
get_conftype_long(
    val_t *val)
{
    if (val->type != CONFTYPE_LONG) {
	error(_("get_conftype_long: val.type is not CONFTYPE_LONG"));
	/*NOTREACHED*/
    }
    return val->v.l;
}

off_t
get_conftype_am64(
    val_t *val)
{
    if (val->type != CONFTYPE_AM64) {
	error(_("get_conftype_am64: val.type is not CONFTYPE_AM64"));
	/*NOTREACHED*/
    }
    return val->v.am64;
}

double
get_conftype_real(
    val_t *val)
{
    if (val->type != CONFTYPE_REAL) {
	error(_("get_conftype_real: val.type is not CONFTYPE_REAL"));
	/*NOTREACHED*/
    }
    return val->v.r;
}

char *
get_conftype_string(
    val_t *val)
{
    if (val->type != CONFTYPE_STRING) {
	error(_("get_conftype_string: val.type is not CONFTYPE_STRING"));
	/*NOTREACHED*/
    }
    return val->v.s;
}

char *
get_conftype_ident(
    val_t *val)
{
    if (val->type != CONFTYPE_IDENT) {
	error(_("get_conftype_ident: val.type is not CONFTYPE_IDENT"));
	/*NOTREACHED*/
    }
    return val->v.s;
}

time_t
get_conftype_time(
    val_t *val)
{
    if (val->type != CONFTYPE_TIME) {
	error(_("get_conftype_time: val.type is not CONFTYPE_TIME"));
	/*NOTREACHED*/
    }
    return val->v.t;
}

ssize_t
get_conftype_size(
    val_t *val)
{
    if (val->type != CONFTYPE_SIZE) {
	error(_("get_conftype_size: val.type is not CONFTYPE_SIZE"));
	/*NOTREACHED*/
    }
    return val->v.size;
}

sl_t *
get_conftype_sl(
    val_t *val)
{
    if (val->type != CONFTYPE_SL) {
	error(_("get_conftype_size: val.type is not CONFTYPE_SL"));
	/*NOTREACHED*/
    }
    return val->v.sl;
}

int
get_conftype_bool(
    val_t *val)
{
    if (val->type != CONFTYPE_BOOL) {
	error(_("get_conftype_bool: val.type is not CONFTYPE_BOOL"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_hold(
    val_t *val)
{
    if (val->type != CONFTYPE_HOLDING) {
	error(_("get_conftype_hold: val.type is not CONFTYPE_HOLDING"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_compress(
    val_t *val)
{
    if (val->type != CONFTYPE_COMPRESS) {
	error(_("get_conftype_compress: val.type is not CONFTYPE_COMPRESS"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_encrypt(
    val_t *val)
{
    if (val->type != CONFTYPE_ENCRYPT) {
	error(_("get_conftype_encrypt: val.type is not CONFTYPE_ENCRYPT"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_estimate(
    val_t *val)
{
    if (val->type != CONFTYPE_ESTIMATE) {
	error(_("get_conftype_extimate: val.type is not CONFTYPE_ESTIMATE"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_strategy(
    val_t *val)
{
    if (val->type != CONFTYPE_STRATEGY) {
	error(_("get_conftype_strategy: val.type is not CONFTYPE_STRATEGY"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_taperalgo(
    val_t *val)
{
    if (val->type != CONFTYPE_TAPERALGO) {
	error(_("get_conftype_taperalgo: val.type is not CONFTYPE_TAPERALGO"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

int
get_conftype_priority(
    val_t *val)
{
    if (val->type != CONFTYPE_PRIORITY) {
	error(_("get_conftype_priority: val.type is not CONFTYPE_PRIORITY"));
	/*NOTREACHED*/
    }
    return val->v.i;
}

exinclude_t
get_conftype_exinclude(
    val_t *val)
{
    if (val->type != CONFTYPE_EXINCLUDE) {
	error(_("get_conftype_exinclude: val.type is not CONFTYPE_EXINCLUDE"));
	/*NOTREACHED*/
    }
    return val->v.exinclude;
}


static void
read_block(
    command_option_t *command_options,
    t_conf_var    *read_var,
    keytab_t *keytab,
    val_t    *valarray,
    char     *prefix,
    char     *errormsg,
    int       read_brace,
    void      (*copy_function)(void))
{
    t_conf_var *np;
    int    saved_conf_line_num;
    int    done;

    if(read_brace) {
	get_conftoken(CONF_LBRACE);
	get_conftoken(CONF_NL);
    }

    done = 0;
    do {
	conf_line_num += 1;
	get_conftoken(CONF_ANY);
	switch(tok) {
	case CONF_RBRACE:
	    done = 1;
	    break;
	case CONF_NL:	/* empty line */
	    break;
	case CONF_END:	/* end of file */
	    done = 1;
	    break;
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
		    conf_parserror(errormsg);
		else {
		    np->read_function(np, &valarray[np->parm]);
		    if(np->validate)
			np->validate(np, &valarray[np->parm]);
		}
	    }
	}
	if(tok != CONF_NL && tok != CONF_END && tok != CONF_RBRACE)
	    get_conftoken(CONF_NL);
    } while(!done);

    /* overwrite with command line option */
    saved_conf_line_num = conf_line_num;
    command_overwrite(command_options, read_var, keytab, valarray, prefix);
    conf_line_num = saved_conf_line_num;
}

void
command_overwrite(
    command_option_t *command_options,
    t_conf_var    *overwrite_var,
    keytab_t *keytab,
    val_t    *valarray,
    char     *prefix)
{
    t_conf_var	     *np;
    keytab_t	     *kt;
    char	     *myprefix;
    command_option_t *command_option;
    int	              duplicate;

    if(!command_options) return;

    for(np = overwrite_var; np->token != CONF_UNKNOWN; np++) {
	for(kt = keytab; kt->token != CONF_UNKNOWN; kt++)
	    if(kt->token == np->token) break;

	if(kt->token == CONF_UNKNOWN) {
	    error(_("command_overwrite: invalid token"));
	    /* NOTREACHED */
	}

	for(command_option = command_options; command_option->name != NULL;
							    command_option++) {
	    myprefix = stralloc2(prefix, kt->keyword);
	    if(strcasecmp(myprefix, command_option->name) == 0) {
		duplicate = 0;
		if (command_option->used == 0 &&
		    valarray[np->parm].seen == -2) {
		    duplicate = 1;
		}
		command_option->used = 1;
		valarray[np->parm].seen = -2;
		if(np->type == CONFTYPE_STRING &&
		   command_option->value[0] != '"') {
		    conf_line = vstralloc("\"", command_option->value, "\"",
					  NULL);
		}
		else {
		    conf_line = stralloc(command_option->value);
		}
		conf_char = conf_line;
		token_pushed = 0;
		conf_line_num = -2;
		np->read_function(np, &valarray[np->parm]);
		amfree(conf_line);
		conf_line = conf_char = NULL;

		if (np->validate)
		    np->validate(np, &valarray[np->parm]);
		if (duplicate == 1) {
		    fprintf(stderr,"Duplicate %s option, using %s\n",
			    command_option->name, command_option->value);
		}
	    }
	    amfree(myprefix);
	}
    }
}

void
free_new_argv(
    int new_argc,
    char **new_argv)
{
    int i;
    for(i=0; i<new_argc; i++)
	amfree(new_argv[i]);
    amfree(new_argv);
}

ssize_t
getconf_readblocksize(void)
{
    tapetype_t *tape;
    char       *conf_tapetype;

    if (conffile_init == 1) {
	conf_tapetype = getconf_str(CNF_TAPETYPE);

	if (!conf_tapetype || strlen(conf_tapetype) == 0)
	    return MAX_TAPE_BLOCK_KB;

	tape = lookup_tapetype(conf_tapetype);
	if (!tape)
	    return MAX_TAPE_BLOCK_KB;
	return tapetype_get_readblocksize(tape);
    }

    return MAX_TAPE_BLOCK_KB;
}
