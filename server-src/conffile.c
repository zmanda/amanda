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
#include "diskfile.h"
#include "driverio.h"
#include "clock.h"

#ifdef HAVE_LIMITS_H
#include <limits.h>
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

/* visible holding disk variables */

holdingdisk_t *holdingdisks;
int num_holdingdisks;

long int unit_divisor = 1;

/* configuration parameters */

val_t server_conf[CNF_CNF];

command_option_t *server_options = NULL;

/* other internal variables */
static holdingdisk_t hdcur;

static tapetype_t tpcur;

static dumptype_t dpcur;

static interface_t ifcur;

static dumptype_t *dumplist = NULL;
static tapetype_t *tapelist = NULL;
static interface_t *interface_list = NULL;

/* predeclare local functions */

char *get_token_name(tok_t);


void validate_positive0   (t_conf_var *, val_t *);
void validate_positive1   (t_conf_var *, val_t *);
void validate_runspercycle(t_conf_var *, val_t *);
void validate_bumppercent (t_conf_var *, val_t *);
void validate_bumpmult    (t_conf_var *, val_t *);
void validate_inparallel  (t_conf_var *, val_t *);
void validate_displayunit (t_conf_var *, val_t *);
void validate_reserve     (t_conf_var *, val_t *);
void validate_use         (t_conf_var *, val_t *);
void validate_chunksize   (t_conf_var *, val_t *);
void validate_blocksize   (t_conf_var *, val_t *);

static void init_defaults(void);
static void read_conffile_recursively(char *filename);

static int read_confline(void);
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
    { "CTIMEOUT", CONF_CTIMEOUT },
    { "CUSTOM", CONF_CUSTOM },
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
    { "REQUIRED", CONF_REQUIRED },
    { "RESERVE", CONF_RESERVE },
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
   { CONF_ETIMEOUT             , CONFTYPE_TIME     , read_time    , CNF_ETIMEOUT             , NULL },
   { CONF_DTIMEOUT             , CONFTYPE_TIME     , read_time    , CNF_DTIMEOUT             , validate_positive1 },
   { CONF_CTIMEOUT             , CONFTYPE_TIME     , read_time    , CNF_CTIMEOUT             , validate_positive1 },
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
   { CONF_UNKNOWN              , CONFTYPE_INT      , NULL         , CNF_CNF                  , NULL }
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

    init_defaults();

    /* We assume that conf_confname & conf are initialized to NULL above */
    read_conffile_recursively(filename);

    /* overwrite with command line option */
    command_overwrite(server_options, server_var, server_keytab, server_conf,
		      "");

    if(got_parserror != -1 ) {
	if(lookup_tapetype(server_conf[CNF_TAPETYPE].v.s) == NULL) {
	    char *save_confname = conf_confname;

	    conf_confname = filename;
	    if(!server_conf[CNF_TAPETYPE].seen)
		conf_parserror("default tapetype %s not defined", server_conf[CNF_TAPETYPE].v.s);
	    else {
		conf_line_num = server_conf[CNF_TAPETYPE].seen;
		conf_parserror("tapetype %s not defined", server_conf[CNF_TAPETYPE].v.s);
	    }
	    conf_confname = save_confname;
	}
    }

    ip = alloc(SIZEOF(interface_t));
    ip->name = stralloc("default");
    ip->seen = server_conf[CNF_NETUSAGE].seen;
    conf_init_string(&ip->value[INTER_COMMENT], "implicit from NETUSAGE");
    conf_init_int(&ip->value[INTER_MAXUSAGE], server_conf[CNF_NETUSAGE].v.i);
    ip->curusage = 0;
    ip->next = interface_list;
    interface_list = ip;

    return got_parserror;
}

void
validate_positive0(
    struct s_conf_var *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val->v.i < 0)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    case CONFTYPE_LONG:
	if(val->v.l < 0)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    case CONFTYPE_AM64:
	if(val->v.am64 < 0)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    default:
	conf_parserror("validate_positive0 invalid type %d\n", val->type);
    }
}

void
validate_positive1(
    struct s_conf_var *np,
    val_t        *val)
{
    switch(val->type) {
    case CONFTYPE_INT:
	if(val->v.i < 1)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    case CONFTYPE_LONG:
	if(val->v.l < 1)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    case CONFTYPE_AM64:
	if(val->v.am64 < 1)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    case CONFTYPE_TIME:
	if(val->v.t < 1)
	    conf_parserror("%s must be positive", get_token_name(np->token));
	break;
    default:
	conf_parserror("validate_positive1 invalid type %d\n", val->type);
    }
}

void
validate_runspercycle(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < -1)
	conf_parserror("runspercycle must be >= -1");
}

void
validate_bumppercent(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 0 || val->v.i > 100)
	conf_parserror("bumppercent must be between 0 and 100");
}

void
validate_inparallel(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 1 || val->v.i >MAX_DUMPERS)
	conf_parserror("inparallel must be between 1 and MAX_DUMPERS (%d)",
		       MAX_DUMPERS);
}

void
validate_bumpmult(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.r < 0.999) {
	conf_parserror("bumpmult must be positive");
    }
}

void
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
	conf_parserror("displayunit must be k,m,g or t.");
    }
}

void
validate_reserve(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.i < 0 || val->v.i > 100)
	conf_parserror("reserve must be between 0 and 100");
}

void
validate_use(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    val->v.am64 = am_floor(val->v.am64, DISK_BLOCK_KB);
}

void
validate_chunksize(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.am64 == 0) {
	val->v.am64 = ((AM64_MAX / 1024) - (2 * DISK_BLOCK_KB));
    }
    else if(val->v.am64 < 0) {
	conf_parserror("Negative chunksize (%lld) is no longer supported", val->v.am64);
    }
    val->v.am64 = am_floor(val->v.am64, (off_t)DISK_BLOCK_KB);
}

void
validate_blocksize(
    struct s_conf_var *np,
    val_t        *val)
{
    np = np;
    if(val->v.l < DISK_BLOCK_KB) {
	conf_parserror("Tape blocksize must be at least %d KBytes",
		  DISK_BLOCK_KB);
    } else if(val->v.l > MAX_TAPE_BLOCK_KB) {
	conf_parserror("Tape blocksize must not be larger than %d KBytes",
		  MAX_TAPE_BLOCK_KB);
    }
}

char *
getconf_byname(
    char *str)
{
    static char *tmpstr;
    char number[NUM_STR_SIZE];
    t_conf_var *np;
    keytab_t *kt;
    char *s;
    char ch;

    tmpstr = stralloc(str);
    s = tmpstr;
    while((ch = *s++) != '\0') {
	if(islower((int)ch))
	    s[-1] = (char)toupper(ch);
    }
    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++) {
	if(kt->keyword && strcmp(kt->keyword, tmpstr) == 0)
	    break;
    }

    if(kt->token == CONF_UNKNOWN)
	return NULL;

    for(np = server_var; np->token != CONF_UNKNOWN; np++) {
	if(np->token == kt->token)
	    break;
    }

    if(np->token == CONF_UNKNOWN) return NULL;

    if(np->type == CONFTYPE_INT) {
	snprintf(number, sizeof(number), "%d", server_conf[np->parm].v.i);
	tmpstr = newstralloc(tmpstr, number);
    } else if(np->type == CONFTYPE_BOOL) {
	if(getconf_boolean(np->parm) == 0) {
	    tmpstr = newstralloc(tmpstr, "off");
	} else {
	    tmpstr = newstralloc(tmpstr, "on");
	}
    } else if(np->type == CONFTYPE_REAL) {
	snprintf(number, sizeof(number), "%lf", server_conf[np->parm].v.r);
	tmpstr = newstralloc(tmpstr, number);
    } else if(np->type == CONFTYPE_AM64){
	snprintf(number, sizeof(number), OFF_T_FMT, 
		 (OFF_T_FMT_TYPE)server_conf[np->parm].v.am64);
	tmpstr = newstralloc(tmpstr, number);
    } else {
	tmpstr = newstralloc(tmpstr, getconf_str(np->parm));
    }

    return tmpstr;
}

int
getconf_seen(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    return(server_conf[np->parm].seen);
}

int
getconf_boolean(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_BOOL) {
	error("getconf_boolean: np is not a CONFTYPE_BOOL");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.i != 0);
}

int
getconf_int(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_INT) {
	error("getconf_int: np is not a CONFTYPE_INT");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.i);
}

long
getconf_long(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_LONG) {
	error("getconf_long: np is not a CONFTYPE_LONG");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.l);
}

time_t
getconf_time(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_TIME) {
	error("getconf_time: np is not a CONFTYPE_TIME");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.t);
}

ssize_t
getconf_size(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_SIZE) {
	error("getconf_size: np is not a CONFTYPE_SIZE");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.size);
}

off_t
getconf_am64(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_AM64) {
	error("getconf_am64: np is not a CONFTYPE_AM64");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.am64);
}

double
getconf_real(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_REAL) {
	error("getconf_real: np is not a CONFTYPE_REAL");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.r);
}

char *
getconf_str(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_STRING && np->type != CONFTYPE_IDENT) {
	error("getconf_str: np is not a CONFTYPE_STRING|CONFTYPE_IDENT: %d", parm);
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.s);
}

int
getconf_taperalgo(
    confparm_t parm)
{
    t_conf_var *np;
    np = get_np(server_var, parm);
    if (np->type != CONFTYPE_TAPERALGO) {
	error("getconf_taperalgo: np is not a CONFTYPE_TAPERALGO");
	/*NOTREACHED*/
    }
    return(server_conf[np->parm].v.i);
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
    conf_init_string(&server_conf[CNF_ORG], s);
    conf_init_string(&server_conf[CNF_MAILTO], "operators");
    conf_init_string(&server_conf[CNF_DUMPUSER], CLIENT_LOGIN);
#ifdef DEFAULT_TAPE_DEVICE
    s = DEFAULT_TAPE_DEVICE;
#else
    s = "/dev/rmt8";
#endif
    conf_init_string(&server_conf[CNF_TAPEDEV], s);
#ifdef DEFAULT_CHANGER_DEVICE
    s = DEFAULT_CHANGER_DEVICE;
#else
    s = "/dev/null";
#endif
    conf_init_string(&server_conf[CNF_CHNGRDEV], s);
    conf_init_string(&server_conf[CNF_CHNGRFILE], "/usr/adm/amanda/changer-status");
#ifdef DEFAULT_RAW_TAPE_DEVICE
    s = DEFAULT_RAW_TAPE_DEVICE;
#else
    s = "/dev/rawft0";
#endif
    conf_init_string   (&server_conf[CNF_LABELSTR]             , ".*");
    conf_init_string   (&server_conf[CNF_TAPELIST]             , "tapelist");
    conf_init_string   (&server_conf[CNF_DISKFILE]             , "disklist");
    conf_init_string   (&server_conf[CNF_INFOFILE]             , "/usr/adm/amanda/curinfo");
    conf_init_string   (&server_conf[CNF_LOGDIR]               , "/usr/adm/amanda");
    conf_init_string   (&server_conf[CNF_INDEXDIR]             , "/usr/adm/amanda/index");
    conf_init_ident    (&server_conf[CNF_TAPETYPE]             , "EXABYTE");
    conf_init_int      (&server_conf[CNF_DUMPCYCLE]            , 10);
    conf_init_int      (&server_conf[CNF_RUNSPERCYCLE]         , 0);
    conf_init_int      (&server_conf[CNF_TAPECYCLE]            , 15);
    conf_init_int      (&server_conf[CNF_NETUSAGE]             , 300);
    conf_init_int      (&server_conf[CNF_INPARALLEL]           , 10);
    conf_init_string   (&server_conf[CNF_DUMPORDER]            , "ttt");
    conf_init_int      (&server_conf[CNF_BUMPPERCENT]          , 0);
    conf_init_am64     (&server_conf[CNF_BUMPSIZE]             , (off_t)10*1024);
    conf_init_real     (&server_conf[CNF_BUMPMULT]             , 1.5);
    conf_init_int      (&server_conf[CNF_BUMPDAYS]             , 2);
    conf_init_string   (&server_conf[CNF_TPCHANGER]            , "");
    conf_init_int      (&server_conf[CNF_RUNTAPES]             , 1);
    conf_init_int      (&server_conf[CNF_MAXDUMPS]             , 1);
    conf_init_time     (&server_conf[CNF_ETIMEOUT]             , (time_t)300);
    conf_init_time     (&server_conf[CNF_DTIMEOUT]             , (time_t)1800);
    conf_init_time     (&server_conf[CNF_CTIMEOUT]             , (time_t)30);
    conf_init_int      (&server_conf[CNF_TAPEBUFS]             , 20);
    conf_init_string   (&server_conf[CNF_RAWTAPEDEV]           , s);
    conf_init_string   (&server_conf[CNF_PRINTER]              , "");
    conf_init_bool     (&server_conf[CNF_AUTOFLUSH]            , 0);
    conf_init_int      (&server_conf[CNF_RESERVE]              , 100);
    conf_init_am64     (&server_conf[CNF_MAXDUMPSIZE]          , (off_t)-1);
    conf_init_string   (&server_conf[CNF_COLUMNSPEC]           , "");
    conf_init_bool     (&server_conf[CNF_AMRECOVER_DO_FSF]     , 1);
    conf_init_string   (&server_conf[CNF_AMRECOVER_CHANGER]    , "");
    conf_init_bool     (&server_conf[CNF_AMRECOVER_CHECK_LABEL], 1);
    conf_init_taperalgo(&server_conf[CNF_TAPERALGO]            , 0);
    conf_init_string   (&server_conf[CNF_DISPLAYUNIT]          , "k");
    conf_init_string   (&server_conf[CNF_KRB5KEYTAB]           , "/.amanda-v5-keytab");
    conf_init_string   (&server_conf[CNF_KRB5PRINCIPAL]        , "service/amanda");
    conf_init_string   (&server_conf[CNF_LABEL_NEW_TAPES]      , "");
    conf_init_bool     (&server_conf[CNF_USETIMESTAMPS]        , 0);

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
    conf_set_compress(&dpcur.value[DUMPTYPE_COMPRESS], COMP_SERV_FAST);
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
	fprintf(stderr, "could not open conf file \"%s\": %s\n", conf_confname,
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
		conf_parserror("cannot open %s: %s\n", fn, strerror(errno));
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
	else conf_parserror("DUMPTYPE, INTERFACE or TAPETYPE expected");
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
		conf_parserror("configuration keyword expected");
	    } else {
		np->read_function(np, &server_conf[np->parm]);
		if(np->validate)
		    np->validate(np, &server_conf[np->parm]);
	    }
	}
    }
    if(tok != CONF_NL)
	get_conftoken(CONF_NL);
    return 1;
}

t_conf_var holding_var [] = {
   { CONF_DIRECTORY, CONFTYPE_STRING, read_string, HOLDING_DISKDIR  , NULL },
   { CONF_COMMENT  , CONFTYPE_STRING, read_string, HOLDING_COMMENT  , NULL },
   { CONF_USE      , CONFTYPE_AM64  , read_am64  , HOLDING_DISKSIZE , validate_use },
   { CONF_CHUNKSIZE, CONFTYPE_AM64  , read_am64  , HOLDING_CHUNKSIZE, validate_chunksize },
   { CONF_UNKNOWN  , CONFTYPE_INT   , NULL       , HOLDING_HOLDING  , NULL }
};

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
    read_block(server_options, holding_var, server_keytab, hdcur.value, prefix,
	       "holding disk parameter expected", 1, NULL);
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
   { CONF_STARTTIME         , CONFTYPE_TIME     , read_time   , DUMPTYPE_START_T           , NULL },
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
    read_block(server_options, dumptype_var, server_keytab, dpcur.value,
	       prefix, "dumptype parameter expected",
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
    conf_init_int      (&dpcur.value[DUMPTYPE_DUMPCYCLE]         , server_conf[CNF_DUMPCYCLE].v.i);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAXDUMPS]          , server_conf[CNF_MAXDUMPS].v.i);
    conf_init_int      (&dpcur.value[DUMPTYPE_MAXPROMOTEDAY]     , 10000);
    conf_init_int      (&dpcur.value[DUMPTYPE_BUMPPERCENT]       , server_conf[CNF_BUMPPERCENT].v.i);
    conf_init_am64     (&dpcur.value[DUMPTYPE_BUMPSIZE]          , server_conf[CNF_BUMPSIZE].v.am64);
    conf_init_int      (&dpcur.value[DUMPTYPE_BUMPDAYS]          , server_conf[CNF_BUMPDAYS].v.i);
    conf_init_real     (&dpcur.value[DUMPTYPE_BUMPMULT]          , server_conf[CNF_BUMPMULT].v.r);
    conf_init_time     (&dpcur.value[DUMPTYPE_START_T]           , (time_t)0);
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
	conf_parserror("dumptype %s already defined on line %d", dp->name, dp->seen);
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
	conf_parserror("dumptype parameter expected");
	return;
    }

    for(i=0; i < DUMPTYPE_DUMPTYPE; i++) {
	if(dt->value[i].seen) {
	    free_val_t(&dpcur.value[i]);
	    copy_val_t(&dpcur.value[i], &dt->value[i]);
	}
    }
}

t_conf_var tapetype_var [] = {
   { CONF_COMMENT  , CONFTYPE_STRING, read_string, TAPETYPE_COMMENT  , NULL },
   { CONF_LBL_TEMPL, CONFTYPE_STRING, read_string, TAPETYPE_LBL_TEMPL, NULL },
   { CONF_BLOCKSIZE, CONFTYPE_SIZE  , read_size  , TAPETYPE_BLOCKSIZE, validate_blocksize },
   { CONF_LENGTH   , CONFTYPE_AM64  , read_am64  , TAPETYPE_LENGTH   , validate_positive0 },
   { CONF_FILEMARK , CONFTYPE_AM64  , read_am64  , TAPETYPE_FILEMARK , NULL },
   { CONF_SPEED    , CONFTYPE_INT   , read_int   , TAPETYPE_SPEED    , validate_positive0 },
   { CONF_FILE_PAD , CONFTYPE_BOOL  , read_bool  , TAPETYPE_FILE_PAD , NULL },
   { CONF_UNKNOWN  , CONFTYPE_INT   , NULL       , TAPETYPE_TAPETYPE , NULL }
};

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
    read_block(server_options, tapetype_var, server_keytab, tpcur.value,
	       prefix, "tapetype parameter expected", 1, &copy_tapetype);
    amfree(prefix);
    get_conftoken(CONF_NL);

    save_tapetype();

    allow_overwrites = save_overwrites;
}

static void
init_tapetype_defaults(void)
{
    conf_init_string(&tpcur.value[TAPETYPE_COMMENT]  , "");
    conf_init_string(&tpcur.value[TAPETYPE_LBL_TEMPL], "");
    conf_init_size  (&tpcur.value[TAPETYPE_BLOCKSIZE], DISK_BLOCK_KB);
    conf_init_am64  (&tpcur.value[TAPETYPE_LENGTH]   , (off_t)2000 * 1024);
    conf_init_am64  (&tpcur.value[TAPETYPE_FILEMARK] , (off_t)1000);
    conf_init_int   (&tpcur.value[TAPETYPE_SPEED]    , 200);
    conf_init_bool  (&tpcur.value[TAPETYPE_FILE_PAD] , 1);
}

static void
save_tapetype(void)
{
    tapetype_t *tp, *tp1;

    tp = lookup_tapetype(tpcur.name);

    if(tp != (tapetype_t *)0) {
	amfree(tpcur.name);
	conf_parserror("tapetype %s already defined on line %d", tp->name, tp->seen);
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
	conf_parserror("tape type parameter expected");
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
    read_block(server_options, interface_var, server_keytab, ifcur.value,
	       prefix, "interface parameter expected", 1, &copy_interface);
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
	conf_parserror("interface %s already defined on line %d", ip->name,
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
	conf_parserror("interface parameter expected");
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
	conf_parserror("full compression rate must be >= 0");
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
	conf_parserror("incremental compression rate must be >= 0");
    }
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
	if(!none && fast && !best && !custom) comp = COMP_SERV_FAST;
	if(!none && !fast && best && !custom) comp = COMP_SERV_BEST;
	if(!none && !fast && !best && custom) comp = COMP_SERV_CUST;
    }

    if((int)comp == -1) {
	conf_parserror("NONE, CLIENT FAST, CLIENT BEST, CLIENT CUSTOM, SERVER FAST, SERVER BEST or SERVER CUSTOM expected");
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
     conf_parserror("NONE, CLIENT or SERVER expected");
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
	conf_parserror("NEVER, AUTO or REQUIRED expected");
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
	conf_parserror("FIRST, FIRSTFIT, LARGEST, LARGESTFIT, SMALLEST or LAST expected");
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
	conf_parserror("LOW, MEDIUM, HIGH or integer expected");
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
	conf_parserror("STANDARD or NOFULL expected");
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
	conf_parserror("CLIENT, SERVER or CALCSIZE expected");
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
    }
    else {
	file = 1;
	if(tok == CONF_EFILE) get_conftoken(CONF_ANY);
    }
    val->v.exinclude.type = file;
    exclude = val->v.exinclude.sl;
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

    val->v.exinclude.sl = exclude;
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
	    *errstr = stralloc2("invalid columnspec: ", s);
#ifdef TEST
	    fprintf(stderr, "%s: %s\n", myname, *errstr);
#endif
	    return -1;
	}
	*eon= '\0';
	cn=StringToColumn(s);
	if (ColumnData[cn].Name == NULL) {
	    *errstr = stralloc2("invalid column name: ", s);
#ifdef TEST
	    fprintf(stderr, "%s: %s\n", myname, *errstr);
#endif
	    return -1;
	}
	if (sscanf(eon+1, "%d:%d", &Space, &Width) != 2) {
	    *errstr = stralloc2("invalid format: ", eon + 1);
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

    printf("AMANDA CONFIGURATION FROM FILE \"%s\":\n\n", filename);

    for(i=0; i < CNF_CNF; i++) {
	for(np=server_var; np->token != CONF_UNKNOWN; np++) {
	    if(np->parm == i)
		break;
	}
	if(np->token == CONF_UNKNOWN)
	    error("server bad value");

	for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
	    if(kt->token == np->token) break;
	if(kt->token == CONF_UNKNOWN)
	    error("server bad token");

	printf("%-21s %s\n", kt->keyword, conf_print(&server_conf[i]));
    }

    for(hp = holdingdisks; hp != NULL; hp = hp->next) {
	printf("\nHOLDINGDISK %s {\n", hp->name);
	for(i=0; i < HOLDING_HOLDING; i++) {
	    for(np=holding_var; np->token != CONF_UNKNOWN; np++) {
		if(np->parm == i)
			break;
	    }
	    if(np->token == CONF_UNKNOWN)
		error("holding bad value");

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++) {
		if(kt->token == np->token)
		    break;
	    }
	    if(kt->token == CONF_UNKNOWN)
		error("holding bad token");

	    printf("      %-9s %s\n", kt->keyword, conf_print(&hp->value[i]));
	}
	printf("}\n");
    }

    for(tp = tapelist; tp != NULL; tp = tp->next) {
	printf("\nDEFINE TAPETYPE %s {\n", tp->name);
	for(i=0; i < TAPETYPE_TAPETYPE; i++) {
	    for(np=tapetype_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error("tapetype bad value");

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error("tapetype bad token");

	    printf("      %-9s %s\n", kt->keyword, conf_print(&tp->value[i]));
	}
	printf("}\n");
    }

    for(dp = dumplist; dp != NULL; dp = dp->next) {
	if(dp->seen == -1)
	    prefix = "#";
	else
	    prefix = "";
	printf("\n%sDEFINE DUMPTYPE %s {\n", prefix, dp->name);
	for(i=0; i < DUMPTYPE_DUMPTYPE; i++) {
	    for(np=dumptype_var; np->token != CONF_UNKNOWN; np++)
		if(np->parm == i) break;
	    if(np->token == CONF_UNKNOWN)
		error("dumptype bad value");

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error("dumptype bad token");

	    printf("%s      %-19s %s\n", prefix, kt->keyword, conf_print(&dp->value[i]));
	}
	printf("%s}\n", prefix);
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
		error("interface bad value");

	    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
		if(kt->token == np->token) break;
	    if(kt->token == CONF_UNKNOWN)
		error("interface bad token");

	    printf("%s      %-9s %s\n", prefix, kt->keyword, conf_print(&ip->value[i]));
	}
	printf("%s}\n",prefix);
    }

}

#ifdef TEST

int
main(
    int argc,
    char *argv[])
{
  char *conffile;
  char *diskfile;
  disklist_t lst;
  int result;
  unsigned long malloc_hist_1, malloc_size_1;
  unsigned long malloc_hist_2, malloc_size_2;

  safe_fd(-1, 0);

  set_pname("conffile");

  /* Don't die when child closes pipe */
  signal(SIGPIPE, SIG_IGN);

  malloc_size_1 = malloc_inuse(&malloc_hist_1);

  startclock();

  if (argc > 1) {
    if (argv[1][0] == '/') {
      config_dir = stralloc(argv[1]);
      config_name = strrchr(config_dir, '/') + 1;
      config_name[-1] = '\0';
      config_dir = newstralloc2(config_dir, config_dir, "/");
    } else {
      config_name = stralloc(argv[1]);
      config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    }
  } else {
    char my_cwd[STR_SIZE];

    if (getcwd(my_cwd, sizeof(my_cwd)) == NULL) {
      error("cannot determine current working directory");
    }
    config_dir = stralloc2(my_cwd, "/");
    if ((config_name = strrchr(my_cwd, '/')) != NULL) {
      config_name = stralloc(config_name + 1);
    }
  }

  conffile = stralloc2(config_dir, CONFFILE_NAME);
  result = read_conffile(conffile);
  if (result == 0) {
      diskfile = getconf_str(CNF_DISKFILE);
      if (diskfile != NULL && access(diskfile, R_OK) == 0) {
	  result = read_diskfile(diskfile, &lst);
      }
  }
  dump_configuration(CONFFILE_NAME);
  amfree(conffile);

  malloc_size_2 = malloc_inuse(&malloc_hist_2);

  if(malloc_size_1 != malloc_size_2) {
    malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
  }

  return result;
}

#endif /* TEST */

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

    for(kt = server_keytab; kt->token != CONF_UNKNOWN; kt++)
	if(kt->token == token) break;

    if(kt->token == CONF_UNKNOWN)
	return("");
    return(kt->keyword);
}

void
parse_server_conf(
    int parse_argc,
    char **parse_argv,
    int *new_argc,
    char ***new_argv)
{
    int i;
    char **my_argv;
    char *myarg, *value;
    command_option_t *server_option;

    server_options = alloc((size_t)(parse_argc+1) * SIZEOF(*server_options));
    server_option = server_options;
    server_option->name = NULL;

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
		    error("expect something after -o");
		myarg = parse_argv[i];
	    }
	    value = index(myarg,'=');
	    if (value == NULL) {
		conf_parserror("Must specify a value for %s.\n", myarg);
	    } else {
		*value = '\0';
		value++;
		server_option->used = 0;
		server_option->name = stralloc(myarg);
		server_option->value = stralloc(value);
		server_option++;
		server_option->name = NULL;
	    }
	}
	else {
	    my_argv[*new_argc] = stralloc(parse_argv[i]);
	    *new_argc += 1;
	}
	i++;
    }
}

void
report_bad_conf_arg(void)
{
    command_option_t *command_option;

    for(command_option = server_options; command_option->name != NULL;
							command_option++) {
	if(command_option->used == 0) {
	    fprintf(stderr,"argument -o%s=%s not used\n",
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

    if(server_options) {
	for(server_option = server_options; server_option->name != NULL;
						server_option++) {
	    amfree(server_option->name);
	    amfree(server_option->value);
        }
	amfree(server_options);
    }

    for(i=0; i<CNF_CNF-1; i++)
	free_val_t(&server_conf[i]);
}
