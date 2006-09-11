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
#include "util.h"

#define CONFFILE_NAME "amanda.conf"

typedef enum {
    CNF_ORG,
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
    CNF_CNF
} confparm_t;

typedef enum tapetype_e  {
    TAPETYPE_COMMENT,
    TAPETYPE_LBL_TEMPL,
    TAPETYPE_BLOCKSIZE,
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
#define tapetype_get_comment(tapetype)   get_conftype_string(&tapetype->value[TAPETYPE_COMMENT])
#define tapetype_get_lbl_templ(tapetype) get_conftype_string(&tapetype->value[TAPETYPE_LBL_TEMPL])
#define tapetype_get_blocksize(tapetype) get_conftype_size  (&tapetype->value[TAPETYPE_BLOCKSIZE])
#define tapetype_get_length(tapetype)    get_conftype_am64  (&tapetype->value[TAPETYPE_LENGTH])
#define tapetype_get_filemark(tapetype)  get_conftype_am64  (&tapetype->value[TAPETYPE_FILEMARK])
#define tapetype_get_speed(tapetype)     get_conftype_int   (&tapetype->value[TAPETYPE_SPEED])
#define tapetype_get_file_pad(tapetype)  get_conftype_bool  (&tapetype->value[TAPETYPE_FILE_PAD])

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
    DUMPTYPE_START_T,
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
#define dumptype_get_start_t(dumptype)            get_conftype_time     (&dumptype->value[DUMPTYPE_START_T])
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

/* this corresponds to the normal output of amanda, but may
 * be adapted to any spacing as you like.
 */
extern ColumnInfo ColumnData[];

extern char *config_name;
extern char *config_dir;

extern holdingdisk_t *holdingdisks;
extern int num_holdingdisks;

void parse_server_conf(int parse_argc, char **parse_argv, int *new_argc,
		       char ***new_argv);
void report_bad_conf_arg(void);
void free_server_config(void);

int read_conffile(char *filename);
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
char *getconf_byname(char *confname);
dumptype_t *lookup_dumptype(char *identifier);
dumptype_t *read_dumptype(char *name, FILE *from, char *fname, int *linenum);
tapetype_t *lookup_tapetype(char *identifier);
interface_t *lookup_interface(char *identifier);
holdingdisk_t *getconf_holdingdisks(void);
long int getconf_unit_divisor(void);
void dump_configuration(char *filename);
int ColumnDataCount(void);
int StringToColumn(char *s);
char LastChar(char *s);
int SetColumDataFromString(ColumnInfo* ci, char *s, char **errstr);

/* this is in securityconf.h */
char *generic_get_security_conf(char *, void *);
#endif /* ! CONFFILE_H */
