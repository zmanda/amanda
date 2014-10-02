/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
 * Copyright (c) 2007-2014 Zmanda, Inc.  All Rights Reserved.
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
 */

#include "amanda.h"
#include "amutil.h"
#include "conffile.h"
#include "ammessage.h"

char *errcode[500];

void init_errcode(void);

void
init_errcode(void)
{
    static int initialize = 0;
    int i =0;

    if (initialize)
	return;
    initialize = 1;

    for(i=0; i < 500; i++) {
	errcode[i] = "UNKNOWN";
    }
#ifdef E2BIG
  errcode[E2BIG] = "E2BIG";
#endif
#ifdef EACCES
  errcode[EACCES] = "EACCES";
#endif
#ifdef EADDRINUSE
  errcode[EADDRINUSE] = "EADDRINUSE";
#endif
#ifdef EADDRNOTAVAIL
  errcode[EADDRNOTAVAIL] = "EADDRNOTAVAIL";
#endif
#ifdef EAFNOSUPPORT
  errcode[EAFNOSUPPORT] = "EAFNOSUPPORT";
#endif
#ifdef EAGAIN
  errcode[EAGAIN] = "EAGAIN";
#endif
#ifdef EALREADY
  errcode[EALREADY] = "EALREADY";
#endif
#ifdef EBADE
  errcode[EBADE] = "EBADE";
#endif
#ifdef EBADF
  errcode[EBADF] = "EBADF";
#endif
#ifdef EBADFD
  errcode[EBADFD] = "EBADFD";
#endif
#ifdef EBADMSG
  errcode[EBADMSG] = "EBADMSG";
#endif
#ifdef EBADR
  errcode[EBADR] = "EBADR";
#endif
#ifdef EBADRQC
  errcode[EBADRQC] = "EBADRQC";
#endif
#ifdef EBADSLT
  errcode[EBADSLT] = "EBADSLT";
#endif
#ifdef EBUSY
  errcode[EBUSY] = "EBUSY";
#endif
#ifdef ECANCELED
  errcode[ECANCELED] = "ECANCELED";
#endif
#ifdef ECHILD
  errcode[ECHILD] = "ECHILD";
#endif
#ifdef ECHRNG
  errcode[ECHRNG] = "ECHRNG";
#endif
#ifdef ECOMM
  errcode[ECOMM] = "ECOMM";
#endif
#ifdef ECONNABORTED
  errcode[ECONNABORTED] = "ECONNABORTED";
#endif
#ifdef ECONNREFUSED
  errcode[ECONNREFUSED] = "ECONNREFUSED";
#endif
#ifdef ECONNRESET
  errcode[ECONNRESET] = "ECONNRESET";
#endif
#ifdef EDEADLK
  errcode[EDEADLK] = "EDEADLK";
#endif
#ifdef EDEADLOCK
  errcode[EDEADLOCK] = "EDEADLOCK";
#endif
#ifdef EDESTADDRREQ
  errcode[EDESTADDRREQ] = "EDESTADDRREQ";
#endif
#ifdef EDOM
  errcode[EDOM] = "EDOM";
#endif
#ifdef EDQUOT
  errcode[EDQUOT] = "EDQUOT";
#endif
#ifdef EEXIST
  errcode[EEXIST] = "EEXIST";
#endif
#ifdef EFAULT
  errcode[EFAULT] = "EFAULT";
#endif
#ifdef EFBIG
  errcode[EFBIG] = "EFBIG";
#endif
#ifdef EHOSTDOWN
  errcode[EHOSTDOWN] = "EHOSTDOWN";
#endif
#ifdef EHOSTUNREACH
  errcode[EHOSTUNREACH] = "EHOSTUNREACH";
#endif
#ifdef EIDRM
  errcode[EIDRM] = "EIDRM";
#endif
#ifdef EILSEQ
  errcode[EILSEQ] = "EILSEQ";
#endif
#ifdef EINPROGRESS
  errcode[EINPROGRESS] = "EINPROGRESS";
#endif
#ifdef EINTR
  errcode[EINTR] = "EINTR";
#endif
#ifdef EINVAL
  errcode[EINVAL] = "EINVAL";
#endif
#ifdef EIO
  errcode[EIO] = "EIO";
#endif
#ifdef EISCONN
  errcode[EISCONN] = "EISCONN";
#endif
#ifdef EISDIR
  errcode[EISDIR] = "EISDIR";
#endif
#ifdef EISNAM
  errcode[EISNAM] = "EISNAM";
#endif
#ifdef EKEYEXPIRED
  errcode[EKEYEXPIRED] = "EKEYEXPIRED";
#endif
#ifdef EKEYREJECTED
  errcode[EKEYREJECTED] = "EKEYREJECTED";
#endif
#ifdef EKEYREVOKED
  errcode[EKEYREVOKED] = "EKEYREVOKED";
#endif
#ifdef EL2HLT
  errcode[EL2HLT] = "EL2HLT";
#endif
#ifdef EL2NSYNC
  errcode[EL2NSYNC] = "EL2NSYNC";
#endif
#ifdef EL3HLT
  errcode[EL3HLT] = "EL3HLT";
#endif
#ifdef EL3RST
  errcode[EL3RST] = "EL3RST";
#endif
#ifdef ELIBACC
  errcode[ELIBACC] = "ELIBACC";
#endif
#ifdef ELIBBAD
  errcode[ELIBBAD] = "ELIBBAD";
#endif
#ifdef ELIBMAX
  errcode[ELIBMAX] = "ELIBMAX";
#endif
#ifdef ELIBSCN
  errcode[ELIBSCN] = "ELIBSCN";
#endif
#ifdef ELIBEXEC
  errcode[ELIBEXEC] = "ELIBEXEC";
#endif
#ifdef ELOOP
  errcode[ELOOP] = "ELOOP";
#endif
#ifdef EMEDIUMTYPE
  errcode[EMEDIUMTYPE] = "EMEDIUMTYPE";
#endif
#ifdef EMFILE
  errcode[EMFILE] = "EMFILE";
#endif
#ifdef EMLINK
  errcode[EMLINK] = "EMLINK";
#endif
#ifdef EMSGSIZE
  errcode[EMSGSIZE] = "EMSGSIZE";
#endif
#ifdef EMULTIHOP
  errcode[EMULTIHOP] = "EMULTIHOP";
#endif
#ifdef ENAMETOOLONG
  errcode[ENAMETOOLONG] = "ENAMETOOLONG";
#endif
#ifdef ENETDOWN
  errcode[ENETDOWN] = "ENETDOWN";
#endif
#ifdef ENETRESET
  errcode[ENETRESET] = "ENETRESET";
#endif
#ifdef ENETUNREACH
  errcode[ENETUNREACH] = "ENETUNREACH";
#endif
#ifdef ENFILE
  errcode[ENFILE] = "ENFILE";
#endif
#ifdef ENOBUFS
  errcode[ENOBUFS] = "ENOBUFS";
#endif
#ifdef ENODATA
  errcode[ENODATA] = "ENODATA";
#endif
#ifdef ENODEV
  errcode[ENODEV] = "ENODEV";
#endif
#ifdef ENOENT
  errcode[ENOENT] = "ENOENT";
#endif
#ifdef ENOEXEC
  errcode[ENOEXEC] = "ENOEXEC";
#endif
#ifdef ENOKEY
  errcode[ENOKEY] = "ENOKEY";
#endif
#ifdef ENOLCK
  errcode[ENOLCK] = "ENOLCK";
#endif
#ifdef ENOLINK
  errcode[ENOLINK] = "ENOLINK";
#endif
#ifdef ENOMEDIUM
  errcode[ENOMEDIUM] = "ENOMEDIUM";
#endif
#ifdef ENOMEM
  errcode[ENOMEM] = "ENOMEM";
#endif
#ifdef ENOMSG
  errcode[ENOMSG] = "ENOMSG";
#endif
#ifdef ENONET
  errcode[ENONET] = "ENONET";
#endif
#ifdef ENOPKG
  errcode[ENOPKG] = "ENOPKG";
#endif
#ifdef ENOPROTOOPT
  errcode[ENOPROTOOPT] = "ENOPROTOOPT";
#endif
#ifdef ENOSPC
  errcode[ENOSPC] = "ENOSPC";
#endif
#ifdef ENOSR
  errcode[ENOSR] = "ENOSR";
#endif
#ifdef ENOSTR
  errcode[ENOSTR] = "ENOSTR";
#endif
#ifdef ENOSYS
  errcode[ENOSYS] = "ENOSYS";
#endif
#ifdef ENOTBLK
  errcode[ENOTBLK] = "ENOTBLK";
#endif
#ifdef ENOTCONN
  errcode[ENOTCONN] = "ENOTCONN";
#endif
#ifdef ENOTDIR
  errcode[ENOTDIR] = "ENOTDIR";
#endif
#ifdef ENOTEMPTY
  errcode[ENOTEMPTY] = "ENOTEMPTY";
#endif
#ifdef ENOTSOCK
  errcode[ENOTSOCK] = "ENOTSOCK";
#endif
#ifdef ENOTSUP
  errcode[ENOTSUP] = "ENOTSUP";
#endif
#ifdef ENOTTY
  errcode[ENOTTY] = "ENOTTY";
#endif
#ifdef ENOTUNIQ
  errcode[ENOTUNIQ] = "ENOTUNIQ";
#endif
#ifdef ENXIO
  errcode[ENXIO] = "ENXIO";
#endif
#ifdef EOPNOTSUPP
  errcode[EOPNOTSUPP] = "EOPNOTSUPP";
#endif
#ifdef EOVERFLOW
  errcode[EOVERFLOW] = "EOVERFLOW";
#endif
#ifdef EPERM
  errcode[EPERM] = "EPERM";
#endif
#ifdef EPFNOSUPPORT
  errcode[EPFNOSUPPORT] = "EPFNOSUPPORT";
#endif
#ifdef EPIPE
  errcode[EPIPE] = "EPIPE";
#endif
#ifdef EPROTO
  errcode[EPROTO] = "EPROTO";
#endif
#ifdef EPROTONOSUPPORT
  errcode[EPROTONOSUPPORT] = "EPROTONOSUPPORT";
#endif
#ifdef EPROTOTYPE
  errcode[EPROTOTYPE] = "EPROTOTYPE";
#endif
#ifdef ERANGE
  errcode[ERANGE] = "ERANGE";
#endif
#ifdef EREMCHG
  errcode[EREMCHG] = "EREMCHG";
#endif
#ifdef EREMOTE
  errcode[EREMOTE] = "EREMOTE";
#endif
#ifdef EREMOTEIO
  errcode[EREMOTEIO] = "EREMOTEIO";
#endif
#ifdef ERESTART
  errcode[ERESTART] = "ERESTART";
#endif
#ifdef EROFS
  errcode[EROFS] = "EROFS";
#endif
#ifdef ESHUTDOWN
  errcode[ESHUTDOWN] = "ESHUTDOWN";
#endif
#ifdef ESPIPE
  errcode[ESPIPE] = "ESPIPE";
#endif
#ifdef ESOCKTNOSUPPORT
  errcode[ESOCKTNOSUPPORT] = "ESOCKTNOSUPPORT";
#endif
#ifdef ESRCH
  errcode[ESRCH] = "ESRCH";
#endif
#ifdef ESTALE
  errcode[ESTALE] = "ESTALE";
#endif
#ifdef ESTRPIPE
  errcode[ESTRPIPE] = "ESTRPIPE";
#endif
#ifdef ETIME
  errcode[ETIME] = "ETIME";
#endif
#ifdef ETIMEDOUT
  errcode[ETIMEDOUT] = "ETIMEDOUT";
#endif
#ifdef ETXTBSY
  errcode[ETXTBSY] = "ETXTBSY";
#endif
#ifdef EUCLEAN
  errcode[EUCLEAN] = "EUCLEAN";
#endif
#ifdef EUNATCH
  errcode[EUNATCH] = "EUNATCH";
#endif
#ifdef EUSERS
  errcode[EUSERS] = "EUSERS";
#endif
#ifdef EWOULDBLOCK
  errcode[EWOULDBLOCK] = "EWOULDBLOCK";
#endif
#ifdef EXDEV
  errcode[EXDEV] = "EXDEV";
#endif
#ifdef EXFULL
  errcode[EXFULL] = "EXFULL";
#endif
#ifdef EOWNERDEAD
  errcode[EOWNERDEAD] = "EOWNERDEAD";
#endif
#ifdef ENOTRECOVERABLE
  errcode[ENOTRECOVERABLE] = "ENOTRECOVERABLE";
#endif
#ifdef ERFKILL
  errcode[ERFKILL] = "ERFKILL";
#endif
#ifdef EHWPOISON
  errcode[EHWPOISON] = "EHWPOISON";
#endif
#ifdef ETOOMANYREFS
  errcode[ETOOMANYREFS] = "ETOOMANYREFS";
#endif
#ifdef ENOTNAM
  errcode[ENOTNAM] = "ENOTNAM";
#endif
#ifdef ENAVAIL
  errcode[ENAVAIL] = "ENAVAIL";
#endif
#ifdef EDOTDOT
  errcode[EDOTDOT] = "EDOTDOT";
#endif
#ifdef ESRMNT
  errcode[ESRMNT] = "ESRMNT";
#endif
#ifdef EADV
  errcode[EADV] = "EADV";
#endif
#ifdef EBFONT
  errcode[EBFONT] = "EBFONT";
#endif
#ifdef ENOANO
  errcode[ENOANO] = "ENOANO";
#endif
#ifdef ENOCSI
  errcode[ENOCSI] = "ENOCSI";
#endif
#ifdef ELNRNG
  errcode[ELNRNG] = "ELNRNG";
#endif
}

typedef struct message_arg_array_s {
    char *key;
    char *value;
} message_arg_array_t;

struct message_s {
    char *file;
    int   line;
    char *process;
    char *running_on;
    char *component;
    char *module;
    int   code;
    int   severity;
    char *msg;
    char *hint;
    char *errnocode;
    char *errnostr;
    message_arg_array_t *arg_array;
};

static char *ammessage_encode_json(char *str);
static void set_message(message_t *message);
static char *severity_name(int severity);

static char *
severity_name(
    int severity)
{
    if (severity == 1)
	return "success";
    else if (severity == 2)
	return "info";
    else if (severity == 4)
	return "message";
    else if (severity == 8)
	return "warning";
    else if (severity == 16)
	return "error";
    else if (severity == 32)
	return "critical";
    else
	return "unknown";
}

static char *
ammessage_encode_json(
    char *str)
{
    int i = 0;
    int len = strlen(str)*2;
    char *s = str;
    char *encoded = g_malloc(len+1);
    char *e = encoded;
    while(*s != '\0') {
	if (i++ >= len) {
	    error("encode_json: str is too long: %s", str);
	}
	if (*s == '\\' || *s == '"')
	    *e++ = '\\';
	*e++ = *s++;
    }
    *e = '\0';
    g_free(str);
    return encoded;
}

static char *
get_argument(
    message_t *message,
    char *key)
{
    int i = 0;
    char *m_message;

    while (message->arg_array[i].key != NULL) {
	if (strcmp(key, message->arg_array[i].key) == 0)
	    return message->arg_array[i].value;
	i++;
    }
    m_message = sprint_message(message);
    g_debug("Not value for key '%s' in message %s", key, m_message);
    g_free(m_message);
    return "";
}

static void
set_message(
    message_t *message)
{
    char *msg = NULL;
    char *hint = NULL;
    GString *result;
    char *m;
    char  num[NUM_STR_SIZE];
    char  code[100];
    char *c;
    int   i;

    init_errcode();
    if (message->code == 123) {
	msg  = "%{errstr}";
    } else if (message->code == 2800000) {
	msg  = "Usage: amcheck [--version] [-am] [-w] [-sclt] [-M <address>] [--client-verbose] [--exact_match] [-o configoption]* <conf> [host [disk]* ]*";
    } else if (message->code == 2800001) {
	msg  = "amcheck-%{version}";
    } else if (message->code == 2800002) {
	msg  = "Multiple -M options";
    } else if (message->code == 2800003) {
	msg  = "Invalid characters in mail address";
    } else if (message->code == 2800004) {
	msg  = "You can't use -a because a mailer is not defined";
    } else if (message->code == 2800005) {
	msg  = "You can't use -m because a mailer is not defined";
    } else if (message->code == 2800006) {
	msg  = "WARNING: No mail address configured in amanda.conf";
    } else if (message->code == 2800007) {
	msg  = "To receive dump results by email configure the "
                 "\"mailto\" parameter in amanda.conf";
    } else if (message->code == 2800008) {
	msg  = "When using -a option please specify -Maddress also";
    } else if (message->code == 2800009) {
	msg  = "Use -Maddress instead of -m";
    } else if (message->code == 2800010) {
	msg  = "Mail address '%{mailto}' in amanda.conf has invalid characters";
    } else if (message->code == 2800011) {
	msg  = "No email will be sent";
    } else if (message->code == 2800012) {
	msg  = "No mail address configured in amanda.conf";
    } else if (message->code == 2800013) {
	msg  = "When using -a option please specify -Maddress also";
    } else if (message->code == 2800014) {
	msg  = "Use -Maddress instead of -m";
    } else if (message->code == 2800015) {
	msg  = get_argument(message, "errstr");
    } else if (message->code == 2800016) {
	msg  = "(brought to you by Amanda %{version})";
    } else if (message->code == 2800017) {
	msg  = "Invalid mailto address '%{mailto}'";
    } else if (message->code == 2800018) {
	msg  = "ERROR: tapelist '%{tapelist}': should be a regular file";
    } else if (message->code == 2800019) {
	msg  = "ERROR: can't access tapelist '%{tapelist}': %{errnostr}";
    } else if (message->code == 2800020) {
	msg  = "ERROR: tapelist '%{tapelist}': not writable: %{errnostr}";
    } else if (message->code == 2800021) {
	msg  = "parent: reaped bogus pid %{pid}";
    } else if (message->code == 2800022) {
	msg  = "ERROR: program %{program}: does not exist";
    } else if (message->code == 2800023) {
	msg  = "ERROR: program %{program}: not a file";
    } else if (message->code == 2800024) {
	msg  = "ERROR: program %{program}: not executable";
    } else if (message->code == 2800025) {
	msg  = "ERROR: program %{program}: not setuid-root";
    } else if (message->code == 2800026) {
	msg  = "amcheck-device terminated with signal %{signal}";
    } else if (message->code == 2800027) {
	msg  = "Amanda Tape Server Host Check";
    } else if (message->code == 2800028) {
	msg  = "-----------------------------";
    } else if (message->code == 2800029) {
	msg  = "ERROR: storage '%{storage}': cannot read label template (lbl-templ) file %{filename}: %{errnostr}";
	hint = "       check permissions";
    } else if (message->code == 2800030) {
	msg  = "ERROR: storage '%{storage}': lbl-templ set but no LPR command defined";
	hint = "       you should reconfigure amanda and make sure it finds a lpr or lp command";
    } else if (message->code == 2800031) {
	msg  = "WARNING: storage '%{storage}': flush-threshold-dumped (%{flush_threshold_dumped}) must be less than or equal to flush-threshold-scheduled (%{flush_threshold_scheduled})";
    } else if (message->code == 2800032) {
	msg  = "WARNING: storage '%{storage}': taperflush (%{taperflush}) must be less than or equal to flush-threshold-scheduled (%{flush_threshold_scheduled})";
    } else if (message->code == 2800033) {
	msg  = "WARNING: storage '%{storage}': autoflush must be set to 'yes' or 'all' if taperflush (%{taperflush}) is greater that 0";
    } else if (message->code == 2800034) {
	msg  = "ERROR: storage '%{storage}': no tapetype specified; you must give a value for the 'tapetype' parameter or the storage";
    } else if (message->code == 2800035) {
	msg  = "ERROR: storage '%{storage}': runtapes is larger or equal to policy '%{policy'}' retention-tapes";
    } else if (message->code == 2800036) {
	msg  = "ERROR: system has %{size:kb_avail} memory, but device-output-buffer-size needs {size:kb_needed}";
    } else if (message->code == 2800037) {
	msg  = "ERROR: Cannot resolve `localhost': %{gai_strerror}";
    } else if (message->code == 2800038) {
	msg  = "ERROR: directory '%{dir}' containing Amanda tools is not accessible: %{errnostr}";
	hint = "       check permissions";
    } else if (message->code == 2800039) {
	msg = "Check permissions";
    } else if (message->code == 2800040) {
	msg  = "ERROR: directory '%{dir}' containing Amanda tools is not accessible: %{errnostr}";
	hint = "       check permissions";
    } else if (message->code == 2800041) {
	msg  = "Check permissions";
    } else if (message->code == 2800042) {
	msg  = "WARNING: '%{program}' is not executable: %{errnostr}, server-compression and indexing will not work";
	hint = "         check permissions";
    } else if (message->code == 2800043) {
	msg  = "Check permissions";
    } else if (message->code == 2800044) {
	msg  = "ERROR: tapelist dir '%{tape_dir}': not writable: %{errnostr}";
	hint = "       check permissions";
    } else if (message->code == 2800045) {
	msg  = "ERROR: tapelist '%{tapefile}' (%{errnostr}), you must create an empty file";
    } else if (message->code == 2800046) {
	msg  = "NOTE: tapelist will be created on the next run";
    } else if (message->code == 2800047) {
	msg  = "ERROR: tapelist '%{tapefile}': parse error";
    } else if (message->code == 2800048) {
	msg  = "WARNING: hold file '%{holdfile}' exists.\nAmdump will sleep as long as this file exists";
	hint = "         You might want to delete the existing hold file";
    } else if (message->code == 2800049) {
	msg  = "Amdump will sleep as long as this file exists";
    } else if (message->code == 2800050) {
	msg  = "You might want to delete the existing hold file";
    } else if (message->code == 2800051) {
	msg  = "WARNING:Parameter \"tapedev\", \"tpchanger\" or storage not specified in amanda.conf";
    } else if (message->code == 2800052) {
	msg  = "ERROR: part-cache-type specified, but no part-size";
    } else if (message->code == 2800053) {
	msg  = "ERROR: part-cache-dir specified, but no part-size";
    } else if (message->code == 2800054) {
	msg  = "ERROR: part-cache-max-size specified, but no part-size";
    } else if (message->code == 2800055) {
	msg  = "ERROR: part-cache-type is DISK, but no part-cache-dir specified";
    } else if (message->code == 2800056) {
	msg  = "ERROR: part-cache-dir '%{part-cache-dir}': %{errnostr}";
    } else if (message->code == 2800057) {
	msg  = "ERROR: part-cache-dir has %{size:kb_avail} available, but needs %{size:kb_needed}";
    } else if (message->code == 2800058) {
	msg  = "ERROR: system has %{size:kb_avail} memory, but part cache needs %{size:kb_needed}";
    } else if (message->code == 2800059) {
	msg  = "ERROR: part-cache-dir specified, but part-cache-type is not DISK";
    } else if (message->code == 2800060) {
	msg  = "ERROR: part_size is zero, but part-cache-type is not 'none'";
    } else if (message->code == 2800061) {
	msg  = "ERROR: part-cache-max-size is specified but no part cache is in use";
    } else if (message->code == 2800062) {
	msg  = "WARNING: part-cache-max-size is greater than part-size";
    } else if (message->code == 2800063) {
	msg  = "WARNING: part-size of %{size:part_size} < 0.1%% of tape length";
    } else if (message->code == 2800064) {
	msg  = "This may create > 1000 parts, severely degrading backup/restore performance."
        " See http://wiki.zmanda.com/index.php/Splitsize_too_small for more information.";
    } else if (message->code == 2800065) {
	msg  = "WARNING: part-cache-max-size of %{size:part_size_max_size} < 0.1%% of tape length";
    } else if (message->code == 2800066) {
	msg  = "ERROR: holding dir '%{holding_dir}' (%{errnostr})";
	hint = "       you must create a directory";
    } else if (message->code == 2800067) {
	msg  = "ERROR: holding disk '%{holding_dir}': not writable: %{errnostr}";
	hint = "       check permissions";
    } else if (message->code == 2800068) {
	msg  = "Check permissions";
    } else if (message->code == 2800069) {
	msg  = "ERROR: holding disk '%{holding_dir}': not searcheable: %{errnostr}";
	hint = "       check permissions of ancestors";
    } else if (message->code == 2800070) {
	msg  = "Check permissions of ancestors of";
    } else if (message->code == 2800071) {
	msg  = "WARNING: holding disk '%{holding_dir}': no space available (%{size:size} requested)";
    } else if (message->code == 2800072) {
	msg  = "WARNING: holding disk '%{holding_dir}': only %{size:avail} available (%{size:requested} requested)";
    } else if (message->code == 2800073) {
	msg = "Holding disk '%{holding_dir}': %{size:avail} disk space available, using %{size:requested} as requested";
    } else if (message->code == 2800074) {
	msg  = "WARNING: holding disk '%{holding_dir}': only %{size:avail} free, using nothing";
    } else if (message->code == 2800075) {
	msg = "WARNING: Not enough free space specified in amanda.conf";
    } else if (message->code == 2800076) {
	msg  = "Holding disk '%{holding_dir}': %{size:avail} disk space available, using %{size:using}";
    } else if (message->code == 2800077) {
	msg  = "ERROR: logdir '%{logdir}' (%{errnostr})";
	hint = "       you must create directory";
    } else if (message->code == 2800078) {
	msg  = "ERROR: log dir '%{logdir}' (%{errnostr}): not writable";
    } else if (message->code == 2800079) {
	msg  = "ERROR: oldlog directory '%{olddir}' is not a directory";
	hint = "       remove the entry and create a new directory";
    } else if (message->code == 2800080) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800081) {
	msg  = "ERROR: oldlog dir '%{oldlogdir}' (%{errnostr}): not writable";
	hint = "       check permissions";
    } else if (message->code == 2800082) {
	msg  = "Check permissions";
    } else if (message->code == 2800083) {
	msg  = "ERROR: oldlog directory '%{oldlogdir}' (%{errnostr}) is not a directory";
	hint = "       remove the entry and create a new directory";
    } else if (message->code == 2800084) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800085) {
	msg  = "WARNING: skipping tape test because amdump or amflush seem to be running";
    } else if (message->code == 2800086) {
	msg  = "WARNING: if they are not, you must run amcleanup";
    } else if (message->code == 2800087) {
	msg  = "NOTE: amdump or amflush seem to be running";
    } else if (message->code == 2800088) {
	msg  = "NOTE: if they are not, you must run amcleanup";
    } else if (message->code == 2800089) {
	msg  = "NOTE: skipping tape checks";
    } else if (message->code == 2800090) {
	msg  = "WARNING: tapecycle (%{tapecycle}) <= runspercycle (%{runspercycle)";
    } else if (message->code == 2800091) {
	msg  = "WARNING: tapecycle (%{tapecycle}) <= runtapes (%{runspercycle})";
    } else if (message->code == 2800092) {
	msg  = "NOTE: conf info dir '%{infodir}' does not exist";
	hint = "      it will be created on the next run";
    } else if (message->code == 2800093) {
	msg  = "NOTE: it will be created on the next run.";
    } else if (message->code == 2800094) {
	msg  = "ERROR: conf info dir '%{infodir}' (%{errnostr})";
    } else if (message->code == 2800095) {
	msg  = "ERROR: info dir '%{infodir}': not a directory";
	hint = "       remove the entry and create a new directory";
    } else if (message->code == 2800096) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800097) {
	msg  = "ERROR: info dir '{infodir}' (%{errnostr}): not writable";
	hint = "       check permissions";
    } else if (message->code == 2800098) {
	msg  = "Check permissions";
    } else if (message->code == 2800099) {
	msg  = "ERROR: Can't copy infofile: %{errmsg}";
    } else if (message->code == 2800100) {
	msg  = "NOTE: host info dir '%{hostinfodir}' does not exist";
	hint = "      It will be created on the next run";
    } else if (message->code == 2800101) {
	msg  = "NOTE: it will be created on the next run";
    } else if (message->code == 2800102) {
	msg  = "ERROR: host info dir '%{hostinfodir}' (%{errnostr})";
    } else if (message->code == 2800103) {
	msg  = "ERROR: info dir '%{hostinfodir}': not a directory";
	hint = "       Remove the entry and create a new directory";
    } else if (message->code == 2800104) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800105) {
	msg  = "ERROR: info dir '%{hostinfodir}': not writable";
	hint = "       Check permissions";
    } else if (message->code == 2800106) {
	msg  = "Check permissions";
    } else if (message->code == 2800107) {
	msg  = "NOTE: info dir '%{diskdir}' does not exist";
	hint = "      it will be created on the next run";
    } else if (message->code == 2800108) {
	msg  = "NOTE: it will be created on the next run.";
    } else if (message->code == 2800109) {
	msg  = "ERROR: info dir '%{diskdir}' (%{errnostr})";
    } else if (message->code == 2800110) {
	msg  = "ERROR: info dir '%{diskdir}': not a directory";
	hint = "       Remove the entry and create a new directory";
    } else if (message->code == 2800111) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800112) {
	msg  = "ERROR: info dir '%{diskdir}': not writable";
	hint = "       Check permissions";
    } else if (message->code == 2800113) {
	msg  = "Check permissions";
    } else if (message->code == 2800114) {
	msg  = "NOTE: info file '%{infofile}' does not exist";
	hint = "      it will be created on the next run";
    } else if (message->code == 2800115) {
	msg  = "NOTE: it will be created on the next run";
    } else if (message->code == 2800116) {
	msg  = "ERROR: info dir '%{diskdir}' (%{errnostr})";
    } else if (message->code == 2800117) {
	msg  = "ERROR: info file '%{infofile}': not a file";
	hint = "       remove the entry and create a new file";
    } else if (message->code == 2800118) {
	msg  = "Remove the entry and create a new file";
    } else if (message->code == 2800119) {
	msg  = "ERROR: info file '%{infofile}': not readable";
	hint = "       Check permissions";
    } else if (message->code == 2800120) {
	msg  = "NOTE: index dir '%{indexdir}' does not exist";
	hint = "      it will be created on the next run";
    } else if (message->code == 2800121) {
	msg  = "NOTE: it will be created on the next run.";
    } else if (message->code == 2800122) {
	msg  = "ERROR: index dir '%{indexdir}' (%{errnostr})";
    } else if (message->code == 2800123) {
	msg  = "ERROR: index dir '%{indexdir}': not a directory";
	hint = "       remove the entry and create a new directory";
    } else if (message->code == 2800124) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800125) {
	msg  = "ERROR: index dir '%{indexdir}': not writable";
    } else if (message->code == 2800126) {
	msg  = "NOTE: index dir '%{hostindexdir}' does not exist";
	hint = "      it will be created on the next run";
    } else if (message->code == 2800127) {
	msg  = "NOTE: it will be created on the next run.";
    } else if (message->code == 2800128) {
	msg  = "ERROR: index dir '%{hostindexdir}' (%{errnostr})";
    } else if (message->code == 2800129) {
	msg  = "ERROR: index dir '%{hostindexdir}': not a directory";
	hint = "       remove the entry and create a new directory";
    } else if (message->code == 2800130) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800131) {
	msg  = "ERROR: index dir '%{hostindexdir}': not writable";
	hint =       "       check permissions";
    } else if (message->code == 2800132) {
	msg  = "NOTE: index dir '%{diskindexdir}' does not exist";
	hint = "      it will be created on the next run";
    } else if (message->code == 2800133) {
	msg  = "NOTE: it will be created on the next run.";
    } else if (message->code == 2800134) {
	msg  = "ERROR: index dir '%{diskindexdir}' (%{'errnostr'})";
	hint = "       check permissions";
    } else if (message->code == 2800135) {
	msg  = "ERROR: index dir '%{diskindexdir}': not a directory";
	hint = "       remove the entry and create a new directory";
    } else if (message->code == 2800136) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800137) {
	msg  = "ERROR: index dir '%{diskindexdir}': is not writable";
	hint = "       check permissions";
    } else if (message->code == 2800138) {
	msg  = "ERROR: server encryption program not specified";
	hint = "Specify \"server-custom-encrypt\" in the dumptype";
    } else if (message->code == 2800139) {
	msg  = "Specify \"server-custom-encrypt\" in the dumptype";
    } else if (message->code == 2800140) {
	msg  = "ERROR: '%{program}' is not executable, server encryption will not work";
	hint = "       check file type";
    } else if (message->code == 2800141) {
	msg  = "Check file type";
    } else if (message->code == 2800142) {
	msg  = "ERROR: server custom compression program not specified";
	hint = "       Specify \"server-custom-compress\" in the dumptype";
    } else if (message->code == 2800143) {
	msg  = "Specify \"server-custom-compress\" in the dumptype";
    } else if (message->code == 2800144) {
	msg  = "ERROR: '%{program}' is not executable, server custom compression will not work";
	hint = "       check file type";
    } else if (message->code == 2800145) {
	msg  = "Check file type";
    } else if (message->code == 2800146) {
	msg  = "ERROR: %{hostname} %{diskname}: tape-splitsize > tape size";
    } else if (message->code == 2800147) {
	msg  = "ERROR: %{hostname} %{diskname}: fallback-splitsize > total available memory";
    } else if (message->code == 2800148) {
	msg  = "ERROR: %{hostname} %{diskname}: fallback-splitsize > tape size";
    } else if (message->code == 2800149) {
	msg  = "WARNING: %{hostname} %{diskname}: tape-splitsize of %{size:tape_splitsize} < 0.1%% of tape length";
    } else if (message->code == 2800150) {
    } else if (message->code == 2800151) {
	msg  = "WARNING: %{hostname} %{diskname}: fallback-splitsize of %{size:fallback_splitsize} < 0.1%% of tape length";
    } else if (message->code == 2800152) {
    } else if (message->code == 2800153) {
	msg  = "ERROR: %{hostname} %{diskname}: Can't compress directtcp data-path";
    } else if (message->code == 2800154) {
	msg  = "ERROR: %{hostname} %{diskname}: Can't encrypt directtcp data-path";
    } else if (message->code == 2800155) {
	msg  = "ERROR: %{hostname} %{diskname}: Holding disk can't be use for directtcp data-path";
    } else if (message->code == 2800156) {
	msg  = "ERROR: %{hostname} %{diskname}: data-path is DIRECTTCP but device do not support it";
    } else if (message->code == 2800157) {
	msg  = "ERROR: %{hostname} %{diskname}: data-path is AMANDA but device do not support it";
    } else if (message->code == 2800158) {
	msg  = "ERROR: %{hostname} %{diskname}: Can't run pre-host-backup script on client";
    } else if (message->code == 2800159) {
	msg  = "ERROR: %{hostname} %{diskname}: Can't run post-host-backup script on client";
    } else if (message->code == 2800160) {
	msg  = "Server check took %{seconds} seconds";
    } else if (message->code == 2800161) {
	msg  = "ERROR: Client %{hostname} does not support selfcheck REQ packet";
	hint = "       Client might be of a very old version";
    } else if (message->code == 2800162) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800163) {
	msg  = "ERROR: Client %{hostname} does not support selfcheck REP packet";
	hint = "       Client might be of a very old version";
    } else if (message->code == 2800164) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800165) {
	msg  = "ERROR: Client %{hostname} does not support sendsize REQ packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800166) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800167) {
	msg  = "ERROR: Client %{hostname} does not support sendsize REP packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800168) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800169) {
	msg  = "ERROR: Client %{hostname} does not support sendbackup REQ packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800170) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800171) {
	msg  = "ERROR: Client %{hostname} does not support sendbackup REP packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800172) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800173) {
	msg  = "ERROR: %{hostname}:%{diskname} %{errstr}";
    } else if (message->code == 2800174) {
	msg  = "ERROR: %{hostname}:%{diskname} (%{device}) host does not support quoted text";
	hint = "       You must upgrade amanda on the client to "
                                    "specify a quoted text/device in the disklist, "
                                    "or don't use quoted text for the device";
    } else if (message->code == 2800175) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a quoted text/device in the disklist, "
                                    "or don't use quoted text for the device";
    } else if (message->code == 2800176) {
	msg  = "ERROR: %{hostname}:%{diskname} (%{device}): selfcheck does not support device";
	hint = "         You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist "
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800177) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist "
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800178) {
	msg  = "ERROR: %{hostname}:%{diskname} (%{device}): sendsize does not support device";
	hint = "         You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800179) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800180) {
	msg  = "ERROR: %{hostname}:%{diskname} (%{device}): sendbackup does not support device";
	hint = "         You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800181) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800182) {
	msg  = "ERROR: Client %{hostname} does not support %{data-path} data-path";
    } else if (message->code == 2800183) {
	msg  = "ERROR: Client %{hostname} does not support directtcp data-path";
    } else if (message->code == 2800184) {
	msg  = "ERROR: %{hostname}:%{diskname} does not support DUMP";
	hint = "       You must upgrade amanda on the client to use DUMP "
                                    "or you can use another program";
    } else if (message->code == 2800185) {
	msg  = "You must upgrade amanda on the client to use DUMP "
                                    "or you can use another program";
    } else if (message->code == 2800186) {
	msg  = "ERROR: %{hostname}:%{diskname} does not support GNUTAR";
	hint = "       You must upgrade amanda on the client to use GNUTAR "
                                    "or you can use another program";
    } else if (message->code == 2800187) {
	msg  = "You must upgrade amanda on the client to use GNUTAR "
                                    "or you can use another program";
    } else if (message->code == 2800188) {
	msg  = "ERROR: %{hostname}:%{diskname} does not support CALCSIZE for estimate, using CLIENT";
	hint = "       You must upgrade amanda on the client to use "
                                    "CALCSIZE for estimate or don't use CALCSIZE for estimate";
    } else if (message->code == 2800189) {
	msg  = "You must upgrade amanda on the client to use "
                                    "CALCSIZE for estimate or don't use CALCSIZE for estimate";
    } else if (message->code == 2800180) {
	msg  = "ERROR: Client %{hostname} does not support custom compression";
	hint = "You must upgrade amanda on the client to use custom compression\n"
	       "Otherwise you can use the default client compression program";
    } else if (message->code == 2800191) {
	msg  = "You must upgrade amanda on the client to use custom compression";
    } else if (message->code == 2800192) {
	msg  = "Otherwise you can use the default client compression program";
    } else if (message->code == 2800193) {
	msg  = "ERROR: Client %{hostname} does not support data encryption";
	hint = "You must upgrade amanda on the client to use encryption program";
    } else if (message->code == 2800194) {
	msg  = "You must upgrade amanda on the client to use encryption program";
    } else if (message->code == 2800195) {
	msg  = "ERROR: %{hostname}: Client encryption with server compression is not supported";
	hint = "       See amanda.conf(5) for detail";
    } else if (message->code == 2800196) {
	msg  = "ERROR: %{hostname}:%{diskname} does not support APPLICATION-API";
	hint = "       Dumptype configuration is not GNUTAR or DUMP. It is case sensitive";
    } else if (message->code == 2800197) {
	msg  = "Dumptype configuration is not GNUTAR or DUMP. It is case sensitive";
    } else if (message->code == 2800198) {
	msg  = "ERROR: application '%{application}' not found";
    } else if (message->code == 2800199) {
	msg  = "WARNING: %{hostname}:%{diskname} does not support client-name in application";
    } else if (message->code == 2800200) {
	msg  = "ERROR: %{hostname}:%{diskname} does not support SCRIPT-API";
    } else if (message->code == 2800201) {
	msg  = "WARNING: %{hostname}:%{diskname} does not support client-name in script";
    } else if (message->code == 2800202) {
	msg  = "Amanda Backup Client Hosts Check";
    } else if (message->code == 2800203) {
	msg  = "--------------------------------";
    } else if (message->code == 2800204) {
	int hostcount = atoi(get_argument(message, "hostcount"));
	int remote_errors = atoi(get_argument(message, "remote_errors"));
	char *a = plural("Client check: %{hostcount} host checked in %{seconds} seconds.",
                         "Client check: %{hostcount} hosts checked in %{seconds} seconds.",
                         hostcount);
	char *b = plural("  %{remote_errors} problem found.",
                         "  %{remote_errors} problems found.",
                         remote_errors);
	msg  = g_strdup_printf("%s%s", a, b);
    } else if (message->code == 2800205) {
    } else if (message->code == 2800206) {
	msg  = "WARNING: %{hostname}: selfcheck request failed: %{errstr}";
    } else if (message->code == 2800207) {
	msg  = "ERROR: %{hostname}: bad features value: '%{features}'";
	hint = "       The amfeature in the reply packet is invalid";
    } else if (message->code == 2800208) {
	msg  = "The amfeature in the reply packet is invalid";
    } else if (message->code == 2800209) {
	msg  = "HOST %{hostname}";
    } else if (message->code == 2800210) {
	msg  = "HOST %{hostname}: %{ok_line}";
    } else if (message->code == 2800211) {
	msg  = "ERROR: %{type}%{hostname}: %{errstr}";
    } else if (message->code == 2800212) {
	msg  = "ERROR: %{hostname}: unknown response: %{errstr}";
    } else if (message->code == 2800213) {
	msg  = "ERROR: Could not find security driver '%{auth}' for host '%{hostname}'. auth for this dle is invalid";
    } else if (message->code == 2800214) {
    } else if (message->code == 2800215) {
	msg = g_strdup_printf("amanda.conf has dump user configured to '%s', but that user does not exist", get_argument(message, "dumpuser"));
    } else if (message->code == 2800216) {
	msg = g_strdup_printf("cannot get username for running user, uid %s is not in your user database", get_argument(message, "uid"));
    } else if (message->code == 2800217) {
	msg = g_strdup_printf("running as user '%s' instead of '%s'.\n"
                "Change user to '%s' or change dump user to '%s' in amanda.conf", get_argument(message, "running_user"), get_argument(message, "expected_user"), get_argument(message, "expected_user"), get_argument(message, "running_user"));
    } else if (message->code == 2800218) {
	msg = g_strdup_printf("could not open temporary amcheck output file %s: %s. Check permissions", get_argument(message, "filename"), get_argument(message, "errnostr"));
    } else if (message->code == 2800219) {
	msg = g_strdup_printf("could not open amcheck output file %s: %s. Check permissions", get_argument(message, "filename"), get_argument(message, "errnostr"));
    } else if (message->code == 2800220) {
	msg = g_strdup_printf("seek temp file: %s", get_argument(message, "errnostr"));
    } else if (message->code == 2800221) {
	msg = g_strdup_printf("fseek main file: %s", get_argument(message, "errnostr"));
    } else if (message->code == 2800222) {
	msg = g_strdup_printf("mailfd write: %s", get_argument(message, "errnostr"));
    } else if (message->code == 2800223) {
	msg = g_strdup_printf("mailfd write: wrote %s instead of %s", get_argument(message, "write_size"), get_argument(message, "expected_size"));
    } else if (message->code == 2800224) {
	msg = g_strdup_printf("Can't fdopen: %s", get_argument(message, "errnostr"));
    } else if (message->code == 2800225) {
	msg = g_strdup_printf("error running mailer %s: %s", get_argument(message, "mailer"), get_argument(message, "errmsg"));
    } else if (message->code == 2800226) {
	msg = g_strdup_printf("could not spawn a process for checking the server: %s", get_argument(message, "errnostr"));
    } else if (message->code == 2800227) {
	msg = g_strdup_printf("nullfd: /dev/null: %s", get_argument(message, "errnostr"));
    } else if (message->code == 2800228) {
	msg = g_strdup_printf("errors processing config file");
    } else if (message->code == 2800229) {
	msg = g_strdup_printf("Invalid mailto address '%s'", get_argument(message, "mailto"));
    } else if (message->code == 2800230) {
	msg = g_strdup_printf("Can't open '%s' for reading: %s", get_argument(message, "filename"), get_argument(message, "errnostr"));
    } else if (message->code == 2900000) {
	msg = "The Application '%{application}' failed: %{errmsg}";
    } else if (message->code == 2900001) {
	msg = "Can't execute application '%{application}'";
    } else if (message->code == 2900002) {
	msg = "The application '%{application}' does not support the 'discover' method";
    } else if (message->code == 2900003) {
	msg = "senddiscover only works with application";
    } else if (message->code == 2900004) {
	msg = "Missing OPTIONS line in senddiscover request";
    } else if (message->code == 2900005) {
	msg = "Application '%{application}': can't create pipe";
    } else if (message->code == 2900006) {
	msg = "Can't dup2: %{errno} %{errnocode} %{errnostr}";
    } else if (message->code == 2900007) {
	msg = "senddiscover require fe_req_xml";
    } else if (message->code == 2900008) {
	msg = "no valid senddiscover request";
    } else if (message->code == 2900009) {
	msg = "no valid senddiscover request";
    } else if (message->code == 2900010) {
	msg = "fork of '%{application} failed: %{errno} %{errnocode} %{errnostr}";
    } else if (message->code == 2900011) {
	msg = "Can't fdopen: %{errno} %{errnocode} %{errnostr}";
    } else if (message->code == 2900012) {
	msg = "%{application} failed: %{errmsg}";
    } else if (message->code == 2900013) {
	msg = "REQ XML error: %{errmsg}";
    } else if (message->code == 2900014) {
	msg = "One DLE required in XML REQ packet";
    } else if (message->code == 2900015) {
	msg = "Only one DLE allowed in XML REQ packet";
    } else if (message->code == 2900016) {
	msg = "Application '%{application}' (pid %{pid}) got signal %{signal}";
    } else if (message->code == 2900017) {
	msg = "Application '%{application}' (pid %{pid}) returned %{return_code}";
    } else {
	msg = "no message for code '%{code}'";
    }

    result = g_string_sized_new(strlen(msg)*2);

    for (m = msg; *m != '\0'; m++) {
	c = code;
	if (*m == '%' && *(m+1) == '%') {
	    g_string_append_c(result, *m);
	    m++;
	} else if (*m == '%' && *(m+1) == '{') {
	    m += 2;
	    while (*m != '}') {
		*c++ = *m++;
	    }
	    *c = '\0';
	    if (strcmp(code, "file") == 0) {
		g_string_append(result, message->file);
	    } else if (strcmp(code, "line") == 0) {
		g_snprintf(num, sizeof(num),
                "%d", message->line);
		g_string_append(result, num);
	    } else if (strcmp(code, "code") == 0) {
		g_snprintf(num, sizeof(num),
                "%d", message->code);
		g_string_append(result, num);
	    } else if (strcmp(code, "severity") == 0) {
		g_string_append(result, severity_name(message->severity));
	    } else if (strcmp(code, "errnostr") == 0) {
		g_free(message->errnostr);
		message->errnostr = NULL;
		i = 0;
		while (message->arg_array[i].key != NULL &&
		       strcmp(message->arg_array[i].key, "errno") != 0) {
		    i++;
		}
		if (message->arg_array[i].key != NULL) {
		    g_string_append(result, strerror(atoi(message->arg_array[i].value)));
		    message->errnostr = g_strdup(strerror(atoi(message->arg_array[i].value)));
		} else {
		    g_string_append(result, "NONE");
		}
	    } else if (strcmp(code, "errnocode") == 0) {
		g_free(message->errnocode);
		message->errnocode = NULL;
		i = 0;
		while (message->arg_array[i].key != NULL &&
		       strcmp(message->arg_array[i].key, "errno") != 0) {
		    i++;
		}
		if (message->arg_array[i].key != NULL) {
		    g_string_append(result, errcode[atoi(message->arg_array[i].value)]);
		    message->errnocode = g_strdup(errcode[atoi(message->arg_array[i].value)]);
		} else {
		    g_string_append(result, "NONE");
		}
	    } else {
		char *c = strchr(code, ':');
		char *format = NULL;
		char *ccode = code;
		if (c) {
		    *c = '\0';
		    format = code;
		    ccode = c+1;
		}
		i = 0;
		while (message->arg_array[i].key != NULL &&
		       strcmp(message->arg_array[i].key, ccode) != 0) {
		    i++;
		}
		if (message->arg_array[i].key != NULL) {
		    if (format) {
			if (strcmp(format,"size") == 0) {
			    long long llvalue = atoll(message->arg_array[i].value);
			    g_string_append_printf(result, "%lld %sB", llvalue/getconf_unit_divisor(),
								       getconf_str(CNF_DISPLAYUNIT));
			} else {
			    g_string_append(result, "BAD-FORMAT");
			}
		    } else {
			g_string_append(result, message->arg_array[i].value);
		    }
		} else {
		    g_string_append(result, "NONE");
		}
	    }
	} else {
	    g_string_append_c(result, *m);
	}
    }
    message->msg = ammessage_encode_json(g_string_free(result, FALSE));
    message->hint = hint;
}

char *
get_message(
    message_t *message)
{
    return message->msg;
}

char *
get_hint(
    message_t *message)
{
    return message->hint;
}

void
delete_message(
    message_t *message)
{
    int i;
    g_free(message->file);
    g_free(message->msg);
    g_free(message->errnocode);
    g_free(message->errnostr);
    for (i = 0; message->arg_array[i].key != NULL; i++) {
	g_free(message->arg_array[i].key);
	g_free(message->arg_array[i].value);
    }
    g_free(message->process);
    g_free(message->running_on);
    g_free(message->component);
    g_free(message->module);
    g_free(message->arg_array);
    g_free(message);
}

message_t *
build_message(
    char *file,
    int   line,
    int   code,
    int   severity,
    int   nb,
    ...)
{
   message_t *message = g_new0(message_t, 1);
   va_list marker;
   int     i;

   message->file = g_strdup(file);
   message->line = line;
   message->process = g_strdup(get_pname());
   message->running_on = g_strdup(get_running_on());
   message->component = g_strdup(get_pcomponent());
   message->module = g_strdup(get_pmodule());
   message->code = code;
   message->severity = severity;
   message->arg_array = g_new0(message_arg_array_t, nb+1);

   va_start(marker, nb);     /* Initialize variable arguments. */
   for (i = 0; i < nb; i++) {
	char *key = va_arg(marker, char *);
	if (strcmp(key,"errno") == 0) {
	    int m_errno = va_arg(marker, int);
	    message->errnocode = errcode[m_errno];
	    message->errnostr = strerror(m_errno);
	} else {
            message->arg_array[i].key = g_strdup(key);
            message->arg_array[i].value = g_strdup(va_arg(marker, char *));
	}
   }
   message->arg_array[i].key = NULL;
   message->arg_array[i].value = NULL;
   va_end( marker );

   set_message(message);

   g_debug("new message: %s:%d:%d:%d %s", message->file, message->line, message->severity, message->code, message->msg);
   return message;
}

char *
sprint_message(
    message_t *message)
{
    int i;
    static int first_message = 1;

    GString *result = g_string_sized_new(512);
    if (first_message) {
	first_message = 0;
    } else {
	g_string_append_c(result, ',');
    }
    g_string_append_printf(result,
        "  {\n" \
        "    \"source_filename\" : \"%s\",\n" \
        "    \"source_line\" : \"%d\",\n" \
        "    \"severity\" : \"%s\",\n" \
        "    \"process\" : \"%s\",\n" \
        "    \"running_on\" : \"%s\",\n" \
        "    \"component\" : \"%s\",\n" \
        "    \"module\" : \"%s\",\n" \
        "    \"code\" : \"%d\",\n" \
        , message->file, message->line, severity_name(message->severity), message->process, message->running_on, message->component, message->module, message->code);
    for (i = 0; message->arg_array[i].key != NULL; i++) {
	g_string_append_printf(result,
	"    \"%s\" : \"%s\",\n", message->arg_array[i].key, message->arg_array[i].value);
    }
    if (!message->msg) {
	set_message(message);
    }
    g_string_append_printf(result,
        "    \"message\" : \"%s\"" \
        , message->msg);
    if (message->hint) {
	g_string_append_printf(result,
	",\n    \"hint\" : \"%s\"" \
        , message->hint);
    }
    g_string_append_printf(result,
	"\n  }\n");

    return g_string_free(result, FALSE);
}

message_t *
print_message(
    message_t *message)
{
    char *msg = sprint_message(message);
    g_printf("%s", msg);
    g_free(msg);
    return message;
}

message_t *
fprint_message(
    FILE      *stream,
    message_t *message)
{
    char *msg = sprint_message(message);
    g_fprintf(stream, "%s", msg);
    g_free(msg);
    return message;
}

message_t *
fdprint_message(
    int       fd,
    message_t *message)
{
    char *msg = sprint_message(message);
    full_write(fd, msg, strlen(msg));
    g_free(msg);
    return message;
}

