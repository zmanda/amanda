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
  #if !defined EDEADLK || EDEADLK != EDEADLOCK
    errcode[EDEADLOCK] = "EDEADLOCK";
  #endif
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

typedef enum {
    MESSAGE_STRING,
    MESSAGE_NULL,
    MESSAGE_TRUE,
    MESSAGE_FALSE,
    MESSAGE_ARRAY,
    MESSAGE_HASH
} message_type_t;

typedef struct json_value_s {
    message_type_t type;
    union {
	char       *string;
	GPtrArray  *array;
	GHashTable *hash;
    };
} json_value_t;

typedef struct message_arg_array_s {
    char *key;
    json_value_t value;
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
    char *quoted_msg;
    char *hint;
    int   merrno;
    char *errnocode;
    char *errnostr;
    int   argument_allocated;
    message_arg_array_t *arg_array;
};

static char *ammessage_encode_json(char *str);
static void set_message(message_t *message, int want_quoted);
static char *severity_name(int severity);
static GString *fix_message_string(message_t *message, gboolean want_quoted, char *msg);

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
    int len;
    unsigned char *s;
    char *encoded;
    char *e;

    if (!str) {
	return g_strdup("null");
    }
    len = strlen(str)*2;
    s = (unsigned char *)str;
    encoded = g_malloc(len+1);
    e = encoded;

    while(*s != '\0') {
	if (i++ >= len) {
	    error("ammessage_encode_json: str is too long: %s", str);
	}
	if (*s == '\\' || *s == '"') {
	    *e++ = '\\';
	    *e++ = *s++;
	} else if (*s == '\b') {
	    *e++ = '\\';
	    *e++ = 'b';
	    s++;
	} else if (*s == '\f') {
	    *e++ = '\\';
	    *e++ = 'f';
	    s++;
	} else if (*s == '\n') {
	    *e++ = '\\';
	    *e++ = 'n';
	    s++;
	} else if (*s == '\r') {
	    *e++ = '\\';
	    *e++ = 'r';
	    s++;
	} else if (*s == '\t') {
	    *e++ = '\\';
	    *e++ = 't';
	    s++;
	} else if (*s == '\v') {
	    *e++ = '\\';
	    *e++ = 'v';
	    s++;
	} else if (*s < 32) {
	    *e++ = '\\';
	    *e++ = 'u';
	    *e++ = '0';
	    *e++ = '0';
	    if ((*s>>4) <= 9)
		*e++ = '0' + (*s>>4);
	    else
		*e++ = 'A' + (*s>4) - 10;
	    if ((*s & 0x0F) <= 9)
		*e++ = '0' + (*s & 0x0F);
	    else
		*e++ = 'A' + (*s & 0x0F) - 10;
	    s++;
	} else {
	    *e++ = *s++;
	}
    }
    *e = '\0';
    return encoded;
}

char *
message_get_argument(
    message_t *message,
    char *key)
{
    int i = 0;
    char *m_message;

    while (message->arg_array[i].key != NULL) {
	if (strcmp(key, message->arg_array[i].key) == 0) {
	    assert(message->arg_array[i].value.type == MESSAGE_STRING);
	    return message->arg_array[i].value.string;
	}
	i++;
    }
    m_message = sprint_message(message);
    g_debug("Not value for key '%s' in message %s", key, m_message);
    g_free(m_message);
    return "";
}

void
message_add_argument(
    message_t *message,
    char *key,
    char *value)
{
    int i = 0;

    while (message->arg_array[i].key != NULL) {
	if (strcmp(key, message->arg_array[i].key) == 0) {
	    assert(message->arg_array[i].value.type == MESSAGE_STRING);
	    g_free(message->arg_array[i].value.string);
	    message->arg_array[i].value.string = g_strdup(value);
	}
	i++;
    }
    if (i > message->argument_allocated) {
	message->argument_allocated *= 2;
	message->arg_array = g_realloc(message->arg_array, (message->argument_allocated+1) * sizeof(message_arg_array_t));
    }
    message->arg_array[i].key = g_strdup(key);
    message->arg_array[i].value.type = MESSAGE_STRING;
    message->arg_array[i].value.string = g_strdup(value);
    i++;
    message->arg_array[i].key = NULL;
    message->arg_array[i].value.type = MESSAGE_NULL;
    message->arg_array[i].value.string = NULL;
}

static void
set_message(
    message_t *message,
    int        want_quoted)
{
    char *msg = NULL;
    char *hint = NULL;
    GString *result;

    init_errcode();

    if (message == NULL)
	return;

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
	msg  = "No mail address configured in amanda.conf";
	hint = "To receive dump results by email configure the "
                 "\"mailto\" parameter in amanda.conf";
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
	msg  = "%{errstr}";
    } else if (message->code == 2800016) {
	msg  = "(brought to you by Amanda %{version})";
    } else if (message->code == 2800017) {
	msg  = "Invalid mailto address '%{mailto}'";
    } else if (message->code == 2800018) {
	msg  = "tapelist '%{tapelist}': should be a regular file";
    } else if (message->code == 2800019) {
	msg  = "can't access tapelist '%{tapelist}': %{errnostr}";
    } else if (message->code == 2800020) {
	msg  = "tapelist '%{tapelist}': not writable: %{errnostr}";
    } else if (message->code == 2800021) {
	msg  = "parent: reaped bogus pid %{pid}";
    } else if (message->code == 2800022) {
	msg  = "program %{program}: does not exist";
    } else if (message->code == 2800023) {
	msg  = "program %{program}: not a file";
    } else if (message->code == 2800024) {
	msg  = "program %{program}: not executable";
    } else if (message->code == 2800025) {
	msg  = "program %{program}: not setuid-root";
    } else if (message->code == 2800026) {
	msg  = "amcheck-device terminated with signal %{signal}";
    } else if (message->code == 2800027) {
	msg  = "Amanda Tape Server Host Check";
    } else if (message->code == 2800028) {
	msg  = "-----------------------------";
    } else if (message->code == 2800029) {
	msg  = "storage '%{storage}': cannot read label template (lbl-templ) file %{filename}: %{errnostr}";
	hint = "check permissions";
    } else if (message->code == 2800030) {
	msg  = "storage '%{storage}': lbl-templ set but no LPR command defined";
	hint = "you should reconfigure amanda and make sure it finds a lpr or lp command";
    } else if (message->code == 2800031) {
	msg  = "storage '%{storage}': flush-threshold-dumped (%{flush_threshold_dumped}) must be less than or equal to flush-threshold-scheduled (%{flush_threshold_scheduled})";
    } else if (message->code == 2800032) {
	msg  = "storage '%{storage}': taperflush (%{taperflush}) must be less than or equal to flush-threshold-scheduled (%{flush_threshold_scheduled})";
    } else if (message->code == 2800033) {
	msg  = "WARNING: storage '%{storage}': autoflush must be set to 'yes' or 'all' if taperflush (%{taperflush}) is greater that 0";
    } else if (message->code == 2800034) {
	msg  = "storage '%{storage}': no tapetype specified; you must give a value for the 'tapetype' parameter or the storage";
    } else if (message->code == 2800035) {
	msg  = "storage '%{storage}': runtapes is larger or equal to policy '%{policy}' retention-tapes";
    } else if (message->code == 2800036) {
	msg  = "system has %{size:kb_avail} memory, but device-output-buffer-size needs {size:kb_needed}";
    } else if (message->code == 2800037) {
	msg  = "Cannot resolve `localhost': %{gai_strerror}";
    } else if (message->code == 2800038) {
	msg  = "directory '%{dir}' containing Amanda tools is not accessible: %{errnostr}";
	hint = "check permissions";
    } else if (message->code == 2800039) {
	msg = "Check permissions";
    } else if (message->code == 2800040) {
	msg  = "directory '%{dir}' containing Amanda tools is not accessible: %{errnostr}";
	hint = "check permissions";
    } else if (message->code == 2800041) {
	msg  = "Check permissions";
    } else if (message->code == 2800042) {
	msg  = "WARNING: '%{program}' is not executable: %{errnostr}, server-compression and indexing will not work";
	hint = "check permissions";
    } else if (message->code == 2800043) {
	msg  = "Check permissions";
    } else if (message->code == 2800044) {
	msg  = "tapelist dir '%{tape_dir}': not writable: %{errnostr}";
	hint = "check permissions";
    } else if (message->code == 2800045) {
	msg  = "tapelist '%{tapefile}' (%{errnostr}), you must create an empty file";
    } else if (message->code == 2800046) {
	msg  = "tapelist file do not exists";
	hint = "it will be created on the next run";
    } else if (message->code == 2800047) {
	msg  = "tapelist '%{tapefile}': parse error";
    } else if (message->code == 2800048) {
	msg  = "hold file '%{holdfile}' exists. Amdump will sleep as long as this file exists";
	hint = "You might want to delete the existing hold file";
    } else if (message->code == 2800049) {
	msg  = "Amdump will sleep as long as this file exists";
    } else if (message->code == 2800050) {
	msg  = "You might want to delete the existing hold file";
    } else if (message->code == 2800051) {
	msg  = "WARNING:Parameter \"tapedev\", \"tpchanger\" or storage not specified in amanda.conf";
    } else if (message->code == 2800052) {
	msg  = "part-cache-type specified, but no part-size";
    } else if (message->code == 2800053) {
	msg  = "part-cache-dir specified, but no part-size";
    } else if (message->code == 2800054) {
	msg  = "part-cache-max-size specified, but no part-size";
    } else if (message->code == 2800055) {
	msg  = "part-cache-type is DISK, but no part-cache-dir specified";
    } else if (message->code == 2800056) {
	msg  = "part-cache-dir '%{part-cache-dir}': %{errnostr}";
    } else if (message->code == 2800057) {
	msg  = "part-cache-dir has %{size:kb_avail} available, but needs %{size:kb_needed}";
    } else if (message->code == 2800058) {
	msg  = "system has %{size:kb_avail} memory, but part cache needs %{size:kb_needed}";
    } else if (message->code == 2800059) {
	msg  = "part-cache-dir specified, but part-cache-type is not DISK";
    } else if (message->code == 2800060) {
	msg  = "part_size is zero, but part-cache-type is not 'none'";
    } else if (message->code == 2800061) {
	msg  = "part-cache-max-size is specified but no part cache is in use";
    } else if (message->code == 2800062) {
	msg  = "WARNING: part-cache-max-size is greater than part-size";
    } else if (message->code == 2800063) {
	msg  = "WARNING: part-size of %{size:part_size} < 0.1%% of tape length";
    } else if (message->code == 2800064) {
	msg  = "This may create > 1000 parts, severely degrading backup/restore performance."
        " See http://wiki.zmanda.com/index.php/Splitsize_too_small for more information.";
    } else if (message->code == 2800065) {
	msg  = "part-cache-max-size of %{size:part_size_max_size} < 0.1%% of tape length";
    } else if (message->code == 2800066) {
	msg  = "holding dir '%{holding_dir}' (%{errnostr})";
	hint = "you must create a directory";
    } else if (message->code == 2800067) {
	msg  = "holding disk '%{holding_dir}': not writable: %{errnostr}";
	hint = "check permissions";
    } else if (message->code == 2800068) {
	msg  = "Check permissions";
    } else if (message->code == 2800069) {
	msg  = "holding disk '%{holding_dir}': not searcheable: %{errnostr}";
	hint = "check permissions of ancestors";
    } else if (message->code == 2800070) {
	msg  = "Check permissions of ancestors of";
    } else if (message->code == 2800071) {
	msg  = "WARNING: holding disk '%{holding_dir}': no space available (%{size:size} requested)";
    } else if (message->code == 2800072) {
	msg  = "WARNING: holding disk '%{holding_dir}': only %{size:avail} available (%{size:requested} requested)";
    } else if (message->code == 2800073) {
	msg = "Holding disk '%{holding_dir}': %{size:avail} disk space available, using %{size:requested} as requested";
    } else if (message->code == 2800074) {
	msg  = "holding disk '%{holding_dir}': only %{size:avail} free, using nothing";
    } else if (message->code == 2800075) {
	msg = "Not enough free space specified in amanda.conf";
    } else if (message->code == 2800076) {
	msg  = "Holding disk '%{holding_dir}': %{size:avail} disk space available, using %{size:using}";
    } else if (message->code == 2800077) {
	msg  = "logdir '%{logdir}' (%{errnostr})";
	hint = "you must create directory";
    } else if (message->code == 2800078) {
	msg  = "log dir '%{logdir}' (%{errnostr}): not writable";
    } else if (message->code == 2800079) {
	msg  = "oldlog directory '%{olddir}' is not a directory";
	hint = "remove the entry and create a new directory";
    } else if (message->code == 2800080) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800081) {
	msg  = "oldlog dir '%{oldlogdir}' (%{errnostr}): not writable";
	hint = "check permissions";
    } else if (message->code == 2800082) {
	msg  = "Check permissions";
    } else if (message->code == 2800083) {
	msg  = "oldlog directory '%{oldlogdir}' (%{errnostr}) is not a directory";
	hint = "remove the entry and create a new directory";
    } else if (message->code == 2800084) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800085) {
	msg  = "skipping tape test because amdump or amflush seem to be running";
	hint = "if they are not, you must run amcleanup";
    } else if (message->code == 2800086) {
	msg  = "if they are not, you must run amcleanup";
    } else if (message->code == 2800087) {
	msg  = "amdump or amflush seem to be running";
	hint = "if they are not, you must run amcleanup";
    } else if (message->code == 2800088) {
	msg  = "if they are not, you must run amcleanup";
    } else if (message->code == 2800089) {
	msg  = "skipping tape checks";
    } else if (message->code == 2800090) {
	msg  = "tapecycle (%{tapecycle}) <= runspercycle (%{runspercycle}";
    } else if (message->code == 2800091) {
	msg  = "tapecycle (%{tapecycle}) <= runtapes (%{runtapes})";
    } else if (message->code == 2800092) {
	msg  = "conf info dir '%{infodir}' does not exist";
	hint = "it will be created on the next run";
    } else if (message->code == 2800093) {
	msg  = "it will be created on the next run.";
    } else if (message->code == 2800094) {
	msg  = "conf info dir '%{infodir}' (%{errnostr})";
    } else if (message->code == 2800095) {
	msg  = "info dir '%{infodir}': not a directory";
	hint = "remove the entry and create a new directory";
    } else if (message->code == 2800096) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800097) {
	msg  = "info dir '{infodir}' (%{errnostr}): not writable";
	hint = "check permissions";
    } else if (message->code == 2800098) {
	msg  = "Check permissions";
    } else if (message->code == 2800099) {
	msg  = "Can't copy infofile: %{errmsg}";
    } else if (message->code == 2800100) {
	msg  = "host info dir '%{hostinfodir}' does not exist";
	hint = "It will be created on the next run";
    } else if (message->code == 2800101) {
	msg  = "it will be created on the next run";
    } else if (message->code == 2800102) {
	msg  = "host info dir '%{hostinfodir}' (%{errnostr})";
    } else if (message->code == 2800103) {
	msg  = "info dir '%{hostinfodir}': not a directory";
	hint = "Remove the entry and create a new directory";
    } else if (message->code == 2800104) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800105) {
	msg  = "info dir '%{hostinfodir}': not writable";
	hint = "Check permissions";
    } else if (message->code == 2800106) {
	msg  = "Check permissions";
    } else if (message->code == 2800107) {
	msg  = "info dir '%{diskdir}' does not exist";
	hint = "it will be created on the next run";
    } else if (message->code == 2800108) {
	msg  = "it will be created on the next run.";
    } else if (message->code == 2800109) {
	msg  = "info dir '%{diskdir}' (%{errnostr})";
    } else if (message->code == 2800110) {
	msg  = "info dir '%{diskdir}': not a directory";
	hint = "Remove the entry and create a new directory";
    } else if (message->code == 2800111) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800112) {
	msg  = "info dir '%{diskdir}': not writable";
	hint = "Check permissions";
    } else if (message->code == 2800113) {
	msg  = "Check permissions";
    } else if (message->code == 2800114) {
	msg  = "info file '%{infofile}' does not exist";
	hint = "it will be created on the next run";
    } else if (message->code == 2800115) {
	msg  = "it will be created on the next run";
    } else if (message->code == 2800116) {
	msg  = "info dir '%{diskdir}' (%{errnostr})";
    } else if (message->code == 2800117) {
	msg  = "info file '%{infofile}': not a file";
	hint = "remove the entry and create a new file";
    } else if (message->code == 2800118) {
	msg  = "Remove the entry and create a new file";
    } else if (message->code == 2800119) {
	msg  = "info file '%{infofile}': not readable";
	hint = "Check permissions";
    } else if (message->code == 2800120) {
	msg  = "index dir '%{indexdir}' does not exist";
	hint = "it will be created on the next run";
    } else if (message->code == 2800121) {
	msg  = "it will be created on the next run.";
    } else if (message->code == 2800122) {
	msg  = "index dir '%{indexdir}' (%{errnostr})";
    } else if (message->code == 2800123) {
	msg  = "index dir '%{indexdir}': not a directory";
	hint = "remove the entry and create a new directory";
    } else if (message->code == 2800124) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800125) {
	msg  = "index dir '%{indexdir}': not writable";
    } else if (message->code == 2800126) {
	msg  = "index dir '%{hostindexdir}' does not exist";
	hint = "it will be created on the next run";
    } else if (message->code == 2800127) {
	msg  = "it will be created on the next run.";
    } else if (message->code == 2800128) {
	msg  = "index dir '%{hostindexdir}' (%{errnostr})";
    } else if (message->code == 2800129) {
	msg  = "index dir '%{hostindexdir}': not a directory";
	hint = "remove the entry and create a new directory";
    } else if (message->code == 2800130) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800131) {
	msg  = "index dir '%{hostindexdir}': not writable";
	hint = "check permissions";
    } else if (message->code == 2800132) {
	msg  = "index dir '%{diskindexdir}' does not exist";
	hint = "it will be created on the next run";
    } else if (message->code == 2800133) {
	msg  = "it will be created on the next run.";
    } else if (message->code == 2800134) {
	msg  = "index dir '%{diskindexdir}' (%{errnostr})";
	hint = "check permissions";
    } else if (message->code == 2800135) {
	msg  = "index dir '%{diskindexdir}': not a directory";
	hint = "remove the entry and create a new directory";
    } else if (message->code == 2800136) {
	msg  = "Remove the entry and create a new directory";
    } else if (message->code == 2800137) {
	msg  = "index dir '%{diskindexdir}': is not writable";
	hint = "check permissions";
    } else if (message->code == 2800138) {
	msg  = "server encryption program not specified";
	hint = "Specify \"server-custom-encrypt\" in the dumptype";
    } else if (message->code == 2800139) {
	msg  = "Specify \"server-custom-encrypt\" in the dumptype";
    } else if (message->code == 2800140) {
	msg  = "'%{program}' is not executable, server encryption will not work";
	hint = "check file type";
    } else if (message->code == 2800141) {
	msg  = "Check file type";
    } else if (message->code == 2800142) {
	msg  = "server custom compression program not specified";
	hint = "Specify \"server-custom-compress\" in the dumptype";
    } else if (message->code == 2800143) {
	msg  = "Specify \"server-custom-compress\" in the dumptype";
    } else if (message->code == 2800144) {
	msg  = "'%{program}' is not executable, server custom compression will not work";
	hint = "check file type";
    } else if (message->code == 2800145) {
	msg  = "Check file type";
    } else if (message->code == 2800146) {
	msg  = "%{hostname} %{diskname}: tape-splitsize > tape size";
    } else if (message->code == 2800147) {
	msg  = "%{hostname} %{diskname}: fallback-splitsize > total available memory";
    } else if (message->code == 2800148) {
	msg  = "%{hostname} %{diskname}: fallback-splitsize > tape size";
    } else if (message->code == 2800149) {
	msg  = "%{hostname} %{diskname}: tape-splitsize of %{size:tape_splitsize} < 0.1%% of tape length";
    } else if (message->code == 2800151) {
	msg  = "%{hostname} %{diskname}: fallback-splitsize of %{size:fallback_splitsize} < 0.1%% of tape length";
    } else if (message->code == 2800153) {
	msg  = "%{hostname} %{diskname}: Can't compress directtcp data-path";
    } else if (message->code == 2800154) {
	msg  = "%{hostname} %{diskname}: Can't encrypt directtcp data-path";
    } else if (message->code == 2800155) {
	msg  = "%{hostname} %{diskname}: Holding disk can't be use for directtcp data-path";
    } else if (message->code == 2800156) {
	msg  = "%{hostname} %{diskname}: data-path is DIRECTTCP but device do not support it";
    } else if (message->code == 2800157) {
	msg  = "%{hostname} %{diskname}: data-path is AMANDA but device do not support it";
    } else if (message->code == 2800158) {
	msg  = "%{hostname} %{diskname}: Can't run pre-host-backup script on client";
    } else if (message->code == 2800159) {
	msg  = "%{hostname} %{diskname}: Can't run post-host-backup script on client";
    } else if (message->code == 2800160) {
	msg  = "Server check took %{seconds} seconds";
    } else if (message->code == 2800161) {
	msg  = "Client %{hostname} does not support selfcheck REQ packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800162) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800163) {
	msg  = "Client %{hostname} does not support selfcheck REP packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800164) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800165) {
	msg  = "Client %{hostname} does not support sendsize REQ packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800166) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800167) {
	msg  = "Client %{hostname} does not support sendsize REP packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800168) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800169) {
	msg  = "Client %{hostname} does not support sendbackup REQ packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800170) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800171) {
	msg  = "Client %{hostname} does not support sendbackup REP packet";
	hint = "Client might be of a very old version";
    } else if (message->code == 2800172) {
	msg  = "Client might be of a very old version";
    } else if (message->code == 2800173) {
	msg  = "%{hostname}:%{diskname} %{errstr}";
    } else if (message->code == 2800174) {
	msg  = "%{hostname}:%{diskname} (%{device}) host does not support quoted text";
	hint = "You must upgrade amanda on the client to "
                                    "specify a quoted text/device in the disklist, "
                                    "or don't use quoted text for the device";
    } else if (message->code == 2800175) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a quoted text/device in the disklist, "
                                    "or don't use quoted text for the device";
    } else if (message->code == 2800176) {
	msg  = "%{hostname}:%{diskname} (%{device}): selfcheck does not support device";
	hint = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist "
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800177) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist "
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800178) {
	msg  = "%{hostname}:%{diskname} (%{device}): sendsize does not support device";
	hint = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800179) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800180) {
	msg  = "%{hostname}:%{diskname} (%{device}): sendbackup does not support device";
	hint = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800181) {
	msg  = "You must upgrade amanda on the client to "
                                    "specify a diskdevice in the disklist"
                                    "or don't specify a diskdevice in the disklist";
    } else if (message->code == 2800182) {
	msg  = "Client %{hostname} does not support %{data-path} data-path";
    } else if (message->code == 2800183) {
	msg  = "Client %{hostname} does not support directtcp data-path";
    } else if (message->code == 2800184) {
	msg  = "%{hostname}:%{diskname} does not support DUMP";
	hint = "You must upgrade amanda on the client to use DUMP "
                                    "or you can use another program";
    } else if (message->code == 2800185) {
	msg  = "You must upgrade amanda on the client to use DUMP "
                                    "or you can use another program";
    } else if (message->code == 2800186) {
	msg  = "%{hostname}:%{diskname} does not support GNUTAR";
	hint = "You must upgrade amanda on the client to use GNUTAR "
                                    "or you can use another program";
    } else if (message->code == 2800187) {
	msg  = "You must upgrade amanda on the client to use GNUTAR "
                                    "or you can use another program";
    } else if (message->code == 2800188) {
	msg  = "%{hostname}:%{diskname} does not support CALCSIZE for estimate, using CLIENT";
	hint = "You must upgrade amanda on the client to use "
                                    "CALCSIZE for estimate or don't use CALCSIZE for estimate";
    } else if (message->code == 2800189) {
	msg  = "You must upgrade amanda on the client to use "
                                    "CALCSIZE for estimate or don't use CALCSIZE for estimate";
    } else if (message->code == 2800180) {
	msg  = "Client %{hostname} does not support custom compression";
	hint = "You must upgrade amanda on the client to use custom compression\n"
	       "Otherwise you can use the default client compression program";
    } else if (message->code == 2800191) {
	msg  = "You must upgrade amanda on the client to use custom compression";
    } else if (message->code == 2800192) {
	msg  = "Otherwise you can use the default client compression program";
    } else if (message->code == 2800193) {
	msg  = "Client %{hostname} does not support data encryption";
	hint = "You must upgrade amanda on the client to use encryption program";
    } else if (message->code == 2800194) {
	msg  = "You must upgrade amanda on the client to use encryption program";
    } else if (message->code == 2800195) {
	msg  = "%{hostname}: Client encryption with server compression is not supported";
	hint = "See amanda.conf(5) for detail";
    } else if (message->code == 2800196) {
	msg  = "%{hostname}:%{diskname} does not support APPLICATION-API";
	hint = "Dumptype configuration is not GNUTAR or DUMP. It is case sensitive";
    } else if (message->code == 2800197) {
	msg  = "Dumptype configuration is not GNUTAR or DUMP. It is case sensitive";
    } else if (message->code == 2800198) {
	msg  = "application '%{application}' not found";
    } else if (message->code == 2800199) {
	msg  = "%{hostname}:%{diskname} does not support client-name in application";
    } else if (message->code == 2800200) {
	msg  = "%{hostname}:%{diskname} does not support SCRIPT-API";
    } else if (message->code == 2800201) {
	msg  = "WARNING: %{hostname}:%{diskname} does not support client-name in script";
    } else if (message->code == 2800202) {
	msg  = "Amanda Backup Client Hosts Check";
    } else if (message->code == 2800203) {
	msg  = "--------------------------------";
    } else if (message->code == 2800204) {
	int hostcount = atoi(message_get_argument(message, "hostcount"));
	int remote_errors = atoi(message_get_argument(message, "remote_errors"));
	char *a = plural("Client check: %{hostcount} host checked in %{seconds} seconds.",
                         "Client check: %{hostcount} hosts checked in %{seconds} seconds.",
                         hostcount);
	char *b = plural("  %{remote_errors} problem found.",
                         "  %{remote_errors} problems found.",
                         remote_errors);
	msg  = g_strdup_printf("%s%s", a, b);
    } else if (message->code == 2800206) {
	msg  = "%{hostname}: selfcheck request failed: %{errstr}";
    } else if (message->code == 2800207) {
	msg  = "%{hostname}: bad features value: '%{features}'";
	hint = "The amfeature in the reply packet is invalid";
    } else if (message->code == 2800208) {
	msg  = "The amfeature in the reply packet is invalid";
    } else if (message->code == 2800209) {
	msg  = "%{dle_hostname}";
    } else if (message->code == 2800210) {
	msg  = "%{ok_line}";
    } else if (message->code == 2800211) {
	msg  = "%{type}%{hostname}: %{errstr}";
    } else if (message->code == 2800212) {
	msg  = "%{hostname}: unknown response: %{errstr}";
    } else if (message->code == 2800213) {
	msg  = "Could not find security driver '%{auth}' for host '%{hostname}'. auth for this dle is invalid";
    } else if (message->code == 2800214) {
    } else if (message->code == 2800215) {
	msg  = "amanda.conf has dump user configured to '%{dumpuser}', but that user does not exist";
    } else if (message->code == 2800216) {
	msg  = "cannot get username for running user, uid %{uid} is not in your user database";
    } else if (message->code == 2800217) {
	msg  = "must be executed as the '%{expected_user}' user instead of the '%{running_user}' user";
	hint = "Change user to '%{expected_user}' or change dumpuser to '%{running_user}' in amanda.conf";
    } else if (message->code == 2800218) {
	msg  = "could not open temporary amcheck output file %{filename}: %{errnostr}";
	hint = "Check permissions";
    } else if (message->code == 2800219) {
	msg  = "could not open amcheck output file %{filename}: %{errnostr}";
	hint = "Check permissions";
    } else if (message->code == 2800220) {
	msg = "seek temp file: %{errnostr}";
    } else if (message->code == 2800221) {
	msg = "fseek main file: %{errnostr}";
    } else if (message->code == 2800222) {
	msg = "mailfd write: %{errnostr}";
    } else if (message->code == 2800223) {
	msg = "mailfd write: wrote %{size:write_size} instead of %{size:expected_size}";
    } else if (message->code == 2800224) {
	msg = "Can't fdopen: %{errnostr}";
    } else if (message->code == 2800225) {
	msg = "error running mailer %{mailer}: %{errmsg}";
    } else if (message->code == 2800226) {
	msg = "could not spawn a process for checking the server: %{errnostr}";
    } else if (message->code == 2800227) {
	msg = "nullfd: /dev/null: %{errnostr}";
    } else if (message->code == 2800228) {
	msg = "errors processing config file";
    } else if (message->code == 2800229) {
	msg = "Invalid mailto address '%{mailto}'";
    } else if (message->code == 2800230) {
	msg = "Can't open '%{filename}' for reading: %{errnostr}";
    } else if (message->code == 2800231) {
	msg = "Multiple DLE's for host '%{hostname}' use different auth methods";
	hint = "Please ensure that all DLE's for the host use the same auth method, including skipped ones";
    } else if (message->code == 2800232) {
	msg = "Multiple DLE's for host '%{hostname}' use different maxdumps values";
	hint = "Please ensure that all DLE's for the host use the same maxdumps value, including skipped ones";
    } else if (message->code == 2800233) {
	msg = "%{hostname} %{diskname}: The tag '%{tag}' match none of the storage dump_selection";
    } else if (message->code == 2800234) {
	msg = "%{hostname} %{diskname}: holdingdisk NEVER with tags matching more than one storage, will be dumped to only one storage";
    } else if (message->code == 2900000) {
	msg = "The Application '%{application}' failed: %{errmsg}";
    } else if (message->code == 2900001) {
	msg = "Can't execute application '%{application}'";
    } else if (message->code == 2900002) {
	msg = "The application '%{application}' does not support the '%{method}' method";
    } else if (message->code == 2900003) {
	msg = "%{service} only works with application";
    } else if (message->code == 2900004) {
	msg = "Missing OPTIONS line in %{service} request";
    } else if (message->code == 2900005) {
	msg = "Application '%{application}': can't create pipe";
    } else if (message->code == 2900006) {
	msg = "Can't dup2: %{errno} %{errnocode} %{errnostr}";
    } else if (message->code == 2900007) {
	msg = "%{service} require fe_req_xml";
    } else if (message->code == 2900008) {
	msg = "no valid %{service} request";
    } else if (message->code == 2900009) {
	msg = "no valid %{service} request";
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
    } else if (message->code == 2900018) {
	msg = "%{name}: %{errmsg}";

    } else if (message->code == 3100005) {
	msg = "senddiscover result";
    } else if (message->code == 3100006) {
	msg = "no senddiscover result to list";

    } else if (message->code == 3600000) {
	msg = "version '%{version}";
    } else if (message->code == 3600001) {
	msg = "distro %{distro}";
    } else if (message->code == 3600002) {
	msg = "platform %{platform}";
    } else if (message->code == 3600003) {
	msg = "Multiple OPTIONS line in selfcheck input";
    } else if (message->code == 3600004) {
	msg = "OPTIONS features=%{features};hostname=%{hostname};";
    } else if (message->code == 3600005) {
	msg = "%{errstr}";
    } else if (message->code == 3600006) {
	msg = "Missing OPTIONS line in selfcheck input";
    } else if (message->code == 3600007) {
	msg = "FORMAT ERROR IN REQUEST PACKET %{err_extra}";
    } else if (message->code == 3600008) {
	msg = "FORMAT ERROR IN REQUEST PACKET";
    } else if (message->code == 3600009) {
	msg = "samba support only one exclude file";
    } else if (message->code == 3600010) {
	msg = "samba does not support exclude list";
    } else if (message->code == 3600011) {
	msg = "samba does not support include file";
    } else if (message->code == 3600012) {
	msg = "samba does not support include list";
    } else if (message->code == 3600013) {
	msg = "DUMP does not support exclude file";
    } else if (message->code == 3600014) {
	msg = "DUMP does not support exclude list";
    } else if (message->code == 3600015) {
	msg = "DUMP does not support include file";
    } else if (message->code == 3600016) {
	msg = "DUMP does not support include list";
    } else if (message->code == 3600017) {
	msg = "client configured for auth=%{auth} while server requested '%{auth-requested}'";
    } else if (message->code == 3600018) {
	msg = "The auth in ~/.ssh/authorized_keys should be \"--auth=ssh\", or use another auth for the DLE";
    } else if (message->code == 3600019) {
	msg = "The auth in the inetd/xinetd configuration must be the same as the DLE";
    } else if (message->code == 3600020) {
	msg = "%{device}: Can't use CALCSIZE for samba estimate, use CLIENT";
    } else if (message->code == 3600021) {
	msg = "%{device}: cannot parse for share/subdir disk entry";
    } else if (message->code == 3600022) {
	msg = "%{device}: subdirectory specified but samba is not v2 or better";
    } else if (message->code == 3600023) {
	msg = "%{device}: cannot find password";
    } else if (message->code == 3600024) {
	msg = "%{device}: password field not 'user%%pass'";
    } else if (message->code == 3600025) {
	msg = "%{device}: cannot make share name";
    } else if (message->code == 3600026) {
	msg = "%{device}: Cannot access /dev/null: %{errnostr}";
    } else if (message->code == 3600027) {
	msg = "%{device}: password write failed: %{errnostr}";
    } else if (message->code == 3600028) {
	msg = "%{device}: Can't fdopen ferr: %{errnostr}";
    } else if (message->code == 3600029) {
	msg = "%{device}: samba access error:";
    } else if (message->code == 3600030) {
	msg = "%{device}: This client is not configured for samba";
    } else if (message->code == 3600031) {
	msg = "%{device}: The DUMP program cannot handle samba shares, use the amsamba application";
    } else if (message->code == 3600032) {
	msg = "%{device}: Application '%{application}': %{errstr}";
    } else if (message->code == 3600033) {
	msg = "%{device}: Application '%{application}': can't run 'support' command";
    } else if (message->code == 3600034) {
	msg = "%{device}: Application '%{application}': doesn't support amanda data-path";
    } else if (message->code == 3600035) {
	msg = "%{device}: Application '%{application}': doesn't support directtcp data-path";
    } else if (message->code == 3600036) {
	msg = "%{device}: Application '%{application}': doesn't support calcsize estimate";
    } else if (message->code == 3600037) {
	msg = "%{device}: Application '%{application}': doesn't support include-file";
    } else if (message->code == 3600038) {
	msg = "%{device}: Application '%{application}': doesn't support include-list";
    } else if (message->code == 3600039) {
	msg = "%{device}: Application '%{application}': doesn't support optional include";
    } else if (message->code == 3600040) {
	msg = "%{device}: Application '%{application}': doesn't support exclude-file";
    } else if (message->code == 3600041) {
	msg = "%{device}: Application '%{application}': doesn't support exclude-list";
    } else if (message->code == 3600042) {
	msg = "%{device}: Application '%{application}': doesn't support optional exclude";
    } else if (message->code == 3600043) {
	msg = "%{device}: Application '%{application}': can't create pipe: %{errnostr}";
    } else if (message->code == 3600044) {
	msg = "%{device}: Application '%{application}': fork failed: %{errnostr}";
    } else if (message->code == 3600045) {
	msg = "%{device}: Can't execute '%{cmd}': %{errnostr}";
    } else if (message->code == 3600046) {
	msg = "%{device}: Can't fdopen app_stderr: %{errnostr}";
    } else if (message->code == 3600047) {
	msg = "%{device}: Application '%{application}': %{errstr}";
    } else if (message->code == 3600048) {
	msg = "%{device}: waitpid failed: %{errnostr}";
    } else if (message->code == 3600049) {
	msg = "%{device}: Application '%{application}': exited with signal %{signal}";
    } else if (message->code == 3600050) {
	msg = "%{device}: Application '%{application}': exited with status %{exit_status}";
    } else if (message->code == 3600051) {
	msg = "%{hostname}: Could not %{type} %{disk} (%{device}): %{errnostr}";
    } else if (message->code == 3600052) {
	msg = "%{disk}";
    } else if (message->code == 3600053) {
	msg = "%{amdevice}";
    } else if (message->code == 3600054) {
	msg = "%{device}";
    } else if (message->code == 3600055) {
	msg = "%{device}: Can't fdopen app_stdout: %{errnostr}";
    } else if (message->code == 3600056) {
	msg = "%{ok_line}";
    } else if (message->code == 3600057) {
	msg = "%{error_line}";
    } else if (message->code == 3600058) {
	msg = "%{errstr}";
    } else if (message->code == 3600059) {
	msg = "%{filename} is not a file";
    } else if (message->code == 3600060) {
	msg = "can not stat '%{filename}': %{errnostr}";
    } else if (message->code == 3600061) {
	msg = "%{dirname} is not a directory";
    } else if (message->code == 3600062) {
	msg = "can not stat '%{dirname}': %{errnostr}";
    } else if (message->code == 3600063) {
	msg = "can not %{noun} '%{ilename}': %{errnostr} (ruid:%{ruid} euid:%{euid})";
    } else if (message->code == 3600064) {
	msg = "%{filename} %{adjective} (ruid:%{ruid} euid:%{euid})";
    } else if (message->code == 3600065) {
	msg = "%{filename} is not owned by root";
    } else if (message->code == 3600066) {
	msg = "%{filename} is not SUID root";
    } else if (message->code == 3600067) {
	msg = "can not stat '%{filename}': %{errnostr}";
    } else if (message->code == 3600068) {
	msg = "cannot get filesystem usage for '%{dirname}: %{errnostr}";
    } else if (message->code == 3600069) {
	msg = "dir '%{dirname}' needs %{size:required}, has nothing available";
    } else if (message->code == 3600070) {
	msg = "dir '%{dirname}' needs %{size:required}, only has %{size:avail} available";
    } else if (message->code == 3600071) {
	msg = "dir '%{dirname}' has more than %{size:avail} available";
    } else if (message->code == 3600072) {
	msg = "DUMP program not available";
    } else if (message->code == 3600073) {
	msg = "RESTORE program not available";
    } else if (message->code == 3600074) {
	msg = "VDUMP program not available";
    } else if (message->code == 3600075) {
	msg = "VRESTORE program not available";
    } else if (message->code == 3600076) {
	msg = "XFSDUMP program not available";
    } else if (message->code == 3600077) {
	msg = "XFSRESTORE program not available";
    } else if (message->code == 3600078) {
	msg = "VXDUMP program not available";
    } else if (message->code == 3600079) {
	msg = "VXRESTORE program not available";
    } else if (message->code == 3600080) {
	msg = "GNUTAR program not available";
    } else if (message->code == 3600081) {
	msg = "SMBCLIENT program not available";
    } else if (message->code == 3600082) {
	msg = "/etc/amandapass is world readable!";
    } else if (message->code == 3600083) {
	msg = "/etc/amandapass is readable, but not by all";
    } else if (message->code == 3600084) {
	msg = "unable to stat /etc/amandapass: %{errnostr}";
    } else if (message->code == 3600085) {
	msg = "unable to open /etc/amandapass: %{errnostr}";
    } else if (message->code == 3600086) {
	msg = "dump will not be able to create the /etc/dumpdates file: %{errnostr}";
    } else if (message->code == 3600087) {
	msg = "%{device}: samba access error: %{errmsg}";
    } else if (message->code == 3600088) {
	msg = "file/dir '%{filename}' is not owned by root";
    } else if (message->code == 3600089) {
	msg = "file/dir '%{filename}' is writable by everyone";
    } else if (message->code == 3600090) {
	msg = "file/dir '%{filename}' is writable by the group";
    } else if (message->code == 3700000) {
	msg = "%{disk}";
    } else if (message->code == 3700001) {
	msg = "amgtar version %{version}";
    } else if (message->code == 3700002) {
	msg = "amgtar gtar-version %{gtar-version}";
    } else if (message->code == 3700003) {
	msg = "Can't get %{gtar-path} version";
    } else if (message->code == 3700004) {
	msg = "amgtar";
    } else if (message->code == 3700005) {
	msg = "GNUTAR program not available";
    } else if (message->code == 3700006) {
	msg = "No GNUTAR-LISTDIR";
    } else if (message->code == 3700007) {
	msg = "bad ONE-FILE-SYSTEM property value '%{value}'";
    } else if (message->code == 3700008) {
	msg = "bad SPARSE property value '%{value}'";
    } else if (message->code == 3700009) {
	msg = "bad ATIME-PRESERVE property value '%{value}'";
    } else if (message->code == 3700010) {
	msg = "bad CHECK-DEVICE property value '%{value}'";
    } else if (message->code == 3700011) {
	msg = "bad NO-UNQUOTE property value '%{value}'";
    } else if (message->code == 3700012) {
	msg = "Can't open disk '%{diskname}': %{errnostr}";
    } else if (message->code == 3700013) {
	msg = "Cannot stat the disk '%{diskname}': %{errnostr}";
    } else if (message->code == 3700014) {
	msg = "Invalid '%{command-options}' COMMAND-OPTIONS";

    } else if (message->code == 4600000) {
	msg = "%{errmsg}";
    } else if (message->code == 4600001) {
	msg = "ERROR %{errmsg}";

    } else {
	msg = "no message for code '%{code}'";
    }

    result = fix_message_string(message, want_quoted, msg);
    if (want_quoted) {
	if (result) {
	    message->quoted_msg = g_string_free(result, FALSE);
	}
    } else {
	if (result) {
	    message->msg = g_string_free(result, FALSE);
	}
	result = fix_message_string(message, FALSE, hint);
	if (result) {
	    message->hint = g_string_free(result, FALSE);
	}
    }
}

static GString *
fix_message_string(
    message_t *message,
    gboolean want_quoted,
    char *msg)
{
    char *m;
    char  num[NUM_STR_SIZE];
    char  code[100];
    char *c;
    int   i;
    char *quoted;
    GString *result;

    if (!msg)
	return NULL;

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
		if (want_quoted) {
		    quoted = quote_string(message->file);
		    g_string_append(result, quoted);
		    g_free(quoted);
		} else {
		    g_string_append(result, message->file);
		}
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
		g_string_append(result, message->errnostr);
	    } else if (strcmp(code, "errnocode") == 0) {
		g_string_append(result, message->errnocode);
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
			assert(message->arg_array[i].value.type == MESSAGE_STRING);
			if (strcmp(format,"size") == 0) {
			    long long llvalue = atoll(message->arg_array[i].value.string);
			    g_string_append_printf(result, "%lld %sB", llvalue/getconf_unit_divisor(),
								       getconf_str(CNF_DISPLAYUNIT));
			} else {
			    g_string_append(result, "BAD-FORMAT");
			}
		    } else {
			if (message->arg_array[i].value.type == MESSAGE_NULL) {
			} else if (message->arg_array[i].value.type == MESSAGE_STRING) {
			    if (message->arg_array[i].value.string == NULL) {
				g_string_append(result, "null");
			    } else if (want_quoted) {
				quoted = quote_string(message->arg_array[i].value.string);
				g_string_append(result, quoted);
				g_free(quoted);
			    } else {
				g_string_append(result, message->arg_array[i].value.string);
			    }
			}
		    }
		} else {
		    g_string_append(result, "NONE");
		}
	    }
	} else {
	    g_string_append_c(result, *m);
	}
    }

    return result;
}

char *
get_message(
    message_t *message)
{
    return message->msg;
}

char *
get_quoted_message(
    message_t *message)
{
    if (!message->quoted_msg)
	set_message(message, 1);
    return message->quoted_msg;
}

char *
message_get_hint(
    message_t *message)
{
    return message->hint;
}

int
message_get_code(
    message_t *message)
{
    return message->code;
}

int
message_get_severity(
    message_t *message)
{
    return message->severity;
}

static void free_message_value_full(gpointer);
static void free_message_value(gpointer);
static void
free_message_value(
    gpointer pointer)
{
    json_value_t *value = pointer;

    if (value->type == MESSAGE_STRING) {
	g_free(value->string);
	value->string = NULL;
    } else if (value->type == MESSAGE_ARRAY) {
	guint i;
	for (i = 0; i < value->array->len; i++) {
	    free_message_value_full(g_ptr_array_index(value->array, i));
	}
	g_ptr_array_free(value->array, TRUE);
	value->array = NULL;
    } else if (value->type == MESSAGE_HASH) {
	g_hash_table_destroy(value->hash);
	value->hash = NULL;
    }
    value->type = MESSAGE_NULL;
}

static void
free_message_value_full(
    gpointer pointer)
{
    json_value_t *value = pointer;

    free_message_value(pointer);
    g_free(value);
}

void
delete_message(
    message_t *message)
{
    int i;

    if (message == NULL)
	return;

    g_free(message->file);
    g_free(message->msg);
    g_free(message->quoted_msg);
    g_free(message->errnostr);
    for (i = 0; message->arg_array[i].key != NULL; i++) {
	g_free(message->arg_array[i].key);
	free_message_value(&(message->arg_array[i].value));
    }
    g_free(message->process);
    g_free(message->running_on);
    g_free(message->component);
    g_free(message->module);
    g_free(message->arg_array);
    g_free(message);
}

void
delete_message_gpointer(
    gpointer data)
{
    delete_message((message_t *)data);
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
   int     i, j;

   message->file = g_strdup(file);
   message->line = line;
   message->process = g_strdup(get_pname());
   message->running_on = g_strdup(get_running_on());
   message->component = g_strdup(get_pcomponent());
   message->module = g_strdup(get_pmodule());
   message->code = code;
   message->severity = severity;
   message->argument_allocated = nb+1,
   message->arg_array = g_new0(message_arg_array_t, nb+2);

   va_start(marker, nb);     /* Initialize variable arguments. */
   for (i = 0,j = 0; i < nb; i++) {
	char *key = va_arg(marker, char *);
	if (strcmp(key,"errno") == 0) {
	    int m_errno = va_arg(marker, int);
	    message->merrno = m_errno;
	    message->errnocode = errcode[m_errno];
	    message->errnostr = g_strdup(strerror(m_errno));
	} else {
            message->arg_array[j].key = g_strdup(key);
	    message->arg_array[j].value.type = MESSAGE_STRING;
            message->arg_array[j].value.string = g_strdup(va_arg(marker, char *));
	    j++;
	}
   }
   message->arg_array[j].key = NULL;
   message->arg_array[j].value.type = MESSAGE_NULL;
   message->arg_array[j].value.string = NULL;
   va_end( marker );

   set_message(message, 0);

   g_debug("new message: %s:%d:%d:%d %s", message->file, message->line, message->severity, message->code, message->msg);
   return message;
}

static int message_indent = 4;
typedef struct message_hash_s {
    GString *r;
    int first;
} message_hash_t;

static void sprint_message_hash(gpointer key, gpointer value, gpointer user_data);
static char *sprint_message_value(json_value_t *value);

static void
sprint_message_hash(
    gpointer gkey,
    gpointer gvalue,
    gpointer user_data)
{
    char *key = gkey;
    json_value_t *value = gvalue;
    message_hash_t *mh = user_data;
    char *result_value = sprint_message_value((json_value_t *)value);

    if (!mh->first) {
	g_string_append(mh->r, ",\n");
    } else {
	mh->first = 0;
    }
    g_string_append_printf(mh->r,"%*c\"%s\" : %s", message_indent, ' ', (char *)key, result_value);
    g_free(result_value);
}

static char *
sprint_message_value(
    json_value_t *value)
{
    char *result = NULL;

    switch (value->type) {
    case MESSAGE_TRUE :
	result = g_strdup("true");
	break;
    case MESSAGE_FALSE :
	result = g_strdup("false");
	break;
    case MESSAGE_NULL :
	result = g_strdup("null");
	break;
    case MESSAGE_STRING :
	{
	    char *encoded = ammessage_encode_json(value->string);
	    result = g_strdup_printf("\"%s\"", encoded);
	    g_free(encoded);
	}
	break;
    case MESSAGE_HASH :
	if (g_hash_table_size(value->hash) == 0) {
	    result = g_strdup("{ }");
	} else {
	    GString *r = g_string_sized_new(512);
	    message_hash_t mh = {r, 1};
	    g_string_append(r, "{\n");
	    message_indent += 2;
	    g_hash_table_foreach(value->hash, sprint_message_hash, &mh);
	    message_indent -= 2;
	    g_string_append_printf(r, "\n%*c}", message_indent, ' ');
	    result = g_string_free(r, FALSE);
	}
	break;
    case MESSAGE_ARRAY :
	if (value->array->len == 0) {
	    result = g_strdup("[ ]");
	} else {
	    GString *r = g_string_sized_new(512);
	    guint i;
	    g_string_append(r, "[\n");
	    message_indent += 2;
	    for (i = 0; i < value->array->len; i++) {
		char *result_value = sprint_message_value(g_ptr_array_index(value->array, i));
		if (i>0) {
		    g_string_append(r, ",\n");
		}
		g_string_append_printf(r, "%*c", message_indent, ' ');
		g_string_append(r, result_value);
		g_free(result_value);
	    }
	    message_indent -= 2;
	    g_string_append_printf(r, "\n%*c]", message_indent, ' ');
	    result = g_string_free(r, FALSE);
	}
	break;
    }

    return result;
}

char *
sprint_message(
    message_t *message)
{
    int i;
    static int first_message = 1;
    GString *result;

    char *json_file;
    char *json_process;
    char *json_running_on;
    char *json_component;
    char *json_module;
    char *json_msg;

    if (message == NULL)
	return NULL;

    message_indent = 4;
    json_file = ammessage_encode_json(message->file);
    json_process = ammessage_encode_json(message->process);
    json_running_on = ammessage_encode_json(message->running_on);
    json_component = ammessage_encode_json(message->component);
    json_module = ammessage_encode_json(message->module);

    result = g_string_sized_new(512);
    if (first_message) {
	first_message = 0;
    } else {
	g_string_append_printf(result, ",\n");
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
        , json_file, message->line, severity_name(message->severity), json_process, json_running_on, json_component, json_module, message->code);
    for (i = 0; message->arg_array[i].key != NULL; i++) {
	char *json_key = ammessage_encode_json(message->arg_array[i].key);
	char *result_value = sprint_message_value(&message->arg_array[i].value);
	g_string_append_printf(result,
	    "    \"%s\" : %s,\n", json_key, result_value);
	g_free(json_key);
	g_free(result_value);
    }
    if (!message->msg) {
	set_message(message, 0);
    }
    json_msg = ammessage_encode_json(message->msg);
    g_string_append_printf(result,
        "    \"message\" : \"%s\"" \
        , json_msg);
    if (message->hint) {
	char *json_hint = ammessage_encode_json(message->hint);
	g_string_append_printf(result,
			",\n    \"hint\" : \"%s\"" \
		        , message->hint);
	g_free(json_hint);
    }
    g_string_append_printf(result,
	"\n  }");

    g_free(json_file);
    g_free(json_process);
    g_free(json_running_on);
    g_free(json_component);
    g_free(json_module);
    g_free(json_msg);

    return g_string_free(result, FALSE);
}

message_t *
print_message(
    message_t *message)
{
    char *msg;

    if (message == NULL)
	return NULL;

    msg = sprint_message(message);
    g_printf("%s", msg);
    g_free(msg);
    return message;
}

message_t *
fprint_message(
    FILE      *stream,
    message_t *message)
{
    char *msg;

    if (message == NULL)
	return NULL;

    msg = sprint_message(message);
    g_fprintf(stream, "%s", msg);
    g_free(msg);
    return message;
}

message_t *
fdprint_message(
    int       fd,
    message_t *message)
{
    char *msg;

    if (message == NULL)
	return NULL;

    msg = sprint_message(message);
    full_write(fd, msg, strlen(msg));
    g_free(msg);
    return message;
}

static char * parse_json_primitive( char *s, int  *i, int   len);
static char *
parse_json_primitive(
    char *s,
    int  *i,
    int   len G_GNUC_UNUSED)
{

    if (strncmp(&s[*i], "null", 4) == 0) {
	*i += 4;
	return NULL;
    } else if (strncmp(&s[*i], "true", 4) == 0) {
	*i += 4;
    } else if (strncmp(&s[*i], "false", 5) == 0) {
	*i += 5;
    }
    return s;
}

static char *json_parse_string(char *s, int *i, int len);
static char *
json_parse_string(
    char *s,
    int  *i,
    int   len)
{
    char *string = g_malloc(len);
    char *sp = string;

    (*i)++;
    for (; *i < len && s[*i] != '\0'; (*i)++) {
	char c = s[*i];

	if (c == '"') {
	    *sp = '\0';
	    return string;
	} else if (c == '\\') {
	    (*i)++;
	    c = s[*i];

	    switch (c) {
		case '"': case '/': case '\\':
		    *sp++ = c;
		    break;
		case 'b':
		case 'f': case 'r': case 'n': case 't':
		    *sp++ = '\\';
		    *sp++ = c;
		    break;
		case'u':
		    break;
		default:
		    break;
	    }
	} else {
	    *sp++ = c;
	}
    }
    g_free(string);
    return NULL;
}

static void parse_json_hash(char *s, int *i, GHashTable *hash);
static void parse_json_array(char *s, int *i, GPtrArray *array);
static void
parse_json_array(
    char *s,
    int  *i,
    GPtrArray *array)
{
    int len = strlen(s);
    char *token;

    (*i)++;
    for (; *i < len && s[*i] != '\0'; (*i)++) {
	char c =  s[*i];

	switch (c) {
	    case '[':
		{
		    json_value_t *value = g_new(json_value_t, 1);
		    value->type = MESSAGE_HASH;
		    value->array = g_ptr_array_sized_new(10);
		    g_ptr_array_add(array, value);
		    parse_json_hash(s, i, value->hash);
		}
		break;
	    case ']':
		return;
		break;
	    case '{':
		{
		    json_value_t *value = g_new(json_value_t, 1);
		    value->type = MESSAGE_HASH;
		    value->hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, free_message_value_full);
		    g_ptr_array_add(array, value);
		    parse_json_hash(s, i, value->hash);
		}
		break;
	    case '}':
		assert(array);
		return;
	    case '"':
		token = json_parse_string(s, i, len);
		assert(token);
		{
		    json_value_t *value = g_new(json_value_t, 1);
		    value->type = MESSAGE_STRING;
		    value->string = token;
		    g_ptr_array_add(array, value);
		}
		token = NULL;

		break;
	    case '\t':
	    case '\r':
	    case '\n':
	    case ':':
	    case ',':
	    case ' ':
		break;

	    default:
		token = parse_json_primitive(s, i, len);
		assert(token==NULL);
		if (token == NULL) {
		    json_value_t *value = g_new(json_value_t, 1);
		    value->type = MESSAGE_NULL;
		    value->string = NULL;
		    g_ptr_array_add(array, value);
		}
		token = NULL;
		break;
	}
    }
    s=s;

    return;
}

static void
parse_json_hash(
    char *s,
    int  *i,
    GHashTable *hash)
{
    int len = strlen(s);
    char *token;
    gboolean expect_key = TRUE;
    char *key = NULL;

    (*i)++;
    for (; *i < len && s[*i] != '\0'; (*i)++) {
	char c =  s[*i];

	switch (c) {
	    case '[':
		if (key) {
		    json_value_t *value = g_new(json_value_t, 1);
		    value->type = MESSAGE_ARRAY;
		    value->array = g_ptr_array_sized_new(10);
		    g_hash_table_insert(hash, key, value);
		    parse_json_array(s, i, value->array);
		    key = NULL;
		    expect_key = TRUE;
		}
		break;
	    case ']':
		break;
	    case '{':
		if (key) {
		    json_value_t *value = g_new(json_value_t, 1);
		    value->type = MESSAGE_HASH;
		    value->hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, free_message_value_full);
		    g_hash_table_insert(hash, key, value);
		    parse_json_hash(s, i, value->hash);
		    key = NULL;
		    expect_key = TRUE;
		} else {
		}
		break;
	    case '}':
		assert(hash);
		return;
	    case '"':
		token = json_parse_string(s, i, len);
		assert(token);
		if (expect_key) {
		    expect_key = FALSE;
		    key = token;
		} else {
		    json_value_t *value = g_new(json_value_t, 1);
		    expect_key = TRUE;
		    value->type = MESSAGE_STRING;
		    value->string = token;
		    g_hash_table_insert(hash, key, value);
		    key = NULL;
		    expect_key = TRUE;
		}
		token = NULL;

		break;
	    case '\t':
	    case '\r':
	    case '\n':
	    case ':':
	    case ',':
	    case ' ':
		break;

	    default:
		token = parse_json_primitive(s, i, len);
		assert(token==NULL);
		if (expect_key) {
		    expect_key = FALSE;
		    key = token;
		} else if (token == NULL) {
		    json_value_t *value = g_new(json_value_t, 1);
		    expect_key = TRUE;
		    value->type = MESSAGE_NULL;
		    value->string = NULL;
		    g_hash_table_insert(hash, key, value);
		    expect_key = TRUE;
		}
		token = NULL;
		break;
	}
    }
    s=s;

    return;
}

GPtrArray *
parse_json_message(
    char *s)
{
    int i;
    int len = strlen(s);
    GPtrArray *message_array = g_ptr_array_sized_new(100);
    char *token;
    message_t *message = NULL;
    gboolean expect_key = TRUE;
    char *key = NULL;
    int nb_arg = 0;

    for (i = 0; i < len && s[i] != '\0'; i++) {
	char c =  s[i];

	switch (c) {
	    case '[':
		if (message && key) {
		    message->arg_array[nb_arg].key = key;
		    message->arg_array[nb_arg].value.type = MESSAGE_ARRAY;
		    message->arg_array[nb_arg].value.array = g_ptr_array_sized_new(10);
		    nb_arg++;
		    if (nb_arg >= message->argument_allocated) {
			message->argument_allocated *=2;
			message->arg_array = g_realloc(message->arg_array, (message->argument_allocated+1) * sizeof(message_arg_array_t));
		    }
		    message->arg_array[nb_arg].key = NULL;
		    message->arg_array[nb_arg].value.type = MESSAGE_NULL;
		    message->arg_array[nb_arg].value.string = NULL;
		    parse_json_array(s, &i, message->arg_array[nb_arg-1].value.array);
		    key = NULL;
		    expect_key = TRUE;
		}
		break;
	    case ']':
		break;
	    case '{':
		if (message && key) {
		    message->arg_array[nb_arg].key = key;
		    message->arg_array[nb_arg].value.type = MESSAGE_HASH;
		    message->arg_array[nb_arg].value.hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, free_message_value_full);
		    nb_arg++;
		    if (nb_arg >= message->argument_allocated) {
			message->argument_allocated *=2;
			message->arg_array = g_realloc(message->arg_array, (message->argument_allocated+1) * sizeof(message_arg_array_t));
		    }
		    message->arg_array[nb_arg].key = NULL;
		    message->arg_array[nb_arg].value.type = MESSAGE_NULL;
		    message->arg_array[nb_arg].value.string = NULL;
		    parse_json_hash(s, &i, message->arg_array[nb_arg-1].value.hash);
		    key = NULL;
		    expect_key = TRUE;
		} else {
		    message = g_new0(message_t, 1);
		    nb_arg = 0;
		    message->argument_allocated = 10;
		    message->arg_array = g_new0(message_arg_array_t, message->argument_allocated+1);
		}
		break;
	    case '}':
		assert(message);
		if (message->merrno != 0 && message->errnocode == NULL) {
		    message->errnocode = errcode[message->merrno];
		}
		if (message->merrno != 0 && message->errnostr == NULL) {
		    message->errnostr = g_strdup(strerror(message->merrno));
		}
		g_ptr_array_add(message_array, message);
		message = NULL;
		break;
	    case '"':
		token = json_parse_string(s, &i, len);
		assert(token);
		assert(message);
		if (expect_key) {
		    expect_key = FALSE;
		    assert(key == NULL);
		    key = token;
		} else {
		    assert(key != NULL);
		    expect_key = TRUE;
		    if (strncmp_const(key, "source_filename") == 0) {
			g_free(key);
			key = NULL;
			message->file = token;
		    } else if (strncmp_const(key, "source_line") == 0) {
			g_free(key);
			key = NULL;
			message->line = atoi(token);
			g_free(token);
		    } else if (strncmp_const(key, "severity") == 0) {
			g_free(key);
			key = NULL;
			if (strncmp_const(token, "success") == 0) {
			    message->severity = MSG_SUCCESS;
			} else if (strncmp_const(token, "info") == 0) {
			    message->severity = MSG_INFO;
			} else if (strncmp_const(token, "message") == 0) {
			    message->severity = MSG_MESSAGE;
			} else if (strncmp_const(token, "warning") == 0) {
			    message->severity = MSG_WARNING;
			} else if (strncmp_const(token, "error") == 0) {
			    message->severity = MSG_ERROR;
			} else { /* critical or any other value */
			    message->severity = MSG_CRITICAL;
			}
			g_free(token);
		    } else if (strncmp_const(key, "code") == 0) {
			g_free(key);
			key = NULL;
			message->code = atoi(token);
			g_free(token);
		    } else if (strncmp_const(key, "message") == 0) {
			g_free(key);
			key = NULL;
			message->msg = token;
		    } else if (strncmp_const(key, "hint") == 0) {
			g_free(key);
			key = NULL;
			message->hint = token;
		    } else if (strncmp_const(key, "process") == 0) {
			g_free(key);
			key = NULL;
			message->process = token;
		    } else if (strncmp_const(key, "running_on") == 0) {
			g_free(key);
			key = NULL;
			message->running_on = token;
		    } else if (strncmp_const(key, "component") == 0) {
			g_free(key);
			key = NULL;
			message->component = token;
		    } else if (strncmp_const(key, "module") == 0) {
			g_free(key);
			key = NULL;
			message->module = token;
		    } else if (strncmp_const(key, "errno") == 0) {
			g_free(key);
			key = NULL;
			message->merrno = atoi(token);
			g_free(token);
		    } else if (strncmp_const(key, "errnocode") == 0) {
			g_free(key);
			key = NULL;
			message->errnocode = token;
		    } else if (strncmp_const(key, "errnostr") == 0) {
			g_free(key);
			key = NULL;
			message->errnostr = token;
		    } else {
			message->arg_array[nb_arg].key = key;
			key = NULL;
			message->arg_array[nb_arg].value.type = MESSAGE_STRING;
			message->arg_array[nb_arg].value.string = token;
			nb_arg++;
			if (nb_arg >= message->argument_allocated) {
			    message->argument_allocated *=2;
			    message->arg_array = g_realloc(message->arg_array, (message->argument_allocated+1) * sizeof(message_arg_array_t));
			}
			message->arg_array[nb_arg].key = NULL;
			message->arg_array[nb_arg].value.type = MESSAGE_NULL;
			message->arg_array[nb_arg].value.string = NULL;
		    }
		}
		token = NULL;

		break;
	    case '\t':
	    case '\r':
	    case '\n':
	    case ':':
	    case ',':
	    case ' ':
		break;

	    default:
		token = parse_json_primitive(s, &i, len);
		assert(token==NULL);
		if (expect_key) {
		    expect_key = FALSE;
		    key = g_strdup(token);
		} else if (token == NULL) {
		    message->arg_array[nb_arg].key = key;
		    message->arg_array[nb_arg].value.type = MESSAGE_NULL;
		    message->arg_array[nb_arg].value.string = NULL;
		    nb_arg++;
		    if (nb_arg >= message->argument_allocated) {
			message->argument_allocated *=2;
			message->arg_array = g_realloc(message->arg_array, (message->argument_allocated+1) * sizeof(message_arg_array_t));
		    }

		    expect_key = TRUE;
		}
		token = NULL;
		break;
	}
    }
    s=s;

    return message_array;
}

