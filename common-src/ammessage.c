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
    int   code;
    int   severity;
    char *msg;
    char *errnocode;
    char *errnostr;
    message_arg_array_t *arg_array;
};

static char *ammessage_encode_json(char *str);
static void set_message(message_t *message);

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

static void
set_message(
    message_t *message)
{
    char *msg;
    GString *result;
    char *m;
    char  num[NUM_STR_SIZE];
    char  code[100];
    char *c;
    int   i;

    init_errcode();
    if (message->code == 2900000) {
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
		g_snprintf(num, sizeof(num),
                "%d", message->severity);
		g_string_append(result, num);
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
		i = 0;
		while (message->arg_array[i].key != NULL &&
		       strcmp(message->arg_array[i].key, code) != 0) {
		    i++;
		}
		if (message->arg_array[i].key != NULL) {
		    g_string_append(result, message->arg_array[i].value);
		} else {
		    g_string_append(result, "NONE");
		}
	    }
	} else {
	    g_string_append_c(result, *m);
	}
    }
    message->msg = ammessage_encode_json(g_string_free(result, FALSE));
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
   message->code = code;
   message->severity = severity;
   message->arg_array = g_new0(message_arg_array_t, nb+1);

   va_start(marker, nb);     /* Initialize variable arguments. */
   for (i = 0; i < nb; i++) {
        message->arg_array[i].key = g_strdup(va_arg(marker, char *));
        message->arg_array[i].value = va_arg(marker, char *);
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
        "    \"severity\" : \"%d\",\n" \
        "    \"code\" : \"%d\",\n" \
        , message->file, message->line, message->severity, message->code);
    for (i = 0; message->arg_array[i].key != NULL; i++) {
	g_string_append_printf(result,
	"    \"%s\" : \"%s\",\n", message->arg_array[i].key, message->arg_array[i].value);
    }
    if (!message->msg) {
	set_message(message);
    }
    g_string_append_printf(result,
        "    \"message\" : \"%s\"\n" \
        "  }\n", message->msg);

    return g_string_free(result, FALSE);
}

void print_message(
    message_t *message)
{
    char *msg = sprint_message(message);
    g_printf("%s", msg);
    g_free(msg);
}

void fprint_message(
    FILE      *stream,
    message_t *message)
{
    char *msg = sprint_message(message);
    g_fprintf(stream, "%s", msg);
    g_free(msg);
}

void fdprint_message(
    int       fd,
    message_t *message)
{
    char *msg = sprint_message(message);
    full_write(fd, msg, strlen(msg));
    g_free(msg);
}

void print_message_free(
    message_t *message)
{
    print_message(message);
    delete_message(message);
}

void fprint_message_free(
    FILE      *stream,
    message_t *message)
{
    fprint_message(stream, message);
    delete_message(message);
}

void fdprint_message_free(
    int       fd,
    message_t *message)
{
    fdprint_message(fd, message);
    delete_message(message);
}

