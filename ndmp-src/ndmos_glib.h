/*
 * Copyright (c) 1998,1999,2000
 *	Traakan, Inc., Los Altos, CA
 *	All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Project:  NDMJOB
 * Ident:    $Id: $
 *
 * Description:
 *	This establishes the environment and options
 *	for the Glib "platform".   That is, it reflects all of the ndmjoblib
 *	abstractions into the corresponding glib abstractions.  This, combined
 *	with the O/S generic ndmos.h, are the foundation
 *	for NDMJOBLIB.
 *
 *	This file is #include'd by ndmos.h when
 *	selected by #ifdef's of NDMOS_ID.
 *
 *	Refer to ndmos.h for explanations of the
 *	macros thar are or can be #define'd here.
 */

#include "amanda.h"
#ifdef HAVE_RPC_RPC_H
#include <rpc/rpc.h>
#else
#error rpc/rpc.h is required to compile ndmp-src
#endif


/* this may need to be autodetected, or NDMOS_MACRO_SET_SOCKADDR may need to be
 * rewritten in terms of Amanda's sockaddr-util.h.  According to ndmpjoblib,
 * only FreeBDS has sin_len. */
#undef NDMOS_OPTION_HAVE_SIN_LEN

#define NDMOS_OPTION_TAPE_SIMULATOR
#define NDMOS_OPTION_ROBOT_SIMULATOR

#define NDMOS_OPTION_USE_SELECT_FOR_CHAN_POLL

#define NDMOS_API_BCOPY(S,D,N) g_memmove((void*)(D), (void*)(S), (N))
/* default: NDMOS_API_BZERO */
#define NDMOS_API_MALLOC(N) g_malloc((N))
#define NDMOS_API_FREE(P) g_free((void*)(P))
#define NDMOS_API_STRTOLL(P,PP,BASE) strtoll((P),(PP),(BASE))
#define NDMOS_API_STRDUP(S) g_strdup((S))
/* default: NDMOS_API_STREND (default: implemented in ndml_util.c) */

/* default: NDMOS_CONST_ALIGN */
/* default: NDMOS_CONST_EWOULDBLOCK */
/* default: NDMOS_CONST_TAPE_REC_MAX */
/* default: NDMOS_CONST_TAPE_REC_MIN */
/* default: NDMOS_CONST_PATH_MAX */
#define NDMOS_CONST_NDMOS_REVISION "Glib-2.2+"

#define NDMOS_MACRO_NEW(T) g_new(T, 1)
#define NDMOS_MACRO_NEWN(T,N) g_new(T, (N))
/* default: NDMOS_MACRO_ZEROFILL */
#define NDMOS_MACRO_SRAND() g_random_set_seed(time(0))
#define NDMOS_MACRO_RAND() g_random_int()
/* default: NDMOS_MACRO_OK_TAPE_REC_LEN */
#define NDMOS_MACRO_FREE(T) g_free(T)

/* extra */
#ifdef assert
#undef assert
#endif
#define assert g_assert
