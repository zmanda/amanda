/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * $Id: arglist.h,v 1.9 2006/06/16 11:33:43 martinea Exp $
 *
 * support macros for variable argument list declaration and definition
 */

#ifndef ARGLIST_H
#define ARGLIST_H

#ifdef STDC_HEADERS

#include <stdarg.h>

#define printf_arglist_function(fdecl, \
				hook_type, hook_name) \
        G_GNUC_PRINTF(1,0) \
        fdecl(hook_type hook_name, ...)

#define printf_arglist_function1(fdecl, \
				 arg1_type, arg1_name, \
				 hook_type, hook_name) \
        G_GNUC_PRINTF(2,0) \
	fdecl(arg1_type arg1_name, \
	      hook_type hook_name, ...)

#define printf_arglist_function2(fdecl, \
				 arg1_type, arg1_name, \
				 arg2_type, arg2_name, \
				 hook_type, hook_name) \
        G_GNUC_PRINTF(3,0) \
	fdecl(arg1_type arg1_name, \
	      arg2_type arg2_name, \
	      hook_type hook_name, ...)

#define printf_arglist_function3(fdecl, \
				 arg1_type, arg1_name, \
				 arg2_type, arg2_name, \
				 arg3_type, arg3_name, \
				 hook_type, hook_name) \
        G_GNUC_PRINTF(4,0) \
        fdecl(arg1_type arg1_name, \
	      arg2_type arg2_name, \
	      arg3_type arg3_name, \
	      hook_type hook_name, ...)

#define arglist_function(fdecl, \
			 hook_type, hook_name) \
        fdecl(hook_type hook_name, ...)

#define arglist_function1(fdecl, \
			  arg1_type, arg1_name, \
			  hook_type, hook_name) \
	fdecl(arg1_type arg1_name, \
	      hook_type hook_name, ...)

#define arglist_function2(fdecl, \
			  arg1_type, arg1_name, \
			  arg2_type, arg2_name, \
			  hook_type, hook_name) \
	fdecl(arg1_type arg1_name, \
	      arg2_type arg2_name, \
	      hook_type hook_name, ...)

#define arglist_function3(fdecl, arg1_type, arg1_name, arg2_type, arg2_name, \
			  arg3_type, arg3_name, hook_type, hook_name) \
	fdecl(arg1_type arg1_name, arg2_type arg2_name, \
	      arg3_type arg3_name, hook_type hook_name, ...)

#define arglist_start(arg,hook_name)	va_start(arg,hook_name)

#else

#include <varargs.h>

#define printf_arglist_function(fdecl, hook_type, hook_name) \
        fdecl(hook_name, va_alist)	\
        hook_type hook_name;		\
        va_dcl

#define printf_arglist_function1(fdecl, arg1_type, arg1_name, hook_type, hook_name) \
	fdecl(arg1_name, hook_name, va_alist)	\
	arg1_type arg1_name;			\
	hook_type hook_name;			\
	va_dcl

#define printf_arglist_function2(fdecl, arg1_type, arg1_name, arg2_type, arg2_name, hook_type, hook_name) \
	fdecl(arg1_name, arg2_name, hook_name, va_alist)	\
	arg1_type arg1_name;					\
	arg2_type arg2_name;					\
	hook_type hook_name;					\
	va_dcl

#define arglist_function(fdecl, hook_type, hook_name) \
        fdecl(hook_name, va_alist)	\
        hook_type hook_name;		\
        va_dcl

#define arglist_function1(fdecl, arg1_type, arg1_name, hook_type, hook_name) \
	fdecl(arg1_name, hook_name, va_alist)	\
	arg1_type arg1_name;			\
	hook_type hook_name;			\
	va_dcl

#define arglist_function2(fdecl, arg1_type, arg1_name, arg2_type, arg2_name, hook_type, hook_name) \
	fdecl(arg1_name, arg2_name, hook_name, va_alist)	\
	arg1_type arg1_name;					\
	arg2_type arg2_name;					\
	hook_type hook_name;					\
	va_dcl

#define arglist_function3(fdecl, arg1_type, arg1_name, arg2_type, arg2_name, \
			  arg3_type, arg3_name, hook_type, hook_name) \
	fdecl(arg1_name, arg2_name, arg3_name, hook_name, va_alist)	\
	arg1_type arg1_name;						\
	arg2_type arg2_name;						\
	arg3_type arg3_name;						\
	hook_type hook_name;						\
	va_dcl

#define arglist_start(arg,hook_name)	va_start(arg)

#endif

#define arglist_val(arg,type)	va_arg(arg,type)
#define arglist_end(arg)	va_end(arg)

#endif /* !ARGLIST_H */
