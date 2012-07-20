/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%{
#include "amglue.h"
%}

/*
 * perl -> C (input)
 */

%typemap(in) guint64 {
    $1 = amglue_SvU64($input);
}

%typemap(in) gint64 {
    $1 = amglue_SvI64($input);
}

%typemap(in) guint32 {
    $1 = amglue_SvU32($input);
}

%typemap(in) gint32 {
    $1 = amglue_SvI32($input);
}

%typemap(in) guint16 {
    $1 = amglue_SvU16($input);
}

%typemap(in) gint16 {
    $1 = amglue_SvI16($input);
}

%typemap(in) guint8 {
    $1 = amglue_SvU8($input);
}

%typemap(in) gint8 {
    $1 = amglue_SvI8($input);
}

/* remaining types depend on the complier to optimize out constant sizeof() 
 * expressions.  The SWIG preprocessor can't perform calculations based on 
 * sizeof(). */

%define typemap_in_unsigned(type)
%typemap(in) type {
    if (sizeof(type) == 1) {
	$1 = amglue_SvU8($input);
    } else if (sizeof(type) == 2) {
	$1 = amglue_SvU16($input);
    } else if (sizeof(type) == 4) {
	$1 = amglue_SvU32($input);
    } else if (sizeof(type) == 8) {
	$1 = amglue_SvU64($input);
    } else {
	croak("Unexpected type >64 bits?"); /* should be optimized out unless sizeof(type) > 8 */
    }
}
%enddef

typemap_in_unsigned(unsigned int)
typemap_in_unsigned(guint)
typemap_in_unsigned(unsigned short)
typemap_in_unsigned(gushort)
typemap_in_unsigned(size_t)
typemap_in_unsigned(gsize)
typemap_in_unsigned(unsigned long long)
typemap_in_unsigned(unsigned long)
typemap_in_unsigned(gulong)
typemap_in_unsigned(off_t)
typemap_in_unsigned(ptrdiff_t)
typemap_in_unsigned(uintmax_t)

%define typemap_in_signed(type)
%typemap(in) type {
    if (sizeof(type) == 1) {
	$1 = amglue_SvI8($input);
    } else if (sizeof(type) == 2) {
	$1 = amglue_SvI16($input);
    } else if (sizeof(type) == 4) {
	$1 = amglue_SvI32($input);
    } else if (sizeof(type) == 8) {
	$1 = amglue_SvI64($input);
    } else {
	g_critical("Unexpected type >64 bits?"); /* should be optimized out unless sizeof(type) > 8 */
    }
}
%enddef

typemap_in_signed(int)
typemap_in_signed(gint)
typemap_in_signed(signed int)
typemap_in_signed(short)
typemap_in_signed(gshort)
typemap_in_signed(signed short)
typemap_in_signed(ssize_t)
typemap_in_signed(gssize)
typemap_in_signed(long long)
typemap_in_signed(signed long long)
typemap_in_signed(long)
typemap_in_signed(signed long)
typemap_in_signed(glong)
typemap_in_signed(intmax_t)

/*
 * C -> perl (output)
 */

/* All conversions from C to Perl create Math::BigInt objects, even when the
 * C datatype is 32 bits or smaller.  This is to ensure that Perl's automatic
 * promotion to double does not silently corrupt arithmetic on large numbers.
 *
 * The complex typemaps here are to ensure that the argument stack is protected
 * against stomping by amglue_newSV*64, which may invoke a significant amount
 * of perl code.  "SP += argvi; PUTBACK;" increments the global stack pointer
 * to cover the arguments processed so far, while "SPAGAIN; SP -= argvi;"
 * restores the local stack pointer.  The latter must be done before the newest
 * argument is added to the stack.  This whole process is a hack around SWIG's
 * habit of invoking (out) typemaps while building the stack, instead of doing
 * so in advance.
 */

/* (these all use newSV*64, relying on C to upcast to a 64-bit integer) */

%define typemap_out_unsigned(type)
%typemap(out) type {
    SV *for_stack;
    SP += argvi; PUTBACK;
    for_stack = sv_2mortal(amglue_newSVu64($1));
    SPAGAIN; SP -= argvi;
    $result = for_stack;
    argvi++;
}
%enddef

typemap_out_unsigned(guint64)
typemap_out_unsigned(guint32)
typemap_out_unsigned(guint16)
typemap_out_unsigned(guint8)
typemap_out_unsigned(unsigned int)
typemap_out_unsigned(guint)
typemap_out_unsigned(unsigned short)
typemap_out_unsigned(gushort)
typemap_out_unsigned(size_t)
typemap_out_unsigned(gsize)
typemap_out_unsigned(unsigned long long)
typemap_out_unsigned(unsigned long)
typemap_out_unsigned(gulong)
typemap_out_unsigned(off_t)
typemap_out_unsigned(ptrdiff_t)
typemap_out_unsigned(uintmax_t)

%define typemap_out_signed(type)
%typemap(out) type {
    SV *for_stack;
    SP += argvi; PUTBACK;
    for_stack = sv_2mortal(amglue_newSVi64($1));
    SPAGAIN; SP -= argvi;
    $result = for_stack;
    argvi++;
}
%enddef

typemap_out_signed(gint64)
typemap_out_signed(gint32)
typemap_out_signed(gint16)
typemap_out_signed(gint8)
typemap_out_signed(int)
typemap_out_signed(gint)
typemap_out_signed(signed int)
typemap_out_signed(short)
typemap_out_signed(gshort)
typemap_out_signed(signed short)
typemap_out_signed(ssize_t)
typemap_out_signed(gssize)
typemap_out_signed(long long)
typemap_out_signed(signed long long)
typemap_out_signed(long)
typemap_out_signed(signed long)
typemap_out_signed(glong)
typemap_out_signed(intmax_t)
