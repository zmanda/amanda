/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as
 * published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#ifndef AMANDA_AMGLUE_H
#define AMANDA_AMGLUE_H

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <glib.h>
#include <glib-object.h>

/* These defines are missing from older glibs, so we add them here */
#ifndef G_MAXINT8
#define G_MAXINT8 (127)
#endif

#ifndef G_MININT8
#define G_MININT8 (-127-1)
#endif

#ifndef G_MAXUINT8
#define G_MAXUINT8 (255)
#endif

#ifndef G_MAXINT16
#define G_MAXINT16 (32767)
#endif

#ifndef G_MININT16
#define G_MININT16 (-32767-1)
#endif

#ifndef G_MAXUINT16
#define G_MAXUINT16 (65535)
#endif

#ifndef G_MAXINT32
#define G_MAXINT32 (2147483647)
#endif

#ifndef G_MININT32
#define G_MININT32 (-2147483647-1)
#endif

#ifndef G_MAXUINT32
#define G_MAXUINT32 (4294967295U)
#endif

/*
 * prototypes for ghashtable.c
 */

/* Turn a GLib hash table (mapping strings to strings) into a reference
 * to a Perl hash table.
 *
 * @param hash: GLib hash table
 * @returns: Perl hashref
 */
SV *g_hash_table_to_hashref(GHashTable *hash);

/*
 * prototypes for bigint.c
 */

/*
 * These functions handle conversion of integers to and from Perl-compatible
 * values.  Most perls do not natively support 64-bit integers, so these functions
 * interface with the Math::BigInt module to support those integers.  The functions
 * also handle conversions from floating-point to integer values, with silent fraction
 * truncation, as perl automatically promotes integers to doubles on overflow.
 */

/* Convert an (unsigned) integer to a Perl SV.  These will always produce a 
 * Math::BigInt object.  Any failure is fatal.  *All* C-to-Perl integer conversions
 * must use these functions.
 *
 * @param v: value to convert
 * @returns: pointer to a new SV (refcount=1)
 */
SV *amglue_newSVi64(gint64 v);
SV *amglue_newSVu64(guint64 v);

/* Convert a Perl SV to an integer of the specified size.  These functions should
 * be used for *all* Perl-to-C integer conversions, since the Perl value may be a
 * Math::BigInt object.  All of these functions will call croak() on an overflow
 * condition, rather than silently truncate.
 *
 * @param sv: perl value to convert
 * @returns: value of the given type
 */
gint64 amglue_SvI64(SV *sv);
guint64 amglue_SvU64(SV *sv);
gint32 amglue_SvI32(SV *sv);
guint32 amglue_SvU32(SV *sv);
gint16 amglue_SvI16(SV *sv);
guint16 amglue_SvU16(SV *sv);
gint8 amglue_SvI8(SV *sv);
guint8 amglue_SvU8(SV *sv);

#endif /* AMANDA_AMGLUE_H */
