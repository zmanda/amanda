/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#ifndef AMANDA_AMGLUE_H
#define AMANDA_AMGLUE_H

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <glib.h>
#include <glib-object.h>

/*
 * prototypes from gvalue.c
 */

/* Extract the value in 'value' and call sv_set*v(sv, ..)
 * appropriately for that type.  If the conversion cannot be 
 * completed, 'sv' will be set to undef, and a warning will be
 * issued (this will always be a programming error).
 *
 * @param value: a "set" GValue
 * @param sv: an SV which will be set according to 'value'.
 */
void amglue_set_sv_from_gvalue(GValue *value, SV *sv);

/* Extract the value in 'sv' and place it in the GValue. The
 * GValue *must* be initialized to the appropriate type --
 * this function will do its best to transform the Perl value
 * as necessary.
 *
 * @param sv: an SV which will be set according to 'value'.
 * @param value: a "set" GValue
 * @returns: TRUE if successful
 */
gboolean amglue_set_gvalue_from_sv(SV *sv, GValue *value);

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

#endif /* AMANDA_AMGLUE_H */
