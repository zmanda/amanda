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

#include "amglue.h"

static void 
foreach_fn(gpointer key_p, gpointer value_p, gpointer user_data_p)
{
    char *key = key_p;
    char *value = value_p;
    HV *hv = user_data_p;
    hv_store(hv, key, strlen(key), newSVpv(value, 0), 0);
}

SV *
g_hash_table_to_hashref(GHashTable *hash)
{
    HV *hv = (HV *)sv_2mortal((SV *)newHV());

    g_hash_table_foreach(hash, foreach_fn, hv);

    return newRV((SV *)hv);
}
