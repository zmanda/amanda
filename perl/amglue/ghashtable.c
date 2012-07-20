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

#include "amglue.h"
#include "conffile.h"

/* PERL_MAGIC_tied is not defined in perl 5.6 */
#if !defined PERL_MAGIC_tied
#define PERL_MAGIC_tied 'P'
#endif

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

static void 
foreach_fn_gslist(gpointer key_p, gpointer value_p, gpointer user_data_p)
{
    char   *key = key_p;
    GSList *value_s = value_p;
    GSList *value;
    HV     *hv = user_data_p;
    AV *list = newAV();

    for(value=value_s; value != NULL; value = value->next) {
	av_push(list, newSVpv(value->data, 0));
    }

    hv_store(hv, key, strlen(key), newRV_noinc((SV*)list), 0);
}

SV *
g_hash_table_to_hashref_gslist(GHashTable *hash)
{
    HV *hv = (HV *)sv_2mortal((SV *)newHV());

    g_hash_table_foreach(hash, foreach_fn_gslist, hv);

    return newRV((SV *)hv);
}

static void 
foreach_fn_property(gpointer key_p, gpointer value_p, gpointer user_data_p)
{
    char       *key = key_p;
    property_t *property = value_p;
    GSList     *value;
    HV         *hv = user_data_p;
    AV         *list = newAV();
    HV         *property_hv = newHV();
    SV         *val;

    hv_store(property_hv, "append", strlen("append"), newSViv(property->append), 0);
    hv_store(property_hv, "priority", strlen("priority"), newSViv(property->priority), 0);
    for(value=property->values; value != NULL; value = value->next) {
	av_push(list, newSVpv(value->data, 0));
    }
    hv_store(property_hv, "values", strlen("values"), newRV_noinc((SV*)list), 0);

    val = newRV_noinc((SV*)property_hv);
    hv_store(hv, key, strlen(key), val, 0);
    mg_set(val);
    SvREFCNT_dec(val);
}

SV *
g_hash_table_to_hashref_property(GHashTable *hash)
{
    HV *hv;
    HV *stash;
    SV *tie;

    hv = newHV();
    tie = newRV_noinc((SV*)newHV());
    stash = gv_stashpv("Amanda::Config::FoldingHash", GV_ADD);
    sv_bless(tie, stash);
    hv_magic(hv, (GV*)tie, PERL_MAGIC_tied);

    hv = (HV *)sv_2mortal((SV *)hv);
    g_hash_table_foreach(hash, foreach_fn_property, hv);

    return newRV((SV *)hv);
}

