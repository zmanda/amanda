/*
 * Copyright (c) 2005 Zmanda Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
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
 * Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>, Ian Turner <ian@zmanda.com>
 */
/*
 * Utilities that aren't quite included in glib
 */

#include "amanda.h"
#include "glib-util.h"

void
_glib_util_foreach_glue(gpointer data, gpointer func)
{
    void (*one_arg_fn)(gpointer *) = func;
    one_arg_fn(data);
}

GValue* g_value_unset_init(GValue* value, GType type) {
    g_return_val_if_fail(value != NULL, NULL);

    if (G_IS_VALUE(value)) {
        g_value_unset(value);
    }
    g_value_init(value, type);
    return value;
}

GValue* g_value_unset_copy(const GValue * from, GValue * to) {
    g_return_val_if_fail(from != NULL, NULL);
    g_return_val_if_fail(to != NULL, NULL);

    g_value_unset_init(to, G_VALUE_TYPE(from));
    g_value_copy(from, to);
    return to;
}

void g_list_free_full(GList * list) {
    GList * cur = list;

    while (cur != NULL) {
        gpointer data = cur->data;
        amfree(data);
        cur = g_list_next(cur);
    }

    g_list_free(list);
}

void g_slist_free_full(GSList * list) {
    GSList * cur = list;

    while (cur != NULL) {
        gpointer data = cur->data;
        amfree(data);
        cur = g_slist_next(cur);
    }

    g_slist_free(list);
}

void g_queue_free_full(GQueue * queue) {
    while (!g_queue_is_empty(queue)) {
        gpointer data;
        data = g_queue_pop_head(queue);
        amfree(data);
    }
    g_queue_free(queue);
}

void g_ptr_array_free_full(GPtrArray * array) {
    size_t i;

    for (i = 0; i < array->len; i ++) {
        amfree(g_ptr_array_index(array, i));
    }
    g_ptr_array_free(array, TRUE);
}

gboolean g_value_compare(GValue * a, GValue * b) {
    if (a == NULL && b == NULL)
        return TRUE;
    if (a == NULL || b == NULL)
        return FALSE;
    if (G_VALUE_TYPE(a) != G_VALUE_TYPE(b))
        return FALSE;
    if (g_value_fits_pointer(a) && g_value_fits_pointer(b)) {
        return g_value_peek_pointer(a) == g_value_peek_pointer(b);
    } else {
        /* Since there is no builtin comparison function, we resort to
           comparing serialized strings. Yuck. */
        char * a_str;
        char * b_str;
        gboolean rval;
        a_str = g_strdup_value_contents(a);
        b_str = g_strdup_value_contents(b);
        rval = (0 == strcmp(a_str, b_str));
        amfree(a_str);
        amfree(b_str);
        return rval;
    }
    
    g_assert_not_reached();
}

static gboolean g_value_set_boolean_from_string(GValue * val, char * string) {
    if (strcasecmp(string, "true") == 0 ||
        strcasecmp(string, "yes") == 0 ||
        strcmp(string, "1") == 0) {
        g_value_set_boolean(val, TRUE);
    } else if (strcasecmp(string, "false") == 0 ||
               strcasecmp(string, "no") == 0 ||
               strcmp(string, "0") == 0) {
        g_value_set_boolean(val, FALSE);
    } else {
        return FALSE;
    }

    return TRUE;
}

static gboolean g_value_set_int_from_string(GValue * val, char * string) {
    long int strto_result;
    char * strto_end;
    strto_result = strtol(string, &strto_end, 0);
    if (*strto_end != '\0' || *string == '\0'
        || strto_result < INT_MIN
        || strto_result > INT_MAX) {
        return FALSE;
    } else { 
        g_value_set_int(val, (int)strto_result);
        return TRUE;
    }
}

static gboolean g_value_set_uint_from_string(GValue * val, char * string) {
    unsigned long int strto_result;
    char * strto_end;
    strto_result = strtoul(string, &strto_end, 0);
    if (*strto_end != '\0' || *string == '\0' || strto_result > UINT_MAX) {
        return FALSE;
    } else {
        g_value_set_uint(val, (guint)strto_result);
        return TRUE;
    }
}

static gboolean g_value_set_uint64_from_string(GValue * val, char * string) {
    unsigned long long int strto_result;
    char * strto_end;
    strto_result = strtoull(string, &strto_end, 0);
    if (*strto_end != '\0' || *string == '\0' || strto_result > G_MAXUINT64) {
        return FALSE;
    } else {
        g_value_set_uint(val, (guint)strto_result);
        return TRUE;
    }
}

/* Flags can contain multiple values. We assume here that values are like
 * C identifiers (that is, they match /[A-Za-z_][A-Za-z0-9_]+/), although
 * that doesn't seem to be a requirement of GLib. With that assumption in
 * mind, we look for the format "FLAG_1 | FLAG_2 | ... | FLAG_N". */
static gboolean g_value_set_flags_from_string(GValue * val, char * string) {
    guint value = 0;
    char * strtok_saveptr;
    char * string_copy;
    char * strtok_first_arg;
    const char delim[] = " \t,|";
    GFlagsClass * flags_class;
    
    flags_class = (GFlagsClass*) g_type_class_ref(G_VALUE_TYPE(val));
    g_return_val_if_fail(flags_class != NULL, FALSE);
    g_return_val_if_fail(G_IS_FLAGS_CLASS(flags_class), FALSE);

    /* Don't let strtok stop on original. */
    strtok_first_arg = string_copy = strdup(string);
    
    for (;;) {
        GFlagsValue * flag_value;
        char * token = strtok_r(strtok_first_arg, delim, &strtok_saveptr);
        strtok_first_arg = NULL;

        if (token == NULL) {
            break;
        }
        
        flag_value = g_flags_get_value_by_name(flags_class, token);
        if (flag_value == NULL) {
            flag_value = g_flags_get_value_by_nick(flags_class, token);
        }
        if (flag_value == NULL) {
            fprintf(stderr, _("Invalid flag %s for type %s\n"), token,
                    g_type_name(G_VALUE_TYPE(val)));
            continue;
        }

        value |= flag_value->value;
    }
    
    amfree(string_copy);
    
    if (value == 0) {
        fprintf(stderr, _("No valid flags for type %s in string %s\n"),
                g_type_name(G_VALUE_TYPE(val)), string);
        return FALSE;
    }
    
    g_value_set_flags(val, value);
    return TRUE;

}

/* This function really ought not to be part of Amanda. In my (Ian's) opinion,
   serialization and deserialization should be a part of the GValue
   interface. But it's not, and here we are. */
gboolean g_value_set_from_string(GValue * val, char * string) {
    g_return_val_if_fail(val != NULL, FALSE);
    g_return_val_if_fail(G_IS_VALUE(val), FALSE);

    if (G_VALUE_HOLDS_BOOLEAN(val)) {
        return g_value_set_boolean_from_string(val, string);
    } else if (G_VALUE_HOLDS_INT(val)) {
        return g_value_set_int_from_string(val, string);
    } else if (G_VALUE_HOLDS_UINT(val)) {
        return g_value_set_uint_from_string(val, string);
    } else if (G_VALUE_HOLDS_UINT64(val)) {
        return g_value_set_uint64_from_string(val, string);
    } else if (G_VALUE_HOLDS_STRING(val)) {
        g_value_set_string(val, string);
        return TRUE;
    } else if (G_VALUE_HOLDS_FLAGS(val)) {
        return g_value_set_flags_from_string(val, string);
    }

    return TRUE;
}

gint
g_compare_strings(
    gconstpointer a,
    gconstpointer b)
{
    return strcmp((char *)a, (char *)b);
}

