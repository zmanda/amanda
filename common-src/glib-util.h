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
/*
 * Utilities that aren't quite included in glib
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>, Ian Turner <ian@zmanda.com>
 */

#ifndef GLIB_UTIL_H
#define GLIB_UTIL_H

#include <glib.h>
#include <glib-object.h>

/* Call the requisite glib init functions, including calling
 * g_init_types and setting up threading support.  This function can
 * be called multiple times with no harm, although it is not
 * re-entrant.
 */
void glib_init(void);

/* like g_[s]list_foreach, but with a function taking only
 * one argument.
 */
#define g_list_foreach_nodata(list, func)				\
    g_list_foreach((list), _glib_util_foreach_glue, (gpointer)(func));
#define g_slist_foreach_nodata(list, func)				\
    g_slist_foreach((list), _glib_util_foreach_glue, (gpointer)(func));
void _glib_util_foreach_glue(gpointer data, gpointer func);

/* This function takes a GValue, which may be zero-filled or
 * initialized. In either case, this function causes the GValue to be
 * initialized with the given type. Note that this function lacks the
 * safety of the standard g_value_ functions; it assumes that the
 * passed value is zeroed or valid.
 *
 * Returns its first argument.*/
GValue* g_value_unset_init(GValue* val, GType type);

/* This does the same thing but also copies the contents of one value
 * into another. Note that this function lacks the safety of the
 * standard g_value_ functions; it assumes that the passed value is
 * zeroed or valid.
 *
 * Returns its second (reset) argument.*/
GValue* g_value_unset_copy(const GValue* from, GValue * to);

/* This function is available in glib-2.28.0 and higher; for lower versions
 * we build our own version with a different name */
#if (GLIB_MAJOR_VERSION < 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION < 28))
void slist_free_full(GSList * list, GDestroyNotify free_fn);
#else
#define slist_free_full(list, free_fn) g_slist_free_full((list), (free_fn))
#endif

/* These functions all take a GLib container, and call free() on all the
 * pointers in the container before free()ing the container itself. */
void g_ptr_array_free_full(GPtrArray * array);

/* g_value_compare() does what you expect. It returns TRUE if and
   only if the two values have the same type and the same value. Note
   that it will return FALSE if the same value is stored with two
   different types: For example, a GValue with a UCHAR of 1 and a
   GValue with a CHAR of 1 will be considered inequal. Also, this is a
   'shallow' comparison; pointers to distinct but equivalent objects
   are considered inequal. */
gboolean g_value_compare(GValue * a, GValue * b);

/* Given a string and a GValue, parse the string and store it in the
   GValue. The GValue should be pre-initalized to whatever type you want
   parsed. */
gboolean g_value_set_from_string(GValue * val, char * string);

/* A GCompareFunc that will sort strings alphabetically (using strcmp) */
gint g_compare_strings(gconstpointer a, gconstpointer b);

/* These functions all take a Flags class and stringify it. They
 * return a NULL-terminated array of strings that can be
 * passed to g_strjoinv(), g_strfreev(), g_strdupv(), and
 * g_strv_length(). Example output looks like:
 * - g_flags_name_to_strv() -> "MEDIA_ACCESS_MODE_READ_ONLY"
 * - g_flags_short_name_to_strv() -> "READ_ONLY"
 * - g_flags_nick_to_strv() -> "read-only"
 */

char ** g_flags_name_to_strv(int value, GType type);
char ** g_flags_short_name_to_strv(int value, GType type);
char ** g_flags_nick_to_strv(int value, GType type);

/* Just like g_strjoinv, but frees the array as well. */
char * g_strjoinv_and_free(char ** strv, const char * seperator);

/* Just like g_strjoinv, but joins like an English list. The string would
 * usually be "and" or "or". */
char * g_english_strjoinv(char ** strv, const char * conjunction);

/* Just like g_english_strjoinv, but also frees the array. */
char * g_english_strjoinv_and_free(char ** strv, const char * conjunction);

/* Replacement for built-in functions. */
#if !(GLIB_CHECK_VERSION(2,6,0))
guint g_strv_length(gchar ** strv);
#endif

#if !GLIB_CHECK_VERSION(2,4,0)
void g_ptr_array_foreach (GPtrArray *array,
			  GFunc func,
                          gpointer user_data);
#endif

/* functions for g_hash_table_new to hash and compare case-insensitive strings */
guint g_str_amanda_hash(gconstpointer v);
gboolean g_str_amanda_equal(gconstpointer v1, gconstpointer v2);

#endif
