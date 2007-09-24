/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2005 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

/* 
 * getcwd.c --
 *
 *	This file provides an implementation of the getcwd procedure
 *	that uses getwd, for systems with getwd but without getcwd.
 */

/* $Id$ */

#include "amanda.h"

#include "getcwd.h"

#ifndef HAVE_GETCWD
#ifndef HAVE_GETWD
#error System does not support getcwd() or getwd()!
#else

/* getwd() but not getcwd() */

char * safe_getcwd(void) {
    char * buffer;
    char * getwd_result;

    buffer = malloc(PATH_MAX + 1);
    getwd_result = getwd(buffer);
    if (getwd_result == NULL) {
        free(buffer);
        return NULL;
    }

    return buffer;
}

#endif /* getwd() */
#else
/* We have getcwd(). */

#ifdef __USE_GNU
/* We are using the GNU library. Easy. */

char * safe_getcwd(void) {
    return getcwd(NULL, 0);
}

#else

/* Some library other than GNU. */

char * safe_getcwd(void) {
    size_t size = 100;
    char * buffer = NULL;
    
    for (;;) {
        buffer = realloc(buffer, size);

        if (getcwd (buffer, size) == buffer)
            return buffer;

        if (errno != ERANGE) {
            free (buffer);
            return NULL;
        }

        size *= 2;
    }

    g_assert_not_reached();
}

#endif /* __USE_GNU */
#endif /* getcwd() */
