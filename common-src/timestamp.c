/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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

#include "amanda.h"
#include "timestamp.h"
#include "conffile.h"
#include <glib.h>

/*
 * Construct a datestamp (YYYYMMDD) from a time_t.
 */
char * get_datestamp_from_time(time_t when) {
    struct tm *tm;
    
    if(when == (time_t)0) {
	when = time((time_t *)NULL);
    }
    
    tm = localtime(&when);
    return g_strdup_printf("%04d%02d%02d",
                           tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
}

/*
 * Construct a timestamp (YYYYMMDDHHMMSS) from a time_t.
 */
char * get_timestamp_from_time(time_t when) {
    struct tm *tm;

    if(when == (time_t)0) {
	when = time((time_t *)NULL);
    } 

    tm = localtime(&when);
    return g_strdup_printf("%04d%02d%02d%02d%02d%02d",
                           tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
                           tm->tm_hour, tm->tm_min, tm->tm_sec);
}

char * get_proper_stamp_from_time(time_t when) {
    /* note that this is reimplemented in perl in perl/Amanda/Util.swg */
    if (getconf_boolean(CNF_USETIMESTAMPS)) {
        return get_timestamp_from_time(when);
    } else {
        return get_datestamp_from_time(when);
    }
}

time_t get_time_from_timestamp(char *timestamp)
{
    struct tm tm;
    char t[5];
    time_t tt;
    tm.tm_year = 0;
    tm.tm_mon = 0;
    tm.tm_mday = 1;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;

    if (strlen(timestamp) >= 4) {
	memcpy(t, timestamp, 4);
	t[4]='\0';
	tm.tm_year = atoi(t) - 1900;
    }

    if (strlen(timestamp) >= 6) {
	memcpy(t, timestamp+4, 2);
	t[2]='\0';
	tm.tm_mon = atoi(t) - 1;
    }

    if (strlen(timestamp) >= 8) {
	memcpy(t, timestamp+6, 2);
	t[2]='\0';
	tm.tm_mday = atoi(t);
    }

    if (strlen(timestamp) >= 10) {
	memcpy(t, timestamp+8, 2);
	t[2]='\0';
	tm.tm_hour = atoi(t);
    }

    if (strlen(timestamp) >= 12) {
	memcpy(t, timestamp+10, 2);
	t[2]='\0';
	tm.tm_min = atoi(t);
    }

    if (strlen(timestamp) >= 14) {
	memcpy(t, timestamp+12, 2);
	t[2]='\0';
	tm.tm_sec = atoi(t);
    }
    tm.tm_wday = 0;
    tm.tm_yday = 0;
    tm.tm_isdst = -1;

    tt = mktime(&tm);

    return(tt);
}

time_state_t get_timestamp_state(char * timestamp) {
    if (timestamp == NULL || *timestamp == '\0') {
        return TIME_STATE_REPLACE;
    } else if (strcmp(timestamp, "X") == 0) {
        return TIME_STATE_UNDEF;
    } else {
        return TIME_STATE_SET;
    }
}

char * get_undef_timestamp(void) {
    return strdup("X");
}
