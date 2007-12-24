/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * Originally living in conffile.c, this stuff supports columnar output in amreport.
 */


#include "amanda.h"
#include "columnar.h"

ColumnInfo ColumnData[] = {
    { "HostName",   0, 12, 12, 0, "%-*.*s", "HOSTNAME" },
    { "Disk",       1, 11, 11, 0, "%-*.*s", "DISK" },
    { "Level",      1, 1,  1,  0, "%*.*d",  "L" },
    { "OrigKB",     1, 7,  0,  0, "%*.*lf", "ORIG-KB" },
    { "OutKB",      1, 7,  0,  0, "%*.*lf", "OUT-KB" },
    { "Compress",   1, 6,  1,  0, "%*.*lf", "COMP%" },
    { "DumpTime",   1, 7,  7,  0, "%*.*s",  "MMM:SS" },
    { "DumpRate",   1, 6,  1,  0, "%*.*lf", "KB/s" },
    { "TapeTime",   1, 6,  6,  0, "%*.*s",  "MMM:SS" },
    { "TapeRate",   1, 6,  1,  0, "%*.*lf", "KB/s" },
    { NULL,         0, 0,  0,  0, NULL,     NULL }
};


int
ColumnDataCount(void )
{
    return (int)(SIZEOF(ColumnData) / SIZEOF(ColumnData[0]));
}

/* conversion from string to table index
 */
int
StringToColumn(
    char *s)
{
    int cn;

    for (cn=0; ColumnData[cn].Name != NULL; cn++) {
    	if (strcasecmp(s, ColumnData[cn].Name) == 0) {
	    break;
	}
    }
    return cn;
}

char
LastChar(
    char *s)
{
    return s[strlen(s)-1];
}

int
SetColumnDataFromString(
    ColumnInfo* ci,
    char *s,
    char **errstr)
{
    ci = ci;

    /* Convert from a Columnspec string to our internal format
     * of columspec. The purpose is to provide this string
     * as configuration paramter in the amanda.conf file or
     * (maybe) as environment variable.
     * 
     * This text should go as comment into the sample amanda.conf
     *
     * The format for such a ColumnSpec string s is a ',' seperated
     * list of triples. Each triple consists of
     *   -the name of the column (as in ColumnData.Name)
     *   -prefix before the column
     *   -the width of the column
     *       if set to -1 it will be recalculated
     *	 to the maximum length of a line to print.
     * Example:
     * 	"Disk=1:17,HostName=1:10,OutKB=1:7"
     * or
     * 	"Disk=1:-1,HostName=1:10,OutKB=1:7"
     *	
     * You need only specify those colums that should be changed from
     * the default. If nothing is specified in the configfile, the
     * above compiled in values will be in effect, resulting in an
     * output as it was all the time.
     *							ElB, 1999-02-24.
     */

    while (s && *s) {
	int Space, Width;
	int cn;
    	char *eon= strchr(s, '=');

	if (eon == NULL) {
	    *errstr = stralloc2(_("invalid columnspec: "), s);
	    return -1;
	}
	*eon= '\0';
	cn=StringToColumn(s);
	if (ColumnData[cn].Name == NULL) {
	    *errstr = stralloc2(_("invalid column name: "), s);
	    return -1;
	}
	if (sscanf(eon+1, "%d:%d", &Space, &Width) != 2) {
	    *errstr = stralloc2(_("invalid format: "), eon + 1);
	    return -1;
	}
	ColumnData[cn].Width= Width;
	ColumnData[cn].PrefixSpace = Space;
	if (LastChar(ColumnData[cn].Format) == 's') {
	    if (Width < 0)
		ColumnData[cn].MaxWidth= 1;
	    else
		if (Width > ColumnData[cn].Precision)
		    ColumnData[cn].Precision= Width;
	}
	else {
	    if (Width < 0) {
		ColumnData[cn].MaxWidth= 1;
	    }
	    else if (Width < ColumnData[cn].Precision)
		ColumnData[cn].Precision = Width;
	}
	s= strchr(eon+1, ',');
	if (s != NULL)
	    s++;
    }
    return 0;
}

