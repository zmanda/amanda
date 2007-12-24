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
 * Originally living in conffile.h, this stuff supports columnar output in amreport.
 */

#ifndef COLUMNAR_H
#define COLUMNAR_H

/* for each column we define some values on how to
 * format this column element
 */
typedef struct {
    char *Name;		/* column name */
    int PrefixSpace;	/* the blank space to print before this
   			 * column. It is used to get the space
			 * between the colums
			 */
    int Width;		/* the width of the column itself */
    int Precision;	/* the precision if its a float */
    int MaxWidth;	/* if set, Width will be recalculated
    			 * to the space needed */
    char *Format;	/* the printf format string for this
   			 * column element
			 */
    char *Title;	/* the title to use for this column */
} ColumnInfo;

extern ColumnInfo ColumnData[];

int ColumnDataCount(void);
int StringToColumn(char *s);
char LastChar(char *s);
int SetColumnDataFromString(ColumnInfo* ci, char *s, char **errstr); /* (sic) */

#endif /* COLUMNAR_H */
