/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
/*
 * $Id: versionsuffix.c,v 1.9 2006/05/25 01:47:11 johnfranks Exp $
 *
 * prints the (possibly empty) suffix appended to amanda program names
 */
#include "amanda.h"
#include "version.h"
#include "util.h"

int main(int argc, char **argv);

int
main(
    int		argc,
    char **	argv)
{
	(void)argc;	/* Quiet unused parameter warning */
	(void)argv;	/* Quiet unused parameter warning */

	/*
	 * Configure program for internationalization:
	 *   1) Only set the message locale for now.
	 *   2) Set textdomain for all amanda related programs to "amanda"
	 *      We don't want to be forced to support dozens of message catalogs.
	 */  
	setlocale(LC_MESSAGES, "C");
	textdomain("amanda"); 

	safe_fd(-1, 0);

	set_pname("versionsuffix");

	g_printf("%s\n", versionsuffix());
	return 0;
}
