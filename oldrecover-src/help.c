/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id: help.c,v 1.2 2006/05/25 01:47:13 johnfranks Exp $
 *
 * implements the "help" command in amrecover
 */

#include "amanda.h"
#include "amrecover.h"

/* print a list of valid commands */
void
help_list(void)
{
    g_printf("valid commands are:\n\n");

    g_printf("add path1 ...     - add to extraction list (shell wildcards)\n");
    g_printf("addx path1 ...    - add to extraction list (regular expressions)\n");
    g_printf("cd directory      - change cwd on virtual file system (shell wildcards)\n");
    g_printf("cdx directory     - change cwd on virtual file system (regular expressions)\n");
    g_printf("clear             - clear extraction list\n");
    g_printf("delete path1 ...  - delete from extraction list (shell wildcards)\n");
    g_printf("deletex path1 ... - delete from extraction list (regular expressions)\n");
    g_printf("extract           - extract selected files from tapes\n");
    g_printf("exit\n");
    g_printf("help\n");
    g_printf("history           - show dump history of disk\n");
    g_printf("list [filename]   - show extraction list, optionally writing to file\n");
    g_printf("lcd directory     - change cwd on local file system\n");
    g_printf("ls                - list directory on virtual file system\n");
    g_printf("lpwd              - show cwd on local file system\n");
    g_printf("mode              - show the method used to extract SMB shares\n");
    g_printf("pwd               - show cwd on virtual file system\n");
    g_printf("quit\n");
    g_printf("listhost          - list hosts\n");
    g_printf("listdisk [diskdevice]              - list disks\n");
    g_printf("setdate {YYYY-MM-DD|--MM-DD|---DD} - set date of look\n");
    g_printf("        {YYYY-MM-DD-HH-MM-SS}      - set date of look\n");
    g_printf("setdisk diskname [mountpoint]      - select disk on dump host\n");
    g_printf("sethost host                       - select dump host\n");
    g_printf("settape [host:][device|default]    - select tape server and/or device\n");
    g_printf("setmode smb|tar                 - select the method used to extract SMB shares\n");
    g_printf("\n");
}
