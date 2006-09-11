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
 * $Id: help.c,v 1.12 2006/05/25 01:47:14 johnfranks Exp $
 *
 * implements the "help" command in amrecover
 */

#include "amanda.h"
#include "amrecover.h"

/* print a list of valid commands */
void
help_list(void)
{
    printf("valid commands are:\n\n");

    printf("add path1 ...     - add to extraction list (shell wildcards)\n");
    printf("addx path1 ...    - add to extraction list (regular expressions)\n");
    printf("cd directory      - change cwd on virtual file system (shell wildcards)\n");
    printf("cdx directory     - change cwd on virtual file system (regular expressions)\n");
    printf("clear             - clear extraction list\n");
    printf("delete path1 ...  - delete from extraction list (shell wildcards)\n");
    printf("deletex path1 ... - delete from extraction list (regular expressions)\n");
    printf("extract           - extract selected files from tapes\n");
    printf("exit\n");
    printf("help\n");
    printf("history           - show dump history of disk\n");
    printf("list [filename]   - show extraction list, optionally writing to file\n");
    printf("lcd directory     - change cwd on local file system\n");
    printf("ls                - list directory on virtual file system\n");
    printf("lpwd              - show cwd on local file system\n");
    printf("mode              - show the method used to extract SMB shares\n");
    printf("pwd               - show cwd on virtual file system\n");
    printf("quit\n");
    printf("listhost          - list hosts\n");
    printf("listdisk [diskdevice]              - list disks\n");
    printf("setdate {YYYY-MM-DD|--MM-DD|---DD} - set date of look\n");
    printf("        {YYYY-MM-DD-HH-MM-SS}      - set date of look\n");
    printf("setdisk diskname [mountpoint]      - select disk on dump host\n");
    printf("sethost host                       - select dump host\n");
    printf("settape [host:][device|default]    - select tape server and/or device\n");
    printf("setmode smb|tar                 - select the method used to extract SMB shares\n");
    printf("\n");
}
