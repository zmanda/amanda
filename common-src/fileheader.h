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
/*
 * $Id: fileheader.h,v 1.16 2006/05/25 01:47:12 johnfranks Exp $
 *
 */

#ifndef FILEHEADER_H
#define FILEHEADER_H

#include "amanda.h"
#include "util.h"		/* for bstrncmp() */

#define STRMAX		256

typedef char string_t[STRMAX];
typedef enum {
    F_UNKNOWN, F_WEIRD, F_TAPESTART, F_TAPEEND, 
    F_DUMPFILE, F_CONT_DUMPFILE, F_SPLIT_DUMPFILE, F_EMPTY
} filetype_t;

typedef struct file_s {
    filetype_t type;
    string_t datestamp;
    int dumplevel;
    int compressed;
    int encrypted;
    string_t comp_suffix;
    string_t encrypt_suffix;
    string_t name;	/* hostname or label */
    string_t disk;
    string_t program;
    string_t srvcompprog;
    string_t clntcompprog;
    string_t srv_encrypt;
    string_t clnt_encrypt;
    string_t recover_cmd;
    string_t uncompress_cmd;
    string_t encrypt_cmd;
    string_t decrypt_cmd;
    string_t srv_decrypt_opt;
    string_t clnt_decrypt_opt;
    string_t cont_filename;
    int is_partial;
    int partnum;
    int totalparts; /* -1 == UNKNOWN */
    size_t blocksize;
} dumpfile_t;

/* local functions */

void	fh_init(dumpfile_t *file);
void	parse_file_header(const char *buffer, dumpfile_t *file, size_t buflen);
void	build_header(char *buffer, const dumpfile_t *file, size_t buflen);
void	print_header(FILE *outf, const dumpfile_t *file);
int	known_compress_type(const dumpfile_t *file);
void	dump_dumpfile_t(const dumpfile_t *file);

#endif /* !FILEHEADER_H */
