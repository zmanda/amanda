/*
 * Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as
 * published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "testutils.h"
#include "fileheader.h"

#define TAPE_HEADER(hdr) ((hdr)->type == F_TAPESTART || (hdr)->type == F_TAPEEND)

static int n_round_trips = 0;

/* actually test the round trip */
static int
try_rt(dumpfile_t *hdr)
{
    char *strval;
    size_t size;
    dumpfile_t hdr2;

    size = 0;
    strval = build_header(hdr, &size, 32768);
    g_assert(strval != NULL);
    fh_init(&hdr2);
    parse_file_header(strval, &hdr2, size);

    if (!headers_are_equal(hdr, &hdr2)) {
	tu_dbg("*** build_header of:\n");
	dump_dumpfile_t(hdr);
	tu_dbg("*** gives:\n");
	tu_dbg("%s", strval);
	tu_dbg("*** which becomes:\n");
	dump_dumpfile_t(&hdr2);
	return 0;
    }

    n_round_trips++;

    dumpfile_free_data(&hdr2);
    return 1;
#define PREV_RT try_rt
}

static int
rt_partnum(dumpfile_t *hdr)
{
    if (hdr->type == F_SPLIT_DUMPFILE) {
	hdr->partnum = 1;
	hdr->totalparts = 2;
	if (!PREV_RT(hdr)) return 0;
	hdr->partnum = 2;
	hdr->totalparts = 2;
	if (!PREV_RT(hdr)) return 0;
	hdr->partnum = 2;
	hdr->totalparts = -1;
	if (!PREV_RT(hdr)) return 0;
    } else if (hdr->type == F_DUMPFILE) {
	hdr->partnum = 1;
	hdr->totalparts = 1;
	if (!PREV_RT(hdr)) return 0;
    } else {
	hdr->partnum = 0;
	hdr->totalparts = 0;
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_partnum
}

static int
rt_compress(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	hdr->compressed = 0;
	strcpy(hdr->comp_suffix, "");
	if (!PREV_RT(hdr)) return 0;
    } else {
	hdr->compressed = 0;
	strcpy(hdr->comp_suffix, "");
	if (!PREV_RT(hdr)) return 0;
	hdr->compressed = 1;
	strcpy(hdr->comp_suffix, ".gz");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_compress
}

static int
rt_encrypt(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	hdr->encrypted = 0;
	strcpy(hdr->encrypt_suffix, "");
	if (!PREV_RT(hdr)) return 0;
    } else {
	hdr->encrypted = 0;
	strcpy(hdr->encrypt_suffix, "");
	if (!PREV_RT(hdr)) return 0;

	/* (note: Amanda seems to use 'enc' as the only suffix, and it doesn't
	 * really use it as a suffix; using .aes here ensures that the fileheader
	 * code can handle real suffixes, too */
	hdr->encrypted = 1;
	strcpy(hdr->encrypt_suffix, ".aes");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_encrypt
}

static int
rt_disk(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	strcpy(hdr->disk, "");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->disk, "/usr");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->disk, ""); /* should be quoted */
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_disk
}

static int
rt_partial(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	hdr->is_partial = 0;
	if (!PREV_RT(hdr)) return 0;
    } else {
	hdr->is_partial = 1;
	if (!PREV_RT(hdr)) return 0;
	hdr->is_partial = 0;
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_partial
}

static int
rt_application(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	strcpy(hdr->application, "");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->application, "MS-PAINT");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_application
}

static int
rt_dle_str(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	strcpy(hdr->dle_str, "");
	if (!PREV_RT(hdr)) return 0;
    } else {
	hdr->dle_str = g_strdup("");
	if (!PREV_RT(hdr)) return 0;
	hdr->dle_str = g_strdup("no-newline");
	if (!PREV_RT(hdr)) return 0;
	hdr->dle_str = g_strdup("ENDDLE\nmy dle\nENDDLE3");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_dle_str
}

static int
rt_program(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	strcpy(hdr->program, "");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->program, "CHECK");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_program
}

static int
rt_encrypt_prog(dumpfile_t *hdr)
{
    /* only do this for F_DUMPFILE, just to spare some repetitive testing */
    if (hdr->type == F_DUMPFILE) {
	strcpy(hdr->srv_encrypt, "");
	strcpy(hdr->clnt_encrypt, "");
	strcpy(hdr->srv_decrypt_opt, "");
	strcpy(hdr->clnt_decrypt_opt, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->srv_encrypt, "my-enc");
	strcpy(hdr->clnt_encrypt, "");
	strcpy(hdr->srv_decrypt_opt, "");
	strcpy(hdr->clnt_decrypt_opt, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->srv_encrypt, "my-enc");
	strcpy(hdr->clnt_encrypt, "");
	strcpy(hdr->srv_decrypt_opt, "-foo");
	strcpy(hdr->clnt_decrypt_opt, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->srv_encrypt, "");
	strcpy(hdr->clnt_encrypt, "its-enc");
	strcpy(hdr->srv_decrypt_opt, "");
	strcpy(hdr->clnt_decrypt_opt, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->srv_encrypt, "");
	strcpy(hdr->clnt_encrypt, "its-enc");
	strcpy(hdr->srv_decrypt_opt, "");
	strcpy(hdr->clnt_decrypt_opt, "-foo");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->srv_encrypt, "");
	strcpy(hdr->clnt_encrypt, "");
	strcpy(hdr->srv_decrypt_opt, "");
	strcpy(hdr->clnt_decrypt_opt, "");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_encrypt_prog
}

static int
rt_compprog(dumpfile_t *hdr)
{
    /* only do this for F_DUMPFILE, just to spare some repetitive testing */
    if (hdr->type == F_DUMPFILE) {
	strcpy(hdr->srvcompprog, "");
	strcpy(hdr->clntcompprog, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->srvcompprog, "my-comp-prog");
	strcpy(hdr->clntcompprog, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->srvcompprog, "");
	strcpy(hdr->clntcompprog, "its-comp-prog");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->srvcompprog, "");
	strcpy(hdr->clntcompprog, "");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_compprog
}

static int
rt_cmds(dumpfile_t *hdr)
{
    /* only do this for F_SPLIT_DUMPFILE, just to spare some repetitive testing */
    if (hdr->type == F_SPLIT_DUMPFILE) {
	strcpy(hdr->recover_cmd, "");
	strcpy(hdr->uncompress_cmd, "");
	strcpy(hdr->decrypt_cmd, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->recover_cmd, "get my data back");
	strcpy(hdr->uncompress_cmd, "");
	strcpy(hdr->decrypt_cmd, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->recover_cmd, "get my data back");
	strcpy(hdr->uncompress_cmd, "make my data bigger |");
	strcpy(hdr->decrypt_cmd, "");
	if (!PREV_RT(hdr)) return 0;
	strcpy(hdr->recover_cmd, "get my data back");
	strcpy(hdr->uncompress_cmd, "make my data bigger |");
	strcpy(hdr->decrypt_cmd, "unscramble it too |");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->recover_cmd, "");
	strcpy(hdr->uncompress_cmd, "");
	strcpy(hdr->decrypt_cmd, "");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_cmds
}

static int
rt_cont_filename(dumpfile_t *hdr)
{
    if (hdr->type == F_DUMPFILE || hdr->type == F_CONT_DUMPFILE) {
	strcpy(hdr->cont_filename, "/next/file");
	if (!PREV_RT(hdr)) return 0;
    } else {
	strcpy(hdr->cont_filename, "");
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_cont_filename
}

static int
rt_dumplevel(dumpfile_t *hdr)
{
    if (TAPE_HEADER(hdr)) {
	hdr->dumplevel = 0;
	if (!PREV_RT(hdr)) return 0;
    } else {
	hdr->dumplevel = 0;
	if (!PREV_RT(hdr)) return 0;
	hdr->dumplevel = 1;
	if (!PREV_RT(hdr)) return 0;
    }
    return 1;
#undef PREV_RT
#define PREV_RT rt_dumplevel
}

static int
rt_name(dumpfile_t *hdr)
{
    if (hdr->type == F_TAPEEND)
	strcpy(hdr->name, "");
    else if (TAPE_HEADER(hdr))
	strcpy(hdr->name, "TEST-LABEL");
    else 
	strcpy(hdr->name, "batcave-web");
    if (!PREV_RT(hdr)) return 0;
    return 1;
#undef PREV_RT
#define PREV_RT rt_name
}

static int
rt_type(dumpfile_t *hdr)
{
    hdr->type = F_DUMPFILE;
    if (!PREV_RT(hdr)) return 0;
    hdr->type = F_CONT_DUMPFILE;
    if (!PREV_RT(hdr)) return 0;
    hdr->type = F_SPLIT_DUMPFILE;
    if (!PREV_RT(hdr)) return 0;
    hdr->type = F_TAPESTART;
    if (!PREV_RT(hdr)) return 0;
    hdr->type = F_TAPEEND;
    if (!PREV_RT(hdr)) return 0;
    return 1;
#undef PREV_RT
#define PREV_RT rt_type
}

/* one function for each field; each fn calls the one above */
static gboolean
test_roundtrip(void)
{
    int rv;
    dumpfile_t hdr;
    fh_init(&hdr);

    /* set up some basic, constant values */
    strcpy(hdr.datestamp, "20100102030405");
    strcpy(hdr.name, "localhost");

    rv = PREV_RT(&hdr);
    tu_dbg("%d round-trips run\n", n_round_trips);
    return (rv) ? TRUE : FALSE;
}

/* doc

encrypted + encrypt_suffix (N special)
compressed + comp_suffix (N special)
blocksize not parsed
partnum/totalparts interaction
{srv,clnt}{compprog,_encrypt} pairs
uncompress/decrypt_cmd invalid without recover_cmd
uncompress/decrypt_cmd require trailing |
default to uncompress if only 2 cmds

*/

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
        TU_TEST(test_roundtrip, 90),
        TU_END()
    };
    return testutils_run_tests(argc, argv, tests);
}
