/*
 * Copyright (c) Zmanda Inc.  All Rights Reserved.
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
#include "util.h"
#include "testutils.h"
#include "simpleprng.h"

static int test_encode(void);
static int test_decode(void);
static int test_roundtrip(void);
static int test_roundtrip_rand(void);

typedef struct {char *in; char *out;} enc_vec;
static int
test_encode(void)
{
    static const enc_vec test_strs[] = {
        {"hi", "hi"},
        {"hi!", "hi%21"},
        {"%", "%25"},
        {"*", "%2a"},
        {"\n", "%0a"},
        {"\nhi\n", "%0ahi%0a"}
    };
    static const int num = sizeof(test_strs)/sizeof(enc_vec);
    int i, ret;
    char *tmp;
    
    ret = TRUE;
    for (i = 0; i < num; i++) {
        tmp = hexencode_string(test_strs[i].in);
        if (!tmp || strcmp(test_strs[i].out, tmp)) {
            ret = FALSE;
            tu_dbg("encode failure:\n")
            tu_dbg("input:    \"%s\"\n", test_strs[i].in);
            tu_dbg("output:   \"%s\"\n", tmp? tmp : "(null)");
            tu_dbg("expected: \"%s\"\n", test_strs[i].out);
        }
        g_free(tmp);
    }
    return ret;
}

typedef struct {char *in; char *out; gboolean expect_err; } dec_vec;
static int
test_decode(void)
{
    static const dec_vec test_strs[] = {
        {"hi", "hi", FALSE},
        {"hi%21", "hi!", FALSE},
        {"%25", "%", FALSE},
        {"%2a", "*", FALSE},
        {"%2A", "*", FALSE},
        {"%0a", "\n", FALSE},
        {"%0A", "\n", FALSE},
        {"%0ahi%0a", "\nhi\n", FALSE},
        {"%", "", TRUE},
        {"%2", "", TRUE},
        {"h%", "", TRUE},
        {"%0h", "", TRUE},
        {"%h0", "", TRUE},
        {"%00", "", TRUE}
    };
    static const int num = sizeof(test_strs)/sizeof(dec_vec);
    int i, ret;
    char *tmp;
    GError *err = NULL;
    
    ret = TRUE;
    for (i = 0; i < num; i++) {
        tmp = hexdecode_string(test_strs[i].in, &err);
        if (!tmp || strcmp(test_strs[i].out, tmp) ||
            (!!err != test_strs[i].expect_err)) {
            ret = FALSE;
            tu_dbg("decode failure:\n")
            tu_dbg("input:     \"%s\"\n", test_strs[i].in);
            tu_dbg("output:    \"%s\"\n", tmp? tmp : "(null)");
            tu_dbg("expected:  \"%s\"\n", test_strs[i].out);
            tu_dbg("error msg: %s\n", err? err->message : "(none)");
        }
        g_clear_error(&err);
        g_free(tmp);
            
    }
    return ret;
}

typedef char* round_vec;
static int
test_roundtrip(void)
{
    static const round_vec test_strs[] = {
        "hi",
        "hi!",
        "hi%21",
        "%",
        "*",
        "\n",
        "h%"
    };
    static const int num = sizeof(test_strs)/sizeof(round_vec);
    int i, ret;
    char *tmp_enc = NULL, *tmp_dec = NULL;
    GError *err = NULL;
    
    ret = TRUE;
    for (i = 0; i < num; i++) {
        tmp_enc = hexencode_string(test_strs[i]);
        tmp_dec = tmp_enc? hexdecode_string(tmp_enc, &err) : NULL;
        if (!tmp_enc || !tmp_dec || strcmp(test_strs[i], tmp_dec) || err) {
            ret = FALSE;
            tu_dbg("roundtrip failure:\n")
            tu_dbg("input:      \"%s\"\n", test_strs[i]);
            tu_dbg("enc output: \"%s\"\n", tmp_enc? tmp_enc : "(null)");
            tu_dbg("dec output: \"%s\"\n", tmp_dec? tmp_dec : "(null)");
            tu_dbg("error msg:  %s\n", err? err->message : "(none)");
        }
        g_clear_error(&err);
        amfree(tmp_enc);
        amfree(tmp_dec);
    }
    return ret;
}

static int
test_roundtrip_rand(void)
{
    int i, ret;
    simpleprng_state_t state;
    char *in, *tmp_enc = NULL, *tmp_dec = NULL;
    size_t size;
    GError *err = NULL;
    
    simpleprng_seed(&state, 0xface);
    ret = TRUE;
    for (i = 0; i < 100; i++) {
        size = simpleprng_rand_byte(&state);
        in = g_malloc0(size+1);
        simpleprng_fill_buffer(&state, in, size);
        tmp_enc = hexencode_string(in);
        tmp_dec = tmp_enc? hexdecode_string(tmp_enc, &err) : NULL;
        if (!tmp_enc || !tmp_dec || strcmp(in, tmp_dec) || err) {
            ret = FALSE;
            tu_dbg("roundtrip failure:\n")
            tu_dbg("input:      \"%s\"\n", in);
            tu_dbg("enc output: \"%s\"\n", tmp_enc? tmp_enc : "(null)");
            tu_dbg("dec output: \"%s\"\n", tmp_dec? tmp_dec : "(null)");
            tu_dbg("error msg:  %s\n", err? err->message : "(none)");
        }
        g_clear_error(&err);
        amfree(tmp_enc);
        amfree(tmp_dec);
        g_free(in);
    }
    return ret;
}

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
        TU_TEST(test_encode, 90),
        TU_TEST(test_decode, 90),
        TU_TEST(test_roundtrip, 90),
        TU_TEST(test_roundtrip_rand, 90),
        TU_END()
    };
    return testutils_run_tests(argc, argv, tests);
}
