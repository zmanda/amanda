/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2007-2015 Zmanda, Inc.  All Rights Reserved.
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

#include <amanda.h>
#include <amutil.h>
#include <amcrc32chw.h>

#if defined __GNUC__ && GCC_VERSION > 40300 && (defined __x86_64__ || defined __i386__ || defined __i486__ || defined __i586__ || defined __i686__)
#define POLY 0x82F63B78

/* Multiply a matrix times a vector over the Galois field of two elements,
 * GF(2).  Each element is a bit in an unsigned integer.  mat must have at
 * least as many entries as the power of two for most significant one bit in
 * vec. */
static inline uint32_t gf2_matrix_times(uint32_t *mat, uint32_t vec)
{
    uint32_t sum;

    sum = 0;
    while (vec) {
        if (vec & 1)
            sum ^= *mat;
        vec >>= 1;
        mat++;
    }
    return sum;
}

/* Multiply a matrix by itself over GF(2).  Both mat and square must have 32
 * rows. */
static inline void gf2_matrix_square(uint32_t *square, uint32_t *mat)
{
    int n;

    for (n = 0; n < 32; n++)
        square[n] = gf2_matrix_times(mat, mat[n]);
}

/* Construct an operator to apply len zeros to a crc.  len must be a power of
 * two.  If len is not a power of two, then the result is the same as for the
 * largest power of two less than len.  The result for len == 0 is the same as
 * for len == 1.  A version of this routine could be easily written for any
 * len, but that is not needed for this application. */
static void crc32c_zeros_op(uint32_t *even, size_t len)
{
    int n;
    uint32_t row;
    uint32_t odd[32];       /* odd-power-of-two zeros operator */

    /* put operator for one zero bit in odd */
    odd[0] = POLY;              /* CRC-32C polynomial */
    row = 1;
    for (n = 1; n < 32; n++) {
        odd[n] = row;
        row <<= 1;
    }

    /* put operator for two zero bits in even */
    gf2_matrix_square(even, odd);

    /* put operator for four zero bits in odd */
    gf2_matrix_square(odd, even);

    /* first square will put the operator for one zero byte (eight zero bits),
     * in even -- next square puts operator for two zero bytes in odd, and so
     * on, until len has been rotated down to zero */
    do {
        gf2_matrix_square(even, odd);
        len >>= 1;
        if (len == 0)
            return;
        gf2_matrix_square(odd, even);
        len >>= 1;
    } while (len);

    /* answer ended up in odd -- copy to even */
    for (n = 0; n < 32; n++)
        even[n] = odd[n];
}

/* Take a length and build four lookup tables for applying the zeros operator
 * for that length, byte-by-byte on the operand. */
static void crc32c_zeros(uint32_t zeros[][256], size_t len)
{
    uint32_t n;
    uint32_t op[32];

    crc32c_zeros_op(op, len);
    for (n = 0; n < 256; n++) {
        zeros[0][n] = gf2_matrix_times(op, n);
        zeros[1][n] = gf2_matrix_times(op, n << 8);
        zeros[2][n] = gf2_matrix_times(op, n << 16);
        zeros[3][n] = gf2_matrix_times(op, n << 24);
    }
}

/* Apply the zeros operator table to crc. */
static inline uint32_t crc32c_shift(uint32_t zeros[][256], uint32_t crc)
{
   uint32_t a=
    zeros[0][crc & 0xff] ^ zeros[1][(crc >> 8) & 0xff] ^
           zeros[2][(crc >> 16) & 0xff] ^ zeros[3][crc >> 24];
   return a;
}

/* Block sizes for three-way parallel crc computation.  LONG and SHORT must
 * both be powers of two.  The associated string constants must be set
 * accordingly, for use in constructing the assembler instructions. */
#define LONG 8192
#define SHORT 256
#define LOW 128

/* Tables for hardware crc that shift a crc by LONG and SHORT zeros. */
static uint32_t crc32c_long[4][256];
static uint32_t crc32c_short[4][256];
static uint32_t crc32c_low[4][256];

/* Initialize tables for shifting crcs. */
void crc32c_init_hw(void)
{
    crc32c_zeros(crc32c_long, LONG);
    crc32c_zeros(crc32c_short, SHORT);
    crc32c_zeros(crc32c_low, LOW);
}

typedef struct {
  union {
    const uint8_t  *b8;
    const uint32_t *b32;
    const uint64_t *b64;
  } b;
} multi_b;

/* Compute CRC-32C using the Intel hardware instruction. */
void crc32c_add_hw(uint8_t *buf, size_t len, crc_t *crc)
{

    multi_b next;
    multi_b end;
    uint32_t crc32_0;
#ifdef __x86_64__
    uint64_t *next64_1;
    uint64_t *next64_2;
    uint64_t *next64_3;
    uint64_t crc64_0, crc64_1, crc64_2, crc64_3; /* need to be 64 bits for crc32q */
#else
    uint32_t *next32_1;
    uint32_t *next32_2;
    uint32_t *next32_3;
    uint32_t crc32_1, crc32_2, crc32_3;
#endif

    next.b.b8 = buf;
    crc->size += len;
    crc32_0 = (uint64_t)crc->crc;
    /* pre-process the crc */
    /* compute the crc for up to seven leading bytes to bring the data pointer
     * to an eight-byte boundary */
    while (len && ((uintptr_t)next.b.b8 & 7) != 0) {
	crc32_0 = __builtin_ia32_crc32qi(crc32_0, *next.b.b8);
        next.b.b8++;
        len--;
    }

#ifdef __x86_64__
    /* compute the crc on sets of LONG*3 bytes, executing three independent crc
     * instructions, each on LONG bytes -- this is optimized for the Nehalem,
     * Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
     * throughput of one crc per cycle, but a latency of three cycles */
    crc64_0 = (uint64_t)crc32_0;
    while (len >= LONG*4) {
        crc64_1 = 0;
        crc64_2 = 0;
        crc64_3 = 0;
	next64_1 = (uint64_t *)(next.b.b64+LONG/8);
	next64_2 = (uint64_t *)(next.b.b64+(LONG/8)*2);
	next64_3 = (uint64_t *)(next.b.b64+(LONG/8)*3);
	end.b.b64 = next64_1;
        do {
	    crc64_0 = __builtin_ia32_crc32di(crc64_0, *next.b.b64++);
	    crc64_1 = __builtin_ia32_crc32di(crc64_1, *next64_1++);
	    crc64_2 = __builtin_ia32_crc32di(crc64_2, *next64_2++);
	    crc64_3 = __builtin_ia32_crc32di(crc64_3, *next64_3++);
        } while (next.b.b64 < end.b.b64);
        crc64_0 = crc32c_shift(crc32c_long, (uint32_t)crc64_0) ^ crc64_1;
        crc64_0 = crc32c_shift(crc32c_long, (uint32_t)crc64_0) ^ crc64_2;
        crc64_0 = crc32c_shift(crc32c_long, (uint32_t)crc64_0) ^ crc64_3;
        len -= LONG*4;
	next.b.b64 = next64_3;
    }

    /* do the same thing, but now on SHORT*3 blocks for the remaining data less
     * than a LONG*3 block */
    while (len >= SHORT*4) {
        crc64_1 = 0;
        crc64_2 = 0;
        crc64_3 = 0;
	next64_1 = (uint64_t *)(next.b.b64+SHORT/8);
	next64_2 = (uint64_t *)(next.b.b64+(SHORT/8)*2);
	next64_3 = (uint64_t *)(next.b.b64+(SHORT/8)*3);
        end.b.b64 = next64_1;
        do {
	    crc64_0 = __builtin_ia32_crc32di(crc64_0, *next.b.b64++);
	    crc64_1 = __builtin_ia32_crc32di(crc64_1, *next64_1++);
	    crc64_2 = __builtin_ia32_crc32di(crc64_2, *next64_2++);
	    crc64_3 = __builtin_ia32_crc32di(crc64_3, *next64_3++);
        } while (next.b.b64 < end.b.b64);
        crc64_0 = crc32c_shift(crc32c_short, crc64_0) ^ crc64_1;
        crc64_0 = crc32c_shift(crc32c_short, crc64_0) ^ crc64_2;
        crc64_0 = crc32c_shift(crc32c_short, crc64_0) ^ crc64_3;
        len -= SHORT*4;
	next.b.b64 = next64_3;
    }

    /* compute the crc on the remaining eight-byte units less than a SHORT*3
     * block */
    end.b.b8 = next.b.b8 + (len - (len & 7));
    while (next.b.b64 < end.b.b64) {
	crc64_0 = __builtin_ia32_crc32di(crc64_0, *next.b.b64++);
    }
    len &= 7;
    crc32_0 = (uint32_t)crc64_0;

#else
    /* compute the crc on sets of LONG*3 bytes, executing three independent crc
     * instructions, each on LONG bytes -- this is optimized for the Nehalem,
     * Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
     * throughput of one crc per cycle, but a latency of three cycles */
    while (len >= LONG*4) {
        crc32_1 = 0;
        crc32_2 = 0;
        crc32_3 = 0;
	next32_1 = (uint32_t *)(next.b.b32+LONG/4);
	next32_2 = (uint32_t *)(next.b.b32+(LONG/4)*2);
	next32_3 = (uint32_t *)(next.b.b32+(LONG/4)*3);
	end.b.b32 = next32_1;
        do {
	    crc32_0 = __builtin_ia32_crc32si(crc32_0, *next.b.b32++);
	    crc32_1 = __builtin_ia32_crc32si(crc32_1, *next32_1++);
	    crc32_2 = __builtin_ia32_crc32si(crc32_2, *next32_2++);
	    crc32_3 = __builtin_ia32_crc32si(crc32_3, *next32_3++);
        } while (next.b.b64 < end.b.b64);
        crc32_0 = crc32c_shift(crc32c_long, crc32_0) ^ crc32_1;
        crc32_0 = crc32c_shift(crc32c_long, crc32_0) ^ crc32_2;
        crc32_0 = crc32c_shift(crc32c_long, crc32_0) ^ crc32_3;
        len -= LONG*4;
	next.b.b32 = next32_2;
    }

    /* do the same thing, but now on SHORT*3 blocks for the remaining data less
     * than a LONG*3 block */
    while (len >= SHORT*4) {
        crc32_1 = 0;
        crc32_2 = 0;
        crc32_3 = 0;
	next32_1 = (uint32_t *)(next.b.b32+SHORT/4);
	next32_2 = (uint32_t *)(next.b.b32+(SHORT/4)*2);
	next32_3 = (uint32_t *)(next.b.b32+(SHORT/4)*3);
        end.b.b32 = next32_1;
        do {
	    crc32_0 = __builtin_ia32_crc32si(crc32_0, *next.b.b32++);
	    crc32_1 = __builtin_ia32_crc32si(crc32_1, *next32_1++);
	    crc32_2 = __builtin_ia32_crc32si(crc32_2, *next32_2++);
	    crc32_3 = __builtin_ia32_crc32si(crc32_3, *next32_3++);
        } while (next.b.b32 < end.b.b32);
        crc32_0 = crc32c_shift(crc32c_short, crc32_0) ^ crc32_1;
        crc32_0 = crc32c_shift(crc32c_short, crc32_0) ^ crc32_2;
        crc32_0 = crc32c_shift(crc32c_short, crc32_0) ^ crc32_3;
        len -= SHORT*4;
	next.b.b32 = next32_2;
    }

    /* compute the crc on the remaining eight-byte units less than a SHORT*3
     * block */
    end.b.b8 = next.b.b8 + (len - (len & 7));
    while (next.b.b32 < end.b.b32) {
	crc32_0 = __builtin_ia32_crc32si(crc32_0, *next.b.b32++);
    }
    len &= 7;
#endif

    /* compute the crc for up to seven trailing bytes */
    crc->crc = crc32_0;
    switch (len) {
        case 7:
            crc->crc = __builtin_ia32_crc32qi(crc->crc, *next.b.b8++);
        case 6:
            crc->crc = __builtin_ia32_crc32hi(crc->crc, *(uint16_t*) next.b.b8);
            next.b.b8 += 2;
        // case 5 is below: 4 + 1
        case 4:
            crc->crc = __builtin_ia32_crc32si(crc->crc, *(uint32_t*) next.b.b8);
            break;
        case 3:
            crc->crc = __builtin_ia32_crc32qi(crc->crc, *next.b.b8++);
        case 2:
            crc->crc = __builtin_ia32_crc32hi(crc->crc, *(uint16_t*) next.b.b8);
            break;
        case 5:
            crc->crc = __builtin_ia32_crc32si(crc->crc, *(uint32_t*) next.b.b8);
            next.b.b8 += 4;
        case 1:
            crc->crc = __builtin_ia32_crc32qi(crc->crc, *next.b.b8);
            break;
        case 0:
            break;
        default:
            // This should never happen; enable in debug code
            //assert(false);
	    break;
    }
}

#else

void crc32c_init_hw(void)
{
   g_error("crc32c_init_hw is not defined");
}

void crc32c_add_hw(uint8_t *buf, size_t len, crc_t *crc)
{
   g_error("crc32c_add_hw is not defined");
}

#endif
