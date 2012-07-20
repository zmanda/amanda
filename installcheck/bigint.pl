# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 74;
use strict;
use warnings;

use lib "@amperldir@";
use Amanda::Tests;
use Math::BigInt;
use Amanda::BigIntCompat;

# define some constants; Perl doesn't have native 64-bit numbers, so
# none are tested 
my $G_MAXUINT64_bigint = Math::BigInt->new('18446744073709551615');
my $G_MAXINT64_bigint = Math::BigInt->new('9223372036854775807');
my $G_MININT64_bigint = Math::BigInt->new('-9223372036854775808');

my $G_MAXUINT32_native = 2 ** 32 - 1;
my $G_MAXUINT32_double = 2.0 ** 32 - 1;
my $G_MAXUINT32_bigint = Math::BigInt->new('4294967295');
my $G_MAXINT32_native = 2 ** 31 - 1;
my $G_MAXINT32_double = 2.0 ** 31 - 1;
my $G_MAXINT32_bigint = Math::BigInt->new('2147483647');
my $G_MININT32_native = - 2 ** 31;
my $G_MININT32_double = - 2.0 ** 31;
my $G_MININT32_bigint = Math::BigInt->new('-2147483648');

my $G_MAXUINT16_native = 2 ** 16 - 1;
my $G_MAXUINT16_double = 2.0 ** 16 - 1;
my $G_MAXUINT16_bigint = Math::BigInt->new('65535');
my $G_MAXINT16_native = 2 ** 15 - 1;
my $G_MAXINT16_double = 2.0 ** 15 - 1;
my $G_MAXINT16_bigint = Math::BigInt->new('32767');
my $G_MININT16_native = - 2 ** 15;
my $G_MININT16_double = - 2.0 ** 15;
my $G_MININT16_bigint = Math::BigInt->new('-32768');

my $G_MAXUINT8_native = 2 ** 8 - 1;
my $G_MAXUINT8_double = 2.0 ** 8 - 1;
my $G_MAXUINT8_bigint = Math::BigInt->new('255');
my $G_MAXINT8_native = 2 ** 7 - 1;
my $G_MAXINT8_double = 2.0 ** 7 - 1;
my $G_MAXINT8_bigint = Math::BigInt->new('127');
my $G_MININT8_native = - 2 ** 7;
my $G_MININT8_double = - 2.0 ** 7;
my $G_MININT8_bigint = Math::BigInt->new('-128');

# first test "taking" integers -- Perl -> C

is(Amanda::Tests::take_guint64(0), "ZERO", 
    "Perl->C guint64 0");
is(Amanda::Tests::take_guint64($G_MAXUINT64_bigint), "MAX", 
    "Perl->C guint64 bigint MAX ($G_MAXUINT64_bigint)");
is(Amanda::Tests::take_gint64(0), "ZERO", 
    "Perl->C gint64 0");
is(Amanda::Tests::take_gint64($G_MAXINT64_bigint), "MAX", 
    "Perl->C gint64 bigint MAX ($G_MAXINT64_bigint)");
is(Amanda::Tests::take_gint64($G_MININT64_bigint), "MIN", 
    "Perl->C gint64 bigint MIN ($G_MININT64_bigint)");

is(Amanda::Tests::take_guint32(0), "ZERO", 
    "Perl->C guint32 0");
is(Amanda::Tests::take_guint32($G_MAXUINT32_bigint), "MAX", 
    "Perl->C guint32 bigint MAX ($G_MAXUINT32_bigint)");
is(Amanda::Tests::take_guint32($G_MAXUINT32_native), "MAX", 
    "Perl->C guint32 native MAX ($G_MAXUINT32_native)");
is(Amanda::Tests::take_guint32($G_MAXUINT32_double), "MAX", 
    "Perl->C guint32 double MAX ($G_MAXUINT32_double)");
is(Amanda::Tests::take_gint32(0), "ZERO", 
    "Perl->C gint32 0");
is(Amanda::Tests::take_gint32($G_MAXINT32_bigint), "MAX", 
    "Perl->C gint32 bigint MAX ($G_MAXINT32_bigint)");
is(Amanda::Tests::take_gint32($G_MAXINT32_native), "MAX", 
    "Perl->C gint32 native MAX ($G_MAXINT32_native)");
is(Amanda::Tests::take_gint32($G_MAXINT32_double), "MAX", 
    "Perl->C gint32 double MAX ($G_MAXINT32_double)");
is(Amanda::Tests::take_gint32($G_MININT32_bigint), "MIN", 
    "Perl->C gint32 bigint MIN ($G_MININT32_bigint)");
is(Amanda::Tests::take_gint32($G_MININT32_native), "MIN", 
    "Perl->C gint32 native MIN ($G_MININT32_native)");
is(Amanda::Tests::take_gint32($G_MININT32_double), "MIN", 
    "Perl->C gint32 double MIN ($G_MININT32_double)");

is(Amanda::Tests::take_guint16(0), "ZERO", 
    "Perl->C guint16 0");
is(Amanda::Tests::take_guint16($G_MAXUINT16_bigint), "MAX", 
    "Perl->C guint16 bigint MAX ($G_MAXUINT16_bigint)");
is(Amanda::Tests::take_guint16($G_MAXUINT16_native), "MAX", 
    "Perl->C guint16 native MAX ($G_MAXUINT16_native)");
is(Amanda::Tests::take_guint16($G_MAXUINT16_double), "MAX", 
    "Perl->C guint16 double MAX ($G_MAXUINT16_double)");
is(Amanda::Tests::take_gint16(0), "ZERO", 
    "Perl->C gint16 0");
is(Amanda::Tests::take_gint16($G_MAXINT16_bigint), "MAX", 
    "Perl->C gint16 bigint MAX ($G_MAXINT16_bigint)");
is(Amanda::Tests::take_gint16($G_MAXINT16_native), "MAX", 
    "Perl->C gint16 native MAX ($G_MAXINT16_native)");
is(Amanda::Tests::take_gint16($G_MAXINT16_double), "MAX", 
    "Perl->C gint16 double MAX ($G_MAXINT16_double)");
is(Amanda::Tests::take_gint16($G_MININT16_bigint), "MIN", 
    "Perl->C gint16 bigint MIN ($G_MININT16_bigint)");
is(Amanda::Tests::take_gint16($G_MININT16_native), "MIN", 
    "Perl->C gint16 native MIN ($G_MININT16_native)");
is(Amanda::Tests::take_gint16($G_MININT16_double), "MIN", 
    "Perl->C gint16 double MIN ($G_MININT16_double)");

is(Amanda::Tests::take_guint8(0), "ZERO", 
    "Perl->C guint8 0");
is(Amanda::Tests::take_guint8($G_MAXUINT8_bigint), "MAX", 
    "Perl->C guint8 bigint MAX ($G_MAXUINT8_bigint)");
is(Amanda::Tests::take_guint8($G_MAXUINT8_native), "MAX", 
    "Perl->C guint8 native MAX ($G_MAXUINT8_native)");
is(Amanda::Tests::take_guint8($G_MAXUINT8_double), "MAX", 
    "Perl->C guint8 double MAX ($G_MAXUINT8_double)");
is(Amanda::Tests::take_gint8(0), "ZERO", 
    "Perl->C gint8 0");
is(Amanda::Tests::take_gint8($G_MAXINT8_bigint), "MAX", 
    "Perl->C gint8 bigint MAX ($G_MAXINT8_bigint)");
is(Amanda::Tests::take_gint8($G_MAXINT8_native), "MAX", 
    "Perl->C gint8 native MAX ($G_MAXINT8_native)");
is(Amanda::Tests::take_gint8($G_MAXINT8_double), "MAX", 
    "Perl->C gint8 double MAX ($G_MAXINT8_double)");
is(Amanda::Tests::take_gint8($G_MININT8_bigint), "MIN", 
    "Perl->C gint8 bigint MIN ($G_MININT8_bigint)");
is(Amanda::Tests::take_gint8($G_MININT8_native), "MIN", 
    "Perl->C gint8 native MIN ($G_MININT8_native)");
is(Amanda::Tests::take_gint8($G_MININT8_double), "MIN", 
    "Perl->C gint8 double MIN ($G_MININT8_double)");

# now test giving integers -- C -> Perl

is(Amanda::Tests::give_guint64("0"), 0, "C -> Perl guint64 0");
is(Amanda::Tests::give_guint64("+"), $G_MAXUINT64_bigint, "C -> Perl guint64 MAX (always bigint)");
is(Amanda::Tests::give_gint64("0"), 0, "C -> Perl gint64 0");
is(Amanda::Tests::give_gint64("+"), $G_MAXINT64_bigint, "C -> Perl gint64 MAX (always bigint)");
is(Amanda::Tests::give_gint64("-"), $G_MININT64_bigint, "C -> Perl gint64 MIN (always bigint)");

is(Amanda::Tests::give_guint32("0"), 0, "C -> Perl guint32 0");
is(Amanda::Tests::give_guint32("+"), $G_MAXUINT32_bigint, "C -> Perl guint32 MAX (always bigint)");
is(Amanda::Tests::give_gint32("0"), 0, "C -> Perl gint32 0");
is(Amanda::Tests::give_gint32("+"), $G_MAXINT32_bigint, "C -> Perl gint32 MAX (always bigint)");
is(Amanda::Tests::give_gint32("-"), $G_MININT32_bigint, "C -> Perl gint32 MIN (always bigint)");

is(Amanda::Tests::give_guint16("0"), 0, "C -> Perl guint16 0");
is(Amanda::Tests::give_guint16("+"), $G_MAXUINT16_bigint, "C -> Perl guint16 MAX (always bigint)");
is(Amanda::Tests::give_gint16("0"), 0, "C -> Perl gint16 0");
is(Amanda::Tests::give_gint16("+"), $G_MAXINT16_bigint, "C -> Perl gint16 MAX (always bigint)");
is(Amanda::Tests::give_gint16("-"), $G_MININT16_bigint, "C -> Perl gint16 MIN (always bigint)");

is(Amanda::Tests::give_guint8("0"), 0, "C -> Perl guint8 0");
is(Amanda::Tests::give_guint8("+"), $G_MAXUINT8_bigint, "C -> Perl guint8 MAX (always bigint)");
is(Amanda::Tests::give_gint8("0"), 0, "C -> Perl gint8 0");
is(Amanda::Tests::give_gint8("+"), $G_MAXINT8_bigint, "C -> Perl gint8 MAX (always bigint)");
is(Amanda::Tests::give_gint8("-"), $G_MININT8_bigint, "C -> Perl gint8 MIN (always bigint)");

# finally, test overflows in Perl -> C conversions; these all croak(), so we capture the errors
# with an eval {}

eval { Amanda::Tests::take_gint64($G_MAXINT64_bigint+1); };
like($@, qr/Expected a signed 64-bit value or smaller/,
    "gint64 rejects numbers greater than max");
eval { Amanda::Tests::take_gint64($G_MININT64_bigint-1); };
like($@, qr/Expected a signed 64-bit value or smaller/,
    "gint64 rejects numbers less than min");
eval { Amanda::Tests::take_guint64($G_MAXUINT64_bigint+1); };
like($@, qr/Expected an unsigned 64-bit value or smaller/, 
    "guint64 rejects numbers greater than max");
eval { Amanda::Tests::take_guint64(-1); };
like($@, qr/Expected an unsigned value, got a negative integer/, 
    "guint64 rejects numbers less than zero");

eval { Amanda::Tests::take_gint32($G_MAXINT32_native+1); };
like($@, qr/Expected a 32-bit integer; value out of range/, 
    "gint32 rejects numbers greater than max");
eval { Amanda::Tests::take_gint32($G_MININT32_native-1); };
like($@, qr/Expected a 32-bit integer; value out of range/, 
    "gint32 rejects numbers less than min");
eval { Amanda::Tests::take_guint32($G_MAXUINT32_native+1); };
like($@, qr/Expected a 32-bit unsigned integer/, 
    "guint32 rejects numbers greater than max");
eval { Amanda::Tests::take_guint32(-1); };
like($@, qr/Expected an unsigned value, got a negative integer/, 
    "guint32 rejects numbers less than zero");

eval { Amanda::Tests::take_gint16($G_MAXINT16_native+1); };
like($@, qr/Expected a 16-bit integer; value out of range/, 
    "gint16 rejects numbers greater than max");
eval { Amanda::Tests::take_gint16($G_MININT16_native-1); };
like($@, qr/Expected a 16-bit integer; value out of range/, 
    "gint16 rejects numbers less than min");
eval { Amanda::Tests::take_guint16($G_MAXUINT16_native+1); };
like($@, qr/Expected a 16-bit unsigned integer/,
    "guint16 rejects numbers greater than max");
eval { Amanda::Tests::take_guint16(-1); };
like($@, qr/Expected an unsigned value, got a negative integer/, 
    "guint16 rejects numbers less than zero");

eval { Amanda::Tests::take_gint8($G_MAXINT8_native+1); };
like($@, qr/Expected a 8-bit integer; value out of range/, 
    "gint8 rejects numbers greater than max");
eval { Amanda::Tests::take_gint8($G_MININT8_native-1); };
like($@, qr/Expected a 8-bit integer; value out of range/, 
    "gint8 rejects numbers less than min");
eval { Amanda::Tests::take_guint8($G_MAXUINT8_native+1); };
like($@, qr/Expected a 8-bit unsigned integer/,
    "guint8 rejects numbers greater than max");
eval { Amanda::Tests::take_guint8(-1); };
like($@, qr/Expected an unsigned value, got a negative integer/, 
    "guint8 rejects numbers less than zero");
