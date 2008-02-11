/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
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
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

%module "Amanda::Tests"
%include "amglue/amglue.swg"
%include "exception.i"

/* import dumptype_t, among others */
%import "Amanda/Types.swg";

%perlcode %{
=head1 NAME

Amanda::Tests -- test functions for installchecks

=head1 SYNOPSIS

This module exists only to provide functions for installcheck scripts to call,
mostly to test that various C-Perl interface techniques are working.

=cut
%}

%inline %{

/*
 * exercise bigint.c / integer.swg
 */

char *take_guint64(guint64 input) {
    if (input == G_MAXUINT64) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint64(gint64 input) {
    if (input == G_MAXINT64) return "MAX";
    if (input == G_MININT64) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_guint32(guint32 input) {
    if (input == G_MAXUINT32) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint32(gint32 input) {
    if (input == G_MAXINT32) return "MAX";
    if (input == G_MININT32) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_guint16(guint16 input) {
    if (input == G_MAXUINT16) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint16(gint16 input) {
    if (input == G_MAXINT16) return "MAX";
    if (input == G_MININT16) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_guint8(guint8 input) {
    if (input == G_MAXUINT8) return "MAX";
    if (input == 0) return "ZERO";
    return "OTHER";
}

char *take_gint8(gint8 input) {
    if (input == G_MAXINT8) return "MAX";
    if (input == G_MININT8) return "MIN";
    if (input == 0) return "ZERO";
    return "OTHER";
}


guint64 give_guint64(char *input) {
    if (input[0] == '+') return G_MAXUINT64;
    return 0;
}

gint64 give_gint64(char *input) {
    if (input[0] == '-') return G_MININT64;
    if (input[0] == '+') return G_MAXINT64;
    return 0;
}

guint32 give_guint32(char *input) {
    if (input[0] == '+') return G_MAXUINT32;
    return 0;
}

gint32 give_gint32(char *input) {
    if (input[0] == '-') return G_MININT32;
    if (input[0] == '+') return G_MAXINT32;
    return 0;
}

guint16 give_guint16(char *input) {
    if (input[0] == '+') return G_MAXUINT16;
    return 0;
}

gint16 give_gint16(char *input) {
    if (input[0] == '-') return G_MININT16;
    if (input[0] == '+') return G_MAXINT16;
    return 0;
}

guint8 give_guint8(char *input) {
    if (input[0] == '+') return G_MAXUINT8;
    return 0;
}

gint8 give_gint8(char *input) {
    if (input[0] == '-') return G_MININT8;
    if (input[0] == '+') return G_MAXINT8;
    return 0;
}

%}
