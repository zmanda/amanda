/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/*
 * This file contains SWIG macros to handle exports from perl modules using
 * the Exporter paackage;
 */

/* Initialization: SWIG sets up @EXPORT, but to be 'use strict'-compatible,
 * we declare @EXPORT_OK and %EXPORT_TAGS.
 */
%perlcode %{
@EXPORT_OK = ();
%EXPORT_TAGS = ();
%}

/* Mark SYMBOLS as exported by default (in @EXPORT)
 *
 * @param SYMBOLS: whitespace-separated list of symbols (used in qw())
 */
%define amglue_export(SYMBOLS)
%perlcode %{
push @EXPORT, qw(SYMBOLS);
%}
%enddef

/* Mark SYMBOLS as exported on request (in @EXPORT_OK)
 *
 * @param SYMBOLS: whitespace-separated list of symbols (used in qw())
 */
%define amglue_export_ok(SYMBOLS)
%perlcode %{
push @EXPORT_OK, qw(SYMBOLS);
%}
%enddef

/* Mark SYMBOLS as exported for tag TAG (in $EXPORT_TAGS{TAG}); also
 * adds SYMBOLS to EXPORT_OK.
 *
 * @param TAG: tag under which to export
 * @param SYMBOLS: whitespace-separated list of symbols (used in qw())
 */
%define amglue_export_tag(TAG, SYMBOLS)
%perlcode %{
push @EXPORT_OK, qw(SYMBOLS);
push @{$EXPORT_TAGS{`TAG`}}, qw(SYMBOLS);
%}
%enddef

/* Copy symbols in tag SRCTAG to tag DESTTAG; this is usually used to
 * include enums or flags into a categorical tag.
 *
 * @param SRCTAG: tag to copy from
 * @param DESTTAG: tag to copy to
 */
%define amglue_copy_to_tag(SRCTAG, DESTTAG)
%perlcode %{
# copy symbols in SRCTAG to DESTTAG
push @{$EXPORT_TAGS{`DESTTAG`}},  @{$EXPORT_TAGS{`SRCTAG`}};
%}
%enddef
