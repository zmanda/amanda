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

%module "Amanda::Types"
%include "amglue/amglue.swg"
%include "exception.i"

%{
#include "fileheader.h"
%}

%perlcode %{
=head1 NAME

Amanda::Types - Amanda data structures that are shared by several modules

=head1 SYNOPSIS

This module includes several types which are not specific to any
single other module.

=head1 API STATUS

New structures may be added, but existing types are stable.

=head1 dumpfile_t

An in-memory representation of an Amanda header, with keys

=over

=item C<type>;

=item C<datestamp>;

=item C<dumplevel>;

=item C<compressed>;

=item C<encrypted>;

=item C<comp_suffix>;

=item C<encrypt_suffix>;

=item C<name> -- hostname or label;

=item C<disk>;

=item C<program>;

=item C<application>;

=item C<srvcompprog>;

=item C<clntcompprog>;

=item C<srv_encrypt>;

=item C<clnt_encrypt>;

=item C<recover_cmd>;

=item C<uncompress_cmd>;

=item C<encrypt_cmd>;

=item C<decrypt_cmd>;

=item C<srv_decrypt_opt>;

=item C<clnt_decrypt_opt>;

=item C<cont_filename>;

=itme C<dle_str>;

=item C<is_partial>;

=item C<partnum>;

=item C<totalparts> (-1 == UNKNOWN); and

=item C<blocksize>.

=back

where C<type> is one of the following constants, which are availble
for import in the tag C<:filetype_t>:

=over

=item C<F_UNKNOWN>;

=item C<F_WEIRD>;

=item C<F_TAPESTART>;

=item C<F_TAPEEND>;

=item C<F_DUMPFILE>;

=item C<F_CONT_DUMPFILE>;

=item C<F_SPLIT_DUMPFILE>; or

=item C<F_EMPTY>.

=back

NOTE: no methods are currently defined on C<dumpfile_t>; interfaces
can be written as needed.  A new dumpfile is created with

    my $hdr = Amanda::Types::dumpfile_t->new();

=cut
%}

amglue_add_flag_tag_fns(filetype_t);
amglue_add_constant(F_UNKNOWN, filetype_t);
amglue_add_constant(F_WEIRD, filetype_t);
amglue_add_constant(F_TAPESTART, filetype_t);
amglue_add_constant(F_TAPEEND, filetype_t);
amglue_add_constant(F_DUMPFILE, filetype_t);
amglue_add_constant(F_CONT_DUMPFILE, filetype_t);
amglue_add_constant(F_SPLIT_DUMPFILE, filetype_t);
amglue_add_constant(F_EMPTY, filetype_t);

typedef char string_t[STRMAX];
%typemap(memberin) string_t {
    strncpy($1, $input, STRMAX);
    if ($1[STRMAX-1] != '\0')
	SWIG_exception(SWIG_ValueError, "String too large for dumpfile_t");
}

typedef struct {

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
    string_t application;
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
    char *dle_str;
    int is_partial;
    int partnum;
    int totalparts; /* -1 == UNKNOWN */
    size_t blocksize;

    %extend {
	/* constructor */
	dumpfile_t(void) {
	    dumpfile_t *df = malloc(sizeof(*df));
	    fh_init(df);
	    return df;
	}

	/* destructor */
	~dumpfile_t(void) {
	    free(self);
	}
    }
} dumpfile_t;

/* TODO: rename to dump_header */
