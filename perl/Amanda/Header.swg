/*
 * Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Header"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Header.pod"

%{
#include "fileheader.h"
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
amglue_copy_to_tag(filetype_t, constants);

typedef char string_t[STRMAX];
%typemap(memberin) string_t {
    strncpy($1, $input, STRMAX);
    if ($1[STRMAX-1] != '\0')
	SWIG_exception(SWIG_ValueError, "String too large for Amanda::Header");
}

%rename(Header) dumpfile_t;
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
	    dumpfile_t *df = g_new(dumpfile_t, 1);
	    fh_init(df);
	    return df;
	}

	/* destructor */
	~dumpfile_t(void) {
	    dumpfile_free(self);
	}

	/* the usual "cheater's typemap" */
	%typemap(out) SV * "$result = $1; argvi++;";
	SV *to_string(size_t min_size, size_t max_size) {
	    size_t size = min_size;
	    char *result;

	    result = build_header(self, &size, max_size);
	    if (!result) {
		/* header didn't fit -> return undef; */
		return &PL_sv_undef;
	    } else {
		STRLEN strlen_size = (STRLEN)size;
		SV *sv;
		g_assert((size_t)strlen_size == size); /* check for casting overflow */
		sv = sv_2mortal(newSVpvn(result, (STRLEN)size));
		g_free(result);
		return sv;
	    }
	}

	void debug_dump(void) {
	    dump_dumpfile_t(self);
	}
    }
} dumpfile_t;

%newobject from_string;
%inline %{
dumpfile_t *C_from_string(const char *string) {
    dumpfile_t *result = g_new(dumpfile_t, 1);
    parse_file_header(string, result, strlen(string));
    return result;
}
%}

%perlcode %{

# SWIG produces a sub-package for the Header "class", in this case named
# Amanda::Header::Header.  For user convenience, we allow Amanda::Header->new(..) to
# do the same thing.  This is a wrapper function, and not just a typeglob assignment,
# because we want to get the right blessing.
sub new {
    shift; # ignore class
    Amanda::Header::Header->new(@_);
}

sub from_string {
    shift; # ignore class
    return C_from_string(@_);
}
%}
