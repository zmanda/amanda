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

%module "Amanda::Cmdline"
%include "amglue/amglue.swg"
%include "exception.i"
%include "amglue/dumpspecs.swg"

%{
#include <glib.h>
#include "cmdline.h"
%}

%perlcode %{
=head1 NAME

Amanda::Cmdline - utilities for handling command lines

=head1 SYNOPSIS

  use Amanda::Cmdline;

  my $spec = Amanda::Cmdline::dumpspec_t->new($host, $disk, $datestamp, $level);
  print "host: $spec->{'host'}; disk: $spec->{'disk'}\n";

  my @specs = Amanda::Cmdline::parse_dumpspecs(["host", "disk", "date"],
			    $Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP);

=head1 API STATUS

Will change.

=head1 Amanda::Cmdline::dumpspec_t Objects

=head2 Instance Variables

=over

=item C<$host>

=item C<$disk>

=item C<$datestamp>

=item C<$level>

=back

=head2 Methods

=over

=item C<format()>

Format the dumpspec as a string.

=back

=head1 Functions

=over

=item C<format_dumpspec_components($host, $disk, $datestamp, $level)>

This function returns a string representing the formatted form of the given dumpspec.  This formatting
is the same as performed by C<format_dumpspec_components>, but does not need a C<dumpspec_t>.

=item C<parse_dumpspecs(@cmdline, $flags)> 

This function parses C<@cmdline> into a list of C<dumpspec_t> objects,
according to C<$flags>, which is a logical combination of zero or
more of C<$CMDLINE_PARSE_DATESTAMP> to recognize datestamps and
C<$CMDLINE_PARSE_LEVEL> to recognize levels.

=back

=head1 SEE ALSO

L<Amanda::Config> handles C<-o> options itself, through C<config_overwrites>.

=cut
%}

/* Add a few methods to make this type act like a class */
typedef struct dumpspec_t {
    %immutable;
    char *host;
    char *disk;
    char *datestamp;
    char *level;
    %mutable;

    %extend {
	/* constructor */
	dumpspec_t(char *host, char *disk, char *datestamp, char *level) {
	    return dumpspec_new(host, disk, datestamp, level);
	}

	~dumpspec_t() {
	    dumpspec_free(self);
	}

	%newobject format;
	char *format() {
	    return cmdline_format_dumpspec(self);
	}
    }
} dumpspec_t;

%rename(format_dumpspec_components) cmdline_format_dumpspec_components;
char *cmdline_format_dumpspec_components(char *host, char *disk, char *datestamp, char *level);

/* Typemap to convert a perl list of strings to the strv that 
 * cmdline_parse_dumpspecs expects.
 */
%typemap(in, numinputs=1) (int argc, char **argv) {
    AV *av;
    int i;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "Expected an arrayref");
    }
    av = (AV *)SvRV($input);

    $1 = av_len(av)+1; /* av_len(av) is like $#av */
    $2 = malloc(sizeof(char *) * $1);
    for (i = 0; i < $1; i++) {
	SV **elt = av_fetch(av, i, 0);
	if (!elt || !SvPOK(*elt)) {
	    SWIG_exception(SWIG_TypeError, "Non-string in arrayref");
	}
	$2[i] = SvPV_nolen(*elt); /* TODO: handle unicode here */
    }
}

/* Free the space allocated by the previous typemap */
%typemap(freearg) (int argc, char **argv) {
    free($2);
}

amglue_add_flag_tag_fns(cmdline_parse_dumpspecs_flags);
amglue_add_constant(CMDLINE_PARSE_DATESTAMP, cmdline_parse_dumpspecs_flags);
amglue_add_constant(CMDLINE_PARSE_LEVEL, cmdline_parse_dumpspecs_flags);
amglue_add_constant(CMDLINE_EMPTY_TO_WILDCARD, cmdline_parse_dumpspecs_flags);

%rename(parse_dumpspecs) cmdline_parse_dumpspecs;
amglue_dumpspec_list *cmdline_parse_dumpspecs(int argc, char **argv, int flags);

/* TODO:
 * convert AV back to GSList as input, convert resulting GSList into an AV of strings
 * on output
 */
/* amglue_dumpspec_list * cmdline_match_holding(amglue_dumpspec_list *dumpspec_list); */

