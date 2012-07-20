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
 * This file contains SWIG macros to handle C constants, enums, and flags
 */

%include "amglue/exports.swg"

/*
 * Internal support for iterating over lists of symbols; users will define
 * the list as e.g.,
 *
 *   #define FOR_ALL_WHATEVER(APPLY) APPLY(SYMBOL1) APPLY(SYMBOL2) ..
 *
 * and then passes FOR_ALL_WHATEVER as the FORALL_FN argument to one of the
 * macros below.  This uses the "X Macro" trick; see
 * http://www.drdobbs.com/blog/archives/2010/06/the_x_macro.html
 *
 * NOTE: SWIG's %define macros add C-style comments when they are expanded, so
 * this method must use #define, not %define.  Also note that this always expands
 * to a single line, making the result very ugly.
 */

#define _amglue_apply_null(V) V
#define _amglue_apply_leading_scalar(V) $V
#define _amglue_apply_trailing_comma(V) V,
#define _amglue_apply_C_scalar(V) C<$V>
#define _amglue_forall_join_whitespace(FORALL_FN) FORALL_FN(_amglue_apply_null)
#define _amglue_forall_scalars(FORALL_FN) FORALL_FN(_amglue_apply_leading_scalar)
#define _amglue_forall_terminated_by_comma(FORALL_FN) FORALL_FN(_amglue_apply_trailing_comma)

/* Rather than try to use glib's flag/enum architecture, which is only used
 * for a few constants (mostly in property.h), .swg files define constants using
 * these macros.  A typical definition would look like:
 *   amglue_add_flag_tag_fns(Permissions);
 *   amglue_add_constant(PERM_READ, Permissions);
 *   amglue_add_constant(PERM_WRITE, Permissions);
 * note that the values of the constants do not appear here, although the header
 * file in which they are defined must be included in the %{ .. %} block.
 *
 * The above would result in:
 *  - typedef int Permissions;
 *  - $PERM_READ and $PERM_WRITE in @EXPORT_OK
 *  - $PERM_READ and $PERM_WRITE in %EXPORT_TAGS{'Permissions'}
 *  - Permissions_to_strings($flags) -> ( name, name, .. )
 *
 * Similarly, amglue_add_enum_tag_fns(FileType) would add the same
 * EXPORTs, but a function
 *  - FileType_to_string($enum) -> name
 */

%define amglue_add_flag_tag_fns(TAG)
typedef int TAG;
amglue_export_tag(TAG, TAG ## _to_strings);
%perlcode %{
my %_ ## TAG ## _VALUES;
# Convert a flag value to a list of names for flags that are set.
sub TAG ## _to_strings {
    my ($flags) = @_;
    my @result = ();

    for my $k (keys %_ ## TAG ## _VALUES) {
	my $v = $_ ## TAG ## _VALUES{$k};

	# is this a matching flag?
	if (($v == 0 && $flags == 0) || ($v != 0 && ($flags & $v) == $v)) {
	    push @result, $k;
	}
    }

    # by default, just return the number as a 1-element list
    if (!@result) {
	return ($flags);
    }

    return @result;
}
%}
%enddef

%define amglue_add_enum_tag_fns(TAG)
typedef int TAG;
amglue_export_tag(TAG, TAG ## _to_string);
%perlcode %{
my %_ ## TAG ## _VALUES;
# Convert an enum value to a single string
sub TAG ## _to_string {
    my ($enumval) = @_;

    for my $k (keys %_ ## TAG ## _VALUES) {
	my $v = $_ ## TAG ## _VALUES{$k};

	# is this a matching flag?
	if ($enumval == $v) {
	    return $k;
	}
    }

    # default, just return the number
    return $enumval;
}
%}
%enddef

/* Add the given constant, assuming the constant name is the 
 * short name
 *
 * @param CONSTNAME: the name of the constant, as used in C code
 * @param TAG: the tag for this constant (enum name, etc.)
 */
%define amglue_add_constant(CONSTNAME, TAG)
enum { CONSTNAME }; /* pass the constant to SWIG */
amglue_export_tag(TAG, $CONSTNAME);
%perlcode %{
$_ ## TAG ## _VALUES{`CONSTNAME`} = $CONSTNAME;
%}
%enddef

/* Add a bunch of constants all at once; this is more efficient with
 * a large list of constants
 *
 * @param FORALL_FN: the FORALL_FN listing all of the constants (see above)
 * @param TAG: the tag for these constants
 */
#define amglue_add_constants(FORALL_FN, TAG) \
enum { _amglue_forall_terminated_by_comma(FORALL_FN) }; \
amglue_export_tag(TAG, _amglue_forall_scalars(FORALL_FN)) \
%perlcode %{ \
    foreach (qw( _amglue_forall_join_whitespace(FORALL_FN))) { \
	$_ ## TAG ## _VALUES{$_} = $$_; \
    } \
%}

/* Add the given constant with a short name
 *
 * @param CONSTNAME: the name of the constant, as used in C code
 * @param SHORTNAME: the name to be shown by TAG_to_string(s) (a string)
 * @param TAG: the tag for this constant (enum name, etc.)
 */
%define amglue_add_constant_short(CONSTNAME, SHORTNAME, TAG)
enum { CONSTNAME }; /* pass the constant to SWIG */
amglue_export_tag(TAG, $CONSTNAME);
%perlcode %{
$_ ## TAG ## _VALUES{`SHORTNAME`} = $CONSTNAME;
%}
%enddef

/* Add the given constant.  No shortname is supplied, so the constant
 * will not be used for conversion to strings.  Use this function for
 * bit combinations and other metadata, e.g., FOO_MASK or FOO_MAX
 *
 * @param CONSTNAME: the name of the constant, as used in C code
 * @param TAG: the tag for this constant (enum name, etc.)
 */
%define amglue_add_constant_noshort(CONSTNAME, TAG)
enum { CONSTNAME }; /* pass the constant to SWIG */
amglue_export_tag(TAG, $CONSTNAME);
%enddef

/* Return the symbols alone, separated by whitespace.  Note that this will
 * being with whitespace, too.  Be careful if using it in POD.
 *
 * @param FORALL_FN: the forall function to use (see above)
 */
#define amglue_constants_list(FORALL_FN) \
    FORALL_FN(_amglue_apply_C_scalar)

