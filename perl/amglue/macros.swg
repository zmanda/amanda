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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/*
 * This file contains SWIG macros and typemaps that complement libamglue.
 */

/*
 * flags and enums
 *
 * Rather than try to use glib's flag/enum architecture, which is only supported
 * for a few constants (mostly in property.h), .swg files define constants using
 * these macros.  A typical definition would look like:
 *   amglue_add_flag_tag_fns(Permissions);
 *   amglue_add_constant_tag(PERM_READ, Permissions);
 *   amglue_add_constant_tag(PERM_WRITE, Permissions);
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
%perlcode %{
my %_ ## TAG ## _VALUES;
# Convert a flag value to a list of names for flags that are set.
sub TAG ## _to_strings {
    my ($flags) = @_;
    my @result = ();

    for my $k (keys %_ ## TAG ## _VALUES) {
	my $v = $_ ## TAG ## _VALUES{$k};

	# is this a matching flag?
	if ($flags & $v != $v) {
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

/* Add the given constant
 *
 * @param CONSTNAME: the name of the constant, as used in C code
 * @param SHORTNAME: the name to be shown by TAG_to_string(s) (a string)
 * @param TAG: the tag for this constant (enum name, etc.)
 */
%define amglue_add_constant(CONSTNAME, SHORTNAME, TAG)
amglue_export_tag(TAG, CONSTNAME);
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
%perlcode %{
amglue_export_tag(TAG, CONSTNAME);
%}
%enddef

/*
 * Export management
 */

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
