/*
 * Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::Feature"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Feature.pod"

%{
#include "amfeatures.h"
%}

/* include the header file directly - this saves us having to remember
 * to add new constants to both places */
#define AMANDA_FEATURE_SWG
%include "../common-src/amfeatures.h"

%{
typedef am_feature_t Set;
%}

typedef struct {
    %extend {
	~Set() {
	    am_release_feature_set(self);
	}
	void add(int feature) {
	    am_add_feature(self, feature);
	}
	void remove(int feature) {
	    am_remove_feature(self, feature);
	}
	int has(int feature) {
	    return am_has_feature(self, feature);
	}
	%newobject as_string;
	char *as_string() {
	    return am_feature_to_string(self);
	}
    };
} Set;

%newobject am_string_to_feature;
extern Set *am_string_to_feature(char *s);
%newobject am_set_default_feature_set;
extern Set *am_set_default_feature_set(void);
%newobject am_init_feature_set;
extern Set *am_init_feature_set();
%newobject features;
extern am_feature_t *am_features(Set *);

%perlcode %{

package Amanda::Feature::Set;

# this does not work, because SWIG uses the string value of objects as a hash key
# even after calling their destructor, which leads to a segfault.
#
# use overload '""' => sub { $_[0]->as_string(); };

sub Amanda::Feature::Set::from_string {
    my $class = shift;
    return Amanda::Feature::am_string_to_feature(@_);
}

sub Amanda::Feature::Set::old {
    my $class = shift;
    return Amanda::Feature::am_set_default_feature_set();
}

sub Amanda::Feature::Set::mine {
    my $class = shift;
    return Amanda::Feature::am_init_feature_set();
}

sub Amanda::Feature::Set::features {
    my $class = shift;
    return Amanmda::Feature::features();
}


package Amanda::Feature;

%}
