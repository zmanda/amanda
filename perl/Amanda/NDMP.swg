/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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

%module "Amanda::NDMP"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/NDMP.pod"

%{
#include "ndmp-proxy.h"
%}

%typemap(in,numinputs=0) char **errmsg (char *msg) {
    msg = NULL;
    $1 = &msg;
}

%typemap(argout) char **errmsg {
    /* raise an exception if there was an error */
    if (*$1) {
	croak("%s", *$1);
    }
}

int  connect_to_ndmp_proxy(char **errmsg);
amglue_export_tag(ndmp, connect_to_ndmp_proxy);
