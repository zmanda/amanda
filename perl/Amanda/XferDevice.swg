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

%module "Amanda::XferDevice"
%include "amglue/amglue.swg"
%include "exception.i"
%import "Amanda/Xfer.swg"
%import "Amanda/Device.swg"

%{
#include "glib-util.h"
#include "amxfer.h"
#include "xfer-device.h"
%}

%perlcode %{
=head1 NAME

Amanda::XferDevice - server-only parts of Amanda::Xfer

This package is automatically imported into L<Amanda::Xfer> if it exists; it
is completely documented there.

=cut
%}

%newobject xfer_source_device;
XferElement *xfer_source_device(
    Device *device);

%newobject xfer_dest_device;
XferElement *xfer_dest_device(
    Device *device,
    size_t max_memory);

/* ---- */

PACKAGE(Amanda::Xfer::Source::Device)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::XferDevice::xfer_source_device)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Device)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::XferDevice::xfer_dest_device)
