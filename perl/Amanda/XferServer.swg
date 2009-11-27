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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::XferServer"
%include "amglue/amglue.swg"
%include "exception.i"
%import "Amanda/Xfer.swg"
%import "Amanda/Device.swg"
%import "Amanda/Header.swg"

%include "Amanda/XferServer.pod"

%{
#include "glib-util.h"
#include "amxfer.h"
#include "xfer-device.h"
#include "xfer-server.h"
%}

%newobject xfer_source_device;
XferElement *xfer_source_device(
    Device *device);

%newobject xfer_dest_device;
XferElement *xfer_dest_device(
    Device *device,
    size_t max_memory);

%newobject xfer_source_holding;
XferElement * xfer_source_holding(
    const char *filename);

%newobject xfer_dest_taper_splitter;
XferElement *xfer_dest_taper_splitter(
    Device *first_device,
    size_t max_memory,
    guint64 part_size,
    gboolean use_mem_cache,
    const char *disk_cache_dirname);

void xfer_dest_taper_start_part(
    XferElement *self,
    gboolean retry_part,
    Device *device,
    dumpfile_t *header);

void xfer_dest_taper_cache_inform(
    XferElement *self,
    const char *filename,
    off_t offset,
    off_t length);

guint64 xfer_dest_taper_get_part_bytes_written(
    XferElement *self);

%newobject xfer_source_taper;
XferElement *xfer_source_taper(void);

void xfer_source_taper_start_part(
    XferElement *self,
    Device *device);

/* ---- */

PACKAGE(Amanda::Xfer::Source::Device)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::XferServer::xfer_source_device)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Device)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::XferServer::xfer_dest_device)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Holding)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::XferServer::xfer_source_holding)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Taper)
XFER_ELEMENT_SUBCLASS()
DECLARE_METHOD(start_part, Amanda::XferServer::xfer_dest_taper_start_part)
DECLARE_METHOD(cache_inform, Amanda::XferServer::xfer_dest_taper_cache_inform)
DECLARE_METHOD(get_part_bytes_written, Amanda::XferServer::xfer_dest_taper_get_part_bytes_written)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Taper::Splitter)
XFER_ELEMENT_SUBCLASS_OF(Amanda::Xfer::Dest::Taper)
DECLARE_CONSTRUCTOR(Amanda::XferServer::xfer_dest_taper_splitter)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Taper)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::XferServer::xfer_source_taper)
DECLARE_METHOD(start_part, Amanda::XferServer::xfer_source_taper_start_part)
DECLARE_METHOD(cache_inform, Amanda::XferServer::xfer_source_taper_cache_inform)
