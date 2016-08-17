/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */
/*
 * $Id: security.c,v 1.28 2006/05/25 01:47:12 johnfranks Exp $
 *
 * Security driver interface for the Amanda backup system.
 */

#include "amanda.h"
#include "packet.h"
#include "security.h"

#ifdef BSD_SECURITY
extern const security_driver_t bsd_security_driver;
#endif
#ifdef KRB5_SECURITY
extern const security_driver_t krb5_security_driver;
#endif
#ifdef RSH_SECURITY
extern const security_driver_t rsh_security_driver;
#endif
#ifdef SSH_SECURITY
extern const security_driver_t ssh_security_driver;
#endif
#ifdef BSDTCP_SECURITY
extern const security_driver_t bsdtcp_security_driver;
#endif
#ifdef SSL_SECURITY
extern const security_driver_t ssl_security_driver;
#endif
#ifdef BSDUDP_SECURITY
extern const security_driver_t bsdudp_security_driver;
#endif
extern const security_driver_t local_security_driver;

static const security_driver_t *drivers[] = {
#ifdef BSD_SECURITY
    &bsd_security_driver,
#endif
#ifdef KRB5_SECURITY
    &krb5_security_driver,
#endif
#ifdef RSH_SECURITY
    &rsh_security_driver,
#endif
#ifdef SSH_SECURITY
    &ssh_security_driver,
#endif
#ifdef BSDTCP_SECURITY
    &bsdtcp_security_driver,
#endif
#ifdef SSL_SECURITY
    &ssl_security_driver,
#endif
#ifdef BSDUDP_SECURITY
    &bsdudp_security_driver,
#endif
    &local_security_driver,
};

/*
 * Given a name of a security type, returns the driver structure
 */
const security_driver_t *
security_getdriver(
    const char *	name)
{
    size_t i;

    assert(name != NULL);

    for (i = 0; i < G_N_ELEMENTS(drivers); i++) {
	if (strcasecmp(name, drivers[i]->name) == 0) {
	    dbprintf(_("security_getdriver(name=%s) returns %p\n"),
		      name, drivers[i]);
	    return (drivers[i]);
	}
    }
    dbprintf(_("security_getdriver(name=%s) returns NULL\n"), name);
    return (NULL);
}

/*
 * For the drivers: initialize the common part of a security_handle_t
 */
void
security_handleinit(
    security_handle_t *		handle,
    const security_driver_t *	driver)
{
    dbprintf(_("security_handleinit(handle=%p, driver=%p (%s))\n"),
	      handle, driver, driver->name);
    handle->driver = driver;
    handle->error = g_strdup(_("unknown protocol error"));
}

void security_seterror(security_handle_t *handle, const char *fmt, ...)
{
    char *buf;
    va_list argp;

    assert(handle->error != NULL);
    arglist_start(argp, fmt);
    buf = g_strdup_vprintf(fmt, argp);
    arglist_end(argp);
    g_free(handle->error);
    handle->error = buf;
    g_debug("security_seterror(handle=%p, driver=%p (%s) error=%s)",
	      handle, handle->driver,
	      handle->driver->name, handle->error);
}

void
security_close(
    security_handle_t *	handle)
{
    dbprintf(_("security_close(handle=%p, driver=%p (%s))\n"),
	      handle, handle->driver,
	      handle->driver->name);
    amfree(handle->error);
    (*handle->driver->close)(handle);
}

/*
 * For the drivers: initialize the common part of a security_stream_t
 */
void
security_streaminit(
    security_stream_t *		stream,
    const security_driver_t *	driver)
{
    dbprintf(_("security_streaminit(stream=%p, driver=%p (%s))\n"),
	      stream, driver, driver->name);
    stream->driver = driver;
    stream->error = g_strdup(_("unknown stream error"));
}

void security_stream_seterror(security_stream_t *stream, const char *fmt, ...)
{
    char *buf;
    va_list argp;

    arglist_start(argp, fmt);
    buf = g_strdup_vprintf(fmt, argp);
    arglist_end(argp);
    g_free(stream->error);
    stream->error = buf;
    g_debug("security_stream_seterr(%p, %s)", stream, stream->error);
}

void
security_stream_close(
    security_stream_t *	stream)
{
    dbprintf(_("security_stream_close(%p)\n"), stream);
    amfree(stream->error);
    (*stream->driver->stream_close)(stream);
}

void
security_stream_close_async(
    security_stream_t *	stream,
    void (*fn)(void *, ssize_t, void *, ssize_t),
    void *arg)
{
    dbprintf(_("security_stream_close_async(%p)\n"), stream);
    amfree(stream->error);
    (*stream->driver->stream_close_async)(stream, fn, arg);
}
