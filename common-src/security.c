/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
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
#include "arglist.h"
#include "packet.h"
#include "security.h"

#ifdef BSD_SECURITY
extern const security_driver_t bsd_security_driver;
#endif
#ifdef KRB4_SECURITY
extern const security_driver_t krb4_security_driver;
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
#ifdef BSDUDP_SECURITY
extern const security_driver_t bsdudp_security_driver;
#endif
extern const security_driver_t local_security_driver;

static const security_driver_t *drivers[] = {
#ifdef BSD_SECURITY
    &bsd_security_driver,
#endif
#ifdef KRB4_SECURITY
    &krb4_security_driver,
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
#ifdef BSDUDP_SECURITY
    &bsdudp_security_driver,
#endif
    &local_security_driver,
};
#define	NDRIVERS	(size_t)(sizeof(drivers) / sizeof(drivers[0]))

/*
 * Given a name of a security type, returns the driver structure
 */
const security_driver_t *
security_getdriver(
    const char *	name)
{
    size_t i;

    assert(name != NULL);

    for (i = 0; i < NDRIVERS; i++) {
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
    handle->error = stralloc(_("unknown protocol error"));
}

printf_arglist_function1(void security_seterror, security_handle_t *, handle,
    const char *, fmt)
{
    static char buf[1024];
    va_list argp;

    assert(handle->error != NULL);
    arglist_start(argp, fmt);
    g_vsnprintf(buf, SIZEOF(buf), fmt, argp);
    arglist_end(argp);
    handle->error = newstralloc(handle->error, buf);
    dbprintf(_("security_seterror(handle=%p, driver=%p (%s) error=%s)\n"),
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
    stream->error = stralloc(_("unknown stream error"));
}

printf_arglist_function1(void security_stream_seterror,
    security_stream_t *, stream,
    const char *, fmt)
{
    static char buf[1024];
    va_list argp;

    arglist_start(argp, fmt);
    g_vsnprintf(buf, SIZEOF(buf), fmt, argp);
    arglist_end(argp);
    stream->error = newstralloc(stream->error, buf);
    dbprintf(_("security_stream_seterr(%p, %s)\n"), stream, stream->error);
}

void
security_stream_close(
    security_stream_t *	stream)
{
    dbprintf(_("security_stream_close(%p)\n"), stream);
    amfree(stream->error);
    (*stream->driver->stream_close)(stream);
}
