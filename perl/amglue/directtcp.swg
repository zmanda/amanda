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

/*
 * SWIG typemaps for DirectTCP-related stuff.
 */

%typemap(out) DirectTCPAddr * {
    /* (we assume this is an *array* of addresses) */
    DirectTCPAddr *iter = $1;
    AV *av = newAV();
    int i = 0;

    while (iter && iter->ipv4) {
	struct in_addr in = { iter->ipv4 };
	char *addr = inet_ntoa(in);
	AV *tuple = newAV();

	g_assert(NULL != av_store(tuple, 0,
		newSVpv(addr, 0)));
	g_assert(NULL != av_store(tuple, 1, newSViv(iter->port)));
	g_assert(NULL != av_store(av, i++, newRV_noinc((SV *)tuple)));
	iter++;
    }

    $result = newRV_noinc((SV *)av);
    argvi++;
}
