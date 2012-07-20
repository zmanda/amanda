/*
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
    /* we assume this is an *array* of addresses, and return an arrayref or, if
     * the result is NULL, undef. */
    DirectTCPAddr *iter = $1;
    AV *av;
    int i;

    i = 0;
    av = newAV();
    while (iter && SU_GET_FAMILY(iter) != 0) {
	char *addr = str_sockaddr_no_port(iter);
	AV *tuple = newAV();

	g_assert(NULL != av_store(tuple, 0, newSVpv(addr, 0)));
	g_assert(NULL != av_store(tuple, 1, newSViv(SU_GET_PORT(iter))));
	g_assert(NULL != av_store(av, i++, newRV_noinc((SV *)tuple)));
	iter++;
    }

    $result = newRV_noinc((SV *)av);
    argvi++;
}

%typemap(in,numinputs=0) DirectTCPAddr **addrs
    (DirectTCPAddr *addrs) {
    addrs = NULL;
    $1 = &addrs;
}
%typemap(argout) DirectTCPAddr **addrs {
    if ($1 && *$1) {
	DirectTCPAddr *iter = *$1;
	AV *av = newAV();
	int i = 0;

	while (iter && SU_GET_FAMILY(iter) != 0) {
	    char *addr = str_sockaddr_no_port(iter);
	    AV *tuple = newAV();

	    g_assert(NULL != av_store(tuple, 0, newSVpv(addr, 0)));
	    g_assert(NULL != av_store(tuple, 1, newSViv(SU_GET_PORT(iter))));
	    g_assert(NULL != av_store(av, i++, newRV_noinc((SV *)tuple)));
	    iter++;
	}

	$result = newRV_noinc((SV *)av);
	argvi++;
    }
}

%typemap(in) DirectTCPAddr *addrs {
    AV *addrs_av;
    int num_addrs, i;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
        SWIG_exception_fail(SWIG_TypeError, "must provide an arrayref of DirectTCPAddrs");
    }
    addrs_av = (AV *)SvRV($input);
    num_addrs = av_len(addrs_av)+1;

    $1 = g_new0(DirectTCPAddr, num_addrs+1);

    for (i = 0; i < num_addrs; i++) {
        SV **svp = av_fetch(addrs_av, i, 0);
        AV *addr_av;
        sockaddr_union addr;
        IV port;

        if (!svp || !SvROK(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVAV
                 || av_len((AV *)SvRV(*svp))+1 != 2) {
            SWIG_exception_fail(SWIG_TypeError, "each DirectTCPAddr must be a 2-element arrayref");
        }

        addr_av = (AV *)SvRV(*svp);

        /* get address */
        svp = av_fetch(addr_av, 0, 0);
        if (!svp || !SvPOK(*svp) || !str_to_sockaddr(SvPV_nolen(*svp), &addr)) {
            SWIG_exception_fail(SWIG_TypeError, "invalid IPv4 addr in address");
        }

        /* get port */
        svp = av_fetch(addr_av, 1, 0);
        if (!svp || !SvIOK(*svp) || (port = SvIV(*svp)) <= 0 || port >= 65536) {
            SWIG_exception_fail(SWIG_TypeError, "invalid port in address");
        }
	SU_SET_PORT(&addr, port);

	copy_sockaddr($1, &addr);
    }
}
