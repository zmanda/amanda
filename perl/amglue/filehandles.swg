/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

%typemap(in) int fd {
    IO *io = NULL;
    PerlIO *pio = NULL;
    int fd = -1;

    if (SvIOK($input)) {
        /* plain old integer */
        $1 = SvIV($input);
    } else {
        /* try extracting as filehandle */
        
        /* note: sv_2io may call die() */
        io = sv_2io($input);
        if (io) {
            pio = IoIFP(io);
        }
        if (pio) {
            fd = PerlIO_fileno(pio);
        }
        if (fd >= 0) {
            $1 = fd;
        } else {
            SWIG_exception(SWIG_TypeError, "Expected integer file descriptor "
                "or file handle for argument $argnum");
        }
    }
}
