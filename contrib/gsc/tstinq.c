/*
 * %W% %G%
 * $Id: tstinq.c,v 1.1 2001/04/15 11:12:37 ant Exp $
 * Copyright (c) 1997 by Matthew Jacob
 *
 *	This software is free software; you can redistribute it and/or
 *	modify it under the terms of the GNU Library General Public
 *	License as published by the Free Software Foundation; version 2.
 *
 *	This software is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *	Library General Public License for more details.
 *
 *	You should have received a copy of the GNU Library General Public
 *	License along with this software; if not, write to the Free
 *	Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *	The author may be reached via electronic communications at
 *
 *		mjacob@feral.com
 *
 *	or, via United States Postal Address
 *
 *		Matthew Jacob
 *		1831 Castro Street
 *		San Francisco, CA, 94131
 */

#include <stdio.h>
#include <errno.h>
#include "gscdds.h"

static void process(char *, int);

int
main(int a, char **v)
{
    int fd;

    while (*++v) {
	fd = open(*v, 0);
	if (fd < 0) {
	    perror(*v);
	    continue;
	}
	process(*v, fd);
	(void) close(fd);
    }
    return (0);
}

static void
process(char *name, int fd)
{
    scmd_t scmd;
    char sb[32], iqd[256], sbyte, c, dt;
    static char cdb[6] = { 0x12, 0, 0, 0, 255, 0 };

    scmd.cdb = cdb;
    scmd.cdblen = sizeof (cdb);
    scmd.data_buf = iqd;
    scmd.datalen = 255;
    scmd.sense_buf = sb;
    scmd.senselen = sizeof (sb);
    scmd.statusp = &sbyte;
    scmd.rw = 1;
    scmd.timeval = 5;

    if (ioctl(fd, GSC_CMD, (caddr_t) &scmd) < 0) {
	perror("GSC_CMD");
	return;
    }
    dt = iqd[0] & 0x1f;
    c = iqd[8+28];
    iqd[8+28] = 0;
    (void) fprintf(stdout, "%s:%-28s|Device Type %d\n", name, &iqd[8], dt);
}
/*
 * mode: c
 * Local variables:
 * c-indent-level: 4
 * c-brace-imaginary-offset: 0
 * c-brace-offset: -4
 * c-argdecl-indent: 4
 * c-label-offset: -4
 * c-continued-statement-offset: 4
 * c-continued-brace-offset: 0
 * End:
 */
