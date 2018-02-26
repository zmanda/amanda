/*
 * %W% %G%
 * $Id: ucfggsc.c,v 1.1 2001/04/15 11:12:37 ant Exp $
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
#include <sys/types.h>
#include <errno.h>
#include <sys/cfgdb.h>
#include <sys/cfgodm.h>
#include <cf.h>
#include <sys/sysconfig.h>
#include <sys/sysmacros.h>
#include <sys/device.h>

#include "gscdds.h"

#define	vprintf	if (verbose) printf

extern mid_t loadext(char *, int, int);

static void
err_exit(char exitcode)
{
    odm_close_class(CuDv_CLASS);
    odm_close_class(PdDv_CLASS);
    odm_terminate();
    exit(exitcode);
}

int
main(int argc, char **argv)
{
    char *logical_name, *ptr;
    char sstring[256];
    struct CuDv cudvobj;
    struct PdDv pddvobj;
    int rc, how_many, errflg, c, majorno, minorno, unit, verbose;
    struct cfg_dd cfg;
    struct cfg_load load;
    int *minor_list;
    extern int optind;
    extern char *optarg;

    verbose = errflg = 0;
    logical_name = NULL;
    while ((c = getopt(argc,argv,"vl:")) != EOF) {
	switch (c) {
	case 'v':
	    verbose++;
	    break;
	case 'l':
	    if (logical_name != NULL)
		errflg++;
	    logical_name = optarg;
	    break;
	default:
	    errflg++;
	}
    }
    if (errflg)
	exit(E_ARGS);

    if (logical_name == NULL)
	exit(E_LNAME);

    if (odm_initialize() == -1)
	exit(E_ODMINIT);

    /* Get Customized Device Object for this device */
    sprintf(sstring,"name = '%s'",logical_name);
    rc = (int) odm_get_first(CuDv_CLASS, sstring, &cudvobj);
    if (rc ==  0) {
	err_exit(E_NOCuDv);
    } else if (rc == -1) {
	err_exit(E_ODMGET);
    }

    if (cudvobj.status == DEFINED)
	err_exit(E_OK);  /* already unconf'd */

    /* get device's predefined object */
    sprintf(sstring,"uniquetype = '%s'", cudvobj.PdDvLn_Lvalue);
    rc = (int) odm_get_first(PdDv_CLASS, sstring, &pddvobj);
    if (rc ==  0)
	err_exit(E_NOPdDv);
    else if (rc == -1)
	err_exit(E_ODMGET);

    /*
     * Call sysconfig() to "terminate" the device.
     * If fails with EBUSY, then device instance is "open",
     * and device cannot be "unconfigured".  Any other errno
     * returned will be ignored since we MUST unconfigure the
     * device even if it reports some other error.
     */

    /* get major number of device */
    majorno = genmajor(pddvobj.DvDr);
    if (majorno == -1) {
	return(E_MAJORNO);
    }
    /* get minor number */
    minor_list = getminor(majorno, &how_many, pddvobj.DvDr);
    if (minor_list == NULL || how_many == 0)
	err_exit (E_MINORNO);
    vprintf("how_many=%d\n", how_many);
    ptr = logical_name;
    ptr += strlen(pddvobj.prefix);
    unit = atoi(ptr);
    if (unit >= how_many) {
	err_exit (E_MINORNO);
    }
    minorno = minor_list[unit];
    vprintf("unit %d minorno %d\n", unit, minorno);

    /* create devno for this device */
    cfg.devno = makedev(majorno, minorno);
    cfg.kmid = 0;
    cfg.ddsptr = (caddr_t) NULL;
    cfg.ddslen = (int) 0;
    cfg.cmd = CFG_TERM;
    if (sysconfig(SYS_CFGDD, &cfg, sizeof(struct cfg_dd)) == -1) {
	if (errno == EBUSY)
	    err_exit(E_BUSY);
    }
    cfg.kmid = loadext(pddvobj.DvDr, FALSE, FALSE);
    if (cfg.kmid == NULL)
	err_exit(E_UNLOADEXT);

    /* Change the status field of device to "DEFINED" */
    cudvobj.status = DEFINED;
    if (odm_change_obj(CuDv_CLASS, &cudvobj) == -1) 
	err_exit(E_ODMUPDATE);

    /*
     * Terminate ODM
     */
    odm_terminate();
    return (E_OK);
}
/*
 * Overrides for Emacs so that we follow Linus's tabbing style.
 * Emacs will notice this stuff at the end of the file and automatically
 * adjust the settings for this buffer only.  This must remain at the end
 * of the file.
 * ---------------------------------------------------------------------------
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
