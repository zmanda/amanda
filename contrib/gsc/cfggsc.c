/*
 * $Id: cfggsc.c,v 1.1 2001/04/15 11:12:37 ant Exp $
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
#include <sys/scsi.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <sys/ioctl.h>
#include <sys/devinfo.h>
#include <sys/device.h>
#include <sys/cfgodm.h>
#include <cf.h>
#include <sys/cfgdb.h>
#include <fcntl.h>
#include <sys/sysconfig.h>
#include <sys/errno.h>
#include <sys/device.h>
#include <sys/mode.h>

#include "gscdds.h"

extern mid_t loadext(char *, int, int);

static int verbose;
static struct gsc_ddsinfo ddsinfo;

int main(int a, char **v);
static void check_add_sockets(dev_t, int, char *, char *);
static int has_driver_get_vpd(char *, int, char *);

#define MKNOD_MODE S_IFCHR|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH

#define	vprintf	if (verbose) printf


static void
err_exit(char exitcode)
{
    odm_close_class(CuDv_CLASS);
    odm_close_class(PdDv_CLASS);
    odm_terminate();
    exit(exitcode);
}

int
main(int a, char **v)
{
    extern char *optarg;
    char sstring[256], bufname[256], conns[NAMESIZE], *ptr, *lname;
    struct Class *cprp, *cusdev, *predev;
    struct CuDvDr dpr, *dprp;
    struct CuDv parobj, cusobj, hdobj, *xprp;
    struct PdDv preobj;
    struct CuAt *attrobj;
    int rc, c, errflg, ipl_phase, unit, munit, howmany, lun, domake;
    dev_t ctl;
    long major;
    int *mlist;
    mid_t kmid;
    struct cfg_dd dd;

    lname = NULL;
    errflg = 0;
    ipl_phase = RUNTIME_CFG;
    (void) memset((void *) &ddsinfo, 0, sizeof ddsinfo);

    while ((c = getopt(a, v, "l:12v")) != EOF) {
	switch (c) {
	case 'l':
	    if (lname != NULL)
		errflg++;
	    lname = optarg;
	    break;
	case 'v':
	    verbose++;
	    break;
	case '1':
	    if (ipl_phase != RUNTIME_CFG)
		errflg++;
	    ipl_phase = PHASE1;
	    break;
	case '2':
	    if (ipl_phase != RUNTIME_CFG)
		errflg++;
	    ipl_phase = PHASE2;
	    break;
	default:
	    errflg++;
	}
    }
    if (errflg)
	exit (E_ARGS);
    if (lname == NULL)
	exit(E_LNAME);

    if (odm_initialize() == -1) {
	return (E_ODMINIT);
    }

    /* lock the database */
    if (odm_lock("/etc/objrepos/config_lock",0) == -1)
	err_exit(E_ODMLOCK);

    /* open customized devices object class */
    if ((int)(cusdev = odm_open_class(CuDv_CLASS)) == -1)
	err_exit(E_ODMOPEN);

    /* search for customized object with this logical name */
    sprintf(sstring, "name = '%s'", lname);
    rc = (int) odm_get_first(cusdev, sstring, &cusobj);
    if (rc == 0) {
	/* No CuDv object with this name */
	err_exit(E_NOCuDv);
    } else if (rc == -1) {
	/* ODM failure */
	err_exit(E_ODMGET);
    }

    /* open predefined devices object class */
    if ((int)(predev = odm_open_class(PdDv_CLASS)) == -1)
	err_exit(E_ODMOPEN);

    /* get predefined device object for this logical name */
    sprintf(sstring, "uniquetype = '%s'", cusobj.PdDvLn_Lvalue);
    rc = (int)odm_get_first(predev, sstring, &preobj);
    if (rc == 0) {
	/* No PdDv object for this device */
	err_exit(E_NOPdDv);
    } else if (rc == -1) {
	/* ODM failure */
	err_exit(E_ODMGET);
    }
    /* close predefined device object class */
    if (odm_close_class(predev) == -1)
	err_exit(E_ODMCLOSE);

    if (ipl_phase != RUNTIME_CFG)
	setleds(preobj.led);

    /*
     * Now, if the device is already configured, we're
     * pretty much done.
     */
    if (cusobj.status == AVAILABLE) {
	/* close customized device object class */
	if (odm_close_class(cusdev) == -1)
		err_exit(E_ODMCLOSE);
	odm_terminate();
	return(E_OK);
    }
    if (cusobj.status != DEFINED) {
	vprintf("bad state: %d\n", cusobj.status);
	err_exit(E_DEVSTATE);
    }

    /* get the device's parent object */
    sprintf(sstring, "name = '%s'", cusobj.parent);
    rc = (int) odm_get_first(cusdev, sstring, &parobj);
    if (rc == 0) {
	/* Parent device not in CuDv */
	err_exit(E_NOCuDvPARENT);
    } else if (rc == -1) {
	/* ODM failure */
	err_exit(E_ODMGET);
    }

    /* Parent MUST be available to continue */
    if (parobj.status != AVAILABLE)
	err_exit(E_PARENTSTATE);


    /* make sure that no other devices are configured     */
    /* at this location                                   */
    sprintf(sstring, "parent = '%s' AND location='%s' AND status=%d",
	    cusobj.parent, cusobj.location, AVAILABLE);
    rc = (int) odm_get_first(cusdev, sstring, &cusobj);
    if (rc == -1) {
	/* odm failure */
	err_exit(E_ODMGET);
    } else if (rc) {
	/* Error: device config'd at this location */
	err_exit(E_AVAILCONNECT);
    }

    memcpy(conns, cusobj.location, NAMESIZE);
    vprintf("now fool with luns: location is %s\n", conns);
    ptr = conns;
    while (*ptr && ptr < &conns[NAMESIZE])
	ptr++;
    ptr--;
    if (ptr < &conns[1]) {
	err_exit(E_BADATTR);
    }
    lun = *ptr - '0';
    vprintf("I see lun %d\n", lun);
    if (lun < 0 || lun >= 8)
	err_exit(E_INVCONNECT);
    ddsinfo.lun = lun;
    /*
     * Generate Target
     */
    if (ptr[-1] == ',') {
	*(--ptr) = 0;
    } else {
	*ptr = 0;
    }
    while (ptr > conns && *ptr != '-')
	ptr--;
    if (*ptr == '-')
	ptr++;
    ddsinfo.target = strtol(ptr, (char **) NULL, 0);
    vprintf("I see tgt %d ptr = %d\n", ddsinfo.target, ptr - conns);

    /*
     * Generate dev_t for adapter
     */
    cprp = odm_open_class(CuDvDr_CLASS) ;
    sprintf(sstring, "value3 = %s", cusobj.parent);
    rc = (int) odm_get_obj(cprp, sstring, &dpr, TRUE);
    if (rc == 0) {
	err_exit(E_NOCuDvPARENT);
    } else if (rc == -1) {
	err_exit(E_ODMGET);
    }
    ddsinfo.busid = (dev_t) makedev(atoi(dpr.value1), atoi(dpr.value2));
    vprintf("I see %d.%d for connecting adapter\n",
	major(ddsinfo.busid), minor(ddsinfo.busid));

    /*
     * Get unit number out of logical name
     */

    ptr = lname;
    ptr += strlen(preobj.prefix);
    unit = atoi(ptr);
    vprintf("I see %d as unit\n", unit);

    /*
     * Okay, now that we have the pertinent information that we'll
     * need (adapter dev_t, device type, target, lbits, shareable,
     * unit number), we can look into actually loading/configuring the
     * current driver.
     */
    (void) sprintf(bufname, "/dev/%s", lname);

    /*
     * Get or generate major number..
     */
    if ((major = (long) genmajor(preobj.DvDr)) == -1) {
	odm_terminate();
	return (E_MAJORNO);
    }
    vprintf("major is %d\n", major);

    /*
     * Let's see if this is the first time through. If it's
     * the first time through, getminor will return NULL
     * or won't have any minors in the list.
     */
    mlist = getminor(major, &howmany, preobj.DvDr);
    vprintf("getminor: %x and howmany %d for %s\n", mlist, howmany,
		preobj.DvDr);

    domake = 1;
    if (mlist != NULL && howmany != 0) {
	/*
	 * We have a list of minors already.
	 * See if we already have the minor
	 * we want defined.
	 */
	for (c = 0; c < howmany; c++) {
	    if (mlist[c] == unit) {
		vprintf("unit %d already has minor\n", unit);
		domake = 0;
		break;
	    }
	}
    }

    if (domake) {
	(void) unlink(bufname);
	/*
	 * Now create the minor number that will match the unit number.
	 * We really don't care whether genminor succeeds, since
	 * we've alreay unlinked the device node.
	 */
	mlist = genminor(preobj.DvDr, major, unit, 1, 1, 1);
	if (mlist == (long *) NULL) {
	    err_exit(E_MINORNO);
	}
	vprintf("making %s as %d.%d with minor returned as %d\n",
		bufname, major, unit, *mlist);
	if (mknod(bufname, MKNOD_MODE, makedev(major, unit))) {
	    err_exit(E_MKSPECIAL);
	}
    } else {
	(void) mknod(bufname, MKNOD_MODE, makedev(major, unit));
    }

    /*
     * Load the driver....
     */
    kmid = loadext(preobj.DvDr, TRUE, FALSE);
    if (!kmid) {
	err_exit(E_LOADEXT);
    }

    /*
     * And configure the driver...
     */
    dd.kmid = kmid;
    dd.devno = makedev(major, unit);
    dd.cmd = CFG_INIT;
    dd.ddsptr = (caddr_t) &ddsinfo;
    dd.ddslen = sizeof (ddsinfo);

    if (sysconfig(SYS_CFGDD, &dd, sizeof (dd)) == CONF_FAIL) {
	int saverr = errno;
	/*
	 * Unload driver...
	 */
	(void) loadext(preobj.DvDr, FALSE, FALSE);
	switch(saverr) {
	case ENODEV:
		err_exit(E_WRONGDEVICE);
		/* NOTREACHED */
		break;
	case EBUSY:
		err_exit(E_AVAILCONNECT);
		/* NOTREACHED */
		break;
	case EINVAL:
	default:
		err_exit(E_CFGINIT);
		/* NOTREACHED */
		break;
	}
    }

    /* now mark the device as available */
    cusobj.status = AVAILABLE;
    if (odm_change_obj(CuDv_CLASS, &cusobj) == -1) {
	/*
	 * Unconfigure driver (for this instance)...
	 */
	dd.kmid = 0;
	dd.ddsptr = (caddr_t) NULL;
	dd.ddslen = (int ) 0;
	dd.cmd = CFG_TERM;
	(void) sysconfig(SYS_CFGDD, &dd, sizeof (dd));
	/*
	 * Unload driver...
	 */
	(void) loadext(preobj.DvDr, FALSE, FALSE);
	err_exit (E_ODMUPDATE);
    }


    (void) odm_terminate();
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
