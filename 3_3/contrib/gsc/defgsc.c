/*
 * $Id: defgsc.c,v 1.1 2001/04/15 11:12:37 ant Exp $
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
#include <sys/cfgdb.h>
#include <sys/cfgodm.h>
#include <cf.h>

static const char *triple = "type = '%s' AND class = '%s' AND subclass = '%s'";
static const char *utype =
	"uniquetype = '%s' and connkey = '%s' and connwhere = '%s'";

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
    extern int optind;
    extern char *optarg;
    char *class, *subclass, *type, *parent, *connect;
    char sstring[256], lname[256];
    char parent_loc[LOCSIZE];
    struct Class *cusdev;
    struct PdDv PdDv;
    struct PdCn PdCn;
    struct CuDv CuDv;
    int seqno, rc, errflg, c;

    errflg = 0;

    class = subclass = type = NULL;
    parent = connect = NULL;

    while ((c = getopt(a, v, "p:w:c:s:t:")) != EOF) {
	switch (c) {
	case 'c':
	    if (class != NULL)
		errflg++;
	    class = optarg;
	    break;
	case 's':
	    if (subclass != NULL)
		errflg++;
	    subclass = optarg;
	    break;
	case 't':
	    if (type != NULL)
		errflg++;
	    type = optarg;
	    break;
	case 'w':
	    if (connect != NULL)
		errflg++;
	    connect = optarg;
	    break;
	case 'p':
	    if (parent != NULL)
		errflg++;
	    parent = optarg;
	    break;
	default:
	    errflg++;
	}
    }
    if (errflg) {
	exit(E_ARGS);
    }

    /*
     * Verify that we have the right triple and all of it specified.
     */
    if (class == NULL || subclass == NULL || type == NULL) {
	exit(E_TYPE);
    }
    if (strcmp(class, "generic") ||
	strcmp(subclass, "scsi") ||
	strcmp(type, "gsc")) {
	exit(E_TYPE);
    }
    /*
     * Verify that a parent and a connection address was specified
     */
    if (parent == NULL || connect == NULL) {
	exit(E_PARENT);
    }

    /*
     * Open up ODM.
     */
    if (odm_initialize() == -1) {
	exit(E_ODMINIT);
    }
    if (odm_lock("/etc/objrepos/config_lock", 0) == -1) {
	err_exit(E_ODMLOCK);
    }
    /*
     * Get the PreDefined Device Object
     */
    (void) sprintf(sstring, triple, type, class, subclass);
    rc = (int) odm_get_first(PdDv_CLASS, sstring, &PdDv);
    if (rc == 0) {
	err_exit(E_NOPdDv);
    } else if (rc == -1) {
	err_exit(E_ODMGET);
    }

    /*
     * Open the Customized Device data base
     */
    if ((int) (cusdev = odm_open_class(CuDv_CLASS)) == -1) {
	err_exit(E_ODMOPEN);
    }
    /*
     * Check parent device
     */
    (void) sprintf(sstring, "name = '%s'", parent);
    rc = (int) odm_get_first(cusdev, sstring, &CuDv);
    if (rc == 0) {
	err_exit(E_NOCuDvPARENT);
    } else if (rc == -1) {
	err_exit(E_ODMGET);
    }
    (void) memset(parent_loc, 0, sizeof parent_loc);
    (void) strncpy(parent_loc, CuDv.location, sizeof CuDv.location);
    (void) sprintf(sstring, utype, CuDv.PdDvLn_Lvalue, subclass , connect);
    rc = (int) odm_get_first(PdCn_CLASS, sstring, &PdCn);
    if (rc == 0) {
	err_exit(E_INVCONNECT);
    } else if (rc == -1) {
	err_exit(E_ODMGET);
    }

    /*
     * Generate a name.
     */

    /* generate logical name for device */
    if ((seqno = genseq(PdDv.prefix)) < 0) {
	err_exit(E_MAKENAME);
    }
    (void) sprintf(lname, "%s%d", PdDv.prefix, seqno);
    (void) sprintf(sstring, "name = '%s'", lname);
    rc = (int) odm_get_first(cusdev, sstring, &CuDv);
    if (rc == -1) {
	err_exit(E_ODMGET);
    } else if (rc != 0) {
	/*
	 * Name already exists
	 */
	err_exit(E_LNAME);
    }
    /* Put device name into new CuDv object */
    (void) strcpy(CuDv.name, lname);

    /*
     * Fill in remainder of new customized device object
     */

    /*
     * Location codes for SCSI devices consists of 4 pairs of
     * digits separated by hyphens. The first two pairs of digits 
     * identify the adapter's slot number. The last two pairs of
     * digits identify the adapter connector number and the scsi
     * connection address. As far as I can tell, there never
     * is a non-zero value on the connector number for SCSI
     * adapters.
     *
     * This is further complicated by a slight difference in
     * this naming convention where Model 320 and Model 220
     * machines define the parent (adapter) location as 00-00-0S,
     * so we'll detect that variant.
     *
     * This can also be further complicated by the fact that
     * connection addresses changed between AIX Release 3.2.X
     * and AIX Release 4.X, where the trailing digit pair went
     * from a simple two digits (Target ID adjacent to Logical
     * Unit Number) to two digits separated by a comma. However,
     * since that is an argument passed in via the command line,
     * we can say, "Not our problem," and drive onwards.
     */
    if (strlen(parent_loc) <= 5) {
	(void) sprintf(CuDv.location, "%s-00-%s", parent_loc, connect);
    } else {
	(void) sprintf(CuDv.location, "%s-%s", parent_loc, connect);
    }

    CuDv.status = DEFINED;
    CuDv.chgstatus = PdDv.chgstatus;
    strcpy(CuDv.ddins, PdDv.DvDr);
    strcpy(CuDv.PdDvLn_Lvalue, PdDv.uniquetype);
    strcpy(CuDv.parent, parent);
    strcpy(CuDv.connwhere, connect);

    if (odm_add_obj(cusdev, &CuDv) == -1) {
	err_exit(E_ODMADD);
    }
    if (odm_close_class(CuDv_CLASS) == -1) {
	err_exit(E_ODMCLOSE);
    }
    odm_terminate();
    /*
     * Device defined successfully.
     * Print device name to stdout with a space appended.
     */
    fprintf(stdout, "%s ", CuDv.name);
    exit(E_OK);
}
/*
 * Overrides for Emacs so that we follow Linus's tabbing style.
 * Emacs will notice this stuff at the end of the file and automatically
 * adjust the settings for this buffer only.  This must remain at the end
 * of the file.
 * ---------------------------------------------------------------------------
n * Local variables:
 * c-indent-level: 4
 * c-brace-imaginary-offset: 0
 * c-brace-offset: -4
 * c-argdecl-indent: 4
 * c-label-offset: -4
 * c-continued-statement-offset: 4
 * c-continued-brace-offset: 0
 * End:
 */
