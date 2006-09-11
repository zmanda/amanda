/*
 * Copyright (c) 1992,1997 Sun Microsystems, Inc.  All Rights Reserved.
 * Sun considers its source code as an unpublished, proprietary
 * trade secret, and it is available only under strict license
 * provisions.  This copyright notice is placed here only to protect
 * Sun in the event the source is deemed a published work.
 * Disassembly, decompilation, or other means of reducing the
 * object code to human readable form is prohibited by the license
 * agreement under which this code is provided to the user or
 * company in possession of this copy.
 *
 * RESTRICTED RIGHTS LEGEND: Use, duplication, or disclosure by the
 * Government is subject to restrictions as set forth in
 * subparagraph (c)(1)(ii) of the Rights in Technical Data and
 * Computer Software clause at DFARS 52.227-7013 and in similar
 * clauses in the FAR and NASA FAR Supplement.
 *
 * These examples are provided with no warranties of any kind,
 * including without limitation accuracy and usefulness, and Sun
 * expressly disclaims all implied warranties of merchantability,
 * fitness for a particular purpose and non-infringement. In no
 * event shall Sun be liable for any damages, including without
 * limitation, direct, special, indirect, or consequential damages
 * arising out of, or relating to, use of these examples by customer
 * or any third party. Sun is under no obligation to provide support
 * to customer for this software.
 *
 * sstest.c
 * A simple tst program for the sample SCSI target driver.
 * To compile: cc (or acc) sstest.c -o sstest
 * Note: the full ANSI conformance flag, -Xt, fails because of the
 *	 system header files.
 *
 * Usage:
 *	sstest [device] [command] [arg]
 * Device is the /devices entry
 * Command can be:
 *	open - just open and close the device
 *	read [arg] - read a block. If arg is specifed, printf the result
 *	write [arg] - write a block. With arg, write "arg"
 *	rew - send the SCSI Rewind command (tests the USCSICMD ioctl)
 *	tur - send the SCSI Test Unit Ready command (SSTIOC_READY ioctl)
 *	errlev [lev] - set the error reporting level to <lev> (see sst_def.h)
 */

#pragma	ident	"@(#)sstest.c 1.4	97/04/07 SMI"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/scsi/impl/uscsi.h>
#include <sys/scsi/generic/commands.h>
#include "sst_def.h"

#define	BUFSZ	512

static void usage(char *name);

main(int argc, char *argv[])
{
	int	fd, nbytes, level, arg;
	char	buf[BUFSZ], *progname, *devname, *command, *value;
	struct uscsi_cmd		scmd;

	if ((argc != 3) && (argc != 4)) {
		usage(argv[0]);
		/*NOTREACHED*/
	}

	progname = argv[0];	/* name of this program */
	devname = argv[1];	/* full path of device name */
	command = argv[2];	/* command to do */
	value = argv[3];	/* command's argument if present */
	arg = (argc == 4);	/* command has arg */



	(void) sprintf(buf, "%s", devname);
	if ((fd = open(buf, O_RDWR)) == -1) {
		perror(buf);
		exit(1);
		/*NOTREACHED*/
	}

	(void) memset((void *)buf, 0, BUFSZ);

	if (strcmp(command, "open") == 0) {
		fprintf(stdout, "Device opened\n");
	} else if (strcmp(command, "read") == 0) {
		if ((nbytes = read(fd, buf, BUFSZ)) != BUFSZ) {
			perror("read");
			(void) close(fd);
			exit(1);
			/*NOTREACHED*/
		}
		if (argc == 4) {
			fprintf(stdout, "Read \"%s\"\n", buf);
		} else {
			fprintf(stdout, "Read %d bytes\n", nbytes);
		}

	} else if (strcmp(command, "write") == 0) {
		if (arg) {
			strcpy(buf, value);
		}
		if ((nbytes = write(fd, buf, BUFSZ)) != BUFSZ) {
			perror("write");
			(void) close(fd);
			exit(1);
			/*NOTREACHED*/
		}
		fprintf(stdout, "Wrote %d bytes\n", nbytes);

	} else if (strcmp(command, "rew") == 0) {
		(void) memset((void *) &scmd, 0, sizeof (scmd));
		scmd.uscsi_flags = 0;
		scmd.uscsi_timeout = 30;
		buf[0] = SCMD_REWIND;
		scmd.uscsi_cdb = buf;
		scmd.uscsi_bufaddr = NULL;
		scmd.uscsi_buflen = 0;
		scmd.uscsi_cdblen = CDB_GROUP0;

		if (ioctl(fd, USCSICMD, &scmd) == -1) {
			perror("rewind ioctl");
			(void) close(fd);
			exit(1);
			/*NOTREACHED*/
		}
		fprintf(stdout, "Device rewound, status = 0x%x\n",
		    scmd.uscsi_status);
	} else if (strcmp(command, "errlev") == 0) {
		if (argc != 4) {
			usage(progname);
			/*NOTREACHED*/
		} else if ((level = atoi(value)) == 0) {
			fprintf(stderr, "Bad error level %s\n", value);
			exit(1);
		}

		if (ioctl(fd, SSTIOC_ERRLEV, &level) == -1) {
			perror("Set error level ioctl");
			(void) close(fd);
			exit(1);
			/*NOTREACHED*/
		}
	} else if (strcmp(command, "tur") == 0) {
		if (ioctl(fd, SSTIOC_READY, NULL) == -1) {
			perror("Ready ioctl");
			(void) close(fd);
			exit(1);
			/*NOTREACHED*/
		}
		fprintf(stdout, "Device is ready\n");
	} else {
		fprintf(stderr, "Unknown command: %s\n", command);
		usage(progname);
		/*NOTREACHED*/
	}

	(void) close(fd);
	exit(0);
}

static void
usage(char *name)
{
	(void) fprintf(stderr, "Usage: %s: [device] [command]\n", name);
	(void) fprintf(stderr, "Device is the full path name of device\n");
	(void) fprintf(stderr, "Command is one of:\n");
	(void) fprintf(stderr, "\topen - just open & close\n");
	(void) fprintf(stderr, "\tread - read a block\n");
	(void) fprintf(stderr, "\twrite - write a block\n");
	(void) fprintf(stderr, "\trew - send the SCSI 'rewind' command\n");
	(void) fprintf(stderr, "\terrlev [lev] - Set the error "
	    "reporting level\n");
	(void) fprintf(stderr, "\ttur - send the SCSI 'test unit ready'"
	    "command\n");
	exit(1);
	/*NOTREACHED*/
}
