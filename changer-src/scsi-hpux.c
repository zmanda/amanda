/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * $Id: scsi-hpux.c,v 1.15 2006/05/25 01:47:07 johnfranks Exp $
 *
 *	scsi-chio.c -- library routines to handle the changer
 *			support for chio based systems
 *
 *	Author: Eric Schnoebelen, eric@cirr.com
 *	interface based on work by: Larry Pyeatt, pyeatt@cs.colostate.edu 
 *	Copyright: 1997, 1998 Eric Schnoebelen
 *		
 *      Michael C. Povel 03.06.98 added dummy for eject_tape
 */

#include "amanda.h"

# include <sys/scsi.h>
# include <sys/mtio.h>  /* for eject_tape ioctls */

char *moddesc = "@(#)" __FILE__
		": HP/UX SCSI changer support routines @(#)";

/* 
 * a cache of the robotics information, for use elsewhere
 */
static struct element_addresses changer_info;
static int changer_info_init = 0;


/* Get the number of the first free slot
 * return > 0 number of empty slot
 * return = 0 no slot free
 * return < 0 error
 */
int GetCurrentSlot(int fd)
{

}

static int get_changer_info(fd)
{
    int rc = 0;

    if (!changer_info_init) {
	rc = ioctl(fd, SIOC_ELEMENT_ADDRESSES, &changer_info);
	changer_info_init++;
    }
    return (rc);
}

int get_clean_state(char *dev)
{
#if 0
/*
  This code works for Linux .... 
  maybe someone can do something like this under HPUX
*/
    int status;
    unsigned char *cmd;
    unsigned char buffer[255];
    int filenr;

    if ((filenr = open(dev, O_RDWR)) < 0) {
        perror(dev);
        return 0;
    }
    memset(buffer, 0, SIZEOF(buffer));

    *((int *) buffer) = 0;      /* length of input data */
    *(((int *) buffer) + 1) = 100;     /* length of output buffer */

    cmd = (char *) (((int *) buffer) + 2);

    cmd[0] = 0x4d;         /* LOG SENSE  */
    cmd[2] = (1 << 6)|0x33;     /* PageControl, PageCode */
    cmd[7] = 00;                 /* allocation length hi */
    cmd[8] = 100;                 /* allocation length lo */

    status = ioctl(filenr, 1 /* SCSI_IOCTL_SEND_COMMAND */ , buffer);

    if (status)
        return 0;

    if ((buffer[16] & 0x1) == 1)
          return 1;

#endif
    return 0;

}


void eject_tape(char *tape)
     /* This function ejects the tape from the drive */
{
/*
  This code works for Linux .... 
  This code works for HPUX too, see 'man 7 mt'
*/
    int mtfd;
    struct mtop mt_com;

    if ((mtfd = open(tape, O_RDWR)) < 0) {
        perror(tape);
        exit(2);
    }
    mt_com.mt_op = MTOFFL;
    mt_com.mt_count = 1;
    if (ioctl(mtfd, MTIOCTOP, (char *)&mt_com) < 0) {
/* Ignore the error
       perror(tape);
       exit(2);
*/
    }
    close(mtfd);
}

/* 
 * this routine checks a specified slot to see if it is empty 
 */
int isempty(int fd, int slot)
{
struct element_status es;
int rc;

    /*
     * fill the cache as required
     */
    get_changer_info(fd);

    es.element = changer_info.first_storage + slot;

    rc = ioctl(fd, SIOC_ELEMENT_STATUS, &es);
    if (rc) {
	g_fprintf(stderr, _("%s: element status query failed: 0x%x %s\n"),
				get_pname(), rc, strerror(errno));
	return(-1);
    }

    return !es.full;
}

/*
 * find the first empty slot 
 */
int find_empty(int fd, int start, int count)
{
struct element_status  es;
int i, rc;

    get_changer_info(fd);

    i = changer_info.first_storage;
    do {
	es.element = i++;
	rc = ioctl(fd, SIOC_ELEMENT_STATUS, &es);
    } while ( (i <= (changer_info.first_storage + changer_info.num_storages))
		&& !rc && es.full);

    if (rc) {
	g_fprintf(stderr,_("%s: element status query failed: 0x%x %s\n"),
				get_pname(), rc, strerror(errno));
	return -1;
    }
    return (i - changer_info.first_storage - 1);
}

/*
 * returns one if there is a tape loaded in the drive 
 */
int drive_loaded(int fd, int drivenum)
{
struct element_status  es;
int                            i,rc;

    get_changer_info(fd);

    es.element = changer_info.first_data_transfer + drivenum;

    rc = ioctl(fd, SIOC_ELEMENT_STATUS, &es);
    if (rc) {
	g_fprintf(stderr,_("%s: drive status quer failed: 0x%x %s\n"),
				get_pname(), rc, strerror(errno));
	return(-1);
    }

    return es.full;
}


/*
 * unloads the drive, putting the tape in the specified slot 
 */
int unload(int fd, int drive, int slot)
{
struct move_medium_parms  move;
int rc;

    get_changer_info(fd);

    /*
     * pick the first transport, just for simplicity
     */
    move.transport = changer_info.first_transport;

    move.source = changer_info.first_data_transfer + drive;
    move.destination = changer_info.first_storage + slot;
    move.invert = 0;

    rc = ioctl(fd, SIOC_MOVE_MEDIUM, &move);
    if (rc){
	g_fprintf(stderr,_("%s: move medium command failed: 0x%x %s\n"),
		get_pname(), rc, strerror(errno));
	return(-2);
    }
    return 0;
}


/*
 * moves tape from the specified slot into the drive 
 */
int load(int fd, int drive, int slot)
{
struct move_medium_parms  move;
int rc;

    get_changer_info(fd);

    /*
     * use the first transport defined in the changer, for lack of a
     * better choice..
     */
    move.transport = changer_info.first_transport;

    move.source = changer_info.first_storage + slot;
    move.destination = changer_info.first_data_transfer + drive;
    move.invert = 0;

    rc = ioctl(fd, SIOC_MOVE_MEDIUM,&move);
    if (rc){
	g_fprintf(stderr,_("%s: drive load failed (MOVE): 0x%x %s\n"),
		get_pname(), rc, strerror(errno));
	return(-2);
    }
    return (rc);
}

int get_slot_count(int fd)
{ 
int rc;

    rc = get_changer_info(fd);
    if (rc) {
        g_fprintf(stderr, _("%s: storage size query failed: 0x%x %s\n"), get_pname(),
						rc, strerror(errno));
        return -1;
    }

    return(changer_info.num_storages);

}

int get_drive_count(int fd)
{ 
    int rc;

    rc = get_changer_info(fd);
    if (rc) {
        g_fprintf(stderr, _("%s: drive count query failed: 0x%x %s\n"), get_pname(),
						rc, strerror(errno));
        return -1;
    }

    return changer_info.num_data_transfers;
}

/* This function should ask the drive if it is ready */
int Tape_Ready(char *tapedev, char *changerdev, int changerfd, int wait)
{
  FILE *out=NULL;
  int cnt=0;

  if (strcmp(tapedev, changerdev) == 0)
    {
      sleep(wait);
      return(0);
    }

  while ((cnt<wait) && (NULL==(out=fopen(tapedev,"w+")))){
    cnt++;
    sleep(1);
  }
  if (out != NULL)
    fclose(out);
  return 0;
}

int OpenDevice(char * tapedev)
{
  int DeviceFD;

  DeviceFD = open(tapedev, O_RDWR);
  dbprintf(_("OpenDevice(%s) returns %d\n"), tapedev, DeviceFD);
  return(DeviceFD);
}

int CloseDevice(char *device, int DeviceFD)
{
  int ret;

  ret = close(DeviceFD);
  dbprintf(_("CloseDevice(%s) returns %d\n"), device, ret);
  return(ret);
}
