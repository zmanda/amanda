/*
 *	$Id: scsi-proto.c,v 1.4 2006/05/25 01:47:10 johnfranks Exp $
 *
 *	scsi-proto.c -- library routines to handle the changer
 *			Prototype file for customization
 *
 *	Author: Eric Schnoebelen, eric@cirr.com
 *	interface based on work by: Larry Pyeatt, pyeatt@cs.colostate.edu 
 *	Copyright: 1997, 1998 Eric Schnoebelen
 *		
 *      Michael C. Povel 03.06.98 added dummy for eject_tape
 */

#include "config.h"
#include "amanda.h"
#include "libscsi.h"

char *modname = "@(#)" __FILE__
		": SCSI support library for the proto scsi interface @(#)";

/* 
 * this routine checks a specified slot to see if it is empty 
 */
int isempty(int fd, int slot)
{
    /*
     * ask the robotics, which have knowledge of the storage elements
     * if the requested slot is empty.
     *
     * nslot available for use when the number of slots needs to be known
     * to allocate memory.
     */
    return (slot_empty? 1 : 0);
}

int get_clean_state(char *dev)
{
   /* Ask the device, if it needs a cleaning */
    return (needs_cleaning? 1 : 0);

}

/*
 *
 */
void eject_tape(char *tape)
/* This function ejects the tape from the drive */
{
    eject_it;
}


/*
 * find the first empty slot 
 */
int find_empty( int fd, int start, int end)
{
    /*
     * find an empty slot to insert a tape into (if required)
     *
     * loop through the list of slots, checking see if it is currently 
     * occupied.
     */
    return (emtpy_slot);
}

/*
 * returns one if there is a tape loaded in the drive 
 */
int drive_loaded(int fd, int drivenum)
{
    /*
     * check the status of the transport (tape drive).
     *
     * return 1 if the drive is occupied, 0 otherwise.
     */
    return (occupied ? 1 : 0);
}


/*
 * unloads the drive, putting the tape in the specified slot 
 */
int unload(int fd, int drive, int slot)
{
    /*
     * unload the specified drive into the specified slot
     * (storage element)
     */
    return (success);
}


/*
 * moves tape from the specified slot into the drive 
 */
int load(int fd, int drive, int slot)
{
    /*
     * load the media from the specified element (slot) into the
     * specified data transfer unit (drive)
     */
    return (success);
}

int get_slot_count(int fd)
{ 

    /*
     * return the number of slots in the robot 
     * to the caller 
     */

    return number_of_storage_elements;
}

int get_drive_count(int fd)
{ 

    /*
     * retreive the number of data-transfer devices
     */
    return number_of_data-transfer_devices;
}
