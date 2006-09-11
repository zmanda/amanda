/*
 *	$Id: libscsi.h,v 1.10 2006/05/25 01:47:07 johnfranks Exp $
 *
 *	libscsi.h -- library header for routines to handle the changer
 *			support for chio based systems
 *
 *	Author: Eric Schnoebelen, eric@cirr.com
 *	based on work by: Larry Pyeatt,  pyeatt@cs.colostate.edu 
 *	Copyright: 1997, Eric Schnoebelen
 *		
 *      Michael C. Povel 03.06.98 added function eject_tape
 */

#ifndef LIBSCSI_H
#define LIBSCSI_H

#include "amanda.h"

/*
 * This function gets the actual cleaning state of the drive 
 */
int get_clean_state(char *tape);

/*
 * This function gets the next empty slot from the changer
 * (From this slot the tape is loaded ...)
 */
int GetCurrentSlot(int fd, int drive);

/*
 * Eject the actual tape from the tapedrive
 */
int eject_tape(char *tape, int type);


/* 
 * is the specified slot empty?
 */
int isempty(int fd, int slot);

/*
 * find the first empty slot 
 */
int find_empty(int fd, int start, int count);

/*
 * returns one if there is a tape loaded in the drive 
 */
int drive_loaded(int fd, int drivenum);


/*
 * unloads the drive, putting the tape in the specified slot 
 */
int unload(int fd, int drive, int slot);

/*
 * moves tape from the specified slot into the drive 
 */
int load(int fd, int drive, int slot);

/* 
 * return the number of slots in the robot
 */
int get_slot_count(int fd);

/*
 * return the number of drives in the robot
 */
int get_drive_count(int fd);

#endif	/* !LIBSCSI_H */
