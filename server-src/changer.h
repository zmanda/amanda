/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * $Id: changer.h,v 1.13 2006/05/25 01:47:19 johnfranks Exp $
 *
 * interface routines for tape changers
 */
#ifndef CHANGER_H
#define CHANGER_H

#include "amanda.h"

extern int changer_debug;
extern char *changer_resultstr;

int changer_init(void);
int changer_reset(char **slotstr);
int changer_clean(char **slotstr);
int changer_eject(char **slotstr);
int changer_label(char *slotsp, char *labelstr);
int changer_info(int *nslotsp, char **curslotstr, int *backwards);
int changer_query(int *nslotsp, char **curslotstr, int *backwards,
		     int *searchable);
int changer_search(char *searchlabel, char **outslotstr, char **devicename);
int changer_loadslot(char *inslotstr, char **outslotstr, char **devicename);
void changer_current(void *user_data,
                        int (*user_init)(void *user_data,
                                         int rc, int nslots, int backwards,
                                         int searchable),
		     int (*user_slot)(void *user_data,
                                      int rc, char *slotstr, char *device));


/* USAGE: changer_find(user_data, init_fxn, slot_fxn, searchlabel)
 *
 * Searches the changer. If searchlabel is not NULL, and the changer has
 * barcode support, then changer_find will load that tape
 * first. Otherwise, changer_find will search through the tape drive
 * one by one until user_slot() returns nonzero.
 *
 * Parameters: user_data: A pointer which is not interpreted by
 *                        changer_find, but passed back to the
 *                        callback functions user_data and user_slot.
 *                        You can use this structure instead of
 *                        globals.
 *             user_init: This function is called right away. Its
 *                        arguments are:
 *                        user_data: (as above)
 *                        rc:     The results of the changer -info
 *                                command.
 *                        nslots: The number of slots in the changer.
 *                        backwards: Whether this changer can go
 *                                backwards or not. Some changers
 *                                can only search in one direction,
 *                                and then the operator must reload
 *                                the tapes.
 *                        searchable: Whether this changer has a
 *                                barcode reader or similar device.
 *             user_slot: This function is called for every slot.
 *                        Searching stops when it returns
 *                        nonzero. Arguments are:
 *                        user_data: (as above)
 *                        rc:     The results of the changer -slot
 *                                command.
 *                        slotstr: The slot which was loaded
 *                        device: The tape device to use to read this slot.
 */



void changer_find(void *user_data,
                     int (*user_init)(void *user_data, int rc,
                                      int nslots, int backwards,
                                      int searchable),
		     int (*user_slot)(void *user_data, int rc,
                                      char *slotstr, char *device),
                     char *searchlabel);

#endif	/* !CHANGER_H */
