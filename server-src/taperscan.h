/*
 * Copyright (c) 2006,2007,2008 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

/*
 * $Id: taperscan.h,v 1.3 2006/05/25 01:47:20 johnfranks Exp $
 *
 * This contains the interface to the tape-scan algorithm implementation.
 * The interface is rather simple: Calling programs (taper, amcheck,
 * amtape) call the function below with a desired tape label, and get back
 * all the relevant label information, along with any other error messages.
 */
#ifndef TAPERSCAN_H
#define TAPERSCAN_H

#include <device.h>

typedef struct taper_scan_tracker_s taper_scan_tracker_t;

/* taper_scan(): Scans the changer to find a tape to use. Reads the tape
 *               label, or invents a new one if label_new_tapes is in use.
 *               Note that the returned label information should not be
 *               re-read, because there may not actually exist a label
 *               on-tape (for WORM or newly-labeled media).
 *
 *               This function may be run multiple times consecutively with
 *               the same tracker; each run will return a different elegible
 *               tape. The 
 *
 * Inputs: wantlabel
 * Outputs: Returns: -1 if an error occured or no tape was found.
 *                    1 if the most desirable tape was found.
 *                    2 if some other labeled tape was found.
 *                    3 if a brand new tape was found, which we should
 *                         label ASAP.
 *          gotlabel: What label was actually retrieved or invented.
 *         timestamp: What the timestamp string on-tape was. May be "X".
 *     error_message: Debugging output.
 *           tapedev: What device to use from now on.
 *           tracker: Pointer to an allocated taper_scan_tracker_t, used for
 *                    persistance between runs.
 *
 * All returned strings are newly-allocated. */

typedef void (*TaperscanOutputFunctor)(void * data, char * msg);
typedef gboolean (*TaperscanProlongFunctor)(void *data);

int taper_scan (char* wantlabel,
                char** gotlabel, char** timestamp,
                char **tapedev,
                taper_scan_tracker_t* tracker,
                TaperscanOutputFunctor output_functor,
                void *output_data,
                TaperscanProlongFunctor prolong_functor,
                void *prolong_data
                );
void FILE_taperscan_output_callback(void *data, char *msg);
void CHAR_taperscan_output_callback(void *data, char *msg);

/* Returns a newly allocated tracker object. */
taper_scan_tracker_t* taper_scan_tracker_new(void);

/* Frees a tracker object. */
void taper_scan_tracker_free(taper_scan_tracker_t*);

#endif	/* !TAPERSCAN_H */
