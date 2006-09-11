/*
 * Copyright (c) 2005 Zmanda Inc.  All Rights Reserved.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
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
 * Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
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


/* taper_scan(): Scans the changer to find a tape to use. Reads the tape
 *               label, or invents a new one if label_new_tapes is in use.
 *               Note that the returned label information should not be
 *               re-read, because there may not actually exist a label
 *               on-tape (for WORM or newly-labeled media).
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
 *
 * All returned strings are newly-allocated. */

int taper_scan (char* wantlabel,
                  char** gotlabel, char** timestamp,
                  char **tapedev,
		  void taperscan_output_callback(void *data, char *msg),
		  void *data);
void FILE_taperscan_output_callback(void *data, char *msg);
void CHAR_taperscan_output_callback(void *data, char *msg);

#endif	/* !TAPERSCAN_H */
