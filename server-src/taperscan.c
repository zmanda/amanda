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
 * $Id: taperscan.c,v 1.17 2006/07/12 12:28:19 martinea Exp $
 *
 * This contains the implementation of the taper-scan algorithm, as it is
 * used by taper, amcheck, and amtape. See the header file taperscan.h for
 * interface information. */

#include "amanda.h"
#include "tapeio.h"
#include "conffile.h"
#include "changer.h"
#include "tapefile.h"

int scan_read_label (char *dev, char *wantlabel,
                       char** label, char** timestamp,
                       char**error_message);
int changer_taper_scan (char *wantlabel, char** gotlabel, char** timestamp,
                        char **tapedev, void (*)(void *data, char *msg),
			void *data);
int scan_slot (void *data, int rc, char *slotstr, char *device);
int taper_scan (char* wantlabel, char** gotlabel, char** timestamp,
		char** tapedev,
		void taperscan_output_callback(void *data, char *msg),
		void *data);
char *find_brand_new_tape_label (void);
void FILE_taperscan_output_callback (void *data, char *msg);
void CHAR_taperscan_output_callback (void *data, char *msg);

/* NO GLOBALS PLEASE! */

/* How does the taper scan algorithm work? Like this:
 * 1) If there is a barcode reader, and we have a recyclable tape, use the
 *    reader to load the oldest tape.
 * 2) Otherwise, search through the changer until we find a new tape
 *    or the oldest recyclable tape.
 * 3) If we couldn't find the oldest recyclable tape or a new tape,
 *    but if in #2 we found *some* recyclable tape, use the oldest one we
 *    found.
 * 4) At this point, we give up.
 */

/* This function checks the label of a single tape, which may or may not
 * have been loaded by the changer. With the addition of char *dev, it has
 * the same interface as taper_scan. 
 * Return value is the same as taper_scan.
 */
int scan_read_label(
    char *dev,
    char *desired_label,
    char** label,
    char** timestamp,
    char** error_message)
{
    char *result = NULL;

    *label = *timestamp = NULL;
    result = tape_rdlabel(dev, timestamp, label);
    if (result != NULL) {
        if (CHECK_NOT_AMANDA_TAPE_MSG(result) &&
            getconf_seen(CNF_LABEL_NEW_TAPES)) {
            amfree(result);
            
            *label = find_brand_new_tape_label();
            if (*label != NULL) {
                *timestamp = stralloc("X");
                vstrextend(error_message,
                           "Found a non-amanda tape, will label it `",
                           *label, "'.\n", NULL);
                return 3;
            }
            vstrextend(error_message,
                       "Found a non-amanda tape, but have no labels left.\n",
			NULL);
            return -1;
        }
        amfree(*timestamp);
        amfree(*label);
        vstrextend(error_message, result, "\n", NULL);
        amfree(result);
        return -1;
    }
    
    if ((*label == NULL) || (*timestamp == NULL)) {
	error("Invalid return from tape_rdlabel");
    }

    vstrextend(error_message, "read label `", *label, "', date `",
               *timestamp, "'\n", NULL);

    if (desired_label != NULL && strcmp(*label, desired_label) == 0) {
        /* Got desired label. */
        return 1;
    }

    /* Is this actually an acceptable tape? */
    if (strcmp(*label, FAKE_LABEL) != 0) {
        char *labelstr;
        labelstr = getconf_str(CNF_LABELSTR);
	if(!match(labelstr, *label)) {
            vstrextend(error_message, "label ", *label,
                       " doesn\'t match labelstr \"",
                       labelstr, "\"\n", NULL);
            return -1;
	} else {
            tape_t *tp;
            if (strcmp(*timestamp, "X") == 0) {
                /* new, labeled tape. */
                return 1;
            }

            tp = lookup_tapelabel(*label);
         
            if(tp == NULL) {
                vstrextend(error_message, "label ", *label,
                     " match labelstr but it not listed in the tapelist file.\n",
                           NULL);
                return -1;
            } else if(tp != NULL && !reusable_tape(tp)) {
                vstrextend(error_message, "cannot overwrite active tape ", *label,
                           "\n", NULL);
                return -1;
            }
        }
    }
  
    /* Yay! We got a good tape! */
    return 2;
}

/* Interface is the same as taper_scan, with the addition of the tapedev
 * output. */
typedef struct {
    char *wantlabel;
    char **gotlabel;
    char **timestamp;
    char **error_message;
    char **tapedev;
    char *first_labelstr_slot;
    int backwards;
    int tape_status;
    void (*taperscan_output_callback)(void *data, char *msg);
    void *data;
} changertrack_t;

int
scan_slot(
     void *data,
     int rc,
     char *slotstr,
     char *device)
{
    int label_result;
    changertrack_t *ct = ((changertrack_t*)data);
    int result;

    switch (rc) {
    default:
	vstrextend(ct->error_message,
		   "fatal changer error: slot ", slotstr, ": ",
		   changer_resultstr, "\n", NULL);
        result = 1;
	break;

    case 1:
	vstrextend(ct->error_message,
		   "changer error: slot ", slotstr, ": ", changer_resultstr,
		   "\n", NULL);
        result = 0;
	break;

    case 0:
	*(ct->error_message) = newvstralloc(*(ct->error_message), "slot ",
					    slotstr, ": ", NULL);
	amfree(*ct->gotlabel);
	amfree(*ct->timestamp);
        label_result = scan_read_label(device, ct->wantlabel, ct->gotlabel,
                                       ct->timestamp, ct->error_message);
        if (label_result == 1 || label_result == 3 ||
            (label_result == 2 && !ct->backwards)) {
            *(ct->tapedev) = stralloc(device);
            ct->tape_status = label_result;
            result = 1;
        } else {
	    if ((label_result == 2) && (ct->first_labelstr_slot == NULL))
		ct->first_labelstr_slot = stralloc(slotstr);
	    result = 0;
	}
	break;
    }
    ct->taperscan_output_callback(ct->data, *(ct->error_message));
    amfree(*(ct->error_message));
    return result;
}

static int 
scan_init(
    void *data,
    int rc,
    int nslots,
    int backwards,
    int searchable)
{
    changertrack_t *ct = ((changertrack_t*)data);
    
    (void)nslots;	/* Quiet unused parameter warning */
    (void)searchable;	/* Quiet unused parameter warning */

    if (rc) {
	vstrextend(ct->error_message,
		   "could not get changer info: ", changer_resultstr, "\n",
		   NULL);
	ct->taperscan_output_callback(ct->data, *(ct->error_message));
	amfree(*(ct->error_message));
    }

    ct->backwards = backwards;
    return 0;
}

int
changer_taper_scan(
    char *wantlabel,
    char **gotlabel,
    char **timestamp,
    char **tapedev,
    void (*taperscan_output_callback)(void *data, char *msg),
    void *data)
{
    char *error_message = NULL;
    changertrack_t local_data;
    char *outslotstr = NULL;
    int result;

    *gotlabel = *timestamp = *tapedev = NULL;
    local_data.wantlabel = wantlabel;
    local_data.gotlabel  = gotlabel;
    local_data.timestamp = timestamp;
    local_data.error_message = &error_message;
    local_data.tapedev = tapedev;
    local_data.first_labelstr_slot = NULL;
    local_data.backwards = 0;
    local_data.tape_status = 0;
    local_data.taperscan_output_callback  = taperscan_output_callback;
    local_data.data = data;

    changer_find(&local_data, scan_init, scan_slot, wantlabel);
    
    if (*(local_data.tapedev)) {
        /* We got it, and it's loaded. */
        return local_data.tape_status;
    } else if (local_data.first_labelstr_slot) {
        /* Use plan B. */
	result = changer_loadslot(local_data.first_labelstr_slot,
				  &outslotstr, tapedev);
	amfree(outslotstr);
        if (result == 0) {
            result = scan_read_label(*tapedev, NULL, gotlabel, timestamp,
				     &error_message);
	    taperscan_output_callback(data, error_message);
	    amfree(error_message);
	    return result;
        }
    }

    /* Didn't find a tape. :-( */
    assert(local_data.tape_status <= 0);
    taperscan_output_callback(data, "changer problem: ");
    taperscan_output_callback(data, changer_resultstr);
    return -1;
}

int taper_scan(char* wantlabel,
               char** gotlabel, char** timestamp, char** tapedev,
	       void (*taperscan_output_callback)(void *data, char *msg),
	       void *data) {

    char *error_message = NULL;
    int result;
    *gotlabel = *timestamp = NULL;

    if (wantlabel == NULL) {
        tape_t *tmp;
        tmp = lookup_last_reusable_tape(0);
        if (tmp != NULL) {
            wantlabel = tmp->label;
        }
    }

    if (changer_init()) {
        result =  changer_taper_scan(wantlabel, gotlabel, timestamp,
	                             tapedev,
				     taperscan_output_callback, data);
    }
    else {
	*tapedev = stralloc(getconf_str(CNF_TAPEDEV));
	result =  scan_read_label(*tapedev, wantlabel,
				  gotlabel, timestamp, &error_message);
	taperscan_output_callback(data, error_message);
	amfree(error_message);
    }

    return result;
}

#define AUTO_LABEL_MAX_LEN 1024
char *
find_brand_new_tape_label(void)
{
    char *format;
    char newlabel[AUTO_LABEL_MAX_LEN];
    char tmpnum[30]; /* 64-bit integers can be 21 digists... */
    char tmpfmt[16];
    char *auto_pos = NULL;
    int i;
    ssize_t label_len, auto_len;
    tape_t *tp;

    if (!getconf_seen(CNF_LABEL_NEW_TAPES)) {
        return NULL;
    }
    format = getconf_str(CNF_LABEL_NEW_TAPES);

    memset(newlabel, 0, AUTO_LABEL_MAX_LEN);
    label_len = 0;
    auto_len = -1; /* Only find the first '%' */
    while (*format != '\0') {
        if (label_len + 4 > AUTO_LABEL_MAX_LEN) {
            fprintf(stderr, "Auto label format is too long!\n");
            return NULL;
        }

        if (*format == '\\') {
            /* Copy the next character. */
            newlabel[label_len++] = format[1];
            format += 2;
        } else if (*format == '%' && auto_len == -1) {
            /* This is the format specifier. */
            auto_pos = newlabel + label_len;
            auto_len = 0;
            while (*format == '%' && label_len < AUTO_LABEL_MAX_LEN) {
                newlabel[label_len++] = '%';
                auto_len ++;
                format ++;
            }
        } else {
            /* Just copy a character. */
            newlabel[label_len++] = *(format++);
        }     
    }

    /* Sometimes we copy the null, sometimes not. */
    if (newlabel[label_len] != '\0') {
        newlabel[label_len++] = '\0';
    }

    if (auto_pos == NULL) {
        fprintf(stderr, "Auto label template contains no '%%'!\n");
        return NULL;
    }

    snprintf(tmpfmt, SIZEOF(tmpfmt), "%%0" SIZE_T_FMT "d",
	     (SIZE_T_FMT_TYPE)auto_len);

    for (i = 1; i < INT_MAX; i ++) {
        snprintf(tmpnum, SIZEOF(tmpnum), tmpfmt, i);
        if (strlen(tmpnum) != (size_t)auto_len) {
            fprintf(stderr, "All possible auto-labels used.\n");
            return NULL;
        }

        strncpy(auto_pos, tmpnum, (size_t)auto_len);

        tp = lookup_tapelabel(newlabel);
        if (tp == NULL) {
            /* Got it. Double-check that this is a labelstr match. */
            if (!match(getconf_str(CNF_LABELSTR), newlabel)) {
                fprintf(stderr, "New label %s does not match labelstr %s!\n",
                        newlabel, getconf_str(CNF_LABELSTR));
                return 0;
            }
            return stralloc(newlabel);
        }
    }

    /* Should not get here unless you have over two billion tapes. */
    fprintf(stderr, "Taper internal error in find_brand_new_tape_label.");
    return 0;
}

void
FILE_taperscan_output_callback(
    void *data,
    char *msg)
{
    if(!msg) return;
    if(strlen(msg) == 0) return;

    if(data)
	fprintf((FILE *)data, "%s", msg);
    else
	printf("%s", msg);
}

void
CHAR_taperscan_output_callback(
    /*@keep@*/	void *data,
    		char *msg)
{
    char **s = (char **)data;

    if(!msg) return;
    if(strlen(msg) == 0) return;

    if(*s)
	strappend(*s, msg);
    else
	*s = stralloc(msg);
}
