/*
 * Copyright (c) 2006,2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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
 * $Id: taperscan.c,v 1.17 2006/07/12 12:28:19 martinea Exp $
 *
 * This contains the implementation of the taper-scan algorithm, as it is
 * used by taper, amcheck, and amtape. See the header file taperscan.h for
 * interface information. */

#include "amanda.h"
#include "conffile.h"
#include "changer.h"
#include "tapefile.h"
#include "device.h"
#include "timestamp.h"
#include "taperscan.h"

struct taper_scan_tracker_s {
    GHashTable * scanned_slots;
};

int scan_read_label (char *dev, char * slot, char *wantlabel,
                       char** label, char** timestamp,
                       char**error_message);
int changer_taper_scan (char *wantlabel, char** gotlabel, char** timestamp,
                        char **tapedev, taper_scan_tracker_t * tracker,
                        TaperscanOutputFunctor output_functor,
                        void *output_data,
                        TaperscanProlongFunctor prolong_functor,
                        void *prolong_data);
int scan_slot (void *data, int rc, char *slotstr, char *device);
char *find_brand_new_tape_label (char **errmsg);
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
 * have been loaded by the changer. With the addition of char *dev, and *slot,
 * it has the same interface as taper_scan. slot should be the slot where
 * this tape is found, or NULL if no changer is in use.
 * Return value is the same as taper_scan.
 */
int scan_read_label(
    char *dev,
    char *slot,
    char *desired_label,
    char** label,
    char** timestamp,
    char** error_message)
{
    Device * device;
    char *labelstr;
    DeviceStatusFlags device_status;
    char *new_label_errmsg;

    g_return_val_if_fail(dev != NULL, -1);

    if (*error_message == NULL)
	*error_message = stralloc("");

    *label = *timestamp = NULL;
    device = device_open(dev);
    g_assert(device != NULL);

    if (device->status != DEVICE_STATUS_SUCCESS ) {
        *error_message = newvstrallocf(*error_message,
                                       _("%sError opening device %s: %s.\n"),
                                       *error_message, dev,
				       device_error_or_status(device));
	g_object_unref(device);
        amfree(*timestamp);
        amfree(*label);
        return -1;
    }

    if (!device_configure(device, TRUE)) {
        *error_message = newvstrallocf(*error_message,
                                       _("%sError configuring device %s: %s.\n"),
                                       *error_message, dev,
				       device_error_or_status(device));
	g_object_unref(device);
        amfree(*timestamp);
        amfree(*label);
        return -1;
    }

    device_status = device_read_label(device);
    
    if (device_status == DEVICE_STATUS_SUCCESS && device->volume_label != NULL) {
        *label = g_strdup(device->volume_label);
        *timestamp = strdup(device->volume_time);
    } else if (device_status & DEVICE_STATUS_VOLUME_UNLABELED) {
        if (!getconf_seen(CNF_LABEL_NEW_TAPES)) {
            *error_message = newvstrallocf(*error_message,
                                           _("%sFound an empty or non-amanda tape.\n"),
                                           *error_message);
	    g_object_unref(device);
            return -1;
        }

	/* If we got a header, but the Device doesn't think it's labeled, then this
	 * tape probably has some data on it, so refuse to automatically label it */
	if (device->volume_header && device->volume_header->type != F_EMPTY) {
            *error_message = newvstrallocf(*error_message,
		       _("%sFound a non-amanda tape; check and relabel it with 'amlabel -f'\n"),
		       *error_message);
	    g_object_unref(device);
            return -1;
	}
	g_object_unref(device);

        *label = find_brand_new_tape_label(&new_label_errmsg);
        if (*label != NULL) {
            *timestamp = stralloc("X");
            *error_message = newvstrallocf(*error_message,
                     _("%sFound an empty tape, will label it `%s'.\n"),
                                           *error_message, *label);

            return 3;
        }
        *error_message = newvstrallocf(*error_message,
				       _("%s%s.\n"),
                                       *error_message, new_label_errmsg);

        return -1;
    } else {
        char * label_errstr;
	label_errstr = g_strdup_printf(_("Error reading label: %s.\n"),
				       device_error_or_status(device));
        *error_message = newvstralloc(*error_message, *error_message,
                                      label_errstr, NULL);
        g_free(label_errstr);
        return -1;
    }

    g_assert(*label != NULL && *timestamp != NULL);
    g_object_unref(device);

    *error_message = newvstrallocf(*error_message,
                                   _("%sread label `%s', date `%s'.\n"),
                                   *error_message, *label, *timestamp);

    /* Register this with the barcode database, even if its not ours. */
    if (slot != NULL) {
        changer_label(slot, *label);
    }
    
    if (desired_label != NULL && strcmp(*label, desired_label) == 0) {
        /* Got desired label. */
        return 1;
    }

    /* Is this actually an acceptable tape? */
    labelstr = getconf_str(CNF_LABELSTR);
    if(!match(labelstr, *label)) {
        *error_message = newvstrallocf(*error_message,
                              _("%slabel \"%s\" doesn't match \"%s\".\n"),
                                       *error_message, *label, labelstr);

        return -1;
    } else {
        tape_t *tp;
        if (strcmp(*timestamp, "X") == 0) {
            /* new, labeled tape. */
            return 1;
        }
        
        tp = lookup_tapelabel(*label);
        
        if(tp == NULL) {
            *error_message =
                newvstrallocf(*error_message, 
                              _("%slabel \"%s\" matches labelstr but it is" 
                                " not listed in the tapelist file.\n"),
                              *error_message, *label);
            return -1;
        } else if(tp != NULL && !reusable_tape(tp)) {
            *error_message = 
                newvstrallocf(*error_message,
                              _("%sTape with label %s is still active" 
                                " and cannot be overwritten.\n"),
                              *error_message, *label);
            return -1;
        }
    }
  
    /* Yay! We got a good tape! */
    return 2;
}

/* Interface is the same as taper_scan, with some additional bookkeeping. */
typedef struct {
    char *wantlabel;
    char **gotlabel;
    char **timestamp;
    char **error_message;
    char **tapedev;
    char *slotstr; /* Best-choice slot number. */
    char *first_labelstr_slot;
    int backwards;
    int tape_status;
    TaperscanOutputFunctor output_callback;
    void *output_data;
    TaperscanProlongFunctor prolong_callback;
    void * prolong_data;
    taper_scan_tracker_t * persistent;
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

    if (ct->prolong_callback &&
        !ct->prolong_callback(ct->prolong_data)) {
        return 1;
    }

    if (ct->persistent != NULL) {
        gpointer key;
        gpointer value;
        if (g_hash_table_lookup_extended(ct->persistent->scanned_slots,
                                         slotstr, &key, &value)) {
            /* We already returned this slot in a previous invocation,
               skip it now. */
            return 0;
        }
    }

    if (*(ct->error_message) == NULL)
	*(ct->error_message) = stralloc("");

    switch (rc) {
    default:
	*(ct->error_message) = newvstrallocf(*(ct->error_message),
		   _("%sfatal changer error: slot %s: %s\n"),
		   *(ct->error_message), slotstr, changer_resultstr);
        result = 1;
	break;

    case 1:
	*(ct->error_message) = newvstrallocf(*(ct->error_message),
		   _("%schanger error: slot %s: %s\n"),
		   *(ct->error_message), slotstr, changer_resultstr);
        result = 0;
	break;

    case 0:
	*(ct->error_message) = newvstrallocf(*(ct->error_message),
					_("slot %s:"), slotstr);
	amfree(*ct->gotlabel);
	amfree(*ct->timestamp);
        label_result = scan_read_label(device, slotstr,
                                       ct->wantlabel, ct->gotlabel,
                                       ct->timestamp, ct->error_message);
        if (label_result == 1 || label_result == 3 ||
            (label_result == 2 && !ct->backwards)) {
            *(ct->tapedev) = stralloc(device);
            ct->tape_status = label_result;
            amfree(ct->slotstr);
            ct->slotstr = stralloc(slotstr);
            result = 1;
        } else {
	    if ((label_result == 2) && (ct->first_labelstr_slot == NULL))
		ct->first_labelstr_slot = stralloc(slotstr);
	    result = 0;
	}
	break;
    }
    ct->output_callback(ct->output_data, *(ct->error_message));
    amfree(*(ct->error_message));
    return result;
}

static int 
scan_init(
    void *data,
    int rc,
    G_GNUC_UNUSED int nslots,
    int backwards,
    G_GNUC_UNUSED int searchable)
{
    changertrack_t *ct = ((changertrack_t*)data);

    if (rc) {
	*(ct->error_message) = newvstrallocf(*(ct->error_message),
		_("%scould not get changer info: %s\n"),
		*(ct->error_message), changer_resultstr);
	ct->output_callback(ct->output_data, *(ct->error_message));
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
    taper_scan_tracker_t * tracker,
    TaperscanOutputFunctor taperscan_output_callback,
    void *output_data,
    TaperscanProlongFunctor prolong_callback,
    void * prolong_data)
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
    local_data.output_callback  = taperscan_output_callback;
    local_data.output_data = output_data;
    local_data.prolong_callback = prolong_callback;
    local_data.prolong_data = prolong_data;
    local_data.persistent = tracker;
    local_data.slotstr = NULL;

    changer_find(&local_data, scan_init, scan_slot, wantlabel);
    
    if (*(local_data.tapedev)) {
        /* We got it, and it's loaded. */
        if (local_data.persistent != NULL && local_data.slotstr != NULL) {
            g_hash_table_insert(local_data.persistent->scanned_slots,
                                local_data.slotstr, NULL);
        } else {
            amfree(local_data.slotstr);
        }
	amfree(local_data.first_labelstr_slot);
        return local_data.tape_status;
    } else if (local_data.first_labelstr_slot) {
        /* Use plan B. */
        if (prolong_callback && !prolong_callback(prolong_data)) {
            return -1;
        }
        result = changer_loadslot(local_data.first_labelstr_slot,
                                  &outslotstr, tapedev);
	amfree(local_data.first_labelstr_slot);
        amfree(outslotstr);
        if (result == 0) {
	    amfree(*gotlabel);
	    amfree(*timestamp);
            result = scan_read_label(*tapedev, NULL, NULL,
                                     gotlabel, timestamp,
                                     &error_message);
            taperscan_output_callback(output_data, error_message);
            amfree(error_message);
            if (result > 0 && local_data.persistent != NULL &&
                local_data.slotstr != NULL) {
                g_hash_table_insert(local_data.persistent->scanned_slots,
                                    local_data.slotstr, NULL);
            } else {
                amfree(local_data.slotstr);
            }
            return result;
        }
    }

    /* Didn't find a tape. :-( */
    assert(local_data.tape_status <= 0);
    return -1;
}

int taper_scan(char* wantlabel,
               char** gotlabel, char** timestamp, char** tapedev,
               taper_scan_tracker_t * tracker,
               TaperscanOutputFunctor output_functor,
               void *output_data,
               TaperscanProlongFunctor prolong_functor,
	       void *prolong_data) {
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
	                             tapedev, tracker,
                                     output_functor, output_data,
                                     prolong_functor, prolong_data);
    } else {
        /* Note that the tracker is not used in this case. */
	*tapedev = stralloc(getconf_str(CNF_TAPEDEV));
	if (*tapedev == NULL) {
	    result = -1;
	    output_functor(output_data, _("No tapedev specified"));
	} else {
	    result =  scan_read_label(*tapedev, NULL, wantlabel, gotlabel,
                                      timestamp, &error_message);
            output_functor(output_data, error_message);
	    amfree(error_message);
	}
    }

    return result;
}

#define AUTO_LABEL_MAX_LEN 1024
char *
find_brand_new_tape_label(char **errmsg)
{
    char *format;
    char newlabel[AUTO_LABEL_MAX_LEN];
    char tmpnum[30]; /* 64-bit integers can be 21 digists... */
    char tmpfmt[16];
    char *auto_pos = NULL;
    int i;
    ssize_t label_len, auto_len;
    tape_t *tp;

    *errmsg = NULL;
    if (!getconf_seen(CNF_LABEL_NEW_TAPES)) {
        return NULL;
    }
    format = getconf_str(CNF_LABEL_NEW_TAPES);

    memset(newlabel, 0, AUTO_LABEL_MAX_LEN);
    label_len = 0;
    auto_len = -1; /* Only find the first '%' */
    while (*format != '\0') {
        if (label_len + 4 > AUTO_LABEL_MAX_LEN) {
	    *errmsg = _("Auto label format is too long!");
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
	*errmsg = _("Auto label template contains no '%%'!");
        return NULL;
    }

    g_snprintf(tmpfmt, SIZEOF(tmpfmt), "%%0%zdd",
	     (size_t)auto_len);

    for (i = 1; i < INT_MAX; i ++) {
        g_snprintf(tmpnum, SIZEOF(tmpnum), tmpfmt, i);
        if (strlen(tmpnum) != (size_t)auto_len) {
	    *errmsg = _("All possible auto-labels used.");
            return NULL;
        }

        strncpy(auto_pos, tmpnum, (size_t)auto_len);

        tp = lookup_tapelabel(newlabel);
        if (tp == NULL) {
            /* Got it. Double-check that this is a labelstr match. */
            if (!match(getconf_str(CNF_LABELSTR), newlabel)) {
	        *errmsg = g_strdup_printf(_("New label %s does not match labelstr %s from amanda.conf"),
                        newlabel, getconf_str(CNF_LABELSTR));
                return NULL;
            }
            return stralloc(newlabel);
        }
    }

    /* Should not get here unless you have over two billion tapes. */
    *errmsg = _("Taper internal error in find_brand_new_tape_label.");
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
	g_fprintf((FILE *)data, "%s", msg);
    else
	g_printf("%s", msg);
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

taper_scan_tracker_t * taper_scan_tracker_new(void) {
    taper_scan_tracker_t * rval = malloc(sizeof(*rval));
    
    rval->scanned_slots = g_hash_table_new_full(g_str_hash, g_str_equal,
                                                g_free, NULL);

    return rval;
}

void taper_scan_tracker_free(taper_scan_tracker_t * tracker) {
    if (tracker->scanned_slots != NULL) {
        g_hash_table_destroy(tracker->scanned_slots);
    }
    
    free(tracker);
}

