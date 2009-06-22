/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id: amlabel.c,v 1.53 2006/07/25 18:27:57 martinea Exp $
 *
 * write an Amanda label on a tape
 */
#include "amanda.h"
#include "conffile.h"
#include "tapefile.h"
#include "changer.h"
#include <device.h>
#include <timestamp.h>
#include <taperscan.h>

/* local functions */

int main(int argc, char **argv);

static void usage(void) {
    g_fprintf(stderr, _("Usage: %s [-f] <conf> <label> [slot <slot-number>] [-o configoption]*\n"),
	    get_pname());
    exit(1);
}

int
main(
    int		argc,
    char **	argv)
{
    char *conf_tapelist;
    char *outslot = NULL;
    char *label, *tapename = NULL;
    char *labelstr, *slotstr;
    char *conf_tapelist_old;
    int have_changer;
    int force, tape_ok;
    tapetype_t *tape;
    size_t tt_blocksize_kb;
    int slotcommand;
    Device * device;
    DeviceStatusFlags device_status;
    char *cfg_opt = NULL;
    config_overwrites_t *cfg_ovr = NULL;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amlabel");

    dbopen(DBG_SUBDIR_SERVER);
    device_api_init();

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);

    cfg_ovr = extract_commandline_config_overwrites(&argc, &argv);

    if(argc > 1 && strcmp(argv[1],"-f") == 0)
	 force=1;
    else force=0;

    if(argc != 3+force && argc != 5+force)
	usage();

    cfg_opt = argv[1+force];
    label = argv[2+force];

    if(argc == 5+force) {
	if(strcmp(argv[3+force], "slot"))
	    usage();
	slotstr = argv[4+force];
	slotcommand = 1;
    } else {
	slotstr = "current";
	slotcommand = 0;
    }

    config_init(CONFIG_INIT_EXPLICIT_NAME, cfg_opt);
    apply_config_overwrites(cfg_ovr);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    if (read_tapelist(conf_tapelist)) {
	error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }

    labelstr = getconf_str(CNF_LABELSTR);

    if(!match(labelstr, label)) {
	error(_("label %s doesn't match labelstr \"%s\""), label, labelstr);
	/*NOTREACHED*/
    }

    if((lookup_tapelabel(label))!=NULL) {
	if(!force) {
	    error(_("label %s already on a tape\n"),label);
	    /*NOTREACHED*/
    	}
    }
    tape = lookup_tapetype(getconf_str(CNF_TAPETYPE));
    tt_blocksize_kb = (size_t)tapetype_get_blocksize(tape);

    if((have_changer = changer_init()) == 0) {
	if(slotcommand) {
	    g_fprintf(stderr,
	     _("%s: no tpchanger specified in \"%s\", so slot command invalid\n"),
		    argv[0], get_config_filename());
	    usage();
	}
	tapename = getconf_str(CNF_TAPEDEV);
	if (tapename == NULL) {
	    error(_("No tapedev specified"));
	}
    } else if(have_changer != 1) {
	error(_("changer initialization failed: %s"), strerror(errno));
	/*NOTREACHED*/
    } else {
	if(changer_loadslot(slotstr, &outslot, &tapename)) {
	    error(_("could not load slot \"%s\": %s"), slotstr, changer_resultstr);
	    /*NOTREACHED*/
	}

	g_printf(_("labeling tape in slot %s (%s):\n"), outslot, tapename);
    }

    tape_ok=1;
    g_printf("Reading label...\n");fflush(stdout);
    device = device_open(tapename);
    g_assert(device != NULL);
    if (device->status != DEVICE_STATUS_SUCCESS) {
        error("Could not open device %s: %s.\n", tapename,
	      device_error(device));
    }

    if (!device_configure(device, TRUE)) {
        error("Could not configure device %s: %s.\n", tapename,
	      device_error(device));
    }

    device_status = device_read_label(device);

    if (device_status & DEVICE_STATUS_VOLUME_UNLABELED) {
	/* if there's no header, then the tape was truly empty; otherwise, there
	 * was *something* on the tape, so let's be careful and require a force */
	if (!device->volume_header || device->volume_header->type == F_EMPTY) {
	    g_printf("Found an empty tape.\n");
	} else {
	    g_printf("Found a non-Amanda tape.\n");
	    if(!force)
		tape_ok=0;
	}
    } else if (device_status & DEVICE_STATUS_VOLUME_ERROR) {
	g_printf("Reading the tape label failed: %s.\n",
		 device_error_or_status(device));
	if (!force)
	    tape_ok = 0;
    } else if (device_status != DEVICE_STATUS_SUCCESS) {
        g_printf("Reading the tape label failed: %s.\n",
		 device_error_or_status(device));
        tape_ok = 0;
    } else {
	/* got an amanda tape */
	g_printf(_("Found Amanda tape %s"),device->volume_label);
	if(match(labelstr, device->volume_label) == 0) {
	    g_printf(_(", but it is not from configuration %s."),
		     get_config_name());
	    if(!force)
		tape_ok=0;
	} else {
	    if((lookup_tapelabel(device->volume_label)) != NULL) {
		g_printf(_(", tape is active"));
		if(!force)
		    tape_ok=0;
	    }
	}
	g_printf("\n");
    }

    if(tape_ok) {
	char *timestamp = NULL;

	g_printf(_("Writing label %s..\n"), label); fflush(stdout);
        
	timestamp = get_undef_timestamp();
        if (!device_start(device, ACCESS_WRITE, label, timestamp)) {
	    error(_("Error writing label: %s.\n"),
		  device_error(device));
            g_assert_not_reached();
	} else if (!device_finish(device)) {
            error(_("Error closing device: %s.\n"),
		  device_error(device));
            g_assert_not_reached();
        }
	amfree(timestamp);

        g_printf(_("Checking label...\n")); fflush(stdout);

        device_status = device_read_label(device);
        if (device_status != DEVICE_STATUS_SUCCESS) {
	    g_printf(_("Checking the tape label failed: %s.\n"),
		     device_error_or_status(device));
            exit(EXIT_FAILURE);
        } else if (device->volume_label == NULL) {
            error(_("no label found.\n"));
            g_assert_not_reached();
        } else if (strcmp(device->volume_label, label) != 0) {
            error(_("Read back a different label: Got %s, but expected %s\n"),
                  device->volume_label, label);
            g_assert_not_reached();
        } else if (get_timestamp_state(device->volume_time) !=
                   TIME_STATE_UNDEF) {
            error(_("Read the right label, but the wrong timestamp: "
                    "Got %s, expected X.\n"), device->volume_time);
            g_assert_not_reached();
        }
        
        /* write tape list */
        
        /* make a copy */
        conf_tapelist_old = stralloc2(conf_tapelist, ".amlabel");
        if(write_tapelist(conf_tapelist_old)) {
            error(_("couldn't write tapelist: %s"), strerror(errno));
            /*NOTREACHED*/
        }
        amfree(conf_tapelist_old);
        
        /* XXX add cur_tape number to tape list structure */
        remove_tapelabel(label);
        add_tapelabel("0", label, NULL);
        if(write_tapelist(conf_tapelist)) {
            error(_("couldn't write tapelist: %s"), strerror(errno));
            /*NOTREACHED*/
        }

        if (have_changer && changer_label(outslot, label) != 0) {
	    error(_("couldn't update barcode database for slot %s, label %s\n"), outslot, label);
	    /*NOTREACHED*/
	}

        g_printf(_("Success!\n"));
    } else {
	g_printf(_("\ntape not labeled\n"));
	exit(EXIT_FAILURE);
    }
    
    g_object_unref(device);
    device = NULL;

    clear_tapelist();
    amfree(outslot);
    amfree(conf_tapelist);
    dbclose();

    return 0;
}
