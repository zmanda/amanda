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
#include "amanda.h"

#include "conffile.h"
#include "device.h"
#include "server_util.h"
#include "version.h"

/* Actually try it. */
static ReadLabelStatusFlags try_read_label(char * device_name) {
    Device * device;

    device = device_open(device_name);
    if (device == NULL) {
        return (READ_LABEL_STATUS_DEVICE_MISSING |
                READ_LABEL_STATUS_DEVICE_ERROR);
    }

    device_set_startup_properties_from_config(device);
    
    return device_read_label(device);
}

/* Convert to output. */
static void print_result(ReadLabelStatusFlags flags) {
    char * flags_str;
    flags_str = g_strjoinv_and_free
        (g_flags_short_name_to_strv(flags,
                                    READ_LABEL_STATUS_FLAGS_TYPE), "\n");
    printf("%s\n", flags_str);
    amfree(flags_str);
}

int
main(
    int		argc,
    char **	argv)
{
    char *conffile;
    char * device_name;
    ReadLabelStatusFlags result;

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

    set_pname("amcleanupdisk");

    dbopen(DBG_SUBDIR_SERVER);

    if(argc < 2 || argc > 3) {
	error(_("Usage: amdevcheck%s <config> [ <device name> ]"),
              versionsuffix());
	/*NOTREACHED*/
    }

    /* parse options */
    config_name = argv[1];

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = vstralloc(config_dir, CONFFILE_NAME, NULL);
    if(read_conffile(conffile)) {
	error(_("errors processing config file \"%s\""), conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    check_running_as(RUNNING_AS_DUMPUSER);

    if (argc == 3) {
        device_name = argv[2];
    } else {
        device_name = getconf_str(CNF_TAPEDEV);
    }

    device_api_init();

    result = try_read_label(device_name);
    print_result(result);
    return result;
}
