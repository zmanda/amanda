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
 * $Id: amrestore.c 6512 2007-05-24 17:00:24Z ian $
 *
 * retrieves files from an amanda tape
 */
/*
 * Pulls all files from the tape that match the hostname, diskname and
 * datestamp regular expressions.
 *
 * If the header is output, only up to DISK_BLOCK_BYTES worth of it is
 * sent, regardless of the tape blocksize.  This makes the disk image
 * look like a holding disk image, and also makes it easier to remove
 * the header (e.g. in amrecover) since it has a fixed size.
 */

#include "amanda.h"
#include "util.h"
#include "fileheader.h"
#include "restore.h"
#include "conffile.h"
#include "device.h"
#include "cmdline.h"

#define CREAT_MODE	0640

/*
 * Print usage message and terminate.
 */

static void
usage(void)
{
    error(_("Usage: amrestore [-b blocksize] [-r|-c] [-p] [-h] [-f fileno] "
    	  "[-l label] [-o configoption]* tape-device|holdingfile"
	  "[hostname [diskname [datestamp [hostname [diskname [datestamp ... ]]]]]]"));
    /*NOTREACHED*/
}

/* Checks if the given tape device is actually a holding disk file. We
   accomplish this by stat()ing the file; if it is a regular file, we
   assume (somewhat dangerously) that it's a holding disk file. If
   it doesn't exist or is not a regular file, we assume it's a device
   name.

   Returns TRUE if we suspect the device is a holding disk, FALSE
   otherwise. */
static gboolean check_device_type(char * device_name) {
    struct stat stat_buf;
    int result;
    
    result = stat(device_name, &stat_buf);

    return !((result != 0 || !S_ISREG(stat_buf.st_mode)));
}

static void handle_holding_disk_restore(char * filename, rst_flags_t * flags,
                                        GSList * dumpspecs) {
    dumpfile_t this_header;
    tapelist_t this_disk;

    bzero(&this_disk, sizeof(this_disk));
    this_disk.label = filename;

    if (!restore_holding_disk(stderr, flags, NULL, &this_disk, NULL,
                              dumpspecs, &this_header, NULL)) {
        g_fprintf(stderr, "%s did not match requested host.\n", filename);
        return;
    }
}

static void handle_tape_restore(char * device_name, rst_flags_t * flags,
                                GSList * dumpspecs, char * check_label) {
    Device * device;
    DeviceStatusFlags device_status;

    dumpfile_t first_restored_file;

    device_api_init();

    fh_init(&first_restored_file);
    
    device = device_open(device_name);
    g_assert(device != NULL);
    if (device->status != DEVICE_STATUS_SUCCESS) {
        error("Could not open device %s: %s.\n", device_name, device_error(device));
    }
    
    if (!set_restore_device_read_buffer_size(device, flags)) {
        error("Error setting read block size: %s.\n", device_error_or_status(device));
    }
    device_status = device_read_label(device);
    if (device_status != DEVICE_STATUS_SUCCESS) {
        error("Error reading volume label: %s.\n", device_error_or_status(device));
    }

    g_assert(device->volume_label != NULL);

    if (!device_start(device, ACCESS_READ, NULL, NULL)) {
        error("Could not open device %s for reading: %s.\n", device_name,
	      device_error(device));
    }

    if (check_label != NULL && strcmp(check_label,
                                      device->volume_label) != 0) {
        error("Wrong label: Expected %s, found %s.\n",
              check_label, device->volume_label);
    }

    search_a_tape(device, stderr, flags, NULL, NULL, dumpspecs,
                  NULL, &first_restored_file, 0, NULL);
}

/*
 * Parses command line, then loops through all files on tape, restoring
 * files that match the command line criteria.
 */

int
main(
    int		argc,
    char **	argv)
{
    extern int optind;
    int opt;
    int holding_disk_mode;
    char *tapename = NULL;
    char *e;
    char *label = NULL;
    rst_flags_t *rst_flags;
    long tmplong;
    GSList *dumpspecs;
    config_overwrites_t *cfg_ovr;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("amrestore");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);
    error_exit_status = 2;

    rst_flags = new_rst_flags();
    rst_flags->inline_assemble = 0;

    cfg_ovr = new_config_overwrites(argc/2);
    /* handle options */
    while( (opt = getopt(argc, argv, "b:cCd:rphf:l:o:")) != -1) {
	switch(opt) {
	case 'b':
	    tmplong = strtol(optarg, &e, 10);
	    rst_flags->blocksize = (ssize_t)tmplong;
	    if(*e == 'k' || *e == 'K') {
		rst_flags->blocksize *= 1024;
	    } else if(*e == 'm' || *e == 'M') {
		rst_flags->blocksize *= 1024 * 1024;
	    } else if(*e != '\0') {
		error(_("invalid blocksize value \"%s\""), optarg);
		/*NOTREACHED*/
	    }
	    break;
	case 'c': rst_flags->compress = 1; break;
	case 'o':
	    add_config_overwrite_opt(cfg_ovr, optarg);
	    break;
	case 'C':
	    rst_flags->compress = 1;
	    rst_flags->comp_type = COMPRESS_BEST_OPT;
	    break;
	case 'r': rst_flags->raw = 1; break;
	case 'p': rst_flags->pipe_to_fd = fileno(stdout); break;
	case 'h': rst_flags->headers = 1; break;
	case 'f': rst_flags->fsf = (off_t)OFF_T_STRTOL(optarg, &e, 10);
	    /*@ignore@*/
	    if(*e != '\0') {
		error(_("invalid fileno value \"%s\""), optarg);
                g_assert_not_reached();
            }
	    /*@end@*/
	    break;
	case 'l':
            if (label) {
                error(_("Cannot specify multiple labels.\n"));
            }
	    label = stralloc(optarg);
	    break;
	default:
	    usage();
	}
    }

    /* initialize a generic configuration without reading anything */
    config_init(0, NULL);
    apply_config_overwrites(cfg_ovr);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    if(rst_flags->compress && rst_flags->raw) {
	g_fprintf(stderr,
		_("Cannot specify both -r (raw) and -c (compressed) output.\n"));
	usage();
    }

    if(optind >= argc) {
	g_fprintf(stderr, _("%s: Must specify tape-device or holdingfile\n"),
			get_pname());
	usage();
    }

    tapename = argv[optind++];

    dumpspecs = cmdline_parse_dumpspecs(argc - optind, argv + optind, 
					CMDLINE_PARSE_DATESTAMP |
					CMDLINE_EMPTY_TO_WILDCARD);

    holding_disk_mode = check_device_type(tapename);

    if (holding_disk_mode) {
        if (label) {
	    g_fprintf(stderr,_("%s: ignoring -l flag when restoring from a file.\n"),
		    get_pname());
        }

        if (rst_flags->fsf > 0) {
            g_fprintf(stderr,
                    "%s: ignoring -f flag when restoring from a file.\n",
		    get_pname());
        }

        handle_holding_disk_restore(tapename, rst_flags, dumpspecs);
    } else {
        handle_tape_restore(tapename, rst_flags, dumpspecs, label);
    }

    dumpspec_list_free(dumpspecs);

    dbclose();

    return 0;
}
