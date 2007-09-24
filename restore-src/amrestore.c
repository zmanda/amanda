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

#define CREAT_MODE	0640

/*
 * Do exit(2) after an error, rather than exit(1).
 */

static void
errexit(void)
{
    exit(2);
}


/*
 * Print usage message and terminate.
 */

static void
usage(void)
{
    error(_("Usage: amrestore [-b blocksize] [-r|-c] [-p] [-h] [-f fileno] "
    	  "[-l label] tape-device|holdingfile [hostname [diskname [datestamp "
	  "[hostname [diskname [datestamp ... ]]]]]]"));
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
                                        match_list_t * match_list) {
    dumpfile_t this_header;
    tapelist_t this_disk;

    bzero(&this_disk, sizeof(this_disk));
    this_disk.label = filename;

    if (!restore_holding_disk(stderr, flags, NULL, &this_disk, NULL,
                              match_list, &this_header, NULL)) {
        fprintf(stderr, "%s did not match requested host.\n", filename);
        return;
    }
}

static void handle_tape_restore(char * device_name, rst_flags_t * flags,
                                match_list_t * match_list, char * check_label) {
    Device * device;

    device_api_init();
    
    device = device_open(device_name);
    if (device == NULL) {
        error("Could not open device.\n");
    }
    
    device_set_startup_properties_from_config(device);
    device_read_label(device);

    if (device->volume_label == NULL) {
        error("Not an Amanda tape.\n");
    } 

    if (!device_start(device, ACCESS_READ, NULL, 0)) {
        error("Could not open device %s for reading.\n", device_name);
    }

    if (check_label != NULL && strcmp(check_label,
                                      device->volume_label) != 0) {
        error("Wrong label: Expected %s, found %s.\n",
              check_label, device->volume_label);
    }

    search_a_tape(device, stderr, flags, NULL, NULL, match_list,
                  NULL, NULL, 0, NULL);
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
    char *errstr;
    int holding_disk_mode;
    char *tapename = NULL;
    match_list_t * match_list = NULL;
    match_list_t * me = NULL;
    int arg_state;
    char *e;
    char *label = NULL;
    rst_flags_t *rst_flags;
    long tmplong;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

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

    erroutput_type = ERR_INTERACTIVE;

    onerror(errexit);

    rst_flags = new_rst_flags();
    rst_flags->inline_assemble = 0;

    parse_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    /* handle options */
    while( (opt = getopt(my_argc, my_argv, "b:cCd:rphf:l:")) != -1) {
	switch(opt) {
	case 'b':
	    tmplong = strtol(optarg, &e, 10);
	    rst_flags->blocksize = (ssize_t)tmplong;
	    if(*e == 'k' || *e == 'K') {
		rst_flags->blocksize *= 1024;
	    } else if(*e == 'm' || *e == 'M') {
		rst_flags->blocksize *= 1024 * 1024;
	    } else if(*e != '\0') {
		error(_("invalid rst_flags->blocksize value \"%s\""), optarg);
		/*NOTREACHED*/
	    }
	    if(rst_flags->blocksize < DISK_BLOCK_BYTES) {
		error(_("minimum block size is %dk"), DISK_BLOCK_BYTES / 1024);
		/*NOTREACHED*/
	    }
	    if(rst_flags->blocksize > MAX_TAPE_BLOCK_KB * 1024) {
		fprintf(stderr,_("maximum block size is %dk, using it\n"),
			MAX_TAPE_BLOCK_KB);
		rst_flags->blocksize = MAX_TAPE_BLOCK_KB * 1024;
		/*NOTREACHED*/
	    }
	    break;
	case 'c': rst_flags->compress = 1; break;
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
	    label = stralloc(optarg);
	    break;
	default:
	    usage();
	}
    }

    set_server_config_from_options();

    if(rst_flags->compress && rst_flags->raw) {
	fprintf(stderr,
		_("Cannot specify both -r (raw) and -c (compressed) output.\n"));
	usage();
    }

    if(optind >= my_argc) {
	fprintf(stderr, _("%s: Must specify tape-device or holdingfile\n"),
			get_pname());
	usage();
    }

    tapename = my_argv[optind++];

#define ARG_GET_HOST 0
#define ARG_GET_DISK 1
#define ARG_GET_DATE 2

    arg_state = ARG_GET_HOST;
    while(optind < my_argc) {
	switch(arg_state) {
	case ARG_GET_HOST:
	    /*
	     * This is a new host/disk/date triple, so allocate a match_list.
	     */
	    me = alloc(SIZEOF(*me));
	    me->hostname = my_argv[optind++];
	    me->diskname = "";
	    me->datestamp = "";
	    me->next = match_list;
	    match_list = me;
	    if(me->hostname[0] != '\0'
	       && (errstr=validate_regexp(me->hostname)) != NULL) {
	        fprintf(stderr, _("%s: bad hostname regex \"%s\": %s\n"),
		        get_pname(), me->hostname, errstr);
	        usage();
	    }
	    arg_state = ARG_GET_DISK;
	    break;
	case ARG_GET_DISK:
	    me->diskname = my_argv[optind++];
	    if(me->diskname[0] != '\0'
	       && (errstr=validate_regexp(me->diskname)) != NULL) {
	        fprintf(stderr, _("%s: bad diskname regex \"%s\": %s\n"),
		        get_pname(), me->diskname, errstr);
	        usage();
	    }
	    arg_state = ARG_GET_DATE;
	    break;
	case ARG_GET_DATE:
	    me->datestamp = my_argv[optind++];
	    if(me->datestamp[0] != '\0'
	       && (errstr=validate_regexp(me->datestamp)) != NULL) {
	        fprintf(stderr, _("%s: bad datestamp regex \"%s\": %s\n"),
		        get_pname(), me->datestamp, errstr);
	        usage();
	    }
	    arg_state = ARG_GET_HOST;
	    break;
	}
    }
    if(match_list == NULL) {
	match_list = malloc(SIZEOF(*match_list));
	match_list->hostname = "";
	match_list->diskname = "";
	match_list->datestamp = "";
        match_list->level = "";
	match_list->next = NULL;
    }

    holding_disk_mode = check_device_type(tapename);

    if (holding_disk_mode) {
        if (label) {
	    fprintf(stderr,_("%s: ignoring -l flag when restoring from a file.\n"),
		    get_pname());
        }

        if (rst_flags->fsf > 0) {
            fprintf(stderr,
                    "%s: ignoring -f flag when restoring from a file.\n",
		    get_pname());
        }

        handle_holding_disk_restore(tapename, rst_flags, match_list);
    } else {
        handle_tape_restore(tapename, rst_flags, match_list, label);
    }
    return 0;
}
