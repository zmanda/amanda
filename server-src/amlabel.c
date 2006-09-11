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
#include "tapeio.h"
#include "changer.h"

#ifdef HAVE_LIBVTBLC
#include <vtblc.h>
#endif /* HAVE_LIBVTBLC */

/* local functions */

int main(int argc, char **argv);
void usage(void);

void
usage(void)
{
    fprintf(stderr, "Usage: %s [-f] <conf> <label> [slot <slot-number>] [-o configoption]*\n",
	    get_pname());
    exit(1);
}

int
main(
    int		argc,
    char **	argv)
{
    char *conffile;
    char *conf_tapelist;
    char *outslot = NULL;
    char *errstr = NULL, *label, *oldlabel=NULL, *tapename = NULL;
    char *labelstr, *slotstr;
    char *olddatestamp=NULL;
    char *conf_tapelist_old;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
#ifdef HAVE_LINUX_ZFTAPE_H
    int fd = -1;
    int isa_zftape;
#endif /* HAVE_LINUX_ZFTAPE_H */
    int have_changer;
    int force, tape_ok;
    tapetype_t *tape;
    size_t tt_blocksize_kb;
    int slotcommand;
    uid_t uid_me;
    uid_t uid_dumpuser;
    char *dumpuser;
    struct passwd *pw;
    int    new_argc;
    char **new_argv;

#ifdef HAVE_LIBVTBLC
    int vtbl_no      = -1;
    char *datestr    = NULL;
    char *rawtapedev = NULL;
    int first_seg, last_seg;
#endif /* HAVE_LIBVTBLC */

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amlabel");

    dbopen(DBG_SUBDIR_SERVER);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    erroutput_type = ERR_INTERACTIVE;

    parse_server_conf(argc, argv, &new_argc, &new_argv);

    if(new_argc > 1 && strcmp(new_argv[1],"-f") == 0)
	 force=1;
    else force=0;

    if(new_argc != 3+force && new_argc != 5+force)
	usage();

    config_name = new_argv[1+force];
    label = new_argv[2+force];

    if(new_argc == 5+force) {
	if(strcmp(new_argv[3+force], "slot"))
	    usage();
	slotstr = new_argv[4+force];
	slotcommand = 1;
    } else {
	slotstr = "current";
	slotcommand = 0;
    }

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if (read_tapelist(conf_tapelist)) {
	error("could not load tapelist \"%s\"", conf_tapelist);
	/*NOTREACHED*/
    }

    uid_me = getuid();
    uid_dumpuser = uid_me;
    dumpuser = getconf_str(CNF_DUMPUSER);

    if ((pw = getpwnam(dumpuser)) == NULL) {
	error("cannot look up dump user \"%s\"", dumpuser);
	/*NOTREACHED*/
    }
    uid_dumpuser = pw->pw_uid;
    if ((pw = getpwuid(uid_me)) == NULL) {
	error("cannot look up my own uid %ld", (long)uid_me);
	/*NOTREACHED*/
    }
    if (uid_me != uid_dumpuser) {
	error("running as user \"%s\" instead of \"%s\"",
	      pw->pw_name, dumpuser);
	/*NOTREACHED*/
    }

    labelstr = getconf_str(CNF_LABELSTR);

    if(!match(labelstr, label)) {
	error("label %s doesn't match labelstr \"%s\"", label, labelstr);
	/*NOTREACHED*/
    }

    if((lookup_tapelabel(label))!=NULL) {
	if(!force) {
	    error("label %s already on a tape\n",label);
	    /*NOTREACHED*/
    	}
    }
    tape = lookup_tapetype(getconf_str(CNF_TAPETYPE));
    tt_blocksize_kb = (size_t)tapetype_get_blocksize(tape);

    if((have_changer = changer_init()) == 0) {
	if(slotcommand) {
	    fprintf(stderr,
	     "%s: no tpchanger specified in \"%s\", so slot command invalid\n",
		    new_argv[0], conffile);
	    usage();
	}
	tapename = stralloc(getconf_str(CNF_TAPEDEV));
#ifdef HAVE_LIBVTBLC
	rawtapedev = stralloc(getconf_str(CNF_RAWTAPEDEV));
#endif /* HAVE_LIBVTBLC */
    } else if(have_changer != 1) {
	error("changer initialization failed: %s", strerror(errno));
	/*NOTREACHED*/
    } else {
	if(changer_loadslot(slotstr, &outslot, &tapename)) {
	    error("could not load slot \"%s\": %s", slotstr, changer_resultstr);
	    /*NOTREACHED*/
	}

	printf("labeling tape in slot %s (%s):\n", outslot, tapename);
    }

#ifdef HAVE_LINUX_ZFTAPE_H
    isa_zftape = is_zftape(tapename);
    if (isa_zftape) {
	if((fd = tape_open(tapename, O_WRONLY)) == -1) {
	    errstr = newstralloc2(errstr, "amlabel: ",
				  (errno == EACCES) ? "tape is write-protected"
				  : strerror(errno));
	    error(errstr);
	    /*NOTREACHED*/
	}
    }
#endif /* HAVE_LINUX_ZFTAPE_H */

    printf("rewinding"); fflush(stdout);

#ifdef HAVE_LINUX_ZFTAPE_H
    if (isa_zftape) {
	if(tapefd_rewind(fd) == -1) {
	    putchar('\n');
	    error(strerror(errno));
	    /*NOTREACHED*/
	}
    }
    else
#endif /* HAVE_LINUX_ZFTAPE_H */
    if((errstr = tape_rewind(tapename)) != NULL) {
	putchar('\n');
	error(errstr);
	/*NOTREACHED*/
    }

    tape_ok=1;
    printf(", reading label");fflush(stdout);
    if((errstr = tape_rdlabel(tapename, &olddatestamp, &oldlabel)) != NULL) {
	printf(", %s\n",errstr);
	tape_ok=1;
    }
    else {
	/* got an amanda tape */
	printf(" %s",oldlabel);
	if(strcmp(oldlabel, FAKE_LABEL) != 0
	   && match(labelstr, oldlabel) == 0) {
	    printf(", tape is in another amanda configuration");
	    if(!force)
		tape_ok=0;
	}
	else {
	    if((lookup_tapelabel(oldlabel)) != NULL) {
		printf(", tape is active");
		if(!force)
		    tape_ok=0;
	    }
	}
	printf("\n");
    }
    amfree(oldlabel);
    amfree(olddatestamp);
	
    printf("rewinding"); fflush(stdout);

#ifdef HAVE_LINUX_ZFTAPE_H
    if (isa_zftape) {
	if(tapefd_rewind(fd) == -1) {
	    putchar('\n');
	    error(strerror(errno));
	    /*NOTREACHED*/
	}
    }
    else
#endif /* HAVE_LINUX_ZFTAPE_H */
    if((errstr = tape_rewind(tapename)) != NULL) {
	putchar('\n');
	error(errstr);
	/*NOTREACHED*/
    }

    if(tape_ok) {
	printf(", writing label %s", label); fflush(stdout);

#ifdef HAVE_LINUX_ZFTAPE_H
	if (isa_zftape) {
	    errstr = tapefd_wrlabel(fd, "X", label,
				    (tt_blocksize_kb * 1024));
	    if(errstr != NULL) {
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	}
	else
#endif /* HAVE_LINUX_ZFTAPE_H */
	errstr = tape_wrlabel(tapename, "X", label,
			      (tt_blocksize_kb * 1024));
	if(errstr != NULL) {
	    putchar('\n');
	    error(errstr);
	    /*NOTREACHED*/
	}

#ifdef HAVE_LINUX_ZFTAPE_H
	if (isa_zftape) {
	    tapefd_weof(fd, (off_t)1);
	}
#endif /* HAVE_LINUX_ZFTAPE_H */

#ifdef HAVE_LINUX_ZFTAPE_H
	if (isa_zftape) {
	    errstr = tapefd_wrendmark(fd, "X",
			              (tt_blocksize_kb * 1024));
	    if(errstr != NULL) {
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	}
	else
#endif /* HAVE_LINUX_ZFTAPE_H */
	errstr = tape_wrendmark(tapename, "X",
			        (tt_blocksize_kb * 1024));
	if(errstr != NULL) {
	    putchar('\n');
	    error(errstr);
	    /*NOTREACHED*/
	}

#ifdef HAVE_LINUX_ZFTAPE_H
	if (isa_zftape) {
	    tapefd_weof(fd, (off_t)1);

	    printf(",\nrewinding"); fflush(stdout); 
     
	    if(tapefd_rewind(fd) == -1) { 
		putchar('\n'); 
		error(strerror(errno)); 
		/*NOTREACHED*/
	    } 
	    close(fd);
#ifdef HAVE_LIBVTBLC
	    /* update volume table */
	    printf(", updating volume table"); fflush(stdout);
    
	    if ((fd = raw_tape_open(rawtapedev, O_RDWR)) == -1) {
		if(errno == EACCES) {
		    errstr = newstralloc(errstr,
					 "updating volume table: raw tape device is write protected");
		} else {
		    errstr = newstralloc2(errstr,
					  "updating volume table: ", strerror(errno));
		}
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	    /* read volume table */
	    if ((num_volumes = read_vtbl(fd, volumes, vtbl_buffer,
					 &first_seg, &last_seg)) == -1 ) {
		errstr = newstralloc2(errstr,
				      "reading volume table: ", strerror(errno));
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	    /* set date and volume label for first entry */
	    vtbl_no = 0;
	    datestr = NULL; 
	    if (set_date(datestr, volumes, num_volumes, vtbl_no)){
		errstr = newstralloc2(errstr,
				      "setting date for entry 1: ", strerror(errno));
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	    if(set_label(label, volumes, num_volumes, vtbl_no)){
		errstr = newstralloc2(errstr,
				      "setting label for entry 1: ", strerror(errno));
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	    /* set date and volume label for last entry */
	    vtbl_no = 1;
	    datestr = NULL; 
	    if (set_date(datestr, volumes, num_volumes, vtbl_no)){
		errstr = newstralloc2(errstr,
				      "setting date for entry 2: ", strerror(errno));
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	    if(set_label("AMANDA Tape End", volumes, num_volumes, vtbl_no)){
		errstr = newstralloc2(errstr,
				      "setting label for entry 2: ", strerror(errno));
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }
	    /* write volume table back */
	    if (write_vtbl(fd, volumes, vtbl_buffer, num_volumes, first_seg,
			   op_mode == trunc)) {
		errstr = newstralloc2(errstr,
				      "writing volume table: ", strerror(errno));
		putchar('\n');
		error(errstr);
		/*NOTREACHED*/
	    }  
	    close(fd);
#endif /* HAVE_LIBVTBLC */
	}
#endif /* HAVE_LINUX_ZFTAPE_H */

	if (tape_ok) {
	    printf(", checking label"); fflush(stdout);

	    if((errstr = tape_rdlabel(tapename, &olddatestamp, &oldlabel)) != NULL) {
		putchar('\n');
		if (strcmp(errstr, "not an amanda tape") != 0) {
		    error(errstr);
		    /*NOTREACHED*/
		}
		error("no label found, are you sure %s is non-rewinding?",
		      tapename);
	        /*NOTREACHED*/
	    }

	    if (strcmp("X", olddatestamp) != 0 ||
		(strcmp(oldlabel, FAKE_LABEL) != 0
		 && strcmp(label, oldlabel) != 0)) {
		putchar('\n');
		error("read label %s back, timestamp %s (expected X), what now?",
		      oldlabel, olddatestamp);
	        /*NOTREACHED*/
	    }
	    amfree(oldlabel);
	    amfree(olddatestamp);

	    /* write tape list */

	    /* make a copy */
       	    conf_tapelist_old = stralloc2(conf_tapelist, ".amlabel");
	    if(write_tapelist(conf_tapelist_old)) {
	        error("couldn't write tapelist: %s", strerror(errno));
		/*NOTREACHED*/
	    }
	    amfree(conf_tapelist_old);

    	    /* XXX add cur_tape number to tape list structure */
	    remove_tapelabel(label);
    	    add_tapelabel("0", label);
	    if(write_tapelist(conf_tapelist)) {
	        error("couldn't write tapelist: %s", strerror(errno));
		/*NOTREACHED*/
	    }
	} /* write tape list */
	printf(", done.\n");
    } else {
	printf("\ntape not labeled\n");
    }

    clear_tapelist();
    free_new_argv(new_argc, new_argv);
    free_server_config();
    amfree(outslot);
    amfree(tapename);
    amfree(conffile);
    amfree(conf_tapelist);
    amfree(config_dir);
    config_name=NULL;
    dbclose();

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    return 0;
}
