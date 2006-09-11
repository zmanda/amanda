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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: tapetype.c,v 1.27 2006/08/24 01:57:16 paddy_s Exp $
 *
 * tests a tape in a given tape unit and prints a tapetype entry for
 * it.  */
#include "amanda.h"

#include "tapeio.h"

#define NBLOCKS 32			/* number of random blocks */

extern int optind;

static char *sProgName;
static char *tapedev;
static int fd;

static size_t blockkb = 32;
static size_t blocksize;

static char *randombytes = (char *) NULL;

#if USE_RAND
/* If the C library does not define random(), try to use rand() by
   defining USE_RAND, but then make sure you are not using hardware
   compression, because the low-order bits of rand() may not be that
   random... :-( */
#define random() rand()
#define srandom(seed) srand(seed)
#endif

static char *getrandombytes(void);
static int writeblock(int fd);
static off_t writeblocks(int fd, off_t nblks);
static void allocrandombytes(void);
static void do_pass0(off_t size, time_t *seconds, int dorewind);
static void do_pass(off_t size, off_t *blocks, off_t *files, time_t *seconds);
static void help(void);
static void initnotrandombytes(void);
static void initrandombytes(void);
static void show_progress(off_t *blocks, off_t *files);
static void usage(void);

int main(int argc, char **argv);

static void
allocrandombytes(void)
{
  size_t i;
  size_t j;
  size_t page_size;
  char   *p;

  if (randombytes == (char *)NULL) {
#if defined(HAVE_GETPAGESIZE)
    page_size = (size_t)getpagesize();
#else
    page_size = (size_t)1024;
#endif
    j = (NBLOCKS * blocksize) + page_size;	/* buffer space plus one page */
    j = am_round(j, page_size);			/* even number of pages */
    p = (char *)alloc(j);
    i = (size_t)(p - (char *)0) & (page_size - 1);/* page boundary offset */
    if(i != 0) {
      randombytes = p + (page_size - i);	/* round up to page boundary */
    } else {
      randombytes = p;				/* alloc already on boundary */
    }
  }
}

static void
initnotrandombytes(void)
{
  unsigned long i;
  unsigned long j;
  char *p;

  allocrandombytes();
  j = NBLOCKS * blocksize;
  p = randombytes;
  for(i=0; i < j; ++i) {
    *p++ = (char) (i % 256);
  }
}

static void
initrandombytes(void)
{
  unsigned long i, j;
  char *p;

  allocrandombytes();
  j = NBLOCKS * blocksize;
  p = randombytes;
  for(i=0; i < j; ++i) {
    *p++ = (char)random();
  }
}

static char *
getrandombytes(void)
{
  static unsigned long counter = 0;

  return randombytes + ((counter++ % NBLOCKS) * blocksize);
}

static int short_write;

static int
writeblock(
    int		fd)
{
  ssize_t w;

  if ((w = tapefd_write(fd, getrandombytes(), blocksize)) == (ssize_t)blocksize) {
    return 1;
  }
  if (w >= 0) {
    short_write = 1;
  } else {
    short_write = 0;
  }
  return 0;
}


/* returns number of blocks actually written */
static off_t
writeblocks(
    int		fd,
    off_t	nblks)
{
  off_t blks = (off_t)0;

  while (blks < nblks) {
    if (! writeblock(fd)) {
      return (off_t)0;
    }
    blks += (off_t)1;
  }

  return blks;
}


static void
usage(void)
{
  fputs("usage: ", stderr);
  fputs(sProgName, stderr);
  fputs(" [-h]", stderr);
  fputs(" [-c]", stderr);
  fputs(" [-o]", stderr);
  fputs(" [-b blocksize]", stderr);
  fputs(" -e estsize", stderr);
  fputs(" [-f tapedev]", stderr);
  fputs(" [-t typename]", stderr);
  fputc('\n', stderr);
}

static void
help(void)
{
  usage();
  fputs("-h			display this message\n"
  	"-c			run hardware compression detection test only\n"
  	"-o			overwrite amanda tape\n"
  	"-b blocksize		record block size (default: 32k)\n"
  	"-e estsize		estimated tape size (No default!)\n"
  	"-f tapedev		tape device name (default: $TAPE)\n"
  	"-t typename		tapetype name (default: unknown-tapetype)\n"
  	"\n"
	"Note: disable hardware compression when running this program.\n",
	stderr);
}


int do_tty;

static void
show_progress(
    off_t *	blocks,
    off_t *	files)
{
  fprintf(stderr, "wrote " OFF_T_FMT " " SIZE_T_FMT "Kb block%s in " OFF_T_FMT " file%s",
	  (OFF_T_FMT_TYPE)*blocks,
	  (SIZE_T_FMT_TYPE)blockkb, (*blocks == (off_t)1) ? "" : "s",
	  (OFF_T_FMT_TYPE)*files, (*files == (off_t)1) ? "" : "s");
}


static void
do_pass(
    off_t	size,
    off_t *	blocks,
    off_t *	files,
    time_t *	seconds)
{
  off_t blks;
  time_t start, end;
  int save_errno;

  if (tapefd_rewind(fd) == -1) {
    fprintf(stderr, "%s: could not rewind %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    exit(1);
  }

  time(&start);

  while(1) {

    if ((blks = writeblocks(fd, size)) <= (off_t)0 || tapefd_weof(fd, (off_t)1) != 0)
      break;
    *blocks += blks;
    *files += (off_t)1;
    if(do_tty) {
      putc('\r', stderr);
      show_progress(blocks, files);
    }
  }
  save_errno = errno;

  time(&end);

  if (*blocks == (off_t)0) {
    fprintf(stderr, "%s: could not write any data in this pass: %s\n",
	    sProgName, short_write ? "short write" : strerror(save_errno));
    exit(1);
  }

  if(end <= start) {
    /*
     * Just in case time warped backward or the device is really, really
     * fast (e.g. /dev/null testing).
     */
    *seconds = 1;
  } else {
    *seconds = end - start;
  }
  if(do_tty) {
    putc('\r', stderr);
  }
  show_progress(blocks, files);
  fprintf(stderr, " in " TIME_T_FMT " second%s (%s)\n",
	  (TIME_T_FMT_TYPE)*seconds, (*seconds == 1) ? "" : "s",
	  short_write ? "short write" : strerror(save_errno));
}


static void
do_pass0(
    off_t	size,
    time_t *	seconds,
    int		dorewind)
{
  off_t blks;
  time_t start, end;
  int save_errno;

  if (dorewind  &&  tapefd_rewind(fd) == -1) {
    fprintf(stderr, "%s: could not rewind %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    exit(1);
  }

  time(&start);

  blks = writeblocks(fd, size);
  tapefd_weof(fd, (off_t)1);

  save_errno = errno;

  time(&end);

  if (blks <= (off_t)0) {
    fprintf(stderr, "%s: could not write any data in this pass: %s\n",
	    sProgName, short_write ? "short write" : strerror(save_errno));
    exit(1);
  }

  if(end <= start) {
    /*
     * Just in case time warped backward or the device is really, really
     * fast (e.g. /dev/null testing).
     */
    *seconds = 1;
  } else {
    *seconds = end - start;
  }
}


int
main(
    int		argc,
    char **	argv)
{
  off_t pass1blocks = (off_t)0;
  off_t pass2blocks = (off_t)0;
  time_t pass1time;
  time_t pass2time;
  time_t timediff;
  off_t pass1files = (off_t)0;
  off_t pass2files = (off_t)0;
  off_t estsize;
  off_t pass0size;
  off_t pass1size;
  off_t pass2size;
  off_t blockdiff;
  off_t filediff;
  size_t filemark;
  unsigned long speed;
  off_t size;
  char *sizeunits;
  int ch;
  char *suffix = NULL;
  char *typename;
  time_t now;
  int hwcompr = 0;
  int comprtstonly = 0;
  int overwrite_label = 0;
  int is_labeled = 0;
  char *result;
  char *datestamp = NULL;
  char *label = NULL;


  if ((sProgName = strrchr(*argv, '/')) == NULL) {
    sProgName = *argv;
  } else {
    sProgName++;
  }

  /* Don't die when child closes pipe */
  signal(SIGPIPE, SIG_IGN);

  estsize = (off_t)0;
  tapedev = getenv("TAPE");
  typename = "unknown-tapetype";

  while ((ch = getopt(argc, argv, "b:e:f:t:hco")) != EOF) {
    switch (ch) {
    case 'b':
      blockkb = (size_t)strtol(optarg, &suffix, 0);
      if (!(*suffix == '\0' || *suffix == 'k' || *suffix == 'K')) {
	if (*suffix == 'm' || *suffix == 'M') {
	  blockkb *= 1024;
	} else if (*suffix == 'g' || *suffix == 'G') {
	  blockkb *= (1024 * 1024);
	} else {
	  fprintf(stderr, "%s: unknown size suffix \'%c\'\n",
		  sProgName, *suffix);
	  return 1;
	}
      }
      break;
    case 'e':
      estsize = OFF_T_STRTOL(optarg, &suffix, 0);
      if (!(*suffix == '\0' || *suffix == 'k' || *suffix == 'K')) {
	if (*suffix == 'm' || *suffix == 'M') {
	  estsize *= (off_t)1024;
	} else if (*suffix == 'g' || *suffix == 'G') {
	  estsize *= (off_t)(1024 * 1024);
	} else {
	  fprintf(stderr, "%s: unknown size suffix \'%c\'\n",
		  sProgName, *suffix);
	  return 1;
	}
      }
      break;

    case 'f':
      tapedev = stralloc(optarg);
      break;

    case 't':
      typename = stralloc(optarg);
      break;

    case 'c':
      comprtstonly = 1;
      break;

    case 'h':
      help();
      return 1;

    case 'o':
      overwrite_label=1;
      break;

    default:
      fprintf(stderr, "%s: unknown option \'%c\'\n", sProgName, ch);
      /*FALLTHROUGH*/

    case '?':
      usage();
      return 1;
    }
  }
  blocksize = blockkb * 1024;

  if (tapedev == NULL) {
    fprintf(stderr, "%s: No tapedev specified\n", sProgName);
    usage();
    return 1;
  }
  if (optind < argc) {
    usage();
    return 1;
  }

  if (estsize == 0) {
    if (comprtstonly) {
      estsize = (off_t)(1024 * 1024);		/* assume 1 GByte for now */
    } else {
      fprintf(stderr, "%s: please specify estimated tape capacity (e.g. '-e 4g')\n", sProgName);
      usage();
      return 1;
    }
  }


/* verifier tape */


  fd = tape_open(tapedev, O_RDONLY, 0);
  if (fd == -1) {
    fprintf(stderr, "%s: could not open %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    return 1;
  }

  if((result = tapefd_rdlabel(fd, &datestamp, &label)) == NULL) {
    is_labeled = 1;
  }
  else if (strcmp(result,"not an amanda tape") == 0) {
    is_labeled = 2;
  }

  if(tapefd_rewind(fd) == -1) {
    fprintf(stderr, "%s: could not rewind %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    tapefd_close(fd);
    return 1;
  }

  tapefd_close(fd);

  if(is_labeled == 1 && overwrite_label == 0) {
    fprintf(stderr, "%s: The tape is an amanda tape, use -o to overwrite the tape\n",
	    sProgName);
    return 1;
  }
  else if(is_labeled == 2 && overwrite_label == 0) {
    fprintf(stderr, "%s: The tape is already used, use -o to overwrite the tape\n",
	    sProgName);
    return 1;
  }

  fd = tape_open(tapedev, O_RDWR, 0);
  if (fd == -1) {
    fprintf(stderr, "%s: could not open %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    return 1;
  }

  do_tty = isatty(fileno(stderr));

  /*
   * Estimate pass: write twice a small file, once with compressable
   * data and once with uncompressable data.
   * The theory is that if the drive is in hardware compression mode
   * we notice a significant difference in writing speed between the two
   * (at least if we can provide data as fast the tape streams).
   */

  initnotrandombytes();

  fprintf(stderr, "Estimate phase 1...");
  pass0size = (off_t)(8 * 1024 / blockkb);
  pass1time = 0;
  pass2time = 0;
  /*
   * To get accurate results, we should write enough data
   * so that rewind/start/stop time is small compared to
   * the total time; let's take 10%.
   * The timer has a 1 sec granularity, so the test
   * should take at least 10 seconds to measure a
   * difference with 10% accuracy; let's take 25 seconds.
   */ 
  while (pass1time < 25 || ((100*(pass2time-pass1time)/pass2time) >= 10) ) {
    if (pass1time != 0) {
      time_t t = pass1time;
      do {
	  pass0size *= (off_t)2;
	  t *= 2;
      } while (t < 25);
    }
    /*
     * first a dummy pass to rewind, stop, start and
     * get drive streaming, then do the real timing
     */
    do_pass0(pass0size, &pass2time, 1);
    do_pass0(pass0size, &pass1time, 0);
    if (pass0size >= (off_t)(10 * 1024 * 1024)) {
      fprintf(stderr,
	"\rTape device is too fast to detect hardware compression...\n");
      break;	/* avoid loops if tape is superfast or broken */
    }
  }
  fprintf(stderr,
	"\rWriting " OFF_T_FMT " Mbyte   compresseable data:  "
	TIME_T_FMT " sec\n",
	(OFF_T_FMT_TYPE)((off_t)blockkb * pass0size / (off_t)1024),
	(TIME_T_FMT_TYPE)pass1time);

  /*
   * now generate uncompressable data and try again
   */
  time(&now);
  srandom((unsigned)now);
  initrandombytes();

  fprintf(stderr, "Estimate phase 2...");
  do_pass0(pass0size, &pass2time, 1);	/* rewind and get drive streaming */
  do_pass0(pass0size, &pass2time, 0);
  fprintf(stderr, "\rWriting " OFF_T_FMT
	" Mbyte uncompresseable data:  " TIME_T_FMT " sec\n",
	(OFF_T_FMT_TYPE)((off_t)blockkb * pass0size / (off_t)1024),
	(TIME_T_FMT_TYPE)pass2time);

  /*
   * Compute the time difference between writing the compressable and
   * uncompressable data.  If it differs more than 20%, then warn
   * user that the tape drive has probably hardware compression enabled.
   */
  if (pass1time > pass2time) {
    /*
     * Strange!  I would expect writing compresseable data to be
     * much faster (or about equal, if hardware compression is disabled)
     */
    timediff = 0;
  } else {
    timediff = pass2time - pass1time;
  }
  if (((100 * timediff) / pass2time) >= 20) {	/* 20% faster? */
    fprintf(stderr, "WARNING: Tape drive has hardware compression enabled\n");
    hwcompr = 1;
  }

  /*
   * Inform about estimated time needed to run the remaining of this program
   */
  fprintf(stderr, "Estimated time to write 2 * %lu Mbyte: ", (unsigned long) (estsize / (off_t)1024));
  pass1time = (time_t)(2.0 * (double)pass2time * (double)estsize /
		(1.0 * (double)pass0size * (double)blockkb));
	/* avoid overflow and underflow by doing math in floating point */
  fprintf(stderr, TIME_T_FMT " sec = ",
	  (TIME_T_FMT_TYPE)pass1time);
  fprintf(stderr, TIME_T_FMT " h " TIME_T_FMT " min\n",
	  (TIME_T_FMT_TYPE)(pass1time/(time_t)3600),
	  (TIME_T_FMT_TYPE)((pass1time%(time_t)3600) / (time_t)60));

  if (comprtstonly) {
	exit(hwcompr);
  }


  /*
   * Do pass 1 -- write files that are 1% of the estimated size until error.
   */
  pass1size = (off_t)(((double)estsize * 0.01) / (double)blockkb); /* 1% of estimate */
  if(pass1size <= (off_t)0) {
    pass1size = (off_t)2;				/* strange end case */
  }
  do_pass(pass1size, &pass1blocks, &pass1files, &pass1time);

  /*
   * Do pass 2 -- write smaller files until error.
   */
  pass2size = pass1size / (off_t)2;
  do_pass(pass2size, &pass2blocks, &pass2files, &pass2time);

  /*
   * Compute the size of a filemark as the difference in data written
   * between pass 1 and pass 2 divided by the difference in number of
   * file marks written between pass 1 and pass 2.  Note that we have
   * to be careful in case size_t is unsigned (i.e. do not subtract
   * things and then check for less than zero).
   */
  if (pass1blocks <= pass2blocks) {
    /*
     * If tape marks take up space, there should be fewer blocks in pass
     * 2 than in pass 1 since we wrote twice as many tape marks.  But
     * odd things happen, so make sure the result does not go negative.
     */
    blockdiff = (off_t)0;
  } else {
    blockdiff = pass1blocks - pass2blocks;
  }
  if (pass2files <= pass1files) {
    /*
     * This should not happen, but just in case ...
     */
    filediff = (off_t)1;
  } else {
    filediff = pass2files - pass1files;
  }
  filemark = (size_t)((blockdiff * (off_t)blockkb) / filediff);

  /*
   * Compute the length as the average of the two pass sizes including
   * tape marks.
   */
  size = ((pass1blocks * (off_t)blockkb + (off_t)filemark * pass1files)
           + (pass2blocks * (off_t)blockkb + (off_t)filemark * pass2files))
	   / (off_t)2;
  if (size >= (off_t)(1024 * 1024 * 1000)) {
    size /= (off_t)(1024 * 1024);
    sizeunits = "gbytes";
  } else if (size >= (off_t)(1024 * 1000)) {
    size /= (off_t)1024;
    sizeunits = "mbytes";
  } else {
    sizeunits = "kbytes";
  }

  /*
   * Compute the speed as the average of the two passes.
   */
  speed = (unsigned long)((((double)pass1blocks
	   * (double)blockkb / (double)pass1time)
           + ((double)pass2blocks * (double)blockkb / (double)pass2time)) / 2.0);

  /*
   * Dump the tapetype.
   */
  printf("define tapetype %s {\n", typename);
  printf("    comment \"just produced by tapetype prog (hardware compression %s)\"\n",
	hwcompr ? "on" : "off");
  printf("    length " OFF_T_FMT " %s\n", (OFF_T_FMT_TYPE)size, sizeunits);
  printf("    filemark " SIZE_T_FMT " kbytes\n", (SIZE_T_FMT_TYPE)filemark);
  printf("    speed %lu kps\n", speed);
  printf("}\n");

  if (tapefd_rewind(fd) == -1) {
    fprintf(stderr, "%s: could not rewind %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    return 1;
  }

  if (tapefd_close(fd) == -1) {
    fprintf(stderr, "%s: could not close %s: %s\n",
	    sProgName, tapedev, strerror(errno));
    return 1;
  }

  return 0;
}
