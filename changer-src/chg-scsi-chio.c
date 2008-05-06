/*
 *  $Id: chg-scsi-chio.c,v 1.12 2006/07/25 18:18:46 martinea Exp $
 *
 *  chg-scsi-chio.c -- generic SCSI changer driver
 *
 *  This program provides a driver to control generic
 *  SCSI changers, no matter what platform.  The host/OS
 *  specific portions of the interface are implemented
 *  in libscsi.a, which contains a module for each host/OS.
 *  The actual interface for HP/UX is in scsi-hpux.c;
 *  chio is in scsi-chio.c, etc..  A prototype system
 *  dependent scsi interface file is in scsi-proto.c.
 *
 *  Copyright 1997, 1998 Eric Schnoebelen <eric@cirr.com>
 *
 * This module based upon seagate-changer, by Larry Pyeatt
 *                  <pyeatt@cs.colostate.edu>
 *
 * The original introductory comments follow:
 *
 * This program was written to control the Seagate/Conner/Archive
 * autoloading DAT drive.  This drive normally has 4 tape capacity
 * but can be expanded to 12 tapes with an optional tape cartridge.
 * This program may also work on onther drives.  Try it and let me
 * know of successes/failures.
 *
 * I have attempted to conform to the requirements for Amanda tape
 * changer interface.  There could be some bugs.  
 *
 * This program works for me under Linux with Gerd Knorr's 
 * <kraxel@cs.tu-berlin.de> SCSI media changer driver installed 
 * as a kernel module.  The kernel module is available at 
 * http://sunsite.unc.edu/pub/Linux/kernel/patches/scsi/scsi-changer*
 * Since the Linux media changer is based on NetBSD, this program
 * should also work for NetBSD, although I have not tried it.
 * It may be necessary to change the IOCTL calls to work on other
 * OS's.  
 *
 * (c) 1897 Larry Pyeatt,  pyeatt@cs.colostate.edu 
 * All Rights Reserved.
 * 
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation.  The author makes no representations about the
 * suitability of this software for any purpose.   It is provided "as is"
 * without express or implied warranty.
 *
 * Michael C. Povel 03.06.98 added ejetct_tape and sleep for external tape
 * devices, and changed some code to allow multiple drives to use their
 * own slots. Also added support for reserverd slots.
 * At the moment these parameters are hard coded and only tested under Linux
 * 
 */

#include "config.h"
#include "amanda.h"
#include "conffile.h"
#include "libscsi.h"
#include "scsi-defs.h"
int Tape_Ready1 ( char *tapedev , int wait);

char *tapestatfile = NULL;

/*----------------------------------------------------------------------------*/

typedef enum{
  NUMDRIVE,EJECT,SLEEP,CLEANMAX,DRIVE,START,END,CLEAN,DEVICE,STATFILE,CLEANFILE,DRIVENUM,
    CHANGERDEV,USAGECOUNT,SCSITAPEDEV, TAPESTATFILE
    } token_t;

typedef struct {
  char *word;
  token_t token;
} tokentable_t;

tokentable_t t_table[]={
  { "number_configs",	NUMDRIVE},
  { "eject",		EJECT},
  { "sleep",		SLEEP},
  { "cleanmax",		CLEANMAX},
  { "config",		DRIVE},
  { "startuse",		START},
  { "enduse",		END},
  { "cleancart",	CLEAN},
  { "dev",		DEVICE},
  { "statfile",		STATFILE},
  { "cleanfile",	CLEANFILE},
  { "drivenum",		DRIVENUM},
  { "changerdev",	CHANGERDEV},
  { "usagecount",	USAGECOUNT},
  { "scsitapedev",	SCSITAPEDEV},
  { "tapestatus",	TAPESTATFILE},
  { NULL,		-1 }
};

changer_t *changer;

void	init_changer_struct(changer_t *chg, size_t number_of_config);
void	dump_changer_struct(changer_t *chg);
void	free_changer_struct(changer_t **changer);
void	parse_line(char *linebuffer,int *token,char **value);
int	read_config(char *configfile, changer_t *chg);
int	get_current_slot(char *count_file);
void	put_current_slot(char *count_file,int slot);
void	usage(char *argv[]);
int	get_relative_target(int fd,int nslots,char *parameter,int loaded, 
                        char *changer_file,int slot_offset,int maxslot);
int	is_positive_number(char *tmp);
int	ask_clean(char *tapedev);
void	clean_tape(int fd,char *tapedev,char *cnt_file, int drivenum, 
                int cleancart, int maxclean,char *usagetime);
int	main(int argc, char *argv[]);


/*
 * Initialize data structures with default values
*/
void
init_changer_struct(
    changer_t *	chg,
    size_t	number_of_config)
{
  size_t i;
 
  memset(chg, 0, SIZEOF(*chg));
  chg->number_of_configs = number_of_config;
  chg->eject = 1;
  chg->sleep = 0;
  chg->cleanmax = 0;
  chg->device = NULL;
  chg->conf = alloc(SIZEOF(config_t) * number_of_config);
  for (i=0; i < number_of_config; i++){
    chg->conf[i].drivenum     = 0;
    chg->conf[i].start        = -1;
    chg->conf[i].end          = -1;
    chg->conf[i].cleanslot    = -1;
    chg->conf[i].device       = NULL;
    chg->conf[i].slotfile     = NULL;
    chg->conf[i].cleanfile    = NULL;
    chg->conf[i].timefile     = NULL;
    chg->conf[i].scsitapedev  = NULL;
    chg->conf[i].tapestatfile = NULL;
    chg->conf[i].changerident = NULL;
    chg->conf[i].tapeident    = NULL;
  }
}

/*
 * Dump of information for debug
*/
void
dump_changer_struct(
    changer_t *	chg)
{
  int i;

  dbprintf(_("Number of configurations: %d\n"), chg->number_of_configs);
  dbprintf(_("Tapes need eject: %s\n"), (chg->eject>0 ? _("Yes") : _("No")));
  dbprintf(_("Tapes need sleep: %lld seconds\n"), (long long)chg->sleep);
  dbprintf(_("Clean cycles    : %d\n"), chg->cleanmax);
  dbprintf(_("Changer device  : %s\n"), chg->device);
  for (i = 0; i < chg->number_of_configs; i++){
    dbprintf(_("Tape config Nr: %d\n"), i);
    dbprintf(_("  Drive number  : %d\n"), chg->conf[i].drivenum);
    dbprintf(_("  Start slot    : %d\n"), chg->conf[i].start);
    dbprintf(_("  End slot      : %d\n"), chg->conf[i].end);
    dbprintf(_("  Clean slot    : %d\n"), chg->conf[i].cleanslot);
    if (chg->conf[i].device != NULL)
      dbprintf(_("  Device name   : %s\n"), chg->conf[i].device);
    else
      dbprintf(_("  Device name   : none\n"));
    if (chg->conf[i].scsitapedev != NULL)
      dbprintf(_("  SCSI Tape dev : %s\n"), chg->conf[i].scsitapedev);
    else
      dbprintf(_("  SCSI Tape dev : none\n"));
    if (chg->conf[i].tapestatfile != NULL)
      dbprintf(_("  stat file     : %s\n"), chg->conf[i].tapestatfile);
    else
      dbprintf(_("  stat file     : none\n"));
    if (chg->conf[i].slotfile != NULL)
      dbprintf(_("  Slot file     : %s\n"), chg->conf[i].slotfile);
    else
      dbprintf(_("  Slot file     : none\n"));
    if (chg->conf[i].cleanfile != NULL)
      dbprintf(_("  Clean file    : %s\n"), chg->conf[i].cleanfile);
    else
      dbprintf(_("  Clean file    : none\n"));
    if (chg->conf[i].timefile != NULL)
      dbprintf(_("  Usage count   : %s\n"), chg->conf[i].timefile);
    else
      dbprintf(_("  Usage count   : none\n"));
  }
}

/*
 * Free all allocated memory
 */
void
free_changer_struct(
changer_t **changer)
{
  changer_t *chg;
  int i;

  assert(changer != NULL);
  assert(*changer != NULL);

  chg = *changer;
  if (chg->device != NULL)
    amfree(chg->device);
  for (i=0; i<chg->number_of_configs; i++){
    if (chg->conf[i].device != NULL)
      amfree(chg->conf[i].device);
    if (chg->conf[i].slotfile != NULL)
      amfree(chg->conf[i].slotfile);
    if (chg->conf[i].cleanfile != NULL)
      amfree(chg->conf[i].cleanfile);
    if (chg->conf[i].timefile != NULL)
      amfree(chg->conf[i].timefile);
  }
  if (chg->conf != NULL)
    amfree(chg->conf);
  chg->conf = NULL;
  chg->device = NULL;
  amfree(*changer);
}

/*
 * This function parses a line, and returns a token and value
 */
void
parse_line(
    char *	linebuffer,
    int *	token,
    char **	value)
{
  char *tok;
  int i;
  int ready = 0;
  *token = -1;  /* No Token found */
  tok=strtok(linebuffer," \t\n");

  while ((tok != NULL) && (tok[0]!='#')&&(ready==0)){
    if (*token != -1){
      *value=tok;
      ready=1;
    } else {
      i=0;
      while ((t_table[i].word != NULL)&&(*token==-1)){
        if (0==strcasecmp(t_table[i].word,tok)){
          *token=t_table[i].token;
        }
        i++;
      }
    }
    tok=strtok(NULL," \t\n");
  }
  return;
}

/*
 * This function reads the specified configfile and fills the structure
*/
int
read_config(
    char *	configfile,
    changer_t *	chg)
{
  size_t numconf;
  FILE *file;
  int init_flag = 0;
  size_t drivenum=0;
  char *linebuffer;
  int token;
  char *value;

  numconf = 1;  /* At least one configuration is assumed */
  /* If there are more, it should be the first entry in the configurationfile */

  if (NULL==(file=fopen(configfile,"r"))){
    return (-1);
  }

  while (NULL != (linebuffer = agets(file))) {
      if (linebuffer[0] == '\0') {
	amfree(linebuffer);
	continue;
      }
      parse_line(linebuffer,&token,&value);
      if (token != -1){
        if (0==init_flag) {
          if (token != NUMDRIVE){
            init_changer_struct(chg, numconf);
          } else {
            numconf = atoi(value);
            init_changer_struct(chg, numconf);
          }
          init_flag=1;
        }
        switch (token){
        case NUMDRIVE: if ((size_t)atoi(value) != numconf)
          g_fprintf(stderr,_("Error: number_drives at wrong place, should be "
                  "first in file\n"));
        break;
        case EJECT:
          chg->eject = atoi(value);
          break;
        case SLEEP:
          chg->sleep = atoi(value);
          break;
        case CHANGERDEV:
          chg->device = stralloc(value);
          break;
        case SCSITAPEDEV:
          chg->conf[drivenum].scsitapedev = stralloc(value);
          break;
        case TAPESTATFILE:
          chg->conf[drivenum].tapestatfile = stralloc(value);
          break;
        case CLEANMAX:
          chg->cleanmax = atoi(value);
          break;
        case DRIVE:
          drivenum = atoi(value);
          if(drivenum >= numconf){
            g_fprintf(stderr,_("Error: drive must be less than number_drives\n"));
          }
          break;
        case DRIVENUM:
          if (drivenum < numconf){
            chg->conf[drivenum].drivenum = atoi(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " drivenum ignored\n"));
          }
          break;
        case START:
          if (drivenum < numconf){
            chg->conf[drivenum].start = atoi(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " startuse ignored\n"));
          }
          break;
        case END:
          if (drivenum < numconf){
            chg->conf[drivenum].end = atoi(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " enduse ignored\n"));
          }
          break;
        case CLEAN:
          if (drivenum < numconf){
            chg->conf[drivenum].cleanslot = atoi(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " cleanslot ignored\n"));
          }
          break;
        case DEVICE:
          if (drivenum < numconf){
            chg->conf[drivenum].device = stralloc(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " device ignored\n"));
          }
          break;
        case STATFILE:
          if (drivenum < numconf){
            chg->conf[drivenum].slotfile = stralloc(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " slotfile ignored\n"));
          }
          break;
        case CLEANFILE:
          if (drivenum < numconf){
            chg->conf[drivenum].cleanfile = stralloc(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " cleanfile ignored\n"));
          }
          break;
        case USAGECOUNT:
          if (drivenum < numconf){
            chg->conf[drivenum].timefile = stralloc(value);
          } else {
            g_fprintf(stderr,_("Error: drive is not less than number_drives"
                    " usagecount ignored\n"));
          }
          break;
        default:
          g_fprintf(stderr,_("Error: Unknown token\n"));
          break;
        }
      }
    amfree(linebuffer);
  }
  amfree(linebuffer);

  fclose(file);
  return 0;
}

/*----------------------------------------------------------------------------*/

/*  The tape drive does not have an idea of current slot so
 *  we use a file to store the current slot.  It is not ideal
 *  but it gets the job done  
 */
int get_current_slot(char *count_file)
{
  FILE *inf;
  int retval;
  if ((inf=fopen(count_file,"r")) == NULL) {
    g_fprintf(stderr, _("%s: unable to open current slot file (%s)\n"),
            get_pname(), count_file);
    return 0;
  }

  if (fscanf(inf, "%d", &retval) != 1) {
    g_fprintf(stderr, _("%s: unable to read current slot file (%s)\n"),
            get_pname(), count_file);
    retval = 0;
  }

  fclose(inf);
  return retval;
}

void put_current_slot(char *count_file,int slot)
{
  FILE *inf;

  if ((inf=fopen(count_file,"w")) == NULL) {
    g_fprintf(stderr, _("%s: unable to open current slot file (%s)\n"),
            get_pname(), count_file);
    exit(2);
  }
  g_fprintf(inf, "%d\n", slot);
  fclose(inf);
}

/* ---------------------------------------------------------------------- 
   This stuff deals with parsing the command line */

typedef struct com_arg
{
  char *str;
  int command_code;
  int takesparam;
} argument;


typedef struct com_stru
{
  int command_code;
  char *parameter;
} command;


void	parse_args(int argc, char *argv[], command *rval);

/* major command line args */
#define COMCOUNT 5
#define COM_SLOT 0
#define COM_INFO 1
#define COM_RESET 2
#define COM_EJECT 3
#define COM_CLEAN 4
argument argdefs[]={{"-slot",COM_SLOT,1},
                    {"-info",COM_INFO,0},
                    {"-reset",COM_RESET,0},
                    {"-eject",COM_EJECT,0},
                    {"-clean",COM_CLEAN,0}};


/* minor command line args */
#define SLOTCOUNT 5
#define SLOT_CUR 0
#define SLOT_NEXT 1
#define SLOT_PREV 2
#define SLOT_FIRST 3
#define SLOT_LAST 4
argument slotdefs[]={{"current",SLOT_CUR,0},
                     {"next",SLOT_NEXT,0},
                     {"prev",SLOT_PREV,0},
                     {"first",SLOT_FIRST,0},
                     {"last",SLOT_LAST,0}};

int is_positive_number(char *tmp) /* is the string a valid positive int? */
{
  int i=0;
  if ((tmp==NULL)||(tmp[0]==0))
    return 0;
  while ((tmp[i]>='0')&&(tmp[i]<='9')&&(tmp[i]!=0))
    i++;
  if (tmp[i]==0)
    return 1;
  else
    return 0;
}

void usage(char *argv[])
{
  int cnt;
  g_printf(_("%s: Usage error.\n"), argv[0]);
  for (cnt=0; cnt < COMCOUNT; cnt++){
    g_printf("      %s    %s",argv[0],argdefs[cnt].str);
    if (argdefs[cnt].takesparam)
      g_printf(_(" <param>\n"));
    else
      g_printf("\n");
  }
  exit(2);
}


void parse_args(int argc, char *argv[],command *rval)
{
  int i=0;
  if ((argc < 2) || (argc > 3)) {
    usage(argv);
    /*NOTREACHED*/
  }

  while ((i<COMCOUNT)&&(strcmp(argdefs[i].str,argv[1])))
    i++;
  if (i == COMCOUNT) {
    usage(argv);
    /*NOTREACHED*/
  }
  rval->command_code = argdefs[i].command_code;
  if (argdefs[i].takesparam) {
    if (argc < 3) {
      usage(argv);
      /*NOTREACHED*/
    }
    rval->parameter=argv[2];      
  }
  else {
    if (argc > 2) {
      usage(argv);
      /*NOTREACHED*/
    }
    rval->parameter=0;
  }
}

/* used to find actual slot number from keywords next, prev, first, etc */
int
get_relative_target(
    int		fd,
    int		nslots,
    char *	parameter,
    int		loaded, 
    char *	changer_file,
    int		slot_offset,
    int		maxslot)
{
  int current_slot,i;

  (void)loaded;		/* Quiet unused warning */
  if (changer_file != NULL)
    {
      current_slot=get_current_slot(changer_file);
    } else {
      current_slot =   GetCurrentSlot(fd, 0);
    }
  if (current_slot > maxslot){
    current_slot = slot_offset;
  }
  if (current_slot < slot_offset){
    current_slot = slot_offset;
  }

  i=0;
  while((i < SLOTCOUNT) && (strcmp(slotdefs[i].str,parameter)))
    i++;

  switch(i) {
  case SLOT_CUR:
    break;

  case SLOT_NEXT:
    if (++current_slot==nslots+slot_offset)
      return slot_offset;
    break;

  case SLOT_PREV:
    if (--current_slot<slot_offset)
      return maxslot;
    break;

  case SLOT_FIRST:
    return slot_offset;

  case SLOT_LAST:
    return maxslot;

  default: 
    g_printf(_("<none> no slot `%s'\n"),parameter);
    close(fd);
    exit(2);
    /*NOTREACHED*/
  }
  return current_slot;
}

/*
 * This function should ask the drive if it wants to be cleaned
 */
int
ask_clean(
    char *	tapedev)
{
  return get_clean_state(tapedev);
}

/*
 * This function should move the cleaning cartridge into the drive
 */
void
clean_tape(
    int		fd,
    char *	tapedev,
    char *	cnt_file,
    int		drivenum,
    int		cleancart,
    int		maxclean,
    char *	usagetime)
{
  int counter;
  char *mailer;

  if (cleancart == -1 ){
    return;
  }

  mailer = getconf_str(CNF_MAILER);

  /* Now we should increment the counter */
  if (cnt_file != NULL){
    counter = get_current_slot(cnt_file);
    counter++;
    if (counter>=maxclean){
      /* Now we should inform the administrator */
      char *mail_cmd;
      FILE *mailf;
      int mail_pipe_opened = 1;
      if (mailer && *mailer != '\0') {
        if (getconf_seen(CNF_MAILTO) && strlen(getconf_str(CNF_MAILTO)) > 0 && 
           validate_mailto(getconf_str(CNF_MAILTO))) {
      	   mail_cmd = vstralloc(mailer,
                             " -s", " \"", _("AMANDA PROBLEM: PLEASE FIX"), "\"",
                             " ", getconf_str(CNF_MAILTO),
                             NULL);
      	   if ((mailf = popen(mail_cmd, "w")) == NULL) {
        	  g_printf(_("Mail failed\n"));
        	  error(_("could not open pipe to \"%s\": %s"),
                    	mail_cmd, strerror(errno));
        	  /*NOTREACHED*/
      	  }
        } else {
	   mail_pipe_opened = 0;
	   mailf = stderr;
           g_fprintf(mailf, _("\nNo mail recipient specified, output redirected to stderr"));
        }
      } else {
        mail_pipe_opened = 0;
        mailf = stderr;
        g_fprintf(mailf, _("\nNo mailer specified; output redirected to stderr"));
      }
      g_fprintf(mailf, _("\nThe usage count of your cleaning tape in slot %d"),
             cleancart);
      g_fprintf(mailf,_("\nis more than %d. (cleanmax)"),maxclean);
      g_fprintf(mailf,_("\nTapedrive %s needs to be cleaned"),tapedev);
      g_fprintf(mailf,_("\nPlease insert a new cleaning tape and reset"));
      g_fprintf(mailf,_("\nthe countingfile %s"),cnt_file);

      if(mail_pipe_opened == 1 && pclose(mailf) != 0) {
      	 error(_("mail command failed: %s"), mail_cmd);
		/*NOTREACHED*/
      }
      

      return;
    }
    put_current_slot(cnt_file,counter);
  }
  load(fd,drivenum,cleancart);
  
  if (drive_loaded(fd, drivenum))
    unload(fd,drivenum,cleancart);  
  unlink(usagetime);
}
/* ----------------------------------------------------------------------*/

int
main(
    int		argc,
    char **	argv)
{
  int loaded;
  int target = -1;
  int oldtarget;
  command com;   /* a little DOS joke */
  
  /*
   * drive_num really should be something from the config file, but..
   * for now, it is set to zero, since most of the common changers
   * used by amanda only have one drive ( until someone wants to 
   * use an EXB60/120, or a Breece Hill Q45.. )
   */
  int    drive_num = 0;
  int need_eject = 0; /* Does the drive need an eject command ? */
  unsigned need_sleep = 0; /* How many seconds to wait for the drive to get ready */
  int clean_slot = -1;
  int maxclean = 0;
  char *clean_file=NULL;
  char *time_file=NULL;

  int use_slots;
  int slot_offset;
  int confnum;

  int fd, slotcnt, drivecnt;
  int endstatus = 0;
  char *changer_dev = NULL;
  char *tape_device = NULL;
  char *changer_file = NULL;
  char *scsitapedevice = NULL;

  /*
   * Configure program for internationalization:
   *   1) Only set the message locale for now.
   *   2) Set textdomain for all amanda related programs to "amanda"
   *      We don't want to be forced to support dozens of message catalogs.
   */  
  setlocale(LC_MESSAGES, "C");
  textdomain("amanda"); 

  set_pname("chg-scsi");

  /* Don't die when child closes pipe */
  signal(SIGPIPE, SIG_IGN);

  dbopen(DBG_SUBDIR_SERVER);
  parse_args(argc,argv,&com);

  changer = alloc(SIZEOF(changer_t));
  config_init(CONFIG_INIT_USE_CWD, NULL);

  if (config_errors(NULL) >= CFGERR_WARNINGS) {
    config_print_errors();
    if (config_errors(NULL) >= CFGERR_ERRORS) {
      g_critical(_("errors processing config file"));
    }
  }

  changer_dev = getconf_str(CNF_CHANGERDEV);
  changer_file = getconf_str(CNF_CHANGERFILE);
  tape_device = getconf_str(CNF_TAPEDEV);

  /* Get the configuration parameters */

  if (strlen(tape_device)==1){
    read_config(changer_file, changer);
    confnum=atoi(tape_device);
    use_slots    = changer->conf[confnum].end-changer->conf[confnum].start+1;
    slot_offset  = changer->conf[confnum].start;
    drive_num    = changer->conf[confnum].drivenum;
    need_eject   = changer->eject;
    need_sleep   = changer->sleep;
    clean_file   = stralloc(changer->conf[confnum].cleanfile);
    clean_slot   = changer->conf[confnum].cleanslot;
    maxclean     = changer->cleanmax;
    if (NULL != changer->conf[confnum].timefile)
      time_file = stralloc(changer->conf[confnum].timefile);
    if (NULL != changer->conf[confnum].slotfile)
      changer_file = stralloc(changer->conf[confnum].slotfile);
    else
      changer_file = NULL;
    if (NULL != changer->conf[confnum].device)
      tape_device  = stralloc(changer->conf[confnum].device);
    if (NULL != changer->device)
      changer_dev  = stralloc(changer->device); 
    if (NULL != changer->conf[confnum].scsitapedev)
      scsitapedevice = stralloc(changer->conf[confnum].scsitapedev);
    if (NULL != changer->conf[confnum].tapestatfile)
      tapestatfile = stralloc(changer->conf[confnum].tapestatfile);
    dump_changer_struct(changer);
    /* get info about the changer */
    fd = OpenDevice(INDEX_CHANGER , changer_dev,
			"changer_dev", changer->conf[confnum].changerident);
    if (fd == -1) {
      int localerr = errno;
      g_fprintf(stderr, _("%s: open: %s: %s\n"), get_pname(), 
              changer_dev, strerror(localerr));
      g_printf(_("%s open: %s: %s\n"), "<none>", changer_dev, strerror(localerr));
      dbprintf(_("open: %s: %s\n"), changer_dev, strerror(localerr));
      return 2;
    }

    if (tape_device == NULL)
      {
        tape_device = stralloc(changer_dev);
      }

    if (scsitapedevice == NULL)
      {
         scsitapedevice = stralloc(tape_device);
      }

    if ((changer->conf[confnum].end == -1) || (changer->conf[confnum].start == -1)){
      slotcnt = get_slot_count(fd);
      use_slots    = slotcnt;
      slot_offset  = 0;
    }
    free_changer_struct(&changer);
  } else {
    /* get info about the changer */
    confnum = 0;
    fd = OpenDevice(INDEX_CHANGER , changer_dev,
			"changer_dev", changer->conf[confnum].changerident);
    if (fd == -1) {
      int localerr = errno;
      g_fprintf(stderr, _("%s: open: %s: %s\n"), get_pname(), 
              changer_dev, strerror(localerr));
      g_printf(_("%s open: %s: %s\n"), _("<none>"), changer_dev, strerror(localerr));
      dbprintf(_("open: %s: %s\n"), changer_dev, strerror(localerr));
      return 2;
    }
    slotcnt = get_slot_count(fd);
    use_slots    = slotcnt;
    slot_offset  = 0;
    drive_num    = 0;
    need_eject   = 0;
    need_sleep   = 0;
  }

  drivecnt = get_drive_count(fd);

  if (drive_num > drivecnt) {
    g_printf(_("%s drive number error (%d > %d)\n"), _("<none>"), 
           drive_num, drivecnt);
    g_fprintf(stderr, _("%s: requested drive number (%d) greater than "
            "number of supported drives (%d)\n"), get_pname(), 
            drive_num, drivecnt);
    dbprintf(_("requested drive number (%d) greater than "
              "number of supported drives (%d)\n"), drive_num, drivecnt);
    CloseDevice("", fd);
    return 2;
  }

  loaded = drive_loaded(fd, drive_num);

  switch(com.command_code) {
  case COM_SLOT:  /* slot changing command */
    if (is_positive_number(com.parameter)) {
      if ((target = atoi(com.parameter))>=use_slots) {
        g_printf(_("<none> no slot `%d'\n"),target);
        close(fd);
        endstatus = 2;
        break;
      } else {
        target = target+slot_offset;
      }
    } else
      target=get_relative_target(fd, use_slots,
                                 com.parameter,
                                 loaded, 
                                 changer_file,slot_offset,slot_offset+use_slots);
    if (loaded) {
      if (changer_file != NULL)
        {
          oldtarget=get_current_slot(changer_file);
        } else {
          oldtarget = GetCurrentSlot(fd, drive_num);
        }
      if ((oldtarget)!=target) {
        if (need_eject)
          eject_tape(scsitapedevice, need_eject);
        (void)unload(fd, drive_num, oldtarget);
        if (ask_clean(scsitapedevice))
          clean_tape(fd,tape_device,clean_file,drive_num,
                     clean_slot,maxclean,time_file);
        loaded=0;
      }
    }
    if (changer_file != NULL)
      {
      put_current_slot(changer_file, target);
    }
    if (!loaded && isempty(fd, target)) {
      g_printf(_("%d slot %d is empty\n"),target-slot_offset,
             target-slot_offset);
      close(fd);
      endstatus = 1;
      break;
    }
    if (!loaded)
      if (load(fd, drive_num, target) != 0) {
        g_printf(_("%d slot %d move failed\n"),target-slot_offset,
               target-slot_offset);  
        close(fd);
        endstatus = 2;
        break;
      }
    if (need_sleep)
      Tape_Ready1(scsitapedevice, need_sleep);
    g_printf(_("%d %s\n"), target-slot_offset, tape_device);
    break;

  case COM_INFO:
    if (changer_file != NULL)
      {
        g_printf("%d ", get_current_slot(changer_file)-slot_offset);
      } else {
        g_printf("%d ", GetCurrentSlot(fd, drive_num)-slot_offset);
      }
    g_printf("%d 1\n", use_slots);
    break;

  case COM_RESET:
    if (changer_file != NULL)
      {
        target=get_current_slot(changer_file);
      } else {
        target = GetCurrentSlot(fd, drive_num);
      }
    if (loaded) {
      if (!isempty(fd, target))
        target=find_empty(fd,0 ,0);
      if (need_eject)
        eject_tape(scsitapedevice, need_eject);
      (void)unload(fd, drive_num, target);
      if (ask_clean(scsitapedevice))
        clean_tape(fd,tape_device,clean_file,drive_num,clean_slot,
                   maxclean,time_file);
    }

    if (isempty(fd, slot_offset)) {
      g_printf(_("0 slot 0 is empty\n"));
      close(fd);
      endstatus = 1;
      break;
    }

    if (load(fd, drive_num, slot_offset) != 0) {
      g_printf(_("%d slot %d move failed\n"),slot_offset,
             slot_offset);  
      close(fd);
      endstatus = 2;
      break;
    }
    if (changer_file != NULL)
    {
      put_current_slot(changer_file, slot_offset);
    }
    if (need_sleep)
      Tape_Ready1(scsitapedevice, need_sleep);
    if (changer_file != NULL)
      {
        g_printf("%d %s\n", get_current_slot(changer_file), tape_device);
      } else {
        g_printf("%d %s\n", GetCurrentSlot(fd, drive_num), tape_device);
      }
    break;

  case COM_EJECT:
    if (loaded) {
      if (changer_file != NULL)
        {
          target=get_current_slot(changer_file);
        } else {
          target = GetCurrentSlot(fd, drive_num);
        }
      if (need_eject)
        eject_tape(scsitapedevice, need_eject);
      (void)unload(fd, drive_num, target);
      if (ask_clean(scsitapedevice))
        clean_tape(fd,tape_device,clean_file,drive_num,clean_slot,
                   maxclean,time_file);
      g_printf("%d %s\n", target, tape_device);
    } else {
      g_printf(_("%d drive was not loaded\n"), target);
      endstatus = 1;
    }
    break;
  case COM_CLEAN:
    if (loaded) {
      if (changer_file  != NULL)
        {
          target=get_current_slot(changer_file);
        } else {
          target = GetCurrentSlot(fd, drive_num);
        }
      if (need_eject)
        eject_tape(scsitapedevice, need_eject);
      (void)unload(fd, drive_num, target);
    } 
    clean_tape(fd,tape_device,clean_file,drive_num,clean_slot,
               maxclean,time_file);
    g_printf(_("%s cleaned\n"), tape_device);
    break;
  };

  CloseDevice("", 0);
  dbclose();
  return endstatus;
}
/*
 * Local variables:
 * indent-tabs-mode: nil
 * tab-width: 4
 * End:
 */
