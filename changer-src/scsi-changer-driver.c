static char rcsid[] = "$Id: scsi-changer-driver.c,v 1.52 2006/07/21 00:25:50 martinea Exp $";
/*
 * Interface to control a tape robot/library connected to the SCSI bus
 *
 * Copyright (c) Thomas Hepper th@ant.han.de
 */

#include "amanda.h"

#include "arglist.h"

#include "scsi-defs.h"

#include "device.h"

extern FILE *debug_file;
extern changer_t *changer;    /* Needed for the infos about emubarcode and labelfile */

int PrintInquiry(SCSIInquiry_T *);
int GenericElementStatus(int DeviceFD, int InitStatus);
int SDXElementStatus(int DeviceFD, int InitStatus);
int DLT448ElementStatus(int DeviceFD, int InitStatus);
ElementInfo_T *LookupElement(int addr);
int GenericResetStatus(int DeviceFD);
int RequestSense(int, ExtendedRequestSense_T *, int  );
void dump_hex(u_char *, size_t, int, int);
void TerminateString(char *string, size_t length);
void ChgExit(char *, char *, int);
int BarCode(int fd);
int LogSense(int fd);
int SenseHandler(int fd, u_char flag, u_char SenseKey, u_char AdditionalSenseCode, u_char AdditionalSenseCodeQualifier, RequestSense_T *buffer);

int SCSI_AlignElements(int DeviceFD, size_t MTE, size_t DTE, size_t STE);

int DoNothing0(void);
int DoNothing1(int);
int DoNothing2(int, int);
int DoNothing3(int, int, int);

int GenericMove(int, int, int);
int SDXMove(int, int, int);
int CheckMove(ElementInfo_T *from, ElementInfo_T *to);
int GenericRewind(int);
/* int GenericStatus(void); */
int GenericFree(void);
int TapeStatus(void);                   /* Is the tape loaded ? */
int DLT4000Eject(char *Device, int type);
int GenericEject(char *Device, int type);
int SCSI_LogSenseClean(char *Device);           /* Does the tape need a clean */
int GenericClean(char *Device);                 /* Does the tape need a clean */
int GenericBarCode(int DeviceFD);               /* Do we have Barcode reader support */
int NoBarCode(int DeviceFD);

int GenericSearch(void);
void Inventory(char *labelfile, int drive, int eject, int start, int stop, int clean);

int TreeFrogBarCode(int DeviceFD);
int EXB_BarCode(int DeviceFD);
int GenericSenseHandler(int fd, u_char flags, u_char SenseKey, u_char AdditionalSenseCode, u_char AdditionalSenseCodeQualifier, RequestSense_T *);

ElementInfo_T *LookupElement(int address);
int eject_tape(char *tapedev, int type);
int unload(int fd, int drive, int slot);
int load(int fd, int drive, int slot);
int GetElementStatus(int DeviceFD);
int drive_loaded(int fd, int drivenum);

/*
 * Log Pages Decode
 */
void WriteErrorCountersPage(LogParameter_T *, size_t);
void ReadErrorCountersPage(LogParameter_T *, size_t);
void C1553APage30(LogParameter_T *, size_t);
void C1553APage37(LogParameter_T *, size_t);
void EXB85058HEPage39(LogParameter_T *, size_t);
void EXB85058HEPage3c(LogParameter_T *, size_t);
int Decode(LogParameter_T *, unsigned *);
int DecodeModeSense(u_char *buffer, size_t offset, char *pstring, char block, FILE *out);

int SCSI_Run(int DeviceFD,
	     Direction_T Direction,
	     CDB_T CDB,
	     size_t CDB_Length,
	     void *DataBuffer,
	     size_t DataBufferLength,
	     RequestSense_T *pRequestSense,
	     size_t RequestSenseLength);

int SCSI_Move(int DeviceFD, u_char chm, int from, int to);
int SCSI_LoadUnload(int DeviceFD, RequestSense_T *pRequestSense, u_char byte1, u_char load);
int SCSI_TestUnitReady(int, RequestSense_T *);
int SCSI_ModeSense(int DeviceFD, u_char *buffer, u_char size, u_char byte1, u_char byte2);
int SCSI_ModeSelect(int DeviceFD,
                    u_char *buffer,
                    u_char length,
                    u_char save,
                    u_char mode,
                    u_char lun);

int SCSI_ReadElementStatus(int DeviceFD,
                           u_char type,
                           u_char lun,
                           u_char VolTag,
                           int StartAddress,
                           size_t NoOfElements,
			   size_t DescriptorSize,
			   u_char **data);

FILE *StatFile;
static int barcode;   /* cache the result from the BarCode function */

SC_COM_T SCSICommand[] = {
  {0x00,
   6,
   "TEST UNIT READY"},
  {0x01,
   6,
   "REWIND"},
  {0x03,
   6,
   "REQUEST SENSE"},
  {0x07,
   6,
   "INITIALIZE ELEMENT STATUS"},
  {0x12,
   6,
   "INQUIRY"},
  {0x13,
   6,
   "ERASE"},
  {0x15,
   6,
   "MODE SELECT"},
  {0x1A,
   6,
   "MODE SENSE"},
  {0x1B,
   6,
   "UNLOAD"},
  {0x4D,
   10,
   "LOG SENSE"},
  {0xA5,
   12,
   "MOVE MEDIUM"},
  { 0xE5,
    12,
   "VENDOR SPECIFIC"},
  {0xB8,
   12,
   "READ ELEMENT STATUS"},
  {0, 0, 0}
};

ChangerCMD_T ChangerIO[] = {
  {"generic_changer",
   "Generic driver changer [generic_changer]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  /* HP Devices */
  {"C1553A",
   "HP Auto Loader [C1553A]",
   GenericMove,
   GenericElementStatus,
   DoNothing1,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  /* Exabyte Devices */
  {"EXB-10e",
   "Exabyte Robot [EXB-10e]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"EXB-120",
   "Exabyte Robot [EXB-120]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   EXB_BarCode,
   GenericSearch,
   GenericSenseHandler},
  {"EXB-210",
   "Exabyte Robot [EXB-210]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   EXB_BarCode,
   GenericSearch,
   GenericSenseHandler},
  {"EXB-85058HE-0000",
   "Exabyte Tape [EXB-85058HE-0000]",
   DoNothing3,
   DoNothing2,
   DoNothing1,
   DoNothing0,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  /* Tandberg Devices */
  {"TDS 1420",
   "Tandberg Robot (TDS 1420)",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
    /* ADIC Devices */
  {"VLS DLT",
   "ADIC VLS DLT Library [VLS DLT]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"VLS SDX",
   "ADIC VLS DLT Library [VLS SDX]",
   SDXMove,
   SDXElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"FastStor DLT",
   "ADIC FastStor DLT Library [FastStor DLT]",
   SDXMove,
   DLT448ElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"Scalar DLT 448",
   "ADIC DLT 448 [Scalar DLT 448]",
   GenericMove,
   DLT448ElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
   /* Sepctra Logic Devices */
  {"215",
   "Spectra Logic TreeFrog[215]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   TreeFrogBarCode,
   GenericSearch,
   GenericSenseHandler},
  /* BreeceHill Q7 */
  {"Quad 7",
   "Breece Hill Quad 7",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  /* Quantum Devices */
  {"L500",
   "ATL [L500]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  /*
   * And now the tape devices
   */
  /* The generic handler if nothing matches */
  {"generic_tape",
   "Generic driver tape [generic_tape]",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   GenericClean,
   GenericRewind,
   NoBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"DLT8000",
   "DLT Tape [DLT8000]",
   DoNothing3,
   DoNothing2,
   DoNothing1,
   DoNothing0,
   DLT4000Eject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"DLT7000",
   "DLT Tape [DLT7000]",
   DoNothing3,
   DoNothing2,
   DoNothing1,
   DoNothing0,
   DLT4000Eject,
   GenericClean,
   GenericRewind,
   GenericBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"DLT4000",
   "DLT Tape [DLT4000]",
   DoNothing3,
   DoNothing2,
   DoNothing1,
   DoNothing0,
   DLT4000Eject,
   GenericClean,
   GenericRewind,
   NoBarCode,
   GenericSearch,
   GenericSenseHandler},
  {"SLR100",
   "Tandberg SLR100",
   GenericMove,
   GenericElementStatus,
   GenericResetStatus,
   GenericFree,
   GenericEject,
   SCSI_LogSenseClean,
   GenericRewind,
   NoBarCode,
   GenericSearch,
   GenericSenseHandler},
   {NULL, NULL, NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
};


LogPageDecode_T DecodePages[] = {
  {2,
   "C1553A",
   WriteErrorCountersPage},
  {3,
   "C1553A",
   ReadErrorCountersPage},
  {0x30,
   "C1553A",
   C1553APage30},
  {0x37,
   "C1553A",
   C1553APage37},
  {2,
   "*",
   WriteErrorCountersPage},
  {3,
   "*",
   ReadErrorCountersPage},
  {0x39,
   "EXB-85058HE-0000",
   EXB85058HEPage39},
  {0x3c,
   "EXB-85058HE-0000",
   EXB85058HEPage3c},
  {0, NULL, NULL}
};


int ElementStatusValid = 0;         /* Set if the READ ELEMENT STATUS was OK, an no error is pending */
int LibModeSenseValid = 0;          /* Set if we did an scussefull MODE SENSE */

char *SlotArgs = 0;
/* Pointer to MODE SENSE Pages */
u_char *pModePage = NULL;
EAAPage_T *pEAAPage = NULL;
DeviceCapabilitiesPage_T *pDeviceCapabilitiesPage = NULL;
u_char *pVendorUnique = NULL;

/*
 *  New way, every element type has its on array
 * which is dynamic allocated by the ElementStatus function,
*/
ElementInfo_T *pMTE = NULL; /*Medium Transport Element */
ElementInfo_T *pSTE = NULL; /*Storage Element */
ElementInfo_T *pIEE = NULL; /*Import Export Element */
ElementInfo_T *pDTE = NULL; /*Data Transfer Element */
size_t MTE = 0;                /*Counter for the above element types */
size_t STE = 0;
size_t IEE = 0;
size_t DTE = 0;

char *chgscsi_datestamp = NULL;       /* Result pointer for tape_rdlabel */
char *chgscsi_label = NULL;           /* Result pointer for tape_rdlabel */
char *chgscsi_result = NULL;          /* Needed for the result string of MapBarCode */

/*
 * This used to be in tape-src/tapeio.c; this is the Device API version.
 */

static char *
tape_rdlabel(
    char *devname,
    char **datestamp,
    char **label)
{
    Device *dev;
    char *r = NULL;

    dev = device_open(devname);
    if (dev->status != DEVICE_STATUS_SUCCESS) {
	r = g_strdup(device_error_or_status(dev));
	g_object_unref(dev);
	return r;
    }

    if (!device_configure(dev, TRUE) || !device_read_label(dev)) {
	r = g_strdup(device_error_or_status(dev));
	g_object_unref(dev);
	return r;
    }

    *datestamp = g_strdup(dev->volume_time);
    *label = g_strdup(dev->volume_label);

    return NULL;
}

/*
 * First all functions which are called from extern
 */


/*
 * Print the scsi-changer-driver version
 */

void
ChangerDriverVersion(void)
{
  DebugPrint(DEBUG_ERROR, SECTION_INFO, "scsi-changer-driver: %s\n",rcsid);
  SCSI_OS_Version();
}

/*
 * Try to generate an template which can be used as an example for the config file
 *
 */
void
PrintConf(void)
{
  extern OpenFiles_T *pDev;
  int count;
  char *cwd;

  g_printf(_("# Please replace every ??? with the correct parameter.\n"));
  g_printf(_("# It is not possible to guess everything :-)\n"));
  g_printf(_("# Remove the line if the option is not needed."));
  g_printf(_("#   Example: cleanmax if you have no cleaning tape"));
  g_printf(_("#\n"));
  g_printf(_(
   "number_configs  1 # Number of configs, you can have more than 1 config\n"
   "                  # if you have for example more than one drive, or you\n"
   "                  # to split your lib to use different dump levels\n"
   "                  #\n"));
  g_printf(_(
   "emubarcode      1 # If you drive has no barcode reader this will try\n"
   "                  # keep an inventory of your tapes to find them faster\n"
   "                  #\n"));
  g_printf(_(
   "havebarcode     0 # Set this to 1 if you have a library with a\n"
   "                  # barcode reader\n"
   "                  #\n"));
  g_printf(_(
   "debuglevel    0:0 # For debuging, see the docs /docs/TAPE-CHANGER\n"
   "                  #\n"));
  g_printf(_(
   "eject         ??? # set this to 1 if your drive needs an eject before move\n"
   "                  #\n"));
  g_printf(_(
   "sleep         ??? # How long to wait after an eject command before moving\n"
   "                  # the tape\n"
   "                  #\n"));

  for (count = 0; count < CHG_MAXDEV ; count++)
    {
      if (pDev[count].dev)
	{
	  if (pDev[count].inquiry != NULL && pDev[count].inquiry->type == TYPE_CHANGER)
	    {
	      g_printf(_("changerdev   %s # This is the device to communicate with the robot\n"), pDev[count].dev);
	      break;
	    }
	}
    }

  /*
   * Did we reach the end of the list ?
   * If no we found an changer and now we try to
   * get the element status for the count of slots
   */
  if (count < CHG_MAXDEV)
    {
      pDev[count].functions->function_status(count, 1);
    } else {
      g_printf(_("changerdev ???       # Ups nothing found. Please check the docs\n"));
    }

  g_printf(_(
   "                     #\n"));
  g_printf(_(
   "                     # Here now comes the config for the first tape\n"));
  g_printf(_(
   "config             0 # This value is the one which is used in the amanda\n"
   "                     # config file to tell the chg-scsi programm which tape\n"
   "                     # and which slots to use\n"
   "                     #\n"));
  g_printf(_(
   "cleancart        ??? # The slot where the cleaning tape is located\n"
   "                     # remove it if you have no cleaning tape\n"
   "                     #\n"));
  g_printf(_(
   "drivenum           0 # Which tape drive to use if there are more than one drive\n"
   "                     #\n"));
  g_printf(_(
   "dev              ??? # Which is the raw device to read/write data from the tape\n"
   "                     # It is important to use the non rewinding tape, like\n"
   "                     # /dev/nrst0 on linux, /dev/nrsa0 on BSD ....\n"
   "                     #\n"));

  /*
   * OK now lets see if we have an direct SCSI channel
   * to the tape
   * If not thats not a problem
   */
  for (count = 0; count < CHG_MAXDEV; count++)
    {
      if (pDev[count].dev)
	{
	  if (pDev[count].inquiry != NULL && pDev[count].inquiry->type == TYPE_TAPE)
	    {
	      g_printf(_(
		"scsitapedev   %s # This is the device to communicate with the tape\n"
	        "                 # to get some device stats, not so important, and\n"
	        "                 # if you run into problems delete it completely\n"
	        "                 #\n"), pDev[count].dev);
	      break;
	    }
	}
    }


  if (STE != 0)
    {
      g_printf(_(
	"startuse          0  # Which is the first slot to use\n"
	"                     #\n"));
      g_printf(_(
	"enduse            %zu  # Which is the last slot to use.\n"), STE);
    } else {
      g_printf(_(
	"startuse         ??? # Which is the first slot to use\n"
	"                     #\n"));
      g_printf(_(
	"enduse           ??? # Which is the last slot to use.\n"));
    }
    g_printf(_(
	"                     # Decrement this value by 1 if you have a\n"
	"                     # cleaning tape in the last slot\n"
	"                     #\n"));

  if ((cwd = getcwd(NULL, 0)) == NULL) {
      cwd = _("<unknown>");
  }

  g_printf(_("statfile %s/tape0-slot #\n"),cwd);
  g_printf(_("cleanfile %s/tape0-clean #\n"), cwd);
  g_printf(_("usagecount %s/tape0-totaltime #\n"), cwd);
  g_printf(_("tapestatus %s/tape0-tapestatus #\n"), cwd);
  g_printf(_("labelfile %s/labelfile #\n"), cwd);
}



/*
 * Try to create a list of tapes and labels which are in the current
 * magazin. The drive must be empty !!
 *
 * labelfile -> file name of the db
 * drive -> which drive should we use
 * eject -> the tape device needs an eject before move
 * start -> start at slot start
 * stop  -> stop at slot stop
 * clean -> if we have an cleaning tape than this is the slot number of it
 *
 * return
 * 0  -> fail
 * 1  -> successfull
 *
 * ToDo:
 * Check if the tape/changer is ready for the next move
 * If an tape is loaded unload it and do initialize element status to
 * get all labels if an bar code reader is installed
 */
void
Inventory(
    char *	labelfile,
    int		drive,
    int		eject,
    int		start,
    int		stop,
    int		clean)
{
  extern OpenFiles_T *pDev;
  size_t x;
  static int inv_done = 0;	/* Inventory function called ?, marker to disable recursion */
  MBC_T *pbarcoderes;		/* Here we will pass the parameter to MapBarCode and get the result */

  (void)start;	/* Quiet unused parameter warning */
  (void)stop;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO,SECTION_MAP_BARCODE, _("##### START Inventory\n"));
  pbarcoderes = alloc(SIZEOF(MBC_T));
  memset(pbarcoderes, 0 , SIZEOF(MBC_T));

  if (inv_done != 0)
    {
      DebugPrint(DEBUG_INFO,SECTION_MAP_BARCODE, _("##### STOP inv_done -> %d Inventory\n"),inv_done);
      free(pbarcoderes);
      return;
      /*NOTREACHED*/
    }
  inv_done = 1;
  barcode = BarCode(INDEX_CHANGER);

  pbarcoderes->action = RESET_VALID;

  MapBarCode(labelfile,pbarcoderes);

  /*
   * Check if an tape is loaded, if yes unload it
   * and do an INIT ELEMENT STATUS
   */

  if (pDTE[0].status == 'F')
    {
      if (eject)
	{
	  (void)eject_tape("", eject);
	}
      (void)unload(INDEX_TAPE, 0, 0);
    }

  GenericResetStatus(INDEX_CHANGER);

  for (x = 0; x < STE; x++)
    {
      if (x == (size_t)clean)
	{
	  continue;
	}

      /*
       * Load the tape, on error try the next
       * error could be an empty slot for example
       */
      if (load(INDEX_CHANGER, drive, x ) != 0)
	{
	  DebugPrint(DEBUG_ERROR,SECTION_MAP_BARCODE, _("Load drive(%d) from(%d) failed\n"), drive, x);
	  continue;
	}

      /*
       * Wait until the tape is ready
       */
      Tape_Ready(INDEX_TAPECTL, 60);

      SCSI_CloseDevice(INDEX_TAPE);

      if ((chgscsi_result = (char *)tape_rdlabel(pDev[INDEX_TAPE].dev, &chgscsi_datestamp, &chgscsi_label)) == NULL)
      {
	pbarcoderes->action = UPDATE_SLOT;
	strncpy(pbarcoderes->data.voltag, chgscsi_label,
		SIZEOF(pbarcoderes->data.voltag));
	pbarcoderes->data.slot = x;
	pbarcoderes->data.from = 0;
	pbarcoderes->data.LoadCount = 1;
	if (BarCode(INDEX_CHANGER) == 1)
	  {
	    strncpy(pbarcoderes->data.barcode, pDTE[drive].VolTag,
		    SIZEOF(pbarcoderes->data.barcode));
	    MapBarCode(labelfile, pbarcoderes);
	  } else {
	    MapBarCode(labelfile, pbarcoderes);
	  }
      } else {
	DebugPrint(DEBUG_ERROR,SECTION_MAP_BARCODE, _("Read label failed\n"));
      }

      if (eject)
	{
	  (void)eject_tape("", eject);
	}

      (void)unload(INDEX_TAPE, drive, x);
    }
  DebugPrint(DEBUG_INFO,SECTION_MAP_BARCODE, _("##### STOP Inventory\n"));
  free(pbarcoderes);
}

/*
 * Check if the slot ist empty
 * slot -> slot number to check
 */
int
isempty(
    int		fd,
    int		slot)
{
  extern OpenFiles_T *pDev;
  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### START isempty\n"));

  if (ElementStatusValid == 0)
    {
      if ( pDev[fd].functions->function_status(fd, 1) != 0)
        {
          DebugPrint(DEBUG_ERROR,SECTION_TAPE,_("##### STOP isempty [-1]\n"));
          return(-1);
	  /*NOTREACHED*/
        }
    }

  if (pSTE[slot].status == 'E')
    {
      DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### STOP isempty [1]\n"));
      return(1);
      /*NOTREACHED*/
    }
  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### STOP isempty [0]\n"));
  return(0);
}

int
get_clean_state(
    char *tapedev)
{
  extern OpenFiles_T *pDev;
  /* Return 1 if cleaning is needed */
  int ret;

  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### START get_clean_state\n"));

  if (pDev[INDEX_TAPECTL].SCSI == 0)
    {
      DebugPrint(DEBUG_ERROR,SECTION_TAPE,_("##### STOP get_clean_state [-1]\n"));
      return(-1);
      /*NOTREACHED*/
    }
  ret=pDev[INDEX_TAPECTL].functions->function_clean(tapedev);
  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### STOP get_clean_state [%d]\n"), ret);
  return(ret);
}

/*
 * eject the tape
 * The parameter tapedev is not used.
 * Type describes if we should force the SCSI eject if available
 * normal eject is done with the ioctl
 */
/* This function ejects the tape from the drive */

int
eject_tape(
    char *	tapedev,
    int		type)
{
  extern OpenFiles_T *pDev;
  int ret;

  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### START eject_tape %s\n"),tapedev);
  if (pDev[INDEX_TAPECTL].functions == NULL)
    return(-1);

  /*
   * Try to read the label
   */
  if (pDev[INDEX_TAPE].avail == 1 && (changer->emubarcode == 1 || BarCode(INDEX_CHANGER)))
    {

      if (pDev[INDEX_TAPECTL].SCSI == 1 && pDev[INDEX_TAPECTL].avail) {
        DebugPrint(DEBUG_INFO,SECTION_TAPE,"##### eject_tape rewind\n");
	pDev[INDEX_TAPECTL].functions->function_rewind(INDEX_TAPECTL);
      } else {
        DebugPrint(DEBUG_INFO,SECTION_TAPE,"##### eject_tape rewind2\n");
	pDev[INDEX_TAPE].functions->function_rewind(INDEX_TAPE);
      }

      if (pDev[INDEX_TAPE].devopen == 1)
	{
          DebugPrint(DEBUG_INFO,SECTION_TAPE,"##### eject_tape close\n");
	  SCSI_CloseDevice(INDEX_TAPE);
	}

      DebugPrint(DEBUG_INFO,SECTION_TAPE,"##### tape_eject tape_rdlabel\n");
      chgscsi_result = (char *)tape_rdlabel(pDev[INDEX_TAPE].dev, &chgscsi_datestamp, &chgscsi_label);
    }

  if (pDev[INDEX_TAPECTL].SCSI == 1 && pDev[INDEX_TAPECTL].avail == 1 && type == 1)
    {
      DebugPrint(DEBUG_INFO,SECTION_TAPE,"##### tape_eject eject\n");
      ret=pDev[INDEX_TAPECTL].functions->function_eject(tapedev, type);
      DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### STOP (SCSI)eject_tape [%d]\n"), ret);
      return(ret);
      /*NOTREACHED*/
    }

  if (pDev[INDEX_TAPE].avail == 1)
    {
      ret=Tape_Ioctl(INDEX_TAPE, IOCTL_EJECT);
      DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### STOP (ioctl)eject_tape [%d]\n"), ret);
      return(ret);
      /*NOTREACHED*/
    }

  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("##### STOP eject_tape [-1]\n"));
  return(-1);
}


/* Find an empty slot, starting at start, ending at start+count */
int
find_empty(
    int	fd,
    int	start,
    int	count)
{
  extern OpenFiles_T *pDev;
  size_t x;
  size_t end;

  DebugPrint(DEBUG_INFO,SECTION_ELEMENT,_("###### START find_empty\n"));

  if (ElementStatusValid == 0)
    {
      if ( pDev[fd].functions->function_status(fd , 1) != 0)
        {
          DebugPrint(DEBUG_ERROR,SECTION_ELEMENT,
		     _("###### END find_empty [-1]\n"));
          return((ssize_t)-1);
	  /*NOTREACHED*/
        }
    }

  if (count == 0)
    {
      end = STE;
    } else {
      end = start + count;
    }

  if (end > STE)
    {
      end = STE;
    }

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,
	     _("start at %zu, end at %zu\n"),
	     (size_t)start,
	     (size_t)end);

  for (x = start; x < end; x++)
    {
      if (pSTE[x].status == 'E')
        {
          DebugPrint(DEBUG_INFO,SECTION_ELEMENT,
		     _("###### END find_empty [%lu]\n"), x);
          return((ssize_t)x);
	  /*NOTREACHED*/
        }
    }
  DebugPrint(DEBUG_ERROR,SECTION_ELEMENT,_("###### END find_empty [-1]\n"));
  return((ssize_t)-1);
}

/*
 * See if the tape is loaded based on the information we
 * got back from the ReadElementStatus
 * return values
 * -1 -> Error (Fatal)
 * 0  -> drive is empty
 * 1  -> drive is loaded
 */
int
drive_loaded(
    int		fd,
    int		drivenum)
{
  extern OpenFiles_T *pDev;

  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("###### START drive_loaded\n"));
  DebugPrint(DEBUG_INFO,SECTION_TAPE,
		_("        drive_loaded : fd %d drivenum %d \n"), fd, drivenum);


  if (ElementStatusValid == 0)
    {
      if (pDev[INDEX_CHANGER].functions->function_status(INDEX_CHANGER, 1) != 0)
	{
	  DebugPrint(DEBUG_ERROR,SECTION_TAPE,_("Fatal error\n"));
	  return(-1);
	  /*NOTREACHED*/
	}
    }

  if (pDTE[drivenum].status == 'E') {
    DebugPrint(DEBUG_INFO,SECTION_TAPE,_("###### STOP drive_loaded (empty)\n"));
    return(0);
    /*NOTREACHED*/
  }
  DebugPrint(DEBUG_INFO,SECTION_TAPE,_("###### STOP drive_loaded (not empty)\n"));
  return(1);
}


/*
 * unload the specified drive into the specified slot
 * (storage element)
 *
 * TODO:
 * Check if the MTE is empty
 */
int
unload(
    int		fd,
    int		drive,
    int		slot)
{
  extern OpenFiles_T *pDev;
  extern int do_inventory;
  MBC_T *pbarcoderes;

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("###### START unload\n"));
  DebugPrint(DEBUG_INFO, SECTION_TAPE,
			_("              unload : fd %d, slot %d, drive %d \n"),
			fd, slot, drive);
  pbarcoderes = alloc(SIZEOF(MBC_T));
  memset(pbarcoderes, 0, SIZEOF(MBC_T));

  /*
   * If the Element Status is not valid try to
   * init it
   */
  if (ElementStatusValid == 0)
    {
      if (pDev[INDEX_CHANGER].functions->function_status(INDEX_CHANGER , 1) != 0)
	{
	  DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("Element Status not valid, reset failed\n"));
	  DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("##### STOP unload (-1)\n"));
	  free(pbarcoderes);
	  return(-1);
	  /*NOTREACHED*/
	}
    }

  DebugPrint(DEBUG_INFO, SECTION_TAPE,
		_("              unload : unload drive %d[%d] slot %d[%d]\n"),
		drive, pDTE[drive].address, slot, pSTE[slot].address);

  /*
   * Unloading an empty tape unit makes no sense
   * so return with an error
   */
  if (pDTE[drive].status == 'E')
    {
      DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("unload : Drive %d address %d is empty\n"), drive, pDTE[drive].address);
      DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("##### STOP unload (-1)\n"));
      free(pbarcoderes);
      return(-1);
      /*NOTREACHED*/
    }

  /*
   * If the destination slot is full
   * try to find an enpty slot
   */
  if (pSTE[slot].status == 'F')
    {
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("unload : Slot %d address %d is full\n"), drive, pSTE[slot].address);
      if ( ElementStatusValid == 0)
	{
	  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("unload: Element Status not valid, can't find an empty slot\n"));
	  free(pbarcoderes);
	  return(-1);
	  /*NOTREACHED*/
	}

      slot = find_empty(fd, 0, 0);
      if (slot == -1 )
      {
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("unload: No Empty slot found\n"));
	      free(pbarcoderes);
	      return(-1);
	      /*NOTREACHED*/
      }
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("unload : found empty one, try to unload to slot %d\n"), slot);
    }



  /*
   * If eject is not set we must read the label info
   */

  if (changer->eject == 0)
    {
      if (pDev[INDEX_TAPE].avail == 1 && (changer->emubarcode == 1 || BarCode(INDEX_CHANGER)))
	{

	  if (pDev[INDEX_TAPECTL].SCSI == 1 && pDev[INDEX_TAPECTL].avail) {
	    pDev[INDEX_TAPECTL].functions->function_rewind(INDEX_TAPECTL);
	  } else {
	    pDev[INDEX_TAPE].functions->function_rewind(INDEX_TAPE);
	  }

	  if (pDev[INDEX_TAPE].devopen == 1)
	    {
	      SCSI_CloseDevice(INDEX_TAPE);
	    }

	  chgscsi_result = (char *)tape_rdlabel(pDev[INDEX_TAPE].dev, &chgscsi_datestamp, &chgscsi_label);
	}
    }

  /*
   * Do the unload/move
   */
  if (pDev[INDEX_CHANGER].functions->function_move(INDEX_CHANGER,
           pDTE[drive].address, pSTE[slot].address) != 0) {
      DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("##### STOP unload (-1 move failed)\n"));
      free(pbarcoderes);
      return(-1);
      /*NOTREACHED*/
    }


  /*
   * Update the Status
   */
  if (pDev[INDEX_CHANGER].functions->function_status(INDEX_CHANGER , 1) != 0)
    {
      DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("##### STOP unload (-1 update status failed)\n"));
      free(pbarcoderes);
      return(-1);
      /*NOTREACHED*/
    }

  /*
   * Did we get an error from tape_rdlabel
   * if no update the vol/label mapping
   * If chgscsi_label is NULL don't do it
   */
  if (chgscsi_result  == NULL && chgscsi_label != NULL && changer->labelfile != NULL)
  {
    /*
     * OK this is only needed if we have emubarcode set
     * There we need an exact inventory to get the search function working
     * and returning correct results
     */
    if (BarCode(INDEX_CHANGER) == 0 && changer->emubarcode == 1)
      {
	/*
	 * We got something, update the db
	 * but before check if the db has as entry the slot
	 * to where we placed the tape, if no force an inventory
	 */
	pbarcoderes->action = FIND_SLOT;
	strncpy(pbarcoderes->data.voltag, chgscsi_label,
		SIZEOF(pbarcoderes->data.voltag));
	strncpy(pbarcoderes->data.barcode, pSTE[slot].VolTag,
	       SIZEOF(pbarcoderes->data.barcode));
	pbarcoderes->data.slot = 0;
	pbarcoderes->data.from = 0;
	pbarcoderes->data.LoadCount = 0;


	if ( MapBarCode(changer->labelfile, pbarcoderes) == 0) /* Nothing known about this, do an Inventory */
	  {
	    do_inventory = 1;
	  } else {
	    if (slot != pbarcoderes->data.slot)
	      {
		DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("Slot DB out of sync, slot %d != map %d"),slot, pbarcoderes->data.slot);
		do_inventory = 1;
	      }
	  }
      }
  }

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP unload(0)\n"));
  free(pbarcoderes);
  return(0);
}


/*
 * load the media from the specified element (slot) into the
 * specified data transfer unit (drive)
 * fd     -> pointer to the internal device structure pDev
 * driver -> which drive in the library
 * slot   -> the slot number from where to load
 *
 * return -> 0 = success
 *           !0 = failure
 */
int
load(
    int		fd,
    int		drive,
    int		slot)
{
  char *result = NULL;          /* Needed for the result of tape_rdlabel */
  int ret;
  extern OpenFiles_T *pDev;
  extern int do_inventory;
  MBC_T *pbarcoderes;

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("###### START load\n"));
  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("%-20s : fd %d, drive %d, slot %d \n"), "load", fd, drive, slot);
  pbarcoderes = alloc(SIZEOF(MBC_T));
  memset(pbarcoderes, 0 , SIZEOF(MBC_T));

  if (ElementStatusValid == 0)
      {
          if (pDev[fd].functions->function_status(fd, 1) != 0)
              {
		DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("##### STOP load (-1)\n"));
		DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("##### STOP load (-1 update status failed)\n"));
		free(pbarcoderes);
		return(-1);
		/*NOTREACHED*/
              }
      }

  /*
   * Check if the requested slot is in the range of available slots
   * The library starts counting at 1, we start at 0, so if the request slot
   * is ge than the value we got from the ModeSense fail with an return value
   * of 2
   */
  if ((size_t)slot >= STE)
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("load : slot %d ge STE %d\n"),slot, STE);
      ChgExit("load", _("slot >= STE"), FATAL);
      /*NOTREACHED*/
    }

  /*
   * And the same for the tape drives
   */
  if (drive >= (int)DTE)
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("load : drive %d ge DTE %d\n"),drive, DTE);
      ChgExit("load", _("drive >= DTE"), FATAL);
      /*NOTREACHED*/
    }

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("load : load drive %d[%d] slot %d[%d]\n"),drive,
	     pDTE[drive].address,
	     slot,
	     pSTE[slot].address);

  if (pDTE[drive].status == 'F')
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("load : Drive %d address %d is full\n"), drive, pDTE[drive].address);
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("##### STOP load (-1 update status failed)\n"));
      free(pbarcoderes);
      return(-1);
      /*NOTREACHED*/
    }

  if (pSTE[slot].status == 'E')
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("load : Slot %d address %d is empty\n"), drive, pSTE[slot].address);
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("##### STOP load (-1 update status failed)\n"));
      free(pbarcoderes);
      return(-1);
      /*NOTREACHED*/
    }

  ret = pDev[fd].functions->function_move(fd, pSTE[slot].address, pDTE[drive].address);

  /*
   * Update the Status
   */
  if (pDev[fd].functions->function_status(fd, 1) != 0)
      {
	DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("##### STOP load (-1 update status failed)\n"));
	free(pbarcoderes);
	return(-1);
	/*NOTREACHED*/
      }

  /*
   * Try to read the label
   * and update the label/slot database
   */
  if (pDev[INDEX_TAPE].avail == 1 && (changer->emubarcode == 1 || BarCode(INDEX_CHANGER)))
    {

      if (pDev[INDEX_TAPECTL].SCSI == 1 && pDev[INDEX_TAPECTL].avail) {
	pDev[INDEX_TAPECTL].functions->function_rewind(INDEX_TAPECTL);
      } else {
	pDev[INDEX_TAPE].functions->function_rewind(INDEX_TAPE);
      }

      if (pDev[INDEX_TAPE].devopen == 1)
	{
	  SCSI_CloseDevice(INDEX_TAPE);
	}

      result = (char *)tape_rdlabel(pDev[INDEX_TAPE].dev, &chgscsi_datestamp, &chgscsi_label);
    }

  /*
   * Did we get an error from tape_rdlabel
   * if no update the vol/label mapping
   */
  if (result  == NULL && changer->labelfile != NULL && chgscsi_label != NULL )
    {
      /*
       * We got something, update the db
       * but before check if the db has as entry the slot
       * to where we placed the tape, if no force an inventory
       */
      strncpy(pbarcoderes->data.voltag, chgscsi_label,
	      SIZEOF(pbarcoderes->data.voltag));
      pbarcoderes->data.slot = 0;
      pbarcoderes->data.from = 0;
      pbarcoderes->data.LoadCount = 0;


      /*
       * If we have an barcode reader we only do an update
       * If emubarcode is set we check if the
       * info in the DB is up to date, if no we set the do_inventory flag
       */

      if (BarCode(INDEX_CHANGER) == 1 && changer->emubarcode == 0)
	{
	  pbarcoderes->action = UPDATE_SLOT;
	  strncpy(pbarcoderes->data.barcode, pDTE[drive].VolTag,
		  SIZEOF(pbarcoderes->data.barcode));
	  pbarcoderes->data.LoadCount = 1;
	  pbarcoderes->data.slot = slot;
	  MapBarCode(changer->labelfile, pbarcoderes);
	}

      if (BarCode(INDEX_CHANGER) == 0 && changer->emubarcode == 1)
	{
	  pbarcoderes->action = FIND_SLOT;
	  if (MapBarCode(changer->labelfile, pbarcoderes) == 0) /* Nothing found, do an inventory */
	    {
	      do_inventory = 1;
	    } else { /* We got something, is it correct ? */
	      if (slot != pbarcoderes->data.slot && do_inventory == 0)
		{
		  DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("Slot DB out of sync, slot %d != map %d"),slot, pbarcoderes->data.slot);
		  ChgExit("Load", _("Label DB out of sync"), FATAL);
		  /*NOTREACHED*/
		} else { /* OK, so increment the load count */
		  pbarcoderes->action = UPDATE_SLOT;
		  pbarcoderes->data.LoadCount = 1;
		  pbarcoderes->data.slot = slot;
		  MapBarCode(changer->labelfile, pbarcoderes);
		}
	    }
	}

      if (BarCode(INDEX_CHANGER) == 1 && changer->emubarcode == 1)
	{
	  ChgExit("Load", _("BarCode == 1 and emubarcode == 1"), FATAL);
	  /*NOTREACHED*/
	}

      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("##### STOP load (%d)\n"),ret);
      free(pbarcoderes);
      return(ret);
      /*NOTREACHED*/
    }
    DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("##### STOP load (%d)\n"),ret);
    free(pbarcoderes);
    return(ret);
}

/*
 * Returns the number of Storage Slots which the library has
 * fd -> pointer to the internal devie structure pDev
 * return -> Number of slots
 */
int
get_slot_count(
    int fd)
{
  extern OpenFiles_T *pDev;

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("###### START get_slot_count\n"));
  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("%-20s : fd %d\n"), "get_slot_count", fd);

  if (ElementStatusValid == 0)
    {
      pDev[fd].functions->function_status(fd, 1);
    }
  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,
	     _("##### STOP get_slot_count (%zu)\n"), STE);
  return((ssize_t)STE);
  /*
   * return the number of slots in the robot
   * to the caller
   */
}


/*
 * retreive the number of data-transfer devices /Tape drives)
 * fd     -> pointer to the internal devie structure pDev
 * return -> -1 on failure
 */
int
get_drive_count(
    int fd)
{

  extern OpenFiles_T *pDev;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("###### START get_drive_count\n"));
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-20s : fd %d\n"), "get_drive_count", fd);

  if (ElementStatusValid == 0)
      {
          if ( pDev[fd].functions->function_status(fd, 1) != 0)
	    {
		DebugPrint(DEBUG_ERROR, SECTION_SCSI, _("Error getting drive count\n"));
		DebugPrint(DEBUG_ERROR, SECTION_SCSI, _("##### STOP get_drive_count (-1)\n"));
		return(-1);
		/*NOTREACHED*/
	    }
      }
  DebugPrint(DEBUG_INFO, SECTION_SCSI,
	     _("###### STOP get_drive_count (%zu drives)\n"), DTE);
  return((ssize_t)DTE);
}

/*
 * Now the internal functions
 */

/*
 * Open the device and placeit in the list of open files
 * The OS has to decide if it is an SCSI Commands capable device
 */

int
OpenDevice(
    int		ip,
    char *	DeviceName,
    char *	ConfigName,
    char *	ident)
{
  extern OpenFiles_T *pDev;
  char tmpstr[16];
  ChangerCMD_T *p = (ChangerCMD_T *)&ChangerIO;

  if (!ConfigName)
	return 1;
  if (!DeviceName)
	return 1;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START OpenDevice\n"));
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("OpenDevice : %s\n"), DeviceName);

  pDev[ip].ConfigName = strdup(ConfigName);
  pDev[ip].dev = strdup(DeviceName);

  if (SCSI_OpenDevice(ip) != 0 )
    {
      if (ident != NULL)   /* Override by config */
      {
        while(p->ident != NULL)
          {
            if (strcmp(ident, p->ident) == 0)
              {
                pDev[ip].functions = p;
		strncpy(pDev[ip].ident, ident, 17);
                DebugPrint(DEBUG_INFO, SECTION_SCSI,_("override using ident = %s, type = %s\n"),p->ident, p->type);
		DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP OpenDevice\n"));
                return(1);
		/*NOTREACHED*/
              }
            p++;
          }
	  ChgExit("OpenDevice", _("ident not found"), FATAL);
	  /*NOTREACHED*/
      } else {
        while(p->ident != NULL)
          {
            if (strcmp(pDev[ip].ident, p->ident) == 0)
              {
                pDev[ip].functions = p;
                DebugPrint(DEBUG_INFO, SECTION_SCSI,_("using ident = %s, type = %s\n"),p->ident, p->type);
		DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP OpenDevice\n"));
                return(1);
		/*NOTREACHED*/
              }
            p++;
          }
      }
      /* Nothing matching found, try generic */
      /* divide generic in generic_type, where type is the */
      /* num returned by the inquiry command */
      p = (ChangerCMD_T *)&ChangerIO;
      g_snprintf(&tmpstr[0], SIZEOF(tmpstr), "%s_%s","generic",pDev[0].type);
      DebugPrint(DEBUG_INFO, SECTION_SCSI,"##### OpenDevice trying GENERIC Device %s\n",tmpstr);
      while(p->ident != NULL)
        {
          if (strcmp(tmpstr, p->ident) == 0)
            {
              pDev[ip].functions = p;
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("using ident = %s, type = %s\n"),p->ident, p->type);
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP OpenDevice\n"));
              return(1);
	      /*NOTREACHED*/
            }
          p++;
        }
    } else { /* Something failed, lets see what */
      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("##### STOP OpenDevice failed\n"));
    }
  pDev[ip].functions = NULL;
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP OpenDevice (nothing found) !!\n"));
  return(0);
}


/*
 * This functions checks if the library has an barcode reader.
 * fd     -> pointer to the internal devie structure pDev
 */
int
BarCode(
    int		fd)
{
  int ret;
  extern OpenFiles_T *pDev;

  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### START BarCode\n"));
  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("%-20s : fd %d\n"), "BarCode", fd);

  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("Ident = [%s], function = [%s]\n"), pDev[fd].ident,
	     pDev[fd].functions->ident);
  ret = pDev[fd].functions->function_barcode(fd);
  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### STOP BarCode (%d)\n"),ret);
  return(ret);
}


/*
 * This functions check if the tape drive is ready
 *
 * fd     -> pointer to the internal devie structure pDev
 * wait -> time to wait for the ready status
 *
 */
int
Tape_Ready(
    int		fd,
    time_t	wait_time)
{
  extern OpenFiles_T *pDev;
  int done;
  int ret;
  time_t cnt = 0;

  RequestSense_T *pRequestSense;
  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### START Tape_Ready\n"));

  /*
   * Which device should we use to get the
   * tape status
   */

  /*
   * First the ioctl tapedevice
   */
  if (pDev[INDEX_TAPE].avail == 1)
    {
      fd = INDEX_TAPE;
    }

  /*
   * But if available and can do SCSI
   * the scsitapedev
   */
  if (pDev[INDEX_TAPECTL].avail == 1 && pDev[INDEX_TAPECTL].SCSI == 1)
    {
      fd = INDEX_TAPECTL;
    }

  if (pDev[fd].avail == 1 && pDev[fd].SCSI == 0)
    {
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("Tape_Ready : Can't send SCSI commands, try ioctl\n"));
      /*
       * Do we get an non negative result.
       * If yes this function is available
       * and we can use it to get the status
       * of the tape
       */
      ret = Tape_Status(fd);
      if (ret >= 0)
	{
	  while (cnt < wait_time)
	    {
	      if ( ret & TAPE_ONLINE)
		{
		  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("Tape_Ready : Ready after %d seconds\n"),cnt);
		  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP Tape_Ready\n"));
		  return(0);
		  /*NOTREACHED*/
		}
	      cnt++;
	      sleep(1);
	      ret = Tape_Status(fd);
	    }

	  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("Tape_Ready : not ready, stop after %d seconds\n"),cnt);
	  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP Tape_Ready\n"));
	  return(0);
	  /*NOTREACHED*/

	}
	DebugPrint(DEBUG_INFO, SECTION_TAPE,_("Tape_Ready : no ioctl interface, will sleep for %d seconds\n"), wait_time);
	sleep(wait_time);
	DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP Tape_Ready\n"));
	return(0);
	/*NOTREACHED*/
    }

  pRequestSense = alloc(SIZEOF(RequestSense_T));

  /*
   * Ignore errors at this point
   */
  GenericRewind(fd);

  /*
   * Wait until we get an ready condition
   */

  done = 0;
  while (!done && (cnt < wait_time))
    {
      ret = SCSI_TestUnitReady(fd, pRequestSense );
      switch (ret)
	{
	case SCSI_OK:
	  done = 1;
	  break;
	case SCSI_SENSE:
	  switch (SenseHandler(fd, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
	    {
	    case SENSE_NO:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) SENSE_NO\n"));
	      done = 1;
	      break;
	    case SENSE_TAPE_NOT_ONLINE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
	      break;
	    case SENSE_IGNORE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) SENSE_IGNORE\n"));
	      done = 1;
	      break;
	    case SENSE_ABORT:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("TapeReady (TestUnitReady) SENSE_ABORT\n"));
	      amfree(pRequestSense);
	      return(-1);
	      /*NOTREACHED*/
	    case SENSE_RETRY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) SENSE_RETRY\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) default (SENSE)\n"));
	      done = 1;
	      break;
	    }
	  break;
	case SCSI_ERROR:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("TapeReady (TestUnitReady) SCSI_ERROR\n"));
	  free(pRequestSense);
	  return(-1);
	  /*NOTREACHED*/
	case SCSI_BUSY:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) SCSI_BUSY\n"));
	  break;
	case SCSI_CHECK:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeReady (TestUnitReady) SCSI_CHECK\n"));
	  break;
	default:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("TapeReady (TestUnitReady) unknown (%d)\n"),ret);
	  break;
	}
      sleep(1);
      cnt++;
    }

  amfree(pRequestSense);
  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("Tape_Ready after %d sec\n"), cnt);
  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP Tape_Ready\n"));
  return(0);
}


int
DecodeSCSI(
    CDB_T	CDB,
    char *	string)
{
  SC_COM_T *pSCSICommand;
  int x;

  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("##### START DecodeSCSI\n"));
  pSCSICommand = (SC_COM_T *)&SCSICommand;

  while (pSCSICommand->name != NULL)
    {
      if (CDB[0] == pSCSICommand->command)
        {
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%s %s"), string, pSCSICommand->name);
          for (x=0; x < pSCSICommand->length; x++)
            {
              DebugPrint(DEBUG_INFO, SECTION_SCSI," %02X", CDB[x]);
            }
          DebugPrint(DEBUG_INFO, SECTION_SCSI,"\n");
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP DecodeSCSI\n"));
          return(0);
	  /*NOTREACHED*/
	}
      pSCSICommand++;
    }
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("Not found %X\n"), CDB[0]);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP DecodeSCSI\n"));
  return(0);
}

int
DecodeModeSense(
    u_char *	buffer,
    size_t	offset,
    char *	pstring,
    char	block,
    FILE *	out)
{
  ReadWriteErrorRecoveryPage_T *prp;
  DisconnectReconnectPage_T *pdrp;
  size_t length = (size_t)buffer[0] - 4 - offset;

  (void)pstring;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START DecodeModeSense\n"));

  dump_hex(buffer, 255, DEBUG_INFO, SECTION_SCSI);

  /* Jump over the Parameter List header  and an offset if we have something
   * Unknown at the start (ADIC-218) at the moment
   *
   */
  buffer = buffer + 4 + offset;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("buffer length = %d\n"), length);

  if (block) /* Do we have an block descriptor page ?*/
    {
      if (out != NULL)
     	 g_fprintf(out, _("DecodeModeSense : Density Code %x\n"), (unsigned)buffer[0]);
      buffer++;

      if (out != NULL)
      	g_fprintf(out, _("DecodeModeSense : Number of Blocks %d\n"), V3(buffer));
      buffer = buffer + 4;

      if (out != NULL)
      	g_fprintf(out, _("DecodeModeSense : Block Length %d\n"), V3(buffer));
      buffer = buffer + 3;
    }

  while (length > 0)
    {
      switch (*buffer & 0x3f)
        {
        case 0:
            pVendorUnique = buffer;
            buffer++;
            break;
        case 0x1:
          prp = (ReadWriteErrorRecoveryPage_T *)buffer;
	  if (out != NULL)
          {
          	g_fprintf(out, _("DecodeModeSense : Read/Write Error Recovery Page\n"));
          	g_fprintf(out,_("\tTransfer Block            %d\n"), prp->tb);
          	g_fprintf(out,_("\tEnable Early Recovery     %d\n"), prp->eer);
          	g_fprintf(out,_("\tPost Error                %d\n"), prp->per);
          	g_fprintf(out,_("\tDisable Transfer on Error %d\n"), prp->dte);
          	g_fprintf(out,_("\tDisable ECC Correction    %d\n"), prp->dcr);
          	g_fprintf(out,_("\tRead Retry Count          %d\n"), prp->ReadRetryCount);
          	g_fprintf(out,_("\tWrite Retry Count         %d\n"), prp->WriteRetryCount);
	  }
          buffer++;
          break;
        case 0x2:
          pdrp = (DisconnectReconnectPage_T *)buffer;
	  if (out != NULL)
          {
          	g_fprintf(out, _("DecodeModeSense : Disconnect/Reconnect Page\n"));
          	g_fprintf(out,_("\tBuffer Full Ratio     %d\n"), pdrp->BufferFullRatio);
          	g_fprintf(out,_("\tBuffer Empty Ratio    %d\n"), pdrp->BufferEmptyRatio);
          	g_fprintf(out,_("\tBus Inactivity Limit  %d\n"),
                  V2(pdrp->BusInactivityLimit));
          	g_fprintf(out,_("\tDisconnect Time Limit %d\n"),
                  V2(pdrp->DisconnectTimeLimit));
          	g_fprintf(out,_("\tConnect Time Limit    %d\n"),
                  V2(pdrp->ConnectTimeLimit));
          	g_fprintf(out,_("\tMaximum Burst Size    %d\n"),
                  V2(pdrp->MaximumBurstSize));
          	g_fprintf(out,_("\tDTDC                  %d\n"), pdrp->DTDC);
	  }
          buffer++;
          break;
        case 0x1d:
          pEAAPage = (EAAPage_T *)buffer;
	  if (out != NULL)
	  {
          	g_fprintf(out,_("DecodeModeSense : Element Address Assignment Page\n"));
          	g_fprintf(out,_("\tMedium Transport Element Address     %d\n"),
                    V2(pEAAPage->MediumTransportElementAddress));
          	g_fprintf(out,_("\tNumber of Medium Transport Elements  %d\n"),
                    V2(pEAAPage->NoMediumTransportElements));
          	g_fprintf(out, _("\tFirst Storage Element Address       %d\n"),
                    V2(pEAAPage->FirstStorageElementAddress));
          	g_fprintf(out, _("\tNumber of  Storage Elements         %d\n"),
                    V2(pEAAPage->NoStorageElements));
          	g_fprintf(out, _("\tFirst Import/Export Element Address %d\n"),
                    V2(pEAAPage->FirstImportExportElementAddress));
          	g_fprintf(out, _("\tNumber of  ImportExport Elements    %d\n"),
                    V2(pEAAPage->NoImportExportElements));
          	g_fprintf(out, _("\tFirst Data Transfer Element Address %d\n"),
                    V2(pEAAPage->FirstDataTransferElementAddress));
          	g_fprintf(out, _("\tNumber of  Data Transfer Elements   %d\n"),
                    V2(pEAAPage->NoDataTransferElements));
	  }
          buffer++;
          break;
        case 0x1f:
          pDeviceCapabilitiesPage = (DeviceCapabilitiesPage_T *)buffer;
	  if (out != NULL)
	  {
          	g_fprintf(out, _("DecodeModeSense : MT can store data cartridges %d\n"),
                    pDeviceCapabilitiesPage->MT);
          	g_fprintf(out, _("DecodeModeSense : ST can store data cartridges %d\n"),
                    pDeviceCapabilitiesPage->ST);
          	g_fprintf(out, _("DecodeModeSense : IE can store data cartridges %d\n"),
                    pDeviceCapabilitiesPage->IE);
          	g_fprintf(out, _("DecodeModeSense : DT can store data cartridges %d\n"),
                    pDeviceCapabilitiesPage->DT);
          	g_fprintf(out, _("DecodeModeSense : MT to MT %d\n"),
                    pDeviceCapabilitiesPage->MT2MT);
          	g_fprintf(out, _("DecodeModeSense : MT to ST %d\n"),
                    pDeviceCapabilitiesPage->MT2ST);
          	g_fprintf(out, _("DecodeModeSense : MT to IE %d\n"),
                    pDeviceCapabilitiesPage->MT2IE);
          	g_fprintf(out, _("DecodeModeSense : MT to DT %d\n"),
                    pDeviceCapabilitiesPage->MT2DT);
          	g_fprintf(out, _("DecodeModeSense : ST to MT %d\n"),
                    pDeviceCapabilitiesPage->ST2ST);
          	g_fprintf(out, _("DecodeModeSense : ST to MT %d\n"),
                    pDeviceCapabilitiesPage->ST2ST);
          	g_fprintf(out, _("DecodeModeSense : ST to DT %d\n"),
                    pDeviceCapabilitiesPage->ST2DT);
          	g_fprintf(out, _("DecodeModeSense : IE to MT %d\n"),
                    pDeviceCapabilitiesPage->IE2MT);
          	g_fprintf(out, _("DecodeModeSense : IE to ST %d\n"),
                    pDeviceCapabilitiesPage->IE2IE);
          	g_fprintf(out, _("DecodeModeSense : IE to ST %d\n"),
                    pDeviceCapabilitiesPage->IE2DT);
          	g_fprintf(out, _("DecodeModeSense : IE to ST %d\n"),
                    pDeviceCapabilitiesPage->IE2DT);
          	g_fprintf(out, _("DecodeModeSense : DT to MT %d\n"),
                    pDeviceCapabilitiesPage->DT2MT);
          	g_fprintf(out, _("DecodeModeSense : DT to ST %d\n"),
                    pDeviceCapabilitiesPage->DT2ST);
          	g_fprintf(out, _("DecodeModeSense : DT to IE %d\n"),
                    pDeviceCapabilitiesPage->DT2IE);
          	g_fprintf(out, _("DecodeModeSense : DT to DT %d\n"),
                    pDeviceCapabilitiesPage->DT2DT);
	  }
          buffer++;
          break;
        default:
          buffer++;  /* set pointer to the length information */
          break;
        }
      /* Error if *buffer (length) is 0 */
      if (*buffer == 0)
        {
          /*           EAAPage = NULL; */
          /*           DeviceCapabilitiesPage = NULL; */
          return(-1);
	  /*NOTREACHED*/
        }
      length = length - (size_t)*buffer - 2;
      buffer = buffer + (size_t)*buffer + 1;
    }
  return(0);
}

int
DecodeSense(
    RequestSense_T *	sense,
    char *		pstring,
    FILE *		out)
{
  if (out == NULL)
    {
      return(0);
      /*NOTREACHED*/
    }
  g_fprintf(out,_("##### START DecodeSense\n"));
  g_fprintf(out,_("%sSense Keys\n"), pstring);
  if (sense->ErrorCode == 0x70)
    {
    g_fprintf(out,_("\tExtended Sense                     \n"));
    } else {
      g_fprintf(out,_("\tErrorCode                     %02x\n"), sense->ErrorCode);
      g_fprintf(out,_("\tValid                         %d\n"), sense->Valid);
    }
  g_fprintf(out,_("\tASC                           %02X\n"), sense->AdditionalSenseCode);
  g_fprintf(out,_("\tASCQ                          %02X\n"), sense->AdditionalSenseCodeQualifier);
  g_fprintf(out,_("\tSense key                     %02X\n"), sense->SenseKey);
  switch (sense->SenseKey)
    {
    case 0:
      g_fprintf(out,_("\t\tNo Sense\n"));
      break;
    case 1:
      g_fprintf(out,_("\t\tRecoverd Error\n"));
      break;
    case 2:
      g_fprintf(out,_("\t\tNot Ready\n"));
      break;
    case 3:
      g_fprintf(out,_("\t\tMedium Error\n"));
      break;
    case 4:
      g_fprintf(out,_("\t\tHardware Error\n"));
      break;
    case 5:
      g_fprintf(out,_("\t\tIllegal Request\n"));
      break;
    case 6:
      g_fprintf(out,_("\t\tUnit Attention\n"));
      break;
    case 7:
      g_fprintf(out,_("\t\tData Protect\n"));
      break;
    case 8:
      g_fprintf(out,_("\t\tBlank Check\n"));
      break;
    case 9:
      g_fprintf(out,_("\t\tVendor uniq\n"));
      break;
    case 0xa:
      g_fprintf(out,_("\t\tCopy Aborted\n"));
      break;
    case 0xb:
      g_fprintf(out,_("\t\tAborted Command\n"));
      break;
    case 0xc:
      g_fprintf(out,_("\t\tEqual\n"));
      break;
    case 0xd:
      g_fprintf(out,_("\t\tVolume Overflow\n"));
      break;
    case 0xe:
      g_fprintf(out,_("\t\tMiscompare\n"));
      break;
    case 0xf:
      g_fprintf(out,_("\t\tReserved\n"));
      break;
    }
  return(0);
}

int
DecodeExtSense(
    ExtendedRequestSense_T *	sense,
    char *			pstring,
    FILE *			out)
{
  ExtendedRequestSense_T *p;

  g_fprintf(out,_("##### START DecodeExtSense\n"));
  p = sense;

  g_fprintf(out,_("%sExtended Sense\n"), pstring);
  DecodeSense((RequestSense_T *)p, pstring, out);
  g_fprintf(out,_("\tLog Parameter Page Code         %02X\n"), sense->LogParameterPageCode);
  g_fprintf(out,_("\tLog Parameter Code              %02X\n"), sense->LogParameterCode);
  g_fprintf(out,_("\tUnderrun/Overrun Counter        %02X\n"), sense->UnderrunOverrunCounter);
  g_fprintf(out,_("\tRead/Write Error Counter        %d\n"), V3((char *)sense->ReadWriteDataErrorCounter));
  if (sense->AdditionalSenseLength > (u_char)sizeof(RequestSense_T))
    {
      if (sense->PF)
        g_fprintf(out,_("\tPower Fail\n"));
      if (sense->BPE)
        g_fprintf(out,_("\tSCSI Bus Parity Error\n"));
      if (sense->FPE)
        g_fprintf(out,_("\tFormatted Buffer parity Error\n"));
      if (sense->ME)
        g_fprintf(out,_("\tMedia Error\n"));
      if (sense->ECO)
        g_fprintf(out,_("\tError Counter Overflow\n"));
      if (sense->TME)
        g_fprintf(out,_("\tTapeMotion Error\n"));
      if (sense->TNP)
        g_fprintf(out,_("\tTape Not Present\n"));
      if (sense->LBOT)
        g_fprintf(out,_("\tLogical Beginning of tape\n"));
      if (sense->TMD)
        g_fprintf(out,_("\tTape Mark Detect Error\n"));
      if (sense->WP)
        g_fprintf(out,_("\tWrite Protect\n"));
      if (sense->FMKE)
        g_fprintf(out,_("\tFilemark Error\n"));
      if (sense->URE)
        g_fprintf(out,_("\tUnder Run Error\n"));
      if (sense->WEI)
        g_fprintf(out,_("\tWrite Error 1\n"));
      if (sense->SSE)
        g_fprintf(out,_("\tServo System Error\n"));
      if (sense->FE)
        g_fprintf(out,_("\tFormatter Error\n"));
      if (sense->UCLN)
        g_fprintf(out,_("\tCleaning Cartridge is empty\n"));
      if (sense->RRR)
        g_fprintf(out,_("\tReverse Retries Required\n"));
      if (sense->CLND)
        g_fprintf(out,_("\tTape Drive has been cleaned\n"));
      if (sense->CLN)
        g_fprintf(out,_("\tTape Drive needs to be cleaned\n"));
      if (sense->PEOT)
        g_fprintf(out,_("\tPhysical End of Tape\n"));
      if (sense->WSEB)
        g_fprintf(out,_("\tWrite Splice Error\n"));
      if (sense->WSEO)
        g_fprintf(out,_("\tWrite Splice Error\n"));
      g_fprintf(out,_("\tRemaing 1024 byte tape blocks   %d\n"), V3((char *)sense->RemainingTape));
      g_fprintf(out,_("\tTracking Retry Counter          %02X\n"), sense->TrackingRetryCounter);
      g_fprintf(out,_("\tRead/Write Retry Counter        %02X\n"), sense->ReadWriteRetryCounter);
      g_fprintf(out,_("\tFault Sympton Code              %02X\n"), sense->FaultSymptomCode);
    }
  return(0);
}

int
PrintInquiry(
    SCSIInquiry_T *	SCSIInquiry)
{
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START PrintInquiry\n"));
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %x\n"), "qualifier", SCSIInquiry->qualifier);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %x\n"), "type", SCSIInquiry->type);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %x\n"), "data_format", SCSIInquiry->data_format);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %X\n"), "ansi_version", SCSIInquiry->ansi_version);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %X\n"), "ecma_version", SCSIInquiry->ecma_version);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %X\n"), "iso_version", SCSIInquiry->iso_version);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %X\n"), "type_modifier", SCSIInquiry->type_modifier);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %x\n"), "removable", SCSIInquiry->removable);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %.8s\n"), "vendor_info", SCSIInquiry->vendor_info);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %.16s\n"), "prod_ident", SCSIInquiry->prod_ident);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %.4s\n"), "prod_version", SCSIInquiry->prod_version);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("%-15s %.19s\n"), "vendor_specific", SCSIInquiry->vendor_specific);
  return(0);
}


int
DoNothing0(void)
{
  dbprintf(_("##### START DoNothing\n"));
  return(0);
}

int
DoNothing1(
    int		unused1)
{
  (void)unused1;	/* Quiet unused parameter warning */

  dbprintf(_("##### START DoNothing\n"));
  return(0);
}

int
DoNothing2(
    int		unused1,
    int		unused2)
{
  (void)unused1;	/* Quiet unused parameter warning */
  (void)unused2;	/* Quiet unused parameter warning */

  dbprintf(_("##### START DoNothing\n"));
  return(0);
}

int
DoNothing3(
    int		unused1,
    int		unused2,
    int		unused3)
{
  (void)unused1;	/* Quiet unused parameter warning */
  (void)unused2;	/* Quiet unused parameter warning */
  (void)unused3;	/* Quiet unused parameter warning */

  dbprintf(_("##### START DoNothing\n"));
  return(0);
}

int
GenericFree(void)
{
  dbprintf(_("##### START GenericFree\n"));
  return(0);
}

int
GenericSearch(void)
{
  dbprintf(_("##### START GenericSearch\n"));
  return(0);
}

int
TreeFrogBarCode(
    int DeviceFD)
{
  extern OpenFiles_T *pDev;

  ModePageTreeFrogVendorUnique_T *pVendor;

  dbprintf(_("##### START TreeFrogBarCode\n"));
  if (pModePage == NULL)
    {
      pModePage = alloc(0xff);
    }

  if (SCSI_ModeSense(DeviceFD, pModePage, 0xff, 0x0, 0x3f) == 0)
    {
      DecodeModeSense(pModePage, 0, _("TreeFrogBarCode :"), 0, debug_file);

      if (pVendorUnique == NULL)
      {
         dbprintf(_("TreeFrogBarCode : no pVendorUnique\n"));
         return(0);
	 /*NOTREACHED*/
      }
      pVendor = ( ModePageTreeFrogVendorUnique_T *)pVendorUnique;

      dbprintf(_("TreeFrogBarCode : EBARCO %d\n"), pVendor->EBARCO);
      dbprintf(_("TreeFrogCheckSum : CHKSUM  %d\n"), pVendor->CHKSUM);

      dump_hex((u_char *)pDev[INDEX_CHANGER].inquiry, INQUIRY_SIZE, DEBUG_INFO, SECTION_ELEMENT);
      return(pVendor->EBARCO);
      /*NOTREACHED*/
    }
  return(0);
}

int
EXB_BarCode(
    int		DeviceFD)
{
  extern OpenFiles_T *pDev;

  ModePageEXB120VendorUnique_T *pVendor;
  ModePageEXB120VendorUnique_T *pVendorWork;

  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### START EXB_BarCode\n"));
  if (pModePage == NULL && LibModeSenseValid == 0)
    {
      pModePage = alloc(0xff);

      if (SCSI_ModeSense(DeviceFD, pModePage, 0xff, 0x8, 0x3f) == 0)
	{
	  DecodeModeSense(pModePage, 0, _("EXB_BarCode :"), 0, debug_file);
	  LibModeSenseValid = 1;
	} else {
	  LibModeSenseValid = -1;
	}
    }

  if (LibModeSenseValid == 1)
    {
      if (pVendorUnique == NULL)
	{
         DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : no pVendorUnique\n"));
         return(0);
	 /*NOTREACHED*/
      }
      pVendor = ( ModePageEXB120VendorUnique_T *)pVendorUnique;

      DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : NBL %d\n"), pVendor->NBL);
      DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : PS  %d\n"), pVendor->PS);
      if (pVendor->NBL == 1 && pVendor->PS == 1 )
        {
          pVendorWork = alloc((size_t)pVendor->ParameterListLength + 2);
          DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : setting NBL to 1\n"));
          memcpy(pVendorWork, pVendor, (size_t)pVendor->ParameterListLength + 2);
          pVendorWork->NBL = 0;
          pVendorWork->PS = 0;
          pVendorWork->RSVD0 = 0;
          if (SCSI_ModeSelect(DeviceFD, (u_char *)pVendorWork, (u_char)(pVendorWork->ParameterListLength + 2), 0, 1, 0) == 0)
            {
              DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : SCSI_ModeSelect OK\n"));
              /* Hack !!!!!!
               */
              pVendor->NBL = 0;

              /* And now again !!!
               */
              GenericResetStatus(DeviceFD);
            } else {
              DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : SCSI_ModeSelect failed\n"));
            }
            amfree(pVendorWork);
        }
      dump_hex((u_char *)pDev[INDEX_CHANGER].inquiry, INQUIRY_SIZE, DEBUG_INFO, SECTION_BARCODE);
      DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("EXB_BarCode : vendor_specific[19] %x\n"),
		 pDev[INDEX_CHANGER].inquiry->vendor_specific[19]);
    }
  return(1);
}

int
NoBarCode(
    int DeviceFD)
{
  (void)DeviceFD;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### START NoBarCode\n"));
  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### STOP  NoBarCode\n"));
  return(0);
}

int
GenericBarCode(
    int		DeviceFD)
{
  (void)DeviceFD;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### START GenericBarCode\n"));
  if ( changer->havebarcode  >= 1)
    {
      DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### STOP GenericBarCode (havebarcode) => %d\n"),changer->havebarcode);
      return(1);
      /*NOTREACHED*/
    }
  DebugPrint(DEBUG_INFO, SECTION_BARCODE,_("##### STOP GenericBarCode => 0\n"));
  return(0);
}

int
SenseHandler(
    int			DeviceFD,
    u_char		flag,
    u_char		SenseKey,
    u_char		AdditionalSenseCode,
    u_char		AdditionalSenseCodeQualifier,
    RequestSense_T *	buffer)
{
  extern OpenFiles_T *pDev;
  int ret = 0;
  dbprintf(_("##### START SenseHandler\n"));
  if (pDev[DeviceFD].inqdone == 1)
    {
      dbprintf(_("Ident = [%s], function = [%s]\n"), pDev[DeviceFD].ident,
		pDev[DeviceFD].functions->ident);
      ret = pDev[DeviceFD].functions->function_error(DeviceFD, flag, SenseKey, AdditionalSenseCode, AdditionalSenseCodeQualifier, buffer);
    } else {
      dbprintf(_("    Ups no sense\n"));
    }
  dbprintf(_("#### STOP SenseHandler\n"));
  return(ret);
}

/*
 * Try to get information about the tape,
 * Tape loaded ? Online etc
 * Use the mtio ioctl to get the information if no SCSI Path
 * to the tape drive is available.
 *
 * TODO:
 * Pass an parameter to identify which unit to use
 * if there are more than one
 * Implement the SCSI path if available
*/
int
TapeStatus(void)
{
  extern OpenFiles_T *pDev;
  int ret;
  int done;
  int cnt;
  RequestSense_T *pRequestSense;

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### START TapeStatus\n"));

  /*
   * If it is an device which understand SCSI commands the
   * normal ioctl (MTIOCGET for example) may fail
   * So try an Inquiry
   */
  if (pDev[INDEX_TAPECTL].SCSI == 1)
    {
      pRequestSense = alloc(SIZEOF(RequestSense_T));
      memset(pRequestSense, 0, SIZEOF(RequestSense_T));

      for (done = 0, cnt = 0; !done && (cnt < 60); cnt++)
	{
	  ret = SCSI_TestUnitReady(INDEX_TAPECTL, pRequestSense);
	  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("TapeStatus TestUnitReady ret %d\n"),ret);
	  switch (ret)
	    {
	    case SCSI_OK:
	    case SCSI_SENSE:
	      switch (SenseHandler(INDEX_TAPECTL, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
		{
		case SENSE_IGNORE:
		case SENSE_NO:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeStatus (TestUnitReady) SENSE_NO\n"));
		  pDTE[0].status = 'F';
		  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### FULL\n"));
		  done = 1;
		  break;

		case SENSE_TAPE_NOT_ONLINE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeStatus (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
		  pDTE[0].status = 'E';
		  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### EMPTY\n"));
		  done = 1;
		  break;

		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("TapeStatus (TestUnitReady) SENSE_ABORT\n"));
		  done = 1;
		  break;

		case SENSE_RETRY:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeStatus (TestUnitReady) SENSE_RETRY\n"));
		  break;

		default:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeStatus (TestUnitReady) default (SENSE)\n"));
		  break;
		}
	      break;

	    case SCSI_ERROR:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("TapeStatus (TestUnitReady) SCSI_ERROR\n"));
	      done = 1;
	      break;

	    case SCSI_BUSY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeStatus (TestUnitReady) SCSI_BUSY\n"));
	      break;

	    case SCSI_CHECK:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("TapeStatus (TestUnitReady) SCSI_CHECK\n"));
	      break;

	    default:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("TapeStatus (TestUnitReady) unknown (%d)\n"),ret);
	      break;

	    }
	  if (!done)
	    sleep(2);
	}
        amfree(pRequestSense);
    } else {
      ret = Tape_Status(INDEX_TAPE);
      if ( ret & TAPE_ONLINE)
	{
	  pDTE[0].status ='F';
	  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### FULL\n"));
	} else {
	  pDTE[0].status = 'E';
	  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### EMPTY\n"));
	}
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP TapeStatus\n"));
    }
    return(0);
}

int
DLT4000Eject(
    char *	Device,
    int		type)
{
  extern OpenFiles_T *pDev;

  RequestSense_T *pRequestSense;
  ExtendedRequestSense_T *pExtendedRequestSense;
  int ret;
  int cnt = 0;
  int done;

  (void)Device;	/* Quiet unused parameter warning */

  dbprintf(_("##### START DLT4000Eject\n"));

  pRequestSense = alloc(SIZEOF(RequestSense_T));
  pExtendedRequestSense = alloc(SIZEOF(ExtendedRequestSense_T));

  if ( type > 1)
    {
      dbprintf(_("DLT4000Eject : use mtio ioctl for eject on %s\n"), pDev[INDEX_TAPE].dev);
      free(pExtendedRequestSense);
      free(pRequestSense);
      return(Tape_Ioctl(INDEX_TAPE, IOCTL_EJECT));
      /*NOTREACHED*/
    }



  if (pDev[INDEX_TAPECTL].SCSI == 0)
    {
      dbprintf(_("DLT4000Eject : Device %s not able to receive SCSI commands\n"), pDev[INDEX_TAPE].dev);
      free(pExtendedRequestSense);
      free(pRequestSense);
      return(Tape_Ioctl(INDEX_TAPE, IOCTL_EJECT));
      /*NOTREACHED*/
    }


  dbprintf(_("DLT4000Eject : SCSI eject on %s = %s\n"), pDev[INDEX_TAPECTL].dev, pDev[INDEX_TAPECTL].ConfigName);

  RequestSense(INDEX_TAPECTL, pExtendedRequestSense, 0);
  DecodeExtSense(pExtendedRequestSense, _("DLT4000Eject : "), debug_file);
  /* Unload the tape, 0 ==  wait for success
   * 0 == unload
   */
  ret = SCSI_LoadUnload(INDEX_TAPECTL, pRequestSense, 0, 0);

  RequestSense(INDEX_TAPECTL, pExtendedRequestSense, 0);
  DecodeExtSense(pExtendedRequestSense, _("DLT4000Eject : "), debug_file);

  /* < 0 == fatal */
  if (ret >= 0) {
      free(pExtendedRequestSense);
      free(pRequestSense);
      return(-1);
      /*NOTREACHED*/
    }

  done = 0;
  while (!done && cnt < 300)
    {
      ret = SCSI_TestUnitReady(INDEX_TAPECTL, pRequestSense);
      DebugPrint(DEBUG_INFO, SECTION_SCSI, _("DLT4000Eject TestUnitReady ret %d\n"),ret);
      switch (ret)
	{
	case SCSI_OK:
	  done = 1;
	  break;
	case SCSI_SENSE:
	  switch (SenseHandler(INDEX_TAPECTL, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
	    {
	    case SENSE_NO:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SENSE_NO\n"));
	      done = 1;
	      break;
	    case SENSE_TAPE_NOT_ONLINE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
	      done = 1;
	      break;
	    case SENSE_IGNORE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SENSE_IGNORE\n"));
	      done = 1;
	      break;
	    case SENSE_ABORT:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SENSE_ABORT\n"));
	      free(pExtendedRequestSense);
	      free(pRequestSense);
	      return(-1);
	      /*NOTREACHED*/
	    case SENSE_RETRY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SENSE_RETRY\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) default (SENSE)\n"));
	      done = 1;
	      break;
	    }
	  break;
	case SCSI_ERROR:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SCSI_ERROR\n"));
	  free(pExtendedRequestSense);
	  free(pRequestSense);
	  return(-1);
	  /*NOTREACHED*/
	case SCSI_BUSY:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SCSI_BUSY\n"));
	  break;
	case SCSI_CHECK:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) SCSI_CHECK\n"));
	  break;
	default:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("DLT4000Eject (TestUnitReady) unknown (%d)\n"),ret);
	  break;
	}

      cnt++;
      sleep(2);
    }

  dbprintf(_("DLT4000Eject : Ready after %d sec, done = %d\n"), cnt * 2, done);

  free(pExtendedRequestSense);
  free(pRequestSense);

  return(0);
}

/*
 * Ejects an tape either with the ioctl interface
 * or by using the SCSI interface if available.
 *
 * TODO:
 * Before unload check if there is an tape in the drive
 *
 */
int
GenericEject(
    char *	Device,
    int		type)
{
  extern OpenFiles_T *pDev;
  RequestSense_T *pRequestSense;
  int ret;
  int cnt = 0;
  int done;

  (void)Device;	/* Quiet unused parameter warning */
  (void)type;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_TAPE, _("##### START GenericEject\n"));

  pRequestSense = alloc(SIZEOF(RequestSense_T));

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("GenericEject : SCSI eject on %s = %s\n"),
             pDev[INDEX_TAPECTL].dev, pDev[INDEX_TAPECTL].ConfigName);

  /*
   * Can we use SCSI commands ?
   */
  if (pDev[INDEX_TAPECTL].SCSI == 1)
    {
      LogSense(INDEX_TAPECTL);
      /*
       * Unload the tape, 1 == don't wait for success
       * 0 == unload
       */
      ret = SCSI_LoadUnload(INDEX_TAPECTL, pRequestSense, 1, 0);

      /* < 0 == fatal */
      if (ret < 0) {
        DebugPrint(DEBUG_INFO, SECTION_SCSI,"GenericEject SCSI_LoadUnload failed\n");
	free(pRequestSense);
	return(-1);
	/*NOTREACHED*/
      }

      done = 0;
      while (!done && cnt < 300)
	{
	  ret = SCSI_TestUnitReady(INDEX_TAPECTL, pRequestSense);
	  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("GenericEject TestUnitReady ret %d\n"),ret);
	  switch (ret)
	    {
	    case SCSI_OK:
	    case SCSI_SENSE:
	      switch (SenseHandler(INDEX_TAPECTL, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
		{
		case SENSE_NO:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) SENSE_NO\n"));
		  break;
		case SENSE_TAPE_NOT_ONLINE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
		  done = 1;
		  break;
		case SENSE_IGNORE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) SENSE_IGNORE\n"));
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericEject (TestUnitReady) SENSE_ABORT\n"));
		  free(pRequestSense);
		  return(-1);
		  /*NOTREACHED*/
		case SENSE_RETRY:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) SENSE_RETRY\n"));
		  break;
		default:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) default (SENSE)\n"));
		  break;
		}
	      break;
	    case SCSI_ERROR:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericEject (TestUnitReady) SCSI_ERROR\n"));
	      free(pRequestSense);
	      return(-1);
	      /*NOTREACHED*/
	    case SCSI_BUSY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) SCSI_BUSY\n"));
	      break;
	    case SCSI_CHECK:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericEject (TestUnitReady) SCSI_CHECK\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericEject (TestUnitReady) unknown (%d)\n"),ret);
	      break;
	    }
	  cnt++;
	  sleep(2);
	}
    } else {
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("GenericEject : Device can't understand SCSI try ioctl\n"));
      Tape_Ioctl(INDEX_TAPECTL, IOCTL_EJECT);
    }
  DebugPrint(DEBUG_INFO, SECTION_TAPE,
  	     _("GenericEject : Ready after %d sec\n"), cnt * 2);
  free(pRequestSense);
  return(0);
}

/*
 * Rewind the tape
 *
 * TODO:
 * Make the retry counter an config option,
 *
 * Return:
 * -1 -> error
 * 0  -> success
 */
int
GenericRewind(
    int		DeviceFD)
{
  CDB_T CDB;
  extern OpenFiles_T *pDev;
  RequestSense_T *pRequestSense;
  int ret;
  int cnt = 0;
  int done;

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### START GenericRewind pDEV -> %d\n"),DeviceFD);


  /*
   * If we can use the SCSI device than use it, else use the ioctl
   * function
   */
  if (pDev[DeviceFD].SCSI == 1)
    {
      pRequestSense = alloc(SIZEOF(RequestSense_T));

      /*
       * Before doing the rewind check if the tape is ready to accept commands
       */

      done = 0;
      while (!done)
	{
	  ret = SCSI_TestUnitReady(DeviceFD, (RequestSense_T *)pRequestSense );
	  DebugPrint(DEBUG_INFO, SECTION_TAPE, _("GenericRewind (TestUnitReady) ret %d\n"),ret);
	  switch (ret)
	    {
	    case SCSI_OK:
	      done = 1;
	      break;
       	    case SCSI_SENSE:
	      switch (SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
		{
		case SENSE_NO:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_NO\n"));
		  done = 1;
		  break;
		case SENSE_TAPE_NOT_ONLINE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
		  free(pRequestSense);
		  return(-1);
		  /*NOTREACHED*/
		case SENSE_IGNORE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_IGNORE\n"));
		  done = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_ABORT\n"));
		  free(pRequestSense);
		  return(-1);
		  /*NOTREACHED*/
		case SENSE_RETRY:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_RETRY\n"));
		  break;
		default:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) default (SENSE)\n"));
		  done = 1;
		  break;
		}  /* switch (SenseHandler(DeviceFD, 0, pRequestSense->SenseKey.... */
	      break;

	    case SCSI_ERROR:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericRewind (TestUnitReady) SCSI_ERROR\n"));
	      free(pRequestSense);
	      return(-1);
	      /*NOTREACHED*/

	    case SCSI_BUSY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SCSI_BUSY\n"));
	      break;
	    case SCSI_CHECK:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SCSI_CHECK\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericRewind (TestUnitReady) unknown (%d)\n"),ret);
	      break;
	    }

	  sleep(1);
	  DebugPrint(DEBUG_INFO, SECTION_TAPE,_(" Wait .... (%d)\n"),cnt);
	  if (cnt > 180)
	    {
	      DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("##### STOP GenericRewind (-1)\n"));
	      free(pRequestSense);
	      return(-1);
	      /*NOTREACHED*/
	    }
	} /* while !done */

      cnt = 0;

      CDB[0] = SC_COM_REWIND;
      CDB[1] = 1;
      CDB[2] = 0;
      CDB[3] = 0;
      CDB[4] = 0;
      CDB[5] = 0;

      done = 0;
      while (!done)
	{
	  ret = SCSI_Run(DeviceFD, Input, CDB, 6,
			 NULL, 0,
			 pRequestSense,
			 SIZEOF(RequestSense_T));

	  DecodeSense(pRequestSense, _("GenericRewind : "), debug_file);

	  if (ret > 0)
	    {
	      if (pRequestSense->SenseKey != UNIT_ATTENTION)
		{
		  done = 1;
		}
	    }
	  if (ret == 0)
	    {
	      done = 1;
	    }
	  if (ret < 0)
	    {
	      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("GenericRewind : failed %d\n"), ret);
	      done = 1;
	    }
	}

      done = 0;
      while (!done && (cnt < 300))
	{
	  ret = SCSI_TestUnitReady(DeviceFD, pRequestSense);
	  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("GenericRewind TestUnitReady ret %d\n"),ret);
	  switch (ret)
	    {
	    case SCSI_OK:
	      done = 1;
	      break;
	    case SCSI_SENSE:
	      switch (SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
		{
		case SENSE_NO:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_NO\n"));
		  done = 1;
		  break;
		case SENSE_TAPE_NOT_ONLINE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
		  free(pRequestSense);
		  return(-1);
		  /*NOTREACHED*/
		case SENSE_IGNORE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_IGNORE\n"));
		  done = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_ABORT\n"));
		  free(pRequestSense);
		  return(-1);
		  /*NOTREACHED*/
		case SENSE_RETRY:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SENSE_RETRY\n"));
		  break;
		default:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) default (SENSE)\n"));
		  done = 1;
		  break;
		}
	      break;
	    case SCSI_ERROR:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericRewind (TestUnitReady) SCSI_ERROR\n"));
	      return(-1);
	      /*NOTREACHED*/

	    case SCSI_BUSY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SCSI_BUSY\n"));
	      break;
	    case SCSI_CHECK:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("GenericRewind (TestUnitReady) SCSI_CHECK\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("GenericRewind (TestUnitReady) unknown (%d)\n"),ret);
	      break;
	    }

	  cnt++;
	  sleep(2);
	}

      amfree(pRequestSense);

      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("GenericRewind : Ready after %d sec, "
      			"done = %d\n"), cnt * 2, done);
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP GenericRewind (0)\n"));
    } else {
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("GenericRewind : use ioctl rewind\n"));
      if (pDev[DeviceFD].devopen == 1)
	{
	  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("Close Device\n"));
	  SCSI_CloseDevice(DeviceFD);
	}
      /* no actual rewind operation here -- the device itself will handle that */
      DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP GenericRewind (0)\n"));
    }

  return(0);
}


/*
 * Check if the tape has the tape clean
 * bit set in the return of an request sense
 *
 */
int
GenericClean(
    char *	Device)
{
  extern OpenFiles_T *pDev;
  ExtendedRequestSense_T ExtRequestSense;
  int ret = 0;

  (void)Device;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### START GenericClean\n"));
  if (pDev[INDEX_TAPECTL].SCSI == 0)
      {
          DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("GenericClean : can't send SCSI commands\n"));
	  DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("##### STOP GenericClean\n"));
          return(0);
	  /*NOTREACHED*/
      }

  /*
   * Request Sense Data, reset the counter
   */
  if ( RequestSense(INDEX_TAPECTL, &ExtRequestSense, 1) == 0)
    {

      DecodeExtSense(&ExtRequestSense, _("GenericClean : "), debug_file);
      if(ExtRequestSense.CLN) {
	ret = 1;
      } else {
	ret = 0;
      }
    } else {
      DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("Got error from RequestSense\n"));
    }
  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP GenericClean (%d)\n"),ret);
  return(ret);
}

int
SCSI_LogSenseClean(
    char *	Device)
{ 
  extern OpenFiles_T *pDev;
  CDB_T CDB;
  RequestSense_T *pRequestSense;
  int ret = 0;
  u_char *buffer;
  size_t size = 128;
      
  (void)Device;	/* Quiet unused parameter warning */
  DebugPrint(DEBUG_INFO, SECTION_TAPE,"##### START SCSI_LogSenseClean\n");
  if (pDev[INDEX_TAPECTL].SCSI == 0)
      {
          DebugPrint(DEBUG_ERROR, SECTION_TAPE,"SCSILogSenseClean : can't send SCSI commands\n");
	  DebugPrint(DEBUG_ERROR, SECTION_TAPE,"##### STOP SCSI_LogSenseClean\n");
          return(0);
	  /*NOTREACHED*/
      }

   if (NULL ==  (buffer = alloc(size))){
          DebugPrint(DEBUG_ERROR, SECTION_TAPE,"SCSI_LogSenseClean : can't alloc buffer\n");
	  DebugPrint(DEBUG_ERROR, SECTION_TAPE,"##### STOP SCSI_LogSenseClean\n");
          return(0);
   }
   if (NULL == (pRequestSense = alloc(SIZEOF(RequestSense_T)))){
          DebugPrint(DEBUG_ERROR, SECTION_TAPE,"SCSI_LogSenseClean : can't alloc memory\n");
	  DebugPrint(DEBUG_ERROR, SECTION_TAPE,"##### STOP SCSI_LogSenseClean\n");
          return(0);
   }
   
   memset(buffer, 0, size);
   CDB[0] = SC_COM_LOG_SENSE;
   CDB[1] = 0;
   CDB[2] = (u_char)(0x40 | 0x33);/* 0x40 for current values 0x33 Head Cleaning Page*/
   CDB[3] = 0;
   CDB[4] = 0;
   CDB[5] = 0;
   CDB[6] = 00;
   MSB2(&CDB[7], size);
   CDB[9] = 0;

   if (SCSI_Run(INDEX_TAPECTL, Input, CDB, 10,
                           buffer,
                           size,
                           pRequestSense,
                           SIZEOF(RequestSense_T)) != 0)
     {
       DecodeSense(pRequestSense, "SCSI_LogSenseClean : ",debug_file);
       free(pRequestSense);
       free(buffer);
       DebugPrint(DEBUG_ERROR, SECTION_TAPE,"##### STOP SCSI_LogSenseClean (0) Page could not be read.\n");
       return(0);
       /*NOTREACHED*/
     }
  if (1==(0x1 & buffer[8])){ /* Bit 0 of the 4th byte in the Clean Head Log Parameter, which are the bytes */
		            /* 4 to 8 on the Log Sense Page 0x33 					  */
    ret = 1;
  }else {
    ret = 0; 
  }  
  DebugPrint(DEBUG_INFO, SECTION_TAPE,"##### STOP SCSI_LogSenseClean (%d)\n",ret);
  free(pRequestSense);
  free(buffer);
  return(ret);
}

int
GenericResetStatus(
    int		DeviceFD)
{
  CDB_T CDB;
  RequestSense_T *pRequestSense;
  int ret = 0;
  int retry = 1;

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("##### START GenericResetStatus\n"));

  pRequestSense = alloc(SIZEOF(RequestSense_T));

  while (retry)
    {
      CDB[0] = SC_COM_IES;   /* */
      CDB[1] = 0;
      CDB[2] = 0;
      CDB[3] = 0;
      CDB[4] = 0;
      CDB[5] = 0;


      ret = SCSI_Run(DeviceFD, Input, CDB, 6,
                                NULL, 0,
                                pRequestSense,
                                SIZEOF(RequestSense_T));

      if (ret < 0)
        {
          /*        g_fprintf(stderr, _("%s: Request Sense[Inquiry]: %02X"), */
          /*                "chs", ((u_char *) &pRequestSense)[0]); */
          /*        for (i = 1; i < SIZEOF(RequestSense_T); i++)                */
          /*          g_fprintf(stderr, " %02X", ((u_char *) &pRequestSense)[i]); */
          /*        g_fprintf(stderr, "\n");    */
	  free(pRequestSense);
          return(ret);
	  /*NOTREACHED*/
        }
      if ( ret > 0 )
        {
          switch (SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
            {
            case SENSE_IGNORE:
	      free(pRequestSense);
              return(0);
              /*NOTREACHED*/
            case SENSE_ABORT:
	      free(pRequestSense);
              return(-1);
              /*NOTREACHED*/
            case SENSE_RETRY:
              retry++;
              if (retry < MAX_RETRIES )
                {
                  DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("GenericResetStatus : retry %d\n"), retry);
                  sleep(2);
                } else {
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericResetStatus : return (-1)\n"));
		  free(pRequestSense);
                  return(-1);
		  /*NOTREACHED*/
                }
              break;
            default:
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericResetStatus :  (default) return (-1)\n"));
	      free(pRequestSense);
              return(-1);
              /*NOTREACHED*/
            }
        }
      if (ret == 0)
        retry = 0;
    }
  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("##### STOP GenericResetStatus (%d)\n"),ret);
  free(pRequestSense);
  return(ret);
}

/* GenericSenseHandler
 * Handles the conditions/sense wich is returned by an SCSI command
 * pwork is an pointer to the structure OpenFiles_T, which is filled with information
 * about the device to which we talk. Information are for example
 * The vendor, the ident, which fd, etc. This strucure is filled when we open the
 * device
 * flag tells how to handle the information passed in the buffer,
 * 0 -> Sense Key available
 * 1 -> No Sense key available
 * buffer is a pointer to the data from the request sense result.
 *
 * TODO:
 * Limit recursion, may run in an infinite loop
 */
int
GenericSenseHandler(
    int			ip,
    u_char		flag,
    u_char		SenseKey,
    u_char		AdditionalSenseCode,
    u_char		AdditionalSenseCodeQualifier,
    RequestSense_T *	pRequestSense)
{
  extern OpenFiles_T *pDev;
  int ret;
  char *info = NULL;

  dbprintf(_("##### START GenericSenseHandler\n"));

  DecodeSense(pRequestSense, _("GenericSenseHandler : "), debug_file);

  ret = Sense2Action(pDev[ip].ident,
		     pDev[ip].inquiry->type,
		     flag, SenseKey,
		     AdditionalSenseCode,
		     AdditionalSenseCodeQualifier,
		     &info);

  dbprintf(_("##### STOP GenericSenseHandler: %s\n"), _(info));
  return(ret);
}

/*
 * Do the move. We don't address the MTE element (the gripper)
 * here. We assume that the library use the right MTE.
 * The difference to GenericMove is that we do an align element
 * before the move.
 *
 * Return:
 *         == 0 -> success
 *         != 0 -> error either from the SCSI command or from
 *                 the element handling
 * TODO:
*/
int
SDXMove(
    int		DeviceFD,
    int		from,
    int		to)
{
  extern OpenFiles_T *pDev;
  ElementInfo_T *pfrom;
  ElementInfo_T *pto;
  int ret;
  int tapestat;
  int moveok;
  int SDX_MTE = 0;      /* This are parameters  passed */
  int SDX_STE = -1;     /* to                          */
  int SDX_DTE = -1;     /* AlignElements               */

  DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### START SDXMove\n"));

  DebugPrint(DEBUG_INFO, SECTION_MOVE,_("%-20s : from = %d, to = %d\n"), "SDXMove", from, to);


  if ((pfrom = LookupElement(from)) == NULL)
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("SDXMove : ElementInfo for %d not found\n"), from);
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
      return(-1);
      /*NOTREACHED*/
    }

  if ((pto = LookupElement(to)) == NULL)
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("SDXMove : ElementInfo for %d not found\n"), to);
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
      return(-1);
      /*NOTREACHED*/
    }

  if (pfrom->status == 'E')
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("SDXMove : from %d is empty\n"), from);
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
      return(-1);
      /*NOTREACHED*/
    }

  if (pto->status == 'F')
    {
      switch (pto->status)
      {
         case CHANGER:
           break;
         case STORAGE:
           DebugPrint(DEBUG_INFO, SECTION_MOVE,_("SDXMove : Destination Element %d Type %d is full\n"),
                pto->address, pto->type);
            to = find_empty(DeviceFD, 0, 0);
	    if (to == -1 )
	    {
		    DebugPrint(DEBUG_ERROR, SECTION_MOVE,_("SDXMove : no empty slot found for unload\n"));
		    return(-1);
		    /*NOTREACHED*/
	    }
            DebugPrint(DEBUG_INFO, SECTION_MOVE,_("SDXMove : Unload to %d\n"), to);
            if ((pto = LookupElement(to)) == NULL)
            {
	      DebugPrint(DEBUG_INFO, SECTION_MOVE, _("SDXMove : ElementInfo for %d not found\n"), to);
	      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
	      return(-1);
	      /*NOTREACHED*/
            }
           break;
         case IMPORT:
           break;
         case TAPETYPE:
           break;
      }
    }

  moveok = CheckMove(pfrom, pto);

  switch (pto->type)
  {
    case TAPETYPE:
      SDX_DTE = pto->address;
      break;
    case STORAGE:
     SDX_STE = pto->address;
     break;
    case IMPORT:
     SDX_STE = pto->address;
     break;
  }

  switch (pfrom->type)
  {
    case TAPETYPE:
      SDX_DTE = pfrom->address;
      break;
    case STORAGE:
     SDX_STE = pfrom->address;
     break;
    case IMPORT:
     SDX_STE = pfrom->address;
     break;
  }

  if (SDX_DTE >= 0 && SDX_STE >= 0)
  {
    ret = SCSI_AlignElements(DeviceFD, SDX_MTE, SDX_DTE, SDX_STE);
    DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### SCSI_AlignElemnts ret = %d\n"),ret);
    if (ret != 0 )
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
      return(-1);
      /*NOTREACHED*/
    }
  } else {
    DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### Error setting STE/DTE %d/%d\n"), SDX_STE, SDX_DTE);
    DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
    return(-1);
    /*NOTREACHED*/
  }

  /*
   * If from is a tape we must check if it is loaded
   * and if yes we have to eject it
  */
  if (pfrom->type == TAPETYPE)
  {
    tapestat = Tape_Status(INDEX_TAPE);
    if ( tapestat & TAPE_ONLINE)
    {
      if (pDev[INDEX_TAPECTL].SCSI == 1)
      {
        ret = eject_tape(pDev[INDEX_TAPECTL].dev,1);
      } else {
        ret = eject_tape(pDev[INDEX_TAPE].dev,2);
      }
    }
  }

  if ((ret == 0) && moveok)
  {
    ret = SCSI_Move(DeviceFD, 0, from, to);
  } else {
    DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
    return(ret);
    /*NOTREACHED*/
  }
  DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP SDXMove\n"));
  return(ret);
}

/*
 * Do the move. We don't address the MTE element (the gripper)
 * here. We assume that the library use the right MTE
 *
 * Return:
 *         == 0 -> success
 *         != 0 -> error either from the SCSI command or from
 *                 the element handling
 * TODO:
*/
int
GenericMove(
    int		DeviceFD,
    int		from,
    int		to)
{
  ElementInfo_T *pfrom;
  ElementInfo_T *pto;
  int ret = 0;

  DebugPrint(DEBUG_INFO, SECTION_MOVE, _("##### START GenericMove\n"));

  DebugPrint(DEBUG_INFO, SECTION_MOVE, _("%-20s : from = %d, to = %d\n"), "GenericMove", from, to);


  if ((pfrom = LookupElement(from)) == NULL)
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE, _("GenericMove : ElementInfo for %d not found\n"), from);
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP GenericMove\n"));
      return(-1);
      /*NOTREACHED*/
    }

  if ((pto = LookupElement(to)) == NULL)
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE, _("GenericMove : ElementInfo for %d not found\n"), to);
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP GenericMove\n"));
      return(-1);
      /*NOTREACHED*/
    }

  if (pfrom->status == 'E')
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE, _("GenericMove : from %d is empty\n"), from);
      DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP GenericMove\n"));
      return(-1);
      /*NOTREACHED*/
    }

  if (pto->status == 'F')
    {
      DebugPrint(DEBUG_INFO, SECTION_MOVE, _("GenericMove : Destination Element %d Type %d is full\n"),
		 pto->address, pto->type);
      to = find_empty(DeviceFD, 0, 0);
      if ( to == -1)
      {
	      DebugPrint(DEBUG_ERROR, SECTION_MOVE, _("GenericMove : no empty slot found\n"));
	      return(-1);
	      /*NOTREACHED*/
      }
      DebugPrint(DEBUG_INFO, SECTION_MOVE, _("GenericMove : Unload to %d\n"), to);
      if ((pto = LookupElement(to)) == NULL)
        {
          DebugPrint(DEBUG_ERROR, SECTION_MOVE, _(" Ups should not happen\n"));
	  DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP GenericMove\n"));
	  return(-1);
	  /*NOTREACHED*/
        }
    }

  if (CheckMove(pfrom, pto))
    {
      ret = SCSI_Move(DeviceFD, 0, from, to);
    }

  DebugPrint(DEBUG_INFO, SECTION_MOVE, _("GenericMove : SCSI_Move return (%d)\n"), ret);
  DebugPrint(DEBUG_INFO, SECTION_MOVE,_("##### STOP GenericMove\n"));
  return(ret);
}

/*
 * Check if a move based on the information we got from the Mode Sense command
 * is legal
 * Return Values:
 * 1 => OK
 * 0 => Not OK
 */

int
CheckMove(
    ElementInfo_T *	from,
    ElementInfo_T *	to)
{
	int moveok = 0;

	DebugPrint(DEBUG_INFO, SECTION_MOVE, _("##### START CheckMove\n"));
	if (pDeviceCapabilitiesPage != NULL )
	  {
	    DebugPrint(DEBUG_INFO, SECTION_MOVE, _("CheckMove : checking if move from %d to %d is legal\n"), from->address, to->address);
	    switch (from->type)
	      {
	      case CHANGER:
		DebugPrint(DEBUG_INFO, SECTION_MOVE, _("CheckMove : MT2"));
		switch (to->type)
		  {
		  case CHANGER:
		    if (pDeviceCapabilitiesPage->MT2MT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("MT\n"));
			moveok = 1;
		      }
		    break;
		  case STORAGE:
		    if (pDeviceCapabilitiesPage->MT2ST == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("ST\n"));
			moveok = 1;
		      }
		    break;
		  case IMPORT:
		    if (pDeviceCapabilitiesPage->MT2IE == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("IE\n"));
			moveok = 1;
		      }
		    break;
		  case TAPETYPE:
		    if (pDeviceCapabilitiesPage->MT2DT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("DT\n"));
			moveok = 1;
		      }
		    break;
		  default:
		    break;
		  }
		break;
	      case STORAGE:
		DebugPrint(DEBUG_INFO, SECTION_MOVE, _("CheckMove : ST2"));
		switch (to->type)
		  {
		  case CHANGER:
		    if (pDeviceCapabilitiesPage->ST2MT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("MT\n"));
			moveok = 1;
		      }
		    break;
		  case STORAGE:
		    if (pDeviceCapabilitiesPage->ST2ST == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("ST\n"));
			moveok = 1;
		      }
		    break;
		  case IMPORT:
		    if (pDeviceCapabilitiesPage->ST2IE == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("IE\n"));
			moveok = 1;
		      }
		    break;
		  case TAPETYPE:
		    if (pDeviceCapabilitiesPage->ST2DT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("DT\n"));
			moveok = 1;
		      }
		    break;
		  default:
		    break;
		  }
		break;
	      case IMPORT:
		DebugPrint(DEBUG_INFO, SECTION_MOVE, _("CheckMove : IE2"));
		switch (to->type)
		  {
		  case CHANGER:
		    if (pDeviceCapabilitiesPage->IE2MT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("MT\n"));
			moveok = 1;
		      }
		    break;
		  case STORAGE:
		    if (pDeviceCapabilitiesPage->IE2ST == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("ST\n"));
			moveok = 1;
		      }
		    break;
		  case IMPORT:
		    if (pDeviceCapabilitiesPage->IE2IE == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("IE\n"));
			moveok = 1;
		      }
		    break;
		  case TAPETYPE:
		    if (pDeviceCapabilitiesPage->IE2DT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("DT\n"));
			moveok = 1;
		      }
		    break;
		  default:
		    break;
		  }
		break;
	      case TAPETYPE:
		DebugPrint(DEBUG_INFO, SECTION_MOVE, _("CheckMove : DT2"));
		switch (to->type)
		  {
		  case CHANGER:
		    if (pDeviceCapabilitiesPage->DT2MT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("MT\n"));
			moveok = 1;
		      }
		    break;
		  case STORAGE:
		    if (pDeviceCapabilitiesPage->DT2ST == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("ST\n"));
			moveok = 1;
		      }
		    break;
		  case IMPORT:
		    if (pDeviceCapabilitiesPage->DT2IE == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("IE\n"));
			moveok = 1;
		      }
		    break;
		  case TAPETYPE:
		    if (pDeviceCapabilitiesPage->DT2DT == 1)
		      {
			DebugPrint(DEBUG_INFO, SECTION_MOVE, _("DT\n"));
			moveok = 1;
		      }
		    break;
		  default:
		    break;
		  }
		break;
	      default:
		break;
	      }
	  } else {
	    DebugPrint(DEBUG_INFO, SECTION_MOVE, _("CheckMove : pDeviceCapabilitiesPage == NULL"));
	    /*
	      ChgExit("CheckMove", _("DeviceCapabilitiesPage == NULL"), FATAL);
	    */
	    moveok=1;
	  }

	DebugPrint(DEBUG_INFO, SECTION_MOVE, _("###### STOP CheckMove\n"));
	return(moveok);
}

/*
 */

int
GetCurrentSlot(
    int		fd,
    int		drive)
{
  extern OpenFiles_T *pDev;
  size_t x;
  dbprintf(_("##### START GetCurrentSlot\n"));

  (void)fd;	/* Quiet unused parameter warning */

  if (pDev[0].SCSI == 0)
      {
          dbprintf(_("GetCurrentSlot : can't send SCSI commands\n"));
          return(-1);
	  /*NOTREACHED*/
      }

  if (ElementStatusValid == 0)
    {
      if (pDev[0].functions->function_status(0, 1) != 0)
        {
          return(-1);
	  /*NOTREACHED*/
        }
    }

  /* If the from address is the as the same as the tape address skip it */
  if (pDTE[drive].from >= 0 && pDTE[drive].from != pDTE[drive].address)
    {
      for (x = 0; x < STE;x++)
        {
          if (pSTE[x].address == pDTE[drive].from)
            return(x);
	    /*NOTREACHED*/
        }
      return(-1);
      /*NOTREACHED*/
    }

  for (x = 0; x < STE;x++)
    {
      if (pSTE[x].status == 'E') {
          return(x);
	  /*NOTREACHED*/
        }
    }

  /* Ups nothing loaded */
  return(-1);
}



/*
 * Reworked function to get the ElementStatus
 * This function will first call the GetElementStatus
 * function to get the Element status,
 * and than check if there are abnormal conditions.
 *
 * If there are error conditions try to fix them
 *
 */
int
GenericElementStatus(
    int		DeviceFD,
    int		InitStatus)
{
  int MTEError = 0;
  int STEError = 0;
  int IEEError = 0;
  int DTEError = 0;

  extern OpenFiles_T *pDev;

  int error = 0;    /* If set do an INIT ELEMENT STATUS */
  size_t x;         /* The standard loop counter :-) */
  int retry = 2;    /* Redo it if an error has been reset */

  (void)InitStatus;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("##### START GenericElementStatus\n"));

  if (pEAAPage == NULL)
    {
      /*
       * If this pointer is null
       * then try to read the parameter with MODE SENSE
       *
       */
      if (pModePage == NULL && LibModeSenseValid == 0)
        {
          pModePage = alloc(0xff);

	  if (SCSI_ModeSense(DeviceFD, pModePage, 0xff, 0x8, 0x3f) == 0)
	    {
	      LibModeSenseValid = 1;
	      DecodeModeSense(pModePage, 0, _("GenericElementStatus :"), 0, debug_file);
	    } else {
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("GetElementStatus : failed SCSI_ModeSense\n"));
	      LibModeSenseValid = -1;
	    }
        }
    }

  while ((GetElementStatus(DeviceFD) == 0) && (retry-- > 0))
    {
      for (x = 0; x < MTE; x++)
	{
	  if (pMTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pMTE[x].ASC, pMTE[x].ASCQ, (RequestSense_T *)&pMTE[x]))
		{
		case SENSE_IES:
		  MTEError = 1;
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericElementStatus : Abort on MTE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      for (x = 0; x < IEE; x++)
	{
	  if (pIEE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pIEE[x].ASC, pIEE[x].ASCQ, (RequestSense_T *)&pIEE[x]))
		{
		case SENSE_IES:
		  IEEError = 1;
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericElementStatus : Abort on IEE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}


      for (x = 0; x < STE; x++)
	{
	  if (pSTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pSTE[x].ASC, pSTE[x].ASCQ, (RequestSense_T *)&pSTE[x]))
		{
		case SENSE_IES:
		  STEError = 1;
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericElementStatus : Abort on IES\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      for (x = 0; x < DTE; x++)
	{
	  if (pDTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pDTE[x].ASC, pDTE[x].ASCQ, (RequestSense_T *)&pDTE[x]))
		{
		case SENSE_IES:
		  DTEError = 1;
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericElementStatus : Abort on DTE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      /*
       * OK, we have an error, do an INIT ELMENT
       * For the tape if not handled by the robot we have
       * to do some extra checks
       */
      if (error == 1)
	{
	  if (GenericResetStatus(DeviceFD) != 0)
	    {
	      ElementStatusValid = 0;
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericElementStatus : Can't init status STEError(%d) MTEError(%d) DTEError(%d) IEEError(%d)\n"), STEError, MTEError, DTEError, IEEError);
	      return(-1);
	      /*NOTREACHED*/
	    }
	  error = 0;
	}

      if (DTEError == 1)
	{
	  TapeStatus();
	  /*
	   * If the status is empty to an move from tape to tape
	   * This is if the tape is ejected, but not unloaded
	   */
	  if (pDTE[0].status == 'E')
	    {
	      DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("GenericElementStatus : try to move tape to tape drive\n"));
	      pDev[DeviceFD].functions->function_move(DeviceFD, pDTE[0].address, pDTE[0].address);
	    }
	}
	  /* Done GetElementStatus */
    }

  if (error != 0)
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("GenericElementStatus : Can't init status (after loop)\n"));
      return(-1);
      /*NOTREACHED*/
    }

  ElementStatusValid = 1;
  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("#### STOP GenericElementStatus\n"));
  return(0);
}


/*
 * This is for the ADIC changer, it seems that they have an diferent
 * offset in the mode sense data before the first mode page (+12)
 */
int
DLT448ElementStatus(
    int		DeviceFD,
    int		InitStatus)
{
  int DTEError = 0;

  extern OpenFiles_T *pDev;

  int error = 0;   /* If set do an INIT ELEMENT STATUS */
  size_t x;        /* The standard loop counter :-) */
  int loop = 2;    /* Redo it if an error has been reset */

  (void)InitStatus;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("##### START DLT448ElementStatus\n"));

  if (pEAAPage == NULL)
    {
      /*
       * If this pointer is null
       * then try to read the parameter with MODE SENSE
       *
       */
      if (pModePage == NULL && LibModeSenseValid == 0)
        {
          pModePage = alloc(0xff);

	  if (SCSI_ModeSense(DeviceFD, pModePage, 0xff, 0x8, 0x3f) == 0)
	    {
	      LibModeSenseValid = 1;
	      DecodeModeSense(pModePage, 12, _("DLT448ElementStatus :"), 0, debug_file);
	    } else {
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("DLT448ElementStatus : failed SCSI_ModeSense\n"));
	      LibModeSenseValid = -1;
	    }
        }
    }

  while (GetElementStatus(DeviceFD) == 0 && loop-- > 0)
    {
      for (x = 0; x < MTE; x++)
	{
	  if (pMTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pMTE[x].ASC, pMTE[x].ASCQ, (RequestSense_T *)&pMTE[x]))
		{
		case SENSE_IES:
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("DLT448ElementStatus : Abort on MTE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      for (x = 0; x < IEE; x++)
	{
	  if (pIEE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pIEE[x].ASC, pIEE[x].ASCQ, (RequestSense_T *)&pIEE[x]))
		{
		case SENSE_IES:
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("DLT448ElementStatus : Abort on IEE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}


      for (x = 0; x < STE; x++)
	{
	  /*
	   * Needed for the hack to guess the tape status if an error
	   * for the tape is pending
	   */
	  if (pSTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pSTE[x].ASC, pSTE[x].ASCQ, (RequestSense_T *)&pSTE[x]))
		{
		case SENSE_IES:
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("DLT448ElementStatus : Abort on IES\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      for (x = 0; x < DTE; x++)
	{
	  if (pDTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pDTE[x].ASC, pDTE[x].ASCQ, (RequestSense_T *)&pDTE[x]))
		{
		case SENSE_IES:
		  DTEError = 1;
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("DLT448ElementStatus : Abort on DTE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      /*
       * OK, we have an error, do an INIT ELMENT
       * For the tape if not handled by the robot we have
       * to do some extra checks
       */
      if (error == 1)
	{
	  if (GenericResetStatus(DeviceFD) != 0)
	    {
	      ElementStatusValid = 0;
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("DLT448ElementStatus : Can't init status\n"));
	      return(-1);
	      /*NOTREACHED*/
	    }
	  error = 0;
	}

      if (DTEError == 1)
	{
	  TapeStatus();
	  /*
	   * If the status is empty to an move from tape to tape
	   * This is if the tape is ejected, but not unloaded
	   */
	  if (pDTE[0].status == 'E')
	    {
	      DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("DLT448ElementStatus : try to move tape to tape drive\n"));
	      pDev[DeviceFD].functions->function_move(DeviceFD, pDTE[0].address, pDTE[0].address);
	    }
	}
	  /* Done GetElementStatus */
    }

  if (error != 0)
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("DLT448ElementStatus : Can't init status (after loop)\n"));
      return(-1);
      /*NOTREACHED*/
    }

  ElementStatusValid = 1;
  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("#### STOP DLT448ElementStatus\n"));
  return(0);
}


/*
 * Much the same like GenericElementStatus but
 * it seemes that for the STE Elements ASC/ASCQ is not set
 * on an error, only the except bit is set
*/
int
SDXElementStatus(
    int		DeviceFD,
    int		InitStatus)
{
  int error = 0;   /* If set do an INIT ELEMENT STATUS */
  size_t x;        /* The standard loop counter :-) */
  int loop = 2;    /* Redo it if an error has been reset */

  (void)InitStatus;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT, _("##### START SDXElementStatus\n"));

  if (pEAAPage == NULL)
    {
      /*
       * If this pointer is null
       * then try to read the parameter with MODE SENSE
       *
       */
      if (pModePage == NULL && LibModeSenseValid == 0)
        {
          pModePage = alloc(0xff);

	  if (SCSI_ModeSense(DeviceFD, pModePage, 0xff, 0x8, 0x3f) == 0)
	    {
	      LibModeSenseValid = 1;
	      DecodeModeSense(pModePage, 0, _("SDXElementStatus :"), 0, debug_file);
	    } else {
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("SDXElementStatus : failed SCSI_ModeSense\n"));
	      LibModeSenseValid = -1;
	    }
        }
    }

  while (GetElementStatus(DeviceFD) == 0 && loop--)
    {
      error = 0;
      for (x = 0; x < MTE; x++)
	{
	  if (pMTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pMTE[x].ASC, pMTE[x].ASCQ, (RequestSense_T *)&pMTE[x]))
		{
		case SENSE_IES:
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("SDXElementStatus : Abort on MTE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      for (x = 0; x < IEE; x++)
	{
	  if (pIEE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pIEE[x].ASC, pIEE[x].ASCQ, (RequestSense_T *)&pIEE[x]))
		{
		case SENSE_IES:
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("SDXElementStatus : Abort on IEE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}


      for (x = 0; x < STE; x++)
	{
	  if (pSTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pSTE[x].ASC, pSTE[x].ASCQ, (RequestSense_T *)&pSTE[x]))
		{
		case SENSE_IES:
		  error = 1;
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("SDXElementStatus : Abort on IES\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      for (x = 0; x < DTE; x++)
	{
	  if (pDTE[x].ASC > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, SENSE_CHG_ELEMENT_STATUS, pDTE[x].ASC, pDTE[x].ASCQ, (RequestSense_T *)&pDTE[x]))
		{
		case SENSE_IES:
		  /*
		  error = 1;
		  */
		  break;
		case SENSE_ABORT:
		  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("SDXElementStatus : Abort on DTE\n"));
		  return(-1);
		  /*NOTREACHED*/
		}
	    }
	}

      /*
       * OK, we have an error, do an INIT ELMENT
       * For the tape if not handled by the robot we have
       * to do some extra checks
       */
      if (error == 1)
	{
	  if (GenericResetStatus(DeviceFD) != 0)
	    {
	      ElementStatusValid = 0;
	      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("SDXElementStatus : Can't init status\n"));
	      return(-1);
	      /*NOTREACHED*/
	    }
	}

      /* Done GetElementStatus */
    }

  if (error != 0)
    {
      DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("SDXElementStatus : Can't init status\n"));
      return(-1);
      /*NOTREACHED*/
    }

  ElementStatusValid = 1;
  TapeStatus();
  DebugPrint(DEBUG_ERROR, SECTION_ELEMENT, _("#### STOP SDXElementStatus\n"));
  return(0);
}


/*
 * Reads the element information from the library. There are 2 ways to do this.
 * Either check the result from the mode sense page to see which types of elements
 * are available (STE/DTE/MTE....), or do an read element status with the option give
 * me all and than check what is available.
 *
 * Only do the read, error handling is done by the calling function
 *
 * Return Values:
 * < 0   -> Error
 * == 0  -> OK
 *
 * TODO:
 */
int
GetElementStatus(
    int DeviceFD)
{
  u_char *DataBuffer = NULL;
  size_t DataBufferLength;
  ElementStatusData_T *ElementStatusData;
  ElementStatusPage_T *ElementStatusPage;
  MediumTransportElementDescriptor_T *MediumTransportElementDescriptor;
  StorageElementDescriptor_T *StorageElementDescriptor;
  DataTransferElementDescriptor_T *DataTransferElementDescriptor;
  ImportExportElementDescriptor_T *ImportExportElementDescriptor;
  size_t x;
  size_t offset;
  size_t length;	/* Length of an Element */
  size_t NoOfElements;

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("##### START GetElementStatus\n"));

  barcode = BarCode(DeviceFD);

  /*
   * If the MODE_SENSE was successfull we use this Information to read the Elelement Info
   */
  if (pEAAPage != NULL)
    {
      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Reading Element Status with the info from mode sense\n"));
      /* First the Medim Transport*/
      if (V2(pEAAPage->NoMediumTransportElements)  > 0)
        {
          MTE = V2(pEAAPage->NoMediumTransportElements) ;
          pMTE = alloc(SIZEOF(ElementInfo_T) * MTE);
          memset(pMTE, 0, SIZEOF(ElementInfo_T) * MTE);

          if (SCSI_ReadElementStatus(DeviceFD,
                                     CHANGER,
                                     0,
                                     (u_char)barcode,
                                     V2(pEAAPage->MediumTransportElementAddress),
                                     (MTE + (size_t)1),
				     SIZEOF(MediumTransportElementDescriptor_T),
                                     &DataBuffer) != 0)
            {
              ChgExit("genericElementStatus",_("Can't read MTE status"), FATAL);
	      /*NOTREACHED*/
            }
          // ElementStatusData = (ElementStatusData_T *)DataBuffer;
          offset = SIZEOF(ElementStatusData_T);

          ElementStatusPage = (ElementStatusPage_T *)&DataBuffer[offset];
          offset = offset + SIZEOF(ElementStatusPage_T);
	  length = V2(ElementStatusPage->length);

          DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("MTE Length %d(%d)\n"), length,
			SIZEOF(MediumTransportElementDescriptor_T));

          for (x = 0; x < MTE; x++)
            {
              MediumTransportElementDescriptor = (MediumTransportElementDescriptor_T *)&DataBuffer[offset];

              if (ElementStatusPage->pvoltag == 1)
                {
                  strncpy((char *)pMTE[x].VolTag,
                          (char *)MediumTransportElementDescriptor->pvoltag,
                          TAG_SIZE);
                  TerminateString(pMTE[x].VolTag, TAG_SIZE+1);
                }

              pMTE[x].type = ElementStatusPage->type;
              pMTE[x].address = V2(MediumTransportElementDescriptor->address);
              pMTE[x].except = MediumTransportElementDescriptor->except;
              pMTE[x].full = MediumTransportElementDescriptor->full;
	      if (MediumTransportElementDescriptor->full > 0)
		{
                  pMTE[x].status = 'F';
		} else {
                  pMTE[x].status = 'E';
		}

	      if (length >= 5)
		{
		  pMTE[x].ASC = MediumTransportElementDescriptor->asc;
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC MTE\n"));
		}

	      if (length >= 6)
		{
		  pMTE[x].ASCQ = MediumTransportElementDescriptor->ascq;
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ MTE\n"));
		}

	      if (length >= 0xa)
		{
		  if (MediumTransportElementDescriptor->svalid == 1)
		    {
		      pMTE[x].from = V2(MediumTransportElementDescriptor->source);
		    } else {
		      pMTE[x].from = -1;
		    }
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source MTE\n"));
		}
	      offset = offset + length;
            }
	    free(DataBuffer);
	    DataBuffer = NULL;
        }
      /*
       * Storage Elements
       */
      if ( V2(pEAAPage->NoStorageElements)  > 0)
        {
          free(pSTE);
          STE = V2(pEAAPage->NoStorageElements);
          pSTE = alloc(SIZEOF(ElementInfo_T) * STE);
          memset(pSTE, 0, SIZEOF(ElementInfo_T) * STE);

          if (SCSI_ReadElementStatus(DeviceFD,
                                     STORAGE,
                                     0,
                                     (u_char)barcode,
                                     V2(pEAAPage->FirstStorageElementAddress),
                                     STE,
				     SIZEOF(StorageElementDescriptor_T),
                                     &DataBuffer) != 0)
            {
              ChgExit("GetElementStatus", _("Can't read STE status"), FATAL);
	      /*NOTREACHED*/
            }
	  assert(DataBuffer != NULL);

          // ElementStatusData = (ElementStatusData_T *)DataBuffer;
          offset = SIZEOF(ElementStatusData_T);

          ElementStatusPage = (ElementStatusPage_T *)&DataBuffer[offset];
          offset = offset + SIZEOF(ElementStatusPage_T);
	  length = V2(ElementStatusPage->length);
          DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("STE Length %d\n"),length);

          for (x = 0; x < STE; x++)
            {
              StorageElementDescriptor = (StorageElementDescriptor_T *)&DataBuffer[offset];
              if (ElementStatusPage->pvoltag == 1)
                {
                  strncpy(pSTE[x].VolTag,
                          (char *)StorageElementDescriptor->pvoltag,
                          TAG_SIZE);
                  TerminateString(pSTE[x].VolTag, TAG_SIZE+1);
                }


              pSTE[x].type = ElementStatusPage->type;
              pSTE[x].address = V2(StorageElementDescriptor->address);
              pSTE[x].except = StorageElementDescriptor->except;
              pSTE[x].full = StorageElementDescriptor->full;
              if (StorageElementDescriptor->full > 0)
		{
		  pSTE[x].status = 'F';
		} else {
		  pSTE[x].status = 'E';
		}

	      if (length >= 5)
		{
		  pSTE[x].ASC = StorageElementDescriptor->asc;
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC STE\n"));
		}

	      if (length >= 6)
		{
		  pSTE[x].ASCQ = StorageElementDescriptor->ascq;
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ STE\n"));
		}

	      if (length >= 0xa)
		{
		  if (StorageElementDescriptor->svalid == 1)
		    {
		      pSTE[x].from = V2(StorageElementDescriptor->source);
		    } else {
		      pSTE[x].from = -1;
		    }
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source STE\n"));
		}

              offset = offset + length;
            }
	    free(DataBuffer);
	    DataBuffer = NULL;
        }
      /*
       * Import/Export Elements
       */
      if ( V2(pEAAPage->NoImportExportElements) > 0)
        {
          free(pIEE);
          IEE = V2(pEAAPage->NoImportExportElements);
          pIEE = alloc(SIZEOF(ElementInfo_T) * IEE);
          memset(pIEE, 0, SIZEOF(ElementInfo_T) * IEE);

          if (SCSI_ReadElementStatus(DeviceFD,
                                     IMPORT,
                                     0,
                                     (u_char)barcode,
                                     V2(pEAAPage->FirstImportExportElementAddress),
                                     IEE,
				     SIZEOF(ImportExportElementDescriptor_T),
                                     &DataBuffer) != 0)
            {
              ChgExit("GetElementStatus", _("Can't read IEE status"), FATAL);
	      /*NOTREACHED*/
            }
	  assert(DataBuffer != NULL);

          // ElementStatusData = (ElementStatusData_T *)DataBuffer;
          offset = SIZEOF(ElementStatusData_T);

          ElementStatusPage = (ElementStatusPage_T *)&DataBuffer[offset];
          offset = offset + SIZEOF(ElementStatusPage_T);
	  length = V2(ElementStatusPage->length);
          DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("IEE Length %d\n"),length);

          for (x = 0; x < IEE; x++)
            {
              ImportExportElementDescriptor = (ImportExportElementDescriptor_T *)&DataBuffer[offset];
              if (ElementStatusPage->pvoltag == 1)
                {
                  strncpy(pIEE[x].VolTag,
                          (char *)ImportExportElementDescriptor->pvoltag,
                          TAG_SIZE);
                  TerminateString(pIEE[x].VolTag, TAG_SIZE+1);
                }
              pIEE[x].type = ElementStatusPage->type;
              pIEE[x].address = V2(ImportExportElementDescriptor->address);
              pIEE[x].except = ImportExportElementDescriptor->except;
              pIEE[x].full = ImportExportElementDescriptor->full;
	      if (ImportExportElementDescriptor->full > 0)
		{
		  pIEE[x].status = 'F';
		} else {
		  pIEE[x].status = 'E';
		}

	      if (length >= 5)
		{
		  pIEE[x].ASC = ImportExportElementDescriptor->asc;
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC IEE\n"));
		}

	      if (length >= 6)
		{
		  pIEE[x].ASCQ = ImportExportElementDescriptor->ascq;
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ IEE\n"));
		}

	      if (length >= 0xa)
		{
		  if (ImportExportElementDescriptor->svalid == 1)
		    {
		      pIEE[x].from = V2(ImportExportElementDescriptor->source);
		    } else {
		      pIEE[x].from = -1;
		    }
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source IEE\n"));
		}

              offset = offset + length;
            }
	    free(DataBuffer);
	    DataBuffer = NULL;
        }
      /*
       * Data Transfer Elements
       */
      if (V2(pEAAPage->NoDataTransferElements) >0)
        {
	  free(pDTE);
          DTE = V2(pEAAPage->NoDataTransferElements) ;
          pDTE = alloc(SIZEOF(ElementInfo_T) * DTE);
          memset(pDTE, 0, SIZEOF(ElementInfo_T) * DTE);

          if (SCSI_ReadElementStatus(DeviceFD,
                                     TAPETYPE,
                                     0,
                                     (u_char)barcode,
                                     V2(pEAAPage->FirstDataTransferElementAddress),
                                     DTE,
				     SIZEOF(DataTransferElementDescriptor_T),
                                     &DataBuffer) != 0)
            {
              ChgExit("GenericElementStatus", _("Can't read DTE status"), FATAL);
	      /*NOTREACHED*/
            }
	  assert(DataBuffer != NULL);

          // ElementStatusData = (ElementStatusData_T *)DataBuffer;
          offset = SIZEOF(ElementStatusData_T);

          ElementStatusPage = (ElementStatusPage_T *)&DataBuffer[offset];
          offset = offset + SIZEOF(ElementStatusPage_T);
	  length = V2(ElementStatusPage->length);
          DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("DTE Length %d\n"),length);

          for (x = 0; x < DTE; x++)
            {
              DataTransferElementDescriptor = (DataTransferElementDescriptor_T *)&DataBuffer[offset];
              if (ElementStatusPage->pvoltag == 1)
                {
                  strncpy(pDTE[x].VolTag,
                          (char *)DataTransferElementDescriptor->pvoltag,
                          TAG_SIZE);
                  TerminateString(pDTE[x].VolTag, TAG_SIZE+1);
                }
              pDTE[x].type = ElementStatusPage->type;
	      pDTE[x].address = V2(DataTransferElementDescriptor->address);
              pDTE[x].except = DataTransferElementDescriptor->except;
              pDTE[x].scsi = DataTransferElementDescriptor->scsi;
              pDTE[x].full = DataTransferElementDescriptor->full;
              if (DataTransferElementDescriptor->full > 0)
		{
		  pDTE[x].status = 'F';
		} else {
		  pDTE[x].status = 'E';
		}

	      if (length >= 5)
	      {
              	pDTE[x].ASC = DataTransferElementDescriptor->asc;
	      } else {
		DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC DTE\n"));
	      }

	      if (length >= 6)
		{
		  pDTE[x].ASCQ = DataTransferElementDescriptor->ascq;
	      } else {
		DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ DTE\n"));
	      }

	      if (length >= 0xa)
		{
		  if (DataTransferElementDescriptor->svalid == 1)
		    {
		      pDTE[x].from = V2(DataTransferElementDescriptor->source);
		    } else {
		      pDTE[x].from = -1;
		    }
		} else {
		  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source STE\n"));
		}

              offset = offset + length;
            }
	    free(DataBuffer);
	    DataBuffer = NULL;
        }
    } else {
      /*
       * And now the old way, when we get here the read mode sense page has failed ...
       */
      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Reading Element Status the old way .... (max 255 elements)\n"));
      if (SCSI_ReadElementStatus(DeviceFD,
                                 0,
                                 0,
                                 (u_char)barcode,
                                 0,
                                 (size_t)0xff,
				 (size_t)0x7f,
                                 &DataBuffer) != 0)
        {
          ChgExit("GenericElementStatus",_("Can't get ElementStatus"), FATAL);
	  /*NOTREACHED*/
        }
      assert(DataBuffer != NULL);

      ElementStatusData = (ElementStatusData_T *)DataBuffer;
      DataBufferLength = V3(ElementStatusData->count);

      offset = SIZEOF(ElementStatusData_T);

      while (offset < DataBufferLength)
        {
          ElementStatusPage = (ElementStatusPage_T *)&DataBuffer[offset];
          NoOfElements = V3(ElementStatusPage->count) / V2(ElementStatusPage->length);
          offset = offset + SIZEOF(ElementStatusPage_T);
	  length = V2(ElementStatusPage->length);

          switch (ElementStatusPage->type)
            {
            case CHANGER:
	      free(pMTE);
              MTE = NoOfElements;
              pMTE = alloc(SIZEOF(ElementInfo_T) * MTE);
              memset(pMTE, 0, SIZEOF(ElementInfo_T) * MTE);

              for (x = 0; x < NoOfElements; x++)
                {
                  MediumTransportElementDescriptor = (MediumTransportElementDescriptor_T *)&DataBuffer[offset];
                  if (ElementStatusPage->pvoltag == 1)
                    {
                      strncpy(pMTE[x].VolTag,
                              (char *)MediumTransportElementDescriptor->pvoltag,
                              TAG_SIZE);
                      TerminateString(pMTE[x].VolTag, TAG_SIZE+1);
                    }
                  pMTE[x].type = ElementStatusPage->type;
                  pMTE[x].address = V2(MediumTransportElementDescriptor->address);
                  pMTE[x].except = MediumTransportElementDescriptor->except;
                  pMTE[x].full = MediumTransportElementDescriptor->full;
		  if (MediumTransportElementDescriptor->full > 0)
		    {
		      pMTE[x].status = 'F';
		    } else {
		      pMTE[x].status = 'E';
		    }

		  if (length >= 5)
		    {
		      pMTE[x].ASC = MediumTransportElementDescriptor->asc;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC MTE\n"));
		    }

	          if (length >= 6)
		    {
		      pMTE[x].ASCQ = MediumTransportElementDescriptor->ascq;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ MTE\n"));
		    }

	          if (length >= 0xa)
		    {
		      if (MediumTransportElementDescriptor->svalid == 1)
			{
			  pMTE[x].from = V2(MediumTransportElementDescriptor->source);
			} else {
			  pMTE[x].from = -1;
			}
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source MTE\n"));
		    }

		  offset = offset + length;
		}
              break;
            case STORAGE:
	      free(pSTE);
              STE = NoOfElements;
              pSTE = alloc(SIZEOF(ElementInfo_T) * STE);
              memset(pSTE, 0, SIZEOF(ElementInfo_T) * STE);

              for (x = 0; x < NoOfElements; x++)
                {
		  StorageElementDescriptor = (StorageElementDescriptor_T *)&DataBuffer[offset];
                  if (ElementStatusPage->pvoltag == 1)
                    {
                      strncpy(pSTE[x].VolTag,
                              (char *)StorageElementDescriptor->pvoltag,
                              TAG_SIZE);
                      TerminateString(pSTE[x].VolTag, TAG_SIZE+1);
                    }

                  pSTE[x].type = ElementStatusPage->type;
                  pSTE[x].address = V2(StorageElementDescriptor->address);
                  pSTE[x].except = StorageElementDescriptor->except;
                  pSTE[x].full = StorageElementDescriptor->full;
		  if (StorageElementDescriptor->full > 0)
		    {
		      pSTE[x].status = 'F';
		    } else {
		      pSTE[x].status = 'E';
		    }

		  if (length >= 5)
		    {
		      pSTE[x].ASC = StorageElementDescriptor->asc;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC STE\n"));
		    }

		  if (length >= 6)
		    {
		      pSTE[x].ASCQ = StorageElementDescriptor->ascq;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ STE\n"));
		    }

		  if (length >= 0xa)
		    {
		      if (StorageElementDescriptor->svalid == 1)
			{
			  pSTE[x].from = V2(StorageElementDescriptor->source);
			} else {
			  pSTE[x].from = -1;
			}
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source STE\n"));
		    }

                  offset = offset + length;
                }
              break;
            case IMPORT:
	      free(pIEE);
              IEE = NoOfElements;
              pIEE = alloc(SIZEOF(ElementInfo_T) * IEE);
              memset(pIEE, 0, SIZEOF(ElementInfo_T) * IEE);

              for (x = 0; x < NoOfElements; x++)
                {
                  ImportExportElementDescriptor = (ImportExportElementDescriptor_T *)&DataBuffer[offset];
                  if (ElementStatusPage->pvoltag == 1)
                    {
                      strncpy(pIEE[x].VolTag,
                              (char *)ImportExportElementDescriptor->pvoltag,
                              TAG_SIZE);
                      TerminateString(pIEE[x].VolTag, TAG_SIZE+1);
                    }
                  ImportExportElementDescriptor = (ImportExportElementDescriptor_T *)&DataBuffer[offset];
                  pIEE[x].type = ElementStatusPage->type;
                  pIEE[x].address = V2(ImportExportElementDescriptor->address);
                  pIEE[x].except = ImportExportElementDescriptor->except;
                  pIEE[x].full = ImportExportElementDescriptor->full;
		  if (ImportExportElementDescriptor->full > 0)
		    {
		      pIEE[x].status = 'F';
		    } else {
		      pIEE[x].status = 'E';
		    }

		  if (length >= 5)
		    {
		      pIEE[x].ASC = ImportExportElementDescriptor->asc;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC IEE\n"));
		    }

		  if (length >= 6)
		    {
		      pIEE[x].ASCQ = ImportExportElementDescriptor->ascq;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ IEE\n"));
		    }

		  if (length >= 0xa)
		    {
		      if (ImportExportElementDescriptor->svalid == 1)
			{
			  pIEE[x].from = V2(ImportExportElementDescriptor->source);
			} else {
			  pIEE[x].from = -1;
			}
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source IEE\n"));
		    }

		  offset = offset + length;
		}
	      break;
            case TAPETYPE:
	      free(pDTE);
              DTE = NoOfElements;
              pDTE = alloc(SIZEOF(ElementInfo_T) * DTE);
              memset(pDTE, 0, SIZEOF(ElementInfo_T) * DTE);

              for (x = 0; x < NoOfElements; x++)
                {
		  DataTransferElementDescriptor = (DataTransferElementDescriptor_T *)&DataBuffer[offset];
                  if (ElementStatusPage->pvoltag == 1)
                    {
                      strncpy(pSTE[x].VolTag,
                              (char *)DataTransferElementDescriptor->pvoltag,
                              TAG_SIZE);
                      TerminateString(pSTE[x].VolTag, TAG_SIZE+1);
                    }
                  pDTE[x].type = ElementStatusPage->type;
                  pDTE[x].address = V2(DataTransferElementDescriptor->address);
                  pDTE[x].except = DataTransferElementDescriptor->except;
                  pDTE[x].scsi = DataTransferElementDescriptor->scsi;
                  pDTE[x].full = DataTransferElementDescriptor->full;
		  if (DataTransferElementDescriptor->full > 0)
		    {
		      pDTE[x].status = 'F';
		    } else {
		      pDTE[x].status = 'E';
		    }

		  if (length >= 5)
		    {
		      pDTE[x].ASC = DataTransferElementDescriptor->asc;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASC DTE\n"));
		    }

		  if (length >= 6)
		    {
		      pDTE[x].ASCQ = DataTransferElementDescriptor->ascq;
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip ASCQ DTE\n"));
		    }

		  if (length >= 0xa)
		    {
		      if (DataTransferElementDescriptor->svalid == 1)
			{
			  pDTE[x].from = V2(DataTransferElementDescriptor->source);
			} else {
			  pDTE[x].from = -1;
			}
		    } else {
		      DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("Skip source STE\n"));
		    }

		  offset = offset + length;
                }
              break;
            default:
              offset = offset + length;
              DebugPrint(DEBUG_ERROR, SECTION_ELEMENT,_("GetElementStatus : UnGknown Type %d\n"),ElementStatusPage->type);
              break;
            }
        }
	free(DataBuffer);
    }

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\n\n\tMedia Transport Elements (robot arms) :\n"));

  for ( x = 0; x < MTE; x++)
    DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\t\tElement #%04d %c\n\t\t\tEXCEPT = %02x\n\t\t\tASC = %02X ASCQ = %02X\n\t\t\tType %d From = %04d\n\t\t\tTAG = %s\n"),
              pMTE[x].address, pMTE[x].status, pMTE[x].except, pMTE[x].ASC,
              pMTE[x].ASCQ, pMTE[x].type, pMTE[x].from, pMTE[x].VolTag);

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\n\n\tStorage Elements (Media slots) :\n"));

  for ( x = 0; x < STE; x++)
    DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\t\tElement #%04d %c\n\t\t\tEXCEPT = %02X\n\t\t\tASC = %02X ASCQ = %02X\n\t\t\tType %d From = %04d\n\t\t\tTAG = %s\n"),
              pSTE[x].address, pSTE[x].status, pSTE[x].except, pSTE[x].ASC,
              pSTE[x].ASCQ, pSTE[x].type, pSTE[x].from, pSTE[x].VolTag);

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\n\n\tData Transfer Elements (tape drives) :\n"));

  for ( x = 0; x < DTE; x++)
    DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\t\tElement #%04d %c\n\t\t\tEXCEPT = %02X\n\t\t\tASC = %02X ASCQ = %02X\n\t\t\tType %d From = %04d\n\t\t\tTAG = %s\n\t\t\tSCSI ADDRESS = %d\n"),
              pDTE[x].address, pDTE[x].status, pDTE[x].except, pDTE[x].ASC,
              pDTE[x].ASCQ, pDTE[x].type, pDTE[x].from, pDTE[x].VolTag,pDTE[x].scsi);

  DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\n\n\tImport/Export Elements  :\n"));

  for ( x = 0; x < IEE; x++)
    DebugPrint(DEBUG_INFO, SECTION_ELEMENT,_("\t\tElement #%04d %c\n\t\t\tEXCEPT = %02X\n\t\t\t\tASC = %02X ASCQ = %02X\n\t\t\tType %d From = %04d\n\t\t\tTAG = %s\n"),
	       pIEE[x].address, pIEE[x].status, pIEE[x].except, pIEE[x].ASC,
	       pIEE[x].ASCQ, pIEE[x].type, pIEE[x].from, pIEE[x].VolTag);



  return(0);
}

/*
 * Get sense data
 * If ClearErrorCounters is set the counters will be reset.
 * Used by GenericClean for example
 *
 * TODO
 */
int
RequestSense(
    int				DeviceFD,
    ExtendedRequestSense_T *	ExtendedRequestSense,
    int				ClearErrorCounters)
{
  CDB_T CDB;
  int ret;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START RequestSense\n"));

  CDB[0] = SC_COM_REQUEST_SENSE;               /* REQUEST SENSE */
  CDB[1] = 0;                                  /* Logical Unit Number = 0, Reserved */
  CDB[2] = 0;                                  /* Reserved */
  CDB[3] = 0;                                  /* Reserved */
  CDB[4] = (u_char)sizeof(ExtendedRequestSense_T);   /* Allocation Length */
  CDB[5] = (u_char)((ClearErrorCounters << 7) & 0x80); /*  */

  memset(ExtendedRequestSense, 0, SIZEOF(ExtendedRequestSense_T));

  ret = SCSI_Run(DeviceFD, Input, CDB, 6,
		 (char *) ExtendedRequestSense,
		 SIZEOF(ExtendedRequestSense_T),
		 (RequestSense_T *) ExtendedRequestSense,
		 SIZEOF(ExtendedRequestSense_T));


  if (ret < 0)
    {
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP RequestSense (%d)\n"),ret);
      return(ret);
      /*NOTREACHED*/
    }

  if ( ret > 0)
    {
      DecodeExtSense(ExtendedRequestSense, "RequestSense : ",debug_file);
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP RequestSense (%d)\n"), ExtendedRequestSense->SenseKey);
      return(ExtendedRequestSense->SenseKey);
      /*NOTREACHED*/
    }

  dump_hex((u_char *)ExtendedRequestSense ,
	   SIZEOF(ExtendedRequestSense_T),
	   DEBUG_INFO, SECTION_SCSI);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP RequestSense (0)\n"));
  return(0);
}


/*
 * Lookup function pointer for device ....
 */


ElementInfo_T *
LookupElement(
    int		address)
{
  size_t x;

  dbprintf(_("##### START LookupElement\n"));

  if (DTE > 0)
    {
      for (x = 0; x < DTE; x++)
        {
          if (pDTE[x].address == address)
	  {
            dbprintf(_("##### STOP LookupElement (DTE)\n"));
            return(&pDTE[x]);
	    /*NOTREACHED*/
	  }
        }
    }

  if (MTE > 0)
    {
      for (x = 0; x < MTE; x++)
        {
          if (pMTE[x].address == address)
	  {
            dbprintf(_("##### STOP LookupElement (MTE)\n"));
            return(&pMTE[x]);
	    /*NOTREACHED*/
	  }
        }
    }

  if (STE > 0)
    {
      for (x = 0; x < STE; x++)
        {
          if (pSTE[x].address == address)
	  {
            dbprintf(_("##### STOP LookupElement (STE)\n"));
            return(&pSTE[x]);
	    /*NOTREACHED*/
	  }
        }
    }

  if (IEE > 0)
    {
      for ( x = 0; x < IEE; x++)
        {
          if (pIEE[x].address == address)
	  {
            dbprintf(_("##### STOP LookupElement (IEE)\n"));
            return(&pIEE[x]);
	    /*NOTREACHED*/
	  }
        }
    }
  return(NULL);
}

/*
 * Here comes everything what decode the log Pages
 *
 * TODO:
 * Fix the result handling from TestUnitReady
 *
 */
int
LogSense(
    int		DeviceFD)
{
  extern OpenFiles_T *pDev;
  CDB_T CDB;
  RequestSense_T *pRequestSense;
  LogSenseHeader_T *LogSenseHeader;
  LogParameter_T *LogParameter;
  struct LogPageDecode *p;
  int found;
  extern char *tapestatfile;
  int i;
  unsigned ParameterCode;
  unsigned value;
  size_t length;
  int count;
  u_char *buffer;
  u_char *logpages;
  size_t nologpages;
  size_t size = 128;

  (void)DeviceFD;	/* Quiet unused parameter warning */

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### START LogSense\n"));

  if ((tapestatfile != NULL) && (pDev[INDEX_TAPECTL].SCSI == 1) &&
      ((StatFile = fopen(tapestatfile,"a")) != NULL))
    {
      pRequestSense = alloc(SIZEOF(RequestSense_T));

      if (GenericRewind(INDEX_TAPECTL) < 0)
        {
          DebugPrint(DEBUG_INFO, SECTION_TAPE,_("LogSense : Rewind failed\n"));
          free(pRequestSense);
	  fclose(StatFile);
          return(0);
	  /*NOTREACHED*/
        }
      /*
       * Try to read the tape label
       */
      if (pDev[INDEX_TAPE].inqdone == 1)
        {
	  if (pDev[INDEX_TAPE].devopen == 1)
	    {
	      SCSI_CloseDevice(INDEX_TAPE);
	    }

	  if ((chgscsi_result = (char *)tape_rdlabel(pDev[INDEX_TAPE].dev, &chgscsi_datestamp, &chgscsi_label)) == NULL)
	    {
	      g_fprintf(StatFile, _("==== %s ==== %s ====\n"), chgscsi_datestamp, chgscsi_label);
	    } else {
	      g_fprintf(StatFile, "%s\n", chgscsi_result);
	    }
	}

      buffer = alloc(size);
      memset(buffer, 0, size);
      /*
       * Get the known log pages
       */

      CDB[0] = SC_COM_LOG_SENSE;
      CDB[1] = 0;
      CDB[2] = 0x40;    /* 0x40 for current values */
      CDB[3] = 0;
      CDB[4] = 0;
      CDB[5] = 0;
      CDB[6] = 00;
      MSB2(&CDB[7], size);
      CDB[9] = 0;

      if (SCSI_Run(INDEX_TAPECTL, Input, CDB, 10,
                              buffer,
                              size,
                              pRequestSense,
                              SIZEOF(RequestSense_T)) != 0)
        {
          DecodeSense(pRequestSense, "LogSense : ",debug_file);
          free(pRequestSense);
          free(buffer);
	  fclose(StatFile);
          return(0);
	  /*NOTREACHED*/
        }

      LogSenseHeader = (LogSenseHeader_T *)buffer;
      nologpages = V2(LogSenseHeader->PageLength);
      logpages = alloc(nologpages);

      memcpy(logpages, buffer + SIZEOF(LogSenseHeader_T), nologpages);

      for (count = 0; count < (int)nologpages; count++) {
        if (logpages[count] != 0  ) {
          memset(buffer, 0, size);
          CDB[0] = SC_COM_LOG_SENSE;
          CDB[1] = 0;
          CDB[2] = (u_char)(0x40 | logpages[count]);/* 0x40 for current values */
          CDB[3] = 0;
          CDB[4] = 0;
          CDB[5] = 0;
          CDB[6] = 00;
          MSB2(&CDB[7], size);
          CDB[9] = 0;

          if (SCSI_Run(INDEX_TAPECTL, Input, CDB, 10,
                                  buffer,
                                  size,
                                  pRequestSense,
                                  SIZEOF(RequestSense_T)) != 0)
            {
              DecodeSense(pRequestSense, "LogSense : ",debug_file);
              free(pRequestSense);
	      free(logpages);
              free(buffer);
	      fclose(StatFile);
              return(0);
	      /*NOTREACHED*/
            }
          LogSenseHeader = (LogSenseHeader_T *)buffer;
          length = V2(LogSenseHeader->PageLength);
          LogParameter = (LogParameter_T *)(buffer + SIZEOF(LogSenseHeader_T));
          /*
           * Decode the log pages
           */
          p = (struct LogPageDecode *)&DecodePages;
          found = 0;

	  dump_hex((u_char *)LogParameter, 64, DEBUG_INFO, SECTION_SCSI);

          while(p->ident != NULL) {
            if ((strcmp(pDev[INDEX_TAPECTL].ident, p->ident) == 0 ||strcmp("*", p->ident) == 0)  && p->LogPage == logpages[count]) {
              p->decode(LogParameter, length);
              found = 1;
              g_fprintf(StatFile, "\n");
              break;
            }
            p++;
          }

          if (!found) {
            g_fprintf(StatFile, _("Logpage No %d = %x\n"), count ,logpages[count]);

            while ((u_char *)LogParameter < (buffer + length)) {
              i = LogParameter->ParameterLength;
              ParameterCode = V2(LogParameter->ParameterCode);
              switch (i) {
              case 1:
                value = V1((u_char *)LogParameter + SIZEOF(LogParameter_T));
                g_fprintf(StatFile, _("ParameterCode %02X = %u(%d)\n"), ParameterCode, value, i);
                break;
              case 2:
                value = V2((u_char *)LogParameter + SIZEOF(LogParameter_T));
                g_fprintf(StatFile, _("ParameterCode %02X = %u(%d)\n"), ParameterCode, value, i);
                break;
              case 3:
                value = V3((u_char *)LogParameter + SIZEOF(LogParameter_T));
                g_fprintf(StatFile, _("ParameterCode %02X = %u(%d)\n"), ParameterCode, value, i);
                break;
              case 4:
                value = V4((u_char *)LogParameter + SIZEOF(LogParameter_T));
                g_fprintf(StatFile, _("ParameterCode %02X = %u(%d)\n"), ParameterCode, value, i);
                break;
              case 5:
                value = V5((u_char *)LogParameter + SIZEOF(LogParameter_T));
                g_fprintf(StatFile, _("ParameterCode %02X = %u(%d)\n"), ParameterCode, value, i);
                break;
              default:
                g_fprintf(StatFile, _("ParameterCode %02X size %d\n"), ParameterCode, i);
              }
              LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
            }
            g_fprintf(StatFile, "\n");
          }
        }
      }

      /*
       * Test only !!!!
       * Reset the cumulative counters
       */
      CDB[0] = SC_COM_LOG_SELECT;
      CDB[1] = 2;
      CDB[2] = 0xc0;
      CDB[3] = 0;
      CDB[4] = 0;
      CDB[5] = 0;
      CDB[6] = 0;
      CDB[7] = 0;
      CDB[8] = 0;
      CDB[9] = 0;

      if (SCSI_Run(INDEX_TAPECTL, Input, CDB, 10,
                              buffer,
                              size,
                              pRequestSense,
                              SIZEOF(RequestSense_T)) != 0)
        {
          DecodeSense(pRequestSense, "LogSense : ",debug_file);
          free(pRequestSense);
	  free(logpages);
	  /*@ignore@*/
          free(buffer);
	  /*@end@*/
	  fclose(StatFile);
          return(0);
	  /*NOTREACHED*/
        }

      free(pRequestSense);
      free(logpages);
      /*@ignore@*/
      free(buffer);
      /*@end@*/
      fclose(StatFile);
    }
  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("##### STOP LogSense\n"));
  return(0);
}

void
WriteErrorCountersPage(
    LogParameter_T *	buffer,
    size_t		length)
{
  int i;
  unsigned value;
  LogParameter_T *LogParameter;
  unsigned ParameterCode;
  LogParameter = buffer;

  g_fprintf(StatFile, _("\tWrite Error Counters Page\n"));

  while ((u_char *)LogParameter < ((u_char *)buffer + length)) {
    i = LogParameter->ParameterLength;
    ParameterCode = V2(LogParameter->ParameterCode);

    value = 0;
    if (Decode(LogParameter, &value) == 0) {
      switch (ParameterCode) {
      case 2:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Rewrites"),
                value);
        break;
      case 3:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Errors Corrected"),
                value);
        break;
      case 4:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Times E. Processed"),
                value);
        break;
      case 5:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Bytes Processed"),
                value);
        break;
      case 6:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Unrecoverable Errors"),
                value);
        break;
      default:
        g_fprintf(StatFile, _("Unknown ParameterCode %02X = %u(%d)\n"),
                ParameterCode,
                value, i);
        break;
      }
    } else {
      g_fprintf(StatFile, _("Error decoding Result\n"));
    }
    LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
  }
}

void
ReadErrorCountersPage(
    LogParameter_T *	buffer,
    size_t		length)
{
  int i;
  unsigned value;
  LogParameter_T *LogParameter;
  unsigned ParameterCode;
  LogParameter = buffer;

  g_fprintf(StatFile, _("\tRead Error Counters Page\n"));

  while ((u_char *)LogParameter < ((u_char *)buffer + length)) {
    i = LogParameter->ParameterLength;
    ParameterCode = V2(LogParameter->ParameterCode);

    value = 0;
    if (Decode(LogParameter, &value) == 0) {
      switch (ParameterCode) {
      case 2:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Rereads"),
                value);
        break;
      case 3:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Errors Corrected"),
                value);
        break;
      case 4:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Times E. Processed"),
                value);
        break;
      case 5:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Bytes Processed"),
                value);
        break;
      case 6:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Unrecoverable Errors"),
                value);
        break;
      default:
        g_fprintf(StatFile, _("Unknown ParameterCode %02X = %u(%d)\n"),
                ParameterCode,
                value, i);
        break;
      }
    } else {
      g_fprintf(StatFile, _("Error decoding Result\n"));
    }
    LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
  }
}

void
C1553APage30(
    LogParameter_T *	buffer,
    size_t		length)
{
  int i;
  unsigned value;
  LogParameter_T *LogParameter;
  unsigned ParameterCode;
  LogParameter = buffer;

  g_fprintf(StatFile, _("\tData compression transfer Page\n"));

  while ((u_char *)LogParameter < ((u_char *)buffer + length)) {
    i = LogParameter->ParameterLength;
    ParameterCode = V2(LogParameter->ParameterCode);

    value = 0;
    if (Decode(LogParameter, &value) == 0) {
      switch (ParameterCode) {
      default:
        g_fprintf(StatFile, _("Unknown ParameterCode %02X = %u(%d)\n"),
                ParameterCode,
                value, i);
        break;
      }
    }
    LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
  }
}

void
C1553APage37(
    LogParameter_T *	buffer,
    size_t		length)
{
  int i;
  unsigned value;
  LogParameter_T *LogParameter;
  unsigned ParameterCode;
  LogParameter = buffer;

  g_fprintf(StatFile, _("\tDrive Counters Page\n"));

  while ((u_char *)LogParameter < ((unsigned char *)buffer + length)) {
    i = LogParameter->ParameterLength;
    ParameterCode = V2(LogParameter->ParameterCode);

    value = 0;
    if (Decode(LogParameter, &value) == 0) {
      switch (ParameterCode) {
      case 1:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total loads"),
                value);
        break;
      case 2:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total write drive errors"),
                value);
        break;
      case 3:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total read drive errors"),
                value);
        break;
      default:
        g_fprintf(StatFile, _("Unknown ParameterCode %02X = %u(%d)\n"),
                ParameterCode,
                value, i);
        break;
      }
    }
    LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
  }
}

void
EXB85058HEPage39(
    LogParameter_T *	buffer,
    size_t		length)
{
  int i;
  unsigned value;
  LogParameter_T *LogParameter;
  unsigned ParameterCode;
  LogParameter = buffer;

  g_fprintf(StatFile, _("\tData Compression Page\n"));

  while ((u_char *)LogParameter < ((unsigned char *)buffer + length)) {
    i = LogParameter->ParameterLength;
    ParameterCode = V2(LogParameter->ParameterCode);

    value = 0;
    if (Decode(LogParameter, &value) == 0) {
      switch (ParameterCode) {
      case 5:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("KB to Compressor"),
                value);
        break;
      case 7:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("KB to tape"),
                value);
        break;
      default:
        g_fprintf(StatFile, _("Unknown ParameterCode %02X = %u(%d)\n"),
                ParameterCode,
                value, i);
        break;
      }
    }
    LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
  }
}

void
EXB85058HEPage3c(
    LogParameter_T *	buffer,
    size_t		length)
{
  int i;
  unsigned value;
  LogParameter_T *LogParameter;
  unsigned ParameterCode;
  LogParameter = buffer;

  g_fprintf(StatFile, _("\tDrive Usage Information Page\n"));

  while ((u_char *)LogParameter < ((unsigned char *)buffer + length)) {
    i = LogParameter->ParameterLength;
    ParameterCode = V2(LogParameter->ParameterCode);

    value = 0;
    if (Decode(LogParameter, &value) == 0) {
      switch (ParameterCode) {
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
        break;
      case 6:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Total Load Count"),
                value);
        break;
      case 7:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("MinutesSince Last Clean"),
                value);
        break;
      case 8:
      case 9:
        break;
      case 0xa:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Cleaning Count"),
                value);
        break;
      case 0xb:
      case 0xc:
      case 0xd:
      case 0xe:
      case 0xf:
      case 0x10:
        break;
      case 0x11:
        g_fprintf(StatFile, _("%-30s = %u\n"),
                _("Time to clean"),
                value);
        break;
      case 0x12:
      case 0x13:
      case 0x14:
        break;
      default:
        g_fprintf(StatFile, _("Unknown ParameterCode %02X = %u(%d)\n"),
                ParameterCode,
                value, i);
        break;
      }
    }
    LogParameter = (LogParameter_T *)((u_char *)LogParameter +  SIZEOF(LogParameter_T) + i);
  }
}

int
Decode(
    LogParameter_T *	LogParameter,
    unsigned *		value)
{

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START Decode\n"));
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("Decode Parameter with length %d\n"), LogParameter->ParameterLength);

  *value = 0;
  switch (LogParameter->ParameterLength) {
  case 1:
    *value = V1((u_char *)LogParameter + SIZEOF(LogParameter_T));
    break;
  case 2:
    *value = V2((u_char *)LogParameter + SIZEOF(LogParameter_T));
    break;
  case 3:
    *value = V3((u_char *)LogParameter + SIZEOF(LogParameter_T));
    break;
  case 4:
    *value = V4((u_char *)LogParameter + SIZEOF(LogParameter_T));
    break;
  case 5:
    *value = V5((u_char *)LogParameter + SIZEOF(LogParameter_T));
    break;
  case 6:
    *value = V6((u_char *)LogParameter + SIZEOF(LogParameter_T));
    break;
  default:
    g_fprintf(StatFile, _("Can't decode ParameterCode %02X size %d\n"),
            V2(LogParameter->ParameterCode), LogParameter->ParameterLength);
    DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP Decode (1)\n"));
    return(1);
    /*NOTREACHED*/
  }
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("Result = %d\n"), *value);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP Decode(0)\n"));
  return(0);
}

void
DumpDev(
    OpenFiles_T *	p,
    char *		device)
{
	if (p != NULL)
	{
		g_printf(_("%s Devicefd   %d\n"), device, p->fd);
		g_printf(_("%s Can SCSI   %d\n"), device, p->SCSI);
		g_printf(_("%s Device     %s\n"), device, (p->dev != NULL)? p->dev:_("No set"));
		g_printf(_("%s ConfigName %s\n"), device, (p->ConfigName != NULL) ? p->ConfigName:_("Not ser"));
	} else {
		g_printf(_("%s Null Pointer ....\n"), device);
	}
	g_printf(_("\n"));
}

void
ChangerReplay(
    char *	option)
{
    u_char buffer[1024];
    FILE *ip;
    int x;
    unsigned bufferx;

    (void)option;	/* Quiet unused parameter warning */

    if ((ip=fopen("/tmp/chg-scsi-trace", "r")) == NULL)
      {
	exit(1);
      }

    for (x = 0; x < 1024; x++)
      {
        if (fscanf(ip, "%2x", &bufferx) == EOF) 
	  {
	    break;
	    /*NOTREACHED*/
	  }
        buffer[x] = (u_char)bufferx;
        x++;
      }

    DecodeModeSense(&buffer[0], 12, "DLT448ElementStatus :", 0, debug_file);
    fclose(ip);
}

/*
 * Display all Information we can get about the library....
 */
void
ChangerStatus(
    char *	option,
    char *	labelfile,
    int		HasBarCode,
    char *	changer_file,
    char *	changer_dev,
    char *	tape_device)
{
  extern OpenFiles_T *pDev;
  size_t x;
  FILE *out;
  ExtendedRequestSense_T ExtRequestSense;
  MBC_T *pbarcoderes;

  ChangerCMD_T *p = (ChangerCMD_T *)&ChangerIO;
  pbarcoderes = alloc(SIZEOF(MBC_T));
  memset(pbarcoderes, 0, SIZEOF(MBC_T));

  if (pModePage == NULL) {
	 pModePage = alloc(0xff);
  }

  if ((out = fdopen(1 , "w")) == NULL)
    {
      g_printf(_("Error fdopen stdout\n"));
      free(pbarcoderes);
      return;
      /*NOTREACHED*/
    }

  if (strcmp("types", option) == 0 || strcmp("all", option) == 0)
  {
    while(p->ident != NULL)
      {
         g_printf (_("Ident = %s, type = %s\n"),p->ident, p->type);
         p++;
      }
    DumpSense();
  }

  if (strcmp("robot", option) == 0 || strcmp("all", option) == 0)
      {
        if (ElementStatusValid == 0)
          {
            if (pDev[INDEX_CHANGER].functions->function_status(pDev[INDEX_CHANGER].fd, 1) != 0)
              {
                g_printf(_("Can not initialize changer status\n"));
		free(pbarcoderes);
	        fclose(out);
                return;
		/*NOTREACHED*/
              }
          }
        /*      0123456789012345678901234567890123456789012 */
	if (HasBarCode)
	{
        	g_printf(_("Address Type Status From Barcode Label\n"));
	} else {
        	g_printf(_("Address Type Status From\n"));
	}
        g_printf(_("-------------------------------------------\n"));


        for ( x = 0; x < MTE; x++)
	if (HasBarCode)
	{
          g_printf(_("%07d MTE  %s  %04d %s "),pMTE[x].address,
                 (pMTE[x].full ? _("Full ") :_("Empty")),
                 pMTE[x].from, pMTE[x].VolTag);

	  if (pMTE[x].full == 1)
	    {
	      pbarcoderes->action = BARCODE_BARCODE;
	      strncpy(pbarcoderes->data.barcode, pMTE[x].VolTag,
		      SIZEOF(pbarcoderes->data.barcode));

	      if (MapBarCode(labelfile, pbarcoderes) == 0 )
		{
		  g_printf(_("No mapping\n"));
		} else {
		  g_printf(_("%s \n"),pbarcoderes->data.voltag);
		}
	    } else {
	      g_printf("\n");
	    }
	} else {
          g_printf(_("%07d MTE  %s  %04d \n"),pMTE[x].address,
                 (pMTE[x].full ? _("Full ") :_("Empty")),
                 pMTE[x].from);
	}


        for ( x = 0; x < STE; x++)
	if (HasBarCode)
	{
          g_printf(_("%07d STE  %s  %04d %s "),pSTE[x].address,
                 (pSTE[x].full ? _("Full "):_("Empty")),
                 pSTE[x].from, pSTE[x].VolTag);

	  if (pSTE[x].full == 1)
	    {
	      pbarcoderes->action = BARCODE_BARCODE;
	      strncpy(pbarcoderes->data.barcode, pSTE[x].VolTag,
		      SIZEOF(pbarcoderes->data.barcode));

	      if (MapBarCode(labelfile, pbarcoderes) == 0 )
		{
		  g_printf(_("No mapping\n"));
		} else {
		  g_printf(_("%s \n"),pbarcoderes->data.voltag);
		}
	    } else {
	      g_printf("\n");
	    }
	} else {
          g_printf(_("%07d STE  %s  %04d %s\n"),pSTE[x].address,
                 (pSTE[x].full ? _("Full"):_("Empty")),
                 pSTE[x].from, pSTE[x].VolTag);
	}


        for ( x = 0; x < DTE; x++)
	if (HasBarCode)
	{
          g_printf(_("%07d DTE  %s  %04d %s "),pDTE[x].address,
                 (pDTE[x].full ? _("Full") : _("Empty")),
                 pDTE[x].from, pDTE[x].VolTag);

	  if (pDTE[x].full == 1)
	    {
	      pbarcoderes->action = BARCODE_BARCODE;
	      strncpy(pbarcoderes->data.barcode, pDTE[x].VolTag,
		      SIZEOF(pbarcoderes->data.barcode));

	      if (MapBarCode(labelfile, pbarcoderes) == 0 )
		{
		  g_printf(_("No mapping\n"));
		} else {
		  g_printf("%s \n",pbarcoderes->data.voltag);
		}
	    } else {
	      g_printf("\n");
	    }

	} else {
          g_printf(_("%07d DTE  %s  %04d %s\n"),pDTE[x].address,
                 (pDTE[x].full ?_( "Full ") : _("Empty")),
                 pDTE[x].from, pDTE[x].VolTag);
	}

        for ( x = 0; x < IEE; x++)
	if (HasBarCode)
	{
          g_printf(_("%07d IEE  %s  %04d %s "),pIEE[x].address,
                 (pIEE[x].full ? _("Full ") : _("Empty")),
                 pIEE[x].from, pIEE[x].VolTag);

	  if (pIEE[x].full == 1)
	    {
	      pbarcoderes->action = BARCODE_BARCODE;
	      strncpy(pbarcoderes->data.barcode, pIEE[x].VolTag,
		      SIZEOF(pbarcoderes->data.barcode));

	      if (MapBarCode(labelfile, pbarcoderes) == 0 )
		{
		  g_printf(_("No mapping\n"));
		} else {
		  g_printf(_("%s \n"),pbarcoderes->data.voltag);
		}
	    } else {
	      g_printf("\n");
	    }

	} else {
          g_printf(_("%07d IEE  %s  %04d %s\n"),pIEE[x].address,
                 (pIEE[x].full ? _("Full ") : _("Empty")),
                 pIEE[x].from, pIEE[x].VolTag);
	}

      }

  if (strcmp("sense", option) == 0 || strcmp("all", option) == 0)
    {
      if (pDev[INDEX_CHANGER].SCSI == 1)
	{
           g_printf(_("\nSense Status from robot:\n"));
           RequestSense(INDEX_CHANGER , &ExtRequestSense, 0);
           DecodeExtSense(&ExtRequestSense, "", out);
	}

      if (pDev[INDEX_TAPE].SCSI == 1)
        {
          g_printf("\n");
          g_printf(_("Sense Status from tape (tapectl):\n"));
          RequestSense(INDEX_TAPE, &ExtRequestSense, 0);
          DecodeExtSense(&ExtRequestSense, "", out);
        }

      if (pDev[INDEX_TAPECTL].SCSI == 1)
        {
          g_printf("\n");
          g_printf(_("Sense Status from tape (tapectl):\n"));
          RequestSense(INDEX_TAPECTL, &ExtRequestSense, 0);
          DecodeExtSense(&ExtRequestSense, "", out);
        }
    }

    if (strcmp("ModeSenseRobot", option) == 0 || strcmp("all", option) == 0)
      {
        g_printf("\n");
        if (SCSI_ModeSense(INDEX_CHANGER, pModePage, 0xff, 0x08, 0x3f) == 0)
          {
            DecodeModeSense(pModePage, 0, "Changer :" , 0, out);
          }
      }

    if (strcmp("ModeSenseTape", option) == 0 || strcmp("all", option) == 0)
      {
        if (pDev[INDEX_TAPECTL].SCSI == 1)
        {
          g_printf("\n");
          if (SCSI_ModeSense(INDEX_TAPECTL, pModePage, 0xff, 0x0, 0x3f) == 0)
            {
              DecodeModeSense(pModePage, 0, "Tape :" , 1, out);
            }
        }
      }

    if (strcmp("fd", option) == 0 || strcmp("all", option) == 0)
    {
      g_printf("changer_dev  %s\n",changer_dev);
      g_printf("changer_file %s\n", changer_file);
      g_printf("tape_device  %s\n\n", tape_device);
      DumpDev(&pDev[INDEX_TAPE], "pTapeDev");
      DumpDev(&pDev[INDEX_TAPECTL], "pTapeDevCtl");
      DumpDev(&pDev[INDEX_CHANGER], "pChangerDev");
    }

  if (GenericClean("") == 1)
    g_printf(_("Tape needs cleaning\n"));

  free(pbarcoderes);
  fclose(out);
}

void
dump_hex(
    u_char *	p,
    size_t	size,
    int		level,
    int		section)
{
    size_t row_count = 0;
    int x;

    while (row_count < size)
    {
        DebugPrint(level, section,"%02X ", (u_char)p[row_count]);
        if (((row_count + 1) % 16) == 0)
          {
            dbprintf("   ");
            for (x = 16; x > 0; x--)
              {
		if (isalnum((u_char)p[row_count - x + 1 ]))
		  DebugPrint(level, section,"%c",(u_char)p[row_count - x + 1]);
		else
		  DebugPrint(level, section,".");
	      }
            DebugPrint(level, section,"\n");
          }
    	row_count++;
    }
    DebugPrint(level, section,"\n");
}

void
TerminateString(
    char *	string,
    size_t	length)
{
  ssize_t x;

  for (x = (ssize_t)length; x >= 0 && !isalnum((int)string[x]); x--)
    string[x] = '\0';
}

void
ChgExit(
    char *	where,
    char *	reason,
    int level)
{
    (void)level;	/* Quiet unused parameter warning */

   dbprintf(_("ChgExit in %s, reason %s\n"), where, reason);
   g_fprintf(stderr,"%s\n",reason);
   exit(2);
}

/* OK here starts a new set of functions.
 * Every function is for one SCSI command.
 * Prefix is SCSI_ and then the SCSI command name
*/

/*
 * SCSI_Run is an wrapper arround SCSI_ExecuteCommand
 * It seems to be an good idea to check first if the device
 * is ready for accepting commands, and if this is true send
 * the command
 */
int
SCSI_Run(
    int			DeviceFD,
    Direction_T		Direction,
    CDB_T		CDB,
    size_t		CDB_Length,
    void *		DataBuffer,
    size_t		DataBufferLength,
    RequestSense_T *	pRequestSense,
    size_t		RequestSenseLength)
{
  int ret = 0;
  int ok = 0;
  int maxtries = 0;
  RequestSense_T *pRqS;

  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  pRqS = (RequestSense_T *)pRequestSense;

  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("SCSI_Run TestUnitReady\n"));
  while (!ok && maxtries < MAXTRIES)
    {
      ret = SCSI_TestUnitReady(DeviceFD, (RequestSense_T *)pRequestSense );
      DebugPrint(DEBUG_INFO, SECTION_SCSI, _("SCSI_Run TestUnitReady ret %d\n"),ret);
      switch (ret)
	{
	case SCSI_OK:
	  ok=1;
	  break;
	case SCSI_SENSE:
	  switch (SenseHandler(DeviceFD, 0, pRqS->SenseKey, pRqS->AdditionalSenseCode, pRqS->AdditionalSenseCodeQualifier, pRqS))
	    {
	    case SENSE_NO:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SENSE_NO\n"));
	      ok=1;
	      break;
	    case SENSE_TAPE_NOT_ONLINE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SENSE_TAPE_NOT_ONLINE\n"));
	      ok=1;
	      break;
	    case SENSE_IGNORE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SENSE_IGNORE\n"));
	      ok=1;
	      break;
	    case SENSE_ABORT:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SENSE_ABORT\n"));
	      return(-1);
	      /*NOTREACHED*/
	    case SENSE_RETRY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SENSE_RETRY\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) default (SENSE)\n"));
	      ok=1;
	      break;
	    }
	  break;
	case SCSI_ERROR:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SCSI_ERROR\n"));
	  return(-1);
	  /*NOTREACHED*/
	case SCSI_BUSY:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SCSI_BUSY\n"));
	  break;
	case SCSI_CHECK:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SCSI_CHECK\n"));
	  break;
	default:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run (TestUnitReady) unknown (%d)\n"),ret);
	  break;
	}
      if (!ok)
      {
	sleep(1);
        maxtries++;
      }
    }

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run TestUnitReady after %d sec:\n"),maxtries);

  if (ok != 1)
    {
      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run Exit %d\n"),ret);
      return(-1);
      /*NOTREACHED*/
    }

  ok = 0;
  maxtries = 0;
  while (!ok && maxtries < MAXTRIES)
    {
      ret = SCSI_ExecuteCommand(DeviceFD,
				Direction,
				CDB,
				CDB_Length,
				DataBuffer,
				DataBufferLength,
				pRequestSense,
				RequestSenseLength);

      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run Exit %d\n"),ret);
      switch (ret)
	{
	case SCSI_OK:
	  ok=1;
	  break;
	case SCSI_SENSE:
	  switch (SenseHandler(DeviceFD, 0, pRqS->SenseKey, pRqS->AdditionalSenseCode, pRqS->AdditionalSenseCodeQualifier, pRqS))
	    {
	    case SENSE_NO:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run SENSE_NO\n"));
	      ok=1;
	      break;
	    case SENSE_TAPE_NOT_ONLINE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run SENSE_TAPE_NOT_ONLINE\n"));
	      ok=1;
	      break;
	    case SENSE_IGNORE:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run SENSE_IGNORE\n"));
	      ok=1;
	      break;
	    case SENSE_ABORT:
	      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run SENSE_ABORT\n"));
	      return(-1);
	      /*NOTREACHED*/
	    case SENSE_RETRY:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run SENSE_RETRY\n"));
	      break;
	    default:
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run default (SENSE)\n"));
	      ok=1;
	      break;
	    }
	  break;
	case SCSI_ERROR:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run SCSI_ERROR\n"));
	  return(-1);
	  /*NOTREACHED*/
	case SCSI_BUSY:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run SCSI_BUSY\n"));
	  break;
	case SCSI_CHECK:
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Run (TestUnitReady) SCSI_CHECK\n"));
	  break;
	default:
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_Run (TestUnitReady) unknown (%d)\n"),ret);
	  break;
	}
      maxtries++;
      sleep(1);
    }

  if (ok == 1)
    {
      return(0);
      /*NOTREACHED*/
    }
  return(-1);
}

/*
 * This a vendor specific command !!!!!!
 * First seen at AIT :-)
 */
int
SCSI_AlignElements(
    int		DeviceFD,
    size_t	AE_MTE,
    size_t	AE_DTE,
    size_t	AE_STE)
{
  RequestSense_T *pRequestSense;
  int retry;
  CDB_T CDB;
  int ret;
  int i;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START SCSI_AlignElements\n"));

  pRequestSense = alloc(SIZEOF(RequestSense_T));

  for (retry = 0; retry < MAX_RETRIES; retry++)
    {
      CDB[0]  = 0xE5;
      CDB[1]  = 0;
      MSB2(&CDB[2],AE_MTE);	/* Which MTE to use, default 0 */
      MSB2(&CDB[4],AE_DTE);	/* Which DTE to use, no range check !! */
      MSB2(&CDB[6],AE_STE);	/* Which STE to use, no range check !! */
      CDB[8]  = 0;
      CDB[9]  = 0;
      CDB[10] = 0;
      CDB[11] = 0;

      ret = SCSI_Run(DeviceFD, Input, CDB, 12,
                                NULL, 0, pRequestSense, SIZEOF(RequestSense_T));

      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_AlignElements : SCSI_Run = %d\n"), ret);
      DecodeSense(pRequestSense, _("SCSI_AlignElements :"),debug_file);

      if (ret < 0)
        {
          DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("%s: Request Sense[Inquiry]: %02X"),
                    "chs", ((u_char *) &pRequestSense)[0]);
          for (i = 1; i < (int)sizeof(RequestSense_T); i++)
            DebugPrint(DEBUG_ERROR, SECTION_SCSI," %02X", ((u_char *) &pRequestSense)[i]);
          DebugPrint(DEBUG_ERROR, SECTION_SCSI,"\n");
          return(ret);
	  /*NOTREACHED*/
        }
      if ( ret > 0)
        {
          switch(SenseHandler(DeviceFD, 0 , pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
            {
            case SENSE_IGNORE:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_AlignElements : SENSE_IGNORE\n"));
              return(0);
              /*NOTREACHED*/
            case SENSE_RETRY:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_AlignElements : SENSE_RETRY no %d\n"), retry);
              break;
            case SENSE_ABORT:
              DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_AlignElements : SENSE_ABORT\n"));
              return(-1);
              /*NOTREACHED*/
            case SENSE_TAPE_NOT_UNLOADED:
              DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_AlignElements : Tape still loaded, eject failed\n"));
              return(-1);
              /*NOTREACHED*/
            default:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_AlignElements : end %d\n"), pRequestSense->SenseKey);
              return(pRequestSense->SenseKey);
              /*NOTREACHED*/
            }
        }
      if (ret == 0)
        {
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_AlignElements : end %d\n"), ret);
          return(ret);
	  /*NOTREACHED*/
        }
    }
  DebugPrint(DEBUG_ERROR, SECTION_SCSI,
	    _("SCSI_AlignElements: Retries exceeded = %d\n"), retry);
  return(-1);
}


int
SCSI_Move(
    int		DeviceFD,
    u_char	chm,
    int		from,
    int		to)
{
  RequestSense_T *pRequestSense;
  int retry;
  CDB_T CDB;
  int ret = -1;
  int i;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START SCSI_Move\n"));

  pRequestSense = alloc(SIZEOF(RequestSense_T));

  for (retry = 0; (ret != 0) && (retry < MAX_RETRIES); retry++)
    {
      CDB[0]  = SC_MOVE_MEDIUM;
      CDB[1]  = 0;
      CDB[2]  = 0;
      CDB[3]  = chm;     /* Address of CHM */
      MSB2(&CDB[4],from);
      MSB2(&CDB[6],to);
      CDB[8]  = 0;
      CDB[9]  = 0;
      CDB[10] = 0;
      CDB[11] = 0;

      ret = SCSI_Run(DeviceFD, Input, CDB, 12,
                                NULL, 0, pRequestSense, SIZEOF(RequestSense_T));

      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Move : SCSI_Run = %d\n"), ret);
      DecodeSense(pRequestSense, _("SCSI_Move :"),debug_file);

      if (ret < 0)
        {
          DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("%s: Request Sense[Inquiry]: %02X"),
                    "chs", ((u_char *) &pRequestSense)[0]);
          for (i = 1; i < (int)sizeof(RequestSense_T); i++)
            DebugPrint(DEBUG_ERROR, SECTION_SCSI," %02X", ((u_char *) &pRequestSense)[i]);
          DebugPrint(DEBUG_INFO, SECTION_SCSI,"\n");
          return(ret);
	  /*NOTREACHED*/
        }
      if ( ret > 0)
        {
          switch(SenseHandler(DeviceFD,  0 , pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
            {
            case SENSE_IGNORE:
              dbprintf(_("SCSI_Move : SENSE_IGNORE\n"));
              return(0);
              /*NOTREACHED*/
            case SENSE_RETRY:
              dbprintf(_("SCSI_Move : SENSE_RETRY no %d\n"), retry);
              break;
            case SENSE_ABORT:
              dbprintf(_("SCSI_Move : SENSE_ABORT\n"));
              return(-1);
              /*NOTREACHED*/
            case SENSE_TAPE_NOT_UNLOADED:
              dbprintf(_("SCSI_Move : Tape still loaded, eject failed\n"));
              return(-1);
              /*NOTREACHED*/
            default:
              dbprintf(_("SCSI_Move : end %d\n"), pRequestSense->SenseKey);
              return(pRequestSense->SenseKey);
              /*NOTREACHED*/
            }
        }
    }
  dbprintf(_("SCSI_Move : end %d\n"), ret);
  return(ret);
}

int
SCSI_LoadUnload(
    int			DeviceFD,
    RequestSense_T *	pRequestSense,
    u_char		byte1,
    u_char		load)
{
  CDB_T CDB;
  int ret;

  dbprintf(_("##### START SCSI_LoadUnload\n"));

  CDB[0] = SC_COM_UNLOAD;
  CDB[1] = byte1;
  CDB[2] = 0;
  CDB[3] = 0;
  CDB[4] = load;
  CDB[5] = 0;


  ret = SCSI_Run(DeviceFD, Input, CDB, 6,
                            NULL, 0,
                            pRequestSense,
                            SIZEOF(RequestSense_T));

  if (ret < 0)
    {
      dbprintf(_("SCSI_Unload : failed %d\n"), ret);
      return(-1);
      /*NOTREACHED*/
    }

  dbprintf(_("##### STOP SCSI_LoadUnload\n"));
  return(ret);
}

int
SCSI_TestUnitReady(
    int			DeviceFD,
    RequestSense_T *	pRequestSense)
{
  CDB_T CDB;
  int ret;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START SCSI_TestUnitReady\n"));

  CDB[0] = SC_COM_TEST_UNIT_READY;
  CDB[1] = 0;
  CDB[2] = 0;
  CDB[3] = 0;
  CDB[4] = 0;
  CDB[5] = 0;

  ret = SCSI_ExecuteCommand(DeviceFD, Input, CDB, 6,
                      NULL, (size_t)0,
                      pRequestSense,
                      SIZEOF(RequestSense_T));

  /*
   * We got an error, so let the calling function handle this
   */
  if (ret > 0)
    {
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("###### STOP SCSI_TestUnitReady (1)\n"));
      return(ret);
      /*NOTREACHED*/
    }

  /*
   * OK, no error condition
   * If no sense is set, the Unit is ready
   */
  if (pRequestSense->ErrorCode == 0 && pRequestSense->SenseKey == 0)
    {
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("###### STOP SCSI_TestUnitReady (1)\n"));
      return(0);
      /*NOTREACHED*/
    }

  /*
   * Some sense is set
   */
  if (pRequestSense->ErrorCode != 0){
    DebugPrint(DEBUG_INFO, SECTION_SCSI,_("###### STOP SCSI_TestUnitReady ErrorCode set\n"));
  }
  if (pRequestSense->SenseKey != 0) {
    DebugPrint(DEBUG_INFO, SECTION_SCSI,_("###### STOP SCSI_TestUnitReady Sense Key set\n"));
  }
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("###### STOP SCSI_TestUnitReady (0)\n"));
  return(SCSI_SENSE);
}


int
SCSI_ModeSelect(
    int		DeviceFD,
    u_char *	buffer,
    u_char	length,
    u_char	save,
    u_char	mode,
    u_char	lun)
{
  CDB_T CDB;
  RequestSense_T *pRequestSense;
  int ret = -1;
  int retry;
  u_char *sendbuf;

  dbprintf(_("##### START SCSI_ModeSelect\n"));

  dbprintf(_("SCSI_ModeSelect start length = %u:\n"), (unsigned)length);
  pRequestSense = alloc(SIZEOF(RequestSense_T));
  sendbuf = alloc((size_t)length + 4);
  memset(sendbuf, 0 , (size_t)length + 4);

  memcpy(&sendbuf[4], buffer, (size_t)length);
  dump_hex(sendbuf, (size_t)length+4, DEBUG_INFO, SECTION_SCSI);

  for (retry = 0; (ret != 0) && (retry < MAX_RETRIES); retry++)
    {
      memset(pRequestSense, 0, SIZEOF(RequestSense_T));

      CDB[0] = SC_COM_MODE_SELECT;
      CDB[1] = (u_char)(((lun << 5) & 0xF0) | ((mode << 4) & 0x10) | (save & 1));
      CDB[2] = 0;
      CDB[3] = 0;
      CDB[4] = (u_char)(length + 4);
      CDB[5] = 0;
      ret = SCSI_Run(DeviceFD, Output, CDB, 6,
                                sendbuf,
                                (size_t)length + 4,
                                pRequestSense,
                                SIZEOF(RequestSense_T));
      if (ret < 0)
        {
          dbprintf(_("SCSI_ModeSelect : ret %d\n"), ret);
	  goto done;
          /*NOTREACHED*/
        }

      if ( ret > 0)
        {
          switch(SenseHandler(DeviceFD, 0, pRequestSense->SenseKey,
			      pRequestSense->AdditionalSenseCode,
			      pRequestSense->AdditionalSenseCodeQualifier,
			      pRequestSense))
            {
            case SENSE_IGNORE:
              dbprintf(_("SCSI_ModeSelect : SENSE_IGNORE\n"));
	      goto done;
              /*NOTREACHED*/

            case SENSE_RETRY:
              dbprintf(_("SCSI_ModeSelect : SENSE_RETRY no %d\n"), retry);
              break;

            default:
	      ret = pRequestSense->SenseKey;
	      goto end;
            }
        }
    }
end:
  dbprintf(_("SCSI_ModeSelect end: %d\n"), ret);

done:
  free(pRequestSense);
  free(sendbuf);
  return(ret);
}



int
SCSI_ModeSense(
    int		DeviceFD,
    u_char *	buffer,
    u_char	size,
    u_char	byte1,
    u_char	byte2)
{
  CDB_T CDB;
  RequestSense_T *pRequestSense;
  int ret = 1;
  int retry = 1;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START SCSI_ModeSense\n"));

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense start length = %d:\n"), size);
  pRequestSense = alloc(SIZEOF(RequestSense_T));

  while (ret && retry < MAX_RETRIES)
    {
      memset(pRequestSense, 0, SIZEOF(RequestSense_T));
      memset(buffer, 0, size);

      CDB[0] = SC_COM_MODE_SENSE;
      CDB[1] = byte1;
      CDB[2] = byte2;
      CDB[3] = 0;
      CDB[4] = size;
      CDB[5] = 0;
      ret = SCSI_Run(DeviceFD, Input, CDB, 6,
                                buffer,
                                size,
                                pRequestSense,
                                SIZEOF(RequestSense_T));
      if (ret < 0)
        {
          return(ret);
	  /*NOTREACHED*/
        }

      if ( ret > 0)
        {
          switch(SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
            {
            case SENSE_IGNORE:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : SENSE_IGNORE\n"));
              return(0);
              /*NOTREACHED*/
            case SENSE_RETRY:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : SENSE_RETRY no %d\n"), retry);
              break;
            default:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : end %d\n"), pRequestSense->SenseKey);
              return(pRequestSense->SenseKey);
              /*NOTREACHED*/
            }
        }
      retry++;
    }

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense end: %d\n"), ret);
  return(ret);
}

int
SCSI_Inquiry(
    int			DeviceFD,
    SCSIInquiry_T *	buffer,
    size_t		size)
{
  CDB_T CDB;
  RequestSense_T *pRequestSense;
  int i;
  int ret = -1;
  int retry = 1;

  assert(size <= UCHAR_MAX);

  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("##### START SCSI_Inquiry\n"));

  DebugPrint(DEBUG_INFO, SECTION_SCSI, _("SCSI_Inquiry start length = %d:\n"), size);

  pRequestSense = alloc((size_t)size);

  while (retry > 0 && retry < MAX_RETRIES)
    {
      memset(buffer, 0, size);
      CDB[0] = SC_COM_INQUIRY;
      CDB[1] = 0;
      CDB[2] = 0;
      CDB[3] = 0;
      CDB[4] = (u_char)size;
      CDB[5] = 0;

      ret = SCSI_ExecuteCommand(DeviceFD, Input, CDB, 6,
                                buffer,
                                size,
                                pRequestSense,
                                SIZEOF(RequestSense_T));
      if (ret < 0)
        {
          DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("%s: Request Sense[Inquiry]: %02X"),
                    "chs", ((u_char *) pRequestSense)[0]);
          for (i = 1; i < (int)sizeof(RequestSense_T); i++)
            DebugPrint(DEBUG_ERROR, SECTION_SCSI," %02X", ((u_char *) pRequestSense)[i]);
          DebugPrint(DEBUG_ERROR, SECTION_SCSI, "\n");
	  DebugPrint(DEBUG_ERROR, SECTION_SCSI, _("Inquiry end: %d\n"), ret);
	  return(ret);
	  /*NOTRACHED*/
        }
      if ( ret > 0)
        {
          switch(SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
            {
            case SENSE_IGNORE:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Inquiry : SENSE_IGNORE\n"));
              return(0);
              /*NOTREACHED*/
            case SENSE_RETRY:
              DebugPrint(DEBUG_INFO, SECTION_SCSI, _("SCSI_Inquiry : SENSE_RETRY no %d\n"), retry);
              break;
            default:
              DebugPrint(DEBUG_ERROR, SECTION_SCSI, _("SCSI_Inquiry : end %d\n"), pRequestSense->SenseKey);
              return(pRequestSense->SenseKey);
              /*NOTREACHED*/
            }
        }
      retry++;
      if (ret == 0)
        {
          dump_hex((u_char *)buffer, size, DEBUG_INFO, SECTION_SCSI);
          DebugPrint(DEBUG_INFO, SECTION_SCSI, _("SCSI_Inquiry : end %d\n"), ret);
          return(ret);
	  /*NOTRACHED*/
        }
    }

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_Inquiry end: %d\n"), ret);
  return(ret);
}

/*
 * Read the Element Status. If DescriptorSize  != 0 then
 * allocate DescriptorSize * NoOfElements for the result from the
 * Read Element Status command.
 * If DescriptorSize == 0 than try to figure out how much space is needed
 * by
 * 1. do an read with an allocation size of 8
 * 2. from the result take the 'Byte Count of Descriptor Available'
 * 3. do again an Read Element Status with the result from 2.
 *
 */
int
SCSI_ReadElementStatus(
    int		DeviceFD,
    u_char	type,
    u_char	lun,
    u_char	VolTag,
    int		StartAddress,
    size_t	NoOfElements,
    size_t	DescriptorSize,
    u_char **	data)
{
  CDB_T CDB;
  size_t DataBufferLength;
  ElementStatusData_T *ElementStatusData;
  RequestSense_T *pRequestSense;
  int retry = 1;
  int ret = -1;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START SCSI_ReadElementStatus\n"));

  pRequestSense = alloc(SIZEOF(RequestSense_T));

  /*
   * How many elements, if <= 0 than exit with an fatal error
   */
  if (NoOfElements == 0)
    {
      ChgExit("SCSI_ReadElementStatus",_("No of Elements passed are le 0"),FATAL);
      /*NOTREACHED*/
    }

  VolTag = (u_char)((VolTag << 4) & 0x10);
  type = (u_char)(type & 0xf);
  lun = (u_char)((lun << 5) & 0xe0);

  /* if  DescriptorSize == 0
   * try to get the allocation length for the second call
   */
  if (DescriptorSize == 0)
    {
      *data = newalloc(*data, 8);
      memset(*data, 0, 8);

      while (retry > 0 && retry < MAX_RETRIES)
	{
	  memset(pRequestSense, 0, SIZEOF(RequestSense_T) );

	  CDB[0] = SC_COM_RES;          /* READ ELEMENT STATUS */
	  CDB[1] = (u_char)(VolTag | type | lun); /* Element Type Code , VolTag, LUN */
	  MSB2(&CDB[2], StartAddress);  /* Starting Element Address */
	  MSB2(&CDB[4], NoOfElements);  /* Number Of Element */
	  CDB[6] = 0;                             /* Reserved */
	  MSB3(&CDB[7],8);                   /* Allocation Length */
	  CDB[10] = 0;                           /* Reserved */
	  CDB[11] = 0;                           /* Control */

	  ret = SCSI_Run(DeviceFD, Input, CDB, 12,
			 *data, 8,
			 pRequestSense, SIZEOF(RequestSense_T));

	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ReadElementStatus : (1) SCSI_Run %d\n"), ret);
	  if (ret < 0)
	    {
	      DecodeSense(pRequestSense, "SCSI_ReadElementStatus :",debug_file);
	      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_ReadElementStatus (%d)\n"),ret);
	      return(ret);
	      /*NOTRACHED*/
	    }
	  if ( ret > 0)
	    {
	      switch(SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
		{
		case SENSE_IGNORE:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : SENSE_IGNORE\n"));
		  retry = 0;
		  break;
		case SENSE_RETRY:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : SENSE_RETRY no %d\n"), retry);
		  sleep(2);
		  break;
		default:
		  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : end %d\n"), pRequestSense->SenseKey);
		  return(pRequestSense->SenseKey);
		  /*NOTREACHED*/
		}
	    }
	  retry++;
	  if (ret == 0)
	    {
	      retry=0;
	    }
	}
      if (retry > 0)
	{
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_ReadElementStatus (%d)\n"),ret);
	  return(ret);
	  /*NOTRACHED*/
	}

      ElementStatusData = (ElementStatusData_T *)*data;
      DataBufferLength = V3(ElementStatusData->count);

      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ReadElementStatus: DataBufferLength %X, ret %d\n"),DataBufferLength, ret);

      dump_hex(*data, 8, DEBUG_INFO, SECTION_ELEMENT);
    } else { /* DescriptorSize != 0 */
      DataBufferLength = NoOfElements * DescriptorSize;
    }

  DataBufferLength = DataBufferLength + 8;
  *data = newalloc(*data, DataBufferLength);
  memset(*data, 0, DataBufferLength);
  retry = 1;

  while (retry > 0 && retry < MAX_RETRIES)
    {
      memset(pRequestSense, 0, SIZEOF(RequestSense_T) );

      CDB[0] = SC_COM_RES;           /* READ ELEMENT STATUS */
      CDB[1] = (u_char)(VolTag | type | lun);  /* Element Type Code, VolTag, LUN */
      MSB2(&CDB[2], StartAddress);   /* Starting Element Address */
      MSB2(&CDB[4], NoOfElements);   /* Number Of Element */
      CDB[6] = 0;                              /* Reserved */
      MSB3(&CDB[7],DataBufferLength);  /* Allocation Length */
      CDB[10] = 0;                                 /* Reserved */
      CDB[11] = 0;                                 /* Control */

      ret = SCSI_Run(DeviceFD, Input, CDB, 12,
                                *data, DataBufferLength,
                                pRequestSense, SIZEOF(RequestSense_T));


      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ReadElementStatus : (2) SCSI_Run %d\n"), ret);
      if (ret < 0)
        {
          DecodeSense(pRequestSense, _("SCSI_ReadElementStatus :"),debug_file);
	  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_ReadElementStatus (%d)\n"),ret);
          return(ret);
	  /*NOTRACHED*/
        }
      if ( ret > 0)
        {
          switch(SenseHandler(DeviceFD, 0, pRequestSense->SenseKey, pRequestSense->AdditionalSenseCode, pRequestSense->AdditionalSenseCodeQualifier, pRequestSense))
            {
            case SENSE_IGNORE:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : SENSE_IGNORE\n"));
              retry = 0;
              break;
            case SENSE_RETRY:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : SENSE_RETRY no %d\n"), retry);
              sleep(2);
              break;
            default:
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_ModeSense : end %d\n"), pRequestSense->SenseKey);
              return(pRequestSense->SenseKey);
              /*NOTREACHED*/
            }
        }
      retry++;
      if (ret == 0)
        {
          retry=0;
        }
    }

  if (retry > 0)
    {
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_ReadElementStatus (%d)\n"),ret);
      return(ret);
      /*NOTRACHED*/
    }

  dump_hex(*data, DataBufferLength, DEBUG_INFO, SECTION_SCSI);
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_ReadElementStatus (%d)\n"),ret);
  return(ret);
}

printf_arglist_function2(void DebugPrint, int, level, int, section, char *, fmt)
{
  va_list argp;
  char buf[1024];
  int dlevel;
  int dsection = -1;
  time_t ti = time(NULL);

  if (changer->debuglevel)
    {
      if (sscanf(changer->debuglevel,"%d:%d", &dlevel, &dsection) != 2) {
      	dbprintf(_("Parse error: line is '%s' expected [0-9]*:[0-9]*\n"),
		  changer->debuglevel);
        dlevel=1;
        dsection=1;
      }
    } else {
      dlevel=1;
      dsection=1;
    }

  arglist_start(argp, fmt);
  g_vsnprintf(buf, SIZEOF(buf), fmt, argp);
  if (dlevel >= level)
    {
      if (section == dsection || dsection == 0)
	{
	  if (strchr(buf, '\n') != NULL && strlen(buf) > 1)
          {
	     dbprintf(_("%ld:%s"), (long)ti, buf);
	  } else {
	     dbprintf("%s", buf);
	  }
	}
    }
  arglist_end(argp);
}
