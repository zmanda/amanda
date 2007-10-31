/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * $Id: scsi-irix.c,v 1.23 2006/05/25 01:47:08 johnfranks Exp $
 *
 * Interface to execute SCSI commands on an SGI Workstation
 *
 * Copyright (c) Thomas Hepper th@ant.han.de
 */


#include "amanda.h"

#include <sys/scsi.h>
#include <sys/dsreq.h>
#include <sys/mtio.h>

#include <scsi-defs.h>

void SCSI_OS_Version()
{
#ifndef lint
   static char rcsid[] = "$Id: scsi-irix.c,v 1.23 2006/05/25 01:47:08 johnfranks Exp $";
   DebugPrint(DEBUG_INFO, SECTION_INFO, "scsi-os-layer: %s\n",rcsid);
#endif
}


/*
 */
int SCSI_OpenDevice(int ip)
{
  extern OpenFiles_T *pDev;
  int DeviceFD;
  int i;
  
  if (pDev[ip].inqdone == 0)
    {
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR | O_EXCL)) >= 0)
        {
          pDev[ip].inqdone = 1;          pDev[ip].SCSI = 0;
          pDev[ip].avail = 1;
          pDev[ip].fd = DeviceFD;
          pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
          if (SCSI_Inquiry(ip, pDev[ip].inquiry, INQUIRY_SIZE) == 0)
            {
              if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER)
                {
                  for (i=0;i < 16 ;i++)
                    pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];
                  for (i=15; i >= 0 && !isalnum((int)pDev[ip].ident[i]) ; i--)
                    {
                      pDev[ip].ident[i] = '\0';
                    }
                  pDev[ip].SCSI = 1;
                  close(DeviceFD);

		  if (pDev[ip].inquiry->type == TYPE_TAPE)
		  {
		          pDev[ip].type = stralloc("tape");
		  }

		  if (pDev[ip].inquiry->type == TYPE_CHANGER)
		  {
		          pDev[ip].type = stralloc("changer");
		  }

                  PrintInquiry(pDev[ip].inquiry);
                  return(1);
                } else { /* ! TYPE_TAPE ! TYPE_CHANGER */
                  close(DeviceFD);
                  free(pDev[ip].inquiry);
                  pDev[ip].inquiry = NULL;
                  pDev[ip].avail = 0;
                  return(0);
                }
            }
	    /* inquiry failed or no SCSI communication available */
            close(DeviceFD);
            free(pDev[ip].inquiry);
            pDev[ip].inquiry = NULL;
            pDev[ip].avail = 0;
            return(0);
        }
    } else {
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR | O_EXCL)) >= 0)
        {
          pDev[ip].fd = DeviceFD;
          pDev[ip].devopen = 1;
          return(1);
        } else {
          pDev[ip].devopen = 0;
          return(0);
        }
    }
  return(0); 
}

int SCSI_CloseDevice(int DeviceFD)
{
  extern OpenFiles_T *pDev;
  int ret = 0;
  
  if (pDev[DeviceFD].devopen == 1)
    {
      pDev[DeviceFD].devopen = 0;
      ret = close(pDev[DeviceFD].fd);
    }

  return(ret);
}

int SCSI_ExecuteCommand(int DeviceFD,
                        Direction_T Direction,
                        CDB_T CDB,
                        size_t CDB_Length,
                        void *DataBuffer,
                        size_t DataBufferLength,
                        RequestSense_T *pRequestSense,
                        size_t RequestSenseLength)
{
  extern OpenFiles_T *pDev;
  ExtendedRequestSense_T ExtendedRequestSense;
  struct dsreq ds;
  int Result;
  int retries = 5;
  
  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  /* Clear buffer for cases where sense is not returned */
  memset(pRequestSense, 0, RequestSenseLength);

  if (pDev[DeviceFD].avail == 0)
    {
      return(SCSI_ERROR);
    }
  
  memset(&ds, 0, SIZEOF(struct dsreq));
  memset(pRequestSense, 0, RequestSenseLength);
  memset(&ExtendedRequestSense, 0 , SIZEOF(ExtendedRequestSense_T)); 
  
  ds.ds_flags = DSRQ_SENSE|DSRQ_TRACE|DSRQ_PRINT; 
  /* Timeout */
  ds.ds_time = 120000;
  /* Set the cmd */
  ds.ds_cmdbuf = (caddr_t)CDB;
  ds.ds_cmdlen = CDB_Length;
  /* Data buffer for results */
  ds.ds_databuf = (caddr_t)DataBuffer;
  ds.ds_datalen = DataBufferLength;
  /* Sense Buffer */
  ds.ds_sensebuf = (caddr_t)pRequestSense;
  ds.ds_senselen = RequestSenseLength;
  
  switch (Direction) 
    {
    case Input:
      ds.ds_flags = ds.ds_flags | DSRQ_READ;
      break;
    case Output:
      ds.ds_flags = ds.ds_flags | DSRQ_WRITE;
      break;
    }
  
  while (--retries > 0) {
    if (pDev[DeviceFD].devopen == 0)
      {
        if (SCSI_OpenDevice(DeviceFD) == 0)
          {
            dbprintf(_("SCSI_ExecuteCommand could not open %s: %s\n"),
                      pDev[DeviceFD].dev,
	              strerror(errno));
            sleep(1); /* Give device a little time befor retry */
            continue;
          }
      }
    Result = ioctl(pDev[DeviceFD].fd, DS_ENTER, &ds);
    SCSI_CloseDevice(DeviceFD);

    if (Result < 0)
      {
        RET(&ds) = DSRT_DEVSCSI;
        SCSI_CloseDevice(DeviceFD);
        return (SCSI_ERROR);
      }
    DecodeSCSI(CDB, "SCSI_ExecuteCommand : ");
    dbprintf(_("\t\t\tSTATUS(%02X) RET(%02X)\n"), STATUS(&ds), RET(&ds));
    switch (STATUS(&ds))
      {
      case ST_BUSY:                /*  BUSY */
        break;

      case STA_RESERV:             /*  RESERV CONFLICT */
        if (retries > 0)
          sleep(2);
        continue;

      case ST_GOOD:                /*  GOOD 0x00 */
        switch (RET(&ds))
          {
          case DSRT_SENSE:
            return(SCSI_SENSE);
          }
          return(SCSI_OK);

      case ST_CHECK:               /*  CHECK CONDITION 0x02 */ 
        switch (RET(&ds))
          {
          case DSRT_SENSE:
            return(SCSI_SENSE);
          }
        return(SCSI_CHECK);

      case ST_COND_MET:            /*  INTERM/GOOD 0x10 */
      default:
        continue;
      }
  }     
  return(SCSI_ERROR);
}

int Tape_Ioctl ( int DeviceFD, int command)
{
  extern OpenFiles_T *pDev;
  struct mtop mtop;
  
  if (pDev[DeviceFD].devopen == 0)
    {
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
    }
  
  switch (command)
    {
    case IOCTL_EJECT:
      mtop.mt_op = MTUNLOAD;
      mtop.mt_count = 1;
      break;
    default:
      break;
    }
  
  ioctl(pDev[DeviceFD].fd, MTIOCTOP, &mtop);
  SCSI_CloseDevice(DeviceFD);
  return(0);
}

int Tape_Status( int DeviceFD)
{
  extern OpenFiles_T *pDev;
  struct mtget mtget;
  int ret = 0;

  if (pDev[DeviceFD].devopen == 0)
    {
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
    }

  if (ioctl(pDev[DeviceFD].fd , MTIOCGET, &mtget) != 0)
    {
      dbprintf(_("Tape_Status error ioctl %s\n"),strerror(errno));
      SCSI_CloseDevice(DeviceFD);
      return(-1);
    }
  
  switch(mtget.mt_dposn)
    {
    case MT_EOT:
      ret = ret | TAPE_EOT;
      break;
    case MT_BOT:
      ret = ret | TAPE_BOT;
      break;
    case MT_WPROT:
      ret = ret | TAPE_WR_PROT;
      break;
    case MT_ONL:
      ret = TAPE_ONLINE;
      break;
    case MT_EOD:
      break;
    case MT_FMK:
      break;
    default:
      break;
    }

  SCSI_CloseDevice(DeviceFD);
  return(ret); 
}

int ScanBus(int print)
{
  DIR *dir;
  struct dirent *dirent;
  extern OpenFiles_T *pDev;
  extern int errno;
  int count = 0;

  if ((dir = opendir("/dev/scsi")) == NULL)
    {
      dbprintf(_("Can not read /dev/scsi: %s"), strerror(errno));
      return 0;
    }

  while ((dirent = readdir(dir)) != NULL)
    {
      if (strstr(dirent->d_name, "sc") != NULL)
      {
        pDev[count].dev = malloc(10);
        pDev[count].inqdone = 0;
        g_sprintf(pDev[count].dev,"/dev/scsi/%s", dirent->d_name);
        if (OpenDevice(count,pDev[count].dev, "Scan", NULL ))
          {
            SCSI_CloseDevice(count);
            pDev[count].inqdone = 0;
            
            if (print)
              {
                g_printf(_("name /dev/scsi/%s "), dirent->d_name);
                
                switch (pDev[count].inquiry->type)
                  {
                  case TYPE_DISK:
                    g_printf(_("Disk"));
                    break;
                  case TYPE_TAPE:
                    g_printf(_("Tape"));
                    break;
                  case TYPE_PRINTER:
                    g_printf(_("Printer"));
                    break;
                  case TYPE_PROCESSOR:
                    g_printf(_("Processor"));
                    break;
                  case TYPE_WORM:
                    g_printf(_("Worm"));
                    break;
                  case TYPE_CDROM:
                    g_printf(_("Cdrom"));
                    break;
                  case TYPE_SCANNER:
                    g_printf(_("Scanner"));
                    break;
                  case TYPE_OPTICAL:
                    g_printf(_("Optical"));
                    break;
                  case TYPE_CHANGER:
                    g_printf(_("Changer"));
                    break;
                  case TYPE_COMM:
                    g_printf(_("Comm"));
                    break;
                  default:
                    g_printf(_("unknown %d"),pDev[count].inquiry->type);
                    break;
                  }
                g_printf("\n");
              }
            count++;
	    g_printf(_("Count %d\n"),count);
          } else {
            free(pDev[count].dev);
            pDev[count].dev=NULL;
          }
      }
    }
  return 0;
}

/*
 * Local variables:
 * indent-tabs-mode: nil
 * c-file-style: gnu
 * End:
 */
