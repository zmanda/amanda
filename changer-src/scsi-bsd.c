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
 * $Id: scsi-bsd.c,v 1.18 2006/05/25 01:47:07 johnfranks Exp $
 *
 * Interface to execute SCSI commands on an BSD System (FreeBSD)
 *
 * Copyright (c) Thomes Hepper th@ant.han.de
 */


#include "amanda.h"

#include <sys/scsiio.h>
#include <sys/mtio.h>

#include <scsi-defs.h>

void SCSI_OS_Version()
{
#ifndef lint
   static char rcsid[] = "$Id: scsi-bsd.c,v 1.18 2006/05/25 01:47:07 johnfranks Exp $";
   DebugPrint(DEBUG_INFO, SECTION_INFO, "scsi-os-layer: %s\n",rcsid);
#endif
}


/*
 * Check if the device is already open,
 * if no open it and save it in the list 
 * of open files.
 * 
 * Return:
 * 0  -> device not opened
 * 1  -> sucess , device open
 */
int SCSI_OpenDevice(int ip)
{
  extern OpenFiles_T *pDev;
  int i;
  char * DeviceName;


  /*
   * If the SCSI inquiry was not done lets try to get
   * some infos about the device
   */
  if (pDev[ip].inqdone == 0) {
    pDev[ip].inqdone = 1;                                                     /* Set it to 1, so the inq is done */
    pDev[ip].SCSI = 0;                                                        /* This will only be set if the inquiry works */
    pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
    
    if (( pDev[ip].fd = open(pDev[ip].dev, O_RDWR)) >= 0)                      /* We need the device in read/write mode */
      {
        pDev[ip].devopen = 1;                                                 /* The device is open for use */
        pDev[ip].avail = 1;                                                   /* And it is available, it could be opened */
        if (SCSI_Inquiry(ip, pDev[ip].inquiry, INQUIRY_SIZE) == 0)            /* Lets try to get the result of an SCSI inquiry */
          {
            if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER)  /* If it worked and we got an type of */
                                                                                                /* either tape or changer continue */
              {
                for (i=0;i < 16 ;i++)                                         /* Copy the product ident to the pDev struct */
                  pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];
                for (i=15; i >= 0 && !isalnum(pDev[ip].ident[i]); i--)        /* And terminate it with an \0, remove all white space */
                  {
                    pDev[ip].ident[i] = '\0';
                  }
                pDev[ip].SCSI = 1;                                            /* OK, its an SCSI device ... */

		  if (pDev[ip].inquiry->type == TYPE_TAPE)
		  {
		          pDev[ip].type = stralloc("tape");
		  }

		  if (pDev[ip].inquiry->type == TYPE_CHANGER)
		  {
		          pDev[ip].type = stralloc("changer");
		  }

                PrintInquiry(pDev[ip].inquiry);                               /* Some debug output */
                return(1);                                                    /* All done */
              } else {
                free(pDev[ip].inquiry);                                       /* The SCSI was ok, but not an TAPE/CHANGER device ... */
                pDev[ip].devopen = 0;
                pDev[ip].avail = 0;
                close(pDev[ip].fd); 
                return(0);                                                    /*Might be an ChgExit is better */
              }
          } else { /* if SCSI_Inquiry */                                      /* The inquiry failed */
            free(pDev[ip].inquiry);                                           /* free the allocated memory */
            pDev[ip].inquiry = NULL;
            return(1);                                                        /* Its not an SCSI device, but can be used for read/write */
          }
      }  /* open() */
    return(0);                                                                /* Open failed .... */
  } else { /* pDev[ip].inqdone */                                             /* OK this is the way we go if the device */
    if (( pDev[ip].fd = open(pDev[ip].dev, O_RDWR)) >= 0)                      /* was opened successfull before */
      {
        pDev[ip].devopen = 1;
        return(1);
      } 
  }
  return(0);                                                                 /* Default, return device not available */
}

/*
 * Close the device 
 * abd set the flags in the device struct 
 *
 */
int SCSI_CloseDevice(int DeviceFD)
{
  int ret;
  extern OpenFiles_T *pDev;
  
  ret = close(pDev[DeviceFD].fd) ;
  pDev[DeviceFD].devopen = 0;
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
  scsireq_t ds;
  int Zero = 0, Result;
  int retries = 5;
  extern int errno;
  
  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  /* Clear buffer for cases where sense is not returned */
  memset(pRequestSense, 0, RequestSenseLength);

  if (pDev[DeviceFD].avail == 0)
    {
      return(SCSI_ERROR);
    }

  memset(&ds, 0, SIZEOF(scsireq_t));
  memset(pRequestSense, 0, RequestSenseLength);
  memset(&ExtendedRequestSense, 0 , SIZEOF(ExtendedRequestSense_T)); 
  
  ds.flags = SCCMD_ESCAPE; 
  /* Timeout */
  ds.timeout = 120000;
  /* Set the cmd */
  memcpy(ds.cmd, CDB, CDB_Length);
  ds.cmdlen = CDB_Length;
  /* Data buffer for results */
  if (DataBufferLength > 0)
    {
      ds.databuf = (caddr_t)DataBuffer;
      ds.datalen = DataBufferLength;
    }
  /* Sense Buffer */
  /*
    ds.sense = (u_char)pRequestSense;
  */
  ds.senselen = RequestSenseLength;
    
  switch (Direction) 
    {
    case Input:
      ds.flags = ds.flags | SCCMD_READ;
      break;
    case Output:
      ds.flags = ds.flags | SCCMD_WRITE;
      break;
    }
    
  while (--retries > 0) {
    
    if (pDev[DeviceFD].devopen == 0)
        if (SCSI_OpenDevice(DeviceFD) == 0)
            return(SCSI_ERROR);
    Result = ioctl(pDev[DeviceFD].fd, SCIOCCOMMAND, &ds);
    SCSI_CloseDevice(DeviceFD);
   
    memcpy(pRequestSense, ds.sense, RequestSenseLength);
    if (Result < 0)
      {
        dbprintf("errno : %s\n",strerror(errno));
        return (SCSI_ERROR);
      }
    dbprintf("SCSI_ExecuteCommand(BSD) %02X STATUS(%02X) \n", CDB[0], ds.retsts);
    switch (ds.retsts)
      {
      case SCCMD_BUSY:                /*  BUSY */
        break;

      case SCCMD_OK:                /*  GOOD */
        return(SCSI_OK);

      case SCCMD_SENSE:               /*  CHECK CONDITION */
        return(SCSI_SENSE);

      default:
        continue;
      }
  }   
  return(SCSI_SENSE);
}

/*
 * Send the command to the device with the
 * ioctl interface
 */
int Tape_Ioctl( int DeviceFD, int command)
{
  extern OpenFiles_T *pDev;
  struct mtop mtop;
  int ret = 0;

  if (pDev[DeviceFD].devopen == 0)
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);

  switch (command)
    {
    case IOCTL_EJECT:
      mtop.mt_op = MTOFFL;
      mtop.mt_count = 1;
      break;
    default:
      break;
    }

  if (ioctl(pDev[DeviceFD].fd , MTIOCTOP, &mtop) != 0)
    {
      dbprintf(_("Tape_Ioctl error ioctl %s\n"),strerror(errno));
      SCSI_CloseDevice(DeviceFD);
      return(-1);
    }

  SCSI_CloseDevice(DeviceFD);
  return(ret);  
}

int Tape_Status( int DeviceFD)
{
/* 
  Not yet
*/
  return(-1);
}

int ScanBus(int print)
{
/*
  Not yet
*/
  return(-1);
}

/*
 * Local variables:
 * indent-tabs-mode: nil
 * c-file-style: gnu
 * End:
 */
