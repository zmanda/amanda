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
 * $Id: scsi-hpux_new.c,v 1.19 2006/05/25 01:47:08 johnfranks Exp $
 *
 * Interface to execute SCSI commands on an HP-UX Workstation
 *
 * Copyright (c) Thomas Hepper th@ant.han.de
 */


#include "amanda.h"

#include <sys/scsi.h>
#include <sys/mtio.h>

#include <scsi-defs.h>

void SCSI_OS_Version()
{
#ifndef lint
    static char rcsid[] = "$Id: scsi-hpux_new.c,v 1.19 2006/05/25 01:47:08 johnfranks Exp $";
   DebugPrint(DEBUG_INFO, SECTION_INFO, "scsi-os-layer: %s\n",rcsid);
#endif
}

int SCSI_OpenDevice(int ip)
{
  extern OpenFiles_T *pDev;
  int DeviceFD;
  int i;

  if (pDev[ip].inqdone == 0)
    {
      pDev[ip].inqdone = 1;
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR| O_NDELAY)) >= 0)
        {
          pDev[ip].avail = 1;
          pDev[ip].fd = DeviceFD;
          pDev[ip].devopen = 1;
          pDev[ip].SCSI = 0;
          pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
          
          if (SCSI_Inquiry(ip, pDev[ip].inquiry, INQUIRY_SIZE) == 0)
            {
              if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER)
                {
                  for (i=0;i < 16;i++)
                    pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];
                  for (i=15; i >= 0 && !isalnum(pDev[ip].inquiry->prod_ident[i]) ; i--)
                    {
                      pDev[ip].inquiry->prod_ident[i] = '\0';
                    }
                  pDev[ip].SCSI = 1;

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
                } else {
                  close(DeviceFD);
                  free(pDev[ip].inquiry);
                  return(0);
              }
            }
            close(DeviceFD);
            free(pDev[ip].inquiry);
            pDev[ip].inquiry = NULL;
            return(1);
        }
    } else {
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR| O_NDELAY)) >= 0)
        {
          pDev[ip].fd = DeviceFD;
          pDev[ip].devopen = 1;
          return(1);
        }
    }

  return(0); 
}

int SCSI_CloseDevice(int DeviceFD)
{
  extern OpenFiles_T *pDev;
  int ret;

  ret = close(pDev[DeviceFD].fd);
  pDev[DeviceFD].devopen = 0;
  return(ret);
}

int SCSI_ExecuteCommand(int DeviceFD,
                        Direction_T Direction,
                        CDB_T CDB,
                        size_t CDB_Length,
                        void *DataBuffer,
                        size_t DataBufferLength,
                        RequestSense_T *RequestSenseBuf,
                        size_t RequestSenseLength)
{
  extern OpenFiles_T *pDev;
  struct sctl_io sctl_io;
  int Retries = 3;
  int Zero = 0, Result;
  
  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  /* Clear buffer for cases where sense is not returned */
  memset(RequestSenseBuf, 0, RequestSenseLength);

  if (pDev[DeviceFD].avail == 0)
    {
      return(SCSI_ERROR);
    }


  memset(&sctl_io, '\0', SIZEOF(struct sctl_io));

  sctl_io.flags = 0;  
  sctl_io.max_msecs = 240000;
  /* Set the cmd */
  memcpy(sctl_io.cdb, CDB, CDB_Length);
  sctl_io.cdb_length = CDB_Length;
  /* Data buffer for results */
  sctl_io.data = DataBuffer;
  sctl_io.data_length = (unsigned)DataBufferLength;

  switch (Direction) 
    {
    case Input:
      sctl_io.flags = sctl_io.flags | SCTL_READ;
      break;
    case Output:
      break;
    }

  while (--Retries > 0) {

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

    DecodeSCSI(CDB, "SCSI_ExecuteCommand : ");
    Result = ioctl(pDev[DeviceFD].fd, SIOC_IO, &sctl_io);
    SCSI_CloseDevice(DeviceFD);
    if (Result < 0)
      {
        return(SCSI_ERROR);
      }
    
    SCSI_CloseDevice(DeviceFD);

    memcpy(RequestSenseBuf, sctl_io.sense, RequestSenseLength);
    
    switch(sctl_io.cdb_status)
      {
      case S_GOOD:
        return(SCSI_OK);

      case S_CHECK_CONDITION:
        return(SCSI_CHECK);

      default:
        return(SCSI_ERROR);
      }
  }
  return(SCSI_ERROR);
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
      dbprintf(_("Tape_Ioctl error ioctl %s\n"), strerror(errno));
      SCSI_CloseDevice(DeviceFD);
      return(-1);
    }

  SCSI_CloseDevice(DeviceFD);
  return(ret);  
}



int Tape_Status( int DeviceFD)
{
  extern OpenFiles_T *pDev;
  struct mtget mtget;
  int ret = 0;

  if (pDev[DeviceFD].devopen == 0)
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);

  if (ioctl(pDev[DeviceFD].fd, MTIOCGET, &mtget) != 0)
  {
     dbprintf(_("Tape_Status error ioctl %s\n"), strerror(errno));
     SCSI_CloseDevice(DeviceFD);
     return(-1);
  }

  dbprintf(_("ioctl -> mtget.mt_gstat %X\n"),mtget.mt_gstat);
  if (GMT_ONLINE(mtget.mt_gstat))
  {
    ret = TAPE_ONLINE;
  }

  if (GMT_BOT(mtget.mt_gstat))
  {
    ret = ret | TAPE_BOT;
  }

  if (GMT_EOT(mtget.mt_gstat))
  {
    ret = ret | TAPE_EOT;
  }

  if (GMT_WR_PROT(mtget.mt_gstat))
  {
    ret = ret | TAPE_WR_PROT;
  }

  SCSI_CloseDevice(DeviceFD);
  return(ret); 
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
