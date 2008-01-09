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
 * $Id: scsi-solaris.c,v 1.26 2006/05/25 01:47:10 johnfranks Exp $
 *
 * Interface to execute SCSI commands on an Sun Workstation
 *
 * Copyright (c) Thomas Hepper th@ant.han.de
 */


#include "amanda.h"

#include <sys/scsi/impl/uscsi.h>

#include <scsi-defs.h>
#include <sys/mtio.h>

void SCSI_OS_Version(void)
{
#ifndef lint
   static char rcsid[] = "$Id: scsi-solaris.c,v 1.26 2006/05/25 01:47:10 johnfranks Exp $";
   DebugPrint(DEBUG_INFO, SECTION_INFO, "scsi-os-layer: %s\n",rcsid);
#endif
}

int SCSI_OpenDevice(int ip)
{
  int DeviceFD;
  int i;
  extern OpenFiles_T *pDev;

  if (pDev[ip].inqdone == 0)
    {
      pDev[ip].inqdone = 1;
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR| O_NONBLOCK)) >= 0)
        {
          pDev[ip].avail = 1;
          pDev[ip].fd = DeviceFD;
          pDev[ip].SCSI = 0;
          pDev[ip].devopen = 1;
          pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
          
          if (SCSI_Inquiry(ip, pDev[ip].inquiry, (unsigned char)INQUIRY_SIZE) == 0)
            {
              if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER)
                {
                  for (i=0;i < 16;i++)
                    pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];
                  for (i=15; i >= 0 && !isalnum((int)pDev[ip].ident[i]) ; i--)
                    {
                      pDev[ip].ident[i] = '\0';
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
            free(pDev[ip].inquiry);
            pDev[ip].inquiry = NULL;
            return(1);
        } else {
          dbprintf(_("SCSI_OpenDevice %s failed\n"), pDev[ip].dev);
          return(0);
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
  int ret;
  extern OpenFiles_T *pDev;

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
                        RequestSense_T *RequestSense,
                        size_t RequestSenseLength)
{
  extern OpenFiles_T *pDev;
  extern FILE * debug_file;
  int ret = 0;
  int retries = 1;
  struct uscsi_cmd Command;
  static int depth = 0;

  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  /* Clear buffer for cases where sense is not returned */
  memset(RequestSense, 0, RequestSenseLength);

  if (pDev[DeviceFD].avail == 0)
    {
      return(SCSI_ERROR);
    }
  
  if (depth++ > 2)
  {
     --depth;
     SCSI_CloseDevice(DeviceFD);
     return SCSI_ERROR;
  }
  memset(&Command, 0, SIZEOF(struct uscsi_cmd));
  memset(RequestSense, 0, RequestSenseLength);
  switch (Direction)
    {
    case Input:
      if (DataBufferLength > 0)
        memset(DataBuffer, 0, DataBufferLength);

      /* Command.uscsi_flags =  USCSI_READ | USCSI_RQENABLE;    */
      Command.uscsi_flags = USCSI_DIAGNOSE | USCSI_ISOLATE
        | USCSI_READ | USCSI_RQENABLE;
      break;
    case Output:
      /* Command.uscsi_flags =  USCSI_WRITE | USCSI_RQENABLE;   */
      Command.uscsi_flags = USCSI_DIAGNOSE | USCSI_ISOLATE
        | USCSI_WRITE | USCSI_RQENABLE;
      break;
    }
  /* Set timeout to 5 minutes. */
  Command.uscsi_timeout = 300;
  Command.uscsi_cdb = (caddr_t) CDB;
  Command.uscsi_cdblen = (u_char)CDB_Length;

  if (DataBufferLength > 0)
    {  
      Command.uscsi_bufaddr = DataBuffer;
      Command.uscsi_buflen = DataBufferLength;
    } else {
/*
 * If there is no data buffer force the direction to write, read with
 * a null buffer will fail (errno 22)
 */
      Command.uscsi_flags = USCSI_DIAGNOSE | USCSI_ISOLATE
        | USCSI_WRITE | USCSI_RQENABLE;
   }

  Command.uscsi_rqbuf = (caddr_t)RequestSense;
  Command.uscsi_rqlen = (u_char)RequestSenseLength;
  DecodeSCSI(CDB, "SCSI_ExecuteCommand : ");
  while (retries > 0)
  {
    if (pDev[DeviceFD].devopen == 0)
      if (SCSI_OpenDevice(DeviceFD) == 0)
        {
	  sleep(1);
	  continue;
        }

    if ((ret = ioctl(pDev[DeviceFD].fd, USCSICMD, &Command)) >= 0)
    {
      ret = Command.uscsi_status;
      break;
    }
    dbprintf(_("ioctl on %d failed, errno %s, ret %d\n"),
	      pDev[DeviceFD].fd, strerror(errno), ret);
#if 0
    RequestSense(DeviceFD, &pExtendedRequestSense, 0);
#endif
    DecodeSense(RequestSense, "SCSI_ExecuteCommand:", debug_file);
    retries--;
  }
  --depth;
  SCSI_CloseDevice(DeviceFD);

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("ioctl ret (%d)\n"),ret);
  return(SCSI_OK);
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
  int ret = -1;

  memset(&mtget, 0, SIZEOF(mtget));
  if (pDev[DeviceFD].devopen == 0)
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
  
  if (ioctl(pDev[DeviceFD].fd , MTIOCGET, &mtget) != 0)
    {
      dbprintf(_("Tape_Status error ioctl %s\n"), strerror(errno));
      SCSI_CloseDevice(DeviceFD);
      return(-1);
    }

  /*
   * I have no idea what is the meaning of the bits in mt_erreg
   * I assume that nothing set is tape loaded
   * 0x2 is no tape online
   */

  DebugPrint(DEBUG_INFO, SECTION_TAPE, _("ioctl result for mt_dsreg (%d)\n"), mtget.mt_dsreg);
  DebugPrint(DEBUG_INFO, SECTION_TAPE, _("ioctl result for mt_erreg (%d)\n"), mtget.mt_erreg);

  if (mtget.mt_erreg == 0)
    {
      ret = ret | TAPE_ONLINE;
    }

  if (mtget.mt_erreg & 0x2)
    {
      ret = ret | TAPE_NOT_LOADED;
    }

  SCSI_CloseDevice(DeviceFD);

  return(ret); 
}

int ScanBus(int print)
{
	(void)print;	/* Quiet unused parameter warning */
	return(-1);
}

/*
 * Local variables:
 * indent-tabs-mode: nil
 * c-file-style: gnu
 * End:
 */
