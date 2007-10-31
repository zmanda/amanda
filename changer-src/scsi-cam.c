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
 * $Id: scsi-cam.c,v 1.15 2006/05/25 01:47:07 johnfranks Exp $
 *
 * Interface to execute SCSI commands on an system with cam support
 * Current support is for FreeBSD 4.x
 *
 * Copyright (c) Thomes Hepper th@ant.han.de
 */


#include "amanda.h"

#ifdef HAVE_CAMLIB_H
# include <camlib.h>
#endif

#include <cam/scsi/scsi_message.h>

#ifdef HAVE_SYS_MTIO_H
#include <sys/mtio.h>
#endif

#include <scsi-defs.h>

extern OpenFiles_T *pChangerDev;
extern OpenFiles_T *pTapeDev;
extern OpenFiles_T *pTapeDevCtl;
extern FILE *debug_file;


void SCSI_OS_Version()
{
#ifndef lint
   static char rcsid[] = "$Id: scsi-cam.c,v 1.15 2006/05/25 01:47:07 johnfranks Exp $";
   DebugPrint(DEBUG_INFO, SECTION_INFO, "scsi-os-layer: %s\n",rcsid);
#endif
}

/* parse string of format 1:2:3 and fill in path, target, lun
   returns 0  if it doesn't look like btl
   returns 1  if parse successful
   calls ChgExit if it looks like btl but formatted improperly
*/

int parse_btl(char *DeviceName, 
          path_id_t *path, target_id_t *target, lun_id_t *lun) 
{
  char *p;
  if (strstr(DeviceName, ":") == NULL) 
    return 0;

  p = strtok(DeviceName, ":");
  if (sscanf(p,"%d", path) != 1) {
      free(DeviceName);
      ChgExit("SCSI_OpenDevice",
	_("Path conversion error. Digits expected"), FATAL);
  }
          
  if ((p = strtok(NULL,":")) == NULL) {
      free(DeviceName);
      ChgExit("SCSI_OpenDevice", _("target in Device Name not found"), FATAL);
  }

  if (sscanf(p,"%d", target) != 1) {
      free(DeviceName);
      ChgExit("SCSI_OpenDevice",
	_("Target conversion error. Digits expected"), FATAL);
  }

  if ((p = strtok(NULL,":")) == NULL) {
      free(DeviceName);
      ChgExit("SCSI_OpenDevice", _("lun in Device Name not found"), FATAL);
  }
  if (sscanf(p,"%d", lun) != 1) {
      free(DeviceName);
      ChgExit("SCSI_OpenDevice",
	_("LUN conversion error. Digits expected"), FATAL);
  }

  return 1;
}

/*
 * Check if the device is already open,
 * if no open it and save it in the list 
 * of open files.
 * DeviceName can be an device name, /dev/nrsa0 for example
 * or an bus:target:lun path, 0:4:0 for bus 0 target 4 lun 0
 */

int SCSI_OpenDevice(int ip)
{
  extern OpenFiles_T *pDev;
  char *DeviceName;
  int DeviceFD;
  int i;
  path_id_t path;
  target_id_t target;
  lun_id_t lun;

  DeviceName = stralloc(pDev[ip].dev);

  if (pDev[ip].inqdone == 0) {
    pDev[ip].inqdone = 1;
    pDev[ip].SCSI = 0;
    pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
    if (parse_btl(DeviceName, &path, &target, &lun)) 
        pDev[ip].curdev = cam_open_btl(path, target, lun, O_RDWR, NULL);
    else
        pDev[ip].curdev = cam_open_device(DeviceName, O_RDWR);

    free(DeviceName);
    if (pDev[ip].curdev) {
      pDev[ip].avail = 1;
      pDev[ip].SCSI = 1;
      pDev[ip].devopen = 1;
      if (SCSI_Inquiry(ip, pDev[ip].inquiry, INQUIRY_SIZE) == 0) {
        if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER) {
          for (i=0;i < 16;i++)
            pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];

          for (i=15; i >= 0 && !isalnum(pDev[ip].ident[i]); i--) {
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
          free(pDev[ip].inquiry);
          return(0);
        }
      } else {
        pDev[ip].SCSI = 0;
        free(pDev[ip].inquiry);
        pDev[ip].inquiry = NULL;
        return(1);
      }
    } else { /* Device open failed */
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice open failed\n"));
      return(0);
    }
  }
  if (parse_btl(DeviceName, &path, &target, &lun))
      pDev[ip].curdev = cam_open_btl(path, target, lun, O_RDWR, NULL);
  else
      pDev[ip].curdev = cam_open_device(DeviceName, O_RDWR);

  free(DeviceName);

  if (pDev[ip].curdev) {
    pDev[ip].devopen = 1;
    return(1);
  } else  {
    return(0);
  }
}

int SCSI_CloseDevice(int DeviceFD)
{
  int ret;
  extern OpenFiles_T *pDev;

  if (pDev[DeviceFD].SCSI == 1)
    {
      cam_close_device(pDev[DeviceFD].curdev);
      pDev[DeviceFD].devopen = 0;
    } else {
      close(pDev[DeviceFD].fd);
    }
  return(0);
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
  ExtendedRequestSense_T ExtendedRequestSense;
  extern OpenFiles_T *pDev;
  union ccb *ccb;
  int ret;
  guint32 ccb_flags;
  OpenFiles_T *pwork = NULL;

  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  /* Clear buffer for cases where sense is not returned */
  memset(pRequestSense, 0, RequestSenseLength);

  if (pDev[DeviceFD].avail == 0)
    {
      return(SCSI_ERROR);
    }

  /* 
   * CLear the SENSE buffer
   */
  bzero(pRequestSense, RequestSenseLength);

  DecodeSCSI(CDB, "SCSI_ExecuteCommand : ");

  ccb = cam_getccb(pDev[DeviceFD].curdev);

  /* Build the CCB */
  bzero(&(&ccb->ccb_h)[1], SIZEOF(struct ccb_scsiio));
  bcopy(&CDB[0], &ccb->csio.cdb_io.cdb_bytes, CDB_Length);

  switch (Direction)
    {
    case Input:
      if (DataBufferLength == 0)
        {
          ccb_flags = CAM_DIR_NONE;
        } else {
          ccb_flags = CAM_DIR_IN;
        }
      break;
    case Output:
      if (DataBufferLength == 0)
        {
          ccb_flags = CAM_DIR_NONE;
        } else {     
          ccb_flags = CAM_DIR_OUT;
        }
      break;
    default:
      ccb_flags = CAM_DIR_NONE;
      break;
    }
  
  cam_fill_csio(&ccb->csio,
                /* retires */ 1,
                /* cbfncp */ NULL,
                /* flags */ ccb_flags,
                /* tag_action */ MSG_SIMPLE_Q_TAG,
                /* data_ptr */ (guint8*)DataBuffer,
                /* dxfer_len */ DataBufferLength,
                /* sense_len */ SSD_FULL_SIZE,
                /* cdb_len */ CDB_Length,
                /* timeout */ 600 * 1000);
  

  if (pDev[DeviceFD].devopen == 0)
    {
      if (SCSI_OpenDevice(DeviceFD) == 0)
        {
	   cam_freeccb(ccb);
	   return(SCSI_ERROR);
	}
    }
  
  ret = cam_send_ccb(pDev[DeviceFD].curdev, ccb);
  SCSI_CloseDevice(DeviceFD);

  if ( ret == -1)
    {
      cam_freeccb(ccb);
      return(SCSI_ERROR);
    }
  
  /* 
   * copy the SENSE data to the Sense Buffer !!
   */
  memcpy(pRequestSense, &ccb->csio.sense_data, RequestSenseLength);
  
  /* ToDo add error handling */
  if ((ccb->ccb_h.status & CAM_STATUS_MASK) != CAM_REQ_CMP)
    {
      dbprintf(_("SCSI_ExecuteCommand return %d\n"), (ccb->ccb_h.status & CAM_STATUS_MASK));
      return(SCSI_ERROR);
    }

  cam_freeccb(ccb);
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
  int ret = 0;
  
  if (pDev[DeviceFD].devopen == 0)
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);

  if (ioctl(pDev[DeviceFD].fd , MTIOCGET, &mtget) != 0)
  {
     dbprintf(_("Tape_Status error ioctl %s\n"), strerror(errno));
     SCSI_CloseDevice(DeviceFD);
     return(-1);
  }

  dbprintf("ioctl -> mtget.mt_dsreg %lX\n",mtget.mt_dsreg);
  dbprintf("ioctl -> mtget.mt_erreg %lX\n",mtget.mt_erreg);

  /*
   * I have no idea what is the meaning of the bits in mt_erreg
   * I assume that nothing set is tape loaded
   * 0x2 is no tape online
   */
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

/*
 * Scans the bus for device with the type 'tape' or 'robot'
 *
 */
int ScanBus(int print)
{
  int bus,target,lun;
  int count = 0;
  extern OpenFiles_T *pDev;
  
  for (bus = 0;bus < 3; bus++)
    {
      pDev[count].dev = malloc(10);
      for (target = 0;target < 8; target++)
        {
          for (lun = 0; lun < 8; lun++)
            {
              g_sprintf(pDev[count].dev, "%d:%d:%d", bus, target, lun);
              pDev[count].inqdone = 0;
              if (OpenDevice(count, pDev[count].dev, "Scan", NULL))
                {
                  if (pDev[count].inquiry->type == TYPE_TAPE ||
                      pDev[count].inquiry->type == TYPE_CHANGER)
                    {
                      count++;
                      pDev[count].dev = malloc(10);
                    } else {
                      if (print)
                        {
                          g_printf(_("bus:target:lun -> %s == "),pDev[count].dev);
                          
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
                    } 
                }
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
