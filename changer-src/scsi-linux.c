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
 * $Id: scsi-linux.c,v 1.30 2006/07/06 11:57:28 martinea Exp $
 *
 * Interface to execute SCSI commands on Linux
 *
 * Copyright (c) Thomas Hepper th@ant.han.de
 */


#include "amanda.h"

#ifdef HAVE_SCSI_SG_H
#include <scsi/sg.h>
#define LINUX_SG
#endif

#ifdef HAVE_SYS_MTIO_H
#include <sys/mtio.h>
#endif

#include <scsi-defs.h>

extern OpenFiles_T *pDev;

void SCSI_OS_Version(void)
{
#ifndef lint
   static char rcsid[] = "$Id: scsi-linux.c,v 1.30 2006/07/06 11:57:28 martinea Exp $";
   DebugPrint(DEBUG_ERROR, SECTION_INFO, "scsi-os-layer: %s\n",rcsid);
#endif
}

int SCSI_CloseDevice(int DeviceFD)
{
  int ret = 0;
  
  if (pDev[DeviceFD].devopen == 1)
    {
      pDev[DeviceFD].devopen = 0;
      ret = close(pDev[DeviceFD].fd);
    }

  return(ret);
}

/* Open a device to talk to an scsi device, either per ioctl, or
 * direct writing....
 * Return:
 * 0 -> error
 * 1 -> OK
 *
 * TODO:
 * Define some readable defs for the falgs which can be set (like in the AIX dreiver)
 *
 */
#ifdef LINUX_SG
int SCSI_OpenDevice(int ip)
{
  int DeviceFD;
  int i;
  int timeout;
  struct stat pstat;
  char *buffer = NULL ;           /* Will contain the device name after checking */
  int openmode = O_RDONLY;

  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### START SCSI_OpenDevice\n"));
  if (pDev[ip].inqdone == 0)
    {
      pDev[ip].inqdone = 1;
      if (strncmp("/dev/sg", pDev[ip].dev, 7) != 0) /* Check if no sg device for an link .... */
        {
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : checking if %s is a sg device\n"), pDev[ip].dev);
          if (lstat(pDev[ip].dev, &pstat) != -1)
            {
              if (S_ISLNK(pstat.st_mode) == 1)
                {
                  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : is a link, checking destination\n"));
                  if ((buffer = (char *)malloc(513)) == NULL)
                    {
                      DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("SCSI_OpenDevice : malloc failed\n"));
                      return(0);
                    }
                  memset(buffer, 0, 513);
                  if (( i = readlink(pDev[ip].dev, buffer, 512)) == -1)
                    {
                      if (errno == ENAMETOOLONG )
                        {
                        } else {
                          pDev[ip].SCSI = 0;
                        }
                    }
                  if ( i >= 7)
                    {
                      if (strncmp("/dev/sg", buffer, 7) == 0)
                        {
                          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : link points to %s\n"), buffer) ;
                          pDev[ip].flags = 1;
                        }
                    }
                } else {/* S_ISLNK(pstat.st_mode) == 1 */
                  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("No link %s\n"), pDev[ip].dev) ;
                  buffer = stralloc(pDev[ip].dev);
                }
            } else {/* lstat(DeviceName, &pstat) != -1 */ 
              DebugPrint(DEBUG_ERROR, SECTION_SCSI,_("can't stat device %s\n"), pDev[ip].dev);
              return(0);
            }
        } else {
          buffer = stralloc(pDev[ip].dev);
          pDev[ip].flags = 1;
        }
      
      if (pDev[ip].flags == 1)
        {
          openmode = O_RDWR;
        }
      
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("Try to open %s\n"), buffer);
      if ((DeviceFD = open(buffer, openmode)) >= 0)
        {
          pDev[ip].avail = 1;
          pDev[ip].devopen = 1;
          pDev[ip].fd = DeviceFD;
        } else {
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice open failed\n"));
	  amfree(buffer);
          return(0);
        }
      
      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("done\n"));
      if ( pDev[ip].flags == 1)
        {
          pDev[ip].SCSI = 1;
        }
      
      pDev[ip].dev = buffer;
      if (pDev[ip].SCSI == 1)
        {
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : use SG interface\n"));
          if ((timeout = ioctl(pDev[ip].fd, SG_GET_TIMEOUT)) > 0) 
            {
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : current timeout %d\n"), timeout);
              timeout = 60000;
              if (ioctl(pDev[ip].fd, SG_SET_TIMEOUT, &timeout) == 0)
                {
                  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : timeout set to %d\n"), timeout);
                }
            }
          pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
          if (SCSI_Inquiry(ip, pDev[ip].inquiry, (u_char)INQUIRY_SIZE) == 0)
            {
              if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER)
                {
                  for (i=0;i < 16;i++)
                    pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];
                  for (i=15; i >= 0 && !isalnum(pDev[ip].ident[i]); i--)
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
                  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice (1)\n"));
                  return(1);
                } else {
                  close(DeviceFD);
                  amfree(pDev[ip].inquiry);
                  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice (0)\n"));
                  return(0);
                }
            } else {
              pDev[ip].SCSI = 0;
              pDev[ip].devopen = 0;
              close(DeviceFD);
              amfree(pDev[ip].inquiry);
              pDev[ip].inquiry = NULL;
              DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice (1)\n"));
              return(1);
            }
        } else /* if (pDev[ip].SCSI == 1) */ {  
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("Device not capable for SCSI commands\n"));
          pDev[ip].SCSI = 0;
          pDev[ip].devopen = 0;
          close(DeviceFD);
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice (1)\n"));
          return(1);
        }
    } else { /* if (pDev[ip].inqdone == 0) */
      if (pDev[ip].flags == 1)
        {
          openmode = O_RDWR;
        } else {
          openmode = O_RDONLY;
        }
      if ((DeviceFD = open(pDev[ip].dev, openmode)) >= 0)
        {
          pDev[ip].devopen = 1;
          pDev[ip].fd = DeviceFD;
          if (pDev[ip].flags == 1)
            {
              if ((timeout = ioctl(pDev[ip].fd, SG_GET_TIMEOUT)) > 0) 
                {
                  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : current timeout %d\n"), timeout);
                  timeout = 60000;
                  if (ioctl(pDev[ip].fd, SG_SET_TIMEOUT, &timeout) == 0)
                    {
                      DebugPrint(DEBUG_INFO, SECTION_SCSI,_("SCSI_OpenDevice : timeout set to %d\n"), timeout);
                    }
                }
            }
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice (1)\n"));
          return(1);
        } else {
          DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice open failed\n"));
          return(0);
        }
    }
  DebugPrint(DEBUG_INFO, SECTION_SCSI,_("##### STOP SCSI_OpenDevice should not happen !!\n"));
  return(0);
}

#define SCSI_OFF SIZEOF(struct sg_header)
int SCSI_ExecuteCommand(int DeviceFD,
                        Direction_T Direction,
                        CDB_T CDB,
                        size_t CDB_Length,
                        void *DataBuffer,
                        size_t DataBufferLength,
                        RequestSense_T *pRequestSense,
                        size_t RequestSenseLength)
{
  struct sg_header *psg_header;
  char *buffer;
  size_t osize = 0;
  ssize_t status;

  /* Basic sanity checks */
  assert(CDB_Length <= UCHAR_MAX);
  assert(RequestSenseLength <= UCHAR_MAX);

  /* Clear buffer for cases where sense is not returned */
  memset(pRequestSense, 0, RequestSenseLength);

  if (pDev[DeviceFD].avail == 0)
    {
      return(-1);
    }

  if (pDev[DeviceFD].devopen == 0)
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
  
  if (SCSI_OFF + CDB_Length + DataBufferLength > 4096) 
    {
      SCSI_CloseDevice(DeviceFD);
      return(-1);
    }

  buffer = (char *)malloc(SCSI_OFF + CDB_Length + DataBufferLength);
  if (buffer == NULL)
    {
      dbprintf(_("SCSI_ExecuteCommand memory allocation failure.\n"));
      SCSI_CloseDevice(DeviceFD);
      return(-1);
    }
  memset(buffer, 0, SCSI_OFF + CDB_Length + DataBufferLength);
  memcpy(buffer + SCSI_OFF, CDB, CDB_Length);
  
  psg_header = (struct sg_header *)buffer;
  if (CDB_Length >= 12)
    {
      psg_header->twelve_byte = 1;
    } else {
      psg_header->twelve_byte = 0;
    }
  psg_header->result = 0;
  psg_header->reply_len = (int)(SCSI_OFF + DataBufferLength);
  
  switch (Direction)
    {
    case Input:
      osize = 0;
      break;
    case Output:
      osize = DataBufferLength;
      break;
    }
  
  DecodeSCSI(CDB, "SCSI_ExecuteCommand : ");
  
  status = write(pDev[DeviceFD].fd, buffer, SCSI_OFF + CDB_Length + osize);
  if ( (status < (ssize_t)0) ||
       (status != (ssize_t)(SCSI_OFF + CDB_Length + osize)) ||
       (psg_header->result != 0)) 
    {
      dbprintf(_("SCSI_ExecuteCommand error send \n"));
      SCSI_CloseDevice(DeviceFD);
      amfree(buffer);
      return(SCSI_ERROR);
    }
  
  memset(buffer, 0, SCSI_OFF + DataBufferLength);
  status = read(pDev[DeviceFD].fd, buffer, SCSI_OFF + DataBufferLength);
  memset(pRequestSense, 0, RequestSenseLength);
  memcpy(pRequestSense, psg_header->sense_buffer, 16);
  
  if ( (status < 0) ||
       (status != (ssize_t)(SCSI_OFF + DataBufferLength)) || 
       (psg_header->result != 0)) 
    { 
      dbprintf(_("SCSI_ExecuteCommand error read \n"));
      dbprintf(_("Status %zd (%zd) %2X\n"), status, SCSI_OFF + DataBufferLength,psg_header->result );
      SCSI_CloseDevice(DeviceFD);
      amfree(buffer);
      return(SCSI_ERROR);
    }

  if (DataBufferLength)
    {
       memcpy(DataBuffer, buffer + SCSI_OFF, DataBufferLength);
    }

  SCSI_CloseDevice(DeviceFD);
  amfree(buffer);
  return(SCSI_OK);
}

#else

static inline int min(int x, int y)
{
  return (x < y ? x : y);
}


static inline int max(int x, int y)
{
  return (x > y ? x : y);
}

int SCSI_OpenDevice(int ip)
{
  int DeviceFD;
  int i;

  if (pDev[ip].inqdone == 0)
    {
      pDev[ip].inqdone = 1;
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR)) >= 0)
        {
          pDev[ip].avail = 1;
          pDev[ip].fd = DeviceFD;
          pDev[ip].SCSI = 0;
          pDev[ip].inquiry = (SCSIInquiry_T *)malloc(INQUIRY_SIZE);
          dbprintf(_("SCSI_OpenDevice : use ioctl interface\n"));
          if (SCSI_Inquiry(ip, pDev[ip].inquiry, (u_char)INQUIRY_SIZE) == 0)
            {
              if (pDev[ip].inquiry->type == TYPE_TAPE || pDev[ip].inquiry->type == TYPE_CHANGER)
                {
                  for (i=0;i < 16 && pDev[ip].inquiry->prod_ident[i] != ' ';i++)
                    pDev[ip].ident[i] = pDev[ip].inquiry->prod_ident[i];
                  pDev[ip].ident[i] = '\0';
                  pDev[ip].SCSI = 1;
                  PrintInquiry(pDev[ip].inquiry);
                  return(1);
                } else {
                  amfree(pDev[ip].inquiry);
                  close(DeviceFD);
                  return(0);
                }
            } else {
              close(DeviceFD);
              amfree(pDev[ip].inquiry);
              pDev[ip].inquiry = NULL;
              return(1);
            }
        }
      return(1); 
    } else {
      if ((DeviceFD = open(pDev[ip].dev, O_RDWR)) >= 0)
        {
          pDev[ip].fd = DeviceFD;
          pDev[ip].devopen = 1;
          return(1);
        } else {
          pDev[ip].devopen = 0;
          return(0);
        }
    }
}

int SCSI_ExecuteCommand(int DeviceFD,
                        Direction_T Direction,
                        CDB_T CDB,
                        int CDB_Length,
                        void *DataBuffer,
                        int DataBufferLength,
                        RequestSense_T *pRequestSense,
                        int RequestSenseLength)
{
  unsigned char *Command;
  int Zero = 0, Result;
 
  if (pDev[DeviceFD].avail == 0)
    {
      return(SCSI_ERROR);
    }

  if (pDev[DeviceFD].devopen == 0)
    {
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
    }

  memset(pRequestSense, 0, RequestSenseLength);
  switch (Direction)
    {
    case Input:
      Command = (unsigned char *)
        malloc(8 + max(DataBufferLength, RequestSenseLength));
      memcpy(&Command[0], &Zero, 4);
      memcpy(&Command[4], &DataBufferLength, 4);
      memcpy(&Command[8], CDB, CDB_Length);
      break;
    case Output:
      Command = (unsigned char *)
        malloc(8 + max(CDB_Length + DataBufferLength, RequestSenseLength));
      memcpy(&Command[0], &DataBufferLength, 4);
      memcpy(&Command[4], &Zero, 4);
      memcpy(&Command[8], CDB, CDB_Length);
      memcpy(&Command[8 + CDB_Length], DataBuffer, DataBufferLength);
      break;
    }
  
  DecodeSCSI(CDB, "SCSI_ExecuteCommand : ");
  
  Result = ioctl(pDev[DeviceFD].fd, SCSI_IOCTL_SEND_COMMAND, Command);
  if (Result != 0)
    memcpy(pRequestSense, &Command[8], RequestSenseLength);
  else if (Direction == Input)
    memcpy(DataBuffer, &Command[8], DataBufferLength);
  amfree(Command);
  SCSI_CloseDevice(DeviceFD);

  switch(Result)
    {
      case 0:
        return(SCSI_OK);
        break;
    default:
      return(SCSI_SENSE);
      break;
    }
}
#endif

/*
 * Send the command to the device with the
 * ioctl interface
 */
int Tape_Ioctl( int DeviceFD, int command)
{
  struct mtop mtop;
  int ret = 0;

  if (pDev[DeviceFD].devopen == 0)
    {
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
    }

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
  struct mtget mtget;
  int ret = 0;

  memset(&mtget, 0, SIZEOF(mtget));
  if (pDev[DeviceFD].devopen == 0)
    {
      if (SCSI_OpenDevice(DeviceFD) == 0)
          return(-1);
    }

  if (ioctl(pDev[DeviceFD].fd , MTIOCGET, &mtget) != 0)
  {
     DebugPrint(DEBUG_ERROR, SECTION_TAPE,_("Tape_Status error ioctl %s\n"),
		strerror(errno));
     SCSI_CloseDevice(DeviceFD);
     return(-1);
  }

  DebugPrint(DEBUG_INFO, SECTION_TAPE,_("ioctl -> mtget.mt_gstat %lX\n"),mtget.mt_gstat);
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
  
  if (GMT_DR_OPEN(mtget.mt_gstat))
    {
      ret = ret | TAPE_NOT_LOADED;
    }
  
  SCSI_CloseDevice(DeviceFD);
  return(ret); 
}

/*
 * This functions scan all /dev/sg* devices
 * It opens the device an print the result of the inquiry 
 *
 */
int ScanBus(int print)
{
  DIR *dir;
  struct dirent *dirent;
  int count = 0;

  if ((dir = opendir("/dev/")) == NULL)
    {
      dbprintf(_("/dev/ error: %s"), strerror(errno));
      return 0;
    }

  while ((dirent = readdir(dir)) != NULL)
    {
      if (strstr(dirent->d_name, "sg") != NULL)
      {
        pDev[count].dev = malloc(10);
        pDev[count].inqdone = 0;
        g_snprintf(pDev[count].dev, SIZEOF(pDev[count].dev),
	    "/dev/%s", dirent->d_name);
        if (OpenDevice(count,pDev[count].dev, "Scan", NULL ))
          {
            SCSI_CloseDevice(count);
            pDev[count].inqdone = 0;
            
            if (print)
              {
                g_printf(_("name /dev/%s "), dirent->d_name);
                
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
            amfree(pDev[count].dev);
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
