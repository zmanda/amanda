/*
 * Copyright (c) 1997, by Sun Microsystems, Inc.
 * All Rights Reserved
 */

/*
 * This file is a product of Sun Microsystems, Inc. and is provided for
 * unrestricted use provided that this legend is included on all tape
 * media and as a part of the software program in whole or part.  Users
 * may copy or modify this file without charge, but are not authorized to
 * license or distribute it to anyone else except as part of a product
 * or program developed by the user.
 *
 * THIS FILE IS PROVIDED AS IS WITH NO WARRANTIES OF ANY KIND INCLUDING THE
 * WARRANTIES OF DESIGN, MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE, OR ARISING FROM A COURSE OF DEALING, USAGE OR TRADE PRACTICE.
 *
 * This file is provided with no support and without any obligation on the
 * part of Sun Microsystems, Inc. to assist in its use, correction,
 * modification or enhancement.
 *
 * SUN MICROSYSTEMS, INC. SHALL HAVE NO LIABILITY WITH RESPECT TO THE
 * INFRINGEMENT OF COPYRIGHTS, TRADE SECRETS OR ANY PATENTS BY THIS FILE
 * OR ANY PART THEREOF.
 *
 * In no event will Sun Microsystems, Inc. be liable for any lost revenue
 * or profits or other special, indirect and consequential damages, even
 * if Sun has been advised of the possibility of such damages.
 *
 * Sun Microsystems, Inc.
 * 2550 Garcia Avenue
 * Mountain View, California  94043
 */

/*
 * Defines and structures used only within the driver, sst.c
 */

#ifndef	_SST_DEF_H
#define	_SST_DEF_H

#pragma ident	"@(#)sst_def.h	1.13	97/10/02 SMI"

#ifdef	__cplusplus
extern "C" {
#endif

#if	defined(_KERNEL) || defined(_KMEMUSER)

#include <sys/note.h>

/*
 * Driver compile options
 */

/*
 * This driver does not reset the SCSI bus. Instead we just give up
 * and complain if the target hangs. If the bus is really stuck, one
 * of the Sun drivers (e.g. sd) will reset it. If you decide that
 * your device is critical and you should in fact reset the bus in
 * this driver, turn on this define below:
#define	SCSI_BUS_RESET
 */

/*
 * Local definitions, for clarity of code
 */
#define	SST_DEVINFO(t)	(((t)->targ_devp)->sd_dev)
#define	SST_MUTEX(t)	(&((t)->targ_devp)->sd_mutex)
#define	ROUTE(t)	(&((t)->targ_devp)->sd_address)

#define	SCBP(pkt)	((struct scsi_status *)(pkt)->pkt_scbp)
#define	SCBP_C(pkt)	((*(pkt)->pkt_scbp) & STATUS_MASK)
#define	CDBP(pkt)	((union scsi_cdb *)(pkt)->pkt_cdbp)

#define	SST_CE_DEBUG1	((1 << 8) | CE_CONT)
#define	SST_CE_DEBUG2	((2 << 8) | CE_CONT)
#define	SST_CE_DEBUG3	((3 << 8) | CE_CONT)
#define	SST_CE_DEBUG4	((4 << 8) | CE_CONT)
#define	SST_LOG		if (sst_debug) sst_log
#define	SST_DEBUG_ENTER	if (sst_debug) debug_enter


/*
 * Private info for scsi targets.
 *
 * Pointed to by the un_private pointer
 * of one of the SCSI_DEVICE structures.
 */
struct scsi_target {
	struct scsi_pkt	*targ_rqs;	/* ptr to request sense command pkt */
	struct scsi_pkt	*targ_pkt;	/* ptr to current command pkt */
	struct	buf	*targ_sbufp;	/* for use in special io */
	kcondvar_t	targ_sbuf_cv;	/* conditional variable for sbufp */
	kcondvar_t	targ_pkt_cv;	/* conditional variable for pkt */
	kcondvar_t	targ_suspend_cv; /* conditional variable for  */
					/* suspended state */
	int		targ_sbuf_busy;	/* Wait Variable */
	int		targ_pkt_busy;	/* Wait Variable */
	int		targ_retry_ct;	/* retry count */
	u_int		targ_state;	/* current state */
	u_int		targ_arq;	/* ARQ mode on this tgt */
	struct scsi_device *targ_devp; /* back pointer to SCSI_DEVICE */
	struct buf	*targ_rqbp;	/* buf for Request Sense packet */
	int		targ_suspended;	/* suspend/resume */
	int		targ_pm_suspended; /* power management suspend */
	int		targ_power_level; /* PM power level */
};

_NOTE(MUTEX_PROTECTS_DATA(scsi_device::sd_mutex, scsi_target))

struct sst_private {
	struct buf 	*priv_bp;	/* bp associated with this packet */
	/*
	 * To handle partial DMA mappings, target may need several
	 * SCSI commands to satisfy packet.  Keep track of remaining
	 * data in this packet in the following two fields.
	 */
	u_int		priv_amt;	/* bytes requested in this chunk */
	u_int		priv_amtdone;	/* bytes done so far in current pkt */
};

_NOTE(DATA_READABLE_WITHOUT_LOCK(scsi_target::{targ_devp targ_rqs
	targ_state targ_sbufp targ_arq}))

_NOTE(SCHEME_PROTECTS_DATA("stable data",
	scsi_device))

_NOTE(SCHEME_PROTECTS_DATA("Unshared data",
	scsi_target::targ_pkt scsi_arq_status scsi_status
	buf scsi_pkt sst_private uio uscsi_cmd scsi_cdb))

/*
 * Driver states
 */
#define	SST_STATE_NIL		0
#define	SST_STATE_CLOSED	1
#define	SST_STATE_OPEN		2

/*
 * Parameters
 */

#define	SST_IO_TIME	30	/* default command timeout, 30sec */

/*
 * 5 seconds is what we'll wait if we get a Busy Status back
 */
#define	SST_BSY_TIMEOUT		(drv_usectohz(5 * 1000000))

/*
 * Number of times we'll retry a normal operation.
 *
 * This includes retries due to transport failure
 * (need to distinguish between Target and Transport failure)
 */
#define	SST_RETRY_COUNT		30

/*
 * sst_callback action codes
 */
#define	COMMAND_DONE		0
#define	COMMAND_DONE_ERROR	1
#define	QUE_COMMAND		2
#define	QUE_SENSE		3
#define	JUST_RETURN		4
#define	CONTINUE_PKT		5

/*
 * Special pkt flag just for this driver.
 * NB: Other pkt_flags defines are in scsi_pkt.h.
 */
#define	FLAG_SENSING    0x0400  /* Running request sense for failed pkt */

#endif	/* defined(_KERNEL) || defined(_KMEMUSER) */

/*
 * Ioctl commands
 */
#define	SSTIOC		('S' << 8)
#define	SSTIOC_READY	(SSTIOC|0)	/* Send a Test Unit Ready command */
#define	SSTIOC_ERRLEV	(SSTIOC|1)	/* Set Error Reporting level */



#if defined(_SYSCALL32)

/*
 * This is an example of how to support a 32-bit application issuing
 * an ILP32 ioctl into a 64-bit driver.  Using fixed data-size
 * types, define a 32-bit version of the data structure as used
 * by the ioctl, along with functions or macros to convert between
 * the ILP32 and LP64 models.  The LP64 driver can then use
 * ddi_copyin/ddi_copyout to access the application's copy of
 * the data structure, but internally use the usual ILP32 or
 * LP64 model, as compiled.
 */
struct sst_uscsi_cmd32 {
	int		uscsi_flags;	/* read, write, etc. see below */
	short		uscsi_status;	/* resulting status  */
	short		uscsi_timeout;	/* Command Timeout */
	caddr32_t	uscsi_cdb;	/* cdb to send to target */
	caddr32_t	uscsi_bufaddr;	/* i/o source/destination */
	size32_t	uscsi_buflen;	/* size of i/o to take place */
	size32_t	uscsi_resid;	/* resid from i/o operation */
	u_char		uscsi_cdblen;	/* # of valid cdb bytes */
	u_char		uscsi_rqlen;	/* size of uscsi_rqbuf */
	u_char		uscsi_rqstatus;	/* status of request sense cmd */
	u_char		uscsi_rqresid;	/* resid of request sense cmd */
	caddr32_t	uscsi_rqbuf;	/* request sense buffer */
	caddr32_t	uscsi_reserved_5;	/* Reserved for Future Use */
};


/*
 * Convert application's ILP32 uscsi_cmd to LP64
 */
#define	sst_uscsi_cmd32touscsi_cmd(u32, ucmd)				\
	ucmd->uscsi_flags	= u32->uscsi_flags;			\
	ucmd->uscsi_status	= u32->uscsi_status;			\
	ucmd->uscsi_timeout	= u32->uscsi_timeout;			\
	ucmd->uscsi_cdb		= (caddr_t)u32->uscsi_cdb;		\
	ucmd->uscsi_bufaddr	= (caddr_t)u32->uscsi_bufaddr;		\
	ucmd->uscsi_buflen	= (size_t)u32->uscsi_buflen;		\
	ucmd->uscsi_resid	= (size_t)u32->uscsi_resid;		\
	ucmd->uscsi_cdblen	= u32->uscsi_cdblen;			\
	ucmd->uscsi_rqlen	= u32->uscsi_rqlen;			\
	ucmd->uscsi_rqstatus	= u32->uscsi_rqstatus;			\
	ucmd->uscsi_rqresid	= u32->uscsi_rqresid;			\
	ucmd->uscsi_rqbuf	= (caddr_t)u32->uscsi_rqbuf;		\
	ucmd->uscsi_reserved_5	= (void *)u32->uscsi_reserved_5;


/*
 * Convert drivers's LP64 uscsi_cmd back to IPL32
 */
#define	sst_uscsi_cmdtouscsi_cmd32(ucmd, u32)				\
	u32->uscsi_flags	= ucmd->uscsi_flags;			\
	u32->uscsi_status	= ucmd->uscsi_status;			\
	u32->uscsi_timeout	= ucmd->uscsi_timeout;			\
	u32->uscsi_cdb		= (caddr32_t)ucmd->uscsi_cdb;		\
	u32->uscsi_bufaddr	= (caddr32_t)ucmd->uscsi_bufaddr;	\
	u32->uscsi_buflen	= (size32_t)ucmd->uscsi_buflen;		\
	u32->uscsi_resid	= (size32_t)ucmd->uscsi_resid;		\
	u32->uscsi_cdblen	= ucmd->uscsi_cdblen;			\
	u32->uscsi_rqlen	= ucmd->uscsi_rqlen;			\
	u32->uscsi_rqstatus	= ucmd->uscsi_rqstatus;			\
	u32->uscsi_rqresid	= ucmd->uscsi_rqresid;			\
	u32->uscsi_rqbuf	= (caddr32_t)ucmd->uscsi_rqbuf;		\
	u32->uscsi_reserved_5	= (caddr32_t)ucmd->uscsi_reserved_5;

#endif /* _SYSCALL32 */

#ifdef	__cplusplus
}
#endif

#endif	/* _SST_DEF_H */
