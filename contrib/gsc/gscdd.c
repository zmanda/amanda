/*
 * $Id: gscdd.c,v 1.1 2001/04/15 11:12:37 ant Exp $
 * Copyright (c) 1996, 1997 by Matthew Jacob
 *
 *	This software is free software; you can redistribute it and/or
 *	modify it under the terms of the GNU Library General Public
 *	License as published by the Free Software Foundation; version 2.
 *
 *	This software is distributed in the hope that it will be useful,
 *	but WITHOUT ANY WARRANTY; without even the implied warranty of
 *	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *	Library General Public License for more details.
 *
 *	You should have received a copy of the GNU Library General Public
 *	License along with this software; if not, write to the Free
 *	Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *	The author may be reached via electronic communications at
 *
 *		mjacob@feral.com
 *
 *	or, via United States Postal Address
 *
 *		Matthew Jacob
 *		1831 Castro Street
 *		San Francisco, CA, 94131
 */

#include <stdio.h>
#include <sys/types.h>
#include <sys/errno.h>
#include <sys/sysmacros.h>
#include <sys/syspest.h>
#include <sys/ioctl.h>
#include <sys/i_machine.h>
#include <sys/systm.h>
#include <sys/conf.h>
#include <sys/devinfo.h>
#include <sys/lockl.h>
#include <sys/device.h>
#include <sys/uio.h>
#include <sys/watchdog.h>
#include <sys/errids.h>
#include <sys/trchkid.h>
#include <sys/priv.h>
#include <sys/iostat.h>
#include <sys/bootrecord.h>
#include <sys/scsi.h>
#include <sys/malloc.h>
#include <sys/sleep.h>
#include <sys/fp_io.h>
#include <sys/pin.h>
#include <sys/lock_alloc.h>
#define	DD_LOCK	37
#include "gscdds.h"

static int strlen(char *s) { char *p = s; while (*p) p++; return p - s; }
static void memset(void *x, int val, size_t amt)
{ char *p = (char *)x; while (--amt) *p++ = (char) val; }
static void memcpy(void *dst, void *src, size_t amt)
 { char *dest = dst, *source = src; while (--amt) *dest++ = *source++; }
#define	bcopy(src, dst, nbytes)	memcpy(dst, src, nbytes)

/*
 * Local Definitions
 */

#define	HKWD_GSC_DD	0x66600000

#define	_COM_TRACE(b, var)	\
	var = strlen(b); trcgenk(0, HKWD_GSC_DD, var, var, b)

#define	Trace0(val, str)	\
	if (scudebug >= val) { \
		int icxq; \
		_COM_TRACE(str, icxq); \
	}
#define	Trace1(val, fmt, arg1)	\
	if (scudebug >= val) { \
		int icxq; char buf[256]; \
		(void) sprintf(buf, fmt, arg1); \
		_COM_TRACE(buf, icxq); \
	}
#define	Trace2(val, fmt, arg1, arg2)	\
	if (scudebug >= val) { \
		int icxq; char buf[256]; \
		(void) sprintf(buf, fmt, arg1, arg2); \
		_COM_TRACE(buf, icxq); \
	}
#define	Trace3(val, fmt, arg1, arg2, arg3)	\
	if (scudebug >= val) { \
		int icxq; char buf[256]; \
		(void) sprintf(buf, fmt, arg1, arg2, arg3); \
		_COM_TRACE(buf, icxq); \
	}
#define	Trace4(val, fmt, arg1, arg2, arg3, arg4)	\
	if (scudebug >= val) { \
		int icxq; char buf[256]; \
		(void) sprintf(buf, fmt, arg1, arg2, arg3, arg4); \
		_COM_TRACE(buf, icxq); \
	}

#define	Trace5(val, fmt, arg1, arg2, arg3, arg4, arg5)	\
	if (scudebug >= val) { \
		int icxq; char buf[256]; \
		(void) sprintf(buf, fmt, arg1, arg2, arg3, arg4, arg5); \
		_COM_TRACE(buf, icxq); \
	}

#define	Trace6(val, fmt, arg1, arg2, arg3, arg4, arg5, arg6)	\
	if (scudebug >= val) { \
		int icxq; char buf[256]; \
		(void) sprintf(buf, fmt, arg1, arg2, arg3, arg4, arg5, arg6); \
		_COM_TRACE(buf, icxq); \
	}

#define	MJ_RTN(VAL)	simple_unlock(&sp->dd_lock); return (VAL)

typedef struct {
    struct sc_buf scsibuf;
    uint index;
} gsc_buf_t;

typedef struct {
    Simple_lock dd_lock;
    Simple_lock buf_lock;
    struct file *fp;		/* file pointer */
    gsc_buf_t cbuf;
#define	cmdbuf cbuf.scsibuf	/* buffer for command */
    gsc_buf_t rbuf;
#define rqsbuf rbuf.scsibuf	/* buffer for request sense */
    dev_t dev;			/* Adapter dev */
    u_char tgt;			/* target ID */
    u_char lun;			/* logical unit */
    u_char isopen;		/* device is open */
    u_char iscfg;		/* non-zero to show this as configured */
    u_char unstart;		/* stop device on unconfigure */
    u_char needresume;		/* needs an SC_RESUME with next command */
} gsc_softc_t;

/*
 * External References
 */
extern int copyin(void *, void *, int);
extern int copyout(void *, void *, int);
extern int devstrat(struct buf *);
extern int nodev(void);
extern void setuerror(int);


/*
 * Device Driver Entry Points
 */
int gsc_config(dev_t, int, struct uio *);
static int gsc_open(dev_t);
static int gsc_close(dev_t);
static int gsc_ioctl(dev_t, int, void *, ulong);
static void gscdd_intr(struct buf *);

/*
 * Static Data
 */
static int scudebug = 10;
static int nunits = 0;
static gsc_softc_t softinfo[MAX_UNITS] = { 0 };
lock_t config_lock = { LOCK_AVAIL };


/*
 * Local Function Prototypes
 */

static int gsopen(gsc_softc_t *);
static void gsclose(gsc_softc_t *, dev_t);
static int gsccmd(dev_t, scmd_t *, ulong);
static int make_rqs(gsc_softc_t *, char, char *, int, int);

/*
 * Configuration Routines
 */

int
gsc_config(dev_t devno, int cmd, struct uio * uiop)
{
    struct gsc_ddsinfo ddsinfo;
    gsc_softc_t *sp;
    int result, i, unit;
    extern int nodev();
    static struct devsw gsc_dsw = {
	gsc_open,	/* entry point for open routine */
	gsc_close,	/* entry point for close routine */
	nodev,		/* entry point for read routine */
	nodev,		/* entry point for write routine */
	gsc_ioctl,	/* entry point for ioctl routine */
	nodev,		/* entry point for strategy routine */
	0,		/* pointer to tty device structure */
	nodev,		/* entry point for select routine */
	gsc_config,	/* entry point for config routine */
	nodev,		/* entry point for print routine */
	nodev,		/* entry point for dump routine */
	nodev,		/* entry point for mpx routine */
	nodev,		/* entry point for revoke routine */
	NULL,		/* pointer to device specific data */
	NULL,		/* select pointer */
	DEV_MPSAFE
    };

    if (lockl(&config_lock, LOCK_SHORT) != LOCK_SUCC) {
	return (EINVAL);
    }
    unit = minor(devno);
    if (unit < 0 || unit >= MAX_UNITS) {
	Trace2(0, "%d: bad unit %d", __LINE__, unit);
	result = EINVAL;
	unlockl(&config_lock);
	return (result);
    }

    switch (cmd) {
    case CFG_INIT:
	Trace2(2, "CFG_INIT: unit %d nunit %d\n", unit, nunits);
	/*
	 * Initialize softinfo, first time around.
	 */
	if (nunits == 0) {
	    memset(softinfo, 0, sizeof (softinfo));
	}
	/*
	 * Copy in DDS information
	 */
	uiomove((caddr_t) &ddsinfo, sizeof ddsinfo, UIO_WRITE, uiop);
	sp = &softinfo[unit];
	if (sp->iscfg) {
	    Trace1(0, "CFG_INIT: unit %d already configd", unit);
	    result = EBUSY;
	    break;
	}
	lock_alloc(&sp->dd_lock, LOCK_ALLOC_PIN, DD_LOCK, -1);
	lock_alloc(&sp->buf_lock, LOCK_ALLOC_PIN, DD_LOCK, -1);
	simple_lock_init(&sp->dd_lock);
	sp->dev = ddsinfo.busid;
	sp->tgt = ddsinfo.target;
	sp->lun = ddsinfo.lun;
	sp->cbuf.index = sp->rbuf.index = unit;
	/*
	 * If this is the first time through:
	 *   Add entry to the device switch table to call this driver
	 *   Pin driver code.
	 */
	if (nunits == 0) {
	    result = devswadd(devno, &gsc_dsw);
	    if (result != 0) {
		Trace1(0, "CFG_INIT: devswadd result: %d", result);
		break;
	    }
	    result = pincode((int (*) ()) gscdd_intr);
	    if (result) {
		Trace1(0, "CFG_INIT: pincode result: %d", result);
		devswdel(devno);
		break;
	    }
	}
	sp->iscfg = 1;
	result = gsopen(sp);
	if (result) {
	    Trace2(0, "CFG_INIT: gsopen returns %d for unit %d", result, unit);
	    sp->iscfg = 0;
	    gsclose(sp, devno);
	    break;
	}
	if (nunits <= unit)
	    nunits = unit + 1;
	sp->iscfg = 1;
	break;

    case CFG_TERM:
	Trace1(2, "CFG_TERM unit %d", unit);
	result = 0;
	sp = &softinfo[unit];
	if (sp->iscfg == 0) {
	    Trace1(0, "CFG_TERM: unit %d not already configd", unit);
	    result = ENXIO;
	    break;
	} else if (sp->isopen) {
	    Trace1(0, "CFG_TERM: unit %d open", unit);
	    result = EBUSY;
	    break;
	}
	sp->iscfg = 0;	/* block further actions */
	gsclose(sp, devno);
	break;

    default:
	result = EINVAL;
	break;
    }
    unlockl(&config_lock);
    return (result);
}

/*
 * Validate that devno is indeed for a SCSI adapter, and set up stuff for it.
 */
static int
gsopen(gsc_softc_t * sp)
{
    struct file *fp;
    int r;
    struct devinfo di;

    Trace2(2, "gsopen: %d.%d", major(sp->dev), minor(sp->dev));
    sp->fp = NULL;
    r = fp_opendev(sp->dev, DREAD|DWRITE|DKERNEL, NULL, 0, &fp);
    if (r) {
	Trace3(0, "%d: fp_opendev unit %d=%d", __LINE__, sp->cbuf.index, r);
	return (r);
    }
    r = fp_ioctl(fp, IOCINFO, (caddr_t) &di, NULL);
    if (r) {
	Trace3(0, "%d: fp_ioctl unit %d=%d", __LINE__, sp->cbuf.index, r);
	(void) fp_close(fp);
	return (r);
    }
    if (di.devtype != DD_BUS || di.devsubtype != DS_SCSI) {
	Trace2(0, "%d: not SCSI bus on unit %d", __LINE__,  sp->cbuf.index);
	(void) fp_close(fp);
	return (r);
    }
    sp->fp = fp;
    sp->unstart = 1;
    if (fp_ioctl(sp->fp, SCIOSTART, (caddr_t) IDLUN(sp->tgt, sp->lun), NULL)) {
	sp->unstart = 0;
    }
    return (0);
}

/*
 * Shut down a device
 */
static void
gsclose(gsc_softc_t *sp, dev_t devno)
{
    int i;
    if (sp->fp != NULL && sp->unstart) {
	(void) fp_ioctl(sp->fp, SCIOSTOP, (caddr_t) IDLUN(sp->tgt, sp->lun), NULL);
	sp->unstart = 0;
    }
    if (sp->fp) {
	(void) fp_close(sp->fp);
	sp->fp = NULL;
    }
    for (i = 0; i < MAX_UNITS; i++) {
	if (softinfo[i].iscfg) {
	    Trace1(0, "gsclose: unit %d still confd", i);
	    break;
	}
    }
    if (i == MAX_UNITS) {
	Trace0(0, "gsclose: All unconfigured now");
	(void) devswdel(devno);
	unpincode((int (*) ()) gscdd_intr);
    }
}

/*
 * VFS entry points
 */

static int
gsc_open(dev_t devno)
{
    gsc_softc_t *sp;
    int unit = minor(devno);

    Trace1(2, "gsc_open: open unit %d", unit);
    if (unit < 0 || unit >= MAX_UNITS) {
	return (ENODEV);
    }
    sp = &softinfo[unit];
    if (sp->iscfg == 0 || sp->fp == NULL) {
	Trace2(0, "%d: bad unit (%d)", __LINE__, unit);
	return (ENODEV);
    }
    simple_lock(&sp->dd_lock);
    if (sp->isopen) {
	simple_unlock(&sp->dd_lock);
	return (EBUSY);
    }
    sp->isopen = 1;
    simple_unlock(&sp->dd_lock);
    return (0);
}

static int
gsc_close(dev_t dev)
{
    gsc_softc_t *sp;
    int unit = minor(dev);

    Trace1(2, "gsc_close: close unit %d", unit);
    if (unit < 0 || unit >= MAX_UNITS) {
	return (ENODEV);
    }
    sp = &softinfo[unit];
    if (sp->iscfg == 0) {
	return (ENODEV);
    }
    simple_lock(&sp->dd_lock);
    sp->isopen = 0;
    simple_unlock(&sp->dd_lock);
    return (0);
}

static int
gsc_ioctl(dev_t dev, int cmd, void *arg, ulong dflag)
{
    switch (cmd) {
    case GSC_CMD:
	return (gsccmd(dev, arg, dflag));

    case GSC_SETDBG:
    {
	int i;
	cmd = copyin(arg, (caddr_t) &i, sizeof (int));
	if (cmd != 0) {
	    return (cmd);
	}
	cmd = scudebug;
	scudebug = i;
	return (copyout((caddr_t) &cmd, arg, sizeof (int)));
    }
    default:
	return (ENOTTY);
    }
}


/****************************************************************************/

static int
gsccmd(dev_t dev, scmd_t *argcmd, ulong dflag)
{
    gsc_softc_t *sp;
    scmd_t local, *l;
    char sbyte, albits;
    struct sc_buf *usc;
    struct buf *Ubp;
    int r, r2, ival, upin, unit, rqvalid, once;

    unit = minor(dev);
    Trace2(1, "%d: cmd for unit %d", __LINE__, minor(dev));
    if (unit < 0 || unit >= MAX_UNITS) {
	setuerror(ENXIO);
	return (ENXIO);
    }
    sp = &softinfo[unit];
    if (sp->iscfg == 0 || sp->fp == NULL) {
	Trace2(0, "gsccmd: bad unit %d (cfg=%d)", unit, sp->iscfg);
	r = ENODEV;
	setuerror(r);
	return (r);
    }
    simple_lock(&sp->dd_lock);
    l = &local;
    if (dflag & DKERNEL) {
	l = argcmd;
    } else {
	r = copyin((caddr_t) argcmd, (caddr_t) l, sizeof (scmd_t));
	if (r != 0) {
	    Trace2(0, "%d: copyin=%d", __LINE__, r);
	    setuerror(r);
	    MJ_RTN (r);
	}
    }
    Trace6(1, "%d: cdblen%d datalen%d snslen%d rw=%d tv=%d", __LINE__,
	   l->cdblen, l->datalen, l->senselen, l->rw, l->timeval);
    sbyte = 0;
    rqvalid = upin = r = r2 = 0;
    usc = &sp->cmdbuf;
    Ubp = &usc->bufstruct;
    memset(usc, 0, sizeof (struct sc_buf));

    /*
     * Check some parameters...
     */

    if (l->cdblen > sizeof (struct sc_cmd)) {
	r = EINVAL;
	goto out;
    }

    /*
     * Setup sc_buf structure
     */
    Ubp->b_iodone = gscdd_intr;
    Ubp->b_dev = sp->dev;
    Ubp->b_flags = B_BUSY | B_MPSAFE;
    Ubp->b_resid = Ubp->b_bcount = l->datalen;
    Ubp->b_xmemd.aspace_id = XMEM_INVAL;
    Ubp->b_event = EVENT_NULL;

    if (l->datalen) {
	Ubp->b_un.b_addr = l->data_buf;
	if (l->rw) {
	    Ubp->b_flags |= B_READ;
	}
	if (dflag & DKERNEL) {
	    r = pinu(l->data_buf, l->datalen, UIO_SYSSPACE);
	} else {
	    r = pinu(l->data_buf, l->datalen, UIO_USERSPACE);
	}
	if (r) {
	    Trace2(0, "%d: pinu buf %d", __LINE__, r);
	    goto out;
	}
	upin++;
	if (dflag & DKERNEL) {
	    r = xmattach(l->data_buf, l->datalen, &Ubp->b_xmemd, SYS_ADSPACE);
	} else {
	    r = xmattach(l->data_buf, l->datalen, &Ubp->b_xmemd, USER_ADSPACE);
	}
	if (r != XMEM_SUCC) {
	    Trace2(0, "%d: xmattach %d", __LINE__, r);
	    r = EFAULT;
	    goto out;
	}
	upin++;
	r = xmemdma(&Ubp->b_xmemd, l->data_buf, XMEM_UNHIDE);
	if (r == XMEM_FAIL) {
	    Trace2(0, "%d: xmemdma %d", __LINE__, r);
	    r = EFAULT;
	    goto out;
	}
	r = 0;
    }
    usc->scsi_command.scsi_id = sp->tgt;
    usc->scsi_command.scsi_length = l->cdblen;
    if (dflag & DKERNEL) {
	bcopy(l->cdb, (caddr_t)&usc->scsi_command.scsi_cmd, l->cdblen);
    } else {
	r = copyin(l->cdb, (caddr_t) & usc->scsi_command.scsi_cmd, l->cdblen);
	if (r != 0) {
	    goto out;
	}
    }
    /* Setting lun in SCSI CDB as well as sc_buf structure */
    usc->lun = sp->lun;
    usc->scsi_command.scsi_cmd.lun &= 0x1F;
    usc->scsi_command.scsi_cmd.lun |= (sp->lun << 5) & 0xE0;
    albits = usc->scsi_command.scsi_cmd.lun;
    usc->timeout_value = l->timeval;
    if (sp->needresume) {
	usc->flags |= SC_RESUME;
	sp->needresume = 0;
    }

    if (scudebug > 1) {
	char *c = (char *) &usc->scsi_command.scsi_cmd;
	char cdbuf[64];
	(void) sprintf(cdbuf,
		       "0x%02x 0x%02x 0x%02x 0x%02x 0x%02x 0x%02x "
		       "0x%02x 0x%02x 0x%02x 0x%02x 0x%02x 0x%02x",
		       c[0], c[1], c[2], c[3], c[4], c[5],
		       c[6], c[7], c[8], c[9], c[10], c[11]);
	Trace2(0, "%d: cdb=%s", __LINE__, cdbuf);
    }

    once = 0;
again:
    Ubp->b_flags &= ~B_DONE;
    r = devstrat(Ubp);
    if (r == 0) {
	ival = disable_lock(INTCLASS1, &sp->buf_lock);
	while ((Ubp->b_flags & B_DONE) == 0) {
	    e_sleep_thread(&Ubp->b_event, &sp->buf_lock, LOCK_HANDLER);
	}
	unlock_enable(ival, &sp->buf_lock);
    } else {
	/*
	 * If ENXIO,  We never actually got started.
	 */
	if (r == ENXIO && once == 0) {
	    once++;
	    usc->flags |= SC_RESUME|SC_DELAY_CMD;
	    goto again;
	}
	sp->needresume = 1;
	Trace2(1, "%d: devstrat=%d", __LINE__, r);
	goto out;
    }

    Trace4(1, "%d: b_flags %x b_error %d b_resid %d", __LINE__,
	   Ubp->b_flags, Ubp->b_error, Ubp->b_resid);
    Trace5(1, "%d: sv %x st %x gc %x as %x", __LINE__,
	   usc->status_validity, usc->scsi_status,
	   usc->general_card_status, usc->adap_q_status);

    if (Ubp->b_flags & B_ERROR) {
	r = Ubp->b_error;
	sp->needresume = 1;
    }

    if (usc->status_validity & SC_SCSI_ERROR) {
	sbyte = (usc->scsi_status & SCSI_STATUS_MASK);
	sp->needresume = 1;
	if (sbyte == SC_CHECK_CONDITION && l->senselen) {
	    struct sc_buf *usl;
	    struct buf *Sbp;

	    r = make_rqs(sp, albits, l->sense_buf, l->senselen,
		     (dflag & DKERNEL) != 0);
	    if (r) {
		Trace2(0, "%d: make_rqs=%d", __LINE__, r);
		goto out;
	    }
	    usl = &sp->rqsbuf;
	    Sbp = &usl->bufstruct;
	    r = devstrat(Sbp);
	    if (r == 0) {
		ival = disable_lock(INTCLASS1, &sp->buf_lock);
		while ((Sbp->b_flags & B_DONE) == 0) {
		    e_sleep_thread(&Sbp->b_event, &sp->buf_lock, LOCK_HANDLER);
		}
		unlock_enable(ival, &sp->buf_lock);
	    } else {
		Trace2(0, "%d:ds=%d for rqs", __LINE__, r);
		goto out;
	    }
	    xmdetach(&Sbp->b_xmemd);
	    if (dflag & DKERNEL) {
		(void) unpinu(l->sense_buf, l->senselen, UIO_SYSSPACE);
	    } else {
		(void) unpinu(l->sense_buf, l->senselen, UIO_USERSPACE);
	    }
	    Trace4(1, "%d SENSE: b_flags %x b_error %d b_resid %d",
		   __LINE__, Sbp->b_flags, Sbp->b_error,
		   Sbp->b_resid);
	    Trace5(1, "%d: sv %x st %x gc %x as %x", __LINE__,
		   usl->status_validity, usl->scsi_status,
		   usl->general_card_status, usl->adap_q_status);
	    if (usl->scsi_status || usl->general_card_status) {
		r = EIO;
	    } else {
		rqvalid = 1;
	    }
	}
    }

    if (usc->status_validity & SC_ADAPTER_ERROR) {
	sp->needresume = 1;
	Trace2(0, "%d: adapter error 0x%x", __LINE__,
	       usc->general_card_status);
	Ubp->b_flags |= B_ERROR;
	switch (usc->general_card_status) {
	case SC_NO_DEVICE_RESPONSE:
	case SC_HOST_IO_BUS_ERR:
	case SC_SCSI_BUS_FAULT:
	case SC_CMD_TIMEOUT:
	case SC_ADAPTER_HDW_FAILURE:
	case SC_ADAPTER_SFW_FAILURE:
	case SC_FUSE_OR_TERMINAL_PWR:
	case SC_SCSI_BUS_RESET:
	default:
	    r = EIO;
	    break;
	}
    }

    /*
     * Log errors through errsave function
     */
    if (usc->status_validity & (SC_SCSI_ERROR|SC_ADAPTER_ERROR)) {
	struct sc_error_log_df log;

	memset(&log, 0, sizeof (log));
	/*
	 * All errors are 'temporary unknown driver error'
	 */
	log.error_id = ERRID_SCSI_ERR6;
	(void) sprintf(log.resource_name, "gsc%d", unit);
	memcpy(&log.scsi_command, &usc->scsi_command, sizeof (struct scsi));
	log.status_validity = usc->status_validity;
	log.scsi_status = usc->scsi_status;
	log.general_card_status	= usc->general_card_status;
	if (rqvalid) {
	    int amt;
	    if (l->senselen > 128)
		amt = 128;
	    else
		amt = l->senselen;
	    (void) copyin(l->sense_buf, log.req_sense_data, amt);
	}
	errsave(&log, sizeof (struct sc_error_log_df));
    }

    if (dflag & DKERNEL) {
	*l->statusp = sbyte;
    } else {
	r2 = copyout(&sbyte, l->statusp, 1);
	if (r2 != 0) {
	    if (r == 0)
		r = r2;
	    goto out;
	}
    }
out:
    if (l->datalen) {
	if (upin > 1) {
	    xmdetach(&Ubp->b_xmemd);
	    upin--;
	}
	if (upin > 0) {
	    if (dflag & DKERNEL) {
		(void) unpinu(l->data_buf, l->datalen, UIO_SYSSPACE);
	    } else {
		(void) unpinu(l->data_buf, l->datalen, UIO_USERSPACE);
	    }
	    upin--;
	}
    }
    Trace2(1, "%d: returning %d", __LINE__, r);
    if (r)
	setuerror(r);
    MJ_RTN (r);
}

static int
make_rqs(gsc_softc_t * sp, char albits, char *uaddr, int ulen, int isk)
{
    struct sc_buf *usl;
    struct buf *Sbp;
    int err, upin;

    if (ulen > 255)
	ulen = 255;
    upin = err = 0;
    usl = &sp->rqsbuf;
    Sbp = &usl->bufstruct;
    memset(usl, 0, sizeof (struct sc_buf));

    Sbp->b_un.b_addr = uaddr;
    Sbp->b_resid = Sbp->b_bcount = ulen;
    Sbp->b_iodone = gscdd_intr;
    Sbp->b_dev = sp->dev;
    Sbp->b_flags = B_BUSY | B_READ | B_MPSAFE;
    Sbp->b_event = EVENT_NULL;
    Sbp->b_xmemd.aspace_id = XMEM_INVAL;

    if (isk)
	err = pinu(uaddr, ulen, UIO_SYSSPACE);
    else
	err = pinu(uaddr, ulen, UIO_USERSPACE);
    if (err)
	goto out;
    upin++;
    if (isk)
	err = xmattach(uaddr, ulen, &Sbp->b_xmemd, SYS_ADSPACE);
    else
	err = xmattach(uaddr, ulen, &Sbp->b_xmemd, USER_ADSPACE);
    if (err != XMEM_SUCC) {
	err = EFAULT;
	goto out;
    }
    upin++;
    err = xmemdma(&Sbp->b_xmemd, Sbp->b_un.b_addr, XMEM_UNHIDE);
    if (err == XMEM_FAIL) {
	err = EFAULT;
	(void) xmdetach(&Sbp->b_xmemd);
	goto out;
    }
    err = 0;

    usl->lun = sp->lun; /* Setting lun in sc_buf structure */
    usl->scsi_command.scsi_id = sp->tgt;
    usl->scsi_command.scsi_length = 6;
    usl->scsi_command.scsi_cmd.scsi_op_code = 0x3;
    usl->scsi_command.scsi_cmd.lun = albits & 0xE0; /*ONLY copy the lun bits*/
    usl->scsi_command.scsi_cmd.scsi_bytes[2] = ulen;
    usl->timeout_value = 2;
    usl->flags = SC_RESUME;
    sp->needresume = 0;

 out:
    if (err) {
	if (upin > 1) {
	    xmdetach(&Sbp->b_xmemd);
	    upin--;
	}
	if (upin > 0) {
	    if (isk)
		(void) unpinu(uaddr, ulen, UIO_SYSSPACE);
	    else
		(void) unpinu(uaddr, ulen, UIO_USERSPACE);
	    upin--;
	}
    }
    return (err);
}

void
gscdd_intr(struct buf * bp)
{
    int lv;
    gsc_softc_t *sp = &softinfo[((gsc_buf_t *)bp)->index];

    lv = disable_lock(INTIODONE, &sp->buf_lock);
    bp->b_flags |= B_DONE;
    unlock_enable(lv, &sp->buf_lock);
    e_wakeup(&bp->b_event);
}
/*
 * mode: c
 * Local variables:
 * c-indent-level: 4
 * c-brace-imaginary-offset: 0
 * c-brace-offset: -4
 * c-argdecl-indent: 4
 * c-label-offset: -4
 * c-continued-statement-offset: 4
 * c-continued-brace-offset: 0
 * End:
 */
