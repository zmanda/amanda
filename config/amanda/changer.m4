# SYNOPSIS
#
#   AMANDA_SETUP_CHANGER
#
# OVERVIEW
#
#   Set up for changers.  This first checks the availability of several
#   changer-related headers, then, based on those results, tries to 
#   compile some test programs for each supported changer API.  It finishes
#   by defining a series of AM_CONDITIONALS which are all used in
#   changer-src/Makefile.am.
#
#   The macro also searches for chio, chs, mtx, and mcutil, which are used
#   from various shell scripts in the changer-src/ directory.
#
AC_DEFUN([AMANDA_SETUP_CHANGER], [
    AC_REQUIRE([AMANDA_PROG_CHIO])
    AC_REQUIRE([AMANDA_PROG_CHS])
    AC_REQUIRE([AMANDA_PROG_MTX])
    AC_REQUIRE([AMANDA_PROG_MCUTIL])

    AC_CHECK_HEADERS( \
	camlib.h \
	chio.h \
	linux/chio.h \
	scsi/sg.h \
	scsi/scsi_ioctl.h \
	sys/chio.h \
	sys/dsreq.h \
	sys/mtio.h \
	sys/scarray.h \
	sys/gscdds.h \
	sys/scsi.h \
	sys/scsiio.h \
	sys/scsi/impl/uscsi.h \
	sys/scsi/scsi/ioctl.h, \
	[], [], [AC_INCLUDES_DEFAULT])

    #
    # chio support
    #
    if test x"$ac_cv_header_sys_scsi_h" = x"yes"; then
	AC_CACHE_CHECK([for HP/UX-like scsi changer support],
	    amanda_cv_hpux_scsi_chio,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/scsi.h>
    ]], [[
	    static struct element_addresses changer_info;
	    int i = SIOC_ELEMENT_ADDRESSES;
	    int j = SIOC_ELEMENT_STATUS;
	    int k = SIOC_MOVE_MEDIUM;
    ]])],[amanda_cv_hpux_scsi_chio=yes],[amanda_cv_hpux_scsi_chio=no])])
	if test x"$amanda_cv_hpux_scsi_chio" = x"yes"; then
	    WANT_SCSI_HPUX=yes
	    WANT_CHG_SCSI=yes
	fi
    fi

    #
    # Linux SCSI based on ioctl
    #
    if test x"$ac_cv_header_sys_mtio_h" = x"yes" &&
       test x"$ac_cv_header_scsi_scsi_ioctl_h" = x"yes"; then 
	    AC_CACHE_CHECK([for Linux like scsi support (ioctl)],
	    amanda_cv_linux_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <scsi/scsi_ioctl.h>
#include <sys/mtio.h>
    ]], [[
	    int device;
	    char *Command;
	    ioctl(device, SCSI_IOCTL_SEND_COMMAND, Command);
    ]])],[amanda_cv_linux_scsi=yes],[amanda_cv_linux_scsi=no])])
    fi

    #
    # Linux SCSI based on sg
    #
    if test x"$ac_cv_header_sys_mtio_h" = x"yes" &&
       test x"$ac_cv_header_scsi_sg_h" = x"yes"; then 
	    AC_CACHE_CHECK([for Linux like scsi support (sg)],
	    amanda_cv_linux_sg_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <scsi/sg.h>
#include <sys/mtio.h>
    ]], [[
	    int device;
	    struct sg_header *psg_header;
	    char *buffer;
	    write(device, buffer, 1);
    ]])],[amanda_cv_linux_sg_scsi=yes],[amanda_cv_linux_sg_scsi=no])])
    fi

    if test x"$amanda_cv_linux_scsi" = x"yes" ||
     test x"$amanda_cv_linux_sg_scsi" = x"yes";then
	    WANT_SCSI_LINUX=yes
	    WANT_CHG_SCSI=yes
    fi

    #
    # HP-UX SCSI
    #
    if test x"$ac_cv_header_sys_mtio_h" = x"yes" &&
       test x"$ac_cv_header_sys_scsi_h" = x"yes"; then 
	    AC_CACHE_CHECK([for HP-UX like scsi support],
	    amanda_cv_hpux_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/scsi.h>
#include <sys/mtio.h>
    ]], [[
	    int device;
	    char *Command;
	    ioctl(device, SIOC_IO, Command);
    ]])],[amanda_cv_hpux_scsi=yes],[amanda_cv_hpux_scsi=no])])
	    if test x"$amanda_cv_hpux_scsi" = x"yes";then
		    WANT_SCSI_HPUX_NEW=yes
		    WANT_CHG_SCSI=yes
		    WANT_CHG_SCSI_CHIO=yes
	    fi
    fi

    #
    # IRIX SCSI
    #
    if test x"$ac_cv_header_sys_mtio_h" = x"yes" &&
       test x"$ac_cv_header_sys_dsreq_h" = x"yes"; then 
	    AC_CACHE_CHECK([for Irix like scsi support],
	    amanda_cv_irix_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <sys/dsreq.h>
#include <sys/mtio.h>
    ]], [[
	    int device=1;
	    char Command;
	    ioctl(device, DS_ENTER, &Command);
    ]])],[amanda_cv_irix_scsi=yes],[amanda_cv_irix_scsi=no])])
	    if test x"$amanda_cv_irix_scsi" = x"yes";then
		    WANT_SCSI_IRIX=yes
		    WANT_CHG_SCSI=yes
	    fi
    fi

    #
    # Solaris  SCSI
    #
    if test x"$ac_cv_header_sys_mtio_h" = x"yes" &&
       test x"$ac_cv_header_sys_scsi_impl_uscsi_h" = x"yes"; then 
	    AC_CACHE_CHECK([for Solaris-like scsi support],
	    amanda_cv_solaris_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <sys/scsi/impl/uscsi.h>
#include <sys/mtio.h>
    ]], [[
	    int device;
	    char *Command;
	    ioctl(device, USCSICMD, Command);
    ]])],[amanda_cv_solaris_scsi=yes],[amanda_cv_solaris_scsi=no])])
	    if test x"$amanda_cv_solaris_scsi" = x"yes";then
		    WANT_SCSI_SOLARIS=yes
		    WANT_CHG_SCSI=yes
	    fi
    fi

    #
    # AIX SCSI
    #
    if test x"$ac_cv_header_sys_tape_h" = x"yes" &&
       test x"$ac_cv_header_sys_scarray_h" = x"yes" &&
       test x"$ac_cv_header_sys_gscdds_h" = x"yes"; then 
	    AC_CACHE_CHECK([for AIX like scsi support],
	    amanda_cv_aix_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <sys/scarray.h>
#include <sys/tape.h>
    ]], [[
	    int device;
	    char *Command;
	    ioctl(device, STIOCMD, Command);
    ]])],[amanda_cv_aix_scsi=yes],[amanda_cv_aix_scsi=no])])
	    if test x"$amanda_cv_aix_scsi" = x"yes";then
		    WANT_SCSI_AIX=yes
		    WANT_CHG_SCSI=yes
	    fi
    fi
    #
    # BSD CAM SCSI
    #
    if test x"$ac_cv_header_cam_cam_h" = x"yes";then
	    AC_CACHE_CHECK([for CAM like scsi support],
	    amanda_cv_cam_scsi,
	    [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
# include <fcntl.h>
# include <cam/cam.h>
# include <cam/cam_ccb.h>
# include <cam/scsi/scsi_message.h>
# include <cam/scsi/scsi_pass.h>
# include <camlib.h>
    ]], [[
	    struct cam_device *curdev;

	    curdev = cam_open_pass("", O_RDWR, NULL);
    ]])],[amanda_cv_cam_scsi=yes],[amanda_cv_cam_scsi=no])])
	    if test x"$amanda_cv_cam_scsi" = x"yes";then
		    WANT_SCSI_CAM=yes
		    WANT_CHG_SCSI=yes
		    AC_CHECK_LIB(cam,main)
	    fi
    fi


    #
    # BSD SCSI
    #
    if test x"$ac_cv_header_sys_mtio_h" = x"yes" &&
       test x"$ac_cv_header_sys_scsiio_h" = x"yes"; then
	AC_CACHE_CHECK([for BSD like scsi support],
	amanda_cv_bsd_scsi,
	[AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <sys/scsiio.h>
#include <sys/mtio.h>
    ]], [[
	int device=1;
	char Command;
	ioctl(device, SCIOCCOMMAND, &Command);
    ]])],[amanda_cv_bsd_scsi=yes],[amanda_cv_bsd_scsi=no])])
	if test x"$amanda_cv_bsd_scsi" = x"yes";then
	   WANT_SCSI_BSD=yes
	   WANT_CHG_SCSI=yes
	fi
    fi

    # Do not build chg-scsi-chio if we cannot find the needed support
    # include files for the SCSI interfaces
    # chio.h and sys/chio.h are chio based systems
    if test x"$ac_cv_header_chio_h" = x"yes" ||
       test x"$ac_cv_header_linux_chio_h" = x"yes" ||
       test x"$ac_cv_header_sys_chio_h" = x"yes"; then
       # chg-scsi does not support FreeBSD 3.0's chio.h; it became backward
       # incompatible with the introduction of camlib.h
       if test x"$ac_cv_header_camlib_h" != x"yes"; then
	 WANT_SCSI_CHIO=yes
	 # prefer to use chg-scsi, unless we already have a driver for that,
	 # in which case set it up as chg-scsi-chio.
	 if test x"$WANT_CHG_SCSI" = x"no"; then
	   WANT_CHG_SCSI=yes
	 else
	   WANT_CHG_SCSI_CHIO=yes
	 fi
       fi
    fi

    # scsi-based implementations
    AM_CONDITIONAL(WANT_CHG_SCSI, test x"$WANT_CHG_SCSI" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_LINUX, test x"$WANT_SCSI_LINUX" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_HPUX_NEW, test x"$WANT_SCSI_HPUX_NEW" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_IRIX, test x"$WANT_SCSI_IRIX" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_SOLARIS, test x"$WANT_SCSI_SOLARIS" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_AIX, test x"$WANT_SCSI_AIX" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_CAM, test x"$WANT_SCSI_CAM" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_BSD, test x"$WANT_SCSI_BSD" = x"yes")

    # scsi-chio-based implementations
    AM_CONDITIONAL(WANT_CHG_SCSI_CHIO, test x"$WANT_CHG_SCSI_CHIO" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_HPUX, test x"$WANT_SCSI_HPUX" = x"yes")
    AM_CONDITIONAL(WANT_SCSI_CHIO, test x"$WANT_SCSI_CHIO" = x"yes")
])
