/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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
 */
/*
 */
#include <amanda.h>

int main(void)
{
    FILE *sec_file = fopen("amanda-security.conf", "w");
    if (!sec_file) {
	fprintf(stderr,"Can't create amanda-security.conf");
	exit(1);
    }
    fprintf(sec_file,"############################################################\n");
    fprintf(sec_file,"# /etc/amanda-security.conf                                #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"# See: man amanda-security.conf                            #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"# This file must be installed at /etc/amanda-security.conf #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"# It list all executables amanda can execute as root.      #\n");
    fprintf(sec_file,"# This file must contains realpath to executable, with     #\n");
    fprintf(sec_file,"# all symbolic links resolved.                             #\n");
    fprintf(sec_file,"# You can use the 'realpath' command to find them.         #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"# It list program and a symbolic name for the program      #\n");
    fprintf(sec_file,"# Followed by the realpath of the binary                   #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"# Uncomment and edit the following lines to let Amanda to  #\n");
    fprintf(sec_file,"# use customized system commands.  If multiple PATH is     #\n");
    fprintf(sec_file,"# necessary, please put them in different lines.           #\n");
    fprintf(sec_file,"# e.g.:                                                    #\n");
    fprintf(sec_file,"# amgtar:GNUTAR_PATH=/usr/bin/tar                          #\n");
    fprintf(sec_file,"# amgtar:GNUTAR_PATH=/usr/bin/tar-1.28                     #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"# If a program and symbolic name is not listed, then the   #\n");
    fprintf(sec_file,"# configured binary is allowed to be run as root.          #\n");
    fprintf(sec_file,"# You can find the configured binary with amgetconf        #\n");
    fprintf(sec_file,"#     amgetconf build.gnutar_path                          #\n");
    fprintf(sec_file,"#     amgetconf build.star_path                            #\n");
    fprintf(sec_file,"#     amgetconf build.bsdtar_path                          #\n");
    fprintf(sec_file,"#                                                          #\n");
    fprintf(sec_file,"############################################################\n");

#ifdef GNUTAR
    fprintf(sec_file,"#runtar:gnutar_path=%s\n", GNUTAR);
    fprintf(sec_file,"#amgtar:gnutar_path=%s\n", GNUTAR);
#else
    fprintf(sec_file,"#runtar:gnutar_path=/no/default/gnutar/path\n");
    fprintf(sec_file,"#amgtar:gnutar_path=/no/default/gnutar/path\n");
#endif
#ifdef STAR
    fprintf(sec_file,"#amstar:star_path=%s\n", STAR);
#else
    fprintf(sec_file,"#amstar:star_path=/no/default/star/path\n");
#endif
#ifdef BSDTAR
    fprintf(sec_file,"#ambsdtar:bsdtar_path=%s\n", BSDTAR);
#else
    fprintf(sec_file,"#ambsdtar:bsdtar_path=/no/default/bsdtar/path\n");
#endif

    fprintf(sec_file,"\n");
    fprintf(sec_file,"#restore_by_amanda_user=no\n");
    fclose(sec_file);

    return 0;
}

