/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id: uparse.y,v 1.13 2006/05/25 01:47:14 johnfranks Exp $
 *
 * parser for amrecover interactive language
 */
%{
#include "amanda.h"
#include "amrecover.h"

void		yyerror(char *s);
extern int	yylex(void);
extern char *	yytext;

%}

/* DECLARATIONS */
%union {
	int	intval;
	double	floatval;
	char *	strval;
	int	subtok;
}

	/* literal keyword tokens */

%token LISTHOST LISTDISK SETHOST SETDISK SETDATE SETTAPE SETMODE SETDEVICE
%token CD CDX QUIT DHIST LS ADD ADDX EXTRACT DASH_H
%token LIST DELETE DELETEX PWD CLEAR HELP LCD LPWD MODE SMB TAR

        /* typed tokens */

%token <strval> PATH
%token <strval> DATE


/* GRAMMAR */
%%

ucommand:
	set_command
  |     display_command
  |     quit_command
  |     add_command
  |     addx_command
  |     delete_command
  |     deletex_command
  |     local_command
  |	help_command
  |     extract_command
  |     {
	    char * errstr = vstralloc("Invalid command: ", yytext, NULL);
	    yyerror(errstr);
	    amfree(errstr);
	    YYERROR;
	} /* Quiets compiler warnings about unused label */
  ;

set_command:
  	LISTHOST { list_host(); }
  |	LISTDISK PATH { list_disk($2); amfree($2); }
  |	LISTDISK { list_disk(NULL); }
  |	SETDATE DATE { set_date($2); amfree($2); }
  |     SETHOST PATH { set_host($2); amfree($2); }
  |     SETDISK PATH PATH { set_disk($2, $3); amfree($2); amfree($3); }
  |     SETDISK PATH { set_disk($2, NULL); amfree($2); }
  |     SETTAPE PATH { set_tape($2); amfree($2); }
  |     SETTAPE { set_tape("default"); }
  |	SETDEVICE PATH { set_device(NULL, $2); }
  |	SETDEVICE DASH_H PATH PATH { set_device($3, $4); }
  |	SETDEVICE { set_device(NULL, NULL); }
  |     CD PATH { cd_glob($2); amfree($2); }
  |     CDX PATH { cd_regex($2); amfree($2); }
  |     SETMODE SMB {
#ifdef SAMBA_CLIENT
			 set_mode(SAMBA_SMBCLIENT);
#endif /* SAMBA_CLIENT */
                    }
  |     SETMODE TAR {
#ifdef SAMBA_CLIENT
			 set_mode(SAMBA_TAR);
#endif /* SAMBA_CLIENT */
                    }
  ;

display_command:
	DHIST { list_disk_history(); }
  |     LS { list_directory(); }
  |     LIST PATH { display_extract_list($2); amfree($2); }
  |     LIST { display_extract_list(NULL); }
  |     PWD { show_directory(); }
  |     CLEAR { clear_extract_list(); }    
  |     MODE { show_mode (); }
  ;

quit_command:
	QUIT { quit(); }
  ;

add_command:
	ADD add_path
  ;

add_path:
	add_path PATH { add_glob($2); amfree($2); }
  |     PATH { add_glob($1); amfree($1); }
  ;

addx_command:
	ADDX addx_path
  ;

addx_path:
	addx_path PATH { add_regex($2); amfree($2); }
  |     PATH { add_regex($1); amfree($1); }
  ;

delete_command:
	DELETE delete_path
  ;

delete_path:
	delete_path PATH { delete_glob($2); amfree($2); }
  |     PATH { delete_glob($1); amfree($1); }
  ;

deletex_command:
	DELETEX deletex_path
  ;

deletex_path:
	deletex_path PATH { delete_regex($2); amfree($2); }
  |     PATH { delete_regex($1); amfree($1); }
  ;

local_command:
	LPWD { char * buf= g_get_current_dir(); puts(buf); free(buf); }
  |     LCD PATH {
		local_cd($2);
		amfree($2);
	}
  ;

help_command:
	HELP { help_list(); }
  ;

extract_command:
	EXTRACT { extract_files(); }
  ;

/* ADDITIONAL C CODE */
%%

void
yyerror(
    char *	s)
{
	g_printf("%s\n", s);
}
