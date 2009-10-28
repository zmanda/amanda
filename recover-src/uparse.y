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
 * $Id$
 *
 * parser for amrecover interactive language
 */
%{
#include "amanda.h"
#include "amrecover.h"

#define DATE_ALLOC_SIZE sizeof("YYYY-MM-DD-HH-MM-SS")   /* includes null */

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

%token LISTHOST LISTDISK LISTPROPERTY
%token SETHOST SETDISK SETDATE SETTAPE SETMODE SETDEVICE SETPROPERTY
%token CD CDX QUIT DHIST LS ADD ADDX EXTRACT DASH_H
%token LIST DELETE DELETEX PWD CLEAR HELP LCD LPWD MODE SMB TAR
%token APPEND PRIORITY
%token NL

        /* typed tokens */

%token <strval> STRING


/* GRAMMAR */
%%

ucommand:
	set_command
  |     setdate_command
  |     display_command
  |     quit_command
  |     add_command
  |     addx_command
  |     delete_command
  |     deletex_command
  |     local_command
  |	help_command
  |     extract_command
  |     invalid_command
  ;

set_command:
	LISTHOST NL { list_host(); }
  |	LISTHOST invalid_string { yyerror("Invalid argument"); }
  |	LISTDISK STRING NL { list_disk($2); amfree($2); }
  |	LISTDISK NL { list_disk(NULL); }
  |	LISTDISK STRING invalid_string { yyerror("Invalid argument"); amfree($2); }
  |	LISTPROPERTY NL { list_property(); }
  |	LISTPROPERTY invalid_string { yyerror("Invalid argument"); }
  |	SETHOST STRING NL { set_host($2); amfree($2); }
  |	SETHOST STRING invalid_string { yyerror("Invalid argument"); amfree($2); }
  |	SETHOST NL { yyerror("Argument required"); }
  |	SETDISK STRING STRING NL { set_disk($2, $3); amfree($2); amfree($3); }
  |	SETDISK STRING NL { set_disk($2, NULL); amfree($2); }
  |	SETDISK STRING STRING invalid_string { yyerror("Invalid argument"); amfree($2); amfree($3); }
  |	SETDISK { yyerror("Argument required"); }
  |	SETTAPE STRING NL { set_tape($2); amfree($2); }
  |	SETTAPE NL { set_tape("default"); }
  |	SETTAPE STRING invalid_string { yyerror("Invalid argument"); amfree($2); }
  |	SETDEVICE STRING NL { set_device(NULL, $2); amfree($2); }
  |	SETDEVICE DASH_H STRING STRING NL { set_device($3, $4); amfree($3); amfree($4);  }
  |	SETDEVICE NL { set_device(NULL, NULL); }
  |	SETDEVICE STRING invalid_string { yyerror("Invalid argument"); amfree($2); }
  |	SETDEVICE DASH_H STRING NL { yyerror("Invalid argument"); amfree($3); }
  |	SETDEVICE DASH_H STRING STRING invalid_string { yyerror("Invalid argument"); amfree($3); amfree($4); }
  |	SETPROPERTY STRING property_value { set_property_name($2, 0); amfree($2); }
  |	SETPROPERTY APPEND STRING property_value { set_property_name($3, 1); amfree($3); }
  |	SETPROPERTY PRIORITY STRING property_value { set_property_name($3, 0); amfree($3); }
  |	SETPROPERTY APPEND PRIORITY STRING property_value { set_property_name($4, 1); amfree($4); }
  |	SETPROPERTY NL { yyerror("Invalid argument"); }
  |	SETPROPERTY APPEND NL { yyerror("Invalid argument"); }
  |	SETPROPERTY PRIORITY NL { yyerror("Invalid argument"); }
  |	SETPROPERTY APPEND PRIORITY NL { yyerror("Invalid argument"); }
  |	CD STRING NL { cd_glob($2, 1); amfree($2); }
  |	CD STRING invalid_string { yyerror("Invalid argument"); }
  |	CD NL { yyerror("Argument required"); }
  |     CDX STRING NL { cd_regex($2, 1); amfree($2); }
  |	CDX STRING invalid_string { yyerror("Invalid argument"); amfree($2); }
  |	CDX NL { yyerror("Argument required"); }
  |	SETMODE SMB NL { set_mode(SAMBA_SMBCLIENT); }
  |	SETMODE TAR NL { set_mode(SAMBA_TAR); }
  |	SETMODE SMB invalid_string { yyerror("Invalid argument"); }
  |	SETMODE TAR invalid_string { yyerror("Invalid argument"); }
  |	SETMODE invalid_string { yyerror("Invalid argument"); }
  |	SETMODE NL { yyerror("Argument required"); }
  ;

setdate_command:
	SETDATE STRING NL {
			time_t now;
			struct tm *t;
			int y=2000, m=0, d=1, h=0, mi=0, s=0;
			int ret;
			char *mydate = $2;

			now = time((time_t *)NULL);
			t = localtime(&now);
			if (t) {
			    y = 1900+t->tm_year;
			    m = t->tm_mon+1;
			    d = t->tm_mday;
			}
			if (sscanf(mydate, "---%d", &d) == 1 ||
			    sscanf(mydate, "--%d-%d", &m, &d) == 2 ||
			    sscanf(mydate, "%d-%d-%d", &y, &m, &d) == 3 ||
			    sscanf(mydate, "%d-%d-%d-%d-%d", &y, &m, &d, &h, &mi) == 5 ||
			    sscanf(mydate, "%d-%d-%d-%d-%d-%d", &y, &m, &d, &h, &mi, &s) == 6) {
			    if (y < 70) {
				y += 2000;
			    } else if (y < 100) {
				y += 1900;
			    }
			    if(y < 1000 || y > 9999) {
				printf("invalid year");
			    } else if(m < 1 || m > 12) {
				printf("invalid month");
			    } else if(d < 1 || d > 31) {
				printf("invalid day");
			    } else if(h < 0 || h > 24) {
				printf("invalid hour");
			    } else if(mi < 0 || mi > 59) {
				printf("invalid minute");
			    } else if(s < 0 || s > 59) {
				printf("invalid second");
			    } else {
				char result[DATE_ALLOC_SIZE];
				if (h == 0 && mi == 0 && s == 0)
				    g_snprintf(result, DATE_ALLOC_SIZE, "%04d-%02d-%02d", y, m, d);
				else
				    g_snprintf(result, DATE_ALLOC_SIZE, "%04d-%02d-%02d-%02d-%02d-%02d", y, m, d, h, mi, s);
				set_date(result);
			    }
			} else {
			    printf("Invalid date: %s\n", mydate);
			}
		     }
  |	SETDATE NL { yyerror("Argument required"); }
  |	SETDATE STRING invalid_string { yyerror("Invalid argument"); }
  ;

display_command:
	DHIST NL { list_disk_history(); }
  |	DHIST invalid_string { yyerror("Invalid argument"); }
  |	LS NL { list_directory(); }
  |	LS invalid_string { yyerror("Invalid argument"); }
  |	LIST STRING NL { display_extract_list($2); amfree($2); }
  |	LIST NL { display_extract_list(NULL); }
  |	LIST STRING invalid_string { yyerror("Invalid argument"); }
  |	PWD NL { show_directory(); }
  |	PWD invalid_string { yyerror("Invalid argument"); }
  |	CLEAR NL { clear_extract_list(); }
  |	CLEAR invalid_string { yyerror("Invalid argument"); }
  |	MODE NL { show_mode (); }
  |	MODE invalid_string { yyerror("Invalid argument"); }
  ;

quit_command:
	QUIT NL { quit(); }
  |	QUIT invalid_string { yyerror("Invalid argument"); }
  ;

add_command:
	ADD add_path NL
  ;

add_path:
	add_path STRING { add_glob($2); amfree($2); }
  |	STRING { add_glob($1); amfree($1); }
  ;

addx_command:
	ADDX addx_path NL
  ;

addx_path:
	addx_path STRING { add_regex($2); amfree($2); }
  |	STRING { add_regex($1); amfree($1); }
  ;

delete_command:
	DELETE delete_path NL
  ;

delete_path:
	delete_path STRING { delete_glob($2); amfree($2); }
  |	STRING { delete_glob($1); amfree($1); }
  ;

deletex_command:
	DELETEX deletex_path NL
  ;

deletex_path:
	deletex_path STRING { delete_regex($2); amfree($2); }
  |	STRING { delete_regex($1); amfree($1); }
  ;

local_command:
	LPWD NL { char * buf= g_get_current_dir(); puts(buf); free(buf); }
  |	LPWD invalid_string { yyerror("Invalid argument"); }
  |	LCD STRING NL {
		local_cd($2);
		amfree($2);
	}
  |	LCD STRING invalid_string { yyerror("Invalid argument"); }
  |	LCD NL { yyerror("Argument required"); }
  ;

help_command:
	HELP NL { help_list(); }
  |	HELP invalid_string { yyerror("Invalid argument"); }
  ;

extract_command:
	EXTRACT NL { extract_files(); }
  |	EXTRACT invalid_string { yyerror("Invalid argument"); }
  ;

invalid_command:
        STRING bogus_string {
	    char * errstr = vstralloc("Invalid command: ", $1, NULL);
	    yyerror(errstr);
	    amfree(errstr);
	    YYERROR;
	} /* Quiets compiler warnings about unused label */
  ;

property_value:
	STRING property_value { add_property_value($1); amfree( $1); }
  |     NL { ; }
  ;

invalid_string:
	STRING bogus_string { amfree($1); }
  ;

bogus_string:
        STRING bogus_string { amfree($1); }
  |	NL { ; }

/* ADDITIONAL C CODE */
%%

void
yyerror(
    char *	s)
{
	g_printf("%s\n", s);
}
