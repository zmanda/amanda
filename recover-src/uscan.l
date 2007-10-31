/*
 * amanda, the advanced maryland automatic network disk archiver
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
 * $Id: uscan.l,v 1.27 2006/07/05 11:15:56 martinea Exp $
 *
 * lexer for amrecover interactive language
 */
%{
#include "amanda.h"
#include "uparse.h"

/*
 * We redefine this here to prevent compiler warning about ignoring fwrite
 * return value...
 */
#undef ECHO
#define ECHO do {						\
	if (fwrite(yytext, (size_t)yyleng, 1, yyout) <= 0) {	\
	    yyerror("ECHO failure");				\
	}							\
} while (0)

#define YY_NO_UNPUT

#define	DATE_ALLOC_SIZE		sizeof("YYYY-MM-DD-HH-MM-SS")	/* includes null */

#define YY_DECL	int yylex(void)
extern int	yylex(void);

extern void	yyerror(char *s);
extern int	yyparse(void);
static int	ll_parse_date(int type, char *text);
int		process_line(char *line);
%}

%x quotedpath

%{
static char *string_buf = NULL;
%}

%%

%{
    /* literal keyword tokens */
%}

listhost	{ return LISTHOST; }
listdisk	{ return LISTDISK; }
sethost		{ return SETHOST; }
setdisk		{ return SETDISK; }
setdate		{ return SETDATE; }
setmode		{ return SETMODE; }
settape		{ return SETTAPE; }
setdevice	{ return SETDEVICE; }
-h		{ return DASH_H; }
cd		{ return CD; }
cdx		{ return CDX; }
quit		{ return QUIT; }
exit		{ return QUIT; }
history		{ return DHIST; }
ls		{ return LS; }
add		{ return ADD; }
addx		{ return ADDX; }
list		{ return LIST; }
delete		{ return DELETE; }
deletex		{ return DELETEX; }
pwd		{ return PWD; }
clear		{ return CLEAR; }
help		{ return HELP; }
\?		{ return HELP; }
lcd		{ return LCD; }
lpwd		{ return LPWD; }
extract		{ return EXTRACT; }
smb		{ return SMB; }
tar		{ return TAR; }
mode		{ return MODE; }

%{
    /* dates */
%}

---[0-9]+		{ return ll_parse_date(1, yytext); }
--[0-9]+-[0-9]+		{ return ll_parse_date(2, yytext); }
[0-9]+-[0-9]+-[0-9]+	{ return ll_parse_date(3, yytext); }
[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+	{ return ll_parse_date(4, yytext); }
[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+	{ return ll_parse_date(5, yytext); }

%{
    /* quoted file names */
%}

\"			{
    if(string_buf != NULL) {
	g_printf("ERROR:string_buf != NULL: %s\n",string_buf);
    }
    BEGIN(quotedpath);
    strappend(string_buf, yytext);
}

<quotedpath>[^\\\"]+	{
    strappend(string_buf, yytext);
}

<quotedpath>\\.	{
    /* escaped character (including quote) */
    strappend(string_buf, yytext);
}

<quotedpath>\"	{ /* saw closing quote - all done */
    strappend(string_buf, yytext);
    yylval.strval = string_buf;
    string_buf = NULL;
    BEGIN(INITIAL);
    return PATH;
}

%{
    /* file names */
%}

[^[:space:][:cntrl:]"]+		{
    yylval.strval = stralloc(yytext);
    return PATH;
}

%{
    /* whitespace */
%}

[[:space:]]+	;     /* whitespace */

%{
    /* anything else */
    /* everything should have been handled by now, so this rule is disabled */
%}

%{
#if 0
.	{ yyerror("invalid character"); }
#endif
%}

%%

int
process_line(
    char *	line)
{
    YY_BUFFER_STATE b;
    int result;

    b = yy_scan_string(line);		/* tell lex to scan lineread */
    result = yyparse();			/* parse lineread and act */
    yy_delete_buffer(b);
    return result;
}

static int
ll_parse_date(
    int		type,
    char *	text)
{
    time_t now;
    struct tm *t;
    int y=2000, m=0, d=1, h=0, mi=0, s=0;
    int ret;

    now = time((time_t *)NULL);
    t = localtime(&now);
    if (t) {
	y = 1900+t->tm_year;
	m = t->tm_mon+1;
	d = t->tm_mday;
    }
    switch(type) {
    case 1:
	if (sscanf(text, "---%d", &d) != 1) {
	    yyerror("invalid date");
        }
        break;
    case 2:
	if (sscanf(text, "--%d-%d", &m, &d) != 2) {
	    yyerror("invalid date");
        }
        break;
    case 3:
	if (sscanf(text, "%d-%d-%d", &y, &m, &d) != 3) {
	    yyerror("invalid date");	    
        }
        break;
    case 4:
	if (sscanf(text, "%d-%d-%d-%d-%d-%d", &y, &m, &d, &h, &mi, &s) != 6) {
	    yyerror("invalid date");
	}
        break;
    case 5:
	if (sscanf(text, "%d-%d-%d-%d-%d", &y, &m, &d, &h, &mi) != 5) {
	    yyerror("invalid date");
	}
        break;
    }

    ret = PATH;				/* cause a parse error */
    if(y < 70) {
	y += 2000;
    } else if(y < 100) {
	y += 1900;
    }
    if(y < 1000 || y > 9999) {
	yyerror("invalid year");
    } else if(m < 1 || m > 12) {
	yyerror("invalid month");
    } else if(d < 1 || d > 31) {
	yyerror("invalid day");
    } else if(h < 0 || h > 24) {
	yyerror("invalid hour");
    } else if(mi < 0 || mi > 59) {
	yyerror("invalid minute");
    } else if(s < 0 || s > 59) {
	yyerror("invalid second");
    } else if(type < 4) {
	yylval.strval = alloc(DATE_ALLOC_SIZE);
	g_snprintf(yylval.strval, DATE_ALLOC_SIZE, "%04d-%02d-%02d", y, m, d);
	ret = DATE;
    } else {
	yylval.strval = alloc(DATE_ALLOC_SIZE);
	g_snprintf(yylval.strval, DATE_ALLOC_SIZE, "%04d-%02d-%02d-%02d-%02d-%02d", y, m, d, h, mi, s);
	ret = DATE;
    }
    return ret;
}

int
yywrap(void)
{
  return 1;
}

