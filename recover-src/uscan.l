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
 * $Id$
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

#define YY_DECL	int yylex(void)
extern int	yylex(void);

extern void	yyerror(char *s);
extern int	yyparse(void);
int		process_line(char *line);
%}

%x quotedstring
%x needdevice
%x needstring
%x needmode
%x astring
%x propertyappend
%x propertypriority

LINEFEED	("\n"|"\r"|"\r\n")
%{
static char *string_buf = NULL;
%}

%%

%{
    /* literal keyword tokens */
%}

listhost	{ BEGIN(needstring); return LISTHOST; }
listdisk	{ BEGIN(needstring); return LISTDISK; }
listproperty	{ BEGIN(needstring); return LISTPROPERTY; }
sethost		{ BEGIN(needstring); return SETHOST; }
setdisk		{ BEGIN(needstring); return SETDISK; }
setdate		{ BEGIN(needstring); return SETDATE; }
setmode		{ BEGIN(needmode); return SETMODE; }
settape		{ BEGIN(needstring); return SETTAPE; }
setdevice	{ BEGIN(needdevice); return SETDEVICE; }
setproperty	{ BEGIN(propertyappend); return SETPROPERTY; }
<needdevice>-h	{ BEGIN(needstring); return DASH_H; }
cd		{ BEGIN(needstring); return CD; }
cdx		{ BEGIN(needstring); return CDX; }
quit		{ BEGIN(needstring); return QUIT; }
exit		{ BEGIN(needstring); return QUIT; }
history		{ BEGIN(needstring); return DHIST; }
ls		{ BEGIN(needstring); return LS; }
add		{ BEGIN(needstring); return ADD; }
addx		{ BEGIN(needstring); return ADDX; }
list		{ BEGIN(needstring); return LIST; }
delete		{ BEGIN(needstring); return DELETE; }
deletex		{ BEGIN(needstring); return DELETEX; }
pwd		{ BEGIN(needstring); return PWD; }
clear		{ BEGIN(needstring); return CLEAR; }
<INITIAL>help	{ BEGIN(INITIAL); return HELP; }
\?		{ BEGIN(needstring); return HELP; }
lcd		{ BEGIN(needstring); return LCD; }
lpwd		{ BEGIN(needstring); return LPWD; }
extract		{ BEGIN(needstring); return EXTRACT; }
mode		{ BEGIN(needstring); return MODE; }
<needmode>tar   { BEGIN(needstring); return TAR; }
<needmode>smb   { BEGIN(needstring); return SMB; }
<INITIAL,needdevice,needmode,needstring,astring,propertyappend,propertypriority>{LINEFEED}	{ BEGIN(INITIAL); return NL; }

%{
    /* quoted file names */
%}

<propertyappend>append {
    BEGIN(propertypriority);
    return APPEND;
}

<propertyappend,propertypriority>priority {
    BEGIN(needstring);
    return PRIORITY;
}

<needdevice,needmode,needstring,astring,propertyappend,propertypriority>\"			{
    if(string_buf != NULL) {
	g_printf("ERROR:string_buf != NULL: %s\n",string_buf);
    }
    BEGIN(quotedstring);
    strappend(string_buf, yytext);
}

<quotedstring>[^\\\"\n\r]+	{
    strappend(string_buf, yytext);
}

<quotedstring>\\.	{
    /* escaped character (including quote) */
    strappend(string_buf, yytext);
}

<quotedstring>\"	{ /* saw closing quote - all done */
    strappend(string_buf, yytext);
    yylval.strval = string_buf;
    string_buf = NULL;
    BEGIN(needstring);
    return STRING;
}

<quotedstring>{LINEFEED} {
    fprintf(stderr,"Unterminated quoted string\n");
    string_buf = NULL;
    BEGIN(INITIAL);
    return NL;
}

%{
    /* file names */
%}

<INITIAL,needdevice,needmode,needstring,astring,propertyappend,propertypriority>[^[:space:][:cntrl:]"]+		{
    yylval.strval = stralloc(yytext);
    BEGIN(needstring);
    return STRING;
}

%{
    /* whitespace */
%}

<INITIAL,needdevice,needmode,needstring,astring,propertyappend,propertypriority>[[:space:]]+ ; /* whitespace */

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

    char *line1 = stralloc2(line, "\n");
    b = yy_scan_string(line1);		/* tell lex to scan lineread */
    result = yyparse();			/* parse lineread and act */
    yy_delete_buffer(b);
    amfree(line1);
    return result;
}


int
yywrap(void)
{
  return 1;
}

