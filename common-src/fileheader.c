/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
 * $Id: fileheader.c 6512 2007-05-24 17:00:24Z ian $
 */

#include "amanda.h"
#include "fileheader.h"
#include <glib.h>
#include "util.h"

static const char *	filetype2str(filetype_t);
static filetype_t	str2filetype(const char *);
static void		strange_header(dumpfile_t *, const char *,
				size_t, const char *, const char *);
static ssize_t 		hexdump(const char *buffer, size_t len);
static char            *quote_heredoc(char *text, char *delimiter_prefix);
static char            *parse_heredoc(char *line, char **saveptr);

void
fh_init(
    dumpfile_t *file)
{
    memset(file, '\0', SIZEOF(*file));
    file->type = F_EMPTY;
    file->blocksize = 0;
}

static void
strange_header(
    dumpfile_t *file,
    const char *buffer,
    size_t	buflen,
    const char *expected,
    const char *actual)
{
    if (actual == NULL)
	actual = "<null>";
    if (expected == NULL)
	expected = "<null>";

    g_fprintf(stderr, _("%s: strange amanda header: \"%.*s\"\n"), get_pname(),
		(int)buflen, buffer);

    g_fprintf(stderr, _("%s: Expected: \"%s\"  Actual: \"%s\"\n"), get_pname(),
		expected, actual);

    file->type = F_WEIRD;
}

/* chop whitespace off of a string, in place */
static void
chomp(char *str)
{
    char *s = str;

    if (!str)
	return;

    /* trim leading space */
    while (g_ascii_isspace(*s)) { s++; }
    if (s != str)
	memmove(str, s, strlen(s)+1);

    /* trim trailing space */
    if (*str) {
	for (s = str+strlen(str)-1; s >= str; s--) {
	    if (!g_ascii_isspace(*s))
		break;
	    *s = '\0';
	}
    }
}

void
parse_file_header(
    const char *buffer,
    dumpfile_t *file,
    size_t	buflen)
{
    char *buf, *line, *tok, *line1;
    size_t lsize;
    char *uqname;
    int in_quotes;
    char *saveptr = NULL;

    /* put the buffer into a writable chunk of memory and nul-term it */
    buf = alloc(buflen + 1);
    memcpy(buf, buffer, buflen);
    buf[buflen] = '\0';
    fh_init(file); 

    /* extract the first unquoted line */
    in_quotes = 0;
    for (line = buf, lsize = 0; lsize < buflen; line++) {
	if ((*line == '\n') && !in_quotes)
	    break;

	if (*line == '"') {
	    in_quotes = !in_quotes;
	} else if ((*line == '\\') && (*(line + 1) == '"')) {
	    line++;
	    lsize++;
	}
	lsize++;
    }
    *line = '\0';
    line1 = alloc(lsize + 1);
    strncpy(line1, buf, lsize);
    line1[lsize] = '\0';
    *line = '\n';

    tok = strtok_r(line1, " ", &saveptr);
    if (tok == NULL) {
        g_fprintf(stderr, _("%s: Empty amanda header: buflen=%zu lsize=%zu\n"), get_pname(),
	    buflen, 
	    lsize);
	hexdump(buffer, lsize);
	strange_header(file, buffer, buflen, _("<Non-empty line>"), tok);
	goto out;
    }

    if (strcmp(tok, "NETDUMP:") != 0 && strcmp(tok, "AMANDA:") != 0) {
	amfree(buf);
	file->type = F_UNKNOWN;
	amfree(line1);
	return;
    }

    tok = strtok_r(NULL, " ", &saveptr);
    if (tok == NULL) {
	strange_header(file, buffer, buflen, _("<file type>"), tok);
	goto out;
    }
    file->type = str2filetype(tok);
    
    switch (file->type) {
    case F_TAPESTART:
	tok = strtok_r(NULL, " ", &saveptr);
	if ((tok == NULL) || (strcmp(tok, "DATE") != 0)) {
	    strange_header(file, buffer, buflen, "DATE", tok);
	    goto out;
	}

	tok = strtok_r(NULL, " ", &saveptr);
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<date stamp>"), tok);
	    goto out;
	}
	strncpy(file->datestamp, tok, SIZEOF(file->datestamp) - 1);

	tok = strtok_r(NULL, " ", &saveptr);
	if ((tok == NULL) || (strcmp(tok, "TAPE") != 0)) {
	    strange_header(file, buffer, buflen, "TAPE", tok);
	    goto out;
	}

	tok = strtok_r(NULL, " ", &saveptr);
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<file type>"), tok);
	    goto out;
	}
	strncpy(file->name, tok, SIZEOF(file->name) - 1);
	break;

    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
    case F_SPLIT_DUMPFILE:
	tok = strtok_r(NULL, " ", &saveptr);
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<date stamp>"), tok);
	    goto out;
	}
	strncpy(file->datestamp, tok, SIZEOF(file->datestamp) - 1);

	tok = strtok_r(NULL, " ", &saveptr);
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<file name>"), tok);
	    goto out;
	}
	strncpy(file->name, tok, SIZEOF(file->name) - 1);

	tok = strquotedstr(&saveptr);
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<disk name>"), tok);
	    goto out;
	}
	uqname = unquote_string(tok);
	strncpy(file->disk, uqname, SIZEOF(file->disk) - 1);
 	amfree(uqname);
	
	if(file->type == F_SPLIT_DUMPFILE) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL || strcmp(tok, "part") != 0) {
		strange_header(file, buffer, buflen, "part", tok);
		goto out;
	    }

	    tok = strtok_r(NULL, "/", &saveptr);
	    if ((tok == NULL) || (sscanf(tok, "%d", &file->partnum) != 1)) {
		strange_header(file, buffer, buflen, _("<part num param>"), tok);
		goto out;
	    }

	    /* If totalparts == -1, then the original dump was done in 
 	       streaming mode (no holding disk), thus we don't know how 
               many parts there are. */
	    tok = strtok_r(NULL, " ", &saveptr);
            if((tok == NULL) || (sscanf(tok, "%d", &file->totalparts) != 1)) {
		strange_header(file, buffer, buflen, _("<total parts param>"), tok);
		goto out;
	    }
	}

	tok = strtok_r(NULL, " ", &saveptr);
	if ((tok == NULL) || (strcmp(tok, "lev") != 0)) {
	    strange_header(file, buffer, buflen, "lev", tok);
	    goto out;
	}

	tok = strtok_r(NULL, " ", &saveptr);
	if ((tok == NULL) || (sscanf(tok, "%d", &file->dumplevel) != 1)) {
	    strange_header(file, buffer, buflen, _("<dump level param>"), tok);
	    goto out;
	}

	tok = strtok_r(NULL, " ", &saveptr);
	if ((tok == NULL) || (strcmp(tok, "comp") != 0)) {
	    strange_header(file, buffer, buflen, "comp", tok);
	    goto out;
	}

	tok = strtok_r(NULL, " ", &saveptr);
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<comp param>"), tok);
	    goto out;
	}
	strncpy(file->comp_suffix, tok, SIZEOF(file->comp_suffix) - 1);

	file->compressed = strcmp(file->comp_suffix, "N");
	/* compatibility with pre-2.2 amanda */
	if (strcmp(file->comp_suffix, "C") == 0)
	    strncpy(file->comp_suffix, ".Z", SIZEOF(file->comp_suffix) - 1);
	       
	tok = strtok_r(NULL, " ", &saveptr);
        /* "program" is optional */
        if (tok == NULL || strcmp(tok, "program") != 0) {
	    break;
	}

        tok = strtok_r(NULL, " ", &saveptr);
        if (tok == NULL) {
	    strange_header(file, buffer, buflen, _("<program name>"), tok);
	    goto out;
	}
        strncpy(file->program, tok, SIZEOF(file->program) - 1);
        if (file->program[0] == '\0')
            strncpy(file->program, "RESTORE", SIZEOF(file->program) - 1);

	if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL)
             break;          /* reached the end of the buffer */

	/* "encryption" is optional */
	if (BSTRNCMP(tok, "crypt") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen, _("<crypt param>"), tok);
		goto out;
	    }
	    strncpy(file->encrypt_suffix, tok,
		    SIZEOF(file->encrypt_suffix) - 1);
	    file->encrypted = BSTRNCMP(file->encrypt_suffix, "N");
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL)
		break;
	}

	/* "srvcompprog" is optional */
	if (BSTRNCMP(tok, "server_custom_compress") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				_("<server custom compress param>"), tok);
		goto out;
	    }
	    strncpy(file->srvcompprog, tok, SIZEOF(file->srvcompprog) - 1);
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL)
		break;      
	}

	/* "clntcompprog" is optional */
	if (BSTRNCMP(tok, "client_custom_compress") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				_("<client custom compress param>"), tok);
		goto out;
	    }
	    strncpy(file->clntcompprog, tok, SIZEOF(file->clntcompprog) - 1);
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL)
		break;
	}

	/* "srv_encrypt" is optional */
	if (BSTRNCMP(tok, "server_encrypt") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				_("<server encrypt param>"), tok);
		goto out;
	    }
	    strncpy(file->srv_encrypt, tok, SIZEOF(file->srv_encrypt) - 1);
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL) 
		break;
	}

	/* "clnt_encrypt" is optional */
	if (BSTRNCMP(tok, "client_encrypt") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				_("<client encrypt param>"), tok);
		goto out;
	    }
	    strncpy(file->clnt_encrypt, tok, SIZEOF(file->clnt_encrypt) - 1);
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL) 
		break;
	}

	/* "srv_decrypt_opt" is optional */
	if (BSTRNCMP(tok, "server_decrypt_option") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				_("<server decrypt param>"), tok);
		goto out;
	    }
	    strncpy(file->srv_decrypt_opt, tok,
		    SIZEOF(file->srv_decrypt_opt) - 1);
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL) 
		break;
	}

	/* "clnt_decrypt_opt" is optional */
	if (BSTRNCMP(tok, "client_decrypt_option") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				_("<client decrypt param>"), tok);
		goto out;
	    }
	    strncpy(file->clnt_decrypt_opt, tok,
		    SIZEOF(file->clnt_decrypt_opt) - 1);
	    if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL) 
		break;
	}
      break;
      

    case F_TAPEEND:
	tok = strtok_r(NULL, " ", &saveptr);
	/* DATE is optional */
	if (tok != NULL) {
	    if (strcmp(tok, "DATE") == 0) {
		tok = strtok_r(NULL, " ", &saveptr);
		if(tok == NULL)
		    file->datestamp[0] = '\0';
		else
		    strncpy(file->datestamp, tok, SIZEOF(file->datestamp) - 1);
	    } else {
		strange_header(file, buffer, buflen, _("<DATE>"), tok);
	   }
	} else {
	    file->datestamp[0] = '\0';
	}
	break;

    case F_NOOP:
	/* nothing follows */
	break;

    default:
	strange_header(file, buffer, buflen,
		_("TAPESTART|DUMPFILE|CONT_DUMPFILE|SPLIT_DUMPFILE|TAPEEND|NOOP"), tok);
	goto out;
    }

    (void)strtok_r(buf, "\n", &saveptr); /* this is the first line */
    /* iterate through the rest of the lines */
    while ((line = strtok_r(NULL, "\n", &saveptr)) != NULL) {
#define SC "CONT_FILENAME="
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0) {
	    line += SIZEOF(SC) - 1;
	    strncpy(file->cont_filename, line,
		    SIZEOF(file->cont_filename) - 1);
	    continue;
	}
#undef SC

#define SC "PARTIAL="
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0) {
	    line += SIZEOF(SC) - 1;
	    file->is_partial = !strcasecmp(line, "yes");
	    continue;
	}
#undef SC
#define SC "APPLICATION="
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0) {
	    line += SIZEOF(SC) - 1;
	    strncpy(file->application, line,
		    SIZEOF(file->application) - 1);
	    continue;
	}
#undef SC

#define SC "DLE="
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0) {
	    line += SIZEOF(SC) - 1;
	    file->dle_str = parse_heredoc(line, &saveptr);
	}
#undef SC

#define SC _("To restore, position tape at start of file and run:")
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0)
	    continue;
#undef SC

#define SC "\tdd if=<tape> "
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0) {
	    char *cmd1, *cmd2, *cmd3=NULL;

	    /* skip over dd command */
	    if ((cmd1 = strchr(line, '|')) == NULL) {

	        strncpy(file->recover_cmd, "BUG",
		        SIZEOF(file->recover_cmd) - 1);
	        continue;
	    }
	    *cmd1++ = '\0';

	    /* block out first pipeline command */
	    if ((cmd2 = strchr(cmd1, '|')) != NULL) {
	      *cmd2++ = '\0';
	      if ((cmd3 = strchr(cmd2, '|')) != NULL)
		*cmd3++ = '\0';
	    }
	   
	    /* clean up some extra spaces in various fields */
	    chomp(cmd1);
	    chomp(cmd2);
	    chomp(cmd3);

	    /* three cmds: decrypt    | uncompress | recover
	     * two   cmds: uncompress | recover
	     * XXX note that if there are two cmds, the first one 
	     * XXX could be either uncompress or decrypt. Since no
	     * XXX code actually call uncompress_cmd/decrypt_cmd, it's ok
	     * XXX for header information.
	     * one   cmds: recover
	     */

	    if ( cmd3 == NULL) {
	      if (cmd2 == NULL) {
		strncpy(file->recover_cmd, cmd1,
			SIZEOF(file->recover_cmd) - 1);
	      } else {
		g_snprintf(file->uncompress_cmd,
			 SIZEOF(file->uncompress_cmd), "%s |", cmd1);
		strncpy(file->recover_cmd, cmd2,
			SIZEOF(file->recover_cmd) - 1);
	      }
	    } else {    /* cmd3 presents:  decrypt | uncompress | recover */
	      g_snprintf(file->decrypt_cmd,
		       SIZEOF(file->decrypt_cmd), "%s |", cmd1);
	      g_snprintf(file->uncompress_cmd,
		       SIZEOF(file->uncompress_cmd), "%s |", cmd2);
	      strncpy(file->recover_cmd, cmd3,
		      SIZEOF(file->recover_cmd) - 1);
	    }
	    continue;
	}
#undef SC
	/* XXX complain about weird lines? */
    }

out:
    amfree(buf);
    amfree(line1);
}

void
dump_dumpfile_t(
    const dumpfile_t *file)
{
	dbprintf(_("Contents of *(dumpfile_t *)%p:\n"), file);
	dbprintf(_("    type             = %d (%s)\n"),
			file->type, filetype2str(file->type));
	dbprintf(_("    datestamp        = '%s'\n"), file->datestamp);
	dbprintf(_("    dumplevel        = %d\n"), file->dumplevel);
	dbprintf(_("    compressed       = %d\n"), file->compressed);
	dbprintf(_("    encrypted        = %d\n"), file->encrypted);
	dbprintf(_("    comp_suffix      = '%s'\n"), file->comp_suffix);
	dbprintf(_("    encrypt_suffix   = '%s'\n"), file->encrypt_suffix);
	dbprintf(_("    name             = '%s'\n"), file->name);
	dbprintf(_("    disk             = '%s'\n"), file->disk);
	dbprintf(_("    program          = '%s'\n"), file->program);
	dbprintf(_("    application      = '%s'\n"), file->application);
	dbprintf(_("    srvcompprog      = '%s'\n"), file->srvcompprog);
	dbprintf(_("    clntcompprog     = '%s'\n"), file->clntcompprog);
	dbprintf(_("    srv_encrypt      = '%s'\n"), file->srv_encrypt);
	dbprintf(_("    clnt_encrypt     = '%s'\n"), file->clnt_encrypt);
	dbprintf(_("    recover_cmd      = '%s'\n"), file->recover_cmd);
	dbprintf(_("    uncompress_cmd   = '%s'\n"), file->uncompress_cmd);
	dbprintf(_("    encrypt_cmd      = '%s'\n"), file->encrypt_cmd);
	dbprintf(_("    decrypt_cmd      = '%s'\n"), file->decrypt_cmd);
	dbprintf(_("    srv_decrypt_opt  = '%s'\n"), file->srv_decrypt_opt);
	dbprintf(_("    clnt_decrypt_opt = '%s'\n"), file->clnt_decrypt_opt);
	dbprintf(_("    cont_filename    = '%s'\n"), file->cont_filename);
	if (file->dle_str)
	    dbprintf(_("    dle_str          = %s\n"), file->dle_str);
	else
	    dbprintf(_("    dle_str          = (null)\n"));
	dbprintf(_("    is_partial       = %d\n"), file->is_partial);
	dbprintf(_("    partnum          = %d\n"), file->partnum);
	dbprintf(_("    totalparts       = %d\n"), file->totalparts);
	if (file->blocksize)
	    dbprintf(_("    blocksize        = %zu\n"), file->blocksize);
}

static void
validate_name(
    const char *name)
{
	if (strlen(name) == 0) {
	    error(_("Invalid name '%s'\n"), name);
	    /*NOTREACHED*/
	}
}

static void
validate_datestamp(
    const char *datestamp)
{
	if (strcmp(datestamp, "X") == 0) {
	    return;
	}

	if ((strlen(datestamp) == 8) && match("^[0-9]{8}$", datestamp)) {
	    return;
	}
	if ((strlen(datestamp) == 14) && match("^[0-9]{14}$", datestamp)) {
	    return;
	}
	error(_("Invalid datestamp '%s'\n"), datestamp);
	/*NOTREACHED*/
}

static void
validate_parts(
    const int partnum,
    const int totalparts)
{
	if (partnum < 1) {
	    error(_("Invalid partnum (%d)\n"), partnum);
	    /*NOTREACHED*/
	}

	if (partnum > totalparts && totalparts >= 0) {
	    error(_("Invalid partnum (%d) > totalparts (%d)\n"),
			partnum, totalparts);
	    /*NOTREACHED*/
	}
}

char *
build_header(const dumpfile_t * file, size_t *size, size_t max_size)
{
    GString *rval, *split_data;
    char *qname;
    char *program;
    size_t min_size;

    min_size = size? *size : max_size;
    dbprintf(_("Building type %s header of %zu-%zu bytes with name='%s' disk='%s' dumplevel=%d and blocksize=%zu\n"),
	    filetype2str(file->type), min_size, max_size,
	    file->name, file->disk, file->dumplevel, file->blocksize);

    rval = g_string_sized_new(min_size);
    split_data = g_string_sized_new(10);
    
    switch (file->type) {
    case F_TAPESTART:
	validate_name(file->name);
	validate_datestamp(file->datestamp);
        g_string_printf(rval, 
                        "AMANDA: TAPESTART DATE %s TAPE %s\n\014\n",
                        file->datestamp, file->name);
	break;
        
    case F_SPLIT_DUMPFILE:
	validate_parts(file->partnum, file->totalparts);
        g_string_printf(split_data,
                        " part %d/%d ", file->partnum, file->totalparts);
        /* FALLTHROUGH */
	

    case F_CONT_DUMPFILE:
    case F_DUMPFILE :
	validate_name(file->name);
	validate_datestamp(file->datestamp);
	qname = quote_string(file->disk);
	program = stralloc(file->program);
	if (match("^.*[.][Ee][Xx][Ee]$", program)) {
		/* Trim ".exe" from program name */
		program[strlen(program) - strlen(".exe")] = '\0';
	}
        g_string_printf(rval, 
                        "AMANDA: %s %s %s %s %s lev %d comp %s program %s",
                        filetype2str(file->type),
                        file->datestamp, file->name, qname,
                        split_data->str,
                        file->dumplevel, file->comp_suffix, program); 
	amfree(program);
	amfree(qname);

        /* only output crypt if it's enabled */
	if (strcmp(file->encrypt_suffix, "enc") == 0) {
            g_string_append_printf(rval, " crypt %s", file->encrypt_suffix);
	}

	if (*file->srvcompprog) {
            g_string_append_printf(rval, " server_custom_compress %s", 
                                   file->srvcompprog);
	} else if (*file->clntcompprog) {
            g_string_append_printf(rval, " client_custom_compress %s",
                                   file->clntcompprog);
	} 
        
	if (*file->srv_encrypt) {
            g_string_append_printf(rval, " server_encrypt %s",
                                   file->srv_encrypt);
	} else if (*file->clnt_encrypt) {
            g_string_append_printf(rval, " client_encrypt %s",
                                   file->clnt_encrypt);
	} 
        
	if (*file->srv_decrypt_opt) {
            g_string_append_printf(rval, " server_decrypt_option %s",
                                   file->srv_decrypt_opt);
        } else if (*file->clnt_decrypt_opt) {
            g_string_append_printf(rval, " client_decrypt_option %s",
                                   file->clnt_decrypt_opt);
	} 
        
        g_string_append_printf(rval, "\n");
        
	if (file->cont_filename[0] != '\0') {
            g_string_append_printf(rval, "CONT_FILENAME=%s\n",
                                   file->cont_filename);
	}
	if (file->application[0] != '\0') {
            g_string_append_printf(rval, "APPLICATION=%s\n", file->application);
	}
	if (file->is_partial != 0) {
            g_string_append_printf(rval, "PARTIAL=YES\n");
	}
	if (file->dle_str && strlen(file->dle_str) < max_size-2048) {
	    char *heredoc = quote_heredoc(file->dle_str, "ENDDLE");
	    g_string_append_printf(rval, "DLE=%s\n", heredoc);
	    amfree(heredoc);
	}
        
        g_string_append_printf(rval,
	    _("To restore, position tape at start of file and run:\n"));

        g_string_append_printf(rval, "\tdd if=<tape> ");
	if (file->blocksize)
	    g_string_append_printf(rval, "bs=%zuk ",
				   file->blocksize / 1024);
	g_string_append_printf(rval, "skip=1 | ");
	if (*file->decrypt_cmd)
	    g_string_append_printf(rval, "%s ", file->decrypt_cmd);
	if (*file->uncompress_cmd)
	    g_string_append_printf(rval, "%s ", file->uncompress_cmd);
	if (*file->recover_cmd)
	    g_string_append_printf(rval, "%s ", file->recover_cmd);
	/* \014 == ^L == form feed */
	g_string_append_printf(rval, "\n\014\n");
	break;

    case F_TAPEEND:
	validate_datestamp(file->datestamp);
        g_string_printf(rval, "AMANDA: TAPEEND DATE %s\n\014\n",
                        file->datestamp);
	break;

    case F_NOOP:
        g_string_printf(rval, "AMANDA: NOOP\n\014\n");
	break;

    case F_UNKNOWN:
    case F_EMPTY:
    case F_WEIRD:
    default:
	error(_("Invalid header type: %d (%s)"),
		file->type, filetype2str(file->type));
	/*NOTREACHED*/
    }
    
    g_string_free(split_data, TRUE);

    /* is it too big? */
    if (rval->len > max_size) {
	g_debug("header is larger than %zu bytes -- cannot create", max_size);
	g_string_free(rval, TRUE);
	return NULL;
    }

    /* Clear extra bytes. */
    if (rval->len < min_size) {
        bzero(rval->str + rval->len, rval->allocated_len - rval->len);
    }
    if (size) {
	*size = MAX(min_size, (size_t)rval->len);
    }
    return g_string_free(rval, FALSE);
}

/*
 * Prints the contents of the file structure.
 */
void
print_header(
    FILE *		outf,
    const dumpfile_t *	file)
{
    char *qdisk;
    char number[NUM_STR_SIZE*2];

    switch(file->type) {
    case F_EMPTY:
	g_fprintf(outf, _("EMPTY file\n"));
	break;

    case F_UNKNOWN:
	g_fprintf(outf, _("UNKNOWN file\n"));
	break;

    case F_WEIRD:
	g_fprintf(outf, _("WEIRD file\n"));
	break;

    case F_TAPESTART:
	g_fprintf(outf, _("start of tape: date %s label %s\n"),
	       file->datestamp, file->name);
	break;

    case F_NOOP:
	g_fprintf(outf, _("NOOP file\n"));
	break;

    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
	qdisk = quote_string(file->disk);
	g_fprintf(outf, "%s: date %s host %s disk %s lev %d comp %s",
	    filetype2str(file->type), file->datestamp, file->name,
	    qdisk, file->dumplevel, file->comp_suffix);
	if (*file->program)
	    g_fprintf(outf, " program %s", file->program);
	if (strcmp(file->encrypt_suffix, "enc") == 0)
	    g_fprintf(outf, " crypt %s", file->encrypt_suffix);
	if (*file->srvcompprog)
	    g_fprintf(outf, " server_custom_compress %s", file->srvcompprog);
	if (*file->clntcompprog)
	    g_fprintf(outf, " client_custom_compress %s", file->clntcompprog);
	if (*file->srv_encrypt)
	    g_fprintf(outf, " server_encrypt %s", file->srv_encrypt);
	if (*file->clnt_encrypt)
	    g_fprintf(outf, " client_encrypt %s", file->clnt_encrypt);
	if (*file->srv_decrypt_opt)
	    g_fprintf(outf, " server_decrypt_option %s", file->srv_decrypt_opt);
	if (*file->clnt_decrypt_opt)
	    g_fprintf(outf, " client_decrypt_option %s", file->clnt_decrypt_opt);
	g_fprintf(outf, "\n");
	amfree(qdisk);
	break;

    case F_SPLIT_DUMPFILE:
        if(file->totalparts > 0){
            g_snprintf(number, SIZEOF(number), "%d", file->totalparts);
        }   
        else g_snprintf(number, SIZEOF(number), "UNKNOWN");
	qdisk = quote_string(file->disk);
        g_fprintf(outf, "split dumpfile: date %s host %s disk %s part %d/%s lev %d comp %s",
                      file->datestamp, file->name, qdisk, file->partnum,
                      number, file->dumplevel, file->comp_suffix);
        if (*file->program)
            g_fprintf(outf, " program %s",file->program);
	if (strcmp(file->encrypt_suffix, "enc") == 0)
	    g_fprintf(outf, " crypt %s", file->encrypt_suffix);
	if (*file->srvcompprog)
	    g_fprintf(outf, " server_custom_compress %s", file->srvcompprog);
	if (*file->clntcompprog)
	    g_fprintf(outf, " client_custom_compress %s", file->clntcompprog);
	if (*file->srv_encrypt)
	    g_fprintf(outf, " server_encrypt %s", file->srv_encrypt);
	if (*file->clnt_encrypt)
	    g_fprintf(outf, " client_encrypt %s", file->clnt_encrypt);
	if (*file->srv_decrypt_opt)
	    g_fprintf(outf, " server_decrypt_option %s", file->srv_decrypt_opt);
	if (*file->clnt_decrypt_opt)
	    g_fprintf(outf, " client_decrypt_option %s", file->clnt_decrypt_opt);
        g_fprintf(outf, "\n");
	amfree(qdisk);
        break;

    case F_TAPEEND:
	g_fprintf(outf, "end of tape: date %s\n", file->datestamp);
	break;
    }
}

int
known_compress_type(
    const dumpfile_t *	file)
{
    if(strcmp(file->comp_suffix, ".Z") == 0)
	return 1;
#ifdef HAVE_GZIP
    if(strcmp(file->comp_suffix, ".gz") == 0)
	return 1;
#endif
    if(strcmp(file->comp_suffix, "cust") == 0)
	return 1;
    return 0;
}

static const struct {
    filetype_t type;
    const char *str;
} filetypetab[] = {
    { F_UNKNOWN, "UNKNOWN" },
    { F_WEIRD, "WEIRD" },
    { F_TAPESTART, "TAPESTART" },
    { F_TAPEEND,  "TAPEEND" },
    { F_DUMPFILE, "FILE" },
    { F_CONT_DUMPFILE, "CONT_FILE" },
    { F_SPLIT_DUMPFILE, "SPLIT_FILE" },
    { F_NOOP, "NOOP" }
};
#define	NFILETYPES	(size_t)(sizeof(filetypetab) / sizeof(filetypetab[0]))

static const char *
filetype2str(
    filetype_t	type)
{
    int i;

    for (i = 0; i < (int)NFILETYPES; i++)
	if (filetypetab[i].type == type)
	    return (filetypetab[i].str);
    return ("UNKNOWN");
}

static filetype_t
str2filetype(
    const char *str)
{
    int i;

    for (i = 0; i < (int)NFILETYPES; i++)
	if (strcmp(filetypetab[i].str, str) == 0)
	    return (filetypetab[i].type);
    return (F_UNKNOWN);
}

gboolean headers_are_equal(dumpfile_t * a, dumpfile_t * b) {
    if (a == NULL && b == NULL)
        return TRUE;

    if (a == NULL || b == NULL)
        return FALSE;

    if (a->type != b->type) return FALSE;
    if (strcmp(a->datestamp, b->datestamp)) return FALSE;
    if (a->dumplevel != b->dumplevel) return FALSE;
    if (a->compressed != b->compressed) return FALSE;
    if (a->encrypted != b->encrypted) return FALSE;
    if (strcmp(a->comp_suffix, b->comp_suffix)) return FALSE;
    if (strcmp(a->encrypt_suffix, b->encrypt_suffix)) return FALSE;
    if (strcmp(a->name, b->name)) return FALSE;
    if (strcmp(a->disk, b->disk)) return FALSE;
    if (strcmp(a->program, b->program)) return FALSE;
    if (strcmp(a->application, b->application)) return FALSE;
    if (strcmp(a->srvcompprog, b->srvcompprog)) return FALSE;
    if (strcmp(a->clntcompprog, b->clntcompprog)) return FALSE;
    if (strcmp(a->srv_encrypt, b->srv_encrypt)) return FALSE;
    if (strcmp(a->clnt_encrypt, b->clnt_encrypt)) return FALSE;
    if (strcmp(a->recover_cmd, b->recover_cmd)) return FALSE;
    if (strcmp(a->uncompress_cmd, b->uncompress_cmd)) return FALSE;
    if (strcmp(a->encrypt_cmd, b->encrypt_cmd)) return FALSE;
    if (strcmp(a->decrypt_cmd, b->decrypt_cmd)) return FALSE;
    if (strcmp(a->srv_decrypt_opt, b->srv_decrypt_opt)) return FALSE;
    if (strcmp(a->clnt_decrypt_opt, b->clnt_decrypt_opt)) return FALSE;
    if (strcmp(a->cont_filename, b->cont_filename)) return FALSE;
    if (a->dle_str != b->dle_str && a->dle_str && b->dle_str
	&& strcmp(a->dle_str, b->dle_str)) return FALSE;
    if (a->is_partial != b->is_partial) return FALSE;
    if (a->partnum != b->partnum) return FALSE;
    if (a->totalparts != b->totalparts) return FALSE;
    if (a->blocksize != b->blocksize) return FALSE;

    return TRUE; /* ok, they're the same */
}

dumpfile_t * dumpfile_copy(dumpfile_t* source) {
    dumpfile_t* rval = malloc(sizeof(dumpfile_t));
    memcpy(rval, source, sizeof(dumpfile_t));
    if (rval->dle_str) rval->dle_str = stralloc(rval->dle_str);
    return rval;
}

void
dumpfile_copy_in_place(
    dumpfile_t *dest,
    dumpfile_t* source)
{
    memcpy(dest, source, sizeof(dumpfile_t));
    if (dest->dle_str) dest->dle_str = stralloc(dest->dle_str);
}

void dumpfile_free_data(dumpfile_t* info) {
    if (info) {
	amfree(info->dle_str);
    }
}

void dumpfile_free(dumpfile_t* info) {
    dumpfile_free_data(info);
    amfree(info);
}

static ssize_t
hexdump(
    const char *buffer,
    size_t	len)
{
    ssize_t rc = -1;

    FILE *stream = popen("od -c -x -", "w");
	
    if (stream != NULL) {
	fflush(stdout);
	rc = (ssize_t)fwrite(buffer, len, 1, stream);
	if (ferror(stream))
	    rc = -1;
	pclose(stream);
    }
    return rc;
}

static char *quote_heredoc(
    char  *text,
    char  *delimiter_prefix)
{
    char *delimiter = stralloc(delimiter_prefix);
    int delimiter_n = 0;
    int delimiter_len = strlen(delimiter);
    char *quoted;

    /* keep picking delimiters until we find one that's not a line in TEXT */
    while (1) {
	char *line = text;
	char *c = text;
	gboolean found_delimiter = FALSE;

	while (1) {
	    if (*c == '\n' || *c == '\0') {
		int linelen = c - line;
		if (linelen == delimiter_len && 0 == strncmp(line, delimiter, linelen)) {
		    found_delimiter = TRUE;
		    break;
		}
		line = c+1;
	    }
	    if (!*c) break;
	    c++;
	}

	if (!found_delimiter)
	    break;

	delimiter = newvstrallocf(delimiter, "%s%d", delimiter_prefix, ++delimiter_n);
	delimiter_len = strlen(delimiter);
    }

    /* we have a delimiter .. now use it */
    quoted = vstrallocf("<<%s\n%s\n%s", delimiter, text, delimiter);
    amfree(delimiter);
    return quoted;
}

static char *parse_heredoc(
    char  *line,
    char **saveptr)
{
    char *result = NULL;

    if (strncmp(line, "<<", 2) == 0) {
	char *keyword = line+2;
	char *new_line;

	while((new_line = strtok_r(NULL, "\n", saveptr)) != NULL &&
	      strcmp(new_line, keyword) != 0) {
	    result = vstrextend(&result, new_line, "\n", NULL);
	}
	/* remove latest '\n' */
	if (strlen(result) > 0)
	    result[strlen(result)-1] = '\0';
    } else {
	result = stralloc(line);
    }
    return result;
}
