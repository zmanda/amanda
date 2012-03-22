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
#include "match.h"
#include <glib.h>
#include "util.h"

static const char *	filetype2str(filetype_t);
static filetype_t	str2filetype(const char *);
static void		strange_header(dumpfile_t *, const char *,
				size_t, const char *, const char *);
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

    g_debug("strange amanda header: \"%.*s\"", (int)buflen, buffer);
    g_debug("Expected: \"%s\"  Actual: \"%s\"", expected, actual);

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
        g_debug("Empty amanda header: buflen=%zu lsize=%zu buf='%s'", buflen, lsize, buf);
	strange_header(file, buffer, buflen, _("<Non-empty line>"), tok);
	goto out;
    }

    if (strcmp(tok, "NETDUMP:") != 0 && strcmp(tok, "AMANDA:") != 0) {
	amfree(buf);
	file->type = F_WEIRD;
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
	} else if (file->type == F_DUMPFILE) {
	    /* only one part in this dump, so call it partnum 1 */
	    file->partnum = 1;
	    file->totalparts = 1;
	} else {
	    file->partnum = 0;
	    file->totalparts = 0;
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

	file->compressed = (0 != strcmp(file->comp_suffix, "N"));
	if (file->compressed) {
	    /* compatibility with pre-2.2 amanda */
	    if (strcmp(file->comp_suffix, "C") == 0)
		strncpy(file->comp_suffix, ".Z", SIZEOF(file->comp_suffix) - 1);
	} else {
	    strcpy(file->comp_suffix, "");
	}

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

	if ((tok = strtok_r(NULL, " ", &saveptr)) == NULL)
             break;          /* reached the end of the buffer */

	/* encryption is optional */
	if (BSTRNCMP(tok, "crypt") == 0) {
	    tok = strtok_r(NULL, " ", &saveptr);
	    if (tok == NULL) {
		strange_header(file, buffer, buflen, _("<crypt param>"), tok);
		goto out;
	    }
	    strncpy(file->encrypt_suffix, tok,
		    SIZEOF(file->encrypt_suffix) - 1);
	    file->encrypted = 1;

	    /* for compatibility with who-knows-what, allow "comp N" to be
	     * equivalent to no compression */
	    if (0 == BSTRNCMP(file->encrypt_suffix, "N")) {
		file->encrypted = 0;
		strcpy(file->encrypt_suffix, "");
	    }

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

#define SC "ORIGSIZE="
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0) {
	    line += SIZEOF(SC) - 1;
	    file->orig_size = OFF_T_ATOI(line);
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
	g_debug(_("Contents of *(dumpfile_t *)%p:"), file);
	g_debug(_("    type             = %d (%s)"),
			file->type, filetype2str(file->type));
	g_debug(_("    datestamp        = '%s'"), file->datestamp);
	g_debug(_("    dumplevel        = %d"), file->dumplevel);
	g_debug(_("    compressed       = %d"), file->compressed);
	g_debug(_("    encrypted        = %d"), file->encrypted);
	g_debug(_("    comp_suffix      = '%s'"), file->comp_suffix);
	g_debug(_("    encrypt_suffix   = '%s'"), file->encrypt_suffix);
	g_debug(_("    name             = '%s'"), file->name);
	g_debug(_("    disk             = '%s'"), file->disk);
	g_debug(_("    program          = '%s'"), file->program);
	g_debug(_("    application      = '%s'"), file->application);
	g_debug(_("    srvcompprog      = '%s'"), file->srvcompprog);
	g_debug(_("    clntcompprog     = '%s'"), file->clntcompprog);
	g_debug(_("    srv_encrypt      = '%s'"), file->srv_encrypt);
	g_debug(_("    clnt_encrypt     = '%s'"), file->clnt_encrypt);
	g_debug(_("    recover_cmd      = '%s'"), file->recover_cmd);
	g_debug(_("    uncompress_cmd   = '%s'"), file->uncompress_cmd);
	g_debug(_("    decrypt_cmd      = '%s'"), file->decrypt_cmd);
	g_debug(_("    srv_decrypt_opt  = '%s'"), file->srv_decrypt_opt);
	g_debug(_("    clnt_decrypt_opt = '%s'"), file->clnt_decrypt_opt);
	g_debug(_("    cont_filename    = '%s'"), file->cont_filename);
	if (file->dle_str)
	    g_debug(_("    dle_str          = %s"), file->dle_str);
	else
	    g_debug(_("    dle_str          = (null)"));
	g_debug(_("    is_partial       = %d"), file->is_partial);
	g_debug(_("    partnum          = %d"), file->partnum);
	g_debug(_("    totalparts       = %d"), file->totalparts);
	if (file->blocksize)
	    g_debug(_("    blocksize        = %zu"), file->blocksize);
}

static void
validate_nonempty_str(
    const char *val,
    const char *name)
{
    if (strlen(val) == 0) {
	error(_("Invalid %s '%s'\n"), name, val);
	/*NOTREACHED*/
    }
}

static void
validate_not_both(
    const char *val1, const char *val2,
    const char *name1, const char *name2)
{
    if (*val1 && *val2) {
	error("cannot set both %s and %s\n", name1, name2);
    }
}

static void
validate_no_space(
    const char *val,
    const char *name)
{
    if (strchr(val, ' ') != NULL) {
	error(_("%s cannot contain spaces\n"), name);
	/*NOTREACHED*/
    }
}

static void
validate_pipe_cmd(
	const char *cmd,
	const char *name)
{
    if (strlen(cmd) && cmd[strlen(cmd)-1] != '|') {
	error("invalid %s (must end with '|'): '%s'\n", name, cmd);
    }
}

static void
validate_encrypt_suffix(
	int encrypted,
	const char *suff)
{
    if (encrypted) {
	if (!suff[0] || (0 == strcmp(suff, "N"))) {
	    error(_("Invalid encrypt_suffix '%s'\n"), suff);
	}
    } else {
	if (suff[0] && (0 != strcmp(suff, "N"))) {
	    error(_("Invalid header: encrypt_suffix '%s' specified but not encrypted\n"), suff);
	}
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
    g_debug(_("Building type %s header of %zu-%zu bytes with name='%s' disk='%s' dumplevel=%d and blocksize=%zu"),
	    filetype2str(file->type), min_size, max_size,
	    file->name, file->disk, file->dumplevel, file->blocksize);

    rval = g_string_sized_new(min_size);
    split_data = g_string_sized_new(10);

    switch (file->type) {
    case F_TAPESTART:
	validate_nonempty_str(file->name, "name");
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
	validate_nonempty_str(file->name, "name");
	validate_nonempty_str(file->program, "program");
	validate_datestamp(file->datestamp);
	validate_encrypt_suffix(file->encrypted, file->encrypt_suffix);
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
                        file->dumplevel,
			file->compressed? file->comp_suffix : "N",
			program); 
	amfree(program);
	amfree(qname);

        /* only output crypt if it's enabled */
	if (file->encrypted) {
	    g_string_append_printf(rval, " crypt %s", file->encrypt_suffix);
	}

	validate_not_both(file->srvcompprog, file->clntcompprog,
			    "srvcompprog", "clntcompprog");
	if (*file->srvcompprog) {
	    validate_no_space(file->srvcompprog, "srvcompprog");
            g_string_append_printf(rval, " server_custom_compress %s",
                                   file->srvcompprog);
	} else if (*file->clntcompprog) {
	    validate_no_space(file->clntcompprog, "clntcompprog");
            g_string_append_printf(rval, " client_custom_compress %s",
                                   file->clntcompprog);
	}

	validate_not_both(file->srv_encrypt, file->clnt_encrypt,
			    "srv_encrypt", "clnt_encrypt");
	if (*file->srv_encrypt) {
	    validate_no_space(file->srv_encrypt, "srv_encrypt");
            g_string_append_printf(rval, " server_encrypt %s",
                                   file->srv_encrypt);
	} else if (*file->clnt_encrypt) {
	    validate_no_space(file->clnt_encrypt, "clnt_encrypt");
            g_string_append_printf(rval, " client_encrypt %s",
                                   file->clnt_encrypt);
	}

	validate_not_both(file->srv_decrypt_opt, file->clnt_decrypt_opt,
			    "srv_decrypt_opt", "clnt_decrypt_opt");
	if (*file->srv_decrypt_opt) {
	    validate_no_space(file->srv_decrypt_opt, "srv_decrypt_opt");
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
	if (file->orig_size > 0) {
	    g_string_append_printf(rval, "ORIGSIZE=%jd\n",
					 (intmax_t)file->orig_size);
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
	if (*file->recover_cmd) {
	    if (*file->decrypt_cmd) {
		validate_pipe_cmd(file->decrypt_cmd, "decrypt_cmd");
		g_string_append_printf(rval, "%s ", file->decrypt_cmd);
	    }
	    if (*file->uncompress_cmd) {
		validate_pipe_cmd(file->uncompress_cmd, "uncompress_cmd");
		g_string_append_printf(rval, "%s ", file->uncompress_cmd);
	    }
	    g_string_append_printf(rval, "%s ", file->recover_cmd);
	} else {
	    if (*file->uncompress_cmd || *file->decrypt_cmd)
		error("cannot specify uncompress_cmd or decrypt_cmd without recover_cmd\n");
	}
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

void
print_header(
    FILE *		outf,
    const dumpfile_t *	file)
{
    char *summ = summarize_header(file);
    g_fprintf(outf, "%s\n", summ);
    g_free(summ);
}

/*
 * Prints the contents of the file structure.
 */
char *
summarize_header(
    const dumpfile_t *	file)
{
    char *qdisk;
    GString *summ;

    switch(file->type) {
    case F_EMPTY:
	return g_strdup(_("EMPTY file"));

    case F_UNKNOWN:
	return g_strdup(_("UNKNOWN file"));

    default:
    case F_WEIRD:
	return g_strdup(_("WEIRD file"));

    case F_TAPESTART:
	return g_strdup_printf(_("start of tape: date %s label %s"),
	       file->datestamp, file->name);

    case F_NOOP:
	return g_strdup(_("NOOP file"));

    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
	qdisk = quote_string(file->disk);
	summ = g_string_new("");
        g_string_printf(summ, "%s: date %s host %s disk %s lev %d comp %s",
	    filetype2str(file->type), file->datestamp, file->name,
	    qdisk, file->dumplevel,
	    file->compressed? file->comp_suffix : "N");
	amfree(qdisk);
	goto add_suffixes;

    case F_SPLIT_DUMPFILE: {
	char totalparts[NUM_STR_SIZE*2];
        if(file->totalparts > 0)
            g_snprintf(totalparts, SIZEOF(totalparts), "%d", file->totalparts);
        else
	    g_snprintf(totalparts, SIZEOF(totalparts), "UNKNOWN");
	qdisk = quote_string(file->disk);
        summ = g_string_new("");
        g_string_printf(summ, "split dumpfile: date %s host %s disk %s"
		      " part %d/%s lev %d comp %s",
                      file->datestamp, file->name, qdisk, file->partnum,
                      totalparts, file->dumplevel,
		      file->compressed? file->comp_suffix : "N");
	amfree(qdisk);
        goto add_suffixes;
    }

    add_suffixes:
	if (*file->program)
	    g_string_append_printf(summ, " program %s", file->program);
	if (strcmp(file->encrypt_suffix, "enc") == 0)
	    g_string_append_printf(summ, " crypt %s", file->encrypt_suffix);
	if (*file->srvcompprog)
	    g_string_append_printf(summ, " server_custom_compress %s", file->srvcompprog);
	if (*file->clntcompprog)
	    g_string_append_printf(summ, " client_custom_compress %s", file->clntcompprog);
	if (*file->srv_encrypt)
	    g_string_append_printf(summ, " server_encrypt %s", file->srv_encrypt);
	if (*file->clnt_encrypt)
	    g_string_append_printf(summ, " client_encrypt %s", file->clnt_encrypt);
	if (*file->srv_decrypt_opt)
	    g_string_append_printf(summ, " server_decrypt_option %s", file->srv_decrypt_opt);
	if (*file->clnt_decrypt_opt)
	    g_string_append_printf(summ, " client_decrypt_option %s", file->clnt_decrypt_opt);
	return g_string_free(summ, FALSE);

    case F_TAPEEND:
	return g_strdup_printf("end of tape: date %s", file->datestamp);
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
	/* make sure we have something */
	if (!result)
	    result = g_strdup("");
	/* remove latest '\n' */
	else if (strlen(result) > 0)
	    result[strlen(result)-1] = '\0';
    } else {
	result = stralloc(line);
    }
    return result;
}
