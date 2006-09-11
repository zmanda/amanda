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
 * $Id: fileheader.c,v 1.40 2006/07/01 00:10:38 paddy_s Exp $
 */

#include "amanda.h"
#include "fileheader.h"

static const char *	filetype2str(filetype_t);
static filetype_t	str2filetype(const char *);
static void		strange_header(dumpfile_t *, const char *,
				size_t, const char *, const char *);

void
fh_init(
    dumpfile_t *file)
{
    memset(file, '\0', SIZEOF(*file));
    file->blocksize = DISK_BLOCK_BYTES;
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

    fprintf(stderr, "%s: strange amanda header: \"%.*s\"\n", get_pname(),
		(int)buflen, buffer);

    fprintf(stderr, "%s: Expected: \"%s\"  Actual: \"%s\"\n", get_pname(),
		expected, actual);

    file->type = F_WEIRD;
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

    /* put the buffer into a writable chunk of memory and nul-term it */
    buf = alloc(buflen + 1);
    memcpy(buf, buffer, buflen);
    buf[buflen] = '\0';
    fh_init(file); 

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

    tok = strtok(line1, " ");
    if (tok == NULL) {
        fprintf(stderr, "%s: Empty amanda header: buflen=" SIZE_T_FMT
	    " lsize=" SIZE_T_FMT "\n", get_pname(),
	    (SIZE_T_FMT_TYPE)buflen, 
	    (SIZE_T_FMT_TYPE)lsize);
	hexdump(buffer, lsize);
	strange_header(file, buffer, buflen, "<Non-empty line>", tok);
	goto out;
    }

    if (strcmp(tok, "NETDUMP:") != 0 && strcmp(tok, "AMANDA:") != 0) {
	amfree(buf);
	file->type = F_UNKNOWN;
	amfree(line1);
	return;
    }

    tok = strtok(NULL, " ");
    if (tok == NULL) {
	strange_header(file, buffer, buflen, "<file type>", tok);
	goto out;
    }
    file->type = str2filetype(tok);
    
    switch (file->type) {
    case F_TAPESTART:
	tok = strtok(NULL, " ");
	if ((tok == NULL) || (strcmp(tok, "DATE") != 0)) {
	    strange_header(file, buffer, buflen, "DATE", tok);
	    goto out;
	}

	tok = strtok(NULL, " ");
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<date stamp>", tok);
	    goto out;
	}
	strncpy(file->datestamp, tok, SIZEOF(file->datestamp) - 1);

	tok = strtok(NULL, " ");
	if ((tok == NULL) || (strcmp(tok, "TAPE") != 0)) {
	    strange_header(file, buffer, buflen, "TAPE", tok);
	    goto out;
	}

	tok = strtok(NULL, " ");
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<file type>", tok);
	    goto out;
	}
	strncpy(file->name, tok, SIZEOF(file->name) - 1);
	break;

    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
    case F_SPLIT_DUMPFILE:
	tok = strtok(NULL, " ");
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<date stamp>", tok);
	    goto out;
	}
	strncpy(file->datestamp, tok, SIZEOF(file->datestamp) - 1);

	tok = strtok(NULL, " ");
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<file name>", tok);
	    goto out;
	}
	strncpy(file->name, tok, SIZEOF(file->name) - 1);

	tok = strquotedstr();
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<disk name>", tok);
	    goto out;
	}
	uqname = unquote_string(tok);
	strncpy(file->disk, uqname, SIZEOF(file->disk) - 1);
 	amfree(uqname);
	
	if(file->type == F_SPLIT_DUMPFILE) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL || strcmp(tok, "part") != 0) {
		strange_header(file, buffer, buflen, "part", tok);
		goto out;
	    }

	    tok = strtok(NULL, "/");
	    if ((tok == NULL) || (sscanf(tok, "%d", &file->partnum) != 1)) {
		strange_header(file, buffer, buflen, "<part num param>", tok);
		goto out;
	    }

	    /* If totalparts == -1, then the original dump was done in 
 	       streaming mode (no holding disk), thus we don't know how 
               many parts there are. */
	    tok = strtok(NULL, " ");
            if((tok == NULL) || (sscanf(tok, "%d", &file->totalparts) != 1)) {
		strange_header(file, buffer, buflen, "<total parts param>", tok);
		goto out;
	    }
	}

	tok = strtok(NULL, " ");
	if ((tok == NULL) || (strcmp(tok, "lev") != 0)) {
	    strange_header(file, buffer, buflen, "lev", tok);
	    goto out;
	}

	tok = strtok(NULL, " ");
	if ((tok == NULL) || (sscanf(tok, "%d", &file->dumplevel) != 1)) {
	    strange_header(file, buffer, buflen, "<dump level param>", tok);
	    goto out;
	}

	tok = strtok(NULL, " ");
	if ((tok == NULL) || (strcmp(tok, "comp") != 0)) {
	    strange_header(file, buffer, buflen, "comp", tok);
	    goto out;
	}

	tok = strtok(NULL, " ");
	if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<comp param>", tok);
	    goto out;
	}
	strncpy(file->comp_suffix, tok, SIZEOF(file->comp_suffix) - 1);

	file->compressed = strcmp(file->comp_suffix, "N");
	/* compatibility with pre-2.2 amanda */
	if (strcmp(file->comp_suffix, "C") == 0)
	    strncpy(file->comp_suffix, ".Z", SIZEOF(file->comp_suffix) - 1);
	       
	tok = strtok(NULL, " ");
        /* "program" is optional */
        if (tok == NULL || strcmp(tok, "program") != 0) {
	    amfree(buf);
	    amfree(line1);
            return;
	}

        tok = strtok(NULL, " ");
        if (tok == NULL) {
	    strange_header(file, buffer, buflen, "<program name>", tok);
	    goto out;
	}
        strncpy(file->program, tok, SIZEOF(file->program) - 1);
        if (file->program[0] == '\0')
            strncpy(file->program, "RESTORE", SIZEOF(file->program) - 1);

	if ((tok = strtok(NULL, " ")) == NULL)
             break;          /* reached the end of the buffer */

	/* "encryption" is optional */
	if (BSTRNCMP(tok, "crypt") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen, "<crypt param>", tok);
		goto out;
	    }
	    strncpy(file->encrypt_suffix, tok,
		    SIZEOF(file->encrypt_suffix) - 1);
	    file->encrypted = BSTRNCMP(file->encrypt_suffix, "N");
	    if ((tok = strtok(NULL, " ")) == NULL)
		break;
	}

	/* "srvcompprog" is optional */
	if (BSTRNCMP(tok, "server_custom_compress") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				"<server custom compress param>", tok);
		goto out;
	    }
	    strncpy(file->srvcompprog, tok, SIZEOF(file->srvcompprog) - 1);
	    if ((tok = strtok(NULL, " ")) == NULL)
		break;      
	}

	/* "clntcompprog" is optional */
	if (BSTRNCMP(tok, "client_custom_compress") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				"<client custom compress param>", tok);
		goto out;
	    }
	    strncpy(file->clntcompprog, tok, SIZEOF(file->clntcompprog) - 1);
	    if ((tok = strtok(NULL, " ")) == NULL)
		break;
	}

	/* "srv_encrypt" is optional */
	if (BSTRNCMP(tok, "server_encrypt") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				"<server encrypt param>", tok);
		goto out;
	    }
	    strncpy(file->srv_encrypt, tok, SIZEOF(file->srv_encrypt) - 1);
	    if ((tok = strtok(NULL, " ")) == NULL) 
		break;
	}

	/* "clnt_encrypt" is optional */
	if (BSTRNCMP(tok, "client_encrypt") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				"<client encrypt param>", tok);
		goto out;
	    }
	    strncpy(file->clnt_encrypt, tok, SIZEOF(file->clnt_encrypt) - 1);
	    if ((tok = strtok(NULL, " ")) == NULL) 
		break;
	}

	/* "srv_decrypt_opt" is optional */
	if (BSTRNCMP(tok, "server_decrypt_option") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				"<server decrypt param>", tok);
		goto out;
	    }
	    strncpy(file->srv_decrypt_opt, tok,
		    SIZEOF(file->srv_decrypt_opt) - 1);
	    if ((tok = strtok(NULL, " ")) == NULL) 
		break;
	}

	/* "clnt_decrypt_opt" is optional */
	if (BSTRNCMP(tok, "client_decrypt_option") == 0) {
	    tok = strtok(NULL, " ");
	    if (tok == NULL) {
		strange_header(file, buffer, buflen,
				"<client decrypt param>", tok);
		goto out;
	    }
	    strncpy(file->clnt_decrypt_opt, tok,
		    SIZEOF(file->clnt_decrypt_opt) - 1);
	    if ((tok = strtok(NULL, " ")) == NULL) 
		break;
	}
      break;
      

    case F_TAPEEND:
	tok = strtok(NULL, " ");
	/* DATE is optional */
	if (tok != NULL) {
	    if (strcmp(tok, "DATE") == 0) {
		tok = strtok(NULL, " ");
		if(tok == NULL)
		    file->datestamp[0] = '\0';
		else
		    strncpy(file->datestamp, tok, SIZEOF(file->datestamp) - 1);
	    } else {
		strange_header(file, buffer, buflen, "<DATE>", tok);
	   }
	} else {
	    file->datestamp[0] = '\0';
	}
	break;

    default:
	strange_header(file, buffer, buflen,
		"TAPESTART|DUMPFILE|CONT_DUMPFILE|SPLIT_DUMPFILE|TAPEEND", tok);
	goto out;
    }

    (void)strtok(buf, "\n"); /* this is the first line */
    /* iterate through the rest of the lines */
    while ((line = strtok(NULL, "\n")) != NULL) {
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

#define SC "To restore, position tape at start of file and run:"
	if (strncmp(line, SC, SIZEOF(SC) - 1) == 0)
	    continue;
#undef SC

#define SC "\tdd if=<tape> bs="
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
		snprintf(file->uncompress_cmd,
			 SIZEOF(file->uncompress_cmd), "%s|", cmd1);
		strncpy(file->recover_cmd, cmd2,
			SIZEOF(file->recover_cmd) - 1);
	      }
	    } else {    /* cmd3 presents:  decrypt | uncompress | recover */
	      snprintf(file->decrypt_cmd,
		       SIZEOF(file->decrypt_cmd), "%s|", cmd1);
	      snprintf(file->uncompress_cmd,
		       SIZEOF(file->uncompress_cmd), "%s|", cmd2);
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
	const char *pname = get_pname();

	dbprintf(("%s: Contents of *(dumpfile_t *)%p:\n", pname, file));
	dbprintf(("%s:     type             = %d (%s)\n", pname,
			file->type, filetype2str(file->type)));
	dbprintf(("%s:     datestamp        = '%s'\n", pname,
			file->datestamp));
	dbprintf(("%s:     dumplevel        = %d\n", pname, file->dumplevel));
	dbprintf(("%s:     compressed       = %d\n", pname, file->compressed));
	dbprintf(("%s:     encrypted        = %d\n", pname, file->encrypted));
	dbprintf(("%s:     comp_suffix      = '%s'\n", pname,
			file->comp_suffix));
	dbprintf(("%s:     encrypt_suffix   = '%s'\n", pname,
			file->encrypt_suffix));
	dbprintf(("%s:     name             = '%s'\n", pname, file->name));
	dbprintf(("%s:     disk             = '%s'\n", pname, file->disk));
	dbprintf(("%s:     program          = '%s'\n", pname, file->program));
	dbprintf(("%s:     srvcompprog      = '%s'\n", pname,
			file->srvcompprog));
	dbprintf(("%s:     clntcompprog     = '%s'\n", pname,
			file->clntcompprog));
	dbprintf(("%s:     srv_encrypt      = '%s'\n", pname,
			file->srv_encrypt));
	dbprintf(("%s:     clnt_encrypt     = '%s'\n", pname,
			file->clnt_encrypt));
	dbprintf(("%s:     recover_cmd      = '%s'\n", pname,
			file->recover_cmd));
	dbprintf(("%s:     uncompress_cmd   = '%s'\n", pname,
			file->uncompress_cmd));
	dbprintf(("%s:     encrypt_cmd      = '%s'\n", pname,
			file->encrypt_cmd));
	dbprintf(("%s:     decrypt_cmd      = '%s'\n", pname,
			file->decrypt_cmd));
	dbprintf(("%s:     srv_decrypt_opt  = '%s'\n", pname,
			file->srv_decrypt_opt));
	dbprintf(("%s:     clnt_decrypt_opt = '%s'\n", pname,
			file->clnt_decrypt_opt));
	dbprintf(("%s:     cont_filename    = '%s'\n", pname,
			file->cont_filename));
	dbprintf(("%s:     is_partial       = %d\n", pname, file->is_partial));
	dbprintf(("%s:     partnum          = %d\n", pname, file->partnum));
	dbprintf(("%s:     totalparts       = %d\n", pname, file->totalparts));
	dbprintf(("%s:     blocksize        = " SIZE_T_FMT "\n", pname,
			(SIZE_T_FMT_TYPE)file->blocksize));
}

static void
validate_name(
    const char *name)
{
	if (strlen(name) == 0) {
	    error("Invalid name '%s'\n", name);
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
	error("Invalid datestamp '%s'\n", datestamp);
	/*NOTREACHED*/
}

static void
validate_parts(
    const int partnum,
    const int totalparts)
{
	if (partnum < 1) {
	    error("Invalid partnum (%d)\n", partnum);
	    /*NOTREACHED*/
	}

	if (partnum > totalparts && totalparts >= 0) {
	    error("Invalid partnum (%d) > totalparts (%d)\n",
			partnum, totalparts);
	    /*NOTREACHED*/
	}
}

void
build_header(
    char *		buffer,
    const dumpfile_t *	file,
    size_t		buflen)
{
    int n;
    char *qname;
    char split_data[128] = "";

    dbprintf(("%s: Building type %d (%s) header of size " SIZE_T_FMT " using:\n",
		get_pname(), file->type, filetype2str(file->type),
		(SIZE_T_FMT_TYPE)buflen));
    dump_dumpfile_t(file);

    memset(buffer,'\0',buflen);

    switch (file->type) {
    case F_TAPESTART:
	validate_name(file->name);
	validate_datestamp(file->datestamp);
	snprintf(buffer, buflen,
	    "AMANDA: TAPESTART DATE %s TAPE %s\n014\n",
	    file->datestamp, file->name);
	break;

    case F_SPLIT_DUMPFILE:
	validate_parts(file->partnum, file->totalparts);
	snprintf(split_data, SIZEOF(split_data),
		 " part %d/%d ", file->partnum, file->totalparts);
	/*FALLTHROUGH*/
	
    case F_CONT_DUMPFILE:
    case F_DUMPFILE :
	validate_name(file->name);
	validate_datestamp(file->datestamp);
	qname = quote_string(file->disk);
        n = snprintf(buffer, buflen,
                     "AMANDA: %s %s %s %s %s lev %d comp %s program %s",
			 filetype2str(file->type),
			 file->datestamp, file->name, qname,
			 split_data,
		         file->dumplevel, file->comp_suffix, file->program); 
	amfree(qname);
	if ( n ) {
	  buffer += n;
	  buflen -= n;
	  n = 0;
	}
     
	if (strcmp(file->encrypt_suffix, "enc") == 0) {  /* only output crypt if it's enabled */
	  n = snprintf(buffer, buflen, " crypt %s", file->encrypt_suffix);
	}
	if ( n ) {
	  buffer += n;
	  buflen -= n;
	  n = 0;
	}

	if (*file->srvcompprog) {
	    n = snprintf(buffer, buflen, " server_custom_compress %s", file->srvcompprog);
	} else if (*file->clntcompprog) {
	    n = snprintf(buffer, buflen, " client_custom_compress %s", file->clntcompprog);
	} 

	if ( n ) {
	  buffer += n;
	  buflen -= n;
	  n = 0;
	}

	if (*file->srv_encrypt) {
	    n = snprintf(buffer, buflen, " server_encrypt %s", file->srv_encrypt);
	} else if (*file->clnt_encrypt) {
	    n = snprintf(buffer, buflen, " client_encrypt %s", file->clnt_encrypt);
	} 

	if ( n ) {
	  buffer += n;
	  buflen -= n;
	  n = 0;
	}
	
	if (*file->srv_decrypt_opt) {
	    n = snprintf(buffer, buflen, " server_decrypt_option %s", file->srv_decrypt_opt);
	} else if (*file->clnt_decrypt_opt) {
	    n = snprintf(buffer, buflen, " client_decrypt_option %s", file->clnt_decrypt_opt);
	} 

	if ( n ) {
	  buffer += n;
	  buflen -= n;
	  n = 0;
	}

	n = snprintf(buffer, buflen, "\n");
	buffer += n;
	buflen -= n;

	if (file->cont_filename[0] != '\0') {
	    n = snprintf(buffer, buflen, "CONT_FILENAME=%s\n",
		file->cont_filename);
	    buffer += n;
	    buflen -= n;
	}
	if (file->is_partial != 0) {
	    n = snprintf(buffer, buflen, "PARTIAL=YES\n");
	    buffer += n;
	    buflen -= n;
	}

	n = snprintf(buffer, buflen, 
	    "To restore, position tape at start of file and run:\n");
	buffer += n;
	buflen -= n;

	/* \014 == ^L == form feed */
	n = snprintf(buffer, buflen,
	    "\tdd if=<tape> bs=" SIZE_T_FMT "k skip=1 | %s %s %s\n\014\n",
	    (SIZE_T_FMT_TYPE)file->blocksize / 1024, file->decrypt_cmd,
	    file->uncompress_cmd, file->recover_cmd);
	break;

    case F_TAPEEND:
	validate_datestamp(file->datestamp);
	snprintf(buffer, buflen, "AMANDA: TAPEEND DATE %s\n\014\n",
	    file->datestamp);
	break;

    case F_UNKNOWN:
    case F_EMPTY:
    case F_WEIRD:
    default:
	error("Invalid header type: %d (%s)",
		file->type, filetype2str(file->type));
	/*NOTREACHED*/
    }
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
	fprintf(outf, "EMPTY file\n");
	break;

    case F_UNKNOWN:
	fprintf(outf, "UNKNOWN file\n");
	break;

    case F_WEIRD:
	fprintf(outf, "WEIRD file\n");
	break;

    case F_TAPESTART:
	fprintf(outf, "start of tape: date %s label %s\n",
	       file->datestamp, file->name);
	break;

    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
	qdisk = quote_string(file->disk);
	fprintf(outf, "%s: date %s host %s disk %s lev %d comp %s",
	    filetype2str(file->type), file->datestamp, file->name,
	    qdisk, file->dumplevel, file->comp_suffix);
	if (*file->program)
	    fprintf(outf, " program %s",file->program);
	if (strcmp(file->encrypt_suffix, "enc") == 0)
	    fprintf(outf, " crypt %s", file->encrypt_suffix);
	if (*file->srvcompprog)
	    fprintf(outf, " server_custom_compress %s", file->srvcompprog);
	if (*file->clntcompprog)
	    fprintf(outf, " client_custom_compress %s", file->clntcompprog);
	if (*file->srv_encrypt)
	    fprintf(outf, " server_encrypt %s", file->srv_encrypt);
	if (*file->clnt_encrypt)
	    fprintf(outf, " client_encrypt %s", file->clnt_encrypt);
	if (*file->srv_decrypt_opt)
	    fprintf(outf, " server_decrypt_option %s", file->srv_decrypt_opt);
	if (*file->clnt_decrypt_opt)
	    fprintf(outf, " client_decrypt_option %s", file->clnt_decrypt_opt);
	fprintf(outf, "\n");
	amfree(qdisk);
	break;

    case F_SPLIT_DUMPFILE:
        if(file->totalparts > 0){
            snprintf(number, SIZEOF(number), "%d", file->totalparts);
        }   
        else snprintf(number, SIZEOF(number), "UNKNOWN");
	qdisk = quote_string(file->disk);
        fprintf(outf, "split dumpfile: date %s host %s disk %s part %d/%s lev %d comp %s",
                      file->datestamp, file->name, qdisk, file->partnum,
                      number, file->dumplevel, file->comp_suffix);
        if (*file->program)
            fprintf(outf, " program %s",file->program);
	if (strcmp(file->encrypt_suffix, "enc") == 0)
	    fprintf(outf, " crypt %s", file->encrypt_suffix);
	if (*file->srvcompprog)
	    fprintf(outf, " server_custom_compress %s", file->srvcompprog);
	if (*file->clntcompprog)
	    fprintf(outf, " client_custom_compress %s", file->clntcompprog);
	if (*file->srv_encrypt)
	    fprintf(outf, " server_encrypt %s", file->srv_encrypt);
	if (*file->clnt_encrypt)
	    fprintf(outf, " client_encrypt %s", file->clnt_encrypt);
	if (*file->srv_decrypt_opt)
	    fprintf(outf, " server_decrypt_option %s", file->srv_decrypt_opt);
	if (*file->clnt_decrypt_opt)
	    fprintf(outf, " client_decrypt_option %s", file->clnt_decrypt_opt);
        fprintf(outf, "\n");
	amfree(qdisk);
        break;

    case F_TAPEEND:
	fprintf(outf, "end of tape: date %s\n", file->datestamp);
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
    { F_SPLIT_DUMPFILE, "SPLIT_FILE" }
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
