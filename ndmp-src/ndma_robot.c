/*
 * Copyright (c) 1998,1999,2000
 *	Traakan, Inc., Los Altos, CA
 *	All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice unmodified, this list of conditions, and the following
 *    disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Project:  NDMJOB
 * Ident:    $Id: $
 *
 * Description:
 *
 */


#include "ndmagents.h"


#ifndef NDMOS_OPTION_NO_ROBOT_AGENT


/*
 * Initialization and Cleanup
 ****************************************************************
 */

/* Initialize -- Set data structure to know value, ignore current value */
int
ndmra_initialize (struct ndm_session *sess)
{
	struct ndm_robot_agent *ra = &sess->robot_acb;

	NDMOS_MACRO_ZEROFILL(ra);

	return 0;
}

/* Commission -- Get agent ready. Entire session has been initialize()d */
int
ndmra_commission (struct ndm_session *sess)
{
	return 0;
}

/* Decommission -- Discard agent */
int
ndmra_decommission (struct ndm_session *sess)
{
	return 0;
}

/* Belay -- Cancel partially issued activation/start */
int
ndmra_belay (struct ndm_session *sess)
{
	return 0;
}




/*
 * Semantic actions -- called from ndma_dispatch()
 ****************************************************************
 */




/*
 * Quantum -- get a bit of work done
 ****************************************************************
 */

int
ndmra_quantum (struct ndm_session *sess)
{
	return 0;
}

#endif /* !NDMOS_OPTION_NO_ROBOT_AGENT */
