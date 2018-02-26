/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 * 
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */

#include "amanda.h"
#include "testutils.h"
#include "ammessage.h"

/*
 * Tests
 */

static gboolean
test_parse_ammessage(void)
{
    char *str =
	"{ \"toto\":\"tata\",\n"
	"  \"number\": 123,\n"
	"  \"bool\": null,\n"
	"  \"array\": [ \"name\", 456, true ],\n"
	"  \"hash\": { \"name\" : \"name\",\n"
	"              \"bool\" : false,\n"
	"              \"number\" : 789 }\n"
	"}";
    GPtrArray *message;
    message = parse_json_message(str);
    message = message;
    return TRUE;
}


/*
 * Main driver
 */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_parse_ammessage, 90),
	TU_END()
    };

    return testutils_run_tests(argc, argv, tests);
}
