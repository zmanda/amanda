@positions@
constant char [] c;
expression e1, e2;
position p1, p2, p3;
@@

//
// We define three different positions:
// - p1 is where the first argument is a character constant and the second one
//   is an expression;
// - p2 is where the first argument is an expression and the second one is a
//   character constant;
// - p3 is where both arguments are expressions.
//

(
stralloc2@p1(c, e2)
|
stralloc2@p1(_(c), e2)
|
stralloc2@p2(e1, c)
|
stralloc2@p2(e1, _(c))
|
stralloc2@p3(e1, e2)
)

@script:python newconstants@
c << positions.c;
uc;
newc1;
newc2;
@@

//
// Here, c is the character constant captured by the rule above, if any. We
// build two more character constants out of it: newc1, where we insert a "%s"
// format string after the constant value (position p1 above), and newc2,
// where we insert a "%s" before the constant value (position p2 above). We
// don't need any such constant for p3 as we will use g_strjoin() as a
// replacement instead.
//
// Note the first line, for some reason it is needed, we cannot just do uc = c.
//

uc = str(c)[1:-1]
coccinelle.newc1 = "\"" + uc + "%s\""
coccinelle.newc2 = "\"%s" + uc + "\""

@@
position positions.p1, positions.p2, positions.p3;
identifier newconstants.newc1, newconstants.newc2;
expression e1, e2;
@@

//
// Now the meat of the transformation:
// - stralloc2(c, e) is replaced with g_strdup_printf(newc1, e);
// - stralloc2(e, c) is replaced with g_strdup_printf(newc2, e);
// - stralloc2(e1, e2) is replaced with g_strjoin(NULL, e1, e2, NULL).
//

(
- stralloc2@p1(e1, e2)
+ g_strdup_printf(newc1, e2)
|
- stralloc2@p2(e1, e2)
+ g_strdup_printf(newc2, e1)
|
- stralloc2@p3(e1, e2)
+ g_strjoin(NULL, e1, e2, NULL)
)

