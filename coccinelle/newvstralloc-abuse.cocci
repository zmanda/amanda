@newvstralloc@
expression x, e;
@@

- x = newvstralloc(x, e, NULL)
+ x = newstralloc(x, e)

@newvstrallocf@
expression x, e;
@@

- x = newvstrallocf(x, e)
+ x = newstralloc(x, e)

