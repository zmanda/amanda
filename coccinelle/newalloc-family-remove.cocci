@newstralloc@
expression x, e;
@@

+ g_free(x);
- x = newstralloc(x, e);
+ x = g_strdup(e);

@newstralloc2@
expression x, e1, e2;
@@

+ g_free(x);
- x = newstralloc2(x, e1, e2);
+ x = stralloc2(e1, e2);

@newvstralloc@
expression x, e;
@@

+ g_free(x);
x =
- newvstralloc(x, e,
+ g_strjoin(NULL, e,
...
  );

@newvstrallocf@
expression x;
expression e;
@@

+ g_free(x);
x =
- newvstrallocf(x, e,
+ g_strdup_printf(e,
...
  );

