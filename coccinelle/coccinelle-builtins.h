#include "/usr/local/share/coccinelle/standard.h"

#define G_GNUC_UNUSED

#define SWIGUNUSED
#define XS(myfn) void myfn(void)

#define G_UNLIKELY(cond) (cond)
#define G_LIKELY(cond) (cond)

#define R_OK 1
#define F_OK 1
#define W_OK 1
#define X_OK 1

#define DUMP ""
#define XFSDUMP ""
#define VXDUMP ""

#define __LINE__ ""

#define DMP_IGNORE ""
#define DMP_NORMAL ""
#define DMP_STRANGE ""
#define DMP_SIZE ""
#define DMP_ERROR ""
#define RESTORE ""

#define G_GNUC_PRINTF(x1, x2)

#define INTERACTIVITY_COMMENT ""
#define TAPERSCAN_COMMENT ""
#define APPLICTION_COMMENT ""
#define INTERACTIVITY_PLUGIN ""
#define TAPERSCAN_PLUGIN ""
#define APPLICTION_PLUGIN ""
