#!/opt/zmanda/amanda/python/bin/python3.6

import sys
import os
import pathlib
import rpmfile
 
def validate_pkg_file(pkgname,tochk):
    try:
        tochk = pathlib.PurePath(tochk)
        tochkdir = tochk.parent

        manif = os.path.realpath('/opt/zmanda/amanda/pkg-manifests/'+pkgname+'.manifest')
        manif = pathlib.PurePath(manif)
        rpm = rpmfile.open(manif)
        if not rpm:
            manif = os.path.realpath('/opt/zmanda/amanda/pkg-manifests/'+pkgname+'.manifest.preun')
            manif = pathlib.PurePath(manif)
            rpm = rpmfile.open(manif)
            if not rpm:
                print("failed to open: {}".format(manif))
                return -1

        # find correct dirindex of name
        dind = rpm.headers.get('dirnames').index( (str(tochkdir) + '/').encode())

        dirs = rpm.headers.get('dirindexes')
        indlo = dirs.index(dind)
        indhi = len(dirs)
        for dirnum in reversed(dirs):
            if ( dirnum == dind ):
                break
            indhi = indhi-1

        ind = rpm.headers.get('basenames').index(tochk.name.encode(),indlo,indhi)

        st = os.lstat(tochk)
        if ( st.st_mtime != rpm.headers.get('filemtimes')[ind] ):
            #print("failed mtime match: ind={}".format(ind))
            return -2
        if ( st.st_size != rpm.headers.get('filesizes')[ind] ):
            #print( "failed size match: ind={}".format(ind))
            return -3

        digests = rpm.headers.get('filedigests')
        digests = ( digests if digests else rpm.headers.get('filemd5s') )

        #print("digest: size={} mtime={} dig={}".format(st.st_size,st.st_mtime,digests[ind]))
        return 0

    except FileNotFoundError as e:
        return -100

    except IOError as e:
        print('ioerror:' + str(e))
        return -101

    except ValueError as e:
        print('val:' +str(e))
        return -102
    except Exception as e:
        print('exc:' +str(e))
        return -103

validate_pkg_file(sys.argv[1],sys.argv[2])
sys.exit(r)
