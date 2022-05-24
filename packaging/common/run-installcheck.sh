#!/bin/bash

cd $(dirname $0)

AMPERL=${AMPERL:-perl}

summarize_tests() {
    local hdr=$1
    local prevOK
    local prevNOK="${hdr}:"
    local num=0
    local max=0

    while read -r l; do 
        num="${l}"
        num="${num#*ok }"
        num="${num%%[^0-9]*}"

        if [[ "$l" == [0-9]*..[0-9]* ]]; then
            max="${l##*[^0-9]}"
            continue;
        elif [[ "$l" == ok\ [0-9]* ]]; then
            printf "%s %d" "$prevNOK" $(( num ))
            prevNOK=
            prevOK=$'\n'
            continue;
        fi

        l="${l#not ok }"
        printf "%s%s: %s\n" "$prevOK" "$hdr" "$l"
        prevOK=
        prevNOK="${hdr}:"
    done;
    num=$(( num ))
    max=$(( max ))
    printf "%s" "${prevOK}" 
    [ $num == $max ] ||
        printf "%s: INCOMPLETE with %d out of %d\n" "$hdr" $num $max
}

setuid_run_test() {
    [[ "$*" == *./amrecover ]] && \
       {
           HARNESS_ACTIVE=1 command $AMPERL "$@"; 
           return $?
       }
    [ $EUID = 0 ] && \
       {
           command su - \
               -c "cd ${PWD}; HARNESS_ACTIVE=1 exec $AMPERL \"\$@\"" \
               amandabackup \
               -- -- "$@";
           return $?; \
       }

    HARNESS_ACTIVE=1 command $AMPERL "$@"
}

host="$(hostname)"
host="${host%%.*}"
host="${host#os-}"
host="${host%${host#?}}${host//[a-z-]}"

# all state is held in here...
rm -rf /tmp/amanda || { echo could not delete old test state; exit -1; }

export AMHOME=$(echo ~amandabackup)
export SHELL=/bin/bash

for i in $(<.all-tests); do 
    hdr="${host}-$i"
    setuid_run_test -I. "./$i" \
            >& >(summarize_tests "$hdr") ||
        printf "%s: EXIT STATUS %d" "$hdr" $?
done;
