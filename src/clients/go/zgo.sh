#!/usr/bin/env sh
echo "#!/usr/bin/env sh\n $ZIG_EXE cc \$@" > zigcc.sh
CC=$(pwd)/zigcc.sh
go $@