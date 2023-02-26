#!/usr/bin/env sh
cat > zigcc.sh << EOF
#!/usr/bin/env sh
$ZIG_EXE cc \$@
EOF

CC="$(pwd)/zigcc.sh"
export CC

# shellcheck disable=SC2068
go $@