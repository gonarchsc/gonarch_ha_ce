#!/usr/bin/bash

txtblk='\e[0;30m' # Black - Regular
txtred='\e[0;31m' # Red
txtgrn='\e[0;32m' # Green
txtylw='\e[0;33m' # Yellow
txtblu='\e[0;34m' # Blue
txtpur='\e[0;35m' # Purple
txtcyn='\e[0;36m' # Cyan
txtwht='\e[0;37m' # White
txtori='\e[0m'

error_handler(){
    echo -ne "${txtred}ERROR\n${txtori}"
    echo -e "An error acurred during the compilation. Please send the whole output of the compile script to ${txtcyn}support@gonarch.tech${txtori} to get some help. Thanks"
    exit
}
v_current_major=$(cat version | awk -F'.' '{print$1}')
v_current_minor=$(cat version | awk -F'.' '{print$2}')
v_current_patch=$(cat version | awk -F'.' '{print$3}')
new_v_patch=$((current_v++))
new_version="${v_current_major}.${v_current_minor}.${new_v_patch}"
echo ${new_version} > version
echo -e "Gonarch HA Community Edition - Compiler"
echo -e "NOTE: This is a development version and should not be used in production"
version=$(cat version)
echo -e "Version: ${version}"

rm -rf compile_tmp && mkdir compile_tmp

module_l=(core check api)

for m in "${module_l[@]}"; do
    echo -ne "- Compile ${m}... "
    pyinstaller --onefile \
    --specpath compile_tmp/ \
    --workpath compile_tmp/ \
    --distpath bin/ \
    --paths code/classes \
    --name ${m} \
    code/${m}.py >/dev/null 2>&1
    if [ "$?" -eq 0 ]; then
        echo -ne "${txtgrn}OK\n${txtori}"
    else
        error_handler
    fi
done

echo -ne "- Create tar file... "
tar -czvf compile_tmp/gonarch_ha_ce.${version}.tar.gz bin/ install.sh resources/ >/dev/null 2>&1
if [ "$?" -eq 0 ]; then
        echo -ne "${txtgrn}OK\n${txtori}"
    else
        error_handler
    fi

echo -ne "- Creating GitHub release ..."
gh release create ${version} compile_tmp/gonarch_ha_ce.${version}.tar.gz
if [ "$?" -eq 0 ]; then
    echo -ne "${txtgrn}OK\n${txtori}"
else
    error_handler
fi

echo -e "Compilation completed"