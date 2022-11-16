#!/bin/bash

PROJECT_NAME="csv_parser_tests"

WARNINGS="-Wformat=2 
          -Wmain 
          -Wparentheses 
          -Wuninitialized
          -Wsign-compare 
          -Werror"

COMPILER_FLAGS="-O2 -mavx2 "
INCLUDE_FOLDERS="-I ../
                 -I ./dependencies/"

pushd "$(dirname ${BASH_SOURCE[0]})"
echo "$(pwd)"

if [ ! -d "build" ]
then
    mkdir build
fi

echo "Building $PROJECT_NAME"
echo
g++ -s $PREPROCESSOR $COMPILER_FLAGS $WARNINGS $INCLUDE_FOLDERS "./src/test_main.cpp" -o "./build/tests.a"

echo
echo "Done"

popd

exit 0
