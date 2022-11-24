@echo off

where cl || (call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat")
  
cd /d %~dp0
  
set PROJECT_NAME="csv_parser_tests"

set COMPILER_FLAGS= ^
/WX /W4 /WL /wd4189 /wd4201 /wd4312 /wd4456 /wd4127 /wd4100 /wd4505 /wd4702 /wd4701 /we4457 /we4456 ^
/Zi /O2i /nologo /MT /EHsc-

set SRC=^
src/test_main.cpp

set INCLUDE_FOLDERS= ^
/I ../ ^
/I ./dependencies/ 

IF NOT EXIST .\build mkdir build

cl /Fo:build/ %PREPROCESSOR% %COMPILER_FLAGS% %SRC% %LIBS% %INCLUDE_FOLDERS% /link /out:build/%PROJECT_NAME%.exe 

