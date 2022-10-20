
@echo off
echo ````````````````````````````````````````````````
echo Starting Export
echo ````````````````````````````````````````````````

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED COMPARE_TOOL_HOME set COMPARE_TOOL_HOME=%CD%
popd

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=com.yonyou.iotdb.test.UpgradeTestFrom2To3
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -DIOTDB_HOME=%COMPARE_TOOL_HOME%

@REM ***** CLASSPATH library setting *****
set CLASSPATH=%COMPARE_TOOL_HOME%\lib\*

REM -----------------------------------------------------------------------------

"%JAVA_HOME%\bin\java" -DCOMPARE_TOOL_HOME=%COMPARE_TOOL_HOME% %JAVA_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %*
set ret_code=%ERRORLEVEL%
goto finally


:err
echo JAVA_HOME environment variable must be set!
set ret_code=1
pause


@REM -----------------------------------------------------------------------------
:finally

ENDLOCAL

EXIT /B %ret_code%
