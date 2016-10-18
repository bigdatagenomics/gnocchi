@REM ----------------------------------------------------------------------------
@REM  Copyright 2001-2006 The Apache Software Foundation.
@REM
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM       http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM ----------------------------------------------------------------------------
@REM
@REM   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
@REM   reserved.

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup
set REPO=


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\etc;"%REPO%"\net\fnothaft\gnocchi-core\0.0.1-SNAPSHOT\gnocchi-core-0.0.1-SNAPSHOT.jar;"%REPO%"\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar;"%REPO%"\org\bdgenomics\utils\utils-misc_2.10\0.2.8\utils-misc_2.10-0.2.8.jar;"%REPO%"\org\apache\parquet\parquet-avro\1.7.0\parquet-avro-1.7.0.jar;"%REPO%"\org\apache\parquet\parquet-column\1.7.0\parquet-column-1.7.0.jar;"%REPO%"\org\apache\parquet\parquet-common\1.7.0\parquet-common-1.7.0.jar;"%REPO%"\org\apache\parquet\parquet-encoding\1.7.0\parquet-encoding-1.7.0.jar;"%REPO%"\org\apache\parquet\parquet-generator\1.7.0\parquet-generator-1.7.0.jar;"%REPO%"\commons-codec\commons-codec\1.5\commons-codec-1.5.jar;"%REPO%"\org\apache\parquet\parquet-hadoop\1.7.0\parquet-hadoop-1.7.0.jar;"%REPO%"\org\apache\parquet\parquet-jackson\1.7.0\parquet-jackson-1.7.0.jar;"%REPO%"\org\codehaus\jackson\jackson-mapper-asl\1.9.11\jackson-mapper-asl-1.9.11.jar;"%REPO%"\org\codehaus\jackson\jackson-core-asl\1.9.11\jackson-core-asl-1.9.11.jar;"%REPO%"\org\apache\parquet\parquet-format\2.3.0-incubating\parquet-format-2.3.0-incubating.jar;"%REPO%"\org\apache\avro\avro\1.8.0\avro-1.8.0.jar;"%REPO%"\com\thoughtworks\paranamer\paranamer\2.7\paranamer-2.7.jar;"%REPO%"\org\apache\commons\commons-compress\1.8.1\commons-compress-1.8.1.jar;"%REPO%"\org\tukaani\xz\1.5\xz-1.5.jar;"%REPO%"\org\scala-lang\scala-library\2.10.4\scala-library-2.10.4.jar;"%REPO%"\org\bdgenomics\adam\adam-cli_2.10\0.19.0\adam-cli_2.10-0.19.0.jar;"%REPO%"\org\apache\directory\studio\org.apache.commons.io\2.4\org.apache.commons.io-2.4.jar;"%REPO%"\commons-io\commons-io\2.4\commons-io-2.4.jar;"%REPO%"\args4j\args4j\2.0.23\args4j-2.0.23.jar;"%REPO%"\org\bdgenomics\utils\utils-cli_2.10\0.2.8\utils-cli_2.10-0.2.8.jar;"%REPO%"\org\bdgenomics\utils\utils-metrics_2.10\0.2.8\utils-metrics_2.10-0.2.8.jar;"%REPO%"\commons-cli\commons-cli\1.2\commons-cli-1.2.jar;"%REPO%"\commons-httpclient\commons-httpclient\3.1\commons-httpclient-3.1.jar;"%REPO%"\commons-logging\commons-logging\1.1.3\commons-logging-1.1.3.jar;"%REPO%"\org\bdgenomics\adam\adam-core_2.10\0.19.0\adam-core_2.10-0.19.0.jar;"%REPO%"\org\bdgenomics\utils\utils-io_2.10\0.2.4\utils-io_2.10-0.2.4.jar;"%REPO%"\com\esotericsoftware\kryo\kryo\2.24.0\kryo-2.24.0.jar;"%REPO%"\com\esotericsoftware\minlog\minlog\1.2\minlog-1.2.jar;"%REPO%"\org\objenesis\objenesis\2.1\objenesis-2.1.jar;"%REPO%"\org\scoverage\scalac-scoverage-plugin_2.10\1.1.1\scalac-scoverage-plugin_2.10-1.1.1.jar;"%REPO%"\org\bdgenomics\bdg-formats\bdg-formats\0.7.0\bdg-formats-0.7.0.jar;"%REPO%"\it\unimi\dsi\fastutil\6.6.5\fastutil-6.6.5.jar;"%REPO%"\org\slf4j\slf4j-log4j12\1.7.12\slf4j-log4j12-1.7.12.jar;"%REPO%"\org\apache\parquet\parquet-scala_2.10\1.8.1\parquet-scala_2.10-1.8.1.jar;"%REPO%"\org\seqdoop\hadoop-bam\7.1.0\hadoop-bam-7.1.0.jar;"%REPO%"\com\github\samtools\htsjdk\1.139\htsjdk-1.139.jar;"%REPO%"\org\apache\commons\commons-jexl\2.1.1\commons-jexl-2.1.1.jar;"%REPO%"\org\apache\ant\ant\1.8.2\ant-1.8.2.jar;"%REPO%"\org\apache\ant\ant-launcher\1.8.2\ant-launcher-1.8.2.jar;"%REPO%"\org\apache\httpcomponents\httpclient\4.5.1\httpclient-4.5.1.jar;"%REPO%"\org\apache\httpcomponents\httpcore\4.4.3\httpcore-4.4.3.jar;"%REPO%"\com\netflix\servo\servo-core\0.10.0\servo-core-0.10.0.jar;"%REPO%"\com\google\code\findbugs\annotations\2.0.0\annotations-2.0.0.jar;"%REPO%"\com\netflix\servo\servo-internal\0.10.0\servo-internal-0.10.0.jar;"%REPO%"\com\google\guava\guava\16.0.1\guava-16.0.1.jar;"%REPO%"\org\slf4j\slf4j-api\1.7.10\slf4j-api-1.7.10.jar;"%REPO%"\log4j\log4j\1.2.17\log4j-1.2.17.jar;"%REPO%"\org\xerial\snappy\snappy-java\1.1.2.1\snappy-java-1.1.2.1.jar;"%REPO%"\net\fnothaft\gnocchi-cli\0.0.1-SNAPSHOT\gnocchi-cli-0.0.1-SNAPSHOT.jar

set ENDORSED_DIR=
if NOT "%ENDORSED_DIR%" == "" set CLASSPATH="%BASEDIR%"\%ENDORSED_DIR%\*;%CLASSPATH%

if NOT "%CLASSPATH_PREFIX%" == "" set CLASSPATH=%CLASSPATH_PREFIX%;%CLASSPATH%

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS%  -classpath %CLASSPATH% -Dapp.name="gnocchi" -Dapp.repo="%REPO%" -Dapp.home="%BASEDIR%" -Dbasedir="%BASEDIR%" net.fnothaft.gnocchi.GnocchiMain %CMD_LINE_ARGS%
if %ERRORLEVEL% NEQ 0 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=%ERRORLEVEL%

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@REM If error code is set to 1 then the endlocal was done already in :error.
if %ERROR_CODE% EQU 0 @endlocal


:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%
