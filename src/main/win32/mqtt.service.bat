@echo off

set DIR=%~dp0
set NAME=MQTTBroker
set JAR_FILE=vertx-mqtt-broker-mod-2.0-SNAPSHOT-fat.jar

:: BatchGotAdmin
:-------------------------------------
REM  --> Check for permissions
>nul 2>&1 "%SYSTEMROOT%\system32\cacls.exe" "%SYSTEMROOT%\system32\config\system"

REM --> If error flag set, we do not have admin.
if '%errorlevel%' NEQ '0' (
    echo Requesting administrative privileges...
    goto UACPrompt
) else ( goto gotAdmin )

:UACPrompt
    echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs"
    set params = %*:"=""
    echo UAC.ShellExecute "cmd.exe", "/c %~s0 %params%", "", "runas", 1 >> "%temp%\getadmin.vbs"

    "%temp%\getadmin.vbs"
    del "%temp%\getadmin.vbs"
    exit /B

:gotAdmin
    pushd "%CD%"
    CD /D "%~dp0"
:--------------------------------------

:parse
IF "%~1"=="install" GOTO install
IF "%~1"=="uninstall" GOTO uninstall
IF "%~1"=="start" GOTO start
IF "%~1"=="stop" GOTO stop
IF "%~1"=="status" GOTO status
IF "%~1"=="" GOTO status
REM SHIFT
:endparse
GOTO eof

REM command ==> java -cp target\vertx-mqtt-broker-mod-2.0-SNAPSHOT-fat.jar io.vertx.core.Starter run io.github.giovibal.mqtt.MQTTBroker -conf config.json -instances 1
:install
prunsrv.exe //IS/%NAME% ^
    --DisplayName="MQTT Broker" ^
    --Description="Starts and manages the MQTT Broker server." ^
    --LogLevel=Debug ^
    --LogPath=%DIR%log ^
    --StartMode=jvm ^
    --Classpath=%DIR%\vertx-mqtt-broker-mod-2.0-SNAPSHOT-fat.jar ^
    --StartClass=io.vertx.core.Starter ^
    --StartParams=run;io.github.giovibal.mqtt.MQTTBroker;-conf;%DIR%config.json;-instances;1 ^
    --Startup=manual ^
    --StopMode=jvm ^
    --StopClass=io.github.giovibal.mqtt.MQTTBroker ^
    --StopMethod=stop ^
    --StdOutput=auto ^
    --StdError=auto ^
    --StopTimeout=10
GOTO result   

:uninstall
prunsrv.exe //DS/%NAME%
GOTO result

:start
prunsrv.exe //ES/%NAME%
GOTO result

:stop
prunsrv.exe //SS/%NAME%
GOTO result

:status
call:install
prunmgr.exe //MQ/%NAME%
start prunmgr.exe //ES/%NAME%
GOTO result

:result
if %ERRORLEVEL% GEQ 1 echo Error
if %ERRORLEVEL% EQU 0 echo OK

:eof


    
    
