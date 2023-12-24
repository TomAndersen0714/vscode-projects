@REM cls
@REM echo off

rem 编码ANSI
set "regpath=HKEY_CLASSES_ROOT\Applications\idea64.exe\shell\open\command"
for /f "delims=" %%a in ('reg query "%regpath%"^|find "默认"^|cscript -nologo -e:jscript "%~f0"') do set "exefile=%%a"
echo;"%exefile%"
if not defined exefile (echo;提取路径失败&pause&exit)
netsh advfirewall firewall add rule name=”idea_in_block” dir=in program= "%exefile%" action=block
netsh advfirewall firewall add rule name=”idea_out_block” dir=out program= "%exefile%" action=block
pause
exit

@REM var m=WSH.StdIn.ReadLine().match(/[A-Z]:.+?\.exe/i);
@REM if(m){WSH.echo(m[0]);}