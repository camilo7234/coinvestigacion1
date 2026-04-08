$ws = New-Object -ComObject WScript.Shell
$s = $ws.CreateShortcut('C:\Users\57321\OneDrive\Escritorio\iniciar_interfaz.lnk')
$s.TargetPath = 'C:\Users\57321\OneDrive\Escritorio\coinvestigacion1-camilo\iniciar_interfaz.bat'
$s.WorkingDirectory = 'C:\Users\57321\OneDrive\Escritorio\coinvestigacion1-camilo'
$s.IconLocation = 'C:\Windows\System32\shell32.dll,3'
$s.Save()
