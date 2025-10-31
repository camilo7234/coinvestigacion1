Supported devices (recommended firmware):
PalmSens1/2
PalmSens3 (2.8)
PalmSens4 (1.7)
EmStat1 (3.7)
EmStat2/3/3+ (7.7)
EmStat Pico / Sensit series (1.3)
EmStat4 (1.1)

PalmSens SDK libraries version 5.9 (PSTrace 5.9.3803).

Recommended for use with Python 3.8, due to a dependency on pythonnet package which does not officially support newer versions.

Run the command `pip -r requirements.txt` to obtain the necessary dependencies.

Drivers need to be installed to discover and connect with PalmSens/EmStat/Sensit instruments, therefore it is currently recommended to install PSTrace.

In some cases the PalmSens.Core.dll and/or PalmSens.Core.Windows.dll libraries may not be found. To resolve this open the pspython folder right-click on the files select properties and unblock them.




