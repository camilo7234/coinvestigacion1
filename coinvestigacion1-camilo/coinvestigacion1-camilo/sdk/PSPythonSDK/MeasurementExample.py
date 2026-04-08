import pspython.pspyinstruments as pspyinstruments
import pspython.pspymethods as pspymethods


def new_data_callback(new_data):
    for type, value in new_data.items():
        print(type + ' = ' + str(value))
    return


manager = pspyinstruments.InstrumentManager(new_data_callback=new_data_callback)
available_instruments = manager.discover_instruments()
print('connecting to ' + available_instruments[0].name)
success = manager.connect(available_instruments[0])

# #Chronoamperometry measurement using helper class
# method = pspymethods.chronoamperometry(interval_time=0.5, e=1.0, run_time=5.0)

# EIS measurement using helper class
method = pspymethods.electrochemical_impedance_spectroscopy()

#Loading exiting cv method and changing its paramters
# import os
# import pspython.pspyfiles as pspyfiles
# scriptDir = os.path.dirname(os.path.realpath(__file__))
# method = pspyfiles.load_method_file(scriptDir + '\\cv.psmethod')
# method.Scanrate = 1
# method.StepPotential = 0.02

if success == 1:
    print('connection established')

    measurement = manager.measure(method)
    if measurement is not None:
        print('measurement finished')
    else:
        print('failed to start measurement')

    success = manager.disconnect()

    if success == 1:
        print('disconnected')
    else:
        print('error while disconnecting')
else:
    print('connection failed')




