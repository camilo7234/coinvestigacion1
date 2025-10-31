import clr
import os
import sys
from pspython import pspydata

# Load DLLs
scriptDir = os.path.dirname(os.path.realpath(__file__))
# This dll contains the classes in which the data is stored
clr.AddReference(scriptDir + '\\pspython\\PalmSens.Core.dll')
# This dll is used to load your session file
clr.AddReference(scriptDir + '\\pspython\\PalmSens.Core.Windows.dll')
# This name space is required for the argument of the peak detection
clr.AddReference('System.Collections')

# Import the static LoadSaveHelperFunctions
from PalmSens.Windows import LoadSaveHelperFunctions
# Import algorithm for LSV/CV peak detection and types required for arguments
from PalmSens.Analysis import SemiDerivativePeakDetection
from System.Collections.Generic import Dictionary
from PalmSens.Plottables import Curve
from System import Double


def load_session_file(path, **kwargs):
    load_peak_data = kwargs.get('load_peak_data', False)
    load_eis_fits = kwargs.get('load_eis_fits', False)

    try:
        session = LoadSaveHelperFunctions.LoadSessionFile(path)
        measurements = []

        for m in session:
            measurements.append(m)

        return measurements
    except:
        error = sys.exc_info()[0]
        print(error)
        return 0


def find_peaks(measurements):
    peakDetect = SemiDerivativePeakDetection()
    d = Dictionary[Curve, Double]()
    for m in measurements:   
        curves = m.GetCurveArray() 

        if type(m.Method).__name__ == "CyclicVoltammetry":            
            for c in curves:
                c.ClearPeaks() # clear previous peaks
                d[c] = 0.1 # min peak height in µA (defined in method editor and/or peak list form in PSTrace)

        if d.Count > 0:
            peakDetect.GetNonOverlappingPeaks(d)
            for n, c in enumerate(curves):
                for p in c.Peaks:
                    print("Peak found in " + m.Title + " curve " + str(n + 1) + ": height = " + str(p.PeakValue) + " µA, E = " + str(p.PeakX) + " V")
    return
    

data = load_session_file(scriptDir + '\\Demo CV DPV EIS IS-C electrode.pssession', load_peak_data=True, load_eis_fits=True)
find_peaks(data)
print("done")

