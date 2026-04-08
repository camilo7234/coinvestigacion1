import os
import pspython.pspyfiles as pspyfiles

# load data
scriptDir = os.path.dirname(os.path.realpath(__file__))
measurements = pspyfiles.load_session_file(scriptDir + '\\Demo CV DPV EIS IS-C electrode.pssession', load_peak_data=True, load_eis_fits=True)
estimated_method_duration = pspyfiles.get_method_estimated_duration(scriptDir + '\\PSDiffPulse.psmethod')