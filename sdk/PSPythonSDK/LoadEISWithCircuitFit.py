import pspython.pspyfiles as pspyfiles


filename = 'eis.1161.b.pssession'

 # Reads measurements and curves from .pssession file
psmeasurements_with_curves = pspyfiles.load_session_file(filename, load_peak_data=True, load_eis_fits=True, smooth_level=0)
print(len(psmeasurements_with_curves))
    
