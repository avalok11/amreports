#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

# Open the csv file as pandas data frame
csvfilepath = r'C:\Users\aleksey.yarkov\OneDrive - AmRest\_Projects\AI_ML\mail ru\export\test.csv'
data = pd.read_csv(csvfilepath, sep=';', low_memory=False)

# Write the resulting data frame to the hdf5 file
hdf5_file_path = r'C:\Users\aleksey.yarkov\OneDrive - AmRest\_Projects\AI_ML\mail ru\export\test.h5'
data.to_hdf(hdf5_file_path, 'data', format='table', complevel=9,
            complib='zlib')
