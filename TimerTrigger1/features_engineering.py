import pandas as pd
import numpy as np
import talib
from talib.abstract import *


def process_ta_functions_group (df: pd.DataFrame, inputs: dict, ta_functions: list) -> pd.DataFrame:
    for func in ta_functions:
        output = globals()[func](inputs)
        # cas ou la fonction retourne une seule liste de outputs
        if len(output) == len(inputs["open"]):
            df[func] = output
        # cas ou la fonction retourne une liste de liste de outputs(ex:bande haute,bande moyenne, bande basse)
        else:
            i = 0
            for out in output:
                df[func+"_"+str(i)] = out
                i += 1
    return df


def add_TA (df: pd.DataFrame):
    inputs = {
    'open': df["Open"],
    'high': df["High"],
    'low': df["Low"],
    'close': df["Close"],
    'volume': df["Volume"]
    }

    overlap_studies = talib.get_function_groups()['Overlap Studies']
    if "MAVP" in overlap_studies:
        overlap_studies.remove("MAVP")
    df = process_ta_functions_group(df, inputs, overlap_studies)
    df = process_ta_functions_group(df, inputs, talib.get_function_groups()['Momentum Indicators'])
    df = process_ta_functions_group(df, inputs, talib.get_function_groups()['Cycle Indicators'])

    for func in talib.get_function_groups()['Volume Indicators']:
        df[func] = globals()[func](inputs)

    for func in talib.get_function_groups()['Volatility Indicators']:
        df[func] = globals()[func](inputs)

    for func in talib.get_function_groups()['Pattern Recognition']:
        df[func] = globals()[func](inputs)
