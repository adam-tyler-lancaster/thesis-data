from pandas import (
    read_csv,
    to_datetime,
    concat,
    notnull,
    merge,
    Timestamp,
    option_context,
    date_range,
)
from numpy import where, nan
from matplotlib.pyplot import subplots, show, savefig
from seaborn import lineplot, set
from os.path import exists
from prettytable import PrettyTable
from arrow import get
from os.path import join, isdir, basename, splitext
from os import scandir
from pandas import DataFrame
from pdfkit import configuration, from_string
from IPython.display import display
from IPython.core.display import HTML
from re import match as rematch
from re import IGNORECASE
from pytimeparse.timeparse import timeparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def time_delta(date_str: str, pos_delta: bool = True, delta: int = 10) -> str:
    # Convert string to datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

    # Subtract 10 days
    if pos_delta:
        new_date_obj = date_obj + timedelta(days=delta)
    else:
        new_date_obj = date_obj - timedelta(days=delta)

    # Convert back to string
    new_date_str = new_date_obj.strftime("%Y-%m-%d %H:%M:%S")

    return new_date_str


def import_csv(file, old_col_name, new_col_name, resample_rate, start_date, end_date):
    df = read_csv(file + ".csv")
    df["Time"] = to_datetime(df["Time"], unit="ms")
    # df['Time2']=df['Time']
    # df['weekday'] = df['Time'].dt.day
    # df['year'] = df['Time'].dt.year
    # df['month'] = df['Time'].dt.month
    # cols = ['year', 'month', 'weekday']
    # df['dayID'] = df[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    # df['dayID'] = df['year'] + df['month'] + df['weekday']
    df.rename(columns={old_col_name: new_col_name}, inplace=True)
    df.set_index("Time", inplace=True)
    df = df.resample(f"{resample_rate}S").mean()
    df = df[~(df.index < start_date)]
    df = df[~(df.index > end_date)]
    df[new_col_name] = df[new_col_name].replace("", nan)  # Convert empty strings to NaN
    return df


def calc_wrm(df_oat, end_date):
    # calculate weighted running mean oat
    # df_rm = df_oat.resample("1D").mean()
    df_rm = df_oat.resample("1D", closed="right", label="right").mean()
    df_rm = df_rm.asfreq("1D", method="pad")  # Ensure the last day is included
    df_rm["OAT-1"] = df_rm["OAT"].shift(1)
    df_rm["OAT-2"] = df_rm["OAT"].shift(2)
    df_rm["OAT-3"] = df_rm["OAT"].shift(3)
    df_rm["OAT-4"] = df_rm["OAT"].shift(4)
    df_rm["OAT-5"] = df_rm["OAT"].shift(5)
    df_rm["OAT-6"] = df_rm["OAT"].shift(6)
    df_rm["OAT-7"] = df_rm["OAT"].shift(7)
    df_rm = df_rm.assign(
        OAT_RM=lambda x: (
            (
                x["OAT-1"]
                + (0.8 * x["OAT-2"])
                + (0.6 * x["OAT-3"])
                + (0.5 * x["OAT-4"])
                + (0.4 * x["OAT-5"])
                + (0.3 * x["OAT-6"])
                + (0.2 * x["OAT-7"])
            )
            / 3.8
        )
    )
    df_rm.drop(df_rm.columns.difference(["OAT_RM"]), axis=1, inplace=True)
    df_rm = df_rm.resample("10min").ffill()
    df_rm = df_rm[~(df_rm.index > end_date)]
    return df_rm


def get_data(
    dir_path, dir_name, oat_dir, resample_rate, start_date, end_date, calculate_OT=True
):
    time_delta_int = 15

    # import data from csv files and process as required - pads startdate with delta to allow WRM to build
    df_oat = import_csv(
        file=oat_dir + "/OAT",
        old_col_name="Air Temperature (full resolution)",
        new_col_name="OAT",
        resample_rate=resample_rate,
        start_date=time_delta(start_date, pos_delta=False, delta=time_delta_int),
        end_date=end_date,
    )
    df_air = import_csv(
        file=str(dir_path) + "/" + dir_name + "_Air",
        old_col_name="Air Temp - Lascar (high res)",
        new_col_name="AIR",
        resample_rate=resample_rate,
        start_date=time_delta(start_date, pos_delta=False, delta=time_delta_int),
        end_date=end_date,
    )
    if calculate_OT:
        df_rad = import_csv(
            file=str(dir_path) + "/" + dir_name + "_Radiance",
            old_col_name="Radiance (high res)",
            new_col_name="RAD",
            resample_rate=resample_rate,
            start_date=time_delta(start_date, pos_delta=False, delta=time_delta_int),
            end_date=end_date,
        )
        df_hum = import_csv(
            file=str(dir_path) + "/" + dir_name + "_Humidity",
            old_col_name="Humidity - Lascar (high res)",
            new_col_name="HUM",
            resample_rate=resample_rate,
            start_date=time_delta(start_date, pos_delta=False, delta=time_delta_int),
            end_date=end_date,
        )
    try:
        df_heat = import_csv(
            file=str(dir_path) + "/" + dir_name + "_Heating",
            old_col_name="Heating on (high res)",
            new_col_name="HEAT",
            resample_rate=resample_rate,
            start_date=time_delta(start_date, pos_delta=False, delta=time_delta_int),
            end_date=end_date,
        )

    except:
        if exists(str(dir_path) + "/no_heating"):
            heating_flag = False
        else:
            raise Exception("no heating file")
    else:
        heating_flag = True
    df_rm = calc_wrm(df_oat=df_oat, end_date=end_date)
    if calculate_OT:
        df = concat([df_oat, df_air, df_rad, df_hum, df_rm], axis=1)
    else:
        df = concat([df_oat, df_air, df_rm], axis=1)
    if heating_flag:
        df = concat([df, df_heat], axis=1)

    # removes data before the start date
    df = df[~(df.index < start_date)]
    df = df[~(df.index > end_date)]

    df = df.loc[:, ~df.columns.duplicated()].copy()
    df["dayID"] = df.index.strftime("%Y-%m-%d")
    # check for valid data
    df["OAT_check"] = notnull(df["OAT"])
    df["AIR_check"] = notnull(df["AIR"])
    df["OAT_RM_check"] = notnull(df["OAT_RM"])
    if heating_flag != False:
        df["HEAT_check"] = notnull(df["HEAT"])
    if calculate_OT:
        df["RAD_check"] = notnull(df["RAD"])
        df["HUM_check"] = notnull(df["HUM"])
        if heating_flag == False:
            df["check"] = (
                df["OAT_check"]
                & df["AIR_check"]
                & df["OAT_RM_check"]
                & df["RAD_check"]
                & df["HUM_check"]
            )
        else:
            df["check"] = (
                df["OAT_check"]
                & df["AIR_check"]
                & df["OAT_RM_check"]
                & df["HEAT_check"]
                & df["RAD_check"]
                & df["HUM_check"]
            )
    else:
        if heating_flag == False:
            df["check"] = df["OAT_check"] & df["AIR_check"] & df["OAT_RM_check"]
        else:
            df["check"] = (
                df["OAT_check"]
                & df["AIR_check"]
                & df["OAT_RM_check"]
                & df["HEAT_check"]
            )
    df.check = df.check.replace({True: 1, False: 0})
    # df.to_csv('out.csv')
    # display(df)
    return df, heating_flag


def calc_OT(T_range, df, calculate_OT=True, CC_temp=21):
    # calculate occupant temp
    if calculate_OT:
        df["OT"] = (df["AIR"] + df["RAD"]) / 2
    else:
        df["OT"] = df["AIR"]
    # calculate comfort temps
    df["T_comf"] = 0.33 * df["OAT_RM"] + 18.8
    df["T_comf_upper"] = df["T_comf"] + T_range
    df["T_comf_lower"] = df["T_comf"] - T_range
    # calculate delta T
    df["delta_T"] = df["OT"] - (df["T_comf"] + T_range)
    df["delta_T_-4"] = df["OT"] - (df["T_comf"] - 4)
    df["delta_T_-3"] = df["OT"] - (df["T_comf"] - 3)
    df["delta_T_-2"] = df["OT"] - (df["T_comf"] - 2)
    df["delta_T_-1"] = df["OT"] - (df["T_comf"] - 1)
    df["delta_T_-0"] = df["OT"] - (df["T_comf"])
    df["delta_T_CC"] = df["OT"] - CC_temp
    return df


def calc_heating_on(df):
    df["HEATING_ON"] = where(df["HEAT"] > df["AIR"] + 5, 1, 0)
    # display(df)
    return df


def check_crit_1_days(df, start_date, end_date, resample_rate=600):
    # calculate hours of exceedance (Criterion 1)
    # Create a copy of the DataFrame to avoid modifying the original
    df_hre = df.copy()
    # Retain only the "delta_T" column for processing
    df_hre.drop(df_hre.columns.difference(["delta_T"]), axis=1, inplace=True)
    # Pad rows before the earliest timestamp
    earliest_timestamp = df_hre.index.min()
    day_before = earliest_timestamp - timedelta(days=1)
    additional_rows = date_range(
        start=day_before,
        end=earliest_timestamp - timedelta(seconds=resample_rate),
        freq=f"{resample_rate}S",
    )
    first_row = df_hre.iloc[[0]].copy()
    first_row = first_row.reindex(additional_rows, method="ffill")
    df_hre = concat([first_row, df_hre])
    # Pad rows after the latest timestamp
    latest_timestamp = df_hre.index.max()
    day_after = latest_timestamp + timedelta(days=1)
    additional_rows = date_range(
        start=latest_timestamp + timedelta(seconds=resample_rate),
        end=day_after,
        freq=f"{resample_rate}S",
    )
    last_row = df_hre.iloc[[-1]].copy()
    last_row = last_row.reindex(additional_rows, method="ffill")
    df_hre = concat([df_hre, last_row])
    # Create a binary column to indicate if "delta_T" exceeds the threshold of 1
    df_hre["cat_1_test"] = df_hre["delta_T"].apply(lambda x: (1 if x >= 1 else 0))
    # Add a base column with a constant value of 1 for aggregation purposes
    df_hre["base"] = 1
    # Resample the data to daily frequency and calculate the sum for each day
    # df_hre = df_hre.resample("1D").sum()
    df_hre = df_hre.resample("1D").sum(min_count=1)  # Sum all days, even if incomplete
    # df_hre = df_hre.asfreq("1D", method="pad")  # Ensure the last day is included
    # df_hre = df_hre.reindex(date_range(df_hre.index.min(), df_hre.index.max(), freq="1D"), method="pad")  # Ensure the first day is included
    # Calculate the percentage of time "delta_T" exceeds the threshold for each day
    df_hre["cat_1_test_per"] = df_hre["cat_1_test"] / df_hre["base"]
    # Determine if the daily percentage exceeds the 3% threshold and assign binary values
    df_hre["cat_1"] = df_hre["cat_1_test_per"].apply(lambda x: (1 if x > 0.03 else 0))
    # Resample the data back to 10-minute intervals and forward-fill the values
    df_hre = df_hre.resample(f"{resample_rate}S").ffill()
    # Retain only the "cat_1" and "base" columns for further processing
    df_hre.drop(df_hre.columns.difference(["cat_1", "base"]), axis=1, inplace=True)
    # df_hre.drop(["delta_T"], axis=1, inplace=True)
    # reset the base col
    df_hre["base"] = 1
    # Concatenate the processed data back with the original DataFrame
    df_hre.index.name = "Time"
    df = concat([df, df_hre], axis=1)
    # trim an extra days
    df = df[~(df.index < start_date)]
    df = df[~(df.index > end_date)]
    # Return the updated DataFrame
    return df


def check_crit_2(df, start_date, end_date, resample_rate=600):
    # calculate daily weighted exceedance (Criterion 2)
    df_we = df.copy()
    # Pad rows before the earliest timestamp
    earliest_timestamp = df_we.index.min()
    day_before = earliest_timestamp - timedelta(days=1)
    additional_rows = date_range(
        start=day_before,
        end=earliest_timestamp - timedelta(seconds=resample_rate),
        freq=f"{resample_rate}S",
    )
    first_row = df_we.iloc[[0]].copy()
    first_row = first_row.reindex(additional_rows, method="ffill")
    df_we = concat([first_row, df_we])
    # Pad rows after the latest timestamp
    latest_timestamp = df_we.index.max()
    day_after = latest_timestamp + timedelta(days=1)
    additional_rows = date_range(
        start=latest_timestamp + timedelta(seconds=resample_rate),
        end=day_after,
        freq=f"{resample_rate}S",
    )
    last_row = df_we.iloc[[-1]].copy()
    last_row = last_row.reindex(additional_rows, method="ffill")
    df_we = concat([df_we, last_row])
    # generate weights
    df_we["delta_T_rounded"] = df_we.delta_T.round()
    df_we["we"] = df_we["delta_T_rounded"].apply(lambda x: (x if x > 0 else 0))
    df_we.drop(df_we.columns.difference(["we"]), axis=1, inplace=True)
    # df_we = df_we.resample("1D").sum()
    df_we = df_we.resample("1D").sum(min_count=1)  # Sum all days, even if incomplete
    # df_we = df_we.asfreq("1D", method="pad")  # Ensure the last day is included
    # df_we = df_we.reindex(date_range(df_we.index.min(), df_we.index.max(), freq="1D"), method="pad")  # Ensure the first day is included
    df_we["we"] = df_we["we"] * (1 / 10)
    df_we = df_we.resample(f"{resample_rate}S").ffill()
    df_we["cat_2"] = df_we["we"].apply(lambda x: (1 if x > 6 else 0))
    df_we.drop(["we"], axis=1, inplace=True)
    df_we.index.name = "Time"
    df = concat([df, df_we], axis=1)
    # trim an extra days
    df = df[~(df.index < start_date)]
    df = df[~(df.index > end_date)]
    # display(df)
    # df_we.tail(50)
    return df


def check_crit_3(df, start_date, end_date, resample_rate=600):
    # calculate daily Upper limit temperature (Criterion 3)
    df_ult = df.copy()
    df_ult.drop(df_ult.columns.difference(["delta_T"]), axis=1, inplace=True)
    # Pad rows before the earliest timestamp
    earliest_timestamp = df_ult.index.min()
    day_before = earliest_timestamp - timedelta(days=1)
    additional_rows = date_range(
        start=day_before,
        end=earliest_timestamp - timedelta(seconds=resample_rate),
        freq=f"{resample_rate}S",
    )
    first_row = df_ult.iloc[[0]].copy()
    first_row = first_row.reindex(additional_rows, method="ffill")
    df_ult = concat([first_row, df_ult])
    # Pad rows after the latest timestamp
    latest_timestamp = df_ult.index.max()
    day_after = latest_timestamp + timedelta(days=1)
    additional_rows = date_range(
        start=latest_timestamp + timedelta(seconds=resample_rate),
        end=day_after,
        freq=f"{resample_rate}S",
    )
    last_row = df_ult.iloc[[-1]].copy()
    last_row = last_row.reindex(additional_rows, method="ffill")
    df_ult = concat([df_ult, last_row])
    # resample to daily frequency and calculate the sum for each day
    # df_ult = df_ult.resample("1D").max()
    df_ult = df_ult.resample("1D").sum(min_count=1)  # Sum all days, even if incomplete
    # df_ult = df_ult.resample("1D", closed="right", label="right").sum()
    # df_ult = df_ult.asfreq("1D", method="pad")  # Ensure the last day is included
    # df_ult = df_ult.reindex(date_range(df_ult.index.min(), df_ult.index.max(), freq="1D"), method="pad")  # Ensure the first day is included
    df_ult["cat_3"] = df_ult["delta_T"].apply(lambda x: (1 if x > 4 else 0))
    df_ult = df_ult.resample(f"{resample_rate}S").ffill()
    df_ult.drop(["delta_T"], axis=1, inplace=True)
    df_ult.index.name = "Time"
    df = concat([df, df_ult], axis=1)
    # trim an extra days
    df = df[~(df.index < start_date)]
    df = df[~(df.index > end_date)]
    # display(df)
    # df_ult.tail(50)
    return df


def check_crit_4(df):
    # calculate band lower limit exccedance (Criterion 4)
    df_llt = df.copy()
    df_llt.drop(
        df_llt.columns.difference(
            ["delta_T_-4", "delta_T_-3", "delta_T_-2", "delta_T_-1", "delta_T_-0"]
        ),
        axis=1,
        inplace=True,
    )
    df_llt["cat_4_-4"] = df_llt["delta_T_-4"].apply(lambda x: (1 if x > 0 else 0))
    df_llt["cat_4_-3"] = df_llt["delta_T_-3"].apply(lambda x: (1 if x > 0 else 0))
    df_llt["cat_4_-2"] = df_llt["delta_T_-2"].apply(lambda x: (1 if x > 0 else 0))
    df_llt["cat_4_-1"] = df_llt["delta_T_-1"].apply(lambda x: (1 if x > 0 else 0))
    df_llt["cat_4_-0"] = df_llt["delta_T_-0"].apply(lambda x: (1 if x > 0 else 0))
    df_llt.drop(
        ["delta_T_-4", "delta_T_-3", "delta_T_-2", "delta_T_-1", "delta_T_-0"],
        axis=1,
        inplace=True,
    )
    df = concat([df, df_llt], axis=1)
    return df


def check_crit_5(df):
    # calculate constant comfort limit (21) excedance (Criterion 5)
    df_lcc = df.copy()
    df_lcc.drop(
        df_lcc.columns.difference(["delta_T_CC"]),
        axis=1,
        inplace=True,
    )
    df_lcc["cat_5"] = df_lcc["delta_T_CC"].apply(lambda x: (1 if x > 0 else 0))
    df_lcc.drop(
        ["delta_T_CC"],
        axis=1,
        inplace=True,
    )
    df = concat([df, df_lcc], axis=1)
    return df


def calc_energy(df, resample_rate):
    # heating power (kW) = Tdiff * SHC * mass_flow_rate
    df["heating power"] = ((10 * 4187 * 0.0094304952) / 1000) * df["HEATING_ON"]
    # heating energy (kWh) = heating_power * sample_period (hours)
    df["energy"] = df["heating power"] * (resample_rate / (60 * 60))
    df["excess_energy"] = df["red_heating"] * df["energy"]
    df["excess_energy_cat_4-4"] = df["heating_cat_4-4"] * df["energy"]
    df["excess_energy_cat_4-3"] = df["heating_cat_4-3"] * df["energy"]
    df["excess_energy_cat_4-2"] = df["heating_cat_4-2"] * df["energy"]
    df["excess_energy_cat_4-1"] = df["heating_cat_4-1"] * df["energy"]
    df["excess_energy_cat_4-0"] = df["heating_cat_4-0"] * df["energy"]
    df["excess_energy_cat_5"] = df["heating_cat_5"] * df["energy"]
    # display(df.head(50))
    return df


def gen_scores(df, heating_flag, resample_rate, print_output, num_decimal_places=1):
    df["score"] = df["cat_1"] + df["cat_2"] + df["cat_3"]
    df["green"] = df["score"].apply(lambda x: (1 if x < 1 else 0))
    df["amber"] = df["score"].apply(lambda x: (1 if x >= 1 and x < 2 else 0))
    df["red"] = df["score"].apply(lambda x: (1 if x >= 2 else 0))
    df["base_checked"] = where(df["check"] == 1, 1, 0)
    df["base_checked"] = df["base"] * df["check"]
    df["green_checked"] = where(df["check"] == 1, 1, 0)
    df["green_checked"] = df["green"] * df["check"]
    df["amber_checked"] = where(df["check"] == 1, 1, 0)
    df["amber_checked"] = df["amber"] * df["check"]
    df["red_checked"] = where(df["check"] == 1, 1, 0)
    df["red_checked"] = df["red"] * df["check"]
    df["cat_4_-4_checked"] = where(df["check"] == 1, 1, 0)
    df["cat_4_-4_checked"] = df["cat_4_-4"] * df["check"]
    df["cat_4_-3_checked"] = where(df["check"] == 1, 1, 0)
    df["cat_4_-3_checked"] = df["cat_4_-3"] * df["check"]
    df["cat_4_-2_checked"] = where(df["check"] == 1, 1, 0)
    df["cat_4_-2_checked"] = df["cat_4_-2"] * df["check"]
    df["cat_4_-1_checked"] = where(df["check"] == 1, 1, 0)
    df["cat_4_-1_checked"] = df["cat_4_-1"] * df["check"]
    df["cat_4_-0_checked"] = where(df["check"] == 1, 1, 0)
    df["cat_4_-0_checked"] = df["cat_4_-0"] * df["check"]
    df["cat_5_checked"] = where(df["check"] == 1, 1, 0)
    df["cat_5_checked"] = df["cat_5"] * df["check"]
    if print_output:
        print(
            "Green (score < 1) -",
            round(
                (df["green_checked"].sum() + 1) / (3600 / resample_rate),
                num_decimal_places,
            ),
            " hours out of ",
            round(
                df["base_checked"].sum() / (3600 / resample_rate), num_decimal_places
            ),
            ", ",
            round(
                ((df["green_checked"].sum() + 1) / df["base_checked"].sum()) * 100,
                num_decimal_places,
            ),
            "% of time",
        )
        print(
            "Amber (score >= 1 and < 2) -",
            round(
                df["amber_checked"].sum() / (3600 / resample_rate), num_decimal_places
            ),
            " hours out of ",
            round(
                df["base_checked"].sum() / (3600 / resample_rate), num_decimal_places
            ),
            ", ",
            round(
                (df["amber_checked"].sum() / df["base_checked"].sum()) * 100,
                num_decimal_places,
            ),
            "% of time",
        )
        print(
            "Red (score >= 2) -",
            round(df["red_checked"].sum() / (3600 / resample_rate), num_decimal_places),
            " hours out of ",
            round(
                df["base_checked"].sum() / (3600 / resample_rate), num_decimal_places
            ),
            ", ",
            round(
                (df["red_checked"].sum() / df["base_checked"].sum()) * 100,
                num_decimal_places,
            ),
            "% of time",
        )
        _out1 = [
            "missing data ->",
            round((1 - (df["OAT"].isna().sum() / df.shape[0])) * 100, 3),
            "% OAT data available (",
            df["OAT"].isna().sum(),
            "data-points out of",
            df.shape[0],
            "), ",
            round((1 - (df["AIR"].isna().sum() / df.shape[0])) * 100, 3),
            "% IAT data available (",
            df["AIR"].isna().sum(),
            "data-points out of",
            df.shape[0],
            ")",
        ]
        _out1 = list(map(str, _out1))
        print(
            "% of time IAT above: OT-4",
            round((df["cat_4_-4_checked"].sum() / df.shape[0]) * 100, 1),
            ", OT-3",
            round((df["cat_4_-3_checked"].sum() / df.shape[0]) * 100, 1),
            ", OT-2",
            round((df["cat_4_-2_checked"].sum() / df.shape[0]) * 100, 1),
            ", OT-1",
            round((df["cat_4_-1_checked"].sum() / df.shape[0]) * 100, 1),
            ", OT",
            round((df["cat_4_-0_checked"].sum() / df.shape[0]) * 100, 1),
        )
        print(
            "% of time IAT above CC temp: ",
            round((df["cat_5_checked"].sum() / df.shape[0]) * 100, 1),
        )
    if heating_flag:
        df["base_heating"] = where(df["HEATING_ON"] == 1, 1, 0)
        df["base_heating"] = df["base_heating"] * df["base_checked"]
        df["green_heating"] = where(df["HEATING_ON"] == 1, 1, 0)
        df["green_heating"] = df["green_heating"] * df["green_checked"]
        df["amber_heating"] = where(df["HEATING_ON"] == 1, 1, 0)
        df["amber_heating"] = df["amber_heating"] * df["amber_checked"]
        df["red_heating"] = where(df["HEATING_ON"] == 1, 1, 0)
        df["red_heating"] = df["red_heating"] * df["red_checked"]
        df["heating_missing_data_oat"] = where(df["OAT"].isna(), 1, 0)
        df["heating_missing_data_oat"] = df["heating_missing_data_oat"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_missing_data_iat"] = where(df["AIR"].isna(), 1, 0)
        df["heating_missing_data_iat"] = df["heating_missing_data_iat"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_missing_data_rt"] = where(df["HEAT"].isna(), 1, 0)
        df["heating_missing_data_rt"] = df["heating_missing_data_rt"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["not_heating_missing_data_oat"] = where(df["OAT"].isna(), 1, 0)
        df["not_heating_missing_data_oat"] = df["not_heating_missing_data_oat"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_missing_data_iat"] = where(df["AIR"].isna(), 1, 0)
        df["not_heating_missing_data_iat"] = df["not_heating_missing_data_iat"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_missing_data_rt"] = where(df["HEAT"].isna(), 1, 0)
        df["not_heating_missing_data_rt"] = df["not_heating_missing_data_rt"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_cat_4-4"] = df["cat_4_-4_checked"]
        df["not_heating_cat_4-4"] = df["not_heating_cat_4-4"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_cat_4-3"] = df["cat_4_-3_checked"]
        df["not_heating_cat_4-3"] = df["not_heating_cat_4-3"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_cat_4-2"] = df["cat_4_-2_checked"]
        df["not_heating_cat_4-2"] = df["not_heating_cat_4-2"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_cat_4-1"] = df["cat_4_-1_checked"]
        df["not_heating_cat_4-1"] = df["not_heating_cat_4-1"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_cat_4-0"] = df["cat_4_-0_checked"]
        df["not_heating_cat_4-0"] = df["not_heating_cat_4-0"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["not_heating_cat_5"] = df["cat_5_checked"]
        df["not_heating_cat_5"] = df["not_heating_cat_5"] * where(
            df["HEATING_ON"] == 1, 0, 1
        )
        df["heating_cat_4-4"] = df["cat_4_-4_checked"]
        df["heating_cat_4-4"] = df["heating_cat_4-4"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_cat_4-3"] = df["cat_4_-3_checked"]
        df["heating_cat_4-3"] = df["heating_cat_4-3"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_cat_4-2"] = df["cat_4_-2_checked"]
        df["heating_cat_4-2"] = df["heating_cat_4-2"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_cat_4-1"] = df["cat_4_-1_checked"]
        df["heating_cat_4-1"] = df["heating_cat_4-1"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_cat_4-0"] = df["cat_4_-0_checked"]
        df["heating_cat_4-0"] = df["heating_cat_4-0"] * where(
            df["HEATING_ON"] == 1, 1, 0
        )
        df["heating_cat_5"] = df["cat_5_checked"]
        df["heating_cat_5"] = df["heating_cat_5"] * where(df["HEATING_ON"] == 1, 1, 0)
        df = calc_energy(df=df, resample_rate=resample_rate)
        if print_output:
            _out2 = [
                ", ",
                round((1 - (df["HEAT"].isna().sum() / df.shape[0])) * 100, 3),
                "% RAD data available (",
                df["HEAT"].isna().sum(),
                "data-points out of",
                df.shape[0],
                ")",
            ]
            _out2 = list(map(str, _out2))
            print(" ".join(_out1) + " ".join(_out2))
            if df["HEATING_ON"].sum() != 0:
                print(
                    "% of time whilst heating on IAT above: OT-4",
                    round(
                        (df["heating_cat_4-4"].sum() / df["HEATING_ON"].sum()) * 100, 1
                    ),
                    ", OT-3",
                    round(
                        (df["heating_cat_4-3"].sum() / df["HEATING_ON"].sum()) * 100, 1
                    ),
                    ", OT-2",
                    round(
                        (df["heating_cat_4-2"].sum() / df["HEATING_ON"].sum()) * 100, 1
                    ),
                    ", OT-1",
                    round(
                        (df["heating_cat_4-1"].sum() / df["HEATING_ON"].sum()) * 100, 1
                    ),
                    ", OT",
                    round(
                        (df["heating_cat_4-0"].sum() / df["HEATING_ON"].sum()) * 100, 1
                    ),
                )
                print(
                    "% of time whilst heating on IAT above CC temp:",
                    round(
                        (df["heating_cat_5"].sum() / df["HEATING_ON"].sum()) * 100, 1
                    ),
                )
            else:
                print(
                    "% of time whilst heating on IAT above: OT-4 0, OT-3 0, OT-2 0, OT-1 0, OT 0"
                )
                print("% of time whilst heating on IAT above CC temp: 0")
            print(
                "% of time whilst heating off IAT above: OT-4",
                round(
                    (
                        df["not_heating_cat_4-4"].sum()
                        / (df.shape[0] - df["HEATING_ON"].sum())
                    )
                    * 100,
                    1,
                ),
                ",  OT-3",
                round(
                    (
                        df["not_heating_cat_4-3"].sum()
                        / (df.shape[0] - df["HEATING_ON"].sum())
                    )
                    * 100,
                    1,
                ),
                ", OT-2",
                round(
                    (
                        df["not_heating_cat_4-2"].sum()
                        / (df.shape[0] - df["HEATING_ON"].sum())
                    )
                    * 100,
                    1,
                ),
                ", OT-1",
                round(
                    (
                        df["not_heating_cat_4-1"].sum()
                        / (df.shape[0] - df["HEATING_ON"].sum())
                    )
                    * 100,
                    1,
                ),
                ", OT",
                round(
                    (
                        df["not_heating_cat_4-0"].sum()
                        / (df.shape[0] - df["HEATING_ON"].sum())
                    )
                    * 100,
                    1,
                ),
            )
            print(
                "% of time whilst heating off IAT above CC temp:",
                round(
                    (
                        df["not_heating_cat_5"].sum()
                        / (df.shape[0] - df["HEATING_ON"].sum())
                    )
                    * 100,
                    1,
                ),
            )
            if df["base_heating"].sum() != 0:
                _temp = round(
                    (df["green_heating"].sum() / df["base_heating"].sum()) * 100,
                    num_decimal_places,
                )
            else:
                _temp = "0"
            print(
                "Green whilst heating active (score < 1) -",
                round(
                    df["green_heating"].sum() / (3600 / resample_rate),
                    num_decimal_places,
                ),
                " hours out of ",
                round(
                    df["base_heating"].sum() / (3600 / resample_rate),
                    num_decimal_places,
                ),
                ", ",
                _temp,
                "% of time",
            )
            if df["base_heating"].sum() != 0:
                round(
                    (df["amber_heating"].sum() / df["base_heating"].sum()) * 100,
                    num_decimal_places,
                )
            else:
                _temp = "0"
            print(
                "Amber whilst heating active (score >= 1 and < 2) -",
                round(
                    df["amber_heating"].sum() / (3600 / resample_rate),
                    num_decimal_places,
                ),
                " hours out of ",
                round(
                    df["base_heating"].sum() / (3600 / resample_rate),
                    num_decimal_places,
                ),
                ", ",
                _temp,
                "% of time",
            )
            if df["base_heating"].sum() != 0:
                round(
                    (df["red_heating"].sum() / df["base_heating"].sum()) * 100,
                    num_decimal_places,
                )
            else:
                _temp = "0"
            print(
                "Red whilst heating active (score >= 2) -",
                round(
                    df["red_heating"].sum() / (3600 / resample_rate),
                    num_decimal_places,
                ),
                " hours out of ",
                round(
                    df["base_heating"].sum() / (3600 / resample_rate),
                    num_decimal_places,
                ),
                ", ",
                _temp,
                "% of time",
            )
            print(
                round(df["excess_energy"].sum(), num_decimal_places),
                "kWh wasted overheating",
            )
            if df["HEATING_ON"].sum() != 0:
                print(
                    "missing data (heating on) ->",
                    round(
                        (
                            1
                            - (
                                df["heating_missing_data_oat"].sum()
                                / df["HEATING_ON"].sum()
                            )
                        )
                        * 100,
                        3,
                    ),
                    "% OAT data available (",
                    df["heating_missing_data_oat"].sum(),
                    "data-points out of",
                    df["HEATING_ON"].sum(),
                    "), ",
                    round(
                        (
                            1
                            - (
                                df["heating_missing_data_iat"].sum()
                                / df["HEATING_ON"].sum()
                            )
                        )
                        * 100,
                        3,
                    ),
                    "% IAT data available (",
                    df["heating_missing_data_iat"].sum(),
                    "data-points out of",
                    df["HEATING_ON"].sum(),
                    "), ",
                    round(
                        (
                            1
                            - (
                                df["heating_missing_data_rt"].sum()
                                / df["HEATING_ON"].sum()
                            )
                        )
                        * 100,
                        3,
                    ),
                    "% RAD data available (",
                    df["heating_missing_data_rt"].sum(),
                    "data-points out of",
                    df["HEATING_ON"].sum(),
                    ")",
                )
            else:
                print(
                    "missing data (heating on) -> -% OAT data available (",
                    df["heating_missing_data_oat"].sum(),
                    "data-points out of",
                    df["HEATING_ON"].sum(),
                    "), -% IAT data available (",
                    df["heating_missing_data_iat"].sum(),
                    "data-points out of",
                    df["HEATING_ON"].sum(),
                    "), -% RAD data available (",
                    df["heating_missing_data_rt"].sum(),
                    "data-points out of",
                    df["HEATING_ON"].sum(),
                    ")",
                )
            print(
                "missing data (heating off) ->",
                round(
                    (
                        1
                        - (
                            df["not_heating_missing_data_oat"].sum()
                            / (df.shape[0] - df["HEATING_ON"].sum())
                        )
                    )
                    * 100,
                    3,
                ),
                "% OAT data available (",
                df["not_heating_missing_data_oat"].sum(),
                "data-points out of",
                (df.shape[0] - df["HEATING_ON"].sum()),
                "), ",
                round(
                    (
                        1
                        - (
                            df["not_heating_missing_data_iat"].sum()
                            / (df.shape[0] - df["HEATING_ON"].sum())
                        )
                    )
                    * 100,
                    3,
                ),
                "% IAT data available (",
                df["not_heating_missing_data_iat"].sum(),
                "data-points out of",
                (df.shape[0] - df["HEATING_ON"].sum()),
                "), ",
                round(
                    (
                        1
                        - (
                            df["not_heating_missing_data_rt"].sum()
                            / (df.shape[0] - df["HEATING_ON"].sum())
                        )
                    )
                    * 100,
                    3,
                ),
                "% RAD data available (",
                df["not_heating_missing_data_rt"].sum(),
                "data-points out of",
                (df.shape[0] - df["HEATING_ON"].sum()),
                ")",
            )
    else:
        if print_output:
            print(" ".join(_out1))
    # display(df.head(200))
    return df


def plot_data(df, start_date, end_date, heating_flag, save_plots=False, file_name=""):
    if file_name == "" and save_plots:
        raise ValueError("file_name cannot be empty if save_plots is True")

    # Use seaborn style defaults and set the default figure size
    if heating_flag:
        fig_height = 13
        fig_rows = 4
    else:
        fig_height = 10
        fig_rows = 3
    set(rc={"figure.figsize": (13, fig_height)})
    fig, axs = subplots(nrows=fig_rows, constrained_layout=True)
    df_res = df.reset_index()
    font_size = 15

    # Ensure Time column is in datetime format
    df_res["Time"] = to_datetime(df_res["Time"])

    # Filter DataFrame to keep only rows within the specified range
    df_res = df_res[(df_res["Time"] >= start_date) & (df_res["Time"] <= end_date)]

    # OAT/indoor plot
    # Define seasonal colors
    season_colors = {
        "Spring": "lightgreen",
        "Summer": "gold",
        "Autumn": "orangered",
        "Winter": "lightblue",
    }

    # Define base seasonal periods
    base_seasons = [
        ("03-01", "05-31", "Spring"),
        ("06-01", "08-31", "Summer"),
        ("09-01", "11-30", "Autumn"),
        ("12-01", "02-28", "Winter"),
    ]

    # Extract unique years
    unique_years = df_res["Time"].dt.year.unique()

    # Shade seasonal regions
    for year in unique_years:
        for start, end, season in base_seasons:
            start_date = to_datetime(f"{year}-{start}")
            if end.startswith("02") and year % 4 == 0:  # Handle leap years for February
                end_date = to_datetime(f"{year}-02-29")
            else:
                end_date = to_datetime(f"{year}-{end}")

            axs[0].axvspan(
                start_date,
                end_date,
                color=season_colors[season],
                alpha=0.2,
                label=season if year == unique_years[0] else None,
            )

    # Plot the data
    lineplot(x="Time", y="AIR", data=df_res, label="Indoor Ambient Temp", ax=axs[0])
    lineplot(x="Time", y="OAT", data=df_res, label="Outside Air Temp", ax=axs[0])
    axs[0].set(ylim=(-15, 45))
    axs[0].set_ylabel("Temperature (°C)", fontsize=font_size)
    axs[0].set(xlabel=None)
    axs[0].set_xlim(df_res["Time"].min(), df_res["Time"].max())
    axs[0].legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=len(axs[0].get_legend().get_texts()),
        frameon=True,
        framealpha=1,
    )

    # Adaptive band plot
    lineplot(x="Time", y="OT", data=df_res, label="Operative Temperature", ax=axs[1])
    lineplot(
        x="Time", y="T_comf", data=df_res, label="Adaptive Band Mid Point", ax=axs[1]
    )
    lineplot(
        x="Time",
        y="T_comf_upper",
        data=df_res,
        label="Adaptive Band Upper limit",
        ax=axs[1],
    )
    lineplot(
        x="Time",
        y="T_comf_lower",
        data=df_res,
        label="Adaptive Band Lower limit",
        ax=axs[1],
    )
    lineplot(x="Time", y="delta_T", data=df_res, label="delta_T", ax=axs[1])
    axs[1].set(ylim=(-15, 45))
    axs[1].set_ylabel("Temperature (°C)", fontsize=font_size)
    axs[1].set(xlabel=None)
    axs[1].set_xlim(df_res["Time"].min(), df_res["Time"].max())
    axs[1].legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=len(axs[1].get_legend().get_texts()),
        frameon=True,
        framealpha=1,
    )

    # Score plot
    lineplot(x="Time", y="score", data=df_res, label="Score", ax=axs[2], zorder=2)
    axs[2].set(ylim=(-0.1, 3.1))
    axs[2].set_ylabel("Score", fontsize=font_size)
    axs[2].set(xlabel=None)
    axs[2].axhline(y=0, color="green", ls="--", linewidth=0.7, zorder=1)
    axs[2].axhline(y=1, color="orange", ls="--", linewidth=0.7, zorder=1)
    axs[2].axhline(y=2, color="red", ls="--", linewidth=0.7, zorder=1)
    axs[2].set_xlim(df_res["Time"].min(), df_res["Time"].max())

    # Heating plots
    if heating_flag:
        lineplot(
            x="Time", y="HEATING_ON", data=df_res, label="Heating on/off", ax=axs[3]
        )
        axs[3].set_ylabel("Heating on/off", fontsize=font_size)
        axs[3].set(xlabel=None)
        axs[3].legend(
            loc="upper left",
            frameon=True,
            framealpha=1,
        )
        ax2 = axs[3].twinx()
        lineplot(
            x="Time",
            y="HEAT",
            data=df_res,
            label="Radiator Flow Temp",
            color="r",
            ax=ax2,
        )
        ax2.set(ylim=(0, 80))
        ax2.set_ylabel("Flow Temperature (°C)", fontsize=font_size)
        ax2.set(xlabel=None)
        ax2.set_xlim(df_res["Time"].min(), df_res["Time"].max())
        ax2.legend(
            loc="upper right",
            frameon=True,
            framealpha=1,
        )

    # save plot
    if save_plots:
        save_path = f"output/{file_name}.png"
        savefig(save_path, dpi=300)

    # show plots
    show()


# return options:
# ee = excess energy (float) if available
# df = full dataframe
# null = nothing
#
# resample rate options:
# Xsec
# Xmin
# Xhour
# Xday
def analyse_data(
    T_range,
    dir_path,
    dir_name,
    oat_dir,
    group,
    start_date,
    end_date,
    resample_rate="10min",
    show_plots=True,
    save_plots=False,
    return_data="ee",
    print_output=True,
    calculate_OT=True,
):
    # data validation
    if return_data not in ["ee", "df", "null"]:
        raise ValueError(
            f"Invalid return_data value: {return_data}. Must be 'ee', 'df', or 'null'."
        )

    resample_rate = timeparse(resample_rate)
    if print_output:
        print("Room -", dir_name, "-", group)

    # Retrieve and preprocess data for the specified directory and date range
    df, heating_flag = get_data(
        dir_path=dir_path,
        dir_name=dir_name,
        oat_dir=oat_dir,
        resample_rate=resample_rate,
        start_date=start_date,
        end_date=end_date,
        calculate_OT=calculate_OT,
    )
    # display(df)
    df = calc_OT(T_range=T_range, df=df, calculate_OT=calculate_OT)
    if heating_flag:
        df = calc_heating_on(df=df)
    df = check_crit_1_days(
        df=df,
        resample_rate=resample_rate,
        start_date=start_date,
        end_date=end_date,
    )
    df = check_crit_2(
        df=df, resample_rate=resample_rate, start_date=start_date, end_date=end_date
    )
    df = check_crit_3(
        df=df, resample_rate=resample_rate, start_date=start_date, end_date=end_date
    )
    df = check_crit_4(df=df)
    df = check_crit_5(df=df)
    df = gen_scores(
        df=df,
        heating_flag=heating_flag,
        resample_rate=resample_rate,
        print_output=print_output,
    )
    # with option_context(
    #     "display.max_rows",
    #     None,
    #     "display.max_columns",
    #     None,
    #     "display.precision",
    #     3,
    # ):
    #     display(df.tail(3050))

    if show_plots:
        plot_data(
            df=df,
            start_date=start_date,
            end_date=end_date,
            heating_flag=heating_flag,
            save_plots=save_plots,
            file_name=dir_name,
        )
    if return_data == "ee":
        if heating_flag:
            return df["excess_energy"].sum()
        else:
            return 0
    elif return_data == "df":
        return df
    elif return_data == "null":
        return
    else:
        raise Exception("bad return_data passed -", str(return_data))


def analyse_data_for_table(
    room,
    data_folder,
    oat_folder,
    start_date,
    end_date,
    resample_rate,
    t_range=4,
    num_decimal_places=1,
    num_decimal_places_big=3,
    monthly=False,
    full_data=False,
    calculate_OT=True,
):
    """
    Analyzes data for a given room and generates results for various metrics.

    Args:
        room (str): Room name.
        data_folder (str): Path to the folder containing room data.
        oat_folder (str): Path to the folder containing outside air temperature (OAT) data.
        start_date (str): Start date of the analysis period (format: 'YYYY-MM-DD HH:MM:SS').
        end_date (str): End date of the analysis period (format: 'YYYY-MM-DD HH:MM:SS').
        resample_rate (str): Resampling rate (e.g., '10min', '1hour').
        t_range (int): Temperature range for analysis (default: 4).
        num_decimal_places (int): Number of decimal places for rounding (default: 1).
        num_decimal_places_big (int): Number of decimal places for larger values (default: 3).
        monthly (bool): Whether to generate monthly results (default: False).
        full_data (bool): Whether to include detailed data points (default: False).

    Returns:
        list: A list of dictionaries containing analysis results.
    """
    # Validate resample rate
    if resample_rate == 0:
        raise ValueError("Resample rate cannot be zero.")

    _resample_rate = timeparse(resample_rate)
    results = [{}, {}, {}]  # Initialize results for different categories

    # Iterate over date blocks (full range and optionally monthly blocks)
    for _, _start_date, _end_date, month_name in date_blocks_iterator(
        start_date=start_date, end_date=end_date, months=monthly
    ):
        # print(
        #     f"Analyzing data for {room} from {_start_date} to {_end_date} with resample rate {resample_rate}"
        # )
        # Initialize result containers for the current month
        results[0][month_name] = {
            "data": [],
            "start_date": _start_date,
            "end_date": _end_date,
        }
        results[1][month_name] = {
            "data": [],
            "start_date": _start_date,
            "end_date": _end_date,
        }
        results[2][month_name] = {}

        latch = False  # first run latch

        # for all test values
        for t in range(t_range, -1, -1):

            # Analyze data for the current date block
            df = analyse_data(
                T_range=t,
                dir_path=data_folder,
                dir_name=room.replace("S_", ""),
                oat_dir=oat_folder,
                group="",
                start_date=_start_date,
                end_date=_end_date,
                resample_rate=resample_rate,
                show_plots=False,
                save_plots=False,
                return_data="df",
                print_output=False,
                calculate_OT=calculate_OT,
            )
            # if t == 3:
            #     df.to_csv(
            #         f'output/{room}_{t}_{datetime.strptime(_start_date, "%Y-%m-%d %H:%M:%S").strftime("%B")}_to_{datetime.strptime(_end_date, "%Y-%m-%d %H:%M:%S").strftime("%B")}.csv'
            #     )
            # print(
            #     f"Analyzing data for {room} from {_start_date} to {_end_date} with resample rate {resample_rate} and T_range {t}"
            # )

            # if not the first test value
            if not (latch):

                # Valid hours
                results[0][month_name]["data"].append(
                    round(df["base_checked"].sum() / (3600 / _resample_rate))
                )
                results[0][month_name]["data"].append(
                    str(df.shape[0])
                )  # Total data points
                results[0][month_name]["data"].append(
                    str(
                        round(
                            (1 - (df["OAT"].isna().sum() / df.shape[0])) * 100,
                            num_decimal_places_big,
                        )
                    )
                )  # % of available OAT data points
                if full_data:
                    results[0][month_name]["data"].append(
                        str(df["OAT"].isna().sum())
                    )  # Missing OAT data points
                results[0][month_name]["data"].append(
                    str(
                        round(
                            (1 - (df["AIR"].isna().sum() / df.shape[0])) * 100,
                            num_decimal_places_big,
                        )
                    )
                )  # % of available IAT data points
                if full_data:
                    results[0][month_name]["data"].append(
                        str(df["AIR"].isna().sum())
                    )  # Missing IAT data points

                # Heating-related metrics
                try:
                    df["green_heating"].sum()  # Check if heating data is available
                except KeyError:
                    # If heating data is not available, pad with placeholders
                    results[0][month_name]["data"].extend(
                        ["-"] * (14 if full_data else 7)
                    )
                else:
                    results[0][month_name]["data"].append(
                        str(
                            round(
                                (1 - (df["HEAT"].isna().sum() / df.shape[0])) * 100,
                                num_decimal_places_big,
                            )
                        )
                    )  # % of radiator flow temperature data points available
                    if full_data:
                        results[0][month_name]["data"].append(
                            str(df["HEAT"].isna().sum())
                        )  # Missing radiator flow temperature data points
                        results[0][month_name]["data"].append(
                            str(df["HEATING_ON"].sum())
                        )  # Data points with heating on
                        results[0][month_name]["data"].append(
                            str((df.shape[0] - df["HEATING_ON"].sum()))
                        )  # Data points with heating off

                    # Missing data while heating on
                    for col, label in [
                        ("heating_missing_data_oat", "OAT"),
                        ("heating_missing_data_iat", "IAT"),
                        ("heating_missing_data_rt", "radiator flow temperature"),
                    ]:
                        if df["HEATING_ON"].sum() != 0:
                            results[0][month_name]["data"].append(
                                str(
                                    round(
                                        (1 - (df[col].sum() / df["HEATING_ON"].sum()))
                                        * 100,
                                        num_decimal_places_big,
                                    )
                                )
                            )  # % of missing data while heating on
                        else:
                            results[0][month_name]["data"].append("-")
                        if full_data:
                            results[0][month_name]["data"].append(
                                str(df[col].sum())
                            )  # Missing data points while heating on

                    # Missing data while heating off
                    for col, label in [
                        ("not_heating_missing_data_oat", "OAT"),
                        ("not_heating_missing_data_iat", "IAT"),
                        ("not_heating_missing_data_rt", "radiator flow temperature"),
                    ]:
                        results[0][month_name]["data"].append(
                            str(
                                round(
                                    (
                                        1
                                        - (
                                            df[col].sum()
                                            / (df.shape[0] - df["HEATING_ON"].sum())
                                        )
                                    )
                                    * 100,
                                    num_decimal_places_big,
                                )
                            )
                        )  # % of missing data while heating off
                        if full_data:
                            results[0][month_name]["data"].append(
                                str(df[col].sum())
                            )  # Missing data points while heating off

                # Calculate percentage of time IAT is above various thresholds (OT-4 to OT)
                results[1][month_name]["data"].append(
                    round(
                        (df["cat_4_-4_checked"].sum() / df.shape[0]) * 100,
                        num_decimal_places,
                    )
                )  # % of time IAT above OT-4
                results[1][month_name]["data"].append(
                    round(
                        (df["cat_4_-3_checked"].sum() / df.shape[0]) * 100,
                        num_decimal_places,
                    )
                )  # % of time IAT above OT-3
                results[1][month_name]["data"].append(
                    round(
                        (df["cat_4_-2_checked"].sum() / df.shape[0]) * 100,
                        num_decimal_places,
                    )
                )  # % of time IAT above OT-2
                results[1][month_name]["data"].append(
                    round(
                        (df["cat_4_-1_checked"].sum() / df.shape[0]) * 100,
                        num_decimal_places,
                    )
                )  # % of time IAT above OT-1
                results[1][month_name]["data"].append(
                    round(
                        (df["cat_4_-0_checked"].sum() / df.shape[0]) * 100,
                        num_decimal_places,
                    )
                )  # % of time IAT above OT

                # Check if heating data is available
                try:
                    df["green_heating"].sum()
                except KeyError:
                    # If heating data is not available, pad with placeholders
                    results[1][month_name]["data"].extend(["-"] * 15)
                    results[1][month_name]["ee_cat4_4"] = None
                    results[1][month_name]["ee_cat4_3"] = None
                    results[1][month_name]["ee_cat4_2"] = None
                    results[1][month_name]["ee_cat4_1"] = None
                    results[1][month_name]["ee_cat4_0"] = None
                else:
                    # Calculate percentage of time IAT is above thresholds while heating is on
                    for threshold, col in [
                        ("OT-4", "heating_cat_4-4"),
                        ("OT-3", "heating_cat_4-3"),
                        ("OT-2", "heating_cat_4-2"),
                        ("OT-1", "heating_cat_4-1"),
                        ("OT", "heating_cat_4-0"),
                    ]:
                        if df["HEATING_ON"].sum() != 0:
                            results[1][month_name]["data"].append(
                                round(
                                    (df[col].sum() / df["HEATING_ON"].sum()) * 100,
                                    num_decimal_places,
                                )
                            )
                        else:
                            results[1][month_name]["data"].append("-")

                    # Calculate percentage of time IAT is above thresholds while heating is off
                    for threshold, col in [
                        ("OT-4", "not_heating_cat_4-4"),
                        ("OT-3", "not_heating_cat_4-3"),
                        ("OT-2", "not_heating_cat_4-2"),
                        ("OT-1", "not_heating_cat_4-1"),
                        ("OT", "not_heating_cat_4-0"),
                    ]:
                        results[1][month_name]["data"].append(
                            round(
                                (df[col].sum() / (df.shape[0] - df["HEATING_ON"].sum()))
                                * 100,
                                num_decimal_places,
                            )
                        )

                    # Calculate kWh wasted overheating while heating is on
                    if df["HEATING_ON"].sum() != 0:
                        for col, key in [
                            ("excess_energy_cat_4-4", "ee_cat4_4"),
                            ("excess_energy_cat_4-3", "ee_cat4_3"),
                            ("excess_energy_cat_4-2", "ee_cat4_2"),
                            ("excess_energy_cat_4-1", "ee_cat4_1"),
                            ("excess_energy_cat_4-0", "ee_cat4_0"),
                        ]:
                            results[1][month_name][key] = df[col].sum()
                            results[1][month_name]["data"].append(
                                round(results[1][month_name][key], num_decimal_places)
                            )
                    else:
                        for key in [
                            "ee_cat4_4",
                            "ee_cat4_3",
                            "ee_cat4_2",
                            "ee_cat4_1",
                            "ee_cat4_0",
                        ]:
                            results[1][month_name][key] = 0
                            results[1][month_name]["data"].append(0)

                # Calculate percentage of time IAT is above CC temperature
                results[1][month_name]["data"].append(
                    round(
                        (df["cat_5_checked"].sum() / df.shape[0]) * 100,
                        num_decimal_places,
                    )
                )

                # Check if heating data is available for CC temperature calculations
                try:
                    df["green_heating"].sum()
                except KeyError:
                    # If heating data is not available, pad with placeholders
                    results[1][month_name]["data"].extend(["-"] * 3)
                    results[1][month_name]["ee_cat5"] = None
                else:
                    # Calculate percentage of time IAT is above CC temperature while heating is on
                    if df["HEATING_ON"].sum() != 0:
                        results[1][month_name]["data"].append(
                            round(
                                (df["heating_cat_5"].sum() / df["HEATING_ON"].sum())
                                * 100,
                                num_decimal_places,
                            )
                        )
                    else:
                        results[1][month_name]["data"].append("-")

                    # Calculate percentage of time IAT is above CC temperature while heating is off
                    results[1][month_name]["data"].append(
                        round(
                            (
                                df["not_heating_cat_5"].sum()
                                / (df.shape[0] - df["HEATING_ON"].sum())
                            )
                            * 100,
                            num_decimal_places,
                        )
                    )

                    # Calculate kWh wasted overheating while IAT is above CC temperature
                    if df["HEATING_ON"].sum() != 0:
                        results[1][month_name]["ee_cat5"] = df[
                            "excess_energy_cat_5"
                        ].sum()
                        results[1][month_name]["data"].append(
                            round(results[1][month_name]["ee_cat5"], num_decimal_places)
                        )
                    else:
                        results[1][month_name]["ee_cat5"] = 0
                        results[1][month_name]["data"].append(0)

                latch = True

            # setup dict for level 2
            results[2][month_name][t] = {
                "data": [],
                "start_date": _start_date,
                "end_date": _end_date,
            }

            # Temperature exceedance metrics (previously df["green_checked"].sum() required +1)
            for col, label in [
                ("green_checked", "Green"),
                ("amber_checked", "Amber"),
                ("red_checked", "Red"),
            ]:
                results[2][month_name][t]["data"].append(
                    str(
                        round(
                            df[col].sum() / (3600 / _resample_rate),
                            num_decimal_places,
                        )
                    )
                    + ""
                )

            # Percentage metrics
            for col, label in [
                ("green_checked", "Green"),
                ("amber_checked", "Amber"),
                ("red_checked", "Red"),
            ]:
                if df["base_checked"].sum() != 0:
                    results[2][month_name][t]["data"].append(
                        str(
                            round(
                                (df[col].sum() / df["base_checked"].sum()) * 100,
                                num_decimal_places,
                            )
                        )
                    )  # % of time in each category
                else:
                    results[2][month_name][t]["data"].append("-")

            # Heating-related metrics
            try:
                df["green_heating"].sum()
            except KeyError:
                # If heating data is not available, pad with placeholders
                results[2][month_name][t]["data"].extend(["-"] * 7)
                results[2][month_name][t]["ee"] = 0
            else:
                for col, label in [
                    ("green_heating", "Green"),
                    ("amber_heating", "Amber"),
                    ("red_heating", "Red"),
                ]:
                    results[2][month_name][t]["data"].append(
                        str(
                            round(
                                df[col].sum() / (3600 / _resample_rate),
                                num_decimal_places,
                            )
                        )
                        + ""
                    )  # Hours in each category while heating
                for col, label in [
                    ("green_heating", "Green"),
                    ("amber_heating", "Amber"),
                    ("red_heating", "Red"),
                ]:
                    if df["base_heating"].sum() != 0:
                        results[2][month_name][t]["data"].append(
                            str(
                                round(
                                    (df[col].sum() / df["base_heating"].sum()) * 100,
                                    num_decimal_places,
                                )
                            )
                        )  # % of time in each category while heating
                    else:
                        results[2][month_name][t]["data"].append("-")

                # Excess energy wasted
                ee_sum = df["excess_energy"].sum()
                results[2][month_name][t]["ee"] = ee_sum
                results[2][month_name][t]["data"].append(
                    str(round(ee_sum, num_decimal_places))
                )  # kWh wasted overheating

    return results


def generate_table(
    base_folder,
    location,
    start_date,
    end_date,
    num_decimal_places=1,
    num_decimal_places_big=3,
    output_to_file=None,
    file_name_prep=None,
    resample_rate="10min",
    monthly=False,
    full_data=False,
    t_range=4,
    tab_2_max_concat=2,
    bms_data_folder="BMS_study/BMS",
    bms_oat_folder="BMS_study",
    rooms_base_folder="study",
    get_BMS=True,
    calculate_OT=True,
):

    results_list = []
    testing = False

    # get BMS data
    if get_BMS:
        _room = "BMS"
        results_list.append(
            {
                "room": _room,
                "results": analyse_data_for_table(
                    room=_room,
                    data_folder=base_folder + bms_data_folder,
                    oat_folder=base_folder + bms_oat_folder,
                    start_date=start_date,
                    end_date=end_date,
                    resample_rate=resample_rate,
                    num_decimal_places=num_decimal_places,
                    num_decimal_places_big=num_decimal_places_big,
                    monthly=monthly,
                    t_range=t_range,
                    calculate_OT=calculate_OT,
                ),
            }
        )

    study_count = 0

    # get study data
    top = join(base_folder, rooms_base_folder)
    for dir in scandir(top):
        dir_path = join(top, dir)
        if isdir(dir_path):
            _room = basename(dir_path).replace("S_", "")
            results_list.append(
                {
                    "room": _room,
                    "results": analyse_data_for_table(
                        room=_room,
                        data_folder=dir_path,
                        oat_folder=top,
                        start_date=start_date,
                        end_date=end_date,
                        resample_rate=resample_rate,
                        num_decimal_places=num_decimal_places,
                        num_decimal_places_big=num_decimal_places_big,
                        monthly=monthly,
                        t_range=t_range,
                        calculate_OT=calculate_OT,
                    ),
                }
            )
            study_count += 1
        if testing and study_count > 0:
            break

    # Define headers for tables
    # table 1 - results[0]
    headers_standard = [
        "Valid hours",
        "Total number of data points",
        "OAT data available %",
        "IAT data available %",
        "HET data available %",
        "OAT data available (heating on) %",
        "IAT data available (heating on) %",
        "HET data available (heating on) %",
        "OAT data available (heating off) %",
        "IAT data available (heating off) %",
        "HET data available (heating off) %",
    ]

    headers_full = [
        "Valid hours",
        "Total number of data points",
        "OAT data available %",
        "OAT data point missing",
        "IAT data available %",
        "IAT data points missing",
        "HET data available %",
        "HET data points missing",
        "number of data points available (heating on)",
        "number of data points available (heating off)",
        "OAT data available (heating on) %",
        "OAT data points missing (heating on)",
        "IAT data available (heating on) %",
        "IAT data points missing (heating on)",
        "HET data available (heating on) %",
        "HET data points missing (heating on)",
        "OAT data available (heating off) %",
        "OAT data points missing (heating off)",
        "IAT data available (heating off) %",
        "IAT data points missing (heating off)",
        "HET data available (heating off) %",
        "HET data points missing (heating off)",
    ]

    headers_base_1 = headers_full if full_data else headers_standard
    headers_1 = generate_headers(headers_base=headers_base_1)

    # table 2 - results[1]
    headers_base_2 = [
        "IAT above OT-4 %",
        "IAT above OT-3 %",
        "IAT above OT-2 %",
        "IAT above OT-1 %",
        "IAT above OT %",
        "IAT above OT-4 (while heating on) %",
        "IAT above OT-3 (while heating on) %",
        "IAT above OT-2 (while heating on) %",
        "IAT above OT-1 (while heating on) %",
        "IAT above OT (while heating on) %",
        "IAT above OT-4 (while heating off) %",
        "IAT above OT-3 (while heating off) %",
        "IAT above OT-2 (while heating off) %",
        "IAT above OT-1 (while heating off) %",
        "IAT above OT (while heating off) %",
        "IAT above OT-4 excess energy kWh",
        "IAT above OT-3 excess energy kWh",
        "IAT above OT-2 excess energy kWh",
        "IAT above OT-1 excess energy kWh",
        "IAT above OT excess energy kWh",
        "IAT above CC %",
        "IAT above CC (while heating on) %",
        "IAT above CC (while heating off) %",
        "IAT above CC excess energy kWh",
    ]

    headers_2 = generate_headers(headers_base=headers_base_2)

    # table 3 - results[2]
    headers_base_3 = [
        "TM52 Green hours",
        "TM52 Amber hours",
        "TM52 Red hours",
        "TM52 Green %",
        "TM52 Amber %",
        "TM52 Red %",
        "TM52 Green - heating on hours",
        "TM52 Amber - heating on hours",
        "TM52 Red - heating on hours",
        "TM52 Green - heating on %",
        "TM52 Amber - heating on %",
        "TM52 Red - heating on %",
        "TM52 excess energy kWh",
    ]

    headers_3 = generate_headers(
        headers_base=headers_base_3, t_range=t_range, max_concat=tab_2_max_concat
    )

    # ee = ee_list[0]

    # initialise Table 1 and 2
    table_1 = PrettyTable(headers_1)
    table_2 = PrettyTable(headers_2)

    # Generate table 1
    for row in populate_table_rows(results_list, 0):
        table_1.add_row(row)

    if file_name_prep is not None:
        file_name = f"{file_name_prep}Summary_Table - table 1"
    else:
        file_name = "Summary_Table - table 1"

    output(
        location,
        start_date,
        end_date,
        table_1,
        file_name=file_name,
        output_to_file=output_to_file,
    )
    # print(table_1)

    # Generate table 2
    for row in populate_table_rows(results_list, 1):
        table_2.add_row(row)

    if file_name_prep is not None:
        file_name = f"{file_name_prep}Summary_Table - table 2"
    else:
        file_name = "Summary_Table - table 2"

    output(
        location,
        start_date,
        end_date,
        table_2,
        file_name=file_name,
        output_to_file=output_to_file,
    )
    # print(table_2)

    # Generate table 3's
    table_rows = populate_table_rows(results_list, 2, max_concat=tab_2_max_concat)

    if isinstance(table_rows[0][0], list):  # Multiple sorted lists
        for idx, (table_list, header_list) in enumerate(zip(table_rows, headers_3)):
            table = PrettyTable(header_list)

            for row in table_list:
                table.add_row(row)

            if file_name_prep is not None:
                file_name = f"{file_name_prep}Summary_Table - table 3 chunk {idx + 1}"
            else:
                file_name = f"Summary_Table - table 3 chunk {idx + 1}"

            output(
                location,
                start_date,
                end_date,
                table,
                file_name=file_name,
                output_to_file=output_to_file,
            )
            # print(f"\nTable for Chunk {idx + 1}:")  # Clearly separate tables
            # print(table)

    else:  # Single sorted list
        table = PrettyTable(headers_3)

        for row in table_rows:
            table.add_row(row)

        if file_name_prep is not None:
            file_name = f"{file_name_prep}Summary_Table - table 3"
        else:
            file_name = "Summary_Table - table 3"

        output(
            location,
            start_date,
            end_date,
            table,
            file_name=file_name,
            output_to_file=output_to_file,
        )
        # print("\nSingle Table:")
        # print(table)

    # raise

    #     first = True

    #             ee = ee_list[0]
    #             x.add_row([basename(dir_path)] + _results)
    #             # print(ee)
    #             df1 = DataFrame(
    #                 [
    #                     ee
    #                     + [ee_list[1]]
    #                     + [ee_list[2]]
    #                     + [ee_list[3]]
    #                     + [ee_list[4]]
    #                     + [ee_list[5]]
    #                     + [ee_list[6]]
    #                 ],
    #                 columns=[
    #                     "T_4",
    #                     "T_3",
    #                     "T_2",
    #                     "T_1",
    #                     "T_0",
    #                     "cat_4-4_ee",
    #                     "cat_4-3_ee",
    #                     "cat_4-2_ee",
    #                     "cat_4-1_ee",
    #                     "cat_4-0_ee",
    #                     "cat_5_ee",
    #                 ],
    #             )
    #             if first:
    #                 first = False
    #                 df = df1
    #             else:
    #                 df = concat([df, df1])

    #     x.add_row(
    #         [
    #             "Total",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             str(round(df["cat_4-4_ee"].sum(), num_decimal_places)),
    #             str(round(df["cat_4-3_ee"].sum(), num_decimal_places)),
    #             str(round(df["cat_4-2_ee"].sum(), num_decimal_places)),
    #             str(round(df["cat_4-1_ee"].sum(), num_decimal_places)),
    #             str(round(df["cat_4-0_ee"].sum(), num_decimal_places)),
    #             "",
    #             "",
    #             "",
    #             str(round(df["cat_5_ee"].sum(), num_decimal_places)),
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             str(round(df["T_4"].sum(), num_decimal_places)),
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             str(round(df["T_3"].sum(), num_decimal_places)),
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             str(round(df["T_2"].sum(), num_decimal_places)),
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             str(round(df["T_1"].sum(), num_decimal_places)),
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             "",
    #             str(round(df["T_0"].sum(), num_decimal_places)),
    #         ]
    #     )


def output(location, start_date, end_date, x, file_name=None, output_to_file=None):

    heading = (
        location
        + " - "
        + get(start_date).format("DD-MMM-YYYY")
        + " to "
        + get(end_date).format("DD-MMM-YYYY")
    )

    # Set default file name if none is provided
    if file_name == None:
        file_name = "Summary_Table"
    # Validate the provided file name
    elif is_valid_filename(file_name) == False:
        raise Exception("Invalid filename -", str(file_name))

    # generate heading
    heading = (
        location
        + " - "
        + get(start_date).format("DD-MMM-YYYY")
        + " to "
        + get(end_date).format("DD-MMM-YYYY")
    )

    # Generate output as a PDF file
    if output_to_file == "pdf":
        generate_pdf(x, heading, file_name)

    # Generate output as an HTML file
    elif output_to_file == "html":
        # Write the table to an HTML file
        with open(file_name + ".html", "w", encoding="utf-8") as f:
            f.write(x.get_html_string())
        print("HTML file created - " + file_name + ".html")

    # Display the table in the console or notebook
    else:
        print(heading)
        display(HTML(x.get_html_string()))


def generate_pdf(x, heading, file_name):
    # Path to wkhtmltopdf executable
    path_wkhtmltopdf = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
    config = configuration(wkhtmltopdf=path_wkhtmltopdf)
    # Convert the table to HTML string
    s = x.get_html_string()
    # Add heading to the HTML content
    s = "<p><b>" + heading + "</b></p>" + s
    # Generate PDF from the HTML string
    from_string(
        s,
        file_name + ".pdf",
        configuration=config,
        options={"orientation": "Landscape", "zoom": 0.5},
        css="table.css",
    )
    print("PDF file created - " + file_name + ".pdf")
    return 0


def is_valid_filename(file_name):
    # Define a set of invalid characters (adjust as needed)
    invalid_chars = frozenset('<>:"/\\|?*')  # Windows-specific restrictions
    reserved_names = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }

    # Check if filename is empty or too long
    if not file_name or len(file_name) > 255:
        return False

    # Check if filename contains invalid characters
    if any(char in invalid_chars for char in file_name):
        return False

    # Extract the base name without the extension
    base_name = splitext(file_name)[0].upper()

    # Check if filename is a reserved system name
    if base_name in reserved_names:
        return False

    return True


def date_blocks_iterator(start_date: str, end_date: str, months: bool):
    # Convert input strings to datetime objects
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

    loop_number = 0  # Initialize loop counter

    # First block: full range with 'full' as the month name
    yield loop_number, start_date, end_date, "full"

    # If months is False, stop here
    if not months:
        return

    current_date = start_date_dt

    # First partial block (from start_date to the end of its month)
    loop_number += 1
    _start_date = start_date_dt.strftime("%Y-%m-%d %H:%M:%S")
    _end_date_dt = (start_date_dt.replace(day=1) + relativedelta(months=1)) - timedelta(
        seconds=1
    )

    if _end_date_dt > end_date_dt:
        _end_date_dt = end_date_dt

    yield loop_number, _start_date, _end_date_dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    ), start_date_dt.strftime("%B")
    current_date = _end_date_dt + timedelta(seconds=1)

    # Generate blocks for full months
    while current_date < end_date_dt:
        loop_number += 1
        _start_date_dt = datetime(current_date.year, current_date.month, 1)
        _end_date_dt = (_start_date_dt + relativedelta(months=1)) - timedelta(seconds=1)

        if _end_date_dt > end_date_dt:
            _end_date_dt = end_date_dt

        yield loop_number, _start_date_dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        ), _end_date_dt.strftime("%Y-%m-%d %H:%M:%S"), _start_date_dt.strftime("%B")

        current_date = _end_date_dt + timedelta(seconds=1)


def parse_time_period(start_date, key):
    """Parses start_date for chronological sorting. 'Full' time periods are treated separately."""
    if key == "full":
        return datetime.min  # Ensure 'Full' is always earliest

    try:
        return datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.max  # Default for malformed dates


def populate_table_rows(results_list, key, max_concat=None):
    """Processes `results_list` and returns sorted rows:
    - Ensures "BMS" is always first in room sorting.
    - Ensures "Full" is always first in time period sorting and properly formatted.
    - For `key = 0` or `1`, removes `start_date` from output and ensures 'Full' retrieval.
    - For `key = 2`, returns chunked test_number objects, concatenated per room-period pair.
    """
    consolidated_list = []
    chunked_lists = []
    room_period_order = []
    room_dict = dict(
        [
            ["B24", "North"],
            ["B25", "North"],
            ["B26", "North"],
            ["B42", "South"],
            ["B43", "South"],
            ["B44", "South"],
            ["C16", "North"],
            ["C17", "North"],
            ["C18", "North"],
            ["C33", "South"],
            ["C34", "South"],
            ["C35", "South"],
            ["D17", "North"],
            ["D18", "North"],
            ["D19", "North"],
            ["D34", "South"],
            ["D35", "South"],
            ["D36", "South"],
        ]
    )

    # Step 1: Collect all unique (room, period) pairs for sorting
    for entry in results_list:
        room = entry["room"]
        results = entry["results"]

        if key >= len(results):
            continue

        result_dict = results[key]

        for period_name in result_dict.keys():
            normalized_period = "Full" if period_name.lower() == "full" else period_name
            if "start_date" in results[key][period_name]:
                room_period_order.append(
                    (room, normalized_period, results[key][period_name]["start_date"])
                )
            else:
                test_data = next(iter(results[key][period_name].values()), None)
                room_period_order.append(
                    (room, normalized_period, test_data["start_date"])
                )

    # Step 2: Sort the (room, period,start_date) pairs with 'BMS' first and 'Full' first
    room_period_order = sorted(
        room_period_order,
        key=lambda x: (
            x[1] != "Full",  # Prioritize 'Full'
            datetime.strptime(x[2], "%Y-%m-%d %H:%M:%S"),  # Sort by date
            x[0] != "BMS",  # Ensure 'BMS' appears first
            x[0],  # Alphabetical order for others
        ),
    )

    # Removing the start_date
    room_period_order = [(item[0], item[1]) for item in room_period_order]

    row_prev_period_name = "Full"

    if key in [0, 1] or (key == 2 and max_concat is None):
        # Standard processing without chunking
        for room, period_name in room_period_order:
            for entry in results_list:
                if entry["room"] != room:
                    continue

                results = entry["results"]
                if key >= len(results):
                    continue

                result_dict = results[key]
                period_data = result_dict.get(period_name) or result_dict.get(
                    "full"
                )  # Ensure 'Full' is recognized

                if not period_data:
                    continue

                row_data = period_data.get("data", [])  # Remove `start_date`
                row_prepend = [room, period_name, room_dict.get(room, "-")]

                if period_name != row_prev_period_name:
                    consolidated_list.append(["-"] * (len(row_data) + len(row_prepend)))

                row_prev_period_name = period_name

                consolidated_list.append(row_prepend + row_data)

        consolidated_list.append(["-"] * (len(row_data) + len(row_prepend)))
        return consolidated_list  # No chunking for key = 0 or 1, or key = 2 without max_concat

    # **Chunking Logic for key = 2**
    test_data_grouped = {}  # Structure: {test_number: {(room, period): data}}

    row_prev_period_name = "Full"

    for entry in results_list:
        room = entry["room"]
        results = entry["results"]

        if key >= len(results):
            continue

        result_dict = results[key]

        for period_name in result_dict.keys():
            normalized_period = "Full" if period_name.lower() == "full" else period_name
            sorted_tests = sorted(
                result_dict[period_name].items()
            )  # Ensure test_numbers are sorted

            for test_num, test_data in sorted_tests:
                if not isinstance(test_data, dict) or "data" not in test_data:
                    continue

                identifier = (room, normalized_period)  # Unique (room, period) pair
                if test_num not in test_data_grouped:
                    test_data_grouped[test_num] = {}

                test_data_grouped[test_num][identifier] = test_data["data"]

    # Step 2: Chunk test_number objects across rooms and periods
    sorted_test_numbers = sorted(
        test_data_grouped.keys()
    )  # Sort test_numbers for consistency

    # Chunk the sorted test numbers into groups of size `max_concat` for processing
    for i in range(0, len(sorted_test_numbers), max_concat):
        chunk = sorted_test_numbers[
            i : i + max_concat
        ]  # Select up to max_concat test numbers
        chunk_data = {}

        # Concatenate test_number data for each room-period pair in sorted order
        for room, period_name in room_period_order:
            for test_num in chunk:
                if (
                    test_num in test_data_grouped
                    and (room, period_name) in test_data_grouped[test_num]
                ):
                    if (room, period_name) not in chunk_data:
                        chunk_data[(room, period_name)] = [
                            room,
                            period_name,
                            room_dict.get(room, "-"),
                        ]  # Start with room & period
                    chunk_data[(room, period_name)] += test_data_grouped[test_num][
                        (room, period_name)
                    ]  # Append test_number data
            # print(chunk_data)

        # Convert dictionary into a list format for return
        chunked_lists.append(list(chunk_data.values()))

    return chunked_lists  # Returns multiple lists, each containing max_concat test_number objects


def generate_headers(headers_base, t_range=None, max_concat=None):
    """Generates nested headers aligned with populate_table_rows output for key=2.
    - If max_concat is set, chunk headers accordingly.
    - If max_concat is None, return a single flattened list.
    """
    if t_range != None:
        # sorted_test_numbers = list(range(t_range, -1, -1))
        sorted_test_numbers = list(
            range(0, t_range + 1)
        )  # Ensure correct order [0,1,2,3,4] for t_range=4
    if t_range == None:
        return ["Room", "Time Period", "Direction"] + headers_base

    elif max_concat is None:
        # Return a single flattened list containing all test_number headers
        headers = ["Room", "Time Period", "Direction"]
        for test_num in sorted_test_numbers:
            headers.extend(
                [f"(CT+{test_num}) {header}" for header in headers_base]
            )  # Prepend test_num
        return headers  # Flat list

    # Otherwise, return chunked lists based on max_concat
    headers_list = []
    for i in range(0, len(sorted_test_numbers), max_concat):
        chunk = sorted_test_numbers[
            i : i + max_concat
        ]  # Select up to max_concat test_numbers
        chunk_headers = [
            "Room",
            "Time Period",
            "Direction",
        ]  # Start each chunk with 'Room'

        for test_num in chunk:
            chunk_headers.extend(
                [f"(CT+{test_num}) {header}" for header in headers_base]
            )  # Prepend test_num

        headers_list.append(chunk_headers)

    return headers_list  # Returns nested list of headers
