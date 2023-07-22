#############################################################################
#   Author: junghoon@rescale.com                                            #
#   Last updated: Jul 16, 2023                                              #
#   Description: Get a job information by workspace admin                   #
#   How to use: Just run this script                                        #
#############################################################################


from datetime import datetime
import os
import bokeh
import pandas as pd
import matplotlib.pyplot as plt


def generateDataframe(path_input, file_input, path_output):
    df_raw = pd.read_csv(path_input + '/' + file_input)
    df_raw = df_raw.drop(["Workspace ID", "Project ID", "ID", "Name", "Rescale On-Demand License", "License Settings", "Unit Charge/Hour", "Walltime", "SKU"], axis=1)
    df_compute = df_raw[df_raw["Type"] == "Compute"].reset_index(drop=True)
    df_compute["Coretype"] = df_compute["Description"].str.extract(r':\s(.*)')
    df_compute["Cores"] = df_compute["Cores"].astype(int)
    df_compute["Charge"] = df_compute["Charge"].str.replace("$", "")
    df_compute["Charge"] = df_compute["Charge"].astype(float)
    df_compute["Submit Date"] = pd.to_datetime(df_compute["Submit Date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_compute["Start Date"] = pd.to_datetime(df_compute["Submit Date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_compute["Stop Date"] = pd.to_datetime(df_compute["Submit Date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_compute = df_compute.drop(["Description"], axis=1)
    column_order = ["Month", "Type", "Project Name", "User", "Job Type", "Software", 
                    "Coretype", "Billing Priority", "Cores", "Submit Date", "Start Date", "Stop Date", "Unit Hours", "Charge"]
    df_compute = df_compute[column_order]
    df_compute.columns = ["Month", "Category", "Project Name", "User", "Job Type", "Software", 
                        "Coretype", "Billing Priority", "Cores", "Submit Date", "Start Date", "Stop Date", "Usage", "Charge [$]"]
    df_storage = df_raw[df_raw["Type"] == "Storage"].reset_index(drop=True)
    df_storage = df_storage.drop(["Project Name", "Software", "Submit Date", "Start Date", "Stop Date", "Billing Priority", "Cores", "Unit Hours", "Job Type"], axis=1)
    df_storage["Usage"] = df_storage["Description"].str.extract(r':\s(.*)')
    df_storage["Charge"] = df_storage["Charge"].str.replace("$", "")
    df_storage["Charge"] = df_storage["Charge"].astype(float)
    df_storage = df_storage.drop(["Description"], axis=1)
    column_order = ["Month", "Type", "User", "Usage", "Charge"]
    df_storage = df_storage[column_order]
    df_storage.columns = ["Month", "Category", "User", "Usage", "Charge [$]"]
    df_connection = df_raw[df_raw["Type"] == "License Proxy"].reset_index(drop=True)
    df_connection = df_connection.drop(["Project Name", "Software", "Submit Date", "Start Date", "Stop Date", "Billing Priority", "Cores", "Unit Hours", "Job Type", "Description"], axis=1)
    df_connection["Charge"] = df_connection["Charge"].str.replace("$", "")
    df_connection["Charge"] = df_connection["Charge"].astype(float)
    column_order = ["Month", "Type", "User", "Charge"]
    df_connection = df_connection[column_order]
    df_connection.columns = ["Month", "Category", "User", "Charge [$]"]
    df_transfer = df_raw[df_raw["Type"] == "Transfer"].reset_index(drop=True)
    df_transfer = df_transfer.drop(["Project Name", "Software", "Submit Date", "Start Date", "Stop Date", "Billing Priority", "Cores", "Unit Hours", "Job Type"], axis=1)
    df_transfer["Usage"] = df_transfer["Description"].str.extract(r':\s(.*)')
    df_transfer["Charge"] = df_transfer["Charge"].str.replace("$", "")
    df_transfer["Charge"] = df_transfer["Charge"].astype(float)
    df_transfer = df_transfer.drop(["Description"], axis=1)
    column_order = ["Month", "Type", "User", "Usage", "Charge"]
    df_transfer = df_transfer[column_order]
    df_transfer.columns = ["Month", "Category", "User", "Usage", "Charge [$]"]
    df = pd.concat([df_compute, df_connection, df_storage, df_transfer], axis=0, ignore_index=True)
    return df


def main():
    path_input = 'Billing summaries'
    workspaceid = '08-773312582'
    period = '2023_04-2023_06'
    file_input = 'billing_' + workspaceid  + '_' + period + '.csv'
    path_output = 'Dataframe'
    df = generateDataframe(path_input, file_input, path_output)
    output = path_output + '/' + 'df_' + workspaceid + '.csv'
    if os.path.exists(output):
        print("Dataframe is already existing")
    else:
        df.to_csv(output, mode="w", index=False)


if __name__ == '__main__':
    main()
