from __future__ import annotations
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
from typing import List
from functools import reduce

scripts = ["consensus", "stress-consensus-test"]

# scripts = ["consensus"]
# scripts = ["stress-consensus-test"]
# protocols = ["alvin", "paxos","raft", "oldRaft"]
protocols = ["alvin", "paxos","raft"]

all_files_prefix = [
    "Avg commit latency",
    "AVG Container memory usage - all",
    "AVG Container memory usage - only application",
    "Container CPU usage",
    "RPS on processed changes"
]

splitted_prefixes = [
    "AVG Container memory usage - all",
    "AVG Container memory usage - only application",
    "Container CPU usage",
]
leader_prefix="Leader elected"

current_directory = os.getcwd()


def find_leader(prefix_path, idx=0):
    files_with_leader_prefix=[file for file in os.listdir(prefix_path) if file.startswith(leader_prefix)]
    file = files_with_leader_prefix[idx]
    full_path = os.path.join(prefix_path, file)
    if os.path.isfile(full_path):
        df = pd.read_csv(full_path, na_values="undefined").drop(["Time"], axis=1)
        last_row_matching_value = df.loc[(df == 1).any(axis=1)].tail(1)

        if not last_row_matching_value.empty:
            is_matching_value = last_row_matching_value.iloc[0].eq(1)
            matching_column = is_matching_value.idxmax()
            return matching_column
        else:
            return df.columns[0]
            
    return None


class Experiment:
    def __init__(self: Experiment, file_path, leader_id, leader_option, protocol,size, name, df = None):
        if df is None:
            self.df = self._read_csvs(file_path)
        else:
            self.df = df
        self.leader_id = leader_id
        # -1 - dont do anything, 0 - drop leaders, 1 - leave only leaders
        self.leader_option = leader_option
        self.protocol = protocol
        self.size = size
        self.name = name

    def _read_csvs(self: Experiment, file_path):
        return pd.read_csv(file_path, na_values="undefined").drop(["Time"], axis=1)

    def calculate_mean_and_std_from_experiment(self: Experiment):
        # return self.df[COLUMN_NAME].mean(), self.df[COLUMN_NAME].std()
        # self.df['keep'] = self.df.apply(lambda row: all([(x > 100000000) for x in row]), axis=1)
        # self.df = self.df.drop(self.df[self.df.keep != True].index)
        # self.df = self.df.drop(["keep"], axis=1)
        return np.nanmean(self.df.to_numpy()), np.nanstd(self.df.to_numpy())

    def calculate_box_plot_values_commit_latency(self: Experiment):
        self.df = self.df[self.df.columns.drop(
            list(self.df.filter(regex='nonheap')))]
        self.df = self.df.dropna()

        if self.leader_option == 1:
            self.leave_leaders()
        if self.leader_option == 0:
            self.drop_leaders()

        data = self.df.to_numpy().flatten()
        data = [1/x for x in data]

        bp = plt.boxplot(data)
        dict1 = {}
        dict1['lower-whisker'] = bp['whiskers'][0].get_ydata()[1]
        dict1['lower-quartile'] = bp['boxes'][0].get_ydata()[1]
        dict1['median'] = bp['medians'][0].get_ydata()[1]
        dict1['upper-quartile'] = bp['boxes'][0].get_ydata()[2]
        dict1['upper-whisker'] = bp['whiskers'][1].get_ydata()[1]
        dict1['Protocol'] = self.protocol
        dict1['Size'] = self.size.split("x")[0]
        dict1['Type'] = self.protocol
        # dict1['Name'] = self.representative_name()
        return dict1 
        
    def calculate_mean_std_values_resource_usage(self: Experiment):
        self.df = self.df[self.df.columns.drop(
            list(self.df.filter(regex='nonheap')))]
        self.df = self.df.dropna()

        if self.leader_option == 1:
            self.leave_leaders()
        if self.leader_option == 0:
            self.drop_leaders()

        (mean, std) = self.calculate_mean_and_std_from_experiment()

        dict1 = {}

        dict1["mean"] = mean
        dict1["std"] = std
        dict1['Protocol'] = self.protocol
        dict1['Name'] = self.name
        dict1['Size'] = self.size
        dict1['Type'] = self.protocol
        if self.leader_option != -1:
            dict1['Role'] = "leader" if self.leader_option == 1 else "cohorts"
            dict1["Type"] = f"{self.protocol}-{dict1['Role']}"
        return dict1
    


    def leave_leaders(self: Experiment):
        if self.leader_id is not None:
            self.df = self.df.loc[:, self.df.columns.str.startswith(self.leader_id)]

    def drop_leaders(self: Experiment):
        if self.leader_id is not None:
            self.df = self.df.drop(self.df.filter(regex=self.leader_id).columns,axis=1)


    def representative_name(self: Experiment):
        return f"{self.protocol} {self.size}"


def find_and_sort_results(results, protocol):
    def sort_func(res):
        return int(res[0].split(" ")[1].split("x")[0])

    results = [res for res in results if protocol in res[0]]
    results.sort(key=sort_func)
    return results


def prepare_experiments_latency(experiments: List[Experiment]):
    results = [(ex.representative_name() ,ex.calculate_box_plot_values_commit_latency())
               for ex in experiments]

    return [find_and_sort_results(results, protocol) for protocol in protocols]

def prepare_experiments_resource_usage(experiments: List[Experiment]):
    results = [(ex.representative_name() ,ex.calculate_mean_std_values_resource_usage())
               for ex in experiments]

    return [find_and_sort_results(results, protocol) for protocol in protocols]


def values_from_experiment(ex):
    return {
        **ex[1],
        "name": ex[0],
    }


def get_values_latency(experiments):
    all = [item for sublist in prepare_experiments_latency(
        experiments) for item in sublist]
    return [values_from_experiment(a) for a in all]

def get_values_resource_usage(experiments):
    all = [item for sublist in prepare_experiments_resource_usage(
        experiments) for item in sublist]
    return [values_from_experiment(a) for a in all]


def get_data_in_csv(script, new_file_name, experiments, prepare_values, add_index=False, index_label=""):
    values = prepare_values(experiments)
    df = pd.DataFrame.from_records(values)

    from pathlib import Path

    output_dir = Path.joinpath(
        Path(os.getcwd()), f"parsed_csvs/{script}")

    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(f"parsed_csvs/{script}/{new_file_name}.csv",
              sep=",", index=add_index, index_label=index_label)


def merge_leaders_and_non_leaders(leaders_file, nonleaders_file):
    leaders = pd.read_csv(leaders_file)
    leaders['name'] = leaders['name'].apply(lambda x: f"{x} - leaders")
    nonleaders = pd.read_csv(nonleaders_file)
    nonleaders['name'] = nonleaders['name'].apply(lambda x: f"{x} - cohorts")

    result = pd.concat([leaders, nonleaders], ignore_index=True)
    return result[result['name'].str.contains('5')]

def stress_test_csv():
    directory = "stress-test-during-processing-changes"    

    for protocol in protocols:
        new_path = os.path.join(current_directory, directory,protocol)
        experiments = []

        for peerset_size in os.listdir(new_path):
            df_same_size = []
            experiment_dir_path = os.path.join(new_path, peerset_size)
            for experiment in os.listdir(experiment_dir_path):
                experiment_path = os.path.join(experiment_dir_path, experiment)
                if experiment.startswith("Avg commit latency"):
                    df_same_size.append(Experiment(experiment_path, -1, -1,protocol, peerset_size, "during changes"))
            merge_df = pd.concat([df.df for df in df_same_size])
            experiments.append(Experiment(experiment_dir_path, -1, -1,protocol, peerset_size, "during changes", df=merge_df))

        get_data_in_csv("stress-tests", f"avg-commit-latency-{protocol}", experiments,lambda exs: get_values_latency(exs), True, "Index")

def resource_usage_csv():
    directory = "resource-usage-during-processing-changes"
    get_resource_dataframes(directory, "during changes","resource usage")


def resource_usage_after_csv():
    directory = "resource-usage-after-processing-changes"
    get_resource_dataframes(directory, "idle","resource usage after")


def get_resource_dataframes(directory, experiment_name: str, subdirectory: str):
    prefixes = {
        "AVG Container memory usage - only": "avg-container-memory-usage-application",
        "Container CPU usage": "cpu-usage",
        "Network bytes received": "network-bytes",
    }
    for protocol in protocols:
        new_path = os.path.join(current_directory, directory,protocol)

        for peerset_size in os.listdir(new_path):
            experiment_dir_path = os.path.join(new_path, peerset_size)
            for prefix in prefixes:
                file_name = prefixes[prefix]
                experiments=[]
                leader_experiments = []
                cohort_experiments = []
                paths_with_prefix=[path for path in os.listdir(experiment_dir_path) if path.startswith(prefix)]

                for idx,experiment in enumerate(paths_with_prefix):
                    leader_id = find_leader(experiment_dir_path,idx)
                    experiment_path = os.path.join(experiment_dir_path, experiment)
                    if prefix is not None:
                        leader_experiments.append(Experiment(experiment_path, leader_id, 1,protocol, peerset_size, experiment_name))
                        cohort_experiments.append(Experiment(experiment_path, leader_id, 0,protocol, peerset_size, experiment_name))
                leader_dfs=[]
                for leader_experiment in leader_experiments:
                    leader_experiment.leave_leaders()
                    df=leader_experiment.df
                    df=df.set_axis(['leader'], axis=1, inplace=False)
                    leader_dfs.append(df)
                leader_df = pd.concat(leader_dfs)
                experiments.append(Experiment(experiment_dir_path, None, 1,protocol, peerset_size, experiment_name, df=leader_df))

                cohort_dfs=[]
                for cohort_experiment in cohort_experiments:
                    cohort_experiment.drop_leaders()
                    df = cohort_experiment.df
                    df=df.set_axis([f"cohort-{id}" for id in range(4)], axis=1, inplace=False)
                    cohort_dfs.append(df)
                cohort_df = pd.concat(cohort_dfs)
                experiments.append(Experiment(experiment_dir_path, None, 0,protocol, peerset_size, experiment_name, df=cohort_df))

                get_data_in_csv(subdirectory, f"{file_name}-{protocol}", experiments, lambda exs:  get_values_resource_usage(exs), True, "Index")


def new_value(row):
    return 1 if sum(row[0:]) > 0 else 0


def delete_followers_csv():
    directory = "ft-half-followers-after-deleting-two-peers"

    for protocol in protocols:
        new_path = os.path.join(current_directory, directory,protocol)

        for peerset_size in os.listdir(new_path):
            experiment_dir_path = os.path.join(new_path, peerset_size)
            # leader_id = find_leader(experiment_dir_path)
            for experiment in os.listdir(experiment_dir_path):
                experiment_path = os.path.join(experiment_dir_path, experiment)
                dir_path = f"parsed_csvs/delete-followers/{protocol}"
                if experiment.startswith("RPS"):
                    df = pd.read_csv(experiment_path, na_values="undefined")
                    df=df.set_axis(['Time','rps'], axis=1, inplace=False)
                    df['Time'] -= df['Time'].min()
                    df['Time'] /= 1000
                    from pathlib import Path
                    output_dir = Path.joinpath(Path(os.getcwd()), dir_path)
                    output_dir.mkdir(parents=True, exist_ok=True)
                    df.to_csv(f"{dir_path}/rps.csv",sep=",", index=True, index_label="Index")

                if experiment.startswith("Chaos"):
                    df = pd.read_csv(experiment_path, na_values="undefined")
                    df['chaos-phase'] = df.apply(lambda row: new_value(row[1:]),axis=1) 
                    df['Time'] -= df['Time'].min()
                    df['Time'] /= 1000
                    df = df[['Time', 'chaos-phase']]
                    df = df[df["chaos-phase"]==1]
                    df.reset_index(drop=True)
                    from pathlib import Path
                    output_dir = Path.joinpath(Path(os.getcwd()), dir_path)
                    output_dir.mkdir(parents=True, exist_ok=True)
                    df.to_csv(f"{dir_path}/chaos-phases.csv",sep=",", index=True, index_label="Index")


def delete_leaders_csv():
    directory = "ft-leader-after-deleting-leaders"

    for protocol in protocols:
        new_path = os.path.join(current_directory, directory,protocol)

        for peerset_size in os.listdir(new_path):
            experiment_dir_path = os.path.join(new_path, peerset_size)
            # leader_id = find_leader(experiment_dir_path)
            for experiment in os.listdir(experiment_dir_path):
                experiment_path = os.path.join(experiment_dir_path, experiment)
                dir_path = f"parsed_csvs/delete-leaders/{protocol}"
                if experiment.startswith("RPS"):
                    df = pd.read_csv(experiment_path, na_values="undefined")
                    df = df.set_axis(['Time', 'rps'], axis=1, inplace=False)
                    df['Time'] -= df['Time'].min()
                    df['Time'] /= 1000
                    from pathlib import Path
                    output_dir = Path.joinpath(Path(os.getcwd()), dir_path)
                    output_dir.mkdir(parents=True, exist_ok=True)
                    df.to_csv(f"{dir_path}/rps.csv",sep=",", index=True, index_label="Index")

                if experiment.startswith("Chaos"):
                    df = pd.read_csv(experiment_path, na_values="undefined")
                    df['chaos-phase'] = df.apply(lambda row: new_value(row[1:]),axis=1) 
                    df['Time'] -= df['Time'].min()
                    df['Time'] /= 1000
                    df = df[['Time', 'chaos-phase']]
                    df = df[df["chaos-phase"]==1]
                    df.reset_index(drop=True)
                    from pathlib import Path
                    output_dir = Path.joinpath(Path(os.getcwd()), dir_path)
                    output_dir.mkdir(parents=True, exist_ok=True)
                    df.to_csv(f"{dir_path}/chaos-phases.csv",sep=",", index=True, index_label="Index")

def synchronization_time_csv():
    directory = "ft-follower-after-deleting-follower"

    experiments = []
    for protocol in protocols:
        new_path = os.path.join(current_directory, directory,protocol)

        for peerset_size in os.listdir(new_path):
            experiment_dir_path = os.path.join(new_path, peerset_size)
            dfs=[]
            paths_with_prefixes = [path for path in os.listdir(experiment_dir_path) if path.startswith("Synchronization")]
            for experiment in paths_with_prefixes:
                experiment_path = os.path.join(experiment_dir_path, experiment)
                dfs.append(Experiment(experiment_path, None, -1,protocol, peerset_size, "synchronization"))
            merged_df = pd.concat([df.df for df in dfs])
            experiments.append(Experiment(experiment_path, None, -1,protocol, peerset_size, "synchronization", merged_df))
        
    get_data_in_csv("synchronization-time", f"synchronization-time", experiments, lambda exs:  get_values_resource_usage(exs), True, "Index")

if __name__ == "__main__":

    # stress_test_csv()
    # resource_usage_csv()
    # resource_usage_after_csv()
    delete_followers_csv()
    # delete_leaders_csv()
    # synchronization_time_csv()

