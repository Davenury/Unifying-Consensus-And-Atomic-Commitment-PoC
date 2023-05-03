import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import itertools

scripts = ["consensus", "stress-consensus-test"]
# scripts = ["consensus"]
# scripts = ["stress-consensus-test"]
protocols = ["alvin", "paxos", "raft"]

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


def find_leader(prefix_path):
    for file in os.listdir(prefix_path):
        full_path = os.path.join(prefix_path, file)
        if os.path.isfile(full_path) and file.startswith("Leader elected"):
            df = pd.read_csv(full_path, na_values="undefined").drop(
                ["Time"], axis=1)
            last_row_matching_value = df.loc[(df == 1).any(axis=1)].tail(1)
            if not last_row_matching_value.empty:
                is_matching_value = last_row_matching_value.iloc[0].eq(1)
                matching_column = is_matching_value.idxmax()
                return matching_column
    return None


class Experiment:
    def __init__(self, name, file_path, leader_id, leader_option):
        self.name = name
        self.df = self._read_csvs(file_path)
        self.leader_id = leader_id
        # -1 - dont do anything, 0 - drop leaders, 1 - leave only leaders
        self.leader_option = leader_option

    def _read_csvs(self, file_path):
        return pd.read_csv(file_path, na_values="undefined").drop(["Time"], axis=1)

    def calculate_mean_and_std_from_experiment(self):
        # return self.df[COLUMN_NAME].mean(), self.df[COLUMN_NAME].std()
        # self.df['keep'] = self.df.apply(lambda row: all([(x > 100000000) for x in row]), axis=1)
        # self.df = self.df.drop(self.df[self.df.keep != True].index)
        # self.df = self.df.drop(["keep"], axis=1)
        return np.nanmean(self.df.to_numpy()), np.nanstd(self.df.to_numpy())

    def calculate_box_plot_values(self):
        self.df = self.df[self.df.columns.drop(
            list(self.df.filter(regex='nonheap')))]
        self.df = self.df.dropna()

        if self.leader_option == 1:
            self.leave_leaders()
        if self.leader_option == 0:
            self.drop_leaders()

        data = self.df.to_numpy().flatten()
        # data = [1/x for x in data]

        bp = plt.boxplot(data)
        dict1 = {}
        dict1['lower-whisker'] = bp['whiskers'][0].get_ydata()[1]
        dict1['lower-quartile'] = bp['boxes'][0].get_ydata()[1]
        dict1['median'] = bp['medians'][0].get_ydata()[1]
        dict1['upper-quartile'] = bp['boxes'][0].get_ydata()[2]
        dict1['upper-whisker'] = bp['whiskers'][1].get_ydata()[1]
        if self.leader_option != -1:
            dict1['peer-role'] = "leader" if self.leader_option == 1 else "cohort"
        return dict1

    def leave_leaders(self):
        regex = "|".join(self.leader_id)
        self.df = self.df.filter(regex=regex)

    def drop_leaders(self):
        for leader in [self.leader_id]:
            self.df = self.df[self.df.columns.drop(
                list(self.df.filter(regex=leader)))]

    def _inner_name(self):
        split = self.name.split("/")
        return f"{split[1]}/{split[2]}"

    def representative_name(self):
        split_name = self.name.split("/")
        return f"{split_name[0]} - {split_name[1]} peers"


def find_and_sort_results(results, protocol):
    def sort_func(res):
        return int(res[0].split(" ")[2].split("x")[0])

    results = [res for res in results if protocol in res[0]]
    results.sort(key=sort_func)
    return results


def prepare_experiments(experiments):
    results = [(ex.representative_name(), ex.calculate_box_plot_values())
               for ex in experiments]

    return [find_and_sort_results(results, protocol) for protocol in protocols]


def values_from_experiment(ex):
    return {
        "name": ex[0],
        **ex[1]
    }


def get_values(experiments):
    all = [item for sublist in prepare_experiments(
        experiments) for item in sublist]
    return [values_from_experiment(a) for a in all]


def get_data_in_csv(script, new_file_name, experiments):
    values = get_values(experiments)
    df = pd.DataFrame.from_records(values)

    from pathlib import Path

    output_dir = Path.joinpath(
        Path(os.getcwd()), f"parsed_csvs/{script}")

    output_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(f"parsed_csvs/{script}/{new_file_name}.csv",
              sep=",", index=False)


def merge_leaders_and_non_leaders(leaders_file, nonleaders_file):
    leaders = pd.read_csv(leaders_file)
    leaders['name'] = leaders['name'].apply(lambda x: f"{x} - leaders")
    nonleaders = pd.read_csv(nonleaders_file)
    nonleaders['name'] = nonleaders['name'].apply(lambda x: f"{x} - cohorts")

    result = pd.concat([leaders, nonleaders], ignore_index=True)
    return result[result['name'].str.contains('5')]

# def plot_experiments():
#     width = 0.2
#     x = np.arange(5)
#     gpac, consensus, pc = prepare_experiments()
#
#     fig, ax = plt.subplots()
#     ax.bar(x - width, [x[1][0] for x in gpac], width, label="Gpac", yerr=[x[1][1] for x in gpac])
#     ax.bar(x, [x[1][0] for x in consensus], width, label="Raft", yerr=[x[1][1] for x in consensus])
#     # ax.bar(x + width, [x[1][0] for x in pc], width, label="2 PC", yerr=[x[1][1] for x in pc])
#
#     ax.set_title(COLUMN_NAME)
#     ax.set_ylabel(f"{COLUMN_NAME} [s]")
#     ax.set_xticks(x)
#     ax.set_xticklabels([x[0].split(" - ")[1] for x in gpac])
#     ax.legend()
#
#     plt.savefig(f"results/{FILE_NAME}.png")


if __name__ == "__main__":

    current_directory = os.getcwd()

    for script in scripts:
        for prefix in all_files_prefix:
            experiments = []
            for protocol in protocols:
                new_path = os.path.join(current_directory, script, protocol)
                if prefix in splitted_prefixes:
                    experiments = []
                for peerset_size in os.listdir(new_path):
                    experiment_dir_path = os.path.join(new_path, peerset_size)
                    leader_id = find_leader(experiment_dir_path)

                    for experiment in os.listdir(experiment_dir_path):
                        if experiment.startswith(prefix):
                            experiment_path = os.path.join(
                                experiment_dir_path, experiment)
                            if prefix in splitted_prefixes:
                                experiments.append(Experiment(f"{protocol}/{peerset_size}/{experiment}",
                                                              experiment_path, leader_id, 1))
                                experiments.append(Experiment(f"{protocol}/{peerset_size}/{experiment}",
                                                              experiment_path, leader_id, 0))
                                get_data_in_csv(
                                    script, f"{prefix}-{protocol}", experiments)

                            else:
                                experiments.append(Experiment(f"{protocol}/{peerset_size}/{experiment}",
                                                              experiment_path, leader_id, -1))
            if prefix not in splitted_prefixes:
                get_data_in_csv(script, f"{prefix}", experiments)
