import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import itertools

COLUMN_NAME = "avg commit latency"
FILE_NAME = "network-bytes-received"
new_file_name = "avg-commit-latency"

leaders = {
    "gpac/3x2": ["peer0-peerset0", "peer0-peerset1"],
    "gpac/4x2": ["peer0-peerset0", "peer0-peerset1"],
    "gpac/5x2": ["peer0-peerset0", "peer0-peerset1"],
    "gpac/6x2": ["peer0-peerset0", "peer0-peerset1"],
    "gpac/10x2": ["peer0-peerset0", "peer0-peerset1"],
    "gpac/20x2": ["peer0-peerset0", "peer0-peerset1"],
    "raft/3x1": ["peer2-peerset0"],
    "raft/4x1": ["peer0-peerset0"],
    "raft/5x1": ["peer0-peerset0"],
    "raft/6x1": ["peer5-peerset0"],
    "raft/10x1": ["peer4-peerset0"],
    "raft/20x1": ["peer19-peerset0"],
    "2pc/3x2": ["peer1-peerset0", "peer0-peerset1"],
    "2pc/4x2": ["peer1-peerset0", "peer3-peerset1"],
    "2pc/5x2": ["peer3-peerset0", "peer0-peerset1"],
    "2pc/6x2": ["peer4-peerset0", "peer5-peerset1"],
    "2pc/10x2": ["peer5-peerset0", "peer4-peerset1"],
    "2pc/20x2": ["peer4-peerset0", "peer0-peerset1"],
}

SWAP_LEADERS = True

if SWAP_LEADERS:
    leaders2 = {}
    for name, leader in leaders.items():
        swapped = ["-".join(l.split("-")[::-1]) for l in leader]
        leaders2[name] = swapped
    leaders = leaders2

LEADERS = -1  # -1 - dont do anything, 0 - drop leaders, 1 - leave only leaders


class Experiment:
    def __init__(self, name):
        self.name = name
        self.df = self._read_csvs()

    def _read_csvs(self):
        return pd.read_csv(f"{self.name}/{FILE_NAME}.csv", na_values="undefined").drop(["Time"], axis=1)

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

        if LEADERS == 1:
            self.leave_leaders()
        if LEADERS == 0:
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
        return dict1

    def leave_leaders(self):
        regex = "|".join(leaders[self._inner_name()])
        self.df = self.df.filter(regex=regex)

    def drop_leaders(self):
        for leader in leaders[self._inner_name()]:
            self.df = self.df[self.df.columns.drop(
                list(self.df.filter(regex=leader)))]

    def _inner_name(self):
        split = self.name.split("/")
        return f"{split[1]}/{split[2]}"

    def representative_name(self):
        split_name = self.name.split("/")
        return f"{split_name[1]} - {split_name[2]} peers"


def discover_experiments(root='results'):
    return [Experiment(ex) for ex in [x[0] for x in os.walk(root) if len(x[0].split("/")) == 3]]


# experiments = discover_experiments()


def find_and_sort_results(results, protocol):
    def sort_func(res):
        return int(res[0].split(" ")[2].split("x")[0])

    results = [res for res in results if protocol in res[0]]
    results.sort(key=sort_func)
    return results


def prepare_experiments():
    results = [(ex.representative_name(), ex.calculate_box_plot_values())
               for ex in experiments]
    return find_and_sort_results(results, "gpac"), find_and_sort_results(results, "raft"), find_and_sort_results(
        results, "2pc")


def values_from_experiment(ex):
    return {
        "name": ex[0],
        **ex[1]
    }


def get_values():
    gpac, raft, twopc = prepare_experiments()
    all = list(itertools.chain(gpac, raft, twopc))
    return [values_from_experiment(a) for a in all]


def get_data_in_csv():
    values = get_values()
    df = pd.DataFrame.from_records(values)
    df.to_csv(f"parsed_csvs/fixes/{new_file_name}.csv", sep=",", index=False)


def separate_ac_from_consensus(df):
    def sort_func(input):
        def inner(k):
            return int(k.split(" - ")[1].split("x")[0]), 0 if "gpac" in k else 1, 0 if "cohort" in k else 1
        return input.apply(lambda x: inner(x))

    consensus = df.loc[df['name'].str.contains('raft')]
    ac = df.loc[(df['name'].str.contains('2pc')) |
                (df['name'].str.contains('gpac'))]

    consensus = consensus.sort_values(by=['name'], key=sort_func)
    ac = ac.sort_values(by=['name'], key=sort_func)

    consensus.to_csv(
        f"parsed_csvs/fixes/{FILE_NAME}-consensus.csv", index=False)
    ac.to_csv(f"parsed_csvs/fixes/{FILE_NAME}-ac.csv", index=False)


def separate_ac_from_consensus_file(file):
    separate_ac_from_consensus(pd.read_csv(file))


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
    # separate_ac_from_consensus("parsed_csvs/fixes/avg-commit-latency.csv")
    get_data_in_csv()
    separate_ac_from_consensus(merge_leaders_and_non_leaders(
        f"parsed_csvs/{FILE_NAME}-leaders.csv", f"parsed_csvs/{FILE_NAME}-non-leaders.csv"))
