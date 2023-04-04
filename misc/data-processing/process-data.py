import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np

COLUMN_NAME = "Container CPU usage"
FILE_NAME = "container_cpu_usage"


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

    def representative_name(self):
        split_name = self.name.split("/")
        return f"{split_name[1]} - {split_name[2]} peers"


def discover_experiments(root='13-06'):
    return [Experiment(ex) for ex in [x[0] for x in os.walk(root) if len(x[0].split("/")) == 3]]


experiments = discover_experiments()


def find_and_sort_results(results, protocol):
    def sort_func(res):
        return int(res[0].split(" ")[2])

    results = [res for res in results if protocol in res[0]]
    results.sort(key=sort_func)
    return results


def prepare_experiments():
    results = [[ex.representative_name(), ex.calculate_mean_and_std_from_experiment()] for ex in experiments]
    return find_and_sort_results(results, "gpac"), find_and_sort_results(results, "consensus"), find_and_sort_results(
        results, "2pc")


def plot_experiments():
    width = 0.2
    x = np.arange(5)
    gpac, consensus, pc = prepare_experiments()

    fig, ax = plt.subplots()
    ax.bar(x - width, [x[1][0] for x in gpac], width, label="Gpac", yerr=[x[1][1] for x in gpac])
    ax.bar(x, [x[1][0] for x in consensus], width, label="Raft", yerr=[x[1][1] for x in consensus])
    # ax.bar(x + width, [x[1][0] for x in pc], width, label="2 PC", yerr=[x[1][1] for x in pc])

    ax.set_title(COLUMN_NAME)
    ax.set_ylabel(f"{COLUMN_NAME} [s]")
    ax.set_xticks(x)
    ax.set_xticklabels([x[0].split(" - ")[1] for x in gpac])
    ax.legend()

    plt.savefig(f"results/{FILE_NAME}.png")


if __name__ == "__main__":
    plot_experiments()
