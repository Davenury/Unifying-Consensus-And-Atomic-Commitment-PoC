import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import itertools

COLUMN_NAME = "changes per second"
FILE_NAME = "changes_per_second"


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
        bp = plt.boxplot(self.df.to_numpy().flatten())
        dict1 = {}
        dict1['lower_whisker'] = bp['whiskers'][0].get_ydata()[1]
        dict1['lower_quartile'] = bp['boxes'][0].get_ydata()[1]
        dict1['median'] = bp['medians'][0].get_ydata()[1]
        dict1['upper_quartile'] = bp['boxes'][0].get_ydata()[2]
        dict1['upper_whisker'] = bp['whiskers'][1].get_ydata()[1]
        return dict1

    def representative_name(self):
        split_name = self.name.split("/")
        return f"{split_name[1]} - {split_name[2]} peers"


def discover_experiments(root='results'):
    return [Experiment(ex) for ex in [x[0] for x in os.walk(root) if len(x[0].split("/")) == 3]]


experiments = discover_experiments()


def find_and_sort_results(results, protocol):
    def sort_func(res):
        return int(res[0].split(" ")[2].split("x")[0])

    results = [res for res in results if protocol in res[0]]
    results.sort(key=sort_func)
    return results


def prepare_experiments():
    results = [(ex.representative_name(), ex.calculate_box_plot_values()) for ex in experiments]
    return find_and_sort_results(results, "gpac"), find_and_sort_results(results, "raft"), find_and_sort_results(
        results, "2pc")


def values_from_experiment(ex):
    return {
        "protocol": ex[0].split(" - ")[0],
        "number_of_peers": ex[0].split(" - ")[1].split(" ")[0],
        **ex[1]
    }

def latex_from_experiment(ex):
    return """
        \\addplot+[
    boxplot prepared={
      median=%s,
      upper quartile=%s,
      lower quartile=%s,
      upper whisker=%s,
      lower whisker=%s
    },
    ] coordinates {};
    """%(ex["median"], ex["upper_quartile"], ex["lower_quartile"], ex["upper_whisker"], ex["lower_whisker"])


def print_list_to_latex(l):
    str = ""
    for x in l:
        str += f"{x},"
    str = str[0:-1]
    return str

def latex_template(all):
    return """
    \\begin{tikzpicture}
        \\begin{axis}
        [
        ytick={%s},
        yticklabels={%s},
        ]\n
        <----PLOTS HERE---->
        \\end{axis}
    \\end{tikzpicture}\n
    """%(print_list_to_latex(list(range(1, len(all)))), print_list_to_latex([x["name"] for x in all]))


def get_values():
    gpac, raft, twopc = prepare_experiments()
    all = list(itertools.chain(gpac, raft, twopc))
    return [values_from_experiment(a) for a in all]

def prepare_latex_code():
    pass

    # template = latex_template(all)
    #
    # latex = "\n".join([latex_from_experiment(ex) for ex in all])
    #
    # result = template.replace("<----PLOTS HERE---->", latex)
    # print(result)


def get_data_in_dat():
    values = get_values()
    df = pd.DataFrame.from_records(values)
    df.to_csv(f"parsed_csvs/{FILE_NAME}.dat", sep="\t", index=False)

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
    get_data_in_dat()
