import pandas as pd
import os

path = ""


class TuplesHandler:
    def __init__(self, dataset_name, target_attribute, sample_size, sample_size_for_extremes=5):
        self.dataset_name = dataset_name
        self.tsv_file_path = os.path.join(path, dataset_name, "AMIE_plus_result.tsv")
        self.target_attribute = target_attribute
        self.sample_size = sample_size
        self.sample_size_for_extremes = sample_size_for_extremes

        self.sample = None

    def select_target_tuples(self):
        tsv_file = pd.read_csv(self.tsv_file_path, sep='\t')
        tsv_file.sort_values(self.target_attribute, ascending=False, inplace=True)
        original_data_size = tsv_file.shape[0]
        random_sample_size = round(original_data_size * self.sample_size - 2 * self.sample_size_for_extremes)

        largest_tuples = tsv_file.iloc[:self.sample_size_for_extremes]
        smallest_tuples = tsv_file.iloc[-self.sample_size_for_extremes:]
        tsv_file = tsv_file.iloc[self.sample_size_for_extremes:-self.sample_size_for_extremes]

        middle_valued_tuples = tsv_file.sample(n=random_sample_size, axis=0)

        self.sample = pd.concat([largest_tuples, smallest_tuples, middle_valued_tuples], axis=0)[['Rule']]
        return self.sample

    def to_csv(self):
        export_path = os.path.join(path, self.dataset_name, "{}_{:02f}.tsv".
                                   format(self.dataset_name, self.sample_size))
        self.sample.to_csv(export_path, sep='\t', header=False, index=False)


if __name__ == "__main__":
    for dataset in ['FB15k-237', 'wikidata_300k']:
        for sample_size in [0.1*x for x in range(1, 11)]:
            handler = TuplesHandler(dataset, "Head Coverage", sample_size=sample_size)
            handler.select_target_tuples()
            handler.to_csv()
