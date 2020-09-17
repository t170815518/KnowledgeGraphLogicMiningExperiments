import pandas as pd
import os
from math import ceil

path = ""


class TuplesHandler:
    '''
        Object takes samples based on statistics from AMIE output in tsv format, and generates new samples in different
        size.
    '''
    def __init__(self, dataset_name, target_attribute, sample_size_for_extremes=5):
        '''
        :param dataset_name: string, used to identify the directory of the file 
        :param target_attribute: string, "Head Convergence"
        :param sample_size_for_extremes: int, the number of selected maximum, minimum and middle rules (e.g. 3x)
        '''
        self.dataset_name = dataset_name
        self.target_attribute = target_attribute
        self.sample_size_for_extremes = sample_size_for_extremes
        
        self.AMIE_file = pd.read_csv(os.path.join(path, dataset_name, "AMIE_plus_result.tsv"), sep='\t')
        self.tuples = pd.read_csv(os.path.join(path, dataset_name, "{}_train.tsv").format(dataset_name), sep='\t',
                                       names=['subject', 'relation', 'object'])
        self.tuples_num = self.tuples.shape[0]
        self.tuples_groups = self.tuples.groupby('relation')

        self.sample_size_interval = [0.1*x for x in range(1, 11)]
        self.selected_mined_rules = None

    def select_mined_rules(self):
        '''
        Select some mined rules from AMIE with maximum, minimum and middle-range target value.
        A tsv file will be generated in the dataset file.
        :return pd.DataFrame, the selected mined rules
        '''
        self.AMIE_file.sort_values(self.target_attribute, ascending=False, inplace=True)

        largest_tuples = self.AMIE_file.iloc[:self.sample_size_for_extremes]
        smallest_tuples = self.AMIE_file.iloc[-self.sample_size_for_extremes:]

        remaining_tuples = self.AMIE_file.iloc[self.sample_size_for_extremes:-self.sample_size_for_extremes]
        middle_tuples = remaining_tuples.sample(n=self.sample_size_for_extremes, axis=0)

        self.selected_mined_rules = pd.concat([largest_tuples, smallest_tuples, middle_tuples], axis=0)
        self.AMIE_file.drop(self.selected_mined_rules.index)
        self.selected_mined_rules.to_csv(os.path.join(path, self.dataset_name, "selected_mined_rules.tsv"), sep='\t')

        return self.selected_mined_rules

    def parse_target_rules(df):
        '''Create a new column in df that stores the relations appearing in the rule'''
        def parser(rule_string):
            relations = []

            buckets, inferenced = rule_string.split("=>")
            relations.append(inferenced.split()[1])
            buckets = buckets.split()
            if len(buckets) == 3:
                relations.append(buckets[1])
            elif len(buckets) == 6:
                relations.extend([buckets[1], buckets[4]])
            else:
                print("%s cannot be parsed.".format(rule_string))

            return relations

        df['Relations'] = df['Rule'].apply(parser)

    def generate_sample_tuples(self, frac, export=True):
        samples = []
        values = self.selected_mined_rules['Relations'].tolist()
        target_relations = list(set([y for x in values for y in x]))  # get all unique relations to a list

        # sample target relations in sampled rules
        for rel in target_relations:
            sample = self.tuples_groups.get_group(rel).sample(frac=frac, axis=0)
            samples.append(sample)
        print("==========Sample remaining tuples==========")

        # remove the target relations from the sample
        remaining_tuples = self.tuples.drop(self.tuples[self.tuples["relation"].apply(lambda x: x in target_relations)].index)
        print("Total Sample Size =", self.tuples_num)
        print("Remaining Sample Size =", remaining_tuples.shape)

        samples.append(remaining_tuples.sample(frac=frac, axis=0))
        samples = pd.concat(samples, axis=0)
        print("{}({}) samples are selected in total.".format(samples.shape[0], samples.shape[0]/self.tuples_num))
        print("=========Sample Completes==========")

        if export:
            samples.to_csv(os.path.join(path, self.dataset_name, "{}_{:.2f}.tsv".format(self.dataset_name, frac)),
                           sep='\t', header=False, index=False)

    def sample(self):
        self.select_mined_rules()
        TuplesHandler.parse_target_rules(self.selected_mined_rules)
        for sample_size in self.sample_size_interval:
            self.generate_sample_tuples(sample_size)


if __name__ == "__main__":
    for dataset in ['wikidata_300k',]:
        print("==========Sampling {}==========".format(dataset))
        handler = TuplesHandler(dataset, "Head Coverage")
        handler.sample()
        print("==========Finish sampling {}==========".format(dataset))

