from process_tsv import TuplesHandler
import pandas as pd
import os

path = ""

class ResultParser:
    '''parse the result from amie output'''
    def __init__(self, dataset, target_attribute):
        self.dataset = dataset
        self.target_attribute = target_attribute
        self.selected_rules = pd.read_csv(os.path.join(path, dataset, "selected_mined_rules.tsv"), sep='\t', index_col=0)
        self.target_attribute_indexes = {"Head Coverage": -10, "Std Confidence": -9, "PCA Confidence": -8}

        TuplesHandler.parse_target_rules(self.selected_rules)
        self.selected_rules[target_attribute] = 0

    def parse_amie_output(self, file_path):
        ''' find the target relations and return their score '''
        def is_contain_relations(l):
            for rel in l:
                if rel not in line:
                    return False
            return True

        with open(file_path, 'r') as f:
            for line in f:
                if line[0] == '?':  # data line starts with ? e.g. ?a
                    split_lines = line.split()
                    for index, row in self.selected_rules.iterrows():
                        # if is_contain_relations(row['Relations']):
                        #     value = float(split_lines[self.target_attribute_indexes[self.target_attribute]])
                        #     self.selected_rules.loc[index, self.target_attribute] = value
                        #     break
                        if row['Rule'] in line:
                            value = float(split_lines[self.target_attribute_indexes[self.target_attribute]])
                            self.selected_rules.loc[index, self.target_attribute] = value
                            break

        return self.selected_rules


if __name__ == '__main__':
    dataset = "wikidata_300k"
    target_attribute = "Head Coverage"

    parser = ResultParser(dataset, target_attribute)
    result = parser.selected_rules
    for size in ["0.10", '0.20', "0.30", "0.40", "0.50", "0.60", "0.70", "0.80", "0.90", "1.00"]:
        buffer = parser.parse_amie_output(os.path.join(path, dataset, "{}_{}_{}.txt".format(dataset, size,
                                                                                            target_attribute)))
        result[size] = buffer[target_attribute]
        print("{} has been parsed.".format(size))
    result.to_csv(os.path.join(path, dataset, "result.csv"))