'''
This module is for automating sampling, re-mining and evaluate tuples based on AMIE+.
'''
from process_tsv import TuplesHandler
from result_parser import ResultParser
import os

path = ""
datasets = ['wikidata_300k','FB15k-237']
interested_attributes = ['Head Coverage', "Std Confidence", "PCA Confidence"]

for dataset in datasets:
    for attr in interested_attributes:
        print("==========")
        print("To process {} {}".format(dataset, attr))
        print("==========")

        handler = TuplesHandler(dataset, attr)
        handler.sample()

        for x in ["0.10", "0.20", "0.30", "0.40", "0.50", "0.60", "0.70", "0.80", "0.90", "1.00"]:
            print("Executing AMIE+ ({})".format(x))
            _ = os.system("java -jar amie_plus.jar {0}/{0}_{1}.tsv > {0}/{0}_{1}.txt".format(dataset, x))
            print("Finish executing AMIE+")

        print("Parsing the result")
        parser = ResultParser(dataset, attr)
        result = parser.selected_rules

        for x in ["0.10", "0.20", "0.30", "0.40", "0.50", "0.60", "0.70", "0.80", "0.90", "1.00"]:
            buffer = parser.parse_amie_output(os.path.join(path, dataset, "{}_{}.txt".format(dataset, x)))
            result[x] = buffer[attr]
            print("{} has been parsed.".format(x))

        result.to_csv(os.path.join(path, dataset, "{}_{}.csv".format(dataset, attr)))
        print("Finish parsing the result, and save the file")

        print("Complete process {} {}".format(datasets, attr))
        print("==========")
