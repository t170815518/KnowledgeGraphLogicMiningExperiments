from process_tsv import TuplesHandler
from result_parser import ResultParser
import os

path = ""
datasets = ['FB15k-237']
interested_attributes = ['Head Coverage', "Std Confidence", "PCA Confidence"]

for dataset in datasets:
    for attr in interested_attributes:
        print("==========")
        print("To process {} {}".format(datasets, attr))
        print("==========")

        handler = TuplesHandler(dataset, attr)
        handler.sample()

        for x in ["0.10", "0.20", "0.30", "0.40", "0.50", "0.60", "0.70", "0.80", "0.90", "1.00"]:
            print("Executing AMIE+ ()".format(x))
            _ = os.system("java -jar amie_plus.jar {}/{}_{}.tsv > ".format(dataset, dataset, x))
            print("Finish executing AMIE+")

        print("Parsing the result")
        parser = ResultParser(dataset, attr)
        result = parser.selected_rules

        for x in ["0.10", "0.20", "0.30", "0.40", "0.50", "0.60", "0.70", "0.80", "0.90", "1.00"]:
            buffer = parser.parse_amie_output(os.path.join(path, dataset, "{}_{}_{}.txt".format(dataset, x, attr)))
            result[x] = buffer[x]
            print("{} has been parsed.".format(x))

        result.to_csv(os.path.join(path, dataset, "{}_{}.csv".format(dataset, attr)))
        print("Finish parsing the result, and save the file")

        print("Complete process {} {}".format(datasets, attr))
        print("==========")
