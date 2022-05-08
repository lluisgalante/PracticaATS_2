import multiprocessing
import time
from multiprocessing import Pool
import argparse
import sys
import matplotlib.pyplot as plt
import numpy as np


class MapReduce:

    def CleanAndMapFile(self, line):
        line = line.lower()
        return self.Mapping(line.replace("", "").replace("\n", "").replace(".", "").replace("!", "")
                            .replace("'", "").replace(",", "").replace(";", "").replace(":", "").replace("-", ""))

    def Mapping(self, line):
        word_dict = dict()
        for word in line.split():
            for letter in set(word):
                if letter not in word_dict:
                    word_dict[letter] = 1
                else:
                    word_dict[letter] += 1
        return word_dict

    def Shuffling(self, list_dict_letters):
        # SECUENCIAL
        dict_total = dict()
        for dict_letters in list_dict_letters:
            for letter in dict_letters:
                if letter not in dict_total:
                    dict_total[letter] = [dict_letters[letter]]
                else:
                    dict_total[letter].append(dict_letters[letter])
        return dict_total

    def Reducing(self, shuffled_dict):
        # SECUENCIAL
        for letter in shuffled_dict:
            shuffled_dict[letter] = sum(shuffled_dict[letter])
        return shuffled_dict


class DataManager:

    def __init__(self):
        self.final_result = list()

    def ReadFile(self, file_name):
        f = open(file_name, encoding="UTF-8")
        file_lines = f.readlines()  # Reads all the lines and return them as each line a string element in a list
        f.close()
        return file_lines

    def WordCounter(self, input_file_lines):
        total_words = 0
        for line in input_file_lines:
            total_words += len(line.split())
        return total_words

    def GenerateResult(self, total_words, reduced_dict, file_name):
        result = list()
        iterator = 0
        for value, key in reduced_dict.items():
            if iterator == 0:
                result.append(file_name)
            else:
                number = (key / total_words) * 100
                percentage_number = str(round(number, 2)) + "%"
                result.append('%s : %s' % (value, percentage_number))
            iterator += 1
        print("File name:", file_name)
        print("Num words:", total_words)

        self.final_result.append(result)

    def PrintAndWriteFileResult(self, destination_file):
        with open(destination_file, 'w', encoding="UTF-8") as f:
            for file_result in self.final_result:
                for data_result in file_result:
                    print(data_result)
                    f.write('%s\n' % data_result)


class HistogramGenerator:
    def __init__(self):
        self.histogram_list = list()

    def GenerateHistogramData(self, total_words, reduced_dict):
        histogram_x = list()
        histogram_y = list()
        histogram_xy = list()
        iterator = 0
        for value, key in reduced_dict.items():
            if iterator != 0:
                histogram_x.append(value)
                number = (key / total_words) * 100
                histogram_y.append(number)
            iterator += 1
        histogram_xy.append(histogram_x)
        histogram_xy.append(histogram_y)

        self.histogram_list.append(histogram)

    def GenerateHistogram(self):
        """ Generate a Histogram based on 'n' files """
        legends = 'File n'
        for i in range(0, len(self.histogram_list)):
            plt.bar(self.histogram_list[i][0], self.histogram_list[i][1],
                    label=legends.replace("n", str(i + 1)))  # Diff colour of each bar will be assigned automatically

        plt.xlabel('Letters')
        plt.ylabel('Frequency(%)')
        plt.legend()


if __name__ == '__main__':

    data = DataManager()
    histogram = HistogramGenerator()
    mapReduced = MapReduce()

    files_name = list()

    source = "filenameN"
    parser = argparse.ArgumentParser()
    for i in range(len(sys.argv) - 1):
        if i == 0:
            parser.add_argument("histogram", help="yes/no")
        else:
            final_source = source.replace("N", str(i))
            parser.add_argument(final_source, help="enter correct file name")

    args = parser.parse_args()

    if len(sys.argv) - 2 == 1:
        files_name.append(args.filename1)
    if len(sys.argv) - 2 == 2:
        files_name.append(args.filename1)
        files_name.append(args.filename2)
    if len(sys.argv) - 2 == 3:
        files_name.append(args.filename1)
        files_name.append(args.filename2)
        files_name.append(args.filename3)

    start_time = time.time()
    for file in files_name:
        input_file_lines = data.ReadFile(file)
        sum_words = data.WordCounter(input_file_lines)

        p = Pool(multiprocessing.cpu_count())
        mapped_list = p.map(mapReduced.CleanAndMapFile, input_file_lines)
        p.close()
        p.join()
        del input_file_lines

        shuffled_dict = mapReduced.Shuffling(mapped_list)
        del mapped_list  # Elimina la variable

        reduced_list = mapReduced.Reducing(shuffled_dict)

        data.GenerateResult(sum_words, reduced_list, file)
        histogram.GenerateHistogramData(sum_words, reduced_list)
        del shuffled_dict

    data.PrintAndWriteFileResult("Result.txt")
    if args.histogram == 'yes':
        histogram.GenerateHistogram()

    plt.show()

    end_time = time.time()
    print("Execution time: ", (end_time - start_time))
