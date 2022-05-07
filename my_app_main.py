import multiprocessing
import time
from multiprocessing import Pool
import argparse
import sys
import matplotlib.pyplot as plt
import numpy as np

class MapReduce:

    def Start_Splitting(self, line):
        line = line.lower()
        return self.Mapping(line.replace("", "").replace("\n", "").replace(".", "").replace("!", "").replace("'", "").replace(",", "").replace(";", "").replace(":", "").replace("-", ""))

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
        #SECUENCIAL
        dict_total = dict()
        for dict_letters in list_dict_letters:
            for letter in dict_letters:
                if letter not in dict_total:
                    dict_total[letter] = [dict_letters[letter]]
                else:
                    dict_total[letter].append(dict_letters[letter])
        return dict_total

    def Reducing(self,  shuffled_dict):
        # SECUENCIAL
        for letter in shuffled_dict:
            shuffled_dict[letter]= sum(shuffled_dict[letter])
        return shuffled_dict

#----------------------------------------------------------------------
def WordCounter(input_file_lines):

    total_words = 0
    for line in input_file_lines:
        total_words += len(line.split())
    return total_words

def ReadFile(file_name):
    f = open(file_name, encoding="UTF-8")
    file_lines = f.readlines()  # Reads all the lines and return them as each line a string element in a list
    f.close()
    return file_lines

def GenerateResult(total_words,reduced_dict, file_name):
    result = []
    iterator = 0
    for value, key in reduced_dict.items():
        if iterator == 0:
            result.append(file_name)
        else:
            numero = (key / total_words) * 100
            string_numero = str(round(numero, 2)) + "%"
            result.append('%s : %s' % (value, string_numero))
        iterator += 1
    print("File name:", file_name)
    print("Num words:", total_words)
    return result


def GenerateHistogram(total_words,reduced_dict, histogram):
    histogramX = []
    histogramY = []
    histograms = list()
    iterator = 0
    for value, key in reduced_dict.items():
        if iterator != 0:
            histogramX.append(value)
            numero = (key / total_words) * 100
            histogramY.append(numero)
        iterator += 1

    histograms.append(sorted(histogramX))
    histograms.append(histogramY)
    if histogram == 'single':
        plt.bar(histogramX, histogramY, align='center')  # A bar chart
        plt.xlabel('Letters')
        plt.ylabel('Frequency(%)')
    return histograms

def GenerateFile(result_file, destination_file):

    with open(destination_file, 'w', encoding="UTF-8") as f:
        for file in result_file:
            for data in file:
                f.write('%s\n' % data)

def GenerateMultipleHistogram(histograms):

    counter_file = 0
    for i in range(0,len(histograms)-1):
        print(len(histograms))
        if counter_file == 0:
            plt.bar(histograms[counter_file][i], histograms[counter_file][i + 1], label='First file')
            counter_file += 1
        if counter_file == 1:
            plt.bar(histograms[counter_file][i], histograms[counter_file][i + 1], label='Second file')
            counter_file += 1
        else:
            plt.bar(histograms[counter_file][i], histograms[counter_file][i+1], label='Extra File')

        print(counter_file)
        plt.xlabel('Letters')
        plt.ylabel('Frequency(%)')
        plt.legend()

#----------------------------------------------------------------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    files = []
    final_result = []
    histogram_list = []

    source = "sourcefileN"
    for i in range(len(sys.argv)-1):
        if i == 0:
            parser.add_argument("histogram", help="single/all/no")
        else:
            final_source = source.replace("N",str(i))
            parser.add_argument(final_source, help="enter correct file name")

    args = parser.parse_args()

    if (len(sys.argv)-2 == 1):
        files.append(args.sourcefile1)
    if (len(sys.argv)-2 == 2):
        files.append(args.sourcefile1)
        files.append(args.sourcefile2)
    if (len(sys.argv)-2 == 3):
        files.append(args.sourcefile1)
        files.append(args.sourcefile2)
        files.append(args.sourcefile3)

    start_time = time.time()
    for file in files:

        input_file_lines = ReadFile(file)
        sum_words = WordCounter(input_file_lines)

        MapReduced = MapReduce()
        p = Pool(multiprocessing.cpu_count())
        mapped_list = p.map(MapReduced.Start_Splitting, input_file_lines)
        p.close()
        p.join()
        del input_file_lines

        shuffled_dict = MapReduced.Shuffling(mapped_list)
        del mapped_list  # Elimina la variable

        reduced_list = MapReduced.Reducing(shuffled_dict)

        final_result.append(GenerateResult(sum_words,reduced_list, file))
        histogram_list.append(GenerateHistogram(sum_words, reduced_list, args.histogram))
        del shuffled_dict

    GenerateFile(final_result, "Result.txt")
    if args.histogram == 'multiple':
        GenerateMultipleHistogram(histogram_list)
    plt.show()

    end_time = time.time()
    print("Execution time: ", (end_time - start_time))
