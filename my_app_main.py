import multiprocessing
from time import sleep, perf_counter, time
import time
from multiprocessing import Pool
import os
import argparse
import sys
import matplotlib.pyplot as plt


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

    t_w = 0
    for line in input_file_lines:
        t_w += len(line.split())
    return t_w
def ReadAndRedimensionFile(file_name, redimension):
    f = open(file_name, encoding="UTF-8")
    file_lines = f.readlines()  # Reads all the lines and return them as each line a string element in a list
    f.close()

    new_file = []
    for i in range(redimension):
        for line in file_lines:
            new_file.append(line)

    return new_file

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
            print(file_name)
        else:
            numero = (key / total_words) * 100
            string_numero = str(round(numero, 2)) + "%"
            result.append('%s : %s' % (value, string_numero))
            print('%s : %s' % (value, string_numero))
        iterator+=1

    return result


def GenerateHistogram(histogram):
    plt.hist(histogram, bins=80, color="red", rwidth=1)
    plt.title("Histograma")
    plt.xlabel("Letras")
    plt.ylabel("Frecuencia")

def GenerateFile(result_file, destination_file):
    with open(destination_file, 'w', encoding="UTF-8") as f:
        for file in result_file:

            iterator = 0
            for values in file:
                if iterator == 0:
                    f.write('%s\n' % (values[0]))
                    print('%s:' % (values[0]))
                else:
                    for key, value in values.items():
                        f.write('%s : %s\n' % (key, value))
                        print('%s : %s' % (key, value))
                iterator = iterator+1



#-----------------------------------------------------------------------------

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    files = []
    final_result = []
    source = "sourcefileN"
    for i in range(len(sys.argv)-1):
        final_source = source.replace("N",str(i+1))
        parser.add_argument(final_source, help="enter correct file name")

    args = parser.parse_args()

    #He intentado mejorarlo pero no se como
    if (len(sys.argv)-1 == 1):
        files.append(args.sourcefile1)
    if (len(sys.argv)-1 == 2):
        files.append(args.sourcefile1)
        files.append(args.sourcefile2)
    if (len(sys.argv) - 1 == 3):
        files.append(args.sourcefile1)
        files.append(args.sourcefile2)
        files.append(args.sourcefile3)

    start_time = time.time()
    for file in files:

        input_file_lines = ReadFile(file)
        sum_words = WordCounter(input_file_lines)
        MapReduced = MapReduce()
        p = Pool(multiprocessing.cpu_count())

        start_time_ALL = time.time()
        maped_list = p.map(MapReduced.Start_Splitting, input_file_lines)
        p.close()
        p.join()
        end_time = time.time()
        print("Execution time Mapping: ", (end_time - start_time))
        del input_file_lines

        start_time = time.time()
        shuffled_dict = MapReduced.Shuffling(maped_list)
        end_time = time.time()
        print("Execution time Shuffling: ", (end_time - start_time))
        del maped_list  # Elimina la variable

        start_time = time.time()
        reduced_list = MapReduced.Reducing(shuffled_dict)

        final_result.append(GenerateResult(sum_words,reduced_list, file))
        del shuffled_dict


    end_time = time.time()
    GenerateFile(final_result, "Result.txt")
    print("Execution time: ", (end_time - start_time))
    plt.show()

    end_time = time.time()
    print("Execution time Reducing: ", (end_time - start_time))

    end_time_ALL = time.time()
    print("Execution time: ", (end_time_ALL - start_time_ALL))
