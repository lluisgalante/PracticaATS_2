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
        """list_word_dict = []
        for word in line.split():
            word = set(word)
            word_dict = dict()
            for letter in word:
                word_dict[letter] = 1
            list_word_dict.append(word_dict)

        new_letters_dict = dict()
        for dict_of_letters in list_word_dict:
            for letter in dict_of_letters:
                if letter not in new_letters_dict.keys():
                    new_letters_dict[letter] = 1
                else:
                    new_letters_dict[letter] = new_letters_dict[letter] + 1"""
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

def GenerateResult(reduced_dict, source_file):
    print(reduced_dict)
    sum_words=0
    porcentage_dict=dict()
    for letter in reduced_dict:
        sum_words+=reduced_dict[letter]
    print(sum_words)
    for letter in reduced_dict:
        numero = (reduced_dict[letter] / (162*500000)) * 100
        string_numero = str(round(numero, 2)) + "%"
        # string_numero_percentage = string_numero + "%"
        reduced_dict[letter] = string_numero

    with open(source_file, 'w', encoding="UTF-8") as f:
        f.write(source_file + '\n')
        for key, value in reduced_dict.items():
            f.write('%s : %s\n' % (key, value))


    """list = []
    list.append([source_file])
    list.append(letters_dictionary)

    GenerateHistogram(histogram)"""
    """return list"""

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

    files = []
    final_result = []
    source = "sourcefileN"
    lines_500k = []
    input_file_lines = open("Sample_1GB.txt", "r", encoding="utf-8").readlines()
    """for x in range(500000):
        for line in input_file:
            lines_500k.append(line)"""

    MapReduced = MapReduce()
    p = Pool(multiprocessing.cpu_count())

    start_time_ALL =  time.time()
    start_time = time.time()
    maped_list = p.map(MapReduced.Start_Splitting, input_file_lines)
    p.close()
    p.join()
    end_time = time.time()
    print("Execution time Mapping: ", (end_time - start_time))
    """del lines_500k"""
    del input_file_lines


    start_time = time.time()
    shuffled_dict = MapReduced.Shuffling(maped_list)
    end_time = time.time()
    print("Execution time Shuffling: ", (end_time - start_time))
    del maped_list #Elimina la variable

    start_time = time.time()
    reduced_list = MapReduced.Reducing(shuffled_dict)
    del shuffled_dict
    #GenerateResult(reduced_list,"Result.txt")
    end_time = time.time()
    print("Execution time Reducing: ", (end_time - start_time))

    end_time_ALL = time.time()
    print("Execution time: ", (end_time_ALL - start_time_ALL))

