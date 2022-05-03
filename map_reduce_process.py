import multiprocessing
from time import sleep, perf_counter, time
import time
from multiprocessing import Process, Manager
import os
import argparse
import sys



class MapReduce:


    def Start(self, line, reduced_list):
        self.Splitting(line, reduced_list)

    def Splitting(self, line, reduced_list):
        splitted_list = []
        for lines in line:
            lines = lines.lower()
            txt = lines.replace("", "")
            txt1 = txt.replace("\n", "")
            txt2 = txt1.replace(".", "")
            txt3 = txt2.replace("!", "")
            txt4 = txt3.replace("?", "")
            txt5 = txt4.replace("'", "")
            txt6 = txt5.replace(",", "")
            txt7 = txt6.replace(";", "")
            txt8 = txt7.replace(":", "")
            splitted_line = txt8.replace("-", "")
            splitted_list.append(splitted_line)
        self.Mapping(splitted_list, reduced_list)

    def Mapping(self, line, reduced_list):
        list_of_words = []
        mapping_return =[]

        for lines in line:
            for word in lines.split():
                list_of_words.append([word])

        for word in list_of_words:
                for element in word:
                    word_dict = dict()
                    word_dict[element] = []
                    for letter in element:
                        word_dict[element].append([letter,1])
                    mapping_return.append([word_dict])

        self.Shuffling(mapping_return, reduced_list)

    def Shuffling(self, lists_dict_words_letters, reduced_list):

        list_shuffling_list=[]
        for list_dict_words_letters in lists_dict_words_letters:
            for word_dict in list_dict_words_letters:
                list_key = list(word_dict.keys())
                key = list_key[0]
                for value in word_dict.values():
                    non_repeated_dict = dict()
                    for letter_int in value:
                        for letter in letter_int:
                            if isinstance(letter, str):
                                if letter in non_repeated_dict:
                                    non_repeated_dict[letter].append(1)
                                else:
                                    non_repeated_dict[letter] = [1]

                    word_dict[key] = non_repeated_dict
                list_shuffling_list.append(word_dict)

        self.Reducing(list_shuffling_list, reduced_list)




    def Reducing(self, list_word_letters_shuffled, reduced_list):

        for word_dict in list_word_letters_shuffled:
            for value in word_dict.values():
                for letter in value:
                    value[letter] = len(value[letter])
        reduced_list.append(list_word_letters_shuffled)

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

def ReadAndDivideFile(file_name):
    f = open(file_name, encoding="UTF-8")
    file_lines = f.readlines()  # Reads all the lines and return them as each line a string element in a list
    f.close()

    file = []
    file1 = []
    file2 = []
    file3 = []
    file4 = []

    for i in range(int(len(file_lines)/4)):
        file1.append(file_lines[i])
    for i in range(int(len(file_lines) / 4)):
        file2.append(file_lines[int(i-(len(file_lines)/4)-1)])
    for i in range(int(len(file_lines) / 4)):
        file3.append(file_lines[int(i-(2*len(file_lines)/4)-1)])
    for i in range(int(len(file_lines) / 4)):
        file4.append(file_lines[int(i-(3*len(file_lines)/4)-1)])

    file.append(file1)
    file.append(file2)
    file.append(file3)
    file.append(file4)

    return file

def GenerateResult(list_words_letters_reduced, source_file):
    sum_Words = 0
    for list in list_words_letters_reduced:
        sum_Words += len(list)

    print("Number of words: ",sum_Words)
    letters_dictionary = dict()

    for list_dict in list_words_letters_reduced:
        for word_dict in list_dict:
            for value in word_dict.values():
                for letter in value:
                    if letter in letters_dictionary:
                        letters_dictionary[letter] = letters_dictionary[letter] + 1
                    else:
                        letters_dictionary[letter] = 1

    for letter in letters_dictionary:
        num = (letters_dictionary[letter] / sum_Words) * 100
        num_percentage = str(round(num, 2)) + "%"
        letters_dictionary[letter] = num_percentage

    list = []
    list.append([source_file])
    list.append(letters_dictionary)

    return list

def GenerateFile(result_file, destination_file):

    with open(destination_file, 'w', encoding="UTF-8") as f:
        for file in result_file:
            iterator = 0
            for values in file:
                if iterator == 0:
                    f.write('%s\n' % (values[0]))
                else:
                    for key, value in values.items():
                        f.write('%s : %s\n' % (key, value))
                iterator = iterator+1

#-----------------------------------------------------------------------------

if __name__ == '__main__':

    """f = open("ArcTecSw_2022_BigData_Practica_Part1_Sample.txt", encoding="UTF-8")
    file_lines = f.readlines()  # Reads all the lines and return them as each line a string element in a list
    f.close()

    new_file = []
    for i in range(150000):
        for line in file_lines:
            new_file.append(line)

    with open("Sample_150k.txt", 'w', encoding="UTF-8") as f:
        for value in new_file:
            f.write(value)"""

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
        input_file = ReadAndDivideFile(file)
        MapReduced = MapReduce()
        with Manager() as manager:
            reduced_list = manager.list()
            for i in range(len(input_file)):
                p = Process(target=MapReduced.Start,args=(input_file[i], reduced_list,))
                p.start()
                p.join()
            final_result.append(GenerateResult(reduced_list, file))

    GenerateFile(final_result, "Result.txt")
    end_time = time.time()
    print("Execution time: ", (end_time - start_time))
