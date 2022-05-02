import multiprocessing
from time import sleep, perf_counter, time
import time
from multiprocessing import Pool
import os


class MapReduce:

    def Start(self, string):
        return self.Splitting(string)

    def Splitting(self, line):
        line = line.lower()
        txt = line.replace("", "")
        txt1 = txt.replace("\n", "")
        txt2 = txt1.replace(".", "")
        txt3 = txt2.replace("!", "")
        txt4 = txt3.replace("?", "")
        txt5 = txt4.replace("'", "")
        txt6 = txt5.replace(",", "")
        txt7 = txt6.replace(";", "")
        txt8 = txt7.replace(":", "")
        splitted_line = txt8.replace("-", "")
        return self.Mapping(splitted_line)

    def Mapping(self, line):
        list_of_words = []
        mapping_return =[]

        for word in line.split():
            list_of_words.append([word])

        for word in list_of_words:
                for element in word:
                    word_dict = dict()
                    word_dict[element] = []
                    for letter in element:
                        word_dict[element].append([letter,1])
                    mapping_return.append([word_dict])

        return self.Shuffling(mapping_return)

    def Shuffling(self, lists_dict_words_letters):

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

        return self.Reducing(list_shuffling_list)




    def Reducing(self, list_word_letters_shuffled ):

        for word_dict in list_word_letters_shuffled:
            for value in word_dict.values():
                for letter in value:
                    value[letter] = len(value[letter])
        return list_word_letters_shuffled

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


def GenerateNewFileResult(list_words_letters_reduced, destination_file):
    sum_Words = 0
    for list in list_words_letters_reduced:
        sum_Words += len(list)

    print(sum_Words)
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

    with open(destination_file, 'w', encoding="UTF-8") as f:
        f.write(destination_file + '\n')
        for key, value in letters_dictionary.items():
            f.write('%s : %s\n' % (key, value))

#-----------------------------------------------------------------------------

if __name__ == '__main__':

    input_file = ReadAndRedimensionFile("ArcTecSw_2022_BigData_Practica_Part1_Sample.txt", 100)
    start_time = time.time()

    MapReduced = MapReduce()
    p = Pool(multiprocessing.cpu_count())
    reduced_list = p.map(MapReduced.Start, input_file)
    p.close()
    p.join()

    end_time = time.time()
    GenerateNewFileResult(reduced_list, "Result_threads.txt")
    print("Execution time: ",(end_time - start_time))

