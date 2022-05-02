from time import sleep, perf_counter, time
import time
import argparse

class MapReduce:
    def Splitting(self, list_file_lines):
        list_file_lines_clean=[]
        for line in list_file_lines:
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
            txt9 = txt8.replace("-", "")
            list_file_lines_clean.append(txt9)

        return list_file_lines_clean



    def Mapping(self, list_of_lines):

        list_of_words = []
        list_of_words_letters =[]

        for string in list_of_lines:
            for word in string.split():
                list_of_words.append([word])

        for word in list_of_words:
                for element in word:
                    word_dict = dict()
                    word_dict[element] = []
                    for letter in element:
                        word_dict[element].append([letter,1])
                    list_of_words_letters.append([word_dict])


        return list_of_words_letters

    def Shuffling(self, list_dicts_words_letters):

        for element in list_dicts_words_letters:
            for word_dict in element:
                values = list(word_dict.values())
                list_key = list(word_dict.keys())
                key = list_key[0]

                for value in word_dict.values():
                    non_repeated_dict=dict()
                    for letter_int in value:
                        for letter in letter_int:
                            if isinstance(letter,str):
                                if letter in non_repeated_dict:
                                    non_repeated_dict[letter].append(1)
                                else:
                                    non_repeated_dict[letter] = [1]

                    word_dict[key] = non_repeated_dict

                    #print(non_repeated_dict)

        return list_dicts_words_letters

    def Reducing(self, list_words_letters_shuffled ):

        for element in list_words_letters_shuffled:
            for word_dict in element:
                for value in word_dict.values():
                    #print(value)
                    for letter in value:
                        value[letter] = len(value[letter])

        return list_words_letters_shuffled

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

    with open(destination_file, 'w', encoding="UTF-8") as f:
        f.write(destination_file + '\n')
        for key, value in letters_dictionary.items():
            f.write('%s : %s\n' % (key, value))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("sourcefile", help="enter correct file name")
    parser.add_argument("iterations", help="enter 1 or x if you want to multiply your source file", type=int)
    parser.add_argument("resultfile", help="enter results file")
    args = parser.parse_args()
    input_file = ReadAndRedimensionFile(args.sourcefile, args.iterations)
    start_time = time.time()

    MapReduced = MapReduce()
    listReturn = MapReduced.Splitting(input_file)
    list_words_letters = MapReduced.Mapping(listReturn)
    list_words_letters_shuffled = MapReduced.Shuffling(list_words_letters)
    list_words_letters_reduceded = MapReduced.Reducing(list_words_letters_shuffled)

    end_time = time.time()
    GenerateNewFileResult(list_words_letters_reduceded, args.resultfile)

    print("Execution time: ",(end_time - start_time))