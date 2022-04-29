from time import sleep, perf_counter
from threading import Thread

splitted_list = []
mapping_list = []
shuffling_list = []
reducing_list = []
class MapReduce:

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
        txt9 = txt8.replace("-", "")

        splitted_list.append(txt9)


    def Mapping(self, line ):

        list_of_words = []

        for word in line.split():
            list_of_words.append([word])

        for word in list_of_words:
                for element in word:
                    word_dict = dict()
                    word_dict[element] = []
                    for letter in element:
                        word_dict[element].append([letter,1])
                    mapping_list.append([word_dict])

    def Shuffling(self, list_dict_words_letters):


            for word_dict in list_dict_words_letters:
                #values = list(word_dict.values())
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
                    shuffling_list.append(word_dict)


    def Reducing(self, list_word_letters_shuffled ):

        for word_dict in list_word_letters_shuffled:
            for value in word_dict.values():
                #print(value)
                for letter in value:
                    value[letter] = len(value[letter])



    def printFinalResult(self, list_words_letters_reduced, ficheroFinal):

        sum_Words = len(list_words_letters_reduced) #177

        letters_dictionary = dict()

        for word_dict in list_words_letters_reduced:
            for value in word_dict.values():
                # print(value)
                for letter in value:
                    if letter in letters_dictionary:
                        #print(letters_dictionary[letter])
                        letters_dictionary[letter] = letters_dictionary[letter] + 1
                    else:
                        letters_dictionary[letter]=1

        for letter in letters_dictionary:
            numero = (letters_dictionary[letter] / sum_Words) * 100
            string_numero =str(round(numero, 2)) + "%"
            #string_numero_percentage = string_numero + "%"
            letters_dictionary[letter] = string_numero

        with open(ficheroFinal, 'w', encoding="UTF-8") as f:
            f.write(ficheroFinal + '\n')
            for key, value in letters_dictionary.items():
                f.write('%s : %s\n' % (key, value))

#----------------------------------------------------------------------

fichero = "ArcTecSw_2022_BigData_Practica_Part1_Sample.txt"


f = open(fichero, encoding="UTF-8")

list_file_lines = f.readlines()  # Reads all the lines and return them as each line a string element in a list



list_file_lines_1000 =[]
for i in range(1000000):
    for line in list_file_lines:
        list_file_lines_1000.append(line)

ficheroFinal= "Sample_1000.txt"
with open(ficheroFinal, 'w', encoding="UTF-8") as f:
    for string in list_file_lines_1000:
        f.write(string)


PruebaMapReduce = MapReduce()

threads = []

for line in list_file_lines:
    string_txt = [line]
    t = Thread(target = PruebaMapReduce.Splitting, args = string_txt)
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(splitted_list)


for element in splitted_list:
    string_txt = [element]
    t = Thread(target = PruebaMapReduce.Mapping, args = string_txt)
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(mapping_list)

for element in mapping_list:
    #string_txt = [element]
    t = Thread(target = PruebaMapReduce.Shuffling, args = [element])
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(shuffling_list)

reducing_list = shuffling_list

for element in reducing_list:
    #string_txt = [element]
    t = Thread(target = PruebaMapReduce.Reducing, args = [[element]]) #POSIBLE MEJORA
    threads.append(t)
    t.start()

for t in threads:
    t.join()


print(reducing_list)


PruebaMapReduce.printFinalResult(reducing_list,"Result.txt")

