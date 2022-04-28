# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

class MapReduce:
    def Splitting(self):
        f = open("ArcTecSw_2022_BigData_Practica_Part1_Sample.txt", encoding="UTF-8")
        list_file_lines = f.readlines() # Reads all the lines and return them as each line a string element in a list
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


        print(list_of_words)
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
        print(list_dicts_words_letters)

        return list_dicts_words_letters

    def Reducing(self, list_words_letters_shuffled ):

        for element in list_words_letters_shuffled:
            for word_dict in element:
                for value in word_dict.values():
                    #print(value)
                    for letter in value:
                        value[letter] = len(value[letter])

        print(list_words_letters_shuffled)
        return list_words_letters_shuffled

    def printFinalResult(self, list_words_letters_reduced, ficheroInicial):

        sum_Words = len(list_words_letters_reduceded) #177
        print(sum_Words)
        letters_dictionary = dict()

        for element in list_words_letters_shuffled:
            for word_dict in element:
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

        print(letters_dictionary)


        with open("Result.txt", 'w', encoding="UTF-8") as f:
            f.write(ficheroInicial + '\n')
            for key, value in letters_dictionary.items():
                f.write('%s : %s\n' % (key, value))



PruebaMapReduce = MapReduce()
listReturn = PruebaMapReduce.Splitting()
list_words_letters = PruebaMapReduce.Mapping(listReturn)
print(list_words_letters)
list_words_letters_shuffled = PruebaMapReduce.Shuffling(list_words_letters)
list_words_letters_reduceded = PruebaMapReduce.Reducing(list_words_letters_shuffled)
print(PruebaMapReduce.printFinalResult(list_words_letters_reduceded, "ArcTecSw_2022_BigData_Practica_Part1_Sample.txt"))
#etters_dict = PruebaMapReduce.Shuffling(list_letters)
#letters_dict = PruebaMapReduce.Reducing(letters_dict)

#print(PruebaMapReduce.printFinalResult(letters_dict))