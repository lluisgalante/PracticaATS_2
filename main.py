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
        dict_words_letters = dict()
        print(list_of_lines)
        for string in list_of_lines:
            list_of_words.append(string.split())
        print( list_of_words)
        for words in  list_of_words:
            for  word in words:
                dict_words_letters[word] = []
                for letter in word:
                    dict_words_letters[word].append([letter, 1])

        return dict_words_letters

    def Shuffling(self, dict_words_letters):


        for key in dict_words_letters:
            for letter in dict_words_letters[key]:
                print(letter)

        return FALSE
        #letters_dict = dict()
        #for element in list_of_letters:
            #for letter in element:
                #if (isinstance(letter, str)):
                    #if letter in letters_dict.keys():
                         #letters_dict[letter].append(1)


                #else:

                        #letters_dict.update({letter: [1]})

        #return letters_dict




    def Reducing(self,letters_dict):

        for key in letters_dict.keys():
            #print(key)
            letters_dict[key] = len(letters_dict[key])


        return letters_dict

    def printFinalResult(self, letters_dict):


        sum_Letters_Text = 0
        for key in letters_dict.keys():

            sum_Letters_Text += letters_dict[key]

        dict_porcentages= dict()
        for key in letters_dict.keys():

            dict_porcentages[key] = (letters_dict[key]/sum_Letters_Text) * 100

        return dict_porcentages






PruebaMapReduce = MapReduce()
listReturn = PruebaMapReduce.Splitting()
list_words_letters = PruebaMapReduce.Mapping(listReturn)
print(list_words_letters)
PruebaMapReduce.Shuffling(list_words_letters)
#etters_dict = PruebaMapReduce.Shuffling(list_letters)
#letters_dict = PruebaMapReduce.Reducing(letters_dict)

#print(PruebaMapReduce.printFinalResult(letters_dict))