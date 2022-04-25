# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

class MapReduce:
    def Splitting(self):
        f = open("ArcTecSw_2022_BigData_Practica_Part1_Sample.txt", encoding="UTF-8")
        list_file_lines = f.readlines() # Reads all the lines and return them as each line a string element in a list
        list_file_lines_clean=[]
        for line in list_file_lines:
            txt = line.replace(" ", "")
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

        list_of_letters = []
        for line in list_of_lines:
            for x in line:

                list_of_letters.append([x, 1])

        return list_of_letters

    def Shuffling(self, list_of_letters):

        letters_dict = dict()
        for element in list_of_letters:
            for letter in element:
                if (isinstance(letter, str)):
                    if letter in letters_dict.keys():
                         letters_dict[letter].append(1)

                    else:
                        # letters_dict[letter] = [1]
                        letters_dict.update({letter: [1]})

        return letters_dict

    def Reducing(self):
        return False

    def printFinalResult(self):
        return False






PruebaMapReduce = MapReduce()
listReturn = PruebaMapReduce.Splitting()
list_letters = PruebaMapReduce.Mapping(listReturn)
#print(list_letters)
print(PruebaMapReduce.Shuffling(list_letters))