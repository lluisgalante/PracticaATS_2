import multiprocessing
import time
from multiprocessing import Pool
import matplotlib.pyplot as plt
import sys


class MapReduce:

    def CleanAndMapFile(self, lines):
        """ Replace all 'no words' values from a file that we do not want to map reduce.
         Once our file lines are cleaned those files will be redirected to mapping function """
        list_file_lines_clean = []
        for line in lines:
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

        return self.Mapping(list_file_lines_clean)

    def Mapping(self, lines):
        """ Receive file lines and create a dictionary that records the number of times a same letter
        is repeated on different words of the file"""
        word_dict = dict()
        for line in lines:
            for word in line.split():
                for letter in set(word):
                    if letter not in word_dict:
                        word_dict[letter] = 1
                    else:
                        word_dict[letter] += 1  # If already exists on dictionary we use existent key
            return word_dict

    def Shuffling(self, list_dict_letters):
        """ Once the list is mapped this function joins all letters with same 'key' and his values in one key """
        dict_shuffled = dict()
        for letter in list_dict_letters:
            if letter not in dict_shuffled:
                dict_shuffled[letter] = [list_dict_letters[letter]]
            else:
                dict_shuffled[letter].append(list_dict_letters[letter])
        return dict_shuffled

    def Reducing(self, shuffled_dict):
        """ Sum all values from the same key. This dict allows to know all the times a letter on file words """
        for letter in shuffled_dict:
            shuffled_dict[letter] = sum(shuffled_dict[letter])
        return shuffled_dict


class DataManager:

    def __init__(self):
        self.final_result = list()  # List were the final result will be saved

    def ReadFile(self, file_name):
        """ From a given file reads all the lines and return them as each line a string element in a list """
        f = open(file_name, encoding="UTF-8")
        file_lines = f.readlines()
        f.close()
        return file_lines

    def WordCounter(self, file_lines):
        """ Counter of the words inside a single file. This will allow us
         to eventually generate the percentage of a value """
        total_words = 0
        for line in file_lines:
            total_words += len(line.split())
        return total_words

    def GenerateResult(self, total_words, reduced_dict, file_name):
        """ Generate result of a given map reduced dict. Key represents the letter and value
        the times that this letter appears on words of the file"""
        result = list()
        iterator = 0
        for value, key in reduced_dict.items():
            if iterator == 0:
                result.append(file_name)  # First we save the name of the file
            else:
                number = (key / total_words) * 100
                percentage_number = str(round(number, 2)) + "%"  # Convert number of times to percentage
                result.append('%s : %s' % (value, percentage_number))
            iterator += 1
        print("Num words:", total_words)
        self.final_result.append(result)

    def PrintAndWriteFileResult(self, destination_file):
        """ Print data result on screen and write this data in a result file """
        with open(destination_file, 'w', encoding="UTF-8") as f:
            for file_result in self.final_result:
                for data_result in file_result:
                    print(data_result)
                    f.write('%s\n' % data_result)


class HistogramGenerator:
    def __init__(self):
        self.histograms_list = list()  # List to store all histograms data

    def GenerateHistogramData(self, total_words, reduced_dict):
        """ Generate histogram data from each file.
        This data of each histogram will be appended to class histograms_list"""
        histogram_x = list()
        histogram_y = list()
        histogram_xy = list()
        for value, key in reduced_dict.items():
            histogram_x.append(value)
            number = (key / total_words) * 100
            histogram_y.append(number)
        histogram_xy.append(histogram_x)
        histogram_xy.append(histogram_y)
        self.histograms_list.append(histogram_xy)

    def GenerateHistogram(self):
        """ Generate a Histogram based on 'n' files """
        legends = 'File n'
        for i in range(0, len(self.histograms_list)):
            plt.bar(self.histograms_list[i][0], self.histograms_list[i][1],
                    label=legends.replace("n", str(i + 1)))  # Diff colour of each bar will be assigned automatically
        plt.xlabel('Letters')
        plt.ylabel('Frequency(%)')
        plt.legend()


def main():
    # 1 - Create objects of different classes and a list to save file names
    data = DataManager()
    histogram = HistogramGenerator()
    mapReduced = MapReduce()
    files_name = list()

    # 2 - Manage parameters and program configuration
    for i in range(1, len(sys.argv)):
        files_name.append(sys.argv[i])

    start_time = time.time()
    # 3 - Map reduce from every file introduced on arguments
    for file in files_name:
        input_file_lines = data.ReadFile(file)
        sum_words = data.WordCounter(input_file_lines)

        # 3.1 Mapping

        mapped_list = mapReduced.CleanAndMapFile(input_file_lines)
        del input_file_lines  # Eliminate list in order to clean memory

        # 3.2 Shuffling
        shuffled_dict = mapReduced.Shuffling(mapped_list)
        del mapped_list  # Eliminate list in order to clean memory

        # 3.3 Reducing
        reduced_list = mapReduced.Reducing(shuffled_dict)
        del shuffled_dict  # Eliminate list in order to clean memory

        # 3.4 Save and generate data
        data.GenerateResult(sum_words, reduced_list, file)
        histogram.GenerateHistogramData(sum_words, reduced_list)
    end_time = time.time()

    # 4- Write result and generate histogram if parameter is 'yes'
    data.PrintAndWriteFileResult("Result.txt")
    histogram.GenerateHistogram()

    print("Execution time: ", (end_time - start_time))
    plt.show()  # Show histogram

if __name__ == "__main__":
    if len(sys.argv) <= 1:  # First argument is always my_app_main.py and it's required to introduce almost 1 more
        print("Incorrect number or arguments")
    else:
        main()
