# -*- coding: utf-8 -*-

from main import reactor_start
import re
import json
import sys
import os

def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()

class Tests():
    def __init__(self):
        # files found in the result folder
        self.result_file_names = []
        # used to accumulate files ids foun in the document
        self.files_ids_list = []
        # an error was already pointed out
        self.error_shown = False

    def test_crawler(self, base_root_path, spider_name, login_id, test=True, crawl=True):
        base_folder = os.path.join(base_root_path, login_id)
        request_file_name = "{}/{}-request.json".format(base_folder, login_id)

        init_data = self.read_and_parse_json(request_file_name)
        init_data['request_params'] = json.dumps(init_data)
        scrape_id = init_data['scrape_id']
        if crawl:
            reactor_start(spider_name, init_data, None, 0, 1)

        if test:
            self.files_ids_list = []
            path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
            result_folder = os.path.join(path, "downloads", scrape_id)
            self.result_file_names = os.listdir(result_folder)

            base_file_names = os.listdir(base_folder)
            request_file_name = "{}-request.json".format(login_id)

            json_file_names = [
                filename for filename in base_file_names
                if filename.endswith("-data_collected.json")
            ]

            for json_file_name in json_file_names:
                result_file_path = os.path.join(result_folder, json_file_name)
                if not os.path.isfile(result_file_path):
                    print("\n", "Missing json file:", result_file_path, sep="\n")
                else:
                    print("\n\nComparing file {}".format(json_file_name))
                    base_file_path = os.path.join(base_folder, json_file_name)
                    result = self.check_json(base_file_path, result_file_path)
                    print(result)
                print("\n",'-----------------------------------------------------------')

            print("\n", "Conparison done.", sep="\n")


    def read_and_parse_json(self, file_path):
        with open(file_path, 'r') as json_file:
            return json.loads(json_file.read())


    # verify if base json has the same values as the result json
    def check_json(self, base_file_path, result_file_path):
        base_file = self.read_and_parse_json(base_file_path)
        result_file = self.read_and_parse_json(result_file_path)
        # ignore errors in all comparisons
        result_file.pop("errors", None)
        return self.dict_compare(base_file, result_file)

    # self.compare two  dictionaries
    def dict_compare(self, d_base, d_result):
        base_keys = set(d_base.keys())
        result_keys = set(d_result.keys())
        shared_keys = base_keys.intersection(result_keys)

        missing = base_keys - result_keys
        if len(missing) > 0:
            if not self.error_shown:
                print(
                    "\n",
                    "Keys {} from the test sample are missing in result. Please add them."
                        .format(missing),
                    "Test Sample: {}".format(base_keys),
                    "Result: {}".format(result_keys),
                    sep="\n"
                )
                self.error_shown = True
            return False

        extra = result_keys - base_keys
        if len(extra) > 0:
            if not self.error_shown:
                print(
                    "\n",
                    "Keys {} in result do not exist in the test sample, they should not be in the result. Please remove them.".format(extra),
                    "Test Sample: {}".format(base_keys),
                    "Result: {}".format(result_keys),
                    sep="\n"
                )
                self.error_shown = True
            return False

        for key in shared_keys:
            result_value = d_result[key]
            base_value = d_base[key]
            
            is_screenshot_id = key == '__screenshots_ids__'
            if is_screenshot_id:
                expected_screenshots_num = len(d_base[key])
                resulting_screenshots_num = len(d_result[key])
                if expected_screenshots_num != resulting_screenshots_num:
                    # file does not exist, handle error.
                    if not self.error_shown:
                        print(
                            "\n",
                            "Number of screenshots_ids does not match.",
                            "    Expected: {}".format(expected_screenshots_num),
                            "    Got: {}".format(resulting_screenshots_num),
                            sep="\n"
                        )
                        self.error_shown = True
                    return False
                continue

            is_file_key = self.check_match(key, "^__.*__$") 
            if is_file_key:
                file_id = result_value['file_id']
                # check if file_id was already used in data_collected
                file_reused = file_id in self.files_ids_list
                # no need to test again, if file is missing error was already shown
                if file_reused: continue
                # add file id to the list of processed ids
                self.files_ids_list.append(file_id)
                # check if identifyed pdf file is in folder
                file_name = "{}.pdf".format(file_id)
                file_exists = file_name in self.result_file_names
                # all ok, no errors
                if file_exists: continue
                # file does not exist, handle error.
                if not self.error_shown:
                    print(
                        "\n",
                        "File is missing: {}".format(file_name),
                        "file_id found under {}".format(key),
                        sep="\n"
                    )
                    self.error_shown = True
                return False

            if not self.compare(base_value, result_value):
                if not self.error_shown:
                    print(
                        "\n",
                        "{} does not have the expected value.".format(key),
                        "Expected: {}".format(base_value),
                        "Got: {}".format(result_value),
                        sep="\n"
                    )
                    self.error_shown = True
                return False

        # dicts are equal
        return True


    # self.compare two values of any type
    def compare(self, base_value, result_value):
        if isinstance(base_value, dict):
            is_dict = isinstance(result_value, dict)
            if not is_dict: return False
            result = self.dict_compare(base_value, result_value)
            return result

        if isinstance(base_value, list):
            is_list = isinstance(result_value, list)
            if not is_list: return False
            result = self.compare_lists(base_value, result_value)
            return result

        # if value is a number or currency, allow for small changes due to running interest rates
        base_number = self.string_to_number(base_value)
        if base_number != None and not is_integer(base_number):
            result_number = self.string_to_number(result_value)
            if result_number == None: return False
            result = self.close_enough(base_number, result_number)
            return result

        # values are equivalent
        result = base_value == result_value
        # avoid errors poping up twice in different scopes
        return result


    # boolean indicating if two floats are up to 5% distance from each other
    # used to avoid tests quickly becoming invalid due to interest rates recalculation.
    def close_enough(self, base_number, result_number):
        return (
            (base_number < (result_number * 1.05)) or
            (base_number > (result_number * 0.95))
        )

    # try o convert a string to float
    def string_to_number(self, s):
        s_type = self.get_scalar_type(s)
        # its already a number, just return it
        if s_type == "NUMBER": return s
        # if string does not sort of like look like a number => nothing to be returned
        if s_type != "NUMERIC_STRING": return None
        number = re.sub("[R\$ ]", "", s)
        proper_number = (
            number.replace(".", "").replace(",", ".")
                if self.check_match(number, ",\d\d$")
                else number.replace(",", "")
        )
        try: return float(proper_number)
        except ValueError: return None

    def get_scalar_type(self, v):
        if isinstance(v, bool): return "BOOLEAN"
        if isinstance(v, (int, float, complex)): return "NUMBER"
        # matches all characters allowed in something like a number
        is_match = self.check_match(v, "[R$ ,.\d]*")
        # if string does not sort of like look like a number => its a generic string
        if not is_match: return "STRING"
        # its a string that looks like a number
        return "NUMERIC_STRING"

    # checks if a string exactly matches a regex
    def check_match(self, s, regex):
        if not s or len(s) == 0 : return False
        result = re.match(regex, s)
        if not result: return False
        (start, end) = result.span()
        is_match = start == 0 and end == len(s)
        return is_match


    # check two lists have the same values, in any order
    def compare_lists(self, base_list, result_list):
        base_len = len(base_list)
        result_len = len(result_list)
        difference = base_len - result_len
        if difference != 0:
            if not self.error_shown:
                print(
                    "\n",
                    "List should have {} members. Found {}.\n".format(
                        base_len, result_len),
                    "    Expected: {}\n".format(base_list),
                    "    Found: {}".format(result_list),
                    sep="\n"
                )
                self.error_shown = True
            return False

        missing_base = [
            value for value in base_list if not value in result_list]
        if (len(missing_base) > 0):
            if not self.error_shown:
                print(
                    "\n",
                    "One or more items list are missing.\n",
                    "    Expected: {}\n".format(missing_base),
                    "    To be in: {}".format(result_list),
                    sep="\n"
                )
                self.error_shown = True
            return False

        extra_result = [
            value for value in result_list if not value in base_list]
        if (len(extra_result) > 0):
            if not self.error_shown:
                print(
                    "\n",
                    "One or more items list were not expected.",
                    "\nUnexpected values: {}".format(extra_result),
                    "\nOriginal list: {}".format(base_list),
                    sep="\n"
                )
                self.error_shown = True
            return False

        # lists are equivalent
        return True

tests = Tests()

args_count=len(sys.argv)
if args_count == 4:
  tests.test_crawler(sys.argv[1], sys.argv[2], sys.argv[3])
elif args_count == 5:
  tests.test_crawler(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "test")
elif args_count == 6:
  tests.test_crawler(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "test", sys.argv[5] == "crawl")
else:
  print("tests.py requires 3 to 5 arguments.")
