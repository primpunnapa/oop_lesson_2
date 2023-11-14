import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))

class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def pivot_table(self, keys_to_pivot_list, keys_to_aggreagte_list, aggregate_func_list):
        unique_values_list = []
        for i in keys_to_pivot_list:
            _list = []
            for j in self.select(keys_to_pivot_list):
                if j[i] not in _list:
                    _list.append(j[i])
            unique_values_list.append(_list)

        import combination_gen
        com = combination_gen.gen_comb_list(unique_values_list)

        pivot = []
        for k in com:
            # print(keys_to_pivot_list[0], keys_to_pivot_list[1], keys_to_pivot_list[2], k)
            combine = self.filter(lambda x: x[keys_to_pivot_list[0]] == k[0])
            for m in range(1, len(keys_to_pivot_list)):
                combine = combine.filter(lambda x: x[keys_to_pivot_list[m]] == k[m])
            comb_list = []
            for n in range(len(keys_to_aggreagte_list)):
                result_comb = combine.aggregate(aggregate_func_list[n], keys_to_aggreagte_list[n])
                comb_list.append(result_comb)
            pivot.append([k, comb_list])
        return pivot

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('players', players)
table4 = Table('teams', teams)
table5 = Table('titanic', titanic)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')
my_table2 = my_DB.search('countries')
my_table3 = my_DB.search('players')
my_table4 = my_DB.search('teams')
my_table5 = my_DB.search('titanic')

# Player on a team with “ia” in the team name played less than 200 minutes and made more than 100 passes.
# my_table3_filtered = my_table3.filter(lambda x: 'ia' in x['team'] and int(x['minutes']) < 200 and int(x['passes']) > 100)
# for i in my_table3_filtered.table:
#     print(i['surname'], i['team'], i['position'])
#     print()
#
# # The average number of games played for teams ranking below 10 versus teams ranking above or equal 10.
# my_table4_filtered_below = my_table4.filter(lambda x: int(x['ranking']) < 10)
# my_table4_filtered_eq_ab = my_table4.filter(lambda x: int(x['ranking']) >= 10)
# print("avg of teams ranking below 10 is", my_table4_filtered_below.aggregate(lambda x: sum(x) / len(x), "games"))
# print("avg of teams ranking above or equal 10 is", my_table4_filtered_eq_ab.aggregate(lambda x: sum(x) / len(x), "games"))
# print()
#
# # The average number of passes made by forwards versus by midfielders
# my_table3_filtered_fw = my_table3.filter(lambda x: x['position'] == 'forward')
# my_table3_filtered_mid = my_table3.filter(lambda x: x['position'] == 'midfielder')
# print("avg of number of passes made by forwards", my_table3_filtered_fw.aggregate(lambda x: sum(x) / len(x), "passes"))
# print("avg of number of passes made by midfielders", my_table3_filtered_mid.aggregate(lambda x: sum(x) / len(x), "passes"))
# print()
#
# # The average fare paid by passengers in the first class versus in the third class
# my_table5_filtered_1 = my_table5.filter(lambda x: int(x['class']) == 1)
# my_table5_filtered_3 = my_table5.filter(lambda x: int(x['class']) == 3)
# print("avg of number of fare paid by first class is", my_table5_filtered_1.aggregate(lambda x: sum(x) / len(x), "fare"))
# print("avg of number of fare paid by third class is", my_table5_filtered_3.aggregate(lambda x: sum(x) / len(x), "fare"))
# print()
#
# # The survival rate of male versus female passengers
# my_table5_filtered_male = my_table5.filter(lambda x: x['gender'] == 'M')
# my_table5_filtered_female = my_table5.filter(lambda x: x['gender'] == 'F')
#
# my_table5_survived_male = my_table5_filtered_male.filter(lambda x: x['survived'] == 'yes')
# my_table5_survived_female = my_table5_filtered_female.filter(lambda x: x['survived'] == 'yes')
#
# male = len(my_table5_survived_male.table) / len(my_table5_filtered_male.table)
# female = len(my_table5_survived_female.table) / len(my_table5_filtered_female.table)
# print("survival rate of male is ", male)
# print("survival rate of female is ", female)

# Test for task3
my_pivot1 = my_table5.pivot_table(['embarked', 'gender', 'class'], ['fare', 'fare', 'fare', 'last'],
                                  [lambda x: min(x), lambda x: max(x), lambda x: sum(x)/len(x), lambda x: len(x)])
print(my_pivot1)

my_pivot2 = my_table3.pivot_table(['position'], ['passes', 'shots'],
                                  [lambda x: sum(x)/len(x), lambda x: sum(x)/len(x)])
print(my_pivot2)

my_table6 = my_table1.join(my_table2, 'country')
my_pivot3 = my_table6.pivot_table(['EU', 'coastline'], ['temperature', 'latitude', 'latitude'],
                                  [lambda x: sum(x)/len(x), lambda x: min(x), lambda x: max(x)])
print(my_pivot3)

my_pivot4 = my_table5.pivot_table(['class', 'gender', 'survived'], ['survived', 'fare'],
                                  [lambda x: len(x), lambda x: sum(x)/len(x)])
print(my_pivot4)


# print(my_table3.table_name, my_table3)

# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
# print(my_table1_filtered)
# print()
#
# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
# print(my_table1_selected)
# print()
#
# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item['temperature']))
# print(sum(temps)/len(temps))
# print()
#
# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
# print()
#
# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search('countries')
# my_table3 = my_table1.join(my_table2, 'country')
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(['city', 'country', 'temperature']))
# print()
#
# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
# print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
# print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
# print()
#
# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
#     if len(my_table1_filtered.table) >= 1:
#         print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
# print()
