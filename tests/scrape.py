from bs4 import BeautifulSoup
import requests

url = "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html"

response = requests.get(url)

htmlref = BeautifulSoup(response.text, "html.parser")

#grab all <a> tags with class "reference internal" and get href links
hrefs = [a["href"] for a in htmlref.find_all("a", class_="reference internal")]

testdict = {}

for href in hrefs: 
  url = "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/" + href
  response = requests.get(url)
  func_page = BeautifulSoup(response.text, "html.parser")
  pre_tags = func_page.find_all("pre")
  examples = []
  for p in pre_tags:
    pre_content = p.get_text()
    examples.append(pre_content)
  testdict[href] = examples
key = list(testdict.keys())[0]

with open("auto_test.py", "w") as file:
    #loop through functions 
    for key in testdict.keys():
      #grab function name
      func_name = key.split("#")[1]
      #grab function examples
      examples = testdict[key]
      #write function examples to file
      if "function" in func_name:
        file.write(f"#{func_name}:\n")
        for chunk in examples:
          code_lines= chunk.split('\n')
          for line in code_lines:
              if line.startswith('>>> ') or line.startswith('... '):
                line = line.strip('>>> ').strip('... ')
                file.write(line + '\n')
          file.write('\n')

