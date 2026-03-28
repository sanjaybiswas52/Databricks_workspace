# Databricks notebook source
# MAGIC %md
# MAGIC ####Reverse string

# COMMAND ----------

# DBTITLE 1,Cell 1: Fixed SyntaxError with string reverse loop
s = "sanjay biswas"

len_str = len(s)
#print(f"{len_str} ")
#print(s[len_str -1:])

str = ""  # Initialize an empty string
for i in range(len_str):
  #print(f"{i} {s[i]}")
  #print(len_str-i -1)
  str += s[(len_str-i) -1]

print(str.upper())


# COMMAND ----------

# MAGIC %md
# MAGIC ####You want:
# MAGIC <ul><li>Input: "sanjay biswas"
# MAGIC <li>Replace all 'a' with 'Z'
# MAGIC <li>Except the first 'a' in the string

# COMMAND ----------

s = "sanjay biswas"

# Find index of the first 'a'
first_a_index = s.find('a')
print(first_a_index) 
print(s[:first_a_index + 1])

# If there's no 'a', just keep the string as-is
if first_a_index == -1:
    result = s
else:
    # Keep everything up to and including the first 'a' unchanged
    prefix = s[:first_a_index + 1]

     # Work on the rest of the string: replace all 'a' with 'Z'
    suffix = s[first_a_index + 1:].replace('a', 'Z')

    # Combine prefix and modified suffix
    result = prefix + suffix

print(result)  # Output: sanjay biswZs


# COMMAND ----------

# MAGIC %md
# MAGIC <b>To count</b> how many times the letter "a" appears in the string "sanjay biswas"

# COMMAND ----------

s = "sanjay biswas"
count_a = s.count('a')
print(count_a)   # Output: 3


# COMMAND ----------

# MAGIC %md
# MAGIC <b>Now want this behavior:
# MAGIC <ul><li>For the list: ["sanjay", "biswas"]
# MAGIC <li>Count how many 'a' are in each word
# MAGIC <li>Print word: that many of times

# COMMAND ----------


words = ["sanjay", "biswas"]

for word in words:
    count_a = word.count('a')   # count how many 'a' in the word
    print(f"count of 'a' in the word {word}: {count_a}" )
    for _ in range(count_a):    # print the word 'count_a' times
        print(word)
