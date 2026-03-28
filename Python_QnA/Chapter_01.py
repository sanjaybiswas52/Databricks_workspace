# Databricks notebook source
# MAGIC %md
# MAGIC ###Chater_01

# COMMAND ----------

# MAGIC %md
# MAGIC ####01 Statement

# COMMAND ----------

def func():
  print("Hello")
  a = 15
  b = a - 10
  return b

print(func())

# COMMAND ----------

# MAGIC %md
# MAGIC ####02. Dynamic Typing
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>dynamic typing</b> means that you don’t need to declare the type of a variable explicitly. The type is determined at runtime based on the value assigned, and you can reassign the same variable to values of different types without error.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Dynamic Typing

# COMMAND ----------

# Variable initially holds an integer
x = 10
print("x:", x, "Type:", type(x))

# Reassign to a string
x = "Hello"
print("x:", x, "Type:", type(x))

# Reassign to a list
x = [1, 2, 3]
print("x:", x, "Type:", type(x))


# COMMAND ----------

# MAGIC %md
# MAGIC ####03. Operators

# COMMAND ----------

# MAGIC %md
# MAGIC #####(i) Exponentiation (**)

# COMMAND ----------

# 2 raised to the power of 3
result = 2 ** 3
print(result)   # Output: 8


# COMMAND ----------

# MAGIC %md
# MAGIC #####(ii) Identity vs Equality (is)
# MAGIC ✅ Summary
# MAGIC <ul><li>== → checks value equality.
# MAGIC <li>is → checks object identity (same memory reference).
# MAGIC <li>Use is for None checks (if x is None:) and identity comparisons, not for general value equality.

# COMMAND ----------

a = [1, 2, 3]
b = [1, 2, 3]

print(a == b)   # True → values are equal
print(a is b)   # False → different objects in memory


# COMMAND ----------

x = [10, 20]
y = x

print(x == y)   # True → values are equal
print(x is y)   # True → both point to the same object


# COMMAND ----------

# MAGIC %md
# MAGIC #####(iii) Bitwise XOR (^)
# MAGIC ✅ Summary
# MAGIC <ul><li> ^ is the bitwise XOR operator.
# MAGIC <li>Returns 1 if bits differ, 0 if bits are the same.
# MAGIC <br>&nbsp;&nbsp;  0101  (5)
# MAGIC <br>^ 0011  (3)
# MAGIC <br>&nbsp;&nbsp;  ----
# MAGIC <br>&nbsp;&nbsp;  0110  (6)
# MAGIC

# COMMAND ----------

a = 5   # binary: 0101
b = 3   # binary: 0011

result = a ^ b
print(result)   # Output: 6


# COMMAND ----------

# MAGIC %md
# MAGIC <b>XOR Trick (Swap without temp variable)

# COMMAND ----------

a = 15
b = 27

# Swap using XOR
a = a ^ b
b = a ^ b
a = a ^ b

print("a:", a)  # Output: 27
print("b:", b)  # Output: 15


# COMMAND ----------

num = 27

# Binary representation
binary_str = bin(num)
print("Binary:", binary_str)   # Output: 0b1111



# COMMAND ----------

# MAGIC %md
# MAGIC #####(iv) Membership operator (in)
# MAGIC ✅ Summary
# MAGIC <ul><li>"in" checks membership in sequences.
# MAGIC <li>Returns True if the element exists, False otherwise.
# MAGIC <li>Commonly used in lists, strings, sets, and dictionaries (checks keys).

# COMMAND ----------

text = "Python is powerful"

print("Python" in text)    # True
print("Java" in text)      # False


# COMMAND ----------

# MAGIC %md
# MAGIC ####04. For loop correction 

# COMMAND ----------

for k in range(0,10):
    if k % 4  == 0:
        print(k * 4)
    else :
        print(k * 3)

# COMMAND ----------

for k in range(0,10):
    if k % 4  == 0:
        print(f"k*4 --> {k * 4}")
    else :
        print(f" k+3 --> {k + 3}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####05. Writ the output of the following python code
# MAGIC <ul><li>The `ange(start, stop, step) function generates numbers starting from start, up to but not including `top, incremented by step.
# MAGIC <li>Here:
# MAGIC <ul><li>start = 2&nbsp;&nbsp;&nbsp;&nbsp;◦ stop = 7 (exclusive → 7 is not included)
# MAGIC <li>step = 2 </ul>
# MAGIC <li>So the sequence is: 2, 4, 6.

# COMMAND ----------

print('\nrange(2,7,2)')
for i in range(2,7,2):
    print(f"i={i} : {i*'$'}")

print('\nrange(3,7,2)')
for i in range(3,7,2):
    print(f"i={i} : {i*'*'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####06. Find and write the output of the followning python code

# COMMAND ----------

for name in ['Jay', 'Riya', 'Tanu', 'Anil']:
    print(name)
    if name[0] == 'I':
        break
    else:
        print('Finished')
print('Got it')

# COMMAND ----------

# MAGIC %md
# MAGIC ####07. List manipulation

# COMMAND ----------

list = [8,9,10]
print(list)

#second entry in index is 17
list[1] = 17
print(list)

#Add 4,5,6 to the end of the list
list.extend([4,5,6])
print(list)

#Remove the first entry from the list
list.pop(0)
print(list)

#Sort the list
list.sort()
print(list)

#Add Delhi in list
list.append('Delhi')
print(list)

#Add Delhi in list
list.insert(0,'Odisha')
print(list)

# COMMAND ----------

# MAGIC %md
# MAGIC ####08. What will be the output of the followinh python code

# COMMAND ----------

v=25
def fun(ch):
    v = 50
    print(v, end = ch)
    v = 2 ** 6
    print(v, end = ch)

print(v, end = "*")
fun("!")
print(v)



# COMMAND ----------

ch = '!'
v = 50
print(v, end = ch)
v = 2 ** 6
print(v, end = ch)


# COMMAND ----------

# MAGIC %md
# MAGIC ####09. Select the correct output of code

# COMMAND ----------

s = "Python is fun"
l = s.split()    # Split string based on whitespace
#s_new = "_".join(l[0].upper()+l[1]+l[2].capitalize())
s_new = "_".join([l[0].upper(),l[1],l[2].capitalize()])
print(s_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ####10. Possible outcome of code. Q8

# COMMAND ----------

import random

STRING = "CBSEONLINE"
NUMBER = random.randint(1,3)
print(NUMBER)
N = 9
while STRING[N] != 'L' :
    print (STRING[N] + STRING[NUMBER] + '#', end = '')
    NUMBER = NUMBER + 1
    N = N - 1
    




# COMMAND ----------

# MAGIC %md
# MAGIC ####11. Possible outcome of code. Q12

# COMMAND ----------

import random

PICK = random.randint(0,3)
print(PICK)

CITY = ["DELHI", "MUMBAI", "CHENNAI", "KOLKATA"]
for I in CITY:
    for J in range(1, PICK):
        print(I, end = ' ')
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ####12. Length conversion

# COMMAND ----------

MINE_TO_KM = 1.609344
KM_TO_MIL = 1/MINE_TO_KM
FEET_TO_INCHES = 12
INCHES_TO_FEET = 1/FEET_TO_INCHES

def miletokm(miles):
    """ Converts miles to kilometers """
    return miles * MINE_TO_KM

def kmtofeet(km):
    """ Converts kilometers to feet """
    return km * KM_TO_MIL*MINE_TO_FEET

def feettoinch(feet):
    """ Converts feet to inches"""
    return feet * FEET_TO_INCHES

def inchestofeet(inches):
    """ Converts inches to feet """
    return inches * INCHES_TO_FEET

print(miletokm(5))
#print(kmtofeet(1))
#print(feettoinch(1)
print(inchestofeet(24))

