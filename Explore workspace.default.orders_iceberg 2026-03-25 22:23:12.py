# Databricks notebook source
TO = 30
for K in range(0,TO):
  if K/4 == 0 :
    print(4*K)
  else:
    print(K+3)
    

# COMMAND ----------

numberA = [9,18,27,36]

for num in numberA:
  for n in range(1, num%8):
    print(n,'#',end = " ")
print()





# COMMAND ----------

print(2**4)

# COMMAND ----------

a = 3
b = 3
print(a is b)

# COMMAND ----------

print(3 ^ 5)

# COMMAND ----------

listA = [8,9,10]
print(listA)

listA[1] = 17
print(listA)

listA.extend([4,5,6])
print(listA)

listA.pop(0)
print(listA)

listA.sort()
print(listA)