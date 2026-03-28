# Databricks notebook source
# MAGIC %md
# MAGIC ##🌟 Collection of Modules

# COMMAND ----------

# MAGIC %md
# MAGIC ###🔹 Types of Libraries

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Python Standard Library
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The Python Standard Library is a collection of built‑in modules and packages that come bundled with Python, providing ready‑to‑use functionality for everyday programming tasks such as <b>file I/O, math, cmath, random, statistics, urllib, modules etc</b>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Examples of Common Modules</b>
# MAGIC <table border="1" cellpadding="10" cellspacing="0">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th>Module</th>
# MAGIC       <th>Purpose</th>
# MAGIC       <th>Example Usage</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>os</td>
# MAGIC       <td>Operating system interface</td>
# MAGIC       <td><code>os.getcwd()</code> → get current directory</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>sys</td>
# MAGIC       <td>System parameters &amp; functions</td>
# MAGIC       <td><code>sys.version</code> → check Python version</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>math</td>
# MAGIC       <td>Mathematical functions</td>
# MAGIC       <td><code>math.sqrt(16)</code> → returns 4.0</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>datetime</td>
# MAGIC       <td>Date &amp; time manipulation</td>
# MAGIC       <td><code>datetime.datetime.now()</code> → current timestamp</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>json</td>
# MAGIC       <td>JSON parsing &amp; serialization</td>
# MAGIC       <td><code>json.loads('{"a":1}')</code> → converts to dict</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>random</td>
# MAGIC       <td>Random number generation</td>
# MAGIC       <td><code><b>random.randint(1,10)</b></code> → random integer</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>shutil</td>
# MAGIC       <td>File operations</td>
# MAGIC       <td><code>shutil.copy("file.txt","backup.txt")</code></td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC

# COMMAND ----------

import random      #random is a standard library

print(random.randint(1,3))

# COMMAND ----------

import datetime

# Get current date and time
current_time = datetime.datetime.now()

print("Current Date and Time:", current_time)
print("Year:", current_time.year)



# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Numpy Library
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>NumPy</b> contains advance math functionalities along with tools to create and manipulate numerical arrays. <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;It is Python’s core library for scientific computing, offering fast, memory-efficient operations on large arrays and matrices. It powers data analysis, machine learning, and numerical simulations, and is widely used in India across industries like finance, retail, and AI research.
# MAGIC
# MAGIC <b>🔍 What Is NumPy?</b>
# MAGIC <br>NumPy (Numerical Python) is a powerful open-source library that provides:
# MAGIC <ul><li>Multidimensional array objects (ndarray) for efficient data storage and manipulation.</li>
# MAGIC <li>Vectorized operations that eliminate slow Python loops.</li>
# MAGIC <li>Mathematical functions for linear algebra, statistics, Fourier transforms, and more.</li>
# MAGIC <li>Random number generation, sorting, masking, and broadcasting.</li></ul>
# MAGIC It’s written in C for performance, but used in Python for flexibility.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Key Features table</b>
# MAGIC <table border="1" cellpadding="6" cellspacing="0">
# MAGIC   <thead>
# MAGIC     <tr>
# MAGIC       <th>Feature</th>
# MAGIC       <th>Description</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr>
# MAGIC       <td>ndarray</td>
# MAGIC       <td>Core data structure for N-dimensional arrays</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Vectorized operations</td>
# MAGIC       <td>Fast element-wise math without explicit loops</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Broadcasting</td>
# MAGIC       <td>Automatic expansion of arrays for operations across shapes</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Linear algebra</td>
# MAGIC       <td>Matrix multiplication, eigenvalues, decomposition</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Random module</td>
# MAGIC       <td>Generate random numbers, distributions, simulations</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td>Integration with other libs</td>
# MAGIC       <td>Works seamlessly with <b>Pandas, SciPy, scikit-learn, TensorFlow</b></td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC

# COMMAND ----------

import numpy as np

# Create a NumPy array
arr = np.array([1, 2, 3, 4, 5])

# Perform operations
print("Original array:", arr)
print("         Mean :",np.mean(arr))



# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Scipy Library
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>SciPy library</b> offers algorithmic and maths tool for scientific calculation.
# MAGIC <br>SciPy libraryin Python. SciPy builds on NumPy and provides advanced mathematical, scientific, and engineering functions.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Using `scipy.stats` for Statistical Functions</b>

# COMMAND ----------

import numpy as np
from scipy import stats

# Generate random data
data = np.random.normal(loc=50, scale=10, size=1000)

# Calculate mean and standard deviation
mean = np.mean(data)
std_dev = np.std(data)

# Perform a one-sample t-test (testing if mean = 50)
t_stat, p_value = stats.ttest_1samp(data, 50)

#print("Data:", data)
print("Mean:", mean)
print("Standard Deviation:", std_dev)
print("T-statistic:", t_stat)
print("P-value:", p_value)


# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Tkinter Library
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>Tkinter </b> provides traditional python user interface toolkit and help to create userfriendly <b>Graphical User Interfaces (GUIs)</b> interface for different type of application. Tkinter provides a simple way to create windows, buttons, labels, and other interactive elements without needing externalackages.

# COMMAND ----------

import tkinter as tk

# Create main window
root = tk.Tk()
root.title("My First Tkinter App")
root.geometry("300x200")

# Add a label
label = tk.Label(root, text="Hello, Tkinter!", font=("Arial", 14))
label.pack(pady=20)

# Add a button
button = tk.Button(root, text="Click Me", command=lambda: label.config(text="Button Clicked!"))
button.pack(pady=10)

# Run the application
root.mainloop()


# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Matplotlib Library
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>Matplotlib</b> is a popular Python library for data visualization. It allows you to create plots, charts, and graphs ranging from simple line plots to complex 2D/3D visualizations. <b>It’s widely used in data science, machine learning, and scientific computing to explore and present data visually.</b>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊Example of a bar chart using Matplotlib to visualize monthly purchase items:</b>

# COMMAND ----------

import matplotlib.pyplot as plt

# Sample data
months = ["Jan 2026", "Feb 2026", "Mar 2026", "Apr 2026", "May 2026", "Jun 2026", "Jul 2026"]
purchases = [120, 180, 240, 300, 220, 150, 170]

# Create the bar chart
plt.figure(figsize=(10, 6))
bars = plt.bar(months, purchases, color=["blue", "orange", "green", "red", "purple", "teal", "gold"])

# Add value labels on top of each bar
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 5, str(yval), ha='center', va='bottom', fontsize=10, color='black')

# Add labels and title
plt.xlabel("Month")
plt.ylabel("Number of Items")
plt.title("Monthly Purchase Items")
plt.grid(axis='y', linestyle='--', alpha=0.5)

# Show the plot
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Simple Line Plot</b>

# COMMAND ----------

import matplotlib.pyplot as plt

# Sample data
x = [1, 2, 3, 4, 5]
y = [2, 4, 6, 8, 10]

# Create a line plot
plt.plot(x, y, label="y = 2x", color="blue", marker="o")

# Add labels and title
plt.xlabel("X-axis")
plt.ylabel("Y-axis")
plt.title("Simple Line Plot with Matplotlib")
plt.legend()

# Show the plot
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###🔹 Structure

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Docstring
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>docstring (documentation string)</b> is a special string used to describe what a function, class, or module does. It helps developers understand the purpose and usage of code without reading through the implementation.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Function with Docstring</b>

# COMMAND ----------

def add_numbers(a, b):
    """    
    Returns:
    int or float: Sum of a and b
    """
    return a + b

# Accessing the docstring
print(add_numbers.__doc__)


# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Variable constants
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>variable</b> is a named reference that stores data which can change during program execution, while a <b>constant</b> is a value that is meant to remain fixed throughout the program.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Variable vs Constant>

# COMMAND ----------

# Variable (can change)
count = 10
print("Initial count:", count)

count = count + 5   # updating variable
print("Updated count:", count)

# Constant (by convention, should not change)
PI = 3.14159
radius = 7
area = PI * (radius ** 2)

print("Area of circle:", area)


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Classes
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>class</b> is a blueprint for creating objects. It defines the attributes (data) and methods (functions) that the objects created from the class will have. Classes are the foundation of Object-Oriented Programming (OOP) in Python, allowing you to model real-world entities and behaviors in code.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Simple Python Class

# COMMAND ----------

class calculator:

    def add(a,b):
        return a + b

    def subtract(a,b):
        return a - b

print(calculator.add(10,5))  # Output: 15


# COMMAND ----------

class calculator:
    def __init__(self, num1, num2):
        self.num1 = num1
        self.num2 = num2

    def add(self):
        return self.num1 + self.num2

    def subtract(self):
        return self.num1 - self.num2

    def multiply(self):
        return self.num1 * self.num2

    def divide(self):
        return self.num1 / self.num2
calculator_instance = calculator(10, 5)
print(calculator_instance.add())  # Output: 15
print(calculator_instance.subtract())  # Output: 5
print(calculator_instance.multiply())  # Output: 50
print(calculator_instance.divide())  # Output: 2


# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Objects
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>an object</b> are representation of some real or abstract entity.
# MAGIC <br>Object is an instance of a class. It represents a specific entity created from the class blueprint, containing both data (attributes) and behavior (methods). Every variable in Python is actually an object, since Python is an object‑oriented language.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Objects in Python

# COMMAND ----------

class Dog:
    def __init__(self, name, breed):
        self.name = name      # attribute
        self.breed = breed    # attribute

    def bark(self):           # method
        return f"{self.name} says Woof!"

# Create objects (instances of Dog class)
dog1 = Dog("Tommy", "Bulldog")
dog2 = Dog("Sheru", "German Shepherd")

# Access attributes and methods
print(dog1.name, "-", dog1.breed)
print(dog1.bark())

print(dog2.name, "-", dog2.breed)
print(dog2.bark())


# COMMAND ----------

# MAGIC %md
# MAGIC <b>✅ Why Objects Are Useful</b>
# MAGIC <ul><li>Allow modeling of real-world entities (dogs, cars, customers, datasets).
# MAGIC <li>Encapsulate data + behavior together.
# MAGIC <li>Enable reusability and scalability in large projects.

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Statements
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>a statement</b> represents an instruction to perform an action, it is a line of code that the interpreter can execute such as assigning a value, printing output, looping, or making a decision.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>📊 Example: Different Statements

# COMMAND ----------

# Assignment statement
x = 10

# Print statement
print("Value of x:", x)

# Conditional statement
if x > 5:
    print("x is greater than 5")



# COMMAND ----------

# MAGIC %md
# MAGIC ####7. Functions
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>function</b> is named group of instruction. It is a reusable block of code that performs a specific task

# COMMAND ----------

def func():
    # Assignment statement
    x = 10

    # Print statement
    print("Value of x:", x)

    # Conditional statement
    if x > 5:
        print("x is greater than 5")

print(func())