from fastapi import FastAPI
from typing import Dict
class Person():

    speccies = "human"
    def __init__(self,name:str,age:int):
        self.name = name
        self.age= age
    

def get_person_name(one_person:Person):
    return one_person.name

def get_age():
    try:
        age = int(input(("enter age : ")))
    except ValueError:
        print("Please enter a valid integer for age :")

    return age 
 
age = get_age()

op = Person(input("What will be your objects name : "),age)


print(op.name,op.speccies,op.age)
print(get_person_name(op))

Person.speccies= "Animal"
op.color = "brown"

print(op.speccies,op.color)

class Child(Person):
    get_age()
    def __init__(self, name, age):
        super().__init__(name, age)
    

c1  = Child("h")


