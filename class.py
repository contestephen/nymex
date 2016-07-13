class employee:

    empCount = 0

    def __init__(self, name, salary):
        self.name = name
        self.salary = salary
        employee.empCount +=1

    def displayCount(empcount):
        print 'total employee count is: %d'  %employee.empCount

    def displayData(self):
        print 'Employee name:', self.name, 'Salary: ', self.salary
    def displayName(self):
        names = []
        names += self.name
        print names
        


