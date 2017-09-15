class Step:

    def __init__(self,name):
        self.name = name
        self.data = None

    def setData(self,data):
        self.data = data

    def perform(self):
        ## return data
        return self.result