class Nsga2:
    def __init__(self, inputPoints, dominates):
        self.inputPoints = inputPoints
        self.dominates = dominates
    
    def execute(self):
        paretoPoints = set()
        candidateRowNr = 0
        dominatedPoints = set()
        while True:
            candidateRow = self.inputPoints[candidateRowNr] # masuk row pertama
            self.inputPoints.remove(candidateRow) # buang row pertamanya
            rowNr = 0
            nonDominated = True # row kandidat yg baru masuk di set true buat non dominated nya
            while len(self.inputPoints) != 0 and rowNr < len(self.inputPoints):
                row = self.inputPoints[rowNr] # ambil row lanjutannya
                if self.dominates(candidateRow, row): # ngecek row kandidat ama row lanjutan di input_point dominasinya
                    # If it is worse on all features remove the row from the array
                    self.inputPoints.remove(row)
                    dominatedPoints.add(tuple(row))
                elif self.dominates(row, candidateRow):
                    nonDominated = False
                    dominatedPoints.add(tuple(candidateRow))
                    rowNr += 1
                else:
                    rowNr += 1

            if nonDominated:
                # add the non-dominated point to the Pareto frontier
                paretoPoints.add(tuple(candidateRow))

            if len(self.inputPoints) == 0:
                break
        return paretoPoints, dominatedPoints