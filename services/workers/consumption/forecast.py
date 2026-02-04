from database.queries import SelectInfos

class ReturnReceptionValues(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def return_values_from_db(self):
        query = "SELECT knr, model, lfdnr_sequence FROM auto_line_feeding.assembly_line WHERE lane = 'reception'"
        df = self.select_bd_infos(query)
        print(df.collect())


ReturnReceptionValues().return_values_from_db()