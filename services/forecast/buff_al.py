from database.queries import SelectInfos


class ReturnBuffAssemblyLineValues(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def return_values_from_db(self):
        return self.select_bd_infos("SELECT knr, model, lfdnr_sequence FROM auto_line_feeding.assembly_line WHERE lane = 'reception'").lazy()