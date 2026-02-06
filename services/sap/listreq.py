from pathlib import Path
import os


class SP02_Session:
    def __init__(self, sap):
        self.sap = sap

    def open(self):
        session, _ = self.sap.run_transaction("/nSP02")
        return session
    

class SP02_Rows:
    def __init__(self, session):
        self.session = session

    def exists(self, element_id: str) -> bool:
        try:
            self.session.findById(element_id)
            return True
        except:
            return False

    def text(self, element_id: str) -> str:
        if self.exists(element_id):
            return self.session.findById(element_id).Text
        return ""

    def first_valid_row(self) -> int:
        i = 0
        while True:
            el = f"wnd[0]/usr/lbl[51,{i}]"
            if self.exists(el) and self.text(el).strip() != "":
                return i
            i += 1

    def iter_rows(self, start: int):
        i = start
        while True:
            el = f"wnd[0]/usr/lbl[51,{i}]"
            if not self.exists(el):
                break
            yield i
            i += 1

    def find_lt22_job(self):
        start = self.first_valid_row()
        for i in self.iter_rows(start):
            name = self.text(f"wnd[0]/usr/lbl[51,{i}]").lower()
            hour = self.text(f"wnd[0]/usr/lbl[30,{i}]")
            if "lt22" in name:
                return {"index": i, "name": name, "hour": hour}
            

class SP02_Actions:
    def __init__(self, sap):
        self.sap = sap
        self.path = Path(os.getenv("SAP_PATH")).resolve()
        self.filename = "alf_lt22"

    def download(self, session, index: int):
        session.findById(f"wnd[0]/usr/chk[1,{index}]").Selected = False
        session.findById(f"wnd[0]/usr/lbl[14,{index}]").setFocus()
        session.findById("wnd[0]").sendVKey(2)
        session.findById("wnd[0]/tbar[1]/btn[48]").press()
        session.findById("wnd[1]/tbar[0]/btn[0]").press()
        session.findById("wnd[1]/usr/ctxtDY_PATH").Text = str(self.path)
        session.findById("wnd[1]/usr/ctxtDY_FILENAME").Text = str(self.filename)
        session.findById("wnd[1]/tbar[0]/btn[11]").press()

    def clean(self, index: int):
        session, _ = self.sap.run_transaction("/nsp02")
        session.findById(f"wnd[0]/usr/chk[1,{index}]").Selected = True
        session.findById("wnd[0]/tbar[1]/btn[14]").press()
        session.findById("wnd[1]/usr/btnSPOP-OPTION1").press()