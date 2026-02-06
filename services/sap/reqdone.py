from datetime import datetime


class LT22_Session:
    def __init__(self, sap):
        self.sap = sap

    def open(self):
        session, _ = self.sap.run_transaction("/nLT22")
        return session


class LT22_Selectors:
    def __init__(self, session):
        self.session = session
        self.node = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[1]/shell"
        self.take = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/cntlSUB_CONTAINER/shellcont/shellcont/shell/shellcont[0]/shell"

    def expand(self, node_id: str):
        self.session.findById(self.node).expandNode(node_id)

    def select(self, node_id: str):
        self.session.findById(self.node).selectNode(node_id)

    def set_top(self, node_id: str):
        self.session.findById(self.node).topNode = node_id

    def press_take(self):
        self.session.findById(self.take).pressButton("TAKE")


class LT22_Parameters:
    def __init__(self, session):
        self.session = session
        self.b01 = "wnd[0]/usr/ssub%_SUBSCREEN_%_SUB%_CONTAINER:SAPLSSEL:2001/ssubSUBSCREEN_CONTAINER2:SAPLSSEL:2000/ssubSUBSCREEN_CONTAINER:SAPLSSEL:1106/ctxt%%DYN002-LOW"

    def set_deposit(self):
        self.session.findById("wnd[0]/usr/ctxtT3_LGNUM").Text = "ANC"
        self.session.findById("wnd[0]/tbar[1]/btn[16]").press()

    def set_b01(self):
        self.session.findById(self.b01).text = "B01"
        self.session.findById("wnd[0]").sendVKey(0)

    def set_pending_only(self):
        self.session.findById("wnd[0]/usr/radT3_OFFTA").select()

    def set_dates_today(self):
        today = datetime.now().strftime("%d.%m.%Y")
        self.session.findById("wnd[0]/usr/ctxtBDATU-LOW").Text = today
        self.session.findById("wnd[0]/usr/ctxtBDATU-HIGH").Text = today

    def set_layout(self):
        self.session.findById("wnd[0]/usr/ctxtLISTV").Text = "/sys_knr"


class LT22_Submit:
    def __init__(self, session):
        self.session = session

    def submit(self):
        self.session.findById("wnd[0]").sendVKey(9)
        self.session.findById("wnd[1]/usr/ctxtPRI_PARAMS-PDEST").text = "LOCL"
        self.session.findById("wnd[1]/tbar[0]/btn[13]").press()
        self.session.findById("wnd[1]/usr/btnSOFORT_PUSH").press()
        self.session.findById("wnd[1]/tbar[0]/btn[11]").press()