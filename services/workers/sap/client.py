from typing import Optional, Tuple
from dotenv import load_dotenv
import win32com.client
import pythoncom
import time, os


load_dotenv("config/.env")


class SAP_Launcher:
    def __init__(self):
        self.sap_path = os.getenv("SAP_PATH")

    def start(self):
        path = self.sap_path.strip('"')
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.Run(f'"{path}"')
        time.sleep(5)

    def get_application(self):
        try:
            sap_gui = win32com.client.GetObject("SAPGUI")
            return sap_gui.GetScriptingEngine
        except:
            return None


class SAP_SessionProvider:
    def __init__(self, launcher: SAP_Launcher):
        self.launcher = launcher
        self.connection_name = os.getenv("SAP_CONNECTION_NAME")

    def get_existing_session(self):
        app = self.launcher.get_application()
        if not app:
            return None
        if app.Children.Count > 0:
            conn = app.Children(0)
            if conn.Children.Count > 0:
                return conn.Children(0)
        return None

    def open_new_session(self):
        app = self.launcher.get_application()
        conn = app.OpenConnection(self.connection_name, True)
        return conn.Children(0)


class SAP_Authenticator:
    def __init__(self):
        self.user = os.getenv("SAP_USER")
        self.password = os.getenv("SAP_PSWD")

    def login(self, session):
        session.findById("wnd[0]/usr/txtRSYST-BNAME").Text = self.user
        session.findById("wnd[0]/usr/pwdRSYST-BCODE").Text = self.password
        session.findById("wnd[0]").sendVKey(0)


class SAP_Client:
    def __init__(self, session_provider: SAP_SessionProvider, authenticator: SAP_Authenticator, launcher: SAP_Launcher):
        self.session_provider = session_provider
        self.authenticator = authenticator
        self.launcher = launcher
        self.session = None
        self.already_opened = False

    def connect(self):
        pythoncom.CoInitialize()

        sess = self.session_provider.get_existing_session()
        if sess:
            self.session = sess
            self.already_opened = True
            return self.session

        self.launcher.start()
        self.session = self.session_provider.open_new_session()
        self.authenticator.login(self.session)
        return self.session

    def run_transaction(self, tcode: str = "/n") -> Tuple[object, bool]:
        if not self.session:
            self.connect()
        self.session.findById("wnd[0]/tbar[0]/okcd").Text = tcode
        self.session.findById("wnd[0]").sendVKey(0)
        return self.session, self.already_opened