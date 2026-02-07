from typing import Optional

class SAPSessionManager:
    session = None

    @classmethod
    def set_session(cls, sess):
        cls.session = sess

    @classmethod
    def get_session(cls):
        return cls.session