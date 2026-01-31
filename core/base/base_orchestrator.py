from threading import Thread, Event
from abc import ABC, abstractmethod
import time


class BaseOrchestrator(ABC):   
    def __init__(self, name):
        self.name = name
        self._stop_event = Event()
        self._thread = None
        self.is_running = False
        self._master_stop = None

    def _should_stop(self, specific_event=None):
        if self._master_stop and self._master_stop.is_set():
            return True
        if specific_event and specific_event.is_set():
            return True
        if self._stop_event.is_set():
            return True
        return False
    
    def _execute_with_timeout(self, target_func, name, timeout):
        try:
            thread = Thread(target=target_func, daemon=True, name=name)
            thread.start()
            
            elapsed = 0
            while thread.is_alive() and elapsed < timeout:
                if self._should_stop():
                    return False
                time.sleep(0.5)
                elapsed += 0.5
            
            if thread.is_alive():
                return False
                
            return True
            
        except Exception as e:
            return False
    
    @abstractmethod
    def start(self):
        pass
    
    @abstractmethod
    def stop(self):
        pass
    
    @abstractmethod
    def _run_loop(self):
        pass