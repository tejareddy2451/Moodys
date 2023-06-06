from abc import ABC, abstractmethod

class abs_connector(ABC):
    @abstractmethod
    def open_connection(self):
        pass
    
    @abstractmethod
    def read_data(self):
        pass

    @abstractmethod
    def read_tgt_data(self):
        pass
    
    @abstractmethod
    def write_data(self, df, write_location):
        pass
    
    
    @abstractmethod
    def close_connection(self):
        pass
