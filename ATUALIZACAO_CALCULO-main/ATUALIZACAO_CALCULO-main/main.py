
import pytz

from datetime import datetime
from manager.manager import Manager

if __name__ == "__main__": 
    tz = pytz.timezone('America/Sao_Paulo')
    today = datetime.now(tz)   
    manager = Manager(today)
    manager.run()
    exit()