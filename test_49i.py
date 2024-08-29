import os
from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.utils import load_config, setup_logging


config = load_config(config_file='nrbdaq.yaml')
tei49i = Thermo49i(config=config)

# setup logging
logfile = os.path.join(os.path.expanduser(config['root']), config['logging']['file'])
logger = setup_logging(file=logfile)

tei49i.get_data()

print(tei49i._data)