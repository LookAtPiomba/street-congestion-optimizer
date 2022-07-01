import subprocess, sys, os
from typing import final

CMD = ['docker-compose', 'up', '-d'] # '--build', 
try:
    subprocess.run(CMD)
except Exception as e:
    print("Exception occurred while starting Docker")
    print(str(e))
finally:
    print('Succesfully executed')
    base_path = os.path.dirname(os.path.realpath(__file__))
    CMD_2 = ['python', base_path+'/app/sumo-starter.py', '-d']
    try:
        subprocess.run(CMD_2)
    except Exception as e:
        print("Exception occurred while calling sumo-starter.py")
        print(str(e))

