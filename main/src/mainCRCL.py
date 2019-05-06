# Created Date: 03/05/2019
# Modified Date:
#
# -----------------------------------------------------------------------------------------------------
# 2nd PROTOTYPE VERSION based on:
#   - VICENZA PILOT for the Flood,
#   - THESSALONIKI PILOT for the HeatWave (1st Prototype)
#   - VALENCIA PILOT for the Fire pilot (simulations)
#
#   Inputs from logger and mySQL database

from bus.message_listener import ListenerThread
from bus.bus_consumer import BusConsumer
import json
from threading import Thread

def runListenThread():
    # Create new threads
    thread1 = ListenerThread(['TOP105DEV_CRCL_INITIALIZATION', 'TOP021_INCIDENT_REPORT'])

    thread1.start()

    print("Existing Main Thread")


if __name__ == "__main__":
    import sys

    runListenThread()

