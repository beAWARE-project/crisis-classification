from bus.message_listener import ListenerThread


def runListenThread():
    # Create new threads
    thread1 = ListenerThread(['TOP105_CRCL_INITIALIZATION'])

    # Start new Threads
    thread1.start()

    print("Exiting Main Thread")
    

if __name__ == "__main__":
    runListenThread()
    
