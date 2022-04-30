from multiprocessing import Process, Lock
lck=Lock()
a=0
def func_print(item):
    lck.acquire()
    try:
        for i in range(5):
            print(item)
    finally:
        lck.release()
if __name__=="__main__":
    items=['Hi','Hello','Bye']
    for item in items:
        p=Process(target=func_print,args=(item,))
        p.start()