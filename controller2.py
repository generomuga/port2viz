import schedule
import time
import os

def job():
    os.system("python multithread.py")

if __name__ == '__main__':

    # Execute initial job
    job()

    # Run scheduled job
    schedule.every(1).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)