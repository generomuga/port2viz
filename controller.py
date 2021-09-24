import os

if __name__ == '__main__':

    try:
        os.system('python migrate.py')
        os.system('python main.py')
    except Exception as err:
        print (err)