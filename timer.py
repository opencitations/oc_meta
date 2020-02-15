import timeit
from meta.run_process import process
from pymantic import sparql
import shutil, os


def patha(str):
    path = os.path.abspath(str)
    return str

def reset():
    ts = sparql.SPARQLServer("http://localhost:9999/blazegraph/sparql")
    ts.update('delete{?x ?y ?z} where{?x ?y ?z}')
    shutil.rmtree("meta\\demo\\dumontier\\corpus\\", ignore_errors=True)
    shutil.rmtree("meta\\demo\\dumontier\\CSVe\\", ignore_errors=True)
    with open(patha("meta\\DEMO\\Dumontier\\auxiliary\\auxiliary.txt"), 'w') as br:
        br.write('')
    br.close()

def timer():
    time_tot = 0
    x = 0
    while x < 10:
        x+=1
        reset()
        t = timeit.Timer(lambda: process("C:\\Users\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\CSV", "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\CSVe", r"C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\CSVe\\indices", r"C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\auxiliary\\auxiliary.txt",  source="https://api.crossref.org/snapshots/monthly/2019/09/all.json.tar.gz"))
        code_to_test = """
process(r"C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\CSV ", r"C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\CSVe", r"C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\CSVe\\indices", r"C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Dumontier\\auxiliary\\auxiliary.txt",  source=r"https://api.crossref.org/snapshots/monthly/2019/09/all.json.tar.gz")
        """
        #elapsed_time = timeit.timeit(code_to_test, number=1)
        elapsed_time = t.timeit(number=1)
        time_tot += elapsed_time
        print(elapsed_time)



    print("total = " + str(time_tot/10))


if __name__ == "__main__":
    timer()