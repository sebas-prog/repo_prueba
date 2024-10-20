import prueba as pr
import pandas as pd
import luigi
import time

class TaskA(luigi.Task):
    """
    Tarea A: Simula trabajo que lleva 2 segundos.
    """
    def run(self):
        print("Running Task A...")
        time.sleep(2)  # Simula trabajo de 2 segundos
        pr.prueba_1()
        print("Task A is done.")
        with self.output().open('w') as f:
            f.write("Output from Task A")

    def output(self):
        return luigi.LocalTarget('task_a.txt')


class TaskB(luigi.Task):
    """
    Tarea B: Simula trabajo que lleva 3 segundos.
    """
    def run(self):
        print("Running Task B...")
        time.sleep(2)  # Simula trabajo de 3 segundos
        pr.prueba_2()
        print("Task B is done.")
        with self.output().open('w') as f:
            f.write("Output from Task B")
        

    def output(self):
        return luigi.LocalTarget('task_b.txt')
    
class TaskC(luigi.Task):
    """
    Tarea B: Simula trabajo que lleva 3 segundos.
    """
    def run(self):
        print("Running Task B...")
        time.sleep(2)  # Simula trabajo de 3 segundos
        pr.prueba_2()
        print("Task B is done.")
        with self.output().open('w') as f:
            f.write("Output from Task B")
    
    def output(self):
        return luigi.LocalTarget('task_c.txt')

class TaskD(luigi.Task):
    """
    Tarea C: Depende de TaskA y TaskB. No se ejecuta hasta que ambas se hayan completado.
    """
    # def requires(self):
    #     return [TaskA()]  # Depende de ambas TaskA y TaskB

    def run(self):
        # yield TaskB()
        # yield TaskC()
        pr.prueba_3()
        with self.output().open('w') as f:
            f.write("Output from Task C")
        # pr.delete_txt_files()
        
    def output(self):
        return luigi.LocalTarget('task_d.txt')

class TaskE(luigi.Task):

    def requires(self):
        # Generar múltiples tareas diferentes con yield
        yield TaskA()
        yield TaskB()
        yield TaskC()
        yield TaskD()
    

    def run(self):
        with self.output().open('w') as f:
            f.write("Output from Task C")
    
    def output(self):
        return luigi.LocalTarget('task_e.txt')


class TaskF(luigi.Task):

    def requires(self):
        # Generar múltiples tareas diferentes con yield
         return [TaskE()]
    

    def run(self):
        with self.output().open('w') as f:
            f.write("Output from Task C")
    
    def output(self):
        return luigi.LocalTarget('task_f.txt')


class TaskG(luigi.Task):
    """
    Tarea C: Depende de TaskA y TaskB. No se ejecuta hasta que ambas se hayan completado.
    """
    def requires(self):
        return [TaskF()]  # Depende de ambas TaskA y TaskB

    def run(self):
        # yield TaskB()
        # yield TaskC()
        pr.prueba_3()
        with self.output().open('w') as f:
            f.write("Output from Task C")
        
    def output(self):
        return luigi.LocalTarget('task_g.txt')
    
class TaskH(luigi.Task):

    def requires(self):
        return [TaskF()]  # Depende de ambas TaskA y TaskB

    def run(self):
        pr.prueba_3()
        with self.output().open('w') as f:
            f.write("Output from Task g")
        
    def output(self):
        return luigi.LocalTarget('task_h.txt')


class Paralelo(luigi.Task):

    def requires(self):
        return [TaskH(),TaskG()]  # Depende de ambas TaskA y TaskB

    def run(self):
        pr.delete_txt_files()
        
    def output(self):
        return luigi.LocalTarget('task_hola.txt')

if __name__ == '__main__':
    luigi.build([Paralelo()], local_scheduler=True)