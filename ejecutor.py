import luigi
import time
import prueba as pr
a =  1
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
        # os.remove(self.output().path)

    def output(self):
        return luigi.LocalTarget('task_b.txt')


class TaskC(luigi.Task):
    """
    Tarea C: Depende de TaskA y TaskB. No se ejecuta hasta que ambas se hayan completado.
    """
    def requires(self):
        return [TaskA(), TaskB()]  # Depende de ambas TaskA y TaskB

    def run(self):
        pr.prueba_3()
        with self.output().open('w') as f:
            f.write("Output from Task C")
        
    def output(self):
        return luigi.LocalTarget('task_c.txt')
    
class TaskD(luigi.Task):
    """
    Tarea C: Depende de TaskA y TaskB. No se ejecuta hasta que ambas se hayan completado.
    """
    def requires(self):
        return [TaskC()]  # Depende de ambas TaskA y TaskB

    def run(self):

        try:
            pr.prueba_4()
            time.sleep(2)
            with self.output(1).open('w') as f:
                f.write("Output from Task D")
        
        except Exception as e:
            with self.output().open('w') as f:
                f.write("Output from Task E")

            self.set_status_message(f"<span style='color: red;'>Error en Tarea Tres: {str(e)}</span>")


    def output(self):
        return luigi.LocalTarget('task_d.txt')
    
class TaskE(luigi.Task):
    """
    Tarea C: Depende de TaskA y TaskB. No se ejecuta hasta que ambas se hayan completado.
    """
    def requires(self):
        return [TaskD()]  # Depende de ambas TaskD

    def run(self):
        try:
            time.sleep(1)

            with self.output().open('w') as f:
                f.write("Output from Task E")

            self.set_status_message("<span style='color: green;'>Tarea completada exitosamente</span>")
        
        except Exception as e:

            global a

            with self.output().open('w') as f:
                f.write("Output from Task E")

            self.set_status_message(f"<span style='color: red;'>Error en Tarea Tres: {str(e)}</span>")
            a = 0


    def output(self):
        return luigi.LocalTarget('task_e.txt')

class TaskF(luigi.Task):
    """
    Tarea C: Depende de TaskA y TaskB. No se ejecuta hasta que ambas se hayan completado.
    """

    def requires(self):
        return [TaskE(),TaskA()]  # Depende de ambas TaskA y TaskB

    def run(self):
        if a == 0:
              raise ValueError("Este es un error de ejemplo")
        else:
            time.sleep(2)
            pr.delete_txt_files()
            
    def output(self):
        return luigi.LocalTarget('task_f.txt')
    

@luigi.Task.event_handler(luigi.Event.FAILURE)
def tarea_fallida(task,exception):
    task.set_status_message(f"<span style='color: red;'>Tarea fallida: </span>")
    print(f"Tarea {task.__class__.__name__} falló con la excepción: {exception}")

if __name__ == '__main__':
    luigi.build([TaskF()], local_scheduler=True)

