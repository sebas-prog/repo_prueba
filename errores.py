import luigi
from luigi import Task, LocalTarget, Event

# Primera tarea
class TareaUno(luigi.Task):
    def output(self):
        return LocalTarget('salida_tarea_uno.txt')

    def run(self):
        try:
            with self.output().open('w') as f:
                f.write('Tarea Uno completada correctamente.')
            self.set_status_message("<span style='color: green;'>Tarea Uno completada exitosamente</span>")
        except Exception as e:
            self.set_status_message(f"<span style='color: red;'>Error en Tarea Uno: {str(e)}</span>")
            raise e

# Segunda tarea
class TareaDos(luigi.Task):
    def output(self):
        return LocalTarget('salida_tarea_dos.txt')

    def run(self):
        try:
            with self.output().open('w') as f:
                f.write('Tarea Dos completada correctamente.')
            self.set_status_message("<span style='color: green;'>Tarea Dos completada exitosamente</span>")
        except Exception as e:
            self.set_status_message(f"<span style='color: red;'>Error en Tarea Dos: {str(e)}</span>")
            raise e

# Tercera tarea
class TareaTres(luigi.Task):
    def output(self):
        return LocalTarget('salida_tarea_tres.txt')

    def run(self):
        try:
            with self.output().open('w') as f:
                f.write('Tarea Tres completada correctamente.')
            self.set_status_message("<span style='color: green;'>Tarea Tres completada exitosamente</span>")
        except Exception as e:
            self.set_status_message(f"<span style='color: red;'>Error en Tarea Tres: {str(e)}</span>")
            raise e

# Eventos globales para todas las tareas
@luigi.Task.event_handler(Event.SUCCESS)
def tarea_exitosa(task):
    task.set_status_message(f"<span style='color: green;'>Tarea {task.__class__.__name__} completada con éxito</span>")
    print(f"Tarea {task.__class__.__name__} completada con éxito.")

@luigi.Task.event_handler(Event.FAILURE)
def tarea_fallida(task, exception):
    task.set_status_message(f"<span style='color: red;'>Tarea {task.__class__.__name__} fallida: {exception}</span>")
    print(f"Tarea {task.__class__.__name__} falló con la excepción: {exception}")

if __name__ == '__main__':
    # Ejecutar múltiples tareas
    luigi.build([TareaUno(), TareaDos(), TareaTres()], local_scheduler=True)
