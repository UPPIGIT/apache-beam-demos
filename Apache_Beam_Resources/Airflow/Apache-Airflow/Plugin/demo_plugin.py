from airflow.plugins_manager import AirflowPlugin

class DemoPlugin(AirflowPlugin):
    name = "demo_plugin"
    operators = []
    sensors = []
