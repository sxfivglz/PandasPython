import mysql.connector

class ConexionDB:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                port=self.port,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            return True
        except mysql.connector.Error as err:
            print(f"Error al conectar a la base de datos: {err}")
            return False

    def ejecutarConsulta(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall(), [i[0] for i in self.cursor.description]
        except mysql.connector.Error as err:
            print(f"Error al ejecutar la consulta: {err}")
            return None, None

    def CerrarConexion(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
