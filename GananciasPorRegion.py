from ConexionDB import ConexionDB
from ProcesoDatos import Datos
import pandas as pd

if __name__ == "__main__":
    
    conexion = {
        "host": "localhost",
        "port": "3307",  
        "user": "root",
        "password": "",
        "database": "northwind"
    }

   
    consulta = """
    SELECT DISTINCT od.OrderID, od.UnitPrice, od.Quantity, TRIM(r.RegionDescription) AS Region
    FROM `order details` AS od
    INNER JOIN orders AS o ON o.OrderID = od.OrderID
    INNER JOIN employees AS e ON e.EmployeeID = o.EmployeeID
    INNER JOIN employeeterritories AS et ON et.EmployeeID = e.EmployeeID
    INNER JOIN territories AS t ON t.TerritoryID = et.TerritoryID
    INNER JOIN region AS r ON r.RegionID = t.RegionID
    """

    db = ConexionDB(**conexion)
    db.connect()
    data, column_names = db.ejecutarConsulta(consulta)
    db.CerrarConexion()

    datos = Datos(data, column_names)

    datos.convertir_a_numerico(['UnitPrice', 'Quantity'])
    datos.calcular_columna('Ganancia', 'UnitPrice * Quantity')
    ganancias_por_region_ordenadas = datos.agrupar_sumar('Region', 'Ganancia')

    ganancias_por_region_ordenadas = ganancias_por_region_ordenadas.rename(columns={'Region': 'Región'})
    ganancias_por_region_ordenadas['Región'] = ganancias_por_region_ordenadas['Región'].str.strip(
    )

 
    pd.set_option('display.max_colwidth', None)
    print("«" * 20)
    print("Ganancias por Región")
    print("«" * 20)
    print(ganancias_por_region_ordenadas.to_string(index=False, justify='left'))
    print("»" * 20)
    datos.exportar_a_csv('ganancias_por_region.csv', ganancias_por_region_ordenadas)