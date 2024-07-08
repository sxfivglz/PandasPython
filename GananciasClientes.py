from ConexionDB import ConexionDB
from ProcesoDatos import Datos
import pandas as pd

if __name__ == "__main__":
    
    conexion = {
        "host": "localhost",
        "port": 3307,
        "user": "root",
        "password": "",
        "database": "northwind"
    }

    consulta = """ 
    SELECT products.ProductName, region.RegionDescription, orders.OrderDate, `Order details`.UnitPrice, `Order details`.Quantity, customers.CompanyName
    FROM products 
    INNER JOIN `Order details` ON products.ProductID = `Order details`.ProductID
    INNER JOIN orders ON orders.OrderID = `Order details`.OrderID 
    INNER JOIN customers ON customers.CustomerID = orders.CustomerID
    INNER JOIN employees ON employees.EmployeeID = orders.EmployeeID
    INNER JOIN employeeterritories ON employeeterritories.EmployeeID = employees.EmployeeID
    INNER JOIN territories ON territories.TerritoryID = employeeterritories.TerritoryID
    INNER JOIN region ON region.RegionID = territories.RegionID
    """
    db = ConexionDB(**conexion)
    db.connect()
    data, columna_final = db.ejecutarConsulta(consulta)
    db.CerrarConexion()
    datos = Datos(data, columna_final)

    datos.convertir_a_numerico(['UnitPrice', 'Quantity'])
    datos.calcular_columna('TotalSales', 'UnitPrice * Quantity')

    datos.df['Year'] = pd.to_datetime(datos.df['OrderDate']).dt.year

    grouped_df = datos.agrupar_sumar(['RegionDescription', 'CompanyName', 'ProductName', 'Year'], 'TotalSales')

    ventas_minimas_df = grouped_df.loc[grouped_df.groupby(['RegionDescription', 'CompanyName'])['TotalSales'].idxmin()]

    ventas_minimas_df['Product_Year'] = ventas_minimas_df['ProductName'] + ' (' + ventas_minimas_df['Year'].astype(str) + ')'

    pivot_df = ventas_minimas_df.pivot(index='CompanyName', columns='RegionDescription', values='Product_Year').reset_index()

    regiones = sorted(datos.df['RegionDescription'].unique())
    columna_final = ['Cliente'] + regiones
    pivot_df.columns = columna_final

    pivot_df = pivot_df.fillna("No aplica")

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    print("«" * 100)
    print("Clientes con ganancias mínimas por año y región:")
    print("«" * 100)
    print(pivot_df)
    datos.exportar_a_csv('clientes_ganancias_min.csv', pivot_df)
