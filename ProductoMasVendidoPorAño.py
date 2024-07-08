import pandas as pd
from ConexionDB import ConexionDB
from ProcesoDatos import Datos

if __name__ == "__main__":
    # Configuración de conexión a la base de datos
    conexion = {
        "host": "localhost",
        "port": 3307,
        "user": "root",
        "password": "",
        "database": "northwind"
    }

    # Consulta SQL para obtener los datos necesarios
    consulta = """
    SELECT 
        categories.CategoryName, 
        products.ProductName,
        region.RegionDescription,
        YEAR(orders.OrderDate) as Year,
        `order details`.UnitPrice,
        `order details`.Quantity,
        customers.CustomerID,
        customers.CompanyName
    FROM products 
    INNER JOIN `order details` ON products.ProductID = `order details`.ProductID
    INNER JOIN orders ON orders.OrderID = `order details`.OrderID 
    INNER JOIN customers ON customers.CustomerID = orders.CustomerID
    INNER JOIN employees ON employees.EmployeeID = orders.EmployeeID
    INNER JOIN employeeterritories ON employeeterritories.EmployeeID = employees.EmployeeID
    INNER JOIN territories ON territories.TerritoryID = employeeterritories.TerritoryID
    INNER JOIN region ON region.RegionID = territories.RegionID
    INNER JOIN categories ON categories.CategoryID = products.CategoryID
    """

    # Conexión a la base de datos
    db = ConexionDB(**conexion)
    db.connect()
    data, columnas = db.ejecutarConsulta(consulta)
    db.CerrarConexion()

    # Crear un objeto Datos con los datos obtenidos
    datos_obj = Datos(data, columnas)

    # Convertir las columnas 'Quantity' y 'UnitPrice' a numéricas
    datos_obj.convertir_a_numerico(['Quantity', 'UnitPrice'])

    # Calcular la ganancia (Ganancias) multiplicando Quantity por UnitPrice
    datos_obj.calcular_columna('Ganancias', 'Quantity * UnitPrice')

    # Filtrar los datos para obtener solo los últimos tres años
    anio_actual = datos_obj.df['Year'].max()
    anios_recientes = range(anio_actual - 2, anio_actual + 1)
    datos_filtrados = datos_obj.df[datos_obj.df['Year'].isin(anios_recientes)]

    # Agrupar por categoría, año, producto y cliente, y sumar las ganancias
    grouped_df = datos_filtrados.groupby(['CategoryName', 'Year', 'ProductName', 'CompanyName'])['Ganancias'].sum().reset_index()

    # Encontrar el producto con la ganancia máxima para cada categoría y año
    max_ganancias_products = grouped_df.loc[grouped_df.groupby(['CategoryName', 'Year'])['Ganancias'].idxmax()]

    # Función para obtener la ganancia máxima y mínima de cada cliente por producto y año
    def obtener_ganancias_clientes(dataframe, category, year, product):
        df_filtered = dataframe[(dataframe['CategoryName'] == category) & (dataframe['Year'] == year) &
                                (dataframe['ProductName'] == product)]
        if len(df_filtered) == 0:
            return pd.DataFrame({
                'CategoryName': [category],
                'Year': [year],
                'ProductName': [product],
                'MaxSoldCustomer': ["No aplica"],
                'MinSoldCustomer': ["No aplica"],
                'GananciasMax': ["No aplica"],
                'GananciasMin': ["No aplica"]
            }).fillna("No aplica")

        max_customer_row = df_filtered.loc[df_filtered['Ganancias'].idxmax()]
        min_customer_row = df_filtered.loc[df_filtered['Ganancias'].idxmin()]

        return pd.DataFrame({
            'CategoryName': [category],
            'Year': [year],
            'ProductName': [product],
            'MaxSoldCustomer': [max_customer_row['CompanyName']],
            'MinSoldCustomer': [min_customer_row['CompanyName']],
            'GananciasMax': [max_customer_row['Ganancias']],
            'GananciasMin': [min_customer_row['Ganancias']]
        }).fillna("No aplica")

    dfs = []

    # Iterar sobre los productos que generaron más ganancias por categoría y año
    for index, row in max_ganancias_products.iterrows():
        category = row['CategoryName']
        year = row['Year']
        product = row['ProductName']

        client_ganancias_info = obtener_ganancias_clientes(grouped_df, category, year, product)
        
        dfs.append(client_ganancias_info)

    clients_ganancias_df = pd.concat(dfs, ignore_index=True)

    merged_df = pd.merge(max_ganancias_products, clients_ganancias_df, on=['CategoryName', 'Year', 'ProductName'], how='left')


    if not merged_df.empty:
        merged_df['concatenacion_columnas'] = merged_df.apply(lambda row: f"{row['ProductName']}, Max Sold to: {row['MaxSoldCustomer']}, Ganancias Max: {row['GananciasMax']}, Min Sold to: {row['MinSoldCustomer']}, Ganancias Min: {row['GananciasMin']}", axis=1)

    # Pivoteando los datos
    try:
        pivot_df = merged_df.pivot_table(index='CategoryName', columns='Year', values='concatenacion_columnas', aggfunc=lambda x: ' '.join(str(v) for v in x))
        # Reordenar las columnas de años al revés
        pivot_df = pivot_df[pivot_df.columns[::-1]]
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(pivot_df)
        # Exportar los datos a un archivo CSV
        datos_obj.exportar_a_csv('productos_mas_vendidos_por_anio.csv', pivot_df)
    except Exception as e:
        print(f"Error al pivotar los datos: {e}")
